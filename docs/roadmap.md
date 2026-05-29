# Implementation Roadmap

Plan of record for moving from the current legacy schema (single `jobs` table, env-based allowlists) to the target schema designed in [migrations/_design/target_schema.sql](../migrations/_design/target_schema.sql).

## Goals

- **Top-down, additive-first rollout**: get peripheral infrastructure (inbox, installer gating, policies, identity) live and verified in production *before* touching the actual benchmark execution path.
- **Bound the cutover risk**: minimise the size of the "true big-bang" moment where behaviour changes; everything that *can* be observed in production without changing behaviour *should* be.
- **Commits as logical units for review**, but deploy each slice as soon as it lands (no months-long branch).

## Architectural target

The target schema is in [`migrations/_design/target_schema.sql`](../migrations/_design/target_schema.sql) with the rationale inline in column/table comments. The principles that produced it are durable enough to repeat here:

- **Subject vs Provenance**: subject data ("what is this job about?") earns typed relational structure because we query it; provenance data ("what envelope caused enqueue?") lives in `job_event.detail` JSONB.
- **Installation is the tenant boundary**: every tenant-scoped policy is per-installation. `allowed_installer` is the one operator-global gate (must come before any installation exists).
- **Soft-disable only**: operator-curated rows (allowlists, policies) toggle an `enabled` flag rather than being deleted. FK chains deliberately omit `ON DELETE CASCADE` to enforce this.
- **GH numeric IDs as natural keys**: installation id, repo id, user id, account id — all stable across renames and case. Login/name columns are display-only.
- **DB enforces structural truths; app enforces workflow rules**: FKs, uniqueness, range checks live in the DB; state-machine transitions, JSON shape discipline, "completed jobs must have metrics" live in Rust.
- **Identity and membership are separate**: `github_repo` is pure identity; `github_installation_repo` records membership with `revoked_at` soft-delete.
- **Handler is token-less, processor is privileged**: the web-facing handler does HMAC verify + bounded inbox insert + 2xx, no GitHub API calls. The orchestrator (= the processor) does all API work, policy evaluation, lineage resolution, and job creation. `github_webhook` is the inbox between them.

## Why incremental, not big-bang

Initial instinct was a single coordinated wipe+redeploy: small blast radius (testing data only), clean cutover, no dual-pipeline complexity. The decisive counter-argument was **debugging surface during the cutover window**. With the additive plan:

- Every slice is independently observable in production
- Bugs surface in DB rows, not in broken `/benchmark` flows
- Existing orchestrator + `jobs` table stay completely untouched through Phase 1
- The "true big-bang" risk shrinks to slice 11 only — by which point every supporting table has been live for as long as Phase 1 took

The legacy table (`jobs`, plural) and target table (`job`, singular) are differently named — coexistence is at the table level, not a column-flag inside one schema. That makes additive deployment cheap.

## Phase 1 — Peripherals

Handler grows a dual-write: every webhook still triggers the existing `/benchmark` flow into legacy `jobs`, AND writes a row into the new inbox. The processor reads from inbox and **does not create benchmark jobs in Phase 1** — but it may still classify webhooks, upsert supporting state (installations, repos, users, PRs), and resolve lineage/policy decisions for job-producing events. For `/benchmark` events specifically, the processor's classification is observational only; the legacy handler→`jobs` path actually runs the bench.

End-state of Phase 1: every webhook arriving in production produces the correct classification and the correct downstream state (installation rows, policy decisions, PR rows, user upserts) — verifiable in DB without affecting the running `/benchmark` flow.

| Slice | What ships | Live impact |
| ----- | ---------- | ----------- |
| 0 | Base enum types, `pgcrypto`, `set_updated_at()` helper (roles already exist from original setup; per-table grants ship with each later slice) | None — DB-only |
| 1 | `github_webhook` inbox table + handler dual-write (new persistence boundary, see risk note) | Rows accumulate; processor doesn't exist yet |
| 2a | Processor scaffold: claim loop, retries, stuck-claim sweep | Rows transition `received → processing → ?` but classify-only |
| 2b | Inbox classifier: `ignored_action` / `ignored_no_command` / `error` cases | Real outcome data flows; observe in DB |
| 3 | `allowed_installer` + installer gate processor logic | Real `github_installation` rows get created on install events |
| 4 | `github_repo` lineage + `supported_repo_root` + `github_installation_repo` | Processor resolves and persists repo identity |
| 5 | `target_repo_policy` + `source_repo_policy` + `trigger_policy` | Processor evaluates policies — but for `/benchmark` PR events, just logs the decision; legacy path still creates the actual job |
| 6 | `github_user` + `github_user_role` | Processor evaluates user authz — logging only for `/benchmark` |
| 7 | `github_pull_request` subject model | Processor materialises PR rows on PR events |

### Implementation

#### Global Notes

- The legacy schema already has `job_status`; incremental migrations should reuse it, not recreate it.
- Add `CREATE EXTENSION IF NOT EXISTS pgcrypto;` early if we rely on `gen_random_uuid()`.
- Each slice should include: migration, grants, Rust DB layer, tests, docs/ops notes/updates.
- Keep code comments minimalistic; don't write a narrative, just document the what (and why, if applicable).

#### Phase 1 Todos

##### Slice 0: Foundations

**Status:**

- [x] Initial implementation completed
- [x] Review in progress (with Codex)
- [x] Complete (ready for next slice)

**Todo's:**

1. Add base SQL helpers/enums that do not conflict with legacy `jobs`.
2. Add/update `set_updated_at()` helper.
3. Verify `sbgh_handler` / `sbgh_orch` roles already exist (no creation needed; managed idempotently by `sbgh-migrate/src/main.rs` since the original setup).
4. Preserve all current `jobs` grants.
5. Add migration tests or a local dry-run against current DB shape.
6. Confirm existing services still start unchanged (inferred from no Rust/table/grant changes; not verified by actually restarting the stack).

**Implementation notes/deviations:**

- Migration file: `migrations/20260527000001_slice0_foundations.sql`. Contains: `pgcrypto` extension, `set_updated_at()` trigger function, 9 enum types (`github_account_type`, `user_role`, `job_kind`, `trigger_kind`, `git_ref_kind`, `job_event_kind`, `job_event_status`, `github_webhook_status`, `github_webhook_outcome`).
- `job_status` enum intentionally NOT re-created — the legacy `20260521000001_init.sql` already defines it with values identical to the target schema (`'queued', 'running', 'completed', 'failed', 'cancelled'`). Later slices reuse it directly.
- Roles `sbgh_handler` and `sbgh_orch` already existed from the original setup. No changes to `sbgh-migrate/src/main.rs` were needed — slice 0 adds only types (not tables/columns), and per-table grants ship with their respective tables in later slices.
- Verification: `just build --no-sccache` (clean release build, migration embedded via `sqlx::migrate!`), `just lint --no-sccache` (clean), `just test --summary --no-sccache` (117 tests pass). No DB integration test run locally — the full docker-compose stack would be needed to verify the migration applies against a live Postgres; deferred until slice 1 where there's actual schema to validate.
- Compose stack untouched — services will pick up the migration on next `docker compose -f docker/docker-compose.yml up -d migrate`.

##### Slice 1: Webhook Inbox + Handler Dual Write

**Status:**

- [x] Initial implementation completed
- [x] Review in progress (with Codex)
- [x] Complete (ready for next slice)

**Todo's:**

1. Add `github_webhook_status`, `github_webhook_outcome`, and `github_webhook`.
2. Grant handler insert-only access to inbox columns.
3. Introduce an `IngestStore`-style boundary that can insert legacy `jobs` + `github_webhook` in one transaction.
4. Handler verifies HMAC, filters unsupported event types, inserts inbox row, and still enqueues legacy jobs.
5. Keep unsupported event types log-only, no DB row.
6. Add tests for duplicate delivery, rollback atomicity, invalid signature no-row, unsupported event no-row.

**Implementation notes/deviations:**

- Migration file: `migrations/20260527000002_slice1_webhook_inbox.sql`. Creates the `github_webhook` table with the claim index. The `status` / `outcome` enums were already created in slice 0, so todo #1 was partially redundant — only the table itself is new in this slice.
- `github_installation_id BIGINT REFERENCES github_installation (id)` column is INTENTIONALLY DEFERRED to slice 3. The FK target doesn't exist yet, and no code path writes that column until slice 3 anyway. Handler writes only `payload_installation_id` (the raw payload value, no FK).
- New persistence boundary: `IngestStore` trait in [crates/sbgh-core/src/db/ingest.rs](../crates/sbgh-core/src/db/ingest.rs), with `PostgresIngestStore` and `InMemoryIngestStore` impls. Two methods: `ingest_webhook` (inbox-only) and `ingest_webhook_and_job` (real transactional dual-write into webhook + legacy `jobs`). `JobStore` remains untouched — orchestrator still uses it for queue claim/transition.
- `IngestOutcome::Recorded { job_id: Option<Uuid> }` distinguishes "webhook + new job" from "webhook + legacy job conflict-skipped" (the latter only happens for deliveries that existed in `jobs` before slice 1 rolled out). Both are success from GitHub's perspective.
- Handler refactored to use `IngestStore`. New event allowlist (`SUPPORTED_EVENT_TYPES = [issue_comment, push, pull_request, create, installation, installation_repositories]`) drops unsupported events at the wire with no DB row. `ping` continues to short-circuit with no DB write. All supported events get a webhook row; only authorized `/benchmark` on a PR also gets a legacy job row.
- All previously-existing handler response strings preserved (`pong`, `ignored`, `not a PR`, `no command`, `unauthorized`, `queued`, `duplicate`). New responses: `recorded` (supported event accepted into inbox without legacy job), `missing delivery id` (BAD_REQUEST when X-GitHub-Delivery header is absent on a supported event).
- Webhook-only payload requirement: `delivery_id` becomes mandatory for supported events (was previously optional). Returns 400 if missing — without it, dedup is impossible.
- Grants added in `sbgh-migrate/src/main.rs`: handler gets INSERT on the 6 columns it writes (`delivery_id`, `event_type`, `action`, `payload_installation_id`, `payload`, `payload_size_bytes`) + SELECT on `(id, delivery_id)` for `ON CONFLICT ... RETURNING id`. Plus `USAGE ON SEQUENCE github_webhook_id_seq` for the BIGSERIAL default to fire. Orchestrator gets full `SELECT, UPDATE` (anticipating the slice 2a claim path).
- Tests updated: harness uses `InMemoryIngestStore`; existing assertions extended to also check `webhook_count()` / `webhooks()`. Five new tests added: `drops_unsupported_event_type_without_inbox_row`, `rejects_missing_delivery_id`, `supported_event_records_webhook_only` (push event path), `duplicate_delivery_on_webhook_only_path_dedupes`, `malformed_issue_comment_payload_still_records_webhook`. Test count: 117 → 122.
- **Rollback atomicity coverage gap**: the Postgres impl in `postgres_ingest.rs` uses a real transaction (`pool.begin()` → conditional `tx.commit()`), so a failed legacy job INSERT correctly rolls back the inbox INSERT. But the in-memory test impl has no transaction semantics (webhook is committed before job enqueue), so the slice-1 unit tests cannot prove the rollback property. Validating it requires a Postgres integration test — deferred until we stand up a test-DB harness (likely slice 2a or a dedicated test-infrastructure slice). Acknowledged limitation rather than a code defect.
- **Malformed `issue_comment` payload bypass** (fixed mid-slice per Codex review): the typed-parse failure path originally returned 400 before inserting the inbox row, breaking both the "all supported events get an inbox row post-HMAC" invariant and GitHub redelivery dedup (a 400 makes GH retry an un-dedupable delivery indefinitely). Fixed by restructuring `handle_issue_comment` to define the `webhook_only` closure before the typed parse, so the parse-failure branch records the inbox row and returns 2xx "bad payload".
- **NULL payload binding** (fixed mid-slice per Codex review): `NewWebhook.payload` changed from `Value` to `Option<Value>` so unparseable bodies bind SQL NULL (not JSON `null`). Lets ops queries use `payload IS NULL` to detect missing/cleared payloads without false matches on legitimately-null JSON bodies.
- Verification: `just build --no-sccache` (clean release build), `just lint --no-sccache` (clean after `just fix` rustfmt pass), `just test --summary --no-sccache` (122 tests, 0 failures). No DB integration test run locally; the docker-compose stack would validate the migration applies against live Postgres.

##### Slice 2a: Processor Scaffold

**Status:**

- [x] Initial implementation completed
- [x] Review in progress (with Codex)
- [x] Complete (ready for next slice)

**Todo's:**

1. Add orchestrator inbox claim loop with `FOR UPDATE SKIP LOCKED`.
2. Implement `received` / `processing` / `retryable_error` / `failed` transitions.
3. Implement attempts/backoff and stuck-claim recovery.
4. Keep classification minimal; no domain-specific effects yet.
5. Add integration tests for concurrent claims, retry backoff, exhausted attempts, stale claim recovery.

**Implementation notes/deviations:**

- **Scaffold ships compiled-in but unwired.** The `webhook_processor` module is added to `sbgh-orchestrator/src/main.rs` with `#[allow(dead_code)]` and `main()` does NOT call `WebhookProcessor::run()`. Slice 2b plugs in a real `Classifier` and spawns the loop alongside the existing job `Runner`. Production behavior at the slice 2a deploy is identical to slice 1's — inbox accumulates rows; nothing reads them yet. This matches the "deploy each slice as soon as it lands" goal without driving any new runtime effects.
- **Data layer (sbgh-core)**:
  - New types in `models.rs`: `WebhookStatus` and `WebhookOutcome` enums (sqlx mappings to the DB enums from slice 0), plus `WebhookOutcome::terminal_status()` for the outcome→status mapping.
  - New module `db/webhook.rs`: `WebhookInbox` trait + `ClaimedWebhook` struct.
  - Postgres impl `db/postgres_webhook.rs`: claim uses single-statement `UPDATE ... WHERE id IN (SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING ...`. All mutating ops conditional on `claim_token` matching + status='processing', so stale-claim writes (sweeper raced ahead) become no-ops. Sweep uses `make_interval(secs => $1)` to parameterize the lease cleanly.
  - In-memory impl `db/in_memory_webhook.rs` (feature-gated `testing`; was `test-support` at the time of slice 2a, renamed in slice 2.5): mirrors the state machine using a single `Mutex` for serialization. Includes test helpers `seed()`, `set_next_attempt_at()`, `set_claimed_at()` so tests don't need to poke private fields.
- **Processor (sbgh-orchestrator)**:
  - `Classifier` trait with two outcomes: `ClassifyOutcome::Terminal(WebhookOutcome)` and `ClassifyOutcome::Retryable(String)`.
  - `NoopClassifier`: production-safe default that returns `Terminal(IgnoredAction)` for everything. Useful as a slice-2b starting point (replace, don't add).
  - `WebhookProcessor::process_one()` is the unit-testable atom (claim → classify → write transition); `WebhookProcessor::run()` composes it with periodic sweeps and idle backoff in a fault-tolerant loop (errors logged + swallowed).
  - Backoff: exponential `base * 2^(attempt-1)` capped at `backoff_max`. `ProcessorConfig::default()` picks 30s base / 15min cap / 5min claim_lease / 5 max attempts — tunable for production via slice 2b config plumbing.
  - Attempts-exhaustion semantics: classifier returns `Retryable` → if `claimed.attempts + 1 >= max_attempts`, promote to `record_permanent_failure` (status=failed, outcome=error) instead of `record_retryable_error`.
- **Tests** (9 new, total 122 → 131):
  - `process_one_terminates_with_outcome`: full claim → terminal cycle.
  - `process_one_returns_false_when_empty`: idle behavior.
  - `retryable_increments_attempts_and_sets_backoff`: first-retry semantics + backoff_base honored.
  - `attempts_exhausted_promotes_to_permanent_failure`: max_attempts threshold respected, AND `attempts` is incremented on the permanent failure path so the final count is accurate (fixed mid-slice per Codex review).
  - `sweep_resets_stuck_processing_rows`: stuck-claim recovery actually transitions rows.
  - `concurrent_claims_pick_disjoint_rows`: two `claim_next` calls return different ids (the `FOR UPDATE SKIP LOCKED` invariant in Postgres terms, here proven via the in-memory Mutex serialization).
  - `stale_claim_writes_are_no_ops`: a processor whose lease was reset by the sweeper cannot corrupt the row's new state via late writes.
  - `complete_clears_last_error_from_prior_retries`: a row that transient-failed then succeeded ends with `last_error = NULL` (no stale error string lingers on the terminal row).
  - `backoff_doubles_until_cap`: unit test for the `backoff_delay` math (covers cap behavior).
- **Fixed mid-slice per Codex review**:
  - `record_permanent_failure` now increments `attempts` (matching `record_retryable_error`'s behavior), so a row that fails after N attempts shows `attempts = N` instead of `N-1`. Affects both Postgres and in-memory impls.
  - `complete` now clears `last_error = NULL`. Previously, a row that transient-errored once and then succeeded would carry the stale error string forever, misleading any ops query scanning for active error states. If we ever want "historical last transient error" semantics, that's a separate column — the in-active row shouldn't carry an error in its primary error field.
- **Coverage limitation carried forward from slice 1**: in-memory inbox has no real Postgres semantics. The `claim_next` concurrency test proves the Mutex-serialized version, but the actual `FOR UPDATE SKIP LOCKED` guarantee on real Postgres remains unproven by these unit tests. Tracked under the same "test-DB harness" gap noted in slice 1.
- **Cargo.toml**: `sbgh-orchestrator` `[dev-dependencies]` now pulls `sbgh-core` with `features = ["testing"]` (was `test-support` at the time of slice 2a, renamed in slice 2.5) so the in-memory inbox is available to tests.
- Verification: `just build --no-sccache` (clean release build), `just lint --no-sccache` (clean after `just fix` rustfmt pass), `just test --summary --no-sccache` (131 tests, 0 failures).

##### Slice 2b: Basic Inbox Classification

**Status:**

- [x] Initial implementation completed
- [x] Review in progress (with Codex)
- [x] Complete (ready for next slice)

**Todo's:**

1. Parse known event envelopes from inbox payload.
2. Classify ignored actions and no-command issue comments.
3. Mark malformed/transient API failures as `retryable_error` or `error` appropriately.
4. Clear payload only for terminal ignored/denied/failed rows where allowed.
5. Verify outcomes in DB without affecting legacy `/benchmark`.

**Implementation notes/deviations:**

- **`BasicClassifier` is now the production classifier.** Lives in `crates/sbgh-orchestrator/src/webhook_processor.rs` alongside the slice 2a `WebhookProcessor` + `NoopClassifier` (the latter is now `#[cfg(test)]`-gated since `BasicClassifier` is the real production wiring).
- **`WebhookProcessor::run()` is now wired into the orchestrator's `main()`.** Runs concurrently with the legacy job `Runner` via `tokio::try_join!`. Either loop returning Err crashes the binary so systemd restarts cleanly. Both share the same Postgres pool (cloned once).
- **Classification matrix for Phase 1**:
  - `issue_comment` with non-`created` action → `ignored_action`
  - `issue_comment.created` on a non-PR issue → `ignored_action`
  - `issue_comment.created` on a PR without `/benchmark` → `ignored_no_command`
  - `issue_comment.created` on a PR with malformed `/benchmark` args → `ignored_no_command` (schema has no distinct "malformed command" outcome; bucketed with no-command for now)
  - `issue_comment.created` on a PR with valid `/benchmark` → `ignored_action` in slice 2b; **pre-slice-6 checkpoint flipped this to `would_enqueue_job`** once slice 5 added real policy evaluation. Slice 9 will then change it to `enqueued_job` + create the new-schema `job` row. Legacy handler→`jobs` path continues to actually run the bench in the meantime.
  - `push` / `pull_request` / `create` / `installation` / `installation_repositories` → **NOT claimed by BasicClassifier; rows stay in `received`** waiting for the slice (3-7) that adds their classifier branch. This is intentional: terminalizing them now would prevent later slices from consuming the events they need (an `installation.created` ignored here would never create its `github_installation` row in slice 3). Fixed mid-slice per Codex review.
  - Anything else, if it somehow reaches the classifier → `error`. The claim filter restricts to `issue_comment`, but the catch-all defends against direct calls / future-slice misconfiguration.
- **Payload-parse failures**: `issue_comment` with NULL payload (handler stored only metadata) or with JSON that doesn't match the typed event shape → `error`. Other event types don't require a parse in slice 2b, so they tolerate NULL payload — but they're also not claimed, so it doesn't matter yet.
- **`ClassifyOutcome::Retryable` is `#[allow(dead_code)]`** for now — `BasicClassifier` never emits it. Later slices that hit GitHub APIs will use it for transient network/rate-limit failures.
- **Payload-clearing on terminal rows** (todo #4): the schema's payload-retention contract already permits the orchestrator to NULL out `payload` on terminal `ignored` / `denied` / `failed` rows. `WebhookInbox`'s current methods don't do this proactively yet — would be a tiny enhancement (a `cleared_payload` flag on the SQL UPDATE), but slice 2b deliberately keeps the payload around for observability during the early observation period. Can land as a follow-up when post-cutover storage growth warrants.
- **Grants**: no changes to `sbgh-migrate/src/main.rs`. Slice 1 already granted `sbgh_orch` full `SELECT, UPDATE` on `github_webhook`, which is everything the processor needs.
- **Tests** (8 new BasicClassifier tests, total 131 → 140):
  - `basic_issue_comment_non_created_is_ignored_action`
  - `basic_issue_comment_on_non_pr_is_ignored_action`
  - `basic_issue_comment_pr_no_command_is_ignored_no_command`
  - `basic_issue_comment_pr_with_benchmark_is_ignored_action_in_phase1` (pins the Phase 1 behavior; renamed by the pre-slice-6 checkpoint to `..._is_would_enqueue_job_in_phase1` once slice 5 added real policy evaluation; slice 9 will flip the assertion again to `enqueued_job`)
  - `basic_issue_comment_null_payload_is_error`
  - `basic_issue_comment_bad_typed_shape_is_error`
  - `basic_classifier_supported_types_is_issue_comment_only` (pins the slice 2b contract — replaces the original `basic_other_supported_events_are_ignored_action` test, which asserted the wrong behavior per Codex review)
  - `basic_classifier_leaves_future_slice_events_in_received` (Codex high finding: end-to-end proof that `installation` rows are NOT claimed/terminalized by slice 2b)
  - `basic_unsupported_event_is_error` (defensive)
- **Dead-code cleanup** as part of wiring: `NoopClassifier` → `#[cfg(test)]` (production now uses `BasicClassifier`); `ScriptedClassifier::seen()` test helper removed (never called); `ClassifyOutcome::Retryable` marked `#[allow(dead_code)]` with a forward-looking doc note.
- **Fixed mid-slice per Codex review**:
  - **High**: BasicClassifier was terminalizing rows for `push` / `pull_request` / `create` / `installation` / `installation_repositories` as `ignored_action`, which would have prevented slices 3-7 from consuming the events they need (e.g., an `installation.created` ignored here would never produce a `github_installation` row in slice 3). Fixed by adding `Classifier::supported_event_types()` + threading the filter through `WebhookInbox::claim_next(&[&str])`. BasicClassifier now declares only `["issue_comment"]` as supported; other event types stay `received` for later slices to claim. Tested end-to-end by `basic_classifier_leaves_future_slice_events_in_received`.
  - **Medium**: `WebhookProcessor::run()` previously swallowed all errors forever, making `tokio::try_join!` in `main.rs` unreachable on persistent infrastructure failures (DB down, grants revoked, schema drift). Added `ProcessorConfig::max_consecutive_errors` (default 10) with TWO independent counters — `consecutive_process_errors` and `consecutive_sweep_errors`. Either reaching the threshold bails `run()` and forces a systemd restart. Counters reset on their own category's success, so a persistently-broken sweep can't be masked by an otherwise-healthy process loop (the original single-counter design had this bug — Codex caught it on the follow-up review).
  - **Low**: roadmap operational note had the dominant outcome inverted — see updated note below.
- Verification: `just build --no-sccache` (clean release build), `just lint --no-sccache` (clean after `just fix` rustfmt pass + dropping one useless-conversion `map_err`), `just test --summary --no-sccache` (140 tests, 0 failures).
- **Operational note for the slice 2b deploy**: the processor will start consuming accumulated `issue_comment` inbox rows from slice 1's deploy window on first start; rows for other event types stay in `received` until their slice. Expected mix in the `github_webhook` table:
  - `status='ignored'` with `outcome IN ('ignored_action', 'ignored_no_command')` — issue_comment rows the classifier terminalized.
  - `status='received'` for `event_type IN ('push', 'pull_request', 'create', 'installation', 'installation_repositories')` — slices 3-7 will start consuming these.
  - `status='processed'` should be empty until slice 9 starts producing `enqueued_job` outcomes.
  - Anything in `status='failed'` (i.e., `outcome='error'`) is the signal to investigate — typically a classifier bug or malformed payload from GH.

##### Slice 2.5: Integration Test Harness + Backfill

**Status:**

- [x] Initial implementation completed
- [x] Integration coverage added (or N/A justified)
- [x] Review in progress (with Codex)
- [x] Complete (ready for next slice)

**Todo's:**

1. Rename existing `test-support` cargo feature to `testing` across the workspace (one unified name for test-only code).
2. Extract a shared `setup_pg() -> Option<(ContainerAsync<Postgres>, Pool)>` helper into `sbgh-core/src/db/test_support.rs`, gated on `testing`, so all integration test files reuse one setup boilerplate + the skip-on-no-Docker pattern.
3. Refactor `sbgh-migrate` from a binary-only crate into a `lib.rs` + `bin/sbgh-migrate.rs` shape; expose `apply_roles()` so grants tests can invoke real role setup against an ephemeral container.
4. Add `crates/sbgh-core/tests/postgres_ingest.rs` covering slice 1: transactional dual-write rollback (job-insert failure rolls back webhook insert), `ON CONFLICT (delivery_id)` SQL-level dedup, NULL vs JSON-null payload distinction, `payload_size_bytes` round-trip.
5. Add `crates/sbgh-core/tests/postgres_webhook.rs` covering slice 2a/2b: `FOR UPDATE SKIP LOCKED` under concurrent claimers, claim-token-guarded conditional updates (stale writes no-op against real Postgres), `sweep_stuck_claims` semantics with `make_interval`, event_type filter restricts claim correctly.
6. Add `crates/sbgh-migrate/tests/grants.rs` (in sbgh-migrate to keep `apply_roles` co-located with its tests, and to avoid a cyclic dev-dep on sbgh-core → sbgh-migrate → sbgh-core) covering slice 1 grant correctness: `sbgh_handler` connects + INSERTs only into approved columns + is rejected on others; `sbgh_orch` SELECT/UPDATE works; webhook sequence USAGE works.
7. Add `crates/sbgh-orchestrator/tests/processor_e2e.rs` covering the full slice 1 + 2a/2b seam: handler-shaped `IngestStore::ingest_webhook` insert → `WebhookProcessor::process_one` → assert terminal status/outcome in DB.
8. Add a new line to the slice status block template: `- [ ] Integration coverage added (or N/A justified)` between "Initial implementation" and "Review in progress". Update all not-yet-complete slices (3-12) to use the new template.

**Implementation notes/deviations:**

- **Feature renamed `test-support` → `testing`**. Now activated explicitly via `dep:testcontainers` / `dep:testcontainers-modules` (moved from `[dev-dependencies]` to optional `[dependencies]`) so the shared helper can be reached from downstream crates' tests, not just from sbgh-core's own. sbgh-core gets a self-dep `sbgh-core = { path = ".", features = ["testing"] }` in `[dev-dependencies]` to activate the feature for its own integration tests.
- **Shared helper landed**: `sbgh-core/src/db/test_support.rs` exposes `setup_pg()` returning `Option<TestPg>` with the same skip-on-no-Docker pattern as the original `postgres_jobs.rs::setup()`. Old `postgres_jobs.rs` refactored to use the shared helper.
- **`sbgh-migrate` split into `lib.rs` + `main.rs`**: role/grant logic moved to `lib.rs::apply_roles()`; `main.rs` now a thin CLI shell that calls into the lib. `sql_string_literal` and its unit tests follow into the lib. Grants tests in `sbgh-migrate/tests/grants.rs` invoke `apply_roles()` directly against an ephemeral Postgres.
- **`sbgh-orchestrator` stays bin-only**; `processor_e2e.rs` uses the same `#[path = "../src/webhook_processor.rs"] mod ...` include pattern the handler tests already use for `routes/mod.rs`. Added `#[allow(dead_code)]` on the path-include because the e2e test exercises a subset of the module's surface (no `run()`, no `NoopClassifier`) and would otherwise trip clippy's `-D warnings`.
- **Tests added (slice 2.5 net)**:
  - `postgres_ingest.rs` (7 tests): SQL-level dedup, dual-write success, **transactional rollback** when jobs INSERT fails (the slice 1 coverage gap Codex flagged), `None payload → SQL NULL` vs `Some(Value::Null) → JSON null`, `payload_size_bytes` round-trip, default `status='received'`.
  - `postgres_webhook.rs` (10 tests): empty filter behavior, **event_type filter leaves non-matching rows in `received`** (the slice 2b high-finding invariant proven against real Postgres), real `FOR UPDATE SKIP LOCKED` concurrency proves disjoint claims, terminal transition, **stale claim-token writes are no-ops at SQL layer**, sweep recovers stuck rows via `make_interval`, sweep leaves fresh rows alone, permanent failure increments attempts (slice 2a Codex-fix invariant), complete clears `last_error`.
  - `grants.rs` (12 tests, in `sbgh-migrate`): handler INSERT into approved jobs columns works, handler INSERT specifying `status` is rejected, handler SELECT `head_sha` is rejected, handler INSERT into approved webhook columns works, handler INSERT specifying webhook `status` is rejected, handler can use webhook sequence USAGE, **handler SELECT on webhook `payload` and `status` is rejected** (added per Codex M-finding on slice 2.5), orch SELECT/UPDATE on jobs works, orch INSERT on jobs is rejected, **orch SELECT/UPDATE on github_webhook works** (the actual slice 2b runtime path; added per Codex M-finding), **orch INSERT on github_webhook is rejected** (handler owns inbox writes; added per Codex M-finding), `apply_roles` is idempotent.
  - `processor_e2e.rs` (4 tests, in `sbgh-orchestrator`): full pipeline `IngestStore` → `WebhookProcessor::process_one` → DB-verified terminal state for `ignored_no_command`, `ignored_action` on `/benchmark` in Phase 1 (pins slice-9 changeover point), `installation` event stays `received` (slice 2b high-finding invariant via the production code path), batch processing through multiple rows.
  - Total test count: 140 → 191 (net new ~33 integration; the remainder are the existing testcontainers tests benefiting from the shared helper, plus path-include duplicates of the orchestrator's existing unit tests).
- **Policy change applied**: every slice status block template (slices 2.5 and 3-12) now has a fourth checkbox `- [ ] Integration coverage added (or N/A justified)` between "Initial implementation" and "Review in progress". The "or N/A justified" wording lets pure-doc slices skip it cleanly. Slices 0, 1, 2a, 2b are NOT retroactively updated — slice 2.5 IS their integration coverage backfill.
- **Verification**: `just build --no-sccache` (clean release build), `just lint --no-sccache` (clean after one `just fix` rustfmt pass + the path-include dead-code allow), `just test --summary --no-sccache` (191 tests, 0 failures, ~7s wall-clock — testcontainers really is sub-second per container with the image warm + nextest's parallel execution).

##### Slice 3: Allowed Installer

**Status:**

- [x] Initial implementation completed
- [x] Integration coverage added (or N/A justified)
- [x] Review in progress (with Codex)
- [ ] Complete (ready for next slice)

**Todo's:**

1. Add `allowed_installer` and `github_installation`.
2. Add admin/CLI seed command for allowed installer by resolving login → numeric account id.
3. Processor handles `installation.created`, resolves account id, checks allowlist, creates installation or denies.
4. Processor handles `installation.suspend` / `installation.unsuspend` by setting/clearing `github_installation.suspended_at`.
5. Processor handles `installation.deleted` as installation removal: do not disable `allowed_installer`; revoke active repo memberships and target policies in later membership/policy slices once those tables exist.
6. Handle disabled installers as `denied_install_allowlist`.
7. Tests for allowed install, denied install, sparse payload fallback, duplicate/concurrent install events, suspend/unsuspend, and deleted install handling.

**Implementation notes/deviations:**

- **Crate rename `sbgh-migrate` → `sbgh-cli`** (forward-looking restructure): the old single-purpose binary becomes a clap subcommand router. `sbgh-cli migrate` (the default subcommand, equivalent to the legacy bin's behavior) keeps the schema+grants one-shot path; `sbgh-cli installer {allow,disable,list}` adds the slice 3 admin commands. Docker image renamed to `sbgh-cli:latest`; compose `migrate` service container renamed `sbgh-cli-migrate` and now passes `command: ["migrate"]`. sanity-check.sh, host-bringup.md, .env.example updated accordingly. Crate dir moved via `git mv` so history is preserved.
- **Migration file**: `migrations/20260527000003_slice3_install_allowlist.sql`. Creates `allowed_installer` + `github_installation` tables (with `set_updated_at` touch triggers), `ALTER TYPE github_webhook_outcome ADD VALUE 'processed_installation'`, and `ALTER TABLE github_webhook ADD COLUMN github_installation_id ... REFERENCES github_installation(id)` — the deferred FK column the slice 1 migration comment promised.
- **New outcome `processed_installation`** added to the `github_webhook_outcome` enum (and `WebhookOutcome::ProcessedInstallation` in Rust, mapping to `WebhookStatus::Processed`). Distinguishes "we created/updated install state" from `IgnoredAction` (event handled but no material state change). Used for every successful `installation.{created,suspend,unsuspend,deleted}` branch.
- **Classifier refactored from a monolith into a router + per-event handlers**: new `EventHandler` trait (one impl per event_type, owns its own DB/API deps); `BasicClassifier` becomes a `HashMap<&str, Arc<dyn EventHandler>>` router built via `BasicClassifier::builder().with_handler(...).build()`. `Classifier::supported_event_types()` signature changed from `&'static [&'static str]` to `&[&'static str]` so the router can compose the list from registered handlers at runtime. `IssueCommentHandler` (slice 2b logic moved out unchanged) + `InstallationHandler` (new, slice 3) are the two production handlers; slices 4-7 register their own as they ship.
- **Builder panics on duplicate event_type registration** (caught at startup, not silently shadowed at runtime). Tested.
- **InstallationStore trait** (`sbgh-core/src/db/installation.rs`) with `PostgresInstallationStore` and `InMemoryInstallationStore` impls. Four methods: `lookup_allowed`, `upsert_installation`, `set_suspended`, `delete_installation`. The `set_suspended` returning `Option<...>` and `delete_installation` returning `bool` both encode "no row existed" as a successful no-op so the processor doesn't have to special-case "we never accepted this install" (it gets `ignored_unknown_installation` instead).
- **`installation.deleted` does a hard DELETE in slice 3** (no dependent tables exist yet). Slices 4-5 will replace this with soft-revoke of memberships + policies in the same transaction once those tables ship. A test in `postgres_installation.rs` pins the slice 3 behavior so the slice-4 change becomes visible.
- **No GitHub API client extension needed by the processor** — the `installation` webhook payload includes `installation.account.{id,login,type}`, so the handler reads everything from the payload. The CLI's `installer allow` and `installer disable --login` commands DO need the API to resolve login → id; they call GitHub's unauthenticated `GET /users/{login}` endpoint directly (60/hr per IP is plenty for operator one-shots). No `GitHubApi` trait change, no App credentials needed in the CLI's env. See the Codex review fix-ups below for why this is unauthenticated rather than App-JWT-signed.
- **InstallationEvent typed payload** added to `sbgh-core::github::webhook` (`pub struct InstallationEvent { action, installation: InstallationDetails }` with nested `InstallationAccount`). Only deserialises the fields slice 3 reads; `repositories` is intentionally NOT parsed (slice 4 consumes `installation_repositories` events for that).
- **Grants** (in `sbgh-cli/src/lib.rs::apply_roles`): orch gets `SELECT` on `allowed_installer` (read-only; the allowlist is operator-curated and a compromised processor must not be able to allowlist itself) and full CRUD (`SELECT, INSERT, UPDATE, DELETE`) on `github_installation`. Handler gets nothing on either table — it doesn't know either exists.
- **CLI installer subcommand**: `sbgh-cli installer allow --login foo [--note ...]`, `... disable {--login foo | --account-id 42}`, `... list`. Both `allow` and `disable --login` resolve the login → numeric id via GitHub's `/users/{login}` first, then hit the SQL path. `disable --account-id` is the emergency fallback that skips the API entirely (works during GH outages / rate-limit, or when the operator already has the id from `installer list`). Tests: pure-SQL paths covered directly; the login-resolution paths covered via an in-process axum mock of `/users/{login}` in `sbgh-cli/tests/installer.rs` — including the rename-collision regression that pins "disable hits the resolved id, not whichever row happens to share the stale display login."
- **Tests added** (50 net new, total 191 → 241):
  - `webhook_processor.rs` unit tests (12 new for slice 3): InstallationHandler covering allowed/denied/disabled-allowlist creates, idempotent re-delivery, suspend/unsuspend roundtrip, suspend-for-unknown-install ignored, deleted removes row, deleted-for-unknown ignored, unknown action ignored_action, null payload error, bad typed shape error, unknown account type error. Plus 3 router tests: only-lists-registered-handlers, leaves-unregistered-event-types-in-received, duplicate-handler-panics.
  - `postgres_installation.rs` (8 new integration tests): lookup_allowed returns disabled rows, lookup returns None for unknown, FK rejects upsert without allowlist row, upsert updates on PK conflict (without clobbering suspended_at), set_suspended None-for-unknown, set_suspended roundtrips via clear, delete returns false for unknown, delete succeeds + second delete is Ok(false).
  - `processor_e2e.rs` (5 new + 1 modified): installation-created-allowed materialises install row, installation-created-unknown is denied (no row created), installation-suspend sets suspended_at, installation-deleted removes row, and the renamed `pipeline_leaves_unregistered_event_types_in_received` (now uses `push` since `installation` IS registered in slice 3).
  - `grants.rs` (5 new integration tests in sbgh-cli): orch can SELECT allowed_installer, orch CANNOT INSERT/UPDATE allowed_installer (security invariant), handler CANNOT touch allowed_installer, orch CRUD on github_installation works, handler CANNOT touch github_installation.
  - `installer.rs` (5 new integration tests in sbgh-cli): list returns rows sorted by login, disable flips is_enabled, disable lookup is case-insensitive, disable returns AccountNotFound for missing login, disable is idempotent.
- **Container start retry timeout bumped** from 3s → 30s in `test_support.rs::wait_for_port_exposed`, AND added `.config/nextest.toml` with a `testcontainers` test-group capped at `max-threads = 8`. With 246 testcontainers spinning up concurrently (vs 191 before slice 3), the previous 3s budget *and* 15s bump occasionally tripped the `PortNotExposed` race on a busy docker daemon. The combined fix (longer per-container timeout + cap on concurrent containers) is empirically stable across repeated runs.
- Verification: `just build` (clean release build), `just lint` (clean after one `just fix` rustfmt pass), `just test --summary` (246 tests, 0 failures, ~13s wall-clock).
- **Fixed mid-slice per Codex review**:
  - **High (Docker `installer allow` blocked by missing env)**: resolved transitively by the JWT fix below — once `installer` no longer needs App credentials, the existing compose `migrate` service config has everything it needs for any `sbgh-cli installer ...` invocation via `docker compose run --rm migrate ...`.
  - **High (App JWT auth on `/users/{login}` is wrong)**: dropped the App-JWT auth from `installer.rs::resolve_account` per the GitHub REST docs (the "Get a user" endpoint accepts user/installation tokens or fine-grained PATs, not App JWTs). Now uses an unauthenticated GET — 60/hr per IP is plenty for an operator one-shot. Removed `SBGH_GH_CLIENT_ID` / `SBGH_GH_PRIVATE_KEY_PATH` from the CLI env requirements; updated docs in `crates/sbgh-cli/src/main.rs` header + `docs/host-bringup.md`.
  - **Medium (login-keyed disable is racy under recycled/renamed logins)**: `disable_installer(pool, login)` now resolves `login → numeric account id` via the same `/users/{login}` path, then delegates to a new `disable_installer_by_account_id(pool, id)` that targets the numeric PK. Added integration tests with an in-process axum mock of `/users/{login}` proving: (a) the resolve-then-disable flow works end-to-end, (b) when two rows share a stale display login but only one has the currently-resolved id, ONLY the resolved row is disabled (`disable_installer_targets_resolved_id_even_after_login_collision`), and (c) disabling a resolved id not on the allowlist surfaces `NotOnAllowlist` rather than silently no-op.
  - **Medium (`github_webhook.github_installation_id` unpopulated FK conflicts with slice 3's hard install delete)**: added `ON DELETE SET NULL` to the FK in the slice 3 migration. Slice 3 has no writer for the column (none needed yet; populating requires the same lookup the install handler runs at create time), but the FK semantic is now pinned for slice 4+ writers. Regression test `delete_installation_nulls_resolved_fk_on_dependent_webhook_rows` simulates a populated column + verifies the install-row DELETE leaves the webhook row in place with the FK column NULL'd.
- Test count after Codex fixes: 246 (was 241 — net +5 from the additional installer e2e tests with the axum mock + the `ON DELETE SET NULL` regression test).

##### Slice 4: Repo Lineage + Installation Membership

**Status:**

- [x] Initial implementation completed
- [x] Integration coverage added (or N/A justified)
- [ ] Review in progress (with Codex)
- [ ] Complete (ready for next slice)

**Todo's:**

1. Add lineage columns to `github_repo`, `supported_repo_root`, and `github_installation_repo`.
2. Add admin/CLI seed for supported root repo.
3. Processor resolves repo lineage via GitHub API and upserts parent/root rows first.
4. Create installation membership only for supported root/fork repos.
5. Processor handles `installation_repositories.added` by creating/restoring accepted memberships.
6. Processor handles `installation_repositories.removed` by setting `github_installation_repo.revoked_at`.
7. Processor handles `installation.deleted` membership cleanup by setting `revoked_at` on all active memberships for the installation.
8. Mark unsupported lineage as `ignored_unsupported_lineage`.
9. Tests for canonical repo, direct fork, fork-of-fork, unresolved/half-populated lineage, unsupported repo, repo added, repo removed, and installation-wide membership revoke.

**Implementation notes/deviations:**

- **Migration file**: `migrations/20260527000004_slice4_repo_lineage.sql`. Creates three new tables (`github_repo`, `supported_repo_root`, `github_installation_repo`) with their `set_updated_at` triggers, adds `deleted_at` to `github_installation` (soft-delete column — see below), and adds the active-membership partial index for slice 5+ policy resolution queries.
- **`github_installation` soft-delete via new `deleted_at` column** (replaces slice 3's hard DELETE). With slice 4's `github_installation_repo` FK landing, the previous hard DELETE would fail RESTRICT on any active memberships. Switched to soft-delete: `installation.deleted` now sets `deleted_at = NOW()` AND bulk-revokes every active membership for the install IN ONE TRANSACTION. The install row is preserved so slice 8+ job FKs remain valid. `apply_roles` now grants orch `SELECT, INSERT, UPDATE` (no DELETE) on `github_installation` — the slice 3 grant test was updated to assert DELETE is REJECTED. Slice 3 tests that pinned the hard-delete behaviour (`delete_installation_returns_false_for_unknown`, `delete_installation_nulls_resolved_fk_on_dependent_webhook_rows`, `delete_installation_succeeds_when_no_dependents_exist`) were rewritten to the slice 4 outcome shape (`DeleteInstallationOutcome { install_found, memberships_revoked }`).
- **Per Codex's slice 3 M2 finding**: the `ON DELETE SET NULL` FK on `github_webhook.github_installation_id` is now dormant but kept. A new test (`delete_installation_soft_deletes_install_and_preserves_webhook_fk`) pins that the soft-delete path leaves the webhook FK intact (since the install row never goes anywhere). Slice 4+ writers can repopulate the column without worrying about the slice 3 DELETE semantic.
- **Store decomposition**: split the slice 4 data layer into a new `RepoStore` (owns `github_repo` + `supported_repo_root`) and an extended `InstallationStore` (slice 3's install methods + new `add_or_restore_membership`, `revoke_membership`, plus the rewritten `delete_installation`). Matches how slices 5-7 will keep adding per-domain stores.
- **Lineage resolution = ONE GitHub API call**. GitHub's `/repos/{owner}/{repo}` response includes `parent` (immediate fork parent) and `source` (ultimate non-fork root) for forks. The processor doesn't walk a chain; it gets the full lineage in one shot and `upsert_repo_lineage` does a transactional topological insert: source → parent (if distinct) → leaf. The leaf is the only row whose lineage columns are written; ancestors get identity-only upserts so a previously-walked ancestor's own lineage isn't clobbered.
- **`is_supported_lineage`** = single SQL query: `EXISTS` join from `github_repo` to `supported_repo_root` either directly (`s.github_repo_id = r.id`) OR via `fork_root_github_repo_id`, AND `is_enabled = TRUE`. Disabled supported_repo_root rows do NOT extend support to their forks — verified by `is_supported_lineage_rejects_disabled_root`.
- **New `GitHubApi::get_repository` method** on the trait, plus matching impl in `OctocrabClient` and `FakeGitHub` (with `set_repo_canonical` / `set_repo_fork` test helpers for staging canned lineage responses).
- **`InstallationRepositoriesHandler`** (new, slice 4):
  - For `added`: for each repo, fetch lineage from GH API, upsert via `RepoStore`, check support gate, add membership via `InstallationStore`. Per-repo decisions aggregate to a single webhook outcome: any accepted → `ProcessedInstallation`; else any unsupported → `IgnoredUnsupportedLineage`; else `IgnoredAction`.
  - For `removed`: per repo, `revoke_membership`. Any transition → `ProcessedInstallation`; else `IgnoredAction`.
  - Unknown actions → `IgnoredAction` (forward-compat).
  - Payload parse failures → `Error`; GH API or DB errors during the batch → `Retryable` so a network blip doesn't drop accepted repos.
  - Malformed `full_name` (no `/`) → log + skip that one repo, don't fail the batch.
- **InstallationRepositoriesEvent + InstallationRepository payload structs** added to `sbgh-core::github::webhook`. Both `repositories_added` and `repositories_removed` arrays are `#[serde(default)]` so we tolerate GitHub only sending the side relevant to the action.
- **CLI `sbgh-cli repo {allow,disable,list}`**: same shape as slice 3's `installer` subcommand. `allow --owner foo --name bar` resolves owner/name → id via unauthenticated `/repos/{owner}/{repo}`, then transactionally upserts both the `github_repo` identity row AND the `supported_repo_root` row. `disable` accepts mutually-exclusive `--owner --name` (resolves via API, rename-resilient) OR `--repo-id` (emergency / GH-outage path) — same dual-mode shape Codex's slice 3 M1 finding produced. `list` joins the operator table back to identity so output shows owner/name instead of just numeric ids.
- **Grants** for the three new tables (`sbgh-cli/src/lib.rs::apply_roles`):
  - `github_repo`: orch `SELECT, INSERT, UPDATE` (no DELETE — repo identity is forever).
  - `supported_repo_root`: orch `SELECT` only (operator-curated; compromised processor must not be able to add a new in-scope repo family).
  - `github_installation_repo`: orch `SELECT, INSERT, UPDATE` (no DELETE — membership history is permanent for slice 5+ policy + slice 8+ job FKs).
  - Handler: nothing on any of the three.
- **Tests added** (59 net new, total 246 → 305):
  - `webhook_processor.rs` unit tests (13 new): canonical-supported / fork-of-supported / fork-of-fork lineage walking, unsupported lineage skipping with identity caching, mixed accepted+rejected aggregation, idempotent re-delivery (granted_at preserved), removed-revokes-membership, removed-for-unknown ignored, GH API error → Retryable, malformed full_name skips one repo only, disabled supported_repo_root denial, unknown action ignored, null payload error.
  - `postgres_installation.rs` (5 new + 3 rewritten): bulk-revoke transactional, install-found vs not, redelivery idempotent (memberships_revoked=0 on second call), `add_or_restore_membership` preserves granted_at across revoke/re-add cycles, `revoke_membership` idempotent (no re-stamp).
  - `postgres_repo.rs` (10 new): all RepoStore paths — identity upsert doesn't clobber lineage, topological insert with ancestors, one-hop fork (parent == source) deduplicates, support gate direct + via-fork-root + disabled-rejection + unknown-repo.
  - `grants.rs` (5 new for slice 4 tables + 1 rewritten for the install delete-grant change).
  - `installer.rs` already covers the slice 3 patterns; slice 4 added `tests/repo.rs` (9 new) covering pure-SQL paths + the login-resolution paths via in-process axum mock of `/repos/{owner}/{repo}`.
  - `processor_e2e.rs` (5 new + 1 modified for slice 3 → slice 4 soft-delete): pipeline-add-creates-membership, pipeline-add-fork-walks-lineage, pipeline-add-unsupported-ignored-but-caches-identity, pipeline-removed-revokes-membership, pipeline-install-deleted-revokes-all-memberships-transactionally.
- Verification: `just build` (clean release build), `just lint` (clean after `just fix`), `just test --summary` (305 tests, 0 failures, ~17s wall-clock).
- **Fixed mid-slice per Codex review**:
  - **High (initial-install repo memberships not materialised)**: `InstallationEvent` now parses GitHub's `repositories` array (the "repos this install can access" list included on `installation.created`). `InstallationHandler::new` widened to take `Arc<dyn RepoStore>` + `Arc<dyn GitHubApi>`; `handle_created` now runs the same lineage+membership materialisation flow per repo after upserting the install row. Webhook-level outcome stays `ProcessedInstallation` regardless of per-repo lineage results (the install creation itself succeeded); per-repo decisions are recorded in `github_installation_repo` rows. Retryable failures during the lineage walk DO propagate so a network blip doesn't drop initial memberships. Without this fix, a fresh install had no `github_installation_repo` rows until a later `installation_repositories.added` event.
  - **Medium (stale `.added` resurrecting membership on soft-deleted install)**: `InstallationStore::add_or_restore_membership` signature changed to `Result<Option<GithubInstallationRepo>>`; the Postgres impl now takes `SELECT id FROM github_installation WHERE id=$1 AND deleted_at IS NULL FOR UPDATE` on the install row before the membership UPSERT. `delete_installation` was reordered to ALSO take `SELECT id FROM github_installation WHERE id=$1 FOR UPDATE` first (then mark `deleted_at`, then revoke memberships). Both paths therefore serialize on the install row's write lock — the actual TOCTOU close, not just a co-tx probe. The in-memory impl mirrors the contract via its single-Mutex serialization. `InstallationRepositoriesHandler` maps `Ok(None)` → `IgnoredUnknownInstallation` for the whole batch (subsequent repos would all hit the same gate).
    - **Note on the first attempt**: the original fix used a plain `SELECT EXISTS` probe in the same transaction as the UPSERT, which was insufficient under READ COMMITTED — Codex caught that a concurrent `delete_installation` could still interleave between the probe and the UPSERT. The fix needed actual row-level locking (`FOR UPDATE`) on the install row in both paths.
  - **Medium (payload `repo.id` vs GH-resolved id mismatch)**: extracted a shared `materialise_repo_membership` helper that does fetch + verify (`summary.id == payload_repo.id`) before lineage upsert. Mismatch → `RepoMembershipOutcome::IdMismatch`, logged with both ids, no membership created. Aggregation: if every repo in the batch was a mismatch → `IgnoredUnsupportedLineage`; partial mix → `ProcessedInstallation` from the accepted side.
  - **Low (stale `InstallationHandler` doc)**: docstring updated to describe the slice-4 soft-delete + bulk-revoke semantic.
- **Regression tests added** (23 net new, 305 → 328): `installation_created_with_supported_initial_repos_creates_memberships`, `installation_created_with_unsupported_initial_repos_still_processed`, `installation_created_with_id_mismatch_in_initial_repos_skips_membership`, `installation_created_with_no_repositories_still_processed_as_before` (slice 3 contract preserved); `repos_added_after_install_soft_deleted_is_ignored_unknown_installation`, `repos_added_with_payload_id_mismatch_skips_that_repo`, `repos_added_all_id_mismatches_aggregates_as_unsupported_lineage`, `add_or_restore_membership_returns_none_for_soft_deleted_install` + `_for_missing_install` (both in-memory and Postgres variants); plus e2e `pipeline_installation_created_with_repositories_materialises_initial_memberships` and `pipeline_repos_added_after_install_soft_deleted_is_ignored_unknown` against real testcontainers Postgres; plus `add_and_delete_race_never_leaves_orphan_active_membership` — a 50-iteration concurrent race test that proves the FOR UPDATE serialization works (negative-case verified: temporarily removing FOR UPDATE from the add path causes the test to fail consistently within a couple of iterations).
- Verification after Codex fixes: `just build` clean, `just lint` clean after `just fix`, `just test --summary` (328 tests, 0 failures, ~18s wall-clock).

##### Slice 5: Target/Source/Trigger Policies

**Status:**

- [x] Initial implementation completed
- [x] Integration coverage added (or N/A justified)
- [ ] Review in progress (with Codex)
- [ ] Complete (ready for next slice)

**Todo's:**

1. Add `target_repo_policy`, `source_repo_policy`, and `trigger_policy`.
2. Add admin/CLI commands to enable target/source/trigger policies.
3. Processor evaluates target/source policy for PR benchmark events, but logs only in Phase 1.
4. Processor evaluates branch/tag trigger policy, but does not create jobs yet.
5. Processor handles `installation.deleted` policy cleanup by setting `target_repo_policy.enabled=FALSE` for revoked memberships in the same transaction as membership revoke.
6. Tests for target denial, source denial, disabled membership, disabled policy, matching trigger policy, and uninstall disabling target policy without deleting rows.

**Implementation notes/deviations:**

- **Migration file**: `migrations/20260527000005_slice5_policies.sql`. Creates `target_repo_policy` (composite PK + FK to `github_installation_repo`), `source_repo_policy` (separate FKs to install + repo, no membership FK — sources can be arbitrary forks), and `trigger_policy` (bigserial PK + composite FK to `target_repo_policy`). Default `is_enabled` per the design: target/source default FALSE (operator opts in); trigger defaults TRUE (once added, presumed active). Partial index `trigger_policy_install_repo_kind_idx WHERE is_enabled` for the processor's hot path.
- **Slice 5 only USES `trigger_kind` values `branch_push` and `tag_created`.** `pr_comment` is the implicit /benchmark path (no trigger_policy row needed); `scheduled` and `manual` are reserved for post-slice-9.
- **`TriggerMatchSpec` typed enum** (sbgh-core/src/models.rs): `#[serde(tag = "kind")]` shape with `BranchPush { branch_name }` (exact match) and `TagCreated { tag_pattern }` (Rust regex). Validated at the CLI `policy trigger add` boundary so malformed specs are rejected before hitting the DB. The processor deserialises from JSONB at evaluation time and matches in code (handful of triggers per repo; SQL regex would be more expensive).
- **`PolicyStore` trait** (sbgh-core/src/db/policy.rs) with `PostgresPolicyStore` + `InMemoryPolicyStore` impls. Two distinct surfaces: processor read paths (`lookup_target_policy`, `lookup_source_policy`, `list_enabled_triggers`) + CLI write paths (`upsert_*`, `disable_*`, `add_trigger_policy`, `list_triggers`).
- **`delete_installation` extension**: the slice-4 transactional cleanup now ALSO bulk-disables every active row in all three policy tables for the install — `trigger_policy` first (FK chain order), then `target_repo_policy`, then `source_repo_policy`. All three statements are predicate-guarded on `is_enabled = TRUE` so re-delivery is idempotent. A new e2e test (`delete_installation_disables_all_policy_rows_in_same_transaction`) pins this behaviour AND verifies other installs' policies stay untouched (no scope-creep disable).
- **`GitHubApi::get_pull_request`** added — returns `PullRequestSummary { head, base }` with each side's `RepoRef { id, owner, name }` + sha + branch. Used by the slice-5 extension to `IssueCommentHandler`'s /benchmark path (need the base+head repo ids to evaluate target+source policies; the issue_comment payload only carries the PR's URL).
- **Three new event handlers**: `PullRequestHandler` (registers `pull_request`, evaluates target+source on `opened`/`reopened`/`synchronize`, ignores other actions), `PushHandler` (registers `push`, strips `refs/heads/` prefix + matches `branch_push` triggers), `CreateHandler` (registers `create`, only acts on `ref_type=tag`, regex-matches `tag_created` triggers).
- **`IssueCommentHandler` was widened** (was a unit struct in slice 2b): now takes `RepoStore`, `PolicyStore`, `GitHubApi`. The `/benchmark` PR branch now fetches the PR via GH API, caches base/head identities, evaluates target+source policies, and emits the corresponding outcome. Phase 1 "accepted policies" terminated as `IgnoredAction` at slice 5 land; the pre-slice-6 checkpoint flipped this to `WouldEnqueueJob` so the shadow accept is queryable. Slice 9 will flip it again to `EnqueuedJob` + create new-schema jobs.
- **Phase 1 logging convention for the new path**: accepted policy → `tracing::info!` with `{installation_id, pr_number, base_repo_id, head_repo_id}` + `WouldEnqueueJob` outcome (after the pre-slice-6 checkpoint; was `IgnoredAction` at slice 5 land). Denied → `DeniedTargetPolicy` / `DeniedSourcePolicy`. The legacy handler may still run benches that the inbox would have denied — the inbox row reflects "what the new pipeline would have decided," which is the slice-11 cutover signal.
- **CLI**: nested `sbgh-cli policy {target,source,trigger} {allow,disable,list}` (trigger uses `add`/`disable`/`list`). Numeric ids only — operator pulls them from `installer list` / `repo list` first. `policy trigger add --kind branch_push --match '<json>' [--args <bench-args>]` validates JSON against `TriggerMatchSpec` at the CLI boundary AND pre-checks that `target_repo_policy` exists for the (install, repo) pair, returning a friendlier error than a raw FK violation.
- **Grants** (in `sbgh-cli/src/lib.rs::apply_roles`): orch gets `SELECT, UPDATE` on all three policy tables (UPDATE needed for the install.deleted bulk-disable path). No INSERT (operator-curated; compromised processor can't add itself a new policy). No DELETE (policy history permanent). Handler nothing.
- **Slice 4 tests updated**: `pipeline_classifies_pr_benchmark_as_ignored_action_in_phase1` (slice 2b/4 test; renamed by the pre-slice-6 checkpoint to `..._as_would_enqueue_job_in_phase1`) now seeds policies + canned PR response since the /benchmark path now does real policy work. `pipeline_leaves_unregistered_event_types_in_received` switched from `push` (now registered) to `star` (handler-allowlist drops at wire; this test seeds directly via IngestStore as defense-in-depth coverage for "unregistered event types stay received").
- **Tests added** (61 net new, total 336 → 397):
  - `webhook_processor.rs` unit tests (~17 new): IssueCommentHandler /benchmark branch — both-enabled / target-denied / disabled-target / source-denied / GH-API-failure-retryable. PullRequestHandler — both-enabled / target-denied / source-denied / non-trigger-action-ignored / synchronize-re-evaluates / null-payload-error. PushHandler — matching/non-matching/disabled-trigger / non-branch-ref. CreateHandler — matching-pattern / non-matching / branch-ref-skipped / malformed-regex-skips-one-trigger-not-batch / null-payload.
  - `postgres_policy.rs` (8 new): membership-FK rejection, target upsert + disable round-trip with note preservation, source-doesn't-require-membership, trigger requires target FK, list_enabled_triggers respects kind filter + is_enabled, list_triggers includes disabled rows for CLI list.
  - `grants.rs` (4 new for slice 5 tables): orch SELECT+UPDATE works / INSERT+DELETE rejected for each of the three policy tables, handler rejected on all three.
  - `postgres_installation.rs` (1 new): `delete_installation_disables_all_policy_rows_in_same_transaction` pins the slice 5 extension to slice 4's bulk-cleanup transaction.
  - `processor_e2e.rs` (6 new): PR-with-policies-passes / PR-target-denied / PR-source-denied / PR-non-trigger-action / push-with-matching-trigger / push-no-match / create-tag-pattern-match / create-branch-skipped.
  - `cli/tests/policy.rs` (10 new): target+source allow/disable round-trips, NotFound on unknown pair, source-no-membership-needed, trigger-add validates match_spec JSON at CLI boundary, trigger-add rejects when target not yet allowed, trigger add+disable round-trip, NotFound on unknown trigger id, list filters by install/repo.
- Verification: `just build` clean, `just lint` clean after `just fix`, `just test --summary` (397 tests, 0 failures, ~23s wall-clock).
- **Fixed mid-slice per Codex review**:
  - **High #1 (target policy eval missed active-membership check)**: added `InstallationStore::is_membership_active(install_id, repo_id) -> bool` returning true iff `github_installation` is active (`deleted_at IS NULL AND suspended_at IS NULL`) AND the membership row exists with `revoked_at IS NULL`. `evaluate_pr_policies` now gates on this BEFORE consulting `lookup_target_policy`, so a stale-but-`is_enabled=TRUE` target row can't slip past after a revoke/suspend/soft-delete. Both `PullRequestHandler` and `IssueCommentHandler` (which share `evaluate_pr_policies`) take the new `Arc<dyn InstallationStore>` dep.
  - **High #1 cascade fix**: `InstallationRepositoriesHandler.handle_removed` now calls `policy_store.disable_target_and_triggers(install, repo)` BEFORE `install_store.revoke_membership(install, repo)`. New `PolicyStore::disable_target_and_triggers` method: one transaction, soft-disables `target_repo_policy` + every matching `trigger_policy` row. Idempotent (predicates filter `is_enabled = TRUE`). The constructor takes a 4th `policy_store` arg.
  - **High #2 (trigger eval bypassed parent target state)**: addressed via the same two-layer fix — `PushHandler` and `CreateHandler` both gain `Arc<dyn InstallationStore>` and check `is_membership_active` BEFORE `list_enabled_triggers`. Combined with the cascade in `handle_removed`, a disabled target's triggers also get disabled there.
  - **Low (stale InstallationHandler docstring)**: rewrote the `deleted` action doc to reflect that slice 5 added policy cleanup to the same transaction (it no longer says "will be soft-disabled once they ship").
- **Regression tests added** (18 net new, 397 → 415): `is_membership_active_returns_true_only_for_active_install_and_membership` (truth-table across 6 states: active/suspended/unsuspended/revoked/restored/soft-deleted), `is_membership_active_returns_false_for_unknown_pair`, `disable_target_and_triggers_cascades_in_single_transaction` (scope-doesn't-creep verified with a second install), `disable_target_and_triggers_is_idempotent`. Handler unit tests: `pr_with_enabled_target_but_revoked_membership_is_denied_target_policy`, `pr_with_enabled_target_but_suspended_install_is_denied_target_policy`, `pr_with_enabled_target_but_soft_deleted_install_is_denied_target_policy`, `repos_removed_cascades_to_disable_target_policy_and_triggers`, `push_with_matching_trigger_but_revoked_membership_is_ignored`, `push_with_matching_trigger_but_soft_deleted_install_is_ignored`, `create_tag_with_matching_trigger_but_revoked_membership_is_ignored`.
- Verification after Codex fixes: `just build` clean, `just lint` clean after `just fix`, `just test --summary` (415 tests, 0 failures, ~22s wall-clock).
- **Second-pass Codex review fix**: the first round's membership-gate covered revoked/suspended/deleted installs, but manual `sbgh-cli policy target disable` still left orphan triggers active (the cascade in `disable_target_and_triggers` only fired from `installation_repositories.removed`). Two complementary fixes:
  - **Runtime safety net**: `PostgresPolicyStore::list_enabled_triggers` (and the in-memory mirror) now joins `target_repo_policy` and requires both rows enabled. Even if any code path forgets to cascade, triggers can't fire on a disabled-parent state. This is the load-bearing fix.
  - **CLI cascade**: `sbgh-cli policy target disable` now runs the trigger-disable + target-disable as one transaction (mirrors `disable_target_and_triggers`), so `policy trigger list` reflects reality immediately rather than relying on the runtime gate to mask the stale state.
  - **Regression tests added** (8 net new, 415 → 423):
    - `list_enabled_triggers_excludes_triggers_whose_parent_target_is_disabled` (Postgres + the gate's recovery on re-enable),
    - `disable_target_policy_cascades_to_disable_matching_triggers` (CLI),
    - `push_with_matching_trigger_but_disabled_parent_target_is_ignored` (PushHandler runtime gate),
    - `push_with_matching_trigger_and_missing_parent_target_is_ignored` (PushHandler against a never-allowed target),
    - `create_tag_with_matching_trigger_but_disabled_parent_target_is_ignored` (CreateHandler).
- Verification after second-pass fixes: `just test --summary` (423 tests, 0 failures, ~23s wall-clock).

##### Pre-slice-6 Design Checkpoint

**Status:**

- [x] Items 1-3 (architectural) landed before slice 6 starts
- [x] Items 4-5 captured as inline todos in their target slices
- [x] Item 6 rationale recorded; no action

**Why this checkpoint exists:** Codex's roadmap meta-review (after slice 5) surfaced six items that could become expensive to retrofit once Phase 1 finishes. Items 1-3 are *architectural* and want to land before any new code in slice 6 commits to a shape we'd then have to reverse. Items 4-5 are *acknowledged-but-deferred* (real, but cheap to address inside their target slices). Item 6 is a deliberate accept-the-tradeoff so we don't re-litigate it later.

**Pre-slice-6 actions (must land first):**

1. **Role scope decision: per-installation, optionally repo-narrowed.** `github_user_role` will be scoped `(github_user_id, github_installation_id NOT NULL, github_repo_id NULLABLE, granted_role)`. This honours the "installation is the tenant boundary" principle ([roadmap.md:16](../docs/roadmap.md)) that every prior policy table already follows. `github_repo_id IS NULL` means install-wide; `IS NOT NULL` means repo-narrowed within that install. There is no cross-installation grant shape — granting one user the same role across N installs is N rows on purpose. Target schema already updated to reflect this; slice 6 migration must match.
    - **Status**: design captured in [target_schema.sql](../migrations/_design/target_schema.sql) — slice 6's migration must match.

2. **New outcome `would_enqueue_job`.** Added to `github_webhook_outcome` so Phase 1 shadow-accepted decisions (slice 5's `/benchmark` / push / tag-trigger accept paths) are queryable in DB rather than collapsed into `ignored_action`. Preserves the Phase 1 verifiability promise ([roadmap.md:38](../docs/roadmap.md)). Slice 5 handlers must be updated to emit it on the accept branch instead of `IgnoredAction`; slice 6 user-authz logging-only path uses the same outcome. Slice 9 will then change the same branches to emit `enqueued_job` once new jobs land. Target schema already updated.
    - **Implementation note**: `WebhookOutcome::terminal_status()` should map `would_enqueue_job` to `WebhookStatus::Processed` (same as `enqueued_job` / `processed_installation`) — a shadow-accept is a successful terminal outcome, not an ignored/denied one. Distinguishing accepted-but-shadow from accepted-and-enqueued lives in the outcome enum, not status.
    - **Status**: implemented.
      - Migration: `migrations/20260527000006_pre_slice6_would_enqueue.sql` (`ALTER TYPE github_webhook_outcome ADD VALUE 'would_enqueue_job';`).
      - `WebhookOutcome::WouldEnqueueJob` variant added; `terminal_status()` maps to `WebhookStatus::Processed`.
      - Four accept paths flipped: `IssueCommentHandler` `/benchmark`, `PullRequestHandler` opened/reopened/synchronize, `PushHandler` matching `branch_push`, `CreateHandler` matching `tag_created` — each emits `WouldEnqueueJob` instead of `IgnoredAction` on the accept branch.
      - Tests renamed + flipped: `*_is_ignored_action_in_phase1` → `*_is_would_enqueue_job_in_phase1` (4 unit, 3 e2e). New Postgres round-trip test `complete_round_trips_would_enqueue_job_outcome` pins the enum-value bind/read path. Slice 5 unit tests that previously asserted `IgnoredAction` on accept paths were latent-broken (passing because no parent `target_repo_policy` was seeded → runtime gate sent them through the no-match path) — fixed by seeding the parent target alongside the trigger.
      - Verification: `just lint` clean, `just test --summary` 424/424 (423 → 424; +1 from the round-trip test).

3. **Target schema refresh.** Done as part of this checkpoint:
    - `github_installation.deleted_at` added (slice 4 drift).
    - `github_webhook.github_installation_id` FK marked `ON DELETE SET NULL` (slice 3 drift; dormant under soft-delete but kept defensively).
    - `github_webhook_outcome` includes `would_enqueue_job` and `processed_installation` (slice 3+ drift).
    - `github_user_role` restructured per item 1.
    - **Status**: applied to [target_schema.sql](../migrations/_design/target_schema.sql).

**Inline todos to add to their target slices:**

1. **Slice 7 — shared PR materialization helper.** Slice 9 needs PR/job links the moment a `/benchmark` comment arrives. A comment can reference a PR whose `pull_request.opened` event predates the new pipeline. Slice 7 must expose a single "materialize PR from GitHub API" helper that both `PullRequestHandler` and `IssueCommentHandler` call, so the comment path doesn't depend on a prior PR event having been seen by the new processor. `GitHubApi::get_pull_request` already exists from slice 5; slice 7 just needs to make it the shared materialization primitive. Add as a todo to slice 7.

2. **Slice 7 or 8 — payload retention for terminal rows.** The "bounded inbox" principle from slice 2b is intact in spirit (the 2 MiB body cap in [main.rs:55](../crates/sbgh-handler/src/main.rs#L55) bounds per-row growth) but the deferred cleanup of payloads on terminal `ignored` / `denied` / `failed` rows has been outstanding since slice 2b. Land it as one of: a small SQL `UPDATE github_webhook SET payload = NULL WHERE status IN ('ignored','denied','failed') AND payload IS NOT NULL AND processed_at < NOW() - INTERVAL '24h'` job invoked by the processor sweep loop, OR a tiny `WebhookInbox::clear_terminal_payloads()` method called by the existing sweep. Preserve `last_error` on failed rows; preserve `payload_size_bytes` always. Add as a todo to slice 7 (cheap) or slice 8 (if the operator wants more observation time first).

**Decision recorded (no action):**

1. **`github_user_login_lower_uniq` + `github_repo_owner_name_lower_uniq` kept.** Codex flagged these as risky given the "GH numeric IDs are natural keys; display names are display-only" principle, since GH login reuse (after 90-day deletion) and rare repo owner+name reuse could 23505 a legitimate insert.

    **Accepted tradeoff:** keep both indexes. Rationale:
    - Login reuse risk is real but our writers all use `INSERT ... ON CONFLICT (id) DO UPDATE login = EXCLUDED.login`, so the stale row's display login is overwritten the next time we encounter the original numeric id. The remaining failure window — a webhook for the *new* user lands before the stale row is updated — surfaces as a loud 23505 → `error` outcome that's trivial to diagnose and fix manually.
    - Repo owner+name reuse requires the *same owner* to recreate a deleted repo with the same name, which has never been observed in practice for our target repos.
    - Removing the indexes loses the "we already cached this (owner, name) under a different numeric id" signal — a genuine data-quality red flag the index currently surfaces loudly.

    If either index ever does fire 23505 in production, that's the signal to revisit; the loud failure is by design.

##### Slice 6: Users + Roles

**Status:**

- [x] Initial implementation completed
- [x] Integration coverage added (or N/A justified)
- [ ] Review in progress (with Codex)
- [ ] Complete (ready for next slice)

**Todo's:**

1. Add `github_user` and `github_user_role`.
2. Add admin/CLI command to grant roles by GitHub numeric user id/login resolution.
3. Processor upserts sender/comment author users.
4. Processor evaluates `trigger_pr_benchmark` for `/benchmark`, but logs only in Phase 1.
5. Tests for authorized user, denied user, disabled/absent role, login rename display refresh.

**Implementation notes/deviations:**

- **Migration file**: `migrations/20260527000007_slice6_users_roles.sql`. Creates `github_user` (id PK, lazy upsert from any sender / PR author sighting; unique `lower(login)` index per the pre-slice-6 checkpoint accepted tradeoff) and `github_user_role` (bigserial PK + UNIQUE NULLS NOT DISTINCT on `(user, install, repo, role)`, plus a `(user, install, role)` prefix index for the processor's `has_role` hot path). Honors the pre-slice-6 role-scope decision: every grant is `(github_user_id, github_installation_id NOT NULL, github_repo_id NULLABLE, granted_role)` — install is the tenant boundary; repo is the optional finer narrowing; NULL repo = install-wide grant.
- **`user_role` enum** was already created in slice 0, but slice 6 is the first user of it. Three values shipped: `trigger_pr_benchmark` (the `/benchmark` authz gate — the slice 6 hot path), `admin` (per the post-slice-6 review M1 fix: implies every other role within the same scope, so an `admin` grant authorizes `/benchmark` too), and `view_results` (still unused in Phase 1; reserved for read-only UI access in a later slice).
- **Pre-slice-6 checkpoint deferral on `push.pusher`**: per the design Q&A, push.pusher / push.sender is NOT upserted in slice 6. The slice 6 goal is `trigger_pr_benchmark` evaluation, and push events are subject-gated (by `trigger_policy`), not user-gated. When a future feature needs the push pusher row materialised, it's a one-line addition in `PushHandler` and the table is lazy-upsert by design — no harm in skipping until the use case lands.
- **`User` payload struct widened**: `sbgh_core::github::webhook::User` grew from `{ login }` to `{ id, login, type }`. The new fields are required (not `#[serde(default)]`) — production GH payloads always carry them on `sender` and `pull_request.user`, and a default sentinel for `id` would be a footgun. All slice 1-5 test fixtures that constructed minimal `{"login": "..."}` users were updated to the full shape.
- **`PullRequestBody` gained `user`** (the PR author) for the same reason — slice 6's `PullRequestHandler` upserts the author so slice 7's `github_pull_request.author_github_user_id` FK target exists by the time PR materialisation runs. PR-author upsert is unconditional (runs even on policy-denied paths) so the user table doesn't sprout discontinuities at the cutover boundary.
- **`UserStore` trait** (sbgh-core/src/db/user.rs): `upsert_user`, `lookup_user`, `lookup_user_by_login`, `grant_role`, `revoke_role`, `list_roles`, `has_role`. After the post-slice-6 review fixes, the `grant_role` Postgres impl runs `INSERT ... ON CONFLICT DO NOTHING` first and, on conflict, runs an unconditional `UPDATE ... SET revoked_at = NULL` (covers both re-grants of revoked rows AND no-op re-grants of active rows). The `has_role` query is `WHERE (granted_role = $3 OR granted_role = 'admin') AND (github_repo_id IS NULL OR github_repo_id = $4) AND revoked_at IS NULL` — install-wide grants wildcard across repos; repo-scoped grants narrow; admin implies all roles within the scope; revoked grants don't authorize anything.
- **`IssueCommentHandler` extended** with `Arc<dyn UserStore>`. New `/benchmark` flow: parse → fetch PR → repo identity upserts → **upsert sender (before authz check, for audit)** → `has_role(sender_id, install, target_repo, trigger_pr_benchmark)` → unauthorized: `DeniedUnauthorized`; authorized + policy accept: `WouldEnqueueJob`. Unknown account_type string on sender → `Error` (defensive, same pattern as slice 3 InstallationHandler).
- **`PullRequestHandler` extended** with `Arc<dyn UserStore>` — upserts `pull_request.user` (the author) before policy eval. No authz check here (only `/benchmark` triggers the gate).
- **`PushHandler` / `CreateHandler` unchanged in slice 6**: push/tag triggers are subject-gated, not user-gated. Per the design Q&A, the principle is "different gates for different jobs" — only `/benchmark` requires a user role.
- **CLI**: `sbgh-cli user {grant,revoke,list}`. `grant --login foo --install N [--repo M] --role trigger_pr_benchmark` resolves login → id via the unauthenticated `/users/{login}` lookup (same path `installer allow` uses) and upserts the `github_user` row before recording the grant. `--user-id` is the emergency / GH-outage path. `revoke` matches on the EXACT `(user, install, repo, role)` quadruple — repo-narrowed revoke does NOT match an install-wide grant, by design. `list --users` lists every known `github_user` (independent of grants); `list [--install N]` lists role grants optionally filtered by install.
- **GH login → id resolver refactored** into shared `crates/sbgh-cli/src/gh_resolve.rs` so `installer.rs` and `user.rs` share one implementation. `installer.rs` had a private duplicate; consolidated to avoid drift. Each caller wraps `ResolveError` into its own typed error.
- **Grants** (in `sbgh-cli/src/lib.rs::apply_roles`): orch gets `SELECT, INSERT, UPDATE` on `github_user` (lazy upsert path; UPDATE refreshes login + user_type on PK conflict, no DELETE — user identity is forever). Orch gets `SELECT only` on `github_user_role` — a compromised processor must NOT be able to grant itself `trigger_pr_benchmark`. All role writes happen via the CLI running as DB owner. Handler nothing on either table.
- **`RoleArg` clap value-enum** in `main.rs` is a thin clap-facing copy of `UserRole` so `--role` gets `--help`-friendly choices without making sbgh-core depend on clap. Three variants; `From<RoleArg> for UserRole` is trivial.
- **Slice 5 test fixtures updated**: every `make_benchmark_handler` test now seeds `alice` (id=42) with a `trigger_pr_benchmark` grant on `(install=1, repo=10)`. `make_pr_handler` adds a user store dep but no pre-seeded grant (PR handler doesn't gate on roles). E2E `pipeline_classifies_pr_benchmark_as_would_enqueue_job_in_phase1` now seeds the user + role grant inline; new e2e `pipeline_benchmark_without_role_grant_is_denied_unauthorized` proves the unauthorized path including the audit-trail invariant (denied user is still upserted into `github_user`).
- **Tests added** (33 net new, total 424 → 457):
  - `postgres_user.rs` (10 new integration tests): upsert idempotency + login refresh on rename, case-insensitive login lookup, grant idempotency on exact quadruple, NULL-repo and specific-repo as distinct buckets, revoke exact-match only (install-wide and repo-scoped are NOT interchangeable), `has_role` install-wide wildcards across repos, `has_role` repo-scoped narrows, no cross-installation leak, no cross-role leak, `list_roles` filter.
  - `webhook_processor.rs` unit tests (5 new): `benchmark_without_role_grant_is_denied_unauthorized`, `benchmark_with_install_wide_grant_is_authorized`, `benchmark_with_grant_on_different_repo_is_denied_unauthorized`, `benchmark_unauthorized_still_upserts_user_for_audit_trail`, `benchmark_with_unknown_sender_account_type_is_error`. Plus `pr_opened_upserts_author_into_github_user` for the PullRequestHandler author-upsert path.
  - `processor_e2e.rs` (1 new + 1 modified): `pipeline_benchmark_without_role_grant_is_denied_unauthorized` against real Postgres + the modified happy-path test now seeds the user/role row inline.
  - `cli/tests/user.rs` (8 new): grant/revoke round-trip by user id, unknown-user pre-check error, GrantNotFound on unmatched revoke, list filters and `list_users`. HTTP-path: grant_role resolves login → upserts user → grants, AccountNotFound for unknown login, revoke targets resolved id even when stale rows share the display login.
  - `cli/tests/grants.rs` (3 new): orch can SELECT+INSERT+UPDATE on github_user but DELETE rejected; orch SELECT-only on github_user_role with INSERT/UPDATE/DELETE all rejected; handler rejected on both.
- **Verification**: `just build` (clean release build), `just lint` (clean after `just fix` + one `doc_lazy_continuation` fix on the IssueCommentHandler decision-table doc), `just test --summary` (457 tests, 0 failures, ~28s wall-clock).
- **Fixed mid-slice per Codex review**:
  - **Medium #1 (admin granted but never authorized anything)**: `has_role` now treats an `admin` grant as implying any role within the same scope. Without this, an install-wide `admin` grant did NOT authorize `/benchmark` — which made `admin` a lie (target schema documents it as "full control" and the CLI exposes it as grantable). Implemented in both `PostgresUserStore::has_role` (`granted_role = $3 OR granted_role = 'admin'`) and the in-memory mirror. Scope rules apply identically: install-wide admin matches any repo; repo-scoped admin matches only that repo.
  - **Medium #2 (revocation deleted rows, violating soft-disable-only principle)**: `github_user_role` gained a `revoked_at TIMESTAMPTZ` column. `revoke_role` now sets `revoked_at = NOW()` rather than DELETEing; `grant_role` clears `revoked_at` on the matching row when re-granting (preserves the original `granted_at`); `has_role` filters `revoked_at IS NULL`; `list_roles` returns BOTH active and revoked rows so the operator's audit trail (`sbgh-cli user list`) shows the full history. New partial index `github_user_role_active_idx ON (user, install, role) WHERE revoked_at IS NULL` keeps the processor's hot-path read against the active subset only. CLI `user list` renders the active/revoked status column.
  - **Migration contradiction removed**: the original slice 6 migration doc-comment claimed "soft-disable-only" but documented revoke as DELETE. Updated to describe the soft-revoke semantic accurately.
- **Regression tests added** (11 net new, 457 → 468):
  - `postgres_user.rs` (+8): `has_role_admin_grant_implies_trigger_pr_benchmark`, `has_role_repo_scoped_admin_only_implies_within_that_repo`, `has_role_admin_does_not_cross_installation_boundary`, `revoke_role_is_soft_and_preserves_audit_history` (id + granted_at survive revoke), `revoke_role_is_idempotent_for_already_revoked_row`, `grant_role_reactivates_revoked_row_preserving_audit`, `list_roles_includes_revoked_rows_for_audit`. The original `has_role_does_not_match_different_role` was renamed + rewritten to use `view_results` (which does NOT imply anything) since admin now intentionally DOES imply trigger_pr_benchmark.
  - `webhook_processor.rs` unit tests (+2): `benchmark_with_admin_grant_is_authorized_via_admin_implies` and `benchmark_with_revoked_grant_is_denied_unauthorized` — both pin the handler-level outcomes of the runtime gate fixes.
  - `cli/tests/user.rs` (modified): the rename-collision test now asserts BOTH rows survive (stale id=999 stays ACTIVE; resolved id=42 goes REVOKED) rather than only the active row remaining.
- **Verification after review fixes**: `just lint` clean, `just test --summary` (468 tests, 0 failures, ~30s wall-clock).
- **Second-pass Codex review fix**:
  - **Medium (repo-scoped grants not tied to install's repo set)**: `sbgh-cli user grant --install A --repo B ...` previously accepted any `(install, repo)` pair as long as both rows existed, even when repo B had no `github_installation_repo` membership for install A. The grant sat inert (the runtime membership gate in `IssueCommentHandler` denied any actual /benchmark attempt), but the footgun was: if repo B was later added to install A, the stale grant silently became active. Added an active-membership precheck in `grant_role_by_user_id`: repo-scoped grants now require `github_installation_repo (install, repo, revoked_at IS NULL)` to exist, surfacing `UserError::NoActiveMembership` otherwise. Install-wide grants (`--repo` omitted) skip the check by design — they apply to whichever repos the install does or will have access to. Mirrors how slice 5's `add_trigger_policy` pre-checks `target_repo_policy` existence.
  - **Low (stale roadmap notes)**: the `user_role` enum bullet still described `admin` as forward-compat/unused (it now authorizes everything in scope after the first-pass M1 fix), and the `UserStore` bullet described `has_role` as exact-role-only and omitted the `revoked_at IS NULL` filter. Both updated to reflect the actual current behavior.
- **Regression tests added** (3 net new, 468 → 471):
  - `cli/tests/user.rs`: `grant_role_by_user_id_rejects_repo_scoped_grant_without_membership`, `grant_role_by_user_id_rejects_repo_scoped_grant_for_revoked_membership`, `grant_role_install_wide_does_not_require_membership` (precheck deliberately skips when repo is None).
  - Helper rename in `cli/tests/user.rs`: `seed_install_repo` now also seeds an active membership row (the post-M1 precheck requires it for repo-scoped grants); a new `seed_install_only` helper supports the precheck-failure tests that need install-but-no-membership state.
- **Verification after second-pass fixes**: `just lint` clean, `just test --summary` (471 tests, 0 failures, ~29s wall-clock).
- **Third-pass Codex review fix**:
  - **Medium (repo-scoped grants survived membership remove / install delete)**: the second-pass M1 precheck enforced "grant time membership must exist" but the symmetric lifecycle cascade was missing — `installation_repositories.removed` revoked membership + disabled policies but left `github_user_role` rows active, and `installation.deleted` soft-deleted the install + revoked memberships + disabled policies but again left user roles active. A grant made while the repo was active would silently come back to life when the repo (or install) was re-added.
    - **Per-repo cascade**: new `UserStore::revoke_repo_scoped_grants(install, repo) -> Result<u64>` method, called by `InstallationRepositoriesHandler.handle_removed` BEFORE `revoke_membership` (same ordering rationale as the slice 5 policy cascade). Only repo-scoped grants are touched; install-wide grants survive because they apply to all repos in the install.
    - **Install-delete cascade**: `PostgresInstallationStore::delete_installation` SQL extended to include `UPDATE github_user_role SET revoked_at = NOW() WHERE github_installation_id = $1 AND revoked_at IS NULL` in the same transaction as the existing membership-revoke + policy-disable cleanup. Both install-wide and repo-scoped grants for the install are revoked. Idempotent for redelivery via the `revoked_at IS NULL` predicate.
    - `InstallationRepositoriesHandler::new` widened to take `Arc<dyn UserStore>` (now 5 args); main.rs + e2e + unit-test constructors updated. `handle_repos_removed` signature also gains the user_store param.
- **Regression tests added** (5 net new, 471 → 476):
  - `postgres_user.rs` (+2): `revoke_repo_scoped_grants_soft_revokes_only_matching_repo` (scope-doesn't-creep verified across install-wide + cross-install + cross-repo grants), `revoke_repo_scoped_grants_is_idempotent`.
  - `postgres_installation.rs` (+1): `delete_installation_soft_revokes_all_user_role_grants` (both install-wide + repo-scoped + cross-install behavior pinned in one test).
  - `webhook_processor.rs` unit (+1): `repos_removed_cascades_to_soft_revoke_repo_scoped_user_roles` (handler-level cascade with the install-wide grant proven to survive).
  - One additional unit test surfaced via the `make_repos_handler` widening (added user_store dep was exercised by existing fixture paths).
- **Verification after third-pass fixes**: `just lint` clean, `just test --summary` (476 tests, 0 failures, ~28s wall-clock).

##### Slice 7: Pull Request Subject Model

**Status:**

- [x] Initial implementation completed
- [x] Integration coverage added (or N/A justified)
- [ ] Review in progress (with Codex)
- [ ] Complete (ready for next slice)

**Todo's:**

1. Add `github_pull_request`.
2. Extend GitHub client to fetch PR details: target repo, source repo, author, head ref/sha.
3. Processor upserts PR rows for PR events.
4. Ensure source/target repos are upserted and lineage-resolved as needed.
5. Processor handles `pull_request.opened` / `reopened` by materializing or refreshing PR subject rows.
6. Processor handles `pull_request.edited` by refreshing mutable PR fields such as title.
7. Processor handles `pull_request.synchronize` by refreshing source/head metadata needed for future job ref resolution.
8. Decide whether `pull_request.closed` is a no-op or needs a future state column; current schema keeps PRs as historical subjects.
9. **Per pre-slice-6 checkpoint item:** expose a single `materialize_pr_from_github_api` helper that both `PullRequestHandler` and `IssueCommentHandler` call, so slice 9's `/benchmark` PR/job linking doesn't depend on having seen the PR's `opened` event through the new pipeline. `GitHubApi::get_pull_request` already exists from slice 5; this is wiring + a shared upsert path.
10. **Per pre-slice-6 checkpoint item:** add `WebhookInbox::clear_terminal_payloads()` (NULLs `payload` on rows where `status IN ('ignored','denied','failed') AND processed_at < NOW() - INTERVAL '24h'`), invoke it from the processor's existing sweep loop. Preserve `last_error` and `payload_size_bytes`. Defer to slice 8 if more observation time is wanted first.
11. Tests for internal PR, cross-fork PR, fork-of-fork source, PR title/author updates, and synchronize refresh.

**Implementation notes/deviations:**

- **Migration file**: `migrations/20260527000008_slice7_pull_request.sql`. Creates `github_pull_request` per the target schema: `(id bigserial, target_repo, source_repo, pr_number, title, author, closed_at, created_at, updated_at)` with UNIQUE on `(target_repo, pr_number)`. Includes the `set_updated_at` trigger.
- **Pre-slice-7 design QA decisions applied**:
  - **closed**: soft-close via `closed_at TIMESTAMPTZ`. Mirrors the lifecycle pattern from `github_installation_repo.revoked_at` and `github_user_role.revoked_at`. Reopen clears `closed_at`. Closed PRs are preserved (slice 8+ job FKs).
  - **synchronize**: refresh head metadata via upsert + re-run policy eval (source repo's policy may have changed since last sync).
  - **PR API surface**: extended `GitHubApi::get_pull_request` to return `PullRequestSummary { number, head, base, title, author }` so the shared materialisation helper can populate `github_pull_request` from one API call — needed for the `/benchmark` path on PRs whose `opened` event predates the new pipeline.
  - **edited**: refresh title via the materialise upsert; re-run policy eval ONLY when the payload's `changes.base` is present (the rare case where the operator actually changed the PR's base ref). Title/body/etc. edits absorb into the upsert and terminate as `IgnoredAction` — a typo fix MUST NOT generate a `WouldEnqueueJob` signal, otherwise slice 9 would turn it into a real "title edit starts benchmark" bug. (Original pre-Codex-review design was "always re-run policy eval"; corrected during slice 7 review.)
- **Shared materialiser**: `materialise_pull_request` in `webhook_processor.rs` is the single upsert path. Takes `&dyn RepoStore + UserStore + PullRequestStore` plus a tiny `PullRequestRepoInput` / `PullRequestAuthorInput` borrow shape. Upserts in FK order: base repo identity, head repo identity, author user, PR row. Both `PullRequestHandler` (data from payload) and `IssueCommentHandler` /benchmark (data from GH API response) call it.
- **PullRequestHandler dispatch** (post-Codex-review action-first ordering — repo fields are unwrapped only by the branches that actually need them, since GH may omit `pull_request.head.repo` for deleted-fork PRs):
  - ignored-by-default (labeled, unlabeled, assigned, …) → `IgnoredAction` with no repo access at all.
  - `closed` → only `base.repo` needed (key for `set_closed_at`). `set_closed_at(Some(NOW()))`, terminal `IgnoredAction`. closed for an unseen PR is a graceful no-op; if even `base.repo` is missing, defensively `IgnoredAction` rather than `Error`.
  - `opened` / `reopened` / `synchronize` → require both `base.repo` and `head.repo`; materialise PR + re-run policy eval. `reopened` also clears `closed_at` (idempotent if already None).
  - `edited` → require both repos; always materialise (title refresh); re-run policy eval ONLY when `changes.base` is present in the payload; otherwise terminate as `IgnoredAction` (title-only edits don't produce enqueue signals).
- **IssueCommentHandler /benchmark**: the slice 5 inline repo-identity upserts were replaced with a `materialise_pull_request` call. Slice 7's shared helper produces the same identity rows plus the author + PR-subject rows, so slice 9 can link a job to the PR by primary key.
- **`User` payload struct** grew a `title: String` field on `PullRequestBody`. All slice 1-6 test fixtures using PR payloads updated to include it.
- **`FakeGitHub::set_pull_request`** kept backward-compat (defaults to `title="test pr title"` + `author=(42, "alice", User)`); a new `set_pull_request_full` takes explicit `title` and `author` for tests that materialise PRs and need to override defaults — especially e2e tests where the issue_comment sender's login could collide with the default PR author on the `lower(login)` unique index.
- **`PullRequestStore` trait** (sbgh-core/src/db/pull_request.rs): `upsert_pull_request`, `lookup_pull_request`, `set_closed_at`. Postgres impl uses `INSERT ... ON CONFLICT (target_repo, pr_number) DO UPDATE SET title = EXCLUDED.title, updated_at = NOW()`. `closed_at` is NOT touched by upsert — only `set_closed_at` writes it; a late opened/edited event won't silently reopen a closed PR.
- **Grants** (in `sbgh-cli/src/lib.rs::apply_roles`): orch gets `SELECT, INSERT, UPDATE` on `github_pull_request` + USAGE on `github_pull_request_id_seq`. No DELETE — slice 8+ job FKs need PR rows to stick around, and closed PRs use soft-close. Handler nothing.
- **Slice 7 payload retention** (pre-slice-6 checkpoint item, landed in this slice): new `WebhookInbox::clear_terminal_payloads(retention)` method NULLs `payload` on `status IN ('ignored', 'denied', 'failed')` rows past the retention window. Invoked from the processor's existing sweep loop alongside `sweep_stuck_claims`. `payload_size_bytes` and `last_error` are preserved. `processed` rows are intentionally NOT cleared — slice 9+ may want the payload for job-context construction. Default retention 24h via `ProcessorConfig::payload_retention`. Wired into both Postgres + in-memory inbox impls.
- **Tests added** (~26 net new, total 476 → 502):
  - `postgres_pull_request.rs` (+8): upsert creates+refreshes title only, upsert never clears closed_at, `(target_repo, pr_number)` uniqueness, author + repo FK enforcement, `set_closed_at` toggle (set/re-set/clear) idempotency, lookup for unknown returns None, internal PR (target == source repo) works.
  - `webhook_processor.rs` unit tests (+6 at slice 7 land; the edited test was later split during the Codex review fix — see `pr_edited_title_only_*` and `pr_edited_with_base_changed_*` below): `pr_opened_materialises_pull_request_row`, `pr_closed_sets_closed_at_and_terminates_ignored_action`, `pr_reopened_clears_closed_at_and_re_runs_policy_eval`, `pr_synchronize_keeps_pr_row_present`, `pr_closed_for_unseen_pr_is_idempotent_no_op`, plus the edited test that was rewritten during review.
  - `postgres_webhook.rs` (+4): `clear_terminal_payloads_nulls_old_terminal_rows`, `clear_terminal_payloads_skips_in_flight_rows`, `clear_terminal_payloads_skips_processed_status_rows`, `clear_terminal_payloads_respects_retention_window`.
  - `grants.rs` (+2): orch can SELECT+INSERT+UPDATE on `github_pull_request` (DELETE rejected), handler rejected on `github_pull_request`.
  - `processor_e2e.rs` (modified): the `pipeline_classifies_pr_benchmark_as_would_enqueue_job_in_phase1` test now uses `set_pull_request_full` with an explicit author matching the issue_comment sender (avoids the `lower(login)` collision) AND asserts the materialised PR row's title.
  - Existing slice 5 test `pr_non_trigger_action_is_ignored_without_policy_lookup` was renamed/restricted to `pr_labeled_or_unlabeled_actions_are_ignored_without_side_effects` since `closed` and `edited` now have intentional side effects under slice 7.
- **Verification**: `just build` (clean release build), `just lint` (clean after `just fix` + one `#[allow(clippy::too_many_arguments)]` on `materialise_pull_request`), `just test --summary` (502 tests, 0 failures, ~31s wall-clock).
- **Fixed mid-slice per Codex review**:
  - **Medium (handler errored when GH omits `head.repo`)**: the original dispatch unwrapped both `base.repo` and `head.repo` BEFORE matching on action, so `closed` / `labeled` / `assigned` / etc. terminalized as `Error` for PRs whose source fork branch had been deleted (GH documents head.repo as optional in that case). Reordered the dispatch: action match first, then conditional repo unwraps. ignored-by-default actions (labeled, unlabeled, assigned, …) need no repo data; `closed` only needs `base.repo` (defensively returns `IgnoredAction` if even that's missing rather than `Error`); opened/reopened/synchronize/edited still require both and return `Error` if either is missing.
  - **Medium (title-only edits would trigger benchmarks at slice 9)**: `pull_request.edited` originally re-ran policy eval unconditionally and emitted `WouldEnqueueJob` for any edit, including title-only fixes. Once slice 9 flips `WouldEnqueueJob` to job creation, every typo edit would start a benchmark. Refined: `edited` always refreshes the title via the materialise upsert, but policy eval ONLY re-runs when `changes.base` is present in the payload (the case where the operator actually changed the PR's base ref, which can shift target repo identity). Title/body/etc. edits absorb into the upsert and terminate as `IgnoredAction`. Required a new `PullRequestChanges` payload type with `base: Option<serde_json::Value>` on `PullRequestEvent`.
- **Regression tests added** (6 net new, 502 → 508; the previous `pr_edited_refreshes_title_and_re_runs_policy_eval` was replaced):
  - `pr_edited_title_only_refreshes_title_but_terminates_ignored_action` (CRITICAL: pins the "title edit doesn't start benchmark" invariant)
  - `pr_edited_with_base_changed_re_runs_policy_eval` (defensive re-eval on base ref change)
  - `pr_closed_without_head_repo_still_sets_closed_at` (M1 fix: deleted-fork close path)
  - `pr_labeled_without_head_repo_terminates_ignored_action` (M1 fix: ignored-by-default actions need no repo data)
- **Verification after review fixes**: `just lint` clean, `just test --summary` (508 tests, 0 failures, ~32s wall-clock).

## Phase 2 — Cutover

**Status:**

- [ ] Initial implementation completed
- [ ] Integration coverage added (or N/A justified)
- [ ] Review in progress (with Codex)
- [ ] Complete (ready for next slice)

| Slice | What ships | Live impact |
| ----- | ---------- | ----------- |
| 8 | `job` + `job_event` + outcome companions + `github_webhook_job` / `github_user_job` / `github_pull_request_job` (tables only) | DB-only; no writers yet |
| 9 | Processor creates `job` rows from inbox (for `/benchmark` and policy-matched events) | **Double jobs**: legacy path creates old `jobs` row, processor creates new `job` row. Don't run new orchestrator yet — new jobs sit at `queued`. The accumulating new-side rows are cleaned up before slice 11 via a one-shot script (see below). |
| 10 | Orchestrator `JobStore` trait abstraction; both implementations available behind a runtime config | None — refactor only |
| 11 | **Flip orchestrator to claim from new `job` table; remove handler's dual-write to legacy `jobs`** | THIS is the cutover. Drain in-flight legacy `jobs` first, then run the cleanup script (below), then flip. |
| 12 | Remove legacy code paths, drop `jobs` table | Cleanup |

### Implementation

#### Phase 2 Todos

##### Pre-Phase-2 Design Checkpoint

**Status:**

- [x] Schema drift audit (Phase 1 outputs vs target_schema.sql)
- [x] Architectural decisions pinned before slice 8 commits to schema
- [x] Deployment + cutover plan implications captured

**Why this checkpoint exists:** Phase 1 (slices 0-7) shipped a lot of structural changes (`deleted_at` / `revoked_at` / `closed_at` lifecycle columns, admin-implies, `would_enqueue_job` outcome, PR materialisation, payload retention). Phase 2 (slices 8-12) commits to job + job_event + job_metric + job_result schema that slices 9-11 depend on; design-level questions are cheap to revisit now and expensive to revisit later. Mirrors the pre-slice-6 checkpoint pattern that surfaced role scope, would_enqueue_job, and target schema staleness in time to fix.

**Schema drift audit — clean.** `target_schema.sql` already incorporates everything slices 6-7 built: `github_installation.deleted_at`, `github_installation_repo.revoked_at`, `github_user_role.revoked_at` + admin-implies semantic, `github_pull_request.closed_at`, `would_enqueue_job` + `processed_installation` outcomes, partial indexes on active grants + active triggers. The slice 8 surface (`job`, `job_event`, `job_metric`, `job_result`, three `github_*_job` subject relations) is FK-consistent with the slice 6-7 tables. No refresh required this round.

**Decisions pinned (must apply to slice 8):**

1. **Phase 1 deployment deferred until slices 8-10 land.** Development continues local-only through slice 10. Slice 11 (the cutover) will be the first deploy. **Consequence**: cutover validation needs to be more thorough than the original "deploy → observe → cutover" plan implied. Slice 11 prep should include (a) a controlled `/benchmark` end-to-end against staging with the full stack; (b) replaying a representative set of saved GitHub webhook payloads through the inbox + processor to verify classification matches expectations; (c) explicit verification that the legacy `jobs` path can be paused without losing in-flight work. Flag added to slice 11's todo list.

2. **`job_status` enum gains a 'claimed' state.** Lifecycle becomes `queued → claimed → running → completed/failed/cancelled`. The orchestrator's claim path transitions queued → claimed (with a `claim_token` / `claimed_at`), then claimed → running when actually executing. Stuck-claim recovery targets `claimed` rows whose `claimed_at` exceeds the lease window and resets them back to `queued` (same shape as the inbox's stuck-claim sweep). **Consequence**: slice 8 migration includes `ALTER TYPE job_status ADD VALUE 'claimed';`. Target schema's enum comment updated. The Rust `JobStatus` enum gains a `Claimed` variant.
    - **Important shared-enum note** (per Codex review): `job_status` is a single Postgres type shared by BOTH the legacy `jobs.status` column ([20260521000001_init.sql](../migrations/20260521000001_init.sql)) AND the new `job.status` column. `ALTER TYPE` adds the value to the enum globally, not per-table — so legacy `jobs` rows could technically be set to `'claimed'`. The plan is that legacy code paths never write the new value, but slice 8 MUST add `JobStatus::Claimed` to the Rust enum BEFORE any production code reads any job row from either table. Otherwise a stray `'claimed'` row in legacy `jobs` (or in new `job` after slice 9's writers exist) would crash sqlx deserialisation.

3. **Slice 9 is forward-only.** New `job` rows only get created for webhook rows arriving AFTER slice 9 deploys. Accumulated `would_enqueue_job` / `received` rows from slices 5-8 stay in the inbox as audit history but do NOT retroactively produce jobs. **Consequence**: simpler slice 9 logic (no backfill code path), and the slice 11 cutover script's `TRUNCATE job CASCADE` is the explicit reset that brings the new pipeline to a clean state.

**`claim_token` + `claimed_at` invariants** (per Codex review; app-layer enforced, not DB CHECKs — consistent with the project's "DB enforces structural truths; app enforces workflow rules" principle from [roadmap.md:19](../docs/roadmap.md)):

- `status='queued'` ⟺ `claim_token IS NULL AND claimed_at IS NULL`
- `status='claimed'` ⟺ `claim_token IS NOT NULL AND claimed_at IS NOT NULL`
- `status IN ('running', 'completed', 'failed', 'cancelled')`: `claim_token` and `claimed_at` are PRESERVED (not cleared) as audit — they record which orchestrator instance picked up the job and when. Slice 10's claim → running transition does NOT touch these columns.
- Stuck-claim sweep: `WHERE status='claimed' AND claimed_at < NOW() - lease` → resets to `queued` AND clears both `claim_token` and `claimed_at` (matching the inbox sweep's behavior on `claim_token` reset).

Slice 10's `JobStore::claim_next` + `JobStore::mark_running` + stuck-claim sweep tests should pin each of these invariants explicitly.

**Inline todo updates flowing from these decisions:**

- Slice 8 todo: add `ALTER TYPE job_status ADD VALUE 'claimed' AFTER 'queued'` to the migration AND add `JobStatus::Claimed` to the Rust enum in the same slice (legacy `jobs.status` shares the enum; reading must not crash on the new value; positioning AFTER 'queued' matches the target schema and avoids the awkward retrofit later).
- Slice 8 todo: add the `claim_token` (`uuid`) and `claimed_at` (`timestamptz`) columns to `job` for the orchestrator claim handoff. Slice 8 tests pin the queued-state invariant (both NULL on insert).
- Slice 10 todo: claim path transitions `queued → claimed` (with token + claimed_at), then `claimed → running` (preserving claim_token + claimed_at as audit). Stuck-claim sweep handles `claimed → queued` recovery (clearing both columns). Tests pin each invariant.
- Slice 11 todo: add a "deployment validation" section to the prep checklist — controlled `/benchmark` against staging + saved-webhook replay before the cutover quiet window.

**target_schema.sql updates applied:** see the `user_role` / `job_status` enum sections + `job` table columns for the `claimed` state addition and the new claim handoff columns.

##### Slice 8: New Job Tables

**Status:**

- [x] Initial implementation completed
- [x] Integration coverage added (or N/A justified)
- [ ] Review in progress (with Codex)
- [ ] Complete (ready for next slice)

**Todo's:**

1. Add `job`, `github_pull_request_job`, `github_webhook_job`, `github_user_job`.
2. Add `job_event`, `job_result`, `job_metric`.
3. **Per pre-Phase-2 checkpoint item:** `ALTER TYPE job_status ADD VALUE 'claimed' AFTER 'queued'` (Postgres enum ordering is awkward to retrofit; matching the target-schema position now avoids the awkward gap later) AND add `JobStatus::Claimed` to the Rust enum in the same slice. Include `claim_token uuid` + `claimed_at timestamptz` columns on the `job` table to support the slice 10 claim handoff; tests pin the queued-state invariant (both NULL on insert).
4. Add indexes after dependent tables exist.
5. Add DB repositories for new job/event/result tables.
6. No production writers yet; integration tests only.

**Implementation notes/deviations:**

- **Migration file**: `migrations/20260529000001_slice8_jobs.sql`. `ALTER TYPE job_status ADD VALUE 'claimed' AFTER 'queued'` lands first (matches the target-schema enum position; PG 15+ allows the `ADD VALUE` inside the migration transaction since the new value is not referenced until subsequent statements). Creates `job` (with the slice 10 claim handoff columns `claim_token uuid` + `claimed_at timestamptz`), `job_event`, `job_metric`, `job_result`, and the three subject-relation tables (`github_pull_request_job`, `github_webhook_job`, `github_user_job`). Indexes per the target schema: `job_queued_idx` (partial on status='queued'), `job_repo_kind_idx`, `job_baseline_commit_idx`, `job_baseline_timeline_idx`, `github_pull_request_job_pr_idx`, `github_user_job_user_idx`, `job_event_job_occurred_at_idx`, `job_event_comment_idx`. `set_updated_at` trigger on `job`.
- **Naming convention** for the slice 8 → slice 12 window: the colliding type names use a `V2` marker (`JobV2`, `NewJobV2`, `JobV2Store`, `PostgresJobV2Store`, `InMemoryJobV2Store`). The non-colliding types ship with their final names (`JobEvent`, `JobMetric`, `JobResult`, `GithubPullRequestJob`, `GithubWebhookJob`, `GithubUserJob`, `NewJobEvent`). Slice 12 removes the legacy `Job` / `NewJob` / `JobStore` types and renames `JobV2` → `Job` etc.
- **`JobStatus` enum** gained `Claimed` between `Queued` and `Running` in the same slice as the migration (per pre-Phase-2 checkpoint M1 fix — legacy `jobs.status` shares the Postgres enum type, so the Rust enum MUST handle the new value before any production code reads any job row from either table). Legacy `jobs` code paths must NEVER write `Claimed`; only the new pipeline uses it.
- **`claim_token` + `claimed_at` invariants** (app-layer enforced per pre-Phase-2 checkpoint, NOT DB CHECKs): `status=Queued` ⇔ both NULL; `status=Claimed` ⇔ both Some; `status IN (Running, Completed, Failed, Cancelled)` → both PRESERVED as audit. Integration tests pin each invariant.
- **`JobV2Store` trait** (sbgh-core/src/db/job_v2.rs): `insert_job`, `lookup_job`, `claim_next_queued`, `mark_running`, `mark_terminal`, `sweep_stuck_claims`, `insert_event`, `record_metric`, `record_result`, `link_to_webhook`, `link_to_user`, `link_to_pull_request`. Postgres + InMemory impls mirror each other for the invariants. The Postgres `claim_next_queued` is a single statement (UPDATE wrapping `SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1`) so the row-pick + transition are atomic without an explicit transaction. `mark_running` / `mark_terminal` are conditional on `(id, claim_token)` so stale-claim writes (sweep raced ahead) become no-ops at the SQL layer.
- **`mark_terminal` requires `status='running'`** — a caller can't skip from claimed → terminal. Forces the claim → run → terminal lifecycle, preserving the execution-started signal. Pinned by `mark_terminal_rejects_transitions_skipping_running`.
- **No production writers in slice 8** — slice 9 wires the `JobV2Store` into the processor (creates the `job` + relation links + queued `job_event` in one transaction). Slice 8's integration tests exercise the data layer directly via `PostgresJobV2Store`.
- **Grants** (in `sbgh-cli/src/lib.rs::apply_roles`):
  - `job`: orch SELECT/INSERT/UPDATE, no DELETE (completed jobs are historical).
  - `job_event`: orch SELECT/INSERT only — append-only timeline; UPDATE rejected by grants.
  - `job_metric`, `job_result`: orch SELECT/INSERT — write-once outcome companions; UPDATE rejected.
  - `github_pull_request_job`, `github_webhook_job`, `github_user_job`: orch SELECT/INSERT — link tables; UPDATE rejected. Composite + UNIQUE constraints catch double-insert at the SQL layer.
  - `job_event_id_seq` USAGE granted to orch (BIGSERIAL backing). Other tables use UUID PKs or composite PKs (no sequences).
  - Handler: rejected on every slice 8 table.
- **Tests added** (~18 net new, total 508 → 526):
  - `postgres_job_v2.rs` (+13): queued-state invariant on insert; full lifecycle queued→claimed→running→completed with audit preservation; stale-token rejection on `mark_running`; `mark_terminal` requires running; stuck-claim sweep recovery + fresh-claim skip; concurrent claims pick disjoint rows; empty queue returns None; composite FK to `github_installation_repo` rejected for unknown pair; job_event round-trip; job_metric write-once PK collision; job_result with optional run_json; subject relation link tables round-trip + UNIQUE constraint enforcement.
  - `grants.rs` (+5): orch INSERT/UPDATE on job (no DELETE); orch INSERT on job_event but UPDATE/DELETE rejected (append-only); orch INSERT on job_metric/job_result but UPDATE rejected (write-once); orch INSERT on three subject-relation link tables but UPDATE rejected; handler rejected on every slice 8 table.
- **Verification**: `just build` clean, `just lint` clean after `just fix` + one `doc_lazy_continuation` rewrap, `just test --summary` (526 tests, 0 failures, ~34s wall-clock).
- **Fixed mid-slice per Codex review**:
  - **High (`mark_terminal` accepted any `JobStatus`)**: the original signature let a caller bug transition `running → queued` while preserving `claim_token`/`claimed_at` — directly violating the queued-state invariant from the pre-Phase-2 checkpoint. Introduced a narrow `TerminalJobStatus` enum (`Completed`/`Failed`/`Cancelled`) with `From` to `JobStatus`. The compiler now rejects any non-terminal transition at the call site; `mark_terminal` takes `TerminalJobStatus` and converts on the SQL bind.
  - **Medium (no atomic job-creation boundary)**: slice 9's docs promised "create job + webhook/user/PR links + queued event in one transaction" but the slice 8 trait only exposed the building blocks. Added `JobCreationRequest` + `NewPullRequestLink` payload types and a `create_job_with_links` trait method that runs the full insert sequence inside a single Postgres transaction. Any FK / UNIQUE failure rolls back the entire creation — no partial job rows. The InMemory mirror builds all rows locally first and then publishes job + links + queued event under a single mutex acquisition (final shape — see the second-pass review fix below; the initial first-pass implementation used staged-then-revert sub-method calls, which leaked partial visibility to concurrent readers and was reworked). Returns a `CreatedJob` bundle (job + webhook_link + optional user/PR links + queued_event) so callers don't need a follow-up lookup. Slice 9 production writers MUST use this path; the individual `insert_job` / `link_to_*` / `insert_event` methods stay on the trait for read-side flexibility and integration testing.
  - **Medium (no way to write resolved commit during claim)**: `mark_running` now takes `Option<ResolvedCommit>` (`{ hash, committed_at }`). For triggers that enqueue with an unresolved commit (branch tip at queue time), the orchestrator resolves during the claim phase and passes the resolved values; the status transition + commit metadata write land atomically under the same `claim_token` guard. `None` for triggers with a concrete commit at enqueue (push/tag). Postgres SQL uses `COALESCE` to leave existing columns untouched when `None`.
  - **Low (InMemory write-once / UNIQUE mismatch with Postgres)**: the in-memory `record_metric` / `record_result` silently overwrote on PK collision; `link_to_webhook` / `link_to_user` / `link_to_pull_request` silently appended duplicate links instead of mirroring the Postgres `UNIQUE (job_id)` / `PRIMARY KEY (job_id)` constraints. Updated all six paths to return `Err` on duplicate; matches the Postgres behavior so unit tests using the in-memory store can't mask a real production bug.
- **Regression tests added** (5 net new, 526 → 531):
  - `create_job_with_links_inserts_job_links_and_queued_event_atomically` — happy path, all five rows land.
  - `create_job_with_links_rolls_back_on_fk_violation` — FK failure leaves zero rows.
  - `create_job_with_links_optional_user_and_pr_links_are_skipped_when_none` — non-PR / no-responsible-user triggers.
  - `mark_running_with_resolved_commit_writes_metadata_atomically` — resolve-during-claim path.
  - `mark_running_without_resolved_commit_leaves_existing_metadata_untouched` — preset-commit path uses COALESCE.
- **Verification after review fixes**: `just lint` clean, `just test --summary` (531 tests, 0 failures, ~39s wall-clock).
- **Second-pass Codex review fix**:
  - **Medium (InMemory `create_job_with_links` released the mutex between sub-inserts)**: the first-pass fix delegated to the individual trait methods (`insert_job`, `link_to_webhook`, etc.), each of which took and released the mutex independently. A concurrent reader could intercept the partially-created state — see the job before its webhook link existed, or even `claim_next_queued` an orphaned row. Postgres hid all of that inside the transaction; the InMemory mirror leaked it. Rewrote the InMemory `create_job_with_links` to build all rows locally first, then acquire the mutex ONCE for the commit block. Nothing is visible to other observers until the commit returns. No revert path is needed because no mutation happens until the all-or-nothing commit.
  - **Regression tests added** (2 net new, 531 → 533):
    - `create_job_with_links_is_atomically_visible` — basic post-call invariant.
    - `concurrent_claim_never_observes_partial_create` — 50-iteration race between `create_job_with_links` and `claim_next_queued`. Each iteration that observes a claim verifies the corresponding webhook link is also committed. Would have failed against the first-pass impl (multi-mutex acquire) by intermittently seeing the orphaned job.
- **Verification after second-pass fix**: `just lint` clean, `just test --summary` (533 tests, 0 failures, ~37s wall-clock).
- **Third-pass Codex review fix**:
  - **Low (stale "staged-then-revert" wording in the first-pass review-fix bullet)**: rewrote to describe the actual final shape — "builds all rows locally first and then publishes job + links + queued event under a single mutex acquisition." Includes a parenthetical pointer that the staged-then-revert shape was the discarded first-pass and the second-pass review fix replaced it.
  - **Low (test strength on `concurrent_claim_never_observes_partial_create`)**: the original assertion checked `created.webhook_link.github_webhook_id == webhook_id` AFTER the create task finished — that's a post-call property and would have passed against the buggy multi-mutex impl too. The strengthened version checks `has_webhook_link_for_job(claimed.id)` INSIDE the claim task, right after the claim observed the row. That proves the link was visible AT CLAIM TIME, not just by post-task. Required a new test-only `InMemoryJobV2Store::has_webhook_link_for_job` accessor (consistent with the existing test-only accessors on other in-memory stores).
- **Verification after third-pass fix**: `just lint` clean, `just test --summary` (533 tests, 0 failures, ~38s wall-clock).

##### Slice 9: Processor Writes New Jobs

**Status:**

- [ ] Initial implementation completed
- [ ] Integration coverage added (or N/A justified)
- [ ] Review in progress (with Codex)
- [ ] Complete (ready for next slice)

**Todo's:**

1. Processor creates real `job` rows for `/benchmark` and trigger-policy events.
2. In the same transaction, insert webhook/job link, PR/job link, owner link, and queued event.
3. Do not write `job_result`, `job_metric`, or terminal job events in this slice; no new-schema execution happens yet.
4. Legacy handler still creates old `jobs`; new jobs accumulate unconsumed.
5. Add inspection queries/docs for comparing old jobs vs new job rows.
6. Before cutover, plan to `TRUNCATE job CASCADE`.

**Implementation notes/deviations:**

(Include any specific implementation notes, deviations, deferrals, findings important for future phases/slices, etc. If none, just write "None").

##### Slice 10: Orchestrator JobStore Refactor

**Status:**

- [ ] Initial implementation completed
- [ ] Integration coverage added (or N/A justified)
- [ ] Review in progress (with Codex)
- [ ] Complete (ready for next slice)

**Todo's:**

1. Split old and new queue implementations behind a runtime config.
2. Introduce a runnable job view for the new schema.
3. Keep production config on old `jobs`.
4. Update tests so libvirt/runner logic can run against either job source.
5. Expect this slice to grow: old `jobs` stores all execution context in columns, while new `job` assembles context across subject/relation/event tables.
6. **Per pre-Phase-2 checkpoint item:** new claim path uses the slice 8 `claim_token` + `claimed_at` columns. Lifecycle: queued → claimed (via FOR UPDATE SKIP LOCKED) → running (when execution actually starts) → terminal. Stuck-claim sweep resets `claimed` rows whose `claimed_at` exceeds the lease back to `queued` (same shape as the inbox sweep).
7. No behavior change in production.

**Implementation notes/deviations:**

(Include any specific implementation notes, deviations, deferrals, findings important for future phases/slices, etc. If none, just write "None").

##### Slice 11: Cutover

**Status:**

- [ ] Initial implementation completed
- [ ] Integration coverage added (or N/A justified)
- [ ] Review in progress (with Codex)
- [ ] Complete (ready for next slice)

**Todo's:**

1. **Pre-cutover validation (per pre-Phase-2 checkpoint item):** since Phase 1 was never deployed for observation, validation requires (a) a controlled `/benchmark` end-to-end against staging with the full stack running, (b) replaying a representative set of saved real GitHub webhook payloads through the inbox + processor and verifying outcomes match expectations, (c) explicit verification that the legacy `jobs` path can be paused/drained without losing in-flight work.
2. Quiet window: stop handler and orchestrator.
3. Drain or intentionally discard legacy `jobs`.
4. Run `TRUNCATE job CASCADE;` and likely `TRUNCATE github_webhook CASCADE;`.
5. Deploy handler inbox-only behavior.
6. Enable processor job creation and orchestrator new queue claiming.
7. Start services and run one controlled `/benchmark`.
8. Verify webhook, job, event, PR comment, result, and metric rows.
9. No formal rollback plan: if cutover fails during the quiet single-user window, keep services stopped or patch forward until the controlled `/benchmark` passes.

**Implementation notes/deviations:**

(Include any specific implementation notes, deviations, deferrals, findings important for future phases/slices, etc. If none, just write "None").

##### Slice 12: Legacy Removal

**Status:**

- [ ] Initial implementation completed
- [ ] Integration coverage added (or N/A justified)
- [ ] Review in progress (with Codex)
- [ ] Complete (ready for next slice)

**Todo's:**

1. Remove old `jobs` enqueue/claim/update code paths.
2. Remove old config allowlists after seed/admin replacements are proven.
3. Remove old grants on `jobs`.
4. Update docs/host bringup/troubleshooting.
5. Soak for 1-2 weeks of stable post-cutover operation before dropping `jobs`.
6. Drop or archive `jobs` once no historical dependency remains.

**Implementation notes/deviations:**

(Include any specific implementation notes, deviations, deferrals, findings important for future phases/slices, etc. If none, just write "None").

### Pre-slice-11 cleanup script

Run between slice 10 and slice 11, after legacy `jobs` has drained:

```sql
-- Clear all accumulated new-pipeline state for a clean cutover.
-- CASCADE handles: job_event, job_metric, job_result,
--                  github_webhook_job, github_user_job, github_pull_request_job
TRUNCATE job CASCADE;

-- Wipe accumulated inbox so post-cutover behaviour starts from a known-empty state.
TRUNCATE github_webhook CASCADE;
```

**Preserved** through cutover: `allowed_installer`, `github_installation`, `github_installation_repo`, `github_repo` (+ lineage), `supported_repo_root`, `target_repo_policy`, `source_repo_policy`, `trigger_policy`, `github_user`, `github_user_role`, `github_pull_request`. All the Phase 1 state that we want to keep using post-cutover.

## Risk notes

- **Slice 11 is the only real risk window.** Every other slice is additive and observable. Plan a maintenance window of "any evening" since user base = me.
- **Slice 10 (`JobStore` trait refactor) prevents codebase chaos in Phase 2.** Without it, new-schema and old-schema code paths sit side-by-side in the orchestrator binary and get tangled. Treat it as load-bearing infrastructure even though it ships nothing user-visible.
- **Slice 10 is the most scope-expansion-prone slice.** It has to bridge very different persistence shapes: old `jobs` has execution context in columns; new `job` assembles context from typed subject/relation/event tables.
- **Slice 5 / 6 processor logic for `/benchmark` events is "log only".** Don't accidentally have the processor short-circuit the legacy handler. The processor's `/benchmark` classification in Phase 1 is purely observational.
- **Dual-write atomicity in slice 1**: handler must insert into legacy `jobs` and `github_webhook` in the same DB transaction; if one fails, both must roll back, otherwise GitHub redelivery reaches an asymmetric state. The current `JobStore::enqueue` shape doesn't naturally compose with a second arbitrary write — slice 1 likely needs a new handler persistence boundary (e.g. an `IngestStore` that owns the dual-write transaction) rather than tacking a best-effort second write onto the existing path.

## Pre-flight before starting

- [ ] Capture current env allowlists (`SBGH_ALLOWED_USERS` / `SBGH_ALLOWED_REPOS`) and decide how each value maps into seed data across the new tables: GH account → `allowed_installer`; target repo lineage roots → `supported_repo_root`; per-installation target opt-ins → `target_repo_policy`; per-installation source trust → `source_repo_policy`; commenter logins → `github_user_role` (`trigger_pr_benchmark`)
- [ ] Capture current GH App credentials, sccache dir, etc. — anything the operator config holds today
- [ ] Confirm Postgres version is ≥ 15 (required for `NULLS NOT DISTINCT`)
- [ ] Set up a local staging Postgres for slice-0 schema dry-run before applying to Hetzner
