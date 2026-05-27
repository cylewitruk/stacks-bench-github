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
  - `issue_comment.created` on a PR with valid `/benchmark` → `ignored_action` in Phase 1; slice 9 changes this branch to `enqueued_job` + creates new `job` row. Legacy handler→`jobs` path continues to actually run the bench in the meantime.
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
  - `basic_issue_comment_pr_with_benchmark_is_ignored_action_in_phase1` (pins the Phase 1 behavior; slice 9 will change the assertion to `enqueued_job`)
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
- [ ] Review in progress (with Codex)
- [ ] Complete (ready for next slice)

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

- [ ] Initial implementation completed
- [ ] Integration coverage added (or N/A justified)
- [ ] Review in progress (with Codex)
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

(Include any specific implementation notes, deviations, deferrals, findings important for future phases/slices, etc. If none, just write "None").

##### Slice 4: Repo Lineage + Installation Membership

**Status:**

- [ ] Initial implementation completed
- [ ] Integration coverage added (or N/A justified)
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

(Include any specific implementation notes, deviations, deferrals, findings important for future phases/slices, etc. If none, just write "None").

##### Slice 5: Target/Source/Trigger Policies

**Status:**

- [ ] Initial implementation completed
- [ ] Integration coverage added (or N/A justified)
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

(Include any specific implementation notes, deviations, deferrals, findings important for future phases/slices, etc. If none, just write "None").

##### Slice 6: Users + Roles

**Status:**

- [ ] Initial implementation completed
- [ ] Integration coverage added (or N/A justified)
- [ ] Review in progress (with Codex)
- [ ] Complete (ready for next slice)

**Todo's:**

1. Add `github_user` and `github_user_role`.
2. Add admin/CLI command to grant roles by GitHub numeric user id/login resolution.
3. Processor upserts sender/comment author users.
4. Processor evaluates `trigger_pr_benchmark` for `/benchmark`, but logs only in Phase 1.
5. Tests for authorized user, denied user, disabled/absent role, login rename display refresh.

**Implementation notes/deviations:**

(Include any specific implementation notes, deviations, deferrals, findings important for future phases/slices, etc. If none, just write "None").

##### Slice 7: Pull Request Subject Model

**Status:**

- [ ] Initial implementation completed
- [ ] Integration coverage added (or N/A justified)
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
9. Tests for internal PR, cross-fork PR, fork-of-fork source, PR title/author updates, and synchronize refresh.

**Implementation notes/deviations:**

(Include any specific implementation notes, deviations, deferrals, findings important for future phases/slices, etc. If none, just write "None").

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

##### Slice 8: New Job Tables

**Status:**

- [ ] Initial implementation completed
- [ ] Integration coverage added (or N/A justified)
- [ ] Review in progress (with Codex)
- [ ] Complete (ready for next slice)

**Todo's:**

1. Add `job`, `github_pull_request_job`, `github_webhook_job`, `github_user_job`.
2. Add `job_event`, `job_result`, `job_metric`.
3. Add indexes after dependent tables exist.
4. Add DB repositories for new job/event/result tables.
5. No production writers yet; integration tests only.

**Implementation notes/deviations:**

(Include any specific implementation notes, deviations, deferrals, findings important for future phases/slices, etc. If none, just write "None").

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
6. No behavior change in production.

**Implementation notes/deviations:**

(Include any specific implementation notes, deviations, deferrals, findings important for future phases/slices, etc. If none, just write "None").

##### Slice 11: Cutover

**Status:**

- [ ] Initial implementation completed
- [ ] Integration coverage added (or N/A justified)
- [ ] Review in progress (with Codex)
- [ ] Complete (ready for next slice)

**Todo's:**

1. Quiet window: stop handler and orchestrator.
2. Drain or intentionally discard legacy `jobs`.
3. Run `TRUNCATE job CASCADE;` and likely `TRUNCATE github_webhook CASCADE;`.
4. Deploy handler inbox-only behavior.
5. Enable processor job creation and orchestrator new queue claiming.
6. Start services and run one controlled `/benchmark`.
7. Verify webhook, job, event, PR comment, result, and metric rows.
8. No formal rollback plan: if cutover fails during the quiet single-user window, keep services stopped or patch forward until the controlled `/benchmark` passes.

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
