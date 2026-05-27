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

- [ ] Initial implementation completed
- [ ] Review in progress (with Codex)
- [ ] Complete (ready for next slice)

**Todo's:**

1. Add `github_webhook_status`, `github_webhook_outcome`, and `github_webhook`.
2. Grant handler insert-only access to inbox columns.
3. Introduce an `IngestStore`-style boundary that can insert legacy `jobs` + `github_webhook` in one transaction.
4. Handler verifies HMAC, filters unsupported event types, inserts inbox row, and still enqueues legacy jobs.
5. Keep unsupported event types log-only, no DB row.
6. Add tests for duplicate delivery, rollback atomicity, invalid signature no-row, unsupported event no-row.

**Implementation notes/deviations:**

(Include any specific implementation notes, deviations, deferrals, findings important for future phases/slices, etc. If none, just write "None").

##### Slice 2a: Processor Scaffold

**Status:**

- [ ] Initial implementation completed
- [ ] Review in progress (with Codex)
- [ ] Complete (ready for next slice)

**Todo's:**

1. Add orchestrator inbox claim loop with `FOR UPDATE SKIP LOCKED`.
2. Implement `received` / `processing` / `retryable_error` / `failed` transitions.
3. Implement attempts/backoff and stuck-claim recovery.
4. Keep classification minimal; no domain-specific effects yet.
5. Add integration tests for concurrent claims, retry backoff, exhausted attempts, stale claim recovery.

**Implementation notes/deviations:**

(Include any specific implementation notes, deviations, deferrals, findings important for future phases/slices, etc. If none, just write "None").

##### Slice 2b: Basic Inbox Classification

**Status:**

- [ ] Initial implementation completed
- [ ] Review in progress (with Codex)
- [ ] Complete (ready for next slice)

**Todo's:**

1. Parse known event envelopes from inbox payload.
2. Classify ignored actions and no-command issue comments.
3. Mark malformed/transient API failures as `retryable_error` or `error` appropriately.
4. Clear payload only for terminal ignored/denied/failed rows where allowed.
5. Verify outcomes in DB without affecting legacy `/benchmark`.

**Implementation notes/deviations:**

(Include any specific implementation notes, deviations, deferrals, findings important for future phases/slices, etc. If none, just write "None").

##### Slice 3: Allowed Installer

**Status:**

- [ ] Initial implementation completed
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
