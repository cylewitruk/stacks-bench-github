# 0005: Job-model decomposition + task-kind platform

- **id:** `0005-task-kind-platform`
- **status:** `shipped`
- **date:** 2026-06-12
- **iteration:** v10 (`v10-job-model-decomposition`)
- **source:** roadmap-v6 (three-layer model) + the **v10 realization** (2026-06,
  from the `0025`/`0031` warming pivot)
- **unblocks:** `0031-reusable-build-jobs`, `0019-block-validation-recipe`

Decomposed the `job` model into orthogonal axes — the old `JobKind` / `TriggerKind`
each conflated several concepts — and proved the model with a new run-shape
(build-only), so warming (`0031`) and block validation (`0019`) become thin
consumers rather than engine changes.

## What shipped

- **Four PG-enum axes on `job`** (`#[derive(sqlx::Type)]`): `source` (who) ·
  `intent` (why / result-role) · `task_kind` (run-shape → recipe dispatch) ·
  `build_target` (which binary). A 5th axis, `report`, is a **derived** Rust
  surface set (not stored). The legacy `trigger_kind` / `job_kind` columns were
  **dropped**; `TriggerKind` survives only on `trigger_policy` (event matcher) +
  `QueuedEventDetail` provenance.
- **Expand-migrate-contract** across three migrations (`…001` add columns +
  backfill → `…002` straggler backfill → `…003` contract: drop old columns,
  `SET NOT NULL`, reindex the baseline lookups on `intent`). `JobAxes::from_legacy`
  is the lossless backfill map, now test-only (creation is axes-native).
- **Creation / dedup / baseline / reporting / provenance** all rewired to the
  axes; the three live flows (PR-comment adhoc, branch-push baseline, Slack adhoc)
  run byte-equivalent through them.
- **Build-only** (the proof): `task_kind=build_only` → `BuildOnlyRecipe`
  (`TaskSpec.build_only`) provisions → builds → publishes → **stops** (no bench
  VM), routes to `ProgressTarget::Silent` → `NoopReportSurface` (no GitHub/Slack),
  and **fails closed** unless the artifact is actually cached (`reused ||
  published`; benchmark runs keep caching best-effort). Dispatch keys on
  `(task_kind, build_target)`; unsupported pairs → `UnsupportedRecipe` (fails
  fast, no silent mis-route).

## Validation

- 815 tests green, lint + build clean; Codex-signed-off per phase (Phases 0–3).
- **On-host smoke (2026-06-12):** v9→v10 upgrade is daemon-side + automatic
  (migrate-at-startup, single instance). The contract migration applied cleanly;
  the daemon claimed + decoded a real job (non-null axis decode on live rows),
  **reused a cached binary**, ran the bench, and completed + reported normally.

## Deviations / deferred

- **`report` stays derived** (a Rust `ReportSurfaceSet`), never a stored scalar
  enum — a stored shape would be a one-to-many `job_report_surface` table, not
  worth it while surfaces derive cleanly from `(source, intent, config)`.
- **Build-only *success* path is host-validation, deferred to `0031`** — a
  freshly-built binary actually landing in the real cache needs the live
  build/fingerprint path; the fail-closed direction is unit-proven. No build-only
  jobs are enqueued until `0031`'s warming trigger.
- **`block_validation` → `UnsupportedRecipe`** until `0019`; **`stacks_inspect`
  builds** unbuilt until then. The `sbgh→sgh` crate split (Open question 1) and
  the recipe registry are separable later refactors.

## Durable decisions (ADR candidates)

- **PG enums over TEXT + registry** (reverses the original `0005` call): a new
  value is an additive `ALTER TYPE … ADD VALUE`; adding a kind always touches code
  anyway, and this project **rolls forward** — so there's no clean rollback past
  the contract migration (snapshot the DB before upgrading).
- **`intent` absorbs `JobKind`**; `task_kind` is the run-shape; the two are
  correlated but distinct axes.
- **Dispatch fails closed on `(task_kind, build_target)`** — an unknown pair never
  silently runs the stacks-bench path.
- **Build-only fail-closed contract:** for a job whose purpose *is* the artifact,
  a publish/cache miss is a failure (unlike opportunistic benchmark caching).
