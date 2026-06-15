# v14: Benchmark Group / Run Model

Item `0037-benchmark-group-run-model`: introduce the neutral
`BenchmarkGroup` / `BenchmarkSpec` / `BenchmarkRun` vocabulary without changing
runtime behavior.

> **Status:** shipped
>
> Shipped as v14. Existing jobs are back-filled and newly-created jobs become
> singleton `group -> spec -> run` rows. Repeated execution, multi-variant
> comparisons, and shared calibration remain follow-ups.

## Items

| Item | Role | Status |
| ---- | ---- | ------ |
| `0037-benchmark-group-run-model` | primary | shipped |

## Why

The daemon currently treats a benchmark request as one job -> one VM run -> one
result artifact. That is too flat for clean repetitions and release-to-release
comparisons, where a single user-facing request should own multiple isolated
executions and one shared result record.

## Scope

Introduce the group/run model as a behavior-preserving schema and store seam:

- **BenchmarkGroup** — the user-facing request, reporting surface, shared
  artifact identity, terminal summary, and host-pinned execution boundary. A
  future worker fleet schedules the group as an indivisible unit.
- **BenchmarkSpec** — one concrete variant in the group: workload + ref/build
  target. The schema supports multiple specs now, but current creation paths
  create one spec until `0039` deliberately lifts that cap.
- **BenchmarkRun** — one isolated VM/snapshot/process execution of a spec. In
  v14 this is the existing `job` row with group/spec/run columns, not a parallel
  lifecycle entity.
- **Workflow steps** — inert ordered step rows (`build`, `run`, with
  `calibrate` reserved) so `0041` can insert shared calibration later without
  redefining group/spec/run ownership.
- **Group artifact namespace** — define a group-scoped store-key helper while
  leaving today's job-scoped artifact keys unchanged.

Non-goals: no repeated execution, no comparison summary, no new LLM schema
fields, and no workflow executor rewrite.

## Phases

### Phase 1: Schema + Backfill

**Goal:** Existing rows become singleton benchmark groups/specs/runs.

**Scope:**

- Add `benchmark_group`, `benchmark_spec`, and `benchmark_workflow_step`.
- Add `benchmark_group_id`, `benchmark_spec_id`, and `benchmark_run_index` to
  `job`.
- Back-fill every existing job as `group -> spec -> run_index 0`.
- Keep `job` as the only claimable lifecycle row.

**Status:**

- [x] Core implementation
- [x] Unit/integration tests
- [x] Reviewed
- [x] Validated — test suite passed

**Acceptance & Validation:**

- [x] Existing jobs are queryable as group/spec/run singletons.
- [x] Build-only groups model only a build step; measured jobs model build +
  run.
- [x] Current job claiming/reporting behavior is unchanged.

**Tests:**

- `crates/sbgh-core/tests/postgres_benchmark_groups.rs`
- `just test --no-sccache`

### Phase 2: Creation Path Wiring

**Goal:** Every current job creation path creates singleton group/spec/run rows
atomically.

**Scope:**

- `insert_job`
- `create_job_with_links`
- `create_unlinked_job`
- In-memory store parity

**Status:**

- [x] Core implementation
- [x] Unit/integration tests
- [x] Reviewed
- [x] Validated — test suite passed

**Acceptance & Validation:**

- [x] Slack, GitHub PR, baseline, and build-only jobs still create one job.
- [x] Each returned job carries its group/spec/run identity.
- [x] Creation rolls back as one unit on Postgres failures.

**Tests:**

- `insert_job_creates_singleton_group_spec_and_run`
- `create_job_with_links_creates_singleton_group_spec_and_run`
- `create_unlinked_build_only_job_creates_build_only_singleton_group`

### Phase 3: Artifact Namespace Seam

**Goal:** Reserve group-scoped artifact addressing without moving artifacts.

**Scope:**

- Add `group_artifact_key(group_prefix, relative)`.
- Keep current `<job_id>/<relative>` artifacts unchanged.

**Status:**

- [x] Core implementation
- [x] Unit tests
- [x] Reviewed
- [x] Validated — test suite passed

**Acceptance & Validation:**

- [x] Group-scoped key shape is defined.
- [x] Existing job-scoped artifact key behavior remains unchanged.

**Tests:**

- `group_artifact_key_is_group_prefix_slash_relative`

## Final Validation

- [x] `just build --no-sccache`
- [x] `just lint --no-sccache`
- [x] `just test --no-sccache` — 875 passed, 1 skipped
- [x] Dry-run the migration against a copy of the Hetzner DB before production
  deploy, because the suite validates new-row creation paths but cannot exercise
  the one-shot pre-existing-job backfill.

### Hetzner Dry-Run Checklist

Run against a snapshot/copy of the production database after applying the v14
migration:

```sql
SELECT count(*) AS ungrouped_jobs
  FROM job
 WHERE benchmark_group_id IS NULL
    OR benchmark_spec_id IS NULL
    OR benchmark_run_index IS NULL;

SELECT
    (SELECT count(*) FROM job) AS jobs,
    (SELECT count(*) FROM benchmark_group) AS groups,
    (SELECT count(*) FROM benchmark_spec) AS specs;

SELECT
    (SELECT count(*) FROM benchmark_workflow_step WHERE step_kind = 'build') AS build_steps,
    (SELECT count(*) FROM job) AS jobs;

SELECT
    (SELECT count(*) FROM benchmark_workflow_step WHERE step_kind = 'run') AS run_steps,
    (SELECT count(*) FROM job WHERE task_kind <> 'build_only') AS measured_jobs;
```

Expected: `ungrouped_jobs = 0`; `jobs = groups = specs`; `build_steps = jobs`;
and `run_steps = measured_jobs`.

Hetzner restore-check result (2026-06-15): backup restored cleanly; v14 migration
dry-ran against 68 pre-existing jobs; counts were `jobs=68`, `groups=68`,
`specs=68`, `build_steps=68`, `run_steps=46`, `measured=46`; all null/orphan
checks passed.

## Follow-Ups

- `0038-isolated-benchmark-repetitions`
- `0039-multi-variant-benchmark-comparisons`
- `0041-shared-benchmark-calibration`
