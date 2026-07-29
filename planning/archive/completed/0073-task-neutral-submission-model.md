# v27.1: Task-Neutral Submission Model Rename

Continuation of
[v26](0062-sandboxed-worker-execution.md). Rename the
persisted request/workflow aggregate and its Rust domain model from
benchmark-specific terminology to their existing task-neutral meaning, without
changing submission, scheduling, execution, reporting, or lifecycle semantics.

> **Status:** shipped — completed locally on 2026-07-28 and continued in
> [v27.2](0064-task-submission-kernel.md).
>
> This is a mechanical vocabulary and schema cutover. The existing creation
> paths, transactions, idempotency, worker pinning/affinity, lazy benchmark-run
> materialization, pull scheduler, fleet coordinator, and runtime behavior
> remain unchanged. Operator-owned HTTP, JSON, and CLI vocabulary changes as
> part of the same atomic cutover; no compatibility aliases are retained.

## Item

- **id:** `0073-task-neutral-submission-model`
- **status:** `shipped`
- **priority:** `high`
- **depends_on:** `0004-worker-fleet`, `0005-task-kind-platform`,
  `0019-block-validation-recipe`
- **unblocks:** `0064-task-submission-kernel`
- **source:** v27 submission-boundary design review (2026-07)

## Problem

The persistence model is task-neutral in behavior but benchmark-specific in
name. Benchmarking, build-only, and block validation all create
`benchmark_group`/`benchmark_spec` rows, and those names leak through domain
types, store ports, fleet attempts, report identity, tests, logs, internal APIs,
and operator interfaces. Building a task-neutral submission kernel on those
names would either preserve a false model or mix old and new vocabulary through
the kernel change.

Renaming while simultaneously changing creation transactions would make review
needlessly difficult: a regression could be caused by mechanical rewiring or
new submission semantics. v27.1 makes the semantics-preserving rename
independently reviewable; v27.2 then implements the kernel against the final
model.

## Existing Semantics to Preserve

```text
request/group row
  └── all existing specs + workflow steps persisted up front
        └── only the first runnable job is initially materialized
              └── pull scheduler claims an existing queued job
                    └── coordinator may materialize the next sequential job
```

- The aggregate is not itself claimable.
- A job remains one concrete schedulable unit.
- An attempt remains one fenced execution of a job.
- Benchmark repetitions/specs remain sequential and lazily materialized.
- Validation/build-only remain singleton shapes.
- Explicit recovery retains its current benchmark-comparison-only eligibility
  and rejection behavior; task-neutral lifecycle semantics remain deferred to
  `0065-job-lifecycle-controls`.
- Existing `worker_id` and `measurement_profile` semantics remain byte-for-byte
  unchanged even though v27.2 will later separate constraints from assignments.

## Rename Map

The migration and corresponding Rust rename cover the complete live universal
model:

| Current | Target |
| ------- | ------ |
| `benchmark_group` | `task_submission` |
| `benchmark_spec` | `task_spec` |
| `benchmark_workflow_step` | `task_workflow_step` |
| `benchmark_step_kind` | `task_step_kind` |
| `benchmark_group_id` | `task_submission_id` |
| `benchmark_spec_id` | `task_spec_id` |
| `benchmark_run_index` | `task_run_index` |
| `recovery_of_group_id` | `recovery_of_submission_id` |
| `BenchmarkGroup` | `TaskSubmission` |
| persisted `BenchmarkSpec` | `SubmissionSpec` |
| `BenchmarkWorkflowStep` | `SubmissionStep` |
| `BenchmarkStepKind` | `SubmissionStepKind` |
| universal `GroupRecovery` / `RecoverGroup` | `SubmissionRecovery` / `RecoverSubmission` |
| universal `recover_group` methods | `recover_submission` |
| `/api/fleet/groups/{id}/recover` | `/api/fleet/submissions/{id}/recover` |
| `fleet recover-group --group-id` | `fleet recover-submission --submission-id` |
| API/CLI `prior_group_id` | `prior_submission_id` |
| API/CLI `new_group_id` | `new_submission_id` |

Rename foreign keys, constraints, indexes, row mappings, query aliases, internal
store/fleet/report fields, test helpers, daemon HTTP routes and DTO fields, CLI
commands/flags/output, emitted log fields, and current architecture/operations
documentation together. Update `migrations/_design/target_schema.sql`.
Historical migration contents and completed archive records retain their
original terminology.

Genuinely benchmark-specific concepts retain benchmark names: comparison,
calibration, metrics/results, workload plans, and benchmark planner inputs.

## Behavior-Preservation Rules

- **DDL rename, not copy/backfill.** Use PostgreSQL `ALTER ... RENAME` in one
  transactional migration so table/column/type OIDs, rows, defaults, and
  relationships are preserved. Rename constraint/index identifiers explicitly.
- **No aggregate redesign.** Do not add submission digests, idempotency tables,
  producer provenance, required capabilities, or new lifecycle fields.
- **No placement redesign.** Keep the existing `worker_id`,
  `measurement_profile`, and `host_key` columns and their current read/write
  behavior. v27.2 owns their semantic cleanup.
- **No writer consolidation.** Existing linked/unlinked benchmark, prepared
  singleton, GitHub, Slack, trigger, admin, and cache-warm creation paths remain.
- **No planner movement.** Mutable-ref/default resolution happens at exactly the
  same point as before.
- **No scheduler/coordinator change.** SQL predicates, ordering, `SKIP LOCKED`,
  assignment/affinity writes, offers, attempts, fencing, cancellation, cleanup,
  and workflow advancement remain logically identical modulo renamed
  identifiers.
- **No wire/protocol change.** `sbgh-proto` payloads and worker messages are
  untouched.
- **Atomic operator-interface rename.** Rename universal daemon HTTP route
  segments, JSON fields, CLI commands/flags, and human-readable CLI labels to
  task-neutral vocabulary. Update every repository-owned caller, fixture,
  script, and current document in the same commit. Do not retain deprecated
  routes, fields, flags, aliases, or DTO adapters: there are no external
  consumers to protect.
- **Preserve real continuity contracts.** Artifact/store keys, GitHub check
  names and markers, Slack reporting identity/metadata, and metric names/labels
  remain unchanged. Their stability preserves stored artifact reachability,
  branch protection and historical checks, in-place Slack updates, alerts, and
  time-series continuity. Emitted log fields have no external consumers yet
  and change atomically to task-neutral vocabulary.
- **No compatibility schema.** Do not keep duplicate tables/columns or add
  database views. The application and migration deploy together.

## Phases

### Phase 1: Inventory and Equivalence Baseline

**Goal:** Define the exact mechanical surface and pin existing behavior before
renaming.

**Scope:**

- Inventory every active SQL/Rust/current-doc occurrence of universal
  group/spec/step/run terminology.
- Inventory every repository-owned HTTP/JSON/CLI producer and consumer,
  including recovery routes, response DTOs, scripts, examples, and operations
  runbooks.
- Classify genuinely benchmark-specific occurrences that must not be renamed.
- Capture representative pre-v27.1 rows for benchmark multi-spec/repetition,
  build-only, validation, worker attempts/events, recovery generations,
  artifacts, and GitHub/Slack reporting identity.
- Record query plans/results for scheduler selection, workflow advancement, and
  reporting joins that are sensitive to renamed indexes or aliases.

**Acceptance & Validation:**

- The rename manifest accounts for every live table, type, column, FK,
  constraint, index, Rust field/type, store method, query alias, and test helper.
- Baseline fixtures cover null/non-null placement, terminal history, and a
  partially completed sequential benchmark submission.
- No proposed change adds, removes, or reinterprets persisted state.

### Phase 2: Transactional Database Rename

**Goal:** Produce the task-neutral schema without transforming data or behavior.

**Scope:**

- Add one PostgreSQL migration containing only the approved table/type/column/
  constraint/index renames.
- Update the target schema and migration-aware tests.
- Verify old and new schemas have identical columns, types, nullability,
  defaults, checks, FK actions, index definitions, enum values, and row data
  after normalizing identifiers.

**Acceptance & Validation:**

- A representative pre-v27.1 database upgrades in one transaction.
- Failure rolls back to the complete old schema; no partially renamed state is
  visible.
- Row counts, UUIDs, timestamps, payloads, results, events, attempts, artifacts,
  generations, and relationships are identical before/after.
- Fresh-schema creation and incremental upgrade produce the same target schema.

### Phase 3: Rust and Operator Interface Rename

**Goal:** Compile the existing application against the renamed schema using
task-neutral domain vocabulary.

**Scope:**

- Rename universal core models, fields, store/fleet ports, Postgres mappings,
  daemon coordinator/report fields, and tests.
- Preserve benchmark-specific plan/result/helper names.
- Rename universal daemon routes/DTOs and CLI commands/flags/output; update all
  repository-owned consumers without compatibility aliases.
- Rename internal observability variables and emitted log fields while
  preserving metric names/labels.
- Update active reference and architecture documentation.

**Acceptance & Validation:**

- Existing producers still call the same number and shape of persistence
  transactions.
- Existing scheduler/coordinator SQL is structurally equivalent modulo renamed
  identifiers.
- Renamed API/CLI operations produce the same status codes, payload values, side
  effects, and errors modulo task-neutral route/field/label names.
- Old universal HTTP routes, JSON field names, CLI commands, and flags are
  absent rather than maintained as aliases.
- GitHub/Slack report markers, artifact paths, and visible content are
  byte-equivalent.
- Emitted log fields use task-neutral vocabulary; metric names/labels are
  unchanged.

### Phase 4: Ratchet and Independent Validation

**Goal:** Make the mechanical rename reviewable as a complete,
semantics-preserving commit.

**Scope:**

- Add a focused check rejecting universal legacy identifiers in production
  Rust, current target schema, and active architecture docs.
- Exclude historical migrations/archive, genuinely benchmark-specific concepts,
  and explicitly documented integration continuity adapters.
- Run full workspace validation and migration equivalence tests.

**Acceptance & Validation:**

- No active persistence/domain code exposes a benchmark-specific name for the
  universal submission aggregate.
- The check permits genuinely benchmark-only concepts and fails on a
  reintroduced universal legacy field/table/type.
- All pre-existing tests pass without changed semantic expectations; contract
  fixtures change only for the approved vocabulary cutover.
- The v27.1 commit can be reviewed and deployed independently of v27.2.

## Final Validation

- [x] `just build --no-sccache`
- [x] `just lint --no-sccache`
- [x] `just test --no-sccache --summary`
- [x] `git diff --check`
- [x] Fresh and upgrade PostgreSQL migration suites pass.
- [x] Before/after schema and data equivalence audit passes.
- [x] Existing benchmark/build-only/validation submission, scheduling, execution,
  recovery, and reporting regression suites pass.
- [x] Renamed API/CLI contract tests prove behavior equivalence, repository-owned
  caller cutover, and absence of compatibility aliases.
- [x] Package-DAG, unused-dependency, docs/registry, target-schema, and
  legacy-name checks pass.

## Validation Evidence

Local validation on 2026-07-28:

- The PostgreSQL upgrade test creates the v26 schema, seeds a representative
  submission/spec/workflow/job graph, applies v27.1, and verifies stable
  relation OIDs, identities, relationships, values, and complete removal of
  legacy live catalog names.
- Fresh databases and the complete PostgreSQL store suite pass against the
  renamed schema.
- API and CLI contract tests prove the new route, JSON fields, command, and
  flag work while their old counterparts are absent.
- The full workspace passes build, lint, Cargo Machete, Clippy, rustfmt,
  package-DAG, documentation/registry, sandbox-policy, and legacy-name checks.
- Nextest passes 866 tests with one environment-gated test skipped.

## Deferred / Non-Goals

- A task-submission application service or new producer port.
- New submission plan/command/receipt types.
- Aggregate request digests or idempotency.
- Moving GitHub/Slack provenance from jobs to submissions.
- Splitting requested constraints from scheduler assignment.
- Persisting required capabilities instead of deriving them.
- Removing `host_key` or old creation methods.
- Freezing refs/defaults earlier than today.
- Any scheduling, coordination, lifecycle, reporting, worker-protocol, or
  user-surface semantic change beyond the planned HTTP/JSON/CLI vocabulary
  cutover.

## Rollout and Rollback

Drain workers, stop daemon writers, verify a restorable PostgreSQL backup,
apply the rename migration, deploy the matching v27.1 daemon/CLI, run schema and
behavior smokes, then resume workers. Do not run pre-v27.1 code against the
renamed schema.

Rollback stops v27.1 writers/workers, restores the pre-migration backup,
deploys the prior binary/CLI, validates schema/version agreement, and resumes.
No down-migration or mixed-schema deployment is supported.

## Continuation

[v27.2](0064-task-submission-kernel.md) implements
`0064-task-submission-kernel` against this final task-neutral model.
