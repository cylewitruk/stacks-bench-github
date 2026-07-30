# v28: Task-Aware Reporting

Successor to
[v27.2](0064-task-submission-kernel.md). Complete the
task-neutral request-to-result path by projecting benchmark, build-only, and
block-validation submissions through one aggregate reporting lifecycle with
task-specific result views and renderers.

> **Status:** shipped — continued in
> [v29](0074-protobuf-fleet-protocol.md).
>
> v27.2 made submission identity, provenance, and persistence task-neutral.
> v28 preserves the durable fleet event path and makes its read/projection side
> task-aware. It adds no producer command or lifecycle mutation.

## Item

- **id:** `0066-task-aware-reporting`
- **status:** `shipped`
- **priority:** `high`
- **depends_on:** `0005-task-kind-platform`, `0017-generic-phase-events`,
  `0019-block-validation-recipe`, `0064-task-submission-kernel`
- **relates_to:** `0072-pre-attempt-terminal-projection`,
  `0028-results-summary-restructure`, `0065-job-lifecycle-controls`
- **unblocks:** `0067-github-block-validation-submission`,
  `0071-github-job-lifecycle-controls`, `0068-watched-ref-task-actions`,
  `0078-slack-task-submission`, `0070-slack-block-validation-controls`
- **source:** block-validation user-story audit and post-v27 sequencing review
  (2026-07)

## Shipped

Implemented one typed, provider-neutral submission report snapshot for
benchmark, build-only, and block-validation tasks. GitHub check/comment
identity is submission-owned, with conservative historical adoption and
fail-loud migration conflicts; Slack retains its submission-owned canonical
message. GitHub and Slack terminal renderers consume the same durable aggregate
snapshot exposed by the API, dispatch task detail exhaustively, avoid
benchmark-only reads for validation, and bound/escape every guest-controlled
detail field.

Added the authenticated `GET /api/submissions/{id}/report` contract,
submission-aware multi-run terminal fencing, task-aware queue/progress/failure
wording, migration/store/API/renderer regression coverage, and a reporting
boundary lint ratchet. Local validation completed with workspace build/lint,
895 passing tests (one environment skip), and `git diff --check`. The
restored-production migration rehearsal and real benchmark/validation restart
canaries remain rollout gates because they require the deployment database,
GitHub App, Slack workspace, and fleet hosts.

## Problem

Fleet execution and durable worker events already support benchmark, build-only,
and block-validation jobs, but report projection remains benchmark-shaped:

- every GitHub Check Run uses the stable name `stacks-bench`;
- queued, running, failure, cancellation, and retry text often says
  “benchmark” regardless of `task_kind`;
- GitHub reconciliation uses a child job as external identity even though
  submission is now the durable request/report aggregate;
- Slack completion unconditionally reads benchmark metrics and profiler
  artifacts; and
- block-validation results are typed on the worker protocol and in persistence,
  but reporting reconstructs a partial view from untyped terminal JSON and no
  typed result API exposes the complete verdict.

This makes a GitHub or Slack validation producer unsafe to expose: correct
execution can still produce misleading checks, wrong retry guidance, duplicate
surfaces across retries/repeats, or benchmark-only result queries.

## Outcome

One submission-scoped projection folds durable job/attempt events into a
surface-neutral snapshot. Shared lifecycle state is rendered through
task-specific detail selected exhaustively by `task_kind`:

```text
durable submission plan + jobs + accepted fleet events + typed results
                              |
                              v
                    SubmissionReportView
                    - report identity
                    - source/commit
                    - aggregate lifecycle
                    - phase/progress
                    - task detail
                              |
                  +-----------+-----------+
                  |                       |
                  v                       v
         GitHub check/comment       Slack snapshot
         renderer + publisher       renderer + publisher
```

Benchmark reporting remains behavior-compatible. Block validation receives its
own stable check, lifecycle vocabulary, typed verdict, bounded invalid-block
detail, and artifact links. Build-only remains silent externally unless a
future policy explicitly requests a surface, but it participates in the typed
operator/API view.

## Design Rules

- **Projection is shared; task detail is closed and typed.** Do not fork the
  event consumer, replay cursor, surface publisher, or terminal state machine.
  Use an exhaustive task-result enum for benchmark, build-only, and block
  validation.
- **Submission is the report aggregate.** Child job and attempt IDs remain
  visible lineage, but the canonical comment, check, or Slack message belongs
  to `task_submission_id`. Sequential benchmark jobs update one aggregate
  surface; they do not create competing terminal surfaces.
- **Project current state; never patch prior prose.** Rebuild the complete
  snapshot from durable state. Replay, restart, and duplicate delivery must
  converge on the same output and external identity.
- **Storage, projection, rendering, and publication stay separate.** Pure
  report models/ports may live in dependency-light `sbgh-core`; SQL remains in
  `sbgh-postgres`; application projection and surface policy remain in
  `sbgh-daemon`; provider calls remain behind `sbgh-github`/`sbgh-slack`.
- **No speculative abstraction.** Benchmark comparison/repetition detail and
  validation range/verdict detail remain task-specific. Introduce no plugin
  registry, general workflow renderer, or new crate without a demonstrated
  compiler boundary.
- **External names are contracts.** Preserve `stacks-bench` for benchmark branch
  protection. Introduce one documented, stable validation check name
  (`stacks-block-validation`). Build-only creates no required check.
- **Typed source of truth.** Validation renderers consume a typed result joined
  to the frozen request payload; they do not infer correctness from
  guest-controlled summary JSON.
- **Bound and escape untrusted detail.** Cap invalid-block entries, individual
  reason length, total rendered bytes, and artifact links. Escape Markdown,
  Slack mentions/code fences, and GitHub link text. Full typed detail remains
  available through the authenticated API/artifact path.
- **Reporting is non-authoritative.** Projection/publication failure never
  changes execution outcome. Durable input remains retryable until the
  idempotent external update succeeds.
- **Lifecycle mutation remains separate.** v28 renders states already produced
  today. Restart/supersession lineage is owned by `0065`; durable terminals
  created before a running attempt are owned by `0072`. Those items extend this
  view rather than creating another reporter.

## Target Contracts

Exact Rust names are implementation-owned, but the boundary must distinguish
shared state from task-specific data:

```rust
struct SubmissionReportView {
    identity: ReportIdentity,
    lifecycle: ReportLifecycle,
    task: TaskReport,
    artifacts: Vec<ReportArtifact>,
}

struct ReportIdentity {
    submission_id: Uuid,
    current_job_id: Option<Uuid>,
    current_attempt_id: Option<Uuid>,
    task_kind: TaskKind,
    source: JobSource,
    repository: String,
    commit: String,
}

enum TaskReport {
    Benchmark(BenchmarkReportView),
    BuildOnly(BuildOnlyReportView),
    BlockValidation(BlockValidationReportView),
}
```

`ReportLifecycle` owns task-neutral queued/running/phase/progress/completed/
failed/cancelled state and ordering. Task views carry only the information that
actually differs:

- benchmark metrics, repeat/variant position, baseline/comparison, and
  submission artifact;
- build/cache outcome for authenticated operator inspection; and
- requested/observed validation range, checked count, verdict, chainstate
  origin, bounded invalid blocks, and validation artifacts.

The authenticated API exposes the same tagged task report DTO rather than
separate ad hoc result endpoints. Later submission-detail work in `0065`
composes this view rather than re-querying result tables.

## Surface Identity and State

- Add aggregate-owned GitHub reporting identity beside aggregate GitHub
  provenance, symmetrical with Slack's submission-owned reporting identity.
  Preserve legacy job events as audit history.
- Reconcile a Check Run by App, stable task check name, head SHA, and a stable
  submission-derived external ID. Lookup failure is not “not found.”
- PR comments continue to use the submission marker and become task-aware.
- A benchmark submission becomes externally terminal only when its frozen
  workflow is terminal; intermediate repeats/specs update current position.
  Block validation is currently a singleton job and follows the same aggregate
  rule without a special publication path.
- Persist/adopt external IDs before later updates can race them. Existing
  search-before-create and fail-closed ambiguity behavior remains.
- Replayed or delayed older events cannot regress a newer aggregate snapshot or
  terminal conclusion.

Any schema migration is forward-only and must preserve existing benchmark check
and comment IDs. Historical identities that cannot be mapped unambiguously
remain auditable and fail closed rather than creating a second surface.

The intentional benchmark-facing behavior changes are limited and
reviewable:

- a new multi-job submission owns one GitHub check/comment identity; later
  repeats/specs update it instead of creating another per-job surface;
- an unambiguous historical external ID is adopted unchanged, never recreated;
  and
- a conflicting or unmappable historical identity stays auditable and blocks
  automatic adoption rather than electing an arbitrary winner.

Benchmark check name, conclusions, metrics, comparison semantics, links, and
wording otherwise remain compatible. The new
`stacks-block-validation` name applies only to validation.

## Phases

### Phase 1: Characterize Existing Reporting and Freeze Contracts

**Goal:** Establish behavioral baselines and the exact aggregate/report states
before refactoring.

**Scope:**

- Inventory GitHub/Slack surface creation, persisted external identities,
  durable projector inputs, benchmark result/comparison reads, and validation
  result persistence.
- Record golden benchmark outputs for queued, started, phase/progress,
  completed, failed, and cancelled states on PR, commit-check, and Slack
  surfaces.
- Define aggregate lifecycle rules for multi-run/multi-spec benchmark,
  singleton validation, build-only, infrastructure failure, and partial
  workflow failure.
- Decide and document the stable validation Check Run name and
  submission-derived external-ID format.

**Acceptance & Validation:**

- [ ] Every current report side effect and persistence owner is accounted for.
- [ ] Golden tests fail on benchmark wording, conclusion, links, or check-name
  drift.
- [ ] Aggregate terminality is defined without inspecting rendered text.
- [ ] No restart/supersession behavior is invented for unshipped lifecycle
  commands.

### Phase 2: Typed Aggregate Read Model

**Goal:** Build one storage-neutral report snapshot from persisted state.

**Scope:**

- Define pure report identity, lifecycle, artifact, and exhaustive task-detail
  types plus a narrow read port.
- Implement Postgres queries/conversions joining the frozen submission plan,
  ordered workflow/jobs, current attempt/event state, and typed task results.
- Decode the frozen block-validation request with the same protocol validation
  used at submission/execution; join it to the accepted typed result.
- Represent missing, legacy, malformed, or not-yet-terminal data explicitly
  rather than fabricating defaults.

**Acceptance & Validation:**

- [ ] Benchmark, build-only, and validation rows map through exhaustive typed
  conversions; unknown task/result combinations fail loudly.
- [ ] Requested validation range comes from frozen demand and observed range
  comes from the accepted result.
- [ ] A negative verdict is distinct from infrastructure failure.
- [ ] Postgres integration tests cover positive/negative validation, missing
  result, legacy data, multi-run benchmark, and contradictory rows.
- [ ] The read model contains no GitHub Markdown, Slack formatting, API client,
  or provider credential.

### Phase 3: Submission-Scoped Surface Identity

**Goal:** Make external reporting identity match the v27 aggregate.

**Scope:**

- Persist GitHub check/comment identity at submission scope and migrate
  unambiguous historical identities conservatively.
- Reconcile/create by task check name plus submission-derived external ID.
- Adapt sequential benchmark jobs to update one aggregate surface while
  retaining job/attempt IDs in audit and diagnostics.
- Preserve Slack's canonical reporting identity, snapshot version fencing,
  search-before-create, and message timestamp adoption.

**Acceptance & Validation:**

- [ ] Crash after external create but before local acknowledgement reconciles
  the same check/comment/message.
- [ ] Repeats and later specs do not create a second aggregate report or
  terminalize it early.
- [ ] Benchmark and validation checks on the same commit coexist because their
  stable names differ.
- [ ] Lookup failure and multiple matches fail closed; neither authorizes
  creation.
- [ ] Migration tests preserve unambiguous historical benchmark identities and
  roll back on conflicting ownership.
- [ ] Representative upgrade fixtures cover one job/one identity, multiple jobs
  sharing one identity, duplicate historical candidates, divergent check or
  comment IDs within one submission, and a legacy submission with no mappable
  identity.
- [ ] Divergent ownership reports the offending submission and external IDs,
  and a late migration failure rolls back all preceding schema/backfill work.

### Phase 4: Task-Aware GitHub Rendering

**Goal:** Render correct checks/comments from the typed aggregate snapshot.

**Scope:**

- Replace global check-name and benchmark-wording decisions with exhaustive
  task report policy.
- Preserve `stacks-bench` output and conclusion semantics: performance is data,
  not a correctness failure.
- Add `stacks-block-validation`: valid is success, invalid blocks are failure,
  infrastructure failure is failure, and cancellation is cancelled.
- Render task-correct initial, queued, running, phase/progress, terminal, retry,
  and artifact text.
- Bound and escape validation detail; link to authenticated typed/full results.

**Acceptance & Validation:**

- [ ] Benchmark GitHub golden outputs remain behavior-compatible.
- [ ] Validation never appears under `stacks-bench` or says “benchmark.”
- [ ] Branch protection can independently require benchmark or validation by
  stable name.
- [ ] Positive, negative, infrastructure-failed, and cancelled validation
  snapshots have the correct GitHub conclusion and bounded detail.
- [ ] Build-only remains externally silent under current policy.

### Phase 5: Task-Aware Slack Rendering

**Goal:** Extend the canonical Slack snapshot without duplicating task
lifecycle logic.

**Scope:**

- Map the shared aggregate snapshot into a task-aware Slack view.
- Keep snapshot-from-current-state, monotonic versioning, debounce, unchanged
  suppression, reactions, and search-before-create unchanged.
- Query benchmark metrics/comparison only for benchmark detail.
- Render validation progress/result without profiler, benchmark database, or
  comparison reads.

**Acceptance & Validation:**

- [ ] Existing benchmark Slack golden snapshots remain compatible.
- [ ] Validation progress, success, invalid verdict, infrastructure failure,
  and cancellation contain no benchmark-only fields or queries.
- [ ] Replay produces one identical current snapshot and cannot regress a newer
  terminal.
- [ ] Emoji remain outside aligned code-block columns; mentions, code fences,
  URLs, and validation reasons are safely rendered.

### Phase 6: Typed API, Replay, and Boundary Enforcement

**Goal:** Expose the read model safely and prevent reporting paths from
diverging again.

**Scope:**

- Add an authenticated submission-report endpoint and tagged public DTO.
- Keep authorization/visibility in daemon composition; never expose worker
  credentials, presigned upload grants, raw guest paths, or unbounded logs.
- Exercise projector catch-up, duplicate delivery, restart, stale update, and
  terminal races for both task kinds.
- Add focused checks that reject a global check-name constant, untyped
  validation-summary inference, direct validation-table SQL in renderers, and
  unconditional benchmark-metric reads.
- Update architecture, daemon API, operations, branch-protection, and
  contributor documentation.

**Acceptance & Validation:**

- [ ] The endpoint returns the same typed task view used by renderers and
  rejects unauthorized cross-repository reads.
- [ ] Durable replay converges after daemon restart for benchmark and
  validation.
- [ ] A stale event cannot overwrite a newer terminal or another task's
  surface.
- [ ] Ratchets fail on reintroduced benchmark-only shared reporting logic.
- [ ] No new submission writer, scheduler branch, worker protocol, or execution
  path is introduced.

## Final Validation

- [ ] `just build --no-sccache`
- [ ] `just lint --no-sccache`
- [ ] `just test --summary --no-sccache`
- [ ] `git diff --check`
- [ ] Fresh and upgrade migration suites pass.
- [ ] Benchmark GitHub and Slack golden suites pass unchanged except for
  explicitly documented aggregate-identity corrections.
- [ ] Validation positive/negative/failure/cancel and bounded-output suites
  pass.
- [ ] Replay/reconcile/fencing suites pass for both task kinds.
- [ ] Package-DAG, unused-dependency, docs/registry, and reporting-boundary
  checks pass.
- [ ] On a real deployment, one benchmark and one block-validation canary each
  submit, execute through the fleet, project, reconcile after daemon restart,
  and expose the typed result.

## Rollout and Rollback

Deploy daemon/API/CLI together:

1. Drain workers, let durable report projection catch up, stop the daemon, and
   create a restorable production backup.
2. Restore that backup into isolated PostgreSQL and apply the complete v28
   migration there. Compare submission/job counts and every mapped
   check/comment/message ID before and after. Investigate any ambiguity
   diagnostic; never bypass it or discard an identity.
3. Only after the restored-production rehearsal passes, apply the migration to
   production and deploy the matching binaries.
4. Verify existing benchmark checks reconcile rather than duplicate, then
   canary one benchmark and one admin-submitted validation on separate stable
   check names. Restart the daemon during each canary and require convergence on
   the same external identity and typed result before resuming producers.

Rollback stops new projection, restores the pre-v28 database backup if schema
changed, and deploys v27 binaries. Do not run v27 against a migrated
aggregate-report schema. Existing external checks/messages remain audit history;
never delete them as rollback.

## Deferred / Non-Goals

- New `/validate` GitHub grammar or authorization (`0067`).
- Task-neutral cancel/restart/replace persistence (`0065`).
- Durable pre-attempt cancellation/supersession projection (`0072`).
- GitHub lifecycle commands (`0071`).
- Task-aware LLM intent, Slack submission, or Slack control commands
  (`0069`/`0078`/`0070`).
- Watched-ref action fan-out or latest-wins policy (`0068`).
- Placeholder/skipped checks (`0014`).
- Benchmark metric/result restructuring (`0028`), new execution-result
  semantics, durable event-ledger redesign, worker protocol changes, or a
  general reporting/plugin framework.

## Follow-Ups

- `0067` and `0078` add GitHub and Slack validation producers through the v27
  submission kernel and consume this task-aware report surface.
- `0065` defines lifecycle mutation; `0072` projects its pre-attempt terminals
  into this same snapshot; `0071` then exposes GitHub controls.
- `0068` reuses the stable per-task check identities and lifecycle policy for
  watched refs.
- `0014` reuses task-aware naming for placeholder/skipped checks that have no
  submission or attempt.
