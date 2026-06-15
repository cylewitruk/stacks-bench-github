# v15: Isolated Benchmark Repetitions

Item `0038-isolated-benchmark-repetitions`: make "repetitions" mean clean,
daemon-orchestrated VM executions under one benchmark group, not
`stacks-bench` in-process loops.

> **Status:** in_progress
>
> Phase 1 is implemented and ready for review. Requests now carry a
> daemon-owned clean-repeat count, enforce a conservative cap before enqueue,
> and normalize every single VM invocation to `stacks-bench --repetitions 1`.
> Phase 2's deploy-safe planning foundation is implemented: requested clean-run
> counts are persisted on `benchmark_spec`, and both stores expose tested
> append/resume primitives for the lazy run chain. Automatic runtime chaining
> remains deferred until the Phase 3/4 carry-forward + single-surface execution
> pieces land, so the Slack surface temporarily rejects requests above one
> clean repetition instead of silently under-delivering.

## Items

| Item | Role | Status |
| ---- | ---- | ------ |
| `0038-isolated-benchmark-repetitions` | primary | in_progress |

## Why

`stacks-bench --repetitions` repeats work inside one process over one
already-touched LVM snapshot and chainstate handle. For `sbgh`, the user word
"repetitions" should mean clean VM executions: fresh source snapshot, fresh VM,
fresh `stacks-bench` process, and normal teardown per sample. Cold/hot behavior
is steered by warmup, not by reusing an already-open chainstate handle.

## Scope

Add daemon-level isolated repeats while preserving the v14 group/spec/run model:

- A request with `N` clean repeats creates one `BenchmarkGroup`, one
  `BenchmarkSpec`, and the first `BenchmarkRun` job with
  `benchmark_run_index = 0`. Later runs are created lazily after the prior run
  finishes and its DB has been carried forward.
- The group stays host-pinned. All repeats use the same host, source repo,
  cached binary, carried SQLite DB, and later shared calibration.
- The build/artifact step runs at most once per build target for the group, or
  is fully cache-reused. Repeats do not re-run build->bench per sample.
- Each VM invocation receives `stacks-bench --repetitions 1`; `--warmup`
  remains the way to steer cold/hot pre-measurement behavior.
- The group's `stacks-bench.db` is carried forward via archived artifacts:
  after run N completes and its job-scoped DB is archived, promote/copy that
  DB into the group namespace; before run N+1 starts, copy the group DB into
  that run's results tmpfs. Do not race VM/tmpfs teardown.
- The group owns the user-facing reporting surface. Individual run jobs update
  that one surface with live "repeat K/N" progress instead of posting separate
  Slack cards, GitHub comments, or check runs per repeat.
- Execution is sequential by invariant, independent of `max_concurrent_jobs`.
  Parallel repeats are deferred because they need a shared-writable-storage or
  per-run-DB merge design.

Non-goals: no multi-ref comparison (`0039`), no shared calibration command
(`0041`), no parallel repeat execution, and no public expert mode for nested
in-process repetitions.

## Design Decisions

- **"repetitions" means clean runs.** The daemon should stop treating user
  repetitions as `stacks-bench --repetitions`; it plans N VM executions and
  passes `--repetitions 1` into each execution. This deliberately changes the
  existing v13 meaning of the user-facing field from cheap in-process loops to
  potentially expensive VM lifecycles, so the clean-repeat cap is also a
  migration safety bound.
- **Use v14 jobs as runs.** A `BenchmarkRun` remains an ordinary claimable
  `job`; scheduler/status/reporting reuse existing job machinery.
- **Report at group level.** Run jobs may still carry the data needed to route
  progress internally, but user-visible Slack/GitHub output belongs to the
  `BenchmarkGroup`. Runs 1..N-1 must not create their own cards/comments/checks.
- **Lazy enqueue enforces ordering.** Only run 0 is queued at group creation.
  After run K completes and carry-forward succeeds, enqueue run K+1 until the
  requested count is reached. This guarantees strict ordering and at most one
  in-flight run per group without adding a blocked job status.
- **Carry SQLite, not live-share it.** SQLite can coordinate multiple writers,
  but sharing a tmpfs-backed file between VMs is the harder problem. v15 uses
  archive-backed copy-out/copy-in between sequential runs.
- **Group DB is bounded for a fixed workload.** Block/tx indexes are
  unique-keyed and idempotent, so after the first run the DB mostly gains small
  measured-run rows.

## Phases

### Phase 1: Request Model + Caps

**Goal:** Add a daemon-owned clean-repeat count without changing execution yet.

**Scope:**

- Extend `WorkloadSpec` / intent resolution / deterministic parser so a request
  can carry `clean_repetitions` (name bikesheddable, but it must be distinct
  from in-process repetitions).
- Preserve `warmup`.
- Add `[runner] max_clean_repetitions` with a conservative default.
- Enforce the cap after parsing/LLM resolution and before enqueue.
- Normalize effective bench args so each VM run will receive
  `--repetitions 1`.
- Document the semantic change from v13 in user-facing help/rejection text:
  "repetitions" now means clean VM runs.

**Status:**

- [x] Core implementation
- [x] Unit/integration tests
- [ ] Reviewed
- [ ] Validated

**Acceptance & Validation:**

- [x] Explicit/NL requests for repeats resolve to clean-repeat count.
- [x] Requests above `max_clean_repetitions` are rejected before enqueue.
- [x] Existing requests with no repeat count behave as one clean run.
- [x] A previously-cheap high repetition count is bounded by
  `max_clean_repetitions` before enqueue.
- [x] No user path can smuggle nested `stacks-bench --repetitions N` as the
  primary UX.

### Phase 2: Group Run Planning

**Goal:** Plan repeat groups while guaranteeing strict run ordering.

**Scope:**

- Add creation/store APIs for appending the next `job` row to an existing
  `benchmark_group_id` / `benchmark_spec_id`.
- Ensure `benchmark_run_index` is allocated uniquely and deterministically.
- Store the requested clean-run count on the group/spec side, or an equivalent
  planner-owned metadata row, so the completion hook knows whether to enqueue
  run K+1.
- Use lazy enqueue: at group creation only run 0 is queued; run K completion
  enqueues run K+1 after carry-forward succeeds.
- Make the lazy-enqueue chain DB-resumable. On startup, the daemon can derive
  from persisted group/run state whether a non-terminal group completed run K
  but still needs run K+1 queued.
- Keep current one-run paths as the `N=1` case.
- Decide terminal policy for partial groups: one failed repeat should mark the
  group as partial/failed with visible reason, not silently disappear.

**Status:**

- [x] Core implementation
- [x] Unit/integration tests
- [ ] Reviewed
- [ ] Validated

**Acceptance & Validation:**

- [x] A repeat request persists the requested clean-run count on the group spec
  while still creating only run 0 initially.
- [x] Store append/resume primitives enforce at most one active run per spec.
- [x] Store append/resume primitives create run jobs strictly in
  `benchmark_run_index` order.
- [x] The next run can be derived from persisted DB state after run K
  completed and before run K+1 exists.
- [x] The `job_benchmark_spec_run_unique` constraint catches duplicate indexes.
- [x] Existing `claim_next_queued` still claims ordinary job rows.
- [x] Build-only/cache-warm jobs remain single-run.
- [x] User-facing Slack requests for `clean_repetitions > 1` reject loudly until
  the runtime chain is activated.
- [ ] Runtime completion/startup hooks call the append/resume primitives after
  Phase 3/4 can carry the DB and update one group surface without per-run
  fan-out.

### Phase 3: Build Reuse + Sequential Execution

**Goal:** Run repeated benchmark jobs without rebuilding per repeat.

**Scope:**

- Factor the execution path so the group build/artifact step happens once per
  build target (or cache hit) and each measured run consumes that artifact.
- Ensure every repeat gets a fresh source snapshot, VM, and bench process.
- Keep the group host-pinned for all repeats.
- Trigger the lazy enqueue of the next run only from the prior run's successful
  completion/carry-forward path.
- Suppress per-run reporting fan-out: each run advances the group's single
  surface with repeat progress and final partial/complete state.
- Keep the implementation as step composition, not separate rigid pipelines for
  benchmark vs block validation vs warming.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests
- [ ] Reviewed
- [ ] Validated

**Acceptance & Validation:**

- [ ] N clean repeats do not run build->bench N times.
- [ ] Every measured repeat still runs in a fresh VM/snapshot.
- [ ] Cache hits and build misses both preserve the same repeat semantics.
- [ ] A failed run stops the lazy-enqueue chain and leaves visible partial
  group state.
- [ ] A repeat group produces one user-facing Slack/GitHub surface, not one
  card/comment/check per run.

### Phase 4: SQLite Carry-Forward

**Goal:** Reuse one `stacks-bench.db` artifact across clean VM executions.

**Scope:**

- After run N, copy/promote the archived job-scoped SQLite DB from
  `<job_id>/...` into the group artifact namespace.
- Before run N+1, copy the group DB into the next run's results tmpfs.
- Keep job-scoped artifacts intact for forensics.
- Make missing/corrupt carried DB a loud group failure, not a silent fresh DB.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests
- [ ] Reviewed
- [ ] Validated

**Acceptance & Validation:**

- [ ] A multi-repeat group produces one shared DB containing rows for all clean
  runs.
- [ ] The final result card/download links the shared DB.
- [ ] A carry-forward failure is visible and terminal/partial according to the
  Phase 2 policy.

### Phase 5: Summary Reporting

**Goal:** Show useful variance information for repeat groups.

**Scope:**

- Aggregate repeated-run metrics from Postgres-promoted per-run `job_metric`
  rows. The shared SQLite DB remains the linked raw-sample artifact, not the
  summary source for v15.
- Render count, min, max, mean, standard deviation, and coefficient of
  variation.
- Keep existing single-run Slack/GitHub rendering unchanged.
- Mark whether the result is a repeated clean-run group.
- Show live repeat progress on the group surface, e.g. "repeat 2/5 running",
  before replacing it with the aggregate result.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests
- [ ] Reviewed
- [ ] Validated

**Acceptance & Validation:**

- [ ] A completed repeat group reports the aggregate variance summary.
- [ ] In-progress groups show the current repeat K/N on the single group
  surface.
- [ ] The report links the shared DB.
- [ ] Single-run jobs still render as today.

## Final Validation

- [ ] `just build`
- [ ] `just lint`
- [ ] `just test`
- [ ] Host smoke: request 2 clean repeats, confirm two fresh VMs/snapshots, one
  build/cache artifact, one shared DB, and a variance summary.

## Follow-Ups

- `0039-multi-variant-benchmark-comparisons` lifts the one-spec active runtime
  cap.
- `0041-shared-benchmark-calibration` inserts a calibration step before measured
  runs.
- `0015-resource-aware-admission` should eventually replace the hard repeat cap
  with richer resource budgeting.
