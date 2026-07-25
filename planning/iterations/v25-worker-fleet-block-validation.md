# v25: First Worker Fleet and Dedicated Block Validation

Turn the single-host daemon into an orchestrator plus workers, then place block
validation on the available dedicated Hetzner host while preserving benchmark
behavior on a separate co-located worker.

> **Status:** planned — depends on the compiler-enforced crate topology from
> [v24.1](../archive/completed/0056-compiler-enforced-crate-boundaries.md).
>
> The deployment target is now concrete: one dedicated Hetzner worker with a
> 64-core CPU, 256 GB RAM, and four 4 TB NVMe drives is available for
> block-validation work. v25 therefore delivers a production-usable first fleet,
> not only a remote-execution design or cloud placeholder.

## Items

| Item | Role | Status |
| ---- | ---- | ------ |
| `0004-worker-fleet` | primary: orchestrator/worker distribution model | planned |
| `0017-generic-phase-events` | load-bearing: task-neutral durable worker events | planned |
| `0019-block-validation-recipe` | co-primary: first remote, heterogeneous task | planned |

Detailed designs remain in
[`0004-worker-fleet.md`](../design/0004-worker-fleet.md),
[`0017-generic-phase-events.md`](../design/0017-generic-phase-events.md), and
[`0019-block-validation-recipe.md`](../design/0019-block-validation-recipe.md).
This iteration is the execution/validation plan and owns status.

## Why

The current daemon executes all work on one host. That caps concurrency and
couples low-variance libvirt benchmarks to workloads with very different
hardware needs. Block validation needs a full multi-terabyte chainstate,
copy-on-write workspaces, probe-driven partitioning, high local-NVMe throughput,
and broad CPU utilization. It should not run on the pinned benchmark substrate.

The available Hetzner machine removes the largest external uncertainty behind
`0004`: there is now a real second host and a real second task kind against which
to validate capability scheduling, worker liveness, artifact transfer, and
failure semantics.

v25 retains two orthogonal seams:

1. A task `Recipe` describes benchmark, build-only, or block-validation work.
2. A local `Driver`/execution substrate runs that task on a worker host.

The fleet layer sits above them. A fleet worker claims one sbgh job; block
validation fans that job into K **local validation shards** on the same worker.
Those shards are not K independently scheduled fleet workers.

## Target Topology

```text
GitHub / Slack / CLI
          |
          v
sbgh-daemon (orchestrator; sole DB client and GitHub/Slack side-effect owner)
    |                           |
    | loopback worker API       | authenticated outbound worker connection
    v                           v
sbgh-worker                  sbgh-worker
benchmark capability         block-validation capability
existing pinned/libvirt      Hetzner: 64 cores / 256 GB / 4x4 TB NVMe
driver                       local dataset + K CoW validation shards
```

At v25 completion the orchestrator does not execute jobs. A one-box benchmark
installation is orchestrator plus a separate loopback worker using the same
protocol as the remote host.

## Scope

- Introduce dependency-light worker protocol types and an orchestrator worker
  API; turn the v24.1 `sbgh-worker` library into the separately deployed worker
  process.
- Persist fully resolved effective task arguments, or an equivalent immutable
  configuration snapshot, when work is enqueued. The workload key and persisted
  arguments must derive from the same tokens; leases carry those arguments so
  worker execution cannot change when daemon defaults drift.
- Register workers with capabilities, resource facts, version, and an
  operator-declared measurement profile; workers dial out and never receive DB
  credentials.
- Capability-match and lease whole scheduling units. Benchmark groups remain
  pinned to one worker for all variants/repeats/calibration/carry-forward.
- Move benchmark/build execution out of the orchestrator into the co-located
  worker and remove the inline execution path.
- Implement task-neutral event ingest, artifact transfer, completion, and
  lease-scoped idempotency.
- Implement block validation as a second task kind on the dedicated host:
  build, dataset preparation, probe, K local CoW shards, and reduction.
- Provide authenticated operator/API enqueue and a bounded GitHub-facing
  trigger/reporting path for block validation.
- Add heartbeat, drain, lease expiry, reconnect, stale-attempt rejection, and
  split orchestrator/worker orphan recovery before production use.
- Deploy and validate the two-worker topology with operational runbooks and
  rollback.

**Non-goals:** no cloud autoscaler/provisioner, spot lifecycle, multi-worker
block-validation sharding, direct worker access to PostgreSQL, generic workflow
engine, shared-write chainstate, cross-worker build scheduling, multi-job
bin-packing on one worker, fleet-wide fairness/quotas, portal UI, or benchmark
execution on the block-validation host. v25 conservatively leases one sbgh job
or one benchmark group to a worker at a time; richer resource-aware admission
remains `0015`.

## Design Rules

- **Workers pull; the orchestrator owns truth and external reporting.** All DB
  state transitions and GitHub/Slack side effects occur in `sbgh-daemon`. It
  alone holds Slack credentials and owns report rendering, debounce, rate
  limiting, retries, and reporting-session state. A worker may receive only a
  short-lived, lease-scoped GitHub token for repository access; it does not
  perform GitHub or Slack reporting.
- **One production execution model.** The inline daemon worker is removed once
  the loopback worker passes parity validation.
- **Protocol DTOs are not DB models.** Worker messages are versioned,
  serializable, owned types with explicit validation.
- **v25 uses exact protocol-version matching.** Rolling mixed-version operation
  is not promised; upgrades drain/stop workers and coordinate orchestrator and
  worker rollout. A compatibility range is a later explicit feature.
- **Lease and attempt identity guard every mutation.** A late event or terminal
  result from a superseded attempt cannot affect the current job.
- **Reliable events are durable before acknowledgement.** Task-neutral event
  ingest and reporter replay land with the first real execution cutover, not
  after benchmark traffic already depends on the network.
- **Scheduling unit affinity is durable.** A benchmark group remains on one
  worker; a block-validation task and all of its local shards remain on one
  dataset host.
- **Block-validation negative result is not infrastructure failure.** Invalid
  blocks produce a completed task with a negative/red result; transport,
  provisioning, timeout, or worker loss are execution failures.
- **Remote baseline safety is fail-closed.** Benchmark comparison uses the
  measurement profile stamped atomically at assignment; absent/mismatched
  profiles disable comparison rather than borrowing another host's baseline.
- **Remote-local cleanup is honest.** The orchestrator records cleanup
  obligations; only the worker that owns local resources can perform them.

## Phases

### Phase 1: Host Characterization and Minimal Control Plane

**Goal:** Prove that a separate worker can securely register, heartbeat, and
claim compatible stub work while the dedicated host's storage/resource model is
measured rather than assumed.

**Scope:**

- Reconcile the `0004`/`0019` protocol contracts around `sbgh-proto`, the
  existing `sbgh-worker`/`sbgh-libvirt` crates, fleet workers, and local
  validation shards.
- Add a dependency-light `sbgh-proto` with the minimum Phase 1 messages:
  register, heartbeat, long-poll, offer/claim acknowledgement, stub complete,
  and stub fail.
- Use exact protocol-version matching for v25 and include an orchestrator-issued
  trace/correlation ID in every stub assignment and related log.
- Add orchestrator-owned worker registry and lease/attempt schema with worker
  status, capabilities, resource facts, software/protocol version,
  measurement profile, last heartbeat, and drain state.
- Add authenticated worker endpoints. Use an outbound worker connection over a
  documented secured network path; workers receive no database credentials.
- Add registration, heartbeat, long-poll, and stub execution transport to the
  existing worker library and its new binary.
- Run the real worker binary over loopback, then from the Hetzner host.
- Inventory the Hetzner CPU topology/NUMA layout, memory, NVMe devices,
  filesystem/reflink support, usable capacity, sustained read/write behavior,
  network path, and failure/backup expectations.
- Select and document the dataset/workspace filesystem and mount layout. Verify
  CoW cloning with the exact command/API v25 will use; do not assume all Linux
  filesystems or RAID layouts preserve the required semantics.
- Define how the canonical block-validation dataset is initially hydrated,
  updated, identified, and protected from writable shard mutation.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests, if applicable
- [ ] Reviewed (Codex)
- [ ] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [ ] A real loopback worker and the Hetzner worker register, heartbeat, receive
  only capability-compatible stub work, and complete it without DB access.
- [ ] Invalid/expired registration credentials and any non-exact protocol
  version mismatch fail closed with actionable diagnostics.
- [ ] Worker/offline/draining state is operator-visible.
- [ ] Hetzner storage measurements prove the selected layout can hold the
  canonical dataset plus the intended number of CoW workspaces with explicit
  headroom.
- [ ] A CoW clone is demonstrated and mutation of a clone does not mutate the
  canonical dataset.
- [ ] CPU/NUMA/NVMe measurements are recorded and no production shard count is
  chosen merely from the advertised 64-core count.

**Tests:**

- Protocol serialization/version fixtures.
- Worker API authentication, capability filtering, and lease-race tests.
- Loopback worker integration test using the real binary/library construction.
- Hetzner host inventory, reflink, storage-throughput, and connectivity smoke
  report.

### Phase 2: Durable Remote Data Plane and Loopback Benchmark Cutover

**Goal:** Run existing benchmark/build jobs through a separate worker and remove
execution from the orchestrator before adding block validation.

**Scope:**

- Add transport and a binary composition root to the v24.1 `sbgh-worker`
  library. Reuse its driver API, recipes, cache/artifact services, and
  `sbgh-libvirt` adapter rather than copying or re-extracting them.
- Keep worker transport/configuration in `sbgh-worker`; keep DB, GitHub/Slack
  clients and credentials, scheduling, reporting, comparison, and webhook
  processing in `sbgh-daemon`.
- Turn the v24.1 owned execution request/task payload into validated worker wire
  DTOs without exposing DB row shapes or backend-only configuration.
- Mint any private-repository credential per job and deliver it short-lived and
  lease-scoped; workers never receive the GitHub App private key or another
  long-lived GitHub credential.
- Implement `0017` before cutover: task-neutral reliable events are
  attempt-scoped, sequence-numbered, durably committed before acknowledgement,
  projected only across a contiguous sequence prefix, idempotent, and replayable
  by a restarted reporter. A stored out-of-order terminal cannot terminalize the
  job or promote artifacts across a sequence gap. Progress/heartbeat delivery
  remains explicitly best-effort where safe.
- Add deterministic hidden identity markers and search-before-create
  reconciliation for PR comments, paralleling Check Run `external_id`
  reconciliation and closing the comment create/persist crash window.
- Propagate the assignment trace/correlation ID through worker logs, events,
  artifact manifests, and orchestrator reporting logs.
- Add artifact upload with size limits, checksums, and store-key results. Select
  one v25 path—authenticated orchestrator upload or lease-scoped object-store
  upload—and preserve the accepted artifact-store decisions. Upload into an
  attempt-scoped staging namespace; only an accepted fenced terminal may
  promote/attach the manifest to the job. Rejected/stale attempts remain
  invisible to consumers and are eligible for GC.
- Stamp `{worker_id, measurement_profile, attempt/lease}` atomically when the
  orchestrator assigns work.
- Run all benchmark variants/repeats/calibration for a group on the same worker.
- Deploy a co-located benchmark worker over loopback and, after parity
  validation, remove the daemon's inline execution path and its transitional
  `sbgh-worker`/`sbgh-driver` dependencies.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests, if applicable
- [ ] Reviewed (Codex)
- [ ] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [ ] `sbgh-worker`, `sbgh-libvirt`, and `sbgh-driver` retain the v24.1
  dependency boundaries and have no dependency on `sbgh-core`, SQLx, Octocrab,
  Axum, Slack, or report-surface crates.
- [ ] `sbgh-worker` has no database or Slack credentials, direct DB client,
  Slack client, or GitHub reporting client. Any GitHub credential it receives
  is short-lived, lease-scoped, and limited to repository access.
- [ ] GitHub/Slack report rendering, debounce, rate limiting, retries,
  idempotency, and reporting-session state execute only in `sbgh-daemon`.
- [ ] A loopback worker completes benchmark and build-only jobs through the
  protocol, including progress, artifacts, reporting, cache publication,
  cancellation, and carried-group behavior.
- [ ] Duplicate/out-of-order reliable events and duplicate terminal submissions
  do not duplicate DB or GitHub/Slack side effects.
- [ ] Projection halts at the first reliable-sequence gap; a later terminal is
  neither accepted nor artifact-promoting until the prefix is contiguous.
- [ ] Killing/restarting the orchestrator during a loopback benchmark replays
  committed task-neutral events and completes reporting without depending on
  the original in-memory channel.
- [ ] A stale/rejected terminal cannot publish staged artifact keys; abandoned
  attempt staging is discoverable for later GC.
- [ ] One trace ID correlates assignment, worker execution, reliable events,
  artifacts, and terminal reporting.
- [ ] Losing the initial-comment response or its DB identity write and then
  restarting reconciliation finds the bot-authored marker and leaves exactly
  one PR comment.
- [ ] A benchmark with no compatible measurement profile is comparison-disabled
  rather than matched to an unsafe baseline.
- [ ] The daemon contains no production path that executes a recipe/driver
  locally after cutover.
- [ ] After cutover, `sbgh-daemon` has no normal dependency on `sbgh-worker`,
  `sbgh-driver`, or `sbgh-libvirt`; it exchanges only versioned `sbgh-proto`
  request/event DTOs with workers.

**Tests:**

- Protocol event/terminal idempotency and stale-attempt tests.
- Reporter restart/replay, out-of-order gap, and terminal-prefix tests from the
  durable attempt-event ledger.
- PR-comment marker reconciliation test across create-success/ID-write failure.
- Artifact checksum, size-limit, interrupted-upload, stage/promote, stale
  terminal, and store-key tests.
- Existing benchmark/build-only suites rerun through loopback transport.
- Host benchmark parity smoke against the v24.1 in-process worker path before
  the daemon-to-worker library edge is removed.

### Phase 3: Block-Validation Recipe

**Goal:** Add block validation as the second task kind without changing fleet
scheduling or execution lifecycle control flow.

**Scope:**

- Add the block-validation task/build target and a validated task payload;
  register the recipe through the v24.1 worker dispatch seam.
- Add a worker-local block-validation execution substrate for:
  `stacks-inspect` build/cache, canonical dataset validation, K CoW workspace
  creation, probe/index-range planning, contiguous shard assignment, bounded
  parallel fan-out, reduction, and cleanup.
- Keep workspace count, shard count, and concurrency as separate values.
- Add a typed block-validation result store and concise GitHub check/comment
  rendering, including invalid-block details and artifact keys for complete
  shard logs/failure lists.
- Add authenticated API/CLI enqueue and one bounded GitHub-facing trigger path.
  Reuse existing repository/ref resolution and job provenance rather than
  creating a parallel build pipeline.
- Configure the Hetzner worker with `block_validation` capability and dataset
  identity/resource facts; the co-located benchmark worker must reject this
  task.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests, if applicable
- [ ] Reviewed (Codex)
- [ ] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [ ] Adding block validation changes task registration/composition and
  task-specific persistence/reporting, but not scheduler, lease, event-ingest,
  cancellation, or terminal-control flow.
- [ ] A known-good fixture completes successfully through probe, K local shards,
  and reduction.
- [ ] Deliberately reported invalid blocks produce a completed negative/red
  result and are not classified as retryable infrastructure failure.
- [ ] VM/host process loss, timeout, transport loss, and setup failure remain
  distinguishable execution failures.
- [ ] The same canonical dataset is never writable by a shard; each shard gets
  its own verified CoW workspace.
- [ ] Benchmark and block-validation progress both persist through the same
  task-neutral event model.

**Tests:**

- Recipe unit tests for probe partitioning, remainder distribution, exit-code
  reduction, invalid-block semantics, timeout, cancellation, and cleanup.
- Filesystem integration tests for dataset identity and CoW isolation.
- Store/reporting tests for positive, negative, and infrastructure outcomes.
- Capability-routing test proving block validation reaches only the dedicated
  worker.

### Phase 4: Distributed Liveness, Recovery, and Drain

**Goal:** Make long-running remote work safe under worker restart, network
partition, orchestrator restart, and planned host maintenance.

**Scope:**

- Add heartbeat and lease TTL policy with explicit attempt fencing and
  stale-event/terminal rejection.
- Define retry/requeue policy by failure class. Never automatically reinterpret
  a completed negative validation result as retryable.
- For a task whose only capable worker is offline or fenced, hold it durably and
  alert instead of entering a generic requeue loop. A successor attempt may
  start only after the worker returns and satisfies cleanup/recovery, or after an
  explicit operator decision records why the prior attempt is safe to abandon.
- On lease expiry, terminalize or requeue according to policy, record a durable
  cleanup obligation, and mark the worker stale/offline.
- On worker startup/reconnect, enumerate its outstanding attempts/cleanup
  obligations and run local idempotent `cleanup_by_job_id`/workspace cleanup.
- Add event resume from the last orchestrator-acknowledged sequence number.
- Reap attempt-scoped staged artifacts whose attempt is terminal-rejected,
  fenced, or expired without an accepted terminal. GC is idempotent, retains an
  auditable grace period, and never removes artifacts attached to an accepted
  result.
- Add graceful drain: stop claiming, finish or explicitly abort current work,
  satisfy cleanup obligations, and deregister.
- Define orchestrator-restart behavior for outstanding long polls, active
  leases, reporter state, and pending terminal/artifact submissions.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests, if applicable
- [ ] Reviewed (Codex)
- [ ] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [ ] Killing a worker during execution cannot produce two accepted terminal
  outcomes for one attempt or allow stale events to mutate a successor attempt.
- [ ] A returning worker cleans its local orphan resources idempotently and
  clears the corresponding obligation.
- [ ] If a worker never returns, the orchestrator reports the unresolved local
  cleanup honestly rather than claiming it was centrally reaped.
- [ ] Loss of the only `block_validation` worker holds and alerts affected work;
  it does not spin through silent requeue attempts or accept the old attempt
  after fencing.
- [ ] Attempt staging without an accepted terminal is reclaimed after the
  configured grace period, while accepted result artifacts are retained.
- [ ] Restarting the orchestrator during a run preserves lease and reporting
  correctness; the worker resumes from an acknowledged event sequence.
- [ ] Drain prevents new claims and leaves no unreported active local work.

**Tests:**

- Deterministic lease/attempt state-machine tests.
- Kill/restart/network-interruption integration tests with controllable clocks.
- Loopback worker and remote-host drain/reconnect smoke tests.

### Phase 5: Dedicated-Host Production Cutover

**Goal:** Operate the first real fleet: loopback benchmark worker plus remote
Hetzner block-validation worker.

**Scope:**

- Install and supervise version-matched `sbgh-worker` services on the local and
  Hetzner hosts, with least-privilege users, secrets, filesystem permissions,
  log retention, and restart policy.
- Hydrate and verify the canonical chainstate dataset on the dedicated host.
- Select K workspace/shard/concurrency values from measured CPU, NUMA, memory,
  NVMe, and end-to-end validation data; keep conservative operator overrides.
- Run staged validation: stub, small fixture, bounded real range, full planned
  range, injected invalid result, worker kill, orchestrator restart, and drain.
- Add operator visibility for worker version/status/capabilities/profile,
  current lease, last heartbeat, dataset identity, cleanup obligations, and
  recent task outcome.
- Document registration-token rotation, worker upgrade compatibility, dataset
  refresh, drain/maintenance, cleanup recovery, rollback, and incident triage.
  v25 upgrades are coordinated: drain and stop workers, upgrade the
  orchestrator and workers to the same protocol version, then restart and
  verify registration before releasing held work.
- Retain a rollback release/configuration that can restore the pre-cutover
  single-host deployment until the two-worker topology completes its soak.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests, if applicable
- [ ] Reviewed (Codex)
- [ ] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [ ] A normal benchmark group completes on the co-located worker with expected
  comparison/reporting behavior.
- [ ] A real block-validation job is claimed by the Hetzner worker, uses the
  verified local dataset, completes K-shard reduction, uploads artifacts, and
  renders its terminal result through the orchestrator.
- [ ] Neither worker can claim a task outside its configured capabilities.
- [ ] PostgreSQL credentials, Slack credentials, and long-lived GitHub App
  credentials exist only on the orchestrator; any worker repository token is
  short-lived, least-privilege, and lease-scoped.
- [ ] Worker and orchestrator restart/upgrade procedures have been exercised,
  not merely documented.
- [ ] The deployment soaks for an agreed period with no duplicate terminal
  writes, stale leases, unbounded artifact growth, or unresolved local cleanup.

**Tests:**

- Two-host operational validation checklist and captured run IDs/artifact keys.
- Known-good and known-negative block-validation runs.
- Failure-injection matrix covering worker process, network, orchestrator,
  artifact upload, and local cleanup.

## Final Validation

- [ ] `just build --no-sccache`
- [ ] `just lint --no-sccache`
- [ ] `just test --summary --no-sccache`
- [ ] Fresh database migrations apply and upgrade from the current deployed
  schema succeeds.
- [ ] Protocol tests prove the current exact-match pair and reject every tested
  mismatch; the coordinated drain/upgrade procedure is exercised.
- [ ] The orchestrator is the sole DB client and GitHub/Slack side-effect owner
  and contains no inline production executor.
- [ ] Loopback benchmark parity and remote block-validation acceptance both
  pass.
- [ ] Lease, event, artifact, restart, drain, and cleanup failure-injection
  checks pass on the deployed topology.
- [ ] Operator runbooks and rollback are reviewed on the actual hosts.

## Follow-Ups

- `0015-resource-aware-admission`: multi-job bin-packing, per-kind quotas, and
  fairness after real fleet utilization is known.
- `0006-aws-cloud-backend`: optional worker provisioner, not an execution path.
- Shared/cross-worker build placement and cache distribution after worker-local
  cache behavior is measured.
- Additional block-validation workers or cross-host shard distribution only if
  one dedicated host proves insufficient; neither is assumed by v25.
- Per-profile benchmark noise floors and profile-sharing ergonomics beyond the
  minimum safe assignment stamp.
