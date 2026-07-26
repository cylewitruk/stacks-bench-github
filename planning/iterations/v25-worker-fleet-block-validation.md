# v25: First Worker Fleet and Dedicated Block Validation

Turn the single-host daemon into an orchestrator plus workers, then place block
validation on the available dedicated Hetzner host while preserving benchmark
behavior on a separate co-located worker.

> **Status:** planned — depends on the compiler-enforced crate topology from
> [v24.1](../archive/completed/0056-compiler-enforced-crate-boundaries.md).
>
> [v24.2](../archive/completed/0058-github-intent-boundaries.md) and
> [v24.3](../archive/completed/0060-slack-snapshot-reporting.md) completed the
> scheduled orchestrator-side continuations before this milestone. They were
> not execution-boundary prerequisites; v24.3 deliberately leaves durable
> event sequencing and replay ownership here.
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
- Persist a versioned, fully resolved task payload when work is enqueued.
  Effective arguments and workload key derive from one resolution pass; any
  symbolic source ref resolves to an immutable commit before the row becomes
  schedulable. Retries reuse the stored payload, and assignments carry its
  canonical hash so daemon-default or branch drift cannot change execution.
- Pre-register workers with server-authorized capabilities and an
  operator-declared measurement profile. Workers dial out with mTLS, report
  resource/dataset facts and version as telemetry, and never receive DB
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
- **mTLS identity; server-owned authorization.** Every worker endpoint requires
  TLS 1.3 mutual X.509 authentication. The client certificate identifies the
  stable worker, while the orchestrator registry—not worker claims—binds its
  allowed capabilities, measurement profile, and drain/enabled state.
- **Worker session is not worker identity.** A fresh process creates a new
  `worker_session_id`. Same-session network reconnect may resume from durable
  ACK state; process restart fences/cleans/requeues and never resumes the old
  attempt.
- **One production execution model.** The inline daemon worker is removed only
  after loopback parity and Phase 4's distributed recovery, cancellation,
  cleanup, and drain gates pass.
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
- **Response loss is idempotent.** Poll returns an existing live offer before
  creating another; accept, heartbeat, events, cancellation, artifact manifest,
  and terminal submission are retry-safe for the same attempt/fence.
- **Block-validation negative result is not infrastructure failure.** Invalid
  blocks produce a completed task with a negative/red result; transport,
  provisioning, timeout, or worker loss are execution failures.
- **Remote baseline safety is fail-closed.** Benchmark comparison uses the
  measurement profile stamped atomically at assignment; absent/mismatched
  profiles disable comparison rather than borrowing another host's baseline.
- **Remote-local cleanup is honest.** The orchestrator records cleanup
  obligations; only the worker that owns local resources can perform them.
- **Remote artifacts use delegated object writes.** Fleet mode requires the
  configured S3-compatible store. Workers receive short-TTL, exact-key
  presigned PUT grants and never object-store credentials; verified manifests
  become visible only through accepted fenced terminals.
- **Single active orchestrator.** v25 does not promise orchestrator HA or leader
  election. Database constraints still fence assignment/terminal races.

## Pinned Fleet Contracts

### Identity and authorization

- Operators provision a private CA, the orchestrator server certificate, and
  one unique client certificate/key per worker. The client certificate carries
  only `urn:sbgh:worker:<worker-uuid>` as its identity URI SAN plus client-auth
  usage; Common Name is not consulted. The daemon terminates TLS 1.3 mTLS
  directly. The worker validates the configured server DNS name, certificate,
  and CA, including over loopback.
- Worker traffic uses a dedicated size/time-limited listener separated from the
  webhook/operator API and exposed only on the worker network/firewall path.
- Certificate issuance is out of band in v25. Normal worker API calls never use
  a shared registration bearer token. Rotation supports an overlap window and
  registry disable/revocation; the Phase 5 runbook exercises it.
- The registry is authoritative for allowed capabilities and
  `measurement_profile`. Reported CPU/memory/storage/dataset facts are validated
  telemetry inside that authorization envelope.
- Every attempt mutation also proves current ownership with
  `{worker_session_id, attempt_id, fencing_generation, opaque lease_token}`.
- A private-repository token is minted on demand only for an active attempt,
  repository-read-only, short-lived, memory-only, and excluded from assignment,
  event, artifact, and log persistence.

### Offer, attempt, lease, and cancellation

```text
queued -> offered -> running -> completed | failed | cancelled
              |          |
              v          v
           expired    cancel_requested -> cancelled
                         |
                         v
                    expired/fenced
```

- `poll` returns the session's existing live offer/attempt or creates one in an
  atomic scheduling transaction. A lost response cannot assign a second unit.
  Each idle session has one outstanding long poll; transport retries use
  bounded exponential backoff with jitter and honor `Retry-After`.
- `accept` is idempotent. Offers and running leases expire by orchestrator time;
  heartbeat renews only the authenticated current session's lease. Validated
  configuration permits at least three heartbeat opportunities per lease TTL.
- Attempt UUID plus a monotonically increasing scheduling-unit fencing
  generation guards every mutation. The opaque lease token is authenticated by
  a daemon-held HMAC key over the worker/session/attempt/fence tuple, scoped to
  one attempt, and reproducible after a lost poll response without plaintext
  token storage.
- Cancellation is durable desired state returned by heartbeat. The first
  orchestrator DB transition wins: an accepted terminal is immutable, while a
  previously committed cancellation rejects later success/failure and permits
  only a cancelled terminal. Non-responsive cancellation expires/fences.
- Terminal acceptance atomically verifies the active fence, a contiguous
  reliable-event prefix through terminal, and the uploaded artifact manifest
  before terminalizing the job or exposing artifact keys.

### Scheduling-unit placement and immutable work

- A benchmark group stores `{worker_id, measurement_profile,
  execution_generation}` on first assignment. Every variant, repeat,
  calibration, carried artifact, and lazily materialized job inherits that
  placement. A block-validation job pins one worker and dataset generation.
- A returning stable worker may continue the current benchmark generation only
  after cleaning its dead session. Moving a partial group to another worker is
  never automatic. An explicit operator recovery starts a new execution
  generation and reruns from the first spec/run; older results remain auditable
  but do not participate in the new comparison.
- Enqueue persists a typed payload version, resolved commit, effective argument
  tokens, workload key, and canonical payload hash. Existing queued rows are
  resolved/backfilled or drained before fleet cutover; assignment never consults
  mutable daemon defaults.

### Reliable-event restart semantics

- Reliable events use a bounded, same-session in-memory resend buffer and strict
  sequence order. Backpressure prevents dropping a reliable phase/terminal
  event; fine progress remains outside that sequence.
- Network reconnect by the same process resumes from the orchestrator's highest
  contiguous durable ACK.
- Worker-process restart creates a new session and fences/cleans/requeues the
  old attempt. v25 has no durable worker outbox or mid-attempt process restart.

### Artifact lifecycle

- Fleet execution requires S3-compatible storage. The orchestrator creates
  short-lived presigned PUTs for unguessable attempt-staging keys with bounded
  size and signed checksum/content headers.
- The worker uploads and submits a typed manifest. The orchestrator verifies
  authenticated attempt/key ownership plus object size/checksum metadata before
  terminal acceptance.
- Only the accepted fenced terminal promotes/attaches keys. Stale or rejected
  staging stays invisible and is GC-eligible after an auditable grace period.

## Phases

### Phase 1: Host Characterization and Minimal Control Plane

**Goal:** Prove that a separate worker can securely register, heartbeat, and
claim compatible stub work while the dedicated host's storage/resource model is
measured rather than assumed.

The host-characterization track starts immediately and runs in parallel with
the control-plane/schema implementation. Its measured filesystem, NUMA,
throughput, and capacity results choose physical layout and shard/concurrency
values; they do not reopen the identity, lease, placement, or recovery
contracts pinned above.

**Scope:**

- Reconcile the `0004`/`0019` protocol contracts around `sbgh-proto`, the
  existing `sbgh-worker`/`sbgh-libvirt` crates, fleet workers, and local
  validation shards.
- Add a dependency-light `sbgh-proto` with the minimum Phase 1 messages:
  register session, heartbeat, long-poll, offer acceptance, stub complete, and
  stub fail.
- Use exact protocol-version matching for v25 and include an orchestrator-issued
  trace/correlation ID in every stub assignment and related log.
- Add orchestrator-owned worker registry, worker-session, scheduling-unit
  placement, attempt/lease/fence, and cancellation schema implementing the
  pinned state machine. Registry policy owns capabilities and measurement
  profile; resource facts cannot elevate authorization.
- Add TLS 1.3 mTLS worker endpoints over a documented secured network path.
  Exercise certificate mismatch, disabled identity, worker-ID mismatch,
  rotation overlap, and revocation. Workers receive no database credentials.
- Add registration, heartbeat, long-poll, and stub execution transport to the
  existing worker library and its new binary.
- Make poll-response loss, duplicate accept, duplicate stub terminal, offer
  expiry, heartbeat renewal, stale session, and fencing behavior deterministic
  before Phase 1 hands out real execution.
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
- [ ] Invalid/expired/revoked client certificates, disabled registry identities,
  and any non-exact protocol version mismatch fail closed with actionable
  diagnostics.
- [ ] A certificate identity cannot register as another worker or claim a
  capability/profile absent from its server-owned registry policy.
- [ ] Losing a poll/accept response returns the same live offer on retry and
  never produces two current attempts for one scheduling unit.
- [ ] A new worker process/session cannot resume or mutate its predecessor's
  live attempt; planned drain and crash/TTL restart paths are distinct.
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
- Worker API mTLS identity/authorization, capability filtering, session,
  response-loss, cancellation ordering, and lease-race tests.
- Loopback worker integration test using the real binary/library construction.
- Hetzner host inventory, reflink, storage-throughput, and connectivity smoke
  report.

### Phase 2: Durable Remote Data Plane and Loopback Benchmark Parity

**Goal:** Run existing benchmark/build jobs through a separate loopback worker
with durable events and artifacts while retaining the inline executor only as a
pre-cutover parity/rollback reference.

**Scope:**

- Add transport and a binary composition root to the v24.1 `sbgh-worker`
  library. Reuse its driver API, recipes, cache/artifact services, and
  `sbgh-libvirt` adapter rather than copying or re-extracting them.
- Keep worker transport/configuration in `sbgh-worker`; keep DB, GitHub/Slack
  clients and credentials, scheduling, reporting, comparison, and webhook
  processing in `sbgh-daemon`.
- Turn the v24.1 owned execution request/task payload into validated worker wire
  DTOs without exposing DB row shapes or backend-only configuration. Enqueue
  persists the resolved commit/effective arguments/workload key/payload hash in
  one immutable contract; assignment and retries reuse it.
- Mint private-repository credentials on demand for an active attempt through
  the mTLS API. Keep them short-lived, repository-read-only, memory-only, and
  outside durable DTOs/logs; workers never receive the GitHub App private key or
  another long-lived credential.
- Implement `0017` before cutover: task-neutral reliable events are
  attempt-scoped, sequence-numbered, durably committed before acknowledgement,
  projected only across a contiguous sequence prefix, idempotent, and replayable
  by a restarted reporter. A stored out-of-order terminal cannot terminalize the
  job or promote artifacts across a sequence gap. Progress/heartbeat delivery
  remains explicitly best-effort where safe.
- Add the bounded same-session reliable resend buffer and ACK-based network
  reconnect. Process restart fences/requeues rather than resuming.
- Add deterministic hidden identity markers and search-before-create
  reconciliation for PR comments, paralleling Check Run `external_id`
  reconciliation and closing the comment create/persist crash window.
- Propagate the assignment trace/correlation ID through worker logs, events,
  artifact manifests, and orchestrator reporting logs.
- Require the S3-compatible artifact backend for fleet mode. Add
  mTLS-authenticated upload-grant/manifest endpoints, short-TTL exact-key
  presigned PUTs, configured object/attempt size limits, signed checksums, and
  store-key results. Upload into an attempt-scoped staging namespace; only an
  accepted fenced terminal may promote/attach a verified manifest. Rejected or
  stale attempts remain invisible and are eligible for GC.
- Stamp `{worker_id, worker_session_id, measurement_profile, attempt_id,
  fencing_generation}` atomically when the orchestrator assigns work.
- Run all benchmark variants/repeats/calibration for a group on the same worker.
- Run a co-located benchmark worker over loopback and record parity against the
  inline path. Do not remove the inline executor until Phase 4's distributed
  cancellation, expiry, restart, cleanup, and drain matrix passes.

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
- [ ] Enqueue-time defaults and symbolic refs are resolved once; daemon default
  or branch drift after enqueue cannot alter assignment or retry payloads.
- [ ] Duplicate/out-of-order reliable events and duplicate terminal submissions
  do not duplicate DB or GitHub/Slack side effects.
- [ ] Projection halts at the first reliable-sequence gap; a later terminal is
  neither accepted nor artifact-promoting until the prefix is contiguous.
- [ ] Killing/restarting the orchestrator during a loopback benchmark replays
  committed task-neutral events and completes reporting without depending on
  the original in-memory channel.
- [ ] A stale/rejected terminal cannot publish staged artifact keys; abandoned
  attempt staging is discoverable for later GC.
- [ ] A worker possesses neither object-store credentials nor persisted
  repository credentials; expired upload grants and attempts cannot write
  outside their exact staging keys.
- [ ] One trace ID correlates assignment, worker execution, reliable events,
  artifacts, and terminal reporting.
- [ ] Losing the initial-comment response or its DB identity write and then
  restarting reconciliation finds the bot-authored marker and leaves exactly
  one PR comment.
- [ ] A benchmark with no compatible measurement profile is comparison-disabled
  rather than matched to an unsafe baseline.
- [ ] Loopback parity is demonstrated without declaring production cutover;
  inline removal remains gated on Phase 4.

**Tests:**

- Protocol event/terminal idempotency and stale-attempt tests.
- Immutable enqueue/assignment fixtures, including default/ref drift and
  pre-cutover queued-row backfill/drain behavior.
- Reporter restart/replay, out-of-order gap, and terminal-prefix tests from the
  durable attempt-event ledger.
- PR-comment marker reconciliation test across create-success/ID-write failure.
- Artifact checksum, size-limit, interrupted-upload, stage/promote, stale
  terminal, expired-grant, wrong-key, and store-key tests against MinIO.
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
  `stacks-inspect` build/cache, immutable canonical-dataset generation
  validation, K verified CoW workspaces, deterministic probe/index-range
  planning, contiguous shard assignment, bounded parallel fan-out, fail-closed
  reduction, process-group cancellation, and cleanup.
- Keep workspace count, shard count, and concurrency as separate values.
- Pin each assignment to a dataset identity containing network, format version,
  covered tip/range, and manifest digest. Refresh and verify a new generation
  separately, switch `current` atomically, and retain pinned generations until
  no attempt/workspace references them.
- Require the exact Phase 1 clone mechanism and startup mutation-isolation
  check. There is no shared-write/symlinked-blob fallback in v25; a host that
  cannot prove isolation loses the capability.
- Partition each validated inclusive probe range with effective shard count
  `min(K, n)`, distributing the remainder across the first shards. Produced
  ranges must be ordered, contiguous, non-overlapping, non-empty, and cover the
  input exactly once.
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
- [ ] Negative completion is accepted only when every shard exits normally in
  `{0, 1}` and at least one returns `1`; cancellation or any setup/timeout/
  process/other-exit failure takes precedence as cancellation or infrastructure
  failure.
- [ ] VM/host process loss, timeout, transport loss, and setup failure remain
  distinguishable execution failures.
- [ ] The same canonical dataset is never writable by a shard; each shard gets
  its own verified CoW workspace pinned to the assignment's immutable dataset
  generation.
- [ ] Benchmark and block-validation progress both persist through the same
  task-neutral event model.

**Tests:**

- Recipe unit tests for exact-cover probe partitioning, remainder distribution,
  mixed exit-code precedence, invalid-block semantics, timeout, cancellation,
  process-group termination, and cleanup.
- Filesystem integration tests for dataset generation identity, atomic refresh,
  retention while pinned, CoW isolation, and capability rejection without
  isolation.
- Store/reporting tests for positive, negative, and infrastructure outcomes.
- Capability-routing test proving block validation reaches only the dedicated
  worker.

### Phase 4: Distributed Liveness, Recovery, and Drain

**Goal:** Make long-running remote work safe under worker restart, network
partition, orchestrator restart, and planned host maintenance.

**Scope:**

- Harden the Phase 1 heartbeat/lease/fence state machine under controllable
  time, process death, network partition, response loss, and orchestrator
  restart; do not introduce a second lifecycle path.
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
- Add same-session network resume from the last orchestrator-acknowledged
  sequence number. A worker-process restart creates a new session and follows
  fence/cleanup/requeue rather than resuming execution.
- Reap attempt-scoped staged artifacts whose attempt is terminal-rejected,
  fenced, or expired without an accepted terminal. GC is idempotent, retains an
  auditable grace period, and never removes artifacts attached to an accepted
  result.
- Add graceful drain: stop claiming, finish or explicitly abort current work,
  satisfy cleanup obligations, and deregister.
- Define orchestrator-restart behavior for outstanding long polls, active
  leases, reporter state, and pending terminal/artifact submissions.
- After the full failure matrix and loopback parity pass, remove the daemon's
  inline execution path and transitional `sbgh-worker`/`sbgh-driver`/
  `sbgh-libvirt` normal dependencies. The daemon thereafter exchanges only
  versioned `sbgh-proto` DTOs with workers.

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
- [ ] Restarting a worker process never resumes its old attempt; it cleans the
  old session and the orchestrator safely retries according to placement policy.
- [ ] Drain prevents new claims and leaves no unreported active local work.
- [ ] The daemon contains no production recipe/driver execution path and no
  normal dependency on `sbgh-worker`, `sbgh-driver`, or `sbgh-libvirt`.

**Tests:**

- Deterministic lease/attempt state-machine tests.
- Kill/restart/network-interruption integration tests with controllable clocks.
- Loopback worker and remote-host drain, same-session reconnect,
  process-restart, response-loss, and cancellation-race smoke tests.

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
- Export and alert on worker heartbeat/lease age, scheduling wait, reliable ACK
  lag/gaps, in-memory resend pressure, attempt-staging bytes/age, and unresolved
  cleanup obligations. Record thresholds in the deployment runbook.
- Document private-CA/server/worker certificate issuance, overlap rotation and
  revocation; drained lease-HMAC-key rotation; worker upgrade compatibility;
  dataset refresh; drain/maintenance; cleanup recovery; rollback; and incident
  triage.
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
- [ ] Disabling a worker identity or removing its certificate authorization
  blocks new sessions/leases, and certificate rotation succeeds without a
  shared bearer credential.
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
- [ ] mTLS identity, server-owned capability/profile authorization, certificate
  rotation/revocation, and attempt-scoped authorization failure tests pass.
- [ ] The orchestrator is the sole DB client and GitHub/Slack side-effect owner
  and contains no inline production executor.
- [ ] Loopback benchmark parity and remote block-validation acceptance both
  pass.
- [ ] Lease, event, artifact, restart, drain, and cleanup failure-injection
  checks pass on the deployed topology.
- [ ] Worker process restart fences/requeues rather than resumes, while
  same-session network reconnect resumes only from the contiguous durable ACK.
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
