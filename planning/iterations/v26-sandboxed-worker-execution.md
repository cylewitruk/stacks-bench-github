# v26: Sandboxed Worker Execution and Block Validation

Continuation of [v25](../archive/completed/0004-worker-fleet.md). Establish one
execution model for fleet jobs: trusted worker control-plane operations stay on
the host, while every repository-built or otherwise untrusted job payload runs
inside a disposable sandbox. Libvirt is the sole production sandbox backend in
v26, and block validation moves from direct host processes into one
resource-bounded VM per assignment.

> **Status:** in_progress — implementation is complete and locally validated;
> independent review and the dedicated-host Phase 3/6 canary validation remain.
>
> v25 shipped the fleet, durable attempt lifecycle, and block validation as a
> second task kind. It intentionally left block validation on a worker-local
> process path. v26 removes that exception without changing fleet scheduling,
> task payloads, result meaning, or orchestrator ownership. Fleet wire protocol
> v2 adds only a bounded, payload-derived offer-requirements projection so the
> worker can enforce local storage/resource admission before accepting a lease;
> the lease/event state machine is unchanged.

## Items

| Item | Role | Status |
| ---- | ---- | ------ |
| `0062-sandboxed-worker-execution` | primary: enforce the worker sandbox invariant | in progress |
| `0063-libvirt-block-validation` | co-primary: run block validation in a resource-profiled VM | in progress |

## Why

Benchmark and build-only assignments currently flow through `Driver` and
`LibvirtDriver`. Block validation instead branches in `sbgh-worker::fleet`,
clones source, invokes Cargo, and executes `stacks-inspect` directly on the
worker host. That has three problems:

1. Repository build scripts and the resulting binary execute with the worker
   daemon's host authority.
2. The worker operator cannot give block validation a clear VM-level CPU,
   memory, placement, and concurrency envelope.
3. The implementation contradicts the documented `{task} × {backend}` model:
   `TaskSpec` excludes block validation and the fleet path explicitly bypasses
   `Driver`.

A single sandbox lifecycle gives operators and maintainers one mental model for
execution, cancellation, forensics, artifact collection, and cleanup. The
invariant is about **untrusted payload execution**, not about moving the trusted
worker daemon or dataset allocator into a guest.

Until v26 cuts over, the v25 direct-host path is not exposed to untrusted or
fork-supplied revisions. Operators either keep `block_validation` disabled or
restrict it to explicitly trusted, reviewed commits.

## Target Shape

```text
orchestrator assignment
        |
        v
sbgh-worker recipe + lease/event supervision       trusted worker process
        |
        v
sbgh-driver TaskSpec + attempt identity             backend-neutral contract
        |
        v
sbgh-libvirt sandbox lifecycle                      trusted host adapter
  ├── resolve one sealed dataset-generation LV
  ├── create K per-attempt LVM-thin snapshots
  ├── define one resource-profiled VM
  ├── attach the K snapshots as raw block devices
  └── collect result, artifacts, and tear down
        |
        v
disposable guest                                    untrusted payload boundary
  ├── build the pinned source revision
  ├── mount K writable snapshot devices
  ├── probe the assigned block range
  ├── run K local stacks-inspect shards
  └── write structured progress/result artifacts
```

One block-validation **assignment owns one VM**. Its K validation shards run as
processes inside that VM and share the VM's resource ceiling. v26 does not turn
shards into fleet-scheduled jobs or create one VM per shard.

The storage model reuses the benchmark substrate rather than the same physical
baseline: benchmarks request one thin snapshot of their selected measurement
origin; block validation requests K snapshots of an exact full-chainstate
generation. Both use the same LVM provision/attach/teardown abstraction.

## Design Rules

- **Sandbox every untrusted payload.** Repository builds, build scripts, and
  produced binaries never execute in the worker process or directly on the
  worker host.
- **No unsafe compatibility fallback.** A missing/unhealthy sandbox disables
  block-validation admission; it never routes the assignment back to v25's
  direct-host executor.
- **Keep the fleet protocol backend-neutral.** Assignments continue to name
  task requirements and logical resources, not libvirt domains, host paths, or
  VM image details. Libvirt is the only supported production backend in v26,
  not a permanent protocol constraint.
- **Keep the trusted host surface narrow.** The worker/libvirt adapter may
  perform fixed infrastructure operations: dataset verification, LVM-thin
  snapshot allocation, domain definition, block-device attachment, artifact
  staging, and cleanup. It must not accept arbitrary host commands from an
  assignment.
- **One assignment, one sandbox.** A block-validation attempt has one disposable
  domain and one aggregate resource profile. Shard parallelism is bounded
  inside that domain.
- **One LVM-thin snapshot per shard.** The host creates K writable snapshots
  from one sealed full-chainstate origin and attaches those snapshots to the
  assignment VM. A shard receives one snapshot; no shard writes another
  shard's filesystem.
- **Canonical data is never guest-attached.** A published dataset generation
  maps to one exact, immutable origin LV. Only its attempt-scoped snapshots are
  attached to the VM. Updates create and publish a new generation; they never
  mutate a published origin in place.
- **One generation contract, one eventual producer.** v26 consumes sealed
  generations compatible with
  [`0052-managed-stacks-node-chainstate-producer`](../design/0052-managed-stacks-node-chainstate-producer.md).
  Until `0052` ships, operators may mint them manually, but must use the same
  LV tags, immutability, manifest, coverage, and identity contract. v26 does
  not create a competing producer format.
- **Shared mechanism does not imply shared baseline.** Benchmark and
  block-validation profiles may select different chainstate origins, sizes,
  freshness, and retention. They share snapshot lifecycle code and safety
  invariants.
- **Identity is attempt-scoped.** Domain names, directories, attachments, and
  normal cleanup include `job_id`, `attempt_id`, and fencing generation where
  relevant. Stale cleanup must not destroy a newer attempt's sandbox.
- **Resource policy is local and authoritative.** The worker operator configures
  block-validation vCPU, memory, CPU placement, maximum shard concurrency, and
  maximum concurrent assignments, plus storage/I/O controls where the selected
  host attachment can enforce them. Unsupported controls fail configuration
  rather than becoming advisory. An assignment outside those limits is declined
  before acceptance; it is never silently clamped after leasing.
- **Pool health and assignment capacity are separate.** One fixed
  data/metadata floor shared with benchmark snapshots rejects an already
  near-full or mis-provisioned thin pool. It is not per-assignment write
  prediction and never scales with K. Current validation rolls block changes
  back and produces only MB-scale SQLite WAL/SHM divergence; if that execution
  contract changes, storage policy must be revisited explicitly. Shard count,
  vCPU/memory, concurrency, and device-attachment limits remain fail-closed
  worker resource policy.
- **Guest block-device identity is explicit.** Snapshot devices use stable
  per-shard serials/mappings rather than kernel enumeration order. The XFS
  duplicate-UUID mount strategy is explicit and tested; v26 initially uses
  stable virtio-scsi identities plus `mount -o nouuid`.
- **Task contracts stay typed.** `sbgh-driver` owns dependency-light execution
  input/output types. `sbgh-worker` explicitly converts between `sbgh-proto`
  wire DTOs and driver types; `sbgh-driver` does not gain a protocol dependency.
- **Results fail closed.** Missing, malformed, identity-mismatched, partial, or
  contradictory guest output is infrastructure failure—not a successful or
  negative validation result. The host partitions and counts the trusted
  assignment range in global coordinates; guest epoch-local command
  translation never controls terminal coverage. Host ingestion follows no
  guest-controlled final symlink and verifies terminal files resolve beneath
  the results share before parsing, cache publication, or artifact upload.
- **Cancellation uses the common lifecycle.** Lease loss, operator
  cancellation, timeout, worker shutdown, and guest failure converge through
  driver teardown and attempt-scoped cleanup. If domain stop/undefine cannot be
  proven, all backing resources are retained and terminal publication is
  withheld until authoritative cleanup succeeds.
- **Secrets stay out of the guest where possible.** Source preparation reuses
  the trusted libvirt source-disk path. Worker mTLS material and fleet lease
  tokens are never mounted into the VM. Any unavoidable guest credential is
  short-lived, least-privilege, and excluded from logs and artifacts.
- **Network access is explicit.** The block-validation profile defaults to no
  general guest egress during validation. Build-time dependency access, if
  required, uses an explicit restricted profile and is not inherited from the
  worker control plane.
- **Reuse the build cache.** `stacks-inspect` uses the shipped benchmark
  `BinaryCacheStore`/`BuildFingerprint` flow, with an executable-kind/target
  discriminator. Cache hits are attached for guest execution; cache misses are
  built in the guest and published by the trusted host adapter. Compiler-cache
  state is guest-local on the disposable boot overlay; no persistent writable
  host cache crosses attempt boundaries.
- **Compiler-owned ratchet.** Production `sbgh-worker` code loses direct process
  spawning once cutover completes; executable host integration remains in
  concrete adapters such as `sbgh-libvirt`.

## Scope

- Extend the driver contract with typed block-validation input and output.
- Carry attempt identity into the execution backend and make cleanup
  attempt-safe.
- Extract the common libvirt provision/start/observe/forensics/teardown
  lifecycle from benchmark-specific orchestration without changing benchmark
  behavior.
- Add operator-owned, task-specific libvirt execution profiles.
- Add a block-validation guest plan and structured progress/result contract.
- Generalize the existing benchmark `ChainstateSnapshot` lifecycle into an
  attempt-scoped snapshot set: one snapshot for benchmark, K for block
  validation.
- Resolve a pinned block-validation dataset generation to one exact sealed
  full-chainstate origin LV and attach only its K snapshots.
- Build and execute `stacks-inspect` inside the guest.
- Reuse the existing binary-cache mechanism for `stacks-inspect` hits,
  guest-built misses, and publication.
- Preserve v25 partitioning, invalid-block parsing, fail-closed reduction,
  artifact keys, progress semantics, terminal result schema, and reporting.
- Remove the direct block-validation execution branch and its task-local
  commit-only source/binary caches from `sbgh-worker`; keep the shared
  driver-owned cache ports.
- Update example worker configuration, architecture docs, operations runbooks,
  and package-DAG enforcement.

**Non-goals:** changing the fleet lease/event state machine beyond the additive
v2 offer-admission summary; distributing one validation across workers; one VM
per shard; adding containers, Firecracker, or Kubernetes; making libvirt part
of the orchestrator API; copying the multi-TB dataset into every boot image;
exposing the canonical dataset read-write; running the worker daemon in a VM;
arbitrary command execution supplied by the orchestrator; changing benchmark
measurement semantics; synchronizing or producing chainstate generations
across workers; multi-orchestrator HA.

## Configuration and Admission

The implemented TOML ownership is explicit:

```toml
[libvirt.lvm]
min_data_free_percent = 5
min_metadata_free_percent = 5

[libvirt.block_validation]
vcpus = 48
memory_bytes = 206158430208
cpu_set = "0-47"
max_shards = 48
max_concurrency = 48
max_parallel_jobs = 1
results_tmpfs_mib = 5120
network = "restricted-build"
chain_config = "/etc/sbgh/worker/stacks-inspect-mainnet.toml"

[libvirt.block_validation.dataset]
generation = "mainnet-2026-07"
network = "mainnet"
format_version = "stacks-core-v3"
covered_start = 0
covered_end = 8020465
manifest_sha256 = "<sha256>"
origin_lv = "mainnet-full-2026-07"
manifest_path = "/srv/sbgh/datasets/mainnet-2026-07/.sbgh-dataset-manifest.json"
snapshot_prefix = "sbgh-block"
mount_options = ["nouuid", "noatime"]
required_origin_tags = ["sbgh_sealed", "sbgh_validated"]
```

Paths and libvirt details remain worker-local. Registration advertises logical
capability and resource facts; server authorization remains authoritative.
Fleet protocol v2 puts only `task kind + dataset identity + requested shard
count + concurrency` on `WorkOffer`; the worker verifies that bounded summary
before acceptance, then verifies the accepted payload projects to the same
requirements. Before accepting an offer, the worker verifies that:

- the configured backend supports the task;
- the task has a matching local execution profile;
- the pinned dataset identity maps to the exact configured, sealed origin LV;
- requested shards/concurrency fit the local profile; and
- the shared fixed pool-health floor passes and the host can attach the
  requested snapshot count within its configured device ceiling.

An unsupported offer is declined without creating an attempt sandbox.

## Phases

### Phase 1: Typed Sandbox Contract and Attempt Identity

**Goal:** Make block validation a real driver task before changing where it
runs.

**Scope:**

- Add pure `BlockValidationTaskSpec`, range/epoch/dataset identity, and typed
  block-validation output types to `sbgh-driver`.
- Add `TaskSpec::BlockValidation` and a typed task-output field to
  `DriverOutcome`; retain the backend forensics summary separately.
- Extend `TaskContext` with `attempt_id` and fencing generation. Do not pass the
  lease token or worker credentials into the backend.
- Add explicit `sbgh-proto` ↔ `sbgh-driver` conversion in `sbgh-worker`.
- Define backend support/admission so capability registration cannot advertise
  a task with no configured driver/profile.
- Narrow normal teardown to an attempt identity; retain a deliberately named
  job-wide recovery sweep only where the orchestrator has proven no newer
  attempt can be active.

**Status:**

- [x] Contract implementation
- [x] Unit and composition tests
- [ ] Reviewed
- [x] Validated locally

**Acceptance & Validation:**

- [x] A fake driver receives the complete block-validation spec through the
  same worker execution entry point as benchmark/build-only.
- [x] Wire types do not enter `sbgh-driver`; package-DAG checks remain green.
- [x] Cleanup tests prove an old attempt cannot address a newer attempt's
  domain or workspace.
- [x] Unsupported or over-limit block-validation offers are declined before
  acceptance.

### Phase 2: Reusable Libvirt Sandbox Lifecycle

**Goal:** Separate backend lifecycle from benchmark-specific guest work without
changing existing benchmark behavior.

**Scope:**

- Factor domain naming, boot disk, source input, cloud-init/result media,
  domain XML, start/poll/cancel, forensics, and teardown into an internal
  sandbox lifecycle.
- Keep benchmark and build-only plans as typed users of that lifecycle.
- Make CPU, memory, CPU-set, network, block-device set, timeout, and artifact
  policy inputs explicit and validated.
- Generalize domain XML from one hard-coded chainstate disk to a typed list of
  snapshot devices with deterministic per-shard serials. Use a scalable
  virtio-scsi controller for the K-device block-validation case.
- Ensure domain XML applies the selected profile and exposes no undeclared host
  devices or paths.
- Preserve the `sbgh-libvirt` crate boundary and `Shell`-based testability.

**Status:**

- [x] Lifecycle refactor
- [x] Existing behavior tests remain green
- [ ] Reviewed
- [x] Validated locally

**Acceptance & Validation:**

- [x] Existing benchmark/build fixtures, forensics, cancellation, cache, and
  cleanup tests are unchanged or have byte-equivalent expectations.
- [x] Domain XML tests prove the requested vCPU, memory, CPU-set, network, and
  exact block-device attachment policy.
- [x] Every domain/resource name is attempt-scoped and deterministic.
- [x] A setup failure and a cancellation both pass through the same idempotent
  teardown state machine.

### Phase 3: Shared LVM-Thin Snapshot Substrate

**Goal:** Reuse benchmark chainstate provisioning while giving each validation
shard its own writable snapshot of one exact, immutable dataset generation.

**Scope:**

- Generalize the existing benchmark `ChainstateSnapshot` into an
  attempt-scoped `ChainstateSnapshotSet`/provider. Benchmark requests one
  snapshot; block validation requests K.
- Resolve `DatasetIdentity.generation` to one exact configured full-chainstate
  origin LV. Do not use "lexicographically latest prefix" selection for a
  pinned block-validation assignment.
- Require the origin to be a sealed, verified generation: LVM permission/tags
  and the read-only mounted manifest must agree with the registered dataset
  identity, covered range, and digest before capability advertisement.
  [`0052`](../design/0052-managed-stacks-node-chainstate-producer.md) is the
  canonical future producer; manually provisioned origins use that same
  contract until it ships. In-place mutation of a published origin is
  forbidden now.
- Provision all K thin snapshots before domain start, with names containing
  attempt identity and shard index. A partial failure removes the successful
  prefix before returning.
- Attach each snapshot as a raw block device with a stable serial, then mount
  it in the guest using the declared XFS duplicate-UUID strategy
  (`-o nouuid`). Probe against one shard snapshot; never attach the origin LV.
- Keep virtiofs limited to the attempt-scoped control/result surface;
  chainstate and persistent writable compiler-cache state do not cross
  virtiofs.
- Apply the same fixed `Data%`/`Meta%` near-full guard before benchmark and
  block-validation thin snapshots. The floor validates pool setup/health; it
  does not reserve or predict K-dependent write divergence.
- On the real host, run the two-snapshot read-write/isolation smoke once, then
  use an end-to-end canary at the intended K to validate attachment and choose
  a conservative initial resource profile. Treat throughput telemetry as
  operational tuning, not a correctness or release gate.
- Record origin generation, origin LV, snapshot identities, and shard/device
  mapping in forensics without leaking host paths into the fleet protocol.

**Status:**

- [x] Shared snapshot-set provider
- [x] Exact-generation resolution and shared pool-health admission
- [x] Multi-device domain/guest mounting
- [ ] One-time real-host read-write/isolation smoke
- [x] Failure and cleanup tests
- [ ] Reviewed
- [ ] Validated

**Acceptance & Validation:**

- [x] Existing benchmark execution still provisions one thin snapshot with
  unchanged base selection/teardown and the shared pre-allocation pool-health
  guard.
- [x] A block assignment provisions exactly K distinct snapshots from the
  payload's exact generation LV; no "latest" fallback is possible.
- [x] The guest sees stable shard-to-device mappings, mounts all K XFS
  snapshots concurrently, and never sees the origin LV.
- [ ] Writes to one shard snapshot change neither the origin nor another shard
  snapshot.
- [ ] Startup refuses the capability when the origin is missing/unverified,
  the shared thin-pool health floor is not met, XFS duplicate-UUID mounting is
  unsupported, or K devices cannot be attached safely.
- [x] Origin permission/tags and its read-only manifest bind the advertised
  dataset generation, covered range, and digest to that exact LV.
- [ ] A manually provisioned generation and a `0052`-produced fixture satisfy
  the same dataset-generation validator and resolve to the same runtime model.
- [ ] Partial snapshot/attach/domain failures leave no accepted job with
  untracked LVs or devices.
- [x] Cleanup is idempotent across worker restart and an already-removed
  domain/snapshot set.
- [ ] Two explicit read-write snapshots mount with `nouuid`; writing either
  changes neither the sealed origin nor its peer, and cleanup leaves no LV.
- [ ] One canary at the configured K completes within the task timeout; K is
  reduced if real runtime or host telemetry shows an unsuitable initial
  resource profile.

### Phase 4: Block-Validation Guest Plan

**Goal:** Build, probe, shard, and reduce block validation entirely inside the
VM.

**Scope:**

- Reuse the trusted source-disk preparation path; do not execute repository
  hooks, build scripts, or produced binaries on the host.
- Reuse the shipped
  [`0025`](../archive/completed/0025-baseline-binary-cache.md) /
  [`0031`](../archive/completed/0031-reusable-build-jobs.md)
  `BinaryCacheStore` and `BuildFingerprint` mechanism. Add an executable-kind
  and target discriminator so `stacks-inspect` cannot collide with
  `stacks-node`.
- On a cache hit, materialize the cached binary through the same minimal source
  disk path and execute it only in the guest. On a miss, build in the guest and
  publish through the existing cache port; host code never executes the
  artifact.
- Render a typed guest plan containing the stable shard-device map, mount
  options, epoch, inclusive range, shard plan/limits, timeout, and output
  locations.
- Run K shard processes inside the VM under the aggregate profile ceiling.
- Emit atomic structured progress and a versioned terminal result manifest.
- Validate result identity, exact range coverage, counts, diagnostic shape, and
  exit classification before mapping it to the v25 terminal DTO.

**Status:**

- [x] Guest plan and runner
- [x] Typed result/progress reader
- [x] Parser/reducer regression coverage
- [ ] Reviewed
- [ ] Validated

**Acceptance & Validation:**

- [x] No Cargo build, repository-produced executable, or `stacks-inspect`
  process runs on the worker host.
- [ ] A cache hit skips Cargo and executes the cached `stacks-inspect` only in
  the VM; a miss builds in the VM and becomes a subsequent hit.
- [x] Cache fingerprints distinguish executable kind/target and include the
  repository, commit, toolchain/build inputs, and image environment. No
  parallel block-validation cache exists.
- [x] Existing inclusive, gap-free, exactly-once partition properties remain
  proven, including small ranges and integer boundaries.
- [x] A negative result is accepted only when all shards terminate normally
  and at least one typed invalid-block diagnostic is present.
- [x] Missing, stale, malformed, partial, or identity-mismatched guest output
  fails as infrastructure error.
- [x] Progress remains monotonic and terminal delivery retains v25 fencing and
  deduplication behavior.
- [x] Worker mTLS keys, lease tokens, and repository credentials do not appear
  in guest media, console logs, or uploaded artifacts.

### Phase 5: Fleet Cutover and Enforcement

**Goal:** Remove the host-execution exception and make regression difficult.

**Scope:**

- Route benchmark, build-only, and block-validation payloads through one
  driver-backed assignment execution path.
- Delete direct block-validation process supervision, its commit-only
  `source_cache`/`binary_cache`, and task-specific cleanup branching from
  `sbgh-worker`; retain the shared driver cache ports.
- Remove Tokio's `process` feature from the production `sbgh-worker`
  dependency surface if no other production use remains.
- Require a healthy libvirt block-validation profile before advertising the
  capability.
- Preserve existing protocol payloads, artifact names, result persistence,
  reports, cancellation precedence, and retry/recovery behavior.
- Update configuration examples and operator migration/rollback instructions.

**Status:**

- [x] Cutover
- [x] Dead path removed
- [x] Boundary ratchets
- [x] Documentation
- [ ] Reviewed
- [ ] Validated

**Acceptance & Validation:**

- [x] Production `sbgh-worker` contains no direct process-spawn path.
- [x] `TaskPayload::BlockValidation` no longer bypasses `Driver`.
- [x] No configuration or error path can fall back from sandbox execution to
  direct host execution.
- [x] Package metadata and the documented crate DAG agree under all features.
- [x] A worker cannot register `block_validation` without a usable sandbox
  backend, resource profile, exact sealed dataset-generation LV, a passing
  shared thin-pool health check, and successful preflight.
- [x] Existing benchmark and build-only end-to-end tests remain green.
- [x] Existing v25 block-validation API, queue, fleet, persistence, report, and
  artifact tests remain green.

### Phase 6: Real-Host Security and Recovery Validation

**Goal:** Prove the boundary on the dedicated worker before production cutover.

**Scope:**

- Validate the block-validation execution profile, exact origin resolution,
  K-snapshot attachment/mounting, and shared binary-cache hit/miss paths on the
  actual libvirt host.
- Exercise success, typed invalid-block failure, infrastructure failure,
  timeout, cancellation, worker loss, and orchestrator reconnect.
- Verify domain, block-device, mount, snapshot-LV, cache, and artifact cleanup.
- Complete v25's deferred parser gate: capture real `stacks-inspect` success
  output and a controlled invalid-block diagnostic from disposable snapshots,
  then prove the production parser classifies both exactly as intended.
- Capture end-to-end canary resource use to tune vCPU/memory/shard settings;
  throughput characterization is not a storage-capacity gate.
- Run a canary deployment and retain the v25 host-process binary only as a
  release rollback artifact. If that release is restored, block validation
  remains disabled rather than returning to direct host execution.

**Status:**

- [ ] Host preflight
- [ ] Failure injection
- [ ] Resource characterization
- [ ] Canary and rollback rehearsal
- [ ] Reviewed
- [ ] Validated

**Acceptance & Validation:**

- [ ] `virsh`/domain inspection confirms the configured vCPU, memory, CPU-set,
  network, virtio-scsi controller, stable shard serials, and exact K snapshot
  devices.
- [ ] No scenario leaves a running domain, mounted shard, or attempt snapshot
  LV after cleanup completes.
- [ ] The sealed origin LV remains unchanged before and after every smoke.
- [ ] The shared near-full pool guard passes, the two-snapshot smoke proves
  read-write isolation, and the configured-K canary completes without an
  attachment, timeout, or cleanup failure.
- [ ] Cancellation and lease loss cannot publish a successful terminal result.
- [ ] A successful real validation produces the same logical result and
  artifact set as the v25 implementation for the same commit/dataset/range.
- [ ] Real positive output and a controlled real invalid-block output are saved
  as parser fixtures; the latter becomes a completed negative result, while
  malformed or non-validation failures remain infrastructure errors.
- [ ] The second identical run is a `stacks-inspect` binary-cache hit and does
  not rebuild.
- [ ] The operator runbook records configuration, preflight, drain, cleanup,
  certificate/secrets boundaries, rollback, and forensic commands.

## Final Validation

- `just build`
- `just lint`
- `just test`
- Package-DAG and unused-dependency checks pass under all features.
- Driver contract and fake-driver composition tests cover every task kind.
- Libvirt XML/lifecycle tests cover every resource field, snapshot device,
  stable serial, and teardown path.
- Property tests cover partitioning and reduction invariants.
- Docker-backed persistence/protocol/reporting tests remain green.
- A libvirt-host smoke proves successful and negative block validation,
  cache miss/hit, cancellation, timeout, restart cleanup, exact-generation
  selection, origin immutability, two-snapshot isolation, configured-K
  attachment, shared thin-pool health, and resource enforcement.
- Real `stacks-inspect` output closes the parser-validation gate deferred from
  v25.
- Documentation and example configurations contain no direct-host
  block-validation execution path.

### Local implementation record (2026-07-27)

- `just build --no-sccache` — passed.
- `just lint --no-sccache` — passed, including docs/registry, package DAG,
  cargo-machete, Clippy, and rustfmt.
- `just test --summary --no-sccache` — 854 passed, 1 environment-gated test
  skipped.
- No persistent writable compiler-cache directory is exposed to guests.
  sccache is confined to the disposable boot overlay; the host-mediated binary
  cache is the only cross-attempt build-reuse channel.
- Block-validation coverage is partitioned and counted from the host-authored
  global assignment range; epoch-local guest CLI translation cannot reduce the
  accepted coverage.
- Snapshot creation explicitly requests read-write permission, benchmark and
  block-validation snapshots share a fixed near-full pool guard, and teardown
  retains all backing resources whenever domain stop/undefine cannot be
  proven.
- K-scaled metadata/write-divergence reserves and synthetic `fio`
  qualification were removed after confirming validation's rollback-based,
  MB-scale write profile. The host gate is now one two-snapshot isolation smoke
  plus a real configured-K canary; throughput remains operational tuning.
- The libvirt composition suite covers a warm `stacks-inspect` cache entry,
  exact sealed-origin admission, two attempt snapshots, VM-only execution,
  monotonic progress, typed reduction, artifact archival, and reverse cleanup.
- Guest-controlled terminal files are opened without following symlinks,
  confined beneath the result share, bounded before parsing or archival, and
  covered by escape and oversized-output regression tests.
- Block-validation preflight verifies the fixed host executables, golden-image
  readability, configured libvirt network, runtime directories, exact sealed
  origin, and shared thin-pool health before capability registration.
- Production `sbgh-worker` has no process-spawn implementation or Tokio
  `process` feature; all three shipped task kinds compose through `Driver`.
- The dedicated-host libvirt/LVM isolation smoke, real `stacks-inspect`
  parser-fixture, failure-injection, and canary checks remain
  the explicit Phase 3/6 gate before this iteration can be reviewed, validated,
  archived, and marked shipped.
- `sbgh-worker --preflight-only` and
  `scripts/qualify-block-validation-lvm.sh` provide the executable host-side
  preflight and two-snapshot isolation smoke; neither was executed
  against a real LVM/libvirt host in this environment.

## Rollout and Rollback

Roll out to the dedicated block-validation worker first:

1. Drain the worker and complete outstanding cleanup obligations.
2. Install/verify the v26 golden image, sealed full-chainstate origin LV,
   virtio-scsi/XFS mount support, and shared thin-pool health floor.
3. Keep the worker stopped/drained and run `sbgh-worker --config
   /etc/sbgh/worker/block-validation.toml --preflight-only`; do not register
   the capability until it passes.
4. Enable the capability for a bounded canary range.
5. Exercise cancellation and cleanup, then run a representative full task.
6. Expand only after origin immutability, pool health, cache, and resource
   checks remain clean.

Rollback drains the worker, disables the capability, cleans v26
attempt-scoped resources, and may restore the prior worker binary/config with
block validation still disabled. Rollback does not return validation jobs to
direct host execution.

## Follow-Ups

- `0052-managed-stacks-node-chainstate-producer` replaces manual generation
  provisioning when it ships; v26 already consumes its tags/manifest/identity
  contract.
- Other sandbox backends may implement the same driver contract later; v26 does
  not pre-design or require them.
- Cross-worker shard scheduling remains a separate scale-out item if one
  resource-profiled VM on the dedicated host proves insufficient.
- Stronger guest egress isolation or a dedicated dependency proxy may be
  extracted after measuring the initial restricted-build profile.
