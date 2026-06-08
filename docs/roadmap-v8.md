# Roadmap 8 — The task-agnostic `Driver` seam (local execution substrate)

> **Read the Scope pivot below first.** This doc was originally "Pluggable
> execution backends (AWS driver)." After the [block-validation
> sketch](./block-validation-taskspec.md) and the Hetzner-vs-AWS analysis, its
> **live scope shrank to Phase 1** — extracting the task-agnostic local `Driver`
> seam. Cloud/AWS is **parked** (Phases 0, 3–6, deferred), and the distribution
> layer moved to [roadmap-v9.md](./roadmap-v9.md). Don't take the original
> AWS-centric phrasing in the deferred sections as the current plan.

Extracts the **local execution-substrate abstraction** — a **task-agnostic
`Driver`** (`run_task(TaskSpec)`) — so any task kind (benchmark today,
block-validation next) runs through one seam on whatever local backend a host
provides (libvirt today; a reflink-fan-out driver for block-val). This is the
**backend axis** of [roadmap-v6.md](./roadmap-v6.md)'s three-layer model,
*local to one host*; scheduling work *across* hosts is [roadmap-v9.md](./roadmap-v9.md).

> **Goal (live):** finish the half-realized seam into a **task-agnostic `Driver`
> trait**. Today the runner drives a job through backend-neutral contracts
> (`Recipe` / `EventSink` / a string-keyed results `summary`), but the only
> `Recipe` impl hardwires `LibvirtDriver`, and that driver's entry point
> (`run_benchmark`) bakes in *benchmark* specifics. Phase 1 makes it
> `run_task(TaskSpec)` — the task supplies its in-VM script, phases, resource
> shape, and result artifacts — **behavior-preserving** (bench-on-libvirt stays
> the only live path). The entire job/queue/reporting/DB/vs-baseline stack reuses
> **unchanged**, and both the v9 fleet and any future cloud worker stand on this
> seam. *(Original goal also added a backend selector + an AWS driver — both now
> parked; see Scope pivot.)*

Process unchanged: Opus implements, Codex reviews, Opus fixes.

> **Sibling docs:** the Check-Run product surface is [roadmap-v4.md](./roadmap-v4.md);
> execution architecture (concurrency, worker/reporter split) is
> [roadmap-v5.md](./roadmap-v5.md); the **multi-task platform** (the *task* axis —
> `Recipe` kinds, block-validation) is [roadmap-v6.md](./roadmap-v6.md);
> change-impact reporting is [roadmap-v7.md](./roadmap-v7.md); the **distributed
> worker fleet** (the distribution axis) is [roadmap-v9.md](./roadmap-v9.md). This
> doc owns the **local `Driver` seam**.

## Scope pivot (2026-06) — v8 is now the **local `Driver` seam**; the fleet moved to v9

After the block-validation sketch and the Hetzner-vs-AWS analysis, the near-term
plan changed. **Cloud-backed ephemeral drivers are parked.** The path forward is
a **distributed worker fleet** (dedicated bare-metal workers declaring
capabilities — bench on pinned boxes, block-validation on big-local-NVMe boxes),
with cloud-ephemeral instances kept in mind as a *later* way to **provision
workers**, not a separate execution path.

That splits this doc's original scope across **two orthogonal seams**:

- **v8 = the local execution-substrate abstraction** — `Driver::run_task(TaskSpec)`,
  local to one host. **Phase 1 is the live, green-lit work** and the foundation
  for everything below. Phase 2 is **superseded by v9** — its one surviving
  requirement (record *where a job ran*, for baseline trust) moves to v9 Phase 4
  as an operator-declared `measurement_profile`.
- **[roadmap-v9.md](./roadmap-v9.md) = the distribution layer** — scheduling work
  onto remote capable machines (worker registry, capabilities, capability-matched
  claim, a thin pull-based worker↔orchestrator API, leases, remote event/artifact
  shipping). Per-task-kind *routing* becomes **capability matching** there.

**AWS phases (0, 3–6) are deferred.** They return later, reframed as a
**worker-provisioner** under v9 (AWS becomes "a way to spin up a temporary
worker"), gated on the cost/variance/hydration data that never justified them up
front. They're left intact below as the design record, marked **Deferred**.

## Why

A second backend is worth it for two reasons:

1. **Elasticity + isolation.** One ephemeral instance per job eliminates the
   **self-inflicted contention from co-located jobs** — no `cpu_sets` /
   `isolcpus` / IRQ-affinity (roadmap-v5's concurrency work exists *because*
   libvirt jobs share one physical host). Concurrency becomes "launch N
   instances," bounded by EC2 limits / cost rather than cores. It does **not**
   buy perfect isolation — shared-tenancy EC2 still has noisy neighbors and a
   shared Nitro/EBS/network substrate — so whether the variance floor actually
   *beats* the pinned libvirt host (~0.6% CV) is exactly what **Phase 0 must
   prove** (and dedicated tenancy is the lever if it doesn't).
2. **No owned hardware.** The libvirt backend needs a dedicated host with LVM,
   libvirt, and a maintained golden image. AWS trades that for per-run spend +
   instance spin-up latency.

### Two axes — and how they map to roadmap-v6's three layers

v6 already draws the layers; v8 makes the middle one's *backend* pluggable:

```text
  Platform        — App, queue, lifecycle, coordinator/worker/reporter,    ← task- AND backend-agnostic
                    reporting (checks/comments), config, DB                   (untouched by v8)
  Stacks substrate — VM runtime: provision/start/poll/teardown/cleanup,     ← the DRIVER layer
                    chainstate clone, git mirror, results pull                 (v8 makes this backend-pluggable)
  Task kinds      — Recipe impls: bench · block-validation · (future)       ← the TASK layer
                    each: in-VM command + phases + resource shape + result    (v6 adds kinds; backend-agnostic)
```

The **`Recipe` is the task axis** (v6); the **`Driver` is the backend axis of
the substrate layer** (this doc). They're orthogonal — `{bench, block-validation}
× {libvirt, AWS}` is a **matrix**, not N×M bespoke drivers, *iff* the `Driver` is
**task-agnostic**.

### The architecture already anticipates this

The runner does **not** know about libvirt. It drives a job through
backend-neutral contracts:

- **`Recipe` trait** ([recipe.rs](../crates/sbgh-daemon/src/recipe.rs)) —
  `execute(ctx, sink, cancel) -> Outcome`; `TaskContext = {job_id, repository,
  commit}`, `TaskOutcome = {status, summary: JSON}`. No VM concepts.
- **`EventSink` / `WorkerEvent`** ([events.rs](../crates/sbgh-daemon/src/events.rs))
  — opaque phase/progress events.
- **A string-keyed results `summary`** — the driver emits a blob keyed by
  `archive_dir` (+ `finish_reason`, `last_phase`, `console_tail`); consumers read
  string keys and touch **no** libvirt type. (One key, `run_json_archived_path`,
  is *bench-task*-specific — see the task-agnosticism note below.)

So the upper stack — queue, claim/lifecycle, GitHub reporting, vs-baseline +
confidence, the DB — is already substrate-agnostic. But the seam is
**half-realized on *both* axes**:

- **Backend:** `BenchRecipe::execute` constructs `LibvirtDriver` directly
  ([bench_recipe.rs:73](../crates/sbgh-daemon/src/bench_recipe.rs)), the runner
  names `BenchRecipe` concretely and reaches past the seam to libvirt's
  `cleanup_by_job_id` ([runner.rs:272](../crates/sbgh-daemon/src/runner.rs)), and
  config is libvirt/LVM-only with no backend selector.
- **Task:** the driver's entry point is `run_benchmark(bench_args)` — it bakes in
  the *bench* cloud-init/script ([cloudinit.rs](../crates/sbgh-daemon/src/libvirt/cloudinit.rs))
  and the bench phase model. A naive `Driver` extraction would inherit that and
  yield a backend that only runs **benchmarks** — forcing a `validate()` method
  (or a whole parallel driver) per task. **Phase 1 fixes this** by making the
  trait `run_task(task_spec)`, with the task supplying the in-VM script, phases,
  resource shape, and result artifacts.

## The libvirt ↔ AWS mapping (1:1 per submodule)

Each `libvirt/` submodule has a natural AWS analog — a substrate swap, not a
redesign:

| libvirt today | AWS analog |
| ---- | ---- |
| LVM-thin chainstate snapshot (`lvm.rs`) | **EBS volume from snapshot** (+ Fast Snapshot Restore — see Risks) |
| qcow2 overlay on golden image (`boot.rs`) | EBS root from a baked **AMI** |
| cloud-init seed ISO (`cloudinit.rs`: generic VM shell + task script) | EC2 **user-data** (same split — generic shell ports; the task script comes from the `TaskSpec`) |
| `virsh define/start/destroy` (`virsh.rs`) | `RunInstances` / `TerminateInstances` |
| build-then-bench redefine (power-off, new mem/vcpus) | **stop → ModifyInstanceAttribute(type) → start** (EBS root persists) |
| `domstate` poll loop (`driver.rs`) | instance-state / SSM poll |
| virtio-fs tmpfs results (`tmpfs.rs`) | pull run.json via **SSM / SSH / S3** |
| host sccache via virtio-fs (`paths.sccache_dir`) | **S3-backed sccache** (`SCCACHE_BUCKET`) — shared across instances |
| git mirror (`git_mirror.rs`) | reuse as-is (host-side), or clone on the instance |
| forensics archiving (`forensics.rs`) | reuse once artifacts are local |

## Risks / things that affect *benchmark trustworthiness*

The whole point is trustworthy numbers, so these are first-class, not footnotes:

- **EBS lazy-load.** A volume freshly restored from a snapshot lazy-loads blocks
  from S3 on first touch — the *first* run reading a block is slower. For
  consistency, enable **Fast Snapshot Restore** on the chainstate snapshot, or
  pre-warm the volume, before the measured replay. (Parallels the chainstate
  warmup the bench already does.) **Phase 0 must quantify this.**
- **Two-phase instance sizing.** libvirt redefines the *same* VM from build
  (16G/4vcpu) to bench (prod-shape) so the measured phase matches production.
  EC2 can't resize a *running* instance; the analog is stop → modify type →
  start (EBS root persists the built binary). **Phase 0 confirms** this
  preserves the build artifact + acceptable stop/start latency.
- **Spot interruptions** (if used) map to the existing `Terminal::Aborted` /
  cancelled path — re-triggerable, not a failure.
- **Instance-type stability.** Pin a fixed instance type (and ideally a fixed
  CPU generation / dedicated tenancy) so runs are comparable; note any
  noisy-neighbor exposure on shared tenancy in the variance re-measure.

---

## Phase 0: Feasibility spike (de-risk before building)

> **Deferred (Scope pivot).** AWS-specific. Returns if/when a cloud
> worker-provisioner is justified under [roadmap-v9.md](./roadmap-v9.md).

**Goal:** validate the AWS primitives end-to-end *manually* and pin the open
design questions before writing a line of driver code.

**Scope:**

- EBS-from-snapshot + **FSR**: clone the chainstate snapshot, measure cold-block
  latency on the first replay vs. a warmed volume. Decide the warm strategy.
- **Two-phase mechanism**: launch large → build → stop → modify instance type →
  start → confirm the built binary persists on the EBS root + measure stop/start
  latency. (Fallback: build+bench on one type, or build-instance →
  AMI/artifact → bench-instance.)
- **Remote exec + artifact pull**: prove SSM (Run Command + the `.phase-log`
  journal read) or SSH gets the same phase signal + run.json the virtio-fs path
  does today.
- **Variance**: a handful of same-commit runs on the chosen shape → CV on
  Execution+Commit, compared to the libvirt floor (~0.6%). This is the
  go/no-go for trustworthiness.
- **Block-val local-NVMe hydration (Codex / block-val sketch) — only if AWS is
  considered for block validation.** Block-val's marf-read I/O profile points at
  local-NVMe instance store (i4i/i3en/im4gn), *not* EBS-from-snapshot — but local
  NVMe starts empty, so the multi-TB chainstate must be hydrated onto it each
  launch (S3 → NVMe, EBS-snapshot → NVMe, or pre-baked AMI). Measure hydration
  time + cost. This is the **go/no-go for AWS vs. a dedicated bare-metal host**
  for block validation; until it competes, **don't start real AWS provisioning
  for block-val** — favor a dedicated bare-metal host (the `remote-daemon`
  worker-fleet direction, [roadmap-v9.md](./roadmap-v9.md)). See
  [block-validation-taskspec.md](./block-validation-taskspec.md).

**Status:**

- [ ] Spike complete — open questions pinned (FSR, two-phase, transport, variance)
- [ ] Block-val hydration measured (or N/A — AWS not pursued for block-val)
- [ ] Reviewed — Codex signed off

---

## Phase 1: Extract the **task-agnostic** `Driver` trait (finish the seam)

**Goal:** turn the half-realized seam into a real plug-in point on **both** axes
— a `Driver` that provisions+runs *any* task on a backend — with **no behavior
change** (bench on libvirt is still the only path that runs).

> **✅ As built (2026-06-08) — trait seam only; cloud-init split deferred.** Per
> an explicit scope call, Phase 1 landed the **minimal behavior-preserving
> seam** and deferred the bigger sub-projects to when a *second consumer*
> justifies them (avoiding speculative abstraction with one task + one backend):
>
> - **Built:** `crate::driver` — `trait Driver { run_task, cleanup_by_job_id }`,
>   neutral `DriverOutcome { status: DriverStatus, summary }`,
>   `DriverStatus { Completed, Failed }`, `Placement { vcpu_cpuset }`, and a
>   minimal `TaskSpec { args }`. The `SinkAdapter` (`PhaseListener`→`EventSink`)
>   moved **inside** `libvirt/driver.rs`; `impl Driver for LibvirtDriver` wraps
>   the (unchanged) inherent `run_benchmark`. `BenchRecipe` holds an
>   `Arc<dyn Driver>`; `JobDeps` + `recover_orphans` dispatch over it; the driver
>   is built once in `Runner::new`. All 650 tests green, lint clean.
> - **Deferred to roadmap-v6** (when block-validation is the second task): the
>   **cloud-init split**; externalizing the **phase model / resource shape /
>   artifact manifest** into `TaskSpec`; and the **fan-out / probe / workspaces /
>   shards / concurrency** fields. The trait *signature* and module placement
>   keep these expressible without an engine change — they're additive when a
>   real second consumer pulls on them. `cleanup_by_job_id` returns `bool` (the
>   existing orphan-recovery contract), not `Result<()>`.
>
> The detailed scope below is the **eventual** target shape (kept as the design
> record); the bullets above are what actually shipped.

**Scope:**

- Define a `Driver` trait (new `crates/sbgh-daemon/src/driver.rs`):
  - `async fn run_task(&self, ctx, spec: &TaskSpec, sink: &dyn EventSink, cancel,
    placement) -> DriverOutcome` — **task-agnostic**. The listener is the
    **neutral `EventSink`** (the libvirt `PhaseListener`→`EventSink` `SinkAdapter`
    from [bench_recipe.rs](../crates/sbgh-daemon/src/bench_recipe.rs) moves
    *inside* the libvirt driver).
  - `async fn cleanup_by_job_id(&self, job_id) -> Result<()>` — the orphan
    primitive the runner currently calls on `LibvirtDriver` directly.
- **`TaskSpec`** — what the *task* (Recipe) hands the driver so the driver itself
  stays bench-free. It carries **no backend envelope** (no cloud-init/user-data,
  no mount/transport details — each `Driver` renders its own; Codex Medium/Low):
  - **task script / command** — what the VM actually runs (today the bench
    invocation; a validation task its own), plus its env/args. The **driver**
    wraps this in *its own* cloud-init/user-data envelope — the `cloudinit.rs`
    split: the **generic VM shell** (mounts, sccache, git checkout, phase-log
    plumbing) is **driver-owned** and rendered per backend (libvirt ISO vs. EC2
    user-data); only the task script is injected from the spec.
  - **phase model** — how to read the `.phase-log` journal → `PhaseLabel` +
    done/success detection (today `phase.rs`'s bench phases).
  - **resource shape** per phase — *abstract* (e.g. "build = large, run =
    prod-shape"), which the **backend** maps (libvirt → mem/vcpus; AWS →
    instance types). v6 calls this the recipe's "resource shape."
  - **artifact manifest** — which files to pull into `archive_dir` (today
    `run.json` + sqlite + binary). The *task* later promotes its typed result
    from them (bench → `JobMetric`); the driver just delivers the archive.
  - **work-phase shape — a shard fan-out, not a single run** (block-validation
    sketch, finding 1; see
    [block-validation-taskspec.md](./block-validation-taskspec.md)). `run_task`
    must dispatch a *set* of shard commands across the backend's compute and
    collect per-shard output; bench is the **K=1** degenerate case. Three
    **separate** counts (finding 3 — don't collapse them): **workspaces** (# of
    writable CoW dataset clones the driver realizes — 1 for bench, N for
    block-val), **shards** (logical work units), **concurrency** (how many run at
    once). Plus an optional **probe/plan** step (finding 2): run a command
    against the *provisioned* dataset, capture stdout, and let the task compute
    the shard partition (block-val derives block ranges from
    `stacks-inspect index-range`). Bench: no probe, 1 workspace, 1 shard.
- **Neutral outcome (Codex Medium).** Today `BenchmarkOutcome` / `OutcomeStatus`
  are **libvirt-module** types; using them in the trait couples AWS to `libvirt`.
  Define `DriverOutcome { status: DriverStatus, summary: serde_json::Value }` in
  the new module; the driver fills the **generic** summary (`archive_dir`,
  `finish_reason`, `last_phase`, `console_tail` + the task's pulled artifact
  paths), and `LibvirtDriver` adapts its internal `BenchmarkOutcome` into it.
  The outcome carries **raw per-shard exit/status only** — never a task verdict
  (block-val sketch, findings 4–5/7). The **Driver stays dumb about task
  meaning**: it provisions, runs declared shard commands, streams events,
  collects artifacts, returns raw outcomes; it does *not* know what a "failed
  block" is. `DriverStatus` distinguishes **infra/execution failure**
  (VM died / transport / timeout → retryable) from a **completed run**; whether a
  *completed* run is a pass or fail is the Recipe's to decide from the raw shard
  results (a negative validation result is a red check, not a driver error).
- **`placement`** abstracts the libvirt-only `vcpu_cpuset` (CPU pinning) — a
  backend-interpreted hint (libvirt → cpuset; AWS → instance type / ignore).
- `BenchRecipe` builds a bench `TaskSpec` and calls `driver.run_task(spec, …)`
  on an `Arc<dyn Driver>` instead of constructing `LibvirtDriver`;
  `LibvirtDriver: Driver`. (The bench-specific cloud-init/phase/result knowledge
  moves from the driver into the bench `TaskSpec` — the v6 task layer.)
- Runner: `JobDeps`/`run` + `recover_orphans` dispatch over `Arc<dyn Driver>`.
- Existing libvirt + recipe + runner tests stay green (behavior-preserving — the
  change is "concrete `run_benchmark` → trait `run_task` with bench specifics
  relocated into the spec").

**Status:**

- [x] Initial implementation completed (trait-seam scope; cloud-init split deferred)
- [x] Integration coverage added (existing libvirt/recipe/runner suites cover it — 650 green)
- [x] Reviewed — Codex signed off (2026-06-08; deferrals accepted, fully-qualified inherent call applied)
- [x] Complete

**Notes:**

- **The cloud-init split is deferred** (not done in Phase 1). Splitting
  `cloudinit.rs` into a driver-owned generic VM shell + a task-supplied script
  only pays off with a *second* task that needs a different in-VM script — so it
  moves to v6 (block-validation). Bench's cloud-init/phase/resource/artifact
  knowledge stays inside `LibvirtDriver` for now; the trait doesn't force it out
  yet.
- **Composes with v6, doesn't depend on it.** v8 landed the task-agnostic
  `Driver` with bench as the only task; v6 adds task kinds on top and grows
  `TaskSpec` (fan-out/probe/workspaces, externalized phase model + script).

---

## Phase 2: Backend selection + config — **superseded by v9** (Codex)

> **Superseded (Scope pivot).** The static `[backend.<task_kind>]` *config
> selector* (a worker-less daemon picking one `Arc<dyn Driver>` per task kind) is
> replaced by **capability matching** in [roadmap-v9.md](./roadmap-v9.md): a
> worker advertises what it can run; the scheduler routes. Don't build
> `[backend.<kind>]` here — future-you would replace it with worker capabilities
> a phase later.

**The one surviving requirement → built in [roadmap-v9.md](./roadmap-v9.md)
Phase 4:** baselines are only comparable within one measurement regime, so jobs
must record **where they ran**. v8's planned `execution_backend` column is
generalized there into an **operator-declared `measurement_profile`** (not a
per-box fingerprint), with a per-profile noise floor — so equalized hosts can
*share* baselines rather than fragment them, and a profile is forked only when
something that actually breaks comparability (disk class, pinning, VM shape)
changes. The original `execution_backend`-column design (typed-enum-as-`TEXT`,
`find_baseline_for` + `job_baseline_*` index dimension, no "remember to purge"
convention) carries over to that profile column.

---

## Phase 3: AWS provisioning **primitives**

> **Deferred (Scope pivot).** AWS-specific. Returns as the cloud
> worker-provisioner under [roadmap-v9.md](./roadmap-v9.md), gated on
> cost/variance/hydration data.

**Goal:** the AWS *provisioning building blocks* — create/attach the cloned
chainstate volume, launch the instance from the AMI, the two-phase resize, and
teardown — as individually testable units against a mock SDK. This is **not yet
a runnable `Driver`**: there's no remote exec, phase observation, or summary —
Phase 4 adds those to turn these primitives into a working `run_task`.

**Scope (new `crates/sbgh-daemon/src/aws/` module, mirroring `libvirt/`):**

- Deps: `aws-sdk-ec2`, `aws-sdk-ssm` (or an SSH client), optionally
  `aws-sdk-s3`; `aws-config` for credential/region resolution.
- `volume.rs` — `CreateVolume` from `chainstate_snapshot_id` (the `lvm.rs`
  analog); FSR-aware; tags `sbgh:job_id`.
- `instance.rs` — `RunInstances` from `ami_id` + user-data, attach the
  chainstate volume; tags `sbgh:job_id`; waits for `running` + status checks.
- `userdata.rs` — render the driver's **generic VM shell** (Phase 1's split) as
  EC2 user-data, injecting the **task script from the `TaskSpec`**; swap only the
  mount/transport bits (attached EBS device mount, results destination) for the
  AWS shape.
- The **two-phase resize** primitive (per Phase 0's decision): stop →
  `ModifyInstanceAttribute(instance_type)` → start, preserving the EBS root.
- `aws/driver.rs` — the `AwsDriver` skeleton + the provisioning lifecycle
  (volume → instance → resize → terminate). Its `Driver::run_task` is a
  **stub/smoke path** here (provision, then immediately tear down); it becomes a
  real run in Phase 4.

**Status:**

- [ ] Initial implementation completed
- [ ] Integration coverage added (mockable SDK seam; a fake AWS client like the
  libvirt `RecordingShell` — each primitive unit-tested without hitting AWS)
- [ ] Reviewed — Codex signed off
- [ ] Complete

**Notes:**

- The AWS driver leans on the **AWS SDK + SSM**, not the `Shell` abstraction,
  for provisioning (it can still use `Shell` for host-side git). The seam is the
  `Driver` trait, not `Shell`.
- The **mockable SDK boundary** (a small trait over the EC2/SSM calls we use) is
  what makes Phase 3's primitives unit-testable — the analog of `RecordingShell`.

---

## Phase 4: Make it a runnable `Driver` — remote exec, forensics & the `summary`

> **Deferred (Scope pivot).** AWS-specific. Note its remote-exec / phase-poll /
> artifact-pull mechanics are conceptually close to what v9's worker↔orchestrator
> API does generically — fold the lessons in there rather than duplicating.

**Goal:** assemble Phase 3's primitives into a **complete, runnable
`AwsDriver::run_task`** by adding the remote-execution, phase-observation,
artifact-pull, and summary path — i.e. actually run the task in the instance
across both phases and produce the results archive. The moment this lands the
**entire** reporting/DB/vs-baseline stack works unchanged (it consumes the same
`summary`), so the roadmap-v7 comparison + all GitHub surfaces light up.

**Scope:**

- Full `run_task` orchestration: provision (Phase 3) → run **build** phase →
  two-phase resize → run **bench** phase → collect → teardown, honoring `cancel`
  (terminate) + `job_timeout_secs`.
- Phase polling: read the in-instance `.phase-log` journal via SSM Run Command
  (or SSH) on the same cadence the libvirt `poll_to_completion` uses; map to
  `PhaseLabel` → `EventSink` (each backend owns its phase→label adapter).
- Artifact pull: fetch `run.json` + console/log tail + the sqlite + the binary
  to the daemon host (SSM, SSH/scp, or instance-uploads-to-S3-then-fetch), into
  the per-job `archive_dir`.
- Build the **same `summary` blob** — the driver fills the generic keys
  (`archive_dir`, `finish_reason`, `last_phase`, `console_tail`) + the task's
  pulled artifact paths. For the bench task that artifact is `run.json` →
  `run_json_archived_path`, so `extract_outcome` / `bench_summary` / the reporter
  consume it with **zero** changes. (A validation task would surface a different
  artifact + its own promotion — a v6 concern, cleanly separated.)
- Cancel/timeout → terminate the instance; surface as `Terminal::Aborted` /
  `Failed` exactly as libvirt does.

**Status:**

- [ ] Initial implementation completed
- [ ] Integration coverage added (summary-contract parity test: an AWS-shaped
  summary drives the existing reporter/extract_outcome path)
- [ ] Reviewed — Codex signed off
- [ ] Complete

---

## Phase 5: Teardown & orphan recovery

> **Deferred (Scope pivot).** AWS-specific resource reaping. The *worker*-level
> orphan/lease story is now a v9 concern; this returns with the cloud provisioner.

**Goal:** never leak an instance or volume; recover after a daemon crash.

**Scope:**

- Teardown (reverse order): terminate the instance, delete the chainstate volume
  (+ any created volumes), best-effort like the libvirt `teardown`.
- `AwsDriver::cleanup_by_job_id` — terminate/delete everything tagged
  `sbgh:job_id = <id>` (the `cleanup_by_job_id` analog the runner already calls).
- Startup orphan sweep: enumerate `sbgh`-tagged instances/volumes with no live
  job and reap them (the AWS half of roadmap-v5 Phase 4C orphan recovery, which
  already terminal-cancels the DB row — only the *resource* cleanup is
  backend-specific).

**Status:**

- [ ] Initial implementation completed
- [ ] Integration coverage added
- [ ] Reviewed — Codex signed off
- [ ] Complete

---

## Phase 6 (optional): concurrency, cost & ops

> **Deferred (Scope pivot).** AWS-specific. Concurrency-by-adding-workers is now
> v9's model; spot/sccache/snapshot-refresh return with the cloud provisioner.

**Goal:** exploit instance-per-job isolation + manage spend.

**Scope:**

- **Drop pinning for AWS.** `max_concurrent_jobs` becomes "concurrent instances"
  (bounded by EC2 limits / cost); the coordinator's slot pool still gates
  concurrency, but the `cpu_sets`/`host_cpus` placement is libvirt-only (a no-op
  hint on AWS). Re-measure the variance floor on the AWS shape and set
  `[reporting].noise_cv_pct` accordingly (roadmap-v7).
- **Spot** support + interruption → `Aborted` (re-triggerable).
- **S3-backed sccache** for warm cross-instance builds (replaces the virtio-fs
  host sccache dir).
- **Chainstate snapshot refresh** job — keep the EBS source snapshot current as
  the chain grows (the analog of maintaining the LVM base chainstate); + FSR
  lifecycle.

**Status:**

- [ ] Design pinned
- [ ] Implementation
- [ ] Reviewed — Codex signed off
- [ ] Complete

---

## Rollout

> **Deferred (Scope pivot).** AWS-specific rollout. Returns with the cloud
> worker-provisioner under [roadmap-v9.md](./roadmap-v9.md) Phase 5. The
> baseline-comparability point below is generalized by v9's `measurement_profile`
> (Phase 4): an equalized fleet *can* share a profile; a genuinely different
> substrate forks one.

- **AMI bake pipeline**: a golden AMI with the toolchain (rust, sccache, the
  bench deps) + the SSM agent + the in-instance bench script — the AMI analog of
  the libvirt golden qcow2. (Packer or an EC2 Image Builder recipe.)
- **IAM**: a daemon role/credentials scoped to the EC2/EBS/SSM/(S3) actions used,
  plus an instance profile for the bench instance (SSM + S3 read/write).
- **VPC/networking**: subnet + security group; SSM reachability (NAT or VPC
  endpoints).
- **Dual-run validation**: run the same commit on two substrates; confirm the
  Execution+Commit numbers are internally consistent (absolute numbers will
  differ by hardware — what matters is per-profile stability + that the
  vs-baseline math is correct within a `measurement_profile`).

## Decisions

1. **Seam at the `Driver` trait, not `Shell`.** `Shell` abstracts host commands;
   the AWS driver provisions via the AWS SDK + SSM. The plug-in point is
   `Driver::{run_task, cleanup_by_job_id}`, with the `PhaseListener` bridge pulled
   inside each driver so it speaks the neutral `EventSink`.
2. **The `Driver` is task-agnostic** (`run_task(TaskSpec)`, not
   `run_benchmark`). Bench/validation specifics — cloud-init script, phase model,
   resource shape, result artifacts — live in the `TaskSpec` the Recipe supplies,
   so `{task} × {backend}` is a matrix. This aligns with v6's three layers (the
   `Driver` = the substrate layer's backend axis) and means v6's task kinds get
   both backends for free.
3. **Results are a generic archive (driver) + a typed result (task).** The driver
   delivers `archive_dir` + forensics + the task's pulled artifacts; the *task*
   promotes the typed result (bench → `JobMetric`). For the bench task this is
   exactly today's `summary` keys, so the reporter/DB/vs-baseline stack is
   untouched — the highest-leverage property, and Phase 4's payoff.
4. **~~Backend resolved per task kind via `[backend.<kind>]`~~ — superseded by
   v9 capability matching.** The static per-kind config selector is replaced by
   workers advertising what they can run; the scheduler routes
   ([roadmap-v9.md](./roadmap-v9.md)). Default-libvirt back-compat is preserved
   by the single-host path running its local `Driver` directly.
5. **Baselines are per `measurement_profile`** (built in v9 Phase 4) — vs-baseline
   comparison (roadmap-v7) only makes sense within one measurement regime, so
   jobs record *where they ran*. v8's planned `execution_backend` column
   generalizes to an **operator-declared** profile (equalized hosts may share one;
   per-profile noise floor; forked only on a real comparability break) — *not* a
   per-box fingerprint. The structural-enforcement design (typed column +
   `find_baseline_for` dimension, no "purge on switch" convention) carries over.
6. **~~Backend choice is per task kind, not per job~~ — generalized by v9.** Per-job
   routing across a fleet — deferred here — is exactly what v9's capability-matched
   scheduler delivers. (`remote-daemon` → [roadmap-v9.md](./roadmap-v9.md).)

## Sequencing notes

- **Phase 0 gates everything** — the spike pins FSR/two-phase/transport/variance
  before any code; a bad variance result is the go/no-go.
- **Phase 1 is independently valuable** — extracting the task-agnostic `Driver`
  trait cleans up *both* the libvirt and the bench coupling even if the AWS work
  never ships, and it's behavior-preserving.
- **v8 (backend axis) ⟂ v6 (task axis), same substrate boundary.** Both ride the
  task-agnostic `Driver`/`TaskSpec` seam; neither blocks the other. Whichever
  lands first defines that seam (Phase 1 here, or v6's `Recipe`-boundary work).
  Sequencing across the two is a product call — block-validation demand vs. AWS
  demand — not a technical dependency.
- **Phases 3 → 4 are the MVP** — Phase 3 builds the provisioning primitives
  (testable in isolation), Phase 4 assembles them into a runnable `run_task`
  (remote exec + phase poll + artifacts + summary); Phase 4 is where the whole
  product stack lights up on AWS.
- **Phase 5 is required before any real use** (resource leaks = cost).
- **Phase 6 is optional polish** — concurrency/cost/ops, deferrable.
- The **upper stack is reused unchanged throughout** — no changes to the job
  engine, reporting, the roadmap-v7 comparison, or the DB.

### `remote-daemon` → promoted to [roadmap-v9.md](./roadmap-v9.md)

What was sketched here as a "future idea" is now the **primary direction**, with
its own roadmap: a **distributed worker fleet** where each host runs a worker
daemon advertising capabilities (`["benchmark", "block-validation"]`) and pulls
compatible work. Key points (full design in v9):

- **A distribution layer, not a `Driver` kind.** The worker runs the *real* local
  Driver (this doc's Phase 1); v9 is the scheduling/transport layer above it. (The
  earlier "just another backend kind that forwards a `TaskSpec`" framing was a
  simplification — corrected in v9.)
- **Pull/subscribe, not SSH** — workers dial *out*, so no inbound holes; hosts can
  be anywhere.
- **Concurrency scales by adding workers** — the answer to the bare-metal
  concurrency ceiling.
- **Cloud returns as a worker provisioner** (v9 Phase 5) — *who* spawns a worker
  host — which is why this doc's AWS phases are parked rather than deleted.
