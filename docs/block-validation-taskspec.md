# Block-validation `TaskSpec` sketch — pre-flight for the Driver seam

**Purpose.** Before roadmap-v8 Phase 1 extracts a *task-agnostic* `Driver` /
`TaskSpec`, validate the seam against a **second, non-bench task**. An
abstraction with one witness per axis (bench + libvirt) is a guess; block
validation is the cheapest real second task to pull on it. This sketch is paper
only — no code — and feeds refinements back into v8 (Phase 1 `Driver` signature),
v9 (capability routing + `measurement_profile` baseline trust), and v6 Phase 3.

Source of truth for the task shape:
`stacks-network/stacks-core:contrib/tools/block-validation.sh` (~1.2k LoC bash,
read in full). What follows is its distilled model.

## What block validation actually does

Five phases, in order:

1. **Build** — clone/checkout `--rev`, `cargo build --bin=stacks-inspect
   --release`. Same shape as bench's build phase.
2. **Dataset** — obtain the **full mainnet chainstate** (multi-TB, 8M+ blocks;
   Hiro snapshot download or a local copy), then make **K copy-on-write clones**,
   one per worker — reflink when the filesystem supports it (XFS/Btrfs/ZFS/APFS),
   else a full copy with a shared `marf.sqlite.blobs` symlink so the big inode
   isn't duplicated K times.
3. **Plan** — **probe the dataset**: `stacks-inspect … index-range` /
   `naka-index-range` returns per-epoch block totals; the task then splits the
   global block range into epoch segments (pre-nakamoto / nakamoto) and each
   segment into K contiguous sub-ranges.
4. **Work** — **fan out K workers in parallel**, each running
   `stacks-inspect validate-block <clone_i> <range_cmd> <start> <end>` against its
   own clone. Block processing is single-threaded, so parallelism is disjoint
   block ranges across clones. Embarrassingly parallel, correctness-oriented,
   variance-irrelevant.
5. **Reduce** — aggregate per-worker exit codes (`0` ok · `1` block failures ·
   other = panic) and `Failed processing block` lines into `results.log`.
   Overall pass/fail = `failures == 0`.

## Bench vs. block validation — the shape table

| Dimension | Bench | Block validation |
| --------- | ----- | ---------------- |
| Build | build node/bench from rev | build `stacks-inspect` from rev |
| Input dataset | golden chainstate (small smoke, ~5k blocks) | full mainnet chainstate (multi-TB, 8M+ blocks) |
| Clone cardinality | 1 | K (one writable CoW clone per worker) |
| Partition | none (static) | **dataset-derived** (probe block totals → split ranges) |
| Work phase | **single** sequential replay | **K parallel** disjoint block sub-ranges |
| Placement | pinned, isolated, low-variance | saturate cores, spot-friendly, variance-agnostic |
| Success | always "succeeds"; value is the timing | **binary correctness** (`failures == 0`) |
| Result | timing metrics (`execution+commit µs`) → `run.json` | pass/fail + failure list → `results.log` |
| Exit-code meaning | n/a | task-interpreted (`0`/`1`/panic differ) |
| Infra profile | bursty, short, latency-sensitive | long (~20h), throughput-bound, predictable |

Bench is the **K=1, static-partition, single-clone** degenerate case of the same
model — which is the good news: one fan-out abstraction covers both.

## What this forces on the seam

Concrete findings — each is a place a bench-first extraction would be wrong:

1. **The work phase is a fan-out of K shards, not a single run.** `run_task`
   must dispatch a *set* of worker commands across the backend's compute
   (cores on one host, VMs, or machines) and collect per-shard output. If we
   extract `run_task` as "build → one measured run," block validation doesn't
   fit. This is the headline.
2. **Partition can require a probe against the live dataset.** Block ranges come
   from `stacks-inspect index-range` *after* the dataset exists — partitioning
   isn't static config. The phase model needs a **plan** slot: run a probe
   command against the provisioned dataset, get stdout, compute the shard list.
   (Bench skips it: K=1, static.)
3. **Workspaces, shards, and concurrency are three separate counts — don't
   collapse them (Codex).** The script happens to set
   `workspaces = shards = concurrency = cores`, but that's an implementation
   coincidence, not the model. Keep them distinct at the boundary:
   - **workspaces** — how many writable CoW clones of the dataset the task needs.
   - **shards** — logical units of work (block sub-ranges); may exceed workspaces.
   - **concurrency** — how many shards run at once (scheduler/placement width).
   The task declares the workspace count and shard plan; the `Driver` realizes
   the CoW clones (reflink / LVM-thin / EBS-from-snapshot) and schedules shards
   onto them respecting `placement`. Keeping these separate lets retries and
   checkpointing evolve (re-run one shard on a free workspace) without touching
   the boundary.
4. **"Success" is a task interpretation of raw exit info, not "exit 0."**
   Block validation treats `1` (block failures — collect them) differently from
   a panic. `DriverOutcome` must carry **raw per-shard exit/status**, and the
   *Recipe* maps it to pass/fail. Don't bake "exit 0 = success" into the Driver.
5. **Reduce is a Recipe concern.** Per-worker raw outputs → typed result
   (failure list vs. timing metrics) is task logic, off the Driver. `summary`
   in `DriverOutcome` must be a task-defined blob, not bench-shaped.
6. **Placement mode is a TaskSpec hint, not a Driver assumption.** Bench needs
   pinned/isolated; block validation wants saturate-cores + spot-OK. The Driver
   must accept both via `placement`, never assume isolation.
7. **Terminal semantics: a negative validation result is not an infra failure
   (Codex).** "The tool ran and found invalid blocks" is a **completed task with
   a negative result** → a *red GitHub check*, not an errored job. Distinct from
   "VM died / transport failed / timeout," which is a **driver/execution
   failure** → retryable/orphan-recovery path. The `Driver` reports only the
   latter class; the *Recipe* turns raw shard outcomes into the
   pass/fail-but-completed verdict. This split must be explicit so block-val
   failures don't trip the reporting non-fatal/retry machinery meant for infra
   faults.

## Proposed unified `run_task` shape

```rust
// Driver = backend axis. Task-agnostic. Owns compute + CoW clones + dispatch.
async fn run_task(
    ctx: &TaskContext,
    spec: &TaskSpec,
    placement: &Placement,   // Pinned-isolated (bench) | Saturate+spot (blockval)
    sink: &mut dyn OutputSink,
    cancel: CancellationToken,
) -> DriverOutcome;

struct TaskSpec {
    build:      BuildSpec,               // rev + which binary to cargo build
    dataset:    DatasetSpec,             // source snapshot + workspace plan
    workspaces: usize,                   // # writable CoW clones (1 for bench)
    plan:       Option<ProbeSpec>,       // optional probe→stdout→partition (blockval)
    shards:     ShardPlan,               // logical work units (static K=1 for bench)
    concurrency: usize,                  // how many shards run at once (≤ workspaces)
    collect:    ArtifactManifest,
    // NO backend envelope: no cloud-init/user-data, no mount/transport — the
    // Driver renders its own. (roadmap-v8 Phase 1 invariant.)
    // workspaces / shards / concurrency are SEPARATE — see finding 3.
}

struct DriverOutcome {
    shards:  Vec<ShardResult { exit, raw_artifacts }>,  // RAW — Recipe reduces
    // Carries only infra-level status. A negative *validation* result is NOT
    // here; the Recipe derives it from raw shard outcomes (finding 7).
}
```

- **Recipe (task axis)** owns: which binary to build, the dataset source, the
  partition logic (incl. the probe), the per-shard command template, the
  reduce (raw → typed result), and the placement hints.
- **Driver (backend axis)** owns: provisioning compute, realizing K CoW clones,
  scheduling shards across compute respecting `placement`, streaming worker
  output to `sink`, collecting artifacts, teardown.

## Infra topology & cloning (the open questions)

**Q: one colossal machine, or map-reduce across many VMs?**

Recommendation: **one large machine, scale cores not machines, CoW clones within
the host** — until a single box can't hold enough cores *or* you need concurrent
validations. Reasoning:

- Block validation is **correctness, not timing** → no isolation/pinning/low-
  variance requirement → pack cores densely; spot/preemptible is fine because a
  shard is an idempotent, re-runnable block sub-range (checkpoint granularity =
  one slice).
- The dominant cost is the **multi-TB chainstate**. Replicating it across M
  machines multiplies the most expensive resource. K CoW clones on one host keep
  storage to ~1 copy + per-worker write deltas (each worker mutates only a small
  fraction during its sub-range).
- I/O bound: block replay is marf-read-heavy. The dev NUC's gen5 NVMe is the
  real reference; cloud parity means **local-NVMe instance classes** (i4i / i3en
  / im4gn), not network EBS — EBS gp3/io2 would likely be the bottleneck.

**Q: reflink vs. LVM-thin vs. EBS-from-snapshot?**

They're not competitors — they sit at different layers and **compose**:

- **Single host, K process-workers** → **reflink** (file-level CoW on local
  NVMe; XFS/Btrfs/ZFS). Exactly what the script does: per-slice clone is
  metadata-only, only runtime writes allocate. Keep `CHAIN_DIR` and the scratch
  dir on the *same* volume so slice0 is also reflinked. This is the right
  primitive for the recommended topology.
- **Per-VM or per-machine fan-out** → one **block-level clone per VM/machine**
  (LVM-thin on-prem, EBS-from-snapshot in cloud), then **reflink slices within**
  each machine. Only needed if you adopt cross-machine fan-out — which the cost
  argument above advises against unless forced.

**Q: AWS or a second dedicated host?**

Block validation's profile (long ~20h, predictable, throughput-bound, no
low-variance need) is a **poor fit for on-demand cloud** and a **good fit for
dedicated bare metal**. AWS-ephemeral's value (sub-minute spin-up, elastic
burst, pay-per-use) is matched to *bench*, not to a recurring 20h batch. Lean:

- **Block validation → a beefy dedicated host** the daemon dispatches to (a
  second Hetzner box) via the **`remote-daemon` distribution layer**
  ([roadmap-v9.md](./roadmap-v9.md)) — a worker advertising the
  `block-validation` capability, *not* a Driver/backend kind. Amortizes far
  better than on-demand for a regularly-run long batch.
- **Bench → AWS-ephemeral or pinned-libvirt.** Bursty + latency-sensitive +
  needs isolation.
- AWS *spot* is viable for block validation **if** you checkpoint at slice
  granularity (you can), but multi-TB chainstate transfer/storage per spot
  instance erodes the savings.
- **If AWS is used, the chainstate likely wants local-NVMe instance store, not
  EBS — which creates a hydration problem (Codex).** v8's AWS story is
  "EBS-from-snapshot," but block-val's marf-read I/O profile points at local
  NVMe (i4i/i3en/im4gn). Local NVMe is ephemeral and starts empty, so the
  multi-TB chainstate must be *hydrated* onto it on every launch (S3 → NVMe,
  EBS-snapshot → NVMe copy, or a pre-baked AMI with a warm restore). How cheaply
  and quickly that hydration runs is **unproven and Phase-0 must measure it** —
  it's a different cost model from EBS-from-snapshot CoW and is the crux of
  whether AWS competes with a dedicated host for this task.

**Architectural consequence:** bench and block validation have **opposite infra
profiles**, so they must be routable to **different substrates**. The original
"per-task-kind `[backend.<kind>]` config" idea was superseded — that routing is
now **capability matching** in the [roadmap-v9.md](./roadmap-v9.md) worker fleet:
a bench worker (pinned host) advertises `benchmark`; a block-val worker
(big-local-NVMe bare metal) advertises `block-validation`; the scheduler routes
by capability. No static per-kind backend config; `remote-daemon` is that
distribution layer, not a Driver kind.

## Refinements this implies

- **roadmap-v8 Phase 1** — `run_task` must express a **shard fan-out** + an
  optional **dataset-probe plan**, not "build + single run." `TaskSpec` keeps
  **workspaces / shards / concurrency as separate counts** (finding 3) and gains
  `plan: Option<ProbeSpec>`; `DriverOutcome` carries **raw per-shard
  exit/artifacts** (Recipe reduces; finding 5). The Driver stays **dumb about
  task meaning** — it provisions, runs declared shard commands, streams events,
  collects artifacts, returns raw outcomes; it never knows what a "failed block"
  is. Placement covers both Pinned-isolated and Saturate+spot.
- **roadmap-v9 (distribution + baseline trust)** — routing bench vs block-val to
  different substrates is **capability matching** in the worker fleet (a worker
  advertises `benchmark` / `block-validation`), *not* a static `[backend.<kind>]`
  config. Baseline trust is an operator-declared **`measurement_profile`** (v9
  Phase 4), not `job.execution_backend`: equalized hosts may share a profile, with
  a per-profile noise floor. (This supersedes the earlier v8 Phase 2 framing.)
- **roadmap-v8 Phase 0** — add a **local-NVMe hydration** measurement: if AWS is
  considered for block-val, prove the multi-TB chainstate can be hydrated onto
  ephemeral instance-store cheaply/quickly enough to compete with a dedicated
  host. Until those numbers exist, **don't start real AWS provisioning for
  block-val.**
- **roadmap-v6 Phase 3** — (a) **terminal semantics** must be explicit
  (finding 7): invalid-blocks-found = completed task + red check, *not* an infra
  failure; VM-died/transport/timeout = driver failure. (b) Open question #2:
  block-val does **not** share bench's chainstate-provisioning shape — it wants
  the *full* chainstate + N CoW workspaces + a probe-driven partition. The
  `Recipe` boundary holds (same five-phase model, different parameters), but
  `sgh-substrate` must expose "provision dataset + N CoW workspaces," not
  "provision one chainstate."
