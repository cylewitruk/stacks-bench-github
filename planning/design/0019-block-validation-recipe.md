# Design 0019: Block-validation recipe (second task kind)

- **id:** `0019-block-validation-recipe`
- **status:** `planned` (`v25-worker-fleet-block-validation`)
- **depends_on:** `0005-task-kind-platform` (shipped),
  `0055-execution-boundary-preparation` (v24)
- **relates_to:** `0004-worker-fleet`, `0037-benchmark-group-run-model`
- **iteration:**
  [`v25-worker-fleet-block-validation`](../iterations/v25-worker-fleet-block-validation.md)
- **source:** roadmap-v6 Phase 3 + the `block-validation-taskspec` sketch +
  v25 dedicated Hetzner worker

Block validation as the **second task kind** — the proof that the platform can
add a task at its registration/composition boundary without changing scheduler,
lease, event, or terminal lifecycle control flow. Distilled from `stacks-core`'s
`contrib/tools/block-validation.sh`; this sketch also validated the shipped `0010`
`Driver` seam against a non-bench task before extraction. *(Converted from the
former `docs/roadmap-v6.md` Phase 3 + `docs/block-validation-taskspec.md`.)*

## The task shape (five phases)

1. **Build** — `cargo build --bin=stacks-inspect` from a rev (same shape as bench's
   build).
2. **Dataset** — obtain the **full mainnet chainstate** (multi-TB) + make **K
   copy-on-write clones**, one per local validation shard (reflink, else copy +
   shared `marf.sqlite.blobs` symlink).
3. **Plan (probe)** — `stacks-inspect … index-range` returns per-epoch block
   totals; the task splits the global range into K contiguous sub-ranges.
4. **Work (fan-out)** — **K parallel local validation shards**, each
   `validate-block <clone_i> <range>` against its own clone. One fleet worker
   owns the whole task and all K shards; shards are not independently scheduled
   across hosts. Embarrassingly parallel, correctness-oriented.
5. **Reduce** — aggregate per-shard exit codes (`0` ok · `1` block-failures ·
   other = panic) → pass/fail.

## How it differs from bench (what the seam must carry)

| Dimension | Bench | Block validation |
| --------- | ----- | ---------------- |
| Work phase | single sequential run | **K-shard parallel** fan-out |
| Partition | none (static) | **dataset-derived** (probe → ranges) |
| Clones | 1 | **K writable CoW** workspaces |
| Placement | pinned, low-variance | saturate cores, spot-friendly |
| Success | timing (always "succeeds") | **binary correctness** |
| Result | metrics → `run.json` | pass/fail + failure list |

Bench is the **K=1, static-partition, single-clone** degenerate case. The seam must
keep **workspaces / shards / concurrency as three separate counts** (don't collapse
them), and support an optional **probe** step that queries the *provisioned*
dataset to compute the partition. `run_task`'s `DriverOutcome` carries **raw
per-shard exit/status only** — the `Recipe` derives pass/fail; the driver never
knows what a "failed block" is.

## Terminal semantics (explicit)

Invalid-blocks-found is a **completed task with a negative result → red GitHub
check**, *not* an errored/retryable job. Only VM-died / transport / timeout is a
driver/execution failure. This keeps block-val results off the reporting
non-fatal/retry machinery meant for infra faults.

## Phase 3 scope (the recipe)

- `BlockValidationRecipe: Recipe` — phases (build → dataset → probe → fan-out →
  reduce), per-shard command, done-detection, resource shape.
- Its result schema (own table) + check/comment render.
- Register `block_validation` + `/validate-blocks` (and/or a trigger policy).
- **Additive lifecycle:** block validation may add its payload, recipe/driver,
  registration, persistence, and rendering. If it forces changes to
  `sbgh-exec` scheduling-independent execution lifecycle or to orchestrator
  claim/lease/event/terminal control flow, that is a boundary leak to fix rather
  than patch around inside the task.

## Infra notes

- Block-val wants the **full chainstate + N CoW workspaces** — *not* bench's single
  small golden chainstate (roadmap-v6 Open question 2). The worker-local
  block-validation substrate in `sbgh-exec` must expose "provision dataset + N
  CoW workspaces."
- I/O-bound on marf reads → wants **big local NVMe / dedicated bare metal**, not
  cloud volumes (the analysis behind `0004`/`0006`). Correctness-oriented → no
  low-variance/pinning need; its own `measurement_profile`.
- The first production substrate is the v25 Hetzner worker (64 CPU cores,
  256 GB RAM, four 4 TB NVMe drives). Phase 1 measures filesystem/reflink,
  capacity, CPU/NUMA, and NVMe behavior before selecting N/K/concurrency or the
  final disk layout.

## Open question

- **Substrate scope** (v6 OQ2): confirmed — block-val does **not** share bench's
  chainstate-provisioning shape; it needs full-chainstate + N CoW workspaces + a
  probe-driven partition.
