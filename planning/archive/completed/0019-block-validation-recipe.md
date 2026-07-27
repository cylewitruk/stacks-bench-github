# 0019: Block-Validation Recipe (Second Task Kind)

- **id:** `0019-block-validation-recipe`
- **status:** `shipped` (`v25-worker-fleet-block-validation`, 2026-07-27)
- **depends_on:** `0005-task-kind-platform` (shipped),
  `0055-execution-boundary-preparation` (v24),
  `0056-compiler-enforced-execution-boundaries` (v24.1)
- **relates_to:** `0004-worker-fleet`, `0037-benchmark-group-run-model`
- **iteration:** `v25-worker-fleet-block-validation` (shipped)
- **review:** Codex implementation and Opus review signed off
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
   verified copy-on-write clones**, one per local validation shard. v25 has no
   shared-write or symlinked-blob fallback: a host that cannot prove clone
   isolation does not advertise the block-validation capability.
3. **Plan (probe)** — `stacks-inspect … index-range` returns per-epoch block
   totals; the task splits the global range into K contiguous sub-ranges.
4. **Work (fan-out)** — **K parallel local validation shards**, each
   `validate-block <clone_i> <range>` against its own clone. One fleet worker
   owns the whole task and all K shards; shards are not independently scheduled
   across hosts. Embarrassingly parallel, correctness-oriented.
5. **Reduce** — aggregate per-shard exit codes (`0` ok · `1` block-failures ·
   other = infrastructure/execution failure) using the precedence below.

## Dataset and partition contract

The canonical dataset is an immutable, worker-local generation. Its identity
contains at least network, dataset/schema format version, covered tip/range, and
a verified manifest digest. An assignment pins one exact dataset generation;
refresh builds and verifies a new generation separately, then atomically
switches the worker's `current` pointer. Pinned generations remain until no
attempt or workspace references them.

The canonical generation is mounted/read as non-writable by the worker task.
Each shard receives a distinct clone created through the exact Phase 1
filesystem command/API. A mutation-isolation smoke is part of capability
qualification and startup verification.

Probe output is parsed into validated inclusive source ranges. For any range of
`n` items and requested shard count `K`, the effective count is `min(K, n)`.
With `q = n / effective` and `r = n % effective`, the first `r` contiguous
shards receive `q + 1` items and the remainder receive `q`. Empty shards are
not launched. Tests prove the resulting ranges are ordered, non-overlapping,
gap-free, and cover the probed range exactly once.

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

Reduction is fail-closed:

- if cancellation was durably requested, cancellation wins;
- if any shard fails setup, times out, is killed, or exits outside `{0, 1}`, the
  task is an execution failure even if another shard found invalid blocks;
- otherwise any exit `1` produces a completed negative result;
- only all-zero shards produce a completed positive result.

Cancellation terminates each shard's complete process group before workspace
cleanup. Partial logs remain attempt-scoped diagnostics and never manufacture a
positive or negative correctness result.

## Phase 3 scope (the recipe)

- `BlockValidationRecipe: Recipe` — phases (build → dataset → probe → fan-out →
  reduce), per-shard command, done-detection, resource shape.
- Its result schema (own table) + check/comment render.
- Register `block_validation` + `/validate-blocks` (and/or a trigger policy).
- **Additive lifecycle:** block validation may add its payload, recipe/driver,
  registration, persistence, and rendering. If it forces changes to
  `sbgh-worker`'s scheduling-independent execution lifecycle or to orchestrator
  claim/lease/event/terminal control flow, that is a boundary leak to fix rather
  than patch around inside the task.

## Infra notes

- Block-val wants the **full chainstate + N CoW workspaces** — *not* bench's single
  small golden chainstate (roadmap-v6 Open question 2). The worker-local
  block-validation adapter must expose "provision dataset + N CoW workspaces"
  through the driver API.
- I/O-bound on marf reads → wants **big local NVMe / dedicated bare metal**, not
  cloud volumes (the analysis behind `0004`/`0006`). Correctness-oriented → no
  benchmark measurement profile; placement instead requires the authorized
  capability and pinned dataset identity.
- The first production substrate is the v25 Hetzner worker (64 CPU cores,
  256 GB RAM, four 4 TB NVMe drives). Phase 1 measures filesystem/reflink,
  capacity, CPU/NUMA, and NVMe behavior before selecting N/K/concurrency or the
  final disk layout.

## v25 Substrate Scope

- v25 did not share benchmark's chainstate-provisioning shape: it used a
  full-chainstate directory plus N filesystem CoW workspaces and a probe-driven
  partition. This records the shipped v25 implementation, not a permanent
  storage decision.

## Shipped Outcome

v25 shipped block validation as the second registered task kind, with immutable
dataset generations, verified copy-on-write shard workspaces, inclusive
gap-free partitioning, strict diagnostic parsing, and fail-closed reduction.
The generic fleet scheduler leases the task by authorized capability; its
driver owns only worker-local execution and never gains orchestrator database,
GitHub, or Slack authority.

The recipe, parser, partition, reduction, cancellation, and protocol paths are
covered by the green v25 workspace suite. Physical dataset hydration and
long-running validation on the dedicated host remain rollout checks in the
[worker-fleet operations guide](../../../docs/worker-fleet-operations.md).

[v26](../../iterations/v26-sandboxed-worker-execution.md) supersedes the v25
substrate detail: it moves the recipe into one resource-profiled libvirt VM,
reuses benchmark's LVM-thin snapshot provider with K snapshots of an exact
sealed origin generation, and forbids repository-built payloads from executing
directly on the worker host.
