# Design 0019: Block-validation recipe (second task kind)

- **id:** `0019-block-validation-recipe`
- **status:** `backlog`
- **depends_on:** `0005-task-kind-platform`
- **source:** roadmap-v6 Phase 3 + the `block-validation-taskspec` sketch

Block validation as the **second task kind** — the proof that the platform
(`0005`) costs ~one crate per kind. Distilled from `stacks-core`'s
`contrib/tools/block-validation.sh`; this sketch also validated the shipped `0010`
`Driver` seam against a non-bench task before extraction. *(Converted from the
former `docs/roadmap-v6.md` Phase 3 + `docs/block-validation-taskspec.md`.)*

## The task shape (five phases)

1. **Build** — `cargo build --bin=stacks-inspect` from a rev (same shape as bench's
   build).
2. **Dataset** — obtain the **full mainnet chainstate** (multi-TB) + make **K
   copy-on-write clones**, one per worker (reflink, else copy + shared
   `marf.sqlite.blobs` symlink).
3. **Plan (probe)** — `stacks-inspect … index-range` returns per-epoch block
   totals; the task splits the global range into K contiguous sub-ranges.
4. **Work (fan-out)** — **K parallel workers**, each `validate-block <clone_i>
   <range>` against its own clone. Embarrassingly parallel, correctness-oriented.
5. **Reduce** — aggregate per-worker exit codes (`0` ok · `1` block-failures ·
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
- **Additive-only:** if it forces a change to `sgh-engine`/`sgh-substrate`, that's a
  boundary leak to fix in the abstraction, logged as a finding — not patched in the
  task.

## Infra notes

- Block-val wants the **full chainstate + N CoW workspaces** — *not* bench's single
  small golden chainstate (roadmap-v6 Open question 2). `sgh-substrate` must expose
  "provision dataset + N CoW workspaces."
- I/O-bound on marf reads → wants **big local NVMe / dedicated bare metal**, not
  cloud volumes (the analysis behind `0004`/`0006`). Correctness-oriented → no
  low-variance/pinning need; its own `measurement_profile`.

## Open question

- **Substrate scope** (v6 OQ2): confirmed — block-val does **not** share bench's
  chainstate-provisioning shape; it needs full-chainstate + N CoW workspaces + a
  probe-driven partition.
