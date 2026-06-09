# Design 0005: Task-kind platform (registry + job typing)

- **id:** `0005-task-kind-platform`
- **status:** `backlog`
- **unblocks:** `0019-block-validation-recipe`
- **source:** roadmap-v6 (Phases 1–2 + the three-layer model)

Graduate the app from a single-purpose **benchmark** runner into a multi-task
platform: a task-kind registry + job typing, so adding a kind (block validation
next, `0019`) is ~a new crate, not an engine change. *(Converted from the former
`docs/roadmap-v6.md`; the block-validation recipe itself is `0019`.)*

## Three-layer model

```text
  Platform        — GitHub App, queue/lifecycle/timeline, coordinator/worker/
                    reporter, reporting, config, admin API/CLI   (task-agnostic)
  Stacks substrate — VM runtime, stacks node + chainstate snapshot, git mirror,
                    tmpfs results                                 (shared by every stacks task)
  Task kinds      — Recipe impls: bench · block-validation · (future)
                    each: phases + in-VM command + result schema + render
```

The middle layer is the point: the stacks node + chainstate is shared by *every*
stacks long-running task, so it's its own substrate beneath the generic platform.

## Target crate map (illustrative; names pending Open question 1)

| New crate | From today | Holds |
| ---- | ---- | ---- |
| `sgh-core` | `sbgh-core` | GitHub App, db, config, models — task-agnostic |
| `sgh-engine` | `sbgh-daemon` engine | generic execution + reporting, generic over `Recipe` |
| `sgh-substrate` | `sbgh-daemon/libvirt` | VM runtime + stacks node/chainstate |
| `sgh-task-bench` | bench summary/template/`JobMetric` | `BenchRecipe` + result schema + render |
| `sgh-task-blockval` | new (`0019`) | `BlockValidationRecipe` + result + render |

The daemon composes `sgh-engine` + `sgh-substrate` + a registry of task crates;
adding a kind = add a `sgh-task-*` crate + register it.

## Phase 1: Crate split (no behavior change; naming TBD)

Cut the crates along the `Recipe`/`Driver` boundary (the seam shipped as `0008` +
`0010`), zero behavior change. Pure mechanical refactor. **The `sbgh-*` → `sgh-*`
rename is *not* committed here** — it's Open question 1; Phase 1 can split the
crates without renaming them (split first, decide naming separately). **No DB
migration here** — the `task_kind` dimension lands in Phase 2.

## Phase 2: Task-kind registry & job typing

- **Migration:** add `task_kind` to `job` as **`TEXT` (not a PG enum)**, defaulting
  existing rows to `benchmark` (additive). Distinct from the existing `JobKind`
  *cadence* enum (`AdHoc`/`Baseline`).
  - **Pinned decision:** `TEXT` validated by an app-side `TaskKindRegistry`, not a
    PG enum — a PG enum would force an `ALTER TYPE` per new kind, undercutting
    "add a task crate + register it, no schema change." If DB-visible kinds are
    ever wanted, promote to a `task_kind` **lookup table** (FK), still no
    per-kind enum migration.
- A `TaskKindRegistry` (`task_kind` → `Recipe` + command(s) + trigger policies);
  bench registers `benchmark` + `/benchmark`.
- Generalize the command handler to dispatch `/<command>` via the registry; the
  coordinator selects the `Recipe` by the claimed job's `task_kind`.

## Dependencies

- Rides the shipped `Recipe` boundary (`0008`) + `Driver` seam (`0010`).
- The **second task kind** (`0019`) forces the **generic phase-event** migration
  (`0017`) — bench keeps its phase events, so `0005` itself doesn't hard-require it.

## Open questions

1. **Rename blast radius** — full `sbgh-*` → `sgh-*` (incl. runtime paths) vs. keep
   `sbgh` internally and only brand the GitHub App `stacks-github`?
2. **Registry vs. Cargo features** — one binary with all kinds compiled in
   (simpler ops) vs. features that trim the binary?
3. **One queue or per-kind queues** — a shared `job` queue + `task_kind` + one
   coordinator, vs. per-kind queues/limits so multi-hour block-validations can't
   starve bench? (Fairness/admission — overlaps `0015`.)
