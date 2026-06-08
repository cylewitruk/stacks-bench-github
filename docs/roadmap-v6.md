# Roadmap 6 — `stacks-github`: a multi-task platform

Successor to [roadmap-v5.md](./roadmap-v5.md). Goal: graduate the app from a
single-purpose **benchmark** runner into a **`stacks-github`** platform that
hosts several long-running task kinds ill-suited to GitHub-hosted runners —
benchmarking first, **block validation** next — over one shared GitHub-App +
queue + VM-execution substrate.

Process unchanged: Opus implements, Codex reviews, Opus fixes.

> **Status: planned — design sketch, not scheduled.** This depends on
> [roadmap-v5.md](./roadmap-v5.md) landing the **task-agnostic worker/reporter
> boundary** (v5's "Forward-looking" constraints). Once that seam holds, the work
> here is largely **mechanical** (crate split + rename) plus **one proof** that
> the seam is real (a second task kind). Do not start before v5 Phases 1–3.

## Why

The bench-specific surface area is small and well-clustered; ~80% of the tree is
already a generic long-running-GitHub-task platform (the analysis is in v5's
forward-looking section). Adding **block validation** — replay/validate stacks
blocks against a chainstate, a multi-hour job — should reuse *everything* except
three pluggable concerns:

- **Trigger / command** — `/validate-blocks` (and/or a policy trigger) vs
  `/benchmark`.
- **Run recipe** — what runs in the VM, its phases, its resource shape, its
  done-detection.
- **Result + render** — the typed result schema + the check/comment rendering.

If v5's `Recipe` boundary holds, a new kind is "implement `Recipe` + register a
command + add a result table" — no engine changes. This roadmap proves that and
draws the crate lines that make it obvious.

## Target architecture: three layers

```text
  ┌─ Platform ────────────────────────────────────────────────────────────┐
  │  GitHub App (auth, webhook ingest, installs, repos, users, policy),    │
  │  job queue + lifecycle + timeline, coordinator/worker/reporter,        │
  │  GitHub reporting (checks + comments), config, admin API/CLI           │
  └───────────────────────────────────────────────────────────────────────┘
        ┌─ Stacks substrate ───────────────────────────────────────────────┐
        │  VM runtime (provision/define/start/poll/teardown/cleanup),       │
        │  stacks node + chainstate LVM snapshot, git mirror, tmpfs results │
        └───────────────────────────────────────────────────────────────────┘
              ┌─ Task kinds (Recipe impls) ──────────────────────────────────┐
              │  bench  ·  block-validation  ·  (future)                      │
              │  each: phases + in-VM command + result schema + render        │
              └───────────────────────────────────────────────────────────────┘
```

The middle layer matters: the **stacks node + chainstate** is shared by *every*
stacks long-running task, not just bench — so it's its own substrate beneath the
generic platform, not part of either the platform or a single task kind.

## Target crate map

A cut-along-the-`Recipe`-boundary split of today's crates. Names neutralise the
`sbgh` (stacks-**bench**-github) prefix to `sgh` (stacks-github):

| New crate | From today | Holds |
| ---- | ---- | ---- |
| `sgh-core` | `sbgh-core` (most) | GitHub App, db (queue/policy/installs/…), config, models — task-agnostic |
| `sgh-engine` | `sbgh-daemon` (coordinator/worker/reporter, progress) | the generic execution engine + reporting, generic over `Recipe` |
| `sgh-substrate` | `sbgh-daemon/libvirt` | VM runtime + stacks node/chainstate plumbing |
| `sgh-task-bench` | `sbgh-daemon/bench_summary`, run template, `JobMetric` | `BenchRecipe` + bench result schema + render |
| `sgh-task-blockval` | new | `BlockValidationRecipe` + its result schema + render |
| `sgh-handler` / `sgh-cli` / `sgh-api` / `sgh-smee` | rename in place | unchanged roles |

The daemon binary composes `sgh-engine` + `sgh-substrate` + a registry of the
task crates. Adding a kind = add a `sgh-task-*` crate + register it.

---

## Phase 1: Crate split & rename (no behaviour change)

**Goal:** Cut the crates along the v5 `Recipe` boundary and rename `sbgh-*` →
`sgh-*`, with zero behaviour change. Pure mechanical refactor — the seam already
exists from v5; this makes it a compile-enforced module boundary.

**What:**

- Extract `sgh-engine` (generic execution + reporting) and `sgh-substrate` (VM +
  stacks node/chainstate) out of `sbgh-daemon`.
- Extract `sgh-task-bench` (the `BenchRecipe`, `JobMetric`, render, run template).
- Rename `sbgh-*` → `sgh-*` across the workspace + scripts (`install-daemon.sh`,
  unit files, `just` recipes, config paths under `/var/lib/sbgh` → decide:
  migrate vs alias).
- Update [host-bringup.md](./host-bringup.md), README, AGENTS.md for the rename.

**Design notes:**

- The rename touches runtime paths (`/var/lib/sbgh`, the service name, the cookie
  path). Decide a migration story (symlink/alias for one release, or a clean
  cutover with a documented manual step) — call it out for Codex.
- **No DB migration *in this phase*** — the crate split + rename is pure code/
  path churn. The schema's task-agnosticism is only *partly* in place after v5
  (v5's additive `phase_started`/`phase_finished` enum migration); the `task_kind`
  dimension is **not** in the schema yet — that migration lands in **Phase 2**,
  not here. (Today's [models.rs:468](../crates/sbgh-core/src/models.rs#L468)
  `JobKind {AdHoc, Baseline}` is trigger *cadence*, orthogonal to task *type*.)

**Status:**

- [ ] Initial implementation completed
- [ ] Integration coverage (workspace builds + full suite green post-split)
- [ ] Reviewed — Codex signed off
- [ ] Complete

---

## Phase 2: Task-kind registry & job typing

**Goal:** Make "which task kind" a first-class, registered dimension of a job, so
the engine dispatches to a `Recipe` by kind.

**What:**

- **Migration:** add a `task_kind` column to `job`, **`TEXT` (not a Postgres
  enum)**, **defaulting existing rows to `benchmark`** — additive, no data
  rewrite. This is distinct from the existing `JobKind` cadence enum
  ([models.rs:468](../crates/sbgh-core/src/models.rs#L468)).
  - **Pinned decision (resolving Codex's storage finding):** `TEXT` validated by
    the app-side `TaskKindRegistry`, **not** a PG enum — otherwise every new task
    kind would need an `ALTER TYPE` migration, which directly undercuts the
    "add a task crate + register it, no engine/schema change" goal
    ([§ target crate map](#target-crate-map)). If DB-visible configured kinds are
    ever wanted, promote to a `task_kind` **lookup table** (FK from `job`) — still
    no enum migration per kind, just an `INSERT`. Validation stays in the registry
    either way.
- Add a `TaskKindRegistry` (`task_kind` → `Recipe` + its command(s) + its trigger
  policies). Bench registers `benchmark` + `/benchmark`.
- Generalise the command handler ([webhook_processor.rs](../crates/sbgh-daemon/src/webhook_processor.rs))
  to dispatch `/<command>` via the registry instead of a hardcoded
  `IssueCommentHandler` branch.
- The coordinator selects the `Recipe` by the claimed job's `task_kind`.

**Design notes:**

- This is the v5 forward-looking constraints 3–5 fully realised at the schema +
  registry level (v5 only needs them honoured in code; v6 makes them structural).
- Per-kind result tables: `job_metric` stays bench's; block-val's lands in Phase 3.

**Status:**

- [ ] Initial implementation completed
- [ ] Integration coverage added (or N/A justified)
- [ ] Reviewed — Codex signed off
- [ ] Complete

---

## Phase 3: Block validation as the second task kind

**Goal:** Prove the seam — add block validation reusing the entire platform +
substrate, touching only a new `sgh-task-blockval` crate + a registration + a
result table.

**What:**

- `BlockValidationRecipe: Recipe` — its phases (build → dataset → probe/plan →
  K-shard fan-out → reduce), its per-shard command, its done-detection, its
  resource shape (workspaces / shards / concurrency as separate counts).
- Its result schema (own table) + check/comment render.
- **Terminal semantics are explicit (block-val sketch finding 7).**
  Invalid-blocks-found is a **completed task with a negative result → red GitHub
  check**, *not* an errored/retryable job. Only VM-died / transport-failed /
  timeout is a driver/execution failure. The Recipe maps the driver's raw
  per-shard outcomes to this verdict; the driver never decides pass/fail. This
  keeps block-val failures off the reporting non-fatal/retry machinery meant for
  infra faults.
- Register `block_validation` + `/validate-blocks` (and/or a trigger policy).
- The only platform/substrate change permitted is *additive*; if Phase 3 needs to
  modify `sgh-engine` or `sgh-substrate`, that's a **boundary leak** to fix in the
  abstraction, not patch in the task — treat it as a finding.

**Design notes:**

- Task shape pre-sketched in
  [block-validation-taskspec.md](./block-validation-taskspec.md) (distilled from
  `stacks-core:contrib/tools/block-validation.sh`) — used to validate the
  roadmap-v8 Phase 1 `Driver` seam against a second, non-bench task before
  extraction. Key divergence from bench: a **K-shard parallel** work phase with a
  **dataset-derived partition** (not a single run), and **full-chainstate + N
  CoW clones** provisioning (Open question #2 below).
- This phase is the acceptance test for v5+v6: a new long-running task kind should
  cost ~one crate. Any engine edit it forces is a design defect, logged.
- Resource shape (multi-hour, different vcpu/memory) feeds v5 Phase 5's
  resource-aware admission — heterogeneous kinds make that more than a nicety.

**Status:**

- [ ] Initial implementation completed
- [ ] Integration coverage added (or N/A justified)
- [ ] Reviewed — Codex signed off
- [ ] Complete

---

## Dependencies & related work

- **Hard dependency on [roadmap-v5.md](./roadmap-v5.md)** — the `Recipe` boundary
  (v5 forward-looking constraints) and the coordinator/worker/reporter. v6 Phase 1
  is a no-op without it.
- **roadmap-v4** reporting (`[reporting]`, check runs) is task-agnostic and is
  reused verbatim per kind.
- **v5 Phase 5 (resource-aware admission)** becomes more valuable here — bench and
  block-validation have very different resource/duration shapes.

## Open questions (for Codex)

1. **Rename blast radius.** Is a `sbgh-*` → `sgh-*` rename worth the runtime-path
   churn (`/var/lib/sbgh`, service name, cookie), or keep `sbgh` internally and
   only brand the GitHub App as `stacks-github`? (Cosmetic vs. structural.)
2. **Substrate scope.** Is the stacks node/chainstate genuinely shared by block
   validation in the same shape as bench, or does block-val want a different
   chainstate provisioning path? (Decides how clean the `sgh-substrate` cut is.)
3. **Registry vs. features.** Task kinds as a runtime registry (one binary,
   all kinds compiled in) vs. Cargo features (build only the kinds you deploy)?
   Runtime registry is simpler operationally; features trim the binary.
4. **One queue or per-kind queues.** Single `job` queue with a `task_kind`
   column + a shared coordinator, or per-kind queues/limits (so a flood of
   multi-hour block-validations can't starve bench)? Fairness/admission policy.
