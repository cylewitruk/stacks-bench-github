# Design 0005: Job-model decomposition + task-kind platform

- **id:** `0005-task-kind-platform`
- **status:** `in_progress` (iteration **v10**)
- **unblocks:** `0019-block-validation-recipe`, `0031-reusable-build-jobs`
- **source:** roadmap-v6 (three-layer model) + the **v10 job-model realization**
  (2026-06, from the `0025`/`0031` warming pivot)

Two coupled graduations: **(1)** decompose the **job model** into orthogonal axes
— today's `JobKind` / `TriggerKind` each conflate several concepts — and **(2)**
graduate from a single-purpose benchmark runner into a multi-task platform (block
validation next, `0019`) via a task-kind registry. The **axes are the
foundation**; the registry + crate modularity ride on top.

## Why now — the realization

`0025`'s warming pivot (`0031`) surfaced it: a **daemon-initiated, build-only,
silent** job has no honest home, because the current enums each carry *multiple*
concepts:

- `TriggerKind {PrComment, BranchPush, TagCreated, SlackAdhoc, …}` = **source**
  (who asked) **+ intent** (why), entangled.
- `JobKind {AdHoc, Baseline}` = **result-role** (anchor-vs-not) — a *measurement*
  concept masquerading as a job *shape*.

So "build-only" doesn't fit `JobKind`, and "daemon warm" doesn't fit
`TriggerKind`. Bolting a clean `task_kind` next to the still-entangled enums would
be a worse *hybrid* than either extreme (and re-migrated again when `0019`/`0020`
land). The fix is to separate the axes once.

## The job-model axes

| Axis | Question | Values (today → near-term) | Storage |
| ---- | ---- | ---- | ---- |
| **source** | who requested it | `github_webhook` · `github_comment` · `slack` · `cli` · `scheduler` · `daemon` | PG enum `job_source` |
| **intent** | why + result-role | `adhoc_benchmark` · `baseline_benchmark` · `block_validation` · `cache_warm` | PG enum `job_intent` |
| **task_kind** | which VM workload runs | `benchmark` · `block_validation` · `build_only` | PG enum `task_kind` (recipe dispatch) |
| **build_target** | which artifact binary | `stacks_bench` · `stacks_inspect` | PG enum `build_target` |
| **report** | where lifecycle/results go (a **set**) | composed of `github_check` · `github_pr_comment` · `slack_thread` (or empty = silent) | **derived** (Rust set, no DB type) |

Key points:

- **`intent` absorbs `JobKind`.** `adhoc_benchmark` / `baseline_benchmark` replace
  `AdHoc` / `Baseline` (`baseline` = "eligible as a comparison anchor"; `adhoc` =
  "user-requested, don't anchor"). `JobKind` **retires**.
- **`task_kind` is the run-shape** (what the VM does after build → recipe
  dispatch). It correlates with `intent` but is its own concept:
  `cache_warm → build_only`; both benchmark intents → `benchmark`;
  `block_validation → block_validation`.
- **`report` is a derived surface *set*, not stored.** Resolved at claim from
  `f(source, intent, config)` into the reporter config (today's `ProgressTarget`),
  so it can't drift from config. It is **composite** — PR ad-hoc is *both*
  `github_check` **and** `github_pr_comment` (today's `PrReport::Both`; the v7
  refactor deliberately couples the GitHub check + comment as one surface), so it
  is a `ReportSurfaceSet`, **not** a scalar enum. That's also *why* it stays
  derived: a scalar PG enum would need a variant per combination
  (`github_check_and_pr_comment`, …); the only honest stored shape would be a
  one-to-many `job_report_surface` table — not worth it while the surfaces derive
  cleanly from the inputs. The independent stored *inputs* are `source` /
  `intent` / `task_kind` / `build_target`.
- **PG enums (`sqlx::Type`), one per axis** — `job_source` / `job_intent` /
  `task_kind` / `build_target`, decoded straight to typed Rust values
  (`JobSource` / `JobIntent` / `TaskKind` / `BuildTarget`). The DB enforces
  validity (an unknown value can't be written), and no raw strings reach
  creation/claim/reporting — Codex's "parse at the boundary, pass typed values" is
  the `sqlx` default. **Decision (reverses the original `0005` `TEXT`+registry
  call):** a new value is an additive `ALTER TYPE … ADD VALUE` migration, and
  that's fine — adding a kind *always* touches code (a `Recipe`, a handler), so
  "no schema change" was a false economy; this project **rolls forward** (the
  hard-to-reverse side of enum migrations doesn't bite); and it matches the
  existing schema (`JobStatus` / `JobKind` / `TriggerKind` / `GitRefKind` are
  already PG enums). The *behavior* mappings stay code-owned dispatch
  (`task_kind → Recipe`, `build_target → binary`, `(source, intent) → report
  surfaces`) — the registry is for behavior, not validity.

## Retiring `TriggerKind` / `JobKind` on jobs — migration map

Existing `job` rows map losslessly (`git_ref_kind` already carries branch-vs-tag,
so `BranchPush` / `TagCreated` collapse to `github_webhook` + the ref kind):

| old `(trigger_kind, job_kind)` | source | intent | task_kind | build_target |
| ---- | ---- | ---- | ---- | ---- |
| `PrComment`, `AdHoc` | `github_comment` | `adhoc_benchmark` | `benchmark` | `stacks_bench` |
| `BranchPush`, `Baseline` | `github_webhook` | `baseline_benchmark` | `benchmark` | `stacks_bench` |
| `TagCreated`, `Baseline` | `github_webhook` | `baseline_benchmark` | `benchmark` | `stacks_bench` |
| `SlackAdhoc`, `AdHoc` | `slack` | `adhoc_benchmark` | `benchmark` | `stacks_bench` |

**`git_ref_kind` survives — it is *not* retired.** `Branch` / `Tag` / `Commit`
stays a job column (a separate ref-shape axis); it's the only thing distinguishing
the two `github_webhook` baseline rows above. So **dedup/provenance keep
`git_ref_kind` (+ ref display) in the key wherever today's behavior does** — a
branch-push baseline and a tag-created baseline at the same commit/workload must
**not** start deduping differently than today.

**Boundary — `trigger_policy.trigger_kind` is a different concept.** There it's
the **event-match** (push / tag) a policy auto-fires on, not a job's source/intent.
Retiring `TriggerKind` *on jobs* does **not** require collapsing the policy matcher
in the same stroke — keep them distinct (or unify deliberately later).

## Three-layer model (the platform this graduates to)

```text
  Platform        — GitHub App, queue/lifecycle/timeline, coordinator/worker/
                    reporter, reporting, config, admin API/CLI   (task-agnostic)
  Stacks substrate — VM runtime, stacks node + chainstate snapshot, git mirror,
                    tmpfs results                                 (shared by every stacks task)
  Task kinds      — Recipe impls: bench · block-validation · build-only · (future)
                    each: phases + in-VM command + result schema + render
```

The middle layer is the point: the stacks node + chainstate is shared by *every*
stacks long-running task. A `task_kind` selects a `Recipe`; `build_only` is the
shape that produces an artifact and stops (no result schema, no render).

### Target crate map (illustrative; names pending Open question 1)

| New crate | From today | Holds |
| ---- | ---- | ---- |
| `sgh-core` | `sbgh-core` | GitHub App, db, config, models — task-agnostic |
| `sgh-engine` | `sbgh-daemon` engine | generic execution + reporting, generic over `Recipe` |
| `sgh-substrate` | `sbgh-daemon/libvirt` | VM runtime + stacks node/chainstate |
| `sgh-task-bench` | bench summary/template/`JobMetric` | `BenchRecipe` + result schema + render |
| `sgh-task-blockval` | new (`0019`) | `BlockValidationRecipe` + result + render |

## Phasing (v10)

Convergence rule: each slice moves **toward** the full axis model — never leave
source/intent entangled as a resting state.

- **Phase 1 — axes + migration (schema).** `CREATE TYPE` the `job_source` /
  `job_intent` / `task_kind` / `build_target` PG enums + the `job` columns + their
  `sqlx::Type` Rust enums; backfill existing rows from the map above
  (expand-migrate-contract). Old enums stay present, mapped, through the cutover.
- **Phase 2 — rewire the pipeline.** Job creation (the handlers), dedup
  (`find_active_job`), claim + **recipe selection by `task_kind`**, reporting
  (derive `report` from the axes), `QueuedEventDetail` provenance — switched to
  the new axes; drop the old enums once reads/writes are off them.
- **Phase 3 — first new consumers.** `build_only` + `build_target` (the `0031`
  warming path: a `source=daemon` / `intent=cache_warm` / `task_kind=build_only`
  job) and the recipe registry (`benchmark` registered; `block_validation` is
  `0019`).

The **crate split** (the table above) is a separable modularity refactor — it can
follow the axes (it doesn't gate them) and may be its own later iteration.

## Dependencies

- Rides the shipped `Recipe` boundary (`0008`) + `Driver` seam (`0010`).
- The second task kind (`0019`) forces the generic phase-event migration (`0017`);
  bench keeps its phase events, so the axes themselves don't hard-require it.

## Open questions

1. **Rename blast radius** — `sbgh-*` → `sgh-*` (incl. runtime paths) vs. keep
   `sbgh` internally and only brand the GitHub App `stacks-github`? (Orthogonal to
   the axes.)
2. **Registry vs. Cargo features** — one binary with all kinds compiled in vs.
   features that trim the binary.
3. **One queue or per-kind queues** — shared `job` queue + `task_kind` + one
   coordinator, vs. per-kind queues so multi-hour block-validations can't starve
   bench (fairness/admission — overlaps `0015`).
4. **`report` derived surface-set vs. stored** — recommend **derived** (no config
   drift); if ever stored it's a one-to-many `job_report_surface` table, **never**
   a scalar enum with a variant per combination.
5. **`trigger_policy` event-match** — keep distinct from the job source/intent
   axes, or unify later?
6. **`intent` granularity** — two sub-points: (a) is `cache_warm` its own `intent`
   value, or just `(source=daemon, task_kind=build_only)`? (Lean: explicit value —
   provenance + a home for future build intents like a CLI `manual_build`.) (b)
   **`block_validation` is a placeholder** — it reads like a task *shape*, not a
   why/result-role, so it risks `intent` re-absorbing `task_kind`. When `0019`
   lands, split it by *use* — `adhoc_block_validation` / `scheduled_block_validation`
   — keeping `intent` = "why" and `task_kind` = "what runs".
