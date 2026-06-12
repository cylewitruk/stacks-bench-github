# v10: Job-model decomposition

Successor to [v9-baseline-binary-cache](v9-baseline-binary-cache.md). Decompose
the **job model** into orthogonal axes — today's `JobKind` / `TriggerKind` each
conflate several concepts — so the platform can honestly express build-only jobs
(`0031` warming), block validation (`0019`), and daemon/CLI-initiated work without
bending measurement-shaped, webhook-coupled enums. Canonical item:
[`0005-task-kind-platform`](../design/0005-task-kind-platform.md) (redesigned).

> **Status:** in_progress — **Phases 0–3 landed** (axes + migration, the
> expand-migrate-contract pipeline rewire, and the build-only proof). The
> highest-blast-radius change in the system (job creation, dedup, claim,
> reporting, provenance, **+ a migration of every existing `job` row**); the
> model + migration map were reviewed before Phase 1. Replaces the parked
> "v10-reusable-build-jobs" — that work (`0031`) is a *consumer* of these axes and
> follows. **Pending host-validation:** the build-only path's actual cache-write
> byte-landing (like v9 pin-protect — the engine wiring is unit-proven).

## Items

| Item | Role | Status |
| ---- | ---- | ------ |
| `0005-task-kind-platform` | primary | in_progress |

## Why

The `0025`/`0031` warming pivot surfaced that `JobKind {AdHoc, Baseline}` is a
*result-role* and `TriggerKind {PrComment, BranchPush, …}` is *source + intent*,
entangled. A daemon-initiated, build-only, silent job fits **neither** — and
bolting a clean `task_kind` next to the still-entangled enums is a worse *hybrid*
than either extreme (and re-migrated again when `0019`/`0020` arrive). Decompose
the axes **once**.

## Scope (this iteration)

Land the **job-model axes** (see the [`0005` design](../design/0005-task-kind-platform.md#the-job-model-axes)):

- `source` · `intent` · `task_kind` · `build_target` as **PG enums**
  (`sqlx::Type` Rust enums — DB-enforced validity, typed values everywhere);
  `report` **derived** at claim (a Rust surface set, not stored).
- **Retire `JobKind` / `TriggerKind` on jobs** — migrate every existing row via
  the lossless map; keep `trigger_policy`'s event-match distinct.
- Prove the model with **one new shape**: a `task_kind=build_only` job runs
  (build VM → publish → stop, silent) — the `0031` warming primitive.

**Out of scope (deferred):** the crate split (`sgh-*` modularity — a separable
refactor); the `block_validation` recipe (`0019`); full pin warming + the picker
(`0031` beyond the build-only proof); the `sbgh→sgh` rename (Open question 1).

## Phases

### Phase 1 — axes + migration (schema)

`CREATE TYPE` the `source` / `intent` / `task_kind` / `build_target` PG enums +
the `job` columns + their `sqlx::Type` Rust enums; backfill existing `job` rows
from the map (expand-migrate-contract); old enums stay present + mapped through
the cutover. **Acceptance:** every existing row has a
correct axis tuple; nothing reads the new columns yet (behavior-preserving).

### Phase 2 — pipeline rewire

Switch job creation (handlers), dedup, claim + **recipe selection by `task_kind`**,
reporting (derive the `report` **surface set** from the axes — composite-capable,
e.g. PR ad-hoc = check **+** comment), and `QueuedEventDetail` provenance to the
new axes; drop the old enums once reads/writes are off them. **Acceptance:** the
three live flows — PR-comment adhoc, branch-push baseline, Slack adhoc — run
**unchanged** through the new axes (same reporting surfaces, same baselines).
Branch-push and tag-created baseline **dedup/provenance stay behavior-equivalent**
— `git_ref_kind` / ref display remain in the key wherever today's dedup uses them.

### Phase 3 — prove build-only

A `task_kind=build_only` job (`source=daemon`, `intent=cache_warm`,
`build_target=stacks_bench`, `report=none`) builds + publishes the binary and
terminal-completes, **silently**. **Acceptance:** a manually-enqueued build-only
job caches a binary and posts nothing — the primitive `0031` warming will enqueue.

## Convergence rule

Each slice moves **toward** the full axis model — never ship source/intent
entangled as a resting state. The model isn't "done" until the old enums are gone.

## Acceptance (iteration)

The `job` model is decomposed into `source` / `intent` / `task_kind` /
`build_target` (+ derived `report`); the existing measurement flows are
byte-equivalent through the new axes; and a **build-only** job is expressible +
runnable — unblocking `0031` (warming) and `0019` (block validation) as thin
consumers, not engine changes.
