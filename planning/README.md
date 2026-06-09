# Planning System

This directory is the canonical repo-local planning system for
`stacks-bench-github` (sbgh). It exists so that *status* and *sequence* are
first-class — the older `docs/roadmap-vN.md` docs mixed durable decisions,
how-to design, phased execution, and status into one file each, which made them
read as a backlog rather than a roadmap.

Workflow: Opus drafts, Codex reviews, Opus fixes. Keep planning docs concise.

> **Transitional state (2026-06).** This is the **skeleton + scheme** only.
> [backlog.md](backlog.md) currently **indexes** the existing
> `docs/roadmap-vN.md` docs in place; their content is converted into
> `design/` + `decisions/` **incrementally, as we work each item**. Until an
> item is converted, its detail still lives in its linked `docs/roadmap-vN.md`.

## Where To Look

- [index.md](index.md) — **the registry**: every item's ID, status, and current
  location (the stable lookup other docs resolve IDs against).
- [backlog.md](backlog.md) — items captured but not assigned to an iteration.
- [iterations/](iterations/) — selected deliverables currently planned or in
  progress (the current-priority surface).
- [design/](design/) — optional detailed plans for backlog/iteration items.
- [decisions/](decisions/) — durable architecture decisions (ADRs).
- [archive/completed/](archive/completed/) — shipped work summaries.
- [archive/rejected/](archive/rejected/) — ideas we deliberately declined.
- [archive/superseded/](archive/superseded/) — historical plans kept for
  archaeology, not current instructions.

`docs/` keeps **reference** material (architecture, guides); planning lives here.

## Lifecycle

```text
backlog -> iteration -> archive/completed
        \-> archive/rejected
        \-> archive/superseded
```

## Item IDs

Implementation items use stable numeric IDs:

```text
NNNN-short-slug
```

Use the same ID everywhere an item appears:

- backlog metadata while unscheduled: `id: 0001-artifact-store`
- iteration item list once selected: `0001-artifact-store`
- optional design doc: `planning/design/0001-artifact-store.md`
- completed archive: `planning/archive/completed/0001-artifact-store.md`

**Single-home rule.** Each item's *full entry* exists in exactly **one** file at a
time — the one its status maps to (see [Statuses](#statuses)). Items are
**moved, not copied** across that boundary: promoting `candidate → planned`
*relocates* the full entry from `backlog.md` into its iteration file; archiving
*relocates* it under `archive/`. Never leave a copy behind, and update
[index.md](index.md) on every move.

**Pointers are references, never copies.** Any other doc (the roadmaps, design
docs, each other) refers to an item by its **stable ID** and resolves its current
status/location via [index.md](index.md) — so those docs never need editing when
an item moves. A pointer is a one-line link, never a second copy of an item's
problem/scope/acceptance.

Iterations are different: they group one or more items into a deliverable and use
their own `vN-*` names. **Decisions** have their own `000N` namespace, separate
from item IDs.

## Statuses

Status is execution state, and **status determines location** — an item's full
entry lives in exactly the file its status maps to:

| Status | Meaning | Lives in |
| ------ | ------- | -------- |
| `backlog` | captured, unscheduled | `backlog.md` |
| `candidate` | plausible near-term, still unscheduled | `backlog.md` |
| `parked` | deliberately deferred — known direction, not now | `backlog.md` |
| `blocked` | needs a decision / external input | *(its current file — unchanged)* |
| `planned` | selected into an iteration | `iterations/vN-*.md` |
| `in_progress` | actively being implemented | `iterations/vN-*.md` |
| `shipped` | implemented + archived | `archive/completed/NNNN-*.md` |
| `superseded` | replaced by a newer plan/design | `archive/superseded/` |
| `rejected` | intentionally not pursued | `archive/rejected/` |

When a status change crosses a location boundary, the item is **moved** (the
single-home rule above) and [index.md](index.md) is updated.

**Transitional exception (migration only):** the pre-planning-system `shipped`
items (`0007`–`0010`) are still indexed at their original `docs/roadmap-vN.md` in
[index.md](index.md) until converted to `archive/completed/`. That's the one
place location lags this table during the migration; new `shipped` items go
straight to `archive/completed/`.

## Item Template

```md
### Short Title

- **id:** `NNNN-stable-kebab-id`
- **status:** `candidate`
- **priority:** `medium`
- **depends_on:** optional item IDs this needs first
- **unblocks:** optional item IDs this enables
- **blocked_by:** optional — a decision/external input it waits on
- **review:** optional — `Codex pending` | `Codex signed off`
- **source:** optional link to a decision, doc, review, or archive note
- **design:** optional link to a detailed design doc (or the current `docs/roadmap-vN.md`)

**Problem:** What is wrong or missing?

**Scope:** What this item would change.

**Acceptance:** Observable checks that make it done.

**Deferred / non-goals:** What this item explicitly leaves out.
```

Keep entries scannable; move worked-through detail to a `design/` doc and durable
rationale to a `decisions/` ADR.

## Decisions vs. Design

- A **decision** records a long-lived architectural rule or tradeoff that can
  feed several items and usually outlives the item that triggered it →
  [decisions/](decisions/).
- A **design** plans *how* to implement one item → [design/](design/).

## Promoting an Item (backlog → iteration)

When a candidate becomes real work:

1. Create/extend the iteration file (`iterations/vN-slug.md`) and **move** the
   full entry out of `backlog.md` (single-home rule).
2. Convert the item's `docs/roadmap-vN.md` into `design/NNNN-slug.md`, then
   shrink/remove the roadmap source so it isn't read as current.
3. Extract any durable rules the work touches into [decisions/](decisions/) ADRs.
4. Define **validation / acceptance before coding**.
5. Update [index.md](index.md) (status + location).

## Archiving Items

When an item leaves backlog or an iteration:

1. Create one archive file named with the same item ID under
   `archive/{completed,rejected,superseded}/`.
2. Start with the item metadata + a concise problem/scope summary.
3. Fold in the matching design doc when one existed and it's still useful.
4. Record what shipped, validation evidence, notable deviations, follow-ups.
5. Remove or mark superseded the live design doc so it isn't read as current.
