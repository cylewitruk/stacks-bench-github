# Iterations

Concrete deliverable specs live here — the **current-priority surface**. An
iteration may contain one or more backlog items, but it should have one
validation story and one clear archive destination.

Use `vN-short-name.md` naming. Iterations are deliverables, not item IDs; they
reference the numbered items they implement.

> **`vN` continues the project's deployment-version lineage, it does *not* reset
> to 1.** The last deployed version was **v3** (see `docs/v2-to-v3-upgrade.md`), so
> deliverables start at **v4** (`v4-artifact-store`). Each iteration is the next
> version milestone; the canonical item identity is still its `NNNN` id.

When an iteration ships, archive each completed item under its own `NNNN-slug.md`
in `archive/completed/`. Keep the iteration file only if it's still useful as a
validation recipe; otherwise move it to `archive/superseded/`.

> **Active:** [v8-slack-card-redesign](v8-slack-card-redesign.md)
> (`0023-slack-card-redesign`, planned). Shipped: v4 (`0001-artifact-store`),
> v5 (`0002-slack-adhoc-profiling`), v6 (`0021-slack-live-timeline`), v7
> (`0022-report-surface-trait`) → [archive/completed/](../archive/completed/).

## Iteration Template

```md
# vN: Deliverable Name

Successor to optional previous iteration. One-paragraph goal.

> **Status:** planned | in_progress | shipped | superseded
>
> Short current-state note: what shipped, what remains, where follow-up moved.

## Items

| Item | Role | Status |
| ---- | ---- | ------ |
| `NNNN-item-id` | primary | planned |

## Why

The problem this iteration solves.

## Scope

What changes in this deliverable.

## Phases

### Phase 1: Name

**Goal:** Concrete objective.

**Scope:**

- Work item.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests, if applicable
- [ ] Reviewed (Codex)
- [ ] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [ ] Acceptance criterion + how it's validated.

**Tests:**

- `crates/…/tests/some_test.rs` (the test name), or a manual/smoke check.

**Notes:** Optional.

## Final Validation

Observable checks for the whole iteration.

## Follow-Ups

New item IDs or backlog entries produced by this iteration.
```
