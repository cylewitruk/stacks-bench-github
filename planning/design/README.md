# Design Docs

Optional detailed plans for backlog or iteration items.

Backlog entries and iteration item lists stay scannable and link here when an
item has worked-through design detail. A design doc isn't required for trivial
tasks.

Design docs use the same item ID as their backlog/iteration entry:

```text
planning/design/0003-results-portal.md
```

> **Current contents:** living design docs for the unshipped items —
> [`0003-results-portal`](0003-results-portal.md),
> [`0004-worker-fleet`](0004-worker-fleet.md),
> [`0005-task-kind-platform`](0005-task-kind-platform.md), and
> [`0019-block-validation-recipe`](0019-block-validation-recipe.md). Shipped items
> keep **no** live design here: `0001`/`0002` were folded into
> [archive/completed/](../archive/completed/) on ship and removed (per the rule
> below); their durable rules live in [decisions/](../decisions/).

Use this directory for living design detail. Once the work ships, consolidate the
backlog/iteration entry and this design into one `archive/completed/` file with
the same ID, then remove the live design doc (or move it to
`archive/superseded/` if it's still useful historical context).
