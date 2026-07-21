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
> [`0019-block-validation-recipe`](0019-block-validation-recipe.md), and
> [`0052-managed-stacks-node-chainstate-producer`](0052-managed-stacks-node-chainstate-producer.md).
> Shipped items
> keep **no** live design here: on ship, each is folded into
> [archive/completed/](../archive/completed/) and removed (per the rule below);
> durable rules live in [decisions/](../decisions/).

Use this directory for living design detail. Once the work ships, consolidate the
backlog/iteration entry and this design into one `archive/completed/` file with
the same ID, then remove the live design doc (or move it to
`archive/superseded/` if it's still useful historical context).
