# Decisions

Durable architecture decisions (ADRs) live here. Decisions are separate from
implementation items: their `000N-*` filenames are a **decision namespace**, not
the backlog/iteration item-ID namespace.

Use a decision for a long-lived architectural rule or tradeoff. Use a
[design](../design/) doc for *how* to implement one item.

Suggested shape:

```md
# Decision 000N: Title

- **status:** draft | accepted | superseded
- **date:** YYYY-MM
- **related items:** optional item IDs

## Decision

## Rationale

## Consequences
```

Accepted decisions usually stay in place even after the item that triggered them
ships.

## Candidates to extract (from the current `docs/roadmap-vN.md`)

These durable rules are buried in the roadmaps' "Decisions" sections; pull each
into an ADR as we work the related item (skeleton phase — not yet extracted):

- **Task ⟂ backend (`Recipe` vs `Driver`)** — the execution seam (roadmap-v8).
- **Orchestrator is the sole DB client; workers pull via a thin API** — not
  shared-Postgres claiming (roadmap-v9).
- **`remote-daemon` is a distribution layer, not a `Driver` kind** (roadmap-v9).
- **`measurement_profile`** — operator-declared comparability label with a
  per-profile noise floor, *not* a per-box fingerprint (roadmap-v9).
- **Cloud is a worker provisioner, not an execution path** (roadmap-v8/v9).
- **Canonical comparison metric = `execution + commit`** (not wall-clock)
  (roadmap-v7 + the variance baseline).
- **Baseline selection: SHA-primary, repo-agnostic, target-branch-anchored**
  (roadmap-v7).
- **Reuse the upstream `stacks-bench` explorer (version-matched, proxied) — don't
  port the viewer** (roadmap-v11).
- **Slack = trigger source + reporting surface, not a new task kind; Socket
  Mode transport** (roadmap-v10).

*Extracted so far (from `0001-artifact-store`):* `0001-artifact-urls-s3-only`,
`0002-artifact-refs-are-store-keys`,
`0003-artifact-export-failure-not-benchmark-failure`.
