# Backlog

**Unscheduled items only** (`backlog` / `candidate` / `parked`). Once an item is
selected it **moves** to an iteration; once terminal it moves to `archive/` — see
the single-home rule in the [README](README.md#item-ids). The complete registry
of every item (all statuses, incl. shipped/in-flight) is [index.md](index.md);
keep entries here compact and push worked-through detail to `design/`.

> **Transitional index (2026-06).** Each item below points at its existing
> `docs/roadmap-vN.md` for full detail; content conversion into `design/` +
> `decisions/` happens incrementally as we work an item. The `(was roadmap-vN)`
> note is the crosswalk. **Sequence is not yet pinned beyond "0001 first"** —
> priorities below are a starting proposal, not a committed order.

## Candidate (near-term)

### 0001 — Artifact store (object-storage-backed run artifacts)

- **id:** `0001-artifact-store`
- **status:** `candidate`
- **priority:** `high`
- **unblocks:** `0002-slack-adhoc-profiling`, `0003-results-portal`
- **design:** [docs/roadmap-v12.md](../docs/roadmap-v12.md) *(was roadmap-v12)*

**Problem:** Run artifacts (SQLite, `run.json`, flamegraph, binary, phase-log)
live on the orchestrator's local disk; neither Slack nor a portal can reach them
there.

**Scope:** A pluggable `ArtifactStore` (local default → S3-compatible / Hetzner
object storage), ship-on-completion, signed-URL fetch.

**Acceptance:** Behavior-preserving with `kind = "local"`; S3 opt-in; consumers
resolve via the store. **Foundational — build first.**

### 0002 — Slack ad-hoc profiling benchmarks

- **id:** `0002-slack-adhoc-profiling`
- **status:** `candidate`
- **priority:** `medium`
- **depends_on:** `0001-artifact-store` (Phase 4 — flamegraph delivery)
- **review:** `Codex signed off` (design)
- **source:** Codex-reviewed cluster (v12 → v10 → v11)
- **design:** [docs/roadmap-v10.md](../docs/roadmap-v10.md) *(was roadmap-v10)*

**Problem:** No-commit, ad-hoc "profile this tx/block from yesterday" requests
have no entry point; the team lives in Slack.

**Scope:** Socket Mode connector + `slack_adhoc` trigger (default rev, workload
via `--txid`/`--block`/`--repetitions`), a generalized `ReportSurface`, and a
flamegraph artifact.

**Acceptance:** `/bench …` in Slack returns a flamegraph for the workload.

## Backlog (unscheduled)

### 0003 — Results portal (web UI + GitHub login)

- **id:** `0003-results-portal`
- **status:** `backlog`
- **priority:** `medium`
- **depends_on:** `0001-artifact-store`
- **review:** `Codex signed off` (design)
- **design:** [docs/roadmap-v11.md](../docs/roadmap-v11.md) *(was roadmap-v11)*

**Problem:** No way to browse runs, watch the queue, or deep-inspect a run's
profile.

**Scope:** A permissioned portal (GitHub OAuth → existing roles, visibility-
scoped) that's an **API client of the orchestrator** (never a second DB client),
reusing the upstream `stacks-bench` explorer version-matched + proxied.

**Acceptance:** A logged-in user browses runs they may see and opens a profiler
trace.

### 0004 — Distributed worker fleet (`remote-daemon`)

- **id:** `0004-worker-fleet`
- **status:** `backlog`
- **priority:** `medium`
- **depends_on:** `0010-driver-seam` (shipped)
- **review:** `Codex signed off` (design)
- **design:** [docs/roadmap-v9.md](../docs/roadmap-v9.md) *(was roadmap-v9)*

**Problem:** A single host caps concurrency and can't serve heterogeneous
hardware (pinned bench boxes vs. big-local-NVMe block-val boxes).

**Scope:** Split `sbgh-daemon` into orchestrator + `sbgh-worker` (shared
`sbgh-exec`); thin pull-based worker API; capability matching; per-
`measurement_profile` baseline trust.

**Acceptance:** A remote worker runs a bench end-to-end; orchestrator stays the
sole DB client.

### 0005 — Block validation (second task kind)

- **id:** `0005-block-validation`
- **status:** `backlog`
- **priority:** `medium`
- **design:** [docs/roadmap-v6.md](../docs/roadmap-v6.md) +
  [docs/block-validation-taskspec.md](../docs/block-validation-taskspec.md)
  *(was roadmap-v6)*

**Problem:** Only one task kind (bench) exists; block validation is the planned
second.

**Scope:** A `BlockValidationRecipe` with a probe-driven, K-shard fan-out over
CoW chainstate workspaces; terminal semantics (invalid-blocks = red check, not
infra failure).

**Acceptance:** A new task kind costs ~one crate, no engine edits.

## Parked

### 0006 — AWS / cloud execution backend

- **id:** `0006-aws-cloud-backend`
- **status:** `parked`
- **priority:** `low`
- **depends_on:** `0004-worker-fleet` (returns as its worker provisioner)
- **design:** [docs/roadmap-v8.md](../docs/roadmap-v8.md) Phases 0, 3–6
  *(was roadmap-v8 cloud phases)*

**Problem:** Owned hardware caps elastic capacity.

**Scope:** EC2/EBS-from-snapshot provisioning.

**Deferred / non-goals:** Parked — returns later as a **worker provisioner** for
`0004`, gated on cost/variance/hydration data; not pursued standalone.
