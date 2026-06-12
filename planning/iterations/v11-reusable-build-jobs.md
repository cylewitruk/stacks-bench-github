# v11: Reusable build jobs (pin warming)

Successor to [v10 (0005)](../archive/completed/0005-task-kind-platform.md). Promote
artifact **production** to a first-class, daemon-initiated job so pinned release
binaries can be **pre-built (warmed)** into the cache — no fake webhook, no fake
measurement, no GitHub check. Canonical item:
[`0031-reusable-build-jobs`](../design/0031-reusable-build-jobs.md), re-cast onto
v10's shipped axes.

> **Status:** in_progress — **Phase 0 settled (Codex-reviewed); Phase 1 next.** v10
> shipped the build-only **run** path (`task_kind=build_only` → `BuildOnlyRecipe`,
> `Silent` reporting, fail-closed cache contract, `(task_kind, build_target)`
> dispatch). v11 is the **enqueue side + planner** — a thin consumer. It also
> closes v10's one deferred host-validation: the first warming run is the on-host
> proof that a freshly-built binary lands in the real cache.

## Items

| Item | Role | Status |
| ---- | ---- | ------ |
| `0031-reusable-build-jobs` | primary | in_progress |

## Why

`0025` (shipped) *protects* pinned release binaries from eviction, but a pin can
only protect a binary that **already exists**. On a cold host, a wiped cache, or a
ref pinned before it was ever benched here, there's nothing warm. Pre-building had
no honest home pre-`0005` (it forced a synthetic webhook, a meaningless
measurement, and an unwanted GitHub check) — but `task_kind=build_only` +
`source=daemon` / `intent=cache_warm` is exactly that home now.

## What v10 already built (do NOT rebuild)

- The build-only **run** path: provision → build → publish → stop (no bench VM),
  **fails closed** unless the artifact is cached (`reused || published`).
- **Silent** reporting (`ProgressTarget::Silent` → `NoopReportSurface`).
- `(task_kind, build_target)` dispatch (only `(BuildOnly, StacksBench)` is live).
- A **webhook-less insert** shape already exists (`create_adhoc_job`: job + queued
  event, no webhook/PR/owner links).

## Resolved design calls (were `0031`'s open questions)

- **Build cadence (OQ1)** → `task_kind=build_only` + `intent=cache_warm` (`0005`).
- **Inline vs enqueue-and-wait (OQ2)** → keep **inline**: a measurement job that
  finds no cached binary still builds inline. Warming only *pre-populates*;
  enqueue-and-wait is a fleet concern (`0004`), deferred.
- **Target ↔ task_kind (OQ3)** → 1:1; warming builds **`stacks-bench` only**.
- **Build-job reporting (OQ4)** → silent (v10). An operator "warm/cold" surface is
  **deferred** — logs + the cache itself are the audit for v11.
- **Fingerprint `target` field (OQ5)** → **deferred to `0019`**. Warming builds one
  target; the field isn't needed until `stacks-inspect` exists (YAGNI).

## Phase 0 design calls (Codex-reviewed, settled)

1. **Creation-path shape** — **rename `create_adhoc_job` → `create_unlinked_job`**
   (Phase 1): Slack is no longer the only no-link creator, so the neutral name is
   worth it now. Add a `QueuedEventDetail::CacheWarm` provenance variant recording
   **which pin / target / ref** caused the daemon enqueue (audit: *"the daemon
   built this pinned binary"*).
2. **In-flight dedup key** — a **dedicated** build-dedup query on active
   `(github_repo_id, git_commit_hash, task_kind=build_only, build_target)`. Do
   **not** overload `find_active_job` — its `workload_key` semantics belong to
   measurement jobs (and a build job's `workload_key` is `NULL`, so `= NULL` would
   never match anyway).
3. **Planner cadence** — **ride the existing pin-recompute hook** (startup + after
   each job): it already resolves `PinnedTarget`s and matches the pin lifecycle. A
   separate interval is easy to add later if warming needs to be more proactive.

## Scope (this iteration)

- A **webhook-less cache-warm creation path**: a `QueuedEventDetail::CacheWarm`
  variant + enqueue via the no-link insert. Axes: `source=daemon`,
  `intent=cache_warm`, `task_kind=build_only`, `build_target=stacks_bench`.
- A **warming planner**: on the pin-recompute hook, for each resolved
  `PinnedTarget` **missing from the cache** (`BinaryCache::has_entry_for`) and **not
  already in-flight** (the Phase-0 dedup key) → enqueue one build-only job.

**Out of scope (deferred):** the fingerprint `target` field + `stacks-inspect`
(`0019`); enqueue-and-wait / fleet build-vs-measure routing (`0004`); any operator
GitHub/Slack surface for build jobs.

## Phases

- **Phase 0 — design (this doc).** Resolve the three open points above + the
  phasing. Review before Phase 1.
- **Phase 1 — webhook-less cache-warm creation.** Rename `create_adhoc_job →
  create_unlinked_job`; add `QueuedEventDetail::CacheWarm` carrying the enqueue
  provenance (trigger/policy id, repo + ref, commit, `build_target` — enough to
  answer *"why did this build happen?"*); enqueue the build-only axes through the
  no-link path. **Note:** the bench-arg normalization paths (`normalize_stored*` /
  `resolve_bench_args`) match on `QueuedEventDetail`, so the new variant lands on
  those sites — `CacheWarm` returns **empty args** (build-only has no bench input).
  Per-path tests. **Acceptance:** a cache-warm job inserts with the right axes +
  provenance, empty bench args, and no webhook/PR/owner links.
- **Phase 2 — warming planner.** Resolve pins → skip-if-cached + skip-if-in-flight
  → enqueue, wired to the recompute hook. **Acceptance:** a pinned ref missing from
  the cache yields exactly one build-only job; none if cached or already in-flight.
- **Phase 3 — host-validation.** On the Hetzner host, a cold pinned ref warms
  (builds → publishes → the cache has the entry → pin recompute protects it),
  silently. **Closes v10's build-only-success thread.**

## Acceptance (iteration)

Pinned release binaries missing from the cache are pre-built by daemon-initiated,
**silent** build-only jobs — dedup'd against both cached and in-flight — so a cold
host / wiped cache / pre-benched pin becomes warm with no operator action, no fake
measurement, and no GitHub surface.
