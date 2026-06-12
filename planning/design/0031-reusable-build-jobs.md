# Design 0031: Reusable build jobs (artifact production + target axis)

- **id:** `0031-reusable-build-jobs`
- **status:** `in_progress` (iteration **v11**)
- **priority:** `medium`
- **depends_on:** `0005-task-kind-platform` (**shipped** — the job-model axes this
  rides: `source` / `intent` / `task_kind` / `build_target`)
- **relates_to:** `0025-baseline-binary-cache` (consumes its cache; supersedes its
  *warming*), `0019-block-validation-recipe` (second `build_target`),
  `0004-worker-fleet` (route build vs measure separately)
- **source:** v9 (`0025`) Phase-2 warming pivot, 2026-06

> **Scope + phasing live in the iteration:**
> [iterations/v11-reusable-build-jobs.md](../iterations/v11-reusable-build-jobs.md).
> This doc is the durable **design rationale**; the open questions it once carried
> are resolved by `0005`'s axes (see the iteration's "Resolved design calls").

Promote **artifact production** to a first-class, daemon-initiated job, separate
from **measurement**, so warming a pinned release binary has an honest home — no
fake webhook, no fake measurement, no GitHub check.

## Insight: split production from measurement

Two independent things are tangled in today's "job":

- **Produce** the artifact — build (+ cache) the binary for a `(build_target,
  commit, build-env)`.
- **Measure** with it — benchmark (`stacks-bench`) or block-validate
  (`stacks-inspect`, `0019`), consuming the artifact.

Decoupling them gives: no fake webhook, no fake measurement, no GitHub check, a
clear audit trail (*"the daemon built this pinned binary"*), and a fleet that can
route build vs measure to different hardware (`0004`).

In `0005`'s axes this is `task_kind = build_only` (the run-shape) producing a
`build_target` binary; warming is `source = daemon` / `intent = cache_warm`. **v10
shipped the build-only *run* path** (build → publish → stop, silent, fail-closed);
v11 adds the **enqueue side + warming planner**.

## Where it sits among the axes

```text
  task_kind    (0005) — WHAT to measure:   benchmark · block-validation · …
  build_target (0005) — WHAT to produce:   stacks-bench · stacks-inspect · …
  backend      (0010) — WHERE to run:      libvirt · (worker fleet, 0004)
```

A build job is `{build_target} × {backend}`; a measurement job is `{task_kind} ×
{backend}` that *consumes* a `{build_target}` artifact. Targets:

| Target | Consumed by | Status |
| ---- | ---- | ---- |
| `stacks-bench` | benchmarking (`0025`) | **live** (v10) |
| `stacks-inspect` | block validation (`0019`) | future |

**Measurement jobs prefer the cached artifact**, building inline only on a miss —
today's `0025` build-skip, generalized per target. Warming just **pre-populates**
that cache; it does not change the inline-on-miss behavior. Cache fingerprints gain
a `target` field when `stacks-inspect` lands (`0019`); today it's implicitly
`stacks-bench`.

## Groundwork in place (banked during v9 / `0025` + v10 / `0005`)

- **`pin_resolver::PinnedTarget`** — resolved pin provenance (trigger/install/repo,
  ref kind+name, commit, bench args): the warm-planner input.
- **`BinaryCache::has_entry_for(commit, env)`** — the repo-agnostic skip-if-cached
  check.
- The **pin recompute** resolves targets on startup + after each job — the warm
  planner rides that same hook (resolve once → protect → warm).
- **v10 shipped:** the build-only run path, `Silent` reporting (no GitHub/Slack),
  the fail-closed cache contract (`reused || published`), `(task_kind,
  build_target)` dispatch, and a webhook-less insert shape (`create_adhoc_job`).
