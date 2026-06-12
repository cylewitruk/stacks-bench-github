# Design 0031: Reusable build jobs (artifact production + target axis)

- **id:** `0031-reusable-build-jobs`
- **status:** `backlog`
- **priority:** `medium`
- **depends_on:** `0005-task-kind-platform` — `build_only` is a `task_kind` and
  warming is a `source=daemon` / `intent=cache_warm` job; both need `0005`'s
  redesigned **job-model axes** (source / intent / task_kind / build_target /
  report) to exist first. This item is the **first consumer** of that model.
- **relates_to:** `0025-baseline-binary-cache` (consumes its cache; supersedes its
  *warming*), `0019-block-validation-recipe` (second `build_target`),
  `0004-worker-fleet` (route build vs measure separately)
- **source:** v9 (`0025`) Phase-2 warming pivot, 2026-06

> **Sequencing (2026-06):** this item is **parked behind `0005`**. The warming
> design below is sound, but it can't be built honestly until the job model is
> decomposed — `JobKind::Build`-vs-`job_class` and the source/intent/report axes
> are `0005`'s redesign, not this item's. Once `0005` lands the axes, warming is a
> thin first consumer: enqueue a `task_kind=build_only` job. The `task_kind` /
> `build_target` framing here folds into `0005`.

Promote **artifact production** to a first-class job, separate from
**measurement**. Today the only durable unit is "a benchmark job that happens to
build first", so *warming* a pinned release binary has no clean home — it forces
a fake webhook, a fake baseline measurement, and an unwanted GitHub check. A
`Build` job removes the square-peg.

## Problem

`0025` (shipped) caches a built `stacks-bench` binary and **protects** pinned
release binaries from eviction. But a pin can only protect a binary that already
exists — on a cold host / wiped cache / a ref pinned before it was benched here,
there's nothing warm. *Warming* (pre-building) has no honest home: the only
job-creation path is webhook-coupled and measurement-shaped, so warming-as-a-
baseline-job needs a synthetic webhook, produces a meaningless measurement, and
posts a GitHub check the operator never asked for.

## Insight: split production from measurement

Two independent things are tangled in today's "job":

- **Produce** the artifact — build (+ cache) the binary for a `(target, commit,
  build-env)`.
- **Measure** with it — benchmark (`stacks-bench`) or block-validate
  (`stacks-inspect`, `0019`), consuming the artifact.

Decoupling them gives: no fake webhook, no fake measurement, no GitHub check, a
clear audit trail (*"the daemon built this pinned binary"*), and a fleet that can
route build vs measure to different hardware (`0004`).

## Model

- **An artifact-production job class** — produces + caches an artifact; terminal
  outcome is "artifact published" (**no `JobMetric`**). *Implementation decision
  (Open question 1):* either add a `JobKind::Build` value, or introduce a sibling
  `job_class = produce | measure` so produce/measure stays **orthogonal** to the
  `AdHoc` / `Baseline` measurement cadences (and `TriggerKind`, the source). The
  rest of this doc says "build job" conceptually, not as a committed enum shape.
- **Target axis** — *which binary* the build produces:

  | Target | Consumed by | Item |
  | ---- | ---- | ---- |
  | `stacks-bench` | benchmarking | `0025` (today) |
  | `stacks-inspect` | block validation | `0019` |
  | (future) | … | … |

  The build **target** is orthogonal to `0005`'s **`task_kind`** (the
  *measurement*): a task needs the binary its measurement runs, so target maps to
  "the binary `task_kind` consumes". Cache fingerprints gain a `target` field
  (today implicitly `stacks-bench`); keyed by `(target, commit, build-env)`.

- **Measurement jobs prefer the cached artifact**, building inline only if
  missing or the cache is off — exactly today's `0025` build-skip, generalized
  per target.

- **Pin warming enqueues build jobs** (target `stacks-bench`) for pinned refs
  whose binary is missing. The runner builds → publishes → `0025`'s pin recompute
  protects it. No measurement, no webhook, no check.

## Where it sits among the axes

```text
  task_kind   (0005)  — WHAT to measure:   benchmark · block-validation · …
  build target (this) — WHAT to produce:   stacks-bench · stacks-inspect · …
  backend     (0010)  — WHERE to run:      libvirt · (worker fleet, 0004)
```

A build job is `{target} × {backend}`; a measurement job is
`{task_kind} × {backend}` that *consumes* a `{target}` artifact.

## Scope (sketch)

- **Schema/model:** *superseded by `0005`* — the produce shape is
  `task_kind=build_only` and `build_target` is a PG enum (per `0005`'s redesigned
  axes; not a `JobKind::Build` value or a `TEXT` column). Build jobs have no
  PR/owner/webhook links → they ride `0005`'s daemon-initiated (webhook-less)
  creation path.
- **Recipe/driver:** a build-only path (provision → build VM → publish → teardown,
  skip the measurement VM). Reuses `0025`'s `publish_built_binary`.
- **Runner:** claim/assemble `Build` jobs; they take a normal concurrency slot.
  Reporting is **silent** (no GitHub surface) — a build job has no PR/commit
  measurement to report; its result is the cached artifact + a terminal status.
- **Warming planner:** pin resolver (`0025`/2b, shipped) → for each pinned target
  missing from the cache (`BinaryCache::has_entry_for`, banked during v9)
  and not already in-flight (a `(repo_id, commit)` active-build dedup query) →
  enqueue a `Build` job.

## Groundwork already in place (banked during v9 / `0025`)

- **`pin_resolver::PinnedTarget`** — resolved pin provenance (trigger/install/repo,
  ref kind+name, commit, bench args): exactly the warm-planner input.
- **`BinaryCache::has_entry_for(commit, env)`** — the repo-agnostic skip-if-cached
  check.
- The pin recompute already resolves targets on startup + after each job — the
  warm planner rides that same resolution (resolve once → protect → warm).

## Open questions

1. **Build cadence vs new dimension** — is `Build` a `JobKind` value, or a
   separate `job_class` (produce vs measure) so a future measurement cadence
   (Baseline/AdHoc) is orthogonal to produce/measure?
2. **Inline build vs enqueue-and-wait** — when a measurement job finds no cached
   binary, does it build inline (today) or enqueue a `Build` job and depend on it?
   Inline is simpler; enqueue-and-wait enables fleet build/measure split (`0004`).
3. **Target ↔ task_kind mapping** — 1:1 (benchmark→stacks-bench,
   blockval→stacks-inspect) for now; could a task_kind need multiple targets?
4. **Build job reporting** — fully silent, or a minimal operator-visible status
   (e.g. a `policy trigger list` "warm/cold" column fed by the cache) without any
   GitHub write?
5. **Fingerprint `target` field** — additive to `0025`'s `BuildFingerprint`;
   existing entries default to `stacks-bench`.
