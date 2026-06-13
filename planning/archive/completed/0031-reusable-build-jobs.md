# 0031: Reusable build jobs (pin warming)

- **id:** `0031-reusable-build-jobs`
- **status:** `shipped`
- **date:** 2026-06
- **iteration:** v11 (`v11-reusable-build-jobs`)
- **depends_on:** `0005-task-kind-platform` (shipped axes),
  `0025-baseline-binary-cache` (host-local binary cache + pin-protect)
- **unblocks:** `0019-block-validation-recipe`, `0004-worker-fleet` build/measure
  routing

Promoted artifact production to a first-class, daemon-initiated job path for
pin warming. A pinned release ref whose binary is missing from the cache now
gets a silent `build_only` job: no fake webhook, no fake measurement, no GitHub
or Slack surface.

## What shipped

- **Webhook-less job creation** — renamed the neutral no-link insert to
  `create_unlinked_job`, shared by Slack ad-hoc jobs and daemon-created warm
  jobs.
- **Cache-warm provenance** — `QueuedEventDetail::CacheWarm` records the
  trigger/policy id, repo/ref, commit, and `build_target` that caused the daemon
  enqueue. Bench-arg normalization returns empty args because build-only jobs
  have no benchmark input.
- **Build-only warm jobs** — enqueued with axes
  `source=daemon`, `intent=cache_warm`, `task_kind=build_only`,
  `build_target=stacks_bench`.
- **Warming planner** — `PinManager::recompute` resolves pinned targets once,
  protects matching cache entries, then warms missing ones.
- **Skip rules** — warm planner skips entries already cached via
  `BinaryCache::has_entry_for(commit, env)`, skips active builds, and skips
  recently failed/cancelled warm attempts for `WARM_RETRY_COOLDOWN_HOURS` (6h).
  The cooldown is DB-backed, so restart does not create an immediate retry loop.
- **Cadence** — warming rides the pin-recompute hook on startup and after each
  job execution. Pinning a ref is the opt-in; there is no separate warm-enable
  flag.
- **Silent reporting** — build-only jobs route to `ProgressTarget::Silent` /
  `NoopReportSurface`; the audit surface is logs + job history.

## Validation

- Unit/integration suite was green at implementation review: 821 tests, lint and
  build clean.
- On-host validation (2026-06): after v11 deployment, the daemon resolved the
  pinned release-family refs, enqueued warm jobs for cold pins, built them, and
  published the binaries into the real cache. The log tail showed the intended
  sequence: `build_done` → `binary cache: published built binary` → teardown →
  terminal job completion.
- A follow-up log wording fix changed the generic terminal message from
  "benchmark completed" to task-neutral "job completed" with `task_kind` /
  `build_target`, because build-only warm jobs are not benchmarks.

## Decisions

1. **Build-only, not fake benchmark** — warming uses v10's
   `task_kind=build_only` / `intent=cache_warm` axes instead of bending a
   baseline benchmark into artifact production.
2. **Keep inline build-on-miss** — measurement jobs still build inline if they
   miss the cache; enqueue-and-wait is deferred to the worker-fleet work
   (`0004`).
3. **Dedup by repo/commit/target** — active/recent warm attempts are keyed by
   `(repo_id, commit, build_target)`. The cache itself remains repo-agnostic by
   `(commit, env)`, so duplicate work across repos is harmless and can be
   optimized later.
4. **Always-on with pins** — if binary cache is enabled and pins exist, warming
   runs on recompute. A future operator surface can add explicit manual warm
   controls if needed.
5. **`stacks-bench` only for now** — fingerprint target fields and
   `stacks-inspect` builds wait for `0019`.

## Follow-Ups

- `0019-block-validation-recipe` — adds `stacks-inspect` as a second build
  target and block validation as the second task kind.
- `0004-worker-fleet` — separate build and measurement placement, and eventual
  enqueue-and-wait behavior instead of duplicate inline builds.
- Operator visibility — optional warm/cold listing or manual `policy warm`
  command if logs/job history are not enough.
