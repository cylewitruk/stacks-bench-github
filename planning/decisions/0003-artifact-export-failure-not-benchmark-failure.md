# Decision 0003: Artifact-export failure is not benchmark failure

- **status:** accepted
- **date:** 2026-06
- **related items:** `0001-artifact-store`

## Decision

If `ArtifactStore::put` fails **after** a benchmark has completed (its local
artifacts and typed result already exist), the job is **not** marked failed. The
local archive + result are retained, the failure is logged loudly, and the upload
is **idempotent and retryable** — re-attempted in the terminal path and/or by a
small repair/sweep path. *"Benchmark failed"* and *"artifact export failed"* are
distinct outcomes that must never be conflated.

## Rationale

Once a benchmark completes, its value — the measurement and the local archive —
already exists. An object-storage hiccup must not discard a valid result or
surface as a (false) benchmark regression. Conflating export failure with
benchmark failure would turn transient infra blips into lost results and noisy
red checks.

## Consequences

- The terminal/ship path **tolerates** a `put` failure: log + retain local + flag
  for retry; the job's benchmark outcome is reported on its own merits.
- Upload is **idempotent** (keyed by `job_id` / artifact key, safe to repeat); a
  follow-up repair/sweep can re-ship runs whose export is pending.
- Until re-exported, a run's artifacts may be **local-only**; the authenticated
  download endpoint (Decision 0001) still serves them from the local copy.
- Local mode (`kind = "local"`) has no upload step and is unaffected.
