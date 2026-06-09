# Decision 0002: Run artifact references are store keys, not raw local paths

- **status:** accepted
- **date:** 2026-06
- **related items:** `0001-artifact-store`

## Decision

A run's artifact pointers in the `summary` blob are **`ArtifactStore` keys**
(resolved via the store), not bare filesystem paths. Consumers read artifacts
through `ArtifactStore::get`, never by opening a path directly. For `LocalFsStore`
a key resolves to **today's exact `results_archive_dir/<job_id>/…` path**, so the
change is behavior-preserving in local mode.

**Concretely (which fields change):** the per-artifact pointer fields in `summary`
(`run_json_archived_path`, `sqlite_archived_path`, `binary_archived_path`,
`phase_log_archived_path`, …) carry **store keys** (`<job_id>/<relative>`) — in
local mode a key **resolves to** today's path via `ArtifactStore::get`, so
consumer behavior is unchanged. **`job_result.archive_dir` stays a
local diagnostic path** (the local archive root): unchanged in local mode, used
as an operator/forensics breadcrumb, **never a fetch reference**. A unified
`artifacts: { name → key }` map is a possible later cleanup, out of scope for
`0001`.

## Rationale

Keys decouple consumers (the reporter, Slack, the portal) from *where* an artifact
physically lives — which is what lets `kind = "local"` switch to `kind = "s3"`
without touching any consumer. Anchoring the local key to the existing path keeps
`job_result.archive_dir` semantics and completion rendering unchanged, so the
first slice ships behind the default with **no behavior change** (existing
reporter/forensics tests stay green as the proof).

## Consequences

- The reporter/forensics read path routes through `ArtifactStore::get`.
- Keys mirror the `<job_id>/<artifact>` layout and are impl-agnostic; switching
  backends doesn't move data semantically.
- `0002`/`0003` consume keys, not paths — they never assume a local filesystem.
