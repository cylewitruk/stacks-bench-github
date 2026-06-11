# 0001: Artifact store

- **id:** `0001-artifact-store`
- **status:** `shipped`
- **date:** 2026-06
- **iteration:** v4 (deployed; see [v3-to-v4-upgrade.md](../../../docs/v3-to-v4-upgrade.md))
- **source:** `docs/roadmap-v12.md`

Made run-artifact storage **pluggable** (local disk default → S3-compatible
object storage) so the Slack (`0002`) and portal (`0003`) surfaces can reach a
run's artifacts off the orchestrator's local disk. Behavior-preserving in the
default `local` mode.

## What shipped

- `crate::artifact_store`: `trait ArtifactStore { put, get, exists, signed_url,
  signed_url_if_fetchable, job_dir }`, a behavior-preserving `LocalFsStore`
  (default) and an `S3Store` (S3-compatible; rusty-s3 signing + reqwest,
  streamed multi-GB upload/download via a local mirror).
- A `[artifacts]` config section (`kind = "local" | "s3"`); built once at
  startup (`build_store`) so a bad endpoint fails fast. S3 credentials are
  **env-only** (`SBGH_ARTIFACTS_S3_*`).
- The libvirt driver archives each artifact (`run.json`, `stacks-bench.db`, the
  `stacks-bench` binary, the phase log) through `put`; the reporter / job-source
  resolve them back through `get` by **store key** (`<job_id>/<relative>`).
  *(Since updated: the `stacks-bench` binary archives via `put_local_only` — a
  host-only forensic copy, never S3-uploaded — as it's large (~250-300 MB) and
  non-portable across host arches.)*
- Live S3 round-trip proven in CI against a pinned MinIO (`s3_round_trip.rs`):
  upload → bucket → presigned GET with no credentials on the client.

## Decisions

- [0001 — Artifact URLs are S3-only](../../decisions/0001-artifact-urls-s3-only.md)
- [0002 — Artifact refs are store keys](../../decisions/0002-artifact-refs-are-store-keys.md)
- [0003 — Export failure ≠ benchmark failure](../../decisions/0003-artifact-export-failure-not-benchmark-failure.md)
