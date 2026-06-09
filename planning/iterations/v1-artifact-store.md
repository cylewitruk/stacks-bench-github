# v1: Artifact store

The first deliverable run through the planning system. Make run-artifact storage
pluggable (local default → S3-compatible object storage) so Slack (`0002`) and
the portal (`0003`) can reach artifacts off the orchestrator's disk.

> **Status:** planned
>
> Promoted from backlog 2026-06; design converted from the former
> `docs/roadmap-v12.md`. Not started — implementation deliberately deferred.

## Items

| Item | Role | Status |
| ---- | ---- | ------ |
| `0001-artifact-store` | primary | planned |

Full design: [design/0001-artifact-store.md](../design/0001-artifact-store.md).
Decisions: [0001 — URLs S3-only](../decisions/0001-artifact-urls-s3-only.md),
[0002 — refs are store keys](../decisions/0002-artifact-refs-are-store-keys.md).

## Why

Artifacts live on the orchestrator's local disk (`results_archive_dir`); neither
Slack nor a portal can reach them. Build the pluggable store **once** so both
surfaces share one layer and one keying scheme.

## Scope

An `ArtifactStore` (`put`/`get`/`signed_url`/`exists`) with a behavior-preserving
`LocalFsStore` (default) and an `S3Store`, ship-on-completion, and per-kind
`[artifacts]` config. Details in the design doc; durable contracts in the two
decisions above.

## Phases

### Phase 1: `ArtifactStore` + `LocalFsStore` (behavior-preserving)

**Goal:** Introduce the trait and route the current archive path through it, with
**zero behavior change** under the default `kind = "local"`.

**Scope:**

- `ArtifactStore` trait + `LocalFsStore` over `results_archive_dir`.
- `summary` artifact pointers become store **keys** (Decision 0002); the
  reporter/forensics read path goes through `ArtifactStore::get`; local keys
  resolve to today's `results_archive_dir/<job_id>/…` paths.

**Status:**

- [ ] Core implementation
- [ ] Unit tests (LocalFsStore round-trip; key↔path resolution)
- [ ] Reviewed (Codex)
- [ ] Validated — acceptance checks below were run

**Acceptance & Validation:**

- [ ] With `kind = "local"`, existing reporter/forensics behavior is unchanged —
  validate via the existing daemon suites staying green (`just test`).
- [ ] `job_result.archive_dir` semantics + completion rendering are unchanged —
  validate via the existing reporter tests.
- [ ] `LocalFsStore::get(key)` returns the same bytes the prior path read did —
  validate via a new unit test.

**Tests:**

- New `artifact_store` unit tests (LocalFsStore round-trip + key resolution).
- Existing `crates/sbgh-daemon` reporter/forensics suites (regression guard).

### Phase 2: `S3Store` + config + ship + signed URLs

**Goal:** Opt-in object storage: ship the bundle to S3 on completion and issue
presigned fetch URLs.

**Scope:**

- `S3Store` (Hetzner/S3-compatible) + `[artifacts]` config (per-kind validation).
- Ship-on-completion `put`; S3 `signed_url` (presigned GET); local `signed_url` →
  `Unsupported` (Decision 0001).

**Status:**

- [ ] Core implementation
- [ ] Integration tests (S3 via a local/mock endpoint; config parse per-kind)
- [ ] Reviewed (Codex)
- [ ] Validated — acceptance checks below were run

**Acceptance & Validation:**

- [ ] With `kind = "s3"`, a completed run's bundle is present in the bucket —
  validate via an integration test against a local S3-compatible endpoint.
- [ ] `S3Store::signed_url` yields a working presigned GET; `LocalFsStore` returns
  `Unsupported` — validate via store unit tests.
- [ ] **An `S3Store::put` failure after a completed benchmark does NOT fail the
  job** — local artifacts retained, logged, upload idempotent/retryable (Decision
  0003) — validate via a fault-injection test (put errors → job still reports its
  benchmark outcome; artifacts remain locally fetchable).
- [ ] `[artifacts]` config validates required fields per kind — validate via
  config parse tests.

**Tests:**

- `S3Store` integration test (mock/local S3); `signed_url` unit tests.
- `[artifacts]` config parse/validation tests in `crates/sbgh-core`.

## Final Validation

- Default (`kind = "local"`): full daemon suite green, no behavior change.
- Opt-in (`kind = "s3"`): a run's artifacts land in object storage and are
  fetchable via a presigned URL.

## Follow-Ups

- Unblocks `0002-slack-adhoc-profiling` (Phase 4) and `0003-results-portal`.
- Deferred (not this iteration): retention/lifecycle policy; worker-side upload
  (lands with `0004`).
