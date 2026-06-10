# v4: Artifact store

The first deliverable run through the planning system. Make run-artifact storage
pluggable (local default → S3-compatible object storage) so Slack (`0002`) and
the portal (`0003`) can reach artifacts off the orchestrator's disk.

*(Iteration `vN` continues the deployment-version lineage — last deployed was v3,
so deliverables start at v4. The canonical item identity is `0001-artifact-store`.)*

> **Status:** in_progress
>
> Promoted from backlog 2026-06; design converted from the former
> `docs/roadmap-v12.md`. **Phases 1 & 2 complete and Codex-signed-off. The
> deferred live S3 round-trip now runs in CI (`s3_round_trip.rs` against a
> pinned MinIO), closing the last gap — code-complete.** Remaining before
> `shipped`: deploy v4 (see [v3-to-v4-upgrade.md](../../docs/v3-to-v4-upgrade.md)).

## Items

| Item | Role | Status |
| ---- | ---- | ------ |
| `0001-artifact-store` | primary | in_progress |

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

**Progress:**

- **1a (done):** `crates/sbgh-daemon/src/artifact_store.rs` — `ArtifactStore` +
  `LocalFsStore` (`put`/`get`/`signed_url`→`Unsupported`/`exists`, with unsafe-key
  rejection) + `artifact_key` + 7 unit tests; green + lint-clean (behind a
  temporary `#![allow(dead_code)]` until wired). Codex-signed-off.
- **1b (done):** the libvirt driver archives through `put`; the `summary`
  pointer fields are now store **keys** (`<job_id>/<relative>`); the
  `reporter`/`job_source`/`progress` readers resolve them via `get`. The
  forensics `archive_*` helpers were removed (logic now in `LocalFsStore`),
  their `*_RELATIVE` consts made `pub`. `allow(dead_code)` retained only for the
  Phase-2 surface (`exists`/`signed_url`/`ArtifactUrlError`). All 659 daemon
  tests stay green — the behavior-preserving guard held.

**Status:**

- [x] Core implementation *(1a store; 1b producer/consumer wiring)*
- [x] Unit tests for the store (round-trip; key↔path; missing/absent; signed_url)
- [x] Reviewed (Codex) — signed off; one ADR-wording nit fixed, store-construction
  centralization deferred to Phase 2
- [x] Validated — acceptance checks below were run

**Acceptance & Validation:**

- [x] With `kind = "local"`, existing reporter/forensics behavior is unchanged —
  validated via the existing daemon suites staying green (659 passed).
- [x] `job_result.archive_dir` semantics + completion rendering are unchanged —
  validated via the existing reporter + `job_source` integration tests.
- [x] `LocalFsStore::get(key)` returns the same bytes the prior path read did —
  validated via the `put_then_get_round_trips_and_lands_at_root_slash_key` test.

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
- **Centralize store construction** — 1b left `LocalFsStore::new` embedded in the
  producer (`driver`) and two readers (`job_source`, `reporter`). Phase 2 builds
  the store **once** from `[artifacts]` config and threads it as an
  `Arc<dyn ArtifactStore>` into the runner/reporter/job-source dependencies, so
  the S3 switch is a single construction-site change rather than re-touching
  every call site (Codex, 1b review).

**Settled forks (2026-06):**

- **Backing crate — `rusty-s3` + the existing `reqwest`, local-first.** `S3Store`
  wraps a `LocalFsStore` (the local mirror = the `archive_dir` breadcrumb,
  retained on upload failure per Decision 0003) plus `rusty-s3` (zero-dep,
  sans-IO SigV4 signing) driven by the workspace `reqwest`. `put` writes local
  then best-effort uploads to S3; `get` serves local-first, falls back to an S3
  download into the mirror; `signed_url` = presigned GET. The done+tested
  `LocalFsStore` is reused untouched; driver/reader code is unchanged beyond
  holding `Arc<dyn ArtifactStore>`. (Rejected: `object_store` — heavier dep tree,
  its `get` returns a stream rather than our materialize-to-local-`PathBuf`
  contract, and it would replace `LocalFsStore`.)
- **Trait becomes async.** `put`/`get`/`exists` are network IO under S3, so they
  become `async` (via `async_trait`); `signed_url` stays **sync** (SigV4 signing
  and `LocalFs → Unsupported` are both pure, no IO). All call sites are already
  async. Revisits 1a's sync trait — acceptable, it's a new internal surface.
- **S3 credentials are env-only.** `endpoint`/`bucket`/`region` come from
  `[artifacts]` TOML; `access_key_id`/`secret_access_key` come **only** from
  `SBGH_ARTIFACTS_S3_*` env (`#[serde(skip)]` + `deny_unknown_fields` makes a TOML
  key a hard error). Mirrors the `api.ingest_token` policy.

**Store model (single configured `Arc<dyn ArtifactStore>`):**

- `kind = "local"` → `LocalFsStore` over `results_archive_dir` (today's behavior,
  `signed_url → Unsupported`).
- `kind = "s3"` → `S3Store { local: LocalFsStore, bucket, creds, http }`:
  - `put`: local mirror write (breadcrumb, always) → best-effort S3 upload,
    **streamed** from disk (`Content-Length` + `ReaderStream`, never buffered);
    returns the **local** size so a failed upload never fails the job (0003).
  - `get`: local-first → else **stream** the S3 download to a `.part` file →
    rename into the mirror (no torn artifact on an interrupted download).
  - `exists`: local mirror OR S3 `HEAD`.
  - `signed_url`: presigned GET (short TTL).
  - `job_dir`: the local mirror's job dir (unchanged `archive_dir` semantics).
  - HTTP client: `connect_timeout` (10s, fail-fast on unreachable) +
    `read_timeout` (120s idle — caps a *stalled* transfer without capping a
    large progressing one), so a hung S3 op can't wedge job completion.

**Codex review round 1 (addressed):** stream uploads/downloads instead of
buffering whole objects in daemon RSS (the run SQLite can be multi-GB); add
connect/read timeouts to the production S3 client; document `[artifacts]` (local
default + commented S3 example + env vars) in `config.example.daemon.toml`.

**Status:**

- [x] Core implementation — `S3Store` (rusty-s3 + reqwest, local-first) +
  `build_store`/`build_store_or_local` factory + `[artifacts]` config + the async
  trait + the centralized wiring (driver/reporter derive from config,
  `JobSource` holds the shared `Arc`, `main` builds once / fails fast).
- [x] Unit tests (config parse per-kind; `S3Store` signing + fault-injection +
  traversal guard; `LocalFsStore` round-trip)
- [x] **Live-endpoint round-trip** — `crates/sbgh-daemon/tests/s3_round_trip.rs`
  runs against a compose-managed **MinIO** (pinned `RELEASE.2025-09-07T16-13-09Z`,
  started by the `minio` nextest setup script). Proves end to end: upload reaches
  the bucket, `get`/`exists` resolve from S3 against an *empty* mirror, and a
  presigned GET fetches with **no client credentials** (MinIO accepts our SigV4
  signature). Closes the prior deferral.
- [x] Reviewed (Codex) — **signed off, code-complete.** Three rounds: streaming
  I/O (no whole-object buffering), connect/idle-read timeouts, `[artifacts]`
  example config, and a per-download unique `.part` temp path. Codex's standing
  caveat (a live round-trip before trusting `kind = "s3"`) is now **satisfied in
  CI** by `s3_round_trip.rs` against real MinIO; a Hetzner-specific smoke test on
  first enable (real endpoint/creds/network) remains recommended, not a gate —
  see [docs/v3-to-v4-upgrade.md](../../docs/v3-to-v4-upgrade.md) Part B.
- [x] Validated — all checks run: full workspace suite green (684 tests, incl.
  the live S3 round-trip), lint clean.

**Acceptance & Validation:**

- [x] With `kind = "s3"`, an uploaded run artifact is present in the bucket —
  validated by `s3_round_trip.rs` against live MinIO (upload → fetched back from
  S3 via a fresh empty-mirror store).
- [x] `S3Store::signed_url` mints a **working presigned GET** (SigV4, targets
  bucket+key); `LocalFsStore` returns `Unsupported` — unit tests
  (`s3_signed_url_is_a_presigned_get`, `signed_url_is_unsupported_for_local`) +
  the live `s3_round_trip.rs` fetch confirms a real verifier honors the URL.
- [x] **An `S3Store::put` failure after a completed benchmark does NOT fail the
  job** — local artifacts retained + still fetchable (Decision 0003) — validated
  via `s3_put_succeeds_locally_when_upload_fails` (unreachable endpoint → `put`
  still returns the local size, `get` serves the retained copy).
- [x] `[artifacts]` config validates required fields per kind — config parse tests
  (`daemon_artifacts_*`: defaults-to-local, s3-from-toml+env, env-only kind,
  missing-field error, TOML-credential rejection).

**Tests:**

- `S3Store` unit tests (signing, fault-injection, job_dir, traversal guard) —
  no live endpoint needed; `signed_url` presign + local-`Unsupported`.
- `[artifacts]` config parse/validation tests in `crates/sbgh-core`.
- **`crates/sbgh-daemon/tests/s3_round_trip.rs`** — live round-trip against the
  compose-managed MinIO (`minio` nextest setup script): upload → S3 fetch from a
  fresh mirror → presigned GET → absent-key handling.

## Final Validation

- Default (`kind = "local"`): full daemon suite green, no behavior change.
- Opt-in (`kind = "s3"`): a run's artifacts land in object storage and are
  fetchable via a presigned URL.

## Follow-Ups

- Unblocks `0002-slack-adhoc-profiling` (Phase 4) and `0003-results-portal`.
- Deferred (not this iteration): retention/lifecycle policy; worker-side upload
  (lands with `0004`).
