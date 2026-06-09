# Design 0001: Artifact store (object-storage-backed run artifacts)

- **id:** `0001-artifact-store`
- **status:** `planned` — iteration
  [v1-artifact-store](../iterations/v1-artifact-store.md)
- **decisions:** [0001 — URLs are S3-only](../decisions/0001-artifact-urls-s3-only.md) ·
  [0002 — refs are store keys](../decisions/0002-artifact-refs-are-store-keys.md) ·
  [0003 — export-fail ≠ bench-fail](../decisions/0003-artifact-export-failure-not-benchmark-failure.md)

Make run-artifact storage **pluggable** behind an `ArtifactStore`, with a local-FS
impl (today's behavior) and an S3-compatible impl (Hetzner object storage). Build
it **once** — both Slack (`0002`) and the portal (`0003`) need artifacts off the
orchestrator's local disk. *(Converted from the former `docs/roadmap-v12.md`.)*

## Why

- Artifacts (the run SQLite, `run.json`, flamegraph, binary, phase-log) live on
  the **orchestrator's local disk** today (`results_archive_dir`). Neither Slack
  (link a flamegraph) nor a portal (fetch a run's SQLite on demand) can reach
  them there.
- `0002` and `0003` would otherwise each invent their own copy of this. One
  layer, one keying scheme, shared by both.
- **Free intra-Hetzner egress** makes object storage cheap: orchestrator → store
  → Slack/portal all stay inside the Hetzner network.

## Scope

- **`ArtifactStore` trait** — `put(job_id, bundle)`, `get(key)`,
  `signed_url(key, ttl)`, `exists(key)`. Keys mirror the current layout
  (`<job_id>/<artifact>`) and are impl-agnostic (Decision 0002).
  - **`LocalFsStore`** — wraps today's `results_archive_dir`; behavior-preserving
    and the default. `signed_url` → `Unsupported` (Decision 0001).
  - **`S3Store`** — Hetzner object storage (any S3-compatible endpoint); supports
    presigned GET.
  - *Impl candidate:* the `object_store` crate already unifies local + S3 behind
    one API — evaluate it as the trait's backing rather than hand-rolling two
    clients.
- **Ship on completion** — after the driver's archive step (the v8 artifact
  path), the orchestrator `put`s the bundle to the store. The `summary` blob's
  per-artifact pointer fields become **store keys** (Decision 0002), which for
  `LocalFsStore` resolve to today's exact path, so existing readers are
  unaffected; `job_result.archive_dir` stays a local diagnostic path (Decision
  0002), never a fetch reference.
- **Export failure ≠ benchmark failure** (Decision 0003) — a `put` failure after
  a completed run does **not** fail the job: retain the local archive/result, log
  loudly, and make the upload idempotent + retryable. Don't conflate the two
  outcomes.
- **Fetch** — S3 mode issues short-TTL presigned GET URLs (consumers fetch
  directly); local mode uses an authenticated download endpoint (Decision 0001).
- **Config** — a new `[artifacts]` section: `kind = "local" | "s3"`, plus the S3
  endpoint/bucket/region/credentials when `s3`. Validated per-kind (the
  established Raw/merge/into_config pattern).

## Out of scope

- **Retention / lifecycle** policy (object-storage expiry) — the keying scheme
  should *support* it; the policy is later.
- **Worker-side upload** — under `0004` (worker fleet) the *worker* holds the
  artifacts at completion and uploads to the store (or hands the orchestrator a
  pointer). The trait + keying are the stable contract; *who* calls `put`
  (orchestrator today, worker post-fleet) is agnostic to it.

## Steps

(These map to the iteration's two phases.)

1. **`ArtifactStore` + `LocalFsStore` (behavior-preserving).** Introduce the trait
   and route the current archive path through it. Per Decision 0002, the
   reporter/forensics read path goes through `ArtifactStore::get` and
   `LocalFsStore` keys resolve to the same `results_archive_dir/<job_id>/…`
   paths; `job_result.archive_dir` semantics and completion rendering are
   unchanged with `kind = "local"` (existing tests are the proof).
2. **`S3Store` + config + ship + signed URLs.** Add the S3 impl, the `[artifacts]`
   config, ship-on-completion, and presigned-URL issuance. Opt-in via
   `kind = "s3"`.

## Relationship

- **Unblocks** `0002-slack-adhoc-profiling` (Phase 4 flamegraph delivery) and
  `0003-results-portal` (artifact fetch). Foundational — build first.
- **Rides** the roadmap-v8 artifact seam; **agnostic** to task kind (`0005`) and
  the execution backend / fleet (`0004`/`0006`).
