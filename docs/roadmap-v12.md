# Roadmap 12 — Artifact store: object-storage-backed run artifacts

> **Status:** Planned · **foundational slice** (small, self-contained) · unblocks
> [roadmap-v10.md](./roadmap-v10.md) (Slack artifact links) +
> [roadmap-v11.md](./roadmap-v11.md) (portal SQLite fetch) · default is
> **behavior-preserving** (local FS).

A small, self-contained slice: make run-artifact storage **pluggable** behind an
`ArtifactStore`, with a local-FS impl (today's behavior) and an S3-compatible
impl (Hetzner object storage). Build it **once**, because both the Slack surface
and the results portal need artifacts off the orchestrator's local disk.

## Why now

- Artifacts (the run SQLite, `run.json`, flamegraph, binary, phase-log) live on
  the **orchestrator's local disk** today (`results_archive_dir`). Neither Slack
  (link a flamegraph) nor a portal (fetch a run's SQLite on demand) can reach
  them there.
- v10 and v11 would otherwise each invent their own copy of this. One layer,
  one keying scheme, shared by both.
- **Free intra-Hetzner egress** makes object storage cheap: orchestrator → store
  → Slack/portal all stay inside the Hetzner network.

## Scope

- **`ArtifactStore` trait** — `put(job_id, bundle)`, `get(key)`,
  `signed_url(key, ttl)`, `exists(key)`. Keys mirror the current layout
  (`<job_id>/<artifact>`), so the addressing is stable across impls.
  - **`LocalFsStore`** — wraps today's `results_archive_dir`; **behavior-
    preserving** and the default.
  - **`S3Store`** — Hetzner object storage (any S3-compatible endpoint).
  - *Impl candidate:* the `object_store` crate already unifies local + S3 behind
    one API — evaluate it as the trait's backing rather than hand-rolling two
    clients.
  - **`signed_url` is an S3-only capability (Codex).** A local filesystem can't
    mint an externally-usable URL, so `LocalFsStore::signed_url` returns
    **`Unsupported`** — callers (Slack/portal) must fall back to an
    **orchestrator/portal-authenticated download endpoint** that streams via
    `get`, and must **not** assume a shareable link exists under `kind = "local"`.
    Signed links are an S3-mode affordance only.
- **Ship on completion** — after the driver's archive step (the v8 artifact
  path), the orchestrator `put`s the bundle to the store. The `summary` blob's
  artifact pointers become **store keys** resolved via the store (not bare local
  paths) — but see Step 1: for `LocalFsStore` a key resolves to **today's exact
  path**, so existing readers are unaffected.
- **Signed GET URLs (S3 mode)** — short-TTL presigned links so Slack and the
  portal fetch **directly** from object storage, without proxying bytes through
  the orchestrator. (Local mode uses the authenticated download endpoint above.)
- **Config** — a new `[artifacts]` section: `kind = "local" | "s3"`, plus the S3
  endpoint/bucket/region/credentials when `s3`. Validated per-kind (the
  established Raw/merge/into_config pattern).

## Out of scope (noted, not built here)

- **Retention / lifecycle** policy (object-storage expiry) — the keying scheme
  should *support* it; the policy is later.
- **Worker-side upload** — under [roadmap-v9.md](./roadmap-v9.md) the *worker*
  holds the artifacts at completion and uploads to the store (or hands the
  orchestrator a pointer). The trait + keying are the stable contract; *who*
  calls `put` (orchestrator today, worker post-v9) is agnostic to it.

## Steps

1. **`ArtifactStore` + `LocalFsStore` (behavior-preserving).** Introduce the
   trait and route the current archive path through it. **Behavior-preservation is
   load-bearing here (Codex):** today the reporter/forensics path reads archived
   `run.json` from local paths, so making `summary` pointers into keys is only
   safe if **`LocalFsStore` keys resolve to the same `results_archive_dir/<job_id>/…`
   paths** *and* the readers go through `ArtifactStore::get` (or a path-compatible
   key). `job_result.archive_dir` semantics and completion rendering must be
   unchanged with `kind = "local"`; existing reporter/forensics tests stay green
   as the proof.
2. **`S3Store` + config + ship + signed URLs.** Add the S3 impl, the
   `[artifacts]` config, ship-on-completion, and presigned-URL issuance. Opt-in
   via `kind = "s3"`.

## Decisions (proposed)

1. **Local is the behavior-preserving default;** S3/object storage is opt-in.
2. **One store, both surfaces** — Slack (v10) and the portal (v11) consume the
   same `ArtifactStore`; do not duplicate.
3. **Direct fetch via signed URLs is an S3-mode affordance** — in S3 mode
   consumers pull straight from object storage (keeps the orchestrator lean;
   matches v9 boundary discipline); in local mode `signed_url` is `Unsupported`
   and consumers use an authenticated download endpoint. Callers must handle
   both, never assume a shareable link exists.
4. **Keying mirrors the `<job_id>/…` layout** and is impl-agnostic, so switching
   backends doesn't move data semantically.

## Relationship to the roadmaps

- **Dependency of** [roadmap-v10.md](./roadmap-v10.md) Phase 4 (flamegraph
  delivery) and [roadmap-v11.md](./roadmap-v11.md) Phases 1/3/4 (portal artifact
  fetch). Sequence this **first**.
- **Rides** the [roadmap-v8.md](./roadmap-v8.md) artifact seam; **agnostic** to
  task kind (v6) and execution backend / fleet (v9).
