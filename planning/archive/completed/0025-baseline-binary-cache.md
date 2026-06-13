# 0025: Release-baseline binary cache

- **id:** `0025-baseline-binary-cache`
- **status:** `shipped`
- **date:** 2026-06
- **iteration:** v9 (`v9-baseline-binary-cache`)
- **successor / consumer:** `0031-reusable-build-jobs` (pin warming)

Added a host-local binary cache for repeated `stacks-bench` release refs. A
cache hit seeds the built binary onto the source disk and skips the build VM; a
miss builds normally and publishes the binary into the cache. Release refs can be
pinned through trigger policies so already-built pinned binaries survive normal
LRU eviction.

## What shipped

- **Binary cache core** (`binary_cache.rs`) — fingerprinted entries with
  `meta.json`, sha verification, atomic publish, size-bounded LRU eviction, and
  pinned-entry protection.
- **Fingerprint contract** — commit + declared toolchain channel + release build
  inputs + target triple + build recipe version + golden-image identity +
  protocol/artifact-affecting version. The toolchain key is pragmatic: a floating
  `stable` channel keys as `stable`, not as an exact `rustc -vV`.
- **Build skip path** — on a hit, the daemon mounts the source disk, copies the
  cached binary to `target/release/stacks-bench`, forces executable mode, chowns
  for the VM, mirrors the binary to results for archival parity, emits
  `build_cached:<digest>`, and skips the build VM.
- **Publish-on-finish** — normal benchmark builds publish the host-local binary
  into the cache after the build phase.
- **Config** — `[artifacts.binary_cache]` with `enabled`, `dir`, and `max_size`;
  disabled by default, preserving the old path.
- **Branch prefix policy matcher** — `TriggerMatchSpec::BranchPrefix`, surfaced
  in the CLI as `policy trigger add --branch-push 'sb-integration/*'`.
- **Pin policy** — additive `trigger_policy.pinned` / `pinned_until` schema,
  admin/API/CLI pin/unpin, and `policy trigger list` pin state.
- **Pin resolver / manager** — resolves pinned refs with read-only
  `git ls-remote`, peels annotated tags, scopes refs by repo, filters expiry,
  recomputes on startup and after each job, and protects matching cache entries
  from eviction. Resolution is all-or-nothing: transient ref lookup failure
  preserves the last pinned set.
- **Bounded subprocesses** — `ls-remote` uses a timeout that kills and reaps the
  child, so a stuck git process cannot leak.

## Validation

- Phase 1 was host-validated: a repeat real `@BenchBot` run reused a cached
  binary and skipped the build VM, reducing the Build stage from minutes to a
  short source-disk seed.
- Phase 2 was deployed with pins configured for the release branch family; the
  daemon resolved the pinned refs and v11 warming successfully built cold pinned
  refs into the cache.
- Full forced eviction-pressure validation was intentionally not run. Expected
  behavior is now shipped: if an already-built pinned entry is evicted contrary
  to policy, treat it as a bug in the pin-protect implementation.

## Decisions

1. **Local-only first** — fleet/S3 sharing rides `0004-worker-fleet`.
2. **Pin on trigger policy** — pinned release families are existing trigger
   policies with `pinned=true`, not a separate config list.
3. **Source-disk seeding over bench-template changes** — keep the bench VM's exec
   path identical to a real build.
4. **Pins protect existing binaries only** — cold-pin prebuild moved to
   `0031-reusable-build-jobs`, implemented as v11's warming planner.
5. **Pinned overflow logs and keeps pins** — the size budget caps the LRU tail;
   pins are a floor, not something eviction may violate.

## Follow-Ups

- `0031-reusable-build-jobs` — daemon-initiated build-only jobs warm cold pinned
  refs.
- `0004-worker-fleet` — shared cache / worker-local warm sets.
- `0027-fine-grained-progress` — eventual build recipe/protocol versioning should
  drive the fingerprint's build-recipe/protocol inputs.
