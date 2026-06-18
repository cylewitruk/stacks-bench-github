# v16: Cache-Hit Minimal Source Disk

Item `0042-cache-hit-minimal-source-disk`: when a fingerprint-matched binary is
already cached, provision a **minimal** source disk (the binary only) instead of
a full `git checkout` of stacks-core, so a cache-hit run goes claim → bench in a
few seconds instead of ~15-20s of source-disk churn the bench never reads.

> **Status:** shipped
>
> Shipped as v16 and validated on-host. Cache-hit runs now split mirror prep
> from source-disk population, resolve the binary cache before populating the
> disk, and provision a minimal binary-only source disk instead of a full
> checkout/chown path.

## Items

| Item | Role | Status |
| ---- | ---- | ------ |
| `0042-cache-hit-minimal-source-disk` | primary | shipped |

## Why

On a binary-cache hit the build VM is already correctly skipped (item `0025`),
but the daemon still provisions the **full** source disk first — `git clone` +
`git checkout --detach <sha>` of stacks-core, then a recursive `chown` of that
tree — before seeding the cached binary into `target/release/stacks-bench`. The
bench only execs the binary; it never reads the source tree (chainstate is a
separate mount). On a Hetzner cache-hit run this was ~18s of claim → bench
(`mkfs` + checkout ≈ 5s, full-tree `chown` ≈ 10s) spent on work the run
discards, and isolated repeats (`0038`) pay it on every clean run.

A host log confirmed the build VM is *not* the problem — it is skipped:

```text
INFO …driver: binary cache: reusing cached stacks-bench binary; skipping the build VM
INFO …driver: starting phase … phase_lifecycle="bench"
```

The cost is the source-disk provisioning that runs regardless of the hit.

## Scope

Make source-disk provisioning **cache-aware**:

- Resolve the binary cache (fingerprint + lookup) **before** populating the
  source disk, producing a typed build plan: cache hit (with the cached binary
  path + digest) or miss. The bare mirror is still ensured/fetched first because
  fingerprinting reads `rust-toolchain(.toml)` from that mirror.
- On a hit: provision a minimal source disk — `mkfs` + the binary at
  `target/release/stacks-bench`, no `git checkout`, no full-tree `chown`. Skip
  the build VM (as today); packing a build-phase cloud-init ISO is optional
  cleanup, not the load-bearing behavior.
- On a miss: keep today's full checkout (the build VM needs the tree), build,
  publish — unchanged.
- Fix the Slack Build row so a cache-hit's host-side seed reads "Staging cached
  binary" / "Reused cached build", never "Building benchmark binaries".

**Non-goals:** carrying/cloning the seeded source disk *across repeats* (run 0
builds it, runs 1..N-1 clone it). Cache-hit repeats already reuse the
fingerprint-matched cached binary; disk-level cloning can follow once the
minimal disk path is proven.

## Design Decisions

- **Resolve the cache before source-disk population, not before mirror prep.**
  Today `provision_artifacts` fetches the commit into the bare mirror and then
  performs the full checkout before `try_reuse_cached_binary` runs. The cache
  decision must move after mirror ensure/fetch (fingerprinting needs the mirror)
  but before the source disk is formatted/populated.
- **Reuse the binary seeding layout, drop the full populate.** On a hit, the
  binary at `target/release/stacks-bench` becomes the *sole* payload. The ~10s
  recursive `chown` disappears because there is no source tree to chown.
- **Audit `$SRC` reads before trusting "binary only".** The bench template
  (`sbgh-bench.sh.tmpl`) must reference nothing under `$SRC` except the binary;
  the minimal-disk path is correct only if that holds. The operator has confirmed
  this for the current bench; Phase 2 verifies it against the template so the
  invariant is checked, not assumed.
- **Fail safe on a stale hit.** If the plan says hit but the cached binary is gone
  at seed time (eviction race), tear down/recreate the source disk through the
  normal full-checkout path and build rather than failing the run.

## Phases

### Phase 1: Resolve cache before source provisioning

**Goal:** Expose the hit/miss decision before the source disk is populated, with
no behavior change yet.

**Scope:**

- Split mirror prep from artifact provisioning: ensure/fetch the commit into the
  bare mirror first, then run binary-cache resolution (fingerprint +
  `cache.get`) before any source-disk write.
- Produce a typed build plan (`CacheHit { binary, digest }` | `Miss`) that later
  phases can consume.
- Provisioning still does the full checkout regardless (a no-op refactor), so the
  reorder is verifiable in isolation.

**Status:**

- [x] Core implementation
- [x] Unit/integration tests
- [x] Reviewed
- [x] Validated

**Acceptance & Validation:**

- [x] The cache decision is computed before any source-disk write.
- [x] The mirror ensure/fetch still happens before fingerprinting.
- [x] Hit and miss paths produce byte-identical provisioning to today (no
  behavior change in this phase).
- [x] Cache-disabled config still always plans a miss.

### Phase 2: Minimal source disk on a hit

**Goal:** On a cache hit, skip the full checkout; seed only the binary.

**Scope:**

- When the plan is a hit, provision the source disk with `mkfs` + the cached
  binary at `target/release/stacks-bench` only — no `git clone`/`checkout`, no
  recursive `chown` of a source tree, and no build VM boot. Avoiding build-ISO
  rendering is allowed but not required.
- On a miss, keep the full checkout + build + publish unchanged.
- Verify against `sbgh-bench.sh.tmpl` that the bench reads nothing from `$SRC`
  beyond the binary; chainstate stays a separate mount.
- If the cached binary is missing at seed time, discard the minimal source disk
  attempt and recreate the source disk through the full checkout + build path.

**Status:**

- [x] Core implementation
- [x] Unit/integration tests
- [x] Reviewed
- [x] Validated

**Acceptance & Validation:**

- [x] A cache-hit run performs no `git checkout` and no full-tree `chown`.
- [x] The bench still execs the cached binary and produces identical results.
- [x] A cache miss is unchanged (full checkout → build → publish → bench).
- [x] A stale hit (binary evicted between plan and seed) falls back, not fails.

### Phase 3: Cache-hit Build-row label

**Goal:** Stop a cache hit from looking like a build VM on the Slack card.

**Scope:**

- Emit a pre-seed cache-hit phase/event if needed so, during the host-side seed,
  the Build row reads "Staging cached binary". Once seeded, it reads "Reused
  cached build · <digest>". It never says "Building benchmark binaries" on a
  hit. The GitHub surface stays "build (cached)".

**Status:**

- [x] Core implementation
- [x] Unit/integration tests
- [x] Reviewed
- [x] Validated

**Acceptance & Validation:**

- [x] The Build row never renders "Building…" on a cache hit.
- [x] A cache miss still renders the normal build progression.

## Final Validation

- [x] `just build`
- [x] `just lint`
- [x] `just test`
- [x] Host smoke: a cache-hit bench logs no `git checkout`, goes claim → bench in
  a few seconds, the build VM stays skipped, and results match a full-checkout
  run; the Build row never shows "Building…". A 2-3 repeat group confirms each
  cache-hit repeat avoids the checkout/chown path.

## Follow-Ups

- `0031` warm jobs benefit identically once a warmed binary is cached; no extra
  work, but worth confirming on the host.
