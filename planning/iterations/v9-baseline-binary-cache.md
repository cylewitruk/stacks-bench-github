# v9: Release-baseline binary cache

Successor to [v8-slack-card-redesign](../archive/completed/0023-slack-card-redesign.md).
Skip the ~5–7 min `stacks-bench` build for refs we bench repeatedly by caching
the built binary — fingerprint-keyed — and staging it in place of the build
phase. **Local-only first**; fleet / S3 sharing deferred.

*(Deployment-version lineage; last deployed was v8. Canonical item identity is
`0025-baseline-binary-cache`.)*

> **Status:** in_progress — Phase 0 spike Codex-signed-off; **Phase 1 built +
> green** (cache module, config, fingerprint inputs, gated driver build-skip), and
> the **`BranchPrefix`** matcher (a Phase 2 slice) landed. The driver's VM-skip
> orchestration is **not** host-validated (no libvirt in CI) — it's gated behind
> `[artifacts.binary_cache].enabled` so the default path is byte-identical.
> **Remaining:** Phase 2 pin policy (migration + `pinned`/`pinned_until` + warm),
> pin-set→eviction wiring, and the "reused cached binary" build-row text. Scope is
> **local-only**; fleet / S3 sharing rides `0004-worker-fleet`.
>
> **Implemented (green, lint-clean):** `crates/sbgh-daemon/src/binary_cache.rs`
> (`BuildFingerprint` + `BinaryCache`: atomic publish, size-bounded LRU,
> pinned-protect, integrity, toolchain pin-guard, image proxy — 12 tests);
> `[artifacts.binary_cache]` config (2 tests); `LibvirtDriver` probe → source-disk
> seed → build-VM skip → publish-on-finish (gated, host-unvalidated);
> `TriggerMatchSpec::BranchPrefix` + webhook matching (1 test).
>
> **Codex review (2 passes), all addressed:** (**High**) `SourceDisk::provision`
> *unmounts* the disk before returning, so the first cut read/seeded an empty host
> `source.mnt`. Fixed: the fingerprint reads `rust-toolchain.toml` from the **bare
> git mirror** (`git show <sha>:…` — no mount), and a hit seeds via new
> `SourceDisk::seed_binary` (re-attach → mount → copy → `chmod 0755` → chown root →
> umount → detach). (**Toolchain contract**) the fingerprint keys by the
> **declared** channel (`1.95.0`, or a floating `stable`) — a pragmatic reuse key,
> not `rustc -vV` provenance, since compiler drift is insignificant for this
> I/O-bound workload. (**Exec mode**) the seeded binary is forced `0o755`.
> (**Atomic-publish doc**) tightened to "new entry or transient miss". Tests:
> `RecordingShell` coverage of the seed sequence, the mirror-read fingerprint
> (declared / missing), and a **driver-level hit** (`enabled_cache_hit_…`: hit →
> seed → Build done → no `virsh`).

## Items

| Item | Role | Status |
| ---- | ---- | ------ |
| `0025-baseline-binary-cache` | primary | planned |

## Why

A `stacks-bench` build is ~5–7 min: cargo `--release` (`lto`, `codegen-units=1`)
plus the build VM's boot / mount / teardown. For the handful of designated
release refs (`cylewitruk/stacks-core` `sb-integration/3.W.X.Y.Z`) benched
repeatedly — e.g. a Slack `bench blocks n..m on 3.4.0.0.3` — that cost is paid
every time for an **unchanging** binary. The binary is deterministic per
`(source commit, build environment)`, so it is cacheable.

## Design (Phase 0 spike)

### Cache key — the build fingerprint

The fingerprint is an **explicit hash of every input that can change the binary**
— not a commit hash trusted to subsume them (Codex: make it load-bearing):

- **`commit_sha`** — the resolved source commit.
- **declared toolchain channel** — the `[toolchain].channel` from
  `rust-toolchain.toml` verbatim (`1.95.0`, or a floating `stable`). A
  **pragmatic** reuse key, not `rustc -vV` provenance: for this I/O-bound workload
  compiler drift within a channel is insignificant next to
  chainstate / disk / VM-profile effects, so a pinned channel keys as itself and a
  floating `stable` keys as `stable`.
- **profile / features / `RUSTFLAGS`** — `[profile.release]` (`lto`,
  `codegen-units`), the feature set, and any `RUSTFLAGS` / build-env the template
  sets.
- **target triple / arch** — `x86_64-unknown-linux-gnu` today (explicit for the
  fleet).
- **build-recipe version** — a daemon constant bumped whenever the build template
  / flags change in a binary-affecting way.
- **environment image** — the golden VM image identity (OS / glibc / linker,
  [config.rs](../../crates/sbgh-core/src/config.rs) `[vm].golden_image`), or the
  operator-declared `measurement_profile` once it lands.
- **artifact protocol / schema version** — a profiler-protocol bump (cf. `0027`)
  must invalidate stale binaries.

`fingerprint = hash(`all of the above`)`. **Soundness rule:** a hit reuses a
binary only on an **exact** fingerprint match — a different image / arch /
toolchain / recipe never silently reuses an incompatible binary.

**Pre-boot probe.** The hit probe runs on the host *before* any VM boots: it reads
`rust-toolchain.toml` from the **bare git mirror** (`git show <sha>:…`, no
source-disk mount) and keys by the **declared** channel. A commit with no
`rust-toolchain.toml` simply isn't cached (build normally). This is a pragmatic
binary-reuse cache, not a bit-for-bit provenance system (Codex).

> **Open (spike):** golden-image identity — an operator-declared `golden_image_id`
> (a short version string in config) vs a cheap proxy (backing-file size + mtime)
> vs a content hash (expensive on a multi-GB qcow2). Lean: operator-declared id,
> folding into `measurement_profile` later.

### Cache layout + eviction

- On disk, sibling to the run archive: `binary-cache/<fingerprint>/` holding
  `stacks-bench` plus a `meta.json` (`commit`, fingerprint inputs, `sha256`,
  `size`, `created_at`, `last_used_at`, `pinned`, `pinned_until`).
- **Size-bounded** by a configured budget (`[artifacts.binary_cache].max_size`,
  e.g. `"10G"` ≈ 33–40 binaries at ~250–300 MB). Eviction: **pinned entries are
  never LRU-evicted**; non-pinned entries are evicted least-recently-used once the
  total exceeds the budget. If pins alone exceed the budget, pins win (never
  evicted) and we **log** that the cap is over-subscribed — the budget caps the
  LRU tail and acts as a floor for pins.
- **Population is free from the existing archive:** every run already keeps its
  binary on the host (`put_local_only`, the v8.x change —
  [artifact_store.rs](../../crates/sbgh-daemon/src/artifact_store.rs)). Phase 1
  **indexes that binary into the cache by fingerprint on completion** — no extra
  build. Warm builds for pinned refs come in Phase 2.
- Integrity: the stored `sha256` is verified before a hit is reused.

### The release / pin policy — *pin on the existing policy, not a parallel config*

Yes, your instinct is right — pinning belongs on the existing **`trigger_policy`**
([models.rs](../../crates/sbgh-core/src/models.rs),
[migration](../../migrations/20260527000005_slice5_policies.sql)), not a new config
surface:

- Add `pinned BOOLEAN NOT NULL DEFAULT FALSE` + `pinned_until TIMESTAMPTZ NULL`
  to `trigger_policy` (additive migration). `pinned = true` means "keep this ref's
  built binary on hand"; `pinned_until` optionally expires the pin (after which
  the entry drops to the LRU tail).
- **The pin follows the ref → commit.** The pinned set = each pinned policy's
  currently-resolved ref → SHA → fingerprint. When a pinned branch advances, the
  new commit's binary is pinned and the old one falls to LRU (no longer the ref's
  head) — so a moving branch self-evicts stale binaries while always keeping its
  current one warm.
- **Branch-prefix gap (spike decision).** Branch matching is **exact** today
  (`branch_name == branch`,
  [webhook_processor.rs](../../crates/sbgh-daemon/src/webhook_processor.rs));
  tags are regex. The release **branches** (`sb-integration/3.*`) want a
  **prefix** match. Lean: add an explicit **prefix** matcher (e.g.
  `TriggerMatchSpec::BranchPrefix { prefix }`, matching `sb-integration/3.`) — a
  plain prefix, **not** a glob, to avoid surprising pattern semantics (Codex) —
  rather than one exact policy per branch.

### Build-skip mechanism — *how the build phase skips safely*

**Where the bench actually runs the binary (Codex correction):** the bench VM
execs `./target/release/stacks-bench` from the **source disk** (`$SRC`, the
persistent `sbgh-src` vdc) —
[sbgh-bench.sh.tmpl:86](../../crates/sbgh-daemon/src/libvirt/templates/sbgh-bench.sh.tmpl).
The build VM's copy to `$RESULTS/stacks-bench` is **archival only**
([sbgh-build.sh.tmpl:76](../../crates/sbgh-daemon/src/libvirt/templates/sbgh-build.sh.tmpl)).
So a skip must put the binary where the *bench* looks — the source disk — not the
results share.

On claim, the host computes the fingerprint and probes the cache **before
provisioning**:

- **Hit → seed the source disk, skip the build VM.** The host provisions the
  source disk as usual, then stages the cached binary at
  `$SRC/target/release/stacks-bench` (exactly where a build would leave it) and
  mirrors it to the results tmpfs `$RESULTS/stacks-bench`, so the archival /
  forensic layout and `binary_archived_path` stay identical to a real build. The
  build VM is **skipped**; the bench VM boots and runs the binary unchanged — **no
  bench-template edit**, since the run path is byte-identical to a normal build.
  This saves cargo **and** the build VM's boot / mount / teardown. The Reporter
  emits the Build row instantly-done ("Reused cached binary @ `<sha>`") and bumps
  `last_used_at`.
- **Ownership normalization moves to the host.** The build VM's
  `chown -R root:root "$SRC"`
  ([sbgh-build.sh.tmpl:45](../../crates/sbgh-daemon/src/libvirt/templates/sbgh-build.sh.tmpl)
  — git's CVE-2022-24765 guard, since the host clones as `sbgh` but the VMs run as
  root) doesn't run on a skip. The host's staging assumes that role: it normalizes
  the source-disk ownership and sets the seeded binary executable (`+x`,
  root-owned) before detaching it, so the root bench VM is satisfied.
- **Miss:** today's path — the build VM produces the binary; on completion it's
  **published into the cache** (atomically — see Phase 1) by fingerprint.
- **Safe-skip guards:** exact fingerprint match (compatible env) **and** verified
  `sha256` **and** the binary present and executable. Any guard failing → fall
  back to a normal build (never a hard failure).

> **Decided (was Open):** mechanism (a) — **seed the source disk** and skip the
> build VM — over (b) changing the bench template to exec `$RESULTS/stacks-bench`.
> (a) keeps the run path identical to a normal build (same disk, same path, same
> exec semantics) and needs no template conditional.

## Phases

### Phase 1: Local fingerprint cache + build-skip

**Scope:** fingerprint computation (the full input set above); the on-disk cache
store (`get` / publish / `evict`, size-bounded LRU) with **atomic publish** —
write to a temp path, verify `sha256` + metadata, then `rename` into the
`<fingerprint>/` dir — under a **per-fingerprint lock** so concurrent same-commit
jobs never race a half-written binary or double-build; populate-on-finish from the
archived binary; the host-side hit probe + **source-disk staging** + build-VM
skip; the Reporter "reused cached binary" Build-row state. **No pinning yet** —
every repeated `(commit, env)` already benefits from LRU alone.

**Acceptance:** a second bench of the same commit + env **skips the build VM** and
reuses the `sha`-verified cached binary; concurrent same-commit jobs never observe
a partial entry; the cache stays under its size budget via LRU; a miss or
guard-failure falls back to a clean build.

### Phase 2: Pinned release policy + prefix matching + warm

**Scope:** `pinned` / `pinned_until` on `trigger_policy` (migration + admin CLI +
the `TriggerPolicy` struct); pinned-set resolution (ref → commit → fingerprint),
pin-protected from LRU; branch **prefix** matching; warm pinned binaries on
policy-ref push / tag and on daemon start.

**Acceptance:** a pinned release ref's binary survives LRU pressure and is
warm-built on push / start, so a Slack `bench … on <release>` skips the build with
**no** prior on-demand run.

### Deferred — fleet / S3 sharing

The cache is host-local. Sharing fingerprinted binaries across a worker fleet (an
S3-backed cache keyed by the **same** fingerprint, each worker pinning its
`measurement_profile` set) rides `0004-worker-fleet` and is **out of scope** here.
Filed as a follow-up.

## Decisions

1. **Pin on the policy, not a parallel config** — `trigger_policy.pinned` /
   `pinned_until` (your call); a pinned ref is just a trigger policy with the flag.
2. **Fingerprint is an explicit hash of all binary-affecting inputs** — commit,
   **declared** toolchain channel (pragmatic, not `rustc -vV`),
   profile/features/RUSTFLAGS, target triple, build-recipe version,
   image/`measurement_profile`, protocol version — exact-match-only hits, **not**
   commit-subsumption.
3. **Skip the whole build VM** on a hit by **seeding the source disk**
   (`$SRC/target/release/stacks-bench`, where the bench execs) — not the results
   share; the host assumes the build VM's source-disk ownership normalization.
4. **Seed the cache from `put_local_only`** archived binaries — population is a
   fingerprint index, not an extra build.
5. **Size-bounded, pinned-priority + LRU** — pins are never evicted (the budget
   caps the LRU tail).
6. **Atomic publish** — entries are written to a temp path, sha/metadata verified,
   then `rename`d into the fingerprint dir under a per-fingerprint lock; no job
   ever reads a half-written binary.

## Open questions (spike → Codex)

- **Golden-image identity:** operator-declared id vs file-stat proxy vs content
  hash (lean: operator id → `measurement_profile`).
- **Branch prefix matching:** an explicit `BranchPrefix` matcher (plain prefix,
  not glob) vs per-branch exact policies (lean: prefix matcher).
- **Pinned-overflow:** warn-and-keep (lean) vs reject new pins past the budget.

## Follow-Ups

- Fleet / S3 shared binary cache (rides `0004-worker-fleet`).
- The fingerprint's `build_recipe_version` should bump from the same
  protocol-version surface `0027` introduces.
