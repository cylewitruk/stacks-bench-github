//! Fingerprint-keyed local cache of built `stacks-bench` binaries.
//!
//! A `stacks-bench` build is ~5–7 min; the binary is deterministic per
//! `(source commit, build environment)`, so it's cacheable. This module owns
//! the on-disk store:
//!
//! - [`BuildFingerprint`] — the **exact-match** key. Every field is
//!   load-bearing (a hit reuses a binary only on an identical fingerprint), so
//!   a different toolchain / image / arch / recipe never silently reuses an
//!   incompatible binary.
//! - [`BinaryCache`] — publishes entries **atomically** (stage in a temp dir →
//!   verify `sha256` → `rename` into place, so a reader never sees a
//!   half-written entry), serves `sha`-verified hits, and holds the store under
//!   a configured size budget by evicting non-pinned entries
//!   **least-recently-used** (pinned entries are never evicted; the budget caps
//!   the LRU tail).
//!
//! The cache is local-only. A worker fleet may share the same fingerprint
//! contract through a remote store, but that is outside this module.
#![allow(dead_code)]

use std::collections::HashSet;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use sbgh_core::config::BinaryCacheConfig;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Filename of the cached binary inside an entry dir.
const BINARY_NAME: &str = "stacks-bench";
/// Filename of the per-entry metadata sidecar.
const META_NAME: &str = "meta.json";
/// Prefix for in-flight staging dirs (never served as a hit).
const TMP_PREFIX: &str = ".staging-";

/// Every input that determines a `stacks-bench` binary's identity. The fields
/// are **load-bearing**: a hit reuses a binary only on an exact [`digest`]
/// match, so a different toolchain / image / arch / recipe never silently
/// reuses an incompatible binary.
///
/// [`digest`]: BuildFingerprint::digest
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildFingerprint {
    /// Resolved source commit (40-hex).
    pub commit: String,
    /// The **declared** toolchain channel from `rust-toolchain.toml` (e.g.
    /// `1.95.0`, or a floating `stable`). A pragmatic reuse key — the
    /// *declaration*, not the resolved `rustc -vV`: for this I/O-bound workload
    /// compiler drift within a channel is insignificant next to
    /// chainstate / disk / VM-profile effects, so we key by the declared
    /// channel rather than chase bit-for-bit provenance.
    pub toolchain: String,
    /// Release profile knobs that change codegen (`lto`, `codegen-units`, …).
    pub profile: String,
    /// Cargo feature set the binary was built with.
    pub features: String,
    /// `RUSTFLAGS` / build-env the template exports.
    pub rustflags: String,
    /// Target triple (`x86_64-unknown-linux-gnu`).
    pub target_triple: String,
    /// Daemon build-recipe version — bumped when the build command / flags
    /// change in a binary-affecting way.
    pub recipe_version: u32,
    /// Golden VM image identity (OS / glibc / linker), or the operator's
    /// `measurement_profile` once it lands.
    pub image_id: String,
    /// Artifact protocol / schema version — a profiler-protocol bump (`0027`)
    /// invalidates stale binaries.
    pub protocol_version: String,
}

impl BuildFingerprint {
    /// Stable lowercase-hex SHA-256 over the canonical JSON of the fields — the
    /// cache directory key. Struct field order is fixed, so the digest is
    /// reproducible across runs and hosts.
    pub fn digest(&self) -> String {
        let json = serde_json::to_vec(self).expect("BuildFingerprint always serializes");
        hex::encode(Sha256::digest(&json))
    }

    /// The build-environment projection — every field except the source
    /// identity (`commit`, `toolchain`). The pin resolver matches a cached
    /// entry by commit **and** this environment.
    pub fn environment(&self) -> CacheEnvironment {
        CacheEnvironment {
            profile: self.profile.clone(),
            features: self.features.clone(),
            rustflags: self.rustflags.clone(),
            target_triple: self.target_triple.clone(),
            recipe_version: self.recipe_version,
            image_id: self.image_id.clone(),
            protocol_version: self.protocol_version.clone(),
        }
    }
}

/// The build-environment half of a [`BuildFingerprint`] — every field *except*
/// the source identity (`commit`, `toolchain`). Pinning matches a cached entry
/// by commit **and** this environment, so a golden-image / recipe / protocol
/// change retires stale same-commit pins instead of keeping them warm forever.
/// Toolchain is deliberately out: same commit implies the same declared
/// `rust-toolchain` under the declared-toolchain reuse contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheEnvironment {
    pub profile: String,
    pub features: String,
    pub rustflags: String,
    pub target_triple: String,
    pub recipe_version: u32,
    pub image_id: String,
    pub protocol_version: String,
}

impl CacheEnvironment {
    /// Whether `fp` was built under this environment (source identity aside).
    fn matches(&self, fp: &BuildFingerprint) -> bool {
        self.profile == fp.profile
            && self.features == fp.features
            && self.rustflags == fp.rustflags
            && self.target_triple == fp.target_triple
            && self.recipe_version == fp.recipe_version
            && self.image_id == fp.image_id
            && self.protocol_version == fp.protocol_version
    }

    /// Complete this environment into a full [`BuildFingerprint`] by adding the
    /// source identity (`commit` + declared `toolchain`). The inverse of
    /// [`BuildFingerprint::environment`] — so the driver's fingerprint assembly
    /// and the pin resolver share one definition of the environment and can't
    /// drift.
    pub fn fingerprint(&self, commit: String, toolchain: String) -> BuildFingerprint {
        BuildFingerprint {
            commit,
            toolchain,
            profile: self.profile.clone(),
            features: self.features.clone(),
            rustflags: self.rustflags.clone(),
            target_triple: self.target_triple.clone(),
            recipe_version: self.recipe_version,
            image_id: self.image_id.clone(),
            protocol_version: self.protocol_version.clone(),
        }
    }
}

/// Construct the opt-in binary cache from config, or `None` when
/// `[artifacts.binary_cache].enabled` is false (the default). Shared by the
/// driver (publish / get) and the pin resolver (set-pinned / evict) so both
/// agree on the same on-disk dir + size budget.
pub fn build_binary_cache(config: &BinaryCacheConfig) -> Option<Arc<BinaryCache>> {
    config
        .enabled
        .then(|| Arc::new(BinaryCache::new(config.dir.clone(), config.max_size.as_bytes())))
}

/// Per-entry metadata, stored alongside the binary as `meta.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheMeta {
    pub fingerprint: BuildFingerprint,
    pub digest: String,
    /// SHA-256 of the cached binary — verified before a hit is reused.
    pub sha256: String,
    pub size_bytes: u64,
    /// Unix seconds; set at publish, bumped on every hit (drives LRU).
    pub last_used: u64,
    pub created_at: u64,
    /// Pin-protected from LRU eviction while `true`.
    pub pinned: bool,
}

/// A served cache hit: the local path to the `sha`-verified binary + its
/// metadata.
pub struct CachedBinary {
    pub path: PathBuf,
    pub meta: CacheMeta,
}

/// On-disk, size-bounded, fingerprint-keyed cache of built binaries. Layout:
/// `<root>/<digest>/{stacks-bench, meta.json}`.
pub struct BinaryCache {
    root: PathBuf,
    max_bytes: u64,
    /// Serializes all mutations (publish-finalize, evict, `last_used` bump) so
    /// concurrent same-store jobs can't corrupt accounting. Entry-level
    /// atomicity is the `rename`; this is stricter than the per-fingerprint
    /// lock the design calls for (it serializes across fingerprints too),
    /// which is the safe direction — finer granularity is a throughput-only
    /// refinement.
    mutate: Mutex<()>,
}

impl BinaryCache {
    pub fn new(root: PathBuf, max_bytes: u64) -> Self {
        Self {
            root,
            max_bytes,
            mutate: Mutex::new(()),
        }
    }

    fn entry_dir(&self, digest: &str) -> PathBuf {
        self.root.join(digest)
    }

    /// Serve a `sha`-verified hit for `fp`, bumping its `last_used` (LRU), or
    /// `None` on a miss / corrupt entry. Best-effort: a missing or mismatched
    /// binary returns `None` so the caller falls back to a normal build.
    pub fn get(&self, fp: &BuildFingerprint, now_unix: u64) -> Option<CachedBinary> {
        let digest = fp.digest();
        let dir = self.entry_dir(&digest);
        let bin = dir.join(BINARY_NAME);
        if !bin.is_file() {
            return None;
        }
        let mut meta = read_meta(&dir)?;
        // Integrity: the binary must hash to the recorded sha256.
        match sha256_file(&bin) {
            Ok(actual) if actual == meta.sha256 => {}
            Ok(_) => {
                tracing::warn!(digest, "binary cache: sha256 mismatch; ignoring corrupt entry");
                return None;
            }
            Err(e) => {
                tracing::warn!(error = %e, digest, "binary cache: failed to hash entry");
                return None;
            }
        }
        let _g = self.lock();
        meta.last_used = now_unix;
        let _ = write_meta(&dir, &meta);
        Some(CachedBinary { path: bin, meta })
    }

    /// Atomically publish `src` as the binary for `fp`, then evict to budget.
    /// `pinned` marks the entry pin-protected. Returns the stored `sha256`.
    ///
    /// Atomic: the binary + meta are staged in a sibling temp dir, then the dir
    /// is `rename`d into `<digest>/`. A reader therefore never sees a
    /// *half-written* entry — it sees the previous entry, the **new** entry, or
    /// (briefly, while an existing entry is being replaced) a miss, which falls
    /// back to a build. (A true never-miss swap would need rename-aside or a
    /// versioned-pointer scheme; the transient miss is harmless here.)
    pub fn publish(
        &self,
        fp: &BuildFingerprint,
        src: &Path,
        now_unix: u64,
        pinned: bool,
    ) -> std::io::Result<String> {
        let digest = fp.digest();
        let sha = sha256_file(src)?;
        let size_bytes = std::fs::metadata(src)?.len();
        let meta = CacheMeta {
            fingerprint: fp.clone(),
            digest: digest.clone(),
            sha256: sha.clone(),
            size_bytes,
            last_used: now_unix,
            created_at: now_unix,
            pinned,
        };

        let _g = self.lock();
        std::fs::create_dir_all(&self.root)?;
        let staging = self
            .root
            .join(format!("{TMP_PREFIX}{digest}"));
        let _ = std::fs::remove_dir_all(&staging);
        std::fs::create_dir_all(&staging)?;
        std::fs::copy(src, staging.join(BINARY_NAME))?;
        write_meta(&staging, &meta)?;
        // `rename` onto a non-empty dir fails, so clear any prior entry first.
        // The window is held under `mutate`, and a reader that misses simply
        // falls back to a build.
        let dir = self.entry_dir(&digest);
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::rename(&staging, &dir)?;

        self.evict_locked(&[digest]);
        Ok(sha)
    }

    /// Re-evaluate the size budget, evicting non-pinned entries LRU until the
    /// store fits. Pinned entries (flagged by [`Self::set_pinned_by_commit`])
    /// are never evicted. Called after the pin resolver recomputes the pinned
    /// set. No-op when already under budget.
    pub fn evict_to_budget(&self) {
        let _g = self.lock();
        self.evict_locked(&[]);
    }

    /// Pin exactly the entries whose source `commit` is in `commits` **and**
    /// whose build environment matches `env`; clear the flag on every other
    /// entry. The pinned set follows the resolved refs (a ref that moved off a
    /// commit un-pins the old binary), and the env guard keeps a pin from
    /// protecting a stale same-commit binary built under a prior image / recipe
    /// era. Called on startup and after each publish.
    pub fn set_pinned_by_commit(&self, commits: &HashSet<String>, env: &CacheEnvironment) {
        let _g = self.lock();
        for meta in self.scan_entries() {
            let want = commits.contains(&meta.fingerprint.commit) && env.matches(&meta.fingerprint);
            if meta.pinned != want {
                let mut updated = meta;
                let dir = self.entry_dir(&updated.digest);
                updated.pinned = want;
                let _ = write_meta(&dir, &updated);
            }
        }
    }

    /// Whether a built binary for `commit` under `env` is already cached — the
    /// warming skip-check. **Repo-agnostic by
    /// design**: the cache is keyed by `(commit, build env)`, not repo, so a
    /// commit's binary counts as present regardless of which repo / fork
    /// reached it. Same match as [`Self::set_pinned_by_commit`] (commit +
    /// env), so a stale-env entry for the commit does NOT count as warm.
    pub fn has_entry_for(&self, commit: &str, env: &CacheEnvironment) -> bool {
        let _g = self.lock();
        self.scan_entries()
            .iter()
            .any(|m| m.fingerprint.commit == commit && env.matches(&m.fingerprint))
    }

    /// Evict non-pinned entries LRU until the store fits the budget. Caller
    /// holds [`Self::mutate`]. `protected` is extra digests to never evict
    /// (e.g. the entry just published).
    fn evict_locked(&self, protected: &[String]) {
        let mut entries = self.scan_entries();
        let mut total: u64 = entries
            .iter()
            .map(|e| e.size_bytes)
            .sum();
        if total <= self.max_bytes {
            return;
        }
        let protected: HashSet<&str> = protected
            .iter()
            .map(String::as_str)
            .collect();
        let pinned_bytes: u64 = entries
            .iter()
            .filter(|e| e.pinned)
            .map(|e| e.size_bytes)
            .sum();
        if pinned_bytes > self.max_bytes {
            tracing::warn!(
                pinned_bytes,
                budget = self.max_bytes,
                "binary cache: pinned entries exceed the size budget; keeping them (the budget \
                 caps the LRU tail, not the pins)"
            );
        }
        // Oldest-used first.
        entries.sort_by_key(|e| e.last_used);
        for e in entries {
            if total <= self.max_bytes {
                break;
            }
            if e.pinned || protected.contains(e.digest.as_str()) {
                continue;
            }
            if std::fs::remove_dir_all(self.entry_dir(&e.digest)).is_ok() {
                total -= e.size_bytes;
                tracing::info!(digest = %e.digest, "binary cache: evicted LRU entry");
            }
        }
    }

    /// Read every entry's metadata. Skips staging dirs and any entry whose
    /// `meta.json` is missing/unreadable.
    fn scan_entries(&self) -> Vec<CacheMeta> {
        let Ok(rd) = std::fs::read_dir(&self.root) else {
            return Vec::new();
        };
        rd.flatten()
            .filter(|e| {
                e.file_type()
                    .map(|t| t.is_dir())
                    .unwrap_or(false)
            })
            .filter(|e| {
                !e.file_name()
                    .to_string_lossy()
                    .starts_with(TMP_PREFIX)
            })
            .filter_map(|e| read_meta(&e.path()))
            .collect()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ()> {
        self.mutate
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }
}

fn read_meta(dir: &Path) -> Option<CacheMeta> {
    let bytes = std::fs::read(dir.join(META_NAME)).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn write_meta(dir: &Path, meta: &CacheMeta) -> std::io::Result<()> {
    let bytes = serde_json::to_vec_pretty(meta).expect("CacheMeta always serializes");
    std::fs::write(dir.join(META_NAME), bytes)
}

/// Stream a file through SHA-256 (a 250–300 MB binary is never read fully into
/// memory). Returns the lowercase-hex digest.
fn sha256_file(path: &Path) -> std::io::Result<String> {
    let mut f = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 1 << 16];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex::encode(hasher.finalize()))
}

/// Read the **declared** toolchain channel from a `rust-toolchain.toml` — the
/// `[toolchain].channel` string verbatim (e.g. `1.95.0`, or a floating
/// `stable`) — for the [`BuildFingerprint::toolchain`] key. `None` only when
/// the channel is absent / unparseable (then the binary isn't cached). A
/// pragmatic binary-reuse key, not bit-for-bit provenance: a pinned channel
/// keys as itself, a floating `stable` keys as `stable`.
pub fn toolchain_channel(toolchain_toml: &str) -> Option<String> {
    let value: toml::Value = toml::from_str(toolchain_toml).ok()?;
    let channel = value
        .get("toolchain")?
        .get("channel")?
        .as_str()?
        .trim()
        .to_string();
    (!channel.is_empty()).then_some(channel)
}

/// Read the legacy `rust-toolchain` format: a single declared channel name,
/// e.g. `stable`, `1.85.0`, or `nightly-2026-01-01`. The cache treats this as
/// the same pragmatic declaration key as `rust-toolchain.toml`.
pub fn legacy_toolchain_channel(toolchain: &str) -> Option<String> {
    let channel = toolchain
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())?;
    (!channel.contains(char::is_whitespace)).then(|| channel.to_string())
}

/// A cheap identity for the golden VM image: `<size>:<mtime_secs>`. The image's
/// OS / glibc / linker fix the binary's runtime ABI, so a changed image must
/// invalidate cached binaries. Hashing a multi-GB qcow2 for every job is too
/// costly, so size and mtime provide the current cache-invalidation proxy.
pub fn image_proxy_id(image_path: &Path) -> std::io::Result<String> {
    let m = std::fs::metadata(image_path)?;
    let mtime = m
        .modified()
        .ok()
        .and_then(|t| {
            t.duration_since(std::time::UNIX_EPOCH)
                .ok()
        })
        .map(|d| d.as_secs())
        .unwrap_or(0);
    Ok(format!("{}:{}", m.len(), mtime))
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    fn fp(commit: &str) -> BuildFingerprint {
        BuildFingerprint {
            commit: commit.into(),
            toolchain: "1.95.0".into(),
            profile: "release;lto=thin;codegen-units=1".into(),
            features: "".into(),
            rustflags: "".into(),
            target_triple: "x86_64-unknown-linux-gnu".into(),
            recipe_version: 1,
            image_id: "ubuntu24-golden-v1".into(),
            protocol_version: "p1".into(),
        }
    }

    /// Write a `src` binary of `size` bytes filled with `fill` (so different
    /// entries hash differently).
    fn write_src(dir: &TempDir, name: &str, size: usize, fill: u8) -> PathBuf {
        let p = dir.path().join(name);
        std::fs::write(&p, vec![fill; size]).unwrap();
        p
    }

    #[test]
    fn digest_is_stable_and_field_sensitive() {
        let a = fp("aaa").digest();
        assert_eq!(a, fp("aaa").digest(), "same inputs → same digest");
        assert_ne!(a, fp("bbb").digest(), "commit change → different digest");

        let mut diff = fp("aaa");
        diff.toolchain = "1.96.0".into();
        assert_ne!(a, diff.digest(), "toolchain change → different digest");

        let mut diff = fp("aaa");
        diff.image_id = "ubuntu26".into();
        assert_ne!(a, diff.digest(), "image change → different digest");
    }

    #[test]
    fn publish_then_get_round_trips_and_bumps_last_used() {
        let tmp = TempDir::new().unwrap();
        let cache = BinaryCache::new(tmp.path().join("cache"), 1 << 30);
        let src = write_src(&tmp, "bin", 1024, 7);

        let sha = cache
            .publish(&fp("aaa"), &src, 100, false)
            .unwrap();
        let hit = cache
            .get(&fp("aaa"), 200)
            .expect("published entry is a hit");
        assert_eq!(hit.meta.sha256, sha);
        assert_eq!(hit.meta.last_used, 200, "get bumps last_used");
        assert_eq!(std::fs::read(&hit.path).unwrap(), vec![7u8; 1024]);
    }

    #[test]
    fn get_misses_when_absent() {
        let tmp = TempDir::new().unwrap();
        let cache = BinaryCache::new(tmp.path().join("cache"), 1 << 30);
        assert!(
            cache
                .get(&fp("absent"), 1)
                .is_none()
        );
    }

    #[test]
    fn get_rejects_a_corrupted_binary() {
        let tmp = TempDir::new().unwrap();
        let cache = BinaryCache::new(tmp.path().join("cache"), 1 << 30);
        let src = write_src(&tmp, "bin", 1024, 7);
        cache
            .publish(&fp("aaa"), &src, 100, false)
            .unwrap();

        // Tamper with the stored binary so its sha no longer matches meta.
        let bin = cache
            .entry_dir(&fp("aaa").digest())
            .join(BINARY_NAME);
        std::fs::write(&bin, vec![9u8; 1024]).unwrap();
        assert!(
            cache
                .get(&fp("aaa"), 200)
                .is_none(),
            "sha mismatch → miss"
        );
    }

    #[test]
    fn republish_replaces_the_entry() {
        let tmp = TempDir::new().unwrap();
        let cache = BinaryCache::new(tmp.path().join("cache"), 1 << 30);
        cache
            .publish(&fp("aaa"), &write_src(&tmp, "v1", 1024, 1), 100, false)
            .unwrap();
        let sha2 = cache
            .publish(&fp("aaa"), &write_src(&tmp, "v2", 2048, 2), 101, false)
            .unwrap();
        let hit = cache
            .get(&fp("aaa"), 102)
            .unwrap();
        assert_eq!(hit.meta.sha256, sha2);
        assert_eq!(hit.meta.size_bytes, 2048, "re-publish replaced the entry");
    }

    #[test]
    fn lru_evicts_oldest_nonpinned_over_budget() {
        let tmp = TempDir::new().unwrap();
        // Budget fits ~2 of the 1 KiB entries.
        let cache = BinaryCache::new(tmp.path().join("cache"), 2500);
        cache
            .publish(&fp("old"), &write_src(&tmp, "a", 1024, 1), 10, false)
            .unwrap();
        cache
            .publish(&fp("mid"), &write_src(&tmp, "b", 1024, 2), 20, false)
            .unwrap();
        // Touch `old` so `mid` becomes the least-recently-used.
        cache.get(&fp("old"), 30);
        // Publishing a third entry pushes the store over budget → evict the LRU.
        cache
            .publish(&fp("new"), &write_src(&tmp, "c", 1024, 3), 40, false)
            .unwrap();

        assert!(
            cache
                .get(&fp("new"), 50)
                .is_some(),
            "just-published kept"
        );
        assert!(
            cache
                .get(&fp("old"), 50)
                .is_some(),
            "recently-used kept"
        );
        assert!(
            cache
                .get(&fp("mid"), 50)
                .is_none(),
            "LRU entry evicted"
        );
    }

    #[test]
    fn pinned_entries_survive_eviction() {
        let tmp = TempDir::new().unwrap();
        let cache = BinaryCache::new(tmp.path().join("cache"), 1500);
        // `pin` is the oldest, but pinned → must survive.
        cache
            .publish(&fp("pin"), &write_src(&tmp, "p", 1024, 1), 10, true)
            .unwrap();
        cache
            .publish(&fp("ad1"), &write_src(&tmp, "q", 1024, 2), 20, false)
            .unwrap();
        cache
            .publish(&fp("ad2"), &write_src(&tmp, "r", 1024, 3), 30, false)
            .unwrap();

        assert!(
            cache
                .get(&fp("pin"), 40)
                .is_some(),
            "pinned entry never evicted"
        );
        // Of the two ad-hoc entries the older (`ad1`) is gone.
        assert!(
            cache
                .get(&fp("ad1"), 40)
                .is_none(),
            "oldest non-pinned evicted"
        );
        assert!(
            cache
                .get(&fp("ad2"), 40)
                .is_some(),
            "newest non-pinned kept"
        );
    }

    #[test]
    fn set_pinned_by_commit_follows_the_resolved_set() {
        let tmp = TempDir::new().unwrap();
        let cache = BinaryCache::new(tmp.path().join("cache"), 1 << 30);
        cache
            .publish(&fp("aaa"), &write_src(&tmp, "a", 64, 1), 10, false)
            .unwrap();
        let env = fp("aaa").environment();

        let pinned = HashSet::from(["aaa".to_string()]);
        cache.set_pinned_by_commit(&pinned, &env);
        assert!(
            cache
                .get(&fp("aaa"), 11)
                .unwrap()
                .meta
                .pinned,
            "matching commit + env → pinned"
        );

        cache.set_pinned_by_commit(&HashSet::new(), &env);
        assert!(
            !cache
                .get(&fp("aaa"), 12)
                .unwrap()
                .meta
                .pinned,
            "un-pinned when the ref moves off the commit"
        );
    }

    #[test]
    fn set_pinned_by_commit_skips_a_stale_environment() {
        let tmp = TempDir::new().unwrap();
        let cache = BinaryCache::new(tmp.path().join("cache"), 1 << 30);
        // An entry for commit "aaa" built under a *prior* golden image.
        let mut old = fp("aaa");
        old.image_id = "ubuntu24-golden-OLD".into();
        cache
            .publish(&old, &write_src(&tmp, "a", 64, 1), 10, false)
            .unwrap();

        // The resolver pins commit "aaa", but under the *current* environment.
        let current_env = fp("aaa").environment();
        cache.set_pinned_by_commit(&HashSet::from(["aaa".to_string()]), &current_env);

        assert!(
            !cache
                .get(&old, 11)
                .unwrap()
                .meta
                .pinned,
            "same commit but a prior-image entry must NOT be pinned",
        );
    }

    #[test]
    fn has_entry_for_is_commit_and_env_scoped() {
        let tmp = TempDir::new().unwrap();
        let cache = BinaryCache::new(tmp.path().join("cache"), 1 << 30);
        cache
            .publish(&fp("aaa"), &write_src(&tmp, "a", 64, 1), 10, false)
            .unwrap();
        let env = fp("aaa").environment();

        assert!(cache.has_entry_for("aaa", &env), "the published commit + env is present");
        assert!(!cache.has_entry_for("bbb", &env), "a different commit is absent");

        // Same commit, prior environment (image) → not 'warm' under current env.
        let mut prior = fp("aaa");
        prior.image_id = "ubuntu26".into();
        assert!(
            !cache.has_entry_for("aaa", &prior.environment()),
            "a prior-env entry doesn't count as warm under the current env",
        );
    }

    #[test]
    fn toolchain_channel_reads_the_declared_channel() {
        // Pragmatic reuse key: pinned keys as itself, floating keys as itself.
        for ch in ["1.95.0", "1.85", "stable", "beta", "nightly", "nightly-2026-01-01"] {
            let t = format!("[toolchain]\nchannel = \"{ch}\"\n");
            assert_eq!(toolchain_channel(&t).as_deref(), Some(ch), "channel {ch}");
        }
        assert!(toolchain_channel("[toolchain]\n").is_none(), "no channel key");
        assert!(toolchain_channel("not toml {{{").is_none(), "garbage");
    }

    #[test]
    fn legacy_toolchain_channel_reads_single_line_declaration() {
        for ch in ["1.95.0", "stable", "nightly-2026-01-01"] {
            let t = format!("{ch}\n");
            assert_eq!(legacy_toolchain_channel(&t).as_deref(), Some(ch), "channel {ch}");
        }
        assert_eq!(legacy_toolchain_channel("\n  stable  \n").as_deref(), Some("stable"));
        assert!(legacy_toolchain_channel("").is_none(), "empty");
        assert!(legacy_toolchain_channel("stable extra").is_none(), "not a single channel token");
    }

    #[test]
    fn image_proxy_id_changes_with_content() {
        let tmp = TempDir::new().unwrap();
        let img = tmp
            .path()
            .join("golden.qcow2");
        std::fs::write(&img, b"aaaa").unwrap();
        let id1 = image_proxy_id(&img).unwrap();
        std::fs::write(&img, b"aaaaaaaaaaaa").unwrap();
        let id2 = image_proxy_id(&img).unwrap();
        assert_ne!(id1, id2, "a size change must change the image id");
        assert!(image_proxy_id(&tmp.path().join("absent")).is_err());
    }
}
