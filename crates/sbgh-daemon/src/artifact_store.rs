//! Pluggable run-artifact storage (item `0001-artifact-store`, iteration v4).
//!
//! Run artifacts (the SQLite db, `run.json`, the `stacks-bench` binary, the
//! phase log) live on the orchestrator's local disk today; Slack (`0002`) and
//! the portal (`0003`) need them off-box. [`ArtifactStore`] is the seam: a
//! local-FS impl (today's behavior) and, in the next slice, an S3-compatible
//! one.
//!
//! Contracts (see `planning/decisions/`):
//! - **0001** — `signed_url` is an S3-only affordance; [`LocalFsStore`] returns
//!   [`ArtifactUrlError::Unsupported`], and a consumer that needs a fetchable
//!   URL falls back to an authenticated download endpoint.
//! - **0002** — a run's artifact references are **store keys**
//!   (`<job_id>/<relative>`), resolved via [`ArtifactStore::get`]; for
//!   `LocalFsStore` a key resolves to today's exact
//!   `results_archive_dir/<job_id>/…` path, so the change is
//!   behavior-preserving.
//!
//! Wiring status (v4 Phase 1b): `put`, `get`, `artifact_key`, and `job_dir` are
//! live — the libvirt driver archives through `put`, and the
//! reporter/job-source/progress readers resolve keys via `get`. `exists`,
//! `signed_url`, and [`ArtifactUrlError`] are the **Phase-2** surface (the S3
//! store and the portal/Slack consumers), so the module keeps
//! `allow(dead_code)` until then.
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::time::Duration;

/// Why a [`signed_url`](ArtifactStore::signed_url) couldn't be produced — kept
/// typed so "this store can't sign" is distinguishable from "signing failed".
#[derive(Debug)]
pub enum ArtifactUrlError {
    /// The store can't mint externally-usable URLs (e.g. local FS). The caller
    /// must fall back to an authenticated download endpoint (Decision 0001).
    Unsupported,
    /// The store can sign in principle, but failed (backend / transient).
    Backend(String),
}

impl std::fmt::Display for ArtifactUrlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unsupported => f.write_str("signed URLs unsupported by this store"),
            Self::Backend(e) => write!(f, "signed URL backend error: {e}"),
        }
    }
}

impl std::error::Error for ArtifactUrlError {}

/// Build the store key for a named artifact of a job: `<job_id>/<relative>`.
/// Keys are impl-agnostic (Decision 0002), so the same addressing works for
/// local FS and object storage.
pub fn artifact_key(job_id: &str, relative: &str) -> String {
    format!("{job_id}/{relative}")
}

/// Pluggable storage for a run's archived artifacts.
pub trait ArtifactStore: Send + Sync {
    /// Store `src` under `key`. Best-effort (matching the prior forensics
    /// semantics): returns the stored byte size, or `None` if `src` is missing
    /// or the store write failed.
    fn put(&self, key: &str, src: &Path) -> Option<u64>;

    /// Resolve `key` to a **local readable path** — for a remote store this
    /// materializes the object locally first. `Err(NotFound)` if absent.
    fn get(&self, key: &str) -> std::io::Result<PathBuf>;

    /// A short-TTL fetchable URL for `key`.
    /// `Err(`[`ArtifactUrlError::Unsupported`]`)` when the store can't sign
    /// (`LocalFsStore` — Decision 0001), kept distinct from a backend
    /// failure so callers can branch the fallback correctly.
    fn signed_url(&self, key: &str, ttl: Duration) -> Result<String, ArtifactUrlError>;

    /// Whether `key` exists in the store.
    fn exists(&self, key: &str) -> bool;
}

/// Local-filesystem store rooted at `results_archive_dir` — the
/// behavior-preserving default. A key `<job_id>/<relative>` maps to
/// `<root>/<job_id>/<relative>`, exactly today's archive layout.
pub struct LocalFsStore {
    root: PathBuf,
}

impl LocalFsStore {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    /// The per-job archive directory (`<root>/<job_id>`) — the local diagnostic
    /// `archive_dir` (Decision 0002: a breadcrumb, not a fetch reference).
    pub fn job_dir(&self, job_id: &str) -> PathBuf {
        self.root.join(job_id)
    }

    /// Resolve `key` to a path **under the root**, rejecting anything that
    /// could escape it: absolute, `..`/`.`/prefix components, or empty.
    /// Keys flow from persisted run-summary data once consumers wire in
    /// (1b), so this is a security boundary, not just hygiene. `None` when
    /// the key is unsafe.
    fn checked_path(&self, key: &str) -> Option<PathBuf> {
        use std::path::Component;
        if key.is_empty()
            || !Path::new(key)
                .components()
                .all(|c| matches!(c, Component::Normal(_)))
        {
            return None;
        }
        Some(self.root.join(key))
    }
}

impl ArtifactStore for LocalFsStore {
    fn put(&self, key: &str, src: &Path) -> Option<u64> {
        if !src.exists() {
            return None;
        }
        let Some(dest) = self.checked_path(key) else {
            tracing::warn!(key, "artifact store: rejected unsafe key (put)");
            return None;
        };
        if let Some(parent) = dest.parent()
            && let Err(e) = std::fs::create_dir_all(parent)
        {
            tracing::warn!(error = %e, key, dest = %dest.display(), "artifact store: create dir failed");
            return None;
        }
        match std::fs::copy(src, &dest) {
            Ok(bytes) => Some(bytes),
            Err(e) => {
                tracing::warn!(error = %e, src = %src.display(), key, "artifact store: copy failed");
                None
            }
        }
    }

    fn get(&self, key: &str) -> std::io::Result<PathBuf> {
        let path = self
            .checked_path(key)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("unsafe artifact key: {key}"),
                )
            })?;
        if path.exists() {
            Ok(path)
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("artifact key not found: {key}"),
            ))
        }
    }

    /// Local FS can't mint an externally-usable URL (Decision 0001).
    fn signed_url(&self, _key: &str, _ttl: Duration) -> Result<String, ArtifactUrlError> {
        Err(ArtifactUrlError::Unsupported)
    }

    fn exists(&self, key: &str) -> bool {
        self.checked_path(key)
            .map(|p| p.exists())
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::TempDir;

    use super::*;

    fn store(tmp: &TempDir) -> LocalFsStore {
        LocalFsStore::new(tmp.path().join("archive"))
    }

    #[test]
    fn artifact_key_is_job_id_slash_relative() {
        assert_eq!(artifact_key("job1", "run.json"), "job1/run.json");
        assert_eq!(
            artifact_key("abc-123", "appdata/stacks-bench.db"),
            "abc-123/appdata/stacks-bench.db"
        );
    }

    #[test]
    fn put_then_get_round_trips_and_lands_at_root_slash_key() {
        let tmp = TempDir::new().unwrap();
        let s = store(&tmp);
        let src = tmp.path().join("run.json");
        std::fs::File::create(&src)
            .unwrap()
            .write_all(b"{\"ok\":true}")
            .unwrap();

        let key = artifact_key("job1", "run.json");
        let size = s.put(&key, &src);
        assert_eq!(size, Some(b"{\"ok\":true}".len() as u64));

        // The key resolves to today's exact `<root>/<job_id>/<relative>` path.
        let got = s.get(&key).unwrap();
        assert_eq!(
            got,
            tmp.path()
                .join("archive/job1/run.json")
        );
        assert_eq!(std::fs::read(&got).unwrap(), b"{\"ok\":true}");
        assert!(s.exists(&key));
    }

    #[test]
    fn put_missing_src_returns_none() {
        let tmp = TempDir::new().unwrap();
        let s = store(&tmp);
        assert_eq!(s.put(&artifact_key("job1", "run.json"), &tmp.path().join("nope")), None);
    }

    #[test]
    fn get_absent_key_is_not_found() {
        let tmp = TempDir::new().unwrap();
        let s = store(&tmp);
        let err = s
            .get(&artifact_key("job1", "run.json"))
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
        assert!(!s.exists(&artifact_key("job1", "run.json")));
    }

    #[test]
    fn signed_url_is_unsupported_for_local() {
        let tmp = TempDir::new().unwrap();
        let err = store(&tmp)
            .signed_url(&artifact_key("job1", "run.json"), Duration::from_secs(60))
            .unwrap_err();
        assert!(matches!(err, ArtifactUrlError::Unsupported));
    }

    #[test]
    fn unsafe_keys_cannot_escape_the_root() {
        let tmp = TempDir::new().unwrap();
        let s = store(&tmp);
        let src = tmp.path().join("payload");
        std::fs::write(&src, b"x").unwrap();
        for bad in ["", "..", "../escape", "job1/../../escape", "/abs/escape", "./x"] {
            assert_eq!(s.put(bad, &src), None, "put must reject {bad:?}");
            assert!(s.get(bad).is_err(), "get must reject {bad:?}");
            assert!(!s.exists(bad), "exists must be false for {bad:?}");
        }
        // Nothing was written outside the root.
        assert!(
            !tmp.path()
                .join("escape")
                .exists()
        );
        assert!(
            !tmp.path()
                .join("abs")
                .exists()
        );
    }

    #[test]
    fn job_dir_is_root_plus_job_id() {
        let tmp = TempDir::new().unwrap();
        assert_eq!(
            store(&tmp).job_dir("abc-123"),
            tmp.path()
                .join("archive/abc-123")
        );
    }
}
