//! Pluggable run-artifact storage (item `0001-artifact-store`, iteration v4).
//!
//! Run artifacts (the SQLite db, `run.json`, the `stacks-bench` binary, the
//! phase log) live on the orchestrator's local disk; Slack (`0002`) and the
//! portal (`0003`) need them off-box. [`ArtifactStore`] is the seam: a local-FS
//! impl ([`LocalFsStore`], today's behavior) and an S3-compatible one
//! ([`S3Store`]). [`build_store`] selects between them from `[artifacts]`
//! config.
//!
//! Contracts (see `planning/decisions/`):
//! - **0001** — `signed_url` is an S3-only affordance; [`LocalFsStore`] returns
//!   [`ArtifactUrlError::Unsupported`], and a consumer that needs a fetchable
//!   URL falls back to an authenticated download endpoint.
//! - **0002** — a run's artifact references are **store keys**
//!   (`<job_id>/<relative>`), resolved via [`ArtifactStore::get`]; for both
//!   stores a key resolves to today's exact `results_archive_dir/<job_id>/…`
//!   local path, so the change is behavior-preserving in local mode.
//! - **0003** — an `S3Store` upload failure after a completed run is **not** a
//!   benchmark failure: [`S3Store::put`] returns the *local* size and retains
//!   the local copy, logging the upload error.
//!
//! Wiring status: `put` / `get` / `job_dir` are live (the libvirt driver
//! archives through `put`; the reporter/job-source/progress readers resolve
//! keys via `get`; [`build_store`] picks the impl from config).
//! `signed_url_if_fetchable` is consumed by the Slack result snapshot's DB
//! download link; `exists` / `signed_url` / [`ArtifactUrlError`] remain for the
//! portal (`0003`) fetch path, so the module keeps `allow(dead_code)`.
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use base64::Engine as _;
use futures::StreamExt;
use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};
#[cfg(test)]
use sbgh_driver::ArtifactSink;
use sbgh_proto::{ArtifactDescriptor, ArtifactGrantResponse, HeaderValue};
use tokio::io::AsyncWriteExt;

/// TTL for the short-lived presigned URLs the store mints for its **own**
/// internal S3 GET/PUT/HEAD requests. Independent of the public
/// [`ArtifactStore::signed_url`] TTL (which the caller chooses).
const INTERNAL_SIGN_TTL: Duration = Duration::from_secs(300);

/// Cap on establishing the TCP/TLS connection to S3 — fails fast when the
/// endpoint is unreachable so a stalled connect can't hang job completion.
const S3_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Per-read **idle** timeout (not a total cap): a transfer that keeps making
/// progress is never cut off, but one that stalls for this long fails — so a
/// hung S3 GET/PUT can't wedge teardown/reporting after a benchmark finishes,
/// while a large-but-progressing multi-GB upload still completes.
const S3_READ_TIMEOUT: Duration = Duration::from_secs(120);

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

/// Build the store key for a submission-scoped artifact:
/// `<submission_prefix>/<relative>`. Singleton run artifacts keep using
/// [`artifact_key`]; this reserves the shared namespace for submission outputs.
pub fn submission_artifact_key(submission_prefix: &str, relative: &str) -> String {
    format!("{submission_prefix}/{relative}")
}

/// Submission-scoped SQLite DB carried between isolated repeat VMs.
pub const SUBMISSION_SQLITE_RELATIVE: &str = "shared/stacks-bench.db";

/// Sibling temp path for a streamed download (`<dest>.<token>.part`), renamed
/// to `dest` on completion so a partial transfer never lands at the key path.
/// The `token` is unique per download so two concurrent cache misses for the
/// same key don't write/rename the same temp file.
fn part_path(dest: &Path, token: &str) -> PathBuf {
    let mut s = dest
        .as_os_str()
        .to_os_string();
    s.push(format!(".{token}.part"));
    PathBuf::from(s)
}

/// Pluggable storage for a run's archived artifacts. `put`/`get`/`exists` are
/// async (object storage is network IO); `signed_url`/`job_dir` are pure (no
/// IO) and stay sync.
#[async_trait::async_trait]
pub trait ArtifactStore: Send + Sync {
    /// Store `src` under `key`. Best-effort (matching the prior forensics
    /// semantics): returns the stored byte size, or `None` if `src` is missing
    /// or the store write failed. For [`S3Store`] the size is the *local*
    /// mirror's (Decision 0003 — a failed S3 upload still returns `Some`).
    async fn put(&self, key: &str, src: &Path) -> Option<u64>;

    /// Like [`put`](ArtifactStore::put), but archive to the **local mirror
    /// only** — never upload to a remote backend. For a large, non-portable
    /// artifact (the in-VM `stacks-bench` binary: ~250-300 MB, built for the
    /// VM's arch) a host-side forensic copy is worth keeping, but shipping it
    /// to object storage every run is pure cost; cross-host binary reuse
    /// belongs in a keyed cache (`0025`), not this forensic archive.
    /// Default = [`put`](ArtifactStore::put) (for a local-only store the
    /// two coincide).
    async fn put_local_only(&self, key: &str, src: &Path) -> Option<u64> {
        self.put(key, src).await
    }

    /// Resolve `key` to a **local readable path** — for a remote store this
    /// materializes the object locally first. `Err(NotFound)` if absent.
    async fn get(&self, key: &str) -> std::io::Result<PathBuf>;

    /// The per-job local archive directory (`<root>/<job_id>`) — the diagnostic
    /// `archive_dir` (Decision 0002: a breadcrumb, not a fetch reference).
    /// Local for both stores (`S3Store` keeps a local mirror).
    fn job_dir(&self, job_id: &str) -> PathBuf;

    /// A short-TTL fetchable URL for `key`.
    /// `Err(`[`ArtifactUrlError::Unsupported`]`)` when the store can't sign
    /// (`LocalFsStore` — Decision 0001), kept distinct from a backend
    /// failure so callers can branch the fallback correctly.
    fn signed_url(&self, key: &str, ttl: Duration) -> Result<String, ArtifactUrlError>;

    /// A signed URL for `key` **only if the object is actually fetchable** —
    /// the gated form a public link (e.g. Slack) must use. Distinct from
    /// `signed_url`, which signs unconditionally: a failed S3 upload still
    /// leaves the local mirror (Decision 0003), so `exists` (local-or-remote)
    /// can't tell "in the bucket" from "local only". This verifies *bucket*
    /// presence first. Default `None` — a store with no shareable URL
    /// (`LocalFsStore`) never offers a link.
    async fn signed_url_if_fetchable(&self, key: &str, ttl: Duration) -> Option<String> {
        let _ = (key, ttl);
        None
    }

    /// Whether `key` exists in the store (local mirror **or** the bucket).
    async fn exists(&self, key: &str) -> bool;

    /// Mint a worker-scoped exact-key PUT. Local storage deliberately cannot
    /// implement this: fleet mode requires a remotely reachable object store.
    fn fleet_put_grant(
        &self,
        _key: &str,
        _size: u64,
        _sha256: &str,
        _ttl: Duration,
    ) -> Result<ArtifactGrantResponse, ArtifactUrlError> {
        Err(ArtifactUrlError::Unsupported)
    }

    fn fleet_get_grant(
        &self,
        _key: &str,
        _ttl: Duration,
    ) -> Result<ArtifactGrantResponse, ArtifactUrlError> {
        Err(ArtifactUrlError::Unsupported)
    }

    /// Verify the immutable metadata signed into a fleet upload grant.
    async fn verify_fleet_upload(&self, _artifact: &ArtifactDescriptor) -> bool {
        false
    }

    async fn promote_fleet_upload(&self, _artifact: &ArtifactDescriptor) -> bool {
        false
    }

    /// Delete unaccepted attempt staging. Accepted result objects are never
    /// passed to this operation.
    async fn delete_fleet_staging(&self, _key: &str) -> bool {
        false
    }
}

/// Restricts the full orchestrator-owned store to the operations execution is
/// allowed to perform.
#[cfg(test)]
struct ExecutionArtifactSink {
    store: Arc<dyn ArtifactStore>,
}

#[async_trait::async_trait]
#[cfg(test)]
impl ArtifactSink for ExecutionArtifactSink {
    async fn put(&self, key: &str, src: &Path) -> Option<u64> {
        self.store.put(key, src).await
    }

    async fn put_local_only(&self, key: &str, src: &Path) -> Option<u64> {
        self.store
            .put_local_only(key, src)
            .await
    }

    async fn get(&self, key: &str) -> std::io::Result<PathBuf> {
        self.store.get(key).await
    }

    fn job_dir(&self, job_id: &str) -> PathBuf {
        self.store.job_dir(job_id)
    }
}

#[cfg(test)]
pub fn execution_sink(store: Arc<dyn ArtifactStore>) -> Arc<dyn ArtifactSink> {
    Arc::new(ExecutionArtifactSink { store })
}

/// Execution-owned artifact-store configuration. Process composition projects
/// the aggregate daemon configuration into this type before constructing the
/// movable execution dependencies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactStoreConfig {
    pub local_root: PathBuf,
    pub backend: ArtifactStoreBackend,
}

impl ArtifactStoreConfig {
    pub fn local(local_root: PathBuf) -> Self {
        Self {
            local_root,
            backend: ArtifactStoreBackend::Local,
        }
    }

    pub fn s3(local_root: PathBuf, config: S3StoreConfig) -> Self {
        Self {
            local_root,
            backend: ArtifactStoreBackend::S3(config),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArtifactStoreBackend {
    Local,
    S3(S3StoreConfig),
}

/// S3-compatible endpoint settings owned by the artifact implementation rather
/// than the aggregate daemon configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3StoreConfig {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
}

/// Pick the configured [`LocalFsStore`] or [`S3Store`]. Fails fast on a bad S3
/// endpoint, so process composition calls this once at startup and shares the
/// resulting dependency with execution and reporting.
pub fn build_store(config: &ArtifactStoreConfig) -> anyhow::Result<Arc<dyn ArtifactStore>> {
    match &config.backend {
        ArtifactStoreBackend::Local => Ok(Arc::new(LocalFsStore::new(config.local_root.clone()))),
        ArtifactStoreBackend::S3(s3) => {
            let http = reqwest::Client::builder()
                .connect_timeout(S3_CONNECT_TIMEOUT)
                .read_timeout(S3_READ_TIMEOUT)
                .build()
                .context("building the HTTP client for the S3 artifact store")?;
            Ok(Arc::new(S3Store::new(config.local_root.clone(), s3, http)?))
        }
    }
}

/// Like [`build_store`] but never fails: on an init error it logs and degrades
/// to a local-only store so tests and recovery paths still archive a breadcrumb
/// (Decision 0003). Production composition uses strict [`build_store`] once.
#[cfg(test)]
pub fn build_store_or_local(config: &ArtifactStoreConfig) -> Arc<dyn ArtifactStore> {
    build_store(config).unwrap_or_else(|e| {
        tracing::error!(error = %e, "artifact store init failed; falling back to local-only archiving");
        Arc::new(LocalFsStore::new(config.local_root.clone()))
    })
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

    /// Resolve `key` to a path **under the root**, rejecting anything that
    /// could escape it: absolute, `..`/`.`/prefix components, or empty. Keys
    /// flow from persisted run-summary data, so this is a security boundary,
    /// not just hygiene. `None` when the key is unsafe. Shared by [`S3Store`]
    /// (same module) to bound its download/sign paths too.
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

#[async_trait::async_trait]
impl ArtifactStore for LocalFsStore {
    async fn put(&self, key: &str, src: &Path) -> Option<u64> {
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

    async fn get(&self, key: &str) -> std::io::Result<PathBuf> {
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

    fn job_dir(&self, job_id: &str) -> PathBuf {
        self.root.join(job_id)
    }

    /// Local FS can't mint an externally-usable URL (Decision 0001).
    fn signed_url(&self, _key: &str, _ttl: Duration) -> Result<String, ArtifactUrlError> {
        Err(ArtifactUrlError::Unsupported)
    }

    async fn exists(&self, key: &str) -> bool {
        self.checked_path(key)
            .map(|p| p.exists())
            .unwrap_or(false)
    }
}

/// S3-compatible store (Hetzner / any S3 endpoint), **local-first**: a
/// [`LocalFsStore`] mirror holds the `archive_dir` breadcrumb and the retained
/// copy (Decision 0003), with S3 as the durable, fetchable backing. Requests
/// are driven by presigned URLs (`rusty-s3` signs, `reqwest` executes), so the
/// store carries no long-lived auth state beyond the credentials.
pub struct S3Store {
    /// The local mirror — `archive_dir`, the put breadcrumb, and the `get`
    /// fast-path / retained copy when an upload fails.
    local: LocalFsStore,
    bucket: Bucket,
    credentials: Credentials,
    http: reqwest::Client,
}

impl S3Store {
    pub fn new(
        local_root: PathBuf,
        cfg: &S3StoreConfig,
        http: reqwest::Client,
    ) -> anyhow::Result<Self> {
        let endpoint = url::Url::parse(&cfg.endpoint)
            .with_context(|| format!("[artifacts] invalid S3 endpoint: {}", cfg.endpoint))?;
        // Path-style addressing is the portable choice for custom endpoints
        // (Hetzner / MinIO), avoiding bucket-as-subdomain DNS requirements.
        let bucket = Bucket::new(endpoint, UrlStyle::Path, cfg.bucket.clone(), cfg.region.clone())
            .context("[artifacts] building the S3 bucket")?;
        let credentials =
            Credentials::new(cfg.access_key_id.clone(), cfg.secret_access_key.clone());
        Ok(Self {
            local: LocalFsStore::new(local_root),
            bucket,
            credentials,
            http,
        })
    }

    /// Upload the local copy of `key` to S3. Errors are surfaced to the caller,
    /// which (per Decision 0003) logs but does not fail the job. The body is
    /// **streamed** from disk (a run's SQLite db can be multi-GB), never read
    /// fully into memory.
    async fn upload(&self, key: &str, src: &Path) -> anyhow::Result<()> {
        let file = tokio::fs::File::open(src)
            .await
            .with_context(|| format!("opening {} for S3 upload", src.display()))?;
        let len = file
            .metadata()
            .await
            .with_context(|| format!("stat {} for S3 upload", src.display()))?
            .len();
        let url = self
            .bucket
            .put_object(Some(&self.credentials), key)
            .sign(INTERNAL_SIGN_TTL);
        // Stream the file as the request body. `Content-Length` is set
        // explicitly so S3 frames a single PutObject (it rejects chunked
        // transfer-encoding for a plain PUT); the presigned URL signs only
        // `host`, so an unsigned length header is fine.
        let body = reqwest::Body::wrap_stream(tokio_util::io::ReaderStream::new(file));
        let resp = self
            .http
            .put(url)
            .header(reqwest::header::CONTENT_LENGTH, len)
            .body(body)
            .send()
            .await
            .context("S3 PUT request failed")?;
        if !resp.status().is_success() {
            anyhow::bail!("S3 PUT {key} returned HTTP {}", resp.status());
        }
        Ok(())
    }

    /// Bucket-side `HEAD` for `key` — the authoritative "is the object really
    /// in S3" check, with **no** local-mirror fast path. `exists` is true
    /// whenever the mirror has it (so a failed upload still shows local);
    /// this is what gates a public download link.
    async fn head_in_bucket(&self, key: &str) -> bool {
        if self
            .local
            .checked_path(key)
            .is_none()
        {
            return false; // unsafe key — never construct a URL for it
        }
        let url = self
            .bucket
            .head_object(Some(&self.credentials), key)
            .sign(INTERNAL_SIGN_TTL);
        match self
            .http
            .head(url)
            .send()
            .await
        {
            Ok(resp) => resp.status().is_success(),
            Err(_) => false,
        }
    }

    async fn fleet_object_matches(&self, key: &str, size: u64, sha256: &str) -> bool {
        if self
            .local
            .checked_path(key)
            .is_none()
        {
            return false;
        }
        let url = self
            .bucket
            .head_object(Some(&self.credentials), key)
            .sign(INTERNAL_SIGN_TTL);
        let Ok(response) = self
            .http
            .head(url)
            .send()
            .await
        else {
            return false;
        };
        if !response.status().is_success() {
            return false;
        }
        let size_matches = response
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            == Some(size);
        let digest_matches = response
            .headers()
            .get("x-amz-meta-sbgh-sha256")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.eq_ignore_ascii_case(sha256));
        size_matches && digest_matches
    }
}

#[async_trait::async_trait]
impl ArtifactStore for S3Store {
    async fn put(&self, key: &str, src: &Path) -> Option<u64> {
        // Local mirror first — the breadcrumb + retained copy, and the
        // authoritative success signal. A rejected/unsafe key or missing source
        // stops here (and never reaches S3).
        let size = self
            .local
            .put(key, src)
            .await?;
        // Best-effort S3 upload; a failure is logged, not fatal (Decision
        // 0003) — the local copy is retained and the upload is retryable.
        if let Err(e) = self.upload(key, src).await {
            tracing::warn!(error = %e, key, "artifact store: S3 upload failed; local copy retained");
        }
        Some(size)
    }

    /// Local mirror only — no S3 upload (see the trait default's rationale).
    /// For the large, non-portable run binary: a host-side forensic copy
    /// that's kept out of object storage.
    async fn put_local_only(&self, key: &str, src: &Path) -> Option<u64> {
        self.local.put(key, src).await
    }

    async fn get(&self, key: &str) -> std::io::Result<PathBuf> {
        // Fast path: present in the local mirror (always true right after a
        // run; may be reaped for an old run, hence the S3 fallback).
        if let Ok(path) = self.local.get(key).await {
            return Ok(path);
        }
        // Bound the download to the mirror root — never write outside it.
        let dest = self
            .local
            .checked_path(key)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("unsafe artifact key: {key}"),
                )
            })?;
        let url = self
            .bucket
            .get_object(Some(&self.credentials), key)
            .sign(INTERNAL_SIGN_TTL);
        let resp = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|e| std::io::Error::other(format!("S3 GET {key}: {e}")))?;
        if !resp.status().is_success() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("artifact key not found in S3: {key} (HTTP {})", resp.status()),
            ));
        }
        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        // **Stream** the body to disk (objects can be multi-GB) via a sibling
        // per-download `.part` file, renamed on completion — an interrupted
        // download never leaves a truncated artifact at the real key path, and
        // concurrent misses for the same key don't collide on the temp file.
        let part = part_path(&dest, &uuid::Uuid::new_v4().to_string());
        let mut file = tokio::fs::File::create(&part).await?;
        let mut stream = resp.bytes_stream();
        let outcome = async {
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|e| {
                    std::io::Error::other(format!("reading S3 GET body for {key}: {e}"))
                })?;
                file.write_all(&chunk).await?;
            }
            file.flush().await
        }
        .await;
        if let Err(e) = outcome {
            // Best-effort cleanup of the partial file; surface the read/write error.
            let _ = tokio::fs::remove_file(&part).await;
            return Err(e);
        }
        tokio::fs::rename(&part, &dest).await?;
        Ok(dest)
    }

    fn job_dir(&self, job_id: &str) -> PathBuf {
        self.local.job_dir(job_id)
    }

    fn signed_url(&self, key: &str, ttl: Duration) -> Result<String, ArtifactUrlError> {
        // Reuse the local store's traversal guard so a crafted key can't be
        // signed into a path outside the run's namespace.
        if self
            .local
            .checked_path(key)
            .is_none()
        {
            return Err(ArtifactUrlError::Backend(format!("unsafe artifact key: {key}")));
        }
        let url = self
            .bucket
            .get_object(Some(&self.credentials), key)
            .sign(ttl);
        Ok(url.to_string())
    }

    async fn signed_url_if_fetchable(&self, key: &str, ttl: Duration) -> Option<String> {
        // The gate (v5 acceptance): only link an object actually in the bucket.
        // `head_in_bucket` (not `exists`) so a failed upload — local mirror
        // present, S3 object absent — yields no link rather than a dead one.
        if self.head_in_bucket(key).await { self.signed_url(key, ttl).ok() } else { None }
    }

    async fn exists(&self, key: &str) -> bool {
        self.local.exists(key).await || self.head_in_bucket(key).await
    }

    fn fleet_put_grant(
        &self,
        key: &str,
        size: u64,
        sha256: &str,
        ttl: Duration,
    ) -> Result<ArtifactGrantResponse, ArtifactUrlError> {
        if self
            .local
            .checked_path(key)
            .is_none()
        {
            return Err(ArtifactUrlError::Backend(format!("unsafe artifact key: {key}")));
        }
        let mut action = self
            .bucket
            .put_object(Some(&self.credentials), key);
        let size_string = size.to_string();
        let checksum = hex::decode(sha256)
            .map_err(|error| ArtifactUrlError::Backend(format!("invalid SHA-256: {error}")))?;
        let checksum = base64::engine::general_purpose::STANDARD.encode(checksum);
        action
            .headers_mut()
            .insert("content-length", size_string.clone());
        action
            .headers_mut()
            .insert("x-amz-checksum-sha256", checksum.clone());
        action
            .headers_mut()
            .insert("x-amz-meta-sbgh-sha256", sha256.to_ascii_lowercase());
        let url = action.sign(ttl);
        Ok(ArtifactGrantResponse {
            method: "PUT".into(),
            key: key.into(),
            url: url.to_string(),
            headers: vec![
                HeaderValue {
                    name: "content-length".into(),
                    value: size_string,
                },
                HeaderValue {
                    name: "x-amz-checksum-sha256".into(),
                    value: checksum,
                },
                HeaderValue {
                    name: "x-amz-meta-sbgh-sha256".into(),
                    value: sha256.to_ascii_lowercase(),
                },
            ],
            expires_at_ms: (chrono::Utc::now()
                + chrono::Duration::from_std(ttl).unwrap_or(chrono::Duration::minutes(5)))
            .timestamp_millis(),
        })
    }

    fn fleet_get_grant(
        &self,
        key: &str,
        ttl: Duration,
    ) -> Result<ArtifactGrantResponse, ArtifactUrlError> {
        if self
            .local
            .checked_path(key)
            .is_none()
        {
            return Err(ArtifactUrlError::Backend(format!("unsafe artifact key: {key}")));
        }
        let url = self
            .bucket
            .get_object(Some(&self.credentials), key)
            .sign(ttl);
        Ok(ArtifactGrantResponse {
            method: "GET".into(),
            key: key.into(),
            url: url.to_string(),
            headers: Vec::new(),
            expires_at_ms: (chrono::Utc::now()
                + chrono::Duration::from_std(ttl).unwrap_or(chrono::Duration::minutes(5)))
            .timestamp_millis(),
        })
    }

    async fn verify_fleet_upload(&self, artifact: &ArtifactDescriptor) -> bool {
        self.fleet_object_matches(&artifact.key, artifact.size, &artifact.sha256)
            .await
    }

    async fn promote_fleet_upload(&self, artifact: &ArtifactDescriptor) -> bool {
        let staging_key = &artifact.key;
        let logical_key = &artifact.logical_key;
        if self
            .local
            .checked_path(staging_key)
            .is_none()
            || self
                .local
                .checked_path(logical_key)
                .is_none()
        {
            return false;
        }
        if !self
            .head_in_bucket(staging_key)
            .await
        {
            return self
                .fleet_object_matches(logical_key, artifact.size, &artifact.sha256)
                .await;
        }
        let copy_source = format!("/{}/{}", self.bucket.name(), staging_key);
        let mut action = self
            .bucket
            .put_object(Some(&self.credentials), logical_key);
        action
            .headers_mut()
            .insert("x-amz-copy-source", copy_source.clone());
        let url = action.sign(INTERNAL_SIGN_TTL);
        let copied = self
            .http
            .put(url)
            .header("x-amz-copy-source", copy_source)
            .send()
            .await
            .is_ok_and(|response| response.status().is_success());
        if !copied {
            return false;
        }
        if !self
            .fleet_object_matches(logical_key, artifact.size, &artifact.sha256)
            .await
        {
            return false;
        }
        self.delete_fleet_staging(staging_key)
            .await
    }

    async fn delete_fleet_staging(&self, key: &str) -> bool {
        if self
            .local
            .checked_path(key)
            .is_none()
        {
            return false;
        }
        let url = self
            .bucket
            .delete_object(Some(&self.credentials), key)
            .sign(INTERNAL_SIGN_TTL);
        self.http
            .delete(url)
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
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
    fn submission_artifact_key_is_submission_prefix_slash_relative() {
        assert_eq!(
            submission_artifact_key("submission1", "shared/stacks-bench.db"),
            "submission1/shared/stacks-bench.db"
        );
    }

    #[tokio::test]
    async fn put_then_get_round_trips_and_lands_at_root_slash_key() {
        let tmp = TempDir::new().unwrap();
        let s = store(&tmp);
        let src = tmp.path().join("run.json");
        std::fs::File::create(&src)
            .unwrap()
            .write_all(b"{\"ok\":true}")
            .unwrap();

        let key = artifact_key("job1", "run.json");
        let size = s.put(&key, &src).await;
        assert_eq!(size, Some(b"{\"ok\":true}".len() as u64));

        // The key resolves to today's exact `<root>/<job_id>/<relative>` path.
        let got = s.get(&key).await.unwrap();
        assert_eq!(
            got,
            tmp.path()
                .join("archive/job1/run.json")
        );
        assert_eq!(std::fs::read(&got).unwrap(), b"{\"ok\":true}");
        assert!(s.exists(&key).await);
    }

    #[tokio::test]
    async fn put_missing_src_returns_none() {
        let tmp = TempDir::new().unwrap();
        let s = store(&tmp);
        assert_eq!(
            s.put(&artifact_key("job1", "run.json"), &tmp.path().join("nope"))
                .await,
            None
        );
    }

    #[tokio::test]
    async fn get_absent_key_is_not_found() {
        let tmp = TempDir::new().unwrap();
        let s = store(&tmp);
        let err = s
            .get(&artifact_key("job1", "run.json"))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
        assert!(
            !s.exists(&artifact_key("job1", "run.json"))
                .await
        );
    }

    #[test]
    fn signed_url_is_unsupported_for_local() {
        let tmp = TempDir::new().unwrap();
        let err = store(&tmp)
            .signed_url(&artifact_key("job1", "run.json"), Duration::from_secs(60))
            .unwrap_err();
        assert!(matches!(err, ArtifactUrlError::Unsupported));
    }

    #[tokio::test]
    async fn unsafe_keys_cannot_escape_the_root() {
        let tmp = TempDir::new().unwrap();
        let s = store(&tmp);
        let src = tmp.path().join("payload");
        std::fs::write(&src, b"x").unwrap();
        for bad in ["", "..", "../escape", "job1/../../escape", "/abs/escape", "./x"] {
            assert_eq!(s.put(bad, &src).await, None, "put must reject {bad:?}");
            assert!(s.get(bad).await.is_err(), "get must reject {bad:?}");
            assert!(!s.exists(bad).await, "exists must be false for {bad:?}");
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

    // ─── S3Store (no live endpoint needed) ───

    fn s3_store(tmp: &TempDir, endpoint: &str) -> S3Store {
        let cfg = S3StoreConfig {
            endpoint: endpoint.to_string(),
            bucket: "sbgh-artifacts".into(),
            region: "fsn1".into(),
            access_key_id: "AKIAEXAMPLE".into(),
            secret_access_key: "s3cr3t".into(),
        };
        // Short timeout so the "unreachable endpoint" tests fail fast.
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        S3Store::new(tmp.path().join("archive"), &cfg, http).unwrap()
    }

    #[test]
    fn s3_signed_url_is_a_presigned_get() {
        // Pure signing — no network. The URL targets the bucket+key and carries
        // a SigV4 signature.
        let tmp = TempDir::new().unwrap();
        let s = s3_store(&tmp, "https://fsn1.example.com");
        let url = s
            .signed_url("job1/run.json", Duration::from_secs(900))
            .unwrap();
        assert!(url.contains("sbgh-artifacts"), "url targets the bucket: {url}");
        assert!(url.contains("job1/run.json"), "url targets the key: {url}");
        assert!(url.contains("X-Amz-Signature"), "url is presigned: {url}");
    }

    #[test]
    fn s3_signed_url_rejects_unsafe_keys() {
        let tmp = TempDir::new().unwrap();
        let s = s3_store(&tmp, "https://fsn1.example.com");
        let err = s
            .signed_url("../escape", Duration::from_secs(60))
            .unwrap_err();
        assert!(matches!(err, ArtifactUrlError::Backend(_)));
    }

    #[test]
    fn fleet_put_grant_is_exact_key_size_and_checksum_bound() {
        let tmp = TempDir::new().unwrap();
        let s = s3_store(&tmp, "https://fsn1.example.com");
        let key = "staging/attempt/nonce/job/run.json";
        let digest = "00".repeat(32);
        let grant = s
            .fleet_put_grant(key, 17, &digest, Duration::from_secs(300))
            .unwrap();
        assert_eq!(grant.method, "PUT");
        assert_eq!(grant.key, key);
        assert!(
            grant
                .url
                .contains("staging/attempt/nonce/job/run.json")
        );
        assert!(
            grant
                .url
                .contains("X-Amz-Signature")
        );
        assert_eq!(
            grant
                .headers
                .iter()
                .find(|header| header.name == "content-length")
                .map(|header| header.value.as_str()),
            Some("17")
        );
        assert_eq!(
            grant
                .headers
                .iter()
                .find(|header| header.name == "x-amz-meta-sbgh-sha256")
                .map(|header| header.value.as_str()),
            Some(digest.as_str())
        );
        assert_eq!(
            grant
                .headers
                .iter()
                .find(|header| header.name == "x-amz-checksum-sha256")
                .map(|header| header.value.as_str()),
            Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
        );
    }

    #[test]
    fn local_store_never_mints_fleet_credentials() {
        let tmp = TempDir::new().unwrap();
        assert!(matches!(
            store(&tmp).fleet_put_grant(
                "staging/attempt/object",
                1,
                &"00".repeat(32),
                Duration::from_secs(60),
            ),
            Err(ArtifactUrlError::Unsupported)
        ));
    }

    /// Decision 0003: an S3 upload failure must NOT fail the put — the local
    /// mirror is written + retained, and `get` still serves it. Pointed at an
    /// unreachable endpoint so the upload errors without a mock server.
    #[tokio::test]
    async fn s3_put_succeeds_locally_when_upload_fails() {
        let tmp = TempDir::new().unwrap();
        let s = s3_store(&tmp, "http://127.0.0.1:9"); // discard port → refused
        let src = tmp.path().join("run.json");
        std::fs::write(&src, b"{\"ok\":true}").unwrap();

        let key = artifact_key("job1", "run.json");
        // Upload fails, but the local mirror write succeeds → Some(size).
        let size = s.put(&key, &src).await;
        assert_eq!(size, Some(b"{\"ok\":true}".len() as u64));

        // The retained local copy is still fetchable via the mirror fast-path.
        let got = s.get(&key).await.unwrap();
        assert_eq!(std::fs::read(&got).unwrap(), b"{\"ok\":true}");
        assert_eq!(
            got,
            tmp.path()
                .join("archive/job1/run.json")
        );
    }

    #[test]
    fn s3_job_dir_is_the_local_mirror() {
        let tmp = TempDir::new().unwrap();
        let s = s3_store(&tmp, "https://fsn1.example.com");
        assert_eq!(
            s.job_dir("abc-123"),
            tmp.path()
                .join("archive/abc-123")
        );
    }
}
