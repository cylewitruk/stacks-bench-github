//! Narrow worker-to-artifact-store port.

use std::path::{Path, PathBuf};

use async_trait::async_trait;

/// The staging/read operations an execution backend needs.
///
/// Accepted-terminal promotion, signed URLs, and consumer-facing artifact
/// resolution intentionally remain outside this contract.
#[async_trait]
pub trait ArtifactSink: Send + Sync {
    async fn put(&self, key: &str, src: &Path) -> Option<u64>;

    async fn put_local_only(&self, key: &str, src: &Path) -> Option<u64> {
        self.put(key, src).await
    }

    async fn get(&self, key: &str) -> std::io::Result<PathBuf>;

    fn job_dir(&self, job_id: &str) -> PathBuf;
}

/// Build the store key for a named artifact of a job.
pub fn artifact_key(job_id: &str, relative: &str) -> String {
    format!("{job_id}/{relative}")
}
