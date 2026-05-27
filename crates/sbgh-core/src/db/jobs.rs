//! `JobStore` trait — the boundary between business logic and persistence.
//!
//! The production impl is [`super::PostgresJobStore`]. The `testing`
//! feature also exposes an in-memory impl ([`super::InMemoryJobStore`]) so
//! handler/orchestrator logic can be unit-tested without a real database.

use async_trait::async_trait;
use uuid::Uuid;

use crate::Result;
use crate::models::{Job, NewJob};

#[async_trait]
pub trait JobStore: Send + Sync + 'static {
    /// Insert a queued job. Returns its id, or `None` if a job with the same
    /// `github_delivery_id` already exists (idempotent — the caller should
    /// treat this as success). Called by the handler; needs only INSERT.
    async fn enqueue(&self, new: &NewJob) -> Result<Option<Uuid>>;

    /// Atomically claim the next queued job and transition it to `running`.
    async fn claim_next(&self) -> Result<Option<Job>>;

    /// Transition a running job to `completed` with a result blob.
    async fn complete(&self, id: Uuid, result: serde_json::Value) -> Result<()>;

    /// Transition a running job to `failed`. `error` is the short human
    /// message; `summary` is the same forensics blob shape we'd store on a
    /// successful job (last phase, console tail, etc.) and is what makes a
    /// failed job actually debuggable after teardown.
    async fn fail(&self, id: Uuid, error: &str, summary: Option<serde_json::Value>) -> Result<()>;

    /// Record the GitHub comment id we're using to report this job's
    /// progress. Called by the orchestrator after it posts the initial
    /// "starting" comment on job pickup.
    async fn set_comment_id(&self, id: Uuid, comment_id: i64) -> Result<()>;

    /// Update the resolved head SHA. The handler enqueues without a SHA
    /// (it doesn't hold GitHub App credentials, so it can't call the PR
    /// API); the orchestrator resolves the SHA at pickup time and writes
    /// it back here before running the benchmark.
    async fn set_head_sha(&self, id: Uuid, head_sha: &str) -> Result<()>;
}
