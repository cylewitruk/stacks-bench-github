//! Slice 8 data layer for the new-schema `job` table family.
//!
//! Naming: the trait + impls carry a `JobV2` / `V2` marker until
//! slice 12 removes the legacy `JobStore` (which lives in
//! `db::jobs`). Slice 12 renames `JobV2Store` → `JobStore` after
//! the legacy is gone.
//!
//! Surface (per the pre-Phase-2 checkpoint):
//!
//! Lifecycle methods:
//!   - `insert_job` — INSERTs a new row at `queued`; tests pin the queued-state
//!     invariant (both `claim_token` and `claimed_at` are NULL on insert).
//!   - `claim_next_queued` — `FOR UPDATE SKIP LOCKED` against the
//!     `job_queued_idx` partial index; transitions queued → claimed and sets
//!     `(claim_token, claimed_at)`.
//!   - `mark_running` — transitions claimed → running; PRESERVES claim handoff
//!     columns as audit (which orchestrator instance won the claim).
//!   - `mark_terminal` — transitions running → completed/failed/ cancelled;
//!     preserves audit columns.
//!   - `sweep_stuck_claims` — resets `claimed` rows whose `claimed_at` exceeds
//!     the lease back to `queued` AND clears `claim_token` + `claimed_at`
//!     (matches the inbox sweep).
//!
//! Outcome companions (write-once):
//!   - `insert_event` — append-only timeline; slice 9 writes the queued event
//!     from the processor, slice 10 writes the execution-phase events from the
//!     orchestrator.
//!   - `record_metric` — promoted bench metrics.
//!   - `record_result` — raw run.json + archive path.
//!
//! Subject relations:
//!   - `link_to_webhook` — webhook→job ingest link.
//!   - `link_to_user` — triggering-user ownership.
//!   - `link_to_pull_request` — PR association.
//!
//! `lookup_job` — slice 9 + slice 10 need this for read paths.

use async_trait::async_trait;
use chrono::Duration;
use uuid::Uuid;

use crate::Result;
use crate::models::{
    GithubPullRequestJob, GithubUserJob, GithubWebhookJob, JobCreationRequest, JobEvent, JobMetric,
    JobResult, JobV2, NewJobEvent, NewJobV2, ResolvedCommit, TerminalJobStatus,
};

/// Slice 8 (post-review): typed bundle returned by
/// `create_job_with_links` so callers (slice 9) can chain follow-up
/// operations against the freshly-created job + links + queued event
/// without an extra lookup.
#[derive(Debug, Clone)]
pub struct CreatedJob {
    pub job: JobV2,
    pub webhook_link: GithubWebhookJob,
    pub user_link: Option<GithubUserJob>,
    pub pull_request_link: Option<GithubPullRequestJob>,
    pub queued_event: JobEvent,
}

/// Slice 9: outcome of `create_job_with_links`, distinguishing a fresh
/// creation from an idempotent no-op when the webhook already has a job.
///
/// Job creation is the first non-idempotent side effect in the classify
/// pipeline; the inbox is at-least-once (a webhook can be reprocessed
/// after a failed `complete()` or a swept claim lease). The
/// `github_webhook_job` `UNIQUE (github_webhook_id)` constraint makes
/// "one job per webhook" structural, and this outcome lets the caller
/// treat a retry that hit the constraint as success without minting a
/// duplicate. Mirrors the `IngestOutcome::Duplicate` pattern.
#[derive(Debug, Clone)]
pub enum JobCreationOutcome {
    /// A new job (+ links + queued event) was created this call.
    /// Boxed because `CreatedJob` dwarfs the unit `AlreadyEnqueued`
    /// variant (clippy `large_enum_variant`).
    Created(Box<CreatedJob>),
    /// A job already existed for this webhook (idempotent retry). No
    /// rows were written; the prior attempt's job stands.
    AlreadyEnqueued,
}

#[async_trait]
pub trait JobV2Store: Send + Sync + 'static {
    async fn insert_job(&self, new: &NewJobV2) -> Result<JobV2>;

    /// Slice 8 (post-review): atomic job-creation boundary. Inserts the
    /// `job` row + webhook→job link + (optional) user → job + (optional)
    /// PR → job + the queued `job_event` in one transaction. A FK or
    /// UNIQUE failure on any of the link inserts ROLLs BACK the entire
    /// creation — no partial job rows.
    ///
    /// This is the path slice 9 wires from the processor. Slice 8
    /// keeps the individual `insert_job` / `link_to_*` / `insert_event`
    /// methods on the trait for read-side flexibility and stand-alone
    /// integration testing, but production writers MUST use
    /// `create_job_with_links` to honour the transactional invariant.
    ///
    /// Slice 9: idempotent on `github_webhook_id`. If a job already
    /// exists for this webhook (a reprocessed delivery after a failed
    /// `complete()` / swept lease), no rows are written and
    /// `JobCreationOutcome::AlreadyEnqueued` is returned — the
    /// `UNIQUE (github_webhook_id)` constraint makes this race-safe even
    /// against a concurrent re-claim.
    async fn create_job_with_links(
        &self,
        request: &JobCreationRequest,
    ) -> Result<JobCreationOutcome>;

    async fn lookup_job(&self, job_id: Uuid) -> Result<Option<JobV2>>;

    /// `FOR UPDATE SKIP LOCKED` on the queued partial index; transitions
    /// queued → claimed and stamps `(claim_token, claimed_at)`. Returns
    /// `Ok(None)` when the queue is empty.
    async fn claim_next_queued(&self, claim_token: Uuid) -> Result<Option<JobV2>>;

    /// claimed → running. Conditional on `(job_id, claim_token)` so a
    /// late writer whose lease was reclaimed by the sweep can't
    /// transition the row. Returns `Ok(false)` if the conditional
    /// match failed.
    ///
    /// Slice 8 (post-review): if the queue-time `git_commit_hash` was
    /// unresolved (branch tip at enqueue), the orchestrator resolves
    /// during the claim phase and passes `Some(ResolvedCommit)` here
    /// so the commit metadata + status transition land atomically
    /// under the same claim_token guard. Pass `None` if the commit was
    /// already concrete at enqueue time (push/tag triggers).
    async fn mark_running(
        &self,
        job_id: Uuid,
        claim_token: Uuid,
        resolved_commit: Option<ResolvedCommit>,
    ) -> Result<bool>;

    /// running → terminal_status. Conditional on `(job_id, claim_token)`
    /// (same stale-claim guard as `mark_running`). The narrowed
    /// `TerminalJobStatus` type prevents the post-review high
    /// finding where a caller bug could transition `running → queued`
    /// and violate the queued-state invariant.
    async fn mark_terminal(
        &self,
        job_id: Uuid,
        claim_token: Uuid,
        terminal_status: TerminalJobStatus,
    ) -> Result<bool>;

    /// Reset `claimed` rows whose `claimed_at` is older than NOW - lease
    /// back to `queued` and CLEAR `claim_token` + `claimed_at` (matches
    /// the inbox sweep's `claim_token` reset). Returns the recovered
    /// row count.
    async fn sweep_stuck_claims(&self, lease: Duration) -> Result<u64>;

    async fn insert_event(&self, new: &NewJobEvent) -> Result<JobEvent>;

    async fn record_metric(&self, metric: &JobMetric) -> Result<()>;

    async fn record_result(&self, result: &JobResult) -> Result<()>;

    async fn link_to_webhook(&self, webhook_id: i64, job_id: Uuid) -> Result<GithubWebhookJob>;

    async fn link_to_user(&self, user_id: i64, job_id: Uuid) -> Result<GithubUserJob>;

    async fn link_to_pull_request(
        &self,
        pull_request_id: i64,
        job_id: Uuid,
        triggering_comment_id: Option<i64>,
    ) -> Result<GithubPullRequestJob>;
}
