//! Data layer for the `job` table family.
//!
//! Surface:
//!
//! Lifecycle methods:
//!   - `insert_job` — INSERTs a new row at `queued`; tests pin the queued-state
//!     invariant (both `claim_token` and `claimed_at` are NULL on insert).
//!   - `claim_next_queued` — `FOR UPDATE SKIP LOCKED` against the
//!     `job_queued_idx` partial index; transitions queued → claimed and sets
//!     `(claim_token, claimed_at)`.
//!   - `mark_running` — transitions claimed → running; PRESERVES claim handoff
//!     columns as audit (which daemon instance won the claim).
//!   - `mark_terminal` — transitions running → completed/failed/ cancelled;
//!     preserves audit columns.
//!   - `sweep_stuck_claims` — resets `claimed` rows whose `claimed_at` exceeds
//!     the lease back to `queued` AND clears `claim_token` + `claimed_at`
//!     (matches the inbox sweep).
//!
//! Outcome companions (write-once):
//!   - `insert_event` — append-only timeline; slice 9 writes the queued event
//!     from the processor, slice 10 writes the execution-phase events from the
//!     daemon.
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
    GithubPullRequestJob, GithubUserJob, GithubWebhookJob, Job, JobCreationRequest, JobEvent,
    JobMetric, JobResult, NewJob, NewJobEvent, ResolvedCommit, TerminalJobStatus,
};

/// Slice 10: atomic "the run completed" write. Bundles the terminal
/// status transition + the write-once outcome companions + the terminal
/// timeline event so they all land in ONE transaction. Without the
/// transaction, a crash between (say) `record_result` and `mark_terminal`
/// would leave a half-finished job that the write-once `job_result` PK
/// then blocks from being re-finished on retry.
///
/// `metric` is `None` when `run.json` was missing/unparseable or didn't
/// carry the full promoted-metric set (the columns are NOT NULL) — the
/// result row + archive path still land so the run is debuggable.
#[derive(Debug, Clone)]
pub struct JobCompletion {
    pub job_id: uuid::Uuid,
    pub claim_token: Uuid,
    pub result: JobResult,
    pub metric: Option<JobMetric>,
    /// `completed` event provenance (forensics summary blob).
    pub event_detail: Option<serde_json::Value>,
}

/// Slice 10: atomic "the run failed" write. Mirrors [`JobCompletion`]
/// but transitions to `failed` and persists an optional forensics
/// `result` (archive path is recorded even on failure so operators can
/// find post-mortem artefacts). `remark` is the short human error stored
/// on the `failed` event.
#[derive(Debug, Clone)]
pub struct JobFailure {
    pub job_id: uuid::Uuid,
    pub claim_token: Uuid,
    pub result: Option<JobResult>,
    pub remark: String,
    pub event_detail: Option<serde_json::Value>,
}

/// Slice 8 (post-review): typed bundle returned by
/// `create_job_with_links` so callers (slice 9) can chain follow-up
/// operations against the freshly-created job + links + queued event
/// without an extra lookup.
#[derive(Debug, Clone)]
pub struct CreatedJob {
    pub job: Job,
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
pub trait JobStore: Send + Sync + 'static {
    async fn insert_job(&self, new: &NewJob) -> Result<Job>;

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

    async fn lookup_job(&self, job_id: Uuid) -> Result<Option<Job>>;

    /// `FOR UPDATE SKIP LOCKED` on the queued partial index; transitions
    /// queued → claimed and stamps `(claim_token, claimed_at)`. Returns
    /// `Ok(None)` when the queue is empty.
    async fn claim_next_queued(&self, claim_token: Uuid) -> Result<Option<Job>>;

    /// claimed → running. Conditional on `(job_id, claim_token)` so a
    /// late writer whose lease was reclaimed by the sweep can't
    /// transition the row. Returns `Ok(false)` if the conditional
    /// match failed.
    ///
    /// Slice 8 (post-review): if the queue-time `git_commit_hash` was
    /// unresolved (branch tip at enqueue), the daemon resolves
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

    /// Slice 10: atomic run-completion. In ONE transaction: transition
    /// `running → completed` (guarded by `(job_id, claim_token,
    /// status='running')`, same stale-claim guard as `mark_terminal`),
    /// insert `job_result`, optionally insert `job_metric`, and append the
    /// `completed` `job_event`. Returns `Ok(false)` if the guard didn't
    /// match (a stale claim raced the sweep) — nothing is written.
    async fn complete_job(&self, completion: &JobCompletion) -> Result<bool>;

    /// Slice 10: atomic run-failure. In ONE transaction: transition
    /// `running → failed` (same guard), optionally insert a forensics
    /// `job_result`, and append the `failed` `job_event`. Returns
    /// `Ok(false)` on a stale-claim guard miss.
    async fn fail_job(&self, failure: &JobFailure) -> Result<bool>;

    /// roadmap-v5 Phase 4C: atomic **cancellation** — a deliberately-stopped
    /// run (operator shutdown/abort), distinct from a failure. Guarded by
    /// `(job_id, claim_token, status IN ('claimed','running'))` like `fail_job`
    /// (an abort can land before *or* after the run started); transitions to
    /// `cancelled` and appends a `cancelled` `job_event` with `remark`. No
    /// forensics `job_result` (a cancelled run produced none). Returns
    /// `Ok(false)` on a stale-claim guard miss.
    async fn cancel_job(&self, job_id: Uuid, claim_token: Uuid, remark: &str) -> Result<bool>;

    /// Orphan recovery (roadmap-v5 Phase 4B-2): every job id currently in
    /// `running`. After a crash/restart these are necessarily orphans — a
    /// freshly-started daemon has no running jobs of its own — so the runner
    /// cleans each one's leaked VM and terminal-cancels the row at startup.
    async fn running_job_ids(&self) -> Result<Vec<Uuid>>;

    /// Orphan recovery (4B-2 + 4C): terminal-**cancel** a job stranded in
    /// `running` by a dead daemon, with NO claim-token guard. The daemon that
    /// claimed it is gone, and this runs at startup before the coordinator
    /// claims anything, so there's no live writer to race — an unconditional
    /// `running → cancelled` (plus a `cancelled` event carrying `remark`) is
    /// safe. A crash-orphan is re-triggerable, not a benchmark failure, so it's
    /// `cancelled` not `failed`. Returns `Ok(false)` if the row wasn't
    /// `running` (already recovered / gone), making a re-run after a
    /// mid-recovery crash idempotent.
    async fn cancel_orphan(&self, job_id: Uuid, remark: &str) -> Result<bool>;

    /// Slice 10: read the `queued` timeline event for a job (carries the
    /// trigger provenance + `bench_args` the daemon forwards to
    /// the run). `None` if the job has no queued event (shouldn't happen
    /// for jobs created via `create_job_with_links`).
    async fn queued_event(&self, job_id: Uuid) -> Result<Option<JobEvent>>;

    /// Slice 11: the PR-subject link for a job, if any. `pr_comment`
    /// jobs have one (so the daemon can post the PR comment);
    /// baseline jobs don't. Carries `github_pull_request_id` — resolve
    /// the PR number via `PullRequestStore::lookup_by_id`.
    async fn pull_request_link(&self, job_id: Uuid) -> Result<Option<GithubPullRequestJob>>;

    /// Slice 11: the most-recent GitHub comment id recorded on the job's
    /// timeline (a `comment_posted` / `comment_updated` event with a
    /// non-null `github_comment_id`). The daemon reads this on
    /// (re-)claim so a job reclaimed after the comment was already posted
    /// EDITS the existing comment instead of posting a duplicate.
    async fn latest_comment_id(&self, job_id: Uuid) -> Result<Option<i64>>;

    /// roadmap-v4: the most-recent Check Run recorded on the job's timeline
    /// (a `check_run_created` event), as `(check_run_id, html_url)`. Read on
    /// (re-)claim so a reclaimed job UPDATEs the existing check (and can
    /// rebuild the "started, see check" comment from the stored URL) rather
    /// than creating a duplicate.
    async fn latest_check_run(&self, job_id: Uuid) -> Result<Option<(i64, Option<String>)>>;

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
