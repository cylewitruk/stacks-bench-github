//! In-memory `JobStore` for unit tests. Single Mutex serialises
//! access; mirrors the Postgres semantics for queued-state
//! invariants, claim handoff, stuck-claim sweep, and the
//! conditional `(id, claim_token)` writes.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, Ordering};

use async_trait::async_trait;
use chrono::{Duration, Utc};
use uuid::Uuid;

use crate::Result;
use crate::db::jobs::{CreatedJob, JobCompletion, JobCreationOutcome, JobFailure, JobStore};
use crate::models::{
    GithubPullRequestJob, GithubUserJob, GithubWebhookJob, Job, JobCreationRequest, JobEvent,
    JobEventKind, JobEventStatus, JobMetric, JobResult, JobStatus, NewJob, NewJobEvent,
    ResolvedCommit, TerminalJobStatus,
};

#[derive(Default)]
pub struct InMemoryJobStore {
    state: Mutex<State>,
    next_event_id: AtomicI64,
}

#[derive(Default)]
struct State {
    jobs: HashMap<Uuid, Job>,
    /// Insertion order — `claim_next_queued` walks in (created_at, id)
    /// order, so we sort on demand.
    insertion_order: Vec<Uuid>,
    events: Vec<JobEvent>,
    metrics: HashMap<Uuid, JobMetric>,
    results: HashMap<Uuid, JobResult>,
    webhook_links: Vec<GithubWebhookJob>,
    user_links: Vec<GithubUserJob>,
    pr_links: Vec<GithubPullRequestJob>,
}

impl InMemoryJobStore {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State::default()),
            next_event_id: AtomicI64::new(1),
        }
    }

    /// Test-only accessor: returns true iff a `github_webhook_job`
    /// link exists for the given job_id right now. Used by the
    /// concurrent-visibility regression test (slice 8 second-pass
    /// review fix) to assert that when a parallel `claim_next_queued`
    /// observed a job, the corresponding webhook link was also
    /// already visible — i.e. there was no partial-commit window.
    pub fn has_webhook_link_for_job(&self, job_id: Uuid) -> bool {
        self.state
            .lock()
            .unwrap()
            .webhook_links
            .iter()
            .any(|l| l.job_id == job_id)
    }

    /// Test-only accessor: snapshot of all `job` rows. Slice 9 handler
    /// unit tests assert how many jobs an accept path created and
    /// inspect their subject identity / commit fields.
    pub fn all_jobs(&self) -> Vec<Job> {
        self.state
            .lock()
            .unwrap()
            .jobs
            .values()
            .cloned()
            .collect()
    }

    /// Test-only accessor: snapshot of all `job_event` rows (slice 9
    /// asserts the queued event + its provenance detail).
    pub fn all_events(&self) -> Vec<JobEvent> {
        self.state
            .lock()
            .unwrap()
            .events
            .clone()
    }

    /// Test-only accessor: snapshot of all `github_user_job` links.
    pub fn user_links(&self) -> Vec<GithubUserJob> {
        self.state
            .lock()
            .unwrap()
            .user_links
            .clone()
    }

    /// Test-only accessor: snapshot of all `github_pull_request_job` links.
    pub fn pr_links(&self) -> Vec<GithubPullRequestJob> {
        self.state
            .lock()
            .unwrap()
            .pr_links
            .clone()
    }
}

#[async_trait]
impl JobStore for InMemoryJobStore {
    async fn insert_job(&self, new: &NewJob) -> Result<Job> {
        let now = Utc::now();
        let id = Uuid::new_v4();
        let job = Job {
            id,
            github_installation_id: new.github_installation_id,
            github_repo_id: new.github_repo_id,
            status: JobStatus::Queued,
            job_kind: new.job_kind,
            trigger_kind: new.trigger_kind,
            git_ref_kind: new.git_ref_kind,
            git_ref_display: new.git_ref_display.clone(),
            git_commit_hash: new.git_commit_hash.clone(),
            git_committed_at: new.git_committed_at,
            claim_token: None,
            claimed_at: None,
            created_at: now,
            updated_at: now,
        };
        let mut s = self.state.lock().unwrap();
        s.jobs.insert(id, job.clone());
        s.insertion_order.push(id);
        Ok(job)
    }

    async fn create_job_with_links(
        &self,
        request: &JobCreationRequest,
    ) -> Result<JobCreationOutcome> {
        // Post-second-review M1 fix: take the mutex ONCE for the entire
        // creation so concurrent in-memory tests can't observe (or
        // claim) a partially-created job between the sub-inserts.
        // Mirrors the all-or-nothing semantic of the Postgres single
        // transaction.
        //
        // Build all the rows locally first, THEN — under a single mutex
        // acquisition — re-check the slice-9 `UNIQUE (github_webhook_id)`
        // idempotency guard and commit every mutation. The check must
        // hold the SAME lock as the commit so a concurrent re-claim
        // can't slip a second job in between (mirrors the Postgres
        // `ON CONFLICT (github_webhook_id)` race-safety).
        let now = Utc::now();
        let job_id = Uuid::new_v4();

        // Build all the rows locally.
        let job = Job {
            id: job_id,
            github_installation_id: request
                .new_job
                .github_installation_id,
            github_repo_id: request.new_job.github_repo_id,
            status: JobStatus::Queued,
            job_kind: request.new_job.job_kind,
            trigger_kind: request.new_job.trigger_kind,
            git_ref_kind: request.new_job.git_ref_kind,
            git_ref_display: request
                .new_job
                .git_ref_display
                .clone(),
            git_commit_hash: request
                .new_job
                .git_commit_hash
                .clone(),
            git_committed_at: request
                .new_job
                .git_committed_at,
            claim_token: None,
            claimed_at: None,
            created_at: now,
            updated_at: now,
        };
        let webhook_link = GithubWebhookJob {
            github_webhook_id: request.github_webhook_id,
            job_id,
            created_at: now,
        };
        let user_link = request
            .triggering_user_id
            .map(|user_id| GithubUserJob {
                github_user_id: user_id,
                job_id,
                created_at: now,
            });
        let pull_request_link = request
            .pull_request_link
            .as_ref()
            .map(|pr| GithubPullRequestJob {
                job_id,
                github_pull_request_id: pr.github_pull_request_id,
                triggering_comment_id: pr.triggering_comment_id,
                created_at: now,
            });
        let queued_event = JobEvent {
            id: self
                .next_event_id
                .fetch_add(1, Ordering::SeqCst),
            job_id,
            event_kind: JobEventKind::Queued,
            event_status: JobEventStatus::Success,
            occurred_at: now,
            github_comment_id: None,
            github_check_run_id: None,
            github_check_run_url: None,
            remark: None,
            detail: request
                .queued_event_detail
                .clone()
                .map(sqlx::types::Json),
        };

        // Commit: single mutex acquisition for ALL mutations (and the
        // idempotency check). Nothing is visible to concurrent readers
        // until this block returns.
        let mut s = self.state.lock().unwrap();
        // Slice 9 idempotency guard — mirror Postgres
        // `UNIQUE (github_webhook_id)`: if this webhook already has a
        // job, this is a reprocessed delivery; write nothing.
        if s.webhook_links
            .iter()
            .any(|l| l.github_webhook_id == request.github_webhook_id)
        {
            return Ok(JobCreationOutcome::AlreadyEnqueued);
        }
        s.jobs
            .insert(job_id, job.clone());
        s.insertion_order.push(job_id);
        s.webhook_links
            .push(webhook_link.clone());
        if let Some(ref link) = user_link {
            s.user_links
                .push(link.clone());
        }
        if let Some(ref link) = pull_request_link {
            s.pr_links.push(link.clone());
        }
        s.events
            .push(queued_event.clone());

        Ok(JobCreationOutcome::Created(Box::new(CreatedJob {
            job,
            webhook_link,
            user_link,
            pull_request_link,
            queued_event,
        })))
    }

    async fn lookup_job(&self, job_id: Uuid) -> Result<Option<Job>> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .jobs
            .get(&job_id)
            .cloned())
    }

    async fn claim_next_queued(&self, claim_token: Uuid) -> Result<Option<Job>> {
        let mut s = self.state.lock().unwrap();
        // Walk in insertion order (FIFO; mirrors the Postgres index ordering).
        let pick = s
            .insertion_order
            .iter()
            .find(|id| {
                s.jobs
                    .get(id)
                    .is_some_and(|j| j.status == JobStatus::Queued)
            })
            .copied();
        let Some(id) = pick else {
            return Ok(None);
        };
        let job = s.jobs.get_mut(&id).unwrap();
        job.status = JobStatus::Claimed;
        job.claim_token = Some(claim_token);
        job.claimed_at = Some(Utc::now());
        job.updated_at = Utc::now();
        Ok(Some(job.clone()))
    }

    async fn mark_running(
        &self,
        job_id: Uuid,
        claim_token: Uuid,
        resolved_commit: Option<ResolvedCommit>,
    ) -> Result<bool> {
        let mut s = self.state.lock().unwrap();
        let Some(job) = s.jobs.get_mut(&job_id) else {
            return Ok(false);
        };
        if job.status != JobStatus::Claimed || job.claim_token != Some(claim_token) {
            return Ok(false);
        }
        job.status = JobStatus::Running;
        if let Some(rc) = resolved_commit {
            job.git_commit_hash = Some(rc.hash);
            // `None` committed_at leaves the existing value (mirrors the
            // Postgres COALESCE).
            if let Some(ts) = rc.committed_at {
                job.git_committed_at = Some(ts);
            }
        }
        job.updated_at = Utc::now();
        Ok(true)
    }

    async fn mark_terminal(
        &self,
        job_id: Uuid,
        claim_token: Uuid,
        terminal_status: TerminalJobStatus,
    ) -> Result<bool> {
        let mut s = self.state.lock().unwrap();
        let Some(job) = s.jobs.get_mut(&job_id) else {
            return Ok(false);
        };
        if job.status != JobStatus::Running || job.claim_token != Some(claim_token) {
            return Ok(false);
        }
        job.status = terminal_status.into();
        job.updated_at = Utc::now();
        Ok(true)
    }

    async fn sweep_stuck_claims(&self, lease: Duration) -> Result<u64> {
        let cutoff = Utc::now() - lease;
        let mut s = self.state.lock().unwrap();
        let mut recovered = 0u64;
        for job in s.jobs.values_mut() {
            if job.status == JobStatus::Claimed
                && job
                    .claimed_at
                    .map(|t| t < cutoff)
                    .unwrap_or(false)
            {
                job.status = JobStatus::Queued;
                job.claim_token = None;
                job.claimed_at = None;
                job.updated_at = Utc::now();
                recovered += 1;
            }
        }
        Ok(recovered)
    }

    async fn complete_job(&self, completion: &JobCompletion) -> Result<bool> {
        // Mirror the Postgres transaction under a single lock: guarded
        // running→completed, then result (+ optional metric) + completed
        // event, all-or-nothing. The status guard prevents a double
        // finish, so the write-once companions can't collide here.
        let mut s = self.state.lock().unwrap();
        match s.jobs.get(&completion.job_id) {
            Some(j)
                if j.status == JobStatus::Running
                    && j.claim_token == Some(completion.claim_token) => {}
            _ => return Ok(false),
        }
        let now = Utc::now();
        let event = JobEvent {
            id: self
                .next_event_id
                .fetch_add(1, Ordering::SeqCst),
            job_id: completion.job_id,
            event_kind: JobEventKind::Completed,
            event_status: JobEventStatus::Success,
            occurred_at: now,
            github_comment_id: None,
            github_check_run_id: None,
            github_check_run_url: None,
            remark: None,
            detail: completion
                .event_detail
                .clone()
                .map(sqlx::types::Json),
        };
        let job = s
            .jobs
            .get_mut(&completion.job_id)
            .unwrap();
        job.status = JobStatus::Completed;
        job.updated_at = now;
        s.results
            .insert(completion.result.job_id, completion.result.clone());
        if let Some(m) = completion.metric.as_ref() {
            s.metrics
                .insert(m.job_id, m.clone());
        }
        s.events.push(event);
        Ok(true)
    }

    async fn fail_job(&self, failure: &JobFailure) -> Result<bool> {
        // `fail_job` accepts `claimed` OR `running` (a job can fail
        // before it starts — preflight resolution / comment posting),
        // unlike `complete_job` which is running-only. This is what lets
        // a persistent preflight failure terminalize instead of looping
        // via the stuck-claim sweep.
        let mut s = self.state.lock().unwrap();
        match s.jobs.get(&failure.job_id) {
            Some(j)
                if matches!(j.status, JobStatus::Claimed | JobStatus::Running)
                    && j.claim_token == Some(failure.claim_token) => {}
            _ => return Ok(false),
        }
        let now = Utc::now();
        let event = JobEvent {
            id: self
                .next_event_id
                .fetch_add(1, Ordering::SeqCst),
            job_id: failure.job_id,
            event_kind: JobEventKind::Failed,
            event_status: JobEventStatus::Fail,
            occurred_at: now,
            github_comment_id: None,
            github_check_run_id: None,
            github_check_run_url: None,
            remark: Some(failure.remark.clone()),
            detail: failure
                .event_detail
                .clone()
                .map(sqlx::types::Json),
        };
        let job = s
            .jobs
            .get_mut(&failure.job_id)
            .unwrap();
        job.status = JobStatus::Failed;
        job.updated_at = now;
        if let Some(r) = failure.result.as_ref() {
            s.results
                .insert(r.job_id, r.clone());
        }
        s.events.push(event);
        Ok(true)
    }

    async fn cancel_job(&self, job_id: Uuid, claim_token: Uuid, remark: &str) -> Result<bool> {
        // Mirror `fail_job` (claimed OR running, claim-token guarded) but
        // transition to `cancelled` + a `cancelled` event, no forensics result.
        let mut s = self.state.lock().unwrap();
        match s.jobs.get(&job_id) {
            Some(j)
                if matches!(j.status, JobStatus::Claimed | JobStatus::Running)
                    && j.claim_token == Some(claim_token) => {}
            _ => return Ok(false),
        }
        let now = Utc::now();
        let event = JobEvent {
            id: self
                .next_event_id
                .fetch_add(1, Ordering::SeqCst),
            job_id,
            event_kind: JobEventKind::Cancelled,
            event_status: JobEventStatus::Fail,
            occurred_at: now,
            github_comment_id: None,
            github_check_run_id: None,
            github_check_run_url: None,
            remark: Some(remark.to_string()),
            detail: None,
        };
        let job = s
            .jobs
            .get_mut(&job_id)
            .unwrap();
        job.status = JobStatus::Cancelled;
        job.updated_at = now;
        s.events.push(event);
        Ok(true)
    }

    async fn running_job_ids(&self) -> Result<Vec<Uuid>> {
        let s = self.state.lock().unwrap();
        Ok(s.jobs
            .values()
            .filter(|j| j.status == JobStatus::Running)
            .map(|j| j.id)
            .collect())
    }

    async fn cancel_orphan(&self, job_id: Uuid, remark: &str) -> Result<bool> {
        // Mirror the Postgres path: unconditional running→cancelled (no claim
        // guard) + a `cancelled` event, idempotent on a re-run.
        let mut s = self.state.lock().unwrap();
        match s.jobs.get(&job_id) {
            Some(j) if j.status == JobStatus::Running => {}
            _ => return Ok(false),
        }
        let now = Utc::now();
        let event = JobEvent {
            id: self
                .next_event_id
                .fetch_add(1, Ordering::SeqCst),
            job_id,
            event_kind: JobEventKind::Cancelled,
            event_status: JobEventStatus::Fail,
            occurred_at: now,
            github_comment_id: None,
            github_check_run_id: None,
            github_check_run_url: None,
            remark: Some(remark.to_string()),
            detail: None,
        };
        let job = s
            .jobs
            .get_mut(&job_id)
            .unwrap();
        job.status = JobStatus::Cancelled;
        job.updated_at = now;
        s.events.push(event);
        Ok(true)
    }

    async fn queued_event(&self, job_id: Uuid) -> Result<Option<JobEvent>> {
        let s = self.state.lock().unwrap();
        Ok(s.events
            .iter()
            .find(|e| e.job_id == job_id && e.event_kind == JobEventKind::Queued)
            .cloned())
    }

    async fn pull_request_link(&self, job_id: Uuid) -> Result<Option<GithubPullRequestJob>> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .pr_links
            .iter()
            .find(|l| l.job_id == job_id)
            .cloned())
    }

    async fn latest_comment_id(&self, job_id: Uuid) -> Result<Option<i64>> {
        let s = self.state.lock().unwrap();
        Ok(s.events
            .iter()
            .filter(|e| {
                e.job_id == job_id
                    && e.github_comment_id.is_some()
                    && matches!(
                        e.event_kind,
                        JobEventKind::CommentPosted | JobEventKind::CommentUpdated
                    )
            })
            // Highest event id == most recent (monotonic counter).
            .max_by_key(|e| e.id)
            .and_then(|e| e.github_comment_id))
    }

    async fn latest_check_run(&self, job_id: Uuid) -> Result<Option<(i64, Option<String>)>> {
        let s = self.state.lock().unwrap();
        Ok(s.events
            .iter()
            .filter(|e| {
                e.job_id == job_id
                    && e.github_check_run_id
                        .is_some()
                    && matches!(e.event_kind, JobEventKind::CheckRunCreated)
            })
            .max_by_key(|e| e.id)
            .map(|e| {
                (
                    e.github_check_run_id
                        .unwrap_or_default(),
                    e.github_check_run_url.clone(),
                )
            }))
    }

    async fn insert_event(&self, new: &NewJobEvent) -> Result<JobEvent> {
        let row = JobEvent {
            id: self
                .next_event_id
                .fetch_add(1, Ordering::SeqCst),
            job_id: new.job_id,
            event_kind: new.event_kind,
            event_status: new.event_status,
            occurred_at: Utc::now(),
            github_comment_id: new.github_comment_id,
            github_check_run_id: new.github_check_run_id,
            github_check_run_url: new
                .github_check_run_url
                .clone(),
            remark: new.remark.clone(),
            detail: new
                .detail
                .clone()
                .map(sqlx::types::Json),
        };
        self.state
            .lock()
            .unwrap()
            .events
            .push(row.clone());
        Ok(row)
    }

    async fn record_metric(&self, metric: &JobMetric) -> Result<()> {
        // Slice 8 (post-review L1 fix): mirror the Postgres write-once
        // PK enforcement. The original impl silently overwrote on
        // duplicate, which could mask double-write bugs in later
        // tests that exercise the in-memory store.
        let mut s = self.state.lock().unwrap();
        if s.metrics
            .contains_key(&metric.job_id)
        {
            return Err(crate::Error::Other(anyhow::anyhow!(
                "job_metric write-once: duplicate row for job_id={}",
                metric.job_id
            )));
        }
        s.metrics
            .insert(metric.job_id, metric.clone());
        Ok(())
    }

    async fn record_result(&self, result: &JobResult) -> Result<()> {
        // Same write-once semantics as `record_metric`.
        let mut s = self.state.lock().unwrap();
        if s.results
            .contains_key(&result.job_id)
        {
            return Err(crate::Error::Other(anyhow::anyhow!(
                "job_result write-once: duplicate row for job_id={}",
                result.job_id
            )));
        }
        s.results
            .insert(result.job_id, result.clone());
        Ok(())
    }

    async fn link_to_webhook(&self, webhook_id: i64, job_id: Uuid) -> Result<GithubWebhookJob> {
        // Slice 8 (post-review L1 fix): mirror the Postgres
        // `UNIQUE (job_id)` on github_webhook_job. Postgres rejects
        // a second link for the same job; the in-memory mirror does
        // the same so the test surfaces the bug rather than silently
        // accumulating duplicate links. Slice 9 added
        // `UNIQUE (github_webhook_id)` — mirror that too.
        let mut s = self.state.lock().unwrap();
        if s.webhook_links
            .iter()
            .any(|l| l.job_id == job_id)
        {
            return Err(crate::Error::Other(anyhow::anyhow!(
                "github_webhook_job UNIQUE(job_id): duplicate link for job_id={job_id}"
            )));
        }
        if s.webhook_links
            .iter()
            .any(|l| l.github_webhook_id == webhook_id)
        {
            return Err(crate::Error::Other(anyhow::anyhow!(
                "github_webhook_job UNIQUE(github_webhook_id): duplicate link for \
                 webhook_id={webhook_id}"
            )));
        }
        let row = GithubWebhookJob {
            github_webhook_id: webhook_id,
            job_id,
            created_at: Utc::now(),
        };
        s.webhook_links
            .push(row.clone());
        Ok(row)
    }

    async fn link_to_user(&self, user_id: i64, job_id: Uuid) -> Result<GithubUserJob> {
        // Mirrors `UNIQUE (job_id)` on github_user_job.
        let mut s = self.state.lock().unwrap();
        if s.user_links
            .iter()
            .any(|l| l.job_id == job_id)
        {
            return Err(crate::Error::Other(anyhow::anyhow!(
                "github_user_job UNIQUE(job_id): duplicate link for job_id={job_id}"
            )));
        }
        let row = GithubUserJob {
            github_user_id: user_id,
            job_id,
            created_at: Utc::now(),
        };
        s.user_links.push(row.clone());
        Ok(row)
    }

    async fn link_to_pull_request(
        &self,
        pull_request_id: i64,
        job_id: Uuid,
        triggering_comment_id: Option<i64>,
    ) -> Result<GithubPullRequestJob> {
        // Mirrors `PRIMARY KEY (job_id)` on github_pull_request_job.
        let mut s = self.state.lock().unwrap();
        if s.pr_links
            .iter()
            .any(|l| l.job_id == job_id)
        {
            return Err(crate::Error::Other(anyhow::anyhow!(
                "github_pull_request_job PK(job_id): duplicate link for job_id={job_id}"
            )));
        }
        let row = GithubPullRequestJob {
            job_id,
            github_pull_request_id: pull_request_id,
            triggering_comment_id,
            created_at: Utc::now(),
        };
        s.pr_links.push(row.clone());
        Ok(row)
    }
}
