//! Postgres-backed `JobV2Store`. Single-statement operations for
//! most paths; `claim_next_queued` is the lone multi-statement query
//! (an UPDATE wrapping a `SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1`)
//! so the claim is atomic without a transaction wrapper.

use async_trait::async_trait;
use chrono::Duration;
use uuid::Uuid;

use crate::Result;
use crate::db::Pool;
use crate::db::job_v2::{CreatedJob, JobCompletion, JobCreationOutcome, JobFailure, JobV2Store};
use crate::models::{
    GithubPullRequestJob, GithubUserJob, GithubWebhookJob, JobCreationRequest, JobEvent,
    JobEventKind, JobEventStatus, JobMetric, JobResult, JobStatus, JobV2, NewJobEvent, NewJobV2,
    ResolvedCommit, TerminalJobStatus,
};

#[derive(Clone)]
pub struct PostgresJobV2Store {
    pool: Pool,
}

impl PostgresJobV2Store {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl JobV2Store for PostgresJobV2Store {
    async fn insert_job(&self, new: &NewJobV2) -> Result<JobV2> {
        // status defaults to 'queued'; claim_token + claimed_at stay
        // NULL on insert (queued-state invariant).
        let row = sqlx::query_as::<_, JobV2>(
            r#"
            INSERT INTO job
                (github_installation_id, github_repo_id, job_kind, trigger_kind,
                 git_ref_kind, git_ref_display, git_commit_hash, git_committed_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING id, github_installation_id, github_repo_id, status, job_kind,
                      trigger_kind, git_ref_kind, git_ref_display, git_commit_hash,
                      git_committed_at, claim_token, claimed_at, created_at, updated_at
            "#,
        )
        .bind(new.github_installation_id)
        .bind(new.github_repo_id)
        .bind(new.job_kind)
        .bind(new.trigger_kind)
        .bind(new.git_ref_kind)
        .bind(&new.git_ref_display)
        .bind(&new.git_commit_hash)
        .bind(new.git_committed_at)
        .fetch_one(&self.pool)
        .await?;
        Ok(row)
    }

    async fn create_job_with_links(
        &self,
        request: &JobCreationRequest,
    ) -> Result<JobCreationOutcome> {
        // Single Postgres transaction: job insert → webhook link →
        // optional user link → optional PR link → queued event. Any FK
        // / UNIQUE / CHECK failure rolls back the entire creation.
        //
        // Slice 9 idempotency: the webhook-link insert uses
        // `ON CONFLICT (github_webhook_id) DO NOTHING`. If the webhook
        // already has a job (a reprocessed delivery), the link returns
        // no row — we roll the whole transaction back (discarding the
        // `job` we just inserted) and report `AlreadyEnqueued`.
        let mut tx = self.pool.begin().await?;

        let job: JobV2 = sqlx::query_as(
            r#"
            INSERT INTO job
                (github_installation_id, github_repo_id, job_kind, trigger_kind,
                 git_ref_kind, git_ref_display, git_commit_hash, git_committed_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING id, github_installation_id, github_repo_id, status, job_kind,
                      trigger_kind, git_ref_kind, git_ref_display, git_commit_hash,
                      git_committed_at, claim_token, claimed_at, created_at, updated_at
            "#,
        )
        .bind(
            request
                .new_job
                .github_installation_id,
        )
        .bind(request.new_job.github_repo_id)
        .bind(request.new_job.job_kind)
        .bind(request.new_job.trigger_kind)
        .bind(request.new_job.git_ref_kind)
        .bind(
            &request
                .new_job
                .git_ref_display,
        )
        .bind(
            &request
                .new_job
                .git_commit_hash,
        )
        .bind(
            request
                .new_job
                .git_committed_at,
        )
        .fetch_one(&mut *tx)
        .await?;

        let webhook_link: Option<GithubWebhookJob> = sqlx::query_as(
            "INSERT INTO github_webhook_job (github_webhook_id, job_id) VALUES ($1, $2)
             ON CONFLICT (github_webhook_id) DO NOTHING
             RETURNING github_webhook_id, job_id, created_at",
        )
        .bind(request.github_webhook_id)
        .bind(job.id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(webhook_link) = webhook_link else {
            // Webhook already has a job from a prior (possibly
            // interrupted) attempt — idempotent no-op. Roll back the
            // job we just inserted so it doesn't orphan.
            tx.rollback().await?;
            return Ok(JobCreationOutcome::AlreadyEnqueued);
        };

        let user_link = if let Some(user_id) = request.triggering_user_id {
            let row: GithubUserJob = sqlx::query_as(
                "INSERT INTO github_user_job (github_user_id, job_id) VALUES ($1, $2)
                 RETURNING github_user_id, job_id, created_at",
            )
            .bind(user_id)
            .bind(job.id)
            .fetch_one(&mut *tx)
            .await?;
            Some(row)
        } else {
            None
        };

        let pull_request_link = if let Some(ref pr) = request.pull_request_link {
            let row: GithubPullRequestJob = sqlx::query_as(
                "INSERT INTO github_pull_request_job
                     (job_id, github_pull_request_id, triggering_comment_id)
                 VALUES ($1, $2, $3)
                 RETURNING job_id, github_pull_request_id, triggering_comment_id, created_at",
            )
            .bind(job.id)
            .bind(pr.github_pull_request_id)
            .bind(pr.triggering_comment_id)
            .fetch_one(&mut *tx)
            .await?;
            Some(row)
        } else {
            None
        };

        let queued_event: JobEvent = sqlx::query_as(
            r#"
            INSERT INTO job_event
                (job_id, event_kind, event_status, github_comment_id, remark, detail)
            VALUES ($1, $2, $3, NULL, NULL, $4)
            RETURNING id, job_id, event_kind, event_status, occurred_at,
                      github_comment_id, remark, detail
            "#,
        )
        .bind(job.id)
        .bind(JobEventKind::Queued)
        .bind(JobEventStatus::Success)
        .bind(&request.queued_event_detail)
        .fetch_one(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(JobCreationOutcome::Created(Box::new(CreatedJob {
            job,
            webhook_link,
            user_link,
            pull_request_link,
            queued_event,
        })))
    }

    async fn lookup_job(&self, job_id: Uuid) -> Result<Option<JobV2>> {
        let row = sqlx::query_as::<_, JobV2>(
            r#"
            SELECT id, github_installation_id, github_repo_id, status, job_kind,
                   trigger_kind, git_ref_kind, git_ref_display, git_commit_hash,
                   git_committed_at, claim_token, claimed_at, created_at, updated_at
              FROM job
             WHERE id = $1
            "#,
        )
        .bind(job_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    async fn claim_next_queued(&self, claim_token: Uuid) -> Result<Option<JobV2>> {
        // FOR UPDATE SKIP LOCKED on the partial queued index; transitions
        // queued → claimed and stamps the claim handoff columns. Single
        // statement so the row-pick and transition are atomic without
        // an explicit transaction.
        let row = sqlx::query_as::<_, JobV2>(
            r#"
            UPDATE job
               SET status      = 'claimed',
                   claim_token = $1,
                   claimed_at  = NOW()
             WHERE id = (
                 SELECT id
                   FROM job
                  WHERE status = 'queued'
               ORDER BY created_at, id
                  FOR UPDATE SKIP LOCKED
                  LIMIT 1
             )
         RETURNING id, github_installation_id, github_repo_id, status, job_kind,
                   trigger_kind, git_ref_kind, git_ref_display, git_commit_hash,
                   git_committed_at, claim_token, claimed_at, created_at, updated_at
            "#,
        )
        .bind(claim_token)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    async fn mark_running(
        &self,
        job_id: Uuid,
        claim_token: Uuid,
        resolved_commit: Option<ResolvedCommit>,
    ) -> Result<bool> {
        // Conditional on (id, claim_token, status=claimed) so a stale
        // writer whose lease was reclaimed by the sweep can't
        // transition the row. claim_token + claimed_at are NOT cleared
        // — they record the winning claim as audit.
        //
        // Slice 8 (post-review): if the queue-time commit was
        // unresolved, the orchestrator passes `Some(ResolvedCommit)`
        // and `mark_running` writes the metadata atomically with the
        // status transition. `COALESCE` semantics on `None`: leave the
        // existing columns untouched.
        let result = sqlx::query(
            r#"
            UPDATE job
               SET status            = 'running',
                   git_commit_hash   = COALESCE($3, git_commit_hash),
                   git_committed_at  = COALESCE($4, git_committed_at)
             WHERE id = $1
               AND claim_token = $2
               AND status = 'claimed'
            "#,
        )
        .bind(job_id)
        .bind(claim_token)
        .bind(
            resolved_commit
                .as_ref()
                .map(|r| r.hash.clone()),
        )
        .bind(
            resolved_commit
                .as_ref()
                .and_then(|r| r.committed_at),
        )
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    async fn mark_terminal(
        &self,
        job_id: Uuid,
        claim_token: Uuid,
        terminal_status: TerminalJobStatus,
    ) -> Result<bool> {
        // Same stale-claim guard as mark_running. Predicate also
        // requires `status = 'running'` so we can't transition
        // directly from claimed → terminal (forces the claim → run →
        // terminal lifecycle). The narrowed `TerminalJobStatus` type
        // makes it impossible at compile time to pass Queued / Claimed
        // / Running here.
        let status: JobStatus = terminal_status.into();
        let result = sqlx::query(
            r#"
            UPDATE job
               SET status = $3
             WHERE id = $1
               AND claim_token = $2
               AND status = 'running'
            "#,
        )
        .bind(job_id)
        .bind(claim_token)
        .bind(status)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    async fn sweep_stuck_claims(&self, lease: Duration) -> Result<u64> {
        // `claimed` past the lease → `queued`, clearing claim handoff
        // columns. `make_interval(secs => $1)` parameterises the lease
        // duration cleanly. Same shape as the inbox sweep.
        let lease_seconds = lease.num_seconds();
        let result = sqlx::query(
            r#"
            UPDATE job
               SET status      = 'queued',
                   claim_token = NULL,
                   claimed_at  = NULL
             WHERE status = 'claimed'
               AND claimed_at < NOW() - make_interval(secs => $1)
            "#,
        )
        .bind(lease_seconds as f64)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    async fn complete_job(&self, completion: &JobCompletion) -> Result<bool> {
        // Single transaction: guarded running→completed, then the
        // write-once result (+ optional metric) + the completed event.
        // The guard (claim_token + status='running') makes a stale
        // finish racing the sweep a clean no-op (rolls back, returns
        // false) instead of corrupting a reclaimed row.
        let mut tx = self.pool.begin().await?;
        let updated = sqlx::query(
            "UPDATE job SET status = 'completed'
             WHERE id = $1 AND claim_token = $2 AND status = 'running'",
        )
        .bind(completion.job_id)
        .bind(completion.claim_token)
        .execute(&mut *tx)
        .await?;
        if updated.rows_affected() != 1 {
            tx.rollback().await?;
            return Ok(false);
        }

        let r = &completion.result;
        sqlx::query("INSERT INTO job_result (job_id, run_json, archive_dir) VALUES ($1, $2, $3)")
            .bind(r.job_id)
            .bind(&r.run_json)
            .bind(&r.archive_dir)
            .execute(&mut *tx)
            .await?;

        if let Some(m) = completion.metric.as_ref() {
            sqlx::query(
                r#"
                INSERT INTO job_metric
                    (job_id, envelope_duration_us, replay_duration_us, total_duration_us,
                     setup_duration_us, execution_duration_us, commit_duration_us,
                     clarity_runtime, transactions, read_length, write_length,
                     measured_blocks, warmup_blocks)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                "#,
            )
            .bind(m.job_id)
            .bind(m.envelope_duration_us)
            .bind(m.replay_duration_us)
            .bind(m.total_duration_us)
            .bind(m.setup_duration_us)
            .bind(m.execution_duration_us)
            .bind(m.commit_duration_us)
            .bind(m.clarity_runtime)
            .bind(m.transactions)
            .bind(m.read_length)
            .bind(m.write_length)
            .bind(m.measured_blocks)
            .bind(m.warmup_blocks)
            .execute(&mut *tx)
            .await?;
        }

        sqlx::query(
            "INSERT INTO job_event (job_id, event_kind, event_status, detail)
             VALUES ($1, $2, $3, $4)",
        )
        .bind(completion.job_id)
        .bind(JobEventKind::Completed)
        .bind(JobEventStatus::Success)
        .bind(&completion.event_detail)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(true)
    }

    async fn fail_job(&self, failure: &JobFailure) -> Result<bool> {
        // Unlike `complete_job` (running-only — you can't complete a job
        // that never ran), `fail_job` accepts `claimed` OR `running`: a
        // job can fail BEFORE it starts running (e.g. preflight: PR head
        // SHA / tag commit resolution or initial comment posting fails).
        // Terminalizing a `claimed` job is what keeps a persistent
        // preflight failure from looping forever via the stuck-claim
        // sweep (claimed → requeued → re-claimed → fails again → ...).
        let mut tx = self.pool.begin().await?;
        let updated = sqlx::query(
            "UPDATE job SET status = 'failed'
             WHERE id = $1 AND claim_token = $2 AND status IN ('claimed', 'running')",
        )
        .bind(failure.job_id)
        .bind(failure.claim_token)
        .execute(&mut *tx)
        .await?;
        if updated.rows_affected() != 1 {
            tx.rollback().await?;
            return Ok(false);
        }

        if let Some(r) = failure.result.as_ref() {
            sqlx::query(
                "INSERT INTO job_result (job_id, run_json, archive_dir) VALUES ($1, $2, $3)",
            )
            .bind(r.job_id)
            .bind(&r.run_json)
            .bind(&r.archive_dir)
            .execute(&mut *tx)
            .await?;
        }

        sqlx::query(
            "INSERT INTO job_event (job_id, event_kind, event_status, remark, detail)
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(failure.job_id)
        .bind(JobEventKind::Failed)
        .bind(JobEventStatus::Fail)
        .bind(&failure.remark)
        .bind(&failure.event_detail)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(true)
    }

    async fn queued_event(&self, job_id: Uuid) -> Result<Option<JobEvent>> {
        let row = sqlx::query_as::<_, JobEvent>(
            r#"
            SELECT id, job_id, event_kind, event_status, occurred_at,
                   github_comment_id, remark, detail
              FROM job_event
             WHERE job_id = $1 AND event_kind = 'queued'
          ORDER BY occurred_at, id
             LIMIT 1
            "#,
        )
        .bind(job_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    async fn pull_request_link(&self, job_id: Uuid) -> Result<Option<GithubPullRequestJob>> {
        let row = sqlx::query_as::<_, GithubPullRequestJob>(
            "SELECT job_id, github_pull_request_id, triggering_comment_id, created_at
               FROM github_pull_request_job
              WHERE job_id = $1",
        )
        .bind(job_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    async fn latest_comment_id(&self, job_id: Uuid) -> Result<Option<i64>> {
        // Most recent comment-bearing event for the job. The github
        // comment id is stable across edits, so MAX(occurred_at) picks
        // the live comment regardless of how many edit events exist.
        let id: Option<i64> = sqlx::query_scalar(
            "SELECT github_comment_id
               FROM job_event
              WHERE job_id = $1
                AND github_comment_id IS NOT NULL
                AND event_kind IN ('comment_posted', 'comment_updated')
           ORDER BY occurred_at DESC, id DESC
              LIMIT 1",
        )
        .bind(job_id)
        .fetch_optional(&self.pool)
        .await?
        .flatten();
        Ok(id)
    }

    async fn insert_event(&self, new: &NewJobEvent) -> Result<JobEvent> {
        let row = sqlx::query_as::<_, JobEvent>(
            r#"
            INSERT INTO job_event
                (job_id, event_kind, event_status, github_comment_id, remark, detail)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id, job_id, event_kind, event_status, occurred_at,
                      github_comment_id, remark, detail
            "#,
        )
        .bind(new.job_id)
        .bind(new.event_kind)
        .bind(new.event_status)
        .bind(new.github_comment_id)
        .bind(&new.remark)
        .bind(&new.detail)
        .fetch_one(&self.pool)
        .await?;
        Ok(row)
    }

    async fn record_metric(&self, metric: &JobMetric) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO job_metric
                (job_id, envelope_duration_us, replay_duration_us, total_duration_us,
                 setup_duration_us, execution_duration_us, commit_duration_us,
                 clarity_runtime, transactions, read_length, write_length,
                 measured_blocks, warmup_blocks)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            "#,
        )
        .bind(metric.job_id)
        .bind(metric.envelope_duration_us)
        .bind(metric.replay_duration_us)
        .bind(metric.total_duration_us)
        .bind(metric.setup_duration_us)
        .bind(metric.execution_duration_us)
        .bind(metric.commit_duration_us)
        .bind(metric.clarity_runtime)
        .bind(metric.transactions)
        .bind(metric.read_length)
        .bind(metric.write_length)
        .bind(metric.measured_blocks)
        .bind(metric.warmup_blocks)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn record_result(&self, result: &JobResult) -> Result<()> {
        sqlx::query("INSERT INTO job_result (job_id, run_json, archive_dir) VALUES ($1, $2, $3)")
            .bind(result.job_id)
            .bind(&result.run_json)
            .bind(&result.archive_dir)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn link_to_webhook(&self, webhook_id: i64, job_id: Uuid) -> Result<GithubWebhookJob> {
        let row = sqlx::query_as::<_, GithubWebhookJob>(
            "INSERT INTO github_webhook_job (github_webhook_id, job_id) VALUES ($1, $2)
             RETURNING github_webhook_id, job_id, created_at",
        )
        .bind(webhook_id)
        .bind(job_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(row)
    }

    async fn link_to_user(&self, user_id: i64, job_id: Uuid) -> Result<GithubUserJob> {
        let row = sqlx::query_as::<_, GithubUserJob>(
            "INSERT INTO github_user_job (github_user_id, job_id) VALUES ($1, $2)
             RETURNING github_user_id, job_id, created_at",
        )
        .bind(user_id)
        .bind(job_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(row)
    }

    async fn link_to_pull_request(
        &self,
        pull_request_id: i64,
        job_id: Uuid,
        triggering_comment_id: Option<i64>,
    ) -> Result<GithubPullRequestJob> {
        let row = sqlx::query_as::<_, GithubPullRequestJob>(
            "INSERT INTO github_pull_request_job
                 (job_id, github_pull_request_id, triggering_comment_id)
             VALUES ($1, $2, $3)
             RETURNING job_id, github_pull_request_id, triggering_comment_id, created_at",
        )
        .bind(job_id)
        .bind(pull_request_id)
        .bind(triggering_comment_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(row)
    }
}
