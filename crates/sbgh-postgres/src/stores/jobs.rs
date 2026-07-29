//! Postgres-backed `JobStore`. Single-statement operations for
//! most paths; `claim_next_queued` is the lone multi-statement query
//! (an UPDATE wrapping a `SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1`)
//! so the claim is atomic without a transaction wrapper.

use crate::IntoCoreResult as _;

use crate::mapping::{Db, IntoDomain};
use async_trait::async_trait;
use chrono::Duration;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::Result;
use crate::db::Pool;
use crate::db::jobs::{
    BaselineAnchor, BaselineMatch, BaselineSelection, BenchmarkRunMetric, JobCompletion,
    JobFailure, JobStore, PendingBenchmarkRun,
};
#[cfg(feature = "testing")]
use crate::db::jobs::{CreatedJob, JobCreationOutcome, NewBenchmarkSpec};
use crate::models::{
    GithubPullRequestJob, GithubUserJob, GithubWebhookJob, Job, JobEvent, JobEventKind,
    JobEventStatus, JobMetric, JobResult, JobStatus, NewJobEvent, QueuedEventDetail,
    ResolvedCommit, SubmissionSpec, SubmissionStepKind, TaskKind, TaskSubmission,
    TerminalJobStatus, uses_shared_calibration,
};
#[cfg(feature = "testing")]
use crate::models::{JobCreationRequest, NewJob};

#[derive(Clone)]
pub struct PostgresJobStore {
    pub(super) pool: Pool,
}

impl PostgresJobStore {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    #[cfg(feature = "testing")]
    pub async fn all_jobs(&self) -> Vec<Job> {
        sqlx::query_as::<_, Db<Job>>(
            r#"
            SELECT id, task_submission_id, task_spec_id, task_run_index,
                   github_installation_id, github_repo_id, status,
                   source, intent, task_kind, build_target, git_ref_kind, git_ref_display,
                   git_commit_hash, git_committed_at, workload_key, claim_token, claimed_at,
                   created_at, updated_at
              FROM job
          ORDER BY created_at, id
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .expect("read jobs")
        .into_domain()
    }

    #[cfg(feature = "testing")]
    pub async fn all_events(&self) -> Vec<JobEvent> {
        sqlx::query_as::<_, Db<JobEvent>>(
            "SELECT id, job_id, event_kind, event_status, occurred_at, github_comment_id, \
             github_check_run_id, github_check_run_url, remark, detail \
             FROM job_event ORDER BY id",
        )
        .fetch_all(&self.pool)
        .await
        .expect("read job events")
        .into_domain()
    }

    #[cfg(feature = "testing")]
    pub async fn user_links(&self) -> Vec<GithubUserJob> {
        sqlx::query_as::<_, Db<GithubUserJob>>(
            r#"
            SELECT github.triggering_user_id AS github_user_id,
                   job.id AS job_id, github.created_at
              FROM task_submission_github github
              JOIN job ON job.task_submission_id = github.task_submission_id
             WHERE github.triggering_user_id IS NOT NULL
            UNION ALL
            SELECT legacy.github_user_id, legacy.job_id, legacy.created_at
              FROM github_user_job legacy
             WHERE NOT EXISTS (
                   SELECT 1
                     FROM job
                     JOIN task_submission_github github
                       ON github.task_submission_id = job.task_submission_id
                    WHERE job.id = legacy.job_id
                      AND github.triggering_user_id IS NOT NULL
             )
             ORDER BY created_at
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .expect("read user job links")
        .into_domain()
    }

    #[cfg(feature = "testing")]
    pub async fn pr_links(&self) -> Vec<GithubPullRequestJob> {
        sqlx::query_as::<_, Db<GithubPullRequestJob>>(
            r#"
            SELECT job.id AS job_id, github.github_pull_request_id,
                   github.triggering_comment_id, github.created_at
              FROM task_submission_github github
              JOIN job ON job.task_submission_id = github.task_submission_id
             WHERE github.github_pull_request_id IS NOT NULL
            UNION ALL
            SELECT legacy.job_id, legacy.github_pull_request_id,
                   legacy.triggering_comment_id, legacy.created_at
              FROM github_pull_request_job legacy
             WHERE NOT EXISTS (
                   SELECT 1
                     FROM job
                     JOIN task_submission_github github
                       ON github.task_submission_id = job.task_submission_id
                    WHERE job.id = legacy.job_id
                      AND github.github_pull_request_id IS NOT NULL
             )
             ORDER BY created_at
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .expect("read pull request job links")
        .into_domain()
    }

    #[cfg(feature = "testing")]
    pub async fn spec(&self, id: Uuid) -> Option<SubmissionSpec> {
        self.lookup_submission_spec(id)
            .await
            .expect("read submission spec")
    }

    #[cfg(feature = "testing")]
    pub async fn specs_for_submission(&self, submission_id: Uuid) -> Vec<SubmissionSpec> {
        self.lookup_submission_specs(submission_id)
            .await
            .expect("read submission specs")
    }

    #[cfg(feature = "testing")]
    pub async fn steps_for_submission(
        &self,
        submission_id: Uuid,
    ) -> Vec<crate::models::SubmissionStep> {
        sqlx::query_as::<_, Db<crate::models::SubmissionStep>>(
            "SELECT id, task_submission_id, step_index, step_kind, task_spec_id, created_at \
             FROM task_workflow_step WHERE task_submission_id = $1 ORDER BY step_index",
        )
        .bind(submission_id)
        .fetch_all(&self.pool)
        .await
        .expect("read submission workflow steps")
        .into_domain()
    }

    #[cfg(feature = "testing")]
    async fn create_submission_specs(
        tx: &mut Transaction<'_, Postgres>,
        specs: &[NewBenchmarkSpec],
    ) -> Result<(Uuid, Vec<Uuid>)> {
        let Some(first) = specs.first() else {
            return Err(crate::Error::Other(anyhow::anyhow!(
                "benchmark submission needs at least one spec"
            )));
        };
        let first_job = &first.new_job;
        let submission_id = Uuid::new_v4();

        sqlx::query(
            r#"
            INSERT INTO task_submission
                (id, github_installation_id, github_repo_id, source, intent, artifact_prefix)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(submission_id)
        .bind(first_job.github_installation_id)
        .bind(first_job.github_repo_id)
        .bind(Db(first_job.axes.source))
        .bind(Db(first_job.axes.intent))
        .bind(submission_id.to_string())
        .execute(&mut **tx)
        .await
        .core()?;

        let submission_measured_run_count = specs
            .iter()
            .map(NewBenchmarkSpec::measured_run_count)
            .sum::<i32>();

        let mut spec_ids = Vec::with_capacity(specs.len());
        let mut step_index = 0i32;
        for (spec_index, spec) in specs.iter().enumerate() {
            let new = &spec.new_job;
            if new.github_installation_id != first_job.github_installation_id
                || new.axes.source != first_job.axes.source
                || new.axes.intent != first_job.axes.intent
            {
                return Err(crate::Error::Other(anyhow::anyhow!(
                    "benchmark submission specs must share installation, source, and intent"
                )));
            }

            let spec_id = Uuid::new_v4();
            let requested_run_count = spec
                .requested_run_count
                .max(1);
            sqlx::query(
                r#"
                INSERT INTO task_spec
                    (id, task_submission_id, spec_index, requested_run_count,
                     baseline_calibration_id, github_repo_id, task_kind, build_target,
                     git_ref_kind, git_ref_display, git_commit_hash, git_committed_at,
                     workload_key)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                "#,
            )
            .bind(spec_id)
            .bind(submission_id)
            .bind(spec_index as i32)
            .bind(requested_run_count)
            .bind(spec.baseline_calibration_id)
            .bind(new.github_repo_id)
            .bind(Db(new.axes.task_kind))
            .bind(Db(new.axes.build_target))
            .bind(Db(new.git_ref_kind))
            .bind(&new.git_ref_display)
            .bind(&new.git_commit_hash)
            .bind(new.git_committed_at)
            .bind(&new.workload_key)
            .execute(&mut **tx)
            .await
            .core()?;

            sqlx::query(
                r#"
                INSERT INTO task_workflow_step
                    (task_submission_id, step_index, step_kind, task_spec_id)
                VALUES ($1, $2, $3, $4)
                "#,
            )
            .bind(submission_id)
            .bind(step_index)
            .bind(Db(SubmissionStepKind::Build))
            .bind(spec_id)
            .execute(&mut **tx)
            .await
            .core()?;
            step_index += 1;

            if uses_shared_calibration(
                new.axes.task_kind,
                new.axes.build_target,
                submission_measured_run_count,
            ) {
                sqlx::query(
                    r#"
                    INSERT INTO task_workflow_step
                        (task_submission_id, step_index, step_kind, task_spec_id)
                    VALUES ($1, $2, $3, $4)
                    "#,
                )
                .bind(submission_id)
                .bind(step_index)
                .bind(Db(SubmissionStepKind::Calibrate))
                .bind(spec_id)
                .execute(&mut **tx)
                .await
                .core()?;
                step_index += 1;
            }

            if new.axes.task_kind != TaskKind::BuildOnly {
                sqlx::query(
                    r#"
                    INSERT INTO task_workflow_step
                        (task_submission_id, step_index, step_kind, task_spec_id)
                    VALUES ($1, $2, $3, $4)
                    "#,
                )
                .bind(submission_id)
                .bind(step_index)
                .bind(Db(SubmissionStepKind::Run))
                .bind(spec_id)
                .execute(&mut **tx)
                .await
                .core()?;
                step_index += 1;
            }

            spec_ids.push(spec_id);
        }

        Ok((submission_id, spec_ids))
    }

    #[cfg(feature = "testing")]
    async fn create_singleton_submission_spec(
        tx: &mut Transaction<'_, Postgres>,
        new: &NewJob,
        requested_run_count: i32,
    ) -> Result<(Uuid, Uuid)> {
        let specs = [NewBenchmarkSpec::singleton(new.clone(), requested_run_count)];
        let (submission_id, spec_ids) = Self::create_submission_specs(tx, &specs)
            .await
            .core()?;
        let spec_id = spec_ids
            .into_iter()
            .next()
            .expect("singleton creation returns one spec id");
        Ok((submission_id, spec_id))
    }

    #[cfg(feature = "testing")]
    fn requested_run_count_from_detail(detail: &serde_json::Value) -> i32 {
        match serde_json::from_value::<QueuedEventDetail>(detail.clone()) {
            Ok(QueuedEventDetail::SlackAdhoc { clean_repetitions, .. }) => {
                clean_repetitions.max(1) as i32
            }
            _ => 1,
        }
    }

    async fn insert_next_run_in_tx(
        tx: &mut Transaction<'_, Postgres>,
        prior: &Job,
        next_index: i32,
    ) -> Result<Job> {
        let job: Db<Job> = sqlx::query_as(
            r#"
            INSERT INTO job
                (task_submission_id, task_spec_id, task_run_index,
                 github_installation_id, github_repo_id, git_ref_kind, git_ref_display,
                 git_commit_hash, git_committed_at, workload_key,
                 source, intent, task_kind, build_target, required_capability)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            RETURNING id, task_submission_id, task_spec_id, task_run_index,
                      github_installation_id, github_repo_id, status,
                      source, intent, task_kind, build_target, git_ref_kind,
                      git_ref_display, git_commit_hash, git_committed_at, workload_key,
                      claim_token, claimed_at, created_at, updated_at
            "#,
        )
        .bind(prior.task_submission_id)
        .bind(prior.task_spec_id)
        .bind(next_index)
        .bind(prior.github_installation_id)
        .bind(prior.github_repo_id)
        .bind(Db(prior.git_ref_kind))
        .bind(&prior.git_ref_display)
        .bind(&prior.git_commit_hash)
        .bind(prior.git_committed_at)
        .bind(&prior.workload_key)
        .bind(Db(prior.source))
        .bind(Db(prior.intent))
        .bind(Db(prior.task_kind))
        .bind(Db(prior.build_target))
        .bind(Db(prior
            .task_kind
            .required_capability()))
        .fetch_one(&mut **tx)
        .await
        .core()?;

        Self::copy_run_provenance_events_in_tx(tx, job.id, prior.id)
            .await
            .core()?;
        Ok(job.0)
    }

    async fn insert_initial_run_for_spec_in_tx(
        tx: &mut Transaction<'_, Postgres>,
        submission: &TaskSubmission,
        spec: &SubmissionSpec,
        prior_job_id: Uuid,
    ) -> Result<Job> {
        let job: Db<Job> = sqlx::query_as(
            r#"
            INSERT INTO job
                (task_submission_id, task_spec_id, task_run_index,
                 github_installation_id, github_repo_id, git_ref_kind, git_ref_display,
                 git_commit_hash, git_committed_at, workload_key,
                 source, intent, task_kind, build_target, required_capability)
            VALUES ($1, $2, 0, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            RETURNING id, task_submission_id, task_spec_id, task_run_index,
                      github_installation_id, github_repo_id, status,
                      source, intent, task_kind, build_target, git_ref_kind,
                      git_ref_display, git_commit_hash, git_committed_at, workload_key,
                      claim_token, claimed_at, created_at, updated_at
            "#,
        )
        .bind(submission.id)
        .bind(spec.id)
        .bind(submission.github_installation_id)
        .bind(spec.github_repo_id)
        .bind(Db(spec.git_ref_kind))
        .bind(&spec.git_ref_display)
        .bind(&spec.git_commit_hash)
        .bind(spec.git_committed_at)
        .bind(&spec.workload_key)
        .bind(Db(submission.source))
        .bind(Db(submission.intent))
        .bind(Db(spec.task_kind))
        .bind(Db(spec.build_target))
        .bind(Db(spec
            .task_kind
            .required_capability()))
        .fetch_one(&mut **tx)
        .await
        .core()?;

        Self::copy_run_provenance_events_in_tx(tx, job.id, prior_job_id)
            .await
            .core()?;
        Ok(job.0)
    }

    async fn copy_run_provenance_events_in_tx(
        tx: &mut Transaction<'_, Postgres>,
        job_id: Uuid,
        prior_job_id: Uuid,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO job_event
                (job_id, event_kind, event_status, github_comment_id,
                 github_check_run_id, github_check_run_url, remark, detail)
            SELECT $1, event_kind, event_status, NULL, NULL, NULL, remark, detail
              FROM job_event
             WHERE job_id = $2
               AND event_kind = 'queued'
          ORDER BY occurred_at DESC, id DESC
             LIMIT 1
            "#,
        )
        .bind(job_id)
        .bind(prior_job_id)
        .execute(&mut **tx)
        .await
        .core()?;

        sqlx::query(
            r#"
            INSERT INTO job_event
                (job_id, event_kind, event_status, github_comment_id,
                 github_check_run_id, github_check_run_url, remark, detail)
            SELECT $1, event_kind, event_status, NULL, NULL, NULL, remark, detail
              FROM job_event
             WHERE job_id = $2
               AND event_kind = 'plan_message_sent'
          ORDER BY occurred_at DESC, id DESC
             LIMIT 1
            "#,
        )
        .bind(job_id)
        .bind(prior_job_id)
        .execute(&mut **tx)
        .await
        .core()?;

        Ok(())
    }
}

/// Prior run + its spec's requested count for v15 lazy enqueue decisions.
#[derive(sqlx::FromRow)]
struct PriorRunRow {
    #[sqlx(flatten)]
    job: Db<Job>,
    requested_run_count: i32,
    spec_index: i32,
}

/// Flattened row for `find_baseline_for`: the anchor columns + the baseline's
/// metric (via `#[sqlx(flatten)]`). The anchor's `job_id` reuses the metric PK,
/// so it isn't selected twice.
#[derive(sqlx::FromRow)]
struct BaselineRow {
    github_repo_id: i64,
    commit: String,
    git_ref_display: String,
    committed_at: Option<chrono::DateTime<chrono::Utc>>,
    #[sqlx(flatten)]
    metric: Db<crate::models::JobMetric>,
}

#[derive(sqlx::FromRow)]
struct BenchmarkRunMetricRow {
    task_run_index: i32,
    #[sqlx(flatten)]
    metric: Db<crate::models::JobMetric>,
}

impl From<BenchmarkRunMetricRow> for BenchmarkRunMetric {
    fn from(row: BenchmarkRunMetricRow) -> Self {
        Self {
            task_run_index: row.task_run_index,
            metric: row.metric.0,
        }
    }
}

impl BaselineRow {
    fn into_match(self, selection: BaselineSelection) -> BaselineMatch {
        BaselineMatch {
            anchor: BaselineAnchor {
                job_id: self.metric.job_id,
                github_repo_id: self.github_repo_id,
                commit: self.commit,
                git_ref_display: self.git_ref_display,
                committed_at: self.committed_at,
                selection,
            },
            metric: self.metric.0,
        }
    }
}

#[async_trait]
impl JobStore for PostgresJobStore {
    #[cfg(feature = "testing")]
    async fn insert_job(&self, new: &NewJob) -> Result<Job> {
        let mut tx = self
            .pool
            .begin()
            .await
            .core()?;
        let (submission_id, spec_id) = Self::create_singleton_submission_spec(&mut tx, new, 1)
            .await
            .core()?;
        // status defaults to 'queued'; claim_token + claimed_at stay NULL on
        // insert (queued-state invariant). v10 (0005): jobs carry the axes
        // natively — `trigger_kind` / `job_kind` are retired on the `job` table.
        let row = sqlx::query_as::<_, Db<Job>>(
            r#"
            INSERT INTO job
                (task_submission_id, task_spec_id, task_run_index,
                 github_installation_id, github_repo_id, git_ref_kind, git_ref_display,
                 git_commit_hash, git_committed_at, workload_key,
                 source, intent, task_kind, build_target, required_capability)
            VALUES ($1, $2, 0, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            RETURNING id, task_submission_id, task_spec_id, task_run_index,
                      github_installation_id, github_repo_id, status,
                      source, intent, task_kind, build_target, git_ref_kind,
                      git_ref_display, git_commit_hash, git_committed_at, workload_key,
                      claim_token, claimed_at, created_at, updated_at
            "#,
        )
        .bind(submission_id)
        .bind(spec_id)
        .bind(new.github_installation_id)
        .bind(new.github_repo_id)
        .bind(Db(new.git_ref_kind))
        .bind(&new.git_ref_display)
        .bind(&new.git_commit_hash)
        .bind(new.git_committed_at)
        .bind(&new.workload_key)
        .bind(Db(new.axes.source))
        .bind(Db(new.axes.intent))
        .bind(Db(new.axes.task_kind))
        .bind(Db(new.axes.build_target))
        .bind(Db(new
            .axes
            .task_kind
            .required_capability()))
        .fetch_one(&mut *tx)
        .await
        .core()?;
        tx.commit().await.core()?;
        Ok(row.0)
    }

    #[cfg(feature = "testing")]
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
        let mut tx = self
            .pool
            .begin()
            .await
            .core()?;
        let (submission_id, spec_id) =
            Self::create_singleton_submission_spec(&mut tx, &request.new_job, 1)
                .await
                .core()?;

        // v10 (0005): jobs carry the axes natively — set by the handler.
        let job: Db<Job> = sqlx::query_as(
            r#"
            INSERT INTO job
                (task_submission_id, task_spec_id, task_run_index,
                 github_installation_id, github_repo_id, git_ref_kind, git_ref_display,
                 git_commit_hash, git_committed_at, workload_key,
                 source, intent, task_kind, build_target, required_capability)
            VALUES ($1, $2, 0, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            RETURNING id, task_submission_id, task_spec_id, task_run_index,
                      github_installation_id, github_repo_id, status,
                      source, intent, task_kind, build_target, git_ref_kind,
                      git_ref_display, git_commit_hash, git_committed_at, workload_key,
                      claim_token, claimed_at, created_at, updated_at
            "#,
        )
        .bind(submission_id)
        .bind(spec_id)
        .bind(
            request
                .new_job
                .github_installation_id,
        )
        .bind(request.new_job.github_repo_id)
        .bind(Db(request.new_job.git_ref_kind))
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
        .bind(&request.new_job.workload_key)
        .bind(Db(request.new_job.axes.source))
        .bind(Db(request.new_job.axes.intent))
        .bind(Db(request.new_job.axes.task_kind))
        .bind(Db(request
            .new_job
            .axes
            .build_target))
        .bind(Db(request
            .new_job
            .axes
            .task_kind
            .required_capability()))
        .fetch_one(&mut *tx)
        .await
        .core()?;

        let webhook_link: Option<Db<GithubWebhookJob>> = sqlx::query_as(
            "INSERT INTO github_webhook_job (github_webhook_id, job_id) VALUES ($1, $2)
             ON CONFLICT (github_webhook_id) DO NOTHING
             RETURNING github_webhook_id, job_id, created_at",
        )
        .bind(request.github_webhook_id)
        .bind(job.id)
        .fetch_optional(&mut *tx)
        .await
        .core()?;
        let Some(webhook_link) = webhook_link else {
            // Webhook already has a job from a prior (possibly
            // interrupted) attempt — idempotent no-op. Roll back the
            // job we just inserted so it doesn't orphan.
            tx.rollback().await.core()?;
            return Ok(JobCreationOutcome::AlreadyEnqueued);
        };

        let user_link = if let Some(user_id) = request.triggering_user_id {
            let row: Db<GithubUserJob> = sqlx::query_as(
                "INSERT INTO github_user_job (github_user_id, job_id) VALUES ($1, $2)
                 RETURNING github_user_id, job_id, created_at",
            )
            .bind(user_id)
            .bind(job.id)
            .fetch_one(&mut *tx)
            .await
            .core()?;
            Some(row.0)
        } else {
            None
        };

        let pull_request_link = if let Some(ref pr) = request.pull_request_link {
            let row: Db<GithubPullRequestJob> = sqlx::query_as(
                "INSERT INTO github_pull_request_job
                     (job_id, github_pull_request_id, triggering_comment_id)
                 VALUES ($1, $2, $3)
                 RETURNING job_id, github_pull_request_id, triggering_comment_id, created_at",
            )
            .bind(job.id)
            .bind(pr.github_pull_request_id)
            .bind(pr.triggering_comment_id)
            .fetch_one(&mut *tx)
            .await
            .core()?;
            Some(row.0)
        } else {
            None
        };

        let queued_event: Db<JobEvent> = sqlx::query_as(
            r#"
            INSERT INTO job_event
                (job_id, event_kind, event_status, github_comment_id,
                 github_check_run_id, github_check_run_url, remark, detail)
            VALUES ($1, $2, $3, NULL, NULL, NULL, NULL, $4)
            RETURNING id, job_id, event_kind, event_status, occurred_at,
                      github_comment_id, github_check_run_id, github_check_run_url,
                      remark, detail
            "#,
        )
        .bind(job.id)
        .bind(Db(JobEventKind::Queued))
        .bind(Db(JobEventStatus::Success))
        .bind(&request.queued_event_detail)
        .fetch_one(&mut *tx)
        .await
        .core()?;

        tx.commit().await.core()?;

        Ok(JobCreationOutcome::Created(Box::new(CreatedJob {
            job: job.0,
            webhook_link: webhook_link.0,
            user_link,
            pull_request_link,
            queued_event: queued_event.0,
        })))
    }

    #[cfg(feature = "testing")]
    async fn create_unlinked_job(
        &self,
        job_id: Uuid,
        new_job: &NewJob,
        queued_event_detail: &serde_json::Value,
        plan_message_ts: Option<&str>,
    ) -> Result<Job> {
        // One transaction: job insert → queued event → the `plan_message_sent`
        // event when the connector pre-posted its message. No webhook/user/PR links
        // and no idempotency guard (see the trait docs); a failure rolls back.
        let mut tx = self
            .pool
            .begin()
            .await
            .core()?;
        let requested_run_count = Self::requested_run_count_from_detail(queued_event_detail);
        let (submission_id, spec_id) =
            Self::create_singleton_submission_spec(&mut tx, new_job, requested_run_count)
                .await
                .core()?;

        // v10 (0005): jobs carry the axes natively — set by the caller. The id
        // is caller-provided so the Slack message can be posted before the job
        // exists.
        let job: Db<Job> = sqlx::query_as(
            r#"
            INSERT INTO job
                (id, task_submission_id, task_spec_id, task_run_index,
                 github_installation_id, github_repo_id, git_ref_kind, git_ref_display,
                 git_commit_hash, git_committed_at, workload_key,
                 source, intent, task_kind, build_target, required_capability)
            VALUES ($1, $2, $3, 0, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            RETURNING id, task_submission_id, task_spec_id, task_run_index,
                      github_installation_id, github_repo_id, status,
                      source, intent, task_kind, build_target, git_ref_kind,
                      git_ref_display, git_commit_hash, git_committed_at, workload_key,
                      claim_token, claimed_at, created_at, updated_at
            "#,
        )
        .bind(job_id)
        .bind(submission_id)
        .bind(spec_id)
        .bind(new_job.github_installation_id)
        .bind(new_job.github_repo_id)
        .bind(Db(new_job.git_ref_kind))
        .bind(&new_job.git_ref_display)
        .bind(&new_job.git_commit_hash)
        .bind(new_job.git_committed_at)
        .bind(&new_job.workload_key)
        .bind(Db(new_job.axes.source))
        .bind(Db(new_job.axes.intent))
        .bind(Db(new_job.axes.task_kind))
        .bind(Db(new_job.axes.build_target))
        .bind(Db(new_job
            .axes
            .task_kind
            .required_capability()))
        .fetch_one(&mut *tx)
        .await
        .core()?;

        sqlx::query(
            r#"
            INSERT INTO job_event
                (job_id, event_kind, event_status, github_comment_id,
                 github_check_run_id, github_check_run_url, remark, detail)
            VALUES ($1, $2, $3, NULL, NULL, NULL, NULL, $4)
            "#,
        )
        .bind(job.id)
        .bind(Db(JobEventKind::Queued))
        .bind(Db(JobEventStatus::Success))
        .bind(queued_event_detail)
        .execute(&mut *tx)
        .await
        .core()?;

        // Record the pre-posted canonical message's `ts` in the same
        // transaction, so the job is never claimable without its identity.
        if let Some(ts) = plan_message_ts {
            sqlx::query(
                r#"
                INSERT INTO job_event
                    (job_id, event_kind, event_status, github_comment_id,
                     github_check_run_id, github_check_run_url, remark, detail)
                VALUES ($1, $2, $3, NULL, NULL, NULL, NULL, $4)
                "#,
            )
            .bind(job.id)
            .bind(Db(JobEventKind::PlanMessageSent))
            .bind(Db(JobEventStatus::Success))
            .bind(serde_json::json!({ "plan_message_ts": ts }))
            .execute(&mut *tx)
            .await
            .core()?;
        }

        tx.commit().await.core()?;
        Ok(job.0)
    }

    async fn append_next_benchmark_run(&self, completed_job_id: Uuid) -> Result<Option<Job>> {
        let mut tx = self
            .pool
            .begin()
            .await
            .core()?;
        let row: Option<PriorRunRow> = sqlx::query_as(
            r#"
            SELECT j.id, j.task_submission_id, j.task_spec_id, j.task_run_index,
                   j.github_installation_id, j.github_repo_id, j.status,
                   j.source, j.intent, j.task_kind, j.build_target, j.git_ref_kind,
                   j.git_ref_display, j.git_commit_hash, j.git_committed_at, j.workload_key,
                   j.claim_token, j.claimed_at, j.created_at, j.updated_at,
                   s.requested_run_count,
                   s.spec_index
              FROM job j
              JOIN task_spec s ON s.id = j.task_spec_id
             WHERE j.id = $1
             FOR UPDATE OF j
            "#,
        )
        .bind(completed_job_id)
        .fetch_optional(&mut *tx)
        .await
        .core()?;
        let Some(row) = row else {
            tx.rollback().await.core()?;
            return Ok(None);
        };
        let prior = row.job;
        let requested_run_count = row.requested_run_count;
        let spec_index = row.spec_index;
        if prior.status != JobStatus::Completed || prior.task_kind == TaskKind::BuildOnly {
            tx.rollback().await.core()?;
            return Ok(None);
        }

        let active: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM job
              WHERE task_submission_id = $1
                AND status IN ('queued', 'claimed', 'running')",
        )
        .bind(prior.task_submission_id)
        .fetch_one(&mut *tx)
        .await
        .core()?;
        if active > 0 {
            tx.rollback().await.core()?;
            return Ok(None);
        }

        let max_index: Option<i32> =
            sqlx::query_scalar("SELECT MAX(task_run_index) FROM job WHERE task_spec_id = $1")
                .bind(prior.task_spec_id)
                .fetch_one(&mut *tx)
                .await
                .core()?;
        let max_index = max_index.unwrap_or(0);
        if max_index != prior.task_run_index {
            tx.rollback().await.core()?;
            return Ok(None);
        }

        let next_index = max_index + 1;
        let job = if next_index < requested_run_count {
            Self::insert_next_run_in_tx(&mut tx, &prior, next_index)
                .await
                .core()?
        } else {
            let next_spec: Option<Db<SubmissionSpec>> = sqlx::query_as(
                r#"
                SELECT id, task_submission_id, spec_index, requested_run_count,
                       baseline_calibration_id, github_repo_id, task_kind, build_target,
                       git_ref_kind, git_ref_display, git_commit_hash, git_committed_at,
                       workload_key, created_at, updated_at
                  FROM task_spec
                 WHERE task_submission_id = $1
                   AND spec_index > $2
              ORDER BY spec_index ASC
                 LIMIT 1
                "#,
            )
            .bind(prior.task_submission_id)
            .bind(spec_index)
            .fetch_optional(&mut *tx)
            .await
            .core()?;
            let Some(next_spec) = next_spec else {
                tx.rollback().await.core()?;
                return Ok(None);
            };

            let existing_next_jobs: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM job WHERE task_spec_id = $1")
                    .bind(next_spec.id)
                    .fetch_one(&mut *tx)
                    .await
                    .core()?;
            if existing_next_jobs > 0 {
                tx.rollback().await.core()?;
                return Ok(None);
            }

            let submission: Db<TaskSubmission> = sqlx::query_as(
                r#"
                SELECT id, github_installation_id, github_repo_id, source, intent,
                       artifact_prefix, contract_version, request_digest,
                       required_worker_id, required_measurement_profile,
                       assigned_worker_id, assigned_measurement_profile,
                       created_at, updated_at
                  FROM task_submission
                 WHERE id = $1
                "#,
            )
            .bind(prior.task_submission_id)
            .fetch_one(&mut *tx)
            .await
            .core()?;
            Self::insert_initial_run_for_spec_in_tx(&mut tx, &submission, &next_spec, prior.id)
                .await
                .core()?
        };

        tx.commit().await.core()?;
        Ok(Some(job))
    }

    #[cfg(feature = "testing")]
    async fn create_unlinked_benchmark_submission(
        &self,
        first_job_id: Uuid,
        specs: &[NewBenchmarkSpec],
        queued_event_detail: &serde_json::Value,
        plan_message_ts: Option<&str>,
    ) -> Result<Job> {
        let mut tx = self
            .pool
            .begin()
            .await
            .core()?;
        let (submission_id, spec_ids) = Self::create_submission_specs(&mut tx, specs)
            .await
            .core()?;
        let Some(first) = specs.first() else {
            tx.rollback().await.core()?;
            return Err(crate::Error::Other(anyhow::anyhow!(
                "benchmark submission needs at least one spec"
            )));
        };
        let first_spec_id = spec_ids[0];
        let new_job = &first.new_job;
        let job: Db<Job> = sqlx::query_as(
            r#"
            INSERT INTO job
                (id, task_submission_id, task_spec_id, task_run_index,
                 github_installation_id, github_repo_id, git_ref_kind, git_ref_display,
                 git_commit_hash, git_committed_at, workload_key,
                 source, intent, task_kind, build_target, required_capability)
            VALUES ($1, $2, $3, 0, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            RETURNING id, task_submission_id, task_spec_id, task_run_index,
                      github_installation_id, github_repo_id, status,
                      source, intent, task_kind, build_target, git_ref_kind,
                      git_ref_display, git_commit_hash, git_committed_at, workload_key,
                      claim_token, claimed_at, created_at, updated_at
            "#,
        )
        .bind(first_job_id)
        .bind(submission_id)
        .bind(first_spec_id)
        .bind(new_job.github_installation_id)
        .bind(new_job.github_repo_id)
        .bind(Db(new_job.git_ref_kind))
        .bind(&new_job.git_ref_display)
        .bind(&new_job.git_commit_hash)
        .bind(new_job.git_committed_at)
        .bind(&new_job.workload_key)
        .bind(Db(new_job.axes.source))
        .bind(Db(new_job.axes.intent))
        .bind(Db(new_job.axes.task_kind))
        .bind(Db(new_job.axes.build_target))
        .bind(Db(new_job
            .axes
            .task_kind
            .required_capability()))
        .fetch_one(&mut *tx)
        .await
        .core()?;

        sqlx::query(
            r#"
            INSERT INTO job_event
                (job_id, event_kind, event_status, github_comment_id,
                 github_check_run_id, github_check_run_url, remark, detail)
            VALUES ($1, $2, $3, NULL, NULL, NULL, NULL, $4)
            "#,
        )
        .bind(job.id)
        .bind(Db(JobEventKind::Queued))
        .bind(Db(JobEventStatus::Success))
        .bind(queued_event_detail)
        .execute(&mut *tx)
        .await
        .core()?;

        if let Some(ts) = plan_message_ts {
            sqlx::query(
                r#"
                INSERT INTO job_event
                    (job_id, event_kind, event_status, github_comment_id,
                     github_check_run_id, github_check_run_url, remark, detail)
                VALUES ($1, $2, $3, NULL, NULL, NULL, NULL, $4)
                "#,
            )
            .bind(job.id)
            .bind(Db(JobEventKind::PlanMessageSent))
            .bind(Db(JobEventStatus::Success))
            .bind(serde_json::json!({ "plan_message_ts": ts }))
            .execute(&mut *tx)
            .await
            .core()?;
        }

        tx.commit().await.core()?;
        Ok(job.0)
    }

    async fn resume_pending_benchmark_runs(&self) -> Result<Vec<Job>> {
        let mut out = Vec::new();
        for pending in self
            .pending_completed_benchmark_runs()
            .await
            .core()?
        {
            if let Some(job) = self
                .append_next_benchmark_run(pending.completed_job_id)
                .await
                .core()?
            {
                out.push(job);
            }
        }
        Ok(out)
    }

    async fn pending_completed_benchmark_runs(&self) -> Result<Vec<PendingBenchmarkRun>> {
        let rows = sqlx::query_as::<_, Db<PendingBenchmarkRun>>(
            r#"
            WITH latest AS (
                SELECT DISTINCT ON (j.task_submission_id)
                       j.id,
                       j.task_submission_id,
                       j.task_spec_id,
                       j.task_run_index,
                       j.status,
                       s.spec_index,
                       s.requested_run_count,
                       s.task_kind
                  FROM job j
                  JOIN task_spec s ON s.id = j.task_spec_id
              ORDER BY j.task_submission_id, s.spec_index DESC, j.task_run_index DESC
            )
            SELECT latest.id AS completed_job_id,
                   latest.task_submission_id,
                   latest.task_spec_id,
                   latest.task_run_index,
                   latest.requested_run_count,
                   g.artifact_prefix
              FROM latest
              JOIN task_submission g ON g.id = latest.task_submission_id
             WHERE latest.status = 'completed'
               AND latest.task_kind <> 'build_only'
               AND NOT EXISTS (
                    SELECT 1
                      FROM job active
                     WHERE active.task_submission_id = latest.task_submission_id
                       AND active.status IN ('queued', 'claimed', 'running')
               )
               AND (
                    latest.task_run_index + 1 < latest.requested_run_count
                    OR EXISTS (
                        SELECT 1
                          FROM task_spec next_spec
                         WHERE next_spec.task_submission_id = latest.task_submission_id
                           AND next_spec.spec_index > latest.spec_index
                           AND NOT EXISTS (
                                SELECT 1
                                  FROM job next_job
                                 WHERE next_job.task_spec_id = next_spec.id
                           )
                    )
               )
          ORDER BY latest.id
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .core()?;
        Ok(rows.into_domain())
    }

    async fn benchmark_run_metrics(&self, task_spec_id: Uuid) -> Result<Vec<BenchmarkRunMetric>> {
        let rows = sqlx::query_as::<_, BenchmarkRunMetricRow>(
            r#"
            SELECT j.task_run_index,
                   m.job_id, m.envelope_duration_us, m.replay_duration_us,
                   m.total_duration_us, m.setup_duration_us, m.execution_duration_us,
                   m.commit_duration_us, m.clarity_runtime, m.transactions,
                   m.read_length, m.write_length, m.measured_blocks,
                   m.warmup_blocks, m.created_at
              FROM job j
              JOIN job_metric m ON m.job_id = j.id
             WHERE j.task_spec_id = $1
          ORDER BY j.task_run_index ASC
            "#,
        )
        .bind(task_spec_id)
        .fetch_all(&self.pool)
        .await
        .core()?;
        Ok(rows
            .into_iter()
            .map(Into::into)
            .collect())
    }

    async fn lookup_job(&self, job_id: Uuid) -> Result<Option<Job>> {
        let row = sqlx::query_as::<_, Db<Job>>(
            r#"
            SELECT id, task_submission_id, task_spec_id, task_run_index,
                   github_installation_id, github_repo_id, status,
                   source, intent, task_kind, build_target, git_ref_kind, git_ref_display,
                   git_commit_hash, git_committed_at, workload_key, claim_token, claimed_at,
                   created_at, updated_at
              FROM job
             WHERE id = $1
            "#,
        )
        .bind(job_id)
        .fetch_optional(&self.pool)
        .await
        .core()?;
        Ok(row.into_domain())
    }

    async fn lookup_task_submission(&self, submission_id: Uuid) -> Result<Option<TaskSubmission>> {
        let row = sqlx::query_as::<_, Db<TaskSubmission>>(
            r#"
            SELECT id, github_installation_id, github_repo_id, source, intent,
                   artifact_prefix, contract_version, request_digest,
                   required_worker_id, required_measurement_profile,
                   assigned_worker_id, assigned_measurement_profile,
                   created_at, updated_at
              FROM task_submission
             WHERE id = $1
            "#,
        )
        .bind(submission_id)
        .fetch_optional(&self.pool)
        .await
        .core()?;
        Ok(row.into_domain())
    }

    async fn lookup_submission_spec(&self, spec_id: Uuid) -> Result<Option<SubmissionSpec>> {
        let row = sqlx::query_as::<_, Db<SubmissionSpec>>(
            r#"
            SELECT id, task_submission_id, spec_index, requested_run_count,
                   baseline_calibration_id,
                   github_repo_id, task_kind, build_target, git_ref_kind,
                   git_ref_display, git_commit_hash, git_committed_at,
                   workload_key, created_at, updated_at
              FROM task_spec
             WHERE id = $1
            "#,
        )
        .bind(spec_id)
        .fetch_optional(&self.pool)
        .await
        .core()?;
        Ok(row.into_domain())
    }

    async fn lookup_submission_specs(&self, submission_id: Uuid) -> Result<Vec<SubmissionSpec>> {
        let rows = sqlx::query_as::<_, Db<SubmissionSpec>>(
            r#"
            SELECT id, task_submission_id, spec_index, requested_run_count,
                   baseline_calibration_id,
                   github_repo_id, task_kind, build_target, git_ref_kind,
                   git_ref_display, git_commit_hash, git_committed_at,
                   workload_key, created_at, updated_at
              FROM task_spec
             WHERE task_submission_id = $1
          ORDER BY spec_index ASC
            "#,
        )
        .bind(submission_id)
        .fetch_all(&self.pool)
        .await
        .core()?;
        Ok(rows.into_domain())
    }

    async fn completed_event_detail(&self, job_id: Uuid) -> Result<Option<serde_json::Value>> {
        let detail = sqlx::query_scalar::<_, Option<serde_json::Value>>(
            r#"
            SELECT detail
              FROM job_event
             WHERE job_id = $1
               AND event_kind = 'completed'
               AND event_status = 'success'
          ORDER BY occurred_at DESC, id DESC
             LIMIT 1
            "#,
        )
        .bind(job_id)
        .fetch_optional(&self.pool)
        .await
        .core()?;
        Ok(detail.flatten())
    }

    async fn claim_next_queued(&self, claim_token: Uuid) -> Result<Option<Job>> {
        // FOR UPDATE SKIP LOCKED on the partial queued index; transitions
        // queued → claimed and stamps the claim handoff columns. Single
        // statement so the row-pick and transition are atomic without
        // an explicit transaction.
        let row = sqlx::query_as::<_, Db<Job>>(
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
         RETURNING id, task_submission_id, task_spec_id, task_run_index,
                   github_installation_id, github_repo_id, status,
                   source, intent, task_kind, build_target, git_ref_kind, git_ref_display,
                   git_commit_hash, git_committed_at, workload_key, claim_token, claimed_at,
                   created_at, updated_at
            "#,
        )
        .bind(claim_token)
        .fetch_optional(&self.pool)
        .await
        .core()?;
        Ok(row.into_domain())
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
        // unresolved, the daemon passes `Some(ResolvedCommit)`
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
        .await
        .core()?;
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
        .bind(Db(status))
        .execute(&self.pool)
        .await
        .core()?;
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
        .await
        .core()?;
        Ok(result.rows_affected())
    }

    async fn complete_job(&self, completion: &JobCompletion) -> Result<bool> {
        // Single transaction: guarded running→completed, then the
        // write-once result (+ optional metric) + the completed event.
        // The guard (claim_token + status='running') makes a stale
        // finish racing the sweep a clean no-op (rolls back, returns
        // false) instead of corrupting a reclaimed row.
        let mut tx = self
            .pool
            .begin()
            .await
            .core()?;
        let updated = sqlx::query(
            "UPDATE job SET status = 'completed'
             WHERE id = $1 AND claim_token = $2 AND status = 'running'",
        )
        .bind(completion.job_id)
        .bind(completion.claim_token)
        .execute(&mut *tx)
        .await
        .core()?;
        if updated.rows_affected() != 1 {
            tx.rollback().await.core()?;
            return Ok(false);
        }

        let r = &completion.result;
        sqlx::query("INSERT INTO job_result (job_id, run_json, archive_dir) VALUES ($1, $2, $3)")
            .bind(r.job_id)
            .bind(&r.run_json)
            .bind(&r.archive_dir)
            .execute(&mut *tx)
            .await
            .core()?;

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
            .await
            .core()?;
        }

        if let Some(calibration_id) = completion.baseline_calibration_id {
            sqlx::query(
                r#"
                UPDATE task_spec spec
                   SET baseline_calibration_id = $2,
                       updated_at = NOW()
                  FROM job j
                 WHERE spec.id = j.task_spec_id
                   AND j.id = $1
                "#,
            )
            .bind(completion.job_id)
            .bind(calibration_id)
            .execute(&mut *tx)
            .await
            .core()?;
        }

        sqlx::query(
            "INSERT INTO job_event (job_id, event_kind, event_status, detail)
             VALUES ($1, $2, $3, $4)",
        )
        .bind(completion.job_id)
        .bind(Db(JobEventKind::Completed))
        .bind(Db(JobEventStatus::Success))
        .bind(&completion.event_detail)
        .execute(&mut *tx)
        .await
        .core()?;

        tx.commit().await.core()?;
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
        let mut tx = self
            .pool
            .begin()
            .await
            .core()?;
        let updated = sqlx::query(
            "UPDATE job SET status = 'failed'
             WHERE id = $1 AND claim_token = $2 AND status IN ('claimed', 'running')",
        )
        .bind(failure.job_id)
        .bind(failure.claim_token)
        .execute(&mut *tx)
        .await
        .core()?;
        if updated.rows_affected() != 1 {
            tx.rollback().await.core()?;
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
            .await
            .core()?;
        }

        sqlx::query(
            "INSERT INTO job_event (job_id, event_kind, event_status, remark, detail)
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(failure.job_id)
        .bind(Db(JobEventKind::Failed))
        .bind(Db(JobEventStatus::Fail))
        .bind(&failure.remark)
        .bind(&failure.event_detail)
        .execute(&mut *tx)
        .await
        .core()?;

        tx.commit().await.core()?;
        Ok(true)
    }

    async fn cancel_job(&self, job_id: Uuid, claim_token: Uuid, remark: &str) -> Result<bool> {
        // Mirror `fail_job` (claimed OR running, claim-token guarded) but
        // transition to `cancelled` and skip the forensics result — a cancelled
        // run produced none. `event_status = Fail` marks the non-success
        // terminal; the `cancelled` event_kind + `cancelled` job status carry
        // the precise (not-a-failure) semantics, and `job_event_status` has no
        // neutral value (audit-only field).
        let mut tx = self
            .pool
            .begin()
            .await
            .core()?;
        let updated = sqlx::query(
            "UPDATE job SET status = 'cancelled'
             WHERE id = $1 AND claim_token = $2 AND status IN ('claimed', 'running')",
        )
        .bind(job_id)
        .bind(claim_token)
        .execute(&mut *tx)
        .await
        .core()?;
        if updated.rows_affected() != 1 {
            tx.rollback().await.core()?;
            return Ok(false);
        }
        sqlx::query(
            "INSERT INTO job_event (job_id, event_kind, event_status, remark)
             VALUES ($1, $2, $3, $4)",
        )
        .bind(job_id)
        .bind(Db(JobEventKind::Cancelled))
        .bind(Db(JobEventStatus::Fail))
        .bind(remark)
        .execute(&mut *tx)
        .await
        .core()?;
        tx.commit().await.core()?;
        Ok(true)
    }

    async fn running_job_ids(&self) -> Result<Vec<Uuid>> {
        let ids: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM job WHERE status = 'running'")
            .fetch_all(&self.pool)
            .await
            .core()?;
        Ok(ids)
    }

    async fn find_active_job(
        &self,
        github_repo_id: i64,
        commit: &str,
        source: crate::models::JobSource,
        workload_key: &str,
    ) -> Result<Option<Uuid>> {
        // v10 (0005): the dedup keys on the job-model `source` axis — the
        // `pr_comment` trigger is `source = github_comment`.
        let id: Option<Uuid> = sqlx::query_scalar(
            r#"
            SELECT id
              FROM job
             WHERE github_repo_id = $1
               AND git_commit_hash = $2
               AND source = $3
               AND workload_key = $4
               AND status IN ('queued', 'claimed', 'running')
             LIMIT 1
            "#,
        )
        .bind(github_repo_id)
        .bind(commit)
        .bind(Db(source))
        .bind(workload_key)
        .fetch_optional(&self.pool)
        .await
        .core()?;
        Ok(id)
    }

    async fn recent_build_attempt(
        &self,
        github_repo_id: i64,
        commit: &str,
        build_target: crate::models::BuildTarget,
        failed_since: chrono::DateTime<chrono::Utc>,
    ) -> Result<Option<Uuid>> {
        // v11 (0031): build-only warm dedup — keyed on the job-model `task_kind`
        // + `build_target` axes (a build job has no `workload_key`). Matches an
        // active build OR one that failed/cancelled within the retry cooldown
        // (`updated_at >= failed_since`), so a persistently-failing warm isn't
        // re-enqueued on every recompute.
        let id: Option<Uuid> = sqlx::query_scalar(
            r#"
            SELECT id
              FROM job
             WHERE github_repo_id = $1
               AND git_commit_hash = $2
               AND task_kind = 'build_only'
               AND build_target = $3
               AND (
                     status IN ('queued', 'claimed', 'running')
                  OR (status IN ('failed', 'cancelled') AND updated_at >= $4)
               )
             LIMIT 1
            "#,
        )
        .bind(github_repo_id)
        .bind(commit)
        .bind(Db(build_target))
        .bind(failed_since)
        .fetch_optional(&self.pool)
        .await
        .core()?;
        Ok(id)
    }

    async fn find_baseline_for(
        &self,
        subject_job_id: Uuid,
        merge_base_sha: &str,
        base_ref: &str,
        merge_base_committed_at: Option<chrono::DateTime<chrono::Utc>>,
        workload_key: &str,
    ) -> Result<Option<BaselineMatch>> {
        // 1. Exact hit at the merge-base SHA, repo-agnostic (uses
        //    `job_baseline_exact_idx (git_commit_hash, workload_key)`). Newest
        //    measurement wins if the same (SHA, workload) ran more than once.
        let exact: Option<BaselineRow> = sqlx::query_as(
            r#"
            WITH subject AS (
                SELECT submission.assigned_measurement_profile AS measurement_profile
                  FROM job subject_job
                  JOIN task_submission submission ON submission.id = subject_job.task_submission_id
                 WHERE subject_job.id = $1
                   AND submission.assigned_measurement_profile IS NOT NULL
            )
            SELECT j.github_repo_id,
                   j.git_commit_hash AS commit,
                   j.git_ref_display,
                   j.git_committed_at AS committed_at,
                   m.job_id, m.envelope_duration_us, m.replay_duration_us,
                   m.total_duration_us, m.setup_duration_us, m.execution_duration_us,
                   m.commit_duration_us, m.clarity_runtime, m.transactions,
                   m.read_length, m.write_length, m.measured_blocks,
                   m.warmup_blocks, m.created_at
              FROM job j
              JOIN task_submission submission ON submission.id = j.task_submission_id
              JOIN subject ON subject.measurement_profile = submission.assigned_measurement_profile
              JOIN job_metric m ON m.job_id = j.id
             WHERE j.git_commit_hash = $2
               AND j.workload_key = $3
               AND j.intent = 'baseline_benchmark'
               AND j.status = 'completed'
          -- Deterministic: freshest measurement, then job id, so the same SHA
          -- benchmarked more than once always resolves to one stable row.
          ORDER BY m.created_at DESC, j.id DESC
             LIMIT 1
            "#,
        )
        .bind(subject_job_id)
        .bind(merge_base_sha)
        .bind(workload_key)
        .fetch_optional(&self.pool)
        .await
        .core()?;
        if let Some(row) = exact {
            return Ok(Some(row.into_match(BaselineSelection::Exact)));
        }

        // 2. Nearest-before on the target branch — needs a fork-point timestamp to
        //    order against (uses `job_baseline_ref_timeline_idx`).
        let Some(ts) = merge_base_committed_at else {
            return Ok(None);
        };
        let nearest: Option<BaselineRow> = sqlx::query_as(
            r#"
            WITH subject AS (
                SELECT submission.assigned_measurement_profile AS measurement_profile
                  FROM job subject_job
                  JOIN task_submission submission ON submission.id = subject_job.task_submission_id
                 WHERE subject_job.id = $1
                   AND submission.assigned_measurement_profile IS NOT NULL
            )
            SELECT j.github_repo_id,
                   j.git_commit_hash AS commit,
                   j.git_ref_display,
                   j.git_committed_at AS committed_at,
                   m.job_id, m.envelope_duration_us, m.replay_duration_us,
                   m.total_duration_us, m.setup_duration_us, m.execution_duration_us,
                   m.commit_duration_us, m.clarity_runtime, m.transactions,
                   m.read_length, m.write_length, m.measured_blocks,
                   m.warmup_blocks, m.created_at
              FROM job j
              JOIN task_submission submission ON submission.id = j.task_submission_id
              JOIN subject ON subject.measurement_profile = submission.assigned_measurement_profile
              JOIN job_metric m ON m.job_id = j.id
             WHERE j.git_ref_display = $2
               AND j.workload_key = $3
               AND j.git_committed_at <= $4
               AND j.intent = 'baseline_benchmark'
               AND j.status = 'completed'
          -- Newest commit at/before the fork-point; ties (shared timestamp /
          -- same commit re-run) break to freshest measurement, then job id.
          ORDER BY j.git_committed_at DESC, m.created_at DESC, j.id DESC
             LIMIT 1
            "#,
        )
        .bind(subject_job_id)
        .bind(base_ref)
        .bind(workload_key)
        .bind(ts)
        .fetch_optional(&self.pool)
        .await
        .core()?;
        Ok(nearest.map(|row| row.into_match(BaselineSelection::NearestBefore)))
    }

    async fn queued_jobs_ordered(&self) -> Result<Vec<Job>> {
        // Same ordering as `claim_next_queued` so the reported position matches
        // the order jobs are actually claimed.
        let rows = sqlx::query_as::<_, Db<Job>>(
            r#"
            SELECT id, task_submission_id, task_spec_id, task_run_index,
                   github_installation_id, github_repo_id, status,
                   source, intent, task_kind, build_target, git_ref_kind, git_ref_display,
                   git_commit_hash, git_committed_at, workload_key, claim_token, claimed_at,
                   created_at, updated_at
              FROM job
             WHERE status = 'queued'
          ORDER BY created_at, id
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .core()?;
        Ok(rows.into_domain())
    }

    async fn cancel_orphan(&self, job_id: Uuid, remark: &str) -> Result<bool> {
        // No claim-token guard (see trait docs): startup-only, the original
        // claimer is dead, so an unconditional running→cancelled is safe and a
        // re-run after a mid-recovery crash is idempotent (a row already off
        // `running` won't match). One transaction for the transition + event.
        let mut tx = self
            .pool
            .begin()
            .await
            .core()?;
        let updated =
            sqlx::query("UPDATE job SET status = 'cancelled' WHERE id = $1 AND status = 'running'")
                .bind(job_id)
                .execute(&mut *tx)
                .await
                .core()?;
        if updated.rows_affected() != 1 {
            tx.rollback().await.core()?;
            return Ok(false);
        }
        sqlx::query(
            "INSERT INTO job_event (job_id, event_kind, event_status, remark)
             VALUES ($1, $2, $3, $4)",
        )
        .bind(job_id)
        .bind(Db(JobEventKind::Cancelled))
        .bind(Db(JobEventStatus::Fail))
        .bind(remark)
        .execute(&mut *tx)
        .await
        .core()?;
        tx.commit().await.core()?;
        Ok(true)
    }

    async fn queued_event(&self, job_id: Uuid) -> Result<Option<JobEvent>> {
        let row = sqlx::query_as::<_, Db<JobEvent>>(
            r#"
            SELECT id, job_id, event_kind, event_status, occurred_at,
                   github_comment_id, github_check_run_id, github_check_run_url,
                   remark, detail
              FROM job_event
             WHERE job_id = $1 AND event_kind = 'queued'
          ORDER BY occurred_at, id
             LIMIT 1
            "#,
        )
        .bind(job_id)
        .fetch_optional(&self.pool)
        .await
        .core()?;
        Ok(row.into_domain())
    }

    async fn pull_request_link(&self, job_id: Uuid) -> Result<Option<GithubPullRequestJob>> {
        let row = sqlx::query_as::<_, Db<GithubPullRequestJob>>(
            r#"
            SELECT $1 AS job_id, github.github_pull_request_id,
                   github.triggering_comment_id, github.created_at
              FROM job
              JOIN task_submission_github github
                ON github.task_submission_id = job.task_submission_id
             WHERE job.id = $1
               AND github.github_pull_request_id IS NOT NULL
            UNION ALL
            SELECT job_id, github_pull_request_id, triggering_comment_id, created_at
              FROM github_pull_request_job
             WHERE job_id = $1
               AND NOT EXISTS (
                   SELECT 1
                     FROM job aggregate_job
                     JOIN task_submission_github github
                       ON github.task_submission_id = aggregate_job.task_submission_id
                    WHERE aggregate_job.id = $1
                      AND github.github_pull_request_id IS NOT NULL
               )
             LIMIT 1
            "#,
        )
        .bind(job_id)
        .fetch_optional(&self.pool)
        .await
        .core()?;
        Ok(row.into_domain())
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
        .await
        .core()?
        .flatten();
        Ok(id)
    }

    async fn latest_check_run(&self, job_id: Uuid) -> Result<Option<(i64, Option<String>)>> {
        // Most recent check-run-bearing event for the job. The check run id
        // is stable across status/output updates, so the newest
        // `check_run_created` event carries the live id + url.
        let row: Option<(Option<i64>, Option<String>)> = sqlx::query_as(
            "SELECT github_check_run_id, github_check_run_url
               FROM job_event
              WHERE job_id = $1
                AND github_check_run_id IS NOT NULL
                AND event_kind = 'check_run_created'
           ORDER BY occurred_at DESC, id DESC
              LIMIT 1",
        )
        .bind(job_id)
        .fetch_optional(&self.pool)
        .await
        .core()?;
        Ok(row.map(|(id, url)| (id.unwrap_or_default(), url)))
    }

    async fn latest_plan_message_ts(&self, job_id: Uuid) -> Result<Option<String>> {
        let ts: Option<String> = sqlx::query_scalar(
            r#"
            SELECT COALESCE(
                (
                    SELECT slack.report_message_ts
                      FROM job
                      JOIN task_submission_slack slack
                        ON slack.task_submission_id = job.task_submission_id
                     WHERE job.id = $1
                ),
                (
                    SELECT detail->>'plan_message_ts'
                      FROM job_event
                     WHERE job_id = $1
                       AND event_kind = 'plan_message_sent'
                       AND detail->>'plan_message_ts' IS NOT NULL
                     ORDER BY occurred_at DESC, id DESC
                     LIMIT 1
                )
            )
            "#,
        )
        .bind(job_id)
        .fetch_one(&self.pool)
        .await
        .core()?;
        Ok(ts)
    }

    async fn record_plan_message_ts(&self, job_id: Uuid, message_ts: &str) -> Result<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .core()?;
        sqlx::query(
            r#"
            UPDATE task_submission_slack slack
               SET report_message_ts = $2
              FROM job
             WHERE job.id = $1
               AND slack.task_submission_id = job.task_submission_id
            "#,
        )
        .bind(job_id)
        .bind(message_ts)
        .execute(&mut *tx)
        .await
        .core()?;
        sqlx::query(
            r#"
            INSERT INTO job_event
                (job_id, event_kind, event_status, detail)
            VALUES ($1, 'plan_message_sent', 'success', $2)
            "#,
        )
        .bind(job_id)
        .bind(serde_json::json!({ "plan_message_ts": message_ts }))
        .execute(&mut *tx)
        .await
        .core()?;
        tx.commit().await.core()?;
        Ok(())
    }

    async fn insert_event(&self, new: &NewJobEvent) -> Result<JobEvent> {
        let row = sqlx::query_as::<_, Db<JobEvent>>(
            r#"
            INSERT INTO job_event
                (job_id, event_kind, event_status, github_comment_id,
                 github_check_run_id, github_check_run_url, remark, detail)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING id, job_id, event_kind, event_status, occurred_at,
                      github_comment_id, github_check_run_id, github_check_run_url,
                      remark, detail
            "#,
        )
        .bind(new.job_id)
        .bind(Db(new.event_kind))
        .bind(Db(new.event_status))
        .bind(new.github_comment_id)
        .bind(new.github_check_run_id)
        .bind(&new.github_check_run_url)
        .bind(&new.remark)
        .bind(&new.detail)
        .fetch_one(&self.pool)
        .await
        .core()?;
        Ok(row.0)
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
        .await
        .core()?;
        Ok(())
    }

    async fn record_result(&self, result: &JobResult) -> Result<()> {
        sqlx::query("INSERT INTO job_result (job_id, run_json, archive_dir) VALUES ($1, $2, $3)")
            .bind(result.job_id)
            .bind(&result.run_json)
            .bind(&result.archive_dir)
            .execute(&self.pool)
            .await
            .core()?;
        Ok(())
    }

    #[cfg(feature = "testing")]
    async fn link_to_webhook(&self, webhook_id: i64, job_id: Uuid) -> Result<GithubWebhookJob> {
        let row = sqlx::query_as::<_, Db<GithubWebhookJob>>(
            "INSERT INTO github_webhook_job (github_webhook_id, job_id) VALUES ($1, $2)
             RETURNING github_webhook_id, job_id, created_at",
        )
        .bind(webhook_id)
        .bind(job_id)
        .fetch_one(&self.pool)
        .await
        .core()?;
        Ok(row.0)
    }

    #[cfg(feature = "testing")]
    async fn link_to_user(&self, user_id: i64, job_id: Uuid) -> Result<GithubUserJob> {
        let row = sqlx::query_as::<_, Db<GithubUserJob>>(
            "INSERT INTO github_user_job (github_user_id, job_id) VALUES ($1, $2)
             RETURNING github_user_id, job_id, created_at",
        )
        .bind(user_id)
        .bind(job_id)
        .fetch_one(&self.pool)
        .await
        .core()?;
        Ok(row.0)
    }

    #[cfg(feature = "testing")]
    async fn link_to_pull_request(
        &self,
        pull_request_id: i64,
        job_id: Uuid,
        triggering_comment_id: Option<i64>,
    ) -> Result<GithubPullRequestJob> {
        let row = sqlx::query_as::<_, Db<GithubPullRequestJob>>(
            "INSERT INTO github_pull_request_job
                 (job_id, github_pull_request_id, triggering_comment_id)
             VALUES ($1, $2, $3)
             RETURNING job_id, github_pull_request_id, triggering_comment_id, created_at",
        )
        .bind(job_id)
        .bind(pull_request_id)
        .bind(triggering_comment_id)
        .fetch_one(&self.pool)
        .await
        .core()?;
        Ok(row.0)
    }
}
