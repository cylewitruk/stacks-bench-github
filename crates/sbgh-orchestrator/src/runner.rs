//! Main orchestrator loop.
//!
//! On pickup the orchestrator:
//!   1. Resolves the PR head SHA via the GitHub API (the handler can't — it has
//!      no App credentials) and writes it back to the job row.
//!   2. Posts the initial "starting" PR comment and persists the returned
//!      comment id, so the phase-progress listener has somewhere to push
//!      updates.
//!   3. Runs the benchmark.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sbgh_core::config::OrchestratorConfig;
use sbgh_core::db::JobStore;
use sbgh_core::github::GitHubApi;
use sbgh_core::models::Job;

use crate::libvirt::{LibvirtDriver, OutcomeStatus, Phase, PhaseListener, Shell};
use crate::progress::ProgressReporter;

const POLL_INTERVAL: Duration = Duration::from_secs(5);

pub struct Runner {
    config: Arc<OrchestratorConfig>,
    jobs: Arc<dyn JobStore>,
    gh: Arc<dyn GitHubApi>,
    shell: Arc<dyn Shell>,
}

impl Runner {
    pub fn new(
        config: OrchestratorConfig,
        jobs: Arc<dyn JobStore>,
        gh: Arc<dyn GitHubApi>,
        shell: Arc<dyn Shell>,
    ) -> Self {
        Self {
            config: Arc::new(config),
            jobs,
            gh,
            shell,
        }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        tracing::info!("orchestrator started");
        loop {
            match self.jobs.claim_next().await {
                Ok(Some(job)) => {
                    if let Err(e) = self.execute(job).await {
                        // Setup-time failure (couldn't resolve SHA, mkdir
                        // failed, etc.). VM-side failures come back via
                        // `BenchmarkOutcome::Failed` instead.
                        tracing::error!(error = ?e, "job setup failed");
                    }
                }
                Ok(None) => tokio::time::sleep(POLL_INTERVAL).await,
                Err(e) => {
                    tracing::error!(error = ?e, "queue claim failed");
                    tokio::time::sleep(POLL_INTERVAL).await;
                }
            }
        }
    }

    /// Owns the job by value so we can attach the resolved head_sha + the
    /// posted comment_id to the in-memory copy before handing it to the
    /// reporter and the driver (both want them populated).
    async fn execute(&self, mut job: Job) -> anyhow::Result<()> {
        tracing::info!(job_id = %job.id, repo = %job.repository, "starting job");

        // Pre-flight: resolve head SHA + post the initial PR comment.
        // Errors here are reported back to the user (via fail) before we
        // give up — the user otherwise sees a silent "/benchmark went
        // nowhere" because the handler doesn't write a comment any more.
        if let Err(e) = self.preflight(&mut job).await {
            let msg = format!("pre-flight failed: {e}");
            tracing::error!(job_id = %job.id, error = ?e, "pre-flight failed");
            let _ = self
                .jobs
                .fail(job.id, &msg, None)
                .await;
            return Err(e);
        }

        let reporter = ProgressReporter::new(self.gh.as_ref(), &job);
        reporter.started().await?;

        let driver = LibvirtDriver::new(self.config.clone(), self.shell.clone());
        let phase_listener = CommentPhaseListener::new(self.gh.clone(), job.clone());
        let outcome = match driver
            .run_benchmark(&job, &phase_listener)
            .await
        {
            Ok(o) => o,
            Err(e) => {
                let msg = e.to_string();
                let _ = self
                    .jobs
                    .fail(job.id, &msg, None)
                    .await;
                let _ = reporter.failed(&msg).await;
                return Err(e);
            }
        };

        match outcome.status {
            OutcomeStatus::Ok => {
                self.jobs
                    .complete(job.id, outcome.summary.clone())
                    .await?;
                reporter
                    .completed(&outcome.summary)
                    .await?;
            }
            OutcomeStatus::Failed(err) => {
                self.jobs
                    .fail(job.id, &err, Some(outcome.summary.clone()))
                    .await?;
                reporter.failed(&err).await?;
            }
        }
        Ok(())
    }

    /// Resolve the head SHA + post the initial PR comment, mutating `job` in
    /// place so the caller's reporter/listener see populated values. Both
    /// steps are idempotent on retry: empty `head_sha`/`None` `comment_id`
    /// trigger the work; populated values are kept as-is.
    async fn preflight(&self, job: &mut Job) -> anyhow::Result<()> {
        if job.head_sha.is_empty() {
            let sha = self
                .gh
                .pr_head_sha(job.installation_id, &job.repository, job.pr_number as u64)
                .await?;
            self.jobs
                .set_head_sha(job.id, &sha)
                .await?;
            job.head_sha = sha;
        }

        if job.comment_id.is_none() {
            let body = format!(
                ":construction: starting benchmark `{id}` (commit `{sha}`)…",
                id = job.id,
                sha = job.head_sha,
            );
            let posted = self
                .gh
                .create_pr_comment(
                    job.installation_id,
                    &job.repository,
                    job.pr_number as u64,
                    &body,
                )
                .await?;
            self.jobs
                .set_comment_id(job.id, posted.id)
                .await?;
            job.comment_id = Some(posted.id);
        }

        Ok(())
    }
}

/// Bridge between the libvirt driver's phase events and the PR comment.
struct CommentPhaseListener {
    gh: Arc<dyn GitHubApi>,
    job: Job,
}

impl CommentPhaseListener {
    fn new(gh: Arc<dyn GitHubApi>, job: Job) -> Self {
        Self { gh, job }
    }
}

#[async_trait]
impl PhaseListener for CommentPhaseListener {
    async fn on_phase(&self, phase: &Phase) {
        let Some(comment_id) = self.job.comment_id else {
            return;
        };
        let body = format!(
            ":construction: benchmark `{id}` — **{phase}** (commit `{sha}`)",
            id = self.job.id,
            phase = phase,
            sha = self.job.head_sha,
        );
        if let Err(e) = self
            .gh
            .update_pr_comment(self.job.installation_id, &self.job.repository, comment_id, &body)
            .await
        {
            // Debug repr surfaces GitHub's response body (status + message),
            // e.g. "Resource not accessible by integration" when the App is
            // missing the Issues: Write permission.
            tracing::warn!(error = ?e, "phase comment update failed");
        }
    }
}
