//! Main orchestrator loop.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sbgh_core::config::Config;
use sbgh_core::db::JobStore;
use sbgh_core::github::GitHubApi;
use sbgh_core::models::Job;

use crate::libvirt::{LibvirtDriver, OutcomeStatus, Phase, PhaseListener, Shell};
use crate::progress::ProgressReporter;

const POLL_INTERVAL: Duration = Duration::from_secs(5);

pub struct Runner {
    config: Arc<Config>,
    jobs: Arc<dyn JobStore>,
    gh: Arc<dyn GitHubApi>,
    shell: Arc<dyn Shell>,
}

impl Runner {
    pub fn new(
        config: Config,
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
                    if let Err(e) = self.execute(&job).await {
                        // Reached only on setup-time failures (e.g. couldn't
                        // create the job dir). VM-side failures come back as
                        // `Ok(BenchmarkOutcome { status: Failed })` and are
                        // routed below.
                        tracing::error!(job_id = %job.id, error = %e, "job setup failed");
                        let _ = self
                            .jobs
                            .fail(job.id, &e.to_string(), None)
                            .await;
                        let _ = self
                            .reporter(&job)
                            .failed(&e.to_string())
                            .await;
                    }
                }
                Ok(None) => tokio::time::sleep(POLL_INTERVAL).await,
                Err(e) => {
                    tracing::error!(error = %e, "queue claim failed");
                    tokio::time::sleep(POLL_INTERVAL).await;
                }
            }
        }
    }

    async fn execute(&self, job: &Job) -> anyhow::Result<()> {
        tracing::info!(job_id = %job.id, repo = %job.repository, "starting job");
        let reporter = self.reporter(job);
        reporter.started().await?;

        let driver = LibvirtDriver::new(self.config.clone(), self.shell.clone());
        let phase_listener = CommentPhaseListener::new(self.gh.clone(), job.clone());
        let outcome = driver
            .run_benchmark(job, &phase_listener)
            .await?;

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

    fn reporter<'a>(&'a self, job: &'a Job) -> ProgressReporter<'a> {
        ProgressReporter::new(self.gh.as_ref(), job)
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
            tracing::warn!(error = %e, "phase comment update failed");
        }
    }
}
