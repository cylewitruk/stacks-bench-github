use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use sbgh_core::bench_args::resolve_bench_args;
use sbgh_core::config::DaemonConfig;
use sbgh_core::db::fleet::{FleetStore, PreparedExecution, ResolvedSpecSource};
use sbgh_core::db::{JobStore, RepoStore};
use sbgh_core::models::{BuildTarget, GitRefKind, TaskKind, uses_shared_calibration};
use sbgh_github::GitHubApi;
use sbgh_proto::{BenchmarkPayload, ReliableEventPayload, TaskPayload};
use sbgh_slack::SlackClient;
use tokio::sync::{Mutex, OnceCell};
use uuid::Uuid;

use crate::artifact_store::{ArtifactStore, GROUP_SQLITE_RELATIVE, group_artifact_key};
use crate::job_source::{ProgressTarget, RunnableJob, RunnableJobStore};
use crate::report::{ReportSurface, build_report_surface};
use crate::report_event::PhaseLabel;
use crate::reporter::{Reporter, ReporterDependencies};
use crate::shutdown::Shutdown;
use crate::slack_report::SlackSessionRegistry;

pub struct FleetCoordinator {
    config: Arc<DaemonConfig>,
    fleet: Arc<dyn FleetStore>,
    jobs: Arc<dyn RunnableJobStore>,
    repeat_jobs: Arc<dyn JobStore>,
    repos: Arc<dyn RepoStore>,
    gh: Arc<dyn GitHubApi>,
    artifacts: Arc<dyn ArtifactStore>,
    slack: Option<Arc<dyn SlackClient>>,
    app_id: Arc<OnceCell<i64>>,
    slack_sessions: Arc<SlackSessionRegistry>,
    surfaces: Mutex<HashMap<Uuid, Arc<dyn ReportSurface>>>,
    last_queue_positions: Mutex<HashMap<Uuid, usize>>,
}

pub struct FleetCoordinatorDependencies {
    pub fleet: Arc<dyn FleetStore>,
    pub jobs: Arc<dyn RunnableJobStore>,
    pub repeat_jobs: Arc<dyn JobStore>,
    pub repos: Arc<dyn RepoStore>,
    pub gh: Arc<dyn GitHubApi>,
    pub artifacts: Arc<dyn ArtifactStore>,
    pub slack: Option<Arc<dyn SlackClient>>,
}

impl FleetCoordinator {
    pub fn new(config: Arc<DaemonConfig>, dependencies: FleetCoordinatorDependencies) -> Self {
        Self {
            config,
            fleet: dependencies.fleet,
            jobs: dependencies.jobs,
            repeat_jobs: dependencies.repeat_jobs,
            repos: dependencies.repos,
            gh: dependencies.gh,
            artifacts: dependencies.artifacts,
            slack: dependencies.slack,
            app_id: Arc::new(OnceCell::new()),
            slack_sessions: Arc::new(SlackSessionRegistry::new()),
            surfaces: Mutex::new(HashMap::new()),
            last_queue_positions: Mutex::new(HashMap::new()),
        }
    }

    pub async fn run(self, shutdown: Shutdown) -> anyhow::Result<()> {
        let mut prepare_interval = tokio::time::interval(Duration::from_secs(1));
        let mut projection_interval = tokio::time::interval(Duration::from_millis(250));
        let mut expiry_interval = tokio::time::interval(Duration::from_secs(2));
        let mut draining = false;
        let mut abort_requested = false;
        loop {
            tokio::select! {
                () = shutdown.draining.cancelled(), if !draining => {
                    draining = true;
                    self.fleet.set_all_workers_draining(true).await?;
                    tracing::info!("fleet drain requested; workers will stop claiming");
                }
                () = shutdown.abort.cancelled(), if !abort_requested => {
                    abort_requested = true;
                    self.fleet.request_cancel_all().await?;
                    tracing::warn!("fleet abort requested; active attempts will cancel");
                }
                _ = prepare_interval.tick(), if !draining => {
                    if let Err(error) = self.prepare_queued().await {
                        tracing::error!(%error, "fleet enqueue preparation failed");
                    }
                }
                _ = projection_interval.tick() => {
                    if let Err(error) = self.project_events().await {
                        tracing::error!(%error, "fleet event projection failed");
                    }
                    if draining && self.fleet.fleet_snapshot().await?.active_attempts == 0 {
                        shutdown.exit.cancel();
                        return Ok(());
                    }
                }
                _ = expiry_interval.tick() => {
                    // Expiry and projection share this coordinator task. Along
                    // with the store's DB-time eligibility predicate, this
                    // prevents a stale attempt from racing an external report
                    // mutation after its successor becomes authoritative.
                    if let Err(error) = self.fleet.expire_stale_attempts().await {
                        tracing::error!(%error, "fleet attempt expiry failed");
                    }
                }
            }
        }
    }

    async fn prepare_queued(&self) -> anyhow::Result<()> {
        let queued = self
            .jobs
            .list_queued()
            .await?;
        self.update_slack_queue_positions(&queued)
            .await;
        for job in queued {
            if matches!(job.task_kind, TaskKind::BlockValidation) {
                // Block-validation enqueue persists its typed payload in the
                // same transaction as job creation.
                continue;
            }
            if let Err(error) = self
                .prepare_job(job.clone())
                .await
            {
                // One malformed job or transient provider failure must not
                // head-of-line block unrelated queued work.
                tracing::error!(job_id = %job.id, %error, "fleet job preparation failed");
            }
        }
        Ok(())
    }

    async fn prepare_job(&self, mut job: RunnableJob) -> anyhow::Result<()> {
        let commit = self
            .freeze_group_sources(&job)
            .await?;
        job.commit.clone_from(&commit);
        let reporter = Reporter::new_with_dependencies(
            self.config.clone(),
            ReporterDependencies {
                jobs: self.jobs.clone(),
                gh: self.gh.clone(),
                artifact_store: self.artifacts.clone(),
                app_id: self.app_id.clone(),
                slack: self.slack.clone(),
                slack_sessions: self.slack_sessions.clone(),
            },
            job.clone(),
        );
        job = reporter
            .ensure_fleet_reporting()
            .await;
        let payload = payload_for(
            &job,
            &self
                .config
                .stacks_bench
                .default_args,
        )?;
        let payload_hash = sbgh_proto::payload_digest(&payload)?;
        self.fleet
            .prepare_execution(&PreparedExecution {
                job_id: job.id,
                commit,
                payload,
                payload_hash,
                worker_id: None,
            })
            .await?;
        Ok(())
    }

    async fn update_slack_queue_positions(&self, queued: &[RunnableJob]) {
        let in_flight = match self
            .fleet
            .fleet_snapshot()
            .await
        {
            Ok(snapshot) => snapshot.active_attempts as usize,
            Err(error) => {
                tracing::warn!(%error, "slack queue projection skipped: fleet snapshot failed");
                return;
            }
        };
        let total = in_flight + queued.len();
        let mut seen = HashSet::new();
        for (index, job) in queued.iter().enumerate() {
            let ProgressTarget::Slack { plan_message_ts: Some(_), .. } = &job.progress else {
                continue;
            };
            if job.benchmark_run_index != 0 {
                continue;
            }
            let ahead = in_flight + index;
            seen.insert(job.id);
            if self
                .last_queue_positions
                .lock()
                .await
                .get(&job.id)
                == Some(&ahead)
            {
                continue;
            }
            let Some(slack) = &self.slack else {
                continue;
            };
            let updated = crate::slack_report::build_slack_surface(
                slack.clone(),
                self.slack_sessions.clone(),
                self.jobs.clone(),
                self.artifacts.clone(),
                job,
            )
            .queue_position(ahead, total)
            .await;
            if updated {
                self.last_queue_positions
                    .lock()
                    .await
                    .insert(job.id, ahead);
            }
        }
        self.last_queue_positions
            .lock()
            .await
            .retain(|job_id, _| seen.contains(job_id));

        let mut active_groups = queued
            .iter()
            .map(|job| job.benchmark_group_id)
            .collect::<HashSet<_>>();
        match self
            .jobs
            .running_job_ids()
            .await
        {
            Ok(ids) => {
                for id in ids {
                    match self
                        .jobs
                        .load_runnable(id)
                        .await
                    {
                        Ok(Some(job)) => {
                            active_groups.insert(job.benchmark_group_id);
                        }
                        Ok(None) => {}
                        Err(error) => {
                            tracing::warn!(%error, "slack session sweep skipped");
                            return;
                        }
                    }
                }
            }
            Err(error) => {
                tracing::warn!(%error, "slack session sweep skipped");
                return;
            }
        }
        self.slack_sessions
            .sweep_abandoned(Duration::from_secs(300), &active_groups);
    }

    async fn freeze_group_sources(&self, job: &RunnableJob) -> anyhow::Result<String> {
        let specs = self
            .jobs
            .benchmark_group_specs(job.benchmark_group_id)
            .await?;
        anyhow::ensure!(!specs.is_empty(), "job {} has no benchmark specs", job.id);
        let mut sources = Vec::with_capacity(specs.len());
        for spec in specs {
            let commit = if let Some(commit) = spec.git_commit_hash {
                commit
            } else if spec.id == job.benchmark_spec_id
                && let ProgressTarget::PullRequest { pr_number, .. } = &job.progress
            {
                self.gh
                    .pr_head_sha(job.installation_id, &job.repository, *pr_number as u64)
                    .await?
            } else {
                let repo = self
                    .repos
                    .lookup_repo(spec.github_repo_id)
                    .await?
                    .with_context(|| {
                        format!("benchmark spec {} references a missing repository", spec.id)
                    })?;
                let repository = format!("{}/{}", repo.owner, repo.name);
                let reference = match spec.git_ref_kind {
                    GitRefKind::Tag => format!("tags/{}", spec.git_ref_display),
                    GitRefKind::Branch | GitRefKind::Commit => spec.git_ref_display,
                };
                self.gh
                    .resolve_commit(job.installation_id, &repository, &reference)
                    .await?
                    .hash
            };
            sources.push(ResolvedSpecSource { spec_id: spec.id, commit });
        }
        anyhow::ensure!(
            self.fleet
                .freeze_group_sources(job.benchmark_group_id, &sources)
                .await?,
            "benchmark group {} changed while source refs were being frozen",
            job.benchmark_group_id
        );
        sources
            .into_iter()
            .find(|source| source.spec_id == job.benchmark_spec_id)
            .map(|source| source.commit)
            .context("current job's benchmark spec was not frozen")
    }

    async fn project_events(&self) -> anyhow::Result<()> {
        let mut blocked_attempts = HashSet::new();
        for event in self
            .fleet
            .unprojected_events(128)
            .await?
        {
            if blocked_attempts.contains(&event.attempt_id) {
                continue;
            }
            if !self
                .fleet
                .attempt_projection_is_authoritative(event.attempt_id)
                .await?
            {
                // The row may have been fetched immediately before expiry or
                // successor-session fencing discarded it. Recheck before any
                // external side effect rather than trusting the batch snapshot.
                continue;
            }
            let Some(job) = self
                .jobs
                .load_runnable(event.job_id)
                .await?
            else {
                anyhow::bail!("fleet event references missing job {}", event.job_id);
            };
            let surface = self
                .surface(event.attempt_id, &job)
                .await;
            let projection = match &event.payload {
                ReliableEventPayload::Phase { label, .. } if label == "accepted" => surface
                    .started()
                    .await
                    .context("projecting accepted event"),
                ReliableEventPayload::Phase { label, elapsed_ms } => surface
                    .phase(
                        &PhaseLabel::new(label.clone(), false),
                        Duration::from_millis(*elapsed_ms),
                    )
                    .await
                    .context("projecting phase event"),
                ReliableEventPayload::Terminal { .. } => {
                    let Some(outcome) = self
                        .fleet
                        .attempt_terminal_outcome(event.attempt_id)
                        .await?
                    else {
                        // The reliable terminal event may arrive before the
                        // terminal transaction. Keep it unprojected and retry.
                        continue;
                    };
                    let terminal_job = job.clone();
                    let reporter = Reporter::new_with_dependencies(
                        self.config.clone(),
                        ReporterDependencies {
                            jobs: self.jobs.clone(),
                            gh: self.gh.clone(),
                            artifact_store: self.artifacts.clone(),
                            app_id: self.app_id.clone(),
                            slack: self.slack.clone(),
                            slack_sessions: self.slack_sessions.clone(),
                        },
                        job,
                    );
                    self.plan_next_repeat(&terminal_job, &outcome)
                        .await?;
                    reporter
                        .report_fleet_terminal(surface.as_ref(), &outcome)
                        .await
                        .context("projecting terminal event")
                }
            };
            if let Err(error) = projection {
                tracing::warn!(
                    attempt_id = %event.attempt_id,
                    reliable_seq = event.reliable_seq,
                    %error,
                    "durable report projection failed; leaving event pending"
                );
                blocked_attempts.insert(event.attempt_id);
                continue;
            }
            if matches!(event.payload, ReliableEventPayload::Terminal { .. }) {
                self.surfaces
                    .lock()
                    .await
                    .remove(&event.attempt_id);
            }
            self.fleet
                .mark_event_projected(event.attempt_id, event.reliable_seq)
                .await?;
        }
        for progress in self
            .fleet
            .unprojected_progress(128)
            .await?
        {
            if blocked_attempts.contains(&progress.attempt_id) {
                continue;
            }
            if !self
                .fleet
                .attempt_projection_is_authoritative(progress.attempt_id)
                .await?
            {
                continue;
            }
            let Some(job) = self
                .jobs
                .load_runnable(progress.job_id)
                .await?
            else {
                anyhow::bail!("fleet progress references missing job {}", progress.job_id);
            };
            let workflow_step = match progress
                .update
                .workflow_step
                .as_str()
            {
                "calibrate" => crate::report_event::WorkflowStep::Calibrate,
                "run" => crate::report_event::WorkflowStep::Run,
                other => {
                    tracing::warn!(
                        attempt_id = %progress.attempt_id,
                        progress_seq = progress.progress_seq,
                        workflow_step = %other,
                        "unsupported progress workflow step; dropping projection"
                    );
                    self.fleet
                        .mark_progress_projected(progress.attempt_id, progress.progress_seq)
                        .await?;
                    continue;
                }
            };
            let update = crate::report_event::ProgressUpdate {
                workflow_step,
                run_index: progress.update.run_index,
                requested_run_count: progress
                    .update
                    .requested_run_count,
                phase: progress.update.phase,
                progress: progress.update.progress,
                total: progress.update.total,
                message: progress.update.message,
            };
            if let Err(error) = self
                .surface(progress.attempt_id, &job)
                .await
                .progress(&update)
                .await
                .context("projecting progress snapshot")
            {
                tracing::warn!(
                    attempt_id = %progress.attempt_id,
                    progress_seq = progress.progress_seq,
                    %error,
                    "durable progress projection failed; leaving update pending"
                );
                blocked_attempts.insert(progress.attempt_id);
                continue;
            }
            self.fleet
                .mark_progress_projected(progress.attempt_id, progress.progress_seq)
                .await?;
        }
        Ok(())
    }

    async fn plan_next_repeat(
        &self,
        job: &RunnableJob,
        outcome: &sbgh_proto::TerminalOutcome,
    ) -> anyhow::Result<()> {
        if !uses_shared_calibration(job.task_kind, job.build_target, job.group_requested_run_count)
        {
            return Ok(());
        }
        let sbgh_proto::TerminalOutcome::Completed { summary, .. } = outcome else {
            return Ok(());
        };
        let key = summary
            .get("sqlite_archived_path")
            .and_then(serde_json::Value::as_str)
            .context("completed repeat is missing sqlite_archived_path")?;
        let source = self
            .artifacts
            .get(key)
            .await
            .with_context(|| format!("resolving carried SQLite object {key}"))?;
        let size = tokio::fs::metadata(&source)
            .await?
            .len();
        anyhow::ensure!(size > 0, "completed repeat SQLite artifact is empty");
        let group_key = group_artifact_key(&job.group_artifact_prefix, GROUP_SQLITE_RELATIVE);
        self.artifacts
            .put(&group_key, &source)
            .await
            .context("promoting carried repeat SQLite")?;
        self.repeat_jobs
            .append_next_benchmark_run(job.id)
            .await?;
        Ok(())
    }

    async fn surface(&self, attempt_id: Uuid, job: &RunnableJob) -> Arc<dyn ReportSurface> {
        if let Some(surface) = self
            .surfaces
            .lock()
            .await
            .get(&attempt_id)
            .cloned()
        {
            return surface;
        }
        let surface: Arc<dyn ReportSurface> = Arc::from(build_report_surface(
            self.gh.clone(),
            self.jobs.clone(),
            self.artifacts.clone(),
            self.slack.as_ref(),
            &self.slack_sessions,
            job,
        ));
        match self
            .fleet
            .report_projection_seed(attempt_id)
            .await
        {
            Ok(Some(seed)) => surface.restore(&seed).await,
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(
                    %attempt_id,
                    %error,
                    "failed to restore durable reporting projection state"
                );
            }
        }
        let mut surfaces = self.surfaces.lock().await;
        if let Some(existing) = surfaces.get(&attempt_id) {
            return existing.clone();
        }
        surfaces.insert(attempt_id, surface.clone());
        surface
    }
}

fn payload_for(job: &RunnableJob, default_args: &str) -> anyhow::Result<TaskPayload> {
    match (job.task_kind, job.build_target) {
        (TaskKind::Benchmark, BuildTarget::StacksBench) => {
            let resolved = resolve_bench_args(&job.bench_args, default_args);
            anyhow::ensure!(
                job.workload_key.as_deref() == Some(resolved.workload_key.as_str()),
                "job {} effective arguments do not match its enqueue-time workload key",
                job.id
            );
            Ok(TaskPayload::Benchmark(BenchmarkPayload {
                effective_args: resolved.effective_args,
                workload_key: Some(resolved.workload_key),
                sqlite_seed_key: sqlite_seed_key(job),
                shared_baseline_calibration: uses_shared_calibration(
                    job.task_kind,
                    job.build_target,
                    job.group_requested_run_count,
                ),
                baseline_calibration_id: job.baseline_calibration_id,
                run_index: job.benchmark_run_index,
                requested_run_count: job.requested_run_count,
            }))
        }
        (TaskKind::BuildOnly, BuildTarget::StacksBench) => Ok(TaskPayload::BuildOnly),
        (task, target) => {
            anyhow::bail!("unsupported fleet task/build combination: {task:?}/{target:?}")
        }
    }
}

fn sqlite_seed_key(job: &RunnableJob) -> Option<String> {
    if uses_shared_calibration(job.task_kind, job.build_target, job.group_requested_run_count)
        && job.group_run_index > 0
    {
        Some(group_artifact_key(&job.group_artifact_prefix, GROUP_SQLITE_RELATIVE))
    } else {
        None
    }
}
