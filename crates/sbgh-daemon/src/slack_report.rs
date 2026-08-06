//! Daemon-owned projection from task lifecycle state into Slack snapshots.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use sbgh_core::db::fleet::{ProjectedReportMutation, ReportProjectionSeed};
use sbgh_core::models::TaskKind;
use sbgh_core::reporting::{ReportLifecycleState, SubmissionReportView, TaskReport};
use sbgh_slack::{
    COMPLETED_REACTION, FAILED_REACTION, MessageIdentityStore, PublishUrgency, QUEUED_REACTION,
    RUNNING_REACTION, ReportingIdentity, RunPosition, SlackClient, SlackMessageTarget,
    SlackProgress, SlackProgressView, SlackSnapshotPublisher, SlackStatus,
};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::artifact_store::ArtifactStore;
use crate::comparison::{ComparisonUnavailable, MultiVariantComparison, VariantDelta, Verdict};
use crate::job_source::{ProgressTarget, RunnableJob, RunnableJobStore};
use crate::report::{
    CompletionReport, ReportSurface, parsed_run, short_pr_error, signed_db_url,
    signed_submission_db_url,
};
use crate::report_event::{PhaseLabel, ProgressUpdate};

const RUN_VERSION_STRIDE: u64 = 1_000_000;

struct RunnableMessageIdentityStore {
    jobs: Arc<dyn RunnableJobStore>,
    job: RunnableJob,
}

#[async_trait]
impl MessageIdentityStore for RunnableMessageIdentityStore {
    async fn persist_message_ts(&self, message_ts: &str) -> sbgh_slack::Result<()> {
        self.jobs
            .set_plan_message_ts(&self.job, message_ts)
            .await
            .map_err(|error| sbgh_slack::SlackError::Persistence(error.to_string()))
    }
}

struct SnapshotSession {
    publisher: Arc<SlackSnapshotPublisher>,
    view: Mutex<SlackProgressView>,
    last_touched: StdMutex<Instant>,
}

impl SnapshotSession {
    fn new(publisher: Arc<SlackSnapshotPublisher>, view: SlackProgressView) -> Self {
        Self {
            publisher,
            view: Mutex::new(view),
            last_touched: StdMutex::new(Instant::now()),
        }
    }

    fn touch(&self) {
        *self
            .last_touched
            .lock()
            .unwrap() = Instant::now();
    }

    fn idle_for(&self) -> Duration {
        self.last_touched
            .lock()
            .unwrap()
            .elapsed()
    }

    async fn try_mutate(
        &self,
        urgency: PublishUrgency,
        update: impl FnOnce(&mut SlackProgressView) -> bool,
    ) -> sbgh_slack::Result<bool> {
        self.touch();
        let snapshot = {
            let mut view = self.view.lock().await;
            let previous = view.version;
            if !update(&mut view) {
                return Ok(false);
            }
            view.version = view
                .version
                .max(previous)
                .saturating_add(1);
            view.clone()
        };
        self.publisher
            .publish(snapshot, urgency)
            .await?;
        Ok(true)
    }

    async fn mutate(
        &self,
        urgency: PublishUrgency,
        update: impl FnOnce(&mut SlackProgressView),
    ) -> sbgh_slack::Result<()> {
        self.try_mutate(urgency, |view| {
            update(view);
            true
        })
        .await?;
        Ok(())
    }
}

/// Submission-scoped snapshot sessions. Submissions execute serially, so a session is
/// the single projection owner across all variants and repetitions.
#[derive(Default)]
pub struct SlackSessionRegistry {
    sessions: StdMutex<HashMap<(Uuid, SlackMessageTarget), Arc<SnapshotSession>>>,
}

impl SlackSessionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    fn get_or_create(
        &self,
        submission_id: Uuid,
        target: SlackMessageTarget,
        create: impl FnOnce() -> SnapshotSession,
    ) -> Arc<SnapshotSession> {
        let session = self
            .sessions
            .lock()
            .unwrap()
            .entry((submission_id, target))
            .or_insert_with(|| Arc::new(create()))
            .clone();
        session.touch();
        session
    }

    fn reap(&self, submission_id: Uuid, target: &SlackMessageTarget) {
        self.sessions
            .lock()
            .unwrap()
            .remove(&(submission_id, target.clone()));
    }

    pub fn sweep_abandoned(&self, grace: Duration, active_submissions: &HashSet<Uuid>) -> usize {
        let mut sessions = self.sessions.lock().unwrap();
        let stale: Vec<_> = sessions
            .iter()
            .filter(|((submission, _), session)| {
                !active_submissions.contains(submission) && session.idle_for() > grace
            })
            .map(|(key, _)| key.clone())
            .collect();
        for key in &stale {
            sessions.remove(key);
        }
        stale.len()
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.sessions
            .lock()
            .unwrap()
            .len()
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub fn build_slack_surface(
    client: Arc<dyn SlackClient>,
    sessions: Arc<SlackSessionRegistry>,
    jobs: Arc<dyn RunnableJobStore>,
    store: Arc<dyn ArtifactStore>,
    job: &RunnableJob,
) -> SlackReportSurface {
    let ProgressTarget::Slack {
        channel,
        message_ts,
        plan_message_ts,
        reporting_identity,
    } = &job.progress
    else {
        unreachable!("Slack surface requires a Slack target");
    };
    let identity = ReportingIdentity::parse(reporting_identity.clone())
        .unwrap_or_else(|_| ReportingIdentity::for_request("", channel, message_ts));
    let target = SlackMessageTarget {
        channel: channel.clone(),
        thread_ts: message_ts.clone(),
    };
    let identity_store: Arc<dyn MessageIdentityStore> = Arc::new(RunnableMessageIdentityStore {
        jobs: jobs.clone(),
        job: job.clone(),
    });
    let initial = initial_view(job, identity.clone());
    let session = sessions.get_or_create(job.task_submission_id, target.clone(), || {
        SnapshotSession::new(
            SlackSnapshotPublisher::new(
                client.clone(),
                target.clone(),
                identity,
                plan_message_ts.clone(),
                Some(identity_store),
            ),
            initial,
        )
    });
    SlackReportSurface {
        client,
        sessions,
        session,
        target,
        job: job.clone(),
        jobs,
        store,
    }
}

fn initial_view(job: &RunnableJob, identity: ReportingIdentity) -> SlackProgressView {
    let title = match job.task_kind {
        TaskKind::Benchmark => "Benchmark",
        TaskKind::BlockValidation => "Block validation",
        TaskKind::BuildOnly => "Build",
    };
    let mut view = SlackProgressView::queued(identity, title, &job.git_ref_display);
    view.commit = (!job.commit.is_empty()).then(|| job.commit.clone());
    view.run = (job.submission_requested_run_count > 1).then_some(RunPosition {
        current: (job.submission_run_index + 1).max(1) as u32,
        total: job
            .submission_requested_run_count
            .max(1) as u32,
    });
    view.version = run_version_base(job);
    view
}

fn run_version_base(job: &RunnableJob) -> u64 {
    (job.submission_run_index
        .max(0) as u64
        + 1)
    .saturating_mul(RUN_VERSION_STRIDE)
}

pub struct SlackReportSurface {
    client: Arc<dyn SlackClient>,
    sessions: Arc<SlackSessionRegistry>,
    session: Arc<SnapshotSession>,
    target: SlackMessageTarget,
    job: RunnableJob,
    jobs: Arc<dyn RunnableJobStore>,
    store: Arc<dyn ArtifactStore>,
}

impl SlackReportSurface {
    /// Refresh a pre-claim queue position through the same versioned session
    /// used by the reporter. If claim/start wins the race, the status check
    /// refuses to project queued state over a newer lifecycle phase.
    pub async fn queue_position(&self, ahead: usize, total: usize) -> bool {
        match self
            .session
            .try_mutate(PublishUrgency::Immediate, |view| {
                if view.status != SlackStatus::Queued {
                    return false;
                }
                view.phase = Some(format!("queue position {}/{}", ahead + 1, total));
                true
            })
            .await
        {
            Ok(updated) => updated,
            Err(error) => {
                tracing::warn!(
                    job_id = %self.job.id,
                    error = ?error,
                    "queue-position: Slack snapshot update failed (non-fatal)"
                );
                false
            }
        }
    }

    async fn react(&self, remove: &str, add: &str) {
        if let Err(error) = self
            .client
            .remove_reaction(&self.target.channel, &self.target.thread_ts, remove)
            .await
        {
            tracing::debug!(error = ?error, reaction = remove, "slack: reaction removal failed");
        }
        if let Err(error) = self
            .client
            .add_reaction(&self.target.channel, &self.target.thread_ts, add)
            .await
        {
            tracing::warn!(error = ?error, reaction = add, "slack: reaction add failed");
        }
    }

    async fn terminal(
        &self,
        status: SlackStatus,
        details: Vec<String>,
        links: Vec<(String, String)>,
        reaction: &str,
    ) -> anyhow::Result<()> {
        self.session
            .mutate(PublishUrgency::Immediate, |view| {
                view.status = status;
                view.phase = None;
                view.progress = None;
                view.details = details;
                view.links = links;
            })
            .await
            .map_err(anyhow::Error::new)?;
        self.react(RUNNING_REACTION, reaction)
            .await;
        self.sessions
            .reap(self.job.task_submission_id, &self.target);
        Ok(())
    }

    async fn repeat_details(&self) -> Vec<String> {
        match self
            .jobs
            .benchmark_run_metrics(self.job.task_spec_id)
            .await
        {
            Ok(metrics) => {
                let mut values: Vec<i64> = metrics
                    .iter()
                    .map(|run| {
                        run.metric
                            .execution_duration_us
                            + run.metric.commit_duration_us
                    })
                    .collect();
                values.sort_unstable();
                if values.is_empty() {
                    return vec!["No promoted repeat metrics available".into()];
                }
                let mean = values
                    .iter()
                    .map(|value| *value as f64)
                    .sum::<f64>()
                    / values.len() as f64;
                vec![
                    format!(
                        "Samples: {}/{}",
                        values.len(),
                        self.job
                            .requested_run_count
                            .max(0)
                    ),
                    format!("Execution+Commit mean: {}", format_us(mean)),
                    format!(
                        "Range: {}–{}",
                        format_us(values[0] as f64),
                        format_us(values[values.len() - 1] as f64)
                    ),
                ]
            }
            Err(error) => {
                tracing::warn!(error = ?error, "slack: loading repeat metrics failed");
                Vec::new()
            }
        }
    }
}

#[async_trait]
impl ReportSurface for SlackReportSurface {
    async fn restore(&self, seed: &ReportProjectionSeed) {
        let mut view = self.session.view.lock().await;
        *view = initial_view(&self.job, view.identity.clone());
        view.version = run_version_base(&self.job).saturating_add(seed.mutation_count);
        match &seed.latest {
            ProjectedReportMutation::Phase(sbgh_fleet::ReliableEventPayload::Phase {
                label,
                ..
            }) if label == "accepted" => {
                view.status = SlackStatus::Preparing;
                view.phase = Some("preparing execution".into());
                view.progress = None;
            }
            ProjectedReportMutation::Phase(sbgh_fleet::ReliableEventPayload::Phase {
                label,
                ..
            }) => {
                view.status = SlackStatus::Running;
                view.phase = Some(human_phase(&PhaseLabel::new(label.clone(), false)));
                view.progress = None;
            }
            ProjectedReportMutation::Phase(sbgh_fleet::ReliableEventPayload::Terminal {
                ..
            }) => {
                tracing::warn!(
                    job_id = %self.job.id,
                    "terminal event unexpectedly appeared in non-terminal projection seed"
                );
            }
            ProjectedReportMutation::Progress(progress) => {
                view.status = SlackStatus::Running;
                view.phase = None;
                view.progress = Some(SlackProgress {
                    label: progress.phase.clone(),
                    current: progress.progress,
                    total: progress.total,
                });
            }
        }
    }

    async fn started(&self) -> anyhow::Result<()> {
        let job = &self.job;
        self.session
            .mutate(PublishUrgency::Immediate, |view| {
                *view = initial_view(job, view.identity.clone());
                view.version = run_version_base(job);
                view.status = SlackStatus::Preparing;
                view.phase = Some("preparing execution".into());
            })
            .await
            .map_err(anyhow::Error::new)?;
        self.react(QUEUED_REACTION, RUNNING_REACTION)
            .await;
        Ok(())
    }

    async fn phase(&self, label: &PhaseLabel, _elapsed: Duration) -> anyhow::Result<()> {
        let phase = human_phase(label);
        self.session
            .mutate(PublishUrgency::Immediate, |view| {
                view.status = SlackStatus::Running;
                view.phase = Some(phase);
                view.progress = None;
            })
            .await
            .map_err(anyhow::Error::new)
    }

    #[cfg(test)]
    async fn heartbeat(&self, label: &PhaseLabel, _elapsed: Duration) -> anyhow::Result<()> {
        let phase = human_phase(label);
        self.session
            .mutate(PublishUrgency::Debounced, |view| {
                view.phase = Some(phase);
            })
            .await
            .map_err(anyhow::Error::new)
    }

    async fn progress(&self, progress: &ProgressUpdate) -> anyhow::Result<()> {
        let progress = SlackProgress {
            label: progress.phase.clone(),
            current: progress.progress,
            total: progress.total,
        };
        self.session
            // Fine progress is explicitly best-effort and may coalesce behind
            // the bounded Slack update interval. Reliable lifecycle phases
            // and terminals use Immediate and propagate transport failure.
            .mutate(PublishUrgency::Debounced, |view| {
                view.status = SlackStatus::Running;
                view.phase = None;
                view.progress = Some(progress);
            })
            .await
            .map_err(anyhow::Error::new)
    }

    async fn completed(&self, report: CompletionReport<'_>) -> anyhow::Result<()> {
        if !matches!(
            report
                .snapshot
                .lifecycle
                .state,
            ReportLifecycleState::Completed
        ) {
            self.session
                .mutate(PublishUrgency::Immediate, |view| {
                    view.status = SlackStatus::Queued;
                    view.phase = Some("run complete; scheduling next run".into());
                    view.progress = None;
                })
                .await
                .map_err(anyhow::Error::new)?;
            return Ok(());
        }

        match &report.snapshot.task {
            TaskReport::BlockValidation(result) => {
                let status = if result.is_valid() == Some(true) {
                    SlackStatus::Completed
                } else {
                    SlackStatus::Failed
                };
                return self
                    .terminal(
                        status,
                        block_validation_details(result),
                        Vec::new(),
                        if result.is_valid() == Some(true) {
                            COMPLETED_REACTION
                        } else {
                            FAILED_REACTION
                        },
                    )
                    .await;
            }
            TaskReport::BuildOnly(_) => return Ok(()),
            TaskReport::Benchmark(_) => {}
        }

        let mut details = if let Some(comparison) = report.multi_variant_comparison {
            comparison_details(comparison)
        } else if self.job.requested_run_count > 1 {
            self.repeat_details().await
        } else {
            parsed_run(self.store.as_ref(), report.summary)
                .await
                .map_or_else(
                    || vec!["Run completed; parsed metrics unavailable".into()],
                    |result| run_details(&result),
                )
        };
        if details.is_empty() {
            details.push("Benchmark completed".into());
        }

        let mut links = Vec::new();
        let db_url = signed_db_url(self.store.as_ref(), report.summary).await;
        let db_url = match db_url {
            Some(url) => Some(url),
            None if self
                .job
                .submission_requested_run_count
                > 1 =>
            {
                signed_submission_db_url(
                    self.store.as_ref(),
                    &self
                        .job
                        .submission_artifact_prefix,
                )
                .await
            }
            None => None,
        };
        if let Some(url) = db_url {
            links.push(("Download profiler data".into(), url));
        }
        self.terminal(SlackStatus::Completed, details, links, COMPLETED_REACTION)
            .await
    }

    async fn failed(&self, snapshot: &SubmissionReportView, error: &str) -> anyhow::Result<()> {
        let mut details = vec![short_pr_error(error)];
        if matches!(&snapshot.task, TaskReport::Benchmark(_))
            && self
                .job
                .submission_requested_run_count
                > 1
        {
            details.extend(self.repeat_details().await);
        }
        self.terminal(SlackStatus::Failed, details, Vec::new(), FAILED_REACTION)
            .await
    }

    async fn cancelled(
        &self,
        _snapshot: &SubmissionReportView,
        reason: &str,
    ) -> anyhow::Result<()> {
        self.terminal(
            SlackStatus::Cancelled,
            vec![short_pr_error(reason)],
            Vec::new(),
            FAILED_REACTION,
        )
        .await
    }
}

fn human_phase(label: &PhaseLabel) -> String {
    let phase = label.to_string();
    if phase.starts_with("build_cached:") {
        "build (cached)".into()
    } else if phase.starts_with("build_cache_staging:") {
        "build (cached staging)".into()
    } else {
        phase
    }
}

fn run_details(result: &crate::bench_summary::RunResult) -> Vec<String> {
    let Some(data) = result.run_data() else {
        return vec!["Run completed; parsed metrics unavailable".into()];
    };
    let mut details = Vec::new();
    if let Some(blocks) = data
        .measured_blocks
        .or(data.blocks)
    {
        details.push(format!("Measured blocks: {blocks}"));
    }
    if let Some(summary) = data.summary.as_ref() {
        if let (Some(execution), Some(commit)) =
            (summary.execution_duration_us, summary.commit_duration_us)
        {
            details.push(format!("Execution+Commit: {}", format_us((execution + commit) as f64)));
        }
        if let Some(transactions) = summary.transactions {
            details.push(format!("Transactions: {transactions}"));
        }
    }
    details
}

fn block_validation_details(
    result: &sbgh_core::reporting::BlockValidationReportView,
) -> Vec<String> {
    let mut details = vec![
        match result.is_valid() {
            Some(true) => "Verdict: valid".into(),
            Some(false) => "Verdict: invalid".into(),
            None => "Verdict: unavailable".into(),
        },
        format!(
            "Checked blocks: {}",
            result
                .checked_blocks
                .unwrap_or(0)
        ),
        match &result.requested {
            Some(sbgh_core::reporting::BlockValidationSelectionReport::Recent { block_count }) => {
                format!("Requested: latest {block_count} Nakamoto blocks")
            }
            Some(sbgh_core::reporting::BlockValidationSelectionReport::Full) => {
                "Requested: full observed history".into()
            }
            Some(sbgh_core::reporting::BlockValidationSelectionReport::Range { range }) => {
                format!("Requested: explicit range {}..={}", range.start, range.end)
            }
            None => "Requested: unavailable".into(),
        },
        match result.observed {
            Some(coverage) => format!(
                "Observed: {} pre-Nakamoto + {} Nakamoto",
                coverage.pre_nakamoto_count, coverage.nakamoto_count
            ),
            None => "Observed: unavailable".into(),
        },
        match result.resolved_range {
            Some(range) => format!("Resolved range: {}..={}", range.start, range.end),
            None => "Resolved range: unavailable".into(),
        },
        format!(
            "Execution: {} shards / {} concurrent",
            result
                .shard_count
                .unwrap_or(0),
            result
                .max_concurrency
                .unwrap_or(0)
        ),
        format!(
            "Chainstate: {}",
            slack_safe_detail(
                result
                    .chainstate_origin
                    .as_deref()
                    .unwrap_or("unknown"),
                160
            )
        ),
    ];
    details.extend(
        result
            .invalid_blocks
            .iter()
            .take(12)
            .map(|invalid| {
                format!(
                    "Shard {} block {}: {}",
                    invalid.shard,
                    slack_safe_detail(&invalid.block, 96),
                    slack_safe_detail(&invalid.reason, 180)
                )
            }),
    );
    if result.invalid_blocks.len() > 12 {
        details.push(format!(
            "{} more invalid blocks; use the authenticated report API for full detail",
            result.invalid_blocks.len() - 12
        ));
    }
    details
}

fn slack_safe_detail(value: &str, max_chars: usize) -> String {
    let mut output = value
        .chars()
        .filter(|character| !character.is_control())
        .take(max_chars)
        .collect::<String>()
        .replace("```", "ʼʼʼ")
        .replace('<', "‹")
        .replace('>', "›")
        .replace('@', "＠");
    if value
        .chars()
        .filter(|character| !character.is_control())
        .count()
        > max_chars
    {
        output.push('…');
    }
    output
}

fn comparison_details(comparison: &MultiVariantComparison) -> Vec<String> {
    let mut details = vec![format!(
        "Baseline {}: {}/{} samples{}",
        comparison
            .baseline
            .ref_display,
        comparison
            .baseline
            .stats
            .completed,
        comparison
            .baseline
            .stats
            .requested
            .max(0),
        comparison
            .baseline
            .stats
            .combined_mean_us
            .map(|value| format!(", {} mean", format_us(value)))
            .unwrap_or_default()
    )];
    details.extend(
        comparison
            .variants
            .iter()
            .map(variant_detail),
    );
    details
}

fn variant_detail(delta: &VariantDelta) -> String {
    let samples = format!(
        "{}/{} samples",
        delta.variant.stats.completed,
        delta
            .variant
            .stats
            .requested
            .max(0)
    );
    let result = match delta.comparison {
        Some(comparison) => {
            let direction = if comparison.delta_pct > 0.05 {
                "slower"
            } else if comparison.delta_pct < -0.05 {
                "faster"
            } else {
                "no change"
            };
            let confidence = match comparison.sigma {
                Some(sigma) if sigma.is_finite() => {
                    format!(", {} ({sigma:.1}σ)", verdict(comparison.verdict))
                }
                Some(_) => format!(", {} (∞σ)", verdict(comparison.verdict)),
                None => format!(", {}", verdict(comparison.verdict)),
            };
            format!("{:+.2}% {direction}{confidence}", comparison.delta_pct)
        }
        None => unavailable(delta.unavailable).into(),
    };
    format!("{}: {samples}, {result}", delta.variant.ref_display)
}

fn verdict(verdict: Verdict) -> &'static str {
    match verdict {
        Verdict::Strong => "strong",
        Verdict::Moderate => "moderate",
        Verdict::Weak => "weak",
        Verdict::Inconclusive => "inconclusive",
        Verdict::Provisional => "provisional",
    }
}

fn unavailable(reason: Option<ComparisonUnavailable>) -> &'static str {
    match reason {
        Some(ComparisonUnavailable::MissingBaselineMetric) => "missing baseline metrics",
        Some(ComparisonUnavailable::MissingVariantMetric) => "missing metrics",
        Some(ComparisonUnavailable::IncomparableWorkload) => "incomparable workload",
        Some(ComparisonUnavailable::DegenerateBaseline) => "invalid baseline",
        None => "comparison unavailable",
    }
}

fn format_us(value: f64) -> String {
    format!("{value:.0} µs")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::comparison::{Comparison, ComparisonVariant, VariantRunStats};

    #[test]
    fn registry_reaps_only_inactive_stale_submissions() {
        let registry = SlackSessionRegistry::new();
        let submission = Uuid::new_v4();
        let target = SlackMessageTarget {
            channel: "C1".into(),
            thread_ts: "1.2".into(),
        };
        let identity = ReportingIdentity::for_request("T1", "C1", "1.2");
        let client = Arc::new(sbgh_slack::test_support::FakeSlackClient::default());
        let session = registry.get_or_create(submission, target.clone(), || {
            SnapshotSession::new(
                SlackSnapshotPublisher::new(client, target, identity.clone(), None, None),
                SlackProgressView::queued(identity, "Benchmark", "main"),
            )
        });

        assert_eq!(
            registry.sweep_abandoned(Duration::from_secs(60), &HashSet::new()),
            0,
            "fresh inactive sessions remain"
        );
        *session
            .last_touched
            .lock()
            .unwrap() = Instant::now() - Duration::from_secs(120);
        assert_eq!(
            registry.sweep_abandoned(Duration::from_secs(60), &HashSet::from([submission])),
            0,
            "active stale sessions remain"
        );
        assert_eq!(
            registry.sweep_abandoned(Duration::from_secs(60), &HashSet::new()),
            1,
            "inactive stale sessions are reaped"
        );
        assert!(registry.is_empty());
    }

    fn variant(name: &str, completed: usize, mean: f64) -> ComparisonVariant {
        ComparisonVariant {
            task_spec_id: Uuid::new_v4(),
            spec_index: 0,
            ref_display: name.into(),
            commit: None,
            baseline_calibration_id: None,
            stats: VariantRunStats {
                requested: 3,
                completed,
                combined_mean_us: Some(mean),
                combined_min_us: Some(mean as i64),
                combined_max_us: Some(mean as i64),
                combined_stddev_us: Some(0.0),
                combined_cv_pct: Some(0.0),
                workload: None,
                workload_consistent: true,
            },
        }
    }

    #[test]
    fn compact_comparison_details_preserve_samples_delta_and_verdict() {
        let comparison = MultiVariantComparison {
            baseline: variant("main", 3, 1_000.0),
            variants: vec![VariantDelta {
                variant: variant("candidate", 2, 1_125.0),
                comparison: Some(Comparison {
                    base_combined_us: 1_000,
                    pr_combined_us: 1_125,
                    delta_pct: 12.5,
                    sigma: Some(3.2),
                    verdict: Verdict::Strong,
                }),
                unavailable: None,
            }],
        };

        assert_eq!(
            comparison_details(&comparison),
            vec![
                "Baseline main: 3/3 samples, 1000 µs mean",
                "candidate: 2/3 samples, +12.50% slower, strong (3.2σ)",
            ]
        );
    }

    #[test]
    fn validation_details_bound_and_escape_block_identity() {
        let result = sbgh_core::reporting::BlockValidationReportView {
            requested: None,
            observed: None,
            resolved_range: None,
            segments: Vec::new(),
            shard_count: None,
            max_concurrency: None,
            verdict: Some(sbgh_core::reporting::BlockValidationVerdict::Invalid),
            checked_blocks: Some(1),
            chainstate_origin: Some("nightly".into()),
            invalid_blocks: vec![sbgh_core::reporting::InvalidBlockReport {
                shard: 0,
                block: format!("<@team>```{}", "b".repeat(300)),
                reason: "invalid".into(),
            }],
        };

        let details = block_validation_details(&result).join("\n");
        assert!(!details.contains("<@team>"));
        assert!(!details.contains("```"));
        assert!(!details.contains(&"b".repeat(100)));
        assert!(details.len() < 1_000);
    }
}
