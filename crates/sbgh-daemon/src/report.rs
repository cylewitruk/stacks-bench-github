//! The job reporting **surface** seam (iteration v7, item 0022).
//!
//! One [`ReportSurface`] per job drives the lifecycle on one target family —
//! the GitHub PR comment + Check Run together ([`GitHubReportSurface`]), the
//! Slack canonical snapshot, or nothing
//! ([`NoopReportSurface`]). [`build_report_surface`] picks the right one from
//! `(ProgressTarget, slack)`. This collapses the old split between
//! `ProgressReporter` (lifecycle) and `ProgressSink` (worker-event drain),
//! which each re-interpreted the `ProgressTarget` separately.
//!
//! Slack benchmark submissions share a daemon-owned projection session whose full
//! state is rendered through `sbgh-slack`; no Slack message is ever parsed or
//! incrementally patched.
//!
//! Projection methods report transport failures to their caller. The fleet
//! projector only acknowledges durable input after the external snapshot
//! converges; legacy inline callers may deliberately log and ignore the error
//! after the benchmark lifecycle has already been persisted.
//!
//! Wired from [`Reporter::run`](crate::reporter) (the lifecycle + the drain
//! loop) and the runner's orphan recovery — both build their surface via
//! [`build_report_surface`], threading the shared `SlackSessionRegistry`.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context as _;
use async_trait::async_trait;
use sbgh_core::db::fleet::ReportProjectionSeed;
use sbgh_github::{CheckRunConclusion, CheckRunOutput, CheckRunState, CheckRunUpdate, GitHubApi};
use tokio::sync::Mutex;

use crate::artifact_store::{ArtifactStore, SUBMISSION_SQLITE_RELATIVE, submission_artifact_key};
use crate::bench_summary::{self, RunResult, thousands};
use crate::comparison::{BaselineComparison, MultiVariantComparison};
use crate::duration::format_elapsed;
use crate::job_source::{ProgressTarget, RunnableJob, RunnableJobStore};
use crate::report_event::{PhaseLabel, ProgressUpdate};
use crate::slack_report::{SlackSessionRegistry, build_slack_surface};
use sbgh_slack::SlackClient;

/// Lifetime of the presigned `stacks-bench.db` download link in a Slack report.
/// Kept in sync with [`bench_summary::DB_LINK_TTL_HUMAN`]; 3 days is under S3's
/// 7-day SigV4 cap and long enough to fetch the DB for local investigation.
const DB_LINK_TTL: Duration = Duration::from_secs(3 * 24 * 60 * 60);

/// Minimum interval between per-phase GitHub edits (comment + check). Phase
/// transitions and heartbeats both go through this debounce; terminal phases
/// bypass it so the final state shows immediately. The first edit after pickup
/// is always allowed through.
const PR_UPDATE_MIN_INTERVAL: Duration = Duration::from_secs(30);

pub(crate) fn pr_report_marker(submission_id: uuid::Uuid) -> String {
    format!("<!-- sbgh-report:{submission_id} -->")
}

pub(crate) fn marked_pr_comment(submission_id: uuid::Uuid, body: &str) -> String {
    let marker = pr_report_marker(submission_id);
    if body
        .lines()
        .any(|line| line.trim() == marker)
    {
        body.to_owned()
    } else {
        format!("{body}\n\n{marker}")
    }
}

/// Successful-run report data passed to surfaces at terminal completion.
/// Surfaces consume only the fields they own: GitHub uses the PR baseline
/// comparison, while Slack uses the multi-variant comparison for v22 ad-hoc
/// comparison submissions.
pub struct CompletionReport<'a> {
    pub summary: &'a serde_json::Value,
    pub baseline_comparison: Option<&'a BaselineComparison>,
    pub multi_variant_comparison: Option<&'a MultiVariantComparison>,
}

/// A job's reporting surface — the whole lifecycle on one target family. Built
/// once per job by [`build_report_surface`]; the reporter drives it (`started`
/// → `phase`/`heartbeat` → one of `completed`/`failed`/`cancelled`).
///
/// Projection errors never change the benchmark outcome. They are returned so
/// a durable projector can leave its input unacknowledged and retry the
/// idempotent snapshot.
#[async_trait]
pub trait ReportSurface: Send + Sync {
    /// Seed in-memory renderer state from already-projected durable history.
    /// Implementations must not emit an external side effect here.
    async fn restore(&self, _seed: &ReportProjectionSeed) {}
    /// Claimed → running.
    async fn started(&self) -> anyhow::Result<()>;
    /// A worker phase transition (`label.is_terminal()` bypasses any debounce).
    async fn phase(&self, label: &PhaseLabel, elapsed: Duration) -> anyhow::Result<()>;
    /// A periodic "still alive" tick within the current phase (best-effort).
    #[cfg(test)]
    async fn heartbeat(&self, label: &PhaseLabel, elapsed: Duration) -> anyhow::Result<()>;
    /// Fine-grained task progress (best-effort).
    async fn progress(&self, progress: &ProgressUpdate) -> anyhow::Result<()>;
    /// Terminal success — the run produced results.
    async fn completed(&self, report: CompletionReport<'_>) -> anyhow::Result<()>;
    /// Terminal failure — the run couldn't run / produce results.
    async fn failed(&self, error: &str) -> anyhow::Result<()>;
    /// Terminal cancellation — deliberately stopped (shutdown / orphan
    /// recovery).
    async fn cancelled(&self, reason: &str) -> anyhow::Result<()>;
}

/// Legacy inline execution persists lifecycle state independently of provider
/// reporting. Keep that policy explicit at its call sites while fleet
/// projection propagates the same error for durable retry.
#[cfg(test)]
pub(crate) fn log_nonfatal_projection(job_id: uuid::Uuid, result: anyhow::Result<()>) {
    if let Err(error) = result {
        tracing::warn!(%job_id, error = ?error, "report projection failed (non-fatal)");
    }
}

/// Build the one reporting surface for `job`: the Slack snapshot for a Slack
/// job with a wired client; an explicit **no-op** for a Slack job *without* one
/// (preserving today's silent degrade — Decision 6 — rather than leaning on a
/// GitHub surface coincidentally no-opping on a comment/check-less target);
/// else the GitHub comment+check surface.
pub fn build_report_surface(
    gh: Arc<dyn GitHubApi>,
    jobs: Arc<dyn RunnableJobStore>,
    store: Arc<dyn ArtifactStore>,
    slack: Option<&Arc<dyn SlackClient>>,
    slack_sessions: &Arc<SlackSessionRegistry>,
    job: &RunnableJob,
) -> Box<dyn ReportSurface> {
    match (&job.progress, slack) {
        (ProgressTarget::Slack { .. }, Some(client)) => {
            Box::new(build_slack_surface(client.clone(), slack_sessions.clone(), jobs, store, job))
        }
        (ProgressTarget::Slack { .. }, None) => Box::new(NoopReportSurface),
        // Build-only/silent jobs (v10 0005) report nothing — the empty surface
        // set, explicit rather than a GitHub surface coincidentally no-opping.
        (ProgressTarget::Silent, _) => Box::new(NoopReportSurface),
        _ => Box::new(GitHubReportSurface::new(gh, job.clone(), store)),
    }
}

// ─────────────────────────── GitHub ───────────────────────────

/// GitHub reporting surface: the PR comment and Check Run **together** (the
/// check URL feeds the comment; the terminal check is owned here, while the
/// phase path keeps it `in_progress`). Absorbs the old `ProgressReporter`
/// GitHub branches and `ProgressSink`'s phase/heartbeat path.
pub struct GitHubReportSurface {
    gh: Arc<dyn GitHubApi>,
    job: RunnableJob,
    store: Arc<dyn ArtifactStore>,
    /// Per-phase edit debounce, shared by comment + check (from
    /// `ProgressSink`).
    phase_state: Mutex<PhaseState>,
}

#[derive(Default)]
struct PhaseState {
    last_update_at: Option<Instant>,
}

impl GitHubReportSurface {
    pub fn new(gh: Arc<dyn GitHubApi>, job: RunnableJob, store: Arc<dyn ArtifactStore>) -> Self {
        Self {
            gh,
            job,
            store,
            phase_state: Mutex::new(PhaseState::default()),
        }
    }

    fn has_comment(&self) -> bool {
        matches!(self.job.progress, ProgressTarget::PullRequest { comment_id: Some(_), .. })
    }

    fn comment_id(&self) -> Option<i64> {
        match self.job.progress {
            ProgressTarget::PullRequest { comment_id, .. } => comment_id,
            // Build-only/silent jobs are routed to `NoopReportSurface`, never
            // here — the arm exists only for exhaustiveness.
            ProgressTarget::CommitCheck { .. }
            | ProgressTarget::Slack { .. }
            | ProgressTarget::Silent => None,
        }
    }

    fn check_run_id(&self) -> Option<i64> {
        match self.job.progress {
            ProgressTarget::PullRequest { check_run_id, .. } => check_run_id,
            ProgressTarget::CommitCheck { check_run_id } => check_run_id,
            ProgressTarget::Slack { .. } | ProgressTarget::Silent => None,
        }
    }

    /// How this job is re-triggered, for the cancelled-check text.
    fn retrigger_hint(&self) -> &'static str {
        match self.job.progress {
            ProgressTarget::PullRequest { .. } => "Re-run with `/benchmark`.",
            ProgressTarget::CommitCheck { .. } => "Re-run by pushing the branch/tag again.",
            ProgressTarget::Slack { .. } => "Re-run by mentioning me with a new `bench …` request.",
            // Unreachable for a silent job (no surface renders this), but the
            // match must cover it.
            ProgressTarget::Silent => "Re-run the build.",
        }
    }

    /// The shared completed render (read + parse the archived `run.json` for
    /// the user-facing metrics) used by both the comment and the check.
    async fn completed_body(
        &self,
        summary: &serde_json::Value,
        comparison: Option<&BaselineComparison>,
    ) -> String {
        if summary
            .get("task")
            .and_then(|value| value.as_str())
            == Some("block_validation")
        {
            let valid = summary
                .get("valid")
                .and_then(|value| value.as_bool())
                .unwrap_or(false);
            let checked = summary
                .get("checked_blocks")
                .and_then(|value| value.as_u64())
                .unwrap_or(0);
            let invalid = summary
                .get("invalid_block_count")
                .and_then(|value| value.as_u64())
                .unwrap_or(0);
            let result = if valid { "passed" } else { "found invalid blocks" };
            return format!(
                "{} block validation **{result}** for commit `{}` — checked `{checked}` blocks, \
                 `{invalid}` invalid.",
                if valid { ":white_check_mark:" } else { ":x:" },
                self.job.commit,
            );
        }
        let archive_dir = summary
            .get("archive_dir")
            .and_then(|v| v.as_str())
            .unwrap_or("/var/lib/sbgh/results");
        let parsed = parsed_run(self.store.as_ref(), summary).await;
        bench_summary::render_pr_comment(
            &self.job.id.to_string(),
            &self.job.commit,
            archive_dir,
            parsed.as_ref(),
            comparison,
        )
    }

    /// Edit the PR comment if this job has one.
    async fn update_comment(&self, body: &str) -> anyhow::Result<()> {
        let ProgressTarget::PullRequest {
            comment_id: Some(comment_id), ..
        } = self.job.progress
        else {
            tracing::debug!(job_id = %self.job.id, "progress (no comment surface)");
            return Ok(());
        };
        self.update_comment_id(comment_id, body)
            .await
    }

    async fn update_comment_id(&self, comment_id: i64, body: &str) -> anyhow::Result<()> {
        let body = marked_pr_comment(self.job.task_submission_id, body);
        self.gh
            .update_pr_comment(self.job.installation_id, &self.job.repository, comment_id, &body)
            .await
            .map_err(anyhow::Error::new)
            .context("updating GitHub PR comment")
            .map(|_| ())
    }

    /// Complete this job's Check Run with `conclusion` if it has one.
    async fn complete_check(
        &self,
        conclusion: CheckRunConclusion,
        output: CheckRunOutput,
    ) -> anyhow::Result<()> {
        let Some(check_run_id) = self.check_run_id() else {
            return Ok(());
        };
        self.gh
            .update_check_run(
                self.job.installation_id,
                &self.job.repository,
                check_run_id,
                CheckRunUpdate {
                    state: CheckRunState::Completed(conclusion),
                    output,
                },
            )
            .await
            .map_err(anyhow::Error::new)
            .context("updating GitHub Check Run")
            .map(|_| ())
    }

    /// Per-phase comment + check update, debounced (from `ProgressSink`). The
    /// reporter owns the check's terminal state, so a terminal phase skips the
    /// check here (no flicker / redundant PATCH).
    async fn try_update(
        &self,
        label: &PhaseLabel,
        elapsed: Duration,
        force: bool,
    ) -> anyhow::Result<()> {
        let comment_id = self.comment_id();
        let check_run_id = if label.is_terminal() { None } else { self.check_run_id() };
        if comment_id.is_none() && check_run_id.is_none() {
            tracing::debug!(job_id = %self.job.id, phase = %label, "phase (no reporting surface)");
            return Ok(());
        }

        // Shared debounce. Brief lock — not held across the network calls.
        {
            let state = self.phase_state.lock().await;
            if !force
                && let Some(last) = state.last_update_at
                && last.elapsed() < PR_UPDATE_MIN_INTERVAL
            {
                tracing::trace!(phase = %label, since_last = ?last.elapsed(), "phase update debounced");
                return Ok(());
            }
        }

        let mut first_error = None;
        if let Some(comment_id) = comment_id {
            let body = format!(
                ":construction: benchmark `{id}` — **{phase}** for `{elapsed}` (commit `{sha}`)",
                id = self.job.id,
                phase = humanize_phase(label),
                elapsed = format_elapsed(elapsed),
                sha = self.job.commit,
            );
            if let Err(error) = self
                .update_comment_id(comment_id, &body)
                .await
            {
                first_error = Some(error.context("updating GitHub phase comment"));
            }
        }

        if let Some(check_run_id) = check_run_id {
            let output = CheckRunOutput {
                title: humanize_phase(label),
                summary: format!(
                    "**{phase}** for `{elapsed}` — commit `{sha}`",
                    phase = humanize_phase(label),
                    elapsed = format_elapsed(elapsed),
                    sha = self.job.commit,
                ),
                text: None,
            };
            if let Err(error) = self
                .gh
                .update_check_run(
                    self.job.installation_id,
                    &self.job.repository,
                    check_run_id,
                    CheckRunUpdate {
                        state: CheckRunState::InProgress,
                        output,
                    },
                )
                .await
            {
                let error = anyhow::Error::new(error).context("updating GitHub phase Check Run");
                if first_error.is_none() {
                    first_error = Some(error);
                } else {
                    tracing::warn!(error = ?error, "additional report projection failure");
                }
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        self.phase_state
            .lock()
            .await
            .last_update_at = Some(Instant::now());
        Ok(())
    }
}

fn combine_projection_results(
    primary: anyhow::Result<()>,
    secondary: anyhow::Result<()>,
) -> anyhow::Result<()> {
    match (primary, secondary) {
        (Ok(()), result) | (result, Ok(())) => result,
        (Err(primary), Err(secondary)) => {
            tracing::warn!(error = ?secondary, "additional report projection failure");
            Err(primary)
        }
    }
}

#[async_trait]
impl ReportSurface for GitHubReportSurface {
    async fn started(&self) -> anyhow::Result<()> {
        self.update_comment(&format!(
            ":rocket: benchmark `{id}` is running on commit `{sha}`.",
            id = self.job.id,
            sha = self.job.commit,
        ))
        .await
    }

    async fn phase(&self, label: &PhaseLabel, elapsed: Duration) -> anyhow::Result<()> {
        let force = label.is_terminal();
        self.try_update(label, elapsed, force)
            .await
    }

    #[cfg(test)]
    async fn heartbeat(&self, label: &PhaseLabel, elapsed: Duration) -> anyhow::Result<()> {
        self.try_update(label, elapsed, false)
            .await
    }

    async fn progress(&self, progress: &ProgressUpdate) -> anyhow::Result<()> {
        let comment_id = self.comment_id();
        let check_run_id = self.check_run_id();
        if comment_id.is_none() && check_run_id.is_none() {
            tracing::debug!(job_id = %self.job.id, phase = %progress.phase, "progress (no reporting surface)");
            return Ok(());
        }

        {
            let state = self.phase_state.lock().await;
            if let Some(last) = state.last_update_at
                && last.elapsed() < PR_UPDATE_MIN_INTERVAL
            {
                tracing::trace!(
                    phase = %progress.phase,
                    since_last = ?last.elapsed(),
                    "progress update debounced",
                );
                return Ok(());
            }
        }

        let summary = format_progress_summary(progress);
        let mut first_error = None;
        if let Some(comment_id) = comment_id {
            let body = format!(
                ":construction: benchmark `{id}` — {summary} (commit `{sha}`)",
                id = self.job.id,
                sha = self.job.commit,
            );
            if let Err(error) = self
                .update_comment_id(comment_id, &body)
                .await
            {
                first_error = Some(error.context("updating GitHub progress comment"));
            }
        }

        if let Some(check_run_id) = check_run_id {
            let output = CheckRunOutput {
                title: "Benchmark progress".into(),
                summary: format!("{summary} — commit `{}`", self.job.commit),
                text: None,
            };
            if let Err(error) = self
                .gh
                .update_check_run(
                    self.job.installation_id,
                    &self.job.repository,
                    check_run_id,
                    CheckRunUpdate {
                        state: CheckRunState::InProgress,
                        output,
                    },
                )
                .await
            {
                let error = anyhow::Error::new(error).context("updating GitHub progress Check Run");
                if first_error.is_none() {
                    first_error = Some(error);
                } else {
                    tracing::warn!(error = ?error, "additional report projection failure");
                }
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        self.phase_state
            .lock()
            .await
            .last_update_at = Some(Instant::now());
        Ok(())
    }

    async fn completed(&self, report: CompletionReport<'_>) -> anyhow::Result<()> {
        let body = self
            .completed_body(report.summary, report.baseline_comparison)
            .await;
        let comment_result = self
            .update_comment(&body)
            .await;
        let pointer = if self.has_comment() { "see comment / details" } else { "see details" };
        let block_validation = report
            .summary
            .get("task")
            .and_then(|value| value.as_str())
            == Some("block_validation");
        let conclusion = if block_validation
            && !report
                .summary
                .get("valid")
                .and_then(|value| value.as_bool())
                .unwrap_or(false)
        {
            CheckRunConclusion::Failure
        } else {
            // A benchmark ran and produced results. Performance is data, not a
            // gate; block validation is a correctness result and can be red.
            CheckRunConclusion::Success
        };
        let check_result = self
            .complete_check(
                conclusion,
                CheckRunOutput {
                    title: format!(
                        "{} {} — complete",
                        if block_validation { "block validation" } else { "benchmark" },
                        self.job.id
                    ),
                    summary: format!("commit `{}` — {pointer}", self.job.commit),
                    text: Some(body),
                },
            )
            .await;
        combine_projection_results(comment_result, check_result)
    }

    async fn failed(&self, error: &str) -> anyhow::Result<()> {
        let snippet = short_pr_error(error);
        let comment_result = self
            .update_comment(&format!(
                ":x: benchmark `{id}` failed: `{snippet}`\n\n_(full details in the daemon logs)_",
                id = self.job.id,
            ))
            .await;
        let check_result = self
            .complete_check(
                CheckRunConclusion::Failure,
                CheckRunOutput {
                    title: format!("benchmark {} — failed", self.job.id),
                    summary: format!("commit `{}` failed", self.job.commit),
                    text: Some(format!(
                        "```\n{snippet}\n```\n\n_(full details in the daemon logs)_"
                    )),
                },
            )
            .await;
        combine_projection_results(comment_result, check_result)
    }

    async fn cancelled(&self, reason: &str) -> anyhow::Result<()> {
        let comment_result = self
            .update_comment(&format!(
                ":no_entry_sign: benchmark `{id}` cancelled: {reason}. Re-run with `/benchmark`.",
                id = self.job.id,
            ))
            .await;
        let hint = self.retrigger_hint();
        let check_result = self
            .complete_check(
                CheckRunConclusion::Cancelled,
                CheckRunOutput {
                    title: format!("benchmark {} — cancelled", self.job.id),
                    summary: format!("commit `{}` — {reason}", self.job.commit),
                    text: Some(format!("Cancelled: {reason}. {hint}")),
                },
            )
            .await;
        combine_projection_results(comment_result, check_result)
    }
}

// ─────────────────────────── No-op ───────────────────────────

/// Reporting surface that does nothing — returned for a Slack target with
/// **no** Slack client wired, preserving today's silent degrade (Decision 6).
pub struct NoopReportSurface;

#[async_trait]
impl ReportSurface for NoopReportSurface {
    async fn started(&self) -> anyhow::Result<()> {
        Ok(())
    }
    async fn phase(&self, _label: &PhaseLabel, _elapsed: Duration) -> anyhow::Result<()> {
        Ok(())
    }
    #[cfg(test)]
    async fn heartbeat(&self, _label: &PhaseLabel, _elapsed: Duration) -> anyhow::Result<()> {
        Ok(())
    }
    async fn progress(&self, _progress: &ProgressUpdate) -> anyhow::Result<()> {
        Ok(())
    }
    async fn completed(&self, _report: CompletionReport<'_>) -> anyhow::Result<()> {
        Ok(())
    }
    async fn failed(&self, _error: &str) -> anyhow::Result<()> {
        Ok(())
    }
    async fn cancelled(&self, _reason: &str) -> anyhow::Result<()> {
        Ok(())
    }
}

// ─────────────────────── shared helpers ───────────────────────

/// Resolve the archived `run.json` store **key** (Decision 0002) → local path →
/// parse. `None` on any missing-key / I/O / parse error. Shared by the GitHub
/// and Slack completed renders.
pub(crate) async fn parsed_run(
    store: &dyn ArtifactStore,
    summary: &serde_json::Value,
) -> Option<RunResult> {
    let run_json_path = match summary
        .get("run_json_archived_path")
        .and_then(|v| v.as_str())
    {
        Some(key) => store.get(key).await.ok(),
        None => None,
    };
    run_json_path
        .as_deref()
        .and_then(read_run_json)
}

/// A presigned download URL for the run's archived `stacks-bench.db`, or
/// `None`. `Some` only in S3 mode with the object **actually in the bucket**
/// (`signed_url_if_fetchable` HEADs it first — the v5 acceptance gate).
pub(crate) async fn signed_db_url(
    store: &dyn ArtifactStore,
    summary: &serde_json::Value,
) -> Option<String> {
    let key = summary
        .get("sqlite_archived_path")
        .and_then(|v| v.as_str())?;
    store
        .signed_url_if_fetchable(key, DB_LINK_TTL)
        .await
}

pub(crate) async fn signed_submission_db_url(
    store: &dyn ArtifactStore,
    submission_prefix: &str,
) -> Option<String> {
    let key = submission_artifact_key(submission_prefix, SUBMISSION_SQLITE_RELATIVE);
    store
        .signed_url_if_fetchable(&key, DB_LINK_TTL)
        .await
}

/// Best-effort read + parse of the archived `run.json`. `None` on any I/O or
/// parse error — a forensics gap never crashes a successful run's report.
fn read_run_json(path: &Path) -> Option<RunResult> {
    match std::fs::read(path) {
        Ok(bytes) => RunResult::from_bytes(&bytes),
        Err(e) => {
            tracing::warn!(error = %e, path = %path.display(), "read archived run.json failed");
            None
        }
    }
}

/// Human display for a phase label on the GitHub surfaces. Binary-cache labels
/// arrive as opaque transport tokens; render them as human text rather than
/// leaking raw digests into the PR comment / check. Any other label is shown
/// verbatim.
fn humanize_phase(label: &PhaseLabel) -> String {
    let name = label.to_string();
    if name.starts_with("build_cached:") {
        "build (cached)".to_string()
    } else if name.starts_with("build_cache_staging:") {
        "build (cached staging)".to_string()
    } else {
        name
    }
}

fn format_progress_summary(progress: &ProgressUpdate) -> String {
    match progress.total {
        Some(total) if total > 0 => {
            format!("{}: {} / {}", progress.phase, thousands(progress.progress), thousands(total))
        }
        _ => format!("{}: {}", progress.phase, thousands(progress.progress)),
    }
}

/// Trim an error chain to something safe to show a PR author: the first
/// non-empty line, the noisy shell-wrapper prefix stripped (prefer what follows
/// `stderr=`), capped at 160 chars with an ellipsis.
pub(crate) fn short_pr_error(error: &str) -> String {
    const MAX_LEN: usize = 160;

    let first = error
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .unwrap_or("(no error message)");

    let stripped = first
        .split_once("stderr=")
        .map(|(_, rhs)| rhs.trim_start())
        .filter(|s| !s.is_empty())
        .unwrap_or(first);

    if stripped.chars().count() <= MAX_LEN {
        stripped.to_string()
    } else {
        let mut out: String = stripped
            .chars()
            .take(MAX_LEN - 1)
            .collect();
        out.push('…');
        out
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex as StdMutex};
    use std::time::Duration;

    use async_trait::async_trait;
    use sbgh_core::db::BenchmarkRunMetric;
    use sbgh_core::models::{GitRefKind, ResolvedCommit};
    use sbgh_github::test_support::{FakeCall, FakeGitHub};
    use sbgh_github::{CheckRunConclusion, CheckRunState};
    use uuid::Uuid;

    use super::*;
    use crate::artifact_store::LocalFsStore;
    use crate::job_source::BaselineRef;

    fn store() -> Arc<dyn ArtifactStore> {
        Arc::new(LocalFsStore::new(std::env::temp_dir()))
    }

    fn completion_report(summary: &serde_json::Value) -> CompletionReport<'_> {
        CompletionReport {
            summary,
            baseline_comparison: None,
            multi_variant_comparison: None,
        }
    }

    fn job_with(progress: ProgressTarget) -> RunnableJob {
        RunnableJob {
            id: Uuid::new_v4(),
            task_submission_id: Uuid::new_v4(),
            task_spec_id: Uuid::new_v4(),
            task_run_index: 0,
            requested_run_count: 1,
            submission_requested_run_count: 1,
            submission_run_index: 0,
            baseline_calibration_id: None,
            submission_artifact_prefix: Uuid::new_v4().to_string(),
            repository: "acme/widgets".into(),
            commit: "abc123".into(),
            git_ref_display: "develop".into(),
            git_ref_kind: GitRefKind::Branch,
            installation_id: 7,
            task_kind: sbgh_core::models::TaskKind::Benchmark,
            build_target: sbgh_core::models::BuildTarget::StacksBench,
            workload_key: None,
            bench_args: vec![],
            progress,
            claim_token: Some(Uuid::new_v4()),
        }
    }

    fn check_job(check_run_id: i64) -> RunnableJob {
        job_with(ProgressTarget::CommitCheck {
            check_run_id: Some(check_run_id),
        })
    }

    fn github(gh: &Arc<FakeGitHub>, job: RunnableJob) -> GitHubReportSurface {
        GitHubReportSurface::new(gh.clone(), job, store())
    }

    fn concluded_state(gh: &FakeGitHub) -> Option<CheckRunState> {
        gh.calls()
            .into_iter()
            .find_map(|c| match c {
                FakeCall::UpdateCheckRun { state, .. } => Some(state),
                _ => None,
            })
    }

    #[test]
    fn pr_report_marker_is_preserved_exactly_once() {
        let submission_id = Uuid::new_v4();
        let marked = marked_pr_comment(submission_id, "snapshot");
        let marker = pr_report_marker(submission_id);
        assert_eq!(
            marked
                .lines()
                .filter(|line| line.trim() == marker)
                .count(),
            1
        );
        assert_eq!(marked_pr_comment(submission_id, &marked), marked);
    }

    // ── GitHub lifecycle (ported from progress.rs) ──

    #[tokio::test]
    async fn github_completed_concludes_check_success() {
        let gh = Arc::new(FakeGitHub::new());
        let summary = serde_json::json!({});
        github(&gh, check_job(11))
            .completed(completion_report(&summary))
            .await
            .unwrap();
        assert_eq!(
            concluded_state(&gh),
            Some(CheckRunState::Completed(CheckRunConclusion::Success))
        );
    }

    #[tokio::test]
    async fn github_failed_concludes_check_failure() {
        let gh = Arc::new(FakeGitHub::new());
        github(&gh, check_job(11))
            .failed("boom: VM died")
            .await
            .unwrap();
        assert_eq!(
            concluded_state(&gh),
            Some(CheckRunState::Completed(CheckRunConclusion::Failure))
        );
    }

    #[tokio::test]
    async fn github_cancelled_concludes_check_cancelled() {
        let gh = Arc::new(FakeGitHub::new());
        github(&gh, check_job(11))
            .cancelled("aborted by shutdown")
            .await
            .unwrap();
        assert_eq!(
            concluded_state(&gh),
            Some(CheckRunState::Completed(CheckRunConclusion::Cancelled))
        );
    }

    /// The cancelled-check re-trigger hint matches how the job re-runs: a
    /// baseline says push-the-ref (never `/benchmark`); a PR job says
    /// `/benchmark`.
    #[tokio::test]
    async fn github_cancelled_text_matches_retrigger_path() {
        let check_text = |gh: &FakeGitHub| -> String {
            gh.calls()
                .into_iter()
                .find_map(|c| match c {
                    FakeCall::UpdateCheckRun { output, .. } => output.text,
                    _ => None,
                })
                .expect("check concluded with text")
        };

        let gh = Arc::new(FakeGitHub::new());
        github(&gh, check_job(11))
            .cancelled("aborted by shutdown")
            .await
            .unwrap();
        let baseline = check_text(&gh);
        assert!(!baseline.contains("/benchmark"), "baseline must not say /benchmark: {baseline}");
        assert!(baseline.contains("push"), "baseline hint mentions pushing: {baseline}");

        let gh_pr = Arc::new(FakeGitHub::new());
        let pr = job_with(ProgressTarget::PullRequest {
            pr_number: 7,
            comment_id: None,
            check_run_id: Some(22),
            check_run_url: None,
        });
        github(&gh_pr, pr)
            .cancelled("aborted by shutdown")
            .await
            .unwrap();
        assert!(check_text(&gh_pr).contains("/benchmark"), "PR job re-runs via /benchmark");
    }

    // ── GitHub phase path (ported from reporter.rs ProgressSink) ──

    #[tokio::test]
    async fn github_phase_sets_check_title_to_phase() {
        let gh = Arc::new(FakeGitHub::new());
        github(&gh, check_job(777))
            .phase(&PhaseLabel::new("building", false), Duration::ZERO)
            .await
            .unwrap();
        let title = gh
            .calls()
            .into_iter()
            .find_map(|c| match c {
                FakeCall::UpdateCheckRun { check_run_id: 777, output, .. } => Some(output.title),
                _ => None,
            })
            .expect("check updated on phase");
        assert_eq!(title, "building");
    }

    #[tokio::test]
    async fn github_phase_updates_both_comment_and_check() {
        let gh = Arc::new(FakeGitHub::new());
        let job = job_with(ProgressTarget::PullRequest {
            pr_number: 7,
            comment_id: Some(800),
            check_run_id: Some(900),
            check_run_url: None,
        });
        let marker = pr_report_marker(job.task_submission_id);
        github(&gh, job)
            .phase(&PhaseLabel::new("running", false), Duration::ZERO)
            .await
            .unwrap();
        let calls = gh.calls();
        assert!(
            calls
                .iter()
                .any(|c| matches!(c, FakeCall::UpdateComment { body, .. }
                    if body.lines().any(|line| line.trim() == marker))),
            "every full-body comment update retains the reconciliation marker"
        );
        assert!(
            calls
                .iter()
                .any(|c| matches!(c, FakeCall::UpdateCheckRun { .. })),
            "check updated"
        );
    }

    #[tokio::test]
    async fn github_projection_propagates_transport_failure_after_attempting_both_surfaces() {
        let gh = Arc::new(FakeGitHub::new());
        gh.fail_update_comment();
        let job = job_with(ProgressTarget::PullRequest {
            pr_number: 7,
            comment_id: Some(800),
            check_run_id: Some(900),
            check_run_url: None,
        });

        let error = github(&gh, job)
            .phase(&PhaseLabel::new("running", false), Duration::ZERO)
            .await
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("phase comment")
        );
        let calls = gh.calls();
        assert!(
            calls
                .iter()
                .any(|call| matches!(call, FakeCall::UpdateComment { .. }))
        );
        assert!(
            calls
                .iter()
                .any(|call| matches!(call, FakeCall::UpdateCheckRun { .. })),
            "a comment failure must not prevent the independently idempotent check update"
        );
    }

    #[tokio::test]
    async fn github_progress_updates_both_comment_and_check() {
        let gh = Arc::new(FakeGitHub::new());
        let job = job_with(ProgressTarget::PullRequest {
            pr_number: 7,
            comment_id: Some(800),
            check_run_id: Some(900),
            check_run_url: None,
        });
        github(&gh, job)
            .progress(&ProgressUpdate {
                workflow_step: crate::report_event::WorkflowStep::Run,
                run_index: 0,
                requested_run_count: 1,
                phase: "replay".into(),
                progress: 42,
                total: Some(100),
                message: Some("Replaying measured entries".into()),
            })
            .await
            .unwrap();

        let calls = gh.calls();
        assert!(
            calls
                .iter()
                .any(|c| matches!(c, FakeCall::UpdateComment { body, .. } if body.contains("replay: 42 / 100"))),
            "comment updated with latest progress"
        );
        assert!(
            calls
                .iter()
                .any(|c| matches!(c, FakeCall::UpdateCheckRun { output, .. } if output.summary.contains("replay: 42 / 100"))),
            "check updated with latest progress"
        );
    }

    #[tokio::test]
    async fn github_phase_skips_check_on_terminal_phase() {
        let gh = Arc::new(FakeGitHub::new());
        github(&gh, check_job(5))
            .phase(&PhaseLabel::new("done", true), Duration::ZERO)
            .await
            .unwrap();
        assert!(
            !gh.calls()
                .iter()
                .any(|c| matches!(c, FakeCall::UpdateCheckRun { .. })),
            "no check churn on a terminal phase"
        );
    }

    /// A minimal store recording only `set_plan_message_ts`.
    #[derive(Default)]
    struct RecordingStore {
        persisted: StdMutex<Vec<String>>,
        metrics: StdMutex<Vec<BenchmarkRunMetric>>,
    }

    #[async_trait]
    impl RunnableJobStore for RecordingStore {
        async fn set_plan_message_ts(&self, _j: &RunnableJob, ts: &str) -> anyhow::Result<()> {
            self.persisted
                .lock()
                .unwrap()
                .push(ts.into());
            Ok(())
        }
        async fn claim_next(&self) -> anyhow::Result<Option<RunnableJob>> {
            Ok(None)
        }
        async fn load_runnable(&self, _id: Uuid) -> anyhow::Result<Option<RunnableJob>> {
            Ok(None)
        }
        async fn list_queued(&self) -> anyhow::Result<Vec<RunnableJob>> {
            Ok(vec![])
        }
        async fn start_running(
            &self,
            _j: &RunnableJob,
            _c: Option<ResolvedCommit>,
        ) -> anyhow::Result<()> {
            Ok(())
        }
        async fn sweep_stuck_claims(&self, _l: chrono::Duration) -> anyhow::Result<u64> {
            Ok(0)
        }
        async fn complete(&self, _j: &RunnableJob, _s: &serde_json::Value) -> anyhow::Result<()> {
            Ok(())
        }
        async fn find_baseline(
            &self,
            _subject_job_id: Uuid,
            _m: &str,
            _b: &str,
            _at: Option<chrono::DateTime<chrono::Utc>>,
            _w: &str,
        ) -> anyhow::Result<Option<BaselineRef>> {
            Ok(None)
        }
        async fn benchmark_run_metrics(
            &self,
            _task_spec_id: Uuid,
        ) -> anyhow::Result<Vec<BenchmarkRunMetric>> {
            Ok(self
                .metrics
                .lock()
                .unwrap()
                .clone())
        }
        async fn fail(
            &self,
            _j: &RunnableJob,
            _e: &str,
            _s: Option<&serde_json::Value>,
        ) -> anyhow::Result<()> {
            Ok(())
        }
        async fn cancel(&self, _j: &RunnableJob, _r: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn set_comment_id(&self, _j: &RunnableJob, _id: i64) -> anyhow::Result<()> {
            Ok(())
        }
        async fn set_check_run(
            &self,
            _j: &RunnableJob,
            _id: i64,
            _u: Option<&str>,
        ) -> anyhow::Result<()> {
            Ok(())
        }
        async fn running_job_ids(&self) -> anyhow::Result<Vec<Uuid>> {
            Ok(vec![])
        }
        async fn cancel_orphan(&self, _id: Uuid, _r: &str) -> anyhow::Result<bool> {
            Ok(false)
        }
    }

    /// A GitHub target routes to the GitHub surface (it concludes the check).
    #[tokio::test]
    async fn factory_github_target_concludes_check() {
        let gh = Arc::new(FakeGitHub::new());
        let jobs: Arc<dyn RunnableJobStore> = Arc::new(RecordingStore::default());
        let surface = build_report_surface(
            gh.clone(),
            jobs,
            store(),
            None,
            &Arc::new(SlackSessionRegistry::new()),
            &check_job(99),
        );
        let summary = serde_json::json!({});
        surface
            .completed(completion_report(&summary))
            .await
            .unwrap();
        assert!(
            gh.calls()
                .iter()
                .any(|c| matches!(c, FakeCall::UpdateCheckRun { check_run_id: 99, .. })),
            "GitHub surface concluded the check"
        );
    }

    // ── short_pr_error (ported home from progress.rs) ──

    #[test]
    fn short_message_unchanged() {
        assert_eq!(short_pr_error("disk full"), "disk full");
    }

    #[test]
    fn first_line_only() {
        let raw = "virsh start sbgh-x failed\nerror: detail\nmore detail";
        assert_eq!(short_pr_error(raw), "virsh start sbgh-x failed");
    }

    #[test]
    fn strips_shell_wrapper_prefix() {
        let raw = "virsh start sbgh-x failed with status exit status: 1: stdout= stderr=error: \
                   Failed to start domain";
        assert_eq!(short_pr_error(raw), "error: Failed to start domain");
    }

    #[test]
    fn truncates_long_single_line_with_ellipsis() {
        let out = short_pr_error(&"a".repeat(500));
        assert!(out.chars().count() <= 160);
        assert!(out.ends_with('…'));
    }

    #[test]
    fn empty_input_has_placeholder() {
        assert_eq!(short_pr_error(""), "(no error message)");
        assert_eq!(short_pr_error("\n\n   \n"), "(no error message)");
    }
}
