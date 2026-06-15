//! The job reporting **surface** seam (iteration v7, item 0022).
//!
//! One [`ReportSurface`] per job owns the *whole* lifecycle on one target
//! family — the GitHub PR comment + Check Run together
//! ([`GitHubReportSurface`]), the Slack live card ([`SlackReportSurface`]), or
//! nothing ([`NoopReportSurface`]). [`build_report_surface`] picks the right
//! one from `(ProgressTarget, slack)`. This collapses the old split between
//! `ProgressReporter` (lifecycle) and `ProgressSink` (worker-event drain),
//! which each re-interpreted the `ProgressTarget` separately.
//!
//! Every method is **non-fatal**: an impl logs and swallows its own transport
//! errors (a reporting failure never fails the benchmark) — hence `()` returns.
//!
//! Wired from [`Reporter::run`](crate::reporter) (the lifecycle + the drain
//! loop) and the runner's orphan recovery — both build their one surface via
//! [`build_report_surface`].

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use sbgh_core::github::{
    CheckRunConclusion, CheckRunOutput, CheckRunState, CheckRunUpdate, GitHubApi,
};
use tokio::sync::Mutex;

use crate::artifact_store::ArtifactStore;
use crate::bench_summary::{self, RunResult};
use crate::comparison::BaselineComparison;
use crate::events::PhaseLabel;
use crate::job_source::{ProgressTarget, RunnableJob, RunnableJobStore};
use crate::libvirt::format_elapsed;
use crate::slack::client::SlackClient;
use crate::slack::timeline::{SlackTimeline, stage_for_phase};

/// Lifetime of the presigned `stacks-bench.db` download link in a Slack card.
/// Kept in sync with [`bench_summary::DB_LINK_TTL_HUMAN`]; 3 days is under S3's
/// 7-day SigV4 cap and long enough to fetch the DB for local investigation.
const DB_LINK_TTL: Duration = Duration::from_secs(3 * 24 * 60 * 60);

/// Minimum interval between per-phase GitHub edits (comment + check). Phase
/// transitions and heartbeats both go through this debounce; terminal phases
/// bypass it so the final state shows immediately. The first edit after pickup
/// is always allowed through.
const PR_UPDATE_MIN_INTERVAL: Duration = Duration::from_secs(30);

/// A job's reporting surface — the whole lifecycle on one target family. Built
/// once per job by [`build_report_surface`]; the reporter drives it (`started`
/// → `phase`/`heartbeat` → one of `completed`/`failed`/`cancelled`).
///
/// Every method is **non-fatal**: an impl logs and swallows its own transport
/// errors (a reporting failure never fails the benchmark). Hence `()` returns,
/// not `Result`.
#[async_trait]
pub trait ReportSurface: Send + Sync {
    /// Claimed → running.
    async fn started(&self);
    /// A worker phase transition (`label.is_terminal()` bypasses any debounce).
    async fn phase(&self, label: &PhaseLabel, elapsed: Duration);
    /// A periodic "still alive" tick within the current phase (best-effort).
    async fn heartbeat(&self, label: &PhaseLabel, elapsed: Duration);
    /// Terminal success — the run produced results.
    async fn completed(&self, summary: &serde_json::Value, comparison: Option<&BaselineComparison>);
    /// Terminal failure — the run couldn't run / produce results.
    async fn failed(&self, error: &str);
    /// Terminal cancellation — deliberately stopped (shutdown / orphan
    /// recovery).
    async fn cancelled(&self, reason: &str);
}

/// Build the one reporting surface for `job`: the Slack live card for a Slack
/// job with a wired client; an explicit **no-op** for a Slack job *without* one
/// (preserving today's silent degrade — Decision 6 — rather than leaning on a
/// GitHub surface coincidentally no-opping on a comment/check-less target);
/// else the GitHub comment+check surface.
pub fn build_report_surface(
    gh: Arc<dyn GitHubApi>,
    jobs: Arc<dyn RunnableJobStore>,
    store: Arc<dyn ArtifactStore>,
    slack: Option<&Arc<dyn SlackClient>>,
    job: &RunnableJob,
) -> Box<dyn ReportSurface> {
    match (&job.progress, slack) {
        (
            ProgressTarget::Slack {
                channel,
                message_ts,
                plan_message_ts,
            },
            Some(client),
        ) => Box::new(SlackReportSurface::new(
            SlackTimeline::new(
                client.clone(),
                jobs,
                job.clone(),
                channel.clone(),
                message_ts.clone(),
                plan_message_ts.clone(),
            ),
            store,
        )),
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

    /// Edit the PR comment if this job has one. Non-fatal.
    async fn update_comment(&self, body: &str) {
        let ProgressTarget::PullRequest {
            comment_id: Some(comment_id), ..
        } = self.job.progress
        else {
            tracing::debug!(job_id = %self.job.id, "progress (no comment surface)");
            return;
        };
        if let Err(e) = self
            .gh
            .update_pr_comment(self.job.installation_id, &self.job.repository, comment_id, body)
            .await
        {
            tracing::warn!(job_id = %self.job.id, error = ?e, "update PR comment failed (non-fatal)");
        }
    }

    /// Complete this job's Check Run with `conclusion` if it has one.
    /// Non-fatal.
    async fn complete_check(&self, conclusion: CheckRunConclusion, output: CheckRunOutput) {
        let Some(check_run_id) = self.check_run_id() else {
            return;
        };
        if let Err(e) = self
            .gh
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
        {
            tracing::warn!(job_id = %self.job.id, error = ?e, "update Check Run failed (non-fatal)");
        }
    }

    /// Per-phase comment + check update, debounced (from `ProgressSink`). The
    /// reporter owns the check's terminal state, so a terminal phase skips the
    /// check here (no flicker / redundant PATCH).
    async fn try_update(&self, label: &PhaseLabel, elapsed: Duration, force: bool) {
        let comment_id = self.comment_id();
        let check_run_id = if label.is_terminal() { None } else { self.check_run_id() };
        if comment_id.is_none() && check_run_id.is_none() {
            tracing::debug!(job_id = %self.job.id, phase = %label, "phase (no reporting surface)");
            return;
        }

        // Shared debounce. Brief lock — not held across the network calls.
        {
            let mut state = self.phase_state.lock().await;
            if !force
                && let Some(last) = state.last_update_at
                && last.elapsed() < PR_UPDATE_MIN_INTERVAL
            {
                tracing::trace!(phase = %label, since_last = ?last.elapsed(), "phase update debounced");
                return;
            }
            state.last_update_at = Some(Instant::now());
        }

        if let Some(comment_id) = comment_id {
            let body = format!(
                ":construction: benchmark `{id}` — **{phase}** for `{elapsed}` (commit `{sha}`)",
                id = self.job.id,
                phase = humanize_phase(label),
                elapsed = format_elapsed(elapsed),
                sha = self.job.commit,
            );
            if let Err(e) = self
                .gh
                .update_pr_comment(
                    self.job.installation_id,
                    &self.job.repository,
                    comment_id,
                    &body,
                )
                .await
            {
                tracing::warn!(error = ?e, "phase comment update failed (non-fatal)");
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
            if let Err(e) = self
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
                tracing::warn!(error = ?e, "phase check update failed (non-fatal)");
            }
        }
    }
}

#[async_trait]
impl ReportSurface for GitHubReportSurface {
    async fn started(&self) {
        self.update_comment(&format!(
            ":rocket: benchmark `{id}` is running on commit `{sha}`.",
            id = self.job.id,
            sha = self.job.commit,
        ))
        .await;
    }

    async fn phase(&self, label: &PhaseLabel, elapsed: Duration) {
        let force = label.is_terminal();
        self.try_update(label, elapsed, force)
            .await;
    }

    async fn heartbeat(&self, label: &PhaseLabel, elapsed: Duration) {
        self.try_update(label, elapsed, false)
            .await;
    }

    async fn completed(
        &self,
        summary: &serde_json::Value,
        comparison: Option<&BaselineComparison>,
    ) {
        let body = self
            .completed_body(summary, comparison)
            .await;
        self.update_comment(&body)
            .await;
        let pointer = if self.has_comment() { "see comment / details" } else { "see details" };
        // The benchmark RAN and produced results → success (perf is data, not a
        // gate — a regression doesn't flip this).
        self.complete_check(
            CheckRunConclusion::Success,
            CheckRunOutput {
                title: format!("benchmark {} — complete", self.job.id),
                summary: format!("commit `{}` — {pointer}", self.job.commit),
                text: Some(body),
            },
        )
        .await;
    }

    async fn failed(&self, error: &str) {
        let snippet = short_pr_error(error);
        self.update_comment(&format!(
            ":x: benchmark `{id}` failed: `{snippet}`\n\n_(full details in the daemon logs)_",
            id = self.job.id,
        ))
        .await;
        self.complete_check(
            CheckRunConclusion::Failure,
            CheckRunOutput {
                title: format!("benchmark {} — failed", self.job.id),
                summary: format!("commit `{}` failed", self.job.commit),
                text: Some(format!("```\n{snippet}\n```\n\n_(full details in the daemon logs)_")),
            },
        )
        .await;
    }

    async fn cancelled(&self, reason: &str) {
        self.update_comment(&format!(
            ":no_entry_sign: benchmark `{id}` cancelled: {reason}. Re-run with `/benchmark`.",
            id = self.job.id,
        ))
        .await;
        let hint = self.retrigger_hint();
        self.complete_check(
            CheckRunConclusion::Cancelled,
            CheckRunOutput {
                title: format!("benchmark {} — cancelled", self.job.id),
                summary: format!("commit `{}` — {reason}", self.job.commit),
                text: Some(format!("Cancelled: {reason}. {hint}")),
            },
        )
        .await;
    }
}

// ─────────────────────────── Slack ───────────────────────────

/// Slack reporting surface: a thin `ReportSurface` adapter over the live
/// [`SlackTimeline`]. Always holds a **real** timeline (the no-client case is
/// [`NoopReportSurface`], not an `Option` here). The artifact store resolves
/// the metrics + DB link in `completed`.
pub struct SlackReportSurface {
    timeline: SlackTimeline,
    store: Arc<dyn ArtifactStore>,
}

impl SlackReportSurface {
    pub fn new(timeline: SlackTimeline, store: Arc<dyn ArtifactStore>) -> Self {
        Self { timeline, store }
    }
}

#[async_trait]
impl ReportSurface for SlackReportSurface {
    async fn started(&self) {
        self.timeline.started().await;
    }

    async fn phase(&self, label: &PhaseLabel, _elapsed: Duration) {
        let name = label.to_string();
        // A binary-cache hit (item 0025, v9) arrives as `build_cached:<digest>`:
        // mark the Build row done with the reused-build subtext + advance to Run.
        if let Some(digest) = name.strip_prefix("build_cached:") {
            self.timeline
                .mark_build_cached(digest)
                .await;
            return;
        }
        // Monotonic: a non-stage / terminal phase (mapped to `None`) or a repeat
        // is a no-op; the terminal card is owned by `completed`/`failed`.
        if let Some(stage) = stage_for_phase(&name) {
            self.timeline
                .advance(stage)
                .await;
        }
    }

    async fn heartbeat(&self, label: &PhaseLabel, _elapsed: Duration) {
        if stage_for_phase(&label.to_string()).is_some() {
            self.timeline
                .heartbeat()
                .await;
        }
    }

    async fn completed(
        &self,
        summary: &serde_json::Value,
        _comparison: Option<&BaselineComparison>,
    ) {
        // Ad-hoc Slack runs aren't PRs → no vs-baseline. Metrics + the presigned
        // DB link (S3 + in-bucket only) are resolved here, then handed to the card.
        let result = parsed_run(self.store.as_ref(), summary).await;
        let db_url = signed_db_url(self.store.as_ref(), summary).await;
        self.timeline
            .completed(result, db_url)
            .await;
    }

    async fn failed(&self, error: &str) {
        let snippet = short_pr_error(error);
        self.timeline
            .failed(&snippet)
            .await;
    }

    async fn cancelled(&self, reason: &str) {
        self.timeline
            .cancelled(reason)
            .await;
    }
}

// ─────────────────────────── No-op ───────────────────────────

/// Reporting surface that does nothing — returned for a Slack target with
/// **no** Slack client wired, preserving today's silent degrade (Decision 6).
pub struct NoopReportSurface;

#[async_trait]
impl ReportSurface for NoopReportSurface {
    async fn started(&self) {}
    async fn phase(&self, _label: &PhaseLabel, _elapsed: Duration) {}
    async fn heartbeat(&self, _label: &PhaseLabel, _elapsed: Duration) {}
    async fn completed(&self, _s: &serde_json::Value, _c: Option<&BaselineComparison>) {}
    async fn failed(&self, _error: &str) {}
    async fn cancelled(&self, _reason: &str) {}
}

// ─────────────────────── shared helpers ───────────────────────

/// Resolve the archived `run.json` store **key** (Decision 0002) → local path →
/// parse. `None` on any missing-key / I/O / parse error. Shared by the GitHub
/// and Slack completed renders.
async fn parsed_run(store: &dyn ArtifactStore, summary: &serde_json::Value) -> Option<RunResult> {
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
async fn signed_db_url(store: &dyn ArtifactStore, summary: &serde_json::Value) -> Option<String> {
    let key = summary
        .get("sqlite_archived_path")
        .and_then(|v| v.as_str())?;
    store
        .signed_url_if_fetchable(key, DB_LINK_TTL)
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

/// Human display for a phase label on the GitHub surfaces. The binary-cache hit
/// (item 0025, v9) arrives as the opaque `build_cached:<digest>`; render it as
/// "build (cached)" rather than leaking the raw digest into the PR comment /
/// check. Any other label is shown verbatim.
fn humanize_phase(label: &PhaseLabel) -> String {
    let name = label.to_string();
    if name.starts_with("build_cached:") { "build (cached)".to_string() } else { name }
}

/// Trim an error chain to something safe to show a PR author: the first
/// non-empty line, the noisy shell-wrapper prefix stripped (prefer what follows
/// `stderr=`), capped at 160 chars with an ellipsis.
fn short_pr_error(error: &str) -> String {
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
    use sbgh_core::github::test_support::{FakeCall, FakeGitHub};
    use sbgh_core::github::{CheckRunConclusion, CheckRunState};
    use sbgh_core::models::{GitRefKind, ResolvedCommit};
    use uuid::Uuid;

    use super::*;
    use crate::artifact_store::LocalFsStore;
    use crate::job_source::BaselineRef;
    use crate::slack::client::COMPLETED_REACTION;

    fn store() -> Arc<dyn ArtifactStore> {
        Arc::new(LocalFsStore::new(std::env::temp_dir()))
    }

    fn job_with(progress: ProgressTarget) -> RunnableJob {
        RunnableJob {
            id: Uuid::new_v4(),
            benchmark_group_id: Uuid::new_v4(),
            benchmark_spec_id: Uuid::new_v4(),
            benchmark_run_index: 0,
            requested_run_count: 1,
            group_artifact_prefix: Uuid::new_v4().to_string(),
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

    // ── GitHub lifecycle (ported from progress.rs) ──

    #[tokio::test]
    async fn github_completed_concludes_check_success() {
        let gh = Arc::new(FakeGitHub::new());
        github(&gh, check_job(11))
            .completed(&serde_json::json!({}), None)
            .await;
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
            .await;
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
            .await;
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
            .await;
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
            .await;
        assert!(check_text(&gh_pr).contains("/benchmark"), "PR job re-runs via /benchmark");
    }

    // ── GitHub phase path (ported from reporter.rs ProgressSink) ──

    #[tokio::test]
    async fn github_phase_sets_check_title_to_phase() {
        let gh = Arc::new(FakeGitHub::new());
        github(&gh, check_job(777))
            .phase(&PhaseLabel::new("building", false), Duration::ZERO)
            .await;
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
        github(&gh, job)
            .phase(&PhaseLabel::new("running", false), Duration::ZERO)
            .await;
        let calls = gh.calls();
        assert!(
            calls
                .iter()
                .any(|c| matches!(c, FakeCall::UpdateComment { .. })),
            "comment updated"
        );
        assert!(
            calls
                .iter()
                .any(|c| matches!(c, FakeCall::UpdateCheckRun { .. })),
            "check updated"
        );
    }

    #[tokio::test]
    async fn github_phase_skips_check_on_terminal_phase() {
        let gh = Arc::new(FakeGitHub::new());
        github(&gh, check_job(5))
            .phase(&PhaseLabel::new("done", true), Duration::ZERO)
            .await;
        assert!(
            !gh.calls()
                .iter()
                .any(|c| matches!(c, FakeCall::UpdateCheckRun { .. })),
            "no check churn on a terminal phase"
        );
    }

    // ── Slack adapter (over the timeline; mechanics tested in timeline.rs) ──

    /// Counts the Slack calls the surface makes through the timeline.
    #[derive(Default)]
    struct FakeSlack {
        posts: StdMutex<usize>,
        updates: StdMutex<usize>,
        added: StdMutex<Vec<String>>,
    }

    #[async_trait]
    impl SlackClient for FakeSlack {
        async fn post_ephemeral(&self, _c: &str, _u: &str, _t: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn post_blocks_in_thread(
            &self,
            _c: &str,
            _ts: &str,
            _b: &serde_json::Value,
            _f: &str,
        ) -> anyhow::Result<String> {
            *self.posts.lock().unwrap() += 1;
            Ok("PLAN_TS".into())
        }
        async fn update_blocks(
            &self,
            _c: &str,
            _ts: &str,
            _b: &serde_json::Value,
            _f: &str,
        ) -> anyhow::Result<()> {
            *self.updates.lock().unwrap() += 1;
            Ok(())
        }
        async fn add_reaction(&self, _c: &str, _ts: &str, r: &str) -> anyhow::Result<()> {
            self.added
                .lock()
                .unwrap()
                .push(r.into());
            Ok(())
        }
        async fn remove_reaction(&self, _c: &str, _ts: &str, _r: &str) -> anyhow::Result<()> {
            Ok(())
        }
    }

    /// A minimal store recording only `set_plan_message_ts`.
    #[derive(Default)]
    struct RecordingStore {
        persisted: StdMutex<Vec<String>>,
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
            _m: &str,
            _b: &str,
            _at: Option<chrono::DateTime<chrono::Utc>>,
            _w: &str,
        ) -> anyhow::Result<Option<BaselineRef>> {
            Ok(None)
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

    fn slack_job() -> RunnableJob {
        job_with(ProgressTarget::Slack {
            channel: "C1".into(),
            message_ts: "REQ".into(),
            plan_message_ts: None,
        })
    }

    /// The Slack surface delegates the lifecycle to the timeline. This fake
    /// client does not implement streaming, so the timeline exercises the
    /// block fallback path while still proving heartbeat/phase/completion reach
    /// Slack and the reaction swaps to ✅.
    #[tokio::test]
    async fn slack_surface_drives_the_timeline() {
        let recording = Arc::new(FakeSlack::default());
        let slack = recording.clone() as Arc<dyn SlackClient>;
        let jobs: Arc<dyn RunnableJobStore> = Arc::new(RecordingStore::default());
        let surface = build_report_surface(
            Arc::new(FakeGitHub::new()),
            jobs,
            store(),
            Some(&slack),
            &slack_job(),
        );

        surface.started().await;
        surface
            .heartbeat(&PhaseLabel::new("running", false), Duration::ZERO)
            .await;
        surface
            .phase(&PhaseLabel::new("running", false), Duration::ZERO)
            .await;
        surface
            .completed(&serde_json::json!({}), None)
            .await;

        assert_eq!(
            *recording
                .posts
                .lock()
                .unwrap(),
            1,
            "card posted exactly once"
        );
        assert!(
            *recording
                .updates
                .lock()
                .unwrap()
                >= 2,
            "advanced + finalized via chat.update"
        );
        assert_eq!(
            *recording
                .added
                .lock()
                .unwrap(),
            vec![COMPLETED_REACTION.to_string()],
            "⏳ swapped to ✅",
        );
    }

    // ── Factory routing + the no-client no-op guard ──

    /// A Slack target with **no** client wired routes to `NoopReportSurface`:
    /// the full lifecycle runs without panicking and makes zero GitHub/Slack
    /// calls (the regression guard for today's silent-degrade edge).
    #[tokio::test]
    async fn factory_slack_without_client_is_a_noop() {
        let gh = Arc::new(FakeGitHub::new());
        let jobs: Arc<dyn RunnableJobStore> = Arc::new(RecordingStore::default());
        let surface = build_report_surface(gh.clone(), jobs, store(), None, &slack_job());

        surface.started().await;
        surface
            .phase(&PhaseLabel::new("running", false), Duration::ZERO)
            .await;
        surface
            .heartbeat(&PhaseLabel::new("running", false), Duration::ZERO)
            .await;
        surface
            .completed(&serde_json::json!({}), None)
            .await;
        surface.failed("boom").await;
        surface
            .cancelled("shutdown")
            .await;

        assert!(gh.calls().is_empty(), "no-op surface makes zero GitHub calls: {:?}", gh.calls());
    }

    /// A GitHub target routes to the GitHub surface (it concludes the check).
    #[tokio::test]
    async fn factory_github_target_concludes_check() {
        let gh = Arc::new(FakeGitHub::new());
        let jobs: Arc<dyn RunnableJobStore> = Arc::new(RecordingStore::default());
        let surface = build_report_surface(gh.clone(), jobs, store(), None, &check_job(99));
        surface
            .completed(&serde_json::json!({}), None)
            .await;
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
