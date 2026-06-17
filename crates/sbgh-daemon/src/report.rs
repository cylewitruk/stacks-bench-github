//! The job reporting **surface** seam (iteration v7, item 0022).
//!
//! One [`ReportSurface`] per job drives the lifecycle on one target family —
//! the GitHub PR comment + Check Run together ([`GitHubReportSurface`]), the
//! Slack live card ([`SlackReportSurface`]), or nothing
//! ([`NoopReportSurface`]). [`build_report_surface`] picks the right one from
//! `(ProgressTarget, slack)`. This collapses the old split between
//! `ProgressReporter` (lifecycle) and `ProgressSink` (worker-event drain),
//! which each re-interpreted the `ProgressTarget` separately.
//!
//! **Surface lifetime vs. card lifetime (v18, 0047).** For GitHub the surface
//! owns the whole lifecycle. For Slack a benchmark *group* shares **one** live
//! card/stream across all its runs, so the per-run [`SlackReportSurface`] is a
//! thin **delegate** into a group-scoped session (`slack::session`) that owns
//! the card, stream keepalive, and reactions for the group's lifetime; the
//! per-run surface re-points it (`begin_run`) and reaps it only on a
//! group-terminal event. The model is `trigger → session(s) → per-run
//! delegate(s)`, kept fan-out friendly so a future job can report to several
//! destinations.
//!
//! Every method is **non-fatal**: an impl logs and swallows its own transport
//! errors (a reporting failure never fails the benchmark) — hence `()` returns.
//!
//! Wired from [`Reporter::run`](crate::reporter) (the lifecycle + the drain
//! loop) and the runner's orphan recovery — both build their surface via
//! [`build_report_surface`], threading the shared `SlackSessionRegistry`.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use sbgh_core::github::{
    CheckRunConclusion, CheckRunOutput, CheckRunState, CheckRunUpdate, GitHubApi,
};
use tokio::sync::Mutex;

use crate::artifact_store::{ArtifactStore, GROUP_SQLITE_RELATIVE, group_artifact_key};
use crate::bench_summary::{self, RunResult};
use crate::comparison::BaselineComparison;
use crate::events::PhaseLabel;
use crate::job_source::{ProgressTarget, RunnableJob, RunnableJobStore};
use crate::libvirt::format_elapsed;
use crate::slack::card::RepeatSummary;
use crate::slack::client::SlackClient;
use crate::slack::session::{SlackSession, SlackSessionRegistry, SlackTarget};
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
    slack_sessions: &Arc<SlackSessionRegistry>,
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
        ) => {
            // v18 (0047): one group-scoped session owns the card + keepalive
            // across every run of the group; this run's surface borrows it. The
            // timeline is built only on the group's first run (get-or-create);
            // later runs re-point it via `begin_run` in `started`.
            let target = SlackTarget {
                channel: channel.clone(),
                thread_ts: message_ts.clone(),
            };
            let session =
                slack_sessions.get_or_create(job.benchmark_group_id, target.clone(), || {
                    Arc::new(SlackTimeline::new(
                        client.clone(),
                        jobs.clone(),
                        job.clone(),
                        channel.clone(),
                        message_ts.clone(),
                        plan_message_ts.clone(),
                    ))
                });
            Box::new(SlackReportSurface::new(
                slack_sessions.clone(),
                session,
                target,
                job.clone(),
                jobs,
                store,
            ))
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

/// The kind of terminal a run reached, for the group-reap decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TerminalOutcome {
    Success,
    Failure,
    Cancel,
}

/// Whether a run is **group-terminal** — and so reaps the shared Slack session.
/// A final-repeat success ends the group; any failure or cancel stops it. A
/// non-final repeat success is *not* group-terminal — the session (card +
/// keepalive) lives on for the next run.
fn is_group_terminal(is_final_repeat: bool, outcome: TerminalOutcome) -> bool {
    match outcome {
        TerminalOutcome::Success => is_final_repeat,
        TerminalOutcome::Failure | TerminalOutcome::Cancel => true,
    }
}

/// Slack reporting surface: a per-run **delegate** into the group-scoped
/// [`SlackSession`] that owns the live card + keepalive (v18, 0047). `started`
/// re-points the card at this run; the terminal methods reap the session only
/// on a group-terminal event ([`is_group_terminal`]). The artifact store
/// resolves the metrics + DB link in `completed`.
pub struct SlackReportSurface {
    /// The registry the session is reaped from on a group-terminal event.
    sessions: Arc<SlackSessionRegistry>,
    /// This group's shared session — owns the live card + keepalive (v18 0047).
    session: Arc<SlackSession>,
    /// The session key's target half, for reaping.
    target: SlackTarget,
    job: RunnableJob,
    jobs: Arc<dyn RunnableJobStore>,
    store: Arc<dyn ArtifactStore>,
}

impl SlackReportSurface {
    pub fn new(
        sessions: Arc<SlackSessionRegistry>,
        session: Arc<SlackSession>,
        target: SlackTarget,
        job: RunnableJob,
        jobs: Arc<dyn RunnableJobStore>,
        store: Arc<dyn ArtifactStore>,
    ) -> Self {
        Self {
            sessions,
            session,
            target,
            job,
            jobs,
            store,
        }
    }

    /// The group-scoped live card this run reports into.
    fn timeline(&self) -> &SlackTimeline {
        self.session.timeline()
    }

    /// Reap the session on a group-terminal event (abort keepalive + drop from
    /// the registry); a non-final repeat success leaves it alive.
    fn reap_if_group_terminal(&self, outcome: TerminalOutcome) {
        if is_group_terminal(
            self.timeline()
                .is_final_repeat(),
            outcome,
        ) {
            self.sessions
                .reap(self.job.benchmark_group_id, &self.target);
        }
    }
}

#[async_trait]
impl ReportSurface for SlackReportSurface {
    async fn started(&self) {
        // Re-point the group card at this run, render it, then (re-)arm the
        // keepalive — in that order: `begin_run` resets the stage to 0 and the
        // keepalive idles at stage 0, so it must follow `started` (stage 1).
        self.timeline()
            .begin_run(&self.job)
            .await;
        self.timeline()
            .started()
            .await;
        self.session
            .ensure_keepalive();
    }

    async fn phase(&self, label: &PhaseLabel, _elapsed: Duration) {
        let name = label.to_string();
        // v16: a cache hit is being staged on the host before the bench VM
        // starts. Surface it as a cached-binary staging row, not as a build VM.
        if let Some(digest) = name.strip_prefix("build_cache_staging:") {
            self.timeline()
                .mark_build_cache_staging(digest)
                .await;
            return;
        }
        // A binary-cache hit (item 0025, v9) arrives as `build_cached:<digest>`:
        // mark the Build row done with the reused-build subtext + advance to Run.
        if let Some(digest) = name.strip_prefix("build_cached:") {
            self.timeline()
                .mark_build_cached(digest)
                .await;
            return;
        }
        // Monotonic: a non-stage / terminal phase (mapped to `None`) or a repeat
        // is a no-op; the terminal card is owned by `completed`/`failed`.
        if let Some(stage) = stage_for_phase(&name) {
            self.timeline()
                .advance(stage)
                .await;
        }
    }

    async fn heartbeat(&self, label: &PhaseLabel, _elapsed: Duration) {
        if stage_for_phase(&label.to_string()).is_some() {
            self.timeline()
                .heartbeat()
                .await;
        }
    }

    async fn completed(
        &self,
        summary: &serde_json::Value,
        _comparison: Option<&BaselineComparison>,
    ) {
        // Run-end is activity: refresh the abandonment clock so the next run's
        // inter-run carry-forward gap is bridged by the sweep's grace TTL.
        self.session.touch();
        // Ad-hoc Slack runs aren't PRs → no vs-baseline. Metrics + the presigned
        // DB link (S3 + in-bucket only) are resolved here, then handed to the card.
        if self
            .timeline()
            .is_repeat_group()
            && !self
                .timeline()
                .is_final_repeat()
        {
            // Non-final repeat: keep the shared card + keepalive for the next run.
            self.timeline()
                .repeat_completed()
                .await;
            return;
        }
        let result = parsed_run(self.store.as_ref(), summary).await;
        let repeat_summary = if self
            .timeline()
            .is_repeat_group()
        {
            self.repeat_summary().await
        } else {
            None
        };
        let db_url = if self
            .timeline()
            .is_repeat_group()
            && !self
                .timeline()
                .is_final_repeat()
        {
            signed_group_db_url(
                self.store.as_ref(),
                self.timeline()
                    .group_artifact_prefix(),
            )
            .await
        } else if self
            .timeline()
            .is_repeat_group()
        {
            // The final run's job-scoped DB has already been seeded from the
            // group DB and appended to by stacks-bench, while the runner's
            // final promotion to the group namespace happens after reporting.
            match signed_db_url(self.store.as_ref(), summary).await {
                Some(url) => Some(url),
                None => {
                    signed_group_db_url(
                        self.store.as_ref(),
                        self.timeline()
                            .group_artifact_prefix(),
                    )
                    .await
                }
            }
        } else {
            signed_db_url(self.store.as_ref(), summary).await
        };
        self.timeline()
            .completed(result, db_url, repeat_summary)
            .await;
        self.reap_if_group_terminal(TerminalOutcome::Success);
    }

    async fn failed(&self, error: &str) {
        let snippet = short_pr_error(error);
        if self
            .timeline()
            .is_repeat_group()
        {
            let (repeat_summary, db_url) = self.repeat_payload().await;
            self.timeline()
                .failed_with_results(&snippet, repeat_summary, db_url)
                .await;
        } else {
            self.timeline()
                .failed(&snippet)
                .await;
        }
        self.reap_if_group_terminal(TerminalOutcome::Failure);
    }

    async fn cancelled(&self, reason: &str) {
        if self
            .timeline()
            .is_repeat_group()
        {
            let (repeat_summary, db_url) = self.repeat_payload().await;
            self.timeline()
                .cancelled_with_results(reason, repeat_summary, db_url)
                .await;
        } else {
            self.timeline()
                .cancelled(reason)
                .await;
        }
        self.reap_if_group_terminal(TerminalOutcome::Cancel);
    }
}

impl SlackReportSurface {
    async fn repeat_payload(&self) -> (Option<RepeatSummary>, Option<String>) {
        let repeat_summary = self.repeat_summary().await;
        let db_url = signed_group_db_url(
            self.store.as_ref(),
            self.timeline()
                .group_artifact_prefix(),
        )
        .await;
        (repeat_summary, db_url)
    }

    async fn repeat_summary(&self) -> Option<RepeatSummary> {
        match self
            .jobs
            .benchmark_run_metrics(
                self.timeline()
                    .benchmark_spec_id(),
            )
            .await
        {
            Ok(metrics) => {
                let metrics: Vec<_> = metrics
                    .into_iter()
                    .map(|run| run.metric)
                    .collect();
                Some(RepeatSummary::from_metrics(
                    self.timeline()
                        .requested_run_count(),
                    &metrics,
                ))
            }
            Err(e) => {
                tracing::warn!(error = ?e, "slack: loading repeat metrics failed");
                None
            }
        }
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

async fn signed_group_db_url(store: &dyn ArtifactStore, group_prefix: &str) -> Option<String> {
    let key = group_artifact_key(group_prefix, GROUP_SQLITE_RELATIVE);
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
    use sbgh_core::db::BenchmarkRunMetric;
    use sbgh_core::github::test_support::{FakeCall, FakeGitHub};
    use sbgh_core::github::{CheckRunConclusion, CheckRunState};
    use sbgh_core::models::{GitRefKind, JobMetric, ResolvedCommit};
    use uuid::Uuid;

    use super::*;
    use crate::artifact_store::LocalFsStore;
    use crate::job_source::BaselineRef;
    use crate::slack::client::{COMPLETED_REACTION, RUNNING_REACTION};

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
        update_blocks: StdMutex<Vec<String>>,
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
            self.update_blocks
                .lock()
                .unwrap()
                .push(_b.to_string());
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
        metrics: StdMutex<Vec<BenchmarkRunMetric>>,
    }

    impl RecordingStore {
        fn with_metrics(metrics: Vec<BenchmarkRunMetric>) -> Self {
            Self {
                persisted: StdMutex::new(Vec::new()),
                metrics: StdMutex::new(metrics),
            }
        }
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
        async fn benchmark_run_metrics(
            &self,
            _benchmark_spec_id: Uuid,
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

    fn slack_job() -> RunnableJob {
        job_with(ProgressTarget::Slack {
            channel: "C1".into(),
            message_ts: "REQ".into(),
            plan_message_ts: None,
        })
    }

    fn repeat_slack_job() -> RunnableJob {
        let mut job = job_with(ProgressTarget::Slack {
            channel: "C1".into(),
            message_ts: "REQ".into(),
            plan_message_ts: Some("PLAN_TS".into()),
        });
        job.benchmark_run_index = 1;
        job.requested_run_count = 3;
        job
    }

    fn metric(job_id: u128, exec: i64, commit: i64) -> BenchmarkRunMetric {
        BenchmarkRunMetric {
            benchmark_run_index: job_id as i32,
            metric: JobMetric {
                job_id: Uuid::from_u128(job_id),
                envelope_duration_us: 0,
                replay_duration_us: 0,
                total_duration_us: exec + commit,
                setup_duration_us: 0,
                execution_duration_us: exec,
                commit_duration_us: commit,
                clarity_runtime: 0,
                transactions: 1,
                read_length: 0,
                write_length: 0,
                measured_blocks: 1,
                warmup_blocks: 0,
                created_at: chrono::Utc::now(),
            },
        }
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
            &Arc::new(SlackSessionRegistry::new()),
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
            vec![RUNNING_REACTION.to_string(), COMPLETED_REACTION.to_string()],
            "⏳ → 🚀 (run 0 started) → ✅ (completed)",
        );
    }

    #[tokio::test]
    async fn slack_repeat_failure_renders_partial_aggregate() {
        let recording = Arc::new(FakeSlack::default());
        let slack = recording.clone() as Arc<dyn SlackClient>;
        let jobs: Arc<dyn RunnableJobStore> = Arc::new(RecordingStore::with_metrics(vec![
            metric(1, 1_000, 100),
            metric(2, 1_200, 200),
        ]));
        let surface = build_report_surface(
            Arc::new(FakeGitHub::new()),
            jobs,
            store(),
            Some(&slack),
            &Arc::new(SlackSessionRegistry::new()),
            &repeat_slack_job(),
        );

        surface
            .failed("bench VM stopped")
            .await;

        let updates = recording
            .update_blocks
            .lock()
            .unwrap();
        let rendered = updates
            .last()
            .expect("failed repeat updates the shared card");
        assert!(rendered.contains("Clean Repeat Summary"), "{rendered}");
        assert!(rendered.contains("2 / 3"), "{rendered}");
        assert!(rendered.contains("Failed: bench VM stopped"), "{rendered}");
        assert_eq!(
            *recording
                .added
                .lock()
                .unwrap(),
            vec![crate::slack::client::FAILED_REACTION.to_string()],
        );
    }

    // ── v18 (0047): group-scoped session lifecycle ──

    #[test]
    fn is_group_terminal_matrix() {
        // Success is group-terminal only on the final repeat.
        assert!(!is_group_terminal(false, TerminalOutcome::Success), "non-final success");
        assert!(is_group_terminal(true, TerminalOutcome::Success), "final success");
        // Any failure/cancel stops the whole group, regardless of index.
        assert!(is_group_terminal(false, TerminalOutcome::Failure));
        assert!(is_group_terminal(true, TerminalOutcome::Failure));
        assert!(is_group_terminal(false, TerminalOutcome::Cancel));
        assert!(is_group_terminal(true, TerminalOutcome::Cancel));
    }

    /// A repeat group's runs share **one** session + keepalive, reaped only on
    /// the final run — the v18 ownership fix (and Codex's non-final re-arm
    /// note).
    #[tokio::test]
    async fn repeat_group_shares_one_session_reaped_only_on_final() {
        let recording = Arc::new(FakeSlack::default());
        let slack = recording.clone() as Arc<dyn SlackClient>;
        let sessions = Arc::new(SlackSessionRegistry::new());
        let gh = Arc::new(FakeGitHub::new());
        let group = Uuid::from_u128(0xABC);
        let target = SlackTarget {
            channel: "C1".into(),
            thread_ts: "REQ".into(),
        };
        let job_for = |idx: i32| {
            let mut j = slack_job(); // Slack { C1, REQ, None }
            j.benchmark_group_id = group;
            j.benchmark_run_index = idx;
            j.requested_run_count = 2;
            j
        };
        let jobs = || -> Arc<dyn RunnableJobStore> {
            Arc::new(RecordingStore::with_metrics(vec![
                metric(0, 1_000, 100),
                metric(1, 1_200, 200),
            ]))
        };

        // ── Run 0 (non-final) ──
        let s0 =
            build_report_surface(gh.clone(), jobs(), store(), Some(&slack), &sessions, &job_for(0));
        s0.started().await;
        assert_eq!(sessions.len(), 1, "session created on the first run");
        assert!(
            sessions
                .get(group, &target)
                .unwrap()
                .keepalive_running(),
            "keepalive armed after the first started()",
        );
        s0.completed(&serde_json::json!({}), None)
            .await; // non-final success
        assert_eq!(sessions.len(), 1, "non-final completion keeps the session");
        assert!(
            sessions
                .get(group, &target)
                .unwrap()
                .keepalive_running(),
            "keepalive persists across the non-final terminal",
        );
        drop(s0);
        assert_eq!(sessions.len(), 1, "dropping the per-run surface does not reap");

        // ── Run 1 (final) — new surface, same registry → same session ──
        let s1 =
            build_report_surface(gh.clone(), jobs(), store(), Some(&slack), &sessions, &job_for(1));
        s1.started().await; // begin_run → started → ensure_keepalive
        assert!(
            sessions
                .get(group, &target)
                .unwrap()
                .keepalive_running(),
            "keepalive still running across the begin_run/started handoff",
        );
        s1.completed(&serde_json::json!({}), None)
            .await; // final success
        assert!(sessions.is_empty(), "final completion reaps the session");
    }

    /// A single-run Slack job is a group of size 1 — its session is reaped on
    /// completion.
    #[tokio::test]
    async fn single_run_slack_job_reaps_on_completion() {
        let recording = Arc::new(FakeSlack::default());
        let slack = recording.clone() as Arc<dyn SlackClient>;
        let sessions = Arc::new(SlackSessionRegistry::new());
        let jobs: Arc<dyn RunnableJobStore> = Arc::new(RecordingStore::default());
        let surface = build_report_surface(
            Arc::new(FakeGitHub::new()),
            jobs,
            store(),
            Some(&slack),
            &sessions,
            &slack_job(),
        );

        surface.started().await;
        assert_eq!(sessions.len(), 1);
        surface
            .completed(&serde_json::json!({}), None)
            .await;
        assert!(sessions.is_empty(), "single-run job reaps its session on completion");
    }

    // ── Factory routing + the no-client no-op guard ──

    /// A Slack target with **no** client wired routes to `NoopReportSurface`:
    /// the full lifecycle runs without panicking and makes zero GitHub/Slack
    /// calls (the regression guard for today's silent-degrade edge).
    #[tokio::test]
    async fn factory_slack_without_client_is_a_noop() {
        let gh = Arc::new(FakeGitHub::new());
        let jobs: Arc<dyn RunnableJobStore> = Arc::new(RecordingStore::default());
        let surface = build_report_surface(
            gh.clone(),
            jobs,
            store(),
            None,
            &Arc::new(SlackSessionRegistry::new()),
            &slack_job(),
        );

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
        let surface = build_report_surface(
            gh.clone(),
            jobs,
            store(),
            None,
            &Arc::new(SlackSessionRegistry::new()),
            &check_job(99),
        );
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
