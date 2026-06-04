//! Main daemon loop.
//!
//! On pickup the daemon:
//!   1. Resolves the commit via the GitHub API (the handler can't — no App
//!      credentials): a PR job's head SHA, or a `tag_created` job's tag →
//!      commit. Writes it back to the job row. (FATAL — no SHA, no run.)
//!   2. Reporting (NON-FATAL, per `[reporting]`): creates the Check Run on the
//!      resolved commit and — for PR jobs — posts the "starting" comment
//!      linking it, persisting both ids so a re-claim reuses them.
//!   3. Runs the benchmark; updates the check (→ `neutral`) + comment at the
//!      end.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use sbgh_core::config::DaemonConfig;
use sbgh_core::github::{CheckRunOutput, CheckRunState, CheckRunUpdate, GitHubApi};
use sbgh_core::models::{GitRefKind, ResolvedCommit};
use tokio::sync::{Mutex, OnceCell};

use crate::job_source::{ProgressTarget, RunnableJob, RunnableJobStore};
use crate::libvirt::{LibvirtDriver, OutcomeStatus, Phase, PhaseListener, Shell, format_elapsed};
use crate::progress::ProgressReporter;

const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Display name of the Check Run the daemon posts (roadmap-v4). Stable so the
/// reconcile filter (`check_name`) and GitHub's per-name dedup both work.
const CHECK_NAME: &str = "stacks-bench";

/// Lease after which a job stranded in `claimed` (claimed but never
/// transitioned to `running` — daemon crashed or preflight errored
/// between `claim_next` and `start_running`) is reclaimed to `queued` by
/// the stuck-claim sweep. The claim→running window is normally
/// sub-second (a GH API call at most), so a few minutes is ample slack
/// without leaving a crashed claim stuck for long.
const CLAIM_LEASE_MINUTES: i64 = 5;

/// Minimum interval between PR-comment edits driven by the
/// `CommentPhaseListener`. Phase transitions and heartbeats both go
/// through this debounce; terminal phases (done/error) bypass it so
/// the user always sees the final state immediately.
///
/// 30s gives us at most ~2 edits/min in the worst case, which is well
/// below any plausible GitHub secondary rate limit and looks calm in
/// the PR's "edit history". The first edit after a comment is created
/// is always allowed through — so the user sees the initial state
/// transition immediately rather than waiting 30s.
const PR_UPDATE_MIN_INTERVAL: Duration = Duration::from_secs(30);

pub struct Runner {
    config: Arc<DaemonConfig>,
    jobs: Arc<dyn RunnableJobStore>,
    gh: Arc<dyn GitHubApi>,
    shell: Arc<dyn Shell>,
    /// The App's numeric id, resolved via `GET /app` and cached on **success**
    /// only — a `get_or_try_init` that leaves the cell empty on error, so a
    /// transient startup blip is retried on the next job rather than disabling
    /// the reconcile for the whole process.
    app_id: OnceCell<i64>,
}

impl Runner {
    pub fn new(
        config: DaemonConfig,
        jobs: Arc<dyn RunnableJobStore>,
        gh: Arc<dyn GitHubApi>,
        shell: Arc<dyn Shell>,
    ) -> Self {
        Self {
            config: Arc::new(config),
            jobs,
            gh,
            shell,
            app_id: OnceCell::new(),
        }
    }

    /// Resolve the App's numeric id via `GET /app`, caching it on success.
    /// Non-fatal: on failure the reconcile is skipped **this time** and the
    /// cell is left empty, so the next `ensure_check` retries (self-heals after
    /// a transient blip).
    async fn resolved_app_id(&self) -> Option<i64> {
        match self
            .app_id
            .get_or_try_init(|| async {
                let id = self
                    .gh
                    .current_app_id()
                    .await?;
                tracing::info!(app_id = id, "resolved GitHub App id (check reconcile enabled)");
                Ok::<i64, sbgh_core::Error>(id)
            })
            .await
        {
            Ok(id) => Some(*id),
            Err(e) => {
                tracing::warn!(error = ?e, "could not resolve App id via GET /app; reconcile skipped (will retry)");
                None
            }
        }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        tracing::info!("daemon started");
        // Resolve the App id once up front (logs the outcome); `ensure_check`
        // reuses the cached value.
        let _ = self.resolved_app_id().await;
        let lease = chrono::Duration::minutes(CLAIM_LEASE_MINUTES);
        loop {
            // Recover jobs stranded in `claimed` (crash / preflight error
            // between claim and start_running) before claiming the next.
            // A single job runs to completion before the loop turns over,
            // so this fires between jobs and at startup — exactly when a
            // stranded claim from a prior (crashed) run needs reclaiming.
            match self
                .jobs
                .sweep_stuck_claims(lease)
                .await
            {
                Ok(n) if n > 0 => {
                    tracing::warn!(recovered = n, "recovered stuck `claimed` jobs")
                }
                Ok(_) => {}
                Err(e) => tracing::error!(error = ?e, "stuck-claim sweep failed"),
            }

            match self.run_once().await {
                Ok(true) => {}
                Ok(false) => tokio::time::sleep(POLL_INTERVAL).await,
                Err(e) => {
                    // Setup-time failure (claim failed, couldn't resolve
                    // SHA, mkdir failed, etc.). VM-side failures come
                    // back via `BenchmarkOutcome::Failed` and are NOT
                    // errors here. Log and keep looping.
                    tracing::error!(error = ?e, "job iteration failed");
                    tokio::time::sleep(POLL_INTERVAL).await;
                }
            }
        }
    }

    /// Claim + execute one job. Returns `Ok(true)` if a job was
    /// processed, `Ok(false)` if the queue was empty. Split out from
    /// `run` so the claim→execute→terminal lifecycle is testable against
    /// any [`RunnableJobStore`] without the infinite poll loop.
    pub async fn run_once(&self) -> anyhow::Result<bool> {
        match self.jobs.claim_next().await? {
            Some(job) => {
                self.execute(job).await?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Owns the job by value so we can attach the resolved commit + the
    /// posted comment_id to the in-memory copy before handing it to the
    /// reporter and the driver (both want them populated).
    async fn execute(&self, mut job: RunnableJob) -> anyhow::Result<()> {
        tracing::info!(
            job_id = %job.id,
            repo = %job.repository,
            git_ref_kind = ?job.git_ref_kind,
            git_ref = %job.git_ref_display,
            commit_preresolved = !job.commit.is_empty(),
            progress = match job.progress {
                ProgressTarget::PullRequest { .. } => "pull_request",
                ProgressTarget::CommitCheck { .. } => "commit_check",
            },
            "claimed job; starting",
        );

        // Pre-flight: resolve the commit + post the initial PR comment.
        // Returns the commit if newly resolved (so `start_running` can
        // persist it). Errors here are reported back via `fail` — the
        // user otherwise sees a silent "/benchmark went nowhere" because
        // the handler doesn't write a comment any more.
        let resolved_commit = match self.preflight(&mut job).await {
            Ok(c) => c,
            Err(e) => {
                let msg = format!("pre-flight failed: {e}");
                tracing::error!(job_id = %job.id, error = ?e, "pre-flight failed");
                let _ = self
                    .jobs
                    .fail(&job, &msg, None)
                    .await;
                return Err(e);
            }
        };

        // Transition `claimed → running`, persisting the resolved commit.
        let commit_persisted = resolved_commit.is_some();
        self.jobs
            .start_running(&job, resolved_commit)
            .await?;
        tracing::info!(
            job_id = %job.id,
            commit = %job.commit,
            commit_persisted,
            "job running (claimed → running)",
        );

        let reporter = ProgressReporter::new(self.gh.as_ref(), &job);

        // Defensive: a job with no resolved commit can't be benchmarked
        // (an empty SHA would produce a confusing `git fetch ''`). In
        // practice this shouldn't trigger — `pr_comment` + `branch_push`
        // carry their commit from enqueue, and `tag_created` jobs resolve
        // it in preflight above. If a commit is somehow still empty, fail
        // terminally now that we're `running` so it records a terminal
        // state instead of looping via the stuck-claim sweep.
        if job.commit.is_empty() {
            let msg = "no resolved commit; cannot benchmark";
            tracing::error!(job_id = %job.id, git_ref = %job.git_ref_display, "{msg}");
            self.jobs
                .fail(&job, msg, None)
                .await?;
            reporter.failed(msg).await;
            return Ok(());
        }

        reporter.started().await;

        let driver = LibvirtDriver::new(self.config.clone(), self.shell.clone());
        let phase_listener = CommentPhaseListener::new(self.gh.clone(), job.clone());
        let outcome = match driver
            .run_benchmark(&job, &phase_listener)
            .await
        {
            Ok(o) => o,
            Err(e) => {
                // Driver-level error (couldn't even start the run). Log
                // the full anyhow chain locally — reporter.failed posts
                // only a short snippet to the PR.
                tracing::error!(
                    job_id = %job.id,
                    error = ?e,
                    "driver returned setup error",
                );
                let msg = format!("{e:#}");
                let _ = self
                    .jobs
                    .fail(&job, &msg, None)
                    .await;
                reporter.failed(&msg).await;
                return Err(e);
            }
        };

        match outcome.status {
            OutcomeStatus::Ok => {
                tracing::info!(
                    job_id = %job.id,
                    repo = %job.repository,
                    commit = %job.commit,
                    "benchmark completed; persisting result",
                );
                self.jobs
                    .complete(&job, &outcome.summary)
                    .await?;
                tracing::info!(job_id = %job.id, "job result persisted (status=completed)");
                reporter
                    .completed(&outcome.summary)
                    .await;
            }
            OutcomeStatus::Failed(err) => {
                // VM-side or libvirt-side failure (virsh start refused,
                // VM powered off before phase=done, timeout, etc.). The
                // `err` already carries enough context for the DB row;
                // surface it locally too so operators don't have to dig
                // through Postgres to see why a run failed.
                tracing::error!(
                    job_id = %job.id,
                    finish_reason = ?outcome.summary.get("finish_reason"),
                    last_phase = ?outcome.summary.get("last_phase"),
                    error = %err,
                    "benchmark failed",
                );
                self.jobs
                    .fail(&job, &err, Some(&outcome.summary))
                    .await?;
                reporter.failed(&err).await;
            }
        }
        Ok(())
    }

    /// Resolve the commit + (for PR-comment jobs) post the initial PR
    /// comment, mutating `job` in place so the caller's reporter/listener
    /// see populated values. Returns `Some(ResolvedCommit)` when the
    /// commit was newly resolved so the caller persists it via
    /// `start_running`.
    ///
    /// Commit resolution covers two cases when `commit` is empty:
    ///   - a PR-comment job → the PR head SHA (`pr_head_sha`). The authored
    ///     date is unknown so `committed_at` stays `None` (fabricating one
    ///     would corrupt baseline-timeline ordering).
    ///   - a `tag_created` baseline job → resolve the tag ref to its commit +
    ///     authored date (`resolve_commit`). `branch_push` jobs carry their
    ///     commit from enqueue and skip this.
    ///
    /// A resolution failure propagates (`?`): the runner then `fail`s the
    /// still-`claimed` job, which now terminalizes cleanly rather than
    /// looping (the `fail_job` claimed-or-running guard).
    async fn preflight(&self, job: &mut RunnableJob) -> anyhow::Result<Option<ResolvedCommit>> {
        // Commit resolution is FATAL (can't benchmark without a SHA, and the
        // check needs a concrete head_sha). Reporting is NON-FATAL.
        let resolved = self
            .resolve_commit(job)
            .await?;
        if !job.commit.is_empty() {
            self.ensure_reporting(job)
                .await;
        }
        Ok(resolved)
    }

    /// Resolve the job's commit when empty: a PR job → the PR head SHA; a
    /// `tag_created` baseline → the tag ref. `branch_push` carries its commit
    /// from enqueue (non-empty → no-op).
    async fn resolve_commit(
        &self,
        job: &mut RunnableJob,
    ) -> anyhow::Result<Option<ResolvedCommit>> {
        if !job.commit.is_empty() {
            return Ok(None);
        }
        if let ProgressTarget::PullRequest { pr_number, .. } = job.progress {
            let sha = self
                .gh
                .pr_head_sha(job.installation_id, &job.repository, pr_number as u64)
                .await?;
            tracing::info!(job_id = %job.id, pr_number, sha = %sha, "resolved PR head SHA at claim time");
            job.commit = sha.clone();
            return Ok(Some(ResolvedCommit { hash: sha, committed_at: None }));
        }
        if job.git_ref_kind == GitRefKind::Tag {
            // Qualified `tags/<name>` so it unambiguously targets the tag.
            let tag_ref = format!("tags/{}", job.git_ref_display);
            let resolved = self
                .gh
                .resolve_commit(job.installation_id, &job.repository, &tag_ref)
                .await?;
            tracing::info!(job_id = %job.id, tag = %job.git_ref_display, commit = %resolved.hash, "resolved tag to commit at claim time");
            job.commit = resolved.hash.clone();
            return Ok(Some(resolved));
        }
        tracing::warn!(
            job_id = %job.id,
            git_ref = %job.git_ref_display,
            "job has no resolved commit; the empty-commit guard will fail it",
        );
        Ok(None)
    }

    /// Create the Check Run (after the commit is resolved) and/or post the
    /// initial PR comment, per `[reporting]`. NON-FATAL: a reporting error is
    /// logged and the run proceeds. Mutates `job.progress` with the created
    /// ids.
    async fn ensure_reporting(&self, job: &mut RunnableJob) {
        match job.progress.clone() {
            ProgressTarget::PullRequest {
                pr_number,
                mut comment_id,
                mut check_run_id,
                mut check_run_url,
            } => {
                // Check first, so the comment can carry its link.
                if self
                    .config
                    .reporting
                    .pr_report
                    .wants_check()
                    && check_run_id.is_none()
                    && let Some((id, url)) = self.ensure_check(job).await
                {
                    check_run_id = Some(id);
                    check_run_url = url;
                }
                if self
                    .config
                    .reporting
                    .pr_report
                    .wants_comment()
                    && comment_id.is_none()
                    && let Some(id) = self
                        .post_initial_comment(job, pr_number, check_run_url.as_deref())
                        .await
                {
                    comment_id = Some(id);
                }
                job.progress = ProgressTarget::PullRequest {
                    pr_number,
                    comment_id,
                    check_run_id,
                    check_run_url,
                };
            }
            ProgressTarget::CommitCheck { mut check_run_id } => {
                if self
                    .config
                    .reporting
                    .baseline_report
                    .wants_check()
                    && check_run_id.is_none()
                    && let Some((id, _)) = self.ensure_check(job).await
                {
                    check_run_id = Some(id);
                }
                job.progress = ProgressTarget::CommitCheck { check_run_id };
            }
        }
    }

    /// Reconcile-or-create the in-progress Check Run on `job.commit`, persist
    /// its id+url, and return them. Non-fatal (returns `None` on error). The
    /// reconcile (when the App id resolved via `GET /app`) reuses a run created
    /// just before a crash rather than duplicating it.
    async fn ensure_check(&self, job: &RunnableJob) -> Option<(i64, Option<String>)> {
        let external_id = job.id.to_string();
        if let Some(app_id) = self.resolved_app_id().await {
            match self
                .gh
                .find_check_run_by_external_id(
                    job.installation_id,
                    &job.repository,
                    &job.commit,
                    CHECK_NAME,
                    app_id,
                    &external_id,
                )
                .await
            {
                Ok(Some(existing)) => {
                    self.persist_check_id(job, existing.id, existing.html_url.as_deref())
                        .await;
                    tracing::info!(job_id = %job.id, check_run_id = existing.id, "reconciled existing check run");
                    return Some((existing.id, existing.html_url));
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(job_id = %job.id, error = ?e, "check reconcile lookup failed; creating")
                }
            }
        }
        let output = CheckRunOutput {
            title: format!("benchmark {}", job.id),
            summary: format!("Benchmarking commit `{}`…", job.commit),
            text: None,
        };
        match self
            .gh
            .create_check_run(
                job.installation_id,
                &job.repository,
                &job.commit,
                CHECK_NAME,
                &external_id,
                CheckRunUpdate {
                    state: CheckRunState::InProgress,
                    output,
                },
            )
            .await
        {
            Ok(posted) => {
                self.persist_check_id(job, posted.id, posted.html_url.as_deref())
                    .await;
                tracing::info!(job_id = %job.id, check_run_id = posted.id, "created check run");
                Some((posted.id, posted.html_url))
            }
            Err(e) => {
                tracing::warn!(job_id = %job.id, error = ?e, "create check run failed (non-fatal)");
                None
            }
        }
    }

    /// Persist the check run identity (`check_run_created` event) the reclaim
    /// path reads back. Non-fatal, but a failure is logged **loudly**: the
    /// in-memory job keeps the id (so this run still updates the right check),
    /// but the next claim won't see it and could create a DUPLICATE check.
    async fn persist_check_id(&self, job: &RunnableJob, id: i64, url: Option<&str>) {
        if let Err(e) = self
            .jobs
            .set_check_run(job, id, url)
            .await
        {
            tracing::warn!(
                job_id = %job.id,
                check_run_id = id,
                error = ?e,
                "PERSISTING check identity failed — a re-claim may duplicate the check (non-fatal)",
            );
        }
    }

    /// Post the initial "starting" PR comment (carrying the check link when
    /// present), persist its id. Non-fatal (returns `None` on error).
    async fn post_initial_comment(
        &self,
        job: &RunnableJob,
        pr_number: i64,
        check_link: Option<&str>,
    ) -> Option<i64> {
        let link = check_link
            .map(|u| format!(" — [view check ↗]({u})"))
            .unwrap_or_default();
        let body = format!(
            ":construction: starting benchmark `{id}` (commit `{sha}`)…{link}",
            id = job.id,
            sha = job.commit,
        );
        match self
            .gh
            .create_pr_comment(job.installation_id, &job.repository, pr_number as u64, &body)
            .await
        {
            Ok(posted) => {
                if let Err(e) = self
                    .jobs
                    .set_comment_id(job, posted.id)
                    .await
                {
                    tracing::warn!(
                        job_id = %job.id,
                        comment_id = posted.id,
                        error = ?e,
                        "PERSISTING comment id failed — a re-claim may double-post the comment \
                         (non-fatal)",
                    );
                }
                tracing::info!(job_id = %job.id, pr_number, comment_id = posted.id, "posted initial PR comment");
                Some(posted.id)
            }
            Err(e) => {
                tracing::warn!(job_id = %job.id, error = ?e, "post initial PR comment failed (non-fatal)");
                None
            }
        }
    }
}

/// Bridge between the libvirt driver's phase events and the PR comment.
///
/// Two callers drive PR comment edits:
///   * `on_phase` — fires once per transition the driver replays from the in-VM
///     phase journal. Terminal phases (`done`/`error`) bypass the debounce;
///     non-terminal phases are debounced.
///   * `on_heartbeat` — fires periodically while the same phase is current,
///     refreshing the elapsed-time annotation. Always debounced.
///
/// The debounce window (`PR_UPDATE_MIN_INTERVAL`) keeps us well below
/// GitHub's secondary rate limits and avoids spamming the PR's edit
/// history. The very first edit after job pickup is always allowed
/// through (last_pr_update_at starts as None), so the user sees
/// something happen immediately when they `/benchmark`.
struct CommentPhaseListener {
    gh: Arc<dyn GitHubApi>,
    job: RunnableJob,
    state: Mutex<CommentState>,
}

#[derive(Default)]
struct CommentState {
    last_pr_update_at: Option<Instant>,
}

impl CommentPhaseListener {
    fn new(gh: Arc<dyn GitHubApi>, job: RunnableJob) -> Self {
        Self {
            gh,
            job,
            state: Mutex::new(CommentState::default()),
        }
    }

    async fn try_update(&self, phase: &Phase, elapsed: Duration, force: bool) {
        // Only PR jobs with a posted comment get per-phase GitHub edits. The
        // Check Run is a status surface (in_progress → neutral at terminal);
        // it doesn't get per-phase output churn here. Baseline/commit-check
        // jobs (and any job without a comment) surface phases in logs only.
        let ProgressTarget::PullRequest {
            comment_id: Some(comment_id), ..
        } = self.job.progress
        else {
            tracing::debug!(job_id = %self.job.id, phase = %phase, "phase (no comment surface)");
            return;
        };

        // Decide whether to actually send the edit. Brief lock — we
        // don't hold across the network call.
        {
            let mut state = self.state.lock().await;
            if !force
                && let Some(last) = state.last_pr_update_at
                && last.elapsed() < PR_UPDATE_MIN_INTERVAL
            {
                tracing::trace!(
                    phase = %phase,
                    since_last = ?last.elapsed(),
                    "PR update debounced",
                );
                return;
            }
            state.last_pr_update_at = Some(Instant::now());
        }

        let body = format!(
            ":construction: benchmark `{id}` — **{phase}** for `{elapsed}` (commit `{sha}`)",
            id = self.job.id,
            phase = phase,
            elapsed = format_elapsed(elapsed),
            sha = self.job.commit,
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

#[async_trait]
impl PhaseListener for CommentPhaseListener {
    async fn on_phase(&self, phase: &Phase) {
        // Terminal phases bypass debounce so the user sees the final
        // state immediately even if we just edited the comment for a
        // heartbeat. The "elapsed in current phase" annotation isn't
        // meaningful here (we've just entered the phase), so pass 0;
        // the prose still reads naturally as "running for 00:00:00".
        let force = phase.is_terminal();
        self.try_update(phase, Duration::ZERO, force)
            .await;
    }

    async fn on_heartbeat(&self, phase: &Phase, elapsed: Duration) {
        // Heartbeats are always debounced — they're inherently a
        // "still alive" signal and missing one is fine.
        self.try_update(phase, elapsed, false)
            .await;
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Mutex as StdMutex;

    use sbgh_core::config::{
        ApiConfig, BaselineReport, DaemonServerConfig, GitHubConfig, LvmConfig, PathsConfig,
        PrReport, ReportingConfig, StacksBenchConfig, VmConfig,
    };
    use sbgh_core::github::test_support::FakeGitHub;
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::*;
    use crate::job_source::ProgressTarget;
    use crate::libvirt::shell::test_support::{PreparedReply, RecordingShell};

    /// A [`RunnableJobStore`] fake that hands out a single pre-staged job
    /// then goes empty, and records which lifecycle methods the runner
    /// called. Backend-neutral — used to prove the runner drives the
    /// abstraction (not the legacy store) to a terminal call.
    struct FakeSource {
        job: StdMutex<Option<RunnableJob>>,
        calls: StdMutex<Vec<&'static str>>,
        /// The commit `start_running` was called with (for asserting
        /// claim-time tag resolution).
        started_commit: StdMutex<Option<ResolvedCommit>>,
        /// Force `set_check_run` / `set_comment_id` to error — for testing that
        /// a persistence failure is non-fatal (the job still terminalizes).
        fail_persist: std::sync::atomic::AtomicBool,
    }

    impl FakeSource {
        fn new(job: RunnableJob) -> Self {
            Self {
                job: StdMutex::new(Some(job)),
                calls: StdMutex::new(Vec::new()),
                started_commit: StdMutex::new(None),
                fail_persist: std::sync::atomic::AtomicBool::new(false),
            }
        }
        fn fail_persist(&self) {
            self.fail_persist
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
        fn record(&self, m: &'static str) {
            self.calls
                .lock()
                .unwrap()
                .push(m);
        }
        fn calls(&self) -> Vec<&'static str> {
            self.calls
                .lock()
                .unwrap()
                .clone()
        }
        fn started_commit(&self) -> Option<ResolvedCommit> {
            self.started_commit
                .lock()
                .unwrap()
                .clone()
        }
    }

    #[async_trait]
    impl RunnableJobStore for FakeSource {
        async fn claim_next(&self) -> anyhow::Result<Option<RunnableJob>> {
            Ok(self
                .job
                .lock()
                .unwrap()
                .take())
        }
        async fn start_running(
            &self,
            _job: &RunnableJob,
            resolved_commit: Option<ResolvedCommit>,
        ) -> anyhow::Result<()> {
            *self
                .started_commit
                .lock()
                .unwrap() = resolved_commit;
            self.record("start_running");
            Ok(())
        }
        async fn complete(
            &self,
            _job: &RunnableJob,
            _summary: &serde_json::Value,
        ) -> anyhow::Result<()> {
            self.record("complete");
            Ok(())
        }
        async fn fail(
            &self,
            _job: &RunnableJob,
            _error: &str,
            _summary: Option<&serde_json::Value>,
        ) -> anyhow::Result<()> {
            self.record("fail");
            Ok(())
        }
        async fn set_comment_id(&self, _job: &RunnableJob, _comment_id: i64) -> anyhow::Result<()> {
            self.record("set_comment_id");
            if self
                .fail_persist
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                anyhow::bail!("forced set_comment_id failure");
            }
            Ok(())
        }
        async fn set_check_run(
            &self,
            _job: &RunnableJob,
            _check_run_id: i64,
            _html_url: Option<&str>,
        ) -> anyhow::Result<()> {
            self.record("set_check_run");
            if self
                .fail_persist
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                anyhow::bail!("forced set_check_run failure");
            }
            Ok(())
        }
        async fn sweep_stuck_claims(&self, _lease: chrono::Duration) -> anyhow::Result<u64> {
            Ok(0)
        }
    }

    fn test_config(tmp: &TempDir) -> DaemonConfig {
        let p = tmp.path();
        DaemonConfig {
            server: DaemonServerConfig {
                database_url: "postgres://unused".into(),
                service_user: "sbgh".into(),
            },
            github: GitHubConfig {
                client_id: "Iv23litest".into(),
                api_base_url: "https://api.github.test".into(),
                private_key_path: PathBuf::from("/dev/null"),
            },
            vm: VmConfig {
                golden_image: p.join("golden.qcow2"),
                build_vcpus: 4,
                bench_vcpus: 2,
                build_memory: sbgh_core::memory::MemorySize::from_gib(16),
                bench_memory: sbgh_core::memory::MemorySize::from_gib(8),
                boot_disk_gib: 64,
                job_timeout_secs: 30,
                network: "default".into(),
                poll_interval_secs: 1,
                heartbeat_interval_secs: 60,
            },
            paths: PathsConfig {
                jobs_dir: p.join("jobs"),
                git_mirror: p.join("mirror.git"),
                results_tmpfs_root: p.join("tmpfs"),
                results_archive_dir: p.join("archive"),
                sccache_dir: p.join("sccache"),
                virsh_binary: "/usr/bin/virsh".into(),
                sudo_binary: "/usr/bin/sudo".into(),
                qemu_img_binary: "/usr/bin/qemu-img".into(),
                cloud_localds_binary: "/usr/bin/cloud-localds".into(),
                git_binary: "/usr/bin/git".into(),
            },
            lvm: LvmConfig {
                vg_name: "sbgh-vg".into(),
                thinpool: "thinpool".into(),
                chainstate_base_prefix: "mainnet-".into(),
                chainstate_snapshot_size_gib: None,
            },
            stacks_bench: StacksBenchConfig { default_args: String::new() },
            api: ApiConfig {
                listen: vec!["127.0.0.1:8787".into()],
                cookie_path: "/tmp/sbgh-test.cookie".into(),
                ingest_token: None,
            },
            // Existing runner tests assert comment-only behaviour; check-mode
            // is exercised by dedicated tests that override this.
            reporting: ReportingConfig {
                pr_report: PrReport::Comment,
                baseline_report: BaselineReport::None,
            },
        }
    }

    /// The runner drives ANY `RunnableJobStore` (here a baseline job with
    /// reporting disabled — `baseline_report = none`, the default
    /// `test_config`) through `start_running` and, when the driver reports
    /// failure, `fail` — proving the runner is backend-agnostic and a
    /// no-surface job never touches GitHub.
    #[tokio::test]
    async fn run_once_drives_unreported_baseline_job_to_terminal_fail() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(&tmp); // baseline_report = none

        let job = RunnableJob {
            id: Uuid::new_v4(),
            repository: "acme/widgets".into(),
            commit: "abc123".into(), // pre-resolved → preflight is a no-op
            git_ref_display: "develop".into(),
            git_ref_kind: GitRefKind::Branch,
            installation_id: 7,
            bench_args: vec![],
            progress: ProgressTarget::CommitCheck { check_run_id: None },
            claim_token: Some(Uuid::new_v4()),
        };
        let source = Arc::new(FakeSource::new(job));

        // With reporting off, GitHub must never be touched; a recording shell
        // that fails the first provisioning command drives the driver to a
        // Failed outcome.
        let gh = Arc::new(FakeGitHub::new());
        let shell = Arc::new(RecordingShell::new());
        shell.reply(PreparedReply::fail(b"boom: git fetch failed"));

        let runner = Runner::new(config, source.clone(), gh.clone(), shell);
        let processed = runner
            .run_once()
            .await
            .unwrap();
        assert!(processed, "claimed + executed one job");

        // Lifecycle: transitioned to running, then failed (driver setup
        // error → Failed outcome). No reporting work for a no-surface job.
        assert_eq!(source.calls(), vec!["start_running", "fail"]);
        assert!(
            gh.calls().is_empty(),
            "a job with reporting disabled must make no GitHub calls (no comment or check)"
        );

        // Queue now empty.
        assert!(
            !runner
                .run_once()
                .await
                .unwrap()
        );
    }

    /// A `tag_created` job is enqueued with no commit; the runner
    /// resolves the tag → commit at claim time (via `resolve_commit`) and
    /// hands the resolved commit (with its authored date) to
    /// `start_running`. Proves the claim-time tag-resolution path.
    #[tokio::test]
    async fn run_once_resolves_tag_commit_in_preflight() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(&tmp);

        let job = RunnableJob {
            id: Uuid::new_v4(),
            repository: "octo/core".into(),
            commit: String::new(), // unresolved — a tag job
            git_ref_display: "release/1.2".into(),
            git_ref_kind: GitRefKind::Tag,
            installation_id: 7,
            bench_args: vec![],
            progress: ProgressTarget::CommitCheck { check_run_id: None },
            claim_token: Some(Uuid::new_v4()),
        };
        let source = Arc::new(FakeSource::new(job));

        let date = chrono::DateTime::parse_from_rfc3339("2026-05-30T12:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let gh = Arc::new(FakeGitHub::new());
        // The runner resolves via the canonical qualified `tags/<name>`
        // form (note the slashy tag name `release/1.2`).
        gh.set_commit("octo/core", "tags/release/1.2", "tagsha123", Some(date));
        // Shell fails the first command → Failed outcome; we only care
        // that resolution ran before the run.
        let shell = Arc::new(RecordingShell::new());
        shell.reply(PreparedReply::fail(b"boom"));

        let runner = Runner::new(config, source.clone(), gh.clone(), shell);
        assert!(
            runner
                .run_once()
                .await
                .unwrap()
        );

        // The tag was resolved and the resolved commit (+ date) handed to
        // start_running.
        let resolved = source
            .started_commit()
            .expect("start_running received a resolved commit");
        assert_eq!(resolved.hash, "tagsha123");
        assert_eq!(resolved.committed_at, Some(date));
        assert!(
            gh.calls()
                .iter()
                .any(|c| matches!(
                    c,
                    sbgh_core::github::test_support::FakeCall::ResolveCommit { .. }
                )),
            "runner must call resolve_commit for a tag job"
        );
    }

    use sbgh_core::github::test_support::FakeCall;

    fn config_with(tmp: &TempDir, pr: PrReport, baseline: BaselineReport) -> DaemonConfig {
        let mut c = test_config(tmp);
        c.reporting = ReportingConfig {
            pr_report: pr,
            baseline_report: baseline,
        };
        c
    }

    /// Drive a (pre-resolved) job to a terminal `fail` via a failing shell,
    /// returning the FakeGitHub + FakeSource call records for assertions.
    async fn run_to_fail(
        config: DaemonConfig,
        job: RunnableJob,
    ) -> (Vec<FakeCall>, Vec<&'static str>) {
        // Jobs here carry a pre-resolved commit, so preflight skips the GitHub
        // commit-resolution call; a failing shell drives a terminal `fail`.
        let source = Arc::new(FakeSource::new(job));
        let gh = Arc::new(FakeGitHub::new());
        let shell = Arc::new(RecordingShell::new());
        shell.reply(PreparedReply::fail(b"boom: provisioning failed"));
        let runner = Runner::new(config, source.clone(), gh.clone(), shell);
        runner
            .run_once()
            .await
            .unwrap();
        (gh.calls(), source.calls())
    }

    fn pr_job(commit: &str, check_run_id: Option<i64>) -> RunnableJob {
        RunnableJob {
            id: Uuid::new_v4(),
            repository: "acme/widgets".into(),
            commit: commit.into(),
            git_ref_display: "feature".into(),
            git_ref_kind: GitRefKind::Branch,
            installation_id: 7,
            bench_args: vec![],
            progress: ProgressTarget::PullRequest {
                pr_number: 7,
                comment_id: None,
                check_run_id,
                check_run_url: None,
            },
            claim_token: Some(Uuid::new_v4()),
        }
    }

    /// PR job in `both` mode: creates an in-progress check, posts a comment
    /// LINKING the check, then completes the check (neutral) on failure.
    #[tokio::test]
    async fn pr_job_both_creates_check_and_linked_comment() {
        let tmp = TempDir::new().unwrap();
        let config = config_with(&tmp, PrReport::Both, BaselineReport::None);
        let (calls, src) = run_to_fail(config, pr_job("abc123", None)).await;

        assert!(
            calls
                .iter()
                .any(|c| matches!(c, FakeCall::CreateCheckRun { .. })),
            "created the PR-head check"
        );
        let comment = calls
            .iter()
            .find_map(|c| match c {
                FakeCall::CreateComment { body, .. } => Some(body.clone()),
                _ => None,
            })
            .expect("posted an initial comment");
        assert!(comment.contains("view check"), "comment links the check: {comment}");
        assert!(
            calls
                .iter()
                .any(|c| matches!(c, FakeCall::UpdateCheckRun { .. })),
            "completed the check on terminal"
        );
        assert!(src.contains(&"set_check_run"), "persisted check identity");
    }

    /// PR job in `comment` mode: no check at all, just the comment.
    #[tokio::test]
    async fn pr_job_comment_mode_skips_check() {
        let tmp = TempDir::new().unwrap();
        let config = config_with(&tmp, PrReport::Comment, BaselineReport::None);
        let (calls, _src) = run_to_fail(config, pr_job("abc123", None)).await;
        assert!(
            calls
                .iter()
                .any(|c| matches!(c, FakeCall::CreateComment { .. })),
            "posted the comment"
        );
        assert!(
            !calls
                .iter()
                .any(|c| matches!(c, FakeCall::CreateCheckRun { .. })),
            "comment mode must not create a check"
        );
    }

    /// Baseline job in `check` mode: a commit check, no comment.
    #[tokio::test]
    async fn baseline_job_check_mode_creates_commit_check_no_comment() {
        let tmp = TempDir::new().unwrap();
        let config = config_with(&tmp, PrReport::Both, BaselineReport::Check);
        let job = RunnableJob {
            progress: ProgressTarget::CommitCheck { check_run_id: None },
            ..pr_job("abc123", None)
        };
        let (calls, src) = run_to_fail(config, job).await;
        assert!(
            calls
                .iter()
                .any(|c| matches!(c, FakeCall::CreateCheckRun { .. })),
            "created the commit check"
        );
        assert!(
            calls
                .iter()
                .any(|c| matches!(c, FakeCall::UpdateCheckRun { .. })),
            "completed the commit check"
        );
        assert!(
            !calls
                .iter()
                .any(|c| matches!(c, FakeCall::CreateComment { .. })),
            "a baseline has no comment surface"
        );
        assert!(src.contains(&"set_check_run"));
    }

    /// The reconcile reuses a check created just before a crash (same
    /// external_id) instead of creating a duplicate.
    #[tokio::test]
    async fn check_reconcile_reuses_existing_run() {
        let tmp = TempDir::new().unwrap();
        let config = config_with(&tmp, PrReport::Check, BaselineReport::None);
        let job = pr_job("abc123", None);
        let job_id = job.id;
        let source = Arc::new(FakeSource::new(job));
        let gh = Arc::new(FakeGitHub::new());
        gh.set_head_sha("acme/widgets", 7, "abc123");
        // A prior run exists for (repo, sha, name, external_id=job id).
        gh.set_existing_check_run("acme/widgets", "abc123", CHECK_NAME, &job_id.to_string(), 9999);
        let shell = Arc::new(RecordingShell::new());
        shell.reply(PreparedReply::fail(b"boom"));
        let runner = Runner::new(config, source.clone(), gh.clone(), shell);
        runner
            .run_once()
            .await
            .unwrap();

        let calls = gh.calls();
        assert!(
            calls
                .iter()
                .any(|c| matches!(c, FakeCall::FindCheckRun { .. })),
            "reconcile looked up by external_id"
        );
        assert!(
            !calls
                .iter()
                .any(|c| matches!(c, FakeCall::CreateCheckRun { .. })),
            "reused the existing run; must NOT create a duplicate"
        );
        // The reused id is persisted so the terminal update targets it.
        assert!(
            matches!(
                calls
                    .iter()
                    .find(|c| matches!(c, FakeCall::UpdateCheckRun { .. })),
                Some(FakeCall::UpdateCheckRun { check_run_id: 9999, .. })
            ),
            "completed the reused check 9999"
        );
    }

    /// Non-fatal reporting: if BOTH `create_check_run` and `create_pr_comment`
    /// error, the benchmark still runs to a terminal state.
    #[tokio::test]
    async fn reporting_create_failures_do_not_fail_the_job() {
        let tmp = TempDir::new().unwrap();
        let config = config_with(&tmp, PrReport::Both, BaselineReport::None);
        let source = Arc::new(FakeSource::new(pr_job("abc123", None)));
        let gh = Arc::new(FakeGitHub::new());
        gh.fail_create_check_run();
        gh.fail_create_comment();
        let shell = Arc::new(RecordingShell::new());
        shell.reply(PreparedReply::fail(b"boom"));
        let runner = Runner::new(config, source.clone(), gh.clone(), shell);
        runner
            .run_once()
            .await
            .unwrap();

        let calls = gh.calls();
        assert!(
            calls
                .iter()
                .any(|c| matches!(c, FakeCall::CreateCheckRun { .. })),
            "attempted the check"
        );
        assert!(
            calls
                .iter()
                .any(|c| matches!(c, FakeCall::CreateComment { .. })),
            "attempted the comment"
        );
        // Both surfaces errored, yet the job reached terminal `fail`.
        assert_eq!(source.calls(), vec!["start_running", "fail"]);
    }

    /// Non-fatal reporting: if PERSISTING the surface ids fails (the
    /// `check_run_created` / `comment_posted` events), the job still
    /// terminalizes and the in-memory ids are retained so the terminal update
    /// still targets the live check.
    #[tokio::test]
    async fn reporting_persistence_failure_does_not_fail_the_job() {
        let tmp = TempDir::new().unwrap();
        let config = config_with(&tmp, PrReport::Both, BaselineReport::None);
        let source = Arc::new(FakeSource::new(pr_job("abc123", None)));
        source.fail_persist(); // set_check_run / set_comment_id will error
        let gh = Arc::new(FakeGitHub::new());
        let shell = Arc::new(RecordingShell::new());
        shell.reply(PreparedReply::fail(b"boom"));
        let runner = Runner::new(config, source.clone(), gh.clone(), shell);
        runner
            .run_once()
            .await
            .unwrap();

        let src = source.calls();
        assert!(src.contains(&"set_check_run"), "attempted to persist the check id");
        assert!(src.contains(&"fail"), "job reached terminal fail despite persistence errors");
        // The check id was retained in-memory (persistence failure swallowed),
        // so the terminal completion still updated the live check.
        let calls = gh.calls();
        assert!(
            calls
                .iter()
                .any(|c| matches!(c, FakeCall::UpdateCheckRun { .. })),
            "terminal update used the retained check id"
        );
    }

    /// If `GET /app` fails, the reconcile is skipped (no lookup) but the check
    /// is still created and the job terminalizes. The `OnceCell` is left empty
    /// (`get_or_try_init` doesn't cache the error), so a later job self-heals.
    #[tokio::test]
    async fn app_id_resolution_failure_skips_reconcile_but_not_the_job() {
        let tmp = TempDir::new().unwrap();
        let config = config_with(&tmp, PrReport::Check, BaselineReport::None);
        let source = Arc::new(FakeSource::new(pr_job("abc123", None)));
        let gh = Arc::new(FakeGitHub::new());
        gh.fail_current_app_id();
        let shell = Arc::new(RecordingShell::new());
        shell.reply(PreparedReply::fail(b"boom"));
        let runner = Runner::new(config, source.clone(), gh.clone(), shell);
        runner
            .run_once()
            .await
            .unwrap();

        let calls = gh.calls();
        assert!(
            !calls
                .iter()
                .any(|c| matches!(c, FakeCall::FindCheckRun { .. })),
            "reconcile is skipped when the App id is unknown"
        );
        assert!(
            calls
                .iter()
                .any(|c| matches!(c, FakeCall::CreateCheckRun { .. })),
            "the check is still created"
        );
        assert!(
            source
                .calls()
                .contains(&"fail"),
            "the job still reached terminal"
        );
    }
}
