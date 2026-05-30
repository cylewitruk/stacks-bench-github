//! Main orchestrator loop.
//!
//! On pickup the orchestrator:
//!   1. Resolves the commit via the GitHub API (the handler can't — no App
//!      credentials): a PR-comment job's head SHA, or a `tag_created` job's tag
//!      → commit. Writes it back to the job row.
//!   2. For PR jobs, posts the initial "starting" PR comment and persists the
//!      returned comment id so the phase-progress listener can edit it.
//!   3. Runs the benchmark.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use sbgh_core::config::OrchestratorConfig;
use sbgh_core::github::GitHubApi;
use sbgh_core::models::{GitRefKind, ResolvedCommit};
use tokio::sync::Mutex;

use crate::job_source::{ProgressTarget, RunnableJob, RunnableJobStore};
use crate::libvirt::{LibvirtDriver, OutcomeStatus, Phase, PhaseListener, Shell, format_elapsed};
use crate::progress::ProgressReporter;

const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Lease after which a job stranded in `claimed` (claimed but never
/// transitioned to `running` — orchestrator crashed or preflight errored
/// between `claim_next` and `start_running`) is reclaimed to `queued` by
/// the stuck-claim sweep. The claim→running window is normally
/// sub-second (a GH API call at most), so a few minutes is ample slack
/// without leaving a crashed claim stuck for long. Only the new-schema
/// `JobV2Source` has a `claimed` state; legacy's sweep is a no-op.
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
    config: Arc<OrchestratorConfig>,
    jobs: Arc<dyn RunnableJobStore>,
    gh: Arc<dyn GitHubApi>,
    shell: Arc<dyn Shell>,
}

impl Runner {
    pub fn new(
        config: OrchestratorConfig,
        jobs: Arc<dyn RunnableJobStore>,
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
        let lease = chrono::Duration::minutes(CLAIM_LEASE_MINUTES);
        loop {
            // Recover jobs stranded in `claimed` (crash / preflight error
            // between claim and start_running) before claiming the next.
            // No-op for the legacy backend. A single job runs to
            // completion before the loop turns over, so this fires
            // between jobs and at startup — exactly when a stranded
            // claim from a prior (crashed) run needs reclaiming.
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
        tracing::info!(job_id = %job.id, repo = %job.repository, "starting job");

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

        // Transition to running, persisting the resolved commit. Legacy
        // is already `running` (this just writes the SHA); the new schema
        // does `claimed → running` here.
        self.jobs
            .start_running(&job, resolved_commit)
            .await?;

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
            let _ = reporter.failed(msg).await;
            return Ok(());
        }

        reporter.started().await?;

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
                let _ = reporter.failed(&msg).await;
                return Err(e);
            }
        };

        match outcome.status {
            OutcomeStatus::Ok => {
                self.jobs
                    .complete(&job, &outcome.summary)
                    .await?;
                reporter
                    .completed(&outcome.summary)
                    .await?;
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
                reporter.failed(&err).await?;
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
        let ProgressTarget::PullRequestComment { pr_number, comment_id } = job.progress else {
            // Non-PR (baseline) job. Resolve a tag's commit if needed;
            // branch_push jobs already carry their commit. Resolve via
            // GitHub's canonical qualified form `tags/<name>` so it
            // unambiguously targets the tag (not a same-named branch).
            if job.commit.is_empty() && job.git_ref_kind == GitRefKind::Tag {
                let tag_ref = format!("tags/{}", job.git_ref_display);
                let resolved = self
                    .gh
                    .resolve_commit(job.installation_id, &job.repository, &tag_ref)
                    .await?;
                tracing::info!(
                    job_id = %job.id,
                    tag = %job.git_ref_display,
                    commit = %resolved.hash,
                    "resolved tag to commit at claim time",
                );
                job.commit = resolved.hash.clone();
                return Ok(Some(resolved));
            }
            if job.commit.is_empty() {
                tracing::warn!(
                    job_id = %job.id,
                    git_ref = %job.git_ref_display,
                    "new-schema job has no resolved commit; the empty-commit guard will fail it",
                );
            }
            return Ok(None);
        };

        let resolved = if job.commit.is_empty() {
            let sha = self
                .gh
                .pr_head_sha(job.installation_id, &job.repository, pr_number as u64)
                .await?;
            job.commit = sha.clone();
            Some(ResolvedCommit { hash: sha, committed_at: None })
        } else {
            None
        };

        if comment_id.is_none() {
            let body = format!(
                ":construction: starting benchmark `{id}` (commit `{sha}`)…",
                id = job.id,
                sha = job.commit,
            );
            let posted = self
                .gh
                .create_pr_comment(job.installation_id, &job.repository, pr_number as u64, &body)
                .await?;
            self.jobs
                .set_comment_id(job, posted.id)
                .await?;
            job.progress = ProgressTarget::PullRequestComment {
                pr_number,
                comment_id: Some(posted.id),
            };
        }

        Ok(resolved)
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
        // Only PR-comment jobs with a posted comment get GitHub edits.
        // New-schema (LogOnly) jobs surface phase changes via logs only
        // in slice 10; both comment posting and intermediate phase
        // `job_event` rows are slice-11 concerns.
        let ProgressTarget::PullRequestComment {
            comment_id: Some(comment_id), ..
        } = self.job.progress
        else {
            tracing::debug!(job_id = %self.job.id, phase = %phase, "phase (log-only)");
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
        GitHubConfig, JobSource, JobsConfig, LvmConfig, OrchestratorServerConfig, PathsConfig,
        StacksBenchConfig, VmConfig,
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
    }

    impl FakeSource {
        fn new(job: RunnableJob) -> Self {
            Self {
                job: StdMutex::new(Some(job)),
                calls: StdMutex::new(Vec::new()),
                started_commit: StdMutex::new(None),
            }
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
            Ok(())
        }
        async fn sweep_stuck_claims(&self, _lease: chrono::Duration) -> anyhow::Result<u64> {
            Ok(0)
        }
    }

    fn test_config(tmp: &TempDir) -> OrchestratorConfig {
        let p = tmp.path();
        OrchestratorConfig {
            server: OrchestratorServerConfig {
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
            jobs: JobsConfig { source: JobSource::Legacy },
        }
    }

    /// The runner drives ANY `RunnableJobStore` (here a new-schema-shaped
    /// `LogOnly` job, no PR) through `start_running` and, when the driver
    /// reports failure, `fail` — proving the runner is backend-agnostic
    /// and the log-only progress path doesn't touch GitHub.
    #[tokio::test]
    async fn run_once_drives_log_only_job_to_terminal_fail() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(&tmp);

        let job = RunnableJob {
            id: Uuid::new_v4(),
            repository: "acme/widgets".into(),
            commit: "abc123".into(), // pre-resolved → preflight is a no-op
            git_ref_display: "develop".into(),
            git_ref_kind: GitRefKind::Branch,
            installation_id: 7,
            bench_args: vec![],
            progress: ProgressTarget::LogOnly,
            claim_token: Some(Uuid::new_v4()),
        };
        let source = Arc::new(FakeSource::new(job));

        // FakeGitHub must never be called for a LogOnly job; a recording
        // shell that fails the first provisioning command drives the
        // driver to a Failed outcome.
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
        // error → Failed outcome). No comment work for a LogOnly job.
        assert_eq!(source.calls(), vec!["start_running", "fail"]);
        assert!(
            !gh.calls()
                .iter()
                .any(|c| matches!(
                    c,
                    sbgh_core::github::test_support::FakeCall::CreateComment { .. }
                        | sbgh_core::github::test_support::FakeCall::UpdateComment { .. }
                )),
            "LogOnly job must not post or edit a GitHub comment"
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
            progress: ProgressTarget::LogOnly,
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
}
