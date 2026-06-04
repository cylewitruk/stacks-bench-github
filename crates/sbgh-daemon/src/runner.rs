//! Main daemon loop.
//!
//! On pickup the daemon:
//!   1. Resolves the commit via the GitHub API (the handler can't — no App
//!      credentials): a PR job's head SHA, or a `tag_created` job's tag →
//!      commit. Writes it back to the job row. (FATAL — no SHA, no run.)
//!   2. Reporting (NON-FATAL, per `[reporting]`): creates the Check Run on the
//!      resolved commit and — for PR jobs — posts the "starting" comment
//!      linking it, persisting both ids so a re-claim reuses them.
//!   3. Runs the benchmark; concludes the check `success`/`failure` (did it
//!      RUN?) + updates the comment at the end.

use std::sync::Arc;
use std::time::Duration;

use sbgh_core::config::DaemonConfig;
use sbgh_core::github::GitHubApi;
use tokio::sync::{OnceCell, mpsc, oneshot};

use crate::bench_recipe::BenchRecipe;
use crate::events::{ChannelSink, Terminal, WorkerEvent};
use crate::job_source::{ProgressTarget, RunnableJob, RunnableJobStore};
use crate::libvirt::Shell;
use crate::recipe::{Recipe, TaskContext, TaskOutcome, TaskStatus};
use crate::reporter::{Prepared, Reporter};

const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Lease after which a job stranded in `claimed` (claimed but never
/// transitioned to `running` — daemon crashed or preflight errored
/// between `claim_next` and `start_running`) is reclaimed to `queued` by
/// the stuck-claim sweep. The claim→running window is normally
/// sub-second (a GH API call at most), so a few minutes is ample slack
/// without leaving a crashed claim stuck for long.
const CLAIM_LEASE_MINUTES: i64 = 5;

/// Bounded capacity of the per-job worker→reporter event channel. Phase
/// transitions are few and heartbeats are droppable, so a small buffer
/// absorbs bursts without ever stalling the worker for long.
const EVENT_BUFFER: usize = 32;

pub struct Runner {
    config: Arc<DaemonConfig>,
    jobs: Arc<dyn RunnableJobStore>,
    gh: Arc<dyn GitHubApi>,
    shell: Arc<dyn Shell>,
    /// Shared App-id cache, resolved via `GET /app` and cached on **success**
    /// only — a `get_or_try_init` that leaves the cell empty on error, so a
    /// transient blip is retried on the next job rather than disabling the
    /// reconcile for the whole process. Cloned into each job's reporter, which
    /// resolves it lazily (see [`resolved_app_id`]).
    app_id: Arc<OnceCell<i64>>,
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
            app_id: Arc::new(OnceCell::new()),
        }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        tracing::info!("daemon started");
        // The App id is resolved lazily by the first job whose reporter actually
        // wants a Check Run (shared, cached, self-healing) — no startup
        // `GET /app` for a daemon that only ever runs no-report jobs.
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

    /// Claim-to-terminal for one job: spawn the per-job [`Reporter`] (which
    /// owns `prepare` + all GitHub/DB side-effects), run the worker inline (the
    /// recipe), and hand the terminal outcome back over the channel. Returns
    /// the reporter's result so setup-level failures still back off the loop.
    async fn execute(&self, job: RunnableJob) -> anyhow::Result<()> {
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

        let recipe =
            BenchRecipe::new(self.config.clone(), self.shell.clone(), job.bench_args.clone());

        let (events_tx, events_rx) = mpsc::channel(EVENT_BUFFER);
        let (prepared_tx, prepared_rx) = oneshot::channel();

        // The reporter task owns prepare + reporting + the terminal write. It
        // gets the shared App-id cache (resolved lazily, only if a check is
        // wanted) for the Check Run reconcile.
        let reporter = Reporter::new(
            self.config.clone(),
            self.jobs.clone(),
            self.gh.clone(),
            self.app_id.clone(),
            job.clone(),
        );
        let handle = tokio::spawn(reporter.run(events_rx, prepared_tx));

        // The worker runs inline: it waits for prepare's resolved commit, runs
        // the recipe (emitting progress to the channel), and sends the terminal.
        run_worker(&recipe, &job, prepared_rx, events_tx).await;

        // Surface the reporter's result (setup-level failures back off the loop;
        // a panic in the reporter task becomes an iteration error).
        match handle.await {
            Ok(result) => result,
            Err(join_err) => Err(anyhow::anyhow!("reporter task panicked: {join_err}")),
        }
    }
}

/// The inline worker: await prepare's go/abort signal, run the recipe (emitting
/// progress onto the channel), and send the terminal outcome. Pure execution —
/// it never touches GitHub or the DB; the reporter owns all of that.
async fn run_worker(
    recipe: &BenchRecipe,
    job: &RunnableJob,
    prepared_rx: oneshot::Receiver<Prepared>,
    events_tx: mpsc::Sender<WorkerEvent>,
) {
    // Wait for the reporter to finish `prepare` and hand us the resolved
    // commit. `Abort` (or a dropped sender) means prepare failed / the job
    // won't run — the reporter already handled any reporting, so we stop.
    let commit = match prepared_rx.await {
        Ok(Prepared::Run { commit }) => commit,
        Ok(Prepared::Abort) | Err(_) => return,
    };

    let sink = ChannelSink::new(events_tx.clone());
    let ctx = TaskContext {
        job_id: job.id,
        repository: &job.repository,
        commit: &commit,
    };
    let terminal = match recipe
        .execute(&ctx, &sink)
        .await
    {
        Ok(outcome) => match outcome.status() {
            TaskStatus::Completed => Terminal::Completed {
                summary: outcome.summary().clone(),
            },
            TaskStatus::Failed(error) => Terminal::Failed {
                error,
                summary: outcome.summary().clone(),
            },
        },
        // A setup-level error (the run couldn't start). Log the full anyhow
        // chain locally; the reporter posts only a short snippet to the PR.
        Err(e) => {
            tracing::error!(job_id = %job.id, error = ?e, "recipe returned setup error");
            Terminal::SetupError { error: format!("{e:#}") }
        }
    };

    // Send the terminal; dropping `events_tx` + the sink's clone afterwards
    // closes the channel, ending the reporter's drain loop.
    let _ = events_tx
        .send(WorkerEvent::Finished(terminal))
        .await;
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Mutex as StdMutex;

    use async_trait::async_trait;
    use sbgh_core::config::{
        ApiConfig, BaselineReport, DaemonServerConfig, GitHubConfig, LvmConfig, PathsConfig,
        PrReport, ReportingConfig, StacksBenchConfig, VmConfig,
    };
    use sbgh_core::github::test_support::FakeGitHub;
    use sbgh_core::models::{GitRefKind, ResolvedCommit};
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::*;
    use crate::job_source::ProgressTarget;
    use crate::libvirt::shell::test_support::{PreparedReply, RecordingShell};
    use crate::reporter::CHECK_NAME;

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
    /// LINKING the check, then completes the check (`failure`) on failure.
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
