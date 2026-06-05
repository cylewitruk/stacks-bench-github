//! Main daemon loop — a **coordinator** over a pool of concurrent job tasks
//! (roadmap-v5 Phase 3).
//!
//! The coordinator claims while execution slots are free (a `Semaphore` sized
//! by `[runner].max_concurrent_jobs`, default 1) and spawns one task per job
//! into a `JoinSet`; the slot frees when the task finishes. Claims stay serial
//! inside the loop — only *execution* parallelizes. Each job task
//! ([`JobDeps::run`]) spawns the per-job [`Reporter`] (commit resolution +
//! Check Run / comment per `[reporting]` + the terminal DB write) and runs the
//! worker (the benchmark recipe) inline against it.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use sbgh_core::config::DaemonConfig;
use sbgh_core::github::GitHubApi;
use tokio::sync::{OnceCell, OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::task::{Id, JoinError, JoinSet};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::bench_recipe::BenchRecipe;
use crate::events::{ChannelSink, Terminal, WorkerEvent};
use crate::job_source::{ProgressTarget, RunnableJob, RunnableJobStore};
use crate::libvirt::Shell;
use crate::recipe::{Recipe, TaskContext, TaskOutcome, TaskStatus};
use crate::reporter::{Prepared, Reporter};

/// How often the coordinator wakes to re-sweep + top up slots while jobs run.
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

/// The runner's shared handles, cloned into each job task so it can run on its
/// own (spawned) without borrowing the coordinator.
#[derive(Clone)]
struct JobDeps {
    config: Arc<DaemonConfig>,
    jobs: Arc<dyn RunnableJobStore>,
    gh: Arc<dyn GitHubApi>,
    shell: Arc<dyn Shell>,
    /// Shared App-id cache, resolved via `GET /app` and cached on **success**
    /// only — a `get_or_try_init` that leaves the cell empty on error, so a
    /// transient blip is retried on the next job rather than disabling the
    /// reconcile for the whole process. Each reporter resolves it lazily (see
    /// [`crate::reporter::resolved_app_id`]).
    app_id: Arc<OnceCell<i64>>,
}

pub struct Runner {
    deps: JobDeps,
    /// Max jobs executed concurrently (`[runner].max_concurrent_jobs`, ≥ 1).
    max_concurrent: usize,
}

impl Runner {
    pub fn new(
        config: DaemonConfig,
        jobs: Arc<dyn RunnableJobStore>,
        gh: Arc<dyn GitHubApi>,
        shell: Arc<dyn Shell>,
    ) -> Self {
        let max_concurrent = config
            .runner
            .max_concurrent_jobs
            .max(1);
        Self {
            deps: JobDeps {
                config: Arc::new(config),
                jobs,
                gh,
                shell,
                app_id: Arc::new(OnceCell::new()),
            },
            max_concurrent,
        }
    }

    /// The coordinator loop: sweep stranded claims, fill every free slot from
    /// the queue, then wait for a task to free a slot or the poll tick. Runs
    /// forever. The per-iteration machinery lives on [`Coordinator`] so it's
    /// unit-testable without the loop.
    pub async fn run(self) -> anyhow::Result<()> {
        tracing::info!(max_concurrent = self.max_concurrent, "daemon started");
        let lease = chrono::Duration::minutes(CLAIM_LEASE_MINUTES);
        let mut coord = Coordinator::new(self.deps, self.max_concurrent);
        loop {
            coord.sweep(lease).await;
            coord.fill_slots().await;
            coord
                .wait_for_progress()
                .await;
        }
    }

    /// Claim + run one job to completion on a standalone slot. The run-loop
    /// test seam — drives the claim→terminal lifecycle without the coordinator.
    /// Returns `Ok(true)` if a job was processed, `Ok(false)` if idle.
    #[cfg(test)]
    pub async fn run_once(&self) -> anyhow::Result<bool> {
        match self
            .deps
            .jobs
            .claim_next()
            .await?
        {
            Some(job) => {
                let permit = Arc::new(Semaphore::new(1))
                    .try_acquire_owned()
                    .expect("a fresh semaphore always has a permit");
                self.deps
                    .clone()
                    .run(job, permit, CancellationToken::new())
                    .await?;
                Ok(true)
            }
            None => Ok(false),
        }
    }
}

/// The concurrent run loop's state: the slot `Semaphore`, the in-flight task
/// set, and the task→job map. Split out from [`Runner::run`] so the fill/reap
/// machinery (the load-bearing Phase-3 code) is unit-testable without the
/// infinite loop.
struct Coordinator {
    deps: JobDeps,
    slots: Arc<Semaphore>,
    tasks: JoinSet<anyhow::Result<()>>,
    /// task id → job id, so a panicking task can be logged with context.
    task_jobs: HashMap<Id, Uuid>,
    /// The daemon-wide shutdown token. Each job gets a **child** token, so an
    /// abort cancels every in-flight run at once, while one job's own
    /// cancellation never propagates back up to siblings (Phase 4 wires the
    /// trigger; in this slice it's never fired).
    shutdown: CancellationToken,
}

impl Coordinator {
    fn new(deps: JobDeps, max_concurrent: usize) -> Self {
        Self {
            deps,
            slots: Arc::new(Semaphore::new(max_concurrent)),
            tasks: JoinSet::new(),
            task_jobs: HashMap::new(),
            shutdown: CancellationToken::new(),
        }
    }

    /// Spawned-but-not-yet-reaped tasks. An **upper bound** on live jobs: a
    /// task can finish (freeing its semaphore permit, so a slot may top up)
    /// before the next `join_next` reaps it. The real concurrency cap is
    /// the semaphore, not this count.
    #[cfg(test)]
    fn in_flight(&self) -> usize {
        self.tasks.len()
    }

    /// Reclaim jobs stranded mid-claim (crash / preflight error between claim
    /// and start_running). The lease keeps it off actively-`running` jobs.
    async fn sweep(&self, lease: chrono::Duration) {
        match self
            .deps
            .jobs
            .sweep_stuck_claims(lease)
            .await
        {
            Ok(n) if n > 0 => tracing::warn!(recovered = n, "recovered stuck `claimed` jobs"),
            Ok(_) => {}
            Err(e) => tracing::error!(error = ?e, "stuck-claim sweep failed"),
        }
    }

    /// Claim into every free slot, spawning a job task per claim (the permit is
    /// moved into the task and frees the slot on completion). Returns how many
    /// were spawned this call.
    async fn fill_slots(&mut self) -> usize {
        let mut spawned = 0;
        while let Ok(permit) = self
            .slots
            .clone()
            .try_acquire_owned()
        {
            match self
                .deps
                .jobs
                .claim_next()
                .await
            {
                Ok(Some(job)) => {
                    let job_id = job.id;
                    let token = self.shutdown.child_token();
                    let id = self
                        .tasks
                        .spawn(
                            self.deps
                                .clone()
                                .run(job, permit, token),
                        )
                        .id();
                    self.task_jobs
                        .insert(id, job_id);
                    spawned += 1;
                }
                Ok(None) => {
                    // Queue empty — release the slot and stop claiming.
                    drop(permit);
                    break;
                }
                Err(e) => {
                    tracing::error!(error = ?e, "claim failed");
                    drop(permit);
                    break;
                }
            }
        }
        spawned
    }

    /// Wait for a task to finish (freeing a slot) or the poll tick (to re-sweep
    /// + top up slots if the queue grew). When idle, just poll.
    async fn wait_for_progress(&mut self) {
        if self.tasks.is_empty() {
            tokio::time::sleep(POLL_INTERVAL).await;
        } else {
            tokio::select! {
                Some(joined) = self.tasks.join_next_with_id() => self.reap(joined),
                _ = tokio::time::sleep(POLL_INTERVAL) => {}
            }
        }
    }

    /// Record the outcome of a finished job task and drop its id→job mapping.
    fn reap(&mut self, joined: Result<(Id, anyhow::Result<()>), JoinError>) {
        match joined {
            Ok((id, result)) => {
                self.task_jobs.remove(&id);
                if let Err(e) = result {
                    // Setup-level failure — the reporter already terminal-failed
                    // the job; logged here for visibility.
                    tracing::error!(error = ?e, "job iteration failed");
                }
            }
            Err(join_err) => {
                // The job task itself panicked. Its reporter (a separate task)
                // observes the channel close and terminal-fails the job; a panic
                // *before* the reporter was gated leaves the job `claimed` (the
                // stuck-claim sweep recovers it) or `running` (Phase-4 recovery).
                let job_id = self
                    .task_jobs
                    .remove(&join_err.id());
                tracing::error!(
                    job_id = ?job_id,
                    error = ?join_err,
                    "job task panicked — recovery via reporter / stuck-claim sweep",
                );
            }
        }
    }
}

impl JobDeps {
    /// Run one claimed job to a terminal state: spawn the per-job [`Reporter`]
    /// (which owns prepare + all GitHub/DB side-effects), run the worker inline
    /// (the recipe), and surface the reporter's result. The `permit` is held
    /// for the job's lifetime, freeing its concurrency slot on completion.
    async fn run(
        self,
        job: RunnableJob,
        _permit: OwnedSemaphorePermit,
        token: CancellationToken,
    ) -> anyhow::Result<()> {
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
        // On `token` cancellation it cleans up + reports aborted.
        run_worker(&recipe, &job, prepared_rx, events_tx, token).await;

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
    token: CancellationToken,
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
    // The recipe honors `token` at its own cancellation-safe points (never
    // mid-provision) and runs its normal teardown on cancel — so we await it to
    // completion rather than dropping the future, which could leak the source
    // loop device. A cancelled token after it returns means "aborted".
    let outcome = recipe
        .execute(&ctx, &sink, &token)
        .await;
    let terminal = if token.is_cancelled() {
        tracing::warn!(job_id = %job.id, "run cancelled; reporting aborted");
        Terminal::Aborted
    } else {
        match outcome {
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
        PrReport, ReportingConfig, RunnerConfig, StacksBenchConfig, VmConfig,
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
            runner: RunnerConfig { max_concurrent_jobs: 1 },
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

    /// Several jobs, run as independent tasks **concurrently** (sharing the
    /// immutable config + App-id cache exactly as the coordinator would), each
    /// reach a terminal state without interfering — the foundation for
    /// `max_concurrent_jobs > 1`.
    #[tokio::test]
    async fn concurrent_jobs_reach_terminal_independently() {
        const N: usize = 5;
        let tmp = TempDir::new().unwrap();
        // Baseline jobs with reporting off → no GitHub, just the lifecycle.
        let config = Arc::new(config_with(&tmp, PrReport::Comment, BaselineReport::None));
        let app_id = Arc::new(OnceCell::new()); // shared across jobs, as in prod

        let mut handles = Vec::with_capacity(N);
        let mut sources = Vec::with_capacity(N);
        for _ in 0..N {
            let job = RunnableJob {
                progress: ProgressTarget::CommitCheck { check_run_id: None },
                ..pr_job("abc123", None)
            };
            let source = Arc::new(FakeSource::new(job.clone()));
            // Each job gets its own shell that fails provisioning → a clean
            // `Terminal::Failed` (a *ran-and-failed*, so `run` returns `Ok`).
            let shell = Arc::new(RecordingShell::new());
            shell.reply(PreparedReply::fail(b"boom: provisioning failed"));
            let deps = JobDeps {
                config: config.clone(),
                jobs: source.clone(),
                gh: Arc::new(FakeGitHub::new()),
                shell,
                app_id: app_id.clone(),
            };
            let permit = Arc::new(Semaphore::new(1))
                .try_acquire_owned()
                .unwrap();
            handles.push(tokio::spawn(deps.run(job, permit, CancellationToken::new())));
            sources.push(source);
        }

        for h in handles {
            // No task panicked, and each reporter drove its job to terminal.
            h.await
                .expect("job task did not panic")
                .expect("job reached terminal without a setup error");
        }
        for source in sources {
            assert_eq!(
                source.calls(),
                vec!["start_running", "fail"],
                "each concurrent job ran to a terminal `fail`",
            );
        }
    }

    /// A store whose `start_running` **blocks** until the test releases it,
    /// then errors — so a job task stays alive (holding its slot) without
    /// needing the worker/driver, and completes the moment it's released.
    /// Tracks the peak number blocked at once.
    struct BlockingSource {
        queue: StdMutex<std::collections::VecDeque<RunnableJob>>,
        gate: tokio::sync::Semaphore,
        blocked: std::sync::atomic::AtomicUsize,
        peak_blocked: std::sync::atomic::AtomicUsize,
    }

    impl BlockingSource {
        fn new(jobs: Vec<RunnableJob>) -> Self {
            Self {
                queue: StdMutex::new(jobs.into()),
                gate: tokio::sync::Semaphore::new(0),
                blocked: std::sync::atomic::AtomicUsize::new(0),
                peak_blocked: std::sync::atomic::AtomicUsize::new(0),
            }
        }
        fn release(&self, n: usize) {
            self.gate.add_permits(n);
        }
        fn peak_blocked(&self) -> usize {
            self.peak_blocked
                .load(std::sync::atomic::Ordering::SeqCst)
        }
        /// Wait until at least `n` jobs are blocked in `start_running`.
        async fn await_blocked(&self, n: usize) {
            for _ in 0..2000 {
                if self
                    .blocked
                    .load(std::sync::atomic::Ordering::SeqCst)
                    >= n
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            panic!("timed out waiting for {n} jobs blocked in start_running");
        }
    }

    #[async_trait]
    impl RunnableJobStore for BlockingSource {
        async fn claim_next(&self) -> anyhow::Result<Option<RunnableJob>> {
            Ok(self
                .queue
                .lock()
                .unwrap()
                .pop_front())
        }
        async fn start_running(
            &self,
            _job: &RunnableJob,
            _resolved: Option<ResolvedCommit>,
        ) -> anyhow::Result<()> {
            use std::sync::atomic::Ordering::SeqCst;
            let now = self
                .blocked
                .fetch_add(1, SeqCst)
                + 1;
            self.peak_blocked
                .fetch_max(now, SeqCst);
            // Block until released, then error so the task completes here (no
            // worker needed) and frees its slot.
            self.gate
                .acquire()
                .await
                .unwrap()
                .forget();
            self.blocked
                .fetch_sub(1, SeqCst);
            anyhow::bail!("released by test")
        }
        async fn sweep_stuck_claims(&self, _lease: chrono::Duration) -> anyhow::Result<u64> {
            Ok(0)
        }
        async fn complete(&self, _: &RunnableJob, _: &serde_json::Value) -> anyhow::Result<()> {
            Ok(())
        }
        async fn fail(
            &self,
            _: &RunnableJob,
            _: &str,
            _: Option<&serde_json::Value>,
        ) -> anyhow::Result<()> {
            Ok(())
        }
        async fn set_comment_id(&self, _: &RunnableJob, _: i64) -> anyhow::Result<()> {
            Ok(())
        }
        async fn set_check_run(
            &self,
            _: &RunnableJob,
            _: i64,
            _: Option<&str>,
        ) -> anyhow::Result<()> {
            Ok(())
        }
    }

    /// The coordinator enforces `max_concurrent_jobs`: it spawns exactly the
    /// limit, never over-claims while full, and tops the slot back up when a
    /// job finishes — observed directly on the [`Coordinator`] fill/reap
    /// seam.
    #[tokio::test]
    async fn coordinator_enforces_limit_and_tops_up() {
        let tmp = TempDir::new().unwrap();
        let config = Arc::new(config_with(&tmp, PrReport::Comment, BaselineReport::None));
        // 4 baseline jobs (commit pre-resolved; reporting off → no GitHub).
        let jobs: Vec<RunnableJob> = (0..4)
            .map(|_| RunnableJob {
                progress: ProgressTarget::CommitCheck { check_run_id: None },
                ..pr_job("abc123", None)
            })
            .collect();
        let source = Arc::new(BlockingSource::new(jobs));
        let deps = JobDeps {
            config,
            jobs: source.clone(),
            gh: Arc::new(FakeGitHub::new()),
            shell: Arc::new(RecordingShell::new()),
            app_id: Arc::new(OnceCell::new()),
        };
        let mut coord = Coordinator::new(deps, 2); // max_concurrent = 2

        // First fill claims exactly the limit (2), not all 4.
        assert_eq!(coord.fill_slots().await, 2, "first fill spawns exactly the limit");
        assert_eq!(coord.in_flight(), 2);
        source.await_blocked(2).await;
        assert_eq!(source.peak_blocked(), 2);

        // No over-claim while full: a second fill spawns nothing.
        assert_eq!(coord.fill_slots().await, 0, "no over-claim while slots are full");
        assert_eq!(coord.in_flight(), 2);

        // Release one job → it completes → reap frees a slot.
        source.release(1);
        while coord.in_flight() == 2 {
            coord
                .wait_for_progress()
                .await;
        }
        assert_eq!(coord.in_flight(), 1);

        // Re-fill tops the slot back up with the 3rd job — exactly one.
        assert_eq!(coord.fill_slots().await, 1, "tops up exactly one freed slot");
        assert_eq!(coord.in_flight(), 2);

        // Drain the rest; the limit must have held throughout.
        source.release(10);
        loop {
            coord.fill_slots().await;
            if coord.in_flight() == 0 {
                break;
            }
            coord
                .wait_for_progress()
                .await;
        }
        assert_eq!(source.peak_blocked(), 2, "never exceeded the configured limit");
    }

    /// When the job's token is cancelled, the worker reports
    /// `Terminal::Aborted` regardless of what the (now cancel-safe) recipe
    /// returns — the abort signal is the token, not the outcome. (The driver's
    /// own teardown-on-cancel is covered in `libvirt::driver`.)
    #[tokio::test]
    async fn a_cancelled_run_is_reported_aborted() {
        let tmp = TempDir::new().unwrap();
        let config = Arc::new(config_with(&tmp, PrReport::Comment, BaselineReport::None));
        // Provisioning fails fast → `execute` returns promptly; the pre-cancelled
        // token then makes the worker report aborted.
        let shell = Arc::new(RecordingShell::new());
        shell.reply(PreparedReply::fail(b"boom: provisioning failed"));
        let recipe = BenchRecipe::new(config, shell, vec![]);
        let job = RunnableJob {
            progress: ProgressTarget::CommitCheck { check_run_id: None },
            ..pr_job("abc123", None)
        };

        let (events_tx, mut events_rx) = mpsc::channel(EVENT_BUFFER);
        let (prepared_tx, prepared_rx) = oneshot::channel();
        prepared_tx
            .send(Prepared::Run { commit: "abc123".into() })
            .unwrap();
        let token = CancellationToken::new();
        token.cancel(); // cancelled before the run → outcome is overridden to aborted

        run_worker(&recipe, &job, prepared_rx, events_tx, token).await;

        match events_rx.recv().await {
            Some(WorkerEvent::Finished(Terminal::Aborted)) => {}
            other => panic!("expected Finished(Aborted), got {other:?}"),
        }
    }
}
