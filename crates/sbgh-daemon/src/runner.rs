//! Main daemon loop: a coordinator over a pool of concurrent job tasks.
//!
//! The coordinator claims while execution slots are free (a pool of slot
//! indices sized by `[runner].max_concurrent_jobs`, default 1) and spawns one
//! task per job into a `JoinSet`; the slot frees when the task is reaped. Each
//! slot maps to a stable cpuset for CPU pinning. Claims stay serial
//! inside the loop — only *execution* parallelizes. Each job task
//! ([`JobDeps::run`]) spawns the per-job [`Reporter`] (commit resolution +
//! Check Run / comment per `[reporting]` + the terminal DB write) and runs the
//! worker (the benchmark recipe) inline against it.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use sbgh_core::bench_args::resolve_bench_args;
use sbgh_core::config::DaemonConfig;
use sbgh_core::db::{JobStore, PolicyStore};
use sbgh_core::github::{CheckRunOutput, CheckRunState, CheckRunUpdate, GitHubApi};
use sbgh_core::models::{BuildTarget, TaskKind, uses_shared_calibration};
use tokio::sync::{OnceCell, mpsc, oneshot};
use tokio::task::{Id, JoinError, JoinSet};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use sbgh_driver::{
    BenchmarkRunContext, BenchmarkTask, CacheControl, ExecutionContext, ExecutionPlacement,
    ExecutionRequest, ExecutionTask,
};
use sbgh_libvirt::{LibvirtConfig, LvmConfig, PathsConfig, Shell, VmConfig};
use sbgh_worker::{BinaryCacheConfig, WorkerRuntime, build_binary_cache};

use crate::artifact_store::{
    ArtifactStore, GROUP_SQLITE_RELATIVE, execution_sink, group_artifact_key,
};
#[cfg(test)]
use crate::artifact_store::{ArtifactStoreConfig, build_store_or_local};
use crate::job_source::{ProgressTarget, RunnableJob, RunnableJobStore};
use crate::pin_manager::{PinManager, RepoIdentityLookup};
use crate::report::build_report_surface;
use crate::reporter::{CHECK_NAME, Prepared, Reporter, ReporterDependencies, resolved_app_id};
use crate::shutdown::Shutdown;
use crate::slack::card::{self, CardCtx};
use crate::slack::client::SlackClient;
use crate::slack::session::SlackSessionRegistry;
use crate::slack::stream::chunks_for_card;

/// How often the coordinator wakes to re-sweep + top up slots while jobs run.
const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Lease after which a job stranded in `claimed` (claimed but never
/// transitioned to `running` — daemon crashed or preflight errored
/// between `claim_next` and `start_running`) is reclaimed to `queued` by
/// the stuck-claim sweep. The claim→running window is normally
/// sub-second (a GH API call at most), so a few minutes is ample slack
/// without leaving a crashed claim stuck for long.
const CLAIM_LEASE_MINUTES: i64 = 5;

/// Grace before a Slack reporting session whose group has no active (queued /
/// running) run is reaped as abandoned. Comfortably exceeds a
/// repeat group's inter-run carry-forward + provisioning gap, so the sweep
/// never reaps a healthy group mid-handoff; with the daemon's reaping otherwise
/// comprehensive, this is a backstop for the rare stranded session.
const SESSION_ABANDON_GRACE: Duration = Duration::from_secs(10 * 60);

/// Bounded capacity of the per-job worker→reporter event channel. Phase
/// transitions are few and heartbeats are droppable, so a small buffer
/// absorbs bursts without ever stalling the worker for long.
const EVENT_BUFFER: usize = 32;

/// Terminal remark stamped on a job recovered from `running` at startup — a
/// crash/kill orphaned it (no result, possibly a leaked VM we just cleaned).
/// We **cancel** rather than re-run: a crash-orphan is re-triggerable, not a
/// benchmark failure, and a crash mid-run may recur. PR jobs are re-triggered
/// with `/benchmark`; baselines by the next push.
const ORPHAN_REMARK: &str = "recovered: orphaned in `running` by a daemon restart/crash";

/// Short, user-facing reason shown on a recovered orphan's **cancelled** Check
/// Run (4C-2) — the longer [`ORPHAN_REMARK`] is the DB-side remark.
const ORPHAN_CHECK_REASON: &str = "the daemon restarted while this run was in progress";

#[async_trait]
trait RepeatRunPlanner: Send + Sync + 'static {
    async fn append_next_benchmark_run(
        &self,
        completed_job_id: Uuid,
    ) -> anyhow::Result<Option<sbgh_core::models::Job>>;
    async fn pending_completed_benchmark_runs(
        &self,
    ) -> anyhow::Result<Vec<sbgh_core::db::jobs::PendingBenchmarkRun>>;
    async fn completed_event_detail(
        &self,
        job_id: Uuid,
    ) -> anyhow::Result<Option<serde_json::Value>>;
}

struct JobStoreRepeatRunPlanner {
    jobs: Arc<dyn JobStore>,
}

#[async_trait]
impl RepeatRunPlanner for JobStoreRepeatRunPlanner {
    async fn append_next_benchmark_run(
        &self,
        completed_job_id: Uuid,
    ) -> anyhow::Result<Option<sbgh_core::models::Job>> {
        self.jobs
            .append_next_benchmark_run(completed_job_id)
            .await
            .map_err(Into::into)
    }

    async fn pending_completed_benchmark_runs(
        &self,
    ) -> anyhow::Result<Vec<sbgh_core::db::jobs::PendingBenchmarkRun>> {
        self.jobs
            .pending_completed_benchmark_runs()
            .await
            .map_err(Into::into)
    }

    async fn completed_event_detail(
        &self,
        job_id: Uuid,
    ) -> anyhow::Result<Option<serde_json::Value>> {
        self.jobs
            .completed_event_detail(job_id)
            .await
            .map_err(Into::into)
    }
}

/// The runner's shared handles, cloned into each job task so it can run on its
/// own (spawned) without borrowing the coordinator.
#[derive(Clone)]
struct JobDeps {
    config: Arc<DaemonConfig>,
    jobs: Arc<dyn RunnableJobStore>,
    gh: Arc<dyn GitHubApi>,
    /// The artifact store built at process composition and shared by execution,
    /// repeat planning, orphan recovery, and reporting surfaces.
    artifact_store: Arc<dyn ArtifactStore>,
    /// In-process execution owner. Shared by per-job execution and startup
    /// orphan recovery.
    worker: Arc<WorkerRuntime>,
    /// Separately injected cache policy handle; never discovered through the
    /// driver.
    cache_control: Option<Arc<dyn CacheControl>>,
    /// Shared App-id cache, resolved via `GET /app` and cached on **success**
    /// only — a `get_or_try_init` that leaves the cell empty on error, so a
    /// transient blip is retried on the next job rather than disabling the
    /// reconcile for the whole process. Each reporter resolves it lazily (see
    /// [`crate::reporter::resolved_app_id`]).
    app_id: Arc<OnceCell<i64>>,
    /// The Slack surface for `ProgressTarget::Slack` jobs, shared into every
    /// reporter. `None` unless `[slack].enabled` wires a client at startup.
    slack: Option<Arc<dyn SlackClient>>,
    /// Binary-cache pin recompute. `None` unless the cache is enabled and
    /// [`Runner::with_pin_recompute`] wires it. Recomputed on startup and after
    /// each job execution, sharing the driver's cache `Arc`.
    pin_manager: Option<Arc<PinManager>>,
    /// Append/resume isolated repetitions from durable DB state. Kept separate
    /// from [`RunnableJobStore`] so the execution view
    /// stays focused on claim/run lifecycle.
    repeat_planner: Option<Arc<dyn RepeatRunPlanner>>,
    /// Group-scoped Slack reporting sessions, shared into every reporter so a
    /// repeat group's runs reuse one live card and keepalive.
    slack_sessions: Arc<SlackSessionRegistry>,
}

pub struct Runner {
    deps: JobDeps,
    /// Max jobs executed concurrently (`[runner].max_concurrent_jobs`, ≥ 1).
    max_concurrent: usize,
}

impl Runner {
    #[cfg(test)]
    pub fn new(
        config: DaemonConfig,
        jobs: Arc<dyn RunnableJobStore>,
        gh: Arc<dyn GitHubApi>,
        shell: Arc<dyn Shell>,
    ) -> Self {
        let artifact_store = build_test_artifact_store(&config);
        Self::new_with_artifact_store(config, jobs, gh, shell, artifact_store)
    }

    pub fn new_with_artifact_store(
        config: DaemonConfig,
        jobs: Arc<dyn RunnableJobStore>,
        gh: Arc<dyn GitHubApi>,
        shell: Arc<dyn Shell>,
        artifact_store: Arc<dyn ArtifactStore>,
    ) -> Self {
        let max_concurrent = config
            .runner
            .max_concurrent_jobs
            .max(1);
        let libvirt_config = LibvirtConfig {
            vm: VmConfig {
                golden_image: config.vm.golden_image.clone(),
                build_vcpus: config.vm.build_vcpus,
                bench_vcpus: config.vm.bench_vcpus,
                build_memory_bytes: config
                    .vm
                    .build_memory
                    .as_bytes(),
                bench_memory_bytes: config
                    .vm
                    .bench_memory
                    .as_bytes(),
                boot_disk_gib: config.vm.boot_disk_gib,
                job_timeout_secs: config.vm.job_timeout_secs,
                network: config.vm.network.clone(),
                poll_interval_secs: config.vm.poll_interval_secs,
                heartbeat_interval_secs: config
                    .vm
                    .heartbeat_interval_secs,
            },
            paths: PathsConfig {
                jobs_dir: config.paths.jobs_dir.clone(),
                git_mirror: config
                    .paths
                    .git_mirror
                    .clone(),
                results_tmpfs_root: config
                    .paths
                    .results_tmpfs_root
                    .clone(),
                results_archive_dir: config
                    .paths
                    .results_archive_dir
                    .clone(),
                sccache_dir: config
                    .paths
                    .sccache_dir
                    .clone(),
                virsh_binary: config
                    .paths
                    .virsh_binary
                    .clone(),
                sudo_binary: config
                    .paths
                    .sudo_binary
                    .clone(),
                qemu_img_binary: config
                    .paths
                    .qemu_img_binary
                    .clone(),
                cloud_localds_binary: config
                    .paths
                    .cloud_localds_binary
                    .clone(),
                git_binary: config
                    .paths
                    .git_binary
                    .clone(),
            },
            lvm: LvmConfig {
                vg_name: config.lvm.vg_name.clone(),
                thinpool: config.lvm.thinpool.clone(),
                chainstate_base_prefix: config
                    .lvm
                    .chainstate_base_prefix
                    .clone(),
                chainstate_snapshot_size_gib: config
                    .lvm
                    .chainstate_snapshot_size_gib,
            },
            service_user: config
                .server
                .service_user
                .clone(),
            host_cpus: config
                .runner
                .host_cpus
                .clone(),
        };
        let binary_cache = build_binary_cache(&BinaryCacheConfig {
            enabled: config
                .artifacts
                .binary_cache
                .enabled,
            max_bytes: config
                .artifacts
                .binary_cache
                .max_size
                .as_bytes(),
            dir: config
                .artifacts
                .binary_cache
                .dir
                .clone(),
        });
        let built_worker = WorkerRuntime::libvirt(
            libvirt_config,
            shell,
            execution_sink(artifact_store.clone()),
            binary_cache,
        );
        let config = Arc::new(config);
        Self {
            deps: JobDeps {
                config,
                jobs,
                gh,
                artifact_store,
                worker: built_worker.runtime,
                cache_control: built_worker.cache_control,
                app_id: Arc::new(OnceCell::new()),
                slack: None,
                pin_manager: None,
                repeat_planner: None,
                slack_sessions: Arc::new(SlackSessionRegistry::new()),
            },
            max_concurrent,
        }
    }

    /// Inject the Slack client used to report `ProgressTarget::Slack` jobs.
    /// Wired at startup only when `[slack].enabled`; without it, those jobs
    /// still run and the Slack surface is a no-op.
    pub fn with_slack(mut self, slack: Arc<dyn SlackClient>) -> Self {
        self.deps.slack = Some(slack);
        self
    }

    /// Enable binary-cache pin recompute. A no-op unless the driver runs a
    /// cache: the [`PinManager`] is built from the
    /// driver's **shared** cache `Arc` (so re-pin / evict and the driver's
    /// publish coordinate under one mutex) plus the policy / repo stores
    /// and `shell` for `ls-remote`. Wired by `main`; absent (or cache off),
    /// pins are never recomputed and the cache behaves exactly as before.
    pub fn with_pin_recompute(
        mut self,
        policy_store: Arc<dyn PolicyStore>,
        repo_store: Arc<dyn RepoIdentityLookup>,
        jobs: Arc<dyn JobStore>,
        shell: Arc<dyn Shell>,
    ) -> Self {
        if let Some(cache) = self
            .deps
            .cache_control
            .clone()
        {
            self.deps.pin_manager = Some(Arc::new(PinManager::new(
                cache,
                policy_store,
                repo_store,
                jobs,
                shell,
                self.deps
                    .config
                    .paths
                    .git_binary
                    .clone(),
                self.deps
                    .config
                    .vm
                    .golden_image
                    .clone(),
            )));
        }
        self
    }

    /// Enable repeat-run lazy chaining. The planner appends the next run
    /// only after the prior run has terminally completed and resumes any
    /// completed-but-not-appended groups at startup from persisted DB state.
    pub fn with_repeat_planning(mut self, jobs: Arc<dyn JobStore>) -> Self {
        self.deps.repeat_planner = Some(Arc::new(JobStoreRepeatRunPlanner { jobs }));
        self
    }

    /// The coordinator loop: sweep stranded claims, fill every free slot from
    /// the queue (until a drain/abort is requested), then wait for a task to
    /// free a slot or the poll tick. Returns when drained/aborted **and** idle,
    /// firing `shutdown.exit` so the rest of the process can stop. The
    /// per-iteration machinery lives on [`Coordinator`] so it's unit-testable
    /// without the loop.
    pub async fn run(self, shutdown: Shutdown) -> anyhow::Result<()> {
        tracing::info!(max_concurrent = self.max_concurrent, "daemon started");
        // Re-pin the resolved binary-cache set BEFORE claiming/recovering, so
        // the first publish's
        // eviction protects the right binaries. Best-effort + bounded; shares
        // the driver's cache `Arc`.
        if let Some(pm) = &self.deps.pin_manager {
            pm.recompute(chrono::Utc::now())
                .await;
        }
        let lease = chrono::Duration::minutes(CLAIM_LEASE_MINUTES);
        // The job tasks get child tokens of `abort`, so an abort cancels them
        // all at once.
        let mut coord = Coordinator::new(self.deps, self.max_concurrent, shutdown.abort.clone());
        // Any job still `running` at startup is an orphan from a crashed or
        // killed prior daemon: clean its leaked VM
        // and terminal-cancel the row before we start claiming fresh work. A
        // failure to even *enumerate* running rows is startup-critical (we can't
        // rule out live orphan VMs), so it propagates → the process exits and
        // systemd `Restart=on-failure` retries rather than claiming blind.
        coord
            .recover_orphans()
            .await?;
        coord
            .resume_pending_repeats()
            .await;
        loop {
            coord.sweep(lease).await;
            coord
                .sweep_abandoned_sessions()
                .await;
            // Once a drain/abort is requested, stop pulling new work; queued
            // jobs wait for the next boot.
            if !shutdown
                .draining
                .is_cancelled()
            {
                coord.fill_slots().await;
            }
            // Report each still-queued job its position (after fill_slots, so the
            // queue is what genuinely remains). Skipped during drain — we're
            // winding down, not advertising waits.
            if !shutdown
                .draining
                .is_cancelled()
            {
                coord
                    .update_queue_positions()
                    .await;
            }
            // Shutdown requested and nothing left in flight → we're done.
            if shutdown
                .draining
                .is_cancelled()
                && coord.in_flight() == 0
            {
                break;
            }
            coord
                .wait_for_progress(&shutdown.draining)
                .await;
        }
        tracing::info!("coordinator drained; signalling process exit");
        shutdown.exit.cancel();
        Ok(())
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
                // Standalone run — no concurrency slot, so no pinning.
                self.deps
                    .clone()
                    .run(job, None, CancellationToken::new())
                    .await?;
                Ok(true)
            }
            None => Ok(false),
        }
    }
}

#[cfg(test)]
fn build_test_artifact_store(config: &DaemonConfig) -> Arc<dyn ArtifactStore> {
    build_store_or_local(&ArtifactStoreConfig::local(
        config
            .paths
            .results_archive_dir
            .clone(),
    ))
}

/// The concurrent run loop's state: the slot pool, the in-flight task set, and
/// the task→job/slot maps. Split out from [`Runner::run`] so the fill/reap
/// machinery is unit-testable without the infinite loop.
struct Coordinator {
    deps: JobDeps,
    /// Free **slot indices** (`0..max_concurrent`). The pool size *is* the
    /// concurrency bound (an empty pool = full), and each in-flight job holds a
    /// stable slot index for its lifetime. CPU pinning maps it to a fixed cpuset
    /// (`[runner].cpu_sets[slot]`), so slot 0 always pins to the
    /// same cores. A slot returns to the pool on reap.
    slots_free: VecDeque<usize>,
    tasks: JoinSet<anyhow::Result<()>>,
    /// task id → job id, so a panicking task can be logged with context.
    task_jobs: HashMap<Id, Uuid>,
    /// task id → slot index, so the slot is returned to `slots_free` on reap.
    task_slots: HashMap<Id, usize>,
    /// The daemon-wide **abort** token. Each job gets a **child** token, so an
    /// abort cancels every in-flight run at once, while one job's own
    /// cancellation never propagates back up to siblings.
    abort: CancellationToken,
    /// Last queue position pushed per queued job id, so the updater only edits
    /// GitHub when a job's position changes. Entries are pruned when jobs leave
    /// the queue. This is in-memory only, so a restart re-pushes once.
    last_positions: HashMap<Uuid, usize>,
}

impl Coordinator {
    fn new(deps: JobDeps, max_concurrent: usize, abort: CancellationToken) -> Self {
        Self {
            deps,
            slots_free: (0..max_concurrent).collect(),
            tasks: JoinSet::new(),
            task_jobs: HashMap::new(),
            task_slots: HashMap::new(),
            abort,
            last_positions: HashMap::new(),
        }
    }

    /// Spawned-but-not-yet-reaped tasks (= occupied slots). An **upper bound**
    /// on live jobs: a task can finish before the next `join_next` reaps it +
    /// returns its slot. The real concurrency cap is the slot pool.
    fn in_flight(&self) -> usize {
        self.tasks.len()
    }

    /// Recover jobs orphaned in `running` by a crashed/killed prior daemon.
    /// Runs once at startup, before any fresh claim, so every
    /// `running` row is necessarily an orphan (this daemon has started none of
    /// its own yet). For each: clean the leaked VM via the handle-less
    /// [`LibvirtDriver::cleanup_by_job_id`], THEN terminal-**cancel** the row
    /// as cancelled because a crash-orphan is re-triggerable, not a benchmark
    /// failure. That order is crash-safe: a crash mid-recovery re-lists the
    /// still-`running` job next boot, so cleanup re-runs idempotently and
    /// no VM is leaked behind a terminal row).
    ///
    /// Recovery dispatches over the shared `Arc<dyn Driver>` (libvirt today).
    ///
    /// Errors only on a failure to **enumerate** running rows (startup-critical
    /// — see [`Runner::run`]). Per-orphan failures are non-fatal: an orphan
    /// whose VM couldn't be fully cleaned is left `running` (not cancelled) so
    /// the next boot retries — cancelling it would strand whatever cleanup
    /// preserved (e.g. an undetachable source loop) with no handle back to it.
    async fn recover_orphans(&self) -> anyhow::Result<()> {
        let ids = self
            .deps
            .jobs
            .running_job_ids()
            .await
            .context(
                "orphan recovery: enumerating `running` jobs failed; refusing to claim with \
                 possibly-live orphan VMs",
            )?;
        if ids.is_empty() {
            return Ok(());
        }
        tracing::warn!(
            count = ids.len(),
            "recovering jobs orphaned in `running` by a prior daemon"
        );
        for id in ids {
            // Clean BEFORE cancelling the row. If cleanup couldn't fully clear
            // the VM (a source loop may still be attached, its backing file
            // preserved), leave the row `running` so the next boot retries —
            // cancelling it now would lose the only handle back to the leak.
            if !self
                .deps
                .worker
                .cleanup_by_job_id(&id.to_string())
                .await
            {
                tracing::error!(
                    job_id = %id,
                    "orphan cleanup incomplete; leaving job `running` to retry recovery next boot",
                );
                continue;
            }
            match self
                .deps
                .jobs
                .cancel_orphan(id, ORPHAN_REMARK)
                .await
            {
                Ok(true) => {
                    tracing::info!(job_id = %id, "recovered orphaned `running` job (cancelled)");
                    // 4C-2: conclude the orphan's stuck `in_progress` check (and
                    // stale comment) as cancelled, via the normal reporting path.
                    self.conclude_orphan_report(id)
                        .await;
                }
                // Raced off `running` between list and cancel — nothing to do.
                Ok(false) => {}
                Err(e) => {
                    tracing::error!(error = ?e, job_id = %id, "orphan recovery: cancel_orphan failed")
                }
            }
        }
        Ok(())
    }

    /// If a daemon stopped after repeat run K completed but before run K+1 was
    /// appended, derive and enqueue the next run from durable DB
    /// state. Best-effort; a failed sweep is retried on the next coordinator
    /// start instead of blocking unrelated jobs.
    async fn resume_pending_repeats(&self) {
        let Some(planner) = &self.deps.repeat_planner else {
            return;
        };
        match planner
            .pending_completed_benchmark_runs()
            .await
        {
            Ok(pending) if pending.is_empty() => {}
            Ok(pending) => {
                let mut enqueued = 0usize;
                for item in pending {
                    let completed_job_id = item.completed_job_id;
                    let promoted = match self
                        .deps
                        .promote_completed_repeat_sqlite(
                            completed_job_id,
                            &item.artifact_prefix,
                            "repeat planner: startup carry-forward failed",
                        )
                        .await
                    {
                        Ok(promoted) => promoted,
                        Err(e) => {
                            tracing::warn!(
                                completed_job_id = %completed_job_id,
                                benchmark_group_id = %item.benchmark_group_id,
                                benchmark_spec_id = %item.benchmark_spec_id,
                                benchmark_run_index = item.benchmark_run_index,
                                requested_run_count = item.requested_run_count,
                                error = ?e,
                                "repeat planner: startup resume skipped pending run",
                            );
                            continue;
                        }
                    };
                    if !promoted {
                        tracing::warn!(
                            completed_job_id = %completed_job_id,
                            benchmark_group_id = %item.benchmark_group_id,
                            benchmark_spec_id = %item.benchmark_spec_id,
                            benchmark_run_index = item.benchmark_run_index,
                            requested_run_count = item.requested_run_count,
                            "repeat planner: startup resume found completed run without detail",
                        );
                        continue;
                    }
                    match planner
                        .append_next_benchmark_run(completed_job_id)
                        .await
                    {
                        Ok(Some(job)) => {
                            enqueued += 1;
                            tracing::info!(
                                completed_job_id = %completed_job_id,
                                next_job_id = %job.id,
                                benchmark_run_index = job.benchmark_run_index,
                                "repeat planner: resumed pending benchmark run",
                            );
                        }
                        Ok(None) => {}
                        Err(e) => {
                            tracing::warn!(
                                completed_job_id = %completed_job_id,
                                error = ?e,
                                "repeat planner: startup append failed; will retry on next daemon start",
                            );
                        }
                    }
                }
                if enqueued > 0 {
                    tracing::info!(enqueued, "repeat planner: resumed pending benchmark runs");
                }
            }
            Err(e) => {
                tracing::warn!(
                    error = ?e,
                    "repeat planner: startup resume failed; will retry on next daemon start",
                );
            }
        }
    }

    /// Conclude a recovered orphan's stuck Check Run (and update its stale
    /// comment) as **cancelled** (4C-2), reusing the normal reporting path so
    /// the gray-check + correct re-trigger hint match a live abort.
    /// Best-effort: the row is already terminal, so a GitHub blip just
    /// leaves the check spinning (no worse than pre-4C-2) and isn't
    /// retried.
    async fn conclude_orphan_report(&self, job_id: Uuid) {
        let job = match self
            .deps
            .jobs
            .load_runnable(job_id)
            .await
        {
            Ok(Some(job)) => job,
            // Row vanished, or nothing to load — nothing to conclude.
            Ok(None) => return,
            Err(e) => {
                tracing::warn!(
                    error = ?e,
                    job_id = %job_id,
                    "orphan recovery: couldn't load reporting context; check may stay in-progress",
                );
                return;
            }
        };
        // Build the orphan's reporting surface (the same factory the reporter
        // uses) and conclude it cancelled — for a Slack orphan this resumes the
        // persisted card + swaps the stuck ⏳; for GitHub it concludes the check.
        let surface = build_report_surface(
            self.deps.gh.clone(),
            self.deps.jobs.clone(),
            self.deps
                .artifact_store
                .clone(),
            self.deps.slack.as_ref(),
            &self.deps.slack_sessions,
            &job,
        );
        surface
            .cancelled(ORPHAN_CHECK_REASON)
            .await;
    }

    /// Report each waiting job's queue position on its surface: a GitHub Check
    /// Run ("queued — N ahead"), or, for a
    /// Slack job whose card was posted, the live card's Job row ("position
    /// N/M"). Called each loop after `fill_slots`, so `in_flight()`
    /// reflects the just-claimed jobs and the queue is what genuinely
    /// remains. A queued job at index `i` has `ahead` runs before it
    /// (`in_flight()` plus `i`). The GitHub check is created/refreshed
    /// `in_progress` and a later claim adopts its persisted id; the Slack
    /// card's Job row is streamed with `chat.update` kept as fallback.
    /// Best-effort: a surface or DB hiccup is logged, never fatal.
    ///
    /// The `last_positions` map suppresses redundant **surface edits** (only
    /// push when a job's position changed) and is pruned to the current
    /// queue.
    async fn update_queue_positions(&mut self) {
        let queued = match self
            .deps
            .jobs
            .list_queued()
            .await
        {
            Ok(q) => q,
            Err(e) => {
                tracing::warn!(error = ?e, "queue-position: list_queued failed");
                return;
            }
        };
        let in_flight = self.in_flight();
        let total = in_flight + queued.len();
        let mut seen = HashSet::new();
        for (i, job) in queued.iter().enumerate() {
            let ahead = in_flight + i;
            // Position-reportable: a **Slack** job whose pre-claim stream/card
            // was already posted (it carries a `plan_message_ts`), or a **GitHub**
            // job whose `[reporting]` wants a pre-claim position check and that
            // already carries a head SHA (PR / branch-push; a tag job resolves
            // only at claim).
            let reportable = match &job.progress {
                ProgressTarget::Slack { plan_message_ts, .. } => {
                    job.benchmark_run_index == 0 && plan_message_ts.is_some()
                }
                _ => {
                    !job.commit.is_empty() && wants_position_check(&self.deps.config.reporting, job)
                }
            };
            if !reportable {
                continue;
            }
            seen.insert(job.id);
            if self
                .last_positions
                .get(&job.id)
                == Some(&ahead)
            {
                continue; // unchanged → no edit
            }
            let updated = match &job.progress {
                ProgressTarget::Slack {
                    channel,
                    plan_message_ts: Some(plan_ts),
                    ..
                } => {
                    self.update_slack_queue_position(job, channel, plan_ts, ahead, total)
                        .await
                }
                _ => {
                    self.ensure_position_check(job, ahead)
                        .await
                }
            };
            if updated {
                self.last_positions
                    .insert(job.id, ahead);
            }
        }
        self.last_positions
            .retain(|id, _| seen.contains(id));
    }

    /// Update the queued Slack card's **Job row** with the live queue position
    /// ("position N/M"). The pre-claim stream/card was posted by the connector
    /// (its `plan_ts`); this appends a `task_update` while the job waits, with
    /// `chat.update` kept as fallback. Pre-claim, the rev hasn't resolved, so
    /// the card carries the rev (not a SHA).
    /// Best-effort — returns whether the card is now up to date.
    async fn update_slack_queue_position(
        &self,
        job: &RunnableJob,
        channel: &str,
        plan_ts: &str,
        ahead: usize,
        total: usize,
    ) -> bool {
        let Some(slack) = &self.deps.slack else {
            return false; // no client wired → nothing to update
        };
        let job_id = job.id.to_string();
        let ctx = CardCtx {
            rev: &job.git_ref_display,
            commit: None,
            commit_url: None,
            job_id: &job_id,
            bench_args: &job.bench_args,
            repeat: None,
            group_run: None,
            cached_build: None,
            cached_build_staging: false,
        };
        let detail = format!("position {}/{}", ahead + 1, total);
        let card = card::queued_card(&ctx, Some(&detail));
        let mut chunks = chunks_for_card(&card);
        for chunk in &mut chunks {
            if let crate::slack::stream::StreamChunk::TaskUpdate(update) = chunk
                && update.id == "job"
            {
                update.title = format!("Queued · {detail}");
            }
        }
        let fallback = format!("Benchmarking {} — queued ({detail})", job.git_ref_display);
        match slack
            .append_stream(channel, plan_ts, &chunks)
            .await
        {
            Ok(()) => return true,
            Err(e) => {
                tracing::warn!(job_id = %job.id, error = ?e, "queue-position: slack stream update failed; falling back to block update");
            }
        }
        let blocks = card::render(&card);
        match slack
            .update_blocks(channel, plan_ts, &blocks, &fallback)
            .await
        {
            Ok(()) => true,
            Err(e) => {
                tracing::warn!(job_id = %job.id, error = ?e, "queue-position: slack card update failed (non-fatal)");
                false
            }
        }
    }

    /// Create-or-update the queued job's position Check Run (`in_progress`
    /// state, "queued — N ahead" body). Mirrors the reporter's
    /// reconcile-or-create-and- persist (restart-safe via
    /// `find_check_run_by_external_id` + the persisted `check_run_created`
    /// event) so a claim later *adopts* this check rather than duplicating
    /// it. Returns whether the surface is now up to date.
    async fn ensure_position_check(&self, job: &RunnableJob, ahead: usize) -> bool {
        let gh = self.deps.gh.as_ref();
        let existing = match job.progress {
            ProgressTarget::PullRequest { check_run_id, .. } => check_run_id,
            ProgressTarget::CommitCheck { check_run_id } => check_run_id,
            // Slack + silent jobs carry no GitHub check (this runs only for jobs
            // whose `[reporting]` wants a position check — see
            // `wants_position_check`).
            ProgressTarget::Slack { .. } | ProgressTarget::Silent => None,
        };

        // Already have a check (a prior tick / re-claim) → just refresh the text.
        if let Some(id) = existing {
            return match gh
                .update_check_run(
                    job.installation_id,
                    &job.repository,
                    id,
                    CheckRunUpdate {
                        state: CheckRunState::InProgress,
                        output: queue_position_output(job.id, ahead),
                    },
                )
                .await
            {
                Ok(_) => true,
                Err(e) => {
                    tracing::warn!(job_id = %job.id, error = ?e, "queue-position: update check failed (non-fatal)");
                    false
                }
            };
        }

        // No check yet: reconcile (dedup a check stranded by a crash before its
        // id was persisted), else create — persisting the id either way so the
        // claim-time reporter adopts it.
        let external_id = job.id.to_string();
        if let Some(app_id) = resolved_app_id(&self.deps.app_id, gh).await
            && let Ok(Some(found)) = gh
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
            // Persist the id FIRST. If it doesn't land, report failure so the
            // position isn't recorded in `last_positions` — the next tick
            // re-reconciles and retries, rather than debouncing away the only
            // chance to record the `check_run_created` event the claim-time
            // reporter reads back to adopt this check.
            if let Err(e) = self
                .deps
                .jobs
                .set_check_run(job, found.id, found.html_url.as_deref())
                .await
            {
                tracing::warn!(job_id = %job.id, check_run_id = found.id, error = ?e, "queue-position: persisting reconciled check id failed; will retry next tick");
                return false;
            }
            // Refresh the text; mirror the existing-check path — a failed update
            // returns `false` so the stale position isn't debounced and the next
            // tick (now via the existing-check path, the id having persisted)
            // retries.
            return match gh
                .update_check_run(
                    job.installation_id,
                    &job.repository,
                    found.id,
                    CheckRunUpdate {
                        state: CheckRunState::InProgress,
                        output: queue_position_output(job.id, ahead),
                    },
                )
                .await
            {
                Ok(_) => true,
                Err(e) => {
                    tracing::warn!(job_id = %job.id, check_run_id = found.id, error = ?e, "queue-position: refreshing reconciled check text failed (non-fatal); will retry");
                    false
                }
            };
        }

        match gh
            .create_check_run(
                job.installation_id,
                &job.repository,
                &job.commit,
                CHECK_NAME,
                &external_id,
                CheckRunUpdate {
                    state: CheckRunState::InProgress,
                    output: queue_position_output(job.id, ahead),
                },
            )
            .await
        {
            Ok(posted) => {
                // Same as the reconcile path: a failed persist returns `false` so
                // the next tick re-finds this check (via reconcile) and retries
                // recording its id, instead of being debounced.
                if let Err(e) = self
                    .deps
                    .jobs
                    .set_check_run(job, posted.id, posted.html_url.as_deref())
                    .await
                {
                    tracing::warn!(job_id = %job.id, check_run_id = posted.id, error = ?e, "queue-position: persisting created check id failed; will retry next tick (reconcile adopts the existing check)");
                    return false;
                }
                true
            }
            Err(e) => {
                tracing::warn!(job_id = %job.id, error = ?e, "queue-position: create check failed (non-fatal)");
                false
            }
        }
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

    /// Reap Slack reporting sessions abandoned mid-group: idle past
    /// [`SESSION_ABANDON_GRACE`] **and** whose group has no active (queued /
    /// running) run. DB-progress-aware so a healthy group in its inter-run
    /// carry-forward gap is never reaped early. Best-effort — a DB read error
    /// skips this sweep (we never reap on uncertainty).
    async fn sweep_abandoned_sessions(&self) {
        if self
            .deps
            .slack_sessions
            .is_empty()
        {
            return; // no sessions → no DB work
        }
        let mut active: HashSet<Uuid> = HashSet::new();
        match self
            .deps
            .jobs
            .list_queued()
            .await
        {
            Ok(jobs) => active.extend(
                jobs.iter()
                    .map(|j| j.benchmark_group_id),
            ),
            Err(e) => {
                tracing::warn!(error = ?e, "slack: session sweep skipped (queued read failed)");
                return;
            }
        }
        let running = match self
            .deps
            .jobs
            .running_job_ids()
            .await
        {
            Ok(ids) => ids,
            Err(e) => {
                tracing::warn!(error = ?e, "slack: session sweep skipped (running read failed)");
                return;
            }
        };
        for id in running {
            match self
                .deps
                .jobs
                .load_runnable(id)
                .await
            {
                Ok(Some(job)) => {
                    active.insert(job.benchmark_group_id);
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(error = ?e, "slack: session sweep skipped (running job read failed)");
                    return;
                }
            }
        }
        let reaped = self
            .deps
            .slack_sessions
            .sweep_abandoned(SESSION_ABANDON_GRACE, &active);
        if reaped > 0 {
            tracing::info!(reaped, "slack: reaped abandoned reporting sessions");
        }
    }

    /// Claim into every free slot, spawning a job task per claim. Each job
    /// holds its slot index until reap (which returns it), so the slot and its
    /// cpuset are stable for the run. Returns how many
    /// were spawned.
    async fn fill_slots(&mut self) -> usize {
        let mut spawned = 0;
        while let Some(slot) = self.slots_free.pop_front() {
            match self
                .deps
                .jobs
                .claim_next()
                .await
            {
                Ok(Some(job)) => {
                    let job_id = job.id;
                    let token = self.abort.child_token();
                    let cpuset = self.cpuset_for_slot(slot);
                    let id = self
                        .tasks
                        .spawn(
                            self.deps
                                .clone()
                                .run(job, cpuset, token),
                        )
                        .id();
                    self.task_jobs
                        .insert(id, job_id);
                    self.task_slots
                        .insert(id, slot);
                    spawned += 1;
                }
                Ok(None) => {
                    // Queue empty — return the slot and stop claiming.
                    self.slots_free
                        .push_front(slot);
                    break;
                }
                Err(e) => {
                    tracing::error!(error = ?e, "claim failed");
                    self.slots_free
                        .push_front(slot);
                    break;
                }
            }
        }
        spawned
    }

    /// The libvirt cpuset configured for a concurrency slot, or `None` when
    /// `[runner].cpu_sets` is unset (vCPUs float).
    fn cpuset_for_slot(&self, slot: usize) -> Option<String> {
        self.deps
            .config
            .runner
            .cpu_sets
            .get(slot)
            .cloned()
    }

    /// Wait for a task to finish (freeing a slot), the poll tick (to re-sweep +
    /// top up slots if the queue grew), or a drain request (to re-evaluate the
    /// exit condition promptly). The `draining` wake arm is disabled once it's
    /// already set, so a drain-with-jobs-in-flight reaps via `join_next` rather
    /// than busy-looping.
    async fn wait_for_progress(&mut self, draining: &CancellationToken) {
        if self.tasks.is_empty() {
            tokio::select! {
                _ = tokio::time::sleep(POLL_INTERVAL) => {}
                _ = draining.cancelled(), if !draining.is_cancelled() => {}
            }
        } else {
            tokio::select! {
                Some(joined) = self.tasks.join_next_with_id() => self.reap(joined),
                _ = tokio::time::sleep(POLL_INTERVAL) => {}
                _ = draining.cancelled(), if !draining.is_cancelled() => {}
            }
        }
    }

    /// Record the outcome of a finished job task, drop its id→job mapping, and
    /// **return its slot** to the free pool so the next claim can reuse it.
    fn reap(&mut self, joined: Result<(Id, anyhow::Result<()>), JoinError>) {
        match joined {
            Ok((id, result)) => {
                self.task_jobs.remove(&id);
                self.free_slot(&id);
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
                // stuck-claim sweep recovers it) or `running` (orphan recovery).
                let id = join_err.id();
                let job_id = self.task_jobs.remove(&id);
                self.free_slot(&id);
                tracing::error!(
                    job_id = ?job_id,
                    error = ?join_err,
                    "job task panicked — recovery via reporter / stuck-claim sweep",
                );
            }
        }
    }

    /// Return a finished task's slot index to the free pool.
    fn free_slot(&mut self, id: &Id) {
        if let Some(slot) = self.task_slots.remove(id) {
            self.slots_free
                .push_back(slot);
        }
    }
}

impl JobDeps {
    /// Run one claimed job to a terminal state: spawn the per-job [`Reporter`]
    /// (which owns prepare + all GitHub/DB side-effects), run the worker inline
    /// (the recipe), and surface the reporter's result. `vcpu_cpuset` is the
    /// job's concurrency-slot CPU pinning, or `None` to float.
    async fn run(
        self,
        job: RunnableJob,
        vcpu_cpuset: Option<String>,
        token: CancellationToken,
    ) -> anyhow::Result<()> {
        tracing::info!(
            job_id = %job.id,
            repo = %job.repository,
            git_ref_kind = ?job.git_ref_kind,
            git_ref = %job.git_ref_display,
            commit_preresolved = !job.commit.is_empty(),
            cpuset = ?vcpu_cpuset,
            progress = match job.progress {
                ProgressTarget::PullRequest { .. } => "pull_request",
                ProgressTarget::CommitCheck { .. } => "commit_check",
                ProgressTarget::Slack { .. } => "slack",
                ProgressTarget::Silent => "silent",
            },
            task_kind = ?job.task_kind,
            build_target = ?job.build_target,
            benchmark_group_id = %job.benchmark_group_id,
            benchmark_spec_id = %job.benchmark_spec_id,
            benchmark_run_index = job.benchmark_run_index,
            requested_run_count = job.requested_run_count,
            "claimed job; starting",
        );

        let (events_tx, events_rx) = mpsc::channel(EVENT_BUFFER);
        let (prepared_tx, prepared_rx) = oneshot::channel();

        // The reporter task owns prepare + reporting + the terminal write. It
        // gets the shared App-id cache (resolved lazily, only if a check is
        // wanted) for the Check Run reconcile.
        let reporter = Reporter::new_with_dependencies(
            self.config.clone(),
            ReporterDependencies {
                jobs: self.jobs.clone(),
                gh: self.gh.clone(),
                artifact_store: self.artifact_store.clone(),
                app_id: self.app_id.clone(),
                slack: self.slack.clone(),
                slack_sessions: self.slack_sessions.clone(),
            },
            job.clone(),
        );
        let handle = tokio::spawn(reporter.run(events_rx, prepared_tx));

        // Preparation remains orchestrator-owned. Only after it resolves the
        // commit do we assemble the owned execution request and cross the
        // in-process worker boundary.
        let worker_completed = match prepared_rx.await {
            Ok(Prepared::Run { commit }) => {
                let request = execution_request_for(
                    &job,
                    commit,
                    vcpu_cpuset,
                    &self
                        .config
                        .stacks_bench
                        .default_args,
                );
                self.worker
                    .run(request, events_tx, token)
                    .await
            }
            Ok(Prepared::Abort) | Err(_) => false,
        };

        // After the job's execution (which may have published a freshly-built
        // binary), recompute the pinned set so a newly-built pinned ref is
        // protected from the next eviction. Best-effort
        // + bounded; the shared cache `Arc` makes this safe alongside concurrent
        // jobs' publishes (one mutex). Idempotent — a cache-hit/failed job that
        // published nothing simply re-affirms the set.
        if let Some(pm) = &self.pin_manager {
            pm.recompute(chrono::Utc::now())
                .await;
        }

        // Surface the reporter's result (setup-level failures back off the loop;
        // a panic in the reporter task becomes an iteration error).
        let reporter_result = match handle.await {
            Ok(result) => result,
            Err(join_err) => Err(anyhow::anyhow!("reporter task panicked: {join_err}")),
        };

        if worker_completed && reporter_result.is_ok() {
            self.append_next_repeat_after_terminal(&job)
                .await;
        }

        reporter_result
    }

    /// After the reporter has persisted a repeat's terminal state, ask the
    /// planner to append the next run. This is deliberately non-fatal
    /// to the just-finished run: a DB hiccup is retryable via startup resume.
    async fn append_next_repeat_after_terminal(&self, job: &RunnableJob) {
        if !job_should_carry_sqlite(job) {
            return;
        }
        let Some(planner) = &self.repeat_planner else {
            return;
        };
        let completed_job_id = job.id;
        let promoted = match self
            .promote_completed_repeat_sqlite(
                completed_job_id,
                &job.group_artifact_prefix,
                "repeat planner: carry-forward after terminal completion failed",
            )
            .await
        {
            Ok(promoted) => promoted,
            Err(e) => {
                tracing::warn!(
                    completed_job_id = %completed_job_id,
                    benchmark_run_index = job.benchmark_run_index,
                    requested_run_count = job.requested_run_count,
                    error = ?e,
                    "repeat planner: will not enqueue next run until carried SQLite DB is available",
                );
                if !job_is_final_group_run(job) {
                    self.fail_repeat_group_surface(
                        job,
                        "repeat group stalled: carrying the shared SQLite DB to the next run \
                         failed",
                    )
                    .await;
                }
                return;
            }
        };
        if !promoted {
            if !job_is_final_group_run(job) {
                self.fail_repeat_group_surface(
                    job,
                    "repeat group stalled: the completed run did not produce a SQLite DB to carry \
                     forward",
                )
                .await;
            }
            return;
        }
        match planner
            .append_next_benchmark_run(completed_job_id)
            .await
        {
            Ok(Some(job)) => {
                tracing::info!(
                    completed_job_id = %completed_job_id,
                    next_job_id = %job.id,
                    benchmark_run_index = job.benchmark_run_index,
                    "repeat planner: enqueued next benchmark run",
                );
            }
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(
                    completed_job_id = %completed_job_id,
                    error = ?e,
                    "repeat planner: append failed after terminal completion; startup resume can retry",
                );
            }
        }
    }

    async fn promote_completed_repeat_sqlite(
        &self,
        completed_job_id: Uuid,
        artifact_prefix: &str,
        context: &str,
    ) -> anyhow::Result<bool> {
        let planner = self
            .repeat_planner
            .as_ref()
            .context("repeat planner not configured")?;
        let Some(detail) = planner
            .completed_event_detail(completed_job_id)
            .await?
        else {
            return Ok(false);
        };
        let sqlite_key = detail
            .get("sqlite_archived_path")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .with_context(|| format!("{context}: sqlite_archived_path is missing"))?;
        let src = self
            .artifact_store
            .get(sqlite_key)
            .await
            .with_context(|| format!("{context}: resolve archived SQLite {sqlite_key}"))?;
        let len = std::fs::metadata(&src)
            .with_context(|| format!("{context}: stat archived SQLite {}", src.display()))?
            .len();
        anyhow::ensure!(len > 0, "{context}: archived SQLite {} is empty", src.display());

        let group_key = group_sqlite_key(artifact_prefix);
        let Some(bytes) = self
            .artifact_store
            .put(&group_key, &src)
            .await
        else {
            anyhow::bail!("{context}: failed to store carried SQLite at {group_key}");
        };
        tracing::info!(
            completed_job_id = %completed_job_id,
            sqlite_archived_path = sqlite_key,
            group_sqlite_key = group_key,
            bytes,
            "repeat planner: carried benchmark SQLite DB",
        );
        Ok(true)
    }

    async fn fail_repeat_group_surface(&self, job: &RunnableJob, reason: &str) {
        let surface = build_report_surface(
            self.gh.clone(),
            self.jobs.clone(),
            self.artifact_store.clone(),
            self.slack.as_ref(),
            &self.slack_sessions,
            job,
        );
        surface.failed(reason).await;
    }
}

fn group_sqlite_key(artifact_prefix: &str) -> String {
    group_artifact_key(artifact_prefix, GROUP_SQLITE_RELATIVE)
}

fn job_should_carry_sqlite(job: &RunnableJob) -> bool {
    uses_shared_calibration(job.task_kind, job.build_target, job.group_requested_run_count)
}

fn job_is_final_group_run(job: &RunnableJob) -> bool {
    job.group_run_index + 1 >= job.group_requested_run_count
}

fn sqlite_seed_key_for(job: &RunnableJob) -> Option<String> {
    if job_should_carry_sqlite(job) && job.group_run_index > 0 {
        Some(group_sqlite_key(&job.group_artifact_prefix))
    } else {
        None
    }
}

fn execution_request_for(
    job: &RunnableJob,
    commit: String,
    vcpu_cpuset: Option<String>,
    default_bench_args: &str,
) -> ExecutionRequest {
    let task = match (job.task_kind, job.build_target) {
        (TaskKind::Benchmark, BuildTarget::StacksBench) => {
            let resolved = resolve_bench_args(&job.bench_args, default_bench_args);
            ExecutionTask::Benchmark(BenchmarkTask {
                args: resolved.effective_args,
                sqlite_seed_key: sqlite_seed_key_for(job),
                shared_baseline_calibration: job_should_carry_sqlite(job),
                baseline_calibration_id: job.baseline_calibration_id,
                run: BenchmarkRunContext {
                    run_index: job.benchmark_run_index,
                    requested_run_count: job.requested_run_count,
                },
            })
        }
        (TaskKind::BuildOnly, BuildTarget::StacksBench) => ExecutionTask::BuildOnly,
        (task_kind, build_target) => ExecutionTask::Unsupported {
            combination: format!("{task_kind:?}/{build_target:?}"),
        },
    };
    ExecutionRequest {
        context: ExecutionContext {
            job_id: job.id,
            repository: job.repository.clone(),
            commit,
        },
        task,
        placement: ExecutionPlacement { vcpu_cpuset },
    }
}

/// Whether a queued job's `[reporting]` config wants a Check Run surface, so a
/// pre-claim position check makes sense. PR jobs key off `pr_report`;
/// baselines off `baseline_report`; comment-only / no-report jobs get none.
fn wants_position_check(reporting: &sbgh_core::config::ReportingConfig, job: &RunnableJob) -> bool {
    match job.progress {
        ProgressTarget::PullRequest { .. } => reporting
            .pr_report
            .wants_check(),
        ProgressTarget::CommitCheck { .. } => reporting
            .baseline_report
            .wants_check(),
        // Slack jobs report into their thread, not a GitHub queue-position
        // check; build-only/silent jobs report nothing at all.
        ProgressTarget::Slack { .. } | ProgressTarget::Silent => false,
    }
}

/// The "queued — N ahead" Check Run output. `ahead` is the number of runs that
/// will be claimed / finish before this one starts.
fn queue_position_output(job_id: Uuid, ahead: usize) -> CheckRunOutput {
    let summary = match ahead {
        0 => "queued — next to run".to_string(),
        1 => "queued — 1 run ahead".to_string(),
        n => format!("queued — {n} runs ahead"),
    };
    CheckRunOutput {
        title: format!("benchmark {job_id} — queued"),
        summary,
        text: Some(
            "Waiting for an execution slot; this updates as the queue advances.".to_string(),
        ),
    }
}

#[cfg(test)]
mod tests;
