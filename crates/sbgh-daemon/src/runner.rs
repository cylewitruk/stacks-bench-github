//! Main daemon loop — a **coordinator** over a pool of concurrent job tasks
//! (roadmap-v5 Phase 3).
//!
//! The coordinator claims while execution slots are free (a pool of slot
//! indices sized by `[runner].max_concurrent_jobs`, default 1) and spawns one
//! task per job into a `JoinSet`; the slot frees when the task is reaped. Each
//! slot maps to a stable cpuset for Phase-5 CPU pinning. Claims stay serial
//! inside the loop — only *execution* parallelizes. Each job task
//! ([`JobDeps::run`]) spawns the per-job [`Reporter`] (commit resolution +
//! Check Run / comment per `[reporting]` + the terminal DB write) and runs the
//! worker (the benchmark recipe) inline against it.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use sbgh_core::config::DaemonConfig;
use sbgh_core::db::{JobStore, PolicyStore, RepoStore};
use sbgh_core::github::{CheckRunOutput, CheckRunState, CheckRunUpdate, GitHubApi};
use sbgh_core::models::{BuildTarget, TaskKind};
use tokio::sync::{OnceCell, mpsc, oneshot};
use tokio::task::{Id, JoinError, JoinSet};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::artifact_store::{GROUP_SQLITE_RELATIVE, build_store_or_local, group_artifact_key};
use crate::bench_recipe::BenchRecipe;
use crate::build_recipe::BuildOnlyRecipe;
use crate::driver::Driver;
use crate::events::{ChannelSink, Terminal, WorkerEvent};
use crate::job_source::{ProgressTarget, RunnableJob, RunnableJobStore};
use crate::libvirt::{LibvirtDriver, Shell};
use crate::pin_manager::PinManager;
use crate::recipe::{Recipe, TaskContext, TaskOutcome, TaskStatus, UnsupportedRecipe};
use crate::report::build_report_surface;
use crate::reporter::{CHECK_NAME, Prepared, Reporter, resolved_app_id};
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

/// Bounded capacity of the per-job worker→reporter event channel. Phase
/// transitions are few and heartbeats are droppable, so a small buffer
/// absorbs bursts without ever stalling the worker for long.
const EVENT_BUFFER: usize = 32;

/// Terminal remark stamped on a job recovered from `running` at startup — a
/// crash/kill orphaned it (no result, possibly a leaked VM we just cleaned).
/// We **cancel** rather than re-run (Phase 4C): a crash-orphan is
/// re-triggerable, not a benchmark failure, and a crash mid-run may recur. PR
/// jobs are re-triggered with `/benchmark`; baselines by the next push.
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
    /// The execution backend (roadmap-v8 Phase 1). Shared by the per-job recipe
    /// and startup orphan recovery; built once from `[backend]` config (libvirt
    /// today) in [`Runner::new`].
    driver: Arc<dyn Driver>,
    /// Shared App-id cache, resolved via `GET /app` and cached on **success**
    /// only — a `get_or_try_init` that leaves the cell empty on error, so a
    /// transient blip is retried on the next job rather than disabling the
    /// reconcile for the whole process. Each reporter resolves it lazily (see
    /// [`crate::reporter::resolved_app_id`]).
    app_id: Arc<OnceCell<i64>>,
    /// The Slack surface for `ProgressTarget::Slack` jobs (item 0002), shared
    /// into every reporter. `None` unless `[slack].enabled` wired a client at
    /// startup (the Socket Mode adapter slice injects it via
    /// [`Runner::with_slack`]).
    slack: Option<Arc<dyn SlackClient>>,
    /// Binary-cache pin recompute (item 0025, v9 Phase 2). `None` unless the
    /// cache is enabled and [`Runner::with_pin_recompute`] wired it. Recomputed
    /// on startup + after each job execution, sharing the driver's cache `Arc`.
    pin_manager: Option<Arc<PinManager>>,
    /// v15 isolated repetitions: append/resume the lazy run chain from durable
    /// DB state. Kept separate from [`RunnableJobStore`] so the execution view
    /// stays focused on claim/run lifecycle.
    repeat_planner: Option<Arc<dyn RepeatRunPlanner>>,
    /// v18 (0047): group-scoped Slack reporting sessions, shared into every
    /// reporter so a repeat group's runs reuse one live card + keepalive.
    slack_sessions: Arc<SlackSessionRegistry>,
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
        let config = Arc::new(config);
        // v8 Phase 1: the backend is the libvirt driver (the only kind today),
        // built from the shell here and shared as an `Arc<dyn Driver>`.
        let driver: Arc<dyn Driver> = Arc::new(LibvirtDriver::new(config.clone(), shell));
        Self {
            deps: JobDeps {
                config,
                jobs,
                gh,
                driver,
                app_id: Arc::new(OnceCell::new()),
                slack: None,
                pin_manager: None,
                repeat_planner: None,
                slack_sessions: Arc::new(SlackSessionRegistry::new()),
            },
            max_concurrent,
        }
    }

    /// Inject the Slack client used to report `ProgressTarget::Slack` jobs
    /// (item 0002). Wired by `main` only when `[slack].enabled`; absent,
    /// those jobs still run and report nothing to Slack (the threaded
    /// surface is a no-op).
    pub fn with_slack(mut self, slack: Arc<dyn SlackClient>) -> Self {
        self.deps.slack = Some(slack);
        self
    }

    /// Enable binary-cache pin recompute (item 0025, v9 Phase 2). A no-op
    /// unless the driver runs a cache: the [`PinManager`] is built from the
    /// driver's **shared** cache `Arc` (so re-pin / evict and the driver's
    /// publish coordinate under one mutex) plus the policy / repo stores
    /// and `shell` for `ls-remote`. Wired by `main`; absent (or cache off),
    /// pins are never recomputed and the cache behaves exactly as before.
    pub fn with_pin_recompute(
        mut self,
        policy_store: Arc<dyn PolicyStore>,
        repo_store: Arc<dyn RepoStore>,
        jobs: Arc<dyn JobStore>,
        shell: Arc<dyn Shell>,
    ) -> Self {
        if let Some(cache) = self
            .deps
            .driver
            .binary_cache()
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

    /// Enable v15 repeat-run lazy chaining. The planner appends the next run
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
        // Binary-cache pin recompute on startup (item 0025, v9 Phase 2): re-pin
        // the resolved set BEFORE claiming/recovering, so the first publish's
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
        // One-time startup recovery (Phase 4B-2 + 4C): any job still `running`
        // is an orphan from a crashed/killed prior daemon — clean its leaked VM
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

/// The concurrent run loop's state: the slot pool, the in-flight task set, and
/// the task→job/slot maps. Split out from [`Runner::run`] so the fill/reap
/// machinery (the load-bearing Phase-3 code) is unit-testable without the
/// infinite loop.
struct Coordinator {
    deps: JobDeps,
    /// Free **slot indices** (`0..max_concurrent`). The pool size *is* the
    /// concurrency bound (an empty pool = full), and each in-flight job holds a
    /// stable slot index for its lifetime — which Phase-5 CPU pinning maps to a
    /// fixed cpuset (`[runner].cpu_sets[slot]`), so slot 0 always pins to the
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
    /// Phase 5: last queue position pushed per queued job id, so the position
    /// updater only edits GitHub when a job's position actually changes (and
    /// prunes entries once a job leaves the queue). In-memory only — a restart
    /// just re-pushes once.
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

    /// Recover jobs orphaned in `running` by a crashed/killed prior daemon
    /// (Phase 4B-2). Runs ONCE at startup, before any fresh claim, so every
    /// `running` row is necessarily an orphan (this daemon has started none of
    /// its own yet). For each: clean the leaked VM via the handle-less
    /// [`LibvirtDriver::cleanup_by_job_id`], THEN terminal-**cancel** the row
    /// (Phase 4C — a crash-orphan is re-triggerable, not a failure) — that
    /// order is crash-safe (a crash mid-recovery re-lists the
    /// still-`running` job next boot, so cleanup re-runs idempotently and
    /// no VM is leaked behind a terminal row).
    ///
    /// Recovery dispatches over the shared `Arc<dyn Driver>` (libvirt today —
    /// v8 Phase 1); v6's task-kind split would pick the cleanup by the orphan's
    /// stored kind.
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
                .driver
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

    /// v15 repeat groups: if a daemon stopped after run K completed but before
    /// run K+1 was appended, derive and enqueue the next run from durable DB
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
        let store = build_store_or_local(self.deps.config.as_ref());
        let surface = build_report_surface(
            self.deps.gh.clone(),
            self.deps.jobs.clone(),
            store,
            self.deps.slack.as_ref(),
            &self.deps.slack_sessions,
            &job,
        );
        surface
            .cancelled(ORPHAN_CHECK_REASON)
            .await;
    }

    /// Phase 5 (+ v8 Slice C): report each waiting job its queue position on
    /// its surface — a GitHub Check Run ("queued — N ahead"), or, for a
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

    /// Claim into every free slot, spawning a job task per claim. Each job
    /// holds its slot index until reap (which returns it), so the slot —
    /// and its Phase-5 cpuset — is stable for the run. Returns how many
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

    /// The libvirt cpuset configured for a concurrency slot (Phase 5 CPU
    /// pinning), or `None` when `[runner].cpu_sets` is unset (vCPUs float).
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
                // stuck-claim sweep recovers it) or `running` (Phase-4 recovery).
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
    /// job's concurrency-slot CPU pinning (Phase 5), `None` to float.
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
        let reporter = Reporter::new(
            self.config.clone(),
            self.jobs.clone(),
            self.gh.clone(),
            self.app_id.clone(),
            self.slack.clone(),
            self.slack_sessions.clone(),
            job.clone(),
        );
        let handle = tokio::spawn(reporter.run(events_rx, prepared_tx));

        // The worker runs inline: it waits for prepare's resolved commit, runs
        // the recipe (emitting progress to the channel), and sends the terminal.
        // On `token` cancellation it cleans up + reports aborted. Recipe is
        // selected by the `(task_kind, build_target)` axes (v10 0005), failing
        // closed on any unsupported pair so a `stacks_inspect` or
        // `block_validation` row can't silently run the stacks-bench path:
        // `build_only` builds + caches the artifact silently; `benchmark` runs
        // the bench.
        let driver = self.driver.clone();
        let worker_completed = match (job.task_kind, job.build_target) {
            (TaskKind::Benchmark, BuildTarget::StacksBench) => {
                let recipe = BenchRecipe::new(
                    driver,
                    job.bench_args.clone(),
                    vcpu_cpuset,
                    sqlite_seed_key_for(&job),
                );
                run_worker(&recipe, &job, prepared_rx, events_tx, token).await
            }
            (TaskKind::BuildOnly, BuildTarget::StacksBench) => {
                let recipe = BuildOnlyRecipe::new(driver, vcpu_cpuset);
                run_worker(&recipe, &job, prepared_rx, events_tx, token).await
            }
            (task_kind, build_target) => {
                let recipe = UnsupportedRecipe::new(format!("{task_kind:?}/{build_target:?}"));
                run_worker(&recipe, &job, prepared_rx, events_tx, token).await
            }
        };

        // After the job's execution (which may have published a freshly-built
        // binary), recompute the pinned set so a newly-built pinned ref is
        // protected from the next eviction (item 0025, v9 Phase 2). Best-effort
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

    /// v15 repeat groups: after the reporter has persisted the terminal state,
    /// ask the planner to append the next run. This is deliberately non-fatal
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
                if !job_is_final_repeat(job) {
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
            if !job_is_final_repeat(job) {
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
        let store = build_store_or_local(self.config.as_ref());
        let src = store
            .get(sqlite_key)
            .await
            .with_context(|| format!("{context}: resolve archived SQLite {sqlite_key}"))?;
        let len = std::fs::metadata(&src)
            .with_context(|| format!("{context}: stat archived SQLite {}", src.display()))?
            .len();
        anyhow::ensure!(len > 0, "{context}: archived SQLite {} is empty", src.display());

        let group_key = group_sqlite_key(artifact_prefix);
        let Some(bytes) = store
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
        let store = build_store_or_local(self.config.as_ref());
        let surface = build_report_surface(
            self.gh.clone(),
            self.jobs.clone(),
            store,
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
    job.task_kind == TaskKind::Benchmark
        && job.build_target == BuildTarget::StacksBench
        && job.requested_run_count > 1
}

fn job_is_final_repeat(job: &RunnableJob) -> bool {
    job.benchmark_run_index + 1 >= job.requested_run_count
}

fn sqlite_seed_key_for(job: &RunnableJob) -> Option<String> {
    if job_should_carry_sqlite(job) && job.benchmark_run_index > 0 {
        Some(group_sqlite_key(&job.group_artifact_prefix))
    } else {
        None
    }
}

/// The inline worker: await prepare's go/abort signal, run the recipe (emitting
/// progress onto the channel), and send the terminal outcome. Pure execution —
/// it never touches GitHub or the DB; the reporter owns all of that.
async fn run_worker<R: Recipe>(
    recipe: &R,
    job: &RunnableJob,
    prepared_rx: oneshot::Receiver<Prepared>,
    events_tx: mpsc::Sender<WorkerEvent>,
    token: CancellationToken,
) -> bool {
    // Wait for the reporter to finish `prepare` and hand us the resolved
    // commit. `Abort` (or a dropped sender) means prepare failed / the job
    // won't run — the reporter already handled any reporting, so we stop.
    let commit = match prepared_rx.await {
        Ok(Prepared::Run { commit }) => commit,
        Ok(Prepared::Abort) | Err(_) => return false,
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
    let completed = matches!(terminal, Terminal::Completed { .. });
    let _ = events_tx
        .send(WorkerEvent::Finished(terminal))
        .await;
    completed
}

/// Whether a queued job's `[reporting]` config wants a Check Run surface, so a
/// pre-claim position check makes sense (Phase 5). PR jobs key off `pr_report`,
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
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Mutex as StdMutex;

    use async_trait::async_trait;
    use sbgh_core::config::{
        ApiConfig, BaselineReport, DaemonServerConfig, GitHubConfig, LvmConfig, PathsConfig,
        PrReport, ReportingConfig, RunnerConfig, StacksBenchConfig, VmConfig,
    };
    use sbgh_core::github::test_support::FakeGitHub;
    use sbgh_core::models::{BuildTarget, GitRefKind, ResolvedCommit, TaskKind};
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::*;
    use crate::driver::{DriverOutcome, DriverStatus, Placement, TaskSpec};
    use crate::events::EventSink;
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
        /// Job ids `running_job_ids` reports as orphans (Phase 4B-2 recovery).
        orphans: StdMutex<Vec<Uuid>>,
        /// Force `running_job_ids` to error — the startup-critical path.
        fail_list: std::sync::atomic::AtomicBool,
        /// The read-only view `load_runnable` returns (4C-2 orphan-check path).
        orphan_runnable: StdMutex<Option<RunnableJob>>,
        /// The queued views `list_queued` returns (Phase 5 position path).
        queued: StdMutex<Vec<RunnableJob>>,
    }

    impl FakeSource {
        fn new(job: RunnableJob) -> Self {
            Self {
                job: StdMutex::new(Some(job)),
                calls: StdMutex::new(Vec::new()),
                started_commit: StdMutex::new(None),
                fail_persist: std::sync::atomic::AtomicBool::new(false),
                orphans: StdMutex::new(Vec::new()),
                fail_list: std::sync::atomic::AtomicBool::new(false),
                orphan_runnable: StdMutex::new(None),
                queued: StdMutex::new(Vec::new()),
            }
        }
        /// Seed the queued views `list_queued` returns (Phase 5 position path).
        fn set_queued(&self, jobs: Vec<RunnableJob>) {
            *self.queued.lock().unwrap() = jobs;
        }
        /// Seed an orphaned `running` job id for the startup-recovery path.
        fn add_orphan(&self, id: Uuid) {
            self.orphans
                .lock()
                .unwrap()
                .push(id);
        }
        /// Seed the [`RunnableJob`] view `load_runnable` returns (the orphan's
        /// reconstructed reporting context, for the 4C-2 check-conclusion
        /// path).
        fn set_orphan_runnable(&self, job: RunnableJob) {
            *self
                .orphan_runnable
                .lock()
                .unwrap() = Some(job);
        }
        /// Make `running_job_ids` error, exercising the startup-critical path.
        fn fail_list(&self) {
            self.fail_list
                .store(true, std::sync::atomic::Ordering::SeqCst);
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

    #[derive(Default)]
    struct FakeRepeatPlanner {
        appended: StdMutex<Vec<Uuid>>,
        pending: StdMutex<Vec<sbgh_core::db::jobs::PendingBenchmarkRun>>,
        details: StdMutex<HashMap<Uuid, serde_json::Value>>,
        detail_calls: StdMutex<Vec<Uuid>>,
        resume_calls: std::sync::atomic::AtomicUsize,
        fail_append: std::sync::atomic::AtomicBool,
    }

    impl FakeRepeatPlanner {
        fn appended(&self) -> Vec<Uuid> {
            self.appended
                .lock()
                .unwrap()
                .clone()
        }

        fn resume_calls(&self) -> usize {
            self.resume_calls
                .load(std::sync::atomic::Ordering::SeqCst)
        }

        fn fail_append(&self) {
            self.fail_append
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }

        fn set_pending(&self, pending: Vec<sbgh_core::db::jobs::PendingBenchmarkRun>) {
            *self.pending.lock().unwrap() = pending;
        }

        fn set_completed_detail(&self, job_id: Uuid, detail: serde_json::Value) {
            self.details
                .lock()
                .unwrap()
                .insert(job_id, detail);
        }

        fn detail_calls(&self) -> Vec<Uuid> {
            self.detail_calls
                .lock()
                .unwrap()
                .clone()
        }
    }

    #[async_trait]
    impl RepeatRunPlanner for FakeRepeatPlanner {
        async fn append_next_benchmark_run(
            &self,
            completed_job_id: Uuid,
        ) -> anyhow::Result<Option<sbgh_core::models::Job>> {
            self.appended
                .lock()
                .unwrap()
                .push(completed_job_id);
            if self
                .fail_append
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                anyhow::bail!("forced append failure");
            }
            Ok(None)
        }

        async fn pending_completed_benchmark_runs(
            &self,
        ) -> anyhow::Result<Vec<sbgh_core::db::jobs::PendingBenchmarkRun>> {
            self.resume_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(self
                .pending
                .lock()
                .unwrap()
                .clone())
        }

        async fn completed_event_detail(
            &self,
            job_id: Uuid,
        ) -> anyhow::Result<Option<serde_json::Value>> {
            self.detail_calls
                .lock()
                .unwrap()
                .push(job_id);
            Ok(self
                .details
                .lock()
                .unwrap()
                .get(&job_id)
                .cloned())
        }
    }

    struct CompletedDriver;

    #[async_trait]
    impl Driver for CompletedDriver {
        async fn run_task(
            &self,
            _ctx: &TaskContext<'_>,
            _spec: &TaskSpec,
            _sink: &dyn EventSink,
            _cancel: &CancellationToken,
            _placement: &Placement,
        ) -> anyhow::Result<DriverOutcome> {
            Ok(DriverOutcome {
                status: DriverStatus::Completed,
                summary: serde_json::json!({ "finish_reason": "test" }),
            })
        }

        async fn cleanup_by_job_id(&self, _job_id: &str) -> bool {
            true
        }
    }

    struct FailedDriver;

    #[async_trait]
    impl Driver for FailedDriver {
        async fn run_task(
            &self,
            _ctx: &TaskContext<'_>,
            _spec: &TaskSpec,
            _sink: &dyn EventSink,
            _cancel: &CancellationToken,
            _placement: &Placement,
        ) -> anyhow::Result<DriverOutcome> {
            Ok(DriverOutcome {
                status: DriverStatus::Failed("bench failed".into()),
                summary: serde_json::json!({ "finish_reason": "bench_failed" }),
            })
        }

        async fn cleanup_by_job_id(&self, _job_id: &str) -> bool {
            true
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
        async fn load_runnable(&self, _job_id: Uuid) -> anyhow::Result<Option<RunnableJob>> {
            Ok(self
                .orphan_runnable
                .lock()
                .unwrap()
                .clone())
        }
        async fn list_queued(&self) -> anyhow::Result<Vec<RunnableJob>> {
            Ok(self
                .queued
                .lock()
                .unwrap()
                .clone())
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
        async fn find_baseline(
            &self,
            _merge_base_sha: &str,
            _base_ref: &str,
            _at: Option<chrono::DateTime<chrono::Utc>>,
            _workload_key: &str,
        ) -> anyhow::Result<Option<crate::job_source::BaselineRef>> {
            Ok(None)
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
        async fn cancel(&self, _job: &RunnableJob, _remark: &str) -> anyhow::Result<()> {
            self.record("cancel");
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
        async fn set_plan_message_ts(
            &self,
            _job: &RunnableJob,
            _message_ts: &str,
        ) -> anyhow::Result<()> {
            self.record("set_plan_message_ts");
            Ok(())
        }
        async fn sweep_stuck_claims(&self, _lease: chrono::Duration) -> anyhow::Result<u64> {
            Ok(0)
        }
        async fn running_job_ids(&self) -> anyhow::Result<Vec<Uuid>> {
            if self
                .fail_list
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                anyhow::bail!("forced running_job_ids failure");
            }
            Ok(self
                .orphans
                .lock()
                .unwrap()
                .clone())
        }
        async fn cancel_orphan(&self, _job_id: Uuid, _remark: &str) -> anyhow::Result<bool> {
            self.record("cancel_orphan");
            Ok(true)
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
                noise_cv_pct: None,
            },
            runner: RunnerConfig {
                max_concurrent_jobs: 1,
                max_clean_repetitions: 5,
                cpu_sets: vec![],
                host_cpus: None,
            },
            artifacts: Default::default(),
            slack: Default::default(),
            llm: Default::default(),
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
            benchmark_group_id: Uuid::new_v4(),
            benchmark_spec_id: Uuid::new_v4(),
            benchmark_run_index: 0,
            requested_run_count: 2,
            group_artifact_prefix: "group-a".into(),
            repository: "acme/widgets".into(),
            commit: "abc123".into(), // pre-resolved → preflight is a no-op
            git_ref_display: "develop".into(),
            git_ref_kind: GitRefKind::Branch,
            installation_id: 7,
            task_kind: TaskKind::Benchmark,
            build_target: BuildTarget::StacksBench,
            workload_key: None,
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

    /// v10 (0005): a build-only job (`task_kind = build_only` → a `Silent`
    /// report target) dispatches the build-only recipe and reports nothing —
    /// GitHub is never touched, even on a build failure. The build itself
    /// (provision → build → publish → stop, no bench) is driver-tested in
    /// `build_only_skips_bench_phase`.
    #[tokio::test]
    async fn run_once_build_only_job_is_silent() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(&tmp);

        let job = RunnableJob {
            id: Uuid::new_v4(),
            benchmark_group_id: Uuid::new_v4(),
            benchmark_spec_id: Uuid::new_v4(),
            benchmark_run_index: 0,
            requested_run_count: 2,
            group_artifact_prefix: "group-a".into(),
            repository: "acme/widgets".into(),
            commit: "abc123".into(), // pre-resolved → preflight is a no-op
            git_ref_display: "develop".into(),
            git_ref_kind: GitRefKind::Branch,
            installation_id: 7,
            task_kind: TaskKind::BuildOnly,
            build_target: BuildTarget::StacksBench,
            workload_key: None,
            bench_args: vec![],
            progress: ProgressTarget::Silent,
            claim_token: Some(Uuid::new_v4()),
        };
        let source = Arc::new(FakeSource::new(job));

        // The first provisioning command fails → the build-only run terminalizes
        // as Failed; a silent job must still make zero GitHub calls.
        let gh = Arc::new(FakeGitHub::new());
        let shell = Arc::new(RecordingShell::new());
        shell.reply(PreparedReply::fail(b"boom: git fetch failed"));

        let runner = Runner::new(config, source.clone(), gh.clone(), shell);
        assert!(
            runner
                .run_once()
                .await
                .unwrap(),
            "claimed + executed one job",
        );

        // Lifecycle ran (start_running → fail), but the silent report surface
        // posted nothing.
        assert_eq!(source.calls(), vec!["start_running", "fail"]);
        assert!(gh.calls().is_empty(), "a build-only (silent) job must make no GitHub calls",);
    }

    #[tokio::test]
    async fn completed_run_appends_next_repeat_after_terminal_persist() {
        let tmp = TempDir::new().unwrap();
        let config = Arc::new(test_config(&tmp));
        let job = RunnableJob {
            id: Uuid::new_v4(),
            benchmark_group_id: Uuid::new_v4(),
            benchmark_spec_id: Uuid::new_v4(),
            benchmark_run_index: 0,
            requested_run_count: 2,
            group_artifact_prefix: "group-a".into(),
            repository: "acme/widgets".into(),
            commit: "abc123".into(),
            git_ref_display: "develop".into(),
            git_ref_kind: GitRefKind::Branch,
            installation_id: 7,
            task_kind: TaskKind::Benchmark,
            build_target: BuildTarget::StacksBench,
            workload_key: None,
            bench_args: vec![],
            progress: ProgressTarget::CommitCheck { check_run_id: None },
            claim_token: Some(Uuid::new_v4()),
        };
        let source = Arc::new(FakeSource::new(job.clone()));
        let planner = Arc::new(FakeRepeatPlanner::default());
        let sqlite_key = crate::artifact_store::artifact_key(
            &job.id.to_string(),
            crate::libvirt::forensics::SQLITE_RELATIVE,
        );
        let sqlite_path = config
            .paths
            .results_archive_dir
            .join(&sqlite_key);
        std::fs::create_dir_all(sqlite_path.parent().unwrap()).unwrap();
        std::fs::write(&sqlite_path, b"sqlite bytes").unwrap();
        planner.set_completed_detail(
            job.id,
            serde_json::json!({ "sqlite_archived_path": sqlite_key }),
        );
        let deps = JobDeps {
            config,
            jobs: source.clone(),
            gh: Arc::new(FakeGitHub::new()),
            driver: Arc::new(CompletedDriver),
            app_id: Arc::new(OnceCell::new()),
            slack: None,
            pin_manager: None,
            repeat_planner: Some(planner.clone()),
            slack_sessions: Default::default(),
        };

        deps.run(job.clone(), None, CancellationToken::new())
            .await
            .expect("completed run should not fail");

        assert_eq!(source.calls(), vec!["start_running", "complete"]);
        assert_eq!(planner.appended(), vec![job.id]);
        let group_sqlite = tmp
            .path()
            .join("archive")
            .join(group_sqlite_key(&job.group_artifact_prefix));
        assert_eq!(std::fs::read(group_sqlite).unwrap(), b"sqlite bytes");
    }

    #[tokio::test]
    async fn repeat_append_failure_is_nonfatal_to_completed_run() {
        let tmp = TempDir::new().unwrap();
        let config = Arc::new(test_config(&tmp));
        let job = RunnableJob {
            id: Uuid::new_v4(),
            benchmark_group_id: Uuid::new_v4(),
            benchmark_spec_id: Uuid::new_v4(),
            benchmark_run_index: 0,
            requested_run_count: 2,
            group_artifact_prefix: "group-b".into(),
            repository: "acme/widgets".into(),
            commit: "abc123".into(),
            git_ref_display: "develop".into(),
            git_ref_kind: GitRefKind::Branch,
            installation_id: 7,
            task_kind: TaskKind::Benchmark,
            build_target: BuildTarget::StacksBench,
            workload_key: None,
            bench_args: vec![],
            progress: ProgressTarget::CommitCheck { check_run_id: None },
            claim_token: Some(Uuid::new_v4()),
        };
        let source = Arc::new(FakeSource::new(job.clone()));
        let planner = Arc::new(FakeRepeatPlanner::default());
        let sqlite_key = crate::artifact_store::artifact_key(
            &job.id.to_string(),
            crate::libvirt::forensics::SQLITE_RELATIVE,
        );
        let sqlite_path = config
            .paths
            .results_archive_dir
            .join(&sqlite_key);
        std::fs::create_dir_all(sqlite_path.parent().unwrap()).unwrap();
        std::fs::write(&sqlite_path, b"sqlite bytes").unwrap();
        planner.set_completed_detail(
            job.id,
            serde_json::json!({ "sqlite_archived_path": sqlite_key }),
        );
        planner.fail_append();
        let deps = JobDeps {
            config,
            jobs: source.clone(),
            gh: Arc::new(FakeGitHub::new()),
            driver: Arc::new(CompletedDriver),
            app_id: Arc::new(OnceCell::new()),
            slack: None,
            pin_manager: None,
            repeat_planner: Some(planner.clone()),
            slack_sessions: Default::default(),
        };

        deps.run(job.clone(), None, CancellationToken::new())
            .await
            .expect("append failure is retryable and must not fail the completed run");

        assert_eq!(source.calls(), vec!["start_running", "complete"]);
        assert_eq!(planner.appended(), vec![job.id]);
    }

    #[tokio::test]
    async fn failed_repeat_does_not_try_to_carry_or_append_next_run() {
        let tmp = TempDir::new().unwrap();
        let config = Arc::new(test_config(&tmp));
        let job = RunnableJob {
            id: Uuid::new_v4(),
            benchmark_group_id: Uuid::new_v4(),
            benchmark_spec_id: Uuid::new_v4(),
            benchmark_run_index: 0,
            requested_run_count: 2,
            group_artifact_prefix: "group-failed".into(),
            repository: "acme/widgets".into(),
            commit: "abc123".into(),
            git_ref_display: "develop".into(),
            git_ref_kind: GitRefKind::Branch,
            installation_id: 7,
            task_kind: TaskKind::Benchmark,
            build_target: BuildTarget::StacksBench,
            workload_key: None,
            bench_args: vec![],
            progress: ProgressTarget::CommitCheck { check_run_id: None },
            claim_token: Some(Uuid::new_v4()),
        };
        let source = Arc::new(FakeSource::new(job.clone()));
        let planner = Arc::new(FakeRepeatPlanner::default());
        let deps = JobDeps {
            config,
            jobs: source.clone(),
            gh: Arc::new(FakeGitHub::new()),
            driver: Arc::new(FailedDriver),
            app_id: Arc::new(OnceCell::new()),
            slack: None,
            pin_manager: None,
            repeat_planner: Some(planner.clone()),
            slack_sessions: Default::default(),
        };

        deps.run(job, None, CancellationToken::new())
            .await
            .expect("a failed run is terminalized by the reporter");

        assert_eq!(source.calls(), vec!["start_running", "fail"]);
        assert!(
            planner
                .detail_calls()
                .is_empty(),
            "failed repeats must not enter carry-forward",
        );
        assert!(planner.appended().is_empty(), "failed repeats must not append the next run",);
    }

    #[tokio::test]
    async fn missing_carried_sqlite_blocks_next_repeat_append() {
        let tmp = TempDir::new().unwrap();
        let config = Arc::new(test_config(&tmp));
        let job = RunnableJob {
            id: Uuid::new_v4(),
            benchmark_group_id: Uuid::new_v4(),
            benchmark_spec_id: Uuid::new_v4(),
            benchmark_run_index: 0,
            requested_run_count: 2,
            group_artifact_prefix: "group-missing".into(),
            repository: "acme/widgets".into(),
            commit: "abc123".into(),
            git_ref_display: "develop".into(),
            git_ref_kind: GitRefKind::Branch,
            installation_id: 7,
            task_kind: TaskKind::Benchmark,
            build_target: BuildTarget::StacksBench,
            workload_key: None,
            bench_args: vec![],
            progress: ProgressTarget::Slack {
                channel: "C1".into(),
                message_ts: "REQ".into(),
                plan_message_ts: Some("PLAN_TS".into()),
            },
            claim_token: Some(Uuid::new_v4()),
        };
        let source = Arc::new(FakeSource::new(job.clone()));
        assert!(!job_is_final_repeat(&job), "fixture must be an intermediate repeat");
        let planner = Arc::new(FakeRepeatPlanner::default());
        planner.set_completed_detail(
            job.id,
            serde_json::json!({ "sqlite_archived_path": "missing/appdata/stacks-bench.db" }),
        );
        let slack = Arc::new(FakePositionSlack::default());
        let deps = JobDeps {
            config,
            jobs: source.clone(),
            gh: Arc::new(FakeGitHub::new()),
            driver: Arc::new(CompletedDriver),
            app_id: Arc::new(OnceCell::new()),
            slack: Some(slack.clone()),
            pin_manager: None,
            repeat_planner: Some(planner.clone()),
            slack_sessions: Default::default(),
        };

        deps.run(job.clone(), None, CancellationToken::new())
            .await
            .expect("missing carried DB is retryable and must not fail the completed run");

        assert_eq!(source.calls(), vec!["start_running", "complete"]);
        assert_eq!(planner.detail_calls(), vec![job.id]);
        assert!(planner.appended().is_empty(), "next repeat waits for carried DB");
        let stops = slack
            .stops
            .lock()
            .unwrap()
            .clone();
        let updates = slack
            .updates
            .lock()
            .unwrap()
            .clone();
        let appends = slack
            .appends
            .lock()
            .unwrap()
            .clone();
        let rendered = stops
            .iter()
            .chain(updates.iter())
            .chain(appends.iter())
            .map(|(_, _, body)| body.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            rendered.contains("repeat group stalled"),
            "carry-forward stall must update the shared Slack surface; stops={stops:?} \
             updates={updates:?} appends={appends:?}",
        );
        assert!(rendered.contains("Clean Repeat Summary"), "{rendered}");
    }

    #[tokio::test]
    async fn coordinator_resumes_pending_repeats_on_startup() {
        let tmp = TempDir::new().unwrap();
        let config = Arc::new(test_config(&tmp));
        let job = pr_job("abc123", None);
        let source = Arc::new(FakeSource::new(job));
        let planner = Arc::new(FakeRepeatPlanner::default());
        let completed_job_id = Uuid::new_v4();
        let sqlite_key = crate::artifact_store::artifact_key(
            &completed_job_id.to_string(),
            crate::libvirt::forensics::SQLITE_RELATIVE,
        );
        let sqlite_path = config
            .paths
            .results_archive_dir
            .join(&sqlite_key);
        std::fs::create_dir_all(sqlite_path.parent().unwrap()).unwrap();
        std::fs::write(&sqlite_path, b"startup sqlite").unwrap();
        planner.set_completed_detail(
            completed_job_id,
            serde_json::json!({ "sqlite_archived_path": sqlite_key }),
        );
        planner.set_pending(vec![sbgh_core::db::jobs::PendingBenchmarkRun {
            completed_job_id,
            benchmark_group_id: Uuid::new_v4(),
            benchmark_spec_id: Uuid::new_v4(),
            benchmark_run_index: 0,
            requested_run_count: 2,
            artifact_prefix: "startup-group".into(),
        }]);
        let deps = JobDeps {
            config,
            jobs: source,
            gh: Arc::new(FakeGitHub::new()),
            driver: Arc::new(CompletedDriver),
            app_id: Arc::new(OnceCell::new()),
            slack: None,
            pin_manager: None,
            repeat_planner: Some(planner.clone()),
            slack_sessions: Default::default(),
        };
        let coord = Coordinator::new(deps, 1, CancellationToken::new());

        coord
            .resume_pending_repeats()
            .await;

        assert_eq!(planner.resume_calls(), 1);
        assert_eq!(planner.appended(), vec![completed_job_id]);
        let group_sqlite = tmp
            .path()
            .join("archive")
            .join(group_sqlite_key("startup-group"));
        assert_eq!(std::fs::read(group_sqlite).unwrap(), b"startup sqlite");
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
            benchmark_group_id: Uuid::new_v4(),
            benchmark_spec_id: Uuid::new_v4(),
            benchmark_run_index: 0,
            requested_run_count: 1,
            group_artifact_prefix: Uuid::new_v4().to_string(),
            repository: "octo/core".into(),
            commit: String::new(), // unresolved — a tag job
            git_ref_display: "release/1.2".into(),
            git_ref_kind: GitRefKind::Tag,
            installation_id: 7,
            task_kind: TaskKind::Benchmark,
            build_target: BuildTarget::StacksBench,
            workload_key: None,
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

    /// v5 gate: a `ProgressTarget::Slack` job enqueues with an **empty** commit
    /// and a rev (`develop`), so it MUST resolve to a commit at claim time (the
    /// **bare** ref, not `tags/…`) and hand it to `start_running` — proving an
    /// accepted Slack job passes `prepare` rather than failing the empty-commit
    /// guard.
    #[tokio::test]
    async fn run_once_resolves_slack_rev_commit_in_preflight() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(&tmp);

        let job = RunnableJob {
            id: Uuid::new_v4(),
            benchmark_group_id: Uuid::new_v4(),
            benchmark_spec_id: Uuid::new_v4(),
            benchmark_run_index: 0,
            requested_run_count: 1,
            group_artifact_prefix: Uuid::new_v4().to_string(),
            repository: "octo/core".into(),
            commit: String::new(), // unresolved — a Slack ad-hoc job
            git_ref_display: "develop".into(),
            git_ref_kind: GitRefKind::Branch,
            installation_id: 7,
            task_kind: TaskKind::Benchmark,
            build_target: BuildTarget::StacksBench,
            workload_key: None,
            bench_args: vec![],
            progress: ProgressTarget::Slack {
                channel: "C1".into(),
                message_ts: "1700000000.000100".into(),
                plan_message_ts: None,
            },
            claim_token: Some(Uuid::new_v4()),
        };
        let source = Arc::new(FakeSource::new(job));

        let gh = Arc::new(FakeGitHub::new());
        // Resolved by the BARE rev (Slack doesn't qualify `tags/…`).
        gh.set_commit("octo/core", "develop", "slacksha", None);
        let shell = Arc::new(RecordingShell::new());
        shell.reply(PreparedReply::fail(b"boom")); // fail fast; we only assert preflight
        let runner = Runner::new(config, source.clone(), gh.clone(), shell);
        assert!(
            runner
                .run_once()
                .await
                .unwrap()
        );

        let resolved = source
            .started_commit()
            .expect("start_running received the resolved Slack commit");
        assert_eq!(resolved.hash, "slacksha");
        assert!(
            gh.calls()
                .iter()
                .any(|c| matches!(
                    c,
                    sbgh_core::github::test_support::FakeCall::ResolveCommit { git_ref, .. }
                        if git_ref == "develop"
                )),
            "runner must resolve the bare Slack rev"
        );
    }

    use sbgh_core::github::test_support::FakeCall;

    fn config_with(tmp: &TempDir, pr: PrReport, baseline: BaselineReport) -> DaemonConfig {
        let mut c = test_config(tmp);
        c.reporting = ReportingConfig {
            pr_report: pr,
            baseline_report: baseline,
            noise_cv_pct: None,
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
            benchmark_group_id: Uuid::new_v4(),
            benchmark_spec_id: Uuid::new_v4(),
            benchmark_run_index: 0,
            requested_run_count: 1,
            group_artifact_prefix: Uuid::new_v4().to_string(),
            repository: "acme/widgets".into(),
            commit: commit.into(),
            git_ref_display: "feature".into(),
            git_ref_kind: GitRefKind::Branch,
            installation_id: 7,
            task_kind: TaskKind::Benchmark,
            build_target: BuildTarget::StacksBench,
            workload_key: None,
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
            let driver: Arc<dyn Driver> = Arc::new(LibvirtDriver::new(config.clone(), shell));
            let deps = JobDeps {
                config: config.clone(),
                jobs: source.clone(),
                gh: Arc::new(FakeGitHub::new()),
                driver,
                app_id: app_id.clone(),
                slack: None,
                pin_manager: None,
                repeat_planner: None,
                slack_sessions: Default::default(),
            };
            handles.push(tokio::spawn(deps.run(job, None, CancellationToken::new())));
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
        async fn load_runnable(&self, _job_id: Uuid) -> anyhow::Result<Option<RunnableJob>> {
            Ok(None)
        }
        async fn list_queued(&self) -> anyhow::Result<Vec<RunnableJob>> {
            Ok(vec![])
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
        async fn find_baseline(
            &self,
            _merge_base_sha: &str,
            _base_ref: &str,
            _at: Option<chrono::DateTime<chrono::Utc>>,
            _workload_key: &str,
        ) -> anyhow::Result<Option<crate::job_source::BaselineRef>> {
            Ok(None)
        }
        async fn fail(
            &self,
            _: &RunnableJob,
            _: &str,
            _: Option<&serde_json::Value>,
        ) -> anyhow::Result<()> {
            Ok(())
        }
        async fn cancel(&self, _: &RunnableJob, _: &str) -> anyhow::Result<()> {
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
        async fn set_plan_message_ts(&self, _: &RunnableJob, _: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn running_job_ids(&self) -> anyhow::Result<Vec<Uuid>> {
            Ok(vec![])
        }
        async fn cancel_orphan(&self, _: Uuid, _: &str) -> anyhow::Result<bool> {
            Ok(false)
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
        let driver: Arc<dyn Driver> =
            Arc::new(LibvirtDriver::new(config.clone(), Arc::new(RecordingShell::new())));
        let deps = JobDeps {
            config,
            jobs: source.clone(),
            gh: Arc::new(FakeGitHub::new()),
            driver,
            app_id: Arc::new(OnceCell::new()),
            slack: None,
            pin_manager: None,
            repeat_planner: None,
            slack_sessions: Default::default(),
        };
        let mut coord = Coordinator::new(deps, 2, CancellationToken::new()); // max_concurrent = 2
        let never_draining = CancellationToken::new();

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
                .wait_for_progress(&never_draining)
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
                .wait_for_progress(&never_draining)
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
        let driver: Arc<dyn Driver> = Arc::new(LibvirtDriver::new(config, shell));
        let recipe = BenchRecipe::new(driver, vec![], None, None);
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

    /// With a drain requested, the coordinator **stops claiming** (queued jobs
    /// wait for the next boot) and, being idle, returns — firing `exit` so the
    /// rest of the process can stop.
    #[tokio::test]
    async fn drain_stops_claiming_and_fires_exit_when_idle() {
        let tmp = TempDir::new().unwrap();
        let config = config_with(&tmp, PrReport::Comment, BaselineReport::None);
        // The source has a job ready, but a drain means it must never be claimed.
        let source = Arc::new(FakeSource::new(pr_job("abc123", None)));
        let runner = Runner::new(
            config,
            source.clone(),
            Arc::new(FakeGitHub::new()),
            Arc::new(RecordingShell::new()),
        );

        let shutdown = Shutdown::new();
        shutdown.draining.cancel(); // drain requested before the loop runs

        runner
            .run(shutdown.clone())
            .await
            .unwrap();

        assert!(shutdown.exit.is_cancelled(), "coordinator fired process-exit on drain-complete",);
        assert!(
            source.calls().is_empty(),
            "drained: the queued job was never claimed/started, got {:?}",
            source.calls(),
        );
    }

    /// Startup recovery (Phase 4B-2 + 4C): a job left `running` by a crashed
    /// daemon is cleaned (the coordinator runs `cleanup_by_job_id` — observed
    /// as a `virsh destroy` on the shell) and terminal-cancelled
    /// (`cancel_orphan`) before the loop starts claiming. Driven with a
    /// pre-set drain so the loop exits straight after recovery.
    #[tokio::test]
    async fn startup_recovers_orphaned_running_job() {
        let tmp = TempDir::new().unwrap();
        let config = config_with(&tmp, PrReport::Comment, BaselineReport::None);
        let source = Arc::new(FakeSource::new(pr_job("abc123", None)));
        source.add_orphan(Uuid::new_v4());

        // `cleanup_by_job_id` for an orphan with no loop attached issues seven
        // shell calls: destroy, undefine, umount(tmpfs), umount(source.mnt),
        // losetup -j (empty), lvremove, git prune.
        let shell = Arc::new(RecordingShell::new());
        shell
            .expect_ok(1) // virsh destroy
            .expect_ok(1) // virsh undefine
            .expect_ok(1) // umount tmpfs
            .expect_ok(1) // umount source.mnt
            .reply(PreparedReply::with_stdout("")) // losetup -j → nothing attached
            .expect_ok(1) // lvremove
            .expect_ok(1); // git prune

        let runner =
            Runner::new(config, source.clone(), Arc::new(FakeGitHub::new()), shell.clone());
        let shutdown = Shutdown::new();
        shutdown.draining.cancel(); // skip the claim loop — we exercise only startup recovery

        runner
            .run(shutdown)
            .await
            .unwrap();

        // Recovery cleaned the leaked VM …
        assert!(
            shell.calls().iter().any(|c| {
                c.program.ends_with("virsh")
                    && c.args
                        .iter()
                        .any(|a| a == "destroy")
            }),
            "recovery ran cleanup_by_job_id (virsh destroy) for the orphan",
        );
        // … and terminal-cancelled the orphaned row.
        assert!(
            source
                .calls()
                .contains(&"cancel_orphan"),
            "recovery terminal-cancelled the orphan via cancel_orphan, got {:?}",
            source.calls(),
        );
    }

    /// Codex 4B-2 Medium: if `cleanup_by_job_id` can't fully clear the VM (the
    /// source loop won't detach), the orphan must be LEFT `running` (no
    /// `cancel_orphan`) so the next boot retries — and a per-orphan failure
    /// stays non-fatal (the loop still runs to a clean exit).
    #[tokio::test]
    async fn startup_leaves_orphan_running_when_cleanup_incomplete() {
        let tmp = TempDir::new().unwrap();
        let config = config_with(&tmp, PrReport::Comment, BaselineReport::None);
        let source = Arc::new(FakeSource::new(pr_job("abc123", None)));
        source.add_orphan(Uuid::new_v4());

        // `losetup -j` lists a device whose `-d` FAILS → cleanup reports incomplete.
        let shell = Arc::new(RecordingShell::new());
        shell
            .expect_ok(1) // virsh destroy
            .expect_ok(1) // virsh undefine
            .expect_ok(1) // umount tmpfs
            .expect_ok(1) // umount source.mnt
            .reply(PreparedReply::with_stdout("/dev/loop9: [2049]:1 (j/source.raw)\n")) // losetup -j
            .reply(PreparedReply::fail("device or resource busy")) // losetup -d fails
            .expect_ok(1) // lvremove
            .expect_ok(1); // git prune

        let runner =
            Runner::new(config, source.clone(), Arc::new(FakeGitHub::new()), shell.clone());
        let shutdown = Shutdown::new();
        shutdown.draining.cancel();

        runner
            .run(shutdown)
            .await
            .unwrap(); // per-orphan failure is non-fatal

        assert!(
            !source
                .calls()
                .contains(&"cancel_orphan"),
            "an incompletely-cleaned orphan must be left `running`, not cancelled; got {:?}",
            source.calls(),
        );
    }

    /// Codex 4B-2 Medium: failure to even *enumerate* running rows is
    /// startup-critical (we can't rule out live orphan VMs), so it propagates —
    /// the runner returns `Err` (process exits → systemd retries) and never
    /// claims fresh work.
    #[tokio::test]
    async fn startup_recovery_aborts_when_listing_running_jobs_fails() {
        let tmp = TempDir::new().unwrap();
        let config = config_with(&tmp, PrReport::Comment, BaselineReport::None);
        let source = Arc::new(FakeSource::new(pr_job("abc123", None)));
        source.fail_list(); // running_job_ids errors

        let runner = Runner::new(
            config,
            source.clone(),
            Arc::new(FakeGitHub::new()),
            Arc::new(RecordingShell::new()),
        );
        let shutdown = Shutdown::new();
        shutdown.draining.cancel(); // even mid-drain, recovery runs first and must abort

        let err = runner
            .run(shutdown)
            .await
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("enumerating `running` jobs failed"),
            "list failure is startup-critical and propagates, got: {err:#}",
        );
        assert!(source.calls().is_empty(), "aborted before any claim, got {:?}", source.calls(),);
    }

    /// Phase 4C-2: a recovered orphan's stuck `in_progress` Check Run is
    /// concluded `cancelled` (gray) via the normal reporting path —
    /// `load_runnable` reconstructs the orphan's reporting context and the
    /// surface's `cancelled` concludes its check.
    #[tokio::test]
    async fn startup_concludes_orphan_check_as_cancelled() {
        let tmp = TempDir::new().unwrap();
        let config = config_with(&tmp, PrReport::Comment, BaselineReport::None);
        let orphan_id = Uuid::new_v4();
        let source = Arc::new(FakeSource::new(pr_job("abc123", None)));
        source.add_orphan(orphan_id);
        // The reconstructed orphan view carries a commit check (id 555) to conclude.
        source.set_orphan_runnable(RunnableJob {
            id: orphan_id,
            benchmark_group_id: Uuid::new_v4(),
            benchmark_spec_id: Uuid::new_v4(),
            benchmark_run_index: 0,
            requested_run_count: 1,
            group_artifact_prefix: Uuid::new_v4().to_string(),
            progress: ProgressTarget::CommitCheck { check_run_id: Some(555) },
            ..pr_job("abc123", None)
        });

        let gh = Arc::new(FakeGitHub::new());
        // `cleanup_by_job_id` (no loop attached) issues seven shell calls.
        let shell = Arc::new(RecordingShell::new());
        shell
            .expect_ok(1) // virsh destroy
            .expect_ok(1) // virsh undefine
            .expect_ok(1) // umount tmpfs
            .expect_ok(1) // umount source.mnt
            .reply(PreparedReply::with_stdout("")) // losetup -j → none
            .expect_ok(1) // lvremove
            .expect_ok(1); // git prune

        let runner = Runner::new(config, source.clone(), gh.clone(), shell);
        let shutdown = Shutdown::new();
        shutdown.draining.cancel();

        runner
            .run(shutdown)
            .await
            .unwrap();

        // The orphan row was cancelled …
        assert!(
            source
                .calls()
                .contains(&"cancel_orphan"),
            "orphan row cancelled, got {:?}",
            source.calls(),
        );
        // … and its stuck check concluded `cancelled` (neutral-gray), not left
        // spinning.
        let concluded = gh
            .calls()
            .into_iter()
            .find_map(|c| match c {
                FakeCall::UpdateCheckRun { check_run_id: 555, state, .. } => Some(state),
                _ => None,
            })
            .expect("the orphan's check was concluded");
        assert_eq!(
            concluded,
            sbgh_core::github::CheckRunState::Completed(
                sbgh_core::github::CheckRunConclusion::Cancelled
            ),
        );
    }

    /// Build a `Coordinator` over a `FakeSource` for the queue-position tests.
    fn position_coord(
        config: DaemonConfig,
        source: Arc<FakeSource>,
        gh: Arc<FakeGitHub>,
    ) -> Coordinator {
        let config = Arc::new(config);
        let driver: Arc<dyn Driver> =
            Arc::new(LibvirtDriver::new(config.clone(), Arc::new(RecordingShell::new())));
        let deps = JobDeps {
            config,
            jobs: source,
            gh,
            driver,
            app_id: Arc::new(OnceCell::new()),
            slack: None,
            pin_manager: None,
            repeat_planner: None,
            slack_sessions: Default::default(),
        };
        Coordinator::new(deps, 1, CancellationToken::new())
    }

    fn create_summary_for(gh: &FakeGitHub, job_id: Uuid) -> Option<String> {
        gh.calls()
            .into_iter()
            .find_map(|c| match c {
                FakeCall::CreateCheckRun { external_id, output, .. }
                    if external_id == job_id.to_string() =>
                {
                    Some(output.summary)
                }
                _ => None,
            })
    }

    fn create_count(gh: &FakeGitHub) -> usize {
        gh.calls()
            .iter()
            .filter(|c| matches!(c, FakeCall::CreateCheckRun { .. }))
            .count()
    }

    /// Phase 5: the coordinator reports each queued job its position on a
    /// freshly created (and persisted, so the claim-time reporter adopts
    /// it) check, with text matching claim order. A second pass with
    /// unchanged positions makes NO new GitHub edits (the `last_positions`
    /// debounce).
    #[tokio::test]
    async fn coordinator_reports_queue_positions_and_debounces() {
        let tmp = TempDir::new().unwrap();
        let config = config_with(&tmp, PrReport::Check, BaselineReport::None);
        let j0 = pr_job("sha0", None);
        let j1 = pr_job("sha1", None);
        let source = Arc::new(FakeSource::new(pr_job("unused", None)));
        source.set_queued(vec![j0.clone(), j1.clone()]);
        let gh = Arc::new(FakeGitHub::new());
        let mut coord = position_coord(config, source.clone(), gh.clone());

        coord
            .update_queue_positions()
            .await;

        // in_flight = 0 → ahead = index. Position text matches claim order.
        assert_eq!(create_summary_for(&gh, j0.id).as_deref(), Some("queued — next to run"));
        assert_eq!(create_summary_for(&gh, j1.id).as_deref(), Some("queued — 1 run ahead"));
        assert_eq!(create_count(&gh), 2);
        // Each created check's id was persisted so a later claim adopts it.
        assert_eq!(
            source
                .calls()
                .iter()
                .filter(|c| **c == "set_check_run")
                .count(),
            2,
        );

        // Second pass, unchanged queue → debounced (no new creates).
        coord
            .update_queue_positions()
            .await;
        assert_eq!(create_count(&gh), 2, "unchanged positions make no new GitHub edits");
    }

    /// A queued job that already has a check (persisted by a prior tick / read
    /// back on re-claim) is **updated**, never duplicated.
    #[tokio::test]
    async fn coordinator_updates_existing_position_check_without_duplicating() {
        let tmp = TempDir::new().unwrap();
        let config = config_with(&tmp, PrReport::Check, BaselineReport::None);
        let source = Arc::new(FakeSource::new(pr_job("unused", None)));
        source.set_queued(vec![pr_job("sha", Some(900))]);
        let gh = Arc::new(FakeGitHub::new());
        let mut coord = position_coord(config, source.clone(), gh.clone());

        coord
            .update_queue_positions()
            .await;

        assert!(
            gh.calls()
                .iter()
                .any(|c| matches!(c, FakeCall::UpdateCheckRun { check_run_id: 900, .. })),
            "the existing check was updated",
        );
        assert_eq!(create_count(&gh), 0, "must update, not create a duplicate");
    }

    /// Eligibility: a baseline job whose `baseline_report` wants no check, and
    /// a job with an unresolved commit (a tag job pre-claim), both get NO
    /// pre-claim position check.
    #[tokio::test]
    async fn coordinator_skips_position_for_no_check_and_unresolved_jobs() {
        let tmp = TempDir::new().unwrap();
        let config = config_with(&tmp, PrReport::Check, BaselineReport::None);
        let baseline = RunnableJob {
            progress: ProgressTarget::CommitCheck { check_run_id: None },
            ..pr_job("sha", None)
        };
        let unresolved = RunnableJob {
            commit: String::new(),
            ..pr_job("sha", None)
        };
        let source = Arc::new(FakeSource::new(pr_job("unused", None)));
        source.set_queued(vec![baseline, unresolved]);
        let gh = Arc::new(FakeGitHub::new());
        let mut coord = position_coord(config, source.clone(), gh.clone());

        coord
            .update_queue_positions()
            .await;

        assert!(
            gh.calls().is_empty(),
            "no-check + unresolved-commit jobs get no position check, got {:?}",
            gh.calls(),
        );
    }

    /// Codex 5.1 Medium: if persisting the check id fails after GitHub create,
    /// the position must NOT be recorded as up-to-date — otherwise the debounce
    /// would suppress the only retry that records the `check_run_created` event
    /// the claim-time reporter adopts. So a second tick still retries.
    #[tokio::test]
    async fn coordinator_retries_position_check_when_persist_fails() {
        let tmp = TempDir::new().unwrap();
        let config = config_with(&tmp, PrReport::Check, BaselineReport::None);
        let source = Arc::new(FakeSource::new(pr_job("unused", None)));
        source.set_queued(vec![pr_job("sha", None)]);
        source.fail_persist(); // set_check_run errors
        let gh = Arc::new(FakeGitHub::new());
        let mut coord = position_coord(config, source.clone(), gh.clone());

        coord
            .update_queue_positions()
            .await;
        coord
            .update_queue_positions()
            .await;

        // A failed persist isn't recorded in `last_positions`, so the position
        // wasn't debounced — the second tick attempted the check again.
        assert_eq!(
            create_count(&gh),
            2,
            "a persist failure must leave the position un-debounced so it retries",
        );
    }

    /// Records Slack stream appends (and fallback `chat.update` calls) so the
    /// queue-position test can assert the queued card was edited and debounced.
    #[derive(Default)]
    struct FakePositionSlack {
        updates: StdMutex<Vec<(String, String, String)>>, // (channel, ts, blocks-json)
        appends: StdMutex<Vec<(String, String, String)>>, // (channel, ts, chunks-json)
        stops: StdMutex<Vec<(String, String, String)>>,   // (channel, ts, chunks+blocks-json)
    }

    #[async_trait]
    impl SlackClient for FakePositionSlack {
        async fn post_ephemeral(&self, _c: &str, _u: &str, _t: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn post_blocks_in_thread(
            &self,
            _c: &str,
            _t: &str,
            _b: &serde_json::Value,
            _f: &str,
        ) -> anyhow::Result<String> {
            Ok("ts".into())
        }
        async fn update_blocks(
            &self,
            channel: &str,
            ts: &str,
            blocks: &serde_json::Value,
            _f: &str,
        ) -> anyhow::Result<()> {
            self.updates
                .lock()
                .unwrap()
                .push((channel.into(), ts.into(), blocks.to_string()));
            Ok(())
        }
        async fn append_stream(
            &self,
            channel: &str,
            ts: &str,
            chunks: &[crate::slack::stream::StreamChunk],
        ) -> anyhow::Result<()> {
            self.appends
                .lock()
                .unwrap()
                .push((channel.into(), ts.into(), serde_json::to_string(chunks).unwrap()));
            Ok(())
        }
        async fn stop_stream(
            &self,
            channel: &str,
            ts: &str,
            _markdown_text: Option<&str>,
            chunks: &[crate::slack::stream::StreamChunk],
            blocks: Option<&serde_json::Value>,
        ) -> anyhow::Result<()> {
            let rendered = format!(
                "{} {}",
                serde_json::to_string(chunks).unwrap(),
                blocks
                    .map(serde_json::Value::to_string)
                    .unwrap_or_default(),
            );
            self.stops
                .lock()
                .unwrap()
                .push((channel.into(), ts.into(), rendered));
            Ok(())
        }
        async fn add_reaction(&self, _c: &str, _t: &str, _r: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn remove_reaction(&self, _c: &str, _t: &str, _r: &str) -> anyhow::Result<()> {
            Ok(())
        }
    }

    /// A queued Slack job (pre-claim: empty commit) with an optional
    /// posted-card `plan_message_ts`.
    fn slack_job(plan_message_ts: Option<&str>) -> RunnableJob {
        RunnableJob {
            commit: String::new(),
            progress: ProgressTarget::Slack {
                channel: "C1".into(),
                message_ts: "REQ".into(),
                plan_message_ts: plan_message_ts.map(Into::into),
            },
            ..pr_job("unused", None)
        }
    }

    fn position_coord_with_slack(
        config: DaemonConfig,
        source: Arc<FakeSource>,
        slack: Arc<dyn SlackClient>,
    ) -> Coordinator {
        let config = Arc::new(config);
        let driver: Arc<dyn Driver> =
            Arc::new(LibvirtDriver::new(config.clone(), Arc::new(RecordingShell::new())));
        let deps = JobDeps {
            config,
            jobs: source,
            gh: Arc::new(FakeGitHub::new()),
            driver,
            app_id: Arc::new(OnceCell::new()),
            slack: Some(slack),
            pin_manager: None,
            repeat_planner: None,
            slack_sessions: Default::default(),
        };
        Coordinator::new(deps, 1, CancellationToken::new())
    }

    /// item 0033 (v12): a queued Slack job whose pre-claim stream was posted
    /// (it carries a `plan_message_ts`) gets its Job row updated with a
    /// streamed `task_update`, debounced like the GitHub position check.
    #[tokio::test]
    async fn coordinator_updates_slack_queue_position_and_debounces() {
        let tmp = TempDir::new().unwrap();
        let config = config_with(&tmp, PrReport::Check, BaselineReport::None);
        let source = Arc::new(FakeSource::new(pr_job("unused", None)));
        source.set_queued(vec![slack_job(Some("PLAN_TS"))]);
        let slack = Arc::new(FakePositionSlack::default());
        let mut coord = position_coord_with_slack(config, source.clone(), slack.clone());

        coord
            .update_queue_positions()
            .await;

        {
            let appends = slack.appends.lock().unwrap();
            assert_eq!(appends.len(), 1, "the queued stream was appended once");
            assert_eq!(appends[0].1, "PLAN_TS", "updated the persisted stream ts");
            // in_flight 0 + 1 queued → total 1, ahead 0 → "position 1/1".
            assert!(
                appends[0]
                    .2
                    .contains("position 1/1"),
                "{}",
                appends[0].2
            );
            assert!(
                appends[0]
                    .2
                    .contains("Queued"),
                "Job row queued: {}",
                appends[0].2
            );
            assert!(
                slack
                    .updates
                    .lock()
                    .unwrap()
                    .is_empty(),
                "stream update succeeded, so no block fallback"
            );
        }

        // Second pass, unchanged position → debounced (no new stream append).
        coord
            .update_queue_positions()
            .await;
        assert_eq!(
            slack
                .appends
                .lock()
                .unwrap()
                .len(),
            1,
            "an unchanged position makes no new edit",
        );
    }

    #[tokio::test]
    async fn coordinator_skips_queue_position_for_appended_repeat_slack_runs() {
        let tmp = TempDir::new().unwrap();
        let config = config_with(&tmp, PrReport::Check, BaselineReport::None);
        let source = Arc::new(FakeSource::new(pr_job("unused", None)));
        let mut repeat = slack_job(Some("PLAN_TS"));
        repeat.benchmark_run_index = 1;
        repeat.requested_run_count = 2;
        source.set_queued(vec![repeat]);
        let slack = Arc::new(FakePositionSlack::default());
        let mut coord = position_coord_with_slack(config, source.clone(), slack.clone());

        coord
            .update_queue_positions()
            .await;

        assert!(
            slack
                .appends
                .lock()
                .unwrap()
                .is_empty(),
            "later repeat runs inherit the group card but must not overwrite it with queue \
             position",
        );
        assert!(
            slack
                .updates
                .lock()
                .unwrap()
                .is_empty(),
            "no block fallback either",
        );
    }

    /// Phase 5 CPU pinning: each concurrency slot maps to its configured
    /// `[runner].cpu_sets` cpuset (stable: slot 0 → cpu_sets[0]); an
    /// out-of-range slot or empty config → no pinning.
    #[tokio::test]
    async fn coordinator_maps_slot_to_configured_cpuset() {
        let tmp = TempDir::new().unwrap();
        let mut config = config_with(&tmp, PrReport::Comment, BaselineReport::None);
        config.runner.cpu_sets = vec!["0-1".into(), "2-3".into()];
        config.runner.host_cpus = Some("4-5".into());
        let coord = position_coord(
            config,
            Arc::new(FakeSource::new(pr_job("abc", None))),
            Arc::new(FakeGitHub::new()),
        );

        assert_eq!(
            coord
                .cpuset_for_slot(0)
                .as_deref(),
            Some("0-1"),
            "slot 0 → first cpuset"
        );
        assert_eq!(
            coord
                .cpuset_for_slot(1)
                .as_deref(),
            Some("2-3"),
            "slot 1 → second cpuset"
        );
        assert_eq!(coord.cpuset_for_slot(2), None, "out-of-range slot is unpinned");

        // No cpu_sets → every slot floats.
        let bare_coord = position_coord(
            config_with(&tmp, PrReport::Comment, BaselineReport::None),
            Arc::new(FakeSource::new(pr_job("abc", None))),
            Arc::new(FakeGitHub::new()),
        );
        assert_eq!(bare_coord.cpuset_for_slot(0), None, "no cpu_sets → unpinned");
    }
}
