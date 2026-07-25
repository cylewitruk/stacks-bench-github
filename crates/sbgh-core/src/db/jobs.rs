//! Data layer for the `job` table family.
//!
//! Surface:
//!
//! Lifecycle methods:
//!   - `insert_job` — INSERTs a new row at `queued`; tests pin the queued-state
//!     invariant (both `claim_token` and `claimed_at` are NULL on insert).
//!   - `claim_next_queued` — `FOR UPDATE SKIP LOCKED` against the
//!     `job_queued_idx` partial index; transitions queued → claimed and sets
//!     `(claim_token, claimed_at)`.
//!   - `mark_running` — transitions claimed → running; PRESERVES claim handoff
//!     columns as audit (which daemon instance won the claim).
//!   - `mark_terminal` — transitions running → completed/failed/ cancelled;
//!     preserves audit columns.
//!   - `sweep_stuck_claims` — resets `claimed` rows whose `claimed_at` exceeds
//!     the lease back to `queued` AND clears `claim_token` + `claimed_at`
//!     (matches the inbox sweep).
//!
//! Outcome companions (write-once):
//!   - `insert_event` — append-only timeline; slice 9 writes the queued event
//!     from the processor, slice 10 writes the execution-phase events from the
//!     daemon.
//!   - `record_metric` — promoted bench metrics.
//!   - `record_result` — raw run.json + archive path.
//!
//! Subject relations:
//!   - `link_to_webhook` — webhook→job ingest link.
//!   - `link_to_user` — triggering-user ownership.
//!   - `link_to_pull_request` — PR association.
//!
//! `lookup_job` — slice 9 + slice 10 need this for read paths.

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use uuid::Uuid;

use crate::Result;
use crate::models::{
    BenchmarkGroup, BenchmarkSpec, GithubPullRequestJob, GithubUserJob, GithubWebhookJob, Job,
    JobCreationRequest, JobEvent, JobEventKind, JobEventStatus, JobMetric, JobResult, NewJob,
    NewJobEvent, ResolvedCommit, TerminalJobStatus, measured_run_count,
};

/// Slice 10: atomic "the run completed" write. Bundles the terminal
/// status transition + the write-once outcome companions + the terminal
/// timeline event so they all land in ONE transaction. Without the
/// transaction, a crash between (say) `record_result` and `mark_terminal`
/// would leave a half-finished job that the write-once `job_result` PK
/// then blocks from being re-finished on retry.
///
/// `metric` is `None` when `run.json` was missing/unparseable or didn't
/// carry the full promoted-metric set (the columns are NOT NULL) — the
/// result row + archive path still land so the run is debuggable.
#[derive(Debug, Clone)]
pub struct JobCompletion {
    pub job_id: uuid::Uuid,
    pub claim_token: Uuid,
    pub result: JobResult,
    pub metric: Option<JobMetric>,
    /// v19: shared stacks-bench baseline calibration id to persist on the
    /// benchmark spec in the same transaction as run completion.
    pub baseline_calibration_id: Option<i64>,
    /// `completed` event provenance (forensics summary blob).
    pub event_detail: Option<serde_json::Value>,
}

/// Slice 10: atomic "the run failed" write. Mirrors [`JobCompletion`]
/// but transitions to `failed` and persists an optional forensics
/// `result` (archive path is recorded even on failure so operators can
/// find post-mortem artefacts). `remark` is the short human error stored
/// on the `failed` event.
#[derive(Debug, Clone)]
pub struct JobFailure {
    pub job_id: uuid::Uuid,
    pub claim_token: Uuid,
    pub result: Option<JobResult>,
    pub remark: String,
    pub event_detail: Option<serde_json::Value>,
}

/// Slice 8 (post-review): typed bundle returned by
/// `create_job_with_links` so callers (slice 9) can chain follow-up
/// operations against the freshly-created job + links + queued event
/// without an extra lookup.
#[derive(Debug, Clone)]
pub struct CreatedJob {
    pub job: Job,
    pub webhook_link: GithubWebhookJob,
    pub user_link: Option<GithubUserJob>,
    pub pull_request_link: Option<GithubPullRequestJob>,
    pub queued_event: JobEvent,
}

/// Slice 9: outcome of `create_job_with_links`, distinguishing a fresh
/// creation from an idempotent no-op when the webhook already has a job.
///
/// Job creation is the first non-idempotent side effect in the classify
/// pipeline; the inbox is at-least-once (a webhook can be reprocessed
/// after a failed `complete()` or a swept claim lease). The
/// `github_webhook_job` `UNIQUE (github_webhook_id)` constraint makes
/// "one job per webhook" structural, and this outcome lets the caller
/// treat a retry that hit the constraint as success without minting a
/// duplicate. Mirrors the `IngestOutcome::Duplicate` pattern.
#[derive(Debug, Clone)]
pub enum JobCreationOutcome {
    /// A new job (+ links + queued event) was created this call.
    /// Boxed because `CreatedJob` dwarfs the unit `AlreadyEnqueued`
    /// variant (clippy `large_enum_variant`).
    Created(Box<CreatedJob>),
    /// A job already existed for this webhook (idempotent retry). No
    /// rows were written; the prior attempt's job stands.
    AlreadyEnqueued,
}

/// roadmap-v7: how the baseline anchor was chosen for a comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BaselineSelection {
    /// A completed baseline existed at the exact merge-base (fork-point) SHA.
    Exact,
    /// No exact baseline; the newest completed baseline on the target branch
    /// at/just-before the fork-point (by `git_committed_at`). Best-effort.
    NearestBefore,
}

/// roadmap-v7: provenance of the baseline a PR run was compared against — what
/// the report names + links to. Carries `github_repo_id` (NOT owner/name): the
/// renderer resolves the repo for the GitHub link, keeping this query free of a
/// `github_repo` join (and the in-memory impl free of repo knowledge).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaselineAnchor {
    pub job_id: Uuid,
    pub github_repo_id: i64,
    pub commit: String,
    pub git_ref_display: String,
    pub committed_at: Option<DateTime<Utc>>,
    pub selection: BaselineSelection,
}

/// roadmap-v7: the baseline match for a comparison — the measured metric plus
/// its [`BaselineAnchor`] provenance. (No `PartialEq` — `JobMetric` carries
/// none; assert on `anchor` + the metric fields you care about.)
#[derive(Debug, Clone)]
pub struct BaselineMatch {
    pub metric: JobMetric,
    pub anchor: BaselineAnchor,
}

/// v15 Phase 4: a completed repeat run whose next run still needs to be
/// materialized. The runner uses this to carry the group SQLite artifact
/// before calling [`JobStore::append_next_benchmark_run`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PendingBenchmarkRun {
    pub completed_job_id: Uuid,
    pub benchmark_group_id: Uuid,
    pub benchmark_spec_id: Uuid,
    pub benchmark_run_index: i32,
    pub requested_run_count: i32,
    pub artifact_prefix: String,
}

/// Promoted metrics for one isolated run inside a benchmark spec.
#[derive(Debug, Clone)]
pub struct BenchmarkRunMetric {
    pub benchmark_run_index: i32,
    pub metric: JobMetric,
}

/// One requested benchmark spec inside a newly-created benchmark group.
///
/// The store API is deliberately N-shaped: v22 caps comparison requests at two
/// variants at validation time, but the persistence layer accepts an ordered
/// list so future cap lifts do not require a schema/API retrofit.
#[derive(Debug, Clone)]
pub struct NewBenchmarkSpec {
    pub new_job: NewJob,
    pub requested_run_count: i32,
    pub baseline_calibration_id: Option<i64>,
}

impl NewBenchmarkSpec {
    pub fn singleton(new_job: NewJob, requested_run_count: i32) -> Self {
        Self {
            new_job,
            requested_run_count,
            baseline_calibration_id: None,
        }
    }

    pub fn measured_run_count(&self) -> i32 {
        measured_run_count(self.new_job.axes.task_kind, self.requested_run_count)
    }
}

#[async_trait]
pub trait JobStore: Send + Sync + 'static {
    async fn insert_job(&self, new: &NewJob) -> Result<Job>;

    /// Slice 8 (post-review): atomic job-creation boundary. Inserts the
    /// `job` row + webhook→job link + (optional) user → job + (optional)
    /// PR → job + the queued `job_event` in one transaction. A FK or
    /// UNIQUE failure on any of the link inserts ROLLs BACK the entire
    /// creation — no partial job rows.
    ///
    /// This is the path slice 9 wires from the processor. Slice 8
    /// keeps the individual `insert_job` / `link_to_*` / `insert_event`
    /// methods on the trait for read-side flexibility and stand-alone
    /// integration testing, but production writers MUST use
    /// `create_job_with_links` to honour the transactional invariant.
    ///
    /// Slice 9: idempotent on `github_webhook_id`. If a job already
    /// exists for this webhook (a reprocessed delivery after a failed
    /// `complete()` / swept lease), no rows are written and
    /// `JobCreationOutcome::AlreadyEnqueued` is returned — the
    /// `UNIQUE (github_webhook_id)` constraint makes this race-safe even
    /// against a concurrent re-claim.
    async fn create_job_with_links(
        &self,
        request: &JobCreationRequest,
    ) -> Result<JobCreationOutcome>;

    /// Create a job with **no GitHub webhook/PR/owner links** — a non-webhook
    /// trigger (Slack ad-hoc, or daemon `cache_warm` warming). Inserts the
    /// `job` row (under the caller-provided `job_id`) + its queued `job_event`
    /// — and, when `plan_message_ts` is `Some`, the `plan_message_sent` event —
    /// all in one transaction, returning the inserted job.
    ///
    /// The Slack connector posts its plan card before creating the job (the
    /// card `ts` needs the id), so it passes that id + the card's `ts` here;
    /// writing `plan_message_sent` in the same transaction means the job is
    /// never claimable without its plan `ts`. Warming passes a generated id +
    /// `None`.
    ///
    /// Deliberately **separate** from
    /// [`create_job_with_links`](Self::create_job_with_links) so the GitHub
    /// webhook path is untouched: there's no webhook link and **no
    /// webhook-keyed idempotency**. These triggers have no at-least-once inbox
    /// behind them — a Slack envelope is acked before enqueuing, and warming
    /// dedups itself against in-flight builds — making this a plain create.
    async fn create_unlinked_job(
        &self,
        job_id: Uuid,
        new_job: &NewJob,
        queued_event_detail: &serde_json::Value,
        plan_message_ts: Option<&str>,
    ) -> Result<Job>;

    /// Create a webhook-less benchmark group with an ordered set of benchmark
    /// specs and enqueue only the first spec's run 0.
    ///
    /// This is the comparison-group sibling of
    /// [`create_unlinked_job`](Self::create_unlinked_job). It has the same
    /// Slack-card atomicity guarantee (queued event + optional
    /// `plan_message_sent` in the creation transaction), but persists multiple
    /// `benchmark_spec` rows up front. Later runs/specs are materialized by
    /// [`append_next_benchmark_run`](Self::append_next_benchmark_run) so the
    /// group preserves the "at most one active run" scheduling invariant.
    async fn create_unlinked_benchmark_group(
        &self,
        first_job_id: Uuid,
        specs: &[NewBenchmarkSpec],
        queued_event_detail: &serde_json::Value,
        plan_message_ts: Option<&str>,
    ) -> Result<Job>;

    async fn lookup_job(&self, job_id: Uuid) -> Result<Option<Job>>;

    async fn lookup_benchmark_group(&self, group_id: Uuid) -> Result<Option<BenchmarkGroup>>;

    async fn lookup_benchmark_spec(&self, spec_id: Uuid) -> Result<Option<BenchmarkSpec>>;

    async fn lookup_benchmark_specs(&self, group_id: Uuid) -> Result<Vec<BenchmarkSpec>>;

    /// The terminal completed event detail for `job_id`, if the run completed.
    /// Failed/cancelled runs intentionally return `None`, which stops a repeat
    /// chain loudly via terminal state rather than carrying stale artifacts.
    async fn completed_event_detail(&self, job_id: Uuid) -> Result<Option<serde_json::Value>>;

    /// `FOR UPDATE SKIP LOCKED` on the queued partial index; transitions
    /// queued → claimed and stamps `(claim_token, claimed_at)`. Returns
    /// `Ok(None)` when the queue is empty.
    async fn claim_next_queued(&self, claim_token: Uuid) -> Result<Option<Job>>;

    /// claimed → running. Conditional on `(job_id, claim_token)` so a
    /// late writer whose lease was reclaimed by the sweep can't
    /// transition the row. Returns `Ok(false)` if the conditional
    /// match failed.
    ///
    /// Slice 8 (post-review): if the queue-time `git_commit_hash` was
    /// unresolved (branch tip at enqueue), the daemon resolves
    /// during the claim phase and passes `Some(ResolvedCommit)` here
    /// so the commit metadata + status transition land atomically
    /// under the same claim_token guard. Pass `None` if the commit was
    /// already concrete at enqueue time (push/tag triggers).
    async fn mark_running(
        &self,
        job_id: Uuid,
        claim_token: Uuid,
        resolved_commit: Option<ResolvedCommit>,
    ) -> Result<bool>;

    /// running → terminal_status. Conditional on `(job_id, claim_token)`
    /// (same stale-claim guard as `mark_running`). The narrowed
    /// `TerminalJobStatus` type prevents the post-review high
    /// finding where a caller bug could transition `running → queued`
    /// and violate the queued-state invariant.
    async fn mark_terminal(
        &self,
        job_id: Uuid,
        claim_token: Uuid,
        terminal_status: TerminalJobStatus,
    ) -> Result<bool>;

    /// Reset `claimed` rows whose `claimed_at` is older than NOW - lease
    /// back to `queued` and CLEAR `claim_token` + `claimed_at` (matches
    /// the inbox sweep's `claim_token` reset). Returns the recovered
    /// row count.
    async fn sweep_stuck_claims(&self, lease: Duration) -> Result<u64>;

    /// Slice 10: atomic run-completion. In ONE transaction: transition
    /// `running → completed` (guarded by `(job_id, claim_token,
    /// status='running')`, same stale-claim guard as `mark_terminal`),
    /// insert `job_result`, optionally insert `job_metric`, and append the
    /// `completed` `job_event`. Returns `Ok(false)` if the guard didn't
    /// match (a stale claim raced the sweep) — nothing is written.
    async fn complete_job(&self, completion: &JobCompletion) -> Result<bool>;

    /// Slice 10: atomic run-failure. In ONE transaction: transition
    /// `running → failed` (same guard), optionally insert a forensics
    /// `job_result`, and append the `failed` `job_event`. Returns
    /// `Ok(false)` on a stale-claim guard miss.
    async fn fail_job(&self, failure: &JobFailure) -> Result<bool>;

    /// roadmap-v5 Phase 4C: atomic **cancellation** — a deliberately-stopped
    /// run (operator shutdown/abort), distinct from a failure. Guarded by
    /// `(job_id, claim_token, status IN ('claimed','running'))` like `fail_job`
    /// (an abort can land before *or* after the run started); transitions to
    /// `cancelled` and appends a `cancelled` `job_event` with `remark`. No
    /// forensics `job_result` (a cancelled run produced none). Returns
    /// `Ok(false)` on a stale-claim guard miss.
    async fn cancel_job(&self, job_id: Uuid, claim_token: Uuid, remark: &str) -> Result<bool>;

    /// Orphan recovery (roadmap-v5 Phase 4B-2): every job id currently in
    /// `running`. After a crash/restart these are necessarily orphans — a
    /// freshly-started daemon has no running jobs of its own — so the runner
    /// cleans each one's leaked VM and terminal-cancels the row at startup.
    async fn running_job_ids(&self) -> Result<Vec<Uuid>>;

    /// roadmap-v5 Phase 5: all `queued` jobs in **claim order** (the same
    /// `ORDER BY created_at, id` `claim_next_queued` picks from), so the
    /// coordinator can report each waiting job its queue position. Read-only.
    async fn queued_jobs_ordered(&self) -> Result<Vec<Job>>;

    /// roadmap-v5 Phase 5 (dedup): the id of an **active** (`queued` /
    /// `claimed` / `running`) job for this exact `(github_repo_id,
    /// git_commit_hash, source, workload_key)`, if any. Used to skip
    /// enqueuing a duplicate `/benchmark` for a commit already being
    /// benchmarked — two jobs on one head SHA would otherwise fight over
    /// the single GitHub check GitHub surfaces per `(name, head_sha)`.
    ///
    /// roadmap-v7: the match is **workload-aware**. A different `workload_key`
    /// (e.g. `/benchmark run --count 1` while a default `/benchmark` is active)
    /// is a genuinely different benchmark and must NOT be deduped — so it is
    /// not matched here, and the caller enqueues it.
    ///
    /// This is a **best-effort** read used for a check-then-insert; it is not a
    /// hard uniqueness guarantee against concurrent processors or a crash
    /// between the check and the inbox completing the webhook (see the caller
    /// in `webhook_processor`). A partial unique index on the active set is
    /// the structural hardening if that's ever needed.
    async fn find_active_job(
        &self,
        github_repo_id: i64,
        commit: &str,
        source: crate::models::JobSource,
        workload_key: &str,
    ) -> Result<Option<Uuid>>;

    /// v11 (item 0031): is there a **build-only** job for `(github_repo_id,
    /// commit, build_target)` that should **block a new warm enqueue** — either
    /// active (queued/claimed/running) OR a **recent terminal failure**
    /// (failed/cancelled with `updated_at >= failed_since`)?
    ///
    /// Pin warming uses it to dedup: an active build avoids a duplicate, and
    /// the recent-failure window is the **warm-retry cooldown** — a build
    /// that fails and leaves the cache cold becomes terminal, so an
    /// active-only check would let the after-each-job recompute re-enqueue
    /// it forever (an endless one-at-a-time retry loop). `completed` builds
    /// are NOT matched: a success caches the binary (the cache check then
    /// skips it), and if that entry is later evicted, re-warming is
    /// correct.
    ///
    /// Deliberately **separate** from
    /// [`find_active_job`](Self::find_active_job) — a build job carries no
    /// `workload_key` (measurement-specific), so it keys on `task_kind =
    /// build_only` + `build_target`. Best-effort check-then-insert, same
    /// caveat as `find_active_job`.
    async fn recent_build_attempt(
        &self,
        github_repo_id: i64,
        commit: &str,
        build_target: crate::models::BuildTarget,
        failed_since: DateTime<Utc>,
    ) -> Result<Option<Uuid>>;

    /// roadmap-v7: find the baseline a PR run should be compared against.
    /// **Repo-agnostic** — a measurement is a property of the commit, not the
    /// repo that ran it — so a fork PR finds an upstream baseline at the same
    /// SHA. Two-step:
    ///
    /// 1. **Exact** — a completed baseline at `merge_base_sha` for the same
    ///    `workload_key` (newest measurement wins).
    /// 2. **Nearest-before** (only if no exact hit AND
    ///    `merge_base_committed_at` is `Some`) — the newest completed baseline
    ///    on `base_ref` for the same `workload_key` with `git_committed_at <=`
    ///    the fork-point. Best-effort, labelled as such.
    ///
    /// Only `intent='baseline_benchmark'`, `status='completed'`,
    /// matching-`workload_key` rows are eligible; a NULL `workload_key`
    /// never matches. `None` when neither step finds one. Ties (a commit
    /// benchmarked more than once, or two commits sharing a timestamp) resolve
    /// **deterministically** to the freshest measurement, then the highest job
    /// id — so a report never silently flips which baseline it cites.
    async fn find_baseline_for(
        &self,
        merge_base_sha: &str,
        base_ref: &str,
        merge_base_committed_at: Option<DateTime<Utc>>,
        workload_key: &str,
    ) -> Result<Option<BaselineMatch>>;

    /// Orphan recovery (4B-2 + 4C): terminal-**cancel** a job stranded in
    /// `running` by a dead daemon, with NO claim-token guard. The daemon that
    /// claimed it is gone, and this runs at startup before the coordinator
    /// claims anything, so there's no live writer to race — an unconditional
    /// `running → cancelled` (plus a `cancelled` event carrying `remark`) is
    /// safe. A crash-orphan is re-triggerable, not a benchmark failure, so it's
    /// `cancelled` not `failed`. Returns `Ok(false)` if the row wasn't
    /// `running` (already recovered / gone), making a re-run after a
    /// mid-recovery crash idempotent.
    async fn cancel_orphan(&self, job_id: Uuid, remark: &str) -> Result<bool>;

    /// Slice 10: read the `queued` timeline event for a job (carries the
    /// trigger provenance + `bench_args` the daemon forwards to
    /// the run). `None` if the job has no queued event (shouldn't happen
    /// for jobs created via `create_job_with_links`).
    async fn queued_event(&self, job_id: Uuid) -> Result<Option<JobEvent>>;

    /// Slice 11: the PR-subject link for a job, if any. `pr_comment`
    /// jobs have one (so the daemon can post the PR comment);
    /// baseline jobs don't. Carries `github_pull_request_id` — resolve
    /// the PR number via `PullRequestStore::lookup_by_id`.
    async fn pull_request_link(&self, job_id: Uuid) -> Result<Option<GithubPullRequestJob>>;

    /// Slice 11: the most-recent GitHub comment id recorded on the job's
    /// timeline (a `comment_posted` / `comment_updated` event with a
    /// non-null `github_comment_id`). The daemon reads this on
    /// (re-)claim so a job reclaimed after the comment was already posted
    /// EDITS the existing comment instead of posting a duplicate.
    async fn latest_comment_id(&self, job_id: Uuid) -> Result<Option<i64>>;

    /// roadmap-v4: the most-recent Check Run recorded on the job's timeline
    /// (a `check_run_created` event), as `(check_run_id, html_url)`. Read on
    /// (re-)claim so a reclaimed job UPDATEs the existing check (and can
    /// rebuild the "started, see check" comment from the stored URL) rather
    /// than creating a duplicate.
    async fn latest_check_run(&self, job_id: Uuid) -> Result<Option<(i64, Option<String>)>>;

    /// item 0002: the most-recent Slack `plan` message `ts` recorded on the
    /// job's timeline (a `plan_message_sent` event, from
    /// `detail->>'plan_message_ts'`). Read on (re-)claim so a reclaimed Slack
    /// job `chat.update`s the existing live-timeline card instead of posting a
    /// duplicate.
    async fn latest_plan_message_ts(&self, job_id: Uuid) -> Result<Option<String>>;

    /// Record a Slack `plan` message `ts` on the job's timeline (a
    /// `plan_message_sent` event) — the writer behind
    /// [`latest_plan_message_ts`]. Keyed by `job_id` so the **pre-claim**
    /// Slack connector (item `0023`, which holds a core [`Job`], not a
    /// `RunnableJob`) can persist the queued card's identity; the daemon's
    /// claimed-world `RunnableJobStore::set_plan_message_ts`
    /// delegates here, so the event shape lives in one place. The default impl
    /// rides [`insert_event`](Self::insert_event); a failure is non-fatal at
    /// the call site, like the comment/check identity writes.
    ///
    /// [`latest_plan_message_ts`]: Self::latest_plan_message_ts
    async fn record_plan_message_ts(&self, job_id: Uuid, message_ts: &str) -> Result<()> {
        self.insert_event(&NewJobEvent {
            job_id,
            event_kind: JobEventKind::PlanMessageSent,
            event_status: JobEventStatus::Success,
            github_comment_id: None,
            github_check_run_id: None,
            github_check_run_url: None,
            remark: None,
            detail: Some(serde_json::json!({ "plan_message_ts": message_ts })),
        })
        .await?;
        Ok(())
    }

    /// v15 Phase 2: append the next queued run for the same benchmark spec
    /// after `completed_job_id` finishes. Returns `Ok(None)` when the job is
    /// not the latest completed run, the requested run count is already
    /// satisfied, the spec is build-only, or another run for the spec is
    /// already queued/claimed/running.
    ///
    /// This is the durable lazy-enqueue primitive: the next row is an ordinary
    /// `job` with `benchmark_run_index = previous_max + 1`, copied subject
    /// identity, and copied queued provenance. Callers can run it after a
    /// completion hook; startup recovery uses [`resume_pending_benchmark_runs`]
    /// to derive the same work from DB state after a restart.
    async fn append_next_benchmark_run(&self, completed_job_id: Uuid) -> Result<Option<Job>>;

    /// v15 Phase 2: DB-resumable lazy-enqueue recovery. Finds benchmark specs
    /// whose latest run completed, whose requested count is not yet satisfied,
    /// and that have no active run, then appends one next run per spec.
    async fn resume_pending_benchmark_runs(&self) -> Result<Vec<Job>>;

    /// v15 Phase 4: completed repeat runs ready for carry-forward + append.
    /// This is the read half of [`resume_pending_benchmark_runs`], split out
    /// so daemon startup can promote the archived SQLite DB before appending
    /// the next run.
    async fn pending_completed_benchmark_runs(&self) -> Result<Vec<PendingBenchmarkRun>>;

    /// v15 Phase 5: promoted metrics for all completed runs in one spec,
    /// ordered by isolated run index. Missing/unparseable runs are absent
    /// because they never produced a `job_metric` row.
    async fn benchmark_run_metrics(
        &self,
        benchmark_spec_id: Uuid,
    ) -> Result<Vec<BenchmarkRunMetric>>;

    async fn insert_event(&self, new: &NewJobEvent) -> Result<JobEvent>;

    async fn record_metric(&self, metric: &JobMetric) -> Result<()>;

    async fn record_result(&self, result: &JobResult) -> Result<()>;

    async fn link_to_webhook(&self, webhook_id: i64, job_id: Uuid) -> Result<GithubWebhookJob>;

    async fn link_to_user(&self, user_id: i64, job_id: Uuid) -> Result<GithubUserJob>;

    async fn link_to_pull_request(
        &self,
        pull_request_id: i64,
        job_id: Uuid,
        triggering_comment_id: Option<i64>,
    ) -> Result<GithubPullRequestJob>;
}
