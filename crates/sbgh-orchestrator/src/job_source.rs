//! Slice 10: the orchestrator's queue abstraction.
//!
//! The runner used to be hard-wired to the legacy `jobs` table via
//! [`sbgh_core::db::JobStore`]. Phase 2 introduces a second persistence
//! shape — the new `job` family (slice 8) assembled across
//! subject/relation/event tables. To run the same libvirt/runner logic
//! against EITHER backend, this module introduces:
//!
//!   - [`RunnableJob`] — a backend-neutral execution view (everything the
//!     driver + progress reporter need), assembled from whichever schema.
//!   - [`RunnableJobStore`] — claim + lifecycle, implemented by [`JobV2Source`]
//!     (over `JobV2Store`, the production default since the slice 11 cutover)
//!     and [`LegacyJobSource`] (over `JobStore`, the retained escape hatch
//!     until slice 12).
//!
//! Slice 10 scope (per the Phase-2 plan): prove the new backend can
//! claim → run → finish and persist the `queued` (slice 9) + terminal
//! (`completed`/`failed`) `job_event` rows plus `job_metric` /
//! `job_result`.
//!
//! Slice 11 (cutover) made `v2` the production backend and added
//! PR-comment posting: `pr_comment` jobs post/edit a PR comment (the
//! comment id is recorded on a `comment_posted` `job_event`, read back
//! on re-claim). Baseline jobs (`branch_push`/`tag_created`) have no PR
//! and stay [`ProgressTarget::LogOnly`]. `tag_created` jobs are enqueued
//! with no commit and resolved to their commit at claim time (the runner
//! calls [`sbgh_core::github::GitHubApi::resolve_commit`] in preflight).
//! STILL deferred: the intermediate phase-event timeline
//! (provision/build/bench `job_event` rows — phase changes are logged
//! only).

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use sbgh_core::db::{JobCompletion, JobFailure, JobStore, JobV2Store, PullRequestStore, RepoStore};
use sbgh_core::models::{
    GitRefKind, JobEventKind, JobEventStatus, JobMetric, JobResult, NewJobEvent, QueuedEventDetail,
    ResolvedCommit,
};
use uuid::Uuid;

use crate::bench_summary::RunResult;

/// Where a job's lifecycle progress should be surfaced.
#[derive(Debug, Clone)]
pub enum ProgressTarget {
    /// Edit a PR comment (legacy jobs always; new-schema `pr_comment`
    /// jobs). `comment_id` is `None` until the orchestrator posts the
    /// initial comment.
    PullRequestComment { pr_number: i64, comment_id: Option<i64> },
    /// Phase progress goes to LOGS ONLY — new-schema baseline jobs
    /// (`branch_push`/`tag_created`) have no PR. STILL deferred for these
    /// (slice 11 left them): intermediate phase `job_event` rows (phase
    /// changes are logged only). The `queued` + terminal
    /// (`completed`/`failed`) events ARE persisted (slice 9's processor
    /// and `complete_job`/`fail_job`). The loud variant name keeps the
    /// missing comment deliberate (vs. an accidental bug).
    LogOnly,
}

/// Backend-neutral execution context for one benchmark run. Assembled by
/// a [`RunnableJobStore`] from either the legacy `jobs` row or the new
/// `job` family.
#[derive(Debug, Clone)]
pub struct RunnableJob {
    pub id: Uuid,
    /// `owner/name`. Drives the git clone URL + (legacy) GitHub API calls.
    pub repository: String,
    /// Resolved commit/SHA to benchmark. Empty when unresolved at claim
    /// time, which the runner resolves in preflight: legacy + new
    /// `pr_comment` via the PR head SHA, `tag_created` via the tag ref.
    /// New `branch_push` carries its commit from enqueue.
    pub commit: String,
    /// Human-readable ref label (PR head branch / watched branch / tag),
    /// for logs + new-schema progress. Legacy jobs use a `PR #N` label.
    pub git_ref_display: String,
    /// What kind of ref `git_ref_display` is. Drives commit resolution
    /// when `commit` is empty: a `Tag` resolves via
    /// `GitHubApi::resolve_commit`. Legacy jobs are always `Branch` (a PR
    /// head).
    pub git_ref_kind: GitRefKind,
    pub installation_id: i64,
    /// Resolved `stacks-bench` CLI args. Empty → the driver falls back to
    /// the configured `default_args`.
    pub bench_args: Vec<String>,
    pub progress: ProgressTarget,
    /// New-schema claim token (`None` for legacy). The new adapter guards
    /// its `running`/terminal writes on `(id, claim_token)` so a write
    /// that lost its lease to the stuck-claim sweep is a no-op.
    pub claim_token: Option<Uuid>,
}

/// Claim + lifecycle over a job queue. Both the legacy and new-schema
/// backends implement this so the runner is backend-agnostic.
#[async_trait]
pub trait RunnableJobStore: Send + Sync + 'static {
    /// Claim the next runnable job. Legacy transitions `queued → running`
    /// atomically; the new schema transitions `queued → claimed` (the
    /// `claimed → running` step happens in [`Self::start_running`] once
    /// execution actually begins).
    async fn claim_next(&self) -> anyhow::Result<Option<RunnableJob>>;

    /// Mark the job as actually running, persisting a commit resolved
    /// during preflight (if any). Legacy: writes `head_sha` (the row is
    /// already `running`). New: `claimed → running` carrying the resolved
    /// commit under the claim-token guard. A `ResolvedCommit` with
    /// `committed_at: None` (the PR-head-resolve case yields only a SHA)
    /// leaves the existing `git_committed_at` untouched.
    async fn start_running(
        &self,
        job: &RunnableJob,
        resolved_commit: Option<ResolvedCommit>,
    ) -> anyhow::Result<()>;

    /// Recover jobs stranded mid-claim. The new schema can leave a row in
    /// `claimed` if the orchestrator crashes (or preflight errors) after
    /// `claim_next` but before `start_running`; this resets `claimed`
    /// rows older than `lease` back to `queued`. Legacy has no `claimed`
    /// state (claim is atomic `queued → running`), so it is a no-op.
    /// Returns the number of rows recovered.
    async fn sweep_stuck_claims(&self, lease: chrono::Duration) -> anyhow::Result<u64>;

    /// Terminal success. `summary` is the orchestrator forensics blob
    /// (archive paths, console tail, finish reason).
    async fn complete(&self, job: &RunnableJob, summary: &serde_json::Value) -> anyhow::Result<()>;

    /// Terminal failure. `summary` is the same forensics blob shape when
    /// available (setup-time failures may have none).
    async fn fail(
        &self,
        job: &RunnableJob,
        error: &str,
        summary: Option<&serde_json::Value>,
    ) -> anyhow::Result<()>;

    /// Persist the GitHub comment id the orchestrator just posted. Only
    /// called for [`ProgressTarget::PullRequestComment`] jobs.
    async fn set_comment_id(&self, job: &RunnableJob, comment_id: i64) -> anyhow::Result<()>;
}

// ─── Legacy adapter (escape hatch until slice 12) ──────────────────────

/// [`RunnableJobStore`] over the legacy `jobs` table. Thin mapping onto
/// the existing [`JobStore`]; preserves the pre-cutover behaviour (every
/// legacy job is a PR-comment job). Selectable via `[jobs].source =
/// "legacy"` until slice 12 removes the legacy path.
pub struct LegacyJobSource {
    jobs: Arc<dyn JobStore>,
}

impl LegacyJobSource {
    pub fn new(jobs: Arc<dyn JobStore>) -> Self {
        Self { jobs }
    }
}

#[async_trait]
impl RunnableJobStore for LegacyJobSource {
    async fn claim_next(&self) -> anyhow::Result<Option<RunnableJob>> {
        let Some(job) = self.jobs.claim_next().await? else {
            return Ok(None);
        };
        Ok(Some(RunnableJob {
            id: job.id,
            repository: job.repository,
            commit: job.head_sha,
            git_ref_display: format!("PR #{}", job.pr_number),
            // Legacy jobs are always a PR head (a branch).
            git_ref_kind: GitRefKind::Branch,
            installation_id: job.installation_id,
            bench_args: legacy_bench_args(&job.args.0),
            progress: ProgressTarget::PullRequestComment {
                pr_number: job.pr_number,
                comment_id: job.comment_id,
            },
            claim_token: None,
        }))
    }

    async fn start_running(
        &self,
        job: &RunnableJob,
        resolved_commit: Option<ResolvedCommit>,
    ) -> anyhow::Result<()> {
        // Legacy is already `running` from `claim_next`; the only
        // persistence here is the head SHA the runner resolved. Legacy
        // stores no commit timestamp, so `committed_at` is irrelevant.
        if let Some(rc) = resolved_commit {
            self.jobs
                .set_head_sha(job.id, &rc.hash)
                .await?;
        }
        Ok(())
    }

    async fn sweep_stuck_claims(&self, _lease: chrono::Duration) -> anyhow::Result<u64> {
        // Legacy `claim_next` is an atomic `queued → running`; there is
        // no intermediate `claimed` state to recover.
        Ok(0)
    }

    async fn complete(&self, job: &RunnableJob, summary: &serde_json::Value) -> anyhow::Result<()> {
        self.jobs
            .complete(job.id, summary.clone())
            .await?;
        Ok(())
    }

    async fn fail(
        &self,
        job: &RunnableJob,
        error: &str,
        summary: Option<&serde_json::Value>,
    ) -> anyhow::Result<()> {
        self.jobs
            .fail(job.id, error, summary.cloned())
            .await?;
        Ok(())
    }

    async fn set_comment_id(&self, job: &RunnableJob, comment_id: i64) -> anyhow::Result<()> {
        self.jobs
            .set_comment_id(job.id, comment_id)
            .await?;
        Ok(())
    }
}

/// Pull the `{"args": [...]}` array off a legacy job's `args` blob into a
/// flat `Vec<String>`. Non-string entries are dropped; a missing/empty
/// array yields `vec![]` (driver falls back to `default_args`).
fn legacy_bench_args(args: &serde_json::Value) -> Vec<String> {
    args["args"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str())
                .map(String::from)
                .collect()
        })
        .unwrap_or_default()
}

// ─── New-schema adapter ────────────────────────────────────────────────

/// [`RunnableJobStore`] over the new `job` family. Composes
/// [`JobV2Store`] (claim + lifecycle) with [`RepoStore`] (resolve
/// `github_repo_id → owner/name`) and [`PullRequestStore`] (resolve a
/// `pr_comment` job's PR number for comment posting), and reads the
/// queued `job_event` for the run's `bench_args`.
///
/// Slice 11 made this the production backend (`[jobs].source = "v2"`):
/// `pr_comment` jobs post/edit a PR comment (the comment id is recorded
/// as a `comment_posted` `job_event`, read back on re-claim for
/// idempotency); baseline jobs (`branch_push`/`tag_created`) have no PR
/// and stay [`ProgressTarget::LogOnly`].
pub struct JobV2Source {
    jobs: Arc<dyn JobV2Store>,
    repos: Arc<dyn RepoStore>,
    pull_requests: Arc<dyn PullRequestStore>,
}

impl JobV2Source {
    pub fn new(
        jobs: Arc<dyn JobV2Store>,
        repos: Arc<dyn RepoStore>,
        pull_requests: Arc<dyn PullRequestStore>,
    ) -> Self {
        Self { jobs, repos, pull_requests }
    }
}

#[async_trait]
impl RunnableJobStore for JobV2Source {
    async fn claim_next(&self) -> anyhow::Result<Option<RunnableJob>> {
        let claim_token = Uuid::new_v4();
        let Some(job) = self
            .jobs
            .claim_next_queued(claim_token)
            .await?
        else {
            return Ok(None);
        };

        // Resolve owner/name. The job's composite FK guarantees the repo
        // row exists, so a cache miss here is a real inconsistency.
        let repo = self
            .repos
            .lookup_repo(job.github_repo_id)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "job {} references unknown github_repo {}",
                    job.id,
                    job.github_repo_id
                )
            })?;

        // bench_args live in the queued event's provenance detail.
        let queued = self
            .jobs
            .queued_event(job.id)
            .await?;
        let bench_args = queued
            .as_ref()
            .and_then(|e| e.detail.as_ref())
            .map(|d| bench_args_from_detail(&d.0))
            .unwrap_or_default();

        // PR-linked jobs (`pr_comment`) report progress on the PR
        // comment; baseline jobs (`branch_push`/`tag_created`) have no PR
        // → log-only. On (re-)claim, an already-posted comment id is read
        // back so a reclaimed job edits rather than double-posts.
        let progress = match self
            .jobs
            .pull_request_link(job.id)
            .await?
        {
            Some(link) => {
                let pr = self
                    .pull_requests
                    .lookup_by_id(link.github_pull_request_id)
                    .await?
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "job {} links unknown github_pull_request {}",
                            job.id,
                            link.github_pull_request_id
                        )
                    })?;
                let comment_id = self
                    .jobs
                    .latest_comment_id(job.id)
                    .await?;
                ProgressTarget::PullRequestComment {
                    pr_number: pr.pr_number as i64,
                    comment_id,
                }
            }
            None => ProgressTarget::LogOnly,
        };

        Ok(Some(RunnableJob {
            id: job.id,
            repository: format!("{}/{}", repo.owner, repo.name),
            commit: job
                .git_commit_hash
                .unwrap_or_default(),
            git_ref_display: job.git_ref_display,
            git_ref_kind: job.git_ref_kind,
            installation_id: job.github_installation_id,
            bench_args,
            progress,
            claim_token: Some(claim_token),
        }))
    }

    async fn start_running(
        &self,
        job: &RunnableJob,
        resolved_commit: Option<ResolvedCommit>,
    ) -> anyhow::Result<()> {
        let claim_token = self.expect_token(job)?;
        let ok = self
            .jobs
            .mark_running(job.id, claim_token, resolved_commit)
            .await?;
        if !ok {
            anyhow::bail!("mark_running for job {} was a no-op (stale claim?)", job.id);
        }
        Ok(())
    }

    async fn sweep_stuck_claims(&self, lease: chrono::Duration) -> anyhow::Result<u64> {
        Ok(self
            .jobs
            .sweep_stuck_claims(lease)
            .await?)
    }

    async fn complete(&self, job: &RunnableJob, summary: &serde_json::Value) -> anyhow::Result<()> {
        let claim_token = self.expect_token(job)?;
        let (result, metric) = extract_outcome(job.id, summary);
        let ok = self
            .jobs
            .complete_job(&JobCompletion {
                job_id: job.id,
                claim_token,
                result,
                metric,
                event_detail: Some(summary.clone()),
            })
            .await?;
        if !ok {
            anyhow::bail!("complete_job for job {} was a no-op (stale claim?)", job.id);
        }
        Ok(())
    }

    async fn fail(
        &self,
        job: &RunnableJob,
        error: &str,
        summary: Option<&serde_json::Value>,
    ) -> anyhow::Result<()> {
        let claim_token = self.expect_token(job)?;
        // Record a forensics result row when the summary carries an
        // archive dir (a VM-side failure still produced artefacts);
        // a setup-time failure (no summary) records the event only.
        let result = summary.and_then(|s| {
            archive_dir(s).map(|dir| JobResult {
                job_id: job.id,
                run_json: None,
                archive_dir: dir,
                created_at: Utc::now(),
            })
        });
        let ok = self
            .jobs
            .fail_job(&JobFailure {
                job_id: job.id,
                claim_token,
                result,
                remark: error.to_string(),
                event_detail: summary.cloned(),
            })
            .await?;
        if !ok {
            // Guard miss: the job is neither `claimed` nor `running`
            // under our token — the sweep already reclaimed our lease (so
            // it's back to `queued` or held by another claim). Don't
            // clobber that; the sweep/next claim handles it. (`fail_job`
            // DOES terminalize a `claimed` job under our token, so a
            // pre-`start_running` preflight failure does NOT reach here —
            // it terminalizes cleanly rather than looping.)
            tracing::warn!(
                job_id = %job.id,
                "fail_job was a no-op (lost our claim to the sweep); leaving for re-claim"
            );
        }
        Ok(())
    }

    async fn set_comment_id(&self, job: &RunnableJob, comment_id: i64) -> anyhow::Result<()> {
        // Slice 11: the new schema has no `comment_id` column — the
        // comment identity lives on a `comment_posted` timeline event.
        // `latest_comment_id` reads it back on (re-)claim so a reclaimed
        // job edits the existing comment instead of posting a duplicate.
        self.jobs
            .insert_event(&NewJobEvent {
                job_id: job.id,
                event_kind: JobEventKind::CommentPosted,
                event_status: JobEventStatus::Success,
                github_comment_id: Some(comment_id),
                remark: None,
                detail: None,
            })
            .await?;
        Ok(())
    }
}

impl JobV2Source {
    fn expect_token(&self, job: &RunnableJob) -> anyhow::Result<Uuid> {
        job.claim_token
            .ok_or_else(|| anyhow::anyhow!("new-schema job {} has no claim token", job.id))
    }
}

/// Extract `bench_args` from a queued event's [`QueuedEventDetail`].
/// `pr_comment` carries a token vec directly; `branch_push`/`tag_created`
/// carry a single optional string that we split on whitespace. Unknown /
/// unparseable detail yields no args (driver falls back to defaults).
fn bench_args_from_detail(detail: &serde_json::Value) -> Vec<String> {
    match serde_json::from_value::<QueuedEventDetail>(detail.clone()) {
        Ok(QueuedEventDetail::PrComment { bench_args, .. }) => bench_args,
        Ok(QueuedEventDetail::BranchPush { bench_args, .. })
        | Ok(QueuedEventDetail::TagCreated { bench_args, .. }) => bench_args
            .map(|s| {
                s.split_whitespace()
                    .map(String::from)
                    .collect()
            })
            .unwrap_or_default(),
        Err(_) => Vec::new(),
    }
}

/// Turn the orchestrator forensics `summary` into the new-schema outcome
/// companions: always a [`JobResult`] (carrying the archived `run.json`
/// content when readable + the archive dir), and a [`JobMetric`] when
/// `run.json` parsed with the full promoted-metric set.
fn extract_outcome(job_id: Uuid, summary: &serde_json::Value) -> (JobResult, Option<JobMetric>) {
    let dir = archive_dir(summary).unwrap_or_default();
    // Read the archived run.json once; store its raw content as
    // `run_json` and (if fully populated) promote it to a `job_metric`.
    let bytes = summary
        .get("run_json_archived_path")
        .and_then(|v| v.as_str())
        .and_then(|p| std::fs::read(p).ok());
    let run_json = bytes
        .as_deref()
        .and_then(|b| serde_json::from_slice::<serde_json::Value>(b).ok())
        .map(sqlx::types::Json);
    let metric = bytes
        .as_deref()
        .and_then(RunResult::from_bytes)
        .and_then(|r| metric_from_run(job_id, &r));
    (
        JobResult {
            job_id,
            run_json,
            archive_dir: dir,
            created_at: Utc::now(),
        },
        metric,
    )
}

fn archive_dir(summary: &serde_json::Value) -> Option<String> {
    summary
        .get("archive_dir")
        .and_then(|v| v.as_str())
        .map(String::from)
}

/// Map a parsed `run.json` to the typed `job_metric` row. Returns `None`
/// unless EVERY promoted metric is present — the columns are `NOT NULL`,
/// so a partial run.json gets a `job_result` (raw) but no `job_metric`.
fn metric_from_run(job_id: Uuid, run: &RunResult) -> Option<JobMetric> {
    let data = run.data.as_ref()?;
    let summary = data.summary.as_ref()?;
    Some(JobMetric {
        job_id,
        envelope_duration_us: secs_to_us(run.duration_secs?),
        replay_duration_us: secs_to_us(data.duration_secs?),
        total_duration_us: summary.total_duration_us? as i64,
        setup_duration_us: summary.setup_duration_us? as i64,
        execution_duration_us: summary.execution_duration_us? as i64,
        commit_duration_us: summary.commit_duration_us? as i64,
        clarity_runtime: summary.clarity_runtime? as i64,
        transactions: summary.transactions? as i64,
        read_length: summary.read_length? as i64,
        write_length: summary.write_length? as i64,
        measured_blocks: data.measured_blocks? as i64,
        warmup_blocks: data.warmup_blocks? as i64,
        created_at: Utc::now(),
    })
}

/// Seconds (f64) → microseconds (i64), clamped at 0 (the column has a
/// `CHECK >= 0`; a negative/NaN duration is nonsensical anyway).
fn secs_to_us(secs: f64) -> i64 {
    if secs.is_finite() && secs > 0.0 { (secs * 1_000_000.0) as i64 } else { 0 }
}
