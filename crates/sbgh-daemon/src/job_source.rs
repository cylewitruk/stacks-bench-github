//! The daemon's queue abstraction.
//!
//! The runner drives the same libvirt logic over a [`RunnableJobStore`],
//! decoupling it from the `job` family's storage shape and giving tests a
//! seam (the runner tests use a fake impl). This module provides:
//!
//!   - [`RunnableJob`] — a storage-neutral execution view (everything the
//!     driver + progress reporter need), assembled from the `job` family.
//!   - [`RunnableJobStore`] — claim + lifecycle, implemented by [`JobSource`]
//!     over [`sbgh_core::db::JobStore`] + [`RepoStore`] + [`PullRequestStore`].
//!
//! `pr_comment` jobs report on a [`ProgressTarget::PullRequest`] (a Check Run
//! on the PR head and/or a summary comment, per `[reporting]`); baseline jobs
//! (`branch_push`/`tag_created`) on a [`ProgressTarget::CommitCheck`] (a
//! commit-level Check Run). The comment id and check run id+url are each
//! recorded on a `comment_posted` / `check_run_created` `job_event`, read back
//! on re-claim so a reclaimed job updates them rather than duplicating.
//! `tag_created` jobs are enqueued with no commit and resolved at claim time
//! (the runner calls [`sbgh_core::github::GitHubApi::resolve_commit`] in
//! preflight). Deferred: the intermediate phase-event timeline (provision/
//! build/bench `job_event` rows — phase changes are logged only).

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sbgh_core::bench_args::normalize_stored_value;
use sbgh_core::db::{
    BaselineSelection, BenchmarkRunMetric, JobCompletion, JobFailure, JobStore, PullRequestStore,
    RepoStore,
};
use sbgh_core::models::{
    GitRefKind, Job, JobEventKind, JobEventStatus, JobMetric, JobResult, NewJobEvent,
    QueuedEventDetail, ResolvedCommit,
};
use uuid::Uuid;

use crate::artifact_store::ArtifactStore;
use crate::bench_summary::RunResult;

/// Where a job's lifecycle progress should be surfaced (roadmap-v4).
#[derive(Debug, Clone)]
pub enum ProgressTarget {
    /// `/benchmark` PR job — a Check Run on the PR head and/or a summary
    /// comment (per `[reporting].pr_report`). `comment_id`/`check_run_id` are
    /// `None` until the daemon creates them; `check_run_url` carries the
    /// created check's link so the comment can point at it (read back on
    /// re-claim).
    PullRequest {
        pr_number: i64,
        comment_id: Option<i64>,
        check_run_id: Option<i64>,
        check_run_url: Option<String>,
    },
    /// Baseline job (`branch_push`/`tag_created`) — a commit-level Check Run
    /// (per `[reporting].baseline_report`), no PR comment. `check_run_id` is
    /// `None` until created / read back on re-claim, and stays `None` when
    /// `baseline_report = none` (the "report nothing" case).
    CommitCheck { check_run_id: Option<i64> },
    /// Slack ad-hoc job (`slack_adhoc`, v5/0002) — reports into the thread on
    /// the user's request message: `channel` is the Slack channel and
    /// `message_ts` the request's timestamp (the thread anchor + the
    /// message the status reaction is added to). No GitHub surface. Assembled
    /// at claim time from a `slack_adhoc` job's `SlackAdhoc` queued detail.
    /// `plan_message_ts` is the live-timeline `plan` card's own message `ts`,
    /// `None` until posted and read back on re-claim (a `plan_message_sent`
    /// event) so a reclaimed job resumes updating the same card.
    Slack {
        channel: String,
        message_ts: String,
        /// The live-timeline `plan` card's own message `ts` — populated at
        /// claim time (read back via `latest_plan_message_ts`) so a
        /// reclaimed job resumes the existing card; `None` until the
        /// card is first posted.
        plan_message_ts: Option<String>,
    },
    /// Build-only job (v10 0005 / item 0031 warming) — builds + caches an
    /// artifact and stops. It has no measurement to render, so it reports
    /// nothing: the empty report surface set (`report = none`). Derived for
    /// `task_kind = build_only`; routed to a no-op report surface.
    Silent,
}

/// Storage-neutral execution context for one benchmark run. Assembled by
/// a [`RunnableJobStore`] from the `job` family.
#[derive(Debug, Clone)]
pub struct RunnableJob {
    pub id: Uuid,
    /// 0037: user-facing benchmark request this run belongs to.
    pub benchmark_group_id: Uuid,
    /// 0037: concrete workload/rev variant within the group.
    pub benchmark_spec_id: Uuid,
    /// 0037/0038: isolated execution number for this spec. Singleton jobs are
    /// 0.
    pub benchmark_run_index: i32,
    /// Requested isolated run count for this spec. Singleton jobs store 1.
    pub requested_run_count: i32,
    /// Group-scoped artifact prefix. Repeat runs use it for the carried SQLite
    /// DB.
    pub group_artifact_prefix: String,
    /// `owner/name`. Drives the git clone URL.
    pub repository: String,
    /// Resolved commit/SHA to benchmark. Empty when unresolved at claim
    /// time, which the runner resolves in preflight: `pr_comment` via the
    /// PR head SHA, `tag_created` via the tag ref. `branch_push` carries
    /// its commit from enqueue.
    pub commit: String,
    /// Human-readable ref label (PR head branch / watched branch / tag),
    /// for logs + progress.
    pub git_ref_display: String,
    /// What kind of ref `git_ref_display` is. Drives commit resolution
    /// when `commit` is empty: a `Tag` resolves via
    /// `GitHubApi::resolve_commit`.
    pub git_ref_kind: GitRefKind,
    pub installation_id: i64,
    /// v10 (0005): the run-shape axis — selects the recipe at dispatch
    /// (`benchmark` → bench, `build_only` → build + cache, silent).
    pub task_kind: sbgh_core::models::TaskKind,
    /// v10 (0005): which artifact binary the recipe builds/runs. Dispatch keys
    /// on `(task_kind, build_target)` and fails closed on unsupported combos,
    /// so a `stacks_inspect` row can't silently run the `stacks_bench`
    /// path.
    pub build_target: sbgh_core::models::BuildTarget,
    /// roadmap-v7: the job's workload key, for the vs-baseline lookup (only a
    /// baseline of the *same* workload is comparable). `None` on pre-v7 rows.
    pub workload_key: Option<String>,
    /// Resolved `stacks-bench` CLI args. Empty → the driver falls back to
    /// the configured `default_args`.
    pub bench_args: Vec<String>,
    pub progress: ProgressTarget,
    /// Claim token. The store guards its `running`/terminal writes on
    /// `(id, claim_token)` so a write that lost its lease to the
    /// stuck-claim sweep is a no-op.
    pub claim_token: Option<Uuid>,
}

/// roadmap-v7: a resolved comparison baseline — the measured metric + the
/// provenance the report links to. The repo is `owner/name` (resolved from the
/// anchor's `github_repo_id` by the store), since the baseline may live in a
/// *different* repo than the PR (a fork PR vs. an upstream baseline).
#[derive(Debug, Clone)]
pub struct BaselineRef {
    pub metric: JobMetric,
    pub repository: String,
    pub commit: String,
    pub git_ref_display: String,
    pub committed_at: Option<DateTime<Utc>>,
    pub selection: BaselineSelection,
}

/// Claim + lifecycle over the job queue. Implemented by [`JobSource`]; the
/// runner tests use a fake impl, which is why this stays a trait.
#[async_trait]
pub trait RunnableJobStore: Send + Sync + 'static {
    /// Claim the next runnable job, transitioning `queued → claimed` (the
    /// `claimed → running` step happens in [`Self::start_running`] once
    /// execution actually begins).
    async fn claim_next(&self) -> anyhow::Result<Option<RunnableJob>>;

    /// Assemble the read-only [`RunnableJob`] view for an existing job by id,
    /// **without** claiming it (no status change; `claim_token = None`). Used
    /// by orphan recovery (4C-2) to conclude a stuck orphan's Check Run +
    /// stale comment through the normal reporting path. `None` if the row
    /// is gone.
    async fn load_runnable(&self, job_id: Uuid) -> anyhow::Result<Option<RunnableJob>>;

    /// All `queued` jobs in claim order, as read-only [`RunnableJob`] views
    /// (`claim_token = None`). Phase 5: the coordinator reports each waiting
    /// job its queue position on a check. Empty when nothing is queued.
    async fn list_queued(&self) -> anyhow::Result<Vec<RunnableJob>>;

    /// Mark the job as actually running, persisting a commit resolved
    /// during preflight (if any): `claimed → running` carrying the
    /// resolved commit under the claim-token guard. A `ResolvedCommit`
    /// with `committed_at: None` (the PR-head-resolve case yields only a
    /// SHA) leaves the existing `git_committed_at` untouched.
    async fn start_running(
        &self,
        job: &RunnableJob,
        resolved_commit: Option<ResolvedCommit>,
    ) -> anyhow::Result<()>;

    /// Recover jobs stranded mid-claim. A row can be left in `claimed` if
    /// the daemon crashes (or preflight errors) after `claim_next`
    /// but before `start_running`; this resets `claimed` rows older than
    /// `lease` back to `queued`. Returns the number of rows recovered.
    async fn sweep_stuck_claims(&self, lease: chrono::Duration) -> anyhow::Result<u64>;

    /// Terminal success. `summary` is the daemon forensics blob
    /// (archive paths, console tail, finish reason).
    async fn complete(&self, job: &RunnableJob, summary: &serde_json::Value) -> anyhow::Result<()>;

    /// roadmap-v7: resolve the baseline a PR run should be compared against
    /// (the merge-base, else nearest-before on the target branch) into a
    /// render-ready [`BaselineRef`] — the baseline metric + its repo
    /// `owner/name` (resolved from the anchor's `github_repo_id`) +
    /// commit/ref/selection. `None` if no comparable baseline exists.
    /// Best-effort; the caller degrades to absolute-only.
    async fn find_baseline(
        &self,
        merge_base_sha: &str,
        base_ref: &str,
        merge_base_committed_at: Option<DateTime<Utc>>,
        workload_key: &str,
    ) -> anyhow::Result<Option<BaselineRef>>;

    /// v15 Phase 5: promoted metrics for all completed isolated runs in the
    /// same benchmark spec. Used by group-level reporting to render aggregate
    /// repeat statistics from durable `job_metric` rows.
    async fn benchmark_run_metrics(
        &self,
        benchmark_spec_id: Uuid,
    ) -> anyhow::Result<Vec<BenchmarkRunMetric>> {
        let _ = benchmark_spec_id;
        Ok(Vec::new())
    }

    /// Terminal failure. `summary` is the same forensics blob shape when
    /// available (setup-time failures may have none).
    async fn fail(
        &self,
        job: &RunnableJob,
        error: &str,
        summary: Option<&serde_json::Value>,
    ) -> anyhow::Result<()>;

    /// Terminal **cancellation** (roadmap-v5 Phase 4C): a deliberately-stopped
    /// run (operator shutdown/abort), recorded as `cancelled` not `failed` so
    /// it doesn't read as a broken benchmark. Like [`fail`](Self::fail) but
    /// with no forensics summary (a cancelled run produced none).
    async fn cancel(&self, job: &RunnableJob, remark: &str) -> anyhow::Result<()>;

    /// Persist the GitHub comment id the daemon just posted. Only
    /// called for [`ProgressTarget::PullRequest`] jobs.
    async fn set_comment_id(&self, job: &RunnableJob, comment_id: i64) -> anyhow::Result<()>;

    /// Persist the Check Run id + html_url the daemon just created (a
    /// `check_run_created` `job_event`), read back on re-claim so a reclaimed
    /// job updates the existing check (and can rebuild the comment link)
    /// instead of creating a duplicate.
    async fn set_check_run(
        &self,
        job: &RunnableJob,
        check_run_id: i64,
        html_url: Option<&str>,
    ) -> anyhow::Result<()>;

    /// Persist the Slack live-timeline `plan` message `ts` the daemon just
    /// posted (a `plan_message_sent` `job_event`), read back on re-claim so a
    /// reclaimed [`ProgressTarget::Slack`] job `chat.update`s the existing card
    /// instead of posting a duplicate. Only called for Slack jobs.
    async fn set_plan_message_ts(&self, job: &RunnableJob, message_ts: &str) -> anyhow::Result<()>;

    /// Orphan recovery (roadmap-v5 Phase 4B-2): job ids stranded in `running`.
    /// At daemon startup these are necessarily orphans from a crashed/killed
    /// prior daemon, so the runner cleans each one's leaked VM (via
    /// `cleanup_by_job_id`) and then [`cancel_orphan`](Self::cancel_orphan)s
    /// it.
    async fn running_job_ids(&self) -> anyhow::Result<Vec<Uuid>>;

    /// Orphan recovery (4B-2 + 4C): terminal-**cancel** a job stranded in
    /// `running`, with no claim-token guard (the claimer is dead; runs at
    /// startup before any new claim). A crash-orphan is re-triggerable, not a
    /// failure, so it's `cancelled`. Returns `false` if the row wasn't
    /// `running` — idempotent.
    async fn cancel_orphan(&self, job_id: Uuid, remark: &str) -> anyhow::Result<bool>;
}

/// [`RunnableJobStore`] over the `job` family. Composes
/// [`JobStore`] (claim + lifecycle) with [`RepoStore`] (resolve
/// `github_repo_id → owner/name`) and [`PullRequestStore`] (resolve a
/// `pr_comment` job's PR number for comment posting), and reads the
/// queued `job_event` for the run's `bench_args`.
///
/// `pr_comment` jobs report on a [`ProgressTarget::PullRequest`] (comment id +
/// check id recorded as `comment_posted` / `check_run_created` `job_event`s,
/// read back on re-claim for idempotency); baseline jobs
/// (`branch_push`/`tag_created`) report on a [`ProgressTarget::CommitCheck`].
pub struct JobSource {
    jobs: Arc<dyn JobStore>,
    repos: Arc<dyn RepoStore>,
    pull_requests: Arc<dyn PullRequestStore>,
    /// The configured artifact store, for resolving a completed job's
    /// `run.json` store key (Decision 0002) when persisting its metric.
    store: Arc<dyn ArtifactStore>,
}

impl JobSource {
    pub fn new(
        jobs: Arc<dyn JobStore>,
        repos: Arc<dyn RepoStore>,
        pull_requests: Arc<dyn PullRequestStore>,
        store: Arc<dyn ArtifactStore>,
    ) -> Self {
        Self {
            jobs,
            repos,
            pull_requests,
            store,
        }
    }
}

#[async_trait]
impl RunnableJobStore for JobSource {
    async fn claim_next(&self) -> anyhow::Result<Option<RunnableJob>> {
        let claim_token = Uuid::new_v4();
        let Some(job) = self
            .jobs
            .claim_next_queued(claim_token)
            .await?
        else {
            return Ok(None);
        };
        Ok(Some(
            self.assemble_runnable(job, Some(claim_token))
                .await?,
        ))
    }

    async fn load_runnable(&self, job_id: Uuid) -> anyhow::Result<Option<RunnableJob>> {
        let Some(job) = self
            .jobs
            .lookup_job(job_id)
            .await?
        else {
            return Ok(None);
        };
        // Read-only view: no claim taken, so `claim_token = None`.
        Ok(Some(
            self.assemble_runnable(job, None)
                .await?,
        ))
    }

    async fn list_queued(&self) -> anyhow::Result<Vec<RunnableJob>> {
        let mut out = Vec::new();
        for job in self
            .jobs
            .queued_jobs_ordered()
            .await?
        {
            // Read-only assembly, claim order preserved (`claim_token = None`).
            out.push(
                self.assemble_runnable(job, None)
                    .await?,
            );
        }
        Ok(out)
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
        let (result, metric) = extract_outcome(job.id, summary, self.store.as_ref()).await;
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

    async fn find_baseline(
        &self,
        merge_base_sha: &str,
        base_ref: &str,
        merge_base_committed_at: Option<DateTime<Utc>>,
        workload_key: &str,
    ) -> anyhow::Result<Option<BaselineRef>> {
        let Some(m) = self
            .jobs
            .find_baseline_for(merge_base_sha, base_ref, merge_base_committed_at, workload_key)
            .await?
        else {
            return Ok(None);
        };
        // Resolve the baseline's repo `owner/name` for the report link — it may
        // be a different repo than the PR (fork PR vs. upstream baseline).
        let repo = self
            .repos
            .lookup_repo(m.anchor.github_repo_id)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("baseline references unknown repo {}", m.anchor.github_repo_id)
            })?;
        Ok(Some(BaselineRef {
            metric: m.metric,
            repository: format!("{}/{}", repo.owner, repo.name),
            commit: m.anchor.commit,
            git_ref_display: m.anchor.git_ref_display,
            committed_at: m.anchor.committed_at,
            selection: m.anchor.selection,
        }))
    }

    async fn benchmark_run_metrics(
        &self,
        benchmark_spec_id: Uuid,
    ) -> anyhow::Result<Vec<BenchmarkRunMetric>> {
        Ok(self
            .jobs
            .benchmark_run_metrics(benchmark_spec_id)
            .await?)
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

    async fn cancel(&self, job: &RunnableJob, remark: &str) -> anyhow::Result<()> {
        let claim_token = self.expect_token(job)?;
        let ok = self
            .jobs
            .cancel_job(job.id, claim_token, remark)
            .await?;
        if !ok {
            // Same stale-claim semantics as `fail`: the sweep reclaimed our
            // lease, so leave the row for re-claim rather than clobbering it.
            tracing::warn!(
                job_id = %job.id,
                "cancel_job was a no-op (lost our claim to the sweep); leaving for re-claim"
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
                github_check_run_id: None,
                github_check_run_url: None,
                remark: None,
                detail: None,
            })
            .await?;
        Ok(())
    }

    async fn set_check_run(
        &self,
        job: &RunnableJob,
        check_run_id: i64,
        html_url: Option<&str>,
    ) -> anyhow::Result<()> {
        // Check identity lives on a `check_run_created` timeline event;
        // `latest_check_run` reads it back on (re-)claim for idempotency.
        self.jobs
            .insert_event(&NewJobEvent {
                job_id: job.id,
                event_kind: JobEventKind::CheckRunCreated,
                event_status: JobEventStatus::Success,
                github_comment_id: None,
                github_check_run_id: Some(check_run_id),
                github_check_run_url: html_url.map(str::to_string),
                remark: None,
                detail: None,
            })
            .await?;
        Ok(())
    }

    async fn set_plan_message_ts(&self, job: &RunnableJob, message_ts: &str) -> anyhow::Result<()> {
        // The claimed-world convenience wrapper over `JobStore::record_plan_message_ts`
        // (which the pre-claim Slack connector also uses, by `job.id`) — the
        // `plan_message_sent` event shape lives there. Read back on (re-)claim via
        // `latest_plan_message_ts` for resume-without-duplicate.
        self.jobs
            .record_plan_message_ts(job.id, message_ts)
            .await?;
        Ok(())
    }

    async fn running_job_ids(&self) -> anyhow::Result<Vec<Uuid>> {
        Ok(self
            .jobs
            .running_job_ids()
            .await?)
    }

    async fn cancel_orphan(&self, job_id: Uuid, remark: &str) -> anyhow::Result<bool> {
        Ok(self
            .jobs
            .cancel_orphan(job_id, remark)
            .await?)
    }
}

impl JobSource {
    fn expect_token(&self, job: &RunnableJob) -> anyhow::Result<Uuid> {
        job.claim_token
            .ok_or_else(|| anyhow::anyhow!("new-schema job {} has no claim token", job.id))
    }

    /// Assemble the storage-neutral [`RunnableJob`] view from a `job` row:
    /// resolve `owner/name`, read `bench_args` from the queued event, and the
    /// reporting surface (a PR's Check Run + comment ids, or a baseline commit
    /// check). Shared by [`claim_next`](RunnableJobStore::claim_next) (with the
    /// fresh claim token) and
    /// [`load_runnable`](RunnableJobStore::load_runnable) (read-only,
    /// `claim_token = None`).
    async fn assemble_runnable(
        &self,
        job: Job,
        claim_token: Option<Uuid>,
    ) -> anyhow::Result<RunnableJob> {
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
        let group = self
            .jobs
            .lookup_benchmark_group(job.benchmark_group_id)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "job {} references unknown benchmark_group {}",
                    job.id,
                    job.benchmark_group_id
                )
            })?;
        let spec = self
            .jobs
            .lookup_benchmark_spec(job.benchmark_spec_id)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "job {} references unknown benchmark_spec {}",
                    job.id,
                    job.benchmark_spec_id
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
            .map(|d| normalize_stored_value(&d.0))
            .unwrap_or_default();

        // The report surface is derived from the job's axes (v10 0005): a
        // `task_kind = build_only` job has no measurement to render and reports
        // nothing (`report = none`); a `source = slack` job reports into its
        // Slack thread (no GitHub surface); PR-linked jobs report on a Check Run
        // + comment; the rest on a commit-level Check Run. The already-created
        // comment/check ids are read back so a reclaimed (or recovered) job
        // updates them rather than duplicating.
        let progress = if job.task_kind == sbgh_core::models::TaskKind::BuildOnly {
            ProgressTarget::Silent
        } else if job.source == sbgh_core::models::JobSource::Slack {
            // `channel`/`message_ts` are reporting provenance in the
            // `SlackAdhoc` queued detail — a `slack_adhoc` job MUST carry it (and
            // never falls through to a commit check).
            let (channel, message_ts) = queued
                .as_ref()
                .and_then(|e| e.detail.as_ref())
                .and_then(|d| serde_json::from_value::<QueuedEventDetail>(d.0.clone()).ok())
                .and_then(|d| match d {
                    QueuedEventDetail::SlackAdhoc { channel, message_ts, .. } => {
                        Some((channel, message_ts))
                    }
                    _ => None,
                })
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "slack_adhoc job {} is missing its SlackAdhoc queued detail \
                         (channel/message_ts)",
                        job.id
                    )
                })?;
            // Read back the live-timeline card's `ts` (if already posted) so a
            // reclaimed job resumes updating it instead of posting a duplicate.
            let plan_message_ts = self
                .jobs
                .latest_plan_message_ts(job.id)
                .await?;
            ProgressTarget::Slack {
                channel,
                message_ts,
                plan_message_ts,
            }
        } else {
            let check_run = self
                .jobs
                .latest_check_run(job.id)
                .await?;
            match self
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
                    ProgressTarget::PullRequest {
                        pr_number: pr.pr_number as i64,
                        comment_id,
                        check_run_id: check_run
                            .as_ref()
                            .map(|(id, _)| *id),
                        check_run_url: check_run.and_then(|(_, url)| url),
                    }
                }
                None => ProgressTarget::CommitCheck {
                    check_run_id: check_run.map(|(id, _)| id),
                },
            }
        };

        Ok(RunnableJob {
            id: job.id,
            benchmark_group_id: job.benchmark_group_id,
            benchmark_spec_id: job.benchmark_spec_id,
            benchmark_run_index: job.benchmark_run_index,
            requested_run_count: spec.requested_run_count,
            group_artifact_prefix: group.artifact_prefix,
            repository: format!("{}/{}", repo.owner, repo.name),
            commit: job
                .git_commit_hash
                .unwrap_or_default(),
            git_ref_display: job.git_ref_display,
            git_ref_kind: job.git_ref_kind,
            installation_id: job.github_installation_id,
            task_kind: job.task_kind,
            build_target: job.build_target,
            workload_key: job.workload_key,
            bench_args,
            progress,
            claim_token,
        })
    }
}

/// Turn the daemon forensics `summary` into the new-schema outcome
/// companions: always a [`JobResult`] (carrying the archived `run.json`
/// content when readable + the archive dir), and a [`JobMetric`] when
/// `run.json` parsed with the full promoted-metric set.
async fn extract_outcome(
    job_id: Uuid,
    summary: &serde_json::Value,
    store: &dyn ArtifactStore,
) -> (JobResult, Option<JobMetric>) {
    let dir = archive_dir(summary).unwrap_or_default();
    // Resolve the run.json store **key** (Decision 0002) to a local path, then
    // read it once: stored raw as `run_json` and (if fully populated) promoted
    // to a `job_metric`.
    let run_json_path = match summary
        .get("run_json_archived_path")
        .and_then(|v| v.as_str())
    {
        Some(key) => store.get(key).await.ok(),
        None => None,
    };
    let bytes = run_json_path.and_then(|p| std::fs::read(p).ok());
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
/// Also reused (roadmap-v7) to build the PR run's metric for the vs-baseline
/// comparison, so the comment's delta is on the same numbers we persist.
pub fn metric_from_run(job_id: Uuid, run: &RunResult) -> Option<JobMetric> {
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
