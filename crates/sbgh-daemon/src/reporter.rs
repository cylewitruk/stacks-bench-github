//! The per-job reporter.
//!
//! Constructed for each fleet job, the [`Reporter`] owns GitHub/Slack report
//! preparation and terminal rendering. The fleet state machine owns durable job
//! lifecycle transitions; workers only execute.
//!
//! [`Reporter::ensure_fleet_reporting`] reconciles provider identities before
//! an assignment is offered. [`Reporter::report_fleet_terminal`] renders a
//! terminal outcome only after fenced acceptance and artifact promotion.
//!
//! A test-only `run` method retains the former in-process path as a
//! behavior-parity harness; it is not compiled into the production daemon.

use std::collections::HashMap;
use std::sync::Arc;

use sbgh_core::config::DaemonConfig;
#[cfg(test)]
use sbgh_core::models::{GitRefKind, ResolvedCommit};
use sbgh_github::{CheckRunOutput, CheckRunState, CheckRunUpdate, GitHubApi};
use tokio::sync::OnceCell;
#[cfg(test)]
use tokio::sync::{mpsc, oneshot};

use crate::artifact_store::ArtifactStore;
#[cfg(test)]
use crate::artifact_store::{ArtifactStoreConfig, build_store_or_local};
use crate::bench_summary::RunResult;
use crate::comparison::{BaselineComparison, GroupComparison, compare};
use crate::job_source::{ProgressTarget, RunnableJob, RunnableJobStore};
#[cfg(test)]
use crate::report::build_report_surface;
#[cfg(test)]
use crate::report::log_nonfatal_projection;
use crate::report::{CompletionReport, ReportSurface, marked_pr_comment, pr_report_marker};
use crate::slack_report::SlackSessionRegistry;
#[cfg(test)]
use sbgh_driver::{Terminal, WorkerEvent};
use sbgh_slack::SlackClient;

/// Resolve the App's numeric id via `GET /app`, cached on **success** in `cell`
/// (a `get_or_try_init` that leaves the cell empty on error, so a transient
/// startup blip is retried on the next job rather than disabling the reconcile
/// for the whole process). `None` → the Check Run reconcile is skipped this
/// time and a fresh check may still be created; PR-comment creation instead
/// fails closed because its marker is not safe without an authenticated author
/// id. The `OnceCell` is shared across the daemon's jobs (the fleet coordinator
/// owns it), so successful resolution happens at most once.
pub async fn resolved_app_id(cell: &OnceCell<i64>, gh: &dyn GitHubApi) -> Option<i64> {
    match cell
        .get_or_try_init(|| async {
            let id = gh.current_app_id().await?;
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

/// Display name of the Check Run the daemon posts. Stable so the
/// reconcile filter (`check_name`) and GitHub's per-name dedup both work.
pub const CHECK_NAME: &str = "stacks-bench";

/// Test-only result passed from the legacy reporter to its in-process worker.
#[derive(Debug)]
#[cfg(test)]
pub enum Prepared {
    Run { commit: String },
    Abort,
}

/// Owns the GitHub + lifecycle side-effects for one job (see module docs).
pub struct Reporter {
    config: Arc<DaemonConfig>,
    jobs: Arc<dyn RunnableJobStore>,
    gh: Arc<dyn GitHubApi>,
    artifact_store: Arc<dyn ArtifactStore>,
    /// Shared App-id cache (the fleet coordinator owns it). Resolved lazily by
    /// `ensure_check` only when a Check Run is actually wanted, so a no-report
    /// job makes no `GET /app` call.
    app_id: Arc<OnceCell<i64>>,
    /// The Slack surface for `ProgressTarget::Slack` jobs, shared from the
    /// coordinator. `None` for a GitHub-only deployment.
    #[allow(dead_code)]
    slack: Option<Arc<dyn SlackClient>>,
    /// Group-scoped Slack reporting sessions, shared from the coordinator so a
    /// repeat group's per-run surfaces reuse one canonical snapshot session.
    #[allow(dead_code)]
    slack_sessions: Arc<SlackSessionRegistry>,
    job: RunnableJob,
}

pub(crate) struct ReporterDependencies {
    pub jobs: Arc<dyn RunnableJobStore>,
    pub gh: Arc<dyn GitHubApi>,
    pub artifact_store: Arc<dyn ArtifactStore>,
    pub app_id: Arc<OnceCell<i64>>,
    pub slack: Option<Arc<dyn SlackClient>>,
    pub slack_sessions: Arc<SlackSessionRegistry>,
}

impl Reporter {
    #[cfg(test)]
    pub fn new(
        config: Arc<DaemonConfig>,
        jobs: Arc<dyn RunnableJobStore>,
        gh: Arc<dyn GitHubApi>,
        app_id: Arc<OnceCell<i64>>,
        slack: Option<Arc<dyn SlackClient>>,
        slack_sessions: Arc<SlackSessionRegistry>,
        job: RunnableJob,
    ) -> Self {
        let artifact_store = build_store_or_local(&ArtifactStoreConfig::local(
            config
                .paths
                .results_archive_dir
                .clone(),
        ));
        Self::new_with_dependencies(
            config,
            ReporterDependencies {
                jobs,
                gh,
                artifact_store,
                app_id,
                slack,
                slack_sessions,
            },
            job,
        )
    }

    pub(crate) fn new_with_dependencies(
        config: Arc<DaemonConfig>,
        dependencies: ReporterDependencies,
        job: RunnableJob,
    ) -> Self {
        let ReporterDependencies {
            jobs,
            gh,
            artifact_store,
            app_id,
            slack,
            slack_sessions,
        } = dependencies;
        Self {
            config,
            jobs,
            gh,
            artifact_store,
            app_id,
            slack,
            slack_sessions,
            job,
        }
    }

    /// Prepare only the external reporting identities for a fleet job.
    /// Execution and lifecycle persistence are owned by the fleet state
    /// machine; this retains the existing search-before-create reconciliation
    /// without claiming or starting the job inline.
    pub(crate) async fn ensure_fleet_reporting(mut self) -> RunnableJob {
        self.ensure_reporting().await;
        self.job
    }

    /// Render an already-persisted fleet terminal through the existing
    /// provider-owned surfaces. This deliberately performs no lifecycle DB
    /// write: terminal acceptance is atomic in `FleetStore`.
    pub(crate) async fn report_fleet_terminal(
        &self,
        surface: &dyn ReportSurface,
        outcome: &sbgh_proto::TerminalOutcome,
    ) -> anyhow::Result<()> {
        match outcome {
            sbgh_proto::TerminalOutcome::Completed { summary, block_validation } => {
                let group_comparison = self.group_comparison().await;
                let baseline_comparison = self
                    .baseline_comparison(summary)
                    .await;
                let mut render_summary = summary.clone();
                if let Some(result) = block_validation
                    && let Some(object) = render_summary.as_object_mut()
                {
                    object.insert(
                        "block_validation".into(),
                        serde_json::to_value(result).unwrap_or(serde_json::Value::Null),
                    );
                }
                surface
                    .completed(CompletionReport {
                        summary: &render_summary,
                        baseline_comparison: baseline_comparison.as_ref(),
                        group_comparison: group_comparison.as_ref(),
                    })
                    .await
            }
            sbgh_proto::TerminalOutcome::Failed { error, .. } => surface.failed(error).await,
            sbgh_proto::TerminalOutcome::Cancelled { reason } => {
                surface
                    .cancelled(reason)
                    .await
            }
        }
    }

    /// The reporter task body. Consumes `self`; see module docs for the
    /// lifecycle and the `Err` semantics.
    #[cfg(test)]
    pub async fn run(
        mut self,
        mut events_rx: mpsc::Receiver<WorkerEvent>,
        prepared_tx: oneshot::Sender<Prepared>,
    ) -> anyhow::Result<()> {
        // ── prepare: resolve commit (FATAL) ───────────────────────────
        let resolved_commit = match self.resolve_commit().await {
            Ok(c) => c,
            Err(e) => {
                let msg = format!("pre-flight failed: {e}");
                tracing::error!(job_id = %self.job.id, error = ?e, "pre-flight failed");
                let _ = self
                    .jobs
                    .fail(&self.job, &msg, None)
                    .await;
                let _ = prepared_tx.send(Prepared::Abort);
                return Err(e);
            }
        };

        // ── prepare: reporting surfaces (NON-FATAL) ───────────────────
        if !self.job.commit.is_empty() {
            self.ensure_reporting().await;
        }

        // ── prepare: claimed → running (persist resolved commit) ──────
        let commit_persisted = resolved_commit.is_some();
        if let Err(e) = self
            .jobs
            .start_running(&self.job, resolved_commit)
            .await
        {
            // Lost the claim (stale) — leave it for the sweep; don't fail.
            tracing::error!(job_id = %self.job.id, error = ?e, "start_running failed (stale claim?)");
            let _ = prepared_tx.send(Prepared::Abort);
            return Err(e);
        }
        tracing::info!(
            job_id = %self.job.id,
            commit = %self.job.commit,
            commit_persisted,
            "job running (claimed → running)",
        );

        // The one reporting surface for this job: the Slack snapshot, an
        // explicit no-op for Slack without a client, or the GitHub
        // comment+check. Built **once** and shared across
        // `started`, the drain loop, and `finish`, so a stateful surface (the
        // Slack message) keeps a single projected state across the whole run.
        let surface = build_report_surface(
            self.gh.clone(),
            self.jobs.clone(),
            self.artifact_store.clone(),
            self.slack.as_ref(),
            &self.slack_sessions,
            &self.job,
        );

        // Defensive empty-commit guard: now `running`, fail terminally so it
        // records a terminal state instead of looping via the sweep.
        if self.job.commit.is_empty() {
            let msg = "no resolved commit; cannot benchmark";
            tracing::error!(job_id = %self.job.id, git_ref = %self.job.git_ref_display, "{msg}");
            let _ = self
                .jobs
                .fail(&self.job, msg, None)
                .await;
            log_nonfatal_projection(self.job.id, surface.failed(msg).await);
            let _ = prepared_tx.send(Prepared::Abort);
            return Ok(());
        }

        log_nonfatal_projection(self.job.id, surface.started().await);

        // ── gate the worker on the resolved commit ────────────────────
        if prepared_tx
            .send(Prepared::Run {
                commit: self.job.commit.clone(),
            })
            .is_err()
        {
            // The worker dropped before the hand-off — it won't run, and the
            // job is already `running`. Terminal-fail it so it can't hang.
            return self
                .abandon(surface.as_ref(), "worker dropped before the run could start")
                .await;
        }

        // ── drain the worker's event stream ───────────────────────────
        let mut terminal_err: Option<anyhow::Error> = None;
        let mut saw_terminal = false;
        while let Some(ev) = events_rx.recv().await {
            match ev {
                WorkerEvent::Phase { label, elapsed } => {
                    log_nonfatal_projection(
                        self.job.id,
                        surface
                            .phase(&label.into(), elapsed)
                            .await,
                    );
                }
                WorkerEvent::Heartbeat { label, elapsed } => {
                    log_nonfatal_projection(
                        self.job.id,
                        surface
                            .heartbeat(&label.into(), elapsed)
                            .await,
                    );
                }
                WorkerEvent::Progress(progress) => {
                    log_nonfatal_projection(
                        self.job.id,
                        surface
                            .progress(&progress.into())
                            .await,
                    );
                }
                WorkerEvent::Finished(terminal) => {
                    saw_terminal = true;
                    terminal_err = self
                        .finish(surface.as_ref(), terminal)
                        .await;
                    // The worker drops its senders after `Finished`, so the
                    // next `recv` returns `None` and the loop ends.
                }
            }
        }

        // The channel closed without a `Finished` after we gated the worker to
        // run — it panicked or exited abnormally. Don't leave the job stuck in
        // `running`: terminal-fail and back off the loop.
        if !saw_terminal {
            return self
                .abandon(surface.as_ref(), "worker exited before reporting a terminal outcome")
                .await;
        }

        match terminal_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Terminal-fail a job whose worker disappeared without a terminal event
    /// (so it can't hang in `running`) and back off the poll loop. The job is
    /// already `running` by the time the worker is gated, so there's nothing to
    /// leave for the `claimed`-only stuck-claim sweep.
    #[cfg(test)]
    async fn abandon(&self, surface: &dyn ReportSurface, msg: &str) -> anyhow::Result<()> {
        tracing::error!(job_id = %self.job.id, "{msg}");
        let _ = self
            .jobs
            .fail(&self.job, msg, None)
            .await;
        log_nonfatal_projection(self.job.id, surface.failed(msg).await);
        Err(anyhow::anyhow!("{msg}"))
    }

    /// Write the terminal state (DB + GitHub). Returns `Some(err)` when the run
    /// loop should back off: a persistence failure, or a recipe `SetupError`
    /// (the run couldn't start) — preserving the prior `execute` semantics.
    /// Best-effort comparison against the baseline for a completed PR run.
    /// Resolves the PR's base ref and head identity (`get_pull_request`), the
    /// merge-base (`compare_commits`), the matching baseline (`find_baseline`),
    /// and this run's metric, then [`compare`]s them. `None` (→ absolute-only
    /// render) for non-PR jobs, no comparable baseline, or any lookup/parse
    /// error — reporting is non-fatal.
    async fn baseline_comparison(&self, summary: &serde_json::Value) -> Option<BaselineComparison> {
        let ProgressTarget::PullRequest { pr_number, .. } = &self.job.progress else {
            return None;
        };
        let pr_number = *pr_number as u64;
        let workload_key = self
            .job
            .workload_key
            .as_deref()?;

        // PR base ref + head identity (the issue_comment payload didn't carry them).
        let pr = match self
            .gh
            .get_pull_request(self.job.installation_id, &self.job.repository, pr_number)
            .await
        {
            Ok(pr) => pr,
            Err(e) => {
                tracing::debug!(job_id = %self.job.id, error = ?e, "vs-baseline: get_pull_request failed (absolute-only)");
                return None;
            }
        };

        // The benchmarked commit was captured at claim time. If the PR branch
        // advanced during the run, the *current* head (and its merge-base /
        // baseline / diff link) no longer matches the metrics we have — comparing
        // them would mislead. Degrade to absolute-only rather than compare
        // across commits.
        if pr.head.sha != self.job.commit {
            tracing::debug!(
                job_id = %self.job.id,
                benchmarked = %self.job.commit,
                current_head = %pr.head.sha,
                "vs-baseline: PR head moved during the run; absolute-only",
            );
            return None;
        }

        // Merge-base (fork point), cross-fork-safe head form.
        let merge_base = match self
            .gh
            .compare_commits(
                self.job.installation_id,
                &self.job.repository,
                &pr.base.branch,
                &pr.head.repo.owner,
                &pr.head.branch,
            )
            .await
        {
            Ok(Some(mb)) => mb,
            Ok(None) => return None,
            Err(e) => {
                tracing::debug!(job_id = %self.job.id, error = ?e, "vs-baseline: compare_commits failed (absolute-only)");
                return None;
            }
        };

        // The matching baseline (exact → nearest-before), repo resolved.
        let baseline = match self
            .jobs
            .find_baseline(
                self.job.id,
                &merge_base.hash,
                &pr.base.branch,
                merge_base.committed_at,
                workload_key,
            )
            .await
        {
            Ok(Some(b)) => b,
            Ok(None) => return None,
            Err(e) => {
                tracing::debug!(job_id = %self.job.id, error = ?e, "vs-baseline: find_baseline failed (absolute-only)");
                return None;
            }
        };

        // This run's metric (the same extraction we persist), from run.json.
        let pr_metric = self
            .pr_metric(summary)
            .await?;
        let comparison = compare(
            &pr_metric,
            &baseline.metric,
            self.config
                .reporting
                .noise_cv_pct,
        )?;

        Some(BaselineComparison {
            comparison,
            baseline_repo: baseline.repository,
            baseline_commit: baseline.commit,
            baseline_ref: baseline.git_ref_display,
            baseline_committed_at: baseline.committed_at,
            selection: baseline.selection,
            base_repo: self.job.repository.clone(),
            head_owner: pr.head.repo.owner,
            head_ref: pr.head.branch,
        })
    }

    /// Best-effort comparison summary for an explicitly grouped multi-variant
    /// benchmark. Computed only after the current terminal state
    /// has been persisted, so the final run's promoted metric is included.
    async fn group_comparison(&self) -> Option<GroupComparison> {
        if self
            .job
            .group_requested_run_count
            <= self.job.requested_run_count
        {
            return None;
        }
        if self.job.group_run_index + 1
            < self
                .job
                .group_requested_run_count
        {
            return None;
        }

        let specs = match self
            .jobs
            .benchmark_group_specs(self.job.benchmark_group_id)
            .await
        {
            Ok(specs) if specs.len() > 1 => specs,
            Ok(_) => return None,
            Err(e) => {
                tracing::debug!(job_id = %self.job.id, error = ?e, "group comparison: loading specs failed");
                return None;
            }
        };

        let mut metrics_by_spec = HashMap::with_capacity(specs.len());
        for spec in &specs {
            match self
                .jobs
                .benchmark_run_metrics(spec.id)
                .await
            {
                Ok(metrics) => {
                    metrics_by_spec.insert(spec.id, metrics);
                }
                Err(e) => {
                    tracing::debug!(
                        job_id = %self.job.id,
                        benchmark_spec_id = %spec.id,
                        error = ?e,
                        "group comparison: loading metrics failed",
                    );
                    return None;
                }
            }
        }

        GroupComparison::from_specs(
            &specs,
            &metrics_by_spec,
            self.config
                .reporting
                .noise_cv_pct,
        )
    }

    /// This run's metric, parsed from the archived `run.json` referenced in the
    /// forensics `summary` — the same `metric_from_run` the persistence path
    /// uses, so the comment's delta is on the numbers we store.
    async fn pr_metric(&self, summary: &serde_json::Value) -> Option<sbgh_core::models::JobMetric> {
        let key = summary
            .get("run_json_archived_path")?
            .as_str()?;
        let path = self
            .artifact_store
            .get(key)
            .await
            .ok()?;
        let bytes = std::fs::read(path).ok()?;
        let run = RunResult::from_bytes(&bytes)?;
        crate::job_source::metric_from_run(self.job.id, &run)
    }

    #[cfg(test)]
    async fn finish(
        &self,
        surface: &dyn ReportSurface,
        terminal: Terminal,
    ) -> Option<anyhow::Error> {
        match terminal {
            Terminal::Completed { summary, .. } => {
                tracing::info!(
                    job_id = %self.job.id,
                    repo = %self.job.repository,
                    commit = %self.job.commit,
                    task_kind = ?self.job.task_kind,
                    build_target = ?self.job.build_target,
                    "job completed; persisting result",
                );
                if let Err(e) = self
                    .jobs
                    .complete(&self.job, &summary)
                    .await
                {
                    tracing::error!(job_id = %self.job.id, error = ?e, "persisting completed result failed");
                    return Some(e);
                }
                tracing::info!(job_id = %self.job.id, "job result persisted (status=completed)");
                let group_comparison = self.group_comparison().await;
                if let Some(comparison) = &group_comparison {
                    tracing::debug!(
                        job_id = %self.job.id,
                        variants = comparison.variants.len(),
                        "group comparison summary computed",
                    );
                }
                let comparison = self
                    .baseline_comparison(&summary)
                    .await;
                log_nonfatal_projection(
                    self.job.id,
                    surface
                        .completed(CompletionReport {
                            summary: &summary,
                            baseline_comparison: comparison.as_ref(),
                            group_comparison: group_comparison.as_ref(),
                        })
                        .await,
                );
                None
            }
            Terminal::Failed { error, summary } => {
                // VM-side or libvirt-side failure (virsh start refused, VM
                // powered off before phase=done, timeout, etc.).
                tracing::error!(
                    job_id = %self.job.id,
                    task_kind = ?self.job.task_kind,
                    build_target = ?self.job.build_target,
                    finish_reason = ?summary.get("finish_reason"),
                    last_phase = ?summary.get("last_phase"),
                    error = %error,
                    "job failed",
                );
                if let Err(e) = self
                    .jobs
                    .fail(&self.job, &error, Some(&summary))
                    .await
                {
                    tracing::error!(job_id = %self.job.id, error = ?e, "persisting failed result failed");
                    return Some(e);
                }
                log_nonfatal_projection(self.job.id, surface.failed(&error).await);
                None
            }
            Terminal::SetupError { error } => {
                // The run couldn't start. Record the forensics-less failure and
                // back off the poll loop (matches the prior `return Err`).
                tracing::error!(job_id = %self.job.id, error = %error, "recipe returned setup error");
                let _ = self
                    .jobs
                    .fail(&self.job, &error, None)
                    .await;
                log_nonfatal_projection(self.job.id, surface.failed(&error).await);
                Some(anyhow::anyhow!("{error}"))
            }
            Terminal::Aborted => {
                // The run already tore its host artifacts down. Record a
                // terminal cancellation:
                // a deliberately-stopped run, not a failure: status `cancelled`
                // + a neutral-gray check, re-triggerable.
                let msg = "aborted by shutdown";
                tracing::warn!(job_id = %self.job.id, "{msg}");
                if let Err(e) = self
                    .jobs
                    .cancel(&self.job, msg)
                    .await
                {
                    // The terminal write didn't land — the job may still be
                    // `running`. Surface it loudly + back off rather than
                    // reporting the job as cleanly handled.
                    tracing::error!(job_id = %self.job.id, error = ?e, "persisting cancelled terminal failed");
                    return Some(e);
                }
                log_nonfatal_projection(self.job.id, surface.cancelled(msg).await);
                None
            }
        }
    }

    /// Resolve the job's commit when empty: a PR job → the PR head SHA; a Slack
    /// ad-hoc job → its rev (`[slack].default_rev` / `--rev`); a `tag_created`
    /// baseline → the tag ref. `branch_push` carries its commit from enqueue
    /// (non-empty → no-op). Mutates `self.job.commit`.
    #[cfg(test)]
    async fn resolve_commit(&mut self) -> anyhow::Result<Option<ResolvedCommit>> {
        if !self.job.commit.is_empty() {
            return Ok(None);
        }
        if let ProgressTarget::PullRequest { pr_number, .. } = self.job.progress {
            let sha = self
                .gh
                .pr_head_sha(self.job.installation_id, &self.job.repository, pr_number as u64)
                .await?;
            tracing::info!(job_id = %self.job.id, pr_number, sha = %sha, "resolved PR head SHA at claim time");
            self.job.commit = sha.clone();
            return Ok(Some(ResolvedCommit { hash: sha, committed_at: None }));
        }
        if let ProgressTarget::Slack { .. } = self.job.progress {
            // A Slack ad-hoc job's rev is a branch/tag/SHA — resolve the **bare**
            // ref, which GitHub's `commits/{ref}` accepts for any of those forms
            // (a tag job qualifies `tags/<name>` because it KNOWS it's a tag; a
            // Slack rev's kind is unknown, so the bare ref is correct).
            let resolved = self
                .gh
                .resolve_commit(
                    self.job.installation_id,
                    &self.job.repository,
                    &self.job.git_ref_display,
                )
                .await?;
            tracing::info!(job_id = %self.job.id, rev = %self.job.git_ref_display, commit = %resolved.hash, "resolved Slack rev to commit at claim time");
            self.job.commit = resolved.hash.clone();
            return Ok(Some(resolved));
        }
        if self.job.git_ref_kind == GitRefKind::Tag {
            // Qualified `tags/<name>` so it unambiguously targets the tag.
            let tag_ref = format!("tags/{}", self.job.git_ref_display);
            let resolved = self
                .gh
                .resolve_commit(self.job.installation_id, &self.job.repository, &tag_ref)
                .await?;
            tracing::info!(job_id = %self.job.id, tag = %self.job.git_ref_display, commit = %resolved.hash, "resolved tag to commit at claim time");
            self.job.commit = resolved.hash.clone();
            return Ok(Some(resolved));
        }
        tracing::warn!(
            job_id = %self.job.id,
            git_ref = %self.job.git_ref_display,
            "job has no resolved commit; the empty-commit guard will fail it",
        );
        Ok(None)
    }

    /// Create the Check Run (after the commit is resolved) and/or post the
    /// initial PR comment, per `[reporting]`. NON-FATAL. Mutates
    /// `self.job.progress` with the created ids.
    async fn ensure_reporting(&mut self) {
        match self.job.progress.clone() {
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
                    && let Some((id, url)) = self.ensure_check().await
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
                        .post_initial_comment(pr_number, check_run_url.as_deref())
                        .await
                {
                    comment_id = Some(id);
                }
                self.job.progress = ProgressTarget::PullRequest {
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
                    && let Some((id, _)) = self.ensure_check().await
                {
                    check_run_id = Some(id);
                }
                self.job.progress = ProgressTarget::CommitCheck { check_run_id };
            }
            // Slack jobs have no GitHub surface to ensure — the request
            // reaction is added by the connector at enqueue, and the threaded
            // result is posted at terminal (connector slice). Nothing here.
            ProgressTarget::Slack { .. } => {}
            // Build-only and silent jobs have no GitHub surface.
            ProgressTarget::Silent => {}
        }
    }

    /// Reconcile-or-create the in-progress Check Run on `job.commit`, persist
    /// its id+url, and return them. Non-fatal (returns `None` on error). The
    /// reconcile (when the App id resolved via `GET /app`) reuses a run created
    /// just before a crash rather than duplicating it.
    async fn ensure_check(&self) -> Option<(i64, Option<String>)> {
        let external_id = self.job.id.to_string();
        if let Some(app_id) = resolved_app_id(&self.app_id, self.gh.as_ref()).await {
            match self
                .gh
                .find_check_run_by_external_id(
                    self.job.installation_id,
                    &self.job.repository,
                    &self.job.commit,
                    CHECK_NAME,
                    app_id,
                    &external_id,
                )
                .await
            {
                Ok(Some(existing)) => {
                    self.persist_check_id(existing.id, existing.html_url.as_deref())
                        .await;
                    tracing::info!(job_id = %self.job.id, check_run_id = existing.id, "reconciled existing check run");
                    return Some((existing.id, existing.html_url));
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(job_id = %self.job.id, error = ?e, "check reconcile lookup failed; creating")
                }
            }
        }
        let output = CheckRunOutput {
            title: format!("benchmark {}", self.job.id),
            summary: format!("Benchmarking commit `{}`…", self.job.commit),
            text: None,
        };
        match self
            .gh
            .create_check_run(
                self.job.installation_id,
                &self.job.repository,
                &self.job.commit,
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
                self.persist_check_id(posted.id, posted.html_url.as_deref())
                    .await;
                tracing::info!(job_id = %self.job.id, check_run_id = posted.id, "created check run");
                Some((posted.id, posted.html_url))
            }
            Err(e) => {
                tracing::warn!(job_id = %self.job.id, error = ?e, "create check run failed (non-fatal)");
                None
            }
        }
    }

    /// Persist the check run identity (`check_run_created` event) the reclaim
    /// path reads back. Non-fatal, but a failure is logged **loudly**: the
    /// in-memory job keeps the id (so this run still updates the right check),
    /// but the next claim won't see it and could create a DUPLICATE check.
    async fn persist_check_id(&self, id: i64, url: Option<&str>) {
        if let Err(e) = self
            .jobs
            .set_check_run(&self.job, id, url)
            .await
        {
            tracing::warn!(
                job_id = %self.job.id,
                check_run_id = id,
                error = ?e,
                "PERSISTING check identity failed — a re-claim may duplicate the check (non-fatal)",
            );
        }
    }

    /// Reconcile or post the initial PR comment and persist its id. Lookup
    /// failure is not permission to create: a later claim may retry safely.
    async fn post_initial_comment(&self, pr_number: i64, check_link: Option<&str>) -> Option<i64> {
        let marker = pr_report_marker(self.job.benchmark_group_id);
        let link = check_link
            .map(|u| format!(" — [view check ↗]({u})"))
            .unwrap_or_default();
        let body = marked_pr_comment(
            self.job.benchmark_group_id,
            &format!(
                ":construction: starting benchmark `{id}` (commit `{sha}`)…{link}",
                id = self.job.id,
                sha = self.job.commit,
            ),
        );
        let Some(app_id) = resolved_app_id(&self.app_id, self.gh.as_ref()).await else {
            tracing::warn!(
                job_id = %self.job.id,
                "PR comment reconciliation skipped because App identity is unavailable"
            );
            return None;
        };
        match self
            .gh
            .find_pr_comment_by_marker(
                self.job.installation_id,
                &self.job.repository,
                pr_number as u64,
                app_id,
                &marker,
            )
            .await
        {
            Ok(Some(existing)) => {
                self.persist_comment_id(existing.id)
                    .await;
                if let Err(error) = self
                    .gh
                    .update_pr_comment(
                        self.job.installation_id,
                        &self.job.repository,
                        existing.id,
                        &body,
                    )
                    .await
                {
                    tracing::warn!(
                        job_id = %self.job.id,
                        comment_id = existing.id,
                        ?error,
                        "reconciled PR comment could not be refreshed"
                    );
                }
                return Some(existing.id);
            }
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(
                    job_id = %self.job.id,
                    ?error,
                    "PR comment reconcile lookup failed; refusing to create blindly"
                );
                return None;
            }
        }
        match self
            .gh
            .create_pr_comment(
                self.job.installation_id,
                &self.job.repository,
                pr_number as u64,
                &body,
            )
            .await
        {
            Ok(posted) => {
                self.persist_comment_id(posted.id)
                    .await;
                tracing::info!(job_id = %self.job.id, pr_number, comment_id = posted.id, "posted initial PR comment");
                Some(posted.id)
            }
            Err(e) => {
                tracing::warn!(job_id = %self.job.id, error = ?e, "post initial PR comment failed (non-fatal)");
                None
            }
        }
    }

    async fn persist_comment_id(&self, comment_id: i64) {
        if let Err(error) = self
            .jobs
            .set_comment_id(&self.job, comment_id)
            .await
        {
            tracing::warn!(
                job_id = %self.job.id,
                comment_id,
                ?error,
                "persisting reconciled PR comment identity failed; marker lookup will retry",
            );
        }
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
    use sbgh_core::db::BenchmarkRunMetric;
    use sbgh_core::models::{BenchmarkSpec, BuildTarget, GitRefKind, JobMetric, TaskKind};
    use sbgh_github::test_support::{FakeCall, FakeGitHub};
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::*;
    use crate::job_source::BaselineRef;
    use sbgh_slack::{COMPLETED_REACTION, RUNNING_REACTION};

    fn job_with(progress: ProgressTarget) -> RunnableJob {
        RunnableJob {
            id: Uuid::new_v4(),
            benchmark_group_id: Uuid::new_v4(),
            benchmark_spec_id: Uuid::new_v4(),
            benchmark_run_index: 0,
            requested_run_count: 1,
            group_requested_run_count: 1,
            group_run_index: 0,
            baseline_calibration_id: None,
            group_artifact_prefix: Uuid::new_v4().to_string(),
            repository: "acme/widgets".into(),
            commit: "abc123".into(),
            git_ref_display: "feature".into(),
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

    /// A minimal [`RunnableJobStore`] that records which lifecycle methods the
    /// reporter called — enough to prove the channel-close terminal-fail path.
    /// `baseline` is the staged `find_baseline` response (roadmap-v7).
    #[derive(Default)]
    struct RecordingStore {
        calls: StdMutex<Vec<&'static str>>,
        baseline: Option<BaselineRef>,
        specs: Vec<BenchmarkSpec>,
        metrics: HashMap<Uuid, Vec<BenchmarkRunMetric>>,
    }

    impl RecordingStore {
        fn rec(&self, m: &'static str) {
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
        fn with_group(
            specs: Vec<BenchmarkSpec>,
            metrics: HashMap<Uuid, Vec<BenchmarkRunMetric>>,
        ) -> Self {
            Self {
                specs,
                metrics,
                ..Self::default()
            }
        }
    }

    #[async_trait]
    impl RunnableJobStore for RecordingStore {
        async fn claim_next(&self) -> anyhow::Result<Option<RunnableJob>> {
            Ok(None)
        }
        async fn load_runnable(&self, _job_id: uuid::Uuid) -> anyhow::Result<Option<RunnableJob>> {
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
            self.rec("start_running");
            Ok(())
        }
        async fn sweep_stuck_claims(&self, _lease: chrono::Duration) -> anyhow::Result<u64> {
            Ok(0)
        }
        async fn complete(
            &self,
            _job: &RunnableJob,
            _summary: &serde_json::Value,
        ) -> anyhow::Result<()> {
            self.rec("complete");
            Ok(())
        }
        async fn find_baseline(
            &self,
            _subject_job_id: Uuid,
            _merge_base_sha: &str,
            _base_ref: &str,
            _at: Option<chrono::DateTime<chrono::Utc>>,
            _workload_key: &str,
        ) -> anyhow::Result<Option<BaselineRef>> {
            Ok(self.baseline.clone())
        }
        async fn benchmark_run_metrics(
            &self,
            benchmark_spec_id: Uuid,
        ) -> anyhow::Result<Vec<BenchmarkRunMetric>> {
            Ok(self
                .metrics
                .get(&benchmark_spec_id)
                .cloned()
                .unwrap_or_default())
        }
        async fn benchmark_group_specs(
            &self,
            _benchmark_group_id: Uuid,
        ) -> anyhow::Result<Vec<BenchmarkSpec>> {
            Ok(self.specs.clone())
        }
        async fn fail(
            &self,
            _job: &RunnableJob,
            _error: &str,
            _summary: Option<&serde_json::Value>,
        ) -> anyhow::Result<()> {
            self.rec("fail");
            Ok(())
        }
        async fn cancel(&self, _job: &RunnableJob, _remark: &str) -> anyhow::Result<()> {
            self.rec("cancel");
            Ok(())
        }
        async fn set_comment_id(&self, _job: &RunnableJob, _id: i64) -> anyhow::Result<()> {
            Ok(())
        }
        async fn set_check_run(
            &self,
            _job: &RunnableJob,
            _id: i64,
            _url: Option<&str>,
        ) -> anyhow::Result<()> {
            Ok(())
        }
        async fn set_plan_message_ts(
            &self,
            _job: &RunnableJob,
            _message_ts: &str,
        ) -> anyhow::Result<()> {
            self.rec("set_plan_message_ts");
            Ok(())
        }
        async fn running_job_ids(&self) -> anyhow::Result<Vec<uuid::Uuid>> {
            Ok(vec![])
        }
        async fn cancel_orphan(&self, _job_id: uuid::Uuid, _remark: &str) -> anyhow::Result<bool> {
            Ok(false)
        }
    }

    /// A `baseline_report = none` config so the reporter's `prepare` touches no
    /// GitHub surfaces (a `CommitCheck` job wants no check) — keeping the
    /// channel-close test focused on the lifecycle, not reporting.
    fn no_report_config(tmp: &TempDir) -> DaemonConfig {
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
                min_data_free_percent: 5.0,
                min_metadata_free_percent: 5.0,
            },
            stacks_bench: StacksBenchConfig { default_args: String::new() },
            api: ApiConfig {
                listen: vec!["127.0.0.1:8787".into()],
                cookie_path: "/tmp/sbgh-test.cookie".into(),
                ingest_token: None,
            },
            reporting: ReportingConfig {
                pr_report: PrReport::Comment,
                baseline_report: BaselineReport::None,
                noise_cv_pct: None,
            },
            runner: RunnerConfig {
                max_concurrent_jobs: 1,
                max_clean_repetitions: 5,
                max_variants: 2,
                max_comparison_lifecycles: 10,
                cpu_sets: vec![],
                host_cpus: None,
            },
            artifacts: Default::default(),
            slack: Default::default(),
            llm: Default::default(),
        }
    }

    /// If the worker is gated to run but the event channel closes **without** a
    /// `Finished` (the worker panicked/exited), the reporter must terminal-fail
    /// the job — not return `Ok` and leave it stuck in `running`.
    #[tokio::test]
    async fn abnormal_channel_close_terminal_fails_the_job() {
        let tmp = TempDir::new().unwrap();
        let store = Arc::new(RecordingStore::default());
        let reporter = Reporter::new(
            Arc::new(no_report_config(&tmp)),
            store.clone(),
            Arc::new(FakeGitHub::new()),
            Arc::new(OnceCell::new()),
            None,
            Default::default(),
            // Pre-resolved commit → prepare's resolve is a no-op; `CommitCheck`
            // + `baseline_report = none` → prepare creates no surfaces.
            job_with(ProgressTarget::CommitCheck { check_run_id: None }),
        );

        // Close the event channel up front: after the reporter gates the worker,
        // its drain loop sees the stream end with no `Finished`.
        let (events_tx, events_rx) = mpsc::channel(4);
        drop(events_tx);
        // Hold the prepared receiver so the reporter's `Run` hand-off succeeds.
        let (prepared_tx, _prepared_rx) = oneshot::channel();

        let result = reporter
            .run(events_rx, prepared_tx)
            .await;

        assert!(result.is_err(), "abnormal close after gating must Err (backoff)");
        let calls = store.calls();
        assert!(calls.contains(&"start_running"), "the job did reach `running`");
        assert!(
            calls.contains(&"fail"),
            "the orphaned `running` job must be terminal-failed, got {calls:?}"
        );
    }

    /// Phase 4C: a `Terminal::Aborted` (operator shutdown) is recorded as a
    /// **cancellation**, not a failure — the store sees `cancel` (not `fail`)
    /// and the Check Run is concluded `cancelled` (neutral-gray), not
    /// `failure`.
    #[tokio::test]
    async fn aborted_terminal_cancels_the_job_and_check() {
        let tmp = TempDir::new().unwrap();
        let store = Arc::new(RecordingStore::default());
        let gh = Arc::new(FakeGitHub::new());
        let reporter = Reporter::new(
            Arc::new(no_report_config(&tmp)),
            store.clone(),
            gh.clone(),
            Arc::new(OnceCell::new()),
            None,
            Default::default(),
            // Pre-resolved commit; a check id already on the job (as if read
            // back on re-claim) so the cancelled conclusion has a target.
            job_with(ProgressTarget::CommitCheck { check_run_id: Some(900) }),
        );

        // Deliver a single `Finished(Aborted)`, then close the channel.
        let (events_tx, events_rx) = mpsc::channel(4);
        events_tx
            .send(WorkerEvent::Finished(Terminal::Aborted))
            .await
            .unwrap();
        drop(events_tx);
        let (prepared_tx, _prepared_rx) = oneshot::channel();

        let result = reporter
            .run(events_rx, prepared_tx)
            .await;
        assert!(result.is_ok(), "a clean abort terminalizes without backoff");

        // Store recorded `cancel`, never `fail`.
        let calls = store.calls();
        assert!(calls.contains(&"cancel"), "abort records a cancellation, got {calls:?}");
        assert!(!calls.contains(&"fail"), "abort must NOT record a failure, got {calls:?}");

        // The check was concluded `cancelled` (neutral-gray), not `failure`.
        let concluded = gh
            .calls()
            .into_iter()
            .find_map(|c| match c {
                FakeCall::UpdateCheckRun { check_run_id: 900, state, .. } => Some(state),
                _ => None,
            })
            .expect("the check was concluded");
        assert_eq!(
            concluded,
            sbgh_github::CheckRunState::Completed(sbgh_github::CheckRunConclusion::Cancelled),
        );
    }

    // ─── Slack snapshot wiring ───

    /// Counts the Slack calls the reporter's snapshot publisher makes through `run`.
    #[derive(Default)]
    struct TimelineFakeSlack {
        posts: StdMutex<usize>,
        updates: StdMutex<usize>,
        update_texts: StdMutex<Vec<String>>,
        added: StdMutex<Vec<String>>,
        messages: StdMutex<Vec<sbgh_slack::FoundMessage>>,
    }

    #[async_trait]
    impl SlackClient for TimelineFakeSlack {
        async fn post_ephemeral(&self, _c: &str, _u: &str, _t: &str) -> sbgh_slack::Result<()> {
            Ok(())
        }
        async fn post_message(
            &self,
            _target: &sbgh_slack::SlackMessageTarget,
            text: &str,
            _identity: &sbgh_slack::ReportingIdentity,
            snapshot_version: u64,
        ) -> sbgh_slack::Result<String> {
            *self.posts.lock().unwrap() += 1;
            self.messages
                .lock()
                .unwrap()
                .push(sbgh_slack::FoundMessage {
                    ts: "PLAN_TS".into(),
                    text: text.into(),
                    snapshot_version,
                });
            Ok("PLAN_TS".into())
        }
        async fn update_message(
            &self,
            _c: &str,
            _ts: &str,
            text: &str,
            _identity: &sbgh_slack::ReportingIdentity,
            _snapshot_version: u64,
        ) -> sbgh_slack::Result<()> {
            *self.updates.lock().unwrap() += 1;
            self.update_texts
                .lock()
                .unwrap()
                .push(text.into());
            Ok(())
        }
        async fn find_messages(
            &self,
            _target: &sbgh_slack::SlackMessageTarget,
            _identity: &sbgh_slack::ReportingIdentity,
        ) -> sbgh_slack::Result<Vec<sbgh_slack::FoundMessage>> {
            Ok(self
                .messages
                .lock()
                .unwrap()
                .clone())
        }
        async fn add_reaction(&self, _c: &str, _ts: &str, r: &str) -> sbgh_slack::Result<()> {
            self.added
                .lock()
                .unwrap()
                .push(r.into());
            Ok(())
        }
        async fn remove_reaction(&self, _c: &str, _ts: &str, _r: &str) -> sbgh_slack::Result<()> {
            Ok(())
        }
    }

    /// A Slack job driven through `run` posts one snapshot, updates it through
    /// phase and completion, and preserves the reaction lifecycle.
    #[tokio::test]
    async fn slack_job_drives_the_snapshot_through_run() {
        let tmp = TempDir::new().unwrap();
        let store = Arc::new(RecordingStore::default());
        let slack = Arc::new(TimelineFakeSlack::default());
        let reporter = Reporter::new(
            Arc::new(no_report_config(&tmp)),
            store.clone(),
            Arc::new(FakeGitHub::new()),
            Arc::new(OnceCell::new()),
            Some(slack.clone()),
            Default::default(),
            job_with(ProgressTarget::Slack {
                channel: "C1".into(),
                message_ts: "REQ".into(),
                reporting_identity: "0".repeat(64),
                plan_message_ts: None,
            }),
        );

        // A phase event advances the snapshot, then a clean completion.
        let (events_tx, events_rx) = mpsc::channel(4);
        events_tx
            .send(WorkerEvent::Phase {
                label: sbgh_driver::PhaseLabel::new("running", false),
                elapsed: std::time::Duration::ZERO,
            })
            .await
            .unwrap();
        events_tx
            .send(WorkerEvent::Finished(Terminal::Completed {
                summary: serde_json::json!({}),
                block_validation: None,
            }))
            .await
            .unwrap();
        drop(events_tx);
        let (prepared_tx, _prepared_rx) = oneshot::channel();

        reporter
            .run(events_rx, prepared_tx)
            .await
            .unwrap();

        // started() posts once; phase(running) + completed update the same ts.
        assert_eq!(*slack.posts.lock().unwrap(), 1, "the canonical message is posted exactly once");
        assert!(*slack.updates.lock().unwrap() >= 2, "phase + terminal snapshot updates");
        assert_eq!(
            *slack.added.lock().unwrap(),
            vec![RUNNING_REACTION.to_string(), COMPLETED_REACTION.to_string()],
            "⏳ → 🚀 (run 0 started) → ✅ (completed)",
        );
        // The posted ts was persisted for resume-on-reclaim.
        assert!(
            store
                .calls()
                .contains(&"set_plan_message_ts"),
            "{:?}",
            store.calls()
        );
    }

    // ─── roadmap-v7: vs-baseline comparison wiring ───

    fn baseline_metric(exec: i64, commit: i64) -> sbgh_core::models::JobMetric {
        sbgh_core::models::JobMetric {
            job_id: Uuid::nil(),
            envelope_duration_us: 0,
            replay_duration_us: 0,
            total_duration_us: 0,
            setup_duration_us: 0,
            execution_duration_us: exec,
            commit_duration_us: commit,
            clarity_runtime: 0,
            transactions: 0,
            read_length: 0,
            write_length: 0,
            measured_blocks: 5000,
            warmup_blocks: 1000,
            created_at: chrono::DateTime::from_timestamp(0, 0).unwrap(),
        }
    }

    fn comparison_spec(
        group_id: Uuid,
        index: i32,
        rev: &str,
        calibration_id: i64,
    ) -> BenchmarkSpec {
        BenchmarkSpec {
            id: Uuid::from_u128((index + 1) as u128),
            benchmark_group_id: group_id,
            spec_index: index,
            requested_run_count: 1,
            baseline_calibration_id: Some(calibration_id),
            github_repo_id: 10,
            task_kind: TaskKind::Benchmark,
            build_target: BuildTarget::StacksBench,
            git_ref_kind: GitRefKind::Branch,
            git_ref_display: rev.into(),
            git_commit_hash: Some(format!("{rev}-sha")),
            git_committed_at: None,
            workload_key: Some("wk".into()),
            created_at: chrono::DateTime::from_timestamp(0, 0).unwrap(),
            updated_at: chrono::DateTime::from_timestamp(0, 0).unwrap(),
        }
    }

    fn run_metric(index: i32, metric: JobMetric) -> BenchmarkRunMetric {
        BenchmarkRunMetric {
            benchmark_run_index: index,
            metric,
        }
    }

    fn side(owner: &str, name: &str, branch: &str, sha: &str) -> sbgh_github::PullRequestSide {
        sbgh_github::PullRequestSide {
            repo: sbgh_github::RepoRef {
                id: 1,
                owner: owner.into(),
                name: name.into(),
            },
            sha: sha.into(),
            branch: branch.into(),
        }
    }

    /// A completed PR run with a comparable baseline produces a vs-baseline
    /// delta: get_pull_request → compare_commits → find_baseline → compare.
    #[tokio::test]
    async fn baseline_comparison_renders_delta_for_pr_run() {
        let tmp = TempDir::new().unwrap();
        // This run's combined Exec+Commit = 718_000 + 300_000 = 1_018_000µs.
        // Place run.json under the config's archive root at the summary key
        // (Decision 0002 — pr_metric resolves it via ArtifactStore::get).
        let run_json_key = "job/run.json";
        let run_json = tmp
            .path()
            .join("archive")
            .join(run_json_key);
        std::fs::create_dir_all(run_json.parent().unwrap()).unwrap();
        std::fs::write(
            &run_json,
            r#"{"success":true,"duration_secs":1100.0,"data":{"measured_blocks":5000,
               "warmup_blocks":1000,"duration_secs":1000.0,"summary":{"total_duration_us":1100000000,
               "setup_duration_us":50000000,"execution_duration_us":718000,"commit_duration_us":300000,
               "transactions":12345,"clarity_runtime":99,"write_length":100,"read_length":200}}}"#,
        )
        .unwrap();

        // Baseline combined = 1_000_000µs → this run is +1.8%.
        let store = Arc::new(RecordingStore {
            calls: Default::default(),
            baseline: Some(BaselineRef {
                metric: baseline_metric(700_000, 300_000),
                repository: "stacks-network/stacks-core".into(),
                commit: "basecommit0".into(),
                git_ref_display: "develop".into(),
                committed_at: chrono::DateTime::from_timestamp(1_716_000_000, 0),
                selection: sbgh_core::db::BaselineSelection::NearestBefore,
            }),
            ..Default::default()
        });

        let gh = Arc::new(FakeGitHub::new());
        // The PR head SHA matches the benchmarked commit (`job_with` → "abc123").
        gh.set_pull_request(
            "cylewitruk/stacks-core",
            7,
            side("cylewitruk", "stacks-core", "develop", "basesha"),
            side("cylewitruk", "stacks-core", "feat/foo", "abc123"),
        );
        gh.set_merge_base(
            "cylewitruk/stacks-core",
            "develop",
            "cylewitruk",
            "feat/foo",
            "mergebase0",
            chrono::DateTime::from_timestamp(1_715_000_000, 0),
        );

        let mut config = no_report_config(&tmp);
        config.reporting.noise_cv_pct = Some(0.37);

        let mut job = job_with(ProgressTarget::PullRequest {
            pr_number: 7,
            comment_id: Some(11),
            check_run_id: Some(22),
            check_run_url: None,
        });
        job.repository = "cylewitruk/stacks-core".into();
        job.workload_key = Some("wk1".into());

        let reporter = Reporter::new(
            Arc::new(config),
            store,
            gh,
            Arc::new(OnceCell::new()),
            None,
            Default::default(),
            job,
        );

        let summary = serde_json::json!({ "run_json_archived_path": run_json_key });
        let c = reporter
            .baseline_comparison(&summary)
            .await
            .expect("a comparable baseline → Some");

        assert!((c.comparison.delta_pct - 1.8).abs() < 0.01, "delta {}", c.comparison.delta_pct);
        assert_eq!(c.comparison.verdict, crate::comparison::Verdict::Strong);
        assert!(c.comparison.sigma.unwrap() > 3.0);
        assert_eq!(c.baseline_repo, "stacks-network/stacks-core");
        assert_eq!(c.baseline_commit, "basecommit0");
        assert_eq!(c.baseline_ref, "develop");
        assert_eq!(c.base_repo, "cylewitruk/stacks-core");
        assert_eq!(c.head_owner, "cylewitruk");
        assert_eq!(c.head_ref, "feat/foo");
        assert_eq!(c.selection, sbgh_core::db::BaselineSelection::NearestBefore);
    }

    #[tokio::test]
    async fn group_comparison_loads_specs_and_metrics_for_final_group_run() {
        let tmp = TempDir::new().unwrap();
        let group_id = Uuid::new_v4();
        let baseline = comparison_spec(group_id, 0, "release/3.4.0.0.2", 11);
        let candidate = comparison_spec(group_id, 1, "release/3.4.0.0.3", 22);
        let store = Arc::new(RecordingStore::with_group(
            vec![baseline.clone(), candidate.clone()],
            HashMap::from([
                (baseline.id, vec![run_metric(0, baseline_metric(700_000, 300_000))]),
                (candidate.id, vec![run_metric(0, baseline_metric(718_000, 300_000))]),
            ]),
        ));
        let mut job = job_with(ProgressTarget::Slack {
            channel: "C1".into(),
            message_ts: "REQ".into(),
            reporting_identity: "0".repeat(64),
            plan_message_ts: Some("PLAN".into()),
        });
        job.benchmark_group_id = group_id;
        job.benchmark_spec_id = candidate.id;
        job.benchmark_run_index = 0;
        job.requested_run_count = 1;
        job.group_run_index = 1;
        job.group_requested_run_count = 2;

        let reporter = Reporter::new(
            Arc::new(no_report_config(&tmp)),
            store,
            Arc::new(FakeGitHub::new()),
            Arc::new(OnceCell::new()),
            None,
            Default::default(),
            job,
        );

        let comparison = reporter
            .group_comparison()
            .await
            .expect("final comparison run computes group summary");

        assert_eq!(
            comparison
                .baseline
                .ref_display,
            "release/3.4.0.0.2"
        );
        assert_eq!(
            comparison
                .baseline
                .baseline_calibration_id,
            Some(11)
        );
        assert_eq!(comparison.variants.len(), 1);
        assert_eq!(
            comparison.variants[0]
                .variant
                .ref_display,
            "release/3.4.0.0.3"
        );
        assert_eq!(
            comparison.variants[0]
                .variant
                .baseline_calibration_id,
            Some(22)
        );
        assert!(
            (comparison.variants[0]
                .comparison
                .as_ref()
                .unwrap()
                .delta_pct
                - 1.8)
                .abs()
                < 1e-9
        );

        let slack = Arc::new(TimelineFakeSlack::default());
        let slack_client: Arc<dyn SlackClient> = slack.clone();
        let sessions = Arc::new(SlackSessionRegistry::new());
        let surface = build_report_surface(
            reporter.gh.clone(),
            reporter.jobs.clone(),
            reporter
                .artifact_store
                .clone(),
            Some(&slack_client),
            &sessions,
            &reporter.job,
        );
        let summary = serde_json::json!({});
        surface
            .completed(CompletionReport {
                summary: &summary,
                baseline_comparison: None,
                group_comparison: Some(&comparison),
            })
            .await
            .unwrap();

        let updates = slack
            .update_texts
            .lock()
            .unwrap();
        let rendered = updates
            .last()
            .expect("comparison updates the shared message");
        assert!(rendered.contains("release/3.4.0.0.2"), "{rendered}");
        assert!(rendered.contains("release/3.4.0.0.3"), "{rendered}");
        assert!(rendered.contains("+1.80% slower"), "{rendered}");
        assert!(rendered.contains("Run: 2/2"), "{rendered}");
    }

    /// A baseline (non-PR) job has no fork-point → no comparison.
    #[tokio::test]
    async fn baseline_comparison_none_for_non_pr_job() {
        let tmp = TempDir::new().unwrap();
        let reporter = Reporter::new(
            Arc::new(no_report_config(&tmp)),
            Arc::new(RecordingStore::default()),
            Arc::new(FakeGitHub::new()),
            Arc::new(OnceCell::new()),
            None,
            Default::default(),
            job_with(ProgressTarget::CommitCheck { check_run_id: Some(1) }),
        );
        assert!(
            reporter
                .baseline_comparison(&serde_json::json!({}))
                .await
                .is_none(),
        );
    }

    /// Codex Phase-3: if the PR branch advanced during the run (current head
    /// SHA ≠ the benchmarked commit), degrade to absolute-only — never
    /// compare the moved head's merge-base against the old commit's
    /// metrics.
    #[tokio::test]
    async fn baseline_comparison_none_when_pr_head_moved() {
        let tmp = TempDir::new().unwrap();
        let gh = Arc::new(FakeGitHub::new());
        // Current head SHA differs from the benchmarked commit ("abc123").
        gh.set_pull_request(
            "cylewitruk/stacks-core",
            7,
            side("cylewitruk", "stacks-core", "develop", "basesha"),
            side("cylewitruk", "stacks-core", "feat/foo", "movedsha"),
        );
        let mut config = no_report_config(&tmp);
        config.reporting.noise_cv_pct = Some(0.37);
        let mut job = job_with(ProgressTarget::PullRequest {
            pr_number: 7,
            comment_id: Some(11),
            check_run_id: Some(22),
            check_run_url: None,
        });
        job.repository = "cylewitruk/stacks-core".into();
        job.workload_key = Some("wk1".into());
        let reporter = Reporter::new(
            Arc::new(config),
            Arc::new(RecordingStore::default()),
            gh,
            Arc::new(OnceCell::new()),
            None,
            Default::default(),
            job,
        );
        assert!(
            reporter
                .baseline_comparison(&serde_json::json!({}))
                .await
                .is_none(),
            "a moved PR head must not compare across commits",
        );
    }
}
