//! Webhook processor.
//!
//! Pulls webhook rows from `github_webhook` via [`WebhookInbox`], hands
//! each to a pluggable [`Classifier`], and writes the resulting
//! outcome back. Implements the queue state machine — claim, terminate,
//! retry-with-backoff, permanent-failure-on-attempts-exhausted,
//! stuck-claim sweep — and runs concurrently with the job `Runner` in
//! production via `tokio::try_join!` from `main`.
//!
//! Slice 2a built the scaffold + lifecycle tests; slice 2b plugged in
//! [`BasicClassifier`] (the Phase 1 production classifier) and started
//! the loop. Later slices (3–7) replace individual branches of
//! `BasicClassifier` with richer logic (installer gate, lineage,
//! policies, user roles, PR materialization); slice 9 changes the
//! `issue_comment` `/benchmark` branch to actually create `job` rows.
//!
//! The scaffold is structured so that every state transition is
//! testable in isolation via [`WebhookProcessor::process_one`], and
//! the long-running [`WebhookProcessor::run`] just composes
//! `process_one` with periodic sweeps and idle backoff.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use chrono::Utc;
use sbgh_core::Result;
use sbgh_core::bench_args::{normalize_stored, resolve_bench_args};
use sbgh_core::db::{
    ClaimedWebhook, InstallationStore, JobStore, NewInstallation, NewPullRequest, NewRepoIdentity,
    NewRepoLineage, NewUser, PolicyStore, PullRequestStore, RepoStore, UserStore, WebhookInbox,
};
use sbgh_core::models::{
    BuildTarget, GitRefKind, GithubAccountType, JobIntent, JobSource, QueuedEventDetail, TaskKind,
    TriggerKind, TriggerMatchSpec, TriggerPolicy, UserRole, WebhookOutcome,
};
use sbgh_core::submission::{
    BenchmarkPlan, BenchmarkVariant, GithubSubmissionProvenance, ProducerKey, ResolvedTaskSource,
    SchedulingConstraints, SubmissionActor, SubmissionCommand, SubmissionDisposition,
    SubmissionProvenance, TaskPlan,
};
use sbgh_github::{
    Command, CreateEvent, GitHubApi, InstallationEvent, InstallationRepositoriesEvent,
    IssueCommentEvent, PullRequestEvent, PushEvent, RepoRef, RepoSummary, parse_command,
};
use sbgh_proto::{InclusiveRange, ValidationEpoch};

/// What a [`Classifier`] decides to do with a claimed webhook.
#[derive(Debug, Clone)]
pub enum ClassifyOutcome {
    /// Final decision; outcome's terminal status is set immediately.
    Terminal(WebhookOutcome),
    /// Transient failure. The processor records the error, increments
    /// attempts, schedules a backoff retry, or — if attempts have run
    /// out — promotes to a permanent failure.
    ///
    /// BasicClassifier (slice 2b) never emits this — its classifications
    /// are all terminal. Later slices that hit GitHub APIs will use it
    /// for transient network/rate-limit failures.
    #[allow(dead_code)]
    Retryable(String),
}

#[async_trait]
pub trait Classifier: Send + Sync + 'static {
    /// Event types this classifier can produce a terminal outcome
    /// for. The processor passes this list to `WebhookInbox::claim_next`
    /// so rows for event types not on the list are LEFT in `received`
    /// status — waiting for a future slice that adds the relevant
    /// classifier branch. This is what prevents earlier slices from
    /// terminalizing rows that later slices need to consume.
    ///
    /// Returned slice borrows from the classifier itself so the router
    /// can compose the list from a runtime-registered set of handlers.
    fn supported_event_types(&self) -> &[&'static str];

    async fn classify(&self, webhook: &ClaimedWebhook) -> ClassifyOutcome;
}

/// Per-event-type unit of classification logic. Plugged into a
/// [`BasicClassifier`] router, which dispatches each claimed row to the
/// handler keyed by `event_type`. Each handler owns whatever
/// per-event-type dependencies it needs (DB stores, GitHub API client,
/// etc.) and exposes a uniform `handle` shape.
///
/// One handler per event type — registering two handlers for the same
/// event type via the builder is a programming error and panics at
/// construction (catches a config mistake at startup, not at runtime).
#[async_trait]
pub trait EventHandler: Send + Sync + 'static {
    /// Event type this handler claims and classifies. Used both as the
    /// router's dispatch key and to compose the processor's claim
    /// filter.
    fn event_type(&self) -> &'static str;

    async fn handle(&self, webhook: &ClaimedWebhook) -> ClassifyOutcome;
}

/// Test-only no-op classifier: everything terminates as
/// `ignored_action`. Used by lifecycle tests that don't care about
/// classification semantics. Production uses [`BasicClassifier`].
#[cfg(test)]
pub struct NoopClassifier {
    supported: Vec<&'static str>,
}

#[cfg(test)]
impl NoopClassifier {
    pub fn new() -> Self {
        // Wide list for lifecycle tests: NoopClassifier handles whatever
        // gets seeded, so tests don't have to remember to set
        // event_type='issue_comment'.
        Self {
            supported: vec![
                "issue_comment",
                "push",
                "pull_request",
                "create",
                "installation",
                "installation_repositories",
            ],
        }
    }
}

#[cfg(test)]
#[async_trait]
impl Classifier for NoopClassifier {
    fn supported_event_types(&self) -> &[&'static str] {
        &self.supported
    }

    async fn classify(&self, _: &ClaimedWebhook) -> ClassifyOutcome {
        ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction)
    }
}

/// Production classifier: a router that dispatches each claimed row to
/// the [`EventHandler`] registered for its `event_type`. The set of
/// supported event types grows by adding handlers in
/// [`BasicClassifierBuilder`] — slice 2b registers only
/// [`IssueCommentHandler`]; slice 3 adds [`InstallationHandler`];
/// later slices register their own.
///
/// Rows whose event type has no registered handler are NOT claimable
/// (filtered out by `claim_next`) so they stay `received` until the
/// slice that knows how to handle them ships. The defensive
/// "no handler matched" branch in `classify` exists only to surface
/// programming errors (e.g., a misconfigured claim filter) as
/// terminal `error` rows instead of an infinite retry loop.
pub struct BasicClassifier {
    handlers: HashMap<&'static str, Arc<dyn EventHandler>>,
    supported: Vec<&'static str>,
}

impl BasicClassifier {
    pub fn builder() -> BasicClassifierBuilder {
        BasicClassifierBuilder::default()
    }
}

#[derive(Default)]
pub struct BasicClassifierBuilder {
    handlers: HashMap<&'static str, Arc<dyn EventHandler>>,
}

impl BasicClassifierBuilder {
    /// Register a handler. Panics if a handler for the same event type
    /// is already registered — duplicate registration is a programming
    /// error we want to fail-fast at startup, not silently shadow at
    /// runtime.
    pub fn with_handler(mut self, handler: Arc<dyn EventHandler>) -> Self {
        let ev = handler.event_type();
        if self
            .handlers
            .insert(ev, handler)
            .is_some()
        {
            panic!("duplicate EventHandler registered for event_type {ev:?}");
        }
        self
    }

    pub fn build(self) -> BasicClassifier {
        // Sorted purely so the supported list is stable across runs;
        // helps with logs and any ops query that prints it.
        let mut supported: Vec<&'static str> = self
            .handlers
            .keys()
            .copied()
            .collect();
        supported.sort_unstable();
        BasicClassifier {
            handlers: self.handlers,
            supported,
        }
    }
}

#[async_trait]
impl Classifier for BasicClassifier {
    fn supported_event_types(&self) -> &[&'static str] {
        &self.supported
    }

    async fn classify(&self, webhook: &ClaimedWebhook) -> ClassifyOutcome {
        match self
            .handlers
            .get(webhook.event_type.as_str())
        {
            Some(h) => h.handle(webhook).await,
            None => {
                // Shouldn't happen — claim filter restricts to registered
                // event types — but if it does, fail terminally rather
                // than loop on retries.
                tracing::error!(
                    event_type = webhook.event_type.as_str(),
                    "BasicClassifier received an unregistered event type; check claim filter"
                );
                ClassifyOutcome::Terminal(WebhookOutcome::Error)
            }
        }
    }
}

// ─── IssueCommentHandler (slice 2b) ────────────────────────────────────

/// Phase 1 issue_comment classification (moved from the old monolithic
/// BasicClassifier in slice 3's router refactor).
///
/// Decision table:
/// - non-`created` actions → `ignored_action`
/// - `created` on a non-PR issue → `ignored_action`
/// - `created` on a PR with no `/benchmark` → `ignored_no_command`
/// - `created` on a PR with malformed `/benchmark` → `ignored_no_command`
///   (schema has no distinct "malformed command" outcome; bucketed with
///   no-command for now)
/// - `created` on a PR with valid `/benchmark` → user authz + policy
///   evaluation. The sender is upserted into `github_user` and checked via
///   `has_role(sender, install, target_repo, trigger_pr_benchmark)`.
///   Unauthorized → `denied_unauthorized` (the user upsert still happens so the
///   attempt has an audit trail). Authorized + policies accepted →
///   `enqueued_job` (creates the `job` row + links via the atomic
///   shared submission-kernel boundary). Deny → `denied_target_policy` /
///   `denied_source_policy`.
/// - NULL / unparseable payload → `error` (can't classify; better terminal than
///   infinite retry)
pub struct IssueCommentHandler {
    repo_store: Arc<dyn RepoStore>,
    policy_store: Arc<dyn PolicyStore>,
    install_store: Arc<dyn InstallationStore>,
    user_store: Arc<dyn UserStore>,
    /// Slice 7: shared materialiser dep. Comments referencing a PR
    /// whose `opened` event predated the new pipeline must still
    /// produce a `github_pull_request` row so slice 9 can link the
    /// job. `materialise_pull_request` upserts the row from the GH
    /// API response.
    pull_request_store: Arc<dyn PullRequestStore>,
    gh: Arc<dyn GitHubApi>,
    /// Slice 9: the accept path creates a `pr_comment` `job` row instead
    /// of emitting the Phase-1 `WouldEnqueueJob` shadow signal.
    job_store: Arc<dyn JobStore>,
    block_validation: Option<Arc<dyn BlockValidationQueue>>,
    /// roadmap-v7: the configured `default_args`, used to compute the job's
    /// `workload_key` at enqueue (a bare `/benchmark` resolves to these).
    /// Defaults to empty; set via [`Self::with_default_args`].
    default_args: String,
}

impl IssueCommentHandler {
    /// Slice 9 widened the constructor to take `job_store`. Slice 7
    /// added `pull_request_store`; slice 6 added `user_store`; slice 5
    /// added `policy_store` + `install_store` + `gh`.
    pub fn new(
        repo_store: Arc<dyn RepoStore>,
        policy_store: Arc<dyn PolicyStore>,
        install_store: Arc<dyn InstallationStore>,
        user_store: Arc<dyn UserStore>,
        pull_request_store: Arc<dyn PullRequestStore>,
        gh: Arc<dyn GitHubApi>,
        job_store: Arc<dyn JobStore>,
    ) -> Self {
        Self {
            repo_store,
            policy_store,
            install_store,
            user_store,
            pull_request_store,
            gh,
            job_store,
            block_validation: None,
            default_args: String::new(),
        }
    }

    /// roadmap-v7: supply the configured `default_args` so enqueued jobs get a
    /// `workload_key`. Builder-style so existing call sites need no change.
    pub fn with_default_args(mut self, default_args: impl Into<String>) -> Self {
        self.default_args = default_args.into();
        self
    }

    pub fn with_block_validation(mut self, queue: Arc<dyn BlockValidationQueue>) -> Self {
        self.block_validation = Some(queue);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockValidationJobRequest {
    pub github_installation_id: i64,
    pub github_repo_id: i64,
    pub commit: String,
    pub webhook_id: i64,
    pub triggering_user_id: i64,
    pub github_pull_request_id: i64,
    pub triggering_comment_id: i64,
    pub epoch: ValidationEpoch,
    pub range: InclusiveRange,
}

#[async_trait]
pub trait BlockValidationQueue: Send + Sync + 'static {
    async fn enqueue(&self, request: BlockValidationJobRequest) -> Result<()>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum IssueTaskCommand {
    Benchmark(Command),
    BlockValidation { epoch: ValidationEpoch, range: InclusiveRange },
}

fn parse_issue_task_command(body: &str) -> std::result::Result<Option<IssueTaskCommand>, ()> {
    const PREFIX: &str = "/validate-blocks";
    let line = body
        .lines()
        .next()
        .unwrap_or_default()
        .trim_end();
    if let Some(rest) = line.strip_prefix(PREFIX) {
        if !rest.starts_with(char::is_whitespace) {
            return Ok(None);
        }
        let tokens = rest
            .split_whitespace()
            .collect::<Vec<_>>();
        if tokens.len() != 3 {
            return Err(());
        }
        let epoch = match tokens[0] {
            "pre-nakamoto" | "pre_nakamoto" => ValidationEpoch::PreNakamoto,
            "nakamoto" => ValidationEpoch::Nakamoto,
            _ => return Err(()),
        };
        let start = tokens[1]
            .parse()
            .map_err(|_| ())?;
        let end = tokens[2]
            .parse()
            .map_err(|_| ())?;
        if start > end {
            return Err(());
        }
        return Ok(Some(IssueTaskCommand::BlockValidation {
            epoch,
            range: InclusiveRange { start, end },
        }));
    }
    parse_command(body)
        .map(|command| command.map(IssueTaskCommand::Benchmark))
        .map_err(|_| ())
}

#[async_trait]
impl EventHandler for IssueCommentHandler {
    fn event_type(&self) -> &'static str {
        "issue_comment"
    }

    async fn handle(&self, webhook: &ClaimedWebhook) -> ClassifyOutcome {
        if webhook.action.as_deref() != Some("created") {
            return ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction);
        }

        let Some(payload) = webhook.payload.as_ref() else {
            return ClassifyOutcome::Terminal(WebhookOutcome::Error);
        };

        let event: IssueCommentEvent = match serde_json::from_value(payload.clone()) {
            Ok(e) => e,
            Err(_) => return ClassifyOutcome::Terminal(WebhookOutcome::Error),
        };

        if event
            .issue
            .pull_request
            .is_none()
        {
            return ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction);
        }

        match parse_issue_task_command(&event.comment.body) {
            // No /benchmark command → no policy work needed.
            Ok(None) | Err(_) => ClassifyOutcome::Terminal(WebhookOutcome::IgnoredNoCommand),
            Ok(Some(command)) => {
                // Slice 5: a `/benchmark` PR comment triggers the same
                // target+source policy evaluation as `pull_request`
                // events. We have to fetch the PR via GH API first to
                // get the base+head repo ids — the issue_comment
                // payload doesn't carry them.
                let install_id = event.installation.id;
                let repository = event
                    .repository
                    .full_name
                    .clone();
                let pr_number = event.issue.number as u64;

                let pr = match self
                    .gh
                    .get_pull_request(install_id, &repository, pr_number)
                    .await
                {
                    Ok(p) => p,
                    Err(e) => {
                        return ClassifyOutcome::Retryable(format!(
                            "get_pull_request({repository}#{pr_number}): {e}"
                        ));
                    }
                };

                // Slice 7: materialise the PR row from the GH API
                // response. Replaces the slice 5 inline repo-identity
                // upserts; the shared helper also upserts the PR
                // author so slice 9's `/benchmark` job link has all
                // its FK targets. Slice 9 captures the returned PR row
                // so the job's PR link can reference it by primary key.
                let pr_row = match materialise_pull_request(
                    self.repo_store.as_ref(),
                    self.user_store.as_ref(),
                    self.pull_request_store
                        .as_ref(),
                    PullRequestRepoInput {
                        id: pr.base.repo.id,
                        owner: &pr.base.repo.owner,
                        name: &pr.base.repo.name,
                    },
                    PullRequestRepoInput {
                        id: pr.head.repo.id,
                        owner: &pr.head.repo.owner,
                        name: &pr.head.repo.name,
                    },
                    pr.number as i32,
                    &pr.title,
                    PullRequestAuthorInput {
                        id: pr.author.id,
                        login: &pr.author.login,
                        account_type: pr.author.account_type,
                    },
                )
                .await
                {
                    Ok(row) => row,
                    Err(out) => return out,
                };

                // Slice 6: upsert the sender first (audit trail for
                // denied attempts), then check `trigger_pr_benchmark`
                // on the TARGET repo. A repo-scoped grant for a
                // different repo in the same install does NOT
                // authorize — that's the design of `has_role`.
                let Some(sender_type) = parse_account_type(&event.sender.account_type) else {
                    return ClassifyOutcome::Terminal(WebhookOutcome::Error);
                };
                if let Err(e) = self
                    .user_store
                    .upsert_user(&NewUser {
                        id: event.sender.id,
                        login: event.sender.login.clone(),
                        user_type: sender_type,
                    })
                    .await
                {
                    return ClassifyOutcome::Retryable(format!("upsert_user(sender): {e}"));
                }
                match self
                    .user_store
                    .has_role(
                        event.sender.id,
                        install_id,
                        pr.base.repo.id,
                        UserRole::TriggerPrBenchmark,
                    )
                    .await
                {
                    Ok(true) => {}
                    Ok(false) => {
                        tracing::warn!(
                            installation_id = install_id,
                            pr_number,
                            sender_id = event.sender.id,
                            sender_login = event.sender.login.as_str(),
                            base_repo_id = pr.base.repo.id,
                            "denied /benchmark: sender lacks trigger_pr_benchmark role on target \
                             repo"
                        );
                        return ClassifyOutcome::Terminal(WebhookOutcome::DeniedUnauthorized);
                    }
                    Err(e) => return ClassifyOutcome::Retryable(format!("has_role: {e}")),
                }

                match evaluate_pr_policies(
                    self.install_store.as_ref(),
                    self.policy_store.as_ref(),
                    install_id,
                    pr.base.repo.id,
                    pr.head.repo.id,
                )
                .await
                {
                    PolicyEvaluation::Accepted => {
                        tracing::info!(
                            installation_id = install_id,
                            pr_number,
                            sender_id = event.sender.id,
                            sender_login = event.sender.login.as_str(),
                            base_repo_id = pr.base.repo.id,
                            head_repo_id = pr.head.repo.id,
                            "/benchmark authorized: sender has trigger role, target+source \
                             policies accepted — creating job"
                        );
                        // Slice 9: create the `pr_comment` ad-hoc job.
                        // The benchmark runs against the PR's HEAD; the
                        // job's repo is the TARGET (base) repo, which is
                        // the membership/policy-gated side. Head SHA is
                        // known from the PR API; committed_at is left for
                        // the daemon to backfill if needed.
                        if let IssueTaskCommand::BlockValidation { epoch, range } = command {
                            let Some(queue) = &self.block_validation else {
                                return ClassifyOutcome::Terminal(WebhookOutcome::IgnoredNoCommand);
                            };
                            let request = BlockValidationJobRequest {
                                github_installation_id: install_id,
                                github_repo_id: pr.base.repo.id,
                                commit: pr.head.sha.clone(),
                                webhook_id: webhook.id,
                                triggering_user_id: event.sender.id,
                                github_pull_request_id: pr_row.id,
                                triggering_comment_id: event.comment.id,
                                epoch,
                                range,
                            };
                            return match queue.enqueue(request).await {
                                Ok(()) => ClassifyOutcome::Terminal(WebhookOutcome::EnqueuedJob),
                                Err(error) => ClassifyOutcome::Retryable(format!(
                                    "enqueue block validation: {error}"
                                )),
                            };
                        }
                        let IssueTaskCommand::Benchmark(cmd) = command else {
                            unreachable!("block-validation command returned above")
                        };
                        let detail = QueuedEventDetail::PrComment {
                            sender_id: event.sender.id,
                            sender_login: event.sender.login.clone(),
                            comment_id: event.comment.id,
                            pr_number: event.issue.number,
                            subcommand: cmd.subcommand.clone(),
                            bench_args: cmd.args.clone(),
                        };
                        let resolved =
                            resolve_bench_args(&normalize_stored(&detail), &self.default_args);
                        let workload_key = resolved.workload_key.clone();
                        let request = GithubBenchmarkSubmission {
                            source: ResolvedTaskSource {
                                github_installation_id: install_id,
                                github_repo_id: pr.base.repo.id,
                                source: JobSource::GithubComment,
                                intent: JobIntent::AdhocBenchmark,
                                task_kind: TaskKind::Benchmark,
                                build_target: BuildTarget::StacksBench,
                                git_ref_kind: GitRefKind::Branch,
                                git_ref_display: pr.head.branch.clone(),
                                commit: pr.head.sha.clone(),
                                committed_at: None,
                                workload_key: Some(workload_key.clone()),
                            },
                            webhook_id: webhook.id,
                            triggering_user_id: Some(event.sender.id),
                            pull_request_id: Some(pr_row.id),
                            triggering_comment_id: Some(event.comment.id),
                            queued_event_detail: queued_detail(detail, &resolved.effective_args),
                        };
                        // Phase 5 dedup (BEST-EFFORT): if an active `/benchmark`
                        // job already covers this exact commit AND the same
                        // workload, skip enqueuing a duplicate — two jobs on one
                        // head SHA would fight over the single check GitHub
                        // surfaces per `(name, head_sha)`.
                        //
                        // roadmap-v7: the match is **workload-aware** — a
                        // *different* workload (e.g. `/benchmark run --count 1`
                        // while a default `/benchmark` is active) is a genuinely
                        // different benchmark, so it is NOT deduped and enqueues
                        // normally (it gets its own per-job check).
                        //
                        // This is a check-then-insert *outside* the atomic job
                        // boundary, so it is NOT a hard guarantee: two concurrent
                        // processors (the queue is `FOR UPDATE SKIP LOCKED`), or a
                        // crash between this decision and the inbox marking the
                        // webhook done, can still slip a duplicate. Both windows
                        // are narrow here (single daemon) and their worst case is
                        // a redundant *re-run* after the original concluded — NOT
                        // the concurrent check collision this prevents. A partial
                        // unique index on active `(github_repo_id, git_commit_hash,
                        // workload_key) WHERE source='github_comment'` is the
                        // structural hardening if a hard guarantee / multi-processor
                        // is ever needed (see roadmap-v5 Phase 5).
                        match self
                            .job_store
                            .find_active_job(
                                pr.base.repo.id,
                                &pr.head.sha,
                                JobSource::GithubComment,
                                &workload_key,
                            )
                            .await
                        {
                            Ok(Some(existing)) => {
                                tracing::info!(
                                    installation_id = install_id,
                                    pr_number,
                                    existing_job = %existing,
                                    head_sha = %pr.head.sha,
                                    "duplicate /benchmark for a commit already being benchmarked; not enqueuing"
                                );
                                // Same Processed bucket as the per-webhook
                                // `AlreadyEnqueued` idempotency — the request is
                                // satisfied by the existing job.
                                ClassifyOutcome::Terminal(WebhookOutcome::EnqueuedJob)
                            }
                            Ok(None) => enqueue_job(self.job_store.as_ref(), request).await,
                            Err(e) => ClassifyOutcome::Retryable(format!("find_active_job: {e}")),
                        }
                    }
                    PolicyEvaluation::DeniedTarget => {
                        ClassifyOutcome::Terminal(WebhookOutcome::DeniedTargetPolicy)
                    }
                    PolicyEvaluation::DeniedSource => {
                        ClassifyOutcome::Terminal(WebhookOutcome::DeniedSourcePolicy)
                    }
                    PolicyEvaluation::Retryable(msg) => ClassifyOutcome::Retryable(msg),
                }
            }
        }
    }
}

// ─── InstallationHandler (slice 3) ─────────────────────────────────────

/// Materialises (or denies) `github_installation` rows in response to
/// `installation.*` webhooks. Slice 3 actions:
///
/// - `created` → allowlist lookup. Allowed → upsert install row →
///   `processed_installation`. Denied/missing/disabled →
///   `denied_install_allowlist`.
/// - `suspend` → set `suspended_at = received_at` on the install row, if one
///   exists → `processed_installation`. Missing row →
///   `ignored_unknown_installation` (we never accepted this install).
/// - `unsuspend` → clear `suspended_at` on the install row → same as suspend
///   for the missing case.
/// - `deleted` → transactionally soft-delete the install row (sets `deleted_at
///   = NOW()`), bulk-revoke every active membership in
///   `github_installation_repo`, AND soft-disable every active target / source
///   / trigger policy row for the install (slice 5 added the policy cleanup to
///   this transaction). The install row is preserved so slice 8+ job FKs remain
///   valid. Outcome: `processed_installation`; missing install →
///   `ignored_unknown_installation`.
/// - any other action → `ignored_action` (forward-compat: GitHub may add new
///   actions; record-and-skip beats retry-forever).
///
/// Payload parse failure → `error`. Storage errors bubble up as
/// `ClassifyOutcome::Retryable`; the processor schedules a backoff
/// retry.
pub struct InstallationHandler {
    install_store: Arc<dyn InstallationStore>,
    repo_store: Arc<dyn RepoStore>,
    gh: Arc<dyn GitHubApi>,
}

impl InstallationHandler {
    /// Slice 4 widened the constructor to take the repo store + GH
    /// client so `installation.created` can materialise initial
    /// memberships from the payload's `repositories` array (Codex's
    /// slice-4 high-finding fix). Slice 3 callers can pass
    /// a repository store plus `Arc::new(FakeGitHub::new())`
    /// in tests that don't exercise the initial-repos path.
    pub fn new(
        install_store: Arc<dyn InstallationStore>,
        repo_store: Arc<dyn RepoStore>,
        gh: Arc<dyn GitHubApi>,
    ) -> Self {
        Self { install_store, repo_store, gh }
    }
}

#[async_trait]
impl EventHandler for InstallationHandler {
    fn event_type(&self) -> &'static str {
        "installation"
    }

    async fn handle(&self, webhook: &ClaimedWebhook) -> ClassifyOutcome {
        let Some(payload) = webhook.payload.as_ref() else {
            return ClassifyOutcome::Terminal(WebhookOutcome::Error);
        };

        let event: InstallationEvent = match serde_json::from_value(payload.clone()) {
            Ok(e) => e,
            Err(_) => return ClassifyOutcome::Terminal(WebhookOutcome::Error),
        };

        let account_type = match parse_account_type(
            &event
                .installation
                .account
                .account_type,
        ) {
            Some(t) => t,
            None => return ClassifyOutcome::Terminal(WebhookOutcome::Error),
        };

        match event.action.as_str() {
            "created" => {
                handle_created(
                    self.install_store.as_ref(),
                    self.repo_store.as_ref(),
                    self.gh.as_ref(),
                    &event,
                    account_type,
                )
                .await
            }
            "suspend" => {
                handle_set_suspended(
                    self.install_store.as_ref(),
                    event.installation.id,
                    Some(webhook.received_at),
                )
                .await
            }
            "unsuspend" => {
                handle_set_suspended(self.install_store.as_ref(), event.installation.id, None).await
            }
            "deleted" => handle_deleted(self.install_store.as_ref(), event.installation.id).await,
            // Forward-compat: anything we don't recognise (e.g., a future
            // `installation.new_permissions_accepted`) is recorded and
            // skipped rather than failing.
            _ => ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction),
        }
    }
}

async fn handle_created(
    install_store: &dyn InstallationStore,
    repo_store: &dyn RepoStore,
    gh: &dyn GitHubApi,
    event: &InstallationEvent,
    account_type: GithubAccountType,
) -> ClassifyOutcome {
    let allowed = match install_store
        .lookup_allowed(event.installation.account.id)
        .await
    {
        Ok(row) => row,
        Err(e) => return ClassifyOutcome::Retryable(format!("lookup_allowed: {e}")),
    };

    let approved = match allowed {
        Some(row) if row.is_enabled => row,
        // Missing or disabled both deny with the same outcome — the
        // schema doesn't distinguish them, and "operator paused this
        // installer" is the same outcome as "operator never approved
        // this installer" from GitHub's perspective.
        other => {
            tracing::warn!(
                installation_id = event.installation.id,
                account_id = event.installation.account.id,
                account_login = event
                    .installation
                    .account
                    .login
                    .as_str(),
                disabled = other.is_some(),
                "denied installation.created: account not on installer allowlist — `sbgh-cli \
                 installer allow --login <account>` to approve"
            );
            return ClassifyOutcome::Terminal(WebhookOutcome::DeniedInstallAllowlist);
        }
    };

    let new = NewInstallation {
        id: event.installation.id,
        github_account_id: approved.github_account_id,
        account_login: event
            .installation
            .account
            .login
            .clone(),
        account_type,
    };
    if let Err(e) = install_store
        .upsert_installation(&new)
        .await
    {
        return ClassifyOutcome::Retryable(format!("upsert_installation: {e}"));
    }
    tracing::info!(
        installation_id = event.installation.id,
        account_login = event
            .installation
            .account
            .login
            .as_str(),
        initial_repos = event.repositories.len(),
        "installation.created: account approved — installation row upserted, materialising \
         initial repo memberships"
    );

    // Slice 4 high-finding fix: materialise initial memberships from
    // the payload's `repositories` array. Without this, a fresh install
    // would have NO `github_installation_repo` rows until a later
    // `installation_repositories.added` event fired. Per-repo outcomes
    // are recorded into membership rows; the webhook-level outcome
    // stays `ProcessedInstallation` regardless of per-repo lineage
    // results (the install creation itself succeeded). Retryable
    // errors during the lineage walk DO propagate so a network blip
    // doesn't drop the initial-repos materialisation on the floor.
    for repo in &event.repositories {
        match materialise_repo_membership(
            repo_store,
            install_store,
            gh,
            event.installation.id,
            repo,
        )
        .await
        {
            RepoMembershipOutcome::Added
            | RepoMembershipOutcome::UnsupportedLineage
            | RepoMembershipOutcome::IdMismatch
            | RepoMembershipOutcome::MalformedFullName => {
                // All non-retryable per-repo outcomes; the install is
                // still considered processed.
            }
            RepoMembershipOutcome::InstallationNotActive => {
                // We JUST upserted the install row above; this branch
                // is only reachable if a concurrent processor
                // soft-deleted it between our upsert and the membership
                // probe — exceedingly unlikely (two distinct webhooks
                // racing), and the right response is to surface it
                // rather than silently swallow.
                tracing::warn!(
                    installation_id = event.installation.id,
                    "installation.created: install was concurrently soft-deleted during \
                     initial-repos materialisation"
                );
                return ClassifyOutcome::Terminal(WebhookOutcome::IgnoredUnknownInstallation);
            }
            RepoMembershipOutcome::Retryable(e) => return ClassifyOutcome::Retryable(e),
        }
    }

    ClassifyOutcome::Terminal(WebhookOutcome::ProcessedInstallation)
}

async fn handle_set_suspended(
    store: &dyn InstallationStore,
    installation_id: i64,
    suspended_at: Option<chrono::DateTime<Utc>>,
) -> ClassifyOutcome {
    match store
        .set_suspended(installation_id, suspended_at)
        .await
    {
        Ok(Some(_)) => ClassifyOutcome::Terminal(WebhookOutcome::ProcessedInstallation),
        // No row to update — likely a suspend/unsuspend for an install
        // we never accepted (allowlist denied at create). Record it and
        // move on; retrying won't help.
        Ok(None) => ClassifyOutcome::Terminal(WebhookOutcome::IgnoredUnknownInstallation),
        Err(e) => ClassifyOutcome::Retryable(format!("set_suspended: {e}")),
    }
}

async fn handle_deleted(store: &dyn InstallationStore, installation_id: i64) -> ClassifyOutcome {
    // Slice 4 changed the underlying semantic: instead of hard-deleting
    // the install row, the store transactionally soft-deletes it (sets
    // deleted_at) AND bulk-revokes every active membership. The outcome
    // mapping stays the same as slice 3 — install_found maps to
    // ProcessedInstallation, missing-install to IgnoredUnknownInstallation
    // — but we now have a `memberships_revoked` count for logs/ops.
    match store
        .delete_installation(installation_id)
        .await
    {
        Ok(outcome) if outcome.install_found => {
            if outcome.memberships_revoked > 0 {
                tracing::info!(
                    installation_id,
                    memberships_revoked = outcome.memberships_revoked,
                    "installation.deleted: soft-deleted install + revoked memberships"
                );
            }
            ClassifyOutcome::Terminal(WebhookOutcome::ProcessedInstallation)
        }
        Ok(_) => ClassifyOutcome::Terminal(WebhookOutcome::IgnoredUnknownInstallation),
        Err(e) => ClassifyOutcome::Retryable(format!("delete_installation: {e}")),
    }
}

fn parse_account_type(s: &str) -> Option<GithubAccountType> {
    match s {
        "User" => Some(GithubAccountType::User),
        "Organization" => Some(GithubAccountType::Organization),
        "Bot" => Some(GithubAccountType::Bot),
        _ => None,
    }
}

// ─── InstallationRepositoriesHandler (slice 4) ─────────────────────────

/// Materialises (or revokes) `github_installation_repo` rows in response
/// to `installation_repositories.{added,removed}` webhooks. Slice 4 actions:
///
/// - `added`   → for each repo: cache-miss-fetch lineage from GH API, upsert
///   `github_repo`, check `is_supported_lineage`, and (if supported)
///   `add_or_restore_membership`. Per-repo decisions roll up to a single
///   webhook outcome: any supported & membership change →
///   `ProcessedInstallation` else any unsupported lineage →
///   `IgnoredUnsupportedLineage` else (e.g. all already-active) →
///   `IgnoredAction`
/// - `removed` → for each repo: `revoke_membership`. Any transition →
///   `ProcessedInstallation`; else `IgnoredAction`.
///
/// GH API failures during the lineage walk become `Retryable` so a
/// network blip doesn't drop accepted repos on the floor.
pub struct InstallationRepositoriesHandler {
    repo_store: Arc<dyn RepoStore>,
    membership_store: Arc<dyn InstallationStore>,
    /// Slice 5 review fix: `handle_removed` cascades into the policy
    /// tables (disabling target_repo_policy + matching trigger_policy
    /// rows) BEFORE revoking the membership, so a stale inbox event
    /// arriving after the revoke can't find a stale-enabled policy.
    policy_store: Arc<dyn PolicyStore>,
    /// Slice 6 third-pass review fix: `handle_removed` ALSO cascades
    /// into `github_user_role` to soft-revoke any repo-scoped grants
    /// for the removed repo. Without this, a grant made while the
    /// repo was active would survive removal AND silently become
    /// effective again if the repo is later re-added.
    user_store: Arc<dyn UserStore>,
    gh: Arc<dyn GitHubApi>,
}

impl InstallationRepositoriesHandler {
    pub fn new(
        repo_store: Arc<dyn RepoStore>,
        membership_store: Arc<dyn InstallationStore>,
        policy_store: Arc<dyn PolicyStore>,
        user_store: Arc<dyn UserStore>,
        gh: Arc<dyn GitHubApi>,
    ) -> Self {
        Self {
            repo_store,
            membership_store,
            policy_store,
            user_store,
            gh,
        }
    }
}

#[async_trait]
impl EventHandler for InstallationRepositoriesHandler {
    fn event_type(&self) -> &'static str {
        "installation_repositories"
    }

    async fn handle(&self, webhook: &ClaimedWebhook) -> ClassifyOutcome {
        let Some(payload) = webhook.payload.as_ref() else {
            return ClassifyOutcome::Terminal(WebhookOutcome::Error);
        };
        let event: InstallationRepositoriesEvent = match serde_json::from_value(payload.clone()) {
            Ok(e) => e,
            Err(_) => return ClassifyOutcome::Terminal(WebhookOutcome::Error),
        };

        let install_id = event.installation.id;

        match event.action.as_str() {
            "added" => {
                handle_repos_added(
                    self.repo_store.as_ref(),
                    self.membership_store.as_ref(),
                    self.gh.as_ref(),
                    install_id,
                    &event.repositories_added,
                )
                .await
            }
            "removed" => {
                handle_repos_removed(
                    self.membership_store.as_ref(),
                    self.policy_store.as_ref(),
                    self.user_store.as_ref(),
                    install_id,
                    &event.repositories_removed,
                )
                .await
            }
            // Forward-compat: any other action we don't recognize is
            // recorded-and-skipped rather than retried forever.
            _ => ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction),
        }
    }
}

/// Per-repo outcome of `materialise_repo_membership`. The caller
/// aggregates these into a single webhook-level outcome.
enum RepoMembershipOutcome {
    /// Membership row created or restored.
    Added,
    /// Repo's lineage doesn't trace to an enabled `supported_repo_root`.
    /// Identity row is still cached for audit.
    UnsupportedLineage,
    /// Skipped because the payload's `repo.id` didn't match what GH's
    /// `/repos/{owner}/{name}` resolved to — webhook is stale across a
    /// rename / name-reuse and we'd risk granting membership to the
    /// wrong repo. The mismatched ids are already logged at the
    /// detection site; the variant is a flag, not a carrier.
    IdMismatch,
    /// Skipped because `full_name` had no `/`.
    MalformedFullName,
    /// `github_installation` has `deleted_at IS NOT NULL` (or the row
    /// doesn't exist at all). Detected at the membership-write boundary;
    /// the entire batch should bail to `IgnoredUnknownInstallation`.
    InstallationNotActive,
    /// Transient infra failure. Propagates up as `Retryable`.
    Retryable(String),
}

/// Resolve one repo from a webhook payload, walk its lineage, gate on
/// support, and add membership if accepted. Shared between
/// `InstallationHandler` (slice 4 high-finding fix: initial repos from
/// `installation.created.repositories`) and
/// `InstallationRepositoriesHandler` (`.added` events).
async fn materialise_repo_membership(
    repo_store: &dyn RepoStore,
    membership_store: &dyn InstallationStore,
    gh: &dyn GitHubApi,
    install_id: i64,
    payload_repo: &sbgh_github::InstallationRepository,
) -> RepoMembershipOutcome {
    let Some((owner, name)) = split_full_name(&payload_repo.full_name) else {
        tracing::warn!(
            full_name = payload_repo
                .full_name
                .as_str(),
            "repo materialisation: malformed full_name, skipping"
        );
        return RepoMembershipOutcome::MalformedFullName;
    };

    // 1. Fetch + verify identity. The payload carries `id`; we cross-check against
    //    what /repos/{owner}/{name} resolves to. A mismatch means the webhook is
    //    stale across a rename or recycled name — granting membership for
    //    `summary.id` would point at a different repo than the one GitHub reported
    //    in the event.
    let summary = match gh
        .get_repository(install_id, owner, name)
        .await
    {
        Ok(s) => s,
        Err(e) => {
            return RepoMembershipOutcome::Retryable(format!(
                "get_repository({owner}/{name}): {e}"
            ));
        }
    };
    if summary.id != payload_repo.id {
        tracing::warn!(
            installation_id = install_id,
            full_name = payload_repo
                .full_name
                .as_str(),
            payload_id = payload_repo.id,
            resolved_id = summary.id,
            "repo materialisation: payload repo.id doesn't match /repos lookup — likely stale \
             webhook across a rename, skipping membership"
        );
        return RepoMembershipOutcome::IdMismatch;
    }

    // 2. Upsert lineage.
    let lineage = lineage_from_summary(&summary);
    if let Err(e) = repo_store
        .upsert_repo_lineage(&lineage)
        .await
    {
        return RepoMembershipOutcome::Retryable(format!(
            "upsert_repo_lineage({owner}/{name}): {e}"
        ));
    }

    // 3. Check support gate.
    let supported = match repo_store
        .is_supported_lineage(summary.id)
        .await
    {
        Ok(s) => s,
        Err(e) => {
            return RepoMembershipOutcome::Retryable(format!(
                "is_supported_lineage({}): {e}",
                summary.id
            ));
        }
    };
    if !supported {
        tracing::info!(
            installation_id = install_id,
            repo_id = summary.id,
            full_name = payload_repo
                .full_name
                .as_str(),
            "repo materialisation: lineage unsupported, skipping membership"
        );
        return RepoMembershipOutcome::UnsupportedLineage;
    }

    // 4. Add/restore membership — defensive against a concurrently soft-deleted
    //    install (None signals deleted_at IS NOT NULL).
    match membership_store
        .add_or_restore_membership(install_id, summary.id)
        .await
    {
        Ok(Some(_)) => {
            tracing::info!(
                installation_id = install_id,
                repo_id = summary.id,
                full_name = payload_repo
                    .full_name
                    .as_str(),
                "repo membership materialised: lineage supported — not yet a benchmark target \
                 (enable target/source policy + grant roles to allow /benchmark)"
            );
            RepoMembershipOutcome::Added
        }
        Ok(None) => RepoMembershipOutcome::InstallationNotActive,
        Err(e) => RepoMembershipOutcome::Retryable(format!(
            "add_or_restore_membership({install_id}, {repo_id}): {e}",
            repo_id = summary.id,
        )),
    }
}

async fn handle_repos_added(
    repo_store: &dyn RepoStore,
    membership_store: &dyn InstallationStore,
    gh: &dyn GitHubApi,
    install_id: i64,
    repos: &[sbgh_github::InstallationRepository],
) -> ClassifyOutcome {
    let mut any_supported = false;
    let mut any_unsupported = false;

    for repo in repos {
        match materialise_repo_membership(repo_store, membership_store, gh, install_id, repo).await
        {
            RepoMembershipOutcome::Added => any_supported = true,
            RepoMembershipOutcome::UnsupportedLineage | RepoMembershipOutcome::IdMismatch => {
                any_unsupported = true
            }
            RepoMembershipOutcome::MalformedFullName => {} // already logged, no aggregate change
            RepoMembershipOutcome::InstallationNotActive => {
                // Whole batch bails: subsequent repos would all hit the
                // same install-deleted gate, no point iterating.
                return ClassifyOutcome::Terminal(WebhookOutcome::IgnoredUnknownInstallation);
            }
            RepoMembershipOutcome::Retryable(e) => return ClassifyOutcome::Retryable(e),
        }
    }

    if any_supported {
        ClassifyOutcome::Terminal(WebhookOutcome::ProcessedInstallation)
    } else if any_unsupported {
        ClassifyOutcome::Terminal(WebhookOutcome::IgnoredUnsupportedLineage)
    } else {
        ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction)
    }
}

async fn handle_repos_removed(
    membership_store: &dyn InstallationStore,
    policy_store: &dyn PolicyStore,
    user_store: &dyn UserStore,
    install_id: i64,
    repos: &[sbgh_github::InstallationRepository],
) -> ClassifyOutcome {
    let mut any_revoked = false;
    for repo in repos {
        // Slice 5 review fix: cascade-disable the target_repo_policy +
        // any matching trigger_policy rows BEFORE revoking the
        // membership. If we revoked first and a stale inbox event
        // raced, it could see (revoked_at SET, target_policy is_enabled
        // TRUE) and incorrectly evaluate as accepted. Policy disable
        // is idempotent (predicate `is_enabled = TRUE`) so re-delivery
        // is safe; revoke_membership itself is idempotent too.
        if let Err(e) = policy_store
            .disable_target_and_triggers(install_id, repo.id)
            .await
        {
            return ClassifyOutcome::Retryable(format!(
                "disable_target_and_triggers({install_id}, {}): {e}",
                repo.id
            ));
        }
        // Slice 6 post-review cascade: bulk-soft-revoke any
        // repo-scoped `github_user_role` grants for this repo. Same
        // ordering rationale as the policy cascade above — better
        // to revoke role grants than to leave a stale grant active
        // until membership is gone. Install-wide grants are NOT
        // touched (they apply to all repos in the install).
        if let Err(e) = user_store
            .revoke_repo_scoped_grants(install_id, repo.id)
            .await
        {
            return ClassifyOutcome::Retryable(format!(
                "revoke_repo_scoped_grants({install_id}, {}): {e}",
                repo.id
            ));
        }
        match membership_store
            .revoke_membership(install_id, repo.id)
            .await
        {
            Ok(Some(_)) => {
                any_revoked = true;
            }
            // Already-revoked OR never-known: idempotent skip. The
            // schema FK guarantees we know about every repo that ever
            // had a membership, so an unknown id here just means GitHub
            // sent us a `removed` for a repo whose `added` we never
            // saw (rare, possible during an outage backfill).
            Ok(None) => {}
            Err(e) => {
                return ClassifyOutcome::Retryable(format!(
                    "revoke_membership({install_id}, {}): {e}",
                    repo.id
                ));
            }
        }
    }
    if any_revoked {
        ClassifyOutcome::Terminal(WebhookOutcome::ProcessedInstallation)
    } else {
        ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction)
    }
}

fn split_full_name(full_name: &str) -> Option<(&str, &str)> {
    full_name.split_once('/')
}

fn lineage_from_summary(summary: &RepoSummary) -> NewRepoLineage {
    NewRepoLineage {
        repo: NewRepoIdentity {
            id: summary.id,
            owner: summary.owner.clone(),
            name: summary.name.clone(),
            default_branch: summary.default_branch.clone(),
        },
        is_fork: summary.is_fork,
        parent: summary
            .parent
            .as_ref()
            .map(repo_ref_to_identity),
        source: summary
            .source
            .as_ref()
            .map(repo_ref_to_identity),
    }
}

fn repo_ref_to_identity(r: &RepoRef) -> NewRepoIdentity {
    NewRepoIdentity {
        id: r.id,
        owner: r.owner.clone(),
        name: r.name.clone(),
        default_branch: None,
    }
}

// ─── Slice 5: PullRequestHandler / PushHandler / CreateHandler ─────────

/// Decision a target+source policy pair produces. Pulled into a typed
/// outcome so both the slice 5 `PullRequestHandler` and the slice 5
/// extension to `IssueCommentHandler`'s /benchmark branch (which
/// needs the same eval) can share the helper.
enum PolicyEvaluation {
    /// Both target + source policies are present AND enabled. Phase 1
    /// logs "would enqueue"; slice 9 will create the job here.
    Accepted,
    /// `target_repo_policy` missing or `is_enabled=FALSE`.
    DeniedTarget,
    /// Target accepted but `source_repo_policy` missing or disabled.
    DeniedSource,
    /// DB error during lookup — surfaces as Retryable to the caller.
    Retryable(String),
}

async fn evaluate_pr_policies(
    install_store: &dyn InstallationStore,
    policy_store: &dyn PolicyStore,
    install_id: i64,
    base_repo_id: i64,
    source_repo_id: i64,
) -> PolicyEvaluation {
    // Slice 5 review fix: gate on active membership BEFORE trusting
    // target_repo_policy. Even if the policy row says is_enabled=TRUE,
    // a revoked membership / deleted install / suspended install means
    // the (install, base_repo) pair isn't currently authorized for
    // benchmark work. Defense-in-depth alongside the
    // disable_target_and_triggers cascade in
    // InstallationRepositoriesHandler.handle_removed.
    match install_store
        .is_membership_active(install_id, base_repo_id)
        .await
    {
        Ok(true) => {}
        Ok(false) => {
            tracing::warn!(
                install_id,
                base_repo_id,
                "policy denied (target): repo membership inactive — revoked, or install deleted / \
                 suspended"
            );
            return PolicyEvaluation::DeniedTarget;
        }
        Err(e) => return PolicyEvaluation::Retryable(format!("is_membership_active: {e}")),
    }
    let target = match policy_store
        .lookup_target_policy(install_id, base_repo_id)
        .await
    {
        Ok(t) => t,
        Err(e) => return PolicyEvaluation::Retryable(format!("lookup_target_policy: {e}")),
    };
    match target {
        Some(t) if t.is_enabled => {}
        _ => {
            tracing::warn!(
                install_id,
                base_repo_id,
                "policy denied (target): target_repo_policy missing or disabled — `sbgh-cli \
                 policy target allow` to enable"
            );
            return PolicyEvaluation::DeniedTarget;
        }
    }
    let source = match policy_store
        .lookup_source_policy(install_id, source_repo_id)
        .await
    {
        Ok(s) => s,
        Err(e) => return PolicyEvaluation::Retryable(format!("lookup_source_policy: {e}")),
    };
    match source {
        Some(s) if s.is_enabled => PolicyEvaluation::Accepted,
        _ => {
            tracing::warn!(
                install_id,
                source_repo_id,
                "policy denied (source): source_repo_policy missing or disabled — `sbgh-cli \
                 policy source allow` to enable"
            );
            PolicyEvaluation::DeniedSource
        }
    }
}

/// Slice 9: create a new-schema `job` (+ webhook/user/PR links + queued
/// aggregate provenance plus the initial `job_event` through the atomic
/// submission-kernel boundary and
/// map the result to a terminal `EnqueuedJob`.
///
/// Any store error becomes `Retryable`: the submission transaction is
/// all-or-nothing, so a failure leaves zero job rows and the webhook is
/// safely reprocessed. All FK targets (install/repo membership, webhook,
/// user, PR) are materialised earlier in each handler, so a persistent
/// FK failure here signals a bug — the max-attempts ceiling then
/// promotes it to a permanent `error` rather than looping forever.
///
/// `enqueue` deliberately creates exactly ONE job per accepted webhook:
/// a single all-or-nothing transaction is retry-safe (a partial-failure
/// loop over multiple per-job persistence calls could duplicate
/// already-created jobs on retry). Push/tag handlers log when more than
/// one trigger matched so multi-trigger fan-out isn't silently dropped.
struct GithubBenchmarkSubmission {
    source: ResolvedTaskSource,
    webhook_id: i64,
    triggering_user_id: Option<i64>,
    pull_request_id: Option<i64>,
    triggering_comment_id: Option<i64>,
    queued_event_detail: serde_json::Value,
}

async fn enqueue_job(
    job_store: &dyn JobStore,
    request: GithubBenchmarkSubmission,
) -> ClassifyOutcome {
    let queued_event_detail = request.queued_event_detail;
    let effective_args = queued_event_detail
        .get("effective_args")
        .and_then(|value| serde_json::from_value::<Vec<String>>(value.clone()).ok())
        .unwrap_or_default();
    let actor = request
        .triggering_user_id
        .map(|user_id| SubmissionActor::GithubUser { user_id })
        .unwrap_or(SubmissionActor::System);
    let command = SubmissionCommand {
        actor,
        producer_key: ProducerKey {
            namespace: "github_webhook".into(),
            key: request.webhook_id.to_string(),
        },
        constraints: SchedulingConstraints::default(),
        task: TaskPlan::Benchmark(BenchmarkPlan {
            variants: vec![BenchmarkVariant {
                source: request.source.clone(),
                requested_run_count: 1,
                baseline_calibration_id: None,
            }],
            effective_args,
        }),
        provenance: SubmissionProvenance {
            queued_event_detail,
            github: Some(GithubSubmissionProvenance {
                webhook_id: request.webhook_id,
                triggering_user_id: request.triggering_user_id,
                pull_request_id: request.pull_request_id,
                triggering_comment_id: request.triggering_comment_id,
            }),
            slack: None,
        },
    };
    match crate::submission::submit(job_store, command).await {
        Ok(receipt) if receipt.disposition == SubmissionDisposition::Created => {
            let job_id = receipt.initial_job_ids[0];
            // The single "delivery X → job Y" breadcrumb: correlate the
            // source webhook to the created job + its routing keys.
            tracing::info!(
                submission_id = %receipt.submission_id,
                job_id = %job_id,
                webhook_id = request.webhook_id,
                installation_id = request.source.github_installation_id,
                repo_id = request.source.github_repo_id,
                source = ?request.source.source,
                intent = ?request.source.intent,
                task_kind = ?request.source.task_kind,
                build_target = ?request.source.build_target,
                git_ref = %request.source.git_ref_display,
                "enqueued task submission"
            );
            ClassifyOutcome::Terminal(WebhookOutcome::EnqueuedJob)
        }
        // Idempotent retry: a prior attempt already committed the canonical
        // aggregate for this webhook. Terminalize without minting a duplicate.
        Ok(_) => {
            tracing::info!(
                webhook_id = request.webhook_id,
                "webhook already maps to a task submission; no duplicate created"
            );
            ClassifyOutcome::Terminal(WebhookOutcome::EnqueuedJob)
        }
        Err(e) => ClassifyOutcome::Retryable(format!("submit task: {e}")),
    }
}

fn queued_detail(detail: QueuedEventDetail, effective_args: &[String]) -> serde_json::Value {
    let mut value = serde_json::to_value(&detail).expect("queued event detail always serializes");
    value
        .as_object_mut()
        .expect("queued event detail serializes as an object")
        .insert(
            "effective_args".into(),
            serde_json::to_value(effective_args).expect("benchmark arguments always serialize"),
        );
    value
}

/// Slice 7: shared PR materialisation. Upsert base+head repo
/// identity, the PR author, and the `github_pull_request` row in the
/// required FK order:
///
///   1. `github_repo` rows for base + head (identity-only — lineage walking
///      lives on `installation_repositories.added`)
///   2. `github_user` row for the author
///   3. `github_pull_request` row (target=base, source=head)
///
/// Called from both `PullRequestHandler` (data straight from the
/// payload) and `IssueCommentHandler` (data from the
/// `get_pull_request` API response — handles the case where the PR's
/// `opened` event predates the new pipeline).
///
/// On any storage error, returns `Err(ClassifyOutcome)` so callers
/// can propagate cleanly without bouncing through `?`.
#[allow(clippy::too_many_arguments)] // 3 stores + 5 payload pieces; bundling them would just be churn.
async fn materialise_pull_request(
    repo_store: &dyn RepoStore,
    user_store: &dyn UserStore,
    pull_request_store: &dyn PullRequestStore,
    base: PullRequestRepoInput<'_>,
    head: PullRequestRepoInput<'_>,
    pr_number: i32,
    title: &str,
    author: PullRequestAuthorInput<'_>,
) -> Result<sbgh_core::models::GithubPullRequest, ClassifyOutcome> {
    repo_store
        .upsert_repo_identity(&NewRepoIdentity {
            id: base.id,
            owner: base.owner.into(),
            name: base.name.into(),
            default_branch: None,
        })
        .await
        .map_err(|e| ClassifyOutcome::Retryable(format!("upsert_repo_identity(base): {e}")))?;
    repo_store
        .upsert_repo_identity(&NewRepoIdentity {
            id: head.id,
            owner: head.owner.into(),
            name: head.name.into(),
            default_branch: None,
        })
        .await
        .map_err(|e| ClassifyOutcome::Retryable(format!("upsert_repo_identity(head): {e}")))?;
    user_store
        .upsert_user(&NewUser {
            id: author.id,
            login: author.login.into(),
            user_type: author.account_type,
        })
        .await
        .map_err(|e| ClassifyOutcome::Retryable(format!("upsert_user(pr_author): {e}")))?;
    pull_request_store
        .upsert_pull_request(&NewPullRequest {
            target_github_repo_id: base.id,
            source_github_repo_id: head.id,
            pr_number,
            title: title.to_string(),
            author_github_user_id: author.id,
        })
        .await
        .map_err(|e| ClassifyOutcome::Retryable(format!("upsert_pull_request: {e}")))
}

/// Slice 7 input for the shared materialiser. Borrowed so the
/// caller can pass refs from a webhook payload or an API response
/// without cloning.
pub struct PullRequestRepoInput<'a> {
    pub id: i64,
    pub owner: &'a str,
    pub name: &'a str,
}

pub struct PullRequestAuthorInput<'a> {
    pub id: i64,
    pub login: &'a str,
    pub account_type: GithubAccountType,
}

/// Handles `pull_request.{opened,reopened,synchronize}` events. Phase 1:
/// resolves the PR's base+head repo identities (caches them in
/// github_repo for slice 7+ PR materialisation), evaluates target+source
/// policies, and TERMINATES with a logged decision. Slice 9 will create
/// new-schema `job` rows for accepted PRs.
///
/// Other actions (closed, labeled, etc.) terminate as `IgnoredAction`
/// — we don't kick off benchmarks on those. The supported_event_types
/// list is `["pull_request"]`; per-action filtering happens here.
pub struct PullRequestHandler {
    repo_store: Arc<dyn RepoStore>,
    policy_store: Arc<dyn PolicyStore>,
    /// Slice 5 review fix: gate the policy eval on active membership
    /// even if the policy row says is_enabled.
    install_store: Arc<dyn InstallationStore>,
    /// Slice 6: upsert the PR author into `github_user` so slice 7's
    /// PR-subject FK target exists by the time PR materialisation runs.
    /// No authz happens here — authoring a PR doesn't require a role
    /// grant; only the `/benchmark` trigger does.
    user_store: Arc<dyn UserStore>,
    /// Slice 7: materialise the `github_pull_request` row so slice 9's
    /// `/benchmark` jobs can link back to a known PR. The shared
    /// `materialise_pull_request` helper is also called from
    /// `IssueCommentHandler` so a `/benchmark` comment can succeed
    /// even when the PR's `opened` event predates the new pipeline.
    pull_request_store: Arc<dyn PullRequestStore>,
}

impl PullRequestHandler {
    pub fn new(
        repo_store: Arc<dyn RepoStore>,
        policy_store: Arc<dyn PolicyStore>,
        install_store: Arc<dyn InstallationStore>,
        user_store: Arc<dyn UserStore>,
        pull_request_store: Arc<dyn PullRequestStore>,
    ) -> Self {
        Self {
            repo_store,
            policy_store,
            install_store,
            user_store,
            pull_request_store,
        }
    }
}

#[async_trait]
impl EventHandler for PullRequestHandler {
    fn event_type(&self) -> &'static str {
        "pull_request"
    }

    async fn handle(&self, webhook: &ClaimedWebhook) -> ClassifyOutcome {
        let Some(payload) = webhook.payload.as_ref() else {
            return ClassifyOutcome::Terminal(WebhookOutcome::Error);
        };
        let event: PullRequestEvent = match serde_json::from_value(payload.clone()) {
            Ok(e) => e,
            Err(_) => return ClassifyOutcome::Terminal(WebhookOutcome::Error),
        };

        // Slice 7 lifecycle dispatch (post-slice-7 review reordering):
        // match on action FIRST, then only require the repo fields
        // each branch actually uses. GH may omit `pull_request.head.repo`
        // when a deleted fork branch leaves the PR's source orphaned;
        // returning Error for `closed` / `labeled` / etc. in that case
        // loses the close/no-op signal.
        //
        //   ignored-by-default (labeled, unlabeled, assigned, …) →
        //     IgnoredAction immediately; no repo access needed.
        //   closed → only base.repo needed (key for set_closed_at).
        //     If base.repo is missing, defensively IgnoredAction —
        //     can't locate the PR, but losing a close signal is better
        //     than terminating as Error.
        //   opened / reopened / synchronize → need BOTH base.repo and
        //     head.repo for materialise + policy eval. Missing →
        //     Error (we can't materialise the PR row).
        //   edited → need both repos. Title-only edits refresh the
        //     PR row but DO NOT re-run policy eval (avoiding the
        //     "title edit starts benchmark" footgun once slice 9
        //     flips WouldEnqueueJob to job creation). Policy eval
        //     re-runs only when `changes.base` is present in the
        //     payload, indicating the operator actually changed the
        //     base ref.
        let install_id = event.installation.id;

        // Fast-path: ignored-by-default actions require no repo access.
        if !matches!(
            event.action.as_str(),
            "closed" | "opened" | "reopened" | "synchronize" | "edited"
        ) {
            return ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction);
        }

        // Closed: only base.repo needed.
        if event.action == "closed" {
            let Some(base_repo) = event
                .pull_request
                .base
                .repo
                .as_ref()
            else {
                // Defensive: closed without base.repo is unprecedented
                // in practice but we'd rather terminate as IgnoredAction
                // (no DB side effect) than Error.
                return ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction);
            };
            if let Err(e) = self
                .pull_request_store
                .set_closed_at(base_repo.id, event.pull_request.number as i32, Some(Utc::now()))
                .await
            {
                return ClassifyOutcome::Retryable(format!("set_closed_at: {e}"));
            }
            return ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction);
        }

        // opened / reopened / synchronize / edited: full materialisation
        // path. Both base.repo and head.repo are required (the PR row's
        // source FK targets head).
        let Some(base_repo) = event
            .pull_request
            .base
            .repo
            .as_ref()
        else {
            return ClassifyOutcome::Terminal(WebhookOutcome::Error);
        };
        let Some(head_repo) = event
            .pull_request
            .head
            .repo
            .as_ref()
        else {
            return ClassifyOutcome::Terminal(WebhookOutcome::Error);
        };

        let (base_owner, base_name) =
            split_full_name(&base_repo.full_name).unwrap_or((&base_repo.full_name, ""));
        let (head_owner, head_name) =
            split_full_name(&head_repo.full_name).unwrap_or((&head_repo.full_name, ""));
        let author = &event.pull_request.user;
        let Some(author_type) = parse_account_type(&author.account_type) else {
            return ClassifyOutcome::Terminal(WebhookOutcome::Error);
        };
        if let Err(out) = materialise_pull_request(
            self.repo_store.as_ref(),
            self.user_store.as_ref(),
            self.pull_request_store
                .as_ref(),
            PullRequestRepoInput {
                id: base_repo.id,
                owner: base_owner,
                name: base_name,
            },
            PullRequestRepoInput {
                id: head_repo.id,
                owner: head_owner,
                name: head_name,
            },
            event.pull_request.number as i32,
            &event.pull_request.title,
            PullRequestAuthorInput {
                id: author.id,
                login: &author.login,
                account_type: author_type,
            },
        )
        .await
        {
            return out;
        }

        // Reopened: clear any prior closed_at on the existing row.
        // Idempotent if already None.
        if event.action == "reopened"
            && let Err(e) = self
                .pull_request_store
                .set_closed_at(base_repo.id, event.pull_request.number as i32, None)
                .await
        {
            return ClassifyOutcome::Retryable(format!("set_closed_at(reopened): {e}"));
        }

        // Edited: skip policy eval unless `changes.base` is present.
        // Title/body/etc. edits absorbed by the materialise upsert
        // above but produce no Phase-1 enqueue signal.
        if event.action == "edited" {
            let base_changed = event
                .changes
                .as_ref()
                .and_then(|c| c.base.as_ref())
                .is_some();
            if !base_changed {
                tracing::debug!(
                    installation_id = install_id,
                    pr_number = event.pull_request.number,
                    "pull_request edited: non-base change, title refreshed but policy NOT \
                     re-evaluated"
                );
                return ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction);
            }
            tracing::info!(
                installation_id = install_id,
                pr_number = event.pull_request.number,
                "pull_request edited: base ref changed, re-running policy eval"
            );
        }

        match evaluate_pr_policies(
            self.install_store.as_ref(),
            self.policy_store.as_ref(),
            install_id,
            base_repo.id,
            head_repo.id,
        )
        .await
        {
            PolicyEvaluation::Accepted => {
                // Slice 9: pull_request events do NOT create jobs. There
                // is no `trigger_kind` for PR-event auto-benchmarking,
                // and auto-benching every PR push is a separate product
                // decision (a future `pr_updated`/`pr_auto` trigger +
                // policy knob). The PR row has been materialised/updated
                // above so a later `/benchmark` comment can link to it;
                // we terminate as `ProcessedPullRequest` — neither a
                // no-op `IgnoredAction` nor the now-misleading
                // `WouldEnqueueJob`.
                tracing::info!(
                    installation_id = install_id,
                    pr_number = event.pull_request.number,
                    base_repo_id = base_repo.id,
                    head_repo_id = head_repo.id,
                    "pull_request: policies accepted — PR state materialised, no job (use \
                     /benchmark to enqueue)"
                );
                ClassifyOutcome::Terminal(WebhookOutcome::ProcessedPullRequest)
            }
            PolicyEvaluation::DeniedTarget => {
                ClassifyOutcome::Terminal(WebhookOutcome::DeniedTargetPolicy)
            }
            PolicyEvaluation::DeniedSource => {
                ClassifyOutcome::Terminal(WebhookOutcome::DeniedSourcePolicy)
            }
            PolicyEvaluation::Retryable(msg) => ClassifyOutcome::Retryable(msg),
        }
    }
}

/// Handles `push` events. Phase 1: looks up `trigger_kind = branch_push`
/// rows for (install, repo), matches each `match_spec.branch_name`
/// against the stripped ref. Any match → log "would enqueue" + terminate
/// as `WouldEnqueueJob` (slice 9 flips this to `EnqueuedJob`). No match
/// → `IgnoredAction` with no log.
///
/// Refs come in as `refs/heads/<name>` from GitHub; we strip the prefix
/// before matching. Non-branch refs (rare on `push`, but possible for
/// internal refs) are silently skipped.
pub struct PushHandler {
    policy_store: Arc<dyn PolicyStore>,
    install_store: Arc<dyn InstallationStore>,
    /// Slice 9: a matched `branch_push` trigger creates a `baseline`
    /// job instead of emitting the Phase-1 `WouldEnqueueJob` signal.
    job_store: Arc<dyn JobStore>,
    /// roadmap-v7: configured `default_args` for the baseline's `workload_key`
    /// (a trigger with NULL `bench_args` resolves to these). Set via
    /// [`Self::with_default_args`]; defaults to empty.
    default_args: String,
}

impl PushHandler {
    pub fn new(
        policy_store: Arc<dyn PolicyStore>,
        install_store: Arc<dyn InstallationStore>,
        job_store: Arc<dyn JobStore>,
    ) -> Self {
        Self {
            policy_store,
            install_store,
            job_store,
            default_args: String::new(),
        }
    }

    /// roadmap-v7: supply the configured `default_args` so baseline jobs get a
    /// `workload_key`. Builder-style so existing call sites need no change.
    pub fn with_default_args(mut self, default_args: impl Into<String>) -> Self {
        self.default_args = default_args.into();
        self
    }
}

#[async_trait]
impl EventHandler for PushHandler {
    fn event_type(&self) -> &'static str {
        "push"
    }

    async fn handle(&self, webhook: &ClaimedWebhook) -> ClassifyOutcome {
        let Some(payload) = webhook.payload.as_ref() else {
            return ClassifyOutcome::Terminal(WebhookOutcome::Error);
        };
        let event: PushEvent = match serde_json::from_value(payload.clone()) {
            Ok(e) => e,
            Err(_) => return ClassifyOutcome::Terminal(WebhookOutcome::Error),
        };
        let Some(branch) = event
            .ref_field
            .strip_prefix("refs/heads/")
        else {
            return ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction);
        };

        // Slice 5 review fix: gate on active membership BEFORE the
        // trigger lookup. A trigger row whose membership was revoked
        // (or whose install was deleted / suspended) must not match
        // — otherwise the slice-9 cutover would start enqueuing jobs
        // for a (install, repo) pair the operator already retired.
        match self
            .install_store
            .is_membership_active(event.installation.id, event.repository.id)
            .await
        {
            Ok(true) => {}
            Ok(false) => return ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction),
            Err(e) => return ClassifyOutcome::Retryable(format!("is_membership_active: {e}")),
        }

        let triggers = match self
            .policy_store
            .list_enabled_triggers(
                event.installation.id,
                event.repository.id,
                TriggerKind::BranchPush,
            )
            .await
        {
            Ok(t) => t,
            Err(e) => return ClassifyOutcome::Retryable(format!("list_enabled_triggers: {e}")),
        };

        let matched: Vec<&TriggerPolicy> = triggers
            .iter()
            .filter(|t| matches_branch_push(&t.match_spec, branch))
            .collect();
        let Some(trigger) = matched.first().copied() else {
            tracing::debug!(
                installation_id = event.installation.id,
                repo_id = event.repository.id,
                branch,
                trigger_count = triggers.len(),
                "push: no branch_push trigger matched"
            );
            return ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction);
        };
        // Slice 9 creates one job per accepted webhook (see `enqueue_job`);
        // surface multi-trigger matches rather than silently dropping them.
        if matched.len() > 1 {
            tracing::warn!(
                installation_id = event.installation.id,
                repo_id = event.repository.id,
                branch,
                matched = matched.len(),
                used_trigger_id = trigger.id,
                "push: multiple branch_push triggers matched; slice 9 enqueues one job for the \
                 first (multi-trigger fan-out deferred)"
            );
        }

        // A branch deletion (or a push introducing no commits) has no
        // head_commit — nothing to benchmark, so don't enqueue.
        let Some(head_commit) = event.head_commit.as_ref() else {
            tracing::info!(
                installation_id = event.installation.id,
                repo_id = event.repository.id,
                branch,
                "push: branch_push trigger matched but no head_commit (deletion?) — not enqueuing"
            );
            return ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction);
        };

        tracing::info!(
            installation_id = event.installation.id,
            repo_id = event.repository.id,
            branch,
            trigger_id = trigger.id,
            "push: branch_push trigger matched — creating job"
        );
        let detail = QueuedEventDetail::BranchPush {
            branch: branch.to_string(),
            trigger_id: trigger.id,
            bench_args: trigger.bench_args.clone(),
        };
        let resolved = resolve_bench_args(&normalize_stored(&detail), &self.default_args);
        let workload_key = resolved.workload_key.clone();
        let request = GithubBenchmarkSubmission {
            source: ResolvedTaskSource {
                github_installation_id: event.installation.id,
                github_repo_id: event.repository.id,
                source: JobSource::GithubWebhook,
                intent: JobIntent::BaselineBenchmark,
                task_kind: TaskKind::Benchmark,
                build_target: BuildTarget::StacksBench,
                git_ref_kind: GitRefKind::Branch,
                git_ref_display: branch.to_string(),
                commit: head_commit.id.clone(),
                committed_at: Some(head_commit.timestamp),
                workload_key: Some(workload_key),
            },
            webhook_id: webhook.id,
            triggering_user_id: None,
            pull_request_id: None,
            triggering_comment_id: None,
            queued_event_detail: queued_detail(detail, &resolved.effective_args),
        };
        enqueue_job(self.job_store.as_ref(), request).await
    }
}

/// Handles `create` events. GitHub fires `create` for branch + tag
/// creation; slice 5 only evaluates `trigger_kind = tag_created`. The
/// `ref_type` field distinguishes — `"branch"` refs are silently skipped
/// (those would have fired a `push` event anyway).
pub struct CreateHandler {
    policy_store: Arc<dyn PolicyStore>,
    install_store: Arc<dyn InstallationStore>,
    /// A matched `tag_created` trigger resolves the mutable tag before
    /// submitting immutable baseline demand.
    job_store: Arc<dyn JobStore>,
    github: Option<Arc<dyn GitHubApi>>,
    /// roadmap-v7: configured `default_args` for the baseline's `workload_key`
    /// (a trigger with NULL `bench_args` resolves to these). Set via
    /// [`Self::with_default_args`]; defaults to empty.
    default_args: String,
}

impl CreateHandler {
    pub fn new(
        policy_store: Arc<dyn PolicyStore>,
        install_store: Arc<dyn InstallationStore>,
        job_store: Arc<dyn JobStore>,
    ) -> Self {
        Self {
            policy_store,
            install_store,
            job_store,
            github: None,
            default_args: String::new(),
        }
    }

    pub fn with_github(mut self, github: Arc<dyn GitHubApi>) -> Self {
        self.github = Some(github);
        self
    }

    /// roadmap-v7: supply the configured `default_args` so baseline jobs get a
    /// `workload_key`. Builder-style so existing call sites need no change.
    pub fn with_default_args(mut self, default_args: impl Into<String>) -> Self {
        self.default_args = default_args.into();
        self
    }
}

#[async_trait]
impl EventHandler for CreateHandler {
    fn event_type(&self) -> &'static str {
        "create"
    }

    async fn handle(&self, webhook: &ClaimedWebhook) -> ClassifyOutcome {
        let Some(payload) = webhook.payload.as_ref() else {
            return ClassifyOutcome::Terminal(WebhookOutcome::Error);
        };
        let event: CreateEvent = match serde_json::from_value(payload.clone()) {
            Ok(e) => e,
            Err(_) => return ClassifyOutcome::Terminal(WebhookOutcome::Error),
        };
        if event.ref_type != "tag" {
            return ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction);
        }

        // Slice 5 review fix (same as PushHandler): membership gate.
        match self
            .install_store
            .is_membership_active(event.installation.id, event.repository.id)
            .await
        {
            Ok(true) => {}
            Ok(false) => return ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction),
            Err(e) => return ClassifyOutcome::Retryable(format!("is_membership_active: {e}")),
        }

        let triggers = match self
            .policy_store
            .list_enabled_triggers(
                event.installation.id,
                event.repository.id,
                TriggerKind::TagCreated,
            )
            .await
        {
            Ok(t) => t,
            Err(e) => return ClassifyOutcome::Retryable(format!("list_enabled_triggers: {e}")),
        };

        let mut matched: Vec<&TriggerPolicy> = Vec::new();
        for t in &triggers {
            match matches_tag_created(&t.match_spec, &event.ref_field) {
                Ok(true) => matched.push(t),
                Ok(false) => {}
                Err(msg) => {
                    // Malformed operator-supplied regex. Log + skip
                    // THIS trigger, don't fail the batch — other
                    // triggers may match correctly. Operator can fix
                    // via `sbgh-cli policy trigger disable --id ...`.
                    tracing::warn!(
                        trigger_id = t.id,
                        error = msg.as_str(),
                        "create: tag_created trigger has malformed match_spec, skipping"
                    );
                }
            }
        }
        let Some(trigger) = matched.first().copied() else {
            tracing::debug!(
                installation_id = event.installation.id,
                repo_id = event.repository.id,
                tag = event.ref_field.as_str(),
                trigger_count = triggers.len(),
                "create: no tag_created trigger matched"
            );
            return ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction);
        };
        // Slice 9 creates one job per accepted webhook (see `enqueue_job`);
        // surface multi-trigger matches rather than silently dropping them.
        if matched.len() > 1 {
            tracing::warn!(
                installation_id = event.installation.id,
                repo_id = event.repository.id,
                tag = event.ref_field.as_str(),
                matched = matched.len(),
                used_trigger_id = trigger.id,
                "create: multiple tag_created triggers matched; slice 9 enqueues one job for the \
                 first (multi-trigger fan-out deferred)"
            );
        }

        tracing::info!(
            installation_id = event.installation.id,
            repo_id = event.repository.id,
            tag = event.ref_field.as_str(),
            trigger_id = trigger.id,
            "create: tag_created trigger matched — creating job"
        );
        let detail = QueuedEventDetail::TagCreated {
            tag: event.ref_field.clone(),
            trigger_id: trigger.id,
            bench_args: trigger.bench_args.clone(),
        };
        let resolved = resolve_bench_args(&normalize_stored(&detail), &self.default_args);
        let workload_key = resolved.workload_key.clone();
        let Some(github) = &self.github else {
            return ClassifyOutcome::Retryable(
                "tag submission requires a GitHub ref resolver".into(),
            );
        };
        let repository = event
            .repository
            .full_name
            .clone();
        let resolved_commit = match github
            .resolve_commit(
                event.installation.id,
                &repository,
                &format!("tags/{}", event.ref_field),
            )
            .await
        {
            Ok(commit) => commit,
            Err(error) => {
                return ClassifyOutcome::Retryable(format!(
                    "resolve tag before submission: {error}"
                ));
            }
        };
        let request = GithubBenchmarkSubmission {
            source: ResolvedTaskSource {
                github_installation_id: event.installation.id,
                github_repo_id: event.repository.id,
                source: JobSource::GithubWebhook,
                intent: JobIntent::BaselineBenchmark,
                task_kind: TaskKind::Benchmark,
                build_target: BuildTarget::StacksBench,
                git_ref_kind: GitRefKind::Tag,
                git_ref_display: event.ref_field.clone(),
                commit: resolved_commit.hash,
                committed_at: resolved_commit.committed_at,
                workload_key: Some(workload_key),
            },
            webhook_id: webhook.id,
            triggering_user_id: None,
            pull_request_id: None,
            triggering_comment_id: None,
            queued_event_detail: queued_detail(detail, &resolved.effective_args),
        };
        enqueue_job(self.job_store.as_ref(), request).await
    }
}

/// Compare a `branch_push` match_spec JSON against the inbound branch
/// name. Returns true on exact match; ignores unknown / wrong-kind
/// match_specs (the trigger_kind column already disambiguates).
///
/// Also reused by the binary-cache pin resolver ([`crate::pin_resolver`]) so
/// the pinned-ref set is selected by the *exact same* predicate that fires the
/// trigger — a ref the daemon would auto-bench is the ref whose binary gets
/// pinned.
pub fn matches_branch_push(match_spec: &serde_json::Value, branch: &str) -> bool {
    match serde_json::from_value::<TriggerMatchSpec>(match_spec.clone()) {
        Ok(TriggerMatchSpec::BranchPush { branch_name }) => branch_name == branch,
        // Plain prefix (item 0025, v9): release-branch families like
        // `sb-integration/3.` auto-trigger + get their binary pin-cached.
        Ok(TriggerMatchSpec::BranchPrefix { prefix }) => branch.starts_with(&prefix),
        _ => false,
    }
}

/// Compare a `tag_created` match_spec JSON's regex against the inbound
/// tag name. Returns `Err(msg)` on regex parse failure so the caller
/// can log + skip the trigger without blowing up the whole batch.
///
/// Reused by the binary-cache pin resolver ([`crate::pin_resolver`]) — see
/// [`matches_branch_push`].
pub fn matches_tag_created(match_spec: &serde_json::Value, tag: &str) -> Result<bool, String> {
    match serde_json::from_value::<TriggerMatchSpec>(match_spec.clone()) {
        Ok(TriggerMatchSpec::TagCreated { tag_pattern }) => regex::Regex::new(&tag_pattern)
            .map(|re| re.is_match(tag))
            .map_err(|e| format!("invalid tag_pattern regex: {e}")),
        Ok(_) => Ok(false),
        Err(e) => Err(format!("match_spec parse failed: {e}")),
    }
}

/// Tunables. Reasonable defaults are picked to play nicely with
/// GitHub's redelivery cadence and a single-daemon deployment.
#[derive(Debug, Clone)]
pub struct ProcessorConfig {
    /// Permanent-failure threshold: when `attempts >= max_attempts`
    /// after a transient failure, the row goes to `failed` instead of
    /// `retryable_error`.
    pub max_attempts: i32,
    /// First retry waits this long; subsequent retries double until
    /// `backoff_max`.
    pub backoff_base: chrono::Duration,
    pub backoff_max: chrono::Duration,
    /// A `processing` row whose `claimed_at` exceeds this is presumed
    /// abandoned and reset to `retryable_error` by the next sweep.
    pub claim_lease: chrono::Duration,
    /// Sleep when no rows are claimable.
    pub idle_sleep: std::time::Duration,
    /// How often `sweep_stuck_claims` runs from inside the main loop.
    pub sweep_interval: std::time::Duration,
    /// Run loop bails after this many consecutive iteration errors
    /// (claim / sweep / row processing). Forces a systemd restart so
    /// persistent infrastructure problems (DB down, grants revoked,
    /// schema drift) are surfaced as a crash rather than spin-logged
    /// silently. Per-row classification errors don't count — those
    /// land as terminal `error` rows.
    pub max_consecutive_errors: u32,
    /// Slice 7 (pre-slice-6 checkpoint todo): how long to keep
    /// payloads on terminal `ignored` / `denied` / `failed` rows
    /// before NULL-ing the `payload` JSONB. `payload_size_bytes`
    /// and `last_error` survive the clear. Default 24h gives ops
    /// a full day of observation before the payload goes.
    pub payload_retention: chrono::Duration,
}

impl Default for ProcessorConfig {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            backoff_base: chrono::Duration::seconds(30),
            backoff_max: chrono::Duration::minutes(15),
            claim_lease: chrono::Duration::minutes(5),
            idle_sleep: std::time::Duration::from_secs(2),
            sweep_interval: std::time::Duration::from_secs(60),
            max_consecutive_errors: 10,
            payload_retention: chrono::Duration::hours(24),
        }
    }
}

/// Whether a terminal outcome warrants a `warn` (vs `info`) in the
/// processor's per-webhook result log. Denials, policy violations, and
/// unclassifiable-payload errors are the operationally interesting ones;
/// everything else (ignored no-ops, processed state, enqueued jobs) is
/// routine.
fn is_alarming_outcome(outcome: WebhookOutcome) -> bool {
    matches!(
        outcome,
        WebhookOutcome::DeniedInstallAllowlist
            | WebhookOutcome::DeniedTargetPolicy
            | WebhookOutcome::DeniedSourcePolicy
            | WebhookOutcome::DeniedUnauthorized
            | WebhookOutcome::Error
    )
}

pub struct WebhookProcessor {
    inbox: Arc<dyn WebhookInbox>,
    classifier: Arc<dyn Classifier>,
    config: ProcessorConfig,
}

impl WebhookProcessor {
    pub fn new(
        inbox: Arc<dyn WebhookInbox>,
        classifier: Arc<dyn Classifier>,
        config: ProcessorConfig,
    ) -> Self {
        Self { inbox, classifier, config }
    }

    /// Claim + classify + write outcome for a single row. Returns
    /// `Ok(true)` if a row was processed, `Ok(false)` if the inbox was
    /// empty (idle). The claim is restricted to event types the
    /// classifier knows how to handle; rows for other types stay
    /// `received` for a future processor with a wider filter.
    pub async fn process_one(&self) -> Result<bool> {
        let supported = self
            .classifier
            .supported_event_types();
        let Some(claimed) = self
            .inbox
            .claim_next(supported)
            .await?
        else {
            return Ok(false);
        };
        let id = claimed.id;
        let token = claimed.claim_token;
        tracing::info!(
            webhook_id = id,
            event = %claimed.event_type,
            action = claimed
                .action
                .as_deref()
                .unwrap_or("-"),
            delivery = %claimed.delivery_id,
            installation_id = ?claimed.payload_installation_id,
            attempt = claimed.attempts + 1,
            "claimed webhook; classifying"
        );
        match self
            .classifier
            .classify(&claimed)
            .await
        {
            ClassifyOutcome::Terminal(outcome) => {
                // Single place that levels EVERY webhook's result —
                // including the early-return Error/Ignored cases the
                // handlers don't log themselves. Deny / policy-violation
                // / hard-error terminals are operationally interesting →
                // warn; routine outcomes (ignored, processed, enqueued) →
                // info.
                if is_alarming_outcome(outcome) {
                    tracing::warn!(
                        webhook_id = id,
                        event = %claimed.event_type,
                        action = claimed.action.as_deref().unwrap_or("-"),
                        ?outcome,
                        "webhook classified (terminal)"
                    );
                } else {
                    tracing::info!(
                        webhook_id = id,
                        event = %claimed.event_type,
                        action = claimed.action.as_deref().unwrap_or("-"),
                        ?outcome,
                        "webhook classified (terminal)"
                    );
                }
                self.inbox
                    .complete(id, token, outcome)
                    .await?;
            }
            ClassifyOutcome::Retryable(err) => {
                // attempts is the value BEFORE this run; the DB
                // increments it inside record_retryable_error. We
                // compare against max_attempts using the value the
                // increment will produce (claimed.attempts + 1).
                let next_attempts = claimed
                    .attempts
                    .saturating_add(1);
                if next_attempts >= self.config.max_attempts {
                    tracing::error!(
                        webhook_id = id,
                        event = %claimed.event_type,
                        attempts = next_attempts,
                        error = %err,
                        "webhook permanently failed (max attempts reached)"
                    );
                    self.inbox
                        .record_permanent_failure(id, token, &err)
                        .await?;
                } else {
                    let delay = backoff_delay(
                        next_attempts,
                        self.config.backoff_base,
                        self.config.backoff_max,
                    );
                    let next_at = Utc::now() + delay;
                    tracing::warn!(
                        webhook_id = id,
                        event = %claimed.event_type,
                        attempt = next_attempts,
                        retry_at = %next_at,
                        error = %err,
                        "webhook classification failed; will retry"
                    );
                    self.inbox
                        .record_retryable_error(id, token, &err, next_at)
                        .await?;
                }
            }
        }
        Ok(true)
    }

    /// Long-running loop: alternates `process_one` with periodic
    /// stuck-claim sweeps and idle backoff. Per-iteration errors
    /// (claim / sweep / row processing) are logged and the loop
    /// continues so a transient hiccup doesn't crash the processor.
    /// BUT: after `max_consecutive_errors` consecutive failures of
    /// EITHER category — process_one or sweep — the loop bails,
    /// forcing a systemd restart rather than silently spin-logging
    /// through a persistent DB or grant problem.
    ///
    /// The two counters are tracked independently so a persistent
    /// sweep failure (e.g., a grant revoked on a column the sweep
    /// updates) can't be masked by an otherwise-healthy process loop —
    /// without the split, a successful `process_one` after a failed
    /// sweep would reset the shared counter and the sweep could stay
    /// broken indefinitely.
    pub async fn run(&self) -> Result<()> {
        tracing::info!(
            supported_events = ?self.classifier.supported_event_types(),
            max_attempts = self.config.max_attempts,
            claim_lease_secs = self.config.claim_lease.num_seconds(),
            sweep_interval_secs = self.config.sweep_interval.as_secs(),
            payload_retention_hours = self.config.payload_retention.num_hours(),
            "webhook processor started"
        );
        let mut last_sweep = Instant::now();
        let mut consecutive_sweep_errors: u32 = 0;
        let mut consecutive_process_errors: u32 = 0;
        loop {
            if last_sweep.elapsed() >= self.config.sweep_interval {
                match self
                    .inbox
                    .sweep_stuck_claims(self.config.claim_lease)
                    .await
                {
                    Ok(n) if n > 0 => {
                        tracing::warn!(recovered = n, "stuck-claim sweep recovered rows");
                        consecutive_sweep_errors = 0;
                    }
                    Ok(_) => {
                        consecutive_sweep_errors = 0;
                    }
                    Err(e) => {
                        tracing::error!(error = ?e, "stuck-claim sweep failed");
                        consecutive_sweep_errors += 1;
                    }
                }
                // Slice 7: NULL payload on terminal rows past the
                // retention window in the same sweep tick. Cheap
                // SQL, no contention with claim path (different
                // status filter). Shares the sweep_errors counter
                // with sweep_stuck_claims — both are "background
                // housekeeping" from the loop's POV.
                match self
                    .inbox
                    .clear_terminal_payloads(self.config.payload_retention)
                    .await
                {
                    Ok(n) if n > 0 => {
                        tracing::info!(cleared = n, "terminal payloads cleared");
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::error!(error = ?e, "terminal-payload clear failed");
                        consecutive_sweep_errors += 1;
                    }
                }
                last_sweep = Instant::now();
            }

            match self.process_one().await {
                Ok(true) => {
                    consecutive_process_errors = 0;
                }
                Ok(false) => {
                    consecutive_process_errors = 0;
                    tokio::time::sleep(self.config.idle_sleep).await;
                }
                Err(e) => {
                    tracing::error!(error = ?e, "webhook processor iteration failed");
                    consecutive_process_errors += 1;
                    tokio::time::sleep(self.config.idle_sleep).await;
                }
            }

            // Either category persistently broken → bail. Common root
            // causes: DB pool exhausted, role grants revoked, schema
            // drift mid-deploy.
            let max = self
                .config
                .max_consecutive_errors;
            if consecutive_process_errors >= max || consecutive_sweep_errors >= max {
                return Err(sbgh_core::Error::Other(anyhow::anyhow!(
                    "webhook processor bailing for systemd restart (consecutive process errors: \
                     {consecutive_process_errors}, consecutive sweep errors: \
                     {consecutive_sweep_errors})"
                )));
            }
        }
    }
}

/// Exponential backoff: `base * 2^(attempt-1)`, capped at `max`.
/// `attempt` is the 1-indexed attempt number (1 for the first retry).
fn backoff_delay(attempt: i32, base: chrono::Duration, max: chrono::Duration) -> chrono::Duration {
    let exp = attempt
        .saturating_sub(1)
        .clamp(0, 30) as u32;
    let factor = 1i64
        .checked_shl(exp)
        .unwrap_or(i64::MAX);
    let scaled_secs = base
        .num_seconds()
        .saturating_mul(factor);
    let raw = chrono::Duration::seconds(scaled_secs);
    if raw > max { max } else { raw }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use sbgh_core::models::{WebhookOutcome, WebhookStatus};
    use sbgh_postgres::db::{
        Pool, PostgresInstallationStore, PostgresJobStore, PostgresPolicyStore,
        PostgresPullRequestStore, PostgresRepoStore, PostgresUserStore, PostgresWebhookInbox,
        TestDb, setup_pg_db,
    };

    use super::*;

    #[test]
    fn block_validation_command_is_bounded_and_typed() {
        assert_eq!(
            parse_issue_task_command("/validate-blocks nakamoto 185630 185999").unwrap(),
            Some(IssueTaskCommand::BlockValidation {
                epoch: ValidationEpoch::Nakamoto,
                range: InclusiveRange { start: 185_630, end: 185_999 },
            })
        );
        assert!(parse_issue_task_command("/validate-blocks nakamoto 20 10").is_err());
        assert!(parse_issue_task_command("/validate-blocks nakamoto 1 2 extra").is_err());
        assert_eq!(parse_issue_task_command("please /validate-blocks nakamoto 1 2"), Ok(None));
    }

    /// Wide event-type filter for tests that exercise the inbox
    /// lifecycle directly (i.e. not through a Classifier).
    const ALL_EVENT_TYPES: &[&str] = &[
        "issue_comment",
        "push",
        "pull_request",
        "create",
        "installation",
        "installation_repositories",
    ];

    fn fast_config() -> ProcessorConfig {
        // Tight defaults so backoff/sweep tests don't sit on
        // wall-clock. backoff_base of 1s and claim_lease of 100ms keep
        // tests deterministic without time mocking.
        ProcessorConfig {
            max_attempts: 3,
            backoff_base: chrono::Duration::seconds(1),
            backoff_max: chrono::Duration::seconds(10),
            claim_lease: chrono::Duration::milliseconds(100),
            idle_sleep: std::time::Duration::from_millis(10),
            sweep_interval: std::time::Duration::from_millis(50),
            max_consecutive_errors: 10,
            payload_retention: chrono::Duration::hours(24),
        }
    }

    struct WebhookRow {
        status: WebhookStatus,
        outcome: Option<WebhookOutcome>,
        claimed_at: Option<chrono::DateTime<Utc>>,
        claim_token: Option<uuid::Uuid>,
        next_attempt_at: chrono::DateTime<Utc>,
        attempts: i32,
        last_error: Option<String>,
        processed_at: Option<chrono::DateTime<Utc>>,
    }

    #[derive(sqlx::FromRow)]
    struct RawWebhookRow {
        status: String,
        outcome: Option<String>,
        claimed_at: Option<chrono::DateTime<Utc>>,
        claim_token: Option<uuid::Uuid>,
        next_attempt_at: chrono::DateTime<Utc>,
        attempts: i32,
        last_error: Option<String>,
        processed_at: Option<chrono::DateTime<Utc>>,
    }

    async fn seed(pool: &Pool, delivery: &str, event: &str) -> i64 {
        sqlx::query_scalar(
            "INSERT INTO github_webhook (delivery_id, event_type, payload_size_bytes) \
             VALUES ($1, $2, 42) RETURNING id",
        )
        .bind(delivery)
        .bind(event)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn test_inbox() -> (TestDb, Pool, Arc<PostgresWebhookInbox>) {
        let (db, pool) = setup_pg_db().await;
        let inbox = Arc::new(PostgresWebhookInbox::new(pool.clone()));
        (db, pool, inbox)
    }

    async fn test_installation_store() -> (TestDb, Arc<PostgresInstallationStore>) {
        let (db, pool) = setup_pg_db().await;
        (db, Arc::new(PostgresInstallationStore::new(pool)))
    }

    fn lazy_pool() -> Pool {
        sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused:unused@localhost/unused")
            .unwrap()
    }

    async fn row(pool: &Pool, id: i64) -> WebhookRow {
        let raw: RawWebhookRow = sqlx::query_as(
            "SELECT status::text, outcome::text, claimed_at, claim_token, next_attempt_at, \
             attempts, last_error, processed_at FROM github_webhook WHERE id = $1",
        )
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap();
        WebhookRow {
            status: serde_json::from_value(serde_json::Value::String(raw.status)).unwrap(),
            outcome: raw
                .outcome
                .map(|value| serde_json::from_value(serde_json::Value::String(value)).unwrap()),
            claimed_at: raw.claimed_at,
            claim_token: raw.claim_token,
            next_attempt_at: raw.next_attempt_at,
            attempts: raw.attempts,
            last_error: raw.last_error,
            processed_at: raw.processed_at,
        }
    }

    async fn set_next_attempt_at(pool: &Pool, id: i64, when: chrono::DateTime<Utc>) {
        sqlx::query("UPDATE github_webhook SET next_attempt_at = $2 WHERE id = $1")
            .bind(id)
            .bind(when)
            .execute(pool)
            .await
            .unwrap();
    }

    async fn set_claimed_at(pool: &Pool, id: i64, when: chrono::DateTime<Utc>) {
        sqlx::query("UPDATE github_webhook SET claimed_at = $2 WHERE id = $1")
            .bind(id)
            .bind(when)
            .execute(pool)
            .await
            .unwrap();
    }

    /// Test classifier that pops a programmed outcome per call.
    /// Records every classified id for assertions.
    struct ScriptedClassifier {
        script: Mutex<Vec<ClassifyOutcome>>,
        seen: Mutex<Vec<i64>>,
    }

    impl ScriptedClassifier {
        fn new(script: Vec<ClassifyOutcome>) -> Arc<Self> {
            Arc::new(Self {
                script: Mutex::new(script),
                seen: Mutex::new(Vec::new()),
            })
        }
    }

    #[async_trait]
    impl Classifier for ScriptedClassifier {
        fn supported_event_types(&self) -> &'static [&'static str] {
            ALL_EVENT_TYPES
        }

        async fn classify(&self, webhook: &ClaimedWebhook) -> ClassifyOutcome {
            self.seen
                .lock()
                .unwrap()
                .push(webhook.id);
            self.script
                .lock()
                .unwrap()
                .remove(0)
        }
    }

    #[tokio::test]
    async fn process_one_terminates_with_outcome() {
        let (_db, pool, inbox) = test_inbox().await;
        let id = seed(&pool, "d-1", "push").await;
        let classifier =
            ScriptedClassifier::new(vec![ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction)]);
        let proc = WebhookProcessor::new(inbox.clone(), classifier, fast_config());

        assert!(
            proc.process_one()
                .await
                .unwrap()
        );
        let row = row(&pool, id).await;
        assert_eq!(row.status, WebhookStatus::Ignored);
        assert_eq!(row.outcome, Some(WebhookOutcome::IgnoredAction));
        assert!(row.processed_at.is_some());
        assert!(row.claim_token.is_none(), "claim cleared on terminal");
    }

    #[tokio::test]
    async fn process_one_returns_false_when_empty() {
        let (_db, _pool, inbox) = test_inbox().await;
        let proc = WebhookProcessor::new(inbox, Arc::new(NoopClassifier::new()), fast_config());
        assert!(
            !proc
                .process_one()
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn retryable_increments_attempts_and_sets_backoff() {
        let (_db, pool, inbox) = test_inbox().await;
        let id = seed(&pool, "d-2", "push").await;
        let classifier =
            ScriptedClassifier::new(vec![ClassifyOutcome::Retryable("transient".into())]);
        let proc = WebhookProcessor::new(inbox.clone(), classifier, fast_config());

        let before = Utc::now();
        assert!(
            proc.process_one()
                .await
                .unwrap()
        );
        let row = row(&pool, id).await;
        assert_eq!(row.status, WebhookStatus::RetryableError);
        assert_eq!(row.attempts, 1);
        assert_eq!(row.last_error.as_deref(), Some("transient"));
        // first retry waits ~1s with fast_config's base.
        assert!(
            row.next_attempt_at >= before + chrono::Duration::milliseconds(900),
            "backoff respected: next_attempt_at={}, before={}",
            row.next_attempt_at,
            before
        );
        assert!(row.claim_token.is_none());
        assert!(row.processed_at.is_none(), "not terminal");
    }

    #[tokio::test]
    async fn attempts_exhausted_promotes_to_permanent_failure() {
        let (_db, pool, inbox) = test_inbox().await;
        let id = seed(&pool, "d-3", "push").await;
        let mut config = fast_config();
        config.max_attempts = 2;
        // Three retryable classifications scheduled, but only the
        // first two should run; the second hits max_attempts and
        // becomes permanent.
        let classifier = ScriptedClassifier::new(vec![
            ClassifyOutcome::Retryable("first".into()),
            ClassifyOutcome::Retryable("second".into()),
        ]);
        let proc = WebhookProcessor::new(inbox.clone(), classifier, config);

        // First retry → retryable_error, next_attempt_at = future.
        proc.process_one()
            .await
            .unwrap();
        // Make the row immediately claimable again.
        // Keep the fixture unambiguously due across the application/DB clock
        // boundary; equality to two separately sampled "now" values is racy.
        set_next_attempt_at(&pool, id, Utc::now() - chrono::Duration::seconds(1)).await;
        // Second attempt → next_attempts (2) >= max_attempts (2) →
        // permanent failure.
        proc.process_one()
            .await
            .unwrap();

        let row = row(&pool, id).await;
        assert_eq!(row.status, WebhookStatus::Failed);
        assert_eq!(row.outcome, Some(WebhookOutcome::Error));
        assert_eq!(row.last_error.as_deref(), Some("second"));
        assert!(row.processed_at.is_some());
        // Both transient + permanent failure paths increment attempts,
        // so after max_attempts=2 worth of failures the row reflects 2,
        // not 1.
        assert_eq!(
            row.attempts, 2,
            "permanent failure must also increment attempts so the count is accurate"
        );
    }

    #[tokio::test]
    async fn sweep_resets_stuck_processing_rows() {
        let (_db, pool, inbox) = test_inbox().await;
        let id = seed(&pool, "d-4", "push").await;
        // Simulate a crashed processor: claim normally, then backdate
        // claimed_at past the lease window.
        let _ = inbox
            .claim_next(ALL_EVENT_TYPES)
            .await
            .unwrap()
            .expect("seeded row must be claimable");
        set_claimed_at(&pool, id, Utc::now() - chrono::Duration::seconds(60)).await;

        let recovered = inbox
            .sweep_stuck_claims(chrono::Duration::seconds(10))
            .await
            .unwrap();
        assert_eq!(recovered, 1);

        let row = row(&pool, id).await;
        assert_eq!(row.status, WebhookStatus::RetryableError);
        assert!(row.claim_token.is_none());
        assert!(row.claimed_at.is_none());
        assert!(
            row.last_error
                .as_deref()
                .unwrap_or("")
                .contains("stuck-claim sweep")
        );
    }

    #[tokio::test]
    async fn concurrent_claims_pick_disjoint_rows() {
        // The semantic we verify is that each claim returns a different row,
        // backed here by the production `FOR UPDATE SKIP LOCKED` query.
        let (_db, pool, inbox) = test_inbox().await;
        let id_a = seed(&pool, "d-a", "push").await;
        let id_b = seed(&pool, "d-b", "push").await;

        let claim1 = inbox
            .claim_next(ALL_EVENT_TYPES)
            .await
            .unwrap()
            .unwrap();
        let claim2 = inbox
            .claim_next(ALL_EVENT_TYPES)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(claim1.id, claim2.id);
        // Both should be in our seeded set.
        assert!([id_a, id_b].contains(&claim1.id));
        assert!([id_a, id_b].contains(&claim2.id));

        // A third claim returns nothing — both are now `processing`.
        assert!(
            inbox
                .claim_next(ALL_EVENT_TYPES)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn stale_claim_writes_are_no_ops() {
        // Processor A claims; sweeper resets the row; processor A
        // tries to complete with its stale token. Must be a no-op:
        // the row stays in retryable_error.
        let (_db, pool, inbox) = test_inbox().await;
        let id = seed(&pool, "d-5", "push").await;

        let claimed = inbox
            .claim_next(ALL_EVENT_TYPES)
            .await
            .unwrap()
            .unwrap();
        // Force the claim to look ancient and sweep it.
        set_claimed_at(&pool, id, Utc::now() - chrono::Duration::seconds(60)).await;
        let recovered = inbox
            .sweep_stuck_claims(chrono::Duration::seconds(10))
            .await
            .unwrap();
        assert_eq!(recovered, 1);

        // Stale processor's late complete: must be a no-op.
        inbox
            .complete(id, claimed.claim_token, WebhookOutcome::IgnoredAction)
            .await
            .unwrap();
        let row = row(&pool, id).await;
        assert_eq!(row.status, WebhookStatus::RetryableError);
        assert!(row.outcome.is_none(), "stale write must not set outcome");
    }

    #[tokio::test]
    async fn complete_clears_last_error_from_prior_retries() {
        // A row that transient-failed once and then succeeded must
        // not leave a stale last_error string visible to ops queries.
        let (_db, pool, inbox) = test_inbox().await;
        let id = seed(&pool, "d-6", "push").await;
        // Sequence: retryable → reset for re-claim → terminal success.
        let classifier = ScriptedClassifier::new(vec![
            ClassifyOutcome::Retryable("transient blip".into()),
            ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction),
        ]);
        let proc = WebhookProcessor::new(inbox.clone(), classifier, fast_config());

        proc.process_one()
            .await
            .unwrap();
        assert_eq!(
            row(&pool, id)
                .await
                .last_error
                .as_deref(),
            Some("transient blip")
        );

        set_next_attempt_at(&pool, id, Utc::now() - chrono::Duration::seconds(1)).await;
        proc.process_one()
            .await
            .unwrap();

        let row = row(&pool, id).await;
        assert_eq!(row.status, WebhookStatus::Ignored);
        assert!(
            row.last_error.is_none(),
            "complete() must clear last_error from prior retry attempts; got {:?}",
            row.last_error
        );
    }

    // ─── BasicClassifier ────────────────────────────────────────────────

    /// Test fixture: a typical `issue_comment` payload. Slice 6 grew
    /// the User struct to require `id` + `type`; sender is `alice`
    /// (id=42), the conventional authorized test user in the
    /// IssueCommentHandler tests.
    fn issue_comment_payload(action: &str, body: &str, is_pr: bool) -> serde_json::Value {
        let pull_request = if is_pr {
            serde_json::json!({ "url": "https://api.github.test/repos/o/r/pulls/1" })
        } else {
            serde_json::Value::Null
        };
        serde_json::json!({
            "action": action,
            "comment": {
                "id": 1,
                "body": body,
                "user": { "id": 42, "login": "alice", "type": "User" },
                "author_association": "MEMBER",
            },
            "issue": {
                "number": 1,
                "pull_request": pull_request,
            },
            "repository": { "full_name": "o/r" },
            "sender": { "id": 42, "login": "alice", "type": "User" },
            "installation": { "id": 1 },
        })
    }

    fn make_claimed(
        event_type: &str,
        action: Option<&str>,
        payload: Option<serde_json::Value>,
    ) -> ClaimedWebhook {
        ClaimedWebhook {
            id: 1,
            claim_token: uuid::Uuid::new_v4(),
            delivery_id: "d".into(),
            event_type: event_type.into(),
            action: action.map(str::to_string),
            payload_installation_id: None,
            payload,
            payload_size_bytes: 0,
            attempts: 0,
            received_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn basic_issue_comment_non_created_is_ignored_action() {
        let webhook = make_claimed(
            "issue_comment",
            Some("deleted"),
            Some(issue_comment_payload("deleted", "anything", true)),
        );
        let outcome = make_issue_comment_handler()
            .handle(&webhook)
            .await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction)));
    }

    #[tokio::test]
    async fn basic_issue_comment_on_non_pr_is_ignored_action() {
        let webhook = make_claimed(
            "issue_comment",
            Some("created"),
            Some(issue_comment_payload("created", "/benchmark", false)),
        );
        let outcome = make_issue_comment_handler()
            .handle(&webhook)
            .await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction)));
    }

    #[tokio::test]
    async fn basic_issue_comment_pr_no_command_is_ignored_no_command() {
        let webhook = make_claimed(
            "issue_comment",
            Some("created"),
            Some(issue_comment_payload("created", "looks great", true)),
        );
        let outcome = make_issue_comment_handler()
            .handle(&webhook)
            .await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::IgnoredNoCommand)));
    }

    /// Build an `IssueCommentHandler` with policy/repo state already
    /// seeded for a /benchmark eval. Returns the handler, the
    /// `FakeGitHub`, the policy/install/user stores (so each test can
    /// stage exactly which policies are enabled), and the slice-9
    /// Postgres stores (so handler tests exercise the production persistence
    /// implementation). The FakeGitHub is pre-seeded with the standard PR
    /// response (base id=10, head id=20).
    async fn make_benchmark_handler() -> (
        TestDb,
        IssueCommentHandler,
        sbgh_github::test_support::FakeGitHub,
        Arc<PostgresPolicyStore>,
        Arc<PostgresInstallationStore>,
        Arc<PostgresUserStore>,
        Arc<PostgresJobStore>,
    ) {
        let (db, pool) = setup_pg_db().await;
        let repo_store = Arc::new(PostgresRepoStore::new(pool.clone()));
        for identity in [
            NewRepoIdentity {
                id: 10,
                owner: "o".into(),
                name: "r".into(),
                default_branch: None,
            },
            NewRepoIdentity {
                id: 20,
                owner: "alice".into(),
                name: "r".into(),
                default_branch: None,
            },
            NewRepoIdentity {
                id: 999,
                owner: "other".into(),
                name: "repo".into(),
                default_branch: None,
            },
        ] {
            repo_store
                .upsert_repo_identity(&identity)
                .await
                .unwrap();
        }
        let policy_store = Arc::new(PostgresPolicyStore::new(pool.clone()));
        // Slice 5 review fix: handler now gates policy eval on
        // is_membership_active. Seed install 1 + membership for the
        // base repo so the happy-path tests pass through the gate.
        // Tests that need to exercise the gate failure paths construct
        // their own install_store state.
        let install_store = Arc::new(PostgresInstallationStore::new(pool.clone()));
        install_store
            .seed_allowed(1, "octo-org", GithubAccountType::Organization, true)
            .await;
        install_store
            .upsert_installation(&NewInstallation {
                id: 1,
                github_account_id: 1,
                account_login: "octo-org".into(),
                account_type: GithubAccountType::Organization,
            })
            .await
            .unwrap();
        let _ = install_store
            .add_or_restore_membership(1, 10)
            .await
            .unwrap();

        let gh = sbgh_github::test_support::FakeGitHub::new();
        // Standard PR: base repo id=10, head repo id=20 (a fork).
        gh.set_pull_request(
            "o/r",
            1,
            sbgh_github::PullRequestSide {
                repo: sbgh_github::RepoRef {
                    id: 10,
                    owner: "o".into(),
                    name: "r".into(),
                },
                sha: "basesha".into(),
                branch: "main".into(),
            },
            sbgh_github::PullRequestSide {
                repo: sbgh_github::RepoRef {
                    id: 20,
                    owner: "alice".into(),
                    name: "r".into(),
                },
                sha: "headsha".into(),
                branch: "feat".into(),
            },
        );
        // Slice 6: pre-seed `alice` (id=42) with the
        // `trigger_pr_benchmark` role on (install=1, repo=10) so the
        // accept-path tests pass authz. Tests that need to exercise
        // the unauthorized branch use `seed_denied_user` below or
        // skip the grant entirely.
        let user_store = Arc::new(PostgresUserStore::new(pool.clone()));
        user_store
            .seed_user(42, "alice", GithubAccountType::User)
            .await;
        user_store
            .seed_role(42, 1, Some(10), UserRole::TriggerPrBenchmark)
            .await;

        let pull_request_store = Arc::new(PostgresPullRequestStore::new(pool.clone()));
        let job_store = Arc::new(PostgresJobStore::new(pool.clone()));
        sqlx::query(
            "INSERT INTO github_webhook \
             (id, delivery_id, event_type, action, payload_size_bytes) \
             VALUES (1, 'unit-benchmark', 'issue_comment', 'created', 0)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let handler = IssueCommentHandler::new(
            repo_store,
            policy_store.clone(),
            install_store.clone(),
            user_store.clone(),
            pull_request_store,
            Arc::new(gh.clone()),
            job_store.clone(),
        );
        (db, handler, gh, policy_store, install_store, user_store, job_store)
    }

    #[tokio::test]
    async fn benchmark_with_both_policies_enabled_enqueues_pr_comment_job() {
        // Slice 9: /benchmark on a PR with target+source policies enabled
        // + an authorized user → accepted → creates a `pr_comment`
        // ad-hoc job (+ webhook/user/PR links + queued event) and
        // terminates as `EnqueuedJob`.
        let (_db, h, _gh, policy_store, _install_store, _user_store, job_store) =
            make_benchmark_handler().await;
        policy_store
            .seed_target(1, 10, true)
            .await; // base
        policy_store
            .seed_source(1, 20, true)
            .await; // head

        let webhook = make_claimed(
            "issue_comment",
            Some("created"),
            Some(issue_comment_payload("created", "/benchmark run --iters=3", true)),
        );
        let outcome = h.handle(&webhook).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::EnqueuedJob)));

        // One job created, with the expected subject identity. The job's
        // repo is the TARGET (base, id=10); the ref is the PR HEAD.
        let jobs = job_store.all_jobs().await;
        assert_eq!(jobs.len(), 1, "exactly one job created");
        let job = &jobs[0];
        assert_eq!(job.github_repo_id, 10, "job runs against the target (base) repo");
        assert_eq!(job.intent, sbgh_core::models::JobIntent::AdhocBenchmark);
        assert_eq!(job.source, sbgh_core::models::JobSource::GithubComment);
        assert_eq!(job.git_ref_kind, GitRefKind::Branch);
        assert_eq!(job.git_ref_display, "feat", "PR head branch");
        assert_eq!(job.git_commit_hash.as_deref(), Some("headsha"));
        assert_eq!(job.status, sbgh_core::models::JobStatus::Queued);

        // Links: triggering user (the commenter) + the PR.
        assert_eq!(
            job_store
                .user_links()
                .await
                .len(),
            1
        );
        assert_eq!(job_store.user_links().await[0].github_user_id, 42);
        assert_eq!(
            job_store
                .pr_links()
                .await
                .len(),
            1
        );
        assert_eq!(
            job_store.pr_links().await[0].triggering_comment_id,
            Some(1),
            "links the triggering comment id"
        );

        // Queued event carries the pr_comment provenance with bench args.
        let events = job_store.all_events().await;
        assert_eq!(events.len(), 1);
        let detail: sbgh_core::models::QueuedEventDetail = serde_json::from_value(
            events[0]
                .detail
                .clone()
                .unwrap(),
        )
        .unwrap();
        match detail {
            sbgh_core::models::QueuedEventDetail::PrComment {
                sender_login,
                subcommand,
                bench_args,
                ..
            } => {
                assert_eq!(sender_login, "alice");
                assert_eq!(subcommand.as_deref(), Some("run"));
                assert_eq!(bench_args, vec!["--iters=3"]);
            }
            other => panic!("expected PrComment provenance, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn benchmark_reprocessed_webhook_does_not_duplicate_job() {
        // Slice 9 (review fix): job creation is the only non-idempotent
        // classify side effect, and the inbox is at-least-once — a
        // webhook can be reprocessed after a failed complete() / swept
        // claim lease. Handling the SAME webhook twice must yield
        // EnqueuedJob both times but only ONE job (idempotent on
        // github_webhook_id).
        let (_db, h, _gh, policy_store, _install_store, _user_store, job_store) =
            make_benchmark_handler().await;
        policy_store
            .seed_target(1, 10, true)
            .await;
        policy_store
            .seed_source(1, 20, true)
            .await;
        let webhook = make_claimed(
            "issue_comment",
            Some("created"),
            Some(issue_comment_payload("created", "/benchmark run", true)),
        );

        let first = h.handle(&webhook).await;
        assert!(matches!(first, ClassifyOutcome::Terminal(WebhookOutcome::EnqueuedJob)));
        // Same webhook re-delivered to the handler (same webhook.id).
        let second = h.handle(&webhook).await;
        assert!(
            matches!(second, ClassifyOutcome::Terminal(WebhookOutcome::EnqueuedJob)),
            "reprocess must still terminalize as EnqueuedJob (idempotent)"
        );
        assert_eq!(
            job_store
                .all_jobs()
                .await
                .len(),
            1,
            "reprocess must NOT create a duplicate job"
        );
    }

    #[tokio::test]
    async fn benchmark_for_a_commit_already_being_benchmarked_is_deduped() {
        // Phase 5 dedup: a SECOND, DISTINCT `/benchmark` (different webhook id,
        // so it clears the per-webhook idempotency) for a commit that already
        // has an active job must NOT enqueue a duplicate — two jobs on one head
        // SHA would fight over GitHub's single check per `(name, head_sha)`.
        let (_db, h, _gh, policy_store, _install_store, _user_store, job_store) =
            make_benchmark_handler().await;
        policy_store
            .seed_target(1, 10, true)
            .await;
        policy_store
            .seed_source(1, 20, true)
            .await;

        // First /benchmark → one job for (repo 10, head "headsha").
        let first = make_claimed(
            "issue_comment",
            Some("created"),
            Some(issue_comment_payload("created", "/benchmark run", true)),
        );
        assert!(matches!(
            h.handle(&first).await,
            ClassifyOutcome::Terminal(WebhookOutcome::EnqueuedJob)
        ));
        assert_eq!(
            job_store
                .all_jobs()
                .await
                .len(),
            1
        );

        // A distinct second webhook for the SAME PR/commit while the first is
        // still active (queued) → deduped, no second job.
        let mut second = make_claimed(
            "issue_comment",
            Some("created"),
            Some(issue_comment_payload("created", "/benchmark run", true)),
        );
        second.id = 2;
        second.delivery_id = "d2".into();

        let outcome = h.handle(&second).await;
        assert!(
            matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::EnqueuedJob)),
            "a deduped request still terminalizes as EnqueuedJob (the benchmark is covered)"
        );
        assert_eq!(
            job_store
                .all_jobs()
                .await
                .len(),
            1,
            "no duplicate job for a commit already being benchmarked",
        );
    }

    #[tokio::test]
    async fn benchmark_with_no_target_policy_is_denied_target_policy() {
        // Slice 5 new behavior: previously (slice 2b) /benchmark with
        // no policies → IgnoredAction. Slice 5 reads the policy and
        // surfaces the denial — even though the legacy handler may
        // still run the bench in Phase 1, the inbox row signals "the
        // new pipeline would have denied this."
        let (_db, h, _gh, _policy_store, _install_store, _user_store, _job_store) =
            make_benchmark_handler().await;
        // No target policy seeded.
        let webhook = make_claimed(
            "issue_comment",
            Some("created"),
            Some(issue_comment_payload("created", "/benchmark run", true)),
        );
        let outcome = h.handle(&webhook).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::DeniedTargetPolicy)));
    }

    #[tokio::test]
    async fn benchmark_with_disabled_target_policy_is_denied_target_policy() {
        // Soft-disabled target (operator paused) takes the same deny
        // path as a missing target row.
        let (_db, h, _gh, policy_store, _install_store, _user_store, _job_store) =
            make_benchmark_handler().await;
        policy_store
            .seed_target(1, 10, false)
            .await;
        policy_store
            .seed_source(1, 20, true)
            .await;
        let webhook = make_claimed(
            "issue_comment",
            Some("created"),
            Some(issue_comment_payload("created", "/benchmark run", true)),
        );
        let outcome = h.handle(&webhook).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::DeniedTargetPolicy)));
    }

    #[tokio::test]
    async fn benchmark_with_no_source_policy_is_denied_source_policy() {
        let (_db, h, _gh, policy_store, _install_store, _user_store, _job_store) =
            make_benchmark_handler().await;
        policy_store
            .seed_target(1, 10, true)
            .await;
        // No source policy.
        let webhook = make_claimed(
            "issue_comment",
            Some("created"),
            Some(issue_comment_payload("created", "/benchmark run", true)),
        );
        let outcome = h.handle(&webhook).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::DeniedSourcePolicy)));
    }

    // ─── Slice 6 role-gate tests ────────────────────────────────────────

    /// Build a `/benchmark` handler with policies enabled but NO role
    /// grant seeded. Tests can then either grant the user explicitly
    /// or assert that the unauthorized path fires.
    async fn make_benchmark_handler_without_role_grant()
    -> (TestDb, IssueCommentHandler, Arc<PostgresUserStore>) {
        let (db, h, _gh, policy_store, _install_store, user_store, _job_store) =
            make_benchmark_handler().await;
        policy_store
            .seed_target(1, 10, true)
            .await;
        policy_store
            .seed_source(1, 20, true)
            .await;
        // Wipe the role seeded by `make_benchmark_handler` so the
        // user starts with no grants. (Building the store from
        // scratch would require duplicating all the install / repo
        // setup; this is simpler.)
        let _ = user_store
            .revoke_role(42, 1, Some(10), UserRole::TriggerPrBenchmark)
            .await;
        (db, h, user_store)
    }

    #[tokio::test]
    async fn benchmark_without_role_grant_is_denied_unauthorized() {
        // Slice 6: a /benchmark from a user with no
        // `trigger_pr_benchmark` grant on the target repo terminates
        // as `DeniedUnauthorized` (NOT `WouldEnqueueJob`), even if
        // both target+source policies are enabled.
        let (_db, h, _user_store) = make_benchmark_handler_without_role_grant().await;
        let webhook = make_claimed(
            "issue_comment",
            Some("created"),
            Some(issue_comment_payload("created", "/benchmark run", true)),
        );
        let outcome = h.handle(&webhook).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::DeniedUnauthorized)));
    }

    #[tokio::test]
    async fn benchmark_with_admin_grant_is_authorized_via_admin_implies() {
        // Post-slice-6 review M1 fix: an `admin` grant must imply
        // `trigger_pr_benchmark` (and every other role) within its
        // scope. Without admin-implies, `admin` would be a lie — the
        // schema documents it as "full control" and the CLI exposes
        // it as grantable.
        let (_db, h, user_store) = make_benchmark_handler_without_role_grant().await;
        // Install-wide admin (NOT trigger_pr_benchmark explicit).
        user_store
            .seed_role(42, 1, None, UserRole::Admin)
            .await;
        let webhook = make_claimed(
            "issue_comment",
            Some("created"),
            Some(issue_comment_payload("created", "/benchmark run", true)),
        );
        let outcome = h.handle(&webhook).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::EnqueuedJob)));
    }

    #[tokio::test]
    async fn benchmark_with_revoked_grant_is_denied_unauthorized() {
        // Post-slice-6 review M2 fix: revoked grants must not
        // authorize even though the row is still in the table.
        let (_db, h, user_store) = make_benchmark_handler_without_role_grant().await;
        user_store
            .seed_role(42, 1, Some(10), UserRole::TriggerPrBenchmark)
            .await;
        // Soft-revoke via the trait.
        user_store
            .revoke_role(42, 1, Some(10), UserRole::TriggerPrBenchmark)
            .await
            .unwrap();
        let webhook = make_claimed(
            "issue_comment",
            Some("created"),
            Some(issue_comment_payload("created", "/benchmark run", true)),
        );
        let outcome = h.handle(&webhook).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::DeniedUnauthorized)));
    }

    #[tokio::test]
    async fn benchmark_with_install_wide_grant_is_authorized() {
        // Install-wide grant (`github_repo_id IS NULL`) authorizes
        // /benchmark on any repo within the install — matching the
        // has_role wildcard semantics.
        let (_db, h, user_store) = make_benchmark_handler_without_role_grant().await;
        user_store
            .seed_role(42, 1, None, UserRole::TriggerPrBenchmark)
            .await;
        let webhook = make_claimed(
            "issue_comment",
            Some("created"),
            Some(issue_comment_payload("created", "/benchmark run", true)),
        );
        let outcome = h.handle(&webhook).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::EnqueuedJob)));
    }

    #[tokio::test]
    async fn benchmark_with_grant_on_different_repo_is_denied_unauthorized() {
        // Repo-scoped grant for a DIFFERENT repo in the same install
        // does NOT authorize. The slice 6 design point: --repo
        // narrows the grant; it doesn't broaden it.
        let (_db, h, user_store) = make_benchmark_handler_without_role_grant().await;
        // Grant on repo=999 — but the canned PR's target is repo=10.
        user_store
            .seed_role(42, 1, Some(999), UserRole::TriggerPrBenchmark)
            .await;
        let webhook = make_claimed(
            "issue_comment",
            Some("created"),
            Some(issue_comment_payload("created", "/benchmark run", true)),
        );
        let outcome = h.handle(&webhook).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::DeniedUnauthorized)));
    }

    #[tokio::test]
    async fn benchmark_unauthorized_still_upserts_user_for_audit_trail() {
        // Upsert-before-authz pattern: even denied attempts leave a
        // github_user row so the audit trail of /benchmark attempts is
        // queryable in DB. If we ever add a "list users who attempted
        // /benchmark" view, it wants to see denied users too.
        let (_db, h, user_store) = make_benchmark_handler_without_role_grant().await;
        let webhook = make_claimed(
            "issue_comment",
            Some("created"),
            Some(issue_comment_payload("created", "/benchmark run", true)),
        );
        let _outcome = h.handle(&webhook).await;
        let user = user_store
            .lookup_user(42)
            .await
            .unwrap();
        assert!(user.is_some(), "denied user must still be upserted");
        assert_eq!(user.unwrap().login, "alice");
    }

    #[tokio::test]
    async fn benchmark_with_unknown_sender_account_type_is_error() {
        // Forward-compat: an unrecognised GH account_type string on
        // the sender object → `Error` (the handler can't safely upsert
        // the user with a bogus type). Same defensive pattern as
        // slice 3's InstallationHandler.
        let (_db, h, _user_store) = make_benchmark_handler_without_role_grant().await;
        let payload = serde_json::json!({
            "action": "created",
            "comment": {
                "id": 1,
                "body": "/benchmark run",
                "user": { "id": 42, "login": "alice", "type": "User" },
                "author_association": "MEMBER",
            },
            "issue": {
                "number": 1,
                "pull_request": { "url": "https://api.github.test/repos/o/r/pulls/1" },
            },
            "repository": { "full_name": "o/r" },
            "sender": { "id": 42, "login": "alice", "type": "MysteryShopper" },
            "installation": { "id": 1 },
        });
        let webhook = make_claimed("issue_comment", Some("created"), Some(payload));
        let outcome = h.handle(&webhook).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::Error)));
    }

    #[tokio::test]
    async fn benchmark_gh_api_failure_is_retryable() {
        // If we can't fetch the PR (GH API down, install token expired,
        // etc.), the policy eval can't complete → Retryable.
        let (_db, h, _gh, _policy_store, _install_store, _user_store, _job_store) =
            make_benchmark_handler().await;
        // Build a webhook for a DIFFERENT PR than the canned one, so
        // FakeGitHub returns an error.
        let payload = serde_json::json!({
            "action": "created",
            "comment": {
                "id": 1,
                "body": "/benchmark run",
                "user": { "id": 42, "login": "alice", "type": "User" },
                "author_association": "MEMBER",
            },
            "issue": {
                "number": 999, // not the seeded PR number
                "pull_request": { "url": "https://api.github.test/repos/o/r/pulls/999" },
            },
            "repository": { "full_name": "o/r" },
            "sender": { "id": 42, "login": "alice", "type": "User" },
            "installation": { "id": 1 },
        });
        let webhook = make_claimed("issue_comment", Some("created"), Some(payload));
        let outcome = h.handle(&webhook).await;
        assert!(matches!(outcome, ClassifyOutcome::Retryable(_)));
    }

    #[tokio::test]
    async fn basic_issue_comment_null_payload_is_error() {
        let webhook = make_claimed("issue_comment", Some("created"), None);
        let outcome = make_issue_comment_handler()
            .handle(&webhook)
            .await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::Error)));
    }

    #[tokio::test]
    async fn basic_issue_comment_bad_typed_shape_is_error() {
        let webhook = make_claimed(
            "issue_comment",
            Some("created"),
            Some(serde_json::json!({ "action": "created", "not": "an issue_comment" })),
        );
        let outcome = make_issue_comment_handler()
            .handle(&webhook)
            .await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::Error)));
    }

    #[tokio::test]
    async fn basic_classifier_only_lists_registered_handlers() {
        // The router only advertises event types it has a registered
        // handler for. A slice-2b-only build (issue_comment handler only)
        // must NOT claim `installation` rows even though the schema has
        // outcomes for them; that's what slice 3's handler registration
        // unlocks. Pinning this contract here catches "added an
        // EventHandler trait impl but forgot to register it in the
        // builder" regressions.
        let only_ic = BasicClassifier::builder()
            .with_handler(Arc::new(make_issue_comment_handler()))
            .build();
        assert_eq!(only_ic.supported_event_types(), &["issue_comment"]);

        let pool = lazy_pool();
        let store = Arc::new(PostgresInstallationStore::new(pool));
        let with_install = BasicClassifier::builder()
            .with_handler(Arc::new(make_issue_comment_handler()))
            .with_handler(Arc::new(make_install_handler(store)))
            .build();
        assert_eq!(with_install.supported_event_types(), &["installation", "issue_comment"]);
    }

    #[tokio::test]
    async fn router_leaves_unregistered_event_types_in_received() {
        // Stronger version of the old "future-slice events stay in
        // received" test: builds the router with ONLY IssueCommentHandler
        // and asserts that an `installation` row is never claimed. The
        // pre-router classifier had to special-case this; the router
        // gets it for free from claim filter composition, but pinning
        // it here guards against someone widening the filter without
        // adding a handler.
        let (_db, pool, inbox) = test_inbox().await;
        let issue_comment_id: i64 = sqlx::query_scalar(
            "INSERT INTO github_webhook (delivery_id, event_type, action, payload, \
             payload_size_bytes) VALUES ('d-ic', 'issue_comment', 'created', $1, 0) RETURNING id",
        )
        .bind(issue_comment_payload("created", "looks good", true))
        .fetch_one(&pool)
        .await
        .unwrap();
        let installation_id = seed(&pool, "d-inst", "installation").await;
        let classifier = BasicClassifier::builder()
            .with_handler(Arc::new(make_issue_comment_handler()))
            .build();
        let proc = WebhookProcessor::new(inbox.clone(), Arc::new(classifier), fast_config());

        assert!(
            proc.process_one()
                .await
                .unwrap()
        );
        assert!(
            !proc
                .process_one()
                .await
                .unwrap()
        );

        let ic = row(&pool, issue_comment_id).await;
        assert!(matches!(ic.status, WebhookStatus::Ignored));

        let inst = row(&pool, installation_id).await;
        assert_eq!(inst.status, WebhookStatus::Received);
        assert!(inst.claim_token.is_none());
        assert!(inst.outcome.is_none());
    }

    #[tokio::test]
    #[should_panic(expected = "duplicate EventHandler")]
    async fn builder_panics_on_duplicate_handler_for_same_event_type() {
        // Programmer error we want surfaced at startup, not silently
        // shadowed at runtime.
        let _ = BasicClassifier::builder()
            .with_handler(Arc::new(make_issue_comment_handler()))
            .with_handler(Arc::new(make_issue_comment_handler()))
            .build();
    }

    #[tokio::test]
    async fn router_with_no_matching_handler_terminates_as_error() {
        // Handler's allowlist + the inbox claim filter should prevent
        // an unregistered event type from ever reaching classify(),
        // but if a misconfiguration lets one slip through, the router
        // records it terminally instead of looping.
        let router = BasicClassifier::builder()
            .with_handler(Arc::new(make_issue_comment_handler()))
            .build();
        let webhook = make_claimed("star", None, None);
        let outcome = router
            .classify(&webhook)
            .await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::Error)));
    }

    // ─── InstallationHandler (slice 3) ──────────────────────────────────

    fn installation_payload(
        action: &str,
        install_id: i64,
        account_id: i64,
        account_type: &str,
    ) -> serde_json::Value {
        installation_payload_with_repos(action, install_id, account_id, account_type, &[])
    }

    /// Same as `installation_payload`, but with a `repositories` array
    /// for the slice 4 high-finding-fix path (`installation.created`
    /// materialises initial memberships from this list).
    fn installation_payload_with_repos(
        action: &str,
        install_id: i64,
        account_id: i64,
        account_type: &str,
        repos: &[(i64, &str)],
    ) -> serde_json::Value {
        let repos_json: Vec<serde_json::Value> = repos
            .iter()
            .map(|(id, fname)| serde_json::json!({ "id": id, "full_name": fname }))
            .collect();
        serde_json::json!({
            "action": action,
            "installation": {
                "id": install_id,
                "account": {
                    "id": account_id,
                    "login": "octo-org",
                    "type": account_type,
                }
            },
            "repositories": repos_json,
        })
    }

    fn installation_webhook(action: &str, payload: serde_json::Value) -> ClaimedWebhook {
        ClaimedWebhook {
            id: 1,
            claim_token: uuid::Uuid::new_v4(),
            delivery_id: "d-inst".into(),
            event_type: "installation".into(),
            action: Some(action.into()),
            payload_installation_id: None,
            payload: Some(payload),
            payload_size_bytes: 0,
            attempts: 0,
            received_at: Utc::now(),
        }
    }

    /// Build an `InstallationHandler` with stub repo store + FakeGitHub
    /// for tests that don't exercise the slice 4 high-finding fix
    /// (initial-repos materialisation from
    /// `installation.created.repositories`). The new() signature widened to
    /// take three args; this keeps the existing single-store tests
    /// readable.
    fn make_install_handler(store: Arc<PostgresInstallationStore>) -> InstallationHandler {
        let pool = lazy_pool();
        InstallationHandler::new(
            store,
            Arc::new(PostgresRepoStore::new(pool)),
            Arc::new(sbgh_github::test_support::FakeGitHub::new()),
        )
    }

    /// Build an `IssueCommentHandler` with stub deps for tests that
    /// don't exercise the slice 5 /benchmark policy path (action !=
    /// 'created', no PR, no /benchmark, etc.). Slice 5 widened the
    /// constructor to take repo + policy + GH; this keeps existing
    /// tests readable.
    fn make_issue_comment_handler() -> IssueCommentHandler {
        let pool = lazy_pool();
        IssueCommentHandler::new(
            Arc::new(PostgresRepoStore::new(pool.clone())),
            Arc::new(PostgresPolicyStore::new(pool.clone())),
            Arc::new(PostgresInstallationStore::new(pool.clone())),
            Arc::new(PostgresUserStore::new(pool.clone())),
            Arc::new(PostgresPullRequestStore::new(pool.clone())),
            Arc::new(sbgh_github::test_support::FakeGitHub::new()),
            Arc::new(PostgresJobStore::new(pool)),
        )
    }

    #[tokio::test]
    async fn installation_created_for_allowed_account_upserts_and_processes() {
        let (_db, store) = test_installation_store().await;
        store
            .seed_allowed(42, "octo-org", GithubAccountType::Organization, true)
            .await;
        let h = make_install_handler(store.clone());
        let w = installation_webhook(
            "created",
            installation_payload("created", 100, 42, "Organization"),
        );

        let outcome = h.handle(&w).await;
        assert!(matches!(
            outcome,
            ClassifyOutcome::Terminal(WebhookOutcome::ProcessedInstallation)
        ));
        let inst = store
            .installation(100)
            .await
            .expect("installation row must be materialised");
        assert_eq!(inst.github_account_id, 42);
        assert_eq!(inst.account_login, "octo-org");
        assert_eq!(inst.account_type, GithubAccountType::Organization);
        assert!(inst.suspended_at.is_none());
    }

    #[tokio::test]
    async fn installation_created_for_unknown_account_is_denied() {
        let (_db, store) = test_installation_store().await;
        let h = make_install_handler(store.clone());
        let w = installation_webhook("created", installation_payload("created", 100, 42, "User"));

        let outcome = h.handle(&w).await;
        assert!(matches!(
            outcome,
            ClassifyOutcome::Terminal(WebhookOutcome::DeniedInstallAllowlist)
        ));
        assert!(
            store
                .installation(100)
                .await
                .is_none(),
            "denied install MUST NOT materialise a row"
        );
    }

    #[tokio::test]
    async fn installation_created_for_disabled_account_is_denied() {
        // Disabled (soft-paused) installer must take the same deny path
        // as an unknown one. Operator pause is operationally identical
        // to "never approved" from the App's perspective.
        let (_db, store) = test_installation_store().await;
        store
            .seed_allowed(42, "octo-org", GithubAccountType::Organization, false)
            .await;
        let h = make_install_handler(store.clone());
        let w = installation_webhook(
            "created",
            installation_payload("created", 100, 42, "Organization"),
        );

        let outcome = h.handle(&w).await;
        assert!(matches!(
            outcome,
            ClassifyOutcome::Terminal(WebhookOutcome::DeniedInstallAllowlist)
        ));
        assert!(
            store
                .installation(100)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn installation_created_is_idempotent_on_redelivery() {
        // GitHub re-delivers webhooks freely. A second installation.created
        // for the same install id must be a no-op upsert, not a new row.
        let (_db, store) = test_installation_store().await;
        store
            .seed_allowed(42, "octo-org", GithubAccountType::Organization, true)
            .await;
        let h = make_install_handler(store.clone());
        let w = installation_webhook(
            "created",
            installation_payload("created", 100, 42, "Organization"),
        );

        assert!(matches!(
            h.handle(&w).await,
            ClassifyOutcome::Terminal(WebhookOutcome::ProcessedInstallation)
        ));
        assert!(matches!(
            h.handle(&w).await,
            ClassifyOutcome::Terminal(WebhookOutcome::ProcessedInstallation)
        ));
        assert_eq!(
            store
                .installations()
                .await
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn installation_suspend_sets_suspended_at() {
        let (_db, store) = test_installation_store().await;
        store
            .seed_allowed(42, "octo-org", GithubAccountType::Organization, true)
            .await;
        let h = make_install_handler(store.clone());
        // First materialise the install.
        h.handle(&installation_webhook(
            "created",
            installation_payload("created", 100, 42, "Organization"),
        ))
        .await;

        let outcome = h
            .handle(&installation_webhook(
                "suspend",
                installation_payload("suspend", 100, 42, "Organization"),
            ))
            .await;
        assert!(matches!(
            outcome,
            ClassifyOutcome::Terminal(WebhookOutcome::ProcessedInstallation)
        ));
        let inst = store
            .installation(100)
            .await
            .unwrap();
        assert!(inst.suspended_at.is_some(), "suspend MUST set suspended_at");
    }

    #[tokio::test]
    async fn installation_unsuspend_clears_suspended_at() {
        let (_db, store) = test_installation_store().await;
        store
            .seed_allowed(42, "octo-org", GithubAccountType::Organization, true)
            .await;
        let h = make_install_handler(store.clone());
        h.handle(&installation_webhook(
            "created",
            installation_payload("created", 100, 42, "Organization"),
        ))
        .await;
        h.handle(&installation_webhook(
            "suspend",
            installation_payload("suspend", 100, 42, "Organization"),
        ))
        .await;

        let outcome = h
            .handle(&installation_webhook(
                "unsuspend",
                installation_payload("unsuspend", 100, 42, "Organization"),
            ))
            .await;
        assert!(matches!(
            outcome,
            ClassifyOutcome::Terminal(WebhookOutcome::ProcessedInstallation)
        ));
        let inst = store
            .installation(100)
            .await
            .unwrap();
        assert!(inst.suspended_at.is_none(), "unsuspend MUST clear suspended_at");
    }

    #[tokio::test]
    async fn installation_suspend_for_unknown_install_is_ignored() {
        // A suspend for an install we never accepted (allowlist denied
        // at create) must not materialise a row; it's harmless to skip.
        let (_db, store) = test_installation_store().await;
        let h = make_install_handler(store.clone());

        let outcome = h
            .handle(&installation_webhook(
                "suspend",
                installation_payload("suspend", 100, 42, "Organization"),
            ))
            .await;
        assert!(matches!(
            outcome,
            ClassifyOutcome::Terminal(WebhookOutcome::IgnoredUnknownInstallation)
        ));
        assert!(
            store
                .installation(100)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn installation_deleted_soft_deletes_install_row() {
        // Slice 4: install.deleted is a soft-delete (sets deleted_at) so
        // membership FKs and future job FKs stay valid. The row is NOT
        // removed.
        let (_db, store) = test_installation_store().await;
        store
            .seed_allowed(42, "octo-org", GithubAccountType::Organization, true)
            .await;
        let h = make_install_handler(store.clone());
        h.handle(&installation_webhook(
            "created",
            installation_payload("created", 100, 42, "Organization"),
        ))
        .await;

        let outcome = h
            .handle(&installation_webhook(
                "deleted",
                installation_payload("deleted", 100, 42, "Organization"),
            ))
            .await;
        assert!(matches!(
            outcome,
            ClassifyOutcome::Terminal(WebhookOutcome::ProcessedInstallation)
        ));
        let row = store
            .installation(100)
            .await
            .expect("soft-delete must keep the row");
        assert!(row.deleted_at.is_some(), "deleted MUST set deleted_at");
    }

    #[tokio::test]
    async fn installation_deleted_for_unknown_install_is_ignored() {
        let (_db, store) = test_installation_store().await;
        let h = make_install_handler(store.clone());

        let outcome = h
            .handle(&installation_webhook(
                "deleted",
                installation_payload("deleted", 100, 42, "Organization"),
            ))
            .await;
        assert!(matches!(
            outcome,
            ClassifyOutcome::Terminal(WebhookOutcome::IgnoredUnknownInstallation)
        ));
    }

    #[tokio::test]
    async fn installation_unknown_action_is_ignored_action() {
        // Forward-compat: a future `installation.new_permissions_accepted`
        // (or whatever GH adds) records-and-skips, not retries forever.
        let (_db, store) = test_installation_store().await;
        store
            .seed_allowed(42, "octo-org", GithubAccountType::Organization, true)
            .await;
        let h = make_install_handler(store);

        let outcome = h
            .handle(&installation_webhook(
                "new_permissions_accepted",
                installation_payload("new_permissions_accepted", 100, 42, "Organization"),
            ))
            .await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction)));
    }

    #[tokio::test]
    async fn installation_null_payload_is_error() {
        let (_db, store) = test_installation_store().await;
        let h = make_install_handler(store);
        let mut w = installation_webhook("created", serde_json::Value::Null);
        w.payload = None;

        let outcome = h.handle(&w).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::Error)));
    }

    #[tokio::test]
    async fn installation_bad_typed_shape_is_error() {
        let (_db, store) = test_installation_store().await;
        let h = make_install_handler(store);
        let w = installation_webhook("created", serde_json::json!({ "action": "created" }));

        let outcome = h.handle(&w).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::Error)));
    }

    #[tokio::test]
    async fn installation_unknown_account_type_is_error() {
        // GH adding a new account type (unlikely but) would land here.
        // Better to record-and-investigate than guess.
        let (_db, store) = test_installation_store().await;
        store
            .seed_allowed(42, "octo-org", GithubAccountType::Organization, true)
            .await;
        let h = make_install_handler(store);
        let w = installation_webhook(
            "created",
            installation_payload("created", 100, 42, "GalaxyBrain"),
        );

        let outcome = h.handle(&w).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::Error)));
    }

    // ─── Slice 4 review-fix tests: install.created initial repos ────────

    /// Build a full InstallationHandler with seeded repo support, for
    /// tests that exercise the slice-4 high-finding fix
    /// (initial-repos materialisation from installation.created).
    async fn make_install_handler_with_supported(
        root_repo_id: i64,
        root_owner: &'static str,
        root_name: &'static str,
    ) -> (
        TestDb,
        InstallationHandler,
        Arc<PostgresInstallationStore>,
        Arc<PostgresRepoStore>,
        sbgh_github::test_support::FakeGitHub,
    ) {
        let (db, pool) = setup_pg_db().await;
        let install_store = Arc::new(PostgresInstallationStore::new(pool.clone()));
        install_store
            .seed_allowed(42, "octo-org", GithubAccountType::Organization, true)
            .await;
        let repo_store = Arc::new(PostgresRepoStore::new(pool));
        repo_store
            .seed_supported_root(root_repo_id, root_owner, root_name, true)
            .await;
        let gh = sbgh_github::test_support::FakeGitHub::new();
        let handler = InstallationHandler::new(
            install_store.clone(),
            repo_store.clone(),
            Arc::new(gh.clone()),
        );
        (db, handler, install_store, repo_store, gh)
    }

    #[tokio::test]
    async fn installation_created_with_supported_initial_repos_creates_memberships() {
        // Codex slice-4 high finding: a fresh install must materialise
        // memberships from the payload's `repositories` array; otherwise
        // there'd be no `github_installation_repo` rows until a later
        // `installation_repositories.added` event happened.
        let (_db, h, install_store, _repo_store, gh) =
            make_install_handler_with_supported(10, "stacks-network", "stacks-core").await;
        gh.set_repo_canonical("stacks-network", "stacks-core", 10);
        let w = installation_webhook(
            "created",
            installation_payload_with_repos(
                "created",
                100,
                42,
                "Organization",
                &[(10, "stacks-network/stacks-core")],
            ),
        );

        let outcome = h.handle(&w).await;
        assert!(matches!(
            outcome,
            ClassifyOutcome::Terminal(WebhookOutcome::ProcessedInstallation)
        ));
        let m = install_store
            .membership(100, 10)
            .await
            .expect("initial membership must be materialised by installation.created");
        assert!(m.revoked_at.is_none(), "fresh membership is active");
    }

    #[tokio::test]
    async fn installation_created_with_unsupported_initial_repos_still_processed() {
        // Install creation itself succeeded; per-repo unsupported
        // results are reflected in the membership table (none created)
        // but the webhook-level outcome stays ProcessedInstallation —
        // ops query "was this install ingested" should answer yes.
        let (_db, h, install_store, _repo_store, gh) =
            make_install_handler_with_supported(10, "stacks-network", "stacks-core").await;
        // 99 is NOT under any supported root.
        gh.set_repo_canonical("randos", "unrelated", 99);
        let w = installation_webhook(
            "created",
            installation_payload_with_repos(
                "created",
                100,
                42,
                "Organization",
                &[(99, "randos/unrelated")],
            ),
        );

        let outcome = h.handle(&w).await;
        assert!(matches!(
            outcome,
            ClassifyOutcome::Terminal(WebhookOutcome::ProcessedInstallation)
        ));
        assert!(
            install_store
                .membership(100, 99)
                .await
                .is_none(),
            "unsupported initial repo must NOT get a membership row"
        );
        // Install row itself created.
        assert!(
            install_store
                .installation(100)
                .await
                .is_some()
        );
    }

    #[tokio::test]
    async fn installation_created_with_id_mismatch_in_initial_repos_skips_membership() {
        // Codex slice-4 M2: the payload says repo.id=10, but GH's
        // /repos lookup resolves to id=99 (rename/recycling staleness).
        // Membership must NOT be granted — otherwise we'd grant on the
        // wrong repo.
        let (_db, h, install_store, _repo_store, gh) =
            make_install_handler_with_supported(10, "stacks-network", "stacks-core").await;
        // GH resolves "stacks-network/stacks-core" → id=99 (NOT 10).
        gh.set_repo_canonical("stacks-network", "stacks-core", 99);
        let w = installation_webhook(
            "created",
            installation_payload_with_repos(
                "created",
                100,
                42,
                "Organization",
                &[(10, "stacks-network/stacks-core")],
            ),
        );

        h.handle(&w).await;
        assert!(
            install_store
                .membership(100, 10)
                .await
                .is_none()
        );
        assert!(
            install_store
                .membership(100, 99)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn installation_created_with_no_repositories_still_processed_as_before() {
        // Slice 3 contract preserved: an install.created with no
        // repositories array (e.g. parsed from a payload that omits
        // the field) still upserts the install and returns
        // ProcessedInstallation without doing any membership work.
        let (_db, h, install_store, _repo_store, _gh) =
            make_install_handler_with_supported(10, "stacks-network", "stacks-core").await;
        let w = installation_webhook(
            "created",
            installation_payload("created", 100, 42, "Organization"),
        );

        let outcome = h.handle(&w).await;
        assert!(matches!(
            outcome,
            ClassifyOutcome::Terminal(WebhookOutcome::ProcessedInstallation)
        ));
        assert!(
            install_store
                .installation(100)
                .await
                .is_some()
        );
        assert!(
            install_store
                .memberships()
                .await
                .is_empty()
        );
    }

    // ─── InstallationRepositoriesHandler (slice 4) ──────────────────────

    use sbgh_github::test_support::FakeGitHub;

    fn repos_event_payload(
        action: &str,
        install_id: i64,
        added: &[(i64, &str)],
        removed: &[(i64, &str)],
    ) -> serde_json::Value {
        let mk = |arr: &[(i64, &str)]| -> Vec<serde_json::Value> {
            arr.iter()
                .map(|(id, fname)| serde_json::json!({ "id": id, "full_name": fname }))
                .collect()
        };
        serde_json::json!({
            "action": action,
            "installation": {
                "id": install_id,
                "account": { "id": 42, "login": "octo-org", "type": "Organization" }
            },
            "repositories_added": mk(added),
            "repositories_removed": mk(removed),
        })
    }

    fn repos_webhook(action: &str, payload: serde_json::Value) -> ClaimedWebhook {
        ClaimedWebhook {
            id: 1,
            claim_token: uuid::Uuid::new_v4(),
            delivery_id: "d-repos".into(),
            event_type: "installation_repositories".into(),
            action: Some(action.into()),
            payload_installation_id: Some(100),
            payload: Some(payload),
            payload_size_bytes: 0,
            attempts: 0,
            received_at: Utc::now(),
        }
    }

    /// Build a handler wired against production Postgres stores + a FakeGitHub.
    /// Seeds an install (id=100) and an allowed account so memberships
    /// can be inserted without FK errors. The caller seeds whatever
    /// supported_repo_root rows + GH-API responses the test needs.
    async fn make_repos_handler() -> (
        TestDb,
        InstallationRepositoriesHandler,
        Arc<PostgresRepoStore>,
        Arc<PostgresInstallationStore>,
        Arc<PostgresPolicyStore>,
        Arc<PostgresUserStore>,
        FakeGitHub,
    ) {
        let (db, pool) = setup_pg_db().await;
        let repo_store = Arc::new(PostgresRepoStore::new(pool.clone()));
        repo_store
            .upsert_repo_identity(&NewRepoIdentity {
                id: 10,
                owner: "stacks-network".into(),
                name: "stacks-core".into(),
                default_branch: None,
            })
            .await
            .unwrap();
        let install_store = Arc::new(PostgresInstallationStore::new(pool.clone()));
        install_store
            .seed_allowed(42, "octo-org", GithubAccountType::Organization, true)
            .await;
        install_store
            .upsert_installation(&NewInstallation {
                id: 100,
                github_account_id: 42,
                account_login: "octo-org".into(),
                account_type: GithubAccountType::Organization,
            })
            .await
            .unwrap();
        let policy_store = Arc::new(PostgresPolicyStore::new(pool.clone()));
        let user_store = Arc::new(PostgresUserStore::new(pool));
        let gh = FakeGitHub::new();
        let handler = InstallationRepositoriesHandler::new(
            repo_store.clone(),
            install_store.clone(),
            policy_store.clone(),
            user_store.clone(),
            Arc::new(gh.clone()),
        );
        (db, handler, repo_store, install_store, policy_store, user_store, gh)
    }

    #[tokio::test]
    async fn repos_added_for_canonical_supported_repo_creates_membership() {
        let (_db, handler, repo_store, install_store, _policy_store, _user_store, gh) =
            make_repos_handler().await;
        // Repo 10 is the canonical root + on the supported list.
        repo_store
            .seed_supported_root(10, "stacks-network", "stacks-core", true)
            .await;
        gh.set_repo_canonical("stacks-network", "stacks-core", 10);

        let outcome = handler
            .handle(&repos_webhook(
                "added",
                repos_event_payload("added", 100, &[(10, "stacks-network/stacks-core")], &[]),
            ))
            .await;

        assert!(matches!(
            outcome,
            ClassifyOutcome::Terminal(WebhookOutcome::ProcessedInstallation)
        ));
        let m = install_store
            .membership(100, 10)
            .await
            .expect("membership must be created");
        assert!(m.revoked_at.is_none(), "new membership is active");
    }

    #[tokio::test]
    async fn repos_added_for_fork_of_supported_root_creates_membership() {
        // Fork whose `source` is the supported canonical. Lineage walk
        // must record both the fork + the source as github_repo rows;
        // the support gate must accept via fork_root_github_repo_id.
        let (_db, handler, repo_store, install_store, _policy_store, _user_store, gh) =
            make_repos_handler().await;
        repo_store
            .seed_supported_root(10, "stacks-network", "stacks-core", true)
            .await;
        let root = RepoRef {
            id: 10,
            owner: "stacks-network".into(),
            name: "stacks-core".into(),
        };
        gh.set_repo_canonical("stacks-network", "stacks-core", 10);
        gh.set_repo_fork("alice", "stacks-core-fork", 20, root.clone(), root);

        let outcome = handler
            .handle(&repos_webhook(
                "added",
                repos_event_payload("added", 100, &[(20, "alice/stacks-core-fork")], &[]),
            ))
            .await;

        assert!(matches!(
            outcome,
            ClassifyOutcome::Terminal(WebhookOutcome::ProcessedInstallation)
        ));
        let fork = repo_store
            .repo(20)
            .await
            .expect("fork must be upserted");
        assert_eq!(fork.fork_root_github_repo_id, Some(10));
        assert!(
            install_store
                .membership(100, 20)
                .await
                .is_some()
        );
    }

    #[tokio::test]
    async fn repos_added_for_fork_of_fork_walks_to_root() {
        // Fork-of-fork: B forks A which forks canonical R. GitHub's
        // /repos response gives us source=R + parent=A in one call, so
        // we only need ONE API request to record the whole chain — the
        // lineage walk inserts R and A as identity rows, then B with
        // fork_root=R.
        let (_db, handler, repo_store, install_store, _policy_store, _user_store, gh) =
            make_repos_handler().await;
        repo_store
            .seed_supported_root(10, "stacks-network", "stacks-core", true)
            .await;
        let root_ref = RepoRef {
            id: 10,
            owner: "stacks-network".into(),
            name: "stacks-core".into(),
        };
        let mid_ref = RepoRef {
            id: 20,
            owner: "alice".into(),
            name: "stacks-core".into(),
        };
        gh.set_repo_canonical("stacks-network", "stacks-core", 10);
        gh.set_repo_fork("bob", "stacks-core", 30, mid_ref, root_ref);

        let outcome = handler
            .handle(&repos_webhook(
                "added",
                repos_event_payload("added", 100, &[(30, "bob/stacks-core")], &[]),
            ))
            .await;

        assert!(matches!(
            outcome,
            ClassifyOutcome::Terminal(WebhookOutcome::ProcessedInstallation)
        ));
        let leaf = repo_store
            .repo(30)
            .await
            .expect("leaf must be upserted");
        assert_eq!(leaf.fork_root_github_repo_id, Some(10));
        assert_eq!(leaf.parent_github_repo_id, Some(20));
        assert!(
            repo_store
                .repo(20)
                .await
                .is_some(),
            "intermediate parent must be upserted too"
        );
        assert!(
            install_store
                .membership(100, 30)
                .await
                .is_some()
        );
    }

    #[tokio::test]
    async fn repos_added_for_unsupported_lineage_skips_membership_but_caches_repo() {
        // The repo row STILL gets recorded (audit trail of "we saw this
        // repo and decided we don't support it") but no membership.
        let (_db, handler, repo_store, install_store, _policy_store, _user_store, gh) =
            make_repos_handler().await;
        // No supported_repo_root seeded.
        gh.set_repo_canonical("randos", "unrelated", 99);

        let outcome = handler
            .handle(&repos_webhook(
                "added",
                repos_event_payload("added", 100, &[(99, "randos/unrelated")], &[]),
            ))
            .await;

        assert!(matches!(
            outcome,
            ClassifyOutcome::Terminal(WebhookOutcome::IgnoredUnsupportedLineage)
        ));
        assert!(
            repo_store
                .repo(99)
                .await
                .is_some(),
            "repo identity cached even when unsupported"
        );
        assert!(
            install_store
                .membership(100, 99)
                .await
                .is_none(),
            "no membership for unsupported"
        );
    }

    #[tokio::test]
    async fn repos_added_mixed_accepted_and_rejected_aggregates_as_processed() {
        // Codex M-fix-style aggregation: any-accepted wins over
        // any-rejected for the webhook-level outcome. Per-repo
        // decisions are recorded in their respective rows.
        let (_db, handler, repo_store, install_store, _policy_store, _user_store, gh) =
            make_repos_handler().await;
        repo_store
            .seed_supported_root(10, "stacks-network", "stacks-core", true)
            .await;
        gh.set_repo_canonical("stacks-network", "stacks-core", 10);
        gh.set_repo_canonical("randos", "unrelated", 99);

        let outcome = handler
            .handle(&repos_webhook(
                "added",
                repos_event_payload(
                    "added",
                    100,
                    &[(10, "stacks-network/stacks-core"), (99, "randos/unrelated")],
                    &[],
                ),
            ))
            .await;

        assert!(matches!(
            outcome,
            ClassifyOutcome::Terminal(WebhookOutcome::ProcessedInstallation)
        ));
        assert!(
            install_store
                .membership(100, 10)
                .await
                .is_some()
        );
        assert!(
            install_store
                .membership(100, 99)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn repos_added_with_no_repos_is_ignored_action() {
        let (_db, handler, _r, _i, _p, _user_store, _gh) = make_repos_handler().await;
        let outcome = handler
            .handle(&repos_webhook("added", repos_event_payload("added", 100, &[], &[])))
            .await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction)));
    }

    #[tokio::test]
    async fn repos_added_disabled_supported_root_is_unsupported() {
        // A disabled supported_repo_root row must NOT extend support to
        // its forks. Operator soft-disabled, processor must respect.
        let (_db, handler, repo_store, install_store, _policy_store, _user_store, gh) =
            make_repos_handler().await;
        repo_store
            .seed_supported_root(10, "stacks-network", "stacks-core", false)
            .await;
        gh.set_repo_canonical("stacks-network", "stacks-core", 10);

        let outcome = handler
            .handle(&repos_webhook(
                "added",
                repos_event_payload("added", 100, &[(10, "stacks-network/stacks-core")], &[]),
            ))
            .await;
        assert!(matches!(
            outcome,
            ClassifyOutcome::Terminal(WebhookOutcome::IgnoredUnsupportedLineage)
        ));
        assert!(
            install_store
                .membership(100, 10)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn repos_added_gh_api_error_is_retryable() {
        // FakeGitHub returns Err for repos that weren't pre-programmed
        // (which lets us simulate API failure deterministically).
        let (_db, handler, _repo_store, _install_store, _policy_store, _user_store, _gh) =
            make_repos_handler().await;
        // No canned response for this repo → FakeGitHub returns Err.
        let outcome = handler
            .handle(&repos_webhook(
                "added",
                repos_event_payload("added", 100, &[(10, "no/such")], &[]),
            ))
            .await;
        assert!(matches!(outcome, ClassifyOutcome::Retryable(_)));
    }

    #[tokio::test]
    async fn repos_added_idempotent_redelivery_restores_no_change() {
        // Second `added` for the same already-active membership: the
        // upsert is a no-op, the lineage walk re-runs the API call but
        // converges to the same row, and the outcome stays
        // ProcessedInstallation (any successful membership op).
        let (_db, handler, repo_store, install_store, _policy_store, _user_store, gh) =
            make_repos_handler().await;
        repo_store
            .seed_supported_root(10, "stacks-network", "stacks-core", true)
            .await;
        gh.set_repo_canonical("stacks-network", "stacks-core", 10);
        let webhook = repos_webhook(
            "added",
            repos_event_payload("added", 100, &[(10, "stacks-network/stacks-core")], &[]),
        );

        handler.handle(&webhook).await;
        let first_granted_at = install_store
            .membership(100, 10)
            .await
            .unwrap()
            .granted_at;

        handler.handle(&webhook).await;
        let second_granted_at = install_store
            .membership(100, 10)
            .await
            .unwrap()
            .granted_at;
        assert_eq!(first_granted_at, second_granted_at, "re-delivery must NOT change granted_at");
    }

    #[tokio::test]
    async fn repos_removed_revokes_active_memberships() {
        let (_db, handler, repo_store, install_store, _policy_store, _user_store, gh) =
            make_repos_handler().await;
        repo_store
            .seed_supported_root(10, "stacks-network", "stacks-core", true)
            .await;
        gh.set_repo_canonical("stacks-network", "stacks-core", 10);
        // Seed via the add path so we exercise the realistic state.
        handler
            .handle(&repos_webhook(
                "added",
                repos_event_payload("added", 100, &[(10, "stacks-network/stacks-core")], &[]),
            ))
            .await;

        let outcome = handler
            .handle(&repos_webhook(
                "removed",
                repos_event_payload("removed", 100, &[], &[(10, "stacks-network/stacks-core")]),
            ))
            .await;
        assert!(matches!(
            outcome,
            ClassifyOutcome::Terminal(WebhookOutcome::ProcessedInstallation)
        ));
        let m = install_store
            .membership(100, 10)
            .await
            .unwrap();
        assert!(m.revoked_at.is_some(), "revoked membership must have revoked_at set");
    }

    #[tokio::test]
    async fn repos_removed_for_unknown_membership_is_ignored_action() {
        // GitHub backfill / out-of-order delivery: a `removed` for a
        // repo we never tracked is a no-op.
        let (_db, handler, _r, _i, _p, _user_store, _gh) = make_repos_handler().await;
        let outcome = handler
            .handle(&repos_webhook(
                "removed",
                repos_event_payload("removed", 100, &[], &[(99, "unknown/repo")]),
            ))
            .await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction)));
    }

    #[tokio::test]
    async fn repos_unknown_action_is_ignored_action() {
        let (_db, handler, _r, _i, _p, _user_store, _gh) = make_repos_handler().await;
        let outcome = handler
            .handle(&repos_webhook(
                "weird_action",
                repos_event_payload("weird_action", 100, &[], &[]),
            ))
            .await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction)));
    }

    #[tokio::test]
    async fn repos_null_payload_is_error() {
        let (_db, handler, _r, _i, _p, _user_store, _gh) = make_repos_handler().await;
        let mut w = repos_webhook("added", serde_json::Value::Null);
        w.payload = None;
        let outcome = handler.handle(&w).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::Error)));
    }

    #[tokio::test]
    async fn repos_added_malformed_full_name_skips_the_repo() {
        // A repo with no '/' in full_name can't be resolved (we'd need
        // owner/name split for the GH API call). Log + skip rather than
        // failing the batch.
        let (_db, handler, repo_store, install_store, _policy_store, _user_store, gh) =
            make_repos_handler().await;
        repo_store
            .seed_supported_root(10, "stacks-network", "stacks-core", true)
            .await;
        gh.set_repo_canonical("stacks-network", "stacks-core", 10);

        let outcome = handler
            .handle(&repos_webhook(
                "added",
                repos_event_payload(
                    "added",
                    100,
                    &[(10, "stacks-network/stacks-core"), (99, "no-slash")],
                    &[],
                ),
            ))
            .await;
        // The good repo still made it through.
        assert!(matches!(
            outcome,
            ClassifyOutcome::Terminal(WebhookOutcome::ProcessedInstallation)
        ));
        assert!(
            install_store
                .membership(100, 10)
                .await
                .is_some()
        );
        assert!(
            install_store
                .membership(100, 99)
                .await
                .is_none()
        );
    }

    // ─── Slice 4 review-fix tests: repos handler robustness ─────────────

    #[tokio::test]
    async fn repos_added_after_install_soft_deleted_is_ignored_unknown_installation() {
        // Codex slice-4 M1: a delayed `installation_repositories.added`
        // arriving after `installation.deleted` must NOT restore
        // membership on a retired install. The store's
        // `add_or_restore_membership` is the line of defense (returns
        // None for soft-deleted installs); the handler maps that to
        // IgnoredUnknownInstallation.
        let (_db, handler, repo_store, install_store, _policy_store, _user_store, gh) =
            make_repos_handler().await;
        repo_store
            .seed_supported_root(10, "stacks-network", "stacks-core", true)
            .await;
        gh.set_repo_canonical("stacks-network", "stacks-core", 10);
        // Soft-delete the install BEFORE the .added event arrives.
        install_store
            .delete_installation(100)
            .await
            .unwrap();

        let outcome = handler
            .handle(&repos_webhook(
                "added",
                repos_event_payload("added", 100, &[(10, "stacks-network/stacks-core")], &[]),
            ))
            .await;
        assert!(matches!(
            outcome,
            ClassifyOutcome::Terminal(WebhookOutcome::IgnoredUnknownInstallation)
        ));
        assert!(
            install_store
                .membership(100, 10)
                .await
                .is_none(),
            "membership MUST NOT be resurrected on a soft-deleted install"
        );
    }

    #[tokio::test]
    async fn repos_added_with_payload_id_mismatch_skips_that_repo() {
        // Codex slice-4 M2: payload says repo.id=10, but the GH lookup
        // for "stacks-network/stacks-core" resolves to id=99
        // (rename/recycling staleness). Membership must NOT be created
        // for either id. Other (consistent) repos in the same batch
        // still process normally.
        let (_db, handler, repo_store, install_store, _policy_store, _user_store, gh) =
            make_repos_handler().await;
        repo_store
            .seed_supported_root(10, "stacks-network", "stacks-core", true)
            .await;
        // Mismatch: payload says 10, GH says 99.
        gh.set_repo_canonical("stacks-network", "stacks-core", 99);
        // A second repo in the same batch with consistent ids.
        repo_store
            .seed_supported_root(20, "stacks-network", "other", true)
            .await;
        gh.set_repo_canonical("stacks-network", "other", 20);

        let outcome = handler
            .handle(&repos_webhook(
                "added",
                repos_event_payload(
                    "added",
                    100,
                    &[(10, "stacks-network/stacks-core"), (20, "stacks-network/other")],
                    &[],
                ),
            ))
            .await;

        // Aggregation: the second repo got membership → ProcessedInstallation.
        assert!(matches!(
            outcome,
            ClassifyOutcome::Terminal(WebhookOutcome::ProcessedInstallation)
        ));
        // The mismatched repo: NO membership for either the payload id
        // or the resolved id.
        assert!(
            install_store
                .membership(100, 10)
                .await
                .is_none()
        );
        assert!(
            install_store
                .membership(100, 99)
                .await
                .is_none()
        );
        // The consistent repo got its membership.
        assert!(
            install_store
                .membership(100, 20)
                .await
                .is_some()
        );
    }

    #[tokio::test]
    async fn repos_added_all_id_mismatches_aggregates_as_unsupported_lineage() {
        // When EVERY repo in the batch was a mismatch (no membership
        // created, no unsupported-lineage hit either), the outcome
        // should reflect "we couldn't accept anything" — bucketed
        // under IgnoredUnsupportedLineage which is the existing
        // umbrella for "no membership for various reasons".
        let (_db, handler, repo_store, _install_store, _policy_store, _user_store, gh) =
            make_repos_handler().await;
        repo_store
            .seed_supported_root(10, "stacks-network", "stacks-core", true)
            .await;
        gh.set_repo_canonical("stacks-network", "stacks-core", 99); // mismatch

        let outcome = handler
            .handle(&repos_webhook(
                "added",
                repos_event_payload("added", 100, &[(10, "stacks-network/stacks-core")], &[]),
            ))
            .await;
        assert!(matches!(
            outcome,
            ClassifyOutcome::Terminal(WebhookOutcome::IgnoredUnsupportedLineage)
        ));
    }

    // ─── Slice 5 unit tests: PullRequest / Push / Create handlers ───────

    /// Slice 6: PR payloads now must include `pull_request.user` (the
    /// author) — `PullRequestHandler` upserts it into `github_user` so
    /// slice 7's `github_pull_request.author_github_user_id` FK target
    /// exists. Standard test author is `alice` (id=42).
    fn pr_event_payload(
        action: &str,
        install_id: i64,
        base_repo_id: i64,
        head_repo_id: i64,
    ) -> serde_json::Value {
        // Slice 7: payload now includes `title` for `github_pull_request`
        // materialisation.
        serde_json::json!({
            "action": action,
            "installation": { "id": install_id },
            "repository": { "id": base_repo_id, "full_name": "o/r" },
            "pull_request": {
                "number": 1,
                "title": "test pr title",
                "user": { "id": 42, "login": "alice", "type": "User" },
                "head": { "ref": "feat", "sha": "headsha",
                          "repo": { "id": head_repo_id, "full_name": "alice/r" } },
                "base": { "ref": "main", "sha": "basesha",
                          "repo": { "id": base_repo_id, "full_name": "o/r" } }
            }
        })
    }

    fn pr_webhook(action: &str, payload: serde_json::Value) -> ClaimedWebhook {
        ClaimedWebhook {
            id: 1,
            claim_token: uuid::Uuid::new_v4(),
            delivery_id: "d-pr".into(),
            event_type: "pull_request".into(),
            action: Some(action.into()),
            payload_installation_id: Some(100),
            payload: Some(payload),
            payload_size_bytes: 0,
            attempts: 0,
            received_at: Utc::now(),
        }
    }

    /// Build a PR handler with policy_store + install_store ready to
    /// seed. The install_store is pre-loaded with an active install
    /// (id=100) + membership for the standard base repo id=10, so
    /// happy-path tests pass through the slice-5-review membership
    /// gate. Tests that exercise the gate failure paths can mutate
    /// the install_store after construction (revoke membership /
    /// soft-delete install / suspend).
    async fn make_pr_handler() -> (
        TestDb,
        PullRequestHandler,
        Arc<PostgresPolicyStore>,
        Arc<PostgresInstallationStore>,
        Arc<PostgresUserStore>,
        Arc<PostgresPullRequestStore>,
    ) {
        let (db, pool) = setup_pg_db().await;
        let repo_store = Arc::new(PostgresRepoStore::new(pool.clone()));
        for identity in [
            NewRepoIdentity {
                id: 10,
                owner: "o".into(),
                name: "r".into(),
                default_branch: None,
            },
            NewRepoIdentity {
                id: 20,
                owner: "alice".into(),
                name: "r".into(),
                default_branch: None,
            },
        ] {
            repo_store
                .upsert_repo_identity(&identity)
                .await
                .unwrap();
        }
        let policy_store = Arc::new(PostgresPolicyStore::new(pool.clone()));
        let install_store = Arc::new(PostgresInstallationStore::new(pool.clone()));
        install_store
            .seed_allowed(42, "octo-org", GithubAccountType::Organization, true)
            .await;
        install_store
            .upsert_installation(&NewInstallation {
                id: 100,
                github_account_id: 42,
                account_login: "octo-org".into(),
                account_type: GithubAccountType::Organization,
            })
            .await
            .unwrap();
        let _ = install_store
            .add_or_restore_membership(100, 10)
            .await
            .unwrap();
        let user_store = Arc::new(PostgresUserStore::new(pool.clone()));
        let pull_request_store = Arc::new(PostgresPullRequestStore::new(pool));
        (
            db,
            PullRequestHandler::new(
                repo_store,
                policy_store.clone(),
                install_store.clone(),
                user_store.clone(),
                pull_request_store.clone(),
            ),
            policy_store,
            install_store,
            user_store,
            pull_request_store,
        )
    }

    #[tokio::test]
    async fn pr_opened_with_both_policies_enabled_is_processed_pull_request() {
        // Slice 9: a pull_request event with both policies enabled
        // materialises PR state but does NOT enqueue a job — there is no
        // trigger_kind for PR-event auto-bench. It terminates as
        // `ProcessedPullRequest` (not `WouldEnqueueJob`, which would
        // imply a job is coming, and not `IgnoredAction`, which would
        // hide that PR state changed).
        let (_db, h, policy_store, _install_store, _user_store, _pr_store) =
            make_pr_handler().await;
        policy_store
            .seed_target(100, 10, true)
            .await;
        policy_store
            .seed_source(100, 20, true)
            .await;
        let w = pr_webhook("opened", pr_event_payload("opened", 100, 10, 20));
        let outcome = h.handle(&w).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::ProcessedPullRequest)));
    }

    #[tokio::test]
    async fn pr_opened_upserts_author_into_github_user() {
        // Slice 6: PullRequestHandler upserts the PR author so slice
        // 7's `github_pull_request.author_github_user_id` FK target
        // exists. Independent of policy eval — even denied PRs upsert.
        let (_db, h, _policy_store, _install_store, user_store, _pr_store) =
            make_pr_handler().await;
        // No policies seeded → DeniedTargetPolicy.
        let w = pr_webhook("opened", pr_event_payload("opened", 100, 10, 20));
        let _ = h.handle(&w).await;
        let author = user_store
            .lookup_user(42)
            .await
            .unwrap();
        assert!(author.is_some(), "PR author MUST be upserted even on denied path");
        assert_eq!(author.unwrap().login, "alice");
    }

    // ─── Slice 7 PR-row materialisation tests ───────────────────────────

    #[tokio::test]
    async fn pr_opened_materialises_pull_request_row() {
        // Slice 7: PullRequestHandler materialises the
        // github_pull_request row via the shared helper.
        let (_db, h, _policy_store, _install_store, _user_store, pr_store) =
            make_pr_handler().await;
        let w = pr_webhook("opened", pr_event_payload("opened", 100, 10, 20));
        let _ = h.handle(&w).await;
        let pr = pr_store
            .lookup_pull_request(10, 1)
            .await
            .unwrap();
        let pr = pr.expect("PR row must be materialised by opened");
        assert_eq!(pr.target_github_repo_id, 10);
        assert_eq!(pr.source_github_repo_id, 20);
        assert_eq!(pr.title, "test pr title");
        assert!(pr.closed_at.is_none(), "fresh PR is active");
    }

    #[tokio::test]
    async fn pr_edited_title_only_refreshes_title_but_terminates_ignored_action() {
        // Slice 7 review fix: an edit that only touches the title
        // MUST NOT re-run policy eval (otherwise slice 9 would turn
        // typo fixes into benchmark triggers). The PR row's title
        // still refreshes via the materialise upsert; the outcome
        // is IgnoredAction, not WouldEnqueueJob.
        let (_db, h, policy_store, _install_store, _user_store, pr_store) = make_pr_handler().await;
        policy_store
            .seed_target(100, 10, true)
            .await;
        policy_store
            .seed_source(100, 20, true)
            .await;
        // First opened to materialise.
        let w_open = pr_webhook("opened", pr_event_payload("opened", 100, 10, 20));
        let _ = h.handle(&w_open).await;

        // Edited with title-only change (no `changes.base`).
        let mut edited_payload = pr_event_payload("edited", 100, 10, 20);
        edited_payload["pull_request"]["title"] = "edited title".into();
        edited_payload["changes"] = serde_json::json!({ "title": { "from": "test pr title" } });
        let w_edit = pr_webhook("edited", edited_payload);
        let outcome = h.handle(&w_edit).await;
        // CRITICAL: title-only edits MUST NOT signal a would-enqueue.
        assert!(
            matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction)),
            "title-only edits must terminate IgnoredAction, not WouldEnqueueJob"
        );
        let pr = pr_store
            .lookup_pull_request(10, 1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pr.title, "edited title", "title must still refresh");
    }

    #[tokio::test]
    async fn pr_edited_with_base_changed_re_runs_policy_eval() {
        // Slice 7 review fix: only when `changes.base` is present
        // does an edited event re-run policy eval. This covers the
        // rare case where the operator changes the PR's base ref,
        // which can shift the target repo identity.
        let (_db, h, policy_store, _install_store, _user_store, pr_store) = make_pr_handler().await;
        policy_store
            .seed_target(100, 10, true)
            .await;
        policy_store
            .seed_source(100, 20, true)
            .await;
        let w_open = pr_webhook("opened", pr_event_payload("opened", 100, 10, 20));
        let _ = h.handle(&w_open).await;

        let mut edited_payload = pr_event_payload("edited", 100, 10, 20);
        edited_payload["pull_request"]["title"] = "edited base too".into();
        edited_payload["changes"] = serde_json::json!({
            "base": { "ref": { "from": "develop" }, "sha": { "from": "deadbeef" } }
        });
        let w_edit = pr_webhook("edited", edited_payload);
        let outcome = h.handle(&w_edit).await;
        assert!(
            matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::ProcessedPullRequest)),
            "edited + changes.base must re-run policy eval (accepted → ProcessedPullRequest, no \
             job)"
        );
        let pr = pr_store
            .lookup_pull_request(10, 1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pr.title, "edited base too");
    }

    #[tokio::test]
    async fn pr_closed_without_head_repo_still_sets_closed_at() {
        // Slice 7 review fix: GH may omit head.repo on a deleted-fork
        // PR. The original handler returned Error for closed in this
        // case, losing the close signal. The dispatch reordering means
        // closed only needs base.repo.
        let (_db, h, _policy_store, _install_store, _user_store, pr_store) =
            make_pr_handler().await;
        // Materialise first via opened (which DOES need head.repo).
        let _ = h
            .handle(&pr_webhook("opened", pr_event_payload("opened", 100, 10, 20)))
            .await;

        // Now build a closed payload with head.repo MISSING.
        let mut closed_payload = pr_event_payload("closed", 100, 10, 20);
        closed_payload["pull_request"]["head"]["repo"] = serde_json::Value::Null;
        let w = pr_webhook("closed", closed_payload);
        let outcome = h.handle(&w).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction)));
        let pr = pr_store
            .lookup_pull_request(10, 1)
            .await
            .unwrap()
            .unwrap();
        assert!(
            pr.closed_at.is_some(),
            "closed must still set closed_at even when head.repo is missing"
        );
    }

    #[tokio::test]
    async fn pr_labeled_without_head_repo_terminates_ignored_action() {
        // Slice 7 review fix: ignored-by-default actions terminate as
        // IgnoredAction WITHOUT requiring head.repo (or base.repo).
        let (_db, h, _policy_store, _install_store, _user_store, _pr_store) =
            make_pr_handler().await;
        let mut payload = pr_event_payload("labeled", 100, 10, 20);
        payload["pull_request"]["head"]["repo"] = serde_json::Value::Null;
        payload["pull_request"]["base"]["repo"] = serde_json::Value::Null;
        let w = pr_webhook("labeled", payload);
        let outcome = h.handle(&w).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction)));
    }

    #[tokio::test]
    async fn pr_closed_sets_closed_at_and_terminates_ignored_action() {
        // Slice 7: closed sets closed_at on the existing PR row,
        // terminates as IgnoredAction (no policy eval, no enqueue
        // signal — closing isn't a benchmark trigger).
        let (_db, h, _policy_store, _install_store, _user_store, pr_store) =
            make_pr_handler().await;
        // Materialise via opened first.
        let w_open = pr_webhook("opened", pr_event_payload("opened", 100, 10, 20));
        let _ = h.handle(&w_open).await;

        let w_close = pr_webhook("closed", pr_event_payload("closed", 100, 10, 20));
        let outcome = h.handle(&w_close).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction)));
        let pr = pr_store
            .lookup_pull_request(10, 1)
            .await
            .unwrap()
            .unwrap();
        assert!(pr.closed_at.is_some(), "closed event must set closed_at");
    }

    #[tokio::test]
    async fn pr_reopened_clears_closed_at_and_re_runs_policy_eval() {
        // Slice 7: reopened clears closed_at on the existing row AND
        // re-runs policy eval (policies may have changed since the
        // close).
        let (_db, h, policy_store, _install_store, _user_store, pr_store) = make_pr_handler().await;
        policy_store
            .seed_target(100, 10, true)
            .await;
        policy_store
            .seed_source(100, 20, true)
            .await;
        // Materialise then close.
        let _ = h
            .handle(&pr_webhook("opened", pr_event_payload("opened", 100, 10, 20)))
            .await;
        let _ = h
            .handle(&pr_webhook("closed", pr_event_payload("closed", 100, 10, 20)))
            .await;
        let closed = pr_store
            .lookup_pull_request(10, 1)
            .await
            .unwrap()
            .unwrap();
        assert!(closed.closed_at.is_some());

        // Now reopen.
        let w_reopen = pr_webhook("reopened", pr_event_payload("reopened", 100, 10, 20));
        let outcome = h.handle(&w_reopen).await;
        // Slice 9: accepted PR events do not enqueue → ProcessedPullRequest.
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::ProcessedPullRequest)));
        let reopened = pr_store
            .lookup_pull_request(10, 1)
            .await
            .unwrap()
            .unwrap();
        assert!(reopened.closed_at.is_none(), "reopened event must clear closed_at");
    }

    #[tokio::test]
    async fn pr_synchronize_keeps_pr_row_present() {
        // Slice 7 + slice 5: synchronize refreshes head metadata (via
        // upsert) AND re-runs policy eval. The PR row must remain
        // present even with no policies (denied path).
        let (_db, h, _policy_store, _install_store, _user_store, pr_store) =
            make_pr_handler().await;
        let w_open = pr_webhook("opened", pr_event_payload("opened", 100, 10, 20));
        let _ = h.handle(&w_open).await;
        let w_sync = pr_webhook("synchronize", pr_event_payload("synchronize", 100, 10, 20));
        let outcome = h.handle(&w_sync).await;
        // No policies seeded → DeniedTargetPolicy on synchronize.
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::DeniedTargetPolicy)));
        let pr = pr_store
            .lookup_pull_request(10, 1)
            .await
            .unwrap();
        assert!(pr.is_some(), "PR row must remain after synchronize even on denied path");
    }

    #[tokio::test]
    async fn pr_closed_for_unseen_pr_is_idempotent_no_op() {
        // closed for a PR whose opened event we never saw is a
        // graceful no-op (no row to update; terminal IgnoredAction).
        let (_db, h, _policy_store, _install_store, _user_store, pr_store) =
            make_pr_handler().await;
        let w = pr_webhook("closed", pr_event_payload("closed", 100, 10, 20));
        let outcome = h.handle(&w).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction)));
        assert!(
            pr_store
                .lookup_pull_request(10, 1)
                .await
                .unwrap()
                .is_none(),
            "closed for an unseen PR must not materialise a row"
        );
    }

    #[tokio::test]
    async fn pr_opened_missing_target_is_denied_target_policy() {
        let (_db, h, _policy_store, _install_store, _user_store, _pr_store) =
            make_pr_handler().await;
        let w = pr_webhook("opened", pr_event_payload("opened", 100, 10, 20));
        let outcome = h.handle(&w).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::DeniedTargetPolicy)));
    }

    #[tokio::test]
    async fn pr_opened_disabled_target_is_denied_target_policy() {
        let (_db, h, policy_store, _install_store, _user_store, _pr_store) =
            make_pr_handler().await;
        policy_store
            .seed_target(100, 10, false)
            .await;
        policy_store
            .seed_source(100, 20, true)
            .await;
        let w = pr_webhook("opened", pr_event_payload("opened", 100, 10, 20));
        let outcome = h.handle(&w).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::DeniedTargetPolicy)));
    }

    #[tokio::test]
    async fn pr_opened_missing_source_is_denied_source_policy() {
        let (_db, h, policy_store, _install_store, _user_store, _pr_store) =
            make_pr_handler().await;
        policy_store
            .seed_target(100, 10, true)
            .await;
        // No source.
        let w = pr_webhook("opened", pr_event_payload("opened", 100, 10, 20));
        let outcome = h.handle(&w).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::DeniedSourcePolicy)));
    }

    #[tokio::test]
    async fn pr_labeled_or_unlabeled_actions_are_ignored_without_side_effects() {
        // Slice 7 lifecycle dispatch: labeled / unlabeled / assigned
        // / etc. are ignored with no DB side effects. closed and
        // edited do have side effects (closed sets closed_at, edited
        // runs materialise + policy eval) and are covered by
        // dedicated tests below.
        let (_db, h, _policy_store, _install_store, _user_store, _pr_store) =
            make_pr_handler().await;
        for action in ["labeled", "unlabeled", "assigned"] {
            let w = pr_webhook(action, pr_event_payload(action, 100, 10, 20));
            let outcome = h.handle(&w).await;
            assert!(
                matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction)),
                "action={action} should ignore"
            );
        }
    }

    #[tokio::test]
    async fn pr_null_payload_is_error() {
        let (_db, h, _ps, _install_store, _user_store, _pr_store) = make_pr_handler().await;
        let mut w = pr_webhook("opened", serde_json::Value::Null);
        w.payload = None;
        let outcome = h.handle(&w).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::Error)));
    }

    #[tokio::test]
    async fn pr_synchronize_action_evaluates_policies_too() {
        // synchronize fires on every new push to a PR's head — should
        // re-evaluate policies (a previously-accepted PR's source repo
        // might have been disabled in the meantime).
        let (_db, h, policy_store, _install_store, _user_store, _pr_store) =
            make_pr_handler().await;
        policy_store
            .seed_target(100, 10, true)
            .await;
        // No source enabled.
        let w = pr_webhook("synchronize", pr_event_payload("synchronize", 100, 10, 20));
        let outcome = h.handle(&w).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::DeniedSourcePolicy)));
    }

    // ─── Slice 5 review-fix tests: membership gate on PR eval ──────────

    #[tokio::test]
    async fn pr_with_enabled_target_but_revoked_membership_is_denied_target_policy() {
        // Codex slice-5 review High #1: a target_repo_policy row with
        // is_enabled=TRUE must NOT cause acceptance if the membership
        // has been revoked since the policy was created.
        let (_db, h, policy_store, install_store, _user_store, _pr_store) = make_pr_handler().await;
        policy_store
            .seed_target(100, 10, true)
            .await;
        policy_store
            .seed_source(100, 20, true)
            .await;
        // Revoke the membership AFTER the policies are seeded.
        install_store
            .revoke_membership(100, 10)
            .await
            .unwrap();
        let w = pr_webhook("opened", pr_event_payload("opened", 100, 10, 20));
        let outcome = h.handle(&w).await;
        assert!(
            matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::DeniedTargetPolicy)),
            "revoked membership must deny target policy regardless of is_enabled state"
        );
    }

    #[tokio::test]
    async fn pr_with_enabled_target_but_suspended_install_is_denied_target_policy() {
        let (_db, h, policy_store, install_store, _user_store, _pr_store) = make_pr_handler().await;
        policy_store
            .seed_target(100, 10, true)
            .await;
        policy_store
            .seed_source(100, 20, true)
            .await;
        install_store
            .set_suspended(100, Some(chrono::Utc::now()))
            .await
            .unwrap();
        let w = pr_webhook("opened", pr_event_payload("opened", 100, 10, 20));
        let outcome = h.handle(&w).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::DeniedTargetPolicy)));
    }

    #[tokio::test]
    async fn pr_with_enabled_target_but_soft_deleted_install_is_denied_target_policy() {
        let (_db, h, policy_store, install_store, _user_store, _pr_store) = make_pr_handler().await;
        policy_store
            .seed_target(100, 10, true)
            .await;
        policy_store
            .seed_source(100, 20, true)
            .await;
        install_store
            .delete_installation(100)
            .await
            .unwrap();
        let w = pr_webhook("opened", pr_event_payload("opened", 100, 10, 20));
        let outcome = h.handle(&w).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::DeniedTargetPolicy)));
    }

    #[tokio::test]
    async fn repos_removed_cascades_to_disable_target_policy_and_triggers() {
        // `handle_removed` cascades to policy_store before revoking the
        // membership. This pins the cascade against the production stores.
        let (_db, handler, _r, install_store, policy_store, _user_store, gh) =
            make_repos_handler().await;
        gh.set_repo_canonical("stacks-network", "stacks-core", 10);
        // Add membership first.
        handler
            .handle(&repos_webhook(
                "added",
                repos_event_payload("added", 100, &[(10, "stacks-network/stacks-core")], &[]),
            ))
            .await;
        // The supported_repo_root needs to be in the handler's repo_store
        // for the .added path; since make_repos_handler doesn't expose it
        // pre-seeded, the membership won't actually be created above
        // (lineage unsupported). We instead seed membership + policy
        // directly for this test, then verify .removed cascades.
        let _ = install_store
            .add_or_restore_membership(100, 10)
            .await
            .unwrap();
        policy_store
            .upsert_target_policy(100, 10, None)
            .await
            .unwrap();
        policy_store
            .add_trigger_policy(
                100,
                10,
                TriggerKind::BranchPush,
                &TriggerMatchSpec::BranchPush { branch_name: "main".into() },
                None,
                None,
            )
            .await
            .unwrap();

        // Now fire the .removed event.
        handler
            .handle(&repos_webhook(
                "removed",
                repos_event_payload("removed", 100, &[], &[(10, "stacks-network/stacks-core")]),
            ))
            .await;

        // Verify cascade: target_policy + trigger both disabled,
        // membership revoked.
        let target = policy_store
            .lookup_target_policy(100, 10)
            .await
            .unwrap()
            .unwrap();
        assert!(!target.is_enabled, "target_repo_policy must be cascade-disabled by .removed");
        let triggers = policy_store
            .list_enabled_triggers(100, 10, TriggerKind::BranchPush)
            .await
            .unwrap();
        assert!(triggers.is_empty(), "trigger_policy must be cascade-disabled by .removed");
        let membership = install_store
            .membership(100, 10)
            .await
            .unwrap();
        assert!(
            membership
                .revoked_at
                .is_some()
        );
    }

    #[tokio::test]
    async fn repos_removed_cascades_to_soft_revoke_repo_scoped_user_roles() {
        // Third-pass review fix: handle_removed cascades into
        // github_user_role too, soft-revoking any repo-scoped grants
        // for the removed repo. Install-wide grants stay active.
        let (_db, handler, _r, install_store, _policy_store, user_store, _gh) =
            make_repos_handler().await;
        let _ = install_store
            .add_or_restore_membership(100, 10)
            .await
            .unwrap();
        user_store
            .seed_user(42, "alice", GithubAccountType::User)
            .await;
        user_store
            .seed_role(42, 100, Some(10), UserRole::TriggerPrBenchmark)
            .await;
        user_store
            .seed_role(42, 100, None, UserRole::TriggerPrBenchmark)
            .await; // install-wide

        // Fire .removed for repo=10.
        handler
            .handle(&repos_webhook(
                "removed",
                repos_event_payload("removed", 100, &[], &[(10, "stacks-network/stacks-core")]),
            ))
            .await;

        // Repo-scoped grant: revoked (has_role returns true ONLY because the
        // install-wide grant survived). Verify the repo-scoped grant
        // specifically by listing.
        let listed = user_store
            .list_roles(Some(100))
            .await
            .unwrap();
        let repo_scoped: Vec<_> = listed
            .iter()
            .filter(|r| r.github_repo_id == Some(10))
            .collect();
        assert_eq!(repo_scoped.len(), 1);
        assert!(
            repo_scoped[0]
                .revoked_at
                .is_some(),
            "repo-scoped grant on the removed repo must be soft-revoked"
        );
        let install_wide: Vec<_> = listed
            .iter()
            .filter(|r| r.github_repo_id.is_none())
            .collect();
        assert_eq!(install_wide.len(), 1);
        assert!(
            install_wide[0]
                .revoked_at
                .is_none(),
            "install-wide grant must survive the per-repo cascade"
        );
    }

    // ─── PushHandler tests ──────────────────────────────────────────────

    fn push_event_payload(install_id: i64, repo_id: i64, branch: &str) -> serde_json::Value {
        serde_json::json!({
            "ref": format!("refs/heads/{branch}"),
            "installation": { "id": install_id },
            "repository": { "id": repo_id, "full_name": "o/r" },
            "head_commit": {
                "id": "pushsha",
                "timestamp": "2026-05-29T10:00:00Z"
            }
        })
    }

    fn push_webhook_claimed(payload: serde_json::Value) -> ClaimedWebhook {
        ClaimedWebhook {
            id: 1,
            claim_token: uuid::Uuid::new_v4(),
            delivery_id: "d-push".into(),
            event_type: "push".into(),
            action: None,
            payload_installation_id: Some(100),
            payload: Some(payload),
            payload_size_bytes: 0,
            attempts: 0,
            received_at: Utc::now(),
        }
    }

    /// Production-store fixture for push/create handler tests.
    async fn make_trigger_stores()
    -> (TestDb, Arc<PostgresPolicyStore>, Arc<PostgresInstallationStore>, Arc<PostgresJobStore>)
    {
        let (db, pool) = setup_pg_db().await;
        let repo_store = PostgresRepoStore::new(pool.clone());
        repo_store
            .upsert_repo_identity(&NewRepoIdentity {
                id: 10,
                owner: "o".into(),
                name: "r".into(),
                default_branch: None,
            })
            .await
            .unwrap();
        let install_store = Arc::new(PostgresInstallationStore::new(pool.clone()));
        install_store
            .seed_allowed(42, "octo-org", GithubAccountType::Organization, true)
            .await;
        install_store
            .upsert_installation(&NewInstallation {
                id: 100,
                github_account_id: 42,
                account_login: "octo-org".into(),
                account_type: GithubAccountType::Organization,
            })
            .await
            .unwrap();
        let _ = install_store
            .add_or_restore_membership(100, 10)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO github_webhook \
             (id, delivery_id, event_type, payload_size_bytes) \
             VALUES (1, 'unit-trigger', 'push', 0)",
        )
        .execute(&pool)
        .await
        .unwrap();
        (
            db,
            Arc::new(PostgresPolicyStore::new(pool.clone())),
            install_store,
            Arc::new(PostgresJobStore::new(pool)),
        )
    }

    #[tokio::test]
    async fn push_with_matching_branch_trigger_enqueues_baseline_job() {
        // Slice 9: a matching branch_push trigger creates a `baseline`
        // job (resolved commit from head_commit) and terminates as
        // `EnqueuedJob`.
        //
        // Parent target must be enabled — slice 5 second-pass runtime
        // gate in `list_enabled_triggers` joins through
        // `target_repo_policy` and filters disabled/missing parents.
        let (_db, policy_store, install_store, job_store) = make_trigger_stores().await;
        policy_store
            .seed_target(100, 10, true)
            .await;
        policy_store
            .seed_trigger(
                100,
                10,
                TriggerKind::BranchPush,
                &TriggerMatchSpec::BranchPush { branch_name: "develop".into() },
                true,
            )
            .await;
        let h = PushHandler::new(policy_store, install_store, job_store.clone());
        let w = push_webhook_claimed(push_event_payload(100, 10, "develop"));
        let outcome = h.handle(&w).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::EnqueuedJob)));

        let jobs = job_store.all_jobs().await;
        assert_eq!(jobs.len(), 1);
        let job = &jobs[0];
        assert_eq!(job.github_repo_id, 10);
        assert_eq!(job.intent, sbgh_core::models::JobIntent::BaselineBenchmark);
        assert_eq!(job.source, sbgh_core::models::JobSource::GithubWebhook);
        assert_eq!(job.git_ref_kind, GitRefKind::Branch);
        assert_eq!(job.git_ref_display, "develop");
        assert_eq!(job.git_commit_hash.as_deref(), Some("pushsha"), "resolved at enqueue");
        assert!(job.git_committed_at.is_some());
        // Automated trigger → no responsible user, no PR link.
        assert!(
            job_store
                .user_links()
                .await
                .is_empty()
        );
        assert!(
            job_store
                .pr_links()
                .await
                .is_empty()
        );
    }

    #[test]
    fn branch_prefix_matches_release_family_only() {
        // item 0025 (v9): a plain-prefix matcher for release-branch families.
        let spec = serde_json::to_value(TriggerMatchSpec::BranchPrefix {
            prefix: "sb-integration/3.".into(),
        })
        .unwrap();
        assert!(matches_branch_push(&spec, "sb-integration/3.4.0.0.3"));
        assert!(matches_branch_push(&spec, "sb-integration/3.3.0.0.4"));
        assert!(!matches_branch_push(&spec, "sb-integration/2.9.0.0.1"));
        assert!(!matches_branch_push(&spec, "feature/unrelated"));
        // Exact `BranchPush` is unaffected.
        let exact =
            serde_json::to_value(TriggerMatchSpec::BranchPush { branch_name: "develop".into() })
                .unwrap();
        assert!(matches_branch_push(&exact, "develop"));
        assert!(!matches_branch_push(&exact, "develop-2"));
    }

    #[tokio::test]
    async fn push_baseline_persists_workload_key_from_default_args() {
        // roadmap-v7 Slice 2: a baseline's `workload_key` is computed at
        // enqueue from `default_args` (the seeded trigger has NULL
        // `bench_args`), so it MATCHES a bare `/benchmark` PR — which also
        // resolves to `default_args`. The cross-trigger equality itself is
        // unit-tested in `sbgh_core::bench_args`; here we assert the handler
        // wires `default_args` through and persists the key.
        const DEFAULT: &str = "--start-at 7800000 --count 5000 --bench-spans-only";
        let (_db, policy_store, install_store, job_store) = make_trigger_stores().await;
        policy_store
            .seed_target(100, 10, true)
            .await;
        policy_store
            .seed_trigger(
                100,
                10,
                TriggerKind::BranchPush,
                &TriggerMatchSpec::BranchPush { branch_name: "develop".into() },
                true,
            )
            .await;
        let h = PushHandler::new(policy_store, install_store, job_store.clone())
            .with_default_args(DEFAULT);
        let w = push_webhook_claimed(push_event_payload(100, 10, "develop"));
        assert!(matches!(
            h.handle(&w).await,
            ClassifyOutcome::Terminal(WebhookOutcome::EnqueuedJob)
        ));

        let jobs = job_store.all_jobs().await;
        let key = jobs[0].workload_key.clone();
        // NULL trigger args → default_args → same key a bare `/benchmark`
        // (empty override) would resolve to.
        let expected = sbgh_core::bench_args::resolve_bench_args(&[], DEFAULT).workload_key;
        assert_eq!(key.as_deref(), Some(expected.as_str()));
        // A different workload must NOT collide.
        let other =
            sbgh_core::bench_args::resolve_bench_args(&["--count".into(), "1".into()], DEFAULT)
                .workload_key;
        assert_ne!(key.as_deref(), Some(other.as_str()));
    }

    #[tokio::test]
    async fn push_matching_trigger_without_head_commit_does_not_enqueue() {
        // Slice 9: a branch deletion (or a commit-less push) arrives with
        // `head_commit: null`. Even when a branch_push trigger matches the
        // ref, there is nothing to benchmark — terminate as IgnoredAction
        // and create NO job (vs. enqueuing one with no resolvable commit).
        let (_db, policy_store, install_store, job_store) = make_trigger_stores().await;
        policy_store
            .seed_target(100, 10, true)
            .await;
        policy_store
            .seed_trigger(
                100,
                10,
                TriggerKind::BranchPush,
                &TriggerMatchSpec::BranchPush { branch_name: "develop".into() },
                true,
            )
            .await;
        let h = PushHandler::new(policy_store, install_store, job_store.clone());
        // No `head_commit` key → parses as None.
        let payload = serde_json::json!({
            "ref": "refs/heads/develop",
            "installation": { "id": 100 },
            "repository": { "id": 10, "full_name": "o/r" }
        });
        let w = push_webhook_claimed(payload);
        let outcome = h.handle(&w).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction)));
        assert!(
            job_store
                .all_jobs()
                .await
                .is_empty(),
            "branch deletion must not enqueue a job"
        );
    }

    #[tokio::test]
    async fn push_with_no_matching_trigger_is_ignored_action() {
        let (_db, policy_store, install_store, job_store) = make_trigger_stores().await;
        policy_store
            .seed_target(100, 10, true)
            .await;
        policy_store
            .seed_trigger(
                100,
                10,
                TriggerKind::BranchPush,
                &TriggerMatchSpec::BranchPush { branch_name: "develop".into() },
                true,
            )
            .await;
        let h = PushHandler::new(policy_store, install_store, job_store);
        let w = push_webhook_claimed(push_event_payload(100, 10, "feature-x"));
        let outcome = h.handle(&w).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction)));
    }

    #[tokio::test]
    async fn push_with_disabled_trigger_does_not_match() {
        // Disabled trigger must not surface in list_enabled_triggers.
        let (_db, policy_store, install_store, job_store) = make_trigger_stores().await;
        policy_store
            .seed_target(100, 10, true)
            .await;
        policy_store
            .seed_trigger(
                100,
                10,
                TriggerKind::BranchPush,
                &TriggerMatchSpec::BranchPush { branch_name: "develop".into() },
                false, // disabled
            )
            .await;
        let h = PushHandler::new(policy_store, install_store, job_store);
        let w = push_webhook_claimed(push_event_payload(100, 10, "develop"));
        let outcome = h.handle(&w).await;
        // Disabled trigger doesn't surface in list_enabled_triggers, so
        // outcome falls through to no-match `IgnoredAction` — NOT
        // `WouldEnqueueJob` (which would mean the disabled trigger
        // mistakenly counted as a match).
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction)));
    }

    #[tokio::test]
    async fn push_non_branch_ref_is_ignored() {
        // Internal refs (refs/internal/...) get skipped without a
        // policy lookup. Rare in practice but defensive.
        let (_db, policy_store, install_store, job_store) = make_trigger_stores().await;
        let h = PushHandler::new(policy_store, install_store, job_store);
        let payload = serde_json::json!({
            "ref": "refs/tags/v1.0",
            "installation": { "id": 100 },
            "repository": { "id": 10, "full_name": "o/r" }
        });
        let w = push_webhook_claimed(payload);
        let outcome = h.handle(&w).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction)));
    }

    // ─── CreateHandler tests ────────────────────────────────────────────

    fn create_event_payload(
        install_id: i64,
        repo_id: i64,
        ref_name: &str,
        ref_type: &str,
    ) -> serde_json::Value {
        serde_json::json!({
            "ref": ref_name,
            "ref_type": ref_type,
            "installation": { "id": install_id },
            "repository": { "id": repo_id, "full_name": "o/r" }
        })
    }

    fn create_webhook_claimed(payload: serde_json::Value) -> ClaimedWebhook {
        ClaimedWebhook {
            id: 1,
            claim_token: uuid::Uuid::new_v4(),
            delivery_id: "d-create".into(),
            event_type: "create".into(),
            action: None,
            payload_installation_id: Some(100),
            payload: Some(payload),
            payload_size_bytes: 0,
            attempts: 0,
            received_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn create_tag_matching_pattern_freezes_commit_before_enqueuing() {
        // A matching tag regex creates a `baseline` submission only after the
        // mutable tag has been resolved to its immutable commit.
        // Terminates as `EnqueuedJob`.
        //
        // Parent target must be enabled — slice 5 second-pass runtime
        // gate filters triggers whose parent target is disabled / missing.
        let (_db, policy_store, install_store, job_store) = make_trigger_stores().await;
        policy_store
            .seed_target(100, 10, true)
            .await;
        policy_store
            .seed_trigger(
                100,
                10,
                TriggerKind::TagCreated,
                &TriggerMatchSpec::TagCreated {
                    tag_pattern: r"^release/\d+\.\d+$".into(),
                },
                true,
            )
            .await;
        let github = sbgh_github::test_support::FakeGitHub::new();
        github.set_commit("o/r", "tags/release/1.2", &"a".repeat(40), None);
        let h = CreateHandler::new(policy_store, install_store, job_store.clone())
            .with_github(Arc::new(github));
        let w = create_webhook_claimed(create_event_payload(100, 10, "release/1.2", "tag"));
        let outcome = h.handle(&w).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::EnqueuedJob)));

        let jobs = job_store.all_jobs().await;
        assert_eq!(jobs.len(), 1);
        let job = &jobs[0];
        assert_eq!(job.intent, sbgh_core::models::JobIntent::BaselineBenchmark);
        assert_eq!(job.source, sbgh_core::models::JobSource::GithubWebhook);
        assert_eq!(job.git_ref_kind, GitRefKind::Tag);
        assert_eq!(job.git_ref_display, "release/1.2");
        assert_eq!(
            job.git_commit_hash.as_deref(),
            Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        );
    }

    #[tokio::test]
    async fn create_tag_not_matching_pattern_is_ignored_action() {
        let (_db, policy_store, install_store, job_store) = make_trigger_stores().await;
        policy_store
            .seed_target(100, 10, true)
            .await;
        policy_store
            .seed_trigger(
                100,
                10,
                TriggerKind::TagCreated,
                &TriggerMatchSpec::TagCreated {
                    tag_pattern: r"^release/\d+\.\d+$".into(),
                },
                true,
            )
            .await;
        let github = sbgh_github::test_support::FakeGitHub::new();
        github.set_commit("o/r", "tags/v1", &"b".repeat(40), None);
        let h = CreateHandler::new(policy_store, install_store, job_store)
            .with_github(Arc::new(github));
        let w = create_webhook_claimed(create_event_payload(100, 10, "feature/foo", "tag"));
        let outcome = h.handle(&w).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction)));
    }

    #[tokio::test]
    async fn create_branch_ref_is_skipped_without_policy_lookup() {
        // ref_type=branch must be silently ignored — those events are
        // already covered by `push`.
        let (_db, policy_store, install_store, job_store) = make_trigger_stores().await;
        let h = CreateHandler::new(policy_store, install_store, job_store);
        let w = create_webhook_claimed(create_event_payload(100, 10, "new-branch", "branch"));
        let outcome = h.handle(&w).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction)));
    }

    #[tokio::test]
    async fn create_tag_with_malformed_regex_skips_trigger_not_batch() {
        // Operator-supplied bad regex should log + skip ONLY that
        // trigger; other (well-formed) triggers in the same batch
        // still evaluate. Modeled here with one bad + one good.
        let (_db, policy_store, install_store, job_store) = make_trigger_stores().await;
        policy_store
            .seed_target(100, 10, true)
            .await;
        policy_store
            .seed_trigger(
                100,
                10,
                TriggerKind::TagCreated,
                &TriggerMatchSpec::TagCreated {
                    tag_pattern: "[malformed".into(),
                },
                true,
            )
            .await;
        policy_store
            .seed_trigger(
                100,
                10,
                TriggerKind::TagCreated,
                &TriggerMatchSpec::TagCreated { tag_pattern: r"^v\d+$".into() },
                true,
            )
            .await;
        let github = sbgh_github::test_support::FakeGitHub::new();
        github.set_commit("o/r", "tags/v1", &"b".repeat(40), None);
        let h = CreateHandler::new(policy_store, install_store, job_store)
            .with_github(Arc::new(github));
        let w = create_webhook_claimed(create_event_payload(100, 10, "v1", "tag"));
        let outcome = h.handle(&w).await;
        // The good trigger still matches and enqueues despite the malformed peer.
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::EnqueuedJob)));
    }

    #[tokio::test]
    async fn create_null_payload_is_error() {
        let (_db, policy_store, install_store, job_store) = make_trigger_stores().await;
        let h = CreateHandler::new(policy_store, install_store, job_store);
        let mut w = create_webhook_claimed(serde_json::Value::Null);
        w.payload = None;
        let outcome = h.handle(&w).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::Error)));
    }

    // ─── Slice 5 review-fix tests: membership gate on push/create ──────

    #[tokio::test]
    async fn push_with_matching_trigger_but_revoked_membership_is_ignored() {
        // Codex slice-5 review High #2: even a perfectly matching
        // trigger_policy row must NOT cause acceptance if the
        // membership has been revoked.
        let (_db, policy_store, install_store, job_store) = make_trigger_stores().await;
        policy_store
            .seed_target(100, 10, true)
            .await;
        policy_store
            .seed_trigger(
                100,
                10,
                TriggerKind::BranchPush,
                &TriggerMatchSpec::BranchPush { branch_name: "develop".into() },
                true,
            )
            .await;
        install_store
            .revoke_membership(100, 10)
            .await
            .unwrap();
        let h = PushHandler::new(policy_store, install_store, job_store);
        let w = push_webhook_claimed(push_event_payload(100, 10, "develop"));
        let outcome = h.handle(&w).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction)));
    }

    #[tokio::test]
    async fn push_with_matching_trigger_but_soft_deleted_install_is_ignored() {
        let (_db, policy_store, install_store, job_store) = make_trigger_stores().await;
        policy_store
            .seed_target(100, 10, true)
            .await;
        policy_store
            .seed_trigger(
                100,
                10,
                TriggerKind::BranchPush,
                &TriggerMatchSpec::BranchPush { branch_name: "develop".into() },
                true,
            )
            .await;
        install_store
            .delete_installation(100)
            .await
            .unwrap();
        let h = PushHandler::new(policy_store, install_store, job_store);
        let w = push_webhook_claimed(push_event_payload(100, 10, "develop"));
        let outcome = h.handle(&w).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction)));
    }

    #[tokio::test]
    async fn create_tag_with_matching_trigger_but_revoked_membership_is_ignored() {
        let (_db, policy_store, install_store, job_store) = make_trigger_stores().await;
        policy_store
            .seed_target(100, 10, true)
            .await;
        policy_store
            .seed_trigger(
                100,
                10,
                TriggerKind::TagCreated,
                &TriggerMatchSpec::TagCreated { tag_pattern: r"^v\d+$".into() },
                true,
            )
            .await;
        install_store
            .revoke_membership(100, 10)
            .await
            .unwrap();
        let h = CreateHandler::new(policy_store, install_store, job_store);
        let w = create_webhook_claimed(create_event_payload(100, 10, "v1", "tag"));
        let outcome = h.handle(&w).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction)));
    }

    // ─── Slice 5 follow-up review-fix tests: parent-target gate ────────

    #[tokio::test]
    async fn push_with_matching_trigger_but_disabled_parent_target_is_ignored() {
        // Slice 5 second-pass review fix: even with active membership +
        // is_enabled trigger row, the trigger MUST NOT match if its
        // parent target_repo_policy is disabled. Operator's manual
        // `policy target disable` is the path this protects against
        // (the cascade in `installation_repositories.removed` already
        // disables the trigger row directly; this gate covers ad-hoc
        // CLI disables that pre-date the cascade fix or that take a
        // different code path).
        let (_db, policy_store, install_store, job_store) = make_trigger_stores().await;
        // Parent target exists but DISABLED.
        policy_store
            .seed_target(100, 10, false)
            .await;
        policy_store
            .seed_trigger(
                100,
                10,
                TriggerKind::BranchPush,
                &TriggerMatchSpec::BranchPush { branch_name: "develop".into() },
                true,
            )
            .await;
        let h = PushHandler::new(policy_store, install_store, job_store);
        let w = push_webhook_claimed(push_event_payload(100, 10, "develop"));
        let outcome = h.handle(&w).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction)));
    }

    #[tokio::test]
    async fn create_tag_with_matching_trigger_but_disabled_parent_target_is_ignored() {
        let (_db, policy_store, install_store, job_store) = make_trigger_stores().await;
        policy_store
            .seed_target(100, 10, false)
            .await;
        policy_store
            .seed_trigger(
                100,
                10,
                TriggerKind::TagCreated,
                &TriggerMatchSpec::TagCreated { tag_pattern: r"^v\d+$".into() },
                true,
            )
            .await;
        let h = CreateHandler::new(policy_store, install_store, job_store);
        let w = create_webhook_claimed(create_event_payload(100, 10, "v1", "tag"));
        let outcome = h.handle(&w).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction)));
    }

    #[test]
    fn backoff_doubles_until_cap() {
        let base = chrono::Duration::seconds(2);
        let cap = chrono::Duration::seconds(16);
        assert_eq!(backoff_delay(1, base, cap), chrono::Duration::seconds(2));
        assert_eq!(backoff_delay(2, base, cap), chrono::Duration::seconds(4));
        assert_eq!(backoff_delay(3, base, cap), chrono::Duration::seconds(8));
        assert_eq!(backoff_delay(4, base, cap), chrono::Duration::seconds(16));
        // capped after that
        assert_eq!(backoff_delay(5, base, cap), chrono::Duration::seconds(16));
        assert_eq!(backoff_delay(99, base, cap), chrono::Duration::seconds(16));
    }
}
