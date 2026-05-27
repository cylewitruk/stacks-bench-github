//! Webhook processor (slices 2a + 2b).
//!
//! Pulls webhook rows from `github_webhook` via [`WebhookInbox`], hands
//! each to a pluggable [`Classifier`], and writes the resulting
//! outcome back. Implements the queue state machine — claim, terminate,
//! retry-with-backoff, permanent-failure-on-attempts-exhausted,
//! stuck-claim sweep — and runs concurrently with the legacy job
//! `Runner` in production via `tokio::try_join!` from `main`.
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
use sbgh_core::db::{
    ClaimedWebhook, InstallationStore, NewInstallation, NewRepoIdentity, NewRepoLineage, RepoStore,
    WebhookInbox,
};
use sbgh_core::github::{
    GitHubApi, InstallationEvent, InstallationRepositoriesEvent, IssueCommentEvent, RepoRef,
    RepoSummary, parse_command,
};
use sbgh_core::models::{GithubAccountType, WebhookOutcome};

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
/// - `created` on a PR with valid `/benchmark` → `ignored_action` in Phase 1.
///   Slice 9 changes this branch to `enqueued_job` + creates the new `job` row.
///   The legacy handler→`jobs` path continues to actually run the bench in the
///   meantime.
/// - NULL / unparseable payload → `error` (can't classify; better terminal than
///   infinite retry)
pub struct IssueCommentHandler;

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

        match parse_command(&event.comment.body) {
            Ok(Some(_)) => ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction),
            Ok(None) | Err(_) => ClassifyOutcome::Terminal(WebhookOutcome::IgnoredNoCommand),
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
///   = NOW()`) AND bulk-revoke every active membership in
///   `github_installation_repo`. The install row is preserved so slice 8+ job
///   FKs remain valid. Slice 5+ policy tables will be soft-disabled in the same
///   transaction once they ship. Outcome: `processed_installation`; missing
///   install → `ignored_unknown_installation`.
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
    /// `Arc::new(InMemoryRepoStore::new())` + `Arc::new(FakeGitHub::new())`
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
        _ => return ClassifyOutcome::Terminal(WebhookOutcome::DeniedInstallAllowlist),
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
    gh: Arc<dyn GitHubApi>,
}

impl InstallationRepositoriesHandler {
    pub fn new(
        repo_store: Arc<dyn RepoStore>,
        membership_store: Arc<dyn InstallationStore>,
        gh: Arc<dyn GitHubApi>,
    ) -> Self {
        Self {
            repo_store,
            membership_store,
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
    payload_repo: &sbgh_core::github::InstallationRepository,
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
        Ok(Some(_)) => RepoMembershipOutcome::Added,
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
    repos: &[sbgh_core::github::InstallationRepository],
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
    install_id: i64,
    repos: &[sbgh_core::github::InstallationRepository],
) -> ClassifyOutcome {
    let mut any_revoked = false;
    for repo in repos {
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

/// Tunables. Reasonable defaults are picked to play nicely with
/// GitHub's redelivery cadence and a single-orchestrator deployment.
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
        }
    }
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
        match self
            .classifier
            .classify(&claimed)
            .await
        {
            ClassifyOutcome::Terminal(outcome) => {
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

    use sbgh_core::db::{InMemoryWebhookInbox, SeedWebhook};
    use sbgh_core::models::{WebhookOutcome, WebhookStatus};

    use super::*;

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
        }
    }

    fn seed(inbox: &InMemoryWebhookInbox, delivery: &str, event: &str) -> i64 {
        inbox.seed(SeedWebhook {
            delivery_id: delivery.into(),
            event_type: event.into(),
            payload_size_bytes: 42,
            ..Default::default()
        })
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
        let inbox = Arc::new(InMemoryWebhookInbox::new());
        let id = seed(&inbox, "d-1", "push");
        let classifier =
            ScriptedClassifier::new(vec![ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction)]);
        let proc = WebhookProcessor::new(inbox.clone(), classifier, fast_config());

        assert!(
            proc.process_one()
                .await
                .unwrap()
        );
        let row = inbox.row(id).unwrap();
        assert_eq!(row.status, WebhookStatus::Ignored);
        assert_eq!(row.outcome, Some(WebhookOutcome::IgnoredAction));
        assert!(row.processed_at.is_some());
        assert!(row.claim_token.is_none(), "claim cleared on terminal");
    }

    #[tokio::test]
    async fn process_one_returns_false_when_empty() {
        let inbox = Arc::new(InMemoryWebhookInbox::new());
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
        let inbox = Arc::new(InMemoryWebhookInbox::new());
        let id = seed(&inbox, "d-2", "push");
        let classifier =
            ScriptedClassifier::new(vec![ClassifyOutcome::Retryable("transient".into())]);
        let proc = WebhookProcessor::new(inbox.clone(), classifier, fast_config());

        let before = Utc::now();
        assert!(
            proc.process_one()
                .await
                .unwrap()
        );
        let row = inbox.row(id).unwrap();
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
        let inbox = Arc::new(InMemoryWebhookInbox::new());
        let id = seed(&inbox, "d-3", "push");
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
        inbox.set_next_attempt_at(id, Utc::now());
        // Second attempt → next_attempts (2) >= max_attempts (2) →
        // permanent failure.
        proc.process_one()
            .await
            .unwrap();

        let row = inbox.row(id).unwrap();
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
        let inbox = Arc::new(InMemoryWebhookInbox::new());
        let id = seed(&inbox, "d-4", "push");
        // Simulate a crashed processor: claim normally, then backdate
        // claimed_at past the lease window.
        let _ = inbox
            .claim_next(ALL_EVENT_TYPES)
            .await
            .unwrap()
            .expect("seeded row must be claimable");
        inbox.set_claimed_at(id, Utc::now() - chrono::Duration::seconds(60));

        let recovered = inbox
            .sweep_stuck_claims(chrono::Duration::seconds(10))
            .await
            .unwrap();
        assert_eq!(recovered, 1);

        let row = inbox.row(id).unwrap();
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
        // Both calls run sequentially in this test (Mutex on the
        // in-memory state), but the semantic we verify is that each
        // claim_next returns a different row id — which is the
        // guarantee FOR UPDATE SKIP LOCKED gives in Postgres.
        let inbox = Arc::new(InMemoryWebhookInbox::new());
        let id_a = seed(&inbox, "d-a", "push");
        let id_b = seed(&inbox, "d-b", "push");

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
        let inbox = Arc::new(InMemoryWebhookInbox::new());
        let id = seed(&inbox, "d-5", "push");

        let claimed = inbox
            .claim_next(ALL_EVENT_TYPES)
            .await
            .unwrap()
            .unwrap();
        // Force the claim to look ancient and sweep it.
        inbox.set_claimed_at(id, Utc::now() - chrono::Duration::seconds(60));
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
        let row = inbox.row(id).unwrap();
        assert_eq!(row.status, WebhookStatus::RetryableError);
        assert!(row.outcome.is_none(), "stale write must not set outcome");
    }

    #[tokio::test]
    async fn complete_clears_last_error_from_prior_retries() {
        // A row that transient-failed once and then succeeded must
        // not leave a stale last_error string visible to ops queries.
        let inbox = Arc::new(InMemoryWebhookInbox::new());
        let id = seed(&inbox, "d-6", "push");
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
            inbox
                .row(id)
                .unwrap()
                .last_error
                .as_deref(),
            Some("transient blip")
        );

        inbox.set_next_attempt_at(id, Utc::now());
        proc.process_one()
            .await
            .unwrap();

        let row = inbox.row(id).unwrap();
        assert_eq!(row.status, WebhookStatus::Ignored);
        assert!(
            row.last_error.is_none(),
            "complete() must clear last_error from prior retry attempts; got {:?}",
            row.last_error
        );
    }

    // ─── BasicClassifier ────────────────────────────────────────────────

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
                "user": { "login": "alice" },
                "author_association": "MEMBER",
            },
            "issue": {
                "number": 1,
                "pull_request": pull_request,
            },
            "repository": { "full_name": "o/r" },
            "sender": { "login": "alice" },
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
        let outcome = IssueCommentHandler
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
        let outcome = IssueCommentHandler
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
        let outcome = IssueCommentHandler
            .handle(&webhook)
            .await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::IgnoredNoCommand)));
    }

    #[tokio::test]
    async fn basic_issue_comment_pr_with_benchmark_is_ignored_action_in_phase1() {
        // Slice 9 will change this to enqueued_job; pinning the Phase 1
        // behavior here keeps the legacy `/benchmark` path intact.
        let webhook = make_claimed(
            "issue_comment",
            Some("created"),
            Some(issue_comment_payload("created", "/benchmark run", true)),
        );
        let outcome = IssueCommentHandler
            .handle(&webhook)
            .await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction)));
    }

    #[tokio::test]
    async fn basic_issue_comment_null_payload_is_error() {
        let webhook = make_claimed("issue_comment", Some("created"), None);
        let outcome = IssueCommentHandler
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
        let outcome = IssueCommentHandler
            .handle(&webhook)
            .await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::Error)));
    }

    #[test]
    fn basic_classifier_only_lists_registered_handlers() {
        // The router only advertises event types it has a registered
        // handler for. A slice-2b-only build (issue_comment handler only)
        // must NOT claim `installation` rows even though the schema has
        // outcomes for them; that's what slice 3's handler registration
        // unlocks. Pinning this contract here catches "added an
        // EventHandler trait impl but forgot to register it in the
        // builder" regressions.
        let only_ic = BasicClassifier::builder()
            .with_handler(Arc::new(IssueCommentHandler))
            .build();
        assert_eq!(only_ic.supported_event_types(), &["issue_comment"]);

        let store = Arc::new(sbgh_core::db::InMemoryInstallationStore::new());
        let with_install = BasicClassifier::builder()
            .with_handler(Arc::new(IssueCommentHandler))
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
        let inbox = Arc::new(InMemoryWebhookInbox::new());
        let issue_comment_id = inbox.seed(SeedWebhook {
            delivery_id: "d-ic".into(),
            event_type: "issue_comment".into(),
            action: Some("created".into()),
            payload: Some(issue_comment_payload("created", "looks good", true)),
            payload_size_bytes: 0,
            ..Default::default()
        });
        let installation_id = inbox.seed(SeedWebhook {
            delivery_id: "d-inst".into(),
            event_type: "installation".into(),
            action: Some("created".into()),
            payload_size_bytes: 0,
            ..Default::default()
        });
        let classifier = BasicClassifier::builder()
            .with_handler(Arc::new(IssueCommentHandler))
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

        let ic = inbox
            .row(issue_comment_id)
            .unwrap();
        assert!(matches!(ic.status, WebhookStatus::Ignored));

        let inst = inbox
            .row(installation_id)
            .unwrap();
        assert_eq!(inst.status, WebhookStatus::Received);
        assert!(inst.claim_token.is_none());
        assert!(inst.outcome.is_none());
    }

    #[test]
    #[should_panic(expected = "duplicate EventHandler")]
    fn builder_panics_on_duplicate_handler_for_same_event_type() {
        // Programmer error we want surfaced at startup, not silently
        // shadowed at runtime.
        let _ = BasicClassifier::builder()
            .with_handler(Arc::new(IssueCommentHandler))
            .with_handler(Arc::new(IssueCommentHandler))
            .build();
    }

    #[tokio::test]
    async fn router_with_no_matching_handler_terminates_as_error() {
        // Handler's allowlist + the inbox claim filter should prevent
        // an unregistered event type from ever reaching classify(),
        // but if a misconfiguration lets one slip through, the router
        // records it terminally instead of looping.
        let router = BasicClassifier::builder()
            .with_handler(Arc::new(IssueCommentHandler))
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
    fn make_install_handler(
        store: Arc<sbgh_core::db::InMemoryInstallationStore>,
    ) -> InstallationHandler {
        InstallationHandler::new(
            store,
            Arc::new(sbgh_core::db::InMemoryRepoStore::new()),
            Arc::new(sbgh_core::github::test_support::FakeGitHub::new()),
        )
    }

    #[tokio::test]
    async fn installation_created_for_allowed_account_upserts_and_processes() {
        let store = Arc::new(sbgh_core::db::InMemoryInstallationStore::new());
        store.seed_allowed(42, "octo-org", GithubAccountType::Organization, true);
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
            .expect("installation row must be materialised");
        assert_eq!(inst.github_account_id, 42);
        assert_eq!(inst.account_login, "octo-org");
        assert_eq!(inst.account_type, GithubAccountType::Organization);
        assert!(inst.suspended_at.is_none());
    }

    #[tokio::test]
    async fn installation_created_for_unknown_account_is_denied() {
        let store = Arc::new(sbgh_core::db::InMemoryInstallationStore::new());
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
                .is_none(),
            "denied install MUST NOT materialise a row"
        );
    }

    #[tokio::test]
    async fn installation_created_for_disabled_account_is_denied() {
        // Disabled (soft-paused) installer must take the same deny path
        // as an unknown one. Operator pause is operationally identical
        // to "never approved" from the App's perspective.
        let store = Arc::new(sbgh_core::db::InMemoryInstallationStore::new());
        store.seed_allowed(42, "octo-org", GithubAccountType::Organization, false);
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
                .is_none()
        );
    }

    #[tokio::test]
    async fn installation_created_is_idempotent_on_redelivery() {
        // GitHub re-delivers webhooks freely. A second installation.created
        // for the same install id must be a no-op upsert, not a new row.
        let store = Arc::new(sbgh_core::db::InMemoryInstallationStore::new());
        store.seed_allowed(42, "octo-org", GithubAccountType::Organization, true);
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
        assert_eq!(store.installations().len(), 1);
    }

    #[tokio::test]
    async fn installation_suspend_sets_suspended_at() {
        let store = Arc::new(sbgh_core::db::InMemoryInstallationStore::new());
        store.seed_allowed(42, "octo-org", GithubAccountType::Organization, true);
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
            .unwrap();
        assert!(inst.suspended_at.is_some(), "suspend MUST set suspended_at");
    }

    #[tokio::test]
    async fn installation_unsuspend_clears_suspended_at() {
        let store = Arc::new(sbgh_core::db::InMemoryInstallationStore::new());
        store.seed_allowed(42, "octo-org", GithubAccountType::Organization, true);
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
            .unwrap();
        assert!(inst.suspended_at.is_none(), "unsuspend MUST clear suspended_at");
    }

    #[tokio::test]
    async fn installation_suspend_for_unknown_install_is_ignored() {
        // A suspend for an install we never accepted (allowlist denied
        // at create) must not materialise a row; it's harmless to skip.
        let store = Arc::new(sbgh_core::db::InMemoryInstallationStore::new());
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
                .is_none()
        );
    }

    #[tokio::test]
    async fn installation_deleted_soft_deletes_install_row() {
        // Slice 4: install.deleted is a soft-delete (sets deleted_at) so
        // membership FKs and future job FKs stay valid. The row is NOT
        // removed.
        let store = Arc::new(sbgh_core::db::InMemoryInstallationStore::new());
        store.seed_allowed(42, "octo-org", GithubAccountType::Organization, true);
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
            .expect("soft-delete must keep the row");
        assert!(row.deleted_at.is_some(), "deleted MUST set deleted_at");
    }

    #[tokio::test]
    async fn installation_deleted_for_unknown_install_is_ignored() {
        let store = Arc::new(sbgh_core::db::InMemoryInstallationStore::new());
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
        let store = Arc::new(sbgh_core::db::InMemoryInstallationStore::new());
        store.seed_allowed(42, "octo-org", GithubAccountType::Organization, true);
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
        let store = Arc::new(sbgh_core::db::InMemoryInstallationStore::new());
        let h = make_install_handler(store);
        let mut w = installation_webhook("created", serde_json::Value::Null);
        w.payload = None;

        let outcome = h.handle(&w).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::Error)));
    }

    #[tokio::test]
    async fn installation_bad_typed_shape_is_error() {
        let store = Arc::new(sbgh_core::db::InMemoryInstallationStore::new());
        let h = make_install_handler(store);
        let w = installation_webhook("created", serde_json::json!({ "action": "created" }));

        let outcome = h.handle(&w).await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::Error)));
    }

    #[tokio::test]
    async fn installation_unknown_account_type_is_error() {
        // GH adding a new account type (unlikely but) would land here.
        // Better to record-and-investigate than guess.
        let store = Arc::new(sbgh_core::db::InMemoryInstallationStore::new());
        store.seed_allowed(42, "octo-org", GithubAccountType::Organization, true);
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
        InstallationHandler,
        Arc<sbgh_core::db::InMemoryInstallationStore>,
        Arc<sbgh_core::db::InMemoryRepoStore>,
        sbgh_core::github::test_support::FakeGitHub,
    ) {
        let install_store = Arc::new(sbgh_core::db::InMemoryInstallationStore::new());
        install_store.seed_allowed(42, "octo-org", GithubAccountType::Organization, true);
        let repo_store = Arc::new(sbgh_core::db::InMemoryRepoStore::new());
        repo_store.seed_supported_root(root_repo_id, root_owner, root_name, true);
        let gh = sbgh_core::github::test_support::FakeGitHub::new();
        let handler = InstallationHandler::new(
            install_store.clone(),
            repo_store.clone(),
            Arc::new(gh.clone()),
        );
        (handler, install_store, repo_store, gh)
    }

    #[tokio::test]
    async fn installation_created_with_supported_initial_repos_creates_memberships() {
        // Codex slice-4 high finding: a fresh install must materialise
        // memberships from the payload's `repositories` array; otherwise
        // there'd be no `github_installation_repo` rows until a later
        // `installation_repositories.added` event happened.
        let (h, install_store, _repo_store, gh) =
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
            .expect("initial membership must be materialised by installation.created");
        assert!(m.revoked_at.is_none(), "fresh membership is active");
    }

    #[tokio::test]
    async fn installation_created_with_unsupported_initial_repos_still_processed() {
        // Install creation itself succeeded; per-repo unsupported
        // results are reflected in the membership table (none created)
        // but the webhook-level outcome stays ProcessedInstallation —
        // ops query "was this install ingested" should answer yes.
        let (h, install_store, _repo_store, gh) =
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
                .is_none(),
            "unsupported initial repo must NOT get a membership row"
        );
        // Install row itself created.
        assert!(
            install_store
                .installation(100)
                .is_some()
        );
    }

    #[tokio::test]
    async fn installation_created_with_id_mismatch_in_initial_repos_skips_membership() {
        // Codex slice-4 M2: the payload says repo.id=10, but GH's
        // /repos lookup resolves to id=99 (rename/recycling staleness).
        // Membership must NOT be granted — otherwise we'd grant on the
        // wrong repo.
        let (h, install_store, _repo_store, gh) =
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
                .is_none()
        );
        assert!(
            install_store
                .membership(100, 99)
                .is_none()
        );
    }

    #[tokio::test]
    async fn installation_created_with_no_repositories_still_processed_as_before() {
        // Slice 3 contract preserved: an install.created with no
        // repositories array (e.g. parsed from a payload that omits
        // the field) still upserts the install and returns
        // ProcessedInstallation without doing any membership work.
        let (h, install_store, _repo_store, _gh) =
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
                .is_some()
        );
        assert!(
            install_store
                .memberships()
                .is_empty()
        );
    }

    // ─── InstallationRepositoriesHandler (slice 4) ──────────────────────

    use sbgh_core::db::InMemoryRepoStore;
    use sbgh_core::github::test_support::FakeGitHub;

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

    /// Build a handler wired against in-memory stores + a FakeGitHub.
    /// Seeds an install (id=100) and an allowed account so memberships
    /// can be inserted without FK errors. The caller seeds whatever
    /// supported_repo_root rows + GH-API responses the test needs.
    async fn make_repos_handler() -> (
        InstallationRepositoriesHandler,
        Arc<InMemoryRepoStore>,
        Arc<sbgh_core::db::InMemoryInstallationStore>,
        FakeGitHub,
    ) {
        let repo_store = Arc::new(InMemoryRepoStore::new());
        let install_store = Arc::new(sbgh_core::db::InMemoryInstallationStore::new());
        install_store.seed_allowed(42, "octo-org", GithubAccountType::Organization, true);
        install_store
            .upsert_installation(&NewInstallation {
                id: 100,
                github_account_id: 42,
                account_login: "octo-org".into(),
                account_type: GithubAccountType::Organization,
            })
            .await
            .unwrap();
        let gh = FakeGitHub::new();
        let handler = InstallationRepositoriesHandler::new(
            repo_store.clone(),
            install_store.clone(),
            Arc::new(gh.clone()),
        );
        (handler, repo_store, install_store, gh)
    }

    #[tokio::test]
    async fn repos_added_for_canonical_supported_repo_creates_membership() {
        let (handler, repo_store, install_store, gh) = make_repos_handler().await;
        // Repo 10 is the canonical root + on the supported list.
        repo_store.seed_supported_root(10, "stacks-network", "stacks-core", true);
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
            .expect("membership must be created");
        assert!(m.revoked_at.is_none(), "new membership is active");
    }

    #[tokio::test]
    async fn repos_added_for_fork_of_supported_root_creates_membership() {
        // Fork whose `source` is the supported canonical. Lineage walk
        // must record both the fork + the source as github_repo rows;
        // the support gate must accept via fork_root_github_repo_id.
        let (handler, repo_store, install_store, gh) = make_repos_handler().await;
        repo_store.seed_supported_root(10, "stacks-network", "stacks-core", true);
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
            .expect("fork must be upserted");
        assert_eq!(fork.fork_root_github_repo_id, Some(10));
        assert!(
            install_store
                .membership(100, 20)
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
        let (handler, repo_store, install_store, gh) = make_repos_handler().await;
        repo_store.seed_supported_root(10, "stacks-network", "stacks-core", true);
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
            .expect("leaf must be upserted");
        assert_eq!(leaf.fork_root_github_repo_id, Some(10));
        assert_eq!(leaf.parent_github_repo_id, Some(20));
        assert!(repo_store.repo(20).is_some(), "intermediate parent must be upserted too");
        assert!(
            install_store
                .membership(100, 30)
                .is_some()
        );
    }

    #[tokio::test]
    async fn repos_added_for_unsupported_lineage_skips_membership_but_caches_repo() {
        // The repo row STILL gets recorded (audit trail of "we saw this
        // repo and decided we don't support it") but no membership.
        let (handler, repo_store, install_store, gh) = make_repos_handler().await;
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
        assert!(repo_store.repo(99).is_some(), "repo identity cached even when unsupported");
        assert!(
            install_store
                .membership(100, 99)
                .is_none(),
            "no membership for unsupported"
        );
    }

    #[tokio::test]
    async fn repos_added_mixed_accepted_and_rejected_aggregates_as_processed() {
        // Codex M-fix-style aggregation: any-accepted wins over
        // any-rejected for the webhook-level outcome. Per-repo
        // decisions are recorded in their respective rows.
        let (handler, repo_store, install_store, gh) = make_repos_handler().await;
        repo_store.seed_supported_root(10, "stacks-network", "stacks-core", true);
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
                .is_some()
        );
        assert!(
            install_store
                .membership(100, 99)
                .is_none()
        );
    }

    #[tokio::test]
    async fn repos_added_with_no_repos_is_ignored_action() {
        let (handler, _r, _i, _gh) = make_repos_handler().await;
        let outcome = handler
            .handle(&repos_webhook("added", repos_event_payload("added", 100, &[], &[])))
            .await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction)));
    }

    #[tokio::test]
    async fn repos_added_disabled_supported_root_is_unsupported() {
        // A disabled supported_repo_root row must NOT extend support to
        // its forks. Operator soft-disabled, processor must respect.
        let (handler, repo_store, install_store, gh) = make_repos_handler().await;
        repo_store.seed_supported_root(10, "stacks-network", "stacks-core", false);
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
                .is_none()
        );
    }

    #[tokio::test]
    async fn repos_added_gh_api_error_is_retryable() {
        // FakeGitHub returns Err for repos that weren't pre-programmed
        // (which lets us simulate API failure deterministically).
        let (handler, _repo_store, _install_store, _gh) = make_repos_handler().await;
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
        let (handler, repo_store, install_store, gh) = make_repos_handler().await;
        repo_store.seed_supported_root(10, "stacks-network", "stacks-core", true);
        gh.set_repo_canonical("stacks-network", "stacks-core", 10);
        let webhook = repos_webhook(
            "added",
            repos_event_payload("added", 100, &[(10, "stacks-network/stacks-core")], &[]),
        );

        handler.handle(&webhook).await;
        let first_granted_at = install_store
            .membership(100, 10)
            .unwrap()
            .granted_at;

        handler.handle(&webhook).await;
        let second_granted_at = install_store
            .membership(100, 10)
            .unwrap()
            .granted_at;
        assert_eq!(first_granted_at, second_granted_at, "re-delivery must NOT change granted_at");
    }

    #[tokio::test]
    async fn repos_removed_revokes_active_memberships() {
        let (handler, repo_store, install_store, gh) = make_repos_handler().await;
        repo_store.seed_supported_root(10, "stacks-network", "stacks-core", true);
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
            .unwrap();
        assert!(m.revoked_at.is_some(), "revoked membership must have revoked_at set");
    }

    #[tokio::test]
    async fn repos_removed_for_unknown_membership_is_ignored_action() {
        // GitHub backfill / out-of-order delivery: a `removed` for a
        // repo we never tracked is a no-op.
        let (handler, _r, _i, _gh) = make_repos_handler().await;
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
        let (handler, _r, _i, _gh) = make_repos_handler().await;
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
        let (handler, _r, _i, _gh) = make_repos_handler().await;
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
        let (handler, repo_store, install_store, gh) = make_repos_handler().await;
        repo_store.seed_supported_root(10, "stacks-network", "stacks-core", true);
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
                .is_some()
        );
        assert!(
            install_store
                .membership(100, 99)
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
        let (handler, repo_store, install_store, gh) = make_repos_handler().await;
        repo_store.seed_supported_root(10, "stacks-network", "stacks-core", true);
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
        let (handler, repo_store, install_store, gh) = make_repos_handler().await;
        repo_store.seed_supported_root(10, "stacks-network", "stacks-core", true);
        // Mismatch: payload says 10, GH says 99.
        gh.set_repo_canonical("stacks-network", "stacks-core", 99);
        // A second repo in the same batch with consistent ids.
        repo_store.seed_supported_root(20, "stacks-network", "other", true);
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
                .is_none()
        );
        assert!(
            install_store
                .membership(100, 99)
                .is_none()
        );
        // The consistent repo got its membership.
        assert!(
            install_store
                .membership(100, 20)
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
        let (handler, repo_store, _install_store, gh) = make_repos_handler().await;
        repo_store.seed_supported_root(10, "stacks-network", "stacks-core", true);
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

    #[tokio::test]
    async fn add_or_restore_membership_returns_none_for_soft_deleted_install() {
        // Direct store-level test (in-memory) for the M1 fix invariant.
        let store = sbgh_core::db::InMemoryInstallationStore::new();
        store.seed_allowed(42, "octo-org", GithubAccountType::Organization, true);
        store
            .upsert_installation(&NewInstallation {
                id: 100,
                github_account_id: 42,
                account_login: "octo-org".into(),
                account_type: GithubAccountType::Organization,
            })
            .await
            .unwrap();
        store
            .delete_installation(100)
            .await
            .unwrap();
        // Repo doesn't even need to exist — the guard short-circuits
        // before any FK is checked.
        let result = store
            .add_or_restore_membership(100, 10)
            .await
            .unwrap();
        assert!(
            result.is_none(),
            "add_or_restore must return None for soft-deleted install (M1 fix)"
        );
    }

    #[tokio::test]
    async fn add_or_restore_membership_returns_none_for_missing_install() {
        let store = sbgh_core::db::InMemoryInstallationStore::new();
        let result = store
            .add_or_restore_membership(999, 10)
            .await
            .unwrap();
        assert!(result.is_none(), "add_or_restore must return None when install doesn't exist");
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
