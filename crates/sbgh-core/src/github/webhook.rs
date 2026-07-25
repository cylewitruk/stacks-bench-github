//! GitHub webhook event types consumed by the daemon.

use serde::{Deserialize, Serialize};

// --- Event payloads (subset we care about) ---

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IssueCommentEvent {
    pub action: String,
    pub comment: Comment,
    pub issue: Issue,
    pub repository: Repository,
    pub sender: User,
    pub installation: Installation,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Comment {
    pub id: i64,
    pub body: String,
    pub user: User,
    pub author_association: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Issue {
    pub number: i64,
    #[serde(default)]
    pub pull_request: Option<PullRequestRef>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PullRequestRef {
    pub url: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Repository {
    pub full_name: String,
}

/// GitHub user/account reference shipped on event `sender` and PR `user`.
/// GitHub returns `"User"`, `"Organization"`, or `"Bot"` for `type`; the raw
/// string is translated at the classifier boundary so unexpected casing
/// produces a clear error instead of silent deserialization failure.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct User {
    pub id: i64,
    pub login: String,
    #[serde(rename = "type")]
    pub account_type: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Installation {
    pub id: i64,
}

/// Subset of the `installation` webhook payload consumed by the processor.
/// `installation.account` is the GitHub account that installed the App and is
/// checked against `allowed_installer`.
///
/// `repositories` is GitHub's "repos this install can access" list
/// included on `installation.created` (and absent on the other
/// actions; `#[serde(default)]` so suspend/unsuspend/deleted don't
/// fail to parse). The processor materializes these as initial memberships at
/// create time; without this, a fresh install would have no
/// `github_installation_repo` rows until a later
/// `installation_repositories.added` event happened.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InstallationEvent {
    pub action: String,
    pub installation: InstallationDetails,
    #[serde(default)]
    pub repositories: Vec<InstallationRepository>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InstallationDetails {
    pub id: i64,
    pub account: InstallationAccount,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InstallationAccount {
    pub id: i64,
    pub login: String,
    /// GitHub returns `"User"`, `"Organization"`, or `"Bot"`. Lowercased
    /// in the model layer via `GithubAccountType` — we keep the raw
    /// string here and translate at the classifier boundary.
    #[serde(rename = "type")]
    pub account_type: String,
}

/// `installation_repositories.{added,removed}` event payload. GitHub
/// includes one OR BOTH of `repositories_added` / `repositories_removed`
/// depending on action; the handler reads whichever is relevant. The
/// repo objects here are identity-only — we fetch full lineage from
/// `/repos/{owner}/{repo}` separately to capture parent/source.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InstallationRepositoriesEvent {
    pub action: String,
    pub installation: InstallationDetails,
    #[serde(default)]
    pub repositories_added: Vec<InstallationRepository>,
    #[serde(default)]
    pub repositories_removed: Vec<InstallationRepository>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InstallationRepository {
    pub id: i64,
    /// `"owner/name"` form. We split this at the handler boundary
    /// rather than asking GitHub to send separate owner/name fields
    /// (which it doesn't in this payload shape).
    pub full_name: String,
}

// ─── pull_request / push / create webhook payloads ────────────────────

/// `pull_request.{opened,reopened,synchronize,...}` event payload. The
/// processor uses the action, installation, and head/base identities for
/// policy evaluation and persists the PR refs and SHA.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PullRequestEvent {
    pub action: String,
    pub installation: Installation,
    pub repository: PullRequestRepo,
    pub pull_request: PullRequestBody,
    /// GitHub includes `changes` on `pull_request.edited`. Title-only edits
    /// must not re-run policy evaluation; only a changed base ref can alter the
    /// target identity and warrant re-evaluation.
    #[serde(default)]
    pub changes: Option<PullRequestChanges>,
}

/// The `changes` object on `pull_request.edited`. Only `base` is relevant to
/// policy evaluation; other edits are absorbed by the upsert path.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PullRequestChanges {
    pub base: Option<serde_json::Value>,
}

/// Subset of GitHub's top-level repository field for PR-related webhooks.
/// This includes `id`; the issue-comment payload uses the narrower
/// [`Repository`] shape because its processor path needs only `full_name`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PullRequestRepo {
    pub id: i64,
    pub full_name: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PullRequestBody {
    pub number: i64,
    pub head: PullRequestBranchRef,
    pub base: PullRequestBranchRef,
    /// PR author, upserted before the pull-request row so its author FK exists.
    pub user: User,
    /// PR title, persisted and refreshed on `pull_request.edited`.
    pub title: String,
}

/// PR head/base entry. `repo` is `Option` because GitHub may omit it
/// when a PR's branch was deleted from a fork (rare, but documented).
/// The handler treats a missing repo as a payload error.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PullRequestBranchRef {
    /// Branch name (GitHub field is `ref`; Rust keyword conflict so
    /// we rename via serde).
    #[serde(rename = "ref")]
    pub branch: String,
    pub sha: String,
    pub repo: Option<PullRequestRepo>,
}

/// `push` event payload. The processor reads the branch ref, repository and
/// installation identities, and the pushed head commit at enqueue time.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PushEvent {
    /// Full ref path, e.g. `"refs/heads/develop"`. The handler strips
    /// the `refs/heads/` prefix before comparing to a
    /// `TriggerMatchSpec::BranchPush.branch_name`.
    #[serde(rename = "ref")]
    pub ref_field: String,
    pub installation: Installation,
    pub repository: PullRequestRepo,
    /// The new head commit after the push. GitHub sends `null` for a
    /// branch deletion (and for a push that introduces no commits), so a
    /// missing `head_commit` means there is nothing to benchmark.
    /// `#[serde(default)]` keeps older fixtures
    /// (and non-branch refs the handler already skips) parsing.
    #[serde(default)]
    pub head_commit: Option<PushCommit>,
}

/// The `head_commit` object on a push: the commit the ref now points at. `id`
/// is the full SHA and `timestamp` is the authored time (RFC3339).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PushCommit {
    pub id: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// `create` event payload (fires on branch and tag creation). The processor
/// evaluates `trigger_kind = 'tag_created'` only for tag refs.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CreateEvent {
    /// Branch or tag name (NOT `refs/heads/...` prefixed — `create`
    /// payloads send the short name).
    #[serde(rename = "ref")]
    pub ref_field: String,
    /// `"tag"` or `"branch"`; only tags are actionable.
    pub ref_type: String,
    pub installation: Installation,
    pub repository: PullRequestRepo,
}
