use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::types::Json;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "job_status", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
}

/// Lifecycle dimension for `github_webhook` rows. Mirrors the
/// `github_webhook_status` DB enum. Distinct from `WebhookOutcome`:
/// status is the queue/processing state; outcome is the specific
/// terminal decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "github_webhook_status", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum WebhookStatus {
    Received,
    Processing,
    Processed,
    Ignored,
    Denied,
    RetryableError,
    Failed,
}

/// Specific processor decision attached to a terminal webhook row.
/// Mirrors the `github_webhook_outcome` DB enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "github_webhook_outcome", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum WebhookOutcome {
    EnqueuedJob,
    /// Pre-slice-6 design checkpoint: Phase 1 shadow accept. The new
    /// pipeline's policy/trigger evaluation said "this would enqueue a
    /// job", but slice 9 hasn't landed yet so no `job` row is created
    /// and the legacy handler→jobs path is what actually runs the
    /// bench. Slice 9 flips the four accept paths
    /// (`IssueCommentHandler` /benchmark, `PullRequestHandler`,
    /// `PushHandler`, `CreateHandler`) to emit `EnqueuedJob` instead.
    /// Same status bucket as `EnqueuedJob` / `ProcessedInstallation`
    /// (= `Processed`) — a shadow accept is a successful terminal
    /// outcome, not an ignored/denied one.
    WouldEnqueueJob,
    /// Slice 3+: terminal "we materialised installation state" — the
    /// processor created/updated a `github_installation` row in response
    /// to an `installation.*` event. Distinct from `IgnoredAction` so
    /// ops queries can separate "no-op event" from "install state changed".
    ProcessedInstallation,
    IgnoredAction,
    IgnoredNoCommand,
    IgnoredUnknownInstallation,
    IgnoredUnsupportedLineage,
    DeniedInstallAllowlist,
    DeniedTargetPolicy,
    DeniedSourcePolicy,
    DeniedUnauthorized,
    Error,
}

impl WebhookOutcome {
    /// Terminal status that pairs with this outcome.
    pub fn terminal_status(self) -> WebhookStatus {
        match self {
            Self::EnqueuedJob | Self::WouldEnqueueJob | Self::ProcessedInstallation => {
                WebhookStatus::Processed
            }
            Self::IgnoredAction
            | Self::IgnoredNoCommand
            | Self::IgnoredUnknownInstallation
            | Self::IgnoredUnsupportedLineage => WebhookStatus::Ignored,
            Self::DeniedInstallAllowlist
            | Self::DeniedTargetPolicy
            | Self::DeniedSourcePolicy
            | Self::DeniedUnauthorized => WebhookStatus::Denied,
            Self::Error => WebhookStatus::Failed,
        }
    }
}

/// GH account kind, mirrors the `github_account_type` DB enum. Bot is
/// included for completeness even though the App is unlikely to be
/// installed by a bot in practice — present so we don't panic if the
/// API ever hands us one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "github_account_type", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum GithubAccountType {
    User,
    Organization,
    Bot,
}

/// Operator-curated allowlist row. PK is the GitHub-assigned numeric
/// account id — stable across renames and case.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct AllowedInstaller {
    pub github_account_id: i64,
    pub account_login: String,
    pub account_type: GithubAccountType,
    pub is_enabled: bool,
    pub note: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// A GitHub App installation we've accepted. `id` is GitHub's numeric
/// installation id, used as the FK target by membership / policy /
/// inbox tables. FK to `allowed_installer.github_account_id` enforces
/// "installation must be backed by a current allowlist entry".
///
/// `deleted_at` is the slice 4 soft-delete marker for `installation.deleted`
/// events — the row stays around (so historical job/membership FKs
/// remain valid) but `deleted_at IS NOT NULL` means it's retired. The
/// "currently active" predicate is `deleted_at IS NULL AND suspended_at IS
/// NULL`.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct GithubInstallation {
    pub id: i64,
    pub github_account_id: i64,
    pub account_login: String,
    pub account_type: GithubAccountType,
    pub suspended_at: Option<DateTime<Utc>>,
    pub deleted_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// GitHub repository identity + fork lineage. `id` is GitHub's numeric
/// repo id (stable across renames and transfers).
///
/// Lineage columns are populated from `/repos/{owner}/{repo}` on first
/// encounter. Convention (no SQL constraint enforces it):
///   - canonical/non-fork: `is_fork=false`, parent + fork_root NULL
///   - fork (any depth):   `is_fork=true`,  fork_root IS NOT NULL
///
/// `parent_github_repo_id` is the IMMEDIATE parent;
/// `fork_root_github_repo_id` is the ultimate non-fork ancestor
/// (GitHub's `source` in the REST response). Both are nullable
/// because slices 4-6 may insert a repo for identity-only purposes
/// (PR target/source) before the lineage walk has run.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct GithubRepo {
    pub id: i64,
    pub owner: String,
    pub name: String,
    pub default_branch: Option<String>,
    pub is_fork: Option<bool>,
    pub parent_github_repo_id: Option<i64>,
    pub fork_root_github_repo_id: Option<i64>,
    pub lineage_checked_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Operator-curated canonical-root row. A repo is in-scope iff its id OR
/// its `fork_root_github_repo_id` matches an enabled row here.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct SupportedRepoRoot {
    pub github_repo_id: i64,
    pub is_enabled: bool,
    pub note: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Per-installation repo membership. `revoked_at IS NULL` means active.
/// Composite PK doubles as the FK anchor for slice 5+ policy + slice 8+
/// job tables that need to prove the (install, repo) pair was ever known.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct GithubInstallationRepo {
    pub github_installation_id: i64,
    pub github_repo_id: i64,
    pub granted_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
}

/// Slice 5: triggers the processor watches for in `branch_push` /
/// `tag_created` event handling. Each `trigger_policy` row carries one
/// of these in `match_spec`. App-layer validated (the DB stores it as
/// arbitrary JSONB; this enum is the contract).
///
/// `branch_name` is an EXACT match against the inbound ref; `tag_pattern`
/// is a Rust regex pattern (matched with `regex::Regex::is_match` at
/// evaluation time). Globs would be more operator-friendly but regex
/// composes better with the eventual job-rerun-on-historical-tag flows
/// slice 9+ may introduce.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TriggerMatchSpec {
    BranchPush { branch_name: String },
    TagCreated { tag_pattern: String },
}

/// `trigger_kind` enum mirror. Slice 5 only USES `BranchPush` and
/// `TagCreated`; `PrComment` is the implicit /benchmark path
/// (no trigger_policy row needed). `Scheduled` and `Manual` are
/// reserved for post-slice-9 work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "trigger_kind", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum TriggerKind {
    PrComment,
    BranchPush,
    TagCreated,
    Scheduled,
    Manual,
}

/// Per-installation target-repo opt-in (the operator says "this install
/// will benchmark PRs against this repo"). Composite PK + FK to
/// `github_installation_repo` enforces "the (install, repo) pair must
/// exist as a membership row" — but currently-active access is a
/// separate app-level join on `revoked_at IS NULL`.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct TargetRepoPolicy {
    pub github_installation_id: i64,
    pub github_repo_id: i64,
    pub is_enabled: bool,
    pub note: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Per-installation source-repo trust (the operator says "this install
/// trusts this repo as the source side of a PR — its code may execute
/// in our benchmark VM"). Unlike `TargetRepoPolicy`, no membership FK
/// — sources can be arbitrary forks the install doesn't own.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct SourceRepoPolicy {
    pub github_installation_id: i64,
    pub github_repo_id: i64,
    pub is_enabled: bool,
    pub note: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Per-installation auto-trigger subscription. Multiple rows per
/// (install, repo) — one per `trigger_kind` + `match_spec` combo.
/// `bench_args` (optional) is forwarded to the eventual job as
/// CLI args once slice 9 starts creating jobs.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct TriggerPolicy {
    pub id: i64,
    pub github_installation_id: i64,
    pub github_repo_id: i64,
    pub trigger_kind: TriggerKind,
    /// Stored as JSONB; deserialise into `TriggerMatchSpec` for typed
    /// matching at evaluation time.
    pub match_spec: sqlx::types::Json<serde_json::Value>,
    pub bench_args: Option<String>,
    pub is_enabled: bool,
    pub note: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Job {
    pub id: Uuid,
    pub status: JobStatus,
    pub repository: String,
    pub pr_number: i64,
    pub head_sha: String,
    pub requested_by: String,
    pub command: String,
    pub args: Json<serde_json::Value>,
    pub installation_id: i64,
    pub comment_id: Option<i64>,
    pub github_delivery_id: Option<String>,
    pub queued_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub result: Option<Json<serde_json::Value>>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewJob {
    pub repository: String,
    pub pr_number: i64,
    pub head_sha: String,
    pub requested_by: String,
    pub command: String,
    pub args: serde_json::Value,
    pub installation_id: i64,
    /// Value of the `X-GitHub-Delivery` header. Used as an idempotency key so
    /// retried webhook deliveries don't enqueue duplicate jobs. `None` is
    /// only legal for synthetic events (tests, manual replays).
    pub github_delivery_id: Option<String>,
}
