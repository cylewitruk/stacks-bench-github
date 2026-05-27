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
            Self::EnqueuedJob | Self::ProcessedInstallation => WebhookStatus::Processed,
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
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct GithubInstallation {
    pub id: i64,
    pub github_account_id: i64,
    pub account_login: String,
    pub account_type: GithubAccountType,
    pub suspended_at: Option<DateTime<Utc>>,
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
