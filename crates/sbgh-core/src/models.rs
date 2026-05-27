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
            Self::EnqueuedJob => WebhookStatus::Processed,
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
