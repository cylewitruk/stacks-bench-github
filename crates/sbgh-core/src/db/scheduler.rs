//! Durable scheduler admission policy and audited maintenance transitions.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::Result;

/// Committed admission policy; a generation fences work prepared before a change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulerPolicy {
    /// Whether new preparation, offers, and acceptance are prohibited.
    pub paused: bool,
    /// Monotonically increasing policy revision.
    pub generation: i64,
    /// Authenticated principal responsible for the last state change.
    pub actor: String,
    /// Operator-provided explanation for that change.
    pub reason: String,
    /// Database timestamp of the last state change.
    pub changed_at: DateTime<Utc>,
}

/// Idempotent administrative request; reusing its ID with other facts is rejected.
#[derive(Debug, Clone)]
pub struct SchedulerChange {
    /// Stable request identity, retained across ambiguous transport failures.
    pub request_id: Uuid,
    /// Desired admission policy.
    pub paused: bool,
    /// Authenticated principal, supplied by the authorization boundary.
    pub actor: String,
    /// Human explanation, not an authenticated user identity.
    pub reason: String,
}

/// Result of applying or replaying an administrative request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchedulerChangeOutcome {
    /// Original result, including no-op requests and idempotent replays.
    Applied(SchedulerPolicy),
    /// The request ID was previously used with different input.
    Conflict,
}

/// One consistent snapshot for waiting on a paused fleet's durable work.
#[derive(Debug, Clone)]
pub struct SchedulerSnapshot {
    /// Current admission policy.
    pub policy: SchedulerPolicy,
    /// Offered or executing attempts, including cancellation requests.
    pub active_attempts: i64,
    /// Teardown acknowledgements still owed by workers.
    pub pending_cleanup: i64,
    /// Attempts with reliable events awaiting projection.
    pub pending_projection: i64,
    /// Active-attempt uploads or accepted-manifest promotion debt, including empty files.
    pub pending_artifacts: i64,
    /// Unreferenced staging retained for garbage collection; does not block maintenance.
    pub garbage_artifacts: i64,
}

/// Storage boundary for scheduler maintenance, independent of worker policy.
#[async_trait]
pub trait SchedulerStore: Send + Sync {
    /// Read durable policy without taking a maintenance lock.
    async fn scheduler_policy(&self) -> Result<SchedulerPolicy>;

    /// Read policy and durable work counters in one database snapshot.
    async fn scheduler_snapshot(&self) -> Result<SchedulerSnapshot>;

    /// Serialize a change with preparation, offer, and acceptance transactions.
    async fn change_scheduler(&self, change: &SchedulerChange) -> Result<SchedulerChangeOutcome>;
}
