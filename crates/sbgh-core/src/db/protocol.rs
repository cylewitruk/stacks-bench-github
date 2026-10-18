//! Audited compatibility policy for bounded rolling upgrades.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::Result;

/// Lowest revision admitted by the daemon.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolPolicy {
    /// Minimum accepted negotiated revision.
    pub minimum_revision: u16,
    /// Monotonic policy generation, including explicit rollback changes.
    pub generation: i64,
    /// Authenticated administrative principal.
    pub actor: String,
    /// Explanation supplied by the operator.
    pub reason: String,
    /// Reference to the operator's rollback qualification evidence.
    pub rollback_evidence: String,
    /// Database time of the last change.
    pub changed_at: DateTime<Utc>,
}

/// Idempotent administrative request to change the compatibility floor.
#[derive(Debug, Clone)]
pub struct ProtocolPolicyChange {
    /// Stable identifier retained across retries.
    pub request_id: Uuid,
    /// Desired minimum supported revision.
    pub minimum_revision: u16,
    /// Authenticated principal, supplied by the API boundary.
    pub actor: String,
    /// Human explanation.
    pub reason: String,
    /// Evidence reference; a declaration, not automatic proof of rollback safety.
    pub rollback_evidence: String,
}

/// Administrative result without changing worker execution authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolPolicyChangeOutcome {
    /// Original receipt, including idempotent replay and no-op changes.
    Applied(ProtocolPolicy),
    /// Request identifier reused with different facts.
    Conflict,
    /// A paused, quiescent scheduler is required for a floor change.
    MaintenanceRequired,
    /// Enabled workers without a live session meeting the requested floor.
    WorkersNotConverged(Vec<Uuid>),
}

/// Persistent protocol administration, separate from worker authorization.
#[async_trait]
pub trait ProtocolPolicyStore: Send + Sync {
    /// Read current policy for readiness and operator inspection.
    async fn protocol_policy(&self) -> Result<ProtocolPolicy>;

    /// Serialize an audited change with registration and admission.
    async fn change_protocol_policy(
        &self,
        change: &ProtocolPolicyChange,
    ) -> Result<ProtocolPolicyChangeOutcome>;
}
