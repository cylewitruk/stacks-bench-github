//! Operator-facing compatibility floor requests and receipts.

use serde::{Deserialize, Serialize};

/// Retry-safe floor change; authorization supplies the actor separately.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProtocolPolicyChangeRequest {
    /// UUID retained across uncertain responses.
    pub request_id: String,
    /// Requested minimum negotiated revision.
    pub minimum_revision: u16,
    /// Operator explanation.
    pub reason: String,
    /// Reference to completed rollback qualification, not automatic proof.
    pub rollback_evidence: String,
}

/// Durable floor policy or the original receipt for a retried change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolPolicyView {
    /// Minimum revision admitted for registration and new work.
    pub minimum_revision: u16,
    /// Monotonic revision of the policy.
    pub generation: i64,
    /// Authenticated administrative principal.
    pub actor: String,
    /// Explanation of the last actual change.
    pub reason: String,
    /// Operator's qualification reference.
    pub rollback_evidence: String,
    /// RFC 3339 database timestamp.
    pub changed_at: String,
}
