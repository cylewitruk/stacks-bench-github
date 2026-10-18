//! Explicit, retry-safe fleet cancellation contracts.

use serde::{Deserialize, Serialize};

/// Required selection scope; newly arriving work is never included in a replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FleetAbortScope {
    /// Only executing work, including expired attempts awaiting cleanup.
    Active,
    /// Currently queued, claimed and executing work.
    AllPending,
}

/// Cancellation request pinned to a separately established scheduler pause.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetAbortRequest {
    /// UUID retained across transport retries.
    pub request_id: String,
    /// Explicit scope; no default destructive selection.
    pub scope: FleetAbortScope,
    /// Current paused scheduler generation.
    pub scheduler_generation: i64,
    /// Operator explanation; actor is supplied by the authenticated API.
    pub reason: String,
}

/// Durable cancellation selection; cleanup and reporting may still be pending.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FleetAbortReceiptView {
    /// Mutation UUID.
    pub request_id: String,
    /// Original cancellation scope.
    pub scope: FleetAbortScope,
    /// Scheduler pause generation used for this transaction.
    pub scheduler_generation: i64,
    /// Authenticated principal.
    pub actor: String,
    /// Original explanation.
    pub reason: String,
    /// RFC 3339 database receipt timestamp.
    pub recorded_at: String,
    /// Frozen job selection.
    pub selected_job_ids: Vec<String>,
    /// Jobs accepting cancellation, not necessarily finished teardown.
    pub accepted_job_ids: Vec<String>,
}
