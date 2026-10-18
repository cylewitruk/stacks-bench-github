//! Explicit cancellation of a frozen workload while admission is paused.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::Result;

/// Required blast radius; neither scope includes work arriving after selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FleetAbortScope {
    /// Executing jobs, including those awaiting teardown after lease expiry.
    Active,
    /// All currently queued, claimed or executing jobs.
    AllPending,
}

/// Attributable abort intent pinned to an existing scheduler pause.
#[derive(Debug, Clone)]
pub struct FleetAbortRequest {
    /// Retry identity; reusing it never selects new work.
    pub request_id: Uuid,
    /// Explicit cancellation scope.
    pub scope: FleetAbortScope,
    /// Required current paused-policy generation.
    pub scheduler_generation: i64,
    /// Authenticated administrative principal.
    pub actor: String,
    /// Operator explanation retained in the receipt.
    pub reason: String,
}

/// Immutable selection and accepted requests, not proof of completed teardown.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FleetAbortReceipt {
    /// Original mutation identity.
    pub request_id: Uuid,
    /// Selected blast radius.
    pub scope: FleetAbortScope,
    /// Pause generation under which cancellation was requested.
    pub scheduler_generation: i64,
    /// Authenticated principal.
    pub actor: String,
    /// Original explanation.
    pub reason: String,
    /// Database receipt time.
    pub recorded_at: DateTime<Utc>,
    /// Frozen existing jobs selected by the request.
    pub selected_job_ids: Vec<Uuid>,
    /// Jobs accepting cancellation; terminal completion may win a concurrent race.
    pub accepted_job_ids: Vec<Uuid>,
}

/// Distinguish replay conflicts and missing maintenance authority from success.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FleetAbortOutcome {
    /// Committed receipt, including retries of the same request.
    Applied(FleetAbortReceipt),
    /// Request identity was reused with different input.
    Conflict,
    /// Admission is not paused at the explicitly required generation.
    MaintenanceRequired,
}

/// Durable abort transaction, independent of daemon process shutdown.
#[async_trait]
pub trait FleetAbortStore: Send + Sync {
    /// Atomically select jobs, request cancellation and persist an immutable receipt.
    async fn abort_fleet(&self, request: &FleetAbortRequest) -> Result<FleetAbortOutcome>;
}
