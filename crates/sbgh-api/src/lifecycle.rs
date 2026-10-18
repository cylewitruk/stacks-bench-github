//! Explicit worker scheduling and process-maintenance requests.

use serde::{Deserialize, Serialize};

/// Effective meaning of cordon for the latest recorded worker contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CordonSemantic {
    /// Negotiated workers remain connected while cordoned.
    Resident,
    /// Legacy workers exit and need a subsequent service start.
    ExitAfterIdle,
    /// No session contract has been recorded.
    Unknown,
}

/// Desired process policy, separate from liveness and the existing cordon flag.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerLifecycleView {
    /// Worker lifecycle policy generation.
    pub generation: i64,
    /// Cordon semantics of the latest session, including offline sessions.
    pub cordon_semantic: CordonSemantic,
    /// Session explicitly targeted for shutdown; does not target its replacement.
    pub shutdown_session_id: Option<String>,
}

/// Coherent worker policy, session and cleanup observation for maintenance waits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerMaintenanceView {
    /// Selected registry identity.
    pub worker_id: String,
    /// Registry admission permission, not proof that a particular task can run.
    pub enabled: bool,
    /// Effective worker-wide admission; absent on older maintenance APIs.
    #[serde(default)]
    pub admission: Option<WorkerAdmissionView>,
    /// Worker-reported startup evidence; absent on older workers or APIs.
    #[serde(default)]
    pub preflight: Option<WorkerPreflightView>,
    /// Lifecycle policy generation; pin it while waiting.
    pub generation: i64,
    /// Persistent worker-specific scheduling restriction.
    pub cordoned: bool,
    /// Most recent session, including ended sessions.
    pub session_id: Option<String>,
    /// Observed session state; expiry alone is not a clean deregistration.
    pub session_status: Option<String>,
    /// Session is unended and inside its database TTL.
    pub session_live: bool,
    /// Effective behavior of a cordon for this session.
    pub cordon_semantic: CordonSemantic,
    /// Negotiated revision, absent until first registration.
    pub protocol_revision: Option<u16>,
    /// Negotiated optional semantics.
    pub protocol_features: Vec<String>,
    /// Exact session targeted by the last explicit shutdown request.
    pub shutdown_session_id: Option<String>,
    /// Unaccepted offers and active attempts for the worker.
    pub active_attempts: i64,
    /// Pending physical cleanup acknowledgements across all worker sessions.
    pub pending_cleanup: i64,
}

/// Observed ability to receive an offer, not a reservation or job-placement promise.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerAdmissionView {
    /// No worker-wide policy, authorization, capacity or cleanup blockers exist.
    pub schedulable: bool,
    /// Stable reason codes explaining why new work cannot currently be offered.
    pub blockers: Vec<String>,
    /// Authorized, advertised and negotiated task capability intersection.
    pub effective_capabilities: Vec<String>,
    /// Measurement policy; a requested job's profile and placement still apply.
    pub measurement_profile: Option<String>,
}

/// Successful startup checks reported by the worker, not remote attestation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerPreflightView {
    /// Version of the check suite and configuration fingerprint encoding.
    pub revision: u32,
    /// Completion time in worker-clock Unix milliseconds.
    pub completed_at_ms: i64,
    /// SHA-256 of the running worker executable.
    pub executable_sha256: String,
    /// SHA-256 of the loaded, default-expanded configuration encoding.
    pub configuration_sha256: String,
}

impl WorkerMaintenanceView {
    /// No active execution or pending cleanup; does not imply the process stopped.
    pub fn quiescent(&self) -> bool {
        self.active_attempts == 0 && self.pending_cleanup == 0
    }
}

/// Independent maintenance intent, never inferred from daemon shutdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerLifecycleAction {
    /// Prevent new work while preserving active execution.
    Cordon,
    /// Remove the worker-specific scheduling restriction.
    Uncordon,
    /// Stop only the current compatible session after cleanup.
    ShutdownAfterIdle,
}

/// Idempotent administrative mutation; the server supplies the actor.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerLifecycleRequest {
    /// UUID retained across ambiguous responses.
    pub request_id: String,
    /// Explicit scheduling or process action.
    pub action: WorkerLifecycleAction,
    /// Operator explanation recorded in the audit.
    pub reason: String,
}

/// Original durable receipt, not a live snapshot of worker state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerLifecycleReceiptView {
    /// Request identity used for retries.
    pub request_id: String,
    /// Affected worker identity.
    pub worker_id: String,
    /// Committed action.
    pub action: WorkerLifecycleAction,
    /// Worker policy generation after the request.
    pub generation: i64,
    /// Exact session targeted for shutdown; never retargeted on replay.
    pub target_session_id: Option<String>,
    /// Authenticated administrative principal.
    pub actor: String,
    /// Recorded operator explanation.
    pub reason: String,
    /// RFC 3339 database timestamp.
    pub recorded_at: String,
}
