//! Audited worker scheduling and session-targeted process control.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sbgh_fleet::{PreflightReport, ProtocolContract};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use self::WorkerAdmissionBlocker as Blocker;
use crate::Result;

/// Independent operator actions; shutdown never rewrites cordon policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerLifecycleAction {
    /// Prevent new acceptance without interrupting accepted work.
    Cordon,
    /// Permit new work if other admission controls allow it.
    Uncordon,
    /// End the current session after work and cleanup have finished.
    ShutdownAfterIdle,
}

/// Retry-safe operator intent with an authenticated actor.
#[derive(Debug, Clone)]
pub struct WorkerLifecycleChange {
    /// Stable request identity across response loss.
    pub request_id: Uuid,
    /// Registry identity to administer.
    pub worker_id: Uuid,
    /// Scheduling or process action.
    pub action: WorkerLifecycleAction,
    /// Principal supplied by the authorization boundary.
    pub actor: String,
    /// Operator explanation.
    pub reason: String,
}

/// Immutable receipt; replay never retargets a shutdown at a newer session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerLifecycleReceipt {
    /// Administrative request identity.
    pub request_id: Uuid,
    /// Worker that was administered.
    pub worker_id: Uuid,
    /// Committed operator action.
    pub action: WorkerLifecycleAction,
    /// Registry lifecycle generation after this request.
    pub generation: i64,
    /// Session targeted by shutdown, otherwise absent.
    pub target_session_id: Option<Uuid>,
    /// Authenticated actor.
    pub actor: String,
    /// Recorded explanation.
    pub reason: String,
    /// Database receipt timestamp.
    pub recorded_at: DateTime<Utc>,
}

/// Administrative outcome without ambiguous boolean failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerLifecycleOutcome {
    /// Original committed receipt, including idempotent replay.
    Applied(WorkerLifecycleReceipt),
    /// No such worker exists.
    NotFound,
    /// Request UUID was already used with different input.
    Conflict,
    /// Shutdown requires a live session negotiating its explicit semantics.
    UnsupportedSession,
}

/// Idle worker directive derived from policy and its immutable contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdleWorkerControl {
    /// Continue bounded polling, including while resident-cordoned.
    Poll,
    /// Preserve the previous contract's drain-and-exit behavior.
    LegacyDrain,
    /// Cleanly deregister and exit this session only.
    ShutdownAfterIdle,
}

/// One database snapshot for targeted maintenance, not proof of host process exit.
#[derive(Debug, Clone)]
pub struct WorkerMaintenanceSnapshot {
    /// Selected registry identity.
    pub worker_id: Uuid,
    /// Registry admission permission, independent of cordon and liveness.
    pub enabled: bool,
    /// Global admission policy observed with the worker's state.
    pub scheduler_paused: bool,
    /// Whether the latest session's bound key remains authorized.
    pub identity_authorized: bool,
    /// Latest contract is supported and meets the current compatibility floor.
    pub protocol_compatible: bool,
    /// Intersection of registry, session and negotiated task support.
    pub effective_capabilities: Vec<String>,
    /// Worker measurement policy; job-specific placement must still match.
    pub measurement_profile: Option<String>,
    /// Generation invalidating a wait after a competing policy change.
    pub generation: i64,
    /// Persistent worker scheduling restriction.
    pub cordoned: bool,
    /// Most recently registered session, including an ended session.
    pub session_id: Option<Uuid>,
    /// Latest session's observed state.
    pub session_status: Option<String>,
    /// Session remains unended and inside its database TTL.
    pub session_live: bool,
    /// Latest session's immutable negotiated semantics.
    pub protocol: Option<ProtocolContract>,
    /// Immutable successful startup checks reported by the latest session.
    pub preflight: Option<PreflightReport>,
    /// Explicit shutdown target; a successor is never implicitly targeted.
    pub shutdown_session_id: Option<Uuid>,
    /// Outstanding offers and authoritative execution attempts for this worker.
    pub active_attempts: i64,
    /// Outstanding physical cleanup acknowledgements across all its sessions.
    pub pending_cleanup: i64,
}

/// Stable reasons a worker cannot currently receive a new offer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerAdmissionBlocker {
    /// Registry authorization is disabled.
    Disabled,
    /// Worker-specific admission is closed.
    Cordoned,
    /// Global admission is paused.
    SchedulerPaused,
    /// Session is absent, expired or ended.
    SessionInactive,
    /// Session has not reached a polling state.
    SessionNotIdle,
    /// Bound identity was revoked or cannot be established.
    IdentityUnauthorized,
    /// Negotiated contract is unsupported or below the floor.
    ProtocolIncompatible,
    /// No authorized task capability has negotiated support.
    NoEffectiveCapabilities,
    /// Current session is targeted for explicit exit.
    ShutdownRequested,
    /// Worker capacity is occupied by an offer or active attempt.
    ActiveAttempt,
    /// Physical cleanup acknowledgement remains outstanding.
    PendingCleanup,
}

impl WorkerAdmissionBlocker {
    /// Stable operator/API code, independent of a human explanation.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Cordoned => "cordoned",
            Self::SchedulerPaused => "scheduler_paused",
            Self::SessionInactive => "session_inactive",
            Self::SessionNotIdle => "session_not_idle",
            Self::IdentityUnauthorized => "identity_unauthorized",
            Self::ProtocolIncompatible => "protocol_incompatible",
            Self::NoEffectiveCapabilities => "no_effective_capabilities",
            Self::ShutdownRequested => "shutdown_requested",
            Self::ActiveAttempt => "active_attempt",
            Self::PendingCleanup => "pending_cleanup",
        }
    }
}

impl WorkerMaintenanceSnapshot {
    /// Evaluate worker-wide admission; this does not promise a matching queued job.
    pub fn admission_blockers(&self) -> Vec<WorkerAdmissionBlocker> {
        [
            (!self.enabled, Blocker::Disabled),
            (self.cordoned, Blocker::Cordoned),
            (self.scheduler_paused, Blocker::SchedulerPaused),
            (!self.session_live, Blocker::SessionInactive),
            (
                !matches!(self.session_status.as_deref(), Some("registered" | "idle" | "draining")),
                Blocker::SessionNotIdle,
            ),
            (!self.identity_authorized, Blocker::IdentityUnauthorized),
            (!self.protocol_compatible, Blocker::ProtocolIncompatible),
            (
                self.effective_capabilities
                    .is_empty(),
                Blocker::NoEffectiveCapabilities,
            ),
            (
                self.session_id.is_some() && self.session_id == self.shutdown_session_id,
                Blocker::ShutdownRequested,
            ),
            (self.active_attempts != 0, Blocker::ActiveAttempt),
            (self.pending_cleanup != 0, Blocker::PendingCleanup),
        ]
        .into_iter()
        .filter_map(|(blocked, reason)| blocked.then_some(reason))
        .collect()
    }
}

/// Durable administration independent of the worker's process lifetime.
#[async_trait]
pub trait WorkerLifecycleStore: Send + Sync {
    /// Read policy, latest session and maintenance debt in one database snapshot.
    async fn worker_maintenance_snapshot(
        &self,
        worker_id: Uuid,
    ) -> Result<Option<WorkerMaintenanceSnapshot>>;

    /// Serialize a lifecycle action against offer/accept and session replacement.
    async fn change_worker_lifecycle(
        &self,
        change: &WorkerLifecycleChange,
    ) -> Result<WorkerLifecycleOutcome>;
}
