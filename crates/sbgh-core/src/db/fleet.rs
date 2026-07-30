//! Persistence port for the worker-fleet state machine.
//!
//! The daemon owns this port. Workers receive the same transport-neutral fleet
//! values through the generated protobuf boundary and never see a database
//! model or provider credential.

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use sbgh_fleet::{
    ArtifactDescriptor, AttemptIdentity, BlockValidationResult, DesiredState, ProgressRequest,
    RegisterSessionRequest, ReliableEventEnvelope, TaskPayload, TerminalOutcome, WorkOffer,
    WorkerCapability,
};
use uuid::Uuid;

use crate::Result;
use crate::models::{JobMetric, JobResult};

#[derive(Debug, Clone)]
pub struct WorkerRegistration {
    pub worker_id: Uuid,
    pub display_name: String,
    pub allowed_capabilities: Vec<WorkerCapability>,
    pub measurement_profile: Option<String>,
    pub enabled: bool,
    pub draining: bool,
}

#[derive(Debug, Clone)]
pub struct WorkerAuthorization {
    pub worker_id: Uuid,
    pub allowed_capabilities: Vec<WorkerCapability>,
    pub measurement_profile: Option<String>,
    pub draining: bool,
}

#[derive(Debug, Clone)]
pub struct WorkerRegistryEntry {
    pub worker_id: Uuid,
    pub display_name: String,
    pub enabled: bool,
    pub draining: bool,
    pub allowed_capabilities: Vec<String>,
    pub measurement_profile: Option<String>,
    pub worker_session_id: Option<Uuid>,
    pub session_status: Option<String>,
    pub software_version: Option<String>,
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    pub session_expires_at: Option<DateTime<Utc>>,
    pub resource_facts: Option<serde_json::Value>,
    pub attempt_id: Option<Uuid>,
    pub job_id: Option<Uuid>,
    pub trace_id: Option<Uuid>,
}

#[derive(Debug, Clone, Default)]
pub struct WorkerPolicyPatch {
    pub display_name: Option<String>,
    pub allowed_capabilities: Option<Vec<WorkerCapability>>,
    pub measurement_profile: Option<Option<String>>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerCertificateRecord {
    pub certificate_sha256: [u8; 32],
    pub created_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerRegistryMutation {
    Applied,
    Unchanged,
    NotFound,
    Busy,
    MissingCertificate,
    Conflict,
}

#[derive(Debug, Clone)]
pub struct AuthorizedSession {
    pub worker_id: Uuid,
    pub worker_session_id: Uuid,
    pub effective_capabilities: Vec<WorkerCapability>,
    pub measurement_profile: Option<String>,
    pub draining: bool,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct PreparedExecution {
    pub job_id: Uuid,
    pub commit: String,
    pub payload: TaskPayload,
    pub payload_hash: String,
}

#[derive(Debug, Clone)]
pub struct ResolvedSpecSource {
    pub spec_id: Uuid,
    pub commit: String,
}

#[derive(Debug, Clone)]
pub struct OfferedAssignment {
    pub offer: WorkOffer,
    pub context_repository: String,
    pub context_commit: String,
    pub installation_id: i64,
    pub github_repo_id: i64,
    pub payload: TaskPayload,
    pub vcpu_cpuset: Option<String>,
    pub measurement_profile: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AttemptHeartbeat {
    pub desired_state: DesiredState,
    pub lease_expires_at: DateTime<Utc>,
    pub highest_contiguous_reliable_seq: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventIngest {
    Inserted,
    Duplicate,
    Stale,
    Conflict,
}

#[derive(Debug, Clone)]
pub struct StoredAttemptEvent {
    pub attempt_id: Uuid,
    pub job_id: Uuid,
    pub reliable_seq: u64,
    pub payload: sbgh_fleet::ReliableEventPayload,
    pub payload_digest: String,
}

#[derive(Debug, Clone)]
pub struct StoredProgress {
    pub attempt_id: Uuid,
    pub job_id: Uuid,
    pub progress_seq: u64,
    pub update: sbgh_fleet::ProgressUpdate,
}

#[derive(Debug, Clone)]
pub enum ProjectedReportMutation {
    Phase(sbgh_fleet::ReliableEventPayload),
    Progress(sbgh_fleet::ProgressUpdate),
}

/// The latest externally projected state plus the number of version-advancing
/// mutations already applied. A restarted orchestrator uses this to seed
/// deterministic snapshot renderers without replaying old side effects.
#[derive(Debug, Clone)]
pub struct ReportProjectionSeed {
    pub mutation_count: u64,
    pub latest: ProjectedReportMutation,
}

#[derive(Debug, Clone)]
pub struct ArtifactGrantRecord {
    pub attempt_id: Uuid,
    pub object_key: String,
    pub logical_key: String,
    pub size: Option<u64>,
    pub sha256: Option<String>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct PendingArtifactPromotion {
    pub attempt_id: Uuid,
    pub artifacts: Vec<ArtifactDescriptor>,
}

#[derive(Debug, Clone)]
pub struct FleetCompletion {
    pub result: JobResult,
    pub metric: Option<JobMetric>,
    pub baseline_calibration_id: Option<i64>,
    pub event_detail: Option<serde_json::Value>,
    pub block_validation: Option<BlockValidationResult>,
    /// Promoted, orchestrator-visible artifact identities. Attempt-scoped
    /// staging keys remain confined to the fleet terminal saga.
    pub artifact_manifest: Vec<ArtifactDescriptor>,
}

#[derive(Debug, Clone)]
pub struct FleetFailure {
    pub result: Option<JobResult>,
    pub remark: String,
    pub event_detail: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub enum FleetTerminalWrite {
    Completed(Box<FleetCompletion>),
    Failed(FleetFailure),
    Cancelled { remark: String },
}

pub struct FleetTerminalSubmission<'a> {
    pub reliable_seq: u64,
    pub payload_digest: &'a str,
    pub outcome: &'a TerminalOutcome,
    pub artifacts: &'a [ArtifactDescriptor],
    pub write: &'a FleetTerminalWrite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalAcceptance {
    Accepted,
    Duplicate,
    Stale,
}

#[derive(Debug, Clone)]
pub struct CleanupObligation {
    pub id: i64,
    pub worker_id: Uuid,
    pub worker_session_id: Uuid,
    pub attempt_id: Uuid,
    pub job_id: Uuid,
    pub requeue_job: bool,
    pub reason: String,
}

#[derive(Debug, Clone)]
pub struct FleetSnapshot {
    pub registered_workers: u64,
    pub online_workers: u64,
    pub draining_workers: u64,
    pub active_attempts: u64,
    pub pending_cleanup: u64,
    pub reliable_event_gap_attempts: u64,
    pub staged_artifact_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct SubmissionRecovery {
    pub prior_submission_id: Uuid,
    pub new_submission_id: Uuid,
    pub first_job_id: Uuid,
    pub execution_generation: u64,
}

#[async_trait]
pub trait WorkerRegistryStore: Send + Sync + 'static {
    async fn create_worker(
        &self,
        registration: &WorkerRegistration,
    ) -> Result<WorkerRegistryMutation>;

    async fn update_worker(
        &self,
        worker_id: Uuid,
        patch: &WorkerPolicyPatch,
    ) -> Result<WorkerRegistryMutation>;

    async fn authorize_certificate(
        &self,
        worker_id: Uuid,
        certificate_sha256: [u8; 32],
    ) -> Result<WorkerRegistryMutation>;

    async fn revoke_certificate(
        &self,
        worker_id: Uuid,
        certificate_sha256: [u8; 32],
    ) -> Result<WorkerRegistryMutation>;

    /// Immediately withdraw authorization and expire this worker's current
    /// session/attempt leases. The normal expiry coordinator performs
    /// fencing, cleanup, and safe requeue.
    async fn emergency_disable_worker(&self, worker_id: Uuid) -> Result<WorkerRegistryMutation>;

    /// Immediately revoke one certificate and expire sessions authenticated
    /// by it. The normal expiry coordinator owns subsequent recovery.
    async fn emergency_revoke_certificate(
        &self,
        worker_id: Uuid,
        certificate_sha256: [u8; 32],
    ) -> Result<WorkerRegistryMutation>;

    async fn worker_certificates(&self, worker_id: Uuid) -> Result<Vec<WorkerCertificateRecord>>;

    async fn workers(&self, worker_id: Option<Uuid>) -> Result<Vec<WorkerRegistryEntry>>;

    async fn authorize_worker(
        &self,
        worker_id: Uuid,
        certificate_sha256: [u8; 32],
    ) -> Result<Option<WorkerAuthorization>>;

    async fn set_worker_draining(
        &self,
        worker_id: Uuid,
        draining: bool,
    ) -> Result<WorkerRegistryMutation>;
}

#[async_trait]
pub trait FleetStore: Send + Sync + 'static {
    async fn register_session(
        &self,
        certificate_worker_id: Uuid,
        certificate_sha256: [u8; 32],
        request: &RegisterSessionRequest,
        session_ttl: Duration,
    ) -> Result<AuthorizedSession>;

    async fn deregister_session(&self, worker_id: Uuid, worker_session_id: Uuid) -> Result<bool>;

    /// Freeze every source ref in a scheduling unit before its first offer.
    ///
    /// Lazy variants/runs then inherit enqueue-generation commits rather than
    /// resolving a mutable branch when they are materialized later.
    async fn freeze_submission_sources(
        &self,
        submission_id: Uuid,
        sources: &[ResolvedSpecSource],
    ) -> Result<bool>;

    async fn prepare_execution(&self, prepared: &PreparedExecution) -> Result<bool>;

    async fn poll_offer(
        &self,
        worker_id: Uuid,
        worker_session_id: Uuid,
        offer_ttl: Duration,
        lease_ttl: Duration,
    ) -> Result<Option<OfferedAssignment>>;

    async fn session_is_draining(&self, worker_id: Uuid, worker_session_id: Uuid) -> Result<bool>;

    async fn session_is_active(&self, worker_id: Uuid, worker_session_id: Uuid) -> Result<bool>;

    async fn offered_assignment(
        &self,
        worker_id: Uuid,
        identity: &AttemptIdentity,
    ) -> Result<Option<OfferedAssignment>>;

    /// Lookup immutable assignment context for an idempotent terminal retry.
    /// Unlike the live-operation lookup, this includes an already-terminal
    /// attempt so a lost response can finish external artifact promotion.
    async fn completion_assignment(
        &self,
        worker_id: Uuid,
        identity: &AttemptIdentity,
    ) -> Result<Option<OfferedAssignment>>;

    async fn accept_offer(
        &self,
        worker_id: Uuid,
        identity: &AttemptIdentity,
        lease_ttl: Duration,
    ) -> Result<bool>;

    async fn heartbeat_attempt(
        &self,
        worker_id: Uuid,
        identity: &AttemptIdentity,
        lease_ttl: Duration,
        reliable_buffer_len: Option<u32>,
    ) -> Result<Option<AttemptHeartbeat>>;

    async fn request_cancel(&self, job_id: Uuid) -> Result<bool>;

    async fn set_all_workers_draining(&self, draining: bool) -> Result<u64>;

    async fn request_cancel_all(&self) -> Result<u64>;

    /// Start a fresh comparison generation from the first spec/run.
    ///
    /// The prior submission remains immutable and auditable. This operation is
    /// deliberately explicit because moving a partial benchmark submission across
    /// measurement environments must never happen through normal retry.
    async fn recover_submission(
        &self,
        submission_id: Uuid,
        target_worker_id: Option<Uuid>,
        reason: &str,
    ) -> Result<SubmissionRecovery>;

    async fn ingest_reliable_event(
        &self,
        worker_id: Uuid,
        event: &ReliableEventEnvelope,
    ) -> Result<EventIngest>;

    async fn unprojected_events(&self, limit: u32) -> Result<Vec<StoredAttemptEvent>>;

    /// Recheck that a previously fetched mutation still belongs to an attempt
    /// allowed to affect external reporting.
    async fn attempt_projection_is_authoritative(&self, attempt_id: Uuid) -> Result<bool>;

    async fn mark_event_projected(&self, attempt_id: Uuid, reliable_seq: u64) -> Result<bool>;

    async fn ingest_progress(&self, worker_id: Uuid, progress: &ProgressRequest) -> Result<bool>;

    async fn unprojected_progress(&self, limit: u32) -> Result<Vec<StoredProgress>>;

    async fn mark_progress_projected(&self, attempt_id: Uuid, progress_seq: u64) -> Result<bool>;

    async fn report_projection_seed(
        &self,
        attempt_id: Uuid,
    ) -> Result<Option<ReportProjectionSeed>>;

    async fn attempt_terminal_outcome(&self, attempt_id: Uuid) -> Result<Option<TerminalOutcome>>;

    /// Return the immutable manifest of an already accepted terminal,
    /// including one whose external object promotion is still pending.
    async fn accepted_terminal_manifest(
        &self,
        attempt_id: Uuid,
    ) -> Result<Option<Vec<ArtifactDescriptor>>>;

    async fn record_artifact_grant(&self, grant: &ArtifactGrantRecord) -> Result<bool>;

    async fn verify_artifact(
        &self,
        identity: &AttemptIdentity,
        artifact: &ArtifactDescriptor,
    ) -> Result<bool>;

    async fn accept_terminal(
        &self,
        worker_id: Uuid,
        identity: &AttemptIdentity,
        submission: &FleetTerminalSubmission<'_>,
    ) -> Result<TerminalAcceptance>;

    async fn pending_artifact_promotions(
        &self,
        limit: u32,
    ) -> Result<Vec<PendingArtifactPromotion>>;

    async fn mark_artifacts_promoted(
        &self,
        attempt_id: Uuid,
        artifacts: &[ArtifactDescriptor],
    ) -> Result<bool>;

    async fn expire_stale_attempts(&self) -> Result<u64>;

    async fn cleanup_obligations(
        &self,
        worker_id: Uuid,
        worker_session_id: Uuid,
    ) -> Result<Vec<CleanupObligation>>;

    async fn complete_cleanup(
        &self,
        worker_id: Uuid,
        worker_session_id: Uuid,
        obligation_id: i64,
    ) -> Result<bool>;

    async fn staged_artifacts_due_for_reap(&self, older_than: DateTime<Utc>)
    -> Result<Vec<String>>;

    /// Record external deletion only after the object store confirms it.
    async fn mark_staged_artifact_reaped(&self, object_key: &str) -> Result<bool>;

    async fn fleet_snapshot(&self) -> Result<FleetSnapshot>;
}
