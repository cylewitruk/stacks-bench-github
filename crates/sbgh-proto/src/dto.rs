use std::collections::BTreeSet;
use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerCapability {
    Benchmark,
    BuildOnly,
    BlockValidation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetIdentity {
    pub generation: String,
    pub network: String,
    pub format_version: String,
    pub covered_start: u64,
    pub covered_end: u64,
    pub manifest_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceFacts {
    pub logical_cpus: u32,
    pub memory_bytes: u64,
    pub storage_bytes: u64,
    pub dataset: Option<DatasetIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisterSessionRequest {
    pub protocol_version: u16,
    pub worker_id: Uuid,
    pub worker_session_id: Uuid,
    pub software_version: String,
    pub advertised_capabilities: BTreeSet<WorkerCapability>,
    pub resources: ResourceFacts,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisterSessionResponse {
    pub protocol_version: u16,
    pub heartbeat_interval_ms: u64,
    pub lease_ttl_ms: u64,
    pub server_time_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CleanupListRequest {
    pub protocol_version: u16,
    pub worker_session_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CleanupItem {
    pub id: i64,
    pub attempt_id: Uuid,
    pub job_id: Uuid,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CleanupCompleteRequest {
    pub protocol_version: u16,
    pub worker_session_id: Uuid,
    pub obligation_id: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeregisterSessionRequest {
    pub protocol_version: u16,
    pub worker_session_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PollRequest {
    pub protocol_version: u16,
    pub worker_session_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PollResponse {
    NoWork {
        retry_after_ms: u64,
    },
    /// The registry is draining this worker. It must stop polling, finish
    /// cleanup, and deregister.
    Drain,
    Offer {
        offer: WorkOffer,
    },
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LeaseToken(pub String);

impl fmt::Debug for LeaseToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LeaseToken([REDACTED])")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptIdentity {
    pub worker_session_id: Uuid,
    pub attempt_id: Uuid,
    pub fencing_generation: u64,
    pub lease_token: LeaseToken,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkOffer {
    pub identity: AttemptIdentity,
    pub job_id: Uuid,
    pub trace_id: Uuid,
    pub capability: WorkerCapability,
    pub payload_hash: String,
    pub offer_expires_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceptOfferRequest {
    pub protocol_version: u16,
    pub identity: AttemptIdentity,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RepositoryToken(pub String);

impl fmt::Debug for RepositoryToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RepositoryToken([REDACTED])")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssignmentContext {
    pub job_id: Uuid,
    pub repository: String,
    pub commit: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BenchmarkPayload {
    pub effective_args: Vec<String>,
    pub workload_key: Option<String>,
    pub sqlite_seed_key: Option<String>,
    pub shared_baseline_calibration: bool,
    pub baseline_calibration_id: Option<i64>,
    pub run_index: i32,
    pub requested_run_count: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidationEpoch {
    PreNakamoto,
    Nakamoto,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InclusiveRange {
    pub start: u64,
    pub end: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockValidationPayload {
    pub dataset: DatasetIdentity,
    pub epoch: ValidationEpoch,
    pub range: InclusiveRange,
    pub requested_shards: u32,
    pub max_concurrency: u32,
    pub timeout_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TaskPayload {
    Benchmark(BenchmarkPayload),
    BuildOnly,
    BlockValidation(BlockValidationPayload),
}

impl TaskPayload {
    pub fn capability(&self) -> WorkerCapability {
        match self {
            Self::Benchmark(_) => WorkerCapability::Benchmark,
            Self::BuildOnly => WorkerCapability::BuildOnly,
            Self::BlockValidation(_) => WorkerCapability::BlockValidation,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Assignment {
    pub identity: AttemptIdentity,
    pub trace_id: Uuid,
    pub context: AssignmentContext,
    pub payload: TaskPayload,
    pub payload_hash: String,
    pub vcpu_cpuset: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceptOfferResponse {
    pub assignment: Assignment,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepositoryCredentialRequest {
    pub protocol_version: u16,
    pub identity: AttemptIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepositoryCredentialResponse {
    pub token: RepositoryToken,
    pub expires_at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DesiredState {
    Continue,
    Cancel,
    Drain,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    pub protocol_version: u16,
    pub identity: AttemptIdentity,
    /// Current same-session reliable resend pressure. Telemetry only.
    pub reliable_buffer_len: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    pub desired_state: DesiredState,
    pub lease_expires_at_ms: i64,
    pub highest_contiguous_reliable_seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ReliableEventPayload {
    Phase { label: String, elapsed_ms: u64 },
    Terminal { outcome_digest: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReliableEventEnvelope {
    pub protocol_version: u16,
    pub identity: AttemptIdentity,
    pub trace_id: Uuid,
    pub reliable_seq: u64,
    pub payload: ReliableEventPayload,
    pub payload_digest: String,
    pub worker_timestamp_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReliableEventAck {
    pub highest_contiguous_reliable_seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgressUpdate {
    pub workflow_step: String,
    pub run_index: i32,
    pub requested_run_count: i32,
    pub phase: String,
    pub progress: u64,
    pub total: Option<u64>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgressRequest {
    pub protocol_version: u16,
    pub identity: AttemptIdentity,
    pub trace_id: Uuid,
    pub progress_seq: u64,
    pub update: ProgressUpdate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactOperation {
    Put,
    Get,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactGrantRequest {
    pub protocol_version: u16,
    pub identity: AttemptIdentity,
    pub operation: ArtifactOperation,
    pub key: String,
    pub size: Option<u64>,
    pub sha256: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeaderValue {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactGrantResponse {
    pub method: String,
    /// Orchestrator-derived attempt staging key. The worker must use this key
    /// in its terminal manifest rather than the requested logical name.
    pub key: String,
    pub url: String,
    pub headers: Vec<HeaderValue>,
    pub expires_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactDescriptor {
    pub key: String,
    pub logical_key: String,
    pub size: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvalidBlock {
    pub shard: u32,
    pub block: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockValidationResult {
    pub valid: bool,
    pub checked_blocks: u64,
    pub invalid_blocks: Vec<InvalidBlock>,
    pub dataset: DatasetIdentity,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TerminalOutcome {
    Completed { summary: serde_json::Value, block_validation: Option<BlockValidationResult> },
    Failed { error: String, summary: Option<serde_json::Value>, retryable: bool },
    Cancelled { reason: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompleteAttemptRequest {
    pub protocol_version: u16,
    pub identity: AttemptIdentity,
    pub trace_id: Uuid,
    pub terminal_reliable_seq: u64,
    pub terminal_payload_digest: String,
    pub outcome: TerminalOutcome,
    pub artifacts: Vec<ArtifactDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompleteAttemptResponse {
    pub accepted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiError {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}
