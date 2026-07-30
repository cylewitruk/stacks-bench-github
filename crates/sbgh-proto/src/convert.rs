use std::collections::BTreeSet;

use sbgh_fleet as domain;
use uuid::Uuid;

use crate::fleet::v1 as wire;

/// Conversion implemented by generated transport messages.
pub trait Wire: Sized {
    type Domain;

    fn into_domain(self) -> Result<Self::Domain, domain::ProtocolError>;
    fn from_domain(value: Self::Domain) -> Self;
}

fn invalid(field: &'static str, reason: impl Into<String>) -> domain::ProtocolError {
    domain::ProtocolError::Invalid { field, reason: reason.into() }
}

fn required<T>(field: &'static str, value: Option<T>) -> Result<T, domain::ProtocolError> {
    value.ok_or_else(|| invalid(field, "required protobuf field is absent"))
}

fn uuid(field: &'static str, value: &str) -> Result<Uuid, domain::ProtocolError> {
    Uuid::parse_str(value).map_err(|_| invalid(field, "must be a canonical UUID"))
}

fn protocol_version(value: u32) -> Result<u16, domain::ProtocolError> {
    u16::try_from(value).map_err(|_| invalid("protocol_version", "exceeds the supported range"))
}

fn capability(value: i32) -> Result<domain::WorkerCapability, domain::ProtocolError> {
    match wire::WorkerCapability::try_from(value).ok() {
        Some(wire::WorkerCapability::Benchmark) => Ok(domain::WorkerCapability::Benchmark),
        Some(wire::WorkerCapability::BuildOnly) => Ok(domain::WorkerCapability::BuildOnly),
        Some(wire::WorkerCapability::BlockValidation) => {
            Ok(domain::WorkerCapability::BlockValidation)
        }
        Some(wire::WorkerCapability::Unspecified) | None => {
            Err(invalid("worker_capability", "must be a recognized non-UNSPECIFIED value"))
        }
    }
}

fn capability_wire(value: domain::WorkerCapability) -> i32 {
    match value {
        domain::WorkerCapability::Benchmark => wire::WorkerCapability::Benchmark as i32,
        domain::WorkerCapability::BuildOnly => wire::WorkerCapability::BuildOnly as i32,
        domain::WorkerCapability::BlockValidation => wire::WorkerCapability::BlockValidation as i32,
    }
}

fn epoch(value: i32) -> Result<domain::ValidationEpoch, domain::ProtocolError> {
    match wire::ValidationEpoch::try_from(value).ok() {
        Some(wire::ValidationEpoch::PreNakamoto) => Ok(domain::ValidationEpoch::PreNakamoto),
        Some(wire::ValidationEpoch::Nakamoto) => Ok(domain::ValidationEpoch::Nakamoto),
        Some(wire::ValidationEpoch::Unspecified) | None => {
            Err(invalid("validation_epoch", "must be a recognized non-UNSPECIFIED value"))
        }
    }
}

fn epoch_wire(value: domain::ValidationEpoch) -> i32 {
    match value {
        domain::ValidationEpoch::PreNakamoto => wire::ValidationEpoch::PreNakamoto as i32,
        domain::ValidationEpoch::Nakamoto => wire::ValidationEpoch::Nakamoto as i32,
    }
}

fn desired_state(value: i32) -> Result<domain::DesiredState, domain::ProtocolError> {
    match wire::DesiredState::try_from(value).ok() {
        Some(wire::DesiredState::Continue) => Ok(domain::DesiredState::Continue),
        Some(wire::DesiredState::Cancel) => Ok(domain::DesiredState::Cancel),
        Some(wire::DesiredState::Drain) => Ok(domain::DesiredState::Drain),
        Some(wire::DesiredState::Unspecified) | None => {
            Err(invalid("desired_state", "must be a recognized non-UNSPECIFIED value"))
        }
    }
}

fn desired_state_wire(value: domain::DesiredState) -> i32 {
    match value {
        domain::DesiredState::Continue => wire::DesiredState::Continue as i32,
        domain::DesiredState::Cancel => wire::DesiredState::Cancel as i32,
        domain::DesiredState::Drain => wire::DesiredState::Drain as i32,
    }
}

fn artifact_operation(value: i32) -> Result<domain::ArtifactOperation, domain::ProtocolError> {
    match wire::ArtifactOperation::try_from(value).ok() {
        Some(wire::ArtifactOperation::Put) => Ok(domain::ArtifactOperation::Put),
        Some(wire::ArtifactOperation::Get) => Ok(domain::ArtifactOperation::Get),
        Some(wire::ArtifactOperation::Unspecified) | None => {
            Err(invalid("artifact_operation", "must be a recognized non-UNSPECIFIED value"))
        }
    }
}

fn artifact_operation_wire(value: domain::ArtifactOperation) -> i32 {
    match value {
        domain::ArtifactOperation::Put => wire::ArtifactOperation::Put as i32,
        domain::ArtifactOperation::Get => wire::ArtifactOperation::Get as i32,
    }
}

fn identity(
    value: wire::AttemptIdentity,
) -> Result<domain::AttemptIdentity, domain::ProtocolError> {
    Ok(domain::AttemptIdentity {
        worker_session_id: uuid("worker_session_id", &value.worker_session_id)?,
        attempt_id: uuid("attempt_id", &value.attempt_id)?,
        fencing_generation: value.fencing_generation,
        lease_token: domain::LeaseToken(value.lease_token),
    })
}

fn identity_wire(value: domain::AttemptIdentity) -> wire::AttemptIdentity {
    wire::AttemptIdentity {
        worker_session_id: value
            .worker_session_id
            .to_string(),
        attempt_id: value.attempt_id.to_string(),
        fencing_generation: value.fencing_generation,
        lease_token: value.lease_token.0,
    }
}

fn range(value: wire::InclusiveRange) -> domain::InclusiveRange {
    domain::InclusiveRange {
        start: value.start,
        end: value.end,
    }
}

fn range_wire(value: domain::InclusiveRange) -> wire::InclusiveRange {
    wire::InclusiveRange {
        start: value.start,
        end: value.end,
    }
}

fn requirements(
    value: wire::OfferRequirements,
) -> Result<domain::OfferRequirements, domain::ProtocolError> {
    match required("offer_requirements.kind", value.kind)? {
        wire::offer_requirements::Kind::Benchmark(_) => Ok(domain::OfferRequirements::Benchmark),
        wire::offer_requirements::Kind::BuildOnly(_) => Ok(domain::OfferRequirements::BuildOnly),
        wire::offer_requirements::Kind::BlockValidation(value) => {
            Ok(domain::OfferRequirements::BlockValidation {
                requested_shards: value.requested_shards,
                max_concurrency: value.max_concurrency,
            })
        }
    }
}

fn requirements_wire(value: domain::OfferRequirements) -> wire::OfferRequirements {
    let kind = match value {
        domain::OfferRequirements::Benchmark => {
            wire::offer_requirements::Kind::Benchmark(wire::Empty {})
        }
        domain::OfferRequirements::BuildOnly => {
            wire::offer_requirements::Kind::BuildOnly(wire::Empty {})
        }
        domain::OfferRequirements::BlockValidation {
            requested_shards,
            max_concurrency,
        } => wire::offer_requirements::Kind::BlockValidation(wire::BlockValidationRequirements {
            requested_shards,
            max_concurrency,
        }),
    };
    wire::OfferRequirements { kind: Some(kind) }
}

fn offer(value: wire::WorkOffer) -> Result<domain::WorkOffer, domain::ProtocolError> {
    Ok(domain::WorkOffer {
        identity: identity(required("work_offer.identity", value.identity)?)?,
        job_id: uuid("job_id", &value.job_id)?,
        trace_id: uuid("trace_id", &value.trace_id)?,
        capability: capability(value.capability)?,
        requirements: requirements(required("work_offer.requirements", value.requirements)?)?,
        payload_hash: value.payload_hash,
        offer_expires_at_ms: value.offer_expires_at_ms,
    })
}

fn offer_wire(value: domain::WorkOffer) -> wire::WorkOffer {
    wire::WorkOffer {
        identity: Some(identity_wire(value.identity)),
        job_id: value.job_id.to_string(),
        trace_id: value.trace_id.to_string(),
        capability: capability_wire(value.capability),
        requirements: Some(requirements_wire(value.requirements)),
        payload_hash: value.payload_hash,
        offer_expires_at_ms: value.offer_expires_at_ms,
    }
}

fn benchmark_payload(value: wire::BenchmarkPayload) -> domain::BenchmarkPayload {
    domain::BenchmarkPayload {
        effective_args: value.effective_args,
        workload_key: value.workload_key,
        sqlite_seed_key: value.sqlite_seed_key,
        shared_baseline_calibration: value.shared_baseline_calibration,
        baseline_calibration_id: value.baseline_calibration_id,
        run_index: value.run_index,
        requested_run_count: value.requested_run_count,
    }
}

fn benchmark_payload_wire(value: domain::BenchmarkPayload) -> wire::BenchmarkPayload {
    wire::BenchmarkPayload {
        effective_args: value.effective_args,
        workload_key: value.workload_key,
        sqlite_seed_key: value.sqlite_seed_key,
        shared_baseline_calibration: value.shared_baseline_calibration,
        baseline_calibration_id: value.baseline_calibration_id,
        run_index: value.run_index,
        requested_run_count: value.requested_run_count,
    }
}

fn validation_payload(
    value: wire::BlockValidationPayload,
) -> Result<domain::BlockValidationPayload, domain::ProtocolError> {
    Ok(domain::BlockValidationPayload {
        epoch: epoch(value.epoch)?,
        range: range(required("block_validation.range", value.range)?),
        requested_shards: value.requested_shards,
        max_concurrency: value.max_concurrency,
        timeout_secs: value.timeout_secs,
    })
}

fn validation_payload_wire(value: domain::BlockValidationPayload) -> wire::BlockValidationPayload {
    wire::BlockValidationPayload {
        epoch: epoch_wire(value.epoch),
        range: Some(range_wire(value.range)),
        requested_shards: value.requested_shards,
        max_concurrency: value.max_concurrency,
        timeout_secs: value.timeout_secs,
    }
}

fn task_payload(value: wire::TaskPayload) -> Result<domain::TaskPayload, domain::ProtocolError> {
    match required("task_payload.kind", value.kind)? {
        wire::task_payload::Kind::Benchmark(value) => {
            Ok(domain::TaskPayload::Benchmark(benchmark_payload(value)))
        }
        wire::task_payload::Kind::BuildOnly(_) => Ok(domain::TaskPayload::BuildOnly),
        wire::task_payload::Kind::BlockValidation(value) => {
            Ok(domain::TaskPayload::BlockValidation(validation_payload(value)?))
        }
    }
}

fn task_payload_wire(value: domain::TaskPayload) -> wire::TaskPayload {
    let kind = match value {
        domain::TaskPayload::Benchmark(value) => {
            wire::task_payload::Kind::Benchmark(benchmark_payload_wire(value))
        }
        domain::TaskPayload::BuildOnly => wire::task_payload::Kind::BuildOnly(wire::Empty {}),
        domain::TaskPayload::BlockValidation(value) => {
            wire::task_payload::Kind::BlockValidation(validation_payload_wire(value))
        }
    };
    wire::TaskPayload { kind: Some(kind) }
}

impl Wire for wire::TaskPayload {
    type Domain = domain::TaskPayload;

    fn into_domain(self) -> Result<Self::Domain, domain::ProtocolError> {
        task_payload(self)
    }

    fn from_domain(value: Self::Domain) -> Self {
        task_payload_wire(value)
    }
}

fn assignment(value: wire::Assignment) -> Result<domain::Assignment, domain::ProtocolError> {
    let context = required("assignment.context", value.context)?;
    Ok(domain::Assignment {
        identity: identity(required("assignment.identity", value.identity)?)?,
        trace_id: uuid("trace_id", &value.trace_id)?,
        context: domain::AssignmentContext {
            job_id: uuid("job_id", &context.job_id)?,
            repository: context.repository,
            commit: context.commit,
        },
        payload: task_payload(required("assignment.payload", value.payload)?)?,
        payload_hash: value.payload_hash,
        vcpu_cpuset: value.vcpu_cpuset,
    })
}

fn assignment_wire(value: domain::Assignment) -> wire::Assignment {
    wire::Assignment {
        identity: Some(identity_wire(value.identity)),
        trace_id: value.trace_id.to_string(),
        context: Some(wire::AssignmentContext {
            job_id: value
                .context
                .job_id
                .to_string(),
            repository: value.context.repository,
            commit: value.context.commit,
        }),
        payload: Some(task_payload_wire(value.payload)),
        payload_hash: value.payload_hash,
        vcpu_cpuset: value.vcpu_cpuset,
    }
}

fn event_payload(
    value: wire::ReliableEventPayload,
) -> Result<domain::ReliableEventPayload, domain::ProtocolError> {
    match required("reliable_event.payload.kind", value.kind)? {
        wire::reliable_event_payload::Kind::Phase(value) => {
            Ok(domain::ReliableEventPayload::Phase {
                label: value.label,
                elapsed_ms: value.elapsed_ms,
            })
        }
        wire::reliable_event_payload::Kind::Terminal(value) => {
            Ok(domain::ReliableEventPayload::Terminal {
                outcome_digest: value.outcome_digest,
            })
        }
    }
}

fn event_payload_wire(value: domain::ReliableEventPayload) -> wire::ReliableEventPayload {
    let kind = match value {
        domain::ReliableEventPayload::Phase { label, elapsed_ms } => {
            wire::reliable_event_payload::Kind::Phase(wire::ReliablePhaseEvent {
                label,
                elapsed_ms,
            })
        }
        domain::ReliableEventPayload::Terminal { outcome_digest } => {
            wire::reliable_event_payload::Kind::Terminal(wire::ReliableTerminalEvent {
                outcome_digest,
            })
        }
    };
    wire::ReliableEventPayload { kind: Some(kind) }
}

fn progress_update(value: wire::ProgressUpdate) -> domain::ProgressUpdate {
    domain::ProgressUpdate {
        workflow_step: value.workflow_step,
        run_index: value.run_index,
        requested_run_count: value.requested_run_count,
        phase: value.phase,
        progress: value.progress,
        total: value.total,
        message: value.message,
    }
}

fn progress_update_wire(value: domain::ProgressUpdate) -> wire::ProgressUpdate {
    wire::ProgressUpdate {
        workflow_step: value.workflow_step,
        run_index: value.run_index,
        requested_run_count: value.requested_run_count,
        phase: value.phase,
        progress: value.progress,
        total: value.total,
        message: value.message,
    }
}

fn header(value: wire::HeaderValue) -> domain::HeaderValue {
    domain::HeaderValue {
        name: value.name,
        value: value.value,
    }
}

fn header_wire(value: domain::HeaderValue) -> wire::HeaderValue {
    wire::HeaderValue {
        name: value.name,
        value: value.value,
    }
}

fn artifact(value: wire::ArtifactDescriptor) -> domain::ArtifactDescriptor {
    domain::ArtifactDescriptor {
        key: value.key,
        logical_key: value.logical_key,
        size: value.size,
        sha256: value.sha256,
    }
}

fn artifact_wire(value: domain::ArtifactDescriptor) -> wire::ArtifactDescriptor {
    wire::ArtifactDescriptor {
        key: value.key,
        logical_key: value.logical_key,
        size: value.size,
        sha256: value.sha256,
    }
}

fn validation_result(
    value: wire::BlockValidationResult,
) -> Result<domain::BlockValidationResult, domain::ProtocolError> {
    Ok(domain::BlockValidationResult {
        valid: value.valid,
        checked_blocks: value.checked_blocks,
        invalid_blocks: value
            .invalid_blocks
            .into_iter()
            .map(|value| domain::InvalidBlock {
                shard: value.shard,
                block: value.block,
                reason: value.reason,
            })
            .collect(),
        chainstate_origin: value.chainstate_origin,
        observed_range: range(required(
            "block_validation_result.observed_range",
            value.observed_range,
        )?),
    })
}

fn validation_result_wire(value: domain::BlockValidationResult) -> wire::BlockValidationResult {
    wire::BlockValidationResult {
        valid: value.valid,
        checked_blocks: value.checked_blocks,
        invalid_blocks: value
            .invalid_blocks
            .into_iter()
            .map(|value| wire::InvalidBlock {
                shard: value.shard,
                block: value.block,
                reason: value.reason,
            })
            .collect(),
        chainstate_origin: value.chainstate_origin,
        observed_range: Some(range_wire(value.observed_range)),
    }
}

fn json(value: &str) -> Result<serde_json::Value, domain::ProtocolError> {
    serde_json::from_str(value)
        .map_err(|error| invalid("summary_json", format!("invalid JSON: {error}")))
}

fn json_wire(value: &serde_json::Value) -> String {
    serde_json::to_string(value).expect("serializing serde_json::Value cannot fail")
}

fn terminal(
    value: wire::TerminalOutcome,
) -> Result<domain::TerminalOutcome, domain::ProtocolError> {
    match required("terminal_outcome.kind", value.kind)? {
        wire::terminal_outcome::Kind::Completed(value) => Ok(domain::TerminalOutcome::Completed {
            summary: json(&value.summary_json)?,
            block_validation: value
                .block_validation
                .map(validation_result)
                .transpose()?,
        }),
        wire::terminal_outcome::Kind::Failed(value) => Ok(domain::TerminalOutcome::Failed {
            error: value.error,
            summary: value
                .summary_json
                .as_deref()
                .map(json)
                .transpose()?,
            retryable: value.retryable,
        }),
        wire::terminal_outcome::Kind::Cancelled(value) => {
            Ok(domain::TerminalOutcome::Cancelled { reason: value.reason })
        }
    }
}

fn terminal_wire(value: domain::TerminalOutcome) -> wire::TerminalOutcome {
    let kind = match value {
        domain::TerminalOutcome::Completed { summary, block_validation } => {
            wire::terminal_outcome::Kind::Completed(wire::CompletedOutcome {
                summary_json: json_wire(&summary),
                block_validation: block_validation.map(validation_result_wire),
            })
        }
        domain::TerminalOutcome::Failed { error, summary, retryable } => {
            wire::terminal_outcome::Kind::Failed(wire::FailedOutcome {
                error,
                summary_json: summary
                    .as_ref()
                    .map(json_wire),
                retryable,
            })
        }
        domain::TerminalOutcome::Cancelled { reason } => {
            wire::terminal_outcome::Kind::Cancelled(wire::CancelledOutcome { reason })
        }
    };
    wire::TerminalOutcome { kind: Some(kind) }
}

impl Wire for wire::TerminalOutcome {
    type Domain = domain::TerminalOutcome;

    fn into_domain(self) -> Result<Self::Domain, domain::ProtocolError> {
        terminal(self)
    }

    fn from_domain(value: Self::Domain) -> Self {
        terminal_wire(value)
    }
}

impl Wire for wire::RegisterRequest {
    type Domain = domain::RegisterSessionRequest;

    fn into_domain(self) -> Result<Self::Domain, domain::ProtocolError> {
        Ok(domain::RegisterSessionRequest {
            protocol_version: protocol_version(self.protocol_version)?,
            worker_id: uuid("worker_id", &self.worker_id)?,
            worker_session_id: uuid("worker_session_id", &self.worker_session_id)?,
            software_version: self.software_version,
            advertised_capabilities: self
                .advertised_capabilities
                .into_iter()
                .map(capability)
                .collect::<Result<BTreeSet<_>, _>>()?,
            resources: required("resources", self.resources)?.into_domain()?,
        })
    }

    fn from_domain(value: Self::Domain) -> Self {
        Self {
            protocol_version: u32::from(value.protocol_version),
            worker_id: value.worker_id.to_string(),
            worker_session_id: value
                .worker_session_id
                .to_string(),
            software_version: value.software_version,
            advertised_capabilities: value
                .advertised_capabilities
                .into_iter()
                .map(capability_wire)
                .collect(),
            resources: Some(wire::ResourceFacts::from_domain(value.resources)),
        }
    }
}

impl Wire for wire::ResourceFacts {
    type Domain = domain::ResourceFacts;

    fn into_domain(self) -> Result<Self::Domain, domain::ProtocolError> {
        Ok(domain::ResourceFacts {
            logical_cpus: self.logical_cpus,
            memory_bytes: self.memory_bytes,
        })
    }

    fn from_domain(value: Self::Domain) -> Self {
        Self {
            logical_cpus: value.logical_cpus,
            memory_bytes: value.memory_bytes,
        }
    }
}

impl Wire for wire::ListCleanupRequest {
    type Domain = domain::CleanupListRequest;

    fn into_domain(self) -> Result<Self::Domain, domain::ProtocolError> {
        Ok(domain::CleanupListRequest {
            worker_session_id: uuid("worker_session_id", &self.worker_session_id)?,
        })
    }

    fn from_domain(value: Self::Domain) -> Self {
        Self {
            worker_session_id: value
                .worker_session_id
                .to_string(),
        }
    }
}

impl Wire for wire::CompleteCleanupRequest {
    type Domain = domain::CleanupCompleteRequest;

    fn into_domain(self) -> Result<Self::Domain, domain::ProtocolError> {
        Ok(domain::CleanupCompleteRequest {
            worker_session_id: uuid("worker_session_id", &self.worker_session_id)?,
            obligation_id: self.obligation_id,
        })
    }

    fn from_domain(value: Self::Domain) -> Self {
        Self {
            worker_session_id: value
                .worker_session_id
                .to_string(),
            obligation_id: value.obligation_id,
        }
    }
}

impl Wire for wire::DeregisterRequest {
    type Domain = domain::DeregisterSessionRequest;

    fn into_domain(self) -> Result<Self::Domain, domain::ProtocolError> {
        Ok(domain::DeregisterSessionRequest {
            worker_session_id: uuid("worker_session_id", &self.worker_session_id)?,
        })
    }

    fn from_domain(value: Self::Domain) -> Self {
        Self {
            worker_session_id: value
                .worker_session_id
                .to_string(),
        }
    }
}

impl Wire for wire::PollRequest {
    type Domain = domain::PollRequest;

    fn into_domain(self) -> Result<Self::Domain, domain::ProtocolError> {
        Ok(domain::PollRequest {
            worker_session_id: uuid("worker_session_id", &self.worker_session_id)?,
        })
    }

    fn from_domain(value: Self::Domain) -> Self {
        Self {
            worker_session_id: value
                .worker_session_id
                .to_string(),
        }
    }
}

impl Wire for wire::RegisterResponse {
    type Domain = domain::RegisterSessionResponse;

    fn into_domain(self) -> Result<Self::Domain, domain::ProtocolError> {
        Ok(domain::RegisterSessionResponse {
            protocol_version: protocol_version(self.protocol_version)?,
            heartbeat_interval_ms: self.heartbeat_interval_ms,
            lease_ttl_ms: self.lease_ttl_ms,
            server_time_ms: self.server_time_ms,
        })
    }

    fn from_domain(value: Self::Domain) -> Self {
        Self {
            protocol_version: u32::from(value.protocol_version),
            heartbeat_interval_ms: value.heartbeat_interval_ms,
            lease_ttl_ms: value.lease_ttl_ms,
            server_time_ms: value.server_time_ms,
        }
    }
}

impl Wire for wire::ListCleanupResponse {
    type Domain = Vec<domain::CleanupItem>;

    fn into_domain(self) -> Result<Self::Domain, domain::ProtocolError> {
        self.items
            .into_iter()
            .map(|item| {
                Ok(domain::CleanupItem {
                    id: item.id,
                    attempt_id: uuid("attempt_id", &item.attempt_id)?,
                    job_id: uuid("job_id", &item.job_id)?,
                    reason: item.reason,
                })
            })
            .collect()
    }

    fn from_domain(value: Self::Domain) -> Self {
        Self {
            items: value
                .into_iter()
                .map(|item| wire::CleanupItem {
                    id: item.id,
                    attempt_id: item.attempt_id.to_string(),
                    job_id: item.job_id.to_string(),
                    reason: item.reason,
                })
                .collect(),
        }
    }
}

impl Wire for wire::PollResponse {
    type Domain = domain::PollResponse;

    fn into_domain(self) -> Result<Self::Domain, domain::ProtocolError> {
        match required("poll_response.result", self.result)? {
            wire::poll_response::Result::NoWork(value) => Ok(domain::PollResponse::NoWork {
                retry_after_ms: value.retry_after_ms,
            }),
            wire::poll_response::Result::Drain(_) => Ok(domain::PollResponse::Drain),
            wire::poll_response::Result::Offer(value) => Ok(domain::PollResponse::Offer {
                offer: Box::new(offer(required("poll_response.offer", value.offer)?)?),
            }),
        }
    }

    fn from_domain(value: Self::Domain) -> Self {
        let result = match value {
            domain::PollResponse::NoWork { retry_after_ms } => {
                wire::poll_response::Result::NoWork(wire::PollNoWork { retry_after_ms })
            }
            domain::PollResponse::Drain => wire::poll_response::Result::Drain(wire::Empty {}),
            domain::PollResponse::Offer { offer } => {
                wire::poll_response::Result::Offer(wire::PollOffer {
                    offer: Some(offer_wire(*offer)),
                })
            }
        };
        Self { result: Some(result) }
    }
}

impl Wire for wire::AcceptRequest {
    type Domain = domain::AcceptOfferRequest;

    fn into_domain(self) -> Result<Self::Domain, domain::ProtocolError> {
        Ok(domain::AcceptOfferRequest {
            identity: identity(required("attempt_identity", self.identity)?)?,
        })
    }

    fn from_domain(value: Self::Domain) -> Self {
        Self {
            identity: Some(identity_wire(value.identity)),
        }
    }
}

impl Wire for wire::FetchRepositoryCredentialRequest {
    type Domain = domain::RepositoryCredentialRequest;

    fn into_domain(self) -> Result<Self::Domain, domain::ProtocolError> {
        Ok(domain::RepositoryCredentialRequest {
            identity: identity(required("attempt_identity", self.identity)?)?,
        })
    }

    fn from_domain(value: Self::Domain) -> Self {
        Self {
            identity: Some(identity_wire(value.identity)),
        }
    }
}

impl Wire for wire::HeartbeatRequest {
    type Domain = domain::HeartbeatRequest;

    fn into_domain(self) -> Result<Self::Domain, domain::ProtocolError> {
        Ok(domain::HeartbeatRequest {
            identity: identity(required("attempt_identity", self.identity)?)?,
            reliable_buffer_len: self.reliable_buffer_len,
        })
    }

    fn from_domain(value: Self::Domain) -> Self {
        Self {
            identity: Some(identity_wire(value.identity)),
            reliable_buffer_len: value.reliable_buffer_len,
        }
    }
}

impl Wire for wire::AcceptResponse {
    type Domain = domain::AcceptOfferResponse;

    fn into_domain(self) -> Result<Self::Domain, domain::ProtocolError> {
        Ok(domain::AcceptOfferResponse {
            assignment: assignment(required("assignment", self.assignment)?)?,
        })
    }

    fn from_domain(value: Self::Domain) -> Self {
        Self {
            assignment: Some(assignment_wire(value.assignment)),
        }
    }
}

impl Wire for wire::FetchRepositoryCredentialResponse {
    type Domain = domain::RepositoryCredentialResponse;

    fn into_domain(self) -> Result<Self::Domain, domain::ProtocolError> {
        Ok(domain::RepositoryCredentialResponse {
            token: domain::RepositoryToken(self.token),
            expires_at_ms: self.expires_at_ms,
        })
    }

    fn from_domain(value: Self::Domain) -> Self {
        Self {
            token: value.token.0,
            expires_at_ms: value.expires_at_ms,
        }
    }
}

impl Wire for wire::HeartbeatResponse {
    type Domain = domain::HeartbeatResponse;

    fn into_domain(self) -> Result<Self::Domain, domain::ProtocolError> {
        Ok(domain::HeartbeatResponse {
            desired_state: desired_state(self.desired_state)?,
            lease_expires_at_ms: self.lease_expires_at_ms,
            highest_contiguous_reliable_seq: self.highest_contiguous_reliable_seq,
        })
    }

    fn from_domain(value: Self::Domain) -> Self {
        Self {
            desired_state: desired_state_wire(value.desired_state),
            lease_expires_at_ms: value.lease_expires_at_ms,
            highest_contiguous_reliable_seq: value.highest_contiguous_reliable_seq,
        }
    }
}

impl Wire for wire::PublishReliableEventRequest {
    type Domain = domain::ReliableEventEnvelope;

    fn into_domain(self) -> Result<Self::Domain, domain::ProtocolError> {
        Ok(domain::ReliableEventEnvelope {
            identity: identity(required("attempt_identity", self.identity)?)?,
            trace_id: uuid("trace_id", &self.trace_id)?,
            reliable_seq: self.reliable_seq,
            payload: event_payload(required("reliable_event.payload", self.payload)?)?,
            payload_digest: self.payload_digest,
            worker_timestamp_ms: self.worker_timestamp_ms,
        })
    }

    fn from_domain(value: Self::Domain) -> Self {
        Self {
            identity: Some(identity_wire(value.identity)),
            trace_id: value.trace_id.to_string(),
            reliable_seq: value.reliable_seq,
            payload: Some(event_payload_wire(value.payload)),
            payload_digest: value.payload_digest,
            worker_timestamp_ms: value.worker_timestamp_ms,
        }
    }
}

impl Wire for wire::PublishReliableEventResponse {
    type Domain = domain::ReliableEventAck;

    fn into_domain(self) -> Result<Self::Domain, domain::ProtocolError> {
        Ok(domain::ReliableEventAck {
            highest_contiguous_reliable_seq: self.highest_contiguous_reliable_seq,
        })
    }

    fn from_domain(value: Self::Domain) -> Self {
        Self {
            highest_contiguous_reliable_seq: value.highest_contiguous_reliable_seq,
        }
    }
}

impl Wire for wire::PublishProgressRequest {
    type Domain = domain::ProgressRequest;

    fn into_domain(self) -> Result<Self::Domain, domain::ProtocolError> {
        Ok(domain::ProgressRequest {
            identity: identity(required("attempt_identity", self.identity)?)?,
            trace_id: uuid("trace_id", &self.trace_id)?,
            progress_seq: self.progress_seq,
            update: progress_update(required("progress.update", self.update)?),
        })
    }

    fn from_domain(value: Self::Domain) -> Self {
        Self {
            identity: Some(identity_wire(value.identity)),
            trace_id: value.trace_id.to_string(),
            progress_seq: value.progress_seq,
            update: Some(progress_update_wire(value.update)),
        }
    }
}

impl Wire for wire::GrantArtifactRequest {
    type Domain = domain::ArtifactGrantRequest;

    fn into_domain(self) -> Result<Self::Domain, domain::ProtocolError> {
        Ok(domain::ArtifactGrantRequest {
            identity: identity(required("attempt_identity", self.identity)?)?,
            operation: artifact_operation(self.operation)?,
            key: self.key,
            size: self.size,
            sha256: self.sha256,
        })
    }

    fn from_domain(value: Self::Domain) -> Self {
        Self {
            identity: Some(identity_wire(value.identity)),
            operation: artifact_operation_wire(value.operation),
            key: value.key,
            size: value.size,
            sha256: value.sha256,
        }
    }
}

impl Wire for wire::GrantArtifactResponse {
    type Domain = domain::ArtifactGrantResponse;

    fn into_domain(self) -> Result<Self::Domain, domain::ProtocolError> {
        Ok(domain::ArtifactGrantResponse {
            method: self.method,
            key: self.key,
            url: self.url,
            headers: self
                .headers
                .into_iter()
                .map(header)
                .collect(),
            expires_at_ms: self.expires_at_ms,
        })
    }

    fn from_domain(value: Self::Domain) -> Self {
        Self {
            method: value.method,
            key: value.key,
            url: value.url,
            headers: value
                .headers
                .into_iter()
                .map(header_wire)
                .collect(),
            expires_at_ms: value.expires_at_ms,
        }
    }
}

impl Wire for wire::CompleteAttemptRequest {
    type Domain = domain::CompleteAttemptRequest;

    fn into_domain(self) -> Result<Self::Domain, domain::ProtocolError> {
        Ok(domain::CompleteAttemptRequest {
            identity: identity(required("attempt_identity", self.identity)?)?,
            trace_id: uuid("trace_id", &self.trace_id)?,
            terminal_reliable_seq: self.terminal_reliable_seq,
            terminal_payload_digest: self.terminal_payload_digest,
            outcome: terminal(required("terminal_outcome", self.outcome)?)?,
            artifacts: self
                .artifacts
                .into_iter()
                .map(artifact)
                .collect(),
        })
    }

    fn from_domain(value: Self::Domain) -> Self {
        Self {
            identity: Some(identity_wire(value.identity)),
            trace_id: value.trace_id.to_string(),
            terminal_reliable_seq: value.terminal_reliable_seq,
            terminal_payload_digest: value.terminal_payload_digest,
            outcome: Some(terminal_wire(value.outcome)),
            artifacts: value
                .artifacts
                .into_iter()
                .map(artifact_wire)
                .collect(),
        }
    }
}

macro_rules! bool_response {
    ($wire:ty, $field:ident) => {
        impl Wire for $wire {
            type Domain = bool;

            fn into_domain(self) -> Result<Self::Domain, domain::ProtocolError> {
                Ok(self.$field)
            }

            fn from_domain(value: Self::Domain) -> Self {
                Self { $field: value }
            }
        }
    };
}

bool_response!(wire::PublishProgressResponse, accepted);
bool_response!(wire::CompleteCleanupResponse, completed);
bool_response!(wire::DeregisterResponse, deregistered);

impl Wire for wire::CompleteAttemptResponse {
    type Domain = domain::CompleteAttemptResponse;

    fn into_domain(self) -> Result<Self::Domain, domain::ProtocolError> {
        Ok(domain::CompleteAttemptResponse { accepted: self.accepted })
    }

    fn from_domain(value: Self::Domain) -> Self {
        Self { accepted: value.accepted }
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use prost::Message;
    use sbgh_fleet::Validate;

    use super::*;

    macro_rules! assert_not_debug {
        ($type:ty) => {
            const _: fn() = || {
                struct DebugImpl;
                trait AmbiguousIfDebug<A> {
                    fn check() {}
                }
                impl<T: ?Sized> AmbiguousIfDebug<()> for T {}
                impl<T: ?Sized + Debug> AmbiguousIfDebug<DebugImpl> for T {}
                let _ = <$type as AmbiguousIfDebug<_>>::check;
            };
        };
    }

    assert_not_debug!(wire::AttemptIdentity);
    assert_not_debug!(wire::FetchRepositoryCredentialResponse);
    assert_not_debug!(wire::GrantArtifactResponse);
    assert_not_debug!(wire::CompleteAttemptRequest);

    fn id(value: &str) -> Uuid {
        Uuid::parse_str(value).unwrap()
    }

    fn attempt_identity() -> domain::AttemptIdentity {
        domain::AttemptIdentity {
            worker_session_id: id("11111111-1111-4111-8111-111111111111"),
            attempt_id: id("22222222-2222-4222-8222-222222222222"),
            fencing_generation: 7,
            lease_token: domain::LeaseToken("a".repeat(64)),
        }
    }

    fn benchmark_payload() -> domain::TaskPayload {
        domain::TaskPayload::Benchmark(domain::BenchmarkPayload {
            effective_args: vec!["--count".into(), "5".into()],
            workload_key: Some("key".into()),
            sqlite_seed_key: Some("shared/stacks-bench.db".into()),
            shared_baseline_calibration: true,
            baseline_calibration_id: Some(4),
            run_index: 1,
            requested_run_count: 3,
        })
    }

    fn assignment(payload: domain::TaskPayload) -> domain::Assignment {
        domain::Assignment {
            identity: attempt_identity(),
            trace_id: id("33333333-3333-4333-8333-333333333333"),
            context: domain::AssignmentContext {
                job_id: id("44444444-4444-4444-8444-444444444444"),
                repository: "stacks-network/stacks-core".into(),
                commit: "1".repeat(40),
            },
            payload,
            payload_hash: "b".repeat(64),
            vcpu_cpuset: Some("2-5".into()),
        }
    }

    fn assert_wire_round_trip<W>(expected: W::Domain)
    where
        W: Wire + Message + Default,
        W::Domain: Clone + Debug + PartialEq,
    {
        let encoded = W::from_domain(expected.clone()).encode_to_vec();
        let decoded = W::decode(encoded.as_slice())
            .unwrap()
            .into_domain()
            .unwrap();
        assert_eq!(decoded, expected);
    }

    #[test]
    fn every_task_payload_variant_round_trips() {
        let values = [
            benchmark_payload(),
            domain::TaskPayload::BuildOnly,
            domain::TaskPayload::BlockValidation(domain::BlockValidationPayload {
                epoch: domain::ValidationEpoch::Nakamoto,
                range: domain::InclusiveRange { start: 10, end: 20 },
                requested_shards: 4,
                max_concurrency: 2,
                timeout_secs: 600,
            }),
        ];

        for value in values {
            let actual = wire::TaskPayload::from_domain(value.clone())
                .into_domain()
                .unwrap();
            assert_eq!(actual, value);
            actual.validate().unwrap();
        }
    }

    #[test]
    fn every_rpc_message_round_trips_through_protobuf_bytes() {
        let worker_id = id("55555555-5555-4555-8555-555555555555");
        let session_id = attempt_identity().worker_session_id;
        let trace_id = id("33333333-3333-4333-8333-333333333333");
        let event_payload = domain::ReliableEventPayload::Phase {
            label: "building".into(),
            elapsed_ms: 42,
        };

        assert_wire_round_trip::<wire::ResourceFacts>(domain::ResourceFacts {
            logical_cpus: 64,
            memory_bytes: 256 * 1024 * 1024 * 1024,
        });
        assert_wire_round_trip::<wire::RegisterRequest>(domain::RegisterSessionRequest {
            protocol_version: domain::PROTOCOL_VERSION,
            worker_id,
            worker_session_id: session_id,
            software_version: "v29-test".into(),
            advertised_capabilities: BTreeSet::from([
                domain::WorkerCapability::Benchmark,
                domain::WorkerCapability::BuildOnly,
                domain::WorkerCapability::BlockValidation,
            ]),
            resources: domain::ResourceFacts {
                logical_cpus: 64,
                memory_bytes: 256 * 1024 * 1024 * 1024,
            },
        });
        assert_wire_round_trip::<wire::RegisterResponse>(domain::RegisterSessionResponse {
            protocol_version: domain::PROTOCOL_VERSION,
            heartbeat_interval_ms: 5_000,
            lease_ttl_ms: 30_000,
            server_time_ms: 1_700_000_000_000,
        });
        assert_wire_round_trip::<wire::ListCleanupRequest>(domain::CleanupListRequest {
            worker_session_id: session_id,
        });
        assert_wire_round_trip::<wire::ListCleanupResponse>(vec![domain::CleanupItem {
            id: 9,
            attempt_id: attempt_identity().attempt_id,
            job_id: id("44444444-4444-4444-8444-444444444444"),
            reason: "worker restart".into(),
        }]);
        assert_wire_round_trip::<wire::CompleteCleanupRequest>(domain::CleanupCompleteRequest {
            worker_session_id: session_id,
            obligation_id: 9,
        });
        assert_wire_round_trip::<wire::CompleteCleanupResponse>(true);
        assert_wire_round_trip::<wire::DeregisterRequest>(domain::DeregisterSessionRequest {
            worker_session_id: session_id,
        });
        assert_wire_round_trip::<wire::DeregisterResponse>(true);
        assert_wire_round_trip::<wire::PollRequest>(domain::PollRequest {
            worker_session_id: session_id,
        });
        for response in [
            domain::PollResponse::NoWork { retry_after_ms: 1_000 },
            domain::PollResponse::Drain,
            domain::PollResponse::Offer {
                offer: Box::new(domain::WorkOffer {
                    identity: attempt_identity(),
                    job_id: id("44444444-4444-4444-8444-444444444444"),
                    trace_id,
                    capability: domain::WorkerCapability::BlockValidation,
                    requirements: domain::OfferRequirements::BlockValidation {
                        requested_shards: 8,
                        max_concurrency: 4,
                    },
                    payload_hash: "b".repeat(64),
                    offer_expires_at_ms: 1_700_000_030_000,
                }),
            },
        ] {
            assert_wire_round_trip::<wire::PollResponse>(response);
        }
        assert_wire_round_trip::<wire::AcceptRequest>(domain::AcceptOfferRequest {
            identity: attempt_identity(),
        });
        assert_wire_round_trip::<wire::AcceptResponse>(domain::AcceptOfferResponse {
            assignment: assignment(benchmark_payload()),
        });
        assert_wire_round_trip::<wire::FetchRepositoryCredentialRequest>(
            domain::RepositoryCredentialRequest { identity: attempt_identity() },
        );
        assert_wire_round_trip::<wire::FetchRepositoryCredentialResponse>(
            domain::RepositoryCredentialResponse {
                token: domain::RepositoryToken("github-token".into()),
                expires_at_ms: 1_700_000_030_000,
            },
        );
        assert_wire_round_trip::<wire::HeartbeatRequest>(domain::HeartbeatRequest {
            identity: attempt_identity(),
            reliable_buffer_len: 3,
        });
        for desired_state in [
            domain::DesiredState::Continue,
            domain::DesiredState::Cancel,
            domain::DesiredState::Drain,
        ] {
            assert_wire_round_trip::<wire::HeartbeatResponse>(domain::HeartbeatResponse {
                desired_state,
                lease_expires_at_ms: 1_700_000_030_000,
                highest_contiguous_reliable_seq: 6,
            });
        }
        for payload in [
            event_payload.clone(),
            domain::ReliableEventPayload::Terminal { outcome_digest: "c".repeat(64) },
        ] {
            assert_wire_round_trip::<wire::PublishReliableEventRequest>(
                domain::ReliableEventEnvelope {
                    identity: attempt_identity(),
                    trace_id,
                    reliable_seq: 7,
                    payload,
                    payload_digest: "d".repeat(64),
                    worker_timestamp_ms: 1_700_000_000_000,
                },
            );
        }
        assert_wire_round_trip::<wire::PublishReliableEventResponse>(domain::ReliableEventAck {
            highest_contiguous_reliable_seq: 7,
        });
        assert_wire_round_trip::<wire::PublishProgressRequest>(domain::ProgressRequest {
            identity: attempt_identity(),
            trace_id,
            progress_seq: 8,
            update: domain::ProgressUpdate {
                workflow_step: "validate".into(),
                run_index: 0,
                requested_run_count: 1,
                phase: "checking".into(),
                progress: 50,
                total: Some(100),
                message: Some("halfway".into()),
            },
        });
        assert_wire_round_trip::<wire::PublishProgressResponse>(true);
        for operation in [domain::ArtifactOperation::Put, domain::ArtifactOperation::Get] {
            assert_wire_round_trip::<wire::GrantArtifactRequest>(domain::ArtifactGrantRequest {
                identity: attempt_identity(),
                operation,
                key: "run/result.json".into(),
                size: Some(42),
                sha256: Some("e".repeat(64)),
            });
        }
        assert_wire_round_trip::<wire::GrantArtifactResponse>(domain::ArtifactGrantResponse {
            method: "PUT".into(),
            key: "attempt/result.json".into(),
            url: "https://objects.example/result?signature=secret".into(),
            headers: vec![domain::HeaderValue {
                name: "content-type".into(),
                value: "application/json".into(),
            }],
            expires_at_ms: 1_700_000_030_000,
        });

        let outcome = domain::TerminalOutcome::Completed {
            summary: serde_json::json!({"status": "ok"}),
            block_validation: Some(domain::BlockValidationResult {
                valid: false,
                checked_blocks: 11,
                invalid_blocks: vec![domain::InvalidBlock {
                    shard: 1,
                    block: "0xabc".into(),
                    reason: "invalid state root".into(),
                }],
                chainstate_origin: "mainnet-nightly".into(),
                observed_range: domain::InclusiveRange { start: 10, end: 20 },
            }),
        };
        assert_wire_round_trip::<wire::CompleteAttemptRequest>(domain::CompleteAttemptRequest {
            identity: attempt_identity(),
            trace_id,
            terminal_reliable_seq: 9,
            terminal_payload_digest: "f".repeat(64),
            outcome,
            artifacts: vec![domain::ArtifactDescriptor {
                key: "attempt/result.json".into(),
                logical_key: "result.json".into(),
                size: 42,
                sha256: "e".repeat(64),
            }],
        });
        assert_wire_round_trip::<wire::CompleteAttemptResponse>(domain::CompleteAttemptResponse {
            accepted: true,
        });
    }

    #[test]
    fn unspecified_enums_missing_messages_and_missing_oneofs_fail_closed() {
        let error = wire::TaskPayload { kind: None }
            .into_domain()
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("required protobuf field")
        );

        let error = wire::BlockValidationPayload {
            epoch: wire::ValidationEpoch::Unspecified as i32,
            range: Some(wire::InclusiveRange { start: 1, end: 2 }),
            requested_shards: 1,
            max_concurrency: 1,
            timeout_secs: 1,
        };
        assert!(validation_payload(error).is_err());

        assert!(capability(wire::WorkerCapability::Unspecified as i32).is_err());
        assert!(capability(i32::MAX).is_err());
        assert!(desired_state(wire::DesiredState::Unspecified as i32).is_err());
        assert!(artifact_operation(wire::ArtifactOperation::Unspecified as i32).is_err());
        assert!(
            wire::AcceptRequest { identity: None }
                .into_domain()
                .is_err()
        );
        assert!(
            wire::RegisterRequest {
                protocol_version: u32::from(domain::PROTOCOL_VERSION),
                worker_id: Uuid::new_v4().to_string(),
                worker_session_id: Uuid::new_v4().to_string(),
                software_version: "test".into(),
                advertised_capabilities: vec![wire::WorkerCapability::Benchmark as i32],
                resources: None,
            }
            .into_domain()
            .is_err()
        );
    }

    #[test]
    fn terminal_json_and_all_outcome_variants_round_trip() {
        let values = [
            domain::TerminalOutcome::Completed {
                summary: serde_json::json!({"nested": {"n": 1}, "ok": true}),
                block_validation: None,
            },
            domain::TerminalOutcome::Failed {
                error: "failed".into(),
                summary: Some(serde_json::json!({"partial": true})),
                retryable: true,
            },
            domain::TerminalOutcome::Cancelled { reason: "operator".into() },
        ];

        for value in values {
            assert_eq!(
                wire::TerminalOutcome::from_domain(value.clone())
                    .into_domain()
                    .unwrap(),
                value
            );
        }
    }
}
