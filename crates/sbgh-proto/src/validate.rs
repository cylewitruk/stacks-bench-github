use std::collections::BTreeSet;
use std::path::{Component, Path};

use thiserror::Error;

use crate::{
    AcceptOfferRequest, ArtifactDescriptor, ArtifactGrantRequest, Assignment, AttemptIdentity,
    CleanupCompleteRequest, CleanupListRequest, CompleteAttemptRequest, DatasetIdentity,
    DeregisterSessionRequest, HeartbeatRequest, InclusiveRange, PROTOCOL_VERSION, PollRequest,
    ProgressRequest, RegisterSessionRequest, ReliableEventEnvelope, ReliableEventPayload,
    RepositoryCredentialRequest, TaskPayload, WorkerCapability,
};

const MAX_LABEL: usize = 96;
const MAX_VERSION: usize = 128;
const MAX_REPOSITORY: usize = 512;
const MAX_ARGS: usize = 256;
const MAX_ARG_BYTES: usize = 16 * 1024;
const MAX_ARTIFACTS: usize = 256;
const MAX_OUTCOME_TEXT: usize = 16 * 1024;
const MAX_INVALID_BLOCKS: usize = 16_384;
pub const MAX_VALIDATION_SHARDS: u32 = 4_096;
pub const MAX_VALIDATION_CONCURRENCY: u32 = 1_024;
pub const MAX_VALIDATION_TIMEOUT_SECS: u64 = 7 * 24 * 60 * 60;
const MIN_WORKER_TIMESTAMP_MS: i64 = 946_684_800_000; // 2000-01-01
const MAX_WORKER_TIMESTAMP_MS: i64 = 32_503_680_000_000; // 3000-01-01

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    #[error("unsupported protocol version {actual}; expected {expected}")]
    Version { actual: u16, expected: u16 },
    #[error("invalid {field}: {reason}")]
    Invalid { field: &'static str, reason: String },
    #[error("serialization failed: {0}")]
    Serialization(String),
}

pub trait Validate {
    fn validate(&self) -> Result<(), ProtocolError>;
}

fn version(actual: u16) -> Result<(), ProtocolError> {
    if actual == PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(ProtocolError::Version {
            actual,
            expected: PROTOCOL_VERSION,
        })
    }
}

fn bounded(field: &'static str, value: &str, max: usize, empty: bool) -> Result<(), ProtocolError> {
    if (!empty && value.is_empty())
        || value.len() > max
        || value
            .chars()
            .any(char::is_control)
    {
        return Err(ProtocolError::Invalid {
            field,
            reason: format!("must be {}..={max} non-control bytes", usize::from(!empty)),
        });
    }
    Ok(())
}

fn sha256(field: &'static str, value: &str) -> Result<(), ProtocolError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        Ok(())
    } else {
        Err(ProtocolError::Invalid {
            field,
            reason: "must be a 64-character hexadecimal SHA-256 digest".into(),
        })
    }
}

fn safe_key(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= 1024
        && key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'-' | b'_' | b'.'))
        && Path::new(key)
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

fn immutable_commit(value: &str) -> bool {
    matches!(value.len(), 40 | 64)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
}

fn session_id(value: uuid::Uuid) -> Result<(), ProtocolError> {
    if value.is_nil() {
        Err(ProtocolError::Invalid {
            field: "worker_session_id",
            reason: "must not be nil".into(),
        })
    } else {
        Ok(())
    }
}

fn identity(identity: &AttemptIdentity) -> Result<(), ProtocolError> {
    if identity
        .worker_session_id
        .is_nil()
        || identity.attempt_id.is_nil()
    {
        return Err(ProtocolError::Invalid {
            field: "attempt identity",
            reason: "UUIDs must not be nil".into(),
        });
    }
    if identity.fencing_generation == 0
        || identity.fencing_generation > i64::MAX as u64
        || identity.lease_token.0.len() < 32
    {
        return Err(ProtocolError::Invalid {
            field: "attempt identity",
            reason: "fence must be non-zero and lease token must be present".into(),
        });
    }
    Ok(())
}

impl Validate for DatasetIdentity {
    fn validate(&self) -> Result<(), ProtocolError> {
        bounded("dataset.generation", &self.generation, MAX_LABEL, false)?;
        bounded("dataset.network", &self.network, MAX_LABEL, false)?;
        bounded("dataset.format_version", &self.format_version, MAX_LABEL, false)?;
        if self.covered_start > self.covered_end {
            return Err(ProtocolError::Invalid {
                field: "dataset.covered_range",
                reason: "start exceeds end".into(),
            });
        }
        if self.covered_end > i64::MAX as u64 {
            return Err(ProtocolError::Invalid {
                field: "dataset.covered_range",
                reason: "end exceeds the durable signed-integer range".into(),
            });
        }
        sha256("dataset.manifest_sha256", &self.manifest_sha256)
    }
}

impl Validate for InclusiveRange {
    fn validate(&self) -> Result<(), ProtocolError> {
        if self.start > self.end {
            return Err(ProtocolError::Invalid {
                field: "range",
                reason: "inclusive start exceeds end".into(),
            });
        }
        if self.end > i64::MAX as u64 {
            return Err(ProtocolError::Invalid {
                field: "range",
                reason: "end exceeds the durable signed-integer range".into(),
            });
        }
        Ok(())
    }
}

impl Validate for RegisterSessionRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        version(self.protocol_version)?;
        if self.worker_id.is_nil()
            || self
                .worker_session_id
                .is_nil()
        {
            return Err(ProtocolError::Invalid {
                field: "worker identity",
                reason: "UUID must not be nil".into(),
            });
        }
        bounded("software_version", &self.software_version, MAX_VERSION, false)?;
        if self
            .advertised_capabilities
            .is_empty()
        {
            return Err(ProtocolError::Invalid {
                field: "advertised_capabilities",
                reason: "at least one capability is required".into(),
            });
        }
        if self.resources.logical_cpus == 0 || self.resources.memory_bytes == 0 {
            return Err(ProtocolError::Invalid {
                field: "resources",
                reason: "logical_cpus and memory_bytes must be non-zero".into(),
            });
        }
        if let Some(dataset) = &self.resources.dataset {
            dataset.validate()?;
            if !self
                .advertised_capabilities
                .contains(&WorkerCapability::BlockValidation)
            {
                return Err(ProtocolError::Invalid {
                    field: "resources.dataset",
                    reason: "dataset requires block_validation capability".into(),
                });
            }
        }
        Ok(())
    }
}

impl Validate for CleanupListRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        version(self.protocol_version)?;
        session_id(self.worker_session_id)
    }
}

impl Validate for CleanupCompleteRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        version(self.protocol_version)?;
        session_id(self.worker_session_id)?;
        if self.obligation_id <= 0 {
            return Err(ProtocolError::Invalid {
                field: "obligation_id",
                reason: "must be positive".into(),
            });
        }
        Ok(())
    }
}

impl Validate for DeregisterSessionRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        version(self.protocol_version)?;
        session_id(self.worker_session_id)
    }
}

impl Validate for PollRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        version(self.protocol_version)?;
        session_id(self.worker_session_id)
    }
}

impl Validate for AcceptOfferRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        version(self.protocol_version)?;
        identity(&self.identity)
    }
}

impl Validate for HeartbeatRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        version(self.protocol_version)?;
        identity(&self.identity)?;
        if self.reliable_buffer_len > 4_096 {
            return Err(ProtocolError::Invalid {
                field: "reliable_buffer_len",
                reason: "exceeds the protocol telemetry bound".into(),
            });
        }
        Ok(())
    }
}

impl Validate for RepositoryCredentialRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        version(self.protocol_version)?;
        identity(&self.identity)
    }
}

impl Validate for ReliableEventEnvelope {
    fn validate(&self) -> Result<(), ProtocolError> {
        version(self.protocol_version)?;
        identity(&self.identity)?;
        if self.reliable_seq == 0 || self.reliable_seq > i64::MAX as u64 {
            return Err(ProtocolError::Invalid {
                field: "reliable_seq",
                reason: "sequence starts at 1".into(),
            });
        }
        if self.trace_id.is_nil() {
            return Err(ProtocolError::Invalid {
                field: "trace_id",
                reason: "must not be nil".into(),
            });
        }
        if !(MIN_WORKER_TIMESTAMP_MS..=MAX_WORKER_TIMESTAMP_MS).contains(&self.worker_timestamp_ms)
        {
            return Err(ProtocolError::Invalid {
                field: "worker_timestamp_ms",
                reason: "is outside the supported timestamp range".into(),
            });
        }
        match &self.payload {
            crate::ReliableEventPayload::Phase { label, elapsed_ms } => {
                bounded("phase label", label, MAX_LABEL, false)?;
                if *elapsed_ms > MAX_VALIDATION_TIMEOUT_SECS * 1_000 {
                    return Err(ProtocolError::Invalid {
                        field: "phase elapsed_ms",
                        reason: "exceeds the maximum task duration".into(),
                    });
                }
            }
            crate::ReliableEventPayload::Terminal { outcome_digest } => {
                sha256("terminal outcome digest", outcome_digest)?;
            }
        }
        sha256("payload_digest", &self.payload_digest)?;
        if crate::payload_digest(&self.payload)? != self.payload_digest {
            return Err(ProtocolError::Invalid {
                field: "payload_digest",
                reason: "does not match canonical payload bytes".into(),
            });
        }
        Ok(())
    }
}

impl Validate for ProgressRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        version(self.protocol_version)?;
        identity(&self.identity)?;
        if self.trace_id.is_nil() || self.progress_seq == 0 || self.progress_seq > i64::MAX as u64 {
            return Err(ProtocolError::Invalid {
                field: "progress identity",
                reason: "trace id and progress sequence must be present".into(),
            });
        }
        bounded("progress.workflow_step", &self.update.workflow_step, MAX_LABEL, false)?;
        bounded("progress.phase", &self.update.phase, MAX_LABEL, false)?;
        if self.update.run_index < 0
            || self
                .update
                .requested_run_count
                < 1
            || self.update.run_index
                >= self
                    .update
                    .requested_run_count
        {
            return Err(ProtocolError::Invalid {
                field: "progress run",
                reason: "run index must be within requested run count".into(),
            });
        }
        if let Some(total) = self.update.total
            && (total == 0 || self.update.progress > total)
        {
            return Err(ProtocolError::Invalid {
                field: "progress total",
                reason: "must be non-zero and not less than progress".into(),
            });
        }
        if let Some(message) = &self.update.message {
            bounded("progress.message", message, 4_096, true)?;
        }
        Ok(())
    }
}

impl Validate for TaskPayload {
    fn validate(&self) -> Result<(), ProtocolError> {
        match self {
            Self::Benchmark(payload) => {
                if payload.effective_args.len() > MAX_ARGS {
                    return Err(ProtocolError::Invalid {
                        field: "effective_args",
                        reason: format!("at most {MAX_ARGS} arguments are allowed"),
                    });
                }
                let bytes = payload
                    .effective_args
                    .iter()
                    .map(String::len)
                    .sum::<usize>();
                if bytes > MAX_ARG_BYTES
                    || payload
                        .effective_args
                        .iter()
                        .any(|arg| {
                            arg.chars()
                                .any(char::is_control)
                        })
                {
                    return Err(ProtocolError::Invalid {
                        field: "effective_args",
                        reason: format!("must contain at most {MAX_ARG_BYTES} non-control bytes"),
                    });
                }
                if payload.run_index < 0
                    || payload.requested_run_count < 1
                    || payload.run_index >= payload.requested_run_count
                {
                    return Err(ProtocolError::Invalid {
                        field: "benchmark run",
                        reason: "run index must be within requested run count".into(),
                    });
                }
                if payload
                    .baseline_calibration_id
                    .is_some_and(|calibration_id| calibration_id <= 0)
                {
                    return Err(ProtocolError::Invalid {
                        field: "baseline_calibration_id",
                        reason: "must be positive when present".into(),
                    });
                }
                if let Some(workload_key) = &payload.workload_key {
                    bounded("workload_key", workload_key, 512, false)?;
                }
                if let Some(seed_key) = &payload.sqlite_seed_key
                    && !safe_key(seed_key)
                {
                    return Err(ProtocolError::Invalid {
                        field: "sqlite_seed_key",
                        reason: "must be a relative normal-component key".into(),
                    });
                }
            }
            Self::BuildOnly => {}
            Self::BlockValidation(payload) => {
                payload.dataset.validate()?;
                payload.range.validate()?;
                if payload.requested_shards == 0
                    || payload.max_concurrency == 0
                    || payload.timeout_secs == 0
                {
                    return Err(ProtocolError::Invalid {
                        field: "block_validation",
                        reason: "shards, concurrency, and timeout must be non-zero".into(),
                    });
                }
                if payload.requested_shards > MAX_VALIDATION_SHARDS
                    || payload.max_concurrency > MAX_VALIDATION_CONCURRENCY
                    || payload.max_concurrency > payload.requested_shards
                    || payload.timeout_secs > MAX_VALIDATION_TIMEOUT_SECS
                {
                    return Err(ProtocolError::Invalid {
                        field: "block_validation",
                        reason: "shard, concurrency, or timeout limit exceeds protocol bounds"
                            .into(),
                    });
                }
                if payload.range.start < payload.dataset.covered_start
                    || payload.range.end > payload.dataset.covered_end
                {
                    return Err(ProtocolError::Invalid {
                        field: "block_validation.range",
                        reason: "range is outside the pinned dataset generation".into(),
                    });
                }
            }
        }
        Ok(())
    }
}

impl Validate for Assignment {
    fn validate(&self) -> Result<(), ProtocolError> {
        identity(&self.identity)?;
        if self.trace_id.is_nil() || self.context.job_id.is_nil() {
            return Err(ProtocolError::Invalid {
                field: "assignment identity",
                reason: "trace and job UUIDs must not be nil".into(),
            });
        }
        bounded("repository", &self.context.repository, MAX_REPOSITORY, false)?;
        let repository_parts = self
            .context
            .repository
            .split('/')
            .collect::<Vec<_>>();
        if repository_parts.len() != 2
            || repository_parts
                .iter()
                .any(|part| {
                    part.is_empty()
                        || !part.bytes().all(|byte| {
                            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')
                        })
                })
        {
            return Err(ProtocolError::Invalid {
                field: "repository",
                reason: "expected a non-empty ASCII owner/name".into(),
            });
        }
        if !immutable_commit(&self.context.commit) {
            return Err(ProtocolError::Invalid {
                field: "commit",
                reason: "must be an immutable 40- or 64-character hexadecimal object id".into(),
            });
        }
        if let Some(cpuset) = &self.vcpu_cpuset
            && (cpuset.is_empty()
                || cpuset.len() > 256
                || !cpuset
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || matches!(byte, b',' | b'-' | b'^')))
        {
            return Err(ProtocolError::Invalid {
                field: "vcpu_cpuset",
                reason: "contains unsupported characters or exceeds 256 bytes".into(),
            });
        }
        self.payload.validate()?;
        sha256("payload_hash", &self.payload_hash)?;
        if crate::payload_digest(&self.payload)? != self.payload_hash {
            return Err(ProtocolError::Invalid {
                field: "payload_hash",
                reason: "does not match canonical task payload".into(),
            });
        }
        Ok(())
    }
}

impl Validate for ArtifactGrantRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        version(self.protocol_version)?;
        identity(&self.identity)?;
        if !safe_key(&self.key) {
            return Err(ProtocolError::Invalid {
                field: "artifact key",
                reason: "must be a relative normal-component key".into(),
            });
        }
        if let Some(digest) = &self.sha256 {
            sha256("artifact sha256", digest)?;
        }
        if self
            .size
            .is_some_and(|size| size > i64::MAX as u64)
        {
            return Err(ProtocolError::Invalid {
                field: "artifact size",
                reason: "exceeds the durable signed-integer range".into(),
            });
        }
        Ok(())
    }
}

impl Validate for ArtifactDescriptor {
    fn validate(&self) -> Result<(), ProtocolError> {
        if !safe_key(&self.key) {
            return Err(ProtocolError::Invalid {
                field: "artifact key",
                reason: "must be a relative normal-component key".into(),
            });
        }
        if !safe_key(&self.logical_key) {
            return Err(ProtocolError::Invalid {
                field: "artifact logical key",
                reason: "must be a relative normal-component key".into(),
            });
        }
        sha256("artifact sha256", &self.sha256)?;
        if self.size > i64::MAX as u64 {
            return Err(ProtocolError::Invalid {
                field: "artifact size",
                reason: "exceeds the durable signed-integer range".into(),
            });
        }
        Ok(())
    }
}

fn validate_artifacts(artifacts: &[ArtifactDescriptor]) -> Result<(), ProtocolError> {
    if artifacts.len() > MAX_ARTIFACTS {
        return Err(ProtocolError::Invalid {
            field: "artifacts",
            reason: format!("at most {MAX_ARTIFACTS} artifacts are allowed"),
        });
    }
    let mut keys = BTreeSet::new();
    let mut logical_keys = BTreeSet::new();
    for artifact in artifacts {
        artifact.validate()?;
        if !keys.insert(&artifact.key) || !logical_keys.insert(&artifact.logical_key) {
            return Err(ProtocolError::Invalid {
                field: "artifacts",
                reason: "object and logical keys must be unique".into(),
            });
        }
    }
    Ok(())
}

impl Validate for CompleteAttemptRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        version(self.protocol_version)?;
        identity(&self.identity)?;
        if self.trace_id.is_nil()
            || self.terminal_reliable_seq == 0
            || self.terminal_reliable_seq > i64::MAX as u64
        {
            return Err(ProtocolError::Invalid {
                field: "terminal identity",
                reason: "trace id and terminal sequence must be present".into(),
            });
        }
        sha256("terminal_payload_digest", &self.terminal_payload_digest)?;
        validate_artifacts(&self.artifacts)?;
        match &self.outcome {
            crate::TerminalOutcome::Completed { block_validation, .. } => {
                if let Some(result) = block_validation {
                    result.dataset.validate()?;
                    if result.checked_blocks > i64::MAX as u64 {
                        return Err(ProtocolError::Invalid {
                            field: "block_validation.checked_blocks",
                            reason: "exceeds the durable signed-integer range".into(),
                        });
                    }
                    if result.invalid_blocks.len() > MAX_INVALID_BLOCKS {
                        return Err(ProtocolError::Invalid {
                            field: "block_validation.invalid_blocks",
                            reason: format!(
                                "at most {MAX_INVALID_BLOCKS} invalid blocks are allowed"
                            ),
                        });
                    }
                    if result.valid
                        != result
                            .invalid_blocks
                            .is_empty()
                    {
                        return Err(ProtocolError::Invalid {
                            field: "block_validation.valid",
                            reason: "must be true exactly when no invalid blocks are present"
                                .into(),
                        });
                    }
                    for invalid in &result.invalid_blocks {
                        bounded("invalid_block.block", &invalid.block, 512, false)?;
                        bounded("invalid_block.reason", &invalid.reason, 4_096, false)?;
                    }
                }
            }
            crate::TerminalOutcome::Failed { error, .. } => {
                bounded("terminal.error", error, MAX_OUTCOME_TEXT, false)?;
            }
            crate::TerminalOutcome::Cancelled { reason } => {
                bounded("terminal.reason", reason, MAX_OUTCOME_TEXT, false)?;
            }
        }
        let outcome_digest = crate::payload_digest(&self.outcome)?;
        let terminal_payload = ReliableEventPayload::Terminal { outcome_digest };
        if crate::payload_digest(&terminal_payload)? != self.terminal_payload_digest {
            return Err(ProtocolError::Invalid {
                field: "terminal_payload_digest",
                reason: "does not bind the submitted terminal outcome".into(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use uuid::Uuid;

    use super::*;
    use crate::{
        AssignmentContext, BenchmarkPayload, CleanupListRequest, CompleteAttemptRequest,
        HeartbeatRequest, LeaseToken, PROTOCOL_VERSION, ResourceFacts, TaskPayload,
        TerminalOutcome,
    };

    #[test]
    fn registration_rejects_dataset_without_authorized_shape() {
        let request = RegisterSessionRequest {
            protocol_version: PROTOCOL_VERSION,
            worker_id: Uuid::new_v4(),
            worker_session_id: Uuid::new_v4(),
            software_version: "test".into(),
            advertised_capabilities: BTreeSet::from([WorkerCapability::Benchmark]),
            resources: ResourceFacts {
                logical_cpus: 1,
                memory_bytes: 1,
                storage_bytes: 0,
                dataset: Some(DatasetIdentity {
                    generation: "g".into(),
                    network: "mainnet".into(),
                    format_version: "1".into(),
                    covered_start: 0,
                    covered_end: 1,
                    manifest_sha256: "00".repeat(32),
                }),
            },
        };
        assert!(request.validate().is_err());
    }

    #[test]
    fn artifact_keys_cannot_escape_attempt_namespace() {
        let descriptor = ArtifactDescriptor {
            key: "../secret".into(),
            logical_key: "job/run.json".into(),
            size: 1,
            sha256: "00".repeat(32),
        };
        assert!(descriptor.validate().is_err());
    }

    #[test]
    fn exact_protocol_version_and_telemetry_bounds_are_enforced() {
        let identity = AttemptIdentity {
            worker_session_id: Uuid::new_v4(),
            attempt_id: Uuid::new_v4(),
            fencing_generation: 1,
            lease_token: LeaseToken("x".repeat(64)),
        };
        let mut heartbeat = HeartbeatRequest {
            protocol_version: PROTOCOL_VERSION + 1,
            identity,
            reliable_buffer_len: 0,
        };
        assert!(matches!(heartbeat.validate(), Err(ProtocolError::Version { .. })));
        heartbeat.protocol_version = PROTOCOL_VERSION;
        heartbeat.reliable_buffer_len = 4_097;
        assert!(heartbeat.validate().is_err());
        assert!(matches!(
            CleanupListRequest {
                protocol_version: PROTOCOL_VERSION + 1,
                worker_session_id: Uuid::new_v4(),
            }
            .validate(),
            Err(ProtocolError::Version { .. })
        ));
    }

    #[test]
    fn assignment_wire_fixture_is_stable_and_secret_free() {
        let assignment = Assignment {
            identity: AttemptIdentity {
                worker_session_id: Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap(),
                attempt_id: Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap(),
                fencing_generation: 7,
                lease_token: LeaseToken("a".repeat(64)),
            },
            trace_id: Uuid::parse_str("33333333-3333-4333-8333-333333333333").unwrap(),
            context: AssignmentContext {
                job_id: Uuid::parse_str("44444444-4444-4444-8444-444444444444").unwrap(),
                repository: "stacks-network/stacks-core".into(),
                commit: "1".repeat(40),
            },
            payload: TaskPayload::Benchmark(BenchmarkPayload {
                effective_args: vec!["--mine-microblocks".into()],
                workload_key: Some("workload-v1".into()),
                sqlite_seed_key: None,
                shared_baseline_calibration: false,
                baseline_calibration_id: None,
                run_index: 0,
                requested_run_count: 1,
            }),
            payload_hash: "b".repeat(64),
            vcpu_cpuset: Some("2-5".into()),
        };
        let json = serde_json::to_string(&assignment).unwrap();
        assert_eq!(
            json,
            concat!(
                "{\"identity\":{\"worker_session_id\":\"11111111-1111-4111-8111-111111111111\",",
                "\"attempt_id\":\"22222222-2222-4222-8222-222222222222\",",
                "\"fencing_generation\":7,\"lease_token\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"},",
                "\"trace_id\":\"33333333-3333-4333-8333-333333333333\",",
                "\"context\":{\"job_id\":\"44444444-4444-4444-8444-444444444444\",",
                "\"repository\":\"stacks-network/stacks-core\",",
                "\"commit\":\"1111111111111111111111111111111111111111\"},",
                "\"payload\":{\"kind\":\"benchmark\",",
                "\"effective_args\":[\"--mine-microblocks\"],",
                "\"workload_key\":\"workload-v1\",\"sqlite_seed_key\":null,",
                "\"shared_baseline_calibration\":false,\"baseline_calibration_id\":null,",
                "\"run_index\":0,\"requested_run_count\":1},",
                "\"payload_hash\":\"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\",\"vcpu_cpuset\":\"2-5\"}"
            )
        );
        assert!(!json.contains("repository_token"));
        assert!(format!("{:?}", assignment.identity.lease_token).contains("[REDACTED]"));
    }

    #[test]
    fn terminal_digest_binds_the_exact_outcome() {
        let identity = AttemptIdentity {
            worker_session_id: Uuid::new_v4(),
            attempt_id: Uuid::new_v4(),
            fencing_generation: 1,
            lease_token: LeaseToken("x".repeat(64)),
        };
        let outcome = TerminalOutcome::Cancelled {
            reason: "operator request".into(),
        };
        let terminal = ReliableEventPayload::Terminal {
            outcome_digest: crate::payload_digest(&outcome).unwrap(),
        };
        let mut request = CompleteAttemptRequest {
            protocol_version: PROTOCOL_VERSION,
            identity,
            trace_id: Uuid::new_v4(),
            terminal_reliable_seq: 1,
            terminal_payload_digest: crate::payload_digest(&terminal).unwrap(),
            outcome,
            artifacts: Vec::new(),
        };
        assert!(request.validate().is_ok());
        request.outcome = TerminalOutcome::Cancelled {
            reason: "different request".into(),
        };
        assert!(request.validate().is_err());
    }
}
