use std::collections::BTreeSet;
use std::path::{Component, Path};

use thiserror::Error;

use crate::{
    AcceptOfferRequest, ArtifactDescriptor, ArtifactGrantRequest, Assignment, AttemptIdentity,
    BlockValidationPayload, BlockValidationResult, BlockValidationSelection,
    CleanupCompleteRequest, CleanupListRequest, CompleteAttemptRequest, DeregisterSessionRequest,
    HeartbeatRequest, InclusiveRange, PROTOCOL_VERSION, PollRequest, ProgressRequest,
    RegisterSessionRequest, RegistrationCheckRequest, ReliableEventEnvelope, ReliableEventPayload,
    RepositoryCredentialRequest, TaskPayload,
};

/// Verify that a worker's concrete validation result faithfully resolves the
/// durable selector. Resource sizing remains worker-owned; coverage does not.
pub fn validate_block_validation_result(
    payload: &BlockValidationPayload,
    result: &BlockValidationResult,
) -> Result<(), ProtocolError> {
    if result
        .observed
        .pre_nakamoto_count
        == 0
        || result.observed.nakamoto_count == 0
    {
        return Err(ProtocolError::Invalid {
            field: "block_validation.observed",
            reason: "both epoch counts must be non-zero".into(),
        });
    }
    let total = result
        .observed
        .pre_nakamoto_count
        .checked_add(result.observed.nakamoto_count)
        .ok_or_else(|| ProtocolError::Invalid {
            field: "block_validation.observed",
            reason: "total coverage overflows u64".into(),
        })?;
    if total > i64::MAX as u64 {
        return Err(ProtocolError::Invalid {
            field: "block_validation.observed",
            reason: "total coverage exceeds the durable signed-integer range".into(),
        });
    }
    let expected_range = match &payload.selection {
        BlockValidationSelection::Recent { block_count } => {
            if *block_count == 0 {
                return Err(ProtocolError::Invalid {
                    field: "block_validation.selection.recent.block_count",
                    reason: "must be non-zero".into(),
                });
            }
            let count = (*block_count).min(result.observed.nakamoto_count);
            InclusiveRange {
                start: total - count,
                end: total - 1,
            }
        }
        BlockValidationSelection::Full => InclusiveRange { start: 0, end: total - 1 },
        BlockValidationSelection::Range { range } => {
            if range.start > range.end || range.end >= total {
                return Err(ProtocolError::Invalid {
                    field: "block_validation.selection.range",
                    reason: "is reversed or exceeds observed coverage".into(),
                });
            }
            range.clone()
        }
    };
    if result.resolved_range != expected_range {
        return Err(ProtocolError::Invalid {
            field: "block_validation.resolved_range",
            reason: "does not match the durable selector and observed coverage".into(),
        });
    }

    let pre_count = result
        .observed
        .pre_nakamoto_count;
    let mut expected_segments = Vec::with_capacity(2);
    if expected_range.start < pre_count {
        let end = expected_range
            .end
            .min(pre_count - 1);
        expected_segments.push(crate::ValidationEpochSegment {
            epoch: crate::ValidationEpoch::PreNakamoto,
            global_range: InclusiveRange {
                start: expected_range.start,
                end,
            },
            local_range: InclusiveRange {
                start: expected_range.start,
                end,
            },
        });
    }
    if expected_range.end >= pre_count {
        let start = expected_range
            .start
            .max(pre_count);
        expected_segments.push(crate::ValidationEpochSegment {
            epoch: crate::ValidationEpoch::Nakamoto,
            global_range: InclusiveRange { start, end: expected_range.end },
            local_range: InclusiveRange {
                start: start - pre_count,
                end: expected_range.end - pre_count,
            },
        });
    }
    if result.segments != expected_segments {
        return Err(ProtocolError::Invalid {
            field: "block_validation.segments",
            reason: "do not exactly and gaplessly cover the resolved range".into(),
        });
    }
    let checked_blocks = expected_range
        .end
        .checked_sub(expected_range.start)
        .and_then(|span| span.checked_add(1))
        .ok_or_else(|| ProtocolError::Invalid {
            field: "block_validation.checked_blocks",
            reason: "resolved range length overflows u64".into(),
        })?;
    if result.checked_blocks != checked_blocks
        || result
            .invalid_blocks
            .iter()
            .any(|invalid| invalid.shard >= result.shard_count)
    {
        return Err(ProtocolError::Invalid {
            field: "block_validation.result",
            reason: "checked count or invalid-block shard does not match the resolved plan".into(),
        });
    }
    Ok(())
}

const MAX_LABEL: usize = 96;
const MAX_VERSION: usize = 128;
const MAX_REPOSITORY: usize = 512;
const MAX_ARGS: usize = 256;
const MAX_ARG_BYTES: usize = 16 * 1024;
const MAX_ARTIFACTS: usize = 256;
pub const MAX_TERMINAL_TEXT_BYTES: usize = 16 * 1024;
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

/// Convert local diagnostic text into the bounded, single-line form accepted
/// by terminal protocol fields. Full-fidelity diagnostics belong in the
/// worker's local logs; the wire carries a safe summary.
pub fn normalize_terminal_text(value: &str, fallback: &str) -> String {
    fn normalize(value: &str) -> String {
        let mut output = String::with_capacity(
            value
                .len()
                .min(MAX_TERMINAL_TEXT_BYTES),
        );
        let mut pending_space = false;
        for character in value.chars() {
            if character.is_control() || character.is_whitespace() {
                pending_space = !output.is_empty();
                continue;
            }
            if pending_space {
                if output.len() == MAX_TERMINAL_TEXT_BYTES {
                    break;
                }
                output.push(' ');
                pending_space = false;
            }
            if output.len() + character.len_utf8() > MAX_TERMINAL_TEXT_BYTES {
                break;
            }
            output.push(character);
        }
        output
    }

    let normalized = normalize(value);
    if !normalized.is_empty() {
        return normalized;
    }
    let fallback = normalize(fallback);
    if fallback.is_empty() { "unspecified terminal outcome".into() } else { fallback }
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
        registration_facts(
            self.protocol_version,
            &self.software_version,
            &self.advertised_capabilities,
            &self.resources,
        )?;
        if self
            .worker_session_id
            .is_nil()
        {
            return Err(ProtocolError::Invalid {
                field: "worker_session_id",
                reason: "UUID must not be nil".into(),
            });
        }
        Ok(())
    }
}

impl Validate for RegistrationCheckRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        registration_facts(
            self.protocol_version,
            &self.software_version,
            &self.advertised_capabilities,
            &self.resources,
        )
    }
}

fn registration_facts(
    protocol_version: u16,
    software_version: &str,
    advertised_capabilities: &BTreeSet<crate::WorkerCapability>,
    resources: &crate::ResourceFacts,
) -> Result<(), ProtocolError> {
    version(protocol_version)?;
    bounded("software_version", software_version, MAX_VERSION, false)?;
    if advertised_capabilities.is_empty() {
        return Err(ProtocolError::Invalid {
            field: "advertised_capabilities",
            reason: "at least one capability is required".into(),
        });
    }
    if resources.logical_cpus == 0 || resources.memory_bytes == 0 {
        return Err(ProtocolError::Invalid {
            field: "resources",
            reason: "logical_cpus and memory_bytes must be non-zero".into(),
        });
    }
    Ok(())
}

impl Validate for CleanupListRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        session_id(self.worker_session_id)
    }
}

impl Validate for CleanupCompleteRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
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
        session_id(self.worker_session_id)
    }
}

impl Validate for PollRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        session_id(self.worker_session_id)
    }
}

impl Validate for AcceptOfferRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        identity(&self.identity)
    }
}

impl Validate for HeartbeatRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
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
        identity(&self.identity)
    }
}

impl Validate for ReliableEventEnvelope {
    fn validate(&self) -> Result<(), ProtocolError> {
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
                match &payload.selection {
                    crate::BlockValidationSelection::Recent { block_count } => {
                        if *block_count == 0 || *block_count > i64::MAX as u64 {
                            return Err(ProtocolError::Invalid {
                                field: "block_validation.selection.recent.block_count",
                                reason: "must be within 1..=i64::MAX".into(),
                            });
                        }
                    }
                    crate::BlockValidationSelection::Full => {}
                    crate::BlockValidationSelection::Range { range } => range.validate()?,
                }
                if payload.timeout_secs == 0 {
                    return Err(ProtocolError::Invalid {
                        field: "block_validation",
                        reason: "timeout must be non-zero".into(),
                    });
                }
                if payload.timeout_secs > MAX_VALIDATION_TIMEOUT_SECS {
                    return Err(ProtocolError::Invalid {
                        field: "block_validation",
                        reason: "timeout exceeds the protocol bound".into(),
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

impl Validate for crate::InvalidBlock {
    fn validate(&self) -> Result<(), ProtocolError> {
        bounded("invalid_block.block", &self.block, 512, false)?;
        bounded("invalid_block.reason", &self.reason, 4_096, false)
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
                    bounded(
                        "block_validation.chainstate_origin",
                        &result.chainstate_origin,
                        512,
                        false,
                    )?;
                    result
                        .resolved_range
                        .validate()?;
                    if result
                        .observed
                        .pre_nakamoto_count
                        == 0
                        || result.observed.nakamoto_count == 0
                    {
                        return Err(ProtocolError::Invalid {
                            field: "block_validation.observed",
                            reason: "both epoch counts must be non-zero".into(),
                        });
                    }
                    if result.shard_count == 0
                        || result.shard_count > MAX_VALIDATION_SHARDS
                        || result.max_concurrency == 0
                        || result.max_concurrency > MAX_VALIDATION_CONCURRENCY
                        || result.max_concurrency > result.shard_count
                    {
                        return Err(ProtocolError::Invalid {
                            field: "block_validation.resource_plan",
                            reason: "invalid shard or concurrency count".into(),
                        });
                    }
                    if result.segments.is_empty() || result.segments.len() > 2 {
                        return Err(ProtocolError::Invalid {
                            field: "block_validation.segments",
                            reason: "must contain one or two epoch segments".into(),
                        });
                    }
                    for segment in &result.segments {
                        segment
                            .global_range
                            .validate()?;
                        segment
                            .local_range
                            .validate()?;
                    }
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
                        invalid.validate()?;
                    }
                }
            }
            crate::TerminalOutcome::Failed { error, .. } => {
                bounded("terminal.error", error, MAX_TERMINAL_TEXT_BYTES, false)?;
            }
            crate::TerminalOutcome::Cancelled { reason } => {
                bounded("terminal.reason", reason, MAX_TERMINAL_TEXT_BYTES, false)?;
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
        CompleteAttemptRequest, HeartbeatRequest, LeaseToken, PROTOCOL_VERSION,
        RegistrationCheckRequest, ResourceFacts, TerminalOutcome, WorkerCapability,
    };

    #[test]
    fn registration_accepts_resource_facts_without_dataset_coordination() {
        let request = RegisterSessionRequest {
            protocol_version: PROTOCOL_VERSION,
            worker_session_id: Uuid::new_v4(),
            software_version: "test".into(),
            advertised_capabilities: BTreeSet::from([WorkerCapability::Benchmark]),
            resources: ResourceFacts {
                logical_cpus: 1,
                memory_bytes: 1,
            },
        };
        assert!(request.validate().is_ok());

        let check = RegistrationCheckRequest {
            protocol_version: request.protocol_version,
            software_version: request.software_version,
            advertised_capabilities: request.advertised_capabilities,
            resources: request.resources,
        };
        assert!(check.validate().is_ok());
    }

    #[test]
    fn terminal_text_normalization_is_non_empty_single_line_and_byte_bounded() {
        assert_eq!(normalize_terminal_text("first\n\tsecond\u{7}", "fallback"), "first second");
        assert_eq!(normalize_terminal_text("\n\t", "local failure"), "local failure");

        let normalized = normalize_terminal_text(&"é".repeat(MAX_TERMINAL_TEXT_BYTES), "fallback");
        assert!(normalized.len() <= MAX_TERMINAL_TEXT_BYTES);
        assert!(!normalized.is_empty());
        assert!(
            !normalized
                .chars()
                .any(char::is_control)
        );
    }

    #[test]
    fn registration_check_reuses_registration_fact_validation() {
        let mut request = RegistrationCheckRequest {
            protocol_version: PROTOCOL_VERSION,
            software_version: "test".into(),
            advertised_capabilities: BTreeSet::from([WorkerCapability::Benchmark]),
            resources: ResourceFacts {
                logical_cpus: 1,
                memory_bytes: 1,
            },
        };
        request
            .advertised_capabilities
            .clear();
        assert!(request.validate().is_err());
        request
            .advertised_capabilities
            .insert(WorkerCapability::Benchmark);
        request.resources.logical_cpus = 0;
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
    fn registration_protocol_version_and_telemetry_bounds_are_enforced() {
        let registration = RegisterSessionRequest {
            protocol_version: PROTOCOL_VERSION + 1,
            worker_session_id: Uuid::new_v4(),
            software_version: "test".into(),
            advertised_capabilities: BTreeSet::from([WorkerCapability::Benchmark]),
            resources: ResourceFacts {
                logical_cpus: 1,
                memory_bytes: 1,
            },
        };
        assert!(matches!(registration.validate(), Err(ProtocolError::Version { .. })));

        let identity = AttemptIdentity {
            worker_session_id: Uuid::new_v4(),
            attempt_id: Uuid::new_v4(),
            fencing_generation: 1,
            lease_token: LeaseToken("x".repeat(64)),
        };
        let heartbeat = HeartbeatRequest {
            identity,
            reliable_buffer_len: 4_097,
        };
        assert!(heartbeat.validate().is_err());
    }

    #[test]
    fn secret_wrappers_redact_debug_output() {
        assert_eq!(format!("{:?}", LeaseToken("secret".into())), "LeaseToken([REDACTED])");
        assert_eq!(
            format!("{:?}", crate::RepositoryToken("secret".into())),
            "RepositoryToken([REDACTED])"
        );
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

    #[test]
    fn terminal_rejects_unbounded_or_controlled_invalid_block_identity() {
        let identity = AttemptIdentity {
            worker_session_id: Uuid::new_v4(),
            attempt_id: Uuid::new_v4(),
            fencing_generation: 1,
            lease_token: LeaseToken("x".repeat(64)),
        };
        let outcome = TerminalOutcome::Completed {
            summary: serde_json::json!({}),
            block_validation: Some(crate::BlockValidationResult {
                valid: false,
                checked_blocks: 1,
                invalid_blocks: vec![crate::InvalidBlock {
                    shard: 0,
                    block: "b".repeat(513),
                    reason: "invalid block".into(),
                }],
                chainstate_origin: "chainstate".into(),
                observed: crate::ObservedValidationIndex {
                    pre_nakamoto_count: 1,
                    nakamoto_count: 1,
                },
                resolved_range: crate::InclusiveRange { start: 1, end: 1 },
                segments: vec![crate::ValidationEpochSegment {
                    epoch: crate::ValidationEpoch::Nakamoto,
                    global_range: crate::InclusiveRange { start: 1, end: 1 },
                    local_range: crate::InclusiveRange { start: 0, end: 0 },
                }],
                shard_count: 1,
                max_concurrency: 1,
            }),
        };
        let terminal = ReliableEventPayload::Terminal {
            outcome_digest: crate::payload_digest(&outcome).unwrap(),
        };
        let mut request = CompleteAttemptRequest {
            identity,
            trace_id: Uuid::new_v4(),
            terminal_reliable_seq: 1,
            terminal_payload_digest: crate::payload_digest(&terminal).unwrap(),
            outcome,
            artifacts: Vec::new(),
        };
        assert_eq!(
            request
                .validate()
                .unwrap_err(),
            ProtocolError::Invalid {
                field: "invalid_block.block",
                reason: "must be 1..=512 non-control bytes".into(),
            }
        );

        let TerminalOutcome::Completed {
            block_validation: Some(result), ..
        } = &mut request.outcome
        else {
            unreachable!()
        };
        result.invalid_blocks[0].block = "bad\nblock".into();
        let terminal = ReliableEventPayload::Terminal {
            outcome_digest: crate::payload_digest(&request.outcome).unwrap(),
        };
        request.terminal_payload_digest = crate::payload_digest(&terminal).unwrap();
        assert!(matches!(
            request.validate(),
            Err(ProtocolError::Invalid {
                field: "invalid_block.block",
                ..
            })
        ));
    }

    #[test]
    fn block_validation_result_must_resolve_selector_and_segments_exactly() {
        let payload = crate::BlockValidationPayload {
            selection: crate::BlockValidationSelection::Range {
                range: crate::InclusiveRange { start: 8, end: 12 },
            },
            timeout_secs: 60,
        };
        let mut result = crate::BlockValidationResult {
            valid: true,
            checked_blocks: 5,
            invalid_blocks: Vec::new(),
            chainstate_origin: "nightly".into(),
            observed: crate::ObservedValidationIndex {
                pre_nakamoto_count: 10,
                nakamoto_count: 10,
            },
            resolved_range: crate::InclusiveRange { start: 8, end: 12 },
            segments: vec![
                crate::ValidationEpochSegment {
                    epoch: crate::ValidationEpoch::PreNakamoto,
                    global_range: crate::InclusiveRange { start: 8, end: 9 },
                    local_range: crate::InclusiveRange { start: 8, end: 9 },
                },
                crate::ValidationEpochSegment {
                    epoch: crate::ValidationEpoch::Nakamoto,
                    global_range: crate::InclusiveRange { start: 10, end: 12 },
                    local_range: crate::InclusiveRange { start: 0, end: 2 },
                },
            ],
            shard_count: 2,
            max_concurrency: 2,
        };
        assert!(validate_block_validation_result(&payload, &result).is_ok());

        result.segments[1]
            .local_range
            .start = 1;
        assert!(matches!(
            validate_block_validation_result(&payload, &result),
            Err(ProtocolError::Invalid {
                field: "block_validation.segments",
                ..
            })
        ));

        result.observed = crate::ObservedValidationIndex {
            pre_nakamoto_count: i64::MAX as u64,
            nakamoto_count: 1,
        };
        assert!(matches!(
            validate_block_validation_result(&payload, &result),
            Err(ProtocolError::Invalid {
                field: "block_validation.observed",
                ..
            })
        ));
    }
}
