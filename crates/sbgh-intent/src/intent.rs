//! Intent-resolution DTOs and validation.
//!
//! The model-facing JSON is deliberately separate from [`WorkloadSpec`]: it can
//! represent an invalid/ambiguous request, and it uses a strict-schema-friendly
//! object shape. Daemon validation then normalizes it into the internal
//! workload type consumed by task-submission adapters.

use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;

use sbgh_core::workload::{
    BenchmarkRequest, BlockSelector, ComparisonRequest, ComparisonVariant, WorkloadSpec,
    WorkloadTarget,
};

const MAX_INVALID_REASON_CHARS: usize = 512;
const MAX_INVALID_ISSUES: usize = 7;
const MAX_INVALID_ISSUE_MESSAGE_CHARS: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ResolutionStatus {
    Resolved,
    Invalid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CreationTaskKind {
    Benchmark,
    BlockValidation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TargetKind {
    Block,
    BlockRange,
    Txids,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum BlockSelectorKind {
    Height,
    Hash,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum IntentIssueField {
    Task,
    Target,
    Block,
    BlockRange,
    Txids,
    Repetitions,
    Warmup,
    Rev,
    VariantRefs,
    Repository,
    ValidationSelection,
    ValidationBlockCount,
    ValidationRange,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum IntentIssueCode {
    Missing,
    Invalid,
    Ambiguous,
    Unsupported,
    NeedsContext,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct IntentIssueJson {
    pub field: IntentIssueField,
    pub code: IntentIssueCode,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct IntentBlockSelectorJson {
    pub kind: BlockSelectorKind,
    pub height: Option<u64>,
    pub hash: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct IntentBlockRangeJson {
    pub start: u64,
    pub end: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ValidationSelectionKind {
    Recent,
    Full,
    Range,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct IntentResolutionJson {
    pub status: ResolutionStatus,
    pub task_kind: Option<CreationTaskKind>,
    pub target_kind: Option<TargetKind>,
    pub block: Option<Vec<IntentBlockSelectorJson>>,
    pub block_range: Option<IntentBlockRangeJson>,
    pub txids: Option<Vec<String>>,
    pub repetitions: Option<u32>,
    pub warmup: Option<u32>,
    pub rev: Option<String>,
    pub variant_refs: Option<Vec<String>>,
    pub repository: Option<String>,
    pub validation_selection: Option<ValidationSelectionKind>,
    pub validation_block_count: Option<u64>,
    pub validation_start: Option<u64>,
    pub validation_end: Option<u64>,
    pub reason: Option<String>,
    pub issues: Option<Vec<IntentIssueJson>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestedSource {
    pub repository: Option<String>,
    pub revision: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationSelection {
    Recent { block_count: Option<u64> },
    Full,
    Range { start: u64, end: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockValidationIntent {
    pub source: RequestedSource,
    pub selection: ValidationSelection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskCreationIntent {
    Benchmark(BenchmarkRequest),
    BlockValidation(BlockValidationIntent),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserIntent {
    Create(TaskCreationIntent),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntentInvalid {
    pub reason: String,
    pub issues: Vec<IntentIssueJson>,
}

impl IntentInvalid {
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
            issues: Vec::new(),
        }
    }

    pub fn user_message(&self) -> String {
        if self.issues.is_empty() {
            return self.reason.clone();
        }
        let mut msg = self.reason.clone();
        for issue in &self.issues {
            msg.push_str("\n• ");
            msg.push_str(issue.field.label());
            msg.push_str(": ");
            msg.push_str(issue.message.trim());
        }
        msg
    }
}

impl From<String> for IntentInvalid {
    fn from(reason: String) -> Self {
        Self::new(reason)
    }
}

impl From<&str> for IntentInvalid {
    fn from(reason: &str) -> Self {
        Self::new(reason)
    }
}

impl IntentIssueField {
    fn label(self) -> &'static str {
        match self {
            Self::Task => "task",
            Self::Target => "target",
            Self::Block => "block",
            Self::BlockRange => "block range",
            Self::Txids => "txids",
            Self::Repetitions => "repetitions",
            Self::Warmup => "warmup",
            Self::Rev => "rev",
            Self::VariantRefs => "variant refs",
            Self::Repository => "repository",
            Self::ValidationSelection => "validation selection",
            Self::ValidationBlockCount => "validation block count",
            Self::ValidationRange => "validation range",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntentOutcome {
    Resolved(UserIntent),
    Invalid(IntentInvalid),
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum IntentValidationError {
    #[error("invalid request: {0}")]
    InvalidShape(String),
    #[error("invalid value `{value}` for `{field}`: {reason}")]
    InvalidValue { field: &'static str, value: String, reason: String },
}

#[derive(Debug, Error)]
pub enum IntentProviderError {
    #[error("{0}")]
    Message(String),
}

#[async_trait]
pub trait IntentResolver: Send + Sync {
    async fn resolve(&self, text: &str) -> Result<IntentOutcome, IntentProviderError>;
}

pub fn validate_intent_resolution(
    intent: IntentResolutionJson,
) -> Result<IntentOutcome, IntentValidationError> {
    match intent.status {
        ResolutionStatus::Invalid => validate_invalid_intent(intent),
        ResolutionStatus::Resolved => validate_resolved_intent(intent),
    }
}

fn validate_invalid_intent(
    intent: IntentResolutionJson,
) -> Result<IntentOutcome, IntentValidationError> {
    if intent.task_kind.is_some()
        || intent.target_kind.is_some()
        || intent.block.is_some()
        || intent.block_range.is_some()
        || intent.txids.is_some()
        || intent.repetitions.is_some()
        || intent.warmup.is_some()
        || intent.rev.is_some()
        || intent.variant_refs.is_some()
        || intent.repository.is_some()
        || intent
            .validation_selection
            .is_some()
        || intent
            .validation_block_count
            .is_some()
        || intent
            .validation_start
            .is_some()
        || intent
            .validation_end
            .is_some()
    {
        return Err(IntentValidationError::InvalidShape(
            "invalid status must not carry target or run fields".into(),
        ));
    }
    let reason = truncate_chars(&non_empty(intent.reason, "reason")?, MAX_INVALID_REASON_CHARS);
    let issues = validate_issues(intent.issues)?;
    Ok(IntentOutcome::Invalid(IntentInvalid { reason, issues }))
}

fn validate_resolved_intent(
    intent: IntentResolutionJson,
) -> Result<IntentOutcome, IntentValidationError> {
    let task_kind = required(intent.task_kind, "task_kind")?;
    match task_kind {
        CreationTaskKind::Benchmark => validate_resolved_benchmark(intent),
        CreationTaskKind::BlockValidation => validate_resolved_block_validation(intent),
    }
}

fn validate_resolved_benchmark(
    intent: IntentResolutionJson,
) -> Result<IntentOutcome, IntentValidationError> {
    require_absent(intent.repository, "repository")?;
    require_absent(intent.validation_selection, "validation_selection")?;
    require_absent(intent.validation_block_count, "validation_block_count")?;
    require_absent(intent.validation_start, "validation_start")?;
    require_absent(intent.validation_end, "validation_end")?;
    let target_kind = intent
        .target_kind
        .ok_or_else(|| {
            IntentValidationError::InvalidShape("resolved status needs target_kind".into())
        })?;
    let repetitions = required(intent.repetitions, "repetitions")?;
    if repetitions == 0 {
        return Err(IntentValidationError::InvalidValue {
            field: "repetitions",
            value: repetitions.to_string(),
            reason: "must be at least 1".into(),
        });
    }
    let warmup = required(intent.warmup, "warmup")?;
    let rev = normalize_optional_text(intent.rev);
    let variant_refs = normalize_variant_refs(intent.variant_refs)?;

    let target = match target_kind {
        TargetKind::Block => {
            require_absent(intent.block_range, "block_range")?;
            require_absent(intent.txids, "txids")?;
            let selectors = required(intent.block, "block")?;
            if selectors.is_empty() {
                return Err(IntentValidationError::InvalidShape(
                    "block target needs at least one selector".into(),
                ));
            }
            WorkloadTarget::Blocks(
                selectors
                    .into_iter()
                    .map(validate_block_selector)
                    .collect::<Result<Vec<_>, _>>()?,
            )
        }
        TargetKind::BlockRange => {
            require_absent(intent.block, "block")?;
            require_absent(intent.txids, "txids")?;
            let range = required(intent.block_range, "block_range")?;
            if range.start > range.end {
                return Err(IntentValidationError::InvalidValue {
                    field: "block_range",
                    value: format!("{}..{}", range.start, range.end),
                    reason: "start must be <= end".into(),
                });
            }
            WorkloadTarget::BlockRange {
                start: range.start,
                end: range.end,
            }
        }
        TargetKind::Txids => {
            require_absent(intent.block, "block")?;
            require_absent(intent.block_range, "block_range")?;
            let txids = required(intent.txids, "txids")?;
            if txids.is_empty() {
                return Err(IntentValidationError::InvalidShape(
                    "txids target needs at least one txid".into(),
                ));
            }
            WorkloadTarget::Txids(
                txids
                    .into_iter()
                    .map(|t| normalize_32_byte_hex("txids", &t, "txid"))
                    .collect::<Result<Vec<_>, _>>()?,
            )
        }
    };

    let workload = WorkloadSpec {
        target,
        clean_repetitions: repetitions,
        warmup: Some(warmup),
        rev,
    };
    if let Some(variants) = variant_refs {
        if workload.rev.is_some() {
            return Err(IntentValidationError::InvalidShape(
                "comparison requests must use variant_refs instead of rev".into(),
            ));
        }
        return Ok(IntentOutcome::Resolved(UserIntent::Create(TaskCreationIntent::Benchmark(
            BenchmarkRequest::Comparison(ComparisonRequest { workload, variants }),
        ))));
    }
    Ok(IntentOutcome::Resolved(UserIntent::Create(TaskCreationIntent::Benchmark(
        BenchmarkRequest::Single(workload),
    ))))
}

fn validate_resolved_block_validation(
    intent: IntentResolutionJson,
) -> Result<IntentOutcome, IntentValidationError> {
    require_absent(intent.target_kind, "target_kind")?;
    require_absent(intent.block, "block")?;
    require_absent(intent.block_range, "block_range")?;
    require_absent(intent.txids, "txids")?;
    require_absent(intent.repetitions, "repetitions")?;
    require_absent(intent.warmup, "warmup")?;
    require_absent(intent.variant_refs, "variant_refs")?;

    let source = RequestedSource {
        repository: normalize_bounded_selector(intent.repository, "repository")?,
        revision: normalize_bounded_selector(intent.rev, "rev")?,
    };
    let selection = match (
        required(intent.validation_selection, "validation_selection")?,
        intent.validation_block_count,
        intent.validation_start,
        intent.validation_end,
    ) {
        (ValidationSelectionKind::Recent, block_count, None, None) => {
            if block_count == Some(0) {
                return Err(IntentValidationError::InvalidValue {
                    field: "validation_block_count",
                    value: "0".into(),
                    reason: "must be at least 1".into(),
                });
            }
            ValidationSelection::Recent { block_count }
        }
        (ValidationSelectionKind::Full, None, None, None) => ValidationSelection::Full,
        (ValidationSelectionKind::Range, None, Some(start), Some(end)) => {
            if start > end {
                return Err(IntentValidationError::InvalidValue {
                    field: "validation_range",
                    value: format!("{start}..{end}"),
                    reason: "start must be <= end".into(),
                });
            }
            ValidationSelection::Range { start, end }
        }
        _ => {
            return Err(IntentValidationError::InvalidShape(
                "validation selector fields do not match the selected recent/full/range mode"
                    .into(),
            ));
        }
    };
    Ok(IntentOutcome::Resolved(UserIntent::Create(TaskCreationIntent::BlockValidation(
        BlockValidationIntent { source, selection },
    ))))
}

fn normalize_bounded_selector(
    value: Option<String>,
    field: &'static str,
) -> Result<Option<String>, IntentValidationError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let value = value.trim();
    if value.is_empty() {
        return Err(IntentValidationError::InvalidValue {
            field,
            value: String::new(),
            reason: "must be non-empty when provided".into(),
        });
    }
    if value.chars().count() > 255 {
        return Err(IntentValidationError::InvalidValue {
            field,
            value: "<overlong>".into(),
            reason: "must be at most 255 characters".into(),
        });
    }
    Ok(Some(value.into()))
}

fn validate_issues(
    issues: Option<Vec<IntentIssueJson>>,
) -> Result<Vec<IntentIssueJson>, IntentValidationError> {
    let mut out = Vec::new();
    for issue in issues
        .unwrap_or_default()
        .into_iter()
        .take(MAX_INVALID_ISSUES)
    {
        if issue
            .message
            .trim()
            .is_empty()
        {
            return Err(IntentValidationError::InvalidShape(
                "issue.message must be non-empty".into(),
            ));
        }
        let message = truncate_chars(issue.message.trim(), MAX_INVALID_ISSUE_MESSAGE_CHARS);
        out.push(IntentIssueJson { message, ..issue });
    }
    Ok(out)
}

fn truncate_chars(s: &str, limit: usize) -> String {
    if s.chars().count() <= limit {
        return s.to_string();
    }
    let mut out: String = s
        .chars()
        .take(limit.saturating_sub(1))
        .collect();
    out.push('…');
    out
}

fn validate_block_selector(
    selector: IntentBlockSelectorJson,
) -> Result<BlockSelector, IntentValidationError> {
    match selector.kind {
        BlockSelectorKind::Height => {
            let height = required(selector.height, "block.height")?;
            require_absent(selector.hash, "block.hash")?;
            Ok(BlockSelector::Height(height))
        }
        BlockSelectorKind::Hash => {
            require_absent(selector.height, "block.height")?;
            let hash = required(selector.hash, "block.hash")?;
            Ok(BlockSelector::Hash(normalize_32_byte_hex("block.hash", &hash, "block hash")?))
        }
    }
}

pub fn normalize_32_byte_hex(
    field: &'static str,
    value: &str,
    label: &str,
) -> Result<String, IntentValidationError> {
    let raw = value.trim();
    let hex = raw
        .strip_prefix("0x")
        .or_else(|| raw.strip_prefix("0X"))
        .unwrap_or(raw);
    if hex.len() != 64 {
        return Err(IntentValidationError::InvalidValue {
            field,
            value: value.to_string(),
            reason: format!("expected a 64-character hex {label}"),
        });
    }
    if !hex
        .bytes()
        .all(|b| b.is_ascii_hexdigit())
    {
        return Err(IntentValidationError::InvalidValue {
            field,
            value: value.to_string(),
            reason: "expected hex digits".into(),
        });
    }
    Ok(hex.to_ascii_lowercase())
}

fn required<T>(value: Option<T>, field: &'static str) -> Result<T, IntentValidationError> {
    value.ok_or_else(|| IntentValidationError::InvalidShape(format!("{field} is required")))
}

fn require_absent<T>(value: Option<T>, field: &'static str) -> Result<(), IntentValidationError> {
    if value.is_some() {
        return Err(IntentValidationError::InvalidShape(format!("{field} must be null")));
    }
    Ok(())
}

fn non_empty(value: Option<String>, field: &'static str) -> Result<String, IntentValidationError> {
    let value = required(value, field)?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(IntentValidationError::InvalidShape(format!("{field} must be non-empty")));
    }
    Ok(trimmed.to_string())
}

fn normalize_optional_text(value: Option<String>) -> Option<String> {
    value.and_then(|v| {
        let trimmed = v.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    })
}

fn normalize_variant_refs(
    value: Option<Vec<String>>,
) -> Result<Option<Vec<ComparisonVariant>>, IntentValidationError> {
    let Some(values) = value else {
        return Ok(None);
    };
    if values.len() < 2 {
        return Err(IntentValidationError::InvalidShape(
            "variant_refs needs at least two refs".into(),
        ));
    }
    let mut variants = Vec::with_capacity(values.len());
    for value in values {
        let rev = value.trim().to_string();
        if rev.is_empty() {
            return Err(IntentValidationError::InvalidShape(
                "variant_refs entries must be non-empty".into(),
            ));
        }
        variants.push(ComparisonVariant { rev });
    }
    Ok(Some(variants))
}

/// JSON schema body for OpenAI `text.format` (`type = json_schema`).
///
/// This is intentionally pinned rather than relying solely on generated schema:
/// OpenAI strict outputs require all fields to be present, nullable fields for
/// optional values, no root union, and `additionalProperties: false`.
pub fn intent_response_text_format() -> Value {
    json!({
        "type": "json_schema",
        "name": "sbgh_task_creation_intent",
        "strict": true,
        "schema": {
            "type": "object",
            "additionalProperties": false,
            "required": [
                "status",
                "task_kind",
                "target_kind",
                "block",
                "block_range",
                "txids",
                "repetitions",
                "warmup",
                "rev",
                "variant_refs",
                "repository",
                "validation_selection",
                "validation_block_count",
                "validation_start",
                "validation_end",
                "reason",
                "issues"
            ],
            "properties": {
                "status": { "type": "string", "enum": ["resolved", "invalid"] },
                "task_kind": {
                    "type": ["string", "null"],
                    "enum": ["benchmark", "block_validation", null]
                },
                "target_kind": {
                    "type": ["string", "null"],
                    "enum": ["block", "block_range", "txids", null],
                    "description": "Benchmark target only. Must be null for block_validation, even when the request says blocks."
                },
                "block": {
                    "type": ["array", "null"],
                    "description": "Benchmark block selectors only. Must be null for block_validation.",
                    "items": {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["kind", "height", "hash"],
                        "properties": {
                            "kind": { "type": "string", "enum": ["height", "hash"] },
                            "height": { "type": ["integer", "null"], "minimum": 0 },
                            "hash": { "type": ["string", "null"] }
                        }
                    }
                },
                "block_range": {
                    "type": ["object", "null"],
                    "description": "Benchmark block range only. Must be null for block_validation; use validation_start and validation_end instead.",
                    "additionalProperties": false,
                    "required": ["start", "end"],
                    "properties": {
                        "start": { "type": "integer", "minimum": 0 },
                        "end": { "type": "integer", "minimum": 0 }
                    }
                },
                "txids": {
                    "type": ["array", "null"],
                    "description": "Benchmark transaction selectors only. Must be null for block_validation.",
                    "items": { "type": "string" }
                },
                "repetitions": {
                    "type": ["integer", "null"],
                    "minimum": 1,
                    "description": "Benchmark-only repetition count. Must be null for block_validation."
                },
                "warmup": {
                    "type": ["integer", "null"],
                    "minimum": 0,
                    "description": "Benchmark-only warmup count. Must be null for block_validation."
                },
                "rev": {
                    "type": ["string", "null"],
                    "description": "Single benchmark ref. Use this for one-ref requests phrased as on/against <ref>."
                },
                "variant_refs": {
                    "type": ["array", "null"],
                    "description": "Exactly two refs only for explicit comparison requests; otherwise null.",
                    "minItems": 2,
                    "maxItems": 2,
                    "items": { "type": "string" }
                },
                "repository": {
                    "type": ["string", "null"],
                    "description": "Optional repository selector for block validation; null uses configured repository."
                },
                "validation_selection": {
                    "type": ["string", "null"],
                    "enum": ["recent", "full", "range", null],
                    "description": "Block-validation selector only. Must be null for benchmark."
                },
                "validation_block_count": {
                    "type": ["integer", "null"],
                    "minimum": 1,
                    "description": "Recent block-validation count. Use this, not target_kind, for requests such as latest 10 blocks."
                },
                "validation_start": {
                    "type": ["integer", "null"],
                    "minimum": 0,
                    "description": "Inclusive block-validation range start; null outside range selection."
                },
                "validation_end": {
                    "type": ["integer", "null"],
                    "minimum": 0,
                    "description": "Inclusive block-validation range end; null outside range selection."
                },
                "reason": { "type": ["string", "null"] },
                "issues": {
                    "type": ["array", "null"],
                    "items": {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["field", "code", "message"],
                        "properties": {
                            "field": {
                                "type": "string",
                                "enum": [
                                    "task",
                                    "target",
                                    "block",
                                    "block_range",
                                    "txids",
                                    "repetitions",
                                    "warmup",
                                    "rev",
                                    "variant_refs",
                                    "repository",
                                    "validation_selection",
                                    "validation_block_count",
                                    "validation_range"
                                ]
                            },
                            "code": {
                                "type": "string",
                                "enum": [
                                    "missing",
                                    "invalid",
                                    "ambiguous",
                                    "unsupported",
                                    "needs_context"
                                ]
                            },
                            "message": { "type": "string" }
                        }
                    }
                }
            }
        }
    })
}

/// Generated schema smoke check. The exact provider payload is
/// [`intent_response_text_format`], but deriving [`JsonSchema`] keeps the DTOs
/// honest as they evolve.
#[cfg(test)]
pub fn generated_intent_schema() -> Value {
    serde_json::to_value(schemars::schema_for!(IntentResolutionJson)).expect("schema serializes")
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvalExpected {
    Resolved,
    Invalid,
}

/// A resolved-intent assertion, run after normalization.
///
/// Status alone cannot catch a model that resolves the right task kind with the
/// wrong fields, so the fixtures that guard known misresolutions carry one.
#[cfg(test)]
pub type IntentCheck = fn(&UserIntent) -> Result<(), String>;

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
pub struct IntentEvalFixture {
    pub name: &'static str,
    pub prompt: &'static str,
    pub expected: EvalExpected,
    pub check: Option<IntentCheck>,
}

#[cfg(test)]
impl IntentEvalFixture {
    pub const fn resolved(name: &'static str, prompt: &'static str) -> Self {
        Self {
            name,
            prompt,
            expected: EvalExpected::Resolved,
            check: None,
        }
    }

    pub const fn resolved_as(name: &'static str, prompt: &'static str, check: IntentCheck) -> Self {
        Self {
            name,
            prompt,
            expected: EvalExpected::Resolved,
            check: Some(check),
        }
    }

    pub const fn invalid(name: &'static str, prompt: &'static str) -> Self {
        Self {
            name,
            prompt,
            expected: EvalExpected::Invalid,
            check: None,
        }
    }
}

#[cfg(test)]
fn expect_block_validation(intent: &UserIntent) -> Result<&BlockValidationIntent, String> {
    let UserIntent::Create(TaskCreationIntent::BlockValidation(validation)) = intent else {
        return Err("expected a block-validation intent".into());
    };
    Ok(validation)
}

#[cfg(test)]
fn expect_benchmark(intent: &UserIntent) -> Result<&BenchmarkRequest, String> {
    let UserIntent::Create(TaskCreationIntent::Benchmark(benchmark)) = intent else {
        return Err("expected a benchmark intent".into());
    };
    Ok(benchmark)
}

/// The v32 Slack misresolution: "latest 10 blocks" resolved as a ten-block
/// benchmark target instead of a recent-block validation.
#[cfg(test)]
fn recent_ten_blocks_at_commit(intent: &UserIntent) -> Result<(), String> {
    let validation = expect_block_validation(intent)?;
    if validation.selection != (ValidationSelection::Recent { block_count: Some(10) }) {
        return Err(format!("expected recent 10 blocks, got {:?}", validation.selection));
    }
    if validation
        .source
        .revision
        .as_deref()
        != Some("1ed2021d9209f1ba7d4d9c9a763296c41f9194bb")
    {
        return Err(format!("expected the requested commit, got {:?}", validation.source.revision));
    }
    Ok(())
}

#[cfg(test)]
fn full_validation_at_commit(intent: &UserIntent) -> Result<(), String> {
    let validation = expect_block_validation(intent)?;
    if validation.selection != ValidationSelection::Full {
        return Err(format!("expected a full validation, got {:?}", validation.selection));
    }
    if validation
        .source
        .revision
        .as_deref()
        != Some("abc123")
    {
        return Err(format!("expected commit abc123, got {:?}", validation.source.revision));
    }
    Ok(())
}

#[cfg(test)]
fn validation_index_range(intent: &UserIntent) -> Result<(), String> {
    let validation = expect_block_validation(intent)?;
    if validation.selection != (ValidationSelection::Range { start: 185_700, end: 186_000 }) {
        return Err(format!("expected range 185700..186000, got {:?}", validation.selection));
    }
    Ok(())
}

/// "10k blocks from height 8500000" is an inclusive range, not a repetition
/// count, and a single `against <ref>` is not a comparison.
#[cfg(test)]
fn ten_thousand_block_range_single_ref(intent: &UserIntent) -> Result<(), String> {
    let BenchmarkRequest::Single(spec) = expect_benchmark(intent)? else {
        return Err("expected a single-ref benchmark, not a comparison".into());
    };
    if spec.target
        != (WorkloadTarget::BlockRange {
            start: 8_500_000,
            end: 8_509_999,
        })
    {
        return Err(format!("expected an inclusive 10k block range, got {:?}", spec.target));
    }
    if spec.clean_repetitions != 2 || spec.warmup != Some(2_500) {
        return Err(format!(
            "expected 2 repetitions and 2500 warmup, got {} and {:?}",
            spec.clean_repetitions, spec.warmup
        ));
    }
    if spec.rev.as_deref() != Some("sb-integration/squash") {
        return Err(format!("expected the requested ref, got {:?}", spec.rev));
    }
    Ok(())
}

/// An unqualified benchmark must still carry the documented run defaults.
#[cfg(test)]
fn single_block_with_default_runs(intent: &UserIntent) -> Result<(), String> {
    let BenchmarkRequest::Single(spec) = expect_benchmark(intent)? else {
        return Err("expected a single benchmark".into());
    };
    if spec.target != WorkloadTarget::Blocks(vec![BlockSelector::Height(8_123_456)]) {
        return Err(format!("expected one block-height target, got {:?}", spec.target));
    }
    if spec.clean_repetitions != 1 || spec.warmup != Some(0) {
        return Err(format!(
            "expected the default 1 repetition and 0 warmup, got {} and {:?}",
            spec.clean_repetitions, spec.warmup
        ));
    }
    Ok(())
}

#[cfg(test)]
pub const EVAL_FIXTURES: &[IntentEvalFixture] = &[
    // Resolved fixtures must include enough concrete information for the
    // current Slack surface. Contextual phrases like "this branch" stay invalid
    // until a future caller supplies PR/comment context.
    IntentEvalFixture::resolved_as(
        "openai_live_eval_benchmark_single_block_defaults",
        "bench block 8123456",
        single_block_with_default_runs,
    ),
    IntentEvalFixture::resolved(
        "openai_live_eval_benchmark_repetition_words",
        "profile block 8123456 ten times",
    ),
    IntentEvalFixture::resolved(
        "openai_live_eval_benchmark_block_range_single_ref",
        "bench blocks 8123456 to 8200000 on 3.4.0.0.3",
    ),
    IntentEvalFixture::resolved_as(
        "openai_live_eval_benchmark_compact_counts_and_warmup",
        "bench 10k blocks from height 8500000, twice, with a 2.5k block warmup, against \
         sb-integration/squash",
        ten_thousand_block_range_single_ref,
    ),
    IntentEvalFixture::resolved(
        "openai_live_eval_benchmark_run_range",
        "run 10 reps with 1000 warmup from 8123456 to 8200000",
    ),
    IntentEvalFixture::resolved(
        "openai_live_eval_benchmark_txid",
        "profile tx f426738843949f576e4eff5ffbb148de9e1a638d20a03c6447cc70490f5156ce twice",
    ),
    IntentEvalFixture::invalid(
        "openai_live_eval_invalid_contextual_tx",
        "bench this tx with 2 warmup iterations",
    ),
    IntentEvalFixture::invalid(
        "openai_live_eval_invalid_bare_hash",
        "benchmark 0xc3b1aad400000000000000000000000000000000000000000000000000000000 on develop",
    ),
    IntentEvalFixture::invalid(
        "openai_live_eval_invalid_contextual_branch",
        "run this branch against block 8123456",
    ),
    IntentEvalFixture::invalid("openai_live_eval_invalid_ambiguous_target", "bench something fast"),
    IntentEvalFixture::invalid(
        "openai_live_eval_invalid_comparison_without_target",
        "compare main and develop",
    ),
    IntentEvalFixture::invalid("openai_live_eval_invalid_malformed_txid", "bench tx not-a-hash"),
    IntentEvalFixture::invalid(
        "openai_live_eval_invalid_reversed_range",
        "bench block 8200000 to 8123456",
    ),
    IntentEvalFixture::invalid(
        "openai_live_eval_invalid_zero_repetitions",
        "bench block 8123456 zero times",
    ),
    IntentEvalFixture::resolved(
        "openai_live_eval_benchmark_txid_single_ref",
        "bench tx f426738843949f576e4eff5ffbb148de9e1a638d20a03c6447cc70490f5156ce on \
         feat/stacks-bench",
    ),
    IntentEvalFixture::invalid(
        "openai_live_eval_invalid_missing_benchmark_target",
        "please run a benchmark",
    ),
    IntentEvalFixture::resolved(
        "openai_live_eval_block_validation_default_selection",
        "run block validation on commit abc123",
    ),
    IntentEvalFixture::resolved(
        "openai_live_eval_block_validation_recent_compact_count",
        "validate the latest 500k blocks on commit abc123",
    ),
    IntentEvalFixture::resolved_as(
        "openai_live_eval_block_validation_recent_exact_commit",
        "validate the latest 10 blocks on commit 1ed2021d9209f1ba7d4d9c9a763296c41f9194bb",
        recent_ten_blocks_at_commit,
    ),
    IntentEvalFixture::resolved_as(
        "openai_live_eval_block_validation_full",
        "run full block validation on commit abc123",
        full_validation_at_commit,
    ),
    IntentEvalFixture::resolved_as(
        "openai_live_eval_block_validation_range",
        "validate global validation indices 185700 through 186000 on abc123",
        validation_index_range,
    ),
    IntentEvalFixture::invalid(
        "openai_live_eval_invalid_contextual_validation",
        "validate this change",
    ),
    IntentEvalFixture::invalid(
        "openai_live_eval_invalid_mixed_task_request",
        "cancel validation and benchmark the replacement",
    ),
];

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntentEvalReport {
    pub total: usize,
    pub passed: usize,
    pub failures: Vec<String>,
}

#[cfg(test)]
pub async fn run_eval_fixtures(
    resolver: &dyn IntentResolver,
    fixtures: &[IntentEvalFixture],
) -> Result<IntentEvalReport, IntentProviderError> {
    let mut failures = Vec::new();
    for fixture in fixtures {
        let outcome = resolver
            .resolve(fixture.prompt)
            .await?;
        let outcome_detail = format!("{outcome:?}");
        let actual = match outcome {
            IntentOutcome::Resolved(_) => EvalExpected::Resolved,
            IntentOutcome::Invalid(_) => EvalExpected::Invalid,
        };
        if actual != fixture.expected {
            failures.push(format!(
                "prompt `{}` expected {:?}, got {outcome_detail}",
                fixture.prompt, fixture.expected
            ));
            continue;
        }
        // A structurally valid resolution can still carry the wrong task
        // fields, so guarded fixtures assert the normalized intent too.
        if let (Some(check), IntentOutcome::Resolved(intent)) = (fixture.check, &outcome)
            && let Err(mismatch) = check(intent)
        {
            failures.push(format!("prompt `{}` resolved wrongly: {mismatch}", fixture.prompt));
        }
    }
    Ok(IntentEvalReport {
        total: fixtures.len(),
        passed: fixtures
            .len()
            .saturating_sub(failures.len()),
        failures,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    const TXID: &str = "0xf426738843949f576e4eff5ffbb148de9e1a638d20a03c6447cc70490f5156ce";
    const HASH: &str = "0xC3B1AAD400000000000000000000000000000000000000000000000000000000";

    fn resolved_base(target_kind: TargetKind) -> IntentResolutionJson {
        IntentResolutionJson {
            status: ResolutionStatus::Resolved,
            task_kind: Some(CreationTaskKind::Benchmark),
            target_kind: Some(target_kind),
            block: None,
            block_range: None,
            txids: None,
            repetitions: Some(1),
            warmup: Some(0),
            rev: None,
            variant_refs: None,
            repository: None,
            validation_selection: None,
            validation_block_count: None,
            validation_start: None,
            validation_end: None,
            reason: None,
            issues: None,
        }
    }

    #[test]
    fn validates_block_height_and_hash_selectors() {
        let mut intent = resolved_base(TargetKind::Block);
        intent.block = Some(vec![
            IntentBlockSelectorJson {
                kind: BlockSelectorKind::Height,
                height: Some(8123456),
                hash: None,
            },
            IntentBlockSelectorJson {
                kind: BlockSelectorKind::Hash,
                height: None,
                hash: Some(HASH.into()),
            },
        ]);
        let IntentOutcome::Resolved(UserIntent::Create(TaskCreationIntent::Benchmark(
            BenchmarkRequest::Single(spec),
        ))) = validate_intent_resolution(intent).unwrap()
        else {
            panic!("expected resolved");
        };
        assert_eq!(spec.clean_repetitions, 1);
        assert_eq!(
            spec.target,
            WorkloadTarget::Blocks(vec![
                BlockSelector::Height(8123456),
                BlockSelector::Hash(
                    "c3b1aad400000000000000000000000000000000000000000000000000000000".into()
                )
            ])
        );
        assert_eq!(spec.to_bench_args()[..2], ["--block", "8123456"]);
    }

    #[test]
    fn validates_block_range_as_start_at_count() {
        let mut intent = resolved_base(TargetKind::BlockRange);
        intent.block_range = Some(IntentBlockRangeJson { start: 10, end: 12 });
        intent.repetitions = Some(3);
        intent.warmup = Some(2);
        intent.rev = Some(" develop ".into());
        let IntentOutcome::Resolved(UserIntent::Create(TaskCreationIntent::Benchmark(
            BenchmarkRequest::Single(spec),
        ))) = validate_intent_resolution(intent).unwrap()
        else {
            panic!("expected resolved");
        };
        assert_eq!(spec.clean_repetitions, 3);
        assert_eq!(spec.target, WorkloadTarget::BlockRange { start: 10, end: 12 });
        assert_eq!(spec.to_bench_args(), vec!["--start-at", "10", "--count", "3", "--warmup", "2"]);
        assert_eq!(spec.rev.as_deref(), Some("develop"));
    }

    #[test]
    fn validates_single_ref_block_range_from_against_phrase_shape() {
        let mut intent = resolved_base(TargetKind::BlockRange);
        intent.block_range = Some(IntentBlockRangeJson {
            start: 8_500_000,
            end: 8_509_999,
        });
        intent.repetitions = Some(2);
        intent.warmup = Some(2_500);
        intent.rev = Some("sb-integration/squash".into());
        intent.variant_refs = None;

        let IntentOutcome::Resolved(UserIntent::Create(TaskCreationIntent::Benchmark(
            BenchmarkRequest::Single(spec),
        ))) = validate_intent_resolution(intent).unwrap()
        else {
            panic!("expected single-ref benchmark");
        };

        assert_eq!(spec.clean_repetitions, 2);
        assert_eq!(spec.warmup, Some(2_500));
        assert_eq!(
            spec.target,
            WorkloadTarget::BlockRange {
                start: 8_500_000,
                end: 8_509_999,
            }
        );
        assert_eq!(spec.rev.as_deref(), Some("sb-integration/squash"));
    }

    #[test]
    fn validates_txids_and_strips_prefix() {
        let mut intent = resolved_base(TargetKind::Txids);
        intent.txids = Some(vec![TXID.into()]);
        let IntentOutcome::Resolved(UserIntent::Create(TaskCreationIntent::Benchmark(
            BenchmarkRequest::Single(spec),
        ))) = validate_intent_resolution(intent).unwrap()
        else {
            panic!("expected resolved");
        };
        assert_eq!(
            spec.target,
            WorkloadTarget::Txids(vec![
                "f426738843949f576e4eff5ffbb148de9e1a638d20a03c6447cc70490f5156ce".into()
            ])
        );
    }

    #[test]
    fn resolved_status_ignores_diagnostic_fields() {
        let mut intent = resolved_base(TargetKind::Txids);
        intent.txids = Some(vec![TXID.into()]);
        intent.repetitions = Some(5);
        intent.warmup = Some(2);
        intent.rev = Some("sb-integration/squash".into());
        intent.reason = Some("model-generated summary".into());
        intent.issues = Some(vec![IntentIssueJson {
            field: IntentIssueField::Target,
            code: IntentIssueCode::NeedsContext,
            message: "diagnostic noise from resolved response".into(),
        }]);

        let IntentOutcome::Resolved(UserIntent::Create(TaskCreationIntent::Benchmark(
            BenchmarkRequest::Single(spec),
        ))) = validate_intent_resolution(intent).unwrap()
        else {
            panic!("expected resolved");
        };

        assert_eq!(spec.clean_repetitions, 5);
        assert_eq!(spec.warmup, Some(2));
        assert_eq!(spec.rev.as_deref(), Some("sb-integration/squash"));
    }

    #[test]
    fn validates_comparison_variant_refs() {
        let mut intent = resolved_base(TargetKind::Txids);
        intent.txids = Some(vec![TXID.into()]);
        intent.repetitions = Some(5);
        intent.warmup = Some(10);
        intent.variant_refs =
            Some(vec![" sb-integration/3.4.0.0.2 ".into(), "sb-integration/3.4.0.0.3".into()]);
        let IntentOutcome::Resolved(UserIntent::Create(TaskCreationIntent::Benchmark(
            BenchmarkRequest::Comparison(comparison),
        ))) = validate_intent_resolution(intent).unwrap()
        else {
            panic!("expected comparison");
        };
        assert_eq!(comparison.workload.rev, None);
        assert_eq!(
            comparison
                .workload
                .clean_repetitions,
            5
        );
        assert_eq!(comparison.workload.warmup, Some(10));
        assert_eq!(
            comparison
                .variants
                .iter()
                .map(|v| v.rev.as_str())
                .collect::<Vec<_>>(),
            vec!["sb-integration/3.4.0.0.2", "sb-integration/3.4.0.0.3"]
        );
    }

    #[test]
    fn comparison_intent_rejects_rev_field() {
        let mut intent = resolved_base(TargetKind::Txids);
        intent.txids = Some(vec![TXID.into()]);
        intent.rev = Some("baseline".into());
        intent.variant_refs = Some(vec!["baseline".into(), "candidate".into()]);
        assert!(matches!(
            validate_intent_resolution(intent),
            Err(IntentValidationError::InvalidShape(msg))
                if msg.contains("variant_refs instead of rev")
        ));
    }

    #[test]
    fn invalid_status_returns_reason_and_no_spec() {
        let intent = IntentResolutionJson {
            status: ResolutionStatus::Invalid,
            task_kind: None,
            target_kind: None,
            block: None,
            block_range: None,
            txids: None,
            repetitions: None,
            warmup: None,
            rev: None,
            variant_refs: None,
            repository: None,
            validation_selection: None,
            validation_block_count: None,
            validation_start: None,
            validation_end: None,
            reason: Some("I need a target".into()),
            issues: Some(vec![IntentIssueJson {
                field: IntentIssueField::Target,
                code: IntentIssueCode::Missing,
                message: " Specify a txid, block, or block range. ".into(),
            }]),
        };
        let IntentOutcome::Invalid(invalid) = validate_intent_resolution(intent).unwrap() else {
            panic!("expected invalid");
        };
        assert_eq!(invalid.reason, "I need a target");
        assert_eq!(
            invalid.issues,
            vec![IntentIssueJson {
                field: IntentIssueField::Target,
                code: IntentIssueCode::Missing,
                message: "Specify a txid, block, or block range.".into(),
            }]
        );
        assert_eq!(
            invalid.user_message(),
            "I need a target\n• target: Specify a txid, block, or block range."
        );
    }

    #[test]
    fn invalid_status_bounds_model_controlled_output() {
        let issues = (0..10)
            .map(|_| IntentIssueJson {
                field: IntentIssueField::Target,
                code: IntentIssueCode::Invalid,
                message: "x".repeat(300),
            })
            .collect();
        let intent = IntentResolutionJson {
            status: ResolutionStatus::Invalid,
            task_kind: None,
            target_kind: None,
            block: None,
            block_range: None,
            txids: None,
            repetitions: None,
            warmup: None,
            rev: None,
            variant_refs: None,
            repository: None,
            validation_selection: None,
            validation_block_count: None,
            validation_start: None,
            validation_end: None,
            reason: Some("r".repeat(600)),
            issues: Some(issues),
        };
        let IntentOutcome::Invalid(invalid) = validate_intent_resolution(intent).unwrap() else {
            panic!("expected invalid");
        };
        assert_eq!(invalid.reason.chars().count(), MAX_INVALID_REASON_CHARS);
        assert!(invalid.reason.ends_with('…'));
        assert_eq!(invalid.issues.len(), MAX_INVALID_ISSUES);
        for issue in invalid.issues {
            assert_eq!(issue.message.chars().count(), MAX_INVALID_ISSUE_MESSAGE_CHARS);
            assert!(issue.message.ends_with('…'));
        }
    }

    #[test]
    fn rejects_wrong_target_payload_for_kind() {
        let mut intent = resolved_base(TargetKind::Block);
        intent.txids = Some(vec![TXID.into()]);
        assert!(matches!(
            validate_intent_resolution(intent),
            Err(IntentValidationError::InvalidShape(_))
        ));
    }

    #[test]
    fn serde_rejects_extra_fields_before_validation() {
        let intent = json!({
            "status": "invalid",
            "target_kind": null,
            "block": null,
            "block_range": null,
            "txids": null,
            "repetitions": null,
            "warmup": null,
            "rev": null,
            "variant_refs": null,
            "reason": "missing target",
            "issues": null,
            "extra": "nope"
        });
        assert!(serde_json::from_value::<IntentResolutionJson>(intent).is_err());
    }

    #[test]
    fn rejects_bad_hashes_and_zero_repetitions() {
        let mut bad_hash = resolved_base(TargetKind::Txids);
        bad_hash.txids = Some(vec!["0xabc".into()]);
        assert!(matches!(
            validate_intent_resolution(bad_hash),
            Err(IntentValidationError::InvalidValue { field: "txids", .. })
        ));

        let mut zero = resolved_base(TargetKind::BlockRange);
        zero.block_range = Some(IntentBlockRangeJson { start: 1, end: 2 });
        zero.repetitions = Some(0);
        assert!(matches!(
            validate_intent_resolution(zero),
            Err(IntentValidationError::InvalidValue { field: "repetitions", .. })
        ));
    }

    #[test]
    fn rejects_inverted_range() {
        let mut intent = resolved_base(TargetKind::BlockRange);
        intent.block_range = Some(IntentBlockRangeJson { start: 12, end: 10 });
        assert!(matches!(
            validate_intent_resolution(intent),
            Err(IntentValidationError::InvalidValue { field: "block_range", .. })
        ));
    }

    #[test]
    fn validates_default_and_explicit_block_validation_intent() {
        let default = IntentResolutionJson {
            status: ResolutionStatus::Resolved,
            task_kind: Some(CreationTaskKind::BlockValidation),
            target_kind: None,
            block: None,
            block_range: None,
            txids: None,
            repetitions: None,
            warmup: None,
            rev: Some(" abc123 ".into()),
            variant_refs: None,
            repository: None,
            validation_selection: Some(ValidationSelectionKind::Recent),
            validation_block_count: None,
            validation_start: None,
            validation_end: None,
            reason: None,
            issues: None,
        };
        assert_eq!(
            validate_intent_resolution(default).unwrap(),
            IntentOutcome::Resolved(UserIntent::Create(TaskCreationIntent::BlockValidation(
                BlockValidationIntent {
                    source: RequestedSource {
                        repository: None,
                        revision: Some("abc123".into()),
                    },
                    selection: ValidationSelection::Recent { block_count: None },
                }
            )))
        );

        let explicit = IntentResolutionJson {
            validation_selection: Some(ValidationSelectionKind::Range),
            validation_block_count: None,
            validation_start: Some(100),
            validation_end: Some(200),
            ..IntentResolutionJson {
                status: ResolutionStatus::Resolved,
                task_kind: Some(CreationTaskKind::BlockValidation),
                target_kind: None,
                block: None,
                block_range: None,
                txids: None,
                repetitions: None,
                warmup: None,
                rev: None,
                variant_refs: None,
                repository: None,
                validation_selection: None,
                validation_block_count: None,
                validation_start: None,
                validation_end: None,
                reason: None,
                issues: None,
            }
        };
        assert!(matches!(
            validate_intent_resolution(explicit).unwrap(),
            IntentOutcome::Resolved(UserIntent::Create(TaskCreationIntent::BlockValidation(
                BlockValidationIntent {
                    selection: ValidationSelection::Range { start: 100, end: 200 },
                    ..
                }
            )))
        ));
    }

    #[test]
    fn block_validation_rejects_cross_task_and_partial_fields() {
        let mut cross_task = resolved_base(TargetKind::BlockRange);
        cross_task.task_kind = Some(CreationTaskKind::BlockValidation);
        cross_task.block_range = Some(IntentBlockRangeJson { start: 1, end: 2 });
        assert!(matches!(
            validate_intent_resolution(cross_task),
            Err(IntentValidationError::InvalidShape(_))
        ));

        let partial = IntentResolutionJson {
            status: ResolutionStatus::Resolved,
            task_kind: Some(CreationTaskKind::BlockValidation),
            target_kind: None,
            block: None,
            block_range: None,
            txids: None,
            repetitions: None,
            warmup: None,
            rev: None,
            variant_refs: None,
            repository: None,
            validation_selection: Some(ValidationSelectionKind::Range),
            validation_block_count: None,
            validation_start: Some(1),
            validation_end: None,
            reason: None,
            issues: None,
        };
        assert!(matches!(
            validate_intent_resolution(partial),
            Err(IntentValidationError::InvalidShape(message))
                if message.contains("selector fields")
        ));

        let mut empty_source = IntentResolutionJson {
            status: ResolutionStatus::Resolved,
            task_kind: Some(CreationTaskKind::BlockValidation),
            target_kind: None,
            block: None,
            block_range: None,
            txids: None,
            repetitions: None,
            warmup: None,
            rev: Some("  ".into()),
            variant_refs: None,
            repository: None,
            validation_selection: Some(ValidationSelectionKind::Recent),
            validation_block_count: None,
            validation_start: None,
            validation_end: None,
            reason: None,
            issues: None,
        };
        assert!(validate_intent_resolution(empty_source.clone()).is_err());
        empty_source.rev = None;
        empty_source.repository = Some(String::new());
        assert!(validate_intent_resolution(empty_source).is_err());
    }

    #[test]
    fn validation_selection_is_exhaustive_and_typed() {
        for (kind, block_count, start, end) in [
            (ValidationSelectionKind::Recent, Some(500_000), None, None),
            (ValidationSelectionKind::Full, None, None, None),
            (ValidationSelectionKind::Range, None, Some(10), Some(20)),
        ] {
            let intent = IntentResolutionJson {
                status: ResolutionStatus::Resolved,
                task_kind: Some(CreationTaskKind::BlockValidation),
                target_kind: None,
                block: None,
                block_range: None,
                txids: None,
                repetitions: None,
                warmup: None,
                rev: None,
                variant_refs: None,
                repository: None,
                validation_selection: Some(kind),
                validation_block_count: block_count,
                validation_start: start,
                validation_end: end,
                reason: None,
                issues: None,
            };
            assert!(matches!(
                validate_intent_resolution(intent),
                Ok(IntentOutcome::Resolved(UserIntent::Create(
                    TaskCreationIntent::BlockValidation(_)
                )))
            ));
        }
    }

    #[test]
    fn schema_payload_is_strict_openai_shape() {
        let format = intent_response_text_format();
        assert_eq!(format["type"], "json_schema");
        assert_eq!(format["strict"], true);
        assert_eq!(format["schema"]["type"], "object");
        assert_eq!(format["schema"]["additionalProperties"], false);
        assert!(format["schema"]["anyOf"].is_null());
        let required = format["schema"]["required"]
            .as_array()
            .unwrap();
        for field in [
            "status",
            "task_kind",
            "target_kind",
            "block",
            "block_range",
            "txids",
            "repetitions",
            "warmup",
            "rev",
            "variant_refs",
            "repository",
            "validation_selection",
            "validation_block_count",
            "validation_start",
            "validation_end",
            "reason",
            "issues",
        ] {
            assert!(
                required
                    .iter()
                    .any(|v| v == field),
                "{field} is required"
            );
        }
        let encoded = format.to_string();
        for forbidden in [
            "worker_id",
            "requested_shards",
            "max_concurrency",
            "timeout_secs",
            "required_capability",
        ] {
            assert!(!encoded.contains(forbidden), "provider schema exposes {forbidden}");
        }
    }

    #[test]
    fn pinned_openai_schema_tracks_dto_properties() {
        let generated = generated_intent_schema();
        assert_eq!(generated["title"], "IntentResolutionJson");

        let format = intent_response_text_format();
        let pinned = &format["schema"];
        let pinned_props = property_names(pinned);
        assert_eq!(
            required_names(pinned),
            pinned_props,
            "OpenAI strict mode expects every field to be required + nullable"
        );
        assert_eq!(
            property_names(&generated),
            pinned_props,
            "root schema properties drifted from IntentResolutionJson"
        );

        let pinned_selector = pinned
            .pointer("/properties/block/items")
            .expect("pinned block selector schema");
        assert_eq!(
            required_names(pinned_selector),
            property_names(pinned_selector),
            "OpenAI strict mode expects every selector field to be required + nullable"
        );
        assert_eq!(
            property_names(generated_def(&generated, "IntentBlockSelectorJson")),
            property_names(pinned_selector),
            "block selector schema properties drifted from IntentBlockSelectorJson"
        );

        let pinned_range = pinned
            .pointer("/properties/block_range")
            .expect("pinned block-range schema");
        assert_eq!(
            required_names(pinned_range),
            property_names(pinned_range),
            "OpenAI strict mode expects every range field to be required"
        );
        assert_eq!(
            property_names(generated_def(&generated, "IntentBlockRangeJson")),
            property_names(pinned_range),
            "block range schema properties drifted from IntentBlockRangeJson"
        );

        let pinned_issue = pinned
            .pointer("/properties/issues/items")
            .expect("pinned issue schema");
        assert_eq!(
            required_names(pinned_issue),
            property_names(pinned_issue),
            "OpenAI strict mode expects every issue field to be required"
        );
        assert_eq!(
            property_names(generated_def(&generated, "IntentIssueJson")),
            property_names(pinned_issue),
            "issue schema properties drifted from IntentIssueJson"
        );
    }

    fn property_names(schema: &Value) -> BTreeSet<String> {
        schema["properties"]
            .as_object()
            .expect("schema has properties")
            .keys()
            .cloned()
            .collect()
    }

    fn required_names(schema: &Value) -> BTreeSet<String> {
        schema["required"]
            .as_array()
            .expect("schema has required fields")
            .iter()
            .map(|v| {
                v.as_str()
                    .expect("required field name")
                    .to_string()
            })
            .collect()
    }

    fn generated_def<'a>(schema: &'a Value, name: &str) -> &'a Value {
        schema
            .pointer(&format!("/$defs/{name}"))
            .or_else(|| schema.pointer(&format!("/definitions/{name}")))
            .expect("generated schema definition")
    }
}
