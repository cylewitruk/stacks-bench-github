//! LLM intent-resolution DTOs and validation.
//!
//! The model-facing JSON is deliberately separate from [`WorkloadSpec`]: it can
//! represent an invalid/ambiguous request, and it uses a strict-schema-friendly
//! object shape. Daemon validation then normalizes it into the internal
//! workload type used by both the deterministic parser and surface adapters.

use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;

use crate::workload::{BlockSelector, WorkloadSpec, WorkloadTarget};

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
    Target,
    Block,
    BlockRange,
    Txids,
    Repetitions,
    Warmup,
    Rev,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct IntentResolutionJson {
    pub status: ResolutionStatus,
    pub target_kind: Option<TargetKind>,
    pub block: Option<Vec<IntentBlockSelectorJson>>,
    pub block_range: Option<IntentBlockRangeJson>,
    pub txids: Option<Vec<String>>,
    pub repetitions: Option<u32>,
    pub warmup: Option<u32>,
    pub rev: Option<String>,
    pub reason: Option<String>,
    pub issues: Option<Vec<IntentIssueJson>>,
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
            Self::Target => "target",
            Self::Block => "block",
            Self::BlockRange => "block range",
            Self::Txids => "txids",
            Self::Repetitions => "repetitions",
            Self::Warmup => "warmup",
            Self::Rev => "rev",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntentOutcome {
    Resolved(WorkloadSpec),
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
    if intent.target_kind.is_some()
        || intent.block.is_some()
        || intent.block_range.is_some()
        || intent.txids.is_some()
        || intent.repetitions.is_some()
        || intent.warmup.is_some()
        || intent.rev.is_some()
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
    if intent.reason.is_some() || intent.issues.is_some() {
        return Err(IntentValidationError::InvalidShape(
            "resolved status must not carry reason or issues".into(),
        ));
    }
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

    Ok(IntentOutcome::Resolved(WorkloadSpec {
        target,
        clean_repetitions: repetitions,
        warmup: Some(warmup),
        rev,
    }))
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

/// JSON schema body for OpenAI `text.format` (`type = json_schema`).
///
/// This is intentionally pinned rather than relying solely on generated schema:
/// OpenAI strict outputs require all fields to be present, nullable fields for
/// optional values, no root union, and `additionalProperties: false`.
pub fn intent_response_text_format() -> Value {
    json!({
        "type": "json_schema",
        "name": "sbgh_benchmark_intent",
        "strict": true,
        "schema": {
            "type": "object",
            "additionalProperties": false,
            "required": [
                "status",
                "target_kind",
                "block",
                "block_range",
                "txids",
                "repetitions",
                "warmup",
                "rev",
                "reason",
                "issues"
            ],
            "properties": {
                "status": { "type": "string", "enum": ["resolved", "invalid"] },
                "target_kind": {
                    "type": ["string", "null"],
                    "enum": ["block", "block_range", "txids", null]
                },
                "block": {
                    "type": ["array", "null"],
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
                    "additionalProperties": false,
                    "required": ["start", "end"],
                    "properties": {
                        "start": { "type": "integer", "minimum": 0 },
                        "end": { "type": "integer", "minimum": 0 }
                    }
                },
                "txids": {
                    "type": ["array", "null"],
                    "items": { "type": "string" }
                },
                "repetitions": { "type": ["integer", "null"], "minimum": 1 },
                "warmup": { "type": ["integer", "null"], "minimum": 0 },
                "rev": { "type": ["string", "null"] },
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
                                    "target",
                                    "block",
                                    "block_range",
                                    "txids",
                                    "repetitions",
                                    "warmup",
                                    "rev"
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

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IntentEvalFixture {
    pub prompt: &'static str,
    pub expected: EvalExpected,
}

#[cfg(test)]
pub const EVAL_FIXTURES: &[IntentEvalFixture] = &[
    // Resolved fixtures must include enough concrete information for the
    // current Slack surface. Contextual phrases like "this branch" stay invalid
    // until a future caller supplies PR/comment context.
    IntentEvalFixture {
        prompt: "bench block 8123456",
        expected: EvalExpected::Resolved,
    },
    IntentEvalFixture {
        prompt: "profile block 8123456 ten times",
        expected: EvalExpected::Resolved,
    },
    IntentEvalFixture {
        prompt: "bench blocks 8123456 to 8200000 on 3.4.0.0.3",
        expected: EvalExpected::Resolved,
    },
    IntentEvalFixture {
        prompt: "run 10 reps with 1000 warmup from 8123456 to 8200000",
        expected: EvalExpected::Resolved,
    },
    IntentEvalFixture {
        prompt: "profile tx f426738843949f576e4eff5ffbb148de9e1a638d20a03c6447cc70490f5156ce twice",
        expected: EvalExpected::Resolved,
    },
    IntentEvalFixture {
        prompt: "bench this tx with 2 warmup iterations",
        expected: EvalExpected::Invalid,
    },
    IntentEvalFixture {
        prompt: "benchmark 0xc3b1aad400000000000000000000000000000000000000000000000000000000 on \
                 develop",
        expected: EvalExpected::Invalid,
    },
    IntentEvalFixture {
        prompt: "run this branch against block 8123456",
        expected: EvalExpected::Invalid,
    },
    IntentEvalFixture {
        prompt: "bench something fast",
        expected: EvalExpected::Invalid,
    },
    IntentEvalFixture {
        prompt: "compare main and develop",
        expected: EvalExpected::Invalid,
    },
    IntentEvalFixture {
        prompt: "bench tx not-a-hash",
        expected: EvalExpected::Invalid,
    },
    IntentEvalFixture {
        prompt: "bench block 8200000 to 8123456",
        expected: EvalExpected::Invalid,
    },
    IntentEvalFixture {
        prompt: "bench block 8123456 zero times",
        expected: EvalExpected::Invalid,
    },
    IntentEvalFixture {
        prompt: "bench tx f426738843949f576e4eff5ffbb148de9e1a638d20a03c6447cc70490f5156ce on \
                 feat/stacks-bench",
        expected: EvalExpected::Resolved,
    },
    IntentEvalFixture {
        prompt: "please run a benchmark",
        expected: EvalExpected::Invalid,
    },
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
        let actual = match outcome {
            IntentOutcome::Resolved(_) => EvalExpected::Resolved,
            IntentOutcome::Invalid(_) => EvalExpected::Invalid,
        };
        if actual != fixture.expected {
            failures.push(format!(
                "prompt `{}` expected {:?}, got {:?}",
                fixture.prompt, fixture.expected, actual
            ));
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
            target_kind: Some(target_kind),
            block: None,
            block_range: None,
            txids: None,
            repetitions: Some(1),
            warmup: Some(0),
            rev: None,
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
        let IntentOutcome::Resolved(spec) = validate_intent_resolution(intent).unwrap() else {
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
        let IntentOutcome::Resolved(spec) = validate_intent_resolution(intent).unwrap() else {
            panic!("expected resolved");
        };
        assert_eq!(spec.clean_repetitions, 3);
        assert_eq!(spec.target, WorkloadTarget::BlockRange { start: 10, end: 12 });
        assert_eq!(
            spec.to_bench_args(),
            vec!["--start-at", "10", "--count", "3", "--repetitions", "1", "--warmup", "2"]
        );
        assert_eq!(spec.rev.as_deref(), Some("develop"));
    }

    #[test]
    fn validates_txids_and_strips_prefix() {
        let mut intent = resolved_base(TargetKind::Txids);
        intent.txids = Some(vec![TXID.into()]);
        let IntentOutcome::Resolved(spec) = validate_intent_resolution(intent).unwrap() else {
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
    fn invalid_status_returns_reason_and_no_spec() {
        let intent = IntentResolutionJson {
            status: ResolutionStatus::Invalid,
            target_kind: None,
            block: None,
            block_range: None,
            txids: None,
            repetitions: None,
            warmup: None,
            rev: None,
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
            target_kind: None,
            block: None,
            block_range: None,
            txids: None,
            repetitions: None,
            warmup: None,
            rev: None,
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
            "target_kind",
            "block",
            "block_range",
            "txids",
            "repetitions",
            "warmup",
            "rev",
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
