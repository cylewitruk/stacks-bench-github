//! Backend-neutral block-validation task contracts.

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidationEpoch {
    PreNakamoto,
    Nakamoto,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InclusiveRange {
    pub start: u64,
    pub end: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BlockValidationSelection {
    Recent { block_count: u64 },
    Full,
    Range { range: InclusiveRange },
}

/// Immutable block-validation demand. The concrete range and resource plan
/// are resolved inside the sandbox against its attached chainstate.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BlockValidationTaskSpec {
    pub selection: BlockValidationSelection,
    pub timeout_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InvalidBlock {
    pub shard: u32,
    pub block: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ObservedValidationIndex {
    pub pre_nakamoto_count: u64,
    pub nakamoto_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ValidationEpochSegment {
    pub epoch: ValidationEpoch,
    pub global_range: InclusiveRange,
    pub local_range: InclusiveRange,
}

/// Typed logical result, kept separate from backend forensics.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BlockValidationOutput {
    pub valid: bool,
    pub checked_blocks: u64,
    pub invalid_blocks: Vec<InvalidBlock>,
    pub chainstate_origin: String,
    pub observed: ObservedValidationIndex,
    pub resolved_range: InclusiveRange,
    pub segments: Vec<ValidationEpochSegment>,
    pub shard_count: u32,
    pub max_concurrency: u32,
}
