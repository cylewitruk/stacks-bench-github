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

/// Fully resolved block-validation input. Host paths and backend details are
/// intentionally absent.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BlockValidationTaskSpec {
    pub epoch: ValidationEpoch,
    pub range: InclusiveRange,
    pub requested_shards: u32,
    pub max_concurrency: u32,
    pub timeout_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InvalidBlock {
    pub shard: u32,
    pub block: String,
    pub reason: String,
}

/// Typed logical result, kept separate from backend forensics.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BlockValidationOutput {
    pub valid: bool,
    pub checked_blocks: u64,
    pub invalid_blocks: Vec<InvalidBlock>,
    pub chainstate_origin: String,
    pub observed_range: InclusiveRange,
}
