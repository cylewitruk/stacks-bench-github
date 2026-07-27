//! Backend-neutral block-validation task contracts.

/// One immutable chainstate generation selected by the orchestrator.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DatasetIdentity {
    pub generation: String,
    pub network: String,
    pub format_version: String,
    pub covered_start: u64,
    pub covered_end: u64,
    pub manifest_sha256: String,
}

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
    pub dataset: DatasetIdentity,
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
    pub dataset: DatasetIdentity,
}
