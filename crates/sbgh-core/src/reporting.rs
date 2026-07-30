//! Provider-neutral, submission-scoped reporting read model.
//!
//! These types contain durable state only. Provider markup, credentials, and
//! publication policy belong to the daemon and adapter crates.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::models::{JobSource, TaskKind};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmissionReportView {
    pub identity: ReportIdentity,
    pub lifecycle: ReportLifecycle,
    pub task: TaskReport,
    pub artifacts: Vec<ReportArtifact>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportIdentity {
    pub submission_id: Uuid,
    pub current_job_id: Option<Uuid>,
    pub current_attempt_id: Option<Uuid>,
    pub task_kind: TaskKind,
    pub source: JobSource,
    pub repository: String,
    pub commit: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportLifecycle {
    pub state: ReportLifecycleState,
    pub phase: Option<String>,
    pub completed_jobs: u32,
    pub total_jobs: u32,
    pub failure: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReportLifecycleState {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "detail", rename_all = "snake_case")]
pub enum TaskReport {
    Benchmark(BenchmarkReportView),
    BuildOnly(BuildOnlyReportView),
    BlockValidation(BlockValidationReportView),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BenchmarkReportView {
    pub requested_runs: u32,
    pub completed_runs: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildOnlyReportView {
    pub cache_outcome: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockValidationReportView {
    pub requested_range: Option<InclusiveReportRange>,
    pub observed_range: Option<InclusiveReportRange>,
    pub verdict: Option<BlockValidationVerdict>,
    pub checked_blocks: Option<u64>,
    pub chainstate_origin: Option<String>,
    pub invalid_blocks: Vec<InvalidBlockReport>,
}

impl BlockValidationReportView {
    pub fn from_result(
        request: Option<&sbgh_fleet::BlockValidationPayload>,
        result: &sbgh_fleet::BlockValidationResult,
    ) -> Self {
        Self {
            requested_range: request.map(|request| InclusiveReportRange {
                start: request.range.start,
                end: request.range.end,
            }),
            observed_range: Some(InclusiveReportRange {
                start: result.observed_range.start,
                end: result.observed_range.end,
            }),
            verdict: Some(if result.valid {
                BlockValidationVerdict::Valid
            } else {
                BlockValidationVerdict::Invalid
            }),
            checked_blocks: Some(result.checked_blocks),
            chainstate_origin: Some(
                result
                    .chainstate_origin
                    .clone(),
            ),
            invalid_blocks: result
                .invalid_blocks
                .iter()
                .map(|invalid| InvalidBlockReport {
                    shard: invalid.shard,
                    block: invalid.block.clone(),
                    reason: invalid.reason.clone(),
                })
                .collect(),
        }
    }

    pub fn is_valid(&self) -> Option<bool> {
        self.verdict
            .map(|verdict| verdict == BlockValidationVerdict::Valid)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct InclusiveReportRange {
    pub start: u64,
    pub end: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockValidationVerdict {
    Valid,
    Invalid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvalidBlockReport {
    pub shard: u32,
    pub block: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportArtifact {
    pub name: String,
    pub key: String,
}

/// Submission-owned GitHub identity. `external_id` is persisted because
/// unambiguous historical checks retain their original job-derived value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmissionGithubReportIdentity {
    pub comment_id: Option<i64>,
    pub check_run_id: Option<i64>,
    pub check_run_url: Option<String>,
    pub check_name: Option<String>,
    pub external_id: Option<String>,
}
