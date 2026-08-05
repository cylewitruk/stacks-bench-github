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
    /// Authenticated operator diagnostics. Provider renderers deliberately do
    /// not include untrusted guest output in GitHub or Slack messages.
    pub forensics: Option<ReportForensics>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportForensics {
    pub last_phase: Option<String>,
    pub console_tail: Option<String>,
    pub console_size_bytes: Option<u64>,
    pub console_artifact_key: Option<String>,
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
    pub requested: Option<BlockValidationSelectionReport>,
    pub observed: Option<ObservedValidationIndexReport>,
    pub resolved_range: Option<InclusiveReportRange>,
    pub segments: Vec<ValidationEpochSegmentReport>,
    pub shard_count: Option<u32>,
    pub max_concurrency: Option<u32>,
    pub verdict: Option<BlockValidationVerdict>,
    pub checked_blocks: Option<u64>,
    pub chainstate_origin: Option<String>,
    pub invalid_blocks: Vec<InvalidBlockReport>,
}

impl BlockValidationReportView {
    pub fn from_request(request: Option<&sbgh_fleet::BlockValidationPayload>) -> Self {
        Self {
            requested: request.map(|request| match &request.selection {
                sbgh_fleet::BlockValidationSelection::Recent { block_count } => {
                    BlockValidationSelectionReport::Recent { block_count: *block_count }
                }
                sbgh_fleet::BlockValidationSelection::Full => BlockValidationSelectionReport::Full,
                sbgh_fleet::BlockValidationSelection::Range { range } => {
                    BlockValidationSelectionReport::Range {
                        range: InclusiveReportRange {
                            start: range.start,
                            end: range.end,
                        },
                    }
                }
            }),
            observed: None,
            resolved_range: None,
            segments: Vec::new(),
            shard_count: None,
            max_concurrency: None,
            verdict: None,
            checked_blocks: None,
            chainstate_origin: None,
            invalid_blocks: Vec::new(),
        }
    }

    pub fn from_result(
        request: Option<&sbgh_fleet::BlockValidationPayload>,
        result: &sbgh_fleet::BlockValidationResult,
    ) -> Self {
        let mut view = Self::from_request(request);
        view.observed = Some(ObservedValidationIndexReport {
            pre_nakamoto_count: result
                .observed
                .pre_nakamoto_count,
            nakamoto_count: result.observed.nakamoto_count,
        });
        view.resolved_range = Some(InclusiveReportRange {
            start: result.resolved_range.start,
            end: result.resolved_range.end,
        });
        view.segments = result
            .segments
            .iter()
            .map(|segment| ValidationEpochSegmentReport {
                epoch: match segment.epoch {
                    sbgh_fleet::ValidationEpoch::PreNakamoto => ValidationEpochReport::PreNakamoto,
                    sbgh_fleet::ValidationEpoch::Nakamoto => ValidationEpochReport::Nakamoto,
                },
                global_range: InclusiveReportRange {
                    start: segment.global_range.start,
                    end: segment.global_range.end,
                },
                local_range: InclusiveReportRange {
                    start: segment.local_range.start,
                    end: segment.local_range.end,
                },
            })
            .collect();
        view.shard_count = Some(result.shard_count);
        view.max_concurrency = Some(result.max_concurrency);
        view.verdict = Some(if result.valid {
            BlockValidationVerdict::Valid
        } else {
            BlockValidationVerdict::Invalid
        });
        view.checked_blocks = Some(result.checked_blocks);
        view.chainstate_origin = Some(
            result
                .chainstate_origin
                .clone(),
        );
        view.invalid_blocks = result
            .invalid_blocks
            .iter()
            .map(|invalid| InvalidBlockReport {
                shard: invalid.shard,
                block: invalid.block.clone(),
                reason: invalid.reason.clone(),
            })
            .collect();
        view
    }

    pub fn is_valid(&self) -> Option<bool> {
        self.verdict
            .map(|verdict| verdict == BlockValidationVerdict::Valid)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BlockValidationSelectionReport {
    Recent { block_count: u64 },
    Full,
    Range { range: InclusiveReportRange },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservedValidationIndexReport {
    pub pre_nakamoto_count: u64,
    pub nakamoto_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidationEpochReport {
    PreNakamoto,
    Nakamoto,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationEpochSegmentReport {
    pub epoch: ValidationEpochReport,
    pub global_range: InclusiveReportRange,
    pub local_range: InclusiveReportRange,
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
