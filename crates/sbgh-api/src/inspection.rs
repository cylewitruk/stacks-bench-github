//! Serializable responses for browsing and following work.

use serde::{Deserialize, Serialize};

use crate::SubmissionReportView;

/// Selected request and optional execution scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkTargetView {
    /// Containing submission UUID.
    pub submission_id: String,
    /// Selected job UUID, absent for request-wide inspection.
    pub job_id: Option<String>,
}

/// Recent request summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmissionSummaryView {
    /// Submission UUID.
    pub id: String,
    /// Repository slug.
    pub repository: String,
    /// Request producer.
    pub source: String,
    /// Aggregate state.
    pub state: String,
    /// RFC3339 creation time.
    pub created_at: String,
    /// Completed job count.
    pub completed_jobs: u32,
    /// Planned job count.
    pub total_jobs: u32,
    /// First variant's source reference.
    pub reference: String,
}

/// Shared typed observation for status, watch, and results.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkSnapshotView {
    /// Resolved scope.
    pub target: WorkTargetView,
    /// Aggregate request report.
    pub report: SubmissionReportView,
    /// Executions in spec/run order.
    pub jobs: Vec<JobInspectionView>,
}

/// Job state, progress, and outputs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobInspectionView {
    /// Job UUID.
    pub id: String,
    /// Zero-based variant index.
    pub spec_index: i32,
    /// Zero-based repeat index.
    pub run_index: i32,
    /// Lifecycle state.
    pub status: String,
    /// Task discriminator.
    pub task_kind: String,
    /// Requested reference.
    pub reference: String,
    /// Resolved revision.
    pub commit: Option<String>,
    /// RFC3339 queue entry time.
    pub created_at: String,
    /// Frozen workload arguments.
    pub effective_args: Vec<String>,
    /// Recorded chainstate origin.
    pub chainstate_origin: Option<String>,
    /// Reliable attempt phase.
    pub phase: Option<String>,
    /// Terminal failure reason.
    pub failure: Option<String>,
    /// Placement and heartbeat.
    pub attempt: Option<AttemptInspectionView>,
    /// Latest attempt progress.
    pub progress: Option<ProgressInspectionView>,
    /// Promoted benchmark measurements.
    pub metrics: Option<BenchmarkMetricsView>,
    /// Recorded task result.
    pub result: Option<serde_json::Value>,
}

/// Worker placement and independent heartbeat freshness.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptInspectionView {
    /// Attempt UUID.
    pub id: String,
    /// Worker UUID.
    pub worker_id: String,
    /// Worker display name.
    pub worker_name: String,
    /// Attempt state.
    pub status: String,
    /// RFC3339 acceptance time.
    pub started_at: Option<String>,
    /// RFC3339 termination time.
    pub finished_at: Option<String>,
    /// RFC3339 session heartbeat time.
    pub heartbeat_at: String,
}

/// Latest accepted progress within an attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgressInspectionView {
    /// Attempt-local sequence.
    pub sequence: i64,
    /// Phase label.
    pub phase: String,
    /// Completed units.
    pub current: u64,
    /// Total units, if known.
    pub total: Option<u64>,
    /// Task-specific explanation.
    pub message: Option<String>,
    /// RFC3339 daemon receipt time.
    pub received_at: String,
}

/// Promoted benchmark measurements.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BenchmarkMetricsView {
    /// Whole execution duration in microseconds.
    pub envelope_duration_us: i64,
    /// Replay duration in microseconds.
    pub replay_duration_us: i64,
    /// Measured blocks.
    pub measured_blocks: i64,
    /// Warmup blocks.
    pub warmup_blocks: i64,
    /// Measured transactions.
    pub transactions: i64,
    /// Setup duration in microseconds.
    pub setup_duration_us: i64,
    /// Execution duration in microseconds.
    pub execution_duration_us: i64,
    /// Commit duration in microseconds.
    pub commit_duration_us: i64,
    /// Clarity runtime cost units.
    pub clarity_runtime: i64,
    /// Logical bytes read.
    pub read_length: i64,
    /// Logical bytes written.
    pub write_length: i64,
}
