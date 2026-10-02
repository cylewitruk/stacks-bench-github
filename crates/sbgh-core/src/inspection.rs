//! Durable operator views of requests, executions, and worker progress.

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::models::JobMetric;
use crate::reporting::{ReportLifecycleState, SubmissionReportView};

/// An unambiguous request or execution selected by the operator.
#[derive(Debug, Clone)]
pub struct WorkTarget {
    /// Containing request.
    pub submission_id: Uuid,
    /// Selected execution, absent for request-wide inspection.
    pub job_id: Option<Uuid>,
}

/// One request in the operator's recent-work listing.
#[derive(Debug, Clone)]
pub struct SubmissionSummary {
    /// Request identity.
    pub id: Uuid,
    /// Repository slug.
    pub repository: String,
    /// Request producer.
    pub source: String,
    /// Aggregate lifecycle state.
    pub state: ReportLifecycleState,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Successfully completed executions.
    pub completed_jobs: u32,
    /// Planned executions, including repeats not yet materialized.
    pub total_jobs: u32,
    /// First variant's reference.
    pub reference: String,
}

/// Request context and individually observable executions.
#[derive(Debug, Clone)]
pub struct WorkSnapshot {
    /// Resolved scope, pinned while watching.
    pub target: WorkTarget,
    /// Aggregate request report.
    pub report: SubmissionReportView,
    /// Executions in spec/run order.
    pub jobs: Vec<JobInspection>,
}

/// State and outputs of one execution.
#[derive(Debug, Clone)]
pub struct JobInspection {
    /// Execution identity.
    pub id: Uuid,
    /// Zero-based variant index.
    pub spec_index: i32,
    /// Zero-based repeat index.
    pub run_index: i32,
    /// Durable lifecycle state.
    pub status: String,
    /// Task discriminator.
    pub task_kind: String,
    /// Requested reference.
    pub reference: String,
    /// Resolved revision.
    pub commit: Option<String>,
    /// Queue entry time.
    pub created_at: DateTime<Utc>,
    /// Frozen workload arguments.
    pub effective_args: Vec<String>,
    /// Recorded execution origin.
    pub chainstate_origin: Option<String>,
    /// Reliable phase of the current attempt.
    pub phase: Option<String>,
    /// Terminal failure or cancellation reason.
    pub failure: Option<String>,
    /// Latest worker placement.
    pub attempt: Option<AttemptInspection>,
    /// Latest accepted progress within that attempt.
    pub progress: Option<ProgressInspection>,
    /// Promoted benchmark metrics.
    pub metrics: Option<JobMetric>,
    /// Recorded benchmark envelope or validation result.
    pub result: Option<serde_json::Value>,
}

/// Placement and freshness of an execution attempt.
#[derive(Debug, Clone)]
pub struct AttemptInspection {
    /// Attempt identity.
    pub id: Uuid,
    /// Worker identity.
    pub worker_id: Uuid,
    /// Worker display name.
    pub worker_name: String,
    /// Attempt state.
    pub status: String,
    /// Acceptance time.
    pub started_at: Option<DateTime<Utc>>,
    /// Terminal time.
    pub finished_at: Option<DateTime<Utc>>,
    /// Session heartbeat, distinct from task progress.
    pub heartbeat_at: DateTime<Utc>,
}

/// Best-effort task progress, durable after daemon acceptance.
#[derive(Debug, Clone)]
pub struct ProgressInspection {
    /// Attempt-local sequence.
    pub sequence: i64,
    /// Phase or unit label.
    pub phase: String,
    /// Units completed.
    pub current: u64,
    /// Total units, when known.
    pub total: Option<u64>,
    /// Task-specific explanation.
    pub message: Option<String>,
    /// Daemon receipt time, independent of the worker clock.
    pub received_at: DateTime<Utc>,
}
