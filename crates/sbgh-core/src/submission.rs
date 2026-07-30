//! Task-submission application and persistence contracts.
//!
//! A submission records immutable demand. Scheduling and execution attempts
//! consume materialized jobs and do not participate in this boundary.

use chrono::{DateTime, Utc};
use sbgh_fleet::{BlockValidationPayload, WorkerCapability};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::models::{BuildTarget, GitRefKind, JobIntent, JobSource, TaskKind};

pub const SUBMISSION_CONTRACT_VERSION: i32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerKey {
    pub namespace: String,
    pub key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SubmissionActor {
    System,
    GithubUser { user_id: i64 },
    SlackUser { team_id: String, user_id: String },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchedulingConstraints {
    pub required_worker_id: Option<Uuid>,
    pub required_measurement_profile: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedTaskSource {
    pub github_installation_id: i64,
    pub github_repo_id: i64,
    pub source: JobSource,
    pub intent: JobIntent,
    pub task_kind: TaskKind,
    pub build_target: BuildTarget,
    pub git_ref_kind: GitRefKind,
    pub git_ref_display: String,
    pub commit: String,
    pub committed_at: Option<DateTime<Utc>>,
    pub workload_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BenchmarkVariant {
    pub source: ResolvedTaskSource,
    pub requested_run_count: i32,
    pub baseline_calibration_id: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BenchmarkPlan {
    pub variants: Vec<BenchmarkVariant>,
    pub effective_args: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildOnlyPlan {
    pub source: ResolvedTaskSource,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockValidationPlan {
    pub source: ResolvedTaskSource,
    pub payload: BlockValidationPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "plan", rename_all = "snake_case")]
pub enum TaskPlan {
    Benchmark(BenchmarkPlan),
    BuildOnly(BuildOnlyPlan),
    BlockValidation(BlockValidationPlan),
}

impl TaskPlan {
    pub fn required_capability(&self) -> WorkerCapability {
        match self {
            Self::Benchmark(_) => TaskKind::Benchmark.required_capability(),
            Self::BuildOnly(_) => TaskKind::BuildOnly.required_capability(),
            Self::BlockValidation(_) => TaskKind::BlockValidation.required_capability(),
        }
    }

    pub fn first_source(&self) -> Option<&ResolvedTaskSource> {
        match self {
            Self::Benchmark(plan) => plan
                .variants
                .first()
                .map(|variant| &variant.source),
            Self::BuildOnly(plan) => Some(&plan.source),
            Self::BlockValidation(plan) => Some(&plan.source),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GithubSubmissionProvenance {
    pub webhook_id: i64,
    pub triggering_user_id: Option<i64>,
    pub pull_request_id: Option<i64>,
    pub triggering_comment_id: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlackSubmissionProvenance {
    pub team_id: String,
    pub channel_id: String,
    pub request_message_ts: String,
    pub reporting_identity: String,
    pub report_message_ts: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubmissionProvenance {
    /// Exact enqueue audit payload. This deliberately remains JSON because
    /// benchmark producers freeze derived `effective_args` beside the tagged
    /// [`crate::models::QueuedEventDetail`].
    pub queued_event_detail: serde_json::Value,
    pub github: Option<GithubSubmissionProvenance>,
    pub slack: Option<SlackSubmissionProvenance>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubmissionCommand {
    pub actor: SubmissionActor,
    pub producer_key: ProducerKey,
    pub constraints: SchedulingConstraints,
    pub task: TaskPlan,
    pub provenance: SubmissionProvenance,
}

#[derive(Debug, Clone)]
pub struct PreparedSubmission {
    pub command: SubmissionCommand,
    pub contract_version: i32,
    pub request_digest: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmissionDisposition {
    Created,
    AlreadySubmitted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmissionReceipt {
    pub submission_id: Uuid,
    pub disposition: SubmissionDisposition,
    pub initial_job_ids: Vec<Uuid>,
}

#[derive(Debug, thiserror::Error)]
pub enum SubmissionError {
    #[error("invalid submission: {0}")]
    Invalid(String),
    #[error("submission idempotency conflict for {namespace}:{key}")]
    Conflict { namespace: String, key: String },
    #[error(transparent)]
    Persistence(#[from] anyhow::Error),
}
