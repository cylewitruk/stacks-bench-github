//! Owned in-process execution request and outcome types.

use uuid::Uuid;

use crate::BenchmarkRunContext;

#[derive(Debug, Clone)]
pub struct ExecutionRequest {
    pub context: ExecutionContext,
    pub task: ExecutionTask,
    pub placement: ExecutionPlacement,
}

#[derive(Debug, Clone)]
pub struct ExecutionContext {
    pub job_id: Uuid,
    pub repository: String,
    pub commit: String,
}

#[derive(Debug, Clone)]
pub enum ExecutionTask {
    Benchmark(BenchmarkTask),
    BuildOnly,
    Unsupported { combination: String },
}

#[derive(Debug, Clone)]
pub struct BenchmarkTask {
    /// Fully resolved argument tokens. Execution must not apply defaults.
    pub args: Vec<String>,
    pub sqlite_seed_key: Option<String>,
    pub shared_baseline_calibration: bool,
    pub baseline_calibration_id: Option<i64>,
    pub run: BenchmarkRunContext,
}

#[derive(Debug, Clone, Default)]
pub struct ExecutionPlacement {
    pub vcpu_cpuset: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskStatus {
    Completed,
    Failed(String),
}

#[derive(Debug)]
pub struct ExecutionOutcome {
    pub status: TaskStatus,
    pub summary: serde_json::Value,
}
