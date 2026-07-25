//! In-process execution boundary. The scheduler assembles an owned request
//! after commit preparation; this module owns task dispatch and backend calls.

use std::sync::Arc;

use serde::Deserialize;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::bench_recipe::BenchRecipe;
use crate::build_recipe::BuildOnlyRecipe;
use crate::driver::{BenchmarkRunContext, Driver};
use crate::events::EventSink;
use crate::recipe::{Recipe, TaskContext, TaskOutcome, TaskStatus, UnsupportedRecipe};

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

#[derive(Clone)]
pub struct ExecutionDependencies {
    pub driver: Arc<dyn Driver>,
}

#[derive(Debug)]
pub struct ExecutionOutcome {
    pub status: TaskStatus,
    pub summary: serde_json::Value,
}

/// Schema-v1 envelope written by
/// `stacks-bench bench baseline calibrate --json`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub(crate) struct BaselineCalibrationResult {
    schema_version: Option<u32>,
    success: Option<bool>,
    result_type: Option<String>,
    result_version: Option<u32>,
    result: Option<BaselineCalibrationData>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
struct BaselineCalibrationData {
    calibration_id: Option<i64>,
}

impl BaselineCalibrationResult {
    pub(crate) fn from_bytes(bytes: &[u8]) -> Option<Self> {
        match serde_json::from_slice::<Self>(bytes) {
            Ok(result) => Some(result),
            Err(error) => {
                tracing::warn!(%error, "failed to parse calibration.json");
                None
            }
        }
    }

    pub(crate) fn calibration_id(&self) -> Option<i64> {
        match (self.schema_version, self.result_type.as_deref(), self.result_version, self.success)
        {
            (Some(1), Some("baseline_calibration"), Some(1), Some(true)) => self
                .result
                .as_ref()
                .and_then(|result| result.calibration_id),
            _ => None,
        }
    }
}

/// Dispatch an owned request. Adding a task requires a variant and composition
/// entry here, without changing claim, cancellation, reporting, or terminal
/// lifecycle code.
pub async fn execute(
    request: ExecutionRequest,
    dependencies: ExecutionDependencies,
    sink: &dyn EventSink,
    cancel: &CancellationToken,
) -> anyhow::Result<ExecutionOutcome> {
    let context = TaskContext {
        job_id: request.context.job_id,
        repository: &request.context.repository,
        commit: &request.context.commit,
    };
    match request.task {
        ExecutionTask::Benchmark(task) => {
            let recipe = BenchRecipe::new(
                dependencies.driver,
                task.args,
                request.placement.vcpu_cpuset,
                task.sqlite_seed_key,
                task.shared_baseline_calibration,
                task.baseline_calibration_id,
                task.run,
            );
            execute_recipe(&recipe, &context, sink, cancel).await
        }
        ExecutionTask::BuildOnly => {
            let recipe = BuildOnlyRecipe::new(dependencies.driver, request.placement.vcpu_cpuset);
            execute_recipe(&recipe, &context, sink, cancel).await
        }
        ExecutionTask::Unsupported { combination } => {
            let recipe = UnsupportedRecipe::new(combination);
            execute_recipe(&recipe, &context, sink, cancel).await
        }
    }
}

async fn execute_recipe<R: Recipe>(
    recipe: &R,
    context: &TaskContext<'_>,
    sink: &dyn EventSink,
    cancel: &CancellationToken,
) -> anyhow::Result<ExecutionOutcome> {
    let outcome = recipe
        .execute(context, sink, cancel)
        .await?;
    Ok(ExecutionOutcome {
        status: outcome.status(),
        summary: outcome.summary().clone(),
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use async_trait::async_trait;

    use super::*;
    use crate::driver::{DriverOutcome, Placement, TaskSpec};
    use crate::events::{PhaseLabel, SinkResult};
    use crate::recipe::TaskContext;

    struct NoopSink;

    #[async_trait]
    impl EventSink for NoopSink {
        async fn phase(&self, _label: PhaseLabel, _elapsed: Duration) -> SinkResult {
            Ok(())
        }

        async fn heartbeat(&self, _label: PhaseLabel, _elapsed: Duration) {}
    }

    struct PanicDriver;

    #[async_trait]
    impl Driver for PanicDriver {
        async fn run_task(
            &self,
            _context: &TaskContext<'_>,
            _task: &TaskSpec,
            _sink: &dyn EventSink,
            _cancel: &CancellationToken,
            _placement: &Placement,
        ) -> anyhow::Result<DriverOutcome> {
            panic!("unsupported dispatch must not touch the backend")
        }

        async fn cleanup_by_job_id(&self, _job_id: &str) -> bool {
            true
        }
    }

    #[tokio::test]
    async fn unsupported_task_fails_closed_without_backend_access() {
        let request = ExecutionRequest {
            context: ExecutionContext {
                job_id: Uuid::nil(),
                repository: "octo/core".into(),
                commit: "abc123".into(),
            },
            task: ExecutionTask::Unsupported {
                combination: "BlockValidation/StacksInspect".into(),
            },
            placement: ExecutionPlacement::default(),
        };
        let outcome = execute(
            request,
            ExecutionDependencies { driver: Arc::new(PanicDriver) },
            &NoopSink,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(matches!(outcome.status, TaskStatus::Failed(_)));
    }

    #[test]
    fn parses_schema_v1_baseline_calibration_payload() {
        let result = BaselineCalibrationResult::from_bytes(
            br#"{
                "schema_version": 1,
                "success": true,
                "result_type": "baseline_calibration",
                "result_version": 1,
                "duration_secs": 12.0,
                "result": { "calibration_id": 12 }
            }"#,
        )
        .unwrap();
        assert_eq!(result.calibration_id(), Some(12));
    }

    #[test]
    fn baseline_calibration_rejects_wrong_type_or_missing_id() {
        let wrong_type = BaselineCalibrationResult::from_bytes(
            br#"{
                "schema_version": 1,
                "success": true,
                "result_type": "run",
                "result_version": 1,
                "result": { "calibration_id": 12 }
            }"#,
        )
        .unwrap();
        assert_eq!(wrong_type.calibration_id(), None);

        let missing_id = BaselineCalibrationResult::from_bytes(
            br#"{
                "schema_version": 1,
                "success": true,
                "result_type": "baseline_calibration",
                "result_version": 1,
                "result": {}
            }"#,
        )
        .unwrap();
        assert_eq!(missing_id.calibration_id(), None);
    }
}
