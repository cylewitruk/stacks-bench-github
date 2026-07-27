//! In-process execution boundary. The scheduler assembles an owned request
//! after commit preparation; this module owns task dispatch and backend calls.

use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::bench_recipe::BenchRecipe;
use crate::build_recipe::BuildOnlyRecipe;
use crate::recipe::{Recipe, TaskOutcome, UnsupportedRecipe};
use sbgh_driver::{
    Driver, EventSink, ExecutionOutcome, ExecutionRequest, ExecutionTask, TaskContext,
};

#[derive(Clone)]
pub struct ExecutionDependencies {
    pub driver: Arc<dyn Driver>,
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
        repository_credential: request
            .context
            .repository_credential
            .as_ref()
            .map(sbgh_driver::RepositoryCredential::expose),
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
    use sbgh_driver::{
        DriverOutcome, ExecutionContext, ExecutionPlacement, PhaseLabel, Placement, SinkResult,
        TaskSpec, TaskStatus,
    };
    use uuid::Uuid;

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
                repository_credential: None,
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
}
