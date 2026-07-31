//! In-process execution boundary. The scheduler assembles an owned request
//! after commit preparation; this module owns task dispatch and backend calls.

use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::bench_recipe::BenchRecipe;
use crate::block_validation_recipe::BlockValidationRecipe;
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
        attempt_id: request.context.attempt_id,
        fencing_generation: request
            .context
            .fencing_generation,
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
        ExecutionTask::BlockValidation(spec) => {
            let recipe = BlockValidationRecipe::new(
                dependencies.driver,
                spec,
                request.placement.vcpu_cpuset,
            );
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
        block_validation: outcome
            .block_validation()
            .cloned(),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::Duration;

    use async_trait::async_trait;

    use super::*;
    use sbgh_driver::{
        BlockValidationTaskSpec, DriverOutcome, ExecutionContext, ExecutionPlacement, PhaseLabel,
        Placement, SinkResult, TaskSpec, TaskStatus,
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

    struct RecordingBlockDriver {
        seen: Mutex<Option<(Uuid, u64, BlockValidationTaskSpec)>>,
    }

    struct MissingBlockOutputDriver;

    #[async_trait]
    impl Driver for MissingBlockOutputDriver {
        async fn run_task(
            &self,
            _context: &TaskContext<'_>,
            _task: &TaskSpec,
            _sink: &dyn EventSink,
            _cancel: &CancellationToken,
            _placement: &Placement,
        ) -> anyhow::Result<DriverOutcome> {
            Ok(DriverOutcome {
                status: sbgh_driver::DriverStatus::Completed,
                summary: serde_json::json!({}),
                output: sbgh_driver::DriverTaskOutput::None,
            })
        }

        async fn cleanup_by_job_id(&self, _job_id: &str) -> bool {
            true
        }
    }

    #[async_trait]
    impl Driver for RecordingBlockDriver {
        async fn run_task(
            &self,
            context: &TaskContext<'_>,
            task: &TaskSpec,
            _sink: &dyn EventSink,
            _cancel: &CancellationToken,
            _placement: &Placement,
        ) -> anyhow::Result<DriverOutcome> {
            let TaskSpec::BlockValidation(spec) = task else {
                panic!("block request was routed as the wrong driver task");
            };
            *self.seen.lock().unwrap() =
                Some((context.attempt_id, context.fencing_generation, spec.clone()));
            let range = match &spec.selection {
                sbgh_driver::BlockValidationSelection::Range { range } => *range,
                _ => sbgh_driver::InclusiveRange { start: 10, end: 12 },
            };
            Ok(DriverOutcome {
                status: sbgh_driver::DriverStatus::Completed,
                summary: serde_json::json!({"sandbox": "libvirt"}),
                output: sbgh_driver::DriverTaskOutput::BlockValidation(
                    sbgh_driver::BlockValidationOutput {
                        valid: true,
                        checked_blocks: 3,
                        invalid_blocks: Vec::new(),
                        chainstate_origin: "vg/mainnet-latest".into(),
                        observed: sbgh_driver::ObservedValidationIndex {
                            pre_nakamoto_count: 10,
                            nakamoto_count: 3,
                        },
                        resolved_range: range,
                        segments: vec![sbgh_driver::ValidationEpochSegment {
                            epoch: sbgh_driver::ValidationEpoch::Nakamoto,
                            global_range: range,
                            local_range: sbgh_driver::InclusiveRange { start: 0, end: 2 },
                        }],
                        shard_count: 3,
                        max_concurrency: 2,
                    },
                ),
            })
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
                attempt_id: Uuid::nil(),
                fencing_generation: 0,
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

    #[tokio::test]
    async fn block_validation_uses_the_common_driver_boundary_with_attempt_identity() {
        let attempt_id = Uuid::new_v4();
        let spec = BlockValidationTaskSpec {
            selection: sbgh_driver::BlockValidationSelection::Range {
                range: sbgh_driver::InclusiveRange { start: 10, end: 12 },
            },
            timeout_secs: 60,
        };
        let driver = Arc::new(RecordingBlockDriver { seen: Mutex::new(None) });
        let outcome = execute(
            ExecutionRequest {
                context: ExecutionContext {
                    job_id: Uuid::new_v4(),
                    attempt_id,
                    fencing_generation: 7,
                    repository: "stacks-network/stacks-core".into(),
                    commit: "abc123".into(),
                    repository_credential: None,
                },
                task: ExecutionTask::BlockValidation(spec.clone()),
                placement: ExecutionPlacement::default(),
            },
            ExecutionDependencies { driver: driver.clone() },
            &NoopSink,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(
            driver
                .seen
                .lock()
                .unwrap()
                .as_ref(),
            Some(&(attempt_id, 7, spec))
        );
        assert_eq!(
            outcome
                .block_validation
                .as_ref()
                .map(|output| output.checked_blocks),
            Some(3)
        );
    }

    #[tokio::test]
    async fn block_validation_cannot_complete_without_a_typed_result() {
        let outcome = execute(
            ExecutionRequest {
                context: ExecutionContext {
                    job_id: Uuid::new_v4(),
                    attempt_id: Uuid::new_v4(),
                    fencing_generation: 1,
                    repository: "stacks-network/stacks-core".into(),
                    commit: "abc123".into(),
                    repository_credential: None,
                },
                task: ExecutionTask::BlockValidation(BlockValidationTaskSpec {
                    selection: sbgh_driver::BlockValidationSelection::Range {
                        range: sbgh_driver::InclusiveRange { start: 10, end: 12 },
                    },
                    timeout_secs: 60,
                }),
                placement: ExecutionPlacement::default(),
            },
            ExecutionDependencies {
                driver: Arc::new(MissingBlockOutputDriver),
            },
            &NoopSink,
            &CancellationToken::new(),
        )
        .await
        .unwrap();

        assert!(matches!(
            outcome.status,
            TaskStatus::Failed(ref error) if error.contains("without a typed result")
        ));
        assert!(
            outcome
                .block_validation
                .is_none()
        );
    }
}
