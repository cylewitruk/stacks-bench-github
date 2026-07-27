//! Sandboxed block-validation recipe.

use std::sync::Arc;

use async_trait::async_trait;
use sbgh_driver::{
    BlockValidationOutput, BlockValidationTaskSpec, Driver, DriverStatus, DriverTaskOutput,
    EventSink, Placement, TaskContext, TaskSpec, TaskStatus,
};
use tokio_util::sync::CancellationToken;

use crate::recipe::{Recipe, TaskOutcome};

pub struct BlockValidationRecipe {
    driver: Arc<dyn Driver>,
    spec: BlockValidationTaskSpec,
    vcpu_cpuset: Option<String>,
}

impl BlockValidationRecipe {
    pub fn new(
        driver: Arc<dyn Driver>,
        spec: BlockValidationTaskSpec,
        vcpu_cpuset: Option<String>,
    ) -> Self {
        Self { driver, spec, vcpu_cpuset }
    }
}

pub struct BlockValidationOutcome {
    status: TaskStatus,
    summary: serde_json::Value,
    output: Option<BlockValidationOutput>,
}

impl TaskOutcome for BlockValidationOutcome {
    fn status(&self) -> TaskStatus {
        self.status.clone()
    }

    fn summary(&self) -> &serde_json::Value {
        &self.summary
    }

    fn block_validation(&self) -> Option<&BlockValidationOutput> {
        self.output.as_ref()
    }
}

#[async_trait]
impl Recipe for BlockValidationRecipe {
    type Outcome = BlockValidationOutcome;

    async fn execute(
        &self,
        ctx: &TaskContext<'_>,
        sink: &dyn EventSink,
        cancel: &CancellationToken,
    ) -> anyhow::Result<Self::Outcome> {
        let outcome = self
            .driver
            .run_task(
                ctx,
                &TaskSpec::BlockValidation(self.spec.clone()),
                sink,
                cancel,
                &Placement {
                    vcpu_cpuset: self.vcpu_cpuset.clone(),
                },
            )
            .await?;
        let (status, output) = match (outcome.status, outcome.output) {
            (DriverStatus::Completed, DriverTaskOutput::BlockValidation(output)) => {
                (TaskStatus::Completed, Some(output))
            }
            (DriverStatus::Completed, DriverTaskOutput::None) => (
                TaskStatus::Failed(
                    "block-validation backend completed without a typed result".into(),
                ),
                None,
            ),
            (DriverStatus::Failed(error), _) => (TaskStatus::Failed(error), None),
        };
        Ok(BlockValidationOutcome {
            status,
            summary: outcome.summary,
            output,
        })
    }
}
