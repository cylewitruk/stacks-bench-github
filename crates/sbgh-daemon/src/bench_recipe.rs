//! The benchmark task kind (roadmap-v5) — the sole [`Recipe`] today.
//!
//! Builds a benchmark [`TaskSpec`] (this run's `bench_args`) + [`Placement`]
//! (its Phase-5 cpuset) and runs it on an injected [`Driver`]
//! (`LibvirtDriver` today — roadmap-v8 Phase 1), mapping the driver's neutral
//! [`DriverOutcome`](crate::driver::DriverOutcome) onto the recipe's
//! [`TaskOutcome`]. The benchmark CLI args live **here**, not on the
//! platform-neutral [`TaskContext`].

use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::driver::{Driver, DriverStatus, Placement, TaskSpec};
use crate::events::EventSink;
use crate::recipe::{Recipe, TaskContext, TaskOutcome, TaskStatus};

/// Benchmark recipe: runs the benchmark task on its execution backend with this
/// run's `bench_args` (a benchmark-specific input the platform never sees) and
/// cpuset.
pub struct BenchRecipe {
    driver: Arc<dyn Driver>,
    bench_args: Vec<String>,
    /// The cpuset this job's VM vCPUs are pinned to (its concurrency slot's
    /// `[runner].cpu_sets` entry), or `None` to float (Phase 5).
    vcpu_cpuset: Option<String>,
}

impl BenchRecipe {
    pub fn new(
        driver: Arc<dyn Driver>,
        bench_args: Vec<String>,
        vcpu_cpuset: Option<String>,
    ) -> Self {
        Self {
            driver,
            bench_args,
            vcpu_cpuset,
        }
    }
}

/// Terminal outcome of a benchmark run — the driver's status + forensics blob.
pub struct BenchOutcome {
    status: TaskStatus,
    summary: serde_json::Value,
}

impl TaskOutcome for BenchOutcome {
    fn status(&self) -> TaskStatus {
        self.status.clone()
    }

    fn summary(&self) -> &serde_json::Value {
        &self.summary
    }
}

#[async_trait]
impl Recipe for BenchRecipe {
    type Outcome = BenchOutcome;

    async fn execute(
        &self,
        ctx: &TaskContext<'_>,
        sink: &dyn EventSink,
        cancel: &CancellationToken,
    ) -> anyhow::Result<Self::Outcome> {
        let spec = TaskSpec {
            args: self.bench_args.clone(),
            build_only: false,
        };
        let placement = Placement {
            vcpu_cpuset: self.vcpu_cpuset.clone(),
        };
        let outcome = self
            .driver
            .run_task(ctx, &spec, sink, cancel, &placement)
            .await?;
        let status = match outcome.status {
            DriverStatus::Completed => TaskStatus::Completed,
            DriverStatus::Failed(e) => TaskStatus::Failed(e),
        };
        Ok(BenchOutcome {
            status,
            summary: outcome.summary,
        })
    }
}
