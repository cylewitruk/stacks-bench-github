//! The build-only task kind (v10 0005 / item 0031 warming).
//!
//! Builds the `build_target` binary and publishes it to the cache, then stops —
//! no in-VM workload, no measurement, no render. Selected for a
//! `task_kind = build_only` job; the daemon-initiated warming primitive
//! (`0031`) enqueues these to pre-populate the cache. The artifact build +
//! publish is the driver's existing path; this recipe just sets
//! [`TaskSpec::build_only`] so the driver stops after publishing instead of
//! running the bench phase.

use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::driver::{Driver, DriverStatus, Placement, TaskSpec};
use crate::events::EventSink;
use crate::recipe::{Recipe, TaskContext, TaskOutcome, TaskStatus};

/// Build-only recipe: provisions + builds + caches the artifact on its
/// execution backend, then stops. Carries no task args (the build is fully
/// determined by the commit + build environment).
pub struct BuildOnlyRecipe {
    driver: Arc<dyn Driver>,
    /// The cpuset this job's VM vCPUs are pinned to (its concurrency slot's
    /// `[runner].cpu_sets` entry), or `None` to float (Phase 5).
    vcpu_cpuset: Option<String>,
}

impl BuildOnlyRecipe {
    pub fn new(driver: Arc<dyn Driver>, vcpu_cpuset: Option<String>) -> Self {
        Self { driver, vcpu_cpuset }
    }
}

/// Terminal outcome of a build-only run — the driver's status + forensics blob
/// (archive dir, finish reason; no `run.json` / metric).
pub struct BuildOnlyOutcome {
    status: TaskStatus,
    summary: serde_json::Value,
}

impl TaskOutcome for BuildOnlyOutcome {
    fn status(&self) -> TaskStatus {
        self.status.clone()
    }

    fn summary(&self) -> &serde_json::Value {
        &self.summary
    }
}

#[async_trait]
impl Recipe for BuildOnlyRecipe {
    type Outcome = BuildOnlyOutcome;

    async fn execute(
        &self,
        ctx: &TaskContext<'_>,
        sink: &dyn EventSink,
        cancel: &CancellationToken,
    ) -> anyhow::Result<Self::Outcome> {
        let spec = TaskSpec {
            args: Vec::new(),
            build_only: true,
            sqlite_seed_key: None,
            shared_baseline_calibration: false,
            baseline_calibration_id: None,
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
        Ok(BuildOnlyOutcome {
            status,
            summary: outcome.summary,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::Duration;

    use uuid::Uuid;

    use super::*;
    use crate::driver::DriverOutcome;
    use crate::events::{PhaseLabel, SinkResult};

    /// A no-op [`EventSink`] — the recipe tests don't assert on progress
    /// events.
    struct NoopSink;

    #[async_trait]
    impl EventSink for NoopSink {
        async fn phase(&self, _label: PhaseLabel, _elapsed: Duration) -> SinkResult {
            Ok(())
        }
        async fn heartbeat(&self, _label: PhaseLabel, _elapsed: Duration) {}
    }

    /// A [`Driver`] that records the [`TaskSpec`] it was handed and returns a
    /// canned status — enough to assert what the recipe asked the backend to
    /// do.
    struct SpyDriver {
        seen: Mutex<Option<TaskSpec>>,
        status: DriverStatus,
    }

    #[async_trait]
    impl Driver for SpyDriver {
        async fn run_task(
            &self,
            _ctx: &TaskContext<'_>,
            spec: &TaskSpec,
            _sink: &dyn EventSink,
            _cancel: &CancellationToken,
            _placement: &Placement,
        ) -> anyhow::Result<DriverOutcome> {
            *self.seen.lock().unwrap() = Some(spec.clone());
            Ok(DriverOutcome {
                status: self.status.clone(),
                summary: serde_json::json!({ "build_only": spec.build_only }),
            })
        }
        async fn cleanup_by_job_id(&self, _job_id: &str) -> bool {
            true
        }
    }

    /// v10 (0005): the build-only recipe drives the backend in build-only mode
    /// (no task args, `build_only = true`) and maps a completed build to a
    /// completed outcome.
    #[tokio::test]
    async fn build_only_recipe_runs_driver_in_build_only_mode() {
        let driver = Arc::new(SpyDriver {
            seen: Mutex::new(None),
            status: DriverStatus::Completed,
        });
        let recipe = BuildOnlyRecipe::new(driver.clone(), Some("2-3".into()));
        let ctx = TaskContext {
            job_id: Uuid::nil(),
            repository: "octo/core",
            commit: "deadbeef",
        };

        let outcome = recipe
            .execute(&ctx, &NoopSink, &CancellationToken::new())
            .await
            .unwrap();

        let spec = driver
            .seen
            .lock()
            .unwrap()
            .clone()
            .expect("recipe ran the driver");
        assert!(spec.build_only, "build-only recipe sets TaskSpec.build_only");
        assert!(spec.args.is_empty(), "build-only carries no task args");
        assert_eq!(outcome.status(), TaskStatus::Completed);
    }
}
