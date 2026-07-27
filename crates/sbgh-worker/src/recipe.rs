//! The task-kind boundary (roadmap-v5 / v6).
//!
//! A [`Recipe`] is one pluggable long-running task type: it runs the task in
//! its execution substrate (a VM, for now) and emits progress as recipe-neutral
//! [`WorkerEvent`](crate::events::WorkerEvent)s. The worker engine is generic
//! over this trait so adding a task kind is a new `Recipe` implementation and
//! composition entry rather than a second execution lifecycle.
//!
//! **Scope of this slice:** establish `execute` + the neutral event/outcome
//! seam. The terminal `render` (outcome → PR-comment/check body) and
//! `persist_result` (typed result rows) move behind the trait when the reporter
//! is reorganized in the next slice; today the benchmark reporter still owns
//! its rendering, so behavior is unchanged.

use async_trait::async_trait;
use sbgh_driver::{BlockValidationOutput, EventSink, TaskContext, TaskStatus};
use serde_json::json;
use tokio_util::sync::CancellationToken;

/// What the platform needs from any recipe's outcome, regardless of task kind:
/// the terminal status and a forensics `summary` blob it persists verbatim
/// (archive paths, finish reason, console tail).
pub trait TaskOutcome: Send {
    fn status(&self) -> TaskStatus;
    fn summary(&self) -> &serde_json::Value;

    fn block_validation(&self) -> Option<&BlockValidationOutput> {
        None
    }
}

/// One long-running task kind. Benchmark, build-only, and block validation all
/// implement this boundary.
#[async_trait]
pub trait Recipe: Send + Sync {
    /// The recipe's terminal outcome (see [`TaskOutcome`]).
    type Outcome: TaskOutcome;

    /// Run the task to completion in its execution substrate, emitting progress
    /// to `sink`. Returns the terminal outcome, or `Err` only for a
    /// catastrophic setup failure (the task could not start) — task-side
    /// failures come back as an `Ok(outcome)` whose `status()` is `Failed`.
    ///
    /// **Cancellation:** the recipe must honor `cancel` at *cancellation-safe*
    /// points only (never mid-provision, where an interrupted teardown could
    /// leak host state). On cancel it runs its normal teardown and returns —
    /// the worker treats `cancel.is_cancelled()` as the abort signal.
    async fn execute(
        &self,
        ctx: &TaskContext<'_>,
        sink: &dyn EventSink,
        cancel: &CancellationToken,
    ) -> anyhow::Result<Self::Outcome>;
}

/// A recipe for an unknown future `(task_kind, build_target)` combination.
/// No current creation path produces one, so this is purely defensive: fail
/// closed rather than silently routing it to the wrong backend.
pub struct UnsupportedRecipe {
    combo: String,
}

impl UnsupportedRecipe {
    pub fn new(combo: impl Into<String>) -> Self {
        Self { combo: combo.into() }
    }
}

/// Terminal outcome of an unsupported task kind — always `Failed`, with an
/// empty forensics blob (nothing ran).
pub struct UnsupportedOutcome {
    status: TaskStatus,
    summary: serde_json::Value,
}

impl TaskOutcome for UnsupportedOutcome {
    fn status(&self) -> TaskStatus {
        self.status.clone()
    }

    fn summary(&self) -> &serde_json::Value {
        &self.summary
    }
}

#[async_trait]
impl Recipe for UnsupportedRecipe {
    type Outcome = UnsupportedOutcome;

    async fn execute(
        &self,
        _ctx: &TaskContext<'_>,
        _sink: &dyn EventSink,
        _cancel: &CancellationToken,
    ) -> anyhow::Result<Self::Outcome> {
        Ok(UnsupportedOutcome {
            status: TaskStatus::Failed(format!(
                "unsupported task_kind/build_target combination `{}` is not implemented",
                self.combo
            )),
            summary: json!({ "unsupported_combo": self.combo }),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use sbgh_driver::{PhaseLabel, SinkResult};
    use uuid::Uuid;

    struct NoopSink;

    #[async_trait]
    impl EventSink for NoopSink {
        async fn phase(&self, _label: PhaseLabel, _elapsed: Duration) -> SinkResult {
            Ok(())
        }
        async fn heartbeat(&self, _label: PhaseLabel, _elapsed: Duration) {}
    }

    /// An unimplemented `task_kind` fails the run fast (with a naming reason)
    /// and touches no execution backend.
    #[tokio::test]
    async fn unsupported_recipe_fails_fast() {
        let recipe = UnsupportedRecipe::new("block_validation");
        let ctx = TaskContext {
            job_id: Uuid::nil(),
            attempt_id: Uuid::nil(),
            fencing_generation: 0,
            repository: "octo/core",
            commit: "abc",
            repository_credential: None,
        };
        let outcome = recipe
            .execute(&ctx, &NoopSink, &CancellationToken::new())
            .await
            .unwrap();
        match outcome.status() {
            TaskStatus::Failed(msg) => assert!(msg.contains("block_validation"), "reason: {msg}"),
            other => panic!("expected Failed, got {other:?}"),
        }
    }
}
