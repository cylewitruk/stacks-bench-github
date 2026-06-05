//! The task-kind boundary (roadmap-v5 / v6).
//!
//! A [`Recipe`] is one pluggable long-running task type: it runs the task in
//! its execution substrate (a VM, for now) and emits progress as recipe-neutral
//! [`WorkerEvent`](crate::events::WorkerEvent)s. The engine (the runner today,
//! the coordinator/worker later) is generic over this trait so adding a task
//! kind — block validation next — is a new `Recipe` impl rather than an engine
//! change.
//!
//! **Scope of this slice:** establish `execute` + the neutral event/outcome
//! seam. The terminal `render` (outcome → PR-comment/check body) and
//! `persist_result` (typed result rows) move behind the trait when the reporter
//! is reorganized in the next slice; today the benchmark reporter still owns
//! its rendering, so behavior is unchanged.

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::events::EventSink;

/// Platform-neutral execution context handed to every recipe: the identity and
/// target the platform resolved, with **no** task-specific fields. A recipe's
/// own input (e.g. the benchmark CLI args) is owned by the recipe, not carried
/// here — so a new task kind doesn't inherit another's fields.
pub struct TaskContext<'a> {
    pub job_id: Uuid,
    /// `owner/name`. Drives the git clone URL.
    pub repository: &'a str,
    /// Resolved commit/SHA to operate on.
    pub commit: &'a str,
}

/// Terminal status of a recipe run: it completed, or it failed with a message.
/// Distinct from the *result* (the perf numbers) — a completed-but-regressed
/// run is still `Completed`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskStatus {
    Completed,
    Failed(String),
}

/// What the platform needs from any recipe's outcome, regardless of task kind:
/// the terminal status and a forensics `summary` blob it persists verbatim
/// (archive paths, finish reason, console tail).
pub trait TaskOutcome: Send {
    fn status(&self) -> TaskStatus;
    fn summary(&self) -> &serde_json::Value;
}

/// One long-running task kind. `BenchRecipe` is the sole impl today.
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
