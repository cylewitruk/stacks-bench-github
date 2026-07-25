//! Recipe-neutral execution events (roadmap-v5).
//!
//! A running task (the **worker**) reports progress to an [`EventSink`] using
//! an opaque [`PhaseLabel`] rather than any task-specific phase type, so the
//! same surface serves benchmarking today and other long-running task kinds
//! later (roadmap-v6). The worker emits through an event sink over a bounded
//! per-job channel; a separate **reporter** task
//! drains the [`WorkerEvent`] stream onto the PR comment + Check Run.
//!
//! Delivery is **two-tier** (roadmap-v5 channel discipline): a phase transition
//! is **reliable** — it must not be silently dropped, so [`EventSink::phase`]
//! returns a [`SinkResult`] (`Err(SinkClosed)` if the reporter is gone);
//! heartbeats and fine-grained progress are **best-effort** and may be dropped
//! under backpressure, so [`EventSink::heartbeat`] and [`EventSink::progress`]
//! have no failure signal by design. The terminal [`Terminal`] outcome rides
//! the same channel as [`WorkerEvent::Finished`].

use std::fmt;
use std::time::Duration;

use async_trait::async_trait;

/// An opaque, task-agnostic phase name plus whether it is a terminal phase.
///
/// A recipe maps its own phase model onto this (e.g. the benchmark driver's
/// `building`/`running`/`done`) so consumers never see task-specific types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhaseLabel {
    name: String,
    is_terminal: bool,
}

impl PhaseLabel {
    pub fn new(name: impl Into<String>, is_terminal: bool) -> Self {
        Self { name: name.into(), is_terminal }
    }

    /// Whether this phase ends the run. Terminal phases bypass the reporting
    /// debounce so the final state is shown immediately.
    pub fn is_terminal(&self) -> bool {
        self.is_terminal
    }
}

impl fmt::Display for PhaseLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.name)
    }
}

/// The event consumer (reporter) is gone, so a **reliable** event could not be
/// delivered: a channel-backed sink returns this when the receiver (the
/// reporter task) has dropped, for example if it panicked mid-run.
#[derive(Debug, Clone, Copy)]
pub struct SinkClosed;

impl fmt::Display for SinkClosed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("event sink closed (reporter gone)")
    }
}

impl std::error::Error for SinkClosed {}

/// Result of a **reliable** [`EventSink::phase`] send.
pub type SinkResult = Result<(), SinkClosed>;

/// Which workflow step produced a fine-grained progress event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowStep {
    Calibrate,
    Run,
}

impl fmt::Display for WorkflowStep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Calibrate => "calibrate",
            Self::Run => "run",
        })
    }
}

/// Best-effort fine-grained progress parsed from `stacks-bench` JSONL stderr.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProgressUpdate {
    pub workflow_step: WorkflowStep,
    pub run_index: i32,
    pub requested_run_count: i32,
    pub phase: String,
    pub progress: u64,
    pub total: Option<u64>,
    pub message: Option<String>,
}

/// Consumes a recipe's progress events. The reporting surface (PR comment +
/// Check Run) implements this; a recipe emits to it without knowing what is
/// downstream.
#[async_trait]
pub trait EventSink: Send + Sync {
    /// Deliver a phase transition. **Reliable:** a phase (and later the
    /// terminal event) must not be silently dropped — returns
    /// `Err(SinkClosed)` if the consumer is gone. `elapsed` is the time in the
    /// new phase (0 on entry).
    async fn phase(&self, label: PhaseLabel, elapsed: Duration) -> SinkResult;

    /// Deliver a periodic "still alive" tick within the current phase.
    /// **Best-effort:** may be dropped under backpressure; no failure signal.
    async fn heartbeat(&self, label: PhaseLabel, elapsed: Duration);

    /// Deliver fine-grained task progress. **Best-effort:** may be dropped
    /// under backpressure; no failure signal.
    async fn progress(&self, _progress: ProgressUpdate) {}
}

/// The terminal result of a run, carried on [`WorkerEvent::Finished`]. The
/// worker extracts these from the recipe's outcome so the channel + reporter
/// stay non-generic.
#[derive(Debug)]
pub enum Terminal {
    /// The task ran and produced results.
    Completed { summary: serde_json::Value },
    /// The task ran but failed (VM died, timeout, …); `summary` is the
    /// forensics blob.
    Failed { error: String, summary: serde_json::Value },
    /// The task could not start (setup error) — no forensics. The run loop
    /// treats this as an iteration error (backoff), matching the prior
    /// `recipe.execute` `Err` path.
    SetupError { error: String },
    /// The run was **cancelled** (operator shutdown/abort, roadmap-v5 Phase 4)
    /// and its host artifacts cleaned up. The reporter records a terminal
    /// failure ("aborted by shutdown") — a clean ✗, re-triggerable.
    Aborted,
}

/// A progress event sent from the worker to its reporter over the per-job
/// channel.
#[derive(Debug)]
pub enum WorkerEvent {
    /// A phase transition. `elapsed` is the time in the new phase (0 on entry).
    Phase { label: PhaseLabel, elapsed: Duration },
    /// A periodic "still alive" tick within the current phase.
    Heartbeat { label: PhaseLabel, elapsed: Duration },
    /// Fine-grained task progress parsed from the worker's JSONL progress file.
    Progress(ProgressUpdate),
    /// The run is over — its terminal outcome.
    Finished(Terminal),
}
