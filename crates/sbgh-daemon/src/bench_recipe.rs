//! The benchmark task kind (roadmap-v5) — the sole [`Recipe`] today.
//!
//! Wraps the libvirt benchmark driver: `execute` runs it and adapts the
//! driver's `Phase` callbacks into recipe-neutral progress
//! [`EventSink`](crate::events::EventSink) calls; the outcome wraps the
//! driver's `BenchmarkOutcome`. The benchmark CLI args (`bench_args`) live
//! **here**, not on the platform-neutral [`TaskContext`].

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sbgh_core::config::DaemonConfig;
use tokio_util::sync::CancellationToken;

use crate::events::{EventSink, PhaseLabel};
use crate::libvirt::{LibvirtDriver, OutcomeStatus, Phase, PhaseListener, Shell};
use crate::recipe::{Recipe, TaskContext, TaskOutcome, TaskStatus};

/// Benchmark recipe: runs the libvirt benchmark driver with this run's
/// `bench_args` (a benchmark-specific input the platform never sees).
pub struct BenchRecipe {
    config: Arc<DaemonConfig>,
    shell: Arc<dyn Shell>,
    bench_args: Vec<String>,
}

impl BenchRecipe {
    pub fn new(config: Arc<DaemonConfig>, shell: Arc<dyn Shell>, bench_args: Vec<String>) -> Self {
        Self { config, shell, bench_args }
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
        let driver = LibvirtDriver::new(self.config.clone(), self.shell.clone());
        let adapter = SinkAdapter { sink };
        let outcome = driver
            .run_benchmark(ctx, &self.bench_args, &adapter, cancel)
            .await?;
        let status = match outcome.status {
            OutcomeStatus::Ok => TaskStatus::Completed,
            OutcomeStatus::Failed(e) => TaskStatus::Failed(e),
        };
        Ok(BenchOutcome {
            status,
            summary: outcome.summary,
        })
    }
}

/// Bridges the libvirt driver's `Phase` callbacks to recipe-neutral
/// [`EventSink`] calls. The `Phase` → `PhaseLabel` mapping lives here so the
/// event layer stays task-agnostic.
struct SinkAdapter<'a> {
    sink: &'a dyn EventSink,
}

impl SinkAdapter<'_> {
    fn label(phase: &Phase) -> PhaseLabel {
        PhaseLabel::new(phase.label(), phase.is_terminal())
    }
}

#[async_trait]
impl PhaseListener for SinkAdapter<'_> {
    async fn on_phase(&self, phase: &Phase) {
        // A just-entered phase → 0 elapsed (matches the prior reporting path,
        // whose prose still reads naturally as "running for 00:00:00").
        //
        // A phase is a RELIABLE event (roadmap-v5 channel discipline): it must
        // not be lost. A `SinkClosed` here means the reporter is gone (it
        // panicked) — surfaced loudly below.
        //
        // TODO(Phase 4 — cancel-safety): *acting* on this (aborting the in-flight
        // run rather than logging-and-continuing) needs the driver cancellation
        // path + `cleanup_by_job_id` Phase 4 builds — a dirty drop here would
        // leak the VM. The `PhaseListener` boundary will grow an abort signal
        // there (`on_phase` returns `()` and the driver has no cancellation path
        // yet). Do not copy this swallow forward without that abort handling.
        if self
            .sink
            .phase(Self::label(phase), Duration::ZERO)
            .await
            .is_err()
        {
            tracing::error!(
                phase = %phase,
                "reliable phase event dropped: reporting sink closed (reporter gone) — \
                 VM-safe abort deferred to Phase 4 cancel-safety (see TODO)",
            );
        }
    }

    async fn on_heartbeat(&self, phase: &Phase, elapsed: Duration) {
        self.sink
            .heartbeat(Self::label(phase), elapsed)
            .await;
    }
}
