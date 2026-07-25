//! Task-agnostic execution backends.
//!
//! A [`Driver`] runs a [`TaskSpec`] to completion on one host's backend
//! (libvirt today) and reports progress to the recipe-neutral
//! [`EventSink`](crate::events::EventSink). It is the *backend* counterpart to
//! [`Recipe`](crate::recipe::Recipe) (the *task* axis): `{task} × {backend}` is
//! a matrix, not one driver per task. Bench is the sole task today, so the only
//! Task input is discriminated so benchmark-only fields cannot leak into
//! build-only work. The task/backend split lets future tasks and remote
//! backends compose without changing scheduler lifecycle code. The libvirt
//! driver is the sole implementation today.

use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::binary_cache::BinaryCache;
use crate::events::EventSink;
use crate::recipe::TaskContext;

/// Task-specific input handed to an execution backend. Placement remains a
/// separate axis.
#[derive(Debug, Clone)]
pub enum TaskSpec {
    Benchmark(BenchmarkTaskSpec),
    BuildOnly,
}

#[derive(Debug, Clone)]
pub struct BenchmarkTaskSpec {
    /// Benchmark CLI arguments replayed into the in-VM task.
    pub args: Vec<String>,
    pub sqlite_seed_key: Option<String>,
    pub shared_baseline_calibration: bool,
    pub baseline_calibration_id: Option<i64>,
    pub benchmark_run: BenchmarkRunContext,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BenchmarkRunContext {
    pub run_index: i32,
    pub requested_run_count: i32,
}

impl Default for BenchmarkRunContext {
    fn default() -> Self {
        Self {
            run_index: 0,
            requested_run_count: 1,
        }
    }
}

/// A backend-interpreted placement hint. Today the only knob is CPU pinning
/// (libvirt maps it to the domain's vCPU/emulator cpuset; other backends map it
/// to an instance shape or ignore it).
#[derive(Debug, Clone, Default)]
pub struct Placement {
    /// The cpuset this job's vCPUs pin to (its concurrency slot's
    /// `[runner].cpu_sets` entry), or `None` to float.
    pub vcpu_cpuset: Option<String>,
}

/// Terminal status of a driver run — completed, or failed with a message. The
/// backend-neutral successor to the libvirt module's `OutcomeStatus`; the
/// `Recipe` maps it onto its own [`TaskStatus`](crate::recipe::TaskStatus).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DriverStatus {
    Completed,
    Failed(String),
}

/// What a driver returns: the terminal status plus a forensics `summary` blob
/// the platform persists verbatim (archive paths, finish reason, console tail).
/// The summary is task-defined; the driver fills it.
#[derive(Debug)]
pub struct DriverOutcome {
    pub status: DriverStatus,
    pub summary: serde_json::Value,
}

/// One execution backend. `LibvirtDriver` is the sole impl today.
#[async_trait]
pub trait Driver: Send + Sync {
    /// Run `spec` to completion on this backend, emitting progress to `sink`.
    /// Returns the terminal outcome, or `Err` only for a catastrophic setup
    /// failure (the task could not start) — task-side failures come back as an
    /// `Ok(outcome)` whose `status` is [`DriverStatus::Failed`].
    ///
    /// **Cancellation:** honored at cancellation-safe points only (never
    /// mid-provision, where an interrupted teardown could leak host state). On
    /// cancel the driver runs its normal teardown and returns.
    async fn run_task(
        &self,
        ctx: &TaskContext<'_>,
        spec: &TaskSpec,
        sink: &dyn EventSink,
        cancel: &CancellationToken,
        placement: &Placement,
    ) -> anyhow::Result<DriverOutcome>;

    /// Best-effort, idempotent teardown of every per-job artifact addressed
    /// purely by `job_id`. A hard-killed daemon can leave host state behind
    /// with no live handle. Returns `false` if cleanup could not be verified,
    /// so the caller leaves the row `running` to retry on the next boot.
    async fn cleanup_by_job_id(&self, job_id: &str) -> bool;

    /// The backend's binary cache, if it runs one — shared as an `Arc` so the
    /// pin manager re-pins and evicts under the **same** mutex the driver
    /// publishes under. Default `None` for backends without a cache; the runner
    /// builds its `PinManager` only when this is `Some`.
    fn binary_cache(&self) -> Option<Arc<BinaryCache>> {
        None
    }
}
