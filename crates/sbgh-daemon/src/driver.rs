//! The backend axis (roadmap-v8 Phase 1): a task-agnostic execution substrate.
//!
//! A [`Driver`] runs a [`TaskSpec`] to completion on one host's backend
//! (libvirt today) and reports progress to the recipe-neutral
//! [`EventSink`](crate::events::EventSink). It is the *backend* counterpart to
//! [`Recipe`](crate::recipe::Recipe) (the *task* axis): `{task} × {backend}` is
//! a matrix, not one driver per task. Bench is the sole task today, so the only
//! [`TaskSpec`] carries the bench args; the seam stays open for
//! block-validation (roadmap-v6) and remote/cloud backends (roadmap-v9 worker
//! fleet) without an engine change.
//!
//! **Scope of v8 Phase 1:** the trait + neutral outcome/placement seam, with
//! the libvirt driver as the sole impl, behavior-preserving. The cloud-init
//! split (driver-owned generic shell vs. a task-supplied in-VM script) is
//! deferred to when a second task kind needs a different script (roadmap-v6).

use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::binary_cache::BinaryCache;
use crate::events::EventSink;
use crate::recipe::TaskContext;

/// What the *task* (a [`Recipe`](crate::recipe::Recipe)) hands the driver: the
/// task-supplied inputs the backend feeds to the in-VM workload. Today bench
/// fills `args` with its CLI args; the driver renders its own backend envelope
/// (cloud-init/user-data) around them — the spec carries **no** backend details
/// (roadmap-v8 Phase 1 invariant).
#[derive(Debug, Clone, Default)]
pub struct TaskSpec {
    /// The task's invocation args (bench: `bench_args`), replayed into the
    /// backend's in-VM task.
    pub args: Vec<String>,
    /// v10 (0005): build-only mode — provision + build + publish the artifact
    /// to the cache, then stop (no in-VM task phase). `false` is the
    /// benchmark shape; `true` is the `0031` warming primitive.
    pub build_only: bool,
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
    /// purely by `job_id` — the orphan-recovery primitive (roadmap-v5 Phase
    /// 4B-2): a hard-killed daemon can leave host state behind with no live
    /// handle. Returns `false` if cleanup couldn't be verified complete, so the
    /// caller leaves the row `running` to retry on the next boot.
    async fn cleanup_by_job_id(&self, job_id: &str) -> bool;

    /// The backend's binary cache, if it runs one — shared as an `Arc` so the
    /// pin manager re-pins / evicts under the **same** mutex the driver
    /// publishes under (item 0025, v9 Phase 2). Default `None` for backends
    /// without a cache; the runner builds its `PinManager` only when this is
    /// `Some`.
    fn binary_cache(&self) -> Option<Arc<BinaryCache>> {
        None
    }
}
