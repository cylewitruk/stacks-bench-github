//! Lifecycle shutdown signalling (roadmap-v5 Phase 4B).
//!
//! Three [`CancellationToken`]s coordinate a clean exit:
//!
//!   - **`abort`** — fired on *Abort*. Parent of every job's child token, so
//!     cancelling it aborts all in-flight runs at once (each driver observes it
//!     at a poll-loop safe point and tears its VM down; see Phase 4A).
//!   - **`draining`** — set on *Drain* or *Abort*. The coordinator stops
//!     claiming new jobs once it's set.
//!   - **`exit`** — fired by the coordinator once it has drained/aborted and is
//!     idle. The API server + webhook processor observe it and stop, so the
//!     top-level `try_join!` completes and the process exits. The coordinator
//!     is the *sole* trigger, so there's always a definite party that ends the
//!     run.
//!
//! Signal → mode:
//!
//! | Signal | Mode |
//! | ---- | ---- |
//! | `SIGINT` ×1 (terminal `ctrl-c`) | **Drain** — stop claiming, let in-flight finish |
//! | `SIGINT` ×2 | **Abort** (escalate) |
//! | `SIGTERM` (`systemctl stop`) | **Abort** — cancel in-flight |

use tokio::signal::unix::{SignalKind, signal};
use tokio_util::sync::CancellationToken;

/// The shared shutdown control signals (see module docs). Cheap to clone — each
/// field is a `CancellationToken` handle.
#[derive(Clone, Default)]
pub struct Shutdown {
    /// Fired on Abort → cancels in-flight workers.
    pub abort: CancellationToken,
    /// Set on Drain or Abort → the coordinator stops claiming.
    pub draining: CancellationToken,
    /// Fired by the coordinator when drained + idle → the other arms stop.
    pub exit: CancellationToken,
}

impl Shutdown {
    pub fn new() -> Self {
        Self::default()
    }
}

/// Translate process signals into shutdown modes. Runs as a top-level task.
///
/// It only ever *requests* drain/abort — it never ends the process. The
/// **coordinator** is the sole party that fires `exit` (after its in-flight
/// runs have aborted + cleaned up), so this task always waits for `exit` before
/// returning. That keeps the "coordinator owns process exit" invariant true
/// even if the top-level join is ever restructured.
pub async fn watch_signals(shutdown: Shutdown) -> anyhow::Result<()> {
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;

    loop {
        tokio::select! {
            _ = sigint.recv() => {
                if shutdown.draining.is_cancelled() {
                    // Second ctrl-c → escalate a drain to an abort.
                    tracing::warn!("second SIGINT — aborting in-flight runs");
                    shutdown.abort.cancel();
                    break;
                }
                tracing::warn!("SIGINT — draining (ctrl-c again to abort in-flight)");
                shutdown.draining.cancel();
            }
            _ = sigterm.recv() => {
                tracing::warn!("SIGTERM — aborting in-flight runs");
                shutdown.draining.cancel();
                shutdown.abort.cancel();
                break;
            }
            // The coordinator finished a drain on its own — done.
            _ = shutdown.exit.cancelled() => return Ok(()),
        }
    }

    // Abort requested — stay alive until the coordinator has fully aborted +
    // drained and fired `exit`, so this task never ends the process before 4A's
    // cleanup/reporting completes.
    shutdown
        .exit
        .cancelled()
        .await;
    Ok(())
}
