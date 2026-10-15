//! Process-local shutdown supervision. Fleet policy changes use authenticated APIs.

use std::future::Future;
use std::time::Duration;

use tokio::signal::unix::{SignalKind, signal};
use tokio_util::sync::CancellationToken;

/// Local process controls; none implicitly mutates fleet scheduling or attempts.
#[derive(Clone, Default)]
pub struct Shutdown {
    /// Legacy in-process runner cancellation; never set by daemon OS signals.
    #[cfg(test)]
    pub abort: CancellationToken,
    /// Stop local intake and preparation.
    pub draining: CancellationToken,
    /// Close listeners and stop local background services.
    pub exit: CancellationToken,
    /// Drop remaining local service futures without waiting for graceful completion.
    pub forced: CancellationToken,
}

impl Shutdown {
    /// Create independent process-local shutdown controls.
    pub fn new() -> Self {
        Self::default()
    }

    /// Stop local services, preserving remote work and persistent worker policy.
    fn quiesce(&self) {
        self.draining.cancel();
        self.exit.cancel();
    }

    /// Escalate a repeated interrupt to local exit, not fleet abort.
    fn interrupt(&self) {
        if self.exit.is_cancelled() {
            self.forced.cancel();
        } else {
            self.quiesce();
        }
    }
}

/// Supervise local services and OS signals with a bounded graceful exit.
pub async fn supervise<F>(shutdown: Shutdown, grace: Duration, services: F) -> anyhow::Result<()>
where
    F: Future<Output = anyhow::Result<()>>,
{
    tokio::select! {
        result = watch_signals(shutdown.clone()) => result,
        result = supervise_services(shutdown, grace, services) => result,
    }
}

/// Bound local draining independently of remote fleet idleness.
async fn supervise_services<F>(
    shutdown: Shutdown,
    grace: Duration,
    services: F,
) -> anyhow::Result<()>
where
    F: Future<Output = anyhow::Result<()>>,
{
    tokio::select! {
        biased;
        () = shutdown.forced.cancelled() => Ok(()),
        result = services => result,
        () = async {
            shutdown.exit.cancelled().await;
            tokio::time::sleep(grace).await;
        } => {
            tracing::warn!("local shutdown grace expired; leaving remote fleet unchanged");
            Ok(())
        }
    }
}

/// SIGTERM/first SIGINT quiesce locally; a subsequent SIGINT forces local exit.
async fn watch_signals(shutdown: Shutdown) -> anyhow::Result<()> {
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;
    loop {
        tokio::select! {
            _ = sigint.recv() => shutdown.interrupt(),
            _ = sigterm.recv() => shutdown.quiesce(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future;
    use tokio::time::Instant;

    use super::*;

    #[test]
    fn signals_quiesce_then_force_local_exit() {
        let shutdown = Shutdown::new();
        shutdown.quiesce();
        assert!(shutdown.exit.is_cancelled());
        assert!(
            shutdown
                .draining
                .is_cancelled()
        );
        assert!(!shutdown.forced.is_cancelled());
        shutdown.interrupt();
        assert!(shutdown.forced.is_cancelled());
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_is_bounded_without_remote_idleness() {
        let shutdown = Shutdown::new();
        shutdown.interrupt();
        let start = Instant::now();
        supervise_services(shutdown.clone(), Duration::from_secs(10), future::pending())
            .await
            .unwrap();
        assert_eq!(Instant::now() - start, Duration::from_secs(10));
    }

    #[tokio::test(start_paused = true)]
    async fn second_interrupt_does_not_wait_for_grace() {
        let shutdown = Shutdown::new();
        shutdown.interrupt();
        shutdown.interrupt();
        let start = Instant::now();
        supervise_services(shutdown.clone(), Duration::from_secs(10), future::pending())
            .await
            .unwrap();
        assert_eq!(Instant::now(), start);
    }
}
