//! Conservative local lease deadlines, independent of transport completion.

use std::future::Future;
use std::time::Duration;

use tokio::time::{Instant, sleep_until};
use tokio_util::sync::CancellationToken;

/// Conservative server-time anchor captured before a registration request.
#[derive(Clone, Copy, Debug)]
pub struct ServerClock {
    started: Instant,
    server_time_ms: i64,
}

impl ServerClock {
    /// Anchor response server time at request dispatch, accounting for transit delay.
    pub fn new(started: Instant, server_time_ms: i64) -> Self {
        Self { started, server_time_ms }
    }

    /// Translate a server expiry to a conservative monotonic local deadline.
    fn deadline(&self, expires_at_ms: i64) -> Instant {
        let remaining = expires_at_ms
            .saturating_sub(self.server_time_ms)
            .max(0) as u64;
        self.started + Duration::from_millis(remaining)
    }
}

/// Execution authority confirmed by a request sent before the server renewed it.
pub struct Lease {
    deadline: Instant,
    ttl: Duration,
    clock: ServerClock,
}

impl Lease {
    /// Count the lease from request dispatch, never from response receipt.
    pub fn new(request_started: Instant, ttl: Duration, clock: ServerClock) -> Self {
        Self {
            deadline: request_started + ttl,
            ttl,
            clock,
        }
    }

    /// Accept a confirmation only while the previous and new deadlines are live.
    pub fn renew(&mut self, request_started: Instant, server_expiry_ms: i64) -> bool {
        let now = Instant::now();
        let renewed = (request_started + self.ttl).min(
            self.clock
                .deadline(server_expiry_ms),
        );
        if now >= self.deadline || now >= renewed {
            return false;
        }
        self.deadline = renewed;
        true
    }

    /// Bound an operation by cancellation and the last confirmed deadline.
    pub async fn during<F: Future>(
        &self,
        cancel: &CancellationToken,
        operation: F,
    ) -> Option<F::Output> {
        tokio::select! {
            biased;
            () = cancel.cancelled() => None,
            () = sleep_until(self.deadline) => None,
            result = operation => Some(result),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future;

    use super::*;
    use tokio::time::{advance, sleep};

    #[tokio::test(start_paused = true)]
    async fn stalled_rpc_expires_at_confirmed_deadline() {
        let start = Instant::now();
        let lease = Lease::new(start, Duration::from_secs(45), ServerClock::new(start, 0));
        assert!(
            lease
                .during(&CancellationToken::new(), future::pending::<()>())
                .await
                .is_none()
        );
        assert_eq!(Instant::now() - start, Duration::from_secs(45));
    }

    #[tokio::test(start_paused = true)]
    async fn delayed_confirmation_does_not_grant_response_relative_ttl() {
        let start = Instant::now();
        let mut lease = Lease::new(start, Duration::from_secs(45), ServerClock::new(start, 0));
        advance(Duration::from_secs(10)).await;
        let sent = Instant::now();
        advance(Duration::from_secs(20)).await;
        assert!(lease.renew(sent, 55_000));
        assert!(
            lease
                .during(&CancellationToken::new(), future::pending::<()>())
                .await
                .is_none()
        );
        assert_eq!(Instant::now() - start, Duration::from_secs(55));
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_wins_over_simultaneous_success_and_cannot_be_revived() {
        let start = Instant::now();
        let mut lease = Lease::new(start, Duration::from_secs(45), ServerClock::new(start, 0));
        assert!(
            lease
                .during(&CancellationToken::new(), sleep(Duration::from_secs(45)))
                .await
                .is_none()
        );
        assert!(!lease.renew(Instant::now(), 90_000));
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_interrupts_pending_rpc_without_waiting_for_expiry() {
        let start = Instant::now();
        let lease = Lease::new(start, Duration::from_secs(45), ServerClock::new(start, 0));
        let cancel = CancellationToken::new();
        cancel.cancel();
        assert!(
            lease
                .during(&cancel, future::pending::<()>())
                .await
                .is_none()
        );
        assert_eq!(Instant::now(), start);
    }

    #[tokio::test(start_paused = true)]
    async fn delayed_acceptance_consumes_initial_lease() {
        let start = Instant::now();
        advance(Duration::from_secs(46)).await;
        let lease = Lease::new(start, Duration::from_secs(45), ServerClock::new(start, 0));
        assert!(
            lease
                .during(&CancellationToken::new(), future::ready(()))
                .await
                .is_none()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn shorter_server_expiry_caps_old_registration_ttl() {
        let start = Instant::now();
        let mut lease = Lease::new(start, Duration::from_secs(45), ServerClock::new(start, 1_000));
        advance(Duration::from_secs(5)).await;
        assert!(lease.renew(Instant::now(), 11_000));
        assert!(
            lease
                .during(&CancellationToken::new(), future::pending::<()>())
                .await
                .is_none()
        );
        assert_eq!(Instant::now() - start, Duration::from_secs(10));
    }
}
