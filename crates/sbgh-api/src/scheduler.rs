//! Scheduler maintenance request and status contracts.

use serde::{Deserialize, Serialize};

/// An explicitly authorized, retry-safe maintenance request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchedulerChangeRequest {
    /// UUID retained when retrying a request after an uncertain response.
    pub request_id: String,
    /// Operator-provided explanation; never an authenticated identity.
    pub reason: String,
}

/// Durable admission policy returned by status and administrative operations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchedulerPolicyView {
    /// Whether new work is barred from preparation, offer, and acceptance.
    pub paused: bool,
    /// Monotonic revision, suitable for detecting intervening maintenance changes.
    pub generation: i64,
    /// Authenticated principal responsible for the last change.
    pub actor: String,
    /// Explanation of the last change.
    pub reason: String,
    /// RFC 3339 database timestamp.
    pub changed_at: String,
}

/// Point-in-time maintenance status; quiescence is not a future reservation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulerStatusView {
    /// Current durable admission policy.
    pub policy: SchedulerPolicyView,
    /// Offered, running, or cancelling attempts.
    pub active_attempts: i64,
    /// Pending worker teardown acknowledgements.
    pub pending_cleanup: i64,
    /// Attempts with unprojected reliable events.
    pub pending_projection: i64,
    /// Active-attempt uploads or accepted-manifest promotion debt, including empty files.
    pub pending_artifacts: i64,
    /// Unreferenced staging retained for garbage collection; does not block maintenance.
    #[serde(default)]
    pub garbage_artifacts: i64,
}

impl SchedulerStatusView {
    /// Whether maintenance can proceed at this observed policy generation.
    pub fn quiescent(&self) -> bool {
        self.policy.paused
            && self.active_attempts == 0
            && self.pending_cleanup == 0
            && self.pending_projection == 0
            && self.pending_artifacts == 0
    }
}

#[cfg(test)]
mod tests {
    use super::{SchedulerPolicyView, SchedulerStatusView};

    #[test]
    fn scheduler_quiescence_requires_paused_policy_and_every_counter_zero() {
        let ready = SchedulerStatusView {
            policy: SchedulerPolicyView {
                paused: true,
                generation: 1,
                actor: "api:admin".into(),
                reason: "maintenance".into(),
                changed_at: "2026-10-16T00:00:00Z".into(),
            },
            active_attempts: 0,
            pending_cleanup: 0,
            pending_projection: 0,
            pending_artifacts: 0,
            garbage_artifacts: 7,
        };
        assert!(ready.quiescent());
        let mut running = ready.clone();
        running.policy.paused = false;
        assert!(!running.quiescent());
        for counter in 0..4 {
            for count in [-1, 1] {
                let mut busy = ready.clone();
                let counters = [
                    &mut busy.active_attempts,
                    &mut busy.pending_cleanup,
                    &mut busy.pending_projection,
                    &mut busy.pending_artifacts,
                ];
                *counters[counter] = count;
                assert!(!busy.quiescent(), "counter {counter} with value {count}");
            }
        }
    }
}
