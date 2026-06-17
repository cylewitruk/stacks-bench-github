//! Group-scoped Slack reporting session (v18, item `0047`).
//!
//! A benchmark group shares **one** Slack card/stream across all its runs, but
//! the runner builds a reporting surface **per run**. v17 attached the stream
//! keepalive to that per-run surface, so an intermediate repeat's terminal
//! aborted it and the shared stream lapsed during the inter-run gap.
//!
//! A [`SlackSession`] owns the shared [`SlackTimeline`] and its keepalive for
//! the whole group's lifetime; per-run [`SlackReportSurface`]s borrow it from
//! the [`SlackSessionRegistry`] and reap it only on a **group-terminal** event.
//! The key includes the Slack target identity so the registry never bakes in
//! "one group, one destination, forever".
//!
//! [`SlackReportSurface`]: crate::report::SlackReportSurface

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::slack::timeline::SlackTimeline;

/// Identity of a Slack reporting destination — the thread the card lives in.
/// Part of the session key (alongside the group id) so future multi-destination
/// reporting isn't designed out.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SlackTarget {
    pub channel: String,
    pub thread_ts: String,
}

/// A group-scoped Slack reporting session: owns the shared live card
/// ([`SlackTimeline`]) and its stream-keepalive for the whole group's lifetime.
/// Per-run surfaces borrow the timeline and call [`ensure_keepalive`]; the
/// session is reaped (keepalive aborted, dropped from the registry) on a
/// group-terminal event.
///
/// [`ensure_keepalive`]: SlackSession::ensure_keepalive
pub struct SlackSession {
    timeline: std::sync::Arc<SlackTimeline>,
    keepalive: Mutex<Option<JoinHandle<()>>>,
    /// Last time a run actively used this session (built / completed). The
    /// abandonment sweep only reaps a session idle past a grace TTL, so this is
    /// refreshed at each run's bookends — never by the (always-looping)
    /// keepalive.
    last_touched: Mutex<Instant>,
}

impl SlackSession {
    fn new(timeline: std::sync::Arc<SlackTimeline>) -> Self {
        Self {
            timeline,
            keepalive: Mutex::new(None),
            last_touched: Mutex::new(Instant::now()),
        }
    }

    /// The shared group card. Per-run surfaces drive it via the usual timeline
    /// API (`begin_run`/`started`/`advance`/terminal).
    pub fn timeline(&self) -> &std::sync::Arc<SlackTimeline> {
        &self.timeline
    }

    /// Mark the session as actively used by a run (resets the abandonment
    /// clock). Called on get-or-create and on successful run completion;
    /// failure/cancel reap the session immediately instead.
    pub fn touch(&self) {
        *self
            .last_touched
            .lock()
            .unwrap() = Instant::now();
    }

    fn idle_for(&self) -> Duration {
        self.last_touched
            .lock()
            .unwrap()
            .elapsed()
    }

    /// Spawn the stream keepalive once, idempotently. Called after the first
    /// `begin_run + started`, when there's a live in-progress row to warm (the
    /// keepalive no-ops at `stage == 0`, so spawning at creation would let it
    /// exit before the first update).
    pub fn ensure_keepalive(&self) {
        let mut guard = self.keepalive.lock().unwrap();
        if guard.is_none() {
            *guard = Some(
                self.timeline
                    .spawn_keepalive(),
            );
        }
    }

    /// Whether the keepalive task is currently spawned (tests).
    #[cfg(test)]
    pub fn keepalive_running(&self) -> bool {
        self.keepalive
            .lock()
            .unwrap()
            .is_some()
    }

    /// Abort the keepalive (on reap or drop). Idempotent.
    fn stop_keepalive(&self) {
        if let Some(handle) = self
            .keepalive
            .lock()
            .unwrap()
            .take()
        {
            handle.abort();
        }
    }
}

impl Drop for SlackSession {
    fn drop(&mut self) {
        if let Some(handle) = self
            .keepalive
            .get_mut()
            .unwrap()
            .take()
        {
            handle.abort();
        }
    }
}

/// Live group-scoped Slack sessions, keyed by `(benchmark_group_id,
/// SlackTarget)`. Daemon-held and shared into the runner/reporter; per-run
/// surfaces get-or-create their session and reap it on a group-terminal event.
#[derive(Default)]
pub struct SlackSessionRegistry {
    sessions: Mutex<HashMap<(Uuid, SlackTarget), std::sync::Arc<SlackSession>>>,
}

impl SlackSessionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// The session for `(group_id, target)`, creating it from `make_timeline`
    /// (invoked only on first use) if absent. Touches it either way — building
    /// a run's surface is run activity, which resets the abandonment clock.
    pub fn get_or_create(
        &self,
        group_id: Uuid,
        target: SlackTarget,
        make_timeline: impl FnOnce() -> std::sync::Arc<SlackTimeline>,
    ) -> std::sync::Arc<SlackSession> {
        let session = self
            .sessions
            .lock()
            .unwrap()
            .entry((group_id, target))
            .or_insert_with(|| std::sync::Arc::new(SlackSession::new(make_timeline())))
            .clone();
        session.touch();
        session
    }

    /// Reap sessions abandoned mid-group: idle past `grace` **and** whose group
    /// has no active (queued/running) run — `active_groups`. Both conditions
    /// are required: "no active run right now" alone is briefly true during
    /// a repeat group's inter-run carry-forward gap, which `grace` bridges.
    /// Returns the number reaped. (Phase 3, 0047.)
    pub fn sweep_abandoned(&self, grace: Duration, active_groups: &HashSet<Uuid>) -> usize {
        let mut sessions = self.sessions.lock().unwrap();
        let stale: Vec<(Uuid, SlackTarget)> = sessions
            .iter()
            .filter(|((group, _), session)| {
                !active_groups.contains(group) && session.idle_for() > grace
            })
            .map(|(key, _)| key.clone())
            .collect();
        for key in &stale {
            if let Some(session) = sessions.remove(key) {
                session.stop_keepalive();
            }
        }
        stale.len()
    }

    /// Reap the session for `(group_id, target)`: abort its keepalive and drop
    /// it from the registry. Idempotent — a surface still holding an `Arc`
    /// keeps the object alive, so the abort is explicit rather than relying
    /// on `Drop`.
    pub fn reap(&self, group_id: Uuid, target: &SlackTarget) {
        let removed = self
            .sessions
            .lock()
            .unwrap()
            .remove(&(group_id, target.clone()));
        if let Some(session) = removed {
            session.stop_keepalive();
        }
    }

    /// The live session for `(group_id, target)`, if any (tests).
    #[cfg(test)]
    pub fn get(
        &self,
        group_id: Uuid,
        target: &SlackTarget,
    ) -> Option<std::sync::Arc<SlackSession>> {
        self.sessions
            .lock()
            .unwrap()
            .get(&(group_id, target.clone()))
            .cloned()
    }

    /// Number of live sessions.
    pub fn len(&self) -> usize {
        self.sessions
            .lock()
            .unwrap()
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
