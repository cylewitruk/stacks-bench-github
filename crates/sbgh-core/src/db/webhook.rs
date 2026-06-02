//! `WebhookInbox` — the data-layer trait the daemon's webhook
//! processor uses to claim, classify, and retire rows in
//! `github_webhook`. Slice 2a introduces the trait + impls; slice 2b
//! plugs in actual classification logic.
//!
//! The state machine the trait operates over (mirrors the DB enums):
//!
//! ```text
//!   received ──claim─→ processing ──complete(outcome)─→ processed|ignored|denied
//!                          │
//!                          ├──record_retryable_error─→ retryable_error ──claim─→ processing ...
//!                          │
//!                          └──record_permanent_failure─→ failed (outcome=error)
//!
//!   processing ──sweep_stuck_claims (lease timeout)─→ retryable_error
//! ```
//!
//! All mutating operations (except `sweep_stuck_claims`) are conditional
//! on `claim_token` matching the row's current value. If the sweeper has
//! reclaimed the row mid-flight, a stale processor's write becomes a
//! no-op rather than corrupting state.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use uuid::Uuid;

use crate::Result;
use crate::models::WebhookOutcome;

/// The subset of a `github_webhook` row the processor receives at
/// claim time. Excludes columns the processor doesn't read (status,
/// outcome, claimed_at, etc.) and columns it can derive from headers
/// (next_attempt_at). `claim_token` is allocated by `claim_next` and
/// must be passed back on every subsequent mutating call.
#[derive(Debug, Clone)]
pub struct ClaimedWebhook {
    pub id: i64,
    pub claim_token: Uuid,
    pub delivery_id: String,
    pub event_type: String,
    pub action: Option<String>,
    pub payload_installation_id: Option<i64>,
    pub payload: Option<Value>,
    pub payload_size_bytes: i32,
    pub attempts: i32,
    pub received_at: DateTime<Utc>,
}

#[async_trait]
pub trait WebhookInbox: Send + Sync + 'static {
    /// Atomically claim the oldest row in `received` or
    /// `retryable_error` whose `next_attempt_at` is in the past AND
    /// whose `event_type` is in the supplied filter list. Sets
    /// status=`processing`, claimed_at=NOW, allocates a fresh
    /// `claim_token`. Returns `None` if nothing is claimable.
    ///
    /// The event-type filter prevents the processor from terminalizing
    /// rows for event types it doesn't yet know how to classify
    /// (e.g., a slice-2b processor passes `["issue_comment"]` so
    /// `installation.created` rows survive in `received` for the
    /// slice-3 processor to consume). Passing an empty slice claims
    /// nothing.
    ///
    /// Postgres impl uses `FOR UPDATE SKIP LOCKED` so concurrent
    /// processors safely pick disjoint rows.
    async fn claim_next(&self, event_types: &[&str]) -> Result<Option<ClaimedWebhook>>;

    /// Mark a claimed row terminal with the given outcome. Status is
    /// derived via `outcome.terminal_status()`. Conditional on
    /// `claim_token` matching; returns Ok with no-op if it doesn't
    /// (the sweeper reclaimed mid-flight).
    async fn complete(&self, id: i64, claim_token: Uuid, outcome: WebhookOutcome) -> Result<()>;

    /// Record a transient processing failure. Increments `attempts`,
    /// sets `last_error`, sets `next_attempt_at` to the caller-supplied
    /// retry time, status=`retryable_error`. Conditional on
    /// `claim_token` matching.
    async fn record_retryable_error(
        &self,
        id: i64,
        claim_token: Uuid,
        error: &str,
        next_attempt_at: DateTime<Utc>,
    ) -> Result<()>;

    /// Record a permanent failure (typically: attempts exhausted).
    /// Sets status=`failed`, outcome=`error`, `last_error`.
    /// Conditional on `claim_token` matching.
    async fn record_permanent_failure(&self, id: i64, claim_token: Uuid, error: &str)
    -> Result<()>;

    /// Reset `processing` rows whose `claimed_at` is older than NOW
    /// minus the lease window back to `retryable_error` so they can be
    /// reclaimed. Clears `claim_token` and `claimed_at`. Returns the
    /// number of rows recovered. Run periodically by the processor's
    /// outer loop.
    async fn sweep_stuck_claims(&self, lease: chrono::Duration) -> Result<u64>;

    /// Slice 7 (pre-slice-6 checkpoint todo): NULL the `payload` JSONB
    /// on terminal rows whose `processed_at` is older than the
    /// supplied retention window. Honours the inbox's "bounded
    /// payload retention" principle: we keep payloads while a row is
    /// in-flight, but a closed-book terminal row only needs the
    /// outcome + last_error (we never re-evaluate from the payload
    /// once an outcome is committed).
    ///
    /// `payload_size_bytes` and `last_error` are preserved — ops
    /// queries still see distribution metrics and failure reasons
    /// after the payload itself is gone.
    ///
    /// Eligible rows: `status IN ('ignored', 'denied', 'failed')`
    /// AND `payload IS NOT NULL` AND `processed_at < NOW() - $1`.
    /// `processed`-status rows (the `enqueued_job` / `would_enqueue_job`
    /// outcomes) are intentionally NOT cleared — slice 9+ may want
    /// the payload for job-context construction, and we can revisit
    /// once that flow is real.
    ///
    /// Returns the number of rows whose payload was cleared.
    async fn clear_terminal_payloads(&self, retention: chrono::Duration) -> Result<u64>;
}
