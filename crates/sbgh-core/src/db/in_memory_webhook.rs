//! In-memory `WebhookInbox` for unit tests of the processor scaffold.
//! Models the same state machine as the Postgres impl: claim, complete,
//! retryable error, permanent failure, stuck-claim sweep. A single
//! `Mutex` around the row collection serializes all operations, which
//! is enough to exercise concurrency semantics deterministically.

use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, Ordering};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use uuid::Uuid;

use crate::Result;
use crate::db::webhook::{ClaimedWebhook, WebhookInbox};
use crate::models::{WebhookOutcome, WebhookStatus};

/// A single row's worth of state, mirroring the github_webhook table.
#[derive(Debug, Clone)]
pub struct InMemoryWebhookRow {
    pub id: i64,
    pub delivery_id: String,
    pub event_type: String,
    pub action: Option<String>,
    pub payload_installation_id: Option<i64>,
    pub payload: Option<Value>,
    pub payload_size_bytes: i32,
    pub received_at: DateTime<Utc>,
    pub status: WebhookStatus,
    pub outcome: Option<WebhookOutcome>,
    pub claimed_at: Option<DateTime<Utc>>,
    pub claim_token: Option<Uuid>,
    pub next_attempt_at: DateTime<Utc>,
    pub attempts: i32,
    pub last_error: Option<String>,
    pub processed_at: Option<DateTime<Utc>>,
}

/// Test-side seed for fresh inbox rows.
#[derive(Debug, Clone, Default)]
pub struct SeedWebhook {
    pub delivery_id: String,
    pub event_type: String,
    pub action: Option<String>,
    pub payload_installation_id: Option<i64>,
    pub payload: Option<Value>,
    pub payload_size_bytes: i32,
    /// Override for received_at; defaults to NOW.
    pub received_at: Option<DateTime<Utc>>,
}

pub struct InMemoryWebhookInbox {
    rows: Mutex<Vec<InMemoryWebhookRow>>,
    next_id: AtomicI64,
}

impl Default for InMemoryWebhookInbox {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryWebhookInbox {
    pub fn new() -> Self {
        Self {
            rows: Mutex::new(Vec::new()),
            next_id: AtomicI64::new(1),
        }
    }

    /// Seed a fresh row in `received` state. Returns the assigned id.
    /// Test helper — production inserts go through `IngestStore`.
    pub fn seed(&self, seed: SeedWebhook) -> i64 {
        let id = self
            .next_id
            .fetch_add(1, Ordering::SeqCst);
        let now = Utc::now();
        self.rows
            .lock()
            .unwrap()
            .push(InMemoryWebhookRow {
                id,
                delivery_id: seed.delivery_id,
                event_type: seed.event_type,
                action: seed.action,
                payload_installation_id: seed.payload_installation_id,
                payload: seed.payload,
                payload_size_bytes: seed.payload_size_bytes,
                received_at: seed
                    .received_at
                    .unwrap_or(now),
                status: WebhookStatus::Received,
                outcome: None,
                claimed_at: None,
                claim_token: None,
                next_attempt_at: now,
                attempts: 0,
                last_error: None,
                processed_at: None,
            });
        id
    }

    pub fn rows(&self) -> Vec<InMemoryWebhookRow> {
        self.rows
            .lock()
            .unwrap()
            .clone()
    }

    pub fn row(&self, id: i64) -> Option<InMemoryWebhookRow> {
        self.rows
            .lock()
            .unwrap()
            .iter()
            .find(|r| r.id == id)
            .cloned()
    }

    /// Test helper: make a row immediately claimable by moving its
    /// `next_attempt_at` back. Useful for retry / backoff tests
    /// without sleeping wall-clock time.
    pub fn set_next_attempt_at(&self, id: i64, when: DateTime<Utc>) {
        if let Some(r) = self
            .rows
            .lock()
            .unwrap()
            .iter_mut()
            .find(|r| r.id == id)
        {
            r.next_attempt_at = when;
        }
    }

    /// Test helper: backdate a row's `claimed_at` to simulate a
    /// crashed/abandoned processor without going through real elapsed
    /// time.
    pub fn set_claimed_at(&self, id: i64, when: DateTime<Utc>) {
        if let Some(r) = self
            .rows
            .lock()
            .unwrap()
            .iter_mut()
            .find(|r| r.id == id)
        {
            r.claimed_at = Some(when);
        }
    }
}

#[async_trait]
impl WebhookInbox for InMemoryWebhookInbox {
    async fn claim_next(&self, event_types: &[&str]) -> Result<Option<ClaimedWebhook>> {
        if event_types.is_empty() {
            return Ok(None);
        }
        let now = Utc::now();
        let token = Uuid::new_v4();
        let mut rows = self.rows.lock().unwrap();
        // Pick the oldest claimable row (received OR retryable_error,
        // next_attempt_at in the past, event_type in the filter).
        let idx = rows
            .iter()
            .enumerate()
            .filter(|(_, r)| {
                matches!(r.status, WebhookStatus::Received | WebhookStatus::RetryableError)
                    && r.next_attempt_at <= now
                    && event_types
                        .iter()
                        .any(|et| *et == r.event_type)
            })
            .min_by(|a, b| (a.1.next_attempt_at, a.1.id).cmp(&(b.1.next_attempt_at, b.1.id)))
            .map(|(i, _)| i);
        let Some(idx) = idx else {
            return Ok(None);
        };
        let r = &mut rows[idx];
        r.status = WebhookStatus::Processing;
        r.claimed_at = Some(now);
        r.claim_token = Some(token);
        Ok(Some(ClaimedWebhook {
            id: r.id,
            claim_token: token,
            delivery_id: r.delivery_id.clone(),
            event_type: r.event_type.clone(),
            action: r.action.clone(),
            payload_installation_id: r.payload_installation_id,
            payload: r.payload.clone(),
            payload_size_bytes: r.payload_size_bytes,
            attempts: r.attempts,
            received_at: r.received_at,
        }))
    }

    async fn complete(&self, id: i64, claim_token: Uuid, outcome: WebhookOutcome) -> Result<()> {
        let mut rows = self.rows.lock().unwrap();
        if let Some(r) = rows
            .iter_mut()
            .find(|r| r.id == id)
        {
            if r.status == WebhookStatus::Processing && r.claim_token == Some(claim_token) {
                r.status = outcome.terminal_status();
                r.outcome = Some(outcome);
                r.processed_at = Some(Utc::now());
                r.claim_token = None;
                r.claimed_at = None;
                // Clear any historical transient-error string from
                // prior retry attempts; the row is now terminal-success.
                r.last_error = None;
            }
        }
        Ok(())
    }

    async fn record_retryable_error(
        &self,
        id: i64,
        claim_token: Uuid,
        error: &str,
        next_attempt_at: DateTime<Utc>,
    ) -> Result<()> {
        let mut rows = self.rows.lock().unwrap();
        if let Some(r) = rows
            .iter_mut()
            .find(|r| r.id == id)
        {
            if r.status == WebhookStatus::Processing && r.claim_token == Some(claim_token) {
                r.status = WebhookStatus::RetryableError;
                r.attempts += 1;
                r.last_error = Some(error.to_string());
                r.next_attempt_at = next_attempt_at;
                r.claim_token = None;
                r.claimed_at = None;
            }
        }
        Ok(())
    }

    async fn record_permanent_failure(
        &self,
        id: i64,
        claim_token: Uuid,
        error: &str,
    ) -> Result<()> {
        let mut rows = self.rows.lock().unwrap();
        if let Some(r) = rows
            .iter_mut()
            .find(|r| r.id == id)
        {
            if r.status == WebhookStatus::Processing && r.claim_token == Some(claim_token) {
                r.status = WebhookStatus::Failed;
                r.outcome = Some(WebhookOutcome::Error);
                // Mirror the Postgres impl: increment attempts so the
                // row reflects the actual final attempt count.
                r.attempts += 1;
                r.last_error = Some(error.to_string());
                r.processed_at = Some(Utc::now());
                r.claim_token = None;
                r.claimed_at = None;
            }
        }
        Ok(())
    }

    async fn clear_terminal_payloads(&self, retention: chrono::Duration) -> Result<u64> {
        let cutoff = Utc::now() - retention;
        let mut rows = self.rows.lock().unwrap();
        let mut cleared = 0u64;
        for r in rows.iter_mut() {
            if matches!(
                r.status,
                WebhookStatus::Ignored | WebhookStatus::Denied | WebhookStatus::Failed
            ) && r.payload.is_some()
                && r.processed_at
                    .map(|t| t < cutoff)
                    .unwrap_or(false)
            {
                r.payload = None;
                cleared += 1;
            }
        }
        Ok(cleared)
    }

    async fn sweep_stuck_claims(&self, lease: chrono::Duration) -> Result<u64> {
        let cutoff = Utc::now() - lease;
        let mut rows = self.rows.lock().unwrap();
        let mut recovered = 0u64;
        for r in rows.iter_mut() {
            if r.status == WebhookStatus::Processing
                && r.claimed_at
                    .map(|t| t < cutoff)
                    .unwrap_or(false)
            {
                r.status = WebhookStatus::RetryableError;
                r.claim_token = None;
                r.claimed_at = None;
                r.last_error = Some(
                    r.last_error
                        .clone()
                        .unwrap_or_default()
                        + " [reclaimed by stuck-claim sweep]",
                );
                recovered += 1;
            }
        }
        Ok(recovered)
    }
}
