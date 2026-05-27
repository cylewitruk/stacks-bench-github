//! `IngestStore` — the boundary the handler uses to record an inbound
//! webhook (and, for `/benchmark` from authorized users, atomically also
//! enqueue a legacy `jobs` row).
//!
//! Distinct from `JobStore`: `IngestStore` owns the dual-write
//! transaction that slice 1 introduces. The orchestrator still uses
//! `JobStore` for queue claim/transition operations.

use async_trait::async_trait;
use serde_json::Value;
use uuid::Uuid;

use crate::Result;
use crate::models::NewJob;

/// Fields the handler can write at HTTP-receipt time: things derivable
/// from the request headers + signature-verified body, with NO GitHub
/// API access. installation id is the raw payload value (no FK target
/// yet — slice 3 adds `github_installation_id` + FK).
///
/// `payload` is `Option<Value>` so unparseable bodies bind SQL NULL (not
/// JSON `null`); ops queries can use `payload IS NULL` to detect
/// missing/cleared payloads without mis-matching legit `null` bodies.
#[derive(Debug, Clone)]
pub struct NewWebhook {
    pub delivery_id: String,
    pub event_type: String,
    pub action: Option<String>,
    pub payload_installation_id: Option<i64>,
    pub payload: Option<Value>,
    pub payload_size_bytes: i32,
}

/// Result of an ingest call.
#[derive(Debug, Clone)]
pub enum IngestOutcome {
    /// A webhook row was written. `job_id` is `Some` when a fresh
    /// legacy job row was also enqueued; `None` when the call was
    /// webhook-only OR when the legacy job's unique constraint on
    /// `github_delivery_id` rejected the insert (only possible for
    /// deliveries that landed in `jobs` before slice 1 rolled out).
    Recorded { webhook_id: i64, job_id: Option<Uuid> },
    /// The webhook's `delivery_id` was already present. No rows were
    /// written; nothing further to do.
    Duplicate,
}

#[async_trait]
pub trait IngestStore: Send + Sync + 'static {
    /// Insert a webhook row only. Used for supported events that don't
    /// trigger a legacy job (push, pull_request, installation,
    /// non-`/benchmark` issue_comments, unauthorized commands).
    /// Idempotent on `delivery_id`.
    async fn ingest_webhook(&self, webhook: &NewWebhook) -> Result<IngestOutcome>;

    /// Insert a webhook row AND a legacy job row in a single
    /// transaction. Used for `/benchmark` from authorized users. If
    /// either INSERT fails, both roll back. Idempotent on the
    /// webhook's `delivery_id`.
    async fn ingest_webhook_and_job(
        &self,
        webhook: &NewWebhook,
        new_job: &NewJob,
    ) -> Result<IngestOutcome>;
}
