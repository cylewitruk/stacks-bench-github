//! `IngestStore` — the boundary the handler uses to record an inbound
//! webhook into the inbox.
//!
//! Since the slice 11 cutover the handler is inbox-only: it records a
//! `github_webhook` row and nothing else. The processor (in the
//! daemon) owns all classification, authorization, and job creation
//! from the inbox.

use async_trait::async_trait;
use serde_json::Value;

use crate::Result;

/// GitHub event types the system records into the inbox. Anything else is
/// dropped without a DB row (DoS-aware). Single source of truth shared by
/// the handler's edge filter and the daemon's `POST /api/webhooks`
/// (the canonical filter — the handler forwards everything once migrated).
pub const SUPPORTED_WEBHOOK_EVENT_TYPES: &[&str] = &[
    "issue_comment",
    "push",
    "pull_request",
    "create",
    "installation",
    "installation_repositories",
];

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
    /// A webhook row was written.
    Recorded { webhook_id: i64 },
    /// The webhook's `delivery_id` was already present. No rows were
    /// written; nothing further to do.
    Duplicate,
}

#[async_trait]
pub trait IngestStore: Send + Sync + 'static {
    /// Insert a webhook row into the inbox. Idempotent on `delivery_id`.
    async fn ingest_webhook(&self, webhook: &NewWebhook) -> Result<IngestOutcome>;
}
