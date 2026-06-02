//! In-memory `IngestStore` for unit tests. Records webhook rows with
//! dedupe-on-`delivery_id`; good enough to exercise handler control flow.

use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, Ordering};

use async_trait::async_trait;

use crate::Result;
use crate::db::ingest::{IngestOutcome, IngestStore, NewWebhook};

/// What we keep about each ingested webhook for test assertions.
/// Mirrors the github_webhook table's handler-written columns.
#[derive(Debug, Clone)]
pub struct WebhookRow {
    pub id: i64,
    pub delivery_id: String,
    pub event_type: String,
    pub action: Option<String>,
    pub payload_installation_id: Option<i64>,
    pub payload_size_bytes: i32,
}

pub struct InMemoryIngestStore {
    webhooks: Mutex<Vec<WebhookRow>>,
    next_id: AtomicI64,
}

impl Default for InMemoryIngestStore {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryIngestStore {
    pub fn new() -> Self {
        Self {
            webhooks: Mutex::new(Vec::new()),
            next_id: AtomicI64::new(1),
        }
    }

    pub fn webhooks(&self) -> Vec<WebhookRow> {
        self.webhooks
            .lock()
            .unwrap()
            .clone()
    }

    pub fn webhook_count(&self) -> usize {
        self.webhooks
            .lock()
            .unwrap()
            .len()
    }

    fn alloc_id(&self) -> i64 {
        self.next_id
            .fetch_add(1, Ordering::SeqCst)
    }

    fn insert_webhook(&self, webhook: &NewWebhook) -> Option<i64> {
        let mut whs = self.webhooks.lock().unwrap();
        if whs
            .iter()
            .any(|w| w.delivery_id == webhook.delivery_id)
        {
            return None;
        }
        let id = self.alloc_id();
        whs.push(WebhookRow {
            id,
            delivery_id: webhook.delivery_id.clone(),
            event_type: webhook.event_type.clone(),
            action: webhook.action.clone(),
            payload_installation_id: webhook.payload_installation_id,
            payload_size_bytes: webhook.payload_size_bytes,
        });
        Some(id)
    }
}

#[async_trait]
impl IngestStore for InMemoryIngestStore {
    async fn ingest_webhook(&self, webhook: &NewWebhook) -> Result<IngestOutcome> {
        Ok(match self.insert_webhook(webhook) {
            Some(id) => IngestOutcome::Recorded { webhook_id: id },
            None => IngestOutcome::Duplicate,
        })
    }
}
