//! In-memory `IngestStore` for unit tests. Wraps an `InMemoryJobStore`
//! for the legacy enqueue half of the dual-write. No real transaction
//! semantics — webhook insertion happens first; if it succeeds, the
//! job enqueue runs unconditionally. Good enough to exercise handler
//! control flow including dedupe.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::Result;
use crate::db::in_memory_jobs::InMemoryJobStore;
use crate::db::ingest::{IngestOutcome, IngestStore, NewWebhook};
use crate::db::jobs::JobStore;
use crate::models::NewJob;

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
    jobs: Arc<InMemoryJobStore>,
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
            jobs: Arc::new(InMemoryJobStore::new()),
            webhooks: Mutex::new(Vec::new()),
            next_id: AtomicI64::new(1),
        }
    }

    pub fn jobs(&self) -> &Arc<InMemoryJobStore> {
        &self.jobs
    }

    pub fn webhooks(&self) -> Vec<WebhookRow> {
        self.webhooks
            .lock()
            .unwrap()
            .clone()
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
            Some(id) => IngestOutcome::Recorded { webhook_id: id, job_id: None },
            None => IngestOutcome::Duplicate,
        })
    }

    async fn ingest_webhook_and_job(
        &self,
        webhook: &NewWebhook,
        new_job: &NewJob,
    ) -> Result<IngestOutcome> {
        let Some(webhook_id) = self.insert_webhook(webhook) else {
            return Ok(IngestOutcome::Duplicate);
        };
        let job_id = self
            .jobs
            .enqueue(new_job)
            .await?;
        Ok(IngestOutcome::Recorded { webhook_id, job_id })
    }
}

impl InMemoryIngestStore {
    pub fn webhook_count(&self) -> usize {
        self.webhooks
            .lock()
            .unwrap()
            .len()
    }
}
