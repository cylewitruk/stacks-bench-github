//! Postgres-backed `IngestStore`. A single `ON CONFLICT` INSERT into the
//! `github_webhook` inbox; idempotent on `delivery_id`.

use async_trait::async_trait;

use crate::Result;
use crate::db::Pool;
use crate::db::ingest::{IngestOutcome, IngestStore, NewWebhook};

#[derive(Clone)]
pub struct PostgresIngestStore {
    pool: Pool,
}

impl PostgresIngestStore {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl IngestStore for PostgresIngestStore {
    async fn ingest_webhook(&self, webhook: &NewWebhook) -> Result<IngestOutcome> {
        let webhook_id: Option<i64> = sqlx::query_scalar(
            r#"
            INSERT INTO github_webhook (
                delivery_id, event_type, action,
                payload_installation_id, payload, payload_size_bytes
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (delivery_id) DO NOTHING
            RETURNING id
            "#,
        )
        .bind(&webhook.delivery_id)
        .bind(&webhook.event_type)
        .bind(&webhook.action)
        .bind(webhook.payload_installation_id)
        .bind(&webhook.payload)
        .bind(webhook.payload_size_bytes)
        .fetch_optional(&self.pool)
        .await?;
        Ok(match webhook_id {
            Some(id) => IngestOutcome::Recorded { webhook_id: id },
            None => IngestOutcome::Duplicate,
        })
    }
}
