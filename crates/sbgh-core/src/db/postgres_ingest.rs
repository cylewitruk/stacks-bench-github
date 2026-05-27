//! Postgres-backed `IngestStore`. Owns the slice 1 dual-write
//! transaction: a single ON CONFLICT INSERT into `github_webhook`
//! plus, when called via `ingest_webhook_and_job`, a second INSERT
//! into the legacy `jobs` table within the same transaction.

use async_trait::async_trait;
use uuid::Uuid;

use crate::Result;
use crate::db::Pool;
use crate::db::ingest::{IngestOutcome, IngestStore, NewWebhook};
use crate::models::NewJob;

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
            Some(id) => IngestOutcome::Recorded { webhook_id: id, job_id: None },
            None => IngestOutcome::Duplicate,
        })
    }

    async fn ingest_webhook_and_job(
        &self,
        webhook: &NewWebhook,
        new_job: &NewJob,
    ) -> Result<IngestOutcome> {
        let mut tx = self.pool.begin().await?;

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
        .fetch_optional(&mut *tx)
        .await?;

        let Some(webhook_id) = webhook_id else {
            tx.commit().await?;
            return Ok(IngestOutcome::Duplicate);
        };

        // Webhook is fresh. Now try the legacy jobs insert. If the
        // delivery_id was already in `jobs` from before slice 1, the
        // partial unique index on jobs(github_delivery_id) rejects
        // the row — that's fine, we keep the webhook (audit value)
        // and report job_id=None.
        let job_id: Option<Uuid> = sqlx::query_scalar(
            r#"
            INSERT INTO jobs (
                repository, pr_number, head_sha, requested_by,
                command, args, installation_id, github_delivery_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (github_delivery_id) WHERE github_delivery_id IS NOT NULL
              DO NOTHING
            RETURNING id
            "#,
        )
        .bind(&new_job.repository)
        .bind(new_job.pr_number)
        .bind(&new_job.head_sha)
        .bind(&new_job.requested_by)
        .bind(&new_job.command)
        .bind(&new_job.args)
        .bind(new_job.installation_id)
        .bind(&new_job.github_delivery_id)
        .fetch_optional(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(IngestOutcome::Recorded { webhook_id, job_id })
    }
}
