//! Postgres-backed `WebhookInbox`. Claim uses `FOR UPDATE SKIP LOCKED`
//! so multiple processors can run concurrently without contending.
//! All mutating operations are conditional on `claim_token` matching so
//! a processor whose lease has expired (sweeper reset the row) can't
//! corrupt subsequent state.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use uuid::Uuid;

use crate::Result;
use crate::db::Pool;
use crate::db::webhook::{ClaimedWebhook, WebhookInbox};
use crate::models::WebhookOutcome;

#[derive(Clone)]
pub struct PostgresWebhookInbox {
    pool: Pool,
}

impl PostgresWebhookInbox {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl WebhookInbox for PostgresWebhookInbox {
    async fn claim_next(&self, event_types: &[&str]) -> Result<Option<ClaimedWebhook>> {
        // Empty filter = nothing claimable. Saves a DB round-trip when
        // the processor's classifier doesn't yet support anything.
        if event_types.is_empty() {
            return Ok(None);
        }
        // Subquery picks the oldest claimable row with FOR UPDATE SKIP
        // LOCKED so concurrent processors get disjoint rows. event_type
        // filter restricts to rows the caller can classify; others
        // stay `received` for a future processor with a wider filter.
        // Single round-trip; sqlx returns the row via RETURNING.
        let claim_token = Uuid::new_v4();
        // sqlx requires owned Strings for TEXT[] binding; the slice is
        // small (≤handful of event types) so the allocation is trivial.
        let event_types_vec: Vec<String> = event_types
            .iter()
            .map(|s| s.to_string())
            .collect();
        let row: Option<(
            i64,
            String,
            String,
            Option<String>,
            Option<i64>,
            Option<Value>,
            i32,
            i32,
            DateTime<Utc>,
        )> = sqlx::query_as(
            r#"
            UPDATE github_webhook
            SET status = 'processing',
                claimed_at = NOW(),
                claim_token = $1
            WHERE id = (
                SELECT id FROM github_webhook
                WHERE status IN ('received', 'retryable_error')
                  AND next_attempt_at <= NOW()
                  AND event_type = ANY($2)
                ORDER BY next_attempt_at, id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING id, delivery_id, event_type, action,
                      payload_installation_id, payload, payload_size_bytes,
                      attempts, received_at
            "#,
        )
        .bind(claim_token)
        .bind(&event_types_vec)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| ClaimedWebhook {
            id: r.0,
            claim_token,
            delivery_id: r.1,
            event_type: r.2,
            action: r.3,
            payload_installation_id: r.4,
            payload: r.5,
            payload_size_bytes: r.6,
            attempts: r.7,
            received_at: r.8,
        }))
    }

    async fn complete(&self, id: i64, claim_token: Uuid, outcome: WebhookOutcome) -> Result<()> {
        // Bind the matching terminal status alongside the outcome so
        // both columns transition in one shot. `last_error` is cleared
        // on success — if the row had transient errors during earlier
        // attempts, those are now historical noise that would mislead
        // ops queries scanning for active error states.
        let status = outcome.terminal_status();
        sqlx::query(
            r#"
            UPDATE github_webhook
            SET status = $1,
                outcome = $2,
                processed_at = NOW(),
                claim_token = NULL,
                claimed_at = NULL,
                last_error = NULL
            WHERE id = $3
              AND claim_token = $4
              AND status = 'processing'
            "#,
        )
        .bind(status)
        .bind(outcome)
        .bind(id)
        .bind(claim_token)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn record_retryable_error(
        &self,
        id: i64,
        claim_token: Uuid,
        error: &str,
        next_attempt_at: DateTime<Utc>,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE github_webhook
            SET status = 'retryable_error',
                attempts = attempts + 1,
                last_error = $1,
                next_attempt_at = $2,
                claim_token = NULL,
                claimed_at = NULL
            WHERE id = $3
              AND claim_token = $4
              AND status = 'processing'
            "#,
        )
        .bind(error)
        .bind(next_attempt_at)
        .bind(id)
        .bind(claim_token)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn record_permanent_failure(
        &self,
        id: i64,
        claim_token: Uuid,
        error: &str,
    ) -> Result<()> {
        // Increments `attempts` like the retryable path so the row
        // accurately reflects how many processing attempts happened
        // before retries were exhausted. Without this, a row with
        // max_attempts=5 would show attempts=4 after the fifth (final)
        // failed attempt.
        sqlx::query(
            r#"
            UPDATE github_webhook
            SET status = 'failed',
                outcome = 'error',
                attempts = attempts + 1,
                last_error = $1,
                processed_at = NOW(),
                claim_token = NULL,
                claimed_at = NULL
            WHERE id = $2
              AND claim_token = $3
              AND status = 'processing'
            "#,
        )
        .bind(error)
        .bind(id)
        .bind(claim_token)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn sweep_stuck_claims(&self, lease: chrono::Duration) -> Result<u64> {
        // Anything claimed for longer than the lease window is presumed
        // dead; revert it to retryable_error so it can be re-claimed.
        // attempts is NOT incremented here — the previous processor may
        // never have actually done any work.
        let lease_seconds = lease.num_seconds();
        let result = sqlx::query(
            r#"
            UPDATE github_webhook
            SET status = 'retryable_error',
                claim_token = NULL,
                claimed_at = NULL,
                last_error = COALESCE(last_error, '') || ' [reclaimed by stuck-claim sweep]'
            WHERE status = 'processing'
              AND claimed_at < NOW() - make_interval(secs => $1)
            "#,
        )
        .bind(lease_seconds as f64)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }
}
