//! Cancellation transitions shared by single-job and audited fleet operations.

use sbgh_core::models::{JobEventKind, JobEventStatus};
use sqlx::{Postgres, Row, Transaction};
use uuid::Uuid;

use super::{discard_attempt_projection, insert_job_event};
use crate::IntoCoreResult;

/// Request cancellation while retaining the caller's transaction and cleanup gate.
pub async fn request(tx: &mut Transaction<'_, Postgres>, job_id: Uuid) -> sbgh_core::Result<bool> {
    // Lock in the same attempt→job order as accept/terminal transitions.
    let attempt = sqlx::query(
        "SELECT attempt_id, worker_session_id, status::text AS status
               FROM worker_attempt
              WHERE job_id = $1
                AND status IN ('offered', 'running', 'cancel_requested')
              FOR UPDATE",
    )
    .bind(job_id)
    .fetch_optional(&mut **tx)
    .await
    .core()?;
    // Match cleanup acknowledgement's obligation -> job lock order.
    let cleanup_ids: Vec<i64> = sqlx::query_scalar(
        "SELECT id FROM worker_cleanup_obligation
         WHERE job_id=$1 AND status='pending' ORDER BY id FOR UPDATE",
    )
    .bind(job_id)
    .fetch_all(&mut **tx)
    .await
    .core()?;
    let job_status: Option<String> =
        sqlx::query_scalar("SELECT status::text FROM job WHERE id = $1 FOR UPDATE")
            .bind(job_id)
            .fetch_optional(&mut **tx)
            .await
            .core()?;
    let Some(job_status) = job_status else {
        return Ok(false);
    };
    if job_status == "cancelled" {
        return Ok(true);
    }
    if let Some(attempt) = attempt {
        let attempt_id: Uuid = attempt
            .try_get("attempt_id")
            .core()?;
        let session_id: Uuid = attempt
            .try_get("worker_session_id")
            .core()?;
        let status: String = attempt
            .try_get("status")
            .core()?;
        if matches!(status.as_str(), "running" | "cancel_requested") {
            sqlx::query(
                "UPDATE worker_attempt
                        SET status = 'cancel_requested', updated_at = NOW()
                      WHERE attempt_id = $1",
            )
            .bind(attempt_id)
            .execute(&mut **tx)
            .await
            .core()?;
            return Ok(true);
        }

        // An offer has not acquired local resources. Fence it and
        // terminal-cancel the job; a concurrent accept serializes on the
        // same attempt row, so exactly one transition wins.
        discard_attempt_projection(tx, attempt_id, "offered work was cancelled").await?;
        sqlx::query(
            "UPDATE worker_attempt
                    SET status = 'fenced', updated_at = NOW()
                  WHERE attempt_id = $1 AND status = 'offered'",
        )
        .bind(attempt_id)
        .execute(&mut **tx)
        .await
        .core()?;
        sqlx::query(
            "UPDATE worker_session
                    SET status = 'idle', last_heartbeat_at = NOW()
                  WHERE worker_session_id = $1 AND status = 'offered'",
        )
        .bind(session_id)
        .execute(&mut **tx)
        .await
        .core()?;
    }
    if !cleanup_ids.is_empty() && job_status == "running" {
        sqlx::query("UPDATE worker_cleanup_obligation SET requeue_job=false WHERE id=ANY($1)")
            .bind(&cleanup_ids)
            .execute(&mut **tx)
            .await
            .core()?;
        return Ok(true);
    }
    let cancelled = sqlx::query(
        "UPDATE job
                SET status = 'cancelled', claim_token = NULL,
                    claimed_at = NULL, updated_at = NOW()
              WHERE id = $1 AND status IN ('queued', 'claimed')",
    )
    .bind(job_id)
    .execute(&mut **tx)
    .await
    .core()?;
    if cancelled.rows_affected() == 1 {
        insert_job_event(
            tx,
            job_id,
            JobEventKind::Cancelled,
            JobEventStatus::Success,
            Some("cancelled before worker execution started"),
            None,
        )
        .await?;
    }
    Ok(cancelled.rows_affected() == 1)
}
