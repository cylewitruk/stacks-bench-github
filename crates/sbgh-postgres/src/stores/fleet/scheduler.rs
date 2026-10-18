//! Admission locking and append-only scheduler administration.

use async_trait::async_trait;
use sbgh_core::db::scheduler::{
    SchedulerChange, SchedulerChangeOutcome, SchedulerPolicy, SchedulerSnapshot, SchedulerStore,
};
use sqlx::{Postgres, Row, Transaction, postgres::PgRow};

use super::{PostgresFleetStore, quiescence};
use crate::IntoCoreResult;

/// Hold a shared policy lock before acquiring any job or worker locks.
pub async fn lock_policy(tx: &mut Transaction<'_, Postgres>) -> sbgh_core::Result<SchedulerPolicy> {
    let row = sqlx::query("SELECT * FROM scheduler_policy WHERE singleton FOR SHARE")
        .fetch_one(&mut **tx)
        .await
        .core()?;
    policy(&row)
}

/// Decode one committed policy row.
fn policy(row: &PgRow) -> sbgh_core::Result<SchedulerPolicy> {
    Ok(SchedulerPolicy {
        paused: row.try_get("paused").core()?,
        generation: row
            .try_get("generation")
            .core()?,
        actor: row.try_get("actor").core()?,
        reason: row.try_get("reason").core()?,
        changed_at: row
            .try_get("changed_at")
            .core()?,
    })
}

#[async_trait]
impl SchedulerStore for PostgresFleetStore {
    async fn scheduler_snapshot(&self) -> sbgh_core::Result<SchedulerSnapshot> {
        let query = format!(
            "SELECT policy.*, counters.* FROM scheduler_policy policy
            CROSS JOIN ({}) counters WHERE singleton",
            quiescence::COUNTERS_SQL
        );
        let row = sqlx::query(sqlx::AssertSqlSafe(query))
            .fetch_one(&self.pool)
            .await
            .core()?;
        Ok(SchedulerSnapshot {
            policy: policy(&row)?,
            active_attempts: row
                .try_get("active_attempts")
                .core()?,
            pending_cleanup: row
                .try_get("pending_cleanup")
                .core()?,
            pending_projection: row
                .try_get("pending_projection")
                .core()?,
            pending_artifacts: row
                .try_get("pending_artifacts")
                .core()?,
            garbage_artifacts: row
                .try_get("garbage_artifacts")
                .core()?,
        })
    }

    async fn scheduler_policy(&self) -> sbgh_core::Result<SchedulerPolicy> {
        let row = sqlx::query("SELECT * FROM scheduler_policy WHERE singleton")
            .fetch_one(&self.pool)
            .await
            .core()?;
        policy(&row)
    }

    async fn change_scheduler(
        &self,
        change: &SchedulerChange,
    ) -> sbgh_core::Result<SchedulerChangeOutcome> {
        if change.actor.trim().is_empty()
            || change.actor.chars().count() > 128
            || change
                .reason
                .trim()
                .is_empty()
            || change.reason.chars().count() > 1024
        {
            return Err(sbgh_core::Error::Config(
                "scheduler actor and reason must be nonempty and bounded".into(),
            ));
        }
        let mut tx = self
            .pool
            .begin()
            .await
            .core()?;
        let row = sqlx::query("SELECT * FROM scheduler_policy WHERE singleton FOR UPDATE")
            .fetch_one(&mut *tx)
            .await
            .core()?;
        let current = policy(&row)?;
        if let Some(row) =
            sqlx::query("SELECT * FROM scheduler_administration_audit WHERE request_id = $1")
                .bind(change.request_id)
                .fetch_optional(&mut *tx)
                .await
                .core()?
        {
            if row
                .try_get::<bool, _>("paused")
                .core()?
                != change.paused
                || row
                    .try_get::<String, _>("actor")
                    .core()?
                    != change.actor
                || row
                    .try_get::<String, _>("reason")
                    .core()?
                    != change.reason
            {
                return Ok(SchedulerChangeOutcome::Conflict);
            }
            return Ok(SchedulerChangeOutcome::Applied(SchedulerPolicy {
                paused: change.paused,
                generation: row
                    .try_get("generation")
                    .core()?,
                actor: row
                    .try_get("policy_actor")
                    .core()?,
                reason: row
                    .try_get("policy_reason")
                    .core()?,
                changed_at: row
                    .try_get("policy_changed_at")
                    .core()?,
            }));
        }
        let changed = current.paused != change.paused;
        let result = if !changed {
            current
        } else {
            let row = sqlx::query("UPDATE scheduler_policy SET paused = $1, generation = generation + 1,
                actor = $2, reason = $3, changed_at = clock_timestamp() WHERE singleton RETURNING *")
                .bind(change.paused).bind(&change.actor).bind(&change.reason)
                .fetch_one(&mut *tx).await.core()?;
            policy(&row)?
        };
        if changed && change.paused {
            // Serialize with lifecycle/recovery without blocking expiry's foreign-key
            // checks. Lock attempts before sessions, matching the expiry sweep.
            sqlx::query(
                "SELECT worker_id FROM worker_registry registry
                WHERE EXISTS (SELECT 1 FROM worker_attempt attempt
                    WHERE attempt.worker_id=registry.worker_id AND attempt.status='offered')
                ORDER BY worker_id FOR NO KEY UPDATE",
            )
            .fetch_all(&mut *tx)
            .await
            .core()?;
            sqlx::query(
                "SELECT attempt_id FROM worker_attempt WHERE status='offered'
                ORDER BY attempt_id FOR UPDATE",
            )
            .fetch_all(&mut *tx)
            .await
            .core()?;
            sqlx::query("UPDATE worker_session session SET status=CASE
                WHEN registry.draining OR registry.shutdown_session_id=session.worker_session_id
                    THEN 'draining'::worker_session_status ELSE 'idle'::worker_session_status END
                FROM worker_registry registry
                WHERE session.worker_id=registry.worker_id AND session.status='offered'
                  AND EXISTS (SELECT 1 FROM worker_attempt attempt
                    WHERE attempt.worker_session_id=session.worker_session_id AND attempt.status='offered')")
                .execute(&mut *tx).await.core()?;
            // No execution exists before acceptance. Withdraw these offers
            // without expiring their resident worker sessions.
            sqlx::query(
                "WITH withdrawn AS (
                UPDATE worker_attempt SET status = 'expired', updated_at = clock_timestamp()
                WHERE status = 'offered' RETURNING job_id, claim_token
            ) UPDATE job SET status = 'queued', claim_token = NULL, claimed_at = NULL,
                updated_at = clock_timestamp()
              FROM withdrawn WHERE job.id = withdrawn.job_id
                AND job.claim_token = withdrawn.claim_token AND job.status = 'claimed'",
            )
            .execute(&mut *tx)
            .await
            .core()?;
        }
        sqlx::query("INSERT INTO scheduler_administration_audit
            (request_id, paused, actor, reason, generation, policy_actor, policy_reason, policy_changed_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)")
            .bind(change.request_id).bind(change.paused).bind(&change.actor).bind(&change.reason)
            .bind(result.generation).bind(&result.actor).bind(&result.reason).bind(result.changed_at)
            .execute(&mut *tx).await.core()?;
        tx.commit().await.core()?;
        Ok(SchedulerChangeOutcome::Applied(result))
    }
}
