//! Serialized compatibility-floor changes and immutable administrative receipts.

use async_trait::async_trait;
use sbgh_core::Error;
use sbgh_core::db::protocol::{
    ProtocolPolicy, ProtocolPolicyChange, ProtocolPolicyChangeOutcome, ProtocolPolicyStore,
};
use sbgh_fleet::{NEGOTIATED_PROTOCOL_REVISION, PREVIOUS_PROTOCOL_REVISION};
use sqlx::{Postgres, Row, Transaction, postgres::PgRow};
use uuid::Uuid;

use super::{PostgresFleetStore, quiescence};
use crate::IntoCoreResult;

/// Lock after scheduler policy, if needed, and before worker/job rows.
pub async fn lock_policy(tx: &mut Transaction<'_, Postgres>) -> sbgh_core::Result<ProtocolPolicy> {
    let row = sqlx::query("SELECT * FROM fleet_protocol_policy WHERE singleton FOR SHARE")
        .fetch_one(&mut **tx)
        .await
        .core()?;
    policy(&row)
}

/// Decode a supported policy; unknown floors fail closed.
fn policy(row: &PgRow) -> sbgh_core::Result<ProtocolPolicy> {
    let revision: i32 = row
        .try_get("minimum_revision")
        .core()?;
    let minimum_revision =
        u16::try_from(revision).map_err(|_| Error::Config("invalid protocol floor".into()))?;
    validate_revision(minimum_revision)?;
    Ok(ProtocolPolicy {
        minimum_revision,
        generation: row
            .try_get("generation")
            .core()?,
        actor: row.try_get("actor").core()?,
        reason: row.try_get("reason").core()?,
        rollback_evidence: row
            .try_get("rollback_evidence")
            .core()?,
        changed_at: row
            .try_get("changed_at")
            .core()?,
    })
}

/// Limit administrative changes to the implemented rolling window.
fn validate_revision(revision: u16) -> sbgh_core::Result<()> {
    if !(PREVIOUS_PROTOCOL_REVISION..=NEGOTIATED_PROTOCOL_REVISION).contains(&revision) {
        return Err(Error::Config("protocol floor is outside the supported rolling window".into()));
    }
    Ok(())
}

#[async_trait]
impl ProtocolPolicyStore for PostgresFleetStore {
    async fn protocol_policy(&self) -> sbgh_core::Result<ProtocolPolicy> {
        policy(
            &sqlx::query("SELECT * FROM fleet_protocol_policy WHERE singleton")
                .fetch_one(&self.pool)
                .await
                .core()?,
        )
    }

    async fn change_protocol_policy(
        &self,
        change: &ProtocolPolicyChange,
    ) -> sbgh_core::Result<ProtocolPolicyChangeOutcome> {
        validate_revision(change.minimum_revision)?;
        for (value, limit) in
            [(&change.actor, 128), (&change.reason, 1024), (&change.rollback_evidence, 2048)]
        {
            if value.trim().is_empty() || value.chars().count() > limit {
                return Err(Error::Config("protocol policy actor, reason and rollback evidence must be nonempty and bounded".into()));
            }
        }
        let mut tx = self
            .pool
            .begin()
            .await
            .core()?;
        let scheduler = super::scheduler::lock_policy(&mut tx).await?;
        let row = sqlx::query("SELECT * FROM fleet_protocol_policy WHERE singleton FOR UPDATE")
            .fetch_one(&mut *tx)
            .await
            .core()?;
        let current = policy(&row)?;
        if let Some(row) =
            sqlx::query("SELECT * FROM fleet_protocol_administration_audit WHERE request_id = $1")
                .bind(change.request_id)
                .fetch_optional(&mut *tx)
                .await
                .core()?
        {
            if row
                .try_get::<i32, _>("minimum_revision")
                .core()?
                != i32::from(change.minimum_revision)
                || row
                    .try_get::<String, _>("actor")
                    .core()?
                    != change.actor
                || row
                    .try_get::<String, _>("reason")
                    .core()?
                    != change.reason
                || row
                    .try_get::<String, _>("rollback_evidence")
                    .core()?
                    != change.rollback_evidence
            {
                return Ok(ProtocolPolicyChangeOutcome::Conflict);
            }
            let receipt = serde_json::from_value(
                row.try_get("receipt")
                    .core()?,
            )
            .core()?;
            return Ok(ProtocolPolicyChangeOutcome::Applied(receipt));
        }
        let changed = current.minimum_revision != change.minimum_revision;
        if changed {
            let query = format!(
                "SELECT active_attempts != 0 OR pending_cleanup != 0
                OR pending_projection != 0 OR pending_artifacts != 0
                FROM ({}) counters",
                quiescence::COUNTERS_SQL
            );
            let busy: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(query))
                .fetch_one(&mut *tx)
                .await
                .core()?;
            if !scheduler.paused || busy {
                return Ok(ProtocolPolicyChangeOutcome::MaintenanceRequired);
            }
            if change.minimum_revision > current.minimum_revision {
                let missing: Vec<Uuid> = sqlx::query_scalar("SELECT registry.worker_id FROM worker_registry registry
                    WHERE registry.enabled AND NOT EXISTS (
                        SELECT 1 FROM worker_session session WHERE session.worker_id = registry.worker_id
                        AND session.ended_at IS NULL AND session.expires_at > clock_timestamp()
                        AND session.status IN ('registered', 'idle', 'offered', 'running', 'draining')
                        AND (session.protocol_contract->>'revision')::integer >= $1)
                    ORDER BY registry.worker_id")
                    .bind(i32::from(change.minimum_revision)).fetch_all(&mut *tx).await.core()?;
                if !missing.is_empty() {
                    return Ok(ProtocolPolicyChangeOutcome::WorkersNotConverged(missing));
                }
            }
        }
        let receipt = if changed {
            let row = sqlx::query(
                "UPDATE fleet_protocol_policy SET minimum_revision = $1,
                generation = generation + 1, actor = $2, reason = $3, rollback_evidence = $4,
                changed_at = clock_timestamp() WHERE singleton RETURNING *",
            )
            .bind(i32::from(change.minimum_revision))
            .bind(&change.actor)
            .bind(&change.reason)
            .bind(&change.rollback_evidence)
            .fetch_one(&mut *tx)
            .await
            .core()?;
            policy(&row)?
        } else {
            current
        };
        sqlx::query("INSERT INTO fleet_protocol_administration_audit
            (request_id, minimum_revision, actor, reason, rollback_evidence, receipt) VALUES ($1,$2,$3,$4,$5,$6)")
            .bind(change.request_id).bind(i32::from(change.minimum_revision)).bind(&change.actor)
            .bind(&change.reason).bind(&change.rollback_evidence).bind(serde_json::to_value(&receipt).core()?)
            .execute(&mut *tx).await.core()?;
        tx.commit().await.core()?;
        Ok(ProtocolPolicyChangeOutcome::Applied(receipt))
    }
}
