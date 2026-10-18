//! Worker policy transitions serialized with admission and session replacement.

use async_trait::async_trait;
use sbgh_core::Error;
use sbgh_core::db::worker_lifecycle::{
    WorkerLifecycleAction, WorkerLifecycleChange, WorkerLifecycleOutcome, WorkerLifecycleReceipt,
    WorkerLifecycleStore, WorkerMaintenanceSnapshot,
};
use sbgh_fleet::{ProtocolContract, ProtocolFeature, ProtocolOffer};
use sqlx::Row;
use uuid::Uuid;

use super::{PostgresFleetStore, capability, scheduler};
use crate::IntoCoreResult;

#[async_trait]
impl WorkerLifecycleStore for PostgresFleetStore {
    async fn worker_maintenance_snapshot(
        &self,
        worker_id: Uuid,
    ) -> sbgh_core::Result<Option<WorkerMaintenanceSnapshot>> {
        let Some(row) = sqlx::query("SELECT registry.worker_id, registry.lifecycle_generation,
            registry.enabled, registry.draining, registry.shutdown_session_id,
            registry.measurement_profile, scheduler.paused AS scheduler_paused,
            protocol.minimum_revision,
            EXISTS (SELECT 1 FROM worker_identity_key identity
                WHERE identity.worker_id=registry.worker_id
                AND identity.identity_key_sha256=session.identity_key_sha256
                AND identity.revoked_at IS NULL) AS identity_authorized,
            ARRAY(SELECT item::text FROM unnest(session.advertised_capabilities) item
                WHERE item=ANY(registry.allowed_capabilities)) AS effective_capabilities,
            session.worker_session_id, session.status::text AS session_status,
            COALESCE(session.ended_at IS NULL AND session.expires_at > statement_timestamp(), false) AS session_live,
            session.protocol_contract, session.preflight_report,
            (SELECT count(*) FROM worker_attempt WHERE worker_id=registry.worker_id
                AND status IN ('offered','running','cancel_requested')) AS active_attempts,
            (SELECT count(*) FROM worker_cleanup_obligation WHERE worker_id=registry.worker_id
                AND status='pending') AS pending_cleanup
            FROM worker_registry registry
            CROSS JOIN scheduler_policy scheduler
            CROSS JOIN fleet_protocol_policy protocol
            LEFT JOIN LATERAL (SELECT * FROM worker_session WHERE worker_id=registry.worker_id
                ORDER BY started_at DESC LIMIT 1) session ON true
            WHERE registry.worker_id=$1 AND scheduler.singleton AND protocol.singleton")
            .bind(worker_id).fetch_optional(&self.pool).await.core()? else {
                return Ok(None);
            };
        let protocol: Option<ProtocolContract> = row
            .try_get::<Option<serde_json::Value>, _>("protocol_contract")
            .core()?
            .map(serde_json::from_value)
            .transpose()
            .core()?;
        let minimum_revision: i32 = row
            .try_get("minimum_revision")
            .core()?;
        let protocol_compatible = protocol
            .as_ref()
            .is_some_and(|contract| {
                i32::from(contract.revision) >= minimum_revision
                    && ProtocolOffer::current()
                        .validate_selection(contract)
                        .is_ok()
            });
        let mut effective_capabilities: Vec<String> = row
            .try_get("effective_capabilities")
            .core()?;
        effective_capabilities.retain(|name| {
            protocol
                .as_ref()
                .is_some_and(|contract| {
                    capability(name)
                        .is_ok_and(|capability| contract.supports_capability(capability))
                })
        });
        Ok(Some(WorkerMaintenanceSnapshot {
            worker_id,
            scheduler_paused: row
                .try_get("scheduler_paused")
                .core()?,
            identity_authorized: row
                .try_get("identity_authorized")
                .core()?,
            protocol_compatible,
            effective_capabilities,
            measurement_profile: row
                .try_get("measurement_profile")
                .core()?,
            enabled: row
                .try_get("enabled")
                .core()?,
            generation: row
                .try_get("lifecycle_generation")
                .core()?,
            cordoned: row
                .try_get("draining")
                .core()?,
            session_id: row
                .try_get("worker_session_id")
                .core()?,
            session_status: row
                .try_get("session_status")
                .core()?,
            session_live: row
                .try_get("session_live")
                .core()?,
            protocol,
            preflight: row
                .try_get::<Option<serde_json::Value>, _>("preflight_report")
                .core()?
                .map(serde_json::from_value)
                .transpose()
                .core()?,
            shutdown_session_id: row
                .try_get("shutdown_session_id")
                .core()?,
            active_attempts: row
                .try_get("active_attempts")
                .core()?,
            pending_cleanup: row
                .try_get("pending_cleanup")
                .core()?,
        }))
    }

    async fn change_worker_lifecycle(
        &self,
        change: &WorkerLifecycleChange,
    ) -> sbgh_core::Result<WorkerLifecycleOutcome> {
        for (text, limit) in [(&change.actor, 128), (&change.reason, 1024)] {
            if text.trim().is_empty() || text.chars().count() > limit {
                return Err(Error::Config(
                    "worker lifecycle actor/reason must be nonempty and bounded".into(),
                ));
            }
        }
        let mut tx = self
            .pool
            .begin()
            .await
            .core()?;
        // Match scheduler -> worker -> attempt ordering used by admission.
        let _scheduler = scheduler::lock_policy(&mut tx).await?;
        // Serialize UUID reuse even when a conflicting retry names another worker.
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 35))")
            .bind(change.request_id.to_string())
            .execute(&mut *tx)
            .await
            .core()?;
        if let Some(value) = sqlx::query_scalar::<_, serde_json::Value>(
            "SELECT receipt FROM worker_lifecycle_audit WHERE request_id=$1",
        )
        .bind(change.request_id)
        .fetch_optional(&mut *tx)
        .await
        .core()?
        {
            let receipt: WorkerLifecycleReceipt = serde_json::from_value(value).core()?;
            return Ok(
                if receipt.worker_id == change.worker_id
                    && receipt.action == change.action
                    && receipt.actor == change.actor
                    && receipt.reason == change.reason
                {
                    WorkerLifecycleOutcome::Applied(receipt)
                } else {
                    WorkerLifecycleOutcome::Conflict
                },
            );
        }
        let Some(worker) = sqlx::query("SELECT draining, shutdown_session_id, lifecycle_generation FROM worker_registry WHERE worker_id=$1 FOR UPDATE")
            .bind(change.worker_id).fetch_optional(&mut *tx).await.core()? else {
                return Ok(WorkerLifecycleOutcome::NotFound);
            };
        let draining: bool = worker
            .try_get("draining")
            .core()?;
        let shutdown: Option<Uuid> = worker
            .try_get("shutdown_session_id")
            .core()?;
        let target = if change.action == WorkerLifecycleAction::ShutdownAfterIdle {
            let Some(session) = sqlx::query("SELECT worker_session_id, protocol_contract FROM worker_session WHERE worker_id=$1 AND ended_at IS NULL AND expires_at > clock_timestamp() ORDER BY started_at DESC LIMIT 1")
                .bind(change.worker_id).fetch_optional(&mut *tx).await.core()? else {
                    return Ok(WorkerLifecycleOutcome::UnsupportedSession);
                };
            let contract: ProtocolContract = serde_json::from_value(
                session
                    .try_get("protocol_contract")
                    .core()?,
            )
            .core()?;
            if !contract
                .features
                .contains(&ProtocolFeature::ShutdownAfterIdle)
            {
                return Ok(WorkerLifecycleOutcome::UnsupportedSession);
            }
            Some(
                session
                    .try_get::<Uuid, _>("worker_session_id")
                    .core()?,
            )
        } else {
            None
        };
        let desired_draining = match change.action {
            WorkerLifecycleAction::Cordon => true,
            WorkerLifecycleAction::Uncordon => false,
            WorkerLifecycleAction::ShutdownAfterIdle => draining,
        };
        let desired_shutdown = target.or(shutdown);
        let changed = desired_draining != draining || desired_shutdown != shutdown;
        let generation = worker
            .try_get::<i64, _>("lifecycle_generation")
            .core()?
            + i64::from(changed);
        sqlx::query("UPDATE worker_registry SET draining=$2, shutdown_session_id=$3, lifecycle_generation=$4, updated_at=clock_timestamp() WHERE worker_id=$1")
            .bind(change.worker_id).bind(desired_draining).bind(desired_shutdown).bind(generation)
            .execute(&mut *tx).await.core()?;
        if change.action != WorkerLifecycleAction::Uncordon {
            // Only unaccepted offers are withdrawn; an accepted attempt keeps its lease.
            sqlx::query("WITH withdrawn AS (
                UPDATE worker_attempt SET status='expired', updated_at=clock_timestamp()
                WHERE worker_id=$1 AND status='offered' RETURNING job_id, claim_token
            ) UPDATE job SET status='queued', claim_token=NULL, claimed_at=NULL, updated_at=clock_timestamp()
                FROM withdrawn WHERE job.id=withdrawn.job_id AND job.claim_token=withdrawn.claim_token AND job.status='claimed'")
                .bind(change.worker_id).execute(&mut *tx).await.core()?;
        }
        sqlx::query("UPDATE worker_session SET status=CASE WHEN $2 THEN 'draining'::worker_session_status ELSE 'idle'::worker_session_status END
            WHERE worker_id=$1 AND status IN ('registered','idle','offered','draining') AND expires_at > clock_timestamp()")
            .bind(change.worker_id).bind(desired_draining).execute(&mut *tx).await.core()?;
        let receipt = WorkerLifecycleReceipt {
            request_id: change.request_id,
            worker_id: change.worker_id,
            action: change.action,
            generation,
            target_session_id: target,
            actor: change.actor.clone(),
            reason: change.reason.clone(),
            recorded_at: sqlx::query_scalar("SELECT clock_timestamp()")
                .fetch_one(&mut *tx)
                .await
                .core()?,
        };
        let action = serde_json::to_value(change.action).core()?;
        sqlx::query("INSERT INTO worker_lifecycle_audit (request_id, worker_id, action, actor, reason, receipt) VALUES ($1,$2,$3,$4,$5,$6)")
            .bind(change.request_id).bind(change.worker_id).bind(action.as_str().unwrap())
            .bind(&change.actor).bind(&change.reason).bind(serde_json::to_value(&receipt).core()?)
            .execute(&mut *tx).await.core()?;
        tx.commit().await.core()?;
        Ok(WorkerLifecycleOutcome::Applied(receipt))
    }
}
