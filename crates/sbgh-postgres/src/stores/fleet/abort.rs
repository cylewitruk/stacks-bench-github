//! Audited fleet cancellation serialized against admission and policy changes.

use async_trait::async_trait;
use sbgh_core::Error;
use sbgh_core::db::fleet_abort::{
    FleetAbortOutcome, FleetAbortReceipt, FleetAbortRequest, FleetAbortScope, FleetAbortStore,
};
use sqlx::Row;
use uuid::Uuid;

use super::{PostgresFleetStore, cancellation};
use crate::IntoCoreResult;

#[async_trait]
impl FleetAbortStore for PostgresFleetStore {
    async fn abort_fleet(
        &self,
        request: &FleetAbortRequest,
    ) -> sbgh_core::Result<FleetAbortOutcome> {
        if request.request_id.is_nil()
            || request.scheduler_generation < 0
            || request
                .actor
                .trim()
                .is_empty()
            || request.actor.chars().count() > 128
            || request
                .reason
                .trim()
                .is_empty()
            || request.reason.chars().count() > 1024
        {
            return Err(Error::Config("fleet abort requires a non-nil UUID, nonnegative generation and bounded actor/reason".into()));
        }
        let mut tx = self
            .pool
            .begin()
            .await
            .core()?;
        // Global admission lock precedes all attempt/cleanup/job locks.
        let policy = sqlx::query(
            "SELECT paused, generation FROM scheduler_policy WHERE singleton FOR UPDATE",
        )
        .fetch_one(&mut *tx)
        .await
        .core()?;
        let previous: Option<serde_json::Value> =
            sqlx::query_scalar("SELECT receipt FROM fleet_abort_audit WHERE request_id=$1")
                .bind(request.request_id)
                .fetch_optional(&mut *tx)
                .await
                .core()?;
        if let Some(previous) = previous {
            let receipt: FleetAbortReceipt = serde_json::from_value(previous).core()?;
            return Ok(
                if receipt.scope == request.scope
                    && receipt.scheduler_generation == request.scheduler_generation
                    && receipt.actor == request.actor
                    && receipt.reason == request.reason
                {
                    FleetAbortOutcome::Applied(receipt)
                } else {
                    FleetAbortOutcome::Conflict
                },
            );
        }
        if !policy
            .try_get::<bool, _>("paused")
            .core()?
            || policy
                .try_get::<i64, _>("generation")
                .core()?
                != request.scheduler_generation
        {
            return Ok(FleetAbortOutcome::MaintenanceRequired);
        }
        let selected_job_ids: Vec<Uuid> = sqlx::query_scalar(
            "SELECT id FROM job WHERE status IN ('queued','claimed','running')
                AND ($1 OR status='running') ORDER BY id",
        )
        .bind(request.scope == FleetAbortScope::AllPending)
        .fetch_all(&mut *tx)
        .await
        .core()?;
        let mut accepted_job_ids = Vec::new();
        for job_id in &selected_job_ids {
            if cancellation::request(&mut tx, *job_id).await? {
                accepted_job_ids.push(*job_id);
            }
        }
        let receipt = FleetAbortReceipt {
            request_id: request.request_id,
            scope: request.scope,
            scheduler_generation: request.scheduler_generation,
            actor: request.actor.clone(),
            reason: request.reason.clone(),
            recorded_at: sqlx::query_scalar("SELECT clock_timestamp()")
                .fetch_one(&mut *tx)
                .await
                .core()?,
            selected_job_ids,
            accepted_job_ids,
        };
        sqlx::query("INSERT INTO fleet_abort_audit (request_id, receipt) VALUES ($1,$2)")
            .bind(request.request_id)
            .bind(serde_json::to_value(&receipt).core()?)
            .execute(&mut *tx)
            .await
            .core()?;
        tx.commit().await.core()?;
        Ok(FleetAbortOutcome::Applied(receipt))
    }
}
