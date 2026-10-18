//! Explicit fleet cancellation; never called by process signal handling.

use axum::Json;
use axum::extract::State;
use sbgh_api::{FleetAbortReceiptView, FleetAbortRequest, FleetAbortScope};
use sbgh_core::db::fleet_abort::{
    FleetAbortOutcome, FleetAbortRequest as AbortRequest, FleetAbortScope as AbortScope,
    FleetAbortStore,
};
use sbgh_postgres::PostgresFleetStore;
use uuid::Uuid;

use super::error::ApiErr;
use super::state::ApiState;

/// Validate explicit intent and commit through the admin-authenticated route.
pub async fn abort(
    State(state): State<ApiState>,
    Json(request): Json<FleetAbortRequest>,
) -> Result<Json<FleetAbortReceiptView>, ApiErr> {
    let request_id = Uuid::parse_str(&request.request_id)
        .map_err(|_| ApiErr::bad_request("request_id must be a UUID"))?;
    if request_id.is_nil()
        || request.scheduler_generation < 0
        || request
            .reason
            .trim()
            .is_empty()
        || request.reason.chars().count() > 1024
    {
        return Err(ApiErr::bad_request(
            "abort requires a non-nil UUID, nonnegative generation and 1–1024 character reason",
        ));
    }
    let scope = match request.scope {
        FleetAbortScope::Active => AbortScope::Active,
        FleetAbortScope::AllPending => AbortScope::AllPending,
    };
    match PostgresFleetStore::new(state.pool)
        .abort_fleet(&AbortRequest {
            request_id,
            scope,
            scheduler_generation: request.scheduler_generation,
            actor: "api:admin".into(),
            reason: request.reason,
        })
        .await?
    {
        FleetAbortOutcome::Applied(receipt) => Ok(Json(FleetAbortReceiptView {
            request_id: receipt.request_id.to_string(),
            scope: request.scope,
            scheduler_generation: receipt.scheduler_generation,
            actor: receipt.actor,
            reason: receipt.reason,
            recorded_at: receipt
                .recorded_at
                .to_rfc3339(),
            selected_job_ids: receipt
                .selected_job_ids
                .iter()
                .map(Uuid::to_string)
                .collect(),
            accepted_job_ids: receipt
                .accepted_job_ids
                .iter()
                .map(Uuid::to_string)
                .collect(),
        })),
        FleetAbortOutcome::Conflict => {
            Err(ApiErr::conflict("request_id was already used with different input"))
        }
        FleetAbortOutcome::MaintenanceRequired => {
            Err(ApiErr::conflict("scheduler must be paused at the specified generation"))
        }
    }
}
