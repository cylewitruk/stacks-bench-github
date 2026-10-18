//! Authenticated, retry-safe worker maintenance administration.

use axum::Json;
use axum::extract::{Path, State};
use sbgh_api::{
    CordonSemantic, WorkerAdmissionView, WorkerLifecycleAction as Action,
    WorkerLifecycleReceiptView, WorkerLifecycleRequest, WorkerMaintenanceView, WorkerPreflightView,
};
use sbgh_core::db::worker_lifecycle::{
    WorkerLifecycleAction, WorkerLifecycleChange, WorkerLifecycleOutcome, WorkerLifecycleStore,
};
use sbgh_fleet::ProtocolFeature;
use sbgh_postgres::PostgresFleetStore;
use uuid::Uuid;

use super::error::ApiErr;
use super::state::ApiState;

/// Return policy and worker debt from one database observation.
pub async fn status(
    State(state): State<ApiState>,
    Path(worker_id): Path<Uuid>,
) -> Result<Json<WorkerMaintenanceView>, ApiErr> {
    let snapshot = PostgresFleetStore::new(state.pool)
        .worker_maintenance_snapshot(worker_id)
        .await?
        .ok_or_else(|| ApiErr::not_found("worker not found"))?;
    let cordon_semantic = match snapshot.protocol.as_ref() {
        Some(protocol)
            if protocol
                .features
                .contains(&ProtocolFeature::ResidentCordon) =>
        {
            CordonSemantic::Resident
        }
        Some(_) => CordonSemantic::ExitAfterIdle,
        None => CordonSemantic::Unknown,
    };
    let blockers = snapshot
        .admission_blockers()
        .into_iter()
        .map(|blocker| blocker.as_str().to_owned())
        .collect::<Vec<_>>();
    Ok(Json(WorkerMaintenanceView {
        worker_id: snapshot.worker_id.to_string(),
        enabled: snapshot.enabled,
        preflight: snapshot
            .preflight
            .map(|report| WorkerPreflightView {
                revision: report.revision,
                completed_at_ms: report.completed_at_ms,
                executable_sha256: report.executable_sha256,
                configuration_sha256: report.configuration_sha256,
            }),
        admission: Some(WorkerAdmissionView {
            schedulable: blockers.is_empty(),
            blockers,
            effective_capabilities: snapshot.effective_capabilities,
            measurement_profile: snapshot.measurement_profile,
        }),
        generation: snapshot.generation,
        cordoned: snapshot.cordoned,
        session_id: snapshot
            .session_id
            .map(|id| id.to_string()),
        session_status: snapshot.session_status,
        session_live: snapshot.session_live,
        cordon_semantic,
        protocol_revision: snapshot
            .protocol
            .as_ref()
            .map(|protocol| protocol.revision),
        protocol_features: snapshot
            .protocol
            .map(|protocol| {
                protocol
                    .features
                    .iter()
                    .map(|feature| feature.as_str().to_string())
                    .collect()
            })
            .unwrap_or_default(),
        shutdown_session_id: snapshot
            .shutdown_session_id
            .map(|id| id.to_string()),
        active_attempts: snapshot.active_attempts,
        pending_cleanup: snapshot.pending_cleanup,
    }))
}

/// Commit an attributable action without implicitly changing other policies.
pub async fn change(
    State(state): State<ApiState>,
    Path(worker_id): Path<Uuid>,
    Json(request): Json<WorkerLifecycleRequest>,
) -> Result<Json<WorkerLifecycleReceiptView>, ApiErr> {
    let request_id = Uuid::parse_str(&request.request_id)
        .map_err(|_| ApiErr::bad_request("request_id must be a UUID"))?;
    if request
        .reason
        .trim()
        .is_empty()
        || request.reason.chars().count() > 1024
    {
        return Err(ApiErr::bad_request("reason must be nonempty and at most 1024 characters"));
    }
    let action = match request.action {
        Action::Cordon => WorkerLifecycleAction::Cordon,
        Action::Uncordon => WorkerLifecycleAction::Uncordon,
        Action::ShutdownAfterIdle => WorkerLifecycleAction::ShutdownAfterIdle,
    };
    match PostgresFleetStore::new(state.pool)
        .change_worker_lifecycle(&WorkerLifecycleChange {
            request_id,
            worker_id,
            action,
            actor: "api:admin".into(),
            reason: request.reason,
        })
        .await?
    {
        WorkerLifecycleOutcome::Applied(receipt) => Ok(Json(WorkerLifecycleReceiptView {
            request_id: receipt.request_id.to_string(),
            worker_id: receipt.worker_id.to_string(),
            action: request.action,
            generation: receipt.generation,
            target_session_id: receipt
                .target_session_id
                .map(|id| id.to_string()),
            actor: receipt.actor,
            reason: receipt.reason,
            recorded_at: receipt
                .recorded_at
                .to_rfc3339(),
        })),
        WorkerLifecycleOutcome::NotFound => Err(ApiErr::not_found("worker not found")),
        WorkerLifecycleOutcome::Conflict => {
            Err(ApiErr::conflict("request_id was already used with different input"))
        }
        WorkerLifecycleOutcome::UnsupportedSession => Err(ApiErr::conflict(
            "shutdown-after-idle requires a live session with negotiated support; legacy workers exit on cordon",
        )),
    }
}
