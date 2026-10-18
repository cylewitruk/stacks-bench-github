//! Authenticated scheduler maintenance; no worker policy mutations.

use axum::Json;
use axum::extract::State;
use sbgh_api::{SchedulerChangeRequest, SchedulerPolicyView, SchedulerStatusView};
use sbgh_core::db::scheduler::{
    SchedulerChange, SchedulerChangeOutcome, SchedulerPolicy, SchedulerStore,
};
use sbgh_postgres::PostgresFleetStore;
use uuid::Uuid;

use super::error::ApiErr;
use super::state::ApiState;

/// Read a consistent policy and quiescence snapshot.
pub async fn status(State(state): State<ApiState>) -> Result<Json<SchedulerStatusView>, ApiErr> {
    let snapshot = PostgresFleetStore::new(state.pool)
        .scheduler_snapshot()
        .await?;
    Ok(Json(SchedulerStatusView {
        policy: policy_view(snapshot.policy),
        active_attempts: snapshot.active_attempts,
        pending_cleanup: snapshot.pending_cleanup,
        pending_projection: snapshot.pending_projection,
        pending_artifacts: snapshot.pending_artifacts,
        garbage_artifacts: snapshot.garbage_artifacts,
    }))
}

/// Stop admission without cancelling accepted work or draining workers.
pub async fn pause(
    State(state): State<ApiState>,
    Json(request): Json<SchedulerChangeRequest>,
) -> Result<Json<SchedulerPolicyView>, ApiErr> {
    change(state, request, true).await
}

/// Restore admission without restarting workers.
pub async fn resume(
    State(state): State<ApiState>,
    Json(request): Json<SchedulerChangeRequest>,
) -> Result<Json<SchedulerPolicyView>, ApiErr> {
    change(state, request, false).await
}

/// Validate and persist one request from an admin-protected route.
async fn change(
    state: ApiState,
    request: SchedulerChangeRequest,
    paused: bool,
) -> Result<Json<SchedulerPolicyView>, ApiErr> {
    let request_id = Uuid::parse_str(&request.request_id)
        .map_err(|_| ApiErr::bad_request("request_id must be a UUID"))?;
    if request
        .reason
        .trim()
        .is_empty()
        || request.reason.chars().count() > 1024
    {
        return Err(ApiErr::bad_request("reason must contain 1–1024 characters and not be blank"));
    }
    let result = PostgresFleetStore::new(state.pool)
        .change_scheduler(&SchedulerChange {
            request_id,
            paused,
            actor: "api:admin".into(),
            reason: request.reason,
        })
        .await?;
    match result {
        SchedulerChangeOutcome::Applied(policy) => Ok(Json(policy_view(policy))),
        SchedulerChangeOutcome::Conflict => {
            Err(ApiErr::conflict("request_id was already used with different input"))
        }
    }
}

/// Convert persistence state into the public API contract.
pub fn policy_view(policy: SchedulerPolicy) -> SchedulerPolicyView {
    SchedulerPolicyView {
        paused: policy.paused,
        generation: policy.generation,
        actor: policy.actor,
        reason: policy.reason,
        changed_at: policy.changed_at.to_rfc3339(),
    }
}
