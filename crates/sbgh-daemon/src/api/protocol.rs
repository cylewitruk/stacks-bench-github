//! Authorized protocol-floor administration.

use axum::Json;
use axum::extract::State;
use sbgh_api::{ProtocolPolicyChangeRequest, ProtocolPolicyView};
use sbgh_core::db::protocol::{
    ProtocolPolicy, ProtocolPolicyChange, ProtocolPolicyChangeOutcome, ProtocolPolicyStore,
};
use sbgh_fleet::{NEGOTIATED_PROTOCOL_REVISION, PREVIOUS_PROTOCOL_REVISION};
use sbgh_postgres::PostgresFleetStore;
use uuid::Uuid;

use super::error::ApiErr;
use super::state::ApiState;

/// Inspect the committed floor without changing it.
pub async fn status(State(state): State<ApiState>) -> Result<Json<ProtocolPolicyView>, ApiErr> {
    Ok(Json(view(
        PostgresFleetStore::new(state.pool)
            .protocol_policy()
            .await?,
    )))
}

/// Change the floor after a serialized maintenance/convergence check.
pub async fn change(
    State(state): State<ApiState>,
    Json(request): Json<ProtocolPolicyChangeRequest>,
) -> Result<Json<ProtocolPolicyView>, ApiErr> {
    let request_id = Uuid::parse_str(&request.request_id)
        .map_err(|_| ApiErr::bad_request("request_id must be a UUID"))?;
    if !(PREVIOUS_PROTOCOL_REVISION..=NEGOTIATED_PROTOCOL_REVISION)
        .contains(&request.minimum_revision)
    {
        return Err(ApiErr::bad_request(
            "minimum_revision is outside the supported rolling window",
        ));
    }
    for (text, limit) in [(&request.reason, 1024), (&request.rollback_evidence, 2048)] {
        if text.trim().is_empty() || text.chars().count() > limit {
            return Err(ApiErr::bad_request(
                "reason and rollback_evidence must be nonempty and bounded",
            ));
        }
    }
    match PostgresFleetStore::new(state.pool)
        .change_protocol_policy(&ProtocolPolicyChange {
            request_id,
            minimum_revision: request.minimum_revision,
            actor: "api:admin".into(),
            reason: request.reason,
            rollback_evidence: request.rollback_evidence,
        })
        .await?
    {
        ProtocolPolicyChangeOutcome::Applied(policy) => Ok(Json(view(policy))),
        ProtocolPolicyChangeOutcome::Conflict => {
            Err(ApiErr::conflict("request_id was already used with different input"))
        }
        ProtocolPolicyChangeOutcome::MaintenanceRequired => Err(ApiErr::conflict(
            "pause the scheduler and wait for attempts, cleanup, projection and artifacts to become quiescent",
        )),
        ProtocolPolicyChangeOutcome::WorkersNotConverged(workers) => {
            Err(ApiErr::conflict(format!(
                "enabled workers without live compatible sessions: {}",
                workers
                    .iter()
                    .map(Uuid::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            )))
        }
    }
}

/// Keep persistence representation out of the public HTTP contract.
fn view(policy: ProtocolPolicy) -> ProtocolPolicyView {
    ProtocolPolicyView {
        minimum_revision: policy.minimum_revision,
        generation: policy.generation,
        actor: policy.actor,
        reason: policy.reason,
        rollback_evidence: policy.rollback_evidence,
        changed_at: policy.changed_at.to_rfc3339(),
    }
}
