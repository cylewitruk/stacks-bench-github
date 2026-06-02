//! `/api/installations` — installed App tenants (operational visibility).

use axum::Json;
use axum::extract::State;
use sbgh_api::InstallationView;
use sbgh_core::models::GithubInstallation;

use crate::api::conv::enum_str;
use crate::api::error::ApiErr;
use crate::api::state::ApiState;

fn view(r: GithubInstallation) -> InstallationView {
    InstallationView {
        id: r.id,
        account_id: r.github_account_id,
        account_login: r.account_login,
        account_type: enum_str(&r.account_type),
        suspended: r.suspended_at.is_some(),
        deleted: r.deleted_at.is_some(),
        created_at: r.created_at.to_rfc3339(),
    }
}

pub async fn list(State(s): State<ApiState>) -> Result<Json<Vec<InstallationView>>, ApiErr> {
    let rows = sqlx::query_as::<_, GithubInstallation>(
        "SELECT id, github_account_id, account_login, account_type, suspended_at, deleted_at, \
         created_at, updated_at FROM github_installation ORDER BY created_at DESC",
    )
    .fetch_all(&s.pool)
    .await?;
    Ok(Json(
        rows.into_iter()
            .map(view)
            .collect(),
    ))
}
