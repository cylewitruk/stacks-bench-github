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
    let rows = sbgh_postgres::application::list_installations(&s.pool).await?;
    Ok(Json(
        rows.into_iter()
            .map(view)
            .collect(),
    ))
}
