//! `/api/installers` — the operator allowlist of accounts permitted to
//! install the App.

use axum::Json;
use axum::extract::State;
use sbgh_api::{AllowInstallerRequest, DisableInstallerRequest, InstallerView};
use sbgh_core::models::AllowedInstaller;
use sbgh_postgres::admin;

use crate::api::conv::enum_str;
use crate::api::error::ApiErr;
use crate::api::extract::ApiJson;
use crate::api::state::ApiState;

fn view(r: AllowedInstaller) -> InstallerView {
    InstallerView {
        account_id: r.github_account_id,
        login: r.account_login,
        account_type: enum_str(&r.account_type),
        is_enabled: r.is_enabled,
        note: r.note,
    }
}

pub async fn list(State(s): State<ApiState>) -> Result<Json<Vec<InstallerView>>, ApiErr> {
    let rows = admin::list_installers(&s.pool).await?;
    Ok(Json(
        rows.into_iter()
            .map(view)
            .collect(),
    ))
}

pub async fn allow(
    State(s): State<ApiState>,
    ApiJson(req): ApiJson<AllowInstallerRequest>,
) -> Result<Json<InstallerView>, ApiErr> {
    let row =
        admin::allow_installer(&s.pool, &s.gh_api_base, &req.login, req.note.as_deref()).await?;
    Ok(Json(view(row)))
}

pub async fn disable(
    State(s): State<ApiState>,
    ApiJson(req): ApiJson<DisableInstallerRequest>,
) -> Result<Json<InstallerView>, ApiErr> {
    let row = match (req.login, req.account_id) {
        (Some(login), None) => admin::disable_installer(&s.pool, &s.gh_api_base, &login).await?,
        (None, Some(id)) => admin::disable_installer_by_account_id(&s.pool, id).await?,
        (None, None) => {
            return Err(ApiErr::bad_request("exactly one of `login` or `account_id` is required"));
        }
        (Some(_), Some(_)) => {
            return Err(ApiErr::bad_request("`login` and `account_id` are mutually exclusive"));
        }
    };
    Ok(Json(view(row)))
}
