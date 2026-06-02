//! `/api/users` + `/api/roles` — known GitHub users and their
//! per-installation role grants.

use axum::Json;
use axum::extract::{Query, State};
use sbgh_api::{GrantRoleResult, RoleRequest, RoleView, UserView};
use sbgh_core::admin;
use sbgh_core::models::{GithubUser, GithubUserRole, UserRole};
use serde::Deserialize;

use crate::api::conv::{enum_str, parse_enum};
use crate::api::error::ApiErr;
use crate::api::extract::ApiJson;
use crate::api::state::ApiState;

#[derive(Debug, Deserialize)]
pub struct InstallFilter {
    install_id: Option<i64>,
}

fn user_view(u: GithubUser) -> UserView {
    UserView {
        id: u.id,
        login: u.login,
        user_type: enum_str(&u.user_type),
    }
}

fn role_view(r: GithubUserRole) -> RoleView {
    RoleView {
        id: r.id,
        user_id: r.github_user_id,
        install_id: r.github_installation_id,
        repo_id: r.github_repo_id,
        role: enum_str(&r.granted_role),
        revoked: r.revoked_at.is_some(),
    }
}

pub async fn list_users(State(s): State<ApiState>) -> Result<Json<Vec<UserView>>, ApiErr> {
    let rows = admin::list_users(&s.pool).await?;
    Ok(Json(
        rows.into_iter()
            .map(user_view)
            .collect(),
    ))
}

pub async fn list_roles(
    State(s): State<ApiState>,
    Query(f): Query<InstallFilter>,
) -> Result<Json<Vec<RoleView>>, ApiErr> {
    let rows = admin::list_roles(&s.pool, f.install_id).await?;
    Ok(Json(
        rows.into_iter()
            .map(role_view)
            .collect(),
    ))
}

fn parse_role(s: &str) -> Result<UserRole, ApiErr> {
    parse_enum(s).ok_or_else(|| {
        ApiErr::bad_request("`role` must be `admin`, `trigger_pr_benchmark`, or `view_results`")
    })
}

pub async fn grant(
    State(s): State<ApiState>,
    ApiJson(req): ApiJson<RoleRequest>,
) -> Result<Json<GrantRoleResult>, ApiErr> {
    let role = parse_role(&req.role)?;
    let outcome = match (req.login, req.user_id) {
        (Some(login), None) => {
            admin::grant_role(&s.pool, &s.gh_api_base, &login, req.install, req.repo, role).await?
        }
        (None, Some(uid)) => {
            admin::grant_role_by_user_id(&s.pool, uid, req.install, req.repo, role).await?
        }
        (None, None) => {
            return Err(ApiErr::bad_request("exactly one of `login` or `user_id` is required"));
        }
        (Some(_), Some(_)) => {
            return Err(ApiErr::bad_request("`login` and `user_id` are mutually exclusive"));
        }
    };
    Ok(Json(GrantRoleResult {
        role: role_view(outcome.role),
        created: outcome.created,
    }))
}

pub async fn revoke(
    State(s): State<ApiState>,
    ApiJson(req): ApiJson<RoleRequest>,
) -> Result<Json<RoleView>, ApiErr> {
    let role = parse_role(&req.role)?;
    let row = match (req.login, req.user_id) {
        (Some(login), None) => {
            admin::revoke_role(&s.pool, &s.gh_api_base, &login, req.install, req.repo, role).await?
        }
        (None, Some(uid)) => {
            admin::revoke_role_by_user_id(&s.pool, uid, req.install, req.repo, role).await?
        }
        (None, None) => {
            return Err(ApiErr::bad_request("exactly one of `login` or `user_id` is required"));
        }
        (Some(_), Some(_)) => {
            return Err(ApiErr::bad_request("`login` and `user_id` are mutually exclusive"));
        }
    };
    Ok(Json(role_view(row)))
}
