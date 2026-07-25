//! `/api/repos` — the operator's supported canonical repo roots. Forks of
//! an enabled root are accepted automatically via lineage.

use axum::Json;
use axum::extract::State;
use sbgh_api::{AllowRepoRequest, DisableRepoRequest, RepoRootView};
use sbgh_postgres::admin;
use sbgh_postgres::admin::AllowedRepoRoot;

use crate::api::error::ApiErr;
use crate::api::extract::ApiJson;
use crate::api::state::ApiState;

fn view(r: AllowedRepoRoot) -> RepoRootView {
    RepoRootView {
        repo_id: r.github_repo_id,
        owner: r.owner,
        name: r.name,
        is_enabled: r.is_enabled,
        note: r.note,
    }
}

pub async fn list(State(s): State<ApiState>) -> Result<Json<Vec<RepoRootView>>, ApiErr> {
    let rows = admin::list_repo_roots(&s.pool).await?;
    Ok(Json(
        rows.into_iter()
            .map(view)
            .collect(),
    ))
}

pub async fn allow(
    State(s): State<ApiState>,
    ApiJson(req): ApiJson<AllowRepoRequest>,
) -> Result<Json<RepoRootView>, ApiErr> {
    let row =
        admin::allow_repo_root(&s.pool, &s.gh_api_base, &req.owner, &req.name, req.note.as_deref())
            .await?;
    Ok(Json(view(row)))
}

pub async fn disable(
    State(s): State<ApiState>,
    ApiJson(req): ApiJson<DisableRepoRequest>,
) -> Result<Json<RepoRootView>, ApiErr> {
    let row = match (req.owner, req.name, req.repo_id) {
        (Some(owner), Some(name), None) => {
            admin::disable_repo_root(&s.pool, &s.gh_api_base, &owner, &name).await?
        }
        (None, None, Some(id)) => admin::disable_repo_root_by_id(&s.pool, id).await?,
        (None, None, None) => {
            return Err(ApiErr::bad_request(
                "exactly one of `owner`+`name` or `repo_id` is required",
            ));
        }
        _ => {
            return Err(ApiErr::bad_request("`owner`+`name` and `repo_id` are mutually exclusive"));
        }
    };
    Ok(Json(view(row)))
}
