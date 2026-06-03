//! `/api/resolve` — translate an `owner/repo` slug into the ids the
//! policy/role admin commands take (`install_id` + `repo_id`). Backs the
//! CLI's `--on owner/repo` sugar.
//!
//! A pure two-row lookup against the daemon's OWN tables — no GitHub call.
//! The install is the active one on `owner`'s account (GitHub Apps install
//! at most once per account); the repo is the `github_repo` row for
//! `owner/repo`, which exists once the daemon has seen it (install / PR
//! event). A missing install/repo is a `404` and a *suspended* install is a
//! `409` — both with a fix hint.
//!
//! Scope: the resolver checks only install-level state (`deleted_at IS NULL
//! AND suspended_at IS NULL`) — enough to avoid handing back an install that
//! could never run a job. It deliberately does NOT require an active
//! `github_installation_repo` membership (a `source` policy legitimately
//! targets a fork that has none). The per-command handlers + policy FKs
//! still enforce membership / target-policy preconditions where those apply.

use axum::Json;
use axum::extract::{Query, State};
use sbgh_api::ResolveRepoResponse;
use serde::Deserialize;

use crate::api::error::ApiErr;
use crate::api::state::ApiState;

#[derive(Debug, Deserialize)]
pub struct ResolveParams {
    owner: String,
    repo: String,
}

/// `GET /api/resolve?owner=&repo=` (read scope).
pub async fn resolve(
    State(s): State<ApiState>,
    Query(p): Query<ResolveParams>,
) -> Result<Json<ResolveRepoResponse>, ApiErr> {
    let owner = p.owner.trim();
    let repo = p.repo.trim();
    if owner.is_empty() || repo.is_empty() {
        return Err(ApiErr::bad_request("both `owner` and `repo` are required"));
    }

    // Installation on the account. `LOWER(...)` because GitHub logins are
    // case-insensitive; `deleted_at IS NULL` excludes a prior uninstall;
    // newest-first guards the (shouldn't-happen) duplicate case. Suspended
    // installs are still returned here so we can give a *specific* 409 below
    // rather than an indistinguishable "no install" 404.
    let install = sqlx::query_as::<_, (i64, String, bool)>(
        r#"
        SELECT id, account_login, (suspended_at IS NOT NULL) AS suspended
          FROM github_installation
         WHERE LOWER(account_login) = LOWER($1)
           AND deleted_at IS NULL
         ORDER BY created_at DESC
         LIMIT 1
        "#,
    )
    .bind(owner)
    .fetch_optional(&s.pool)
    .await?;

    let Some((install_id, account_login, suspended)) = install else {
        return Err(ApiErr::not_found(format!(
            "no active installation for account `{owner}` — install the App there first"
        )));
    };
    if suspended {
        return Err(ApiErr::conflict(format!(
            "installation for `{owner}` is suspended — unsuspend it before configuring \
             policies/grants (jobs are denied while suspended)"
        )));
    }

    // Repo identity — materialised from installation / PR events.
    let repo_row = sqlx::query_as::<_, (i64, String, String)>(
        r#"
        SELECT id, owner, name
          FROM github_repo
         WHERE LOWER(owner) = LOWER($1)
           AND LOWER(name) = LOWER($2)
        "#,
    )
    .bind(owner)
    .bind(repo)
    .fetch_optional(&s.pool)
    .await?;

    let Some((repo_id, repo_owner, repo_name)) = repo_row else {
        return Err(ApiErr::not_found(format!(
            "repo `{owner}/{repo}` is not known to the daemon yet — install the App on it or open \
             a PR from it so it gets materialised"
        )));
    };

    Ok(Json(ResolveRepoResponse {
        install_id,
        account_login,
        repo_id,
        repo_owner,
        repo_name,
    }))
}
