//! Operator commands for `github_user_role` grants (slice 6).
//!
//! `user grant --login foo --install 100 [--repo 10] --role
//! trigger_pr_benchmark` resolves login → numeric user id via the same
//! unauthenticated `/users/{login}` lookup used by `installer allow`,
//! upserts the `github_user` row, then inserts the role grant.
//!
//! `--repo N` narrows the grant to a single repo within the install.
//! Omitting `--repo` grants the role install-wide. See the slice 6
//! migration doc-comment for the scope model rationale; in short,
//! grants are always per-installation (matching the principle that
//! "installation is the tenant boundary") and repo is the optional
//! finer narrowing.
//!
//! `user revoke` is the inverse; `user list` is the read.

use crate::mapping::Db;
use thiserror::Error;

use crate::admin::gh_resolve::{ResolveError, resolve_account};
use crate::db::{GrantRoleOutcome, Pool, PostgresUserStore, UserStore};
use crate::models::{GithubUser, GithubUserRole, UserRole};

#[derive(Debug, Error)]
pub enum UserError {
    #[error("GitHub API: account `{0}` not found")]
    AccountNotFound(String),
    #[error(
        "Database: no grant matched (user={user_id}, install={install_id}, repo={repo_id:?}, \
         role={role:?})"
    )]
    GrantNotFound { user_id: i64, install_id: i64, repo_id: Option<i64>, role: UserRole },
    #[error("Database: user id {0} unknown")]
    UnknownUser(i64),
    #[error(
        "Database: no active github_installation_repo membership for (install={install_id}, \
         repo={repo_id}) — add the repo to the installation first; this pre-check prevents a \
         stale grant from silently becoming active if the repo is later added to the install"
    )]
    NoActiveMembership { install_id: i64, repo_id: i64 },
    #[error("GitHub API rejected the request ({status}): {body}")]
    GithubRejected { status: u16, body: String },
    #[error("HTTP transport: {0}")]
    Http(#[from] reqwest::Error),
    #[error("Database: {0}")]
    Db(#[from] sqlx::Error),
    #[error("Datastore: {0}")]
    Store(#[from] crate::Error),
    #[error("unsupported GitHub account type: {0}")]
    UnsupportedAccountType(String),
}

impl From<ResolveError> for UserError {
    fn from(e: ResolveError) -> Self {
        match e {
            ResolveError::AccountNotFound(s) => Self::AccountNotFound(s),
            ResolveError::GithubRejected { status, body } => Self::GithubRejected { status, body },
            ResolveError::Http(e) => Self::Http(e),
            ResolveError::UnsupportedAccountType(s) => Self::UnsupportedAccountType(s),
        }
    }
}

/// Resolve login → id via GH API, upsert `github_user`, then grant.
pub async fn grant_role(
    pool: &Pool,
    api_base_url: &str,
    login: &str,
    install_id: i64,
    repo_id: Option<i64>,
    role: UserRole,
) -> Result<GrantRoleOutcome, UserError> {
    let resolved = resolve_account(api_base_url, login).await?;

    // Upsert the user row so the FK target exists. The processor would
    // do this anyway on the next sighting, but doing it here lets
    // `user grant` work for a user we've never seen a webhook from.
    sqlx::query(
        "INSERT INTO github_user (id, login, user_type)
         VALUES ($1, $2, $3)
         ON CONFLICT (id) DO UPDATE
             SET login = EXCLUDED.login,
                 user_type = EXCLUDED.user_type,
                 updated_at = NOW()",
    )
    .bind(resolved.id)
    .bind(&resolved.login)
    .bind(Db(resolved.account_type))
    .execute(pool)
    .await?;

    grant_role_by_user_id(pool, resolved.id, install_id, repo_id, role).await
}

/// Grant by numeric user id. Exposed so operators with the id in hand
/// can skip the GH API round-trip, and so integration tests can drive
/// the SQL path directly.
///
/// Pre-checks two things to surface friendlier errors than raw FK
/// violations:
///
///   1. The user exists.
///   2. If `repo_id` is Some, an active `github_installation_repo` membership
///      exists for `(install_id, repo_id)`. This blocks the operator typo of
///      granting a role on a repo the installation can't see — otherwise the
///      grant sits inert (the runtime membership gate denies it) but would
///      silently become active if the repo were later added to the
///      installation. Install-wide grants (`repo_id = None`) skip this check by
///      design (they apply to whichever repos the install does or will have
///      access to).
pub async fn grant_role_by_user_id(
    pool: &Pool,
    user_id: i64,
    install_id: i64,
    repo_id: Option<i64>,
    role: UserRole,
) -> Result<GrantRoleOutcome, UserError> {
    let user_exists: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM github_user WHERE id = $1)")
            .bind(user_id)
            .fetch_one(pool)
            .await?;
    if !user_exists {
        return Err(UserError::UnknownUser(user_id));
    }

    if let Some(rid) = repo_id {
        let membership_active: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1 FROM github_installation_repo
                 WHERE github_installation_id = $1
                   AND github_repo_id = $2
                   AND revoked_at IS NULL
            )",
        )
        .bind(install_id)
        .bind(rid)
        .fetch_one(pool)
        .await?;
        if !membership_active {
            return Err(UserError::NoActiveMembership { install_id, repo_id: rid });
        }
    }

    let store = PostgresUserStore::new(pool.clone());
    let outcome = store
        .grant_role(user_id, install_id, repo_id, role, None)
        .await?;
    Ok(outcome)
}

/// Resolve login → id, then revoke. See `grant_role` for the API rationale.
pub async fn revoke_role(
    pool: &Pool,
    api_base_url: &str,
    login: &str,
    install_id: i64,
    repo_id: Option<i64>,
    role: UserRole,
) -> Result<GithubUserRole, UserError> {
    let resolved = resolve_account(api_base_url, login).await?;
    revoke_role_by_user_id(pool, resolved.id, install_id, repo_id, role).await
}

pub async fn revoke_role_by_user_id(
    pool: &Pool,
    user_id: i64,
    install_id: i64,
    repo_id: Option<i64>,
    role: UserRole,
) -> Result<GithubUserRole, UserError> {
    let store = PostgresUserStore::new(pool.clone());
    let row = store
        .revoke_role(user_id, install_id, repo_id, role)
        .await?;
    row.ok_or(UserError::GrantNotFound {
        user_id,
        install_id,
        repo_id,
        role,
    })
}

pub async fn list_roles(
    pool: &Pool,
    install_id: Option<i64>,
) -> Result<Vec<GithubUserRole>, UserError> {
    let store = PostgresUserStore::new(pool.clone());
    Ok(store
        .list_roles(install_id)
        .await?)
}

/// `sbgh-cli user list` companion: show every `github_user` row known
/// to the database (independent of role grants). Lets operators pick
/// an id without going through the GH API a second time.
pub async fn list_users(pool: &Pool) -> Result<Vec<GithubUser>, UserError> {
    let rows = sqlx::query_as::<_, Db<GithubUser>>(
        "SELECT id, login, user_type, created_at, updated_at
           FROM github_user
       ORDER BY login",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| row.0)
        .collect())
}
