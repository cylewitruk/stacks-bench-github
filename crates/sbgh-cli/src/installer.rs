//! Operator commands for the `allowed_installer` allowlist.
//!
//! The allowlist gates which GitHub accounts may install the App: the
//! processor checks every `installation.created` webhook against this table
//! and denies installs whose account isn't enabled. See
//! `migrations/_design/target_schema.sql` for the schema rationale.
//!
//! The login→id lookup uses GitHub's `/users/{login}` endpoint
//! **unauthenticated**. That endpoint doesn't accept App JWTs (per the
//! REST docs for "Get a user"), and an operator running this CLI
//! at-most-a-handful-of-times-per-deploy is well inside the 60/hr
//! unauthenticated rate limit. Bonus: no App private-key plumbing
//! needed in the CLI's env, so `installer allow` runs cleanly from a
//! docker one-shot with just `DATABASE_URL`.
//!
//! Returns concrete typed errors (`InstallerError`) rather than `anyhow`
//! so the integration tests can match on specific failure shapes (account
//! not found, GitHub API rejected the request, etc.) without
//! string-matching.

use reqwest::header::{ACCEPT, HeaderMap, HeaderValue, USER_AGENT};
use sbgh_core::db::Pool;
use sbgh_core::models::{AllowedInstaller, GithubAccountType};
use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum InstallerError {
    #[error("GitHub API: account `{0}` not found")]
    AccountNotFound(String),
    #[error("Database: account_id {0} not on the allowlist")]
    NotOnAllowlist(i64),
    #[error("GitHub API rejected the request ({status}): {body}")]
    GithubRejected { status: u16, body: String },
    #[error("HTTP transport: {0}")]
    Http(#[from] reqwest::Error),
    #[error("Database: {0}")]
    Db(#[from] sqlx::Error),
    #[error("unsupported GitHub account type: {0}")]
    UnsupportedAccountType(String),
}

/// Resolve `login` → `(account_id, account_type)` via GitHub's `/users/{login}`
/// endpoint (which works for both User and Organization accounts), then
/// upsert the row in `allowed_installer` with `is_enabled=TRUE`.
///
/// Idempotent on re-run: re-allowing an already-allowed account is a no-op
/// beyond refreshing `account_login` (in case the user renamed) and
/// resetting `is_enabled` back to TRUE.
pub async fn allow_installer(
    pool: &Pool,
    api_base_url: &str,
    login: &str,
    note: Option<&str>,
) -> Result<AllowedInstaller, InstallerError> {
    let resolved = resolve_account(api_base_url, login).await?;

    let row: AllowedInstaller = sqlx::query_as(
        "INSERT INTO allowed_installer
             (github_account_id, account_login, account_type, is_enabled, note)
         VALUES ($1, $2, $3, TRUE, $4)
         ON CONFLICT (github_account_id) DO UPDATE
             SET account_login = EXCLUDED.account_login,
                 account_type  = EXCLUDED.account_type,
                 is_enabled    = TRUE,
                 note          = COALESCE(EXCLUDED.note, allowed_installer.note),
                 updated_at    = NOW()
         RETURNING github_account_id, account_login, account_type,
                   is_enabled, note, created_at, updated_at",
    )
    .bind(resolved.id)
    .bind(&resolved.login)
    .bind(resolved.account_type)
    .bind(note)
    .fetch_one(pool)
    .await?;
    Ok(row)
}

/// Soft-disable an installer by login. Resolves the login → numeric
/// account id via the GitHub API first, then disables by id — display
/// logins aren't unique (renames; eventual re-use of recycled logins)
/// and the schema deliberately keys on `github_account_id`.
///
/// Errors with `AccountNotFound` if GitHub can't resolve the login, or
/// `NotOnAllowlist` if the resolved account isn't in `allowed_installer`.
pub async fn disable_installer(
    pool: &Pool,
    api_base_url: &str,
    login: &str,
) -> Result<AllowedInstaller, InstallerError> {
    let resolved = resolve_account(api_base_url, login).await?;
    disable_installer_by_account_id(pool, resolved.id).await
}

/// Disable by numeric account id. Exposed as a library entry point so
/// operators (or future tooling) can disable directly when they already
/// have the id, and so integration tests can exercise the SQL path
/// without an HTTP round-trip.
pub async fn disable_installer_by_account_id(
    pool: &Pool,
    github_account_id: i64,
) -> Result<AllowedInstaller, InstallerError> {
    let row: Option<AllowedInstaller> = sqlx::query_as(
        "UPDATE allowed_installer
            SET is_enabled = FALSE, updated_at = NOW()
          WHERE github_account_id = $1
       RETURNING github_account_id, account_login, account_type,
                 is_enabled, note, created_at, updated_at",
    )
    .bind(github_account_id)
    .fetch_optional(pool)
    .await?;
    row.ok_or(InstallerError::NotOnAllowlist(github_account_id))
}

pub async fn list_installers(pool: &Pool) -> Result<Vec<AllowedInstaller>, InstallerError> {
    let rows = sqlx::query_as(
        "SELECT github_account_id, account_login, account_type,
                is_enabled, note, created_at, updated_at
           FROM allowed_installer
       ORDER BY account_login",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

#[derive(Debug, Clone)]
struct ResolvedAccount {
    id: i64,
    login: String,
    account_type: GithubAccountType,
}

async fn resolve_account(
    api_base_url: &str,
    login: &str,
) -> Result<ResolvedAccount, InstallerError> {
    let url = format!("{}/users/{}", api_base_url.trim_end_matches('/'), login);

    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/vnd.github+json"));
    headers.insert(USER_AGENT, HeaderValue::from_static("sbgh-cli"));

    let resp = reqwest::Client::new()
        .get(&url)
        .headers(headers)
        .send()
        .await?;

    if resp.status().as_u16() == 404 {
        return Err(InstallerError::AccountNotFound(login.to_string()));
    }
    if !resp.status().is_success() {
        let status = resp.status().as_u16();
        let body = resp
            .text()
            .await
            .unwrap_or_default();
        return Err(InstallerError::GithubRejected { status, body });
    }

    #[derive(Deserialize)]
    struct UserResp {
        id: i64,
        login: String,
        #[serde(rename = "type")]
        kind: String,
    }
    let body: UserResp = resp.json().await?;
    let account_type = match body.kind.as_str() {
        "User" => GithubAccountType::User,
        "Organization" => GithubAccountType::Organization,
        "Bot" => GithubAccountType::Bot,
        other => return Err(InstallerError::UnsupportedAccountType(other.to_string())),
    };
    Ok(ResolvedAccount {
        id: body.id,
        login: body.login,
        account_type,
    })
}
