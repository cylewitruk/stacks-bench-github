//! Shared GitHub login → numeric id resolver. Used by
//! `installer.rs` (allowlist by org/user login) and `user.rs` (role
//! grants by user login).
//!
//! The endpoint is GitHub's unauthenticated `GET /users/{login}`,
//! which works for both User and Organization accounts. 60/hr per IP
//! is plenty for operator one-shots — no App credentials needed in
//! the CLI's env. See `installer.rs` for the original rationale.

use reqwest::header::{ACCEPT, HeaderMap, HeaderValue, USER_AGENT};
use sbgh_core::models::GithubAccountType;
use serde::Deserialize;
use thiserror::Error;

/// Concrete typed errors so callers can `match` on specific failures
/// (account not found, GH rejected, etc.) without string-matching.
/// Each caller crate wraps this in its own error enum.
#[derive(Debug, Error)]
pub enum ResolveError {
    #[error("GitHub API: account `{0}` not found")]
    AccountNotFound(String),
    #[error("GitHub API rejected the request ({status}): {body}")]
    GithubRejected { status: u16, body: String },
    #[error("HTTP transport: {0}")]
    Http(#[from] reqwest::Error),
    #[error("unsupported GitHub account type: {0}")]
    UnsupportedAccountType(String),
}

#[derive(Debug, Clone)]
pub struct ResolvedAccount {
    pub id: i64,
    pub login: String,
    pub account_type: GithubAccountType,
}

pub async fn resolve_account(
    api_base_url: &str,
    login: &str,
) -> Result<ResolvedAccount, ResolveError> {
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
        return Err(ResolveError::AccountNotFound(login.to_string()));
    }
    if !resp.status().is_success() {
        let status = resp.status().as_u16();
        let body = resp
            .text()
            .await
            .unwrap_or_default();
        return Err(ResolveError::GithubRejected { status, body });
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
        other => return Err(ResolveError::UnsupportedAccountType(other.to_string())),
    };
    Ok(ResolvedAccount {
        id: body.id,
        login: body.login,
        account_type,
    })
}
