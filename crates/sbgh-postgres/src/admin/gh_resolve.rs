//! Shared GitHub login → numeric id resolver. Used by `installer.rs`
//! (allowlist by org/user login) and `user.rs` (role grants by user
//! login), and (via [`http_client`] / [`is_valid_github_name`]) by
//! `repo.rs`.
//!
//! The endpoint is GitHub's unauthenticated `GET /users/{login}`, which
//! works for both User and Organization accounts. 60/hr per IP is plenty
//! for operator one-shots — no App credentials needed.

use std::time::Duration;

use reqwest::header::{ACCEPT, HeaderValue};
use serde::Deserialize;
use thiserror::Error;

use crate::models::GithubAccountType;

/// Shared HTTP client for GitHub resolution, with a bounded timeout so an
/// `/api` request task can't hang on a slow upstream.
pub fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .user_agent("sbgh")
        .build()
        .expect("building reqwest client")
}

/// Conservative validation of a GitHub login / repo name **before** it is
/// interpolated into a URL path segment. Rejects anything outside the
/// login/repo-name charset — `/`, whitespace, `#`, `%`, `?`, control
/// chars — and the `.`/`..` path-traversal segments (which the `url` crate
/// would otherwise normalize away). Valid GitHub names are a strict subset
/// of what this allows, so it never rejects a real name.
pub fn is_valid_github_name(s: &str) -> bool {
    if s.is_empty() || s.len() > 100 || s == "." || s == ".." {
        return false;
    }
    s.bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}

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
    // Reject path-injecting / nonsense logins before URL construction — a
    // name GitHub can't have is the same as "not found" to the caller.
    if !is_valid_github_name(login) {
        return Err(ResolveError::AccountNotFound(login.to_string()));
    }
    let url = format!("{}/users/{}", api_base_url.trim_end_matches('/'), login);

    let resp = http_client()
        .get(&url)
        .header(ACCEPT, HeaderValue::from_static("application/vnd.github+json"))
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

#[cfg(test)]
mod tests {
    use super::is_valid_github_name as v;

    #[test]
    fn rejects_path_injection_and_traversal() {
        // Valid GitHub-ish names.
        assert!(v("octocat"));
        assert!(v("my-org"));
        assert!(v("repo.js"));
        assert!(v("a_b-c.d"));
        // Injection / traversal / junk.
        assert!(!v(""));
        assert!(!v("."));
        assert!(!v(".."));
        assert!(!v("a/b"));
        assert!(!v("a%2Fb"));
        assert!(!v("a b"));
        assert!(!v("a#b"));
        assert!(!v("a?b"));
        assert!(!v(&"x".repeat(101)));
    }
}
