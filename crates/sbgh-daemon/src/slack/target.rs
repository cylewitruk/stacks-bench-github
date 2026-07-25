//! Resolve `[slack].default_repository` ("owner/name") to the FK ids a Slack
//! ad-hoc job is enqueued against (item `0002`, v5 wiring).
//!
//! A pure two-row lookup against the daemon's OWN tables — no GitHub call —
//! mirroring `/api/resolve`: the install is the active (non-deleted,
//! non-suspended) one on `owner`'s account; the repo is the `github_repo` row
//! for `owner/name`, materialised once the daemon has seen it (install / PR
//! event). Run ONCE at startup so a misconfigured `default_repository` fails
//! fast — before the Socket Mode receive loop accepts any mention.

use std::fmt;

use sbgh_postgres::Pool;

/// The resolved code-under-test for Slack jobs: the configured default repo as
/// its `(installation, repo)` FK ids. Resolved once at startup via
/// [`resolve_target`] and held by the connector for the process lifetime.
#[derive(Debug, Clone, Copy)]
pub struct SlackJobTarget {
    pub installation_id: i64,
    pub repo_id: i64,
}

/// Why resolving `[slack].default_repository` failed — each maps to an
/// actionable misconfiguration (mirrors `/api/resolve`'s 400/404/409). A
/// startup-fatal error: the daemon won't accept Slack mentions it can't
/// enqueue.
#[derive(Debug)]
pub enum ResolveTargetError {
    /// Not a single non-empty `owner/name` slug.
    MalformedRepository(String),
    /// No active (non-deleted) installation on `owner`'s account.
    InstallationNotFound(String),
    /// The installation exists but is suspended (jobs would be denied).
    InstallationSuspended(String),
    /// The repo isn't materialised in the daemon's tables yet.
    RepoNotFound(String),
    /// A database error during resolution.
    Db(sqlx::Error),
}

impl fmt::Display for ResolveTargetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MalformedRepository(s) => {
                write!(f, "`[slack].default_repository` = `{s}` is not a single `owner/name` slug")
            }
            Self::InstallationNotFound(owner) => write!(
                f,
                "no active installation for account `{owner}` — install the App there first"
            ),
            Self::InstallationSuspended(owner) => write!(
                f,
                "installation for `{owner}` is suspended — unsuspend it before enabling Slack"
            ),
            Self::RepoNotFound(slug) => write!(
                f,
                "repo `{slug}` is not known to the daemon yet — install the App on it or open a \
                 PR from it so it gets materialised"
            ),
            Self::Db(e) => write!(f, "database error resolving the Slack default repository: {e}"),
        }
    }
}

impl std::error::Error for ResolveTargetError {}

impl From<sqlx::Error> for ResolveTargetError {
    fn from(e: sqlx::Error) -> Self {
        Self::Db(e)
    }
}

/// Split a `default_repository` value into `(owner, name)`, rejecting anything
/// that isn't exactly one non-empty `owner/name` pair (a second `/`, an empty
/// side, or no `/` at all).
fn parse_slug(slug: &str) -> Result<(&str, &str), ResolveTargetError> {
    let trimmed = slug.trim();
    match trimmed.split_once('/') {
        Some((owner, name)) if !owner.is_empty() && !name.is_empty() && !name.contains('/') => {
            Ok((owner, name))
        }
        _ => Err(ResolveTargetError::MalformedRepository(slug.to_string())),
    }
}

/// Resolve `default_repository` ("owner/name") to its `(installation, repo)` FK
/// ids. Case-insensitive on both the account login and the repo owner/name
/// (GitHub identifiers are case-insensitive). See the module docs for the
/// lookup semantics.
pub async fn resolve_target(
    pool: &Pool,
    default_repository: &str,
) -> Result<SlackJobTarget, ResolveTargetError> {
    let (owner, name) = parse_slug(default_repository)?;

    // Active installation on the account. Suspended installs are still selected
    // here so we can give a *specific* "suspended" error rather than an
    // indistinguishable "not found" one (mirrors `/api/resolve`).
    let install = sqlx::query_as::<_, (i64, bool)>(
        r#"
        SELECT id, (suspended_at IS NOT NULL) AS suspended
          FROM github_installation
         WHERE LOWER(account_login) = LOWER($1)
           AND deleted_at IS NULL
         ORDER BY created_at DESC
         LIMIT 1
        "#,
    )
    .bind(owner)
    .fetch_optional(pool)
    .await?;

    let Some((installation_id, suspended)) = install else {
        return Err(ResolveTargetError::InstallationNotFound(owner.to_string()));
    };
    if suspended {
        return Err(ResolveTargetError::InstallationSuspended(owner.to_string()));
    }

    // Repo identity — materialised from install / PR events.
    let repo = sqlx::query_as::<_, (i64,)>(
        r#"
        SELECT id
          FROM github_repo
         WHERE LOWER(owner) = LOWER($1)
           AND LOWER(name) = LOWER($2)
        "#,
    )
    .bind(owner)
    .bind(name)
    .fetch_optional(pool)
    .await?;

    let Some((repo_id,)) = repo else {
        return Err(ResolveTargetError::RepoNotFound(format!("{owner}/{name}")));
    };

    Ok(SlackJobTarget { installation_id, repo_id })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_owner_name() {
        assert!(matches!(parse_slug("octo/core"), Ok(("octo", "core"))));
        // Surrounding whitespace is trimmed.
        assert!(matches!(parse_slug("  octo/core  "), Ok(("octo", "core"))));
    }

    #[test]
    fn rejects_malformed_slugs() {
        for bad in ["octo", "octo/", "/core", "octo/core/extra", "", "   ", "/"] {
            assert!(
                matches!(parse_slug(bad), Err(ResolveTargetError::MalformedRepository(_))),
                "expected `{bad}` to be rejected"
            );
        }
    }
}
