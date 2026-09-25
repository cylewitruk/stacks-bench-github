//! Resolve Slack `owner/name` repositories to installed targets and enforce
//! operator policy. The configured default is checked at startup; every
//! submission is checked again so revocations take effect without restart.

use std::fmt;

use async_trait::async_trait;
use sbgh_postgres::Pool;
use sbgh_slack::SlackJobTarget;

/// Resolves a Slack code repository and enforces its current operator policy.
#[async_trait]
pub trait SlackRepositoryResolver: Send + Sync {
    /// Resolve a repository only when its current installation and policies allow it.
    async fn resolve(&self, repository: &str) -> sbgh_core::Result<SlackJobTarget>;
}

/// Postgres-backed repository resolver; policy is checked for every request.
pub struct PostgresSlackRepositoryResolver {
    pool: Pool,
}

impl PostgresSlackRepositoryResolver {
    /// Build a resolver against the daemon's database.
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl SlackRepositoryResolver for PostgresSlackRepositoryResolver {
    async fn resolve(&self, repository: &str) -> sbgh_core::Result<SlackJobTarget> {
        let target = resolve_target(&self.pool, repository)
            .await
            .map_err(|error| sbgh_core::Error::Unauthorized(error.to_string()))?;
        let allowed: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT 1
                  FROM github_installation i
                  JOIN allowed_installer a
                    ON a.github_account_id = i.github_account_id
                  JOIN github_installation_repo m
                    ON m.github_installation_id = i.id
                  JOIN github_repo r
                    ON r.id = m.github_repo_id
                  JOIN target_repo_policy t
                    ON t.github_installation_id = i.id
                   AND t.github_repo_id = r.id
                  JOIN source_repo_policy s
                    ON s.github_installation_id = i.id
                   AND s.github_repo_id = r.id
                  JOIN supported_repo_root root
                    ON root.github_repo_id = r.id
                    OR root.github_repo_id = r.fork_root_github_repo_id
                 WHERE i.id = $1
                   AND r.id = $2
                   AND i.deleted_at IS NULL
                   AND i.suspended_at IS NULL
                   AND a.is_enabled
                   AND m.revoked_at IS NULL
                   AND t.is_enabled
                   AND s.is_enabled
                   AND root.is_enabled
            )
            "#,
        )
        .bind(target.installation_id)
        .bind(target.repo_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?;
        if !allowed {
            return Err(sbgh_core::Error::Unauthorized(format!(
                "Slack repository `{repository}` requires an active allowed installation, enabled supported lineage, and enabled target and source policies"
            )));
        }
        Ok(target)
    }
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
    use sbgh_core::db::{InstallationStore, NewInstallation, RepoStore};
    use sbgh_core::models::GithubAccountType;
    use sbgh_postgres::test_support::setup_pg_db;
    use sbgh_postgres::{PostgresInstallationStore, PostgresPolicyStore, PostgresRepoStore};

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

    #[tokio::test]
    async fn explicit_repository_requires_current_installation_and_policies() {
        let (_db, pool) = setup_pg_db().await;
        let installations = PostgresInstallationStore::new(pool.clone());
        let repos = PostgresRepoStore::new(pool.clone());
        let policies = PostgresPolicyStore::new(pool.clone());
        installations
            .seed_allowed(1, "octo", GithubAccountType::Organization, true)
            .await;
        installations
            .upsert_installation(&NewInstallation {
                id: 2,
                github_account_id: 1,
                account_login: "octo".into(),
                account_type: GithubAccountType::Organization,
            })
            .await
            .unwrap();
        repos
            .seed_supported_root(3, "octo", "core", true)
            .await;
        installations
            .add_or_restore_membership(2, 3)
            .await
            .unwrap();
        policies
            .seed_target(2, 3, true)
            .await;
        policies
            .seed_source(2, 3, true)
            .await;

        let resolver = PostgresSlackRepositoryResolver::new(pool);
        let target = resolver
            .resolve("octo/core")
            .await
            .unwrap();
        assert_eq!(target.installation_id, 2);
        assert_eq!(target.repo_id, 3);

        policies
            .seed_source(2, 3, false)
            .await;
        assert!(
            resolver
                .resolve("octo/core")
                .await
                .is_err()
        );
        policies
            .seed_source(2, 3, true)
            .await;
        policies
            .seed_target(2, 3, false)
            .await;
        assert!(
            resolver
                .resolve("octo/core")
                .await
                .is_err()
        );
        policies
            .seed_target(2, 3, true)
            .await;
        repos
            .disable_supported_root(3)
            .await
            .unwrap();
        assert!(
            resolver
                .resolve("octo/core")
                .await
                .is_err()
        );
        repos
            .upsert_supported_root(3, None)
            .await
            .unwrap();
        installations
            .revoke_membership(2, 3)
            .await
            .unwrap();
        assert!(
            resolver
                .resolve("octo/core")
                .await
                .is_err()
        );
        installations
            .add_or_restore_membership(2, 3)
            .await
            .unwrap();
        installations
            .seed_allowed(1, "octo", GithubAccountType::Organization, false)
            .await;
        assert!(
            resolver
                .resolve("octo/core")
                .await
                .is_err()
        );
    }
}
