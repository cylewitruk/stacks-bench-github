//! Integration test: `[slack].default_repository` ("owner/name") resolves to
//! its `(installation, repo)` FK ids against real Postgres (item 0002, v5
//! wiring). Proves the happy path + each actionable misconfiguration error.

use sbgh_core::db::{
    InstallationStore, NewInstallation, Pool, PostgresInstallationStore, setup_pg_db,
};
use sbgh_core::models::GithubAccountType;

// Daemon is a bin-only crate; pull in the unit under test via path include.
// `target.rs` depends only on `sbgh_core::db::Pool` + `sqlx`, so no other
// daemon modules are needed here.
#[path = "../src/slack/target.rs"]
mod target;

use target::{ResolveTargetError, resolve_target};

/// Seed an installation (by `account_login`) and, optionally, a `github_repo`
/// row for `owner/name`. `suspended` marks the install suspended.
async fn seed(
    pool: &Pool,
    install_id: i64,
    account_login: &str,
    suspended: bool,
    repo: Option<(i64, &str, &str)>,
) {
    sqlx::query(
        "INSERT INTO allowed_installer (github_account_id, account_login, account_type) VALUES \
         ($1, $2, 'organization') ON CONFLICT DO NOTHING",
    )
    .bind(install_id)
    .bind(account_login)
    .execute(pool)
    .await
    .unwrap();

    PostgresInstallationStore::new(pool.clone())
        .upsert_installation(&NewInstallation {
            id: install_id,
            github_account_id: install_id,
            account_login: account_login.into(),
            account_type: GithubAccountType::Organization,
        })
        .await
        .unwrap();

    if suspended {
        sqlx::query("UPDATE github_installation SET suspended_at = NOW() WHERE id = $1")
            .bind(install_id)
            .execute(pool)
            .await
            .unwrap();
    }

    if let Some((repo_id, owner, name)) = repo {
        sqlx::query("INSERT INTO github_repo (id, owner, name) VALUES ($1, $2, $3)")
            .bind(repo_id)
            .bind(owner)
            .bind(name)
            .execute(pool)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn resolves_owner_name_to_ids() {
    let (_db, pool) = setup_pg_db().await;
    seed(&pool, 100, "octo", false, Some((10, "octo", "core"))).await;

    let target = resolve_target(&pool, "octo/core")
        .await
        .expect("a known repo resolves");
    assert_eq!(target.installation_id, 100);
    assert_eq!(target.repo_id, 10);
}

/// GitHub identifiers are case-insensitive: a differently-cased config value
/// resolves the same row.
#[tokio::test]
async fn resolution_is_case_insensitive() {
    let (_db, pool) = setup_pg_db().await;
    seed(&pool, 101, "Octo", false, Some((11, "Octo", "Core"))).await;

    let target = resolve_target(&pool, "octo/core")
        .await
        .expect("case-insensitive match");
    assert_eq!(target.installation_id, 101);
    assert_eq!(target.repo_id, 11);
}

#[tokio::test]
async fn unknown_account_is_installation_not_found() {
    let (_db, pool) = setup_pg_db().await;
    // No install seeded for `ghost`.
    let err = resolve_target(&pool, "ghost/core")
        .await
        .unwrap_err();
    assert!(matches!(err, ResolveTargetError::InstallationNotFound(o) if o == "ghost"));
}

#[tokio::test]
async fn suspended_install_is_rejected() {
    let (_db, pool) = setup_pg_db().await;
    seed(&pool, 102, "octo", true, Some((12, "octo", "core"))).await;

    let err = resolve_target(&pool, "octo/core")
        .await
        .unwrap_err();
    assert!(matches!(err, ResolveTargetError::InstallationSuspended(o) if o == "octo"));
}

#[tokio::test]
async fn unknown_repo_is_repo_not_found() {
    let (_db, pool) = setup_pg_db().await;
    // Install exists, but the repo was never materialised.
    seed(&pool, 103, "octo", false, None).await;

    let err = resolve_target(&pool, "octo/core")
        .await
        .unwrap_err();
    assert!(matches!(err, ResolveTargetError::RepoNotFound(s) if s == "octo/core"));
}

#[tokio::test]
async fn malformed_repository_is_rejected_before_any_query() {
    let (_db, pool) = setup_pg_db().await;
    let err = resolve_target(&pool, "no-slash")
        .await
        .unwrap_err();
    assert!(matches!(err, ResolveTargetError::MalformedRepository(_)));
}
