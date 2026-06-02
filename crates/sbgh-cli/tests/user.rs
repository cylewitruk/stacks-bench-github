//! Slice 6 integration tests for `sbgh-cli user ...` admin commands.
//!
//! Coverage split mirrors `installer.rs`:
//! - SQL paths (`grant_role_by_user_id`, `revoke_role_by_user_id`,
//!   `list_roles`, `list_users`) run directly against Postgres.
//! - Login-resolution paths (`grant_role`, `revoke_role`) use an in-process
//!   axum mock of `/users/{login}` — identical pattern to the slice 3 installer
//!   tests so the rename-resilient lookup is covered without hitting
//!   api.github.com.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::Json;
use axum::extract::{Path as AxumPath, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use sbgh_cli::{
    UserError, grant_role, grant_role_by_user_id, list_roles, list_users, revoke_role,
    revoke_role_by_user_id,
};
use sbgh_core::db::{Pool, setup_pg_db};
use sbgh_core::models::UserRole;
use tokio::sync::oneshot;

/// Seed install + repo identity AND an active membership row. The
/// membership is what slice 6's `grant_role_by_user_id` pre-check
/// requires for repo-scoped grants (post-review M1 fix). Tests that
/// want to exercise the precheck-failure path skip this and use
/// `seed_install_only` instead.
async fn seed_install_repo(pool: &Pool, install_id: i64, repo_id: i64) {
    seed_install_only(pool, install_id).await;
    sqlx::query(
        "INSERT INTO github_repo (id, owner, name) VALUES ($1, 'o', $2) ON CONFLICT DO NOTHING",
    )
    .bind(repo_id)
    .bind(format!("r{repo_id}"))
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO github_installation_repo (github_installation_id, github_repo_id) VALUES \
         ($1, $2) ON CONFLICT DO NOTHING",
    )
    .bind(install_id)
    .bind(repo_id)
    .execute(pool)
    .await
    .unwrap();
}

/// Seed install + identity only (no membership). For tests that
/// exercise the M1 precheck-failure path.
async fn seed_install_only(pool: &Pool, install_id: i64) {
    sqlx::query(
        "INSERT INTO allowed_installer (github_account_id, account_login, account_type) VALUES \
         ($1, 'octo', 'organization') ON CONFLICT DO NOTHING",
    )
    .bind(install_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO github_installation (id, github_account_id, account_login, account_type) \
         VALUES ($1, $1, 'octo', 'organization') ON CONFLICT DO NOTHING",
    )
    .bind(install_id)
    .execute(pool)
    .await
    .unwrap();
}

async fn seed_user(pool: &Pool, user_id: i64, login: &str) {
    sqlx::query(
        "INSERT INTO github_user (id, login, user_type) VALUES ($1, $2, 'user') ON CONFLICT DO \
         NOTHING",
    )
    .bind(user_id)
    .bind(login)
    .execute(pool)
    .await
    .unwrap();
}

// ─── Mock GH /users/{login} ────────────────────────────────────────────

async fn start_mock_gh(accounts: HashMap<String, (i64, String)>) -> (String, oneshot::Sender<()>) {
    let state = Arc::new(accounts);
    let app = axum::Router::new()
        .route("/users/{login}", get(get_user))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        let server = axum::serve(listener, app).with_graceful_shutdown(async move {
            let _ = rx.await;
        });
        let _ = server.await;
    });
    (format!("http://{addr}"), tx)
}

async fn get_user(
    State(accounts): State<Arc<HashMap<String, (i64, String)>>>,
    AxumPath(login): AxumPath<String>,
) -> impl IntoResponse {
    match accounts.get(&login) {
        Some((id, kind)) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "id": id,
                "login": login,
                "type": kind,
            })),
        )
            .into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

// ─── SQL-path tests ────────────────────────────────────────────────────

#[tokio::test]
async fn grant_then_revoke_round_trip_by_user_id() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    seed_user(&pool, 42, "alice").await;

    let granted = grant_role_by_user_id(&pool, 42, 100, Some(10), UserRole::TriggerPrBenchmark)
        .await
        .unwrap();
    assert!(granted.created);
    assert_eq!(granted.role.github_user_id, 42);

    let revoked = revoke_role_by_user_id(&pool, 42, 100, Some(10), UserRole::TriggerPrBenchmark)
        .await
        .unwrap();
    assert_eq!(revoked.id, granted.role.id);
}

#[tokio::test]
async fn grant_role_by_user_id_rejects_unknown_user() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    // No github_user row seeded.

    let err = grant_role_by_user_id(&pool, 999, 100, Some(10), UserRole::TriggerPrBenchmark)
        .await
        .unwrap_err();
    assert!(matches!(err, UserError::UnknownUser(999)));
}

#[tokio::test]
async fn revoke_role_returns_grant_not_found_for_unmatched_quadruple() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    seed_user(&pool, 42, "alice").await;

    let err = revoke_role_by_user_id(&pool, 42, 100, Some(10), UserRole::TriggerPrBenchmark)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        UserError::GrantNotFound {
            user_id: 42,
            install_id: 100,
            ..
        }
    ));
}

#[tokio::test]
async fn list_roles_filters_and_list_users_lists_everyone() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    seed_install_repo(&pool, 200, 10).await;
    seed_user(&pool, 42, "alice").await;
    seed_user(&pool, 43, "bob").await;
    grant_role_by_user_id(&pool, 42, 100, None, UserRole::TriggerPrBenchmark)
        .await
        .unwrap();
    grant_role_by_user_id(&pool, 43, 200, None, UserRole::Admin)
        .await
        .unwrap();

    let install_100 = list_roles(&pool, Some(100))
        .await
        .unwrap();
    assert_eq!(install_100.len(), 1);
    assert_eq!(install_100[0].github_user_id, 42);

    let all_users = list_users(&pool)
        .await
        .unwrap();
    assert_eq!(all_users.len(), 2);
}

// ─── Post-review M1: membership precheck ──────────────────────────────

#[tokio::test]
async fn grant_role_by_user_id_rejects_repo_scoped_grant_without_membership() {
    // Post-review M1 fix: a repo-scoped grant requires an active
    // github_installation_repo membership for (install, repo).
    // Without this precheck, a typo would silently create a stale
    // grant that becomes active if the repo is later added to the
    // install.
    let (_db, pool) = setup_pg_db().await;
    seed_install_only(&pool, 100).await;
    seed_user(&pool, 42, "alice").await;
    // Seed a github_repo row but NO membership.
    sqlx::query("INSERT INTO github_repo (id, owner, name) VALUES (10, 'o', 'r10')")
        .execute(&pool)
        .await
        .unwrap();

    let err = grant_role_by_user_id(&pool, 42, 100, Some(10), UserRole::TriggerPrBenchmark)
        .await
        .unwrap_err();
    assert!(
        matches!(err, UserError::NoActiveMembership { install_id: 100, repo_id: 10 }),
        "got: {err:?}"
    );
}

#[tokio::test]
async fn grant_role_by_user_id_rejects_repo_scoped_grant_for_revoked_membership() {
    // Same precheck must catch the case where the membership row
    // exists but has been soft-revoked.
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    seed_user(&pool, 42, "alice").await;
    // Revoke the membership.
    sqlx::query(
        "UPDATE github_installation_repo SET revoked_at = NOW() WHERE github_installation_id = \
         100 AND github_repo_id = 10",
    )
    .execute(&pool)
    .await
    .unwrap();

    let err = grant_role_by_user_id(&pool, 42, 100, Some(10), UserRole::TriggerPrBenchmark)
        .await
        .unwrap_err();
    assert!(matches!(err, UserError::NoActiveMembership { install_id: 100, repo_id: 10 }));
}

#[tokio::test]
async fn grant_role_install_wide_does_not_require_membership() {
    // Install-wide grants apply to whichever repos the install does
    // or will have access to — the precheck deliberately skips
    // membership when repo_id is None.
    let (_db, pool) = setup_pg_db().await;
    seed_install_only(&pool, 100).await;
    seed_user(&pool, 42, "alice").await;
    // No memberships at all on install=100.

    let outcome = grant_role_by_user_id(&pool, 42, 100, None, UserRole::TriggerPrBenchmark)
        .await
        .unwrap();
    assert!(outcome.created, "install-wide grant must succeed without any membership");
}

// ─── HTTP-path tests via in-process axum mock ──────────────────────────

#[tokio::test]
async fn grant_role_resolves_login_then_upserts_user_then_grants() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;

    let mut accounts = HashMap::new();
    accounts.insert("alice".to_string(), (42_i64, "User".to_string()));
    let (api_base, _shutdown) = start_mock_gh(accounts).await;

    // Note: no `github_user` row pre-seeded — grant_role MUST upsert
    // it via the resolve_account result.
    let outcome =
        grant_role(&pool, &api_base, "alice", 100, Some(10), UserRole::TriggerPrBenchmark)
            .await
            .unwrap();
    assert!(outcome.created);
    assert_eq!(outcome.role.github_user_id, 42);

    // Verify the user row materialised.
    let users = list_users(&pool)
        .await
        .unwrap();
    assert_eq!(users.len(), 1);
    assert_eq!(users[0].login, "alice");
}

#[tokio::test]
async fn grant_role_returns_account_not_found_for_unknown_login() {
    let (_db, pool) = setup_pg_db().await;

    let (api_base, _shutdown) = start_mock_gh(HashMap::new()).await;

    let err = grant_role(&pool, &api_base, "ghost", 100, None, UserRole::TriggerPrBenchmark)
        .await
        .unwrap_err();
    assert!(matches!(err, UserError::AccountNotFound(s) if s == "ghost"));
}

#[tokio::test]
async fn revoke_role_resolves_login_and_targets_resolved_id() {
    // Rename-resilience: even if a stale `github_user` row shares the
    // login with a different numeric id, revoke targets the GH-resolved
    // id — not whichever row happens to share the display login.
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;

    // Stale row: id=999, login="alice"
    seed_user(&pool, 999, "alice").await;
    grant_role_by_user_id(&pool, 999, 100, Some(10), UserRole::TriggerPrBenchmark)
        .await
        .unwrap();
    // Current row: id=42, login="alice" (different person, same display)
    seed_user(&pool, 42, "alice-current").await;
    grant_role_by_user_id(&pool, 42, 100, Some(10), UserRole::TriggerPrBenchmark)
        .await
        .unwrap();

    let mut accounts = HashMap::new();
    accounts.insert("alice".to_string(), (42_i64, "User".to_string()));
    let (api_base, _shutdown) = start_mock_gh(accounts).await;

    // Revoke "alice" — GH resolves to id=42, so the stale id=999 grant
    // MUST survive ACTIVE. The id=42 grant goes to revoked (soft-revoke
    // post-review fix: both rows remain in the table for audit).
    revoke_role(&pool, &api_base, "alice", 100, Some(10), UserRole::TriggerPrBenchmark)
        .await
        .unwrap();

    let all = list_roles(&pool, Some(100))
        .await
        .unwrap();
    assert_eq!(all.len(), 2, "both rows must remain (audit) — only revoked_at differs");
    let active: Vec<_> = all
        .iter()
        .filter(|r| r.revoked_at.is_none())
        .collect();
    let revoked: Vec<_> = all
        .iter()
        .filter(|r| r.revoked_at.is_some())
        .collect();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].github_user_id, 999, "stale id grant must remain ACTIVE");
    assert_eq!(revoked.len(), 1);
    assert_eq!(revoked[0].github_user_id, 42, "resolved id grant must be the revoked one");
}
