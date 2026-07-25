//! Slice 3 integration tests for the `sbgh-cli installer ...` admin
//! commands.
//!
//! Coverage split:
//! - SQL paths (`disable_installer_by_account_id`, `list_installers`) run
//!   against a real Postgres without HTTP.
//! - The full `allow`/`disable`-by-login path is exercised via a tiny
//!   in-process axum mock that impersonates `/users/{login}` — this is the only
//!   way to cover the codex-flagged "resolve login → id then disable by id"
//!   semantic end-to-end without hitting api.github.com.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::Json;
use axum::extract::{Path as AxumPath, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use sbgh_postgres::admin::{
    InstallerError, allow_installer, disable_installer, disable_installer_by_account_id,
    list_installers,
};
use sbgh_postgres::db::{Pool, setup_pg_db};
use tokio::sync::oneshot;

async fn seed_owner_row(pool: &Pool, account_id: i64, login: &str, is_enabled: bool) {
    sqlx::query(
        "INSERT INTO allowed_installer (github_account_id, account_login, account_type, \
         is_enabled, note) VALUES ($1, $2, 'organization', $3, 'seeded by test')",
    )
    .bind(account_id)
    .bind(login)
    .bind(is_enabled)
    .execute(pool)
    .await
    .unwrap();
}

// ─── Mock GH /users/{login} server ─────────────────────────────────────

/// Bind a minimal HTTP server that responds to `GET /users/{login}` like
/// the GitHub REST endpoint does. Returns `(api_base_url, shutdown)`;
/// dropping the shutdown sender ends the server's task.
async fn start_mock_gh(
    accounts: HashMap<String, (i64, String)>, // login → (id, "User"|"Organization")
) -> (String, oneshot::Sender<()>) {
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

// ─── SQL paths (no HTTP) ───────────────────────────────────────────────

#[tokio::test]
async fn list_installers_returns_seeded_rows_sorted_by_login() {
    let (_db, pool) = setup_pg_db().await;
    seed_owner_row(&pool, 1, "zorro", true).await;
    seed_owner_row(&pool, 2, "alice", true).await;
    seed_owner_row(&pool, 3, "marvin", false).await;

    let rows = list_installers(&pool)
        .await
        .unwrap();
    let logins: Vec<&str> = rows
        .iter()
        .map(|r| r.account_login.as_str())
        .collect();
    assert_eq!(logins, vec!["alice", "marvin", "zorro"]);
    let marvin = rows
        .iter()
        .find(|r| r.account_login == "marvin")
        .unwrap();
    assert!(!marvin.is_enabled);
}

#[tokio::test]
async fn disable_by_account_id_flips_is_enabled_to_false() {
    let (_db, pool) = setup_pg_db().await;
    seed_owner_row(&pool, 42, "octo", true).await;

    let row = disable_installer_by_account_id(&pool, 42)
        .await
        .unwrap();
    assert!(!row.is_enabled);
}

#[tokio::test]
async fn disable_by_account_id_returns_not_on_allowlist_for_unknown() {
    let (_db, pool) = setup_pg_db().await;

    let err = disable_installer_by_account_id(&pool, 99)
        .await
        .unwrap_err();
    assert!(matches!(err, InstallerError::NotOnAllowlist(99)));
}

#[tokio::test]
async fn disable_by_account_id_is_idempotent() {
    // Re-disabling an already-disabled row succeeds; operators may run
    // the command twice and we shouldn't flag a non-issue.
    let (_db, pool) = setup_pg_db().await;
    seed_owner_row(&pool, 42, "octo", false).await;

    let row = disable_installer_by_account_id(&pool, 42)
        .await
        .unwrap();
    assert!(!row.is_enabled);
}

// ─── login → id resolution path (via mock /users/{login}) ──────────────

#[tokio::test]
async fn allow_installer_resolves_login_and_upserts_row() {
    let (_db, pool) = setup_pg_db().await;
    let (api_base, _shutdown) =
        start_mock_gh(HashMap::from([("octo-org".to_string(), (42, "Organization".to_string()))]))
            .await;

    let row = allow_installer(&pool, &api_base, "octo-org", Some("ops note"))
        .await
        .unwrap();
    assert_eq!(row.github_account_id, 42);
    assert_eq!(row.account_login, "octo-org");
    assert!(row.is_enabled);
    assert_eq!(row.note.as_deref(), Some("ops note"));
}

#[tokio::test]
async fn allow_installer_returns_account_not_found_when_gh_404s() {
    let (_db, pool) = setup_pg_db().await;
    let (api_base, _shutdown) = start_mock_gh(HashMap::new()).await;

    let err = allow_installer(&pool, &api_base, "ghost", None)
        .await
        .unwrap_err();
    assert!(matches!(err, InstallerError::AccountNotFound(login) if login == "ghost"));
}

#[tokio::test]
async fn disable_installer_resolves_login_then_disables_by_id() {
    // Codex's medium finding fix: even though the operator typed a
    // display login, the SQL UPDATE must target the numeric id we
    // resolved from GitHub (not the stale login potentially attached to
    // an old allowlist row).
    let (_db, pool) = setup_pg_db().await;
    // Seed the row keyed by the same numeric id GH will resolve to.
    seed_owner_row(&pool, 42, "octo-org", true).await;
    let (api_base, _shutdown) =
        start_mock_gh(HashMap::from([("octo-org".to_string(), (42, "Organization".to_string()))]))
            .await;

    let row = disable_installer(&pool, &api_base, "octo-org")
        .await
        .unwrap();
    assert!(!row.is_enabled);
}

#[tokio::test]
async fn disable_installer_targets_resolved_id_even_after_login_collision() {
    // Concretely demonstrates why login-keyed disable was wrong:
    // - Old row: github_account_id=42, login="octo-org" (still in the DB from a
    //   previous allow but never refreshed since the account was renamed).
    // - GH currently maps "octo-org" → id 99 (the account that recycled the login).
    // The disable MUST target id 99 (the current account the operator
    // means) and leave id 42 alone.
    let (_db, pool) = setup_pg_db().await;
    // Two rows, both with login `octo-org` due to the stale display name.
    seed_owner_row(&pool, 42, "octo-org", true).await;
    seed_owner_row(&pool, 99, "octo-org", true).await;
    let (api_base, _shutdown) =
        start_mock_gh(HashMap::from([("octo-org".to_string(), (99, "Organization".to_string()))]))
            .await;

    disable_installer(&pool, &api_base, "octo-org")
        .await
        .unwrap();

    let id_42_enabled: bool =
        sqlx::query_scalar("SELECT is_enabled FROM allowed_installer WHERE github_account_id = 42")
            .fetch_one(&pool)
            .await
            .unwrap();
    let id_99_enabled: bool =
        sqlx::query_scalar("SELECT is_enabled FROM allowed_installer WHERE github_account_id = 99")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(id_42_enabled, "old row (stale login) must NOT be disabled");
    assert!(!id_99_enabled, "current row (resolved id) must BE disabled");
}

#[tokio::test]
async fn disable_installer_for_resolved_id_not_in_allowlist_errors() {
    let (_db, pool) = setup_pg_db().await;
    // GH knows the login but the operator never allowed this account.
    let (api_base, _shutdown) =
        start_mock_gh(HashMap::from([("octo-org".to_string(), (42, "Organization".to_string()))]))
            .await;

    let err = disable_installer(&pool, &api_base, "octo-org")
        .await
        .unwrap_err();
    assert!(matches!(err, InstallerError::NotOnAllowlist(42)));
}
