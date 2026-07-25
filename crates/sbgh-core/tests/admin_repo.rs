//! Slice 4 integration tests for `sbgh-cli repo ...`. Pure-SQL paths
//! (`disable_repo_root_by_id`, `list_repo_roots`) run against a real
//! Postgres. The login-resolution paths (`allow_repo_root`,
//! `disable_repo_root` by owner/name) are covered via an in-process
//! axum mock of `/repos/{owner}/{repo}`.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::Json;
use axum::extract::{Path as AxumPath, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use sbgh_core::admin::{
    RepoError, allow_repo_root, disable_repo_root, disable_repo_root_by_id, list_repo_roots,
};
use sbgh_core::db::{Pool, setup_pg_db};
use tokio::sync::oneshot;

async fn seed_supported_row(pool: &Pool, repo_id: i64, owner: &str, name: &str, is_enabled: bool) {
    sqlx::query("INSERT INTO github_repo (id, owner, name) VALUES ($1, $2, $3)")
        .bind(repo_id)
        .bind(owner)
        .bind(name)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO supported_repo_root (github_repo_id, is_enabled) VALUES ($1, $2)")
        .bind(repo_id)
        .bind(is_enabled)
        .execute(pool)
        .await
        .unwrap();
}

// ─── Mock GH /repos/{owner}/{repo} server ──────────────────────────────

async fn start_mock_gh(
    repos: HashMap<(String, String), serde_json::Value>,
) -> (String, oneshot::Sender<()>) {
    let state = Arc::new(repos);
    let app = axum::Router::new()
        .route("/repos/{owner}/{name}", get(get_repo))
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

async fn get_repo(
    State(repos): State<Arc<HashMap<(String, String), serde_json::Value>>>,
    AxumPath((owner, name)): AxumPath<(String, String)>,
) -> impl IntoResponse {
    match repos.get(&(owner, name)) {
        Some(body) => (StatusCode::OK, Json(body.clone())).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

fn canonical_repo_body(id: i64, owner: &str, name: &str) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "name": name,
        "default_branch": "main",
        "owner": { "login": owner },
        "fork": false,
    })
}

// ─── pure-SQL paths ────────────────────────────────────────────────────

#[tokio::test]
async fn list_repo_roots_returns_seeded_rows_sorted_by_owner_name() {
    let (_db, pool) = setup_pg_db().await;
    seed_supported_row(&pool, 1, "zorro", "zrepo", true).await;
    seed_supported_row(&pool, 2, "alice", "arepo", true).await;
    seed_supported_row(&pool, 3, "alice", "brepo", false).await;

    let rows = list_repo_roots(&pool)
        .await
        .unwrap();
    let pairs: Vec<(String, String)> = rows
        .iter()
        .map(|r| (r.owner.clone(), r.name.clone()))
        .collect();
    assert_eq!(
        pairs,
        vec![
            ("alice".to_string(), "arepo".to_string()),
            ("alice".to_string(), "brepo".to_string()),
            ("zorro".to_string(), "zrepo".to_string()),
        ]
    );
    assert!(!rows[1].is_enabled, "soft-disabled row reported as is_enabled=false");
}

#[tokio::test]
async fn disable_repo_root_by_id_flips_is_enabled_to_false() {
    let (_db, pool) = setup_pg_db().await;
    seed_supported_row(&pool, 10, "stacks-network", "stacks-core", true).await;

    let row = disable_repo_root_by_id(&pool, 10)
        .await
        .unwrap();
    assert!(!row.is_enabled);
    assert_eq!(row.owner, "stacks-network");
}

#[tokio::test]
async fn disable_repo_root_by_id_returns_not_on_supported_list_for_unknown() {
    let (_db, pool) = setup_pg_db().await;
    let err = disable_repo_root_by_id(&pool, 999)
        .await
        .unwrap_err();
    assert!(matches!(err, RepoError::NotOnSupportedList(999)));
}

// ─── login-resolution paths (via mock /repos/{owner}/{repo}) ───────────

#[tokio::test]
async fn allow_repo_root_resolves_owner_name_and_upserts_identity_and_supported() {
    let (_db, pool) = setup_pg_db().await;
    let (api_base, _shutdown) = start_mock_gh(HashMap::from([(
        ("stacks-network".to_string(), "stacks-core".to_string()),
        canonical_repo_body(10, "stacks-network", "stacks-core"),
    )]))
    .await;

    let row = allow_repo_root(&pool, &api_base, "stacks-network", "stacks-core", Some("canonical"))
        .await
        .unwrap();
    assert_eq!(row.github_repo_id, 10);
    assert!(row.is_enabled);

    // Both rows must exist.
    let repo_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM github_repo WHERE id = 10")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(repo_count, 1);
    let supported_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM supported_repo_root WHERE github_repo_id = 10")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(supported_count, 1);
}

#[tokio::test]
async fn allow_repo_root_returns_repo_not_found_when_gh_404s() {
    let (_db, pool) = setup_pg_db().await;
    let (api_base, _shutdown) = start_mock_gh(HashMap::new()).await;

    let err = allow_repo_root(&pool, &api_base, "ghost", "repo", None)
        .await
        .unwrap_err();
    assert!(matches!(err, RepoError::RepoNotFound(s) if s == "ghost/repo"));
}

#[tokio::test]
async fn allow_repo_root_is_idempotent_on_redelivery() {
    // Re-running `allow` for an already-allowed repo refreshes
    // owner/name (in case of rename) and re-asserts is_enabled=TRUE,
    // without disturbing fork lineage columns.
    let (_db, pool) = setup_pg_db().await;
    let (api_base, _shutdown) = start_mock_gh(HashMap::from([(
        ("stacks-network".to_string(), "stacks-core".to_string()),
        canonical_repo_body(10, "stacks-network", "stacks-core"),
    )]))
    .await;

    allow_repo_root(&pool, &api_base, "stacks-network", "stacks-core", None)
        .await
        .unwrap();
    // Mark disabled, then re-allow → re-enables.
    disable_repo_root_by_id(&pool, 10)
        .await
        .unwrap();
    let row = allow_repo_root(&pool, &api_base, "stacks-network", "stacks-core", None)
        .await
        .unwrap();
    assert!(row.is_enabled, "second allow must re-enable a previously-disabled row");
}

#[tokio::test]
async fn disable_repo_root_resolves_then_disables_by_id() {
    // Codex-style invariant carried forward from slice 3: the SQL
    // UPDATE must target the numeric PK that the GH API resolves to,
    // not whichever row happens to share the typed owner/name.
    let (_db, pool) = setup_pg_db().await;
    seed_supported_row(&pool, 10, "stacks-network", "stacks-core", true).await;
    let (api_base, _shutdown) = start_mock_gh(HashMap::from([(
        ("stacks-network".to_string(), "stacks-core".to_string()),
        canonical_repo_body(10, "stacks-network", "stacks-core"),
    )]))
    .await;

    let row = disable_repo_root(&pool, &api_base, "stacks-network", "stacks-core")
        .await
        .unwrap();
    assert!(!row.is_enabled);
}
