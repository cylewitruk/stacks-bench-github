//! The daemon's `/api` server (roadmap-v3 Phase 2).
//!
//! Phase 2 stands up the skeleton: a public `GET /api/health`, the
//! bearer-token auth layer (`ingest` / `read` / `admin` scopes — see
//! [`auth`]), the operator cookie bootstrap, and a multi-listener serve.
//! The business endpoints (webhook submit, listings, admin) attach to the
//! scope-gated groups in Phase 3.

mod auth;
mod conv;
mod error;
mod extract;
mod fleet;
mod health;
mod installations;
mod installers;
mod jobs;
mod policies;
mod repos;
mod resolve;
mod state;
mod users;
mod webhooks;

#[cfg(test)]
mod endpoint_tests;

use std::future::IntoFuture;
use std::io::Write;
use std::net::SocketAddr;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
pub use auth::{ApiTokens, Scope};
use axum::Router;
use axum::routing::{get, post};
pub use state::ApiState;
use tower_http::trace::TraceLayer;
use uuid::Uuid;

/// Build the `/api` router. `/api/health` is public; the rest are gated by
/// scope-grouped sub-routers (`ingest` / `read`, plus `admin` in Phase 3b).
/// `/api/webhooks` carries POST (ingest) and GET (read) under different
/// scopes — the merge combines the two method routers, each keeping its
/// own auth layer.
pub fn build_router(state: ApiState, tokens: Arc<ApiTokens>) -> Router {
    // `ingest` = handler webhook submit. `read` = all GET listings. `admin`
    // = all writes. Paths shared across methods (e.g. GET vs POST
    // `/api/installers`) live in different groups; the merge combines the
    // method routers, each keeping its own auth layer.
    let ingest = auth::protect(
        Router::new().route("/api/webhooks", post(webhooks::submit)),
        tokens.clone(),
        Scope::Ingest,
    );
    let read = auth::protect(
        Router::new()
            .route("/api/whoami", get(health::whoami))
            .route("/api/webhooks", get(webhooks::list))
            .route("/api/installations", get(installations::list))
            .route("/api/resolve", get(resolve::resolve))
            .route("/api/installers", get(installers::list))
            .route("/api/repos", get(repos::list))
            .route("/api/policies/target", get(policies::list_target))
            .route("/api/policies/source", get(policies::list_source))
            .route("/api/policies/triggers", get(policies::list_triggers))
            .route("/api/users", get(users::list_users))
            .route("/api/roles", get(users::list_roles))
            .route("/api/jobs", get(jobs::list))
            .route("/api/fleet", get(fleet::overview))
            .route("/api/fleet/metrics", get(fleet::metrics)),
        tokens.clone(),
        Scope::Read,
    );
    let admin = auth::protect(
        Router::new()
            .route("/api/installers", post(installers::allow))
            .route("/api/installers/disable", post(installers::disable))
            .route("/api/repos", post(repos::allow))
            .route("/api/repos/disable", post(repos::disable))
            .route("/api/policies/target", post(policies::allow_target))
            .route("/api/policies/target/disable", post(policies::disable_target))
            .route("/api/policies/source", post(policies::allow_source))
            .route("/api/policies/source/disable", post(policies::disable_source))
            .route("/api/policies/triggers", post(policies::add_trigger))
            .route("/api/policies/triggers/{id}/disable", post(policies::disable_trigger))
            .route("/api/policies/triggers/{id}/pin", post(policies::pin_trigger))
            .route("/api/roles", post(users::grant))
            .route("/api/roles/revoke", post(users::revoke))
            .route("/api/jobs/block-validation", post(jobs::enqueue_block_validation))
            .route("/api/fleet/workers/{id}/drain", post(fleet::set_drain))
            .route("/api/fleet/jobs/{id}/cancel", post(fleet::cancel_job))
            .route("/api/fleet/groups/{id}/recover", post(fleet::recover_group)),
        tokens,
        Scope::Admin,
    );
    Router::new()
        .route("/api/health", get(health::health))
        .merge(ingest)
        .merge(read)
        .merge(admin)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

/// Generate a fresh operator (`admin`) token, write it to `path` mode
/// 0600, and return it. The local CLI reads the same file to authenticate
/// without a stored secret (the `bitcoind` cookie model). Regenerated each
/// boot, so a stale cookie can't outlive a restart.
pub fn bootstrap_cookie(path: &Path) -> anyhow::Result<String> {
    // Two v4 UUIDs → 256 bits of randomness as 64 hex chars.
    let token = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating cookie dir {}", parent.display()))?;
        }
    }
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
        .with_context(|| format!("opening cookie file {}", path.display()))?;
    // `.mode(0o600)` only applies when the file is *created*. Tighten an
    // already-existing file BEFORE writing the secret into it, so a
    // pre-existing world-readable cookie can never hold the token.
    file.set_permissions(std::fs::Permissions::from_mode(0o600))
        .with_context(|| format!("setting cookie file mode {}", path.display()))?;
    file.write_all(token.as_bytes())
        .with_context(|| format!("writing cookie file {}", path.display()))?;
    Ok(token)
}

/// Bind every configured listener and serve `router` on each concurrently.
/// Binds eagerly so a bad address / in-use port fails at startup. Returns
/// when any listener errors (which crashes the daemon → systemd
/// restart, same as the runner/processor arms).
pub async fn serve(
    listen: &[String],
    router: Router,
    shutdown: tokio_util::sync::CancellationToken,
) -> anyhow::Result<()> {
    if listen.is_empty() {
        anyhow::bail!("[api].listen is empty — nothing to bind");
    }
    let mut serves = Vec::with_capacity(listen.len());
    for addr in listen {
        let socket: SocketAddr = addr
            .parse()
            .with_context(|| format!("parsing [api].listen entry {addr:?}"))?;
        let listener = tokio::net::TcpListener::bind(socket)
            .await
            .with_context(|| format!("binding api listener on {socket}"))?;
        tracing::info!(%socket, "api listening");
        // Stop accepting + finish in-flight requests once the daemon exits.
        let token = shutdown.clone();
        serves.push(
            axum::serve(listener, router.clone())
                .with_graceful_shutdown(async move { token.cancelled().await })
                .into_future(),
        );
    }
    futures::future::try_join_all(serves).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use super::*;

    /// An `ApiState` over a *lazy* pool — never connects, so it's fine for
    /// the stateless routes (health/whoami) that don't query.
    fn test_state() -> ApiState {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://test:test@localhost/test")
            .unwrap();
        ApiState {
            pool: pool.clone(),
            ingest: Arc::new(sbgh_postgres::PostgresIngestStore::new(pool)),
            gh_api_base: "https://api.github.com".into(),
        }
    }

    #[tokio::test]
    async fn health_is_public_and_ok() {
        let tokens = Arc::new(ApiTokens::new("admin".into(), None, None).unwrap());
        let resp = build_router(test_state(), tokens)
            .oneshot(
                Request::builder()
                    .uri("/api/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes();
        let parsed: sbgh_api::HealthResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed.status, "ok");
    }

    #[tokio::test]
    async fn whoami_requires_auth_and_echoes_scope() {
        let tokens = Arc::new(ApiTokens::new("admintok".into(), None, None).unwrap());

        // No token → 401.
        let unauth = build_router(test_state(), tokens.clone())
            .oneshot(
                Request::builder()
                    .uri("/api/whoami")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unauth.status(), StatusCode::UNAUTHORIZED);

        // Admin token (satisfies read) → 200, scope echoed.
        let resp = build_router(test_state(), tokens)
            .oneshot(
                Request::builder()
                    .uri("/api/whoami")
                    .header("authorization", "Bearer admintok")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes();
        let parsed: sbgh_api::WhoamiResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed.scope, "admin");
    }

    #[test]
    fn bootstrap_cookie_writes_0600_and_roundtrips() {
        let dir = tempfile::tempdir().unwrap();
        // Nested path exercises the create_dir_all branch.
        let path = dir
            .path()
            .join("daemon/.cookie");
        let token = bootstrap_cookie(&path).unwrap();
        assert_eq!(token.len(), 64);
        assert_eq!(sbgh_api::read_cookie(&path).unwrap(), token);
        assert_eq!(mode_of(&path), 0o600);
    }

    #[test]
    fn bootstrap_cookie_tightens_preexisting_world_readable_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".cookie");
        // Pre-create a world-readable cookie, mimicking a file left behind
        // with broader permissions.
        std::fs::write(&path, "stale").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert_eq!(mode_of(&path), 0o644);

        let token = bootstrap_cookie(&path).unwrap();
        assert_eq!(mode_of(&path), 0o600, "pre-existing file must be tightened to 0600");
        assert_eq!(sbgh_api::read_cookie(&path).unwrap(), token);
    }

    fn mode_of(path: &std::path::Path) -> u32 {
        std::fs::metadata(path)
            .unwrap()
            .permissions()
            .mode()
            & 0o777
    }
}
