use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use sbgh_daemon::api::{ApiState, ApiTokens, build_router};
use sbgh_postgres::db::PostgresIngestStore;
use tower::ServiceExt;

#[tokio::test]
async fn production_api_router_exposes_public_health() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://unused:unused@localhost/unused")
        .unwrap();
    let state = ApiState {
        ingest: Arc::new(PostgresIngestStore::new(pool.clone())),
        pool,
        gh_api_base: "https://api.github.invalid".into(),
        block_validation: None,
    };
    let tokens = Arc::new(ApiTokens::new("admin".into(), None, None).unwrap());
    let response = build_router(state, tokens)
        .oneshot(
            Request::builder()
                .uri("/api/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}
