//! `/api/webhooks` — write-through submit (replaces the handler's direct
//! INSERT) and inbox listing for operational visibility.

use axum::Json;
use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::HeaderMap;
use sbgh_api::{WebhookSubmitResponse, WebhookSummary};
use sbgh_postgres::db::{IngestOutcome, NewWebhook, SUPPORTED_WEBHOOK_EVENT_TYPES};
use serde::Deserialize;
use serde_json::Value;

use crate::api::error::ApiErr;
use crate::api::state::ApiState;

/// `github_webhook_status` enum values — used to validate the optional
/// `status` filter before binding it (so a bad value is a clean 400, not a
/// Postgres cast error).
const WEBHOOK_STATUSES: &[&str] =
    &["received", "processing", "processed", "ignored", "denied", "retryable_error", "failed"];

/// `POST /api/webhooks` (ingest scope). The handler forwards the raw
/// GitHub body + `X-GitHub-Event` / `X-GitHub-Delivery` headers after HMAC
/// verification; the daemon owns the event-type allowlist, extracts
/// `action` / `installation_id`, and write-through-persists with dedup.
pub async fn submit(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<WebhookSubmitResponse>, ApiErr> {
    // Event type first: an unsupported event is dropped without a row and
    // without needing a delivery id (DoS-aware, matching the edge filter).
    let event = header(&headers, "x-github-event")
        .ok_or_else(|| ApiErr::bad_request("missing X-GitHub-Event header"))?;
    if !SUPPORTED_WEBHOOK_EVENT_TYPES.contains(&event.as_str()) {
        tracing::debug!(event, "webhook submit: event type not on allowlist — ignored");
        return Ok(Json(WebhookSubmitResponse {
            result: "ignored".into(),
            id: None,
            reason: Some("unsupported_event_type".into()),
        }));
    }

    // Supported events must carry a non-empty delivery id (the inbox dedup
    // key). Reject missing OR blank — a blank id would otherwise be recorded
    // as `delivery_id = ''` and defeat dedup.
    let delivery = header(&headers, "x-github-delivery")
        .filter(|d| !d.trim().is_empty())
        .ok_or_else(|| ApiErr::bad_request("missing or empty X-GitHub-Delivery header"))?;

    // Parse once for the stored payload + derived fields. An unparseable
    // body stores NULL payload (the signature-verified bytes are still
    // forensically useful), matching the handler's prior behaviour.
    let payload = serde_json::from_slice::<Value>(&body).ok();
    let action = payload
        .as_ref()
        .and_then(|v| v.get("action"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let payload_installation_id = payload
        .as_ref()
        .and_then(|v| v.get("installation"))
        .and_then(|v| v.get("id"))
        .and_then(Value::as_i64);

    let webhook = NewWebhook {
        delivery_id: delivery.clone(),
        event_type: event.clone(),
        action: action.clone(),
        payload_installation_id,
        payload,
        payload_size_bytes: body.len() as i32,
    };

    match state
        .ingest
        .ingest_webhook(&webhook)
        .await?
    {
        IngestOutcome::Recorded { webhook_id } => {
            tracing::info!(
                webhook_id,
                delivery,
                event,
                action = action.as_deref().unwrap_or("-"),
                installation_id = ?payload_installation_id,
                "recorded webhook into inbox (via /api)"
            );
            Ok(Json(WebhookSubmitResponse {
                result: "recorded".into(),
                id: Some(webhook_id),
                reason: None,
            }))
        }
        IngestOutcome::Duplicate => {
            tracing::info!(delivery, event, "duplicate webhook delivery (via /api)");
            Ok(Json(WebhookSubmitResponse {
                result: "duplicate".into(),
                id: None,
                reason: None,
            }))
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ListParams {
    event_type: Option<String>,
    status: Option<String>,
    limit: Option<i64>,
}

/// `GET /api/webhooks` (read scope). Most-recent-first inbox rows, with
/// optional `event_type` / `status` filters and a `limit` (default 50,
/// max 500).
pub async fn list(
    State(state): State<ApiState>,
    Query(params): Query<ListParams>,
) -> Result<Json<Vec<WebhookSummary>>, ApiErr> {
    if let Some(s) = &params.status {
        if !WEBHOOK_STATUSES.contains(&s.as_str()) {
            return Err(ApiErr::bad_request(format!("unknown status {s:?}")));
        }
    }
    let limit = params
        .limit
        .unwrap_or(50)
        .clamp(1, 500);

    let rows = sqlx::query_as::<_, WebhookRow>(
        r#"
        SELECT id, delivery_id, event_type, action, payload_installation_id,
               status::text AS status, outcome::text AS outcome,
               attempts, received_at, processed_at
          FROM github_webhook
         WHERE ($1::text IS NULL OR event_type = $1)
           AND ($2::text IS NULL OR status = $2::github_webhook_status)
         ORDER BY received_at DESC
         LIMIT $3
        "#,
    )
    .bind(&params.event_type)
    .bind(&params.status)
    .bind(limit)
    .fetch_all(&state.pool)
    .await?;

    Ok(Json(
        rows.into_iter()
            .map(WebhookRow::into_summary)
            .collect(),
    ))
}

#[derive(sqlx::FromRow)]
struct WebhookRow {
    id: i64,
    delivery_id: String,
    event_type: String,
    action: Option<String>,
    payload_installation_id: Option<i64>,
    status: String,
    outcome: Option<String>,
    attempts: i32,
    received_at: chrono::DateTime<chrono::Utc>,
    processed_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl WebhookRow {
    fn into_summary(self) -> WebhookSummary {
        WebhookSummary {
            id: self.id,
            delivery_id: self.delivery_id,
            event_type: self.event_type,
            action: self.action,
            installation_id: self.payload_installation_id,
            status: self.status,
            outcome: self.outcome,
            attempts: self.attempts,
            received_at: self.received_at.to_rfc3339(),
            processed_at: self
                .processed_at
                .map(|t| t.to_rfc3339()),
        }
    }
}

fn header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)?
        .to_str()
        .ok()
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::Router;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::response::Response;
    use http_body_util::BodyExt;
    use sbgh_postgres::db::{Pool, PostgresIngestStore, setup_pg_db};
    use serde::de::DeserializeOwned;
    use tower::ServiceExt;

    use super::super::{ApiState, ApiTokens, build_router};

    fn router(pool: &Pool) -> Router {
        let tokens = Arc::new(
            ApiTokens::new("admintok".into(), Some("ingesttok".into()), Some("readtok".into()))
                .unwrap(),
        );
        let state = ApiState {
            pool: pool.clone(),
            ingest: Arc::new(PostgresIngestStore::new(pool.clone())),
            gh_api_base: "https://api.github.com".into(),
            worker_ca_certificate: "worker-ca.pem".into(),
        };
        build_router(state, tokens)
    }

    async fn submit(
        router: &Router,
        delivery: &str,
        event: &str,
        token: &str,
        body: &str,
    ) -> (StatusCode, serde_json::Value) {
        let req = Request::builder()
            .method("POST")
            .uri("/api/webhooks")
            .header("authorization", format!("Bearer {token}"))
            .header("x-github-event", event)
            .header("x-github-delivery", delivery)
            .body(Body::from(body.to_string()))
            .unwrap();
        let resp = router
            .clone()
            .oneshot(req)
            .await
            .unwrap();
        let status = resp.status();
        let json = json_body(resp).await;
        (status, json)
    }

    fn get(uri: &str, token: &str) -> Request<Body> {
        Request::builder()
            .uri(uri)
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap()
    }

    async fn json_body<T: DeserializeOwned>(resp: Response) -> T {
        let bytes = resp
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn submit_persists_and_dedupes() {
        let (_db, pool) = setup_pg_db().await;
        let router = router(&pool);
        let body = r#"{"action":"opened","installation":{"id":42}}"#;

        let (s1, j1) = submit(&router, "d-1", "pull_request", "ingesttok", body).await;
        assert_eq!(s1, StatusCode::OK);
        assert_eq!(j1["result"], "recorded");
        assert!(j1["id"].as_i64().unwrap() > 0);

        let (s2, j2) = submit(&router, "d-1", "pull_request", "ingesttok", body).await;
        assert_eq!(s2, StatusCode::OK);
        assert_eq!(j2["result"], "duplicate");

        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM github_webhook WHERE delivery_id='d-1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(count, 1);

        let (action, inst): (Option<String>, Option<i64>) = sqlx::query_as(
            "SELECT action, payload_installation_id FROM github_webhook WHERE delivery_id='d-1'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(action.as_deref(), Some("opened"));
        assert_eq!(inst, Some(42));
    }

    #[tokio::test]
    async fn submit_ignores_unsupported_event() {
        let (_db, pool) = setup_pg_db().await;
        let router = router(&pool);
        let (s, j) = submit(&router, "d-star", "star", "ingesttok", "{}").await;
        assert_eq!(s, StatusCode::OK);
        assert_eq!(j["result"], "ignored");
        assert_eq!(j["reason"], "unsupported_event_type");

        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM github_webhook WHERE delivery_id='d-star'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn list_returns_recent_with_filters() {
        let (_db, pool) = setup_pg_db().await;
        let router = router(&pool);
        submit(&router, "l-1", "push", "ingesttok", "{}").await;
        submit(&router, "l-2", "pull_request", "ingesttok", r#"{"installation":{"id":7}}"#).await;

        let all: Vec<sbgh_api::WebhookSummary> = json_body(
            router
                .clone()
                .oneshot(get("/api/webhooks", "readtok"))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(all.len(), 2);

        let pushes: Vec<sbgh_api::WebhookSummary> = json_body(
            router
                .clone()
                .oneshot(get("/api/webhooks?event_type=push", "readtok"))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(pushes.len(), 1);
        assert_eq!(pushes[0].event_type, "push");

        // Unknown status filter → 400.
        let bad = router
            .clone()
            .oneshot(get("/api/webhooks?status=bogus", "readtok"))
            .await
            .unwrap();
        assert_eq!(bad.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn webhook_routes_enforce_per_method_scope() {
        // Proves POST(ingest)/GET(read) on the same `/api/webhooks` path
        // keep their distinct auth after the merge.
        let (_db, pool) = setup_pg_db().await;
        let router = router(&pool);

        // POST with read token → 403 (needs ingest).
        let (post_read, _) = submit(&router, "a-1", "push", "readtok", "{}").await;
        assert_eq!(post_read, StatusCode::FORBIDDEN);

        // POST with no token → 401.
        let noauth = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/webhooks")
                    .header("x-github-event", "push")
                    .header("x-github-delivery", "a-2")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(noauth.status(), StatusCode::UNAUTHORIZED);

        // GET with ingest token → 403 (needs read).
        let get_ingest = router
            .clone()
            .oneshot(get("/api/webhooks", "ingesttok"))
            .await
            .unwrap();
        assert_eq!(get_ingest.status(), StatusCode::FORBIDDEN);

        // GET with read token → 200.
        let get_read = router
            .clone()
            .oneshot(get("/api/webhooks", "readtok"))
            .await
            .unwrap();
        assert_eq!(get_read.status(), StatusCode::OK);
    }

    /// POST with `event` + optional `delivery` headers (no delivery when
    /// `delivery` is `None`).
    async fn post_maybe_delivery(
        router: &Router,
        event: &str,
        delivery: Option<&str>,
        body: &str,
    ) -> Response {
        let mut builder = Request::builder()
            .method("POST")
            .uri("/api/webhooks")
            .header("authorization", "Bearer ingesttok")
            .header("x-github-event", event);
        if let Some(d) = delivery {
            builder = builder.header("x-github-delivery", d);
        }
        router
            .clone()
            .oneshot(
                builder
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn unsupported_event_ignored_without_delivery() {
        // The DoS-aware contract: an unsupported event is dropped with no
        // row and WITHOUT requiring a delivery id.
        let (_db, pool) = setup_pg_db().await;
        let router = router(&pool);
        let resp = post_maybe_delivery(&router, "star", None, "{}").await;
        assert_eq!(resp.status(), StatusCode::OK);
        let j: serde_json::Value = json_body(resp).await;
        assert_eq!(j["result"], "ignored");
        assert_eq!(j["reason"], "unsupported_event_type");
    }

    #[tokio::test]
    async fn supported_event_missing_delivery_is_400() {
        let (_db, pool) = setup_pg_db().await;
        let router = router(&pool);
        let resp = post_maybe_delivery(&router, "push", None, "{}").await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn supported_event_blank_delivery_is_400_not_recorded() {
        // A present-but-blank delivery id must be rejected, not stored as
        // `delivery_id = ''` (which would defeat dedup).
        let (_db, pool) = setup_pg_db().await;
        let router = router(&pool);
        let resp = post_maybe_delivery(&router, "push", Some("   "), "{}").await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM github_webhook")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0, "blank delivery must not record a row");
    }

    #[tokio::test]
    async fn malformed_payload_records_null_payload() {
        // A supported event with an unparseable body is still recorded (the
        // signature-verified bytes are forensically useful); the stored
        // payload is SQL NULL.
        let (_db, pool) = setup_pg_db().await;
        let router = router(&pool);
        let resp = post_maybe_delivery(&router, "push", Some("m-1"), "not json at all").await;
        assert_eq!(resp.status(), StatusCode::OK);
        let j: serde_json::Value = json_body(resp).await;
        assert_eq!(j["result"], "recorded");

        let is_null: bool = sqlx::query_scalar(
            "SELECT payload IS NULL FROM github_webhook WHERE delivery_id='m-1'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(is_null, "unparseable body must store SQL NULL payload");
    }
}
