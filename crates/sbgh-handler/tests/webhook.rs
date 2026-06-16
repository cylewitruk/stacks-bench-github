//! Handler tests. Post-Phase-4 the handler is a **verify-and-forward**
//! shim: it checks the HMAC signature, short-circuits `ping`, and forwards
//! every other verified delivery to the daemon `/api`. It does NOT
//! parse payloads, filter event types, authorize, or touch a DB — the
//! daemon owns all of that. These tests drive the real route against a
//! mock daemon and assert what the handler forwards + how it maps the
//! daemon's response back to GitHub.

use std::sync::{Arc, Mutex};

use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, Request, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::post;
use hmac::{Hmac, KeyInit, Mac};
use http_body_util::BodyExt;
use sbgh_api::Client;
use sbgh_core::config::{HandlerApiConfig, HandlerConfig, ServerConfig, WebhookConfig};
use sha2::Sha256;
use tower::ServiceExt;

#[path = "../src/routes/mod.rs"]
mod routes;
#[path = "../src/state.rs"]
mod state;

use state::AppState;

const SECRET: &str = "test-webhook-secret";
const INGEST_TOKEN: &str = "ingest-token-xyz";

// ─── Mock daemon `/api/webhooks` ─────────────────────────────────────────

/// One captured forward, so tests can assert the handler passed through the
/// raw body + headers + bearer token verbatim.
#[derive(Clone, Default)]
struct Captured {
    authorization: Option<String>,
    event: Option<String>,
    delivery: Option<String>,
    body: Vec<u8>,
}

#[derive(Clone)]
struct MockState {
    captured: Arc<Mutex<Vec<Captured>>>,
    status: StatusCode,
    body: serde_json::Value,
}

async fn mock_submit(
    State(s): State<MockState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let hdr = |k: &str| {
        headers
            .get(k)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    };
    s.captured
        .lock()
        .unwrap()
        .push(Captured {
            authorization: hdr("authorization"),
            event: hdr("x-github-event"),
            delivery: hdr("x-github-delivery"),
            body: body.to_vec(),
        });
    (s.status, axum::Json(s.body.clone()))
}

struct MockDaemon {
    base_url: String,
    captured: Arc<Mutex<Vec<Captured>>>,
    /// Aborted on drop so the spawned server + its listener don't outlive
    /// the test (nextest flags such leaks).
    server: tokio::task::JoinHandle<()>,
}

impl Drop for MockDaemon {
    fn drop(&mut self) {
        self.server.abort();
    }
}

/// Spawn a one-shot mock daemon that records every `/api/webhooks` POST and
/// replies with `status` + `resp` (a `WebhookSubmitResponse`-shaped value).
async fn mock_daemon(status: StatusCode, resp: serde_json::Value) -> MockDaemon {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let app = axum::Router::new()
        .route("/api/webhooks", post(mock_submit))
        .with_state(MockState {
            captured: captured.clone(),
            status,
            body: resp,
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    MockDaemon {
        base_url: format!("http://{addr}"),
        captured,
        server,
    }
}

fn ok_response(result: &str) -> serde_json::Value {
    serde_json::json!({ "result": result })
}

// ─── Handler harness ─────────────────────────────────────────────────────

fn test_config(api_url: String) -> HandlerConfig {
    HandlerConfig {
        server: ServerConfig {
            bind_addr: "127.0.0.1:0".into(),
        },
        webhook: WebhookConfig { secret: SECRET.into() },
        api: HandlerApiConfig {
            url: api_url,
            ingest_token: INGEST_TOKEN.into(),
        },
    }
}

struct Harness {
    router: axum::Router,
    daemon: MockDaemon,
}

/// Wire the real handler route to a mock daemon that replies `status`/`resp`.
async fn setup(status: StatusCode, resp: serde_json::Value) -> Harness {
    let daemon = mock_daemon(status, resp).await;
    let config = test_config(daemon.base_url.clone());
    let api = Client::new(
        config.api.url.clone(),
        Some(
            config
                .api
                .ingest_token
                .clone(),
        ),
    );
    let router = routes::router().with_state(AppState { config, api });
    Harness { router, daemon }
}

/// Default success daemon (records, returns 200 `recorded`).
async fn setup_ok() -> Harness {
    setup(StatusCode::OK, ok_response("recorded")).await
}

fn sign(body: &[u8]) -> String {
    let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(SECRET.as_bytes()).unwrap();
    mac.update(body);
    let bytes = mac.finalize().into_bytes();
    format!("sha256={}", hex::encode::<&[u8]>(bytes.as_ref()))
}

async fn post_webhook(
    router: &axum::Router,
    event: &str,
    body: Vec<u8>,
    signature: Option<&str>,
    delivery: Option<&str>,
) -> (StatusCode, String) {
    let mut req = Request::builder()
        .method("POST")
        .uri("/webhook")
        .header(header::CONTENT_TYPE, "application/json")
        .header("x-github-event", event);
    if let Some(sig) = signature {
        req = req.header("x-hub-signature-256", sig);
    }
    if let Some(d) = delivery {
        req = req.header("x-github-delivery", d);
    }
    let resp = router
        .clone()
        .oneshot(
            req.body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}

// ─── Signature gating: never forwards ────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn rejects_missing_signature_without_forwarding() {
    let h = setup_ok().await;
    let (status, _) =
        post_webhook(&h.router, "issue_comment", b"{}".to_vec(), None, Some("d1")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        h.daemon
            .captured
            .lock()
            .unwrap()
            .len(),
        0,
        "unsigned must not reach /api"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn rejects_bad_signature_without_forwarding() {
    let h = setup_ok().await;
    let (status, _) = post_webhook(
        &h.router,
        "issue_comment",
        b"{}".to_vec(),
        Some("sha256=deadbeef"),
        Some("d1"),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        h.daemon
            .captured
            .lock()
            .unwrap()
            .len(),
        0
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn ping_returns_pong_without_forwarding() {
    let h = setup_ok().await;
    let body = b"{}".to_vec();
    let sig = sign(&body);
    let (status, text) = post_webhook(&h.router, "ping", body, Some(&sig), Some("d1")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(text, "pong");
    assert_eq!(
        h.daemon
            .captured
            .lock()
            .unwrap()
            .len(),
        0,
        "ping is answered locally"
    );
}

// ─── Forwarding ──────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn forwards_raw_body_and_headers_with_ingest_token() {
    let h = setup_ok().await;
    // Deliberately not valid issue_comment JSON: the handler must forward
    // the bytes verbatim, never parse them.
    let body = br#"{"raw":"bytes","action":"created"}"#.to_vec();
    let sig = sign(&body);
    let (status, text) =
        post_webhook(&h.router, "issue_comment", body.clone(), Some(&sig), Some("delivery-7"))
            .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(text, "recorded", "handler echoes the daemon's result");

    let cap = h
        .daemon
        .captured
        .lock()
        .unwrap();
    assert_eq!(cap.len(), 1, "exactly one forward");
    let c = &cap[0];
    assert_eq!(c.event.as_deref(), Some("issue_comment"));
    assert_eq!(c.delivery.as_deref(), Some("delivery-7"));
    assert_eq!(c.body, body, "body forwarded byte-for-byte");
    assert_eq!(
        c.authorization.as_deref(),
        Some(format!("Bearer {INGEST_TOKEN}").as_str()),
        "ingest token presented to /api"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_delivery_is_forwarded_as_absent_not_empty() {
    // A missing X-GitHub-Delivery must stay absent through the client (not
    // become `""`), so the daemon's "supported event needs a delivery id"
    // check fires instead of recording a blank dedup key.
    let h = setup_ok().await;
    let body = b"{}".to_vec();
    let sig = sign(&body);
    let (status, _) = post_webhook(&h.router, "push", body, Some(&sig), None).await;
    assert_eq!(status, StatusCode::OK);
    let cap = h
        .daemon
        .captured
        .lock()
        .unwrap();
    assert_eq!(cap.len(), 1);
    assert_eq!(cap[0].delivery, None, "absent delivery header must not be sent as empty");
}

#[tokio::test(flavor = "multi_thread")]
async fn forwards_unsupported_event_and_echoes_ignored() {
    // The handler no longer filters event types — it forwards `star` and
    // echoes whatever the daemon decides ("ignored").
    let h = setup(StatusCode::OK, ok_response("ignored")).await;
    let body = b"{}".to_vec();
    let sig = sign(&body);
    let (status, text) = post_webhook(&h.router, "star", body, Some(&sig), Some("d1")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(text, "ignored");
    assert_eq!(
        h.daemon
            .captured
            .lock()
            .unwrap()
            .len(),
        1,
        "filtering is the daemon's job now"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn echoes_duplicate_result() {
    let h = setup(StatusCode::OK, ok_response("duplicate")).await;
    let body = b"{}".to_vec();
    let sig = sign(&body);
    let (status, text) = post_webhook(&h.router, "push", body, Some(&sig), Some("d1")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(text, "duplicate");
}

// ─── Daemon error mapping ────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn daemon_5xx_maps_to_502_so_github_retries() {
    let h = setup(StatusCode::INTERNAL_SERVER_ERROR, serde_json::json!({ "error": "boom" })).await;
    let body = b"{}".to_vec();
    let sig = sign(&body);
    let (status, _) = post_webhook(&h.router, "push", body, Some(&sig), Some("d1")).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY, "5xx upstream → 502 (GitHub redelivers)");
}

#[tokio::test(flavor = "multi_thread")]
async fn daemon_4xx_is_propagated_not_retried() {
    // A permanent client error (e.g. bad ingest token = 401) is surfaced as
    // itself, not a 502 — no point making GitHub retry a request that can't
    // succeed.
    let h = setup(StatusCode::UNAUTHORIZED, serde_json::json!({ "error": "bad token" })).await;
    let body = b"{}".to_vec();
    let sig = sign(&body);
    let (status, _) = post_webhook(&h.router, "push", body, Some(&sig), Some("d1")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test(flavor = "multi_thread")]
async fn daemon_unreachable_maps_to_502() {
    // Point the handler at a dead port — no listener.
    let config = test_config("http://127.0.0.1:1".into());
    let api = Client::new(
        config.api.url.clone(),
        Some(
            config
                .api
                .ingest_token
                .clone(),
        ),
    );
    let router = routes::router().with_state(AppState { config, api });
    let body = b"{}".to_vec();
    let sig = sign(&body);
    let (status, _) = post_webhook(&router, "push", body, Some(&sig), Some("d1")).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY, "transport failure → 502");
}
