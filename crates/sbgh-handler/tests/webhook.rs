//! End-to-end handler tests. Slice 11 made the handler **inbox-only**:
//! it is restricted to signature verification, event-type filtering, and
//! webhook-inbox recording. It does NOT parse `/benchmark`, authorize, or
//! enqueue a legacy `jobs` row — the processor owns all of that. Any
//! GitHub API call OR legacy-job write from the handler would be a
//! regression (the `.jobs()`-empty assertions pin the latter).

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use hmac::{Hmac, Mac};
use http_body_util::BodyExt;
use sbgh_core::config::{AuthorizationConfig, HandlerConfig, ServerConfig, WebhookConfig};
use sbgh_core::db::InMemoryIngestStore;
use sha2::Sha256;
use tower::ServiceExt;

#[path = "../src/routes/mod.rs"]
mod routes;
#[path = "../src/state.rs"]
mod state;

use state::AppState;

const SECRET: &str = "test-webhook-secret";

fn test_config() -> HandlerConfig {
    HandlerConfig {
        server: ServerConfig {
            database_url: "postgres://unused".into(),
            bind_addr: "127.0.0.1:0".into(),
        },
        webhook: WebhookConfig { secret: SECRET.into() },
        authorization: AuthorizationConfig {
            allowed_repositories: ["acme/widgets".into()]
                .into_iter()
                .collect(),
            allowed_users: Default::default(),
            allowed_associations: ["MEMBER".into(), "OWNER".into()]
                .into_iter()
                .collect(),
        },
    }
}

struct Harness {
    router: axum::Router,
    ingest: Arc<InMemoryIngestStore>,
}

fn setup() -> Harness {
    let ingest = Arc::new(InMemoryIngestStore::new());
    let state = AppState {
        config: test_config(),
        ingest: ingest.clone(),
    };
    let router = routes::router().with_state(state);
    Harness { router, ingest }
}

fn sign(body: &[u8]) -> String {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(SECRET.as_bytes()).unwrap();
    mac.update(body);
    let bytes = mac.finalize().into_bytes();
    format!("sha256={}", hex::encode::<&[u8]>(bytes.as_ref()))
}

fn issue_comment_payload(
    repo: &str,
    body: &str,
    sender: &str,
    association: &str,
    is_pr: bool,
) -> Vec<u8> {
    let pull_request = if is_pr {
        serde_json::json!({ "url": format!("https://api.github.test/repos/{repo}/pulls/42") })
    } else {
        serde_json::Value::Null
    };
    // Slice 6 grew the User struct to require numeric id + GH account
    // type. Handler tests don't care about either field beyond "the
    // typed deserialiser accepts the payload" — pick stable
    // placeholders.
    serde_json::to_vec(&serde_json::json!({
        "action": "created",
        "comment": {
            "id": 9999,
            "body": body,
            "user": { "id": 1234, "login": sender, "type": "User" },
            "author_association": association,
        },
        "issue": {
            "number": 42,
            "pull_request": pull_request,
        },
        "repository": { "full_name": repo },
        "sender": { "id": 1234, "login": sender, "type": "User" },
        "installation": { "id": 7 },
    }))
    .unwrap()
}

async fn post_webhook(
    router: &axum::Router,
    event: &str,
    body: Vec<u8>,
    signature: Option<&str>,
) -> (StatusCode, String) {
    post_webhook_with_delivery(router, event, body, signature, Some("delivery-test-1")).await
}

async fn post_webhook_with_delivery(
    router: &axum::Router,
    event: &str,
    body: Vec<u8>,
    signature: Option<&str>,
    delivery_id: Option<&str>,
) -> (StatusCode, String) {
    let mut req = Request::builder()
        .method("POST")
        .uri("/webhook")
        .header(header::CONTENT_TYPE, "application/json")
        .header("x-github-event", event);
    if let Some(sig) = signature {
        req = req.header("x-hub-signature-256", sig);
    }
    if let Some(d) = delivery_id {
        req = req.header("x-github-delivery", d);
    }
    let req = req
        .body(Body::from(body))
        .unwrap();
    let resp = router
        .clone()
        .oneshot(req)
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    let body = String::from_utf8(bytes.to_vec()).unwrap();
    (status, body)
}

// ─── Signature / wire-level gating ───────────────────────────────────────

#[tokio::test]
async fn rejects_missing_signature() {
    let h = setup();
    let (status, _) = post_webhook(&h.router, "ping", b"{}".to_vec(), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    // Slice 1 invariant: unsigned requests never reach the inbox.
    assert_eq!(h.ingest.webhook_count(), 0);
    assert!(
        h.ingest
            .jobs()
            .snapshot()
            .is_empty()
    );
}

#[tokio::test]
async fn rejects_bad_signature() {
    let h = setup();
    let (status, _) =
        post_webhook(&h.router, "ping", b"{}".to_vec(), Some("sha256=deadbeef")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(h.ingest.webhook_count(), 0);
    assert!(
        h.ingest
            .jobs()
            .snapshot()
            .is_empty()
    );
}

#[tokio::test]
async fn ping_returns_pong_without_inbox_row() {
    let h = setup();
    let body = b"{}".to_vec();
    let sig = sign(&body);
    let (status, text) = post_webhook(&h.router, "ping", body, Some(&sig)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(text, "pong");
    // ping is GitHub's connectivity probe; intentionally not recorded.
    assert_eq!(h.ingest.webhook_count(), 0);
}

#[tokio::test]
async fn drops_unsupported_event_type_without_inbox_row() {
    let h = setup();
    let body = b"{}".to_vec();
    let sig = sign(&body);
    let (status, text) = post_webhook(&h.router, "star", body, Some(&sig)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(text, "ignored");
    // Slice 1 invariant: only allowlisted event types reach the inbox.
    assert_eq!(h.ingest.webhook_count(), 0);
    assert!(
        h.ingest
            .jobs()
            .snapshot()
            .is_empty()
    );
}

#[tokio::test]
async fn rejects_missing_delivery_id() {
    let h = setup();
    let body = b"{}".to_vec();
    let sig = sign(&body);
    let (status, _) = post_webhook_with_delivery(
        &h.router,
        "push",
        body,
        Some(&sig),
        None, // no X-GitHub-Delivery header
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(h.ingest.webhook_count(), 0);
}

// ─── Supported events: webhook-only path ─────────────────────────────────

#[tokio::test]
async fn supported_event_records_webhook_only() {
    // A `push` event hits the inbox but doesn't enqueue a legacy job.
    let h = setup();
    let body = serde_json::to_vec(&serde_json::json!({
        "ref": "refs/heads/develop",
        "installation": { "id": 7 },
    }))
    .unwrap();
    let sig = sign(&body);

    let (status, text) =
        post_webhook_with_delivery(&h.router, "push", body, Some(&sig), Some("push-delivery-1"))
            .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(text, "recorded");
    assert_eq!(h.ingest.webhook_count(), 1);
    let webhook = &h.ingest.webhooks()[0];
    assert_eq!(webhook.event_type, "push");
    assert_eq!(webhook.payload_installation_id, Some(7));
    assert!(
        h.ingest
            .jobs()
            .snapshot()
            .is_empty(),
        "push events must NOT enqueue legacy jobs"
    );
}

#[tokio::test]
async fn issue_comment_records_webhook_only_no_job() {
    // Inbox-only (slice 11): a `/benchmark` issue_comment is recorded
    // like any other event — NO legacy job, NO authz, NO command parse.
    // The processor handles all of that from the inbox. The empty
    // `.jobs()` is the load-bearing assertion: the handler must never
    // write legacy `jobs` post-cutover (else a rollback to the legacy
    // backend could re-run them).
    let h = setup();
    let body =
        issue_comment_payload("acme/widgets", "/benchmark run --iters=5", "alice", "MEMBER", true);
    let sig = sign(&body);
    let (status, text) = post_webhook(&h.router, "issue_comment", body, Some(&sig)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(text, "recorded");

    assert_eq!(h.ingest.webhook_count(), 1);
    let webhook = &h.ingest.webhooks()[0];
    assert_eq!(webhook.event_type, "issue_comment");
    assert_eq!(webhook.action.as_deref(), Some("created"));
    assert_eq!(webhook.payload_installation_id, Some(7));
    assert!(
        h.ingest
            .jobs()
            .snapshot()
            .is_empty(),
        "inbox-only handler must NOT enqueue a legacy job"
    );
}

#[tokio::test]
async fn issue_comment_recorded_regardless_of_authz_or_command() {
    // The handler no longer gates on authz / command shape — every
    // signature-valid issue_comment is recorded for the processor.
    // (Cases that used to short-circuit as "unauthorized" / "no command"
    // / "not a PR" now all just record.)
    for body in [
        issue_comment_payload("acme/widgets", "looks good!", "alice", "MEMBER", true), /* no command */
        issue_comment_payload("acme/widgets", "/benchmark", "bob", "NONE", true), // "unauthorized"
        issue_comment_payload("evil/repo", "/benchmark", "alice", "OWNER", true), /* disallowed
                                                                                   * repo */
        issue_comment_payload("acme/widgets", "/benchmark", "alice", "MEMBER", false), // not a PR
    ] {
        let h = setup();
        let sig = sign(&body);
        let (status, text) = post_webhook(&h.router, "issue_comment", body, Some(&sig)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(text, "recorded");
        assert_eq!(h.ingest.webhook_count(), 1);
        assert!(
            h.ingest
                .jobs()
                .snapshot()
                .is_empty()
        );
    }
}

#[tokio::test]
async fn duplicate_issue_comment_delivery_dedupes() {
    // Same X-GitHub-Delivery → same logical webhook → one inbox row.
    let h = setup();
    let body = issue_comment_payload("acme/widgets", "/benchmark", "alice", "MEMBER", true);
    let sig = sign(&body);

    let (s1, b1) = post_webhook_with_delivery(
        &h.router,
        "issue_comment",
        body.clone(),
        Some(&sig),
        Some("repeat-me"),
    )
    .await;
    assert_eq!(s1, StatusCode::OK);
    assert_eq!(b1, "recorded");

    let (s2, b2) =
        post_webhook_with_delivery(&h.router, "issue_comment", body, Some(&sig), Some("repeat-me"))
            .await;
    assert_eq!(s2, StatusCode::OK);
    assert_eq!(b2, "duplicate");

    assert_eq!(h.ingest.webhook_count(), 1, "second delivery must not record a second webhook");
    assert!(
        h.ingest
            .jobs()
            .snapshot()
            .is_empty()
    );
}

#[tokio::test]
async fn malformed_issue_comment_payload_still_records_webhook() {
    // Body is signature-verified but isn't a well-formed issue_comment.
    // Inbox-only still records it (valid JSON → payload stored) so GH
    // redeliveries dedupe against the inbox row. Returning 4xx here would
    // trigger un-dedupable GH retry storms.
    let h = setup();
    let bad_body = br#"{"action": "created", "this is": "not an issue_comment"}"#.to_vec();
    let sig = sign(&bad_body);
    let (status, text) = post_webhook(&h.router, "issue_comment", bad_body, Some(&sig)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(text, "recorded");
    assert_eq!(h.ingest.webhook_count(), 1);
    let webhook = &h.ingest.webhooks()[0];
    assert_eq!(webhook.event_type, "issue_comment");
    assert!(
        h.ingest
            .jobs()
            .snapshot()
            .is_empty(),
        "malformed payload must NOT enqueue a legacy job"
    );
}

#[tokio::test]
async fn duplicate_delivery_on_webhook_only_path_dedupes() {
    // Even for webhook-only events (no job enqueue), redeliveries must
    // be deduped on the inbox.
    let h = setup();
    let body = serde_json::to_vec(&serde_json::json!({
        "ref": "refs/heads/develop",
        "installation": { "id": 7 },
    }))
    .unwrap();
    let sig = sign(&body);

    let (s1, b1) = post_webhook_with_delivery(
        &h.router,
        "push",
        body.clone(),
        Some(&sig),
        Some("redeliver-2"),
    )
    .await;
    assert_eq!(s1, StatusCode::OK);
    assert_eq!(b1, "recorded");

    let (s2, b2) =
        post_webhook_with_delivery(&h.router, "push", body, Some(&sig), Some("redeliver-2")).await;
    assert_eq!(s2, StatusCode::OK);
    assert_eq!(b2, "duplicate");

    assert_eq!(h.ingest.webhook_count(), 1);
}
