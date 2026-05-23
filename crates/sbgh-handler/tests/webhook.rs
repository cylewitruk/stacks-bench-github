//! End-to-end handler tests. Asserts the handler is restricted to: signature
//! verification, authorization, and enqueuing a job row. Any GitHub API call
//! from the handler would be a regression — the handler holds no App
//! credentials. The orchestrator is responsible for all GitHub-side I/O.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use hmac::{Hmac, Mac};
use http_body_util::BodyExt;
use sbgh_core::config::{AuthorizationConfig, HandlerConfig, ServerConfig, WebhookConfig};
use sbgh_core::db::InMemoryJobStore;
use sbgh_core::models::JobStatus;
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
    jobs: Arc<InMemoryJobStore>,
}

fn setup() -> Harness {
    let jobs = Arc::new(InMemoryJobStore::new());
    let state = AppState {
        config: test_config(),
        jobs: jobs.clone(),
    };
    let router = routes::router().with_state(state);
    Harness { router, jobs }
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
    serde_json::to_vec(&serde_json::json!({
        "action": "created",
        "comment": {
            "id": 9999,
            "body": body,
            "user": { "login": sender },
            "author_association": association,
        },
        "issue": {
            "number": 42,
            "pull_request": pull_request,
        },
        "repository": { "full_name": repo },
        "sender": { "login": sender },
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

#[tokio::test]
async fn rejects_missing_signature() {
    let h = setup();
    let (status, _) = post_webhook(&h.router, "ping", b"{}".to_vec(), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn rejects_bad_signature() {
    let h = setup();
    let (status, _) =
        post_webhook(&h.router, "ping", b"{}".to_vec(), Some("sha256=deadbeef")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn ping_returns_pong() {
    let h = setup();
    let body = b"{}".to_vec();
    let sig = sign(&body);
    let (status, text) = post_webhook(&h.router, "ping", body, Some(&sig)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(text, "pong");
}

#[tokio::test]
async fn ignores_non_pr_issue_comment() {
    let h = setup();
    let body = issue_comment_payload("acme/widgets", "/benchmark", "alice", "MEMBER", false);
    let sig = sign(&body);
    let (status, text) = post_webhook(&h.router, "issue_comment", body, Some(&sig)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(text, "not a PR");
    assert!(h.jobs.snapshot().is_empty());
}

#[tokio::test]
async fn ignores_comment_without_command() {
    let h = setup();
    let body = issue_comment_payload("acme/widgets", "looks good!", "alice", "MEMBER", true);
    let sig = sign(&body);
    let (status, text) = post_webhook(&h.router, "issue_comment", body, Some(&sig)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(text, "no command");
    assert!(h.jobs.snapshot().is_empty());
}

#[tokio::test]
async fn rejects_disallowed_repo() {
    let h = setup();
    let body = issue_comment_payload("evil/repo", "/benchmark", "alice", "OWNER", true);
    let sig = sign(&body);
    let (status, text) = post_webhook(&h.router, "issue_comment", body, Some(&sig)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(text, "unauthorized");
    assert!(h.jobs.snapshot().is_empty());
}

#[tokio::test]
async fn rejects_disallowed_association() {
    let h = setup();
    let body = issue_comment_payload("acme/widgets", "/benchmark", "bob", "NONE", true);
    let sig = sign(&body);
    let (status, text) = post_webhook(&h.router, "issue_comment", body, Some(&sig)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(text, "unauthorized");
    assert!(h.jobs.snapshot().is_empty());
}

#[tokio::test]
async fn happy_path_enqueues_without_head_sha() {
    // Pinning the post-refactor contract: the handler enqueues with an empty
    // head_sha and a NULL comment_id. The orchestrator fills both in on
    // pickup. If a future change re-introduces a GitHub API call here, the
    // role-split blast radius opens back up — fail the test instead.
    let h = setup();

    let body =
        issue_comment_payload("acme/widgets", "/benchmark run --iters=5", "alice", "MEMBER", true);
    let sig = sign(&body);
    let (status, text) = post_webhook(&h.router, "issue_comment", body, Some(&sig)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(text, "queued");

    let jobs = h.jobs.snapshot();
    assert_eq!(jobs.len(), 1);
    let job = &jobs[0];
    assert_eq!(job.status, JobStatus::Queued);
    assert_eq!(job.repository, "acme/widgets");
    assert_eq!(job.pr_number, 42);
    assert_eq!(job.requested_by, "alice");
    assert_eq!(job.command, "run");
    assert_eq!(job.installation_id, 7);
    assert_eq!(job.args.0, serde_json::json!({ "args": ["--iters=5"] }));
    assert!(
        job.head_sha.is_empty(),
        "handler must not resolve head_sha (no App credentials); got {:?}",
        job.head_sha
    );
    assert!(job.comment_id.is_none(), "handler must not post a comment");
}

#[tokio::test]
async fn duplicate_delivery_id_is_deduped() {
    // GitHub redelivers on 5xx and on operator "Redeliver" clicks. Same
    // X-GitHub-Delivery means same logical webhook, even across retries.
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
    assert_eq!(b1, "queued");

    let (s2, b2) =
        post_webhook_with_delivery(&h.router, "issue_comment", body, Some(&sig), Some("repeat-me"))
            .await;
    assert_eq!(s2, StatusCode::OK);
    assert_eq!(b2, "duplicate");

    assert_eq!(h.jobs.snapshot().len(), 1, "second delivery must not enqueue");
}
