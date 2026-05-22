//! End-to-end handler tests using the real axum router with fake GH + in-memory
//! queue.

use std::path::PathBuf;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use hmac::{Hmac, Mac};
use http_body_util::BodyExt;
use sbgh_core::config::{
    AuthorizationConfig, Config, GitHubConfig, LvmConfig, PathsConfig, ServerConfig,
    StacksBenchConfig, VmConfig,
};
use sbgh_core::db::InMemoryJobStore;
use sbgh_core::github::test_support::{FakeCall, FakeGitHub};
use sbgh_core::models::JobStatus;
use sha2::Sha256;
use tower::ServiceExt;

// `routes` and `state` are inline modules in the binary crate, so we re-import
// them here by including the source files. This keeps the test binary self-
// contained without exposing the modules as a `lib.rs`.
#[path = "../src/routes/mod.rs"]
mod routes;
#[path = "../src/state.rs"]
mod state;

use state::AppState;

const SECRET: &str = "test-webhook-secret";

fn test_config() -> Config {
    Config {
        server: ServerConfig {
            database_url: "postgres://unused".into(),
            bind_addr: "127.0.0.1:0".into(),
            service_user: "sbgh".into(),
        },
        github: GitHubConfig {
            client_id: "Iv23litest".into(),
            api_base_url: "https://api.github.test".into(),
            private_key_path: PathBuf::from("/dev/null"),
            webhook_secret: SECRET.into(),
        },
        authorization: AuthorizationConfig {
            allowed_repositories: ["acme/widgets".into()]
                .into_iter()
                .collect(),
            allowed_users: Default::default(),
            allowed_associations: ["MEMBER".into(), "OWNER".into()]
                .into_iter()
                .collect(),
        },
        vm: VmConfig {
            golden_image: PathBuf::from("/tmp/golden.qcow2"),
            vcpus: 2,
            memory_gib: 8,
            boot_disk_gib: 64,
            job_timeout_secs: 60,
            network: "default".into(),
        },
        paths: PathsConfig {
            jobs_dir: PathBuf::from("/tmp/jobs"),
            git_mirror: PathBuf::from("/tmp/git/stacks-core.git"),
            results_tmpfs_root: PathBuf::from("/tmp/results-tmpfs"),
            results_archive_dir: PathBuf::from("/tmp/results-archive"),
            virsh_binary: PathBuf::from("/usr/bin/virsh"),
            sudo_binary: PathBuf::from("/usr/bin/sudo"),
            qemu_img_binary: PathBuf::from("/usr/bin/qemu-img"),
            cloud_localds_binary: PathBuf::from("/usr/bin/cloud-localds"),
            git_binary: PathBuf::from("/usr/bin/git"),
        },
        lvm: LvmConfig {
            vg_name: "sbgh-vg".into(),
            thinpool: "thinpool".into(),
            chainstate_base_prefix: "mainnet-".into(),
            chainstate_snapshot_size_gib: 64,
        },
        stacks_bench: StacksBenchConfig { default_args: String::new() },
    }
}

struct Harness {
    router: axum::Router,
    jobs: Arc<InMemoryJobStore>,
    gh: Arc<FakeGitHub>,
}

fn setup() -> Harness {
    let jobs = Arc::new(InMemoryJobStore::new());
    let gh = Arc::new(FakeGitHub::new());
    let state = AppState {
        config: test_config(),
        jobs: jobs.clone(),
        gh: gh.clone(),
    };
    let router = routes::router().with_state(state);
    Harness { router, jobs, gh }
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
    assert!(h.gh.calls().is_empty());
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
async fn happy_path_enqueues_and_comments() {
    let h = setup();
    h.gh.set_head_sha("acme/widgets", 42, "abc123def456");

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
    assert_eq!(job.head_sha, "abc123def456");
    assert_eq!(job.requested_by, "alice");
    assert_eq!(job.command, "run");
    assert_eq!(job.installation_id, 7);
    assert_eq!(job.comment_id, Some(1000));
    assert_eq!(job.args.0, serde_json::json!({ "args": ["--iters=5"] }));

    let calls = h.gh.calls();
    assert!(matches!(&calls[0], FakeCall::HeadSha { pr_number: 42, .. }));
    let posted = calls
        .iter()
        .find_map(|c| match c {
            FakeCall::CreateComment { body, .. } => Some(body.clone()),
            _ => None,
        })
        .expect("expected a CreateComment");
    assert!(posted.contains("position **#1**"));
    assert!(posted.contains(&job.id.to_string()));
}

#[tokio::test]
async fn duplicate_delivery_id_is_deduped() {
    // GitHub redelivers on 5xx and on operator "Redeliver" clicks. The same
    // X-GitHub-Delivery value identifies the same logical webhook, even
    // across retries, so it's our idempotency key.
    let h = setup();
    h.gh.set_head_sha("acme/widgets", 42, "abc");

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

#[tokio::test]
async fn queue_position_increments() {
    let h = setup();
    for i in 0..3 {
        let body = issue_comment_payload(
            "acme/widgets",
            &format!("/benchmark run{i}"),
            "alice",
            "MEMBER",
            true,
        );
        let sig = sign(&body);
        let delivery = format!("delivery-pos-{i}");
        let (status, _) = post_webhook_with_delivery(
            &h.router,
            "issue_comment",
            body,
            Some(&sig),
            Some(&delivery),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }
    assert_eq!(h.jobs.snapshot().len(), 3);

    let posted_positions: Vec<String> =
        h.gh.calls()
            .into_iter()
            .filter_map(|c| match c {
                FakeCall::CreateComment { body, .. } => Some(body),
                _ => None,
            })
            .collect();
    assert_eq!(posted_positions.len(), 3);
    assert!(posted_positions[0].contains("position **#1**"));
    assert!(posted_positions[1].contains("position **#2**"));
    assert!(posted_positions[2].contains("position **#3**"));
}
