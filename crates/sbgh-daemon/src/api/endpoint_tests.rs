//! HTTP-layer tests for the Phase-3b admin + listing endpoints: auth
//! scoping, empty listings, a write round-trip + validation, and one
//! resolve-path test via a mock GitHub. The GitHub-resolution *logic*
//! itself is covered by the relocated `sbgh-cli` tests; here we exercise
//! the routing/auth/serialization/error-mapping the API adds on top.

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use sbgh_core::db::SubmissionStore;
use sbgh_core::models::{
    BuildTarget, GitRefKind, JobIntent, JobSource, QueuedEventDetail, TaskKind,
};
use sbgh_core::submission::{
    BlockValidationPlan, PreparedSubmission, ProducerKey, ResolvedTaskSource,
    SchedulingConstraints, SubmissionActor, SubmissionCommand, SubmissionProvenance, TaskPlan,
};
use sbgh_fleet::{BlockValidationPayload, BlockValidationSelection};
use sbgh_postgres::db::PostgresJobStore;
use sbgh_postgres::db::{Pool, PostgresIngestStore, setup_pg_db};
use tower::ServiceExt;

use super::{ApiState, ApiTokens, build_router};

fn router_with(pool: Pool, gh_api_base: String) -> Router {
    router_with_artifacts(
        pool,
        gh_api_base,
        Arc::new(crate::artifact_store::LocalFsStore::new(
            std::env::temp_dir().join("sbgh-api-endpoint-unused"),
        )),
    )
}

fn router_with_artifacts(
    pool: Pool,
    gh_api_base: String,
    artifacts: Arc<dyn crate::artifact_store::ArtifactStore>,
) -> Router {
    let tokens = Arc::new(
        ApiTokens::new("admintok".into(), Some("ingesttok".into()), Some("readtok".into()))
            .unwrap(),
    );
    let state = ApiState {
        pool: pool.clone(),
        ingest: Arc::new(PostgresIngestStore::new(pool)),
        artifacts,
        gh_api_base,
        block_validation: None,
    };
    build_router(state, tokens)
}

#[tokio::test]
async fn job_artifacts_require_admin_and_stream_only_recorded_names() {
    let (_db, pool) = setup_pg_db().await;
    for query in [
        "INSERT INTO allowed_installer (github_account_id, account_login, account_type) \
         VALUES (100, 'octo', 'organization')",
        "INSERT INTO github_installation (id, github_account_id, account_login, account_type) \
         VALUES (100, 100, 'octo', 'organization')",
        "INSERT INTO github_repo (id, owner, name) VALUES (10, 'o', 'r')",
        "INSERT INTO github_installation_repo (github_installation_id, github_repo_id) \
         VALUES (100, 10)",
    ] {
        sqlx::query(query)
            .execute(&pool)
            .await
            .unwrap();
    }
    let commit = "a".repeat(40);
    let prepared = PreparedSubmission {
        command: SubmissionCommand {
            actor: SubmissionActor::System,
            producer_key: ProducerKey {
                namespace: "artifact-api-test".into(),
                key: "one".into(),
            },
            constraints: SchedulingConstraints::default(),
            task: TaskPlan::BlockValidation(BlockValidationPlan {
                source: ResolvedTaskSource {
                    github_installation_id: 100,
                    github_repo_id: 10,
                    source: JobSource::Cli,
                    intent: JobIntent::BlockValidation,
                    task_kind: TaskKind::BlockValidation,
                    build_target: BuildTarget::StacksInspect,
                    git_ref_kind: GitRefKind::Commit,
                    git_ref_display: commit.clone(),
                    commit,
                    committed_at: None,
                    workload_key: None,
                },
                payload: BlockValidationPayload {
                    selection: BlockValidationSelection::Recent { block_count: 1 },
                    timeout_secs: 60,
                },
            }),
            provenance: SubmissionProvenance {
                queued_event_detail: serde_json::to_value(QueuedEventDetail::BlockValidation {
                    selection: BlockValidationSelection::Recent { block_count: 1 },
                })
                .unwrap(),
                github: None,
                slack: None,
            },
        },
        contract_version: 1,
        request_digest: "1".repeat(64),
    };
    let receipt = PostgresJobStore::new(pool.clone())
        .persist_submission(&prepared)
        .await
        .unwrap();
    let job_id = receipt.initial_job_ids[0];
    let root = tempfile::tempdir().unwrap();
    let key = format!("{job_id}/block-validation/result.json");
    let path = root.path().join(&key);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, b"{}\n").unwrap();
    sqlx::query("UPDATE job SET status = 'completed' WHERE id = $1")
        .bind(job_id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO job_event (job_id, event_kind, event_status, detail) \
         VALUES ($1, 'completed', 'success', $2)",
    )
    .bind(job_id)
    .bind(serde_json::json!({"artifacts": [{
        "key": key,
        "logical_key": key,
        "size": 3,
        "sha256": "a".repeat(64),
    }]}))
    .execute(&pool)
    .await
    .unwrap();
    let router = router_with_artifacts(
        pool,
        "http://unused".into(),
        Arc::new(crate::artifact_store::LocalFsStore::new(root.path().into())),
    );
    let list_uri = format!("/api/jobs/{job_id}/artifacts");
    let (status, _) = send(&router, "GET", &list_uri, None, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = send(&router, "GET", &list_uri, Some("readtok"), None).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, list) = send(&router, "GET", &list_uri, Some("admintok"), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list[0]["name"], "block-validation/result.json");
    assert_eq!(list[0]["size"], 3);
    let unknown_uri = format!("/api/jobs/{}/artifacts", uuid::Uuid::new_v4());
    let (status, _) = send(&router, "GET", &unknown_uri, Some("admintok"), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let cat_uri =
        format!("/api/jobs/{job_id}/artifacts/content?name=block-validation%2Fresult.json");
    let (status, _) = send(&router, "GET", &cat_uri, Some("readtok"), None).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _) = send(
        &router,
        "GET",
        &format!("/api/jobs/{job_id}/artifacts/content?name=other"),
        Some("admintok"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = send(
        &router,
        "GET",
        &format!("/api/jobs/{job_id}/artifacts/content?name=..%2Fother"),
        Some("admintok"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(cat_uri)
                .header("authorization", "Bearer admintok")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    assert_eq!(body.as_ref(), b"{}\n");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .await
            .unwrap()
    });
    let client = sbgh_api::Client::new(format!("http://{address}"), Some("admintok".into()));
    let listed = client
        .list_job_artifacts(&job_id.to_string())
        .await
        .unwrap();
    assert_eq!(listed[0].name, "block-validation/result.json");
    let downloaded = root
        .path()
        .join("downloaded.json");
    let mut output = tokio::fs::File::create(&downloaded)
        .await
        .unwrap();
    client
        .copy_job_artifact(&job_id.to_string(), "block-validation/result.json", &mut output)
        .await
        .unwrap();
    assert_eq!(
        tokio::fs::read(downloaded)
            .await
            .unwrap(),
        b"{}\n"
    );
    server.abort();
}

/// A pool that never connects — fine for auth-rejection cases (the layer
/// runs before any handler touches the DB).
fn lazy_pool() -> Pool {
    sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://t:t@localhost/t")
        .unwrap()
}

async fn send(
    router: &Router,
    method: &str,
    uri: &str,
    token: Option<&str>,
    body: Option<&str>,
) -> (StatusCode, serde_json::Value) {
    let mut b = Request::builder()
        .method(method)
        .uri(uri);
    if let Some(t) = token {
        b = b.header("authorization", format!("Bearer {t}"));
    }
    let body = match body {
        Some(j) => {
            b = b.header("content-type", "application/json");
            Body::from(j.to_string())
        }
        None => Body::empty(),
    };
    let resp = router
        .clone()
        .oneshot(b.body(body).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    let json = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, json)
}

/// Spawn a one-shot GitHub mock returning a fixed org account for any path.
async fn mock_github() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let app = axum::Router::new().fallback(|| async {
        axum::Json(serde_json::json!({ "id": 4242, "login": "acme", "type": "Organization" }))
    });
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .unwrap();
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn auth_scoping_rejects_wrong_and_missing_tokens() {
    // Auth runs before any handler, so a lazy (never-connecting) pool is
    // fine for the rejection cases.
    let router = router_with(lazy_pool(), "http://unused".into());

    // admin endpoint with read token → 403
    let (s, _) =
        send(&router, "POST", "/api/installers", Some("readtok"), Some(r#"{"login":"x"}"#)).await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    // admin endpoint with no token → 401
    let (s, _) = send(&router, "POST", "/api/roles", None, Some("{}")).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
    // read endpoint with ingest token → 403
    let (s, _) = send(&router, "GET", "/api/jobs", Some("ingesttok"), None).await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    // read endpoint with no token → 401
    let (s, _) = send(&router, "GET", "/api/installations", None, None).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
    let (s, _) = send(
        &router,
        "GET",
        &format!("/api/submissions/{}/report", uuid::Uuid::new_v4()),
        None,
        None,
    )
    .await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn authenticated_submission_report_uses_not_found_envelope() {
    let (_db, pool) = setup_pg_db().await;
    let router = router_with(pool, "http://unused".into());
    let (status, body) = send(
        &router,
        "GET",
        &format!("/api/submissions/{}/report", uuid::Uuid::new_v4()),
        Some("readtok"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], "not_found");
}

#[tokio::test]
async fn worker_policy_is_admin_mutated_and_starts_inert() {
    let (_db, pool) = setup_pg_db().await;
    let router = router_with(pool, "http://unused".into());
    let worker_id = uuid::Uuid::new_v4();
    let body = serde_json::json!({
        "worker_id": worker_id,
        "display_name": "operator-host",
        "capabilities": ["block_validation"],
        "measurement_profile": null
    })
    .to_string();
    let (status, created) =
        send(&router, "POST", "/api/fleet/workers", Some("admintok"), Some(&body)).await;
    assert_eq!(status, StatusCode::OK, "{created}");
    assert_eq!(created["worker"]["worker_id"], worker_id.to_string());
    assert_eq!(created["worker"]["enabled"], false);
    assert_eq!(created["worker"]["draining"], true);
    assert_eq!(created["identities"], serde_json::json!([]));
    let (status, retried) =
        send(&router, "POST", "/api/fleet/workers", Some("admintok"), Some(&body)).await;
    assert_eq!(status, StatusCode::OK, "{retried}");
    assert_eq!(retried["worker"]["worker_id"], worker_id.to_string());

    let invalid = serde_json::json!({
        "worker_id": uuid::Uuid::new_v4(),
        "display_name": "unknown-field",
        "capabilities": ["build_only"],
        "measurement_profile": null,
        "unexpected": true
    })
    .to_string();
    let (status, _) =
        send(&router, "POST", "/api/fleet/workers", Some("admintok"), Some(&invalid)).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);

    let (status, _) = send(
        &router,
        "PATCH",
        &format!("/api/fleet/workers/{worker_id}"),
        Some("readtok"),
        Some(r#"{"enabled":true}"#),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, failed) = send(
        &router,
        "PATCH",
        &format!("/api/fleet/workers/{worker_id}"),
        Some("admintok"),
        Some(r#"{"enabled":true}"#),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(failed["error"]["message"], "worker has no active identity key");
}

#[tokio::test]
async fn empty_listings_return_ok_empty_array() {
    let (_db, pool) = setup_pg_db().await;
    let router = router_with(pool, "http://unused".into());
    for uri in [
        "/api/installers",
        "/api/repos",
        "/api/jobs",
        "/api/installations",
        "/api/policies/target",
        "/api/policies/source",
        "/api/policies/triggers",
        "/api/users",
        "/api/roles",
    ] {
        let (s, j) = send(&router, "GET", uri, Some("readtok"), None).await;
        assert_eq!(s, StatusCode::OK, "GET {uri}");
        assert!(
            j.as_array()
                .map(|a| a.is_empty())
                .unwrap_or(false),
            "GET {uri} should be []; got {j:?}"
        );
    }
}

#[tokio::test]
async fn target_policy_roundtrip_and_validation() {
    let (_db, pool) = setup_pg_db().await;
    // Seed the FK chain: allowed_installer → installation → repo → membership.
    for q in [
        "INSERT INTO allowed_installer (github_account_id, account_login, account_type) VALUES \
         (100,'acme','organization')",
        "INSERT INTO github_installation (id, github_account_id, account_login, account_type) \
         VALUES (200,100,'acme','organization')",
        "INSERT INTO github_repo (id, owner, name) VALUES (300,'acme','widgets')",
        "INSERT INTO github_installation_repo (github_installation_id, github_repo_id) VALUES \
         (200,300)",
    ] {
        sqlx::query(q)
            .execute(&pool)
            .await
            .unwrap();
    }
    let router = router_with(pool, "http://unused".into());

    // allow → enabled
    let (s, j) = send(
        &router,
        "POST",
        "/api/policies/target",
        Some("admintok"),
        Some(r#"{"install_id":200,"repo_id":300,"note":"t"}"#),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{j:?}");
    assert_eq!(j["is_enabled"], true);

    // list → one row
    let (s, j) =
        send(&router, "GET", "/api/policies/target?install_id=200", Some("readtok"), None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(j.as_array().unwrap().len(), 1);

    // disable → not enabled
    let (s, j) = send(
        &router,
        "POST",
        "/api/policies/target/disable",
        Some("admintok"),
        Some(r#"{"install_id":200,"repo_id":300}"#),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(j["is_enabled"], false);

    // validation: installer disable with neither login nor account_id → 400
    let (s, _) =
        send(&router, "POST", "/api/installers/disable", Some("admintok"), Some("{}")).await;
    assert_eq!(s, StatusCode::BAD_REQUEST);

    // validation: unknown role → 400
    let (s, _) = send(
        &router,
        "POST",
        "/api/roles",
        Some("admintok"),
        Some(r#"{"user_id":1,"install":200,"role":"wizard"}"#),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn allow_installer_resolves_login_via_github() {
    let (_db, pool) = setup_pg_db().await;
    let gh = mock_github().await;
    let router = router_with(pool, gh);

    let (s, j) = send(
        &router,
        "POST",
        "/api/installers",
        Some("admintok"),
        Some(r#"{"login":"acme","note":"n"}"#),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{j:?}");
    assert_eq!(j["account_id"], 4242);
    assert_eq!(j["account_type"], "organization");
    assert_eq!(j["is_enabled"], true);

    let (_s, list) = send(&router, "GET", "/api/installers", Some("readtok"), None).await;
    assert_eq!(list.as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn malformed_and_unknown_field_bodies_use_the_error_envelope() {
    // Auth + ApiJson extraction run before the handler touches the DB, so a
    // lazy pool is fine — these never query.
    let router = router_with(lazy_pool(), "http://unused".into());

    // Malformed JSON → 400 with the ApiError envelope (not axum's plain text).
    let (s, j) =
        send(&router, "POST", "/api/policies/target", Some("admintok"), Some("{ not json")).await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    assert_eq!(j["error"]["code"], "bad_request");

    // Unknown field (`nope`) → 400 via `deny_unknown_fields`.
    let (s, j) = send(
        &router,
        "POST",
        "/api/policies/target",
        Some("admintok"),
        Some(r#"{"install_id":1,"repo_id":2,"nope":3}"#),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    assert_eq!(j["error"]["code"], "bad_request");
}

// ─── sbgh-api Client ↔ server contract (Phase 5) ─────────────────────────
//
// The CLI is now a pure `sbgh_api::Client`. These drive the REAL router over
// a TCP listener via that client (not raw HTTP), pinning the request/response
// DTO round-trip and the error-envelope decoding the CLI depends on.

/// Serve the real `/api` router on a loopback port; abort the task on drop.
async fn spawn_api(pool: Pool, gh_api_base: String) -> (String, tokio::task::JoinHandle<()>) {
    let router = router_with(pool, gh_api_base);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    (format!("http://{addr}"), server)
}

#[tokio::test]
async fn api_client_round_trips_listings_and_installer() {
    use sbgh_api::{AllowInstallerRequest, Client, DisableInstallerRequest};

    let (_db, pool) = setup_pg_db().await;
    let gh = mock_github().await; // any /users/* → org id 4242 login "acme"
    let (base, server) = spawn_api(pool, gh).await;
    let client = Client::new(base, Some("admintok".to_string()));

    // Public health + read-scoped whoami (admin satisfies read).
    assert_eq!(
        client
            .health()
            .await
            .unwrap()
            .status,
        "ok"
    );
    assert_eq!(
        client
            .whoami()
            .await
            .unwrap()
            .scope,
        "admin"
    );

    // Every listing endpoint deserializes (empty on a fresh DB) — the
    // client↔server response-DTO contract across the whole read surface.
    assert!(
        client
            .list_installers()
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        client
            .list_repos()
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        client
            .list_target_policies(None)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        client
            .list_source_policies(None)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        client
            .list_triggers(None, None)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        client
            .list_users()
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        client
            .list_roles(None)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        client
            .list_installations()
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        client
            .list_jobs(None, None)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        client
            .list_webhooks(None, None, None)
            .await
            .unwrap()
            .is_empty()
    );

    // Write round-trip with server-side resolution (mock GitHub → org 4242).
    let v = client
        .allow_installer(&AllowInstallerRequest {
            login: "acme".into(),
            note: Some("n".into()),
        })
        .await
        .unwrap();
    assert_eq!(v.account_id, 4242);
    assert_eq!(v.login, "acme");
    assert!(v.is_enabled);

    let installers = client
        .list_installers()
        .await
        .unwrap();
    assert_eq!(installers.len(), 1);
    assert_eq!(installers[0].account_id, 4242);

    // Disable by id round-trips back to the view.
    let d = client
        .disable_installer(&DisableInstallerRequest {
            login: None,
            account_id: Some(4242),
        })
        .await
        .unwrap();
    assert!(!d.is_enabled);

    server.abort();
}

#[tokio::test]
async fn resolve_endpoint_happy_unknown_and_suspended() {
    let (_db, pool) = setup_pg_db().await;
    for q in [
        // FK chain: github_installation.github_account_id → allowed_installer.
        "INSERT INTO allowed_installer (github_account_id, account_login, account_type) VALUES \
         (100,'acme','organization')",
        "INSERT INTO github_installation (id, github_account_id, account_login, account_type) \
         VALUES (200,100,'acme','organization')",
        "INSERT INTO github_repo (id, owner, name) VALUES (300,'acme','widgets')",
        // A suspended install on a different account — resolved, then 409'd.
        "INSERT INTO allowed_installer (github_account_id, account_login, account_type) VALUES \
         (101,'paused','user')",
        "INSERT INTO github_installation (id, github_account_id, account_login, account_type, \
         suspended_at) VALUES (201,101,'paused','user', NOW())",
        "INSERT INTO github_repo (id, owner, name) VALUES (301,'paused','widgets')",
    ] {
        sqlx::query(q)
            .execute(&pool)
            .await
            .unwrap();
    }
    let router = router_with(pool, "http://unused".into());

    // Happy path — resolves install + repo; owner match is case-insensitive.
    let (s, j) =
        send(&router, "GET", "/api/resolve?owner=Acme&repo=widgets", Some("readtok"), None).await;
    assert_eq!(s, StatusCode::OK, "{j:?}");
    assert_eq!(j["install_id"], 200);
    assert_eq!(j["repo_id"], 300);
    assert_eq!(j["account_login"], "acme");
    assert_eq!(j["repo_owner"], "acme");
    assert_eq!(j["repo_name"], "widgets");

    // Unknown account → 404.
    let (s, _) =
        send(&router, "GET", "/api/resolve?owner=nope&repo=widgets", Some("readtok"), None).await;
    assert_eq!(s, StatusCode::NOT_FOUND);

    // Known account, unknown repo → 404.
    let (s, _) =
        send(&router, "GET", "/api/resolve?owner=acme&repo=ghost", Some("readtok"), None).await;
    assert_eq!(s, StatusCode::NOT_FOUND);

    // Suspended install → 409 (distinct from "no install").
    let (s, _) =
        send(&router, "GET", "/api/resolve?owner=paused&repo=widgets", Some("readtok"), None).await;
    assert_eq!(s, StatusCode::CONFLICT);

    // Read scope required: the ingest token is rejected.
    let (s, _) =
        send(&router, "GET", "/api/resolve?owner=acme&repo=widgets", Some("ingesttok"), None).await;
    assert_eq!(s, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn api_client_resolve_repo() {
    use sbgh_api::{Client, ClientError};

    let (_db, pool) = setup_pg_db().await;
    for q in [
        "INSERT INTO allowed_installer (github_account_id, account_login, account_type) VALUES \
         (100,'acme','organization')",
        "INSERT INTO github_installation (id, github_account_id, account_login, account_type) \
         VALUES (200,100,'acme','organization')",
        "INSERT INTO github_repo (id, owner, name) VALUES (300,'acme','widgets')",
    ] {
        sqlx::query(q)
            .execute(&pool)
            .await
            .unwrap();
    }
    let (base, server) = spawn_api(pool, "http://unused".into()).await;
    let client = Client::new(base, Some("admintok".to_string()));

    // Full DTO round-trip across the client↔server boundary.
    let r = client
        .resolve_repo("acme", "widgets")
        .await
        .unwrap();
    assert_eq!(r.install_id, 200);
    assert_eq!(r.repo_id, 300);
    assert_eq!(r.account_login, "acme");
    assert_eq!(r.repo_owner, "acme");
    assert_eq!(r.repo_name, "widgets");

    // Unknown repo surfaces as a typed 404 (what the CLI maps to a friendly
    // "install the App / open a PR first" message).
    let err = client
        .resolve_repo("acme", "ghost")
        .await
        .unwrap_err();
    match err {
        ClientError::Api { status, .. } => assert_eq!(status, 404),
        other => panic!("expected Api error, got {other:?}"),
    }

    server.abort();
}

#[tokio::test]
async fn api_client_surfaces_error_envelope() {
    use sbgh_api::{Client, ClientError, DisableInstallerRequest};

    let (_db, pool) = setup_pg_db().await;
    let (base, server) = spawn_api(pool, "http://unused".into()).await;
    let client = Client::new(base, Some("admintok".to_string()));

    // Neither login nor account_id → server 400, surfaced as a typed Api
    // error (the envelope the CLI relies on for friendly messages).
    let err = client
        .disable_installer(&DisableInstallerRequest::default())
        .await
        .unwrap_err();
    match err {
        ClientError::Api { status, .. } => assert_eq!(status, 400),
        other => panic!("expected Api error, got {other:?}"),
    }

    server.abort();
}
