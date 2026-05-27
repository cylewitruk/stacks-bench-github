//! End-to-end integration tests for the inbox pipeline:
//! handler-shaped `IngestStore::ingest_webhook` insert → real
//! `WebhookProcessor::process_one` → terminal status/outcome in DB.
//!
//! This is the highest-confidence "the inbox pipeline actually works"
//! test we can write before slice 9's job creation lands. It exercises
//! the production Postgres impls (no in-memory stand-ins), the real
//! router-based BasicClassifier with its handlers, and the real
//! WebhookProcessor — all wired together against a fresh testcontainers
//! Postgres.

use std::sync::Arc;

use sbgh_core::db::{
    IngestStore, NewWebhook, Pool, PostgresIngestStore, PostgresInstallationStore,
    PostgresWebhookInbox, setup_pg,
};
use sbgh_core::models::{GithubAccountType, WebhookOutcome, WebhookStatus};

// Pull in the orchestrator's webhook_processor module via path include
// (orchestrator is a bin-only crate so its modules aren't normally
// reachable from tests). Same pattern the handler integration tests
// use for routes/mod.rs.
//
// allow(dead_code): the e2e tests exercise process_one + the router
// builder but not run() or NoopClassifier; from this test binary's POV
// some symbols are dead, but they're real production code in the main
// binary.
#[path = "../src/webhook_processor.rs"]
#[allow(dead_code)]
mod webhook_processor;

use webhook_processor::{
    BasicClassifier, InstallationHandler, IssueCommentHandler, ProcessorConfig, WebhookProcessor,
};

async fn read_row_status(pool: &Pool, delivery: &str) -> (WebhookStatus, Option<WebhookOutcome>) {
    sqlx::query_as("SELECT status, outcome FROM github_webhook WHERE delivery_id = $1")
        .bind(delivery)
        .fetch_one(pool)
        .await
        .expect("read row")
}

fn issue_comment_webhook(delivery: &str, body: &str, is_pr: bool) -> NewWebhook {
    let pull_request = if is_pr {
        serde_json::json!({ "url": "https://api.github.test/repos/o/r/pulls/1" })
    } else {
        serde_json::Value::Null
    };
    let payload = serde_json::json!({
        "action": "created",
        "comment": {
            "id": 1,
            "body": body,
            "user": { "login": "alice" },
            "author_association": "MEMBER",
        },
        "issue": {
            "number": 1,
            "pull_request": pull_request,
        },
        "repository": { "full_name": "o/r" },
        "sender": { "login": "alice" },
        "installation": { "id": 42 },
    });
    let size = serde_json::to_vec(&payload)
        .unwrap()
        .len() as i32;
    NewWebhook {
        delivery_id: delivery.into(),
        event_type: "issue_comment".into(),
        action: Some("created".into()),
        payload_installation_id: Some(42),
        payload: Some(payload),
        payload_size_bytes: size,
    }
}

fn installation_webhook(
    delivery: &str,
    action: &str,
    install_id: i64,
    account_id: i64,
) -> NewWebhook {
    let payload = serde_json::json!({
        "action": action,
        "installation": {
            "id": install_id,
            "account": {
                "id": account_id,
                "login": "octo-org",
                "type": "Organization",
            }
        }
    });
    let size = serde_json::to_vec(&payload)
        .unwrap()
        .len() as i32;
    NewWebhook {
        delivery_id: delivery.into(),
        event_type: "installation".into(),
        action: Some(action.into()),
        payload_installation_id: Some(install_id),
        payload: Some(payload),
        payload_size_bytes: size,
    }
}

fn build_processor(pool: &Pool) -> WebhookProcessor {
    let inbox = Arc::new(PostgresWebhookInbox::new(pool.clone()));
    let installation_store = Arc::new(PostgresInstallationStore::new(pool.clone()));
    let classifier = BasicClassifier::builder()
        .with_handler(Arc::new(IssueCommentHandler))
        .with_handler(Arc::new(InstallationHandler::new(installation_store)))
        .build();
    WebhookProcessor::new(inbox, Arc::new(classifier), ProcessorConfig::default())
}

async fn seed_allowed_org(pool: &Pool, account_id: i64, login: &str) {
    sqlx::query(
        "INSERT INTO allowed_installer (github_account_id, account_login, account_type) VALUES \
         ($1, $2, 'organization')",
    )
    .bind(account_id)
    .bind(login)
    .execute(pool)
    .await
    .expect("seed allowed_installer");
}

#[tokio::test]
async fn pipeline_classifies_pr_no_command_as_ignored_no_command() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    let ingest = PostgresIngestStore::new(pool.clone());
    ingest
        .ingest_webhook(&issue_comment_webhook("e2e-1", "looks great", true))
        .await
        .unwrap();

    let processor = build_processor(&pool);
    assert!(
        processor
            .process_one()
            .await
            .unwrap(),
        "processor must claim and classify the seeded row"
    );

    let (status, outcome) = read_row_status(&pool, "e2e-1").await;
    assert_eq!(status, WebhookStatus::Ignored);
    assert_eq!(outcome, Some(WebhookOutcome::IgnoredNoCommand));
}

#[tokio::test]
async fn pipeline_classifies_pr_benchmark_as_ignored_action_in_phase1() {
    // Slice 9 will change this to `enqueued_job` + create a `job` row.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    let ingest = PostgresIngestStore::new(pool.clone());
    ingest
        .ingest_webhook(&issue_comment_webhook("e2e-bench", "/benchmark run", true))
        .await
        .unwrap();

    let processor = build_processor(&pool);
    processor
        .process_one()
        .await
        .unwrap();

    let (status, outcome) = read_row_status(&pool, "e2e-bench").await;
    assert_eq!(status, WebhookStatus::Ignored);
    assert_eq!(outcome, Some(WebhookOutcome::IgnoredAction));
}

#[tokio::test]
async fn pipeline_leaves_unregistered_event_types_in_received() {
    // Slice 2b high-finding-fix invariant, proven end-to-end against
    // real Postgres: rows for event types with no registered handler
    // stay `received` for a future slice to consume.
    //
    // (Slice 2b used `installation` for this test, but slice 3 now
    // registers an InstallationHandler; `push` is the current
    // unregistered placeholder until slice 4-7 add it.)
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    let ingest = PostgresIngestStore::new(pool.clone());
    ingest
        .ingest_webhook(&NewWebhook {
            delivery_id: "e2e-push".into(),
            event_type: "push".into(),
            action: None,
            payload_installation_id: Some(42),
            payload: Some(serde_json::json!({ "ref": "refs/heads/main" })),
            payload_size_bytes: 24,
        })
        .await
        .unwrap();

    let processor = build_processor(&pool);
    assert!(
        !processor
            .process_one()
            .await
            .unwrap(),
        "processor must NOT claim a `push` row in slice 3"
    );

    let (status, outcome) = read_row_status(&pool, "e2e-push").await;
    assert_eq!(
        status,
        WebhookStatus::Received,
        "push row must remain `received` for a future slice's processor"
    );
    assert!(outcome.is_none());
}

#[tokio::test]
async fn pipeline_processes_multiple_rows_in_a_loop() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    let ingest = PostgresIngestStore::new(pool.clone());
    for i in 0..3 {
        ingest
            .ingest_webhook(&issue_comment_webhook(&format!("loop-{i}"), "nothing special", true))
            .await
            .unwrap();
    }

    let processor = build_processor(&pool);
    let mut processed = 0;
    while processor
        .process_one()
        .await
        .unwrap()
    {
        processed += 1;
    }
    assert_eq!(processed, 3);

    let terminal_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM github_webhook WHERE delivery_id LIKE 'loop-%' AND status = \
         'ignored' AND outcome = 'ignored_no_command'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(terminal_count, 3);
}

// ─── Slice 3: installation pipeline ────────────────────────────────────

#[tokio::test]
async fn pipeline_installation_created_for_allowed_account_materialises_install_row() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_allowed_org(&pool, 42, "octo-org").await;

    let ingest = PostgresIngestStore::new(pool.clone());
    ingest
        .ingest_webhook(&installation_webhook("e2e-inst-1", "created", 100, 42))
        .await
        .unwrap();

    let processor = build_processor(&pool);
    assert!(
        processor
            .process_one()
            .await
            .unwrap()
    );

    let (status, outcome) = read_row_status(&pool, "e2e-inst-1").await;
    assert_eq!(status, WebhookStatus::Processed);
    assert_eq!(outcome, Some(WebhookOutcome::ProcessedInstallation));

    // The install row must exist with the right account fields.
    let (login, account_type): (String, GithubAccountType) =
        sqlx::query_as("SELECT account_login, account_type FROM github_installation WHERE id = $1")
            .bind(100_i64)
            .fetch_one(&pool)
            .await
            .expect("install row must exist after processed_installation outcome");
    assert_eq!(login, "octo-org");
    assert_eq!(account_type, GithubAccountType::Organization);
}

#[tokio::test]
async fn pipeline_installation_created_for_unknown_account_is_denied() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    // Intentionally NO seed: account 42 is unknown to the allowlist.

    let ingest = PostgresIngestStore::new(pool.clone());
    ingest
        .ingest_webhook(&installation_webhook("e2e-inst-deny", "created", 100, 42))
        .await
        .unwrap();

    let processor = build_processor(&pool);
    processor
        .process_one()
        .await
        .unwrap();

    let (status, outcome) = read_row_status(&pool, "e2e-inst-deny").await;
    assert_eq!(status, WebhookStatus::Denied);
    assert_eq!(outcome, Some(WebhookOutcome::DeniedInstallAllowlist));

    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM github_installation WHERE id = $1)")
            .bind(100_i64)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(!exists, "denied install MUST NOT materialise a github_installation row");
}

#[tokio::test]
async fn pipeline_installation_suspend_sets_suspended_at() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_allowed_org(&pool, 42, "octo-org").await;
    let ingest = PostgresIngestStore::new(pool.clone());
    ingest
        .ingest_webhook(&installation_webhook("e2e-inst-create", "created", 100, 42))
        .await
        .unwrap();
    ingest
        .ingest_webhook(&installation_webhook("e2e-inst-suspend", "suspend", 100, 42))
        .await
        .unwrap();

    let processor = build_processor(&pool);
    while processor
        .process_one()
        .await
        .unwrap()
    {}

    let (_status, outcome) = read_row_status(&pool, "e2e-inst-suspend").await;
    assert_eq!(outcome, Some(WebhookOutcome::ProcessedInstallation));

    let suspended_at: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT suspended_at FROM github_installation WHERE id = $1")
            .bind(100_i64)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(suspended_at.is_some(), "suspend MUST set suspended_at");
}

#[tokio::test]
async fn pipeline_installation_deleted_removes_install_row() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_allowed_org(&pool, 42, "octo-org").await;
    let ingest = PostgresIngestStore::new(pool.clone());
    ingest
        .ingest_webhook(&installation_webhook("e2e-inst-create", "created", 100, 42))
        .await
        .unwrap();
    ingest
        .ingest_webhook(&installation_webhook("e2e-inst-delete", "deleted", 100, 42))
        .await
        .unwrap();

    let processor = build_processor(&pool);
    while processor
        .process_one()
        .await
        .unwrap()
    {}

    let (_status, outcome) = read_row_status(&pool, "e2e-inst-delete").await;
    assert_eq!(outcome, Some(WebhookOutcome::ProcessedInstallation));

    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM github_installation WHERE id = $1)")
            .bind(100_i64)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(!exists, "deleted install MUST remove github_installation row");
}
