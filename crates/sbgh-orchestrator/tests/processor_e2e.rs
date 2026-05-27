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
    PostgresRepoStore, PostgresWebhookInbox, setup_pg,
};
use sbgh_core::github::RepoRef;
use sbgh_core::github::test_support::FakeGitHub;
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
    BasicClassifier, InstallationHandler, InstallationRepositoriesHandler, IssueCommentHandler,
    ProcessorConfig, WebhookProcessor,
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
    build_processor_with_gh(pool, Arc::new(FakeGitHub::new()))
}

/// Build the production processor with all slice 4 handlers registered,
/// using the caller-supplied GitHub API client. Slice 4's
/// `InstallationRepositoriesHandler` calls into the GH API for lineage
/// resolution; tests that exercise it stage canned responses on the
/// `FakeGitHub` first.
fn build_processor_with_gh(pool: &Pool, gh: Arc<FakeGitHub>) -> WebhookProcessor {
    let inbox = Arc::new(PostgresWebhookInbox::new(pool.clone()));
    let installation_store = Arc::new(PostgresInstallationStore::new(pool.clone()));
    let repo_store = Arc::new(PostgresRepoStore::new(pool.clone()));
    let classifier = BasicClassifier::builder()
        .with_handler(Arc::new(IssueCommentHandler))
        .with_handler(Arc::new(InstallationHandler::new(
            installation_store.clone(),
            repo_store.clone(),
            gh.clone(),
        )))
        .with_handler(Arc::new(InstallationRepositoriesHandler::new(
            repo_store,
            installation_store,
            gh,
        )))
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
async fn pipeline_installation_deleted_soft_deletes_install_row() {
    // Slice 4: soft-delete (sets deleted_at) instead of slice 3's hard
    // DELETE. The row stays so membership / future job FKs remain
    // valid.
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

    let deleted_at: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT deleted_at FROM github_installation WHERE id = $1")
            .bind(100_i64)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(deleted_at.is_some(), "soft-delete must set deleted_at");
}

// ─── Slice 4: installation_repositories pipeline ────────────────────────

fn repos_added_webhook(delivery: &str, install_id: i64, repos: &[(i64, &str)]) -> NewWebhook {
    let payload = serde_json::json!({
        "action": "added",
        "installation": {
            "id": install_id,
            "account": { "id": 42, "login": "octo-org", "type": "Organization" }
        },
        "repositories_added": repos
            .iter()
            .map(|(id, fname)| serde_json::json!({ "id": id, "full_name": fname }))
            .collect::<Vec<_>>(),
        "repositories_removed": [],
    });
    let size = serde_json::to_vec(&payload)
        .unwrap()
        .len() as i32;
    NewWebhook {
        delivery_id: delivery.into(),
        event_type: "installation_repositories".into(),
        action: Some("added".into()),
        payload_installation_id: Some(install_id),
        payload: Some(payload),
        payload_size_bytes: size,
    }
}

fn repos_removed_webhook(delivery: &str, install_id: i64, repos: &[(i64, &str)]) -> NewWebhook {
    let payload = serde_json::json!({
        "action": "removed",
        "installation": {
            "id": install_id,
            "account": { "id": 42, "login": "octo-org", "type": "Organization" }
        },
        "repositories_added": [],
        "repositories_removed": repos
            .iter()
            .map(|(id, fname)| serde_json::json!({ "id": id, "full_name": fname }))
            .collect::<Vec<_>>(),
    });
    let size = serde_json::to_vec(&payload)
        .unwrap()
        .len() as i32;
    NewWebhook {
        delivery_id: delivery.into(),
        event_type: "installation_repositories".into(),
        action: Some("removed".into()),
        payload_installation_id: Some(install_id),
        payload: Some(payload),
        payload_size_bytes: size,
    }
}

/// Seed the full slice-4 prerequisite chain: allowlist entry, install
/// row, supported_repo_root for the canonical repo. The caller stages
/// GitHub API responses on the returned FakeGitHub.
async fn seed_install_and_supported_root(
    pool: &Pool,
    install_id: i64,
    root_id: i64,
    root_owner: &str,
    root_name: &str,
) -> Arc<FakeGitHub> {
    seed_allowed_org(pool, 42, "octo-org").await;
    sqlx::query(
        "INSERT INTO github_installation (id, github_account_id, account_login, account_type) \
         VALUES ($1, 42, 'octo-org', 'organization')",
    )
    .bind(install_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO github_repo (id, owner, name) VALUES ($1, $2, $3)")
        .bind(root_id)
        .bind(root_owner)
        .bind(root_name)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO supported_repo_root (github_repo_id) VALUES ($1)")
        .bind(root_id)
        .execute(pool)
        .await
        .unwrap();
    let gh = Arc::new(FakeGitHub::new());
    gh.set_repo_canonical(root_owner, root_name, root_id);
    gh
}

#[tokio::test]
async fn pipeline_installation_repositories_added_creates_membership_for_supported_repo() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    let gh = seed_install_and_supported_root(&pool, 100, 10, "stacks-network", "stacks-core").await;
    let ingest = PostgresIngestStore::new(pool.clone());
    ingest
        .ingest_webhook(&repos_added_webhook(
            "e2e-repos-add",
            100,
            &[(10, "stacks-network/stacks-core")],
        ))
        .await
        .unwrap();

    let processor = build_processor_with_gh(&pool, gh);
    assert!(
        processor
            .process_one()
            .await
            .unwrap()
    );

    let (status, outcome) = read_row_status(&pool, "e2e-repos-add").await;
    assert_eq!(status, WebhookStatus::Processed);
    assert_eq!(outcome, Some(WebhookOutcome::ProcessedInstallation));

    let revoked_at: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
        "SELECT revoked_at FROM github_installation_repo WHERE github_installation_id = $1 AND \
         github_repo_id = $2",
    )
    .bind(100_i64)
    .bind(10_i64)
    .fetch_one(&pool)
    .await
    .expect("membership row must exist after ProcessedInstallation outcome");
    assert!(revoked_at.is_none(), "fresh membership must be active");
}

#[tokio::test]
async fn pipeline_installation_repositories_added_for_fork_walks_lineage() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    let gh = seed_install_and_supported_root(&pool, 100, 10, "stacks-network", "stacks-core").await;
    let root = RepoRef {
        id: 10,
        owner: "stacks-network".into(),
        name: "stacks-core".into(),
    };
    gh.set_repo_fork("alice", "stacks-core-fork", 20, root.clone(), root);

    let ingest = PostgresIngestStore::new(pool.clone());
    ingest
        .ingest_webhook(&repos_added_webhook(
            "e2e-fork-add",
            100,
            &[(20, "alice/stacks-core-fork")],
        ))
        .await
        .unwrap();

    let processor = build_processor_with_gh(&pool, gh);
    processor
        .process_one()
        .await
        .unwrap();

    let (_status, outcome) = read_row_status(&pool, "e2e-fork-add").await;
    assert_eq!(outcome, Some(WebhookOutcome::ProcessedInstallation));

    // Fork row inserted with fork_root pointing at the supported root.
    let fork_root: Option<i64> =
        sqlx::query_scalar("SELECT fork_root_github_repo_id FROM github_repo WHERE id = 20")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(fork_root, Some(10), "lineage walk must record fork_root");
    // Membership for the fork created.
    let m_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM github_installation_repo WHERE github_installation_id = 100 AND \
         github_repo_id = 20",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(m_count, 1);
}

#[tokio::test]
async fn pipeline_installation_repositories_added_for_unsupported_lineage_is_ignored() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    let gh = seed_install_and_supported_root(&pool, 100, 10, "stacks-network", "stacks-core").await;
    // Pre-program a totally unrelated canonical repo — NOT on the
    // supported list.
    gh.set_repo_canonical("randos", "unrelated", 99);

    let ingest = PostgresIngestStore::new(pool.clone());
    ingest
        .ingest_webhook(&repos_added_webhook("e2e-unsupported", 100, &[(99, "randos/unrelated")]))
        .await
        .unwrap();

    let processor = build_processor_with_gh(&pool, gh);
    processor
        .process_one()
        .await
        .unwrap();

    let (status, outcome) = read_row_status(&pool, "e2e-unsupported").await;
    assert_eq!(status, WebhookStatus::Ignored);
    assert_eq!(outcome, Some(WebhookOutcome::IgnoredUnsupportedLineage));

    // Repo identity STILL cached even though we don't grant membership.
    let repo_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM github_repo WHERE id = 99")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(repo_count, 1);
    let m_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM github_installation_repo WHERE github_installation_id = 100 AND \
         github_repo_id = 99",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(m_count, 0, "unsupported lineage must NOT create membership");
}

#[tokio::test]
async fn pipeline_installation_repositories_removed_revokes_membership() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    let gh = seed_install_and_supported_root(&pool, 100, 10, "stacks-network", "stacks-core").await;
    let ingest = PostgresIngestStore::new(pool.clone());
    // First add, then remove.
    ingest
        .ingest_webhook(&repos_added_webhook(
            "e2e-r-add",
            100,
            &[(10, "stacks-network/stacks-core")],
        ))
        .await
        .unwrap();
    ingest
        .ingest_webhook(&repos_removed_webhook(
            "e2e-r-rm",
            100,
            &[(10, "stacks-network/stacks-core")],
        ))
        .await
        .unwrap();

    let processor = build_processor_with_gh(&pool, gh);
    while processor
        .process_one()
        .await
        .unwrap()
    {}

    let (_status, outcome) = read_row_status(&pool, "e2e-r-rm").await;
    assert_eq!(outcome, Some(WebhookOutcome::ProcessedInstallation));
    let revoked_at: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
        "SELECT revoked_at FROM github_installation_repo WHERE github_installation_id = 100 AND \
         github_repo_id = 10",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(revoked_at.is_some(), "removed event must set revoked_at");
}

#[tokio::test]
async fn pipeline_installation_created_with_repositories_materialises_initial_memberships() {
    // Codex slice-4 high finding: an `installation.created` payload
    // with `repositories` MUST create initial memberships, end-to-end
    // against real Postgres.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_allowed_org(&pool, 42, "octo-org").await;
    // Seed a supported root the install will own.
    sqlx::query(
        "INSERT INTO github_repo (id, owner, name) VALUES (10, 'stacks-network', 'stacks-core')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO supported_repo_root (github_repo_id) VALUES (10)")
        .execute(&pool)
        .await
        .unwrap();
    let gh = Arc::new(FakeGitHub::new());
    gh.set_repo_canonical("stacks-network", "stacks-core", 10);

    let payload = serde_json::json!({
        "action": "created",
        "installation": {
            "id": 100,
            "account": { "id": 42, "login": "octo-org", "type": "Organization" }
        },
        "repositories": [
            { "id": 10, "full_name": "stacks-network/stacks-core" }
        ]
    });
    let size = serde_json::to_vec(&payload)
        .unwrap()
        .len() as i32;
    let ingest = PostgresIngestStore::new(pool.clone());
    ingest
        .ingest_webhook(&NewWebhook {
            delivery_id: "e2e-create-with-repos".into(),
            event_type: "installation".into(),
            action: Some("created".into()),
            payload_installation_id: Some(100),
            payload: Some(payload),
            payload_size_bytes: size,
        })
        .await
        .unwrap();

    let processor = build_processor_with_gh(&pool, gh);
    processor
        .process_one()
        .await
        .unwrap();

    let (_status, outcome) = read_row_status(&pool, "e2e-create-with-repos").await;
    assert_eq!(outcome, Some(WebhookOutcome::ProcessedInstallation));
    let m_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM github_installation_repo WHERE github_installation_id = 100 AND \
         github_repo_id = 10 AND revoked_at IS NULL",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        m_count, 1,
        "installation.created with `repositories` MUST materialise initial memberships"
    );
}

#[tokio::test]
async fn pipeline_repos_added_after_install_soft_deleted_is_ignored_unknown() {
    // Codex slice-4 M1 fix, end-to-end: install is created, then
    // deleted, then a delayed `installation_repositories.added` for
    // the same install arrives. The handler must return
    // IgnoredUnknownInstallation and NOT resurrect membership.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    let gh = seed_install_and_supported_root(&pool, 100, 10, "stacks-network", "stacks-core").await;
    let ingest = PostgresIngestStore::new(pool.clone());
    ingest
        .ingest_webhook(&installation_webhook("e2e-soft-del", "deleted", 100, 42))
        .await
        .unwrap();
    ingest
        .ingest_webhook(&repos_added_webhook(
            "e2e-stale-add",
            100,
            &[(10, "stacks-network/stacks-core")],
        ))
        .await
        .unwrap();

    let processor = build_processor_with_gh(&pool, gh);
    while processor
        .process_one()
        .await
        .unwrap()
    {}

    let (status, outcome) = read_row_status(&pool, "e2e-stale-add").await;
    assert_eq!(status, WebhookStatus::Ignored);
    assert_eq!(outcome, Some(WebhookOutcome::IgnoredUnknownInstallation));
    let m_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM github_installation_repo WHERE github_installation_id = 100 AND \
         revoked_at IS NULL",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        m_count, 0,
        "no active membership may exist after stale .added on soft-deleted install"
    );
}

#[tokio::test]
async fn pipeline_installation_deleted_revokes_all_memberships_transactionally() {
    // Slice 4 invariant: install.deleted soft-deletes the install AND
    // bulk-revokes every active membership. End-to-end against real
    // Postgres.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    let gh = seed_install_and_supported_root(&pool, 100, 10, "stacks-network", "stacks-core").await;
    let ingest = PostgresIngestStore::new(pool.clone());
    ingest
        .ingest_webhook(&repos_added_webhook(
            "e2e-md-add",
            100,
            &[(10, "stacks-network/stacks-core")],
        ))
        .await
        .unwrap();
    ingest
        .ingest_webhook(&installation_webhook("e2e-md-delete", "deleted", 100, 42))
        .await
        .unwrap();

    let processor = build_processor_with_gh(&pool, gh);
    while processor
        .process_one()
        .await
        .unwrap()
    {}

    let (_status, outcome) = read_row_status(&pool, "e2e-md-delete").await;
    assert_eq!(outcome, Some(WebhookOutcome::ProcessedInstallation));

    let deleted_at: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT deleted_at FROM github_installation WHERE id = 100")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(deleted_at.is_some(), "install row soft-deleted");

    let revoked_at: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
        "SELECT revoked_at FROM github_installation_repo WHERE github_installation_id = 100 AND \
         github_repo_id = 10",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        revoked_at.is_some(),
        "membership must be bulk-revoked in the same transaction as install soft-delete"
    );
}
