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
    PostgresJobV2Store, PostgresPolicyStore, PostgresPullRequestStore, PostgresRepoStore,
    PostgresUserStore, PostgresWebhookInbox, setup_pg,
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
    BasicClassifier, CreateHandler, InstallationHandler, InstallationRepositoriesHandler,
    IssueCommentHandler, ProcessorConfig, PullRequestHandler, PushHandler, WebhookProcessor,
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
            "user": { "id": 99, "login": "alice", "type": "User" },
            "author_association": "MEMBER",
        },
        "issue": {
            "number": 1,
            "pull_request": pull_request,
        },
        "repository": { "full_name": "o/r" },
        "sender": { "id": 99, "login": "alice", "type": "User" },
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
    let policy_store = Arc::new(PostgresPolicyStore::new(pool.clone()));
    let user_store = Arc::new(PostgresUserStore::new(pool.clone()));
    let pull_request_store = Arc::new(PostgresPullRequestStore::new(pool.clone()));
    let job_v2_store = Arc::new(PostgresJobV2Store::new(pool.clone()));
    let classifier = BasicClassifier::builder()
        .with_handler(Arc::new(IssueCommentHandler::new(
            repo_store.clone(),
            policy_store.clone(),
            installation_store.clone(),
            user_store.clone(),
            pull_request_store.clone(),
            gh.clone(),
            job_v2_store.clone(),
        )))
        .with_handler(Arc::new(InstallationHandler::new(
            installation_store.clone(),
            repo_store.clone(),
            gh.clone(),
        )))
        .with_handler(Arc::new(InstallationRepositoriesHandler::new(
            repo_store.clone(),
            installation_store.clone(),
            policy_store.clone(),
            user_store.clone(),
            gh,
        )))
        .with_handler(Arc::new(PullRequestHandler::new(
            repo_store,
            policy_store.clone(),
            installation_store.clone(),
            user_store,
            pull_request_store,
        )))
        .with_handler(Arc::new(PushHandler::new(
            policy_store.clone(),
            installation_store.clone(),
            job_v2_store.clone(),
        )))
        .with_handler(Arc::new(CreateHandler::new(policy_store, installation_store, job_v2_store)))
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
async fn pipeline_classifies_pr_benchmark_as_enqueued_job() {
    // Slice 9: /benchmark on a PR with both policies enabled + an
    // authorized user CREATES a `pr_comment` job (+ webhook/user/PR
    // links + queued event) in one transaction and terminates as
    // `EnqueuedJob`. The handler fetches the PR via GH API to find the
    // base+head repo ids, so we stage a canned response on the FakeGitHub.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    // Identity rows for base + head (FK targets for the policies).
    sqlx::query(
        "INSERT INTO github_repo (id, owner, name) VALUES (10, 'o', 'r'), (20, 'alice', 'r')",
    )
    .execute(&pool)
    .await
    .unwrap();
    seed_allowed_org(&pool, 42, "octo-org").await;
    sqlx::query(
        "INSERT INTO github_installation (id, github_account_id, account_login, account_type) \
         VALUES (42, 42, 'octo-org', 'organization')",
    )
    .execute(&pool)
    .await
    .unwrap();
    // Membership for the target repo (FK target of target_repo_policy).
    sqlx::query(
        "INSERT INTO github_installation_repo (github_installation_id, github_repo_id) VALUES \
         (42, 10)",
    )
    .execute(&pool)
    .await
    .unwrap();
    // Both policies enabled.
    sqlx::query(
        "INSERT INTO target_repo_policy (github_installation_id, github_repo_id, is_enabled) \
         VALUES (42, 10, TRUE)",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO source_repo_policy (github_installation_id, github_repo_id, is_enabled) \
         VALUES (42, 20, TRUE)",
    )
    .execute(&pool)
    .await
    .unwrap();
    // Slice 6: /benchmark sender (alice, id=99 from
    // issue_comment_webhook) needs the trigger_pr_benchmark role on
    // the target install/repo for the new authz gate to accept.
    sqlx::query("INSERT INTO github_user (id, login, user_type) VALUES (99, 'alice', 'user')")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO github_user_role (github_user_id, github_installation_id, github_repo_id, \
         granted_role) VALUES (99, 42, 10, 'trigger_pr_benchmark')",
    )
    .execute(&pool)
    .await
    .unwrap();

    let ingest = PostgresIngestStore::new(pool.clone());
    ingest
        .ingest_webhook(&issue_comment_webhook("e2e-bench", "/benchmark run", true))
        .await
        .unwrap();

    let gh = Arc::new(FakeGitHub::new());
    // issue_comment_webhook hardcodes sender=alice (id=99). Use
    // set_pull_request_full so the PR author matches that user —
    // FakeGitHub's default uses id=42 / login=alice which would
    // collide with the seeded sender row on the lower(login) unique
    // index (slice 7 materialisation upserts the author).
    gh.set_pull_request_full(
        "o/r",
        1,
        sbgh_core::github::PullRequestSide {
            repo: sbgh_core::github::RepoRef {
                id: 10,
                owner: "o".into(),
                name: "r".into(),
            },
            sha: "basesha".into(),
            branch: "main".into(),
        },
        sbgh_core::github::PullRequestSide {
            repo: sbgh_core::github::RepoRef {
                id: 20,
                owner: "alice-fork".into(),
                name: "r".into(),
            },
            sha: "headsha".into(),
            branch: "feat".into(),
        },
        "e2e pr title",
        sbgh_core::github::PullRequestAuthor {
            id: 99,
            login: "alice".into(),
            account_type: sbgh_core::models::GithubAccountType::User,
        },
    );
    let processor = build_processor_with_gh(&pool, gh);
    processor
        .process_one()
        .await
        .unwrap();

    let (status, outcome) = read_row_status(&pool, "e2e-bench").await;
    assert_eq!(status, WebhookStatus::Processed);
    assert_eq!(outcome, Some(WebhookOutcome::EnqueuedJob));

    // Slice 7: confirm the PR row was materialised by the shared
    // helper invoked from IssueCommentHandler.
    let pr_title: String = sqlx::query_scalar(
        "SELECT title FROM github_pull_request WHERE target_github_repo_id = 10 AND pr_number = 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(pr_title, "e2e pr title");

    // Slice 9: exactly one job, against the TARGET repo, ref = PR head,
    // queued, with all links + the queued event committed atomically.
    let (job_id, job_repo, job_kind, trigger, ref_kind, ref_display, commit, job_status): (
        uuid::Uuid,
        i64,
        String,
        String,
        String,
        String,
        Option<String>,
        String,
    ) = sqlx::query_as(
        "SELECT id, github_repo_id, job_kind::text, trigger_kind::text, git_ref_kind::text, \
         git_ref_display, git_commit_hash, status::text FROM job",
    )
    .fetch_one(&pool)
    .await
    .expect("exactly one job row");
    assert_eq!(job_repo, 10, "job runs against the target (base) repo");
    assert_eq!(job_kind, "ad_hoc");
    assert_eq!(trigger, "pr_comment");
    assert_eq!(ref_kind, "branch");
    assert_eq!(ref_display, "feat", "PR head branch");
    assert_eq!(commit.as_deref(), Some("headsha"));
    assert_eq!(job_status, "queued");

    // Webhook link points back to the e2e-bench webhook row.
    let linked_webhook: i64 =
        sqlx::query_scalar("SELECT github_webhook_id FROM github_webhook_job WHERE job_id = $1")
            .bind(job_id)
            .fetch_one(&pool)
            .await
            .expect("webhook link exists");
    let webhook_row: i64 =
        sqlx::query_scalar("SELECT id FROM github_webhook WHERE delivery_id = 'e2e-bench'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(linked_webhook, webhook_row);

    // Owner link = the commenter (id=99); PR link = the materialised PR.
    let owner: i64 =
        sqlx::query_scalar("SELECT github_user_id FROM github_user_job WHERE job_id = $1")
            .bind(job_id)
            .fetch_one(&pool)
            .await
            .expect("user link exists");
    assert_eq!(owner, 99);
    let (pr_link_id, comment_id): (i64, Option<i64>) = sqlx::query_as(
        "SELECT github_pull_request_id, triggering_comment_id FROM github_pull_request_job WHERE \
         job_id = $1",
    )
    .bind(job_id)
    .fetch_one(&pool)
    .await
    .expect("PR link exists");
    assert!(pr_link_id > 0);
    assert_eq!(comment_id, Some(1), "links the triggering comment id");

    // Exactly one queued job_event with pr_comment provenance.
    let (event_kind, detail): (String, Option<serde_json::Value>) =
        sqlx::query_as("SELECT event_kind::text, detail FROM job_event WHERE job_id = $1")
            .bind(job_id)
            .fetch_one(&pool)
            .await
            .expect("exactly one queued event");
    assert_eq!(event_kind, "queued");
    assert_eq!(
        detail
            .unwrap()
            .get("trigger")
            .and_then(|v| v.as_str()),
        Some("pr_comment")
    );
}

#[tokio::test]
async fn pipeline_benchmark_without_role_grant_is_denied_unauthorized() {
    // Slice 6 e2e: same setup as the happy-path test above, EXCEPT no
    // `github_user_role` row is seeded. The outcome flips from
    // `WouldEnqueueJob` to `DeniedUnauthorized`. The user is still
    // upserted (audit trail for the denied attempt).
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    sqlx::query(
        "INSERT INTO github_repo (id, owner, name) VALUES (10, 'o', 'r'), (20, 'alice', 'r')",
    )
    .execute(&pool)
    .await
    .unwrap();
    seed_allowed_org(&pool, 42, "octo-org").await;
    sqlx::query(
        "INSERT INTO github_installation (id, github_account_id, account_login, account_type) \
         VALUES (42, 42, 'octo-org', 'organization')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO github_installation_repo (github_installation_id, github_repo_id) VALUES \
         (42, 10)",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO target_repo_policy (github_installation_id, github_repo_id, is_enabled) \
         VALUES (42, 10, TRUE)",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO source_repo_policy (github_installation_id, github_repo_id, is_enabled) \
         VALUES (42, 20, TRUE)",
    )
    .execute(&pool)
    .await
    .unwrap();
    // NO github_user_role row — alice (id=99) is unknown to the
    // authz table.

    let ingest = PostgresIngestStore::new(pool.clone());
    ingest
        .ingest_webhook(&issue_comment_webhook("e2e-unauth", "/benchmark run", true))
        .await
        .unwrap();

    let gh = Arc::new(FakeGitHub::new());
    // See the happy-path test for why we use set_pull_request_full
    // with author id=99 (matches the issue_comment sender; avoids
    // login collision on the lower(login) unique index).
    gh.set_pull_request_full(
        "o/r",
        1,
        sbgh_core::github::PullRequestSide {
            repo: sbgh_core::github::RepoRef {
                id: 10,
                owner: "o".into(),
                name: "r".into(),
            },
            sha: "basesha".into(),
            branch: "main".into(),
        },
        sbgh_core::github::PullRequestSide {
            repo: sbgh_core::github::RepoRef {
                id: 20,
                owner: "alice-fork".into(),
                name: "r".into(),
            },
            sha: "headsha".into(),
            branch: "feat".into(),
        },
        "e2e unauth pr title",
        sbgh_core::github::PullRequestAuthor {
            id: 99,
            login: "alice".into(),
            account_type: sbgh_core::models::GithubAccountType::User,
        },
    );
    let processor = build_processor_with_gh(&pool, gh);
    processor
        .process_one()
        .await
        .unwrap();

    let (status, outcome) = read_row_status(&pool, "e2e-unauth").await;
    assert_eq!(status, WebhookStatus::Denied);
    assert_eq!(outcome, Some(WebhookOutcome::DeniedUnauthorized));

    // Audit trail invariant: even denied attempts upsert the user.
    let upserted: Option<String> =
        sqlx::query_scalar("SELECT login FROM github_user WHERE id = 99")
            .fetch_optional(&pool)
            .await
            .unwrap();
    assert_eq!(upserted.as_deref(), Some("alice"));
}

#[tokio::test]
async fn pipeline_leaves_unregistered_event_types_in_received() {
    // Slice 2b high-finding-fix invariant, proven end-to-end against
    // real Postgres: rows for event types with no registered handler
    // stay `received` indefinitely.
    //
    // History: slice 2b used `installation`; slice 3 registered that.
    // Slice 4 used `push`; slice 5 registered that. After slice 5, the
    // handler-level allowlist (slice 1's SUPPORTED_EVENT_TYPES) and
    // the orchestrator handler set are the SAME. To still pin this
    // invariant, we directly seed an `unknown_event_type` row via the
    // IngestStore — which bypasses the handler allowlist — and assert
    // the processor doesn't claim it. Defense in depth.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    let ingest = PostgresIngestStore::new(pool.clone());
    ingest
        .ingest_webhook(&NewWebhook {
            delivery_id: "e2e-unknown".into(),
            event_type: "star".into(),
            action: Some("created".into()),
            payload_installation_id: Some(42),
            payload: Some(serde_json::json!({ "action": "created" })),
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
        "processor must NOT claim a row for an event type with no registered handler"
    );

    let (status, outcome) = read_row_status(&pool, "e2e-unknown").await;
    assert_eq!(
        status,
        WebhookStatus::Received,
        "unregistered-event row must remain `received` indefinitely"
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

// ─── Slice 5: pull_request / push / create policy evaluation ──────────

fn pull_request_webhook(
    delivery: &str,
    action: &str,
    install_id: i64,
    base_repo_id: i64,
    head_repo_id: i64,
) -> NewWebhook {
    let payload = serde_json::json!({
        "action": action,
        "installation": { "id": install_id },
        "repository": { "id": base_repo_id, "full_name": "o/r" },
        "pull_request": {
            "number": 1,
            "title": "test pr title",
            "user": { "id": 99, "login": "alice", "type": "User" },
            "head": {
                "ref": "feat",
                "sha": "headsha",
                "repo": { "id": head_repo_id, "full_name": "alice/r" }
            },
            "base": {
                "ref": "main",
                "sha": "basesha",
                "repo": { "id": base_repo_id, "full_name": "o/r" }
            }
        }
    });
    let size = serde_json::to_vec(&payload)
        .unwrap()
        .len() as i32;
    NewWebhook {
        delivery_id: delivery.into(),
        event_type: "pull_request".into(),
        action: Some(action.into()),
        payload_installation_id: Some(install_id),
        payload: Some(payload),
        payload_size_bytes: size,
    }
}

fn push_webhook(delivery: &str, install_id: i64, repo_id: i64, branch: &str) -> NewWebhook {
    let payload = serde_json::json!({
        "ref": format!("refs/heads/{branch}"),
        "installation": { "id": install_id },
        "repository": { "id": repo_id, "full_name": "o/r" },
        "head_commit": { "id": "e2epushsha", "timestamp": "2026-05-29T10:00:00Z" }
    });
    let size = serde_json::to_vec(&payload)
        .unwrap()
        .len() as i32;
    NewWebhook {
        delivery_id: delivery.into(),
        event_type: "push".into(),
        action: None,
        payload_installation_id: Some(install_id),
        payload: Some(payload),
        payload_size_bytes: size,
    }
}

fn create_tag_webhook(delivery: &str, install_id: i64, repo_id: i64, tag: &str) -> NewWebhook {
    let payload = serde_json::json!({
        "ref": tag,
        "ref_type": "tag",
        "installation": { "id": install_id },
        "repository": { "id": repo_id, "full_name": "o/r" }
    });
    let size = serde_json::to_vec(&payload)
        .unwrap()
        .len() as i32;
    NewWebhook {
        delivery_id: delivery.into(),
        event_type: "create".into(),
        action: None,
        payload_installation_id: Some(install_id),
        payload: Some(payload),
        payload_size_bytes: size,
    }
}

/// Seed install + base repo + head repo + membership for the base
/// (since target_repo_policy FKs to membership). Returns nothing; the
/// caller seeds the policies it wants.
async fn seed_install_with_base_and_head(
    pool: &Pool,
    install_id: i64,
    base_repo_id: i64,
    head_repo_id: i64,
) {
    seed_allowed_org(pool, 42, "octo-org").await;
    sqlx::query(
        "INSERT INTO github_installation (id, github_account_id, account_login, account_type) \
         VALUES ($1, 42, 'octo-org', 'organization')",
    )
    .bind(install_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO github_repo (id, owner, name) VALUES ($1, 'o', 'r'), ($2, 'alice', 'r')",
    )
    .bind(base_repo_id)
    .bind(head_repo_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO github_installation_repo (github_installation_id, github_repo_id) VALUES \
         ($1, $2)",
    )
    .bind(install_id)
    .bind(base_repo_id)
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn pipeline_pull_request_with_both_policies_enabled_is_processed_pull_request() {
    // Slice 9: a pull_request event with both policies enabled
    // materialises PR state but does NOT enqueue a job (no trigger_kind
    // for PR-event auto-bench). It terminates as `ProcessedPullRequest`,
    // and the `job` table stays empty.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_with_base_and_head(&pool, 100, 10, 20).await;
    sqlx::query(
        "INSERT INTO target_repo_policy (github_installation_id, github_repo_id, is_enabled) \
         VALUES (100, 10, TRUE)",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO source_repo_policy (github_installation_id, github_repo_id, is_enabled) \
         VALUES (100, 20, TRUE)",
    )
    .execute(&pool)
    .await
    .unwrap();

    let ingest = PostgresIngestStore::new(pool.clone());
    ingest
        .ingest_webhook(&pull_request_webhook("e2e-pr-ok", "opened", 100, 10, 20))
        .await
        .unwrap();

    let processor = build_processor(&pool);
    processor
        .process_one()
        .await
        .unwrap();

    let (status, outcome) = read_row_status(&pool, "e2e-pr-ok").await;
    assert_eq!(status, WebhookStatus::Processed);
    assert_eq!(outcome, Some(WebhookOutcome::ProcessedPullRequest));

    // No job created on the PR-event path.
    let job_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM job")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(job_count, 0, "pull_request events do not enqueue jobs in slice 9");
    // But the PR row was materialised so a later /benchmark can link it.
    let pr_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM github_pull_request WHERE target_github_repo_id = 10 AND pr_number \
         = 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(pr_count, 1, "PR row materialised");
}

#[tokio::test]
async fn pipeline_pull_request_target_denied_is_denied_target_policy() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_with_base_and_head(&pool, 100, 10, 20).await;
    // No target policy seeded → denied.
    let ingest = PostgresIngestStore::new(pool.clone());
    ingest
        .ingest_webhook(&pull_request_webhook("e2e-pr-deny", "opened", 100, 10, 20))
        .await
        .unwrap();

    let processor = build_processor(&pool);
    processor
        .process_one()
        .await
        .unwrap();

    let (status, outcome) = read_row_status(&pool, "e2e-pr-deny").await;
    assert_eq!(status, WebhookStatus::Denied);
    assert_eq!(outcome, Some(WebhookOutcome::DeniedTargetPolicy));
}

#[tokio::test]
async fn pipeline_pull_request_source_denied_is_denied_source_policy() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_with_base_and_head(&pool, 100, 10, 20).await;
    sqlx::query(
        "INSERT INTO target_repo_policy (github_installation_id, github_repo_id, is_enabled) \
         VALUES (100, 10, TRUE)",
    )
    .execute(&pool)
    .await
    .unwrap();
    // No source policy → denied.

    let ingest = PostgresIngestStore::new(pool.clone());
    ingest
        .ingest_webhook(&pull_request_webhook("e2e-pr-srcdeny", "opened", 100, 10, 20))
        .await
        .unwrap();

    let processor = build_processor(&pool);
    processor
        .process_one()
        .await
        .unwrap();

    let (_status, outcome) = read_row_status(&pool, "e2e-pr-srcdeny").await;
    assert_eq!(outcome, Some(WebhookOutcome::DeniedSourcePolicy));
}

#[tokio::test]
async fn pipeline_pull_request_non_trigger_action_is_ignored_action() {
    // `labeled` shouldn't trigger policy evaluation.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_with_base_and_head(&pool, 100, 10, 20).await;
    let ingest = PostgresIngestStore::new(pool.clone());
    ingest
        .ingest_webhook(&pull_request_webhook("e2e-pr-labeled", "labeled", 100, 10, 20))
        .await
        .unwrap();

    let processor = build_processor(&pool);
    processor
        .process_one()
        .await
        .unwrap();

    let (status, outcome) = read_row_status(&pool, "e2e-pr-labeled").await;
    assert_eq!(status, WebhookStatus::Ignored);
    assert_eq!(outcome, Some(WebhookOutcome::IgnoredAction));
}

#[tokio::test]
async fn pipeline_push_with_matching_branch_trigger_enqueues_baseline_job() {
    // Slice 9: a push to a watched branch creates a `baseline` job
    // (resolved commit from head_commit) + webhook link + queued event,
    // and terminates as `EnqueuedJob`. No PR/user link (automated trigger).
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_with_base_and_head(&pool, 100, 10, 20).await;
    sqlx::query(
        "INSERT INTO target_repo_policy (github_installation_id, github_repo_id, is_enabled) \
         VALUES (100, 10, TRUE)",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO trigger_policy (github_installation_id, github_repo_id, trigger_kind, \
         match_spec, is_enabled) VALUES (100, 10, 'branch_push', \
         '{\"kind\":\"branch_push\",\"branch_name\":\"develop\"}'::jsonb, TRUE)",
    )
    .execute(&pool)
    .await
    .unwrap();

    let ingest = PostgresIngestStore::new(pool.clone());
    ingest
        .ingest_webhook(&push_webhook("e2e-push-match", 100, 10, "develop"))
        .await
        .unwrap();

    let processor = build_processor(&pool);
    processor
        .process_one()
        .await
        .unwrap();

    let (status, outcome) = read_row_status(&pool, "e2e-push-match").await;
    assert_eq!(status, WebhookStatus::Processed);
    assert_eq!(outcome, Some(WebhookOutcome::EnqueuedJob));

    let (job_id, repo, kind, trigger, ref_display, commit): (
        uuid::Uuid,
        i64,
        String,
        String,
        String,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT id, github_repo_id, job_kind::text, trigger_kind::text, git_ref_display, \
         git_commit_hash FROM job",
    )
    .fetch_one(&pool)
    .await
    .expect("exactly one job");
    assert_eq!(repo, 10);
    assert_eq!(kind, "baseline");
    assert_eq!(trigger, "branch_push");
    assert_eq!(ref_display, "develop");
    assert_eq!(commit.as_deref(), Some("e2epushsha"), "resolved at enqueue");
    // Automated trigger: webhook link present, no user/PR link.
    let webhook_links: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM github_webhook_job WHERE job_id = $1")
            .bind(job_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(webhook_links, 1);
    let user_links: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM github_user_job WHERE job_id = $1")
            .bind(job_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(user_links, 0);
}

#[tokio::test]
async fn pipeline_push_with_no_matching_trigger_is_ignored_action() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_with_base_and_head(&pool, 100, 10, 20).await;
    sqlx::query(
        "INSERT INTO target_repo_policy (github_installation_id, github_repo_id, is_enabled) \
         VALUES (100, 10, TRUE)",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO trigger_policy (github_installation_id, github_repo_id, trigger_kind, \
         match_spec, is_enabled) VALUES (100, 10, 'branch_push', \
         '{\"kind\":\"branch_push\",\"branch_name\":\"develop\"}'::jsonb, TRUE)",
    )
    .execute(&pool)
    .await
    .unwrap();

    let ingest = PostgresIngestStore::new(pool.clone());
    // Push to a DIFFERENT branch → no trigger matches.
    ingest
        .ingest_webhook(&push_webhook("e2e-push-nomatch", 100, 10, "feature-x"))
        .await
        .unwrap();

    let processor = build_processor(&pool);
    processor
        .process_one()
        .await
        .unwrap();

    let (_status, outcome) = read_row_status(&pool, "e2e-push-nomatch").await;
    assert_eq!(outcome, Some(WebhookOutcome::IgnoredAction));
}

#[tokio::test]
async fn pipeline_create_tag_with_matching_pattern_trigger_enqueues_baseline_job() {
    // A matching tag creates a `baseline` job with an UNRESOLVED commit
    // (the create event has no SHA — the orchestrator resolves the tag
    // → commit at claim time) and terminates as `EnqueuedJob`.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_with_base_and_head(&pool, 100, 10, 20).await;
    sqlx::query(
        "INSERT INTO target_repo_policy (github_installation_id, github_repo_id, is_enabled) \
         VALUES (100, 10, TRUE)",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO trigger_policy (github_installation_id, github_repo_id, trigger_kind, \
         match_spec, is_enabled) VALUES (100, 10, 'tag_created', \
         '{\"kind\":\"tag_created\",\"tag_pattern\":\"^release/\\\\d+\\\\.\\\\d+$\"}'::jsonb, \
         TRUE)",
    )
    .execute(&pool)
    .await
    .unwrap();

    let ingest = PostgresIngestStore::new(pool.clone());
    ingest
        .ingest_webhook(&create_tag_webhook("e2e-tag-match", 100, 10, "release/1.2"))
        .await
        .unwrap();

    let processor = build_processor(&pool);
    processor
        .process_one()
        .await
        .unwrap();

    let (status, outcome) = read_row_status(&pool, "e2e-tag-match").await;
    assert_eq!(status, WebhookStatus::Processed);
    assert_eq!(outcome, Some(WebhookOutcome::EnqueuedJob));

    let (kind, trigger, ref_kind, ref_display, commit): (
        String,
        String,
        String,
        String,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT job_kind::text, trigger_kind::text, git_ref_kind::text, git_ref_display, \
         git_commit_hash FROM job",
    )
    .fetch_one(&pool)
    .await
    .expect("exactly one job");
    assert_eq!(kind, "baseline");
    assert_eq!(trigger, "tag_created");
    assert_eq!(ref_kind, "tag");
    assert_eq!(ref_display, "release/1.2");
    assert!(commit.is_none(), "tag job queued with unresolved commit");
}

#[tokio::test]
async fn pipeline_create_branch_is_ignored_no_trigger_eval() {
    // `create` for ref_type=branch should be silently skipped — those
    // events fire alongside an actual `push`, which is what we
    // evaluate triggers on.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_with_base_and_head(&pool, 100, 10, 20).await;
    let ingest = PostgresIngestStore::new(pool.clone());
    let payload = serde_json::json!({
        "ref": "new-feature",
        "ref_type": "branch",
        "installation": { "id": 100 },
        "repository": { "id": 10, "full_name": "o/r" }
    });
    let size = serde_json::to_vec(&payload)
        .unwrap()
        .len() as i32;
    ingest
        .ingest_webhook(&NewWebhook {
            delivery_id: "e2e-create-branch".into(),
            event_type: "create".into(),
            action: None,
            payload_installation_id: Some(100),
            payload: Some(payload),
            payload_size_bytes: size,
        })
        .await
        .unwrap();

    let processor = build_processor(&pool);
    processor
        .process_one()
        .await
        .unwrap();

    let (status, outcome) = read_row_status(&pool, "e2e-create-branch").await;
    assert_eq!(status, WebhookStatus::Ignored);
    assert_eq!(outcome, Some(WebhookOutcome::IgnoredAction));
}
