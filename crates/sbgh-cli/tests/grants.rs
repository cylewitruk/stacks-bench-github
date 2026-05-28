//! Integration tests for the role/grant setup that `sbgh-cli migrate`
//! applies. Boots a fresh testcontainers Postgres, runs migrations,
//! calls `apply_roles()`, then connects AS the narrow roles and
//! asserts each can do what it's supposed to — and is REJECTED on
//! what it isn't.
//!
//! Pinning these matters because the deploy-time security boundary
//! is "compromised handler can't fabricate completed-job rows" and
//! "compromised orchestrator can't insert webhooks." Both rest on
//! the column-level GRANT specificity that's easy to drift on.

use sbgh_cli::apply_roles;
use sbgh_core::db::{self, Pool, setup_pg};
use sqlx::Row;

const HANDLER_PW: &str = "handler-test-pw";
const ORCH_PW: &str = "orch-test-pw";

async fn handler_pool(owner_pool: &Pool) -> Pool {
    // Derive the handler DSN by swapping the user. testcontainers
    // started us with postgres://postgres:postgres@... ; we want
    // postgres://sbgh_handler:HANDLER_PW@... pointed at the same DB.
    role_pool(owner_pool, "sbgh_handler", HANDLER_PW).await
}

async fn orch_pool(owner_pool: &Pool) -> Pool {
    role_pool(owner_pool, "sbgh_orch", ORCH_PW).await
}

async fn role_pool(owner_pool: &Pool, role: &str, password: &str) -> Pool {
    // Read the database name and host:port from the owner pool's
    // connection options, then build a new DSN with the narrow role.
    let opts = owner_pool.connect_options();
    let host = opts.get_host();
    let port = opts.get_port();
    let database = opts
        .get_database()
        .unwrap_or("postgres");
    let url = format!("postgres://{role}:{password}@{host}:{port}/{database}");
    db::connect(&url)
        .await
        .expect("connect as narrow role")
}

#[tokio::test]
async fn handler_can_insert_into_jobs_approved_columns() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    apply_roles(&pool, HANDLER_PW, ORCH_PW)
        .await
        .unwrap();
    let handler = handler_pool(&pool).await;

    // INSERT into the columns the grant approves — must succeed.
    let result = sqlx::query(
        "INSERT INTO jobs (repository, pr_number, head_sha, requested_by, command, args, \
         installation_id, github_delivery_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind("acme/widgets")
    .bind(1_i64)
    .bind("")
    .bind("alice")
    .bind("run")
    .bind(serde_json::json!({}))
    .bind(1_i64)
    .bind("grant-test-1")
    .execute(&handler)
    .await;
    assert!(result.is_ok(), "handler INSERT into approved columns must succeed: {result:?}");
}

#[tokio::test]
async fn handler_cannot_insert_status_column() {
    // The slice-1 security argument: a compromised handler must NOT
    // be able to fabricate a status='completed' row with a fake
    // result blob. The column grant intentionally omits status.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    apply_roles(&pool, HANDLER_PW, ORCH_PW)
        .await
        .unwrap();
    let handler = handler_pool(&pool).await;

    let result = sqlx::query(
        "INSERT INTO jobs (repository, pr_number, head_sha, requested_by, command, args, \
         installation_id, github_delivery_id, status) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, \
         'completed')",
    )
    .bind("acme/widgets")
    .bind(1_i64)
    .bind("")
    .bind("alice")
    .bind("run")
    .bind(serde_json::json!({}))
    .bind(1_i64)
    .bind("grant-test-status")
    .execute(&handler)
    .await;
    assert!(
        result.is_err(),
        "handler INSERT specifying `status` MUST be rejected (would let a compromised handler \
         fabricate completed rows); got: {result:?}"
    );
}

#[tokio::test]
async fn handler_cannot_select_jobs_head_sha() {
    // Handler may SELECT only id + github_delivery_id (needed for
    // ON CONFLICT RETURNING). Reading head_sha / args / result would
    // let a compromised handler enumerate other PRs' job content.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    apply_roles(&pool, HANDLER_PW, ORCH_PW)
        .await
        .unwrap();
    let handler = handler_pool(&pool).await;

    let result = sqlx::query("SELECT head_sha FROM jobs LIMIT 1")
        .fetch_optional(&handler)
        .await;
    assert!(result.is_err(), "handler SELECT head_sha MUST be rejected; got: {result:?}");
}

#[tokio::test]
async fn handler_can_insert_into_github_webhook_approved_columns() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    apply_roles(&pool, HANDLER_PW, ORCH_PW)
        .await
        .unwrap();
    let handler = handler_pool(&pool).await;

    // INSERT covering the slice-1 approved column set: delivery_id,
    // event_type, action, payload_installation_id, payload,
    // payload_size_bytes. Server-side DEFAULT fills id, status,
    // received_at, next_attempt_at, attempts.
    let result = sqlx::query(
        "INSERT INTO github_webhook (delivery_id, event_type, action, payload_installation_id, \
         payload, payload_size_bytes) VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind("webhook-grant-1")
    .bind("issue_comment")
    .bind(Some("created"))
    .bind(Some(1_i64))
    .bind(Some(serde_json::json!({})))
    .bind(2_i32)
    .execute(&handler)
    .await;
    assert!(
        result.is_ok(),
        "handler INSERT into approved webhook columns must succeed: {result:?}"
    );
}

#[tokio::test]
async fn handler_cannot_insert_status_into_github_webhook() {
    // Slice 1 column-grant invariant: a compromised handler must NOT
    // be able to mark a webhook as 'processed' or set an outcome.
    // Those columns are processor-only.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    apply_roles(&pool, HANDLER_PW, ORCH_PW)
        .await
        .unwrap();
    let handler = handler_pool(&pool).await;

    let result = sqlx::query(
        "INSERT INTO github_webhook (delivery_id, event_type, payload_size_bytes, status) VALUES \
         ($1, $2, $3, 'processed')",
    )
    .bind("webhook-grant-status")
    .bind("issue_comment")
    .bind(2_i32)
    .execute(&handler)
    .await;
    assert!(result.is_err(), "handler INSERT specifying status MUST be rejected; got: {result:?}");
}

#[tokio::test]
async fn handler_can_use_webhook_id_sequence() {
    // The BIGSERIAL id column needs the implicit sequence USAGE grant
    // (separately from the table grants) for the server-side default
    // to fire on handler INSERTs. Slice 1 grants this explicitly.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    apply_roles(&pool, HANDLER_PW, ORCH_PW)
        .await
        .unwrap();
    let handler = handler_pool(&pool).await;

    let row = sqlx::query(
        "INSERT INTO github_webhook (delivery_id, event_type, payload_size_bytes) VALUES ($1, $2, \
         $3) RETURNING id",
    )
    .bind("seq-grant-1")
    .bind("issue_comment")
    .bind(0_i32)
    .fetch_one(&handler)
    .await
    .expect("handler must be able to use github_webhook_id_seq");
    let id: i64 = row.get("id");
    assert!(id > 0);
}

#[tokio::test]
async fn orch_can_select_and_update_jobs() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    apply_roles(&pool, HANDLER_PW, ORCH_PW)
        .await
        .unwrap();
    // Seed a row as owner so orch has something to read.
    sqlx::query(
        "INSERT INTO jobs (repository, pr_number, head_sha, requested_by, command, args, \
         installation_id) VALUES ('a/b', 1, 'sha', 'alice', 'run', '{}'::jsonb, 1)",
    )
    .execute(&pool)
    .await
    .unwrap();

    let orch = orch_pool(&pool).await;
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM jobs")
        .fetch_one(&orch)
        .await
        .expect("orch SELECT must succeed");
    assert_eq!(count, 1);

    let result = sqlx::query("UPDATE jobs SET status = 'running'")
        .execute(&orch)
        .await;
    assert!(result.is_ok(), "orch UPDATE must succeed: {result:?}");
}

#[tokio::test]
async fn orch_cannot_insert_jobs() {
    // Orch reads + transitions but does NOT enqueue (handler's job).
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    apply_roles(&pool, HANDLER_PW, ORCH_PW)
        .await
        .unwrap();
    let orch = orch_pool(&pool).await;

    let result = sqlx::query(
        "INSERT INTO jobs (repository, pr_number, head_sha, requested_by, command, args, \
         installation_id) VALUES ('a/b', 1, 'sha', 'alice', 'run', '{}'::jsonb, 1)",
    )
    .execute(&orch)
    .await;
    assert!(
        result.is_err(),
        "orch INSERT into jobs MUST be rejected (handler owns enqueue); got: {result:?}"
    );
}

#[tokio::test]
async fn orch_can_select_and_update_github_webhook() {
    // Mirror of `orch_can_select_and_update_jobs` but for the slice 1
    // inbox table. This is the actual runtime path now that slice 2b
    // is wired in — the processor's claim loop is SELECT + UPDATE on
    // github_webhook all day.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    apply_roles(&pool, HANDLER_PW, ORCH_PW)
        .await
        .unwrap();
    // Seed a row as owner so orch has something to read + update.
    sqlx::query(
        "INSERT INTO github_webhook (delivery_id, event_type, payload_size_bytes) VALUES \
         ('orch-rw-1', 'issue_comment', 0)",
    )
    .execute(&pool)
    .await
    .unwrap();

    let orch = orch_pool(&pool).await;
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM github_webhook")
        .fetch_one(&orch)
        .await
        .expect("orch SELECT on github_webhook must succeed");
    assert_eq!(count, 1);

    // Real processor-shape UPDATE: flip a fresh row through to
    // `processing` like claim_next would.
    let result = sqlx::query(
        "UPDATE github_webhook SET status = 'processing', claimed_at = NOW(), claim_token = $1",
    )
    .bind(uuid::Uuid::new_v4())
    .execute(&orch)
    .await;
    assert!(result.is_ok(), "orch UPDATE on github_webhook must succeed: {result:?}");
}

#[tokio::test]
async fn orch_cannot_insert_github_webhook() {
    // The handler's job. A compromised orchestrator must NOT be able
    // to fabricate webhook rows (which could then be processed into
    // jobs the orch creates itself once slice 9 lands).
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    apply_roles(&pool, HANDLER_PW, ORCH_PW)
        .await
        .unwrap();
    let orch = orch_pool(&pool).await;

    let result = sqlx::query(
        "INSERT INTO github_webhook (delivery_id, event_type, payload_size_bytes) VALUES \
         ('orch-insert-1', 'issue_comment', 0)",
    )
    .execute(&orch)
    .await;
    assert!(
        result.is_err(),
        "orch INSERT into github_webhook MUST be rejected (handler owns inbox writes); got: \
         {result:?}"
    );
}

#[tokio::test]
async fn handler_cannot_select_webhook_payload() {
    // Handler may SELECT only id + delivery_id from github_webhook
    // (needed for the ON CONFLICT RETURNING flow). Reading payload or
    // status would let a compromised handler enumerate other tenants'
    // signed webhook bodies / processor decisions.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    apply_roles(&pool, HANDLER_PW, ORCH_PW)
        .await
        .unwrap();
    let handler = handler_pool(&pool).await;

    let payload_result = sqlx::query("SELECT payload FROM github_webhook LIMIT 1")
        .fetch_optional(&handler)
        .await;
    assert!(
        payload_result.is_err(),
        "handler SELECT payload MUST be rejected; got: {payload_result:?}"
    );

    let status_result = sqlx::query("SELECT status FROM github_webhook LIMIT 1")
        .fetch_optional(&handler)
        .await;
    assert!(
        status_result.is_err(),
        "handler SELECT status MUST be rejected; got: {status_result:?}"
    );
}

// ─── Slice 3: allowed_installer + github_installation grants ───────────

#[tokio::test]
async fn orch_can_select_allowed_installer() {
    // The processor's installation.created path SELECTs allowed_installer
    // to evaluate the allowlist. Grant must permit this.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    apply_roles(&pool, HANDLER_PW, ORCH_PW)
        .await
        .unwrap();
    // Seed as owner.
    sqlx::query(
        "INSERT INTO allowed_installer (github_account_id, account_login, account_type) VALUES \
         (42, 'octo', 'organization')",
    )
    .execute(&pool)
    .await
    .unwrap();

    let orch = orch_pool(&pool).await;
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM allowed_installer")
        .fetch_one(&orch)
        .await
        .expect("orch SELECT on allowed_installer must succeed");
    assert_eq!(count, 1);
}

#[tokio::test]
async fn orch_cannot_insert_or_update_allowed_installer() {
    // The allowlist is operator-curated. A compromised orchestrator
    // must NOT be able to add itself to the allowlist or flip an
    // existing row to is_enabled=TRUE.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    apply_roles(&pool, HANDLER_PW, ORCH_PW)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO allowed_installer (github_account_id, account_login, account_type, \
         is_enabled) VALUES (42, 'octo', 'organization', FALSE)",
    )
    .execute(&pool)
    .await
    .unwrap();

    let orch = orch_pool(&pool).await;
    let insert_result = sqlx::query(
        "INSERT INTO allowed_installer (github_account_id, account_login, account_type) VALUES \
         (99, 'evil', 'user')",
    )
    .execute(&orch)
    .await;
    assert!(insert_result.is_err(), "orch INSERT on allowed_installer MUST be rejected");

    let update_result =
        sqlx::query("UPDATE allowed_installer SET is_enabled = TRUE WHERE github_account_id = 42")
            .execute(&orch)
            .await;
    assert!(update_result.is_err(), "orch UPDATE on allowed_installer MUST be rejected");
}

#[tokio::test]
async fn handler_cannot_touch_allowed_installer() {
    // Handler has no business knowing the allowlist exists.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    apply_roles(&pool, HANDLER_PW, ORCH_PW)
        .await
        .unwrap();
    let handler = handler_pool(&pool).await;

    let select_result = sqlx::query("SELECT 1 FROM allowed_installer LIMIT 1")
        .fetch_optional(&handler)
        .await;
    assert!(select_result.is_err(), "handler SELECT on allowed_installer MUST be rejected");
}

#[tokio::test]
async fn orch_can_select_insert_update_github_installation_but_not_delete() {
    // Slice 4 changed install.deleted from hard-DELETE to soft-delete
    // (UPDATE sets deleted_at). DELETE is intentionally NOT granted —
    // a compromised processor must not be able to nuke history that
    // slice 5+ policy + slice 8+ job FKs depend on.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    apply_roles(&pool, HANDLER_PW, ORCH_PW)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO allowed_installer (github_account_id, account_login, account_type) VALUES \
         (42, 'octo', 'organization')",
    )
    .execute(&pool)
    .await
    .unwrap();

    let orch = orch_pool(&pool).await;
    sqlx::query(
        "INSERT INTO github_installation (id, github_account_id, account_login, account_type) \
         VALUES (100, 42, 'octo', 'organization')",
    )
    .execute(&orch)
    .await
    .expect("orch INSERT on github_installation must succeed");
    sqlx::query("UPDATE github_installation SET suspended_at = NOW() WHERE id = 100")
        .execute(&orch)
        .await
        .expect("orch UPDATE on github_installation must succeed");
    sqlx::query("UPDATE github_installation SET deleted_at = NOW() WHERE id = 100")
        .execute(&orch)
        .await
        .expect("orch UPDATE deleted_at (the slice 4 soft-delete) must succeed");
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM github_installation")
        .fetch_one(&orch)
        .await
        .expect("orch SELECT on github_installation must succeed");
    assert_eq!(count, 1);

    let delete_result = sqlx::query("DELETE FROM github_installation WHERE id = 100")
        .execute(&orch)
        .await;
    assert!(
        delete_result.is_err(),
        "orch DELETE on github_installation MUST be rejected — slice 4 switched to soft-delete to \
         preserve historical FK targets"
    );
}

#[tokio::test]
async fn handler_cannot_touch_github_installation() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    apply_roles(&pool, HANDLER_PW, ORCH_PW)
        .await
        .unwrap();
    let handler = handler_pool(&pool).await;

    let select_result = sqlx::query("SELECT 1 FROM github_installation LIMIT 1")
        .fetch_optional(&handler)
        .await;
    assert!(select_result.is_err(), "handler SELECT on github_installation MUST be rejected");
}

// ─── Slice 4: github_repo + supported_repo_root + github_installation_repo

#[tokio::test]
async fn orch_can_select_insert_update_github_repo() {
    // Processor inserts identity + lineage rows; UPDATE refreshes mutable
    // fields on later encounters. NO DELETE — repo identity is forever.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    apply_roles(&pool, HANDLER_PW, ORCH_PW)
        .await
        .unwrap();
    let orch = orch_pool(&pool).await;

    sqlx::query("INSERT INTO github_repo (id, owner, name) VALUES (10, 'o', 'r1')")
        .execute(&orch)
        .await
        .expect("orch INSERT on github_repo must succeed");
    sqlx::query("UPDATE github_repo SET default_branch = 'main' WHERE id = 10")
        .execute(&orch)
        .await
        .expect("orch UPDATE on github_repo must succeed");
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM github_repo")
        .fetch_one(&orch)
        .await
        .expect("orch SELECT on github_repo must succeed");
    assert_eq!(count, 1);
}

#[tokio::test]
async fn orch_can_select_supported_repo_root_but_not_insert_or_update() {
    // supported_repo_root is operator-curated. Compromised processor
    // must NOT be able to allowlist a new repo family.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    apply_roles(&pool, HANDLER_PW, ORCH_PW)
        .await
        .unwrap();
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

    let orch = orch_pool(&pool).await;
    // SELECT must succeed (processor reads the gate).
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM supported_repo_root")
        .fetch_one(&orch)
        .await
        .expect("orch SELECT on supported_repo_root must succeed");
    assert_eq!(count, 1);

    // INSERT must be rejected.
    sqlx::query(
        "INSERT INTO github_repo (id, owner, name) VALUES (99, 'evil', 'new'); INSERT INTO \
         supported_repo_root (github_repo_id) VALUES (99)",
    )
    .execute(&orch)
    .await
    .expect_err("orch INSERT on supported_repo_root MUST be rejected");

    // UPDATE must be rejected too — orch can't flip is_enabled.
    sqlx::query("UPDATE supported_repo_root SET is_enabled = FALSE WHERE github_repo_id = 10")
        .execute(&orch)
        .await
        .expect_err("orch UPDATE on supported_repo_root MUST be rejected");
}

#[tokio::test]
async fn handler_cannot_touch_supported_repo_root_or_github_repo() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    apply_roles(&pool, HANDLER_PW, ORCH_PW)
        .await
        .unwrap();
    let handler = handler_pool(&pool).await;

    sqlx::query("SELECT 1 FROM supported_repo_root LIMIT 1")
        .fetch_optional(&handler)
        .await
        .expect_err("handler SELECT on supported_repo_root MUST be rejected");
    sqlx::query("SELECT 1 FROM github_repo LIMIT 1")
        .fetch_optional(&handler)
        .await
        .expect_err("handler SELECT on github_repo MUST be rejected");
}

#[tokio::test]
async fn orch_can_select_insert_update_github_installation_repo() {
    // INSERT on installation_repositories.added, UPDATE revoked_at on
    // .removed + bulk on installation.deleted. NO DELETE — membership
    // history is permanent.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    apply_roles(&pool, HANDLER_PW, ORCH_PW)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO allowed_installer (github_account_id, account_login, account_type) VALUES \
         (42, 'octo', 'organization')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO github_installation (id, github_account_id, account_login, account_type) \
         VALUES (100, 42, 'octo', 'organization')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO github_repo (id, owner, name) VALUES (10, 'o', 'r1')")
        .execute(&pool)
        .await
        .unwrap();

    let orch = orch_pool(&pool).await;
    sqlx::query(
        "INSERT INTO github_installation_repo (github_installation_id, github_repo_id) VALUES \
         (100, 10)",
    )
    .execute(&orch)
    .await
    .expect("orch INSERT on github_installation_repo must succeed");
    sqlx::query(
        "UPDATE github_installation_repo SET revoked_at = NOW() WHERE github_installation_id = \
         100 AND github_repo_id = 10",
    )
    .execute(&orch)
    .await
    .expect("orch UPDATE on github_installation_repo must succeed");

    let delete_result =
        sqlx::query("DELETE FROM github_installation_repo WHERE github_installation_id = 100")
            .execute(&orch)
            .await;
    assert!(
        delete_result.is_err(),
        "orch DELETE on github_installation_repo MUST be rejected (membership history is \
         permanent — preserved for slice 5+ policy + slice 8+ job FKs)"
    );
}

#[tokio::test]
async fn handler_cannot_touch_github_installation_repo() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    apply_roles(&pool, HANDLER_PW, ORCH_PW)
        .await
        .unwrap();
    let handler = handler_pool(&pool).await;
    sqlx::query("SELECT 1 FROM github_installation_repo LIMIT 1")
        .fetch_optional(&handler)
        .await
        .expect_err("handler SELECT on github_installation_repo MUST be rejected");
}

// ─── Slice 5: target_repo_policy + source_repo_policy + trigger_policy ──

async fn seed_install_repo(pool: &Pool, install_id: i64, repo_id: i64) {
    // Seed allowlist + install + repo + membership so policy FKs are
    // satisfiable.
    sqlx::query(
        "INSERT INTO allowed_installer (github_account_id, account_login, account_type) VALUES \
         ($1, 'octo', 'organization') ON CONFLICT DO NOTHING",
    )
    .bind(install_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO github_installation (id, github_account_id, account_login, account_type) \
         VALUES ($1, $1, 'octo', 'organization') ON CONFLICT DO NOTHING",
    )
    .bind(install_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO github_repo (id, owner, name) VALUES ($1, 'o', 'r') ON CONFLICT DO NOTHING",
    )
    .bind(repo_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO github_installation_repo (github_installation_id, github_repo_id) VALUES \
         ($1, $2) ON CONFLICT DO NOTHING",
    )
    .bind(install_id)
    .bind(repo_id)
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn orch_can_select_and_update_target_repo_policy_but_not_insert_or_delete() {
    // Operator-curated: CLI (owner) inserts, processor only SELECTs +
    // UPDATEs (for the install.deleted bulk-disable path). No INSERT
    // → compromised processor can't add itself a new policy. No
    // DELETE → policy history is permanent.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    apply_roles(&pool, HANDLER_PW, ORCH_PW)
        .await
        .unwrap();
    seed_install_repo(&pool, 100, 10).await;
    // Seed a row as owner.
    sqlx::query(
        "INSERT INTO target_repo_policy (github_installation_id, github_repo_id, is_enabled) \
         VALUES (100, 10, TRUE)",
    )
    .execute(&pool)
    .await
    .unwrap();

    let orch = orch_pool(&pool).await;
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM target_repo_policy")
        .fetch_one(&orch)
        .await
        .expect("orch SELECT on target_repo_policy must succeed");
    assert_eq!(count, 1);
    // UPDATE must succeed (install.deleted bulk-disable path).
    sqlx::query(
        "UPDATE target_repo_policy SET is_enabled = FALSE WHERE github_installation_id = 100",
    )
    .execute(&orch)
    .await
    .expect("orch UPDATE on target_repo_policy must succeed");
    // INSERT must be rejected.
    sqlx::query(
        "INSERT INTO target_repo_policy (github_installation_id, github_repo_id) VALUES (100, 10)",
    )
    .execute(&orch)
    .await
    .expect_err("orch INSERT on target_repo_policy MUST be rejected");
    // DELETE must be rejected.
    sqlx::query("DELETE FROM target_repo_policy WHERE github_installation_id = 100")
        .execute(&orch)
        .await
        .expect_err("orch DELETE on target_repo_policy MUST be rejected");
}

#[tokio::test]
async fn orch_can_select_and_update_source_repo_policy_but_not_insert_or_delete() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    apply_roles(&pool, HANDLER_PW, ORCH_PW)
        .await
        .unwrap();
    seed_install_repo(&pool, 100, 10).await;
    sqlx::query(
        "INSERT INTO source_repo_policy (github_installation_id, github_repo_id, is_enabled) \
         VALUES (100, 10, TRUE)",
    )
    .execute(&pool)
    .await
    .unwrap();

    let orch = orch_pool(&pool).await;
    sqlx::query("SELECT COUNT(*) FROM source_repo_policy")
        .fetch_optional(&orch)
        .await
        .expect("orch SELECT on source_repo_policy must succeed");
    sqlx::query(
        "UPDATE source_repo_policy SET is_enabled = FALSE WHERE github_installation_id = 100",
    )
    .execute(&orch)
    .await
    .expect("orch UPDATE on source_repo_policy must succeed");
    sqlx::query(
        "INSERT INTO source_repo_policy (github_installation_id, github_repo_id) VALUES (100, 10)",
    )
    .execute(&orch)
    .await
    .expect_err("orch INSERT on source_repo_policy MUST be rejected");
}

#[tokio::test]
async fn orch_can_select_and_update_trigger_policy_but_not_insert_or_delete() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    apply_roles(&pool, HANDLER_PW, ORCH_PW)
        .await
        .unwrap();
    seed_install_repo(&pool, 100, 10).await;
    sqlx::query(
        "INSERT INTO target_repo_policy (github_installation_id, github_repo_id, is_enabled) \
         VALUES (100, 10, TRUE)",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO trigger_policy (github_installation_id, github_repo_id, trigger_kind, \
         match_spec) VALUES (100, 10, 'branch_push', \
         '{\"kind\":\"branch_push\",\"branch_name\":\"main\"}'::jsonb)",
    )
    .execute(&pool)
    .await
    .unwrap();

    let orch = orch_pool(&pool).await;
    sqlx::query("SELECT COUNT(*) FROM trigger_policy")
        .fetch_optional(&orch)
        .await
        .expect("orch SELECT on trigger_policy must succeed");
    sqlx::query("UPDATE trigger_policy SET is_enabled = FALSE WHERE github_installation_id = 100")
        .execute(&orch)
        .await
        .expect("orch UPDATE on trigger_policy must succeed");
    sqlx::query(
        "INSERT INTO trigger_policy (github_installation_id, github_repo_id, trigger_kind, \
         match_spec) VALUES (100, 10, 'branch_push', '{}'::jsonb)",
    )
    .execute(&orch)
    .await
    .expect_err("orch INSERT on trigger_policy MUST be rejected");
}

// ─── Slice 6: github_user + github_user_role grants ───────────────────

#[tokio::test]
async fn orch_can_select_insert_update_github_user_but_not_delete() {
    // Lazy upsert: orch INSERTs on first sighting and UPDATEs on PK
    // conflict (display field refresh). No DELETE — user identity
    // is forever (same rationale as github_repo).
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    apply_roles(&pool, HANDLER_PW, ORCH_PW)
        .await
        .unwrap();
    let orch = orch_pool(&pool).await;

    sqlx::query("INSERT INTO github_user (id, login, user_type) VALUES (42, 'alice', 'user')")
        .execute(&orch)
        .await
        .expect("orch INSERT on github_user must succeed (lazy upsert path)");
    sqlx::query("UPDATE github_user SET login = 'alice-renamed' WHERE id = 42")
        .execute(&orch)
        .await
        .expect("orch UPDATE on github_user must succeed (display refresh)");
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM github_user WHERE id = 42")
        .fetch_one(&orch)
        .await
        .expect("orch SELECT on github_user must succeed");
    assert_eq!(count, 1);
    sqlx::query("DELETE FROM github_user WHERE id = 42")
        .execute(&orch)
        .await
        .expect_err("orch DELETE on github_user MUST be rejected");
}

#[tokio::test]
async fn orch_can_select_github_user_role_but_not_write() {
    // Operator-curated: only the CLI (owner) may grant or revoke
    // roles. A compromised processor MUST NOT be able to grant
    // itself trigger_pr_benchmark.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    apply_roles(&pool, HANDLER_PW, ORCH_PW)
        .await
        .unwrap();
    seed_install_repo(&pool, 100, 10).await;
    // Seed as owner.
    sqlx::query("INSERT INTO github_user (id, login, user_type) VALUES (42, 'alice', 'user')")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO github_user_role (github_user_id, github_installation_id, granted_role) \
         VALUES (42, 100, 'trigger_pr_benchmark')",
    )
    .execute(&pool)
    .await
    .unwrap();

    let orch = orch_pool(&pool).await;
    // SELECT must succeed (processor's has_role path).
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM github_user_role")
        .fetch_one(&orch)
        .await
        .expect("orch SELECT on github_user_role must succeed");
    assert_eq!(count, 1);
    // INSERT must be rejected.
    sqlx::query(
        "INSERT INTO github_user_role (github_user_id, github_installation_id, granted_role) \
         VALUES (42, 100, 'admin')",
    )
    .execute(&orch)
    .await
    .expect_err("orch INSERT on github_user_role MUST be rejected");
    // UPDATE must be rejected.
    sqlx::query("UPDATE github_user_role SET granted_role = 'admin' WHERE github_user_id = 42")
        .execute(&orch)
        .await
        .expect_err("orch UPDATE on github_user_role MUST be rejected");
    // DELETE must be rejected.
    sqlx::query("DELETE FROM github_user_role WHERE github_user_id = 42")
        .execute(&orch)
        .await
        .expect_err("orch DELETE on github_user_role MUST be rejected");
}

#[tokio::test]
async fn handler_cannot_touch_user_or_role_tables() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    apply_roles(&pool, HANDLER_PW, ORCH_PW)
        .await
        .unwrap();
    let handler = handler_pool(&pool).await;
    for table in ["github_user", "github_user_role"] {
        let q = format!("SELECT 1 FROM {table} LIMIT 1");
        sqlx::query(&q)
            .fetch_optional(&handler)
            .await
            .expect_err(&format!("handler SELECT on {table} MUST be rejected"));
    }
}

#[tokio::test]
async fn handler_cannot_touch_any_policy_table() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    apply_roles(&pool, HANDLER_PW, ORCH_PW)
        .await
        .unwrap();
    let handler = handler_pool(&pool).await;
    for table in ["target_repo_policy", "source_repo_policy", "trigger_policy"] {
        let q = format!("SELECT 1 FROM {table} LIMIT 1");
        sqlx::query(&q)
            .fetch_optional(&handler)
            .await
            .expect_err(&format!("handler SELECT on {table} MUST be rejected"));
    }
}

#[tokio::test]
async fn apply_roles_is_idempotent() {
    // Re-running apply_roles MUST be safe — the deploy invokes it on
    // every container start. Verify the second call doesn't error,
    // and the resulting grants are unchanged.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    apply_roles(&pool, HANDLER_PW, ORCH_PW)
        .await
        .expect("first apply");
    apply_roles(&pool, HANDLER_PW, ORCH_PW)
        .await
        .expect("second apply must be idempotent");

    // Smoke: handler can still INSERT into approved columns.
    let handler = handler_pool(&pool).await;
    let result = sqlx::query(
        "INSERT INTO jobs (repository, pr_number, head_sha, requested_by, command, args, \
         installation_id, github_delivery_id) VALUES ('a/b', 1, 'sha', 'alice', 'run', \
         '{}'::jsonb, 1, 'idem-1')",
    )
    .execute(&handler)
    .await;
    assert!(result.is_ok(), "handler grants survive a second apply_roles call");
}
