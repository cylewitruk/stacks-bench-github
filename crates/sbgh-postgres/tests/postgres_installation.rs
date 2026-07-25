//! Slice 3 integration tests for `PostgresInstallationStore` against a
//! real Postgres. Covers the allowlist-FK enforcement,
//! upsert semantics, suspend/unsuspend, deletion, and the
//! lookup-after-disable case the processor relies on.

use sbgh_core::models::GithubAccountType;
use sbgh_postgres::db::{
    InstallationStore, NewInstallation, PostgresInstallationStore, setup_pg_db,
};
use sqlx::AssertSqlSafe;

async fn seed_allowed(
    pool: &sbgh_postgres::db::Pool,
    account_id: i64,
    login: &str,
    is_enabled: bool,
) {
    sqlx::query(
        "INSERT INTO allowed_installer (github_account_id, account_login, account_type, \
         is_enabled) VALUES ($1, $2, 'organization', $3)",
    )
    .bind(account_id)
    .bind(login)
    .bind(is_enabled)
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn lookup_allowed_returns_disabled_rows_too() {
    // The processor relies on getting back is_enabled=FALSE rows so it
    // can take the deny path. If lookup_allowed silently filtered
    // disabled rows, a paused installer would be treated like a NEW
    // installer (potentially a different deny outcome later, or worse
    // — slipping past entirely if the deny was conflated with "missing").
    let (_db, pool) = setup_pg_db().await;
    seed_allowed(&pool, 42, "octo", false).await;
    let store = PostgresInstallationStore::new(pool);

    let row = store
        .lookup_allowed(42)
        .await
        .unwrap();
    let row = row.expect("disabled row must still be returned");
    assert!(!row.is_enabled, "disabled flag must survive the round-trip");
}

#[tokio::test]
async fn lookup_allowed_returns_none_for_unknown_account() {
    let (_db, pool) = setup_pg_db().await;
    let store = PostgresInstallationStore::new(pool);
    assert!(
        store
            .lookup_allowed(999)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn upsert_installation_fails_without_allowlist_row() {
    // FK from github_installation.github_account_id to
    // allowed_installer.github_account_id enforces "every install is
    // backed by a current allowlist row". The processor calls
    // lookup_allowed BEFORE upsert; this test verifies the DB also
    // enforces it as a backstop — if the lookup was somehow skipped,
    // the FK fails the insert.
    let (_db, pool) = setup_pg_db().await;
    let store = PostgresInstallationStore::new(pool);

    let result = store
        .upsert_installation(&NewInstallation {
            id: 100,
            github_account_id: 42,
            account_login: "octo".into(),
            account_type: GithubAccountType::Organization,
        })
        .await;
    assert!(
        result.is_err(),
        "FK to allowed_installer must reject installs whose account isn't allowlisted; got: \
         {result:?}"
    );
}

#[tokio::test]
async fn upsert_installation_updates_on_pk_conflict() {
    let (_db, pool) = setup_pg_db().await;
    seed_allowed(&pool, 42, "octo", true).await;
    let store = PostgresInstallationStore::new(pool);

    store
        .upsert_installation(&NewInstallation {
            id: 100,
            github_account_id: 42,
            account_login: "octo".into(),
            account_type: GithubAccountType::Organization,
        })
        .await
        .unwrap();
    // Second upsert with renamed login + flipped type.
    let updated = store
        .upsert_installation(&NewInstallation {
            id: 100,
            github_account_id: 42,
            account_login: "octo-org".into(),
            account_type: GithubAccountType::User,
        })
        .await
        .unwrap();
    assert_eq!(updated.account_login, "octo-org");
    assert_eq!(updated.account_type, GithubAccountType::User);
    assert!(
        updated.suspended_at.is_none(),
        "upsert must NOT clobber suspended_at — that's set_suspended's job"
    );
}

#[tokio::test]
async fn set_suspended_returns_none_for_unknown_install() {
    // suspend webhook for an install we never accepted is a no-op.
    let (_db, pool) = setup_pg_db().await;
    let store = PostgresInstallationStore::new(pool);

    let result = store
        .set_suspended(999, Some(chrono::Utc::now()))
        .await
        .unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn set_suspended_roundtrips_via_clear() {
    let (_db, pool) = setup_pg_db().await;
    seed_allowed(&pool, 42, "octo", true).await;
    let store = PostgresInstallationStore::new(pool);
    store
        .upsert_installation(&NewInstallation {
            id: 100,
            github_account_id: 42,
            account_login: "octo".into(),
            account_type: GithubAccountType::Organization,
        })
        .await
        .unwrap();

    let now = chrono::Utc::now();
    let suspended = store
        .set_suspended(100, Some(now))
        .await
        .unwrap()
        .unwrap();
    assert!(
        suspended
            .suspended_at
            .is_some()
    );

    let cleared = store
        .set_suspended(100, None)
        .await
        .unwrap()
        .unwrap();
    assert!(cleared.suspended_at.is_none(), "set_suspended(None) MUST clear the timestamp");
}

#[tokio::test]
async fn delete_installation_returns_install_not_found_for_unknown() {
    let (_db, pool) = setup_pg_db().await;
    let store = PostgresInstallationStore::new(pool);
    let outcome = store
        .delete_installation(999)
        .await
        .unwrap();
    assert!(!outcome.install_found);
    assert_eq!(outcome.memberships_revoked, 0);
}

#[tokio::test]
async fn delete_installation_soft_deletes_install_and_preserves_webhook_fk() {
    // Slice 4 changed slice 3's hard DELETE to a soft-delete (sets
    // deleted_at). The slice 3 ON DELETE SET NULL FK on
    // github_webhook.github_installation_id is now defensive-only —
    // verify that path stays untouched (the webhook row's FK should
    // STILL point at the install, since the install row didn't go
    // anywhere).
    let (_db, pool) = setup_pg_db().await;
    seed_allowed(&pool, 42, "octo", true).await;
    let store = PostgresInstallationStore::new(pool.clone());
    store
        .upsert_installation(&NewInstallation {
            id: 100,
            github_account_id: 42,
            account_login: "octo".into(),
            account_type: GithubAccountType::Organization,
        })
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO github_webhook
             (delivery_id, event_type, payload_size_bytes, github_installation_id)
         VALUES ('fk-test-1', 'installation', 0, 100)",
    )
    .execute(&pool)
    .await
    .unwrap();

    let outcome = store
        .delete_installation(100)
        .await
        .unwrap();
    assert!(outcome.install_found);
    assert_eq!(outcome.memberships_revoked, 0);

    // Install row: still present, deleted_at set.
    let install = store
        .upsert_installation(&NewInstallation {
            id: 100,
            github_account_id: 42,
            account_login: "octo".into(),
            account_type: GithubAccountType::Organization,
        })
        .await
        .unwrap();
    // The upsert returns the post-update row; deleted_at is set from
    // the prior call and the upsert deliberately doesn't clobber it.
    assert!(
        install.deleted_at.is_some(),
        "soft-delete must set deleted_at and upsert must not clobber it"
    );

    // Webhook FK still points at the (now soft-deleted) install — no
    // CASCADE happens.
    let resolved: Option<i64> = sqlx::query_scalar(
        "SELECT github_installation_id FROM github_webhook WHERE delivery_id = 'fk-test-1'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(resolved, Some(100), "soft-delete must NOT null out the webhook FK");
}

#[tokio::test]
async fn delete_installation_redelivery_is_idempotent() {
    let (_db, pool) = setup_pg_db().await;
    seed_allowed(&pool, 42, "octo", true).await;
    let store = PostgresInstallationStore::new(pool);
    store
        .upsert_installation(&NewInstallation {
            id: 100,
            github_account_id: 42,
            account_login: "octo".into(),
            account_type: GithubAccountType::Organization,
        })
        .await
        .unwrap();

    let first = store
        .delete_installation(100)
        .await
        .unwrap();
    assert!(first.install_found);

    let second = store
        .delete_installation(100)
        .await
        .unwrap();
    assert!(second.install_found);
    assert_eq!(second.memberships_revoked, 0, "re-delivery must NOT re-revoke anything");
}

#[tokio::test]
async fn delete_installation_disables_all_policy_rows_in_same_transaction() {
    // Slice 5: install.deleted must ALSO soft-disable every policy
    // row belonging to the install — target, source, trigger — in the
    // same transaction as the membership revoke + install soft-delete.
    // Otherwise a delete-then-recreate cycle would silently inherit
    // stale policies the operator didn't re-approve.
    let (_db, pool) = setup_pg_db().await;
    seed_allowed(&pool, 42, "octo", true).await;
    let store = PostgresInstallationStore::new(pool.clone());
    store
        .upsert_installation(&NewInstallation {
            id: 100,
            github_account_id: 42,
            account_login: "octo".into(),
            account_type: GithubAccountType::Organization,
        })
        .await
        .unwrap();
    sqlx::query("INSERT INTO github_repo (id, owner, name) VALUES (10, 'o', 'r')")
        .execute(&pool)
        .await
        .unwrap();
    store
        .add_or_restore_membership(100, 10)
        .await
        .unwrap();
    // Seed enabled policies of all three kinds for this install.
    sqlx::query(
        "INSERT INTO target_repo_policy (github_installation_id, github_repo_id, is_enabled) \
         VALUES (100, 10, TRUE)",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO source_repo_policy (github_installation_id, github_repo_id, is_enabled) \
         VALUES (100, 10, TRUE)",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO trigger_policy (github_installation_id, github_repo_id, trigger_kind, \
         match_spec, is_enabled) VALUES (100, 10, 'branch_push', \
         '{\"kind\":\"branch_push\",\"branch_name\":\"main\"}'::jsonb, TRUE)",
    )
    .execute(&pool)
    .await
    .unwrap();
    // Also seed a policy for a DIFFERENT install to verify the
    // delete doesn't scope-creep.
    sqlx::query(
        "INSERT INTO allowed_installer (github_account_id, account_login, account_type) VALUES \
         (43, 'other', 'organization')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO github_installation (id, github_account_id, account_login, account_type) \
         VALUES (200, 43, 'other', 'organization')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO github_installation_repo (github_installation_id, github_repo_id) VALUES \
         (200, 10)",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO target_repo_policy (github_installation_id, github_repo_id, is_enabled) \
         VALUES (200, 10, TRUE)",
    )
    .execute(&pool)
    .await
    .unwrap();

    store
        .delete_installation(100)
        .await
        .unwrap();

    // All three policy types for install 100 disabled.
    for table in ["target_repo_policy", "source_repo_policy", "trigger_policy"] {
        let q = format!(
            "SELECT COUNT(*) FROM {table} WHERE github_installation_id = 100 AND is_enabled = TRUE"
        );
        let count: i64 = sqlx::query_scalar(AssertSqlSafe(q))
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0, "{table} for install 100 must be all disabled");
    }
    // Install 200's policy is UNTOUCHED.
    let other_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM target_repo_policy WHERE github_installation_id = 200 AND \
         is_enabled = TRUE",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(other_count, 1, "policy for OTHER install must not be scope-creep-disabled");
}

#[tokio::test]
async fn delete_installation_soft_revokes_all_user_role_grants() {
    // Slice 6 third-pass review fix: install.deleted must ALSO
    // soft-revoke every active github_user_role row for the install
    // — both install-wide and repo-scoped grants — in the same
    // transaction. Otherwise, a delete-then-recreate cycle would
    // silently inherit stale role grants the operator didn't
    // re-approve.
    let (_db, pool) = setup_pg_db().await;
    seed_allowed(&pool, 42, "octo", true).await;
    let store = PostgresInstallationStore::new(pool.clone());
    store
        .upsert_installation(&NewInstallation {
            id: 100,
            github_account_id: 42,
            account_login: "octo".into(),
            account_type: GithubAccountType::Organization,
        })
        .await
        .unwrap();
    sqlx::query("INSERT INTO github_repo (id, owner, name) VALUES (10, 'o', 'r')")
        .execute(&pool)
        .await
        .unwrap();
    store
        .add_or_restore_membership(100, 10)
        .await
        .unwrap();
    sqlx::query("INSERT INTO github_user (id, login, user_type) VALUES (42, 'alice', 'user')")
        .execute(&pool)
        .await
        .unwrap();
    // Three active grants: install-wide, repo-scoped, plus a grant
    // on a DIFFERENT install (must not be scope-creeped).
    sqlx::query(
        "INSERT INTO github_user_role (github_user_id, github_installation_id, granted_role) \
         VALUES (42, 100, 'trigger_pr_benchmark')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO github_user_role (github_user_id, github_installation_id, github_repo_id, \
         granted_role) VALUES (42, 100, 10, 'admin')",
    )
    .execute(&pool)
    .await
    .unwrap();
    // Cross-install grant.
    sqlx::query(
        "INSERT INTO allowed_installer (github_account_id, account_login, account_type) VALUES \
         (43, 'other', 'organization')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO github_installation (id, github_account_id, account_login, account_type) \
         VALUES (200, 43, 'other', 'organization')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO github_user_role (github_user_id, github_installation_id, granted_role) \
         VALUES (42, 200, 'trigger_pr_benchmark')",
    )
    .execute(&pool)
    .await
    .unwrap();

    store
        .delete_installation(100)
        .await
        .unwrap();

    // Both grants on install=100 must be soft-revoked.
    let install_100_active: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM github_user_role WHERE github_installation_id = 100 AND revoked_at \
         IS NULL",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(install_100_active, 0, "all grants on deleted install must be soft-revoked");

    // BOTH grants must still exist (soft, not hard).
    let install_100_total: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM github_user_role WHERE github_installation_id = 100",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(install_100_total, 2, "soft-revoke must preserve audit rows");

    // Cross-install grant on install=200 must be UNTOUCHED.
    let install_200_active: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM github_user_role WHERE github_installation_id = 200 AND revoked_at \
         IS NULL",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(install_200_active, 1, "grant on OTHER install must survive");
}

#[tokio::test]
async fn delete_installation_bulk_revokes_active_memberships_transactionally() {
    // Slice 4 invariant: installation.deleted must soft-delete the
    // install AND revoke every active membership for it in one tx.
    // Already-revoked memberships are skipped (no re-stamp); never-
    // owned memberships of other installs are untouched.
    let (_db, pool) = setup_pg_db().await;
    seed_allowed(&pool, 42, "octo", true).await;
    let store = PostgresInstallationStore::new(pool.clone());
    store
        .upsert_installation(&NewInstallation {
            id: 100,
            github_account_id: 42,
            account_login: "octo".into(),
            account_type: GithubAccountType::Organization,
        })
        .await
        .unwrap();
    // Second install belonging to the same account, distinct memberships.
    store
        .upsert_installation(&NewInstallation {
            id: 200,
            github_account_id: 42,
            account_login: "octo".into(),
            account_type: GithubAccountType::Organization,
        })
        .await
        .unwrap();
    // Two repos exist as identity rows (no lineage needed for this test).
    sqlx::query(
        "INSERT INTO github_repo (id, owner, name) VALUES (10, 'o', 'r1'), (11, 'o', 'r2')",
    )
    .execute(&pool)
    .await
    .unwrap();
    // Membership matrix:
    //   install 100 → repo 10 (active), repo 11 (already revoked)
    //   install 200 → repo 10 (active, must NOT be revoked by install-100 delete)
    store
        .add_or_restore_membership(100, 10)
        .await
        .unwrap();
    store
        .add_or_restore_membership(100, 11)
        .await
        .unwrap();
    store
        .revoke_membership(100, 11)
        .await
        .unwrap();
    store
        .add_or_restore_membership(200, 10)
        .await
        .unwrap();

    let outcome = store
        .delete_installation(100)
        .await
        .unwrap();
    assert!(outcome.install_found);
    assert_eq!(
        outcome.memberships_revoked, 1,
        "only the one currently-active membership of install 100 should transition"
    );

    // Install 200's membership untouched.
    let cross_install_revoked: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
        "SELECT revoked_at FROM github_installation_repo WHERE github_installation_id = 200 AND \
         github_repo_id = 10",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(cross_install_revoked.is_none(), "membership for OTHER install must be untouched");
}

#[tokio::test]
async fn add_or_restore_membership_clears_revoked_at_and_preserves_granted_at() {
    let (_db, pool) = setup_pg_db().await;
    seed_allowed(&pool, 42, "octo", true).await;
    let store = PostgresInstallationStore::new(pool.clone());
    store
        .upsert_installation(&NewInstallation {
            id: 100,
            github_account_id: 42,
            account_login: "octo".into(),
            account_type: GithubAccountType::Organization,
        })
        .await
        .unwrap();
    sqlx::query("INSERT INTO github_repo (id, owner, name) VALUES (10, 'o', 'r1')")
        .execute(&pool)
        .await
        .unwrap();

    let first = store
        .add_or_restore_membership(100, 10)
        .await
        .unwrap()
        .expect("active install must return Some");
    let original_granted_at = first.granted_at;
    store
        .revoke_membership(100, 10)
        .await
        .unwrap();

    let restored = store
        .add_or_restore_membership(100, 10)
        .await
        .unwrap()
        .expect("active install must return Some");
    assert!(restored.revoked_at.is_none(), "restore must clear revoked_at");
    assert_eq!(
        restored.granted_at, original_granted_at,
        "restore must preserve the original granted_at (audit history)"
    );
}

#[tokio::test]
async fn revoke_membership_is_idempotent() {
    let (_db, pool) = setup_pg_db().await;
    seed_allowed(&pool, 42, "octo", true).await;
    let store = PostgresInstallationStore::new(pool.clone());
    store
        .upsert_installation(&NewInstallation {
            id: 100,
            github_account_id: 42,
            account_login: "octo".into(),
            account_type: GithubAccountType::Organization,
        })
        .await
        .unwrap();
    sqlx::query("INSERT INTO github_repo (id, owner, name) VALUES (10, 'o', 'r1')")
        .execute(&pool)
        .await
        .unwrap();
    store
        .add_or_restore_membership(100, 10)
        .await
        .unwrap();

    let first = store
        .revoke_membership(100, 10)
        .await
        .unwrap();
    assert!(first.is_some());
    let original_revoked_at = first
        .unwrap()
        .revoked_at
        .unwrap();

    let second = store
        .revoke_membership(100, 10)
        .await
        .unwrap();
    assert!(
        second.is_none(),
        "re-revoking an already-revoked membership must be Ok(None) — no re-stamp"
    );

    // Verify the original timestamp is preserved.
    let current: chrono::DateTime<chrono::Utc> = sqlx::query_scalar(
        "SELECT revoked_at FROM github_installation_repo WHERE github_installation_id = 100 AND \
         github_repo_id = 10",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(current, original_revoked_at);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn add_and_delete_race_never_leaves_orphan_active_membership() {
    // Codex follow-up review: the slice-4 fix using a plain probe in
    // `add_or_restore_membership` left a TOCTOU window — a concurrent
    // `delete_installation` could revoke + soft-delete BETWEEN the
    // probe and the membership UPSERT, leaving the inconsistent state
    // `(deleted_at IS NOT NULL, revoked_at IS NULL)`. The fix is that
    // BOTH paths now take `SELECT ... FOR UPDATE` on the install row,
    // serializing them at the row-lock layer.
    //
    // This test races the two operations many times and asserts the
    // invariant that after every iteration, EITHER:
    //   - the install was soft-deleted AND any membership is revoked, OR
    //   - the install is active AND the membership is active.
    // The "active + revoked" combinations are valid; the bad state is
    // "deleted_at IS NOT NULL AND revoked_at IS NULL" — that's what
    // the FOR UPDATE serialization prevents.
    let (_db, pool) = setup_pg_db().await;
    // Set up: allowed installer + repo identity. The install row is
    // re-created each iteration.
    sqlx::query(
        "INSERT INTO allowed_installer (github_account_id, account_login, account_type) VALUES \
         (42, 'octo', 'organization')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO github_repo (id, owner, name) VALUES (10, 'o', 'r1')")
        .execute(&pool)
        .await
        .unwrap();

    const ITERATIONS: usize = 50;
    for i in 0..ITERATIONS {
        let install_id = 1000 + (i as i64);
        // Fresh install per iteration so we don't have to worry about
        // cleaning up state between runs.
        sqlx::query(
            "INSERT INTO github_installation (id, github_account_id, account_login, account_type) \
             VALUES ($1, 42, 'octo', 'organization')",
        )
        .bind(install_id)
        .execute(&pool)
        .await
        .unwrap();

        let store_a = PostgresInstallationStore::new(pool.clone());
        let store_d = PostgresInstallationStore::new(pool.clone());
        let add_task = tokio::spawn(async move {
            store_a
                .add_or_restore_membership(install_id, 10)
                .await
        });
        let delete_task = tokio::spawn(async move {
            store_d
                .delete_installation(install_id)
                .await
        });
        let (add_res, delete_res) = tokio::join!(add_task, delete_task);
        let _ = add_res.unwrap().unwrap();
        let _ = delete_res.unwrap().unwrap();

        // The invariant: post-race state is consistent. "Membership
        // active" means a row EXISTS with revoked_at IS NULL — careful
        // not to use a LEFT JOIN here, because `NULL IS NULL` is TRUE
        // and a missing membership row would look like an "active"
        // one. EXISTS-with-predicate avoids that footgun.
        let install_deleted: bool = sqlx::query_scalar(
            "SELECT deleted_at IS NOT NULL FROM github_installation WHERE id = $1",
        )
        .bind(install_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        let active_membership: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT 1
                  FROM github_installation_repo
                 WHERE github_installation_id = $1
                   AND github_repo_id = 10
                   AND revoked_at IS NULL
            )
            "#,
        )
        .bind(install_id)
        .fetch_one(&pool)
        .await
        .unwrap();

        assert!(
            !(install_deleted && active_membership),
            "iteration {i}: install was soft-deleted but membership was still active — the FOR \
             UPDATE serialization is broken",
        );
    }
}

#[tokio::test]
async fn is_membership_active_returns_true_only_for_active_install_and_membership() {
    // Slice 5 review fix: this is the runtime gate used by every
    // policy-evaluating handler. Truth table:
    //   install active + membership active     → true
    //   install soft-deleted                   → false
    //   install suspended                      → false
    //   no install row                         → false
    //   no membership row                      → false
    //   membership revoked                     → false
    let (_db, pool) = setup_pg_db().await;
    seed_allowed(&pool, 42, "octo", true).await;
    let store = PostgresInstallationStore::new(pool.clone());
    store
        .upsert_installation(&NewInstallation {
            id: 100,
            github_account_id: 42,
            account_login: "octo".into(),
            account_type: GithubAccountType::Organization,
        })
        .await
        .unwrap();
    sqlx::query("INSERT INTO github_repo (id, owner, name) VALUES (10, 'o', 'r')")
        .execute(&pool)
        .await
        .unwrap();
    // No membership yet.
    assert!(
        !store
            .is_membership_active(100, 10)
            .await
            .unwrap()
    );

    store
        .add_or_restore_membership(100, 10)
        .await
        .unwrap();
    assert!(
        store
            .is_membership_active(100, 10)
            .await
            .unwrap(),
        "active install + active membership → true"
    );

    // Suspend the install.
    store
        .set_suspended(100, Some(chrono::Utc::now()))
        .await
        .unwrap();
    assert!(
        !store
            .is_membership_active(100, 10)
            .await
            .unwrap(),
        "suspended install → false"
    );

    // Unsuspend → true again.
    store
        .set_suspended(100, None)
        .await
        .unwrap();
    assert!(
        store
            .is_membership_active(100, 10)
            .await
            .unwrap()
    );

    // Revoke membership.
    store
        .revoke_membership(100, 10)
        .await
        .unwrap();
    assert!(
        !store
            .is_membership_active(100, 10)
            .await
            .unwrap(),
        "revoked membership → false"
    );

    // Restore membership.
    store
        .add_or_restore_membership(100, 10)
        .await
        .unwrap();
    assert!(
        store
            .is_membership_active(100, 10)
            .await
            .unwrap()
    );

    // Soft-delete the install.
    store
        .delete_installation(100)
        .await
        .unwrap();
    assert!(
        !store
            .is_membership_active(100, 10)
            .await
            .unwrap(),
        "soft-deleted install → false"
    );
}

#[tokio::test]
async fn is_membership_active_returns_false_for_unknown_pair() {
    let (_db, pool) = setup_pg_db().await;
    let store = PostgresInstallationStore::new(pool);
    assert!(
        !store
            .is_membership_active(999, 999)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn add_or_restore_membership_returns_none_for_soft_deleted_install() {
    // Codex slice-4 M1 fix: a delayed `installation_repositories.added`
    // arriving after `installation.deleted` must NOT restore membership
    // on a retired install. The store probes `deleted_at IS NULL`
    // in-tx and returns Ok(None) when the install is gone.
    let (_db, pool) = setup_pg_db().await;
    seed_allowed(&pool, 42, "octo", true).await;
    let store = PostgresInstallationStore::new(pool.clone());
    store
        .upsert_installation(&NewInstallation {
            id: 100,
            github_account_id: 42,
            account_login: "octo".into(),
            account_type: GithubAccountType::Organization,
        })
        .await
        .unwrap();
    sqlx::query("INSERT INTO github_repo (id, owner, name) VALUES (10, 'o', 'r1')")
        .execute(&pool)
        .await
        .unwrap();

    // Soft-delete the install.
    store
        .delete_installation(100)
        .await
        .unwrap();

    let result = store
        .add_or_restore_membership(100, 10)
        .await
        .unwrap();
    assert!(
        result.is_none(),
        "add_or_restore_membership MUST return None when install is soft-deleted"
    );

    // Verify no row was inserted in the membership table either.
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM github_installation_repo WHERE github_installation_id = 100 \
         AND github_repo_id = 10)",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(!exists, "no row should have been inserted");
}

#[tokio::test]
async fn add_or_restore_membership_returns_none_for_missing_install() {
    let (_db, pool) = setup_pg_db().await;
    let store = PostgresInstallationStore::new(pool);
    // Repo doesn't exist either, but the install guard short-circuits
    // before the FK check, so this is Ok(None) not a FK error.
    let result = store
        .add_or_restore_membership(999, 10)
        .await
        .unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn revoke_membership_returns_none_for_unknown_pair() {
    let (_db, pool) = setup_pg_db().await;
    let store = PostgresInstallationStore::new(pool);
    assert!(
        store
            .revoke_membership(999, 999)
            .await
            .unwrap()
            .is_none()
    );
}
