//! Slice 5 integration tests for `PostgresPolicyStore` against a real
//! testcontainers Postgres. Covers all three policy tables — the
//! target/source upsert + soft-disable semantics, the FK-enforced
//! relationship from trigger_policy → target_repo_policy, and the
//! processor's hot-path `list_enabled_triggers` query.

use sbgh_core::db::{PolicyStore, Pool, PostgresPolicyStore, setup_pg};
use sbgh_core::models::{TriggerKind, TriggerMatchSpec};

/// Seed allowlist + install + repo + membership so the slice 5
/// policy FKs are all satisfiable.
async fn seed_install_repo(pool: &Pool, install_id: i64, repo_id: i64) {
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
async fn lookup_target_policy_returns_none_when_no_row() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    let store = PostgresPolicyStore::new(pool);
    assert!(
        store
            .lookup_target_policy(999, 999)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn upsert_target_policy_enforces_membership_fk() {
    // No `github_installation_repo` row for (100, 10) → FK violation.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    let store = PostgresPolicyStore::new(pool);
    let result = store
        .upsert_target_policy(100, 10, None)
        .await;
    assert!(
        result.is_err(),
        "FK from target_repo_policy → github_installation_repo must reject orphan inserts; got: \
         {result:?}"
    );
}

#[tokio::test]
async fn upsert_target_policy_then_disable_round_trip() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresPolicyStore::new(pool);

    let row = store
        .upsert_target_policy(100, 10, Some("operator note"))
        .await
        .unwrap();
    assert!(row.is_enabled);
    assert_eq!(row.note.as_deref(), Some("operator note"));

    let disabled = store
        .disable_target_policy(100, 10)
        .await
        .unwrap()
        .expect("row must exist");
    assert!(!disabled.is_enabled);

    // Re-enable via upsert — and verify note isn't clobbered when the
    // re-allow doesn't provide one.
    let re = store
        .upsert_target_policy(100, 10, None)
        .await
        .unwrap();
    assert!(re.is_enabled, "upsert re-enables");
    assert_eq!(
        re.note.as_deref(),
        Some("operator note"),
        "upsert without note preserves existing note"
    );
}

#[tokio::test]
async fn upsert_source_policy_does_not_require_membership() {
    // Source policy intentionally has no membership FK — sources can
    // be arbitrary forks the install doesn't own.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    // Need install + repo for the per-table FKs (just no membership).
    sqlx::query(
        "INSERT INTO allowed_installer (github_account_id, account_login, account_type) VALUES \
         (100, 'octo', 'organization')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO github_installation (id, github_account_id, account_login, account_type) \
         VALUES (100, 100, 'octo', 'organization')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO github_repo (id, owner, name) VALUES (20, 'alice', 'fork')")
        .execute(&pool)
        .await
        .unwrap();
    let store = PostgresPolicyStore::new(pool);

    let row = store
        .upsert_source_policy(100, 20, None)
        .await
        .unwrap();
    assert!(row.is_enabled);
}

#[tokio::test]
async fn add_trigger_policy_requires_target_fk() {
    // FK from trigger_policy → target_repo_policy (composite). Adding
    // a trigger before the target row must fail.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresPolicyStore::new(pool);

    let result = store
        .add_trigger_policy(
            100,
            10,
            TriggerKind::BranchPush,
            &TriggerMatchSpec::BranchPush { branch_name: "develop".into() },
            None,
            None,
        )
        .await;
    assert!(
        result.is_err(),
        "FK to target_repo_policy must reject orphan trigger inserts; got: {result:?}"
    );
}

#[tokio::test]
async fn add_then_list_enabled_triggers_round_trip() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresPolicyStore::new(pool);
    store
        .upsert_target_policy(100, 10, None)
        .await
        .unwrap();

    let branch_trigger = store
        .add_trigger_policy(
            100,
            10,
            TriggerKind::BranchPush,
            &TriggerMatchSpec::BranchPush { branch_name: "develop".into() },
            Some("--cargo-args --release"),
            None,
        )
        .await
        .unwrap();
    assert!(branch_trigger.is_enabled);
    assert_eq!(
        branch_trigger
            .bench_args
            .as_deref(),
        Some("--cargo-args --release")
    );

    // List with the right kind returns it.
    let rows = store
        .list_enabled_triggers(100, 10, TriggerKind::BranchPush)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, branch_trigger.id);

    // List with the wrong kind returns empty.
    let tag_rows = store
        .list_enabled_triggers(100, 10, TriggerKind::TagCreated)
        .await
        .unwrap();
    assert!(tag_rows.is_empty());

    // Disabling the trigger removes it from the enabled list.
    store
        .disable_trigger_policy(branch_trigger.id)
        .await
        .unwrap();
    let rows = store
        .list_enabled_triggers(100, 10, TriggerKind::BranchPush)
        .await
        .unwrap();
    assert!(rows.is_empty(), "disabled trigger must NOT appear in enabled list");
}

#[tokio::test]
async fn list_enabled_triggers_excludes_triggers_whose_parent_target_is_disabled() {
    // Slice 5 second-pass review fix: even with `trigger_policy.is_enabled
    // = TRUE`, a trigger must NOT surface if its parent
    // `target_repo_policy.is_enabled = FALSE`. Without this, a manual
    // `sbgh-cli policy target disable` (which doesn't cascade through
    // the same code path as `installation_repositories.removed`) would
    // leave triggers effective.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresPolicyStore::new(pool);
    store
        .upsert_target_policy(100, 10, None)
        .await
        .unwrap();
    let trigger = store
        .add_trigger_policy(
            100,
            10,
            TriggerKind::BranchPush,
            &TriggerMatchSpec::BranchPush { branch_name: "develop".into() },
            None,
            None,
        )
        .await
        .unwrap();

    // Sanity: with both enabled, the trigger appears.
    let rows = store
        .list_enabled_triggers(100, 10, TriggerKind::BranchPush)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, trigger.id);

    // Disable ONLY the parent target; trigger row stays is_enabled=TRUE.
    store
        .disable_target_policy(100, 10)
        .await
        .unwrap();
    let rows = store
        .list_enabled_triggers(100, 10, TriggerKind::BranchPush)
        .await
        .unwrap();
    assert!(rows.is_empty(), "trigger must NOT surface when parent target_repo_policy is disabled");

    // Re-enable target → trigger reappears (since its is_enabled was
    // never flipped here).
    store
        .upsert_target_policy(100, 10, None)
        .await
        .unwrap();
    let rows = store
        .list_enabled_triggers(100, 10, TriggerKind::BranchPush)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "re-enabling target makes the trigger visible again");
}

#[tokio::test]
async fn disable_target_and_triggers_cascades_in_single_transaction() {
    // Slice 5 review fix: the InstallationRepositoriesHandler.removed
    // path calls this method BEFORE revoking the membership, so a
    // stale inbox event arriving later sees a disabled policy +
    // disabled triggers and correctly fails policy eval.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    // Also seed a SECOND install+repo to verify cascade doesn't
    // scope-creep.
    seed_install_repo(&pool, 200, 10).await;
    let store = PostgresPolicyStore::new(pool.clone());
    store
        .upsert_target_policy(100, 10, None)
        .await
        .unwrap();
    store
        .upsert_target_policy(200, 10, None)
        .await
        .unwrap();
    store
        .add_trigger_policy(
            100,
            10,
            TriggerKind::BranchPush,
            &TriggerMatchSpec::BranchPush { branch_name: "main".into() },
            None,
            None,
        )
        .await
        .unwrap();
    store
        .add_trigger_policy(
            100,
            10,
            TriggerKind::TagCreated,
            &TriggerMatchSpec::TagCreated { tag_pattern: r"^v\d+$".into() },
            None,
            None,
        )
        .await
        .unwrap();
    // Trigger for the OTHER install — must NOT be cascade-disabled.
    let other_trigger = store
        .add_trigger_policy(
            200,
            10,
            TriggerKind::BranchPush,
            &TriggerMatchSpec::BranchPush { branch_name: "main".into() },
            None,
            None,
        )
        .await
        .unwrap();

    store
        .disable_target_and_triggers(100, 10)
        .await
        .unwrap();

    // Install 100's target + both triggers all disabled.
    let target_100 = store
        .lookup_target_policy(100, 10)
        .await
        .unwrap()
        .unwrap();
    assert!(!target_100.is_enabled);
    let triggers_100 = store
        .list_triggers(100, 10)
        .await
        .unwrap();
    assert_eq!(triggers_100.len(), 2);
    assert!(
        triggers_100
            .iter()
            .all(|t| !t.is_enabled)
    );

    // Install 200's target + trigger UNTOUCHED.
    let target_200 = store
        .lookup_target_policy(200, 10)
        .await
        .unwrap()
        .unwrap();
    assert!(target_200.is_enabled, "other install's target must NOT be cascade-disabled");
    let triggers_200 = store
        .list_enabled_triggers(200, 10, TriggerKind::BranchPush)
        .await
        .unwrap();
    assert_eq!(triggers_200.len(), 1);
    assert_eq!(triggers_200[0].id, other_trigger.id);
}

#[tokio::test]
async fn disable_target_and_triggers_is_idempotent() {
    // Predicates filter `is_enabled = TRUE` so re-running against
    // already-disabled rows is a no-op.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresPolicyStore::new(pool);
    store
        .upsert_target_policy(100, 10, None)
        .await
        .unwrap();
    store
        .disable_target_and_triggers(100, 10)
        .await
        .unwrap();
    // Second call must succeed silently.
    store
        .disable_target_and_triggers(100, 10)
        .await
        .unwrap();
}

#[tokio::test]
async fn list_triggers_returns_disabled_too() {
    // `list_triggers` (the CLI list path) includes disabled rows so
    // operators can see them.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresPolicyStore::new(pool);
    store
        .upsert_target_policy(100, 10, None)
        .await
        .unwrap();

    let t = store
        .add_trigger_policy(
            100,
            10,
            TriggerKind::BranchPush,
            &TriggerMatchSpec::BranchPush { branch_name: "main".into() },
            None,
            None,
        )
        .await
        .unwrap();
    store
        .disable_trigger_policy(t.id)
        .await
        .unwrap();
    let rows = store
        .list_triggers(100, 10)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "list_triggers includes disabled rows");
    assert!(!rows[0].is_enabled);
}
