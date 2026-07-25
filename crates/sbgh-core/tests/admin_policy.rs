//! Slice 5 integration tests for `sbgh-cli policy {target,source,trigger}`.
//! Pure-SQL paths — no GitHub API mocking needed (operator types
//! numeric ids directly into the CLI).

use sbgh_core::admin::{
    PolicyError, add_trigger_policy, allow_source_policy, allow_target_policy,
    disable_source_policy, disable_target_policy, disable_trigger_policy, list_source_policies,
    list_target_policies, list_trigger_policies,
};
use sbgh_core::db::{Pool, setup_pg_db};
use sbgh_core::models::TriggerKind;

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
    // Use repo_id in the name so multiple seeded repos don't hit the
    // (lower(owner), lower(name)) unique index.
    sqlx::query(
        "INSERT INTO github_repo (id, owner, name) VALUES ($1, 'o', $2) ON CONFLICT DO NOTHING",
    )
    .bind(repo_id)
    .bind(format!("r{repo_id}"))
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
async fn allow_target_policy_then_disable_round_trip() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;

    let row = allow_target_policy(&pool, 100, 10, Some("operator note"))
        .await
        .unwrap();
    assert!(row.is_enabled);
    let disabled = disable_target_policy(&pool, 100, 10)
        .await
        .unwrap();
    assert!(!disabled.is_enabled);
}

#[tokio::test]
async fn disable_target_policy_cascades_to_disable_matching_triggers() {
    // Slice 5 second-pass review fix: an operator-initiated
    // `policy target disable` now soft-disables every matching
    // trigger_policy row in the same transaction. Without this, the
    // runtime gate in `list_enabled_triggers` would still protect us
    // (it joins on parent target), but `policy trigger list` would
    // show stale `ENABLED` flags and mislead operators.
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    allow_target_policy(&pool, 100, 10, None)
        .await
        .unwrap();
    let trigger = add_trigger_policy(
        &pool,
        100,
        10,
        TriggerKind::BranchPush,
        "{\"kind\":\"branch_push\",\"branch_name\":\"main\"}",
        None,
        None,
    )
    .await
    .unwrap();
    assert!(trigger.is_enabled);

    disable_target_policy(&pool, 100, 10)
        .await
        .unwrap();

    // Trigger now disabled too.
    let triggers = list_trigger_policies(&pool, Some(100), Some(10))
        .await
        .unwrap();
    assert_eq!(triggers.len(), 1);
    assert!(
        !triggers[0].is_enabled,
        "policy target disable MUST cascade to disable matching trigger_policy rows"
    );
}

#[tokio::test]
async fn disable_target_policy_returns_not_found_for_unknown_pair() {
    let (_db, pool) = setup_pg_db().await;
    let err = disable_target_policy(&pool, 999, 999)
        .await
        .unwrap_err();
    assert!(matches!(err, PolicyError::NotFound { install_id: 999, repo_id: 999 }));
}

#[tokio::test]
async fn allow_source_policy_does_not_require_membership() {
    let (_db, pool) = setup_pg_db().await;
    // Just install + repo, no membership.
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

    let row = allow_source_policy(&pool, 100, 20, None)
        .await
        .unwrap();
    assert!(row.is_enabled);
    disable_source_policy(&pool, 100, 20)
        .await
        .unwrap();
}

#[tokio::test]
async fn add_trigger_rejects_invalid_match_spec_json_at_cli_boundary() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    // Seed target so the trigger FK is satisfiable.
    allow_target_policy(&pool, 100, 10, None)
        .await
        .unwrap();

    let err = add_trigger_policy(
        &pool,
        100,
        10,
        TriggerKind::BranchPush,
        "{\"kind\":\"nonsense\"}",
        None,
        None,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, PolicyError::InvalidMatchSpec(_)));
}

#[tokio::test]
async fn add_trigger_rejects_when_target_not_yet_allowed() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    // No target seeded → CLI rejects with friendly error.

    let err = add_trigger_policy(
        &pool,
        100,
        10,
        TriggerKind::BranchPush,
        "{\"kind\":\"branch_push\",\"branch_name\":\"main\"}",
        None,
        None,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, PolicyError::MissingTargetForTrigger { install_id: 100, repo_id: 10 }));
}

#[tokio::test]
async fn add_then_disable_trigger_round_trip() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    allow_target_policy(&pool, 100, 10, None)
        .await
        .unwrap();

    let t = add_trigger_policy(
        &pool,
        100,
        10,
        TriggerKind::BranchPush,
        "{\"kind\":\"branch_push\",\"branch_name\":\"develop\"}",
        Some("--release"),
        None,
    )
    .await
    .unwrap();
    assert!(t.is_enabled);
    assert_eq!(t.bench_args.as_deref(), Some("--release"));

    let disabled = disable_trigger_policy(&pool, t.id)
        .await
        .unwrap();
    assert!(!disabled.is_enabled);
}

#[tokio::test]
async fn disable_trigger_returns_not_found_for_unknown_id() {
    let (_db, pool) = setup_pg_db().await;
    let err = disable_trigger_policy(&pool, 999)
        .await
        .unwrap_err();
    assert!(matches!(err, PolicyError::TriggerNotFound(999)));
}

#[tokio::test]
async fn list_policies_filter_by_install_id() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    seed_install_repo(&pool, 200, 10).await;
    allow_target_policy(&pool, 100, 10, None)
        .await
        .unwrap();
    allow_target_policy(&pool, 200, 10, None)
        .await
        .unwrap();
    allow_source_policy(&pool, 100, 10, None)
        .await
        .unwrap();

    let targets = list_target_policies(&pool, Some(100))
        .await
        .unwrap();
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].github_installation_id, 100);

    let all_targets = list_target_policies(&pool, None)
        .await
        .unwrap();
    assert_eq!(all_targets.len(), 2, "None filter returns all rows");

    let sources = list_source_policies(&pool, Some(100))
        .await
        .unwrap();
    assert_eq!(sources.len(), 1);
}

#[tokio::test]
async fn list_triggers_filter_by_install_and_repo() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    seed_install_repo(&pool, 100, 11).await;
    allow_target_policy(&pool, 100, 10, None)
        .await
        .unwrap();
    allow_target_policy(&pool, 100, 11, None)
        .await
        .unwrap();
    add_trigger_policy(
        &pool,
        100,
        10,
        TriggerKind::BranchPush,
        "{\"kind\":\"branch_push\",\"branch_name\":\"main\"}",
        None,
        None,
    )
    .await
    .unwrap();
    add_trigger_policy(
        &pool,
        100,
        11,
        TriggerKind::BranchPush,
        "{\"kind\":\"branch_push\",\"branch_name\":\"main\"}",
        None,
        None,
    )
    .await
    .unwrap();

    let scoped = list_trigger_policies(&pool, Some(100), Some(10))
        .await
        .unwrap();
    assert_eq!(scoped.len(), 1);
    assert_eq!(scoped[0].github_repo_id, 10);

    let install_wide = list_trigger_policies(&pool, Some(100), None)
        .await
        .unwrap();
    assert_eq!(install_wide.len(), 2);
}
