use sbgh_postgres::db::{migrate, setup_pg_db_to};

const V30_MIGRATION: i64 = 20_260_730_000_001;

#[tokio::test]
async fn v33_adds_validation_role_and_replaces_static_result_columns() {
    let (_db, pool) = setup_pg_db_to(V30_MIGRATION).await;

    let old_role_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
               FROM pg_enum
               JOIN pg_type ON pg_type.oid = pg_enum.enumtypid
              WHERE pg_type.typname = 'user_role'
                AND pg_enum.enumlabel = 'trigger_block_validation'
         )",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(!old_role_exists);

    migrate(&pool).await.unwrap();

    let role_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
               FROM pg_enum
               JOIN pg_type ON pg_type.oid = pg_enum.enumtypid
              WHERE pg_type.typname = 'user_role'
                AND pg_enum.enumlabel = 'trigger_block_validation'
         )",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(role_exists);

    sqlx::query(
        "INSERT INTO allowed_installer
             (github_account_id, account_login, account_type)
         VALUES (100, 'owner', 'organization')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO github_installation
             (id, github_account_id, account_login, account_type)
         VALUES (200, 100, 'owner', 'organization')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO github_user (id, login, user_type)
         VALUES (300, 'operator', 'user')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO github_user_role
             (github_user_id, github_installation_id, granted_role)
         VALUES (300, 200, 'trigger_block_validation')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("DELETE FROM github_user_role WHERE granted_role = 'trigger_block_validation'")
        .execute(&pool)
        .await
        .unwrap();
    let residual_grants: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
           FROM github_user_role
          WHERE granted_role = 'trigger_block_validation'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        residual_grants, 0,
        "rollback cleanup must remove, not merely revoke, rows an older enum decoder cannot read"
    );

    let columns: Vec<String> = sqlx::query_scalar(
        "SELECT column_name
           FROM information_schema.columns
          WHERE table_schema = 'public'
            AND table_name = 'block_validation_result'
          ORDER BY column_name",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    for expected in [
        "pre_nakamoto_count",
        "nakamoto_count",
        "resolved_start",
        "resolved_end",
        "epoch_segments",
        "shard_count",
        "max_concurrency",
    ] {
        assert!(
            columns
                .iter()
                .any(|column| column == expected),
            "missing {expected}"
        );
    }
    assert!(
        !columns
            .iter()
            .any(|column| column == "observed_start")
    );
    assert!(
        !columns
            .iter()
            .any(|column| column == "observed_end")
    );

    let constraints: Vec<String> = sqlx::query_scalar(
        "SELECT conname
           FROM pg_constraint
          WHERE conrelid = 'block_validation_result'::regclass
          ORDER BY conname",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    for expected in [
        "block_validation_observed_total_check",
        "block_validation_resolved_coverage_check",
        "block_validation_checked_count_check",
    ] {
        assert!(
            constraints
                .iter()
                .any(|constraint| constraint == expected),
            "missing {expected}"
        );
    }
}
