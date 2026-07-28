use sbgh_postgres::db::{migrate, setup_pg_db_to};
use uuid::Uuid;

const V26_MIGRATION: i64 = 20_260_728_000_001;

#[tokio::test]
async fn v27_rename_preserves_submission_objects_rows_and_relationships() {
    let (_db, pool) = setup_pg_db_to(V26_MIGRATION).await;
    let submission_id = Uuid::new_v4();
    let spec_id = Uuid::new_v4();
    let job_id = Uuid::new_v4();

    sqlx::query(
        "INSERT INTO allowed_installer
            (github_account_id, account_login, account_type)
         VALUES (100, 'octo', 'organization')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO github_installation
            (id, github_account_id, account_login, account_type)
         VALUES (100, 100, 'octo', 'organization')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO github_repo (id, owner, name) VALUES (10, 'o', 'r')")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO github_installation_repo
            (github_installation_id, github_repo_id)
         VALUES (100, 10)",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO benchmark_group
            (id, github_installation_id, github_repo_id, source, intent, artifact_prefix)
         VALUES ($1, 100, 10, 'slack', 'adhoc_benchmark', $2)",
    )
    .bind(submission_id)
    .bind(submission_id.to_string())
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO benchmark_spec
            (id, benchmark_group_id, spec_index, github_repo_id, task_kind,
             build_target, git_ref_kind, git_ref_display, git_commit_hash,
             requested_run_count)
         VALUES ($1, $2, 0, 10, 'benchmark', 'stacks_bench', 'commit',
                 '0123456789abcdef0123456789abcdef01234567',
                 '0123456789abcdef0123456789abcdef01234567', 1)",
    )
    .bind(spec_id)
    .bind(submission_id)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO benchmark_workflow_step
            (benchmark_group_id, step_index, step_kind, benchmark_spec_id)
         VALUES ($1, 0, 'build', $2), ($1, 1, 'run', $2)",
    )
    .bind(submission_id)
    .bind(spec_id)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO job
            (id, benchmark_group_id, benchmark_spec_id, benchmark_run_index,
             github_installation_id, github_repo_id, source, intent, task_kind,
             build_target, git_ref_kind, git_ref_display, git_commit_hash)
         VALUES ($1, $2, $3, 0, 100, 10, 'slack', 'adhoc_benchmark',
                 'benchmark', 'stacks_bench', 'commit',
                 '0123456789abcdef0123456789abcdef01234567',
                 '0123456789abcdef0123456789abcdef01234567')",
    )
    .bind(job_id)
    .bind(submission_id)
    .bind(spec_id)
    .execute(&pool)
    .await
    .unwrap();

    let old_oids: (i64, i64, i64) = sqlx::query_as(
        "SELECT 'benchmark_group'::regclass::oid::bigint,
                'benchmark_spec'::regclass::oid::bigint,
                'benchmark_workflow_step'::regclass::oid::bigint",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    migrate(&pool).await.unwrap();

    let new_oids: (i64, i64, i64) = sqlx::query_as(
        "SELECT 'task_submission'::regclass::oid::bigint,
                'task_spec'::regclass::oid::bigint,
                'task_workflow_step'::regclass::oid::bigint",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(new_oids, old_oids, "ALTER RENAME must preserve relation OIDs");

    let persisted: (Uuid, Uuid, Uuid, i32, String) = sqlx::query_as(
        "SELECT submission.id, spec.id, job.id, job.task_run_index,
                submission.artifact_prefix
           FROM task_submission submission
           JOIN task_spec spec ON spec.task_submission_id = submission.id
           JOIN job ON job.task_spec_id = spec.id
          WHERE submission.id = $1",
    )
    .bind(submission_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(persisted, (submission_id, spec_id, job_id, 0, submission_id.to_string()));

    let steps: Vec<(i32, String)> = sqlx::query_as(
        "SELECT step_index, step_kind::text
           FROM task_workflow_step
          WHERE task_submission_id = $1
          ORDER BY step_index",
    )
    .bind(submission_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(steps, vec![(0, "build".to_string()), (1, "run".to_string())]);

    let legacy_catalog_names: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
           FROM (
                 SELECT relname AS name
                   FROM pg_class
                  WHERE relnamespace = 'public'::regnamespace
                 UNION ALL
                 SELECT conname
                   FROM pg_constraint
                  WHERE connamespace = 'public'::regnamespace
                 UNION ALL
                 SELECT typname
                   FROM pg_type
                  WHERE typnamespace = 'public'::regnamespace
                ) names
          WHERE name ~ 'benchmark_(group|spec|workflow|step|run)'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(legacy_catalog_names, 0);
}
