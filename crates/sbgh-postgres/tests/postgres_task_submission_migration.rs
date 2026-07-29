use sbgh_postgres::db::{Pool, migrate, setup_pg_db_to};
use uuid::Uuid;

type MigratedContract = (
    Option<i32>,
    Option<String>,
    Option<Uuid>,
    Option<Uuid>,
    Option<String>,
    Option<String>,
    String,
);

const V26_MIGRATION: i64 = 20_260_728_000_001;
const V27_1_MIGRATION: i64 = 20_260_728_000_002;

#[derive(Debug, Clone, Copy)]
struct LegacySubmission {
    submission_id: Uuid,
    job_id: Uuid,
}

async fn seed_tenant(pool: &Pool) {
    sqlx::query(
        "INSERT INTO allowed_installer
            (github_account_id, account_login, account_type)
         VALUES (100, 'octo', 'organization')",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO github_installation
            (id, github_account_id, account_login, account_type)
         VALUES (100, 100, 'octo', 'organization')",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO github_repo (id, owner, name) VALUES (10, 'o', 'r')")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO github_installation_repo
            (github_installation_id, github_repo_id)
         VALUES (100, 10)",
    )
    .execute(pool)
    .await
    .unwrap();
}

async fn seed_submission(pool: &Pool, source: &str, created_at: &str) -> LegacySubmission {
    let submission_id = Uuid::new_v4();
    let spec_id = Uuid::new_v4();
    let job_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO task_submission
            (id, github_installation_id, github_repo_id, source, intent,
             artifact_prefix, created_at, updated_at)
         VALUES ($1, 100, 10, $2::job_source, 'adhoc_benchmark', $3,
                 $4::timestamptz, $4::timestamptz)",
    )
    .bind(submission_id)
    .bind(source)
    .bind(submission_id.to_string())
    .bind(created_at)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO task_spec
            (id, task_submission_id, spec_index, github_repo_id, task_kind,
             build_target, git_ref_kind, git_ref_display, git_commit_hash,
             requested_run_count, created_at, updated_at)
         VALUES ($1, $2, 0, 10, 'benchmark', 'stacks_bench', 'commit',
                 '0123456789abcdef0123456789abcdef01234567',
                 '0123456789abcdef0123456789abcdef01234567', 1,
                 $3::timestamptz, $3::timestamptz)",
    )
    .bind(spec_id)
    .bind(submission_id)
    .bind(created_at)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO job
            (id, task_submission_id, task_spec_id, task_run_index,
             github_installation_id, github_repo_id, source, intent, task_kind,
             build_target, git_ref_kind, git_ref_display, git_commit_hash,
             created_at, updated_at)
         VALUES ($1, $2, $3, 0, 100, 10, $4::job_source, 'adhoc_benchmark',
                 'benchmark', 'stacks_bench', 'commit',
                 '0123456789abcdef0123456789abcdef01234567',
                 '0123456789abcdef0123456789abcdef01234567',
                 $5::timestamptz, $5::timestamptz)",
    )
    .bind(job_id)
    .bind(submission_id)
    .bind(spec_id)
    .bind(source)
    .bind(created_at)
    .execute(pool)
    .await
    .unwrap();
    LegacySubmission { submission_id, job_id }
}

async fn seed_queued_event(
    pool: &Pool,
    job_id: Uuid,
    occurred_at: &str,
    detail: serde_json::Value,
) {
    sqlx::query(
        "INSERT INTO job_event
            (job_id, event_kind, event_status, occurred_at, detail)
         VALUES ($1, 'queued', 'success', $2::timestamptz, $3)",
    )
    .bind(job_id)
    .bind(occurred_at)
    .bind(detail)
    .execute(pool)
    .await
    .unwrap();
}

async fn seed_github_link(pool: &Pool, job_id: Uuid, delivery_id: &str) -> i64 {
    let webhook_id: i64 = sqlx::query_scalar(
        "INSERT INTO github_webhook
            (delivery_id, event_type, payload_size_bytes)
         VALUES ($1, 'issue_comment', 0)
         RETURNING id",
    )
    .bind(delivery_id)
    .fetch_one(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO github_webhook_job (github_webhook_id, job_id)
         VALUES ($1, $2)",
    )
    .bind(webhook_id)
    .bind(job_id)
    .execute(pool)
    .await
    .unwrap();
    webhook_id
}

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
    let worker_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO worker_registry
            (worker_id, identity_uri, display_name, allowed_capabilities,
             measurement_profile)
         VALUES ($1, $2, 'historical worker',
                 ARRAY['benchmark']::worker_capability[], 'profile-v1')",
    )
    .bind(worker_id)
    .bind(format!("urn:sbgh:worker:{worker_id}"))
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE benchmark_group
            SET worker_id = $2, measurement_profile = 'profile-v1'
          WHERE id = $1",
    )
    .bind(submission_id)
    .bind(worker_id)
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

    let migrated_contract: MigratedContract = sqlx::query_as(
        "SELECT submission.contract_version, submission.request_digest,
                submission.required_worker_id, submission.assigned_worker_id,
                submission.required_measurement_profile,
                submission.assigned_measurement_profile,
                job.required_capability::text
           FROM task_submission submission
           JOIN job ON job.task_submission_id = submission.id
          WHERE submission.id = $1",
    )
    .bind(submission_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        migrated_contract,
        (
            None,
            None,
            Some(worker_id),
            Some(worker_id),
            Some("profile-v1".into()),
            Some("profile-v1".into()),
            "benchmark".into(),
        )
    );

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

#[tokio::test]
async fn v27_kernel_backfills_github_and_deterministic_slack_provenance() {
    let (_db, pool) = setup_pg_db_to(V27_1_MIGRATION).await;
    seed_tenant(&pool).await;

    let github = seed_submission(&pool, "github_comment", "2026-07-01T00:00:00Z").await;
    let github_webhook_id = seed_github_link(&pool, github.job_id, "delivery-1").await;
    sqlx::query(
        "INSERT INTO github_user (id, login, user_type)
         VALUES (200, 'octocat', 'user')",
    )
    .execute(&pool)
    .await
    .unwrap();
    let pull_request_id: i64 = sqlx::query_scalar(
        "INSERT INTO github_pull_request
            (target_github_repo_id, source_github_repo_id, pr_number, title,
             author_github_user_id)
         VALUES (10, 10, 42, 'Migration fixture', 200)
         RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO github_user_job (github_user_id, job_id) VALUES (200, $1)")
        .bind(github.job_id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO github_pull_request_job
            (job_id, github_pull_request_id, triggering_comment_id)
         VALUES ($1, $2, 300)",
    )
    .bind(github.job_id)
    .bind(pull_request_id)
    .execute(&pool)
    .await
    .unwrap();
    let github_detail = serde_json::json!({
        "kind": "pull_request_comment",
        "triggering_user_id": 200,
    });
    seed_queued_event(&pool, github.job_id, "2026-07-01T00:00:01Z", github_detail.clone()).await;

    let earliest_slack = seed_submission(&pool, "slack", "2026-07-02T00:00:00Z").await;
    let duplicate_slack = seed_submission(&pool, "slack", "2026-07-03T00:00:00Z").await;
    let reporting_identity = "slack-request:team:channel:123.456";
    let earliest_detail = serde_json::json!({
        "kind": "slack",
        "team_id": "T1",
        "channel": "C1",
        "message_ts": "123.456",
        "reporting_identity": reporting_identity,
    });
    let duplicate_detail = serde_json::json!({
        "kind": "slack",
        "team_id": "T1",
        "channel": "C1",
        "message_ts": "123.456",
        "reporting_identity": reporting_identity,
        "redelivery": true,
    });
    seed_queued_event(
        &pool,
        earliest_slack.job_id,
        "2026-07-02T00:00:01Z",
        earliest_detail.clone(),
    )
    .await;
    seed_queued_event(
        &pool,
        duplicate_slack.job_id,
        "2026-07-03T00:00:01Z",
        duplicate_detail.clone(),
    )
    .await;
    sqlx::query(
        "INSERT INTO job_event
            (job_id, event_kind, event_status, occurred_at, detail)
         VALUES ($1, 'plan_message_sent', 'success',
                 '2026-07-02T00:00:02Z',
                 '{\"plan_message_ts\":\"123.999\"}'::jsonb)",
    )
    .bind(earliest_slack.job_id)
    .execute(&pool)
    .await
    .unwrap();

    migrate(&pool).await.unwrap();

    let github_row: (i64, Option<i64>, Option<i64>, Option<i64>) = sqlx::query_as(
        "SELECT github_webhook_id, triggering_user_id, github_pull_request_id,
                triggering_comment_id
           FROM task_submission_github
          WHERE task_submission_id = $1",
    )
    .bind(github.submission_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(github_row, (github_webhook_id, Some(200), Some(pull_request_id), Some(300)));

    let github_key: (Uuid, Vec<Uuid>) = sqlx::query_as(
        "SELECT task_submission_id, initial_job_ids
           FROM task_submission_idempotency
          WHERE producer_namespace = 'github_webhook' AND producer_key = $1",
    )
    .bind(github_webhook_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(github_key, (github.submission_id, vec![github.job_id]));

    let slack_row: (Uuid, String, String, String, Option<String>) = sqlx::query_as(
        "SELECT task_submission_id, team_id, channel_id, request_message_ts,
                report_message_ts
           FROM task_submission_slack
          WHERE reporting_identity = $1",
    )
    .bind(reporting_identity)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        slack_row,
        (
            earliest_slack.submission_id,
            "T1".into(),
            "C1".into(),
            "123.456".into(),
            Some("123.999".into()),
        )
    );

    let slack_key: (Uuid, Vec<Uuid>) = sqlx::query_as(
        "SELECT task_submission_id, initial_job_ids
           FROM task_submission_idempotency
          WHERE producer_namespace = 'slack_request' AND producer_key = $1",
    )
    .bind(reporting_identity)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(slack_key, (earliest_slack.submission_id, vec![earliest_slack.job_id]));

    let provenance: Vec<(Uuid, String, Option<String>, serde_json::Value)> = sqlx::query_as(
        "SELECT task_submission_id, actor_kind, actor_identity, queued_event_detail
           FROM task_submission_provenance
          ORDER BY created_at, task_submission_id",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(provenance.len(), 3);
    assert!(provenance.iter().any(|row| {
        row.0 == github.submission_id
            && row.1 == "github_user"
            && row.2.as_deref() == Some("200")
            && row.3 == github_detail
    }));
    assert!(provenance.iter().any(|row| {
        row.0 == earliest_slack.submission_id
            && row.1 == "slack_user"
            && row.2.as_deref() == Some("legacy")
            && row.3 == earliest_detail
    }));
    assert!(provenance.iter().any(|row| {
        row.0 == duplicate_slack.submission_id
            && row.1 == "slack_user"
            && row.2.as_deref() == Some("legacy")
            && row.3 == duplicate_detail
    }));
}

#[tokio::test]
async fn v27_kernel_rejects_ambiguous_github_and_slack_producer_identity() {
    let (_db, pool) = setup_pg_db_to(V27_1_MIGRATION).await;
    seed_tenant(&pool).await;
    let submission = seed_submission(&pool, "slack", "2026-07-01T00:00:00Z").await;
    seed_github_link(&pool, submission.job_id, "ambiguous-delivery").await;
    seed_queued_event(
        &pool,
        submission.job_id,
        "2026-07-01T00:00:01Z",
        serde_json::json!({
            "team_id": "T1",
            "channel": "C1",
            "message_ts": "123.456",
            "reporting_identity": "ambiguous-request",
        }),
    )
    .await;

    let error = migrate(&pool)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("both GitHub and Slack producer identities"), "{error}");
    let submission_id = submission
        .submission_id
        .to_string();
    assert!(error.contains(&submission_id), "{error}");
    let kernel_table: Option<String> =
        sqlx::query_scalar("SELECT to_regclass('task_submission_idempotency')::text")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        kernel_table.is_none(),
        "the failed migration must roll back its earlier DDL and backfills"
    );
}

#[tokio::test]
async fn v27_kernel_requires_in_flight_attempts_and_cleanup_to_be_drained() {
    let (_db, pool) = setup_pg_db_to(V27_1_MIGRATION).await;
    seed_tenant(&pool).await;
    let submission = seed_submission(&pool, "cli", "2026-07-01T00:00:00Z").await;
    sqlx::query(
        "UPDATE job
            SET status = 'running', claim_token = $2, claimed_at = NOW()
          WHERE id = $1",
    )
    .bind(submission.job_id)
    .bind(Uuid::new_v4())
    .execute(&pool)
    .await
    .unwrap();

    let worker_id = Uuid::new_v4();
    let worker_session_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO worker_registry
            (worker_id, identity_uri, display_name, allowed_capabilities)
         VALUES ($1, $2, 'migration worker',
                 ARRAY['benchmark']::worker_capability[])",
    )
    .bind(worker_id)
    .bind(format!("urn:sbgh:worker:{worker_id}"))
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO worker_session
            (worker_session_id, worker_id, status, protocol_version,
             software_version, advertised_capabilities, effective_capabilities,
             resource_facts, expires_at)
         VALUES ($1, $2, 'running', 3, 'test',
                 ARRAY['benchmark']::worker_capability[],
                 ARRAY['benchmark']::worker_capability[], '{}'::jsonb,
                 NOW() + INTERVAL '1 hour')",
    )
    .bind(worker_session_id)
    .bind(worker_id)
    .execute(&pool)
    .await
    .unwrap();

    let active_attempt_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO worker_attempt
            (attempt_id, job_id, task_submission_id, worker_id, worker_session_id,
             status, fencing_generation, execution_generation, claim_token,
             trace_id, capability, payload_hash, offer_expires_at,
             lease_expires_at, accepted_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 1, 1, $6, $7,
                 'benchmark', repeat('a', 64), NOW() + INTERVAL '1 minute',
                 NOW() + INTERVAL '5 minutes', NOW())",
    )
    .bind(active_attempt_id)
    .bind(submission.job_id)
    .bind(submission.submission_id)
    .bind(worker_id)
    .bind(worker_session_id)
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .execute(&pool)
    .await
    .unwrap();

    let cleanup_attempt_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO worker_attempt
            (attempt_id, job_id, task_submission_id, worker_id, worker_session_id,
             status, fencing_generation, execution_generation, claim_token,
             trace_id, capability, payload_hash, offer_expires_at,
             lease_expires_at)
         VALUES ($1, $2, $3, $4, $5, 'fenced', 2, 1, $6, $7,
                 'benchmark', repeat('b', 64), NOW() + INTERVAL '1 minute',
                 NOW() + INTERVAL '5 minutes')",
    )
    .bind(cleanup_attempt_id)
    .bind(submission.job_id)
    .bind(submission.submission_id)
    .bind(worker_id)
    .bind(worker_session_id)
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO worker_cleanup_obligation
            (worker_id, worker_session_id, attempt_id, job_id, requeue_job, reason)
         VALUES ($1, $2, $3, $4, TRUE, 'migration fixture')",
    )
    .bind(worker_id)
    .bind(worker_session_id)
    .bind(cleanup_attempt_id)
    .bind(submission.job_id)
    .execute(&pool)
    .await
    .unwrap();

    let error = migrate(&pool)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("drain in-flight execution"), "{error}");
    assert!(error.contains("claimed/running jobs: 1"), "{error}");
    assert!(error.contains("active attempts: 1"), "{error}");
    assert!(error.contains("pending cleanup obligations: 1"), "{error}");
}
