use sbgh_core::db::{JobStore, SubmissionStore};
use sbgh_core::models::{
    BuildTarget, GitRefKind, JobIntent, JobSource, QueuedEventDetail, TaskKind,
};
use sbgh_core::reporting::{ReportLifecycleState, TaskReport};
use sbgh_core::submission::{
    BlockValidationPlan, PreparedSubmission, ProducerKey, ResolvedTaskSource,
    SchedulingConstraints, SubmissionActor, SubmissionCommand, SubmissionProvenance, TaskPlan,
};
use sbgh_fleet::{BlockValidationPayload, InclusiveRange, ValidationEpoch};
use sbgh_postgres::db::{Pool, PostgresJobStore, setup_pg_db};
use uuid::Uuid;

async fn seed_install_repo(pool: &Pool) {
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

fn validation_submission() -> PreparedSubmission {
    let commit = "a".repeat(40);
    let payload = BlockValidationPayload {
        epoch: ValidationEpoch::Nakamoto,
        range: InclusiveRange { start: 10, end: 20 },
        requested_shards: 2,
        max_concurrency: 2,
        timeout_secs: 60,
    };
    PreparedSubmission {
        command: SubmissionCommand {
            actor: SubmissionActor::System,
            producer_key: ProducerKey {
                namespace: "report-test".into(),
                key: "validation".into(),
            },
            constraints: SchedulingConstraints::default(),
            task: TaskPlan::BlockValidation(BlockValidationPlan {
                source: ResolvedTaskSource {
                    github_installation_id: 100,
                    github_repo_id: 10,
                    source: JobSource::Cli,
                    intent: JobIntent::BlockValidation,
                    task_kind: TaskKind::BlockValidation,
                    build_target: BuildTarget::StacksInspect,
                    git_ref_kind: GitRefKind::Commit,
                    git_ref_display: commit.clone(),
                    commit,
                    committed_at: None,
                    workload_key: None,
                },
                payload,
            }),
            provenance: SubmissionProvenance {
                queued_event_detail: serde_json::to_value(QueuedEventDetail::BlockValidation {
                    range_start: 10,
                    range_end: 20,
                    requested_shards: 2,
                    max_concurrency: 2,
                })
                .unwrap(),
                github: None,
                slack: None,
            },
        },
        contract_version: 1,
        request_digest: "1".repeat(64),
    }
}

#[tokio::test]
async fn validation_report_joins_frozen_request_without_fabricating_a_verdict() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool).await;
    let store = PostgresJobStore::new(pool.clone());
    let receipt = store
        .persist_submission(&validation_submission())
        .await
        .unwrap();

    let report = store
        .submission_report(receipt.submission_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.lifecycle.state, ReportLifecycleState::Queued);
    let TaskReport::BlockValidation(detail) = report.task else {
        panic!("expected block-validation detail")
    };
    assert_eq!(
        detail
            .requested_range
            .unwrap()
            .start,
        10
    );
    assert_eq!(
        detail
            .requested_range
            .unwrap()
            .end,
        20
    );
    assert_eq!(detail.verdict, None);
    assert_eq!(detail.observed_range, None);

    let worker_id = Uuid::new_v4();
    let session_id = Uuid::new_v4();
    let attempt_id = Uuid::new_v4();
    let job_id = receipt.initial_job_ids[0];
    sqlx::query(
        "INSERT INTO worker_registry
            (worker_id, display_name, allowed_capabilities)
         VALUES ($1, 'report worker',
                 ARRAY['block_validation']::worker_capability[])",
    )
    .bind(worker_id)
    .execute(&pool)
    .await
    .unwrap();
    let identity = [0x42_u8; 32];
    sqlx::query(
        "INSERT INTO worker_identity_key (identity_key_sha256, worker_id)
         VALUES ($1, $2)",
    )
    .bind(identity.as_slice())
    .bind(worker_id)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO worker_session
            (worker_session_id, worker_id, status, protocol_version,
             software_version, advertised_capabilities, effective_capabilities,
             resource_facts, expires_at, identity_key_sha256)
         VALUES ($1, $2, 'running', 3, 'test',
                 ARRAY['block_validation']::worker_capability[],
                 ARRAY['block_validation']::worker_capability[], '{}'::jsonb,
                 NOW() + INTERVAL '1 hour', $3)",
    )
    .bind(session_id)
    .bind(worker_id)
    .bind(identity.as_slice())
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO worker_attempt
            (attempt_id, job_id, task_submission_id, worker_id,
             worker_session_id, status, fencing_generation,
             execution_generation, claim_token, trace_id, capability,
             payload_hash, offer_expires_at, lease_expires_at)
         SELECT $1, job.id, job.task_submission_id, $2, $3, 'offered', 1, 1,
                $4, $5, 'block_validation', job.execution_payload_hash,
                NOW() + INTERVAL '1 minute', NOW() + INTERVAL '5 minutes'
           FROM job WHERE job.id = $6",
    )
    .bind(attempt_id)
    .bind(worker_id)
    .bind(session_id)
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .bind(job_id)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO block_validation_result
            (job_id, attempt_id, chainstate_origin, observed_start,
             observed_end, valid, checked_blocks, invalid_blocks,
             artifact_manifest)
         VALUES ($1, $2, 'nightly-mainnet', 10, 20, FALSE, 11,
                 '[{\"shard\":0,\"block\":\"15\",\"reason\":\"bad hash\"}]',
                 '[{\"key\":\"reports/full.json\",\"logical_key\":\"full\",\
                    \"size\":12,\"sha256\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"}]')",
    )
    .bind(job_id)
    .bind(attempt_id)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("UPDATE job SET status = 'completed' WHERE id = $1")
        .bind(job_id)
        .execute(&pool)
        .await
        .unwrap();

    let report = store
        .submission_report(receipt.submission_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.lifecycle.state, ReportLifecycleState::Completed);
    let TaskReport::BlockValidation(detail) = report.task else {
        panic!("expected block-validation detail")
    };
    assert_eq!(detail.verdict, Some(sbgh_core::reporting::BlockValidationVerdict::Invalid));
    assert_eq!(
        detail
            .observed_range
            .unwrap()
            .end,
        20
    );
    assert_eq!(detail.invalid_blocks[0].block, "15");
    assert_eq!(report.artifacts[0].key, "reports/full.json");
}

#[tokio::test]
async fn github_report_identity_is_submission_owned_and_conflicts_fail_closed() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool).await;
    let store = PostgresJobStore::new(pool);
    let receipt = store
        .persist_submission(&validation_submission())
        .await
        .unwrap();
    let job_id = receipt.initial_job_ids[0];

    store
        .record_submission_check(
            job_id,
            42,
            Some("https://example.test/check/42"),
            "stacks-block-validation",
            &format!("submission:{}:stacks-block-validation", receipt.submission_id),
        )
        .await
        .unwrap();
    let identity = store
        .submission_github_report_identity(receipt.submission_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(identity.check_run_id, Some(42));
    assert_eq!(identity.check_name.as_deref(), Some("stacks-block-validation"));

    assert!(
        store
            .record_submission_check(job_id, 43, None, "stacks-block-validation", "different",)
            .await
            .is_err(),
        "a later projection must not replace aggregate ownership"
    );
}
