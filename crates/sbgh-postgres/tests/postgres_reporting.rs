use sbgh_core::db::{JobStore, SubmissionStore};
use sbgh_core::inspection::WorkTarget;
use sbgh_core::models::{
    BuildTarget, GitRefKind, JobIntent, JobSource, QueuedEventDetail, TaskKind,
};
use sbgh_core::reporting::{ReportLifecycleState, TaskReport};
use sbgh_core::submission::{
    BenchmarkPlan, BenchmarkVariant, BlockValidationPlan, PreparedSubmission, ProducerKey,
    ResolvedTaskSource, SchedulingConstraints, SubmissionActor, SubmissionCommand,
    SubmissionProvenance, TaskPlan,
};
use sbgh_fleet::{BlockValidationPayload, InclusiveRange};
use sbgh_postgres::application::{inspect_work, job_artifacts, list_submissions, resolve_work};
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
        selection: sbgh_fleet::BlockValidationSelection::Range {
            range: InclusiveRange { start: 10, end: 20 },
        },
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
                    selection: sbgh_fleet::BlockValidationSelection::Range {
                        range: InclusiveRange { start: 10, end: 20 },
                    },
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

/// Add a second queued repeat without changing the original fixture identity.
async fn add_repeat(pool: &Pool, original: Uuid, id: Uuid) {
    sqlx::query(
        "INSERT INTO job (id, task_submission_id, task_spec_id, task_run_index,
             github_installation_id, github_repo_id, source, intent, task_kind, build_target,
             git_ref_kind, git_ref_display, git_commit_hash, required_capability)
         SELECT $1, task_submission_id, task_spec_id, task_run_index + 1,
                github_installation_id, github_repo_id, source, intent, task_kind, build_target,
                git_ref_kind, git_ref_display, git_commit_hash, required_capability FROM job WHERE id = $2",
    )
    .bind(id)
    .bind(original)
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn work_inspection_resolves_ids_and_preserves_job_scope_with_ambiguous_matches() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool).await;
    let receipt = PostgresJobStore::new(pool.clone())
        .persist_submission(&validation_submission())
        .await
        .unwrap();
    let job_id = receipt.initial_job_ids[0];
    let matches = resolve_work(&pool, &job_id.to_string()[..8], None)
        .await
        .unwrap();
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0].job_id, Some(job_id));
    assert_eq!(matches[0].submission_id, receipt.submission_id);
    let snapshot = inspect_work(&pool, matches[0].clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.target.job_id, Some(job_id));
    assert_eq!(snapshot.jobs[0].status, "queued");
    assert!(
        snapshot.jobs[0]
            .progress
            .is_none()
    );
    assert!(
        snapshot.jobs[0]
            .attempt
            .is_none()
    );
    assert!(
        resolve_work(&pool, &Uuid::new_v4().to_string(), None)
            .await
            .unwrap()
            .is_empty()
    );

    // UUID namespaces can overlap. Unqualified selection must remain ambiguous.
    add_repeat(&pool, job_id, receipt.submission_id).await;
    let selector = receipt
        .submission_id
        .to_string();
    assert_eq!(
        resolve_work(&pool, &selector, None)
            .await
            .unwrap()
            .len(),
        2
    );
    let job = resolve_work(&pool, &selector, Some("job"))
        .await
        .unwrap();
    let submission = resolve_work(&pool, &selector, Some("submission"))
        .await
        .unwrap();
    assert_eq!(job[0].job_id, Some(receipt.submission_id));
    assert_eq!(submission[0].job_id, None);
    let snapshot = inspect_work(&pool, submission[0].clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.jobs.len(), 2);
    assert_eq!(
        snapshot
            .report
            .lifecycle
            .total_jobs,
        2
    );
}

#[tokio::test]
async fn work_inspection_browses_aggregate_state_and_selects_running_repeat() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool).await;
    let receipt = PostgresJobStore::new(pool.clone())
        .persist_submission(&validation_submission())
        .await
        .unwrap();
    let job_id = receipt.initial_job_ids[0];
    add_repeat(&pool, job_id, Uuid::new_v4()).await;
    sqlx::query("UPDATE job SET status = 'running' WHERE id = $1")
        .bind(job_id)
        .execute(&pool)
        .await
        .unwrap();
    let snapshot = inspect_work(
        &pool,
        WorkTarget {
            submission_id: receipt.submission_id,
            job_id: None,
        },
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        snapshot
            .report
            .identity
            .current_job_id,
        Some(job_id)
    );
    assert_eq!(
        snapshot
            .report
            .lifecycle
            .state,
        ReportLifecycleState::Running
    );
    let summaries = list_submissions(&pool, None, true, 1)
        .await
        .unwrap();
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].repository, "o/r");
    assert_eq!(summaries[0].total_jobs, 2);
    assert!(
        list_submissions(&pool, Some("completed"), false, 1)
            .await
            .unwrap()
            .is_empty()
    );
    sqlx::query("UPDATE job SET status = 'completed'")
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        list_submissions(&pool, None, true, 1)
            .await
            .unwrap()
            .is_empty()
    );
    let completed = list_submissions(&pool, Some("completed"), false, 1)
        .await
        .unwrap();
    assert_eq!(completed[0].completed_jobs, 2);
}

/// Place a running attempt with its own worker session.
async fn inspection_attempt(pool: &Pool, job_id: Uuid, generation: i64) -> Uuid {
    sqlx::query("UPDATE job SET status = 'running' WHERE id = $1")
        .bind(job_id)
        .execute(pool)
        .await
        .unwrap();
    let worker_id = Uuid::new_v4();
    let session_id = Uuid::new_v4();
    let attempt_id = Uuid::new_v4();
    let identity = vec![generation as u8; 32];
    sqlx::query(
        "INSERT INTO worker_registry (worker_id, display_name, allowed_capabilities)
        VALUES ($1, 'inspection worker', ARRAY['block_validation']::worker_capability[])",
    )
    .bind(worker_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO worker_identity_key (identity_key_sha256, worker_id) VALUES ($1, $2)")
        .bind(&identity)
        .bind(worker_id)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO worker_session (worker_session_id, worker_id, status, protocol_version,
        software_version, advertised_capabilities, effective_capabilities, resource_facts,
        expires_at, identity_key_sha256) VALUES ($1, $2, 'running', 3, 'test',
        ARRAY['block_validation']::worker_capability[], ARRAY['block_validation']::worker_capability[],
        '{}', NOW() + INTERVAL '1 hour', $3)")
        .bind(session_id).bind(worker_id).bind(&identity).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO worker_attempt (attempt_id, job_id, task_submission_id, worker_id,
        worker_session_id, status, fencing_generation, execution_generation, claim_token, trace_id,
        capability, payload_hash, offer_expires_at, lease_expires_at, accepted_at)
        SELECT $1, job.id, job.task_submission_id, $2, $3, 'running', $4, 1, $5, $6,
        'block_validation', repeat('a', 64), NOW() + INTERVAL '1 minute', NOW() + INTERVAL '5 minutes', NOW()
        FROM job WHERE id = $7")
        .bind(attempt_id).bind(worker_id).bind(session_id).bind(generation)
        .bind(Uuid::new_v4()).bind(Uuid::new_v4()).bind(job_id).execute(pool).await.unwrap();
    attempt_id
}

#[tokio::test]
async fn work_inspection_progress_is_scoped_to_latest_attempt_and_current_phase() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool).await;
    let receipt = PostgresJobStore::new(pool.clone())
        .persist_submission(&validation_submission())
        .await
        .unwrap();
    let job_id = receipt.initial_job_ids[0];
    let old = inspection_attempt(&pool, job_id, 1).await;
    sqlx::query("UPDATE worker_attempt SET status = 'fenced' WHERE attempt_id = $1")
        .bind(old)
        .execute(&pool)
        .await
        .unwrap();
    let current = inspection_attempt(&pool, job_id, 2).await;
    for (id, sequence, count) in [(old, 999_i64, 999_u64), (current, 1, 42), (current, 2, 43)] {
        sqlx::query(
            "INSERT INTO worker_attempt_progress (attempt_id, progress_seq, trace_id, update)
            VALUES ($1, $2, $3, $4)",
        )
        .bind(id)
        .bind(sequence)
        .bind(Uuid::new_v4())
        .bind(serde_json::json!({"workflow_step":"run", "run_index":0, "requested_run_count":1,
                "phase":"validate", "progress":count, "total":100, "message":"validating"}))
        .execute(&pool)
        .await
        .unwrap();
    }
    let target = WorkTarget {
        submission_id: receipt.submission_id,
        job_id: Some(job_id),
    };
    let snapshot = inspect_work(&pool, target.clone())
        .await
        .unwrap()
        .unwrap();
    let job = &snapshot.jobs[0];
    assert_eq!(
        job.attempt
            .as_ref()
            .unwrap()
            .id,
        current
    );
    assert_eq!(
        job.attempt
            .as_ref()
            .unwrap()
            .worker_name,
        "inspection worker"
    );
    assert_eq!(
        job.progress
            .as_ref()
            .unwrap()
            .current,
        43
    );
    assert_eq!(
        job.progress
            .as_ref()
            .unwrap()
            .sequence,
        2
    );
    assert!(
        job.progress
            .as_ref()
            .unwrap()
            .received_at
            <= chrono::Utc::now()
    );
    sqlx::query(
        "INSERT INTO worker_attempt_event (attempt_id, reliable_seq, trace_id,
        payload_kind, payload, payload_digest) VALUES ($1, 1, $2, 'phase',
        '{\"kind\":\"phase\",\"label\":\"archiving\",\"elapsed_ms\":500}', repeat('a', 64))",
    )
    .bind(current)
    .bind(Uuid::new_v4())
    .execute(&pool)
    .await
    .unwrap();
    let snapshot = inspect_work(&pool, target)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        snapshot.jobs[0]
            .phase
            .as_deref(),
        Some("archiving")
    );
    assert!(
        snapshot.jobs[0]
            .progress
            .is_none(),
        "previous-phase progress must not look current"
    );
}

#[tokio::test]
async fn work_inspection_preserves_planned_repeats_and_promoted_benchmark_results() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool).await;
    let mut prepared = validation_submission();
    let mut source = prepared
        .command
        .task
        .first_source()
        .unwrap()
        .clone();
    source.intent = JobIntent::AdhocBenchmark;
    source.task_kind = TaskKind::Benchmark;
    source.build_target = BuildTarget::StacksBench;
    let effective_args =
        vec!["--count".to_owned(), "25000".to_owned(), "--warmup".to_owned(), "5000".to_owned()];
    prepared.command.task = TaskPlan::Benchmark(BenchmarkPlan {
        variants: vec![BenchmarkVariant {
            source,
            requested_run_count: 3,
            baseline_calibration_id: None,
        }],
        effective_args: effective_args.clone(),
    });
    prepared
        .command
        .provenance
        .queued_event_detail = serde_json::json!({"effective_args":effective_args});
    let receipt = PostgresJobStore::new(pool.clone())
        .persist_submission(&prepared)
        .await
        .unwrap();
    let job_id = receipt.initial_job_ids[0];
    sqlx::query("INSERT INTO job_metric (job_id, envelope_duration_us, replay_duration_us, total_duration_us,
        setup_duration_us, execution_duration_us, commit_duration_us, clarity_runtime, transactions,
        read_length, write_length, measured_blocks, warmup_blocks)
        VALUES ($1, 2000000, 1000000, 900000, 100000, 200000, 600000, 42, 60702, 1234, 5678, 20000, 5000)")
        .bind(job_id).execute(&pool).await.unwrap();
    sqlx::query(
        "INSERT INTO job_result (job_id, run_json, archive_dir) VALUES ($1, $2, 'results/job')",
    )
    .bind(job_id)
    .bind(serde_json::json!({"success":true,"result":{"measured_entries":20000}}))
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO job_event (job_id, event_kind, event_status, detail)
        VALUES ($1, 'completed', 'success', $2)",
    )
    .bind(job_id)
    .bind(serde_json::json!({"summary":{"chainstate_origin":"vg0/bench-canonical"}}))
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("UPDATE job SET status = 'completed' WHERE id = $1")
        .bind(job_id)
        .execute(&pool)
        .await
        .unwrap();
    let snapshot = inspect_work(
        &pool,
        WorkTarget {
            submission_id: receipt.submission_id,
            job_id: None,
        },
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        snapshot
            .report
            .lifecycle
            .completed_jobs,
        1
    );
    assert_eq!(
        snapshot
            .report
            .lifecycle
            .total_jobs,
        3
    );
    assert_eq!(
        snapshot
            .report
            .lifecycle
            .state,
        ReportLifecycleState::Queued
    );
    let job = &snapshot.jobs[0];
    assert_eq!(job.effective_args, effective_args);
    assert_eq!(
        job.chainstate_origin
            .as_deref(),
        Some("vg0/bench-canonical")
    );
    assert_eq!(
        job.metrics
            .as_ref()
            .unwrap()
            .measured_blocks,
        20000
    );
    assert_eq!(
        job.metrics
            .as_ref()
            .unwrap()
            .warmup_blocks,
        5000
    );
    assert_eq!(job.result.as_ref().unwrap()["success"], true);
    let active = list_submissions(&pool, None, true, 1)
        .await
        .unwrap();
    assert_eq!(active[0].total_jobs, 3);
    assert_eq!(active[0].completed_jobs, 1);
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
    assert!(matches!(
        detail.requested,
        Some(sbgh_core::reporting::BlockValidationSelectionReport::Range {
            range: sbgh_core::reporting::InclusiveReportRange { start: 10, end: 20 }
        })
    ));
    assert_eq!(detail.verdict, None);
    assert_eq!(detail.observed, None);

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
            (job_id, attempt_id, chainstate_origin, pre_nakamoto_count,
             nakamoto_count, resolved_start, resolved_end, epoch_segments,
             shard_count, max_concurrency, valid, checked_blocks,
             invalid_blocks, artifact_manifest)
         VALUES ($1, $2, 'nightly-mainnet', 10, 11, 10, 20,
                 '[{\"epoch\":\"nakamoto\",\"global_range\":{\"start\":10,\"end\":20},\
                    \"local_range\":{\"start\":0,\"end\":10}}]', 2, 2, FALSE, 11,
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
            .resolved_range
            .unwrap()
            .end,
        20
    );
    assert_eq!(detail.invalid_blocks[0].block, "15");
    assert_eq!(report.artifacts[0].key, "reports/full.json");
    let observed = inspect_work(
        &pool,
        WorkTarget {
            submission_id: receipt.submission_id,
            job_id: Some(job_id),
        },
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        observed.jobs[0]
            .chainstate_origin
            .as_deref(),
        Some("nightly-mainnet")
    );
    assert_eq!(
        observed.jobs[0]
            .result
            .as_ref()
            .unwrap()["valid"],
        false
    );
    assert_eq!(
        observed.jobs[0]
            .result
            .as_ref()
            .unwrap()["checked_blocks"],
        11
    );
}

#[tokio::test]
async fn failed_submission_report_exposes_terminal_forensic_artifacts() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool).await;
    let store = PostgresJobStore::new(pool.clone());
    let receipt = store
        .persist_submission(&validation_submission())
        .await
        .unwrap();
    let job_id = receipt.initial_job_ids[0];
    sqlx::query("UPDATE job SET status = 'failed' WHERE id = $1")
        .bind(job_id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        r#"
        INSERT INTO job_event (job_id, event_kind, event_status, remark, detail)
        VALUES ($1, 'failed', 'fail', 'VM reported phase=error', $2)
        "#,
    )
    .bind(job_id)
    .bind(serde_json::json!({
        "attempt_id": Uuid::new_v4(),
        "artifacts": [{
            "key": "job/console.tail.log",
            "logical_key": "job/console.tail.log",
            "size": 1024,
            "sha256": "a".repeat(64),
        }],
        "summary": {
            "last_phase": "build",
            "console_tail": "compiler failed",
            "console_size_bytes": 2048,
            "console_archived_path": "job/console.tail.log",
        },
    }))
    .execute(&pool)
    .await
    .unwrap();

    let report = store
        .submission_report(receipt.submission_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.lifecycle.state, ReportLifecycleState::Failed);
    assert_eq!(
        report
            .lifecycle
            .failure
            .as_deref(),
        Some("VM reported phase=error")
    );
    assert_eq!(report.artifacts.len(), 1);
    assert_eq!(report.artifacts[0].name, "job/console.tail.log");
    assert_eq!(report.artifacts[0].key, "job/console.tail.log");
    let forensics = report
        .forensics
        .expect("terminal forensics");
    assert_eq!(
        forensics
            .last_phase
            .as_deref(),
        Some("build")
    );
    assert_eq!(
        forensics
            .console_tail
            .as_deref(),
        Some("compiler failed")
    );
    assert_eq!(forensics.console_size_bytes, Some(2048));
    assert_eq!(
        forensics
            .console_artifact_key
            .as_deref(),
        Some("job/console.tail.log")
    );
}

#[tokio::test]
async fn job_artifacts_are_scoped_to_a_recorded_terminal_manifest() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool).await;
    let store = PostgresJobStore::new(pool.clone());
    let receipt = store
        .persist_submission(&validation_submission())
        .await
        .unwrap();
    let job_id = receipt.initial_job_ids[0];
    assert!(
        job_artifacts(&pool, Uuid::new_v4())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        job_artifacts(&pool, job_id)
            .await
            .unwrap(),
        Some(vec![])
    );

    sqlx::query("UPDATE job SET status = 'completed' WHERE id = $1")
        .bind(job_id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO job_event (job_id, event_kind, event_status, detail) \
         VALUES ($1, 'completed', 'success', $2)",
    )
    .bind(job_id)
    .bind(serde_json::json!({"artifacts": [{
        "key": format!("{job_id}/block-validation/result.json"),
        "logical_key": format!("{job_id}/block-validation/result.json"),
        "size": 2,
        "sha256": "a".repeat(64),
    }]}))
    .execute(&pool)
    .await
    .unwrap();

    let artifacts = job_artifacts(&pool, job_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(artifacts.len(), 1);
    assert_eq!(artifacts[0].logical_key, format!("{job_id}/block-validation/result.json"));
    assert_eq!(artifacts[0].size, 2);
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
