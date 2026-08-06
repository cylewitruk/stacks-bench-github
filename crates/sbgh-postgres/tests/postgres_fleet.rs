use std::collections::BTreeSet;

use chrono::{Duration, Utc};
use sbgh_core::db::JobStore;
use sbgh_core::db::fleet::{
    ArtifactGrantRecord, EventIngest, FleetCompletion, FleetStore, FleetTerminalSubmission,
    FleetTerminalWrite, PreparedExecution, ProjectedReportMutation, ResolvedSpecSource,
    TerminalAcceptance, WorkerPolicyPatch, WorkerRegistration, WorkerRegistryMutation,
    WorkerRegistryStore,
};
use sbgh_core::models::{
    BuildTarget, GitRefKind, GithubAccountType, JobAxes, JobIntent, JobResult, JobSource, NewJob,
    QueuedEventDetail, TaskKind,
};
use sbgh_fleet::{
    ArtifactDescriptor, AttemptIdentity, BlockValidationPayload, BlockValidationResult,
    InclusiveRange, PROTOCOL_VERSION, ProgressRequest, ProgressUpdate, RegisterSessionRequest,
    ReliableEventEnvelope, ReliableEventPayload, ResourceFacts, TaskPayload, TerminalOutcome,
    ValidationEpoch, WorkerCapability,
};
use sbgh_postgres::db::{
    InstallationStore, NewInstallation, Pool, PostgresInstallationStore, setup_pg_db,
};
use sbgh_postgres::{PostgresFleetStore, PostgresJobStore, PreparedJobProvenance};
use uuid::Uuid;

async fn seed_install_repo(pool: &Pool, install_id: i64, repo_id: i64) {
    sqlx::query(
        "INSERT INTO allowed_installer
             (github_account_id, account_login, account_type)
         VALUES ($1, 'octo', 'organization')",
    )
    .bind(install_id)
    .execute(pool)
    .await
    .unwrap();
    let installations = PostgresInstallationStore::new(pool.clone());
    installations
        .upsert_installation(&NewInstallation {
            id: install_id,
            github_account_id: install_id,
            account_login: "octo".into(),
            account_type: GithubAccountType::Organization,
        })
        .await
        .unwrap();
    sqlx::query("INSERT INTO github_repo (id, owner, name) VALUES ($1, 'o', 'r')")
        .bind(repo_id)
        .execute(pool)
        .await
        .unwrap();
    installations
        .add_or_restore_membership(install_id, repo_id)
        .await
        .unwrap();
}

#[tokio::test]
async fn registry_identity_revocation_is_irreversible_and_immediate() {
    let (_db, pool) = setup_pg_db().await;
    let store = PostgresFleetStore::new(pool.clone());
    let worker_id = Uuid::new_v4();
    let identity_digest = [0x31; 32];
    assert_eq!(
        store
            .create_worker(&registration(worker_id))
            .await
            .unwrap(),
        WorkerRegistryMutation::Applied
    );
    assert_eq!(
        store
            .authorize_identity(worker_id, identity_digest)
            .await
            .unwrap(),
        WorkerRegistryMutation::Applied
    );
    assert!(
        store
            .authorize_worker(identity_digest)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        store
            .revoke_identity(worker_id, identity_digest)
            .await
            .unwrap(),
        WorkerRegistryMutation::Busy,
        "the final identity of an enabled worker requires drain"
    );
    store
        .set_worker_draining(worker_id, true)
        .await
        .unwrap();
    assert_eq!(
        store
            .revoke_identity(worker_id, identity_digest)
            .await
            .unwrap(),
        WorkerRegistryMutation::Applied
    );
    assert!(
        store
            .authorize_worker(identity_digest)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .authorize_identity(worker_id, identity_digest)
            .await
            .unwrap(),
        WorkerRegistryMutation::Conflict
    );
    let deletion = sqlx::query("DELETE FROM worker_identity_key WHERE identity_key_sha256 = $1")
        .bind(identity_digest.as_slice())
        .execute(&pool)
        .await
        .unwrap_err()
        .to_string();
    assert!(deletion.contains("immutable audit history"), "{deletion}");
}

#[tokio::test]
async fn worker_creation_is_idempotent_but_rejects_conflicting_reuse() {
    let (_db, pool) = setup_pg_db().await;
    let store = PostgresFleetStore::new(pool);
    let worker_id = Uuid::new_v4();
    let registration = registration(worker_id);
    assert_eq!(
        store
            .create_worker(&registration)
            .await
            .unwrap(),
        WorkerRegistryMutation::Applied
    );
    assert_eq!(
        store
            .create_worker(&registration)
            .await
            .unwrap(),
        WorkerRegistryMutation::Unchanged
    );
    let mut conflicting = registration;
    conflicting.display_name = "different".into();
    assert_eq!(
        store
            .create_worker(&conflicting)
            .await
            .unwrap(),
        WorkerRegistryMutation::Conflict
    );
}

#[tokio::test]
async fn concurrent_identity_ownership_elects_one_worker() {
    let (_db, pool) = setup_pg_db().await;
    let store = PostgresFleetStore::new(pool.clone());
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();
    store
        .create_worker(&registration(first))
        .await
        .unwrap();
    store
        .create_worker(&registration(second))
        .await
        .unwrap();
    let identity_digest = [0x42; 32];
    let (left, right) = tokio::join!(
        store.authorize_identity(first, identity_digest),
        store.authorize_identity(second, identity_digest),
    );
    let mut outcomes = [left.unwrap(), right.unwrap()];
    outcomes.sort_by_key(|outcome| match outcome {
        WorkerRegistryMutation::Applied => 0,
        WorkerRegistryMutation::Conflict => 1,
        _ => 2,
    });
    assert_eq!(outcomes, [WorkerRegistryMutation::Applied, WorkerRegistryMutation::Conflict]);
}

#[tokio::test]
async fn live_session_binds_identity_and_requires_quiescent_revocation() {
    let (_db, pool) = setup_pg_db().await;
    let store = PostgresFleetStore::new(pool.clone());
    let worker_id = Uuid::new_v4();
    let session_id = Uuid::new_v4();
    let identity_digest = [0x53; 32];
    store
        .create_worker(&registration(worker_id))
        .await
        .unwrap();
    store
        .authorize_identity(worker_id, identity_digest)
        .await
        .unwrap();
    <PostgresFleetStore as FleetStore>::register_session(
        &store,
        worker_id,
        identity_digest,
        &session(worker_id, session_id),
        Duration::minutes(5),
    )
    .await
    .unwrap();
    let persisted: Vec<u8> = sqlx::query_scalar(
        "SELECT identity_key_sha256 FROM worker_session WHERE worker_session_id = $1",
    )
    .bind(session_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(persisted, identity_digest);
    assert_eq!(
        store
            .revoke_identity(worker_id, identity_digest)
            .await
            .unwrap(),
        WorkerRegistryMutation::Busy
    );
    store
        .set_worker_draining(worker_id, true)
        .await
        .unwrap();
    assert_eq!(
        store
            .revoke_identity(worker_id, identity_digest)
            .await
            .unwrap(),
        WorkerRegistryMutation::Applied
    );
    assert!(
        !store
            .session_is_active(worker_id, session_id)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn emergency_revocation_withdraws_authorization_and_expires_the_live_session() {
    let (_db, pool) = setup_pg_db().await;
    let store = PostgresFleetStore::new(pool);
    let worker_id = Uuid::new_v4();
    let session_id = Uuid::new_v4();
    let identity_digest = [0x59; 32];
    store
        .create_worker(&registration(worker_id))
        .await
        .unwrap();
    store
        .authorize_identity(worker_id, identity_digest)
        .await
        .unwrap();
    <PostgresFleetStore as FleetStore>::register_session(
        &store,
        worker_id,
        identity_digest,
        &session(worker_id, session_id),
        Duration::minutes(5),
    )
    .await
    .unwrap();

    assert_eq!(
        store
            .emergency_revoke_identity(worker_id, identity_digest)
            .await
            .unwrap(),
        WorkerRegistryMutation::Applied
    );
    assert!(
        store
            .authorize_worker(identity_digest)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        !store
            .session_is_active(worker_id, session_id)
            .await
            .unwrap()
    );
    let identitys = store
        .worker_identities(worker_id)
        .await
        .unwrap();
    assert_eq!(identitys.len(), 1);
    assert!(
        identitys[0]
            .revoked_at
            .is_some()
    );
    assert!(identitys[0].created_at <= Utc::now());
}

#[tokio::test]
async fn emergency_disable_reuses_lease_expiry_fencing_and_cleanup() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresFleetStore::new(pool.clone());
    let worker_id = Uuid::new_v4();
    let session_id = Uuid::new_v4();
    let identity_digest = [0x5a; 32];
    let job_id = Uuid::new_v4();
    store
        .create_worker(&registration(worker_id))
        .await
        .unwrap();
    store
        .authorize_identity(worker_id, identity_digest)
        .await
        .unwrap();
    <PostgresFleetStore as FleetStore>::register_session(
        &store,
        worker_id,
        identity_digest,
        &session(worker_id, session_id),
        Duration::minutes(5),
    )
    .await
    .unwrap();
    enqueue_build(&store, job_id).await;
    running_attempt(&store, worker_id, session_id, job_id).await;

    assert_eq!(
        store
            .emergency_disable_worker(worker_id)
            .await
            .unwrap(),
        WorkerRegistryMutation::Applied
    );
    assert!(
        store
            .authorize_worker(identity_digest)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .expire_stale_attempts()
            .await
            .unwrap(),
        1
    );
    let pending_cleanup: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM worker_cleanup_obligation
          WHERE worker_id = $1 AND status = 'pending'",
    )
    .bind(worker_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(pending_cleanup, 1);
}

#[tokio::test]
async fn drain_policy_update_serializes_with_offer_polling() {
    let (_db, pool) = setup_pg_db().await;
    let store = PostgresFleetStore::new(pool.clone());
    let worker_id = Uuid::new_v4();
    let session_id = Uuid::new_v4();
    let identity_digest = [0x5b; 32];
    store
        .create_worker(&registration(worker_id))
        .await
        .unwrap();
    store
        .authorize_identity(worker_id, identity_digest)
        .await
        .unwrap();
    <PostgresFleetStore as FleetStore>::register_session(
        &store,
        worker_id,
        identity_digest,
        &session(worker_id, session_id),
        Duration::minutes(5),
    )
    .await
    .unwrap();

    let mut policy_tx = pool.begin().await.unwrap();
    sqlx::query("SELECT 1 FROM worker_registry WHERE worker_id = $1 FOR UPDATE")
        .bind(worker_id)
        .fetch_one(&mut *policy_tx)
        .await
        .unwrap();
    let polling_store = store.clone();
    let poll = tokio::spawn(async move {
        polling_store
            .poll_offer(worker_id, session_id, Duration::seconds(30), Duration::seconds(60))
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(!poll.is_finished(), "poll must wait for the policy row lock");
    sqlx::query(
        "UPDATE worker_registry
            SET draining = TRUE, updated_at = NOW()
          WHERE worker_id = $1",
    )
    .bind(worker_id)
    .execute(&mut *policy_tx)
    .await
    .unwrap();
    policy_tx
        .commit()
        .await
        .unwrap();

    assert!(
        poll.await
            .unwrap()
            .unwrap()
            .is_none()
    );
    let status: String =
        sqlx::query_scalar("SELECT status::text FROM worker_session WHERE worker_session_id = $1")
            .bind(session_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(status, "draining");
}

#[tokio::test]
async fn enabling_requires_an_active_identity_and_policy_changes_require_drain() {
    let (_db, pool) = setup_pg_db().await;
    let store = PostgresFleetStore::new(pool);
    let worker_id = Uuid::new_v4();
    let mut inert = registration(worker_id);
    inert.enabled = false;
    inert.draining = true;
    store
        .create_worker(&inert)
        .await
        .unwrap();
    assert_eq!(
        store
            .update_worker(
                worker_id,
                &WorkerPolicyPatch {
                    enabled: Some(true),
                    ..WorkerPolicyPatch::default()
                },
            )
            .await
            .unwrap(),
        WorkerRegistryMutation::MissingIdentity
    );
    store
        .authorize_identity(worker_id, [0x64; 32])
        .await
        .unwrap();
    assert_eq!(
        store
            .update_worker(
                worker_id,
                &WorkerPolicyPatch {
                    enabled: Some(true),
                    ..WorkerPolicyPatch::default()
                },
            )
            .await
            .unwrap(),
        WorkerRegistryMutation::Applied
    );
    assert_eq!(
        store
            .update_worker(
                worker_id,
                &WorkerPolicyPatch {
                    allowed_capabilities: Some(vec![WorkerCapability::BlockValidation]),
                    ..WorkerPolicyPatch::default()
                },
            )
            .await
            .unwrap(),
        WorkerRegistryMutation::Applied,
        "worker remains drained until the operator explicitly undrains it"
    );
}

fn registration(worker_id: Uuid) -> WorkerRegistration {
    WorkerRegistration {
        worker_id,
        display_name: "loopback".into(),
        allowed_capabilities: vec![WorkerCapability::BuildOnly],
        measurement_profile: None,
        enabled: true,
        draining: false,
    }
}

fn session(_worker_id: Uuid, worker_session_id: Uuid) -> RegisterSessionRequest {
    RegisterSessionRequest {
        protocol_version: PROTOCOL_VERSION,
        worker_session_id,
        software_version: env!("CARGO_PKG_VERSION").into(),
        advertised_capabilities: BTreeSet::from([WorkerCapability::BuildOnly]),
        resources: ResourceFacts {
            logical_cpus: 4,
            memory_bytes: 8 * 1024 * 1024 * 1024,
        },
    }
}

async fn enqueue_build(store: &PostgresFleetStore, job_id: Uuid) {
    let payload = TaskPayload::BuildOnly;
    let payload_hash = sbgh_fleet::payload_digest(&payload).unwrap();
    store
        .enqueue_prepared_job(
            job_id,
            &NewJob {
                github_installation_id: 100,
                github_repo_id: 10,
                axes: JobAxes {
                    source: JobSource::Daemon,
                    intent: JobIntent::CacheWarm,
                    task_kind: TaskKind::BuildOnly,
                    build_target: BuildTarget::StacksBench,
                },
                git_ref_kind: GitRefKind::Commit,
                git_ref_display: "1111111111111111111111111111111111111111".into(),
                git_commit_hash: Some("1111111111111111111111111111111111111111".into()),
                git_committed_at: None,
                workload_key: None,
            },
            &serde_json::to_value(QueuedEventDetail::CacheWarm {
                trigger_id: 1,
                git_ref: "main".into(),
                commit: "1111111111111111111111111111111111111111".into(),
                build_target: BuildTarget::StacksBench,
            })
            .unwrap(),
            &PreparedExecution {
                job_id,
                commit: "1111111111111111111111111111111111111111".into(),
                payload,
                payload_hash,
            },
            &PreparedJobProvenance::default(),
        )
        .await
        .unwrap();
}

async fn enqueue_benchmark(store: &PostgresFleetStore, job_id: Uuid) {
    let payload = TaskPayload::Benchmark(sbgh_fleet::BenchmarkPayload {
        effective_args: vec!["--mine-microblocks".into()],
        workload_key: Some("workload-v1".into()),
        sqlite_seed_key: None,
        shared_baseline_calibration: false,
        baseline_calibration_id: None,
        run_index: 0,
        requested_run_count: 1,
    });
    let payload_hash = sbgh_fleet::payload_digest(&payload).unwrap();
    store
        .enqueue_prepared_job(
            job_id,
            &NewJob {
                github_installation_id: 100,
                github_repo_id: 10,
                axes: JobAxes {
                    source: JobSource::Daemon,
                    intent: JobIntent::AdhocBenchmark,
                    task_kind: TaskKind::Benchmark,
                    build_target: BuildTarget::StacksBench,
                },
                git_ref_kind: GitRefKind::Commit,
                git_ref_display: "1111111111111111111111111111111111111111".into(),
                git_commit_hash: Some("1111111111111111111111111111111111111111".into()),
                git_committed_at: None,
                workload_key: Some("workload-v1".into()),
            },
            &serde_json::to_value(QueuedEventDetail::SlackAdhoc {
                channel: "C1".into(),
                message_ts: "1.0".into(),
                reporting_identity: None,
                bench_args: vec!["--mine-microblocks".into()],
                clean_repetitions: 1,
            })
            .unwrap(),
            &PreparedExecution {
                job_id,
                commit: "1111111111111111111111111111111111111111".into(),
                payload,
                payload_hash,
            },
            &PreparedJobProvenance::default(),
        )
        .await
        .unwrap();
}

async fn enqueue_slack_block_validation(store: &PostgresFleetStore, job_id: Uuid) {
    let selection = sbgh_fleet::BlockValidationSelection::Range {
        range: InclusiveRange { start: 100, end: 101 },
    };
    let payload = TaskPayload::BlockValidation(BlockValidationPayload {
        selection: selection.clone(),
        timeout_secs: 60,
    });
    store
        .enqueue_prepared_job(
            job_id,
            &NewJob {
                github_installation_id: 100,
                github_repo_id: 10,
                axes: JobAxes {
                    source: JobSource::Slack,
                    intent: JobIntent::BlockValidation,
                    task_kind: TaskKind::BlockValidation,
                    build_target: BuildTarget::StacksInspect,
                },
                git_ref_kind: GitRefKind::Commit,
                git_ref_display: "1111111111111111111111111111111111111111".into(),
                git_commit_hash: Some("1111111111111111111111111111111111111111".into()),
                git_committed_at: None,
                workload_key: None,
            },
            &serde_json::to_value(QueuedEventDetail::BlockValidation { selection }).unwrap(),
            &PreparedExecution {
                job_id,
                commit: "1111111111111111111111111111111111111111".into(),
                payload: payload.clone(),
                payload_hash: sbgh_fleet::payload_digest(&payload).unwrap(),
            },
            &PreparedJobProvenance::default(),
        )
        .await
        .unwrap();
}

async fn running_attempt(
    store: &PostgresFleetStore,
    worker_id: Uuid,
    session_id: Uuid,
    job_id: Uuid,
) -> (AttemptIdentity, Uuid) {
    let offered = store
        .poll_offer(worker_id, session_id, Duration::seconds(30), Duration::seconds(60))
        .await
        .unwrap()
        .expect("prepared compatible job must be offered");
    assert_eq!(offered.offer.job_id, job_id);
    assert!(
        store
            .accept_offer(worker_id, &offered.offer.identity, Duration::seconds(60))
            .await
            .unwrap()
    );
    (offered.offer.identity, offered.offer.trace_id)
}

fn event(
    identity: &AttemptIdentity,
    trace_id: Uuid,
    reliable_seq: u64,
    payload: ReliableEventPayload,
) -> ReliableEventEnvelope {
    ReliableEventEnvelope {
        identity: identity.clone(),
        trace_id,
        reliable_seq,
        payload_digest: sbgh_fleet::payload_digest(&payload).unwrap(),
        payload,
        worker_timestamp_ms: Utc::now().timestamp_millis(),
    }
}

#[tokio::test]
async fn prepared_execution_payload_is_immutable_after_enqueue() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresFleetStore::new(pool.clone());
    let job_id = Uuid::new_v4();
    enqueue_benchmark(&store, job_id).await;

    let changed = TaskPayload::Benchmark(sbgh_fleet::BenchmarkPayload {
        effective_args: vec!["--count".into(), "999".into()],
        workload_key: Some("drifted".into()),
        sqlite_seed_key: None,
        shared_baseline_calibration: false,
        baseline_calibration_id: None,
        run_index: 0,
        requested_run_count: 1,
    });
    assert!(
        !store
            .prepare_execution(&PreparedExecution {
                job_id,
                commit: "2222222222222222222222222222222222222222".into(),
                payload_hash: sbgh_fleet::payload_digest(&changed).unwrap(),
                payload: changed,
            })
            .await
            .unwrap(),
        "an already-prepared job must reject ref/default drift"
    );
    let stored: (String, serde_json::Value) =
        sqlx::query_as("SELECT git_commit_hash, execution_payload FROM job WHERE id = $1")
            .bind(job_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(stored.0, "1111111111111111111111111111111111111111");
    assert_eq!(
        serde_json::from_value::<TaskPayload>(stored.1).unwrap(),
        TaskPayload::Benchmark(sbgh_fleet::BenchmarkPayload {
            effective_args: vec!["--mine-microblocks".into()],
            workload_key: Some("workload-v1".into()),
            sqlite_seed_key: None,
            shared_baseline_calibration: false,
            baseline_calibration_id: None,
            run_index: 0,
            requested_run_count: 1,
        })
    );
}

#[tokio::test]
async fn scheduling_unit_sources_are_frozen_once_before_lazy_runs() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let jobs = PostgresJobStore::new(pool.clone());
    let job = jobs
        .insert_job(&NewJob {
            github_installation_id: 100,
            github_repo_id: 10,
            axes: JobAxes {
                source: JobSource::Slack,
                intent: JobIntent::AdhocBenchmark,
                task_kind: TaskKind::Benchmark,
                build_target: BuildTarget::StacksBench,
            },
            git_ref_kind: GitRefKind::Branch,
            git_ref_display: "develop".into(),
            git_commit_hash: None,
            git_committed_at: None,
            workload_key: Some("workload-v1".into()),
        })
        .await
        .unwrap();
    let store = PostgresFleetStore::new(pool.clone());
    let frozen = ResolvedSpecSource {
        spec_id: job.task_spec_id,
        commit: "1111111111111111111111111111111111111111".into(),
    };
    assert!(
        store
            .freeze_submission_sources(job.task_submission_id, std::slice::from_ref(&frozen))
            .await
            .unwrap()
    );
    assert!(
        store
            .freeze_submission_sources(job.task_submission_id, std::slice::from_ref(&frozen))
            .await
            .unwrap(),
        "a lost response must make source freezing idempotent"
    );
    assert!(
        store
            .freeze_submission_sources(
                job.task_submission_id,
                &[ResolvedSpecSource {
                    spec_id: job.task_spec_id,
                    commit: "2222222222222222222222222222222222222222".into(),
                }],
            )
            .await
            .is_err(),
        "a mutable ref cannot be re-resolved after the scheduling unit is frozen"
    );
    let stored: (Option<String>, Option<String>) = sqlx::query_as(
        r#"
        SELECT spec.git_commit_hash, job.git_commit_hash
          FROM task_spec spec
          JOIN job ON job.task_spec_id = spec.id
         WHERE job.id = $1
        "#,
    )
    .bind(job.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(stored.0.as_deref(), Some(frozen.commit.as_str()));
    assert_eq!(stored.1.as_deref(), Some(frozen.commit.as_str()));
}

#[tokio::test]
async fn response_loss_and_successor_session_are_safely_fenced() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresFleetStore::new(pool.clone());
    let worker_id = Uuid::new_v4();
    let first_session = Uuid::new_v4();
    store
        .seed_worker(&registration(worker_id))
        .await
        .unwrap();
    store
        .register_session(worker_id, &session(worker_id, first_session), Duration::minutes(5))
        .await
        .unwrap();
    let stored_resources: serde_json::Value = sqlx::query_scalar(
        "SELECT resource_facts FROM worker_session WHERE worker_session_id = $1",
    )
    .bind(first_session)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        stored_resources,
        serde_json::json!({
            "logical_cpus": 4,
            "memory_bytes": 8_u64 * 1024 * 1024 * 1024,
        })
    );
    let job_id = Uuid::new_v4();
    enqueue_build(&store, job_id).await;

    let first_offer = store
        .poll_offer(worker_id, first_session, Duration::seconds(30), Duration::seconds(60))
        .await
        .unwrap()
        .unwrap();
    let replayed_offer = store
        .poll_offer(worker_id, first_session, Duration::seconds(30), Duration::seconds(60))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        replayed_offer
            .offer
            .identity
            .attempt_id,
        first_offer
            .offer
            .identity
            .attempt_id,
        "poll response loss must replay one offer"
    );
    assert!(
        store
            .accept_offer(worker_id, &first_offer.offer.identity, Duration::seconds(60))
            .await
            .unwrap()
    );
    assert!(
        store
            .accept_offer(worker_id, &first_offer.offer.identity, Duration::seconds(60))
            .await
            .unwrap(),
        "accept response loss must be idempotent"
    );
    let predecessor_phase = event(
        &first_offer.offer.identity,
        first_offer.offer.trace_id,
        1,
        ReliableEventPayload::Phase {
            label: "predecessor phase".into(),
            elapsed_ms: 10,
        },
    );
    assert_eq!(
        store
            .ingest_reliable_event(worker_id, &predecessor_phase)
            .await
            .unwrap(),
        EventIngest::Inserted
    );
    let fetched_before_fence = store
        .unprojected_events(10)
        .await
        .unwrap();
    assert_eq!(fetched_before_fence.len(), 1);

    let successor = Uuid::new_v4();
    store
        .register_session(worker_id, &session(worker_id, successor), Duration::minutes(5))
        .await
        .unwrap();
    assert!(
        !store
            .attempt_projection_is_authoritative(fetched_before_fence[0].attempt_id)
            .await
            .unwrap(),
        "a mutation fetched before fencing must fail the pre-side-effect authority recheck"
    );
    assert_eq!(
        store
            .ingest_reliable_event(worker_id, &predecessor_phase)
            .await
            .unwrap(),
        EventIngest::Stale,
        "a fenced attempt must return a typed stale outcome instead of a retryable store error"
    );
    assert!(
        store
            .unprojected_events(10)
            .await
            .unwrap()
            .is_empty(),
        "a fenced predecessor's phase cannot regress the successor's reporting surface"
    );
    let discarded: Option<String> = sqlx::query_scalar(
        "SELECT projection_discarded_reason
           FROM worker_attempt_event
          WHERE attempt_id = $1 AND reliable_seq = 1",
    )
    .bind(
        first_offer
            .offer
            .identity
            .attempt_id,
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(discarded.as_deref(), Some("predecessor worker session was fenced"));
    assert!(
        store
            .heartbeat_attempt(worker_id, &first_offer.offer.identity, Duration::seconds(60), None,)
            .await
            .unwrap()
            .is_none(),
        "a successor process must fence its predecessor"
    );
    let obligations = store
        .cleanup_obligations(worker_id, successor)
        .await
        .unwrap();
    assert_eq!(obligations.len(), 1);
    assert!(
        !store
            .complete_cleanup(worker_id, Uuid::new_v4(), obligations[0].id)
            .await
            .unwrap(),
        "cleanup requires the current authorized session"
    );
    assert!(
        store
            .complete_cleanup(worker_id, successor, obligations[0].id)
            .await
            .unwrap()
    );
    let successor_offer = store
        .poll_offer(worker_id, successor, Duration::seconds(30), Duration::seconds(60))
        .await
        .unwrap()
        .unwrap();
    assert_ne!(
        successor_offer
            .offer
            .identity
            .attempt_id,
        first_offer
            .offer
            .identity
            .attempt_id
    );
    assert!(
        successor_offer
            .offer
            .identity
            .fencing_generation
            > first_offer
                .offer
                .identity
                .fencing_generation
    );
}

#[tokio::test]
async fn idle_drain_is_observable_and_stops_new_claims() {
    let (_db, pool) = setup_pg_db().await;
    let store = PostgresFleetStore::new(pool.clone());
    let worker_id = Uuid::new_v4();
    let session_id = Uuid::new_v4();
    store
        .seed_worker(&registration(worker_id))
        .await
        .unwrap();
    store
        .register_session(worker_id, &session(worker_id, session_id), Duration::minutes(5))
        .await
        .unwrap();
    assert_eq!(
        store
            .set_all_workers_draining(true)
            .await
            .unwrap(),
        1
    );
    assert!(
        store
            .poll_offer(worker_id, session_id, Duration::seconds(30), Duration::seconds(60),)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .session_is_draining(worker_id, session_id)
            .await
            .unwrap()
    );
    assert!(
        store
            .session_is_active(worker_id, session_id)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn drained_capability_reduction_closes_live_session() {
    let (_db, pool) = setup_pg_db().await;
    let store = PostgresFleetStore::new(pool);
    let worker_id = Uuid::new_v4();
    let session_id = Uuid::new_v4();
    store
        .seed_worker(&registration(worker_id))
        .await
        .unwrap();
    store
        .register_session(worker_id, &session(worker_id, session_id), Duration::minutes(5))
        .await
        .unwrap();

    assert_eq!(
        store
            .update_worker(
                worker_id,
                &WorkerPolicyPatch {
                    allowed_capabilities: Some(vec![WorkerCapability::Benchmark]),
                    measurement_profile: Some(Some("profile-v1".into())),
                    ..WorkerPolicyPatch::default()
                },
            )
            .await
            .unwrap(),
        WorkerRegistryMutation::Busy
    );
    store
        .set_worker_draining(worker_id, true)
        .await
        .unwrap();
    assert_eq!(
        store
            .update_worker(
                worker_id,
                &WorkerPolicyPatch {
                    allowed_capabilities: Some(vec![WorkerCapability::Benchmark]),
                    measurement_profile: Some(Some("profile-v1".into())),
                    ..WorkerPolicyPatch::default()
                },
            )
            .await
            .unwrap(),
        WorkerRegistryMutation::Applied
    );
    assert!(
        !store
            .session_is_active(worker_id, session_id)
            .await
            .unwrap(),
        "a policy change is effective only after closing the prior session"
    );
}

#[tokio::test]
async fn reliable_prefix_and_terminal_submission_are_idempotent() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresFleetStore::new(pool);
    let worker_id = Uuid::new_v4();
    let session_id = Uuid::new_v4();
    store
        .seed_worker(&registration(worker_id))
        .await
        .unwrap();
    store
        .register_session(worker_id, &session(worker_id, session_id), Duration::minutes(5))
        .await
        .unwrap();
    let job_id = Uuid::new_v4();
    enqueue_build(&store, job_id).await;
    let (identity, trace_id) = running_attempt(&store, worker_id, session_id, job_id).await;
    assert!(
        store
            .request_cancel(job_id)
            .await
            .unwrap()
    );
    assert!(
        store
            .request_cancel(job_id)
            .await
            .unwrap(),
        "lost cancellation responses must be retry-safe"
    );
    assert_eq!(
        store
            .heartbeat_attempt(worker_id, &identity, Duration::seconds(60), Some(0))
            .await
            .unwrap()
            .unwrap()
            .desired_state,
        sbgh_fleet::DesiredState::Cancel
    );

    let terminal = TerminalOutcome::Cancelled { reason: "operator test".into() };
    let terminal_payload = ReliableEventPayload::Terminal {
        outcome_digest: sbgh_fleet::payload_digest(&terminal).unwrap(),
    };
    let second = event(&identity, trace_id, 2, terminal_payload.clone());
    assert_eq!(
        store
            .ingest_reliable_event(worker_id, &second)
            .await
            .unwrap(),
        EventIngest::Inserted
    );
    assert!(
        store
            .unprojected_events(10)
            .await
            .unwrap()
            .is_empty(),
        "projection must stop at a reliable-sequence gap"
    );
    assert!(
        store
            .accept_terminal(
                worker_id,
                &identity,
                &FleetTerminalSubmission {
                    reliable_seq: 2,
                    payload_digest: &second.payload_digest,
                    outcome: &terminal,
                    artifacts: &[],
                    write: &FleetTerminalWrite::Cancelled { remark: "operator test".into() },
                },
            )
            .await
            .is_err(),
        "terminal acceptance must require a contiguous durable prefix"
    );

    let first = event(
        &identity,
        trace_id,
        1,
        ReliableEventPayload::Phase {
            label: "build".into(),
            elapsed_ms: 10,
        },
    );
    store
        .ingest_reliable_event(worker_id, &first)
        .await
        .unwrap();
    assert_eq!(
        store
            .ingest_reliable_event(worker_id, &first)
            .await
            .unwrap(),
        EventIngest::Duplicate
    );
    let conflicting = event(
        &identity,
        trace_id,
        1,
        ReliableEventPayload::Phase {
            label: "different".into(),
            elapsed_ms: 11,
        },
    );
    assert_eq!(
        store
            .ingest_reliable_event(worker_id, &conflicting)
            .await
            .unwrap(),
        EventIngest::Conflict,
        "sequence reuse with different content is a typed non-retryable conflict"
    );
    assert_eq!(
        store
            .unprojected_events(10)
            .await
            .unwrap()[0]
            .reliable_seq,
        1
    );
    assert!(
        store
            .mark_event_projected(identity.attempt_id, 1)
            .await
            .unwrap()
    );
    let seed = store
        .report_projection_seed(identity.attempt_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(seed.mutation_count, 1);
    assert!(matches!(
        seed.latest,
        ProjectedReportMutation::Phase(ReliableEventPayload::Phase { ref label, .. })
            if label == "build"
    ));
    let progress = ProgressRequest {
        identity: identity.clone(),
        trace_id,
        progress_seq: 1,
        update: ProgressUpdate {
            workflow_step: "run".into(),
            run_index: 0,
            requested_run_count: 1,
            phase: "validate".into(),
            progress: 5,
            total: Some(10),
            message: None,
        },
    };
    assert!(
        store
            .ingest_progress(worker_id, &progress)
            .await
            .unwrap()
    );
    assert!(
        store
            .mark_progress_projected(identity.attempt_id, 1)
            .await
            .unwrap()
    );
    let seed = store
        .report_projection_seed(identity.attempt_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(seed.mutation_count, 2);
    assert!(matches!(
        seed.latest,
        ProjectedReportMutation::Progress(ProgressUpdate { progress: 5, .. })
    ));
    assert_eq!(
        store
            .unprojected_events(10)
            .await
            .unwrap()[0]
            .reliable_seq,
        2
    );

    let write = FleetTerminalWrite::Cancelled { remark: "operator test".into() };
    let conflicting = TerminalOutcome::Cancelled {
        reason: "different outcome".into(),
    };
    assert!(
        store
            .accept_terminal(
                worker_id,
                &identity,
                &FleetTerminalSubmission {
                    reliable_seq: 2,
                    payload_digest: &second.payload_digest,
                    outcome: &conflicting,
                    artifacts: &[],
                    write: &FleetTerminalWrite::Cancelled {
                        remark: "different outcome".into(),
                    },
                },
            )
            .await
            .is_err(),
        "completion must match the outcome digest committed in the terminal event"
    );
    assert_eq!(
        store
            .accept_terminal(
                worker_id,
                &identity,
                &FleetTerminalSubmission {
                    reliable_seq: 2,
                    payload_digest: &second.payload_digest,
                    outcome: &terminal,
                    artifacts: &[],
                    write: &write,
                },
            )
            .await
            .unwrap(),
        TerminalAcceptance::Accepted
    );
    assert_eq!(
        store
            .accept_terminal(
                worker_id,
                &identity,
                &FleetTerminalSubmission {
                    reliable_seq: 2,
                    payload_digest: &second.payload_digest,
                    outcome: &terminal,
                    artifacts: &[],
                    write: &write,
                },
            )
            .await
            .unwrap(),
        TerminalAcceptance::Duplicate
    );
}

#[tokio::test]
async fn cancellation_terminalizes_queued_work_once_without_creating_an_attempt() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresFleetStore::new(pool.clone());
    let job_id = Uuid::new_v4();
    enqueue_build(&store, job_id).await;

    assert!(
        store
            .request_cancel(job_id)
            .await
            .unwrap()
    );
    assert!(
        store
            .request_cancel(job_id)
            .await
            .unwrap(),
        "queued cancellation retries are idempotent"
    );
    let status: String = sqlx::query_scalar("SELECT status::text FROM job WHERE id = $1")
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "cancelled");
    let attempts: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM worker_attempt WHERE job_id = $1")
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(attempts, 0, "queue-only cancellation must not synthesize an attempt");
    let cancellation_events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
           FROM job_event
          WHERE job_id = $1 AND event_kind = 'cancelled'",
    )
    .bind(job_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(cancellation_events, 1, "a retry must not duplicate the audit event");
}

#[tokio::test]
async fn cancellation_fences_an_offer_before_worker_execution_starts() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresFleetStore::new(pool.clone());
    let worker_id = Uuid::new_v4();
    let session_id = Uuid::new_v4();
    store
        .seed_worker(&registration(worker_id))
        .await
        .unwrap();
    store
        .register_session(worker_id, &session(worker_id, session_id), Duration::minutes(5))
        .await
        .unwrap();
    let job_id = Uuid::new_v4();
    enqueue_build(&store, job_id).await;
    let offered = store
        .poll_offer(worker_id, session_id, Duration::seconds(30), Duration::seconds(60))
        .await
        .unwrap()
        .unwrap();

    assert!(
        store
            .request_cancel(job_id)
            .await
            .unwrap()
    );
    assert!(
        !store
            .accept_offer(worker_id, &offered.offer.identity, Duration::seconds(60))
            .await
            .unwrap(),
        "an offer cannot start after cancellation committed first"
    );
    assert!(
        store
            .request_cancel(job_id)
            .await
            .unwrap(),
        "cancellation is idempotent"
    );
    let states: (String, String) = sqlx::query_as(
        "SELECT job.status::text, attempt.status::text
           FROM job
           JOIN worker_attempt attempt ON attempt.job_id = job.id
          WHERE job.id = $1",
    )
    .bind(job_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(states, ("cancelled".into(), "fenced".into()));
}

#[tokio::test]
async fn concurrent_accept_and_cancel_serialize_without_an_unsafe_intermediate_state() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresFleetStore::new(pool.clone());
    let worker_id = Uuid::new_v4();
    let session_id = Uuid::new_v4();
    store
        .seed_worker(&registration(worker_id))
        .await
        .unwrap();
    store
        .register_session(worker_id, &session(worker_id, session_id), Duration::minutes(5))
        .await
        .unwrap();
    let job_id = Uuid::new_v4();
    enqueue_build(&store, job_id).await;
    let offered = store
        .poll_offer(worker_id, session_id, Duration::seconds(30), Duration::seconds(60))
        .await
        .unwrap()
        .unwrap();

    let (accepted, cancelled) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(
            store.accept_offer(worker_id, &offered.offer.identity, Duration::seconds(60)),
            store.request_cancel(job_id),
        )
    })
    .await
    .expect("accept/cancel lock ordering must not deadlock");
    let accepted = accepted.unwrap();
    assert!(cancelled.unwrap());

    let states: (String, String) = sqlx::query_as(
        "SELECT job.status::text, attempt.status::text
           FROM job
           JOIN worker_attempt attempt ON attempt.job_id = job.id
          WHERE job.id = $1",
    )
    .bind(job_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    match (accepted, states) {
        (false, (job, attempt)) => {
            assert_eq!((job.as_str(), attempt.as_str()), ("cancelled", "fenced"));
        }
        (true, (job, attempt)) => {
            assert_eq!((job.as_str(), attempt.as_str()), ("running", "cancel_requested"));
        }
    }
}

#[tokio::test]
async fn terminal_after_orchestrator_lease_expiry_is_fenced() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresFleetStore::new(pool.clone());
    let worker_id = Uuid::new_v4();
    let session_id = Uuid::new_v4();
    store
        .seed_worker(&registration(worker_id))
        .await
        .unwrap();
    store
        .register_session(worker_id, &session(worker_id, session_id), Duration::minutes(5))
        .await
        .unwrap();
    let job_id = Uuid::new_v4();
    enqueue_build(&store, job_id).await;
    let (identity, trace_id) = running_attempt(&store, worker_id, session_id, job_id).await;
    let terminal = TerminalOutcome::Cancelled { reason: "late worker".into() };
    let terminal_event = event(
        &identity,
        trace_id,
        1,
        ReliableEventPayload::Terminal {
            outcome_digest: sbgh_fleet::payload_digest(&terminal).unwrap(),
        },
    );
    store
        .ingest_reliable_event(worker_id, &terminal_event)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE worker_attempt
            SET lease_expires_at = NOW() - INTERVAL '1 second'
          WHERE attempt_id = $1",
    )
    .bind(identity.attempt_id)
    .execute(&pool)
    .await
    .unwrap();

    assert_eq!(
        store
            .accept_terminal(
                worker_id,
                &identity,
                &FleetTerminalSubmission {
                    reliable_seq: 1,
                    payload_digest: &terminal_event.payload_digest,
                    outcome: &terminal,
                    artifacts: &[],
                    write: &FleetTerminalWrite::Cancelled { remark: "late worker".into() },
                },
            )
            .await
            .unwrap(),
        TerminalAcceptance::Stale
    );
    assert_eq!(
        store
            .expire_stale_attempts()
            .await
            .unwrap(),
        1
    );
    assert!(
        store
            .unprojected_events(10)
            .await
            .unwrap()
            .is_empty(),
        "expired-attempt events must never reach a successor's reporting surface"
    );
    let discarded: Option<String> = sqlx::query_scalar(
        "SELECT projection_discarded_reason
           FROM worker_attempt_event
          WHERE attempt_id = $1 AND reliable_seq = 1",
    )
    .bind(identity.attempt_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(discarded.as_deref(), Some("attempt lease expired before report projection"));
}

#[tokio::test]
async fn capability_routes_only_to_a_compatible_worker() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresFleetStore::new(pool.clone());
    let build_worker = Uuid::new_v4();
    let block_worker = Uuid::new_v4();
    let build_session = Uuid::new_v4();
    let block_session = Uuid::new_v4();
    store
        .seed_worker(&registration(build_worker))
        .await
        .unwrap();
    store
        .seed_worker(&WorkerRegistration {
            worker_id: block_worker,
            display_name: "block".into(),
            allowed_capabilities: vec![WorkerCapability::BlockValidation],
            measurement_profile: None,
            enabled: true,
            draining: false,
        })
        .await
        .unwrap();
    store
        .register_session(build_worker, &session(build_worker, build_session), Duration::minutes(5))
        .await
        .unwrap();
    store
        .register_session(
            block_worker,
            &RegisterSessionRequest {
                protocol_version: PROTOCOL_VERSION,
                worker_session_id: block_session,
                software_version: env!("CARGO_PKG_VERSION").into(),
                advertised_capabilities: BTreeSet::from([WorkerCapability::BlockValidation]),
                resources: ResourceFacts {
                    logical_cpus: 64,
                    memory_bytes: 256 * 1024 * 1024 * 1024,
                },
            },
            Duration::minutes(5),
        )
        .await
        .unwrap();

    let job_id = Uuid::new_v4();
    let payload = TaskPayload::BlockValidation(BlockValidationPayload {
        selection: sbgh_fleet::BlockValidationSelection::Range {
            range: InclusiveRange { start: 100, end: 199 },
        },
        timeout_secs: 60,
    });
    store
        .enqueue_prepared_job(
            job_id,
            &NewJob {
                github_installation_id: 100,
                github_repo_id: 10,
                axes: JobAxes {
                    source: JobSource::Daemon,
                    intent: JobIntent::BlockValidation,
                    task_kind: TaskKind::BlockValidation,
                    build_target: BuildTarget::StacksInspect,
                },
                git_ref_kind: GitRefKind::Commit,
                git_ref_display: "1111111111111111111111111111111111111111".into(),
                git_commit_hash: Some("1111111111111111111111111111111111111111".into()),
                git_committed_at: None,
                workload_key: None,
            },
            &serde_json::to_value(QueuedEventDetail::BlockValidation {
                selection: sbgh_fleet::BlockValidationSelection::Range {
                    range: InclusiveRange { start: 100, end: 199 },
                },
            })
            .unwrap(),
            &PreparedExecution {
                job_id,
                commit: "1111111111111111111111111111111111111111".into(),
                payload: payload.clone(),
                payload_hash: sbgh_fleet::payload_digest(&payload).unwrap(),
            },
            &PreparedJobProvenance::default(),
        )
        .await
        .unwrap();

    assert!(
        store
            .poll_offer(build_worker, build_session, Duration::seconds(30), Duration::seconds(60),)
            .await
            .unwrap()
            .is_none()
    );
    let offered = store
        .poll_offer(block_worker, block_session, Duration::seconds(30), Duration::seconds(60))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(offered.offer.job_id, job_id);
    assert_eq!(offered.offer.capability, WorkerCapability::BlockValidation);
    assert_eq!(offered.offer.requirements, sbgh_fleet::OfferRequirements::from(&payload));
    assert!(
        store
            .accept_offer(block_worker, &offered.offer.identity, Duration::seconds(60))
            .await
            .unwrap()
    );
    let block_result = BlockValidationResult {
        valid: true,
        checked_blocks: 100,
        invalid_blocks: Vec::new(),
        chainstate_origin: "vg0/mainnet-2026-07-28".into(),
        observed: sbgh_fleet::ObservedValidationIndex {
            pre_nakamoto_count: 100,
            nakamoto_count: 901,
        },
        resolved_range: InclusiveRange { start: 100, end: 199 },
        segments: vec![sbgh_fleet::ValidationEpochSegment {
            epoch: ValidationEpoch::Nakamoto,
            global_range: InclusiveRange { start: 100, end: 199 },
            local_range: InclusiveRange { start: 0, end: 99 },
        }],
        shard_count: 4,
        max_concurrency: 4,
    };
    let terminal = TerminalOutcome::Completed {
        summary: serde_json::json!({"chainstate_origin": block_result.chainstate_origin}),
        block_validation: Some(block_result.clone()),
    };
    let terminal_event = event(
        &offered.offer.identity,
        offered.offer.trace_id,
        1,
        ReliableEventPayload::Terminal {
            outcome_digest: sbgh_fleet::payload_digest(&terminal).unwrap(),
        },
    );
    store
        .ingest_reliable_event(block_worker, &terminal_event)
        .await
        .unwrap();
    assert_eq!(
        store
            .accept_terminal(
                block_worker,
                &offered.offer.identity,
                &FleetTerminalSubmission {
                    reliable_seq: 1,
                    payload_digest: &terminal_event.payload_digest,
                    outcome: &terminal,
                    artifacts: &[],
                    write: &FleetTerminalWrite::Completed(Box::new(FleetCompletion {
                        result: JobResult {
                            job_id,
                            run_json: None,
                            archive_dir: "job".into(),
                            created_at: Utc::now(),
                        },
                        metric: None,
                        baseline_calibration_id: None,
                        event_detail: None,
                        block_validation: Some(block_result),
                        artifact_manifest: Vec::new(),
                    })),
                },
            )
            .await
            .unwrap(),
        TerminalAcceptance::Accepted
    );
    let persisted: (String, i64, i64, i64, i32, i32) = sqlx::query_as(
        "SELECT chainstate_origin, pre_nakamoto_count, nakamoto_count,
                resolved_start, shard_count, max_concurrency
           FROM block_validation_result
          WHERE job_id = $1",
    )
    .bind(job_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(persisted, ("vg0/mainnet-2026-07-28".into(), 100, 901, 100, 4, 4));
}

#[tokio::test]
async fn slack_job_is_not_offered_until_canonical_report_message_is_durable() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresFleetStore::new(pool.clone());
    let worker_id = Uuid::new_v4();
    let session_id = Uuid::new_v4();
    store
        .seed_worker(&WorkerRegistration {
            worker_id,
            display_name: "block".into(),
            allowed_capabilities: vec![WorkerCapability::BlockValidation],
            measurement_profile: None,
            enabled: true,
            draining: false,
        })
        .await
        .unwrap();
    store
        .register_session(
            worker_id,
            &RegisterSessionRequest {
                protocol_version: PROTOCOL_VERSION,
                worker_session_id: session_id,
                software_version: env!("CARGO_PKG_VERSION").into(),
                advertised_capabilities: BTreeSet::from([WorkerCapability::BlockValidation]),
                resources: ResourceFacts {
                    logical_cpus: 64,
                    memory_bytes: 256 * 1024 * 1024 * 1024,
                },
            },
            Duration::minutes(5),
        )
        .await
        .unwrap();

    let job_id = Uuid::new_v4();
    enqueue_slack_block_validation(&store, job_id).await;
    assert!(
        store
            .poll_offer(worker_id, session_id, Duration::seconds(30), Duration::seconds(60))
            .await
            .unwrap()
            .is_none(),
        "Slack work without submission reporting provenance must stay queued"
    );

    let submission_id: Uuid =
        sqlx::query_scalar("SELECT task_submission_id FROM job WHERE id = $1")
            .bind(job_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    sqlx::query(
        r#"
        INSERT INTO task_submission_slack
            (task_submission_id, team_id, channel_id, request_message_ts,
             reporting_identity, report_message_ts)
        VALUES ($1, 'T1', 'C1', '1.0', $2, NULL)
        "#,
    )
    .bind(submission_id)
    .bind("d".repeat(64))
    .execute(&pool)
    .await
    .unwrap();
    assert!(
        store
            .poll_offer(worker_id, session_id, Duration::seconds(30), Duration::seconds(60))
            .await
            .unwrap()
            .is_none(),
        "routing provenance without a canonical report message must stay queued"
    );

    sqlx::query(
        "UPDATE task_submission_slack SET report_message_ts = '1.1' \
         WHERE task_submission_id = $1",
    )
    .bind(submission_id)
    .execute(&pool)
    .await
    .unwrap();
    let offered = store
        .poll_offer(worker_id, session_id, Duration::seconds(30), Duration::seconds(60))
        .await
        .unwrap()
        .expect("durably reportable Slack job should be offered");
    assert_eq!(offered.offer.job_id, job_id);
}

#[tokio::test]
async fn unreported_slack_admission_expires_but_a_durable_message_wins() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let fleet = PostgresFleetStore::new(pool.clone());

    let expired_job_id = Uuid::new_v4();
    enqueue_slack_block_validation(&fleet, expired_job_id).await;
    let expired_submission_id: Uuid =
        sqlx::query_scalar("SELECT task_submission_id FROM job WHERE id = $1")
            .bind(expired_job_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    sqlx::query(
        r#"
        INSERT INTO task_submission_slack
            (task_submission_id, team_id, channel_id, request_message_ts,
             reporting_identity)
        VALUES ($1, 'T1', 'C1', '1.0', $2)
        "#,
    )
    .bind(expired_submission_id)
    .bind("e".repeat(64))
    .execute(&pool)
    .await
    .unwrap();

    assert!(
        fleet
            .expire_unreported_slack_jobs(Duration::minutes(5), "reporting timed out")
            .await
            .unwrap()
            .is_empty(),
        "a fresh Slack admission must retain its grace period"
    );
    sqlx::query("UPDATE job SET created_at = NOW() - INTERVAL '6 minutes' WHERE id = $1")
        .bind(expired_job_id)
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        fleet
            .expire_unreported_slack_jobs(Duration::minutes(5), "reporting timed out")
            .await
            .unwrap(),
        vec![expired_job_id]
    );
    let terminal: (String, String) = sqlx::query_as(
        "SELECT job.status::text, event.remark
           FROM job
           JOIN job_event event ON event.job_id = job.id
          WHERE job.id = $1 AND event.event_kind = 'failed'",
    )
    .bind(expired_job_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(terminal, ("failed".into(), "reporting timed out".into()));

    let reportable_job_id = Uuid::new_v4();
    enqueue_slack_block_validation(&fleet, reportable_job_id).await;
    let reportable_submission_id: Uuid =
        sqlx::query_scalar("SELECT task_submission_id FROM job WHERE id = $1")
            .bind(reportable_job_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    sqlx::query(
        r#"
        INSERT INTO task_submission_slack
            (task_submission_id, team_id, channel_id, request_message_ts,
             reporting_identity, report_message_ts)
        VALUES ($1, 'T1', 'C1', '2.0', $2, '2.1')
        "#,
    )
    .bind(reportable_submission_id)
    .bind("f".repeat(64))
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("UPDATE job SET created_at = NOW() - INTERVAL '6 minutes' WHERE id = $1")
        .bind(reportable_job_id)
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        fleet
            .expire_unreported_slack_jobs(Duration::zero(), "connector unavailable")
            .await
            .unwrap()
            .is_empty(),
        "a durable canonical message must defeat admission expiry"
    );
    let status: String = sqlx::query_scalar("SELECT status::text FROM job WHERE id = $1")
        .bind(reportable_job_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "queued");
    assert_eq!(
        fleet
            .fail_queued_slack_jobs_without_connector("connector unavailable")
            .await
            .unwrap(),
        vec![reportable_job_id],
        "missing connector fails even work whose initial message was persisted"
    );
}

#[tokio::test]
async fn missing_slack_provenance_is_bounded_by_reporting_admission() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let fleet = PostgresFleetStore::new(pool.clone());
    let job_id = Uuid::new_v4();
    enqueue_slack_block_validation(&fleet, job_id).await;

    assert_eq!(
        fleet
            .expire_unreported_slack_jobs(Duration::zero(), "connector unavailable")
            .await
            .unwrap(),
        vec![job_id]
    );
    let status: String = sqlx::query_scalar("SELECT status::text FROM job WHERE id = $1")
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "failed");
}

#[tokio::test]
async fn explicit_submission_recovery_can_target_a_compatible_worker() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresFleetStore::new(pool.clone());
    let job_id = Uuid::new_v4();
    enqueue_benchmark(&store, job_id).await;
    let submission_id: Uuid =
        sqlx::query_scalar("SELECT task_submission_id FROM job WHERE id = $1")
            .bind(job_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    let request_digest = "a".repeat(64);
    sqlx::query(
        "UPDATE task_submission
            SET contract_version = 1, request_digest = $2
          WHERE id = $1",
    )
    .bind(submission_id)
    .bind(&request_digest)
    .execute(&pool)
    .await
    .unwrap();
    let incompatible_worker = Uuid::new_v4();
    store
        .seed_worker(&registration(incompatible_worker))
        .await
        .unwrap();
    assert!(
        store
            .recover_submission(
                submission_id,
                Some(incompatible_worker),
                "operator-approved host recovery",
            )
            .await
            .is_err(),
        "an explicit recovery target must be authorized for benchmark work"
    );
    let target_worker = Uuid::new_v4();
    store
        .seed_worker(&WorkerRegistration {
            worker_id: target_worker,
            display_name: "replacement benchmark host".into(),
            allowed_capabilities: vec![WorkerCapability::Benchmark],
            measurement_profile: Some("replacement-profile".into()),
            enabled: true,
            draining: false,
        })
        .await
        .unwrap();
    let recovery = store
        .recover_submission(submission_id, Some(target_worker), "operator-approved host recovery")
        .await
        .unwrap();
    assert_eq!(recovery.prior_submission_id, submission_id);
    assert_eq!(recovery.execution_generation, 2);
    let row: (i64, Option<Uuid>, Uuid, Option<i32>, Option<String>) = sqlx::query_as(
        "SELECT execution_generation, required_worker_id, recovery_of_submission_id,
                contract_version, request_digest
           FROM task_submission WHERE id = $1",
    )
    .bind(recovery.new_submission_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.0, 2);
    assert_eq!(row.1, Some(target_worker));
    assert_eq!(row.2, submission_id);
    assert_eq!(row.3, Some(1));
    assert_eq!(row.4.as_deref(), Some(request_digest.as_str()));
    let copied_hash: String =
        sqlx::query_scalar("SELECT execution_payload_hash FROM job WHERE id = $1")
            .bind(recovery.first_job_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        copied_hash,
        sbgh_fleet::payload_digest(&TaskPayload::Benchmark(sbgh_fleet::BenchmarkPayload {
            effective_args: vec!["--mine-microblocks".into()],
            workload_key: Some("workload-v1".into()),
            sqlite_seed_key: None,
            shared_baseline_calibration: false,
            baseline_calibration_id: None,
            run_index: 0,
            requested_run_count: 1,
        }))
        .unwrap()
    );
}

#[tokio::test]
async fn accepted_terminal_waits_for_durable_artifact_promotion() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresFleetStore::new(pool.clone());
    let worker_id = Uuid::new_v4();
    let session_id = Uuid::new_v4();
    store
        .seed_worker(&registration(worker_id))
        .await
        .unwrap();
    store
        .register_session(worker_id, &session(worker_id, session_id), Duration::minutes(5))
        .await
        .unwrap();
    let job_id = Uuid::new_v4();
    enqueue_build(&store, job_id).await;
    let (identity, trace_id) = running_attempt(&store, worker_id, session_id, job_id).await;
    let artifact = ArtifactDescriptor {
        key: format!("staging/{}/nonce/{job_id}/run.json", identity.attempt_id),
        logical_key: format!("{job_id}/run.json"),
        size: 7,
        sha256: "22".repeat(32),
    };
    let grant = ArtifactGrantRecord {
        attempt_id: identity.attempt_id,
        object_key: artifact.key.clone(),
        logical_key: artifact.logical_key.clone(),
        size: Some(artifact.size),
        sha256: Some(artifact.sha256.clone()),
        expires_at: Utc::now() + Duration::minutes(5),
    };
    assert!(
        store
            .record_artifact_grant(&ArtifactGrantRecord {
                expires_at: Utc::now() + Duration::minutes(10),
                ..grant.clone()
            })
            .await
            .unwrap()
    );
    assert!(
        store
            .record_artifact_grant(&grant)
            .await
            .unwrap(),
        "a lost grant response is safe to retry with the same identity"
    );
    assert!(
        !store
            .record_artifact_grant(&ArtifactGrantRecord {
                object_key: format!("{}-different", artifact.key),
                sha256: Some("33".repeat(32)),
                ..grant
            })
            .await
            .unwrap(),
        "one logical artifact cannot be rebound to different staged content"
    );
    let grant_rows: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM worker_artifact_staging WHERE attempt_id = $1")
            .bind(identity.attempt_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(grant_rows, 1, "grant retries must not leak staging rows");
    assert!(
        store
            .verify_artifact(&identity, &artifact)
            .await
            .unwrap()
    );
    let terminal = TerminalOutcome::Cancelled { reason: "test".into() };
    let terminal_event = event(
        &identity,
        trace_id,
        1,
        ReliableEventPayload::Terminal {
            outcome_digest: sbgh_fleet::payload_digest(&terminal).unwrap(),
        },
    );
    store
        .ingest_reliable_event(worker_id, &terminal_event)
        .await
        .unwrap();
    assert!(
        store
            .accept_terminal(
                worker_id,
                &identity,
                &FleetTerminalSubmission {
                    reliable_seq: 1,
                    payload_digest: &terminal_event.payload_digest,
                    outcome: &terminal,
                    artifacts: &[],
                    write: &FleetTerminalWrite::Cancelled { remark: "test".into() },
                },
            )
            .await
            .is_err(),
        "an accepted terminal cannot silently omit an already-verified artifact"
    );
    assert_eq!(
        store
            .accept_terminal(
                worker_id,
                &identity,
                &FleetTerminalSubmission {
                    reliable_seq: 1,
                    payload_digest: &terminal_event.payload_digest,
                    outcome: &terminal,
                    artifacts: std::slice::from_ref(&artifact),
                    write: &FleetTerminalWrite::Cancelled { remark: "test".into() },
                },
            )
            .await
            .unwrap(),
        TerminalAcceptance::Accepted
    );
    assert!(
        store
            .attempt_terminal_outcome(identity.attempt_id)
            .await
            .unwrap()
            .is_none(),
        "reporting must wait for accepted artifact promotion"
    );
    assert_eq!(
        store
            .accepted_terminal_manifest(identity.attempt_id)
            .await
            .unwrap()
            .unwrap(),
        vec![artifact.clone()]
    );
    let pending = store
        .pending_artifact_promotions(10)
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].attempt_id, identity.attempt_id);
    assert!(
        store
            .mark_artifacts_promoted(identity.attempt_id, std::slice::from_ref(&artifact))
            .await
            .unwrap()
    );
    assert!(
        store
            .attempt_terminal_outcome(identity.attempt_id)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        store
            .pending_artifact_promotions(10)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn staged_artifact_is_marked_reaped_only_after_external_delete_ack() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresFleetStore::new(pool.clone());
    let worker_id = Uuid::new_v4();
    let session_id = Uuid::new_v4();
    store
        .seed_worker(&registration(worker_id))
        .await
        .unwrap();
    store
        .register_session(worker_id, &session(worker_id, session_id), Duration::minutes(5))
        .await
        .unwrap();
    let job_id = Uuid::new_v4();
    enqueue_build(&store, job_id).await;
    let (identity, _) = running_attempt(&store, worker_id, session_id, job_id).await;
    let object_key = format!("staging/{}/nonce/{job_id}/run.json", identity.attempt_id);
    store
        .record_artifact_grant(&ArtifactGrantRecord {
            attempt_id: identity.attempt_id,
            object_key: object_key.clone(),
            logical_key: format!("{job_id}/run.json"),
            size: Some(7),
            sha256: Some("33".repeat(32)),
            expires_at: Utc::now() - Duration::days(2),
        })
        .await
        .unwrap();
    sqlx::query("UPDATE worker_attempt SET status = 'fenced' WHERE attempt_id = $1")
        .bind(identity.attempt_id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE worker_artifact_staging
            SET created_at = NOW() - INTERVAL '2 days'
          WHERE attempt_id = $1",
    )
    .bind(identity.attempt_id)
    .execute(&pool)
    .await
    .unwrap();

    assert_eq!(
        store
            .staged_artifacts_due_for_reap(Utc::now() - Duration::hours(24))
            .await
            .unwrap(),
        vec![object_key.clone()]
    );
    let status: String = sqlx::query_scalar(
        "SELECT status::text FROM worker_artifact_staging WHERE object_key = $1",
    )
    .bind(&object_key)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(status, "granted", "selection is not an external-delete acknowledgement");
    assert!(
        store
            .mark_staged_artifact_reaped(&object_key)
            .await
            .unwrap()
    );
    assert!(
        store
            .staged_artifacts_due_for_reap(Utc::now() - Duration::hours(24))
            .await
            .unwrap()
            .is_empty()
    );
}
