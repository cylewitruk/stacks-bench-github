use std::collections::BTreeSet;

use chrono::{Duration, Utc};
use sbgh_core::db::JobStore;
use sbgh_core::db::fleet::{
    ArtifactGrantRecord, EventIngest, FleetCompletion, FleetStore, FleetTerminalSubmission,
    FleetTerminalWrite, PreparedExecution, ProjectedReportMutation, ResolvedSpecSource,
    TerminalAcceptance, WorkerRegistration,
};
use sbgh_core::models::{
    BuildTarget, GitRefKind, GithubAccountType, JobAxes, JobIntent, JobResult, JobSource, NewJob,
    QueuedEventDetail, TaskKind,
};
use sbgh_postgres::db::{
    InstallationStore, NewInstallation, Pool, PostgresInstallationStore, setup_pg_db,
};
use sbgh_postgres::{PostgresFleetStore, PostgresJobStore, PreparedJobProvenance};
use sbgh_proto::{
    ArtifactDescriptor, AttemptIdentity, BlockValidationPayload, BlockValidationResult,
    InclusiveRange, PROTOCOL_VERSION, ProgressRequest, ProgressUpdate, RegisterSessionRequest,
    ReliableEventEnvelope, ReliableEventPayload, ResourceFacts, TaskPayload, TerminalOutcome,
    ValidationEpoch, WorkerCapability,
};
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

fn session(worker_id: Uuid, worker_session_id: Uuid) -> RegisterSessionRequest {
    RegisterSessionRequest {
        protocol_version: PROTOCOL_VERSION,
        worker_id,
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
    let payload_hash = sbgh_proto::payload_digest(&payload).unwrap();
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
                worker_id: None,
            },
            &PreparedJobProvenance::default(),
        )
        .await
        .unwrap();
}

async fn enqueue_benchmark(store: &PostgresFleetStore, job_id: Uuid) {
    let payload = TaskPayload::Benchmark(sbgh_proto::BenchmarkPayload {
        effective_args: vec!["--mine-microblocks".into()],
        workload_key: Some("workload-v1".into()),
        sqlite_seed_key: None,
        shared_baseline_calibration: false,
        baseline_calibration_id: None,
        run_index: 0,
        requested_run_count: 1,
    });
    let payload_hash = sbgh_proto::payload_digest(&payload).unwrap();
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
                worker_id: None,
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
        protocol_version: PROTOCOL_VERSION,
        identity: identity.clone(),
        trace_id,
        reliable_seq,
        payload_digest: sbgh_proto::payload_digest(&payload).unwrap(),
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

    let changed = TaskPayload::Benchmark(sbgh_proto::BenchmarkPayload {
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
                payload_hash: sbgh_proto::payload_digest(&changed).unwrap(),
                payload: changed,
                worker_id: None,
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
        TaskPayload::Benchmark(sbgh_proto::BenchmarkPayload {
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
        spec_id: job.benchmark_spec_id,
        commit: "1111111111111111111111111111111111111111".into(),
    };
    assert!(
        store
            .freeze_group_sources(job.benchmark_group_id, std::slice::from_ref(&frozen))
            .await
            .unwrap()
    );
    assert!(
        store
            .freeze_group_sources(job.benchmark_group_id, std::slice::from_ref(&frozen))
            .await
            .unwrap(),
        "a lost response must make source freezing idempotent"
    );
    assert!(
        store
            .freeze_group_sources(
                job.benchmark_group_id,
                &[ResolvedSpecSource {
                    spec_id: job.benchmark_spec_id,
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
          FROM benchmark_spec spec
          JOIN job ON job.benchmark_spec_id = spec.id
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
        .upsert_worker(&registration(worker_id))
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
        .upsert_worker(&registration(worker_id))
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
async fn registry_removal_or_capability_reduction_revokes_live_session() {
    let (_db, pool) = setup_pg_db().await;
    let store = PostgresFleetStore::new(pool);
    let worker_id = Uuid::new_v4();
    let session_id = Uuid::new_v4();
    store
        .upsert_worker(&registration(worker_id))
        .await
        .unwrap();
    store
        .register_session(worker_id, &session(worker_id, session_id), Duration::minutes(5))
        .await
        .unwrap();

    store
        .upsert_worker(&WorkerRegistration {
            worker_id,
            display_name: "reduced".into(),
            allowed_capabilities: vec![WorkerCapability::Benchmark],
            measurement_profile: Some("profile-v1".into()),
            enabled: true,
            draining: false,
        })
        .await
        .unwrap();
    assert!(
        !store
            .session_is_active(worker_id, session_id)
            .await
            .unwrap(),
        "a live session cannot retain a removed capability"
    );

    assert_eq!(
        store
            .disable_workers_except(&[Uuid::new_v4()])
            .await
            .unwrap(),
        1
    );
    assert!(
        !store
            .session_is_active(worker_id, session_id)
            .await
            .unwrap(),
        "an identity removed from declarative policy is disabled"
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
        .upsert_worker(&registration(worker_id))
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
        sbgh_proto::DesiredState::Cancel
    );

    let terminal = TerminalOutcome::Cancelled { reason: "operator test".into() };
    let terminal_payload = ReliableEventPayload::Terminal {
        outcome_digest: sbgh_proto::payload_digest(&terminal).unwrap(),
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
        protocol_version: PROTOCOL_VERSION,
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
        .upsert_worker(&registration(worker_id))
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
        .upsert_worker(&registration(worker_id))
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
        .upsert_worker(&registration(worker_id))
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
            outcome_digest: sbgh_proto::payload_digest(&terminal).unwrap(),
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
        .upsert_worker(&registration(build_worker))
        .await
        .unwrap();
    store
        .upsert_worker(&WorkerRegistration {
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
                worker_id: block_worker,
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
        epoch: ValidationEpoch::Nakamoto,
        range: InclusiveRange { start: 100, end: 199 },
        requested_shards: 8,
        max_concurrency: 4,
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
                range_start: 100,
                range_end: 199,
                requested_shards: 8,
                max_concurrency: 4,
            })
            .unwrap(),
            &PreparedExecution {
                job_id,
                commit: "1111111111111111111111111111111111111111".into(),
                payload: payload.clone(),
                payload_hash: sbgh_proto::payload_digest(&payload).unwrap(),
                worker_id: Some(block_worker),
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
    assert_eq!(offered.offer.requirements, sbgh_proto::OfferRequirements::from(&payload));
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
        observed_range: InclusiveRange { start: 0, end: 1_000 },
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
            outcome_digest: sbgh_proto::payload_digest(&terminal).unwrap(),
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
    let persisted: (String, i64, i64) = sqlx::query_as(
        "SELECT chainstate_origin, observed_start, observed_end
           FROM block_validation_result
          WHERE job_id = $1",
    )
    .bind(job_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(persisted, ("vg0/mainnet-2026-07-28".into(), 0, 1_000));
}

#[tokio::test]
async fn explicit_group_recovery_can_target_a_compatible_worker() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresFleetStore::new(pool.clone());
    let job_id = Uuid::new_v4();
    enqueue_benchmark(&store, job_id).await;
    let group_id: Uuid = sqlx::query_scalar("SELECT benchmark_group_id FROM job WHERE id = $1")
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    let incompatible_worker = Uuid::new_v4();
    store
        .upsert_worker(&registration(incompatible_worker))
        .await
        .unwrap();
    assert!(
        store
            .recover_group(group_id, Some(incompatible_worker), "operator-approved host recovery",)
            .await
            .is_err(),
        "an explicit recovery target must be authorized for benchmark work"
    );
    let target_worker = Uuid::new_v4();
    store
        .upsert_worker(&WorkerRegistration {
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
        .recover_group(group_id, Some(target_worker), "operator-approved host recovery")
        .await
        .unwrap();
    assert_eq!(recovery.prior_group_id, group_id);
    assert_eq!(recovery.execution_generation, 2);
    let row: (i64, Option<Uuid>, Uuid) = sqlx::query_as(
        "SELECT execution_generation, worker_id, recovery_of_group_id
           FROM benchmark_group WHERE id = $1",
    )
    .bind(recovery.new_group_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.0, 2);
    assert_eq!(row.1, Some(target_worker));
    assert_eq!(row.2, group_id);
    let copied_hash: String =
        sqlx::query_scalar("SELECT execution_payload_hash FROM job WHERE id = $1")
            .bind(recovery.first_job_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        copied_hash,
        sbgh_proto::payload_digest(&TaskPayload::Benchmark(sbgh_proto::BenchmarkPayload {
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
        .upsert_worker(&registration(worker_id))
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
            outcome_digest: sbgh_proto::payload_digest(&terminal).unwrap(),
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
        .upsert_worker(&registration(worker_id))
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
