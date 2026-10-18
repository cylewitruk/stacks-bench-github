//! Coordinator reconstruction around durable terminal projection boundaries.

use std::assert_matches;
use std::sync::Arc;

use sbgh_core::db::fleet::{
    FleetCompletion, FleetFailure, FleetStore, FleetTerminalSubmission, FleetTerminalWrite,
    TerminalAcceptance, WorkerRegistration,
};
use sbgh_core::models::JobResult;
use sbgh_fleet::{
    BenchmarkPayload, ReliableEventEnvelope, ReliableEventPayload, TaskPayload, TerminalOutcome,
    WorkerCapability, payload_digest,
};
use sbgh_github::api::{CheckRunConclusion, CheckRunState};
use sbgh_github::test_support::{FakeCall, FakeGitHub};
use sbgh_postgres::PostgresFleetStore;
use sbgh_postgres::db::setup_pg_db;
use tempfile::TempDir;
use uuid::Uuid;

use super::{seed_active_attempt, test_coordinator as coordinator};

#[tokio::test]
async fn terminal_projection_after_restart_reuses_durable_provider_identity() {
    terminal_recovery(false, CheckRunConclusion::Cancelled).await;
}

#[tokio::test]
async fn terminal_projection_ack_loss_replays_same_provider_identity() {
    terminal_recovery(true, CheckRunConclusion::Cancelled).await;
}

#[tokio::test]
async fn successful_terminal_projection_after_restart_reuses_provider_identity() {
    terminal_recovery(false, CheckRunConclusion::Success).await;
}

#[tokio::test]
async fn successful_terminal_projection_ack_loss_replays_same_provider_identity() {
    terminal_recovery(true, CheckRunConclusion::Success).await;
}

#[tokio::test]
async fn failed_terminal_projection_after_restart_reuses_provider_identity() {
    terminal_recovery(false, CheckRunConclusion::Failure).await;
}

#[tokio::test]
async fn failed_terminal_projection_ack_loss_replays_same_provider_identity() {
    terminal_recovery(true, CheckRunConclusion::Failure).await;
}

/// Build matching worker and persistence terminal facts without benchmark measurements.
fn terminal_fixture(
    job_id: Uuid,
    conclusion: CheckRunConclusion,
) -> (TerminalOutcome, FleetTerminalWrite, &'static str) {
    match conclusion {
        CheckRunConclusion::Cancelled => (
            TerminalOutcome::Cancelled {
                reason: "qualification cancellation".into(),
            },
            FleetTerminalWrite::Cancelled {
                remark: "qualification cancellation".into(),
            },
            "cancelled",
        ),
        CheckRunConclusion::Failure => (
            TerminalOutcome::Failed {
                error: "qualification failure".into(),
                summary: None,
                retryable: false,
            },
            FleetTerminalWrite::Failed(FleetFailure {
                result: None,
                remark: "qualification failure".into(),
                event_detail: None,
            }),
            "failed",
        ),
        CheckRunConclusion::Success => (
            TerminalOutcome::Completed {
                summary: serde_json::json!({}),
                block_validation: None,
            },
            FleetTerminalWrite::Completed(Box::new(FleetCompletion {
                result: JobResult {
                    job_id,
                    run_json: None,
                    archive_dir: job_id.to_string(),
                    created_at: chrono::Utc::now(),
                },
                metric: None,
                baseline_calibration_id: None,
                event_detail: None,
                block_validation: None,
                artifact_manifest: Vec::new(),
            })),
            "completed",
        ),
        _ => unreachable!("fixture covers worker terminal outcomes"),
    }
}

/// Interrupt either before provider mutation or after mutation but before its durable ack.
async fn terminal_recovery(lose_projection_ack: bool, conclusion: CheckRunConclusion) {
    let (_db, pool) = setup_pg_db().await;
    let directory = TempDir::new().unwrap();
    let fleet = PostgresFleetStore::new(pool.clone());
    let gh = Arc::new(FakeGitHub::new());
    let worker = Uuid::new_v4();
    fleet
        .seed_worker(&WorkerRegistration {
            worker_id: worker,
            display_name: "projection-recovery".into(),
            allowed_capabilities: vec![WorkerCapability::Benchmark],
            measurement_profile: Some("test-profile".into()),
            enabled: true,
            draining: false,
        })
        .await
        .unwrap();
    let offer = seed_active_attempt(
        &pool,
        &fleet,
        worker,
        TaskPayload::Benchmark(BenchmarkPayload {
            effective_args: Vec::new(),
            workload_key: None,
            sqlite_seed_key: None,
            shared_baseline_calibration: false,
            baseline_calibration_id: None,
            run_index: 0,
            requested_run_count: 1,
        }),
    )
    .await;

    let original = coordinator(&pool, &directory, gh.clone());
    let job = original
        .jobs
        .load_runnable(offer.job_id)
        .await
        .unwrap()
        .unwrap();
    original
        .surface(offer.identity.attempt_id, &job)
        .await
        .unwrap();
    let checks: Vec<_> = gh
        .calls()
        .into_iter()
        .filter_map(|call| match call {
            FakeCall::CreateCheckRun { returned_id, external_id, .. } => {
                Some((returned_id, external_id))
            }
            _ => None,
        })
        .collect();
    assert_eq!(checks.len(), 1);
    let (check_id, external_id) = &checks[0];
    assert!(
        external_id.contains(
            &job.task_submission_id
                .to_string()
        )
    );

    let (outcome, write, terminal_status) = terminal_fixture(offer.job_id, conclusion);
    let payload = ReliableEventPayload::Terminal {
        outcome_digest: payload_digest(&outcome).unwrap(),
    };
    let event = ReliableEventEnvelope {
        identity: offer.identity.clone(),
        trace_id: offer.trace_id,
        reliable_seq: 1,
        payload_digest: payload_digest(&payload).unwrap(),
        payload,
        worker_timestamp_ms: chrono::Utc::now().timestamp_millis(),
    };
    fleet
        .ingest_reliable_event(worker, &event)
        .await
        .unwrap();
    let submission = FleetTerminalSubmission {
        reliable_seq: 1,
        payload_digest: &event.payload_digest,
        outcome: &outcome,
        artifacts: &[],
        write: &write,
    };
    assert_matches!(
        fleet
            .accept_terminal(worker, &offer.identity, &submission)
            .await
            .unwrap(),
        TerminalAcceptance::Accepted
    );

    if lose_projection_ack {
        sqlx::raw_sql(
            r#"
            CREATE FUNCTION reject_projection_ack() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF NEW.projected_reliable_seq > OLD.projected_reliable_seq THEN
                    RAISE EXCEPTION 'injected projection acknowledgement loss';
                END IF;
                RETURN NEW;
            END $$;
            CREATE TRIGGER reject_projection_ack BEFORE UPDATE ON worker_attempt
                FOR EACH ROW EXECUTE FUNCTION reject_projection_ack();
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();
        let error = original
            .project_events()
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("injected projection acknowledgement loss"));
        sqlx::query("DROP TRIGGER reject_projection_ack ON worker_attempt")
            .execute(&pool)
            .await
            .unwrap();
    }
    assert_eq!(
        fleet
            .unprojected_events(10)
            .await
            .unwrap()
            .len(),
        1
    );
    drop(original);

    let rebuilt = coordinator(&pool, &directory, gh.clone());
    rebuilt
        .project_events()
        .await
        .unwrap();
    assert!(
        fleet
            .unprojected_events(10)
            .await
            .unwrap()
            .is_empty()
    );
    assert_matches!(
        fleet
            .accept_terminal(worker, &offer.identity, &submission)
            .await
            .unwrap(),
        TerminalAcceptance::Duplicate
    );
    rebuilt
        .project_events()
        .await
        .unwrap();
    drop(rebuilt);
    coordinator(&pool, &directory, gh.clone())
        .project_events()
        .await
        .unwrap();

    let calls = gh.calls();
    assert_eq!(
        calls
            .iter()
            .filter(|call| matches!(call, FakeCall::CreateCheckRun { .. }))
            .count(),
        1
    );
    let terminal_updates: Vec<_> = calls
        .iter()
        .filter_map(|call| match call {
            FakeCall::UpdateCheckRun {
                check_run_id,
                state: CheckRunState::Completed(actual_conclusion),
                output,
                ..
            } => {
                assert_eq!(*actual_conclusion, conclusion);
                Some((*check_run_id, &output.summary))
            }
            _ => None,
        })
        .collect();
    assert_eq!(terminal_updates.len(), if lose_projection_ack { 2 } else { 1 });
    for (id, summary) in terminal_updates {
        assert_eq!(id, *check_id);
        assert!(summary.contains(&job.commit));
    }
    let state: (String, i64, i64, i64) = sqlx::query_as("SELECT status::text, terminal_reliable_seq, projected_reliable_seq, fencing_generation FROM worker_attempt WHERE attempt_id = $1")
        .bind(offer.identity.attempt_id).fetch_one(&pool).await.unwrap();
    assert_eq!(state, (terminal_status.into(), 1, 1, 1));
    let job_status: String = sqlx::query_scalar("SELECT status::text FROM job WHERE id = $1")
        .bind(offer.job_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(job_status, terminal_status);
    let attempt_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM worker_attempt WHERE job_id = $1")
            .bind(offer.job_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(attempt_count, 1, "projection replay must not create another execution");
}
