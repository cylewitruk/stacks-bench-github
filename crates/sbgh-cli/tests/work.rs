//! Exercise observation and downloads through the actual CLI and typed API client.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{Value, json};
use tokio::process::Command;

const JOB: &str = "88ed079a-b617-49a0-902d-217f23284ee6";
const SUBMISSION: &str = "11ed079a-b617-49a0-902d-217f23284ee6";

/// Observation server that records selectors and advances the selected job.
#[derive(Default)]
struct Fixture {
    /// IDs received by the API.
    selectors: Vec<String>,
    /// Final selected-job outcome.
    outcome: String,
}

/// Task snapshot with an unrelated failed sibling to detect scope mistakes.
fn snapshot(selected_status: &str) -> Value {
    let job = json!({"id":JOB,"spec_index":0,"run_index":0,"status":selected_status,
        "task_kind":"benchmark","reference":"branch","commit":"a".repeat(40),
        "created_at":"2026-10-02T00:00:00Z","effective_args":[],"chainstate_origin":"vg/base",
        "phase":"replay","failure":null,"attempt":null,"progress":null,"metrics":null,"result":null});
    let mut sibling = job.clone();
    sibling["id"] = json!("99ed079a-b617-49a0-902d-217f23284ee6");
    sibling["run_index"] = json!(1);
    sibling["status"] = json!("failed");
    json!({"target":{"submission_id":SUBMISSION,"job_id":JOB},
        "report":{
            "identity":{"submission_id":SUBMISSION,"current_job_id":JOB,"current_attempt_id":null,
                "task_kind":"benchmark","source":"cli","repository":"o/r","commit":"a".repeat(40)},
            "lifecycle":{"state":"failed","phase":"replay","completed_jobs":0,"total_jobs":2,"failure":"sibling failed"},
            "task":{"kind":"benchmark","detail":{"requested_runs":2,"completed_runs":0}},
            "artifacts":[],"forensics":null},
        "jobs":[job,sibling]})
}

/// Respond with a running snapshot, then the configured terminal outcome.
async fn inspect(
    State(state): State<Arc<Mutex<Fixture>>>,
    Query(query): Query<HashMap<String, String>>,
) -> Json<Value> {
    let mut fixture = state.lock().unwrap();
    fixture
        .selectors
        .push(query["id"].clone());
    let status = if fixture.selectors.len() == 1 { "running" } else { &fixture.outcome };
    Json(snapshot(status))
}

/// Run a CLI process against an isolated local observation API.
async fn run(args: &[&str], router: Router) -> (std::process::Output, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .await
            .unwrap();
    });
    let root = tempfile::tempdir().unwrap();
    let cookie = root.path().join("cookie");
    std::fs::write(&cookie, "test-cookie").unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_sbgh-cli"))
        .args(["--api-url", &format!("http://{address}"), "--cookie"])
        .arg(&cookie)
        .args(args)
        .output()
        .await
        .unwrap();
    (output, server)
}

#[tokio::test]
async fn watch_json_pins_resolution_and_finishes_on_selected_job() {
    let fixture = Arc::new(Mutex::new(Fixture {
        outcome: "completed".into(),
        ..Fixture::default()
    }));
    let router = Router::new()
        .route("/api/work", get(inspect))
        .with_state(fixture.clone());
    let (output, server) = run(&["watch", "88ed079a", "--interval", "1", "--json"], router).await;
    server.abort();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let stdout = String::from_utf8(output.stdout).unwrap();
    let records = stdout
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(records.len(), 2);
    assert_eq!(records[1]["jobs"][0]["status"], "completed");
    assert_eq!(
        fixture
            .lock()
            .unwrap()
            .selectors,
        ["88ed079a".to_owned(), format!("job:{JOB}")]
    );
}

#[tokio::test]
async fn watch_failed_job_emits_terminal_json_and_exits_unsuccessfully() {
    let fixture = Arc::new(Mutex::new(Fixture {
        outcome: "failed".into(),
        ..Fixture::default()
    }));
    let router = Router::new()
        .route("/api/work", get(inspect))
        .with_state(fixture);
    let (output, server) = run(&["watch", JOB, "--interval", "1", "--json"], router).await;
    server.abort();
    assert!(!output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let terminal: Value = serde_json::from_str(stdout.lines().last().unwrap()).unwrap();
    assert_eq!(terminal["jobs"][0]["status"], "failed");
    assert!(String::from_utf8_lossy(&output.stderr).contains("watched work failed"));
}

#[tokio::test]
async fn report_accepts_printed_job_ids_and_preserves_legacy_json_contract() {
    for arguments in
        [vec!["jobs", "report", JOB], vec!["jobs", "report", "--submission-id", SUBMISSION]]
    {
        let fixture = Arc::new(Mutex::new(Fixture {
            outcome: "completed".into(),
            ..Fixture::default()
        }));
        let router = Router::new()
            .route("/api/work", get(inspect))
            .route(
                "/api/submissions/{id}/report",
                get(|| async { Json(snapshot("completed")["report"].clone()) }),
            )
            .with_state(fixture.clone());
        let (output, server) = run(&arguments, router).await;
        server.abort();
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
        let report: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(report["identity"]["submission_id"], SUBMISSION);
        let expected =
            if arguments.len() == 3 { JOB.to_owned() } else { format!("submission:{SUBMISSION}") };
        assert_eq!(
            fixture
                .lock()
                .unwrap()
                .selectors,
            [expected]
        );
    }
}

/// Serve a recorded artifact, or fail after resolving its job.
fn artifact_router(fail: bool) -> Router {
    Router::new()
        .route("/api/work", get(|| async { Json(snapshot("completed")) }))
        .route(
            "/api/jobs/{id}/artifacts/content",
            get(move || async move {
                if fail {
                    (StatusCode::SERVICE_UNAVAILABLE, "unavailable")
                } else {
                    (StatusCode::OK, "artifact bytes")
                }
            }),
        )
}

#[tokio::test]
async fn artifact_get_streams_to_file_and_refuses_overwrite() {
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("result.db");
    let path = destination.to_str().unwrap();
    let args = ["artifacts", "get", JOB, "appdata/stacks-bench.db", "--output", path, "--json"];
    let (output, server) = run(&args, artifact_router(false)).await;
    server.abort();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert_eq!(std::fs::read(&destination).unwrap(), b"artifact bytes");
    let receipt: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(receipt["bytes"], 14);
    assert_eq!(receipt["job_id"], JOB);
    let (output, server) = run(&args, artifact_router(false)).await;
    server.abort();
    assert!(!output.status.success());
    assert_eq!(std::fs::read(&destination).unwrap(), b"artifact bytes");
    assert_eq!(
        std::fs::read_dir(root.path())
            .unwrap()
            .count(),
        1
    );
}

#[tokio::test]
async fn failed_artifact_get_removes_temporary_file_and_does_not_publish_destination() {
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("result.db");
    let (output, server) = run(
        &[
            "artifacts",
            "get",
            JOB,
            "appdata/stacks-bench.db",
            "--output",
            destination.to_str().unwrap(),
        ],
        artifact_router(true),
    )
    .await;
    server.abort();
    assert!(!output.status.success());
    assert!(!destination.exists());
    assert_eq!(
        std::fs::read_dir(root.path())
            .unwrap()
            .count(),
        0
    );
}
