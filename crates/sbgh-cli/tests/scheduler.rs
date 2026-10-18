//! Exercise scheduler controls through the real CLI and an isolated HTTP server.

use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{Value, json};
use tokio::process::Command;

/// Construct one snapshot, including zero-byte artifact debt.
fn snapshot(paused: bool, generation: i64, artifacts: i64) -> Value {
    json!({"policy":{"paused":paused,"generation":generation,"actor":"api:admin","reason":"test","changed_at":"2026-10-16T00:00:00Z"},
        "active_attempts":0,"pending_cleanup":0,"pending_projection":0,"pending_artifacts":artifacts})
}

#[tokio::test]
async fn fleet_abort_cli_requires_scope_and_keeps_receipt_json_clean() {
    for args in [vec!["abort"], vec!["abort", "--generation", "1", "--reason", "test"]] {
        let result = run_fleet(&args, Router::new()).await;
        assert!(!result.status.success());
        assert!(String::from_utf8_lossy(&result.stderr).contains("--scope"));
    }
    let request_id = "00000000-0000-0000-0000-000000000042";
    let receipt = json!({"request_id":request_id,"scope":"all_pending","scheduler_generation":3,
        "actor":"api:admin","reason":"operator test","recorded_at":"2026-10-17T00:00:00Z",
        "selected_job_ids":["job-1"],"accepted_job_ids":["job-1"]});
    let expected = receipt.clone();
    let router = Router::new().route("/api/fleet/abort", post(move |Json(request): Json<Value>| async move {
        assert_eq!(request, json!({"request_id":request_id,"scope":"all_pending","scheduler_generation":3,"reason":"operator test"}));
        Json(receipt)
    }));
    let result = run_fleet(
        &[
            "abort",
            "--scope",
            "all-pending",
            "--generation",
            "3",
            "--reason",
            "operator test",
            "--request-id",
            request_id,
            "--json",
        ],
        router,
    )
    .await;
    assert!(result.status.success(), "{}", String::from_utf8_lossy(&result.stderr));
    assert_eq!(serde_json::from_slice::<Value>(&result.stdout).unwrap(), expected);
    assert!(String::from_utf8_lossy(&result.stderr).contains(request_id));
}

#[tokio::test]
async fn fleet_abort_cli_does_not_confuse_request_acceptance_with_cleanup_completion() {
    let router = Router::new().route(
        "/api/fleet/abort",
        post(|Json(request): Json<Value>| async move {
            Json(
                json!({"request_id":request["request_id"],"scope":"active","scheduler_generation":1,
            "actor":"api:admin","reason":"test","recorded_at":"2026-10-17T00:00:00Z",
            "selected_job_ids":["job-1"],"accepted_job_ids":["job-1"]}),
            )
        }),
    );
    let result =
        run_fleet(&["abort", "--scope", "active", "--generation", "1", "--reason", "test"], router)
            .await;
    assert!(result.status.success());
    assert!(
        String::from_utf8_lossy(&result.stdout)
            .contains("selected=1 accepted=1 cleanup=not_confirmed")
    );
}

/// Run one scheduler operation through the real executable.
async fn run(args: &[&str], router: Router) -> std::process::Output {
    let mut fleet_args = vec!["scheduler"];
    fleet_args.extend_from_slice(args);
    run_fleet(&fleet_args, router).await
}

/// Run a fleet operation and always stop the local server afterwards.
async fn run_fleet(args: &[&str], router: Router) -> std::process::Output {
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
        .env("RUST_LOG", "debug")
        .args(["--api-url", &format!("http://{address}"), "--cookie"])
        .arg(cookie)
        .arg("fleet")
        .args(args)
        .kill_on_drop(true)
        .output()
        .await
        .unwrap();
    server.abort();
    let _ = server.await;
    output
}

#[tokio::test]
async fn scheduler_status_preserves_first_line_used_by_host_update_scripts() {
    for scheduler in [None, Some(snapshot(false, 0, 0)["policy"].clone())] {
        let mut overview = json!({
            "summary": {
                "registered_workers": 1, "online_workers": 1, "draining_workers": 0,
                "active_attempts": 0, "pending_cleanup": 0,
                "reliable_event_gap_attempts": 0, "staged_artifact_bytes": 0
            },
            "workers": []
        });
        if let Some(policy) = scheduler {
            overview["scheduler"] = policy;
        }
        let router = Router::new().route("/api/fleet", get(move || async move { Json(overview) }));
        let output = run_fleet(&["status"], router).await;
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert_eq!(
            stdout.lines().next(),
            Some("workers=1 online=1 draining=0 attempts=0 cleanup=0 gaps=0 staged_bytes=0")
        );
        assert!(
            stdout
                .lines()
                .any(|line| line.starts_with("scheduler="))
        );
    }
}

/// Show selected semantics while remaining compatible with older API responses.
#[tokio::test]
async fn fleet_status_reports_selected_protocol_without_guessing_legacy_daemon_state() {
    for (protocol, expected) in [
        (None, "protocol=unknown features=unknown"),
        (
            Some(json!({"offer":null,"revision":1,"features":["build_only_v1"]})),
            "protocol=1 features=build_only_v1",
        ),
        (
            Some(
                json!({"offer":{"minimum_revision":1,"maximum_revision":2,"features":["build_only_v1","future"]},"revision":2,"features":["build_only_v1"]}),
            ),
            "protocol=2 features=build_only_v1",
        ),
    ] {
        let mut worker = json!({
            "worker_id":"worker", "display_name":"test", "enabled":true,
            "draining":false, "capabilities":["build_only"], "session_status":"idle"
        });
        if let Some(protocol) = protocol {
            worker["protocol"] = protocol;
        }
        let overview = json!({
            "summary": {
                "registered_workers":1,"online_workers":1,"draining_workers":0,
                "active_attempts":0,"pending_cleanup":0,
                "reliable_event_gap_attempts":0,"staged_artifact_bytes":0
            },
            "workers":[worker]
        });
        let router = Router::new().route("/api/fleet", get(move || async move { Json(overview) }));
        let output = run_fleet(&["status"], router).await;
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert!(stdout.contains(expected), "{stdout}");
        assert!(!stdout.contains("future"), "unselected features must not appear as enabled");
    }
}

/// Keep machine output separate from the retry identity and audit request.
#[tokio::test]
async fn lifecycle_cli_transmits_only_explicit_action_and_keeps_json_clean() {
    let worker = "00000000-0000-0000-0000-000000000021";
    let request_id = "00000000-0000-0000-0000-000000000042";
    for (command, action) in [
        ("cordon", "cordon"),
        ("uncordon", "uncordon"),
        ("shutdown-after-idle", "shutdown_after_idle"),
    ] {
        let receipt = json!({"request_id":request_id,"worker_id":worker,"action":action,
            "generation":1,"target_session_id":null,"actor":"api:admin","reason":"maintenance",
            "recorded_at":"2026-10-17T00:00:00Z"});
        let expected = receipt.clone();
        let router = Router::new().route(
            &format!("/api/fleet/workers/{worker}/lifecycle"),
            post(move |Json(request): Json<Value>| async move {
                assert_eq!(
                    request,
                    json!({"request_id":request_id,"action":action,"reason":"maintenance"})
                );
                Json(receipt)
            }),
        );
        let result = run_fleet(
            &[
                command,
                "--worker-id",
                worker,
                "--request-id",
                request_id,
                "--reason",
                "maintenance",
                "--json",
            ],
            router,
        )
        .await;
        assert!(result.status.success(), "{}", String::from_utf8_lossy(&result.stderr));
        assert_eq!(serde_json::from_slice::<Value>(&result.stdout).unwrap(), expected);
        assert!(String::from_utf8_lossy(&result.stderr).contains(request_id));
    }
}

/// Coherent quiescent state; overrides model intermediate maintenance phases.
fn maintenance() -> Value {
    json!({"worker_id":"00000000-0000-0000-0000-000000000021",
        "enabled":true,"admission":null,"preflight":null,"generation":1,"cordoned":true,"session_id":"00000000-0000-0000-0000-000000000022",
        "session_status":"draining","session_live":true,"cordon_semantic":"resident",
        "protocol_revision":2,"protocol_features":["resident_cordon","shutdown_after_idle"],
        "shutdown_session_id":null,"active_attempts":0,"pending_cleanup":0})
}

/// Admission wait distinguishes a live worker from policy readiness.
#[tokio::test]
async fn lifecycle_wait_schedulable_requires_known_unblocked_admission() {
    let mut ready = maintenance();
    ready["cordoned"] = false.into();
    ready["admission"] = json!({"schedulable":true,"blockers":[],"effective_capabilities":["build_only"],"measurement_profile":null});
    let mut paused = ready.clone();
    paused["admission"]["schedulable"] = false.into();
    paused["admission"]["blockers"] = json!(["scheduler_paused"]);
    let result = wait_worker("schedulable", vec![paused, ready.clone()]).await;
    assert!(result.status.success(), "{}", String::from_utf8_lossy(&result.stderr));
    assert_eq!(serde_json::from_slice::<Value>(&result.stdout).unwrap(), ready);
    assert!(String::from_utf8_lossy(&result.stderr).contains("scheduler_paused"));
    for (snapshot, message) in [
        (maintenance(), "worker is cordoned"),
        (
            {
                let mut unknown = ready.clone();
                unknown
                    .as_object_mut()
                    .unwrap()
                    .remove("admission");
                unknown
            },
            "does not report admission blockers",
        ),
    ] {
        let result = wait_worker("schedulable", vec![snapshot]).await;
        assert!(!result.status.success());
        assert!(String::from_utf8_lossy(&result.stderr).contains(message));
        assert!(result.stdout.is_empty());
    }
}

/// Read-only worker endpoint returning successive observations.
fn maintenance_router(values: Vec<Value>) -> Router {
    Router::new()
        .route("/api/fleet/workers/00000000-0000-0000-0000-000000000021/lifecycle", get(status))
        .with_state(Arc::new(Mutex::new(values)))
}

/// Invoke maintenance waiting with a short test deadline.
async fn wait_worker(until: &str, values: Vec<Value>) -> std::process::Output {
    run_fleet(
        &[
            "wait-worker",
            "--worker-id",
            "00000000-0000-0000-0000-000000000021",
            "--until",
            until,
            "--timeout-seconds",
            "3",
            "--json",
        ],
        maintenance_router(values),
    )
    .await
}

#[tokio::test]
async fn lifecycle_wait_is_cleanup_gated_and_json_clean() {
    let ready = maintenance();
    let mut busy = ready.clone();
    busy["pending_cleanup"] = 1.into();
    let result = wait_worker("quiescent", vec![busy, ready.clone()]).await;
    assert!(result.status.success(), "{}", String::from_utf8_lossy(&result.stderr));
    assert_eq!(serde_json::from_slice::<Value>(&result.stdout).unwrap(), ready);
    assert!(String::from_utf8_lossy(&result.stderr).contains("cleanup=1"));
}

#[tokio::test]
async fn lifecycle_worker_status_renders_selected_semantics_and_wait_pins_explicit_receipt() {
    let snapshot = maintenance();
    let result = run_fleet(
        &["worker-status", "--worker-id", "00000000-0000-0000-0000-000000000021"],
        maintenance_router(vec![snapshot.clone()]),
    )
    .await;
    assert!(result.status.success());
    let stdout = String::from_utf8_lossy(&result.stdout);
    assert!(stdout.contains("preflight=unreported"));
    assert!(stdout.contains(
        "cordon_semantic=resident protocol=2 features=resident_cordon,shutdown_after_idle"
    ));
    for extra in [["--generation", "2"], ["--session-id", "00000000-0000-0000-0000-000000000023"]] {
        let result = run_fleet(
            &[
                "wait-worker",
                "--worker-id",
                "00000000-0000-0000-0000-000000000021",
                "--json",
                extra[0],
                extra[1],
            ],
            maintenance_router(vec![snapshot.clone()]),
        )
        .await;
        assert!(!result.status.success(), "wrong receipt must not pass on a quiescent snapshot");
        assert!(result.stdout.is_empty());
    }
}

#[tokio::test]
async fn lifecycle_status_exposes_reported_preflight_without_treating_it_as_admission() {
    let mut snapshot = maintenance();
    snapshot["preflight"] = json!({
        "revision":1,"completed_at_ms":1792213533000_i64,
        "executable_sha256":"ab".repeat(32),"configuration_sha256":"cd".repeat(32)
    });
    let human = run_fleet(
        &["worker-status", "--worker-id", "00000000-0000-0000-0000-000000000021"],
        maintenance_router(vec![snapshot.clone()]),
    )
    .await;
    assert!(human.status.success());
    let stdout = String::from_utf8_lossy(&human.stdout);
    assert!(stdout.contains("preflight=reported(revision=1,completed_at_ms=1792213533000"));
    assert!(stdout.contains("cordoned=true"));
    let structured = run_fleet(
        &["worker-status", "--worker-id", "00000000-0000-0000-0000-000000000021", "--json"],
        maintenance_router(vec![snapshot.clone()]),
    )
    .await;
    assert!(structured.status.success());
    assert_eq!(serde_json::from_slice::<Value>(&structured.stdout).unwrap(), snapshot);
}

#[tokio::test]
async fn lifecycle_wait_rejects_policy_and_session_changes_and_does_not_call_expiry_exit() {
    for (patch, expected) in [
        (json!({"cordoned":false}), "not cordoned"),
        (json!({"generation":2}), "generation changed"),
        (json!({"session_id":"00000000-0000-0000-0000-000000000023"}), "session changed"),
        (json!({"session_status":"offline","session_live":false}), "timed out"),
    ] {
        let initial = maintenance();
        let mut changed = initial.clone();
        changed
            .as_object_mut()
            .unwrap()
            .extend(
                patch
                    .as_object()
                    .unwrap()
                    .clone(),
            );
        let result = wait_worker("deregistered", vec![initial, changed]).await;
        assert!(!result.status.success());
        assert!(
            String::from_utf8_lossy(&result.stderr).contains(expected),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(result.stdout.is_empty());
    }
}

#[tokio::test]
async fn lifecycle_wait_deregistered_requires_acknowledgement_and_zero_debt() {
    let mut waiting = maintenance();
    waiting["session_status"] = "deregistered".into();
    waiting["session_live"] = false.into();
    waiting["active_attempts"] = 1.into();
    let mut ready = waiting.clone();
    ready["active_attempts"] = 0.into();
    let result = wait_worker("deregistered", vec![waiting, ready.clone()]).await;
    assert!(result.status.success(), "{}", String::from_utf8_lossy(&result.stderr));
    assert_eq!(serde_json::from_slice::<Value>(&result.stdout).unwrap(), ready);
}

#[tokio::test]
async fn lifecycle_wait_deadline_includes_stalled_api_request() {
    let router = Router::new().route(
        "/api/fleet/workers/00000000-0000-0000-0000-000000000021/lifecycle",
        get(|| async {
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            Json(maintenance())
        }),
    );
    let started = std::time::Instant::now();
    let result = run_fleet(
        &[
            "wait-worker",
            "--worker-id",
            "00000000-0000-0000-0000-000000000021",
            "--timeout-seconds",
            "1",
            "--json",
        ],
        router,
    )
    .await;
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("timed out"));
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
    assert!(result.stdout.is_empty());
}

/// Keep machine output separate from the retry identity and audit request.
#[tokio::test]
async fn protocol_floor_cli_keeps_receipt_json_clean_and_transmits_audit_facts() {
    let request_id = "00000000-0000-0000-0000-000000000042";
    let receipt = json!({"minimum_revision":2,"generation":1,"actor":"api:admin",
        "reason":"upgrade","rollback_evidence":"qualification/test","changed_at":"2026-10-17T00:00:00Z"});
    let expected = receipt.clone();
    let router = Router::new().route("/api/fleet/protocol", post(move |Json(request): Json<Value>| async move {
        assert_eq!(request, json!({"request_id":request_id,"minimum_revision":2,"reason":"upgrade","rollback_evidence":"qualification/test"}));
        Json(receipt)
    }));
    let output = run_fleet(
        &[
            "protocol",
            "set-floor",
            "--minimum-revision",
            "2",
            "--reason",
            "upgrade",
            "--rollback-evidence",
            "qualification/test",
            "--request-id",
            request_id,
            "--json",
        ],
        router,
    )
    .await;
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert_eq!(serde_json::from_slice::<Value>(&output.stdout).unwrap(), expected);
    assert!(String::from_utf8_lossy(&output.stderr).contains(request_id));
}

/// Return the next snapshot, retaining the final one for subsequent polls.
async fn status(State(state): State<Arc<Mutex<Vec<Value>>>>) -> Json<Value> {
    let mut values = state.lock().unwrap();
    Json(if values.len() > 1 { values.remove(0) } else { values[0].clone() })
}

/// Configure a read-only polling endpoint.
fn router(values: Vec<Value>) -> Router {
    Router::new()
        .route("/api/fleet/scheduler", get(status))
        .with_state(Arc::new(Mutex::new(values)))
}

#[tokio::test]
async fn scheduler_wait_pins_generation_and_emits_clean_json() {
    let output = run(
        &["wait", "--timeout-seconds", "5", "--json"],
        router(vec![snapshot(true, 1, 1), snapshot(true, 1, 0)]),
    )
    .await;
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["pending_artifacts"], 0);
    assert!(String::from_utf8_lossy(&output.stderr).contains("artifacts=1"));
}

/// Retained garbage remains observable in both formats without delaying maintenance.
#[tokio::test]
async fn scheduler_wait_reports_garbage_without_blocking() {
    let mut ready = snapshot(true, 1, 0);
    ready["garbage_artifacts"] = json!(3);
    for output_args in
        [vec!["wait", "--timeout-seconds", "1", "--json"], vec!["wait", "--timeout-seconds", "1"]]
    {
        let output = run(&output_args, router(vec![ready.clone()])).await;
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
        if output_args.contains(&"--json") {
            let value: Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(value["garbage_artifacts"], 3);
            assert_eq!(value["pending_artifacts"], 0);
        } else {
            let text = String::from_utf8_lossy(&output.stdout);
            assert!(text.contains("quiescent=true"));
            assert!(text.contains("garbage_artifacts=3"));
        }
    }
}

#[tokio::test]
async fn scheduler_wait_rejects_resume_generation_change_and_timeout() {
    for (values, message) in [
        (vec![snapshot(false, 0, 0)], "scheduler is running"),
        (vec![snapshot(true, 1, 1), snapshot(true, 3, 0)], "generation changed"),
        (vec![snapshot(true, 1, 1)], "timed out"),
    ] {
        let output = run(&["wait", "--timeout-seconds", "2", "--json"], router(values)).await;
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains(message));
        assert!(output.stdout.is_empty());
    }
}

#[tokio::test]
async fn scheduler_pause_preserves_request_identity_and_json_output() {
    let requests = Arc::new(Mutex::new(Vec::<Value>::new()));
    let capture = requests.clone();
    let router = Router::new().route(
        "/api/fleet/scheduler/pause",
        post(move |Json(body): Json<Value>| {
            let capture = capture.clone();
            async move {
                capture
                    .lock()
                    .unwrap()
                    .push(body);
                Json(snapshot(true, 1, 0)["policy"].clone())
            }
        }),
    );
    let id = "37bebd8e-6daa-414d-99b9-d662a572639c";
    let output = run(&["pause", "--request-id", id, "--reason", "test", "--json"], router).await;
    assert!(output.status.success());
    assert_eq!(requests.lock().unwrap()[0], json!({"request_id":id,"reason":"test"}));
    assert!(String::from_utf8_lossy(&output.stderr).contains(id));
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["paused"], true);
}
