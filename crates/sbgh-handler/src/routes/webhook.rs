//! GitHub webhook entry point.
//!
//! The handler is deliberately the narrowest surface in the system. It:
//!   1. Reads the raw body (signature is over the bytes, not parsed JSON).
//!   2. Verifies the HMAC-SHA256 signature against the webhook secret.
//!   3. Dispatches on `X-GitHub-Event`.
//!   4. For `issue_comment`: parses the `/benchmark` command, checks the
//!      authorization allowlist, and enqueues a job row.
//!
//! Notably absent: any GitHub API call. The handler holds **no** App
//! credentials. Head-SHA resolution and the initial PR comment are both
//! deferred to the orchestrator, which runs on the host with the App
//! private key. The handler's only DB grant is `INSERT ON jobs`.

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use sbgh_core::config::AuthorizationConfig;
use sbgh_core::github::{IssueCommentEvent, parse_command, verify_signature};
use sbgh_core::models::NewJob;

use crate::state::AppState;

const EVENT_HEADER: &str = "x-github-event";
const SIGNATURE_HEADER: &str = "x-hub-signature-256";
const DELIVERY_HEADER: &str = "x-github-delivery";

pub async fn handle(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let signature = match headers
        .get(SIGNATURE_HEADER)
        .and_then(|v| v.to_str().ok())
    {
        Some(s) => s,
        None => return (StatusCode::UNAUTHORIZED, "missing signature").into_response(),
    };
    if let Err(e) = verify_signature(&state.config.webhook.secret, &body, signature) {
        tracing::warn!(error = %e, "rejecting webhook: bad signature");
        return (StatusCode::UNAUTHORIZED, "invalid signature").into_response();
    }

    let event = headers
        .get(EVENT_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let delivery_id = headers
        .get(DELIVERY_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);

    match event {
        "ping" => (StatusCode::OK, "pong").into_response(),
        "issue_comment" => handle_issue_comment(state, &body, delivery_id).await,
        other => {
            tracing::debug!(event = other, "ignoring event");
            (StatusCode::OK, "ignored").into_response()
        }
    }
}

async fn handle_issue_comment(
    state: AppState,
    body: &[u8],
    delivery_id: Option<String>,
) -> axum::response::Response {
    let event: IssueCommentEvent = match serde_json::from_slice(body) {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!(error = %e, "failed to decode issue_comment payload");
            return (StatusCode::BAD_REQUEST, "bad payload").into_response();
        }
    };

    if event.action != "created" {
        return (StatusCode::OK, "ignored").into_response();
    }
    if event
        .issue
        .pull_request
        .is_none()
    {
        return (StatusCode::OK, "not a PR").into_response();
    }

    let command = match parse_command(&event.comment.body) {
        Ok(Some(c)) => c,
        Ok(None) => return (StatusCode::OK, "no command").into_response(),
        Err(e) => {
            tracing::info!(error = %e, "malformed command");
            return (StatusCode::OK, "malformed command").into_response();
        }
    };

    if let Err(reason) = authorized(&state.config.authorization, &event) {
        tracing::warn!(
            user = %event.sender.login,
            repo = %event.repository.full_name,
            %reason,
            "rejecting unauthorized command"
        );
        return (StatusCode::OK, "unauthorized").into_response();
    }

    // head_sha is left empty: only the orchestrator (which holds the App
    // private key) can hit `GET /repos/{}/pulls/{}` to resolve it. It does
    // so on job pickup and writes back via JobStore::set_head_sha.
    let new = NewJob {
        repository: event
            .repository
            .full_name
            .clone(),
        pr_number: event.issue.number,
        head_sha: String::new(),
        requested_by: event.sender.login.clone(),
        command: command
            .subcommand
            .unwrap_or_else(|| "default".into()),
        args: serde_json::json!({ "args": command.args }),
        installation_id: event.installation.id,
        github_delivery_id: delivery_id.clone(),
    };

    match state.jobs.enqueue(&new).await {
        Ok(Some(_id)) => (StatusCode::OK, "queued").into_response(),
        Ok(None) => {
            tracing::info!(
                delivery = ?delivery_id,
                repo = %event.repository.full_name,
                pr = event.issue.number,
                "ignoring duplicate webhook delivery"
            );
            (StatusCode::OK, "duplicate").into_response()
        }
        Err(e) => {
            tracing::error!(error = ?e, "failed to enqueue job");
            (StatusCode::INTERNAL_SERVER_ERROR, "queue error").into_response()
        }
    }
}

fn authorized(cfg: &AuthorizationConfig, event: &IssueCommentEvent) -> Result<(), &'static str> {
    if !cfg
        .allowed_repositories
        .is_empty()
        && !cfg
            .allowed_repositories
            .contains(&event.repository.full_name)
    {
        return Err("repo not in allowlist");
    }
    if cfg
        .allowed_users
        .contains(&event.sender.login)
    {
        return Ok(());
    }
    if cfg
        .allowed_associations
        .contains(
            &event
                .comment
                .author_association,
        )
    {
        return Ok(());
    }
    Err("user not in allowlist and association not permitted")
}
