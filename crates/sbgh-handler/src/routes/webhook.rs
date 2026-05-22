//! GitHub webhook entry point.
//!
//! Flow:
//!   1. Read raw body (signature is over the bytes, not parsed JSON).
//!   2. Verify HMAC-SHA256 signature against the App webhook secret.
//!   3. Dispatch on `X-GitHub-Event` header.
//!   4. For `issue_comment`: check authorization, parse `/benchmark` command,
//!      resolve PR head SHA, enqueue a job, and reply on the PR with the queue
//!      position.

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
    if let Err(e) = verify_signature(
        &state
            .config
            .github
            .webhook_secret,
        &body,
        signature,
    ) {
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

    let head_sha = match state
        .gh
        .pr_head_sha(event.installation.id, &event.repository.full_name, event.issue.number as u64)
        .await
    {
        Ok(sha) => sha,
        Err(e) => {
            tracing::error!(error = %e, "failed to resolve head sha");
            return (StatusCode::INTERNAL_SERVER_ERROR, "head sha lookup failed").into_response();
        }
    };

    let new = NewJob {
        repository: event
            .repository
            .full_name
            .clone(),
        pr_number: event.issue.number,
        head_sha,
        requested_by: event.sender.login.clone(),
        command: command
            .subcommand
            .unwrap_or_else(|| "default".into()),
        args: serde_json::json!({ "args": command.args }),
        installation_id: event.installation.id,
        github_delivery_id: delivery_id.clone(),
    };

    let (job_id, position) = match state.jobs.enqueue(&new).await {
        Ok(Some(x)) => x,
        Ok(None) => {
            tracing::info!(
                delivery = ?delivery_id,
                repo = %event.repository.full_name,
                pr = event.issue.number,
                "ignoring duplicate webhook delivery"
            );
            return (StatusCode::OK, "duplicate").into_response();
        }
        Err(e) => {
            tracing::error!(error = %e, "failed to enqueue job");
            return (StatusCode::INTERNAL_SERVER_ERROR, "queue error").into_response();
        }
    };

    let body = format!(
        ":hourglass_flowing_sand: queued at position **#{position}** (job `{job_id}`). I'll \
         update this comment when it starts."
    );
    match state
        .gh
        .create_pr_comment(
            event.installation.id,
            &event.repository.full_name,
            event.issue.number as u64,
            &body,
        )
        .await
    {
        Ok(comment) => {
            if let Err(e) = state
                .jobs
                .set_comment_id(job_id, comment.id)
                .await
            {
                tracing::error!(error = %e, "failed to persist comment id");
            }
        }
        Err(e) => tracing::error!(error = %e, "failed to post initial PR comment"),
    }

    (StatusCode::OK, "queued").into_response()
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
