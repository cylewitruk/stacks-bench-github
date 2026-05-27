//! GitHub webhook entry point.
//!
//! The handler is deliberately the narrowest surface in the system. It:
//!   1. Reads the raw body (signature is over the bytes, not parsed JSON).
//!   2. Verifies the HMAC-SHA256 signature against the webhook secret.
//!   3. Drops unsupported event types at the wire (no DB row).
//!   4. For supported events, records a webhook inbox row.
//!   5. For `issue_comment` specifically: parses the `/benchmark` command,
//!      checks the authorization allowlist, and atomically enqueues a legacy
//!      `jobs` row alongside the inbox row when authorized.
//!
//! Notably absent: any GitHub API call. The handler holds NO App
//! credentials. Head-SHA resolution and the initial PR comment are both
//! deferred to the orchestrator, which runs on the host with the App
//! private key. The handler's DB grants are INSERT-only on the columns
//! it writes.

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use sbgh_core::config::AuthorizationConfig;
use sbgh_core::db::{IngestOutcome, NewWebhook};
use sbgh_core::github::{IssueCommentEvent, parse_command, verify_signature};
use sbgh_core::models::NewJob;
use serde_json::Value;

use crate::state::AppState;

const EVENT_HEADER: &str = "x-github-event";
const SIGNATURE_HEADER: &str = "x-hub-signature-256";
const DELIVERY_HEADER: &str = "x-github-delivery";

/// GitHub events the handler accepts into the inbox. Unsupported event
/// types (stars, forks, etc.) are dropped at the wire with no DB row.
/// The processor (slices 2+) decides what to do with each accepted
/// event; the handler is a thin transport.
const SUPPORTED_EVENT_TYPES: &[&str] = &[
    "issue_comment",
    "push",
    "pull_request",
    "create",
    "installation",
    "installation_repositories",
];

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

    // ping is GitHub's connectivity probe; it carries no payload worth
    // recording and predates the inbox model. Reply pong without a DB
    // write, same as before.
    if event == "ping" {
        return (StatusCode::OK, "pong").into_response();
    }

    // Drop anything not on the supported-event allowlist before
    // touching the DB. This is the handler's only filter; everything
    // else is processor work.
    if !SUPPORTED_EVENT_TYPES.contains(&event) {
        tracing::debug!(event, "dropping unsupported event type");
        return (StatusCode::OK, "ignored").into_response();
    }

    let delivery_id = headers
        .get(DELIVERY_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let Some(delivery_id) = delivery_id else {
        tracing::warn!("rejecting webhook: missing X-GitHub-Delivery");
        return (StatusCode::BAD_REQUEST, "missing delivery id").into_response();
    };

    // Parse the body once as a generic Value for inbox storage and
    // for extracting action / installation id. If JSON parsing fails
    // the inbox row is still written with NULL payload — the
    // signature-verified bytes are forensically interesting on their
    // own and we don't want to silently drop deliveries.
    let payload_value = serde_json::from_slice::<Value>(&body).ok();
    let action = payload_value
        .as_ref()
        .and_then(|v| v.get("action"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let payload_installation_id = payload_value
        .as_ref()
        .and_then(|v| v.get("installation"))
        .and_then(|v| v.get("id"))
        .and_then(Value::as_i64);

    let webhook = NewWebhook {
        delivery_id: delivery_id.clone(),
        event_type: event.to_string(),
        action: action.clone(),
        payload_installation_id,
        payload: payload_value.clone(),
        payload_size_bytes: body.len() as i32,
    };

    // issue_comment is the only event that may trigger a legacy job
    // enqueue. Everything else goes through the webhook-only path.
    if event == "issue_comment" {
        return handle_issue_comment(state, &webhook, &body, &delivery_id).await;
    }

    match state
        .ingest
        .ingest_webhook(&webhook)
        .await
    {
        Ok(IngestOutcome::Recorded { .. }) => (StatusCode::OK, "recorded").into_response(),
        Ok(IngestOutcome::Duplicate) => {
            tracing::info!(delivery = %delivery_id, event, "duplicate webhook delivery");
            (StatusCode::OK, "duplicate").into_response()
        }
        Err(e) => {
            tracing::error!(error = ?e, "failed to record webhook");
            (StatusCode::INTERNAL_SERVER_ERROR, "ingest error").into_response()
        }
    }
}

async fn handle_issue_comment(
    state: AppState,
    webhook: &NewWebhook,
    body: &[u8],
    delivery_id: &str,
) -> axum::response::Response {
    // For any non-success path (typed-parse failure, wrong action, no
    // command, unauthorized, etc.), we still record the webhook (audit
    // + future processor input + GH redelivery dedupe) and return 2xx.
    // Crucial: GH never sees a 4xx that would trigger an un-dedupable
    // retry storm — the inbox row IS the dedup key.
    let webhook_only = |reason: &'static str| {
        let state = state.clone();
        let webhook = webhook.clone();
        let delivery_id = delivery_id.to_string();
        async move {
            match state
                .ingest
                .ingest_webhook(&webhook)
                .await
            {
                Ok(IngestOutcome::Recorded { .. }) | Ok(IngestOutcome::Duplicate) => {
                    (StatusCode::OK, reason).into_response()
                }
                Err(e) => {
                    tracing::error!(error = ?e, delivery = %delivery_id, "failed to record webhook");
                    (StatusCode::INTERNAL_SERVER_ERROR, "ingest error").into_response()
                }
            }
        }
    };

    let event: IssueCommentEvent = match serde_json::from_slice(body) {
        Ok(e) => e,
        Err(e) => {
            // Body is signature-verified but doesn't match the typed
            // shape (truncated, GH schema drift, etc.). Record the
            // webhook anyway so GH redeliveries dedupe against the
            // inbox row. If the body was syntactically valid JSON but
            // had the wrong shape, the stored payload is inspectable
            // later; if it was syntactically invalid, payload is SQL
            // NULL (only event_type/delivery_id/size survive for
            // forensics).
            tracing::warn!(error = %e, delivery = %delivery_id, "issue_comment payload decode failed");
            return webhook_only("bad payload").await;
        }
    };

    if event.action != "created" {
        return webhook_only("ignored").await;
    }
    if event
        .issue
        .pull_request
        .is_none()
    {
        return webhook_only("not a PR").await;
    }

    let command = match parse_command(&event.comment.body) {
        Ok(Some(c)) => c,
        Ok(None) => return webhook_only("no command").await,
        Err(e) => {
            tracing::info!(error = %e, "malformed command");
            return webhook_only("malformed command").await;
        }
    };

    if let Err(reason) = authorized(&state.config.authorization, &event) {
        tracing::warn!(
            user = %event.sender.login,
            repo = %event.repository.full_name,
            %reason,
            "rejecting unauthorized command"
        );
        return webhook_only("unauthorized").await;
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
        github_delivery_id: Some(delivery_id.to_string()),
    };

    match state
        .ingest
        .ingest_webhook_and_job(webhook, &new)
        .await
    {
        Ok(IngestOutcome::Recorded { job_id: Some(_), .. }) => {
            (StatusCode::OK, "queued").into_response()
        }
        Ok(IngestOutcome::Recorded { job_id: None, .. }) => {
            // Webhook recorded; legacy job conflict-skipped (delivery
            // already existed in `jobs` from before slice 1). Treat as
            // success for GH (no retry needed).
            tracing::info!(
                delivery = %delivery_id,
                "webhook recorded but legacy job already existed for this delivery"
            );
            (StatusCode::OK, "recorded").into_response()
        }
        Ok(IngestOutcome::Duplicate) => {
            tracing::info!(
                delivery = %delivery_id,
                repo = %event.repository.full_name,
                pr = event.issue.number,
                "duplicate webhook delivery"
            );
            (StatusCode::OK, "duplicate").into_response()
        }
        Err(e) => {
            tracing::error!(error = ?e, "failed to ingest webhook + job");
            (StatusCode::INTERNAL_SERVER_ERROR, "ingest error").into_response()
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
