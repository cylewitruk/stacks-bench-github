//! GitHub webhook entry point.
//!
//! The handler is deliberately the narrowest surface in the system. It:
//!   1. Reads the raw body (signature is over the bytes, not parsed JSON).
//!   2. Verifies the HMAC-SHA256 signature against the webhook secret.
//!   3. Drops unsupported event types at the wire (no DB row).
//!   4. For supported events, records a webhook inbox row — and nothing else.
//!
//! Slice 11 (cutover) made the handler **inbox-only**: it no longer
//! parses `/benchmark`, checks the authorization allowlist, or enqueues
//! a legacy `jobs` row. The processor (running in the orchestrator) owns
//! ALL classification, authorization, and job creation from the inbox.
//! This is also what makes the cutover reversible — the handler stops
//! writing legacy `jobs`, so flipping back to the legacy backend can't
//! consume jobs created after the cutover.
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
use sbgh_core::db::{IngestOutcome, NewWebhook};
use sbgh_core::github::verify_signature;
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

    // Inbox-only: every supported event (issue_comment included) is just
    // recorded. The processor owns command parsing, authorization, and
    // job creation from the inbox.
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
