//! GitHub webhook entry point.
//!
//! The handler is deliberately the narrowest surface in the system. It:
//!   1. Reads the raw body (signature is over the bytes, not parsed JSON).
//!   2. Verifies the HMAC-SHA256 signature against the webhook secret.
//!   3. Forwards the verified delivery to the daemon `/api`.
//!
//! That's the whole job. The handler does NOT parse the payload, filter
//! event types, check the authorization allowlist, or touch a database —
//! the daemon owns all of that. It holds NO GitHub App credentials and NO
//! DB role: a compromised handler can submit signature-verified deliveries
//! (which it could already do by forwarding bytes) and nothing more.

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use sbgh_api::ClientError;

use crate::signature::verify_signature;
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
        None => {
            tracing::warn!("rejecting webhook: missing X-Hub-Signature-256 header");
            return (StatusCode::UNAUTHORIZED, "missing signature").into_response();
        }
    };
    if let Err(e) = verify_signature(&state.config.webhook.secret, &body, signature) {
        tracing::warn!(error = %e, "rejecting webhook: bad signature");
        return (StatusCode::UNAUTHORIZED, "invalid signature").into_response();
    }

    let event = headers
        .get(EVENT_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    // ping is GitHub's connectivity probe; reply pong locally without a
    // round-trip to the daemon (it would be filtered as unsupported anyway).
    if event == "ping" {
        return (StatusCode::OK, "pong").into_response();
    }

    // Forward as-is. The daemon owns the event-type allowlist (it returns
    // "ignored" for unsupported events) and the delivery-id requirement, so
    // we pass whatever headers GitHub sent without second-guessing them. A
    // missing delivery stays absent (not `""`) so the daemon's required-id
    // check fires rather than recording a blank dedup key.
    let delivery = headers
        .get(DELIVERY_HEADER)
        .and_then(|v| v.to_str().ok());

    match state
        .api
        .submit_webhook(event, delivery, body.to_vec())
        .await
    {
        Ok(resp) => {
            tracing::info!(
                delivery = delivery.unwrap_or("-"),
                event,
                result = %resp.result,
                "forwarded webhook to /api"
            );
            (StatusCode::OK, resp.result).into_response()
        }
        // A 4xx from the daemon is a permanent client error (e.g. a bad
        // ingest token, or a supported event with no delivery id) — retrying
        // won't help, so surface it and let GitHub stop redelivering. A
        // transport failure or 5xx is the daemon being unreachable/unhealthy;
        // return 502 so GitHub retries and the delivery isn't lost.
        Err(ClientError::Api { status, code, message }) if (400..500).contains(&status) => {
            tracing::error!(
                delivery = delivery.unwrap_or("-"),
                event,
                status,
                code,
                %message,
                "webhook rejected by /api"
            );
            (StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY), "rejected")
                .into_response()
        }
        Err(e) => {
            tracing::error!(
                delivery = delivery.unwrap_or("-"),
                event,
                error = %e,
                "failed to forward webhook to /api"
            );
            (StatusCode::BAD_GATEWAY, "upstream error").into_response()
        }
    }
}
