//! Smee message → HTTP forward.
//!
//! A smee SSE message's `data` is a JSON object of the shape:
//!
//! ```json
//! {
//!   "host": "smee.io",
//!   "user-agent": "GitHub-Hookshot/...",
//!   "x-github-event": "issue_comment",
//!   "x-github-delivery": "<uuid>",
//!   "x-hub-signature-256": "sha256=...",
//!   "content-type": "application/json",
//!   "body":      { ... },         // parsed JSON payload
//!   "query":     {},              // typically empty for GitHub
//!   "timestamp": 1234567890
//! }
//! ```
//!
//! Every top-level key except `body` / `query` / `timestamp` is a candidate
//! HTTP header. Before forwarding we additionally strip:
//!
//!   - **Hop-by-hop headers** (RFC 7230 §6.1: `connection`, `keep-alive`,
//!     `proxy-authenticate`, `proxy-authorization`, `te`, `trailer`,
//!     `transfer-encoding`, `upgrade`) — they describe a single transport hop
//!     and must not cross the smee→localhost boundary.
//!   - **`host`** — we're POSTing to localhost, not smee.io; reqwest sets the
//!     correct Host from the target URL.
//!   - **`content-length`** — the body is re-serialized below, so the original
//!     length is stale; reqwest computes the right one from the new bytes.
//!
//! See `should_forward_header` for the canonical list.
//!
//! The body is re-serialized and POSTed to the target. Because the receiver
//! (sbgh-handler) verifies the GitHub HMAC against the raw body bytes, our
//! re-serialization MUST produce the same bytes GitHub originally sent —
//! that's why `serde_json` is built with `preserve_order` at the workspace
//! level (matches V8's behaviour that upstream smee-client relies on).

use anyhow::{Context, Result};
use reqwest::header::HeaderName;
use serde_json::{Map, Value};

/// The result of parsing a smee SSE message into something forwardable.
///
/// `headers` is the post-filter list (hop-by-hop, `host`, `content-length`
/// already removed — see `should_forward_header`). `body` is the JSON body
/// re-serialized into bytes, ready to hand to `reqwest::RequestBuilder::body`.
#[derive(Debug)]
pub struct ParsedRequest {
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// Pure transformation: pull the body bytes + headers out of a smee message.
/// Split out so we can unit-test it without a live HTTP target.
pub fn parse_smee_payload(raw: &str) -> Result<ParsedRequest> {
    let mut payload: Map<String, Value> =
        serde_json::from_str(raw).context("smee payload was not a JSON object")?;

    let body = payload
        .remove("body")
        .context("smee payload missing required 'body' field")?;
    payload.remove("query");
    payload.remove("timestamp");

    let body_bytes = serde_json::to_vec(&body).context("re-serialize body for forwarding")?;

    let headers: Vec<(String, String)> = payload
        .into_iter()
        .filter_map(|(k, v)| {
            let Value::String(s) = v else {
                tracing::trace!(header = %k, "skipping non-string header field");
                return None;
            };
            if !should_forward_header(&k) {
                tracing::trace!(header = %k, "stripping header before forward");
                return None;
            }
            Some((k, s))
        })
        .collect();

    Ok(ParsedRequest { headers, body: body_bytes })
}

/// Whether to pass a given header through to the local target.
///
/// Two categories are dropped:
///   1. **Hop-by-hop headers** (RFC 7230 §6.1). These describe a single
///      transport hop and must never cross proxy boundaries. Forwarding e.g. a
///      stale `Transfer-Encoding: chunked` from the GitHub→smee hop into our
///      smee→localhost hop is incorrect framing.
///   2. **Headers the receiving stack will (and should) regenerate**: `Host` —
///      we're POSTing to localhost, not smee.io; reqwest sets the correct Host
///      from the target URL. Forwarding the original would break virtual-host
///      routing on any handler behind a reverse proxy. `Content-Length` — the
///      body is re-serialized in `parse_smee_payload`, so the original length
///      is stale; reqwest computes the right one from the bytes we set with
///      `.body()`.
fn should_forward_header(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    !matches!(
        lower.as_str(),
        // Hop-by-hop (RFC 7230 §6.1)
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
            // Reqwest regenerates these from the request itself.
            | "host"
            | "content-length"
    )
}

/// The GitHub webhook headers worth surfacing in logs whenever smee acts
/// on a delivery. All optional — a non-GitHub or malformed delivery may
/// omit any of them. Header names are matched case-insensitively.
#[derive(Debug, Default)]
pub struct DeliveryMeta {
    pub delivery: Option<String>,
    pub event: Option<String>,
    pub hook_id: Option<String>,
    pub hook_target_id: Option<String>,
    pub hook_target_type: Option<String>,
}

/// Result of a forward attempt: the target's response status plus the
/// GitHub delivery headers, so the caller can log "which delivery" at the
/// level the status warrants.
pub struct ForwardOutcome {
    pub status: reqwest::StatusCode,
    pub meta: DeliveryMeta,
}

/// Pull the GitHub delivery/event/hook headers out of the (already
/// hop-by-hop-filtered) forward header list for logging.
fn extract_delivery_meta(headers: &[(String, String)]) -> DeliveryMeta {
    let get = |name: &str| {
        headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.clone())
    };
    DeliveryMeta {
        delivery: get("x-github-delivery"),
        event: get("x-github-event"),
        hook_id: get("x-github-hook-id"),
        hook_target_id: get("x-github-hook-installation-target-id"),
        hook_target_type: get("x-github-hook-installation-target-type"),
    }
}

pub async fn forward(client: &reqwest::Client, target: &str, raw: &str) -> Result<ForwardOutcome> {
    let ParsedRequest { headers, body } = parse_smee_payload(raw)?;
    let meta = extract_delivery_meta(&headers);

    let mut req = client.post(target);
    for (k, v) in &headers {
        match HeaderName::try_from(k.as_str()) {
            Ok(name) => req = req.header(name, v),
            Err(e) => tracing::warn!(header = %k, error = %e, "skipping invalid header name"),
        }
    }

    let resp = req
        .body(body)
        .send()
        .await
        .context("POST to forwarding target")?;
    Ok(ForwardOutcome { status: resp.status(), meta })
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    #[test]
    fn parse_extracts_body_and_keeps_github_headers() {
        let raw = r#"{
            "host": "smee.io",
            "x-github-event": "issue_comment",
            "x-github-delivery": "abc-123",
            "x-hub-signature-256": "sha256=deadbeef",
            "content-type": "application/json",
            "user-agent": "GitHub-Hookshot/abcd",
            "body": {"action": "created", "issue": {"number": 42}},
            "query": {},
            "timestamp": 1234567890
        }"#;

        let parsed = parse_smee_payload(raw).unwrap();
        let body_str = String::from_utf8(parsed.body).unwrap();
        assert_eq!(body_str, r#"{"action":"created","issue":{"number":42}}"#);

        let header_map: std::collections::HashMap<_, _> = parsed
            .headers
            .into_iter()
            .collect();
        // The signature header MUST come through — without it the handler 401s
        // everything and the whole tunnel is useless.
        assert_eq!(
            header_map
                .get("x-hub-signature-256")
                .map(String::as_str),
            Some("sha256=deadbeef")
        );
        assert_eq!(
            header_map
                .get("x-github-event")
                .map(String::as_str),
            Some("issue_comment")
        );
        assert_eq!(
            header_map
                .get("x-github-delivery")
                .map(String::as_str),
            Some("abc-123")
        );
        assert_eq!(
            header_map
                .get("content-type")
                .map(String::as_str),
            Some("application/json")
        );
        // Smee.io's own `Host` must NOT be forwarded — we're talking to localhost.
        assert!(!header_map.contains_key("host"));
        // Reserved smee fields are stripped.
        assert!(!header_map.contains_key("body"));
        assert!(!header_map.contains_key("query"));
        assert!(!header_map.contains_key("timestamp"));
    }

    #[test]
    fn parse_strips_hop_by_hop_and_framing_headers() {
        // Regression test for the Codex review: any header that describes the
        // upstream transport hop must not leak into the downstream POST.
        let raw = r#"{
            "host": "smee.io",
            "content-length": "9999",
            "connection": "keep-alive",
            "keep-alive": "timeout=5",
            "transfer-encoding": "chunked",
            "te": "trailers",
            "upgrade": "websocket",
            "trailer": "Expires",
            "proxy-authenticate": "Basic",
            "proxy-authorization": "Basic xxx",
            "x-github-event": "issue_comment",
            "body": {"keep_this": true}
        }"#;
        let parsed = parse_smee_payload(raw).unwrap();
        let keys: Vec<&str> = parsed
            .headers
            .iter()
            .map(|(k, _)| k.as_str())
            .collect();
        assert_eq!(keys, vec!["x-github-event"], "only the GitHub semantic header should survive");
    }

    #[test]
    fn header_filter_is_case_insensitive() {
        // Smee.io lowercases all keys today, but be defensive.
        let raw = r#"{
            "Host": "smee.io",
            "Content-Length": "9999",
            "TRANSFER-ENCODING": "chunked",
            "X-Github-Event": "ping",
            "body": {}
        }"#;
        let parsed = parse_smee_payload(raw).unwrap();
        let keys: Vec<&str> = parsed
            .headers
            .iter()
            .map(|(k, _)| k.as_str())
            .collect();
        // Mixed-case framing/host headers must still be dropped.
        assert!(
            !keys
                .iter()
                .any(|k| k.eq_ignore_ascii_case("host"))
        );
        assert!(
            !keys
                .iter()
                .any(|k| k.eq_ignore_ascii_case("content-length"))
        );
        assert!(
            !keys
                .iter()
                .any(|k| k.eq_ignore_ascii_case("transfer-encoding"))
        );
        assert!(
            keys.iter()
                .any(|k| k.eq_ignore_ascii_case("x-github-event"))
        );
    }

    #[test]
    fn parse_preserves_body_key_order_for_hmac_compat() {
        // The whole reason for the `preserve_order` feature on serde_json:
        // if we change body key order on the round trip, GitHub's HMAC over
        // the original body no longer validates and the handler 401s every
        // delivery that comes through smee. This test pins the invariant.
        let raw = r#"{
            "body": {"zeta": 1, "alpha": 2, "middle": [9, 8, 7]},
            "timestamp": 1
        }"#;
        let parsed = parse_smee_payload(raw).unwrap();
        let body_str = String::from_utf8(parsed.body).unwrap();
        assert_eq!(body_str, r#"{"zeta":1,"alpha":2,"middle":[9,8,7]}"#);
    }

    #[test]
    fn parse_drops_non_string_header_values() {
        // Malformed payload with object-valued top-level keys: those can't
        // be HTTP headers, so we skip them instead of erroring the whole
        // delivery.
        let raw = r#"{
            "body": {},
            "x-github-event": "ping",
            "garbage": {"not": "a string"}
        }"#;
        let parsed = parse_smee_payload(raw).unwrap();
        let keys: Vec<&str> = parsed
            .headers
            .iter()
            .map(|(k, _)| k.as_str())
            .collect();
        assert_eq!(keys, vec!["x-github-event"]);
    }

    #[test]
    fn parse_errors_when_body_missing() {
        let raw = r#"{"timestamp": 1}"#;
        let err = parse_smee_payload(raw).unwrap_err();
        assert!(
            err.to_string()
                .contains("'body'")
        );
    }

    #[test]
    fn parse_errors_on_invalid_json() {
        assert!(parse_smee_payload("{not json").is_err());
    }

    /// End-to-end: spin up a 1-shot TCP listener, run `forward`, and assert
    /// the captured request bytes have our method + headers + body. We avoid
    /// `wiremock` to keep dev-deps minimal — the HTTP request we send is
    /// trivial to parse by hand.
    #[tokio::test]
    async fn forward_posts_body_and_headers_to_target() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        let captured: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
        let captured_clone = captured.clone();

        let server = tokio::spawn(async move {
            let (mut socket, _) = listener
                .accept()
                .await
                .unwrap();
            let mut buf = Vec::new();
            // Read until the client finishes writing. We can't read until EOF
            // (reqwest keeps the conn open), so we read whatever's available
            // then respond. 8 KiB is more than enough for these small payloads.
            let mut tmp = [0u8; 8192];
            let n = socket
                .read(&mut tmp)
                .await
                .unwrap();
            buf.extend_from_slice(&tmp[..n]);
            *captured_clone.lock().unwrap() = Some(buf);
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                .await
                .unwrap();
            socket.shutdown().await.ok();
        });

        let target = format!("http://{addr}/webhook");
        let client = reqwest::Client::new();
        let raw = r#"{
            "x-github-event": "issue_comment",
            "x-github-delivery": "deliv-1",
            "content-type": "application/json",
            "body": {"hello": "world"},
            "timestamp": 1
        }"#;
        let outcome = forward(&client, &target, raw)
            .await
            .unwrap();
        assert_eq!(outcome.status, reqwest::StatusCode::OK);
        // Delivery headers are surfaced for logging.
        assert_eq!(
            outcome
                .meta
                .delivery
                .as_deref(),
            Some("deliv-1")
        );
        assert_eq!(outcome.meta.event.as_deref(), Some("issue_comment"));

        server.await.unwrap();

        let raw_request = captured
            .lock()
            .unwrap()
            .take()
            .expect("server captured request");
        let request_text = String::from_utf8_lossy(&raw_request);
        assert!(request_text.starts_with("POST /webhook "), "got: {request_text}");
        assert!(
            request_text
                .to_lowercase()
                .contains("x-github-event: issue_comment")
        );
        assert!(
            request_text
                .to_lowercase()
                .contains("x-github-delivery: deliv-1")
        );
        // Body must arrive verbatim (compact JSON, no key reorder).
        assert!(request_text.ends_with("{\"hello\":\"world\"}"));
    }
}
