//! The real [`SlackClient`] — Slack **Web API** over `reqwest` (item 0002
//! wiring, 3d-2).
//!
//! The methods we use (`chat.postMessage`, `chat.postEphemeral`,
//! `reactions.add`/`remove`) are plain bot-token JSON POSTs to
//! `https://slack.com/api/<method>`, each returning `{ "ok": bool, "error"?:
//! .. }`. Kept on our existing `reqwest`/rustls stack and deliberately separate
//! from the Socket Mode transport (slack-morphism) so the outbound surface
//! stays small and the trait impl owns no third-party types.

use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;

use crate::slack::client::SlackClient;

/// Slack Web API root. Slack does not offer per-workspace API hosts, so this is
/// a constant (unlike the configurable GitHub base).
const SLACK_API_BASE: &str = "https://slack.com/api";

/// Connect timeout for a Web API call (TCP + TLS handshake).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Total per-request timeout. Slack responses are small + fast, so a call that
/// outlasts this is stalled — and these are awaited in the reporter's terminal
/// path (after the DB write), so an unbounded hang would delay slot cleanup.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

/// A bot-token Web API client. One per process (the bot token is
/// workspace-wide).
pub struct WebApiClient {
    http: reqwest::Client,
    bot_token: String,
    api_base: String,
}

impl WebApiClient {
    pub fn new(bot_token: String) -> Self {
        Self {
            http: reqwest::Client::builder()
                .connect_timeout(CONNECT_TIMEOUT)
                .timeout(REQUEST_TIMEOUT)
                .build()
                .expect("building reqwest client"),
            bot_token,
            api_base: SLACK_API_BASE.to_string(),
        }
    }

    /// POST `body` to a Web API `method` with bot-token auth and surface a
    /// logical failure (`ok=false`) — or a transport/HTTP error — as `Err`.
    async fn call(&self, method: &str, body: serde_json::Value) -> anyhow::Result<()> {
        let url = format!("{}/{method}", self.api_base);
        let resp = self
            .http
            .post(&url)
            .bearer_auth(&self.bot_token)
            .json(&body)
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            // 429 / 5xx — no JSON `ok` envelope to read; report the code.
            anyhow::bail!("slack {method} HTTP {status}");
        }
        let parsed: SlackApiResponse = resp.json().await?;
        interpret_response(method, parsed.ok, parsed.error.as_deref())
    }
}

/// The common `{ ok, error }` envelope every Web API method returns (extra
/// per-method fields are ignored).
#[derive(Deserialize)]
struct SlackApiResponse {
    ok: bool,
    error: Option<String>,
}

/// Map a parsed `{ ok, error }` envelope to a `Result` — `Ok` on `ok=true`,
/// else an `Err` naming the method + Slack's error code. Pure, so it's unit
/// tested without the network.
fn interpret_response(method: &str, ok: bool, error: Option<&str>) -> anyhow::Result<()> {
    if ok {
        Ok(())
    } else {
        anyhow::bail!("slack {method} failed: {}", error.unwrap_or("unknown"));
    }
}

#[async_trait]
impl SlackClient for WebApiClient {
    async fn post_ephemeral(&self, channel: &str, user: &str, text: &str) -> anyhow::Result<()> {
        self.call(
            "chat.postEphemeral",
            serde_json::json!({ "channel": channel, "user": user, "text": text }),
        )
        .await
    }

    async fn post_in_thread(
        &self,
        channel: &str,
        thread_ts: &str,
        text: &str,
    ) -> anyhow::Result<()> {
        self.call(
            "chat.postMessage",
            serde_json::json!({ "channel": channel, "thread_ts": thread_ts, "text": text }),
        )
        .await
    }

    async fn add_reaction(&self, channel: &str, ts: &str, reaction: &str) -> anyhow::Result<()> {
        self.call(
            "reactions.add",
            serde_json::json!({ "channel": channel, "timestamp": ts, "name": reaction }),
        )
        .await
    }

    async fn remove_reaction(&self, channel: &str, ts: &str, reaction: &str) -> anyhow::Result<()> {
        self.call(
            "reactions.remove",
            serde_json::json!({ "channel": channel, "timestamp": ts, "name": reaction }),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ok_response_is_success() {
        assert!(interpret_response("chat.postMessage", true, None).is_ok());
    }

    #[test]
    fn not_ok_surfaces_the_error_code() {
        let err = interpret_response("reactions.add", false, Some("already_reacted"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("reactions.add"), "{err}");
        assert!(err.contains("already_reacted"), "{err}");
    }

    #[test]
    fn not_ok_without_error_is_still_an_error() {
        assert!(interpret_response("chat.postMessage", false, None).is_err());
    }
}
