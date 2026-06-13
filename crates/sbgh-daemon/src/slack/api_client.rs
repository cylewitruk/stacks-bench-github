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
use crate::slack::stream::StreamChunk;

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

    /// POST `body` to a Web API `method` with bot-token auth, returning the
    /// parsed envelope on success. A transport/HTTP error or a logical failure
    /// (`ok=false`) is surfaced as `Err`.
    async fn call(
        &self,
        method: &str,
        body: serde_json::Value,
    ) -> anyhow::Result<SlackApiResponse> {
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
        interpret_response(method, parsed.ok, parsed.error.as_deref())?;
        Ok(parsed)
    }
}

fn start_stream_body(
    channel: &str,
    thread_ts: &str,
    recipient_user_id: &str,
    recipient_team_id: &str,
    chunks: &[StreamChunk],
) -> serde_json::Value {
    serde_json::json!({
        "channel": channel,
        "thread_ts": thread_ts,
        "recipient_user_id": recipient_user_id,
        "recipient_team_id": recipient_team_id,
        "task_display_mode": "plan",
        "chunks": chunks,
    })
}

fn append_stream_body(channel: &str, ts: &str, chunks: &[StreamChunk]) -> serde_json::Value {
    serde_json::json!({
        "channel": channel,
        "ts": ts,
        "chunks": chunks,
    })
}

fn stop_stream_body(
    channel: &str,
    ts: &str,
    markdown_text: Option<&str>,
    chunks: &[StreamChunk],
    blocks: Option<&serde_json::Value>,
) -> serde_json::Value {
    let mut body = serde_json::json!({
        "channel": channel,
        "ts": ts,
    });
    if chunks.is_empty()
        && let Some(text) = markdown_text
    {
        body["markdown_text"] = serde_json::Value::String(text.to_string());
    }
    if !chunks.is_empty() {
        body["chunks"] = serde_json::to_value(chunks).expect("stream chunks serialize");
    }
    if let Some(blocks) = blocks {
        body["blocks"] = blocks.clone();
    }
    body
}

/// The common `{ ok, error }` envelope every Web API method returns (plus the
/// posted message `ts`, present on `chat.postMessage`; other fields ignored).
#[derive(Deserialize)]
struct SlackApiResponse {
    ok: bool,
    error: Option<String>,
    ts: Option<String>,
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
        .await?;
        Ok(())
    }

    async fn post_blocks_in_thread(
        &self,
        channel: &str,
        thread_ts: &str,
        blocks: &serde_json::Value,
        fallback: &str,
    ) -> anyhow::Result<String> {
        let resp = self
            .call(
                "chat.postMessage",
                serde_json::json!({
                    "channel": channel,
                    "thread_ts": thread_ts,
                    "blocks": blocks,
                    "text": fallback,
                }),
            )
            .await?;
        resp.ts
            .ok_or_else(|| anyhow::anyhow!("slack chat.postMessage returned ok but no ts"))
    }

    async fn start_plan_stream(
        &self,
        channel: &str,
        thread_ts: &str,
        recipient_user_id: &str,
        recipient_team_id: &str,
        _markdown_text: &str,
        chunks: &[StreamChunk],
    ) -> anyhow::Result<String> {
        let resp = self
            .call(
                "chat.startStream",
                start_stream_body(channel, thread_ts, recipient_user_id, recipient_team_id, chunks),
            )
            .await?;
        resp.ts
            .ok_or_else(|| anyhow::anyhow!("slack chat.startStream returned ok but no ts"))
    }

    async fn append_stream(
        &self,
        channel: &str,
        ts: &str,
        chunks: &[StreamChunk],
    ) -> anyhow::Result<()> {
        self.call("chat.appendStream", append_stream_body(channel, ts, chunks))
            .await?;
        Ok(())
    }

    async fn stop_stream(
        &self,
        channel: &str,
        ts: &str,
        markdown_text: Option<&str>,
        chunks: &[StreamChunk],
        blocks: Option<&serde_json::Value>,
    ) -> anyhow::Result<()> {
        self.call("chat.stopStream", stop_stream_body(channel, ts, markdown_text, chunks, blocks))
            .await?;
        Ok(())
    }

    async fn update_blocks(
        &self,
        channel: &str,
        ts: &str,
        blocks: &serde_json::Value,
        fallback: &str,
    ) -> anyhow::Result<()> {
        self.call(
            "chat.update",
            serde_json::json!({
                "channel": channel,
                "ts": ts,
                "blocks": blocks,
                "text": fallback,
            }),
        )
        .await?;
        Ok(())
    }

    async fn add_reaction(&self, channel: &str, ts: &str, reaction: &str) -> anyhow::Result<()> {
        self.call(
            "reactions.add",
            serde_json::json!({ "channel": channel, "timestamp": ts, "name": reaction }),
        )
        .await?;
        Ok(())
    }

    async fn remove_reaction(&self, channel: &str, ts: &str, reaction: &str) -> anyhow::Result<()> {
        self.call(
            "reactions.remove",
            serde_json::json!({ "channel": channel, "timestamp": ts, "name": reaction }),
        )
        .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bench_summary::PlanTaskStatus;
    use crate::slack::card::CardRow;
    use crate::slack::stream::{StreamChunk, TaskUpdate};

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

    #[test]
    fn start_stream_body_matches_slack_contract() {
        let row = CardRow {
            title: "Queued".into(),
            status: PlanTaskStatus::Pending,
            details: Some("position 1/2".into()),
            output: None,
            source: None,
        };
        let chunks = vec![StreamChunk::TaskUpdate(TaskUpdate::from_row("job", &row))];
        assert_eq!(
            start_stream_body("C1", "111.222", "U1", "T1", &chunks),
            serde_json::json!({
                "channel": "C1",
                "thread_ts": "111.222",
                "recipient_user_id": "U1",
                "recipient_team_id": "T1",
                "task_display_mode": "plan",
                "chunks": [{
                    "type": "task_update",
                    "id": "job",
                    "title": "Queued · position 1/2",
                    "status": "pending",
                }],
            })
        );
    }

    #[test]
    fn append_stream_body_sends_chunks_without_markdown_text() {
        let chunks = vec![StreamChunk::PlanUpdate {
            title: "Benchmark develop @ abcdef12".into(),
        }];
        assert_eq!(
            append_stream_body("C1", "222.333", &chunks),
            serde_json::json!({
                "channel": "C1",
                "ts": "222.333",
                "chunks": [{
                    "type": "plan_update",
                    "title": "Benchmark develop @ abcdef12",
                }],
            })
        );
    }

    #[test]
    fn stop_stream_body_can_finalize_with_bottom_blocks() {
        let blocks = serde_json::json!([{ "type": "divider" }]);
        assert_eq!(
            stop_stream_body("C1", "222.333", Some("Done"), &[], Some(&blocks)),
            serde_json::json!({
                "channel": "C1",
                "ts": "222.333",
                "markdown_text": "Done",
                "blocks": [{ "type": "divider" }],
            })
        );
    }

    #[test]
    fn stop_stream_body_prefers_chunks_over_markdown_text() {
        let chunks = vec![StreamChunk::PlanUpdate {
            title: "Benchmark develop @ abcdef12".into(),
        }];
        assert_eq!(
            stop_stream_body("C1", "222.333", Some("Done"), &chunks, None),
            serde_json::json!({
                "channel": "C1",
                "ts": "222.333",
                "chunks": [{
                    "type": "plan_update",
                    "title": "Benchmark develop @ abcdef12",
                }],
            })
        );
    }
}
