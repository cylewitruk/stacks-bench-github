//! Slack Web API adapter over `reqwest`.

use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;

use crate::{FoundMessage, ReportingIdentity, Result, SlackClient, SlackError, SlackMessageTarget};

const SLACK_API_BASE: &str = "https://slack.com/api";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const METADATA_EVENT_TYPE: &str = "sbgh_progress";

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

    async fn call(&self, method: &str, body: serde_json::Value) -> Result<SlackApiResponse> {
        let response = self
            .http
            .post(format!("{}/{method}", self.api_base))
            .bearer_auth(&self.bot_token)
            .json(&body)
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            return Err(SlackError::HttpStatus {
                method: method.to_string(),
                status,
            });
        }
        let parsed: SlackApiResponse = response.json().await?;
        validate_envelope(method, parsed.ok, parsed.error.as_deref())?;
        Ok(parsed)
    }

    async fn bot_user_id(&self) -> Result<String> {
        self.call("auth.test", serde_json::json!({}))
            .await?
            .user_id
            .ok_or_else(|| SlackError::Protocol("auth.test returned ok but no user_id".into()))
    }
}

fn validate_envelope(method: &str, ok: bool, error: Option<&str>) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(SlackError::Api {
            method: method.to_string(),
            code: error
                .unwrap_or("unknown")
                .to_string(),
        })
    }
}

fn metadata(identity: &ReportingIdentity, snapshot_version: u64) -> serde_json::Value {
    serde_json::json!({
        "event_type": METADATA_EVENT_TYPE,
        "event_payload": {
            "reporting_identity": identity.as_str(),
            "snapshot_version": snapshot_version,
        }
    })
}

#[derive(Debug, Deserialize)]
struct SlackApiResponse {
    ok: bool,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    ts: Option<String>,
    #[serde(default)]
    user_id: Option<String>,
    #[serde(default)]
    messages: Vec<MessageEnvelope>,
    #[serde(default)]
    has_more: bool,
}

#[derive(Debug, Deserialize)]
struct MessageEnvelope {
    ts: String,
    #[serde(default)]
    text: String,
    #[serde(default)]
    user: Option<String>,
    #[serde(default)]
    metadata: Option<MessageMetadata>,
}

#[derive(Debug, Deserialize)]
struct MessageMetadata {
    event_type: String,
    event_payload: MessageMetadataPayload,
}

#[derive(Debug, Deserialize)]
struct MessageMetadataPayload {
    reporting_identity: String,
    #[serde(default)]
    snapshot_version: u64,
}

#[async_trait]
impl SlackClient for WebApiClient {
    async fn post_ephemeral(&self, channel: &str, user: &str, text: &str) -> Result<()> {
        self.call(
            "chat.postEphemeral",
            serde_json::json!({ "channel": channel, "user": user, "text": text }),
        )
        .await?;
        Ok(())
    }

    async fn post_message(
        &self,
        target: &SlackMessageTarget,
        text: &str,
        identity: &ReportingIdentity,
        snapshot_version: u64,
    ) -> Result<String> {
        self.call(
            "chat.postMessage",
            serde_json::json!({
                "channel": target.channel,
                "thread_ts": target.thread_ts,
                "text": text,
                "metadata": metadata(identity, snapshot_version),
                "unfurl_links": false,
                "unfurl_media": false,
            }),
        )
        .await?
        .ts
        .ok_or_else(|| SlackError::Protocol("chat.postMessage returned ok but no ts".into()))
    }

    async fn update_message(
        &self,
        channel: &str,
        ts: &str,
        text: &str,
        identity: &ReportingIdentity,
        snapshot_version: u64,
    ) -> Result<()> {
        self.call(
            "chat.update",
            serde_json::json!({
                "channel": channel,
                "ts": ts,
                "text": text,
                "metadata": metadata(identity, snapshot_version),
                "as_user": true,
            }),
        )
        .await?;
        Ok(())
    }

    async fn find_messages(
        &self,
        target: &SlackMessageTarget,
        identity: &ReportingIdentity,
    ) -> Result<Vec<FoundMessage>> {
        let bot_user_id = self.bot_user_id().await?;
        let response = self
            .call(
                "conversations.replies",
                serde_json::json!({
                    "channel": target.channel,
                    "ts": target.thread_ts,
                    "limit": 100,
                    "include_all_metadata": true,
                }),
            )
            .await?;
        if response.has_more {
            return Err(SlackError::Reconciliation(format!(
                "conversations.replies exceeded the bounded page for {}/{}",
                target.channel, target.thread_ts
            )));
        }
        Ok(response
            .messages
            .into_iter()
            .filter(|message| message.user.as_deref() == Some(bot_user_id.as_str()))
            .filter_map(|message| {
                let metadata = message.metadata?;
                (metadata.event_type == METADATA_EVENT_TYPE
                    && metadata
                        .event_payload
                        .reporting_identity
                        == identity.as_str())
                .then_some(FoundMessage {
                    ts: message.ts,
                    text: message.text,
                    snapshot_version: metadata
                        .event_payload
                        .snapshot_version,
                })
            })
            .collect())
    }

    async fn add_reaction(&self, channel: &str, ts: &str, reaction: &str) -> Result<()> {
        self.call(
            "reactions.add",
            serde_json::json!({ "channel": channel, "timestamp": ts, "name": reaction }),
        )
        .await?;
        Ok(())
    }

    async fn remove_reaction(&self, channel: &str, ts: &str, reaction: &str) -> Result<()> {
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

    #[test]
    fn logical_api_failure_preserves_method_and_error_code() {
        let error = validate_envelope("reactions.add", false, Some("already_reacted"))
            .unwrap_err()
            .to_string();
        assert!(error.contains("reactions.add"), "{error}");
        assert!(error.contains("already_reacted"), "{error}");
        assert!(validate_envelope("chat.postMessage", true, None).is_ok());
        assert!(validate_envelope("chat.postMessage", false, None).is_err());
    }

    #[test]
    fn metadata_contains_only_identity_and_projection_order() {
        let identity = ReportingIdentity::for_request("T1", "C1", "1.2");
        assert_eq!(
            metadata(&identity, 42),
            serde_json::json!({
                "event_type": "sbgh_progress",
                "event_payload": {
                    "reporting_identity": identity.as_str(),
                    "snapshot_version": 42,
                }
            })
        );
    }
}
