//! Socket Mode receive loop (item 0002 wiring, 3d-2).
//!
//! `slack-morphism` owns the WebSocket transport — `apps.connections.open`, the
//! reconnect/backoff, and the envelope ack discipline. Our job is the thin
//! adapter on top: map each `app_mention` push into a [`MentionEvent`] and hand
//! it to the [`SlackConnector`] (which does authz → resolve → enqueue → react).
//! Outbound Web API calls go through
//! [`WebApiClient`](crate::slack::api_client::WebApiClient) (reqwest), so the
//! only slack-morphism surface here is the inbound socket.
//!
//! The connector rides into slack-morphism's fn-pointer callbacks via its
//! per-listener **user state** (the callbacks can't capture environment).

use std::sync::Arc;

use sbgh_core::config::SlackConfig;
use sbgh_core::db::JobStore;
use slack_morphism::prelude::*;
use tokio_util::sync::CancellationToken;

use crate::slack::connector::{MentionEvent, SlackConnector};
use crate::slack::target::SlackJobTarget;

/// Map a Slack push event to our [`MentionEvent`], or `None` for any push that
/// isn't an `app_mention` (the only kind the connector handles). Pure — the
/// `.0`s are the slack-morphism id newtypes' public `String`; missing text
/// degrades to empty (→ an empty-request rejection downstream).
pub fn mention_from_callback(callback: &SlackPushEventCallback) -> Option<MentionEvent> {
    let SlackEventCallbackBody::AppMention(mention) = &callback.event else {
        return None;
    };
    Some(MentionEvent {
        team_id: callback.team_id.0.clone(),
        user: mention.user.0.clone(),
        channel: mention.channel.0.clone(),
        message_ts: mention.origin.ts.0.clone(),
        text: mention
            .content
            .text
            .clone()
            .unwrap_or_default(),
    })
}

/// The push-event callback. A fn pointer (slack-morphism's callback type), so
/// the connector is fetched from the listener's user state rather than
/// captured.
///
/// **Ack discipline:** slack-morphism awaits this callback before acking the
/// Socket Mode envelope, and Slack redelivers (→ a duplicate enqueue) if the
/// ack misses its ~3s budget. So the orchestration (authz → parse → DB enqueue
/// → `reactions.add`) is **spawned** and we return `Ok` immediately — the ack
/// never waits on a slow DB / Web API call. `handle_mention` swallows its own
/// failures, and a non-`app_mention` push is simply ignored.
async fn on_push_event(
    event: SlackPushEventCallback,
    _client: Arc<SlackHyperClient>,
    state: SlackClientEventsUserState,
) -> UserCallbackResult<()> {
    let Some(mention) = mention_from_callback(&event) else {
        return Ok(());
    };
    let connector = {
        let states = state.read().await;
        states
            .get_user_state::<Arc<SlackConnector>>()
            .cloned()
    };
    spawn_dispatch(connector, mention);
    Ok(())
}

/// Spawn the benchmark orchestration off the ack path (see [`on_push_event`]).
/// Fire-and-forget: `handle_mention` owns its own error handling, so the
/// `JoinHandle` is dropped. `None` connector ⇒ a wiring bug (the listener was
/// built without its user state).
fn spawn_dispatch(connector: Option<Arc<SlackConnector>>, mention: MentionEvent) {
    match connector {
        Some(connector) => {
            tokio::spawn(async move {
                connector
                    .handle_mention(mention)
                    .await
            });
        }
        None => tracing::error!("slack: connector missing from listener user state"),
    }
}

/// Run the Socket Mode receive loop until `shutdown` fires, then close the
/// socket cleanly. `app_token` (xapp-) opens the WS; `web_client` is the shared
/// Web API client (same bot token the reporter posts results with). Returns
/// `Err` only on a startup failure (bad/missing app token, TLS) — the caller
/// decides whether that is fatal (it isn't: Slack is an optional surface).
pub async fn run(
    cfg: SlackConfig,
    target: SlackJobTarget,
    jobs: Arc<dyn JobStore>,
    web_client: Arc<dyn crate::slack::client::SlackClient>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let app_token_value = cfg
        .app_token
        .clone()
        .ok_or_else(|| {
            anyhow::anyhow!("slack: [slack].enabled but SBGH_SLACK_APP_TOKEN is unset")
        })?;

    // The connector (authz/resolve/enqueue/react) shares the Web API client and
    // rides into the listener via user state.
    let connector = Arc::new(SlackConnector::new(cfg, target, jobs, web_client));

    let slack_client = Arc::new(SlackHyperClient::new(SlackClientHyperConnector::new()?));
    let environment = Arc::new(
        SlackClientEventsListenerEnvironment::new(slack_client).with_user_state(connector),
    );
    let callbacks = SlackSocketModeListenerCallbacks::new().with_push_events(on_push_event);
    let listener = SlackClientSocketModeListener::new(
        &SlackClientSocketModeConfig::new(),
        environment,
        callbacks,
    );

    let app_token = SlackApiToken::new(SlackApiTokenValue(app_token_value));
    listener
        .listen_for(&app_token)
        .await?;
    listener.start().await;
    tracing::info!("slack: socket mode connected; listening for bot mentions");

    // The WSS clients run in background tasks; hold the listener alive until the
    // daemon drains, then tear the socket down.
    shutdown.cancelled().await;
    tracing::info!("slack: shutdown requested; closing socket mode");
    listener.shutdown().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use sbgh_core::db::InMemoryJobStore;
    use sbgh_core::models::TriggerKind;

    use super::*;
    use crate::slack::client::{QUEUED_REACTION, SlackClient};

    /// A realistic Slack Socket Mode `event_callback` envelope for an
    /// `app_mention` (the shape slack-morphism deserializes off the wire).
    const APP_MENTION_JSON: &str = r#"{
        "team_id": "T_OK",
        "api_app_id": "A123",
        "event": {
            "type": "app_mention",
            "user": "U_OK",
            "channel": "C1",
            "text": "<@U07BOT> bench --block 184231",
            "ts": "1700000000.000100"
        },
        "event_id": "Ev123",
        "event_time": 1700000000
    }"#;

    fn parse_callback() -> SlackPushEventCallback {
        serde_json::from_str(APP_MENTION_JSON).expect("a valid app_mention envelope")
    }

    #[test]
    fn maps_app_mention_to_mention_event() {
        let mention = mention_from_callback(&parse_callback()).expect("an app_mention maps");
        assert_eq!(mention.team_id, "T_OK");
        assert_eq!(mention.user, "U_OK");
        assert_eq!(mention.channel, "C1");
        assert_eq!(mention.message_ts, "1700000000.000100");
        assert_eq!(mention.text, "<@U07BOT> bench --block 184231");
    }

    #[derive(Default)]
    struct FakeSlackClient {
        reactions: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl SlackClient for FakeSlackClient {
        async fn post_ephemeral(&self, _c: &str, _u: &str, _t: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn post_blocks_in_thread(
            &self,
            _c: &str,
            _ts: &str,
            _b: &serde_json::Value,
            _f: &str,
        ) -> anyhow::Result<String> {
            Ok("ts".into())
        }
        async fn update_blocks(
            &self,
            _c: &str,
            _ts: &str,
            _b: &serde_json::Value,
            _f: &str,
        ) -> anyhow::Result<()> {
            Ok(())
        }
        async fn add_reaction(&self, _c: &str, _ts: &str, reaction: &str) -> anyhow::Result<()> {
            self.reactions
                .lock()
                .unwrap()
                .push(reaction.into());
            Ok(())
        }
        async fn remove_reaction(&self, _c: &str, _ts: &str, _r: &str) -> anyhow::Result<()> {
            Ok(())
        }
    }

    fn cfg() -> SlackConfig {
        SlackConfig {
            enabled: true,
            app_token: Some("xapp-x".into()),
            bot_token: Some("xoxb-x".into()),
            default_repository: "octo/core".into(),
            default_rev: "develop".into(),
            allowed_team_ids: vec!["T_OK".into()],
            allowed_user_ids: vec!["U_OK".into()],
        }
    }

    /// End-to-end (sans live socket): a parsed `app_mention` envelope maps to a
    /// `MentionEvent` and, dispatched through a real `SlackConnector`, enqueues
    /// exactly one ad-hoc job and reacts ⏳ — proving the inbound adapter +
    /// connector compose.
    #[tokio::test]
    async fn parsed_mention_dispatched_through_connector_enqueues_job() {
        let store = Arc::new(InMemoryJobStore::new());
        let slack = Arc::new(FakeSlackClient::default());
        let target = SlackJobTarget {
            installation_id: 100,
            repo_id: 10,
        };
        let connector = SlackConnector::new(cfg(), target, store.clone(), slack.clone());

        let mention = mention_from_callback(&parse_callback()).unwrap();
        connector
            .handle_mention(mention)
            .await;

        let jobs = store.all_jobs();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].trigger_kind, TriggerKind::SlackAdhoc);
        assert_eq!(
            *slack
                .reactions
                .lock()
                .unwrap(),
            vec![QUEUED_REACTION.to_string()],
        );
    }

    /// The ack-path fix: `spawn_dispatch` runs the orchestration off-thread
    /// (so the Socket Mode envelope is acked immediately) yet the job still
    /// enqueues. Proven by spawning, then yielding until the fire-and-forget
    /// task lands the job.
    #[tokio::test]
    async fn spawn_dispatch_enqueues_off_the_ack_path() {
        let store = Arc::new(InMemoryJobStore::new());
        let slack = Arc::new(FakeSlackClient::default());
        let target = SlackJobTarget {
            installation_id: 100,
            repo_id: 10,
        };
        let connector = Arc::new(SlackConnector::new(cfg(), target, store.clone(), slack));

        let mention = mention_from_callback(&parse_callback()).unwrap();
        spawn_dispatch(Some(connector), mention);
        // Returns immediately (the ack); the spawned task enqueues shortly after.
        // Yield until it lands (bounded, so a regression fails rather than hangs).
        for _ in 0..1000 {
            if !store.all_jobs().is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(store.all_jobs().len(), 1, "spawned dispatch must enqueue the job");
    }
}
