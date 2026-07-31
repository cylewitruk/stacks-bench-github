//! Socket Mode receive loop.
//!
//! `slack-morphism` owns the WebSocket transport — `apps.connections.open`, the
//! reconnect/backoff, and the envelope ack discipline. Our job is the thin
//! adapter on top: map each `app_mention` push into a [`MentionEvent`] and hand
//! it to the [`SlackConnector`] (which does authz → resolve → enqueue → react).
//! Outbound Web API calls go through
//! [`WebApiClient`](crate::WebApiClient) (reqwest), so the
//! only slack-morphism surface here is the inbound socket.
//!
//! The connector rides into slack-morphism's fn-pointer callbacks via its
//! per-listener **user state** (the callbacks can't capture environment).

use std::sync::Arc;

use slack_morphism::errors::SlackClientError;
use slack_morphism::prelude::*;
use tokio_util::sync::CancellationToken;

use crate::{
    MentionEvent, Result, SlackClient, SlackConnector, SlackConnectorConfig, SlackError,
    TaskSubmissionPort,
};

/// Socket credentials and connector policy projected by the daemon.
#[derive(Debug, Clone)]
pub struct SlackSocketConfig {
    pub app_token: Option<String>,
    pub connector: SlackConnectorConfig,
}

#[derive(Debug, Clone, Copy)]
pub struct SocketRunOptions {
    pub intent_rate_limit_per_minute: u32,
    pub max_clean_repetitions: u32,
    pub max_variants: u32,
    pub max_comparison_lifecycles: u32,
    pub binary_cache_enabled: bool,
}

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

/// The listener error handler. slack-morphism **auto-reconnects** after *any*
/// listener error (it drops the dead socket and dials a fresh one, looping
/// until shutdown), so this only chooses the log severity — it never affects
/// recovery. Slack recycles each WSS connection roughly every ~10 min and often
/// drops it without a close handshake (`ResetWithoutClosingHandshake`); that
/// transport churn is **routine** → `debug`. Anything else is unexpected →
/// `warn`. Without this handler the library default logs *every* such reset at
/// `error`, which inverts the real severity (and a persistent connect failure
/// only whispers at `trace`).
///
/// The returned status matters only to the HTTP Events API listener; it's
/// ignored on the Socket Mode path (we mirror the library default).
fn on_listener_error(
    err: Box<dyn std::error::Error + Send + Sync + 'static>,
    _client: Arc<SlackHyperClient>,
    _state: SlackClientEventsUserState,
) -> HttpStatusCode {
    if is_transient_socket_error(&*err) {
        tracing::debug!(error = %err, "slack: socket transport reset; slack-morphism is reconnecting");
    } else {
        tracing::warn!(error = %err, "slack: listener error; slack-morphism will attempt to reconnect");
    }
    HttpStatusCode::BAD_REQUEST
}

/// Whether a listener error is routine socket-transport churn (Slack's periodic
/// WSS recycle / abrupt reset) rather than something unexpected. slack-morphism
/// wraps a tungstenite read error as a `SocketModeProtocolError` whose message
/// is `"Slack WSS error: …"`, but **reuses that same variant** for genuine
/// anomalies (e.g. `"Unexpected binary received from Slack: …"`) — so match the
/// transport-error prefix, not just the variant, to leave anomalies at `warn`.
fn is_transient_socket_error(err: &(dyn std::error::Error + Send + Sync + 'static)) -> bool {
    let Some(SlackClientError::SocketModeProtocolError(e)) = err.downcast_ref::<SlackClientError>()
    else {
        return false;
    };
    e.message
        .starts_with("Slack WSS error:")
}

/// Run the Socket Mode receive loop until `shutdown` fires, then close the
/// socket cleanly. `app_token` (xapp-) opens the WS; `web_client` is the shared
/// Web API client (same bot token the reporter posts results with). Returns
/// `Err` only on a startup failure (bad/missing app token, TLS) — the caller
/// decides whether that is fatal (it isn't: Slack is an optional surface).
pub async fn run(
    cfg: SlackSocketConfig,
    jobs: Arc<dyn TaskSubmissionPort>,
    web_client: Arc<dyn SlackClient>,
    intent_resolver: Option<Arc<dyn sbgh_intent::IntentResolver>>,
    options: SocketRunOptions,
    shutdown: CancellationToken,
) -> Result<()> {
    let app_token_value = cfg
        .app_token
        .clone()
        .ok_or_else(|| {
            SlackError::Socket("[slack].enabled but SBGH_SLACK_APP_TOKEN is unset".into())
        })?;

    // The connector (authz/resolve/enqueue/react) shares the Web API client and
    // rides into the listener via user state.
    let mut connector = SlackConnector::new(cfg.connector, jobs, web_client)
        .with_max_clean_repetitions(options.max_clean_repetitions)
        .with_comparison_limits(options.max_variants, options.max_comparison_lifecycles)
        .with_binary_cache_enabled(options.binary_cache_enabled);
    if let Some(resolver) = intent_resolver {
        connector = connector.with_intent_resolver(resolver, options.intent_rate_limit_per_minute);
    }
    let connector = Arc::new(connector);

    let socket_connector =
        SlackClientHyperConnector::new().map_err(|error| SlackError::Socket(error.to_string()))?;
    let slack_client = Arc::new(SlackHyperClient::new(socket_connector));
    let environment = Arc::new(
        SlackClientEventsListenerEnvironment::new(slack_client)
            .with_error_handler(on_listener_error)
            .with_user_state(connector),
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
        .await
        .map_err(|error| SlackError::Socket(error.to_string()))?;
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
#[path = "socket_tests.rs"]
mod tests;
