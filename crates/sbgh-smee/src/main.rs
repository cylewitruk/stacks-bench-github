//! `sbgh-smee` — a Rust port of [smee-client](https://github.com/probot/smee-client),
//! used for tunneling GitHub webhook deliveries from a smee.io channel down to
//! a locally-running `sbgh-handler` during development.
//!
//! The protocol is dirt-simple: open an SSE stream to the smee channel; for
//! each `message` event the server hands us a JSON object whose keys are HTTP
//! header names (plus `body`, `query`, `timestamp`), and we POST the body to
//! the target with the original headers reconstructed — minus hop-by-hop and
//! framing headers that don't belong on the second hop. See [`forward`] for
//! the exact rules.

mod forward;

use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use futures::StreamExt;
use reqwest_eventsource::{Error as EventSourceError, Event, EventSource};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::prelude::*;

#[derive(Parser, Debug)]
#[command(version, about = "Forward webhook deliveries from a smee.io channel to a local URL")]
struct Args {
    /// Smee channel URL, e.g. https://smee.io/abc
    #[arg(long)]
    channel: String,

    /// Local target URL.
    #[arg(long, default_value = "http://localhost:8080/webhook")]
    target: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Pin the rustls provider before any TLS (the workspace tree carries both
    // `ring` and `aws-lc-rs`, so the default is otherwise ambiguous).
    let _ = rustls::crypto::ring::default_provider().install_default();
    init_tracing();
    let args = Args::parse();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    let sse_client = build_sse_client()?;

    info!(
        channel_host = %channel_host(&args.channel),
        target = %args.target,
        "sbgh-smee started"
    );

    let mut es = EventSource::new(sse_client.get(&args.channel))?;
    while let Some(event) = es.next().await {
        match event {
            Ok(Event::Open) => info!("connected to smee channel"),
            Ok(Event::Message(msg)) if msg.event == "ready" || msg.event == "ping" => {
                tracing::debug!(event = %msg.event, "control event");
            }
            Ok(Event::Message(msg)) => {
                match forward::forward(&client, &args.target, &msg.data).await {
                    Ok(outcome) => {
                        let m = &outcome.meta;
                        let status = outcome.status;
                        let delivery = m
                            .delivery
                            .as_deref()
                            .unwrap_or("-");
                        let event = m
                            .event
                            .as_deref()
                            .unwrap_or("-");
                        let hook_id = m
                            .hook_id
                            .as_deref()
                            .unwrap_or("-");
                        let hook_target_id = m
                            .hook_target_id
                            .as_deref()
                            .unwrap_or("-");
                        let hook_target_type = m
                            .hook_target_type
                            .as_deref()
                            .unwrap_or("-");
                        if status.is_success() {
                            info!(
                                %status,
                                delivery,
                                event,
                                hook_id,
                                hook_target_id,
                                hook_target_type,
                                "forwarded delivery"
                            );
                        } else {
                            // Forwarded, but the handler rejected it (bad
                            // signature, missing delivery id, ingest error).
                            warn!(
                                %status,
                                delivery,
                                event,
                                hook_id,
                                hook_target_id,
                                hook_target_type,
                                "forwarded delivery rejected by target (non-success status)"
                            );
                        }
                    }
                    // Couldn't parse the smee payload or reach the target —
                    // nothing was delivered.
                    Err(e) => warn!(error = %e, "forward failed (parse or transport error)"),
                }
            }
            Err(error) => {
                warn!(
                    error = %describe_sse_error(error),
                    "sse error; auto-reconnecting"
                );
            }
        }
    }
    Ok(())
}

fn build_sse_client() -> reqwest_sse::Result<reqwest_sse::Client> {
    // This client must not have a whole-request timeout: an SSE response is
    // intentionally long-lived. Bound only connection establishment.
    reqwest_sse::Client::builder()
        .connect_timeout(Duration::from_secs(30))
        .https_only(true)
        .build()
}

fn channel_host(channel: &str) -> String {
    reqwest_sse::Url::parse(channel)
        .ok()
        .and_then(|url| url.host_str().map(ToOwned::to_owned))
        .unwrap_or_else(|| "<invalid>".to_owned())
}

fn describe_sse_error(error: EventSourceError) -> String {
    match error {
        // Reqwest normally includes the request URL in Display/Debug output.
        // The Smee channel path is a bearer credential, so strip it while
        // retaining the nested transport cause needed for operations.
        EventSourceError::Transport(error) => {
            format!("transport: {:?}", error.without_url())
        }
        EventSourceError::InvalidContentType(content_type, _) => {
            format!("unexpected content type: {content_type:?}")
        }
        EventSourceError::InvalidStatusCode(status, _) => {
            format!("unexpected HTTP status: {status}")
        }
        EventSourceError::Utf8(_) => "SSE response was not valid UTF-8".to_owned(),
        EventSourceError::Parser(_) => "SSE response could not be parsed".to_owned(),
        EventSourceError::InvalidLastEventId(_) => "SSE event ID was invalid".to_owned(),
        EventSourceError::StreamEnded => "SSE stream ended".to_owned(),
    }
}

fn init_tracing() {
    tracing_subscriber::registry()
        .with(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,sbgh_smee=debug")),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_log_field_excludes_bearer_path() {
        assert_eq!(channel_host("https://smee.io/super-secret?token=also-secret"), "smee.io");
        assert_eq!(channel_host("not a URL"), "<invalid>");
    }

    #[tokio::test]
    async fn sse_client_has_https_transport_and_redacts_request_url() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral loopback listener");
        let address = listener.local_addr().expect("read listener address");
        drop(listener);

        let secret = "super-secret-channel";
        let error = build_sse_client()
            .expect("build SSE client")
            .get(format!("https://{address}/{secret}"))
            .send()
            .await
            .expect_err("closed loopback port should reject the connection");

        assert!(error.is_connect(), "HTTPS should reach the connector: {error:?}");
        let description = describe_sse_error(EventSourceError::Transport(error));
        assert!(description.starts_with("transport:"));
        assert!(!description.contains(secret), "channel credential leaked: {description}");
    }
}
