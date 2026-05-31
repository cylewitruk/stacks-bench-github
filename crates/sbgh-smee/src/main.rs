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
use reqwest_eventsource::{Event, EventSource};
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
    init_tracing();
    let args = Args::parse();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;

    info!(channel = %args.channel, target = %args.target, "sbgh-smee started");

    let mut es = EventSource::get(&args.channel);
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
            Err(e) => warn!(error = %e, "sse error; auto-reconnecting"),
        }
    }
    Ok(())
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
