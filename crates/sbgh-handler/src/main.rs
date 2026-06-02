mod routes;
mod state;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Context;
use axum::Router;
use clap::Parser;
use sbgh_api::Client;
use sbgh_core::config::HandlerConfig;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::trace::TraceLayer;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, fmt};

use crate::state::AppState;

#[derive(Parser, Debug)]
#[command(version, about = "stacks-bench GitHub App webhook handler")]
struct Args {
    /// Path to an env file to source secrets from. If omitted, `./.env` in the
    /// working directory is loaded best-effort. When this flag IS supplied, a
    /// missing or unreadable file is a fatal error rather than a silent miss.
    #[arg(long, value_name = "PATH")]
    env_file: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    load_env(args.env_file.as_deref())?;
    init_tracing();

    let config = HandlerConfig::load().context("loading config")?;
    // No DB: verified deliveries are forwarded to the daemon `/api`
    // with the `ingest`-scope token. The daemon owns all persistence. Pin a
    // short timeout — this is a web-facing path and a stalled daemon must
    // not tie up handler request capacity.
    let api = Client::with_timeout(
        config.api.url.clone(),
        Some(
            config
                .api
                .ingest_token
                .clone(),
        ),
        Duration::from_secs(10),
    );

    let bind_addr: SocketAddr = config
        .server
        .bind_addr
        .parse()
        .context("parsing server.bind_addr")?;

    let state = AppState { config, api };

    let app = Router::new()
        .merge(routes::router())
        .layer(RequestBodyLimitLayer::new(2 * 1024 * 1024))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    tracing::info!(%bind_addr, "starting sbgh-handler");
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

fn init_tracing() {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(fmt::layer())
        .init();
}

fn load_env(explicit: Option<&Path>) -> anyhow::Result<()> {
    match explicit {
        Some(path) => {
            dotenvy::from_path(path)
                .with_context(|| format!("loading env file from {}", path.display()))?;
        }
        None => {
            let _ = dotenvy::dotenv();
        }
    }
    Ok(())
}
