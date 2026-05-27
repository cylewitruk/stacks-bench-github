mod routes;
mod state;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use axum::Router;
use clap::Parser;
use sbgh_core::config::HandlerConfig;
use sbgh_core::db::{self, PostgresIngestStore};
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
    // Connect using the narrow `sbgh_handler` role — INSERT-only. The
    // database schema is established by `sbgh-cli migrate` (running as the
    // owner role) before this binary starts.
    let pool = db::connect(&config.server.database_url)
        .await
        .context("connecting to postgres")?;
    let ingest = Arc::new(PostgresIngestStore::new(pool));

    let bind_addr: SocketAddr = config
        .server
        .bind_addr
        .parse()
        .context("parsing server.bind_addr")?;

    let state = AppState { config, ingest };

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
