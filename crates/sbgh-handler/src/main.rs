mod routes;
mod state;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use axum::Router;
use clap::Parser;
use sbgh_core::config::Config;
use sbgh_core::db::{self, PostgresJobStore};
use sbgh_core::github::{AppCredentials, InstallationTokenCache, OctocrabClient};
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::trace::TraceLayer;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, fmt};

use crate::state::AppState;

#[derive(Parser, Debug)]
#[command(version, about = "stacks-bench GitHub App webhook handler")]
struct Args {
    /// Path to an env file to source secrets from (e.g.
    /// `~/.config/sbgh/secrets.env`). If omitted, `./.env` in the working
    /// directory is loaded best-effort. When this flag IS supplied, a
    /// missing or unreadable file is a fatal error rather than a silent
    /// miss.
    #[arg(long, value_name = "PATH")]
    env_file: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    load_env(args.env_file.as_deref())?;
    init_tracing();

    let config = Config::load().context("loading config")?;
    let pool = db::connect(&config.server.database_url)
        .await
        .context("connecting to postgres")?;
    db::migrate(&pool)
        .await
        .context("running migrations")?;

    let creds =
        AppCredentials::from_pem_file(&config.github.client_id, &config.github.private_key_path)
            .context("loading github app private key")?;
    let tokens = InstallationTokenCache::new(
        creds,
        config
            .github
            .api_base_url
            .clone(),
    );
    let gh = Arc::new(OctocrabClient::new(tokens));
    let jobs = Arc::new(PostgresJobStore::new(pool));

    let bind_addr: SocketAddr = config
        .server
        .bind_addr
        .parse()
        .context("parsing server.bind_addr")?;

    let state = AppState { config, jobs, gh };

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

/// Populate `std::env` from a dotenv file. `dotenvy` does not overwrite
/// vars already set in the process environment, so shell-exported values
/// always win over both an explicit `--env-file` and the implicit `./.env`.
fn load_env(explicit: Option<&Path>) -> anyhow::Result<()> {
    match explicit {
        Some(path) => {
            dotenvy::from_path(path)
                .with_context(|| format!("loading env file from {}", path.display()))?;
        }
        None => {
            // Best-effort: a missing `.env` is fine, just means the caller
            // sourced their secrets through the shell or some other mechanism.
            let _ = dotenvy::dotenv();
        }
    }
    Ok(())
}
