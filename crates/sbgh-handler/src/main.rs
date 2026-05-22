mod routes;
mod state;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use axum::Router;
use sbgh_core::config::Config;
use sbgh_core::db::{self, PostgresJobStore};
use sbgh_core::github::{AppCredentials, InstallationTokenCache, OctocrabClient};
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::trace::TraceLayer;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, fmt};

use crate::state::AppState;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = dotenvy::dotenv();
    init_tracing();

    let config = Config::load().context("loading config")?;
    let pool = db::connect(&config.server.database_url)
        .await
        .context("connecting to postgres")?;
    db::migrate(&pool)
        .await
        .context("running migrations")?;

    let creds =
        AppCredentials::from_pem_file(config.github.app_id, &config.github.private_key_path)
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
