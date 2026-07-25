pub mod config;
mod routes;
mod signature;
mod state;

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::Context;
use axum::Router;
use sbgh_api::Client;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::trace::TraceLayer;

pub use crate::state::AppState;
pub use config::{HandlerApiConfig, HandlerConfig, ServerConfig, WebhookConfig};

/// Build the production handler router, including its request-size and tracing
/// middleware.
pub fn build_router(state: AppState) -> Router {
    Router::new()
        .merge(routes::router())
        .layer(RequestBodyLimitLayer::new(2 * 1024 * 1024))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

/// Compose and run the webhook handler from validated process configuration.
pub async fn run(config: HandlerConfig) -> anyhow::Result<()> {
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
    let app = build_router(AppState { config, api });

    tracing::info!(%bind_addr, "starting sbgh-handler");
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
