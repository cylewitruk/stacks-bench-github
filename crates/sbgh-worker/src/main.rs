use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Debug, Parser)]
#[command(version, about = "stacks-bench fleet worker")]
struct Args {
    #[arg(long)]
    config: PathBuf,
    /// Validate the local sandbox and immutable chainstate origin without
    /// connecting to the orchestrator.
    #[arg(long)]
    preflight_only: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(fmt::layer().json())
        .init();
    let args = Args::parse();
    let config =
        sbgh_worker::WorkerConfig::load(&args.config).context("loading worker configuration")?;
    let resources =
        sbgh_worker::discover_host_resources().context("discovering worker host resources")?;
    config
        .validate_host_resources(&resources)
        .context("validating worker profiles against host resources")?;
    tracing::info!(
        logical_cpus = resources.logical_cpus,
        memory_bytes = resources.memory_bytes,
        "discovered worker host resources"
    );
    if args.preflight_only {
        sbgh_worker::preflight_local_execution(&config)
            .await
            .context("preflighting local worker execution")?;
        tracing::info!("local worker execution preflight passed");
        return Ok(());
    }
    let shutdown = CancellationToken::new();
    let signal = shutdown.clone();
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let mut interrupt =
                signal(SignalKind::interrupt()).expect("installing worker SIGINT handler");
            let mut terminate =
                signal(SignalKind::terminate()).expect("installing worker SIGTERM handler");
            tokio::select! {
                _ = interrupt.recv() => {}
                _ = terminate.recv() => {}
            }
        }
        #[cfg(not(unix))]
        let _ = tokio::signal::ctrl_c().await;
        signal.cancel();
    });
    sbgh_worker::run_fleet(config, resources, shutdown).await
}
