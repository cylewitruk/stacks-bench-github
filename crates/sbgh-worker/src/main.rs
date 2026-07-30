use std::path::PathBuf;

use anyhow::Context;
use clap::{Parser, Subcommand};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Debug, Parser)]
#[command(version, about = "stacks-bench fleet worker")]
struct Args {
    #[arg(long)]
    config: Option<PathBuf>,
    /// Validate the local sandbox and immutable chainstate origin without
    /// connecting to the orchestrator.
    #[arg(long)]
    preflight_only: bool,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Manage the worker's local identity key.
    Identity {
        #[command(subcommand)]
        action: IdentityAction,
    },
}

#[derive(Debug, Subcommand)]
enum IdentityAction {
    /// Create a new P-256 PKCS#8 key and print its public SPKI.
    Generate {
        #[arg(long)]
        private_key: PathBuf,
    },
    /// Print the public SPKI for an existing private key.
    Public {
        #[arg(long)]
        private_key: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(fmt::layer().json())
        .init();
    let args = Args::parse();
    if let Some(Command::Identity { action }) = args.command {
        let public = match action {
            IdentityAction::Generate { private_key } => {
                sbgh_worker::identity::generate(&private_key)
            }
            IdentityAction::Public { private_key } => sbgh_worker::identity::public(&private_key),
        }?;
        print!("{public}");
        return Ok(());
    }
    let config_path = args
        .config
        .context("--config is required when running the worker")?;
    let config =
        sbgh_worker::WorkerConfig::load(&config_path).context("loading worker configuration")?;
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
