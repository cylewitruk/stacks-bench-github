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
    /// Diagnose the worker-owned fleet transport.
    Fleet {
        #[command(subcommand)]
        action: FleetAction,
    },
}

#[derive(Debug, Subcommand)]
enum FleetAction {
    /// Check gRPC health, or read-only registration readiness.
    Check {
        /// Stop after the standard gRPC health response; enrollment is not required.
        #[arg(long)]
        connectivity_only: bool,
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

impl Args {
    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !(self.preflight_only && self.command.is_some()),
            "--preflight-only cannot be combined with a subcommand"
        );
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(fmt::layer().json())
        .init();
    let args = Args::parse();
    args.validate()?;
    if let Some(Command::Identity { action }) = &args.command {
        let public = match action {
            IdentityAction::Generate { private_key } => {
                sbgh_worker::identity::generate(private_key)
            }
            IdentityAction::Public { private_key } => sbgh_worker::identity::public(private_key),
        }?;
        print!("{public}");
        return Ok(());
    }
    let config_path = args
        .config
        .context("--config is required when running the worker")?;
    let config =
        sbgh_worker::WorkerConfig::load(&config_path).context("loading worker configuration")?;
    if let Some(Command::Fleet {
        action: FleetAction::Check { connectivity_only },
    }) = args.command
    {
        if connectivity_only {
            sbgh_worker::check_connectivity(&config)
                .await
                .context("checking fleet connectivity")?;
            println!(
                "connectivity=ok endpoint={} transport=grpc+tls1.3 health=serving authorization=not_checked",
                config.orchestrator_url
            );
            return Ok(());
        }
        let resources =
            sbgh_worker::discover_host_resources().context("discovering worker host resources")?;
        log_host_resources(&resources);
        let report = sbgh_worker::check_registration(&config, resources)
            .await
            .context("checking fleet registration")?;
        let registration = report.registration;
        let effective_capabilities = registration
            .effective_capabilities
            .iter()
            .map(|capability| capability.as_str())
            .collect::<Vec<_>>()
            .join(",");
        println!(
            "connectivity=ok endpoint={} transport=grpc+tls1.3 registration=authorized worker_id={} protocol_version={} effective_capabilities={} measurement_profile={} draining={} clock_skew_ms={}",
            config.orchestrator_url,
            registration.worker_id,
            registration.protocol_version,
            effective_capabilities,
            registration
                .measurement_profile
                .as_deref()
                .unwrap_or("none"),
            registration.draining,
            report.clock_skew_ms,
        );
        return Ok(());
    }
    let resources =
        sbgh_worker::discover_host_resources().context("discovering worker host resources")?;
    log_host_resources(&resources);
    config
        .validate_host_resources(&resources)
        .context("validating worker profiles against host resources")?;
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

fn log_host_resources(resources: &sbgh_worker::HostResources) {
    tracing::info!(
        logical_cpus = resources.facts().logical_cpus,
        memory_bytes = resources.facts().memory_bytes,
        "discovered worker host resources"
    );
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::{Args, Command, FleetAction};

    #[test]
    fn fleet_check_supports_pre_enrollment_connectivity_mode() {
        let args = Args::try_parse_from([
            "sbgh-worker",
            "--config",
            "/etc/sbgh/worker/combined.toml",
            "fleet",
            "check",
            "--connectivity-only",
        ])
        .unwrap();
        assert!(matches!(
            args.command,
            Some(Command::Fleet {
                action: FleetAction::Check { connectivity_only: true }
            })
        ));
    }

    #[test]
    fn fleet_check_checks_registration_readiness_by_default() {
        let args = Args::try_parse_from([
            "sbgh-worker",
            "--config",
            "/etc/sbgh/worker/combined.toml",
            "fleet",
            "check",
        ])
        .unwrap();
        assert!(matches!(
            args.command,
            Some(Command::Fleet {
                action: FleetAction::Check { connectivity_only: false }
            })
        ));
    }

    #[test]
    fn preflight_only_conflicts_with_fleet_diagnostics() {
        let args = Args::try_parse_from([
            "sbgh-worker",
            "--config",
            "/etc/sbgh/worker/combined.toml",
            "--preflight-only",
            "fleet",
            "check",
        ])
        .unwrap();
        assert!(args.validate().is_err());
    }
}
