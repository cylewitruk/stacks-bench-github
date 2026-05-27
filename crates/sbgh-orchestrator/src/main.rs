mod bench_summary;
mod libvirt;
mod progress;
mod runner;
mod webhook_processor;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use sbgh_core::config::OrchestratorConfig;
use sbgh_core::db::{self, PostgresJobStore, PostgresWebhookInbox};
use sbgh_core::github::{AppCredentials, InstallationTokenCache, OctocrabClient};
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, fmt};

use crate::libvirt::SystemShell;
use crate::runner::Runner;
use crate::webhook_processor::{BasicClassifier, ProcessorConfig, WebhookProcessor};

#[derive(Parser, Debug)]
#[command(version, about = "stacks-bench job orchestrator (libvirt benchmark runner)")]
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

    let config = OrchestratorConfig::load().context("loading config")?;
    // Connect using the narrow `sbgh_orch` role — SELECT + UPDATE on jobs.
    // Schema setup runs in the separate `sbgh-migrate` binary as the DB
    // owner, never here.
    let pool = db::connect(&config.server.database_url).await?;

    let creds =
        AppCredentials::from_pem_file(&config.github.client_id, &config.github.private_key_path)?;
    let tokens = InstallationTokenCache::new(
        creds,
        config
            .github
            .api_base_url
            .clone(),
    );
    let gh = Arc::new(OctocrabClient::new(tokens));
    let jobs = Arc::new(PostgresJobStore::new(pool.clone()));
    let shell = Arc::new(SystemShell::new(&config.paths.sudo_binary));

    // Slice 2b: webhook processor runs concurrently with the legacy
    // job runner. Both loop indefinitely; if either returns Err the
    // orchestrator crashes and systemd restarts it.
    let webhook_inbox = Arc::new(PostgresWebhookInbox::new(pool));
    let processor =
        WebhookProcessor::new(webhook_inbox, Arc::new(BasicClassifier), ProcessorConfig::default());

    let runner = Runner::new(config, jobs, gh, shell);

    tokio::try_join!(runner.run(), async {
        processor
            .run()
            .await
            .map_err(anyhow::Error::from)
    })?;
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
