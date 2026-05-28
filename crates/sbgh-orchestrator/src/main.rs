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
use sbgh_core::db::{
    self, PostgresInstallationStore, PostgresJobStore, PostgresPolicyStore,
    PostgresPullRequestStore, PostgresRepoStore, PostgresUserStore, PostgresWebhookInbox,
};
use sbgh_core::github::{AppCredentials, InstallationTokenCache, OctocrabClient};
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, fmt};

use crate::libvirt::SystemShell;
use crate::runner::Runner;
use crate::webhook_processor::{
    BasicClassifier, CreateHandler, InstallationHandler, InstallationRepositoriesHandler,
    IssueCommentHandler, ProcessorConfig, PullRequestHandler, PushHandler, WebhookProcessor,
};

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
    // Schema setup runs in the separate `sbgh-cli migrate` binary as the DB
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
    //
    // The classifier is composed from per-event-type EventHandlers
    // (slice 3 router refactor). Each slice 3-7 adds more handlers
    // here as they ship — the set of registered handlers is what
    // determines which event types the processor will claim from the
    // inbox (others stay `received` for a future slice).
    let webhook_inbox = Arc::new(PostgresWebhookInbox::new(pool.clone()));
    let installation_store = Arc::new(PostgresInstallationStore::new(pool.clone()));
    let repo_store = Arc::new(PostgresRepoStore::new(pool.clone()));
    let policy_store = Arc::new(PostgresPolicyStore::new(pool.clone()));
    let user_store = Arc::new(PostgresUserStore::new(pool.clone()));
    let pull_request_store = Arc::new(PostgresPullRequestStore::new(pool));
    let classifier = BasicClassifier::builder()
        .with_handler(Arc::new(IssueCommentHandler::new(
            repo_store.clone(),
            policy_store.clone(),
            installation_store.clone(),
            user_store.clone(),
            pull_request_store.clone(),
            gh.clone(),
        )))
        .with_handler(Arc::new(InstallationHandler::new(
            installation_store.clone(),
            repo_store.clone(),
            gh.clone(),
        )))
        .with_handler(Arc::new(InstallationRepositoriesHandler::new(
            repo_store.clone(),
            installation_store.clone(),
            policy_store.clone(),
            user_store.clone(),
            gh.clone(),
        )))
        .with_handler(Arc::new(PullRequestHandler::new(
            repo_store,
            policy_store.clone(),
            installation_store.clone(),
            user_store,
            pull_request_store,
        )))
        .with_handler(Arc::new(PushHandler::new(policy_store.clone(), installation_store.clone())))
        .with_handler(Arc::new(CreateHandler::new(policy_store, installation_store)))
        .build();
    let processor =
        WebhookProcessor::new(webhook_inbox, Arc::new(classifier), ProcessorConfig::default());

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
