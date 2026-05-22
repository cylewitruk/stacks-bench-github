mod libvirt;
mod progress;
mod runner;

use std::sync::Arc;

use anyhow::Context;
use sbgh_core::config::Config;
use sbgh_core::db::{self, PostgresJobStore};
use sbgh_core::github::{AppCredentials, InstallationTokenCache, OctocrabClient};
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, fmt};

use crate::libvirt::SystemShell;
use crate::runner::Runner;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = dotenvy::dotenv();
    init_tracing();

    let config = Config::load().context("loading config")?;
    let pool = db::connect(&config.server.database_url).await?;
    db::migrate(&pool)
        .await
        .context("running migrations")?;

    let creds =
        AppCredentials::from_pem_file(config.github.app_id, &config.github.private_key_path)?;
    let tokens = InstallationTokenCache::new(
        creds,
        config
            .github
            .api_base_url
            .clone(),
    );
    let gh = Arc::new(OctocrabClient::new(tokens));
    let jobs = Arc::new(PostgresJobStore::new(pool));
    let shell = Arc::new(SystemShell::new(&config.paths.sudo_binary));

    let runner = Runner::new(config, jobs, gh, shell);
    runner.run().await?;
    Ok(())
}

fn init_tracing() {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(fmt::layer())
        .init();
}
