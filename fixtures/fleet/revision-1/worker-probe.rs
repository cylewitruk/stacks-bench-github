//! Test launcher linked against the unmodified pinned worker library.
//!
//! Host resources are injected because the compatibility runner may not be Linux.
//! Transport, registration, polling, recovery and shutdown use the worker itself.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let path = PathBuf::from(
        std::env::args()
            .nth(1)
            .context("usage: sbgh-previous-worker-probe CONFIG")?,
    );
    let config = sbgh_worker::WorkerConfig::load(&path)?;
    let resources = sbgh_worker::HostResources::new((0..8).collect(), 32 * 1024 * 1024 * 1024)?;
    sbgh_worker::check_registration(&config, resources.clone()).await?;
    tokio::time::timeout(
        Duration::from_secs(60),
        sbgh_worker::run_fleet(config, resources, CancellationToken::new()),
    )
    .await
    .context("compatibility worker exceeded its test deadline")?
}
