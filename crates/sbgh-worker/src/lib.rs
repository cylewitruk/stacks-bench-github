//! In-process worker execution application.

mod bench_recipe;
pub mod binary_cache;
mod block_validation_recipe;
mod build_recipe;
mod config;
mod events;
mod execution;
mod fleet;
mod host_resources;
pub mod identity;
mod recipe;
mod remote_artifacts;
mod transport;

use std::sync::Arc;

use sbgh_driver::{ArtifactSink, CacheControl};
use sbgh_libvirt::{LibvirtConfig, LibvirtDriver, Shell};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub use binary_cache::{BinaryCache, BinaryCacheConfig, build_binary_cache};
pub use config::WorkerConfig;
pub use execution::execute;
pub use fleet::preflight_local_execution;
pub use fleet::run as run_fleet;
pub use host_resources::discover_host_resources;
pub use sbgh_driver::{
    BenchmarkRunContext, BenchmarkTask, ExecutionContext, ExecutionPlacement, ExecutionRequest,
    ExecutionTask, Terminal, WorkerEvent,
};

use crate::execution::ExecutionDependencies;

/// The in-process execution owner. v25 can place this unchanged behind a
/// transport process.
pub struct WorkerRuntime {
    driver: Arc<dyn sbgh_driver::Driver>,
}

/// A composed worker plus the separately-owned cache-control handle used by
/// orchestrator-side pin management.
pub struct BuiltWorker {
    pub runtime: Arc<WorkerRuntime>,
    pub cache_control: Option<Arc<dyn CacheControl>>,
}

impl WorkerRuntime {
    pub fn libvirt(
        config: LibvirtConfig,
        shell: Arc<dyn Shell>,
        artifacts: Arc<dyn ArtifactSink>,
        cache: Option<Arc<BinaryCache>>,
    ) -> BuiltWorker {
        let cache_store = cache
            .as_ref()
            .map(|cache| cache.clone() as Arc<dyn sbgh_driver::BinaryCacheStore>);
        let cache_control = cache.map(|cache| cache as Arc<dyn CacheControl>);
        let driver: Arc<dyn sbgh_driver::Driver> =
            Arc::new(LibvirtDriver::new(config, shell, artifacts, cache_store));
        BuiltWorker {
            runtime: Arc::new(Self { driver }),
            cache_control,
        }
    }

    pub fn with_driver(driver: Arc<dyn sbgh_driver::Driver>) -> Arc<Self> {
        Arc::new(Self { driver })
    }

    pub async fn run(
        &self,
        request: ExecutionRequest,
        events_tx: mpsc::Sender<WorkerEvent>,
        token: CancellationToken,
    ) -> bool {
        events::run_execution(
            request,
            ExecutionDependencies { driver: self.driver.clone() },
            events_tx,
            token,
        )
        .await
    }

    pub async fn cleanup_by_job_id(&self, job_id: &str) -> bool {
        self.driver
            .cleanup_by_job_id(job_id)
            .await
    }

    pub async fn cleanup_attempt(&self, job_id: &str, attempt_id: &str) -> bool {
        self.driver
            .cleanup_attempt(job_id, attempt_id)
            .await
    }
}
