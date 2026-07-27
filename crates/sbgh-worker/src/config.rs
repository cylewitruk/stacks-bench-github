use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use anyhow::{Context, ensure};
use sbgh_libvirt::LibvirtConfig;
use sbgh_proto::{DatasetIdentity, ResourceFacts, WorkerCapability};
use serde::Deserialize;
use uuid::Uuid;

use crate::BinaryCacheConfig;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerConfig {
    pub worker_id: Uuid,
    pub orchestrator_url: String,
    pub client_certificate: PathBuf,
    pub client_private_key: PathBuf,
    pub server_ca_certificate: PathBuf,
    pub capabilities: BTreeSet<WorkerCapability>,
    pub resources: ResourceFacts,
    pub libvirt: Option<LibvirtConfig>,
    pub binary_cache: Option<BinaryCacheConfig>,
}

impl WorkerConfig {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading worker configuration {}", path.display()))?;
        let config: Self = toml::from_str(&text)
            .with_context(|| format!("parsing worker configuration {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(!self.worker_id.is_nil(), "worker_id must not be nil");
        let url = reqwest::Url::parse(&self.orchestrator_url)
            .context("orchestrator_url is not a valid URL")?;
        ensure!(url.scheme() == "https", "orchestrator_url must use HTTPS");
        ensure!(
            url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none()
                && matches!(url.path(), "" | "/"),
            "orchestrator_url must be an HTTPS origin without credentials, query, fragment, or path"
        );
        ensure!(
            self.capabilities
                == self
                    .capabilities
                    .iter()
                    .copied()
                    .collect(),
            "capability set is invalid"
        );
        ensure!(!self.capabilities.is_empty(), "at least one capability is required");
        ensure!(
            self.resources.logical_cpus > 0 && self.resources.memory_bytes > 0,
            "resource facts require non-zero CPU and memory"
        );
        if self
            .capabilities
            .iter()
            .any(|capability| {
                matches!(capability, WorkerCapability::Benchmark | WorkerCapability::BuildOnly)
            })
        {
            ensure!(self.libvirt.is_some(), "benchmark/build capability requires [libvirt]");
        }
        if self
            .capabilities
            .contains(&WorkerCapability::BlockValidation)
        {
            let libvirt = self
                .libvirt
                .as_ref()
                .context("block_validation capability requires [libvirt]")?;
            let block = libvirt
                .block_validation
                .as_ref()
                .context("block_validation capability requires [libvirt.block_validation]")?;
            let dataset = self
                .resources
                .dataset
                .as_ref()
                .context("block_validation capability requires resources.dataset")?;
            validate_dataset(dataset)?;
            for (name, path) in [
                ("chain_config", &block.chain_config),
                ("dataset.manifest_path", &block.dataset.manifest_path),
            ] {
                ensure!(path.is_absolute(), "{name} must be an absolute path");
            }
            ensure!(
                block.vcpus > 0
                    && block.vcpus <= self.resources.logical_cpus
                    && block.memory_bytes > 0
                    && u64::from(block.results_tmpfs_mib)
                        .checked_mul(1024 * 1024)
                        .and_then(|tmpfs| block
                            .memory_bytes
                            .checked_add(tmpfs))
                        .is_some_and(|reserved| reserved <= self.resources.memory_bytes),
                "block-validation profile exceeds registered CPU or memory"
            );
            ensure!(
                block.max_shards > 0
                    && block.max_concurrency > 0
                    && block.max_concurrency <= block.max_shards,
                "invalid block-validation shard/concurrency limits"
            );
            ensure!(
                block.dataset.generation == dataset.generation
                    && block.dataset.network == dataset.network
                    && block.dataset.format_version == dataset.format_version
                    && block.dataset.covered_start == dataset.covered_start
                    && block.dataset.covered_end == dataset.covered_end
                    && block
                        .dataset
                        .manifest_sha256
                        .eq_ignore_ascii_case(&dataset.manifest_sha256),
                "libvirt block dataset must exactly match resources.dataset"
            );
        }
        Ok(())
    }
}

fn validate_dataset(dataset: &DatasetIdentity) -> anyhow::Result<()> {
    use sbgh_proto::Validate;
    dataset
        .validate()
        .map_err(anyhow::Error::new)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_in_worker_examples_stay_parseable() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        for name in
            ["config.example.worker-benchmark.toml", "config.example.worker-block-validation.toml"]
        {
            WorkerConfig::load(&root.join(name)).unwrap();
        }
    }

    #[test]
    fn worker_url_rejects_ambient_credentials_and_path_confusion() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut config =
            WorkerConfig::load(&root.join("config.example.worker-benchmark.toml")).unwrap();
        for invalid in [
            "https://user@fleet.example",
            "https://fleet.example/v1",
            "https://fleet.example?target=other",
            "http://fleet.example",
        ] {
            config.orchestrator_url = invalid.into();
            assert!(config.validate().is_err(), "{invalid} unexpectedly passed");
        }
    }
}
