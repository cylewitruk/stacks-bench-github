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
    pub block_validation: Option<BlockValidationConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlockValidationConfig {
    pub canonical_dataset: PathBuf,
    pub workspace_root: PathBuf,
    pub source_cache: PathBuf,
    pub binary_cache: PathBuf,
    pub chain_config: PathBuf,
    #[serde(default = "default_git")]
    pub git_binary: PathBuf,
    #[serde(default = "default_cargo")]
    pub cargo_binary: PathBuf,
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
            let block = self
                .block_validation
                .as_ref()
                .context("block_validation capability requires [block_validation]")?;
            let dataset = self
                .resources
                .dataset
                .as_ref()
                .context("block_validation capability requires resources.dataset")?;
            validate_dataset(dataset)?;
            for (name, path) in [
                ("canonical_dataset", &block.canonical_dataset),
                ("workspace_root", &block.workspace_root),
                ("source_cache", &block.source_cache),
                ("binary_cache", &block.binary_cache),
                ("chain_config", &block.chain_config),
                ("git_binary", &block.git_binary),
                ("cargo_binary", &block.cargo_binary),
            ] {
                ensure!(path.is_absolute(), "{name} must be an absolute path");
            }
            ensure!(
                block.canonical_dataset != block.workspace_root
                    && !block
                        .canonical_dataset
                        .starts_with(&block.workspace_root)
                    && !block
                        .workspace_root
                        .starts_with(&block.canonical_dataset),
                "canonical_dataset and workspace_root must be disjoint"
            );
            ensure!(
                block.source_cache != block.binary_cache,
                "source_cache and binary_cache must be distinct"
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

fn default_git() -> PathBuf {
    "/usr/bin/git".into()
}

fn default_cargo() -> PathBuf {
    "/usr/bin/cargo".into()
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
