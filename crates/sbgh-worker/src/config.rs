use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use anyhow::{Context, ensure};
use sbgh_fleet::{ResourceFacts, WorkerCapability};
use sbgh_libvirt::LibvirtConfig;
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
        if let Some(libvirt) = &self.libvirt {
            ensure!(
                libvirt.vm.network == "sandbox-egress",
                "execution workers must use the policy-managed sandbox-egress network"
            );
        }
        if self
            .capabilities
            .iter()
            .any(|capability| {
                matches!(capability, WorkerCapability::Benchmark | WorkerCapability::BuildOnly)
            })
        {
            let libvirt = self
                .libvirt
                .as_ref()
                .context("benchmark/build capability requires [libvirt]")?;
            ensure!(
                libvirt.benchmark.build_vcpus > 0
                    && libvirt
                        .benchmark
                        .build_memory_bytes
                        > 0
                    && libvirt
                        .benchmark
                        .job_timeout_secs
                        > 0,
                "benchmark/build capability requires non-zero build resources"
            );
            if self
                .capabilities
                .contains(&WorkerCapability::Benchmark)
            {
                ensure!(
                    libvirt.benchmark.bench_vcpus > 0
                        && libvirt
                            .benchmark
                            .bench_memory_bytes
                            > 0,
                    "benchmark capability requires non-zero benchmark resources"
                );
            }
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
            ensure!(
                block
                    .chain_config
                    .is_absolute(),
                "chain_config must be an absolute path"
            );
            ensure!(
                block.vcpus > 0
                    && block.memory_bytes > 0
                    && u64::from(block.results_tmpfs_mib)
                        .checked_mul(1024 * 1024)
                        .and_then(|tmpfs| block
                            .memory_bytes
                            .checked_add(tmpfs))
                        .is_some(),
                "block-validation profile has invalid CPU or memory"
            );
            ensure!(
                block.max_shards > 0
                    && block.max_concurrency > 0
                    && block.max_concurrency <= block.max_shards,
                "invalid block-validation shard/concurrency limits"
            );
        }
        Ok(())
    }

    /// Validate operator-selected guest profiles against measured host
    /// capacity. Build and execution VMs are sequential within an assignment,
    /// so each phase is checked independently rather than summed.
    pub fn validate_host_resources(&self, resources: &ResourceFacts) -> anyhow::Result<()> {
        ensure!(
            resources.logical_cpus > 0 && resources.memory_bytes > 0,
            "discovered host resources require non-zero CPU and memory"
        );
        if self
            .capabilities
            .iter()
            .any(|capability| {
                matches!(capability, WorkerCapability::Benchmark | WorkerCapability::BuildOnly)
            })
        {
            let benchmark = &self
                .libvirt
                .as_ref()
                .context("benchmark/build capability requires [libvirt]")?
                .benchmark;
            ensure!(
                benchmark.build_vcpus <= resources.logical_cpus
                    && benchmark.build_memory_bytes <= resources.memory_bytes,
                "benchmark build profile exceeds discovered host CPU or memory"
            );
            if self
                .capabilities
                .contains(&WorkerCapability::Benchmark)
            {
                ensure!(
                    benchmark.bench_vcpus <= resources.logical_cpus
                        && benchmark.bench_memory_bytes <= resources.memory_bytes,
                    "benchmark execution profile exceeds discovered host CPU or memory"
                );
            }
        }
        if self
            .capabilities
            .contains(&WorkerCapability::BlockValidation)
        {
            let block = self
                .libvirt
                .as_ref()
                .and_then(|libvirt| {
                    libvirt
                        .block_validation
                        .as_ref()
                })
                .context("block_validation capability requires [libvirt.block_validation]")?;
            let reserved_memory = block
                .memory_bytes
                .checked_add(u64::from(block.results_tmpfs_mib) * 1024 * 1024)
                .context("block-validation memory reservation overflows")?;
            ensure!(
                block.vcpus <= resources.logical_cpus && reserved_memory <= resources.memory_bytes,
                "block-validation profile exceeds discovered host CPU or memory"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_in_worker_examples_stay_parseable() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let benchmark =
            WorkerConfig::load(&root.join("config.example.worker-benchmark.toml")).unwrap();
        let block =
            WorkerConfig::load(&root.join("config.example.worker-block-validation.toml")).unwrap();

        assert_eq!(
            benchmark
                .libvirt
                .as_ref()
                .unwrap()
                .vm
                .network,
            "sandbox-egress"
        );
        assert_eq!(
            benchmark
                .libvirt
                .as_ref()
                .unwrap()
                .vm
                .network,
            block
                .libvirt
                .as_ref()
                .unwrap()
                .vm
                .network
        );
    }

    #[test]
    fn static_host_resource_stanza_is_rejected() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let text =
            std::fs::read_to_string(root.join("config.example.worker-benchmark.toml")).unwrap();
        let stale = format!("{text}\n[resources]\nlogical_cpus = 8\nmemory_bytes = 34359738368\n");

        let error = toml::from_str::<WorkerConfig>(&stale).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("unknown field `resources`")
        );
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

    #[test]
    fn execution_worker_rejects_an_unmanaged_guest_network() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut config =
            WorkerConfig::load(&root.join("config.example.worker-benchmark.toml")).unwrap();
        config
            .libvirt
            .as_mut()
            .unwrap()
            .vm
            .network = "default".into();

        let error = config.validate().unwrap_err();

        assert!(
            error
                .to_string()
                .contains("policy-managed sandbox-egress")
        );
    }

    #[test]
    fn runtime_host_capacity_validates_each_configured_profile() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let benchmark =
            WorkerConfig::load(&root.join("config.example.worker-benchmark.toml")).unwrap();
        let block =
            WorkerConfig::load(&root.join("config.example.worker-block-validation.toml")).unwrap();

        let ample = ResourceFacts {
            logical_cpus: 64,
            memory_bytes: 256 * 1024 * 1024 * 1024,
        };
        benchmark
            .validate_host_resources(&ample)
            .unwrap();
        block
            .validate_host_resources(&ample)
            .unwrap();

        assert!(
            benchmark
                .validate_host_resources(&ResourceFacts {
                    logical_cpus: 3,
                    memory_bytes: ample.memory_bytes,
                })
                .unwrap_err()
                .to_string()
                .contains("build profile")
        );
        assert!(
            block
                .validate_host_resources(&ResourceFacts {
                    logical_cpus: ample.logical_cpus,
                    memory_bytes: 196 * 1024 * 1024 * 1024,
                })
                .unwrap_err()
                .to_string()
                .contains("block-validation profile")
        );
    }
}
