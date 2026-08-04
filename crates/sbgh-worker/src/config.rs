use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use anyhow::{Context, ensure};
use sbgh_fleet::WorkerCapability;
use sbgh_libvirt::{
    BenchmarkProfile, BlockValidationProfile, LibvirtConfig, LvmConfig, PathsConfig, VmConfig,
};
use serde::Deserialize;

use crate::BinaryCacheConfig;
use crate::host_resources::{HostResources, parse_cpu_list};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerConfig {
    pub orchestrator_url: String,
    pub identity_private_key: PathBuf,
    pub workspace: WorkspaceConfig,
    pub sandbox: SandboxConfig,
    pub commands: CommandsConfig,
    pub lvm: LvmConfig,
    pub benchmark: Option<BenchmarkConfig>,
    pub block_validation: Option<BlockValidationProfile>,
    pub binary_cache: Option<BinaryCacheConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceConfig {
    pub jobs_dir: PathBuf,
    pub git_mirror: PathBuf,
    pub results_tmpfs_root: PathBuf,
    pub results_archive_dir: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxConfig {
    pub service_user: String,
    pub golden_image: PathBuf,
    pub boot_disk_gib: u32,
    pub host_cpus: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandsConfig {
    pub virsh: PathBuf,
    pub sudo: PathBuf,
    pub qemu_img: PathBuf,
    pub cloud_localds: PathBuf,
    pub git: PathBuf,
}

/// Worker-owned benchmark resource and CPU-placement policy. Build and
/// measurement phases are sequential and intentionally share one isolated CPU
/// set on a single-assignment worker.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BenchmarkConfig {
    pub build_vcpus: u32,
    pub bench_vcpus: u32,
    pub build_memory_bytes: u64,
    pub bench_memory_bytes: u64,
    pub job_timeout_secs: u64,
    pub cpu_set: String,
}

impl BenchmarkConfig {
    fn libvirt_profile(&self) -> BenchmarkProfile {
        BenchmarkProfile {
            build_vcpus: self.build_vcpus,
            bench_vcpus: self.bench_vcpus,
            build_memory_bytes: self.build_memory_bytes,
            bench_memory_bytes: self.bench_memory_bytes,
            job_timeout_secs: self.job_timeout_secs,
        }
    }
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
            self.benchmark.is_some()
                || self
                    .block_validation
                    .is_some(),
            "at least one recipe section is required"
        );
        if let Some(benchmark) = &self.benchmark {
            ensure!(
                benchmark.build_vcpus > 0
                    && benchmark.build_memory_bytes > 0
                    && benchmark.job_timeout_secs > 0
                    && benchmark.bench_vcpus > 0
                    && benchmark.bench_memory_bytes > 0,
                "benchmark recipe requires non-zero build and execution resources"
            );
            let cpus = parse_cpu_list(&benchmark.cpu_set, "benchmark.cpu_set")?;
            ensure!(
                usize::try_from(benchmark.build_vcpus).is_ok_and(|vcpus| vcpus <= cpus.len())
                    && usize::try_from(benchmark.bench_vcpus)
                        .is_ok_and(|vcpus| vcpus <= cpus.len()),
                "benchmark.cpu_set must contain at least as many CPUs as each benchmark phase"
            );
        }
        if let Some(block) = &self.block_validation {
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
                block.target_blocks_per_shard > 0
                    && block.max_shards > 0
                    && block.max_concurrency > 0
                    && block.max_concurrency <= block.max_shards,
                "invalid block-validation shard/concurrency limits"
            );
            let cpus = parse_cpu_list(
                block
                    .cpu_set
                    .as_deref()
                    .context("block_validation.cpu_set is required")?,
                "block_validation.cpu_set",
            )?;
            ensure!(
                usize::try_from(block.vcpus).is_ok_and(|vcpus| vcpus <= cpus.len()),
                "block_validation.cpu_set must contain at least as many CPUs as block_validation.vcpus"
            );
        }
        if let Some(host_cpus) = &self.sandbox.host_cpus {
            let host = parse_cpu_list(host_cpus, "sandbox.host_cpus")?;
            if let Some(benchmark) = &self.benchmark {
                let guest = parse_cpu_list(&benchmark.cpu_set, "benchmark.cpu_set")?;
                ensure!(
                    host.is_disjoint(&guest),
                    "sandbox.host_cpus and benchmark.cpu_set must be disjoint"
                );
            }
            if let Some(block) = &self.block_validation {
                let guest = parse_cpu_list(
                    block
                        .cpu_set
                        .as_deref()
                        .context("block_validation.cpu_set is required")?,
                    "block_validation.cpu_set",
                )?;
                ensure!(
                    host.is_disjoint(&guest),
                    "sandbox.host_cpus and block_validation.cpu_set must be disjoint"
                );
            }
        }
        Ok(())
    }

    pub fn advertised_capabilities(&self) -> BTreeSet<WorkerCapability> {
        let mut capabilities = BTreeSet::new();
        if self.benchmark.is_some() {
            capabilities.insert(WorkerCapability::Benchmark);
            capabilities.insert(WorkerCapability::BuildOnly);
        }
        if self
            .block_validation
            .is_some()
        {
            capabilities.insert(WorkerCapability::BlockValidation);
        }
        capabilities
    }

    pub fn libvirt_config(&self) -> LibvirtConfig {
        LibvirtConfig {
            vm: VmConfig {
                golden_image: self
                    .sandbox
                    .golden_image
                    .clone(),
                boot_disk_gib: self.sandbox.boot_disk_gib,
                network: "sandbox-egress".into(),
                poll_interval_secs: 5,
                heartbeat_interval_secs: 60,
            },
            benchmark: self
                .benchmark
                .as_ref()
                .map(BenchmarkConfig::libvirt_profile)
                .unwrap_or_default(),
            paths: PathsConfig {
                jobs_dir: self
                    .workspace
                    .jobs_dir
                    .clone(),
                git_mirror: self
                    .workspace
                    .git_mirror
                    .clone(),
                results_tmpfs_root: self
                    .workspace
                    .results_tmpfs_root
                    .clone(),
                results_archive_dir: self
                    .workspace
                    .results_archive_dir
                    .clone(),
                virsh_binary: self.commands.virsh.clone(),
                sudo_binary: self.commands.sudo.clone(),
                qemu_img_binary: self.commands.qemu_img.clone(),
                cloud_localds_binary: self
                    .commands
                    .cloud_localds
                    .clone(),
                git_binary: self.commands.git.clone(),
            },
            lvm: self.lvm.clone(),
            service_user: self
                .sandbox
                .service_user
                .clone(),
            host_cpus: self.sandbox.host_cpus.clone(),
            block_validation: self.block_validation.clone(),
        }
    }

    /// Validate operator-selected guest profiles against measured host
    /// capacity. Build and execution VMs are sequential within an assignment,
    /// so each phase is checked independently rather than summed.
    pub fn validate_host_resources(&self, resources: &HostResources) -> anyhow::Result<()> {
        let facts = resources.facts();
        ensure!(
            facts.logical_cpus > 0 && facts.memory_bytes > 0,
            "discovered host resources require non-zero CPU and memory"
        );
        if let Some(benchmark) = &self.benchmark {
            ensure!(
                benchmark.build_vcpus <= facts.logical_cpus
                    && benchmark.build_memory_bytes <= facts.memory_bytes,
                "benchmark build profile exceeds discovered host resources: requested_vcpus={} \
                 requested_memory_bytes={} online_cpus={} memory_bytes={}",
                benchmark.build_vcpus,
                benchmark.build_memory_bytes,
                facts.logical_cpus,
                facts.memory_bytes,
            );
            ensure!(
                benchmark.bench_vcpus <= facts.logical_cpus
                    && benchmark.bench_memory_bytes <= facts.memory_bytes,
                "benchmark execution profile exceeds discovered host resources: \
                 requested_vcpus={} requested_memory_bytes={} online_cpus={} memory_bytes={}",
                benchmark.bench_vcpus,
                benchmark.bench_memory_bytes,
                facts.logical_cpus,
                facts.memory_bytes,
            );
            validate_online_cpu_set(
                &benchmark.cpu_set,
                "benchmark.cpu_set",
                resources.online_cpus(),
            )?;
        }
        if let Some(block) = &self.block_validation {
            let reserved_memory = block
                .memory_bytes
                .checked_add(u64::from(block.results_tmpfs_mib) * 1024 * 1024)
                .context("block-validation memory reservation overflows")?;
            ensure!(
                block.vcpus <= facts.logical_cpus && reserved_memory <= facts.memory_bytes,
                "block-validation profile exceeds discovered host resources: requested_vcpus={} \
                 requested_memory_bytes={} online_cpus={} memory_bytes={}",
                block.vcpus,
                reserved_memory,
                facts.logical_cpus,
                facts.memory_bytes,
            );
            validate_online_cpu_set(
                block
                    .cpu_set
                    .as_deref()
                    .context("block_validation.cpu_set is required")?,
                "block_validation.cpu_set",
                resources.online_cpus(),
            )?;
        }
        if let Some(host_cpus) = &self.sandbox.host_cpus {
            validate_online_cpu_set(host_cpus, "sandbox.host_cpus", resources.online_cpus())?;
        }
        Ok(())
    }
}

fn validate_online_cpu_set(
    value: &str,
    field: &str,
    online_cpus: &BTreeSet<u32>,
) -> anyhow::Result<()> {
    let configured = parse_cpu_list(value, field)?;
    let offline = configured
        .difference(online_cpus)
        .copied()
        .collect::<Vec<_>>();
    ensure!(offline.is_empty(), "{field} contains offline CPUs: {offline:?}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn host_resources(logical_cpus: u32, memory_bytes: u64) -> HostResources {
        HostResources::new((0..logical_cpus).collect(), memory_bytes).unwrap()
    }

    #[test]
    fn checked_in_worker_examples_stay_parseable() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let benchmark =
            WorkerConfig::load(&root.join("config.example.worker-benchmark.toml")).unwrap();
        let block =
            WorkerConfig::load(&root.join("config.example.worker-block-validation.toml")).unwrap();
        let combined =
            WorkerConfig::load(&root.join("config.example.worker-combined.toml")).unwrap();

        assert_eq!(
            benchmark.advertised_capabilities(),
            BTreeSet::from([WorkerCapability::Benchmark, WorkerCapability::BuildOnly])
        );
        assert_eq!(
            block.advertised_capabilities(),
            BTreeSet::from([WorkerCapability::BlockValidation])
        );
        assert_eq!(
            combined.advertised_capabilities(),
            BTreeSet::from([
                WorkerCapability::Benchmark,
                WorkerCapability::BuildOnly,
                WorkerCapability::BlockValidation,
            ])
        );
        assert_eq!(
            benchmark
                .libvirt_config()
                .vm
                .network,
            "sandbox-egress"
        );
        assert_eq!(
            combined
                .libvirt_config()
                .vm
                .network,
            "sandbox-egress"
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
    fn retired_identity_capability_and_recipe_fields_are_rejected() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let text =
            std::fs::read_to_string(root.join("config.example.worker-benchmark.toml")).unwrap();
        for stale in [
            format!("worker_id = \"{}\"\n{text}", uuid::Uuid::new_v4()),
            format!("capabilities = [\"benchmark\"]\n{text}"),
            format!("client_certificate = \"/tmp/client.crt\"\n{text}"),
            format!("{text}\n[libvirt]\nservice_user = \"stale\"\n"),
        ] {
            assert!(toml::from_str::<WorkerConfig>(&stale).is_err());
        }
    }

    #[test]
    fn retired_block_validation_chain_config_is_rejected() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let text =
            std::fs::read_to_string(root.join("config.example.worker-block-validation.toml"))
                .unwrap();
        let stale = text.replace(
            "results_tmpfs_mib = 5120",
            "results_tmpfs_mib = 5120\n\
             chain_config = \"/etc/sbgh/worker/stacks-inspect-mainnet.toml\"",
        );

        let error = toml::from_str::<WorkerConfig>(&stale).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("unknown field `chain_config`")
        );
    }

    #[test]
    fn block_validation_shard_policy_fails_closed() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let path = root.join("config.example.worker-block-validation.toml");

        let mut excessive_concurrency = WorkerConfig::load(&path).unwrap();
        let profile = excessive_concurrency
            .block_validation
            .as_mut()
            .unwrap();
        profile.max_concurrency = profile.max_shards + 1;
        assert!(
            excessive_concurrency
                .validate()
                .unwrap_err()
                .to_string()
                .contains("shard/concurrency limits")
        );

        let mut zero_target = WorkerConfig::load(&path).unwrap();
        zero_target
            .block_validation
            .as_mut()
            .unwrap()
            .target_blocks_per_shard = 0;
        assert!(
            zero_target
                .validate()
                .unwrap_err()
                .to_string()
                .contains("shard/concurrency limits")
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
    fn runtime_host_capacity_validates_each_configured_profile() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let benchmark =
            WorkerConfig::load(&root.join("config.example.worker-benchmark.toml")).unwrap();
        let block =
            WorkerConfig::load(&root.join("config.example.worker-block-validation.toml")).unwrap();

        let ample = host_resources(64, 256 * 1024 * 1024 * 1024);
        benchmark
            .validate_host_resources(&ample)
            .unwrap();
        block
            .validate_host_resources(&ample)
            .unwrap();

        assert!(
            benchmark
                .validate_host_resources(&host_resources(3, ample.facts().memory_bytes))
                .unwrap_err()
                .to_string()
                .contains("build profile")
        );
        assert!(
            block
                .validate_host_resources(&host_resources(64, 196 * 1024 * 1024 * 1024))
                .unwrap_err()
                .to_string()
                .contains("block-validation profile")
        );
    }

    #[test]
    fn worker_owned_cpu_placement_must_be_online_and_disjoint() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut config =
            WorkerConfig::load(&root.join("config.example.worker-combined.toml")).unwrap();
        let resources = host_resources(64, 256 * 1024 * 1024 * 1024);
        config
            .validate_host_resources(&resources)
            .unwrap();

        config
            .benchmark
            .as_mut()
            .unwrap()
            .cpu_set = "0-2,64".into();
        assert!(
            config
                .validate_host_resources(&resources)
                .unwrap_err()
                .to_string()
                .contains("offline CPUs: [64]")
        );

        config
            .benchmark
            .as_mut()
            .unwrap()
            .cpu_set = "48-51".into();
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("must be disjoint")
        );
    }
}
