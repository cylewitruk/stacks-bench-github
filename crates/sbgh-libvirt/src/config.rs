use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VmConfig {
    pub golden_image: PathBuf,
    pub boot_disk_gib: u32,
    pub network: String,
    pub poll_interval_secs: u64,
    pub heartbeat_interval_secs: u64,
}

/// Resources used only by benchmark and build-only recipes.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BenchmarkProfile {
    pub build_vcpus: u32,
    pub bench_vcpus: u32,
    pub build_memory_bytes: u64,
    pub bench_memory_bytes: u64,
    pub job_timeout_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PathsConfig {
    pub jobs_dir: PathBuf,
    pub git_mirror: PathBuf,
    pub results_tmpfs_root: PathBuf,
    pub results_archive_dir: PathBuf,
    pub virsh_binary: PathBuf,
    pub sudo_binary: PathBuf,
    pub qemu_img_binary: PathBuf,
    pub cloud_localds_binary: PathBuf,
    pub git_binary: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LvmConfig {
    pub vg_name: String,
    pub thinpool: String,
    pub chainstate_base_prefix: String,
    pub chainstate_snapshot_size_gib: Option<u32>,
    /// Fixed setup-health floor shared by every thin-snapshot workload.
    ///
    /// This rejects an already near-full pool; it is not a prediction of an
    /// assignment's write divergence.
    #[serde(default = "default_min_thin_pool_free_percent")]
    pub min_data_free_percent: f64,
    /// Fixed setup-health floor for the thin-pool metadata LV.
    #[serde(default = "default_min_thin_pool_free_percent")]
    pub min_metadata_free_percent: f64,
}

fn default_min_thin_pool_free_percent() -> f64 {
    5.0
}

impl LvmConfig {
    pub(crate) fn validate_pool_health_policy(&self) -> anyhow::Result<()> {
        for (name, percent) in [
            ("min_data_free_percent", self.min_data_free_percent),
            ("min_metadata_free_percent", self.min_metadata_free_percent),
        ] {
            anyhow::ensure!(
                percent.is_finite() && percent > 0.0 && percent <= 100.0,
                "{name} must be finite, greater than 0, and at most 100"
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LibvirtConfig {
    pub vm: VmConfig,
    #[serde(default)]
    pub benchmark: BenchmarkProfile,
    pub paths: PathsConfig,
    pub lvm: LvmConfig,
    pub service_user: String,
    pub host_cpus: Option<String>,
    #[serde(default)]
    pub block_validation: Option<BlockValidationProfile>,
}

/// Operator-owned resource and storage policy for one block-validation VM.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlockValidationProfile {
    pub vcpus: u32,
    pub memory_bytes: u64,
    pub cpu_set: Option<String>,
    pub target_blocks_per_shard: u64,
    pub max_shards: u32,
    pub max_concurrency: u32,
    #[serde(default = "default_max_parallel_jobs")]
    pub max_parallel_jobs: u32,
    #[serde(default = "default_block_results_tmpfs_mib")]
    pub results_tmpfs_mib: u32,
    #[serde(default = "default_snapshot_prefix")]
    pub snapshot_prefix: String,
    #[serde(default = "default_mount_options")]
    pub mount_options: Vec<String>,
}

fn default_max_parallel_jobs() -> u32 {
    1
}

fn default_block_results_tmpfs_mib() -> u32 {
    5_120
}

fn default_snapshot_prefix() -> String {
    "sbgh-block".into()
}

fn default_mount_options() -> Vec<String> {
    vec!["nouuid".into(), "noatime".into()]
}
