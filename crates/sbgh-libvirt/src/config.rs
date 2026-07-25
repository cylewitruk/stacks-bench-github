use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct VmConfig {
    pub golden_image: PathBuf,
    pub build_vcpus: u32,
    pub bench_vcpus: u32,
    pub build_memory_bytes: u64,
    pub bench_memory_bytes: u64,
    pub boot_disk_gib: u32,
    pub job_timeout_secs: u64,
    pub network: String,
    pub poll_interval_secs: u64,
    pub heartbeat_interval_secs: u64,
}

#[derive(Debug, Clone)]
pub struct PathsConfig {
    pub jobs_dir: PathBuf,
    pub git_mirror: PathBuf,
    pub results_tmpfs_root: PathBuf,
    pub results_archive_dir: PathBuf,
    pub sccache_dir: PathBuf,
    pub virsh_binary: PathBuf,
    pub sudo_binary: PathBuf,
    pub qemu_img_binary: PathBuf,
    pub cloud_localds_binary: PathBuf,
    pub git_binary: PathBuf,
}

#[derive(Debug, Clone)]
pub struct LvmConfig {
    pub vg_name: String,
    pub thinpool: String,
    pub chainstate_base_prefix: String,
    pub chainstate_snapshot_size_gib: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct LibvirtConfig {
    pub vm: VmConfig,
    pub paths: PathsConfig,
    pub lvm: LvmConfig,
    pub service_user: String,
    pub host_cpus: Option<String>,
}
