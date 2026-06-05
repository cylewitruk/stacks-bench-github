//! Configuration.
//!
//! The handler and the daemon hold different secrets and so are
//! deliberately given separate config schemas:
//!
//! - [`HandlerConfig`] — webhook-facing service. Only the HMAC secret and the
//!   daemon `/api` URL + ingest token. **No GitHub App credentials, no DB
//!   access, no authorization allowlist** (the daemon owns authorization).
//! - [`DaemonConfig`] — host-side benchmark runner. Holds the App private key,
//!   libvirt/LVM knobs, and everything required to run a job.
//!
//! Each binary loads its own type from its own TOML file. They never share a
//! config dir on disk — see [`HANDLER_DEFAULT_CONFIG_PATH`] /
//! [`DAEMON_DEFAULT_CONFIG_PATH`].
//!
//! Loading precedence (lowest → highest):
//!   1. compiled-in defaults
//!   2. TOML config file (see [`HandlerConfig::load`] / [`DaemonConfig::load`])
//!   3. environment variables (always win)

use std::env;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::memory::MemorySize;
use crate::{Error, Result};

/// Default on-disk path for the handler's TOML config. Bind-mounted into the
/// container at the same path so file refs work in both contexts.
pub const HANDLER_DEFAULT_CONFIG_PATH: &str = "/etc/sbgh/handler/config.toml";
/// Default on-disk path for the daemon's TOML config.
pub const DAEMON_DEFAULT_CONFIG_PATH: &str = "/etc/sbgh/daemon/config.toml";

const HANDLER_HOME_RELATIVE: &str = ".config/sbgh/handler/config.toml";
const DAEMON_HOME_RELATIVE: &str = ".config/sbgh/daemon/config.toml";

// ─────────────────────────── Shared subtypes ───────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub bind_addr: String,
}

// ─────────────────────────── HandlerConfig ───────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandlerConfig {
    pub server: ServerConfig,
    pub webhook: WebhookConfig,
    pub api: HandlerApiConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookConfig {
    /// HMAC-SHA256 secret used to verify inbound webhook signatures. The
    /// handler holds nothing else GitHub-related — no App ID, no private
    /// key — so a compromised handler cannot impersonate the App.
    pub secret: String,
}

/// How the handler reaches the daemon `/api` to submit verified
/// deliveries. Replaces the handler's former direct DB access.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandlerApiConfig {
    /// Base URL of the daemon `/api` (e.g.
    /// `http://host.docker.internal:8787`). From `[api].url` or
    /// `SBGH_API_URL`.
    pub url: String,
    /// Shared static token presented for the `ingest` scope — must match
    /// the daemon's `SBGH_API_INGEST_TOKEN`. **Env-only**
    /// (`SBGH_API_INGEST_TOKEN`), never read from the TOML, so the secret
    /// doesn't live in the bind-mounted config file.
    pub ingest_token: String,
}

impl HandlerConfig {
    pub fn load() -> Result<Self> {
        let path = resolve_config_path(HANDLER_DEFAULT_CONFIG_PATH, HANDLER_HOME_RELATIVE);
        Self::load_layered(path.as_deref())
    }

    pub fn load_layered(file: Option<&std::path::Path>) -> Result<Self> {
        let mut raw = RawHandler::default();
        if let Some(p) = file
            && p.exists()
        {
            let body = std::fs::read_to_string(p)?;
            let from_file: RawHandler = toml::from_str(&body)
                .map_err(|e| Error::Config(format!("parsing {}: {e}", p.display())))?;
            raw.merge(from_file);
        }
        raw.apply_env();
        raw.into_config()
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawHandler {
    server: RawServer,
    webhook: RawWebhook,
    api: RawHandlerApi,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawServer {
    bind_addr: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawWebhook {
    secret: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawHandlerApi {
    url: Option<String>,
    /// **Env-only** (`SBGH_API_INGEST_TOKEN`); `deny_unknown_fields` makes
    /// an `ingest_token` key in `[api]` a hard error, keeping the secret
    /// out of the bind-mounted TOML.
    #[serde(skip)]
    ingest_token: Option<String>,
}

impl RawHandler {
    fn merge(&mut self, other: RawHandler) {
        merge_opt(&mut self.server.bind_addr, other.server.bind_addr);
        merge_opt(&mut self.webhook.secret, other.webhook.secret);
        merge_opt(&mut self.api.url, other.api.url);
        // `api.ingest_token` is env-only (#[serde(skip)]); nothing to merge
        // from a file.
    }

    fn apply_env(&mut self) {
        env_into(&mut self.server.bind_addr, "SBGH_BIND_ADDR");
        env_into(&mut self.webhook.secret, "SBGH_WEBHOOK_SECRET");
        env_into(&mut self.api.url, "SBGH_API_URL");
        env_into(&mut self.api.ingest_token, "SBGH_API_INGEST_TOKEN");
    }

    fn into_config(self) -> Result<HandlerConfig> {
        Ok(HandlerConfig {
            server: ServerConfig {
                bind_addr: self
                    .server
                    .bind_addr
                    .unwrap_or_else(|| "0.0.0.0:8080".into()),
            },
            webhook: WebhookConfig {
                secret: required(self.webhook.secret, "[webhook].secret / SBGH_WEBHOOK_SECRET")?,
            },
            api: HandlerApiConfig {
                url: required(self.api.url, "[api].url / SBGH_API_URL")?,
                ingest_token: required(self.api.ingest_token, "SBGH_API_INGEST_TOKEN")?,
            },
        })
    }
}

// ─────────────────────────── DaemonConfig ───────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonConfig {
    pub server: DaemonServerConfig,
    pub github: GitHubConfig,
    pub vm: VmConfig,
    pub paths: PathsConfig,
    pub lvm: LvmConfig,
    pub stacks_bench: StacksBenchConfig,
    pub api: ApiConfig,
    pub reporting: ReportingConfig,
    pub runner: RunnerConfig,
}

/// Daemon run-loop tuning (roadmap-v5). Deliberately **not** under `[vm]`: the
/// limit is on daemon execution *slots*, not VM capacity — task kinds become
/// non-VM in v6, so the run-loop knob lives on its own.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunnerConfig {
    /// Maximum jobs executed concurrently. Default `1` (sequential — the
    /// historical behavior). Raise it only when the host can run that many
    /// jobs at once (each is a full VM today). Values below 1 are clamped to 1.
    pub max_concurrent_jobs: usize,
    /// Optional CPU pinning (roadmap-v5 Phase 5), one **libvirt cpuset** per
    /// concurrency slot — e.g. `["0-1", "2-3"]` pins slot 0's VM to cores 0,1
    /// and slot 1's to 2,3. Length must be ≥ `max_concurrent_jobs`. Empty (the
    /// default) → no pinning, vCPUs float across all host cores. Pinning each
    /// concurrent benchmark to dedicated cores removes scheduler jitter +
    /// core-sharing between jobs; it can't partition shared L3 / memory
    /// bandwidth (single-socket), so measure before trusting concurrent runs.
    #[serde(default)]
    pub cpu_sets: Vec<String>,
    /// Host cpuset for the qemu **emulator/I-O threads** (e.g. `"4-5"`), pinned
    /// *off* the benchmark cores so emulator activity doesn't jitter a measured
    /// run. Only applied when `cpu_sets` is set. `None` → emulator threads not
    /// pinned.
    #[serde(default)]
    pub host_cpus: Option<String>,
}

/// The daemon's `/api` server (roadmap-v3). Reachable only from the
/// local CLI (loopback) and the handler container (docker bridge) — never
/// a public interface.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiConfig {
    /// Listen addresses. Loopback for the local CLI; add the docker-bridge
    /// gateway IP so the handler container can reach the host daemon (a
    /// container cannot reach a loopback-only bind). Never a public
    /// interface.
    pub listen: Vec<String>,
    /// Where the daemon writes the operator (`admin`) cookie at startup,
    /// mode 0600, regenerated each boot. The local CLI reads it.
    pub cookie_path: PathBuf,
    /// Shared static token the handler presents for the `ingest` scope.
    /// `None` disables ingest auth (no caller can submit webhooks) — fine
    /// until the handler is migrated onto the API.
    pub ingest_token: Option<String>,
}

/// Daemon-specific server bits. No `bind_addr` (it doesn't listen).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonServerConfig {
    pub database_url: String,
    /// OS user the daemon runs as. Used to chown the per-job source
    /// mount inside libvirt.
    pub service_user: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitHubConfig {
    /// GitHub App **Client ID** (e.g. `Iv23li...`) — JWT `iss` claim when
    /// minting installation tokens.
    pub client_id: String,
    pub api_base_url: String,
    pub private_key_path: PathBuf,
}

/// Which surfaces a job's benchmark result is reported on (roadmap-v4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportingConfig {
    /// PR (`/benchmark`) jobs. Default `both` (a Check Run + a summary comment
    /// that links to it).
    pub pr_report: PrReport,
    /// Baseline (`branch_push`/`tag_created`) jobs. Default `check` (a
    /// commit-level Check Run); `none` keeps them headless/DB-only.
    pub baseline_report: BaselineReport,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrReport {
    Comment,
    Check,
    Both,
}

impl PrReport {
    pub fn wants_comment(self) -> bool {
        matches!(self, PrReport::Comment | PrReport::Both)
    }
    pub fn wants_check(self) -> bool {
        matches!(self, PrReport::Check | PrReport::Both)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BaselineReport {
    Check,
    None,
}

impl BaselineReport {
    pub fn wants_check(self) -> bool {
        matches!(self, BaselineReport::Check)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VmConfig {
    pub golden_image: PathBuf,
    /// vCPUs allocated for the **build phase**. Cargo parallelises
    /// across cores; more vCPUs ≈ faster cargo. Defaults to 4.
    pub build_vcpus: u32,
    /// vCPUs allocated for the **bench phase**. Should match the
    /// production deployment shape — a stacks-node typically runs on
    /// 2–4 cores in prod, so the bench is meaningless if measured
    /// against 16+ cores worth of parallel block validation. Defaults
    /// to 2.
    pub bench_vcpus: u32,
    /// Memory the VM gets for its **build phase**. Stacks-core's
    /// `lto=fat` release link of `stacks-bench` peaks at ~7.6 GiB
    /// RSS; 16G is the practical floor (lower OOM-kills the build).
    /// Accepts the IEC short-form syntax (e.g. `"16G"`, `"8192M"`).
    pub build_memory: MemorySize,
    /// Memory the VM gets for its **bench phase** — typically smaller
    /// than `build_memory` to match the production deployment size
    /// that's being measured. Page cache + kernel memory pressure
    /// directly affect block-replay timing, so running the bench at
    /// build memory would make the numbers non-representative of
    /// production. Defaults to `"8G"` (the stacks-node prod target).
    /// Same syntax as `build_memory`.
    pub bench_memory: MemorySize,
    pub boot_disk_gib: u32,
    pub job_timeout_secs: u64,
    /// libvirt network to attach to the VM (default: `default`, NAT outbound).
    pub network: String,
    /// How often the daemon polls the in-VM phase log + virsh
    /// domstate. Each poll runs a `virsh domstate` subprocess (~50–
    /// 100ms), so lower values = more CPU on the host. 5s is the
    /// sensible floor for our workload — actual phases (`building`,
    /// `running`) last minutes to hours, so the phase-change detection
    /// latency is invisible.
    pub poll_interval_secs: u64,
    /// How often the daemon emits a heartbeat log line (and a
    /// throttled PR-comment refresh) showing the current phase + elapsed
    /// time in that phase. Independent of poll interval so we can poll
    /// rarely but still surface liveness frequently (or vice versa).
    pub heartbeat_interval_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PathsConfig {
    pub jobs_dir: PathBuf,
    pub git_mirror: PathBuf,
    /// tmpfs root where per-job results directories are mounted on the host.
    pub results_tmpfs_root: PathBuf,
    /// Persistent destination for `run.sqlite` after a job completes.
    pub results_archive_dir: PathBuf,
    /// Host-side directory bind-mounted into every job's VM via virtio-fs
    /// as the sccache compiler cache. Persistent across jobs — that's the
    /// whole point. sccache enforces its own size cap (`SCCACHE_CACHE_SIZE`,
    /// see template) so this dir grows to at most ~20 GiB even if you
    /// never clean it manually.
    pub sccache_dir: PathBuf,
    pub virsh_binary: PathBuf,
    pub sudo_binary: PathBuf,
    pub qemu_img_binary: PathBuf,
    pub cloud_localds_binary: PathBuf,
    pub git_binary: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LvmConfig {
    pub vg_name: String,
    pub thinpool: String,
    /// Prefix used to discover the newest base chainstate LV (e.g. `mainnet-`).
    pub chainstate_base_prefix: String,
    /// Size hint for `lvcreate --snapshot -L`.
    ///
    /// - `None` (default): no `-L` — thin snapshot in the pool.
    /// - `Some(n)`: thick snapshot with an `n GiB` COW exception store.
    pub chainstate_snapshot_size_gib: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StacksBenchConfig {
    /// Placeholder until we lock down the `/benchmark` arg shape.
    pub default_args: String,
}

impl DaemonConfig {
    pub fn load() -> Result<Self> {
        let path = resolve_config_path(DAEMON_DEFAULT_CONFIG_PATH, DAEMON_HOME_RELATIVE);
        Self::load_layered(path.as_deref())
    }

    pub fn load_layered(file: Option<&std::path::Path>) -> Result<Self> {
        let mut raw = RawDaemon::default();
        if let Some(p) = file
            && p.exists()
        {
            let body = std::fs::read_to_string(p)?;
            let from_file: RawDaemon = toml::from_str(&body)
                .map_err(|e| Error::Config(format!("parsing {}: {e}", p.display())))?;
            raw.merge(from_file);
        }
        raw.apply_env();
        raw.into_config()
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawDaemon {
    server: RawDaemonServer,
    github: RawGitHub,
    vm: RawVm,
    paths: RawPaths,
    lvm: RawLvm,
    stacks_bench: RawStacksBench,
    api: RawApi,
    reporting: RawReporting,
    runner: RawRunner,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawReporting {
    pr_report: Option<PrReport>,
    baseline_report: Option<BaselineReport>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawRunner {
    max_concurrent_jobs: Option<usize>,
    cpu_sets: Option<Vec<String>>,
    host_cpus: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawApi {
    listen: Option<Vec<String>>,
    cookie_path: Option<PathBuf>,
    /// **Env-only** (`SBGH_API_INGEST_TOKEN`) — never read from the TOML
    /// file so the secret stays out of config. `#[serde(skip)]` +
    /// `deny_unknown_fields` makes an `ingest_token` key in `[api]` a hard
    /// error rather than silently honoring it.
    #[serde(skip)]
    ingest_token: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawDaemonServer {
    database_url: Option<String>,
    service_user: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawGitHub {
    client_id: Option<String>,
    api_base_url: Option<String>,
    private_key_path: Option<PathBuf>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawVm {
    golden_image: Option<PathBuf>,
    build_vcpus: Option<u32>,
    bench_vcpus: Option<u32>,
    build_memory: Option<MemorySize>,
    bench_memory: Option<MemorySize>,
    boot_disk_gib: Option<u32>,
    job_timeout_secs: Option<u64>,
    network: Option<String>,
    poll_interval_secs: Option<u64>,
    heartbeat_interval_secs: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawPaths {
    jobs_dir: Option<PathBuf>,
    git_mirror: Option<PathBuf>,
    results_tmpfs_root: Option<PathBuf>,
    results_archive_dir: Option<PathBuf>,
    sccache_dir: Option<PathBuf>,
    virsh_binary: Option<PathBuf>,
    sudo_binary: Option<PathBuf>,
    qemu_img_binary: Option<PathBuf>,
    cloud_localds_binary: Option<PathBuf>,
    git_binary: Option<PathBuf>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawLvm {
    vg_name: Option<String>,
    thinpool: Option<String>,
    chainstate_base_prefix: Option<String>,
    chainstate_snapshot_size_gib: Option<u32>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawStacksBench {
    default_args: Option<String>,
}

impl RawDaemon {
    fn merge(&mut self, other: RawDaemon) {
        merge_opt(&mut self.server.database_url, other.server.database_url);
        merge_opt(&mut self.server.service_user, other.server.service_user);

        merge_opt(&mut self.github.client_id, other.github.client_id);
        merge_opt(&mut self.github.api_base_url, other.github.api_base_url);
        merge_opt(&mut self.github.private_key_path, other.github.private_key_path);

        merge_opt(&mut self.reporting.pr_report, other.reporting.pr_report);
        merge_opt(
            &mut self.reporting.baseline_report,
            other
                .reporting
                .baseline_report,
        );

        merge_opt(
            &mut self
                .runner
                .max_concurrent_jobs,
            other
                .runner
                .max_concurrent_jobs,
        );
        merge_opt(&mut self.runner.cpu_sets, other.runner.cpu_sets);
        merge_opt(&mut self.runner.host_cpus, other.runner.host_cpus);

        merge_opt(&mut self.vm.golden_image, other.vm.golden_image);
        merge_opt(&mut self.vm.build_vcpus, other.vm.build_vcpus);
        merge_opt(&mut self.vm.bench_vcpus, other.vm.bench_vcpus);
        merge_opt(&mut self.vm.build_memory, other.vm.build_memory);
        merge_opt(&mut self.vm.bench_memory, other.vm.bench_memory);
        merge_opt(&mut self.vm.boot_disk_gib, other.vm.boot_disk_gib);
        merge_opt(&mut self.vm.job_timeout_secs, other.vm.job_timeout_secs);
        merge_opt(&mut self.vm.network, other.vm.network);
        merge_opt(&mut self.vm.poll_interval_secs, other.vm.poll_interval_secs);
        merge_opt(
            &mut self
                .vm
                .heartbeat_interval_secs,
            other
                .vm
                .heartbeat_interval_secs,
        );

        merge_opt(&mut self.paths.jobs_dir, other.paths.jobs_dir);
        merge_opt(&mut self.paths.git_mirror, other.paths.git_mirror);
        merge_opt(&mut self.paths.results_tmpfs_root, other.paths.results_tmpfs_root);
        merge_opt(
            &mut self.paths.results_archive_dir,
            other
                .paths
                .results_archive_dir,
        );
        merge_opt(&mut self.paths.sccache_dir, other.paths.sccache_dir);
        merge_opt(&mut self.paths.virsh_binary, other.paths.virsh_binary);
        merge_opt(&mut self.paths.sudo_binary, other.paths.sudo_binary);
        merge_opt(&mut self.paths.qemu_img_binary, other.paths.qemu_img_binary);
        merge_opt(
            &mut self
                .paths
                .cloud_localds_binary,
            other
                .paths
                .cloud_localds_binary,
        );
        merge_opt(&mut self.paths.git_binary, other.paths.git_binary);

        merge_opt(&mut self.lvm.vg_name, other.lvm.vg_name);
        merge_opt(&mut self.lvm.thinpool, other.lvm.thinpool);
        merge_opt(
            &mut self
                .lvm
                .chainstate_base_prefix,
            other
                .lvm
                .chainstate_base_prefix,
        );
        merge_opt(
            &mut self
                .lvm
                .chainstate_snapshot_size_gib,
            other
                .lvm
                .chainstate_snapshot_size_gib,
        );

        merge_opt(
            &mut self.stacks_bench.default_args,
            other
                .stacks_bench
                .default_args,
        );

        merge_opt(&mut self.api.listen, other.api.listen);
        merge_opt(&mut self.api.cookie_path, other.api.cookie_path);
        // `api.ingest_token` is env-only (#[serde(skip)]); nothing to merge
        // from a file.
    }

    fn apply_env(&mut self) {
        env_into(&mut self.server.database_url, "DATABASE_URL");
        env_into(&mut self.server.service_user, "SBGH_SERVICE_USER");

        env_into(&mut self.github.client_id, "SBGH_GH_CLIENT_ID");
        env_into(&mut self.github.api_base_url, "SBGH_GH_API_BASE_URL");
        env_path_into(&mut self.github.private_key_path, "SBGH_GH_PRIVATE_KEY_PATH");

        env_path_into(&mut self.vm.golden_image, "SBGH_VM_GOLDEN_IMAGE");
        env_parse_into(&mut self.vm.build_vcpus, "SBGH_VM_BUILD_VCPUS");
        env_parse_into(&mut self.vm.bench_vcpus, "SBGH_VM_BENCH_VCPUS");
        env_parse_into(&mut self.vm.build_memory, "SBGH_VM_BUILD_MEMORY");
        env_parse_into(&mut self.vm.bench_memory, "SBGH_VM_BENCH_MEMORY");
        env_parse_into(&mut self.vm.boot_disk_gib, "SBGH_VM_BOOT_DISK_GIB");
        env_parse_into(&mut self.vm.job_timeout_secs, "SBGH_VM_JOB_TIMEOUT_SECS");
        env_into(&mut self.vm.network, "SBGH_VM_NETWORK");
        env_parse_into(&mut self.vm.poll_interval_secs, "SBGH_VM_POLL_INTERVAL_SECS");
        env_parse_into(
            &mut self
                .vm
                .heartbeat_interval_secs,
            "SBGH_VM_HEARTBEAT_INTERVAL_SECS",
        );

        env_path_into(&mut self.paths.jobs_dir, "SBGH_JOBS_DIR");
        env_path_into(&mut self.paths.git_mirror, "SBGH_GIT_MIRROR");
        env_path_into(&mut self.paths.results_tmpfs_root, "SBGH_RESULTS_TMPFS_ROOT");
        env_path_into(&mut self.paths.results_archive_dir, "SBGH_RESULTS_ARCHIVE_DIR");
        env_path_into(&mut self.paths.sccache_dir, "SBGH_SCCACHE_DIR");
        env_path_into(&mut self.paths.virsh_binary, "SBGH_VIRSH_BIN");
        env_path_into(&mut self.paths.sudo_binary, "SBGH_SUDO_BIN");
        env_path_into(&mut self.paths.qemu_img_binary, "SBGH_QEMU_IMG_BIN");
        env_path_into(
            &mut self
                .paths
                .cloud_localds_binary,
            "SBGH_CLOUD_LOCALDS_BIN",
        );
        env_path_into(&mut self.paths.git_binary, "SBGH_GIT_BIN");

        env_into(&mut self.lvm.vg_name, "SBGH_LVM_VG");
        env_into(&mut self.lvm.thinpool, "SBGH_LVM_THINPOOL");
        env_into(
            &mut self
                .lvm
                .chainstate_base_prefix,
            "SBGH_LVM_CHAINSTATE_PREFIX",
        );
        env_parse_into(
            &mut self
                .lvm
                .chainstate_snapshot_size_gib,
            "SBGH_LVM_SNAPSHOT_GIB",
        );

        env_into(&mut self.stacks_bench.default_args, "SBGH_STACKS_BENCH_ARGS");

        env_csv_into(&mut self.api.listen, "SBGH_API_LISTEN");
        env_path_into(&mut self.api.cookie_path, "SBGH_API_COOKIE_PATH");
        env_into(&mut self.api.ingest_token, "SBGH_API_INGEST_TOKEN");

        env_parse_into(
            &mut self
                .runner
                .max_concurrent_jobs,
            "SBGH_RUNNER_MAX_CONCURRENT_JOBS",
        );
        // cpu_sets is a list of cpusets (each may contain commas, e.g. "0,2"),
        // so it's `;`-separated rather than CSV.
        if let Ok(v) = std::env::var("SBGH_RUNNER_CPU_SETS") {
            self.runner.cpu_sets = Some(
                v.split(';')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect(),
            );
        }
        env_into(&mut self.runner.host_cpus, "SBGH_RUNNER_HOST_CPUS");
    }

    fn into_config(self) -> Result<DaemonConfig> {
        Ok(DaemonConfig {
            server: DaemonServerConfig {
                database_url: required(self.server.database_url, "DATABASE_URL")?,
                service_user: self
                    .server
                    .service_user
                    .unwrap_or_else(|| "sbgh".into()),
            },
            github: GitHubConfig {
                client_id: required(
                    self.github.client_id,
                    "[github].client_id / SBGH_GH_CLIENT_ID",
                )?,
                api_base_url: self
                    .github
                    .api_base_url
                    .unwrap_or_else(|| "https://api.github.com".into()),
                private_key_path: required(
                    self.github.private_key_path,
                    "[github].private_key_path / SBGH_GH_PRIVATE_KEY_PATH",
                )?,
            },
            vm: VmConfig {
                golden_image: required(self.vm.golden_image, "[vm].golden_image")?,
                // Build phase defaults — give cargo plenty of parallelism
                // and headroom; 16 GiB is the rustc-link-OOM floor for
                // stacks-core's lto=fat release profile.
                build_vcpus: self
                    .vm
                    .build_vcpus
                    .unwrap_or(4),
                build_memory: self
                    .vm
                    .build_memory
                    .unwrap_or_else(|| MemorySize::from_gib(16)),
                // Bench phase defaults match the production stacks-node
                // deployment shape so per-block timings are representative.
                bench_vcpus: self
                    .vm
                    .bench_vcpus
                    .unwrap_or(2),
                bench_memory: self
                    .vm
                    .bench_memory
                    .unwrap_or_else(|| MemorySize::from_gib(8)),
                boot_disk_gib: self
                    .vm
                    .boot_disk_gib
                    .unwrap_or(64),
                job_timeout_secs: self
                    .vm
                    .job_timeout_secs
                    .unwrap_or(21_600),
                network: self
                    .vm
                    .network
                    .unwrap_or_else(|| "default".into()),
                poll_interval_secs: self
                    .vm
                    .poll_interval_secs
                    .unwrap_or(5),
                heartbeat_interval_secs: self
                    .vm
                    .heartbeat_interval_secs
                    .unwrap_or(60),
            },
            paths: PathsConfig {
                jobs_dir: self
                    .paths
                    .jobs_dir
                    .unwrap_or_else(|| PathBuf::from("/var/lib/sbgh/jobs")),
                git_mirror: self
                    .paths
                    .git_mirror
                    .unwrap_or_else(|| PathBuf::from("/var/lib/sbgh/git/stacks-core.git")),
                results_tmpfs_root: self
                    .paths
                    .results_tmpfs_root
                    .unwrap_or_else(|| PathBuf::from("/run/sbgh/jobs")),
                results_archive_dir: self
                    .paths
                    .results_archive_dir
                    .unwrap_or_else(|| PathBuf::from("/var/lib/sbgh/results")),
                sccache_dir: self
                    .paths
                    .sccache_dir
                    .unwrap_or_else(|| PathBuf::from("/var/lib/sbgh/sccache")),
                virsh_binary: self
                    .paths
                    .virsh_binary
                    .unwrap_or_else(|| PathBuf::from("/usr/bin/virsh")),
                sudo_binary: self
                    .paths
                    .sudo_binary
                    .unwrap_or_else(|| PathBuf::from("/usr/bin/sudo")),
                qemu_img_binary: self
                    .paths
                    .qemu_img_binary
                    .unwrap_or_else(|| PathBuf::from("/usr/bin/qemu-img")),
                cloud_localds_binary: self
                    .paths
                    .cloud_localds_binary
                    .unwrap_or_else(|| PathBuf::from("/usr/bin/cloud-localds")),
                git_binary: self
                    .paths
                    .git_binary
                    .unwrap_or_else(|| PathBuf::from("/usr/bin/git")),
            },
            lvm: LvmConfig {
                vg_name: required(self.lvm.vg_name, "[lvm].vg_name")?,
                thinpool: required(self.lvm.thinpool, "[lvm].thinpool")?,
                chainstate_base_prefix: self
                    .lvm
                    .chainstate_base_prefix
                    .unwrap_or_else(|| "mainnet-".into()),
                chainstate_snapshot_size_gib: self
                    .lvm
                    .chainstate_snapshot_size_gib,
            },
            stacks_bench: StacksBenchConfig {
                default_args: self
                    .stacks_bench
                    .default_args
                    .unwrap_or_default(),
            },
            api: ApiConfig {
                // Loopback only by default; the operator adds the
                // docker-bridge gateway IP for the handler hop.
                listen: self
                    .api
                    .listen
                    .unwrap_or_else(|| vec!["127.0.0.1:8787".into()]),
                cookie_path: self
                    .api
                    .cookie_path
                    .unwrap_or_else(|| PathBuf::from("/etc/sbgh/daemon/.cookie")),
                ingest_token: self.api.ingest_token,
            },
            reporting: ReportingConfig {
                pr_report: self
                    .reporting
                    .pr_report
                    .unwrap_or(PrReport::Both),
                baseline_report: self
                    .reporting
                    .baseline_report
                    .unwrap_or(BaselineReport::Check),
            },
            runner: {
                // Default 1 = sequential (historical behavior). Clamp 0 → 1.
                let max_concurrent_jobs = self
                    .runner
                    .max_concurrent_jobs
                    .unwrap_or(1)
                    .max(1);
                let cpu_sets = self
                    .runner
                    .cpu_sets
                    .unwrap_or_default();
                // Pinning is all-or-nothing per slot: if set, every concurrency
                // slot needs a cpuset, else some jobs would run unpinned and
                // contend with pinned ones.
                if !cpu_sets.is_empty() && cpu_sets.len() < max_concurrent_jobs {
                    return Err(Error::Config(format!(
                        "[runner].cpu_sets has {} entries but max_concurrent_jobs is {}; provide \
                         one cpuset per slot (or none to disable pinning)",
                        cpu_sets.len(),
                        max_concurrent_jobs,
                    )));
                }
                RunnerConfig {
                    max_concurrent_jobs,
                    cpu_sets,
                    host_cpus: self.runner.host_cpus,
                }
            },
        })
    }
}

// ─────────────────────────── Shared helpers ───────────────────────────

fn merge_opt<T>(dst: &mut Option<T>, src: Option<T>) {
    if let Some(v) = src {
        *dst = Some(v);
    }
}

fn env_into(dst: &mut Option<String>, key: &str) {
    if let Ok(v) = env::var(key) {
        *dst = Some(v);
    }
}

fn env_path_into(dst: &mut Option<PathBuf>, key: &str) {
    if let Ok(v) = env::var(key) {
        *dst = Some(PathBuf::from(v));
    }
}

fn env_parse_into<T: std::str::FromStr>(dst: &mut Option<T>, key: &str) {
    if let Ok(v) = env::var(key)
        && let Ok(parsed) = v.parse::<T>()
    {
        *dst = Some(parsed);
    }
}

fn env_csv_into(dst: &mut Option<Vec<String>>, key: &str) {
    if let Ok(v) = env::var(key) {
        *dst = Some(
            v.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect(),
        );
    }
}

fn required<T>(value: Option<T>, name: &str) -> Result<T> {
    value.ok_or_else(|| Error::Config(format!("missing required config: {name}")))
}

/// Pick a config path for the given default + home-relative location.
///
/// Order:
///   1. `$SBGH_CONFIG` if set (verbatim — typos do not silently fall back).
///   2. The system default if it exists.
///   3. `$HOME/<home_relative>` if it exists.
///   4. None — fall through to env-only loading.
fn resolve_config_path(system_default: &str, home_relative: &str) -> Option<PathBuf> {
    if let Ok(explicit) = env::var("SBGH_CONFIG") {
        return Some(PathBuf::from(explicit));
    }
    let system = PathBuf::from(system_default);
    if system.exists() {
        return Some(system);
    }
    if let Some(home) = env::var_os("HOME") {
        let path = PathBuf::from(home).join(home_relative);
        if path.exists() {
            return Some(path);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(content: &str) -> tempfile::NamedTempFile {
        use std::io::Write;
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(content.as_bytes())
            .unwrap();
        f
    }

    /// Process-wide env mutex (covers plain `cargo test`; nextest runs each
    /// test in its own process and doesn't need this).
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    struct EnvGuard {
        saved: Vec<(&'static str, Option<String>)>,
        _lock: std::sync::MutexGuard<'static, ()>,
    }
    impl EnvGuard {
        fn set(vars: &[(&'static str, &str)]) -> Self {
            let lock = ENV_LOCK
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let saved: Vec<_> = vars
                .iter()
                .map(|(k, v)| {
                    let old = env::var(k).ok();
                    unsafe { env::set_var(k, v) };
                    (*k, old)
                })
                .collect();
            EnvGuard { saved, _lock: lock }
        }
    }
    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (k, old) in &self.saved {
                unsafe {
                    match old {
                        Some(v) => env::set_var(k, v),
                        None => env::remove_var(k),
                    }
                };
            }
        }
    }

    // ─── HandlerConfig ───

    fn handler_env() -> Vec<(&'static str, &'static str)> {
        vec![
            ("SBGH_API_URL", "http://api:8787"),
            ("SBGH_API_INGEST_TOKEN", "ingest-tok"),
            ("SBGH_WEBHOOK_SECRET", "hunter2"),
        ]
    }

    #[test]
    fn handler_loads_from_env_only() {
        let _g = EnvGuard::set(&handler_env());
        let cfg = HandlerConfig::load_layered(None).unwrap();
        assert_eq!(cfg.webhook.secret, "hunter2");
        assert_eq!(cfg.api.url, "http://api:8787");
        assert_eq!(cfg.api.ingest_token, "ingest-tok");
    }

    #[test]
    fn handler_api_ingest_token_in_toml_is_rejected() {
        let _g = EnvGuard::set(&handler_env());
        // The ingest token is env-only; a TOML `ingest_token` key must be a
        // hard error (deny_unknown_fields), keeping the secret out of the
        // bind-mounted config file.
        let f = write("[api]\nurl = \"http://api\"\ningest_token = \"leaked\"\n");
        let err = HandlerConfig::load_layered(Some(f.path())).unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn handler_toml_overrides_defaults_env_overrides_toml() {
        let mut env = handler_env();
        env.push(("SBGH_BIND_ADDR", "127.0.0.1:9000"));
        let _g = EnvGuard::set(&env);
        let f = write(
            r#"
            [server]
            bind_addr = "0.0.0.0:8081"

            [api]
            url = "http://toml-api"
            "#,
        );
        let cfg = HandlerConfig::load_layered(Some(f.path())).unwrap();
        assert_eq!(cfg.server.bind_addr, "127.0.0.1:9000", "env wins over TOML");
        // TOML supplies [api].url; env (handler_env) overrides it.
        assert_eq!(cfg.api.url, "http://api:8787", "env wins over TOML for api.url");
    }

    #[test]
    fn handler_missing_secret_errors() {
        // API env present but no webhook secret → Config error.
        let _g = EnvGuard::set(&[("SBGH_API_URL", "http://api"), ("SBGH_API_INGEST_TOKEN", "tok")]);
        let err = HandlerConfig::load_layered(None).unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    // ─── DaemonConfig ───

    fn daemon_env() -> Vec<(&'static str, &'static str)> {
        vec![
            ("DATABASE_URL", "postgres://o"),
            ("SBGH_GH_CLIENT_ID", "Iv23litest123"),
            ("SBGH_GH_PRIVATE_KEY_PATH", "/tmp/key.pem"),
            ("SBGH_LVM_VG", "sbgh-vg"),
            ("SBGH_LVM_THINPOOL", "thinpool"),
            ("SBGH_VM_GOLDEN_IMAGE", "/var/lib/libvirt/images/golden.qcow2"),
        ]
    }

    #[test]
    fn daemon_loads_from_env_only() {
        let _g = EnvGuard::set(&daemon_env());
        let cfg = DaemonConfig::load_layered(None).unwrap();
        assert_eq!(cfg.github.client_id, "Iv23litest123");
        assert_eq!(cfg.lvm.vg_name, "sbgh-vg");
        // Default split: 4 vCPU / 16 GiB for build, 2 vCPU / 8 GiB for bench.
        assert_eq!(cfg.vm.build_vcpus, 4);
        assert_eq!(cfg.vm.bench_vcpus, 2);
        assert_eq!(cfg.vm.build_memory, crate::memory::MemorySize::from_gib(16));
        assert_eq!(cfg.vm.bench_memory, crate::memory::MemorySize::from_gib(8));
    }

    #[test]
    fn daemon_env_overrides_toml() {
        // Env wins on collision for both vcpus AND memory; memory parses
        // the IEC short-form ("12G") the same way the TOML loader does.
        let mut env = daemon_env();
        env.push(("SBGH_VM_BUILD_VCPUS", "12"));
        env.push(("SBGH_VM_BUILD_MEMORY", "24G"));
        let _g = EnvGuard::set(&env);
        let f = write("[vm]\nbuild_vcpus = 4\nbuild_memory = \"16G\"\n");
        let cfg = DaemonConfig::load_layered(Some(f.path())).unwrap();
        assert_eq!(cfg.vm.build_vcpus, 12);
        assert_eq!(cfg.vm.build_memory, crate::memory::MemorySize::from_gib(24));
    }

    #[test]
    fn daemon_runner_cpu_pinning_parses() {
        let _g = EnvGuard::set(&daemon_env());
        let f = write(
            "[runner]\nmax_concurrent_jobs = 2\ncpu_sets = [\"0-1\", \"2-3\"]\nhost_cpus = \
             \"4-5\"\n",
        );
        let cfg = DaemonConfig::load_layered(Some(f.path())).unwrap();
        assert_eq!(cfg.runner.max_concurrent_jobs, 2);
        assert_eq!(cfg.runner.cpu_sets, vec!["0-1".to_string(), "2-3".to_string()]);
        assert_eq!(
            cfg.runner
                .host_cpus
                .as_deref(),
            Some("4-5")
        );
    }

    #[test]
    fn daemon_runner_defaults_to_no_pinning() {
        let _g = EnvGuard::set(&daemon_env());
        let cfg = DaemonConfig::load_layered(None).unwrap();
        assert_eq!(cfg.runner.max_concurrent_jobs, 1);
        assert!(cfg.runner.cpu_sets.is_empty(), "no pinning by default");
        assert!(cfg.runner.host_cpus.is_none());
    }

    #[test]
    fn daemon_runner_cpu_sets_shorter_than_max_errors() {
        // Pinning is all-or-nothing per slot: fewer cpusets than slots would
        // leave some jobs unpinned and contending with pinned ones.
        let _g = EnvGuard::set(&daemon_env());
        let f = write("[runner]\nmax_concurrent_jobs = 3\ncpu_sets = [\"0-1\", \"2-3\"]\n");
        let err = DaemonConfig::load_layered(Some(f.path())).unwrap_err();
        assert!(matches!(err, Error::Config(_)), "got: {err:?}");
    }

    #[test]
    fn daemon_missing_required_field_errors() {
        let _g = EnvGuard::set(&[("SBGH_GH_CLIENT_ID", "Iv23litest")]);
        let err = DaemonConfig::load_layered(None).unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn daemon_api_defaults() {
        let _g = EnvGuard::set(&daemon_env());
        let cfg = DaemonConfig::load_layered(None).unwrap();
        assert_eq!(cfg.api.listen, vec!["127.0.0.1:8787".to_string()]);
        assert_eq!(cfg.api.cookie_path, PathBuf::from("/etc/sbgh/daemon/.cookie"));
        assert_eq!(cfg.api.ingest_token, None);
    }

    #[test]
    fn daemon_api_from_toml_and_env() {
        // TOML supplies the multi-listener bind + cookie path; env supplies
        // the ingest token (the secret stays out of the config file).
        let mut env = daemon_env();
        env.push(("SBGH_API_INGEST_TOKEN", "ingest-secret"));
        let _g = EnvGuard::set(&env);
        let f = write(
            "[api]\nlisten = [\"127.0.0.1:9000\", \"172.17.0.1:9000\"]\ncookie_path = \
             \"/etc/sbgh/daemon/.cookie\"\n",
        );
        let cfg = DaemonConfig::load_layered(Some(f.path())).unwrap();
        assert_eq!(cfg.api.listen, vec!["127.0.0.1:9000", "172.17.0.1:9000"]);
        assert_eq!(
            cfg.api
                .ingest_token
                .as_deref(),
            Some("ingest-secret")
        );
    }

    #[test]
    fn daemon_api_ingest_token_in_toml_is_rejected() {
        // The ingest secret is env-only; a TOML `[api].ingest_token` key is
        // an unknown field (hard error), not silently honored.
        let _g = EnvGuard::set(&daemon_env());
        let f = write("[api]\ningest_token = \"should-not-be-in-config\"\n");
        let err = DaemonConfig::load_layered(Some(f.path())).unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    // ─── resolve_config_path ───

    #[test]
    fn resolve_explicit_via_sbgh_config_wins() {
        let _g = EnvGuard::set(&[("SBGH_CONFIG", "/some/explicit/path.toml")]);
        let chosen = resolve_config_path("/etc/sbgh/handler/config.toml", ".config/sbgh/x.toml");
        assert_eq!(chosen, Some(PathBuf::from("/some/explicit/path.toml")));
    }

    #[test]
    fn resolve_returns_none_when_nothing_exists() {
        let _g = EnvGuard::set(&[]);
        // Unset SBGH_CONFIG explicitly in case it leaked from the surrounding shell.
        let _saved = env::var("SBGH_CONFIG").ok();
        unsafe { env::remove_var("SBGH_CONFIG") };
        let chosen = resolve_config_path(
            "/definitely/does/not/exist.toml",
            "almost-certainly-not-here.toml",
        );
        assert!(chosen.is_none());
        if let Some(v) = _saved {
            unsafe { env::set_var("SBGH_CONFIG", v) };
        }
    }
}
