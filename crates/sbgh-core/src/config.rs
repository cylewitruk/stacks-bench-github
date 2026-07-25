//! Configuration.
//!
//! Configuration for the host-side daemon.
//!
//! Loading precedence (lowest → highest):
//!   1. compiled-in defaults
//!   2. TOML config file (see [`DaemonConfig::load`])
//!   3. environment variables (always win)

use std::env;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::memory::MemorySize;
use crate::{Error, Result};

/// Default on-disk path for the daemon's TOML config.
pub const DAEMON_DEFAULT_CONFIG_PATH: &str = "/etc/sbgh/daemon/config.toml";

const DAEMON_HOME_RELATIVE: &str = ".config/sbgh/daemon/config.toml";

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
    pub artifacts: ArtifactsConfig,
    pub slack: SlackConfig,
    pub llm: LlmConfig,
}

/// Daemon run-loop tuning. Deliberately **not** under `[vm]`: the limit is on
/// execution slots, not VM capacity, and not every task must use a VM.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunnerConfig {
    /// Maximum jobs executed concurrently. Default `1` (sequential — the
    /// historical behavior). Raise it only when the host can run that many
    /// jobs at once (each is a full VM today). Values below 1 are clamped to 1.
    pub max_concurrent_jobs: usize,
    /// Maximum daemon-level clean VM repetitions a user request may fan out to.
    /// Request validation applies this cap before enqueue.
    pub max_clean_repetitions: u32,
    /// Maximum comparison variants accepted from Slack/LLM requests.
    pub max_variants: u32,
    /// Maximum measured VM lifecycles a comparison request may imply
    /// (`variants × clean repetitions`).
    pub max_comparison_lifecycles: u32,
    /// Optional CPU pinning, one **libvirt cpuset** per
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

/// The daemon's `/api` server. Reachable only from the local CLI (loopback) and
/// the handler container (Docker bridge), never as a public interface.
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

/// Which surfaces receive a job's benchmark result.
// No `Eq`: `noise_cv_pct: Option<f64>` is only `PartialEq`.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct ReportingConfig {
    /// PR (`/benchmark`) jobs. Default `both` (a Check Run + a summary comment
    /// that links to it).
    pub pr_report: PrReport,
    /// Baseline (`branch_push`/`tag_created`) jobs. Default `check` (a
    /// commit-level Check Run); `none` keeps them headless/DB-only.
    pub baseline_report: BaselineReport,
    /// The measured per-run coefficient of variation of the combined
    /// Execution+Commit metric, as a **percent** (e.g. `0.37`). Drives
    /// the vs-baseline confidence (sigma). `None` → the delta is shown but the
    /// confidence reads "provisional" until the host noise floor is
    /// re-measured.
    pub noise_cv_pct: Option<f64>,
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

/// LLM-backed intent extraction. Disabled by default; when enabled, Slack input
/// can be resolved through a structured OpenAI response. The API key is
/// environment-only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LlmConfig {
    pub enabled: bool,
    pub provider: LlmProvider,
    pub model: String,
    pub input_max_chars: usize,
    pub timeout_secs: u64,
    pub per_user_rate_limit_per_minute: u32,
    /// Env-only (`SBGH_OPENAI_API_KEY`). `Some` iff enabled.
    pub openai_api_key: Option<String>,
}

impl Default for LlmConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            provider: LlmProvider::OpenAi,
            // Small structured-output-capable default; operators can override
            // as newer OpenAI model families land.
            model: "gpt-5-mini".into(),
            input_max_chars: 1_000,
            timeout_secs: 15,
            per_user_rate_limit_per_minute: 5,
            openai_api_key: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LlmProvider {
    #[serde(rename = "openai")]
    #[default]
    OpenAi,
}

impl std::str::FromStr for LlmProvider {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "openai" => Ok(Self::OpenAi),
            other => Err(format!("unknown llm provider: {other} (expected `openai`)")),
        }
    }
}

/// Where run artifacts are stored and fetched. `local` (default) keeps on-disk
/// behavior; `s3` ships artifacts to S3-compatible object storage for off-box
/// fetch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactsConfig {
    pub kind: ArtifactStoreKind,
    /// S3 settings — `Some` iff `kind == S3` (enforced at load).
    pub s3: Option<S3Config>,
    /// Local fingerprint-keyed `stacks-bench` binary cache.
    pub binary_cache: BinaryCacheConfig,
}

impl Default for ArtifactsConfig {
    /// Local FS, today's behavior — the default when `[artifacts]` is absent.
    fn default() -> Self {
        Self {
            kind: ArtifactStoreKind::Local,
            s3: None,
            binary_cache: BinaryCacheConfig::default(),
        }
    }
}

/// Local cache of built `stacks-bench` binaries. Opt-in: a fingerprint-matched
/// binary is reused instead of rebuilt, skipping the ~5–7 min build VM.
/// Local-only — fleet / S3 sharing is deferred.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BinaryCacheConfig {
    /// Gate the cache — `false` (default) keeps today's always-build behavior.
    pub enabled: bool,
    /// On-disk size budget. Pinned entries are kept past it; non-pinned evict
    /// least-recently-used. Default `10G`.
    pub max_size: MemorySize,
    /// Cache root directory. Default `/var/lib/sbgh/binary-cache`.
    pub dir: PathBuf,
}

impl Default for BinaryCacheConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_size: MemorySize::from_gib(10),
            dir: PathBuf::from("/var/lib/sbgh/binary-cache"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactStoreKind {
    #[default]
    Local,
    S3,
}

impl std::str::FromStr for ArtifactStoreKind {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "local" => Ok(Self::Local),
            "s3" => Ok(Self::S3),
            other => {
                Err(format!("unknown artifact store kind: {other} (expected `local` or `s3`)"))
            }
        }
    }
}

/// S3-compatible endpoint settings. `endpoint`/`bucket`/`region` come from the
/// `[artifacts]` TOML; the credentials are **env-only**
/// (`SBGH_ARTIFACTS_S3_*`), mirroring `api.ingest_token`, so the secret never
/// lands in the config file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct S3Config {
    /// Base endpoint URL, e.g. `https://fsn1.your-objectstorage.com`.
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
}

/// Slack ad-hoc profiling connector. Disabled by default; when `enabled`, the
/// orchestrator opens a Socket Mode connection and
/// serves `@BenchBot` mention benches. The code under test is a **constant**
/// (`default_repository`/`default_rev`); the workload is the variable (the
/// mention's args). Tokens are **env-only**; identities are allowlisted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlackConfig {
    /// Gate the connector — `false` (default) means it never starts, so the
    /// rest of this section is inert. Required fields are only enforced when
    /// `true` (the established per-mode validation pattern).
    pub enabled: bool,
    /// App-level token (`xapp-…`, `connections:write`) authenticating the
    /// Socket Mode connection. **Env-only** (`SBGH_SLACK_APP_TOKEN`).
    /// `Some` iff enabled.
    pub app_token: Option<String>,
    /// Bot token (`xoxb-…`) authorizing Web API calls (post / react / upload).
    /// **Env-only** (`SBGH_SLACK_BOT_TOKEN`). `Some` iff enabled.
    pub bot_token: Option<String>,
    /// The constant code under test: `owner/name` repo a Slack bench runs
    /// against. Resolved to a commit via the existing claim-time path.
    pub default_repository: String,
    /// Default rev (branch/tag/sha) of `default_repository`, overridable per
    /// request via `--rev`.
    pub default_rev: String,
    /// Allowlisted Slack workspace ids (`team_id`) — a mention from any other
    /// workspace is rejected. The authenticated socket says nothing about *who*
    /// sent a command, so every mention is checked against this.
    pub allowed_team_ids: Vec<String>,
    /// Allowlisted Slack user ids permitted to trigger benches.
    pub allowed_user_ids: Vec<String>,
}

impl Default for SlackConfig {
    /// Disabled — the default when `[slack]` is absent.
    fn default() -> Self {
        Self {
            enabled: false,
            app_token: None,
            bot_token: None,
            default_repository: String::new(),
            default_rev: String::new(),
            allowed_team_ids: Vec::new(),
            allowed_user_ids: Vec::new(),
        }
    }
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
    artifacts: RawArtifacts,
    slack: RawSlack,
    llm: RawLlm,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawArtifacts {
    kind: Option<ArtifactStoreKind>,
    endpoint: Option<String>,
    bucket: Option<String>,
    region: Option<String>,
    /// **Env-only** (`SBGH_ARTIFACTS_S3_ACCESS_KEY_ID`) — `#[serde(skip)]` +
    /// `deny_unknown_fields` makes a TOML key a hard error (secret stays out of
    /// the config file), mirroring `api.ingest_token`.
    #[serde(skip)]
    access_key_id: Option<String>,
    /// **Env-only** (`SBGH_ARTIFACTS_S3_SECRET_ACCESS_KEY`).
    #[serde(skip)]
    secret_access_key: Option<String>,
    binary_cache: RawBinaryCache,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawBinaryCache {
    enabled: Option<bool>,
    max_size: Option<MemorySize>,
    dir: Option<PathBuf>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawSlack {
    enabled: Option<bool>,
    default_repository: Option<String>,
    default_rev: Option<String>,
    allowed_team_ids: Option<Vec<String>>,
    allowed_user_ids: Option<Vec<String>>,
    /// **Env-only** (`SBGH_SLACK_APP_TOKEN`) — `#[serde(skip)]` so a TOML key
    /// is a hard error; the `xapp-` secret stays out of the config file.
    #[serde(skip)]
    app_token: Option<String>,
    /// **Env-only** (`SBGH_SLACK_BOT_TOKEN`).
    #[serde(skip)]
    bot_token: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawLlm {
    enabled: Option<bool>,
    provider: Option<LlmProvider>,
    model: Option<String>,
    input_max_chars: Option<usize>,
    timeout_secs: Option<u64>,
    per_user_rate_limit_per_minute: Option<u32>,
    /// **Env-only** (`SBGH_OPENAI_API_KEY`) — never read from TOML.
    #[serde(skip)]
    openai_api_key: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawReporting {
    pr_report: Option<PrReport>,
    baseline_report: Option<BaselineReport>,
    noise_cv_pct: Option<f64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawRunner {
    max_concurrent_jobs: Option<usize>,
    max_clean_repetitions: Option<u32>,
    max_variants: Option<u32>,
    max_comparison_lifecycles: Option<u32>,
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
        merge_opt(&mut self.reporting.noise_cv_pct, other.reporting.noise_cv_pct);

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
        merge_opt(
            &mut self
                .runner
                .max_clean_repetitions,
            other
                .runner
                .max_clean_repetitions,
        );
        merge_opt(&mut self.runner.max_variants, other.runner.max_variants);
        merge_opt(
            &mut self
                .runner
                .max_comparison_lifecycles,
            other
                .runner
                .max_comparison_lifecycles,
        );

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

        merge_opt(&mut self.artifacts.kind, other.artifacts.kind);
        merge_opt(&mut self.artifacts.endpoint, other.artifacts.endpoint);
        merge_opt(&mut self.artifacts.bucket, other.artifacts.bucket);
        merge_opt(&mut self.artifacts.region, other.artifacts.region);
        // `artifacts.{access_key_id,secret_access_key}` are env-only.
        merge_opt(
            &mut self
                .artifacts
                .binary_cache
                .enabled,
            other
                .artifacts
                .binary_cache
                .enabled,
        );
        merge_opt(
            &mut self
                .artifacts
                .binary_cache
                .max_size,
            other
                .artifacts
                .binary_cache
                .max_size,
        );
        merge_opt(
            &mut self
                .artifacts
                .binary_cache
                .dir,
            other
                .artifacts
                .binary_cache
                .dir,
        );

        merge_opt(&mut self.slack.enabled, other.slack.enabled);
        merge_opt(&mut self.slack.default_repository, other.slack.default_repository);
        merge_opt(&mut self.slack.default_rev, other.slack.default_rev);
        merge_opt(&mut self.slack.allowed_team_ids, other.slack.allowed_team_ids);
        merge_opt(&mut self.slack.allowed_user_ids, other.slack.allowed_user_ids);
        // `slack.{app_token,bot_token}` are env-only.

        merge_opt(&mut self.llm.enabled, other.llm.enabled);
        merge_opt(&mut self.llm.provider, other.llm.provider);
        merge_opt(&mut self.llm.model, other.llm.model);
        merge_opt(&mut self.llm.input_max_chars, other.llm.input_max_chars);
        merge_opt(&mut self.llm.timeout_secs, other.llm.timeout_secs);
        merge_opt(
            &mut self
                .llm
                .per_user_rate_limit_per_minute,
            other
                .llm
                .per_user_rate_limit_per_minute,
        );
        // `llm.openai_api_key` is env-only.
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
        env_parse_into(
            &mut self
                .runner
                .max_clean_repetitions,
            "SBGH_RUNNER_MAX_CLEAN_REPETITIONS",
        );
        env_parse_into(&mut self.runner.max_variants, "SBGH_RUNNER_MAX_VARIANTS");
        env_parse_into(
            &mut self
                .runner
                .max_comparison_lifecycles,
            "SBGH_RUNNER_MAX_COMPARISON_LIFECYCLES",
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

        env_parse_into(&mut self.artifacts.kind, "SBGH_ARTIFACTS_KIND");
        env_into(&mut self.artifacts.endpoint, "SBGH_ARTIFACTS_S3_ENDPOINT");
        env_into(&mut self.artifacts.bucket, "SBGH_ARTIFACTS_S3_BUCKET");
        env_into(&mut self.artifacts.region, "SBGH_ARTIFACTS_S3_REGION");
        env_into(&mut self.artifacts.access_key_id, "SBGH_ARTIFACTS_S3_ACCESS_KEY_ID");
        env_into(
            &mut self
                .artifacts
                .secret_access_key,
            "SBGH_ARTIFACTS_S3_SECRET_ACCESS_KEY",
        );
        env_parse_into(
            &mut self
                .artifacts
                .binary_cache
                .enabled,
            "SBGH_ARTIFACTS_BINARY_CACHE_ENABLED",
        );
        env_parse_into(
            &mut self
                .artifacts
                .binary_cache
                .max_size,
            "SBGH_ARTIFACTS_BINARY_CACHE_MAX_SIZE",
        );
        env_path_into(
            &mut self
                .artifacts
                .binary_cache
                .dir,
            "SBGH_ARTIFACTS_BINARY_CACHE_DIR",
        );

        env_parse_into(&mut self.slack.enabled, "SBGH_SLACK_ENABLED");
        env_into(&mut self.slack.default_repository, "SBGH_SLACK_DEFAULT_REPOSITORY");
        env_into(&mut self.slack.default_rev, "SBGH_SLACK_DEFAULT_REV");
        env_csv_into(&mut self.slack.allowed_team_ids, "SBGH_SLACK_ALLOWED_TEAM_IDS");
        env_csv_into(&mut self.slack.allowed_user_ids, "SBGH_SLACK_ALLOWED_USER_IDS");
        env_into(&mut self.slack.app_token, "SBGH_SLACK_APP_TOKEN");
        env_into(&mut self.slack.bot_token, "SBGH_SLACK_BOT_TOKEN");

        env_parse_into(&mut self.llm.enabled, "SBGH_LLM_ENABLED");
        env_parse_into(&mut self.llm.provider, "SBGH_LLM_PROVIDER");
        env_into(&mut self.llm.model, "SBGH_LLM_MODEL");
        env_parse_into(&mut self.llm.input_max_chars, "SBGH_LLM_INPUT_MAX_CHARS");
        env_parse_into(&mut self.llm.timeout_secs, "SBGH_LLM_TIMEOUT_SECS");
        env_parse_into(
            &mut self
                .llm
                .per_user_rate_limit_per_minute,
            "SBGH_LLM_PER_USER_RATE_LIMIT_PER_MINUTE",
        );
        env_into(&mut self.llm.openai_api_key, "SBGH_OPENAI_API_KEY");
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
                noise_cv_pct: self.reporting.noise_cv_pct,
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
                    max_clean_repetitions: self
                        .runner
                        .max_clean_repetitions
                        .unwrap_or(5)
                        .max(1),
                    max_variants: self
                        .runner
                        .max_variants
                        .unwrap_or(2)
                        .max(1),
                    max_comparison_lifecycles: self
                        .runner
                        .max_comparison_lifecycles
                        .unwrap_or(10)
                        .max(1),
                    cpu_sets,
                    host_cpus: self.runner.host_cpus,
                }
            },
            artifacts: {
                let kind = self
                    .artifacts
                    .kind
                    .unwrap_or_default();
                // `s3` settings are required iff `kind = s3`; for `local` any
                // stray S3 keys are simply ignored (no store to apply them to).
                let s3 = match kind {
                    ArtifactStoreKind::Local => None,
                    ArtifactStoreKind::S3 => Some(S3Config {
                        endpoint: required(
                            self.artifacts.endpoint,
                            "[artifacts].endpoint (kind = s3)",
                        )?,
                        bucket: required(self.artifacts.bucket, "[artifacts].bucket (kind = s3)")?,
                        region: required(self.artifacts.region, "[artifacts].region (kind = s3)")?,
                        access_key_id: required(
                            self.artifacts.access_key_id,
                            "SBGH_ARTIFACTS_S3_ACCESS_KEY_ID (kind = s3)",
                        )?,
                        secret_access_key: required(
                            self.artifacts
                                .secret_access_key,
                            "SBGH_ARTIFACTS_S3_SECRET_ACCESS_KEY (kind = s3)",
                        )?,
                    }),
                };
                let raw_bc = self.artifacts.binary_cache;
                let default_bc = BinaryCacheConfig::default();
                let binary_cache = BinaryCacheConfig {
                    enabled: raw_bc
                        .enabled
                        .unwrap_or(default_bc.enabled),
                    max_size: raw_bc
                        .max_size
                        .unwrap_or(default_bc.max_size),
                    dir: raw_bc
                        .dir
                        .unwrap_or(default_bc.dir),
                };
                ArtifactsConfig { kind, s3, binary_cache }
            },
            slack: {
                let enabled = self
                    .slack
                    .enabled
                    .unwrap_or(false);
                // Required fields are enforced only when the connector is on,
                // so a disabled (default) section needs nothing — same per-mode
                // shape as `[artifacts]`. An enabled connector with an empty
                // allowlist would accept nobody, so require at least one id.
                if enabled {
                    let team_ids = normalize_ids(
                        self.slack
                            .allowed_team_ids
                            .unwrap_or_default(),
                    );
                    let user_ids = normalize_ids(
                        self.slack
                            .allowed_user_ids
                            .unwrap_or_default(),
                    );
                    if team_ids.is_empty() || user_ids.is_empty() {
                        return Err(Error::Config(
                            "[slack].enabled = true requires non-empty allowed_team_ids and \
                             allowed_user_ids (an empty allowlist authorizes nobody)"
                                .into(),
                        ));
                    }
                    SlackConfig {
                        enabled,
                        app_token: Some(required(
                            self.slack.app_token,
                            "SBGH_SLACK_APP_TOKEN (slack enabled)",
                        )?),
                        bot_token: Some(required(
                            self.slack.bot_token,
                            "SBGH_SLACK_BOT_TOKEN (slack enabled)",
                        )?),
                        default_repository: required(
                            self.slack.default_repository,
                            "[slack].default_repository (slack enabled)",
                        )?,
                        default_rev: required(
                            self.slack.default_rev,
                            "[slack].default_rev (slack enabled)",
                        )?,
                        allowed_team_ids: team_ids,
                        allowed_user_ids: user_ids,
                    }
                } else {
                    SlackConfig::default()
                }
            },
            llm: {
                let raw = self.llm;
                let default = LlmConfig::default();
                let enabled = raw
                    .enabled
                    .unwrap_or(default.enabled);
                let provider = raw
                    .provider
                    .unwrap_or(default.provider);
                let model = raw
                    .model
                    .unwrap_or(default.model);
                let input_max_chars = raw
                    .input_max_chars
                    .unwrap_or(default.input_max_chars);
                let timeout_secs = raw
                    .timeout_secs
                    .unwrap_or(default.timeout_secs);
                let per_user_rate_limit_per_minute = raw
                    .per_user_rate_limit_per_minute
                    .unwrap_or(default.per_user_rate_limit_per_minute);
                let openai_api_key = if enabled {
                    Some(required(raw.openai_api_key, "SBGH_OPENAI_API_KEY (llm enabled)")?)
                } else {
                    raw.openai_api_key
                };
                LlmConfig {
                    enabled,
                    provider,
                    model,
                    input_max_chars,
                    timeout_secs,
                    per_user_rate_limit_per_minute,
                    openai_api_key,
                }
            },
        })
    }
}

// ─────────────────────────── Shared helpers ───────────────────────────

/// Trim each id and drop blank entries, so a TOML `["", "  "]` (which `env_csv`
/// already filters for env input) collapses to empty rather than passing as a
/// "non-empty" allowlist that authorizes nobody.
fn normalize_ids(ids: Vec<String>) -> Vec<String> {
    ids.into_iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

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
        assert_eq!(
            cfg.runner
                .max_clean_repetitions,
            5
        );
        assert_eq!(cfg.runner.max_variants, 2);
        assert_eq!(
            cfg.runner
                .max_comparison_lifecycles,
            10
        );
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
        assert_eq!(
            cfg.runner
                .max_clean_repetitions,
            5
        );
        assert_eq!(cfg.runner.max_variants, 2);
        assert_eq!(
            cfg.runner
                .max_comparison_lifecycles,
            10
        );
        assert!(cfg.runner.cpu_sets.is_empty(), "no pinning by default");
        assert!(cfg.runner.host_cpus.is_none());
    }

    #[test]
    fn daemon_runner_clean_repetition_cap_toml_then_env_override() {
        let mut env = daemon_env();
        env.push(("SBGH_RUNNER_MAX_CLEAN_REPETITIONS", "7"));
        let _g = EnvGuard::set(&env);
        let f = write("[runner]\nmax_clean_repetitions = 3\n");
        let cfg = DaemonConfig::load_layered(Some(f.path())).unwrap();
        assert_eq!(
            cfg.runner
                .max_clean_repetitions,
            7
        );
    }

    #[test]
    fn daemon_runner_comparison_caps_toml_then_env_override() {
        let mut env = daemon_env();
        env.push(("SBGH_RUNNER_MAX_VARIANTS", "4"));
        env.push(("SBGH_RUNNER_MAX_COMPARISON_LIFECYCLES", "12"));
        let _g = EnvGuard::set(&env);
        let f = write("[runner]\nmax_variants = 3\nmax_comparison_lifecycles = 8\n");
        let cfg = DaemonConfig::load_layered(Some(f.path())).unwrap();
        assert_eq!(cfg.runner.max_variants, 4);
        assert_eq!(
            cfg.runner
                .max_comparison_lifecycles,
            12
        );
    }

    #[test]
    fn daemon_binary_cache_defaults_off() {
        let _g = EnvGuard::set(&daemon_env());
        let cfg = DaemonConfig::load_layered(None).unwrap();
        let bc = &cfg.artifacts.binary_cache;
        assert!(!bc.enabled, "binary cache is opt-in (default off)");
        assert_eq!(bc.max_size, crate::memory::MemorySize::from_gib(10));
        assert_eq!(bc.dir, std::path::PathBuf::from("/var/lib/sbgh/binary-cache"));
    }

    #[test]
    fn daemon_binary_cache_toml_then_env_override() {
        let mut env = daemon_env();
        env.push(("SBGH_ARTIFACTS_BINARY_CACHE_MAX_SIZE", "20G"));
        let _g = EnvGuard::set(&env);
        let f = write(
            "[artifacts.binary_cache]\nenabled = true\nmax_size = \"5G\"\ndir = \"/srv/cache\"\n",
        );
        let cfg = DaemonConfig::load_layered(Some(f.path())).unwrap();
        let bc = &cfg.artifacts.binary_cache;
        assert!(bc.enabled, "TOML enables the cache");
        // Env wins over TOML for max_size; dir falls through from TOML.
        assert_eq!(bc.max_size, crate::memory::MemorySize::from_gib(20));
        assert_eq!(bc.dir, std::path::PathBuf::from("/srv/cache"));
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

    // ─── ArtifactsConfig ───

    #[test]
    fn daemon_artifacts_defaults_to_local() {
        let _g = EnvGuard::set(&daemon_env());
        let cfg = DaemonConfig::load_layered(None).unwrap();
        assert_eq!(cfg.artifacts.kind, ArtifactStoreKind::Local);
        assert!(cfg.artifacts.s3.is_none(), "no S3 settings under local");
    }

    #[test]
    fn daemon_artifacts_s3_from_toml_and_env() {
        // endpoint/bucket/region from TOML; credentials env-only (out of the
        // config file).
        let mut env = daemon_env();
        env.push(("SBGH_ARTIFACTS_S3_ACCESS_KEY_ID", "AKIAEXAMPLE"));
        env.push(("SBGH_ARTIFACTS_S3_SECRET_ACCESS_KEY", "s3cr3t"));
        let _g = EnvGuard::set(&env);
        let f = write(
            "[artifacts]\nkind = \"s3\"\nendpoint = \"https://fsn1.example.com\"\nbucket = \
             \"sbgh-artifacts\"\nregion = \"fsn1\"\n",
        );
        let cfg = DaemonConfig::load_layered(Some(f.path())).unwrap();
        assert_eq!(cfg.artifacts.kind, ArtifactStoreKind::S3);
        let s3 = cfg
            .artifacts
            .s3
            .expect("s3 settings present");
        assert_eq!(s3.endpoint, "https://fsn1.example.com");
        assert_eq!(s3.bucket, "sbgh-artifacts");
        assert_eq!(s3.region, "fsn1");
        assert_eq!(s3.access_key_id, "AKIAEXAMPLE");
        assert_eq!(s3.secret_access_key, "s3cr3t");
    }

    #[test]
    fn daemon_artifacts_kind_via_env_only() {
        // A fully env-driven deployment (no TOML) can still opt into S3.
        let mut env = daemon_env();
        env.push(("SBGH_ARTIFACTS_KIND", "s3"));
        env.push(("SBGH_ARTIFACTS_S3_ENDPOINT", "https://s3.example.com"));
        env.push(("SBGH_ARTIFACTS_S3_BUCKET", "b"));
        env.push(("SBGH_ARTIFACTS_S3_REGION", "r"));
        env.push(("SBGH_ARTIFACTS_S3_ACCESS_KEY_ID", "k"));
        env.push(("SBGH_ARTIFACTS_S3_SECRET_ACCESS_KEY", "s"));
        let _g = EnvGuard::set(&env);
        let cfg = DaemonConfig::load_layered(None).unwrap();
        assert_eq!(cfg.artifacts.kind, ArtifactStoreKind::S3);
        assert_eq!(
            cfg.artifacts
                .s3
                .unwrap()
                .bucket,
            "b"
        );
    }

    #[test]
    fn daemon_artifacts_s3_missing_required_field_errors() {
        // kind = s3 but no bucket → hard error (per-kind validation).
        let mut env = daemon_env();
        env.push(("SBGH_ARTIFACTS_S3_ACCESS_KEY_ID", "k"));
        env.push(("SBGH_ARTIFACTS_S3_SECRET_ACCESS_KEY", "s"));
        let _g = EnvGuard::set(&env);
        let f = write("[artifacts]\nkind = \"s3\"\nendpoint = \"https://s3.example.com\"\n");
        let err = DaemonConfig::load_layered(Some(f.path())).unwrap_err();
        assert!(matches!(err, Error::Config(_)), "got: {err:?}");
    }

    #[test]
    fn daemon_artifacts_s3_credentials_in_toml_are_rejected() {
        // The S3 secret is env-only; a TOML `access_key_id` key is an unknown
        // field (hard error), keeping the secret out of the config file.
        let _g = EnvGuard::set(&daemon_env());
        let f = write("[artifacts]\nkind = \"s3\"\naccess_key_id = \"leaked\"\n");
        let err = DaemonConfig::load_layered(Some(f.path())).unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    // ─── SlackConfig ───

    #[test]
    fn daemon_slack_defaults_to_disabled() {
        let _g = EnvGuard::set(&daemon_env());
        let cfg = DaemonConfig::load_layered(None).unwrap();
        assert!(!cfg.slack.enabled);
        assert!(cfg.slack.app_token.is_none());
        assert!(cfg.slack.bot_token.is_none());
    }

    #[test]
    fn daemon_slack_enabled_from_toml_and_env() {
        // repo/rev/allowlists from TOML; tokens env-only.
        let mut env = daemon_env();
        env.push(("SBGH_SLACK_APP_TOKEN", "xapp-abc"));
        env.push(("SBGH_SLACK_BOT_TOKEN", "xoxb-def"));
        let _g = EnvGuard::set(&env);
        let f = write(
            "[slack]\nenabled = true\ndefault_repository = \
             \"stacks-network/stacks-core\"\ndefault_rev = \"develop\"\nallowed_team_ids = \
             [\"T1\"]\nallowed_user_ids = [\"U1\", \"U2\"]\n",
        );
        let cfg = DaemonConfig::load_layered(Some(f.path())).unwrap();
        assert!(cfg.slack.enabled);
        assert_eq!(cfg.slack.default_repository, "stacks-network/stacks-core");
        assert_eq!(cfg.slack.default_rev, "develop");
        assert_eq!(cfg.slack.allowed_team_ids, vec!["T1"]);
        assert_eq!(cfg.slack.allowed_user_ids, vec!["U1", "U2"]);
        assert_eq!(cfg.slack.app_token.as_deref(), Some("xapp-abc"));
        assert_eq!(cfg.slack.bot_token.as_deref(), Some("xoxb-def"));
    }

    #[test]
    fn daemon_slack_enabled_missing_token_errors() {
        // enabled but no app token (env unset) → hard error.
        let _g = EnvGuard::set(&daemon_env());
        let f = write(
            "[slack]\nenabled = true\ndefault_repository = \"o/r\"\ndefault_rev = \
             \"develop\"\nallowed_team_ids = [\"T1\"]\nallowed_user_ids = [\"U1\"]\n",
        );
        let err = DaemonConfig::load_layered(Some(f.path())).unwrap_err();
        assert!(matches!(err, Error::Config(_)), "got: {err:?}");
    }

    #[test]
    fn daemon_slack_enabled_empty_allowlist_errors() {
        // An enabled connector with no allowlisted ids would authorize nobody.
        let mut env = daemon_env();
        env.push(("SBGH_SLACK_APP_TOKEN", "xapp-abc"));
        env.push(("SBGH_SLACK_BOT_TOKEN", "xoxb-def"));
        let _g = EnvGuard::set(&env);
        let f = write(
            "[slack]\nenabled = true\ndefault_repository = \"o/r\"\ndefault_rev = \
             \"develop\"\nallowed_team_ids = [\"T1\"]\n",
        );
        let err = DaemonConfig::load_layered(Some(f.path())).unwrap_err();
        assert!(matches!(err, Error::Config(_)), "got: {err:?}");
    }

    #[test]
    fn daemon_slack_disabled_ignores_missing_fields() {
        // Disabled (or absent) → no required-field enforcement.
        let _g = EnvGuard::set(&daemon_env());
        let f = write("[slack]\nenabled = false\n");
        let cfg = DaemonConfig::load_layered(Some(f.path())).unwrap();
        assert!(!cfg.slack.enabled);
    }

    #[test]
    fn daemon_slack_blank_allowlist_entry_is_rejected() {
        // A TOML `[""]` / `["  "]` must not pass as a non-empty allowlist (it
        // authorizes nobody); blanks are trimmed away → empty → error.
        let mut env = daemon_env();
        env.push(("SBGH_SLACK_APP_TOKEN", "xapp-abc"));
        env.push(("SBGH_SLACK_BOT_TOKEN", "xoxb-def"));
        let _g = EnvGuard::set(&env);
        let f = write(
            "[slack]\nenabled = true\ndefault_repository = \"o/r\"\ndefault_rev = \
             \"develop\"\nallowed_team_ids = [\"T1\"]\nallowed_user_ids = [\"\", \"   \"]\n",
        );
        let err = DaemonConfig::load_layered(Some(f.path())).unwrap_err();
        assert!(matches!(err, Error::Config(_)), "got: {err:?}");
    }

    // ─── LlmConfig ───

    #[test]
    fn daemon_llm_defaults_to_disabled() {
        let _g = EnvGuard::set(&daemon_env());
        let cfg = DaemonConfig::load_layered(None).unwrap();
        assert!(!cfg.llm.enabled);
        assert_eq!(cfg.llm.provider, LlmProvider::OpenAi);
        assert_eq!(cfg.llm.model, "gpt-5-mini");
        assert!(
            cfg.llm
                .openai_api_key
                .is_none()
        );
    }

    #[test]
    fn daemon_llm_enabled_from_toml_and_env() {
        let mut env = daemon_env();
        env.push(("SBGH_OPENAI_API_KEY", "sk-test"));
        let _g = EnvGuard::set(&env);
        let f = write(
            "[llm]\nenabled = true\nprovider = \"openai\"\nmodel = \"gpt-test\"\ninput_max_chars \
             = 500\ntimeout_secs = 7\nper_user_rate_limit_per_minute = 3\n",
        );
        let cfg = DaemonConfig::load_layered(Some(f.path())).unwrap();
        assert!(cfg.llm.enabled);
        assert_eq!(cfg.llm.provider, LlmProvider::OpenAi);
        assert_eq!(cfg.llm.model, "gpt-test");
        assert_eq!(cfg.llm.input_max_chars, 500);
        assert_eq!(cfg.llm.timeout_secs, 7);
        assert_eq!(
            cfg.llm
                .per_user_rate_limit_per_minute,
            3
        );
        assert_eq!(
            cfg.llm
                .openai_api_key
                .as_deref(),
            Some("sk-test")
        );
    }

    #[test]
    fn daemon_llm_enabled_missing_key_errors() {
        let _g = EnvGuard::set(&daemon_env());
        let f = write("[llm]\nenabled = true\n");
        let err = DaemonConfig::load_layered(Some(f.path())).unwrap_err();
        assert!(matches!(err, Error::Config(_)), "got: {err:?}");
    }

    #[test]
    fn daemon_llm_openai_key_in_toml_is_rejected() {
        let _g = EnvGuard::set(&daemon_env());
        let f = write("[llm]\nopenai_api_key = \"should-not-be-in-config\"\n");
        let err = DaemonConfig::load_layered(Some(f.path())).unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn daemon_slack_allowlist_ids_are_trimmed() {
        // Surrounding whitespace is stripped and stored normalized.
        let mut env = daemon_env();
        env.push(("SBGH_SLACK_APP_TOKEN", "xapp-abc"));
        env.push(("SBGH_SLACK_BOT_TOKEN", "xoxb-def"));
        let _g = EnvGuard::set(&env);
        let f = write(
            "[slack]\nenabled = true\ndefault_repository = \"o/r\"\ndefault_rev = \
             \"develop\"\nallowed_team_ids = [\"  T1  \"]\nallowed_user_ids = [\"U1\", \"  U2 \
             \"]\n",
        );
        let cfg = DaemonConfig::load_layered(Some(f.path())).unwrap();
        assert_eq!(cfg.slack.allowed_team_ids, vec!["T1"]);
        assert_eq!(cfg.slack.allowed_user_ids, vec!["U1", "U2"]);
    }

    #[test]
    fn daemon_slack_token_in_toml_is_rejected() {
        // The app/bot tokens are env-only; a TOML key is an unknown field.
        let _g = EnvGuard::set(&daemon_env());
        let f = write("[slack]\nenabled = true\napp_token = \"xapp-leaked\"\n");
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
