//! Configuration.
//!
//! Loading precedence (lowest → highest):
//!   1. compiled-in defaults
//!   2. TOML config file. Path resolution:
//!        - `$SBGH_CONFIG` if set (used verbatim; missing file is silently
//!          treated as env-only, no fallback)
//!        - else `/etc/sbgh/config.toml` if it exists
//!        - else `$HOME/.config/sbgh/config.toml` if it exists
//!        - else: no file layer, defaults + env only
//!   3. environment variables (always win; intended for operational overrides
//!      and for keeping secrets out of source-controlled files)
//!
//! Any field can live in either the TOML file or env. The convention is to
//! put secrets (webhook secret, private key path, DB URL) in env when the
//! TOML is checked in, and in the TOML when it lives outside the repo at
//! mode 0600. The loader doesn't enforce a split.

use std::collections::HashSet;
use std::env;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::{Error, Result};

/// Paths the loader checks when `SBGH_CONFIG` is not set, in order. First
/// existing file wins. System path comes before the per-user XDG path so a
/// host with both gets the system one; if neither is present we fall back to
/// env-only.
const DEFAULT_CONFIG_PATHS: &[&str] = &["/etc/sbgh/config.toml"];
const HOME_CONFIG_RELATIVE: &str = ".config/sbgh/config.toml";

// ─────────────────────────── Public config struct ───────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub github: GitHubConfig,
    pub authorization: AuthorizationConfig,
    pub vm: VmConfig,
    pub paths: PathsConfig,
    pub lvm: LvmConfig,
    pub stacks_bench: StacksBenchConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub bind_addr: String,
    pub database_url: String,
    /// OS user the orchestrator runs as. Used to chown the per-job source
    /// mount.
    pub service_user: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitHubConfig {
    /// GitHub App **Client ID** (e.g. `Iv23li...`) — used as the JWT `iss`
    /// claim when minting installation tokens. Both Client ID and the older
    /// numeric App ID work at GitHub today; we standardise on Client ID per
    /// GitHub's 2024 recommendation.
    pub client_id: String,
    pub api_base_url: String,
    pub private_key_path: PathBuf,
    /// Webhook signing secret. May come from either the TOML file or the
    /// `SBGH_GH_WEBHOOK_SECRET` env var; env wins on collision.
    pub webhook_secret: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AuthorizationConfig {
    pub allowed_repositories: HashSet<String>,
    pub allowed_users: HashSet<String>,
    pub allowed_associations: HashSet<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VmConfig {
    pub golden_image: PathBuf,
    pub vcpus: u32,
    pub memory_gib: u32,
    pub boot_disk_gib: u32,
    pub job_timeout_secs: u64,
    /// libvirt network to attach to the VM (default: `default`, NAT outbound).
    pub network: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PathsConfig {
    pub jobs_dir: PathBuf,
    pub git_mirror: PathBuf,
    /// tmpfs root where per-job results directories are mounted on the host.
    pub results_tmpfs_root: PathBuf,
    /// Persistent destination for `run.sqlite` after a job completes.
    pub results_archive_dir: PathBuf,
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
    /// - `None` (default): the snapshot is created **without** `-L`. This is
    ///   correct for **thin** snapshots of a thin-pool origin — the snapshot
    ///   shares the pool and grows on demand.
    /// - `Some(n)`: thick snapshot with an `n GiB` COW exception store. Use
    ///   only when the base chainstate LV is a thick (non-thin) volume.
    ///
    /// Passing a size against a thin origin would create a thick snapshot of
    /// a thin volume, defeating the cheap-snapshot property and (on some
    /// lvm2 versions) erroring.
    pub chainstate_snapshot_size_gib: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StacksBenchConfig {
    /// Placeholder until we lock down the `/benchmark` arg shape — threaded
    /// verbatim into the cloud-init startup script.
    pub default_args: String,
}

// ─────────────────────────── Loader ───────────────────────────

impl Config {
    pub fn load() -> Result<Self> {
        let explicit = env::var("SBGH_CONFIG").ok();
        let mut search = Vec::with_capacity(DEFAULT_CONFIG_PATHS.len() + 1);
        for p in DEFAULT_CONFIG_PATHS {
            search.push(PathBuf::from(p));
        }
        if let Some(home) = env::var_os("HOME") {
            search.push(PathBuf::from(home).join(HOME_CONFIG_RELATIVE));
        }
        let path = pick_config_path(explicit.as_deref(), &search);
        Self::load_layered(path.as_deref())
    }

    /// Test-friendly entry: explicit path (or `None` to skip file layer).
    pub fn load_layered(file: Option<&std::path::Path>) -> Result<Self> {
        let mut raw: RawConfig = RawConfig::default();

        if let Some(p) = file
            && p.exists()
        {
            let body = std::fs::read_to_string(p)?;
            let from_file: RawConfig = toml::from_str(&body)
                .map_err(|e| Error::Config(format!("parsing {}: {e}", p.display())))?;
            raw.merge(from_file);
        }

        raw.apply_env();
        raw.into_config()
    }
}

// ─────────────────────────── Internal layered shape
// ───────────────────────────
//
// Every field is Optional so layers can compose. `into_config` checks required
// fields once at the end.

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawConfig {
    server: RawServer,
    github: RawGitHub,
    authorization: RawAuth,
    vm: RawVm,
    paths: RawPaths,
    lvm: RawLvm,
    stacks_bench: RawStacksBench,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawServer {
    bind_addr: Option<String>,
    database_url: Option<String>,
    service_user: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawGitHub {
    client_id: Option<String>,
    api_base_url: Option<String>,
    private_key_path: Option<PathBuf>,
    webhook_secret: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawAuth {
    allowed_repositories: Option<Vec<String>>,
    allowed_users: Option<Vec<String>>,
    allowed_associations: Option<Vec<String>>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawVm {
    golden_image: Option<PathBuf>,
    vcpus: Option<u32>,
    memory_gib: Option<u32>,
    boot_disk_gib: Option<u32>,
    job_timeout_secs: Option<u64>,
    network: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawPaths {
    jobs_dir: Option<PathBuf>,
    git_mirror: Option<PathBuf>,
    results_tmpfs_root: Option<PathBuf>,
    results_archive_dir: Option<PathBuf>,
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

impl RawConfig {
    fn merge(&mut self, other: RawConfig) {
        merge_opt(&mut self.server.bind_addr, other.server.bind_addr);
        merge_opt(&mut self.server.database_url, other.server.database_url);
        merge_opt(&mut self.server.service_user, other.server.service_user);

        merge_opt(&mut self.github.client_id, other.github.client_id);
        merge_opt(&mut self.github.api_base_url, other.github.api_base_url);
        merge_opt(&mut self.github.private_key_path, other.github.private_key_path);
        merge_opt(&mut self.github.webhook_secret, other.github.webhook_secret);

        merge_opt(
            &mut self
                .authorization
                .allowed_repositories,
            other
                .authorization
                .allowed_repositories,
        );
        merge_opt(
            &mut self
                .authorization
                .allowed_users,
            other
                .authorization
                .allowed_users,
        );
        merge_opt(
            &mut self
                .authorization
                .allowed_associations,
            other
                .authorization
                .allowed_associations,
        );

        merge_opt(&mut self.vm.golden_image, other.vm.golden_image);
        merge_opt(&mut self.vm.vcpus, other.vm.vcpus);
        merge_opt(&mut self.vm.memory_gib, other.vm.memory_gib);
        merge_opt(&mut self.vm.boot_disk_gib, other.vm.boot_disk_gib);
        merge_opt(&mut self.vm.job_timeout_secs, other.vm.job_timeout_secs);
        merge_opt(&mut self.vm.network, other.vm.network);

        merge_opt(&mut self.paths.jobs_dir, other.paths.jobs_dir);
        merge_opt(&mut self.paths.git_mirror, other.paths.git_mirror);
        merge_opt(&mut self.paths.results_tmpfs_root, other.paths.results_tmpfs_root);
        merge_opt(
            &mut self.paths.results_archive_dir,
            other
                .paths
                .results_archive_dir,
        );
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
    }

    fn apply_env(&mut self) {
        // server
        env_into(&mut self.server.bind_addr, "SBGH_BIND_ADDR");
        env_into(&mut self.server.database_url, "DATABASE_URL");
        env_into(&mut self.server.service_user, "SBGH_SERVICE_USER");

        // github
        env_into(&mut self.github.client_id, "SBGH_GH_CLIENT_ID");
        env_into(&mut self.github.api_base_url, "SBGH_GH_API_BASE_URL");
        env_path_into(&mut self.github.private_key_path, "SBGH_GH_PRIVATE_KEY_PATH");
        env_into(&mut self.github.webhook_secret, "SBGH_GH_WEBHOOK_SECRET");

        // authorization (CSV in env)
        env_csv_into(
            &mut self
                .authorization
                .allowed_repositories,
            "SBGH_ALLOWED_REPOS",
        );
        env_csv_into(
            &mut self
                .authorization
                .allowed_users,
            "SBGH_ALLOWED_USERS",
        );
        env_csv_into(
            &mut self
                .authorization
                .allowed_associations,
            "SBGH_ALLOWED_ASSOCIATIONS",
        );

        // vm
        env_path_into(&mut self.vm.golden_image, "SBGH_VM_GOLDEN_IMAGE");
        env_parse_into(&mut self.vm.vcpus, "SBGH_VM_VCPUS");
        env_parse_into(&mut self.vm.memory_gib, "SBGH_VM_MEMORY_GIB");
        env_parse_into(&mut self.vm.boot_disk_gib, "SBGH_VM_BOOT_DISK_GIB");
        env_parse_into(&mut self.vm.job_timeout_secs, "SBGH_VM_JOB_TIMEOUT_SECS");
        env_into(&mut self.vm.network, "SBGH_VM_NETWORK");

        // paths
        env_path_into(&mut self.paths.jobs_dir, "SBGH_JOBS_DIR");
        env_path_into(&mut self.paths.git_mirror, "SBGH_GIT_MIRROR");
        env_path_into(&mut self.paths.results_tmpfs_root, "SBGH_RESULTS_TMPFS_ROOT");
        env_path_into(&mut self.paths.results_archive_dir, "SBGH_RESULTS_ARCHIVE_DIR");
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

        // lvm
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

        // stacks-bench
        env_into(&mut self.stacks_bench.default_args, "SBGH_STACKS_BENCH_ARGS");
    }

    fn into_config(self) -> Result<Config> {
        Ok(Config {
            server: ServerConfig {
                bind_addr: self
                    .server
                    .bind_addr
                    .unwrap_or_else(|| "0.0.0.0:8080".into()),
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
                    "SBGH_GH_PRIVATE_KEY_PATH",
                )?,
                webhook_secret: required(self.github.webhook_secret, "SBGH_GH_WEBHOOK_SECRET")?,
            },
            authorization: AuthorizationConfig {
                allowed_repositories: self
                    .authorization
                    .allowed_repositories
                    .map(set_from)
                    .unwrap_or_default(),
                allowed_users: self
                    .authorization
                    .allowed_users
                    .map(set_from)
                    .unwrap_or_default(),
                allowed_associations: self
                    .authorization
                    .allowed_associations
                    .map(set_from)
                    .unwrap_or_else(|| {
                        ["OWNER", "MEMBER", "COLLABORATOR"]
                            .iter()
                            .map(|s| s.to_string())
                            .collect()
                    }),
            },
            vm: VmConfig {
                golden_image: required(self.vm.golden_image, "[vm].golden_image")?,
                vcpus: self.vm.vcpus.unwrap_or(2),
                memory_gib: self
                    .vm
                    .memory_gib
                    .unwrap_or(8),
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
                // Propagate as-is. `None` (the default) means "thin snapshot,
                // no -L"; see the field doc on LvmConfig.
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
        })
    }
}

// ─────────────────────────── Small helpers ───────────────────────────

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

fn set_from(v: Vec<String>) -> HashSet<String> {
    v.into_iter().collect()
}

fn required<T>(value: Option<T>, name: &str) -> Result<T> {
    value.ok_or_else(|| Error::Config(format!("missing required config: {name}")))
}

/// Decide which config file (if any) to load, given an optional explicit
/// override and a search-path of fallback locations.
///
/// Precedence:
///   1. `explicit` is honoured verbatim. It's not required to exist — the
///      downstream `load_layered` silently treats a missing file as env-only,
///      so an explicit-but-typo'd path doesn't accidentally hit a fallback.
///   2. Otherwise: first existing file in `search` wins. Pure function; we pass
///      the search list in so this stays unit-testable.
fn pick_config_path(explicit: Option<&str>, search: &[PathBuf]) -> Option<PathBuf> {
    if let Some(p) = explicit {
        return Some(PathBuf::from(p));
    }
    search
        .iter()
        .find(|p| p.exists())
        .cloned()
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

    /// These tests mutate process-wide environment variables. Under nextest
    /// each test runs in its own process so isolation is free, but plain
    /// `cargo test` shares one process across multiple threads, so we must
    /// serialize ourselves. A poisoned mutex (i.e. a test panicked while
    /// holding the lock) is harmless — we just want exclusivity, not
    /// invariants.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Set env vars for a single test, restoring them on drop. Owns the
    /// global env lock for the test's lifetime.
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
                    // Safety: ENV_LOCK ensures no other test mutates env
                    // concurrently for the duration of this guard.
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

    fn required_env_vars() -> Vec<(&'static str, &'static str)> {
        vec![
            ("DATABASE_URL", "postgres://x"),
            ("SBGH_GH_CLIENT_ID", "Iv23litest123"),
            ("SBGH_GH_PRIVATE_KEY_PATH", "/tmp/key.pem"),
            ("SBGH_GH_WEBHOOK_SECRET", "hunter2"),
            ("SBGH_LVM_VG", "sbgh-vg"),
            ("SBGH_LVM_THINPOOL", "thinpool"),
            ("SBGH_VM_GOLDEN_IMAGE", "/var/lib/libvirt/images/golden.qcow2"),
        ]
    }

    #[test]
    fn loads_from_env_only() {
        let _g = EnvGuard::set(&required_env_vars());
        let cfg = Config::load_layered(None).unwrap();
        assert_eq!(cfg.github.client_id, "Iv23litest123");
        assert_eq!(cfg.lvm.vg_name, "sbgh-vg");
        assert_eq!(cfg.vm.vcpus, 2);
        assert_eq!(cfg.vm.memory_gib, 8);
        assert_eq!(cfg.vm.boot_disk_gib, 64);
        assert!(
            cfg.authorization
                .allowed_associations
                .contains("OWNER")
        );
    }

    #[test]
    fn toml_file_overrides_defaults() {
        let _g = EnvGuard::set(&required_env_vars());
        let f = write(
            r#"
            [vm]
            vcpus = 4
            memory_gib = 16

            [authorization]
            allowed_repositories = ["acme/widgets"]
            "#,
        );
        let cfg = Config::load_layered(Some(f.path())).unwrap();
        assert_eq!(cfg.vm.vcpus, 4);
        assert_eq!(cfg.vm.memory_gib, 16);
        assert!(
            cfg.authorization
                .allowed_repositories
                .contains("acme/widgets")
        );
    }

    #[test]
    fn env_overrides_toml() {
        let mut required = required_env_vars();
        required.push(("SBGH_VM_VCPUS", "12"));
        let _g = EnvGuard::set(&required);
        let f = write("[vm]\nvcpus = 4\n");
        let cfg = Config::load_layered(Some(f.path())).unwrap();
        assert_eq!(cfg.vm.vcpus, 12);
    }

    #[test]
    fn missing_required_field_errors() {
        // Don't set DATABASE_URL etc.
        let _g = EnvGuard::set(&[("SBGH_GH_CLIENT_ID", "Iv23litest")]);
        let err = Config::load_layered(None).unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    // ─── pick_config_path ───

    #[test]
    fn pick_explicit_path_wins_even_when_missing() {
        // If the user sets SBGH_CONFIG explicitly, we don't try to be clever —
        // pass it through verbatim. load_layered will silently fall back to
        // env-only if the file doesn't exist, which matches existing behaviour.
        let chosen = pick_config_path(Some("/some/explicit/path.toml"), &[]);
        assert_eq!(chosen, Some(PathBuf::from("/some/explicit/path.toml")));
    }

    #[test]
    fn pick_picks_first_existing_search_path() {
        let dir = tempfile::TempDir::new().unwrap();
        let a = dir.path().join("a.toml");
        let b = dir.path().join("b.toml");
        std::fs::write(&b, "").unwrap();
        let chosen = pick_config_path(None, &[a.clone(), b.clone()]);
        assert_eq!(chosen, Some(b), "should skip non-existent `a` and pick `b`");
    }

    #[test]
    fn pick_returns_first_match_in_declared_order() {
        // System path should win over the home-dir fallback when both exist.
        let dir = tempfile::TempDir::new().unwrap();
        let system = dir.path().join("system.toml");
        let home = dir.path().join("home.toml");
        std::fs::write(&system, "").unwrap();
        std::fs::write(&home, "").unwrap();
        let chosen = pick_config_path(None, &[system.clone(), home]);
        assert_eq!(chosen, Some(system));
    }

    #[test]
    fn pick_returns_none_when_nothing_exists() {
        let dir = tempfile::TempDir::new().unwrap();
        let chosen = pick_config_path(None, &[dir.path().join("nope.toml")]);
        assert!(chosen.is_none());
    }
}
