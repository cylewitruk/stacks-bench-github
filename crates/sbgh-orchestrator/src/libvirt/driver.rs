//! End-to-end libvirt benchmark driver.
//!
//! For each job:
//!   1. Prepare a fresh artifact directory.
//!   2. Refresh the bare git mirror, fetch the PR head SHA.
//!   3. Provision: boot qcow2 overlay, source raw+ext4 (cloned + checked out),
//!      LVM-thin chainstate snapshot, host tmpfs for results, cloud-init ISO.
//!   4. Render the domain XML, `virsh define`, `virsh start`.
//!   5. Poll loop (1s cadence): emit phase changes via the `PhaseListener`;
//!      finish when phase=done OR domain transitions to shut-off OR timeout.
//!   6. *Collect forensics* (last phase value, console tail, archived SQLite).
//!   7. Tear down everything in reverse order.
//!
//! Failure handling: the driver only returns `Err` on truly catastrophic setup
//! failures (e.g. can't `mkdir` the job dir). Anything that happens once
//! provisioning has begun comes back as `Ok(BenchmarkOutcome { status: Failed,
//! .. })` so the runner can still record forensics on the job row.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use sbgh_core::config::Config;
use sbgh_core::models::Job;

use crate::libvirt::boot::BootDisk;
use crate::libvirt::cloudinit::{CloudInitArtifacts, CloudInitParams};
use crate::libvirt::domain::{self, DomainSpec};
use crate::libvirt::lvm::ChainstateSnapshot;
use crate::libvirt::phase::{self, Phase};
use crate::libvirt::shell::Shell;
use crate::libvirt::source::SourceDisk;
use crate::libvirt::tmpfs::ResultsTmpfs;
use crate::libvirt::virsh::{self, DomState};
use crate::libvirt::{forensics, git_mirror};

const POLL_INTERVAL: Duration = Duration::from_secs(1);
const RESULTS_TMPFS_MIB: u32 = 256;
const RESULTS_SHARE_TAG: &str = "results";

#[async_trait]
pub trait PhaseListener: Send + Sync {
    async fn on_phase(&self, phase: &Phase);
}

#[allow(dead_code)] // used by tests; kept on the public surface as a convenience
pub struct NoopPhaseListener;

#[async_trait]
impl PhaseListener for NoopPhaseListener {
    async fn on_phase(&self, _phase: &Phase) {}
}

#[derive(Debug)]
pub struct BenchmarkOutcome {
    pub status: OutcomeStatus,
    pub summary: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutcomeStatus {
    Ok,
    Failed(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FinishReason {
    PhaseDone,
    PhaseError,
    ShutOff,
    Timeout,
}

impl FinishReason {
    fn label(&self) -> &'static str {
        match self {
            FinishReason::PhaseDone => "phase_done",
            FinishReason::PhaseError => "phase_error",
            FinishReason::ShutOff => "shut_off",
            FinishReason::Timeout => "timeout",
        }
    }
}

/// Aggregates the host-side artifacts so cleanup can run unconditionally even
/// if provisioning aborts mid-way.
#[derive(Default)]
struct JobArtifacts {
    boot: Option<BootDisk>,
    source: Option<SourceDisk>,
    chainstate: Option<ChainstateSnapshot>,
    tmpfs: Option<ResultsTmpfs>,
    domain_defined: bool,
    domain_started: bool,
}

pub struct LibvirtDriver {
    config: Arc<Config>,
    shell: Arc<dyn Shell>,
}

impl LibvirtDriver {
    pub fn new(config: Arc<Config>, shell: Arc<dyn Shell>) -> Self {
        Self { config, shell }
    }

    pub async fn run_benchmark(
        &self,
        job: &Job,
        listener: &dyn PhaseListener,
    ) -> anyhow::Result<BenchmarkOutcome> {
        let job_id = job.id.to_string();
        let domain_name = format!("sbgh-{job_id}");
        let job_dir = self
            .config
            .paths
            .jobs_dir
            .join(&job_id);
        std::fs::create_dir_all(&job_dir)?;

        let mut arts = JobArtifacts::default();
        let started = Instant::now();

        // Run the inner pipeline. Any error becomes a Failed outcome with
        // whatever forensics we can recover.
        let inner_result: anyhow::Result<FinishReason> = self
            .provision_define_start_poll(job, &job_id, &job_dir, &domain_name, &mut arts, listener)
            .await;

        // --- forensics (must happen BEFORE teardown) ----------------------
        let last_phase = arts
            .tmpfs
            .as_ref()
            .and_then(|t| phase::read(&t.phase_file()))
            .map(|p| p.label().to_string());

        let (sqlite_archived_path, sqlite_size_bytes) = arts
            .tmpfs
            .as_ref()
            .map(|t| {
                forensics::archive_sqlite(
                    &t.sqlite_file(),
                    &self
                        .config
                        .paths
                        .results_archive_dir,
                    &job_id,
                )
            })
            .unwrap_or((None, None));

        let (console_tail, console_size_bytes) =
            forensics::console_tail(&job_dir.join("console.log"));

        // --- teardown -----------------------------------------------------
        self.teardown(arts, &domain_name, &job_id, &job_dir)
            .await;

        // --- build summary ------------------------------------------------
        let duration_secs = started.elapsed().as_secs();
        let summary = serde_json::json!({
            "job_id": job.id,
            "head_sha": job.head_sha,
            "repository": job.repository,
            "duration_secs": duration_secs,
            "finish_reason": match &inner_result {
                Ok(r) => r.label(),
                Err(_) => "setup_error",
            },
            "last_phase": last_phase,
            "console_tail": console_tail,
            "console_size_bytes": console_size_bytes,
            "sqlite_archived_path": sqlite_archived_path,
            "sqlite_size_bytes": sqlite_size_bytes,
        });

        // Only an explicit phase=done counts as success. A `shut off` domain
        // without the script having written `done` first means the VM died
        // mid-flight — kernel panic, cloud-init failure, manual `virsh
        // destroy`, etc. Treat as failure but keep the forensics blob.
        let status = match inner_result {
            Ok(FinishReason::PhaseDone) => OutcomeStatus::Ok,
            Ok(FinishReason::ShutOff) => OutcomeStatus::Failed(format!(
                "VM powered off before reporting phase=done (last_phase={})",
                last_phase
                    .as_deref()
                    .unwrap_or("<none>")
            )),
            Ok(FinishReason::PhaseError) => OutcomeStatus::Failed("VM reported phase=error".into()),
            Ok(FinishReason::Timeout) => OutcomeStatus::Failed(format!(
                "VM exceeded job_timeout_secs={}",
                self.config
                    .vm
                    .job_timeout_secs
            )),
            Err(e) => OutcomeStatus::Failed(e.to_string()),
        };

        Ok(BenchmarkOutcome { status, summary })
    }

    async fn provision_define_start_poll(
        &self,
        job: &Job,
        job_id: &str,
        job_dir: &Path,
        domain_name: &str,
        arts: &mut JobArtifacts,
        listener: &dyn PhaseListener,
    ) -> anyhow::Result<FinishReason> {
        // Git mirror.
        let repo_url = format!("https://github.com/{}.git", job.repository);
        git_mirror::ensure(self.shell.as_ref(), &self.config.paths, &repo_url).await?;
        git_mirror::fetch_sha(self.shell.as_ref(), &self.config.paths, job_id, &job.head_sha)
            .await?;

        // Boot disk.
        arts.boot = Some(
            BootDisk::provision(self.shell.as_ref(), &self.config.paths, &self.config.vm, job_dir)
                .await?,
        );

        // Source disk.
        let source_mount = job_dir.join("source.mnt");
        arts.source = Some(
            SourceDisk::provision(
                self.shell.as_ref(),
                &self.config.paths,
                job_dir,
                &source_mount,
                &job.head_sha,
                &self
                    .config
                    .server
                    .service_user,
            )
            .await?,
        );

        // Chainstate snapshot.
        arts.chainstate = Some(
            ChainstateSnapshot::provision(self.shell.as_ref(), &self.config.lvm, job_id).await?,
        );

        // Results tmpfs.
        arts.tmpfs = Some(
            ResultsTmpfs::mount(
                self.shell.as_ref(),
                &self.config.paths,
                job_id,
                RESULTS_TMPFS_MIB,
                &self
                    .config
                    .server
                    .service_user,
            )
            .await?,
        );

        // Cloud-init ISO.
        let stacks_bench_args = derive_stacks_bench_args(
            job,
            &self
                .config
                .stacks_bench
                .default_args,
        );
        let tmpfs_ref = arts.tmpfs.as_ref().unwrap();
        let chainstate_ref = arts
            .chainstate
            .as_ref()
            .unwrap();
        let cidata = CloudInitArtifacts::build(
            self.shell.as_ref(),
            &self.config.paths,
            job_dir,
            &CloudInitParams {
                job_id,
                head_sha: &job.head_sha,
                stacks_bench_args: &stacks_bench_args,
                chainstate_mount: "/var/lib/stacks-chainstate",
                source_mount: "/opt/stacks-core",
                results_share_tag: RESULTS_SHARE_TAG,
                results_mount: "/results",
            },
        )
        .await?;

        // Render + write XML.
        let console_log = job_dir.join("console.log");
        let domain_xml_path = job_dir.join("domain.xml");
        let xml = domain::render(&DomainSpec {
            name: domain_name,
            vcpus: self.config.vm.vcpus,
            memory_gib: self.config.vm.memory_gib,
            boot_disk_path: &arts
                .boot
                .as_ref()
                .unwrap()
                .path,
            chainstate_dev_path: &chainstate_ref.device,
            source_disk_path: &arts
                .source
                .as_ref()
                .unwrap()
                .path,
            cidata_iso_path: &cidata.iso_path,
            results_share_dir: &tmpfs_ref.mount_dir,
            results_share_tag: RESULTS_SHARE_TAG,
            console_log_path: &console_log,
            network: &self.config.vm.network,
        })?;
        std::fs::write(&domain_xml_path, &xml)?;

        // Define + start.
        virsh::define(self.shell.as_ref(), &self.config.paths, &domain_xml_path).await?;
        arts.domain_defined = true;
        virsh::start(self.shell.as_ref(), &self.config.paths, domain_name).await?;
        arts.domain_started = true;

        // Poll.
        let phase_file = tmpfs_ref.phase_file();
        let reason = self
            .poll_to_completion(domain_name, &phase_file, listener)
            .await;
        Ok(reason)
    }

    async fn poll_to_completion(
        &self,
        domain_name: &str,
        phase_file: &Path,
        listener: &dyn PhaseListener,
    ) -> FinishReason {
        let started = Instant::now();
        let timeout = Duration::from_secs(
            self.config
                .vm
                .job_timeout_secs,
        );
        let mut last_phase: Option<Phase> = None;

        loop {
            if let Some(p) = phase::read(phase_file)
                && last_phase.as_ref() != Some(&p)
            {
                tracing::info!(domain = domain_name, phase = %p, "phase change");
                listener.on_phase(&p).await;
                if p.is_terminal() {
                    return match p {
                        Phase::Done => FinishReason::PhaseDone,
                        Phase::Error => FinishReason::PhaseError,
                        _ => unreachable!(),
                    };
                }
                last_phase = Some(p);
            }

            match virsh::domstate(self.shell.as_ref(), &self.config.paths, domain_name).await {
                Ok(DomState::ShutOff) | Ok(DomState::Undefined) => return FinishReason::ShutOff,
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "domstate poll failed"),
            }

            if started.elapsed() > timeout {
                return FinishReason::Timeout;
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    async fn teardown(&self, arts: JobArtifacts, domain_name: &str, job_id: &str, job_dir: &Path) {
        if arts.domain_started || arts.domain_defined {
            if let Err(e) =
                virsh::destroy(self.shell.as_ref(), &self.config.paths, domain_name).await
            {
                tracing::warn!(error = %e, "virsh destroy failed");
            }
        }
        if arts.domain_defined
            && let Err(e) =
                virsh::undefine(self.shell.as_ref(), &self.config.paths, domain_name).await
        {
            tracing::warn!(error = %e, "virsh undefine failed");
        }
        if let Some(t) = arts.tmpfs
            && let Err(e) = t
                .unmount(self.shell.as_ref())
                .await
        {
            tracing::warn!(error = %e, "tmpfs unmount failed");
        }
        if let Some(c) = arts.chainstate
            && let Err(e) = c
                .teardown(self.shell.as_ref())
                .await
        {
            tracing::warn!(error = %e, "lvm teardown failed");
        }
        if let Some(s) = arts.source
            && let Err(e) = s.teardown()
        {
            tracing::warn!(error = %e, "source disk delete failed");
        }
        if let Some(b) = arts.boot
            && let Err(e) = b.teardown()
        {
            tracing::warn!(error = %e, "boot disk delete failed");
        }
        git_mirror::prune(self.shell.as_ref(), &self.config.paths, job_id).await;
        if let Err(e) = std::fs::remove_dir_all(job_dir) {
            tracing::warn!(error = %e, "job dir cleanup failed");
        }
    }
}

fn derive_stacks_bench_args(job: &Job, default: &str) -> String {
    if let Some(arr) = job.args.0["args"].as_array()
        && !arr.is_empty()
    {
        return arr
            .iter()
            .filter_map(|v| v.as_str())
            .collect::<Vec<_>>()
            .join(" ");
    }
    default.to_string()
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use chrono::Utc;
    use sbgh_core::config::{
        AuthorizationConfig, GitHubConfig, LvmConfig, PathsConfig, ServerConfig, StacksBenchConfig,
        VmConfig,
    };
    use sbgh_core::models::{Job, JobStatus};
    use sqlx::types::Json;
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::*;
    use crate::libvirt::shell::test_support::{PreparedReply, RecordingShell};

    fn test_config(tmp: &TempDir) -> Config {
        let p = tmp.path();
        Config {
            server: ServerConfig {
                bind_addr: "127.0.0.1:0".into(),
                database_url: "postgres://unused".into(),
                service_user: "sbgh".into(),
            },
            github: GitHubConfig {
                client_id: "Iv23litest".into(),
                api_base_url: "https://api.github.test".into(),
                private_key_path: PathBuf::from("/dev/null"),
                webhook_secret: "unused".into(),
            },
            authorization: AuthorizationConfig::default(),
            vm: VmConfig {
                golden_image: p.join("golden.qcow2"),
                vcpus: 2,
                memory_gib: 8,
                boot_disk_gib: 64,
                // Big enough that we never reach the timeout in the test.
                job_timeout_secs: 30,
                network: "default".into(),
            },
            paths: PathsConfig {
                jobs_dir: p.join("jobs"),
                git_mirror: p.join("mirror.git"),
                results_tmpfs_root: p.join("tmpfs"),
                results_archive_dir: p.join("archive"),
                virsh_binary: "/usr/bin/virsh".into(),
                sudo_binary: "/usr/bin/sudo".into(),
                qemu_img_binary: "/usr/bin/qemu-img".into(),
                cloud_localds_binary: "/usr/bin/cloud-localds".into(),
                git_binary: "/usr/bin/git".into(),
            },
            lvm: LvmConfig {
                vg_name: "sbgh-vg".into(),
                thinpool: "thinpool".into(),
                chainstate_base_prefix: "mainnet-".into(),
                chainstate_snapshot_size_gib: 64,
            },
            stacks_bench: StacksBenchConfig { default_args: String::new() },
        }
    }

    fn fake_job() -> Job {
        Job {
            id: Uuid::new_v4(),
            status: JobStatus::Running,
            repository: "acme/widgets".into(),
            pr_number: 42,
            head_sha: "abc123def456".into(),
            requested_by: "alice".into(),
            command: "run".into(),
            args: Json(serde_json::json!({ "args": ["--iters=2"] })),
            installation_id: 7,
            comment_id: Some(1000),
            github_delivery_id: Some("fake-delivery".into()),
            queued_at: Utc::now(),
            started_at: Some(Utc::now()),
            finished_at: None,
            result: None,
            error: None,
        }
    }

    /// Build a shell that returns canned outputs in the order the driver
    /// will issue them, all the way through provisioning, virsh, and teardown.
    fn happy_path_shell() -> RecordingShell {
        let shell = RecordingShell::new();
        shell
            .expect_ok(1) // git fetch_sha
            .expect_ok(1) // qemu-img create
            .expect_ok(1) // truncate (source)
            .expect_ok(1) // mkfs.ext4
            .reply(PreparedReply::with_stdout("/dev/loop42\n")) // losetup -fP --show
            .expect_ok(1) // mount loop
            .expect_ok(1) // chown
            .expect_ok(1) // git clone --reference
            .expect_ok(1) // git checkout
            .expect_ok(1) // umount source
            .expect_ok(1) // losetup -d
            .reply(PreparedReply::with_stdout("  mainnet-2026-05-21\n")) // lvs
            .expect_ok(1) // lvcreate snapshot
            .expect_ok(1) // mount tmpfs
            .expect_ok(1) // cloud-localds
            .expect_ok(1) // virsh define
            .expect_ok(1) // virsh start
            // poll loop exits on first iter when .phase=done; no domstate calls
            .expect_ok(1) // virsh destroy
            .expect_ok(1) // virsh undefine
            .expect_ok(1) // umount tmpfs
            .expect_ok(1) // lvremove
            .expect_ok(1); // git update-ref -d (prune)
        shell
    }

    #[tokio::test]
    async fn end_to_end_happy_path_with_recording_shell() {
        let tmp = TempDir::new().unwrap();
        let cfg = Arc::new(test_config(&tmp));
        let job = fake_job();

        // Pre-create the bare mirror so git_mirror::ensure() is a no-op,
        // and pre-create the tmpfs mount dir + write .phase=done so the
        // poll loop exits on its very first iteration (the recording shell
        // can't actually mount the tmpfs).
        std::fs::create_dir_all(&cfg.paths.git_mirror).unwrap();
        let tmpfs_dir = cfg
            .paths
            .results_tmpfs_root
            .join(job.id.to_string());
        std::fs::create_dir_all(&tmpfs_dir).unwrap();
        std::fs::write(tmpfs_dir.join(".phase"), b"done\n").unwrap();

        let shell = Arc::new(happy_path_shell());
        let driver = LibvirtDriver::new(cfg.clone(), shell.clone());
        let outcome = driver
            .run_benchmark(&job, &NoopPhaseListener)
            .await
            .expect("driver should return Ok even on VM-side failures");

        // Outcome status + key summary fields.
        assert_eq!(outcome.status, OutcomeStatus::Ok);
        assert_eq!(outcome.summary["finish_reason"], "phase_done");
        assert_eq!(outcome.summary["last_phase"], "done");
        assert_eq!(outcome.summary["head_sha"], "abc123def456");
        assert_eq!(outcome.summary["repository"], "acme/widgets");

        // Command sequence: assert the exact shell program names in order,
        // skipping the `program` -> basename comparison detail.
        let calls = shell.calls();
        let programs: Vec<String> = calls
            .iter()
            .map(|c| {
                std::path::Path::new(&c.program)
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        let expected = [
            "git",           // fetch_sha
            "qemu-img",      // boot create
            "truncate",      // source: sparse file
            "mkfs.ext4",     // source: format
            "losetup",       // source: attach loop
            "mount",         // source: mount loop
            "chown",         // source: chown to service user
            "git",           // source: clone --reference
            "git",           // source: checkout sha
            "umount",        // source: unmount
            "losetup",       // source: detach loop
            "lvs",           // chainstate: pick base
            "lvcreate",      // chainstate: snapshot
            "mount",         // tmpfs mount
            "cloud-localds", // cidata ISO
            "virsh",         // define
            "virsh",         // start
            "virsh",         // destroy
            "virsh",         // undefine
            "umount",        // tmpfs unmount
            "lvremove",      // chainstate teardown
            "git",           // mirror prune
        ];
        assert_eq!(programs, expected, "command order mismatch");

        // Privileged calls: anything LVM, mount/umount, mkfs, losetup, chown, virsh.
        for (i, c) in calls.iter().enumerate() {
            let needs_priv = matches!(
                programs[i].as_str(),
                "lvs"
                    | "lvcreate"
                    | "lvremove"
                    | "mkfs.ext4"
                    | "losetup"
                    | "mount"
                    | "umount"
                    | "chown"
                    | "virsh"
            );
            assert_eq!(
                c.privileged, needs_priv,
                "privilege mismatch at index {i} ({})",
                programs[i]
            );
        }

        // Per-job dir must be gone after teardown.
        assert!(
            !cfg.paths
                .jobs_dir
                .join(job.id.to_string())
                .exists()
        );
    }

    #[tokio::test]
    async fn vm_shutoff_without_phase_done_is_failure() {
        let tmp = TempDir::new().unwrap();
        let cfg = Arc::new(test_config(&tmp));
        let job = fake_job();

        std::fs::create_dir_all(&cfg.paths.git_mirror).unwrap();
        let tmpfs_dir = cfg
            .paths
            .results_tmpfs_root
            .join(job.id.to_string());
        std::fs::create_dir_all(&tmpfs_dir).unwrap();
        // Note: no .phase file pre-written. The poll loop sees no phase, then
        // queries virsh domstate, which we'll return as "shut off". This
        // simulates a VM that crashed before writing phase=done.

        let shell = Arc::new(RecordingShell::new());
        shell
            .expect_ok(1) // git fetch_sha
            .expect_ok(1) // qemu-img create
            .expect_ok(1) // truncate (source)
            .expect_ok(1) // mkfs.ext4
            .reply(PreparedReply::with_stdout("/dev/loop42\n")) // losetup -fP --show
            .expect_ok(1) // mount loop
            .expect_ok(1) // chown
            .expect_ok(1) // git clone --reference
            .expect_ok(1) // git checkout
            .expect_ok(1) // umount source
            .expect_ok(1) // losetup -d
            .reply(PreparedReply::with_stdout("  mainnet-2026-05-21\n")) // lvs
            .expect_ok(1) // lvcreate snapshot
            .expect_ok(1) // mount tmpfs
            .expect_ok(1) // cloud-localds
            .expect_ok(1) // virsh define
            .expect_ok(1) // virsh start
            // First poll: phase file absent → falls through to domstate.
            .reply(PreparedReply::with_stdout("shut off\n")) // virsh domstate
            // Teardown
            .expect_ok(1) // virsh destroy
            .expect_ok(1) // virsh undefine
            .expect_ok(1) // umount tmpfs
            .expect_ok(1) // lvremove
            .expect_ok(1); // git prune

        let driver = LibvirtDriver::new(cfg.clone(), shell.clone());
        let outcome = driver
            .run_benchmark(&job, &NoopPhaseListener)
            .await
            .unwrap();

        match &outcome.status {
            OutcomeStatus::Failed(msg) => {
                assert!(msg.contains("powered off"), "got: {msg}");
                assert!(msg.contains("last_phase=<none>"), "got: {msg}");
            }
            other => panic!("expected Failed, got {other:?}"),
        }
        assert_eq!(outcome.summary["finish_reason"], "shut_off");
        assert!(outcome.summary["last_phase"].is_null());
    }

    #[tokio::test]
    async fn vm_phase_error_returns_failed_outcome_with_forensics() {
        let tmp = TempDir::new().unwrap();
        let cfg = Arc::new(test_config(&tmp));
        let job = fake_job();

        std::fs::create_dir_all(&cfg.paths.git_mirror).unwrap();
        let tmpfs_dir = cfg
            .paths
            .results_tmpfs_root
            .join(job.id.to_string());
        std::fs::create_dir_all(&tmpfs_dir).unwrap();
        // Phase=error → driver should classify the outcome as Failed but
        // still go through cleanup + return Ok.
        std::fs::write(tmpfs_dir.join(".phase"), b"error\n").unwrap();
        // Drop a small console.log into the job dir so we can verify
        // the tail makes it into the summary.
        let job_dir = cfg
            .paths
            .jobs_dir
            .join(job.id.to_string());
        std::fs::create_dir_all(&job_dir).unwrap();
        std::fs::write(job_dir.join("console.log"), b"kernel panic at 0x...").unwrap();

        let shell = Arc::new(happy_path_shell());
        let driver = LibvirtDriver::new(cfg.clone(), shell.clone());
        let outcome = driver
            .run_benchmark(&job, &NoopPhaseListener)
            .await
            .unwrap();

        match outcome.status {
            OutcomeStatus::Failed(msg) => assert!(msg.contains("phase=error"), "got: {msg}"),
            other => panic!("expected Failed, got {other:?}"),
        }
        assert_eq!(outcome.summary["finish_reason"], "phase_error");
        assert_eq!(outcome.summary["last_phase"], "error");
        assert_eq!(
            outcome.summary["console_tail"]
                .as_str()
                .unwrap(),
            "kernel panic at 0x..."
        );
    }
}
