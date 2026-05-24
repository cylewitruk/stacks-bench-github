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
use sbgh_core::config::OrchestratorConfig;
use sbgh_core::models::Job;

use crate::libvirt::boot::BootDisk;
use crate::libvirt::cloudinit::{CloudInitArtifacts, CloudInitParams};
use crate::libvirt::domain::{self, DomainSpec};
use crate::libvirt::lvm::ChainstateSnapshot;
use crate::libvirt::phase::{self, Phase};
use crate::libvirt::shell::{Shell, spec_priv};
use crate::libvirt::source::SourceDisk;
use crate::libvirt::tmpfs::ResultsTmpfs;
use crate::libvirt::virsh::{self, DomState};
use crate::libvirt::{forensics, git_mirror};

/// Size of the per-job host tmpfs that holds the virtio-fs results
/// share. Must accommodate everything the VM writes there before
/// shutdown: the stacks-bench SQLite store, the `stacks-bench` binary
/// itself (snapshotted alongside the DB so its schema is always
/// readable later), the run.json stdout capture, and the phase
/// journal. Stacks-bench's release binary with `lto=fat` is ~60–100
/// MiB, so the old 256 MiB ceiling was tight. 5 GiB is generous and
/// the cost is host RAM only while a job is running; we only ever run
/// one job concurrently.
const RESULTS_TMPFS_MIB: u32 = 5120;
const RESULTS_SHARE_TAG: &str = "results";
/// Virtio-fs tag for the persistent sccache cache share. Must match
/// the `<target dir>` element of the second `<filesystem>` block in
/// the rendered domain XML and the in-VM mount in `sbgh-run.sh.tmpl`.
const SCCACHE_SHARE_TAG: &str = "sccache";
/// In-VM mountpoint for the sccache cache share.
const SCCACHE_MOUNT: &str = "/var/cache/sccache";
/// `SCCACHE_CACHE_SIZE` value handed to sccache inside the VM. sccache
/// LRU-evicts to keep the cache dir under this ceiling; the host
/// `paths.sccache_dir` won't grow past this regardless of how many
/// jobs run against it.
const SCCACHE_MAX_SIZE: &str = "20G";

#[async_trait]
pub trait PhaseListener: Send + Sync {
    /// Called once per phase transition observed in the in-VM journal.
    /// Multiple transitions between polls are replayed in order.
    async fn on_phase(&self, phase: &Phase);

    /// Called periodically while the same phase is current, so the
    /// listener can refresh "still alive, currently in X for Y" UI
    /// (PR comment, status page, etc.). Default no-op.
    async fn on_heartbeat(&self, _phase: &Phase, _elapsed: Duration) {}
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
    config: Arc<OrchestratorConfig>,
    shell: Arc<dyn Shell>,
}

impl LibvirtDriver {
    pub fn new(config: Arc<OrchestratorConfig>, shell: Arc<dyn Shell>) -> Self {
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
            .and_then(|t| phase::read_last(&t.phase_log()))
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

        // Archive the append-only phase journal alongside the SQLite —
        // cheap (a few hundred bytes) and makes per-job "how long was
        // each phase" trivial to answer after the job dir is gone.
        let (phase_log_archived_path, phase_log_size_bytes) = arts
            .tmpfs
            .as_ref()
            .map(|t| {
                forensics::archive_phase_log(
                    &t.phase_log(),
                    &self
                        .config
                        .paths
                        .results_archive_dir,
                    &job_id,
                )
            })
            .unwrap_or((None, None));

        // Snapshot the stacks-bench binary that produced this run. The
        // template `cp`s it into $RESULTS just before phase=done; archive
        // it next to the SQLite so post-hoc `bench list` / `bench summary`
        // queries always have the exact-version reader. Missing when the
        // VM didn't reach the collecting phase (e.g. build failed).
        let (binary_archived_path, binary_size_bytes) = arts
            .tmpfs
            .as_ref()
            .map(|t| {
                forensics::archive_binary(
                    &t.stacks_bench_binary(),
                    &self
                        .config
                        .paths
                        .results_archive_dir,
                    &job_id,
                )
            })
            .unwrap_or((None, None));

        // Raw JSON stdout from `stacks-bench bench run --json`. Useful
        // both for the PR-comment summary builder and as the canonical
        // human-readable summary of what each run produced.
        let (run_json_archived_path, run_json_size_bytes) = arts
            .tmpfs
            .as_ref()
            .map(|t| {
                forensics::archive_run_json(
                    &t.run_json(),
                    &self
                        .config
                        .paths
                        .results_archive_dir,
                    &job_id,
                )
            })
            .unwrap_or((None, None));

        // Chown the serial console log to sbgh before we try to read it.
        // libvirt-qemu creates this file as itself (typically
        // libvirt-qemu:libvirt-qemu mode 0600), so a plain open from
        // sbgh hits EACCES and we lose the only artifact telling us
        // what happened inside the VM. Best-effort — if the chown
        // fails, forensics::console_tail will still log its EACCES
        // warning and we proceed with whatever forensics we have.
        let console_log = job_dir.join("console.log");
        if console_log.exists() {
            let owner = format!(
                "{u}:{u}",
                u = self
                    .config
                    .server
                    .service_user
            );
            let console_s = console_log
                .display()
                .to_string();
            match self
                .shell
                .run(spec_priv(Path::new("/usr/bin/chown"), &[&owner, &console_s]))
                .await
            {
                Ok(out) if out.status.success() => {}
                Ok(out) => tracing::warn!(
                    status = ?out.status,
                    stderr = %String::from_utf8_lossy(&out.stderr),
                    "chown console.log returned non-zero; forensics may be incomplete",
                ),
                Err(e) => {
                    tracing::warn!(error = %e, "chown console.log failed; forensics may be incomplete")
                }
            }
        }

        let (console_tail, console_size_bytes) = forensics::console_tail(&console_log);

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
            "archive_dir": forensics::job_archive_root(
                &self.config.paths.results_archive_dir,
                &job_id,
            ),
            "sqlite_archived_path": sqlite_archived_path,
            "sqlite_size_bytes": sqlite_size_bytes,
            "binary_archived_path": binary_archived_path,
            "binary_size_bytes": binary_size_bytes,
            "run_json_archived_path": run_json_archived_path,
            "run_json_size_bytes": run_json_size_bytes,
            "phase_log_archived_path": phase_log_archived_path,
            "phase_log_size_bytes": phase_log_size_bytes,
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
                sccache_share_tag: SCCACHE_SHARE_TAG,
                sccache_mount: SCCACHE_MOUNT,
                sccache_max_size: SCCACHE_MAX_SIZE,
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
            sccache_share_dir: &self.config.paths.sccache_dir,
            sccache_share_tag: SCCACHE_SHARE_TAG,
            console_log_path: &console_log,
            network: &self.config.vm.network,
        })?;
        std::fs::write(&domain_xml_path, &xml)?;

        // Define + start.
        virsh::define(self.shell.as_ref(), &self.config.paths, &domain_xml_path).await?;
        arts.domain_defined = true;
        virsh::start(self.shell.as_ref(), &self.config.paths, domain_name).await?;
        arts.domain_started = true;

        // Poll the journal + virsh domstate. The phase log is append-
        // only so we never miss a transition even with multi-second
        // polling.
        let phase_log = tmpfs_ref.phase_log();
        let reason = self
            .poll_to_completion(domain_name, &phase_log, listener)
            .await;
        Ok(reason)
    }

    async fn poll_to_completion(
        &self,
        domain_name: &str,
        phase_log: &Path,
        listener: &dyn PhaseListener,
    ) -> FinishReason {
        let started = Instant::now();
        let timeout = Duration::from_secs(
            self.config
                .vm
                .job_timeout_secs,
        );
        let poll_interval = Duration::from_secs(
            self.config
                .vm
                .poll_interval_secs
                .max(1),
        );
        let heartbeat_interval = Duration::from_secs(
            self.config
                .vm
                .heartbeat_interval_secs
                .max(1),
        );

        // Where we are in the (append-only) phase journal. The poll loop
        // advances it after consuming each new line.
        let mut journal_offset: u64 = 0;
        // Most recent observed phase + when we first saw it. The
        // heartbeat reports elapsed time within the current phase, not
        // wall-clock since job start — operators care about "is the
        // current step making progress?".
        let mut current_phase: Option<Phase> = None;
        let mut current_phase_started: Instant = Instant::now();
        let mut last_heartbeat: Instant = Instant::now();

        loop {
            // Replay any new journal entries. Multiple transitions in
            // one poll window get emitted in order so the listener sees
            // every state change, not just the most recent.
            for (_when, p) in phase::read_since(phase_log, &mut journal_offset) {
                tracing::info!(domain = domain_name, phase = %p, "phase change");
                listener.on_phase(&p).await;
                current_phase_started = Instant::now();
                current_phase = Some(p.clone());
                if p.is_terminal() {
                    return match p {
                        Phase::Done => FinishReason::PhaseDone,
                        Phase::Error => FinishReason::PhaseError,
                        _ => unreachable!("Phase::is_terminal only returns true for Done/Error"),
                    };
                }
            }

            // Heartbeat — periodic liveness signal. INFO log + listener
            // callback so the PR comment (or whatever) can surface the
            // elapsed time. Listener is responsible for any throttling
            // on its own emit side (e.g. PR comments are debounced).
            if last_heartbeat.elapsed() >= heartbeat_interval
                && let Some(p) = current_phase.as_ref()
            {
                let elapsed = current_phase_started.elapsed();
                tracing::info!(
                    domain = domain_name,
                    phase = %p,
                    elapsed = %phase::format_elapsed(elapsed),
                    "heartbeat",
                );
                listener
                    .on_heartbeat(p, elapsed)
                    .await;
                last_heartbeat = Instant::now();
            }

            // VM-state sanity check. If the domain has powered off
            // without writing a terminal phase to the journal, treat as
            // a `ShutOff` failure — the run-script either died before
            // it could `phase "error"` or the kernel/cloud-init panicked.
            match virsh::domstate(self.shell.as_ref(), &self.config.paths, domain_name).await {
                Ok(DomState::ShutOff) | Ok(DomState::Undefined) => return FinishReason::ShutOff,
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "domstate poll failed"),
            }

            if started.elapsed() > timeout {
                return FinishReason::Timeout;
            }
            tokio::time::sleep(poll_interval).await;
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
        GitHubConfig, LvmConfig, OrchestratorServerConfig, PathsConfig, StacksBenchConfig, VmConfig,
    };
    use sbgh_core::models::{Job, JobStatus};
    use sqlx::types::Json;
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::*;
    use crate::libvirt::shell::test_support::{PreparedReply, RecordingShell};

    fn test_config(tmp: &TempDir) -> OrchestratorConfig {
        let p = tmp.path();
        OrchestratorConfig {
            server: OrchestratorServerConfig {
                database_url: "postgres://unused".into(),
                service_user: "sbgh".into(),
            },
            github: GitHubConfig {
                client_id: "Iv23litest".into(),
                api_base_url: "https://api.github.test".into(),
                private_key_path: PathBuf::from("/dev/null"),
            },
            vm: VmConfig {
                golden_image: p.join("golden.qcow2"),
                vcpus: 2,
                memory_gib: 8,
                boot_disk_gib: 64,
                // Big enough that we never reach the timeout in the test.
                job_timeout_secs: 30,
                network: "default".into(),
                // Tight intervals so the test driver doesn't sleep for
                // multiple seconds between poll iterations.
                poll_interval_secs: 1,
                heartbeat_interval_secs: 60,
            },
            paths: PathsConfig {
                jobs_dir: p.join("jobs"),
                git_mirror: p.join("mirror.git"),
                results_tmpfs_root: p.join("tmpfs"),
                results_archive_dir: p.join("archive"),
                sccache_dir: p.join("sccache"),
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
                chainstate_snapshot_size_gib: None,
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
            .expect_ok(1) // rmdir lost+found (ext4 leftover, blocks git clone)
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
        // and pre-create the tmpfs mount dir + write a `.phase-log`
        // entry of `done` so the poll loop exits on its very first
        // iteration (the recording shell can't actually mount the tmpfs).
        std::fs::create_dir_all(&cfg.paths.git_mirror).unwrap();
        let tmpfs_dir = cfg
            .paths
            .results_tmpfs_root
            .join(job.id.to_string());
        std::fs::create_dir_all(&tmpfs_dir).unwrap();
        std::fs::write(tmpfs_dir.join(".phase-log"), b"1700000000 done\n").unwrap();

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
            "rmdir",         // source: drop ext4 lost+found
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
        // Note: no .phase-log pre-written. The poll loop finds an empty
        // journal, then queries virsh domstate, which we'll return as
        // "shut off". This simulates a VM that crashed before writing
        // any phase entries.

        let shell = Arc::new(RecordingShell::new());
        shell
            .expect_ok(1) // git fetch_sha
            .expect_ok(1) // qemu-img create
            .expect_ok(1) // truncate (source)
            .expect_ok(1) // mkfs.ext4
            .reply(PreparedReply::with_stdout("/dev/loop42\n")) // losetup -fP --show
            .expect_ok(1) // mount loop
            .expect_ok(1) // chown
            .expect_ok(1) // rmdir lost+found (ext4 leftover, blocks git clone)
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
        std::fs::write(tmpfs_dir.join(".phase-log"), b"1700000000 error\n").unwrap();
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
