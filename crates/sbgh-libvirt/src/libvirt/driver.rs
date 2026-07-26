//! End-to-end libvirt benchmark driver.
//!
//! For each job:
//!   1. Prepare a fresh artifact directory.
//!   2. Refresh the bare git mirror, fetch the PR head SHA.
//!   3. Provision: boot qcow2 overlay, source raw+ext4 (either a full checkout
//!      or a minimal cached-binary disk), LVM-thin chainstate snapshot, host
//!      tmpfs for results, cloud-init ISO.
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

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use async_trait::async_trait;
use sbgh_driver::artifact::{ArtifactSink, artifact_key};
use sbgh_driver::cache::{BinaryCacheStore, BuildFingerprint, CacheEnvironment};
use sbgh_driver::{
    BenchmarkRunContext, BenchmarkTaskSpec, Driver, DriverOutcome, DriverStatus, EventSink,
    PhaseLabel, Placement, ProgressUpdate, TaskContext, TaskSpec, WorkflowStep,
};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use crate::LibvirtConfig;
use crate::bench_progress::parse_progress_line;
use crate::fingerprint;
use crate::libvirt::boot::BootDisk;
use crate::libvirt::cloudinit::{BenchPhaseParams, CloudInitArtifacts, CloudInitCommon};
use crate::libvirt::domain::{self, DomainSpec};
use crate::libvirt::lvm::ChainstateSnapshot;
use crate::libvirt::phase::{self, Phase, PollMode};
use crate::libvirt::progress::ProgressTailer;
use crate::libvirt::shell::{Shell, spec, spec_priv};
use crate::libvirt::source::SourceDisk;
use crate::libvirt::tmpfs::ResultsTmpfs;
use crate::libvirt::virsh::{self, DomState};
use crate::libvirt::{forensics, git_mirror};

/// Schema-v1 envelope written by
/// `stacks-bench bench baseline calibrate --json`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
struct BaselineCalibrationResult {
    schema_version: Option<u32>,
    success: Option<bool>,
    result_type: Option<String>,
    result_version: Option<u32>,
    result: Option<BaselineCalibrationData>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
struct BaselineCalibrationData {
    calibration_id: Option<i64>,
}

impl BaselineCalibrationResult {
    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        match serde_json::from_slice::<Self>(bytes) {
            Ok(result) => Some(result),
            Err(error) => {
                tracing::warn!(%error, "failed to parse calibration.json");
                None
            }
        }
    }

    fn calibration_id(&self) -> Option<i64> {
        match (self.schema_version, self.result_type.as_deref(), self.result_version, self.success)
        {
            (Some(1), Some("baseline_calibration"), Some(1), Some(true)) => self
                .result
                .as_ref()
                .and_then(|result| result.calibration_id),
            _ => None,
        }
    }
}

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

// ── Binary-cache build fingerprint constants ──
// The repo-pinned `[profile.release]` (lto/codegen-units) is commit-determined;
// these capture the daemon's invariant build invocation + environment
// dimensions (the rest of the fingerprint — commit, resolved toolchain, golden
// image — is per-run).
const BUILD_PROFILE: &str = "release";
const BUILD_TARGET_TRIPLE: &str = "x86_64-unknown-linux-gnu";
/// Bump on any binary-affecting change to `sbgh-build.sh.tmpl`.
const BUILD_RECIPE_VERSION: u32 = 2;
/// Until `0027`'s profiler-protocol versioning lands.
const BUILD_PROTOCOL_VERSION: &str = "v1";

/// Unix seconds now (binary-cache LRU timestamps). Saturates to 0 pre-epoch.
fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Assemble the build fingerprint for `commit`. Reads
/// `rust-toolchain.toml` (or legacy `rust-toolchain`) from the **bare git
/// mirror** (no source-disk mount — the disk is detached after provisioning),
/// keys by the **declared** toolchain channel (pragmatic reuse, not `rustc -vV`
/// provenance), and folds in the golden-image identity. `None` when no
/// supported toolchain file / golden image is readable.
async fn assemble_fingerprint(
    shell: &dyn Shell,
    git_binary: &Path,
    mirror: &Path,
    golden_image: &Path,
    commit: &str,
) -> Option<BuildFingerprint> {
    let toolchain = read_declared_toolchain(shell, git_binary, mirror, commit).await?;
    let env = current_cache_environment(golden_image)?;
    Some(env.fingerprint(commit.to_string(), toolchain))
}

async fn read_declared_toolchain(
    shell: &dyn Shell,
    git_binary: &Path,
    mirror: &Path,
    commit: &str,
) -> Option<String> {
    if let Some(toolchain_toml) =
        show_repo_file(shell, git_binary, mirror, commit, "rust-toolchain.toml").await
        && let Some(channel) = fingerprint::toolchain_channel(&toolchain_toml)
    {
        return Some(channel);
    }
    let legacy = show_repo_file(shell, git_binary, mirror, commit, "rust-toolchain").await?;
    fingerprint::legacy_toolchain_channel(&legacy)
}

/// The daemon's **current** build environment — the [`CacheEnvironment`] half
/// of a fingerprint (everything but the per-run `commit` + `toolchain`): the
/// invariant build invocation (profile / triple / recipe / protocol) plus the
/// golden-image identity. `None` when the golden image is unreadable.
///
/// Single source of truth for the env, shared by `assemble_fingerprint` (the
/// build path) and the pin resolver (`set_pinned_by_commit`), so a cached
/// entry's stored env and the daemon's current env are compared on identical
/// terms.
pub fn current_cache_environment(golden_image: &Path) -> Option<CacheEnvironment> {
    let image_id = fingerprint::image_proxy_id(golden_image).ok()?;
    Some(CacheEnvironment {
        profile: BUILD_PROFILE.to_string(),
        features: String::new(),
        rustflags: String::new(),
        target_triple: BUILD_TARGET_TRIPLE.to_string(),
        recipe_version: BUILD_RECIPE_VERSION,
        image_id,
        protocol_version: BUILD_PROTOCOL_VERSION.to_string(),
    })
}

/// `git --git-dir=<mirror> show <sha>:<rel>` → file contents, or `None` if the
/// path is absent at that commit (or git errs). Reads from the bare mirror, so
/// the fingerprint needs no source-disk mount.
async fn show_repo_file(
    shell: &dyn Shell,
    git_binary: &Path,
    mirror: &Path,
    sha: &str,
    rel: &str,
) -> Option<String> {
    let mirror_s = mirror.display().to_string();
    let target = format!("{sha}:{rel}");
    let out = shell
        .run(spec(git_binary, &["--git-dir", &mirror_s, "show", &target]))
        .await
        .ok()?;
    if !out.status.success() {
        return None;
    }
    String::from_utf8(out.stdout).ok()
}

#[async_trait]
pub trait PhaseListener: Send + Sync {
    /// Called once per phase transition observed in the in-VM journal.
    /// Multiple transitions between polls are replayed in order.
    async fn on_phase(&self, phase: &Phase);

    /// Called periodically while the same phase is current, so the
    /// listener can refresh "still alive, currently in X for Y" UI
    /// (PR comment, status page, etc.). Default no-op.
    async fn on_heartbeat(&self, _phase: &Phase, _elapsed: Duration) {}

    /// Called for fine-grained `stacks-bench` progress parsed from stderr
    /// JSONL. Default no-op because older tests and non-benchmark phases
    /// only care about phase transitions.
    async fn on_progress(&self, _progress: ProgressUpdate) {}
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
    /// The run was cancelled (operator shutdown/abort). Only honored at the
    /// poll loop — provisioning runs atomically (its loop-device window can't
    /// be safely interrupted), so cancel is observed once the VM is running.
    Cancelled,
}

impl FinishReason {
    fn label(&self) -> &'static str {
        match self {
            FinishReason::PhaseDone => "phase_done",
            FinishReason::PhaseError => "phase_error",
            FinishReason::ShutOff => "shut_off",
            FinishReason::Timeout => "timeout",
            FinishReason::Cancelled => "cancelled",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ProgressContext {
    workflow_step: WorkflowStep,
    run_index: i32,
    requested_run_count: i32,
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

#[derive(Clone, Debug)]
enum BuildPlan {
    CacheHit { binary: PathBuf, digest: String },
    Miss,
}

/// The driver's per-run inputs threaded through provisioning: the target
/// repository and commit (from the platform [`TaskContext`]) plus this run's
/// benchmark args. Bundled so the provisioning helpers stay within the
/// argument-count budget.
struct RunInputs<'a> {
    repository: &'a str,
    commit: &'a str,
    bench_args: &'a [String],
    sqlite_seed_key: Option<&'a str>,
    shared_baseline_calibration: bool,
    baseline_calibration_id: Option<i64>,
    benchmark_run: BenchmarkRunContext,
    /// Build-only mode stops after the build VM publishes the artifact and
    /// skips the bench phase.
    build_only: bool,
}

pub struct LibvirtDriver {
    config: Arc<LibvirtConfig>,
    shell: Arc<dyn Shell>,
    artifact_store: Arc<dyn ArtifactSink>,
    /// Opt-in `stacks-bench` binary cache. `None` when
    /// `[artifacts.binary_cache].enabled` is false (the default), in which case
    /// the build/bench flow is byte-identical to before.
    binary_cache: Option<Arc<dyn BinaryCacheStore>>,
}

impl LibvirtDriver {
    pub fn new(
        config: LibvirtConfig,
        shell: Arc<dyn Shell>,
        artifact_store: Arc<dyn ArtifactSink>,
        binary_cache: Option<Arc<dyn BinaryCacheStore>>,
    ) -> Self {
        Self {
            config: Arc::new(config),
            shell,
            artifact_store,
            binary_cache,
        }
    }

    /// Run one libvirt job to a terminal outcome. The shared
    /// provision → build → publish path, then either the bench phase
    /// (`build_only = false`) or a stop after publish (`build_only = true`).
    /// Forensics and teardown are identical.
    pub async fn run_benchmark(
        &self,
        ctx: &TaskContext<'_>,
        spec: &TaskSpec,
        listener: &dyn PhaseListener,
        cancel: &CancellationToken,
        // The libvirt cpuset this job's slot owns, or `None` to float. Threaded
        // down to the domain XML.
        vcpu_cpuset: Option<&str>,
    ) -> anyhow::Result<BenchmarkOutcome> {
        let job_id = ctx.job_id.to_string();
        let domain_name = format!("sbgh-{job_id}");
        let job_dir = self
            .config
            .paths
            .jobs_dir
            .join(&job_id);
        std::fs::create_dir_all(&job_dir)?;

        let mut arts = JobArtifacts::default();
        let started = Instant::now();
        let (
            bench_args,
            sqlite_seed_key,
            shared_baseline_calibration,
            baseline_calibration_id,
            benchmark_run,
            build_only,
        ) = match spec {
            TaskSpec::Benchmark(BenchmarkTaskSpec {
                args,
                sqlite_seed_key,
                shared_baseline_calibration,
                baseline_calibration_id,
                benchmark_run,
            }) => (
                args.as_slice(),
                sqlite_seed_key.as_deref(),
                *shared_baseline_calibration,
                *baseline_calibration_id,
                *benchmark_run,
                false,
            ),
            TaskSpec::BuildOnly => {
                (&[][..], None, false, None, BenchmarkRunContext::default(), true)
            }
        };
        let inputs = RunInputs {
            repository: ctx.repository,
            commit: ctx.commit,
            bench_args,
            sqlite_seed_key,
            shared_baseline_calibration,
            baseline_calibration_id,
            benchmark_run,
            build_only,
        };

        // Run the inner pipeline. Any error becomes a Failed outcome with
        // whatever forensics we can recover.
        let inner_result: anyhow::Result<FinishReason> = self
            .provision_define_start_poll(
                &inputs,
                &job_id,
                &job_dir,
                &domain_name,
                &mut arts,
                listener,
                cancel,
                vcpu_cpuset,
            )
            .await;

        // --- forensics (must happen BEFORE teardown) ----------------------
        let last_phase = arts
            .tmpfs
            .as_ref()
            .and_then(|t| phase::read_last(&t.phase_log()))
            .map(|p| p.label().to_string());

        // Archive the per-job artifacts through the configured store (local FS,
        // or S3 with a local mirror). The summary records each
        // artifact's store **key** (`<job_id>/<relative>`, Decision 0002), not a
        // bare path; `put` returns the byte size, or `None` when the VM produced
        // no such file (or, for S3, the local mirror write failed — a failed S3
        // upload is logged but still returns the local size, Decision 0003).
        let store = self.artifact_store.clone();
        let tmpfs = arts.tmpfs.as_ref();

        let (sqlite_archived_path, sqlite_size_bytes) =
            archive_optional(store.as_ref(), &job_id, forensics::SQLITE_RELATIVE, tmpfs, |t| {
                t.sqlite_file()
            })
            .await;

        // The append-only phase journal — cheap, makes per-job "how long was
        // each phase" trivial after the job dir is gone.
        let (phase_log_archived_path, phase_log_size_bytes) =
            archive_optional(store.as_ref(), &job_id, forensics::PHASE_LOG_RELATIVE, tmpfs, |t| {
                t.phase_log()
            })
            .await;

        // The stacks-bench binary that produced this run — kept as a host-side
        // forensic copy (the exact-version DB reader), but archived **locally
        // only**: it's large (~250-300 MB) and non-portable (built for the VM's
        // arch), so uploading every run's binary to object storage is pure cost.
        // Cross-host binary reuse is a keyed cache's job (0025), not this
        // archive. Missing when the VM didn't reach collecting.
        let binary_key = artifact_key(&job_id, forensics::BINARY_RELATIVE);
        let binary_size_bytes = match tmpfs {
            Some(t) => {
                store
                    .put_local_only(&binary_key, &t.stacks_bench_binary())
                    .await
            }
            None => None,
        };
        let binary_archived_path = binary_size_bytes.map(|_| binary_key);

        // Raw JSON stdout from `stacks-bench bench run --json` — the source of
        // the curated PR-comment metrics (read back via `ArtifactStore::get`).
        let (run_json_archived_path, run_json_size_bytes) =
            archive_optional(store.as_ref(), &job_id, forensics::RUN_JSON_RELATIVE, tmpfs, |t| {
                t.run_json()
            })
            .await;

        let (run_progress_archived_path, run_progress_size_bytes) = archive_optional(
            store.as_ref(),
            &job_id,
            forensics::RUN_PROGRESS_JSONL_RELATIVE,
            tmpfs,
            |t| t.run_progress_jsonl(),
        )
        .await;

        let (calibration_json_archived_path, calibration_json_size_bytes) = archive_optional(
            store.as_ref(),
            &job_id,
            forensics::CALIBRATION_JSON_RELATIVE,
            tmpfs,
            |t| t.calibration_json(),
        )
        .await;

        let (calibration_progress_archived_path, calibration_progress_size_bytes) =
            archive_optional(
                store.as_ref(),
                &job_id,
                forensics::CALIBRATION_PROGRESS_JSONL_RELATIVE,
                tmpfs,
                |t| t.calibration_progress_jsonl(),
            )
            .await;
        let baseline_calibration_id = inputs
            .baseline_calibration_id
            .or_else(|| tmpfs.and_then(baseline_calibration_id_from_tmpfs));

        // Chown the serial console log to sbgh before we try to read it.
        // libvirt-qemu creates this file as itself (typically
        // libvirt-qemu:libvirt-qemu mode 0600), so a plain open from
        // sbgh hits EACCES and we lose the only artifact telling us
        // what happened inside the VM. Best-effort — if the chown
        // fails, forensics::console_tail will still log its EACCES
        // warning and we proceed with whatever forensics we have.
        let console_log = job_dir.join("console.log");
        if console_log.exists() {
            let owner = format!("{u}:{u}", u = self.config.service_user);
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
            "job_id": ctx.job_id,
            "head_sha": ctx.commit,
            "repository": ctx.repository,
            "duration_secs": duration_secs,
            "finish_reason": match &inner_result {
                Ok(r) => r.label(),
                Err(_) => "setup_error",
            },
            "last_phase": last_phase,
            "console_tail": console_tail,
            "console_size_bytes": console_size_bytes,
            "archive_dir": store.job_dir(&job_id),
            "sqlite_archived_path": sqlite_archived_path,
            "sqlite_size_bytes": sqlite_size_bytes,
            "binary_archived_path": binary_archived_path,
            "binary_size_bytes": binary_size_bytes,
            "run_json_archived_path": run_json_archived_path,
            "run_json_size_bytes": run_json_size_bytes,
            "run_progress_archived_path": run_progress_archived_path,
            "run_progress_size_bytes": run_progress_size_bytes,
            "calibration_json_archived_path": calibration_json_archived_path,
            "calibration_json_size_bytes": calibration_json_size_bytes,
            "calibration_progress_archived_path": calibration_progress_archived_path,
            "calibration_progress_size_bytes": calibration_progress_size_bytes,
            "baseline_calibration_id": baseline_calibration_id,
            "phase_log_archived_path": phase_log_archived_path,
            "phase_log_size_bytes": phase_log_size_bytes,
        });

        // Only an explicit phase=done counts as success. A `shut off` domain
        // without the script having written `done` first means the VM died
        // mid-flight — kernel panic, cloud-init failure, manual `virsh
        // destroy`, etc. Treat as failure but keep the forensics blob.
        let missing_shared_calibration =
            inputs.shared_baseline_calibration && baseline_calibration_id.is_none();
        let status = match inner_result {
            _ if missing_shared_calibration => OutcomeStatus::Failed(
                "shared baseline calibration did not produce a baseline id".into(),
            ),
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
            // The worker overrides this to `Aborted` via the token; `Failed` is
            // the safe fallback if that check is ever missed.
            Ok(FinishReason::Cancelled) => {
                OutcomeStatus::Failed("run cancelled by shutdown".into())
            }
            Err(e) => OutcomeStatus::Failed(e.to_string()),
        };

        Ok(BenchmarkOutcome { status, summary })
    }

    #[allow(clippy::too_many_arguments)]
    async fn provision_define_start_poll(
        &self,
        inputs: &RunInputs<'_>,
        job_id: &str,
        job_dir: &Path,
        domain_name: &str,
        arts: &mut JobArtifacts,
        listener: &dyn PhaseListener,
        cancel: &CancellationToken,
        vcpu_cpuset: Option<&str>,
    ) -> anyhow::Result<FinishReason> {
        // ── one-time provisioning shared by both phases ────────────────
        // Fetch the commit into the bare mirror first (fingerprinting
        // reads toolchain files from it), then resolve the binary-cache plan
        // before any source-disk write. A hit can therefore provision a minimal
        // source disk instead of doing a full checkout and chown.
        self.prepare_git_mirror(inputs, job_id)
            .await?;
        let plan = self
            .resolve_build_plan(inputs)
            .await;
        let (cidata, plan) = self
            .provision_artifacts(inputs, job_id, job_dir, arts, plan, listener)
            .await?;

        // ── phase 1: build VM (skipped on a binary-cache hit) ─────────
        // If a fingerprint-matched binary is cached, the
        // source disk has already been provisioned with just that binary, so
        // skip the build VM entirely. Gated by
        // `[artifacts.binary_cache].enabled` — disabled (default) always builds.
        let reused = self
            .mark_cached_binary_reused(inputs, &plan, listener)
            .await;
        let mut published = false;
        if !reused {
            // Succeeds on phase=build_done OR (ShutOff after seeing
            // BuildDone). Anything else (error phase, ShutOff without
            // BuildDone, timeout) aborts the whole job — no bench attempt.
            let build_reason = self
                .run_phase(
                    "build",
                    PollMode::Build,
                    self.config.vm.build_vcpus,
                    self.config
                        .vm
                        .build_memory_bytes,
                    &cidata.build_iso_path,
                    job_dir,
                    domain_name,
                    arts,
                    listener,
                    cancel,
                    vcpu_cpuset,
                    None,
                )
                .await?;
            match build_reason {
                FinishReason::PhaseDone => {
                    // success path — fall through to bench
                }
                other => return Ok(other),
            }
            // Populate the cache from the freshly-built binary.
            published = self
                .publish_built_binary(inputs, arts)
                .await;
        }

        // A build-only job stops here: there is no bench phase. Its purpose is
        // the cached artifact, so it succeeds ONLY if the artifact is
        // now in the cache: `reused` (already warm) or freshly `published`.
        // Otherwise fail closed — cache disabled, no binary, or a fingerprint /
        // publish error (a benchmark run keeps caching best-effort; here the
        // artifact *is* the job).
        if inputs.build_only {
            if reused || published {
                return Ok(FinishReason::PhaseDone);
            }
            anyhow::bail!(
                "build-only job produced no cached artifact (the binary cache is disabled, the \
                 build produced no binary, or publishing failed)"
            );
        }

        if let Some(calibrate_iso_path) = cidata
            .calibrate_iso_path
            .as_ref()
        {
            let calibrate_reason = self
                .run_phase(
                    "calibrate",
                    PollMode::Bench,
                    self.config.vm.bench_vcpus,
                    self.config
                        .vm
                        .bench_memory_bytes,
                    calibrate_iso_path,
                    job_dir,
                    domain_name,
                    arts,
                    listener,
                    cancel,
                    vcpu_cpuset,
                    Some(ProgressContext {
                        workflow_step: WorkflowStep::Calibrate,
                        run_index: inputs.benchmark_run.run_index,
                        requested_run_count: inputs
                            .benchmark_run
                            .requested_run_count,
                    }),
                )
                .await?;
            match calibrate_reason {
                FinishReason::PhaseDone => {}
                other => return Ok(other),
            }
        } else if inputs.shared_baseline_calibration
            && inputs
                .baseline_calibration_id
                .is_none()
        {
            anyhow::bail!("shared baseline calibration requested without calibration phase or id");
        }

        // ── phase 2: bench VM ─────────────────────────────────────────
        // Same domain name, redefined at bench memory + cidata. The
        // existing boot/source/chainstate/tmpfs/sccache stay attached
        // by virtue of being referenced in the new XML.
        let bench_reason = self
            .run_phase(
                "bench",
                PollMode::Bench,
                self.config.vm.bench_vcpus,
                self.config
                    .vm
                    .bench_memory_bytes,
                &cidata.bench_iso_path,
                job_dir,
                domain_name,
                arts,
                listener,
                cancel,
                vcpu_cpuset,
                Some(ProgressContext {
                    workflow_step: WorkflowStep::Run,
                    run_index: inputs.benchmark_run.run_index,
                    requested_run_count: inputs
                        .benchmark_run
                        .requested_run_count,
                }),
            )
            .await?;
        Ok(bench_reason)
    }

    async fn prepare_git_mirror(&self, inputs: &RunInputs<'_>, job_id: &str) -> anyhow::Result<()> {
        let repo_url = format!("https://github.com/{}.git", inputs.repository);
        git_mirror::ensure(self.shell.as_ref(), &self.config.paths, &repo_url).await?;
        git_mirror::fetch_sha(self.shell.as_ref(), &self.config.paths, job_id, inputs.commit).await
    }

    /// Resolve the binary-cache plan before source-disk provisioning.
    /// The caller has already fetched `inputs.commit` into the bare mirror, so
    /// fingerprinting can read `rust-toolchain(.toml)` without a source mount.
    async fn resolve_build_plan(&self, inputs: &RunInputs<'_>) -> BuildPlan {
        let Some(cache) = self.binary_cache.as_deref() else {
            return BuildPlan::Miss;
        };
        let Some(fp) = self
            .fingerprint_for(inputs.commit)
            .await
        else {
            tracing::info!(
                commit = inputs.commit,
                "binary cache: no fingerprint (missing/unreadable rust-toolchain(.toml) or golden \
                 image); building"
            );
            return BuildPlan::Miss;
        };
        let Some(hit) = cache.get(&fp, unix_now()) else {
            return BuildPlan::Miss;
        };
        BuildPlan::CacheHit {
            binary: hit.path,
            digest: hit.digest,
        }
    }

    /// Report a planned binary-cache hit after provisioning has already seeded
    /// the minimal source disk. Returns `true` to skip the build VM.
    async fn mark_cached_binary_reused(
        &self,
        inputs: &RunInputs<'_>,
        plan: &BuildPlan,
        listener: &dyn PhaseListener,
    ) -> bool {
        let BuildPlan::CacheHit { digest, .. } = plan else {
            return false;
        };
        let short = &digest[..digest.len().min(12)];
        tracing::info!(
            commit = inputs.commit,
            digest = %digest,
            "binary cache: reusing cached stacks-bench binary; skipping the build VM"
        );
        // Drive the reporter with a single opaque `build_cached:<digest>`
        // phase. Each surface interprets it; the build VM that normally emits
        // `building`/`build_done` never runs.
        listener
            .on_phase(&Phase::Other(format!("build_cached:{short}")))
            .await;
        true
    }

    /// Publish a freshly-built binary into the cache. Called after a successful
    /// build so the next run of the same `(commit, environment)` can skip it.
    /// Returns whether the artifact is now cached — best-effort for a benchmark
    /// run (a miss just means the next run rebuilds), but the build-only path
    /// treats a `false` as failure (the cached artifact *is* the job). A miss
    /// (`false`) means: the cache is disabled, no binary was produced, or
    /// fingerprinting / publishing failed.
    async fn publish_built_binary(&self, inputs: &RunInputs<'_>, arts: &JobArtifacts) -> bool {
        let Some(cache) = self.binary_cache.as_deref() else {
            return false;
        };
        let Some(t) = arts.tmpfs.as_ref() else {
            return false;
        };
        let binary = t.stacks_bench_binary();
        if !binary.exists() {
            return false;
        }
        let Some(fp) = self
            .fingerprint_for(inputs.commit)
            .await
        else {
            return false;
        };
        match cache.publish(&fp, &binary, unix_now(), false) {
            Ok(_) => {
                tracing::info!(commit = inputs.commit, "binary cache: published built binary");
                true
            }
            Err(e) => {
                tracing::warn!(error = %e, "binary cache: publishing the built binary failed");
                false
            }
        }
    }

    /// This run's build fingerprint — a config-bound wrapper over the free
    /// [`assemble_fingerprint`] (reads the toolchain from the bare git mirror).
    async fn fingerprint_for(&self, commit: &str) -> Option<BuildFingerprint> {
        assemble_fingerprint(
            self.shell.as_ref(),
            &self.config.paths.git_binary,
            &self.config.paths.git_mirror,
            &self.config.vm.golden_image,
            commit,
        )
        .await
    }

    /// One-time provisioning: artifacts that live across both VM
    /// lifecycles (boot disk, source disk, chainstate snapshot, results
    /// tmpfs) + the two cidata ISOs.
    async fn provision_artifacts(
        &self,
        inputs: &RunInputs<'_>,
        job_id: &str,
        job_dir: &Path,
        arts: &mut JobArtifacts,
        plan: BuildPlan,
        listener: &dyn PhaseListener,
    ) -> anyhow::Result<(CloudInitArtifacts, BuildPlan)> {
        // Boot disk — single qcow2 overlay reused across both phases.
        // The bench VM boots from the same disk the build VM left
        // behind; cloud-init's per-instance-id re-run is the mechanism
        // that gets a different script executed on the second boot.
        arts.boot = Some(
            BootDisk::provision(self.shell.as_ref(), &self.config.paths, &self.config.vm, job_dir)
                .await?,
        );

        // Source disk — persists across both phases. A miss provisions the full
        // checkout for the build VM. A cache hit provisions only the binary the
        // bench VM execs, avoiding checkout + full-tree chown.
        let source_mount = job_dir.join("source.mnt");
        let plan = match plan {
            BuildPlan::CacheHit { binary, digest } => {
                let short = &digest[..digest.len().min(12)];
                listener
                    .on_phase(&Phase::Other(format!("build_cache_staging:{short}")))
                    .await;
                match SourceDisk::provision_minimal(
                    self.shell.as_ref(),
                    job_dir,
                    &source_mount,
                    &binary,
                    &self.config.service_user,
                )
                .await
                {
                    Ok(source) => {
                        arts.source = Some(source);
                        BuildPlan::CacheHit { binary, digest }
                    }
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            "binary cache: minimal source-disk seed failed; falling back to full checkout + build"
                        );
                        arts.source = Some(
                            SourceDisk::provision(
                                self.shell.as_ref(),
                                &self.config.paths,
                                job_dir,
                                &source_mount,
                                inputs.commit,
                                &self.config.service_user,
                            )
                            .await?,
                        );
                        BuildPlan::Miss
                    }
                }
            }
            BuildPlan::Miss => {
                arts.source = Some(
                    SourceDisk::provision(
                        self.shell.as_ref(),
                        &self.config.paths,
                        job_dir,
                        &source_mount,
                        inputs.commit,
                        &self.config.service_user,
                    )
                    .await?,
                );
                BuildPlan::Miss
            }
        };

        // Chainstate snapshot — only the bench phase mounts it, but
        // the device is attached to the domain in both phases (build
        // phase ignores it). Saves a domain-XML difference.
        arts.chainstate = Some(
            ChainstateSnapshot::provision(self.shell.as_ref(), &self.config.lvm, job_id).await?,
        );

        // Results tmpfs — shared between phases. Phase journal lives
        // here, so both VMs append to the same `.phase-log`.
        arts.tmpfs = Some(
            ResultsTmpfs::mount(
                self.shell.as_ref(),
                &self.config.paths,
                job_id,
                RESULTS_TMPFS_MIB,
                &self.config.service_user,
            )
            .await?,
        );
        if let (Some(seed_key), Some(tmpfs)) = (inputs.sqlite_seed_key, arts.tmpfs.as_ref()) {
            seed_sqlite_from_store(self.artifact_store.as_ref(), seed_key, tmpfs).await?;
        }
        if let (BuildPlan::CacheHit { binary, .. }, Some(tmpfs)) = (&plan, arts.tmpfs.as_ref()) {
            let _ = std::fs::copy(binary, tmpfs.stacks_bench_binary());
        }

        // Cloud-init: two ISOs, one per phase, distinct instance-ids
        // so cloud-init re-runs user-data on the second boot.
        let stacks_bench_args = inputs.bench_args.join(" ");
        let cidata = CloudInitArtifacts::build(
            self.shell.as_ref(),
            &self.config.paths,
            job_dir,
            &CloudInitCommon {
                job_id,
                head_sha: inputs.commit,
                chainstate_mount: "/var/lib/stacks-chainstate",
                source_mount: "/opt/stacks-core",
                results_share_tag: RESULTS_SHARE_TAG,
                results_mount: "/results",
                sccache_share_tag: SCCACHE_SHARE_TAG,
                sccache_mount: SCCACHE_MOUNT,
                sccache_max_size: SCCACHE_MAX_SIZE,
            },
            &BenchPhaseParams {
                stacks_bench_args: &stacks_bench_args,
                shared_baseline_calibration: inputs.shared_baseline_calibration,
                baseline_calibration_id: inputs.baseline_calibration_id,
            },
        )
        .await?;
        Ok((cidata, plan))
    }

    /// Render the domain XML at this phase's memory/vcpu/cidata,
    /// `virsh define` (replaces any prior inactive definition for the
    /// same name), `virsh start`, then poll until success-or-failure.
    ///
    /// On clean exit (success or per-phase failure), this returns
    /// `Ok(FinishReason)`. The caller decides whether to continue to
    /// the next phase based on which `FinishReason` came back.
    ///
    /// We intentionally do NOT undefine the domain here — that
    /// happens in teardown. `virsh define` on the next call replaces
    /// the existing inactive definition with the new XML.
    #[allow(clippy::too_many_arguments)] // top-level orchestration; readability wins
    async fn run_phase(
        &self,
        phase_label: &'static str,
        mode: PollMode,
        vcpus: u32,
        memory_bytes: u64,
        cidata_iso_path: &Path,
        job_dir: &Path,
        domain_name: &str,
        arts: &mut JobArtifacts,
        listener: &dyn PhaseListener,
        cancel: &CancellationToken,
        vcpu_cpuset: Option<&str>,
        progress_context: Option<ProgressContext>,
    ) -> anyhow::Result<FinishReason> {
        tracing::info!(domain = domain_name, phase_lifecycle = phase_label, "starting phase");

        let tmpfs_ref = arts
            .tmpfs
            .as_ref()
            .expect("tmpfs provisioned before run_phase");
        let chainstate_ref = arts
            .chainstate
            .as_ref()
            .expect("chainstate provisioned before run_phase");
        let boot_ref = arts
            .boot
            .as_ref()
            .expect("boot provisioned before run_phase");
        let source_ref = arts
            .source
            .as_ref()
            .expect("source provisioned before run_phase");

        let console_log = job_dir.join("console.log");
        let domain_xml_path = job_dir.join(format!("domain.{phase_label}.xml"));
        // Pin the domain UUID across phases. Without this, libvirt auto-
        // generates a fresh UUID on the bench-phase `virsh define` and
        // rejects it because the build-phase definition (same name,
        // different auto UUID) is still registered.
        let job_uuid = domain_name
            .strip_prefix("sbgh-")
            .unwrap_or(domain_name);
        let xml = domain::render(&DomainSpec {
            name: domain_name,
            uuid: job_uuid,
            vcpus,
            memory_bytes,
            boot_disk_path: &boot_ref.path,
            chainstate_dev_path: &chainstate_ref.device,
            source_disk_path: &source_ref.path,
            cidata_iso_path,
            results_share_dir: &tmpfs_ref.mount_dir,
            results_share_tag: RESULTS_SHARE_TAG,
            sccache_share_dir: &self.config.paths.sccache_dir,
            sccache_share_tag: SCCACHE_SHARE_TAG,
            console_log_path: &console_log,
            network: &self.config.vm.network,
            vcpu_cpuset,
            // Emulator threads pin to the host cores only when this job's vCPUs
            // are pinned (`[runner].host_cpus`); meaningless otherwise.
            emulator_cpuset: vcpu_cpuset.and(
                self.config
                    .host_cpus
                    .as_deref(),
            ),
        })?;
        std::fs::write(&domain_xml_path, &xml)?;

        virsh::define(self.shell.as_ref(), &self.config.paths, &domain_xml_path).await?;
        arts.domain_defined = true;
        let mut progress_tailer = progress_context.map(|ctx| {
            let path = match ctx.workflow_step {
                WorkflowStep::Calibrate => tmpfs_ref.calibration_progress_jsonl(),
                WorkflowStep::Run => tmpfs_ref.run_progress_jsonl(),
            };
            (ProgressTailer::new(path), ctx)
        });
        virsh::start(self.shell.as_ref(), &self.config.paths, domain_name).await?;
        arts.domain_started = true;

        let phase_log = tmpfs_ref.phase_log();
        let reason = self
            .poll_to_completion(
                domain_name,
                &phase_log,
                progress_tailer.as_mut(),
                listener,
                mode,
                cancel,
            )
            .await;
        Ok(reason)
    }

    async fn poll_to_completion(
        &self,
        domain_name: &str,
        phase_log: &Path,
        mut progress_tailer: Option<&mut (ProgressTailer, ProgressContext)>,
        listener: &dyn PhaseListener,
        mode: PollMode,
        cancel: &CancellationToken,
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
        // advances it after consuming each new line. The journal is
        // shared across both VMs (it lives on the results tmpfs); we
        // start at offset 0 each phase but `read_since` is idempotent —
        // any entries from the previous phase that we re-replay get
        // listener-emitted, which is fine (CommentPhaseListener
        // debounces) and aligns the heartbeat's "current phase" with
        // reality from a fresh poll perspective.
        let mut journal_offset: u64 = 0;
        // Most recent observed phase + when we first saw it. The
        // heartbeat reports elapsed time within the current phase, not
        // wall-clock since job start — operators care about "is the
        // current step making progress?".
        let mut current_phase: Option<Phase> = None;
        let mut current_phase_started: Instant = Instant::now();
        let mut last_heartbeat: Instant = Instant::now();
        // True once we've seen the success phase for this `mode`. A
        // subsequent clean ShutOff is then "success poweroff" rather
        // than a "VM died unexpectedly" failure. The build VM
        // *deliberately* powers off after writing BuildDone (via
        // cloud-init power_state); we don't want that to look like
        // crash.
        let mut success_phase_seen = false;

        loop {
            // Shutdown/abort observed at a safe point — the VM is running, so
            // the caller's normal teardown (with handles) will tear it down.
            if cancel.is_cancelled() {
                tracing::warn!(domain = domain_name, "run cancelled; stopping poll");
                Self::drain_progress_tailer(domain_name, &mut progress_tailer, listener, true)
                    .await;
                return FinishReason::Cancelled;
            }

            // Replay any new journal entries. Multiple transitions in
            // one poll window get emitted in order so the listener sees
            // every state change, not just the most recent.
            for (_when, p) in phase::read_since(phase_log, &mut journal_offset) {
                tracing::info!(domain = domain_name, phase = %p, "phase change");
                listener.on_phase(&p).await;
                current_phase_started = Instant::now();
                current_phase = Some(p.clone());

                if p == Phase::Error {
                    Self::drain_progress_tailer(domain_name, &mut progress_tailer, listener, true)
                        .await;
                    return FinishReason::PhaseError;
                }
                if p.is_success_for(mode) {
                    success_phase_seen = true;
                    // We DON'T return immediately on success — the VM
                    // is still powering off (cloud-init's poweroff
                    // takes a few seconds after the script exits). We
                    // let the next domstate poll detect the ShutOff,
                    // which we then map to PhaseDone because
                    // `success_phase_seen` is true.
                }
            }
            Self::drain_progress_tailer(domain_name, &mut progress_tailer, listener, false).await;

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
                    elapsed_secs = elapsed.as_secs(),
                    "heartbeat",
                );
                listener
                    .on_heartbeat(p, elapsed)
                    .await;
                last_heartbeat = Instant::now();
            }

            // VM-state sanity check. If the domain has powered off:
            //   - success_phase_seen=true  → clean exit, treat as PhaseDone (build VM does
            //     this every successful run; bench VM also does this).
            //   - success_phase_seen=false → VM died without writing the success phase. The
            //     run-script either died before it could `phase "error"` or the
            //     kernel/cloud-init panicked. Treat as ShutOff failure.
            match virsh::domstate(self.shell.as_ref(), &self.config.paths, domain_name).await {
                Ok(DomState::ShutOff) | Ok(DomState::Undefined) => {
                    Self::drain_progress_tailer(domain_name, &mut progress_tailer, listener, true)
                        .await;
                    return if success_phase_seen {
                        FinishReason::PhaseDone
                    } else {
                        FinishReason::ShutOff
                    };
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "domstate poll failed"),
            }

            if started.elapsed() > timeout {
                Self::drain_progress_tailer(domain_name, &mut progress_tailer, listener, true)
                    .await;
                return FinishReason::Timeout;
            }
            // Sleep until the next poll, but wake immediately on cancellation
            // so abort is prompt while the VM runs (the top-of-loop check then
            // returns `Cancelled`).
            tokio::select! {
                _ = tokio::time::sleep(poll_interval) => {}
                _ = cancel.cancelled() => {}
            }
        }
    }

    async fn drain_progress_tailer(
        domain_name: &str,
        progress_tailer: &mut Option<&mut (ProgressTailer, ProgressContext)>,
        listener: &dyn PhaseListener,
        final_drain: bool,
    ) {
        let Some((tailer, ctx)) = progress_tailer.as_mut() else {
            return;
        };
        match tailer.drain(final_drain) {
            Ok(lines) => {
                for line in lines {
                    tracing::debug!(
                        domain = domain_name,
                        progress_log = %tailer.path().display(),
                        final_drain,
                        line,
                        "stacks-bench progress stderr line",
                    );
                    let Some(event) = parse_progress_line(&line) else {
                        continue;
                    };
                    listener
                        .on_progress(ProgressUpdate {
                            workflow_step: ctx.workflow_step,
                            run_index: ctx.run_index,
                            requested_run_count: ctx.requested_run_count,
                            phase: event.phase,
                            progress: event.progress,
                            total: event.total,
                            message: event.message,
                        })
                        .await;
                }
            }
            Err(e) => tracing::warn!(
                domain = domain_name,
                progress_log = %tailer.path().display(),
                error = %e,
                "failed to tail stacks-bench progress file",
            ),
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

    /// Best-effort, idempotent teardown of every per-job artifact addressed
    /// purely by `job_id` — no live `JobArtifacts` handle required. This is
    /// the orphan-recovery primitive: a hard-killed daemon or dead reporter can
    /// leave a VM plus its disks/mounts behind with
    /// no driver to run the normal `teardown`. Every step is
    /// logged-and-continued, so a missing artifact (already gone, or never
    /// created) never blocks the rest.
    ///
    /// Order mirrors `teardown`, with the two reconstruct-from-id wrinkles the
    /// handle path gets for free: the source `source.mnt` is unmounted and its
    /// dynamically-named loop device is found via `losetup -j <source.raw>` and
    /// detached **before** the job dir (with the backing file) is removed.
    ///
    /// The return value tracks **only the source loop device** — *not* whether
    /// every artifact was removed. `true` means the loop is verified gone, so
    /// deleting the job dir (with `source.raw`) and failing the row are safe;
    /// `false` means it couldn't be verified-gone, so the job dir — hence
    /// `source.raw`, the only handle to re-find the loop — is **preserved** and
    /// the caller MUST leave the row `running` for the next boot to retry
    /// (else failing it would strand a leaked loop with no way back to it).
    ///
    /// The loop is singled out because it's the one artifact whose recovery
    /// *needs* the backing file: every other step (domain destroy/undefine,
    /// tmpfs/`source.mnt` unmount, `lvremove`, git ref prune) is id-addressable
    /// without it, so those stay best-effort — a transient failure is logged
    /// and does **not** hold the row `running` (we don't want to wedge the
    /// job lifecycle on a flaky `lvremove`/`virsh`; the next op or an
    /// operator reclaims the stray resource).
    pub async fn cleanup_by_job_id(&self, job_id: &str) -> bool {
        let domain_name = format!("sbgh-{job_id}");
        let job_dir = self
            .config
            .paths
            .jobs_dir
            .join(job_id);
        tracing::info!(job_id, domain = domain_name, "orphan cleanup: starting");

        // 1. Domain: destroy a still-running VM, then drop its inactive definition.
        //    Both are unconditional — a destroy/undefine of an already-off/absent
        //    domain is a harmless non-zero we just log.
        if let Err(e) = virsh::destroy(self.shell.as_ref(), &self.config.paths, &domain_name).await
        {
            tracing::warn!(error = %e, domain = domain_name, "orphan cleanup: virsh destroy failed");
        }
        if let Err(e) = virsh::undefine(self.shell.as_ref(), &self.config.paths, &domain_name).await
        {
            tracing::warn!(error = %e, domain = domain_name, "orphan cleanup: virsh undefine failed");
        }

        // 2. Results tmpfs (lives under results_tmpfs_root/<job-id>, OUTSIDE the job
        //    dir, so `remove_dir_all(job_dir)` below won't reach it).
        let tmpfs_dir = self
            .config
            .paths
            .results_tmpfs_root
            .join(job_id);
        self.umount_best_effort(&tmpfs_dir, "results tmpfs")
            .await;
        let _ = std::fs::remove_dir(&tmpfs_dir);

        // 3. Source disk: unmount `source.mnt` (a crash can leave it mounted) and
        //    detach the loop device. The `losetup -fP` device name is dynamic
        //    (`/dev/loopN`), so it's only recoverable by querying the job-id-named
        //    backing file — the piece the 4A cleanup lacked. `source_loop_clear` gates
        //    the job-dir removal below: never delete `source.raw` while a loop that
        //    needs it to be re-found may still be attached.
        self.umount_best_effort(&job_dir.join("source.mnt"), "source.mnt")
            .await;
        let source_loop_clear = self
            .detach_source_loop(&job_dir.join("source.raw"))
            .await;

        // 4. Chainstate LVM snapshot: lvremove --force <vg>/sbgh-<job-id>-chainstate.
        let target = format!("{}/sbgh-{job_id}-chainstate", self.config.lvm.vg_name);
        match self
            .shell
            .run(spec_priv(Path::new("/usr/sbin/lvremove"), &["--force", &target]))
            .await
        {
            Ok(out) if out.status.success() => {}
            Ok(out) => tracing::warn!(
                stderr = %String::from_utf8_lossy(&out.stderr),
                target,
                "orphan cleanup: lvremove non-zero (snapshot likely absent)",
            ),
            Err(e) => tracing::warn!(error = %e, target, "orphan cleanup: lvremove failed"),
        }

        // 5. Git per-job ref (idempotent), then — only if the source loop is verified
        //    gone — the whole job dir (boot/source raw, cidata, domain XML, console.log
        //    all live under it). If a loop may still be attached, PRESERVE the dir:
        //    `source.raw` is the only handle the next recovery has to re-find and
        //    detach the leaked device.
        git_mirror::prune(self.shell.as_ref(), &self.config.paths, job_id).await;
        if !source_loop_clear {
            tracing::error!(
                job_id,
                job_dir = %job_dir.display(),
                "orphan cleanup INCOMPLETE: source loop may still be attached; preserving job dir \
                 (source.raw) for retry on next boot",
            );
            return false;
        }
        match std::fs::remove_dir_all(&job_dir) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => tracing::warn!(error = %e, "orphan cleanup: job dir removal failed"),
        }
        true
    }

    /// `umount <path>` best-effort. A non-zero exit ("not mounted") is the
    /// common, expected case during orphan recovery, so it's only logged at
    /// debug; an actual *error* invoking umount is a warning.
    async fn umount_best_effort(&self, path: &Path, what: &'static str) {
        let path_s = path.display().to_string();
        match self
            .shell
            .run(spec_priv(Path::new("/usr/bin/umount"), &[&path_s]))
            .await
        {
            Ok(out) if out.status.success() => {}
            Ok(_) => tracing::debug!(path = %path_s, what, "orphan cleanup: not mounted (ok)"),
            Err(e) => {
                tracing::warn!(error = %e, path = %path_s, what, "orphan cleanup: umount errored")
            }
        }
    }

    /// Find and detach any loop device backed by `raw` via `losetup -j`. The
    /// `provision`-time device name (`/dev/loopN`) is dynamic, so it can only
    /// be recovered by querying the backing file — exactly the leak the 4A
    /// cleanup couldn't address from the job id alone.
    ///
    /// Returns `true` when no loop remains attached to `raw` (safe to delete
    /// the backing file): `losetup -j` exited 0 and either listed nothing, or
    /// every device it listed detached cleanly. Returns `false` if the query
    /// couldn't be run, exited non-zero, or any `losetup -d` failed — a loop
    /// may still be attached, and any working `losetup` lists an attached
    /// loop, so the only path to deleting `raw` with a live loop is a
    /// query/detach failure, which this catches.
    async fn detach_source_loop(&self, raw: &Path) -> bool {
        let raw_s = raw.display().to_string();
        let out = match self
            .shell
            .run(spec_priv(Path::new("/usr/sbin/losetup"), &["-j", &raw_s]))
            .await
        {
            Ok(out) => out,
            Err(e) => {
                // Couldn't even spawn the query — can't assert the loop is gone.
                tracing::warn!(error = %e, raw = %raw_s, "orphan cleanup: losetup -j failed; can't verify loop is detached");
                return false;
            }
        };
        // A non-zero `losetup -j` is a genuine query failure (a *missing* file
        // exits 0 with empty output on util-linux). Empty stdout then tells us
        // nothing — refuse to green-light deleting `source.raw`; preserve + retry.
        if !out.status.success() {
            tracing::warn!(
                raw = %raw_s,
                stderr = %String::from_utf8_lossy(&out.stderr),
                "orphan cleanup: losetup -j exited non-zero; can't verify loop is detached",
            );
            return false;
        }
        // `losetup -j <file>` prints one line per association:
        //   /dev/loop7: [2049]:12345 (/path/source.raw)
        // Empty output (exit 0) means nothing is attached — the common case.
        let listing = String::from_utf8_lossy(&out.stdout);
        let mut all_clear = true;
        for line in listing.lines() {
            let Some(dev) = line
                .split(':')
                .next()
                .map(str::trim)
                .filter(|d| !d.is_empty())
            else {
                continue;
            };
            match self
                .shell
                .run(spec_priv(Path::new("/usr/sbin/losetup"), &["-d", dev]))
                .await
            {
                Ok(o) if o.status.success() => {
                    tracing::info!(
                        loop_dev = dev,
                        "orphan cleanup: detached leaked source loop device"
                    )
                }
                Ok(o) => {
                    tracing::warn!(
                        stderr = %String::from_utf8_lossy(&o.stderr),
                        loop_dev = dev,
                        "orphan cleanup: losetup -d non-zero; loop may still be attached",
                    );
                    all_clear = false;
                }
                Err(e) => {
                    tracing::warn!(error = %e, loop_dev = dev, "orphan cleanup: losetup -d failed; loop may still be attached");
                    all_clear = false;
                }
            }
        }
        all_clear
    }
}

fn baseline_calibration_id_from_tmpfs(tmpfs: &ResultsTmpfs) -> Option<i64> {
    let from_json = std::fs::read(tmpfs.calibration_json())
        .ok()
        .and_then(|bytes| BaselineCalibrationResult::from_bytes(&bytes))
        .and_then(|result| result.calibration_id());
    from_json.or_else(|| {
        std::fs::read_to_string(tmpfs.baseline_id_file())
            .ok()
            .and_then(|s| s.trim().parse::<i64>().ok())
    })
}

async fn archive_optional(
    store: &dyn ArtifactSink,
    job_id: &str,
    relative: &'static str,
    tmpfs: Option<&ResultsTmpfs>,
    path: impl FnOnce(&ResultsTmpfs) -> PathBuf,
) -> (Option<String>, Option<u64>) {
    let key = artifact_key(job_id, relative);
    let size = match tmpfs {
        Some(tmpfs) => {
            store
                .put(&key, &path(tmpfs))
                .await
        }
        None => None,
    };
    (size.as_ref().map(|_| key), size)
}

/// Bridges the libvirt driver's `Phase` callbacks to recipe-neutral
/// [`EventSink`] calls. The `Phase` → [`PhaseLabel`] mapping lives here so the
/// driver speaks the neutral event surface. Keeping this adapter inside the
/// libvirt driver means the
/// `Driver` trait takes a backend-neutral `&dyn EventSink`.
struct SinkAdapter<'a> {
    sink: &'a dyn EventSink,
}

impl SinkAdapter<'_> {
    fn label(phase: &Phase) -> PhaseLabel {
        PhaseLabel::new(phase.label(), phase.is_terminal())
    }
}

#[async_trait]
impl PhaseListener for SinkAdapter<'_> {
    async fn on_phase(&self, phase: &Phase) {
        // A just-entered phase → 0 elapsed (matches the prior reporting path,
        // whose prose still reads naturally as "running for 00:00:00").
        //
        // A phase is a reliable event: it must
        // not be lost. A `SinkClosed` here means the reporter is gone (it
        // panicked) — surfaced loudly below.
        //
        // TODO(cancel-safety): acting on this (aborting the in-flight
        // run rather than logging-and-continuing) needs the driver cancellation
        // path plus `cleanup_by_job_id` are complete; a dirty drop here would
        // leak the VM. The `PhaseListener` boundary will grow an abort signal
        // there (`on_phase` returns `()` and the driver has no cancellation path
        // yet). Do not copy this swallow forward without that abort handling.
        if self
            .sink
            .phase(Self::label(phase), Duration::ZERO)
            .await
            .is_err()
        {
            tracing::error!(
                phase = %phase,
                "reliable phase event dropped: reporting sink closed (reporter gone) — \
                 VM-safe abort deferred until cancel-safety is implemented (see TODO)",
            );
        }
    }

    async fn on_heartbeat(&self, phase: &Phase, elapsed: Duration) {
        self.sink
            .heartbeat(Self::label(phase), elapsed)
            .await;
    }

    async fn on_progress(&self, progress: ProgressUpdate) {
        self.sink
            .progress(progress)
            .await;
    }
}

async fn seed_sqlite_from_store(
    store: &dyn ArtifactSink,
    seed_key: &str,
    tmpfs: &ResultsTmpfs,
) -> anyhow::Result<()> {
    let src = store
        .get(seed_key)
        .await
        .with_context(|| format!("resolve carried SQLite artifact {seed_key}"))?;
    let dest = tmpfs.sqlite_file();
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create carried SQLite parent {}", parent.display()))?;
    }
    std::fs::copy(&src, &dest).with_context(|| {
        format!(
            "seed carried SQLite artifact {seed_key} from {} to {}",
            src.display(),
            dest.display()
        )
    })?;
    tracing::info!(
        seed_key,
        dest = %dest.display(),
        "seeded carried benchmark SQLite DB",
    );
    Ok(())
}

#[async_trait]
impl Driver for LibvirtDriver {
    /// Bench's `TaskSpec.args` are this run's benchmark CLI args; `Placement`
    /// carries the placement cpuset. Wraps the inherent
    /// [`run_benchmark`](LibvirtDriver::run_benchmark) (bench specifics still
    /// live there; the cloud-init split remains deferred) and adapts
    /// its `BenchmarkOutcome` into the neutral [`DriverOutcome`].
    async fn run_task(
        &self,
        ctx: &TaskContext<'_>,
        spec: &TaskSpec,
        sink: &dyn EventSink,
        cancel: &CancellationToken,
        placement: &Placement,
    ) -> anyhow::Result<DriverOutcome> {
        let adapter = SinkAdapter { sink };
        let outcome = self
            .run_benchmark(
                ctx,
                spec,
                &adapter,
                cancel,
                placement
                    .vcpu_cpuset
                    .as_deref(),
            )
            .await?;
        let status = match outcome.status {
            OutcomeStatus::Ok => DriverStatus::Completed,
            OutcomeStatus::Failed(e) => DriverStatus::Failed(e),
        };
        Ok(DriverOutcome {
            status,
            summary: outcome.summary,
        })
    }

    async fn cleanup_by_job_id(&self, job_id: &str) -> bool {
        // Fully-qualified inherent call: inherent methods win over trait methods
        // in path resolution, so this is unambiguously the inherent impl — not a
        // recursive trait call.
        LibvirtDriver::cleanup_by_job_id(self, job_id).await
    }
}

#[cfg(test)]
mod tests;
