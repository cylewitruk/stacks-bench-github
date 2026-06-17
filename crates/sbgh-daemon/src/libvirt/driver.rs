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
use sbgh_core::bench_args::effective_arg_string;
use sbgh_core::config::DaemonConfig;
use tokio_util::sync::CancellationToken;

use crate::artifact_store::{artifact_key, build_store_or_local};
use crate::binary_cache::{self, BinaryCache, BuildFingerprint, CacheEnvironment};
use crate::driver::{Driver, DriverOutcome, DriverStatus, Placement, TaskSpec};
use crate::events::{EventSink, PhaseLabel};
use crate::libvirt::boot::BootDisk;
use crate::libvirt::cloudinit::{BenchPhaseParams, CloudInitArtifacts, CloudInitCommon};
use crate::libvirt::domain::{self, DomainSpec};
use crate::libvirt::lvm::ChainstateSnapshot;
use crate::libvirt::phase::{self, Phase, PollMode};
use crate::libvirt::shell::{Shell, spec, spec_priv};
use crate::libvirt::source::SourceDisk;
use crate::libvirt::tmpfs::ResultsTmpfs;
use crate::libvirt::virsh::{self, DomState};
use crate::libvirt::{forensics, git_mirror};
use crate::recipe::TaskContext;

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

// ── Binary-cache build fingerprint constants (item 0025, v9) ──
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

/// Assemble the build fingerprint for `commit` (item 0025, v9). Reads
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
        && let Some(channel) = binary_cache::toolchain_channel(&toolchain_toml)
    {
        return Some(channel);
    }
    let legacy = show_repo_file(shell, git_binary, mirror, commit, "rust-toolchain").await?;
    binary_cache::legacy_toolchain_channel(&legacy)
}

/// The daemon's **current** build environment — the [`CacheEnvironment`] half
/// of a fingerprint (everything but the per-run `commit` + `toolchain`): the
/// invariant build invocation (profile / triple / recipe / protocol) plus the
/// golden-image identity. `None` when the golden image is unreadable.
///
/// Single source of truth for the env, shared by [`assemble_fingerprint`] (the
/// build path) and the pin resolver (`set_pinned_by_commit`), so a cached
/// entry's stored env and the daemon's current env are compared on identical
/// terms.
pub fn current_cache_environment(golden_image: &Path) -> Option<CacheEnvironment> {
    let image_id = binary_cache::image_proxy_id(golden_image).ok()?;
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
    /// v10 (0005): build-only mode — stop after the build VM publishes the
    /// artifact; skip the bench phase entirely.
    build_only: bool,
}

pub struct LibvirtDriver {
    config: Arc<DaemonConfig>,
    shell: Arc<dyn Shell>,
    /// Opt-in `stacks-bench` binary cache (item 0025, v9). `None` when
    /// `[artifacts.binary_cache].enabled` is false (the default), in which case
    /// the build/bench flow is byte-identical to before.
    binary_cache: Option<Arc<BinaryCache>>,
}

impl LibvirtDriver {
    pub fn new(config: Arc<DaemonConfig>, shell: Arc<dyn Shell>) -> Self {
        let binary_cache = binary_cache::build_binary_cache(&config);
        Self { config, shell, binary_cache }
    }

    /// Run one libvirt job to a terminal outcome. The shared
    /// provision → build → publish path, then either the bench phase
    /// (`build_only = false`) or a stop after publish (`build_only = true`,
    /// v10 0005 — the warming primitive). Forensics + teardown are identical.
    pub async fn run_benchmark(
        &self,
        ctx: &TaskContext<'_>,
        spec: &TaskSpec,
        listener: &dyn PhaseListener,
        cancel: &CancellationToken,
        // Phase 5 CPU pinning: the libvirt cpuset this job's slot owns, or
        // `None` to float. Threaded down to the domain XML.
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
        let inputs = RunInputs {
            repository: ctx.repository,
            commit: ctx.commit,
            bench_args: &spec.args,
            sqlite_seed_key: spec
                .sqlite_seed_key
                .as_deref(),
            build_only: spec.build_only,
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
        // or S3 with a local mirror — Phase 2). The summary records each
        // artifact's store **key** (`<job_id>/<relative>`, Decision 0002), not a
        // bare path; `put` returns the byte size, or `None` when the VM produced
        // no such file (or, for S3, the local mirror write failed — a failed S3
        // upload is logged but still returns the local size, Decision 0003).
        let store = build_store_or_local(self.config.as_ref());
        let tmpfs = arts.tmpfs.as_ref();

        let sqlite_key = artifact_key(&job_id, forensics::SQLITE_RELATIVE);
        let sqlite_size_bytes = match tmpfs {
            Some(t) => {
                store
                    .put(&sqlite_key, &t.sqlite_file())
                    .await
            }
            None => None,
        };
        let sqlite_archived_path = sqlite_size_bytes.map(|_| sqlite_key);

        // The append-only phase journal — cheap, makes per-job "how long was
        // each phase" trivial after the job dir is gone.
        let phase_log_key = artifact_key(&job_id, forensics::PHASE_LOG_RELATIVE);
        let phase_log_size_bytes = match tmpfs {
            Some(t) => {
                store
                    .put(&phase_log_key, &t.phase_log())
                    .await
            }
            None => None,
        };
        let phase_log_archived_path = phase_log_size_bytes.map(|_| phase_log_key);

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
        let run_json_key = artifact_key(&job_id, forensics::RUN_JSON_RELATIVE);
        let run_json_size_bytes = match tmpfs {
            Some(t) => {
                store
                    .put(&run_json_key, &t.run_json())
                    .await
            }
            None => None,
        };
        let run_json_archived_path = run_json_size_bytes.map(|_| run_json_key);

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
        // v16: fetch the commit into the bare mirror first (fingerprinting
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
        // item 0025 (v9) + v16: if a fingerprint-matched binary is cached, the
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
                        .build_memory
                        .as_bytes(),
                    &cidata.build_iso_path,
                    job_dir,
                    domain_name,
                    arts,
                    listener,
                    cancel,
                    vcpu_cpuset,
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

        // v10 (0005): a build-only job stops here — there is no bench phase. Its
        // purpose is the cached artifact, so it succeeds ONLY if the artifact is
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
                    .bench_memory
                    .as_bytes(),
                &cidata.bench_iso_path,
                job_dir,
                domain_name,
                arts,
                listener,
                cancel,
                vcpu_cpuset,
            )
            .await?;
        Ok(bench_reason)
    }

    async fn prepare_git_mirror(&self, inputs: &RunInputs<'_>, job_id: &str) -> anyhow::Result<()> {
        let repo_url = format!("https://github.com/{}.git", inputs.repository);
        git_mirror::ensure(self.shell.as_ref(), &self.config.paths, &repo_url).await?;
        git_mirror::fetch_sha(self.shell.as_ref(), &self.config.paths, job_id, inputs.commit).await
    }

    /// Resolve the binary-cache plan before source-disk provisioning (v16).
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
            digest: hit.meta.digest,
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
        // Drive the reporter with a single opaque `build_cached:<digest>` phase
        // (item 0025, v9): each surface interprets it — the Slack card marks the
        // Build row done with a "Reused cached build · <digest>" title; the
        // GitHub surface shows "build (cached)". The build VM that normally emits
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
                    &self
                        .config
                        .server
                        .service_user,
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
                                &self
                                    .config
                                    .server
                                    .service_user,
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
                        &self
                            .config
                            .server
                            .service_user,
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
                &self
                    .config
                    .server
                    .service_user,
            )
            .await?,
        );
        if let (Some(seed_key), Some(tmpfs)) = (inputs.sqlite_seed_key, arts.tmpfs.as_ref()) {
            seed_sqlite_from_store(self.config.as_ref(), seed_key, tmpfs).await?;
        }
        if let (BuildPlan::CacheHit { binary, .. }, Some(tmpfs)) = (&plan, arts.tmpfs.as_ref()) {
            let _ = std::fs::copy(binary, tmpfs.stacks_bench_binary());
        }

        // Cloud-init: two ISOs, one per phase, distinct instance-ids
        // so cloud-init re-runs user-data on the second boot.
        let stacks_bench_args = effective_arg_string(
            inputs.bench_args,
            &self
                .config
                .stacks_bench
                .default_args,
        );
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
                    .runner
                    .host_cpus
                    .as_deref(),
            ),
        })?;
        std::fs::write(&domain_xml_path, &xml)?;

        virsh::define(self.shell.as_ref(), &self.config.paths, &domain_xml_path).await?;
        arts.domain_defined = true;
        virsh::start(self.shell.as_ref(), &self.config.paths, domain_name).await?;
        arts.domain_started = true;

        let phase_log = tmpfs_ref.phase_log();
        let reason = self
            .poll_to_completion(domain_name, &phase_log, listener, mode, cancel)
            .await;
        Ok(reason)
    }

    async fn poll_to_completion(
        &self,
        domain_name: &str,
        phase_log: &Path,
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

            // VM-state sanity check. If the domain has powered off:
            //   - success_phase_seen=true  → clean exit, treat as PhaseDone (build VM does
            //     this every successful run; bench VM also does this).
            //   - success_phase_seen=false → VM died without writing the success phase. The
            //     run-script either died before it could `phase "error"` or the
            //     kernel/cloud-init panicked. Treat as ShutOff failure.
            match virsh::domstate(self.shell.as_ref(), &self.config.paths, domain_name).await {
                Ok(DomState::ShutOff) | Ok(DomState::Undefined) => {
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
    /// purely by `job_id` — no live [`JobArtifacts`] handle required. This is
    /// the orphan-recovery primitive (roadmap-v5 Phase 4B-2): a hard-killed
    /// daemon or dead reporter can leave a VM plus its disks/mounts behind with
    /// no driver to run the normal [`teardown`](Self::teardown). Every step is
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

/// Bridges the libvirt driver's `Phase` callbacks to recipe-neutral
/// [`EventSink`] calls. The `Phase` → [`PhaseLabel`] mapping lives here so the
/// driver speaks the neutral event surface — roadmap-v8 Phase 1 moved this
/// adapter *inside* the libvirt driver (it was on the bench recipe), so the
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
        // A phase is a RELIABLE event (roadmap-v5 channel discipline): it must
        // not be lost. A `SinkClosed` here means the reporter is gone (it
        // panicked) — surfaced loudly below.
        //
        // TODO(Phase 4 — cancel-safety): *acting* on this (aborting the in-flight
        // run rather than logging-and-continuing) needs the driver cancellation
        // path + `cleanup_by_job_id` Phase 4 builds — a dirty drop here would
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
                 VM-safe abort deferred to Phase 4 cancel-safety (see TODO)",
            );
        }
    }

    async fn on_heartbeat(&self, phase: &Phase, elapsed: Duration) {
        self.sink
            .heartbeat(Self::label(phase), elapsed)
            .await;
    }
}

async fn seed_sqlite_from_store(
    config: &DaemonConfig,
    seed_key: &str,
    tmpfs: &ResultsTmpfs,
) -> anyhow::Result<()> {
    let store = build_store_or_local(config);
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
    /// carries the Phase-5 cpuset. Wraps the inherent
    /// [`run_benchmark`](LibvirtDriver::run_benchmark) (bench specifics still
    /// live there — the cloud-init split is deferred to roadmap-v6) and adapts
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

    /// Share this driver's binary-cache `Arc` (when the cache is enabled) so
    /// the runner's pin manager re-pins / evicts under the same mutex (item
    /// 0025).
    fn binary_cache(&self) -> Option<Arc<BinaryCache>> {
        self.binary_cache.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use sbgh_core::config::{
        ApiConfig, BaselineReport, DaemonServerConfig, GitHubConfig, LvmConfig, PathsConfig,
        PrReport, ReportingConfig, RunnerConfig, StacksBenchConfig, VmConfig,
    };
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::*;
    use crate::job_source::{ProgressTarget, RunnableJob};
    use crate::libvirt::shell::test_support::{PreparedReply, RecordingShell};

    /// A platform-neutral [`TaskContext`] borrowed from a fake job — the
    /// `run_benchmark` inputs the driver actually reads.
    fn ctx_of(job: &RunnableJob) -> TaskContext<'_> {
        TaskContext {
            job_id: job.id,
            repository: &job.repository,
            commit: &job.commit,
        }
    }

    // ── binary-cache fingerprint (item 0025, v9) ──

    #[tokio::test]
    async fn assemble_fingerprint_reads_toolchain_from_the_mirror() {
        let tmp = TempDir::new().unwrap();
        let golden = tmp
            .path()
            .join("golden.qcow2");
        std::fs::write(&golden, b"img").unwrap();
        let shell = RecordingShell::new();
        shell.reply(PreparedReply::with_stdout("[toolchain]\nchannel = \"1.95.0\"\n"));

        let fp = assemble_fingerprint(
            &shell,
            std::path::Path::new("/usr/bin/git"),
            std::path::Path::new("/var/lib/sbgh/mirror.git"),
            &golden,
            "deadbeef",
        )
        .await
        .expect("declared toolchain → Some fingerprint");
        assert_eq!(fp.commit, "deadbeef");
        assert_eq!(fp.toolchain, "1.95.0");
        assert_eq!(fp.target_triple, "x86_64-unknown-linux-gnu");

        // Read from the bare mirror via `git --git-dir … show <sha>:<file>` — no
        // source-disk mount (the disk is detached after provisioning).
        let calls = shell.calls();
        assert!(
            calls[0]
                .program
                .ends_with("git")
        );
        assert!(
            calls[0]
                .args
                .iter()
                .any(|a| a == "--git-dir")
        );
        assert!(
            calls[0]
                .args
                .iter()
                .any(|a| a == "show")
        );
        assert!(
            calls[0]
                .args
                .iter()
                .any(|a| a == "deadbeef:rust-toolchain.toml")
        );
    }

    #[tokio::test]
    async fn assemble_fingerprint_falls_back_to_legacy_toolchain_file() {
        let tmp = TempDir::new().unwrap();
        let golden = tmp
            .path()
            .join("golden.qcow2");
        std::fs::write(&golden, b"img").unwrap();
        let shell = RecordingShell::new();
        shell.reply(PreparedReply::fail("fatal: path does not exist"));
        shell.reply(PreparedReply::with_stdout("stable\n"));

        let fp = assemble_fingerprint(
            &shell,
            std::path::Path::new("/usr/bin/git"),
            std::path::Path::new("/var/lib/sbgh/mirror.git"),
            &golden,
            "deadbeef",
        )
        .await
        .expect("legacy rust-toolchain → Some fingerprint");
        assert_eq!(fp.toolchain, "stable");

        let calls = shell.calls();
        assert!(
            calls[0]
                .args
                .iter()
                .any(|a| a == "deadbeef:rust-toolchain.toml")
        );
        assert!(
            calls[1]
                .args
                .iter()
                .any(|a| a == "deadbeef:rust-toolchain")
        );
    }

    #[tokio::test]
    async fn assemble_fingerprint_keys_floating_channel_and_none_when_missing() {
        let tmp = TempDir::new().unwrap();
        let golden = tmp
            .path()
            .join("golden.qcow2");
        std::fs::write(&golden, b"img").unwrap();

        // A floating channel is keyed as itself (pragmatic reuse, not provenance).
        let floating = RecordingShell::new();
        floating.reply(PreparedReply::with_stdout("[toolchain]\nchannel = \"stable\"\n"));
        let fp = assemble_fingerprint(
            &floating,
            std::path::Path::new("/git"),
            std::path::Path::new("/m"),
            &golden,
            "sha",
        )
        .await
        .expect("floating channel still keys");
        assert_eq!(fp.toolchain, "stable");

        // Both toolchain file probes fail → None.
        let missing = RecordingShell::new();
        missing.reply(PreparedReply::fail("fatal: path does not exist"));
        missing.reply(PreparedReply::fail("fatal: path does not exist"));
        assert!(
            assemble_fingerprint(
                &missing,
                std::path::Path::new("/git"),
                std::path::Path::new("/m"),
                &golden,
                "sha",
            )
            .await
            .is_none(),
            "missing toolchain files → None"
        );
    }

    #[derive(Default)]
    struct RecordingListener {
        phases: std::sync::Mutex<Vec<Phase>>,
    }

    #[async_trait::async_trait]
    impl PhaseListener for RecordingListener {
        async fn on_phase(&self, phase: &Phase) {
            self.phases
                .lock()
                .unwrap()
                .push(phase.clone());
        }
    }

    /// Enabled cache + a fingerprint-matched entry: the driver resolves the hit
    /// before source provisioning, creates a minimal binary-only source disk,
    /// reports the cached build, and skips the build VM.
    #[tokio::test]
    async fn enabled_cache_hit_uses_minimal_source_disk_and_skips_build_vm() {
        let tmp = TempDir::new().unwrap();
        let mut cfg = test_config(&tmp);
        // `image_proxy_id` needs a real golden-image file; enable the cache.
        std::fs::write(&cfg.vm.golden_image, b"golden").unwrap();
        let cache_dir = tmp.path().join("bincache");
        cfg.artifacts
            .binary_cache
            .enabled = true;
        cfg.artifacts.binary_cache.dir = cache_dir.clone();

        // The exact fingerprint the driver will compute for this run, then a
        // cached binary published under it.
        let toolchain = "[toolchain]\nchannel = \"1.95.0\"\n";
        let fp = BuildFingerprint {
            commit: "abc123def456".into(),
            toolchain: binary_cache::toolchain_channel(toolchain).unwrap(),
            profile: BUILD_PROFILE.into(),
            features: String::new(),
            rustflags: String::new(),
            target_triple: BUILD_TARGET_TRIPLE.into(),
            recipe_version: BUILD_RECIPE_VERSION,
            image_id: binary_cache::image_proxy_id(&cfg.vm.golden_image).unwrap(),
            protocol_version: BUILD_PROTOCOL_VERSION.into(),
        };
        let cached_bin = tmp.path().join("cached-bin");
        std::fs::write(&cached_bin, b"CACHED").unwrap();
        binary_cache::BinaryCache::new(cache_dir.clone(), 1 << 30)
            .publish(&fp, &cached_bin, 1, false)
            .unwrap();

        let cfg = Arc::new(cfg);
        let job = fake_job();
        std::fs::create_dir_all(&cfg.paths.git_mirror).unwrap();
        let tmpfs_dir = cfg
            .paths
            .results_tmpfs_root
            .join(job.id.to_string());
        std::fs::create_dir_all(&tmpfs_dir).unwrap();
        std::fs::write(tmpfs_dir.join(".phase-log"), b"1700000000 done\n").unwrap();

        // Shell: fetch, `git show` for the fingerprint, minimal source seed,
        // shared artifacts, bench VM only, teardown.
        let shell = RecordingShell::new();
        shell
            .expect_ok(1) // git fetch_sha
            .reply(PreparedReply::with_stdout(toolchain)) // git show <sha>:rust-toolchain.toml
            .expect_ok(1) // qemu-img create
            .expect_ok(1) // truncate (minimal source)
            .expect_ok(1) // mkfs.ext4
            .reply(PreparedReply::with_stdout("/dev/loop9\n")) // losetup --show
            .expect_ok(1) // mount
            .expect_ok(1) // chown empty fs to service user
            .expect_ok(1) // chown seeded binary tree to root
            .expect_ok(1) // umount
            .expect_ok(1) // losetup -d
            .reply(PreparedReply::with_stdout("  mainnet-2026-05-21\n")) // lvs
            .expect_ok(1) // lvcreate snapshot
            .expect_ok(1) // mount tmpfs
            .expect_ok(1) // cloud-localds (build ISO, optional but still rendered)
            .expect_ok(1) // cloud-localds (bench ISO)
            // ── NO build VM lifecycle on a cache hit ──
            .expect_ok(1) // virsh define (bench)
            .expect_ok(1) // virsh start (bench)
            .reply(PreparedReply::with_stdout("shut off\n")) // domstate bench
            .expect_ok(1) // virsh destroy
            .expect_ok(1) // virsh undefine
            .expect_ok(1) // umount tmpfs
            .expect_ok(1) // lvremove
            .expect_ok(1); // git update-ref -d
        let shell = Arc::new(shell);

        let driver = LibvirtDriver::new(cfg.clone(), shell.clone());
        let listener = RecordingListener::default();
        let outcome = driver
            .run_benchmark(
                &ctx_of(&job),
                &task_spec(vec![], false),
                &listener,
                &CancellationToken::new(),
                None,
            )
            .await;
        let outcome = outcome.expect("cache-hit benchmark should run");
        assert_eq!(outcome.status, OutcomeStatus::Ok);

        // The reporter sees staging, then a cached-build completion.
        assert_eq!(
            *listener
                .phases
                .lock()
                .unwrap(),
            vec![
                Phase::Other(format!("build_cache_staging:{}", &fp.digest()[..12])),
                Phase::Other(format!("build_cached:{}", &fp.digest()[..12])),
                Phase::Done,
            ]
        );
        // It fetched the commit, read the toolchain, ran the minimal seed
        // sequence, skipped the checkout/chown-heavy path, and never touched a
        // build VM.
        let calls = shell.calls();
        assert!(calls.iter().any(|c| {
            c.args
                .iter()
                .any(|a| a == "abc123def456:rust-toolchain.toml")
        }));
        assert!(
            !calls
                .iter()
                .any(|c| c.program.ends_with("git")
                    && c.args
                        .iter()
                        .any(|a| a == "clone" || a == "checkout")),
            "cache hit must not checkout a source tree"
        );
        assert!(calls.iter().any(|c| {
            c.program.ends_with("losetup")
                && c.args
                    .contains(&"-d".to_string())
        }));
        assert!(
            !calls
                .iter()
                .any(|c| c.program.ends_with("virsh")
                    && c.args
                        .iter()
                        .any(|a| a.contains("domain.build.xml"))),
            "no build VM define/start on a cache hit"
        );
    }

    #[tokio::test]
    async fn sqlite_seed_key_copies_group_db_into_results_tmpfs() {
        let tmp = TempDir::new().unwrap();
        let cfg = test_config(&tmp);
        let seed_key =
            crate::artifact_store::group_artifact_key("group1", "shared/stacks-bench.db");
        let seed_path = cfg
            .paths
            .results_archive_dir
            .join(&seed_key);
        std::fs::create_dir_all(seed_path.parent().unwrap()).unwrap();
        std::fs::write(&seed_path, b"group sqlite").unwrap();

        let tmpfs = ResultsTmpfs {
            mount_dir: tmp.path().join("results"),
            size_mib: 256,
        };
        std::fs::create_dir_all(&tmpfs.mount_dir).unwrap();

        seed_sqlite_from_store(&cfg, &seed_key, &tmpfs)
            .await
            .unwrap();

        assert_eq!(std::fs::read(tmpfs.sqlite_file()).unwrap(), b"group sqlite");
    }

    fn test_config(tmp: &TempDir) -> DaemonConfig {
        let p = tmp.path();
        DaemonConfig {
            server: DaemonServerConfig {
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
                build_vcpus: 4,
                bench_vcpus: 2,
                build_memory: sbgh_core::memory::MemorySize::from_gib(16),
                bench_memory: sbgh_core::memory::MemorySize::from_gib(8),
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
            api: ApiConfig {
                listen: vec!["127.0.0.1:8787".into()],
                cookie_path: "/tmp/sbgh-test.cookie".into(),
                ingest_token: None,
            },
            // The driver doesn't report; value is irrelevant here.
            reporting: ReportingConfig {
                pr_report: PrReport::Both,
                baseline_report: BaselineReport::Check,
                noise_cv_pct: None,
            },
            runner: RunnerConfig {
                max_concurrent_jobs: 1,
                max_clean_repetitions: 5,
                cpu_sets: vec![],
                host_cpus: None,
            },
            artifacts: Default::default(),
            slack: Default::default(),
            llm: Default::default(),
        }
    }

    fn task_spec(args: Vec<String>, build_only: bool) -> TaskSpec {
        TaskSpec {
            args,
            build_only,
            sqlite_seed_key: None,
        }
    }

    fn fake_job() -> RunnableJob {
        RunnableJob {
            id: Uuid::new_v4(),
            benchmark_group_id: Uuid::new_v4(),
            benchmark_spec_id: Uuid::new_v4(),
            benchmark_run_index: 0,
            requested_run_count: 1,
            group_artifact_prefix: Uuid::new_v4().to_string(),
            repository: "acme/widgets".into(),
            commit: "abc123def456".into(),
            git_ref_display: "PR #42".into(),
            git_ref_kind: sbgh_core::models::GitRefKind::Branch,
            installation_id: 7,
            task_kind: sbgh_core::models::TaskKind::Benchmark,
            build_target: sbgh_core::models::BuildTarget::StacksBench,
            workload_key: None,
            bench_args: vec!["--iters=2".into()],
            progress: ProgressTarget::PullRequest {
                pr_number: 42,
                comment_id: Some(1000),
                check_run_id: None,
                check_run_url: None,
            },
            claim_token: None,
        }
    }

    /// Build a shell that returns canned outputs in the order the driver
    /// will issue them, all the way through provisioning, both VM
    /// lifecycles, and teardown. The test pre-writes the phase log
    /// with both `build_done` and `done` so each poll loop sees its
    /// success phase on iteration 1; the very next domstate poll
    /// returns ShutOff (success-poweroff) which we map to PhaseDone.
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
            // Two cloud-localds calls — one per cidata ISO (build, bench).
            .expect_ok(1) // cloud-localds (build)
            .expect_ok(1) // cloud-localds (bench)
            // ── build VM lifecycle ──────────────────────────────────
            .expect_ok(1) // virsh define (build)
            .expect_ok(1) // virsh start (build)
            .reply(PreparedReply::with_stdout("shut off\n")) // virsh domstate (build poll, ShutOff after seeing BuildDone)
            // ── bench VM lifecycle ──────────────────────────────────
            .expect_ok(1) // virsh define (bench, replaces inactive build def)
            .expect_ok(1) // virsh start (bench)
            .reply(PreparedReply::with_stdout("shut off\n")) // virsh domstate (bench poll, ShutOff after seeing Done)
            // ── teardown ────────────────────────────────────────────
            .expect_ok(1) // virsh destroy
            .expect_ok(1) // virsh undefine
            .expect_ok(1) // umount tmpfs
            .expect_ok(1) // lvremove
            .expect_ok(1); // git update-ref -d (prune)
        shell
    }

    /// Like [`happy_path_shell`] but for a **build-only** run: the bench VM
    /// lifecycle is absent — the driver stops after the build VM publishes.
    fn build_only_shell() -> RecordingShell {
        let shell = RecordingShell::new();
        shell
            .expect_ok(1) // git fetch_sha
            .expect_ok(1) // qemu-img create
            .expect_ok(1) // truncate (source)
            .expect_ok(1) // mkfs.ext4
            .reply(PreparedReply::with_stdout("/dev/loop42\n")) // losetup -fP --show
            .expect_ok(1) // mount loop
            .expect_ok(1) // chown
            .expect_ok(1) // rmdir lost+found
            .expect_ok(1) // git clone --reference
            .expect_ok(1) // git checkout
            .expect_ok(1) // umount source
            .expect_ok(1) // losetup -d
            .reply(PreparedReply::with_stdout("  mainnet-2026-05-21\n")) // lvs
            .expect_ok(1) // lvcreate snapshot
            .expect_ok(1) // mount tmpfs
            // Both cidata ISOs are still provisioned upfront (provision is shared).
            .expect_ok(1) // cloud-localds (build)
            .expect_ok(1) // cloud-localds (bench)
            // ── build VM lifecycle ──────────────────────────────────
            .expect_ok(1) // virsh define (build)
            .expect_ok(1) // virsh start (build)
            .reply(PreparedReply::with_stdout("shut off\n")) // virsh domstate (build poll, ShutOff after BuildDone)
            // ── NO bench VM lifecycle — build-only stops after publish ──
            // ── teardown ────────────────────────────────────────────
            .expect_ok(1) // virsh destroy
            .expect_ok(1) // virsh undefine
            .expect_ok(1) // umount tmpfs
            .expect_ok(1) // lvremove
            .expect_ok(1); // git update-ref -d (prune)
        shell
    }

    /// v10 (0005): a build-only run goes provision → build → stop — the bench
    /// VM lifecycle is skipped entirely. And (M1, Codex) because its
    /// purpose is the cached artifact, with the binary cache disabled (the
    /// default test config) it **fails closed** instead of reporting a
    /// hollow success — while still never touching the bench VM.
    #[tokio::test]
    async fn build_only_skips_bench_and_fails_closed_without_cache() {
        let tmp = TempDir::new().unwrap();
        let cfg = Arc::new(test_config(&tmp));
        let job = fake_job();

        std::fs::create_dir_all(&cfg.paths.git_mirror).unwrap();
        let tmpfs_dir = cfg
            .paths
            .results_tmpfs_root
            .join(job.id.to_string());
        std::fs::create_dir_all(&tmpfs_dir).unwrap();
        // Build-only: only the build VM runs, so seed `build_done` (no `done`).
        std::fs::write(tmpfs_dir.join(".phase-log"), b"1700000000 build_done\n").unwrap();

        let shell = Arc::new(build_only_shell());
        let driver = LibvirtDriver::new(cfg.clone(), shell.clone());
        let outcome = driver
            .run_benchmark(
                &ctx_of(&job),
                &task_spec(vec![], true), // build_only
                &NoopPhaseListener,
                &CancellationToken::new(),
                None,
            )
            .await
            .expect("driver returns Ok even when the run fails");

        // M1: the build ran but nothing was cached (cache disabled) → fail closed
        // rather than a hollow "build-only succeeded".
        match outcome.status {
            OutcomeStatus::Failed(ref m) => {
                assert!(m.contains("no cached artifact"), "fail-closed reason: {m}")
            }
            other => panic!("expected fail-closed Failed, got {other:?}"),
        }
        // The build VM still ran (last phase = build) and no bench measurement
        // was produced.
        assert_eq!(outcome.summary["last_phase"], "build_done");
        assert!(
            outcome.summary["run_json_archived_path"].is_null(),
            "build-only produces no run.json",
        );

        // The command sequence proves the bench VM lifecycle is absent.
        let programs: Vec<String> = shell
            .calls()
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
            "git",
            "qemu-img",
            "truncate",
            "mkfs.ext4",
            "losetup",
            "mount",
            "chown",
            "rmdir",
            "git",
            "git",
            "umount",
            "losetup",
            "lvs",
            "lvcreate",
            "mount",
            "cloud-localds", // build ISO
            "cloud-localds", // bench ISO (still provisioned)
            "virsh",         // define (build)
            "virsh",         // start (build)
            "virsh",         // domstate poll → ShutOff after BuildDone
            // no bench define/start/poll
            "virsh",    // destroy
            "virsh",    // undefine
            "umount",   // tmpfs
            "lvremove", // chainstate
            "git",      // mirror prune
        ];
        assert_eq!(programs, expected, "build-only must skip the bench VM lifecycle");
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
        // Two-phase happy path: build VM writes `build_done`, then bench
        // VM writes `done`. Both pre-seeded so each phase's poll loop
        // observes its success phase on the first read.
        std::fs::write(tmpfs_dir.join(".phase-log"), b"1700000000 build_done\n1700000060 done\n")
            .unwrap();

        let shell = Arc::new(happy_path_shell());
        let driver = LibvirtDriver::new(cfg.clone(), shell.clone());
        let outcome = driver
            .run_benchmark(
                &ctx_of(&job),
                &task_spec(job.bench_args.clone(), false),
                &NoopPhaseListener,
                &CancellationToken::new(),
                None,
            )
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
            "cloud-localds", // cidata ISO (build)
            "cloud-localds", // cidata ISO (bench)
            // build phase
            "virsh", // define (build)
            "virsh", // start (build)
            "virsh", // domstate poll → ShutOff after BuildDone
            // bench phase — same domain redefined with new memory + cidata
            "virsh", // define (bench)
            "virsh", // start (bench)
            "virsh", // domstate poll → ShutOff after Done
            // teardown
            "virsh",    // destroy
            "virsh",    // undefine
            "umount",   // tmpfs unmount
            "lvremove", // chainstate teardown
            "git",      // mirror prune
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
            .run_benchmark(
                &ctx_of(&job),
                &task_spec(job.bench_args.clone(), false),
                &NoopPhaseListener,
                &CancellationToken::new(),
                None,
            )
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
            .run_benchmark(
                &ctx_of(&job),
                &task_spec(job.bench_args.clone(), false),
                &NoopPhaseListener,
                &CancellationToken::new(),
                None,
            )
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

    /// Cancellation is honored at the poll loop (not mid-provision): a
    /// pre-cancelled token lets provisioning finish atomically, then the poll
    /// loop returns `cancelled` and the **normal teardown** (with handles —
    /// including the source loop device) runs.
    #[tokio::test]
    async fn cancellation_breaks_at_poll_loop_and_tears_down() {
        let tmp = TempDir::new().unwrap();
        let cfg = Arc::new(test_config(&tmp));
        let job = fake_job();
        std::fs::create_dir_all(&cfg.paths.git_mirror).unwrap();
        std::fs::create_dir_all(
            cfg.paths
                .results_tmpfs_root
                .join(job.id.to_string()),
        )
        .unwrap();

        let shell = Arc::new(happy_path_shell());
        let driver = LibvirtDriver::new(cfg.clone(), shell.clone());
        let cancel = CancellationToken::new();
        cancel.cancel(); // pre-cancelled → the poll loop's top check fires first

        let outcome = driver
            .run_benchmark(
                &ctx_of(&job),
                &task_spec(job.bench_args.clone(), false),
                &NoopPhaseListener,
                &cancel,
                None,
            )
            .await
            .expect("driver returns Ok");

        assert_eq!(
            outcome
                .summary
                .get("finish_reason")
                .and_then(|v| v.as_str()),
            Some("cancelled"),
            "the run ended via the poll-loop cancellation",
        );
        let calls = shell.calls();
        let issued = |prog: &str, arg: &str| {
            calls.iter().any(|c| {
                c.program.ends_with(prog)
                    && c.args
                        .iter()
                        .any(|a| a == arg)
            })
        };
        // The exact High-finding regression: provisioning ran **atomically** —
        // the source loop device was detached (`losetup -d`) before cancel was
        // ever observed, so it can't leak.
        assert!(
            issued("losetup", "-d"),
            "provision completed (source loop device detached) before cancellation",
        );
        // And the normal teardown ran (destroyed the running domain by name).
        assert!(issued("virsh", "destroy"), "teardown destroyed the domain on cancel");
    }

    /// Handle-less orphan cleanup (Phase 4B-2): from a job id alone,
    /// `cleanup_by_job_id` must destroy/undefine the domain, unmount the
    /// results tmpfs AND the source mount, find+detach the dynamically-named
    /// loop device via `losetup -j`, lvremove the chainstate snapshot, prune
    /// the git ref, and remove the job dir — in that order, best-effort.
    #[tokio::test]
    async fn cleanup_by_job_id_reconstructs_full_teardown_from_id() {
        let tmp = TempDir::new().unwrap();
        let cfg = Arc::new(test_config(&tmp));
        let job_id = "orphan-123";

        // A leftover job dir (with the source.raw backing file) + tmpfs dir,
        // as a crashed daemon would leave them.
        let job_dir = cfg
            .paths
            .jobs_dir
            .join(job_id);
        std::fs::create_dir_all(&job_dir).unwrap();
        std::fs::write(job_dir.join("source.raw"), b"raw").unwrap();
        std::fs::create_dir_all(
            cfg.paths
                .results_tmpfs_root
                .join(job_id),
        )
        .unwrap();

        // Canned replies in the exact order cleanup issues them. `losetup -j`
        // reports one association so a `losetup -d` must follow.
        let shell = Arc::new(RecordingShell::new());
        shell
            .expect_ok(1) // virsh destroy
            .expect_ok(1) // virsh undefine
            .expect_ok(1) // umount results tmpfs
            .expect_ok(1) // umount source.mnt
            .reply(PreparedReply::with_stdout(
                "/dev/loop42: [2049]:7 (/var/lib/sbgh/jobs/orphan-123/source.raw)\n",
            )) // losetup -j
            .expect_ok(1) // losetup -d /dev/loop42
            .expect_ok(1) // lvremove
            .expect_ok(1); // git update-ref -d (prune)

        let driver = LibvirtDriver::new(cfg.clone(), shell.clone());
        driver
            .cleanup_by_job_id(job_id)
            .await;

        let calls = shell.calls();
        let prog = |i: usize| {
            std::path::Path::new(&calls[i].program)
                .file_name()
                .unwrap()
                .to_string_lossy()
                .into_owned()
        };
        let programs: Vec<String> = (0..calls.len())
            .map(prog)
            .collect();
        assert_eq!(
            programs,
            [
                "virsh",    // destroy
                "virsh",    // undefine
                "umount",   // results tmpfs
                "umount",   // source.mnt
                "losetup",  // -j (find loop)
                "losetup",  // -d (detach)
                "lvremove", // chainstate snapshot
                "git",      // ref prune
            ],
            "cleanup command order",
        );
        // The detach targeted the exact device `losetup -j` reported.
        assert!(
            calls[5]
                .args
                .contains(&"-d".to_string())
                && calls[5]
                    .args
                    .contains(&"/dev/loop42".to_string()),
            "losetup -d must detach the device losetup -j surfaced",
        );
        // lvremove targets the job-id-named snapshot.
        assert!(
            calls[6]
                .args
                .iter()
                .any(|a| a == "sbgh-vg/sbgh-orphan-123-chainstate"),
            "lvremove must target the per-job snapshot",
        );
        // The job dir (and its source.raw) is gone.
        assert!(!job_dir.exists(), "job dir removed");
    }

    /// `losetup -j` with no association (the source disk was already torn down,
    /// or never provisioned) must NOT issue a `losetup -d`, and cleanup still
    /// completes the rest. Proves the no-leak path is also the no-spurious-op
    /// path.
    #[tokio::test]
    async fn cleanup_by_job_id_skips_loop_detach_when_none_attached() {
        let tmp = TempDir::new().unwrap();
        let cfg = Arc::new(test_config(&tmp));
        let job_id = "orphan-empty";

        let shell = Arc::new(RecordingShell::new());
        shell
            .expect_ok(1) // virsh destroy
            .expect_ok(1) // virsh undefine
            .expect_ok(1) // umount results tmpfs
            .expect_ok(1) // umount source.mnt
            .reply(PreparedReply::with_stdout("")) // losetup -j → nothing attached
            .expect_ok(1) // lvremove
            .expect_ok(1); // git update-ref -d (prune)

        let driver = LibvirtDriver::new(cfg.clone(), shell.clone());
        driver
            .cleanup_by_job_id(job_id)
            .await;

        let calls = shell.calls();
        let detaches = calls
            .iter()
            .filter(|c| {
                c.program.ends_with("losetup")
                    && c.args
                        .contains(&"-d".to_string())
            })
            .count();
        assert_eq!(detaches, 0, "no loop attached → no losetup -d");
        // lvremove + prune still ran after the (empty) loop query.
        assert!(
            calls.iter().any(|c| c
                .program
                .ends_with("lvremove")),
            "cleanup continues past the empty loop query",
        );
    }

    /// Codex 4B-2 Medium: when the source loop device can't be detached,
    /// cleanup must PRESERVE the job dir (so `source.raw` — the only handle to
    /// re-find the loop — survives) and report incomplete (`false`), so the
    /// caller leaves the row `running` for retry instead of failing it and
    /// stranding the leak.
    #[tokio::test]
    async fn cleanup_by_job_id_preserves_backing_file_when_loop_detach_fails() {
        let tmp = TempDir::new().unwrap();
        let cfg = Arc::new(test_config(&tmp));
        let job_id = "orphan-stuck-loop";

        let job_dir = cfg
            .paths
            .jobs_dir
            .join(job_id);
        std::fs::create_dir_all(&job_dir).unwrap();
        std::fs::write(job_dir.join("source.raw"), b"raw").unwrap();

        let shell = Arc::new(RecordingShell::new());
        shell
            .expect_ok(1) // virsh destroy
            .expect_ok(1) // virsh undefine
            .expect_ok(1) // umount tmpfs
            .expect_ok(1) // umount source.mnt
            .reply(PreparedReply::with_stdout(
                "/dev/loop42: [2049]:7 (/var/lib/sbgh/jobs/orphan-stuck-loop/source.raw)\n",
            )) // losetup -j
            .reply(PreparedReply::fail("losetup: cannot detach: device or resource busy")) // -d fails
            .expect_ok(1) // lvremove
            .expect_ok(1); // git prune

        let driver = LibvirtDriver::new(cfg.clone(), shell.clone());
        let clean = driver
            .cleanup_by_job_id(job_id)
            .await;

        assert!(!clean, "a failed loop detach must report incomplete cleanup");
        assert!(
            job_dir
                .join("source.raw")
                .exists(),
            "source.raw must survive so the next recovery can re-find the loop",
        );
        assert!(job_dir.exists(), "job dir preserved on incomplete cleanup");
    }

    /// Codex 4B-2 re-review Medium: a **non-zero** `losetup -j` (a genuine
    /// query failure, not a missing-file no-op) must NOT be read as "all
    /// clear" just because stdout is empty — we can't enumerate, so we
    /// can't safely delete `source.raw`. Cleanup must preserve the backing
    /// file, issue no blind `losetup -d`, and report incomplete.
    #[tokio::test]
    async fn cleanup_by_job_id_preserves_backing_file_when_losetup_query_fails() {
        let tmp = TempDir::new().unwrap();
        let cfg = Arc::new(test_config(&tmp));
        let job_id = "orphan-query-fail";

        let job_dir = cfg
            .paths
            .jobs_dir
            .join(job_id);
        std::fs::create_dir_all(&job_dir).unwrap();
        std::fs::write(job_dir.join("source.raw"), b"raw").unwrap();

        let shell = Arc::new(RecordingShell::new());
        shell
            .expect_ok(1) // virsh destroy
            .expect_ok(1) // virsh undefine
            .expect_ok(1) // umount tmpfs
            .expect_ok(1) // umount source.mnt
            .reply(PreparedReply::fail("losetup: cannot read /dev: permission denied")) // -j non-zero, empty stdout
            .expect_ok(1) // lvremove
            .expect_ok(1); // git prune

        let driver = LibvirtDriver::new(cfg.clone(), shell.clone());
        let clean = driver
            .cleanup_by_job_id(job_id)
            .await;

        assert!(!clean, "a non-zero losetup -j must report incomplete cleanup");
        let calls = shell.calls();
        assert!(
            !calls.iter().any(|c| {
                c.program.ends_with("losetup")
                    && c.args
                        .contains(&"-d".to_string())
            }),
            "must not blindly detach when the query itself failed",
        );
        assert!(
            job_dir
                .join("source.raw")
                .exists(),
            "source.raw preserved when the loop query fails",
        );
    }
}
