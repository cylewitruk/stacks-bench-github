use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex as StdMutex;

use async_trait::async_trait;
use sbgh_core::config::{
    ApiConfig, BaselineReport, DaemonServerConfig, GitHubConfig, LvmConfig, PathsConfig, PrReport,
    ReportingConfig, RunnerConfig, StacksBenchConfig, VmConfig,
};
use sbgh_core::models::{BuildTarget, GitRefKind, ResolvedCommit, TaskKind};
use sbgh_github::test_support::FakeGitHub;
use tempfile::TempDir;
use uuid::Uuid;

use super::*;
use crate::job_source::ProgressTarget;
use crate::report::ReportSurface;
use crate::reporter::CHECK_NAME;
use sbgh_driver::TaskContext;
use sbgh_driver::{
    Driver, DriverOutcome, DriverStatus, EventSink, Placement, TaskSpec, Terminal, WorkerEvent,
};
use sbgh_libvirt::shell_test_support::{PreparedReply, RecordingShell};
use sbgh_libvirt::{
    LibvirtDriver, LvmConfig as LibvirtLvmConfig, PathsConfig as LibvirtPathsConfig,
    VmConfig as LibvirtVmConfig,
};

/// A [`RunnableJobStore`] fake that hands out a single pre-staged job
/// then goes empty, and records which lifecycle methods the runner
/// called. Backend-neutral — used to prove the runner drives the
/// abstraction (not the legacy store) to a terminal call.
struct FakeSource {
    job: StdMutex<Option<RunnableJob>>,
    calls: StdMutex<Vec<&'static str>>,
    /// The commit `start_running` was called with (for asserting
    /// claim-time tag resolution).
    started_commit: StdMutex<Option<ResolvedCommit>>,
    /// Force `set_check_run` / `set_comment_id` to error — for testing that
    /// a persistence failure is non-fatal (the job still terminalizes).
    fail_persist: std::sync::atomic::AtomicBool,
    /// Job ids `running_job_ids` reports as orphans (Phase 4B-2 recovery).
    orphans: StdMutex<Vec<Uuid>>,
    /// Force `running_job_ids` to error — the startup-critical path.
    fail_list: std::sync::atomic::AtomicBool,
    /// The read-only view `load_runnable` returns (4C-2 orphan-check path).
    orphan_runnable: StdMutex<Option<RunnableJob>>,
    /// The queued views `list_queued` returns (Phase 5 position path).
    queued: StdMutex<Vec<RunnableJob>>,
}

impl FakeSource {
    fn new(job: RunnableJob) -> Self {
        Self {
            job: StdMutex::new(Some(job)),
            calls: StdMutex::new(Vec::new()),
            started_commit: StdMutex::new(None),
            fail_persist: std::sync::atomic::AtomicBool::new(false),
            orphans: StdMutex::new(Vec::new()),
            fail_list: std::sync::atomic::AtomicBool::new(false),
            orphan_runnable: StdMutex::new(None),
            queued: StdMutex::new(Vec::new()),
        }
    }
    /// Seed the queued views `list_queued` returns (Phase 5 position path).
    fn set_queued(&self, jobs: Vec<RunnableJob>) {
        *self.queued.lock().unwrap() = jobs;
    }
    /// Seed an orphaned `running` job id for the startup-recovery path.
    fn add_orphan(&self, id: Uuid) {
        self.orphans
            .lock()
            .unwrap()
            .push(id);
    }
    /// Seed the [`RunnableJob`] view `load_runnable` returns (the orphan's
    /// reconstructed reporting context, for the 4C-2 check-conclusion
    /// path).
    fn set_orphan_runnable(&self, job: RunnableJob) {
        *self
            .orphan_runnable
            .lock()
            .unwrap() = Some(job);
    }
    /// Make `running_job_ids` error, exercising the startup-critical path.
    fn fail_list(&self) {
        self.fail_list
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
    fn fail_persist(&self) {
        self.fail_persist
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
    fn record(&self, m: &'static str) {
        self.calls
            .lock()
            .unwrap()
            .push(m);
    }
    fn calls(&self) -> Vec<&'static str> {
        self.calls
            .lock()
            .unwrap()
            .clone()
    }
    fn started_commit(&self) -> Option<ResolvedCommit> {
        self.started_commit
            .lock()
            .unwrap()
            .clone()
    }
}

#[derive(Default)]
struct FakeRepeatPlanner {
    appended: StdMutex<Vec<Uuid>>,
    pending: StdMutex<Vec<sbgh_core::db::jobs::PendingBenchmarkRun>>,
    details: StdMutex<HashMap<Uuid, serde_json::Value>>,
    detail_calls: StdMutex<Vec<Uuid>>,
    resume_calls: std::sync::atomic::AtomicUsize,
    fail_append: std::sync::atomic::AtomicBool,
}

impl FakeRepeatPlanner {
    fn appended(&self) -> Vec<Uuid> {
        self.appended
            .lock()
            .unwrap()
            .clone()
    }

    fn resume_calls(&self) -> usize {
        self.resume_calls
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    fn fail_append(&self) {
        self.fail_append
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    fn set_pending(&self, pending: Vec<sbgh_core::db::jobs::PendingBenchmarkRun>) {
        *self.pending.lock().unwrap() = pending;
    }

    fn set_completed_detail(&self, job_id: Uuid, detail: serde_json::Value) {
        self.details
            .lock()
            .unwrap()
            .insert(job_id, detail);
    }

    fn detail_calls(&self) -> Vec<Uuid> {
        self.detail_calls
            .lock()
            .unwrap()
            .clone()
    }
}

#[async_trait]
impl RepeatRunPlanner for FakeRepeatPlanner {
    async fn append_next_benchmark_run(
        &self,
        completed_job_id: Uuid,
    ) -> anyhow::Result<Option<sbgh_core::models::Job>> {
        self.appended
            .lock()
            .unwrap()
            .push(completed_job_id);
        if self
            .fail_append
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            anyhow::bail!("forced append failure");
        }
        Ok(None)
    }

    async fn pending_completed_benchmark_runs(
        &self,
    ) -> anyhow::Result<Vec<sbgh_core::db::jobs::PendingBenchmarkRun>> {
        self.resume_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(self
            .pending
            .lock()
            .unwrap()
            .clone())
    }

    async fn completed_event_detail(
        &self,
        job_id: Uuid,
    ) -> anyhow::Result<Option<serde_json::Value>> {
        self.detail_calls
            .lock()
            .unwrap()
            .push(job_id);
        Ok(self
            .details
            .lock()
            .unwrap()
            .get(&job_id)
            .cloned())
    }
}

struct CompletedDriver;

#[async_trait]
impl Driver for CompletedDriver {
    async fn run_task(
        &self,
        _ctx: &TaskContext<'_>,
        _spec: &TaskSpec,
        _sink: &dyn EventSink,
        _cancel: &CancellationToken,
        _placement: &Placement,
    ) -> anyhow::Result<DriverOutcome> {
        Ok(DriverOutcome {
            status: DriverStatus::Completed,
            summary: serde_json::json!({ "finish_reason": "test" }),
            output: sbgh_driver::DriverTaskOutput::None,
        })
    }

    async fn cleanup_by_job_id(&self, _job_id: &str) -> bool {
        true
    }
}

struct FailedDriver;

#[async_trait]
impl Driver for FailedDriver {
    async fn run_task(
        &self,
        _ctx: &TaskContext<'_>,
        _spec: &TaskSpec,
        _sink: &dyn EventSink,
        _cancel: &CancellationToken,
        _placement: &Placement,
    ) -> anyhow::Result<DriverOutcome> {
        Ok(DriverOutcome {
            status: DriverStatus::Failed("bench failed".into()),
            summary: serde_json::json!({ "finish_reason": "bench_failed" }),
            output: sbgh_driver::DriverTaskOutput::None,
        })
    }

    async fn cleanup_by_job_id(&self, _job_id: &str) -> bool {
        true
    }
}

#[async_trait]
impl RunnableJobStore for FakeSource {
    async fn claim_next(&self) -> anyhow::Result<Option<RunnableJob>> {
        Ok(self
            .job
            .lock()
            .unwrap()
            .take())
    }
    async fn load_runnable(&self, _job_id: Uuid) -> anyhow::Result<Option<RunnableJob>> {
        Ok(self
            .orphan_runnable
            .lock()
            .unwrap()
            .clone())
    }
    async fn list_queued(&self) -> anyhow::Result<Vec<RunnableJob>> {
        Ok(self
            .queued
            .lock()
            .unwrap()
            .clone())
    }
    async fn start_running(
        &self,
        _job: &RunnableJob,
        resolved_commit: Option<ResolvedCommit>,
    ) -> anyhow::Result<()> {
        *self
            .started_commit
            .lock()
            .unwrap() = resolved_commit;
        self.record("start_running");
        Ok(())
    }
    async fn complete(
        &self,
        _job: &RunnableJob,
        _summary: &serde_json::Value,
    ) -> anyhow::Result<()> {
        self.record("complete");
        Ok(())
    }
    async fn find_baseline(
        &self,
        _subject_job_id: Uuid,
        _merge_base_sha: &str,
        _base_ref: &str,
        _at: Option<chrono::DateTime<chrono::Utc>>,
        _workload_key: &str,
    ) -> anyhow::Result<Option<crate::job_source::BaselineRef>> {
        Ok(None)
    }
    async fn fail(
        &self,
        _job: &RunnableJob,
        _error: &str,
        _summary: Option<&serde_json::Value>,
    ) -> anyhow::Result<()> {
        self.record("fail");
        Ok(())
    }
    async fn cancel(&self, _job: &RunnableJob, _remark: &str) -> anyhow::Result<()> {
        self.record("cancel");
        Ok(())
    }
    async fn set_comment_id(&self, _job: &RunnableJob, _comment_id: i64) -> anyhow::Result<()> {
        self.record("set_comment_id");
        if self
            .fail_persist
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            anyhow::bail!("forced set_comment_id failure");
        }
        Ok(())
    }
    async fn set_check_run(
        &self,
        _job: &RunnableJob,
        _check_run_id: i64,
        _html_url: Option<&str>,
    ) -> anyhow::Result<()> {
        self.record("set_check_run");
        if self
            .fail_persist
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            anyhow::bail!("forced set_check_run failure");
        }
        Ok(())
    }
    async fn set_plan_message_ts(
        &self,
        _job: &RunnableJob,
        _message_ts: &str,
    ) -> anyhow::Result<()> {
        self.record("set_plan_message_ts");
        Ok(())
    }
    async fn sweep_stuck_claims(&self, _lease: chrono::Duration) -> anyhow::Result<u64> {
        Ok(0)
    }
    async fn running_job_ids(&self) -> anyhow::Result<Vec<Uuid>> {
        if self
            .fail_list
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            anyhow::bail!("forced running_job_ids failure");
        }
        Ok(self
            .orphans
            .lock()
            .unwrap()
            .clone())
    }
    async fn cancel_orphan(&self, _job_id: Uuid, _remark: &str) -> anyhow::Result<bool> {
        self.record("cancel_orphan");
        Ok(true)
    }
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
            job_timeout_secs: 30,
            network: "default".into(),
            poll_interval_secs: 1,
            heartbeat_interval_secs: 60,
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
            chainstate_snapshot_size_gib: None,
            min_data_free_percent: 5.0,
            min_metadata_free_percent: 5.0,
        },
        stacks_bench: StacksBenchConfig { default_args: String::new() },
        api: ApiConfig {
            listen: vec!["127.0.0.1:8787".into()],
            cookie_path: "/tmp/sbgh-test.cookie".into(),
            ingest_token: None,
        },
        // Existing runner tests assert comment-only behaviour; check-mode
        // is exercised by dedicated tests that override this.
        reporting: ReportingConfig {
            pr_report: PrReport::Comment,
            baseline_report: BaselineReport::None,
            noise_cv_pct: None,
        },
        runner: RunnerConfig {
            max_concurrent_jobs: 1,
            max_clean_repetitions: 5,
            max_variants: 2,
            max_comparison_lifecycles: 10,
            cpu_sets: vec![],
            host_cpus: None,
        },
        artifacts: Default::default(),
        slack: Default::default(),
        llm: Default::default(),
    }
}

fn test_libvirt_driver(config: Arc<DaemonConfig>, shell: Arc<dyn Shell>) -> Arc<dyn Driver> {
    let artifact_store = build_test_artifact_store(&config);
    let binary_cache = build_binary_cache(&BinaryCacheConfig {
        enabled: config
            .artifacts
            .binary_cache
            .enabled,
        max_bytes: config
            .artifacts
            .binary_cache
            .max_size
            .as_bytes(),
        dir: config
            .artifacts
            .binary_cache
            .dir
            .clone(),
    })
    .map(|cache| cache as Arc<dyn sbgh_driver::BinaryCacheStore>);
    Arc::new(LibvirtDriver::new(
        LibvirtConfig {
            vm: LibvirtVmConfig {
                golden_image: config.vm.golden_image.clone(),
                build_vcpus: config.vm.build_vcpus,
                bench_vcpus: config.vm.bench_vcpus,
                build_memory_bytes: config
                    .vm
                    .build_memory
                    .as_bytes(),
                bench_memory_bytes: config
                    .vm
                    .bench_memory
                    .as_bytes(),
                boot_disk_gib: config.vm.boot_disk_gib,
                job_timeout_secs: config.vm.job_timeout_secs,
                network: config.vm.network.clone(),
                poll_interval_secs: config.vm.poll_interval_secs,
                heartbeat_interval_secs: config
                    .vm
                    .heartbeat_interval_secs,
            },
            paths: LibvirtPathsConfig {
                jobs_dir: config.paths.jobs_dir.clone(),
                git_mirror: config
                    .paths
                    .git_mirror
                    .clone(),
                results_tmpfs_root: config
                    .paths
                    .results_tmpfs_root
                    .clone(),
                results_archive_dir: config
                    .paths
                    .results_archive_dir
                    .clone(),
                virsh_binary: config
                    .paths
                    .virsh_binary
                    .clone(),
                sudo_binary: config
                    .paths
                    .sudo_binary
                    .clone(),
                qemu_img_binary: config
                    .paths
                    .qemu_img_binary
                    .clone(),
                cloud_localds_binary: config
                    .paths
                    .cloud_localds_binary
                    .clone(),
                git_binary: config
                    .paths
                    .git_binary
                    .clone(),
            },
            lvm: LibvirtLvmConfig {
                vg_name: config.lvm.vg_name.clone(),
                thinpool: config.lvm.thinpool.clone(),
                chainstate_base_prefix: config
                    .lvm
                    .chainstate_base_prefix
                    .clone(),
                chainstate_snapshot_size_gib: config
                    .lvm
                    .chainstate_snapshot_size_gib,
                min_data_free_percent: config
                    .lvm
                    .min_data_free_percent,
                min_metadata_free_percent: config
                    .lvm
                    .min_metadata_free_percent,
            },
            service_user: config
                .server
                .service_user
                .clone(),
            host_cpus: config
                .runner
                .host_cpus
                .clone(),
            block_validation: None,
        },
        shell,
        execution_sink(artifact_store),
        binary_cache,
    ))
}

fn benchmark_job(
    benchmark_run_index: i32,
    requested_run_count: i32,
    group_run_index: i32,
    group_requested_run_count: i32,
) -> RunnableJob {
    RunnableJob {
        id: Uuid::new_v4(),
        benchmark_group_id: Uuid::new_v4(),
        benchmark_spec_id: Uuid::new_v4(),
        benchmark_run_index,
        requested_run_count,
        group_requested_run_count,
        group_run_index,
        baseline_calibration_id: None,
        group_artifact_prefix: "group-a".into(),
        repository: "acme/widgets".into(),
        commit: "abc123".into(),
        git_ref_display: "develop".into(),
        git_ref_kind: GitRefKind::Branch,
        installation_id: 7,
        task_kind: TaskKind::Benchmark,
        build_target: BuildTarget::StacksBench,
        workload_key: None,
        bench_args: vec![],
        progress: ProgressTarget::CommitCheck { check_run_id: None },
        claim_token: Some(Uuid::new_v4()),
    }
}

#[test]
fn execution_request_is_owned_discriminated_and_fails_closed() {
    let mut job = benchmark_job(2, 4, 2, 4);
    job.repository = "octo/core".into();
    job.bench_args = vec!["--count=10".into()];
    let request =
        execution_request_for(&job, "resolved-sha".into(), Some("2-3".into()), "--default");
    drop(job);

    assert_eq!(request.context.repository, "octo/core");
    assert_eq!(request.context.commit, "resolved-sha");
    assert_eq!(
        request
            .placement
            .vcpu_cpuset
            .as_deref(),
        Some("2-3")
    );
    match request.task {
        ExecutionTask::Benchmark(task) => {
            assert_eq!(task.args, ["--count=10"]);
            assert_eq!(task.run.run_index, 2);
            assert_eq!(task.run.requested_run_count, 4);
        }
        other => panic!("expected benchmark task, got {other:?}"),
    }

    let mut unsupported = benchmark_job(0, 1, 0, 1);
    unsupported.build_target = BuildTarget::StacksInspect;
    assert!(matches!(
        execution_request_for(&unsupported, "sha".into(), None, "--default").task,
        ExecutionTask::Unsupported { .. }
    ));
}

#[test]
fn execution_request_uses_the_same_resolved_tokens_as_its_workload_key() {
    let mut job = benchmark_job(0, 1, 0, 1);
    let resolved = sbgh_core::bench_args::resolve_bench_args(&[], "--count 10");
    job.workload_key = Some(resolved.workload_key.clone());

    let request = execution_request_for(&job, "sha".into(), None, "--count 10");
    match request.task {
        ExecutionTask::Benchmark(task) => {
            assert_eq!(task.args, resolved.effective_args);
            assert_eq!(sbgh_core::bench_args::workload_key(&task.args), job.workload_key.unwrap(),);
        }
        other => panic!("expected benchmark task, got {other:?}"),
    }
}

#[test]
fn stale_workload_key_does_not_add_a_runtime_failure_path() {
    let mut job = benchmark_job(0, 1, 0, 1);
    job.workload_key = Some("stale-key".into());
    let request = execution_request_for(&job, "sha".into(), None, "--count 10");
    match request.task {
        ExecutionTask::Benchmark(task) => assert_eq!(task.args, ["--count", "10"]),
        other => panic!("expected benchmark task, got {other:?}"),
    }
}

#[test]
fn sqlite_carry_uses_group_sequence_not_spec_run_index() {
    let first = benchmark_job(0, 1, 0, 2);
    assert!(job_should_carry_sqlite(&first));
    assert_eq!(sqlite_seed_key_for(&first), None);

    let second_spec_first_run = benchmark_job(0, 1, 1, 2);
    assert!(job_should_carry_sqlite(&second_spec_first_run));
    assert_eq!(
        sqlite_seed_key_for(&second_spec_first_run),
        Some(group_sqlite_key(&second_spec_first_run.group_artifact_prefix)),
    );
    assert!(
        job_is_final_group_run(&second_spec_first_run),
        "spec 1 run 0 is the final group run in a two-variant comparison"
    );
}

/// The runner drives ANY `RunnableJobStore` (here a baseline job with
/// reporting disabled — `baseline_report = none`, the default
/// `test_config`) through `start_running` and, when the driver reports
/// failure, `fail` — proving the runner is backend-agnostic and a
/// no-surface job never touches GitHub.
#[tokio::test]
async fn run_once_drives_unreported_baseline_job_to_terminal_fail() {
    let tmp = TempDir::new().unwrap();
    let config = test_config(&tmp); // baseline_report = none

    let job = RunnableJob {
        id: Uuid::new_v4(),
        benchmark_group_id: Uuid::new_v4(),
        benchmark_spec_id: Uuid::new_v4(),
        benchmark_run_index: 0,
        requested_run_count: 2,
        group_requested_run_count: 2,
        group_run_index: 0,
        baseline_calibration_id: None,
        group_artifact_prefix: "group-a".into(),
        repository: "acme/widgets".into(),
        commit: "abc123".into(), // pre-resolved → preflight is a no-op
        git_ref_display: "develop".into(),
        git_ref_kind: GitRefKind::Branch,
        installation_id: 7,
        task_kind: TaskKind::Benchmark,
        build_target: BuildTarget::StacksBench,
        workload_key: None,
        bench_args: vec![],
        progress: ProgressTarget::CommitCheck { check_run_id: None },
        claim_token: Some(Uuid::new_v4()),
    };
    let source = Arc::new(FakeSource::new(job));

    // With reporting off, GitHub must never be touched; a recording shell
    // that fails the first provisioning command drives the driver to a
    // Failed outcome.
    let gh = Arc::new(FakeGitHub::new());
    let shell = Arc::new(RecordingShell::new());
    shell.reply(PreparedReply::fail(b"boom: git fetch failed"));

    let runner = Runner::new(config, source.clone(), gh.clone(), shell);
    let processed = runner
        .run_once()
        .await
        .unwrap();
    assert!(processed, "claimed + executed one job");

    // Lifecycle: transitioned to running, then failed (driver setup
    // error → Failed outcome). No reporting work for a no-surface job.
    assert_eq!(source.calls(), vec!["start_running", "fail"]);
    assert!(
        gh.calls().is_empty(),
        "a job with reporting disabled must make no GitHub calls (no comment or check)"
    );

    // Queue now empty.
    assert!(
        !runner
            .run_once()
            .await
            .unwrap()
    );
}

/// v10 (0005): a build-only job (`task_kind = build_only` → a `Silent`
/// report target) dispatches the build-only recipe and reports nothing —
/// GitHub is never touched, even on a build failure. The build itself
/// (provision → build → publish → stop, no bench) is driver-tested in
/// `build_only_skips_bench_phase`.
#[tokio::test]
async fn run_once_build_only_job_is_silent() {
    let tmp = TempDir::new().unwrap();
    let config = test_config(&tmp);

    let job = RunnableJob {
        id: Uuid::new_v4(),
        benchmark_group_id: Uuid::new_v4(),
        benchmark_spec_id: Uuid::new_v4(),
        benchmark_run_index: 0,
        requested_run_count: 2,
        group_requested_run_count: 2,
        group_run_index: 0,
        baseline_calibration_id: None,
        group_artifact_prefix: "group-a".into(),
        repository: "acme/widgets".into(),
        commit: "abc123".into(), // pre-resolved → preflight is a no-op
        git_ref_display: "develop".into(),
        git_ref_kind: GitRefKind::Branch,
        installation_id: 7,
        task_kind: TaskKind::BuildOnly,
        build_target: BuildTarget::StacksBench,
        workload_key: None,
        bench_args: vec![],
        progress: ProgressTarget::Silent,
        claim_token: Some(Uuid::new_v4()),
    };
    let source = Arc::new(FakeSource::new(job));

    // The first provisioning command fails → the build-only run terminalizes
    // as Failed; a silent job must still make zero GitHub calls.
    let gh = Arc::new(FakeGitHub::new());
    let shell = Arc::new(RecordingShell::new());
    shell.reply(PreparedReply::fail(b"boom: git fetch failed"));

    let runner = Runner::new(config, source.clone(), gh.clone(), shell);
    assert!(
        runner
            .run_once()
            .await
            .unwrap(),
        "claimed + executed one job",
    );

    // Lifecycle ran (start_running → fail), but the silent report surface
    // posted nothing.
    assert_eq!(source.calls(), vec!["start_running", "fail"]);
    assert!(gh.calls().is_empty(), "a build-only (silent) job must make no GitHub calls",);
}

#[tokio::test]
async fn completed_run_appends_next_repeat_after_terminal_persist() {
    let tmp = TempDir::new().unwrap();
    let config = Arc::new(test_config(&tmp));
    let job = RunnableJob {
        id: Uuid::new_v4(),
        benchmark_group_id: Uuid::new_v4(),
        benchmark_spec_id: Uuid::new_v4(),
        benchmark_run_index: 0,
        requested_run_count: 2,
        group_requested_run_count: 2,
        group_run_index: 0,
        baseline_calibration_id: None,
        group_artifact_prefix: "group-a".into(),
        repository: "acme/widgets".into(),
        commit: "abc123".into(),
        git_ref_display: "develop".into(),
        git_ref_kind: GitRefKind::Branch,
        installation_id: 7,
        task_kind: TaskKind::Benchmark,
        build_target: BuildTarget::StacksBench,
        workload_key: None,
        bench_args: vec![],
        progress: ProgressTarget::CommitCheck { check_run_id: None },
        claim_token: Some(Uuid::new_v4()),
    };
    let source = Arc::new(FakeSource::new(job.clone()));
    let planner = Arc::new(FakeRepeatPlanner::default());
    let sqlite_key =
        crate::artifact_store::artifact_key(&job.id.to_string(), sbgh_libvirt::SQLITE_RELATIVE);
    let sqlite_path = config
        .paths
        .results_archive_dir
        .join(&sqlite_key);
    std::fs::create_dir_all(sqlite_path.parent().unwrap()).unwrap();
    std::fs::write(&sqlite_path, b"sqlite bytes").unwrap();
    planner.set_completed_detail(job.id, serde_json::json!({ "sqlite_archived_path": sqlite_key }));
    let deps = JobDeps {
        artifact_store: build_test_artifact_store(config.as_ref()),
        config,
        jobs: source.clone(),
        gh: Arc::new(FakeGitHub::new()),
        worker: WorkerRuntime::with_driver(Arc::new(CompletedDriver)),
        cache_control: None,
        app_id: Arc::new(OnceCell::new()),
        slack: None,
        pin_manager: None,
        repeat_planner: Some(planner.clone()),
        slack_sessions: Default::default(),
    };

    deps.run(job.clone(), None, CancellationToken::new())
        .await
        .expect("completed run should not fail");

    assert_eq!(source.calls(), vec!["start_running", "complete"]);
    assert_eq!(planner.appended(), vec![job.id]);
    let group_sqlite = tmp
        .path()
        .join("archive")
        .join(group_sqlite_key(&job.group_artifact_prefix));
    assert_eq!(std::fs::read(group_sqlite).unwrap(), b"sqlite bytes");
}

#[tokio::test]
async fn repeat_append_failure_is_nonfatal_to_completed_run() {
    let tmp = TempDir::new().unwrap();
    let config = Arc::new(test_config(&tmp));
    let job = RunnableJob {
        id: Uuid::new_v4(),
        benchmark_group_id: Uuid::new_v4(),
        benchmark_spec_id: Uuid::new_v4(),
        benchmark_run_index: 0,
        requested_run_count: 2,
        group_requested_run_count: 2,
        group_run_index: 0,
        baseline_calibration_id: None,
        group_artifact_prefix: "group-b".into(),
        repository: "acme/widgets".into(),
        commit: "abc123".into(),
        git_ref_display: "develop".into(),
        git_ref_kind: GitRefKind::Branch,
        installation_id: 7,
        task_kind: TaskKind::Benchmark,
        build_target: BuildTarget::StacksBench,
        workload_key: None,
        bench_args: vec![],
        progress: ProgressTarget::CommitCheck { check_run_id: None },
        claim_token: Some(Uuid::new_v4()),
    };
    let source = Arc::new(FakeSource::new(job.clone()));
    let planner = Arc::new(FakeRepeatPlanner::default());
    let sqlite_key =
        crate::artifact_store::artifact_key(&job.id.to_string(), sbgh_libvirt::SQLITE_RELATIVE);
    let sqlite_path = config
        .paths
        .results_archive_dir
        .join(&sqlite_key);
    std::fs::create_dir_all(sqlite_path.parent().unwrap()).unwrap();
    std::fs::write(&sqlite_path, b"sqlite bytes").unwrap();
    planner.set_completed_detail(job.id, serde_json::json!({ "sqlite_archived_path": sqlite_key }));
    planner.fail_append();
    let deps = JobDeps {
        artifact_store: build_test_artifact_store(config.as_ref()),
        config,
        jobs: source.clone(),
        gh: Arc::new(FakeGitHub::new()),
        worker: WorkerRuntime::with_driver(Arc::new(CompletedDriver)),
        cache_control: None,
        app_id: Arc::new(OnceCell::new()),
        slack: None,
        pin_manager: None,
        repeat_planner: Some(planner.clone()),
        slack_sessions: Default::default(),
    };

    deps.run(job.clone(), None, CancellationToken::new())
        .await
        .expect("append failure is retryable and must not fail the completed run");

    assert_eq!(source.calls(), vec!["start_running", "complete"]);
    assert_eq!(planner.appended(), vec![job.id]);
}

#[tokio::test]
async fn failed_repeat_does_not_try_to_carry_or_append_next_run() {
    let tmp = TempDir::new().unwrap();
    let config = Arc::new(test_config(&tmp));
    let job = RunnableJob {
        id: Uuid::new_v4(),
        benchmark_group_id: Uuid::new_v4(),
        benchmark_spec_id: Uuid::new_v4(),
        benchmark_run_index: 0,
        requested_run_count: 2,
        group_requested_run_count: 2,
        group_run_index: 0,
        baseline_calibration_id: None,
        group_artifact_prefix: "group-failed".into(),
        repository: "acme/widgets".into(),
        commit: "abc123".into(),
        git_ref_display: "develop".into(),
        git_ref_kind: GitRefKind::Branch,
        installation_id: 7,
        task_kind: TaskKind::Benchmark,
        build_target: BuildTarget::StacksBench,
        workload_key: None,
        bench_args: vec![],
        progress: ProgressTarget::CommitCheck { check_run_id: None },
        claim_token: Some(Uuid::new_v4()),
    };
    let source = Arc::new(FakeSource::new(job.clone()));
    let planner = Arc::new(FakeRepeatPlanner::default());
    let deps = JobDeps {
        artifact_store: build_test_artifact_store(config.as_ref()),
        config,
        jobs: source.clone(),
        gh: Arc::new(FakeGitHub::new()),
        worker: WorkerRuntime::with_driver(Arc::new(FailedDriver)),
        cache_control: None,
        app_id: Arc::new(OnceCell::new()),
        slack: None,
        pin_manager: None,
        repeat_planner: Some(planner.clone()),
        slack_sessions: Default::default(),
    };

    deps.run(job, None, CancellationToken::new())
        .await
        .expect("a failed run is terminalized by the reporter");

    assert_eq!(source.calls(), vec!["start_running", "fail"]);
    assert!(
        planner
            .detail_calls()
            .is_empty(),
        "failed repeats must not enter carry-forward",
    );
    assert!(planner.appended().is_empty(), "failed repeats must not append the next run",);
}

#[tokio::test]
async fn missing_carried_sqlite_blocks_next_repeat_append() {
    let tmp = TempDir::new().unwrap();
    let config = Arc::new(test_config(&tmp));
    let job = RunnableJob {
        id: Uuid::new_v4(),
        benchmark_group_id: Uuid::new_v4(),
        benchmark_spec_id: Uuid::new_v4(),
        benchmark_run_index: 0,
        requested_run_count: 2,
        group_requested_run_count: 2,
        group_run_index: 0,
        baseline_calibration_id: None,
        group_artifact_prefix: "group-missing".into(),
        repository: "acme/widgets".into(),
        commit: "abc123".into(),
        git_ref_display: "develop".into(),
        git_ref_kind: GitRefKind::Branch,
        installation_id: 7,
        task_kind: TaskKind::Benchmark,
        build_target: BuildTarget::StacksBench,
        workload_key: None,
        bench_args: vec![],
        progress: ProgressTarget::Slack {
            channel: "C1".into(),
            message_ts: "REQ".into(),
            reporting_identity: "0".repeat(64),
            plan_message_ts: Some("PLAN_TS".into()),
        },
        claim_token: Some(Uuid::new_v4()),
    };
    let source = Arc::new(FakeSource::new(job.clone()));
    assert!(!job_is_final_group_run(&job), "fixture must be an intermediate group run");
    let planner = Arc::new(FakeRepeatPlanner::default());
    planner.set_completed_detail(
        job.id,
        serde_json::json!({ "sqlite_archived_path": "missing/appdata/stacks-bench.db" }),
    );
    let slack = Arc::new(FakePositionSlack::default());
    let deps = JobDeps {
        artifact_store: build_test_artifact_store(config.as_ref()),
        config,
        jobs: source.clone(),
        gh: Arc::new(FakeGitHub::new()),
        worker: WorkerRuntime::with_driver(Arc::new(CompletedDriver)),
        cache_control: None,
        app_id: Arc::new(OnceCell::new()),
        slack: Some(slack.clone()),
        pin_manager: None,
        repeat_planner: Some(planner.clone()),
        slack_sessions: Default::default(),
    };

    deps.run(job.clone(), None, CancellationToken::new())
        .await
        .expect("missing carried DB is retryable and must not fail the completed run");

    assert_eq!(source.calls(), vec!["start_running", "complete"]);
    assert_eq!(planner.detail_calls(), vec![job.id]);
    assert!(planner.appended().is_empty(), "next repeat waits for carried DB");
    let updates = slack
        .updates
        .lock()
        .unwrap()
        .clone();
    let rendered = updates
        .iter()
        .map(|(_, _, body, _)| body.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        rendered.contains("repeat group stalled"),
        "carry-forward stall must update the shared Slack surface; updates={updates:?}",
    );
    assert!(rendered.contains("No promoted repeat metrics available"), "{rendered}");
}

#[tokio::test]
async fn coordinator_resumes_pending_repeats_on_startup() {
    let tmp = TempDir::new().unwrap();
    let config = Arc::new(test_config(&tmp));
    let job = pr_job("abc123", None);
    let source = Arc::new(FakeSource::new(job));
    let planner = Arc::new(FakeRepeatPlanner::default());
    let completed_job_id = Uuid::new_v4();
    let sqlite_key = crate::artifact_store::artifact_key(
        &completed_job_id.to_string(),
        sbgh_libvirt::SQLITE_RELATIVE,
    );
    let sqlite_path = config
        .paths
        .results_archive_dir
        .join(&sqlite_key);
    std::fs::create_dir_all(sqlite_path.parent().unwrap()).unwrap();
    std::fs::write(&sqlite_path, b"startup sqlite").unwrap();
    planner.set_completed_detail(
        completed_job_id,
        serde_json::json!({ "sqlite_archived_path": sqlite_key }),
    );
    planner.set_pending(vec![sbgh_core::db::jobs::PendingBenchmarkRun {
        completed_job_id,
        benchmark_group_id: Uuid::new_v4(),
        benchmark_spec_id: Uuid::new_v4(),
        benchmark_run_index: 0,
        requested_run_count: 2,
        artifact_prefix: "startup-group".into(),
    }]);
    let deps = JobDeps {
        artifact_store: build_test_artifact_store(config.as_ref()),
        config,
        jobs: source,
        gh: Arc::new(FakeGitHub::new()),
        worker: WorkerRuntime::with_driver(Arc::new(CompletedDriver)),
        cache_control: None,
        app_id: Arc::new(OnceCell::new()),
        slack: None,
        pin_manager: None,
        repeat_planner: Some(planner.clone()),
        slack_sessions: Default::default(),
    };
    let coord = Coordinator::new(deps, 1, CancellationToken::new());

    coord
        .resume_pending_repeats()
        .await;

    assert_eq!(planner.resume_calls(), 1);
    assert_eq!(planner.appended(), vec![completed_job_id]);
    let group_sqlite = tmp
        .path()
        .join("archive")
        .join(group_sqlite_key("startup-group"));
    assert_eq!(std::fs::read(group_sqlite).unwrap(), b"startup sqlite");
}

/// A `tag_created` job is enqueued with no commit; the runner
/// resolves the tag → commit at claim time (via `resolve_commit`) and
/// hands the resolved commit (with its authored date) to
/// `start_running`. Proves the claim-time tag-resolution path.
#[tokio::test]
async fn run_once_resolves_tag_commit_in_preflight() {
    let tmp = TempDir::new().unwrap();
    let config = test_config(&tmp);

    let job = RunnableJob {
        id: Uuid::new_v4(),
        benchmark_group_id: Uuid::new_v4(),
        benchmark_spec_id: Uuid::new_v4(),
        benchmark_run_index: 0,
        requested_run_count: 1,
        group_requested_run_count: 1,
        group_run_index: 0,
        baseline_calibration_id: None,
        group_artifact_prefix: Uuid::new_v4().to_string(),
        repository: "octo/core".into(),
        commit: String::new(), // unresolved — a tag job
        git_ref_display: "release/1.2".into(),
        git_ref_kind: GitRefKind::Tag,
        installation_id: 7,
        task_kind: TaskKind::Benchmark,
        build_target: BuildTarget::StacksBench,
        workload_key: None,
        bench_args: vec![],
        progress: ProgressTarget::CommitCheck { check_run_id: None },
        claim_token: Some(Uuid::new_v4()),
    };
    let source = Arc::new(FakeSource::new(job));

    let date = chrono::DateTime::parse_from_rfc3339("2026-05-30T12:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let gh = Arc::new(FakeGitHub::new());
    // The runner resolves via the canonical qualified `tags/<name>`
    // form (note the slashy tag name `release/1.2`).
    gh.set_commit("octo/core", "tags/release/1.2", "tagsha123", Some(date));
    // Shell fails the first command → Failed outcome; we only care
    // that resolution ran before the run.
    let shell = Arc::new(RecordingShell::new());
    shell.reply(PreparedReply::fail(b"boom"));

    let runner = Runner::new(config, source.clone(), gh.clone(), shell);
    assert!(
        runner
            .run_once()
            .await
            .unwrap()
    );

    // The tag was resolved and the resolved commit (+ date) handed to
    // start_running.
    let resolved = source
        .started_commit()
        .expect("start_running received a resolved commit");
    assert_eq!(resolved.hash, "tagsha123");
    assert_eq!(resolved.committed_at, Some(date));
    assert!(
        gh.calls()
            .iter()
            .any(|c| matches!(c, sbgh_github::test_support::FakeCall::ResolveCommit { .. })),
        "runner must call resolve_commit for a tag job"
    );
}

/// v5 gate: a `ProgressTarget::Slack` job enqueues with an **empty** commit
/// and a rev (`develop`), so it MUST resolve to a commit at claim time (the
/// **bare** ref, not `tags/…`) and hand it to `start_running` — proving an
/// accepted Slack job passes `prepare` rather than failing the empty-commit
/// guard.
#[tokio::test]
async fn run_once_resolves_slack_rev_commit_in_preflight() {
    let tmp = TempDir::new().unwrap();
    let config = test_config(&tmp);

    let job = RunnableJob {
        id: Uuid::new_v4(),
        benchmark_group_id: Uuid::new_v4(),
        benchmark_spec_id: Uuid::new_v4(),
        benchmark_run_index: 0,
        requested_run_count: 1,
        group_requested_run_count: 1,
        group_run_index: 0,
        baseline_calibration_id: None,
        group_artifact_prefix: Uuid::new_v4().to_string(),
        repository: "octo/core".into(),
        commit: String::new(), // unresolved — a Slack ad-hoc job
        git_ref_display: "develop".into(),
        git_ref_kind: GitRefKind::Branch,
        installation_id: 7,
        task_kind: TaskKind::Benchmark,
        build_target: BuildTarget::StacksBench,
        workload_key: None,
        bench_args: vec![],
        progress: ProgressTarget::Slack {
            channel: "C1".into(),
            message_ts: "1700000000.000100".into(),
            reporting_identity: "0".repeat(64),
            plan_message_ts: None,
        },
        claim_token: Some(Uuid::new_v4()),
    };
    let source = Arc::new(FakeSource::new(job));

    let gh = Arc::new(FakeGitHub::new());
    // Resolved by the BARE rev (Slack doesn't qualify `tags/…`).
    gh.set_commit("octo/core", "develop", "slacksha", None);
    let shell = Arc::new(RecordingShell::new());
    shell.reply(PreparedReply::fail(b"boom")); // fail fast; we only assert preflight
    let runner = Runner::new(config, source.clone(), gh.clone(), shell);
    assert!(
        runner
            .run_once()
            .await
            .unwrap()
    );

    let resolved = source
        .started_commit()
        .expect("start_running received the resolved Slack commit");
    assert_eq!(resolved.hash, "slacksha");
    assert!(
        gh.calls()
            .iter()
            .any(|c| matches!(
                c,
                sbgh_github::test_support::FakeCall::ResolveCommit { git_ref, .. }
                    if git_ref == "develop"
            )),
        "runner must resolve the bare Slack rev"
    );
}

use sbgh_github::test_support::FakeCall;

fn config_with(tmp: &TempDir, pr: PrReport, baseline: BaselineReport) -> DaemonConfig {
    let mut c = test_config(tmp);
    c.reporting = ReportingConfig {
        pr_report: pr,
        baseline_report: baseline,
        noise_cv_pct: None,
    };
    c
}

/// Drive a (pre-resolved) job to a terminal `fail` via a failing shell,
/// returning the FakeGitHub + FakeSource call records for assertions.
async fn run_to_fail(config: DaemonConfig, job: RunnableJob) -> (Vec<FakeCall>, Vec<&'static str>) {
    // Jobs here carry a pre-resolved commit, so preflight skips the GitHub
    // commit-resolution call; a failing shell drives a terminal `fail`.
    let source = Arc::new(FakeSource::new(job));
    let gh = Arc::new(FakeGitHub::new());
    let shell = Arc::new(RecordingShell::new());
    shell.reply(PreparedReply::fail(b"boom: provisioning failed"));
    let runner = Runner::new(config, source.clone(), gh.clone(), shell);
    runner
        .run_once()
        .await
        .unwrap();
    (gh.calls(), source.calls())
}

fn pr_job(commit: &str, check_run_id: Option<i64>) -> RunnableJob {
    RunnableJob {
        id: Uuid::new_v4(),
        benchmark_group_id: Uuid::new_v4(),
        benchmark_spec_id: Uuid::new_v4(),
        benchmark_run_index: 0,
        requested_run_count: 1,
        group_requested_run_count: 1,
        group_run_index: 0,
        baseline_calibration_id: None,
        group_artifact_prefix: Uuid::new_v4().to_string(),
        repository: "acme/widgets".into(),
        commit: commit.into(),
        git_ref_display: "feature".into(),
        git_ref_kind: GitRefKind::Branch,
        installation_id: 7,
        task_kind: TaskKind::Benchmark,
        build_target: BuildTarget::StacksBench,
        workload_key: None,
        bench_args: vec![],
        progress: ProgressTarget::PullRequest {
            pr_number: 7,
            comment_id: None,
            check_run_id,
            check_run_url: None,
        },
        claim_token: Some(Uuid::new_v4()),
    }
}

/// PR job in `both` mode: creates an in-progress check, posts a comment
/// LINKING the check, then completes the check (`failure`) on failure.
#[tokio::test]
async fn pr_job_both_creates_check_and_linked_comment() {
    let tmp = TempDir::new().unwrap();
    let config = config_with(&tmp, PrReport::Both, BaselineReport::None);
    let (calls, src) = run_to_fail(config, pr_job("abc123", None)).await;

    assert!(
        calls
            .iter()
            .any(|c| matches!(c, FakeCall::CreateCheckRun { .. })),
        "created the PR-head check"
    );
    let comment = calls
        .iter()
        .find_map(|c| match c {
            FakeCall::CreateComment { body, .. } => Some(body.clone()),
            _ => None,
        })
        .expect("posted an initial comment");
    assert!(comment.contains("view check"), "comment links the check: {comment}");
    assert!(
        calls
            .iter()
            .any(|c| matches!(c, FakeCall::UpdateCheckRun { .. })),
        "completed the check on terminal"
    );
    assert!(src.contains(&"set_check_run"), "persisted check identity");
}

/// PR job in `comment` mode: no check at all, just the comment.
#[tokio::test]
async fn pr_job_comment_mode_skips_check() {
    let tmp = TempDir::new().unwrap();
    let config = config_with(&tmp, PrReport::Comment, BaselineReport::None);
    let (calls, _src) = run_to_fail(config, pr_job("abc123", None)).await;
    assert!(
        calls
            .iter()
            .any(|c| matches!(c, FakeCall::CreateComment { .. })),
        "posted the comment"
    );
    assert!(
        !calls
            .iter()
            .any(|c| matches!(c, FakeCall::CreateCheckRun { .. })),
        "comment mode must not create a check"
    );
}

/// Baseline job in `check` mode: a commit check, no comment.
#[tokio::test]
async fn baseline_job_check_mode_creates_commit_check_no_comment() {
    let tmp = TempDir::new().unwrap();
    let config = config_with(&tmp, PrReport::Both, BaselineReport::Check);
    let job = RunnableJob {
        progress: ProgressTarget::CommitCheck { check_run_id: None },
        ..pr_job("abc123", None)
    };
    let (calls, src) = run_to_fail(config, job).await;
    assert!(
        calls
            .iter()
            .any(|c| matches!(c, FakeCall::CreateCheckRun { .. })),
        "created the commit check"
    );
    assert!(
        calls
            .iter()
            .any(|c| matches!(c, FakeCall::UpdateCheckRun { .. })),
        "completed the commit check"
    );
    assert!(
        !calls
            .iter()
            .any(|c| matches!(c, FakeCall::CreateComment { .. })),
        "a baseline has no comment surface"
    );
    assert!(src.contains(&"set_check_run"));
}

/// The reconcile reuses a check created just before a crash (same
/// external_id) instead of creating a duplicate.
#[tokio::test]
async fn check_reconcile_reuses_existing_run() {
    let tmp = TempDir::new().unwrap();
    let config = config_with(&tmp, PrReport::Check, BaselineReport::None);
    let job = pr_job("abc123", None);
    let job_id = job.id;
    let source = Arc::new(FakeSource::new(job));
    let gh = Arc::new(FakeGitHub::new());
    gh.set_head_sha("acme/widgets", 7, "abc123");
    // A prior run exists for (repo, sha, name, external_id=job id).
    gh.set_existing_check_run("acme/widgets", "abc123", CHECK_NAME, &job_id.to_string(), 9999);
    let shell = Arc::new(RecordingShell::new());
    shell.reply(PreparedReply::fail(b"boom"));
    let runner = Runner::new(config, source.clone(), gh.clone(), shell);
    runner
        .run_once()
        .await
        .unwrap();

    let calls = gh.calls();
    assert!(
        calls
            .iter()
            .any(|c| matches!(c, FakeCall::FindCheckRun { .. })),
        "reconcile looked up by external_id"
    );
    assert!(
        !calls
            .iter()
            .any(|c| matches!(c, FakeCall::CreateCheckRun { .. })),
        "reused the existing run; must NOT create a duplicate"
    );
    // The reused id is persisted so the terminal update targets it.
    assert!(
        matches!(
            calls
                .iter()
                .find(|c| matches!(c, FakeCall::UpdateCheckRun { .. })),
            Some(FakeCall::UpdateCheckRun { check_run_id: 9999, .. })
        ),
        "completed the reused check 9999"
    );
}

/// A create that reached GitHub before its id was persisted is reconciled by
/// the stable App-authored marker rather than duplicated on reclaim.
#[tokio::test]
async fn comment_reconcile_closes_create_persist_crash_window() {
    let tmp = TempDir::new().unwrap();
    let config = config_with(&tmp, PrReport::Comment, BaselineReport::None);
    let job = pr_job("abc123", None);
    let marker = crate::report::pr_report_marker(job.benchmark_group_id);
    let source = Arc::new(FakeSource::new(job));
    let gh = Arc::new(FakeGitHub::new());
    gh.set_existing_comment("acme/widgets", 7, 4242, &marker, 8765);
    let shell = Arc::new(RecordingShell::new());
    shell.reply(PreparedReply::fail(b"boom"));

    Runner::new(config, source.clone(), gh.clone(), shell)
        .run_once()
        .await
        .unwrap();

    let calls = gh.calls();
    assert!(calls.iter().any(
        |call| matches!(call, FakeCall::FindComment { marker: found, .. } if found == &marker)
    ));
    assert!(
        !calls
            .iter()
            .any(|call| matches!(call, FakeCall::CreateComment { .. })),
        "replay must not create a second PR comment"
    );
    assert!(
        calls
            .iter()
            .any(|call| matches!(call, FakeCall::UpdateComment {
                comment_id: 8765,
                body,
                ..
            } if body.lines().any(|line| line.trim() == marker))),
        "the reconciled comment remains the reporting target and retains its marker"
    );
    assert!(
        source
            .calls()
            .contains(&"set_comment_id")
    );
}

#[tokio::test]
async fn comment_reconcile_failure_is_not_treated_as_not_found() {
    let tmp = TempDir::new().unwrap();
    let config = config_with(&tmp, PrReport::Comment, BaselineReport::None);
    let source = Arc::new(FakeSource::new(pr_job("abc123", None)));
    let gh = Arc::new(FakeGitHub::new());
    gh.fail_find_comment();
    let shell = Arc::new(RecordingShell::new());
    shell.reply(PreparedReply::fail(b"boom"));

    Runner::new(config, source, gh.clone(), shell)
        .run_once()
        .await
        .unwrap();

    let calls = gh.calls();
    assert!(
        calls
            .iter()
            .any(|call| matches!(call, FakeCall::FindComment { .. }))
    );
    assert!(
        !calls
            .iter()
            .any(|call| matches!(call, FakeCall::CreateComment { .. })),
        "lookup failure must not authorize a potentially duplicate comment"
    );
}

/// Non-fatal reporting: if BOTH `create_check_run` and `create_pr_comment`
/// error, the benchmark still runs to a terminal state.
#[tokio::test]
async fn reporting_create_failures_do_not_fail_the_job() {
    let tmp = TempDir::new().unwrap();
    let config = config_with(&tmp, PrReport::Both, BaselineReport::None);
    let source = Arc::new(FakeSource::new(pr_job("abc123", None)));
    let gh = Arc::new(FakeGitHub::new());
    gh.fail_create_check_run();
    gh.fail_create_comment();
    let shell = Arc::new(RecordingShell::new());
    shell.reply(PreparedReply::fail(b"boom"));
    let runner = Runner::new(config, source.clone(), gh.clone(), shell);
    runner
        .run_once()
        .await
        .unwrap();

    let calls = gh.calls();
    assert!(
        calls
            .iter()
            .any(|c| matches!(c, FakeCall::CreateCheckRun { .. })),
        "attempted the check"
    );
    assert!(
        calls
            .iter()
            .any(|c| matches!(c, FakeCall::CreateComment { .. })),
        "attempted the comment"
    );
    // Both surfaces errored, yet the job reached terminal `fail`.
    assert_eq!(source.calls(), vec!["start_running", "fail"]);
}

/// Non-fatal reporting: if PERSISTING the surface ids fails (the
/// `check_run_created` / `comment_posted` events), the job still
/// terminalizes and the in-memory ids are retained so the terminal update
/// still targets the live check.
#[tokio::test]
async fn reporting_persistence_failure_does_not_fail_the_job() {
    let tmp = TempDir::new().unwrap();
    let config = config_with(&tmp, PrReport::Both, BaselineReport::None);
    let source = Arc::new(FakeSource::new(pr_job("abc123", None)));
    source.fail_persist(); // set_check_run / set_comment_id will error
    let gh = Arc::new(FakeGitHub::new());
    let shell = Arc::new(RecordingShell::new());
    shell.reply(PreparedReply::fail(b"boom"));
    let runner = Runner::new(config, source.clone(), gh.clone(), shell);
    runner
        .run_once()
        .await
        .unwrap();

    let src = source.calls();
    assert!(src.contains(&"set_check_run"), "attempted to persist the check id");
    assert!(src.contains(&"fail"), "job reached terminal fail despite persistence errors");
    // The check id was retained in-memory (persistence failure swallowed),
    // so the terminal completion still updated the live check.
    let calls = gh.calls();
    assert!(
        calls
            .iter()
            .any(|c| matches!(c, FakeCall::UpdateCheckRun { .. })),
        "terminal update used the retained check id"
    );
}

/// If `GET /app` fails, the reconcile is skipped (no lookup) but the check
/// is still created and the job terminalizes. The `OnceCell` is left empty
/// (`get_or_try_init` doesn't cache the error), so a later job self-heals.
#[tokio::test]
async fn app_id_resolution_failure_skips_reconcile_but_not_the_job() {
    let tmp = TempDir::new().unwrap();
    let config = config_with(&tmp, PrReport::Check, BaselineReport::None);
    let source = Arc::new(FakeSource::new(pr_job("abc123", None)));
    let gh = Arc::new(FakeGitHub::new());
    gh.fail_current_app_id();
    let shell = Arc::new(RecordingShell::new());
    shell.reply(PreparedReply::fail(b"boom"));
    let runner = Runner::new(config, source.clone(), gh.clone(), shell);
    runner
        .run_once()
        .await
        .unwrap();

    let calls = gh.calls();
    assert!(
        !calls
            .iter()
            .any(|c| matches!(c, FakeCall::FindCheckRun { .. })),
        "reconcile is skipped when the App id is unknown"
    );
    assert!(
        calls
            .iter()
            .any(|c| matches!(c, FakeCall::CreateCheckRun { .. })),
        "the check is still created"
    );
    assert!(
        source
            .calls()
            .contains(&"fail"),
        "the job still reached terminal"
    );
}

/// Several jobs, run as independent tasks **concurrently** (sharing the
/// immutable config + App-id cache exactly as the coordinator would), each
/// reach a terminal state without interfering — the foundation for
/// `max_concurrent_jobs > 1`.
#[tokio::test]
async fn concurrent_jobs_reach_terminal_independently() {
    const N: usize = 5;
    let tmp = TempDir::new().unwrap();
    // Baseline jobs with reporting off → no GitHub, just the lifecycle.
    let config = Arc::new(config_with(&tmp, PrReport::Comment, BaselineReport::None));
    let app_id = Arc::new(OnceCell::new()); // shared across jobs, as in prod

    let mut handles = Vec::with_capacity(N);
    let mut sources = Vec::with_capacity(N);
    for _ in 0..N {
        let job = RunnableJob {
            progress: ProgressTarget::CommitCheck { check_run_id: None },
            ..pr_job("abc123", None)
        };
        let source = Arc::new(FakeSource::new(job.clone()));
        // Each job gets its own shell that fails provisioning → a clean
        // `Terminal::Failed` (a *ran-and-failed*, so `run` returns `Ok`).
        let shell = Arc::new(RecordingShell::new());
        shell.reply(PreparedReply::fail(b"boom: provisioning failed"));
        let driver: Arc<dyn Driver> = test_libvirt_driver(config.clone(), shell);
        let deps = JobDeps {
            artifact_store: build_test_artifact_store(config.as_ref()),
            config: config.clone(),
            jobs: source.clone(),
            gh: Arc::new(FakeGitHub::new()),
            worker: WorkerRuntime::with_driver(driver),
            cache_control: None,
            app_id: app_id.clone(),
            slack: None,
            pin_manager: None,
            repeat_planner: None,
            slack_sessions: Default::default(),
        };
        handles.push(tokio::spawn(deps.run(job, None, CancellationToken::new())));
        sources.push(source);
    }

    for h in handles {
        // No task panicked, and each reporter drove its job to terminal.
        h.await
            .expect("job task did not panic")
            .expect("job reached terminal without a setup error");
    }
    for source in sources {
        assert_eq!(
            source.calls(),
            vec!["start_running", "fail"],
            "each concurrent job ran to a terminal `fail`",
        );
    }
}

/// A store whose `start_running` **blocks** until the test releases it,
/// then errors — so a job task stays alive (holding its slot) without
/// needing the worker/driver, and completes the moment it's released.
/// Tracks the peak number blocked at once.
struct BlockingSource {
    queue: StdMutex<std::collections::VecDeque<RunnableJob>>,
    gate: tokio::sync::Semaphore,
    blocked: std::sync::atomic::AtomicUsize,
    peak_blocked: std::sync::atomic::AtomicUsize,
}

impl BlockingSource {
    fn new(jobs: Vec<RunnableJob>) -> Self {
        Self {
            queue: StdMutex::new(jobs.into()),
            gate: tokio::sync::Semaphore::new(0),
            blocked: std::sync::atomic::AtomicUsize::new(0),
            peak_blocked: std::sync::atomic::AtomicUsize::new(0),
        }
    }
    fn release(&self, n: usize) {
        self.gate.add_permits(n);
    }
    fn peak_blocked(&self) -> usize {
        self.peak_blocked
            .load(std::sync::atomic::Ordering::SeqCst)
    }
    /// Wait until at least `n` jobs are blocked in `start_running`.
    async fn await_blocked(&self, n: usize) {
        for _ in 0..2000 {
            if self
                .blocked
                .load(std::sync::atomic::Ordering::SeqCst)
                >= n
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        panic!("timed out waiting for {n} jobs blocked in start_running");
    }
}

#[async_trait]
impl RunnableJobStore for BlockingSource {
    async fn claim_next(&self) -> anyhow::Result<Option<RunnableJob>> {
        Ok(self
            .queue
            .lock()
            .unwrap()
            .pop_front())
    }
    async fn load_runnable(&self, _job_id: Uuid) -> anyhow::Result<Option<RunnableJob>> {
        Ok(None)
    }
    async fn list_queued(&self) -> anyhow::Result<Vec<RunnableJob>> {
        Ok(vec![])
    }
    async fn start_running(
        &self,
        _job: &RunnableJob,
        _resolved: Option<ResolvedCommit>,
    ) -> anyhow::Result<()> {
        use std::sync::atomic::Ordering::SeqCst;
        let now = self
            .blocked
            .fetch_add(1, SeqCst)
            + 1;
        self.peak_blocked
            .fetch_max(now, SeqCst);
        // Block until released, then error so the task completes here (no
        // worker needed) and frees its slot.
        self.gate
            .acquire()
            .await
            .unwrap()
            .forget();
        self.blocked
            .fetch_sub(1, SeqCst);
        anyhow::bail!("released by test")
    }
    async fn sweep_stuck_claims(&self, _lease: chrono::Duration) -> anyhow::Result<u64> {
        Ok(0)
    }
    async fn complete(&self, _: &RunnableJob, _: &serde_json::Value) -> anyhow::Result<()> {
        Ok(())
    }
    async fn find_baseline(
        &self,
        _subject_job_id: Uuid,
        _merge_base_sha: &str,
        _base_ref: &str,
        _at: Option<chrono::DateTime<chrono::Utc>>,
        _workload_key: &str,
    ) -> anyhow::Result<Option<crate::job_source::BaselineRef>> {
        Ok(None)
    }
    async fn fail(
        &self,
        _: &RunnableJob,
        _: &str,
        _: Option<&serde_json::Value>,
    ) -> anyhow::Result<()> {
        Ok(())
    }
    async fn cancel(&self, _: &RunnableJob, _: &str) -> anyhow::Result<()> {
        Ok(())
    }
    async fn set_comment_id(&self, _: &RunnableJob, _: i64) -> anyhow::Result<()> {
        Ok(())
    }
    async fn set_check_run(&self, _: &RunnableJob, _: i64, _: Option<&str>) -> anyhow::Result<()> {
        Ok(())
    }
    async fn set_plan_message_ts(&self, _: &RunnableJob, _: &str) -> anyhow::Result<()> {
        Ok(())
    }
    async fn running_job_ids(&self) -> anyhow::Result<Vec<Uuid>> {
        Ok(vec![])
    }
    async fn cancel_orphan(&self, _: Uuid, _: &str) -> anyhow::Result<bool> {
        Ok(false)
    }
}

/// The coordinator enforces `max_concurrent_jobs`: it spawns exactly the
/// limit, never over-claims while full, and tops the slot back up when a
/// job finishes — observed directly on the [`Coordinator`] fill/reap
/// seam.
#[tokio::test]
async fn coordinator_enforces_limit_and_tops_up() {
    let tmp = TempDir::new().unwrap();
    let config = Arc::new(config_with(&tmp, PrReport::Comment, BaselineReport::None));
    // 4 baseline jobs (commit pre-resolved; reporting off → no GitHub).
    let jobs: Vec<RunnableJob> = (0..4)
        .map(|_| RunnableJob {
            progress: ProgressTarget::CommitCheck { check_run_id: None },
            ..pr_job("abc123", None)
        })
        .collect();
    let source = Arc::new(BlockingSource::new(jobs));
    let driver: Arc<dyn Driver> =
        test_libvirt_driver(config.clone(), Arc::new(RecordingShell::new()));
    let deps = JobDeps {
        artifact_store: build_test_artifact_store(config.as_ref()),
        config,
        jobs: source.clone(),
        gh: Arc::new(FakeGitHub::new()),
        worker: WorkerRuntime::with_driver(driver),
        cache_control: None,
        app_id: Arc::new(OnceCell::new()),
        slack: None,
        pin_manager: None,
        repeat_planner: None,
        slack_sessions: Default::default(),
    };
    let mut coord = Coordinator::new(deps, 2, CancellationToken::new()); // max_concurrent = 2
    let never_draining = CancellationToken::new();

    // First fill claims exactly the limit (2), not all 4.
    assert_eq!(coord.fill_slots().await, 2, "first fill spawns exactly the limit");
    assert_eq!(coord.in_flight(), 2);
    source.await_blocked(2).await;
    assert_eq!(source.peak_blocked(), 2);

    // No over-claim while full: a second fill spawns nothing.
    assert_eq!(coord.fill_slots().await, 0, "no over-claim while slots are full");
    assert_eq!(coord.in_flight(), 2);

    // Release one job → it completes → reap frees a slot.
    source.release(1);
    while coord.in_flight() == 2 {
        coord
            .wait_for_progress(&never_draining)
            .await;
    }
    assert_eq!(coord.in_flight(), 1);

    // Re-fill tops the slot back up with the 3rd job — exactly one.
    assert_eq!(coord.fill_slots().await, 1, "tops up exactly one freed slot");
    assert_eq!(coord.in_flight(), 2);

    // Drain the rest; the limit must have held throughout.
    source.release(10);
    loop {
        coord.fill_slots().await;
        if coord.in_flight() == 0 {
            break;
        }
        coord
            .wait_for_progress(&never_draining)
            .await;
    }
    assert_eq!(source.peak_blocked(), 2, "never exceeded the configured limit");
}

/// When the job's token is cancelled, the worker reports
/// `Terminal::Aborted` regardless of what the (now cancel-safe) recipe
/// returns — the abort signal is the token, not the outcome. (The driver's
/// own teardown-on-cancel is covered in `libvirt::driver`.)
#[tokio::test]
async fn a_cancelled_run_is_reported_aborted() {
    let tmp = TempDir::new().unwrap();
    let config = Arc::new(config_with(&tmp, PrReport::Comment, BaselineReport::None));
    // Provisioning fails fast → `execute` returns promptly; the pre-cancelled
    // token then makes the worker report aborted.
    let shell = Arc::new(RecordingShell::new());
    shell.reply(PreparedReply::fail(b"boom: provisioning failed"));
    let driver: Arc<dyn Driver> = test_libvirt_driver(config, shell);
    let job = RunnableJob {
        progress: ProgressTarget::CommitCheck { check_run_id: None },
        ..pr_job("abc123", None)
    };

    let (events_tx, mut events_rx) = mpsc::channel(EVENT_BUFFER);
    let token = CancellationToken::new();
    token.cancel(); // cancelled before the run → outcome is overridden to aborted

    WorkerRuntime::with_driver(driver)
        .run(execution_request_for(&job, "abc123".into(), None, "--default"), events_tx, token)
        .await;

    match events_rx.recv().await {
        Some(WorkerEvent::Finished(Terminal::Aborted)) => {}
        other => panic!("expected Finished(Aborted), got {other:?}"),
    }
}

/// With a drain requested, the coordinator **stops claiming** (queued jobs
/// wait for the next boot) and, being idle, returns — firing `exit` so the
/// rest of the process can stop.
#[tokio::test]
async fn drain_stops_claiming_and_fires_exit_when_idle() {
    let tmp = TempDir::new().unwrap();
    let config = config_with(&tmp, PrReport::Comment, BaselineReport::None);
    // The source has a job ready, but a drain means it must never be claimed.
    let source = Arc::new(FakeSource::new(pr_job("abc123", None)));
    let runner = Runner::new(
        config,
        source.clone(),
        Arc::new(FakeGitHub::new()),
        Arc::new(RecordingShell::new()),
    );

    let shutdown = Shutdown::new();
    shutdown.draining.cancel(); // drain requested before the loop runs

    runner
        .run(shutdown.clone())
        .await
        .unwrap();

    assert!(shutdown.exit.is_cancelled(), "coordinator fired process-exit on drain-complete",);
    assert!(
        source.calls().is_empty(),
        "drained: the queued job was never claimed/started, got {:?}",
        source.calls(),
    );
}

/// Startup recovery (Phase 4B-2 + 4C): a job left `running` by a crashed
/// daemon is cleaned (the coordinator runs `cleanup_by_job_id` — observed
/// as a `virsh destroy` on the shell) and terminal-cancelled
/// (`cancel_orphan`) before the loop starts claiming. Driven with a
/// pre-set drain so the loop exits straight after recovery.
#[tokio::test]
async fn startup_recovers_orphaned_running_job() {
    let tmp = TempDir::new().unwrap();
    let config = config_with(&tmp, PrReport::Comment, BaselineReport::None);
    let source = Arc::new(FakeSource::new(pr_job("abc123", None)));
    source.add_orphan(Uuid::new_v4());

    // Cleanup first proves the domain state, then destroys/undefines it before
    // touching any backing resource.
    let shell = Arc::new(RecordingShell::new());
    shell
        .reply(PreparedReply::with_stdout("running\n")) // virsh domstate
        .expect_ok(1) // virsh destroy
        .expect_ok(1) // virsh undefine
        .expect_ok(1) // umount tmpfs
        .expect_ok(1) // umount source.mnt
        .reply(PreparedReply::with_stdout("")) // losetup -j → nothing attached
        .expect_ok(1) // lvremove
        .expect_ok(1); // git prune

    let runner = Runner::new(config, source.clone(), Arc::new(FakeGitHub::new()), shell.clone());
    let shutdown = Shutdown::new();
    shutdown.draining.cancel(); // skip the claim loop — we exercise only startup recovery

    runner
        .run(shutdown)
        .await
        .unwrap();

    // Recovery cleaned the leaked VM …
    assert!(
        shell.calls().iter().any(|c| {
            c.program.ends_with("virsh")
                && c.args
                    .iter()
                    .any(|a| a == "destroy")
        }),
        "recovery ran cleanup_by_job_id (virsh destroy) for the orphan",
    );
    // … and terminal-cancelled the orphaned row.
    assert!(
        source
            .calls()
            .contains(&"cancel_orphan"),
        "recovery terminal-cancelled the orphan via cancel_orphan, got {:?}",
        source.calls(),
    );
}

/// Codex 4B-2 Medium: if `cleanup_by_job_id` can't fully clear the VM (the
/// source loop won't detach), the orphan must be LEFT `running` (no
/// `cancel_orphan`) so the next boot retries — and a per-orphan failure
/// stays non-fatal (the loop still runs to a clean exit).
#[tokio::test]
async fn startup_leaves_orphan_running_when_cleanup_incomplete() {
    let tmp = TempDir::new().unwrap();
    let config = config_with(&tmp, PrReport::Comment, BaselineReport::None);
    let source = Arc::new(FakeSource::new(pr_job("abc123", None)));
    source.add_orphan(Uuid::new_v4());

    // `losetup -j` lists a device whose `-d` FAILS → cleanup reports incomplete.
    let shell = Arc::new(RecordingShell::new());
    shell
        .reply(PreparedReply::with_stdout("running\n")) // virsh domstate
        .expect_ok(1) // virsh destroy
        .expect_ok(1) // virsh undefine
        .expect_ok(1) // umount tmpfs
        .expect_ok(1) // umount source.mnt
        .reply(PreparedReply::with_stdout("/dev/loop9: [2049]:1 (j/source.raw)\n")) // losetup -j
        .reply(PreparedReply::fail("device or resource busy")) // losetup -d fails
        .expect_ok(1) // lvremove
        .expect_ok(1); // git prune

    let runner = Runner::new(config, source.clone(), Arc::new(FakeGitHub::new()), shell.clone());
    let shutdown = Shutdown::new();
    shutdown.draining.cancel();

    runner
        .run(shutdown)
        .await
        .unwrap(); // per-orphan failure is non-fatal

    assert!(
        !source
            .calls()
            .contains(&"cancel_orphan"),
        "an incompletely-cleaned orphan must be left `running`, not cancelled; got {:?}",
        source.calls(),
    );
}

/// Codex 4B-2 Medium: failure to even *enumerate* running rows is
/// startup-critical (we can't rule out live orphan VMs), so it propagates —
/// the runner returns `Err` (process exits → systemd retries) and never
/// claims fresh work.
#[tokio::test]
async fn startup_recovery_aborts_when_listing_running_jobs_fails() {
    let tmp = TempDir::new().unwrap();
    let config = config_with(&tmp, PrReport::Comment, BaselineReport::None);
    let source = Arc::new(FakeSource::new(pr_job("abc123", None)));
    source.fail_list(); // running_job_ids errors

    let runner = Runner::new(
        config,
        source.clone(),
        Arc::new(FakeGitHub::new()),
        Arc::new(RecordingShell::new()),
    );
    let shutdown = Shutdown::new();
    shutdown.draining.cancel(); // even mid-drain, recovery runs first and must abort

    let err = runner
        .run(shutdown)
        .await
        .unwrap_err();
    assert!(
        format!("{err:#}").contains("enumerating `running` jobs failed"),
        "list failure is startup-critical and propagates, got: {err:#}",
    );
    assert!(source.calls().is_empty(), "aborted before any claim, got {:?}", source.calls(),);
}

/// Phase 4C-2: a recovered orphan's stuck `in_progress` Check Run is
/// concluded `cancelled` (gray) via the normal reporting path —
/// `load_runnable` reconstructs the orphan's reporting context and the
/// surface's `cancelled` concludes its check.
#[tokio::test]
async fn startup_concludes_orphan_check_as_cancelled() {
    let tmp = TempDir::new().unwrap();
    let config = config_with(&tmp, PrReport::Comment, BaselineReport::None);
    let orphan_id = Uuid::new_v4();
    let source = Arc::new(FakeSource::new(pr_job("abc123", None)));
    source.add_orphan(orphan_id);
    // The reconstructed orphan view carries a commit check (id 555) to conclude.
    source.set_orphan_runnable(RunnableJob {
        id: orphan_id,
        benchmark_group_id: Uuid::new_v4(),
        benchmark_spec_id: Uuid::new_v4(),
        benchmark_run_index: 0,
        requested_run_count: 1,
        baseline_calibration_id: None,
        group_artifact_prefix: Uuid::new_v4().to_string(),
        progress: ProgressTarget::CommitCheck { check_run_id: Some(555) },
        ..pr_job("abc123", None)
    });

    let gh = Arc::new(FakeGitHub::new());
    // `cleanup_by_job_id` (no loop attached) issues seven shell calls.
    let shell = Arc::new(RecordingShell::new());
    shell
        .expect_ok(1) // virsh destroy
        .expect_ok(1) // virsh undefine
        .expect_ok(1) // umount tmpfs
        .expect_ok(1) // umount source.mnt
        .reply(PreparedReply::with_stdout("")) // losetup -j → none
        .expect_ok(1) // lvremove
        .expect_ok(1); // git prune

    let runner = Runner::new(config, source.clone(), gh.clone(), shell);
    let shutdown = Shutdown::new();
    shutdown.draining.cancel();

    runner
        .run(shutdown)
        .await
        .unwrap();

    // The orphan row was cancelled …
    assert!(
        source
            .calls()
            .contains(&"cancel_orphan"),
        "orphan row cancelled, got {:?}",
        source.calls(),
    );
    // … and its stuck check concluded `cancelled` (neutral-gray), not left
    // spinning.
    let concluded = gh
        .calls()
        .into_iter()
        .find_map(|c| match c {
            FakeCall::UpdateCheckRun { check_run_id: 555, state, .. } => Some(state),
            _ => None,
        })
        .expect("the orphan's check was concluded");
    assert_eq!(
        concluded,
        sbgh_github::CheckRunState::Completed(sbgh_github::CheckRunConclusion::Cancelled),
    );
}

/// Build a `Coordinator` over a `FakeSource` for the queue-position tests.
fn position_coord(
    config: DaemonConfig,
    source: Arc<FakeSource>,
    gh: Arc<FakeGitHub>,
) -> Coordinator {
    let config = Arc::new(config);
    let driver: Arc<dyn Driver> =
        test_libvirt_driver(config.clone(), Arc::new(RecordingShell::new()));
    let deps = JobDeps {
        artifact_store: build_test_artifact_store(config.as_ref()),
        config,
        jobs: source,
        gh,
        worker: WorkerRuntime::with_driver(driver),
        cache_control: None,
        app_id: Arc::new(OnceCell::new()),
        slack: None,
        pin_manager: None,
        repeat_planner: None,
        slack_sessions: Default::default(),
    };
    Coordinator::new(deps, 1, CancellationToken::new())
}

fn create_summary_for(gh: &FakeGitHub, job_id: Uuid) -> Option<String> {
    gh.calls()
        .into_iter()
        .find_map(|c| match c {
            FakeCall::CreateCheckRun { external_id, output, .. }
                if external_id == job_id.to_string() =>
            {
                Some(output.summary)
            }
            _ => None,
        })
}

fn create_count(gh: &FakeGitHub) -> usize {
    gh.calls()
        .iter()
        .filter(|c| matches!(c, FakeCall::CreateCheckRun { .. }))
        .count()
}

/// Phase 5: the coordinator reports each queued job its position on a
/// freshly created (and persisted, so the claim-time reporter adopts
/// it) check, with text matching claim order. A second pass with
/// unchanged positions makes NO new GitHub edits (the `last_positions`
/// debounce).
#[tokio::test]
async fn coordinator_reports_queue_positions_and_debounces() {
    let tmp = TempDir::new().unwrap();
    let config = config_with(&tmp, PrReport::Check, BaselineReport::None);
    let j0 = pr_job("sha0", None);
    let j1 = pr_job("sha1", None);
    let source = Arc::new(FakeSource::new(pr_job("unused", None)));
    source.set_queued(vec![j0.clone(), j1.clone()]);
    let gh = Arc::new(FakeGitHub::new());
    let mut coord = position_coord(config, source.clone(), gh.clone());

    coord
        .update_queue_positions()
        .await;

    // in_flight = 0 → ahead = index. Position text matches claim order.
    assert_eq!(create_summary_for(&gh, j0.id).as_deref(), Some("queued — next to run"));
    assert_eq!(create_summary_for(&gh, j1.id).as_deref(), Some("queued — 1 run ahead"));
    assert_eq!(create_count(&gh), 2);
    // Each created check's id was persisted so a later claim adopts it.
    assert_eq!(
        source
            .calls()
            .iter()
            .filter(|c| **c == "set_check_run")
            .count(),
        2,
    );

    // Second pass, unchanged queue → debounced (no new creates).
    coord
        .update_queue_positions()
        .await;
    assert_eq!(create_count(&gh), 2, "unchanged positions make no new GitHub edits");
}

/// A queued job that already has a check (persisted by a prior tick / read
/// back on re-claim) is **updated**, never duplicated.
#[tokio::test]
async fn coordinator_updates_existing_position_check_without_duplicating() {
    let tmp = TempDir::new().unwrap();
    let config = config_with(&tmp, PrReport::Check, BaselineReport::None);
    let source = Arc::new(FakeSource::new(pr_job("unused", None)));
    source.set_queued(vec![pr_job("sha", Some(900))]);
    let gh = Arc::new(FakeGitHub::new());
    let mut coord = position_coord(config, source.clone(), gh.clone());

    coord
        .update_queue_positions()
        .await;

    assert!(
        gh.calls()
            .iter()
            .any(|c| matches!(c, FakeCall::UpdateCheckRun { check_run_id: 900, .. })),
        "the existing check was updated",
    );
    assert_eq!(create_count(&gh), 0, "must update, not create a duplicate");
}

/// Eligibility: a baseline job whose `baseline_report` wants no check, and
/// a job with an unresolved commit (a tag job pre-claim), both get NO
/// pre-claim position check.
#[tokio::test]
async fn coordinator_skips_position_for_no_check_and_unresolved_jobs() {
    let tmp = TempDir::new().unwrap();
    let config = config_with(&tmp, PrReport::Check, BaselineReport::None);
    let baseline = RunnableJob {
        progress: ProgressTarget::CommitCheck { check_run_id: None },
        ..pr_job("sha", None)
    };
    let unresolved = RunnableJob {
        commit: String::new(),
        ..pr_job("sha", None)
    };
    let source = Arc::new(FakeSource::new(pr_job("unused", None)));
    source.set_queued(vec![baseline, unresolved]);
    let gh = Arc::new(FakeGitHub::new());
    let mut coord = position_coord(config, source.clone(), gh.clone());

    coord
        .update_queue_positions()
        .await;

    assert!(
        gh.calls().is_empty(),
        "no-check + unresolved-commit jobs get no position check, got {:?}",
        gh.calls(),
    );
}

/// Codex 5.1 Medium: if persisting the check id fails after GitHub create,
/// the position must NOT be recorded as up-to-date — otherwise the debounce
/// would suppress the only retry that records the `check_run_created` event
/// the claim-time reporter adopts. So a second tick still retries.
#[tokio::test]
async fn coordinator_retries_position_check_when_persist_fails() {
    let tmp = TempDir::new().unwrap();
    let config = config_with(&tmp, PrReport::Check, BaselineReport::None);
    let source = Arc::new(FakeSource::new(pr_job("unused", None)));
    source.set_queued(vec![pr_job("sha", None)]);
    source.fail_persist(); // set_check_run errors
    let gh = Arc::new(FakeGitHub::new());
    let mut coord = position_coord(config, source.clone(), gh.clone());

    coord
        .update_queue_positions()
        .await;
    coord
        .update_queue_positions()
        .await;

    // A failed persist isn't recorded in `last_positions`, so the position
    // wasn't debounced — the second tick attempted the check again.
    assert_eq!(
        create_count(&gh),
        2,
        "a persist failure must leave the position un-debounced so it retries",
    );
}

/// Records canonical Slack queue-position snapshots.
#[derive(Default)]
struct FakePositionSlack {
    updates: StdMutex<Vec<(String, String, String, u64)>>, // (channel, ts, text, version)
}

#[async_trait]
impl SlackClient for FakePositionSlack {
    async fn post_ephemeral(&self, _c: &str, _u: &str, _t: &str) -> sbgh_slack::Result<()> {
        Ok(())
    }
    async fn post_message(
        &self,
        _target: &sbgh_slack::SlackMessageTarget,
        _text: &str,
        _identity: &sbgh_slack::ReportingIdentity,
        _snapshot_version: u64,
    ) -> sbgh_slack::Result<String> {
        Ok("ts".into())
    }
    async fn update_message(
        &self,
        channel: &str,
        ts: &str,
        text: &str,
        _identity: &sbgh_slack::ReportingIdentity,
        snapshot_version: u64,
    ) -> sbgh_slack::Result<()> {
        self.updates
            .lock()
            .unwrap()
            .push((channel.into(), ts.into(), text.into(), snapshot_version));
        Ok(())
    }
    async fn find_messages(
        &self,
        _target: &sbgh_slack::SlackMessageTarget,
        _identity: &sbgh_slack::ReportingIdentity,
    ) -> sbgh_slack::Result<Vec<sbgh_slack::FoundMessage>> {
        Ok(Vec::new())
    }
    async fn add_reaction(&self, _c: &str, _t: &str, _r: &str) -> sbgh_slack::Result<()> {
        Ok(())
    }
    async fn remove_reaction(&self, _c: &str, _t: &str, _r: &str) -> sbgh_slack::Result<()> {
        Ok(())
    }
}

/// A queued Slack job (pre-claim: empty commit) with an optional
/// canonical-message `plan_message_ts`.
fn slack_job(plan_message_ts: Option<&str>) -> RunnableJob {
    RunnableJob {
        commit: String::new(),
        progress: ProgressTarget::Slack {
            channel: "C1".into(),
            message_ts: "REQ".into(),
            reporting_identity: "0".repeat(64),
            plan_message_ts: plan_message_ts.map(Into::into),
        },
        ..pr_job("unused", None)
    }
}

fn position_coord_with_slack(
    config: DaemonConfig,
    source: Arc<FakeSource>,
    slack: Arc<dyn SlackClient>,
) -> Coordinator {
    let config = Arc::new(config);
    let driver: Arc<dyn Driver> =
        test_libvirt_driver(config.clone(), Arc::new(RecordingShell::new()));
    let deps = JobDeps {
        artifact_store: build_test_artifact_store(config.as_ref()),
        config,
        jobs: source,
        gh: Arc::new(FakeGitHub::new()),
        worker: WorkerRuntime::with_driver(driver),
        cache_control: None,
        app_id: Arc::new(OnceCell::new()),
        slack: Some(slack),
        pin_manager: None,
        repeat_planner: None,
        slack_sessions: Default::default(),
    };
    Coordinator::new(deps, 1, CancellationToken::new())
}

/// A queued Slack job with a persisted message timestamp gets a full
/// queue-position snapshot, debounced like the GitHub position check.
#[tokio::test]
async fn coordinator_updates_slack_queue_position_and_debounces() {
    let tmp = TempDir::new().unwrap();
    let config = config_with(&tmp, PrReport::Check, BaselineReport::None);
    let source = Arc::new(FakeSource::new(pr_job("unused", None)));
    source.set_queued(vec![slack_job(Some("PLAN_TS"))]);
    let slack = Arc::new(FakePositionSlack::default());
    let mut coord = position_coord_with_slack(config, source.clone(), slack.clone());

    coord
        .update_queue_positions()
        .await;

    {
        let updates = slack.updates.lock().unwrap();
        assert_eq!(updates.len(), 1, "the queued snapshot was updated once");
        assert_eq!(updates[0].1, "PLAN_TS", "updated the persisted message ts");
        // in_flight 0 + 1 queued → total 1, ahead 0 → "position 1/1".
        assert!(
            updates[0]
                .2
                .contains("queue position 1/1"),
            "{}",
            updates[0].2
        );
        assert!(
            updates[0]
                .2
                .contains("Queued"),
            "snapshot queued: {}",
            updates[0].2
        );
    }

    // Second pass, unchanged position → no new update.
    coord
        .update_queue_positions()
        .await;
    assert_eq!(
        slack
            .updates
            .lock()
            .unwrap()
            .len(),
        1,
        "an unchanged position makes no new edit",
    );
}

#[tokio::test]
async fn queued_position_cannot_regress_a_started_slack_snapshot() {
    let tmp = TempDir::new().unwrap();
    let config = config_with(&tmp, PrReport::Check, BaselineReport::None);
    let job = slack_job(Some("PLAN_TS"));
    let source = Arc::new(FakeSource::new(pr_job("unused", None)));
    source.set_queued(vec![job.clone()]);
    let slack = Arc::new(FakePositionSlack::default());
    let mut coord = position_coord_with_slack(config, source, slack.clone());

    let surface = build_slack_surface(
        slack.clone(),
        coord
            .deps
            .slack_sessions
            .clone(),
        coord.deps.jobs.clone(),
        coord
            .deps
            .artifact_store
            .clone(),
        &job,
    );
    surface
        .started()
        .await
        .unwrap();
    assert_eq!(
        slack
            .updates
            .lock()
            .unwrap()
            .len(),
        1
    );

    // Simulate the coordinator finishing a stale pre-claim queue scan after
    // the reporter has already projected the job into Preparing.
    coord
        .update_queue_positions()
        .await;

    let updates = slack.updates.lock().unwrap();
    assert_eq!(updates.len(), 1, "the shared session must reject stale queued state");
    assert!(
        updates[0]
            .2
            .contains("Preparing"),
        "{}",
        updates[0].2
    );
}

#[tokio::test]
async fn started_snapshot_advances_beyond_a_prior_queue_position() {
    let tmp = TempDir::new().unwrap();
    let config = config_with(&tmp, PrReport::Check, BaselineReport::None);
    let job = slack_job(Some("PLAN_TS"));
    let source = Arc::new(FakeSource::new(pr_job("unused", None)));
    source.set_queued(vec![job.clone()]);
    let slack = Arc::new(FakePositionSlack::default());
    let mut coord = position_coord_with_slack(config, source, slack.clone());

    coord
        .update_queue_positions()
        .await;
    let surface = build_slack_surface(
        slack.clone(),
        coord
            .deps
            .slack_sessions
            .clone(),
        coord.deps.jobs.clone(),
        coord
            .deps
            .artifact_store
            .clone(),
        &job,
    );
    surface
        .started()
        .await
        .unwrap();

    let updates = slack.updates.lock().unwrap();
    assert_eq!(updates.len(), 2);
    assert!(
        updates[0]
            .2
            .contains("queue position 1/1"),
        "{}",
        updates[0].2
    );
    assert!(
        updates[1]
            .2
            .contains("Preparing"),
        "{}",
        updates[1].2
    );
    assert!(updates[1].3 > updates[0].3, "started must advance the version fence");
}

#[tokio::test]
async fn coordinator_skips_queue_position_for_appended_repeat_slack_runs() {
    let tmp = TempDir::new().unwrap();
    let config = config_with(&tmp, PrReport::Check, BaselineReport::None);
    let source = Arc::new(FakeSource::new(pr_job("unused", None)));
    let mut repeat = slack_job(Some("PLAN_TS"));
    repeat.benchmark_run_index = 1;
    repeat.requested_run_count = 2;
    repeat.group_requested_run_count = 2;
    repeat.group_run_index = 1;
    source.set_queued(vec![repeat]);
    let slack = Arc::new(FakePositionSlack::default());
    let mut coord = position_coord_with_slack(config, source.clone(), slack.clone());

    coord
        .update_queue_positions()
        .await;

    assert!(
        slack
            .updates
            .lock()
            .unwrap()
            .is_empty(),
        "later repeat runs inherit the group message but must not overwrite it with queue \
             position",
    );
}

/// Phase 5 CPU pinning: each concurrency slot maps to its configured
/// `[runner].cpu_sets` cpuset (stable: slot 0 → cpu_sets[0]); an
/// out-of-range slot or empty config → no pinning.
#[tokio::test]
async fn coordinator_maps_slot_to_configured_cpuset() {
    let tmp = TempDir::new().unwrap();
    let mut config = config_with(&tmp, PrReport::Comment, BaselineReport::None);
    config.runner.cpu_sets = vec!["0-1".into(), "2-3".into()];
    config.runner.host_cpus = Some("4-5".into());
    let coord = position_coord(
        config,
        Arc::new(FakeSource::new(pr_job("abc", None))),
        Arc::new(FakeGitHub::new()),
    );

    assert_eq!(
        coord
            .cpuset_for_slot(0)
            .as_deref(),
        Some("0-1"),
        "slot 0 → first cpuset"
    );
    assert_eq!(
        coord
            .cpuset_for_slot(1)
            .as_deref(),
        Some("2-3"),
        "slot 1 → second cpuset"
    );
    assert_eq!(coord.cpuset_for_slot(2), None, "out-of-range slot is unpinned");

    // No cpu_sets → every slot floats.
    let bare_coord = position_coord(
        config_with(&tmp, PrReport::Comment, BaselineReport::None),
        Arc::new(FakeSource::new(pr_job("abc", None))),
        Arc::new(FakeGitHub::new()),
    );
    assert_eq!(bare_coord.cpuset_for_slot(0), None, "no cpu_sets → unpinned");
}
