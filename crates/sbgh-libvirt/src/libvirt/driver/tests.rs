use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use sbgh_driver::{
    ArtifactSink, BinaryCacheStore, BlockValidationTaskSpec, CachedBinary, DriverStatus,
    DriverTaskOutput, InclusiveRange,
};
use tempfile::TempDir;
use uuid::Uuid;

use super::*;
use crate::libvirt::shell::test_support::{PreparedReply, RecordingShell};
use crate::{
    BenchmarkProfile, BlockValidationProfile, LibvirtConfig, LvmConfig, PathsConfig, VmConfig,
};

struct TestJob {
    id: Uuid,
    repository: String,
    commit: String,
    bench_args: Vec<String>,
}

fn create_bare_mirror(path: &Path) {
    std::fs::create_dir_all(path.join("objects")).unwrap();
    std::fs::create_dir_all(path.join("refs")).unwrap();
    std::fs::write(path.join("HEAD"), b"ref: refs/heads/main\n").unwrap();
    std::fs::write(path.join("config"), b"[core]\n\tbare = true\n").unwrap();
}

/// A platform-neutral [`TaskContext`] borrowed from a fake job — the
/// `run_benchmark` inputs the driver actually reads.
fn ctx_of(job: &TestJob) -> TaskContext<'_> {
    TaskContext {
        job_id: job.id,
        attempt_id: job.id,
        fencing_generation: 0,
        repository: &job.repository,
        commit: &job.commit,
        repository_credential: None,
    }
}

struct LocalArtifactSink {
    root: PathBuf,
}

#[async_trait::async_trait]
impl ArtifactSink for LocalArtifactSink {
    async fn put(&self, key: &str, src: &Path) -> Option<u64> {
        let dest = self.root.join(key);
        std::fs::create_dir_all(dest.parent()?).ok()?;
        std::fs::copy(src, dest).ok()
    }

    async fn get(&self, key: &str) -> std::io::Result<PathBuf> {
        let path = self.root.join(key);
        path.is_file()
            .then_some(path)
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::NotFound))
    }

    fn job_dir(&self, job_id: &str) -> PathBuf {
        self.root.join(job_id)
    }
}

#[test]
fn parses_schema_v1_baseline_calibration_payload() {
    let result = BaselineCalibrationResult::from_bytes(
        br#"{
            "schema_version": 1,
            "success": true,
            "result_type": "baseline_calibration",
            "result_version": 1,
            "duration_secs": 12.0,
            "result": { "calibration_id": 12 }
        }"#,
    )
    .unwrap();
    assert_eq!(result.calibration_id(), Some(12));
}

#[test]
fn baseline_calibration_rejects_wrong_type_or_missing_id() {
    let wrong_type = BaselineCalibrationResult::from_bytes(
        br#"{
            "schema_version": 1,
            "success": true,
            "result_type": "run",
            "result_version": 1,
            "result": { "calibration_id": 12 }
        }"#,
    )
    .unwrap();
    assert_eq!(wrong_type.calibration_id(), None);

    let missing_id = BaselineCalibrationResult::from_bytes(
        br#"{
            "schema_version": 1,
            "success": true,
            "result_type": "baseline_calibration",
            "result_version": 1,
            "result": {}
        }"#,
    )
    .unwrap();
    assert_eq!(missing_id.calibration_id(), None);
}

#[derive(Default)]
struct TestCache {
    entries: Mutex<HashMap<String, CachedBinary>>,
}

impl BinaryCacheStore for TestCache {
    fn get(&self, fingerprint: &BuildFingerprint, _now_unix: u64) -> Option<CachedBinary> {
        let entries = self.entries.lock().unwrap();
        let hit = entries.get(&fingerprint.digest())?;
        Some(CachedBinary {
            path: hit.path.clone(),
            digest: hit.digest.clone(),
            sha256: hit.sha256.clone(),
            size_bytes: hit.size_bytes,
            last_used: hit.last_used,
            pinned: hit.pinned,
        })
    }

    fn publish(
        &self,
        fingerprint: &BuildFingerprint,
        src: &Path,
        now_unix: u64,
        _pinned: bool,
    ) -> std::io::Result<String> {
        let digest = fingerprint.digest();
        self.entries
            .lock()
            .unwrap()
            .insert(
                digest.clone(),
                CachedBinary {
                    path: src.to_path_buf(),
                    digest,
                    sha256: String::new(),
                    size_bytes: std::fs::metadata(src)?.len(),
                    last_used: now_unix,
                    pinned: false,
                },
            );
        Ok(String::new())
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
        None,
        "deadbeef",
        BuildArtifact::StacksBench,
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
        None,
        "deadbeef",
        BuildArtifact::StacksBench,
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
        None,
        "sha",
        BuildArtifact::StacksBench,
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
            None,
            "sha",
            BuildArtifact::StacksBench,
        )
        .await
        .is_none(),
        "missing toolchain files → None"
    );
}

#[derive(Default)]
struct RecordingListener {
    phases: std::sync::Mutex<Vec<Phase>>,
    progresses: std::sync::Mutex<Vec<ProgressUpdate>>,
}

#[async_trait::async_trait]
impl PhaseListener for RecordingListener {
    async fn on_phase(&self, phase: &Phase) {
        self.phases
            .lock()
            .unwrap()
            .push(phase.clone());
    }

    async fn on_progress(&self, progress: ProgressUpdate) {
        self.progresses
            .lock()
            .unwrap()
            .push(progress);
    }
}

/// Enabled cache + a fingerprint-matched entry: the driver resolves the hit
/// before source provisioning, creates a minimal binary-only source disk,
/// reports the cached build, and skips the build VM.
#[tokio::test]
async fn enabled_cache_hit_uses_minimal_source_disk_and_skips_build_vm() {
    let tmp = TempDir::new().unwrap();
    let cfg = test_config(&tmp);
    // `image_proxy_id` needs a real golden-image file; enable the cache.
    std::fs::write(&cfg.vm.golden_image, b"golden").unwrap();
    let cache = Arc::new(TestCache::default());

    // The exact fingerprint the driver will compute for this run, then a
    // cached binary published under it.
    let toolchain = "[toolchain]\nchannel = \"1.95.0\"\n";
    let fp = BuildFingerprint {
        artifact: BuildArtifact::StacksBench,
        repository: None,
        commit: "abc123def456".into(),
        toolchain: fingerprint::toolchain_channel(toolchain).unwrap(),
        profile: BUILD_PROFILE.into(),
        features: String::new(),
        rustflags: String::new(),
        target_triple: BUILD_TARGET_TRIPLE.into(),
        recipe_version: BUILD_RECIPE_VERSION,
        image_id: fingerprint::image_proxy_id(&cfg.vm.golden_image).unwrap(),
        protocol_version: BUILD_PROTOCOL_VERSION.into(),
    };
    let cached_bin = tmp.path().join("cached-bin");
    std::fs::write(&cached_bin, b"CACHED").unwrap();
    cache
        .publish(&fp, &cached_bin, 1, false)
        .unwrap();

    let cfg = Arc::new(cfg);
    let job = fake_job();
    create_bare_mirror(&cfg.paths.git_mirror);
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
        .reply(PreparedReply::with_stdout("mainnet-2026-05-21|Vri---tz-k\n")) // immutable origin
        .reply(PreparedReply::with_stdout("10|5\n")) // shared pool health
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

    let driver = test_driver_with_cache(&cfg, shell.clone(), Some(cache));
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
async fn sqlite_seed_key_copies_submission_db_into_results_tmpfs() {
    let tmp = TempDir::new().unwrap();
    let cfg = test_config(&tmp);
    let seed_key = "submission1/shared/stacks-bench.db".to_string();
    let seed_path = cfg
        .paths
        .results_archive_dir
        .join(&seed_key);
    std::fs::create_dir_all(seed_path.parent().unwrap()).unwrap();
    std::fs::write(&seed_path, b"submission sqlite").unwrap();

    let tmpfs = ResultsTmpfs {
        mount_dir: tmp.path().join("results"),
        size_mib: 256,
    };
    std::fs::create_dir_all(&tmpfs.mount_dir).unwrap();

    let store = LocalArtifactSink {
        root: cfg
            .paths
            .results_archive_dir
            .clone(),
    };
    seed_sqlite_from_store(&store, &seed_key, &tmpfs)
        .await
        .unwrap();

    assert_eq!(std::fs::read(tmpfs.sqlite_file()).unwrap(), b"submission sqlite");
}

#[test]
fn block_diagnostic_collection_is_bounded_and_rejects_symlinks() {
    use std::os::unix::fs::symlink;

    let tmp = TempDir::new().unwrap();
    std::fs::write(
        tmp.path()
            .join("shard-0.stdout.log"),
        b"diagnostic",
    )
    .unwrap();
    std::fs::write(
        tmp.path()
            .join("block-validation-result.json"),
        b"{}",
    )
    .unwrap();
    std::fs::write(
        tmp.path()
            .join("unrelated-secret"),
        b"ignored",
    )
    .unwrap();

    let paths = block_diagnostic_paths(tmp.path()).unwrap();
    assert_eq!(paths.len(), 2);
    assert!(
        paths
            .iter()
            .all(|path| path.file_name().unwrap() != "unrelated-secret")
    );

    let outside = tmp.path().join("outside");
    std::fs::write(&outside, b"host data").unwrap();
    symlink(
        &outside,
        tmp.path()
            .join("shard-1.stderr.log"),
    )
    .unwrap();
    assert!(
        block_diagnostic_paths(tmp.path())
            .unwrap_err()
            .to_string()
            .contains("not a regular file")
    );
}

#[test]
fn baseline_calibration_id_reads_json_or_handoff_file() {
    let tmp = TempDir::new().unwrap();
    let tmpfs = ResultsTmpfs {
        mount_dir: tmp.path().to_path_buf(),
        size_mib: 64,
    };
    std::fs::write(
            tmpfs.calibration_json(),
            br#"{"schema_version":1,"success":true,"result_type":"baseline_calibration","result_version":1,"result":{"calibration_id":12}}"#,
        )
        .unwrap();
    assert_eq!(baseline_calibration_id_from_tmpfs(&tmpfs), Some(12));

    std::fs::remove_file(tmpfs.calibration_json()).unwrap();
    std::fs::write(tmpfs.baseline_id_file(), "13\n").unwrap();
    assert_eq!(baseline_calibration_id_from_tmpfs(&tmpfs), Some(13));
}

fn test_config(tmp: &TempDir) -> LibvirtConfig {
    let p = tmp.path();
    LibvirtConfig {
        vm: VmConfig {
            golden_image: p.join("golden.qcow2"),
            boot_disk_gib: 64,
            network: "sandbox-egress".into(),
            // Tight intervals so the test driver doesn't sleep for
            // multiple seconds between poll iterations.
            poll_interval_secs: 1,
            heartbeat_interval_secs: 60,
        },
        benchmark: BenchmarkProfile {
            build_vcpus: 4,
            bench_vcpus: 2,
            build_memory_bytes: 16 * 1024 * 1024 * 1024,
            bench_memory_bytes: 8 * 1024 * 1024 * 1024,
            // Big enough that we never reach the timeout in the test.
            job_timeout_secs: 30,
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
        service_user: "sbgh".into(),
        host_cpus: None,
        block_validation: None,
    }
}

fn enable_block_profile(config: &mut LibvirtConfig) {
    config.block_validation = Some(BlockValidationProfile {
        vcpus: 4,
        memory_bytes: 8 * 1024 * 1024 * 1024,
        cpu_set: Some("0-3".into()),
        target_blocks_per_shard: 2,
        max_shards: 4,
        max_concurrency: 4,
        max_parallel_jobs: 1,
        results_tmpfs_mib: 512,
        snapshot_prefix: "sbgh-block".into(),
        mount_options: vec!["nouuid".into()],
    });
}

fn prepare_sandbox_preflight_files(config: &mut LibvirtConfig, tmp: &TempDir) {
    use std::os::unix::fs::PermissionsExt;

    std::fs::write(&config.vm.golden_image, b"golden").unwrap();
    for (name, path) in [
        ("sudo", &mut config.paths.sudo_binary),
        ("virsh", &mut config.paths.virsh_binary),
        ("qemu-img", &mut config.paths.qemu_img_binary),
        (
            "cloud-localds",
            &mut config
                .paths
                .cloud_localds_binary,
        ),
        ("git", &mut config.paths.git_binary),
    ] {
        *path = tmp.path().join(name);
        std::fs::write(&*path, b"#!/bin/sh\nexit 0\n").unwrap();
        std::fs::set_permissions(&*path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
}

#[tokio::test]
async fn benchmark_preflight_proves_shared_sandbox_and_immutable_origin() {
    let tmp = TempDir::new().unwrap();
    let mut config = test_config(&tmp);
    prepare_sandbox_preflight_files(&mut config, &tmp);
    let shell = RecordingShell::new();
    shell
        .expect_ok(1) // qemu-img info
        .expect_ok(1) // virsh net-info
        .expect_ok(1) // root-owned sandbox policy check
        .reply(PreparedReply::with_stdout("mainnet-2026-05-21|Vri---tz-k\n"))
        .reply(PreparedReply::with_stdout("10.0|5.0\n"));
    let shell = Arc::new(shell);
    let driver = test_driver(&config, shell.clone());

    driver
        .preflight_benchmark()
        .await
        .unwrap();

    assert!(config.paths.jobs_dir.is_dir());
    let calls = shell.calls();
    assert_eq!(calls.len(), 5);
    assert!(
        calls[1]
            .args
            .iter()
            .any(|arg| arg == "sandbox-egress")
    );
    assert!(
        calls[3]
            .args
            .iter()
            .any(|arg| arg == "lv_name,lv_attr")
    );
    assert_eq!(calls[2].program, "/usr/local/libexec/sbgh-check-sandbox-network");
    assert!(calls[2].privileged);
}

#[tokio::test]
async fn sandbox_preflight_rejects_an_unmanaged_network_before_host_commands() {
    let tmp = TempDir::new().unwrap();
    let mut config = test_config(&tmp);
    config.vm.network = "renamed-default".into();
    let shell = Arc::new(RecordingShell::new());
    let driver = test_driver(&config, shell.clone());

    let error = driver
        .preflight_benchmark()
        .await
        .unwrap_err();

    assert!(format!("{error:#}").contains("policy-managed `sandbox-egress`"));
    assert!(shell.calls().is_empty());
}

#[tokio::test]
async fn block_validation_preflight_proves_fixed_sandbox_dependencies() {
    let tmp = TempDir::new().unwrap();
    let mut config = test_config(&tmp);
    enable_block_profile(&mut config);
    prepare_sandbox_preflight_files(&mut config, &tmp);
    let shell = RecordingShell::new();
    shell
        .expect_ok(1) // qemu-img info
        .expect_ok(1) // virsh net-info
        .expect_ok(1) // root-owned sandbox policy check
        .reply(PreparedReply::with_stdout("mainnet-origin|Vri---tz-k\n"))
        .reply(PreparedReply::with_stdout("10.0|5.0\n"));
    let shell = Arc::new(shell);
    let driver = test_driver(&config, shell.clone());
    driver
        .preflight_block_validation()
        .await
        .unwrap();

    assert!(config.paths.jobs_dir.is_dir());
    assert!(
        config
            .paths
            .results_tmpfs_root
            .is_dir()
    );
    assert!(
        config
            .paths
            .results_archive_dir
            .is_dir()
    );
    let calls = shell.calls();
    assert!(
        calls[0]
            .args
            .iter()
            .any(|arg| arg == "info")
    );
    assert!(
        calls[1]
            .args
            .iter()
            .any(|arg| arg == "net-info")
            && calls[1]
                .args
                .iter()
                .any(|arg| arg == "sandbox-egress")
    );
    assert!(calls[1].privileged);
    assert_eq!(calls[2].program, "/usr/local/libexec/sbgh-check-sandbox-network");
    assert!(calls[2].privileged);
}

#[tokio::test]
async fn block_validation_cache_hit_runs_in_one_vm_and_returns_typed_output() {
    let tmp = TempDir::new().unwrap();
    let mut config = test_config(&tmp);
    enable_block_profile(&mut config);
    std::fs::write(&config.vm.golden_image, b"golden").unwrap();
    create_bare_mirror(&config.paths.git_mirror);

    let job = fake_job();
    let toolchain = "[toolchain]\nchannel = \"1.95.0\"\n";
    let fingerprint = BuildFingerprint {
        artifact: BuildArtifact::StacksInspect,
        repository: Some(job.repository.clone()),
        commit: job.commit.clone(),
        toolchain: "1.95.0".into(),
        profile: BUILD_PROFILE.into(),
        features: String::new(),
        rustflags: String::new(),
        target_triple: BUILD_TARGET_TRIPLE.into(),
        recipe_version: BLOCK_BUILD_RECIPE_VERSION,
        image_id: fingerprint::image_proxy_id(&config.vm.golden_image).unwrap(),
        protocol_version: BUILD_PROTOCOL_VERSION.into(),
    };
    let cached_binary = tmp
        .path()
        .join("cached-stacks-inspect");
    std::fs::write(&cached_binary, b"CACHED STACKS INSPECT").unwrap();
    let cache = Arc::new(TestCache::default());
    cache
        .publish(&fingerprint, &cached_binary, 1, false)
        .unwrap();

    let results = config
        .paths
        .results_tmpfs_root
        .join(job.id.to_string());
    std::fs::create_dir_all(&results).unwrap();
    std::fs::write(
        results.join(".phase-log"),
        b"1700000000 starting\n\
          1700000001 build_cached\n\
          1700000002 probe\n\
          1700000003 validating\n\
          1700000004 reduced\n\
          1700000005 done\n",
    )
    .unwrap();
    std::fs::write(
        results.join("run.progress.jsonl"),
        br#"{"schema_version":1,"event_type":"progress","event_version":1,"progress":{"phase":"validate","current":1,"total":2,"message":"checked 2 blocks"}}
{"schema_version":1,"event_type":"progress","event_version":1,"progress":{"phase":"validate","current":2,"total":2,"message":"checked 3 blocks"}}
"#,
    )
    .unwrap();
    for shard in 0..2 {
        std::fs::write(results.join(format!("shard-{shard}.stdout.log")), b"").unwrap();
        std::fs::write(results.join(format!("shard-{shard}.stderr.log")), b"").unwrap();
    }
    std::fs::write(
        results.join("block-validation-result.json"),
        serde_json::to_vec(&serde_json::json!({
            "schema_version": 1,
            "job_id": job.id,
            "attempt_id": job.id,
            "fencing_generation": 0,
            "chainstate_origin": "sbgh-vg/mainnet-origin",
            "selection": {"kind": "range", "range": {"start": 10, "end": 12}},
            "observed": {"pre_nakamoto_count": 101, "nakamoto_count": 1},
            "resolved_range": {"start": 10, "end": 12},
            "segments": [{
                "epoch": "pre_nakamoto",
                "global_range": {"start": 10, "end": 12},
                "local_range": {"start": 10, "end": 12}
            }],
            "shard_count": 2,
            "max_concurrency": 2,
            "shards": [
                {
                    "index": 0,
                    "start": 10,
                    "end": 11,
                    "exit_code": 0,
                    "stdout_file": "shard-0.stdout.log",
                    "stderr_file": "shard-0.stderr.log",
                },
                {
                    "index": 1,
                    "start": 12,
                    "end": 12,
                    "exit_code": 0,
                    "stdout_file": "shard-1.stdout.log",
                    "stderr_file": "shard-1.stderr.log",
                },
            ],
        }))
        .unwrap(),
    )
    .unwrap();

    let shell = RecordingShell::new();
    shell
        .expect_ok(1) // git fetch exact commit into the existing mirror
        .reply(PreparedReply::with_stdout(toolchain)) // fingerprint toolchain
        .expect_ok(1) // qemu-img boot overlay
        .expect_ok(1) // truncate minimal source disk
        .expect_ok(1) // mkfs.ext4
        .reply(PreparedReply::with_stdout("/dev/loop9\n")) // losetup
        .expect_ok(1) // mount source disk
        .expect_ok(1) // chown empty source disk
        .expect_ok(1) // chown cached-binary tree to root
        .expect_ok(1) // unmount source disk
        .expect_ok(1) // detach source loop
        .reply(PreparedReply::with_stdout("mainnet-origin|Vri---tz-k\n")) // latest immutable origin
        .reply(PreparedReply::with_stdout("10.0|5.0\n")) // pool health
        .expect_ok(2) // K=2 thin snapshots
        .expect_ok(1) // results tmpfs
        .expect_ok(1) // block-validation cidata
        .expect_ok(1) // virsh define
        .expect_ok(1) // virsh start
        .reply(PreparedReply::with_stdout("shut off\n")) // phase=done + clean poweroff
        .expect_ok(1) // virsh destroy
        .expect_ok(1) // virsh undefine
        .expect_ok(1) // results tmpfs unmount
        .expect_ok(2) // reverse-order K snapshot cleanup
        .expect_ok(1); // git ref prune
    let shell = Arc::new(shell);
    let driver = test_driver_with_cache(&config, shell.clone(), Some(cache));
    let listener = RecordingListener::default();
    let spec = BlockValidationTaskSpec {
        selection: sbgh_driver::BlockValidationSelection::Range {
            range: InclusiveRange { start: 10, end: 12 },
        },
        timeout_secs: 30,
    };

    let outcome = driver
        .run_block_validation(&ctx_of(&job), &spec, &listener, &CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(outcome.status, DriverStatus::Completed);
    let DriverTaskOutput::BlockValidation(output) = outcome.output else {
        panic!("expected typed block-validation output");
    };
    assert!(output.valid);
    assert_eq!(output.checked_blocks, 3);
    assert!(
        output
            .invalid_blocks
            .is_empty()
    );
    assert_eq!(output.chainstate_origin, "sbgh-vg/mainnet-origin");
    assert_eq!(
        output.observed,
        sbgh_driver::ObservedValidationIndex {
            pre_nakamoto_count: 101,
            nakamoto_count: 1,
        }
    );
    assert_eq!(output.resolved_range, InclusiveRange { start: 10, end: 12 });
    assert_eq!(outcome.summary["chainstate_origin"], "sbgh-vg/mainnet-origin");
    assert_eq!(
        *listener
            .progresses
            .lock()
            .unwrap(),
        vec![
            ProgressUpdate {
                workflow_step: WorkflowStep::Run,
                run_index: 0,
                requested_run_count: 1,
                phase: "validate".into(),
                progress: 1,
                total: Some(2),
                message: Some("checked 2 blocks".into()),
            },
            ProgressUpdate {
                workflow_step: WorkflowStep::Run,
                run_index: 0,
                requested_run_count: 1,
                phase: "validate".into(),
                progress: 2,
                total: Some(2),
                message: Some("checked 3 blocks".into()),
            },
        ]
    );

    let calls = shell.calls();
    assert_eq!(
        calls
            .iter()
            .filter(|call| call
                .program
                .ends_with("lvcreate"))
            .count(),
        2,
    );
    assert!(
        calls
            .iter()
            .filter(|call| call
                .program
                .ends_with("lvcreate"))
            .all(|call| call
                .args
                .iter()
                .any(|arg| arg == "sbgh-vg/mainnet-origin"))
    );
    assert!(
        calls.iter().all(|call| !call
            .program
            .ends_with("cargo")
            && !call
                .program
                .ends_with("stacks-inspect")),
        "repository build and executable must never run on the host"
    );
    assert!(
        !calls.iter().any(|call| {
            call.program.ends_with("git")
                && call
                    .args
                    .iter()
                    .any(|arg| arg == "clone" || arg == "checkout")
        }),
        "cache hit must not provision a source checkout"
    );
    let removed: Vec<_> = calls
        .iter()
        .filter(|call| {
            call.program
                .ends_with("lvremove")
        })
        .flat_map(|call| call.args.iter())
        .filter(|arg| arg.starts_with("sbgh-vg/sbgh-block-"))
        .cloned()
        .collect();
    assert_eq!(removed.len(), 2);
    assert!(removed[0].ends_with("-s0001"));
    assert!(removed[1].ends_with("-s0000"));

    for name in [
        "shard-0.stdout.log",
        "shard-0.stderr.log",
        "shard-1.stdout.log",
        "shard-1.stderr.log",
        "block-validation-result.json",
        "invalid-blocks.json",
    ] {
        assert!(
            config
                .paths
                .results_archive_dir
                .join(job.id.to_string())
                .join("block-validation")
                .join(name)
                .is_file(),
            "missing archived block-validation artifact {name}",
        );
    }
}

#[tokio::test]
async fn attempt_cleanup_refuses_to_address_a_newer_attempt_snapshot() {
    let tmp = TempDir::new().unwrap();
    let mut config = test_config(&tmp);
    enable_block_profile(&mut config);
    let shell = Arc::new(RecordingShell::new());
    shell.reply(PreparedReply::with_stdout(
        "sbgh-block-job-old-g1-s0000\nsbgh-block-job-new-g2-s0000\n",
    ));
    shell.expect_ok(1);
    let driver = test_driver(&config, shell.clone());

    assert!(
        !driver
            .cleanup_block_snapshot_set("job", "old")
            .await,
        "unexpected newer-attempt row must fail cleanup closed"
    );
    let calls = shell.calls();
    let removes: Vec<_> = calls
        .iter()
        .filter(|call| {
            call.program
                .ends_with("lvremove")
        })
        .collect();
    assert_eq!(removes.len(), 1);
    assert!(
        removes[0]
            .args
            .iter()
            .any(|arg| arg.contains("job-old"))
    );
    assert!(
        removes[0]
            .args
            .iter()
            .all(|arg| !arg.contains("job-new"))
    );
}

#[tokio::test]
async fn attempt_cleanup_rejects_unsafe_identity_before_host_commands() {
    let tmp = TempDir::new().unwrap();
    let mut config = test_config(&tmp);
    enable_block_profile(&mut config);
    let shell = Arc::new(RecordingShell::new());
    let driver = test_driver(&config, shell.clone());

    assert!(
        !driver
            .cleanup_attempt("job", "../newer-attempt")
            .await
    );
    assert!(shell.calls().is_empty());
}

fn test_driver(config: &LibvirtConfig, shell: Arc<dyn Shell>) -> LibvirtDriver {
    test_driver_with_cache(config, shell, None)
}

fn test_driver_with_cache(
    config: &LibvirtConfig,
    shell: Arc<dyn Shell>,
    cache: Option<Arc<dyn BinaryCacheStore>>,
) -> LibvirtDriver {
    LibvirtDriver::new(
        config.clone(),
        shell,
        Arc::new(LocalArtifactSink {
            root: config
                .paths
                .results_archive_dir
                .clone(),
        }),
        cache,
    )
}

fn task_spec(args: Vec<String>, build_only: bool) -> TaskSpec {
    if build_only {
        TaskSpec::BuildOnly
    } else {
        TaskSpec::Benchmark(BenchmarkTaskSpec {
            args,
            sqlite_seed_key: None,
            shared_baseline_calibration: false,
            baseline_calibration_id: None,
            benchmark_run: Default::default(),
        })
    }
}

fn fake_job() -> TestJob {
    TestJob {
        id: Uuid::new_v4(),
        repository: "acme/widgets".into(),
        commit: "abc123def456".into(),
        bench_args: vec!["--iters=2".into()],
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
        .reply(PreparedReply::with_stdout("mainnet-2026-05-21|Vri---tz-k\n")) // immutable origin
        .reply(PreparedReply::with_stdout("10|5\n")) // shared pool health
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
        .reply(PreparedReply::with_stdout("mainnet-2026-05-21|Vri---tz-k\n")) // immutable origin
        .reply(PreparedReply::with_stdout("10|5\n")) // shared pool health
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

    create_bare_mirror(&cfg.paths.git_mirror);
    let tmpfs_dir = cfg
        .paths
        .results_tmpfs_root
        .join(job.id.to_string());
    std::fs::create_dir_all(&tmpfs_dir).unwrap();
    // Build-only: only the build VM runs, so seed `build_done` (no `done`).
    std::fs::write(tmpfs_dir.join(".phase-log"), b"1700000000 build_done\n").unwrap();

    let shell = Arc::new(build_only_shell());
    let driver = test_driver(&cfg, shell.clone());
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
    assert!(outcome.summary["run_json_archived_path"].is_null(), "build-only produces no run.json",);

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
        "mount",
        "cloud-localds", // build ISO
        "cloud-localds", // bench ISO (still provisioned)
        "virsh",         // define (build)
        "virsh",         // start (build)
        "virsh",         // domstate poll → ShutOff after BuildDone
        // no bench define/start/poll
        "virsh",  // destroy
        "virsh",  // undefine
        "umount", // tmpfs
        "git",    // mirror prune
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
    create_bare_mirror(&cfg.paths.git_mirror);
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
    std::fs::write(
            tmpfs_dir.join("run.progress.jsonl"),
            br#"{"schema_version":1,"event_type":"progress","event_version":1,"progress":{"phase":"replay","progress":42,"total":100,"message":"Replaying measured entries"}}"#,
        )
        .unwrap();

    let shell = Arc::new(happy_path_shell());
    let driver = test_driver(&cfg, shell.clone());
    let listener = RecordingListener::default();
    let outcome = driver
        .run_benchmark(
            &ctx_of(&job),
            &task_spec(job.bench_args.clone(), false),
            &listener,
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
    assert_eq!(outcome.summary["chainstate_origin"], "sbgh-vg/mainnet-2026-05-21");
    assert_eq!(
        outcome.summary["run_progress_archived_path"]
            .as_str()
            .unwrap(),
        format!("{}/{}", job.id, forensics::RUN_PROGRESS_JSONL_RELATIVE)
    );
    assert_eq!(
        std::fs::read_to_string(
            cfg.paths
                .results_archive_dir
                .join(
                    outcome.summary["run_progress_archived_path"]
                        .as_str()
                        .unwrap()
                )
        )
        .unwrap(),
        r#"{"schema_version":1,"event_type":"progress","event_version":1,"progress":{"phase":"replay","progress":42,"total":100,"message":"Replaying measured entries"}}"#
    );
    assert_eq!(
        *listener
            .progresses
            .lock()
            .unwrap(),
        vec![ProgressUpdate {
            workflow_step: WorkflowStep::Run,
            run_index: 0,
            requested_run_count: 1,
            phase: "replay".into(),
            progress: 42,
            total: Some(100),
            message: Some("Replaying measured entries".into()),
        }]
    );

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
        "lvs",           // shared thin-pool health
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
        assert_eq!(c.privileged, needs_priv, "privilege mismatch at index {i} ({})", programs[i]);
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

    create_bare_mirror(&cfg.paths.git_mirror);
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
        .reply(PreparedReply::with_stdout("mainnet-2026-05-21|Vri---tz-k\n")) // immutable origin
        .reply(PreparedReply::with_stdout("10|5\n")) // shared pool health
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

    let driver = test_driver(&cfg, shell.clone());
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

    create_bare_mirror(&cfg.paths.git_mirror);
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
    let driver = test_driver(&cfg, shell.clone());
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
    create_bare_mirror(&cfg.paths.git_mirror);
    std::fs::create_dir_all(
        cfg.paths
            .results_tmpfs_root
            .join(job.id.to_string()),
    )
    .unwrap();

    let shell = Arc::new(happy_path_shell());
    let driver = test_driver(&cfg, shell.clone());
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
        .reply(PreparedReply::with_stdout("running\n")) // virsh domstate
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

    let driver = test_driver(&cfg, shell.clone());
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
            "virsh",    // domstate
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
        calls[6]
            .args
            .contains(&"-d".to_string())
            && calls[6]
                .args
                .contains(&"/dev/loop42".to_string()),
        "losetup -d must detach the device losetup -j surfaced",
    );
    // lvremove targets the job-id-named snapshot.
    assert!(
        calls[7]
            .args
            .iter()
            .any(|a| a == "sbgh-vg/sbgh-orphan-123-chainstate"),
        "lvremove must target the per-job snapshot",
    );
    // The job dir (and its source.raw) is gone.
    assert!(!job_dir.exists(), "job dir removed");
}

#[tokio::test]
async fn cleanup_preserves_every_dependency_when_domain_destroy_fails() {
    let tmp = TempDir::new().unwrap();
    let mut config = test_config(&tmp);
    enable_block_profile(&mut config);
    let cfg = Arc::new(config);
    let job_id = "orphan-live-domain";
    let job_dir = cfg
        .paths
        .jobs_dir
        .join(job_id);
    std::fs::create_dir_all(&job_dir).unwrap();
    std::fs::write(job_dir.join("source.raw"), b"raw").unwrap();

    let shell = Arc::new(RecordingShell::new());
    shell
        .reply(PreparedReply::with_stdout("running\n"))
        .reply(PreparedReply::fail("libvirt transport failed"));

    let driver = test_driver(&cfg, shell.clone());
    assert!(
        !driver
            .cleanup_attempt("job", job_id)
            .await
    );
    assert!(
        job_dir
            .join("source.raw")
            .exists()
    );
    let calls = shell.calls();
    assert_eq!(calls.len(), 2, "cleanup must stop after failed destroy");
    assert!(
        calls[0]
            .args
            .contains(&"domstate".into())
    );
    assert!(
        calls[1]
            .args
            .contains(&"destroy".into())
    );
    assert!(
        calls.iter().all(|call| {
            !call
                .program
                .ends_with("lvremove")
                && !call.program.ends_with("lvs")
        }),
        "a live domain's snapshots must not even be enumerated for removal"
    );
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
        .reply(PreparedReply::with_stdout("running\n")) // virsh domstate
        .expect_ok(1) // virsh destroy
        .expect_ok(1) // virsh undefine
        .expect_ok(1) // umount results tmpfs
        .expect_ok(1) // umount source.mnt
        .reply(PreparedReply::with_stdout("")) // losetup -j → nothing attached
        .expect_ok(1) // lvremove
        .expect_ok(1); // git update-ref -d (prune)

    let driver = test_driver(&cfg, shell.clone());
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
        .reply(PreparedReply::with_stdout("running\n")) // virsh domstate
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

    let driver = test_driver(&cfg, shell.clone());
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
        .reply(PreparedReply::with_stdout("running\n")) // virsh domstate
        .expect_ok(1) // virsh destroy
        .expect_ok(1) // virsh undefine
        .expect_ok(1) // umount tmpfs
        .expect_ok(1) // umount source.mnt
        .reply(PreparedReply::fail("losetup: cannot read /dev: permission denied")) // -j non-zero, empty stdout
        .expect_ok(1) // lvremove
        .expect_ok(1); // git prune

    let driver = test_driver(&cfg, shell.clone());
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
