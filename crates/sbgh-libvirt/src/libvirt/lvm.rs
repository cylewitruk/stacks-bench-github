//! LVM snapshot management for sandbox chainstate disks.
//!
//! Benchmarks retain their legacy latest-prefix single snapshot. Block
//! validation instead resolves one exact sealed generation and creates K
//! explicit read-write thin snapshots. Both paths share one fixed near-full
//! pool-health guard; it is deliberately not assignment-capacity prediction.

use std::path::PathBuf;

use anyhow::{Context, ensure};
use sbgh_driver::DatasetIdentity;

use crate::{BlockDatasetConfig, LvmConfig};

use crate::libvirt::shell::{Shell, check, spec, spec_priv};

const DATASET_MANIFEST: &str = ".sbgh-dataset-manifest.json";
const DATASET_FILE_DIGESTS: &str = ".sbgh-dataset-files.sha256";

#[derive(Debug)]
pub struct ChainstateSnapshot {
    pub vg: String,
    pub name: String,
    pub device: PathBuf,
}

impl ChainstateSnapshot {
    /// Provision a snapshot of the newest base LV matching the configured
    /// prefix.
    pub async fn provision(
        shell: &dyn Shell,
        cfg: &LvmConfig,
        job_id: &str,
    ) -> anyhow::Result<Self> {
        let base = pick_latest_base(shell, cfg).await?;
        let name = format!("sbgh-{job_id}-chainstate");
        let origin = format!("{}/{}", cfg.vg_name, base);

        // `-L` is included only when explicitly configured (thick snapshot
        // path). Against a thin-pool origin we omit it so lvcreate produces
        // a thin snapshot that lives in the same pool.
        let snapshot_size_arg = cfg
            .chainstate_snapshot_size_gib
            .map(|g| format!("{g}G"));
        if snapshot_size_arg.is_none() {
            validate_thin_pool_health(shell, cfg).await?;
        }
        let mut args: Vec<&str> = vec!["--snapshot", "--name", &name, "--setactivationskip", "n"];
        if let Some(ref s) = snapshot_size_arg {
            args.push("-L");
            args.push(s);
        }
        args.push(&origin);

        let out = shell
            .run(spec_priv(std::path::Path::new("/usr/sbin/lvcreate"), &args))
            .await?;
        check(&out, &format!("lvcreate snapshot of {origin}"))?;

        Ok(Self {
            vg: cfg.vg_name.clone(),
            name: name.clone(),
            device: PathBuf::from(format!("/dev/{}/{name}", cfg.vg_name)),
        })
    }

    pub async fn teardown(self, shell: &dyn Shell) -> anyhow::Result<()> {
        let target = format!("{}/{}", self.vg, self.name);
        let out = shell
            .run(spec_priv(std::path::Path::new("/usr/sbin/lvremove"), &["--force", &target]))
            .await?;
        check(&out, &format!("lvremove {target}"))
    }
}

/// One shard-addressable thin snapshot. Its name contains the full attempt
/// identity so cleanup for an older attempt cannot collide with a newer one.
#[derive(Debug)]
pub struct ShardSnapshot {
    pub shard: u32,
    pub vg: String,
    pub name: String,
    pub device: PathBuf,
    pub serial: String,
}

#[derive(Debug, Default)]
pub struct ChainstateSnapshotSet {
    pub origin: String,
    pub snapshots: Vec<ShardSnapshot>,
}

impl ChainstateSnapshotSet {
    #[allow(clippy::too_many_arguments)]
    pub async fn provision_exact(
        shell: &dyn Shell,
        lvm: &LvmConfig,
        dataset: &BlockDatasetConfig,
        identity: &DatasetIdentity,
        job_id: &str,
        attempt_id: &str,
        fencing_generation: u64,
        count: u32,
    ) -> anyhow::Result<Self> {
        ensure!(count > 0, "block validation requires at least one snapshot");
        validate_exact_generation(shell, lvm, dataset, identity).await?;
        let origin = format!("{}/{}", lvm.vg_name, dataset.origin_lv);
        let mut set = Self {
            origin: origin.clone(),
            snapshots: Vec::with_capacity(count as usize),
        };
        for shard in 0..count {
            let name = snapshot_name(
                &dataset.snapshot_prefix,
                job_id,
                attempt_id,
                fencing_generation,
                shard,
            )?;
            let out = shell
                .run(spec_priv(
                    PathBuf::from("/usr/sbin/lvcreate").as_path(),
                    &[
                        "--snapshot",
                        "--permission",
                        "rw",
                        "--name",
                        &name,
                        "--setactivationskip",
                        "n",
                        &origin,
                    ],
                ))
                .await;
            let result =
                out.and_then(|out| check(&out, &format!("lvcreate shard {shard} from {origin}")));
            if let Err(error) = result {
                set.teardown_best_effort(shell)
                    .await;
                return Err(error).context(format!(
                    "provisioning block-validation snapshot set failed at shard {shard}"
                ));
            }
            set.snapshots
                .push(ShardSnapshot {
                    shard,
                    vg: lvm.vg_name.clone(),
                    device: PathBuf::from(format!("/dev/{}/{name}", lvm.vg_name)),
                    serial: format!("sbgh-block-{shard:04}"),
                    name,
                });
        }
        Ok(set)
    }

    pub async fn teardown(self, shell: &dyn Shell) -> anyhow::Result<()> {
        let mut first = None;
        for snapshot in self
            .snapshots
            .into_iter()
            .rev()
        {
            if let Err(error) = remove_shard(shell, &snapshot).await
                && first.is_none()
            {
                first = Some(error);
            }
        }
        match first {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn teardown_best_effort(&mut self, shell: &dyn Shell) {
        while let Some(snapshot) = self.snapshots.pop() {
            if let Err(error) = remove_shard(shell, &snapshot).await {
                tracing::warn!(
                    %error,
                    snapshot = snapshot.name,
                    "failed to roll back partially provisioned snapshot set"
                );
            }
        }
    }
}

async fn remove_shard(shell: &dyn Shell, snapshot: &ShardSnapshot) -> anyhow::Result<()> {
    let target = format!("{}/{}", snapshot.vg, snapshot.name);
    let out = shell
        .run(spec_priv(PathBuf::from("/usr/sbin/lvremove").as_path(), &["--force", &target]))
        .await?;
    check(&out, &format!("lvremove {target}"))
}

fn snapshot_name(
    prefix: &str,
    job_id: &str,
    attempt_id: &str,
    fencing_generation: u64,
    shard: u32,
) -> anyhow::Result<String> {
    for (name, value) in [("prefix", prefix), ("job_id", job_id), ("attempt_id", attempt_id)] {
        ensure!(
            !value.is_empty()
                && value
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'_' | b'.' | b'-')),
            "invalid LVM snapshot {name}"
        );
    }
    let name = format!("{prefix}-{job_id}-{attempt_id}-g{fencing_generation}-s{shard:04}");
    ensure!(name.len() <= 127, "LVM snapshot name exceeds 127 bytes");
    Ok(name)
}

#[derive(Debug, Deserialize)]
struct DatasetManifest {
    generation: String,
    network: String,
    format_version: String,
    covered_start: u64,
    covered_end: u64,
    files_sha256: String,
}

use serde::Deserialize;

/// Verify that the configured generation is the payload's exact sealed origin
/// and that the shared thin pool is not already near full.
pub async fn validate_exact_generation(
    shell: &dyn Shell,
    lvm: &LvmConfig,
    dataset: &BlockDatasetConfig,
    identity: &DatasetIdentity,
) -> anyhow::Result<()> {
    ensure!(
        dataset.generation == identity.generation
            && dataset.network == identity.network
            && dataset.format_version == identity.format_version
            && dataset.covered_start == identity.covered_start
            && dataset.covered_end == identity.covered_end
            && dataset
                .manifest_sha256
                .eq_ignore_ascii_case(&identity.manifest_sha256),
        "assignment dataset identity does not match configured sealed generation"
    );
    ensure!(
        dataset
            .mount_options
            .iter()
            .any(|option| option == "nouuid"),
        "block dataset mount_options must include `nouuid` for XFS snapshots"
    );
    ensure!(
        dataset
            .manifest_path
            .file_name()
            .is_some_and(|name| name == DATASET_MANIFEST),
        "dataset manifest_path must name {DATASET_MANIFEST}"
    );

    let origin = format!("{}/{}", lvm.vg_name, dataset.origin_lv);
    let out = shell
        .run(spec_priv(
            PathBuf::from("/usr/sbin/lvs").as_path(),
            &["--noheadings", "--separator", "|", "--options", "lv_name,lv_attr,lv_tags", &origin],
        ))
        .await?;
    check(&out, &format!("lvs sealed origin {origin}"))?;
    let origin_output = String::from_utf8(out.stdout)?;
    let rows: Vec<_> = origin_output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect();
    ensure!(rows.len() == 1, "expected exactly one exact origin LV {origin}");
    let columns: Vec<_> = rows[0]
        .split('|')
        .map(str::trim)
        .collect();
    ensure!(columns.len() == 3, "malformed lvs origin output");
    ensure!(columns[0] == dataset.origin_lv, "lvs returned a different origin LV");
    ensure!(
        columns[1].as_bytes().get(1) == Some(&b'r'),
        "dataset origin LV must be read-only/sealed"
    );
    let tags: Vec<_> = columns[2]
        .split(',')
        .map(str::trim)
        .filter(|tag| !tag.is_empty())
        .collect();
    for tag in ["sbgh_sealed", "sbgh_validated"] {
        ensure!(tags.contains(&tag), "dataset origin is missing mandatory LVM tag `{tag}`");
    }
    for tag in &dataset.required_origin_tags {
        ensure!(tags.contains(&tag.as_str()), "dataset origin is missing required LVM tag `{tag}`");
    }

    let manifest_path = dataset
        .manifest_path
        .display()
        .to_string();
    let out = shell
        .run(spec(
            PathBuf::from("/usr/bin/findmnt").as_path(),
            &["--noheadings", "--output", "SOURCE,OPTIONS", "--target", &manifest_path],
        ))
        .await?;
    check(&out, "findmnt dataset manifest")?;
    let mount = String::from_utf8(out.stdout)?;
    let mut mount_fields = mount.split_whitespace();
    let mount_source = mount_fields
        .next()
        .context("findmnt returned no dataset mount source")?;
    let mount_options = mount_fields
        .next()
        .context("findmnt returned no dataset mount options")?;
    ensure!(
        mount_options
            .split(',')
            .any(|option| option == "ro"),
        "sealed dataset manifest must be read through a read-only origin mount"
    );
    let configured_device = format!("/dev/{}/{}", lvm.vg_name, dataset.origin_lv);
    let source_real = canonical_device(shell, mount_source).await?;
    let configured_real = canonical_device(shell, &configured_device).await?;
    ensure!(
        source_real == configured_real,
        "dataset manifest mount is not backed by the configured exact origin LV"
    );
    let digest = sha256_with_shell(shell, &dataset.manifest_path).await?;
    ensure!(
        digest.eq_ignore_ascii_case(&identity.manifest_sha256),
        "dataset manifest digest does not match assignment identity"
    );
    let manifest_bytes = std::fs::read(&dataset.manifest_path).with_context(|| {
        format!(
            "reading dataset manifest {}",
            dataset
                .manifest_path
                .display()
        )
    })?;
    let manifest: DatasetManifest =
        serde_json::from_slice(&manifest_bytes).context("parsing dataset generation manifest")?;
    ensure!(
        manifest.generation == identity.generation
            && manifest.network == identity.network
            && manifest.format_version == identity.format_version
            && manifest.covered_start == identity.covered_start
            && manifest.covered_end == identity.covered_end,
        "dataset manifest fields do not match assignment identity"
    );
    ensure!(
        manifest.files_sha256.len() == 64
            && manifest
                .files_sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit()),
        "dataset file-list digest is invalid"
    );
    let file_digests = dataset
        .manifest_path
        .with_file_name(DATASET_FILE_DIGESTS);
    ensure!(
        sha256_with_shell(shell, &file_digests)
            .await?
            .eq_ignore_ascii_case(&manifest.files_sha256),
        "dataset file list does not match the sealed manifest"
    );

    validate_thin_pool_health(shell, lvm).await
}

async fn sha256_with_shell(shell: &dyn Shell, path: &std::path::Path) -> anyhow::Result<String> {
    let path = path.display().to_string();
    let out = shell
        .run(spec(PathBuf::from("/usr/bin/sha256sum").as_path(), &[&path]))
        .await?;
    check(&out, &format!("sha256sum {path}"))?;
    out.stdout
        .split(|byte| byte.is_ascii_whitespace())
        .find(|field| !field.is_empty())
        .context("sha256sum returned no digest")
        .and_then(|digest| {
            String::from_utf8(digest.to_vec()).context("sha256sum digest is not UTF-8")
        })
}

async fn canonical_device(shell: &dyn Shell, device: &str) -> anyhow::Result<String> {
    let out = shell
        .run(spec(PathBuf::from("/usr/bin/readlink").as_path(), &["-f", device]))
        .await?;
    check(&out, &format!("readlink -f {device}"))?;
    let path = String::from_utf8(out.stdout)?
        .trim()
        .to_string();
    ensure!(!path.is_empty(), "readlink returned an empty device path");
    Ok(path)
}

async fn validate_thin_pool_health(shell: &dyn Shell, lvm: &LvmConfig) -> anyhow::Result<()> {
    lvm.validate_pool_health_policy()?;
    let pool = format!("{}/{}", lvm.vg_name, lvm.thinpool);
    let out = shell
        .run(spec_priv(
            PathBuf::from("/usr/sbin/lvs").as_path(),
            &[
                "--noheadings",
                "--units",
                "b",
                "--nosuffix",
                "--separator",
                "|",
                "--options",
                "data_percent,metadata_percent",
                &pool,
            ],
        ))
        .await?;
    check(&out, &format!("lvs thin-pool headroom {pool}"))?;
    let line = String::from_utf8(out.stdout)?
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .context("lvs returned no thin-pool health row")?
        .to_string();
    let columns: Vec<_> = line
        .split('|')
        .map(str::trim)
        .collect();
    ensure!(columns.len() == 2, "malformed lvs thin-pool output");
    let data_percent: f64 = columns[0]
        .parse()
        .context("invalid thin-pool data_percent")?;
    let metadata_percent: f64 = columns[1]
        .parse()
        .context("invalid thin-pool metadata_percent")?;
    let data_free = 100.0 - data_percent;
    let metadata_free = 100.0 - metadata_percent;
    ensure!(
        data_free >= lvm.min_data_free_percent,
        "thin-pool data headroom {data_free:.2}% is below configured {:.2}%",
        lvm.min_data_free_percent
    );
    ensure!(
        metadata_free >= lvm.min_metadata_free_percent,
        "thin-pool metadata headroom {metadata_free:.2}% is below configured {:.2}%",
        lvm.min_metadata_free_percent
    );
    Ok(())
}

async fn pick_latest_base(shell: &dyn Shell, cfg: &LvmConfig) -> anyhow::Result<String> {
    let out = shell
        .run(spec_priv(
            std::path::Path::new("/usr/sbin/lvs"),
            &[
                "--noheadings",
                "--options=lv_name",
                "--select",
                &format!("vg_name={} && lv_name=~^{}", cfg.vg_name, cfg.chainstate_base_prefix),
            ],
        ))
        .await?;
    check(&out, "lvs (listing chainstate bases)")?;

    let stdout = String::from_utf8(out.stdout)?;
    let mut candidates: Vec<String> = stdout
        .lines()
        .map(str::trim)
        .filter(|s| s.starts_with(&cfg.chainstate_base_prefix))
        .map(str::to_string)
        .collect();

    if candidates.is_empty() {
        anyhow::bail!(
            "no base chainstate LV found in VG {} matching prefix {:?}",
            cfg.vg_name,
            cfg.chainstate_base_prefix
        );
    }

    candidates.sort();
    Ok(candidates.pop().unwrap())
}

#[cfg(test)]
mod tests {
    use crate::{BlockDatasetConfig, LvmConfig};
    use tempfile::TempDir;

    use super::*;
    use crate::libvirt::shell::test_support::{PreparedReply, RecordingShell};

    fn cfg() -> LvmConfig {
        LvmConfig {
            vg_name: "sbgh-vg".into(),
            thinpool: "thinpool".into(),
            chainstate_base_prefix: "mainnet-".into(),
            // Default for new deployments: thin snapshot, no -L.
            chainstate_snapshot_size_gib: None,
            min_data_free_percent: 5.0,
            min_metadata_free_percent: 5.0,
        }
    }

    fn sealed_dataset(directory: &TempDir) -> (BlockDatasetConfig, DatasetIdentity) {
        let manifest_path = directory
            .path()
            .join(".sbgh-dataset-manifest.json");
        std::fs::write(
            &manifest_path,
            r#"{"generation":"mainnet-g1","network":"mainnet","format_version":"v3","covered_start":0,"covered_end":999,"files_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}"#,
        )
        .unwrap();
        let identity = DatasetIdentity {
            generation: "mainnet-g1".into(),
            network: "mainnet".into(),
            format_version: "v3".into(),
            covered_start: 0,
            covered_end: 999,
            manifest_sha256: "a".repeat(64),
        };
        (
            BlockDatasetConfig {
                generation: identity.generation.clone(),
                network: identity.network.clone(),
                format_version: identity
                    .format_version
                    .clone(),
                covered_start: identity.covered_start,
                covered_end: identity.covered_end,
                manifest_sha256: identity
                    .manifest_sha256
                    .clone(),
                origin_lv: "mainnet-full-g1".into(),
                manifest_path,
                snapshot_prefix: "sbgh-block".into(),
                mount_options: vec!["nouuid".into(), "noatime".into()],
                required_origin_tags: vec!["sbgh_sealed".into(), "sbgh_validated".into()],
            },
            identity,
        )
    }

    fn expect_valid_generation(shell: &RecordingShell, data_percent: f64, metadata_percent: f64) {
        shell.reply(PreparedReply::with_stdout(
            "mainnet-full-g1|Vri---tz-k|sbgh_sealed,sbgh_validated\n",
        ));
        shell.reply(PreparedReply::with_stdout(
            "/dev/mapper/sbgh--vg-mainnet--full--g1 ro,nouuid\n",
        ));
        shell.reply(PreparedReply::with_stdout("/dev/dm-7\n"));
        shell.reply(PreparedReply::with_stdout("/dev/dm-7\n"));
        shell.reply(PreparedReply::with_stdout(format!("{}  manifest\n", "a".repeat(64))));
        shell.reply(PreparedReply::with_stdout(format!("{}  file-list\n", "b".repeat(64))));
        shell.reply(PreparedReply::with_stdout(format!("{data_percent}|{metadata_percent}\n")));
    }

    #[tokio::test]
    async fn thin_snapshot_omits_size_flag() {
        // Default path: thin pool origin → lvcreate must NOT receive `-L`.
        let shell = RecordingShell::new();
        shell.reply(PreparedReply::with_stdout(
            "  mainnet-2026-05-20\n  mainnet-2026-05-21\n  mainnet-2026-04-30\n",
        ));
        shell.reply(PreparedReply::with_stdout("10|5\n"));
        shell.expect_ok(1); // lvcreate

        let snap = ChainstateSnapshot::provision(&shell, &cfg(), "job123")
            .await
            .unwrap();
        assert_eq!(snap.name, "sbgh-job123-chainstate");
        assert_eq!(snap.device, PathBuf::from("/dev/sbgh-vg/sbgh-job123-chainstate"));

        let calls = shell.calls();
        assert_eq!(calls.len(), 3);
        assert!(
            calls[0]
                .program
                .ends_with("lvs")
        );
        assert!(calls[0].privileged);
        assert!(
            calls[2]
                .program
                .ends_with("lvcreate")
        );
        assert!(
            calls[2]
                .args
                .contains(&"sbgh-job123-chainstate".to_string()),
            "expected snapshot name in args"
        );
        assert!(
            calls[2]
                .args
                .iter()
                .any(|a| a == "sbgh-vg/mainnet-2026-05-21"),
            "expected to target the latest base"
        );
        assert!(
            !calls[2]
                .args
                .iter()
                .any(|a| a == "-L"),
            "thin snapshot must NOT receive -L (got args: {:?})",
            calls[2].args
        );
    }

    #[tokio::test]
    async fn thick_snapshot_includes_size_flag() {
        // Opt-in: setting chainstate_snapshot_size_gib reverts to the classic
        // -L COW-size form for thick origins.
        let mut cfg = cfg();
        cfg.chainstate_snapshot_size_gib = Some(64);

        let shell = RecordingShell::new();
        shell.reply(PreparedReply::with_stdout("  mainnet-2026-05-21\n"));
        shell.expect_ok(1);

        ChainstateSnapshot::provision(&shell, &cfg, "job1")
            .await
            .unwrap();

        let calls = shell.calls();
        let lvcreate_args = &calls[1].args;
        let l_pos = lvcreate_args
            .iter()
            .position(|a| a == "-L")
            .expect("thick snapshot must include -L");
        assert_eq!(
            lvcreate_args
                .get(l_pos + 1)
                .map(String::as_str),
            Some("64G")
        );
    }

    #[tokio::test]
    async fn errors_when_no_base_matches_prefix() {
        let shell = RecordingShell::new();
        shell.reply(PreparedReply::with_stdout("  testnet-2026-05-21\n"));
        let err = ChainstateSnapshot::provision(&shell, &cfg(), "job1")
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("no base chainstate")
        );
    }

    #[tokio::test]
    async fn benchmark_thin_snapshot_shares_the_near_full_pool_guard() {
        let shell = RecordingShell::new();
        shell.reply(PreparedReply::with_stdout("  mainnet-2026-05-21\n"));
        shell.reply(PreparedReply::with_stdout("96|5\n"));

        let error = ChainstateSnapshot::provision(&shell, &cfg(), "job1")
            .await
            .unwrap_err();

        assert!(format!("{error:#}").contains("data headroom"));
        assert!(
            shell
                .calls()
                .iter()
                .all(|call| !call
                    .program
                    .ends_with("lvcreate"))
        );
    }

    #[tokio::test]
    async fn teardown_runs_lvremove() {
        let shell = RecordingShell::new();
        shell
            .reply(PreparedReply::with_stdout("  mainnet-2026-05-21\n"))
            .reply(PreparedReply::with_stdout("10|5\n"))
            .expect_ok(2); // lvcreate, lvremove

        let snap = ChainstateSnapshot::provision(&shell, &cfg(), "j")
            .await
            .unwrap();
        snap.teardown(&shell)
            .await
            .unwrap();

        let calls = shell.calls();
        assert!(
            calls
                .last()
                .unwrap()
                .program
                .ends_with("lvremove")
        );
        assert!(
            calls
                .last()
                .unwrap()
                .args
                .contains(&"sbgh-vg/sbgh-j-chainstate".into())
        );
    }

    #[tokio::test]
    async fn exact_generation_provisions_one_attempt_scoped_snapshot_per_shard() {
        let directory = TempDir::new().unwrap();
        let (dataset, identity) = sealed_dataset(&directory);
        let shell = RecordingShell::new();
        expect_valid_generation(&shell, 10.0, 5.0);
        shell.expect_ok(3);

        let set = ChainstateSnapshotSet::provision_exact(
            &shell,
            &cfg(),
            &dataset,
            &identity,
            "job",
            "attempt",
            9,
            3,
        )
        .await
        .unwrap();
        assert_eq!(set.origin, "sbgh-vg/mainnet-full-g1");
        assert_eq!(set.snapshots.len(), 3);
        assert_eq!(set.snapshots[1].serial, "sbgh-block-0001");
        assert!(
            set.snapshots
                .iter()
                .all(|snapshot| snapshot
                    .name
                    .contains("job-attempt-g9"))
        );
        let calls = shell.calls();
        let creates: Vec<_> = calls
            .iter()
            .filter(|call| {
                call.program
                    .ends_with("lvcreate")
            })
            .collect();
        assert_eq!(creates.len(), 3);
        assert!(creates.iter().all(|call| {
            call.args
                .contains(&"sbgh-vg/mainnet-full-g1".into())
        }));
        assert!(creates.iter().all(|call| {
            call.args
                .windows(2)
                .any(|args| args == ["--permission", "rw"])
        }));
    }

    #[tokio::test]
    async fn partial_snapshot_failure_removes_the_successful_prefix() {
        let directory = TempDir::new().unwrap();
        let (dataset, identity) = sealed_dataset(&directory);
        let shell = RecordingShell::new();
        expect_valid_generation(&shell, 10.0, 5.0);
        shell.expect_ok(1);
        shell.reply(PreparedReply::fail("allocation failed"));
        shell.expect_ok(1);

        let error = ChainstateSnapshotSet::provision_exact(
            &shell,
            &cfg(),
            &dataset,
            &identity,
            "job",
            "attempt",
            1,
            3,
        )
        .await
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("shard 1")
        );
        let calls = shell.calls();
        let last = calls.last().unwrap();
        assert!(
            last.program
                .ends_with("lvremove")
        );
        assert!(
            last.args
                .iter()
                .any(|arg| arg.contains("g1-s0000"))
        );
        assert!(
            !last
                .args
                .iter()
                .any(|arg| arg == "sbgh-vg/mainnet-full-g1")
        );
    }

    #[tokio::test]
    async fn thin_pool_headroom_fails_before_any_snapshot_is_created() {
        let directory = TempDir::new().unwrap();
        let (dataset, identity) = sealed_dataset(&directory);
        let shell = RecordingShell::new();
        expect_valid_generation(&shell, 96.0, 5.0);

        let error = ChainstateSnapshotSet::provision_exact(
            &shell,
            &cfg(),
            &dataset,
            &identity,
            "job",
            "attempt",
            1,
            2,
        )
        .await
        .unwrap_err();
        assert!(format!("{error:#}").contains("data headroom"));
        assert!(
            shell
                .calls()
                .iter()
                .all(|call| !call
                    .program
                    .ends_with("lvcreate"))
        );
    }

    #[tokio::test]
    async fn fixed_metadata_floor_rejects_a_near_full_pool_independent_of_k() {
        let directory = TempDir::new().unwrap();
        let (dataset, identity) = sealed_dataset(&directory);
        let shell = RecordingShell::new();
        expect_valid_generation(&shell, 10.0, 96.0);

        let error = ChainstateSnapshotSet::provision_exact(
            &shell,
            &cfg(),
            &dataset,
            &identity,
            "job",
            "attempt",
            1,
            3,
        )
        .await
        .unwrap_err();
        assert!(format!("{error:#}").contains("metadata headroom"));
        assert!(
            shell
                .calls()
                .iter()
                .all(|call| !call
                    .program
                    .ends_with("lvcreate"))
        );
    }

    #[tokio::test]
    async fn pool_health_floor_does_not_scale_with_shard_count() {
        let directory = TempDir::new().unwrap();
        let (dataset, identity) = sealed_dataset(&directory);
        let shell = RecordingShell::new();
        // 5.1% free is above the fixed 5% setup-health floor. Requesting
        // multiple shards must not manufacture an additional reserve.
        expect_valid_generation(&shell, 10.0, 94.9);
        shell.expect_ok(3);

        let set = ChainstateSnapshotSet::provision_exact(
            &shell,
            &cfg(),
            &dataset,
            &identity,
            "job",
            "attempt",
            1,
            3,
        )
        .await
        .unwrap();

        assert_eq!(set.snapshots.len(), 3);
    }

    #[tokio::test]
    async fn sealed_manifest_must_bind_the_dataset_file_list() {
        let directory = TempDir::new().unwrap();
        let (dataset, identity) = sealed_dataset(&directory);
        let shell = RecordingShell::new();
        shell.reply(PreparedReply::with_stdout(
            "mainnet-full-g1|Vri---tz-k|sbgh_sealed,sbgh_validated\n",
        ));
        shell.reply(PreparedReply::with_stdout(
            "/dev/mapper/sbgh--vg-mainnet--full--g1 ro,nouuid\n",
        ));
        shell.reply(PreparedReply::with_stdout("/dev/dm-7\n"));
        shell.reply(PreparedReply::with_stdout("/dev/dm-7\n"));
        shell.reply(PreparedReply::with_stdout(format!("{}  manifest\n", "a".repeat(64))));
        shell.reply(PreparedReply::with_stdout(format!("{}  file-list\n", "c".repeat(64))));

        let error = validate_exact_generation(&shell, &cfg(), &dataset, &identity)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("file list")
        );
        assert!(
            shell
                .calls()
                .iter()
                .all(|call| !call
                    .program
                    .ends_with("lvcreate"))
        );
    }
}
