//! LVM snapshot management for sandbox chainstate disks.
//!
//! Every workload resolves the newest local immutable read-only origin and
//! receives only explicit read-write attempt snapshots. Both paths share one
//! fixed near-full pool-health guard.

use std::path::PathBuf;

use crate::LvmConfig;
use anyhow::{Context, ensure};

use crate::libvirt::shell::{Shell, check, spec_priv};

#[derive(Debug)]
pub struct ChainstateSnapshot {
    pub origin: String,
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
        let origin = validate_latest_origin(shell, cfg).await?;
        let name = format!("sbgh-{job_id}-chainstate");

        // `-L` is included only when explicitly configured (thick snapshot
        // path). Against a thin-pool origin we omit it so lvcreate produces
        // a thin snapshot that lives in the same pool.
        let snapshot_size_arg = cfg
            .chainstate_snapshot_size_gib
            .map(|g| format!("{g}G"));
        create_read_write_snapshot(shell, &name, &origin, snapshot_size_arg.as_deref()).await?;

        Ok(Self {
            origin,
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

/// Validate the newest local immutable origin without allocating a snapshot.
pub async fn validate_latest_origin(shell: &dyn Shell, cfg: &LvmConfig) -> anyhow::Result<String> {
    let origin = resolve_latest_read_only_origin(shell, cfg).await?;
    if cfg
        .chainstate_snapshot_size_gib
        .is_none()
    {
        validate_thin_pool_health(shell, cfg).await?;
    }
    Ok(origin)
}

async fn create_read_write_snapshot(
    shell: &dyn Shell,
    name: &str,
    origin: &str,
    size: Option<&str>,
) -> anyhow::Result<()> {
    let mut args =
        vec!["--snapshot", "--permission", "rw", "--name", name, "--setactivationskip", "n"];
    if let Some(size) = size {
        args.push("-L");
        args.push(size);
    }
    args.push(origin);

    let out = shell
        .run(spec_priv(PathBuf::from("/usr/sbin/lvcreate").as_path(), &args))
        .await?;
    check(&out, &format!("lvcreate read-write snapshot of {origin}"))
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
    pub async fn provision_latest(
        shell: &dyn Shell,
        lvm: &LvmConfig,
        snapshot_prefix: &str,
        job_id: &str,
        attempt_id: &str,
        fencing_generation: u64,
        count: u32,
    ) -> anyhow::Result<Self> {
        ensure!(count > 0, "block validation requires at least one snapshot");
        let origin = validate_latest_origin(shell, lvm).await?;
        let mut set = Self {
            origin: origin.clone(),
            snapshots: Vec::with_capacity(count as usize),
        };
        for shard in 0..count {
            let name =
                snapshot_name(snapshot_prefix, job_id, attempt_id, fencing_generation, shard)?;
            let result = create_read_write_snapshot(shell, &name, &origin, None).await;
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

async fn resolve_latest_read_only_origin(
    shell: &dyn Shell,
    cfg: &LvmConfig,
) -> anyhow::Result<String> {
    let out = shell
        .run(spec_priv(
            std::path::Path::new("/usr/sbin/lvs"),
            &[
                "--noheadings",
                "--separator",
                "|",
                "--options",
                "lv_name,lv_attr",
                "--select",
                &format!("vg_name={} && lv_name=~^{}", cfg.vg_name, cfg.chainstate_base_prefix),
            ],
        ))
        .await?;
    check(&out, "lvs (listing chainstate bases)")?;

    let mut candidates: Vec<(String, String)> = parse_origin_rows(&out.stdout)?
        .into_iter()
        .filter(|(name, _)| name.starts_with(&cfg.chainstate_base_prefix))
        .collect();

    if candidates.is_empty() {
        anyhow::bail!(
            "no base chainstate LV found in VG {} matching prefix {:?}",
            cfg.vg_name,
            cfg.chainstate_base_prefix
        );
    }

    candidates.sort_by(|left, right| left.0.cmp(&right.0));
    let (name, attr) = candidates
        .pop()
        .expect("non-empty candidates were checked");
    ensure_read_only(&name, &attr)?;
    Ok(format!("{}/{}", cfg.vg_name, name))
}

fn parse_origin_rows(stdout: &[u8]) -> anyhow::Result<Vec<(String, String)>> {
    String::from_utf8(stdout.to_vec())?
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| {
            let columns: Vec<_> = line
                .split('|')
                .map(str::trim)
                .collect();
            ensure!(columns.len() == 2, "malformed lvs origin output");
            Ok((columns[0].to_string(), columns[1].to_string()))
        })
        .collect()
}

fn ensure_read_only(name: &str, attr: &str) -> anyhow::Result<()> {
    ensure!(attr.as_bytes().get(1) == Some(&b'r'), "chainstate origin LV {name} must be read-only");
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::LvmConfig;

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

    fn expect_valid_origin(shell: &RecordingShell, data_percent: f64, metadata_percent: f64) {
        shell.reply(PreparedReply::with_stdout("mainnet-full-g1|Vri---tz-k\n"));
        shell.reply(PreparedReply::with_stdout(format!("{data_percent}|{metadata_percent}\n")));
    }

    #[tokio::test]
    async fn thin_snapshot_omits_size_flag() {
        // Default path: thin pool origin → lvcreate must NOT receive `-L`.
        let shell = RecordingShell::new();
        shell.reply(PreparedReply::with_stdout(
            "mainnet-2026-05-20|Vri---tz-k\n\
             mainnet-2026-05-21|Vri---tz-k\n\
             mainnet-2026-04-30|Vri---tz-k\n",
        ));
        shell.reply(PreparedReply::with_stdout("10|5\n"));
        shell.expect_ok(1); // lvcreate

        let snap = ChainstateSnapshot::provision(&shell, &cfg(), "job123")
            .await
            .unwrap();
        assert_eq!(snap.name, "sbgh-job123-chainstate");
        assert_eq!(snap.origin, "sbgh-vg/mainnet-2026-05-21");
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
        assert!(
            calls[2]
                .args
                .windows(2)
                .any(|args| args == ["--permission", "rw"])
        );
    }

    #[tokio::test]
    async fn thick_snapshot_includes_size_flag() {
        // Opt-in: setting chainstate_snapshot_size_gib reverts to the classic
        // -L COW-size form for thick origins.
        let mut cfg = cfg();
        cfg.chainstate_snapshot_size_gib = Some(64);

        let shell = RecordingShell::new();
        shell.reply(PreparedReply::with_stdout("mainnet-2026-05-21|Vri---tz-k\n"));
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
        assert!(
            lvcreate_args
                .windows(2)
                .any(|args| args == ["--permission", "rw"])
        );
    }

    #[tokio::test]
    async fn errors_when_no_base_matches_prefix() {
        let shell = RecordingShell::new();
        shell.reply(PreparedReply::with_stdout("testnet-2026-05-21|Vri---tz-k\n"));
        let err = ChainstateSnapshot::provision(&shell, &cfg(), "job1")
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("no base chainstate")
        );
    }

    #[tokio::test]
    async fn benchmark_rejects_a_writable_latest_origin_before_allocation() {
        let shell = RecordingShell::new();
        shell.reply(PreparedReply::with_stdout(
            "mainnet-2026-05-20|Vri---tz-k\n\
             mainnet-2026-05-21|Vwi---tz-k\n",
        ));

        let error = ChainstateSnapshot::provision(&shell, &cfg(), "job1")
            .await
            .unwrap_err();

        assert!(format!("{error:#}").contains("must be read-only"));
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
    async fn benchmark_thin_snapshot_shares_the_near_full_pool_guard() {
        let shell = RecordingShell::new();
        shell.reply(PreparedReply::with_stdout("mainnet-2026-05-21|Vri---tz-k\n"));
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
            .reply(PreparedReply::with_stdout("mainnet-2026-05-21|Vri---tz-k\n"))
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
    async fn latest_origin_provisions_one_attempt_scoped_snapshot_per_shard() {
        let shell = RecordingShell::new();
        expect_valid_origin(&shell, 10.0, 5.0);
        shell.expect_ok(3);

        let set = ChainstateSnapshotSet::provision_latest(
            &shell,
            &cfg(),
            "sbgh-block",
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
        let shell = RecordingShell::new();
        expect_valid_origin(&shell, 10.0, 5.0);
        shell.expect_ok(1);
        shell.reply(PreparedReply::fail("allocation failed"));
        shell.expect_ok(1);

        let error = ChainstateSnapshotSet::provision_latest(
            &shell,
            &cfg(),
            "sbgh-block",
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
        let shell = RecordingShell::new();
        expect_valid_origin(&shell, 96.0, 5.0);

        let error = ChainstateSnapshotSet::provision_latest(
            &shell,
            &cfg(),
            "sbgh-block",
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
    async fn block_validation_rejects_a_writable_latest_origin() {
        let shell = RecordingShell::new();
        shell.reply(PreparedReply::with_stdout("mainnet-full-g1|Vwi---tz-k\n"));

        let error = ChainstateSnapshotSet::provision_latest(
            &shell,
            &cfg(),
            "sbgh-block",
            "job",
            "attempt",
            1,
            1,
        )
        .await
        .unwrap_err();

        assert!(format!("{error:#}").contains("must be read-only"));
        assert_eq!(shell.calls().len(), 1);
    }

    #[tokio::test]
    async fn fixed_metadata_floor_rejects_a_near_full_pool_independent_of_k() {
        let shell = RecordingShell::new();
        expect_valid_origin(&shell, 10.0, 96.0);

        let error = ChainstateSnapshotSet::provision_latest(
            &shell,
            &cfg(),
            "sbgh-block",
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
        let shell = RecordingShell::new();
        // 5.1% free is above the fixed 5% setup-health floor. Requesting
        // multiple shards must not manufacture an additional reserve.
        expect_valid_origin(&shell, 10.0, 94.9);
        shell.expect_ok(3);

        let set = ChainstateSnapshotSet::provision_latest(
            &shell,
            &cfg(),
            "sbgh-block",
            "job",
            "attempt",
            1,
            3,
        )
        .await
        .unwrap();

        assert_eq!(set.snapshots.len(), 3);
    }
}
