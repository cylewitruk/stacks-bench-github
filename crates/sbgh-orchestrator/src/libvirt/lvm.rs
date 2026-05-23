//! LVM thin-snapshot management for the per-job chainstate disk.
//!
//! Workflow:
//!   1. Pick the latest base LV matching `chainstate_base_prefix` (e.g.
//!      `mainnet-`).
//!   2. `lvcreate --snapshot --name sbgh-<job-id>-chainstate <vg>/<base>`
//!   3. Attach `/dev/<vg>/sbgh-<job-id>-chainstate` to the VM as `vdb`.
//!   4. On job teardown: `lvremove --force <vg>/sbgh-<job-id>-chainstate`.
//!
//! Base LV discovery uses `lvs` (machine-readable output) and picks the
//! lexicographically largest name matching the prefix — works because we
//! agreed on ISO date suffixes like `mainnet-2026-05-21`.

use std::path::PathBuf;

use sbgh_core::config::LvmConfig;

use crate::libvirt::shell::{Shell, check, spec_priv};

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
    use sbgh_core::config::LvmConfig;

    use super::*;
    use crate::libvirt::shell::test_support::{PreparedReply, RecordingShell};

    fn cfg() -> LvmConfig {
        LvmConfig {
            vg_name: "sbgh-vg".into(),
            thinpool: "thinpool".into(),
            chainstate_base_prefix: "mainnet-".into(),
            // Default for new deployments: thin snapshot, no -L.
            chainstate_snapshot_size_gib: None,
        }
    }

    #[tokio::test]
    async fn thin_snapshot_omits_size_flag() {
        // Default path: thin pool origin → lvcreate must NOT receive `-L`.
        let shell = RecordingShell::new();
        shell.reply(PreparedReply::with_stdout(
            "  mainnet-2026-05-20\n  mainnet-2026-05-21\n  mainnet-2026-04-30\n",
        ));
        shell.expect_ok(1); // lvcreate

        let snap = ChainstateSnapshot::provision(&shell, &cfg(), "job123")
            .await
            .unwrap();
        assert_eq!(snap.name, "sbgh-job123-chainstate");
        assert_eq!(snap.device, PathBuf::from("/dev/sbgh-vg/sbgh-job123-chainstate"));

        let calls = shell.calls();
        assert_eq!(calls.len(), 2);
        assert!(
            calls[0]
                .program
                .ends_with("lvs")
        );
        assert!(calls[0].privileged);
        assert!(
            calls[1]
                .program
                .ends_with("lvcreate")
        );
        assert!(
            calls[1]
                .args
                .contains(&"sbgh-job123-chainstate".to_string()),
            "expected snapshot name in args"
        );
        assert!(
            calls[1]
                .args
                .iter()
                .any(|a| a == "sbgh-vg/mainnet-2026-05-21"),
            "expected to target the latest base"
        );
        assert!(
            !calls[1]
                .args
                .iter()
                .any(|a| a == "-L"),
            "thin snapshot must NOT receive -L (got args: {:?})",
            calls[1].args
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
    async fn teardown_runs_lvremove() {
        let shell = RecordingShell::new();
        shell
            .reply(PreparedReply::with_stdout("  mainnet-2026-05-21\n"))
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
}
