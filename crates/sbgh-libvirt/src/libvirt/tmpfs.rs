//! Per-job host tmpfs directory holding the results share (mounted into the
//! VM via virtio-fs). RAM-backed so the SQLite output is never written to
//! persistent storage from the VM side.

use std::path::{Path, PathBuf};

use crate::PathsConfig;

use crate::libvirt::shell::{Shell, check, spec_priv};

pub struct ResultsTmpfs {
    pub mount_dir: PathBuf,
    #[allow(dead_code)] // surfaced for diagnostics in chunk D
    pub size_mib: u32,
}

impl ResultsTmpfs {
    pub async fn mount(
        shell: &dyn Shell,
        paths: &PathsConfig,
        job_id: &str,
        size_mib: u32,
        service_user: &str,
    ) -> anyhow::Result<Self> {
        let mount_dir = paths
            .results_tmpfs_root
            .join(job_id);
        std::fs::create_dir_all(&mount_dir)?;
        let mount_s = mount_dir
            .display()
            .to_string();

        let out = shell
            .run(spec_priv(
                Path::new("/usr/bin/mount"),
                &[
                    "-t",
                    "tmpfs",
                    "-o",
                    &format!("size={size_mib}M,mode=0775,uid={service_user},gid={service_user}"),
                    "tmpfs",
                    &mount_s,
                ],
            ))
            .await?;
        check(&out, &format!("mount tmpfs at {mount_s}"))?;

        Ok(Self { mount_dir, size_mib })
    }

    pub async fn unmount(self, shell: &dyn Shell) -> anyhow::Result<()> {
        let mount_s = self
            .mount_dir
            .display()
            .to_string();
        let out = shell
            .run(spec_priv(Path::new("/usr/bin/umount"), &[&mount_s]))
            .await?;
        check(&out, &format!("umount tmpfs {mount_s}"))?;
        let _ = std::fs::remove_dir(&self.mount_dir);
        Ok(())
    }

    /// Path to the append-only phase journal the in-VM `sbgh-run.sh`
    /// writes to (one `<unix-seconds> <phase>` line per transition).
    /// The daemon's poll loop tails this; forensics archives it
    /// per-job for after-the-fact "what took so long" debugging.
    pub fn phase_log(&self) -> PathBuf {
        self.mount_dir
            .join(".phase-log")
    }

    /// Path to the SQLite database stacks-bench writes inside the results
    /// share. The in-VM template invokes `stacks-bench --db "$RESULTS" ...`;
    /// stacks-bench then materialises its store under
    /// `<--db>/appdata/stacks-bench.db`. We archive this file off after
    /// the VM shuts down.
    pub fn sqlite_file(&self) -> PathBuf {
        self.mount_dir
            .join("appdata")
            .join("stacks-bench.db")
    }

    /// Path to the stacks-bench binary snapshot the in-VM script copies
    /// here just before phase=done. We archive it next to the SQLite so
    /// the canonical reader of the DB schema is always paired with the
    /// data it produced.
    pub fn stacks_bench_binary(&self) -> PathBuf {
        self.binary("stacks-bench")
    }

    pub fn binary(&self, executable_name: &str) -> PathBuf {
        self.mount_dir
            .join(executable_name)
    }

    pub fn block_result_manifest(&self) -> PathBuf {
        self.mount_dir
            .join("block-validation-result.json")
    }

    pub fn chain_config(&self) -> PathBuf {
        self.mount_dir
            .join("chain-config.toml")
    }

    /// Path to the JSON stdout capture of `stacks-bench bench run --json`.
    /// Archived for use by the PR-comment summary builder + as a
    /// human-readable record of the run's high-level metrics.
    pub fn run_json(&self) -> PathBuf {
        self.mount_dir
            .join("run.json")
    }

    /// JSONL stderr progress from `stacks-bench bench run --json`.
    pub fn run_progress_jsonl(&self) -> PathBuf {
        self.mount_dir
            .join("run.progress.jsonl")
    }

    /// JSON stdout from `stacks-bench bench baseline calibrate --json`.
    pub fn calibration_json(&self) -> PathBuf {
        self.mount_dir
            .join("calibration.json")
    }

    /// JSONL stderr progress from `stacks-bench bench baseline calibrate
    /// --json`.
    pub fn calibration_progress_jsonl(&self) -> PathBuf {
        self.mount_dir
            .join("calibration.progress.jsonl")
    }

    /// Small handoff file written by the calibration phase and read by the
    /// measured run phase in the same repeat-submission job.
    pub fn baseline_id_file(&self) -> PathBuf {
        self.mount_dir
            .join("baseline-id")
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::libvirt::shell::test_support::RecordingShell;

    fn paths_in(dir: &TempDir) -> PathsConfig {
        PathsConfig {
            jobs_dir: dir.path().join("jobs"),
            git_mirror: dir.path().join("mirror.git"),
            results_tmpfs_root: dir.path().join("results"),
            results_archive_dir: dir.path().join("archive"),
            virsh_binary: "/usr/bin/virsh".into(),
            sudo_binary: "/usr/bin/sudo".into(),
            qemu_img_binary: "/usr/bin/qemu-img".into(),
            cloud_localds_binary: "/usr/bin/cloud-localds".into(),
            git_binary: "/usr/bin/git".into(),
        }
    }

    #[tokio::test]
    async fn mount_invokes_mount_with_size_and_user() {
        let tmp = TempDir::new().unwrap();
        let paths = paths_in(&tmp);
        let shell = RecordingShell::new();
        shell.expect_ok(1);

        let r = ResultsTmpfs::mount(&shell, &paths, "j1", 256, "sbgh")
            .await
            .unwrap();
        assert_eq!(
            r.mount_dir,
            paths
                .results_tmpfs_root
                .join("j1")
        );
        assert_eq!(r.phase_log(), r.mount_dir.join(".phase-log"));
        assert_eq!(
            r.run_progress_jsonl(),
            r.mount_dir
                .join("run.progress.jsonl")
        );
        assert_eq!(
            r.calibration_progress_jsonl(),
            r.mount_dir
                .join("calibration.progress.jsonl")
        );

        let calls = shell.calls();
        assert_eq!(calls.len(), 1);
        assert!(calls[0].privileged);
        assert!(
            calls[0]
                .args
                .contains(&"tmpfs".to_string())
        );
        assert!(
            calls[0]
                .args
                .iter()
                .any(|a| a.contains("size=256M"))
        );
        assert!(
            calls[0]
                .args
                .iter()
                .any(|a| a.contains("uid=sbgh"))
        );
    }
}
