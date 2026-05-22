//! Per-job host tmpfs directory holding the results share (mounted into the
//! VM via virtio-fs). RAM-backed so the SQLite output is never written to
//! persistent storage from the VM side.

use std::path::{Path, PathBuf};

use sbgh_core::config::PathsConfig;

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

    pub fn phase_file(&self) -> PathBuf {
        self.mount_dir.join(".phase")
    }

    #[allow(dead_code)] // consumed by the result archiver in chunk D
    pub fn sqlite_file(&self) -> PathBuf {
        self.mount_dir
            .join("run.sqlite")
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
        assert_eq!(r.phase_file(), r.mount_dir.join(".phase"));

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
