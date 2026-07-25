//! Per-job boot disk: a qcow2 overlay backed by the golden image.
//!
//! `qemu-img create -f qcow2 -F qcow2 -b <golden> <dest> <size>` creates a
//! COW overlay; writes from the VM accumulate in the overlay and the golden
//! image is never modified.

use std::path::{Path, PathBuf};

use crate::{PathsConfig, VmConfig};

use crate::libvirt::shell::{Shell, check, spec};

pub struct BootDisk {
    pub path: PathBuf,
}

impl BootDisk {
    pub async fn provision(
        shell: &dyn Shell,
        paths: &PathsConfig,
        vm: &VmConfig,
        job_dir: &Path,
    ) -> anyhow::Result<Self> {
        let dest = job_dir.join("boot.qcow2");
        let size = format!("{}G", vm.boot_disk_gib);
        let golden = vm
            .golden_image
            .display()
            .to_string();

        let out = shell
            .run(spec(
                &paths.qemu_img_binary,
                &[
                    "create",
                    "-f",
                    "qcow2",
                    "-F",
                    "qcow2",
                    "-b",
                    &golden,
                    &dest.display().to_string(),
                    &size,
                ],
            ))
            .await?;
        check(&out, &format!("qemu-img create {}", dest.display()))?;

        Ok(Self { path: dest })
    }

    pub fn teardown(self) -> std::io::Result<()> {
        match std::fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::{PathsConfig, VmConfig};

    use super::*;
    use crate::libvirt::shell::test_support::RecordingShell;

    fn cfgs() -> (PathsConfig, VmConfig) {
        (
            PathsConfig {
                jobs_dir: "/tmp/jobs".into(),
                git_mirror: "/tmp/git".into(),
                results_tmpfs_root: "/run/sbgh".into(),
                results_archive_dir: "/var/lib/sbgh/results".into(),
                sccache_dir: "/var/lib/sbgh/sccache".into(),
                virsh_binary: "/usr/bin/virsh".into(),
                sudo_binary: "/usr/bin/sudo".into(),
                qemu_img_binary: "/usr/bin/qemu-img".into(),
                cloud_localds_binary: "/usr/bin/cloud-localds".into(),
                git_binary: "/usr/bin/git".into(),
            },
            VmConfig {
                golden_image: PathBuf::from("/var/lib/libvirt/images/golden.qcow2"),
                build_vcpus: 4,
                bench_vcpus: 2,
                build_memory_bytes: 16 * 1024 * 1024 * 1024,
                bench_memory_bytes: 8 * 1024 * 1024 * 1024,
                boot_disk_gib: 64,
                job_timeout_secs: 60,
                network: "default".into(),
                poll_interval_secs: 5,
                heartbeat_interval_secs: 60,
            },
        )
    }

    #[tokio::test]
    async fn invokes_qemu_img_with_backing_file() {
        let (paths, vm) = cfgs();
        let shell = RecordingShell::new();
        shell.expect_ok(1);
        let job_dir = std::path::Path::new("/tmp/jobs/job1");
        let disk = BootDisk::provision(&shell, &paths, &vm, job_dir)
            .await
            .unwrap();
        assert_eq!(disk.path, PathBuf::from("/tmp/jobs/job1/boot.qcow2"));

        let calls = shell.calls();
        assert_eq!(calls.len(), 1);
        let c = &calls[0];
        assert!(!c.privileged);
        assert!(
            c.program
                .ends_with("qemu-img")
        );
        assert!(
            c.args
                .iter()
                .any(|a| a == "qcow2")
        );
        assert!(
            c.args
                .iter()
                .any(|a| a == "/var/lib/libvirt/images/golden.qcow2")
        );
        assert!(
            c.args
                .iter()
                .any(|a| a == "/tmp/jobs/job1/boot.qcow2")
        );
        assert!(
            c.args
                .iter()
                .any(|a| a == "64G")
        );
    }
}
