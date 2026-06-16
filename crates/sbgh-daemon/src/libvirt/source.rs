//! Per-job source disk: a sparse raw file, ext4-formatted on the host. The
//! normal path checks out `stacks-core` at the job SHA; the cache-hit path
//! creates a minimal disk containing only `target/release/stacks-bench`.
//! Attached to the VM as `vdc`.
//!
//! Steps:
//!   1. `truncate -s 8G <source.raw>` (host-writable)
//!   2. `mkfs.ext4 -F -L sbgh-src <source.raw>` (root)
//!   3. `losetup -fP --show <source.raw>` (root) — returns loop device
//!   4. `mount <loop> <mountpoint>` (root)
//!   5. `chown -R sbgh <mountpoint>` (root) — so user-owned git can write
//!   6. `rmdir <mountpoint>/lost+found` (user) — mke2fs always creates this;
//!      git clone refuses to clone into a non-empty dir
//!   7. `git clone --reference <mirror> --shared <mirror> <mountpoint>` (user)
//!   8. `git -C <mountpoint> checkout <sha>` (user)
//!   9. `umount <mountpoint>` (root)
//!  10. `losetup -d <loop>` (root)
//!
//! Cleanup invariant: once `losetup` attaches a loop device, the function MUST
//! detach it before returning, even on failure. Same for the mount: once
//! mounted, MUST be unmounted. Cleanup steps run best-effort and are logged
//! if they fail.
//!
//! At job teardown the raw file is deleted along with the rest of the job dir.

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use sbgh_core::config::PathsConfig;

use crate::libvirt::shell::{Shell, check, spec, spec_priv};

const SOURCE_SIZE: &str = "8G";

#[derive(Debug)]
pub struct SourceDisk {
    pub path: PathBuf,
}

impl SourceDisk {
    /// Build the source disk and clone stacks-core@sha into it.
    /// `service_user` is the OS user the daemon runs as; it'll own the
    /// mounted filesystem so the unprivileged `git clone` step can write.
    pub async fn provision(
        shell: &dyn Shell,
        paths: &PathsConfig,
        job_dir: &Path,
        mount_dir: &Path,
        sha: &str,
        service_user: &str,
    ) -> anyhow::Result<Self> {
        let raw = job_dir.join("source.raw");
        let raw_s = raw.display().to_string();
        std::fs::create_dir_all(mount_dir)?;

        // 1. sparse file (user-writable)
        let out = shell
            .run(spec(Path::new("/usr/bin/truncate"), &["-s", SOURCE_SIZE, &raw_s]))
            .await?;
        check(&out, &format!("truncate {raw_s}"))?;

        // 2. mkfs (root)
        let out = shell
            .run(spec_priv(Path::new("/usr/sbin/mkfs.ext4"), &["-F", "-L", "sbgh-src", &raw_s]))
            .await?;
        check(&out, &format!("mkfs.ext4 {raw_s}"))?;

        // 3. losetup — from this point on, the loop device MUST be detached before we
        //    return, regardless of what happens.
        let out = shell
            .run(spec_priv(Path::new("/usr/sbin/losetup"), &["-fP", "--show", &raw_s]))
            .await?;
        check(&out, "losetup attach")?;
        let loop_dev = String::from_utf8(out.stdout)?
            .trim()
            .to_string();
        if loop_dev.is_empty() {
            anyhow::bail!("losetup returned empty loop device");
        }

        // 4-8. Mount + populate + unmount, with guaranteed unmount on error.
        let populate_result =
            mount_and_populate(shell, paths, &loop_dev, mount_dir, sha, service_user).await;

        // 9. Always attempt to detach the loop device. If populate succeeded but detach
        //    failed, that's fatal — the loop is still attached on the host, and handing
        //    the raw file to the VM in that state means two writers on the same backing
        //    store.
        let detach_result = detach_loop(shell, &loop_dev).await;

        match (populate_result, detach_result) {
            (Ok(()), Ok(())) => Ok(Self { path: raw }),
            (Ok(()), Err(detach_err)) => Err(detach_err.context(format!(
                "loop device {loop_dev} still attached after populate; refusing to hand off"
            ))),
            (Err(populate_err), Ok(())) => Err(populate_err),
            (Err(populate_err), Err(detach_err)) => {
                tracing::warn!(error = %detach_err, "losetup -d also failed during error cleanup");
                Err(populate_err)
            }
        }
    }

    /// Build a minimal source disk for a binary-cache hit: format the raw disk
    /// and seed only `target/release/stacks-bench`. No git checkout is needed
    /// because the bench VM only execs the binary from `$SRC`.
    pub async fn provision_minimal(
        shell: &dyn Shell,
        job_dir: &Path,
        mount_dir: &Path,
        binary: &Path,
        service_user: &str,
    ) -> anyhow::Result<Self> {
        let raw = job_dir.join("source.raw");
        let raw_s = raw.display().to_string();
        std::fs::create_dir_all(mount_dir)?;

        let out = shell
            .run(spec(Path::new("/usr/bin/truncate"), &["-s", SOURCE_SIZE, &raw_s]))
            .await?;
        check(&out, &format!("truncate {raw_s}"))?;

        let out = shell
            .run(spec_priv(Path::new("/usr/sbin/mkfs.ext4"), &["-F", "-L", "sbgh-src", &raw_s]))
            .await?;
        check(&out, &format!("mkfs.ext4 {raw_s}"))?;

        let out = shell
            .run(spec_priv(Path::new("/usr/sbin/losetup"), &["-fP", "--show", &raw_s]))
            .await?;
        check(&out, "losetup attach (minimal seed)")?;
        let loop_dev = String::from_utf8(out.stdout)?
            .trim()
            .to_string();
        if loop_dev.is_empty() {
            anyhow::bail!("losetup returned empty loop device");
        }

        let seed_result =
            mount_and_seed_minimal(shell, &loop_dev, mount_dir, binary, service_user).await;
        let detach_result = detach_loop(shell, &loop_dev).await;

        match (seed_result, detach_result) {
            (Ok(()), Ok(())) => Ok(Self { path: raw }),
            (Ok(()), Err(detach_err)) => Err(detach_err.context(format!(
                "loop device {loop_dev} still attached after minimal seed; refusing to hand off"
            ))),
            (Err(seed_err), Ok(())) => Err(seed_err),
            (Err(seed_err), Err(detach_err)) => {
                tracing::warn!(error = %detach_err, "losetup -d also failed during minimal seed cleanup");
                Err(seed_err)
            }
        }
    }

    pub fn teardown(self) -> std::io::Result<()> {
        match std::fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }
}

/// Mount the loop device, run the chown + clone + checkout, then unmount.
/// If any inner step fails, the function still attempts the unmount before
/// returning the original error.
async fn mount_and_populate(
    shell: &dyn Shell,
    paths: &PathsConfig,
    loop_dev: &str,
    mount_dir: &Path,
    sha: &str,
    service_user: &str,
) -> anyhow::Result<()> {
    let mount_s = mount_dir
        .display()
        .to_string();

    // mount
    let out = shell
        .run(spec_priv(Path::new("/usr/bin/mount"), &[loop_dev, &mount_s]))
        .await?;
    check(&out, &format!("mount {loop_dev} -> {mount_s}"))?;

    // Inner work — chown + clone + checkout. Failures here flow through but
    // we still need to unmount.
    let inner = populate_checkout(shell, paths, mount_dir, sha, service_user).await;

    // Unmount unconditionally. If the inner populate succeeded but unmount
    // failed, that's fatal: the filesystem is still mounted on the host, and
    // we cannot hand the underlying raw file to a VM in that state without
    // creating two concurrent writers.
    let unmount_result = unmount(shell, &mount_s).await;

    match (inner, unmount_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Ok(()), Err(unmount_err)) => Err(unmount_err
            .context(format!("{mount_s} still mounted after populate; refusing to hand off"))),
        (Err(populate_err), Ok(())) => Err(populate_err),
        (Err(populate_err), Err(unmount_err)) => {
            tracing::warn!(error = %unmount_err, "umount also failed during error cleanup");
            Err(populate_err)
        }
    }
}

async fn populate_checkout(
    shell: &dyn Shell,
    paths: &PathsConfig,
    mount_dir: &Path,
    sha: &str,
    service_user: &str,
) -> anyhow::Result<()> {
    let mount_s = mount_dir
        .display()
        .to_string();

    // chown
    let out = shell
        .run(spec_priv(
            Path::new("/usr/bin/chown"),
            &["-R", &format!("{service_user}:{service_user}"), &mount_s],
        ))
        .await?;
    check(&out, &format!("chown -R {service_user} {mount_s}"))?;

    // Remove ext4's `lost+found`. mke2fs creates it (required by fsck);
    // git clone would otherwise refuse with "destination path '…' already
    // exists and is not an empty directory". It's empty by construction
    // (mke2fs initialises it that way) and we just chowned the parent to
    // `service_user`, so a plain unprivileged `rmdir` is enough.
    let lost_found = mount_dir
        .join("lost+found")
        .display()
        .to_string();
    let out = shell
        .run(spec(Path::new("/usr/bin/rmdir"), &[&lost_found]))
        .await?;
    check(&out, &format!("rmdir {lost_found}"))?;

    // git clone (from local mirror)
    let mirror = paths
        .git_mirror
        .display()
        .to_string();
    let out = shell
        .run(spec(
            &paths.git_binary,
            &["clone", "--reference", &mirror, "--shared", &mirror, &mount_s],
        ))
        .await?;
    check(&out, "git clone (from mirror)")?;

    // git checkout sha
    let out = shell
        .run(spec(&paths.git_binary, &["-C", &mount_s, "checkout", "--detach", sha]))
        .await?;
    check(&out, &format!("git checkout {sha}"))?;

    Ok(())
}

async fn mount_and_seed_minimal(
    shell: &dyn Shell,
    loop_dev: &str,
    mount_dir: &Path,
    binary: &Path,
    service_user: &str,
) -> anyhow::Result<()> {
    let mount_s = mount_dir
        .display()
        .to_string();

    let out = shell
        .run(spec_priv(Path::new("/usr/bin/mount"), &[loop_dev, &mount_s]))
        .await?;
    check(&out, &format!("mount {loop_dev} -> {mount_s} (minimal seed)"))?;

    // Fresh ext4 roots are owned by root; make the empty mount writable by the
    // daemon before the host-side copy. This is cheap because there is no
    // checkout tree yet.
    let chown_result = chown_mount(shell, &mount_s, service_user).await;
    let inner = match chown_result {
        Ok(()) => seed_into_mount(shell, mount_dir, binary).await,
        Err(e) => Err(e),
    };
    let unmount_result = unmount(shell, &mount_s).await;

    match (inner, unmount_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Ok(()), Err(unmount_err)) => Err(unmount_err
            .context(format!("{mount_s} still mounted after minimal seed; refusing to hand off"))),
        (Err(seed_err), Ok(())) => Err(seed_err),
        (Err(seed_err), Err(unmount_err)) => {
            tracing::warn!(error = %unmount_err, "umount also failed during minimal seed cleanup");
            Err(seed_err)
        }
    }
}

/// Copy `binary` to `<mount>/target/release/stacks-bench` (creating the dir),
/// then chown the mounted tree to root so the root bench VM gets the same
/// hand-off state the build VM's `chown` produces.
async fn seed_into_mount(shell: &dyn Shell, mount_dir: &Path, binary: &Path) -> anyhow::Result<()> {
    // The mount is owned by the service user from provisioning, so the daemon can
    // create dirs + copy before the root chown.
    let dest = mount_dir.join("target/release/stacks-bench");
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::copy(binary, &dest)?;
    // Force executable mode: a cache copy that lost its `+x` bit would pass sha
    // verification but fail at exec time in the bench VM.
    std::fs::set_permissions(&dest, std::fs::Permissions::from_mode(0o755))?;

    let mount_s = mount_dir
        .display()
        .to_string();
    let out = shell
        .run(spec_priv(Path::new("/usr/bin/chown"), &["-R", "root:root", &mount_s]))
        .await?;
    check(&out, &format!("chown -R root:root {mount_s} (seed)"))
}

async fn chown_mount(shell: &dyn Shell, mount_s: &str, service_user: &str) -> anyhow::Result<()> {
    let out = shell
        .run(spec_priv(
            Path::new("/usr/bin/chown"),
            &["-R", &format!("{service_user}:{service_user}"), mount_s],
        ))
        .await?;
    check(&out, &format!("chown -R {service_user} {mount_s}"))
}

async fn unmount(shell: &dyn Shell, mount_s: &str) -> anyhow::Result<()> {
    let out = shell
        .run(spec_priv(Path::new("/usr/bin/umount"), &[mount_s]))
        .await?;
    check(&out, &format!("umount {mount_s}"))
}

async fn detach_loop(shell: &dyn Shell, loop_dev: &str) -> anyhow::Result<()> {
    let out = shell
        .run(spec_priv(Path::new("/usr/sbin/losetup"), &["-d", loop_dev]))
        .await?;
    check(&out, &format!("losetup -d {loop_dev}"))
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::libvirt::shell::test_support::{PreparedReply, RecordingShell};

    fn paths_in(dir: &TempDir) -> PathsConfig {
        PathsConfig {
            jobs_dir: dir.path().join("jobs"),
            git_mirror: dir.path().join("mirror.git"),
            results_tmpfs_root: dir.path().join("results"),
            results_archive_dir: dir.path().join("archive"),
            sccache_dir: dir.path().join("sccache"),
            virsh_binary: "/usr/bin/virsh".into(),
            sudo_binary: "/usr/bin/sudo".into(),
            qemu_img_binary: "/usr/bin/qemu-img".into(),
            cloud_localds_binary: "/usr/bin/cloud-localds".into(),
            git_binary: "/usr/bin/git".into(),
        }
    }

    #[tokio::test]
    async fn full_provisioning_sequence() {
        let tmp = TempDir::new().unwrap();
        let paths = paths_in(&tmp);
        let job_dir = tmp.path().join("jobs/job1");
        std::fs::create_dir_all(&job_dir).unwrap();
        let mount_dir = tmp.path().join("mnt");

        let shell = RecordingShell::new();
        shell
            .expect_ok(1) // 0: truncate
            .expect_ok(1) // 1: mkfs.ext4
            .reply(PreparedReply::with_stdout("/dev/loop42\n")) // 2: losetup -fP
            .expect_ok(1) // 3: mount
            .expect_ok(1) // 4: chown
            .expect_ok(1) // 5: rmdir lost+found
            .expect_ok(1) // 6: git clone
            .expect_ok(1) // 7: git checkout
            .expect_ok(1) // 8: umount
            .expect_ok(1); // 9: losetup -d

        let disk = SourceDisk::provision(&shell, &paths, &job_dir, &mount_dir, "abc123", "sbgh")
            .await
            .unwrap();
        assert_eq!(disk.path, job_dir.join("source.raw"));

        let calls = shell.calls();
        assert!(
            calls[0]
                .program
                .ends_with("truncate")
        );
        assert!(!calls[0].privileged);
        assert!(
            calls[1]
                .program
                .ends_with("mkfs.ext4")
        );
        assert!(calls[1].privileged);
        assert!(
            calls[2]
                .program
                .ends_with("losetup")
        );
        assert!(
            calls[3]
                .program
                .ends_with("mount")
        );
        assert!(
            calls[3]
                .args
                .contains(&"/dev/loop42".to_string())
        );
        // 5: rmdir lost+found (unprivileged — sbgh owns the dir post-chown).
        assert!(
            calls[5]
                .program
                .ends_with("rmdir")
        );
        assert!(!calls[5].privileged);
        assert!(
            calls[5]
                .args
                .iter()
                .any(|a| a.ends_with("lost+found"))
        );
        assert!(
            calls[6]
                .program
                .ends_with("git")
        );
        assert!(
            calls[6]
                .args
                .iter()
                .any(|a| a == "--reference")
        );
        assert!(
            calls[7]
                .args
                .iter()
                .any(|a| a == "abc123")
        );
        assert!(
            calls[8]
                .program
                .ends_with("umount")
        );
        assert!(
            calls[9]
                .program
                .ends_with("losetup")
        );
        assert!(
            calls[9]
                .args
                .contains(&"-d".to_string())
        );
    }

    #[tokio::test]
    async fn minimal_provision_seeds_binary_without_checkout() {
        let tmp = TempDir::new().unwrap();
        let job_dir = tmp.path().join("jobs/job1");
        std::fs::create_dir_all(&job_dir).unwrap();
        let mount_dir = tmp.path().join("mnt");
        let binary = tmp
            .path()
            .join("cached-stacks-bench");
        std::fs::write(&binary, b"CACHED").unwrap();

        let shell = RecordingShell::new();
        shell
            .expect_ok(1) // truncate
            .expect_ok(1) // mkfs.ext4
            .reply(PreparedReply::with_stdout("/dev/loop9\n")) // losetup -fP
            .expect_ok(1) // mount
            .expect_ok(1) // chown empty fs to service user
            .expect_ok(1) // chown seeded tree to root
            .expect_ok(1) // umount
            .expect_ok(1); // losetup -d

        let disk = SourceDisk::provision_minimal(&shell, &job_dir, &mount_dir, &binary, "sbgh")
            .await
            .unwrap();

        assert_eq!(disk.path, job_dir.join("source.raw"));
        assert_eq!(
            std::fs::read(mount_dir.join("target/release/stacks-bench")).unwrap(),
            b"CACHED"
        );

        let calls = shell.calls();
        assert!(
            !calls
                .iter()
                .any(|c| c.program.ends_with("git")),
            "minimal cache-hit source disk must not checkout a tree"
        );
        assert!(
            !calls.iter().any(|c| c
                .args
                .iter()
                .any(|a| a == "lost+found")),
            "minimal path does not prepare a git clone destination"
        );
    }

    /// If `git clone` fails, we must still unmount the loop device and detach
    /// the loop dev. Regression test for the leak that the code review caught.
    #[tokio::test]
    async fn failure_after_mount_still_unmounts_and_detaches() {
        let tmp = TempDir::new().unwrap();
        let paths = paths_in(&tmp);
        let job_dir = tmp.path().join("jobs/job1");
        std::fs::create_dir_all(&job_dir).unwrap();
        let mount_dir = tmp.path().join("mnt");

        let shell = RecordingShell::new();
        shell
            .expect_ok(1) // truncate
            .expect_ok(1) // mkfs.ext4
            .reply(PreparedReply::with_stdout("/dev/loop42\n")) // losetup -fP
            .expect_ok(1) // mount
            .expect_ok(1) // chown
            .expect_ok(1) // rmdir lost+found
            .reply(PreparedReply::fail("fatal: repository not found")) // git clone fails
            .expect_ok(1) // umount (cleanup)
            .expect_ok(1); // losetup -d (cleanup)

        let err = SourceDisk::provision(&shell, &paths, &job_dir, &mount_dir, "abc123", "sbgh")
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("git clone")
        );

        let calls = shell.calls();
        // git checkout must NOT have run.
        assert!(
            !calls.iter().any(|c| c
                .args
                .iter()
                .any(|a| a == "checkout")),
            "git checkout should be skipped after clone failure"
        );
        // umount + losetup -d MUST have run.
        let umount_ran = calls
            .iter()
            .any(|c| c.program.ends_with("umount"));
        let detach_ran = calls.iter().any(|c| {
            c.program.ends_with("losetup")
                && c.args
                    .contains(&"-d".to_string())
        });
        assert!(umount_ran, "umount must run on cleanup");
        assert!(detach_ran, "losetup -d must run on cleanup");
    }

    /// If everything succeeds but unmount fails, refuse to hand off — the
    /// filesystem is still mounted on the host and giving the raw file to a
    /// VM would create two writers on the same backing store.
    #[tokio::test]
    async fn unmount_failure_after_successful_populate_is_fatal() {
        let tmp = TempDir::new().unwrap();
        let paths = paths_in(&tmp);
        let job_dir = tmp.path().join("jobs/job1");
        std::fs::create_dir_all(&job_dir).unwrap();
        let mount_dir = tmp.path().join("mnt");

        let shell = RecordingShell::new();
        shell
            .expect_ok(1) // truncate
            .expect_ok(1) // mkfs.ext4
            .reply(PreparedReply::with_stdout("/dev/loop42\n")) // losetup -fP
            .expect_ok(1) // mount
            .expect_ok(1) // chown
            .expect_ok(1) // rmdir lost+found
            .expect_ok(1) // git clone
            .expect_ok(1) // git checkout
            .reply(PreparedReply::fail("umount: target is busy")) // umount fails
            .expect_ok(1); // losetup -d (still attempted)

        let err = SourceDisk::provision(&shell, &paths, &job_dir, &mount_dir, "abc", "sbgh")
            .await
            .unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("still mounted after populate"),
            "expected fatal unmount message, got: {msg}"
        );
    }

    /// Same idea for losetup: if populate + unmount succeed but the loop
    /// detach fails, the device is still attached on the host. Refuse.
    #[tokio::test]
    async fn detach_failure_after_successful_populate_is_fatal() {
        let tmp = TempDir::new().unwrap();
        let paths = paths_in(&tmp);
        let job_dir = tmp.path().join("jobs/job1");
        std::fs::create_dir_all(&job_dir).unwrap();
        let mount_dir = tmp.path().join("mnt");

        let shell = RecordingShell::new();
        shell
            .expect_ok(1) // truncate
            .expect_ok(1) // mkfs.ext4
            .reply(PreparedReply::with_stdout("/dev/loop42\n")) // losetup -fP
            .expect_ok(1) // mount
            .expect_ok(1) // chown
            .expect_ok(1) // rmdir lost+found
            .expect_ok(1) // git clone
            .expect_ok(1) // git checkout
            .expect_ok(1) // umount
            .reply(PreparedReply::fail("losetup: cannot detach: device or resource busy"));

        let err = SourceDisk::provision(&shell, &paths, &job_dir, &mount_dir, "abc", "sbgh")
            .await
            .unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("still attached after populate"),
            "expected fatal detach message, got: {msg}"
        );
    }

    /// If `mount` itself fails, the loop device must still be detached.
    #[tokio::test]
    async fn mount_failure_still_detaches_loop() {
        let tmp = TempDir::new().unwrap();
        let paths = paths_in(&tmp);
        let job_dir = tmp.path().join("jobs/job1");
        std::fs::create_dir_all(&job_dir).unwrap();
        let mount_dir = tmp.path().join("mnt");

        let shell = RecordingShell::new();
        shell
            .expect_ok(1) // truncate
            .expect_ok(1) // mkfs.ext4
            .reply(PreparedReply::with_stdout("/dev/loop42\n")) // losetup -fP
            .reply(PreparedReply::fail("mount: wrong fs type")) // mount fails
            // umount won't run (we never mounted), but losetup -d still must:
            .expect_ok(1); // losetup -d (cleanup)

        let err = SourceDisk::provision(&shell, &paths, &job_dir, &mount_dir, "abc123", "sbgh")
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("mount")
        );

        let calls = shell.calls();
        let detach_ran = calls.iter().any(|c| {
            c.program.ends_with("losetup")
                && c.args
                    .contains(&"-d".to_string())
        });
        assert!(detach_ran, "losetup -d must run after mount failure");
    }
}
