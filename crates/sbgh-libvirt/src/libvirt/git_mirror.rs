//! Bare git mirror of stacks-core, maintained on the host.
//!
//! The mirror is shared across jobs. Per-job source disks are populated from
//! it via `git clone --reference <mirror>` so we never re-download the entire
//! object graph. PR refs are fetched on demand.
//!
//! **Concurrency (roadmap-v5):** the mirror is the one piece of shared mutable
//! state on the otherwise per-job execution path, so every mutation is
//! serialized by [`struct@MIRROR_LOCK`].

use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::PathsConfig;
use anyhow::{Context, ensure};
use tokio::sync::Mutex;

use crate::libvirt::shell::{Shell, check, spec};

/// Serializes mutations of the shared bare mirror across concurrent jobs. There
/// is exactly one mirror per worker, so a process-global lock is the natural
/// granularity.
///
/// Without it, two jobs running at once could race `git` on one repo: a
/// fresh-host double `clone --mirror` (the `ensure` TOCTOU), `FETCH_HEAD`
/// clobbering, and packed-refs / object-store contention. The guarded
/// operations are seconds-long at most — trivial next to a benchmark run — so
/// serializing them is free insurance. (Per-job refs `refs/sbgh/<job_id>`
/// already avoid ref *name* collisions; this closes the repo-level races.)
///
/// **Scope:** process-local — it protects the mirror only within a single
/// worker. That matches the deployment model (one worker profile owns its
/// `paths.git_mirror`). Running *two* worker processes against the same mirror
/// path would need an OS-level file lock (`flock`) instead; out of scope today.
static MIRROR_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
static MIRROR_CLONE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Ensure the bare mirror exists at `paths.git_mirror`. Idempotent.
/// If absent, clone `repo_url` as a bare mirror.
pub async fn ensure(
    shell: &dyn Shell,
    paths: &PathsConfig,
    repo_url: &str,
    repository_token: Option<&str>,
) -> anyhow::Result<()> {
    // Held across the exists()-check + clone so two fresh-host jobs can't both
    // clone into the same directory (TOCTOU).
    let _guard = MIRROR_LOCK.lock().await;
    if paths.git_mirror.exists() {
        return validate_bare_mirror(&paths.git_mirror);
    }
    let parent = paths
        .git_mirror
        .parent()
        .context("git mirror path has no parent directory")?;
    std::fs::create_dir_all(parent)?;
    let staging_root = staging_path(&paths.git_mirror)?;
    std::fs::create_dir(&staging_root).with_context(|| {
        format!("reserving git mirror staging directory {}", staging_root.display())
    })?;
    let staging = staging_root.join("mirror.git");
    let mut command =
        spec(&paths.git_binary, &["clone", "--mirror", repo_url, &staging.display().to_string()]);
    if let Some(token) = repository_token {
        command = command.with_repository_token(token);
    }
    let result = async {
        let out = shell.run(command).await?;
        check(&out, &format!("git clone --mirror {repo_url}"))?;
        validate_bare_mirror(&staging)?;
        std::fs::rename(&staging, &paths.git_mirror).with_context(|| {
            format!(
                "publishing staged git mirror {} as {}",
                staging.display(),
                paths.git_mirror.display()
            )
        })?;
        let _ = std::fs::remove_dir(&staging_root);
        Ok(())
    }
    .await;
    if result.is_err() {
        let _ = std::fs::remove_dir_all(&staging_root);
    }
    result
}

fn staging_path(mirror: &Path) -> anyhow::Result<PathBuf> {
    let parent = mirror
        .parent()
        .context("git mirror path has no parent directory")?;
    let name = mirror
        .file_name()
        .and_then(|name| name.to_str())
        .context("git mirror path has no UTF-8 file name")?;
    Ok(parent.join(format!(
        ".{name}.clone-{}-{}",
        std::process::id(),
        MIRROR_CLONE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    )))
}

fn validate_bare_mirror(mirror: &Path) -> anyhow::Result<()> {
    ensure!(mirror.is_dir(), "git mirror {} is not a directory", mirror.display());
    ensure!(
        mirror.join("HEAD").is_file()
            && mirror
                .join("config")
                .is_file()
            && mirror
                .join("objects")
                .is_dir()
            && mirror.join("refs").is_dir(),
        "git mirror {} is incomplete or is not a bare repository",
        mirror.display()
    );
    Ok(())
}

/// Fetch a specific commit SHA into the mirror so we can clone it locally
/// afterwards. Uses a synthetic ref under `refs/sbgh/<job_id>` to keep refs
/// scoped per job (cleaned up by `prune`).
pub async fn fetch_sha(
    shell: &dyn Shell,
    paths: &PathsConfig,
    job_id: &str,
    sha: &str,
    repository_token: Option<&str>,
) -> anyhow::Result<()> {
    let _guard = MIRROR_LOCK.lock().await;
    let refspec = format!("+{sha}:refs/sbgh/{job_id}");
    let mut command = spec(
        &paths.git_binary,
        &[
            "--git-dir",
            &paths
                .git_mirror
                .display()
                .to_string(),
            "fetch",
            "origin",
            &refspec,
        ],
    );
    if let Some(token) = repository_token {
        command = command.with_repository_token(token);
    }
    let out = shell.run(command).await?;
    check(&out, &format!("git fetch sha {sha} into refs/sbgh/{job_id}"))
}

/// Drop the per-job ref. Best-effort: failures are logged but not bubbled.
pub async fn prune(shell: &dyn Shell, paths: &PathsConfig, job_id: &str) {
    let _guard = MIRROR_LOCK.lock().await;
    let _ = shell
        .run(spec(
            &paths.git_binary,
            &[
                "--git-dir",
                &paths
                    .git_mirror
                    .display()
                    .to_string(),
                "update-ref",
                "-d",
                &format!("refs/sbgh/{job_id}"),
            ],
        ))
        .await;
}

#[cfg(test)]
mod tests {
    use std::os::unix::process::ExitStatusExt;
    use std::process::{ExitStatus, Output};
    use std::sync::Mutex as StdMutex;

    use crate::PathsConfig;
    use async_trait::async_trait;
    use tempfile::TempDir;

    use super::*;
    use crate::libvirt::shell::test_support::RecordingShell;

    fn create_bare_mirror(path: &Path) {
        std::fs::create_dir_all(path.join("objects")).unwrap();
        std::fs::create_dir_all(path.join("refs")).unwrap();
        std::fs::write(path.join("HEAD"), b"ref: refs/heads/main\n").unwrap();
        std::fs::write(path.join("config"), b"[core]\n\tbare = true\n").unwrap();
    }

    #[derive(Default)]
    struct CloneShell {
        calls: StdMutex<Vec<crate::libvirt::shell::CommandSpec>>,
        fail: bool,
    }

    #[async_trait]
    impl Shell for CloneShell {
        async fn run(&self, command: crate::libvirt::shell::CommandSpec) -> anyhow::Result<Output> {
            if !self.fail {
                let destination = Path::new(command.args.last().unwrap());
                create_bare_mirror(destination);
            }
            self.calls
                .lock()
                .unwrap()
                .push(command);
            Ok(Output {
                status: ExitStatus::from_raw(i32::from(self.fail) << 8),
                stdout: Vec::new(),
                stderr: if self.fail { b"clone failed\n".to_vec() } else { Vec::new() },
            })
        }
    }

    fn paths_in(dir: &TempDir) -> PathsConfig {
        PathsConfig {
            jobs_dir: dir.path().join("jobs"),
            git_mirror: dir
                .path()
                .join("git")
                .join("repo.git"),
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
    async fn ensure_skips_when_mirror_exists() {
        let dir = TempDir::new().unwrap();
        let paths = paths_in(&dir);
        create_bare_mirror(&paths.git_mirror);
        let shell = RecordingShell::new();
        ensure(&shell, &paths, "https://example/foo.git", None)
            .await
            .unwrap();
        assert!(shell.calls().is_empty());
    }

    #[tokio::test]
    async fn ensure_runs_clone_mirror_when_absent() {
        let dir = TempDir::new().unwrap();
        let paths = paths_in(&dir);
        let shell = CloneShell::default();
        ensure(&shell, &paths, "https://example/foo.git", None)
            .await
            .unwrap();
        let calls = shell.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert!(
            calls[0]
                .args
                .contains(&"--mirror".to_string())
        );
        assert!(
            calls[0]
                .args
                .contains(&"https://example/foo.git".to_string())
        );
        assert!(paths.git_mirror.exists());
        assert!(
            !calls[0]
                .args
                .last()
                .unwrap()
                .ends_with("repo.git")
        );
    }

    #[tokio::test]
    async fn failed_clone_leaves_neither_final_nor_staging_mirror() {
        let dir = TempDir::new().unwrap();
        let paths = paths_in(&dir);
        let shell = CloneShell {
            fail: true,
            ..CloneShell::default()
        };

        assert!(
            ensure(&shell, &paths, "https://example/foo.git", None)
                .await
                .is_err()
        );
        assert!(!paths.git_mirror.exists());
        assert_eq!(
            std::fs::read_dir(
                paths
                    .git_mirror
                    .parent()
                    .unwrap()
            )
            .unwrap()
            .count(),
            0
        );
    }

    #[tokio::test]
    async fn incomplete_existing_mirror_is_rejected_without_running_git() {
        let dir = TempDir::new().unwrap();
        let paths = paths_in(&dir);
        std::fs::create_dir_all(&paths.git_mirror).unwrap();
        let shell = RecordingShell::new();

        let error = ensure(&shell, &paths, "https://example/foo.git", None)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("incomplete")
        );
        assert!(shell.calls().is_empty());
    }

    #[tokio::test]
    async fn fetch_sha_uses_namespaced_ref() {
        let dir = TempDir::new().unwrap();
        let paths = paths_in(&dir);
        let shell = RecordingShell::new();
        shell.expect_ok(1);
        fetch_sha(&shell, &paths, "job1", "deadbeef", None)
            .await
            .unwrap();
        let calls = shell.calls();
        assert!(
            calls[0]
                .args
                .contains(&"fetch".to_string())
        );
        assert!(
            calls[0]
                .args
                .iter()
                .any(|a| a == "+deadbeef:refs/sbgh/job1")
        );
    }

    /// A `Shell` that holds each command open briefly and records the peak
    /// number of commands in flight at once — so the test can observe whether
    /// two concurrent mirror fetches overlap.
    #[derive(Default)]
    struct ConcurrencyProbeShell {
        in_flight: std::sync::atomic::AtomicUsize,
        max_in_flight: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl Shell for ConcurrencyProbeShell {
        async fn run(
            &self,
            _cmd: crate::libvirt::shell::CommandSpec,
        ) -> anyhow::Result<std::process::Output> {
            use std::sync::atomic::Ordering::SeqCst;
            let now = self
                .in_flight
                .fetch_add(1, SeqCst)
                + 1;
            self.max_in_flight
                .fetch_max(now, SeqCst);
            // Hold the critical section open so an *unserialized* peer would be
            // observed in flight at the same time (peak would reach 2).
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            self.in_flight
                .fetch_sub(1, SeqCst);
            use std::os::unix::process::ExitStatusExt;
            Ok(std::process::Output {
                status: std::process::ExitStatus::from_raw(0),
                stdout: Vec::new(),
                stderr: Vec::new(),
            })
        }
    }

    /// Two jobs fetching into the one shared mirror at the same time must be
    /// serialized by `MIRROR_LOCK` — never both touching the repo at once.
    #[tokio::test]
    async fn concurrent_mirror_fetches_are_serialized() {
        let dir = TempDir::new().unwrap();
        let paths = paths_in(&dir);
        let shell = ConcurrencyProbeShell::default();

        let (a, b) = tokio::join!(
            fetch_sha(&shell, &paths, "jobA", "sha-a", None),
            fetch_sha(&shell, &paths, "jobB", "sha-b", None),
        );
        a.unwrap();
        b.unwrap();

        assert_eq!(
            shell
                .max_in_flight
                .load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the mirror lock must serialize concurrent fetches into the shared bare repo",
        );
    }
}
