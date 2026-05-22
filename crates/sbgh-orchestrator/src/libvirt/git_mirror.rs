//! Bare git mirror of stacks-core, maintained on the host.
//!
//! The mirror is shared across jobs. Per-job source disks are populated from
//! it via `git clone --reference <mirror>` so we never re-download the entire
//! object graph. PR refs are fetched on demand.

use sbgh_core::config::PathsConfig;

use crate::libvirt::shell::{Shell, check, spec};

/// Ensure the bare mirror exists at `paths.git_mirror`. Idempotent.
/// If absent, clone `repo_url` as a bare mirror.
pub async fn ensure(shell: &dyn Shell, paths: &PathsConfig, repo_url: &str) -> anyhow::Result<()> {
    if paths.git_mirror.exists() {
        return Ok(());
    }
    if let Some(parent) = paths.git_mirror.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let out = shell
        .run(spec(
            &paths.git_binary,
            &[
                "clone",
                "--mirror",
                repo_url,
                &paths
                    .git_mirror
                    .display()
                    .to_string(),
            ],
        ))
        .await?;
    check(&out, &format!("git clone --mirror {repo_url}"))
}

/// Fetch a specific commit SHA into the mirror so we can clone it locally
/// afterwards. Uses a synthetic ref under `refs/sbgh/<job_id>` to keep refs
/// scoped per job (cleaned up by `prune`).
pub async fn fetch_sha(
    shell: &dyn Shell,
    paths: &PathsConfig,
    job_id: &str,
    sha: &str,
) -> anyhow::Result<()> {
    let refspec = format!("+{sha}:refs/sbgh/{job_id}");
    let out = shell
        .run(spec(
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
        ))
        .await?;
    check(&out, &format!("git fetch sha {sha} into refs/sbgh/{job_id}"))
}

/// Drop the per-job ref. Best-effort: failures are logged but not bubbled.
pub async fn prune(shell: &dyn Shell, paths: &PathsConfig, job_id: &str) {
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
    use sbgh_core::config::PathsConfig;
    use tempfile::TempDir;

    use super::*;
    use crate::libvirt::shell::test_support::RecordingShell;

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
        std::fs::create_dir_all(&paths.git_mirror).unwrap();
        let shell = RecordingShell::new();
        ensure(&shell, &paths, "https://example/foo.git")
            .await
            .unwrap();
        assert!(shell.calls().is_empty());
    }

    #[tokio::test]
    async fn ensure_runs_clone_mirror_when_absent() {
        let dir = TempDir::new().unwrap();
        let paths = paths_in(&dir);
        let shell = RecordingShell::new();
        shell.expect_ok(1);
        ensure(&shell, &paths, "https://example/foo.git")
            .await
            .unwrap();
        let calls = shell.calls();
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
    }

    #[tokio::test]
    async fn fetch_sha_uses_namespaced_ref() {
        let dir = TempDir::new().unwrap();
        let paths = paths_in(&dir);
        let shell = RecordingShell::new();
        shell.expect_ok(1);
        fetch_sha(&shell, &paths, "job1", "deadbeef")
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
}
