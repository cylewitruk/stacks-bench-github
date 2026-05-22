//! Render the per-job cloud-init `user-data` + `meta-data` and pack them into
//! a NoCloud seed ISO via `cloud-localds`. The VM picks up the ISO as a
//! cdrom-attached device and runs the embedded startup script on first boot.

use std::path::{Path, PathBuf};

use sbgh_core::config::PathsConfig;

use crate::libvirt::shell::{Shell, check, spec};

pub struct CloudInitParams<'a> {
    pub job_id: &'a str,
    pub head_sha: &'a str,
    pub stacks_bench_args: &'a str,
    /// Mountpoint inside the VM for the chainstate snapshot (vdb).
    pub chainstate_mount: &'a str,
    /// Mountpoint inside the VM for the source disk (vdc).
    pub source_mount: &'a str,
    /// Virtio-fs tag for the host tmpfs share (matches `<target dir>` in domain
    /// XML).
    pub results_share_tag: &'a str,
    /// Mountpoint inside the VM for the results share.
    pub results_mount: &'a str,
}

pub struct CloudInitArtifacts {
    pub iso_path: PathBuf,
}

const RUN_SCRIPT: &str = include_str!("templates/sbgh-run.sh.tmpl");

impl CloudInitArtifacts {
    pub async fn build(
        shell: &dyn Shell,
        paths: &PathsConfig,
        job_dir: &Path,
        params: &CloudInitParams<'_>,
    ) -> anyhow::Result<Self> {
        let user_data = render_user_data(params);
        let meta_data = render_meta_data(params.job_id);

        let user_path = job_dir.join("user-data");
        let meta_path = job_dir.join("meta-data");
        let iso_path = job_dir.join("cidata.iso");

        std::fs::write(&user_path, user_data)?;
        std::fs::write(&meta_path, meta_data)?;

        let out = shell
            .run(spec(
                &paths.cloud_localds_binary,
                &[
                    &iso_path.display().to_string(),
                    &user_path
                        .display()
                        .to_string(),
                    &meta_path
                        .display()
                        .to_string(),
                ],
            ))
            .await?;
        check(&out, &format!("cloud-localds {}", iso_path.display()))?;

        Ok(Self { iso_path })
    }
}

fn render_meta_data(job_id: &str) -> String {
    format!("instance-id: sbgh-{job_id}\nlocal-hostname: sbgh-{job_id}\n")
}

fn render_user_data(p: &CloudInitParams<'_>) -> String {
    // Substitute simple `{{ name }}` placeholders. We avoid pulling in a
    // templating crate because the substitution set is small and fixed.
    let script = RUN_SCRIPT
        .replace("{{ head_sha }}", p.head_sha)
        .replace("{{ stacks_bench_args }}", p.stacks_bench_args)
        .replace("{{ chainstate_mount }}", p.chainstate_mount)
        .replace("{{ source_mount }}", p.source_mount)
        .replace("{{ results_share_tag }}", p.results_share_tag)
        .replace("{{ results_mount }}", p.results_mount);

    format!(
        "#cloud-config\nwrite_files:\n  - path: /usr/local/bin/sbgh-run.sh\n    permissions: \
         '0755'\n    content: |\n{indented}\n\nruncmd:\n  - [ /usr/local/bin/sbgh-run.sh \
         ]\npower_state:\n  mode: poweroff\n  condition: True\n",
        indented = indent_for_yaml_block(&script, 6)
    )
}

fn indent_for_yaml_block(s: &str, n: usize) -> String {
    let pad = " ".repeat(n);
    s.lines()
        .map(|l| format!("{pad}{l}"))
        .collect::<Vec<_>>()
        .join("\n")
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

    fn sample_params<'a>() -> CloudInitParams<'a> {
        CloudInitParams {
            job_id: "job1",
            head_sha: "abc123",
            stacks_bench_args: "--iters 5",
            chainstate_mount: "/var/lib/stacks-chainstate",
            source_mount: "/opt/stacks-core",
            results_share_tag: "results",
            results_mount: "/results",
        }
    }

    #[tokio::test]
    async fn build_writes_seed_iso() {
        let tmp = TempDir::new().unwrap();
        let paths = paths_in(&tmp);
        let job_dir = tmp.path().join("jobs/job1");
        std::fs::create_dir_all(&job_dir).unwrap();
        let shell = RecordingShell::new();
        shell.expect_ok(1);

        let arts = CloudInitArtifacts::build(&shell, &paths, &job_dir, &sample_params())
            .await
            .unwrap();

        assert_eq!(arts.iso_path, job_dir.join("cidata.iso"));
        // user-data + meta-data should be on disk
        let user_data = std::fs::read_to_string(job_dir.join("user-data")).unwrap();
        let meta_data = std::fs::read_to_string(job_dir.join("meta-data")).unwrap();
        assert!(meta_data.contains("instance-id: sbgh-job1"));
        assert!(user_data.contains("#cloud-config"));
        assert!(user_data.contains("abc123"));
        assert!(user_data.contains("--iters 5"));
        assert!(user_data.contains("/opt/stacks-core"));

        let calls = shell.calls();
        assert_eq!(calls.len(), 1);
        assert!(
            calls[0]
                .program
                .ends_with("cloud-localds")
        );
        assert_eq!(calls[0].args.len(), 3);
        assert!(calls[0].args[0].ends_with("cidata.iso"));
    }

    #[test]
    fn render_substitutes_all_placeholders() {
        let s = render_user_data(&sample_params());
        assert!(!s.contains("{{"), "unsubstituted placeholder: {s}");
        assert!(s.contains("power_state"));
        assert!(s.contains("poweroff"));
    }
}
