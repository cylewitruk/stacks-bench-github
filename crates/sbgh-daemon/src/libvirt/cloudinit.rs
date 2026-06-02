//! Render the per-job cloud-init `user-data` + `meta-data` and pack
//! them into NoCloud seed ISOs via `cloud-localds`. Two ISOs per job:
//! one for the **build** boot, one for the **bench** boot. Distinct
//! `instance-id` values on the meta-data side make cloud-init treat
//! the second boot as a fresh instance and re-run user-data scripts,
//! which is how we get a different script to run on each phase.

use std::path::{Path, PathBuf};

use sbgh_core::config::PathsConfig;

use crate::libvirt::shell::{Shell, check, spec};

/// Inputs shared by both phases. These get baked into both ISOs
/// identically — paths, share tags, ownership identity. Phase-specific
/// knobs (script body, instance-id suffix, args) ride in the per-phase
/// builder methods.
pub struct CloudInitCommon<'a> {
    pub job_id: &'a str,
    pub head_sha: &'a str,
    /// Mountpoint inside the VM for the chainstate snapshot (vdb).
    /// Only the bench phase actually mounts it.
    pub chainstate_mount: &'a str,
    /// Mountpoint inside the VM for the source disk (vdc).
    pub source_mount: &'a str,
    /// Virtio-fs tag for the host tmpfs share (matches `<target dir>` in
    /// domain XML).
    pub results_share_tag: &'a str,
    /// Mountpoint inside the VM for the results share.
    pub results_mount: &'a str,
    /// Virtio-fs tag for the persistent sccache cache share. Only the
    /// build phase mounts it.
    pub sccache_share_tag: &'a str,
    /// Mountpoint inside the VM for the sccache cache.
    pub sccache_mount: &'a str,
    /// Cache-size cap to pass to sccache (`SCCACHE_CACHE_SIZE`).
    /// Bytes-with-unit format, e.g. `"20G"`.
    pub sccache_max_size: &'a str,
}

/// Bench-phase-specific extras. `stacks_bench_args` is the only
/// user-controlled field — it's substituted into the run command verbatim.
pub struct BenchPhaseParams<'a> {
    pub stacks_bench_args: &'a str,
}

/// Both rendered NoCloud ISOs, paths relative to the per-job dir.
/// Daemon attaches one at a time as the cidata cdrom of the
/// build / bench VM lifecycles.
pub struct CloudInitArtifacts {
    pub build_iso_path: PathBuf,
    pub bench_iso_path: PathBuf,
}

const BUILD_SCRIPT: &str = include_str!("templates/sbgh-build.sh.tmpl");
const BENCH_SCRIPT: &str = include_str!("templates/sbgh-bench.sh.tmpl");

impl CloudInitArtifacts {
    pub async fn build(
        shell: &dyn Shell,
        paths: &PathsConfig,
        job_dir: &Path,
        common: &CloudInitCommon<'_>,
        bench: &BenchPhaseParams<'_>,
    ) -> anyhow::Result<Self> {
        let build_iso = pack_iso(
            shell,
            paths,
            job_dir,
            "build",
            &render_build_user_data(common),
            &meta_data_for_phase(common.job_id, "build"),
        )
        .await?;
        let bench_iso = pack_iso(
            shell,
            paths,
            job_dir,
            "bench",
            &render_bench_user_data(common, bench),
            &meta_data_for_phase(common.job_id, "bench"),
        )
        .await?;
        Ok(Self {
            build_iso_path: build_iso,
            bench_iso_path: bench_iso,
        })
    }
}

/// Write user-data + meta-data + invoke cloud-localds to produce a
/// NoCloud seed ISO. Suffix gives us per-phase filenames in the per-job
/// dir (`user-data.build` / `cidata.build.iso` vs `.bench`).
async fn pack_iso(
    shell: &dyn Shell,
    paths: &PathsConfig,
    job_dir: &Path,
    suffix: &str,
    user_data: &str,
    meta_data: &str,
) -> anyhow::Result<PathBuf> {
    let user_path = job_dir.join(format!("user-data.{suffix}"));
    let meta_path = job_dir.join(format!("meta-data.{suffix}"));
    let iso_path = job_dir.join(format!("cidata.{suffix}.iso"));

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
    Ok(iso_path)
}

/// Meta-data — phase suffix on instance-id is what tells cloud-init
/// the second boot is a "new instance" and so should re-run user-data
/// modules (write_files, runcmd, etc.) rather than treating it as a
/// normal reboot.
fn meta_data_for_phase(job_id: &str, phase: &str) -> String {
    format!("instance-id: sbgh-{job_id}-{phase}\nlocal-hostname: sbgh-{job_id}-{phase}\n")
}

fn render_build_user_data(c: &CloudInitCommon<'_>) -> String {
    // Build phase doesn't read stacks_bench_args; only the bench
    // phase invokes stacks-bench.
    let script = BUILD_SCRIPT
        .replace("{{ head_sha }}", c.head_sha)
        .replace("{{ source_mount }}", c.source_mount)
        .replace("{{ results_share_tag }}", c.results_share_tag)
        .replace("{{ results_mount }}", c.results_mount)
        .replace("{{ sccache_share_tag }}", c.sccache_share_tag)
        .replace("{{ sccache_mount }}", c.sccache_mount)
        .replace("{{ sccache_max_size }}", c.sccache_max_size);
    wrap_in_cloud_config(&script, "sbgh-build.sh")
}

fn render_bench_user_data(c: &CloudInitCommon<'_>, b: &BenchPhaseParams<'_>) -> String {
    let script = BENCH_SCRIPT
        .replace("{{ head_sha }}", c.head_sha)
        .replace("{{ source_mount }}", c.source_mount)
        .replace("{{ chainstate_mount }}", c.chainstate_mount)
        .replace("{{ results_share_tag }}", c.results_share_tag)
        .replace("{{ results_mount }}", c.results_mount)
        .replace("{{ stacks_bench_args }}", b.stacks_bench_args);
    wrap_in_cloud_config(&script, "sbgh-bench.sh")
}

/// Wrap a bash script in the cloud-config YAML envelope cloud-init
/// expects in NoCloud user-data. The script is written to
/// /usr/local/bin/<basename> mode 0755 and `runcmd` invokes it.
/// `power_state: poweroff` ensures the VM shuts down after the script
/// exits — daemon polls for the shut-off and proceeds to the
/// next phase (or teardown).
fn wrap_in_cloud_config(script: &str, basename: &str) -> String {
    format!(
        "#cloud-config\nwrite_files:\n  - path: /usr/local/bin/{basename}\n    permissions: \
         '0755'\n    content: |\n{indented}\n\nruncmd:\n  - [ /usr/local/bin/{basename} \
         ]\npower_state:\n  mode: poweroff\n  condition: True\n",
        indented = indent_for_yaml_block(script, 6)
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
            sccache_dir: dir.path().join("sccache"),
            virsh_binary: "/usr/bin/virsh".into(),
            sudo_binary: "/usr/bin/sudo".into(),
            qemu_img_binary: "/usr/bin/qemu-img".into(),
            cloud_localds_binary: "/usr/bin/cloud-localds".into(),
            git_binary: "/usr/bin/git".into(),
        }
    }

    fn sample_common<'a>() -> CloudInitCommon<'a> {
        CloudInitCommon {
            job_id: "job1",
            head_sha: "abc123",
            chainstate_mount: "/var/lib/stacks-chainstate",
            source_mount: "/opt/stacks-core",
            results_share_tag: "results",
            results_mount: "/results",
            sccache_share_tag: "sccache",
            sccache_mount: "/var/cache/sccache",
            sccache_max_size: "20G",
        }
    }

    fn sample_bench<'a>() -> BenchPhaseParams<'a> {
        BenchPhaseParams { stacks_bench_args: "--iters 5" }
    }

    #[tokio::test]
    async fn build_writes_two_seed_isos() {
        let tmp = TempDir::new().unwrap();
        let paths = paths_in(&tmp);
        let job_dir = tmp.path().join("jobs/job1");
        std::fs::create_dir_all(&job_dir).unwrap();
        let shell = RecordingShell::new();
        // cloud-localds runs twice — once per phase.
        shell.expect_ok(2);

        let arts =
            CloudInitArtifacts::build(&shell, &paths, &job_dir, &sample_common(), &sample_bench())
                .await
                .unwrap();

        assert_eq!(arts.build_iso_path, job_dir.join("cidata.build.iso"));
        assert_eq!(arts.bench_iso_path, job_dir.join("cidata.bench.iso"));

        // Per-phase user-data and meta-data should both land on disk.
        let build_user = std::fs::read_to_string(job_dir.join("user-data.build")).unwrap();
        let bench_user = std::fs::read_to_string(job_dir.join("user-data.bench")).unwrap();
        let build_meta = std::fs::read_to_string(job_dir.join("meta-data.build")).unwrap();
        let bench_meta = std::fs::read_to_string(job_dir.join("meta-data.bench")).unwrap();

        // Different instance-ids — this is the cloud-init signal for
        // "treat second boot as a fresh instance, re-run user-data".
        assert!(build_meta.contains("instance-id: sbgh-job1-build"));
        assert!(bench_meta.contains("instance-id: sbgh-job1-bench"));

        // Phase-appropriate scripts embedded.
        assert!(build_user.contains("sbgh-build.sh"));
        assert!(build_user.contains("phase \"build_done\""));
        assert!(!build_user.contains("stacks-bench bench run"));

        assert!(bench_user.contains("sbgh-bench.sh"));
        assert!(bench_user.contains("stacks-bench"));
        assert!(bench_user.contains("--iters 5"));
        assert!(!bench_user.contains("phase \"build_done\""));

        // cloud-localds invoked twice.
        let calls = shell.calls();
        assert_eq!(calls.len(), 2);
        for c in &calls {
            assert!(
                c.program
                    .ends_with("cloud-localds")
            );
            assert_eq!(c.args.len(), 3);
        }
    }

    #[test]
    fn build_script_substitutes_all_placeholders() {
        let s = render_build_user_data(&sample_common());
        assert!(!s.contains("{{"), "unsubstituted placeholder in build script: {s}");
        assert!(s.contains("power_state"));
    }

    #[test]
    fn bench_script_substitutes_all_placeholders() {
        let s = render_bench_user_data(&sample_common(), &sample_bench());
        assert!(!s.contains("{{"), "unsubstituted placeholder in bench script: {s}");
        assert!(s.contains("--iters 5"));
        assert!(s.contains("power_state"));
    }
}
