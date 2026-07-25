//! Thin `virsh` wrappers — define/start/destroy/undefine/domstate. Every call
//! goes through the `Shell` trait so the driver can be tested with a recorder.

use std::path::Path;

use crate::PathsConfig;

use crate::libvirt::shell::{Shell, check, spec_priv};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DomState {
    Running,
    ShutOff,
    Paused,
    Other,
    Undefined,
}

impl DomState {
    fn parse(s: &str) -> Self {
        match s.trim() {
            "running" => DomState::Running,
            "shut off" => DomState::ShutOff,
            "paused" => DomState::Paused,
            "" => DomState::Undefined,
            _ => DomState::Other,
        }
    }
}

pub async fn define(shell: &dyn Shell, paths: &PathsConfig, xml_path: &Path) -> anyhow::Result<()> {
    let out = shell
        .run(spec_priv(&paths.virsh_binary, &["define", &xml_path.display().to_string()]))
        .await?;
    check(&out, &format!("virsh define {}", xml_path.display()))
}

pub async fn start(shell: &dyn Shell, paths: &PathsConfig, name: &str) -> anyhow::Result<()> {
    let out = shell
        .run(spec_priv(&paths.virsh_binary, &["start", name]))
        .await?;
    check(&out, &format!("virsh start {name}"))
}

pub async fn destroy(shell: &dyn Shell, paths: &PathsConfig, name: &str) -> anyhow::Result<()> {
    let out = shell
        .run(spec_priv(&paths.virsh_binary, &["destroy", name]))
        .await?;
    // `virsh destroy` on an already-shutoff domain returns non-zero — accept that.
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        if stderr.contains("not running") || stderr.contains("is not active") {
            return Ok(());
        }
        check(&out, &format!("virsh destroy {name}"))?;
    }
    Ok(())
}

pub async fn undefine(shell: &dyn Shell, paths: &PathsConfig, name: &str) -> anyhow::Result<()> {
    let out = shell
        .run(spec_priv(&paths.virsh_binary, &["undefine", name]))
        .await?;
    check(&out, &format!("virsh undefine {name}"))
}

pub async fn domstate(
    shell: &dyn Shell,
    paths: &PathsConfig,
    name: &str,
) -> anyhow::Result<DomState> {
    let out = shell
        .run(spec_priv(&paths.virsh_binary, &["domstate", name]))
        .await?;
    if !out.status.success() {
        // Domain not defined yet (or already undefined).
        return Ok(DomState::Undefined);
    }
    Ok(DomState::parse(&String::from_utf8_lossy(&out.stdout)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::libvirt::shell::test_support::{PreparedReply, RecordingShell};

    fn paths() -> PathsConfig {
        PathsConfig {
            jobs_dir: "/tmp".into(),
            git_mirror: "/tmp".into(),
            results_tmpfs_root: "/tmp".into(),
            results_archive_dir: "/tmp".into(),
            sccache_dir: "/tmp".into(),
            virsh_binary: "/usr/bin/virsh".into(),
            sudo_binary: "/usr/bin/sudo".into(),
            qemu_img_binary: "/usr/bin/qemu-img".into(),
            cloud_localds_binary: "/usr/bin/cloud-localds".into(),
            git_binary: "/usr/bin/git".into(),
        }
    }

    #[test]
    fn parses_domstate_strings() {
        assert_eq!(DomState::parse("running\n"), DomState::Running);
        assert_eq!(DomState::parse("  shut off"), DomState::ShutOff);
        assert_eq!(DomState::parse(""), DomState::Undefined);
        assert_eq!(DomState::parse("idle"), DomState::Other);
    }

    #[tokio::test]
    async fn domstate_reads_stdout() {
        let shell = RecordingShell::new();
        shell.reply(PreparedReply::with_stdout("shut off\n"));
        let s = domstate(&shell, &paths(), "sbgh-job1")
            .await
            .unwrap();
        assert_eq!(s, DomState::ShutOff);
    }

    #[tokio::test]
    async fn destroy_tolerates_already_shutoff() {
        let shell = RecordingShell::new();
        shell.reply(PreparedReply::fail(
            "error: Requested operation is not valid: domain is not running",
        ));
        destroy(&shell, &paths(), "sbgh-job1")
            .await
            .unwrap();
    }
}
