//! `Shell` is the thin boundary between provisioners and the host. Everything
//! that runs a command — including `sudo` for privileged ops — goes through
//! this trait, so provisioner modules can be unit-tested by injecting a
//! `RecordingShell` that captures intended commands without executing them.
//!
//! Privileged commands are invoked via `run_priv`, which prepends
//! `sudo -n -- <prog> <args>` so the sudoers entry can match on exact paths.

use std::path::Path;
use std::process::Output;

use async_trait::async_trait;

#[derive(Debug, Clone)]
pub struct CommandSpec {
    pub program: String,
    pub args: Vec<String>,
    pub privileged: bool,
    pub stdin: Option<Vec<u8>>,
}

#[async_trait]
pub trait Shell: Send + Sync {
    async fn run(&self, cmd: CommandSpec) -> anyhow::Result<Output>;
}

// ─────────────────────────── helpers used by provisioners
// ───────────────────────────

pub fn spec(program: &Path, args: &[&str]) -> CommandSpec {
    CommandSpec {
        program: program.display().to_string(),
        args: args
            .iter()
            .map(|s| s.to_string())
            .collect(),
        privileged: false,
        stdin: None,
    }
}

pub fn spec_priv(program: &Path, args: &[&str]) -> CommandSpec {
    CommandSpec {
        privileged: true,
        ..spec(program, args)
    }
}

/// Convert raw command output into a clean error if the process exited
/// non-zero.
pub fn check(out: &Output, ctx: &str) -> anyhow::Result<()> {
    if out.status.success() {
        Ok(())
    } else {
        let stdout = String::from_utf8_lossy(&out.stdout);
        let stderr = String::from_utf8_lossy(&out.stderr);
        anyhow::bail!(
            "{ctx} failed with status {}: stdout={} stderr={}",
            out.status,
            stdout.trim(),
            stderr.trim(),
        )
    }
}

// ─────────────────────────── real impl ───────────────────────────

pub struct SystemShell {
    sudo: std::path::PathBuf,
}

impl SystemShell {
    pub fn new(sudo: impl Into<std::path::PathBuf>) -> Self {
        Self { sudo: sudo.into() }
    }
}

#[async_trait]
impl Shell for SystemShell {
    async fn run(&self, cmd: CommandSpec) -> anyhow::Result<Output> {
        use std::process::Stdio;

        use tokio::io::AsyncWriteExt;
        use tokio::process::Command;

        let mut command = if cmd.privileged {
            let mut c = Command::new(&self.sudo);
            c.arg("-n")
                .arg("--")
                .arg(&cmd.program)
                .args(&cmd.args);
            c
        } else {
            let mut c = Command::new(&cmd.program);
            c.args(&cmd.args);
            c
        };

        command
            .stdin(if cmd.stdin.is_some() { Stdio::piped() } else { Stdio::null() })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        tracing::debug!(?cmd, "executing");
        let mut child = command.spawn()?;
        if let Some(input) = cmd.stdin
            && let Some(mut sin) = child.stdin.take()
        {
            sin.write_all(&input).await?;
            sin.shutdown().await?;
        }
        let output = child
            .wait_with_output()
            .await?;
        Ok(output)
    }
}

// ─────────────────────────── recording impl ───────────────────────────

#[cfg(test)]
pub mod test_support {
    use std::collections::VecDeque;
    use std::os::unix::process::ExitStatusExt;
    use std::process::{ExitStatus, Output};
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::{CommandSpec, Shell};

    pub struct PreparedReply {
        pub status: i32,
        pub stdout: Vec<u8>,
        pub stderr: Vec<u8>,
    }

    impl PreparedReply {
        pub fn ok() -> Self {
            Self {
                status: 0,
                stdout: Vec::new(),
                stderr: Vec::new(),
            }
        }
        pub fn with_stdout(stdout: impl Into<Vec<u8>>) -> Self {
            Self {
                status: 0,
                stdout: stdout.into(),
                stderr: Vec::new(),
            }
        }
        pub fn fail(stderr: impl Into<Vec<u8>>) -> Self {
            Self {
                status: 1,
                stdout: Vec::new(),
                stderr: stderr.into(),
            }
        }
    }

    #[derive(Default)]
    pub struct RecordingShell {
        replies: Mutex<VecDeque<PreparedReply>>,
        recorded: Mutex<Vec<CommandSpec>>,
    }

    impl RecordingShell {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn expect_ok(&self, n: usize) -> &Self {
            let mut q = self.replies.lock().unwrap();
            for _ in 0..n {
                q.push_back(PreparedReply::ok());
            }
            self
        }

        pub fn reply(&self, r: PreparedReply) -> &Self {
            self.replies
                .lock()
                .unwrap()
                .push_back(r);
            self
        }

        pub fn calls(&self) -> Vec<CommandSpec> {
            self.recorded
                .lock()
                .unwrap()
                .clone()
        }
    }

    #[async_trait]
    impl Shell for RecordingShell {
        async fn run(&self, cmd: CommandSpec) -> anyhow::Result<Output> {
            self.recorded
                .lock()
                .unwrap()
                .push(cmd.clone());
            let reply = self
                .replies
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(PreparedReply::ok);
            Ok(Output {
                status: ExitStatus::from_raw(reply.status << 8),
                stdout: reply.stdout,
                stderr: reply.stderr,
            })
        }
    }
}
