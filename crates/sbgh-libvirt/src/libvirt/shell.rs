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

#[derive(Clone)]
pub struct CommandSpec {
    pub program: String,
    pub args: Vec<String>,
    pub privileged: bool,
    pub stdin: Option<Vec<u8>>,
    secret_env: Vec<(String, String)>,
}

impl std::fmt::Debug for CommandSpec {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CommandSpec")
            .field("program", &self.program)
            .field("args", &self.args)
            .field("privileged", &self.privileged)
            .field(
                "stdin",
                &self
                    .stdin
                    .as_ref()
                    .map(|value| value.len()),
            )
            .field(
                "secret_env",
                &self
                    .secret_env
                    .iter()
                    .map(|(name, _)| (name, "[REDACTED]"))
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl CommandSpec {
    pub fn with_repository_token(mut self, token: &str) -> Self {
        use base64::Engine as _;

        // GitHub's smart-HTTP Git endpoint expects installation tokens as
        // HTTP Basic credentials. Bearer authentication is for GitHub's API;
        // using it here causes Git to fall back to an interactive username
        // prompt, which fails closed in the non-interactive worker.
        let credentials =
            base64::engine::general_purpose::STANDARD.encode(format!("x-access-token:{token}"));
        self.secret_env.extend([
            ("GIT_CONFIG_COUNT".into(), "1".into()),
            ("GIT_CONFIG_KEY_0".into(), "http.https://github.com/.extraheader".into()),
            ("GIT_CONFIG_VALUE_0".into(), format!("Authorization: Basic {credentials}")),
        ]);
        self
    }
}

#[async_trait]
pub trait Shell: Send + Sync {
    async fn run(&self, cmd: CommandSpec) -> anyhow::Result<Output>;

    /// Like [`run`](Shell::run), but bounds the command with `timeout` and
    /// **kills + reaps the child on timeout** so a wedged process can't leak.
    /// For callers where the slot / boot is time-bounded and a hung
    /// subprocess must not accumulate — the pin manager's `git ls-remote` (item
    /// 0025). The default delegates to `run` (test fakes complete promptly, so
    /// the bound is moot); [`SystemShell`] overrides it with the real timeout +
    /// kill.
    ///
    /// **Scope:** the bound covers the spawn + `wait` phase, **not** an
    /// optional `stdin` write. That's fine for stdinless commands (the
    /// `ls-remote` use case); a large-`stdin` command could still block on
    /// the write before the timeout applies — bound the whole write+wait
    /// yourself for those.
    async fn run_bounded(
        &self,
        cmd: CommandSpec,
        timeout: std::time::Duration,
    ) -> anyhow::Result<Output> {
        let _ = timeout;
        self.run(cmd).await
    }
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
        secret_env: Vec::new(),
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

    /// Build (but don't spawn) the `tokio` command for `cmd` — privilege
    /// wrapper + piped stdio. Shared by [`run`](Shell::run) and
    /// [`run_bounded`](Shell::run_bounded).
    fn build_command(&self, cmd: &CommandSpec) -> tokio::process::Command {
        use std::process::Stdio;

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
            .envs(
                cmd.secret_env
                    .iter()
                    .map(|(name, value)| (name, value)),
            )
            .stdin(if cmd.stdin.is_some() { Stdio::piped() } else { Stdio::null() })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        command
    }
}

#[async_trait]
impl Shell for SystemShell {
    async fn run(&self, cmd: CommandSpec) -> anyhow::Result<Output> {
        use tokio::io::AsyncWriteExt;

        // TRACE rather than DEBUG: the daemon polls `virsh
        // domstate` every few seconds for the duration of every job,
        // so per-command logging at DEBUG buries the actually-useful
        // phase-change / heartbeat / failure lines. Re-enable with
        // RUST_LOG=trace if you need the per-invocation detail.
        tracing::trace!(?cmd, "executing");
        let mut child = self
            .build_command(&cmd)
            .spawn()?;
        if let Some(input) = &cmd.stdin
            && let Some(mut sin) = child.stdin.take()
        {
            sin.write_all(input).await?;
            sin.shutdown().await?;
        }
        let output = child
            .wait_with_output()
            .await?;
        Ok(output)
    }

    async fn run_bounded(
        &self,
        cmd: CommandSpec,
        timeout: std::time::Duration,
    ) -> anyhow::Result<Output> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        tracing::trace!(?cmd, ?timeout, "executing (bounded)");
        let mut command = self.build_command(&cmd);
        // Backstop only: if this whole future is dropped before we reap (e.g. an
        // *outer* timeout), kill the child rather than leak it. The inner
        // timeout below reaps explicitly.
        command.kill_on_drop(true);
        let mut child = command.spawn()?;
        if let Some(input) = &cmd.stdin
            && let Some(mut sin) = child.stdin.take()
        {
            sin.write_all(input).await?;
            sin.shutdown().await?;
        }
        // Own the pipes so `child.wait()` can borrow `&mut child` while we drain
        // stdout/stderr **concurrently** — draining sequentially could deadlock
        // on a full pipe buffer. (`wait_with_output` does this internally but
        // consumes the child, leaving nothing to kill on timeout.)
        let mut out_pipe = child.stdout.take();
        let mut err_pipe = child.stderr.take();
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let collect = async {
            let read_out = async {
                match out_pipe.as_mut() {
                    Some(p) => {
                        p.read_to_end(&mut stdout)
                            .await
                    }
                    None => Ok(0),
                }
            };
            let read_err = async {
                match err_pipe.as_mut() {
                    Some(p) => {
                        p.read_to_end(&mut stderr)
                            .await
                    }
                    None => Ok(0),
                }
            };
            let (ro, re, status) = tokio::join!(read_out, read_err, child.wait());
            ro?;
            re?;
            status
        };
        match tokio::time::timeout(timeout, collect).await {
            Ok(status) => Ok(Output {
                status: status?,
                stdout,
                stderr,
            }),
            Err(_) => {
                // Explicit SIGKILL **and reap** so no zombie/leaked child
                // outlives the call (the `kill_on_drop` backstop reaps lazily).
                let _ = child.kill().await;
                anyhow::bail!("`{}` timed out after {timeout:?} (process killed)", cmd.program)
            }
        }
    }
}

#[cfg(test)]
mod credential_tests {
    use base64::Engine as _;

    use super::spec;

    #[test]
    fn repository_token_is_redacted_from_command_debug_output() {
        let token = "github_pat_secret-value";
        let command =
            spec(std::path::Path::new("/usr/bin/git"), &["fetch"]).with_repository_token(token);
        let debug = format!("{command:?}");

        assert!(!debug.contains(token));
        assert!(debug.contains("GIT_CONFIG_VALUE_0"));
        assert!(debug.contains("[REDACTED]"));

        let encoded =
            base64::engine::general_purpose::STANDARD.encode(format!("x-access-token:{token}"));
        assert_eq!(command.secret_env[2].0, "GIT_CONFIG_VALUE_0");
        assert_eq!(command.secret_env[2].1, format!("Authorization: Basic {encoded}"));
    }
}

// ─────────────────────────── recording impl ───────────────────────────

#[cfg(any(test, feature = "testing"))]
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

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::Duration;

    use super::*;

    /// `run_bounded` returns the output normally when the command finishes
    /// inside the bound.
    #[tokio::test]
    async fn run_bounded_completes_a_fast_command() {
        let shell = SystemShell::new("/usr/bin/sudo");
        let out = shell
            .run_bounded(spec(Path::new("/bin/echo"), &["hi"]), Duration::from_secs(5))
            .await
            .unwrap();
        assert!(out.status.success());
        assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "hi");
    }

    /// A command that outlives the bound errors (timed out). The child is
    /// killed via `kill_on_drop` — the bound is what `ls-remote` relies on so a
    /// wedged remote can't hold the runner slot or leak a git process.
    #[tokio::test]
    async fn run_bounded_times_out_and_errors_on_a_slow_command() {
        let shell = SystemShell::new("/usr/bin/sudo");
        let err = shell
            .run_bounded(spec(Path::new("/bin/sleep"), &["5"]), Duration::from_millis(50))
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("timed out"),
            "got: {err}"
        );
    }
}
