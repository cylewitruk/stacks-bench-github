//! Slash-command parser for PR comments.
//!
//! Supported syntax:
//!   /benchmark                        -> default benchmark
//!   /benchmark <subcommand> [args...] -> subcommand with optional args
//!
//! Only the first line of the comment is parsed, and the command must be at
//! the very start of that line (no leading whitespace), to avoid mistaking
//! quoted or indented text for a command.

use serde::{Deserialize, Serialize};

use crate::{GitHubError as Error, GitHubResult as Result};

const PREFIX: &str = "/benchmark";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Command {
    pub subcommand: Option<String>,
    pub args: Vec<String>,
}

/// Try to parse a command out of a PR comment body. Returns `Ok(None)` if the
/// comment does not contain a command at all (the common case), `Ok(Some(cmd))`
/// for a well-formed command, and `Err(_)` for a malformed one.
pub fn parse_command(body: &str) -> Result<Option<Command>> {
    let Some(first_line) = body.lines().next() else {
        return Ok(None);
    };
    let line = first_line.trim_end();

    if !line.starts_with(PREFIX) {
        return Ok(None);
    }

    let rest = &line[PREFIX.len()..];
    if !rest.is_empty() && !rest.starts_with(char::is_whitespace) {
        return Ok(None);
    }

    let mut tokens = rest.split_whitespace();
    let subcommand = tokens
        .next()
        .map(str::to_string);
    let args: Vec<String> = tokens
        .map(str::to_string)
        .collect();

    if let Some(sub) = subcommand.as_deref()
        && !is_valid_token(sub)
    {
        return Err(Error::InvalidCommand(format!("invalid subcommand: {sub:?}")));
    }
    for arg in &args {
        if !is_valid_arg(arg) {
            return Err(Error::InvalidCommand(format!("invalid argument: {arg:?}")));
        }
    }

    Ok(Some(Command { subcommand, args }))
}

fn is_valid_token(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

fn is_valid_arg(s: &str) -> bool {
    !s.is_empty()
        && s.chars().all(|c| {
            c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '=' | '.' | ':' | '/' | ',')
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_bare_command() {
        let cmd = parse_command("/benchmark")
            .unwrap()
            .unwrap();
        assert_eq!(cmd.subcommand, None);
        assert!(cmd.args.is_empty());
    }

    #[test]
    fn parses_subcommand_and_args() {
        let cmd = parse_command("/benchmark run --iters=10")
            .unwrap()
            .unwrap();
        assert_eq!(cmd.subcommand.as_deref(), Some("run"));
        assert_eq!(cmd.args, vec!["--iters=10"]);
    }

    #[test]
    fn ignores_non_command_comment() {
        assert!(
            parse_command("LGTM!")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn rejects_indented_command() {
        // Indented commands could be quoted text; refuse to act on them.
        assert!(
            parse_command("  /benchmark")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn rejects_inline_command() {
        assert!(
            parse_command("hey /benchmark run")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn rejects_shell_metacharacters() {
        assert!(parse_command("/benchmark run; rm -rf /").is_err());
    }

    #[test]
    fn only_parses_first_line() {
        let cmd = parse_command("/benchmark\nmore stuff")
            .unwrap()
            .unwrap();
        assert_eq!(cmd.subcommand, None);
    }
}
