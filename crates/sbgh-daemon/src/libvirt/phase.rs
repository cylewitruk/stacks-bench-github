//! Read the append-only `.phase-log` journal written by the in-VM
//! `sbgh-run.sh` script.
//!
//! Format: one `<unix-seconds> <phase>` line per transition, appended by
//! the VM's `phase()` shell function. Append-only so the daemon
//! can poll on a slow cadence (5+ s) without losing fast transitions
//! (e.g. `starting → building` can be sub-second). The host reads new
//! lines since the last poll's byte offset and emits one `PhaseListener`
//! callback per entry.
//!
//! Unknown phase tokens are accepted (forward-compat).

use std::fmt;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Phase {
    Starting,
    Building,
    /// Build VM finished a successful `cargo build`. Terminal for the
    /// build VM only — daemon transitions to the bench VM after
    /// seeing this. NOT terminal for the bench VM (bench finishes on
    /// `Done`).
    BuildDone,
    Running,
    Collecting,
    Done,
    Error,
    Other(String),
}

/// Which phase counts as "successful completion" for the current VM.
/// Used by the daemon's two-phase lifecycle to distinguish the
/// build VM (succeeds on BuildDone) from the bench VM (succeeds on Done).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollMode {
    Build,
    Bench,
}

impl Phase {
    /// True when this phase ends the *bench* VM's lifecycle (the
    /// "final" terminal — the run is over either way). Kept as
    /// `is_terminal` for back-compat with existing callers; new
    /// callers should prefer `is_success_for(mode)` when they need to
    /// distinguish build-success from bench-success.
    pub fn is_terminal(&self) -> bool {
        matches!(self, Phase::Done | Phase::Error)
    }

    /// True when this phase is a successful completion for the given
    /// `PollMode`. `Phase::Error` always counts as completion (failed
    /// completion) regardless of mode — the daemon's poll loop
    /// uses this to decide when to stop polling.
    pub fn is_success_for(&self, mode: PollMode) -> bool {
        match mode {
            PollMode::Build => matches!(self, Phase::BuildDone),
            PollMode::Bench => matches!(self, Phase::Done),
        }
    }

    pub fn label(&self) -> &str {
        match self {
            Phase::Starting => "starting",
            Phase::Building => "building",
            Phase::BuildDone => "build_done",
            Phase::Running => "running",
            Phase::Collecting => "collecting",
            Phase::Done => "done",
            Phase::Error => "error",
            Phase::Other(s) => s,
        }
    }
}

impl FromStr for Phase {
    type Err = std::convert::Infallible;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s.trim() {
            "starting" => Phase::Starting,
            "building" => Phase::Building,
            "build_done" => Phase::BuildDone,
            "running" => Phase::Running,
            "collecting" => Phase::Collecting,
            "done" => Phase::Done,
            "error" => Phase::Other("error".into()), // see note below
            "" => Phase::Other(String::new()),
            other => Phase::Other(other.to_string()),
        })
        // Note: we map "error" through Other so the Phase::Error variant
        // isn't accidentally reachable from a typo; the canonical
        // construction path is explicit-string matching below.
        .map(|p| match p {
            Phase::Other(s) if s == "error" => Phase::Error,
            other => other,
        })
    }
}

impl fmt::Display for Phase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

/// Read the entire journal and return the last (most-recent) phase.
/// Used by post-mortem forensics to record `last_phase` after teardown.
pub fn read_last(phase_log: &Path) -> Option<Phase> {
    let body = match std::fs::read_to_string(phase_log) {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
        Err(e) => {
            tracing::warn!(error = %e, path = %phase_log.display(), "failed to read phase log");
            return None;
        }
    };
    body.lines()
        .map(str::trim)
        .rfind(|l| !l.is_empty())
        .and_then(|line| parse_line(line).map(|(_, p)| p))
}

/// Read any new `(timestamp, phase)` entries appended since the last
/// call. `offset` is updated to the new EOF on success.
///
/// Robust to a journal that grew between the last poll: we seek to
/// `*offset`, read to EOF, parse each complete line, and only advance
/// `*offset` past lines we successfully consumed (so a partial final
/// line gets re-read whole next time, not split mid-write).
pub fn read_since(phase_log: &Path, offset: &mut u64) -> Vec<(SystemTime, Phase)> {
    let mut file = match std::fs::File::open(phase_log) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Vec::new(),
        Err(e) => {
            tracing::warn!(error = %e, path = %phase_log.display(), "open phase log failed");
            return Vec::new();
        }
    };

    let file_len = match file.metadata() {
        Ok(m) => m.len(),
        Err(e) => {
            tracing::warn!(error = %e, "stat phase log failed");
            return Vec::new();
        }
    };

    // The journal is append-only, so the file should never shrink. If
    // it does (corruption, deleted-and-recreated), reset to read from
    // the start — better to re-emit everything than to skip entries.
    if file_len < *offset {
        tracing::warn!(
            old_offset = *offset,
            new_len = file_len,
            "phase log shrunk; replaying from start"
        );
        *offset = 0;
    }

    if let Err(e) = file.seek(SeekFrom::Start(*offset)) {
        tracing::warn!(error = %e, "seek phase log failed");
        return Vec::new();
    }

    let mut new_bytes = Vec::with_capacity((file_len - *offset) as usize);
    if let Err(e) = file.read_to_end(&mut new_bytes) {
        tracing::warn!(error = %e, "read phase log failed");
        return Vec::new();
    }

    let text = String::from_utf8_lossy(&new_bytes);
    let mut consumed_bytes: u64 = 0;
    let mut entries = Vec::new();

    for raw_line in text.split_inclusive('\n') {
        if !raw_line.ends_with('\n') {
            // Partial trailing line — leave it for the next poll so we
            // don't half-parse a mid-write entry.
            break;
        }
        consumed_bytes += raw_line.len() as u64;
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        match parse_line(line) {
            Some(entry) => entries.push(entry),
            None => {
                tracing::warn!(line, "malformed phase log entry, ignoring");
            }
        }
    }

    *offset += consumed_bytes;
    entries
}

/// Parse a single journal line: `<unix-seconds> <phase-token>`.
fn parse_line(line: &str) -> Option<(SystemTime, Phase)> {
    let (ts_str, phase_str) = line.split_once(' ')?;
    let ts_secs: u64 = ts_str.parse().ok()?;
    let when = UNIX_EPOCH + Duration::from_secs(ts_secs);
    let phase = Phase::from_str(phase_str.trim()).ok()?;
    Some((when, phase))
}

/// Pretty-print a duration as `HH:MM:SS`. Used in heartbeat logs and
/// PR comment elapsed-time annotations.
pub fn format_elapsed(d: Duration) -> String {
    let secs = d.as_secs();
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    format!("{h:02}:{m:02}:{s:02}")
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::TempDir;

    use super::*;

    #[test]
    fn parses_known_phases() {
        assert_eq!(Phase::from_str("building").unwrap(), Phase::Building);
        assert_eq!(Phase::from_str("done\n").unwrap(), Phase::Done);
        assert!(
            Phase::from_str("done")
                .unwrap()
                .is_terminal()
        );
        assert!(
            Phase::from_str("error")
                .unwrap()
                .is_terminal()
        );
        assert!(
            !Phase::from_str("running")
                .unwrap()
                .is_terminal()
        );
    }

    #[test]
    fn unknown_phase_is_preserved() {
        let p = Phase::from_str("future-phase").unwrap();
        assert_eq!(p, Phase::Other("future-phase".into()));
        assert_eq!(p.label(), "future-phase");
    }

    #[test]
    fn read_last_missing_file_returns_none() {
        let dir = TempDir::new().unwrap();
        assert!(read_last(&dir.path().join(".phase-log")).is_none());
    }

    #[test]
    fn read_last_returns_final_entry() {
        let dir = TempDir::new().unwrap();
        let p = dir.path().join(".phase-log");
        std::fs::write(&p, "1700000000 starting\n1700000005 building\n1700000300 running\n")
            .unwrap();
        assert_eq!(read_last(&p), Some(Phase::Running));
    }

    #[test]
    fn read_since_replays_in_order_and_advances_offset() {
        let dir = TempDir::new().unwrap();
        let p = dir.path().join(".phase-log");
        std::fs::write(&p, "1700000000 starting\n1700000005 building\n").unwrap();
        let mut offset = 0;
        let entries = read_since(&p, &mut offset);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].1, Phase::Starting);
        assert_eq!(entries[1].1, Phase::Building);
        let after_first_read = offset;
        assert!(after_first_read > 0);

        // Second read with no new content returns nothing, offset unchanged.
        let again = read_since(&p, &mut offset);
        assert!(again.is_empty());
        assert_eq!(offset, after_first_read);

        // Append one more entry; we should see only that one on next read.
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&p)
            .unwrap();
        f.write_all(b"1700000300 running\n")
            .unwrap();
        let entries = read_since(&p, &mut offset);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].1, Phase::Running);
    }

    #[test]
    fn read_since_does_not_advance_past_partial_line() {
        // Simulate a mid-write race: the VM has written `1700000000 sta`
        // but not the newline. We must NOT consume that partial line —
        // the next poll should see the whole line once it's complete.
        let dir = TempDir::new().unwrap();
        let p = dir.path().join(".phase-log");
        std::fs::write(&p, "1700000000 building\n1700000005 ru").unwrap();
        let mut offset = 0;
        let entries = read_since(&p, &mut offset);
        assert_eq!(entries.len(), 1, "only the complete line should be returned");
        assert_eq!(entries[0].1, Phase::Building);

        // Complete the partial line.
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&p)
            .unwrap();
        f.write_all(b"nning\n")
            .unwrap();
        let entries = read_since(&p, &mut offset);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].1, Phase::Running);
    }

    #[test]
    fn read_since_handles_shrunk_file_by_resetting() {
        let dir = TempDir::new().unwrap();
        let p = dir.path().join(".phase-log");
        std::fs::write(&p, "1700000000 building\n1700000005 running\n").unwrap();
        let mut offset = 0;
        let _ = read_since(&p, &mut offset);
        assert!(offset > 0);

        // Truncate (simulates file recreated after a crash). offset is
        // now larger than file length; read_since should reset to 0 and
        // re-emit everything rather than silently skipping.
        std::fs::write(&p, "1700000100 starting\n").unwrap();
        let entries = read_since(&p, &mut offset);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].1, Phase::Starting);
    }

    #[test]
    fn format_elapsed_pads_components() {
        assert_eq!(format_elapsed(Duration::from_secs(0)), "00:00:00");
        assert_eq!(format_elapsed(Duration::from_secs(5)), "00:00:05");
        assert_eq!(format_elapsed(Duration::from_secs(65)), "00:01:05");
        assert_eq!(format_elapsed(Duration::from_secs(3 * 3600 + 7 * 60 + 9)), "03:07:09");
    }
}
