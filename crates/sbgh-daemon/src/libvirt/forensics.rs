//! Collect post-mortem signals from a job before its artifacts are torn down.
//!
//! Everything in here is best-effort: missing files are reported as `None`,
//! not errors. The output ends up in the job's `result` JSONB column so we
//! can see what happened after the per-job directory has been deleted.
//!
//! The actual archiving (copying the per-job artifacts into
//! `results_archive_dir`) now goes through
//! [`ArtifactStore`](crate::artifact_store::ArtifactStore); this module keeps
//! the console-tail reader and the per-artifact relative names that define the
//! archive layout.

use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

const CONSOLE_TAIL_BYTES: usize = 64 * 1024;

/// Per-job archive layout — each artifact lands at
/// `<results_archive_dir>/<job_id>/<relative>`:
///
/// ```text
/// <root>/<job_id>/
///   ├── stacks-bench         ← the binary that produced this run
///   ├── appdata/
///   │   └── stacks-bench.db  ← preserves stacks-bench's own --db layout
///   ├── run.json             ← raw JSON output from `bench run --json`
///   └── phase.log            ← in-VM phase journal (timestamped)
/// ```
///
/// The layout matches the directory `stacks-bench --db <DIR>` expects, so
/// post-hoc investigation is just `cd <root>/<job-id> && ./stacks-bench --db .
/// bench list` — no symlinks, no `--db ../../something` gymnastics. The driver
/// builds store keys from these via
/// [`artifact_key`](crate::artifact_store::artifact_key).
pub const SQLITE_RELATIVE: &str = "appdata/stacks-bench.db";
pub const BINARY_RELATIVE: &str = "stacks-bench";
pub const RUN_JSON_RELATIVE: &str = "run.json";
pub const PHASE_LOG_RELATIVE: &str = "phase.log";

/// Read the last `max_bytes` of a text file. Returns `None` if the file
/// doesn't exist; logs and returns `None` on any other error.
pub fn console_tail(path: &Path) -> (Option<String>, Option<u64>) {
    let meta = match std::fs::metadata(path) {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return (None, None),
        Err(e) => {
            tracing::warn!(error = %e, path = %path.display(), "stat console.log failed");
            return (None, None);
        }
    };
    let size = meta.len();
    let mut file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) => {
            tracing::warn!(error = %e, "open console.log failed");
            return (None, Some(size));
        }
    };
    let offset = size.saturating_sub(CONSOLE_TAIL_BYTES as u64);
    if offset > 0
        && let Err(e) = file.seek(SeekFrom::Start(offset))
    {
        tracing::warn!(error = %e, "seek console.log failed");
        return (None, Some(size));
    }
    let mut buf = Vec::with_capacity((size as usize).min(CONSOLE_TAIL_BYTES));
    if let Err(e) = file.read_to_end(&mut buf) {
        tracing::warn!(error = %e, "read console.log failed");
        return (None, Some(size));
    }
    (Some(String::from_utf8_lossy(&buf).into_owned()), Some(size))
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn console_tail_missing_file_returns_none() {
        let dir = TempDir::new().unwrap();
        let (tail, size) = console_tail(&dir.path().join("nope.log"));
        assert!(tail.is_none());
        assert!(size.is_none());
    }

    #[test]
    fn console_tail_returns_last_n_bytes() {
        let dir = TempDir::new().unwrap();
        let p = dir.path().join("c.log");
        // Make it larger than CONSOLE_TAIL_BYTES.
        let body: String = "x".repeat(CONSOLE_TAIL_BYTES + 1024) + "TAILSTART";
        std::fs::write(&p, &body).unwrap();

        let (tail, size) = console_tail(&p);
        let tail = tail.unwrap();
        assert!(tail.ends_with("TAILSTART"));
        assert!(tail.len() <= CONSOLE_TAIL_BYTES + "TAILSTART".len());
        assert_eq!(size.unwrap(), body.len() as u64);
    }

    #[test]
    fn console_tail_returns_full_file_when_small() {
        let dir = TempDir::new().unwrap();
        let p = dir.path().join("c.log");
        std::fs::write(&p, b"hello world").unwrap();
        let (tail, _) = console_tail(&p);
        assert_eq!(tail.as_deref(), Some("hello world"));
    }
}
