//! Collect post-mortem signals from a job before its artifacts are torn down.
//!
//! Everything in here is best-effort: missing files are reported as `None`,
//! not errors. The output ends up in the job's `result` JSONB column so we
//! can see what happened after the per-job directory has been deleted.

use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

const CONSOLE_TAIL_BYTES: usize = 64 * 1024;

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

/// Per-job archive layout. All four artifacts land in
/// `<results_archive_dir>/<job_id>/`:
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
/// The layout matches the directory `stacks-bench --db <DIR>` expects,
/// so post-hoc investigation is just:
///
/// ```bash
/// cd /var/lib/sbgh/results/<job-id>
/// ./stacks-bench --db . bench list
/// ```
///
/// no symlinks, no `--db ../../something` gymnastics.
const SQLITE_RELATIVE: &str = "appdata/stacks-bench.db";
const BINARY_RELATIVE: &str = "stacks-bench";
const RUN_JSON_RELATIVE: &str = "run.json";
const PHASE_LOG_RELATIVE: &str = "phase.log";

/// Build the per-job archive root: `<archive_dir>/<job_id>/`.
pub fn job_archive_root(archive_dir: &Path, job_id: &str) -> PathBuf {
    archive_dir.join(job_id)
}

/// Copy the SQLite output into the per-job archive at
/// `<job-dir>/appdata/stacks-bench.db` (matching stacks-bench's own
/// `--db <DIR>` layout). Returns the archived path and byte size,
/// or `(None, None)` if no file was produced.
pub fn archive_sqlite(
    src: &Path,
    archive_dir: &Path,
    job_id: &str,
) -> (Option<PathBuf>, Option<u64>) {
    archive_into(src, archive_dir, job_id, SQLITE_RELATIVE, "sqlite archive")
}

/// Copy the stacks-bench binary into the per-job archive at
/// `<job-dir>/stacks-bench`. The binary is the source of truth for the
/// DB schema — keeping it paired with the data means you don't need to
/// remember which stacks-core commit produced this DB to read it later.
pub fn archive_binary(
    src: &Path,
    archive_dir: &Path,
    job_id: &str,
) -> (Option<PathBuf>, Option<u64>) {
    archive_into(src, archive_dir, job_id, BINARY_RELATIVE, "stacks-bench binary archive")
}

/// Copy the stacks-bench `--json` stdout capture into the per-job
/// archive at `<job-dir>/run.json`. Human-readable summary of the run
/// and the source of the curated metrics in the PR comment.
pub fn archive_run_json(
    src: &Path,
    archive_dir: &Path,
    job_id: &str,
) -> (Option<PathBuf>, Option<u64>) {
    archive_into(src, archive_dir, job_id, RUN_JSON_RELATIVE, "run.json archive")
}

/// Copy the append-only phase journal into the per-job archive at
/// `<job-dir>/phase.log`. Timestamped record of every phase transition;
/// useful for "what took so long?" investigations after the per-job
/// dir is gone.
pub fn archive_phase_log(
    src: &Path,
    archive_dir: &Path,
    job_id: &str,
) -> (Option<PathBuf>, Option<u64>) {
    archive_into(src, archive_dir, job_id, PHASE_LOG_RELATIVE, "phase-log archive")
}

/// Copy `src` into `<archive_dir>/<job_id>/<relative>`, creating any
/// intermediate dirs. Returns the archived path + byte size on success,
/// or `(None, None)` if `src` doesn't exist OR the copy fails. Logs
/// warnings for any operation that fails but never errors — forensics
/// is always best-effort.
fn archive_into(
    src: &Path,
    archive_dir: &Path,
    job_id: &str,
    relative: &str,
    label: &str,
) -> (Option<PathBuf>, Option<u64>) {
    if !src.exists() {
        return (None, None);
    }
    let dest = job_archive_root(archive_dir, job_id).join(relative);
    if let Some(parent) = dest.parent()
        && let Err(e) = std::fs::create_dir_all(parent)
    {
        tracing::warn!(error = %e, %label, dest = %dest.display(), "create archive dir failed");
        return (None, None);
    }
    match std::fs::copy(src, &dest) {
        Ok(bytes) => (Some(dest), Some(bytes)),
        Err(e) => {
            tracing::warn!(error = %e, src = %src.display(), %label, "archive copy failed");
            (None, None)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

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

    #[test]
    fn archive_sqlite_skips_when_missing() {
        let dir = TempDir::new().unwrap();
        let (path, size) = archive_sqlite(&dir.path().join("nope.sqlite"), dir.path(), "job1");
        assert!(path.is_none());
        assert!(size.is_none());
    }

    #[test]
    fn archive_sqlite_lands_in_appdata_under_job_dir() {
        let dir = TempDir::new().unwrap();
        let src = dir
            .path()
            .join("stacks-bench.db");
        let mut f = std::fs::File::create(&src).unwrap();
        f.write_all(b"sqlite-data")
            .unwrap();
        let archive = dir.path().join("archive");

        let (path, size) = archive_sqlite(&src, &archive, "job1");
        // Layout mirrors stacks-bench's --db <DIR> expectation so
        // `./stacks-bench --db . bench ...` works against the archived
        // job dir without further setup.
        let expected = archive.join("job1/appdata/stacks-bench.db");
        assert_eq!(path.as_deref(), Some(expected.as_path()));
        assert_eq!(size, Some(b"sqlite-data".len() as u64));
        assert!(expected.exists());
    }

    #[test]
    fn archive_binary_lands_at_job_dir_root() {
        let dir = TempDir::new().unwrap();
        let src = dir
            .path()
            .join("stacks-bench");
        std::fs::write(&src, b"ELF...").unwrap();
        let archive = dir.path().join("archive");

        let (path, size) = archive_binary(&src, &archive, "job2");
        let expected = archive.join("job2/stacks-bench");
        assert_eq!(path.as_deref(), Some(expected.as_path()));
        assert_eq!(size, Some(b"ELF...".len() as u64));
    }

    #[test]
    fn archive_run_json_and_phase_log_land_at_job_dir_root() {
        let dir = TempDir::new().unwrap();
        let src_json = dir.path().join("run.json");
        std::fs::write(&src_json, b"{\"ok\":true}").unwrap();
        let src_log = dir.path().join("phase-log");
        std::fs::write(&src_log, b"1700000000 done\n").unwrap();
        let archive = dir.path().join("archive");

        let (j_path, _) = archive_run_json(&src_json, &archive, "job3");
        let (l_path, _) = archive_phase_log(&src_log, &archive, "job3");
        assert_eq!(
            j_path.as_deref(),
            Some(
                archive
                    .join("job3/run.json")
                    .as_path()
            )
        );
        assert_eq!(
            l_path.as_deref(),
            Some(
                archive
                    .join("job3/phase.log")
                    .as_path()
            )
        );
    }

    #[test]
    fn job_archive_root_is_archive_dir_plus_job_id() {
        let root = job_archive_root(Path::new("/var/lib/sbgh/results"), "abc-123");
        assert_eq!(root, PathBuf::from("/var/lib/sbgh/results/abc-123"));
    }
}
