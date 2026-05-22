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

/// Copy the SQLite output from per-job tmpfs to `archive_dir/<job_id>.sqlite`.
/// Returns the archived path and its size, or `None` if no file was produced.
pub fn archive_sqlite(
    src: &Path,
    archive_dir: &Path,
    job_id: &str,
) -> (Option<PathBuf>, Option<u64>) {
    if !src.exists() {
        return (None, None);
    }
    if let Err(e) = std::fs::create_dir_all(archive_dir) {
        tracing::warn!(error = %e, "create archive dir failed");
        return (None, None);
    }
    let dest = archive_dir.join(format!("{job_id}.sqlite"));
    match std::fs::copy(src, &dest) {
        Ok(bytes) => (Some(dest), Some(bytes)),
        Err(e) => {
            tracing::warn!(error = %e, src = %src.display(), "sqlite archive copy failed");
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
    fn archive_sqlite_copies_to_archive_dir() {
        let dir = TempDir::new().unwrap();
        let src = dir.path().join("run.sqlite");
        let mut f = std::fs::File::create(&src).unwrap();
        f.write_all(b"sqlite-data")
            .unwrap();
        let archive = dir.path().join("archive");

        let (path, size) = archive_sqlite(&src, &archive, "job1");
        assert_eq!(
            path.as_deref(),
            Some(
                archive
                    .join("job1.sqlite")
                    .as_path()
            )
        );
        assert_eq!(size, Some(b"sqlite-data".len() as u64));
        assert!(
            archive
                .join("job1.sqlite")
                .exists()
        );
    }
}
