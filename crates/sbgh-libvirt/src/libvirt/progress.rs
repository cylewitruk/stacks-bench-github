//! Best-effort tailing for `stacks-bench --json` stderr JSONL progress files.
//!
//! v20 Phase 1 only captures and tails raw lines. Parsing and report-surface
//! events land in the later phases.

use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::libvirt::guest_file;

const MAX_PROGRESS_FILE_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug)]
pub struct ProgressTailer {
    path: PathBuf,
    offset: u64,
    pending: Vec<u8>,
}

impl ProgressTailer {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            offset: 0,
            pending: Vec::new(),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Return newly completed lines. Missing files are normal during VM startup
    /// and for older `stacks-bench` builds that do not emit progress JSONL.
    pub fn drain(&mut self, final_drain: bool) -> std::io::Result<Vec<String>> {
        let mut file = match guest_file::open_regular(&self.path) {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };

        let len = file.metadata()?.len();
        if len > MAX_PROGRESS_FILE_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "guest progress file exceeds {MAX_PROGRESS_FILE_BYTES} bytes: {}",
                    self.path.display()
                ),
            ));
        }
        if len < self.offset {
            self.offset = 0;
            self.pending.clear();
        }

        file.seek(SeekFrom::Start(self.offset))?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        self.offset += bytes.len() as u64;
        self.pending.extend(bytes);

        let mut lines = Vec::new();
        while let Some(pos) = self
            .pending
            .iter()
            .position(|b| *b == b'\n')
        {
            let mut line = self
                .pending
                .drain(..=pos)
                .collect::<Vec<_>>();
            if line.last() == Some(&b'\n') {
                line.pop();
            }
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            lines.push(String::from_utf8_lossy(&line).into_owned());
        }

        if final_drain && !self.pending.is_empty() {
            let line = std::mem::take(&mut self.pending);
            lines.push(String::from_utf8_lossy(&line).into_owned());
        }

        Ok(lines)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn missing_file_is_empty() {
        let dir = TempDir::new().unwrap();
        let mut tailer = ProgressTailer::new(
            dir.path()
                .join("missing.jsonl"),
        );

        assert!(
            tailer
                .drain(false)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn drains_incrementally_and_keeps_partial_until_final() {
        let dir = TempDir::new().unwrap();
        let path = dir
            .path()
            .join("progress.jsonl");
        std::fs::write(&path, b"{\"a\":1}\n{\"b\"").unwrap();
        let mut tailer = ProgressTailer::new(&path);

        assert_eq!(tailer.drain(false).unwrap(), vec!["{\"a\":1}"]);
        std::fs::write(&path, b"{\"a\":1}\n{\"b\":2}\n{\"c\":3}").unwrap();
        assert_eq!(tailer.drain(false).unwrap(), vec!["{\"b\":2}"]);
        assert_eq!(tailer.drain(true).unwrap(), vec!["{\"c\":3}"]);
    }

    #[test]
    fn file_truncation_resets_offset() {
        let dir = TempDir::new().unwrap();
        let path = dir
            .path()
            .join("progress.jsonl");
        std::fs::write(&path, b"old-long\n").unwrap();
        let mut tailer = ProgressTailer::new(&path);

        assert_eq!(tailer.drain(false).unwrap(), vec!["old-long"]);
        std::fs::write(&path, b"new\n").unwrap();
        assert_eq!(tailer.drain(false).unwrap(), vec!["new"]);
    }
}
