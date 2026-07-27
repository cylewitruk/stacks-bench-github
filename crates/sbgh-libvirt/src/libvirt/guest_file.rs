//! Safe reads of files written through the guest-visible results share.
//!
//! Repository code runs as root inside the disposable VM and can replace a
//! result filename with a symlink. Host ingestion therefore opens the final
//! path component with `O_NOFOLLOW` and accepts regular files only.

use std::fs::{File, OpenOptions};
use std::io::{self, Read};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

pub fn open_regular(path: &Path) -> io::Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    if !file.metadata()?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("guest output is not a regular file: {}", path.display()),
        ));
    }
    Ok(file)
}

/// Verify a terminal guest output resolves beneath its trusted share root.
/// Call only after the VM is stopped, so the canonical-path check and later
/// ingestion cannot race a guest rename.
pub fn verify_beneath(root: &Path, path: &Path) -> io::Result<()> {
    let root = root.canonicalize()?;
    let resolved = path.canonicalize()?;
    if !resolved.starts_with(&root) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "guest output escapes results root: {} -> {}",
                path.display(),
                resolved.display()
            ),
        ));
    }
    open_regular(path).map(|_| ())
}

pub fn read_bounded(path: &Path, max_bytes: u64) -> io::Result<Vec<u8>> {
    let mut file = open_regular(path)?;
    let mut bytes = Vec::new();
    file.by_ref()
        .take(max_bytes.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > max_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("guest output exceeds {max_bytes} bytes: {}", path.display()),
        ));
    }
    Ok(bytes)
}

pub fn read_to_string_bounded(path: &Path, max_bytes: u64) -> io::Result<String> {
    String::from_utf8(read_bounded(path, max_bytes)?)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::symlink;

    use tempfile::TempDir;

    use super::*;

    #[test]
    fn rejects_a_guest_controlled_symlink() {
        let directory = TempDir::new().unwrap();
        let target = directory
            .path()
            .join("target");
        let link = directory
            .path()
            .join("result");
        std::fs::write(&target, b"secret").unwrap();
        symlink(&target, &link).unwrap();

        assert!(open_regular(&link).is_err());
        assert!(open_regular(&target).is_ok());
    }

    #[test]
    fn rejects_an_intermediate_symlink_that_escapes_the_share() {
        let directory = TempDir::new().unwrap();
        let share = directory.path().join("share");
        let outside = directory
            .path()
            .join("outside");
        std::fs::create_dir_all(&share).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::write(outside.join("result"), b"secret").unwrap();
        symlink(&outside, share.join("nested")).unwrap();

        assert!(verify_beneath(&share, &share.join("nested/result")).is_err());
    }

    #[test]
    fn bounded_read_rejects_oversized_guest_output() {
        let directory = TempDir::new().unwrap();
        let result = directory
            .path()
            .join("result");
        std::fs::write(&result, b"12345").unwrap();

        assert!(read_bounded(&result, 4).is_err());
        assert_eq!(read_bounded(&result, 5).unwrap(), b"12345");
    }
}
