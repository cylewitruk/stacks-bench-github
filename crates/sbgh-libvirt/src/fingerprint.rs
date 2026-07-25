use std::path::Path;

pub(crate) fn toolchain_channel(toolchain_toml: &str) -> Option<String> {
    let value: toml::Value = toml::from_str(toolchain_toml).ok()?;
    let channel = value
        .get("toolchain")?
        .get("channel")?
        .as_str()?
        .trim()
        .to_string();
    (!channel.is_empty()).then_some(channel)
}

pub(crate) fn legacy_toolchain_channel(toolchain: &str) -> Option<String> {
    let channel = toolchain
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())?;
    (!channel.contains(char::is_whitespace)).then(|| channel.to_string())
}

pub(crate) fn image_proxy_id(image_path: &Path) -> std::io::Result<String> {
    let metadata = std::fs::metadata(image_path)?;
    let modified = metadata
        .modified()
        .ok()
        .and_then(|time| {
            time.duration_since(std::time::UNIX_EPOCH)
                .ok()
        })
        .map(|duration| duration.as_secs())
        .unwrap_or(0);
    Ok(format!("{}:{modified}", metadata.len()))
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn toolchain_channel_reads_the_declared_channel() {
        for channel in ["1.95.0", "1.85", "stable", "beta", "nightly", "nightly-2026-01-01"] {
            let contents = format!("[toolchain]\nchannel = \"{channel}\"\n");
            assert_eq!(toolchain_channel(&contents).as_deref(), Some(channel), "channel {channel}",);
        }
        assert!(toolchain_channel("[toolchain]\n").is_none(), "no channel key");
        assert!(toolchain_channel("not toml {{{").is_none(), "garbage");
    }

    #[test]
    fn legacy_toolchain_channel_reads_single_line_declaration() {
        for channel in ["1.95.0", "stable", "nightly-2026-01-01"] {
            let contents = format!("{channel}\n");
            assert_eq!(
                legacy_toolchain_channel(&contents).as_deref(),
                Some(channel),
                "channel {channel}",
            );
        }
        assert_eq!(legacy_toolchain_channel("\n  stable  \n").as_deref(), Some("stable"));
        assert!(legacy_toolchain_channel("").is_none(), "empty");
        assert!(legacy_toolchain_channel("stable extra").is_none(), "not a single channel token",);
    }

    #[test]
    fn image_proxy_id_changes_with_content() {
        let tmp = TempDir::new().unwrap();
        let image = tmp
            .path()
            .join("golden.qcow2");
        std::fs::write(&image, b"aaaa").unwrap();
        let first = image_proxy_id(&image).unwrap();
        std::fs::write(&image, b"aaaaaaaaaaaa").unwrap();
        let second = image_proxy_id(&image).unwrap();
        assert_ne!(first, second, "a size change must change the image id");
        assert!(image_proxy_id(&tmp.path().join("absent")).is_err());
    }
}
