//! Successful startup check provenance retained unchanged through recovery.

use std::env;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use sbgh_fleet::PreflightReport;
use sha2::{Digest, Sha256};

use crate::{HostResources, WorkerConfig, preflight_local_execution};

/// Check configured recipes and identify the exact executable and loaded config.
/// The report is self-reported provenance, not attestation or continuing health.
pub async fn preflight_report(
    config: &WorkerConfig,
    resources: &HostResources,
) -> anyhow::Result<PreflightReport> {
    config
        .validate_host_resources(resources)
        .context("validating worker profiles against host resources")?;
    let configuration_sha256 = configuration_digest(config)?;
    let executable_sha256 = tokio::task::spawn_blocking(executable_digest).await??;
    preflight_local_execution(config).await?;
    let report = PreflightReport {
        revision: 1,
        completed_at_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_millis()
            .try_into()?,
        executable_sha256,
        configuration_sha256,
    };
    tracing::info!(
        preflight_revision = report.revision,
        completed_at_ms = report.completed_at_ms,
        executable_sha256 = %report.executable_sha256,
        configuration_sha256 = %report.configuration_sha256,
        "local worker execution preflight passed"
    );
    Ok(report)
}

/// Fingerprint loaded values, including defaults, without opening key files.
fn configuration_digest(config: &WorkerConfig) -> anyhow::Result<String> {
    let mut digest = Sha256::new();
    digest.update(b"sbgh-worker-preflight-config-v1\0");
    digest.update(serde_json::to_vec(config)?);
    Ok(hex::encode(digest.finalize()))
}

/// Linux's proc handle identifies this image even after atomic replacement.
fn executable_digest() -> anyhow::Result<String> {
    let executable = if cfg!(target_os = "linux") {
        "/proc/self/exe".into()
    } else {
        env::current_exe().context("locating running worker executable")?
    };
    file_digest(&executable).context("hashing running worker executable")
}

/// Stream a binary into the digest without retaining its contents in memory.
fn file_digest(path: &Path) -> anyhow::Result<String> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0; 64 * 1024];
    loop {
        let length = file.read(&mut buffer)?;
        if length == 0 {
            break;
        }
        digest.update(&buffer[..length]);
    }
    Ok(hex::encode(digest.finalize()))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Write;

    use super::*;

    #[test]
    fn executable_hash_streams_exact_bytes() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"abc")
            .unwrap();
        assert_eq!(
            file_digest(file.path()).unwrap(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn preflight_config_digest_tracks_loaded_values_without_reading_private_keys() {
        let path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config.example.worker-combined.toml");
        let text = fs::read_to_string(path).unwrap();
        let config: WorkerConfig = toml::from_str(&text).unwrap();
        let commented: WorkerConfig =
            toml::from_str(&format!("# formatting is not configuration\n{text}\n")).unwrap();
        let initial = configuration_digest(&config).unwrap();
        assert_eq!(initial, configuration_digest(&commented).unwrap());
        let mut changed = config.clone();
        changed
            .lvm
            .chainstate_base_prefix = "other-".into();
        assert_ne!(initial, configuration_digest(&changed).unwrap());

        let mut key = tempfile::NamedTempFile::new().unwrap();
        changed.identity_private_key = key.path().to_owned();
        let before_key_change = configuration_digest(&changed).unwrap();
        key.write_all(b"private-key-content-must-not-be-part-of-config-fingerprint")
            .unwrap();
        assert_eq!(before_key_change, configuration_digest(&changed).unwrap());
    }

    #[tokio::test]
    async fn failed_resource_preflight_cannot_produce_success_evidence() {
        let config = WorkerConfig::load(
            &Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../config.example.worker-combined.toml"),
        )
        .unwrap();
        let resources = HostResources::new([0].into(), 1024).unwrap();
        let error = preflight_report(&config, &resources)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("validating worker profiles against host resources")
        );
    }
}
