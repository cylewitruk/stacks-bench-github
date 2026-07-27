use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, ensure};
use sbgh_proto::{
    DatasetIdentity, MAX_LONG_POLL_SECS, MAX_VALIDATION_CONCURRENCY, MAX_VALIDATION_SHARDS,
    MAX_VALIDATION_TIMEOUT_SECS, Validate, WorkerCapability,
};
use serde::Deserialize;
use uuid::Uuid;

const CONFIG_ENV: &str = "SBGH_FLEET_CONFIG";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetConfig {
    pub listen: SocketAddr,
    pub server_certificate: PathBuf,
    pub server_private_key: PathBuf,
    pub client_ca_certificate: PathBuf,
    pub lease_hmac_key: PathBuf,
    #[serde(default = "default_heartbeat_seconds")]
    pub heartbeat_seconds: u64,
    #[serde(default = "default_lease_seconds")]
    pub lease_seconds: u64,
    #[serde(default = "default_offer_seconds")]
    pub offer_seconds: u64,
    #[serde(default = "default_session_seconds")]
    pub session_seconds: u64,
    #[serde(default = "default_long_poll_seconds")]
    pub long_poll_seconds: u64,
    #[serde(default = "default_request_timeout_seconds")]
    pub request_timeout_seconds: u64,
    #[serde(default = "default_upload_grant_seconds")]
    pub upload_grant_seconds: u64,
    #[serde(default = "default_staging_gc_grace_seconds")]
    pub staging_gc_grace_seconds: u64,
    #[serde(default = "default_max_artifact_bytes")]
    pub max_artifact_bytes: u64,
    #[serde(default = "default_max_attempt_artifact_bytes")]
    pub max_attempt_artifact_bytes: u64,
    /// Optional bounded `/validate-blocks <epoch> <start> <end>` PR-comment
    /// trigger. Dataset identity remains server-owned and is never accepted
    /// from the comment.
    pub github_block_validation: Option<GitHubBlockValidationConfig>,
    pub workers: Vec<ConfiguredWorker>,
    #[serde(default)]
    pub datasets: Vec<ConfiguredDataset>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitHubBlockValidationConfig {
    pub worker_id: Uuid,
    pub network: String,
    pub requested_shards: u32,
    pub max_concurrency: u32,
    pub timeout_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfiguredWorker {
    pub id: Uuid,
    pub display_name: String,
    /// Lowercase SHA-256 fingerprints of client certificates currently
    /// authorized for this worker identity. Two entries provide a bounded
    /// overlap window during rotation.
    pub certificate_sha256: BTreeSet<String>,
    pub capabilities: BTreeSet<WorkerCapability>,
    pub measurement_profile: Option<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub draining: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfiguredDataset {
    pub worker_id: Uuid,
    #[serde(default)]
    pub current: bool,
    #[serde(flatten)]
    pub identity: DatasetIdentity,
}

impl FleetConfig {
    pub fn load_from_env() -> anyhow::Result<Self> {
        let path = std::env::var_os(CONFIG_ENV)
            .map(PathBuf::from)
            .context(format!("{CONFIG_ENV} must name the fleet configuration file"))?;
        Self::load(&path)
    }

    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let bytes = std::fs::read(path)
            .with_context(|| format!("reading fleet configuration {}", path.display()))?;
        let text = std::str::from_utf8(&bytes)
            .with_context(|| format!("fleet configuration {} is not UTF-8", path.display()))?;
        let config: Self = toml::from_str(text)
            .with_context(|| format!("parsing fleet configuration {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(self.heartbeat_seconds > 0, "heartbeat_seconds must be non-zero");
        ensure!(
            self.lease_seconds
                >= self
                    .heartbeat_seconds
                    .saturating_mul(3),
            "lease_seconds must allow at least three heartbeat opportunities"
        );
        ensure!(self.offer_seconds > 0, "offer_seconds must be non-zero");
        ensure!(
            self.session_seconds
                >= self
                    .lease_seconds
                    .saturating_mul(2),
            "session_seconds must cover two lease intervals"
        );
        ensure!(self.long_poll_seconds > 0, "long_poll_seconds must be non-zero");
        ensure!(
            self.long_poll_seconds <= MAX_LONG_POLL_SECS,
            "long_poll_seconds exceeds protocol maximum {MAX_LONG_POLL_SECS}"
        );
        ensure!(
            self.request_timeout_seconds > self.long_poll_seconds,
            "request_timeout_seconds must exceed long_poll_seconds"
        );
        ensure!(self.upload_grant_seconds > 0, "upload_grant_seconds must be non-zero");
        ensure!(
            self.staging_gc_grace_seconds >= self.upload_grant_seconds
                && self.staging_gc_grace_seconds <= i64::MAX as u64,
            "staging_gc_grace_seconds must cover one upload grant and fit DB time"
        );
        ensure!(self.max_artifact_bytes > 0, "max_artifact_bytes must be non-zero");
        ensure!(
            self.max_attempt_artifact_bytes >= self.max_artifact_bytes,
            "max_attempt_artifact_bytes must cover one artifact"
        );
        ensure!(!self.workers.is_empty(), "at least one worker must be registered");
        let mut ids = BTreeSet::new();
        for worker in &self.workers {
            ensure!(!worker.id.is_nil(), "configured worker ID must not be nil");
            ensure!(ids.insert(worker.id), "duplicate configured worker {}", worker.id);
            ensure!(
                !worker
                    .display_name
                    .trim()
                    .is_empty(),
                "worker {} has an empty display name",
                worker.id
            );
            ensure!(!worker.capabilities.is_empty(), "worker {} has no capabilities", worker.id);
            ensure!(
                !worker
                    .certificate_sha256
                    .is_empty(),
                "worker {} has no authorized certificate fingerprint",
                worker.id
            );
            ensure!(
                worker
                    .certificate_sha256
                    .iter()
                    .all(|fingerprint| {
                        fingerprint.len() == 64
                            && fingerprint
                                .bytes()
                                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                    }),
                "worker {} has an invalid SHA-256 certificate fingerprint",
                worker.id
            );
            if worker
                .capabilities
                .contains(&WorkerCapability::Benchmark)
            {
                ensure!(
                    worker
                        .measurement_profile
                        .as_deref()
                        .is_some_and(|value| !value.is_empty()),
                    "benchmark worker {} requires measurement_profile",
                    worker.id
                );
            }
        }
        let mut generations = BTreeSet::new();
        let mut current = BTreeSet::new();
        for dataset in &self.datasets {
            dataset
                .identity
                .validate()
                .map_err(anyhow::Error::new)
                .context("invalid configured dataset identity")?;
            ensure!(
                generations.insert(
                    dataset
                        .identity
                        .generation
                        .clone()
                ),
                "duplicate configured dataset generation {}",
                dataset.identity.generation
            );
            let worker = self
                .workers
                .iter()
                .find(|worker| worker.id == dataset.worker_id)
                .context("configured dataset worker_id is not registered")?;
            ensure!(
                worker
                    .capabilities
                    .contains(&WorkerCapability::BlockValidation),
                "dataset {} belongs to a worker without block_validation capability",
                dataset.identity.generation
            );
            if dataset.current {
                ensure!(
                    current.insert((
                        dataset.worker_id,
                        dataset
                            .identity
                            .network
                            .clone()
                    )),
                    "more than one current dataset is configured for worker {} network {}",
                    dataset.worker_id,
                    dataset.identity.network
                );
            }
        }
        for worker in self
            .workers
            .iter()
            .filter(|worker| {
                worker.enabled
                    && worker
                        .capabilities
                        .contains(&WorkerCapability::BlockValidation)
            })
        {
            ensure!(
                self.datasets
                    .iter()
                    .any(|dataset| dataset.worker_id == worker.id && dataset.current),
                "enabled block-validation worker {} requires a current configured dataset",
                worker.id
            );
        }
        if let Some(trigger) = &self.github_block_validation {
            ensure!(
                trigger.requested_shards > 0
                    && trigger.max_concurrency > 0
                    && trigger.timeout_secs > 0,
                "github_block_validation limits must be non-zero"
            );
            ensure!(
                trigger.requested_shards <= MAX_VALIDATION_SHARDS
                    && trigger.max_concurrency <= MAX_VALIDATION_CONCURRENCY
                    && trigger.max_concurrency <= trigger.requested_shards
                    && trigger.timeout_secs <= MAX_VALIDATION_TIMEOUT_SECS,
                "github_block_validation limits exceed protocol bounds"
            );
            ensure!(
                !trigger
                    .network
                    .trim()
                    .is_empty(),
                "github_block_validation network must not be empty"
            );
            let worker = self
                .workers
                .iter()
                .find(|worker| worker.id == trigger.worker_id)
                .context("github_block_validation worker_id is not registered")?;
            ensure!(
                worker.enabled
                    && worker
                        .capabilities
                        .contains(&WorkerCapability::BlockValidation),
                "github_block_validation worker must be enabled and authorized for block_validation"
            );
        }
        Ok(())
    }

    pub fn heartbeat_interval(&self) -> Duration {
        Duration::from_secs(self.heartbeat_seconds)
    }

    pub fn lease_ttl(&self) -> Duration {
        Duration::from_secs(self.lease_seconds)
    }

    pub fn offer_ttl(&self) -> Duration {
        Duration::from_secs(self.offer_seconds)
    }

    pub fn session_ttl(&self) -> Duration {
        Duration::from_secs(self.session_seconds)
    }

    pub fn long_poll_timeout(&self) -> Duration {
        Duration::from_secs(self.long_poll_seconds)
    }

    pub fn upload_grant_ttl(&self) -> Duration {
        Duration::from_secs(self.upload_grant_seconds)
    }

    pub fn staging_gc_grace(&self) -> Duration {
        Duration::from_secs(self.staging_gc_grace_seconds)
    }

    pub fn request_timeout(&self) -> Duration {
        Duration::from_secs(self.request_timeout_seconds)
    }
}

const fn default_heartbeat_seconds() -> u64 {
    10
}

const fn default_lease_seconds() -> u64 {
    45
}

const fn default_offer_seconds() -> u64 {
    30
}

const fn default_session_seconds() -> u64 {
    90
}

const fn default_long_poll_seconds() -> u64 {
    20
}

const fn default_upload_grant_seconds() -> u64 {
    300
}

const fn default_staging_gc_grace_seconds() -> u64 {
    24 * 60 * 60
}

const fn default_request_timeout_seconds() -> u64 {
    120
}

const fn default_max_artifact_bytes() -> u64 {
    8 * 1024 * 1024 * 1024
}

const fn default_max_attempt_artifact_bytes() -> u64 {
    32 * 1024 * 1024 * 1024
}

const fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lease_must_allow_three_heartbeats() {
        let config = FleetConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            server_certificate: "server.pem".into(),
            server_private_key: "server.key".into(),
            client_ca_certificate: "ca.pem".into(),
            lease_hmac_key: "lease.key".into(),
            heartbeat_seconds: 10,
            lease_seconds: 29,
            offer_seconds: 30,
            session_seconds: 90,
            long_poll_seconds: 20,
            upload_grant_seconds: 300,
            staging_gc_grace_seconds: 86_400,
            request_timeout_seconds: 120,
            max_artifact_bytes: 1,
            max_attempt_artifact_bytes: 1,
            github_block_validation: None,
            workers: vec![ConfiguredWorker {
                id: Uuid::new_v4(),
                display_name: "worker".into(),
                certificate_sha256: BTreeSet::from(["00".repeat(32)]),
                capabilities: BTreeSet::from([WorkerCapability::BuildOnly]),
                measurement_profile: None,
                enabled: true,
                draining: false,
            }],
            datasets: Vec::new(),
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn checked_in_fleet_example_stays_parseable() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config.example.fleet.toml");
        FleetConfig::load(&path).unwrap();
    }

    #[test]
    fn long_poll_must_fit_the_transport_budget() {
        let mut config = FleetConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            server_certificate: "server.pem".into(),
            server_private_key: "server.key".into(),
            client_ca_certificate: "ca.pem".into(),
            lease_hmac_key: "lease.key".into(),
            heartbeat_seconds: 10,
            lease_seconds: 45,
            offer_seconds: 30,
            session_seconds: 90,
            long_poll_seconds: MAX_LONG_POLL_SECS + 1,
            upload_grant_seconds: 300,
            staging_gc_grace_seconds: 86_400,
            request_timeout_seconds: 120,
            max_artifact_bytes: 1,
            max_attempt_artifact_bytes: 1,
            github_block_validation: None,
            workers: vec![ConfiguredWorker {
                id: Uuid::new_v4(),
                display_name: "worker".into(),
                certificate_sha256: BTreeSet::from(["00".repeat(32)]),
                capabilities: BTreeSet::from([WorkerCapability::BuildOnly]),
                measurement_profile: None,
                enabled: true,
                draining: false,
            }],
            datasets: Vec::new(),
        };
        assert!(config.validate().is_err());
        config.long_poll_seconds = MAX_LONG_POLL_SECS;
        config.validate().unwrap();
    }
}
