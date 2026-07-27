use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use sbgh_driver::ArtifactSink;
use sbgh_proto::{
    ArtifactDescriptor, ArtifactGrantRequest, ArtifactOperation, AttemptIdentity, PROTOCOL_VERSION,
};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::transport::FleetClient;

pub struct RemoteArtifactSink {
    client: FleetClient,
    identity: AttemptIdentity,
    local_root: PathBuf,
    artifacts: Mutex<Vec<ArtifactDescriptor>>,
}

impl RemoteArtifactSink {
    pub fn new(client: FleetClient, identity: AttemptIdentity, local_root: PathBuf) -> Arc<Self> {
        Arc::new(Self {
            client,
            identity,
            local_root,
            artifacts: Mutex::new(Vec::new()),
        })
    }

    pub async fn manifest(&self) -> Vec<ArtifactDescriptor> {
        self.artifacts
            .lock()
            .await
            .clone()
    }

    fn local_path(&self, key: &str) -> Option<PathBuf> {
        let path = Path::new(key);
        if path
            .components()
            .all(|component| matches!(component, std::path::Component::Normal(_)))
        {
            Some(self.local_root.join(path))
        } else {
            None
        }
    }

    async fn record_artifact(&self, descriptor: ArtifactDescriptor) -> bool {
        let mut manifest = self.artifacts.lock().await;
        record_manifest_entry(&mut manifest, descriptor)
    }
}

fn record_manifest_entry(
    manifest: &mut Vec<ArtifactDescriptor>,
    descriptor: ArtifactDescriptor,
) -> bool {
    match manifest
        .iter()
        .find(|artifact| artifact.logical_key == descriptor.logical_key)
    {
        Some(existing) => existing == &descriptor,
        None => {
            manifest.push(descriptor);
            true
        }
    }
}

#[async_trait]
impl ArtifactSink for RemoteArtifactSink {
    async fn put(&self, key: &str, src: &Path) -> Option<u64> {
        let size = tokio::fs::metadata(src)
            .await
            .ok()?
            .len();
        let sha256 = sha256_file(src).await.ok()?;
        let grant = self
            .client
            .artifact_grant(&ArtifactGrantRequest {
                protocol_version: PROTOCOL_VERSION,
                identity: self.identity.clone(),
                operation: ArtifactOperation::Put,
                key: key.into(),
                size: Some(size),
                sha256: Some(sha256.clone()),
            })
            .await
            .ok()?;
        self.client
            .upload(&grant, src)
            .await
            .ok()?;
        let descriptor = ArtifactDescriptor {
            key: grant.key,
            logical_key: key.into(),
            size,
            sha256,
        };
        if !self
            .record_artifact(descriptor)
            .await
        {
            tracing::error!(
                attempt_id = %self.identity.attempt_id,
                logical_key = key,
                "artifact retry attempted to rebind a logical manifest key"
            );
            return None;
        }
        Some(size)
    }

    async fn put_local_only(&self, key: &str, src: &Path) -> Option<u64> {
        // Fleet workers are disposable execution hosts. A worker-only
        // forensic copy would be invisible to the orchestrator, so archive it
        // through the same delegated object write.
        self.put(key, src).await
    }

    async fn get(&self, key: &str) -> std::io::Result<PathBuf> {
        let destination = self
            .local_path(key)
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "unsafe artifact key")
            })?;
        if destination.is_file() {
            return Ok(destination);
        }
        let grant = self
            .client
            .artifact_grant(&ArtifactGrantRequest {
                protocol_version: PROTOCOL_VERSION,
                identity: self.identity.clone(),
                operation: ArtifactOperation::Get,
                key: key.into(),
                size: None,
                sha256: None,
            })
            .await
            .map_err(std::io::Error::other)?;
        self.client
            .download(&grant, &destination)
            .await
            .map_err(std::io::Error::other)?;
        Ok(destination)
    }

    fn job_dir(&self, job_id: &str) -> PathBuf {
        self.local_root.join(job_id)
    }
}

async fn sha256_file(path: &Path) -> std::io::Result<String> {
    use tokio::io::AsyncReadExt;

    let mut file = tokio::fs::File::open(path).await?;
    let mut hash = Sha256::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        hash.update(&buffer[..read]);
    }
    Ok(hex::encode(hash.finalize()))
}

#[cfg(test)]
mod tests {
    use sbgh_proto::ArtifactDescriptor;

    use super::record_manifest_entry;

    fn descriptor(key: &str, digest: &str) -> ArtifactDescriptor {
        ArtifactDescriptor {
            key: format!("staging/attempt/{digest}/{key}"),
            logical_key: key.into(),
            size: 17,
            sha256: digest.into(),
        }
    }

    #[test]
    fn identical_artifact_retries_do_not_duplicate_the_terminal_manifest() {
        let mut manifest = Vec::new();
        let artifact = descriptor("job/run.json", &"ab".repeat(32));
        assert!(record_manifest_entry(&mut manifest, artifact.clone()));
        assert!(record_manifest_entry(&mut manifest, artifact));
        assert_eq!(manifest.len(), 1);
    }

    #[test]
    fn logical_manifest_key_cannot_be_rebound() {
        let mut manifest = Vec::new();
        assert!(
            record_manifest_entry(&mut manifest, descriptor("job/run.json", &"ab".repeat(32)),)
        );
        assert!(!record_manifest_entry(
            &mut manifest,
            descriptor("job/run.json", &"cd".repeat(32)),
        ));
        assert_eq!(manifest.len(), 1);
    }
}
