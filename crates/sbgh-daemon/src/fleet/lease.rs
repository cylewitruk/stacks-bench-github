use std::path::Path;

use anyhow::{Context, ensure};
use hmac::{Hmac, KeyInit, Mac};
use sbgh_proto::{AttemptIdentity, LeaseToken};
use sha2::Sha256;
use subtle::ConstantTimeEq;
use uuid::Uuid;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
pub struct LeaseSigner {
    key: Vec<u8>,
}

impl std::fmt::Debug for LeaseSigner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("LeaseSigner([REDACTED])")
    }
}

impl LeaseSigner {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let metadata = std::fs::metadata(path)
                .with_context(|| format!("stat lease HMAC key {}", path.display()))?;
            ensure!(
                metadata.permissions().mode() & 0o077 == 0,
                "lease HMAC key {} must not be accessible by group/other",
                path.display()
            );
        }
        let key = std::fs::read(path)
            .with_context(|| format!("reading lease HMAC key {}", path.display()))?;
        ensure!(key.len() >= 32, "lease HMAC key must contain at least 32 bytes");
        Ok(Self { key })
    }

    #[cfg(test)]
    pub fn from_bytes(key: &[u8]) -> anyhow::Result<Self> {
        ensure!(key.len() >= 32, "lease HMAC key must contain at least 32 bytes");
        Ok(Self { key: key.to_vec() })
    }

    pub fn issue(
        &self,
        worker_id: Uuid,
        worker_session_id: Uuid,
        attempt_id: Uuid,
        fencing_generation: u64,
    ) -> LeaseToken {
        let bytes = signing_bytes(worker_id, worker_session_id, attempt_id, fencing_generation);
        let mut mac = HmacSha256::new_from_slice(&self.key).expect("HMAC accepts arbitrary keys");
        mac.update(&bytes);
        LeaseToken(hex::encode(mac.finalize().into_bytes()))
    }

    pub fn verify(&self, worker_id: Uuid, identity: &AttemptIdentity) -> bool {
        let expected = self.issue(
            worker_id,
            identity.worker_session_id,
            identity.attempt_id,
            identity.fencing_generation,
        );
        expected
            .0
            .as_bytes()
            .ct_eq(
                identity
                    .lease_token
                    .0
                    .as_bytes(),
            )
            .into()
    }
}

fn signing_bytes(
    worker_id: Uuid,
    worker_session_id: Uuid,
    attempt_id: Uuid,
    fencing_generation: u64,
) -> [u8; 56] {
    let mut bytes = [0_u8; 56];
    bytes[..16].copy_from_slice(worker_id.as_bytes());
    bytes[16..32].copy_from_slice(worker_session_id.as_bytes());
    bytes[32..48].copy_from_slice(attempt_id.as_bytes());
    bytes[48..].copy_from_slice(&fencing_generation.to_be_bytes());
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_is_bound_to_every_attempt_coordinate() {
        let signer = LeaseSigner::from_bytes(&[7; 32]).unwrap();
        let worker = Uuid::new_v4();
        let mut identity = AttemptIdentity {
            worker_session_id: Uuid::new_v4(),
            attempt_id: Uuid::new_v4(),
            fencing_generation: 1,
            lease_token: LeaseToken(String::new()),
        };
        identity.lease_token = signer.issue(
            worker,
            identity.worker_session_id,
            identity.attempt_id,
            identity.fencing_generation,
        );
        assert!(signer.verify(worker, &identity));
        assert!(!signer.verify(Uuid::new_v4(), &identity));
        identity.fencing_generation += 1;
        assert!(!signer.verify(worker, &identity));
    }
}
