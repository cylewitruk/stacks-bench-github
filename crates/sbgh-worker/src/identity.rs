use std::io::Write;
use std::path::Path;

use anyhow::{Context, ensure};
use p256::elliptic_curve::rand_core::OsRng;
use p256::pkcs8::{DecodePrivateKey, EncodePrivateKey, EncodePublicKey, LineEnding};

pub fn generate(path: &Path) -> anyhow::Result<String> {
    let secret = p256::SecretKey::random(&mut OsRng);
    let pem = secret
        .to_pkcs8_pem(LineEnding::LF)
        .context("encoding worker identity")?;
    let mut options = std::fs::OpenOptions::new();
    options
        .write(true)
        .create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .with_context(|| format!("creating worker identity {}", path.display()))?;
    file.write_all(pem.as_bytes())
        .with_context(|| format!("writing worker identity {}", path.display()))?;
    public(path)
}

pub fn public(path: &Path) -> anyhow::Result<String> {
    require_private_permissions(path)?;
    let pem = std::fs::read_to_string(path)
        .with_context(|| format!("reading worker identity {}", path.display()))?;
    let secret = p256::SecretKey::from_pkcs8_pem(&pem).context("parsing P-256 worker identity")?;
    secret
        .public_key()
        .to_public_key_pem(LineEnding::LF)
        .context("encoding worker public identity")
}

pub fn require_private_permissions(path: &Path) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(path)
            .with_context(|| format!("stat worker identity {}", path.display()))?
            .permissions()
            .mode();
        ensure!(
            mode & 0o077 == 0,
            "worker identity {} must not be accessible by group/other",
            path.display()
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generation_is_create_new_and_mode_0600() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory
            .path()
            .join("identity.key");
        let public_key = generate(&path).unwrap();
        assert!(public_key.contains("BEGIN PUBLIC KEY"));
        assert!(generate(&path).is_err());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(path)
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
    }
}
