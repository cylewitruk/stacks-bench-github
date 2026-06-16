pub mod admin;
pub mod bench_args;
pub mod config;
pub mod db;
pub mod error;
pub mod github;
pub mod memory;
pub mod models;

pub use error::{Error, Result};

/// Install the process-wide rustls crypto provider (`ring`).
///
/// Our dependency tree pulls **both** rustls providers — `aws-lc-rs` (via
/// reqwest) and `ring` (via slack-morphism / sqlx) — and Cargo unifies them
/// onto one rustls, so it can't auto-select a process-level default and panics
/// at the first TLS handshake. Every binary must call this once, before any
/// TLS, to pin the provider. Idempotent: a second call (or one already
/// installed) is a no-op.
pub fn install_default_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

#[cfg(test)]
mod tests {
    /// Before the install, rustls has no process default and auto-detection
    /// panics (both providers compiled in); after it, `get_default` is `Some`.
    #[test]
    fn installs_a_process_default_crypto_provider() {
        super::install_default_crypto_provider();
        assert!(rustls::crypto::CryptoProvider::get_default().is_some());
    }
}
