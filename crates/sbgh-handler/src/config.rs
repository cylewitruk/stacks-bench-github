use std::env;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const DEFAULT_CONFIG_PATH: &str = "/etc/sbgh/handler/config.toml";
const HOME_RELATIVE_CONFIG_PATH: &str = ".config/sbgh/handler/config.toml";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandlerConfig {
    pub server: ServerConfig,
    pub webhook: WebhookConfig,
    pub api: HandlerApiConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub bind_addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookConfig {
    pub secret: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandlerApiConfig {
    pub url: String,
    /// Environment-only shared token for the daemon's ingest scope.
    pub ingest_token: String,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("configuration error: {0}")]
    Config(String),
    #[error("reading configuration: {0}")]
    Io(#[from] std::io::Error),
}

impl HandlerConfig {
    pub fn load() -> Result<Self, ConfigError> {
        let path = resolve_config_path();
        Self::load_layered(path.as_deref())
    }

    pub fn load_layered(file: Option<&Path>) -> Result<Self, ConfigError> {
        let mut raw = RawHandler::default();
        if let Some(path) = file
            && path.exists()
        {
            let body = std::fs::read_to_string(path)?;
            let from_file: RawHandler = toml::from_str(&body)
                .map_err(|e| ConfigError::Config(format!("parsing {}: {e}", path.display())))?;
            raw.merge(from_file);
        }
        raw.apply_env();
        raw.into_config()
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawHandler {
    server: RawServer,
    webhook: RawWebhook,
    api: RawHandlerApi,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawServer {
    bind_addr: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawWebhook {
    secret: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawHandlerApi {
    url: Option<String>,
    /// The ingest token must never be accepted from the bind-mounted file.
    #[serde(skip)]
    ingest_token: Option<String>,
}

impl RawHandler {
    fn merge(&mut self, other: Self) {
        merge_opt(&mut self.server.bind_addr, other.server.bind_addr);
        merge_opt(&mut self.webhook.secret, other.webhook.secret);
        merge_opt(&mut self.api.url, other.api.url);
    }

    fn apply_env(&mut self) {
        env_into(&mut self.server.bind_addr, "SBGH_BIND_ADDR");
        env_into(&mut self.webhook.secret, "SBGH_WEBHOOK_SECRET");
        env_into(&mut self.api.url, "SBGH_API_URL");
        env_into(&mut self.api.ingest_token, "SBGH_API_INGEST_TOKEN");
    }

    fn into_config(self) -> Result<HandlerConfig, ConfigError> {
        Ok(HandlerConfig {
            server: ServerConfig {
                bind_addr: self
                    .server
                    .bind_addr
                    .unwrap_or_else(|| "0.0.0.0:8080".into()),
            },
            webhook: WebhookConfig {
                secret: required(self.webhook.secret, "[webhook].secret / SBGH_WEBHOOK_SECRET")?,
            },
            api: HandlerApiConfig {
                url: required(self.api.url, "[api].url / SBGH_API_URL")?,
                ingest_token: required(self.api.ingest_token, "SBGH_API_INGEST_TOKEN")?,
            },
        })
    }
}

fn merge_opt<T>(destination: &mut Option<T>, source: Option<T>) {
    if let Some(value) = source {
        *destination = Some(value);
    }
}

fn env_into(destination: &mut Option<String>, key: &str) {
    if let Ok(value) = env::var(key) {
        *destination = Some(value);
    }
}

fn required<T>(value: Option<T>, name: &str) -> Result<T, ConfigError> {
    value.ok_or_else(|| ConfigError::Config(format!("missing required config: {name}")))
}

fn resolve_config_path() -> Option<PathBuf> {
    if let Ok(explicit) = env::var("SBGH_CONFIG") {
        return Some(PathBuf::from(explicit));
    }
    let system = PathBuf::from(DEFAULT_CONFIG_PATH);
    if system.exists() {
        return Some(system);
    }
    if let Some(home) = env::var_os("HOME") {
        let path = PathBuf::from(home).join(HOME_RELATIVE_CONFIG_PATH);
        if path.exists() {
            return Some(path);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::{Mutex, MutexGuard};

    use super::*;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct EnvGuard {
        saved: Vec<(&'static str, Option<String>)>,
        _lock: MutexGuard<'static, ()>,
    }

    impl EnvGuard {
        fn scoped(vars: &[(&'static str, Option<&str>)]) -> Self {
            let lock = ENV_LOCK
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let saved = vars
                .iter()
                .map(|(key, value)| {
                    let old = env::var(key).ok();
                    unsafe {
                        match value {
                            Some(value) => env::set_var(key, value),
                            None => env::remove_var(key),
                        }
                    }
                    (*key, old)
                })
                .collect();
            Self { saved, _lock: lock }
        }

        fn set(vars: &[(&'static str, &str)]) -> Self {
            let vars = vars
                .iter()
                .map(|(key, value)| (*key, Some(*value)))
                .collect::<Vec<_>>();
            Self::scoped(&vars)
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (key, old) in &self.saved {
                unsafe {
                    match old {
                        Some(value) => env::set_var(key, value),
                        None => env::remove_var(key),
                    }
                }
            }
        }
    }

    fn handler_env() -> Vec<(&'static str, &'static str)> {
        vec![
            ("SBGH_API_URL", "http://api:8787"),
            ("SBGH_API_INGEST_TOKEN", "ingest-tok"),
            ("SBGH_WEBHOOK_SECRET", "hunter2"),
        ]
    }

    fn write(content: &str) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(content.as_bytes())
            .unwrap();
        file
    }

    #[test]
    fn loads_from_environment_only() {
        let _guard = EnvGuard::set(&handler_env());
        let config = HandlerConfig::load_layered(None).unwrap();
        assert_eq!(config.webhook.secret, "hunter2");
        assert_eq!(config.api.url, "http://api:8787");
        assert_eq!(config.api.ingest_token, "ingest-tok");
    }

    #[test]
    fn ingest_token_in_toml_is_rejected() {
        let _guard = EnvGuard::set(&handler_env());
        let file = write("[api]\nurl = \"http://api\"\ningest_token = \"leaked\"\n");
        assert!(matches!(
            HandlerConfig::load_layered(Some(file.path())),
            Err(ConfigError::Config(_))
        ));
    }

    #[test]
    fn environment_overrides_toml() {
        let mut vars = handler_env();
        vars.push(("SBGH_BIND_ADDR", "127.0.0.1:9000"));
        let _guard = EnvGuard::set(&vars);
        let file =
            write("[server]\nbind_addr = \"0.0.0.0:8081\"\n[api]\nurl = \"http://toml-api\"\n");
        let config = HandlerConfig::load_layered(Some(file.path())).unwrap();
        assert_eq!(config.server.bind_addr, "127.0.0.1:9000");
        assert_eq!(config.api.url, "http://api:8787");
    }

    #[test]
    fn missing_secret_is_an_error() {
        let _guard = EnvGuard::scoped(&[
            ("SBGH_API_URL", Some("http://api")),
            ("SBGH_API_INGEST_TOKEN", Some("tok")),
            ("SBGH_WEBHOOK_SECRET", None),
        ]);
        assert!(matches!(HandlerConfig::load_layered(None), Err(ConfigError::Config(_))));
    }
}
