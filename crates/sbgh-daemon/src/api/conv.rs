//! Enum ⇄ wire-string helpers. The core models derive `Serialize` /
//! `Deserialize` with `#[serde(rename_all = "snake_case")]`, so a unit
//! variant round-trips through a JSON string — no per-enum match tables.

/// `GithubAccountType::User` → `"user"`, etc.
pub fn enum_str<T: serde::Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_default()
}

/// `"trigger_pr_benchmark"` → `Some(UserRole::TriggerPrBenchmark)`.
pub fn parse_enum<T: serde::de::DeserializeOwned>(s: &str) -> Option<T> {
    serde_json::from_value(serde_json::Value::String(s.to_string())).ok()
}
