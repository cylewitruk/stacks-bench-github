//! In-memory `InstallationStore` for slice 3 unit tests. Maintains two
//! `HashMap`s behind a single `Mutex`: one for the operator-curated
//! allowlist (seeded via `seed_allowed`), one for materialised installs.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::Result;
use crate::db::installation::{InstallationStore, NewInstallation};
use crate::models::{AllowedInstaller, GithubAccountType, GithubInstallation};

#[derive(Default)]
pub struct InMemoryInstallationStore {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    allowlist: HashMap<i64, AllowedInstaller>,
    installs: HashMap<i64, GithubInstallation>,
}

impl InMemoryInstallationStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Seed an `allowed_installer` row from a test. Defaults to enabled.
    pub fn seed_allowed(
        &self,
        github_account_id: i64,
        login: &str,
        account_type: GithubAccountType,
        is_enabled: bool,
    ) {
        let now = Utc::now();
        let row = AllowedInstaller {
            github_account_id,
            account_login: login.to_string(),
            account_type,
            is_enabled,
            note: None,
            created_at: now,
            updated_at: now,
        };
        self.state
            .lock()
            .unwrap()
            .allowlist
            .insert(github_account_id, row);
    }

    /// Read all materialised installations (test introspection).
    pub fn installations(&self) -> Vec<GithubInstallation> {
        self.state
            .lock()
            .unwrap()
            .installs
            .values()
            .cloned()
            .collect()
    }

    /// Fetch a specific installation by id (test introspection).
    pub fn installation(&self, id: i64) -> Option<GithubInstallation> {
        self.state
            .lock()
            .unwrap()
            .installs
            .get(&id)
            .cloned()
    }
}

#[async_trait]
impl InstallationStore for InMemoryInstallationStore {
    async fn lookup_allowed(&self, github_account_id: i64) -> Result<Option<AllowedInstaller>> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .allowlist
            .get(&github_account_id)
            .cloned())
    }

    async fn upsert_installation(&self, new: &NewInstallation) -> Result<GithubInstallation> {
        let mut state = self.state.lock().unwrap();
        let now = Utc::now();
        let row = state
            .installs
            .entry(new.id)
            .and_modify(|r| {
                r.account_login = new.account_login.clone();
                r.account_type = new.account_type;
                r.updated_at = now;
            })
            .or_insert_with(|| GithubInstallation {
                id: new.id,
                github_account_id: new.github_account_id,
                account_login: new.account_login.clone(),
                account_type: new.account_type,
                suspended_at: None,
                created_at: now,
                updated_at: now,
            })
            .clone();
        Ok(row)
    }

    async fn set_suspended(
        &self,
        installation_id: i64,
        suspended_at: Option<DateTime<Utc>>,
    ) -> Result<Option<GithubInstallation>> {
        let mut state = self.state.lock().unwrap();
        match state
            .installs
            .get_mut(&installation_id)
        {
            None => Ok(None),
            Some(row) => {
                row.suspended_at = suspended_at;
                row.updated_at = Utc::now();
                Ok(Some(row.clone()))
            }
        }
    }

    async fn delete_installation(&self, installation_id: i64) -> Result<bool> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .installs
            .remove(&installation_id)
            .is_some())
    }
}
