//! In-memory `InstallationStore` for unit tests. Holds allowlist,
//! installs, and memberships behind a single `Mutex` so concurrency
//! semantics stay deterministic.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::Result;
use crate::db::installation::{DeleteInstallationOutcome, InstallationStore, NewInstallation};
use crate::models::{
    AllowedInstaller, GithubAccountType, GithubInstallation, GithubInstallationRepo,
};

#[derive(Default)]
pub struct InMemoryInstallationStore {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    allowlist: HashMap<i64, AllowedInstaller>,
    installs: HashMap<i64, GithubInstallation>,
    memberships: HashMap<(i64, i64), GithubInstallationRepo>,
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

    pub fn installations(&self) -> Vec<GithubInstallation> {
        self.state
            .lock()
            .unwrap()
            .installs
            .values()
            .cloned()
            .collect()
    }

    pub fn installation(&self, id: i64) -> Option<GithubInstallation> {
        self.state
            .lock()
            .unwrap()
            .installs
            .get(&id)
            .cloned()
    }

    /// Read all memberships (test introspection).
    pub fn memberships(&self) -> Vec<GithubInstallationRepo> {
        self.state
            .lock()
            .unwrap()
            .memberships
            .values()
            .cloned()
            .collect()
    }

    pub fn membership(&self, install_id: i64, repo_id: i64) -> Option<GithubInstallationRepo> {
        self.state
            .lock()
            .unwrap()
            .memberships
            .get(&(install_id, repo_id))
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
                deleted_at: None,
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

    async fn delete_installation(&self, installation_id: i64) -> Result<DeleteInstallationOutcome> {
        let mut state = self.state.lock().unwrap();
        // 1. probe
        let install_found = state
            .installs
            .contains_key(&installation_id);
        if !install_found {
            return Ok(DeleteInstallationOutcome {
                install_found: false,
                memberships_revoked: 0,
            });
        }

        // 2. bulk-revoke memberships
        let now = Utc::now();
        let mut memberships_revoked: u64 = 0;
        for ((inst, _repo), row) in state.memberships.iter_mut() {
            if *inst == installation_id && row.revoked_at.is_none() {
                row.revoked_at = Some(now);
                memberships_revoked += 1;
            }
        }

        // 3. soft-delete the install (sticky on re-delivery)
        if let Some(row) = state
            .installs
            .get_mut(&installation_id)
            && row.deleted_at.is_none()
        {
            row.deleted_at = Some(now);
            row.updated_at = now;
        }

        Ok(DeleteInstallationOutcome {
            install_found: true,
            memberships_revoked,
        })
    }

    async fn add_or_restore_membership(
        &self,
        installation_id: i64,
        github_repo_id: i64,
    ) -> Result<Option<GithubInstallationRepo>> {
        let mut state = self.state.lock().unwrap();
        // Guard: install must exist AND not be soft-deleted. Mirrors
        // the Postgres impl's `deleted_at IS NULL` predicate. Without
        // this, a stale `installation_repositories.added` arriving
        // after `installation.deleted` would resurrect membership on
        // a retired install.
        let active = state
            .installs
            .get(&installation_id)
            .is_some_and(|i| i.deleted_at.is_none());
        if !active {
            return Ok(None);
        }
        let now = Utc::now();
        let row = state
            .memberships
            .entry((installation_id, github_repo_id))
            .and_modify(|r| {
                r.revoked_at = None; // restore preserves granted_at
            })
            .or_insert_with(|| GithubInstallationRepo {
                github_installation_id: installation_id,
                github_repo_id,
                granted_at: now,
                revoked_at: None,
            })
            .clone();
        Ok(Some(row))
    }

    async fn is_membership_active(
        &self,
        installation_id: i64,
        github_repo_id: i64,
    ) -> Result<bool> {
        let state = self.state.lock().unwrap();
        let install_active = state
            .installs
            .get(&installation_id)
            .is_some_and(|i| i.deleted_at.is_none() && i.suspended_at.is_none());
        if !install_active {
            return Ok(false);
        }
        let membership_active = state
            .memberships
            .get(&(installation_id, github_repo_id))
            .is_some_and(|m| m.revoked_at.is_none());
        Ok(membership_active)
    }

    async fn revoke_membership(
        &self,
        installation_id: i64,
        github_repo_id: i64,
    ) -> Result<Option<GithubInstallationRepo>> {
        let mut state = self.state.lock().unwrap();
        match state
            .memberships
            .get_mut(&(installation_id, github_repo_id))
        {
            Some(row) if row.revoked_at.is_none() => {
                row.revoked_at = Some(Utc::now());
                Ok(Some(row.clone()))
            }
            _ => Ok(None),
        }
    }
}
