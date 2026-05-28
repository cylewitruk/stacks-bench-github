//! In-memory `UserStore` for unit tests. Single Mutex serialises all
//! access; `has_role` mirrors the Postgres wildcard semantics
//! (NULL repo grants match any repo within the install).

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, Ordering};

use async_trait::async_trait;
use chrono::Utc;

use crate::Result;
use crate::db::user::{GrantRoleOutcome, NewUser, UserStore};
use crate::models::{GithubAccountType, GithubUser, GithubUserRole, UserRole};

#[derive(Default)]
pub struct InMemoryUserStore {
    state: Mutex<State>,
    next_role_id: AtomicI64,
}

#[derive(Default)]
struct State {
    users: HashMap<i64, GithubUser>,
    /// Stored as Vec to preserve insertion order for `list_roles`
    /// determinism. Uniqueness invariant maintained by `grant_role`.
    roles: Vec<GithubUserRole>,
}

impl InMemoryUserStore {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State::default()),
            next_role_id: AtomicI64::new(1),
        }
    }

    /// Test helper: pre-seed a user without going through `upsert_user`.
    pub fn seed_user(&self, id: i64, login: &str, user_type: GithubAccountType) {
        let now = Utc::now();
        self.state
            .lock()
            .unwrap()
            .users
            .insert(
                id,
                GithubUser {
                    id,
                    login: login.to_string(),
                    user_type,
                    created_at: now,
                    updated_at: now,
                },
            );
    }

    /// Test helper: pre-seed a role grant without going through
    /// `grant_role`. Caller's responsibility to keep (user, install,
    /// repo, role) unique. Seeded grants start active
    /// (`revoked_at = None`).
    pub fn seed_role(
        &self,
        github_user_id: i64,
        github_installation_id: i64,
        github_repo_id: Option<i64>,
        granted_role: UserRole,
    ) {
        let id = self
            .next_role_id
            .fetch_add(1, Ordering::SeqCst);
        self.state
            .lock()
            .unwrap()
            .roles
            .push(GithubUserRole {
                id,
                github_user_id,
                github_installation_id,
                github_repo_id,
                granted_role,
                granted_at: Utc::now(),
                granted_by_github_user_id: None,
                revoked_at: None,
            });
    }
}

#[async_trait]
impl UserStore for InMemoryUserStore {
    async fn upsert_user(&self, new: &NewUser) -> Result<GithubUser> {
        let mut s = self.state.lock().unwrap();
        let now = Utc::now();
        let entry = s
            .users
            .entry(new.id)
            .or_insert_with(|| GithubUser {
                id: new.id,
                login: new.login.clone(),
                user_type: new.user_type,
                created_at: now,
                updated_at: now,
            });
        entry.login = new.login.clone();
        entry.user_type = new.user_type;
        entry.updated_at = now;
        Ok(entry.clone())
    }

    async fn lookup_user(&self, user_id: i64) -> Result<Option<GithubUser>> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .users
            .get(&user_id)
            .cloned())
    }

    async fn lookup_user_by_login(&self, login: &str) -> Result<Option<GithubUser>> {
        let target = login.to_lowercase();
        Ok(self
            .state
            .lock()
            .unwrap()
            .users
            .values()
            .find(|u| u.login.to_lowercase() == target)
            .cloned())
    }

    async fn grant_role(
        &self,
        github_user_id: i64,
        github_installation_id: i64,
        github_repo_id: Option<i64>,
        granted_role: UserRole,
        granted_by_github_user_id: Option<i64>,
    ) -> Result<GrantRoleOutcome> {
        let mut s = self.state.lock().unwrap();
        if let Some(existing) = s.roles.iter_mut().find(|r| {
            r.github_user_id == github_user_id
                && r.github_installation_id == github_installation_id
                && r.github_repo_id == github_repo_id
                && r.granted_role == granted_role
        }) {
            // Re-grant: clear revoked_at if previously revoked.
            existing.revoked_at = None;
            return Ok(GrantRoleOutcome {
                role: existing.clone(),
                created: false,
            });
        }
        let row = GithubUserRole {
            id: self
                .next_role_id
                .fetch_add(1, Ordering::SeqCst),
            github_user_id,
            github_installation_id,
            github_repo_id,
            granted_role,
            granted_at: Utc::now(),
            granted_by_github_user_id,
            revoked_at: None,
        };
        s.roles.push(row.clone());
        Ok(GrantRoleOutcome { role: row, created: true })
    }

    async fn revoke_role(
        &self,
        github_user_id: i64,
        github_installation_id: i64,
        github_repo_id: Option<i64>,
        granted_role: UserRole,
    ) -> Result<Option<GithubUserRole>> {
        let mut s = self.state.lock().unwrap();
        let matching = s.roles.iter_mut().find(|r| {
            r.github_user_id == github_user_id
                && r.github_installation_id == github_installation_id
                && r.github_repo_id == github_repo_id
                && r.granted_role == granted_role
                && r.revoked_at.is_none()
        });
        Ok(matching.map(|r| {
            r.revoked_at = Some(Utc::now());
            r.clone()
        }))
    }

    async fn list_roles(&self, install_id: Option<i64>) -> Result<Vec<GithubUserRole>> {
        let s = self.state.lock().unwrap();
        let mut out: Vec<GithubUserRole> = s
            .roles
            .iter()
            .filter(|r| install_id.is_none_or(|id| r.github_installation_id == id))
            .cloned()
            .collect();
        out.sort_by_key(|r| (r.github_installation_id, r.github_user_id, r.granted_role as i32));
        Ok(out)
    }

    async fn revoke_repo_scoped_grants(
        &self,
        github_installation_id: i64,
        github_repo_id: i64,
    ) -> Result<u64> {
        let now = Utc::now();
        let mut s = self.state.lock().unwrap();
        let mut count = 0u64;
        for r in &mut s.roles {
            if r.github_installation_id == github_installation_id
                && r.github_repo_id == Some(github_repo_id)
                && r.revoked_at.is_none()
            {
                r.revoked_at = Some(now);
                count += 1;
            }
        }
        Ok(count)
    }

    async fn has_role(
        &self,
        github_user_id: i64,
        github_installation_id: i64,
        repo_id: i64,
        role: UserRole,
    ) -> Result<bool> {
        // Mirrors the Postgres impl: active grants only
        // (`revoked_at.is_none()`), with admin-implies semantics —
        // an `admin` grant in the same scope authorizes any role.
        Ok(self
            .state
            .lock()
            .unwrap()
            .roles
            .iter()
            .any(|r| {
                r.revoked_at.is_none()
                    && r.github_user_id == github_user_id
                    && r.github_installation_id == github_installation_id
                    && (r.granted_role == role || r.granted_role == UserRole::Admin)
                    && (r.github_repo_id.is_none() || r.github_repo_id == Some(repo_id))
            }))
    }
}
