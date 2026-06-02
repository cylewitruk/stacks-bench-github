//! Operator admin operations (installer allowlist, supported repo roots,
//! per-installation policies, user role grants) + GitHub `login`/`owner-name`
//! → id resolution.
//!
//! These were the `sbgh-cli` lib; they moved here in roadmap-v3 Phase 3 so
//! the daemon's `/api` server owns the logic (the CLI becomes a thin
//! API client in Phase 5). Each function takes a `Pool` and (for the
//! resolve-needing ones) the GitHub API base URL.

pub mod gh_resolve;
pub mod installer;
pub mod policy;
pub mod repo;
pub mod user;

pub use gh_resolve::{ResolveError, ResolvedAccount, resolve_account};
pub use installer::{
    InstallerError, allow_installer, disable_installer, disable_installer_by_account_id,
    list_installers,
};
pub use policy::{
    PolicyError, add_trigger_policy, allow_source_policy, allow_target_policy,
    disable_source_policy, disable_target_policy, disable_trigger_policy, list_source_policies,
    list_target_policies, list_trigger_policies,
};
pub use repo::{
    AllowedRepoRoot, RepoError, allow_repo_root, disable_repo_root, disable_repo_root_by_id,
    list_repo_roots,
};
pub use user::{
    UserError, grant_role, grant_role_by_user_id, list_roles, list_users, revoke_role,
    revoke_role_by_user_id,
};
