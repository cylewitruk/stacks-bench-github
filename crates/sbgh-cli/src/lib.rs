//! Library half of the `sbgh-cli` crate.
//!
//! Since roadmap-v3 Phase 5 the CLI bin (`src/main.rs`) is a pure `/api`
//! client. The admin operations live in `sbgh_core::admin` (the `/api`
//! server owns them); they're re-exported here only for the integration
//! tests in `tests/*.rs`, which exercise the admin *logic* against a
//! Postgres — the same logic the server runs.
//!
//! Phase 6 removed `apply_roles` (the DB role-split provisioning): the role
//! split collapsed to a single owner, so there are no narrow roles left to
//! create or grant.

pub use sbgh_core::admin::{
    AllowedRepoRoot, InstallerError, PolicyError, RepoError, UserError, add_trigger_policy,
    allow_installer, allow_repo_root, allow_source_policy, allow_target_policy, disable_installer,
    disable_installer_by_account_id, disable_repo_root, disable_repo_root_by_id,
    disable_source_policy, disable_target_policy, disable_trigger_policy, grant_role,
    grant_role_by_user_id, list_installers, list_repo_roots, list_roles, list_source_policies,
    list_target_policies, list_trigger_policies, list_users, revoke_role, revoke_role_by_user_id,
};
