//! Library half of the `sbgh-cli` crate. The bin (`src/main.rs`)
//! is the clap subcommand shell; the actual role/grant logic + the
//! installer admin operations live here so integration tests in
//! `sbgh-cli/tests/*.rs` can exercise them against an ephemeral
//! testcontainers Postgres without shelling out to the binary.

pub mod gh_resolve;
pub mod installer;
pub mod policy;
pub mod repo;
pub mod user;

use anyhow::Context;
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
use sbgh_core::db::Pool;
pub use user::{
    UserError, grant_role, grant_role_by_user_id, list_roles, list_users, revoke_role,
    revoke_role_by_user_id,
};

/// Create (if missing) and reset passwords + grants for the two narrow
/// roles the runtime services use.
///
/// Why not a SQL migration? `CREATE ROLE ... PASSWORD <literal>`
/// doesn't accept bind parameters, so the password has to be
/// string-interpolated. Doing that from Rust (with proper SQL
/// string-literal escaping) is safer than templating a migration file
/// and easier than coordinating per-host secrets through a migration
/// tool.
///
/// Idempotent: re-running just re-applies grants and resets passwords
/// to whatever is in the env. Safe to run on every deploy.
pub async fn apply_roles(
    pool: &Pool,
    handler_password: &str,
    orch_password: &str,
) -> anyhow::Result<()> {
    let mut tx = pool.begin().await?;

    // sbgh_handler: column-level grants.
    //
    // Columns it CAN INSERT into = exactly the fields the handler's
    // `PostgresJobStore::enqueue` writes. By NOT granting INSERT on
    // `status`, `result`, `started_at`, `finished_at`, `error`, etc.,
    // a compromised handler cannot fabricate a `status='completed'`
    // row with a fake result blob — those columns can only be written
    // by sbgh_orch's UPDATE. Server-side DEFAULTs (gen_random_uuid for
    // `id`, 'queued' for `status`, NOW() for `queued_at`) still fire
    // normally for omitted columns; you only need INSERT grant on a
    // column to *specify a value* for it.
    //
    // Columns it CAN SELECT = the two referenced by enqueue's SQL:
    //   - `id`                 read back by the RETURNING clause
    //   - `github_delivery_id` referenced by the ON CONFLICT predicate
    // Granting SELECT on these two specifically — not the whole table —
    // means a compromised handler still can't enumerate other PRs'
    // job rows (head_sha, args, result, …).
    upsert_role(&mut tx, "sbgh_handler", handler_password).await?;
    sqlx::query("REVOKE ALL ON TABLE jobs FROM sbgh_handler")
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "GRANT INSERT (repository, pr_number, head_sha, requested_by, command, args, \
         installation_id, github_delivery_id) ON TABLE jobs TO sbgh_handler",
    )
    .execute(&mut *tx)
    .await?;
    sqlx::query("GRANT SELECT (id, github_delivery_id) ON TABLE jobs TO sbgh_handler")
        .execute(&mut *tx)
        .await?;

    // sbgh_orch: SELECT + UPDATE on the full table. No INSERT (handler
    // enqueues), no DELETE (we never delete from the queue; rows
    // transition to completed/failed and are kept for audit).
    upsert_role(&mut tx, "sbgh_orch", orch_password).await?;
    sqlx::query("REVOKE ALL ON TABLE jobs FROM sbgh_orch")
        .execute(&mut *tx)
        .await?;
    sqlx::query("GRANT SELECT, UPDATE ON TABLE jobs TO sbgh_orch")
        .execute(&mut *tx)
        .await?;

    // Slice 1: webhook inbox grants.
    //
    // Handler writes only the columns it can know at HTTP-receipt time:
    // delivery_id, event_type, action, payload_installation_id, payload,
    // payload_size_bytes. Server-side DEFAULTs fire for id, status,
    // received_at, next_attempt_at, attempts. SELECT on (id, delivery_id)
    // supports `INSERT ... ON CONFLICT (delivery_id) DO NOTHING RETURNING
    // id` semantics. By NOT granting status/outcome/claimed_at/etc., a
    // compromised handler can't mark webhooks processed or fabricate
    // terminal outcomes — those columns are processor-only.
    sqlx::query("REVOKE ALL ON TABLE github_webhook FROM sbgh_handler")
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "GRANT INSERT (delivery_id, event_type, action, payload_installation_id, payload, \
         payload_size_bytes) ON TABLE github_webhook TO sbgh_handler",
    )
    .execute(&mut *tx)
    .await?;
    sqlx::query("GRANT SELECT (id, delivery_id) ON TABLE github_webhook TO sbgh_handler")
        .execute(&mut *tx)
        .await?;
    // Handler needs USAGE on the implicit sequence backing the BIGSERIAL
    // id column so the server-side default can fire on its INSERTs.
    sqlx::query("GRANT USAGE ON SEQUENCE github_webhook_id_seq TO sbgh_handler")
        .execute(&mut *tx)
        .await?;

    // sbgh_orch: full inbox claim/processing access. Same shape as for
    // `jobs` — SELECT + UPDATE, no INSERT, no DELETE.
    sqlx::query("REVOKE ALL ON TABLE github_webhook FROM sbgh_orch")
        .execute(&mut *tx)
        .await?;
    sqlx::query("GRANT SELECT, UPDATE ON TABLE github_webhook TO sbgh_orch")
        .execute(&mut *tx)
        .await?;

    // Slice 3: allowed_installer + github_installation.
    //
    // allowed_installer is operator-curated — only the owner (and the CLI
    // running as owner) writes it. Orchestrator only needs SELECT to
    // evaluate "is this installer allowed?" on installation.created. No
    // INSERT/UPDATE — a compromised processor must NOT be able to add a
    // hostile account to the allowlist.
    sqlx::query("REVOKE ALL ON TABLE allowed_installer FROM sbgh_handler, sbgh_orch")
        .execute(&mut *tx)
        .await?;
    sqlx::query("GRANT SELECT ON TABLE allowed_installer TO sbgh_orch")
        .execute(&mut *tx)
        .await?;

    // github_installation is processor-owned: the processor creates rows
    // on installation.created, updates suspended_at on suspend/unsuspend,
    // soft-deletes (sets deleted_at) on installation.deleted. Handler
    // never touches it. Slice 4 switched delete to soft-delete so the
    // membership FK doesn't conflict; UPDATE covers all four operations.
    sqlx::query("REVOKE ALL ON TABLE github_installation FROM sbgh_handler, sbgh_orch")
        .execute(&mut *tx)
        .await?;
    sqlx::query("GRANT SELECT, INSERT, UPDATE ON TABLE github_installation TO sbgh_orch")
        .execute(&mut *tx)
        .await?;

    // Slice 4: github_repo + supported_repo_root + github_installation_repo.
    //
    // github_repo is processor-owned identity cache: the processor inserts
    // rows on first encounter (both the repo itself and any parent/source
    // in its lineage) from the GitHub API. Operator's `sbgh-cli repo allow`
    // also inserts the canonical-root identity row (running as owner, not
    // through orch's grant). Slices 7+ refresh mutable fields (default_branch,
    // lineage_checked_at) on PR events.
    sqlx::query("REVOKE ALL ON TABLE github_repo FROM sbgh_handler, sbgh_orch")
        .execute(&mut *tx)
        .await?;
    sqlx::query("GRANT SELECT, INSERT, UPDATE ON TABLE github_repo TO sbgh_orch")
        .execute(&mut *tx)
        .await?;

    // supported_repo_root is operator-curated, same shape as
    // allowed_installer: orch SELECT-only (read the gate), CLI-as-owner
    // writes. A compromised processor must NOT be able to add itself a
    // new in-scope repo family.
    sqlx::query("REVOKE ALL ON TABLE supported_repo_root FROM sbgh_handler, sbgh_orch")
        .execute(&mut *tx)
        .await?;
    sqlx::query("GRANT SELECT ON TABLE supported_repo_root TO sbgh_orch")
        .execute(&mut *tx)
        .await?;

    // github_installation_repo is processor-owned: insert on
    // installation_repositories.added, UPDATE revoked_at on .removed +
    // bulk on installation.deleted. No DELETE (rows are retained as
    // historical audit trail; FK target for slice 5+ policy + slice 8+ job).
    sqlx::query("REVOKE ALL ON TABLE github_installation_repo FROM sbgh_handler, sbgh_orch")
        .execute(&mut *tx)
        .await?;
    sqlx::query("GRANT SELECT, INSERT, UPDATE ON TABLE github_installation_repo TO sbgh_orch")
        .execute(&mut *tx)
        .await?;

    // Slice 5: target_repo_policy + source_repo_policy + trigger_policy.
    //
    // Target/source policies are operator-curated AND processor-managed:
    // the CLI (running as owner) writes them on operator approval, and
    // the processor UPDATEs `is_enabled = FALSE` on install.deleted's
    // bulk-disable path. Orchestrator gets SELECT + UPDATE (no INSERT,
    // no DELETE). A compromised processor cannot allowlist a new
    // (install, repo) policy pair; it can only flip existing rows'
    // enabled bit — and even that is bounded to the install.deleted
    // cleanup branch in the codebase.
    //
    // Handler never touches these tables.
    sqlx::query(
        "REVOKE ALL ON TABLE target_repo_policy, source_repo_policy, trigger_policy FROM \
         sbgh_handler, sbgh_orch",
    )
    .execute(&mut *tx)
    .await?;
    sqlx::query("GRANT SELECT, UPDATE ON TABLE target_repo_policy TO sbgh_orch")
        .execute(&mut *tx)
        .await?;
    sqlx::query("GRANT SELECT, UPDATE ON TABLE source_repo_policy TO sbgh_orch")
        .execute(&mut *tx)
        .await?;
    sqlx::query("GRANT SELECT, UPDATE ON TABLE trigger_policy TO sbgh_orch")
        .execute(&mut *tx)
        .await?;

    // Slice 6: github_user + github_user_role.
    //
    // github_user is lazy-upserted by the processor on first sighting
    // of a sender / PR author — orch needs INSERT + UPDATE (the UPSERT
    // refreshes `login` + `user_type` on PK conflict). SELECT for the
    // CLI's `user list` join + the handler's `has_role` path. No
    // DELETE: user identity is forever (same rationale as github_repo).
    //
    // github_user_role is OPERATOR-CURATED. Orch gets SELECT only — a
    // compromised processor must NOT be able to grant itself a role
    // (especially `trigger_pr_benchmark`, which is the `/benchmark`
    // gate). All writes happen via `sbgh-cli user {grant,revoke}`
    // running as the DB owner.
    //
    // Handler never touches either table.
    sqlx::query("REVOKE ALL ON TABLE github_user, github_user_role FROM sbgh_handler, sbgh_orch")
        .execute(&mut *tx)
        .await?;
    sqlx::query("GRANT SELECT, INSERT, UPDATE ON TABLE github_user TO sbgh_orch")
        .execute(&mut *tx)
        .await?;
    sqlx::query("GRANT SELECT ON TABLE github_user_role TO sbgh_orch")
        .execute(&mut *tx)
        .await?;

    // Slice 7: github_pull_request.
    //
    // PR rows are materialised by the processor on opened/reopened/
    // synchronize/edited and refreshed on edited (title) and
    // closed/reopened (closed_at). Orch needs SELECT + INSERT +
    // UPDATE. No DELETE — slice 8+ job FKs depend on PR rows
    // sticking around, and closed PRs use soft-close (`closed_at`).
    // Handler never touches this table.
    sqlx::query("REVOKE ALL ON TABLE github_pull_request FROM sbgh_handler, sbgh_orch")
        .execute(&mut *tx)
        .await?;
    sqlx::query("GRANT SELECT, INSERT, UPDATE ON TABLE github_pull_request TO sbgh_orch")
        .execute(&mut *tx)
        .await?;
    sqlx::query("GRANT USAGE ON SEQUENCE github_pull_request_id_seq TO sbgh_orch")
        .execute(&mut *tx)
        .await?;

    // Slice 8: new-schema job tables.
    //
    // `job` is populated by the slice 9 processor (INSERT on create,
    // UPDATE on claim → running → terminal transitions, UPDATE on
    // stuck-claim sweep). No DELETE — completed jobs are historical
    // records.
    //
    // `job_event` is append-only timeline writes (INSERT only); the
    // processor + orchestrator both write to it.
    //
    // `job_metric`, `job_result` are write-once outcome companions
    // (orchestrator INSERT after successful execution).
    //
    // `github_pull_request_job`, `github_webhook_job`, `github_user_job`
    // are subject relations the slice 9 processor INSERTs alongside
    // the job creation. UNIQUE constraints catch double-insert.
    //
    // Handler never touches any of these tables.
    //
    // Bigserial sequences: `job_event_id_seq` needs USAGE for the
    // INSERT path; the other tables use UUID PKs (no sequence) or
    // composite PKs (no sequence on the link tables).
    sqlx::query(
        "REVOKE ALL ON TABLE job, job_event, job_metric, job_result, github_pull_request_job, \
         github_webhook_job, github_user_job FROM sbgh_handler, sbgh_orch",
    )
    .execute(&mut *tx)
    .await?;
    sqlx::query("GRANT SELECT, INSERT, UPDATE ON TABLE job TO sbgh_orch")
        .execute(&mut *tx)
        .await?;
    sqlx::query("GRANT SELECT, INSERT ON TABLE job_event TO sbgh_orch")
        .execute(&mut *tx)
        .await?;
    sqlx::query("GRANT USAGE ON SEQUENCE job_event_id_seq TO sbgh_orch")
        .execute(&mut *tx)
        .await?;
    sqlx::query("GRANT SELECT, INSERT ON TABLE job_metric TO sbgh_orch")
        .execute(&mut *tx)
        .await?;
    sqlx::query("GRANT SELECT, INSERT ON TABLE job_result TO sbgh_orch")
        .execute(&mut *tx)
        .await?;
    sqlx::query("GRANT SELECT, INSERT ON TABLE github_pull_request_job TO sbgh_orch")
        .execute(&mut *tx)
        .await?;
    sqlx::query("GRANT SELECT, INSERT ON TABLE github_webhook_job TO sbgh_orch")
        .execute(&mut *tx)
        .await?;
    sqlx::query("GRANT SELECT, INSERT ON TABLE github_user_job TO sbgh_orch")
        .execute(&mut *tx)
        .await?;

    // USAGE on the schema is required even with table-level grants. CONNECT
    // on the database is implicit for any login role; no need to grant
    // explicitly.
    sqlx::query("GRANT USAGE ON SCHEMA public TO sbgh_handler, sbgh_orch")
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;
    Ok(())
}

/// Create a LOGIN role if missing, then unconditionally ALTER its password.
/// Names are constants (never user input) so direct interpolation is safe.
async fn upsert_role(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    role: &'static str,
    password: &str,
) -> anyhow::Result<()> {
    // Existence check happens in Rust, NOT in a `DO $$ ... EXECUTE '...'`
    // wrapper. Reason: the password literal `'pw'` embedded inside an
    // outer EXECUTE string would close the outer single-quoted string
    // prematurely (postgres tokenises the next char as a fresh token —
    // typically a numeric literal that explodes with "trailing junk
    // after numeric literal"). Doing the conditional from Rust means
    // each statement has exactly ONE layer of single-quoting around the
    // password, which `sql_string_literal` already handles correctly.
    let pw_lit = sql_string_literal(password);

    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)")
            .bind(role)
            .fetch_one(&mut **tx)
            .await
            .with_context(|| format!("probing for role {role}"))?;

    if !exists {
        let create = format!("CREATE ROLE {role} LOGIN PASSWORD {pw_lit}");
        sqlx::query(&create)
            .execute(&mut **tx)
            .await
            .with_context(|| format!("creating role {role}"))?;
    }

    let alter = format!("ALTER ROLE {role} WITH LOGIN PASSWORD {pw_lit}");
    sqlx::query(&alter)
        .execute(&mut **tx)
        .await
        .with_context(|| format!("resetting password for role {role}"))?;
    Ok(())
}

/// Escape an arbitrary string into a Postgres single-quoted string literal.
/// The only metacharacter inside `'…'` is `'` itself (doubled to escape).
pub(crate) fn sql_string_literal(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for ch in s.chars() {
        if ch == '\'' {
            out.push('\'');
        }
        out.push(ch);
    }
    out.push('\'');
    out
}

#[cfg(test)]
mod tests {
    use super::sql_string_literal;

    #[test]
    fn plain_password_quoted() {
        assert_eq!(sql_string_literal("hunter2"), "'hunter2'");
    }

    #[test]
    fn single_quote_is_doubled() {
        assert_eq!(sql_string_literal("it's"), "'it''s'");
        assert_eq!(sql_string_literal("''"), "''''''");
    }

    #[test]
    fn empty_string_round_trips() {
        assert_eq!(sql_string_literal(""), "''");
    }
}
