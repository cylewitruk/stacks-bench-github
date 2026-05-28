//! Slice 6 integration tests: `PostgresUserStore` against a real Postgres.
//! Covers user upsert idempotency, grant uniqueness (NULLS NOT DISTINCT),
//! revoke semantics, and the `has_role` wildcard rules — especially the
//! "install-wide grant matches any repo, repo-scoped grant does NOT
//! match a different repo" gate that the slice 6 `/benchmark` authz
//! check depends on.

use sbgh_core::db::{NewUser, PostgresUserStore, UserStore, setup_pg};
use sbgh_core::models::{GithubAccountType, UserRole};

/// Seed an install + repo so role-grant FKs are satisfiable. Slice 6
/// grants FK at `github_installation` and (optionally) `github_repo`.
async fn seed_install_repo(pool: &sbgh_core::db::Pool, install_id: i64, repo_id: i64) {
    sqlx::query(
        "INSERT INTO allowed_installer (github_account_id, account_login, account_type) VALUES \
         ($1, 'octo-org', 'organization') ON CONFLICT DO NOTHING",
    )
    .bind(install_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO github_installation (id, github_account_id, account_login, account_type) \
         VALUES ($1, $1, 'octo-org', 'organization') ON CONFLICT DO NOTHING",
    )
    .bind(install_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO github_repo (id, owner, name) VALUES ($1, 'o', $2) ON CONFLICT DO NOTHING",
    )
    .bind(repo_id)
    .bind(format!("r{repo_id}"))
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn upsert_user_is_idempotent_and_refreshes_display_fields() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    let store = PostgresUserStore::new(pool.clone());

    let first = store
        .upsert_user(&NewUser {
            id: 42,
            login: "alice".into(),
            user_type: GithubAccountType::User,
        })
        .await
        .unwrap();
    assert_eq!(first.id, 42);
    assert_eq!(first.login, "alice");

    // Re-upsert with renamed login + Bot type — PK conflict refreshes
    // display columns, does NOT create a second row.
    let second = store
        .upsert_user(&NewUser {
            id: 42,
            login: "alice-bot".into(),
            user_type: GithubAccountType::Bot,
        })
        .await
        .unwrap();
    assert_eq!(second.id, 42);
    assert_eq!(second.login, "alice-bot");
    assert_eq!(second.user_type, GithubAccountType::Bot);
    assert!(second.updated_at >= first.created_at);
}

#[tokio::test]
async fn lookup_user_by_login_is_case_insensitive() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    let store = PostgresUserStore::new(pool.clone());
    store
        .upsert_user(&NewUser {
            id: 42,
            login: "Alice".into(),
            user_type: GithubAccountType::User,
        })
        .await
        .unwrap();
    assert!(
        store
            .lookup_user_by_login("alice")
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        store
            .lookup_user_by_login("ALICE")
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        store
            .lookup_user_by_login("nope")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn grant_role_is_idempotent_on_exact_quadruple() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresUserStore::new(pool.clone());
    store
        .upsert_user(&NewUser {
            id: 42,
            login: "alice".into(),
            user_type: GithubAccountType::User,
        })
        .await
        .unwrap();

    let first = store
        .grant_role(42, 100, Some(10), UserRole::TriggerPrBenchmark, None)
        .await
        .unwrap();
    assert!(first.created, "first grant must be newly created");

    let second = store
        .grant_role(42, 100, Some(10), UserRole::TriggerPrBenchmark, None)
        .await
        .unwrap();
    assert!(!second.created, "re-grant must be idempotent");
    assert_eq!(first.role.id, second.role.id, "must return the SAME row");
}

#[tokio::test]
async fn grant_role_treats_null_repo_and_specific_repo_as_distinct() {
    // NULLS NOT DISTINCT means (user, install, NULL, role) is its own
    // bucket — different from (user, install, repo=10, role). An
    // operator who has both an install-wide grant AND a repo-scoped
    // grant ends up with two distinct rows.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresUserStore::new(pool.clone());
    store
        .upsert_user(&NewUser {
            id: 42,
            login: "alice".into(),
            user_type: GithubAccountType::User,
        })
        .await
        .unwrap();

    let install_wide = store
        .grant_role(42, 100, None, UserRole::TriggerPrBenchmark, None)
        .await
        .unwrap();
    let repo_scoped = store
        .grant_role(42, 100, Some(10), UserRole::TriggerPrBenchmark, None)
        .await
        .unwrap();
    assert!(install_wide.created);
    assert!(repo_scoped.created);
    assert_ne!(install_wide.role.id, repo_scoped.role.id, "different rows");

    // A second install-wide grant collapses; a second repo-scoped
    // grant collapses; but they don't collapse INTO each other.
    let install_wide_redux = store
        .grant_role(42, 100, None, UserRole::TriggerPrBenchmark, None)
        .await
        .unwrap();
    assert!(!install_wide_redux.created);
    assert_eq!(install_wide.role.id, install_wide_redux.role.id);
}

#[tokio::test]
async fn revoke_role_exact_match_only() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresUserStore::new(pool.clone());
    store
        .upsert_user(&NewUser {
            id: 42,
            login: "alice".into(),
            user_type: GithubAccountType::User,
        })
        .await
        .unwrap();
    store
        .grant_role(42, 100, None, UserRole::TriggerPrBenchmark, None)
        .await
        .unwrap();

    // Repo-narrowed revoke must NOT delete the install-wide grant.
    let nothing = store
        .revoke_role(42, 100, Some(10), UserRole::TriggerPrBenchmark)
        .await
        .unwrap();
    assert!(nothing.is_none(), "repo-narrowed revoke must NOT match an install-wide grant");

    // The right shape DOES match.
    let removed = store
        .revoke_role(42, 100, None, UserRole::TriggerPrBenchmark)
        .await
        .unwrap();
    assert!(removed.is_some());

    // Idempotent: second revoke is a no-op.
    let none_again = store
        .revoke_role(42, 100, None, UserRole::TriggerPrBenchmark)
        .await
        .unwrap();
    assert!(none_again.is_none());
}

#[tokio::test]
async fn has_role_install_wide_grant_matches_any_repo() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    seed_install_repo(&pool, 100, 11).await;
    let store = PostgresUserStore::new(pool.clone());
    store
        .upsert_user(&NewUser {
            id: 42,
            login: "alice".into(),
            user_type: GithubAccountType::User,
        })
        .await
        .unwrap();
    store
        .grant_role(42, 100, None, UserRole::TriggerPrBenchmark, None)
        .await
        .unwrap();

    for repo_id in [10, 11, 12, 999] {
        // Even unknown repo ids match — install-wide grant doesn't
        // care which repo; the gate is the install boundary. The
        // caller is expected to have already verified the repo
        // belongs to the install via the membership store.
        assert!(
            store
                .has_role(42, 100, repo_id, UserRole::TriggerPrBenchmark)
                .await
                .unwrap(),
            "install-wide grant must match repo_id={repo_id}"
        );
    }
}

#[tokio::test]
async fn has_role_repo_scoped_grant_matches_only_that_repo() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    seed_install_repo(&pool, 100, 11).await;
    let store = PostgresUserStore::new(pool.clone());
    store
        .upsert_user(&NewUser {
            id: 42,
            login: "alice".into(),
            user_type: GithubAccountType::User,
        })
        .await
        .unwrap();
    store
        .grant_role(42, 100, Some(10), UserRole::TriggerPrBenchmark, None)
        .await
        .unwrap();

    assert!(
        store
            .has_role(42, 100, 10, UserRole::TriggerPrBenchmark)
            .await
            .unwrap()
    );
    assert!(
        !store
            .has_role(42, 100, 11, UserRole::TriggerPrBenchmark)
            .await
            .unwrap(),
        "repo-scoped grant for repo=10 must NOT authorize repo=11"
    );
}

#[tokio::test]
async fn has_role_does_not_cross_installation_boundary() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    seed_install_repo(&pool, 200, 10).await;
    let store = PostgresUserStore::new(pool.clone());
    store
        .upsert_user(&NewUser {
            id: 42,
            login: "alice".into(),
            user_type: GithubAccountType::User,
        })
        .await
        .unwrap();
    // Grant on install=100 install-wide.
    store
        .grant_role(42, 100, None, UserRole::TriggerPrBenchmark, None)
        .await
        .unwrap();

    assert!(
        store
            .has_role(42, 100, 10, UserRole::TriggerPrBenchmark)
            .await
            .unwrap()
    );
    assert!(
        !store
            .has_role(42, 200, 10, UserRole::TriggerPrBenchmark)
            .await
            .unwrap(),
        "grant on install=100 must NOT authorize install=200"
    );
}

#[tokio::test]
async fn has_role_does_not_match_different_non_admin_role() {
    // Post-review M1 fix updated this assertion: admin IMPLIES all
    // other roles within the same scope, so we use view_results
    // (which does NOT imply anything) to assert the "different role
    // doesn't match" invariant. The admin-implies behavior gets its
    // own dedicated test below.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresUserStore::new(pool.clone());
    store
        .upsert_user(&NewUser {
            id: 42,
            login: "alice".into(),
            user_type: GithubAccountType::User,
        })
        .await
        .unwrap();
    // alice has view_results, not trigger_pr_benchmark.
    store
        .grant_role(42, 100, None, UserRole::ViewResults, None)
        .await
        .unwrap();

    assert!(
        store
            .has_role(42, 100, 10, UserRole::ViewResults)
            .await
            .unwrap()
    );
    assert!(
        !store
            .has_role(42, 100, 10, UserRole::TriggerPrBenchmark)
            .await
            .unwrap(),
        "view_results does NOT imply trigger_pr_benchmark"
    );
}

// ─── Post-slice-6 review fixes: admin-implies + soft-revoke ──────────

#[tokio::test]
async fn has_role_admin_grant_implies_trigger_pr_benchmark() {
    // Post-review M1 fix: target schema documents `admin` as "full
    // control" + the CLI exposes it as grantable. `has_role` must
    // treat an `admin` grant as implying any role within the same
    // scope, or `admin` is a lie.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    seed_install_repo(&pool, 100, 11).await;
    let store = PostgresUserStore::new(pool.clone());
    store
        .upsert_user(&NewUser {
            id: 42,
            login: "alice".into(),
            user_type: GithubAccountType::User,
        })
        .await
        .unwrap();
    // Install-wide admin grant (no trigger_pr_benchmark explicit).
    store
        .grant_role(42, 100, None, UserRole::Admin, None)
        .await
        .unwrap();

    assert!(
        store
            .has_role(42, 100, 10, UserRole::TriggerPrBenchmark)
            .await
            .unwrap(),
        "install-wide admin must imply trigger_pr_benchmark on repo=10"
    );
    assert!(
        store
            .has_role(42, 100, 11, UserRole::TriggerPrBenchmark)
            .await
            .unwrap(),
        "install-wide admin must imply trigger_pr_benchmark on any repo in the install"
    );
    assert!(
        store
            .has_role(42, 100, 10, UserRole::ViewResults)
            .await
            .unwrap(),
        "admin implies view_results too"
    );
}

#[tokio::test]
async fn has_role_repo_scoped_admin_only_implies_within_that_repo() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    seed_install_repo(&pool, 100, 11).await;
    let store = PostgresUserStore::new(pool.clone());
    store
        .upsert_user(&NewUser {
            id: 42,
            login: "alice".into(),
            user_type: GithubAccountType::User,
        })
        .await
        .unwrap();
    // Repo-scoped admin on repo=10.
    store
        .grant_role(42, 100, Some(10), UserRole::Admin, None)
        .await
        .unwrap();

    assert!(
        store
            .has_role(42, 100, 10, UserRole::TriggerPrBenchmark)
            .await
            .unwrap(),
        "repo-scoped admin implies trigger_pr_benchmark on its repo"
    );
    assert!(
        !store
            .has_role(42, 100, 11, UserRole::TriggerPrBenchmark)
            .await
            .unwrap(),
        "repo-scoped admin does NOT imply outside its repo"
    );
}

#[tokio::test]
async fn has_role_admin_does_not_cross_installation_boundary() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    seed_install_repo(&pool, 200, 10).await;
    let store = PostgresUserStore::new(pool.clone());
    store
        .upsert_user(&NewUser {
            id: 42,
            login: "alice".into(),
            user_type: GithubAccountType::User,
        })
        .await
        .unwrap();
    store
        .grant_role(42, 100, None, UserRole::Admin, None)
        .await
        .unwrap();

    assert!(
        !store
            .has_role(42, 200, 10, UserRole::TriggerPrBenchmark)
            .await
            .unwrap(),
        "admin on install=100 must NOT authorize install=200"
    );
}

#[tokio::test]
async fn revoke_role_is_soft_and_preserves_audit_history() {
    // Post-review M2 fix: revoke sets revoked_at rather than
    // DELETEing the row. The audit trail (grant id, granted_at,
    // granted_by) survives the revoke.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresUserStore::new(pool.clone());
    store
        .upsert_user(&NewUser {
            id: 42,
            login: "alice".into(),
            user_type: GithubAccountType::User,
        })
        .await
        .unwrap();
    let granted = store
        .grant_role(42, 100, Some(10), UserRole::TriggerPrBenchmark, None)
        .await
        .unwrap();
    assert!(
        granted
            .role
            .revoked_at
            .is_none(),
        "freshly granted is active"
    );

    let revoked = store
        .revoke_role(42, 100, Some(10), UserRole::TriggerPrBenchmark)
        .await
        .unwrap()
        .expect("revoke must return the affected row");
    assert_eq!(revoked.id, granted.role.id, "soft-revoke keeps the same row id");
    assert!(revoked.revoked_at.is_some(), "revoked_at must be set");
    assert_eq!(revoked.granted_at, granted.role.granted_at, "granted_at preserved");

    // has_role must now return false even though the row exists.
    assert!(
        !store
            .has_role(42, 100, 10, UserRole::TriggerPrBenchmark)
            .await
            .unwrap(),
        "has_role must filter revoked grants"
    );

    // The row is still present in the table — verifiable via list.
    let listed = store
        .list_roles(Some(100))
        .await
        .unwrap();
    assert_eq!(listed.len(), 1, "revoked row must remain in list (audit)");
    assert!(listed[0].revoked_at.is_some());
}

#[tokio::test]
async fn revoke_role_is_idempotent_for_already_revoked_row() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresUserStore::new(pool.clone());
    store
        .upsert_user(&NewUser {
            id: 42,
            login: "alice".into(),
            user_type: GithubAccountType::User,
        })
        .await
        .unwrap();
    store
        .grant_role(42, 100, Some(10), UserRole::TriggerPrBenchmark, None)
        .await
        .unwrap();
    store
        .revoke_role(42, 100, Some(10), UserRole::TriggerPrBenchmark)
        .await
        .unwrap();
    // Second revoke is a no-op.
    let again = store
        .revoke_role(42, 100, Some(10), UserRole::TriggerPrBenchmark)
        .await
        .unwrap();
    assert!(again.is_none(), "double-revoke must be a no-op");
}

#[tokio::test]
async fn grant_role_reactivates_revoked_row_preserving_audit() {
    // Re-grant after revoke clears revoked_at on the SAME row —
    // granted_at stays put so the original grant timestamp survives
    // the revoke/re-grant cycle. Same shape as
    // `github_installation_repo.add_or_restore_membership`.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresUserStore::new(pool.clone());
    store
        .upsert_user(&NewUser {
            id: 42,
            login: "alice".into(),
            user_type: GithubAccountType::User,
        })
        .await
        .unwrap();
    let original = store
        .grant_role(42, 100, Some(10), UserRole::TriggerPrBenchmark, None)
        .await
        .unwrap();
    store
        .revoke_role(42, 100, Some(10), UserRole::TriggerPrBenchmark)
        .await
        .unwrap();

    let regranted = store
        .grant_role(42, 100, Some(10), UserRole::TriggerPrBenchmark, None)
        .await
        .unwrap();
    assert!(!regranted.created, "re-grant returns created=false (same row)");
    assert_eq!(regranted.role.id, original.role.id, "same row id");
    assert!(
        regranted
            .role
            .revoked_at
            .is_none(),
        "revoked_at cleared"
    );
    assert_eq!(regranted.role.granted_at, original.role.granted_at, "granted_at preserved");

    // has_role now active again.
    assert!(
        store
            .has_role(42, 100, 10, UserRole::TriggerPrBenchmark)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn list_roles_includes_revoked_rows_for_audit() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresUserStore::new(pool.clone());
    store
        .upsert_user(&NewUser {
            id: 42,
            login: "alice".into(),
            user_type: GithubAccountType::User,
        })
        .await
        .unwrap();
    store
        .grant_role(42, 100, Some(10), UserRole::TriggerPrBenchmark, None)
        .await
        .unwrap();
    store
        .grant_role(42, 100, None, UserRole::Admin, None)
        .await
        .unwrap();
    // Revoke the trigger_pr_benchmark grant; admin stays active.
    store
        .revoke_role(42, 100, Some(10), UserRole::TriggerPrBenchmark)
        .await
        .unwrap();

    let all = store
        .list_roles(Some(100))
        .await
        .unwrap();
    assert_eq!(all.len(), 2, "list returns both active AND revoked rows");
    let revoked_count = all
        .iter()
        .filter(|r| r.revoked_at.is_some())
        .count();
    assert_eq!(revoked_count, 1);
}

#[tokio::test]
async fn revoke_repo_scoped_grants_soft_revokes_only_matching_repo() {
    // Third-pass review fix: bulk-soft-revoke MUST scope to the
    // specific (install, repo) — install-wide grants, grants for
    // other repos in the same install, and grants on other installs
    // are all untouched.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    seed_install_repo(&pool, 100, 11).await;
    seed_install_repo(&pool, 200, 10).await;
    let store = PostgresUserStore::new(pool.clone());
    store
        .upsert_user(&NewUser {
            id: 42,
            login: "alice".into(),
            user_type: GithubAccountType::User,
        })
        .await
        .unwrap();
    // Four grants: two on the target (install=100, repo=10),
    // one install-wide on install=100, one cross-install on
    // (install=200, repo=10). Only the first two should get revoked.
    store
        .grant_role(42, 100, Some(10), UserRole::TriggerPrBenchmark, None)
        .await
        .unwrap();
    store
        .grant_role(42, 100, Some(10), UserRole::Admin, None)
        .await
        .unwrap();
    store
        .grant_role(42, 100, None, UserRole::TriggerPrBenchmark, None)
        .await
        .unwrap();
    store
        .grant_role(42, 200, Some(10), UserRole::TriggerPrBenchmark, None)
        .await
        .unwrap();

    let count = store
        .revoke_repo_scoped_grants(100, 10)
        .await
        .unwrap();
    assert_eq!(count, 2, "must revoke both repo-scoped grants on (100, 10)");

    // Install-wide on install=100 untouched.
    assert!(
        store
            .has_role(42, 100, 11, UserRole::TriggerPrBenchmark)
            .await
            .unwrap(),
        "install-wide grant must survive the per-repo cascade"
    );
    // Cross-install grant untouched.
    assert!(
        store
            .has_role(42, 200, 10, UserRole::TriggerPrBenchmark)
            .await
            .unwrap()
    );
    // Repo-scoped on (100, 10) no longer authorize (revoked).
    // But install-wide on install=100 still wildcards — so has_role
    // for (42, 100, 10) is still true via the install-wide grant.
    // Verify the targeted rows specifically:
    let listed = store
        .list_roles(Some(100))
        .await
        .unwrap();
    let install_100_revoked: Vec<_> = listed
        .iter()
        .filter(|r| r.github_repo_id == Some(10) && r.revoked_at.is_some())
        .collect();
    assert_eq!(install_100_revoked.len(), 2);
}

#[tokio::test]
async fn revoke_repo_scoped_grants_is_idempotent() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresUserStore::new(pool.clone());
    store
        .upsert_user(&NewUser {
            id: 42,
            login: "alice".into(),
            user_type: GithubAccountType::User,
        })
        .await
        .unwrap();
    store
        .grant_role(42, 100, Some(10), UserRole::TriggerPrBenchmark, None)
        .await
        .unwrap();

    let first = store
        .revoke_repo_scoped_grants(100, 10)
        .await
        .unwrap();
    assert_eq!(first, 1);
    // Re-delivery: second call sees nothing active to revoke.
    let second = store
        .revoke_repo_scoped_grants(100, 10)
        .await
        .unwrap();
    assert_eq!(second, 0);
}

#[tokio::test]
async fn list_roles_filter_by_install() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    seed_install_repo(&pool, 200, 10).await;
    let store = PostgresUserStore::new(pool.clone());
    store
        .upsert_user(&NewUser {
            id: 42,
            login: "alice".into(),
            user_type: GithubAccountType::User,
        })
        .await
        .unwrap();
    store
        .grant_role(42, 100, None, UserRole::TriggerPrBenchmark, None)
        .await
        .unwrap();
    store
        .grant_role(42, 200, None, UserRole::TriggerPrBenchmark, None)
        .await
        .unwrap();

    let scoped = store
        .list_roles(Some(100))
        .await
        .unwrap();
    assert_eq!(scoped.len(), 1);
    assert_eq!(scoped[0].github_installation_id, 100);

    let all = store
        .list_roles(None)
        .await
        .unwrap();
    assert_eq!(all.len(), 2);
}
