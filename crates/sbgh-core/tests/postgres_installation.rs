//! Slice 3 integration tests for `PostgresInstallationStore` against a
//! real testcontainers Postgres. Covers the allowlist-FK enforcement,
//! upsert semantics, suspend/unsuspend, deletion, and the
//! lookup-after-disable case the processor relies on.

use sbgh_core::db::{InstallationStore, NewInstallation, PostgresInstallationStore, setup_pg};
use sbgh_core::models::GithubAccountType;

async fn seed_allowed(pool: &sbgh_core::db::Pool, account_id: i64, login: &str, is_enabled: bool) {
    sqlx::query(
        "INSERT INTO allowed_installer (github_account_id, account_login, account_type, \
         is_enabled) VALUES ($1, $2, 'organization', $3)",
    )
    .bind(account_id)
    .bind(login)
    .bind(is_enabled)
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn lookup_allowed_returns_disabled_rows_too() {
    // The processor relies on getting back is_enabled=FALSE rows so it
    // can take the deny path. If lookup_allowed silently filtered
    // disabled rows, a paused installer would be treated like a NEW
    // installer (potentially a different deny outcome later, or worse
    // — slipping past entirely if the deny was conflated with "missing").
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_allowed(&pool, 42, "octo", false).await;
    let store = PostgresInstallationStore::new(pool);

    let row = store
        .lookup_allowed(42)
        .await
        .unwrap();
    let row = row.expect("disabled row must still be returned");
    assert!(!row.is_enabled, "disabled flag must survive the round-trip");
}

#[tokio::test]
async fn lookup_allowed_returns_none_for_unknown_account() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    let store = PostgresInstallationStore::new(pool);
    assert!(
        store
            .lookup_allowed(999)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn upsert_installation_fails_without_allowlist_row() {
    // FK from github_installation.github_account_id to
    // allowed_installer.github_account_id enforces "every install is
    // backed by a current allowlist row". The processor calls
    // lookup_allowed BEFORE upsert; this test verifies the DB also
    // enforces it as a backstop — if the lookup was somehow skipped,
    // the FK fails the insert.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    let store = PostgresInstallationStore::new(pool);

    let result = store
        .upsert_installation(&NewInstallation {
            id: 100,
            github_account_id: 42,
            account_login: "octo".into(),
            account_type: GithubAccountType::Organization,
        })
        .await;
    assert!(
        result.is_err(),
        "FK to allowed_installer must reject installs whose account isn't allowlisted; got: \
         {result:?}"
    );
}

#[tokio::test]
async fn upsert_installation_updates_on_pk_conflict() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_allowed(&pool, 42, "octo", true).await;
    let store = PostgresInstallationStore::new(pool);

    store
        .upsert_installation(&NewInstallation {
            id: 100,
            github_account_id: 42,
            account_login: "octo".into(),
            account_type: GithubAccountType::Organization,
        })
        .await
        .unwrap();
    // Second upsert with renamed login + flipped type.
    let updated = store
        .upsert_installation(&NewInstallation {
            id: 100,
            github_account_id: 42,
            account_login: "octo-org".into(),
            account_type: GithubAccountType::User,
        })
        .await
        .unwrap();
    assert_eq!(updated.account_login, "octo-org");
    assert_eq!(updated.account_type, GithubAccountType::User);
    assert!(
        updated.suspended_at.is_none(),
        "upsert must NOT clobber suspended_at — that's set_suspended's job"
    );
}

#[tokio::test]
async fn set_suspended_returns_none_for_unknown_install() {
    // suspend webhook for an install we never accepted is a no-op.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    let store = PostgresInstallationStore::new(pool);

    let result = store
        .set_suspended(999, Some(chrono::Utc::now()))
        .await
        .unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn set_suspended_roundtrips_via_clear() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_allowed(&pool, 42, "octo", true).await;
    let store = PostgresInstallationStore::new(pool);
    store
        .upsert_installation(&NewInstallation {
            id: 100,
            github_account_id: 42,
            account_login: "octo".into(),
            account_type: GithubAccountType::Organization,
        })
        .await
        .unwrap();

    let now = chrono::Utc::now();
    let suspended = store
        .set_suspended(100, Some(now))
        .await
        .unwrap()
        .unwrap();
    assert!(
        suspended
            .suspended_at
            .is_some()
    );

    let cleared = store
        .set_suspended(100, None)
        .await
        .unwrap()
        .unwrap();
    assert!(cleared.suspended_at.is_none(), "set_suspended(None) MUST clear the timestamp");
}

#[tokio::test]
async fn delete_installation_returns_false_for_unknown() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    let store = PostgresInstallationStore::new(pool);
    assert!(
        !store
            .delete_installation(999)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn delete_installation_nulls_resolved_fk_on_dependent_webhook_rows() {
    // Codex M2 fix: github_webhook.github_installation_id is declared
    // ON DELETE SET NULL so slice 3's hard DELETE of github_installation
    // doesn't trip the FK if a writer (slice 4+) starts populating the
    // column. The webhook row itself stays in place — its raw
    // `payload_installation_id` still records what the payload claimed.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_allowed(&pool, 42, "octo", true).await;
    let store = PostgresInstallationStore::new(pool.clone());
    store
        .upsert_installation(&NewInstallation {
            id: 100,
            github_account_id: 42,
            account_login: "octo".into(),
            account_type: GithubAccountType::Organization,
        })
        .await
        .unwrap();
    // Insert a webhook row with the resolved FK populated (no writer
    // sets this in slice 3, but we simulate it here to pin the FK
    // semantic for future slices).
    sqlx::query(
        "INSERT INTO github_webhook
             (delivery_id, event_type, payload_size_bytes, github_installation_id)
         VALUES ('fk-test-1', 'installation', 0, 100)",
    )
    .execute(&pool)
    .await
    .unwrap();

    assert!(
        store
            .delete_installation(100)
            .await
            .unwrap()
    );

    // Webhook row survives; resolved FK is now NULL.
    let resolved: Option<i64> = sqlx::query_scalar(
        "SELECT github_installation_id FROM github_webhook WHERE delivery_id = 'fk-test-1'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        resolved.is_none(),
        "ON DELETE SET NULL must null the resolved FK on dependent webhook rows"
    );
}

#[tokio::test]
async fn delete_installation_succeeds_when_no_dependents_exist() {
    // Slice 3: no FK-referencing tables yet (memberships/policies land
    // in slices 4-5), so hard DELETE works. Slices 4-5 will replace
    // this path with soft-revoke + this test will need to assert the
    // dependents soft-revoke too.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_allowed(&pool, 42, "octo", true).await;
    let store = PostgresInstallationStore::new(pool);
    store
        .upsert_installation(&NewInstallation {
            id: 100,
            github_account_id: 42,
            account_login: "octo".into(),
            account_type: GithubAccountType::Organization,
        })
        .await
        .unwrap();

    assert!(
        store
            .delete_installation(100)
            .await
            .unwrap()
    );
    assert!(
        !store
            .delete_installation(100)
            .await
            .unwrap(),
        "second delete must be Ok(false)"
    );
}
