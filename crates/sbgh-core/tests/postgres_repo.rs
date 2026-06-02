//! Slice 4 integration tests for `PostgresRepoStore` against a real
//! Postgres. Covers identity vs lineage upsert
//! semantics, the self-referential topological insert, the support
//! gate (id-match + fork-root-match), and the operator soft-disable.

use sbgh_core::db::{NewRepoIdentity, NewRepoLineage, PostgresRepoStore, RepoStore, setup_pg_db};

#[tokio::test]
async fn lookup_repo_returns_none_for_unknown_id() {
    let (_db, pool) = setup_pg_db().await;
    let store = PostgresRepoStore::new(pool);
    assert!(
        store
            .lookup_repo(999)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn upsert_repo_identity_does_not_clobber_existing_lineage() {
    // CLI seed runs identity-only; a previously-walked lineage must
    // survive (otherwise a re-seed during operations would null out
    // the processor's runtime lineage cache).
    let (_db, pool) = setup_pg_db().await;
    let store = PostgresRepoStore::new(pool);
    // First populate full lineage (a fork).
    store
        .upsert_repo_lineage(&NewRepoLineage {
            repo: NewRepoIdentity {
                id: 20,
                owner: "alice".into(),
                name: "fork".into(),
                default_branch: Some("main".into()),
            },
            is_fork: true,
            parent: Some(NewRepoIdentity {
                id: 10,
                owner: "root".into(),
                name: "canonical".into(),
                default_branch: None,
            }),
            source: Some(NewRepoIdentity {
                id: 10,
                owner: "root".into(),
                name: "canonical".into(),
                default_branch: None,
            }),
        })
        .await
        .unwrap();
    // Now identity-only upsert: lineage columns must NOT be touched.
    let after = store
        .upsert_repo_identity(&NewRepoIdentity {
            id: 20,
            owner: "alice-renamed".into(),
            name: "fork".into(),
            default_branch: Some("dev".into()),
        })
        .await
        .unwrap();
    assert_eq!(after.owner, "alice-renamed", "identity owner refreshed");
    assert_eq!(
        after
            .default_branch
            .as_deref(),
        Some("dev")
    );
    assert_eq!(after.is_fork, Some(true), "is_fork must survive identity-only upsert");
    assert_eq!(after.fork_root_github_repo_id, Some(10), "fork_root must survive");
}

#[tokio::test]
async fn upsert_repo_lineage_inserts_ancestors_topologically() {
    // Fork-of-fork: leaf 30, parent 20, root 10. All three must end
    // up as github_repo rows; the leaf's FKs must point at 20 and 10.
    let (_db, pool) = setup_pg_db().await;
    let store = PostgresRepoStore::new(pool);
    store
        .upsert_repo_lineage(&NewRepoLineage {
            repo: NewRepoIdentity {
                id: 30,
                owner: "bob".into(),
                name: "fork".into(),
                default_branch: None,
            },
            is_fork: true,
            parent: Some(NewRepoIdentity {
                id: 20,
                owner: "alice".into(),
                name: "fork".into(),
                default_branch: None,
            }),
            source: Some(NewRepoIdentity {
                id: 10,
                owner: "root".into(),
                name: "canonical".into(),
                default_branch: None,
            }),
        })
        .await
        .unwrap();

    let leaf = store
        .lookup_repo(30)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(leaf.is_fork, Some(true));
    assert_eq!(leaf.parent_github_repo_id, Some(20));
    assert_eq!(leaf.fork_root_github_repo_id, Some(10));
    assert!(
        leaf.lineage_checked_at
            .is_some()
    );

    assert!(
        store
            .lookup_repo(20)
            .await
            .unwrap()
            .is_some(),
        "parent identity row created"
    );
    assert!(
        store
            .lookup_repo(10)
            .await
            .unwrap()
            .is_some(),
        "source identity row created"
    );
}

#[tokio::test]
async fn upsert_repo_lineage_handles_one_hop_fork_without_duplicate_ancestor_insert() {
    // One-hop fork: parent == source. The Postgres impl checks
    // Some(par.id) != source_id to skip the duplicate INSERT — verify
    // we don't crash on a conflict from inserting the same row twice.
    let (_db, pool) = setup_pg_db().await;
    let store = PostgresRepoStore::new(pool);
    let root = NewRepoIdentity {
        id: 10,
        owner: "root".into(),
        name: "canonical".into(),
        default_branch: None,
    };
    store
        .upsert_repo_lineage(&NewRepoLineage {
            repo: NewRepoIdentity {
                id: 20,
                owner: "alice".into(),
                name: "fork".into(),
                default_branch: None,
            },
            is_fork: true,
            parent: Some(root.clone()),
            source: Some(root),
        })
        .await
        .expect("one-hop fork lineage must succeed without ancestor duplicate-insert error");
}

#[tokio::test]
async fn is_supported_lineage_accepts_direct_root() {
    let (_db, pool) = setup_pg_db().await;
    let store = PostgresRepoStore::new(pool);
    store
        .upsert_repo_identity(&NewRepoIdentity {
            id: 10,
            owner: "stacks-network".into(),
            name: "stacks-core".into(),
            default_branch: None,
        })
        .await
        .unwrap();
    store
        .upsert_supported_root(10, Some("canonical"))
        .await
        .unwrap();

    assert!(
        store
            .is_supported_lineage(10)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn is_supported_lineage_accepts_fork_of_supported_root() {
    let (_db, pool) = setup_pg_db().await;
    let store = PostgresRepoStore::new(pool);
    // Seed root + supported entry.
    store
        .upsert_repo_identity(&NewRepoIdentity {
            id: 10,
            owner: "root".into(),
            name: "canonical".into(),
            default_branch: None,
        })
        .await
        .unwrap();
    store
        .upsert_supported_root(10, None)
        .await
        .unwrap();
    // Seed fork with fork_root pointing at the supported root.
    store
        .upsert_repo_lineage(&NewRepoLineage {
            repo: NewRepoIdentity {
                id: 20,
                owner: "alice".into(),
                name: "fork".into(),
                default_branch: None,
            },
            is_fork: true,
            parent: Some(NewRepoIdentity {
                id: 10,
                owner: "root".into(),
                name: "canonical".into(),
                default_branch: None,
            }),
            source: Some(NewRepoIdentity {
                id: 10,
                owner: "root".into(),
                name: "canonical".into(),
                default_branch: None,
            }),
        })
        .await
        .unwrap();

    assert!(
        store
            .is_supported_lineage(20)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn is_supported_lineage_rejects_disabled_root() {
    let (_db, pool) = setup_pg_db().await;
    let store = PostgresRepoStore::new(pool);
    store
        .upsert_repo_identity(&NewRepoIdentity {
            id: 10,
            owner: "stacks-network".into(),
            name: "stacks-core".into(),
            default_branch: None,
        })
        .await
        .unwrap();
    store
        .upsert_supported_root(10, None)
        .await
        .unwrap();
    store
        .disable_supported_root(10)
        .await
        .unwrap();

    assert!(
        !store
            .is_supported_lineage(10)
            .await
            .unwrap(),
        "disabled supported_repo_root must NOT count as supported"
    );
}

#[tokio::test]
async fn is_supported_lineage_returns_false_for_unknown_repo() {
    let (_db, pool) = setup_pg_db().await;
    let store = PostgresRepoStore::new(pool);
    assert!(
        !store
            .is_supported_lineage(999)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn disable_supported_root_returns_none_for_unknown() {
    let (_db, pool) = setup_pg_db().await;
    let store = PostgresRepoStore::new(pool);
    assert!(
        store
            .disable_supported_root(999)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn list_supported_roots_returns_join_with_owner_name() {
    let (_db, pool) = setup_pg_db().await;
    let store = PostgresRepoStore::new(pool);
    store
        .upsert_repo_identity(&NewRepoIdentity {
            id: 10,
            owner: "stacks-network".into(),
            name: "stacks-core".into(),
            default_branch: None,
        })
        .await
        .unwrap();
    store
        .upsert_supported_root(10, Some("canonical"))
        .await
        .unwrap();

    let rows = store
        .list_supported_roots()
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.github_repo_id, 10);
    assert_eq!(row.owner, "stacks-network");
    assert_eq!(row.name, "stacks-core");
    assert_eq!(row.note.as_deref(), Some("canonical"));
}
