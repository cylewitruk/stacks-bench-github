-- roadmap-v3 Phase 6: collapse the DB role split.
--
-- The handler (Phase 4) and CLI (Phase 5) are now API clients; the
-- daemon is the sole DB client and connects as the owner. The narrow
-- `sbgh_handler` / `sbgh_orch` roles (formerly provisioned by the retired
-- `sbgh-cli migrate` → `apply_roles`) are unused — drop them.
--
-- Best-effort and idempotent:
--   * Guarded on existence, so it's a no-op on fresh databases (CI, tests)
--     where the roles were never created.
--   * `DROP OWNED BY` first removes the roles' grants (a role can't be
--     dropped while privileges still depend on it); then `DROP ROLE`.
--   * Wrapped so a Postgres that denies the owner `DROP ROLE` (e.g. a
--     managed instance without CREATEROLE) logs a notice instead of failing
--     startup. The leftover roles are harmless — manual cleanup only.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'sbgh_handler') THEN
        BEGIN
            EXECUTE 'DROP OWNED BY sbgh_handler';
            EXECUTE 'DROP ROLE sbgh_handler';
        EXCEPTION
            WHEN insufficient_privilege THEN
                RAISE NOTICE 'skipping DROP ROLE sbgh_handler (insufficient privilege)';
        END;
    END IF;

    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'sbgh_orch') THEN
        BEGIN
            EXECUTE 'DROP OWNED BY sbgh_orch';
            EXECUTE 'DROP ROLE sbgh_orch';
        EXCEPTION
            WHEN insufficient_privilege THEN
                RAISE NOTICE 'skipping DROP ROLE sbgh_orch (insufficient privilege)';
        END;
    END IF;
END $$;
