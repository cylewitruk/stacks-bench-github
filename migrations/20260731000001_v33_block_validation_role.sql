-- v33 / 0067: task-specific GitHub authorization for `/validate`.
--
-- PostgreSQL does not permit use of a newly-added enum value until the
-- transaction which adds it commits. Keep this migration add-value-only;
-- grants are created later through the admin API/CLI.
ALTER TYPE user_role ADD VALUE IF NOT EXISTS 'trigger_block_validation';
