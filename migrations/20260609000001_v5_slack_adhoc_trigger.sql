-- v5 (item 0002): Slack ad-hoc profiling trigger.
--
-- Slack is a new trigger source for the ad-hoc, no-commit profiling case: an
-- `@sbgh bench --block/--txid …` mention enqueues a job whose code-under-test is
-- a constant (`[slack].default_rev`) and whose workload is the variable. The new
-- `trigger_kind` value distinguishes those jobs from the GitHub-driven ones.
--
-- `ADD VALUE IF NOT EXISTS` is transaction-safe on PG 12+ (we're on 18) and
-- idempotent; the new value is not USED in this migration.

ALTER TYPE trigger_kind ADD VALUE IF NOT EXISTS 'slack_adhoc';
