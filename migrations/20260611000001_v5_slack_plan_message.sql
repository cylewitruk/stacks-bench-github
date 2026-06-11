-- v5 (item 0002): Slack live-timeline plan message.
--
-- The Slack ad-hoc result card is a `plan` block posted when the job starts
-- running and `chat.update`d as it advances (Build → Benchmark → Archive). To
-- resume updating the SAME card after a daemon restart (a reclaimed job), the
-- posted message's Slack `ts` is persisted as a `plan_message_sent` job_event
-- (the ts string lives in `detail->>'plan_message_ts'`) and read back at claim
-- time — mirroring how `comment_posted` / `check_run_created` carry the PR
-- comment / Check Run identity.
--
-- `ADD VALUE IF NOT EXISTS` is transaction-safe on PG 12+ (we're on 18) and
-- idempotent.

ALTER TYPE job_event_kind ADD VALUE IF NOT EXISTS 'plan_message_sent';
