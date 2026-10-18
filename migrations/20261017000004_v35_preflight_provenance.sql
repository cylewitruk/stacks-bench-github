-- Successful local checks are immutable registration facts, not authorization.
ALTER TABLE worker_session ADD COLUMN preflight_report JSONB;
ALTER TABLE worker_session ADD CONSTRAINT worker_session_preflight_object
    CHECK (preflight_report IS NULL OR jsonb_typeof(preflight_report) = 'object');
