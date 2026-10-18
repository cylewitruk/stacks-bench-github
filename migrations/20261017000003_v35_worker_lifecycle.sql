ALTER TABLE worker_registry
    ADD COLUMN lifecycle_generation BIGINT NOT NULL DEFAULT 0 CHECK (lifecycle_generation >= 0),
    ADD COLUMN shutdown_session_id UUID REFERENCES worker_session(worker_session_id) ON DELETE RESTRICT;

CREATE TABLE worker_lifecycle_audit (
    request_id UUID PRIMARY KEY,
    worker_id UUID NOT NULL REFERENCES worker_registry(worker_id) ON DELETE RESTRICT,
    action TEXT NOT NULL CHECK (action IN ('cordon', 'uncordon', 'shutdown_after_idle')),
    actor TEXT NOT NULL CHECK (length(actor) BETWEEN 1 AND 128),
    reason TEXT NOT NULL CHECK (length(reason) BETWEEN 1 AND 1024),
    receipt JSONB NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TRIGGER worker_lifecycle_audit_append_only
BEFORE UPDATE OR DELETE OR TRUNCATE ON worker_lifecycle_audit
FOR EACH STATEMENT EXECUTE FUNCTION reject_scheduler_audit_mutation();
