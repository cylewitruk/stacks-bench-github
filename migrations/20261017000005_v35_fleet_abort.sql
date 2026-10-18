CREATE TABLE fleet_abort_audit (
    request_id UUID PRIMARY KEY,
    receipt JSONB NOT NULL CHECK (jsonb_typeof(receipt) = 'object'),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TRIGGER fleet_abort_audit_append_only
BEFORE UPDATE OR DELETE OR TRUNCATE ON fleet_abort_audit
FOR EACH STATEMENT EXECUTE FUNCTION reject_scheduler_audit_mutation();
