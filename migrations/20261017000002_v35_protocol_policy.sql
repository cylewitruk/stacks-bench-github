CREATE TABLE fleet_protocol_policy (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    minimum_revision INTEGER NOT NULL DEFAULT 1 CHECK (minimum_revision > 0),
    generation BIGINT NOT NULL DEFAULT 0 CHECK (generation >= 0),
    actor TEXT NOT NULL,
    reason TEXT NOT NULL,
    rollback_evidence TEXT NOT NULL,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO fleet_protocol_policy (actor, reason, rollback_evidence)
VALUES ('migration', 'Retain deployed revision-1 compatibility', 'Initial compatibility window');

CREATE TABLE fleet_protocol_administration_audit (
    request_id UUID PRIMARY KEY,
    minimum_revision INTEGER NOT NULL CHECK (minimum_revision > 0),
    actor TEXT NOT NULL CHECK (length(actor) BETWEEN 1 AND 128),
    reason TEXT NOT NULL CHECK (length(reason) BETWEEN 1 AND 1024),
    rollback_evidence TEXT NOT NULL CHECK (length(rollback_evidence) BETWEEN 1 AND 2048),
    receipt JSONB NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TRIGGER protocol_audit_append_only
BEFORE UPDATE OR DELETE OR TRUNCATE ON fleet_protocol_administration_audit
FOR EACH STATEMENT EXECUTE FUNCTION reject_scheduler_audit_mutation();
