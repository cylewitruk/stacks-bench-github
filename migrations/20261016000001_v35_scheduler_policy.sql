CREATE TABLE scheduler_policy (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    paused BOOLEAN NOT NULL DEFAULT FALSE,
    generation BIGINT NOT NULL DEFAULT 0 CHECK (generation >= 0),
    actor TEXT NOT NULL,
    reason TEXT NOT NULL,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO scheduler_policy (actor, reason)
VALUES ('migration', 'Initial running scheduler policy');

CREATE TABLE scheduler_administration_audit (
    request_id UUID PRIMARY KEY,
    paused BOOLEAN NOT NULL,
    actor TEXT NOT NULL CHECK (length(actor) BETWEEN 1 AND 128),
    reason TEXT NOT NULL CHECK (length(reason) BETWEEN 1 AND 1024),
    generation BIGINT NOT NULL CHECK (generation >= 0),
    policy_actor TEXT NOT NULL,
    policy_reason TEXT NOT NULL,
    policy_changed_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE FUNCTION reject_scheduler_audit_mutation() RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'scheduler administration audit is append-only';
END;
$$;

CREATE TRIGGER scheduler_audit_append_only
BEFORE UPDATE OR DELETE OR TRUNCATE ON scheduler_administration_audit
FOR EACH STATEMENT EXECUTE FUNCTION reject_scheduler_audit_mutation();

-- Expand-only: previous daemons can still insert attempts. They do not enforce
-- this policy and must not serve while scheduler maintenance is relied upon.
ALTER TABLE worker_attempt ADD COLUMN scheduler_generation BIGINT NOT NULL DEFAULT 0;
