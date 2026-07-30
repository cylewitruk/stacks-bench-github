CREATE TABLE worker_identity_key (
    identity_key_sha256 BYTEA PRIMARY KEY,
    worker_id UUID NOT NULL REFERENCES worker_registry(worker_id) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at TIMESTAMPTZ,
    CHECK (octet_length(identity_key_sha256) = 32),
    CHECK (revoked_at IS NULL OR revoked_at >= created_at),
    UNIQUE (worker_id, identity_key_sha256)
);

CREATE INDEX worker_identity_key_active_worker_idx
    ON worker_identity_key (worker_id)
    WHERE revoked_at IS NULL;

CREATE FUNCTION prevent_worker_identity_key_delete()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION
        'worker identity digests are immutable audit history';
END;
$$;

CREATE TRIGGER worker_identity_key_no_delete
    BEFORE DELETE ON worker_identity_key
    FOR EACH ROW
    EXECUTE FUNCTION prevent_worker_identity_key_delete();

ALTER TABLE worker_session
    ADD COLUMN identity_key_sha256 BYTEA NOT NULL,
    ADD CONSTRAINT worker_session_identity_key_fk
        FOREIGN KEY (worker_id, identity_key_sha256)
        REFERENCES worker_identity_key(worker_id, identity_key_sha256)
        ON DELETE RESTRICT,
    ADD CONSTRAINT worker_session_identity_key_length
        CHECK (octet_length(identity_key_sha256) = 32);

COMMENT ON COLUMN worker_session.identity_key_sha256 IS
    'Authenticated worker SPKI SHA-256 used for this session.';
