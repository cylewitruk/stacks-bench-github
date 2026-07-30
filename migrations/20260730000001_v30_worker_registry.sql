CREATE TABLE worker_certificate (
    certificate_sha256 BYTEA PRIMARY KEY,
    worker_id UUID NOT NULL REFERENCES worker_registry(worker_id) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at TIMESTAMPTZ,
    CHECK (octet_length(certificate_sha256) = 32),
    CHECK (revoked_at IS NULL OR revoked_at >= created_at),
    UNIQUE (worker_id, certificate_sha256)
);

CREATE INDEX worker_certificate_active_worker_idx
    ON worker_certificate (worker_id)
    WHERE revoked_at IS NULL;

CREATE FUNCTION prevent_worker_certificate_delete()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION
        'worker certificate fingerprints are immutable audit history';
END;
$$;

CREATE TRIGGER worker_certificate_no_delete
    BEFORE DELETE ON worker_certificate
    FOR EACH ROW
    EXECUTE FUNCTION prevent_worker_certificate_delete();

ALTER TABLE worker_session
    ADD COLUMN certificate_sha256 BYTEA,
    ADD CONSTRAINT worker_session_certificate_fk
        FOREIGN KEY (worker_id, certificate_sha256)
        REFERENCES worker_certificate(worker_id, certificate_sha256)
        ON DELETE RESTRICT,
    ADD CONSTRAINT worker_session_certificate_length
        CHECK (
            certificate_sha256 IS NULL
            OR octet_length(certificate_sha256) = 32
        );

COMMENT ON COLUMN worker_session.certificate_sha256 IS
    'Authenticated leaf certificate for v30+ sessions; NULL only for pre-v30 history.';
