-- v25: durable worker-fleet control/data plane and block-validation results.

CREATE TYPE worker_capability AS ENUM (
    'benchmark',
    'build_only',
    'block_validation'
);

CREATE TYPE worker_session_status AS ENUM (
    'registered',
    'idle',
    'offered',
    'running',
    'draining',
    'offline',
    'deregistered'
);

CREATE TYPE worker_attempt_status AS ENUM (
    'offered',
    'running',
    'cancel_requested',
    'completed',
    'failed',
    'cancelled',
    'expired',
    'fenced'
);

CREATE TYPE artifact_staging_status AS ENUM (
    'granted',
    'uploaded',
    'verified',
    'promoted',
    'rejected',
    'reaped'
);

CREATE TYPE cleanup_obligation_status AS ENUM (
    'pending',
    'completed',
    'abandoned'
);

CREATE TABLE worker_registry (
    worker_id UUID PRIMARY KEY,
    display_name TEXT NOT NULL,
    allowed_capabilities worker_capability[] NOT NULL,
    measurement_profile TEXT,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    draining BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (cardinality(allowed_capabilities) > 0),
    CHECK (measurement_profile IS NULL OR length(measurement_profile) BETWEEN 1 AND 128)
);

CREATE TABLE worker_session (
    worker_session_id UUID PRIMARY KEY,
    worker_id UUID NOT NULL REFERENCES worker_registry(worker_id) ON DELETE RESTRICT,
    status worker_session_status NOT NULL,
    protocol_version INTEGER NOT NULL CHECK (protocol_version > 0),
    software_version TEXT NOT NULL,
    advertised_capabilities worker_capability[] NOT NULL,
    effective_capabilities worker_capability[] NOT NULL,
    resource_facts JSONB NOT NULL,
    reliable_buffer_len INTEGER NOT NULL DEFAULT 0
        CHECK (reliable_buffer_len BETWEEN 0 AND 4096),
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ,
    CHECK (cardinality(advertised_capabilities) > 0),
    CHECK (cardinality(effective_capabilities) > 0),
    CHECK ((status IN ('offline', 'deregistered')) = (ended_at IS NOT NULL))
);

CREATE UNIQUE INDEX worker_one_live_session_idx
    ON worker_session (worker_id)
    WHERE status IN ('registered', 'idle', 'offered', 'running', 'draining');
CREATE INDEX worker_session_expiry_idx
    ON worker_session (expires_at)
    WHERE status NOT IN ('offline', 'deregistered');

ALTER TABLE benchmark_group
    ADD COLUMN worker_id UUID REFERENCES worker_registry(worker_id) ON DELETE RESTRICT,
    ADD COLUMN measurement_profile TEXT,
    ADD COLUMN recovery_of_group_id UUID
        REFERENCES benchmark_group(id) ON DELETE RESTRICT,
    ADD COLUMN execution_generation BIGINT NOT NULL DEFAULT 1
        CHECK (execution_generation > 0),
    ADD COLUMN fencing_generation BIGINT NOT NULL DEFAULT 0
        CHECK (fencing_generation >= 0);

CREATE INDEX benchmark_group_recovery_idx
    ON benchmark_group (recovery_of_group_id, execution_generation)
    WHERE recovery_of_group_id IS NOT NULL;

-- A task is schedulable by the fleet only after the daemon has resolved every
-- mutable input and persisted this immutable, canonically-hashed payload.
ALTER TABLE job
    ADD COLUMN execution_payload_version INTEGER,
    ADD COLUMN execution_payload JSONB,
    ADD COLUMN execution_payload_hash TEXT,
    ADD CONSTRAINT job_execution_payload_all_or_none CHECK (
        (execution_payload_version IS NULL
         AND execution_payload IS NULL
         AND execution_payload_hash IS NULL)
        OR
        (execution_payload_version > 0
         AND execution_payload IS NOT NULL
         AND execution_payload_hash ~ '^[0-9a-f]{64}$')
    );

CREATE TABLE worker_attempt (
    attempt_id UUID PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES job(id) ON DELETE RESTRICT,
    benchmark_group_id UUID NOT NULL REFERENCES benchmark_group(id) ON DELETE RESTRICT,
    worker_id UUID NOT NULL REFERENCES worker_registry(worker_id) ON DELETE RESTRICT,
    worker_session_id UUID NOT NULL REFERENCES worker_session(worker_session_id) ON DELETE RESTRICT,
    status worker_attempt_status NOT NULL,
    fencing_generation BIGINT NOT NULL CHECK (fencing_generation > 0),
    execution_generation BIGINT NOT NULL CHECK (execution_generation > 0),
    claim_token UUID NOT NULL,
    trace_id UUID NOT NULL UNIQUE,
    capability worker_capability NOT NULL,
    payload_hash TEXT NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
    offer_expires_at TIMESTAMPTZ NOT NULL,
    lease_expires_at TIMESTAMPTZ NOT NULL,
    highest_contiguous_reliable_seq BIGINT NOT NULL DEFAULT 0
        CHECK (highest_contiguous_reliable_seq >= 0),
    projected_reliable_seq BIGINT NOT NULL DEFAULT 0
        CHECK (projected_reliable_seq >= 0
               AND projected_reliable_seq <= highest_contiguous_reliable_seq),
    terminal_reliable_seq BIGINT CHECK (terminal_reliable_seq > 0),
    terminal_payload_digest TEXT
        CHECK (terminal_payload_digest IS NULL
               OR terminal_payload_digest ~ '^[0-9a-f]{64}$'),
    terminal_outcome JSONB,
    terminal_artifacts JSONB,
    accepted_at TIMESTAMPTZ,
    terminal_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (job_id, fencing_generation),
    CHECK (status <> 'offered' OR accepted_at IS NULL),
    CHECK (status NOT IN ('running', 'cancel_requested', 'completed', 'failed', 'cancelled')
           OR accepted_at IS NOT NULL),
    CHECK (
        (status IN ('completed', 'failed', 'cancelled')
         AND terminal_at IS NOT NULL
         AND terminal_reliable_seq IS NOT NULL
         AND terminal_payload_digest IS NOT NULL
         AND terminal_outcome IS NOT NULL
         AND terminal_artifacts IS NOT NULL)
        OR
        (status NOT IN ('completed', 'failed', 'cancelled')
         AND terminal_at IS NULL
         AND terminal_reliable_seq IS NULL
         AND terminal_payload_digest IS NULL
         AND terminal_outcome IS NULL
         AND terminal_artifacts IS NULL)
    )
);

CREATE UNIQUE INDEX worker_attempt_one_active_job_idx
    ON worker_attempt (job_id)
    WHERE status IN ('offered', 'running', 'cancel_requested');
CREATE UNIQUE INDEX worker_attempt_one_active_unit_idx
    ON worker_attempt (benchmark_group_id)
    WHERE status IN ('offered', 'running', 'cancel_requested');
CREATE INDEX worker_attempt_lease_expiry_idx
    ON worker_attempt (lease_expires_at)
    WHERE status IN ('offered', 'running', 'cancel_requested');
CREATE INDEX worker_attempt_offer_expiry_idx
    ON worker_attempt (offer_expires_at)
    WHERE status = 'offered';
CREATE INDEX worker_attempt_session_idx
    ON worker_attempt (worker_session_id, status);

CREATE TABLE worker_attempt_event (
    attempt_id UUID NOT NULL REFERENCES worker_attempt(attempt_id) ON DELETE RESTRICT,
    reliable_seq BIGINT NOT NULL CHECK (reliable_seq > 0),
    trace_id UUID NOT NULL,
    payload_kind TEXT NOT NULL,
    payload JSONB NOT NULL,
    payload_digest TEXT NOT NULL CHECK (payload_digest ~ '^[0-9a-f]{64}$'),
    worker_timestamp TIMESTAMPTZ,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    projected_at TIMESTAMPTZ,
    projection_discarded_reason TEXT,
    CHECK (projection_discarded_reason IS NULL OR projected_at IS NOT NULL),
    PRIMARY KEY (attempt_id, reliable_seq)
);

CREATE INDEX worker_attempt_event_projection_idx
    ON worker_attempt_event (attempt_id, reliable_seq)
    WHERE projected_at IS NULL;

-- Fine progress is best-effort on the wire but durable once acknowledged.
-- It has its own sequence so dropping one update never stalls reliable phase
-- or terminal projection.
CREATE TABLE worker_attempt_progress (
    attempt_id UUID NOT NULL REFERENCES worker_attempt(attempt_id) ON DELETE RESTRICT,
    progress_seq BIGINT NOT NULL CHECK (progress_seq > 0),
    trace_id UUID NOT NULL,
    update JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    projected_at TIMESTAMPTZ,
    projection_discarded_reason TEXT,
    CHECK (projection_discarded_reason IS NULL OR projected_at IS NOT NULL),
    PRIMARY KEY (attempt_id, progress_seq)
);

CREATE INDEX worker_attempt_progress_projection_idx
    ON worker_attempt_progress (received_at)
    WHERE projected_at IS NULL;

CREATE TABLE worker_artifact_staging (
    attempt_id UUID NOT NULL REFERENCES worker_attempt(attempt_id) ON DELETE RESTRICT,
    object_key TEXT NOT NULL,
    logical_key TEXT NOT NULL,
    status artifact_staging_status NOT NULL,
    size_bytes BIGINT CHECK (size_bytes IS NULL OR size_bytes >= 0),
    sha256 TEXT CHECK (sha256 IS NULL OR sha256 ~ '^[0-9a-f]{64}$'),
    grant_expires_at TIMESTAMPTZ,
    verified_at TIMESTAMPTZ,
    promoted_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (attempt_id, object_key),
    UNIQUE (attempt_id, logical_key),
    CHECK (object_key !~ '(^|/)\\.\\.?(/|$)' AND object_key !~ '^/'),
    CHECK (logical_key !~ '(^|/)\\.\\.?(/|$)' AND logical_key !~ '^/')
);

CREATE INDEX worker_artifact_staging_gc_idx
    ON worker_artifact_staging (created_at)
    WHERE status IN ('granted', 'uploaded', 'verified', 'rejected');

CREATE TABLE worker_cleanup_obligation (
    id BIGSERIAL PRIMARY KEY,
    worker_id UUID NOT NULL REFERENCES worker_registry(worker_id) ON DELETE RESTRICT,
    worker_session_id UUID NOT NULL REFERENCES worker_session(worker_session_id) ON DELETE RESTRICT,
    attempt_id UUID NOT NULL REFERENCES worker_attempt(attempt_id) ON DELETE RESTRICT,
    job_id UUID NOT NULL REFERENCES job(id) ON DELETE RESTRICT,
    status cleanup_obligation_status NOT NULL DEFAULT 'pending',
    requeue_job BOOLEAN NOT NULL,
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    UNIQUE (attempt_id),
    CHECK ((status = 'pending') = (completed_at IS NULL))
);

CREATE TABLE block_validation_dataset (
    generation TEXT PRIMARY KEY,
    worker_id UUID NOT NULL REFERENCES worker_registry(worker_id) ON DELETE RESTRICT,
    network TEXT NOT NULL,
    format_version TEXT NOT NULL,
    covered_start BIGINT NOT NULL CHECK (covered_start >= 0),
    covered_end BIGINT NOT NULL CHECK (covered_end >= covered_start),
    manifest_sha256 TEXT NOT NULL CHECK (manifest_sha256 ~ '^[0-9a-f]{64}$'),
    is_current BOOLEAN NOT NULL DEFAULT FALSE,
    verified_at TIMESTAMPTZ NOT NULL,
    retired_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (worker_id, manifest_sha256)
);

CREATE UNIQUE INDEX block_validation_one_current_dataset_idx
    ON block_validation_dataset (worker_id, network)
    WHERE is_current AND retired_at IS NULL;

CREATE TABLE block_validation_result (
    job_id UUID PRIMARY KEY REFERENCES job(id) ON DELETE RESTRICT,
    attempt_id UUID NOT NULL UNIQUE REFERENCES worker_attempt(attempt_id) ON DELETE RESTRICT,
    dataset_generation TEXT NOT NULL
        REFERENCES block_validation_dataset(generation) ON DELETE RESTRICT,
    valid BOOLEAN NOT NULL,
    checked_blocks BIGINT NOT NULL CHECK (checked_blocks >= 0),
    invalid_blocks JSONB NOT NULL,
    artifact_manifest JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TYPE job_event_kind ADD VALUE IF NOT EXISTS 'phase_started';
ALTER TYPE job_event_kind ADD VALUE IF NOT EXISTS 'phase_finished';
