-- Read-only recovery evidence for one job, including superseded attempts.
-- Run with psql -X -v ON_ERROR_STOP=1 -v job_id=<job-uuid> -f this-file.
-- This records database state, not proof of physical VM/resource teardown.
\set ON_ERROR_STOP on
\pset pager off
\x on

BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout = '10s';
SET LOCAL lock_timeout = '2s';
SET LOCAL timezone = 'UTC';

SELECT CURRENT_TIMESTAMP AS captured_at,
       :'job_id'::uuid AS requested_job_id,
       EXISTS (SELECT 1 FROM job WHERE id = :'job_id'::uuid) AS job_exists;

SELECT id AS job_id, status, created_at, updated_at
  FROM job
 WHERE id = :'job_id'::uuid;

SELECT attempt_id, worker_id, worker_session_id, status,
       fencing_generation, execution_generation,
       created_at, accepted_at, lease_expires_at, terminal_at, updated_at,
       terminal_reliable_seq, terminal_payload_digest
  FROM worker_attempt
 WHERE job_id = :'job_id'::uuid
 ORDER BY fencing_generation;

SELECT id AS obligation_id, attempt_id, worker_id, worker_session_id,
       status, requeue_job, reason, created_at, completed_at
  FROM worker_cleanup_obligation
 WHERE job_id = :'job_id'::uuid
 ORDER BY id;

SELECT worker_session_id, worker_id, status,
       started_at, last_heartbeat_at, expires_at, ended_at
  FROM worker_session
 WHERE worker_session_id IN (
       SELECT worker_session_id
         FROM worker_attempt
        WHERE job_id = :'job_id'::uuid
 )
 ORDER BY started_at, worker_session_id;

COMMIT;
