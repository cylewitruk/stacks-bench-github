-- v27.2: aggregate idempotency/provenance, explicit scheduling constraints
-- versus assignments, and task-neutral per-job capability.

DO $$
DECLARE
    in_flight_jobs BIGINT;
    active_attempts BIGINT;
    pending_cleanup BIGINT;
BEGIN
    SELECT count(*)
      INTO in_flight_jobs
      FROM job
     WHERE status IN ('claimed', 'running');

    SELECT count(*)
      INTO active_attempts
      FROM worker_attempt
     WHERE status IN ('offered', 'running', 'cancel_requested');

    SELECT count(*)
      INTO pending_cleanup
      FROM worker_cleanup_obligation
     WHERE status = 'pending';

    IF in_flight_jobs > 0 OR active_attempts > 0 OR pending_cleanup > 0 THEN
        RAISE EXCEPTION
            'drain in-flight execution before applying the v27.2 submission migration (claimed/running jobs: %, active attempts: %, pending cleanup obligations: %)',
            in_flight_jobs, active_attempts, pending_cleanup;
    END IF;
END
$$;

ALTER TABLE task_submission
    ADD COLUMN contract_version INTEGER,
    ADD COLUMN request_digest TEXT,
    ADD COLUMN required_worker_id UUID
        REFERENCES worker_registry(worker_id) ON DELETE RESTRICT,
    ADD COLUMN required_measurement_profile TEXT,
    ADD COLUMN assigned_worker_id UUID
        REFERENCES worker_registry(worker_id) ON DELETE RESTRICT,
    ADD COLUMN assigned_measurement_profile TEXT,
    ADD CONSTRAINT task_submission_contract_all_or_none CHECK (
        (contract_version IS NULL AND request_digest IS NULL)
        OR
        (contract_version > 0 AND request_digest ~ '^[0-9a-f]{64}$')
    ),
    ADD CONSTRAINT task_submission_required_profile_valid CHECK (
        required_measurement_profile IS NULL
        OR length(required_measurement_profile) BETWEEN 1 AND 128
    ),
    ADD CONSTRAINT task_submission_assigned_profile_valid CHECK (
        assigned_measurement_profile IS NULL
        OR length(assigned_measurement_profile) BETWEEN 1 AND 128
    ),
    ADD CONSTRAINT task_submission_worker_placement_consistent CHECK (
        required_worker_id IS NULL
        OR assigned_worker_id IS NULL
        OR required_worker_id = assigned_worker_id
    ),
    ADD CONSTRAINT task_submission_profile_placement_consistent CHECK (
        required_measurement_profile IS NULL
        OR assigned_measurement_profile IS NULL
        OR required_measurement_profile = assigned_measurement_profile
    );

-- The legacy fields mixed requested placement with scheduler assignment. Copy
-- non-null values conservatively into both roles because history cannot prove
-- which writer first populated them.
UPDATE task_submission
   SET required_worker_id = worker_id,
       assigned_worker_id = worker_id,
       required_measurement_profile = measurement_profile,
       assigned_measurement_profile = measurement_profile;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM task_submission WHERE host_key IS NOT NULL) THEN
        RAISE EXCEPTION 'cannot drop populated task_submission.host_key';
    END IF;
END
$$;

ALTER TABLE task_submission
    DROP COLUMN worker_id,
    DROP COLUMN measurement_profile,
    DROP COLUMN host_key;

CREATE TABLE task_submission_idempotency (
    producer_namespace TEXT NOT NULL,
    producer_key TEXT NOT NULL,
    task_submission_id UUID NOT NULL
        REFERENCES task_submission(id) ON DELETE CASCADE,
    contract_version INTEGER,
    request_digest TEXT,
    initial_job_ids UUID[] NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (producer_namespace, producer_key),
    UNIQUE (task_submission_id),
    CHECK (length(producer_namespace) BETWEEN 1 AND 64),
    CHECK (length(producer_key) BETWEEN 1 AND 512),
    CHECK (cardinality(initial_job_ids) > 0),
    CHECK (
        (contract_version IS NULL AND request_digest IS NULL)
        OR
        (contract_version > 0 AND request_digest ~ '^[0-9a-f]{64}$')
    )
);

CREATE TABLE task_submission_provenance (
    task_submission_id UUID PRIMARY KEY
        REFERENCES task_submission(id) ON DELETE CASCADE,
    actor_kind TEXT NOT NULL,
    actor_identity TEXT,
    queued_event_detail JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (actor_kind IN ('system', 'github_user', 'slack_user')),
    CHECK ((actor_kind = 'system') = (actor_identity IS NULL))
);

CREATE TABLE task_submission_github (
    task_submission_id UUID PRIMARY KEY
        REFERENCES task_submission(id) ON DELETE CASCADE,
    github_webhook_id BIGINT UNIQUE
        REFERENCES github_webhook(id) ON DELETE RESTRICT,
    triggering_user_id BIGINT REFERENCES github_user(id) ON DELETE RESTRICT,
    github_pull_request_id BIGINT
        REFERENCES github_pull_request(id) ON DELETE RESTRICT,
    triggering_comment_id BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (
        (github_pull_request_id IS NULL AND triggering_comment_id IS NULL)
        OR github_pull_request_id IS NOT NULL
    )
);

CREATE INDEX task_submission_github_user_idx
    ON task_submission_github (triggering_user_id, created_at DESC)
    WHERE triggering_user_id IS NOT NULL;
CREATE INDEX task_submission_github_pr_idx
    ON task_submission_github (github_pull_request_id)
    WHERE github_pull_request_id IS NOT NULL;

CREATE TABLE task_submission_slack (
    task_submission_id UUID PRIMARY KEY
        REFERENCES task_submission(id) ON DELETE CASCADE,
    team_id TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    request_message_ts TEXT NOT NULL,
    reporting_identity TEXT NOT NULL UNIQUE,
    report_message_ts TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (length(team_id) BETWEEN 1 AND 64),
    CHECK (length(channel_id) BETWEEN 1 AND 64),
    CHECK (length(request_message_ts) BETWEEN 1 AND 64),
    CHECK (length(reporting_identity) BETWEEN 1 AND 128),
    CHECK (report_message_ts IS NULL OR length(report_message_ts) BETWEEN 1 AND 64)
);

-- Preserve aggregate-level provenance and replay keys for historical GitHub
-- submissions. Their canonical request digest remains unknown by design.
INSERT INTO task_submission_github
    (task_submission_id, github_webhook_id, triggering_user_id,
     github_pull_request_id, triggering_comment_id)
SELECT DISTINCT ON (job.task_submission_id)
       job.task_submission_id,
       webhook.github_webhook_id,
       owner.github_user_id,
       pull_request.github_pull_request_id,
       pull_request.triggering_comment_id
  FROM job
  JOIN github_webhook_job webhook ON webhook.job_id = job.id
  LEFT JOIN github_user_job owner ON owner.job_id = job.id
  LEFT JOIN github_pull_request_job pull_request ON pull_request.job_id = job.id
 ORDER BY job.task_submission_id, job.created_at, job.id;

INSERT INTO task_submission_idempotency
    (producer_namespace, producer_key, task_submission_id,
     contract_version, request_digest, initial_job_ids)
SELECT 'github_webhook', github.github_webhook_id::text,
       github.task_submission_id, NULL, NULL, ARRAY[
           (SELECT job.id
              FROM job
             WHERE job.task_submission_id = github.task_submission_id
             ORDER BY job.created_at, job.id
             LIMIT 1)
       ]::uuid[]
  FROM task_submission_github github;

INSERT INTO task_submission_provenance
    (task_submission_id, actor_kind, actor_identity, queued_event_detail)
SELECT github.task_submission_id,
       CASE WHEN github.triggering_user_id IS NULL
            THEN 'system' ELSE 'github_user' END,
       github.triggering_user_id::text,
       COALESCE((
           SELECT event.detail
             FROM job
             JOIN job_event event ON event.job_id = job.id
            WHERE job.task_submission_id = github.task_submission_id
              AND event.event_kind = 'queued'
            ORDER BY event.occurred_at, event.id
            LIMIT 1
       ), '{}'::jsonb)
  FROM task_submission_github github;

-- One aggregate has exactly one originating producer request. Do not conceal
-- malformed history by arbitrarily preferring GitHub or Slack during the
-- once-only backfill.
DO $$
DECLARE
    offending_submission_ids TEXT;
BEGIN
    SELECT string_agg(candidate.task_submission_id::text, ', '
                      ORDER BY candidate.task_submission_id::text)
      INTO offending_submission_ids
      FROM (
          SELECT DISTINCT github.task_submission_id
            FROM task_submission_github github
            JOIN job ON job.task_submission_id = github.task_submission_id
            JOIN job_event event
              ON event.job_id = job.id
             AND event.event_kind = 'queued'
           WHERE job.source = 'slack'
             AND event.detail->>'channel' IS NOT NULL
             AND event.detail->>'message_ts' IS NOT NULL
             AND event.detail->>'reporting_identity' IS NOT NULL
      ) candidate;

    IF offending_submission_ids IS NOT NULL THEN
        RAISE EXCEPTION
            'v27.2 migration found task submissions with both GitHub and Slack producer identities: %',
            offending_submission_ids;
    END IF;
END
$$;

-- Recover Slack identity from queued events. Rows without a reporting
-- identity predate snapshot reconciliation and keep job-event history only.
-- Historical redeliveries can have produced more than one aggregate for the
-- same reporting identity. Elect the earliest aggregate, matching the kernel's
-- first-writer-wins contract; every duplicate still receives generic audit
-- provenance below.
INSERT INTO task_submission_slack
    (task_submission_id, team_id, channel_id, request_message_ts,
     reporting_identity, report_message_ts)
WITH submission_candidates AS (
    SELECT DISTINCT ON (job.task_submission_id)
           job.task_submission_id,
           submission.created_at AS submission_created_at,
           COALESCE(event.detail->>'team_id', 'legacy') AS team_id,
           event.detail->>'channel' AS channel_id,
           event.detail->>'message_ts' AS request_message_ts,
           event.detail->>'reporting_identity' AS reporting_identity,
           (
               SELECT sent.detail->>'plan_message_ts'
                 FROM job_event sent
                WHERE sent.job_id = job.id
                  AND sent.event_kind = 'plan_message_sent'
                ORDER BY sent.occurred_at DESC, sent.id DESC
                LIMIT 1
           ) AS report_message_ts
      FROM job
      JOIN task_submission submission ON submission.id = job.task_submission_id
      JOIN job_event event
        ON event.job_id = job.id
       AND event.event_kind = 'queued'
     WHERE job.source = 'slack'
       AND event.detail->>'channel' IS NOT NULL
       AND event.detail->>'message_ts' IS NOT NULL
       AND event.detail->>'reporting_identity' IS NOT NULL
     ORDER BY job.task_submission_id, job.created_at, job.id,
              event.occurred_at, event.id
),
canonical_requests AS (
    SELECT DISTINCT ON (reporting_identity)
           task_submission_id, team_id, channel_id, request_message_ts,
           reporting_identity, report_message_ts
      FROM submission_candidates
     ORDER BY reporting_identity, submission_created_at, task_submission_id
)
SELECT task_submission_id, team_id, channel_id, request_message_ts,
       reporting_identity, report_message_ts
  FROM canonical_requests;

INSERT INTO task_submission_idempotency
    (producer_namespace, producer_key, task_submission_id,
     contract_version, request_digest, initial_job_ids)
SELECT 'slack_request', slack.reporting_identity, slack.task_submission_id,
       NULL, NULL, ARRAY[
           (SELECT job.id
              FROM job
             WHERE job.task_submission_id = slack.task_submission_id
             ORDER BY job.created_at, job.id
             LIMIT 1)
       ]::uuid[]
  FROM task_submission_slack slack
ON CONFLICT (producer_namespace, producer_key) DO NOTHING;

INSERT INTO task_submission_provenance
    (task_submission_id, actor_kind, actor_identity, queued_event_detail)
SELECT DISTINCT ON (job.task_submission_id)
       job.task_submission_id, 'slack_user', 'legacy',
       COALESCE(event.detail, '{}'::jsonb)
  FROM job
  JOIN job_event event
    ON event.job_id = job.id
   AND event.event_kind = 'queued'
 WHERE job.source = 'slack'
   AND event.detail->>'channel' IS NOT NULL
   AND event.detail->>'message_ts' IS NOT NULL
   AND event.detail->>'reporting_identity' IS NOT NULL
 ORDER BY job.task_submission_id, event.occurred_at, event.id
ON CONFLICT (task_submission_id) DO NOTHING;

ALTER TABLE job
    ADD COLUMN required_capability worker_capability;

UPDATE job
   SET required_capability = CASE task_kind
       WHEN 'benchmark' THEN 'benchmark'::worker_capability
       WHEN 'build_only' THEN 'build_only'::worker_capability
       WHEN 'block_validation' THEN 'block_validation'::worker_capability
   END;

ALTER TABLE job
    ALTER COLUMN required_capability SET NOT NULL;

CREATE INDEX job_queued_capability_idx
    ON job (required_capability, created_at, id)
    WHERE status = 'queued';
