-- v28: submission-scoped GitHub reporting identity.
--
-- This is deliberately separate from task_submission_github: provenance
-- describes the originating request, while a report may also be requested for
-- CLI/scheduler demand. Legacy job_event rows remain immutable audit history.

DO $$
DECLARE
    active_jobs BIGINT;
    active_attempts BIGINT;
    pending_cleanup BIGINT;
BEGIN
    SELECT COUNT(*) INTO active_jobs
      FROM job WHERE status IN ('claimed', 'running');
    SELECT COUNT(*) INTO active_attempts
      FROM worker_attempt
     WHERE status IN ('offered', 'running', 'cancel_requested');
    SELECT COUNT(*) INTO pending_cleanup
      FROM worker_cleanup_obligation
     WHERE completed_at IS NULL;
    IF active_jobs > 0 OR active_attempts > 0 OR pending_cleanup > 0 THEN
        RAISE EXCEPTION
            'v28 requires drained projection ownership; claimed/running jobs: %, active attempts: %, pending cleanup obligations: %',
            active_jobs, active_attempts, pending_cleanup;
    END IF;
END $$;

CREATE TABLE task_submission_github_report (
    task_submission_id UUID PRIMARY KEY
        REFERENCES task_submission(id) ON DELETE CASCADE,
    comment_id BIGINT UNIQUE,
    check_run_id BIGINT UNIQUE,
    check_run_url TEXT,
    check_name TEXT,
    external_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (
        (check_run_id IS NULL AND check_name IS NULL AND external_id IS NULL)
        OR
        (check_run_id IS NOT NULL
         AND check_name IN ('stacks-bench', 'stacks-block-validation')
         AND length(external_id) BETWEEN 1 AND 128)
    )
);

DO $$
DECLARE
    conflict RECORD;
BEGIN
    SELECT job.task_submission_id,
           array_agg(DISTINCT event.github_comment_id ORDER BY event.github_comment_id) AS ids
      INTO conflict
      FROM job
      JOIN job_event event ON event.job_id = job.id
     WHERE event.github_comment_id IS NOT NULL
  GROUP BY job.task_submission_id
    HAVING COUNT(DISTINCT event.github_comment_id) > 1
     LIMIT 1;
    IF FOUND THEN
        RAISE EXCEPTION
            'v28 conflicting GitHub comment ownership: submission %, ids %',
            conflict.task_submission_id, conflict.ids;
    END IF;

    SELECT job.task_submission_id,
           array_agg(DISTINCT event.github_check_run_id ORDER BY event.github_check_run_id) AS ids
      INTO conflict
      FROM job
      JOIN job_event event ON event.job_id = job.id
     WHERE event.github_check_run_id IS NOT NULL
  GROUP BY job.task_submission_id
    HAVING COUNT(DISTINCT event.github_check_run_id) > 1
     LIMIT 1;
    IF FOUND THEN
        RAISE EXCEPTION
            'v28 conflicting GitHub check ownership: submission %, ids %',
            conflict.task_submission_id, conflict.ids;
    END IF;

    SELECT event.github_comment_id AS external_id,
           array_agg(DISTINCT job.task_submission_id ORDER BY job.task_submission_id) AS submissions
      INTO conflict
      FROM job
      JOIN job_event event ON event.job_id = job.id
     WHERE event.github_comment_id IS NOT NULL
  GROUP BY event.github_comment_id
    HAVING COUNT(DISTINCT job.task_submission_id) > 1
     LIMIT 1;
    IF FOUND THEN
        RAISE EXCEPTION
            'v28 GitHub comment % is owned by multiple submissions %',
            conflict.external_id, conflict.submissions;
    END IF;

    SELECT event.github_check_run_id AS external_id,
           array_agg(DISTINCT job.task_submission_id ORDER BY job.task_submission_id) AS submissions
      INTO conflict
      FROM job
      JOIN job_event event ON event.job_id = job.id
     WHERE event.github_check_run_id IS NOT NULL
  GROUP BY event.github_check_run_id
    HAVING COUNT(DISTINCT job.task_submission_id) > 1
     LIMIT 1;
    IF FOUND THEN
        RAISE EXCEPTION
            'v28 GitHub check % is owned by multiple submissions %',
            conflict.external_id, conflict.submissions;
    END IF;
END $$;

INSERT INTO task_submission_github_report
    (task_submission_id, comment_id)
SELECT job.task_submission_id, MIN(event.github_comment_id)
  FROM job
  JOIN job_event event ON event.job_id = job.id
 WHERE event.github_comment_id IS NOT NULL
GROUP BY job.task_submission_id;

INSERT INTO task_submission_github_report
    (task_submission_id, check_run_id, check_run_url, check_name, external_id)
SELECT DISTINCT ON (job.task_submission_id)
       job.task_submission_id,
       event.github_check_run_id,
       event.github_check_run_url,
       CASE job.task_kind
           WHEN 'benchmark' THEN 'stacks-bench'
           WHEN 'block_validation' THEN 'stacks-block-validation'
           ELSE NULL
       END,
       job.id::text
  FROM job
  JOIN job_event event ON event.job_id = job.id
 WHERE event.github_check_run_id IS NOT NULL
   AND event.event_kind = 'check_run_created'
   AND job.task_kind <> 'build_only'
 ORDER BY job.task_submission_id, event.occurred_at, event.id
ON CONFLICT (task_submission_id) DO UPDATE
    SET check_run_id = EXCLUDED.check_run_id,
        check_run_url = EXCLUDED.check_run_url,
        check_name = EXCLUDED.check_name,
        external_id = EXCLUDED.external_id,
        updated_at = NOW();
