//! Shared maintenance counters for scheduler waits and protocol-floor changes.

/// Count execution/promotion debt separately from retained, unreferenced staging.
pub const COUNTERS_SQL: &str = r#"
    WITH staging AS (
        SELECT staging.status,
               attempt.status IN ('offered', 'running', 'cancel_requested')
               OR (attempt.status IN ('completed', 'failed', 'cancelled') AND EXISTS (
                   SELECT 1 FROM jsonb_array_elements(
                       COALESCE(attempt.terminal_artifacts, '[]'::jsonb)
                   ) artifact WHERE artifact->>'key' = staging.object_key
               )) AS required
        FROM worker_artifact_staging staging
        JOIN worker_attempt attempt USING (attempt_id)
        WHERE staging.status IN ('granted', 'uploaded', 'verified', 'rejected')
    )
    SELECT
        (SELECT count(*) FROM worker_attempt WHERE status IN ('offered', 'running', 'cancel_requested')) AS active_attempts,
        (SELECT count(*) FROM worker_cleanup_obligation WHERE status = 'pending') AS pending_cleanup,
        (SELECT count(*) FROM worker_attempt WHERE highest_contiguous_reliable_seq > projected_reliable_seq) AS pending_projection,
        (SELECT count(*) FROM staging WHERE required AND status != 'rejected') AS pending_artifacts,
        (SELECT count(*) FROM staging WHERE NOT required) AS garbage_artifacts
"#;
