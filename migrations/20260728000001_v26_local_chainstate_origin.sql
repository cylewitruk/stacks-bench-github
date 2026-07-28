-- v26 removes orchestrator-coordinated dataset identities. Workers select the
-- newest local read-only origin and report what the guest actually observed.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM job
         WHERE task_kind = 'block_validation'
           AND status IN ('queued', 'claimed', 'running')
    ) THEN
        RAISE EXCEPTION
            'drain active block-validation jobs before applying the v26 protocol migration';
    END IF;
END
$$;

ALTER TABLE block_validation_result
    ADD COLUMN chainstate_origin TEXT,
    ADD COLUMN observed_start BIGINT,
    ADD COLUMN observed_end BIGINT,
    ADD COLUMN legacy_dataset_identity JSONB;

UPDATE block_validation_result result
   SET chainstate_origin = 'legacy-dataset:' || result.dataset_generation,
       legacy_dataset_identity = jsonb_build_object(
           'generation', dataset.generation,
           'worker_id', dataset.worker_id,
           'network', dataset.network,
           'format_version', dataset.format_version,
           'covered_start', dataset.covered_start,
           'covered_end', dataset.covered_end,
           'manifest_sha256', dataset.manifest_sha256,
           'verified_at', dataset.verified_at
       )
  FROM block_validation_dataset dataset
 WHERE dataset.generation = result.dataset_generation;

ALTER TABLE block_validation_result
    ALTER COLUMN chainstate_origin SET NOT NULL,
    ADD CONSTRAINT block_validation_origin_nonempty
        CHECK (btrim(chainstate_origin) <> ''),
    ADD CONSTRAINT block_validation_observed_range
        CHECK (
            (observed_start IS NULL AND observed_end IS NULL)
            OR (
                observed_start >= 0
                AND observed_end >= observed_start
            )
        ),
    ADD CONSTRAINT block_validation_legacy_identity_object
        CHECK (
            legacy_dataset_identity IS NULL
            OR jsonb_typeof(legacy_dataset_identity) = 'object'
        ),
    DROP COLUMN dataset_generation;

DROP TABLE block_validation_dataset;
