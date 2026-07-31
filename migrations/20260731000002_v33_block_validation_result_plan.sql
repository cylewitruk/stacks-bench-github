-- v33 / 0079: persist the attempt-scoped chainstate observation and local
-- execution plan. The fleet has not been deployed, so no legacy result rows
-- exist and the retired range columns need no compatibility projection.
ALTER TABLE block_validation_result
    DROP COLUMN observed_start,
    DROP COLUMN observed_end,
    ADD COLUMN pre_nakamoto_count BIGINT NOT NULL
        CHECK (pre_nakamoto_count > 0),
    ADD COLUMN nakamoto_count BIGINT NOT NULL
        CHECK (nakamoto_count > 0),
    ADD COLUMN resolved_start BIGINT NOT NULL
        CHECK (resolved_start >= 0),
    ADD COLUMN resolved_end BIGINT NOT NULL
        CHECK (resolved_end >= resolved_start),
    ADD COLUMN epoch_segments JSONB NOT NULL,
    ADD COLUMN shard_count INTEGER NOT NULL
        CHECK (shard_count > 0),
    ADD COLUMN max_concurrency INTEGER NOT NULL
        CHECK (max_concurrency > 0 AND max_concurrency <= shard_count),
    ADD CONSTRAINT block_validation_observed_total_check
        CHECK (
            pre_nakamoto_count
                <= 9223372036854775807::BIGINT - nakamoto_count
        ),
    ADD CONSTRAINT block_validation_resolved_coverage_check
        CHECK (resolved_end < pre_nakamoto_count + nakamoto_count),
    ADD CONSTRAINT block_validation_checked_count_check
        CHECK (checked_blocks = resolved_end - resolved_start + 1);
