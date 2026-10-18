-- Existing sessions use the operator-approved deployed revision-1 contract.
-- The advertisement stays separate from the selected, immutable session contract.
ALTER TABLE worker_session ADD COLUMN protocol_offer JSONB;
ALTER TABLE worker_session ADD COLUMN protocol_contract JSONB NOT NULL DEFAULT
    '{"revision":1,"features":["build_only_v1","benchmark_v1","block_validation_v1"]}'::jsonb;
