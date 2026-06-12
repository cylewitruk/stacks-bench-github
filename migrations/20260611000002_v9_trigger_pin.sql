-- v9 (item 0025-baseline-binary-cache): pin a release/baseline trigger policy so
-- its built `stacks-bench` binary is kept in the host binary cache past the size
-- budget (non-pinned entries evict least-recently-used). `pinned_until`
-- optionally expires the pin, after which the entry drops back to the LRU tail.
--
-- Additive + defaulted, so existing rows are unaffected and a v8/earlier binary
-- (which never selects these columns) reads the table fine on rollback.
ALTER TABLE trigger_policy
    ADD COLUMN pinned BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN pinned_until TIMESTAMPTZ;
