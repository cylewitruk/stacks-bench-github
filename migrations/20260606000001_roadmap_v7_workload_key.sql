-- roadmap-v7 Slice 2: per-job workload key for baseline comparison.
--
-- `workload_key` is a stable hash of a job's *effective* stacks-bench workload
-- args (see crates/sbgh-core/src/bench_args.rs). It lets a PR run be compared
-- only against a baseline that measured the IDENTICAL workload. Nullable: jobs
-- enqueued before this column existed carry NULL until (optionally) backfilled,
-- and a NULL never matches the lookup (so such a row simply can't serve as a
-- baseline).
ALTER TABLE job ADD COLUMN workload_key TEXT;

-- Repo-agnostic exact fork-point hit: the merge-base SHA + workload. Repo is
-- intentionally NOT in the key — a measurement is a property of the commit, not
-- the repo that ran it, so a fork PR finds an upstream baseline at the same SHA.
CREATE INDEX job_baseline_exact_idx
    ON job (git_commit_hash, workload_key)
    WHERE job_kind = 'baseline' AND status = 'completed';

-- Ref-leading nearest-before timeline: newest baseline on the target branch
-- at/just-before the fork-point, for the same workload. The slice-8
-- `job_baseline_timeline_idx` is `github_repo_id`-leading and does NOT serve
-- this repo-agnostic query, so this ref-leading partial index is required.
CREATE INDEX job_baseline_ref_timeline_idx
    ON job (git_ref_display, workload_key, git_committed_at DESC)
    WHERE job_kind = 'baseline' AND status = 'completed';
