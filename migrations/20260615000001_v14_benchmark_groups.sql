-- v14 (item 0037): introduce benchmark groups/specs while keeping the
-- existing `job` row as the claimable run. This is a modeling seam only:
-- current creation paths create singleton groups with one spec and one run.

CREATE TYPE benchmark_step_kind AS ENUM ('build', 'calibrate', 'run');

CREATE TABLE benchmark_group (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    github_installation_id BIGINT NOT NULL,
    github_repo_id BIGINT NOT NULL REFERENCES github_repo(id) ON DELETE RESTRICT,
    source job_source NOT NULL,
    intent job_intent NOT NULL,
    artifact_prefix TEXT NOT NULL UNIQUE,
    host_key TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE benchmark_spec (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    benchmark_group_id UUID NOT NULL REFERENCES benchmark_group(id) ON DELETE CASCADE,
    spec_index INTEGER NOT NULL CHECK (spec_index >= 0),
    github_repo_id BIGINT NOT NULL REFERENCES github_repo(id) ON DELETE RESTRICT,
    task_kind task_kind NOT NULL,
    build_target build_target NOT NULL,
    git_ref_kind git_ref_kind NOT NULL,
    git_ref_display TEXT NOT NULL,
    git_commit_hash TEXT,
    git_committed_at TIMESTAMPTZ,
    workload_key TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (benchmark_group_id, spec_index)
);

CREATE TABLE benchmark_workflow_step (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    benchmark_group_id UUID NOT NULL REFERENCES benchmark_group(id) ON DELETE CASCADE,
    step_index INTEGER NOT NULL CHECK (step_index >= 0),
    step_kind benchmark_step_kind NOT NULL,
    benchmark_spec_id UUID REFERENCES benchmark_spec(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (benchmark_group_id, step_index)
);

ALTER TABLE job
    ADD COLUMN benchmark_group_id UUID REFERENCES benchmark_group(id) ON DELETE RESTRICT,
    ADD COLUMN benchmark_spec_id UUID REFERENCES benchmark_spec(id) ON DELETE RESTRICT,
    ADD COLUMN benchmark_run_index INTEGER CHECK (benchmark_run_index >= 0),
    ADD CONSTRAINT job_benchmark_spec_run_unique UNIQUE (benchmark_spec_id, benchmark_run_index);

-- Back-fill every existing job as group -> spec -> run singleton. A build-only
-- group has only the build step; benchmark/block-validation groups also get the
-- measured run step. The `calibrate` step kind exists for 0041 but is unused.
WITH rows AS (
    SELECT
        j.id AS job_id,
        gen_random_uuid() AS group_id,
        gen_random_uuid() AS spec_id,
        j.github_installation_id,
        j.github_repo_id,
        j.source,
        j.intent,
        j.task_kind,
        j.build_target,
        j.git_ref_kind,
        j.git_ref_display,
        j.git_commit_hash,
        j.git_committed_at,
        j.workload_key,
        j.created_at,
        j.updated_at
    FROM job j
    WHERE j.benchmark_group_id IS NULL
),
insert_groups AS (
    INSERT INTO benchmark_group
        (id, github_installation_id, github_repo_id, source, intent,
         artifact_prefix, created_at, updated_at)
    SELECT group_id, github_installation_id, github_repo_id, source, intent,
           group_id::text, created_at, updated_at
    FROM rows
),
insert_specs AS (
    INSERT INTO benchmark_spec
        (id, benchmark_group_id, spec_index, github_repo_id, task_kind,
         build_target, git_ref_kind, git_ref_display, git_commit_hash,
         git_committed_at, workload_key, created_at, updated_at)
    SELECT spec_id, group_id, 0, github_repo_id, task_kind, build_target,
           git_ref_kind, git_ref_display, git_commit_hash, git_committed_at,
           workload_key, created_at, updated_at
    FROM rows
),
insert_build_steps AS (
    INSERT INTO benchmark_workflow_step
        (benchmark_group_id, step_index, step_kind, benchmark_spec_id, created_at)
    SELECT group_id, 0, 'build'::benchmark_step_kind, spec_id, created_at
    FROM rows
),
insert_run_steps AS (
    INSERT INTO benchmark_workflow_step
        (benchmark_group_id, step_index, step_kind, benchmark_spec_id, created_at)
    SELECT group_id, 1, 'run'::benchmark_step_kind, spec_id, created_at
    FROM rows
    WHERE task_kind <> 'build_only'
)
UPDATE job j
   SET benchmark_group_id = rows.group_id,
       benchmark_spec_id = rows.spec_id,
       benchmark_run_index = 0
  FROM rows
 WHERE j.id = rows.job_id;

ALTER TABLE job ALTER COLUMN benchmark_group_id SET NOT NULL;
ALTER TABLE job ALTER COLUMN benchmark_spec_id SET NOT NULL;
ALTER TABLE job ALTER COLUMN benchmark_run_index SET NOT NULL;

CREATE INDEX benchmark_spec_group_idx ON benchmark_spec (benchmark_group_id, spec_index);
CREATE INDEX benchmark_workflow_step_group_idx
    ON benchmark_workflow_step (benchmark_group_id, step_index);
CREATE INDEX job_benchmark_group_idx
    ON job (benchmark_group_id, benchmark_spec_id, benchmark_run_index);
