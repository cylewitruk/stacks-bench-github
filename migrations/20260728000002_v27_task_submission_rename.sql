-- v27.1: rename the universal request/workflow aggregate from its historical
-- benchmark-specific vocabulary to task-neutral submission vocabulary.
-- This migration changes identifiers only; PostgreSQL preserves object OIDs,
-- data, defaults, relationships, and index definitions.

ALTER TYPE benchmark_step_kind RENAME TO task_step_kind;

ALTER TABLE benchmark_group RENAME TO task_submission;
ALTER TABLE benchmark_spec RENAME TO task_spec;
ALTER TABLE benchmark_workflow_step RENAME TO task_workflow_step;

ALTER TABLE task_submission
    RENAME COLUMN recovery_of_group_id TO recovery_of_submission_id;

ALTER TABLE task_spec
    RENAME COLUMN benchmark_group_id TO task_submission_id;

ALTER TABLE task_workflow_step
    RENAME COLUMN benchmark_group_id TO task_submission_id;
ALTER TABLE task_workflow_step
    RENAME COLUMN benchmark_spec_id TO task_spec_id;

ALTER TABLE job
    RENAME COLUMN benchmark_group_id TO task_submission_id;
ALTER TABLE job
    RENAME COLUMN benchmark_spec_id TO task_spec_id;
ALTER TABLE job
    RENAME COLUMN benchmark_run_index TO task_run_index;

ALTER TABLE worker_attempt
    RENAME COLUMN benchmark_group_id TO task_submission_id;

-- PostgreSQL does not rename constraints or indexes when their owning
-- table/column is renamed. Keep catalog names aligned with the new model.
ALTER TABLE task_submission
    RENAME CONSTRAINT benchmark_group_pkey TO task_submission_pkey;
ALTER TABLE task_submission
    RENAME CONSTRAINT benchmark_group_id_not_null TO task_submission_id_not_null;
ALTER TABLE task_submission
    RENAME CONSTRAINT benchmark_group_github_installation_id_not_null
    TO task_submission_github_installation_id_not_null;
ALTER TABLE task_submission
    RENAME CONSTRAINT benchmark_group_github_repo_id_not_null
    TO task_submission_github_repo_id_not_null;
ALTER TABLE task_submission
    RENAME CONSTRAINT benchmark_group_github_repo_id_fkey
    TO task_submission_github_repo_id_fkey;
ALTER TABLE task_submission
    RENAME CONSTRAINT benchmark_group_source_not_null
    TO task_submission_source_not_null;
ALTER TABLE task_submission
    RENAME CONSTRAINT benchmark_group_intent_not_null
    TO task_submission_intent_not_null;
ALTER TABLE task_submission
    RENAME CONSTRAINT benchmark_group_artifact_prefix_not_null
    TO task_submission_artifact_prefix_not_null;
ALTER TABLE task_submission
    RENAME CONSTRAINT benchmark_group_artifact_prefix_key
    TO task_submission_artifact_prefix_key;
ALTER TABLE task_submission
    RENAME CONSTRAINT benchmark_group_created_at_not_null
    TO task_submission_created_at_not_null;
ALTER TABLE task_submission
    RENAME CONSTRAINT benchmark_group_updated_at_not_null
    TO task_submission_updated_at_not_null;
ALTER TABLE task_submission
    RENAME CONSTRAINT benchmark_group_worker_id_fkey
    TO task_submission_worker_id_fkey;
ALTER TABLE task_submission
    RENAME CONSTRAINT benchmark_group_recovery_of_group_id_fkey
    TO task_submission_recovery_of_submission_id_fkey;
ALTER TABLE task_submission
    RENAME CONSTRAINT benchmark_group_execution_generation_not_null
    TO task_submission_execution_generation_not_null;
ALTER TABLE task_submission
    RENAME CONSTRAINT benchmark_group_execution_generation_check
    TO task_submission_execution_generation_check;
ALTER TABLE task_submission
    RENAME CONSTRAINT benchmark_group_fencing_generation_not_null
    TO task_submission_fencing_generation_not_null;
ALTER TABLE task_submission
    RENAME CONSTRAINT benchmark_group_fencing_generation_check
    TO task_submission_fencing_generation_check;

ALTER TABLE task_spec
    RENAME CONSTRAINT benchmark_spec_pkey TO task_spec_pkey;
ALTER TABLE task_spec
    RENAME CONSTRAINT benchmark_spec_id_not_null TO task_spec_id_not_null;
ALTER TABLE task_spec
    RENAME CONSTRAINT benchmark_spec_benchmark_group_id_not_null
    TO task_spec_task_submission_id_not_null;
ALTER TABLE task_spec
    RENAME CONSTRAINT benchmark_spec_benchmark_group_id_fkey
    TO task_spec_task_submission_id_fkey;
ALTER TABLE task_spec
    RENAME CONSTRAINT benchmark_spec_spec_index_not_null
    TO task_spec_spec_index_not_null;
ALTER TABLE task_spec
    RENAME CONSTRAINT benchmark_spec_spec_index_check
    TO task_spec_spec_index_check;
ALTER TABLE task_spec
    RENAME CONSTRAINT benchmark_spec_github_repo_id_not_null
    TO task_spec_github_repo_id_not_null;
ALTER TABLE task_spec
    RENAME CONSTRAINT benchmark_spec_github_repo_id_fkey
    TO task_spec_github_repo_id_fkey;
ALTER TABLE task_spec
    RENAME CONSTRAINT benchmark_spec_task_kind_not_null
    TO task_spec_task_kind_not_null;
ALTER TABLE task_spec
    RENAME CONSTRAINT benchmark_spec_build_target_not_null
    TO task_spec_build_target_not_null;
ALTER TABLE task_spec
    RENAME CONSTRAINT benchmark_spec_git_ref_kind_not_null
    TO task_spec_git_ref_kind_not_null;
ALTER TABLE task_spec
    RENAME CONSTRAINT benchmark_spec_git_ref_display_not_null
    TO task_spec_git_ref_display_not_null;
ALTER TABLE task_spec
    RENAME CONSTRAINT benchmark_spec_created_at_not_null
    TO task_spec_created_at_not_null;
ALTER TABLE task_spec
    RENAME CONSTRAINT benchmark_spec_updated_at_not_null
    TO task_spec_updated_at_not_null;
ALTER TABLE task_spec
    RENAME CONSTRAINT benchmark_spec_benchmark_group_id_spec_index_key
    TO task_spec_task_submission_id_spec_index_key;
ALTER TABLE task_spec
    RENAME CONSTRAINT benchmark_spec_requested_run_count_not_null
    TO task_spec_requested_run_count_not_null;
ALTER TABLE task_spec
    RENAME CONSTRAINT benchmark_spec_requested_run_count_check
    TO task_spec_requested_run_count_check;

ALTER TABLE task_workflow_step
    RENAME CONSTRAINT benchmark_workflow_step_pkey
    TO task_workflow_step_pkey;
ALTER TABLE task_workflow_step
    RENAME CONSTRAINT benchmark_workflow_step_id_not_null
    TO task_workflow_step_id_not_null;
ALTER TABLE task_workflow_step
    RENAME CONSTRAINT benchmark_workflow_step_benchmark_group_id_not_null
    TO task_workflow_step_task_submission_id_not_null;
ALTER TABLE task_workflow_step
    RENAME CONSTRAINT benchmark_workflow_step_benchmark_group_id_fkey
    TO task_workflow_step_task_submission_id_fkey;
ALTER TABLE task_workflow_step
    RENAME CONSTRAINT benchmark_workflow_step_step_index_not_null
    TO task_workflow_step_step_index_not_null;
ALTER TABLE task_workflow_step
    RENAME CONSTRAINT benchmark_workflow_step_step_index_check
    TO task_workflow_step_step_index_check;
ALTER TABLE task_workflow_step
    RENAME CONSTRAINT benchmark_workflow_step_step_kind_not_null
    TO task_workflow_step_step_kind_not_null;
ALTER TABLE task_workflow_step
    RENAME CONSTRAINT benchmark_workflow_step_benchmark_spec_id_fkey
    TO task_workflow_step_task_spec_id_fkey;
ALTER TABLE task_workflow_step
    RENAME CONSTRAINT benchmark_workflow_step_created_at_not_null
    TO task_workflow_step_created_at_not_null;
ALTER TABLE task_workflow_step
    RENAME CONSTRAINT benchmark_workflow_step_benchmark_group_id_step_index_key
    TO task_workflow_step_task_submission_id_step_index_key;

ALTER TABLE job
    RENAME CONSTRAINT job_benchmark_group_id_not_null
    TO job_task_submission_id_not_null;
ALTER TABLE job
    RENAME CONSTRAINT job_benchmark_group_id_fkey
    TO job_task_submission_id_fkey;
ALTER TABLE job
    RENAME CONSTRAINT job_benchmark_spec_id_not_null
    TO job_task_spec_id_not_null;
ALTER TABLE job
    RENAME CONSTRAINT job_benchmark_spec_id_fkey
    TO job_task_spec_id_fkey;
ALTER TABLE job
    RENAME CONSTRAINT job_benchmark_run_index_not_null
    TO job_task_run_index_not_null;
ALTER TABLE job
    RENAME CONSTRAINT job_benchmark_run_index_check
    TO job_task_run_index_check;
ALTER TABLE job
    RENAME CONSTRAINT job_benchmark_spec_run_unique
    TO job_task_spec_run_unique;

ALTER TABLE worker_attempt
    RENAME CONSTRAINT worker_attempt_benchmark_group_id_not_null
    TO worker_attempt_task_submission_id_not_null;
ALTER TABLE worker_attempt
    RENAME CONSTRAINT worker_attempt_benchmark_group_id_fkey
    TO worker_attempt_task_submission_id_fkey;

ALTER INDEX benchmark_group_recovery_idx
    RENAME TO task_submission_recovery_idx;
ALTER INDEX benchmark_spec_group_idx
    RENAME TO task_spec_submission_idx;
ALTER INDEX benchmark_workflow_step_group_idx
    RENAME TO task_workflow_step_submission_idx;
ALTER INDEX job_benchmark_group_idx
    RENAME TO job_task_submission_idx;
