ALTER TABLE benchmark_spec
    ADD COLUMN baseline_calibration_id BIGINT;

-- v19: repeat groups run one shared baseline calibration before the measured
-- VM runs. Existing singleton/build-only groups stay build->run / build-only.
-- Keep this SQL predicate in lockstep with
-- sbgh_core::models::uses_shared_calibration().
UPDATE benchmark_workflow_step step
   SET step_index = step_index + 1
  FROM benchmark_spec spec
 WHERE step.benchmark_spec_id = spec.id
   AND step.step_kind = 'run'
   AND spec.task_kind = 'benchmark'
   AND spec.build_target = 'stacks_bench'
   AND spec.requested_run_count > 1;

INSERT INTO benchmark_workflow_step
    (benchmark_group_id, step_index, step_kind, benchmark_spec_id)
SELECT spec.benchmark_group_id, 1, 'calibrate', spec.id
  FROM benchmark_spec spec
 WHERE spec.task_kind = 'benchmark'
   AND spec.build_target = 'stacks_bench'
   AND spec.requested_run_count > 1
   AND NOT EXISTS (
       SELECT 1
         FROM benchmark_workflow_step step
        WHERE step.benchmark_spec_id = spec.id
          AND step.step_kind = 'calibrate'
   );
