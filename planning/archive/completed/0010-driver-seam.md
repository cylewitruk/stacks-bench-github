# 0010: Task-agnostic `Driver` seam

- **id:** `0010-driver-seam`
- **status:** `shipped`
- **date:** 2026-06
- **source:** `docs/roadmap-v8.md` (Phase 1; cloud Phases 0/3–6 →
  `0006-aws-cloud-backend`)

Extracted the local execution-substrate abstraction — a task-agnostic `Driver` —
finishing the half-realized seam (task ⟂ backend).

## What shipped

- New `crate::driver`: `trait Driver { run_task, cleanup_by_job_id }`, neutral
  `DriverOutcome { status: DriverStatus, summary }`, `DriverStatus`, `Placement`,
  minimal `TaskSpec { args }`.
- The `SinkAdapter` (`PhaseListener`→`EventSink`) moved inside `libvirt/driver.rs`;
  `impl Driver for LibvirtDriver` wraps the unchanged inherent `run_benchmark` and
  delegates `cleanup_by_job_id` (fully-qualified inherent call).
- `BenchRecipe` holds `Arc<dyn Driver>`; `JobDeps` + `recover_orphans` dispatch over
  it; the driver is built once in `Runner::new`. Behavior-preserving — bench-on-
  libvirt stays the only live path.

## Validation

- All 650 tests green, lint clean; Codex-signed-off (2026-06-08).

## Deviations from the roadmap

- **Scope reduced to the trait seam only** (explicit call). The cloud-init split, the
  fan-out/probe/workspace `TaskSpec` fields, and externalized
  phase-model/artifact-manifest were **deferred to block validation**
  (`0005`/`0019`) — avoiding speculative abstraction with one task + one backend.
  `cleanup_by_job_id` returns `bool` (existing contract), not `Result<()>`.

## Durable decisions (ADR candidate)

- **Task ⟂ backend:** `Recipe` (task axis) vs `Driver` (backend axis);
  `{task} × {backend}` is a matrix, not one driver per task. *(Not yet extracted as
  a standalone ADR.)*
