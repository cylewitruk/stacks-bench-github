# 0008: Execution architecture (coordinator / worker / reporter)

- **id:** `0008-execution-architecture`
- **status:** `shipped`
- **source:** `docs/roadmap-v5.md`
- **follow-ups:** `0014-preclaim-placeholder-checks`,
  `0015-resource-aware-admission`, `0016-db-enforced-same-sha-dedup`,
  `0017-generic-phase-events`

The daemon's concurrent execution architecture: a coordinator over per-job worker
+ reporter tasks, plus concurrency, cancellation, orphan recovery, and CPU pinning.

## What shipped

- Split the serial `execute()` into three roles over a bounded mpsc `WorkerEvent`
  channel: long-lived **coordinator**, per-job **worker** (pure driver execution,
  emits events), per-job **reporter** (sole GitHub + DB/lifecycle owner). Generic
  over a `Recipe` trait.
- Driver concurrency-safety audit; the shared git mirror fixed with a process-global
  `MIRROR_LOCK` (all other per-run state already job_id-namespaced).
- Coordinator slot pool (`[runner].max_concurrent_jobs`, default 1) + `JoinSet`,
  panic-isolated.
- Two-mode signal shutdown: SIGINT×1 drain, SIGTERM/SIGINT×2 abort (cancel-safe
  poll-loop teardown, no VM/loop leak).
- Handle-less `cleanup_by_job_id` + startup `recover_orphans` (idempotent crash
  recovery; loop-device detach via `losetup -j`).
- `cancelled` terminal status (gray check, not red ✗) for abort + crash-orphan;
  cancelled runs excluded from baselines by construction (no `job_metric`).
- Queued "#N ahead" position reporting (debounced) + same-SHA `/benchmark` dedup
  (`find_active_job`).
- Per-slot CPU pinning (`[runner].cpu_sets`/`host_cpus` → cpuset + emulatorpin);
  setup documentation + `scripts/irq-affinity.sh`.

## Validation

- Live-smoked on the host (stop mid-run → gray Cancelled + re-run; `kill -9` +
  restart → orphan VM cleaned, row cancelled, stuck check concluded). All four
  phases Codex-signed-off, 530+ tests green.

## Durable decisions (ADR candidates)

- The reporter is the sole owner of every GitHub call + lifecycle/DB write; the
  worker is pure execution (never GitHub/DB); the coordinator touches only
  queue-state transitions.
- Channel discipline: Phase/Finished reliable (`send().await`); Heartbeat
  best-effort (`try_send`).
- Abort is poll-loop-only, never mid-provision; orphan/abort → **cancel, not re-run,
  not fail**.
- One reporter per job (isolates GitHub latency, scopes debounce). The `Recipe` seam
  is the load-bearing boundary for v6 platformization and the shipped `0010` Driver
  seam.

## Deferred → backlog

- `0014-preclaim-placeholder-checks` (Phase 5.3) · `0015-resource-aware-admission`
  (5.2/5.4) · `0016-db-enforced-same-sha-dedup` (5.1) · `0017-generic-phase-events`.
