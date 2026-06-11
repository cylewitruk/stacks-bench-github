# Backlog

**Unscheduled items only** (`backlog` / `candidate` / `parked`). Once an item is
selected it **moves** to an iteration; once terminal it moves to `archive/` — see
the single-home rule in the [README](README.md#item-ids). The complete registry
of every item (all statuses, incl. shipped/in-flight) is [index.md](index.md);
keep entries here compact and push worked-through detail to `design/`.

*`0001-artifact-store` shipped (iteration v4, 2026-06) →
[archive/completed/0001-artifact-store.md](archive/completed/0001-artifact-store.md).*

*`0002-slack-adhoc-profiling` shipped (iteration v5, 2026-06) →
[archive/completed/0002-slack-adhoc-profiling.md](archive/completed/0002-slack-adhoc-profiling.md).
Its live-timeline follow-on `0021-slack-live-timeline` shipped (iteration v6) →
[archive/completed/0021-slack-live-timeline.md](archive/completed/0021-slack-live-timeline.md).*

## Backlog (unscheduled)

### 0003 — Results portal (web UI + GitHub login)

- **id:** `0003-results-portal`
- **status:** `backlog`
- **priority:** `medium`
- **depends_on:** `0001-artifact-store`
- **review:** `Codex signed off` (design)
- **design:** [design/0003-results-portal.md](design/0003-results-portal.md)

**Problem:** No way to browse runs, watch the queue, or deep-inspect a run's profile.

**Scope:** A permissioned portal (GitHub OAuth → existing roles, visibility-scoped)
that's an **API client of the orchestrator** (never a second DB client), reusing the
upstream `stacks-bench` explorer version-matched + proxied.

**Acceptance:** A logged-in user browses runs they may see and opens a profiler trace.

### 0004 — Distributed worker fleet (`remote-daemon`)

- **id:** `0004-worker-fleet`
- **status:** `backlog`
- **priority:** `medium`
- **depends_on:** `0010-driver-seam` (shipped)
- **review:** `Codex signed off` (design)
- **design:** [design/0004-worker-fleet.md](design/0004-worker-fleet.md)

**Problem:** A single host caps concurrency and can't serve heterogeneous hardware
(pinned bench boxes vs. big-local-NVMe block-val boxes).

**Scope:** Split `sbgh-daemon` into orchestrator + `sbgh-worker` (shared `sbgh-exec`);
thin pull-based worker API; capability matching; per-`measurement_profile` baseline
trust.

**Acceptance:** A remote worker runs a bench end-to-end; orchestrator stays the sole
DB client.

### 0005 — Task-kind platform (registry + job typing)

- **id:** `0005-task-kind-platform`
- **status:** `backlog`
- **priority:** `medium`
- **unblocks:** `0019-block-validation-recipe`
- **design:** [design/0005-task-kind-platform.md](design/0005-task-kind-platform.md)
  *(from roadmap-v6 platform work)*

**Problem:** The engine is implicitly bench-only; a second task kind needs a task-kind
registry + job typing so adding a kind is ~a new crate, not an engine change.

**Scope:** Crate split along the `Recipe`/`Driver` boundary (crate **naming TBD**
— the `sbgh-*`→`sgh-*` rename is an open question, not committed), a task-kind
registry + `task_kind` job typing, per-kind result tables/render; the three-layer
model (platform / Stacks substrate / task).

**Acceptance:** A new task kind registers + types its jobs without engine edits.

### 0019 — Block-validation recipe (second task kind)

- **id:** `0019-block-validation-recipe`
- **status:** `backlog`
- **priority:** `medium`
- **depends_on:** `0005-task-kind-platform`
- **review:** `Codex signed off` (design sketch)
- **design:** [design/0019-block-validation-recipe.md](design/0019-block-validation-recipe.md)
  *(from roadmap-v6 Phase 3 + block-validation-taskspec)*

**Problem:** Block validation is the planned second task kind — the proof that the
platform costs ~one crate per kind.

**Scope:** A `BlockValidationRecipe` with a probe-driven, K-shard fan-out over CoW
chainstate workspaces; terminal semantics (invalid-blocks = red check, not infra
failure).

**Acceptance:** Block validation lands on top of `0005` with no engine edits.

### 0014 — Pre-claim placeholder / skipped checks + queue visibility

- **id:** `0014-preclaim-placeholder-checks`
- **status:** `backlog`
- **priority:** `medium`
- **source:** `archive/completed/0007` (v4 Phase 3) + `0008` (v5 Phase 5.3)

**Problem:** A still-DB-queued job has no reporter, so there's no pre-`/benchmark`
check or pre-claim position visibility; a denied source leaves no `skipped`
breadcrumb.

**Scope:** Policy-gated placeholder/`skipped` checks on PR sync + a pre-claim
queued-position updater keyed by `(installation, repo, PR, head_sha)`
(migration-bearing).

**Acceptance:** A PR shows a placeholder/queued check before its job is claimed.

### 0015 — Resource-aware admission / budgets

- **id:** `0015-resource-aware-admission`
- **status:** `backlog`
- **priority:** `low`
- **source:** `archive/completed/0008` (v5 Phase 5.2/5.4)

**Problem:** Admission is a flat `max_concurrent_jobs` count, ignoring heterogeneous
per-job resource shapes.

**Scope:** Admit by Σ(per-job vCPU/memory) ≤ host capacity + in-flight position
reporting.

**Acceptance:** Concurrent admission respects host capacity, not just a count.

**Deferred / non-goals:** Only pays off at `max_concurrent > 1`.

### 0017 — Generic phase-event enum

- **id:** `0017-generic-phase-events`
- **status:** `backlog`
- **priority:** `low`
- **unblocks:** `0019-block-validation-recipe`
- **source:** `archive/completed/0008` (v5 forward-looking constraint)

**Problem:** `job_event` carries bench-specific `PhaseBuild*`/`PhaseBench*`; a second
task kind needs task-agnostic phase events.

**Scope:** Collapse to `PhaseStarted{label}`/`PhaseFinished{label}` via an additive
`ALTER TYPE … ADD VALUE` migration.

**Acceptance:** Phase events carry a task-agnostic label; bench + block-val both fit.

### 0013 — Drop legacy `jobs` table

- **id:** `0013-drop-legacy-jobs-table`
- **status:** `backlog`
- **priority:** `low`
- **source:** `archive/completed/0011` (v2 slice 12) + `0012` (v3 Phase 1)

**Problem:** The legacy `jobs` table is abandoned (no code path, no grants) but still
physically present, awaiting a soak window.

**Scope:** A one-line `DROP TABLE jobs` migration.

**Acceptance:** The legacy table is gone; nothing references it.

### 0020 — LLM intent resolution for Slack benches

- **id:** `0020-llm-intent-resolution`
- **status:** `backlog`
- **priority:** `low`
- **depends_on:** `0002-slack-adhoc-profiling` (the mention surface + the
  `resolve_workload` seam it defines)
- **source:** v5 scoping (2026-06)

**Problem:** v5 resolves a Slack bench request with a deterministic flag parser;
the team would prefer **natural language** ("profile yesterday's slow block
~5×") over memorized flags.

**Scope:** An LLM-backed `resolve_workload` impl behind v5's seam — raw mention
text → structured `WorkloadSpec` (txid/block/repetitions/rev), then the **same**
deterministic validator (the LLM never emits raw `bench_args`, so it can't
inject flags). Authz stays *before* resolution; an uncertain resolver asks a
clarifying question in-thread.

**Acceptance:** A natural-language `@BenchBot` request resolves to the correct
`WorkloadSpec` (or asks a clarifying question), under the same authz + validation
guards as the flag parser.

**Deferred / non-goals:** No new task/execution work; rides v5's surface + bench
path. Model/provider choice + prompt design are part of its own design.

### 0024 — Slack card stage timings

- **id:** `0024-slack-card-stage-timings`
- **status:** `backlog`
- **priority:** `low`
- **depends_on:** `0023-slack-card-redesign` (the 4-stage card it annotates)
- **source:** v8 Phase 2 (2026-06) — timing deferred from the card redesign

**Problem:** The v8 card shows tense titles + descriptive italic details, but a
completed row carries no duration ("Built in 1m 45s") and the in-progress detail
doesn't tick a live elapsed ("Building… [1m 30s]") — the mock's timing.

**Scope:** Per-row **live elapsed** (heartbeat-driven, debounced like the PR
comment) + **completed durations** in each row's `output`, and a total on the
Finalize row. Needs **persisted per-stage timing** so durations survive a daemon
restart (resume re-renders the card), and a debounce so the ticking doesn't spam
`chat.update`.

**Acceptance:** A running card's active row shows a ticking elapsed; completed
rows show their stage duration; all survive a resume.

**Deferred / non-goals:** No card-layout change (rides the v8 render); kept
**separate from v8 Phase 3** so the pre-claim queue seam stays uncluttered.

*`0022-report-surface-trait` shipped (iteration v7, 2026-06) →
[archive/completed/0022-report-surface-trait.md](archive/completed/0022-report-surface-trait.md).*

## Parked

### 0006 — AWS / cloud execution backend

- **id:** `0006-aws-cloud-backend`
- **status:** `parked`
- **priority:** `low`
- **depends_on:** `0004-worker-fleet` (returns as its worker provisioner)
- **design:** historical only — roadmap-v8 cloud phases, see the
  [rollup crosswalk](archive/superseded/rollup-roadmap-register-2026-06.md)
  *(parked; no live design until justified)*

**Problem:** Owned hardware caps elastic capacity.

**Scope:** EC2/EBS-from-snapshot provisioning.

**Deferred / non-goals:** Parked — returns later as a **worker provisioner** for
`0004`, gated on cost/variance/hydration data; not pursued standalone.

### 0016 — DB-enforced same-SHA PR dedup

- **id:** `0016-db-enforced-same-sha-dedup`
- **status:** `parked`
- **priority:** `low`
- **source:** `archive/completed/0008` (v5 Phase 5.1 follow-up)

**Problem:** Same-SHA PR `/benchmark` dedup is best-effort (check-then-insert outside
the atomic boundary).

**Scope:** A partial unique index on `(repo, commit) WHERE trigger_kind='pr_comment'
AND status IN active`, with the violation mapped to `AlreadyEnqueued`.

**Deferred / non-goals:** Parked — premature for a single processor; revisit at
multi-processor.

### 0018 — Auto-rerun confidence gate

- **id:** `0018-auto-rerun-confidence-gate`
- **status:** `parked`
- **priority:** `low`
- **source:** `archive/completed/0009` (v7 Phase 4)

**Problem:** A single run can't separate a real regression from noise on suspicious
(~1–2σ) or shock (>20%) deltas.

**Scope:** Rerun on suspicious/shock bands (bypassing same-SHA dedup), aggregate
`(repo, commit)` metrics to a CI (`SEM = σ/√N`), cap at `max_reruns`, surface
"confirmed over N runs".

**Deferred / non-goals:** Parked — design-only, gated on the operational re-baseline
that sets the noise floor.
