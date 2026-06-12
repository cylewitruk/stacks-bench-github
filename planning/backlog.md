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

*`0005-task-kind-platform` **shipped** as iteration **v10** (the multi-axis job
model: source / intent / task_kind / build_target / derived report; build-only
proven) — as-built record in
[archive/completed/0005-task-kind-platform.md](archive/completed/0005-task-kind-platform.md).*

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

### 0020 — LLM intent resolution (Slack + PRs)

- **id:** `0020-llm-intent-resolution`
- **status:** `backlog`
- **priority:** `medium`
- **depends_on:** `0002-slack-adhoc-profiling` (the mention surface + the
  `resolve_workload` seam it defines)
- **source:** v5 scoping (2026-06); high-value list (2026-06) added the PR
  surface, a concrete provider, and the abuse cap

**Problem:** Bench requests are resolved with a deterministic flag parser; the
team would prefer **natural language** ("bench blocks n..m on 3.4.0.0.3",
"profile yesterday's slow block ~5×") over memorized flags, on **both** the
Slack mention surface and PR comments.

**Scope:** An LLM-backed `resolve_workload` impl behind v5's seam, shared by the
Slack and PR surfaces. **Grammar-first** — the deterministic parser stays
authoritative; the LLM runs only when the flag grammar doesn't match (or the
text is plainly NL). Raw text → structured `WorkloadSpec`
(txid/block/repetitions/rev) via the provider's **structured-output / JSON-schema
mode**, then the **same** deterministic validator (the LLM never emits raw
`bench_args`, so it can't inject flags). Authz stays *before* resolution; an
uncertain resolver asks a clarifying question in-thread / on the PR. Provider is
**configurable** (default: the current small structured-output-capable OpenAI
model, exact name chosen at implementation time) behind a provider-agnostic
trait; the API key is **env-only** (`SBGH_OPENAI_API_KEY`, a TOML key is a hard
error — mirrors `slack.*_token` / `artifacts.s3.*`). Abuse guards: an **input
length cap** + per-user rate limit + ref-existence validation.

**Acceptance:** A natural-language request on Slack or a PR resolves to the
correct `WorkloadSpec` (or asks a clarifying question), under the same authz +
validation guards as the flag parser, with the grammar path still taken when it
matches.

**Deferred / non-goals:** No new task/execution work; rides v5's surface + bench
path. Prompt design + the exact provider/structured-output binding are part of
its own design.

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

### 0026 — Central block/tx index cache (pre-seed)

- **id:** `0026-central-block-index-cache`
- **status:** `backlog`
- **priority:** `medium`
- **depends_on:** *(none)* — gated on a schema spike of `stacks-bench.db`
- **source:** high-value list (2026-06)

**Problem:** Benching a range far from chain tip pays a large indexing cost —
`stacks-bench` walks backward from tip (8.2M+ blocks, +1/~5s) to resolve the
canonical Stacks block per height. That walk is recomputed per job even though,
below the reorg horizon, the canonical mapping is stable.

**Scope:** Extract the **block/tx index portion** (resolved canonical
height→block mapping and tx index) from completed `stacks-bench.db` files and
**merge into a central store** — idempotent, conflict-aware (deeper/newer wins; a
disagreement *below* the finality depth is flagged, not merged). The store is
**keyed and validated by provenance** — network (mainnet/testnet),
chainstate/source identity, the index-schema version, and the
stacks-core/stacks-bench DB-schema version — so a pre-seed can never mix networks
or incompatible layouts. **Pre-seed** a fresh bench DB with the requested range
when present and final, so the bench only indexes the uncovered tail, then merges
its new portion back. A **finality-depth guard** is load-bearing — pre-seeding a
not-yet-final block corrupts the run.

**Acceptance:** A bench over a previously-indexed, final range starts measuring
without re-walking from tip; a partially-covered range indexes only the
uncovered tail and merges it back.

**Deferred / non-goals:** Storage model (canonical SQLite "index pack" copied
range-wise vs Postgres + re-inject), exact tables to extract, per-network
scoping, and the finality depth are design questions. **Highest-risk of the
batch** (reorg correctness + stacks-core schema coupling) — wants a schema spike
before an iteration.

### 0027 — Fine-grained bench progress (JSONL)

- **id:** `0027-fine-grained-progress`
- **status:** `backlog`
- **priority:** `medium`
- **depends_on:** a profiler-protocol change in the `stacks-bench` integration
  branches; feeds `0024-slack-card-stage-timings` + the `ReportSurface` heartbeat
- **relates_to:** `0017-generic-phase-events`
- **source:** high-value list (2026-06)

**Problem:** `--json` emits only the final result on stdout, so the daemon can't
surface sub-phase progress ("indexing 2345/5000", "warming up 111/2000",
"measuring 4876/10000") — the card's in-progress rows show only a static detail.

**Scope:** `stacks-bench` emits **structured progress** as JSONL on a **dedicated
channel**, with a small stable schema (`{phase, current, total}` plus phase-change
events), **versioned** with the profiler protocol since it lands across all 7
integration branches. **Invariant:** stdout stays reserved for the final `--json`
result and raw stderr is excluded (it carries log/rustc noise); the leading
options are a dedicated `--progress-json-fd N` or a sentinel-prefixed line, and if
the sentinel wins it must name the exact stream/file it rides and guarantee it
can't corrupt an existing parser. Daemon-side, the runner parses the stream,
**debounces** (the PR-comment / queue-position throttle discipline), and feeds
`ReportSurface` heartbeats → the v8 card's active-row detail.

**Acceptance:** A running bench drives a live sub-phase counter on its surface
(Slack card / PR comment) without spamming `chat.update`.

**Deferred / non-goals:** Progress schema fields, channel choice (fd vs
sentinel), and throttle interval are design questions. Dovetails with `0024`
(durations) — candidates to co-schedule.

### 0028 — Results-summary restructure

- **id:** `0028-results-summary-restructure`
- **status:** `backlog`
- **priority:** `medium`
- **depends_on:** *(none)* — render-side, over the existing `RunResult` / `metric_table`
- **source:** high-value list (2026-06)

**Problem:** The single metrics table mixes run *context* with timing *numbers*,
sits the headline Setup/Execution/Commit figures next to Clarity cost, and renders
raw microseconds (`1,655,018 µs avg`) — noisy and hard to read.

**Scope:**

- An **overview/intro** section, separate from the timings: blocks measured (with
  range), transactions, warmup, baseline calibration time, active filtering flags,
  repeats. Most is on hand — `RunData.{blocks,warmup_blocks,measured_blocks}`,
  `RunSummary.transactions`, and the job's effective args / `WorkloadSpec`.
- **Re-section** the numbers: the core **Setup, Execution, Commit** table stands
  alone; **Clarity execution cost** (runtime, read/write) moves to its own section
  so it doesn't dilute the headline bench figures.
- **Humanize durations** — render the largest sensible unit (`1,655,018 µs` →
  `1.66s`) via a shared formatter reused by the PR comment and the Slack card.

**Acceptance:** A completed run renders an overview block, a core-timing table in
human units, and a separate Clarity-cost section, on both the PR comment and the
Slack card.

**Deferred / non-goals / upstream:** Clarity **read/write counts** and **baseline
calibration time** are **not** in `run.json` today (only read/write *lengths*
exist) — surfacing them is a `stacks-bench` change across the integration
branches, tracked here but landing with the protocol bump (cf. `0027`). Per-block
detail is `0029`.

### 0029 — Per-block / per-tx timing detail

- **id:** `0029-per-block-timing-detail`
- **status:** `backlog`
- **priority:** `medium`
- **depends_on:** `0028-results-summary-restructure` (rides the restructured
  summary); the portal `0003` for the full table
- **source:** high-value list (2026-06)

**Problem:** The summary shows only run-level aggregates — no way to see *which*
block or transaction was slow.

**Scope:**

- **Per-block** timings render from `run.json`'s `targets[]` (each a
  `TargetSummary` with its own setup/exec/commit) — no db query for block-level.
- Slack can't show the full set (50-block / 12k-char message limits; a run can
  measure thousands of blocks), so the card shows a **top-N slowest** table; the
  **full, sortable** table is the **portal's** job (`0003`).
- **Per-tx** timings aren't in `run.json` (only an aggregate `transactions`
  count) — they need querying the archived `stacks-bench.db` (a new sqlite-read
  capability), so per-tx sits behind per-block.

**Acceptance:** A completed run's card shows the top-N slowest blocks with their
per-phase timings; the full breakdown is reachable in the portal.

**Deferred / non-goals:** Slack has **no carousel block** — interactive "paging"
would be button/menu interactivity that `chat.update`s the message, a new
interaction-handler wiring beyond today's `app_mention` path; prefer top-N inline
plus the portal over building paging. Per-tx detail and the full sortable table
are deferred as above.

### 0030 — Results Q&A agent ("why was this slow?")

- **id:** `0030-results-qa-agent`
- **status:** `backlog`
- **priority:** `low`
- **depends_on:** `0020-llm-intent-resolution` (provider / structured-output
  infra); `0001-artifact-store` (the fetchable `stacks-bench.db`)
- **relates_to:** `0003-results-portal`, `0029-per-block-timing-detail`
- **source:** high-value list (2026-06)

**Problem:** Deep-diving a result ("why was this transaction slow?") today means
downloading `stacks-bench.db` and writing SQL by hand.

**Scope:** A **schema-aware agent** over a run's `stacks-bench.db` that answers
natural-language questions by querying it. **Read-only** against a fetched copy
(sandboxed), via a constrained query tool (parameterized / whitelisted SQL, never
arbitrary writes), with row/time/cost caps. Rides the **same provider
abstraction** as `0020` (env-only key, configurable model). Surface: a thread
reply on a Slack results card and/or the portal.

**Acceptance:**

- A user asks a results question in-thread and gets a grounded answer derived
  from that run's db, under read-only and resource guards.
- The agent connects in **read-only SQLite mode** (`mode=ro` / `query_only`)
  against an **immutable, fetched copy** of `stacks-bench.db` — never the live
  archive, never a writable handle — enforced as an explicit guard, not left to
  prompt discipline.

**Deferred / non-goals:** No write access, ever; not a general SQL console.
Distinct from `0020` (which resolves bench *inputs*); this answers over *outputs*.
Prompt / tooling design is its own design.

### 0031 — Reusable build jobs (artifact production + target axis)

- **id:** `0031-reusable-build-jobs`
- **status:** `backlog`
- **priority:** `medium`
- **depends_on:** `0005-task-kind-platform` (the redesigned job-model axes —
  `task_kind` / `build_target` / source / intent — a daemon-initiated build-only
  job needs)
- **relates_to:** `0025-baseline-binary-cache` (consumes its cache; supersedes its
  *warming*), `0019-block-validation-recipe`, `0004-worker-fleet`
- **design:** [design/0031-reusable-build-jobs.md](design/0031-reusable-build-jobs.md)
- **source:** v9 (`0025`) Phase-2 warming pivot (2026-06)

**Problem:** Warming a pinned release binary is a daemon-initiated, **build-only**,
silent job — which the current measurement-shaped, webhook-coupled
`JobKind`/`TriggerKind` model can't express without a fake webhook / measurement /
check. It needs `0005`'s clean job-model axes first.

**Scope:** A **build-only** task that produces + caches a `build_target` binary
(no measurement, silent), and **pin warming** that enqueues it for pinned refs
missing from the cache → `0025`'s recompute then protects them. Build-only + the
`build_target` axis are the **first consumers** of `0005`'s redesigned job model,
so this **follows `0005`**. Builds on shipped v9 groundwork
(`pin_resolver::PinnedTarget`, `BinaryCache::has_entry_for`).

**Acceptance:** A pinned release ref with no cached binary is pre-built by a
daemon-initiated build-only job (`source=daemon`, `intent=cache_warm`,
`task_kind=build_only`, `report=none`) and then protected by `0025`'s pin recompute.

*`0022-report-surface-trait` shipped (iteration v7, 2026-06) →
[archive/completed/0022-report-surface-trait.md](archive/completed/0022-report-surface-trait.md).*

*`0023-slack-card-redesign` shipped (iteration v8, 2026-06) →
[archive/completed/0023-slack-card-redesign.md](archive/completed/0023-slack-card-redesign.md).*

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
