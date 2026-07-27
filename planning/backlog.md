# Backlog

**Unscheduled items only** (`backlog` / `candidate` / `parked`). Once an item is
selected it **moves** to an iteration; once terminal it moves to `archive/` —
see the single-home rule in the [README](README.md#item-ids). The complete
registry of every item (all statuses, incl. shipped/in-flight) is
[index.md](index.md); keep entries here compact and push worked-through detail
to `design/`.

*`0001-artifact-store` shipped (iteration v4, 2026-06) →
[archive/completed/0001-artifact-store.md](archive/completed/0001-artifact-store.md).*

*`0002-slack-adhoc-profiling` shipped (iteration v5, 2026-06) →
[archive/completed/0002-slack-adhoc-profiling.md](archive/completed/0002-slack-adhoc-profiling.md).
Its live-timeline follow-on `0021-slack-live-timeline` shipped (iteration v6) →
[archive/completed/0021-slack-live-timeline.md](archive/completed/0021-slack-live-timeline.md).*

## Backlog (unscheduled)

*`0004-worker-fleet`, load-bearing event item
`0017-generic-phase-events`, and `0019-block-validation-recipe` shipped in v25
(2026-07) → [fleet](archive/completed/0004-worker-fleet.md),
[events](archive/completed/0017-generic-phase-events.md), and
[block validation](archive/completed/0019-block-validation-recipe.md).*

*`0005-task-kind-platform` **shipped** as iteration **v10** (the multi-axis job
model: source / intent / task_kind / build_target / derived report; build-only
proven) — as-built record in
[archive/completed/0005-task-kind-platform.md](archive/completed/0005-task-kind-platform.md).*

### 0014 — Pre-claim placeholder / skipped checks + queue visibility

- **id:** `0014-preclaim-placeholder-checks`
- **status:** `backlog`
- **priority:** `medium`
- **source:** `archive/completed/0007` (v4 Phase 3) + `0008` (v5 Phase 5.3)

**Problem:** A still-DB-queued job has no reporter, so there's no
pre-`/benchmark` check or pre-claim position visibility; a denied source leaves
no `skipped` breadcrumb.

**Scope:** Policy-gated placeholder/`skipped` checks on PR sync + a pre-claim
queued-position updater keyed by `(installation, repo, PR, head_sha)`
(migration-bearing).

**Acceptance:** A PR shows a placeholder/queued check before its job is claimed.

### 0032 — Supersede stale PR-head benchmarks

- **id:** `0032-supersede-stale-pr-head-runs`
- **status:** `backlog`
- **priority:** `medium`
- **depends_on:** `0008-execution-architecture`
- **relates_to:** `0014-preclaim-placeholder-checks`,
  `0037-benchmark-group-run-model`
- **source:** operational UX follow-up (2026-06)

**Problem:** When a PR receives a new push while an older-head benchmark is still
queued or running, the daemon may finish work for a commit nobody cares about
anymore, wasting host time and leaving the useful new HEAD waiting.

**Scope:** On `pull_request.synchronize`, detect active PR benchmark jobs for the
same PR whose `git_commit_hash` is not the new `head_sha`, cancel/supersede them
with neutral reporting ("superseded by newer PR head …"), then enqueue the new
HEAD normally. Queued stale jobs can be cancelled directly; running stale jobs
use the runner's cancellation path so VM teardown and reporting stay normal.
Baseline/push/tag jobs are unaffected.

After `0037`, supersession should target the active benchmark group for the PR
HEAD and cancel its child runs/workflow coherently, not leave orphaned group
artifacts or mixed terminal surfaces.

**Acceptance:** Pushing a new commit to a PR cancels any queued/running benchmark
for the previous PR HEAD and schedules exactly one run for the new HEAD, with the
old surface marked neutral/cancelled rather than failed.

### 0035 — Slack App Home status dashboard

- **id:** `0035-slack-app-home-status`
- **status:** `backlog`
- **priority:** `medium`
- **depends_on:** `0005-task-kind-platform`
- **relates_to:** `0003-results-portal`, `0014-preclaim-placeholder-checks`,
  `0031-reusable-build-jobs`, `0037-benchmark-group-run-model`
- **source:** Slack operator UX follow-up (2026-06)

**Problem:** Operators and Slack users have no single place inside Slack to see
what the daemon is doing. Queue state is scattered across thread messages and logs;
cache-warming jobs are silent by design; recently-finished runs require a CLI or
log tail. The full portal remains parked, but a lightweight Slack Home tab can
cover the common "what's happening right now?" need.

**Scope:** Add a Slack App Home MVP. Enable the Home tab in app/manifest
documentation, subscribe to `app_home_opened`, and add `views.publish` to the
Slack Web API client. Route `app_home_opened` and a single `refresh` button
action through Socket Mode, acking immediately and spawning the publish work like
`app_mention`. Render a private Home tab with daemon status, capacity
(`running/max_concurrent`), currently-running jobs, queued jobs in claim order,
recent terminal jobs, and pinned/warming context where available. The MVP is
**pull-driven** only: publish on open and on manual refresh; do not store recent
viewers or push background updates. Once `0037` lands, show group identity and
child run state instead of flattening multi-run work into unrelated jobs.

**Acceptance:** Opening BenchBot's Home tab publishes a Block Kit status view
for that user; pressing Refresh republishes the latest view; queued/running
ordering matches the runner's claim order; silent cache-warm build jobs are
visible as daemon work; failures to publish are logged but never affect job
execution.

**Deferred / non-goals:** No recent-viewer registry, background refresh loop,
settings, modals, job cancellation buttons, warm-trigger buttons, or full portal
replacement. Automatic Home-tab updates can come later once we know the view is
useful.

### 0036 — PR-comment LLM intent resolution

- **id:** `0036-pr-comment-llm-intent`
- **status:** `backlog`
- **priority:** `medium`
- **depends_on:** `0020-llm-intent-resolution`
- **relates_to:** `0032-supersede-stale-pr-head-runs`,
  `0037-benchmark-group-run-model`, `0038-isolated-benchmark-repetitions`,
  `0039-multi-variant-benchmark-comparisons`
- **source:** v13 Phase 4 follow-up

**Problem:** v13 proved the LLM intent resolver on Slack, but GitHub PR comments
still require the explicit `/benchmark` grammar. The shared `llm` and
`WorkloadSpec` seams were built so the PR surface can reuse the same
schema-validated resolver.

**Scope:** Add natural-language benchmark intent to PR comments after existing
policy/authz checks. Reuse the v13 resolver, validation, deterministic fast
path, rate limiting, and structured invalid diagnostics. Preserve explicit
`/benchmark` compatibility while NL rolls out. Invalid/ambiguous input should
reply on the PR without enqueueing.

**Acceptance:** A PR comment natural-language benchmark request resolves to the
same `WorkloadSpec` as the equivalent Slack input and enqueues the expected job;
invalid input receives a clear PR reply and does not enqueue; off-policy users
do not trigger provider calls.

**Deferred / non-goals:** No new provider, no model tools, no GitHub-side modal
equivalent. Ref existence remains daemon-owned, as in v13. Clean repeats and
multi-variant comparison grammar/schema belong with `0038`/`0039`, not the
initial PR-surface reuse.



### 0052 — Managed stacks-node chainstate producer

- **id:** `0052-managed-stacks-node-chainstate-producer`
- **status:** `backlog`
- **priority:** `medium`
- **depends_on:** `0015-resource-aware-admission`
- **relates_to:** `0026-central-block-index-cache`,
  `0039-multi-variant-benchmark-comparisons`
- **source:** chainstate freshness/provenance design discussion (2026-06)
- **design:**
  [design/0052-managed-stacks-node-chainstate-producer.md](design/0052-managed-stacks-node-chainstate-producer.md)

**Problem:** Chainstate archives and nightly LVs are produced outside sbgh's
admission and provenance model. That makes near-tip benchmark requests slow to
set up, leaves snapshot creation invisible to job planning, and gives weak
compatibility metadata for branch/release comparisons.

**Scope:** Put a real `stacks-node` under sbgh management. While the host is
idle, sbgh runs the node on a writable base LV. Before benchmark/index jobs or
planned cut points, sbgh gracefully stops the node, verifies quiescence, takes
tagged LVM-thin snapshots, records provenance, and restarts the node when idle.
Support an initial forward reconstruction from an older release through
release/epoch/daily cut points, then ongoing nightly and release/epoch
snapshots.

**Acceptance:** sbgh can produce a tagged read-only chainstate snapshot by
gracefully stopping a managed node, records producer version/tip/epoch metadata,
keeps benchmark jobs off the mutable base LV, and restarts the node when the
queue is idle. Admission treats node sync/snapshot work and benchmark/index
groups as mutually exclusive on the host.

**Deferred / non-goals:** No live snapshot from a writing node, no benchmark
directly against the mutable node LV, no automatic ref-expansion policy, and no
attempt to make near-tip snapshots reusable for v23 ledger facts before the
finality boundary says they are safe.

### 0046 — Reaction state from `reactions.list` (drop brute-force removal)

- **id:** `0046-slack-reaction-state-from-api`
- **status:** `backlog`
- **priority:** `low`
- **relates_to:** `0044-slack-reaction-lifecycle`
- **source:** v17 `swap_reaction` review (2026-06)

**Problem:** The reaction lifecycle clears prior reactions by *guessing*: the
timeline's `swap_reaction` speculatively calls `reactions.remove` for each
candidate emoji (⏳/🚀), most of which are no-ops, and the connector add/removes
👀 separately. This brute-force sweep scales poorly as the emoji set grows and
can't clear a reaction it doesn't know is there (e.g. a leaked 👀 when the
connector's best-effort ack-removal failed).

**Scope:** Add `reactions.list` to the Slack Web API client and use it to read
the bot's current reactions on the target message, removing exactly those that
aren't the target before adding the target — replacing the candidate-set
guesswork with the actual state. Drops the speculative no-op removes and
self-heals any leaked/unexpected reaction.

**Acceptance:** A reaction transition removes exactly the reactions actually
present (per `reactions.list`) and adds the target, with no speculative removes;
a leaked prior reaction is cleared on the next transition.

**Deferred / non-goals:** Trades the speculative removes for one `reactions.list`
read per transition — only worth it as the reaction set grows; no change to the
lifecycle/emoji set itself.


### 0049 — Direct libvirt RPC driver spike (`libvirt-pure`)

- **id:** `0049-libvirt-pure-driver-spike`
- **status:** `backlog`
- **priority:** `low`
- **depends_on:** `0010-driver-seam` (shipped)
- **relates_to:** `0004-worker-fleet`, `0019-block-validation-recipe`
- **source:** host log / integration cleanup follow-up (2026-06)

**Problem:** The current libvirt integration shells out to `virsh` and other
privileged host tools through `sudo`. It works, but it creates noisy system logs,
depends on CLI output/semantics, and makes libvirt errors harder to classify
than a typed API would. Future libvirt CLI drift would be an avoidable source of
host breakage.

**Scope:** Spike replacing the `virsh` subset of the libvirt driver with direct
libvirt RPC over the local Unix socket, using `libvirt-pure` if it proves mature
enough. Prove the lifecycle operations the daemon actually needs: connect to the
system libvirt socket, define a domain from the existing XML, start it, poll
state, destroy it, and undefine it, with typed error handling and no shelling out
to `virsh`. Keep the work behind the existing driver seam and preserve the
current shell implementation as a fallback until the host proof is boring.

The spike should also document the boundary: non-libvirt privileged filesystem
operations (`mkfs`, mount/umount, ownership fixes, LVM/thin snapshot work) are
not automatically solved by libvirt RPC and should stay separate unless a safe
replacement is identified. Evaluate host permissions explicitly (libvirt group,
Polkit, socket ownership) so removing `sudo virsh` does not turn into a broader
privilege change by accident.

**Acceptance:**

- On a host-compatible environment, a disposable domain can be
  define/start/poll/destroy/undefine'd through direct libvirt RPC using the
  existing generated XML.
- The spike records which current `virsh` calls can be replaced directly, which
  shell/sudo calls remain outside libvirt, and any `libvirt-pure` gaps or
  maturity risks. As of the initial backlog entry, `libvirt-pure` is attractive
  because it is pure Rust / Tokio and avoids C libvirt bindings, but it is young
  and low-adoption, so this must be proven before committing to migration.
- If viable, a follow-up migration plan keeps the shell driver as a fallback
  until direct RPC has passed host validation.

**Deferred / non-goals:** No immediate removal of the shell driver, no rewrite
of the full provisioning flow, no fleet scheduling changes, and no attempt to
replace non-libvirt host setup commands in the same slice.

### 0050 — Adopt `stacks-bench` schema-v1 JSON natively

- **id:** `0050-stacks-bench-schema-v1-native`
- **status:** `planned`
- **priority:** `medium`

Promoted to `v21-stacks-bench-schema-v1-native` →
[iterations/v21-stacks-bench-schema-v1-native.md](iterations/v21-stacks-bench-schema-v1-native.md).

*`0037-benchmark-group-run-model` shipped (iteration v14, 2026-06) →
[archive/completed/0037-benchmark-group-run-model.md](archive/completed/0037-benchmark-group-run-model.md).*

*`0038-isolated-benchmark-repetitions` shipped (iteration v15, 2026-06) →
[archive/completed/0038-isolated-benchmark-repetitions.md](archive/completed/0038-isolated-benchmark-repetitions.md).*

*`0039-multi-variant-benchmark-comparisons` in progress (iteration v22) →
[iterations/v22-multi-variant-benchmark-comparisons.md](iterations/v22-multi-variant-benchmark-comparisons.md).*

*`0041-shared-benchmark-calibration` shipped (iteration v19, 2026-06) →
[archive/completed/0041-shared-benchmark-calibration.md](archive/completed/0041-shared-benchmark-calibration.md).*

### 0015 — Resource-aware admission / budgets

- **id:** `0015-resource-aware-admission`
- **status:** `backlog`
- **priority:** `low`
- **relates_to:** `0038-isolated-benchmark-repetitions`,
  `0039-multi-variant-benchmark-comparisons`
- **source:** `archive/completed/0008` (v5 Phase 5.2/5.4)

**Problem:** Admission is a flat `max_concurrent_jobs` count, ignoring
heterogeneous per-job resource shapes.

**Scope:** Admit by Σ(per-job vCPU/memory) ≤ host capacity + in-flight position
reporting.

**Acceptance:** Concurrent admission respects host capacity, not just a count,
and can account for group fan-out (`variants × clean_repetitions`) once
`0038`/`0039` land. Admission treats a benchmark group as a host-pinned unit
whose total expected duration/resource footprint may monopolize that host until
the group completes.

**Deferred / non-goals:** Only pays off at `max_concurrent > 1`.

### 0013 — Drop legacy `jobs` table

- **id:** `0013-drop-legacy-jobs-table`
- **status:** `backlog`
- **priority:** `low`
- **source:** `archive/completed/0011` (v2 slice 12) + `0012` (v3 Phase 1)

**Problem:** The legacy `jobs` table is abandoned (no code path, no grants) but
still physically present, awaiting a soak window.

**Scope:** A one-line `DROP TABLE jobs` migration.

**Acceptance:** The legacy table is gone; nothing references it.

*`0020-llm-intent-resolution` shipped (iteration v13, 2026-06) →
[archive/completed/0020-llm-intent-resolution.md](archive/completed/0020-llm-intent-resolution.md).
The PR-comment follow-up is tracked as `0036`.*

*`0024-slack-card-stage-timings` shipped (iteration v12, 2026-06) →
[archive/completed/0024-slack-card-stage-timings.md](archive/completed/0024-slack-card-stage-timings.md);
it shipped with `0033-slack-streamed-plan-updates`.*

### 0034 — Historical stable toolchain resolution

- **id:** `0034-historical-stable-toolchain`
- **status:** `backlog`
- **priority:** `low`
- **depends_on:** `0025-baseline-binary-cache`
- **source:** Hetzner v11 warm-build observation (2026-06): old integration
  branches used legacy `rust-toolchain` containing `stable`

**Problem:** Legacy `rust-toolchain` files that say `stable` currently cache
under the literal `stable` declaration. That matches Rust's normal "use current
stable at build time" meaning, but it is not necessarily what we want for
benchmark archaeology: an old release branch may be better built with the
stable compiler that was current when the commit/tag was made.

**Scope:** Decide whether `stable` in a legacy `rust-toolchain` should remain a
literal cache key or resolve to a historical concrete Rust version. If adopted,
resolve by commit date for branch refs; for tags, prefer annotated tagger date
and fall back to commit date for lightweight tags. The resolved concrete version
must be used both in the cache fingerprint and in the build VM's selected
toolchain (for example via `RUSTUP_TOOLCHAIN`), with a local date→version cache
so builds do not depend on network availability.

**Acceptance:** The policy is explicit and tested. If historical resolution is
enabled, a legacy `stable` commit from an older date builds with the expected
concrete toolchain and caches under that concrete version; if disabled, it keeps
today's literal `stable` behavior.

**Deferred / non-goals:** Do not change current v9/v11 cache semantics as an
implicit bug fix. This is a policy choice and should be opt-in or clearly
documented if made default.

### 0026 — Central chainstate index ledger (pre-seed)

- **id:** `0026-central-block-index-cache`
- **status:** `in_progress`

Promoted to `v23-central-block-tx-index-cache` →
[iterations/v23-central-block-tx-index-cache.md](iterations/v23-central-block-tx-index-cache.md).

### 0027 — Fine-grained bench progress (JSONL)

- **id:** `0027-fine-grained-progress`
- **status:** `in_progress`
- **priority:** `medium`

Promoted to `v20-fine-grained-bench-progress` →
[iterations/v20-fine-grained-bench-progress.md](iterations/v20-fine-grained-bench-progress.md).

### 0028 — Results-summary restructure

- **id:** `0028-results-summary-restructure`
- **status:** `backlog`
- **priority:** `medium`
- **depends_on:** *(none)* — render-side, over the existing `RunResult` /
  `metric_table`
- **relates_to:** `0037-benchmark-group-run-model`,
  `0038-isolated-benchmark-repetitions`,
  `0041-shared-benchmark-calibration`
- **source:** high-value list (2026-06)

**Problem:** The single metrics table mixes run *context* with timing *numbers*,
sits the headline Setup/Execution/Commit figures next to Clarity cost, and
renders raw microseconds (`1,655,018 µs avg`) — noisy and hard to read.

**Scope:**

- An **overview/intro** section, separate from the timings: blocks measured
  (with range), transactions, warmup, baseline calibration time, active
  filtering flags, repeats. Most is on hand —
  `RunData.{blocks,warmup_blocks,measured_blocks}`, `RunSummary.transactions`,
  and the job's effective args / `WorkloadSpec`. After `0037`/`0038`, this must
  distinguish group-level clean repeats from any per-run/in-process count; after
  `0041`, it should separate calibration from measured workload time and note
  calibration provenance (`shared group baseline` vs. `per-run calibration`) so
  grouped variance summaries are self-documenting.
- **Re-section** the numbers: the core **Setup, Execution, Commit** table stands
  alone; **Clarity execution cost** (runtime, read/write) moves to its own
  section so it doesn't dilute the headline bench figures.
- **Humanize durations** — render the largest sensible unit (`1,655,018 µs` →
  `1.66s`) via a shared formatter reused by the PR comment and Slack snapshot.

**Acceptance:** A completed run renders an overview block, a core-timing table
in human units, and a separate Clarity-cost section, on both the PR comment and
the Slack snapshot.

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
- **relates_to:** `0037-benchmark-group-run-model`,
  `0038-isolated-benchmark-repetitions`,
  `0039-multi-variant-benchmark-comparisons`
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
  capability), so per-tx sits behind per-block. Once `0037` lands, breakdowns
  must preserve group/spec/run identity so slow blocks/txs can be compared
  across repeats or variants instead of flattened into one ambiguous list.

**Acceptance:** A completed run's card shows the top-N slowest blocks with their
per-phase timings; the full breakdown is reachable in the portal.

**Deferred / non-goals:** Slack has **no carousel block** — interactive "paging"
would be button/menu interactivity that `chat.update`s the message, a new
interaction-handler wiring beyond today's `app_mention` path; prefer top-N
inline plus the portal over building paging. Per-tx detail and the full sortable
table are deferred as above.

### 0030 — Results Q&A agent ("why was this slow?")

- **id:** `0030-results-qa-agent`
- **status:** `backlog`
- **priority:** `low`
- **depends_on:** `0020-llm-intent-resolution` (provider / structured-output
  infra); `0001-artifact-store` (the fetchable `stacks-bench.db`)
- **relates_to:** `0003-results-portal`, `0029-per-block-timing-detail`,
  `0037-benchmark-group-run-model`
- **source:** high-value list (2026-06)

**Problem:** Deep-diving a result ("why was this transaction slow?") today means
downloading `stacks-bench.db` and writing SQL by hand.

**Scope:** A **schema-aware agent** over a run/group's `stacks-bench.db` that
answers natural-language questions by querying it. **Read-only** against a
fetched copy (sandboxed), via a constrained query tool (parameterized /
whitelisted SQL, never arbitrary writes), with row/time/cost caps. Rides the
**same provider abstraction** as `0020` (env-only key, configurable model).
Surface: a thread reply to the Slack result message and/or the portal. After `0037`,
questions must be scoped to group/spec/run identity so comparisons and repeats
remain explainable.

**Acceptance:**

- A user asks a results question in-thread and gets a grounded answer derived
  from that run's db, under read-only and resource guards.
- The agent connects in **read-only SQLite mode** (`mode=ro` / `query_only`)
  against an **immutable, fetched copy** of `stacks-bench.db` — never the live
  archive, never a writable handle — enforced as an explicit guard, not left to
  prompt discipline.

**Deferred / non-goals:** No write access, ever; not a general SQL console.
Distinct from `0020` (which resolves bench *inputs*); this answers over
*outputs*. Prompt / tooling design is its own design.

*`0031-reusable-build-jobs` shipped (iteration v11, 2026-06) →
[archive/completed/0031-reusable-build-jobs.md](archive/completed/0031-reusable-build-jobs.md).*

*`0022-report-surface-trait` shipped (iteration v7, 2026-06) →
[archive/completed/0022-report-surface-trait.md](archive/completed/0022-report-surface-trait.md).*

*`0023-slack-card-redesign` shipped (iteration v8, 2026-06) →
[archive/completed/0023-slack-card-redesign.md](archive/completed/0023-slack-card-redesign.md).*

## Parked

### 0003 — Results portal (web UI + GitHub login)

- **id:** `0003-results-portal`
- **status:** `parked`
- **priority:** `medium`
- **depends_on:** `0001-artifact-store`
- **review:** `Codex signed off` (design)
- **design:** [design/0003-results-portal.md](design/0003-results-portal.md)

**Problem:** No way to browse runs, watch the queue, or deep-inspect a run's
profile.

**Scope:** A permissioned portal (GitHub OAuth → existing roles,
visibility-scoped) that's an **API client of the orchestrator** (never a second
DB client), reusing the upstream `stacks-bench` explorer version-matched +
proxied.

**Acceptance:** A logged-in user browses runs they may see and opens a profiler
trace.

**Deferred / non-goals:** Parked — we may be able to expose these interactions
via the Slack app instead.

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
- **relates_to:** `0037-benchmark-group-run-model`

**Problem:** Same-SHA PR `/benchmark` dedup is best-effort (check-then-insert
outside the atomic boundary).

**Scope:** A partial unique index on `(repo, commit) WHERE
trigger_kind='pr_comment' AND status IN active`, with the violation mapped to
`AlreadyEnqueued`.

**Deferred / non-goals:** Parked — premature for a single processor; revisit at
multi-processor. If revived, rewrite against the post-v10 source/intent axes and
the `0037` group model rather than the historical `trigger_kind` column.

### 0018 — Auto-rerun confidence gate

- **id:** `0018-auto-rerun-confidence-gate`
- **status:** `parked`
- **priority:** `low`
- **source:** `archive/completed/0009` (v7 Phase 4)
- **relates_to:** `0037-benchmark-group-run-model`,
  `0038-isolated-benchmark-repetitions`,
  `0039-multi-variant-benchmark-comparisons`

**Problem:** A single run can't separate a real regression from noise on
suspicious (~1–2σ) or shock (>20%) deltas.

**Scope:** Rerun on suspicious/shock bands (bypassing same-SHA dedup), aggregate
`(repo, commit)` metrics to a CI (`SEM = σ/√N`), cap at `max_reruns`, surface
"confirmed over N runs". If revived after `0037`/`0038`, implement this as a
policy that requests additional clean `BenchmarkRun`s inside a `BenchmarkGroup`,
not as an ad-hoc rerun loop outside the group model.

**Deferred / non-goals:** Parked — partly superseded by clean repetitions and
noise-aware comparisons, but still useful as an automatic policy layer once the
group/run model sets the noise floor.
