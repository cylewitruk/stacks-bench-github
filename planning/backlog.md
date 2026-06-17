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

### 0004 — Distributed worker fleet (`remote-daemon`)

- **id:** `0004-worker-fleet`
- **status:** `backlog`
- **priority:** `medium`
- **depends_on:** `0010-driver-seam` (shipped)
- **review:** `Codex signed off` (design)
- **design:** [design/0004-worker-fleet.md](design/0004-worker-fleet.md)

**Problem:** A single host caps concurrency and can't serve heterogeneous
hardware (pinned bench boxes vs. big-local-NVMe block-val boxes).

**Scope:** Split `sbgh-daemon` into orchestrator + `sbgh-worker` (shared
`sbgh-exec`); thin pull-based worker API; capability matching;
per-`measurement_profile` baseline trust. After `0037`, a benchmark group is an
indivisible host-pinned scheduling unit: all repeats/variants/calibration for
that group run on the same worker so carried DBs and host-stable calibration
remain valid.

**Acceptance:** A remote worker runs a bench end-to-end; orchestrator stays the
sole DB client.

*`0005-task-kind-platform` **shipped** as iteration **v10** (the multi-axis job
model: source / intent / task_kind / build_target / derived report; build-only
proven) — as-built record in
[archive/completed/0005-task-kind-platform.md](archive/completed/0005-task-kind-platform.md).*

### 0019 — Block-validation recipe (second task kind)

- **id:** `0019-block-validation-recipe`
- **status:** `backlog`
- **priority:** `medium`
- **depends_on:** `0005-task-kind-platform`
- **relates_to:** `0037-benchmark-group-run-model`
- **review:** `Codex signed off` (design sketch)
- **design:**
  [design/0019-block-validation-recipe.md](design/0019-block-validation-recipe.md)
  *(from roadmap-v6 Phase 3 + block-validation-taskspec)*

**Problem:** Block validation is the planned second task kind — the proof that
the platform costs ~one crate per kind.

**Scope:** A `BlockValidationRecipe` with a probe-driven, K-shard fan-out over
CoW chainstate workspaces; terminal semantics (invalid-blocks = red check, not
infra failure).

**Acceptance:** Block validation lands on top of `0005` with no engine edits,
and composes with the reusable build/workflow model from `0037` rather than
creating a separate build pipeline.

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
what the daemon is doing. Queue state is scattered across thread cards and logs;
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

### 0040 — Slack queue receipt before claimed stream

- **id:** `0040-slack-queue-receipt-before-stream`
- **status:** `backlog`
- **priority:** `medium`
- **depends_on:** `0033-slack-streamed-plan-updates`
- **relates_to:** `0014-preclaim-placeholder-checks`,
  `0035-slack-app-home-status`
- **source:** live Slack streaming observation (2026-06)

**Problem:** Starting a Slack `chat.startStream` plan card at enqueue time means
the message can sit idle while queued. Slack may expire the server-side streaming
state before the worker claims the job, so the first reporter append sees
`message_not_in_streaming_state` and permanently falls back to `chat.update`.
That is safe, but it loses the no-collapse stream behavior before the benchmark
has even started. The current pre-claim queue-position updater only appends when
position changes; a job stuck at the same position behind one long run can
therefore produce no stream activity for the whole wait.

**Scope:** Split the Slack surface into two phases. At enqueue, post a normal
threaded queue receipt (plain Block Kit or text) that acknowledges the request,
shows current queue position, and can be `chat.update`d as the position changes
without involving a `plan` block. When a worker claims the job, start the
streamed plan card for the actual run and persist that stream `ts` for reporter
updates. The receipt can either be finalized ("claimed; live card below") or
left as queue history, but it should not be the plan stream itself.

Treat this as a refactor of the existing queue-position path, not new scheduler
machinery: today's Slack branch of `update_queue_positions` appends
`task_update`s to the pre-claim stream; under this item it `chat.update`s the
queue receipt instead. The reportable gate keys off the receipt `ts` pre-claim,
while the reporter owns the run stream from claim onward. This brings Slack in
line with the GitHub lifecycle, where pre-claim position is a placeholder/check
surface and the running reporter adopts the job later.

**Acceptance:** A queued Slack job shows an immediate receipt with queue
position; position updates edit only that receipt; the streamed plan card is
created at claim time and remains stream-active through the run; a long queue
wait no longer causes `message_not_in_streaming_state` on the first run update.
The claim-time stream keepalive starts as soon as the run stream is created, so
a slow first phase (for example VM provisioning before the next semantic update)
does not recreate the same expiry window.

**Deferred / non-goals:** No automatic Slack Home integration, no cancellation
buttons, and no attempt to keep a pre-claim plan stream alive with heartbeats.
This is a surface/lifecycle split, not a scheduler change.

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

### 0048 — Transient vs permanent stream-append errors

- **id:** `0048-slack-stream-error-classification`
- **status:** `backlog`
- **priority:** `low`
- **relates_to:** `0033-slack-streamed-plan-updates`, `0047-slack-reporting-session`
- **source:** v18 Phase 2 review (2026-06)

**Problem:** `touch_stream`/`upsert_stream_or_blocks` treat **any**
`append_stream` error as permanent stream death — they flip `streaming = false`
and the card falls back to `chat.update` for the rest of its life. A *transient*
Slack/API blip therefore permanently abandons streaming (and stops the keepalive)
for a card that's otherwise fine. Only `message_not_in_streaming_state` /
`MissingMessage` are genuinely terminal for the stream.

**Scope:** Extend `classify_stream_error` (or the call sites) to distinguish
**transient** append failures (network/5xx/rate-limit) from **permanent** ones
(`not_in_streaming_state`, `message_not_found`, `not_owned`). On transient, keep
`streaming = true` and let the next tick/append retry, with a bounded
consecutive-failure budget before giving up; only permanent errors flip to block
mode.

**Acceptance:** A single transient append error does not permanently disable
streaming — the next keepalive/append retries on the stream; a genuine
not-streaming/missing error still falls back to `chat.update` as today.

**Deferred / non-goals:** No change to the keepalive cadence or the block-update
render path; the failure budget/backoff shape is a design detail.

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

*`0037-benchmark-group-run-model` shipped (iteration v14, 2026-06) →
[archive/completed/0037-benchmark-group-run-model.md](archive/completed/0037-benchmark-group-run-model.md).*

*`0038-isolated-benchmark-repetitions` is in progress as iteration **v15** →
[iterations/v15-isolated-benchmark-repetitions.md](iterations/v15-isolated-benchmark-repetitions.md).*

### 0039 — Multi-variant benchmark comparisons

- **id:** `0039-multi-variant-benchmark-comparisons`
- **status:** `backlog`
- **priority:** `medium`
- **depends_on:** `0037-benchmark-group-run-model`
- **relates_to:** `0020-llm-intent-resolution`, `0028-results-summary-restructure`,
  `0015-resource-aware-admission`, `0038-isolated-benchmark-repetitions`
- **source:** experiment-group modeling follow-up (2026-06)

**Problem:** Users legitimately want ad-hoc release/branch comparisons such as
"benchmark tx X on 3.4.0.0.3 and compare it with 3.4.0.0.2". Today comparison
logic is mostly PR-vs-baseline shaped; there is no first-class way to run two
explicit variants under one request and summarize their delta.

**Scope:** Lift the 0037 runtime cap from one active `BenchmarkSpec` to multiple
variants for the same workload. Variant identity belongs to the daemon's
group/spec/run mapping (run → variant/ref/build target), not to columns that may
or may not exist inside `stacks-bench.db`; treat the shared DB as the raw sample
store. Each variant resolves/builds independently through the same reusable
build-VM/cache path and runs in its own isolated VM/snapshot/process, writing
into the group's shared `stacks-bench.db` via the same carry-forward mechanic as
`0038` when multiple executions are involved. The final report renders a
comparison summary modeled on the existing PR-vs-baseline delta, but without
requiring PR semantics. The LLM resolver can later emit this as a structured
comparison request; the initial slice may expose a narrower explicit CLI/Slack
syntax.

Land a daemon-side `max_variants` cap before any LLM/Slack/PR field can fan out
to multiple VM lifecycles. When combined with clean repeats, enforce the product
(`variants × clean_repetitions`) under a hard cap until `0015` grows richer
resource budgets.

**Acceptance:** A request comparing two explicit refs runs both variants,
persists both results into one group DB, and reports a clear delta summary
(percentage change, links/artifacts, and a noise-aware classification) on the
selected surface. The summary must not crown a winner on a sub-noise delta; when
variants have repeats, classify the delta against per-variant variance and the
existing PR-vs-baseline threshold discipline. Requests exceeding `max_variants`
or the total lifecycle cap are rejected before enqueue. Existing PR baseline
comparisons continue to work unchanged.

**Deferred / non-goals:** No matrix comparisons, no automatic baseline
selection beyond the explicitly requested refs, and no parallel variant
scheduling until resource-aware admission/worker-fleet policy exists.

### 0041 — Shared benchmark calibration pass

- **id:** `0041-shared-benchmark-calibration`
- **status:** `backlog`
- **priority:** `medium`
- **depends_on:** `0037-benchmark-group-run-model`
- **relates_to:** `0038-isolated-benchmark-repetitions`,
  `0039-multi-variant-benchmark-comparisons`, `0027-fine-grained-progress`
- **source:** experiment-group modeling follow-up (2026-06)

**Problem:** Every `stacks-bench bench run` invocation currently performs its
own baseline overhead calibration. In a clean-repeat or multi-variant group,
that means each fresh VM repeats calibration work and may add noise that is not
the workload under test.

**Scope:** Add an explicit calibration primitive to `stacks-bench` and the
daemon. The intended shape is a separate `stacks-bench bench calibrate` command
that records calibration output into the group DB, plus a `bench run` option to
reuse an existing calibration and skip recalibrating inside each repeat/variant
VM. The daemon can execute one calibration run at the start of a
`BenchmarkGroup` (potentially in its own VM for the same isolation discipline),
carry the calibrated DB forward, and then run each measured VM with calibration
disabled. This should land as a new workflow step in the model established by
`0037`, not as a retrofit to BenchmarkSpec/BenchmarkRun semantics: calibration
is group-scoped setup for measured runs, not another variant being compared.
The methodological assumption is explicit: `stacks-bench` calibration is a
host-stable block-commit baseline, measured by committing empty blocks in a fork
until tail timings converge. For a host-pinned group, sharing one calibration
removes per-run calibration noise from `0038` variance and cancels the baseline
from `0039` variant deltas.

**Acceptance:** A group can perform one calibration pass, then run multiple
measured executions that reuse that calibration data. The final summary clearly
distinguishes calibration time from measured workload time, and falling back to
per-run calibration remains possible when no reusable calibration exists.

**Deferred / non-goals:** Do not change `stacks-bench` calibration semantics
implicitly for standalone CLI users. Do not optimize calibration distribution
across remote workers until worker-fleet/resource policy exists.

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

### 0017 — Generic phase-event enum

- **id:** `0017-generic-phase-events`
- **status:** `backlog`
- **priority:** `low`
- **unblocks:** `0019-block-validation-recipe`
- **source:** `archive/completed/0008` (v5 forward-looking constraint)

**Problem:** `job_event` carries bench-specific `PhaseBuild*`/`PhaseBench*`; a
second task kind needs task-agnostic phase events.

**Scope:** Collapse to `PhaseStarted{label}`/`PhaseFinished{label}` via an
additive `ALTER TYPE … ADD VALUE` migration.

**Acceptance:** Phase events carry a task-agnostic label; bench + block-val both
fit.

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

### 0026 — Central block/tx index cache (pre-seed)

- **id:** `0026-central-block-index-cache`
- **status:** `backlog`
- **priority:** `medium`
- **depends_on:** *(none)* — gated on a schema spike of `stacks-bench.db`
- **relates_to:** `0037-benchmark-group-run-model`,
  `0038-isolated-benchmark-repetitions`
- **source:** high-value list (2026-06)

**Problem:** Benching a range far from chain tip pays a large indexing cost —
`stacks-bench` walks backward from tip (8.2M+ blocks, +1/~5s) to resolve the
canonical Stacks block per height. That walk is recomputed per job even though,
below the reorg horizon, the canonical mapping is stable.

**Scope:** Extract the **block/tx index portion** (resolved canonical
height→block mapping and tx index) from completed `stacks-bench.db` files and
**merge into a central store** — idempotent, conflict-aware (deeper/newer wins;
a disagreement *below* the finality depth is flagged, not merged). The store is
**keyed and validated by provenance** — network (mainnet/testnet),
chainstate/source identity, the index-schema version, and the
stacks-core/stacks-bench DB-schema version — so a pre-seed can never mix
networks or incompatible layouts. **Pre-seed** a fresh bench DB with the
requested range when present and final, so the bench only indexes the uncovered
tail, then merges its new portion back. A **finality-depth guard** is
load-bearing — pre-seeding a not-yet-final block corrupts the run.

**Acceptance:** A bench over a previously-indexed, final range starts measuring
without re-walking from tip; a partially-covered range indexes only the
uncovered tail and merges it back.

**Group interaction:** Once `0037`/`0038` exist, pre-seeding must be aware of the
group DB carry-forward mechanic: seed only the missing index portion of the
group's DB, never overwrite prior repeats/variants already carried forward.

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
  branches; feeds `0024-slack-card-stage-timings` + the `ReportSurface`
  heartbeat
- **relates_to:** `0017-generic-phase-events`,
  `0037-benchmark-group-run-model`, `0041-shared-benchmark-calibration`
- **source:** high-value list (2026-06)

**Problem:** `--json` emits only the final result on stdout, so the daemon can't
surface sub-phase progress ("indexing 2345/5000", "warming up 111/2000",
"measuring 4876/10000") — the card's in-progress rows show only a static detail.

**Scope:** `stacks-bench` emits **structured progress** as JSONL on a
**dedicated channel**, with a small stable schema (`{phase, current, total}`
plus phase-change events), **versioned** with the profiler protocol since it
lands across all 7 integration branches. **Invariant:** stdout stays reserved
for the final `--json` result and raw stderr is excluded (it carries log/rustc
noise); the leading options are a dedicated `--progress-json-fd N` or a
sentinel-prefixed line, and if the sentinel wins it must name the exact
stream/file it rides and guarantee it can't corrupt an existing parser.
Daemon-side, the runner parses the stream, **debounces** (the PR-comment /
queue-position throttle discipline), and feeds `ReportSurface` heartbeats → the
v8 card's active-row detail. After `0037`, progress events should be attributed
to a workflow step / run so calibration, build, measured run, and future
block-validation work do not collapse into one ambiguous stream.

**Acceptance:** A running bench drives a live sub-phase counter on its surface
(Slack card / PR comment) without spamming `chat.update`.

**Deferred / non-goals:** Progress schema fields, channel choice (fd vs
sentinel), and throttle interval are design questions. Dovetails with `0024`
(durations) — candidates to co-schedule.

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
  `1.66s`) via a shared formatter reused by the PR comment and the Slack card.

**Acceptance:** A completed run renders an overview block, a core-timing table
in human units, and a separate Clarity-cost section, on both the PR comment and
the Slack card.

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
Surface: a thread reply on a Slack results card and/or the portal. After `0037`,
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
