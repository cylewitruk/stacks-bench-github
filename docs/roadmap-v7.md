# Roadmap 7 — Change-impact reporting (vs-baseline delta + confidence)

Successor workstream to [roadmap-v4.md](./roadmap-v4.md). v4 built the **Check
Run surface**; it explicitly deferred the *content* of that surface to here (v4
"Dependencies & related work": *"the vs-baseline delta … is the content the
check's markdown summary is meant to carry … a separate workstream"*). This
roadmap is that content.

> **Goal:** make a benchmark report answer the only question a reviewer
> actually has — *"did this change make block replay faster or slower, and can
> I trust the number?"* — by comparing the run against the **baseline of the
> commit the PR branched from** and attaching a **confidence signal** derived
> from the measured noise floor. A later phase adds an **auto-rerun gate** that
> repeats a run when one sample isn't trustworthy and aggregates to a real
> confidence interval.

Process unchanged: Opus implements, Codex reviews, Opus fixes.

> **Successor/sibling docs:** the Check-Run product surface + permissions live in
> [roadmap-v4.md](./roadmap-v4.md); execution architecture (concurrency, the
> worker/reporter split) in [roadmap-v5.md](./roadmap-v5.md); the multi-task
> platform in [roadmap-v6.md](./roadmap-v6.md). This doc owns the
> **comparison/confidence** product.

## Why

Today both surfaces render **absolute** metrics only
([bench_summary::render_pr_comment](../crates/sbgh-daemon/src/bench_summary.rs)):
*"5000 blocks in 18m, Execution 740µs/block, Commit 340µs/block."* That tells a
reviewer nothing about whether their change helped or hurt — they'd have to
hunt down a comparable run and divide by hand. The value of the bot is the
**delta against a recent fork-point baseline** ("this PR is 1.8% slower than the
commit it branched from"), plus an honest **confidence** read so a sub-noise
wiggle isn't mistaken for a regression.

The data plumbing is already done (this is mostly a *query + math + render*
slice, not a pipeline build):

- [job_metric](../crates/sbgh-core/src/db/postgres_jobs.rs) is written on every
  successful completion — typed `execution_duration_us` / `commit_duration_us`
  columns (no JSONB traversal), plus `measured_blocks` / `warmup_blocks` for the
  workload-match guard.
- [job.git_committed_at](../crates/sbgh-core/src/models.rs) carries the commit's
  own timestamp — the timeline-ordering key for nearest-before.
- Baselines are tagged `job_kind = Baseline`, `trigger_kind ∈ {BranchPush,
  TagCreated}` and already accrue via the `branch_push` triggers.
- The slice-8 schema shipped the supporting partial indexes (`job_baseline_*`).

What's missing is only: a baseline-lookup query (no `JobStore` method reads
`job_metric` today), the comparison/confidence math, and the render line.

## Comparison metric

Compare on **`execution_duration_us + commit_duration_us`** (the combined
Execution+Commit budget), *not* envelope wall-clock. Rationale is empirical: in
the variance study the two buckets vary inversely run-to-run but their **sum is
conserved at 0.37% CV**, while envelope wall-clock carries VM-boot / archive /
teardown noise unrelated to the benched code. (See the variance baseline study;
the per-bucket CVs are ~0.7–1% but the sum is 0.37%.)

These raw fields are **totals over the measured blocks**, so a comparison is
only meaningful when the **workload matches** — same start height, count,
warmup, and spans/profiler flags (different blocks = different work, even at the
same count). This is **not** a post-hoc guard on the chosen candidate; it is a
**filter on the lookup itself** (see Codex Medium #1): the baseline at the exact
merge-base SHA may have run a *different* workload, in which case we must keep
looking, not compare incorrectly or degrade while a matching baseline exists.

**Workload key (a schema reclassification).** Per the project's subject-vs-
provenance rule, the bench args were JSONB *provenance* only because nothing
queried them — the baseline lookup now *does* filter on them, so they earn a
typed, indexed **`workload_key`** column on `job`. It's a normalized/canonical
form (or stable hash) of the *workload-controlling* bench args (start-at, count,
warmup, `--bench-spans-only`, `--no-profiler-kv`, …) — i.e. the resolved args
minus the daemon-injected structural flags (`--json`/`--db`/`bench run`/
`--source`/`--dangerous-no-chainstate-copy`). It's known at **enqueue** for every
job (default_args, or the `/benchmark` override), so it's subject, not outcome,
and lives on `job`. A plain `/benchmark` and a baseline both resolve (via
`bench_args::resolve_bench_args`) to `default_args` → identical key → comparable. A
`/benchmark <custom args>` run gets a distinct key and simply finds no matching
baseline → absolute-only. (A config change to `default_args` naturally
invalidates stale comparisons — the key captures the *effective* args, which is
the correct behaviour.) **The concrete `sha256` spec + worked examples are in
Phase 0.**

## Baseline selection — target-branch anchored, SHA-primary

The anchor is derived from the PR itself, never a hardcoded branch, so it works
in **any fork** an allowed user benchmarks in:

- `pr.base.repo` → which repo the change merges *into*.
- `pr.base.ref` → which branch's timeline (`develop` in the canonical fork,
  `develop2` in a testing fork, …).

**Step 1 — merge-base.** Call the GitHub compare API on the **base** repo with
the **cross-fork-safe `owner:ref` head form** (Codex Medium #3 — a bare branch
name or SHA is ambiguous when the head lives in a fork):

```text
GET /repos/{base_owner}/{base_repo}/compare/{base.ref}...{head_owner}:{head_ref}
```

`merge_base_commit` gives the fork-point `sha` + its `committed_at`. The
`owner:ref` form works for **both** same-repo (head_owner == base_owner) and
cross-fork PRs, so the client takes both head identity parts explicitly rather
than a pre-joined string. This is the commit the PR branched from — comparing
against it isolates the PR's own changes from any drift the base branch accrued
since. (Phase 0 verifies the render for both PR shapes.)

**Step 2 — exact hit (repo-agnostic).** A commit SHA is content-addressed, so
`abc123` in `stacks-network/stacks-core` and `abc123` in a `cylewitruk` fork are
the *same object* — and the benchmark replays the same code, so the measurement
is a property of **the commit, not the repo that ran it**. The exact lookup
therefore ignores repo entirely:

```text
newest completed baseline
WHERE git_commit_hash = merge_base.sha
  AND workload_key    = pr.workload_key      -- must match, or keep looking
```

This is the primary, unambiguous path, and it is what lets a fork PR built on an
upstream `develop` commit compare against the **upstream** baseline at that
commit. The `workload_key` filter is part of the lookup, not a post-hoc check:
the same SHA may carry baselines of different workloads, and we want the matching
one. (Tiebreak when the same (SHA, workload) was benchmarked in more than one
repo: most recent measurement.)

**Step 3 — nearest-before (best-effort fallback).** Only when the exact
merge-base wasn't benchmarked (it was an intermediate commit, not a push HEAD).
Pick the newest baseline *before* the fork-point on the same branch name:

```text
newest completed baseline
WHERE git_ref_display  = base.ref
  AND workload_key     = pr.workload_key
  AND git_committed_at <= merge_base.committed_at
ordered by git_committed_at DESC          -- dedup identical SHAs across forks
```

Repo-agnostic again: a fork's `develop` that tracks upstream is the *same
commits* (same SHAs/timestamps), which dedup collapses; a genuinely divergent
`develop2` carries its own SHAs and coexists.

**Step 4 — render the anchor explicitly.** Because the baseline may live in a
*different repo* than the PR (fork PR → upstream baseline), the report must name
exactly what it compared against, with a link (see Render).

### Trusted-repo set (the one open scoping call)

Step 3 ranges over "repos whose baselines are interchangeable" (a fork network).
Options, increasing precision:

1. **Ref-name across all known repos** *(MVP — no new data)*. Works for the
   normal case (forks mirror upstream branch names); the loose edge — two
   *unrelated* repos that both have a divergent same-named branch — is already
   bounded because we only ingest baselines from App-installed, operator-allowed
   repos, and Step 2's exact-SHA hit (where correctness lives) is unaffected.
2. **Fork-network metadata** *(hardening)*. Persist `source_repo_id` on
   `github_repo` (from the webhook `repository.source.id` / `parent.id`) and
   scope Step 3 to the network root. Add if mixing ever bites.
3. Operator-designated canonical repo per installation — explicit, manual; not
   pursued unless 1 and 2 prove insufficient.

**Decision:** ship #1, note #2 as the documented hardening. Step 3 is labelled
best-effort in the report, so a fuzzy candidate set is acceptable there; Step 2
carries the precise path.

## Confidence with n=1

There's one PR run and (usually) one baseline run per commit — no per-commit
repeats to compute a stddev from. But the **noise has been characterized**
separately (CV on the combined metric), which lets us express a delta in
**sigmas** even from single measurements:

- The difference of two independent single runs has **√2×** the relative noise
  of one run, so `σ_diff ≈ √2 · CV`.
- `z = |delta%| / σ_diff%`.

| `|z|` | Verdict rendered |
| ---- | ---- |
| `< 1σ` | inconclusive (within noise) |
| `1–2σ` | weak signal |
| `2–3σ` | moderate |
| `≥ 3σ` | strong (likely real) |

`CV` is a **config knob** (`[reporting] noise_cv_pct`), not a constant, because
the noise floor is host-specific and changes with infra. (Refinement of the
earlier change-impact threshold note: that used a single-run 3σ≈1.1%; for a
*difference* the right 3σ is ≈ `√2·CV·3` — re-derive when `noise_cv_pct` is
re-measured.)

> **Prerequisite flagged:** the established 0.37% CV is **stale** — it was
> measured SMT-on + unpinned. The pinned / SMT-off host must be **re-baselined**
> and `noise_cv_pct` set from the new number before the confidence read is
> trustworthy. Until then the delta is still shown; the sigma is labelled
> provisional.

When multiple recent baselines exist near the anchor, a later refinement can use
their **empirical** stddev instead of the configured constant; MVP uses the
constant.

## Render

Extend the renderer to take an `Option<BaselineComparison>` carrying the anchor
(`sha`, `repo`, `ref`, `committed_at`, selection-reason), the **PR head identity**
(`head_owner`, `head_ref` — needed for the cross-fork-safe compare link below),
and the computed `delta_pct` / `sigma` / `verdict`. Headline + provenance:

```text
**Execution+Commit** 1080.0s · ▲ 1.8% slower (≈3.5σ — likely real)
Compared against [`stacks-network/stacks-core@abc1234`](…/commit/abc1234)
· committed 2026-05-30 · nearest baseline before fork-point
  (merge-base def4567 not benchmarked)
  · [diff](…/compare/abc1234...cylewitruk:feat-foo)
```

- Two honesty signals: **which commit** (linked) and **how it was chosen**
  (exact fork-point vs. nearest-before, naming the un-benchmarked merge-base).
- The compare link uses the same **cross-fork-safe head form** as the Step-1 API
  call (Codex Low, pass 3): `/compare/<baseline_sha>...<head_owner>:<head_ref>` —
  a bare `<head>` would resolve to a nonexistent/ambiguous branch in the *base*
  repo for a fork PR. (For same-repo PRs `head_owner == base_owner`, so it's
  still correct.) This is why `BaselineComparison` carries the head owner/ref.
- **No comparable baseline** (workload mismatch, empty base-branch timeline,
  base repo not managed) → absolute-only with a one-line reason. Reporting stays
  non-fatal end-to-end (per v4): a baseline-lookup or compare-API failure
  degrades to absolute, never fails the job.

**PR jobs only** (Codex Low #4): a vs-baseline delta needs a PR fork-point, so
it applies to `/benchmark` runs — shared across that job's *two* surfaces (the
PR comment + the PR-head Check `output`, same renderer). **Baseline commit
checks have no merge-base and stay absolute-only** — the renderer simply gets
`None` for them. "Both surfaces" never means baseline checks.

---

## Phase 0: Prerequisites & data audit

**Goal:** Confirm the assumptions the rest rests on before building queries
against them.

**Static checks — DONE (codebase audit, 2026-06-06):**

- ✅ **`git_ref_display` on a `branch_push` baseline is the bare branch name.**
  The webhook strips `refs/heads/` ([webhook_processor.rs:1697](../crates/sbgh-daemon/src/webhook_processor.rs#L1697))
  and stores the remainder (`"develop2"`) at
  [:1791](../crates/sbgh-daemon/src/webhook_processor.rs#L1791). `tag_created`
  stores the bare tag with `GitRefKind::Tag` (:1935); `pr_comment` stores the PR
  head branch (:463). The Step-3 nearest-before key (`git_ref_display =
  base.ref`) therefore aligns: a baseline pushed to `develop2` matches a PR
  targeting `develop2`. (Exact-SHA Step 2 is ref-agnostic, so tag/branch shape is
  irrelevant there.)
- ✅ **Effective workload args are recoverable + a clean key exists** (spec
  below). Resolution is
  [`bench_args::effective_arg_string`](../crates/sbgh-core/src/bench_args.rs)
  (Slice 1 relocated this from the daemon's old `derive_stacks_bench_args`):
  empty → `default_args`, else the override **fully replaces** defaults. Stored
  args live in the `queued` `job_event.detail` as `QueuedEventDetail`
  ([models.rs:535](../crates/sbgh-core/src/models.rs#L535)) — `PrComment.bench_args:
  Vec<String>` / `{BranchPush,TagCreated}.bench_args: Option<String>`. Structural
  flags (`--json --db … bench run --source … --dangerous-no-chainstate-copy`) and
  the `run` subcommand are injected by the
  [bench template](../crates/sbgh-daemon/src/libvirt/templates/sbgh-bench.sh.tmpl)
  and are **never** in `bench_args`, so they're excluded for free.

**Live checks — explicit OPERATOR tasks (need the running host / GitHub):**

- [ ] Confirm baseline `job_metric` rows actually accrue after a `branch_push`
  trigger completes on the host (policy present + run finishes → row written).
- [x] **Confirmed (2026-06-06)** the compare API returns `merge_base_commit.sha` +
  `commit.committer.date` via the **`{head_owner}:{head_ref}` head form** for
  **both same-repo and cross-fork** — Phase 1 client contract locked. Cross-fork
  (`stacks-network/stacks-core` base vs. `cylewitruk:develop2`) → `fa58f05…` @
  2026-05-21; same-repo-fork (`cylewitruk:develop2` vs. `cylewitruk:sbgh/test-1`)
  → `c9bfe10…` @ 2026-05-29. **No PR needed**: compare operates on refs across a
  fork network, so the `gh api` call between upstream and a fork branch settles
  GitHub's cross-fork semantics:

  ```bash
  gh api repos/stacks-network/stacks-core/compare/develop...cylewitruk:develop2 \
    --jq '{merge_base: .merge_base_commit.sha, when: .merge_base_commit.commit.committer.date}'
  ```

  (Our-side request/parse/degrade correctness is covered separately by the
  Slice-3 fake integration test.)

### Workload key — concrete spec

```text
effective    = effective_arg_string(bench_args, config.stacks_bench.default_args)
tokens       = effective.split_whitespace()         // matches bash `read -r -a`; drop empties
workload_key = lower_hex( sha256( compact_json_array(tokens) ) )
```

- Computed at **enqueue** (subject data on `job.workload_key`), reusing the
  *same* `bench_args::resolve_bench_args` + config `default_args` so the key always
  equals the real invocation's workload.
- **Order-preserving** (order-sensitive). The dominant comparison — a bare
  `/benchmark` PR vs. a baseline that tracks `default_args` — is order-identical
  by construction; reordered *custom* args are custom workloads with no expected
  baseline. (Order-insensitive flag-pairing is a documented future refinement,
  not MVP.)
- **Shell-word-ish by current contract** (Codex Phase-0 caveat). `split_whitespace()`
  mirrors the bench template's `read -r -a` exactly, so **quoted/escaped argument
  values are not supported today** — no current bench arg needs them. Long-term
  future-proofing (not MVP): make `bench_args` a `Vec<String>` end-to-end —
  including `trigger_policy.bench_args`, which is a stringly `Option<String>`
  today ([models.rs:296](../crates/sbgh-core/src/models.rs#L296)) — so the key
  derives from structured tokens instead of re-splitting a string.

Worked examples, with `default_args = "--start-at 7800000 --count 5000 --warmup
1000 --no-profiler-kv --bench-spans-only"`:

| Trigger | stored `bench_args` | effective | canonical JSON (pre-hash) |
| ---- | ---- | ---- | ---- |
| bare `/benchmark` | `[]` | `default_args` | `["--start-at","7800000","--count","5000","--warmup","1000","--no-profiler-kv","--bench-spans-only"]` |
| `/benchmark run --count 5000` | `["--count","5000"]` | `--count 5000` | `["--count","5000"]` |
| `branch_push`, `trigger_policy.bench_args = NULL` | `None`→`[]` | `default_args` | *(identical to bare `/benchmark`)* → **same key ✅** |
| `branch_push`, `bench_args = <default_args verbatim>` | `Some(…)` | `default_args` | *(identical)* → **same key ✅** |
| `tag_created`, `bench_args = "--start-at 7900000 --count 5000"` | `Some(…)` | `--start-at 7900000 --count 5000` | `["--start-at","7900000","--count","5000"]` |

**Critical property:** a bare `/benchmark` PR matches a baseline whose
`trigger_policy.bench_args` is **NULL or equals `default_args`** — both resolve
through `bench_args::resolve_bench_args` to the same string → identical key. **Operator
recommendation: leave baseline `trigger_policy.bench_args` NULL** so baselines
auto-track `default_args` and always match bare-`/benchmark` PRs. A change to
`default_args` naturally invalidates older comparisons (each job's key reflects
the effective args at *its* enqueue) — which is correct.

**Backfill:** recompute `workload_key` for existing baseline rows from their
`queued` detail using the *current* `default_args`; where unrecoverable, leave
NULL (NULL never matches the lookup, so the row simply won't serve as a
baseline).

**Status:**

- [x] Static audit complete — assumptions confirmed, workload-key spec pinned
- [x] Static findings + spec reviewed — Codex signed off (shell-word-ish caveat
  documented above)
- [ ] Live checks signed off by operator (baseline rows accrue · compare-API
  shape for same-repo + cross-fork)

---

## Phase 1: Baseline lookup (query + merge-base resolve)

**Goal:** Give the daemon the two reads it needs — the merge-base from GitHub
and the baseline metric from the DB.

**Scope:**

- **GitHub client** ([github/client.rs](../crates/sbgh-core/src/github/client.rs)):
  add `compare_commits(base_owner, base_repo, base_ref, head_owner, head_ref) ->
  MergeBase { sha, committed_at }` to the `GitHubApi` trait + `OctocrabClient` +
  the [fake](../crates/sbgh-core/src/github/test_support.rs). Takes head
  owner+ref **separately** and builds the `{head_owner}:{head_ref}` form so
  cross-fork is correct by construction (Codex Medium #3). Raw GET if octocrab's
  typed compare model is lossy (follow v4 Phase 1's precedent).
- **`JobStore`** ([db/jobs.rs](../crates/sbgh-core/src/db/jobs.rs) + PG +
  in-memory): add `find_baseline_for(merge_base_sha, base_ref,
  merge_base_committed_at, workload_key) -> Option<BaselineMetric>` implementing
  exact-hit → nearest-before, **filtering both candidate sets on `workload_key`**
  (Codex Medium #1), and returning the anchor metadata (sha/repo/ref/
  committed_at/selection-reason) alongside the metric.
- **Migrations:**
  - `job.workload_key TEXT` (the subject-vs-provenance reclassification — see
    Comparison metric), backfilled from the queued-event detail for existing
    rows where recoverable, NULL otherwise (NULL never matches → those just don't
    serve as baselines; acceptable).
  - **Two** partial indexes (Codex Medium #2 — the existing
    `job_baseline_timeline_idx` is `repo_id`-leading and does **not** serve the
    repo-agnostic nearest-before query):
    - exact: `(git_commit_hash, workload_key)` `WHERE job_kind='baseline' AND
      status='completed'`.
    - nearest-before: **ref-leading** `(git_ref_display, workload_key,
      git_committed_at DESC)` `WHERE job_kind='baseline' AND status='completed'`.
  - (Alternative considered & rejected for MVP: scope nearest-before to a repo/
    trusted set so the existing repo-leading index applies — that's the
    fork-metadata hardening path, Decision #4, not the MVP.)

**Status: complete** (Slices 1–4 + Phase-0 audit, all Codex-signed-off).

- [x] Initial implementation completed (Slices 1–4)
- [x] Integration coverage added (PG + in-memory: exact, nearest-before,
  repo-agnostic, workload/ref-scoped, NULL-key ignored, deterministic tie-break)
- [x] Reviewed — Codex signed off (each slice)
- [x] Complete

**Notes:**

- In-memory impl mirrors the same selection logic so reporter tests don't need
  Postgres.
- **Deferred (Codex note, non-blocking):** the partial indexes don't cover the
  secondary `m.created_at` tie-break (it lives on `job_metric`), so a
  same-ref/same-workload timestamp tie needs the join-sort. Fine at expected
  volume; revisit only if this query shows up in profiling.

### Implementation plan

*Drafted; coding is gated on the live compare-API check (esp. the cross-fork
`{head_owner}:{head_ref}` shape). Four slices, one commit each, Codex review
between — usual rhythm.*

**Slice 1 — workload-key helper (sbgh-core, no DB).** Relocate the two pure
functions out of the daemon into a shared `bench_args` module so enqueue and the
driver share one source of truth (DRY):

- `effective_arg_string(stored: &[String], default: &str) -> String` — the old
  daemon `derive_stacks_bench_args`, relocated to
  [bench_args.rs](../crates/sbgh-core/src/bench_args.rs); driver calls the moved fn.
- `normalize_stored(detail: &QueuedEventDetail) -> Vec<String>` — today's
  `bench_args_from_detail` logic.
- `resolve_bench_args(stored: &[String], default: &str) -> ResolvedBenchArgs`
  returning **both** the canonical tokens and the key (Codex Slice-1 refinement —
  call sites avoid recomputing, tests assert the exact tokens):

  ```rust
  pub struct ResolvedBenchArgs {
      pub effective_args: Vec<String>,   // effective_args(...).split_whitespace()
      pub workload_key:   String,        // hex(sha256(compact_json(effective_args)))
  }
  ```

  (`sha2` already a workspace dep.) The driver consumes `effective_args` (joined)
  for the run; enqueue persists `workload_key`.
- Unit tests assert the spec table: bare-PR key == NULL-baseline key; a custom
  arg set differs; the exact canonical token vector round-trips.

**Slice 2 — migration + enqueue wiring.**

- SQL: `ALTER TABLE job ADD COLUMN workload_key TEXT;` (nullable) + the two
  partial indexes:

  ```sql
  CREATE INDEX job_baseline_exact_idx
      ON job (git_commit_hash, workload_key)
      WHERE job_kind = 'baseline' AND status = 'completed';
  CREATE INDEX job_baseline_ref_timeline_idx
      ON job (git_ref_display, workload_key, git_committed_at DESC)
      WHERE job_kind = 'baseline' AND status = 'completed';
  ```

  (The slice-8 `job_baseline_{commit,timeline}_idx` are repo-leading and unused
  by v7 — leave them; dropping is a separate cleanup once nothing else
  references them.)
- `NewJob` / `JobCreationRequest` gain `workload_key: Option<String>`; the
  **enqueue path** ([webhook_processor.rs](../crates/sbgh-daemon/src/webhook_processor.rs),
  which already builds `QueuedEventDetail` and holds `DaemonConfig`) sets
  `workload_key(effective_args(normalize_stored(detail), config.stacks_bench.default_args))`.
  *Wiring check:* confirm `default_args` is in scope there (it already reads
  policy/config — low risk).
- Test: enqueue a bare `/benchmark` and a NULL-`bench_args` baseline → assert the
  **persisted keys are equal**.
- **Workload-aware dedup** (Codex Slice-2 finding): the same-SHA `/benchmark`
  dedup (`find_active_job`, roadmap-v5 Phase 5) gains a `workload_key` arg so it
  only suppresses an active job of the **same** workload. A *different* workload
  on the same SHA (e.g. `/benchmark run --count 1` while a default is active) is
  a distinct benchmark → not deduped → enqueues normally (its own per-job
  check). The deferred partial-unique-index hardening becomes
  `(github_repo_id, git_commit_hash, workload_key)`.
  - *Future product hardening (Codex note, not a blocker):* concurrent
    different-workload runs on one SHA share the same check **name family**. If
    GitHub's checks UI gets muddy, include a short workload label/fingerprint in
    the check display name to disambiguate them. Deferred until observed.

**Slice 3 — `compare_commits` (GitHub client).**

- Trait + `OctocrabClient` + fake:
  `compare_commits(base_owner, base_repo, base_ref, head_owner, head_ref) ->
  Result<Option<MergeBase>>`, `MergeBase { sha, committed_at }`.
- Raw `GET …/compare/{base_ref}...{head_owner}:{head_ref}` with a minimal DTO
  (`merge_base_commit.{sha, commit.committer.date}`) — the v4 raw-call pattern
  where octocrab's typed model is lossy. 404 / missing merge-base → `Ok(None)`
  (caller degrades to absolute-only).
- Fake: a (base_ref, head) → MergeBase map plus a not-found mode.

**Slice 4 — `find_baseline_for` (JobStore + PG + in-memory).**

- `find_baseline_for(merge_base_sha, base_ref, merge_base_committed_at,
  workload_key) -> Result<Option<BaselineMatch>>`.
- `BaselineMatch { metric: JobMetric, anchor: BaselineAnchor }`;
  `BaselineAnchor { repo_id, repo_owner, repo_name, sha, ref_display,
  committed_at, selection: Exact | NearestBefore }` — repo owner/name joined from
  `github_repo` for the render link; `selection` drives the provenance line.
- PG: exact (SHA + workload_key, repo-agnostic, newest metric) → else
  nearest-before (ref + workload_key + `committed_at <=`, newest) — both hit the
  new indexes. In-memory mirrors the two-step logic.
- Tests (`setup_pg_db` + in-memory): exact hit · nearest-before ·
  **repo-agnostic** (baseline under a *different* `github_repo_id` than the PR's
  base repo, found by SHA) · workload mismatch → None · ref mismatch → None ·
  NULL-key baseline ignored.

**Out of scope for Phase 1:** the `compare()` delta/sigma math (Phase 2) and any
render/reporter wiring (Phase 3). Phase 1 lands the two reads + the key —
nothing user-visible yet.

**Backfill:** existing baselines carry NULL `workload_key` until backfilled, so
they won't match until then — but the **pending host re-baseline** (needed for
the noise floor regardless) produces fresh, keyed baselines, so a historical
backfill is low-priority — a separate maintenance command if wanted at all. When
it runs, it MUST use the **fallible** `try_normalize_stored_value` (Slice 1) so an
unparseable historical `detail` becomes a **NULL** key, not a false match against
default baselines (Codex Slice-1 finding); the infallible `normalize_stored_value`
is for the driver's runtime fallback only.

---

## Phase 2: Comparison + confidence (pure module)

**Goal:** A side-effect-free `compare()` that turns (run metric, baseline
metric, `noise_cv_pct`) into a `BaselineComparison`.

**Scope:**

- Combined-metric delta on Execution+Commit. The `workload_key` match is already
  enforced by the Phase 1 lookup, so `compare()` receives a workload-matched
  baseline; it still **defensively asserts** `measured_blocks` / `warmup_blocks`
  equality and returns incomparable on mismatch (belt-and-suspenders against a
  key collision or backfill gap).
- Sigma model: `σ_diff = √2 · noise_cv_pct`, `z = |delta%| / σ_diff`, verdict
  bands as above.
- Unit-tested against the variance-study numbers (a 1.8% delta at CV 0.37%
  lands in the expected band; a 0.3% delta reads inconclusive; a workload
  mismatch returns incomparable).

**Status:**

- [x] Initial implementation completed (`comparison.rs`)
- [x] Integration coverage added (6 unit tests: slower/faster/sub-noise/
  provisional/workload-mismatch/degenerate)
- [x] Reviewed — Codex signed off (`≥ 3σ` boundary wording fixed)
- [x] Complete

---

## Phase 3: Render integration

**Goal:** Surface the delta + confidence + linked anchor on both surfaces.

**Scope:**

- Extend [bench_summary](../crates/sbgh-daemon/src/bench_summary.rs)'s renderer
  with an `Option<BaselineComparison>` arg; add the headline delta line +
  provenance line + compare link. Absolute-only fallback with reason when
  `None`.
- Wire it through the [Reporter](../crates/sbgh-daemon/src/reporter.rs)'s
  `finish()` `Terminal::Completed` seam: after parsing the run summary, resolve
  merge-base (Phase 1) → `find_baseline_for` → `compare()` (Phase 2) → pass into
  the renderer. Every step non-fatal — any failure degrades to absolute-only.
- Config: `[reporting] noise_cv_pct` (default empty → sigma omitted/provisional
  until set) + example + a one-line note that it must track the re-baseline.

**Status:**

- [x] Initial implementation completed (reporter wiring + `bench_summary`
  render + `[reporting].noise_cv_pct` + `allow(dead_code)` removed)
- [x] Integration coverage added (render + reporter-orchestration tests incl.
  moved-head guard + ref-encoding; existing suites green)
- [x] Reviewed — Codex signed off (moved-PR-head guard + diff-link ref encoding)
- [x] Complete

> **Phases 1–3 are the deployable MVP.** Build work for roadmap-v7 is done. The
> remaining items are operator/host tasks (baselines accruing, host re-baseline
> → set `noise_cv_pct`) and the optional Phase 4 auto-rerun gate.

**Notes:**

- Plumbing added: `RunnableJob.workload_key` (set in `assemble_runnable`),
  `RunnableJobStore::find_baseline` (→ `BaselineRef`, resolves the baseline's
  repo `owner/name` in the daemon layer from the anchor's `github_repo_id`),
  `metric_from_run` made `pub` so the reporter builds the PR metric from the
  same extraction it persists.
- The comparison is computed in the **Reporter** (`baseline_comparison`), which
  has `gh`+`config`+`jobs`; `ProgressReporter` only has `gh`+`job`, so the
  `Option<&BaselineComparison>` is passed down into `completed()` → the renderer.
- **PR jobs only**; baseline checks pass `None` → absolute-only (a PR run with
  no comparable baseline also renders absolute-only — no explicit "no baseline"
  note, to avoid the PR-vs-baseline-job ambiguity in the shared renderer).
- Entire path is **non-fatal**: any of get_pull_request / compare_commits /
  find_baseline / parse failing → `None` → absolute-only.

---

## Phase 4 (optional, later): Auto-rerun gate

**Goal:** When a single sample isn't trustworthy, repeat the run and aggregate
to a real confidence interval before stating a verdict.

**Scope (design sketch — not built until the MVP lands + the host is
re-baselined):**

- **Two triggers, one mechanism:** a delta in the **suspicious band** (near the
  noise floor, ~1–2σ → could be noise) *or* in a **shock band** (implausibly
  large, e.g. > 20% → likely an anomaly worth confirming) both reduce to: *don't
  trust n=1 here — gather more samples.*
- **Aggregate:** group `job_metric` rows by `(repo, commit)`; with N runs,
  `SEM = σ/√N`, and the verdict uses a real CI. Stop when the CI **excludes the
  noise floor** (confirms signal) or **brackets zero** (confirms noise), capped
  at `max_reruns`.
- **Re-enqueue:** programmatically queue a repeat of the same SHA, **deliberately
  bypassing** the same-SHA `find_active_job` dedup (v5) — a gate rerun is an
  intentional duplicate.
- **Surface:** the report shows "confirmed over N runs" with the tightened CI.

**Status:**

- [ ] Design pinned (post-MVP)
- [ ] Initial implementation completed
- [ ] Reviewed — Codex signed off
- [ ] Complete

**Notes:**

- Deferred on purpose: building a convergence gate before `noise_cv_pct` is
  re-measured on the pinned host would tune against a stale floor.

---

## Decisions

1. **Comparison metric = Execution+Commit total**, workload-guarded — not
   envelope wall-clock. (Empirical: sum conserved at 0.37% CV; envelope carries
   infra noise.)
2. **Anchor = the PR's target branch + fork-point**, not a hardcoded `develop` —
   so it works in any fork; the reference travels with the PR's commit graph.
3. **Baseline lookup is SHA-primary and repo-agnostic** for the exact hit (a
   measurement is a property of the commit, not the repo) — this is what lets a
   fork PR use an upstream baseline. Nearest-before is a ref-scoped, clearly
   labelled best-effort fallback.
4. **Trusted-repo set = ref-name across known repos (MVP)**, which requires a
   **ref-leading partial index** for nearest-before (the existing timeline index
   is repo-leading and won't serve it — Codex Medium #2); fork-network metadata
   (`source_repo_id`, scoping nearest-before by repo so the existing index
   applies) is the documented hardening, added only if mixing bites.
5. **Confidence = sigmas vs the measured noise floor**, with the **√2**
   correction for comparing two single runs; `noise_cv_pct` is config, not a
   constant, and must track the re-baseline.
6. **Entire path is non-fatal** (inherits v4): a missing baseline, compare-API
   error, or lookup failure degrades to absolute-only, never fails the job.
7. **Workload identity is promoted to a typed `job.workload_key`** and is a
   **filter on the baseline lookup**, not a post-hoc guard (Codex Medium #1).
   Justified by the subject-vs-provenance rule: the args are now queried, so they
   stop being JSONB-only provenance. Comparison applies to **PR `/benchmark`
   jobs only** — baseline commit checks have no merge-base and stay absolute-only
   (Codex Low #4).

## Sequencing notes

- **Phase 0 gates everything** — cheap audit, confirms the match keys + that
  baseline data exists to compare against.
- **Phases 1 → 2 → 3 are the MVP** and deliver the headline value (a linked,
  confidence-annotated delta on every comparable PR run).
- **Phase 4 is optional and last**, and is explicitly blocked on the host
  re-baseline (so the convergence gate tunes against the real noise floor).
- **Independent of v4 Phase 3** (placeholder checks) and v5's deferred
  admission pieces — this is the report *content*, orthogonal to those surfaces.
