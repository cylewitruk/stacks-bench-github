# Roadmap 4 — Check Run reporting

Successor to [roadmap-v3.md](./roadmap-v3.md). Goal: surface benchmark results
as **GitHub Check Runs** (non-required) on the relevant commit — on the **PR
head** for PR-triggered runs (paired with a summary comment that links to the
check), and on the **pushed/tagged commit** for baseline runs. Checks are the
canonical status surface; the PR comment is the visible human summary (the
Coveralls/Codecov model). PR-path creation is gated by the policy decision the
daemon already computes, so a PR check appears only on repos sbgh manages.

> **Conclusion semantics (revised post-deploy):** the check concludes
> `success` when the benchmark **ran** (produced results) and `failure` when it
> **failed to run** (setup error, VM died, panic, timeout). Perf is data, not a
> gate — a slow/regressed-but-completed run is still `success`. Because the
> check is non-required, a `failure` is a visible red ✗ without blocking the
> merge. (`neutral`/`skipped` remain for the Phase-3 placeholder paths.) The
> check also gets **per-phase `output` PATCHes** while `in_progress` —
> `output.title` ("building"/"running"/…) drives the consolidated one-liner —
> sharing the comment's debounce so the API cadence is ~1 extra PATCH per
> debounce window (see Phase 2).

Process is unchanged: Opus implements, Codex reviews, Opus fixes.

> **Status: Phases 1–2 shipped** (the MVP), implemented + Codex-reviewed +
> deploying. Phase 0's cross-fork spike is **deferred** (the same-repo PR +
> baseline-commit paths are verified and don't depend on it; cross-fork only
> matters when `stacks-network` upstream onboards). Phase 4 is **partial**
> (config + `checks:write` consent done; the host-bringup rollout note pending).
> Phase 3 (placeholder/`skipped` checks) is **re-sequenced onto the v5
> architecture** — its feature scope still stands, but its original mechanism (the
> `pr-check-sync` task, Decision #3) is superseded by the v5 Reporter, so it now
> builds on v5 and lands with [v5](./roadmap-v5.md) Phase 5 (see the Phase 3
> note). Process unchanged: Opus implements, Codex reviews, Opus fixes.
>
> **Successor docs (where the rest moved):** execution architecture — concurrency,
> the worker/reporter split, signal/shutdown — is [roadmap-v5.md](./roadmap-v5.md);
> the multi-task `stacks-github` platform is [roadmap-v6.md](./roadmap-v6.md). v4
> stays the source of truth for the Check-Run **product** surface, the
> `checks:write` permission, and rollout (Phases 0 & 4 remain open here).

## Why

The runner currently reports via a PR **comment** (`create_pr_comment` →
heartbeat `update_pr_comment` → final summary, in
[runner.rs](../crates/sbgh-daemon/src/runner.rs) +
[progress.rs](../crates/sbgh-daemon/src/progress.rs)). A comment is easy to
miss and carries no status semantics. A **Check Run** is strictly better for
this:

- Renders in the PR's **Checks** tab — where reviewers already look — with a
  rich markdown `output` (the vs-baseline delta table fits perfectly).
- It concludes `success`/`failure` on whether the benchmark **ran** (not on
  the numbers), and being **non-required** it reports without gating a merge —
  a `failure` is a visible red ✗, not a block. (`skipped`/`neutral` remain for
  the Phase-3 placeholder paths.)
- Check creation is gated by the **same path that creates the job** —
  `/benchmark` authz/policy for PR jobs, configured `branch_push`/`tag_created`
  triggers for baseline jobs — so a check appears only where a benchmark
  legitimately runs. (The optional Phase 3 placeholder check separately reuses
  the existing `pull_request` open/sync policy evaluation — `evaluate_pr_policies`
  in [webhook_processor.rs](../crates/sbgh-daemon/src/webhook_processor.rs).)

### Explicitly out of scope: merge-queue (`merge_group`) integration

We considered reporting into `stacks-network/stacks-core`'s merge queue's
pre-merge checks and **rejected it** — the ~30-min benchmark is a poor fit:
required → blocks the queue ~30 min per group; non-required → the group
merges before the bench finishes, so the report lands post-merge and is never
seen pre-merge. Performance impact is also largely additive per PR, so the
merge-group combined commit buys little accuracy over the PR head. Decision
recorded; not revisited in this roadmap.

## Reporting model

Checks are the status surface; comments are the human summary (the Coveralls/
Codecov pattern). The check is always created **before the comment** (once the
commit SHA is resolved) so its `html_url` can be linked from the comment. By
trigger:

| Trigger | Check Run | Comment |
| ---- | ---- | ---- |
| `/benchmark` (PR) | on the PR head SHA: `in_progress` (per-phase `output.title`) → `success`/`failure` | yes — an immediate "started, see check ↗" reply, updated in place to the results + delta (one comment, keeps the check link) |
| `branch_push` baseline (e.g. `develop`) | on the **pushed commit**: `in_progress` (per-phase) → `success`/`failure`, results in `output` | none (no PR) |
| `tag_created` baseline | on the **tagged commit** | none |

- **Baselines attach to the commit, not the Actions tab.** GitHub's Actions
  tab is Actions-only; an App's Check Run is commit-anchored and renders in the
  commit's check list (and on the commit page). So every `develop` baseline
  commit carries its result — a better surface than headless.
- **Auto-on-PR-sync benchmarking is deferred** — not a current trigger, and it
  would mean a new policy type + a 30-min job per PR push on a single serial
  runner. Phase 3's placeholder/`skipped` check covers the *discoverability*
  half without auto-running; a real auto-PR trigger can come later.

---

## Phase 0: Cross-fork feasibility spike

**Goal:** De-risk the assumption the whole design rests on — that a Check Run
posted on the **base** repo against a PR's **head SHA** renders on the PR —
*before* building the reporter. Unlike comments (which don't care where the
commit object lives), the Checks API is SHA/repo-bound, and **cross-fork PRs
are the risky case**: the head commit lives in the fork, where the App may not
be installed.

**Scope:**

- Spike against a real **fork** PR into a repo where the App is installed: post
  a `neutral` check on the base repo at the fork's head SHA; confirm it renders
  in the PR's Checks tab. Repeat for an internal (same-repo) PR.
- Pin the posting target + fallback from the result. Likely outcomes:
  - Base-repo @ head SHA renders for both → proceed as designed.
  - Cross-fork doesn't render → **fall back to comment for cross-fork PRs,
    check for same-repo PRs** (reporter picks per `head_repo_id ==
    base_repo_id`), or post on the head repo when the App is installed there
    (your multi-tenant model installs the App on forks too).

**Status:**

- [ ] Spike complete — posting target + cross-fork fallback decided
- [ ] Reviewed — Codex signed off

**Notes:**

- **Deferred until upstream onboarding.** Phases 1–2 shipped against the
  same-repo path (internal PRs where head == base, plus baseline commit checks)
  — which has no cross-fork concern and is verified live. The spike (does a
  base-repo check on a *fork's* head SHA render?) only matters when
  `stacks-network` upstream onboards contributor forks; we'll run it then and
  add the comment-fallback for cross-fork PRs if needed.
- Throwaway spike, not production code; its output is a **decision** that would
  pin the PR posting target + surface-selection fallback.

---

## Phase 1: GitHub Checks API client

**Goal:** Give the daemon's GitHub client the ability to create/update Check
Runs, mirroring the existing comment methods.

**Scope:**

- Extend the `GitHubApi` trait + `OctocrabClient`
  ([github/client.rs](../crates/sbgh-core/src/github/client.rs)) with
  `create_check_run` / `update_check_run` — **both return `PostedCheckRun { id,
  html_url }`** so live paths never re-fetch the URL:
  - `POST /repos/{owner}/{repo}/check-runs`,
    `PATCH /repos/{owner}/{repo}/check-runs/{id}`.
  - Posted per the **Phase 0** decision — base/target repo (`job.repository`)
    against the PR head SHA (`job.commit`) if cross-fork rendering is
    confirmed, else the surface-selection fallback.
- New wire types: `CheckRunStatus` (`queued`/`in_progress`/`completed`),
  `CheckRunConclusion` (`success`/`failure`/`neutral`/`skipped`), `CheckRunOutput`
  (`title`/`summary`/`text`), `PostedCheckRun { id, html_url }` (mirrors
  `PostedComment`; `html_url` from the create response is what the PR comment
  links to — without it the "started, see check" reply needs a second lookup).
- Fake impl in [github/test_support.rs](../crates/sbgh-core/src/github/test_support.rs)
  so runner/handler tests can assert the check lifecycle without GitHub.

**Status:**

- [x] Initial implementation completed
- [x] Integration coverage added (or N/A justified)
- [x] Reviewed — Codex signed off
- [x] Complete

**Notes:**

- Requires the App's **`checks:write`** permission (see Phase 4) — the calls
  fail (`403 resource not accessible by integration`) until the permission is
  granted; reporting degrades rather than failing the run. (`403` is the
  degrade trigger; `401` would mean bad credentials.)
- **`external_id` model gap (deferred to Phase 2's reconcile use):** the
  reconcile GET deserializes a minimal DTO because octocrab's typed `CheckRun`
  omits `external_id`.
- **create/update bypass octocrab's `CheckRun` model entirely** (Codex-flagged
  bug fix): octocrab 0.51–0.53 type `CheckRun.pull_requests` as the full
  `PullRequest` (needs `node_id`), but GitHub returns minimal PR refs — so
  `.send()` *fails to deserialize after the check is created*, orphaning it.
  We keep octocrab's auth/transport but do a raw POST/PATCH with our own
  2-field `CheckRunWriteResp { id, html_url }`. Regression test pins it.

---

## Phase 2: Check Run as the run reporter

**Goal:** Drive a Check Run through the benchmark lifecycle for every run — on
the PR head for `/benchmark`, on the commit for baselines — and (PR runs only)
pair it with the summary comment.

**Scope:**

- **Progress target carries the surface(s)** (in
  [job_source.rs](../crates/sbgh-daemon/src/job_source.rs)): the PR target gains
  an optional `check_run_id` alongside `comment_id`; baseline jobs (today
  `ProgressTarget::LogOnly`) get a new `CommitCheck { sha, check_run_id:
  Option<_> }` target (`None` until `create_check_run` succeeds — and it may stay
  `None`, since reporting is non-fatal) so they post a commit-level check instead
  of running silent (see Decisions #2).
- Runner + `JobReporter`, at each existing lifecycle point:
  - **Create the check once the job has a resolved commit SHA** — a check needs a
    concrete `head_sha`. `branch_push` carries its commit from enqueue, but PR
    (`/benchmark`) and `tag_created` jobs resolve theirs in preflight
    (`pr_head_sha` / `resolve_commit(tags/<tag>)`), so the check is created
    **after** that resolution, not at claim. It's still created **before** the
    comment (`create_check_run` `in_progress`, `"Benchmarking <sha>…"`) so the
    comment can carry its `html_url`.
  - phase/heartbeat → `update_check_run` while `in_progress`: the
    `ProgressPhaseListener` PATCHes `output.title` to the phase name
    (`building`/`running`/`collecting`/…) — which drives the one-liner in the
    PR's consolidated checks view — and `output.summary` with elapsed. Shares
    the comment's debounce (`PR_UPDATE_MIN_INTERVAL`), so this adds at most
    ~1 extra PATCH per debounce window vs. terminal-only updates. Terminal
    phases are skipped here — the reporter owns the conclusion (below).
  - success (benchmark RAN) → `completed` / `success` with the metrics (+
    vs-baseline delta once available). PR runs mirror the summary into the
    comment; baselines carry it in the check `output` (their only surface).
  - failure (benchmark failed to RUN — setup error, VM died, panic, timeout) →
    `completed` / `failure` with `finish_reason` + console tail in
    `output.text`. A slow-but-completed run is still `success` — perf is data,
    not a gate. The check is **non-required**, so a `failure` is a red ✗ that
    doesn't block the merge.
- **Persistent check identity (crash/reclaim).** Mirror the comment mechanism:
  the comment id is persisted on a `CommentPosted` `job_event` (via the
  `github_comment_id` column) and read back on re-claim
  ([job_source.rs](../crates/sbgh-daemon/src/job_source.rs),
  [in_memory_jobs.rs](../crates/sbgh-core/src/db/in_memory_jobs.rs)). A check
  needs the analog — a new `CheckRunCreated` `JobEventKind` and **new
  `github_check_run_id` + `github_check_run_url` columns** on `job_event`, read
  back on re-claim. Do NOT overload `github_comment_id`: in `both` mode a job
  carries both ids, and a check-run id and a comment id are distinct GitHub
  objects. **This implies a DB migration** (a new enum value and two new
  columns).
  - **Persist the URL, not just the id.** Closes the reclaim gap Codex flagged:
    a crash after `create_check_run` + persisting the id but *before* posting the
    PR comment leaves re-claim able to `update_check_run` by id but with no
    `html_url` to build the "started, see check" comment. Persisting
    `github_check_run_url` (from the create response) removes any need for a
    second fetch or a brittle URL-construction guess; `update_check_run` also
    returns `{ id, html_url }` so live paths never re-fetch either.
  - **Residual window — create succeeds, then crash before the DB event.**
    Persisting id/URL only closes crashes *after* `CheckRunCreated` commits; a
    crash in the gap between `create_check_run` succeeding on GitHub and that
    event being written leaves nothing persisted, so a retry would create a
    **duplicate** check (the GitHub side-effect and the local commit aren't
    atomic). Mitigation: set a deterministic `external_id` (the job id) on every
    check and **reconcile before creating** — list the SHA's check runs for our
    App and reuse a match on `external_id` rather than creating a second. (Scope
    the lookup to the retry path to keep the happy path one call. If skipped, the
    residual failure mode is a harmless duplicate check — cosmetic,
    non-fatal — so the `external_id` reconcile is the clean close, not a
    correctness blocker.) **Impl note:** confirm the list call (e.g. `GET
    /repos/{o}/{r}/commits/{sha}/check-runs`, filtered to our App + check name)
    actually returns `external_id` and matches narrowly enough to never reuse
    another App's or another job's run.
- **Failure policy: the *entire* reporting surface is non-fatal.** Neither a
  check nor a comment create/update error (permission not yet granted, transient
  5xx, posting-target mismatch) may fail the benchmark or mark the job failed —
  reporting is cosmetic, not a job outcome. Degradation per case:
  - PR `both`: a failed check still leaves the comment, and vice-versa; if
    **both** fail, log + DB result only.
  - Baseline: there is **no comment surface**, so a failed commit check degrades
    to **log + DB result only** (equivalent to `baseline_report = none`) — the
    implementation must not look for a baseline comment path.
  - **Behavior change to flag:** today the initial PR comment is posted in runner
    preflight with `.await?`, so a comment failure **fails the job**
    ([runner.rs](../crates/sbgh-daemon/src/runner.rs)). This policy makes that
    path non-fatal too — implement it explicitly; don't leave comment-posting
    fatal while checks are non-fatal.
- **Policy-gated by construction.** A `/benchmark` run only happens for an
  authorized PR, so the PR check appears only where it should — no extra gating.
  A baseline check appears only because the operator added the
  `branch_push`/`tag_created` trigger. Both inherit their gating; the reporter
  adds none.

**Status:**

- [x] Initial implementation completed
- [x] Integration coverage added (or N/A justified)
- [x] Reviewed — Codex signed off
- [x] Complete

**Notes:**

- Surface selection per `[reporting]` config (Decisions #1): PR runs default to
  `both` (check + linked summary comment), baselines to a commit check.
- **Conclusion is `success`/`failure` on whether the benchmark RAN** (revised
  from the original always-`neutral` — see the top note). Non-required, so a
  `failure` is a red ✗ that doesn't block; perf is data, not a gate.
- **Per-phase `output` updates** (post-MVP ask): the `ProgressPhaseListener`
  PATCHes `output.title` (the consolidated one-liner: `building`/`running`/…)
  and `output.summary` while `in_progress`, sharing the comment's debounce;
  terminal phases are owned by the reporter's conclusion.
- **App id for the reconcile is auto-resolved** via `GET /app` at startup
  (cached, self-healing on a transient blip) — no `app_id` config value.
- **The whole reporting surface is non-fatal** — check *and* comment
  create/update/persist failures are logged, never fail the benchmark.

---

## Phase 3 (optional): policy-gated placeholder / skipped checks on PR sync

**Goal:** Make the check appear on qualifying PRs *before* anyone runs
`/benchmark`, and explicitly mark configured-but-ineligible PRs.

**Scope:**

- In `PullRequestHandler.handle` (open/sync), branch on the existing
  `evaluate_pr_policies` result:

  | Result | Check behavior |
  | ---- | ---- |
  | `Accepted` | Create a `neutral` check: "comment `/benchmark` to run." |
  | `DeniedTarget` | **No check** — repo isn't an sbgh target; no noise. |
  | `DeniedSource` | `skipped` check with a reason ("source fork not trusted — operator must `policy source allow` it"). |

- `/benchmark` then **adopts that same check** (the `neutral` placeholder →
  `in_progress` → `success`/`failure` + results) rather than creating a fresh
  one.
- **Placeholder needs its own persistence (NOT the Phase 2 `job_event`
  column).** The placeholder check is created *before a job exists*, so there's
  no `job_event` row to store its id on. Phase 3 needs identity keyed by
  `(installation, repo, PR, head_sha)` — e.g. a column/row on or beside
  `github_pull_request` (which `PullRequestHandler` already materialises),
  updated per-sync since a check run is per-SHA. When `/benchmark` enqueues, the
  current head_sha's check id is **copied/linked into the job** so the runner's
  Phase 2 path updates it instead of creating a second check.

**Status:**

- [ ] Initial implementation completed
- [ ] Integration coverage added (or N/A justified)
- [ ] Reviewed — Codex signed off
- [ ] Complete

**Notes:**

- **Re-sequenced onto v5 (read this first).** The *feature* below still stands,
  but **do not build it via the `pr-check-sync` task described next** — that
  mechanism is **superseded by [roadmap-v5.md](./roadmap-v5.md)**. v5 relocates
  all GitHub side-effects out of the runner and into a dedicated **Reporter**,
  which is the natural home for a placeholder check — so the "keep handlers pure
  via a bespoke task" workaround is no longer needed. Build this **after v5**, on
  the Reporter, and land it with **v5 Phase 5** (which already absorbs the
  per-running-job half of the `queued (N before)` updater; this Phase 3 supplies
  the **pre-claim** placeholder persistence the two share). The original
  `(installation, repo, PR, head_sha)` persistence design above is still correct.
- **Architectural tension (the now-superseded reasoning).** Today GitHub
  side-effects live in the **runner**; webhook handlers are pure DB
  classification. Decision #3 kept that invariant by enqueuing a lightweight
  `pr-check-sync` task instead of calling `GitHubApi` inline. **v5's Reporter
  removes this tension entirely** — kept here only to explain why the task
  approach is no longer the plan. Phase 2 delivered value without any of this;
  Phase 3 is the discoverability layer and can ship later or be dropped.

---

## Phase 4: Config, permissions & rollout

**Goal:** Make reporting configurable and roll out the new App permission
safely.

**Scope:**

- Config toggles under a new **`[reporting]`** section (runner/reporting
  behavior, not daemon HTTP-API — keep out of `[api]`):
  - `pr_report = comment | check | both` (default `both` once `checks:write` is
    live — check + linked summary comment; `comment` before the permission).
  - `baseline_report = check | none` (default `check` — the commit-level
    baseline check; `none` keeps baselines headless/DB-only).
- **`checks:write` permission bump** on the GitHub App. Adding a permission
  triggers a **re-consent prompt every installer must approve** before it
  takes effect — document the rollout (a new section in
  [host-bringup.md](./host-bringup.md), and a note in the README quick-setup).
- Until an install has granted it, PR runs fall back to `comment` and baselines
  to `none` (headless/DB-only) — detect the `403` and degrade, or key off config.

**Status:** partial.

- [x] `[reporting]` config (`pr_report` / `baseline_report`) + example
- [x] `checks:write` granted on the operator's fork install (`cylewitruk`)
- [ ] `host-bringup.md` rollout note + README quick-setup mention
- [ ] Multi-installer re-consent rollout (when `stacks-network` upstream onboards)

**Notes:**

- The `app_id`-via-config idea was dropped — the numeric App id is auto-resolved
  via `GET /app` at startup (see Phase 2).

---

## Dependencies & related work

- **vs-baseline delta** (the `project_change_impact_tracking` design — PR
  result vs the latest `develop` baseline, ~1% threshold on Execution+Commit)
  is the *content* the check's markdown summary is meant to carry. It's a
  **separate** workstream: until it lands, the check shows **absolute** metrics
  (exactly the current comment's content). Baseline *data* already accrues via
  the `branch_push` triggers shipped earlier.
- **A DB migration IS required** (corrected): Phase 2's crash/reclaim handling
  needs persisted `github_check_run_id` + `github_check_run_url` columns and a
  `CheckRunCreated` `job_event` kind (see Phase 2 — the URL closes the
  post-create/pre-comment reclaim gap; a deterministic `external_id` + reconcile
  closes the residual pre-DB-event window). Baseline commit checks are jobs too,
  so they share this same `job_event` identity — no extra storage.
  Only Phase 3 needs a **separate** persistence for the *pre-job* placeholder
  check (keyed by install/repo/PR/head_sha, not a `job_event` — see Phase 3),
  linked into the job at enqueue.

## Decisions

Cross-fork rendering and the check-failure policy are resolved in-plan (Phase 0
and Phase 2's failure-policy bullet). The remaining design choices are decided
below — recommended calls with rationale, for Codex to confirm or push back on
in review:

1. **PR reporting → check + summary comment (Codecov split), default `both`.**
   The check is the status surface; the comment is the visible summary that
   **links to** it (posted after the check so the link exists). `[reporting]
   .pr_report` can force `check`-only or `comment`-only; `comment` is also the
   pre-permission fallback. Baselines have no comment — the commit check's
   `output` is their summary.
2. **`ProgressTarget` shape → extend the PR target with an optional
   `check_run_id`, AND add a `CommitCheck { sha, check_run_id: Option<_> }`
   target for baselines** (replacing `LogOnly` for baseline jobs; `LogOnly` stays
   for tests / no-report). Both ids are `Option` — they don't exist until
   `create_check_run` succeeds, and may stay `None` since reporting is non-fatal.
   Rename the PR variant to e.g. `PullRequest { pr_number, comment_id:
   Option<_>, check_run_id: Option<_> }`. Comment + check are two surfaces of one
   PR target; a baseline has the single commit-check surface.
3. **Phase 3 placement → v5 Reporter-owned placeholder check. (Previous
   `pr-check-sync` task decision superseded by [roadmap-v5.md](./roadmap-v5.md).)**
   The original decision enqueued a lightweight `pr-check-sync` task so webhook
   handlers stayed pure-DB and the runner owned GitHub I/O. v5 makes that moot:
   GitHub side-effects move into a dedicated **Reporter**, the natural owner of a
   pre-claim placeholder check — so Phase 3 builds on the Reporter (post-v5,
   landing with v5 Phase 5), not a bespoke task. (Phase 3 is optional; it lands
   only if/when it does — now on the v5 architecture.)
4. **`DeniedSource` → `skipped` check with a reason**, not silence. The repo IS
   a configured target, so a contributor from an untrusted fork gets an
   actionable breadcrumb; noise stays low because it only appears on managed
   targets. (Contrast `DeniedTarget` → no check, since the repo isn't managed.)

## Sequencing notes

- **Phase 0 (cross-fork spike) gates the PR path only** — its outcome pins the
  PR posting target before that reporter code is written. The baseline
  commit-check half of Phase 2 (same-repo commit) doesn't wait on it.
- **Phases 1 → 2 are the MVP** and deliver the headline value (checks on
  benchmarked PRs **and** on `develop` baseline commits); they can land before
  the `checks:write` rollout by shipping behind `pr_report = comment` /
  `baseline_report = none`.
- **Phase 4's permission bump gates activation** — sequence the re-consent
  rollout before flipping the defaults to `both` / `check`.
- **Phase 3 is optional / last** — it's the discoverability layer and carries
  the only real architectural decision (handler↔GitHub coupling).
- The **vs-baseline delta is independent** — the check is its surface, not a
  prerequisite; either can land first.
