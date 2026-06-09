# 0007: Check-run surface

- **id:** `0007-check-run-surface`
- **status:** `shipped`
- **source:** `docs/roadmap-v4.md` (Phases 1–2 shipped; Phase 4 partial)
- **follow-ups:** `0014-preclaim-placeholder-checks`

The GitHub Check-Run status surface for every run, alongside the PR comment.

## What shipped

- Checks API on the `GitHubApi` trait + `OctocrabClient` (`create_check_run` /
  `update_check_run` via raw POST/PATCH, bypassing octocrab's broken typed model) +
  a fake.
- A Check Run driven through the full lifecycle per run: created on the PR head SHA
  (`/benchmark`) or the pushed/tagged commit (baseline), `in_progress` →
  `completed` `success`|`failure`, with per-phase `output` PATCHes (shared debounce).
- Codecov-style split: the check is the status surface, created *before* the comment
  so the comment links its `html_url`; PR → `both`, baseline → single commit check.
- The whole reporting surface made **non-fatal** (check/comment failures log +
  degrade; the preflight comment flipped non-fatal too).
- Crash/reclaim persistence: `CheckRunCreated` job_event + `github_check_run_id`/
  `_url` columns; deterministic `external_id` (job id) + reconcile-before-create.
- Phase 4 (partial): `[reporting]` config (`pr_report`/`baseline_report`);
  `checks:write` granted on the operator fork install.

## Validation

- Phases 1–2 Codex-signed-off with integration coverage; same-repo PR + baseline
  paths verified live. A regression test pins the raw POST/PATCH fix.

## Durable decisions (ADR candidates)

- Conclusion semantics: `success` = benchmark **ran** (produced results), `failure`
  = **failed to run** — perf is data, not a gate; a slow-but-completed run is
  `success` (a non-blocking red ✗).
- Reporting surface is non-fatal by policy; the check is always created before the
  comment.
- Check identity persists via id **and** url; reconcile-before-create dedups the
  non-atomic GitHub-vs-DB commit window.

## Deferred → backlog

- Phase 3 placeholder/`skipped` checks (pre-`/benchmark` discoverability) →
  `0014-preclaim-placeholder-checks` (merged with the v5 queue-position thread).
- Phase 0 cross-fork feasibility spike + multi-installer `checks:write` rollout —
  deferred until upstream onboards forks (kept as this item's history, not a
  separate backlog item).
- Rejected: merge-queue integration; auto-on-PR-sync benchmarking (would need a new
  policy type + a serial 30-min job per push).
