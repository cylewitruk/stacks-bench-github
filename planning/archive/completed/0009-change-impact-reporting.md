# 0009: Change-impact reporting (vs-baseline delta + confidence)

- **id:** `0009-change-impact-reporting`
- **status:** `shipped`
- **source:** `docs/roadmap-v7.md` (Phases 1–3)
- **follow-ups:** `0018-auto-rerun-confidence-gate`

PR comments show a vs-baseline delta + confidence instead of absolute numbers in a
vacuum.

## What shipped

- Typed indexed `job.workload_key` (sha256 of canonical effective bench-args) +
  workload-aware same-SHA `/benchmark` dedup.
- Cross-fork merge-base via `compare_commits(...)` on the GitHub client
  (cross-fork-safe head form).
- `find_baseline_for(...)`: exact SHA hit (repo-agnostic) → ref-scoped
  nearest-before fallback, workload-key-filtered, with anchor metadata; PG +
  in-memory + two partial indexes.
- Pure `comparison.rs`: Execution+Commit delta, `σ_diff = √2·noise_cv_pct`, z-based
  verdict bands, measured/warmup-block equality guard (→ incomparable).
- Reporter wiring (non-fatal end-to-end → degrades to absolute-only) +
  `bench_summary` render (headline delta + provenance + cross-fork-safe linked
  anchor/diff).
- `[reporting].noise_cv_pct` knob (empty → sigma omitted); PR jobs only, baselines
  stay absolute.

## Validation

- Phase 2 unit-tested against the variance study; Phase 1/3 integration coverage
  (exact / nearest-before / repo-agnostic / workload-scoped / tie-break + render +
  reporter orchestration); Codex-signed-off.

## Durable decisions (ADR candidates)

- Canonical comparison metric = **Execution+Commit total** (conserved at 0.37% CV),
  not envelope wall-clock.
- Baseline anchor = PR target branch + fork-point (never hardcoded); lookup
  SHA-primary + repo-agnostic for the exact hit; nearest-before is ref-scoped
  best-effort.
- Confidence = sigmas vs the measured noise floor with the √2 correction;
  `noise_cv_pct` is config, not a constant.
- Whole reporting path non-fatal → absolute-only on any failure.

## Deferred → backlog

- Phase 4 auto-rerun gate (confidence/convergence policy) →
  `0018-auto-rerun-confidence-gate`.
- Re-baseline / noise-floor measurement is **operational** (an operator/host task
  with no code deliverable) — not a backlog item.
