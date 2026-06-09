# 0011: Inbox / job pipeline + v2 schema cutover

- **id:** `0011-job-pipeline-cutover`
- **status:** `shipped`
- **source:** `docs/roadmap-v2.md`
- **follow-ups:** `0013-drop-legacy-jobs-table`

Incremental, additive migration from the legacy single-`jobs` table to the
designed target schema (inbox → policies → identity → new `job` model → cutover).

## What shipped

- `github_webhook` inbox + handler dual-write (legacy + inbox in one transaction);
  processor scaffold with `FOR UPDATE SKIP LOCKED` claim/retry/stuck-claim-sweep; a
  router-based classifier (`BasicClassifier` → per-event `EventHandler`s).
- Identity/tenancy tables + handlers (`allowed_installer`, `github_installation`,
  repo lineage, `github_installation_repo`), resolved from one
  `/repos/{owner}/{repo}` call.
- Authz layer (`target/source_repo_policy`, `trigger_policy`, `github_user`/`_role`,
  per-install scope, soft-revoke) + `github_pull_request` subject model
  (`materialise_pull_request`, soft-close).
- New job schema: `job` + `job_event`/`job_metric`/`job_result` + `github_*_job` link
  tables, `job_status` gained `claimed`, atomic `create_job_with_links`.
- Processor writes real `job` rows (idempotent on `github_webhook_id`);
  `RunnableJobStore` over legacy + v2; cutover prep (`[jobs].source` → `v2`, handler
  inbox-only).
- `sbgh-cli` admin tool (installer/repo/policy/user) + per-table role grants.

## Validation

- Every slice Opus → Codex → fix, gated on build/lint/test; tests grew 117 → 562;
  `setup_pg` testcontainers harness added. Slices 0–11 complete (slice 11 as
  code-prep; the deploy/cleanup-script run is a manual operator step).

## Durable decisions (ADR candidates)

- Subject-vs-provenance split (typed columns vs `job_event.detail` JSONB).
- DB enforces structure (FK/unique/CHECK); app enforces workflow (state machine,
  claim-token invariants).
- Installation = tenant boundary; `allowed_installer` the sole global gate.
  Soft-disable only (no cascade deletes). GitHub numeric IDs as natural keys.
  Token-less handler / privileged processor split with the inbox as an
  at-least-once boundary. Atomic transactional job creation.

## Deferred → backlog

- Physical `DROP TABLE jobs` after a soak window → `0013-drop-legacy-jobs-table`.
- Multi-trigger fan-out per webhook (slice 9 takes first match) — noted in history,
  not a committed item.
