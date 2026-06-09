# 0012: API-fronted daemon + role/config collapse

- **id:** `0012-api-fronted-daemon`
- **status:** `shipped`
- **source:** `docs/roadmap-v3.md`
- **follow-ups:** `0013-drop-legacy-jobs-table`

Collapsed the three-role / two-config security model into a single DB-owning
daemon fronted by an authenticated HTTP API; handler + CLI became thin DB-less
clients.

## What shipped

- Phase 1: removed legacy `JobStore`/`LegacyJobSource`/`ingest_webhook_and_job`/
  `[jobs].source`; renamed `JobV2*` → final `Job*`; revoked legacy `jobs` grants.
- Phase 2: `sbgh-api` crate (DTOs + thin `reqwest` client); axum server in the
  daemon; bearer-token auth (`ingest`/`read`/`admin`, constant-time); `0600` cookie
  bootstrap; `[api]` config; `/api/health` + `/whoami`.
- Phase 3: full endpoint surface (webhooks inbox, installers/repos/policies/users/
  roles/installations/jobs CRUD + lists, server-side login→id); admin logic moved to
  `sbgh-core/src/admin/`; daemon on the owner DSN.
- Phase 4: `sbgh-handler` HMAC-verify-and-forward via the API client (dropped its DB
  pool + parsing + `[authorization]`).
- Phase 5: `sbgh-cli` pure API client + read commands; `migrate` retained
  temporarily.
- Phase 6: migrations-at-daemon-startup; dropped CLI `migrate` + the compose
  one-shot; dropped the `sbgh_handler`/`sbgh_orch` role split → one config, one
  DB-touching component.

## Validation

- All six phases Codex-signed-off + merged, gated on build/lint/test (560 after
  Phase 2). Two original follow-ups since completed (`sbgh-cli` onto shared pg;
  crate renamed `sbgh-daemon`).

## Durable decisions (ADR candidates)

- Single DB-owning daemon fronted by an authenticated API; handler/CLI are thin
  DB-less clients.
- Wire contract in a dedicated `sbgh-api` crate (DB shapes don't leak onto the API).
- Scope-based bearer auth (admin⇒read), DB internals never serialized. Server-side
  name→id resolution. Migrations at daemon startup. Handler fail-safe forwarding
  (4xx propagate, 5xx/transport → 502).

## Deferred → backlog

- Physical `DROP TABLE jobs` (Phase 1 left the table abandoned awaiting a soak
  window) → `0013-drop-legacy-jobs-table`.
