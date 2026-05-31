# Roadmap 3 — API-fronted daemon

Successor to [roadmap-v2.md](./roadmap-v2.md) (Phase 1 + Phase 2 cutover). Goal:
collapse the three-role / two-config security model into a single
DB-owning orchestrator that exposes an authenticated HTTP API; `sbgh-handler`
and `sbgh-cli` become thin, DB-less clients. Full design in
[orchestrator-api.md](./orchestrator-api.md).

Process is unchanged: Opus implements, Codex reviews, Opus fixes.

## Why

After the Phase-2 cutover the handler was already reduced to a single
inbox INSERT, so the `sbgh_handler` / `sbgh_orch` / owner role split now
costs more in operational complexity (two configs, grant juggling, an
owner DSN sitting in `docker/.env`) than it buys in isolation. Folding all
DB access behind the orchestrator retires that complexity, removes
operator creds from the repo, and gives first-class operational
visibility (status/listing endpoints) — see
[orchestrator-api.md#goals](./orchestrator-api.md#goals).

---

## Phase 1: Legacy removal (close out roadmap-v2.md slice 12)

**Goal:** Execute the deferred slice-12 cleanup so we start from the
smallest possible surface. Closes out roadmap-v2.md.

**Scope:**

- Drop the legacy `jobs` code path: `JobStore`, `LegacyJobSource`, the
  `[jobs].source` flag (and `JobSource` enum), `ingest_webhook_and_job`.
- Rename `JobV2*` → `Job*` (store, models, helpers).
- Drop the legacy `jobs` table after the soak window (separate migration).
- Remove the `sbgh_orch` legacy-`jobs` grants.

**Status:**

- [ ] Initial implementation completed
- [ ] Integration coverage added (or N/A justified)
- [ ] Review in progress (with Codex)
- [ ] Complete

**Notes:** Independent of the API work — can land first to reduce noise.

---

## Phase 2: API scaffold (server, auth, config)

**Goal:** Stand up the `/api` surface and its auth/config plumbing with no
business endpoints yet — just the skeleton both clients will plug into.

**Scope:**

- New `sbgh-api` crate: request/response DTOs (serde) + a thin typed
  `reqwest` client. No business logic.
- axum server in the orchestrator bound per
  [orchestrator-api.md#binding](./orchestrator-api.md#binding).
- Bearer-token auth middleware with `ingest` / `read` / `admin` scopes;
  constant-time compare; `401`/`403` handling.
- Cookie-file bootstrap (write `0600` token at startup) + the shared
  ingest token config.
- `[api]` config section; `GET /api/health`.

**Status:**

- [ ] Initial implementation completed
- [ ] Integration coverage added (or N/A justified)
- [ ] Review in progress (with Codex)
- [ ] Complete

---

## Phase 3: API surface (all endpoints)

**Goal:** Implement every endpoint the handler and CLI need, including the
listing/status endpoints.

**Scope:**

- **Move the orchestrator to the owner DSN** — the admin endpoints write
  to the installer / repo / policy / user tables, which `sbgh_orch` has no
  grants on; without this the API compiles and fails every admin write.
  (Dropping the now-unused roles is deferred to phase 6; this is just the
  connection switch.)
- `POST /api/webhooks` (write-through inbox persist + dedup + event
  filter) and `GET /api/webhooks`.
- Installations, installers, repos, policies (target/source/trigger),
  users/roles, jobs — per
  [orchestrator-api.md#endpoint-surface](./orchestrator-api.md#endpoint-surface).
- Server-side GitHub `login`/`owner-name` → id resolution via the public
  endpoints (unauthenticated by default; installation token only where one
  applies — see
  [orchestrator-api.md#server-side-github-resolution](./orchestrator-api.md#server-side-github-resolution)).
- Error model + structured request logging (reusing the level
  conventions from the recent logging pass).

**Status:**

- [ ] Initial implementation completed
- [ ] Integration coverage added (or N/A justified)
- [ ] Review in progress (with Codex)
- [ ] Complete

---

## Phase 4: Refactor `sbgh-handler` onto the API

**Goal:** Handler becomes HMAC-verify + forward; drops all DB access.

**Scope:**

- Replace the inbox INSERT with `POST /api/webhooks` via the `sbgh-api`
  client + ingest token.
- Keep HMAC verification, ping/pong, and raw-body forwarding; drop the
  Postgres pool, payload parse-for-fields, and the supported-event
  filter (now owned by the daemon).
- Handler config: drop `DATABASE_URL`; add `SBGH_API_URL` +
  `SBGH_API_INGEST_TOKEN`. Compose: handler reaches host daemon via the
  bridge gateway.

**Status:**

- [ ] Initial implementation completed
- [ ] Integration coverage added (or N/A justified)
- [ ] Review in progress (with Codex)
- [ ] Complete

---

## Phase 5: Refactor `sbgh-cli` onto the API

**Goal:** CLI becomes a pure API client; no DB, no owner creds.

**Scope:**

- Route every `installer` / `repo` / `policy` / `user` command through
  the `sbgh-api` client; auto-read the cookie file (`--cookie` override),
  `--api-url` flag.
- Add the new read commands the API enables: `installation list`,
  `webhook tail`, `jobs list`, `status`.
- Drop direct `sbgh-core::db` use from the admin/read commands.
- **`migrate` stays.** It (and its owner DSN) is the one remaining
  direct-DB path in the CLI and is **not** removed here — it must keep
  working until phase 6 implements startup migrations to replace it.
  Removing it now would strand the compose `migrate` service with nothing
  to run.

**Status:**

- [ ] Initial implementation completed
- [ ] Integration coverage added (or N/A justified)
- [ ] Review in progress (with Codex)
- [ ] Complete

---

## Phase 6: Collapse (migrations-at-startup, single role, one config)

**Goal:** The actual payoff — with both clients off the DB, fold the rest.
Not "remaining cleanup"; this is the step that justifies the exercise.

**Scope (in order):**

1. Implement migrations at orchestrator startup (forward-only; single
   instance) — this must land **before** removing `migrate`, or there's a
   window with no migrator.
2. *Then* remove the CLI `migrate` subcommand and the compose `migrate`
   one-shot service.
3. Delete the role split: drop `sbgh_handler` / `sbgh_orch` and the
   `apply_roles` grant machinery (the orchestrator already connects as
   owner since phase 3).
4. Remove the second config dir; one config, one DB-touching component.
5. Docs: update host-bringup / architecture; fold the `v1-to-v2` upgrade
   notes forward.
6. (Optional, last) rename `sbgh-orchestrator` → `sbgh-daemon`.

**Status:**

- [ ] Initial implementation completed
- [ ] Integration coverage added (or N/A justified)
- [ ] Review in progress (with Codex)
- [ ] Complete

---

## Sequencing notes

- Phase 1 is independent; do it first to shrink the surface.
- Phases 2–3 build the API behind the existing direct-DB paths (no client
  behavior change yet — both can coexist during development).
- Phases 4–5 flip the clients over; they're independent of each other.
- Phase 6 can only run once **both** 4 and 5 have landed (you can't drop a
  role still in use).
