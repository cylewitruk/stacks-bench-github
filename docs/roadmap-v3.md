# Roadmap 3 — API-fronted daemon

Successor to [roadmap-v2.md](./roadmap-v2.md) (Phase 1 + Phase 2 cutover). Goal:
collapse the three-role / two-config security model into a single
DB-owning daemon that exposes an authenticated HTTP API; `sbgh-handler`
and `sbgh-cli` become thin, DB-less clients. Full design in
[daemon-api.md](./daemon-api.md).

Process is unchanged: Opus implements, Codex reviews, Opus fixes.

> **Status: complete.** All six phases are implemented, reviewed (Codex
> signed off), and merged into the working tree, plus two follow-ups: the
> `sbgh-cli` suites moved off testcontainers onto the shared `setup_pg_db()`,
> and the crate was renamed `sbgh-orchestrator` → `sbgh-daemon`. The single
> remaining deferred item is the physical `DROP TABLE jobs` (Phase 1 — code
> path long gone; awaiting a soak window before the one-line drop migration).

## Why

After the Phase-2 cutover the handler was already reduced to a single
inbox INSERT, so the `sbgh_handler` / `sbgh_orch` / owner role split now
costs more in operational complexity (two configs, grant juggling, an
owner DSN sitting in `docker/.env`) than it buys in isolation. Folding all
DB access behind the daemon retires that complexity, removes
operator creds from the repo, and gives first-class operational
visibility (status/listing endpoints) — see
[daemon-api.md#goals](./daemon-api.md#goals).

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

- [x] Initial implementation completed
- [x] Integration coverage added (or N/A justified)
- [x] Reviewed — Codex signed off
- [x] Complete

**Notes:**

- **Code removal done; table drop deferred.** Everything in scope except
  the physical `DROP TABLE jobs` is done. The legacy `jobs` table is left
  in place (abandoned, no role has grants on it) pending the soak window;
  dropping it is a one-line follow-up migration. roadmap-v2 slice 12's
  todos 5–6 (soak + drop) carry forward as that follow-up.
- **Removed:** legacy `Job`/`NewJob` models, `JobStore` trait +
  Postgres/InMemory impls (`db/jobs.rs`, `db/postgres_jobs.rs`,
  `db/in_memory_jobs.rs`), `LegacyJobSource`, `IngestStore::ingest_webhook_and_job`
  (+ the `IngestOutcome::Recorded.job_id` field, now always-None),
  `[jobs].source` (`JobSource`/`JobsConfig`, parsing, env, defaults,
  tests), and the `sbgh_orch`/`sbgh_handler` grants on `jobs`.
- **Renamed** the whole `JobV2*` family to its final names via a single
  `JobV2`→`Job` token rename (every V2 type contains that substring):
  `JobV2`→`Job`, `NewJobV2`→`NewJob`, `JobV2Store`→`JobStore`,
  `Postgres/InMemoryJobV2Store`→`…JobStore`, `JobV2Source`→`JobSource`;
  files `job_v2.rs`→`jobs.rs` etc. (legacy deleted first to free the
  names). The `RunnableJobStore` trait + `JobSource` adapter stay (one
  impl, but the runner tests use a fake — it's a test seam).
- **Tests:** 562 → 544 (−18: the legacy ingest dual-write tests, the
  `jobs`-grant tests, the legacy `postgres_jobs.rs` suite, the
  `[jobs].source` config tests, and the handler `.jobs()`-empty assertions
  — the last now structural since the handler holds no job store).
- **Verification:** `just build` clean, `just lint` clean (after `just
  fix`), `just test` 544/544.
- **Codex review fixes (round 1):**
  - **High — `apply_roles` no longer revoked stale legacy `jobs` grants.**
    Removing the REVOKE statements was wrong: on a fresh DB the roles never
    had grants, but on an *already-upgraded* DB the prior `sbgh_orch`
    SELECT/UPDATE grant persists. Re-added an explicit revoke — a
    `DO`-block `REVOKE ALL ON TABLE jobs FROM sbgh_handler, sbgh_orch`
    guarded on `pg_tables` existence so it stays a no-op once the soak
    follow-up drops the table. New regression test
    `apply_roles_revokes_stale_legacy_jobs_grants` (seed a stale grant →
    re-apply → assert orch SELECT on `jobs` rejected). Tests 544 → 545.
  - **High (round 2) — verify the *column-level* handler grant too.** The
    pre-cutover handler grant was column-level `INSERT (cols)` / `SELECT
    (cols)`, a distinct ACL from table-level. Strengthened the regression
    test to seed those exact shapes and assert both handler INSERT and
    SELECT are rejected after re-apply. Empirical result: a table-level
    `REVOKE ALL ON TABLE jobs` **does** clear column-level grants in
    Postgres, so the `DO`-block revoke is sufficient — no explicit
    column-level revoke needed.
  - **Medium — `docs/v1-to-v2-upgrade.md` stale.** It still referenced the
    removed `[jobs].source` flag (verify-`v2` precondition, runner-source
    row, `"legacy"` rollback escape hatch). Added a prominent "historical —
    predates Phase 1" banner and corrected the actionable precondition.
  - **Low — stale comments.** `main.rs` ("`sbgh_orch` … SELECT + UPDATE on
    jobs" → inbox + `job` family) and the `IssueCommentHandler` decision
    table (`would_enqueue_job` + legacy-handler-runs-bench → `enqueued_job`
    via `create_job_with_links`).

---

## Phase 2: API scaffold (server, auth, config)

**Goal:** Stand up the `/api` surface and its auth/config plumbing with no
business endpoints yet — just the skeleton both clients will plug into.

**Scope:**

- New `sbgh-api` crate: request/response DTOs (serde) + a thin typed
  `reqwest` client. No business logic.
- axum server in the daemon bound per
  [daemon-api.md#binding](./daemon-api.md#binding).
- Bearer-token auth middleware with `ingest` / `read` / `admin` scopes;
  constant-time compare; `401`/`403` handling.
- Cookie-file bootstrap (write `0600` token at startup) + the shared
  ingest token config.
- `[api]` config section; `GET /api/health`.

**Status:**

- [x] Initial implementation completed
- [x] Integration coverage added (or N/A justified)
- [x] Reviewed — Codex signed off
- [x] Complete

**Notes:**

- **`sbgh-api` crate** (new workspace member): API-shaped DTOs
  (`HealthResponse`, `WhoamiResponse`, `ApiError`/`ErrorBody`) + a thin
  typed `reqwest` `Client` (`health()`, bearer auth, error-envelope
  decoding) + `read_cookie`. Deliberately separate from `sbgh-core` models
  so DB shapes don't leak into the wire contract; server + both clients
  depend on it.
- **`[api]` config** on `DaemonConfig`: `listen` (Vec, default
  `["127.0.0.1:8787"]`, env `SBGH_API_LISTEN` csv), `cookie_path` (default
  `/etc/sbgh/daemon/.cookie`), `ingest_token` (env
  `SBGH_API_INGEST_TOKEN`, kept out of the file). Example config updated.
- **Auth** (`api/auth.rs`): `Scope { Ingest, Read, Admin }` with
  `satisfies` (admin⇒read; ingest orthogonal); `ApiTokens` resolves a
  bearer to a scope via **constant-time** compare (`subtle`); `protect()`
  gates a route group via `route_layer` (401 missing/invalid, 403
  insufficient, `ApiError` JSON body). The resolved scope is inserted into
  request extensions for handlers.
- **Server** (`api/mod.rs`): `build_router` (`/api/health` public +
  `/api/whoami` read-scoped), `bootstrap_cookie` (256-bit token, 0600,
  regenerated each boot, creates parent dir), `serve` (binds **all**
  listeners eagerly, serves concurrently via `try_join_all`). Wired into
  `main` as a third `try_join!` arm alongside runner + processor.
- **Deviation — `/api/whoami` added.** axum's `route_layer` panics on a
  group with zero routes, so the empty Phase-2 scope groups don't work.
  Added `GET /api/whoami` (read scope; echoes the caller's resolved scope)
  — a genuinely useful "confirm my cookie reaches the daemon" diagnostic
  that also makes the auth layer live in production, not just tests.
  Documented in `daemon-api.md`.
- **Tests** (+12, 545 → 557): config `[api]` defaults + toml/env; auth
  `satisfies` matrix, token→scope resolve, and the 401/403/200 matrix via
  oneshot; health public+ok; whoami requires-auth+echoes-scope; cookie
  bootstrap writes 0600 + round-trips through `read_cookie`.
- **Verification:** `just build` clean, `just lint` clean (after `just
  fix`), `just test` 557/557.
- **Codex review fixes (round 1)** — hardening before the real
  admin/write endpoints inherit this auth layer (+3 tests, 557 → 560):
  - **Medium — empty/duplicate tokens could become credentials.**
    `ApiTokens::new` now returns `Result` and rejects empty/whitespace
    tokens (so `SBGH_API_INGEST_TOKEN=""` can't make an empty-token `Bearer`
    header resolve as `ingest`) and tokens shared across scopes; `resolve`
    also short-circuits an empty presented token. Test
    `new_rejects_empty_and_duplicate_tokens`.
  - **Medium — pre-existing cookie files kept unsafe perms.** `.mode(0o600)`
    only applies on *create*; a pre-existing 0644 cookie would be rewritten
    in place. `bootstrap_cookie` now `set_permissions(0o600)` after open and
    **before** writing the secret (no world-readable window). Test
    `bootstrap_cookie_tightens_preexisting_world_readable_file` (pre-creates
    0644).
  - **Low — `ingest_token` env-only.** It's now `#[serde(skip)]` on `RawApi`,
    so a TOML `[api].ingest_token` is a hard `deny_unknown_fields` error
    rather than silently honored (matches the doc/design intent). Test
    `daemon_api_ingest_token_in_toml_is_rejected`.
  - **Low — client missing `whoami()`.** Added `Client::whoami()` so the
    shared wire contract covers the diagnostic the daemon serves.

---

## Phase 3: API surface (all endpoints)

**Goal:** Implement every endpoint the handler and CLI need, including the
listing/status endpoints.

**Scope:**

- **Move the daemon to the owner DSN** — the admin endpoints write
  to the installer / repo / policy / user tables, which `sbgh_orch` has no
  grants on; without this the API compiles and fails every admin write.
  (Dropping the now-unused roles is deferred to phase 6; this is just the
  connection switch.)
- `POST /api/webhooks` (write-through inbox persist + dedup + event
  filter) and `GET /api/webhooks`.
- Installations, installers, repos, policies (target/source/trigger),
  users/roles, jobs — per
  [daemon-api.md#endpoint-surface](./daemon-api.md#endpoint-surface).
- Server-side GitHub `login`/`owner-name` → id resolution via the public
  endpoints (unauthenticated by default; installation token only where one
  applies — see
  [daemon-api.md#server-side-github-resolution](./daemon-api.md#server-side-github-resolution)).
- Error model + structured request logging (reusing the level
  conventions from the recent logging pass).

**Status:** (split into 3a + 3b — this is a large phase)

- [x] Initial implementation completed
- [x] Integration coverage added (or N/A justified)
- [x] Reviewed — Codex signed off
- [x] Complete

**Notes:**

- **3a — foundation + webhooks (done).** Owner-DSN switch (example config +
  host-bringup point at the `sbgh` owner DSN, since the API admin writes
  need privileges `sbgh_orch` lacks). `SUPPORTED_WEBHOOK_EVENT_TYPES` moved
  to `sbgh-core` (single source of truth; handler now references it).
  `ApiState` (owner pool + write-through `IngestStore` + `gh_api_base`) and
  an `ApiErr` → `ApiError`-envelope error model. `POST /api/webhooks`
  (ingest scope; write-through persist + dedup + event-allowlist filter,
  `recorded`/`duplicate`/`ignored`) and `GET /api/webhooks` (read scope;
  filters `event_type`/`status`/`limit`). `sbgh-api` DTOs
  (`WebhookSubmitResponse`, `WebhookSummary`) + `Client::submit_webhook` /
  `list_webhooks`. `auth::protect` made generic over router state. Tests
  (+8 via setup_pg): persist/dedup, action+installation extraction,
  unsupported-event ignored (no row), list + filters + 400-on-bad-status,
  and **per-method scope** on `/api/webhooks` (POST=ingest, GET=read on the
  same path — proving the merge keeps distinct auth). `just build`/`lint`
  clean; webhook + api tests green.
- **3b — admin + listings (done).**
  - **Relocation:** moved the admin/CRUD + GitHub-resolution modules
    (`installer`/`repo`/`policy`/`user`/`gh_resolve`) from `sbgh-cli/src`
    to `sbgh-core/src/admin/` (via `git mv` + `sbgh_core::`→`crate::`), so
    the API server owns the logic without the daemon depending
    backwards on the CLI. `sbgh-cli` now re-exports them from
    `sbgh_core::admin` (CLI bin + tests unchanged; `apply_roles` stays in
    the CLI — migrate-only, deleted in Phase 6). The CLI tests' three
    `sbgh_cli::<mod>::Error` imports repointed to the flat re-exports.
  - **Endpoints** (all per the design's surface): installers / repos /
    policies (target/source/trigger) / users / roles writes + lists,
    installations + jobs lists. Built as scope-gated route groups
    (`ingest`/`read`/`admin`) merged so GET(read)/POST(admin) on the same
    path keep distinct auth. Server-side GitHub `login`/`owner-name` → id
    resolution runs inside the relocated admin fns (the handler passes
    `gh_api_base`).
  - **Error model:** `From` impls map each admin error enum
    (`InstallerError`/`RepoError`/`PolicyError`/`UserError`) to HTTP —
    not-found→404, precondition→409, bad input→400/422, GitHub→502,
    DB→503. `ApiErr` gained a `detail` field: DB/GitHub internals are
    **logged server-side, never serialized** (the Codex 3a fix, now
    applied across all admin errors).
  - **DTOs + client:** API-shaped views + request bodies in `sbgh-api`
    (mapped from core models via a tiny serde `enum_str`/`parse_enum`
    helper, so DB columns don't leak) + ~20 typed `Client` methods.
  - **Tests** (+4 endpoint tests via setup_pg, +mock-GitHub): auth scoping
    (admin/read/ingest 401/403), empty listings, target-policy
    allow→list→disable round-trip + validation (400s), and
    `allow_installer` resolving a login through a spawned mock GitHub. The
    resolution *logic* itself stays covered by the relocated `sbgh-cli`
    tests.
- **Codex review fixes (round 1):**
  - **JSON extractor envelope (Medium).** A bare `Json<T>` extractor
    returns axum's plain-text rejection, bypassing the `ApiError`
    envelope. Added an `ApiJson<T>` `FromRequest` wrapper (`api/extract.rs`)
    that maps any `JsonRejection` → `ApiErr::bad_request(...)`, and swapped
    every admin request extractor onto it (responses stay `Json<View>`).
  - **GitHub resolver hardening (Medium).** The relocated resolver was
    still CLI-shaped: no timeout, raw login interpolated into the URL.
    Added a shared `gh_resolve::http_client()` (10s timeout, `sbgh` UA)
    used by both `resolve_account` and `repo.rs::resolve_repo`, plus
    `is_valid_github_name()` validation **before** URL construction
    (rejects `/`, whitespace, `%`/`#`/`?`, control chars, and the `.`/`..`
    traversal segments) — an unresolvable name maps to not-found.
  - **`deny_unknown_fields` (Low).** Added to all 8 `*Request` DTOs (not
    views/responses) so a typoed field is a 400 rather than a silent drop.
  - **Tests (+2):** `malformed_and_unknown_field_bodies_use_the_error_envelope`
    (malformed JSON and an unknown field both → 400 with the `ApiError`
    envelope) and a `gh_resolve` unit test covering the
    path-injection/traversal rejections.
- **Verification:** `just build`/`just lint` clean; `just test` 32/32 on
  the `api` filter (now on the shared-container nextest setup — no flake).

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

- [x] Initial implementation completed
- [x] Integration coverage added (or N/A justified)
- [x] Reviewed — Codex signed off
- [x] Complete

**Implementation notes:**

- **Route.** `webhook.rs` is now verify-and-forward: HMAC check, local
  `ping`→`pong`, then `Client::submit_webhook(event, delivery, body)` with
  the raw bytes. Dropped the event-type filter, delivery-id check, and
  payload parse (the daemon owns them) plus the DB pool / `NewWebhook` /
  `IngestOutcome`. Response mapping: `Ok` → 200 echoing the daemon's
  `result`; a daemon **4xx** is propagated (permanent — no retry storm); a
  **5xx or transport failure** → 502 so GitHub redelivers.
- **State / main.** `AppState` swaps `Arc<dyn IngestStore>` for a
  `sbgh_api::Client`; `main` drops the pool and builds the client from the
  new config. `Cargo.toml` drops `sqlx`/`uuid`, adds `sbgh-api`.
- **Config.** `HandlerConfig` drops `[server].database_url`, adds
  `[api].url` (`SBGH_API_URL`) + `ingest_token` (**env-only**
  `SBGH_API_INGEST_TOKEN`, `deny_unknown_fields` rejects it in TOML).
- **Tests** (+9, no DB): real route against a mock daemon — asserts
  unsigned/bad-sig never forward, `ping` answered locally, raw body +
  headers + bearer token forwarded verbatim, unsupported events delegated
  (echo `ignored`), and the 4xx-propagate / 5xx-&-unreachable→502 mapping.
- **Deploy wiring.** compose handler service drops `DATABASE_URL` + the
  postgres/migrate deps, adds `SBGH_API_URL` + `host.docker.internal`
  host-gateway; secrets move to `secrets.env` (handler + daemon both
  hold the shared `SBGH_API_INGEST_TOKEN`); daemon systemd unit gains
  an `EnvironmentFile`. Examples + host-bringup updated; `sbgh_handler` DB
  role marked unused-pending-Phase-6.

**Codex review fixes (round 1):**

- **Empty delivery id (Medium).** A missing `X-GitHub-Delivery` became `""`
  and the client always sent it as a present header, so a supported event
  recorded `delivery_id = ''` instead of the intended 400. Fixed on both
  sides: `submit_webhook` now takes `delivery: Option<&str>` and omits the
  header when absent; the daemon rejects missing **or blank/whitespace**
  ids. Tests: handler (absent stays absent, not `""`) + daemon (blank → 400,
  no row).
- **No client timeout (Medium).** `Client` had no request timeout. Added a
  30s default (`new`) plus `with_timeout`; the web-facing handler pins 10s
  so a stalled daemon can't tie up request capacity.
- **Dead handler authorization config (Medium/Low).** `HandlerConfig` still
  parsed `[authorization]` / `SBGH_ALLOWED_*` though the handler authorizes
  nothing. Removed the field, the `AuthorizationConfig` type, and the auth
  helpers entirely; dropped it from the example + host-bringup (authz is the
  daemon's DB-backed allowlists).
- **Stale DB-role docs (Low).** Compose header + host-bringup role
  table/troubleshooting updated: daemon connects as the **owner**
  (not `sbgh_orch`), the handler row shows no DB, and the obsolete
  `permission denied for table jobs` entry became a handler→`/api` 502 /
  ingest-token guide.

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

- [x] Initial implementation completed
- [x] Integration coverage added (or N/A justified)
- [x] Reviewed — Codex signed off
- [x] Complete

**Implementation notes:**

- **CLI bin rewrite.** `installer`/`repo`/`policy`/`user` handlers now build
  a `sbgh_api::Client` and call its typed methods, formatting the response
  DTOs. Added global `--api-url` (default `http://127.0.0.1:8787`) and
  `--cookie` (default `/etc/sbgh/daemon/.cookie`); the client is built
  lazily (reads the admin cookie) so `migrate` never needs it. GitHub
  resolution is gone from the CLI — the daemon resolves server-side, so
  `SBGH_GH_API_BASE_URL` and the `sbgh_core::admin` calls dropped from the
  bin. Exactly-one-of (`--login`/`--account-id`, etc.) is now enforced by
  the server (single source of truth); clap `group` keeps the
  mutual-exclusion client-side.
- **New read commands:** `installation list`, `webhook tail`
  (`--event-type`/`--status`/`--limit`), `jobs list` (`--status`/`--limit`),
  `status` (health + whoami scope).
- **`migrate` unchanged.** Still the lone direct-DB path (owner DSN +
  `apply_roles`); `lib.rs` (`apply_roles` + the `sbgh_core::admin`
  re-exports the CLI tests use) is untouched. `Cargo.toml` adds `sbgh-api`,
  drops now-unused `reqwest`/`serde`/`thiserror` (keeps `sqlx` for migrate).
- **Coverage (+2).** New `sbgh_api::Client`↔real-server contract tests
  (drive the actual router over TCP): every listing endpoint deserializes,
  an installer allow→list→disable write round-trip via mock-GitHub
  resolution, and the error-envelope decode (400 → typed `ClientError::Api`).
  The existing testcontainers CLI suites still cover the admin *logic* (via
  the `sbgh_core::admin` re-exports).
- **Docs.** host-bringup admin section rewritten for the cookie/API model
  (`sudo -u sbgh sbgh-cli …`, no DB cred); examples reflect server-side
  resolution.

---

## Phase 6: Collapse (migrations-at-startup, single role, one config)

**Goal:** The actual payoff — with both clients off the DB, fold the rest.
Not "remaining cleanup"; this is the step that justifies the exercise.

**Scope (in order):**

1. Implement migrations at daemon startup (forward-only; single
   instance) — this must land **before** removing `migrate`, or there's a
   window with no migrator.
2. *Then* remove the CLI `migrate` subcommand and the compose `migrate`
   one-shot service.
3. Delete the role split: drop `sbgh_handler` / `sbgh_orch` and the
   `apply_roles` grant machinery (the daemon already connects as
   owner since phase 3).
4. Remove the second config dir; one config, one DB-touching component.
5. Docs: update host-bringup / architecture; fold the `v1-to-v2` upgrade
   notes forward.
6. (Optional, last) rename the crate to `sbgh-daemon` — **done**: crate,
   binary, `DaemonConfig`, the `/etc/sbgh/daemon` paths, systemd unit,
   install script, and docs all use the new name.

**Status:**

- [x] Initial implementation completed
- [x] Integration coverage added (or N/A justified)
- [x] Reviewed — Codex signed off
- [x] Complete

**Implementation notes:**

- **Startup migrations (1).** The daemon runs `db::migrate(&pool)`
  right after connecting (as the owner), before serving. Single instance —
  sqlx applies only pending *up* migrations.
- **`migrate` retired (2).** Removed the CLI `Migrate` subcommand +
  `run_migrate` (a subcommand is now required) and the compose `migrate`
  one-shot service + its owner/role-password env. Nothing depended on it
  (the handler dropped its `migrate` dep in Phase 4).
- **Role split deleted (3).** Removed `apply_roles` / `upsert_role` /
  `sql_string_literal` from `sbgh-cli` (lib is now just the
  `sbgh_core::admin` re-exports for tests); deleted `tests/grants.rs` and
  the `apply_roles` setup calls from the installer/repo/policy/user suites.
  Added a **best-effort, idempotent** forward migration that `DROP`s the
  legacy `sbgh_handler` / `sbgh_orch` roles (guarded on existence, catches
  `insufficient_privilege`) — no-op on fresh DBs/CI, drops them on the real
  deploy. `sbgh-cli` deps: `sqlx` → dev-only, dropped `tracing`.
- **Config (4).** The CLI has no DB config/DSN (Phase 5); with `migrate`
  gone the owner DSN lives only in the daemon's config — the sole
  DB-touching component. Compose/`.env`/`sanity-check.sh` drop the two
  role passwords; one role (owner) remains.
- **Docs (5).** host-bringup §6 rewritten (no migrate one-shot, single
  role, startup migrations); compose header, `.env.example`,
  `sanity-check.sh`, `daemon-api.md`, and `AGENTS.md` updated; the
  `v1-to-v2` runbook banner folded forward to note the role-split removal.
- **Coverage.** The Phase-5 client↔server contract tests + the full
  postgres suite now exercise the startup-migration path (every test DB is
  built by `db::migrate`, which includes the new drop-roles migration).
- **Follow-ups (since completed):** the `sbgh-cli` suites moved off
  testcontainers onto the shared `setup_pg_db()` (testcontainers removed
  workspace-wide), and the optional crate rename to `sbgh-daemon` (item 6)
  landed.

---

## Sequencing notes

- Phase 1 is independent; do it first to shrink the surface.
- Phases 2–3 build the API behind the existing direct-DB paths (no client
  behavior change yet — both can coexist during development).
- Phases 4–5 flip the clients over; they're independent of each other.
- Phase 6 can only run once **both** 4 and 5 have landed (you can't drop a
  role still in use).
