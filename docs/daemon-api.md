# Daemon API (design)

Status: **implemented** — including the v25 fleet operator surface. Tracked by
[`0012` (api-fronted daemon)](../planning/archive/completed/0012-api-fronted-daemon.md).

The daemon becomes the single component that owns the database, the
GitHub App key, and the only config file. `sbgh-handler` and `sbgh-cli`
stop touching Postgres entirely and talk to the daemon over an
authenticated HTTP/JSON API under `/api`. This collapses the
three-role/two-config security model into one trusted daemon plus thin,
DB-less clients.

## Goals

- **One DB owner.** Only the daemon connects to Postgres. The
  `sbgh_handler` / `sbgh_orch` role split and the owner DSN in
  `docker/.env` both disappear.
- **Edge holds no DB, no App key.** The handler keeps the webhook HMAC
  secret *and* a submit-scoped ingest token — and nothing else; it
  verifies signatures and forwards. A handler compromise leaks both (the
  attacker can submit forged, HMAC-valid deliveries to the daemon, bounded
  by the daemon's own authz) but never gains DB access or the App key.
- **No operator creds in the repo.** `sbgh-cli` authenticates via a
  daemon-generated cookie file (the `bitcoind` model), not a checked-out
  DSN.
- **Operational visibility.** First-class listing endpoints
  (installations, webhook inbox, jobs) so "what happened to delivery X"
  is a CLI call, not a `psql` archaeology dig.

## Non-goals

- No public/internet exposure of `/api`. It is reachable only from the
  handler (docker bridge) and the operator host (loopback).
- No gRPC/protobuf. REST/JSON keeps it curl-able and integration-friendly;
  cross-service typing comes from a shared Rust crate, not codegen (see
  [Transport](#transport--format)).
- No re-verification of HMAC at the daemon — that stays at the edge.

## Architecture

**Before (the pre-API model, retired):**

```text
internet → handler ──INSERT──▶ github_webhook (inbox) ◀──poll── daemon
                                                                  └─ App key, jobs, processor, runner
operator → sbgh-cli ──owner DSN──▶ Postgres (migrate, installer/repo/policy/user admin)
```

**After (now — Phases 4–6 landed, this is the live shape):**

```text
internet → handler ──HMAC verify──▶ POST /api/webhooks ─┐
                                       (ingest token)    │
operator → sbgh-cli ─────────────────▶ /api/* ───────────┤
                                       (cookie token)     ▼
                                                    daemon ──▶ Postgres (sole client, owner)
                                                       └─ App key, processor, fleet scheduler,
                                                          reporting, GitHub resolution
```

The `github_webhook` inbox table and the processor loop are **unchanged** —
only the *writer* of the inbox moves from "handler via SQL" to
"daemon via its own `POST /api/webhooks` handler" (write-through:
persisted before the 200, so durability is preserved).

## Transport & format

- **HTTP/1.1 + JSON**, base path `/api`. axum (already a handler dep).
- **Shared `sbgh-api` crate** holds the request/response DTOs (plain
  serde) and a thin typed `reqwest` client. The daemon (server),
  handler (client), and CLI (client) all depend on it, so a contract
  change breaks the build everywhere — the compile-time-contract benefit
  of gRPC without proto/codegen, because this is a single-language
  monorepo.
- Idempotent operations (webhook submit; all `allow`/`grant` upserts)
  keep their existing `ON CONFLICT` semantics — safe to retry.

## Authentication & authorization

Auth is HTTP-layer (bearer token); the path prefix is cosmetic, the
**token scope** is the boundary.

### Scopes

| Scope | Holder | Grants |
| ---- | ---- | ---- |
| `ingest` | handler | `POST /api/webhooks` only |
| `read` | operator / dashboards (optional) | all `GET` endpoints |
| `admin` | operator (`sbgh-cli`) | everything (`read` + writes) |

A token presents as `Authorization: Bearer <token>`. The server maps each
configured token to a scope and constant-time-compares. Wrong/missing
token → `401`; valid token, insufficient scope → `403`.

### Token provisioning

- **Operator (`admin`) — cookie file.** On startup the daemon
  writes a random token to `/etc/sbgh/daemon/.cookie` (mode `0600`,
  owner-only), regenerated each boot. `sbgh-cli`, run by the same user on
  the same host, reads it automatically — **no operator secret in any
  config or the repo.** Directly retires the `docker/.env` owner-DSN smell.
- **Handler (`ingest`) — shared static token.** The handler is a
  long-running service in a container, so a cookie file is awkward across
  the boundary. v1: a token the operator sets in *both* the daemon
  config and the handler's `secrets.env` (alongside the HMAC secret it
  already holds). Hardening follow-up: have the daemon generate it
  and write it to a path bind-mounted read-only into the handler
  container (uid-scoped), eliminating the hand-set value.
- **Read-only token (optional):** a third token for future dashboards;
  not required for v1.

### Binding

The `/api` listener binds to **two interfaces**: host loopback (for the
local CLI) *and* the docker-bridge gateway IP (so the handler container
can reach the host daemon — a container **cannot** reach a loopback-only
bind). Both are required; neither is a public interface, and the bridge
bind should be firewalled to the docker subnet. The token is the boundary
on the handler→daemon hop (which crosses the container→host bridge —
see [Deployment](#deployment--topology)).

## Endpoint surface

Everything `sbgh-cli` and the handler need today, plus listing endpoints.
Request bodies/queries mirror the current CLI args; responses are **stable
`sbgh-api` DTOs**, mapped deliberately from `sbgh-core::models` — *not*
serialized straight from the core/DB-shaped structs, so internal columns
added later don't leak into the public API.

### Webhooks (replaces the handler's direct INSERT)

| Method | Path | Scope | Purpose |
| ---- | ---- | ---- | ---- |
| `POST` | `/api/webhooks` | `ingest` | Submit a verified delivery → inbox |
| `GET` | `/api/webhooks` | `read` | List inbox rows (filter `event_type`, `status`, `limit`) |

`POST /api/webhooks` mirrors GitHub's own shape: the handler forwards the
raw JSON body plus `X-GitHub-Event` / `X-GitHub-Delivery` (and the
`X-GitHub-Hook-*` headers for logging) after HMAC verification. The daemon
owns the `SUPPORTED_EVENT_TYPES` allowlist (single source of truth — the
handler no longer duplicates it). Submit-time outcomes:

- **Event type not on the allowlist** (stars, forks, …) → `200`, **no row
  inserted** — preserving today's DoS-aware wire-drop.
- **Allowlisted event** → extract `action` / `installation_id` and insert
  with `ON CONFLICT (delivery_id)` → `recorded`, or `duplicate` on an
  idempotent re-submit.

```json
// 200 OK — allowlisted, stored
{ "result": "recorded", "id": 12345 }   // or { "result": "duplicate" }
// 200 OK — not on the allowlist, NOT stored
{ "result": "ignored", "reason": "unsupported_event_type" }
```

(Whether the *processor* later acts on a stored event — vs. recording an
`ignored_action` outcome on the row — is a separate, downstream decision,
not a submit-time response.)

### Installations (new — operational visibility)

| Method | Path | Scope | Purpose |
| ---- | ---- | ---- | ---- |
| `GET` | `/api/installations` | `read` | List installs (id, account, suspended/deleted, created) |
| `GET` | `/api/installations/{id}` | `read` | Detail: memberships, policies, role grants |

### Installer allowlist (`sbgh-cli installer …`)

| Method | Path | Scope | Replaces |
| ---- | ---- | ---- | ---- |
| `GET` | `/api/installers` | `read` | `installer list` |
| `POST` | `/api/installers` | `admin` | `installer allow` (`{login, note?}`) |
| `POST` | `/api/installers/disable` | `admin` | `installer disable` (`{login}` or `{account_id}`) |

### Supported repo roots (`sbgh-cli repo …`)

| Method | Path | Scope | Replaces |
| ---- | ---- | ---- | ---- |
| `GET` | `/api/repos` | `read` | `repo list` |
| `POST` | `/api/repos` | `admin` | `repo allow` (`{owner, name, note?}`) |
| `POST` | `/api/repos/disable` | `admin` | `repo disable` (`{owner, name}` or `{repo_id}`) |

### Policies (`sbgh-cli policy …`)

| Method | Path | Scope | Replaces |
| ---- | ---- | ---- | ---- |
| `GET` | `/api/policies/target` | `read` | `policy target list` (filter `install_id`) |
| `POST` | `/api/policies/target` | `admin` | `policy target allow` |
| `POST` | `/api/policies/target/disable` | `admin` | `policy target disable` |
| `GET` | `/api/policies/source` | `read` | `policy source list` |
| `POST` | `/api/policies/source` | `admin` | `policy source allow` |
| `POST` | `/api/policies/source/disable` | `admin` | `policy source disable` |
| `GET` | `/api/policies/triggers` | `read` | `policy trigger list` (filter `install_id`, `repo_id`) |
| `POST` | `/api/policies/triggers` | `admin` | `policy trigger add` (`{install_id, repo_id, kind, match_spec, bench_args?, note?}`) |
| `POST` | `/api/policies/triggers/{id}/disable` | `admin` | `policy trigger disable` |

### Users & role grants (`sbgh-cli user …`)

| Method | Path | Scope | Replaces |
| ---- | ---- | ---- | ---- |
| `GET` | `/api/users` | `read` | `user list --users` |
| `GET` | `/api/roles` | `read` | `user list` (filter `install_id`) |
| `POST` | `/api/roles` | `admin` | `user grant` (`{login` or `user_id`, `install`, `repo?`, `role}`) |
| `POST` | `/api/roles/revoke` | `admin` | `user revoke` |

### Jobs, health & diagnostics (new)

| Method | Path | Scope | Purpose |
| ---- | ---- | ---- | ---- |
| `GET` | `/api/jobs` | `read` | List jobs (filter `status`, `limit`) — run visibility |
| `GET` | `/api/health` | none | Liveness probe |
| `GET` | `/api/whoami` | `read` | Echo the scope the caller's token resolved to — confirm auth without a side effect (landed in Phase 2 with health + the auth layer) |

### Worker fleet and block validation

These operator endpoints are separate from the dedicated TLS 1.3 mTLS worker
listener. Workers cannot call the cookie-authenticated operator API.

| Method | Path | Scope | Purpose |
| ---- | ---- | ---- | ---- |
| `GET` | `/api/fleet` | `read` | Registry/session/resource, active attempt, trace, and cleanup summary |
| `GET` | `/api/fleet/metrics` | `read` | Prometheus text for heartbeat/lease, wait, ACK lag, resend pressure, staging, cleanup |
| `POST` | `/api/jobs/block-validation` | `admin` | Enqueue a fully specified block-validation job |
| `POST` | `/api/fleet/workers/{id}/drain` | `admin` | Set/clear durable drain |
| `POST` | `/api/fleet/jobs/{id}/cancel` | `admin` | Durably request cancellation of a running fleet attempt |
| `POST` | `/api/fleet/groups/{id}/recover` | `admin` | Start a new generation from the first spec/run, optionally pinned to a compatible `worker_id` |

The bounded GitHub command `/validate-blocks <epoch> <start> <end>` uses the
same PR-role and target/source policy checks as `/benchmark`. Shard,
concurrency, timeout, and worker are server-owned fleet configuration, never
comment input. The selected read-only chainstate origin is worker-local.

### Not an endpoint

- **Migrations** run at daemon startup, not via the API.
- The `migrate` subcommand of `sbgh-cli` is retired (done in Phase 6 — the
  daemon now applies migrations at startup; see
  [Migrations & role collapse](#migrations--role-collapse)).

## Server-side GitHub resolution

The CLI commands that resolve `login → account_id` / `owner/name →
repo_id` today do so from the *client* via GitHub's public
`/users/{login}` / `/repos/{owner}/{name}` endpoints (unauthenticated,
60/hr/IP). In the new model the **daemon** performs resolution and
the CLI needs no GitHub access at all. Be precise about the credential:

- These are **public** endpoints, and the default path stays
  **unauthenticated (clientless) HTTP** — same as today, just server-side.
  This matters because resolution targets frequently have **no
  installation** the daemon could mint a token for: a *prospective
  installer* (`installer allow` before they've installed) and *source
  forks* (`policy source allow` for an arbitrary fork) belong to no
  installation.
- Where the target *does* belong to a known installation, the daemon MAY
  use that installation token to lift rate limits — but it must fall back
  to the unauthenticated public call when no token applies. (A bare App
  JWT is **not** assumed to raise the limit on arbitrary public `/users` /
  `/repos` reads.)

The API accepts the human-friendly identifiers (`login`, `owner`/`name`)
and the daemon resolves + upserts in one transaction. A resolution failure
surfaces as `502` (`github_resolution_failed`).

## Error model

```json
{ "error": { "code": "snake_case_code", "message": "human readable" } }
```

| Status | When |
| ---- | ---- |
| `400` | Malformed body / invalid params |
| `401` | Missing or invalid token |
| `403` | Valid token, insufficient scope |
| `404` | Unknown id |
| `409` | Constraint conflict not handled by upsert |
| `502` | GitHub id resolution failed (upstream) |
| `503` | DB unavailable |

A duplicate webhook submit is **not** an error — `200` with `result:
"duplicate"` (idempotent), matching today's handler behavior.

## Config changes

### Daemon (gains an `[api]` section)

```toml
[api]
# TWO binds: loopback for the local CLI, and the docker-bridge gateway IP
# so the handler container can reach the host daemon. A container cannot
# reach a service bound only to host loopback, so both are required.
# Never a public interface; firewall the bridge bind to the docker subnet.
listen = ["127.0.0.1:8787", "172.17.0.1:8787"]  # bridge IP is host-specific
# Operator cookie (admin scope), regenerated each boot, mode 0600.
cookie_path = "/etc/sbgh/daemon/.cookie"
# Handler ingest token (v1: shared static; matches the handler's
# secrets.env). A hardening follow-up generates + file-distributes this.
ingest_token = "env:SBGH_API_INGEST_TOKEN"
```

The daemon's `[server].database_url` must move from the `sbgh_orch`
role to the **owner** as soon as it serves admin writes — i.e. in
**phase 3**, not at the phase-6 role collapse. `sbgh_orch` has no write
grants on the installer / repo / policy / user tables, so the admin
endpoints would compile and then fail every write. Using the owner DSN
early costs nothing in blast radius (the daemon already holds the App
key); the *cleanup* of dropping the now-unused roles still happens in
phase 6. (Alternative: add temporary `sbgh_orch` write grants in phase 3
— but moving to the owner DSN is simpler since that's the end state.)

### Handler (loses the DB, gains the API client)

- **Drops:** `DATABASE_URL`, the Postgres pool, the `sbgh_handler` role,
  and the inbox-insert / payload-parse code.
- **Keeps:** `SBGH_WEBHOOK_SECRET` (HMAC).
- **Adds:** `SBGH_API_URL` (e.g. the bridge gateway) + `SBGH_API_INGEST_TOKEN`.

### `sbgh-cli` (loses owner creds) — final state, after phase 6

- **Drops:** `DATABASE_URL` (owner DSN), the `migrate` subcommand, all
  direct `sbgh-core::db` use. (Per [`0012` (api-fronted daemon)](../planning/archive/completed/0012-api-fronted-daemon.md),
  `migrate` is the last of these to go — it survives until phase 6
  replaces it with startup migrations.)
- **Adds:** `--api-url` (default `http://127.0.0.1:8787`) and auto-reads
  the cookie file (default `/etc/sbgh/daemon/.cookie`, override via
  `--cookie`). No secret in the repo.

### docker-compose

- **Remove** the `migrate` one-shot service and the `sbgh_handler` /
  `sbgh_orch` password env. Postgres + handler + smee remain.
- The handler container reaches the host daemon's `/api` via the
  docker-bridge gateway (`extra_hosts: ["host.docker.internal:host-gateway"]`
  or the bridge IP).

## Migrations & role collapse

Once the handler (phase 4) and CLI (phase 5) are off the DB, the
daemon is the **sole** client:

- **Migrations run at startup** — the daemon applies pending
  forward-only migrations before serving. Safe here: single instance, no
  concurrent-migration race, and `sqlx migrate` only applies *pending up*
  migrations (a rollback to an older binary never tries to un-migrate).
  **Rollback caveat:** that only protects the *down* direction. An older
  binary can still fail after a rollback if the newer binary applied a
  schema / enum / data-shape change the old code can't read (we hit
  exactly this with a new enum value once). Rule: **rolling back across a
  schema-changing release is only safe when the migration is
  backward-compatible with the previous binary** — otherwise roll forward
  or restore from backup, don't downgrade.
- **Roles collapse to one.** The daemon already connects as the
  owner (moved in phase 3 for the admin writes); phase 6 just drops the
  now-unused `sbgh_handler` / `sbgh_orch` roles and deletes the
  `apply_roles` grant machinery. Owner alongside the App key does **not**
  widen the daemon's blast radius — it was already the most-trusted
  component. What we *removed* is DB access from the internet-facing edge
  and the owner DSN from the repo.

## Deployment & topology

The handler runs in a container; the daemon runs on the host. So
the handler→daemon hop crosses the **container→host** boundary (the
former handler→Postgres hop stayed inside the compose network). The
daemon's `/api` listener must therefore bind an interface the
container can reach (the docker-bridge gateway) plus loopback for the
CLI, and stay off any public interface. The token secures the hop; the
binding limits who can attempt it.

## Observability

Every `/api` call logs method, path, resolved scope, and outcome, reusing
the level convention from the recent logging pass (denials/policy
violations → `warn`, actions → `info`). The webhook-submit log keeps the
`delivery` / `event` / `installation_id` correlation fields. No payload
bodies, tokens, or signatures are ever logged.

## Security analysis (before → after)

| Surface | Before | After |
| ---- | ---- | ---- |
| Internet-facing edge | HMAC secret + DB INSERT grant | HMAC secret + submit-scoped ingest token |
| Operator DB credential | owner DSN in `docker/.env` (repo clone) | none — operator gets an admin API cookie (`0600`, generated, never committed); the daemon keeps the owner DSN |
| DB roles | owner + `sbgh_orch` + `sbgh_handler` | one (owner = the daemon) |
| Stolen edge cred | forge inbox rows (bounded by daemon authz) | forge inbox rows via `ingest` token (same bound) |
| Stolen operator cred | full DB (owner DSN) | `admin` API (no raw SQL); cookie harder to exfiltrate |
| Daemon | App key + jobs + processor | App key + jobs + processor + DB owner + `/api` |

The trade is deliberate: a slightly larger trusted daemon (which was
already the most-trusted component) in exchange for a low-trust edge (HMAC
secret + a submit-only token, no DB, no App key) and no operator secrets
in the repo. The role split's value had already collapsed after the
Phase-2 cutover left the handler with a single INSERT.

## Decisions (as built)

1. **Handler ingest-token distribution** — **shared static**: the operator
   sets the same token in the daemon's `SBGH_API_INGEST_TOKEN` and the
   handler's `secrets.env`. The daemon-generated + bind-mounted-file
   hardening remains a future option.
2. **Read-only scope** — the `read` scope exists (and `admin` satisfies it,
   so the CLI's read commands work); a *separate* read-only token for
   dashboards is deferred until one appears.
3. **Listener binding** — settled: loopback for the local CLI plus the
   docker host-gateway (`host.docker.internal:host-gateway`, which resolves
   to the bridge gateway) for the handler container.
4. **Rename to `sbgh-daemon`** — done in Phase 6; the crate, binary, config,
   and this doc all use `sbgh-daemon`.
