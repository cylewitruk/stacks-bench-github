# v2 → v3 upgrade runbook

The end-to-end playlist for cutting a **running** v2 deployment over to the
roadmap-v3 "API-fronted daemon" architecture: the handler and CLI stop
touching Postgres and become `/api` clients, the role split collapses to a
single owner, migrations move to daemon startup, and the host binary is
renamed `sbgh-orchestrator` → `sbgh-daemon`.

For a **fresh** install, follow [host-bringup.md](./host-bringup.md) instead
— it brings a host up directly on v3. This doc is only for upgrading a host
already running v2.

> **One-way by design.** The startup migration drops the `sbgh_handler` /
> `sbgh_orch` roles, and the config dir / systemd unit are renamed. There is
> no down-migration — and once the v3 daemon has run, the v2 binary **cannot
> re-migrate** until you either restore the §4 DB snapshot or manually
> un-record the v3 migration row (then `git revert` + redeploy recreates the
> roles). The owner DSN and all data stay intact, so a clean rollback is
> possible, but it is **not** just "revert the code" — follow
> [§7](#7-rollback) exactly.

## What actually changes

| Layer | v2 (deployed) | v3 (this cutover) |
| ---- | ---- | ---- |
| Handler | writes Postgres directly as `sbgh_handler` (`DATABASE_URL`) | verifies HMAC, forwards to the daemon's `POST /api/webhooks` (ingest token); **no DB** |
| Host binary | `sbgh-orchestrator`, connects as `sbgh_orch` | `sbgh-daemon`, connects as the **owner**, self-migrates at startup, serves `/api` |
| Operator CLI | owner DSN (raw SQL) | pure `/api` client (admin cookie, no DB cred) |
| DB roles | owner + `sbgh_orch` + `sbgh_handler` | one (owner) — the narrow roles are dropped by a startup migration |
| Migrations | `migrate` compose one-shot (`sbgh-cli migrate` + `apply_roles`) | the daemon applies them at startup; the `migrate` service is removed |
| Config dir (host) | `/etc/sbgh/orchestrator` | `/etc/sbgh/daemon` |
| Secrets | DB role passwords in `docker/.env` | a shared ingest token in both `secrets.env` files; no handler DB password |

**Authorization is unchanged.** The DB-backed allowlists
(`allowed_installer`, `supported_repo_root`, `*_repo_policy`,
`github_user_role`) you seeded for v2 carry over verbatim — v3 only moves
*where* the admin writes happen (CLI → `/api` instead of CLI → SQL). No data
migration, no re-seeding. The handler's old `[authorization]` TOML block is
removed (it was already vestigial in v2 — the processor authorizes from the
DB).

## Preconditions

- You're on the v3 commit (`git log --oneline -1`).
- A maintenance/quiet window: GitHub queues + retries webhooks while the
  handler is briefly down, so nothing is lost, but avoid in-flight runs you
  care about.
- DB **owner** access: the `POSTGRES_OWNER_PASSWORD` from the original
  bringup. The bootstrap `sbgh` Postgres user is a superuser — required so
  the startup migration can `DROP ROLE` the narrow roles.
- Keep `docker/.env` (`POSTGRES_OWNER_PASSWORD`, `SMEE_CHANNEL`).

## 1. Generate the shared ingest token

One token, presented by the handler and validated by the daemon. It goes in
**both** `secrets.env` files, so generate it once and reuse it:

```bash
INGEST=$(openssl rand -hex 32); echo "$INGEST"   # note it down
```

## 2. Daemon config — move + repoint (host side)

```bash
# Stop + disable the OLD host unit (we replace it in §5).
sudo systemctl disable --now sbgh-orchestrator.service

# Rename the config dir. config.toml, the App PEM, and any .cookie move with it.
sudo mv /etc/sbgh/orchestrator /etc/sbgh/daemon
sudo chown -R sbgh:sbgh /etc/sbgh/daemon

sudo -u sbgh $EDITOR /etc/sbgh/daemon/config.toml
# Change:
#   [server].database_url → postgres://sbgh:<POSTGRES_OWNER_PASSWORD>@127.0.0.1:5432/sbgh
#                           (the OWNER — NOT the old sbgh_orch DSN; the daemon
#                            now migrates + serves the /api admin writes)
# Add (see config.example.daemon.toml for the section):
#   [api]
#   listen      = ["127.0.0.1:8787", "172.17.0.1:8787"]   # loopback for the CLI +
#                                                          # the docker-bridge gateway
#                                                          # so the handler container
#                                                          # can reach /api
#   cookie_path = "/etc/sbgh/daemon/.cookie"
# Also fix any file paths that referenced /etc/sbgh/orchestrator (e.g. the
# PEM's [github].private_key_path).

# Env-only secret for the unit's EnvironmentFile.
sudo tee /etc/sbgh/daemon/secrets.env >/dev/null <<EOF
SBGH_API_INGEST_TOKEN=$INGEST
EOF
sudo chmod 0600 /etc/sbgh/daemon/secrets.env
sudo chown sbgh:sbgh /etc/sbgh/daemon/secrets.env
```

## 3. Handler config — drop DB, add the ingest token (container side)

```bash
sudo -u sbgh-handler $EDITOR /etc/sbgh/handler/config.toml
# - DELETE the entire [authorization] block. v3 rejects unknown config keys
#   (`deny_unknown_fields`), so a leftover [authorization] is a HARD startup
#   error for the new handler — this step is not optional.
# - Ensure an [api] section with the daemon URL:
#       [api]
#       url = "http://host.docker.internal:8787"

# Add the shared ingest token to the handler's secrets.env (keep the existing
# SBGH_WEBHOOK_SECRET). Use the SAME value as §1 / §2.
echo "SBGH_API_INGEST_TOKEN=$INGEST" | sudo tee -a /etc/sbgh/handler/secrets.env >/dev/null
```

The handler no longer needs a `DATABASE_URL` (v2 set it via compose env; the
v3 compose service drops it). Optionally trim the now-unused
`SBGH_HANDLER_DB_PASSWORD` / `SBGH_ORCH_DB_PASSWORD` from `docker/.env`.

## 4. Pull the new code + build, then snapshot the DB

```bash
cd /path/to/stacks-bench-github
git fetch origin && git checkout main && git pull   # or the v3 tag/commit
just build                                           # → target/release/sbgh-daemon
```

**Snapshot the database before the cutover.** This is the clean rollback
anchor: once the v3 daemon runs (§5) it records the role-drop migration in
`_sqlx_migrations`, after which the v2 binary can no longer migrate (see
[§7](#7-rollback)). Take the dump now, while the DB is still in its v2 state:

```bash
PW=<POSTGRES_OWNER_PASSWORD>
pg_dump "postgres://sbgh:$PW@127.0.0.1:5432/sbgh" -Fc \
  -f /var/lib/sbgh/pre-v3-upgrade.dump
```

## 5. The cutover (order matters)

The daemon's startup migration **drops `sbgh_handler` / `sbgh_orch`**. The
old handler is still connected as `sbgh_handler`, so it must be stopped
first — but Postgres must stay up so the daemon can migrate. So: stop the old
edge, boot the daemon, then bring the new edge up.

```bash
# 1. Stop the OLD edge containers ONLY — leave postgres running.
docker compose -f docker/docker-compose.yml stop handler smee

# 2. Install + start the v3 daemon. It connects as owner, applies all pending
#    migrations (incl. the role-drop), writes the admin cookie, and serves
#    /api. install-daemon.sh installs the renamed unit + binary and starts it.
sudo ./scripts/install-daemon.sh
# Remove the stale old unit + binary now that sbgh-daemon is live.
sudo rm -f /etc/systemd/system/sbgh-orchestrator.service /usr/local/bin/sbgh-orchestrator
sudo systemctl daemon-reload

journalctl -u sbgh-daemon -f
#   expect: "applying database migrations at startup" → "api listening" on
#   127.0.0.1:8787 and 172.17.0.1:8787. Ctrl-C once it's serving.

# 3. Bring the v3 containers up. This rebuilds the handler + smee images from
#    v3 code and recreates them with the new config (SBGH_API_URL +
#    host.docker.internal); the old `migrate` one-shot is gone. Postgres is
#    unchanged, so it isn't disturbed.
docker compose -f docker/docker-compose.yml up -d --build
```

The new handler now reads `SBGH_API_URL` + the ingest token and forwards
verified deliveries to the daemon's `/api` (which is already up from step 2).

## 6. Verify

```bash
PW=<POSTGRES_OWNER_PASSWORD>
DSN="postgres://sbgh:$PW@127.0.0.1:5432/sbgh"

# Roles collapsed to one — the narrow roles are gone.
psql "$DSN" -tAc \
  "SELECT coalesce(string_agg(rolname,','),'(none)') FROM pg_roles \
   WHERE rolname IN ('sbgh_handler','sbgh_orch')"          # → (none)

# /api is reachable and the operator cookie works.
sudo -u sbgh sbgh-cli status                               # → api: ok   scope: admin

# Existing allowlists carried over (sanity — should match what you had on v2).
sudo -u sbgh sbgh-cli installer list
sudo -u sbgh sbgh-cli installation list

# Handler is forwarding (trigger a webhook, or re-deliver one from the App's
# "Advanced" page, then):
docker compose -f docker/docker-compose.yml logs --tail=20 handler   # "forwarded webhook to /api"
sudo -u sbgh sbgh-cli webhook tail --limit 5

# Optional end-to-end: post `/benchmark` on a PR in an allowed repo and watch
# the daemon pick it up.
journalctl -u sbgh-daemon -f
```

Run [`scripts/sanity-check.sh`](../scripts/sanity-check.sh) for a fuller
host check (it now expects the single owner role + `/etc/sbgh/daemon`).

## 7. Rollback

There is no down-migration. Worse, a naïve rollback **strands you**: once the
v3 daemon started in §5 it recorded migration `20260601000001`
(the role drop) in the in-DB `_sqlx_migrations` table. The v2 binary's
embedded migrator doesn't know that version, and `db::migrate()` runs with
`ignore_missing = false`, so the reverted v2 `migrate`/`apply_roles` path
**fails with a "missing migration" error before it can recreate the roles**.
You must first put `_sqlx_migrations` back to the v2 set. Two ways:

**A. Restore the §4 snapshot (preferred).** The dump includes
`_sqlx_migrations` (minus the v3 row) and all data, so the reverted v2 migrate
runs cleanly and `apply_roles` recreates the roles. Roles are cluster-global
and *not* in the dump — that's fine, `apply_roles` creates them.

```bash
PW=<POSTGRES_OWNER_PASSWORD>

# Stop v3 first (nothing may be connected to the `sbgh` DB during the restore).
sudo systemctl disable --now sbgh-daemon.service
docker compose -f docker/docker-compose.yml down

# Restore the pre-cutover snapshot (drops + recreates the `sbgh` DB).
pg_restore -d "postgres://sbgh:$PW@127.0.0.1:5432/postgres" \
  --clean --create /var/lib/sbgh/pre-v3-upgrade.dump

# Revert code + the v2 unit/config dir, then redeploy. The v2 `migrate`
# one-shot re-runs apply_roles and recreates sbgh_handler / sbgh_orch.
git checkout <v2-tag>                                  # or: git revert … ; then rebuild
sudo mv /etc/sbgh/daemon /etc/sbgh/orchestrator        # + point [server].database_url
                                                       #   back at the sbgh_orch DSN
sudo ./scripts/install-orchestrator.sh                 # from the reverted tree
docker compose -f docker/docker-compose.yml up -d --build
```

**B. No snapshot — un-record the v3 migration by hand.** The only v3-era
migration is the role drop (no schema change), so deleting its row makes the
DB match the v2 binary again; the v2 `apply_roles` then recreates the roles:

```bash
PW=<POSTGRES_OWNER_PASSWORD>
psql "postgres://sbgh:$PW@127.0.0.1:5432/sbgh" \
  -c "DELETE FROM _sqlx_migrations WHERE version = 20260601000001"
# then revert code + redeploy as in A — the v2 migrate now succeeds and
# apply_roles recreates sbgh_handler / sbgh_orch.
```

Either way the owner DSN and table data are intact. Don't attempt a partial
downgrade (new daemon + old handler, or vice versa) — the role and `/api`
boundaries changed together.
