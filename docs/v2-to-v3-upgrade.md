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
# REMOVE the v2 [jobs] section (e.g. `[jobs]\nsource = "v2"`). roadmap-v3
#   Phase 1 deleted that flag, and v3 rejects unknown config keys
#   (deny_unknown_fields) — a leftover [jobs] block fails startup with
#   "unknown field `jobs`".
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
#    /api. install-daemon.sh installs the unit + the daemon/CLI binaries and
#    starts it.
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

---

## Alternative: fresh start (wiped database)

If you don't need the existing benchmark history, a wipe-and-reinstall is
**simpler** than the in-place upgrade: a fresh DB has no v2 migration history
to reconcile and no in-use roles to drop mid-flight, so there's no §5
ordering dance and no §7 rollback trap.

**What you lose:** the DB job history + metrics (your baseline/delta data),
the webhook inbox, the seeded allowlists, and all installation state. The
archived SQLite results on disk (`paths.results_archive_dir`) survive, but
the `job_result` rows that index them don't. The host infra (LVM, libvirt,
golden image, users, the App registration + PEM) is untouched.

### A1. Stop the old services + wipe the DB

Pick one. The startup migration's role-drop is a no-op on a truly-fresh
cluster, and just cleans up the lingering roles if you keep the cluster.

```bash
PW=<POSTGRES_OWNER_PASSWORD>

# (A) Keep the Postgres cluster, drop just the `sbgh` database:
sudo systemctl disable --now sbgh-orchestrator.service        # old v2 unit
docker compose -f docker/docker-compose.yml stop handler smee # leave postgres up
# Two calls: DROP DATABASE can't run inside the implicit transaction psql
# wraps a multi-statement -c in. PGPASSWORD (not a DSN) so passwords with
# URL-special chars don't need escaping.
export PGPASSWORD="$PW"
psql -h 127.0.0.1 -U sbgh -d postgres -v ON_ERROR_STOP=1 \
  -c "DROP DATABASE IF EXISTS sbgh WITH (FORCE);"
psql -h 127.0.0.1 -U sbgh -d postgres -v ON_ERROR_STOP=1 \
  -c "CREATE DATABASE sbgh OWNER sbgh;"
unset PGPASSWORD

# (B) — or — wipe the whole data dir (fresh cluster; initdb recreates the
# `sbgh` owner from POSTGRES_OWNER_PASSWORD on next `up`):
#   sudo systemctl disable --now sbgh-orchestrator.service
#   docker compose -f docker/docker-compose.yml down
#   # POSTGRES_DATA_DIR lives in docker/.env, not your shell. Source it so the
#   # rm targets the SAME path compose mounts — without this a customized value
#   # silently falls back to the default below and wipes the wrong dir:
#   set -a; . docker/.env; set +a
#   sudo rm -rf "${POSTGRES_DATA_DIR:-/var/lib/sbgh/postgres}"/*
#   docker compose -f docker/docker-compose.yml up -d postgres
```

### A2. Deploy v3

Do the **config edits from [§2](#2-daemon-config--move--repoint-host-side) +
[§3](#3-handler-config--drop-db-add-the-ingest-token-container-side)** (move
the config dir, owner DSN, `[api]` section, drop the handler
`[authorization]` block, the shared ingest token). Then:

```bash
git fetch origin && git checkout main && git pull
just build
sudo ./scripts/install-daemon.sh                              # boots + migrates the FRESH schema + serves /api
sudo rm -f /etc/systemd/system/sbgh-orchestrator.service /usr/local/bin/sbgh-orchestrator
sudo systemctl daemon-reload
docker compose -f docker/docker-compose.yml up -d --build
```

### A3. Re-seed the authorization model

The wipe cleared the allowlists + installation state. The key ordering rule:
**policies and grants reference installation / membership / repo rows that
the daemon only materializes from GitHub installation + PR events** — so you
seed in this order:

1. **Allowlists first** (standalone — the daemon resolves logins/repos
   server-side):

   ```bash
   # The canonical root. Its forks are accepted via lineage.
   sudo -u sbgh sbgh-cli repo allow --owner stacks-network --name stacks-core

   # Every account that will install the App.
   sudo -u sbgh sbgh-cli installer allow --login cylewitruk         # personal testing fork
   sudo -u sbgh sbgh-cli installer allow --login stacks-network     # upstream org
   sudo -u sbgh sbgh-cli installer allow --login cylewitruk-stacks  # a contributor's own fork
   ```

2. **(Re)install the App** on each account via the GitHub UI, selecting its
   `stacks-core`. The wipe dropped the `github_installation` row, so the
   daemon needs a fresh **`installation.created`** to rebuild installation +
   membership state.

   > **If the App is still installed from v2, uninstall it first, then install
   > it again.** Merely reconfiguring the repo selection on an existing install
   > fires only `installation_repositories`, which the daemon discards as noise
   > for an unknown installation — leaving you with no `github_installation`
   > row and `/benchmark` failing authz. (Redelivering the original
   > `installation.created` from the App's *Advanced → Recent Deliveries* page
   > is an equivalent alternative.)

   A clean install fires `installation.created` + `installation_repositories`,
   and the daemon records the `github_installation` (an **install-id**), the
   `github_installation_repo` **membership**, and the `github_repo` identity
   for each fork-of-the-root.

3. **Collect the ids.** `install-id`s come from `installation list`;
   `repo-id`s are GitHub's numeric repo ids (`gh api repos/<owner>/<name>
   --jq .id`, or `repo list` for the root):

   ```bash
   sudo -u sbgh sbgh-cli installation list
   sudo -u sbgh sbgh-cli repo list
   ```

4. **Per installation:** an enabled **target** policy on the base repo, a
   **source** policy on each trusted fork, and the `trigger-pr-benchmark`
   grants. Authz for a `/benchmark` is checked against the **base (target)
   repo's installation**.

   Worked example for the intended setup:

   ```bash
   # --- The operator's personal fork install (cylewitruk/stacks-core) ---
   I_CYL=<cylewitruk install-id>;  R_CYL=$(gh api repos/cylewitruk/stacks-core --jq .id)
   sudo -u sbgh sbgh-cli policy target allow --install-id $I_CYL --repo-id $R_CYL
   sudo -u sbgh sbgh-cli policy source allow --install-id $I_CYL --repo-id $R_CYL  # internal PRs: source == base
   sudo -u sbgh sbgh-cli user grant --login cylewitruk --install $I_CYL --role trigger-pr-benchmark

   # --- The upstream install (stacks-network/stacks-core) ---
   I_SN=<stacks-network install-id>;  R_SN=$(gh api repos/stacks-network/stacks-core --jq .id)
   sudo -u sbgh sbgh-cli policy target allow --install-id $I_SN --repo-id $R_SN
   # Authorize the ORG users on upstream — deliberately NOT `cylewitruk`
   # (a role grant's absence is the denial):
   sudo -u sbgh sbgh-cli user grant --login cylewitruk-stacks --install $I_SN --role trigger-pr-benchmark
   # ...repeat `user grant --login <foo-stacks> --install $I_SN ...` per org contributor.
   ```

5. **Trust contributor forks as PR sources into upstream.** A PR from
   `<fork>` into `stacks-network/stacks-core` also needs a **source** policy
   on `(I_SN, <fork repo-id>)` — the source gate is what allows that fork's
   code to run in the bench VM. The catch: the source policy FK needs the
   fork's `github_repo` row to exist, which the daemon only materializes once
   it has *seen* the fork (the fork is itself installed, or a PR from it
   reaches the inbox). So the smooth path is event-driven:

   ```bash
   # The first /benchmark from a not-yet-trusted fork returns
   # `denied_source_policy`; the daemon logs the fork's source_repo_id:
   journalctl -u sbgh-daemon | grep "policy denied (source)"   # → source_repo_id=<N>
   sudo -u sbgh sbgh-cli policy source allow --install-id $I_SN --repo-id <N>
   # then re-run /benchmark on the PR.
   ```

   A contributor's **own** fork install (`<foo-stacks>/stacks-core`) is seeded
   exactly like `cylewitruk`'s above (target + source on its own repo + a
   self grant), which also materializes its `github_repo` so you can add its
   source policy on `I_SN`.

### A4. Verify

```bash
sudo -u sbgh sbgh-cli status                    # api: ok   scope: admin
sudo -u sbgh sbgh-cli installer list
sudo -u sbgh sbgh-cli installation list
sudo -u sbgh sbgh-cli user list --install $I_SN # grants on the upstream install
```

Then post `/benchmark` on a PR (as an authorized user) and watch
`journalctl -u sbgh-daemon -f` pick it up.
