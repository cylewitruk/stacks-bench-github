# v1 → v2 upgrade runbook

> **Historical — superseded by [`0012` (api-fronted daemon)](../planning/archive/completed/0012-api-fronted-daemon.md).** Two waves
> have obsoleted parts of this runbook:
>
> - **Phase 1** removed the legacy `jobs` code path entirely: the
>   `[jobs].source` flag (and its `"legacy"` value) no longer exist, and the
>   runner is unconditionally on the `job` family. Any step below that
>   mentions `[jobs].source` is obsolete — there's nothing to set, and the
>   runner-only "legacy" escape hatch in §9 is gone (rollback is a git
>   revert + redeploy, as that section concludes).
> - **Phases 4–6** removed the DB role split this runbook configures. The
>   handler and CLI are now `/api` clients with no DB access; there is a
>   single DB role (the owner); `apply_roles`, the `sbgh_handler` /
>   `sbgh_orch` roles, and the `migrate` container/subcommand are gone (the
>   daemon migrates at startup). Any step below about role grants,
>   `SBGH_HANDLER_DB_PASSWORD` / `SBGH_ORCH_DB_PASSWORD`, or the `migrate`
>   one-shot no longer applies.
>
> Kept for the historical cutover record; a current deployment is always on
> v2 with the Phase-6 single-role topology.

The end-to-end playlist for cutting a **running** deployment over from the
legacy `jobs` queue (v1) to the new `job` family (v2). This is the
operational counterpart to the Phase 2 cutover (roadmap slices 8–11).

For a **fresh** install, follow [host-bringup.md](./host-bringup.md)
instead — it already brings the stack up on `v2` (the default). This doc
is only for upgrading a host that is already serving `/benchmark` on v1.

> **One-way by design.** Per the "go for it" cutover decision, the new
> handler is **inbox-only** and its DB grant to legacy `jobs` is
> **revoked**. Flipping `[jobs].source` back to `legacy` is a
> runner-only escape hatch — it does **not** restore handler dual-write.
> A true rollback means reverting the git commit and redeploying (see
> [§9](#9-rollback-and-patch-forward)).

## What actually changes

| Layer | v1 (legacy) | v2 (this cutover) |
| ---- | ---- | ---- |
| Handler | Verifies HMAC, parses `/benchmark`, **writes `jobs` rows** directly | Inbox-only: records every event as a `github_webhook` row, nothing else |
| `sbgh_handler` DB grant | `INSERT`/`SELECT` on `jobs` | `INSERT` on `github_webhook` only — **all `jobs` access revoked** |
| Job creation | Handler, synchronously | Daemon **processor**: classifies inbox rows → creates `job` (+ subject links) |
| Runner source | `jobs` table | `job` family, selected by `[jobs].source = "v2"` |
| Tag baselines | n/a | `CreateHandler` enqueues a `baseline` job; runner resolves the tag→commit at claim time |

Three moving parts must all advance together: the **handler image**
(inbox-only code), the **CLI/migrate image** (the grant revoke lives in
`apply_roles`), and the **host daemon binary** (processor + v2
runner). Deploy them in the order below or the handler will hit
`permission denied for table jobs` against the new grants.

## 1. Preconditions

- You're on the cutover commit (or later) — `git log --oneline -1` should
  show the slice 11 cutover landed. (On the original cutover commit the
  runner backend was set via `[jobs].source = "v2"`; roadmap-v3 Phase 1
  later removed that flag, so on current code there is nothing to verify —
  the runner is always on the `job` family.)
- A maintenance/quiet window: no in-flight benchmark you care about, and
  you can tolerate webhooks queuing for a few minutes.
- DB owner access (`postgres://sbgh:$POSTGRES_OWNER_PASSWORD@...`) for the
  one-off cleanup SQL. The `migrate` container already has this.
- `docker/.env` still holds the three DB passwords from the original
  bringup (the migrate step re-asserts them; don't rotate them here).

## 2. Drain the legacy queue

Let any v1 jobs finish so nothing is lost when the runner stops claiming
from `jobs`.

```bash
# Watch the legacy queue drain. Wait until no rows are queued/claimed/running.
psql "$DATABASE_URL" -c "
  SELECT status, count(*)
  FROM jobs
  WHERE status IN ('queued','claimed','running')
  GROUP BY status;
"
```

When that returns zero rows, stop the **old** host daemon so it
stops claiming legacy work and releases the DB connections:

```bash
sudo systemctl stop sbgh-daemon
# or, if you were foreground-running it during bringup: ctrl-C that shell.
```

## 3. Pull the new code

```bash
cd /path/to/stacks-bench-github
git fetch origin
git checkout main && git pull        # or the specific cutover tag/commit
```

## 4. Rebuild + redeploy the containers (handler, smee, migrate)

This rebuilds all three container images and runs the one-shot `migrate`,
which applies any new schema migrations **and** re-runs `apply_roles` —
the step that revokes `sbgh_handler`'s legacy `jobs` access. `handler`
and `smee` `depends_on: migrate (service_completed_successfully)`, so the
**new** inbox-only handler only starts *after* the grant change lands —
the old handler is already gone by then.

```bash
# Bring the containerized services down first so the OLD handler can't
# race the grant revoke (it would log `permission denied for table jobs`).
docker compose -f docker/docker-compose.yml down

# Rebuild images + run migrate + start the new handler & smee.
docker compose -f docker/docker-compose.yml up -d --build
```

Confirm the migrate one-shot succeeded and the grants flipped:

```bash
# migrate must show "migrate complete" then exit 0.
docker compose -f docker/docker-compose.yml logs migrate

# The handler should now have NO privileges on `jobs` (empty result) and
# INSERT on github_webhook.
psql "$DATABASE_URL" -c "
  SELECT table_name, privilege_type
  FROM information_schema.role_table_grants
  WHERE grantee = 'sbgh_handler'
  ORDER BY table_name, privilege_type;
"
```

Expected: rows for `github_webhook` (`INSERT`), and **no rows** for
`jobs`.

## 5. Clean accumulated shadow state (one-off)

Slices 5–10 ran the processor in shadow mode, so `job` / `github_webhook`
already hold pre-cutover rows. Wipe them so post-cutover behaviour starts
from a known-empty state. Run **once**, during the quiet window, after
migrate and **before** starting the new daemon.

```bash
psql "$DATABASE_URL" -f scripts/pre-cutover-cleanup.sql
```

This `TRUNCATE`s `job CASCADE` (and its `job_event` / `job_metric` /
`job_result` / subject-link dependents) and `github_webhook CASCADE`. It
**preserves** all Phase 1 state — `allowed_installer`,
`github_installation*`, `github_repo*`, `*_repo_policy`, `trigger_policy`,
`github_user*`, `github_pull_request`. The legacy `jobs` table is left
intact (it's dropped later, in slice 12, after a soak).

## 6. Rebuild + restart the host daemon (processor + v2 runner)

The daemon stays on the host (it needs LVM + libvirt + the golden
image). Rebuild it from the new code and reinstall the systemd unit. It
boots reading `[jobs].source = "v2"` from
`/etc/sbgh/daemon/config.toml`.

```bash
# Confirm the live config selects v2 (env SBGH_JOBS_SOURCE overrides it).
grep -A2 '^\[jobs\]' /etc/sbgh/daemon/config.toml

# Rebuild the binary and (re)install + restart the service. The script is
# idempotent and restarts the unit automatically.
just build
sudo ./scripts/install-daemon.sh

journalctl -u sbgh-daemon -f
```

Boot log should show `daemon started`. If `[jobs].source` is
missing or invalid the daemon **hard-errors on startup** (no silent
fallback) — add the `[jobs]` block from
`config.example.daemon.toml` and restart.

## 7. Seed the v2 authorization model

This is the step the v1 instructions glossed over. v1 authorized
`/benchmark` from the handler's `[authorization].allowed_repositories`
config allowlist. v2 moved authorization into **DB-backed roles +
policies** that the daemon evaluates — none of which a v1 host has
populated. That's why the first post-cutover `/benchmark` is rejected with
`sender lacks trigger_pr_benchmark role on target repo`.

### GitHub App event subscriptions (GitHub-side prerequisite)

Before the DB seeding below matters, GitHub has to actually *deliver* the
events the v2 processor handles. v1 subscribed to **Issue comment** only;
that's still all `/benchmark` needs. The other handlers:

| Handler event | Subscription | Needed for |
| ---- | ---- | ---- |
| `issue_comment` | already on (v1) | `/benchmark` PR commands |
| `installation`, `installation_repositories` | **auto-delivered to every App** — not in the subscribe list | membership materialization (§7 step 3) |
| `push` | **add it** | `branch_push` baselines (e.g. develop) |
| `create` | **add it** | `tag_created` baselines (release tags) |
| `pull_request` | optional | pre-materializing PR rows ahead of `/benchmark` |

`push` / `create` / `pull_request` are all covered by the **Contents:
Read** + **Pull requests** permissions the App already holds, so ticking
them under *App settings → Permissions & events → Subscribe to events* is a
**subscription-only** change — the installation does **not** need to
re-accept anything (only *permission* changes require re-acceptance).

If you're only running on-demand `/benchmark`, skip this — nothing to
change. Add `push` + `create` only when you want auto-baselines (then also
add `policy trigger` rows, see the end of this section).

A `/benchmark` PR comment now runs this gauntlet, in order; **every** layer
must be satisfied:

| # | Check | Backing table | Seeded by |
| ---- | ---- | ---- | ---- |
| 1 | Installing account is allowlisted | `allowed_installer` | `installer allow` |
| 2 | Target repo lineage traces to an enabled supported root | `supported_repo_root` | `repo allow` |
| 3 | Installation + repo membership materialized | `github_installation` / `github_installation_repo` | the `installation` webhook — **not** the CLI |
| 4 | Sender holds the trigger role | `github_user_role` | `user grant` |
| 5 | Target repo opted in | `target_repo_policy` | `policy target allow` |
| 6 | Source (head) repo trusted | `source_repo_policy` | `policy source allow` |

All commands run through the migrate container (it holds the owner DSN);
`installer` / `repo` / `user` resolve logins + names → numeric ids via
GitHub's unauthenticated API (60/hr/IP), so no App credentials are needed:

```bash
C="docker compose -f docker/docker-compose.yml run --rm migrate"
```

### Worked example (from the rejection log)

`installation_id=134845175`, `base_repo_id=562059944`,
`sender_login=cylewitruk`.

```bash
# 1. Allowlist the account that installed the App (gates installation.created).
$C installer allow --login cylewitruk

# 2. Allow the CANONICAL root only — forks are accepted transparently.
$C repo allow --owner stacks-network --name stacks-core
```

**Forks are accepted via lineage — don't allow the fork itself.** When the
daemon processes your fork's `installation`/`installation_repositories`
event, it calls `/repos/<owner>/<fork>`, reads GitHub's `source` (the
network root), and stores it as the fork's `fork_root_github_repo_id`.
`is_supported_lineage` then matches the enabled root against *either* the
repo's own id *or* its `fork_root_github_repo_id` — so allowing
`stacks-network/stacks-core` transparently accepts every fork of it
(`cylewitruk/stacks-core` included). Two caveats:

- **It must be a real GitHub fork.** GitHub only reports `source`/`parent`
  for repos created via the fork button. A mirror-pushed independent repo
  has no source → `fork_root_github_repo_id` stays NULL → unsupported. In
  that case allow the fork itself: `repo allow --owner <you> --name <fork>`.
- **Allow the root BEFORE the membership webhook is (re)processed.** A fork
  added while the root was unallowed was recorded `ignored_unsupported_lineage`
  with no membership; the redelivery in step 3 re-fetches its lineage and
  flips it to supported.

After step 3, confirm the fork resolved to the enabled root:

```bash
psql "$DATABASE_URL" -c "
  SELECT r.owner||'/'||r.name           AS fork,
         root.owner||'/'||root.name     AS root,
         s.is_enabled                   AS root_enabled
  FROM github_repo r
  LEFT JOIN github_repo root      ON root.id = r.fork_root_github_repo_id
  LEFT JOIN supported_repo_root s ON s.github_repo_id = r.fork_root_github_repo_id
  WHERE r.id = 562059944;
"
```

**Step 3 is webhook-driven — there is no `import installation` command.**
The daemon writes `github_installation` + `github_installation_repo`
only while processing an `installation` / `installation_repositories`
event whose repo lineage is supported. If the App was installed before the
root was allowed, that event was recorded as `ignored_unsupported_lineage`
and left no membership. Re-trigger it now that §6's daemon is
running:

- App settings → **Advanced → Recent Deliveries** → pick an `installation`
  (or `installation_repositories`) delivery → **Redeliver**. (Or toggle a
  repo in the App's installation config, which fires
  `installation_repositories.added`.)

Verify membership materialized before continuing — this mirrors the exact
"active membership" predicate the processor enforces:

```bash
psql "$DATABASE_URL" -c "
  SELECT m.github_repo_id,
         (i.deleted_at IS NULL AND i.suspended_at IS NULL
          AND m.revoked_at IS NULL) AS active
  FROM github_installation i
  JOIN github_installation_repo m ON m.github_installation_id = i.id
  WHERE i.id = 134845175;
"
```

You need a row for `github_repo_id = 562059944` with `active = t`. Then:

```bash
# 4. Grant the trigger role install-wide (add `--repo 562059944` to scope it).
$C user grant --login cylewitruk --install 134845175 --role trigger-pr-benchmark

# 5. Target opt-in. FK-requires the membership row from step 3 — it errors
#    if membership isn't there yet.
$C policy target allow --install-id 134845175 --repo-id 562059944

# 6. Source trust. For a same-repo PR head==base, so reuse the same id;
#    for a fork PR use the HEAD repo's id instead.
$C policy source allow --install-id 134845175 --repo-id 562059944
```

Confirm the grants/policies landed:

```bash
$C user list --install 134845175
$C policy target list --install-id 134845175
$C policy source list --install-id 134845175
```

> **Auto-trigger baselines** (develop-push / release-tag) additionally need
> `policy trigger add` rows — e.g.
> `$C policy trigger add --install-id 134845175 --repo-id 562059944 --kind branch_push --match '{"kind":"branch_push","branch_name":"develop"}'`.
> Not required for on-demand `/benchmark`.

## 8. Validate on a live `/benchmark`

Post a fresh `/benchmark` (or **Redeliver** a recent Issue-comment from
the App's *Advanced → Recent Deliveries* page) and walk the new path:

```bash
# 1. Handler recorded the event in the inbox (inbox-only — NOT a job).
psql "$DATABASE_URL" -c "
  SELECT id, event_type, status, received_at
  FROM github_webhook
  ORDER BY received_at DESC LIMIT 5;
"

# 2. The daemon processor turned the matched comment into a v2 job.
psql "$DATABASE_URL" -c "
  SELECT id, job_kind, trigger_kind, status, git_commit_hash, created_at
  FROM job
  ORDER BY created_at DESC LIMIT 5;
"

# 3. The runner claimed it and posted a PR comment (look for the
#    comment_posted event + the github_comment_id it recorded).
psql "$DATABASE_URL" -c "
  SELECT job_id, event_kind, event_status, github_comment_id, occurred_at
  FROM job_event
  ORDER BY occurred_at DESC LIMIT 10;
"
```

Cross-check the logs:

- **handler**: logs the inbound POST; **no** PR comment is posted by the
  handler anymore (that moved to the daemon).
- **daemon**: `processor` classifies the inbox row and creates a
  `job`; the `runner` claims it, posts the *"⏳ queued…"* PR comment, and
  drives `building → running → collecting → done`.
- The PR itself gets the bot comment and the ✅/❌ summary.

Tag baselines: push a release tag that matches a `trigger_policy`
`tag_pattern`; expect a `baseline` job with `git_commit_hash` initially
`NULL`, resolved to a real SHA once the runner claims it.

If a `job` is created but no PR comment appears, check the daemon
holds **Issues: Read & write** and the installation accepted the
permission (same 401 troubleshooting as
[host-bringup.md §8](./host-bringup.md#8-troubleshooting)).

## 9. Rollback and patch-forward

The cutover is deliberately not cleanly reversible (handler is inbox-only;
its `jobs` grant is gone). Two recovery paths:

- **Runner-only escape hatch (partial).** Set `[jobs].source = "legacy"`
  (or `SBGH_JOBS_SOURCE=legacy`) and restart the daemon — the
  runner claims from `jobs` again. But the **new handler still won't
  write** legacy jobs, so no *new* legacy work is created. This only
  helps drain residual legacy rows; it is **not** a functional rollback.

- **Full rollback (revert the commit).** Per the "go for it" decision,
  the real undo is git:

  ```bash
  git checkout <pre-cutover-commit>
  docker compose -f docker/docker-compose.yml down
  docker compose -f docker/docker-compose.yml up -d --build   # re-runs migrate → RESTORES handler jobs grant
  just build && sudo ./scripts/install-daemon.sh         # old daemon, legacy runner
  ```

  Re-running `migrate` on the old CLI image re-grants `sbgh_handler` its
  `jobs` INSERT, so the old handler resumes dual-write. The `job` /
  `github_webhook` rows created during the brief v2 window are orphaned
  but harmless (and were already truncated at cutover).

  > Do **not** re-run `scripts/pre-cutover-cleanup.sql` on rollback — it
  > targets the v2 tables, not `jobs`.

## 10. Known stale docs (Phase-1 remnants)

Some comments/docs still describe the old handler→`jobs` flow and are
being migrated incrementally (tracked in the roadmap). They don't affect
the upgrade but will read as contradictory:

- `docker/docker-compose.yml` header comments still say handler/orch have
  `INSERT`/`UPDATE` on `jobs` (the live grants are inbox-only post-migrate).
- Parts of [architecture.md](./architecture.md) remain a Phase-1 snapshot
  (it has a top-of-file Phase-2 note; the data-flow sections are being
  updated slice by slice).
- [host-bringup.md](./host-bringup.md) §7's forensics query still reads
  the legacy `jobs` table; the v2 equivalents are the `job` / `job_result`
  queries in [§8](#8-validate-on-a-live-benchmark) above.
