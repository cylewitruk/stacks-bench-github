# v4 → v5 upgrade runbook

Cutting a **running** v4 deployment over to v5 (**Slack ad-hoc profiling**, item
`0002-slack-adhoc-profiling`). v5 adds Slack as a new **trigger source** +
**reporting surface**: an `@sbgh bench --block/--txid …` mention enqueues an
ad-hoc benchmark whose code-under-test is a constant (`[slack].default_rev`) and
whose workload is the variable, with results posted back in the request's
thread.

For a **fresh** install, follow [host-bringup.md](./host-bringup.md). This doc
is only for upgrading a host already running v4 (see
[v3-to-v4-upgrade.md](./v3-to-v4-upgrade.md) if you're still on v3).

> **A light upgrade.** Slack is **opt-in** (`[slack].enabled = false` by
> default), so Part A is a trivial code bump that **preserves v4 behavior
> exactly** — the only schema change is one *additive* `trigger_kind` enum value
> that auto-applies and stays unused until you enable Slack. Part B (turn Slack
> on) is optional and the only place new config + secrets appear. The one thing
> to read before rolling back is the [enum caveat](#rollback).

## What actually changes

| Layer | v4 (deployed) | v5 (this upgrade) |
| ---- | ---- | ---- |
| Trigger sources | GitHub: PR `/benchmark`, `branch_push`, `tag_created` | **+ `slack_adhoc`** (an `@sbgh` mention), opt-in |
| Reporting surfaces | PR comment + commit Check Run | **+ a Slack thread** (result + ⏳→✅/❌ reaction) for Slack jobs |
| Config | `[artifacts]` (v4) | **+ a new optional `[slack]` section; default `enabled = false`** |
| Secrets | `SBGH_API_INGEST_TOKEN`, `SBGH_ARTIFACTS_S3_*` | `+ SBGH_SLACK_APP_TOKEN` / `SBGH_SLACK_BOT_TOKEN` **only if** you enable Slack (env-only, like the others) |
| Database | — | **one additive migration**: `ALTER TYPE trigger_kind ADD VALUE 'slack_adhoc'` — auto-applied at startup, idempotent, unused until Slack is on |
| Dependency | — | `+ slack-morphism` (Socket Mode client) — transparent; just rebuild |
| Host binary / units / containers | `sbgh-daemon`, `/etc/sbgh/daemon`, the compose stack | **unchanged** (no public endpoint — Socket Mode is an outbound WebSocket) |

**Behavior is preserved with Slack off.** A v5 daemon with no `[slack]` section
(or `enabled = false`) drives PR / baseline jobs exactly as v4 did and opens no
Slack connection. The new enum value exists in the type but no row uses it.

## Part A — deploy v5 (mandatory, Slack off)

No config change required. Pull, build, restart.

```bash
cd /path/to/stacks-bench-github
git fetch origin && git checkout main && git pull     # or the v5 tag/commit
just build                                            # → target/release/sbgh-daemon

# Reinstall the binary + unit and restart the daemon (install-daemon.sh is
# idempotent — it overwrites the binary and restarts the service).
sudo ./scripts/install-daemon.sh
journalctl -u sbgh-daemon -f
#   expect a normal startup: "migrations applied" (the one slack_adhoc enum add),
#   "api listening", and NO "slack: socket mode connected" line (Slack is off).
#   Ctrl-C once it's serving.
```

The webhook/handler containers are unaffected by v5 — no rebuild needed unless
you're pulling other changes.

### Verify (Slack off)

Post `/benchmark` on a PR in an allowed repo (or push a watched branch) and let
a run complete — the PR comment / commit check render exactly as under v4. That
is the whole of the mandatory upgrade.

## Part B — enable Slack (optional)

The full procedure lives in **[slack-setup.md](./slack-setup.md)** (create the
app from [slack-app-manifest.yaml](./slack-app-manifest.yaml), collect the two
tokens, find the allowlist ids, invite the bot, smoke-test). In brief:

### B1. Create the app + tokens

From [slack-app-manifest.yaml](./slack-app-manifest.yaml): create the app, install
it to the workspace (→ the `xoxb-…` **bot token**), then generate an `xapp-…`
**app-level token** with `connections:write` by hand (Socket Mode; not part of
the manifest).

### B2. Daemon config — add `[slack]`

```bash
sudo -u sbgh $EDITOR /etc/sbgh/daemon/config.toml
# Add (see config.example.daemon.toml for the documented block):
#   [slack]
#   enabled            = true
#   default_repository = "stacks-network/stacks-core"  # the constant code under test
#   default_rev        = "develop"
#   allowed_team_ids   = ["T0123ABCD"]                  # non-empty
#   allowed_user_ids   = ["U0123ABCD"]                  # non-empty
# default_repository must already be known to the daemon (installed on it, or a
# PR opened from it) — a v5 daemon fails fast at startup otherwise.
```

### B3. Tokens — env-only, into the unit's `secrets.env`

```bash
sudo tee -a /etc/sbgh/daemon/secrets.env >/dev/null <<'EOF'
SBGH_SLACK_APP_TOKEN=xapp-...
SBGH_SLACK_BOT_TOKEN=xoxb-...
EOF
sudo chmod 0600 /etc/sbgh/daemon/secrets.env
sudo chown sbgh:sbgh /etc/sbgh/daemon/secrets.env

sudo systemctl restart sbgh-daemon
journalctl -u sbgh-daemon -f
#   expect: "slack: ad-hoc profiling enabled" + "slack: socket mode connected".
#   A bad/missing token or an unresolvable default_repository fails fast at
#   startup (Slack is validated before the socket opens).
```

A TOML `app_token` / `bot_token` key is a **hard startup error** (env-only, like
`[api].ingest_token` and `SBGH_ARTIFACTS_S3_*`).

### B4. Smoke-test

Invite the bot to a channel (`/invite @sbgh` — it only sees mentions and replies
where it's a member), then from an allowlisted user:

```text
@sbgh bench --block <n>
```

Expect: ⏳ on your message → a threaded reply with the metrics → ⏳ swapped for
✅ (or ❌ on failure). A denied / garbled request gets an **ephemeral**
(invoker-only) reply and no reaction. A Slack-side failure is logged and never
crashes the daemon — Slack is an optional surface.

## Rollback

- **Disable Slack (stay on v5):** set `[slack].enabled = false` (or remove the
  block) and `systemctl restart sbgh-daemon`. The socket closes; PR / baseline
  jobs continue unaffected. This is the preferred "back out Slack" path.
- **From v5 back to v4 (code):** revert the code and redeploy
  (`git checkout <v4-tag> && just build && sudo ./scripts/install-daemon.sh`).
  The `slack_adhoc` enum value **stays** in the type (Postgres has no
  `DROP VALUE`) — harmless, since v4 simply never produces it.

  > **⚠ Caveat — only if you enabled Slack and it created jobs.** A v4 binary's
  > `trigger_kind` enum has no `slack_adhoc` variant, so it **cannot decode** any
  > `slack_adhoc` **job row** and will error when a query reads one (claim, list,
  > load). If Slack was only ever off, there are no such rows and rollback is
  > clean. If it was used, either **don't roll back** (disable Slack on v5
  > instead), or first remove the ad-hoc rows — they're never baselines, so
  > deleting is safe, and the `job_event` / `job_metric` / `job_result` children
  > are `ON DELETE CASCADE`, so a single delete cleans them up:
  >
  > ```sql
  > -- Inspect first; delete only if you must roll the binary back to v4.
  > SELECT id, status FROM job WHERE trigger_kind = 'slack_adhoc';
  > DELETE FROM job WHERE trigger_kind = 'slack_adhoc';  -- children cascade
  > ```

No DB snapshot is required for this upgrade.
