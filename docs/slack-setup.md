# Slack ad-hoc profiling setup

How to stand up the `@BenchBot` benchmark bot (item 0002, iteration v5). The app
definition lives in [`slack-app-manifest.yaml`](slack-app-manifest.yaml); the
daemon-side config is the `[slack]` block in `config.example.daemon.toml`.

## 1. Create the app

1. <https://api.slack.com/apps> → **Create New App** → **From a manifest**.
2. Pick the workspace, paste [`slack-app-manifest.yaml`](slack-app-manifest.yaml),
   create.
3. **Install to Workspace** (grants the bot token).

## 2. Collect the two tokens

Both are **env-only** secrets — never put them in the TOML (a key there is a
hard error).

| Token | Where | Scope | Env var |
| ---- | ---- | ---- | ---- |
| Bot (`xoxb-…`) | OAuth & Permissions → Bot User OAuth Token | from manifest | `SBGH_SLACK_BOT_TOKEN` |
| App-level (`xapp-…`) | Basic Information → App-Level Tokens → Generate | `connections:write` | `SBGH_SLACK_APP_TOKEN` |

The App-Level Token powers Socket Mode and is **not** in the manifest — generate
it by hand with the `connections:write` scope.

## 3. Find the allowlist ids

Authz requires BOTH the workspace and the sender to be allowlisted.

- **Team id** (`T…`): workspace **Settings & administration → Workspace
  settings**, or any deep link.
- **User ids** (`U…`): a member's profile → **Copy member ID**.

## 4. Configure the daemon

```toml
[slack]
enabled            = true
default_repository = "stacks-network/stacks-core"  # the constant code under test
default_rev        = "develop"
allowed_team_ids   = ["T0123ABCD"]
allowed_user_ids   = ["U0123ABCD"]
```

`default_repository` must already be known to the daemon (installed on it, or a
PR opened from it) — startup fails fast otherwise.

## 5. Invite + smoke test

> The bot is addressed by its **display name** (`BenchBot` in the manifest, but
> workspace-renamable) — examples below use `@BenchBot`; substitute whatever you
> named it. Nothing in the daemon hardcodes the name: Slack delivers mentions by
> the bot's user id, and the daemon strips the leading `<@id>` token regardless.

1. Invite the bot to the channel: `/invite @BenchBot` (it can only see mentions and
   reply in channels it is a member of).
2. Restart the daemon — the log shows `slack: socket mode connected`.
3. From an allowlisted user: `@BenchBot bench --block <n>`.
4. Expect: ⏳ on your message → a threaded result with the metrics → ⏳ swapped
   for ✅ (or ❌ on failure). A denied/garbled request gets an ephemeral
   (invoker-only) reply and no reaction.
