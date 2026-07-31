# Slack setup

Slack is an optional task-submission and progress surface. The daemon
connects through Socket Mode, so the Slack App needs no public request URL.
The checked-in [manifest](slack-app-manifest.yaml) pins the required events and
least-privilege bot scopes.

## Create the App

1. Open <https://api.slack.com/apps>.
2. Select **Create New App**, then **From a manifest**.
3. Choose the workspace and paste
   [slack-app-manifest.yaml](slack-app-manifest.yaml).
4. Create the App and select **Install to Workspace**.

Reinstall an existing App after changing the manifest so new scopes take
effect.

## Create tokens

Both tokens are environment-only secrets. A TOML token field is rejected.

| Token | Location | Required scope | Environment variable |
| --- | --- | --- | --- |
| Bot token (`xoxb-…`) | OAuth & Permissions | Manifest bot scopes | `SBGH_SLACK_BOT_TOKEN` |
| App token (`xapp-…`) | Basic Information, App-Level Tokens | `connections:write` | `SBGH_SLACK_APP_TOKEN` |

The App token opens the Socket Mode connection and is not represented in the
manifest.

Add both values to `/etc/sbgh/daemon/secrets.env`, owned by `sbgh` and mode
`0600`.

## Configure authorization

Slack requests require both an allowed workspace and an allowed user. Obtain:

- the workspace team ID (`T…`) from workspace settings or a Slack deep link;
- each user ID (`U…`) from **Profile -> Copy member ID**.

Enable the daemon connector:

```toml
[slack]
enabled = true
default_repository = "stacks-network/stacks-core"
default_rev = "develop"
allowed_team_ids = ["T0123ABCD"]
allowed_user_ids = ["U0123ABCD"]
# Optional: omit to inherit allowed_user_ids; use [] to disable validation.
block_validation_user_ids = ["U0123ABCD"]
```

The default repository must already be known to the daemon. Startup fails
closed if required IDs, repository state, or tokens are missing.
Block-validation selector limits and timeout come from
`[tasks.block_validation]`; Slack input cannot choose workers, shards,
concurrency, or timeouts. Users may naturally request recent coverage (with an
optional count), full coverage, or an explicit range when the corresponding
daemon policy permits it. A non-empty effective validation allowlist requires
that complete task policy at startup. Set `block_validation_user_ids = []` for
a benchmark-only Slack surface.

Slack task creation is natural-language only, so enabling Slack also requires
the intent resolver:

```toml
[llm]
enabled = true
provider = "openai"
model = "gpt-5-mini"
input_max_chars = 1000
timeout_secs = 15
per_user_rate_limit_per_minute = 5
```

Set `SBGH_OPENAI_API_KEY` in the daemon environment. Conversational task
creation requires the intent provider; CLI-style flags are rejected. All model
output is schema-validated and revalidated against daemon policy before
submission.

## Verify

1. Invite the bot to a channel with `/invite @BenchBot`.
2. Restart `sbgh-daemon`.
3. Confirm `slack: socket mode connected` in the daemon journal.
4. From an allowed user, mention the bot:

   ```text
   @BenchBot benchmark block <height> on <branch-or-commit>
   @BenchBot validate the latest 500k blocks on <branch-or-commit>
   @BenchBot run block validation on commit <branch-or-commit>
   ```

The bot adds an acknowledgement reaction and creates one normal threaded
message. Queue, phase, bounded progress, and terminal state update that same
timestamp. The complete message is rendered from current durable state; the
daemon never parses or incrementally patches its previous text.

The message carries opaque Slack metadata containing request identity and a
monotonic snapshot version. It contains no repository, user input, or secret.
If the timestamp was not persisted, reconciliation searches only the
originating thread and adopts exactly one matching message from the configured
bot. Lookup failure or multiple matches fail closed and retry without posting a
duplicate.

Denied or malformed requests receive an ephemeral reply and create no task.
Benchmark and block-validation creation can be authorized independently. A
caller with neither entitlement is rejected before an LLM call; after
resolution, the selected task's own allowlist is authoritative.
