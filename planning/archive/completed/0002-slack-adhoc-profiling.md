# 0002: Slack ad-hoc profiling

- **id:** `0002-slack-adhoc-profiling`
- **status:** `shipped`
- **date:** 2026-06
- **iteration:** v5 (deployed; see [v4-to-v5-upgrade.md](../../../docs/v4-to-v5-upgrade.md))
- **source:** `docs/roadmap-v10.md`

Added **Slack** as a first-class way to trigger + read benchmarks for the
**ad-hoc, no-commit** case ("profile this tx/block from yesterday"). The code
under test is a constant (`[slack].default_repository`/`default_rev`); the
workload is the variable (`--txid`/`--block`/`--repetitions`), resolved from an
`@mention`. Live-verified end-to-end on the Hetzner host.

## What shipped

- **Trigger source** — a Socket Mode connector (`slack-morphism`, `hyper`
  feature) on an outbound WebSocket, no public endpoint. `app_mention` events →
  a deterministic workload parser (`--txid`/`--block` repeatable + mutually
  exclusive, `--repetitions`/`--warmup`/`--rev`). Authz (team + user allowlist)
  runs **before** parsing; rejections are ephemeral with no enqueue.
- **Ad-hoc enqueue** — a webhook-less `create_adhoc_job` + a `slack_adhoc`
  `trigger_kind`; `[slack].default_repository` resolved to `(install, repo)` FK
  ids at startup (fails fast). A job's bare rev resolves to a commit at claim
  time, so a Slack job passes the reporter's empty-commit guard.
- **Reporting surface** — `ProgressTarget::Slack`: a ⏳ reaction on the request
  at enqueue, a threaded **`plan`-block** result card (`slack-messaging`) at
  terminal with the metrics + an S3 `stacks-bench.db` download link (presence-
  gated via `signed_url_if_fetchable`), and a ⏳→✅/❌ reaction swap.
- **Tokens are env-only** (`SBGH_SLACK_APP_TOKEN`/`SBGH_SLACK_BOT_TOKEN`); the
  connector stays behind `[slack].enabled` (default false). App definition,
  setup, and upgrade docs: [slack-app-manifest.yaml](../../../docs/slack-app-manifest.yaml),
  [slack-setup.md](../../../docs/slack-setup.md), [v4-to-v5-upgrade.md](../../../docs/v4-to-v5-upgrade.md).

## Follow-ons

- The live `plan`-card **timeline** (post-early + `chat.update` through the run)
  is its own iteration — `0021-slack-live-timeline` (v6).
- Natural-language intent resolution — `0020-llm-intent-resolution` (shipped in
  v13 for Slack; PR comments deferred).

## Decisions

- Entry surface is the `@mention`, not a slash command (a slash command leaves
  no channel message to thread on; an LLM intent resolver plugs in behind the
  same `resolve_workload` seam).
