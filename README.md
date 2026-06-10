# stacks-bench-github

A GitHub App that runs the [`stacks-bench`](https://github.com/cylewitruk/stacks-core/tree/feat/stacks-bench/stacks-bench)
tool against pull requests — automatically, or on a `/benchmark` slash-command
in a PR comment — and reports results back on the PR.

## Components

A single Cargo workspace (`crates/`):

| Crate | Role |
| ---- | ---- |
| `sbgh-daemon` | The trusted core: owns Postgres, serves the authenticated `/api`, runs the webhook **processor** and the libvirt **runner**, and holds the GitHub App key. Runs on the benchmark host. |
| `sbgh-handler` | Thin edge: verifies the webhook HMAC and forwards each delivery to the daemon's `/api`. No DB, no App key. Runs in a container. |
| `sbgh-cli` | Operator CLI — a pure `/api` client (cookie auth) for the installer/repo/policy/user allowlists plus read commands (`jobs list`, `webhook tail`, …). |
| `sbgh-api` | Shared wire DTOs + a typed `reqwest` client used by the daemon (server) and both clients. |
| `sbgh-core` | Shared library: config, db, GitHub auth/client, models. |
| `sbgh-smee` | smee.io → handler webhook forwarder (local/dev delivery). |

Postgres is the only persistent state, and the **daemon is its sole client**;
the handler and CLI reach the daemon over `/api`.

## Docs

- [docs/architecture.md](docs/architecture.md) — system design + security model.
- [docs/daemon-api.md](docs/daemon-api.md) — the `/api` surface, auth, and topology.
- [docs/host-bringup.md](docs/host-bringup.md) — provision a benchmark host from scratch.
- [docs/v2-to-v3-upgrade.md](docs/v2-to-v3-upgrade.md) — upgrade an existing v2 deployment to v3.
- [docs/v3-to-v4-upgrade.md](docs/v3-to-v4-upgrade.md) — upgrade v3 → v4 (the artifact store; opt-in S3).
- [docs/v4-to-v5-upgrade.md](docs/v4-to-v5-upgrade.md) — upgrade v4 → v5 (Slack ad-hoc profiling; opt-in).
- [docs/slack-setup.md](docs/slack-setup.md) — register + configure the `@sbgh` Slack bot.
- [planning/index.md](planning/index.md) — **roadmap / backlog registry** (every
  item: shipped, planned, parked); guide + detail in [planning/](planning/README.md).
  The old `docs/roadmap-vN.md` docs were migrated here (shipped work →
  `planning/archive/completed/`).

## Common commands

The operator CLI must run as the daemon user to read the admin cookie. The
snippets below assume this alias:

```bash
alias sbgh='sudo -u sbgh sbgh-cli'   # runs as the daemon user (reads the 0600 cookie)
```

`--on owner/repo` resolves the install-id + repo-id server-side; raw
`--install-id`/`--repo-id` stay available as the escape hatch.

```bash
# 1. Onboard an account, then install the App on its repo via GitHub.
sbgh installer allow --login <account>
sbgh repo allow --owner stacks-network --name stacks-core   # canonical root; forks via lineage

# 2. Authorize PR benchmarking for an installed repo.
sbgh policy target allow --on <owner>/<repo>
sbgh policy source allow --on <owner>/<repo>                # internal PRs: source == target
sbgh user grant --login <user> --on <owner>/<repo> --role trigger-pr-benchmark
#   → comment `/benchmark` on a PR.

# 3. Automatic baselines (headless jobs; results land in the DB).
sbgh policy trigger add --on <owner>/<repo> --branch-push develop
sbgh policy trigger add --on <owner>/<repo> --tag-created '^v\d+\.\d+\.\d+$'
```

Roles: `admin` | `trigger-pr-benchmark` | `view-results`. Cross-account source
trust (a fork into a different org's install) uses raw `--install-id`/`--repo-id`.

| Read command | Shows |
| ---- | ---- |
| `sbgh status` | API reachability + cookie scope |
| `sbgh installation list` | installed accounts + install-ids |
| `sbgh policy trigger list --install-id <id>` | configured auto-triggers |
| `sbgh jobs list` | benchmark jobs + status |
| `sbgh webhook tail` | recent webhook inbox rows |

## Development

```bash
just build   # build the workspace
just lint    # clippy + rustfmt check
just test    # run the suite (needs Docker — a shared Postgres is started for DB tests)
```
