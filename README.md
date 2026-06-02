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
- [docs/roadmap-v3.md](docs/roadmap-v3.md) — the API-fronted-daemon refactor (complete).

## Development

```bash
just build   # build the workspace
just lint    # clippy + rustfmt check
just test    # run the suite (needs Docker — a shared Postgres is started for DB tests)
```
