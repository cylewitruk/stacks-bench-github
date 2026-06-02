# Architecture

> **Status note.** This document predates roadmap-v3; some lower sections
> still describe older internals. The current top-level shape (roadmap-v3
> Phases 4–6): the **handler** is a thin verify-and-forward shim — it checks
> the webhook HMAC and forwards the raw delivery to the **daemon's** `/api`
> (`POST /api/webhooks`, ingest token). It holds **no** DB access and **no**
> App key. The daemon owns Postgres outright: it filters event types, writes
> the `github_webhook` inbox, then runs the **processor** (classify /
> authorize → create `job` rows) and the **runner** (execute in libvirt VMs).
> `sbgh-cli` is a pure `/api` client (cookie auth). The legacy `jobs`-table
> path was removed in Phase 1. Where a section below still says "handler
> records a webhook to Postgres", read "handler forwards to `/api`; the
> daemon records it."

## Overview

`stacks-bench-github` is a GitHub App that runs the [`stacks-bench`](https://github.com/cylewitruk/stacks-core/tree/feat/stacks-bench/stacks-bench) tool against pull requests, either automatically or in response to `/benchmark` slash-commands posted in PR comments.

The system is split into two Rust binaries plus a shared library, all in a single Cargo workspace:

```text
crates/
  sbgh-core/         shared library: config, db, github (auth/client/webhook/command), models, error
  sbgh-handler/      axum HTTP server: verifies the webhook HMAC and forwards each delivery to the daemon's /api (no DB)
  sbgh-daemon/       long-running worker: owns Postgres — serves /api, processes the inbox into jobs, then executes them in libvirt VMs
```

A Postgres database (run locally via `docker/docker-compose.yml`) is the only persistent state, and the **daemon is its sole client**. The handler and `sbgh-cli` never touch Postgres — they reach the daemon over the authenticated `/api` (see [daemon-api.md](./daemon-api.md)).

## Data flow

```text
PR comment "/benchmark"
        │
        ▼
GitHub ──webhook──► sbgh-handler ──HMAC verify──► daemon: POST /api/webhooks
                                                  (ingest token)
                                                     │
                                              daemon filters event type +
                                              writes github_webhook inbox
                                                     │
                                              processor: classify +
                                              authorize + INSERT job
                                                     │
                                              Postgres (job, queued)
                                                     │
                                              runner: claim (FOR UPDATE
                                              SKIP LOCKED) → run
                                                     │
                                              virsh + VM
                                                     │
                          ┌──comment "running"───────┤  (daemon posts/
                          │                          │   edits the PR comment)
                          └──comment "done + result"─┘
```

The `/api` server, the processor, and the runner all live in `sbgh-daemon`.

## Components

### `sbgh-handler`

| Concern | Where |
| ---- | ---- |
| HTTP server | [crates/sbgh-handler/src/main.rs](../crates/sbgh-handler/src/main.rs) |
| Webhook route | [crates/sbgh-handler/src/routes/webhook.rs](../crates/sbgh-handler/src/routes/webhook.rs) |
| Signature verify | [crates/sbgh-core/src/github/webhook.rs](../crates/sbgh-core/src/github/webhook.rs) |
| `/api` client | [crates/sbgh-api/src/client.rs](../crates/sbgh-api/src/client.rs) (`submit_webhook`) |

Per request (verify-and-forward since roadmap-v3 Phase 4):

1. Read raw body (signature is over bytes, not parsed JSON).
2. Verify `X-Hub-Signature-256` (HMAC-SHA256, constant-time compare).
3. Short-circuit `ping` → `pong` (no forward).
4. Forward the raw body + `X-GitHub-Event` / `X-GitHub-Delivery` to the daemon's `POST /api/webhooks` (with the ingest token) and map its result back to GitHub (2xx on success; 502 if the daemon is unreachable so GitHub redelivers). That's it — **no** payload parse, event-type filtering, authorization, job creation, or DB access. The daemon owns the event allowlist, the inbox write, authorization, and job creation.

### `sbgh-daemon`

| Concern | Where |
| ---- | ---- |
| Main loop | [crates/sbgh-daemon/src/runner.rs](../crates/sbgh-daemon/src/runner.rs) |
| Queue claim | [crates/sbgh-core/src/db/jobs.rs](../crates/sbgh-core/src/db/jobs.rs) |
| Comment updates | [crates/sbgh-daemon/src/progress.rs](../crates/sbgh-daemon/src/progress.rs) |
| libvirt driver | [crates/sbgh-daemon/src/libvirt/driver.rs](../crates/sbgh-daemon/src/libvirt/driver.rs) |

Single-threaded poll loop. We only run one benchmark at a time on the libvirt host, so `claim_next` uses `SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1` and the loop processes the job to completion before claiming the next.

## GitHub App authentication

Two layers of credential:

| Credential | Lifetime | Scope | Storage |
| ---- | ---- | ---- | ---- |
| App private key (PEM) | long-lived | the App | file on disk, mode `0600`, path in `SBGH_GH_PRIVATE_KEY_PATH` |
| App JWT (RS256) | ≤ 10 min | the App | in-memory, minted per installation-token mint |
| Installation access token | ~1 hour | one installation | in-memory cache, keyed by `installation_id` |
| Webhook secret | long-lived | inbound HMAC | the handler's `SBGH_WEBHOOK_SECRET` (env var) |

The private key never lives in an env var — env vars get into process listings, log scrapers, and crash dumps. It's a file on disk, owned by the service user, with restrictive permissions. The webhook secret is fine in an env var because it isn't a signing key for outbound calls.

Installation tokens are minted + cached in memory by [`InstallationTokenCache`](../crates/sbgh-core/src/github/auth.rs), refreshed when less than 5 minutes remain. Only the **daemon** uses it — it holds the App key; the handler does not. (The implementation lives in `sbgh-core`, but the handler never instantiates it.)

## Local development

```bash
# Start Postgres
docker compose -f docker/docker-compose.yml up -d postgres

# Daemon env: DATABASE_URL (owner DSN), the SBGH_GH_* App secrets,
# SBGH_API_INGEST_TOKEN, and the LVM/VM bits. See config.example.daemon.toml
# for the full surface.
cp .env.example .env
chmod 0600 .env
# edit .env, point SBGH_GH_PRIVATE_KEY_PATH at your local PEM

# Run the daemon — it applies migrations at startup (no sqlx-cli needed),
# serves /api, and runs the processor + runner.
cargo run -p sbgh-daemon

# The handler runs in Docker (`docker compose up -d handler`), or as a host
# binary with its OWN env (SBGH_API_URL → the daemon, SBGH_WEBHOOK_SECRET,
# SBGH_API_INGEST_TOKEN). `sbgh-cli` is a pure /api client (reads the cookie).
```

For local webhook delivery from GitHub, use `smee.io` or `cloudflared tunnel` and point the App's webhook URL at the tunnel.

## Daemon: libvirt benchmark driver

For each job, the daemon runs a self-contained VM. The lifecycle is in [crates/sbgh-daemon/src/libvirt/driver.rs](../crates/sbgh-daemon/src/libvirt/driver.rs):

1. **Provision** (host-side):
    - qcow2 boot overlay backed by the configured golden image.
    - Raw ext4 source disk, populated by `git clone --reference <mirror>` then `git checkout <sha>` from a bare host mirror.
    - LVM-thin snapshot of the newest `<chainstate_base_prefix>*` LV.
    - Host tmpfs at `paths.results_tmpfs_root/<job-id>`, shared into the VM over virtio-fs.
    - cloud-init NoCloud ISO with the per-job startup script.
2. **Define + start**: render the libvirt domain XML programmatically with `quick-xml`, `virsh define`, `virsh start`.
3. **Poll loop** (1s cadence): emit phase changes to the PR comment when `/results/.phase` changes; finish when phase=`done`, phase=`error`, domain transitions to `shut off`, or `vm.job_timeout_secs` elapses.
4. **Forensics** (before teardown): capture last phase value, tail of `console.log` (64 KiB), archive `run.sqlite` to `paths.results_archive_dir/<job-id>.sqlite`.
5. **Teardown**: `virsh destroy + undefine`, unmount tmpfs, `lvremove` the chainstate snapshot, delete source + boot files, prune the per-job ref from the mirror, `rm -rf` the per-job dir.

Failure modes are surfaced as `BenchmarkOutcome { status: Failed(_), summary }` — the summary still carries all forensics. The runner records both on the job row (`status=failed`, `error=...`, `result=summary`).

### In-VM startup script

The VM-side scripts are [sbgh-build.sh.tmpl](../crates/sbgh-daemon/src/libvirt/templates/sbgh-build.sh.tmpl) (build phase) and [sbgh-bench.sh.tmpl](../crates/sbgh-daemon/src/libvirt/templates/sbgh-bench.sh.tmpl) (benchmark phase). Phases they write (host polls these):

| Phase | Meaning |
| ---- | ---- |
| `starting` | results virtio-fs mounted |
| `building` | `cargo build --release -p stacks-bench` running |
| `running` | `stacks-bench` executing against the chainstate |
| `collecting` | sync before shutdown |
| `done` | normal completion (cloud-init `power_state` then triggers poweroff) |
| `error` | a step failed; see `console.log` tail |

## Operator setup

This section is for the host that runs `sbgh-daemon`. The handler has none of these requirements; it runs in a container and only needs to receive webhook deliveries (via smee) and reach the daemon's `/api`.

### Required host packages (Debian/Ubuntu)

```text
qemu-system-x86_64, libvirt-daemon-system, virtinst, cloud-image-utils
git, lvm2, util-linux, e2fsprogs
```

`cloud-image-utils` provides `cloud-localds`; `util-linux` provides `losetup`, `mount`, `umount`, `truncate`.

### User + sudoers

The daemon runs as a dedicated `sbgh` user. Privileged commands are invoked via `sudo -n -- <binary> <args>`, so each binary must be allowlisted by path:

```text
# /etc/sudoers.d/sbgh
sbgh ALL=(root) NOPASSWD: /usr/sbin/lvcreate, /usr/sbin/lvremove, /usr/sbin/lvs
sbgh ALL=(root) NOPASSWD: /usr/sbin/mkfs.ext4, /usr/sbin/losetup
sbgh ALL=(root) NOPASSWD: /usr/bin/mount, /usr/bin/umount, /usr/bin/chown
sbgh ALL=(root) NOPASSWD: /usr/bin/virsh
```

(Paths in the allowlist must match the values in `[paths]` in the config; the defaults above line up with stock Debian/Ubuntu.)

### LVM layout

The daemon does not create the thin-pool or the base chainstate LV — both are operator-managed and refreshed out-of-band.

```text
# one-time
pvcreate /dev/<disk>
vgcreate sbgh-vg /dev/<disk>
lvcreate -L 2T --thinpool thinpool sbgh-vg

# refreshed nightly by the out-of-band chainstate loader, named with the date
# so `chainstate_base_prefix` discovery picks the newest one lexicographically
lvcreate -V 500G --thin --name mainnet-2026-05-21 sbgh-vg/thinpool
mkfs.xfs /dev/sbgh-vg/mainnet-2026-05-21
# (populate it with the chainstate snapshot, then deactivate)
```

### Local quickstart (sans GitHub App)

```bash
# Postgres
docker compose -f docker/docker-compose.yml up -d postgres

# Build
cargo build --release --workspace

# Daemon env (the daemon is the only host binary). Set DATABASE_URL, the
# SBGH_GH_* App secrets if you have an App, and SBGH_API_INGEST_TOKEN. See
# config.example.daemon.toml for the full config surface.
cp .env.example .env && chmod 0600 .env
$EDITOR .env

# Run the daemon — it applies migrations at startup, serves /api, and runs
# the processor + runner. (The handler runs in Docker via docker-compose;
# `sbgh-cli` is a pure /api client that reads the daemon's cookie.)
target/release/sbgh-daemon
```

## Open design questions

- **Webhook delivery while offline**: GitHub retries failed webhook deliveries, but if the handler is down for an extended period we lose commands. Could add a periodic reconciliation that polls open PRs for missed `/benchmark` comments.
- **Multiple installations / multi-tenant**: the current design handles N installations naturally (installation id is on the job row) but we haven't decided whether the allowlist should be per-installation.
- **Result format**: `BenchmarkOutcome.summary` is `serde_json::Value` for now; should become a typed struct once we know what `stacks-bench` emits.
- **VM teardown on daemon crash**: needs a startup sweep that destroys orphaned VMs from jobs left in `running` state.
