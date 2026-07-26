# Architecture

## Overview

`stacks-bench-github` is a GitHub App that runs the [`stacks-bench`](https://github.com/cylewitruk/stacks-core/tree/feat/stacks-bench/stacks-bench) tool against pull requests, either automatically or in response to `/benchmark` slash-commands posted in PR comments.

The system is one Cargo workspace with eleven crates and four binaries:

```text
crates/
  sbgh-api/          wire DTOs and typed daemon API client
  sbgh-core/         dependency-light domain policy, ports, configuration, and models
  sbgh-driver/       backend-neutral execution contracts
  sbgh-github/       GitHub App authentication and Octocrab adapter
  sbgh-libvirt/      concrete libvirt execution adapter
  sbgh-postgres/     SQLx stores, migrations, row mappings, and admin queries
  sbgh-worker/       in-process execution orchestration and recipes
  sbgh-handler/      library + HTTP binary for webhook verification/forwarding
  sbgh-cli/          operator API-client binary
  sbgh-daemon/       library + host binary for orchestration and inline execution
  sbgh-smee/         local-development smee.io forwarding binary
```

A Postgres database (run locally via `docker/docker-compose.yml`) is the only persistent state, and the **daemon is its sole client**. The handler and `sbgh-cli` never touch Postgres — they reach the daemon over the authenticated `/api` (see [daemon-api.md](./daemon-api.md)).
Concrete persistence and GitHub integrations live in `sbgh-postgres` and
`sbgh-github`, respectively.

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
| HTTP server | [crates/sbgh-handler/src/lib.rs](../crates/sbgh-handler/src/lib.rs) |
| Webhook route | [crates/sbgh-handler/src/routes/webhook.rs](../crates/sbgh-handler/src/routes/webhook.rs) |
| Signature verify | [crates/sbgh-handler/src/signature.rs](../crates/sbgh-handler/src/signature.rs) |
| `/api` client | [crates/sbgh-api/src/client.rs](../crates/sbgh-api/src/client.rs) (`submit_webhook`) |

For each verify-and-forward request:

1. Read raw body (signature is over bytes, not parsed JSON).
2. Verify `X-Hub-Signature-256` (HMAC-SHA256, constant-time compare).
3. Short-circuit `ping` → `pong` (no forward).
4. Forward the raw body + `X-GitHub-Event` / `X-GitHub-Delivery` to the daemon's `POST /api/webhooks` (with the ingest token) and map its result back to GitHub (2xx on success; 502 if the daemon is unreachable so GitHub redelivers). That's it — **no** payload parse, event-type filtering, authorization, job creation, or DB access. The daemon owns the event allowlist, the inbox write, authorization, and job creation.

### `sbgh-daemon`

| Concern | Where |
| ---- | ---- |
| Main loop | [crates/sbgh-daemon/src/runner.rs](../crates/sbgh-daemon/src/runner.rs) |
| Queue contract | [crates/sbgh-core/src/db/jobs.rs](../crates/sbgh-core/src/db/jobs.rs) |
| PostgreSQL queue implementation | [crates/sbgh-postgres/src/stores/jobs.rs](../crates/sbgh-postgres/src/stores/jobs.rs) |
| Worker events | [crates/sbgh-driver/src/events.rs](../crates/sbgh-driver/src/events.rs) |
| Report surfaces | [crates/sbgh-daemon/src/report.rs](../crates/sbgh-daemon/src/report.rs) |
| libvirt driver | [crates/sbgh-libvirt/src/libvirt/driver.rs](../crates/sbgh-libvirt/src/libvirt/driver.rs) |

The coordinator claims serially with `SELECT ... FOR UPDATE SKIP LOCKED LIMIT
1`, then executes up to `[runner].max_concurrent_jobs` jobs concurrently.
Configured CPU sets give each execution slot stable placement.

## GitHub App authentication

Two layers of credential:

| Credential | Lifetime | Scope | Storage |
| ---- | ---- | ---- | ---- |
| App private key (PEM) | long-lived | the App | file on disk, mode `0600`, path in `SBGH_GH_PRIVATE_KEY_PATH` |
| App JWT (RS256) | ≤ 10 min | the App | in-memory, minted per installation-token mint |
| Installation access token | ~1 hour | one installation | in-memory cache, keyed by `installation_id` |
| Webhook secret | long-lived | inbound HMAC | the handler's `SBGH_WEBHOOK_SECRET` (env var) |

The private key never lives in an env var — env vars get into process listings, log scrapers, and crash dumps. It's a file on disk, owned by the service user, with restrictive permissions. The webhook secret is fine in an env var because it isn't a signing key for outbound calls.

Installation tokens are minted and cached in memory by
[`InstallationTokenCache`](../crates/sbgh-github/src/auth.rs), refreshed when
less than 5 minutes remain. Only the **daemon** uses it: the handler never
receives the App key.

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

# Build, then run the daemon. It applies migrations at startup, serves /api,
# and runs the processor + runner.
just build
target/debug/sbgh-daemon

# The handler runs in Docker (`docker compose up -d handler`), or as a host
# binary with its OWN env (SBGH_API_URL → the daemon, SBGH_WEBHOOK_SECRET,
# SBGH_API_INGEST_TOKEN). `sbgh-cli` is a pure /api client (reads the cookie).
```

For local webhook delivery from GitHub, use `smee.io` or `cloudflared tunnel` and point the App's webhook URL at the tunnel.

## Daemon: libvirt benchmark driver

For each job, the in-process worker runs a self-contained VM. The lifecycle is
in [crates/sbgh-libvirt/src/libvirt/driver.rs](../crates/sbgh-libvirt/src/libvirt/driver.rs):

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

The VM-side scripts are
[sbgh-build.sh.tmpl](../crates/sbgh-libvirt/src/libvirt/templates/sbgh-build.sh.tmpl)
(build phase) and
[sbgh-bench.sh.tmpl](../crates/sbgh-libvirt/src/libvirt/templates/sbgh-bench.sh.tmpl)
(benchmark phase). Phases they write (host polls these):

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

## Fleet boundary

Execution remains in-process after v24.1, but its dependency direction is now
compiler-enforced through three Cargo boundaries:
`sbgh-driver` for the internal driver API, `sbgh-libvirt` for the concrete
backend, and an in-process `sbgh-worker` library for dispatch and recipes.
v25 adds the worker protocol and separate process; worker registration,
networking, leases, durable remote events, and remote artifacts are not part of
the current deployment.

Workers emit task-neutral events and outcomes rather than performing external
reporting. `sbgh-daemon` remains the sole DB client and GitHub/Slack side-effect
owner, including reporting credentials, rendering, debounce, rate limiting,
retries, and reporting-session state. A worker may receive a short-lived,
lease-scoped GitHub token for repository access, but never Slack credentials or
a GitHub/Slack reporting client.

The movable closure starts at the owned dispatcher and concrete libvirt
backend:

- request/task dispatch, recipes, driver contracts, and worker events;
- the worker-side artifact port and binary cache;
- the production libvirt modules and their host-side helpers.

Cargo enforces the source boundary, while
[`check-package-dag.py`](../scripts/check-package-dag.py) verifies the allowed
workspace DAG and rejects forbidden transitive execution dependencies. The
daemon owns the full artifact store and hands the worker only the narrow
staging/read port. Its direct `sbgh-driver`, `sbgh-worker`, and
`sbgh-libvirt` dependencies are transitional in-process composition edges;
v25 removes them when protocol DTOs replace internal execution types.
