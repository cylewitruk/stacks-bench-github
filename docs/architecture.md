# Architecture

## Overview

`stacks-bench-github` is a GitHub App that runs the [`stacks-bench`](https://github.com/cylewitruk/stacks-core/tree/feat/stacks-bench/stacks-bench) tool against pull requests, either automatically or in response to `/benchmark` slash-commands posted in PR comments.

The system is split into two Rust binaries plus a shared library, all in a single Cargo workspace:

```text
crates/
  sbgh-core/         shared library: config, db, github (auth/client/webhook/command), models, error
  sbgh-handler/      axum HTTP server that receives GitHub webhooks and enqueues jobs
  sbgh-orchestrator/ long-running worker that dequeues and executes jobs in libvirt VMs
```

A Postgres database (run locally via `docker/docker-compose.yml`) is the only persistent state, and the only IPC between the two services.

## Data flow

```text
PR comment "/benchmark"
        │
        ▼
GitHub  ──webhook──►  sbgh-handler  ──INSERT──►  Postgres (jobs)
                          │                          ▲
                          └──comment "queued #N"─────┤
                                                     │
                                              SELECT FOR UPDATE
                                              SKIP LOCKED
                                                     │
                                              sbgh-orchestrator
                                                     │
                                              virsh + VM
                                                     │
                          ┌──comment "running"───────┤
                          │                          │
                          └──comment "done + result"─┘
```

## Components

### `sbgh-handler`

| Concern | Where |
| ---- | ---- |
| HTTP server | [crates/sbgh-handler/src/main.rs](../crates/sbgh-handler/src/main.rs) |
| Webhook route | [crates/sbgh-handler/src/routes/webhook.rs](../crates/sbgh-handler/src/routes/webhook.rs) |
| Signature verify | [crates/sbgh-core/src/github/webhook.rs](../crates/sbgh-core/src/github/webhook.rs) |
| Command parser | [crates/sbgh-core/src/github/command.rs](../crates/sbgh-core/src/github/command.rs) |
| Queue insert | [crates/sbgh-core/src/db/jobs.rs](../crates/sbgh-core/src/db/jobs.rs) |

Per request:

1. Read raw body (signature is over bytes, not parsed JSON).
2. Verify `X-Hub-Signature-256` (HMAC-SHA256, constant-time compare).
3. Dispatch on `X-GitHub-Event`. Currently we act on `issue_comment` events.
4. Parse the first line of the comment as a `/benchmark` command (strictly anchored at start of line, alphanumeric args only).
5. Authorize the sender against the repo allowlist and association allowlist.
6. Insert a job and reply with the queue position. Store the new comment id on the job row so the orchestrator can edit it later.

### `sbgh-orchestrator`

| Concern | Where |
| ---- | ---- |
| Main loop | [crates/sbgh-orchestrator/src/runner.rs](../crates/sbgh-orchestrator/src/runner.rs) |
| Queue claim | [crates/sbgh-core/src/db/jobs.rs](../crates/sbgh-core/src/db/jobs.rs) |
| Comment updates | [crates/sbgh-orchestrator/src/progress.rs](../crates/sbgh-orchestrator/src/progress.rs) |
| libvirt driver | [crates/sbgh-orchestrator/src/libvirt/driver.rs](../crates/sbgh-orchestrator/src/libvirt/driver.rs) |

Single-threaded poll loop. We only run one benchmark at a time on the libvirt host, so `claim_next` uses `SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1` and the loop processes the job to completion before claiming the next.

## GitHub App authentication

Two layers of credential:

| Credential | Lifetime | Scope | Storage |
| ---- | ---- | ---- | ---- |
| App private key (PEM) | long-lived | the App | file on disk, mode `0600`, path in `SBGH_GH_PRIVATE_KEY_PATH` |
| App JWT (RS256) | ≤ 10 min | the App | in-memory, minted per installation-token mint |
| Installation access token | ~1 hour | one installation | in-memory cache, keyed by `installation_id` |
| Webhook secret | long-lived | inbound HMAC | `SBGH_GH_WEBHOOK_SECRET` (env var) |

The private key never lives in an env var — env vars get into process listings, log scrapers, and crash dumps. It's a file on disk, owned by the service user, with restrictive permissions. The webhook secret is fine in an env var because it isn't a signing key for outbound calls.

Installation tokens are cached in memory by [`InstallationTokenCache`](../crates/sbgh-core/src/github/auth.rs) and refreshed when less than 5 minutes remain. Both binaries share this cache implementation via `sbgh-core`.

## Local development

```bash
# Start Postgres
docker compose -f docker/docker-compose.yml up -d

# Run migrations (requires sqlx-cli: cargo install sqlx-cli --no-default-features --features postgres,rustls)
export DATABASE_URL=postgres://sbgh:sbgh@127.0.0.1:5432/sbgh
sqlx migrate run

# Configure env
cp .env.example .env
chmod 0600 .env
# edit .env, point SBGH_GH_PRIVATE_KEY_PATH at your local PEM

# Run
cargo run -p sbgh-handler
cargo run -p sbgh-orchestrator
```

For local webhook delivery from GitHub, use `smee.io` or `cloudflared tunnel` and point the App's webhook URL at the tunnel.

## Orchestrator: libvirt benchmark driver

For each job, the orchestrator runs a self-contained VM. The lifecycle is in [crates/sbgh-orchestrator/src/libvirt/driver.rs](../crates/sbgh-orchestrator/src/libvirt/driver.rs):

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

The VM-side script is at [crates/sbgh-orchestrator/src/libvirt/templates/sbgh-run.sh.tmpl](../crates/sbgh-orchestrator/src/libvirt/templates/sbgh-run.sh.tmpl). Phases it writes (host polls these):

| Phase | Meaning |
| ---- | ---- |
| `starting` | results virtio-fs mounted |
| `building` | `cargo build --release -p stacks-bench` running |
| `running` | `stacks-bench` executing against the chainstate |
| `collecting` | sync before shutdown |
| `done` | normal completion (cloud-init `power_state` then triggers poweroff) |
| `error` | a step failed; see `console.log` tail |

## Operator setup

This section is for the host that runs `sbgh-orchestrator`. The handler has none of these requirements; it can run anywhere with network access to GitHub and Postgres.

### Required host packages (Debian/Ubuntu)

```text
qemu-system-x86_64, libvirt-daemon-system, virtinst, cloud-image-utils
git, lvm2, util-linux, e2fsprogs
```

`cloud-image-utils` provides `cloud-localds`; `util-linux` provides `losetup`, `mount`, `umount`, `truncate`.

### User + sudoers

The orchestrator runs as a dedicated `sbgh` user. Privileged commands are invoked via `sudo -n -- <binary> <args>`, so each binary must be allowlisted by path:

```text
# /etc/sudoers.d/sbgh
sbgh ALL=(root) NOPASSWD: /usr/sbin/lvcreate, /usr/sbin/lvremove, /usr/sbin/lvs
sbgh ALL=(root) NOPASSWD: /usr/sbin/mkfs.ext4, /usr/sbin/losetup
sbgh ALL=(root) NOPASSWD: /usr/bin/mount, /usr/bin/umount, /usr/bin/chown
sbgh ALL=(root) NOPASSWD: /usr/bin/virsh
```

(Paths in the allowlist must match the values in `[paths]` in the config; the defaults above line up with stock Debian/Ubuntu.)

### LVM layout

The orchestrator does not create the thin-pool or the base chainstate LV — both are operator-managed and refreshed out-of-band.

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
docker compose -f docker/docker-compose.yml up -d

# Build
cargo build --release --workspace

# Env (substitute real values)
cp .env.example .env && chmod 0600 .env
$EDITOR .env   # set SBGH_GH_* secrets, DATABASE_URL

# Optional TOML overrides
sudo install -m 0644 config.example.toml /etc/sbgh/config.toml

# Run (handler self-applies migrations on boot)
target/release/sbgh-handler &
target/release/sbgh-orchestrator
```

## Open design questions

- **Webhook delivery while offline**: GitHub retries failed webhook deliveries, but if the handler is down for an extended period we lose commands. Could add a periodic reconciliation that polls open PRs for missed `/benchmark` comments.
- **Multiple installations / multi-tenant**: the current design handles N installations naturally (installation id is on the job row) but we haven't decided whether the allowlist should be per-installation.
- **Result format**: `BenchmarkOutcome.summary` is `serde_json::Value` for now; should become a typed struct once we know what `stacks-bench` emits.
- **VM teardown on orchestrator crash**: needs a startup sweep that destroys orphaned VMs from jobs left in `running` state.
