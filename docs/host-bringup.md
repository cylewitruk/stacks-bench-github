# Host bringup

Step-by-step for going from a fresh Linux host with libvirt installed to a working `/benchmark` flow on a real PR. Assumes you've already read [architecture.md](./architecture.md).

## 0. Prerequisites

| Component | Version / notes |
| ---- | ---- |
| OS | Debian 12 or Ubuntu 24.04 (other distros work, paths may shift) |
| Kernel | KVM available (`grep -E 'vmx\|svm' /proc/cpuinfo`) |
| libvirt | 9.x or newer, daemon running |
| qemu | 8.x or newer (virtio-fs requires this) |
| LVM2 | with a thin-pool already created (see [architecture.md](./architecture.md#lvm-layout)) |
| Postgres | local Docker is fine (`docker compose -f docker/docker-compose.yml up -d`) |

Sanity check the host can run KVM domains at all:

```bash
sudo systemctl status libvirtd
virsh list --all
virsh net-list --all      # 'default' should be active
```

If `default` is missing or inactive:

```bash
sudo virsh net-start default
sudo virsh net-autostart default
```

Required packages (Debian/Ubuntu):

```bash
sudo apt update
sudo apt install -y \
  qemu-system-x86 qemu-utils libvirt-daemon-system virtinst virtiofsd \
  cloud-image-utils libguestfs-tools \
  git lvm2 util-linux e2fsprogs xfsprogs
```

`virtiofsd` is the userspace daemon libvirt spawns to back the virtio-fs
results share. It ships in its own apt package on Debian/Ubuntu; without
it `virsh start` aborts with "Unable to find a satisfying virtiofsd".

## 1. Build the golden Ubuntu 24 image

We start from the official Ubuntu 24.04 cloud image and bake in the toolchain `stacks-bench` will need at build time.

A helper lives at [scripts/build-golden-image.sh](../scripts/build-golden-image.sh). The first run takes ~5–10 minutes and ~3 GiB of disk.

```bash
sudo ./scripts/build-golden-image.sh /var/lib/libvirt/images/sbgh-golden-ubuntu24.qcow2
```

What it does:

1. Downloads `noble-server-cloudimg-amd64.img` from cloud-images.ubuntu.com.
2. Resizes to 32 GiB.
3. Uses `virt-customize` to:
   - install `qemu-guest-agent`, `xfsprogs`, build toolchain (`git build-essential pkg-config libssl-dev libclang-dev cmake clang zstd`)
   - install rustup → `/opt/{rustup,cargo}` system-wide, with symlinks in `/usr/local/bin`
   - enable `qemu-guest-agent` to start on boot
   - truncate `/etc/machine-id` and `/var/lib/dbus/machine-id` so each VM gets a unique id on first boot
4. Drops the image at the destination path you passed.

Verify the image is bootable + has the toolchain:

```bash
sudo virt-customize -a /var/lib/libvirt/images/sbgh-golden-ubuntu24.qcow2 \
  --run-command 'cargo --version && rustc --version'
```

You should see `cargo 1.x` and `rustc 1.x`.

## 2. Configure the host LVM layout

The orchestrator needs three things from LVM, all of which usually live inside the **existing OS volume group** (typically `vg0` or one named after the hostname — whatever `sudo vgs` shows). You do **not** need a dedicated VG; renaming the OS VG is invasive (GRUB + initramfs + reboot) and not worth it.

| Inside the VG | Purpose | Notes |
| ---- | ---- | ---- |
| `sbgh-meta` | Linear XFS LV mounted at `/var/lib/sbgh` | Per-job artifacts, archived SQLite results, bare git mirror. ~50–150 GiB is plenty. |
| `thinpool` | Thin pool | Holds base chainstate LVs and per-job snapshots. Size = total chainstate baselines you want to keep × ~2–3×. |
| `mainnet-YYYY-MM-DD` | Thin LV, XFS | One per chainstate baseline. The orchestrator snapshots whichever is lexicographically newest. |

### Playbook

```bash
# Substitute your actual VG name everywhere. `sudo vgs` shows what you have.
VG=vg0

# ─── Metadata LV (persistent: /var/lib/sbgh) ────────────────────────────
sudo lvcreate -L 150G -n sbgh-meta "$VG"
sudo mkfs.xfs -f "/dev/$VG/sbgh-meta"
sudo mkdir -p /var/lib/sbgh

META_UUID=$(sudo blkid -s UUID -o value "/dev/$VG/sbgh-meta")
echo "UUID=$META_UUID  /var/lib/sbgh  xfs  defaults,noatime  0  2" \
  | sudo tee -a /etc/fstab
sudo systemctl daemon-reload
sudo mount /var/lib/sbgh
findmnt /var/lib/sbgh         # confirm

# ─── Thin pool ──────────────────────────────────────────────────────────
# -Zn        : don't zero new blocks (faster; chainstate writes overwrite anyway)
# --chunksize: smaller chunks = better space efficiency for snapshot churn
sudo lvcreate --type thin-pool --name thinpool -L 1500G \
    --chunksize 256K -Zn "$VG"
sudo lvchange --monitor y "$VG/thinpool"

# ─── First base chainstate LV ───────────────────────────────────────────
# Use the helper script — it creates today's dated LV, downloads + verifies
# the latest Hiro mainnet snapshot in parallel via aria2c, extracts it, and
# rotates out any older `mainnet-*` LVs (skipping any with active snapshots).
sudo apt install -y aria2 zstd
sudo ./scripts/download-chainstate.sh
# See `scripts/download-chainstate.sh --help` for --vg / --date / --keep-old /
# --connections / etc. Allow several hours; run inside tmux/screen if remote.
```

Manual equivalent (skip if you ran the script):

```bash
# Create the LV
sudo lvcreate -V 500G --thin --name mainnet-2026-05-23 "$VG/thinpool"
sudo mkfs.xfs "/dev/$VG/mainnet-2026-05-23"

# Populate it (mount, write chainstate, unmount, deactivate so snapshots can
# be created against it).
sudo mount "/dev/$VG/mainnet-2026-05-23" /mnt
# ... rsync / curl / extract ...
sudo umount /mnt
sudo lvchange -an "$VG/mainnet-2026-05-23"
```

### Refreshing the chainstate later

The script is idempotent across days: just rerun `sudo ./scripts/download-chainstate.sh` whenever you want a fresher baseline. It picks today's date for the new LV name, and by default rotates out the previous one (with `--keep-old` to retain history). Active benchmark runs against the old LV are protected — the rotation step skips any LV with active snapshots.

### Thin vs thick snapshots

The orchestrator creates per-job snapshots of the base chainstate LV. Two modes, controlled by `[lvm].chainstate_snapshot_size_gib` in `config.toml`:

- **Thin (default, leave the field unset)**: `lvcreate --snapshot` runs without `-L`, so the snapshot lives in the thin pool and grows on demand. This is what you want when the base LV is itself thin (the playbook above). Cheap, fast, no upfront allocation.
- **Thick (set the field to a GiB value)**: `lvcreate --snapshot -L NG` allocates a fixed COW exception store outside the pool. Use only if your base chainstate LV is a thick (non-thin) volume — otherwise the result is a thick snapshot of a thin volume, which loses the cheap-snapshot property and (on some lvm2 versions) errors out.

If you're following the playbook above, leave the field unset.

## 3. Service users + sudoers

Two host users, one per service. Keeping them separate is the filesystem
half of the security boundary — without it, a compromised handler
container could read the orchestrator's bind-mounted GitHub App PEM and
impersonate the App. (The Postgres half is in §6.)

Four distinct uids total. Two of them only exist *inside* containers
(postgres and smee) and need no host user — only the host file
ownership of their bind mounts (postgres) or nothing at all (smee)
matters:

| Identity | uid/gid | Where it lives | Holds |
| ---- | ---- | ---- | ---- |
| postgres (container) | 900/900 | Owns `/var/lib/sbgh/postgres` on the host. | DB on disk |
| `sbgh-handler` (host) | 901/901 | Owns `/etc/sbgh/handler` on the host. The handler container is built with this uid so the bind-mounted config is readable. | webhook HMAC secret only |
| `sbgh` (host) | 902/902 | Runs the orchestrator on the host (libvirt, LVM, sudoers). Owns `/etc/sbgh/orchestrator`. | GitHub App private key |
| smee (container only) | 903/903 | No host user, no bind mounts, no secrets. Distinct uid only for defense-in-depth against a future container escape. | — |

All four are in the system uid range (100–999) so `useradd --system`
doesn't warn. Numbers picked in the low 900s to dodge the common Ubuntu
allocations at the top of that range (997 `systemd-timesync`, 998
`systemd-network`, 999 `dnsmasq`).

Confirm the ids are free first:

```bash
getent passwd 900 901 902 903     # all four lines must be empty
getent group  900 901 902 903     # ditto
```

If any are taken, pick alternatives and set the corresponding env vars
in `docker/.env` to match — substitute the same numbers in the `useradd`
commands below — then rebuild the images (`docker compose -f
docker/docker-compose.yml build --no-cache`). The smee uid
(`SBGH_SMEE_UID`/`SBGH_SMEE_GID`) is runtime-only and doesn't need a
rebuild.

```bash
# Handler-container shadow user. Owns the handler-side config dir. No
# --create-home, no shell — it's purely a filesystem identity.
sudo groupadd --system --gid 901 sbgh-handler
sudo useradd  --system --uid 901 --gid 901 \
              --shell /usr/sbin/nologin sbgh-handler

# Orchestrator service user. Runs the actual binary on the host.
sudo groupadd --system --gid 902 sbgh
sudo useradd  --system --uid 902 --gid 902 \
              --shell /usr/sbin/nologin sbgh
sudo usermod -a -G libvirt sbgh        # virsh access without sudo for read-only ops

# Per-job subdirs on the XFS metadata LV (mounted at /var/lib/sbgh in §2)
# and the tmpfs root on /run. All sbgh-owned — handler never touches
# these.
sudo install -d -m 0755 -o sbgh -g sbgh /var/lib/sbgh/jobs
sudo install -d -m 0755 -o sbgh -g sbgh /var/lib/sbgh/results
sudo install -d -m 0755 -o sbgh -g sbgh /var/lib/sbgh/git
sudo install -d -m 0755 -o sbgh -g sbgh /run/sbgh
sudo install -d -m 0755 -o sbgh -g sbgh /run/sbgh/jobs
```

The in-container postgres uid (900) doesn't need a matching host user —
it's only used as a numeric owner for `/var/lib/sbgh/postgres` (created
in §6).

Install the sudoers fragment. Only the orchestrator user needs sudo;
`sbgh-handler` runs entirely inside an unprivileged container.

```bash
sudo tee /etc/sudoers.d/sbgh >/dev/null <<'EOF'
sbgh ALL=(root) NOPASSWD: /usr/sbin/lvcreate, /usr/sbin/lvremove, /usr/sbin/lvs
sbgh ALL=(root) NOPASSWD: /usr/sbin/mkfs.ext4, /usr/sbin/losetup
sbgh ALL=(root) NOPASSWD: /usr/bin/mount, /usr/bin/umount, /usr/bin/chown
sbgh ALL=(root) NOPASSWD: /usr/bin/virsh
EOF
sudo chmod 0440 /etc/sudoers.d/sbgh
sudo visudo -cf /etc/sudoers.d/sbgh     # syntax check
```

Verify as `sbgh`:

```bash
sudo -u sbgh sudo -n /usr/sbin/lvs --version
sudo -u sbgh sudo -n /usr/bin/virsh --version
```

## 4. Register a dev GitHub App

You don't need a public domain — webhook delivery will be tunneled (next section).

1. Go to **<https://github.com/settings/apps/new>** (or your org's developer settings).
2. Fill in:
   - **GitHub App name**: anything unique, e.g. `sbgh-dev-<your-handle>`
   - **Homepage URL**: anything (e.g. your repo URL)
   - **Webhook URL**: paste a fresh smee.io channel URL — visit [smee.io](https://smee.io/) in a browser, it'll redirect you to one, copy that. Or programmatically: `curl -sI https://smee.io/new | awk -F': ' 'tolower($1)=="location"{print $2}' | tr -d '\r\n'`. (We use smee.io because GitHub App webhooks are delivered to a single App-level URL — `gh webhook forward` only works for repo/org webhooks, not App webhooks.)
   - **Webhook secret**: paste a long random string. Generate one with:

     ```bash
     openssl rand -hex 32
     ```

     Save this — it'll go in `/etc/sbgh/handler/secrets.env` as
     `SBGH_WEBHOOK_SECRET` in the next section. The orchestrator never
     sees it (it doesn't need to verify webhook signatures).
3. **Repository permissions**:

    | Permission | Access |
    | ---- | ---- |
    | Contents | Read-only |
    | Issues | Read & write |
    | Metadata | Read-only |
    | Pull requests | Read & write |

4. **Subscribe to events**: `Issue comment` (that's the only one we need; PR comments come through this event).
5. **Where can this GitHub App be installed?**: "Only on this account" (for dev).
6. Click **Create GitHub App**.
7. From the new app's settings page, copy the **Client ID** (the `Iv23li…` value listed near the top, right under "About") — that's `SBGH_GH_CLIENT_ID`. GitHub also displays an "App ID" (a numeric value); we don't use that. Both work today, but Client ID is the recommended modern form and the only one that survives if GitHub ever deprecates the legacy App ID auth path.
8. Scroll down → **Generate a private key**. The browser downloads a `.pem` file. Move it to the orchestrator's config dir (the handler never sees this file):

    ```bash
    sudo install -d -m 0700 -o sbgh -g sbgh /etc/sbgh/orchestrator
    sudo install -m 0600 -o sbgh -g sbgh \
      ~/Downloads/sbgh-dev-*.pem /etc/sbgh/orchestrator/github-app.private-key.pem
    ```

9. In the left sidebar of the app page, click **Install App** → install on your account → choose the fork of `stacks-core` you'll be testing against. Note the **installation ID** in the URL (`/settings/installations/<N>`).

## 5. Lay out the two config directories

Two disjoint dirs, one per service, owned by different users. This is
the filesystem half of the security boundary: a compromised handler
container can read `/etc/sbgh/handler` (its own bind mount) but not
`/etc/sbgh/orchestrator` (owned by a different uid, never mounted into
the handler container).

| Path | Owner / mode | Files | Read by |
| ---- | ---- | ---- | ---- |
| `/etc/sbgh/handler/` | `sbgh-handler:sbgh-handler` 0700 | `config.toml`, `secrets.env` | handler container |
| `/etc/sbgh/orchestrator/` | `sbgh:sbgh` 0700 | `config.toml`, `github-app.private-key.pem` | host orchestrator |

Both dirs are bind-mounted into their respective containers at the
*same* path on both sides, so file references inside the TOML
(`private_key_path = "/etc/sbgh/orchestrator/github-app.private-key.pem"`)
resolve identically on host and in container.

### 5a. Handler config

```bash
# Directory (uid 997 from §3).
sudo install -d -m 0700 -o sbgh-handler -g sbgh-handler /etc/sbgh/handler

# config.toml: non-secret settings (allowlist, bind addr).
sudo install -m 0600 -o sbgh-handler -g sbgh-handler \
  config.example.handler.toml /etc/sbgh/handler/config.toml
sudo -u sbgh-handler $EDITOR /etc/sbgh/handler/config.toml
# Set at minimum:
#   [authorization].allowed_repositories = ["<your-handle>/stacks-core"]

# secrets.env: env_file for the handler container. The webhook HMAC
# secret is the ONLY secret the handler ever sees — no App key.
# DATABASE_URL is overridden by compose to use the narrow sbgh_handler
# role, so leave it out of this file.
sudo tee /etc/sbgh/handler/secrets.env >/dev/null <<EOF
SBGH_WEBHOOK_SECRET=<openssl rand -hex 32>
EOF
sudo chmod 0600 /etc/sbgh/handler/secrets.env
sudo chown sbgh-handler:sbgh-handler /etc/sbgh/handler/secrets.env
```

### 5b. Orchestrator config

```bash
# Directory (uid 998 from §3). The PEM from step 4.8 already lives here.
sudo install -d -m 0700 -o sbgh -g sbgh /etc/sbgh/orchestrator

# config.toml: App credentials, LVM/libvirt knobs, etc.
sudo install -m 0600 -o sbgh -g sbgh \
  config.example.orchestrator.toml /etc/sbgh/orchestrator/config.toml
sudo -u sbgh $EDITOR /etc/sbgh/orchestrator/config.toml
# Set at minimum:
#   [server].database_url       = "postgres://sbgh_orch:<SBGH_ORCH_DB_PASSWORD>@127.0.0.1:5432/sbgh"
#                                 (use the same value you put in docker/.env in §6)
#   [github].client_id          = "Iv23li..."   (from step 4.7)
#   [github].private_key_path   = "/etc/sbgh/orchestrator/github-app.private-key.pem"
#   [lvm].vg_name               = "vg0"
#   [lvm].thinpool              = "thinpool"
#   [vm].golden_image           = "/var/lib/libvirt/images/sbgh-golden-ubuntu24.qcow2"
```

## 6. Run handler + smee + Postgres + migrate in Docker

The handler, smee, Postgres, and a one-shot migrate job run in containers
via [docker/docker-compose.yml](../docker/docker-compose.yml). The
orchestrator stays on the host (it needs LVM + libvirt + the golden
image).

### Database role split (the Postgres half of the boundary)

| DB role | Holds password | Grants | Used by |
| ---- | ---- | ---- | ---- |
| `sbgh` | `POSTGRES_OWNER_PASSWORD` | full ownership of the `sbgh` database | `sbgh-migrate` one-shot only |
| `sbgh_handler` | `SBGH_HANDLER_DB_PASSWORD` | `USAGE` on schema, `INSERT` on `jobs` | handler container |
| `sbgh_orch` | `SBGH_ORCH_DB_PASSWORD` | `USAGE` on schema, `SELECT, UPDATE` on `jobs` | host orchestrator |

Roles + grants are (re)applied on every `docker compose up` by the
`sbgh-migrate` service (Rust binary, `crates/sbgh-migrate`). It connects
as the owner, runs schema migrations, then upserts the two narrow roles
with whatever passwords are currently in `docker/.env`. Handler + smee
`depends_on: service_completed_successfully` so they never see a
half-migrated schema or a role without grants.

### One-time setup

```bash
# Docker if not installed.
sudo apt install -y docker.io docker-compose-v2
sudo usermod -a -G docker $USER     # log out + back in for this to take effect

# Runtime env. Required values (SMEE_CHANNEL + all three DB passwords)
# have no sensible defaults; compose will refuse to start without them.
cp docker/.env.example docker/.env
$EDITOR docker/.env

# Generate three distinct passwords. Keep them out of shell history.
for v in POSTGRES_OWNER_PASSWORD SBGH_HANDLER_DB_PASSWORD SBGH_ORCH_DB_PASSWORD; do
    echo "$v=$(openssl rand -base64 32)"
done >> docker/.env

# The orchestrator config.toml needs the SBGH_ORCH_DB_PASSWORD value too
# (host-side DB URL). Same value, two places — there is no shared file
# the orchestrator and the migrate container both read.

# Prepare the host-side Postgres data directory. We bind-mount this into
# the container so the data survives `docker volume prune`. The container
# runs rootless as uid 900 (overriding the upstream image's 999 to dodge
# dnsmasq) and the entrypoint skips its usual chown when not running as
# root — so the host dir MUST already exist owned by uid 900 mode 0700
# before first start.
sudo install -d -m 0700 -o 900 -g 900 /var/lib/sbgh/postgres
```

### Bring up the stack

```bash
docker compose -f docker/docker-compose.yml up -d --build
```

What gets built + run:

| Service | Image | Listens | DB role | Talks to |
| ---- | ---- | ---- | ---- | ---- |
| `sbgh-postgres` | `postgres:18-trixie` (uid `${POSTGRES_UID:-900}`) | 127.0.0.1:5432 | — | — |
| `sbgh-migrate` | local `migrate` target (one-shot) | — | `sbgh` (owner) | `postgres:5432` |
| `sbgh-handler` | local `handler` target (uid `${SBGH_UID:-901}`) | 127.0.0.1:8080 | `sbgh_handler` | `postgres:5432` (INSERT only) |
| `sbgh-smee` | local `smee` target (uid `${SBGH_SMEE_UID:-903}`) | — | — | smee.io (SSE in), `handler:8080` (HTTP out) |

All four containers run rootless. The handler + smee in-container uid
must match the host `sbgh-handler` uid so the mode-0600 config bind-
mounted from `/etc/sbgh/handler` is readable. Defaults to 901; override
via `SBGH_UID` / `SBGH_GID` in `docker/.env` if your host uses a
different id (check with `id sbgh-handler`). Rebuild after changing
those:

```bash
docker compose -f docker/docker-compose.yml build --no-cache
```

### Tail + verify

```bash
# All logs
docker compose -f docker/docker-compose.yml logs -f

# Just the handler
docker compose -f docker/docker-compose.yml logs -f handler

# The migrate run — should have "migrate complete" then exited 0.
docker compose -f docker/docker-compose.yml logs migrate

# Quick health check
curl -i http://127.0.0.1:8080/health    # → 200 OK
```

The smee container picks up `SMEE_CHANNEL` from `docker/.env` and starts
forwarding to `http://handler:8080/webhook` over the docker network.
After `migrate` exits successfully, handler + smee start and stay up.

To re-run migrations (e.g. after pulling new code that adds a SQL
migration):

```bash
docker compose -f docker/docker-compose.yml run --rm migrate
```

### Why not the orchestrator too?

Three reasons it stays on the host:

- It calls `lvcreate`/`lvremove` via sudo — easy to wire from host, awkward from inside a container.
- It calls `virsh` — same, plus the libvirt socket is host-side.
- It needs read access to `/var/lib/libvirt/images/sbgh-golden-ubuntu24.qcow2` and write access under `/var/lib/sbgh/jobs/`, both of which are host paths the libvirt-qemu apparmor profile already knows about.

### Build + run the orchestrator (host-side)

```bash
# Build
cargo build --release -p sbgh-orchestrator

# Foreground run for first-time debugging
sudo -u sbgh \
  RUST_LOG=info,sbgh_orchestrator=debug,sqlx=warn \
  target/release/sbgh-orchestrator
```

(For a long-running setup, write a systemd unit — see `docs/architecture.md` for the operator-setup notes.)

Successful boot:

```text
INFO sbgh_orchestrator: orchestrator started
```

It'll sit there polling the queue every 5 seconds. The handler in Docker writes jobs to the same Postgres; the orchestrator on the host picks them up.

## 7. First real run

1. Go to a PR on your fork.
2. Comment exactly `/benchmark` on its own line.
3. Within a few seconds you should see:
   - **sbgh-smee** logs `forwarded delivery status=200`.
   - **handler** logs the inbound POST (visible at `RUST_LOG=debug` via `tower_http::trace`) and a new comment appears on the PR: *"⏳ queued at position **1** (job `<uuid>`)…"*.
   - **orchestrator** (Linux only): claims the job, starts provisioning, defines + starts the domain. On a macOS dev machine without the orchestrator running, the row simply stays in `queued` — that's the expected handler-only mode for inbound-side validation.
4. The PR comment updates as phases change: `building → running → collecting → done`.
5. On success the comment becomes a ✅ with the summary JSON; on failure ❌ with the error + console tail.

To watch the VM serial console live during the run, before the job dir is cleaned up:

```bash
sudo tail -F /var/lib/sbgh/jobs/<job-id>/console.log
```

(Path is logged by the orchestrator when the job starts.)

To list domains while a job is running:

```bash
sudo virsh list --all
sudo virsh dominfo sbgh-<job-id>
```

After the job finishes, query Postgres for the full forensics blob:

```bash
psql "$DATABASE_URL" -c "
  SELECT id, status, error,
         result->'finish_reason'   AS finish_reason,
         result->'last_phase'      AS last_phase,
         result->'sqlite_size_bytes' AS sqlite_size
  FROM jobs
  ORDER BY queued_at DESC LIMIT 5;
"
```

And the archived SQLite output for the latest job:

```bash
ls -lh /var/lib/sbgh/results/
sqlite3 /var/lib/sbgh/results/<job-id>.sqlite '.tables'
```

## 8. Troubleshooting

### Webhook isn't reaching the handler

- Check `sbgh-smee` is still running and shows `connected to smee channel`.
- Re-deliver an old webhook from **App settings → Advanced → Recent Deliveries** to retry without re-typing `/benchmark`.
- `curl -v http://localhost:8080/health` should return 200.

### `sbgh-smee` is connected but only logs `event=ping`

You're subscribed to a smee.io channel that nobody is delivering to. GitHub's "Recent Deliveries" page may show successful 200s — that just means smee.io accepted the POST for *its* channel, not that any client received it. **The smee URL set on the App's Webhook URL field and the one passed to `sbgh-smee --channel` must be byte-identical.** Open both in a browser and compare; the App's channel will show the delivery, the other won't.

### Handler logs the inbound webhook but the bot can't post a comment (HTTP 401)

```text
http.method=POST http.url=https://api.github.com/repos/.../issues/N/comments
http.status_code=401 otel.status_code="ERROR"
failed to post initial PR comment
```

Two possible causes, in order:

1. **Permission isn't granted**: App settings → **Permissions & events → Repository permissions → Issues** must be **"Read & write"**. PR comments go through the issues endpoint, not the pull-request reviews endpoint.
2. **Permission was changed after install and the installation hasn't accepted it**: visit <https://github.com/settings/installations>, find the App, click **Configure** — there'll be a yellow banner *"This GitHub App is requesting new permissions. Review →"*. Accept it.

After fixing either, **restart the handler** (the in-memory installation token cache holds the old token for up to ~1 hour) and hit **Redeliver** on the GitHub delivery page.

### Signature verification fails (401)

- `SBGH_WEBHOOK_SECRET` in `/etc/sbgh/handler/secrets.env` must match exactly what you set in the App. No surrounding quotes, no trailing newline.
- If you copy-pasted, regenerate with `openssl rand -hex 32` and update both sides.

### `loading github app private key` error at orchestrator startup

(The handler never loads the PEM — if you see this error, it's the orchestrator on the host.)

- `sudo -u sbgh ls -l /etc/sbgh/orchestrator/github-app.private-key.pem` — must be readable as user `sbgh` (mode `0600`, owner `sbgh:sbgh`).
- PEM file must start with `-----BEGIN RSA PRIVATE KEY-----` (GitHub gives you PKCS#1; `jsonwebtoken` accepts both PKCS#1 and PKCS#8).

### Handler logs `permission denied for table jobs`

The handler is trying a query other than `INSERT`. By design `sbgh_handler` only has `INSERT` (see §6 role table). Either:

- A code change introduced a `SELECT`/`UPDATE` from the handler path — that's a regression of the role split; either move the query to the orchestrator or widen the grant deliberately, don't paper over it.
- The migrate container didn't run (or ran with stale passwords). Check `docker compose logs migrate`; re-run with `docker compose run --rm migrate`.

### `password authentication failed for user "sbgh_handler"` (or `sbgh_orch`)

The role's password in Postgres no longer matches what the handler/orchestrator was given. Likely cause: someone edited `docker/.env` but didn't re-run migrate (which resets passwords to match). Fix:

```bash
docker compose -f docker/docker-compose.yml run --rm migrate
docker compose -f docker/docker-compose.yml restart handler
# (and restart the host orchestrator)
```

### `installation token mint failed: 404`

- The App is registered but **not installed** on the target repo. Re-check step 4.9.

### `no base chainstate LV found in VG ... matching prefix mainnet-`

- `sudo lvs` — confirm at least one LV exists in your VG (whatever `[lvm].vg_name` is set to) with a name starting with `mainnet-`.
- The orchestrator uses `lvs --select 'lv_name=~^mainnet-'` (regex). If your LV is in a different VG or named differently, override `chainstate_base_prefix` in the config.

### `lvcreate` fails with "Snapshots of snapshots are not supported"

- You tried to snapshot from an already-snapshotted LV. The base LV must be the original thin volume, not another snapshot. Run `sudo lvs -a` and check the `Origin` column.

### VM boots but cloud-init never runs

- Confirm `cidata.iso` was attached: `sudo virsh dumpxml sbgh-<job-id> | grep -A1 cidata`.
- Inspect the ISO contents: `sudo isoinfo -i /var/lib/sbgh/jobs/<id>/cidata.iso -l`.
- In the VM console (`virsh console sbgh-<id>` while running), look for `cloud-init[...] Datasource detected: NoCloud`. If it falls back to `Datasource detected: None`, the ISO label is wrong — `cloud-localds` defaults to `cidata` which is what we want.

### `cargo build` fails inside the VM

- The console log will tell you which dep is missing. Add it to the `--install` list in [scripts/build-golden-image.sh](../scripts/build-golden-image.sh) and rebuild the golden image.
- Common stacks-core deps that aren't in `build-essential`: `libsqlite3-dev`, `protobuf-compiler`.

### Job dir was deleted before you could look at it

By default the orchestrator deletes the per-job dir on completion. The console tail and last phase are persisted to Postgres (`jobs.result`) regardless, and the SQLite output goes to `paths.results_archive_dir`. For first-time bringup debugging where you need to step through the artifacts:

```bash
# Stop teardown by setting an environment override
SBGH_DEBUG_KEEP_JOB_DIR=1 target/release/sbgh-orchestrator
```

(That flag isn't wired up yet — open a TODO. Until then, comment out the `remove_dir_all(job_dir)` line in [crates/sbgh-orchestrator/src/libvirt/driver.rs](../crates/sbgh-orchestrator/src/libvirt/driver.rs) during bringup.)

### Domain stuck in `paused` or `crashed`

```bash
sudo virsh dominfo sbgh-<job-id>
sudo virsh dumpxml sbgh-<job-id> | less
```

If the orchestrator died mid-job, you may have orphaned domains and LVs:

```bash
sudo virsh list --all | grep sbgh-
sudo lvs | grep sbgh-

# clean up an orphan
sudo virsh destroy sbgh-<job-id> 2>/dev/null
sudo virsh undefine sbgh-<job-id>
sudo lvremove --force <vg>/sbgh-<job-id>-chainstate   # <vg> = your [lvm].vg_name
sudo umount /run/sbgh/jobs/<job-id> 2>/dev/null
sudo rm -rf /var/lib/sbgh/jobs/<job-id> /run/sbgh/jobs/<job-id>
```

(A "sweep on startup" step on the orchestrator that reaps these is on the v2 list.)
