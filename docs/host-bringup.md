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
  qemu-system-x86 qemu-utils libvirt-daemon-system virtinst \
  cloud-image-utils libguestfs-tools \
  git lvm2 util-linux e2fsprogs xfsprogs
```

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

## 3. Service user + sudoers

```bash
sudo useradd --system --create-home --shell /usr/sbin/nologin sbgh
sudo usermod -a -G libvirt sbgh        # virsh access without sudo for read-only ops

# Per-job subdirs on the XFS metadata LV (mounted at /var/lib/sbgh in §2)
# and the tmpfs root on /run.
sudo install -d -m 0755 -o sbgh -g sbgh /var/lib/sbgh/jobs
sudo install -d -m 0755 -o sbgh -g sbgh /var/lib/sbgh/results
sudo install -d -m 0755 -o sbgh -g sbgh /var/lib/sbgh/git
sudo install -d -m 0755 -o sbgh -g sbgh /run/sbgh
sudo install -d -m 0755 -o sbgh -g sbgh /run/sbgh/jobs
```

Install the sudoers fragment:

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

     Save this — it's `SBGH_GH_WEBHOOK_SECRET`.
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
8. Scroll down → **Generate a private key**. The browser downloads a `.pem` file. Move it somewhere safe and lock it down:

    ```bash
    sudo install -m 0600 -o sbgh -g sbgh \
      ~/Downloads/sbgh-dev-*.pem /etc/sbgh/github-app.private-key.pem
    ```

9. In the left sidebar of the app page, click **Install App** → install on your account → choose the fork of `stacks-core` you'll be testing against. Note the **installation ID** in the URL (`/settings/installations/<N>`).

## 5. Tunnel webhook deliveries to localhost

Use the workspace-local `sbgh-smee` binary — a ~150 LoC Rust port of `smee-client` that avoids pulling in the npm dep tree. It's an SSE consumer that subscribes to the smee.io channel set on the App and POSTs each delivery to a local URL with the original GitHub headers reconstructed.

```bash
cargo run --release -p sbgh-smee -- \
  --channel https://smee.io/<your-channel> \
  --target http://localhost:8080/webhook
```

Leave this running. It logs each forwarded delivery. When the handler is running, you can verify by re-delivering a past webhook from the App settings page (**Advanced → Recent Deliveries → Redeliver**).

### Why not `gh webhook forward` or `npm smee-client`?

- `gh webhook forward` (a `gh` CLI extension) only works with **repository** / **organization** webhooks, not GitHub App webhooks. Our App's webhook URL is set at the App level, so this isn't applicable.
- `npm install --global smee-client` works but pulls in Node + a transitive dep tree, which we don't otherwise need on a dev machine. `sbgh-smee` is the same protocol, audited per-line, and shares the rest of the workspace's Rust deps.

### HMAC compatibility note

`sbgh-smee` re-serializes the JSON body before forwarding, which means GitHub's HMAC-SHA256 over the original body needs the re-serialized bytes to match exactly. We achieve this by enabling `serde_json`'s `preserve_order` feature workspace-wide so the body's key order survives the round trip (the upstream Node `smee-client` relies on V8 having the same property). If your handler ever rejects forwarded deliveries with `401 invalid signature`, this is the first thing to check.

## 6. Configure + run the services

### Postgres

```bash
docker compose -f docker/docker-compose.yml up -d
```

### Environment

Two ways to load secrets, pick one (or mix):

**A. `.env` in the repo** — convenient for prod where the host is dedicated to this service:

```bash
cp .env.example .env
chmod 0600 .env
```

**B. Secrets file outside the repo** — recommended when you share the repo with other tools/people (or with an AI assistant) and don't want secrets in any path they can read:

```bash
mkdir -p ~/.config/sbgh && chmod 700 ~/.config/sbgh
cp .env.example ~/.config/sbgh/secrets.env
chmod 600 ~/.config/sbgh/secrets.env
```

Then point the binaries at it with `--env-file`:

```bash
sbgh-handler      --env-file ~/.config/sbgh/secrets.env
sbgh-orchestrator --env-file ~/.config/sbgh/secrets.env
```

(With `--env-file`, a missing/unreadable file is a hard error. Without it, `./.env` is loaded best-effort and a missing file is silently tolerated.)

Edit whichever file you chose and set at minimum:

```bash
DATABASE_URL=postgres://sbgh:sbgh@127.0.0.1:5432/sbgh
SBGH_GH_CLIENT_ID=<from step 4.7>
SBGH_GH_PRIVATE_KEY_PATH=/etc/sbgh/github-app.private-key.pem
SBGH_GH_WEBHOOK_SECRET=<from step 4.2>
SBGH_ALLOWED_REPOS=<your-handle>/stacks-core
SBGH_VM_GOLDEN_IMAGE=/var/lib/libvirt/images/sbgh-golden-ubuntu24.qcow2
SBGH_LVM_VG=vg0                  # whatever `sudo vgs` shows — usually your OS VG
SBGH_LVM_THINPOOL=thinpool
RUST_LOG=info,sbgh_handler=debug,sbgh_orchestrator=debug
```

Either way, variables you `export` in your shell before launching the binary win over what's in the file — useful for one-off overrides without editing.

#### Or: put everything (including secrets) in a TOML file

The split between "secrets in env" and "everything else in TOML" is a convention, not enforced — `Config::load` accepts `webhook_secret`, `private_key_path`, `client_id`, and `database_url` from either source, with env winning on collision. If you'd rather keep one file and skip the env dance entirely (handy on a personal dev box), drop a TOML at one of these paths and the loader will find it without any flag:

```text
$SBGH_CONFIG                          # if set, takes precedence
/etc/sbgh/config.toml                 # system path
~/.config/sbgh/config.toml            # user path (XDG-style fallback)
```

```bash
# personal dev box
mkdir -p ~/.config/sbgh && chmod 700 ~/.config/sbgh
cp config.example.toml ~/.config/sbgh/config.toml
chmod 600 ~/.config/sbgh/config.toml
$EDITOR ~/.config/sbgh/config.toml    # add webhook_secret, private_key_path, etc.

# prod / systemd
sudo install -m 0644 -o sbgh -g sbgh config.example.toml /etc/sbgh/config.toml
sudo $EDITOR /etc/sbgh/config.toml
```

The "outside the repo at mode 0600" combo gives you the same leakage properties as an env file without the shell-sourcing dance.

### Build + run

```bash
cargo build --release --workspace

# handler — apply migrations + listen for webhooks on :8080
SBGH_CONFIG=/etc/sbgh/config.toml \
  target/release/sbgh-handler &

# orchestrator — claim + execute jobs
sudo -u sbgh -E \
  SBGH_CONFIG=/etc/sbgh/config.toml \
  target/release/sbgh-orchestrator
```

(For a long-running setup, write systemd units; for first-run debugging, foreground is easier.)

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

- The `SBGH_GH_WEBHOOK_SECRET` env value must match exactly what you set in the App. No surrounding quotes, no trailing newline.
- If you copy-pasted, regenerate with `openssl rand -hex 32` and update both sides.

### `loading github app private key` error at handler startup

- `ls -l $SBGH_GH_PRIVATE_KEY_PATH` — must be readable by the user running the handler (mode `0600`, owned by that user is ideal).
- PEM file must start with `-----BEGIN RSA PRIVATE KEY-----` (GitHub gives you PKCS#1; `jsonwebtoken` accepts both PKCS#1 and PKCS#8).

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
