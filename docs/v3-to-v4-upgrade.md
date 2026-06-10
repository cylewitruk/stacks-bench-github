# v3 → v4 upgrade runbook

Cutting a **running** v3 deployment over to v4 (the **artifact store**, item
`0001-artifact-store`). v4 makes run-artifact storage pluggable behind an
`[artifacts]` config section: the default `local` is behavior-preserving, and an
opt-in `s3` mode ships artifacts to S3-compatible object storage (Hetzner) so
off-box consumers (Slack, the portal) can fetch them.

For a **fresh** install, follow [host-bringup.md](./host-bringup.md) — it brings
a host up on the current architecture. This doc is only for upgrading a host
already running v3 (see [v2-to-v3-upgrade.md](./v2-to-v3-upgrade.md) if you're
still on v2).

> **A light upgrade.** Unlike v2→v3, this is **not** a hard cutover: no DB
> migration, no role/binary/container changes, and **`local` mode reproduces v3
> behavior exactly**. Part A (the code bump) is mandatory and trivial; Part B
> (opt into S3) is optional and the only place new config + secrets appear.

## What actually changes

| Layer | v3 (deployed) | v4 (this upgrade) |
| ---- | ---- | ---- |
| Artifact storage | local disk under `[paths].results_archive_dir` | pluggable: `local` (same) **or** `s3` (durable, off-box, presigned-fetchable) |
| Config | — | a new optional `[artifacts]` section; **default `local`** |
| Secrets | `SBGH_API_INGEST_TOKEN` | `+ SBGH_ARTIFACTS_S3_*` **only if** you enable S3 (env-only, like the ingest token) |
| Run summary | per-artifact pointers are **absolute paths** | per-artifact pointers are **store keys** (`<job_id>/<relative>`), resolved through the store (Decision 0002) — local keys map to the same on-disk path |
| Database | — | **no migration**; the keys live in the existing `job_event`/`job_result` JSON, not new columns |
| Host binary / units / containers | `sbgh-daemon`, `/etc/sbgh/daemon`, the compose stack | **unchanged** |

**Behavior is preserved in `local` mode.** A v4 daemon with no `[artifacts]`
section (or `kind = "local"`) archives to exactly the same
`results_archive_dir/<job_id>/…` paths v3 used and renders identical PR
comments / checks. The only internal change is that a new run's summary stores
*keys* instead of absolute paths, resolved back to the same files.

## Part A — deploy v4 (mandatory, local mode)

No config change required. Pull, build, restart.

```bash
cd /path/to/stacks-bench-github
git fetch origin && git checkout main && git pull     # or the v4 tag/commit
just build                                            # → target/release/sbgh-daemon

# Reinstall the binary + unit and restart the daemon (install-daemon.sh is
# idempotent — it overwrites the binary and restarts the service).
sudo ./scripts/install-daemon.sh
journalctl -u sbgh-daemon -f
#   expect a normal startup: migrations applied (none new for v4), "api
#   listening". Ctrl-C once it's serving.
```

The webhook/handler containers are unaffected by v4 — no rebuild needed unless
you're pulling other changes.

### Verify (local mode)

Post `/benchmark` on a PR in an allowed repo (or push a watched branch) and let
a run complete, then confirm the artifacts landed on disk exactly as before:

```bash
# The per-job archive dir under results_archive_dir, as in v3:
#   run.json, appdata/stacks-bench.db (the SQLite), stacks-bench (the binary),
#   phase.log
ls -laR /var/lib/sbgh/results/<job-id>/
sudo -u sbgh sbgh-cli webhook tail --limit 5
```

The PR comment / commit check render the same metrics as v3. That's the whole
of the mandatory upgrade.

> **Historical artifacts (known limitation, low impact).** Runs completed
> *before* v4 stored **absolute paths** in their summaries; v4's key-based
> reader (`ArtifactStore::get`) rejects absolute/`..` paths as a traversal
> guard, so it won't re-resolve a pre-v4 run's `run.json` from disk. This only
> affects **re-rendering an already-completed historical job** (orphan
> recovery / a manual re-render) — the raw `run.json` content and metrics for
> those jobs are already persisted in `job_result`, and every **new** run works
> normally. No action needed.

## Part B — opt into S3 (optional)

> **Smoke-test first.** The S3 code path — signing, streaming, fault-tolerance,
> and a full upload→bucket→presigned-GET round-trip — is covered in CI against a
> real S3 server (MinIO, `s3_round_trip.rs`), so the implementation is proven.
> Still run [§B4](#b4-verify-the-live-round-trip) once against **your** bucket on
> first enable, to catch endpoint / credential / network specifics that CI can't
> (region, DNS, firewall, key scoping).

S3 mode keeps the local archive as a diagnostic breadcrumb **and** retained
copy, then best-effort uploads each artifact to the bucket. An upload failure is
logged and **never fails the benchmark** (Decision 0003) — the local copy
stays fetchable.

### B1. Provision a bucket + credentials

In the Hetzner console (or any S3-compatible provider), create:

- an object-storage **bucket** (e.g. `sbgh-artifacts`) in a region close to the
  orchestrator — intra-Hetzner egress is free, which is the whole point;
- an **access key / secret** scoped to that bucket.

Note the **endpoint** URL (e.g. `https://fsn1.your-objectstorage.com`), the
**bucket** name, and the **region** (e.g. `fsn1`).

### B2. Daemon config — add `[artifacts]`

```bash
sudo -u sbgh $EDITOR /etc/sbgh/daemon/config.toml
# Add (see config.example.daemon.toml for the documented block):
#   [artifacts]
#   kind     = "s3"
#   endpoint = "https://fsn1.your-objectstorage.com"
#   bucket   = "sbgh-artifacts"
#   region   = "fsn1"
# Do NOT put the access key / secret here — they are env-only (a TOML
# `access_key_id` key is a HARD startup error, like [api].ingest_token).
```

### B3. Credentials — env-only, into the unit's `secrets.env`

```bash
sudo tee -a /etc/sbgh/daemon/secrets.env >/dev/null <<'EOF'
SBGH_ARTIFACTS_S3_ACCESS_KEY_ID=<access-key>
SBGH_ARTIFACTS_S3_SECRET_ACCESS_KEY=<secret-key>
EOF
sudo chmod 0600 /etc/sbgh/daemon/secrets.env
sudo chown sbgh:sbgh /etc/sbgh/daemon/secrets.env

sudo systemctl restart sbgh-daemon
journalctl -u sbgh-daemon -f
#   A bad endpoint fails fast at startup ("building the artifact store") — the
#   daemon validates the S3 client before serving, so a typo surfaces here, not
#   silently per-job.
```

### B4. Verify the live round-trip

Run a benchmark to completion, then confirm the artifacts reached the bucket.
Use any S3 client pointed at your endpoint (`aws` CLI, `mc`, `rclone`):

```bash
# List the run's objects in the bucket (keys are `<job-id>/<relative>`).
# Recursive, so the nested SQLite under appdata/ shows up too.
aws --endpoint-url https://fsn1.your-objectstorage.com \
    s3 ls --recursive s3://sbgh-artifacts/<job-id>/
#   expect: <job-id>/run.json, <job-id>/appdata/stacks-bench.db (the SQLite),
#           <job-id>/stacks-bench (the binary), <job-id>/phase.log

# The local breadcrumb is still written too (retained copy):
ls -laR /var/lib/sbgh/results/<job-id>/

# Fault-tolerance sanity (optional): temporarily point `endpoint` at an
# unreachable host and run a benchmark — the run still completes and the PR
# comment renders; the daemon logs "S3 upload failed; local copy retained".
```

Once the round-trip is confirmed, S3 mode is live. (Fetchable **presigned
URLs** — `signed_url` — exist in the store but have no consumer until the Slack
`0002` / portal `0003` slices; nothing to verify here yet.)

## Rollback

Trivial — there's no migration to unwind.

- **From S3 back to local:** set `kind = "local"` (or remove the `[artifacts]`
  block) and `systemctl restart sbgh-daemon`. Local archiving continues
  unaffected; objects already in the bucket are harmless to leave (or delete via
  your S3 client). No data is lost — the local mirror always held a copy.
- **From v4 back to v3 (code):** revert the code and redeploy
  (`git checkout <v3-tag> && just build && sudo ./scripts/install-daemon.sh`).
  On-disk artifacts are untouched. Caveat: runs completed **under v4** stored
  *key*-style summary pointers, which the v3 reader treats as literal paths and
  won't re-resolve — same low-impact, re-render-only limitation as the
  historical-artifacts note above (the raw content is in `job_result`). New runs
  under the restored v3 work normally.

No DB snapshot is required for this upgrade.
