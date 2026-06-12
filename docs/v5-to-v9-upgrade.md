# v5 → v9 upgrade runbook

Cutting a **running** v5 deployment up to v9. This single doc spans the
reporting-only iterations (v6–v8) and the one that adds host-side behavior
(v9), because **only v9 touches host operations at all**:

- **v6** Slack live timeline · **v7** reporting-surface trait · **v8** Slack card
  redesign — **pure reporting refinements**. No config, no secrets, no schema, no
  sudoers, no new units. A v5 host that ran [v4-to-v5](./v4-to-v5-upgrade.md)
  reaches v8 by a plain code bump (Part A) with nothing else to do.
- **v9** Release-baseline **binary cache** (item `0025-baseline-binary-cache`).
  An **opt-in**, local, fingerprint-keyed cache of the built `stacks-bench`
  binary: a cache hit **skips the ~5–7 min build VM** for a commit already built
  under the same environment. Off by default.

For a **fresh** install, follow [host-bringup.md](./host-bringup.md). This doc is
only for upgrading a host already running v5 (see
[v4-to-v5-upgrade.md](./v4-to-v5-upgrade.md) if you're still on v4).

> **A very light upgrade.** v6–v8 preserve v5 behavior exactly. v9's cache is
> **opt-in** (`[artifacts.binary_cache].enabled = false` by default), so Part A is
> a behavior-preserving code bump. Part B (turn the cache on) is optional, needs
> **no new sudoers and no new secrets**, and is the only place new config appears.
> The one schema change is the Phase-2 pin policy's **additive** migration
> (`trigger_policy.pinned` / `pinned_until`) — auto-applied at startup, safe on
> rollback (older binaries never select the new columns).

## What actually changes

| Layer | v5 (deployed) | v9 (this upgrade) |
| ---- | ---- | ---- |
| Reporting (v6–v8) | PR comment + check + Slack thread | Richer Slack **card** (live queue, tense timeline, results table + download button) — **render-only, no host change** |
| Build path (v9) | Always build `stacks-bench` in the build VM | **Optional cache**: a fingerprint hit seeds the prebuilt binary onto the source disk and **skips the build VM** |
| Config | `[slack]` (v5) | **+ a new optional `[artifacts.binary_cache]`; default `enabled = false`** |
| Secrets | unchanged | **none new** — the cache is local-only |
| Sudoers / permissions | `losetup` / `mount` / `umount` / `chown` (source-disk provisioning) | **unchanged** — the cache's seed path reuses that exact privileged set |
| Database | — | **one additive migration** — `trigger_policy.pinned` / `pinned_until` (the Phase-2 release-pin policy), auto-applied at startup, idempotent, safe on rollback |
| Disk | per-job artifacts + archive on `/var/lib/sbgh` | **+ the cache** under `/var/lib/sbgh/binary-cache` (`max_size`, default `10G`) |
| Host binary / units / containers | `sbgh-daemon`, the compose stack | **unchanged** (daemon-only code; handler/webhook/smee untouched) |

**Behavior is preserved with the cache off.** A v9 daemon with no
`[artifacts.binary_cache]` section (or `enabled = false`) builds every job exactly
as v8 did — the cache code never runs.

## Part A — deploy v9 (mandatory, cache off)

No config change required. Pull, build, restart.

```bash
cd /path/to/stacks-bench-github
git fetch origin && git checkout main && git pull     # or the v9 tag/commit
just build                                            # → target/release/sbgh-daemon

# install-daemon.sh is idempotent — overwrites the binary + restarts the service.
sudo ./scripts/install-daemon.sh
journalctl -u sbgh-daemon -f
#   expect a normal startup: "migrations applied" (the additive trigger-pin migration),
#   "api listening", and the Slack lines as before. No binary-cache line (it's off).
#   Ctrl-C once it's serving.
```

The handler / webhook / smee containers are unaffected by v9 — no rebuild needed
unless you're pulling other changes.

### Verify (cache off)

Run a PR `/benchmark` (or a Slack `@BenchBot bench …`) and let it complete — the
PR comment / commit check / Slack card render exactly as under v8, and every job
still builds in its build VM. That is the whole of the mandatory upgrade.

## Part B — enable the binary cache (optional)

The cache pays off when you **re-bench the same commit** (a release / integration
ref benched repeatedly): the first run builds + publishes the binary, later runs
of the same `(commit, build environment)` reuse it and skip the build VM.

### B1. Disk headroom

The cache lives on the `sbgh-meta` LV mounted at `/var/lib/sbgh` (the same LV that
holds per-job artifacts + the archive). Each `stacks-bench` binary is ~250–300 MB;
the default `max_size = "10G"` holds ~33–40 of them (pinned entries kept past it,
the rest evicted least-recently-used). Make sure the LV has that headroom — bump
it (`lvextend`) or set a smaller `max_size`. The cache **dir is auto-created**
(`sbgh`-owned); no manual `mkdir`.

### B2. Daemon config — add `[artifacts.binary_cache]`

```bash
sudo -u sbgh $EDITOR /etc/sbgh/daemon/config.toml
# Add (see config.example.daemon.toml for the documented block):
#   [artifacts.binary_cache]
#   enabled  = true
#   max_size = "10G"                       # pins kept past it; rest LRU
#   dir      = "/var/lib/sbgh/binary-cache"
```

No new secrets, and **no sudoers change** — the seed path uses `losetup` /
`mount` / `chown` / `umount`, already allowed for source-disk provisioning
([host-bringup.md](./host-bringup.md) §3).

### B3. Restart

```bash
sudo systemctl restart sbgh-daemon
journalctl -u sbgh-daemon -f   # normal startup; the cache logs only per-job (below)
```

### B4. Smoke-test the skip (the gate before trusting it)

Bench **the same commit twice** (e.g. a Slack `@BenchBot bench … on <release-ref>`,
or a PR `/benchmark`, then repeat):

1. **First run** builds normally and populates the cache — look for
   `binary cache: published built binary`.
2. **Second run** (same commit, same golden image / recipe) **skips the build
   VM** — look for
   `binary cache: reusing cached stacks-bench binary; skipping the build VM`, a
   visibly shorter run (no build phase), and a coherent card / check (Build shows
   done, the bench runs and reports as usual).

If anything is off, the cache is **best-effort**: a miss, an unreadable
`rust-toolchain.toml`, or a seed error logs and **falls back to a normal build** —
it never fails a job.

## Notes

- **Safe to delete.** The cache is purely derived — `rm -rf
  /var/lib/sbgh/binary-cache` (or lower `max_size`) just forces rebuilds.
- **Golden-image changes self-invalidate.** The fingerprint includes the golden
  image's identity, so rebuilding / swapping the image makes prior cached binaries
  miss automatically — no manual purge.
- **Phase 2 pin policy.** The additive `trigger_policy.pinned` / `pinned_until`
  migration ships with this upgrade; pin/unpin a release trigger with
  `sbgh-cli policy trigger pin --id <id> [--until <rfc3339>]` (and see it in
  `policy trigger list` as `pin=…`). The pin's **effect is active**: the daemon
  recomputes the pinned set on startup and after each job run — resolving
  each pinned ref to its current commit via `git ls-remote` (public repos;
  unauthenticated) — and **protects** those binaries from LRU eviction past the
  size budget. An expired `pinned_until` drops the binary back to the LRU tail.
  Resolution is best-effort + all-or-nothing: a transient `ls-remote`/network
  failure preserves the last pinned set rather than clearing it. *Still to come
  (2c): pre-**building** a pinned ref's binary even if it's never been
  benchmarked — today a pin protects an already-built binary, it doesn't yet
  warm a missing one.*

## Rollback

- **Disable the cache (stay on v9):** set
  `[artifacts.binary_cache].enabled = false` (or remove the block) and
  `systemctl restart sbgh-daemon`. Every job builds again, exactly as v8. The
  cache dir is harmless to leave or delete. This is the preferred back-out.
- **From v9 back to v8/v5 (code):** revert and redeploy
  (`git checkout <tag> && just build && sudo ./scripts/install-daemon.sh`). The
  pin migration is **additive** — an older binary never selects
  `trigger_policy.pinned` / `pinned_until`, so the columns simply sit unused (no
  unwind needed; Postgres keeps them, harmless). The cache dir is just files —
  leave or delete it. (If you're rolling back past v5 and Slack created
  `slack_adhoc` jobs, see the [v4-to-v5 enum caveat](./v4-to-v5-upgrade.md#rollback).)

No DB snapshot is required for this upgrade.
