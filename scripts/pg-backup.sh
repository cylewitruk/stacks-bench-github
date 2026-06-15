#!/usr/bin/env bash
# Back up the daemon's Postgres database to a single `.tar.zst` artifact and
# prune old backups, keeping the newest N. Designed to run unattended from a
# systemd timer (see systemd/sbgh-pg-backup.{service,timer}), but also runnable
# by hand.
#
# Mechanics: `pg_dump -Ft` (tar archive format) is streamed out of the
# `sbgh-postgres` container and compressed with zstd on the host — yielding a
# `<dir>/sbgh-<db>-<utc-timestamp>.tar.zst` that `pg_restore` can read after a
# `zstd -d`. The dump goes to a `.part` temp first and is atomically renamed
# only after it verifies, so a crashed/partial run never leaves a file that
# looks like a good backup.
#
# Auth: pg_dump runs INSIDE the container over the local unix socket (trust
# auth, same as the compose healthcheck), so no password handling is needed.
#
# Config — env vars (the unit sets these) or flags (flags win):
#   PG_CONTAINER  --container   Postgres container name      (sbgh-postgres)
#   PG_USER       --user        Postgres role                (sbgh)
#   PG_DB         --db          Database to dump             (sbgh)
#   BACKUP_DIR    --dir         Output directory            (/var/lib/sbgh/backups)
#   RETENTION     --retention   Backups to keep (>=1)        (7)
#   ZSTD_LEVEL    --level       zstd level 1..19             (19)
#
# Usage:
#   sudo ./scripts/pg-backup.sh
#   sudo ./scripts/pg-backup.sh --dir /mnt/backups --retention 14
#   sudo RETENTION=30 ./scripts/pg-backup.sh

set -euo pipefail

PG_CONTAINER="${PG_CONTAINER:-sbgh-postgres}"
PG_USER="${PG_USER:-sbgh}"
PG_DB="${PG_DB:-sbgh}"
BACKUP_DIR="${BACKUP_DIR:-/var/lib/sbgh/backups}"
RETENTION="${RETENTION:-7}"
ZSTD_LEVEL="${ZSTD_LEVEL:-19}"

usage() {
    cat <<'EOF'
Usage: pg-backup.sh [--container N] [--user U] [--db D] [--dir PATH]
                    [--retention K] [--level L]

Dumps the Postgres DB to <dir>/sbgh-<db>-<utc>.tar.zst and keeps the newest
K backups. All options also read from the matching env var (flags win):
PG_CONTAINER, PG_USER, PG_DB, BACKUP_DIR, RETENTION, ZSTD_LEVEL.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --container) PG_CONTAINER="${2:?--container needs a value}"; shift 2 ;;
        --user)      PG_USER="${2:?--user needs a value}"; shift 2 ;;
        --db)        PG_DB="${2:?--db needs a value}"; shift 2 ;;
        --dir)       BACKUP_DIR="${2:?--dir needs a value}"; shift 2 ;;
        --retention) RETENTION="${2:?--retention needs a value}"; shift 2 ;;
        --level)     ZSTD_LEVEL="${2:?--level needs a value}"; shift 2 ;;
        -h|--help)   usage; exit 0 ;;
        *) echo "unknown arg: $1" >&2; usage >&2; exit 2 ;;
    esac
done

[[ "$RETENTION" =~ ^[0-9]+$ && "$RETENTION" -ge 1 ]] \
    || { echo "error: --retention must be an integer >= 1 (got '$RETENTION')" >&2; exit 2; }
[[ "$ZSTD_LEVEL" =~ ^[0-9]+$ && "$ZSTD_LEVEL" -ge 1 && "$ZSTD_LEVEL" -le 19 ]] \
    || { echo "error: --level must be 1..19 (got '$ZSTD_LEVEL')" >&2; exit 2; }

command -v docker >/dev/null || { echo "error: docker not found on PATH" >&2; exit 1; }
command -v zstd   >/dev/null || { echo "error: zstd not found on PATH (apt-get install zstd)" >&2; exit 1; }

# Container must be up, else pg_dump can't connect.
running=$(docker inspect -f '{{.State.Running}}' "$PG_CONTAINER" 2>/dev/null || echo "missing")
[[ "$running" == "true" ]] \
    || { echo "error: container '$PG_CONTAINER' is not running (state: $running)" >&2; exit 1; }

mkdir -p "$BACKUP_DIR"
[[ -w "$BACKUP_DIR" ]] || { echo "error: backup dir '$BACKUP_DIR' is not writable" >&2; exit 1; }

TS=$(date -u +%Y%m%dT%H%M%SZ)
DEST="$BACKUP_DIR/sbgh-$PG_DB-$TS.tar.zst"
PART="$DEST.part"
# A stray .part from a previously-killed run would block this one.
rm -f -- "$PART"

echo "[1/4] Dumping '$PG_DB' from '$PG_CONTAINER' -> ${DEST##*/} (zstd -$ZSTD_LEVEL)..."
# pipefail (from set -o above) makes a pg_dump failure fail the pipeline even
# though zstd would still exit 0 on the truncated stream. No -t/-i on exec:
# this runs without a TTY under systemd.
if ! docker exec "$PG_CONTAINER" pg_dump -U "$PG_USER" -d "$PG_DB" -Ft \
        | zstd -c -q -T0 "-$ZSTD_LEVEL" > "$PART"; then
    echo "error: pg_dump | zstd failed; discarding partial $PART" >&2
    rm -f -- "$PART"
    exit 1
fi

echo "[2/4] Verifying archive integrity..."
# Decompress + list the tar in one pass: proves the zstd stream is intact AND
# that it's a real pg_dump tar (its TOC is always 'toc.dat'). Captured rather
# than piped to grep -q so an early pipe close can't SIGPIPE-fail the check.
listing=$(zstd -dc "$PART" 2>/dev/null | tar -tf - 2>/dev/null || true)
if ! grep -qx 'toc.dat' <<<"$listing"; then
    echo "error: '$PART' is not a valid pg_dump tar (no toc.dat); discarding" >&2
    rm -f -- "$PART"
    exit 1
fi

# Atomic publish: same-dir rename, so readers never see a half-written backup.
mv -f -- "$PART" "$DEST"
size=$(du -h "$DEST" | cut -f1)
echo "[3/4] Wrote $DEST ($size)."

echo "[4/4] Pruning to newest $RETENTION..."
shopt -s nullglob
backups=( "$BACKUP_DIR"/sbgh-"$PG_DB"-*.tar.zst )
shopt -u nullglob
if (( ${#backups[@]} > RETENTION )); then
    # Filenames embed a sortable UTC stamp, so a reverse name sort is
    # newest-first; everything past index RETENTION is older surplus.
    mapfile -t sorted < <(printf '%s\n' "${backups[@]}" | sort -r)
    for old in "${sorted[@]:RETENTION}"; do
        rm -f -- "$old"
        echo "  pruned $(basename "$old")"
    done
else
    echo "  ${#backups[@]} backup(s) on disk, nothing to prune."
fi

echo "Done."
