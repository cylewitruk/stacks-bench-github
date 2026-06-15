#!/usr/bin/env bash
# Restore a pg-backup.sh `.tar.zst` into a THROWAWAY scratch database inside the
# postgres container and validate it — without ever touching the live `sbgh`
# database. The scratch DB is dropped on exit (success or failure) via a trap.
#
# Two jobs in one pass:
#   1. Proves the backup is actually restorable (zstd intact + pg_restore clean).
#   2. Doubles as the v14 (0037) migration dry-run: if the restored copy predates
#      v14 (no `benchmark_group` table), it applies the v14 migration to the
#      scratch copy and then runs the group->spec->run checklist on REAL data —
#      the exact backfill the test suite can't exercise. If the backup already
#      carries the v14 schema, it just validates it as-is.
#
# Checklist (all must hold): no NULL group/spec/run FK columns; jobs == groups
# == specs; build_steps == jobs; run_steps == measured (non-build_only) jobs; no
# orphan specs/runs.
#
# Auth: every psql/pg_restore runs INSIDE the container over the local socket
# (trust), same as pg-backup.sh — no password handling.
#
# Config — env or flags (flags win):
#   PG_CONTAINER  --container   Postgres container name   (sbgh-postgres)
#   PG_USER       --user        Postgres role (superuser) (sbgh)
#   PG_DB         --db          LIVE db name, guarded against (sbgh)
#   BACKUP_DIR    --dir         where to find newest backup (/var/lib/sbgh/backups)
#                 --migration   v14 SQL to dry-run (default: repo migrations/…)
#                 --no-migrate  never apply a migration; validate as-restored
#                 --keep        don't drop the scratch DB (for manual poking)
#
# Usage:
#   sudo ./scripts/pg-restore-check.sh                       # newest backup
#   sudo ./scripts/pg-restore-check.sh /var/lib/sbgh/backups/sbgh-sbgh-….tar.zst
#   sudo ./scripts/pg-restore-check.sh --keep <file>

set -euo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)

PG_CONTAINER="${PG_CONTAINER:-sbgh-postgres}"
PG_USER="${PG_USER:-sbgh}"
PG_DB="${PG_DB:-sbgh}"
BACKUP_DIR="${BACKUP_DIR:-/var/lib/sbgh/backups}"
MIGRATION="$REPO_ROOT/migrations/20260615000001_v14_benchmark_groups.sql"
BACKUP=""
NO_MIGRATE=0
KEEP=0

usage() {
    cat <<'EOF'
Usage: pg-restore-check.sh [--container N] [--user U] [--db D] [--dir PATH]
                           [--migration SQL] [--no-migrate] [--keep] [BACKUP]

Restores BACKUP (a pg-backup.sh .tar.zst; default: newest in --dir) into a
throwaway scratch DB inside the container, optionally dry-runs the v14
migration, and runs the group->spec->run checklist. Never touches the live db.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --container) PG_CONTAINER="${2:?--container needs a value}"; shift 2 ;;
        --user)      PG_USER="${2:?--user needs a value}"; shift 2 ;;
        --db)        PG_DB="${2:?--db needs a value}"; shift 2 ;;
        --dir)       BACKUP_DIR="${2:?--dir needs a value}"; shift 2 ;;
        --migration) MIGRATION="${2:?--migration needs a value}"; shift 2 ;;
        --no-migrate) NO_MIGRATE=1; shift ;;
        --keep)      KEEP=1; shift ;;
        -h|--help)   usage; exit 0 ;;
        -*) echo "unknown arg: $1" >&2; usage >&2; exit 2 ;;
        *) [[ -z "$BACKUP" ]] || { echo "error: multiple backups given" >&2; exit 2; }
           BACKUP="$1"; shift ;;
    esac
done

command -v docker >/dev/null || { echo "error: docker not found on PATH" >&2; exit 1; }
command -v zstd   >/dev/null || { echo "error: zstd not found on PATH" >&2; exit 1; }

# Default to the newest backup in BACKUP_DIR.
if [[ -z "$BACKUP" ]]; then
    shopt -s nullglob
    found=( "$BACKUP_DIR"/sbgh-*.tar.zst )
    shopt -u nullglob
    (( ${#found[@]} > 0 )) || { echo "error: no .tar.zst in $BACKUP_DIR (pass one explicitly)" >&2; exit 1; }
    mapfile -t sorted < <(printf '%s\n' "${found[@]}" | sort -r)
    BACKUP="${sorted[0]}"
fi
[[ -f "$BACKUP" ]] || { echo "error: backup not found: $BACKUP" >&2; exit 1; }

running=$(docker inspect -f '{{.State.Running}}' "$PG_CONTAINER" 2>/dev/null || echo "missing")
[[ "$running" == "true" ]] \
    || { echo "error: container '$PG_CONTAINER' is not running (state: $running)" >&2; exit 1; }

# Scratch identifiers (valid SQL ident: lowercase + underscores). Guard hard
# against ever naming the live DB.
SCRATCH="sbgh_restorecheck_$(date -u +%Y%m%d_%H%M%S)_$$"
[[ "$SCRATCH" != "$PG_DB" ]] || { echo "error: refusing — scratch name equals live db '$PG_DB'" >&2; exit 1; }
CTAR="/tmp/$SCRATCH.tar"
CMIG="/tmp/$SCRATCH.migration.sql"

psql_scratch() { docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d "$SCRATCH" -tAqc "$1"; }

cleanup() {
    docker exec "$PG_CONTAINER" rm -f "$CTAR" "$CMIG" 2>/dev/null || true
    if (( KEEP == 1 )); then
        echo
        echo "NOTE: kept scratch db '$SCRATCH' (--keep). Inspect:"
        echo "      docker exec -it $PG_CONTAINER psql -U $PG_USER -d $SCRATCH"
        echo "      drop it:  docker exec $PG_CONTAINER dropdb -U $PG_USER --force $SCRATCH"
    else
        docker exec "$PG_CONTAINER" dropdb -U "$PG_USER" --if-exists --force "$SCRATCH" 2>/dev/null || true
    fi
}
trap cleanup EXIT

echo "Backup:   $BACKUP ($(du -h "$BACKUP" | cut -f1))"
echo "Scratch:  $SCRATCH (in $PG_CONTAINER; dropped on exit)"
echo

echo "[1/4] Creating scratch DB and restoring backup..."
docker exec "$PG_CONTAINER" createdb -U "$PG_USER" "$SCRATCH"
# Stream-decompress the .tar.zst straight into the container (no host temp);
# pipefail catches a corrupt zstd stream. pg_restore -Ft needs a seekable file,
# hence the in-container temp rather than a stdin pipe.
zstd -dc "$BACKUP" | docker exec -i "$PG_CONTAINER" sh -c "cat > '$CTAR'"
if ! restore_log=$(docker exec "$PG_CONTAINER" pg_restore -U "$PG_USER" -d "$SCRATCH" \
        --no-owner --no-privileges "$CTAR" 2>&1); then
    echo "FAIL: pg_restore reported errors:" >&2
    echo "$restore_log" | sed 's/^/    /' >&2
    exit 1
fi
[[ -z "$restore_log" ]] || { echo "  pg_restore warnings:"; echo "$restore_log" | sed 's/^/    /'; }
echo "  restored cleanly."

echo "[2/4] Checking schema state..."
has_group=$(psql_scratch "SELECT to_regclass('public.benchmark_group') IS NOT NULL")
if [[ "$has_group" == "t" ]]; then
    echo "  backup already carries the v14 schema — validating as-is."
elif (( NO_MIGRATE == 1 )); then
    echo "  pre-v14 backup and --no-migrate set; nothing to validate. Done."
    exit 0
else
    [[ -f "$MIGRATION" ]] || { echo "error: migration file not found: $MIGRATION" >&2; exit 1; }
    jobs_before=$(psql_scratch "SELECT count(*) FROM job")
    echo "  pre-v14 backup ($jobs_before existing jobs) — dry-running the v14 migration..."
    docker cp "$MIGRATION" "$PG_CONTAINER:$CMIG" >/dev/null
    # --single-transaction + ON_ERROR_STOP: the whole migration (DDL + backfill +
    # SET NOT NULL) applies atomically or rolls back, exactly like sqlx will.
    if ! mig_log=$(docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d "$SCRATCH" \
            --single-transaction -v ON_ERROR_STOP=1 -f "$CMIG" 2>&1); then
        echo "FAIL: v14 migration did not apply to the restored copy:" >&2
        echo "$mig_log" | sed 's/^/    /' >&2
        exit 1
    fi
    echo "  v14 migration applied to $jobs_before jobs."
fi

echo "[3/4] Running group -> spec -> run checklist..."
counts=$(psql_scratch "
SELECT 'ungrouped='   ||count(*) FROM job WHERE benchmark_group_id IS NULL OR benchmark_spec_id IS NULL OR benchmark_run_index IS NULL
UNION ALL SELECT 'jobs='        ||count(*) FROM job
UNION ALL SELECT 'groups='      ||count(*) FROM benchmark_group
UNION ALL SELECT 'specs='       ||count(*) FROM benchmark_spec
UNION ALL SELECT 'build_steps=' ||count(*) FROM benchmark_workflow_step WHERE step_kind='build'
UNION ALL SELECT 'run_steps='   ||count(*) FROM benchmark_workflow_step WHERE step_kind='run'
UNION ALL SELECT 'measured='    ||count(*) FROM job WHERE task_kind <> 'build_only'
UNION ALL SELECT 'orphan_specs='||count(*) FROM benchmark_spec s LEFT JOIN benchmark_group g ON g.id=s.benchmark_group_id WHERE g.id IS NULL
UNION ALL SELECT 'orphan_runs=' ||count(*) FROM job j LEFT JOIN benchmark_spec s ON s.id=j.benchmark_spec_id WHERE s.id IS NULL
")

# A single query returns all 9 rows together, so if 'jobs=' is present they all
# are — guard against a failed/empty read silently passing as 0==0.
[[ "$counts" == *"jobs="* ]] || { echo "FAIL: could not read counts from scratch DB" >&2; exit 1; }

declare -A C
while IFS='=' read -r k v; do [[ -n "$k" ]] && C[$k]="$v"; done <<<"$counts"

printf '  counts: jobs=%s groups=%s specs=%s | build_steps=%s | run_steps=%s measured=%s\n' \
    "${C[jobs]}" "${C[groups]}" "${C[specs]}" "${C[build_steps]}" "${C[run_steps]}" "${C[measured]}"
if [[ "${C[jobs]}" == "0" ]]; then
    echo "  note: 0 jobs in this backup — invariants hold trivially (near-empty DB)."
fi
echo

overall_ok=1
report() { # label, bool(1=pass)
    if (( $2 )); then echo "  [PASS] $1"; else echo "  [FAIL] $1"; overall_ok=0; fi
}
# Bare array refs in $(( … )); an unset element is 0, but the guard above
# guarantees every key is present here.
report "no NULL group/spec/run columns (ungrouped=${C[ungrouped]})"     "$(( C[ungrouped]==0 ))"
report "jobs == groups (${C[jobs]} == ${C[groups]})"                    "$(( C[jobs]==C[groups] ))"
report "jobs == specs  (${C[jobs]} == ${C[specs]})"                     "$(( C[jobs]==C[specs] ))"
report "build_steps == jobs (${C[build_steps]} == ${C[jobs]})"          "$(( C[build_steps]==C[jobs] ))"
report "run_steps == measured (${C[run_steps]} == ${C[measured]})"      "$(( C[run_steps]==C[measured] ))"
report "no orphan specs (orphan_specs=${C[orphan_specs]})"              "$(( C[orphan_specs]==0 ))"
report "no orphan runs  (orphan_runs=${C[orphan_runs]})"                "$(( C[orphan_runs]==0 ))"

echo
echo "[4/4] Result:"
if (( overall_ok == 1 )); then
    echo "  PASS — backup restores and the v14 group/spec/run model is consistent."
    exit 0
else
    echo "  FAIL — see the [FAIL] lines above." >&2
    exit 1
fi
