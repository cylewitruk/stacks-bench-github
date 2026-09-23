#!/usr/bin/env bash
# Restore a pg-backup.sh `.tar.zst` into a THROWAWAY scratch database inside the
# postgres container and validate it — without ever touching the live `sbgh`
# database. The scratch DB is dropped on exit (success or failure) via a trap.
#
# Two jobs in one pass:
#   1. Proves the backup is actually restorable (zstd intact + pg_restore clean).
#   2. Validates the submission->spec->run relationships using either the
#      current task-neutral names or the pre-v27 benchmark names. For a backup
#      predating v14, it can still apply the v14 migration before validation.
#
# Checklist (all must hold): jobs have submission/spec/run identities; every
# submission has a job; build/run steps match their specs; and no relationship
# is orphaned or crosses submission identities.
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
migration, and checks submission/spec/run relationships. Never touches the
live db.
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
has_submission=$(psql_scratch "SELECT to_regclass('public.task_submission') IS NOT NULL")
has_group=$(psql_scratch "SELECT to_regclass('public.benchmark_group') IS NOT NULL")
if [[ "$has_submission" == "t" ]]; then
    echo "  backup carries the current task-submission schema — validating as-is."
    submission_table=task_submission
    spec_table=task_spec
    step_table=task_workflow_step
    job_submission_column=task_submission_id
    job_spec_column=task_spec_id
    job_run_column=task_run_index
    spec_submission_column=task_submission_id
    step_submission_column=task_submission_id
    step_spec_column=task_spec_id
elif [[ "$has_group" == "t" ]]; then
    echo "  backup carries the legacy v14-v26 schema — validating as-is."
    submission_table=benchmark_group
    spec_table=benchmark_spec
    step_table=benchmark_workflow_step
    job_submission_column=benchmark_group_id
    job_spec_column=benchmark_spec_id
    job_run_column=benchmark_run_index
    spec_submission_column=benchmark_group_id
    step_submission_column=benchmark_group_id
    step_spec_column=benchmark_spec_id
elif (( NO_MIGRATE == 1 )); then
    echo "  pre-v14 backup and --no-migrate set; restore passed but relationship validation is unavailable."
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
    submission_table=benchmark_group
    spec_table=benchmark_spec
    step_table=benchmark_workflow_step
    job_submission_column=benchmark_group_id
    job_spec_column=benchmark_spec_id
    job_run_column=benchmark_run_index
    spec_submission_column=benchmark_group_id
    step_submission_column=benchmark_group_id
    step_spec_column=benchmark_spec_id
fi

echo "[3/4] Running submission -> spec -> run checklist..."
counts=$(psql_scratch "
SELECT 'unlinked_jobs=' ||count(*) FROM job
 WHERE $job_submission_column IS NULL OR $job_spec_column IS NULL OR $job_run_column IS NULL
UNION ALL SELECT 'jobs=' ||count(*) FROM job
UNION ALL SELECT 'submissions=' ||count(*) FROM $submission_table
UNION ALL SELECT 'specs=' ||count(*) FROM $spec_table
UNION ALL SELECT 'build_steps=' ||count(*) FROM $step_table WHERE step_kind='build'
UNION ALL SELECT 'run_steps=' ||count(*) FROM $step_table WHERE step_kind='run'
UNION ALL SELECT 'runnable_specs=' ||count(*) FROM $spec_table WHERE task_kind <> 'build_only'
UNION ALL SELECT 'empty_submissions=' ||count(*)
  FROM $submission_table submission
  LEFT JOIN job ON job.$job_submission_column = submission.id
 WHERE job.id IS NULL
UNION ALL SELECT 'orphan_specs=' ||count(*)
  FROM $spec_table spec
  LEFT JOIN $submission_table submission ON submission.id = spec.$spec_submission_column
 WHERE submission.id IS NULL
UNION ALL SELECT 'orphan_jobs=' ||count(*)
  FROM job
  LEFT JOIN $submission_table submission ON submission.id = job.$job_submission_column
  LEFT JOIN $spec_table spec ON spec.id = job.$job_spec_column
 WHERE submission.id IS NULL OR spec.id IS NULL
UNION ALL SELECT 'cross_submission_jobs=' ||count(*)
  FROM job
  JOIN $spec_table spec ON spec.id = job.$job_spec_column
 WHERE spec.$spec_submission_column <> job.$job_submission_column
UNION ALL SELECT 'orphan_steps=' ||count(*)
  FROM $step_table step
  LEFT JOIN $submission_table submission ON submission.id = step.$step_submission_column
  LEFT JOIN $spec_table spec ON spec.id = step.$step_spec_column
 WHERE submission.id IS NULL OR (step.$step_spec_column IS NOT NULL AND spec.id IS NULL)
UNION ALL SELECT 'cross_submission_steps=' ||count(*)
  FROM $step_table step
  JOIN $spec_table spec ON spec.id = step.$step_spec_column
 WHERE spec.$spec_submission_column <> step.$step_submission_column
")

declare -A C
while IFS='=' read -r k v; do [[ -n "$k" ]] && C[$k]="$v"; done <<<"$counts"

required_counts=(
    unlinked_jobs jobs submissions specs build_steps run_steps runnable_specs
    empty_submissions orphan_specs orphan_jobs cross_submission_jobs
    orphan_steps cross_submission_steps
)
for key in "${required_counts[@]}"; do
    [[ -n "${C[$key]+set}" ]] || {
        echo "FAIL: relationship query omitted '$key'" >&2
        exit 1
    }
done

printf '  counts: submissions=%s specs=%s jobs=%s | build_steps=%s | run_steps=%s runnable_specs=%s\n' \
    "${C[submissions]}" "${C[specs]}" "${C[jobs]}" "${C[build_steps]}" \
    "${C[run_steps]}" "${C[runnable_specs]}"
if [[ "${C[jobs]}" == "0" ]]; then
    echo "  note: 0 jobs in this backup — invariants hold trivially (near-empty DB)."
fi
echo

overall_ok=1
report() { # label, bool(1=pass)
    if (( $2 )); then echo "  [PASS] $1"; else echo "  [FAIL] $1"; overall_ok=0; fi
}
# Bare array refs in $(( … )); the guard above guarantees every key is present.
report "all jobs have submission/spec/run identities (unlinked_jobs=${C[unlinked_jobs]})" "$(( C[unlinked_jobs]==0 ))"
report "every submission has a job (empty_submissions=${C[empty_submissions]})" "$(( C[empty_submissions]==0 ))"
report "build_steps == specs (${C[build_steps]} == ${C[specs]})" "$(( C[build_steps]==C[specs] ))"
report "run_steps == runnable_specs (${C[run_steps]} == ${C[runnable_specs]})" "$(( C[run_steps]==C[runnable_specs] ))"
report "no orphan specs (orphan_specs=${C[orphan_specs]})" "$(( C[orphan_specs]==0 ))"
report "no orphan jobs (orphan_jobs=${C[orphan_jobs]})" "$(( C[orphan_jobs]==0 ))"
report "jobs and specs share submission identity (cross_submission_jobs=${C[cross_submission_jobs]})" "$(( C[cross_submission_jobs]==0 ))"
report "no orphan workflow steps (orphan_steps=${C[orphan_steps]})" "$(( C[orphan_steps]==0 ))"
report "steps and specs share submission identity (cross_submission_steps=${C[cross_submission_steps]})" "$(( C[cross_submission_steps]==0 ))"

echo
echo "[4/4] Result:"
if (( overall_ok == 1 )); then
    echo "  PASS — backup restores and the submission/spec/run model is consistent."
    exit 0
else
    echo "  FAIL — see the [FAIL] lines above." >&2
    exit 1
fi
