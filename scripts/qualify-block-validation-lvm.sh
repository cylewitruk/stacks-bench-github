#!/usr/bin/env bash
# One-time v26 writable-snapshot/isolation smoke. Dry-run is the default.
set -euo pipefail

usage() {
    cat >&2 <<'EOF'
usage:
  qualify-block-validation-lvm.sh [--execute] REPORT.md VG ORIGIN_LV ORIGIN_MOUNT

The origin must already be mounted read-only at ORIGIN_MOUNT. The script
creates two disposable read-write thin snapshots, mounts them with the
production XFS safety options, proves origin/peer write isolation, and removes
every resource. It does not benchmark throughput or predict per-assignment
write capacity.
EOF
    exit 2
}

execute=0
if [[ ${1:-} == "--execute" ]]; then
    execute=1
    shift
fi
[[ $# -eq 4 ]] || usage

report=$1
vg=$2
origin_lv=$3
origin_mount=$4

name_re='^[A-Za-z0-9+_.-]+$'
[[ $vg =~ $name_re ]] || { echo "invalid VG name: $vg" >&2; exit 2; }
[[ $origin_lv =~ $name_re ]] || { echo "invalid origin LV name: $origin_lv" >&2; exit 2; }
[[ $origin_mount == /* && -d $origin_mount ]] ||
    { echo "ORIGIN_MOUNT must be an existing absolute directory" >&2; exit 2; }
[[ ! -e $report ]] || { echo "refusing to overwrite report: $report" >&2; exit 2; }

origin_device="/dev/$vg/$origin_lv"

if [[ $execute -eq 0 ]]; then
    cat <<EOF
DRY RUN — no LVM or mount command was executed.
report:        $report
origin:        $origin_device
origin_mount:  $origin_mount
snapshots:     2

Re-run with --execute after draining the worker and reviewing these values.
EOF
    exit 0
fi

[[ $EUID -eq 0 ]] || { echo "--execute must run as root" >&2; exit 2; }
for command in \
    lvs lvcreate lvremove findmnt readlink mount mountpoint umount xargs
do
    command -v "$command" >/dev/null ||
        { echo "required command is unavailable: $command" >&2; exit 2; }
done
mount_source=$(findmnt --noheadings --output SOURCE --target "$origin_mount" | xargs)
mount_options=$(findmnt --noheadings --output OPTIONS --target "$origin_mount" | xargs)
[[ ,$mount_options, == *,ro,* ]] ||
    { echo "origin is not mounted read-only" >&2; exit 1; }
[[ $(readlink -f "$mount_source") == "$(readlink -f "$origin_device")" ]] ||
    { echo "ORIGIN_MOUNT is not backed by the exact configured origin LV" >&2; exit 1; }

origin_row=$(lvs --noheadings --separator '|' --options lv_name,lv_attr "$vg/$origin_lv")
IFS='|' read -r actual_origin origin_attr <<<"$origin_row"
actual_origin=${actual_origin//[[:space:]]/}
origin_attr=${origin_attr//[[:space:]]/}
[[ $actual_origin == "$origin_lv" && ${origin_attr:1:1} == r ]] ||
    { echo "origin LV is not the exact read-only LV" >&2; exit 1; }

mkdir -p "$(dirname "$report")"
scratch=$(mktemp -d /run/sbgh-worker/.v26-lvm-smoke.XXXXXX)
run_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
snapshot_prefix="sbgh-v26-smoke-${run_id}"
declare -a snapshot_names=()
declare -a mount_paths=()
cleaned=0

cleanup_resources() {
    local failed=0
    local index
    for ((index=${#mount_paths[@]} - 1; index >= 0; index--)); do
        if mountpoint -q "${mount_paths[$index]}"; then
            umount "${mount_paths[$index]}" || failed=1
        fi
        rmdir "${mount_paths[$index]}" 2>/dev/null || true
    done
    for ((index=${#snapshot_names[@]} - 1; index >= 0; index--)); do
        lvremove --force "$vg/${snapshot_names[$index]}" || failed=1
    done
    rmdir "$scratch" 2>/dev/null || true
    cleaned=1
    return "$failed"
}

on_exit() {
    local status=$?
    trap - EXIT
    if [[ $cleaned -eq 0 ]] && ! cleanup_resources; then
        echo "smoke cleanup failed; inspect LVs with prefix $snapshot_prefix" >&2
        exit 1
    fi
    exit "$status"
}
trap on_exit EXIT

for index in 0 1; do
    name="${snapshot_prefix}-s$(printf '%04d' "$index")"
    lvcreate \
        --snapshot \
        --permission rw \
        --name "$name" \
        --setactivationskip n \
        "$vg/$origin_lv"
    snapshot_names+=("$name")
    snapshot_attr=$(lvs --noheadings --options lv_attr "$vg/$name" | xargs)
    [[ ${snapshot_attr:1:1} == w ]] ||
        { echo "snapshot $name is not writable (attr=$snapshot_attr)" >&2; exit 1; }

    mount_path="$scratch/shard-$(printf '%04d' "$index")"
    mkdir "$mount_path"
    mount_paths+=("$mount_path")
    mount -t xfs -o nouuid,noatime,nodev,nosuid,noexec "/dev/$vg/$name" "$mount_path"
done

for index in 0 1; do
    other=$((1 - index))
    marker=".sbgh-v26-isolation-${run_id}-${index}"
    printf 'attempt-scoped mutation from snapshot %d\n' "$index" \
        >"${mount_paths[$index]}/$marker"
    [[ ! -e "$origin_mount/$marker" ]] ||
        { echo "snapshot $index mutated the immutable origin" >&2; exit 1; }
    [[ ! -e "${mount_paths[$other]}/$marker" ]] ||
        { echo "snapshot $index mutated snapshot $other" >&2; exit 1; }
done

cleanup_resources
leftovers=$(lvs --noheadings --options lv_name --select \
    "vg_name=$vg && lv_name=~^$snapshot_prefix" | xargs)
[[ -z $leftovers ]] || { echo "smoke left snapshot LVs: $leftovers" >&2; exit 1; }
{
    echo "# v26 block-validation LVM isolation smoke"
    echo
    echo "- captured_at_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "- host: $(hostname -f 2>/dev/null || hostname)"
    echo "- origin: $vg/$origin_lv"
    echo "- origin_read_only: passed"
    echo "- writable_snapshots: 2"
    echo "- origin_write_isolation: passed"
    echo "- peer_write_isolation: passed"
    echo "- leftover_snapshot_lvs: none"
} >"$report"

echo "wrote $report"
