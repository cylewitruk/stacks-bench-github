#!/usr/bin/env bash
# One-time v26 writable-snapshot/isolation smoke. Dry-run is the default.
set -euo pipefail

usage() {
    cat >&2 <<'EOF'
usage:
  qualify-block-validation-lvm.sh [--execute] REPORT.md VG ORIGIN_LV ORIGIN_MOUNT

The origin must already be mounted read-only at ORIGIN_MOUNT and contain
.sbgh-dataset-manifest.json plus .sbgh-dataset-files.sha256. The script creates
two disposable read-write thin snapshots, mounts them with the production XFS
safety options, proves origin/peer write isolation, and removes every resource.
It does not benchmark throughput or predict per-assignment write capacity.
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
manifest="$origin_mount/.sbgh-dataset-manifest.json"
file_list="$origin_mount/.sbgh-dataset-files.sha256"

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
    lvs lvcreate lvremove findmnt readlink mount mountpoint umount sha256sum \
    python3 awk xargs
do
    command -v "$command" >/dev/null ||
        { echo "required command is unavailable: $command" >&2; exit 2; }
done
[[ -f $manifest && -f $file_list ]] ||
    { echo "sealed dataset manifest/file-list is missing" >&2; exit 2; }

mount_source=$(findmnt --noheadings --output SOURCE --target "$manifest" | xargs)
mount_options=$(findmnt --noheadings --output OPTIONS --target "$manifest" | xargs)
[[ ,$mount_options, == *,ro,* ]] ||
    { echo "origin manifest is not on a read-only mount" >&2; exit 1; }
[[ $(readlink -f "$mount_source") == "$(readlink -f "$origin_device")" ]] ||
    { echo "ORIGIN_MOUNT is not backed by the exact configured origin LV" >&2; exit 1; }

origin_row=$(lvs --noheadings --separator '|' --options lv_name,lv_attr,lv_tags "$vg/$origin_lv")
IFS='|' read -r actual_origin origin_attr origin_tags <<<"$origin_row"
actual_origin=${actual_origin//[[:space:]]/}
origin_attr=${origin_attr//[[:space:]]/}
origin_tags=${origin_tags//[[:space:]]/}
[[ $actual_origin == "$origin_lv" && ${origin_attr:1:1} == r ]] ||
    { echo "origin LV is not the exact read-only LV" >&2; exit 1; }
for tag in sbgh_sealed sbgh_validated; do
    [[ ,$origin_tags, == *,$tag,* ]] ||
        { echo "origin LV is missing mandatory tag: $tag" >&2; exit 1; }
done

manifest_digest_before=$(sha256sum "$manifest" | awk '{print $1}')
file_list_expected=$(python3 - "$manifest" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    value = json.load(handle).get("files_sha256", "")
if len(value) != 64 or any(character not in "0123456789abcdefABCDEF" for character in value):
    raise SystemExit("manifest files_sha256 is invalid")
print(value.lower())
PY
)
file_list_actual=$(sha256sum "$file_list" | awk '{print $1}')
[[ $file_list_actual == "$file_list_expected" ]] ||
    { echo "dataset file list does not match its manifest" >&2; exit 1; }

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
    [[ $(sha256sum "$mount_path/.sbgh-dataset-manifest.json" | awk '{print $1}') == \
        "$manifest_digest_before" ]] ||
        { echo "snapshot $index manifest differs from origin" >&2; exit 1; }
    [[ $(sha256sum "$mount_path/.sbgh-dataset-files.sha256" | awk '{print $1}') == \
        "$file_list_expected" ]] ||
        { echo "snapshot $index file list differs from its manifest" >&2; exit 1; }
done

for index in 0 1; do
    other=$((1 - index))
    marker=".sbgh-v26-isolation-${run_id}-${index}"
    printf 'attempt-scoped mutation from snapshot %d\n' "$index" \
        >"${mount_paths[$index]}/$marker"
    [[ ! -e "$origin_mount/$marker" ]] ||
        { echo "snapshot $index mutated the sealed origin" >&2; exit 1; }
    [[ ! -e "${mount_paths[$other]}/$marker" ]] ||
        { echo "snapshot $index mutated snapshot $other" >&2; exit 1; }
done

cleanup_resources
leftovers=$(lvs --noheadings --options lv_name --select \
    "vg_name=$vg && lv_name=~^$snapshot_prefix" | xargs)
[[ -z $leftovers ]] || { echo "smoke left snapshot LVs: $leftovers" >&2; exit 1; }
[[ $(sha256sum "$manifest" | awk '{print $1}') == "$manifest_digest_before" ]] ||
    { echo "sealed origin manifest changed during smoke" >&2; exit 1; }

{
    echo "# v26 block-validation LVM isolation smoke"
    echo
    echo "- captured_at_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "- host: $(hostname -f 2>/dev/null || hostname)"
    echo "- origin: $vg/$origin_lv"
    echo "- origin_manifest_sha256: $manifest_digest_before"
    echo "- writable_snapshots: 2"
    echo "- origin_write_isolation: passed"
    echo "- peer_write_isolation: passed"
    echo "- leftover_snapshot_lvs: none"
    echo "- sealed_origin_manifest_unchanged: passed"
} >"$report"

echo "wrote $report"
