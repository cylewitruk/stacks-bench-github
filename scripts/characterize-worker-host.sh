#!/usr/bin/env bash
# Non-destructive worker qualification. Writes one auditable Markdown report.
set -euo pipefail

if [[ $# -ne 2 ]]; then
    echo "usage: $0 OUTPUT.md WORKSPACE_FILESYSTEM_PATH" >&2
    exit 2
fi

output=$1
workspace=$2
if [[ ! -d "$workspace" || ! -w "$workspace" ]]; then
    echo "workspace must be an existing writable directory: $workspace" >&2
    exit 2
fi

scratch=$(mktemp -d "$workspace/.sbgh-characterize.XXXXXX")
cleanup() {
    rm -rf -- "$scratch"
}
trap cleanup EXIT

source_file="$scratch/source"
clone_file="$scratch/clone"
printf 'canonical\n' > "$source_file"
if ! cp --reflink=always "$source_file" "$clone_file"; then
    echo "required cp --reflink=always operation failed" >&2
    exit 1
fi
printf 'mutated\n' > "$clone_file"
if [[ $(<"$source_file") != "canonical" ]]; then
    echo "CoW mutation isolation failed" >&2
    exit 1
fi

{
    echo "# SBGH worker host characterization"
    echo
    echo "- captured_at_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "- hostname: $(hostname -f 2>/dev/null || hostname)"
    echo "- kernel: $(uname -srmo)"
    echo "- workspace: $workspace"
    echo "- reflink_mutation_isolation: passed"
    echo
    echo "## CPU and NUMA"
    echo
    echo '```text'
    lscpu
    command -v numactl >/dev/null && numactl --hardware || true
    echo '```'
    echo
    echo "## Memory"
    echo
    echo '```text'
    free -h
    echo '```'
    echo
    echo "## Storage and mounts"
    echo
    echo '```text'
    lsblk -e7 -o NAME,MODEL,SERIAL,SIZE,ROTA,TYPE,FSTYPE,MOUNTPOINTS
    findmnt -T "$workspace" -o TARGET,SOURCE,FSTYPE,OPTIONS
    df -hT "$workspace"
    echo '```'
    if command -v fio >/dev/null; then
        echo
        echo "## Bounded fio sample"
        echo
        echo '```text'
        fio --name=sbgh-characterize --directory="$scratch" --size=4G \
            --rw=randread --bs=1M --iodepth=32 --numjobs=1 \
            --direct=1 --runtime=30 --time_based --group_reporting
        echo '```'
    fi
} > "$output"

echo "wrote $output"
