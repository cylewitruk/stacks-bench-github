#!/usr/bin/env bash
# Steer device interrupts onto the HOST cores (off the benchmark cores), so a
# disk/NIC IRQ can't preempt a measured run and add jitter. By DEFAULT this only
# PRINTS the commands (review them, then run yourself); pass --apply to execute.
#
# This is an OPTIONAL, last-mile refinement, and only for NON-managed device
# IRQs. The high-rate managed IRQs (NVMe / multiqueue NIC) are kept off the
# bench cores by the kernel cmdline `isolcpus=...,managed_irq,...` at boot — they
# are NOT movable via /proc (see below). Between that, `nohz_full` (timers), and
# the daemon's `<emulatorpin>` (VM I/O threads on the host cores), the sources
# that matter are already handled. Reach for this only if your serial-vs-
# concurrent A/B shows jitter that tracks with I/O. Keep the chosen host CPUs
# consistent with the worker profile's CPU placement.
#
# NVMe per-queue IRQs (e.g. `nvme0q1`) are kernel-MANAGED — a /proc write
# no-ops, so --apply reports them as skipped; use `managed_irq` in isolcpus
# (kernel cmdline) for those, not this script.
#
# Run `irq-affinity.sh --help` for options.

set -euo pipefail

HOST_CPUS=""
MATCH=""
APPLY=0

usage() {
    cat <<'EOF'
Usage: irq-affinity.sh --host-cpus <cpu-list> [--match <regex>] [--apply]

  --host-cpus <list>  CPUs that MAY service device IRQs (your host/OS cores,
                      i.e. NOT the bench cores). A Linux cpu-list: "4-5",
                      "4,5", "4-5,10-11". REQUIRED.
  --match <regex>     Only steer IRQs whose device name matches this (extended
                      regex, e.g. 'nvme|en|eth'). Default: every numbered IRQ.
  --apply             Actually write the affinities (needs root). Default is to
                      just print the commands for you to review + run.
  -h, --help          This help.

Examples:
  # Print the commands for a 6-core box whose host cores are 4,5:
  scripts/irq-affinity.sh --host-cpus 4-5

  # Apply, but only to storage + network IRQs:
  sudo scripts/irq-affinity.sh --host-cpus 4-5 --match 'nvme|en|eth|virtio' --apply
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --host-cpus) HOST_CPUS="${2:?--host-cpus needs a value}"; shift 2 ;;
        --match)     MATCH="${2:?--match needs a value}"; shift 2 ;;
        --apply)     APPLY=1; shift ;;
        -h|--help)   usage; exit 0 ;;
        *) echo "unknown arg: $1" >&2; usage >&2; exit 2 ;;
    esac
done

[[ -n "$HOST_CPUS" ]] || { echo "error: --host-cpus is required" >&2; usage >&2; exit 2; }
[[ "$HOST_CPUS" =~ ^[0-9]+([-,][0-9]+)*$ ]] \
    || { echo "error: --host-cpus must be a cpu-list like '4-5' or '4,5'" >&2; exit 2; }
if [[ $APPLY -eq 1 && $EUID -ne 0 ]]; then
    echo "error: --apply writes /proc/irq/*/smp_affinity_list; run as root (sudo)" >&2
    exit 1
fi

# ─── irqbalance ──────────────────────────────────────────────────────────
# It auto-spreads IRQs across all cores and would undo manual affinity.
if systemctl is-active --quiet irqbalance 2>/dev/null; then
    if [[ $APPLY -eq 1 ]]; then
        systemctl disable --now irqbalance
        echo "# disabled irqbalance (was active)"
    else
        echo "sudo systemctl disable --now irqbalance   # else it undoes manual affinity"
    fi
fi

# ─── per-IRQ ─────────────────────────────────────────────────────────────
[[ -r /proc/interrupts ]] || { echo "error: /proc/interrupts not readable (Linux only)" >&2; exit 1; }
# /proc/interrupts rows: " <irq>:  <count/CPU...>  <controller>  <name>".
# Only NUMBERED first columns are device IRQs (LOC/NMI/etc. are per-CPU and not
# repinnable); the device name is the trailing field.
moved=0 skipped=0
while IFS= read -r line; do
    irq="${line%%:*}"; irq="${irq//[[:space:]]/}"
    [[ "$irq" =~ ^[0-9]+$ ]] || continue
    name="${line##* }"
    [[ -n "$MATCH" && ! "$name" =~ $MATCH ]] && continue
    aff="/proc/irq/$irq/smp_affinity_list"
    [[ -e "$aff" ]] || continue

    if [[ $APPLY -eq 1 ]]; then
        if echo "$HOST_CPUS" >"$aff" 2>/dev/null; then
            echo "# irq $irq ($name) -> $HOST_CPUS"
            moved=$((moved + 1))
        else
            echo "# irq $irq ($name) -> skipped (kernel-managed; can't repin)"
            skipped=$((skipped + 1))
        fi
    else
        echo "echo $HOST_CPUS | sudo tee /proc/irq/$irq/smp_affinity_list   # $name"
    fi
done </proc/interrupts

if [[ $APPLY -eq 1 ]]; then
    echo "# done: $moved IRQ(s) steered to CPUs $HOST_CPUS, $skipped kernel-managed (skipped)"
    echo "# NOTE: /proc affinities reset on reboot — re-run after boot, or add to a startup unit."
else
    echo "# (review the above, then run them — or re-run with --apply as root)"
    echo "# NOTE: these reset on reboot; some NVMe per-queue IRQs are kernel-managed and won't take."
fi
