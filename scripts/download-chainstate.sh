#!/usr/bin/env bash
# Download the latest Stacks mainnet chainstate, verify SHA-256, extract into
# a fresh dated LVM-thin volume, and (by default) lvremove older chainstate
# baselines so the new one becomes what the orchestrator picks up.
#
# The orchestrator selects the lexicographically-newest LV matching
# `[lvm].chainstate_base_prefix`, so naming the new LV with today's date
# automatically makes it the active baseline.
#
# Run `download-chainstate.sh --help` for options.

set -euo pipefail

# ─── defaults ──────────────────────────────────────────────────────────
VG=vg0
THINPOOL=thinpool
PREFIX=mainnet-
DATE_STR=$(date -u +%Y-%m-%d)
BASE_SIZE=500G
SCRATCH_SIZE=300G
CONNECTIONS=8
KEEP_OLD=0
URL=https://archive.hiro.so/mainnet/stacks-blockchain/mainnet-stacks-blockchain-latest.tar.zst
SHA_URL=https://archive.hiro.so/mainnet/stacks-blockchain/mainnet-stacks-blockchain-latest.sha256

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Download + verify + extract the latest Stacks mainnet chainstate into a
fresh dated LVM-thin volume.

Options:
  --date YYYY-MM-DD    Suffix for the new base LV. Default: today (UTC).
  --vg NAME            Volume group. Default: ${VG}.
  --thinpool NAME      Thin pool inside the VG. Default: ${THINPOOL}.
  --prefix STR         Chainstate LV name prefix. Default: ${PREFIX}.
                       (Must match \`[lvm].chainstate_base_prefix\` in the
                       orchestrator config.)
  --base-size SIZE     Virtual size of the new base LV. Default: ${BASE_SIZE}.
  --scratch-size SIZE  Virtual size of scratch LV for the .zst. Default: ${SCRATCH_SIZE}.
  --connections N      aria2 parallel connections. Default: ${CONNECTIONS}.
  --keep-old           Skip rotation — leave older <prefix>* LVs in place.
                       Default behaviour is to lvremove any chainstate LV
                       strictly older (lexicographic) than the new one,
                       SKIPPING any that have active snapshots (e.g. a
                       benchmark in flight).
  --url URL            Override archive URL. The .sha256 sidecar is
                       derived as URL_basename minus .tar.zst plus .sha256.
  -h, --help           Show this help and exit.

Safety:
  - The scratch LV is always cleaned up on exit (even on failure).
  - If extraction fails partway through, the new base LV is left in place
    so you can inspect it. Remove it manually before retrying.
  - Rotation never removes an LV that has active snapshots.
EOF
}

# ─── arg parsing ───────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --date)         DATE_STR=$2;          shift 2 ;;
        --vg)           VG=$2;                shift 2 ;;
        --thinpool)     THINPOOL=$2;          shift 2 ;;
        --prefix)       PREFIX=$2;            shift 2 ;;
        --base-size)    BASE_SIZE=$2;         shift 2 ;;
        --scratch-size) SCRATCH_SIZE=$2;      shift 2 ;;
        --connections)  CONNECTIONS=$2;       shift 2 ;;
        --keep-old)     KEEP_OLD=1;           shift ;;
        --url)
            URL=$2
            # Derive sha sidecar from the archive URL: strip `.tar.zst`, add `.sha256`.
            SHA_URL="${2%.tar.zst}.sha256"
            shift 2
            ;;
        -h|--help)      usage; exit 0 ;;
        *)              echo "unknown arg: $1" >&2; usage >&2; exit 2 ;;
    esac
done

BASE_LV="${PREFIX}${DATE_STR}"
SCRATCH_LV="sbgh-scratch-$$"
MOUNT_BASE=/mnt/sbgh-base
MOUNT_SCRATCH=/mnt/sbgh-scratch

# ─── pre-flight ────────────────────────────────────────────────────────
for cmd in aria2c zstd tar curl awk sudo lvs lvcreate lvremove lvchange mkfs.xfs mount umount; do
    command -v "$cmd" >/dev/null \
        || { echo "missing required command: $cmd" >&2; exit 1; }
done

if sudo lvs --noheadings -o lv_name "$VG" 2>/dev/null \
    | awk '{print $1}' | grep -qx "$BASE_LV"; then
    echo "Base LV $VG/$BASE_LV already exists. Pick a different --date or remove it first." >&2
    exit 1
fi

# ─── cleanup trap (scratch is always reaped; base only on partial failure) ─
SCRATCH_CREATED=0
cleanup() {
    local rc=$?
    set +e
    mountpoint -q "$MOUNT_SCRATCH" 2>/dev/null && sudo umount "$MOUNT_SCRATCH"
    mountpoint -q "$MOUNT_BASE"    2>/dev/null && sudo umount "$MOUNT_BASE"
    [[ $SCRATCH_CREATED -eq 1 ]] && sudo lvremove -y "$VG/$SCRATCH_LV"
    sudo lvchange -an "$VG/$BASE_LV" 2>/dev/null
    sudo rmdir "$MOUNT_SCRATCH" "$MOUNT_BASE" 2>/dev/null
    return $rc
}
trap cleanup EXIT

# ─── 1. scratch LV ─────────────────────────────────────────────────────
echo "[1/5] Creating scratch LV $VG/$SCRATCH_LV ($SCRATCH_SIZE virtual)..."
sudo lvcreate -V "$SCRATCH_SIZE" --thin --name "$SCRATCH_LV" "$VG/$THINPOOL"
SCRATCH_CREATED=1
sudo mkfs.xfs -q "/dev/$VG/$SCRATCH_LV"
sudo mkdir -p "$MOUNT_SCRATCH"
sudo mount "/dev/$VG/$SCRATCH_LV" "$MOUNT_SCRATCH"

# ─── 2. expected SHA ───────────────────────────────────────────────────
echo "[2/5] Fetching expected SHA-256 from $SHA_URL..."
EXPECTED_SHA=$(curl --fail --silent --show-error --location "$SHA_URL" | awk '{print $1}')
echo "      expected: $EXPECTED_SHA"

# ─── 3. parallel download + verify ─────────────────────────────────────
echo "[3/5] Downloading + verifying via aria2 ($CONNECTIONS connections)..."
sudo aria2c \
    --dir="$MOUNT_SCRATCH" \
    --out=archive.tar.zst \
    --max-connection-per-server="$CONNECTIONS" \
    --split="$CONNECTIONS" \
    --min-split-size=64M \
    --file-allocation=falloc \
    --checksum=sha-256="$EXPECTED_SHA" \
    --summary-interval=30 \
    "$URL"

# ─── 4. create + populate the new base LV ──────────────────────────────
echo "[4/5] Creating base LV $VG/$BASE_LV and extracting..."
sudo lvcreate -V "$BASE_SIZE" --thin --name "$BASE_LV" "$VG/$THINPOOL"
sudo mkfs.xfs -q "/dev/$VG/$BASE_LV"
sudo mkdir -p "$MOUNT_BASE"
sudo mount "/dev/$VG/$BASE_LV" "$MOUNT_BASE"

sudo zstd --decompress --stdout "$MOUNT_SCRATCH/archive.tar.zst" \
    | sudo tar --extract --file - --directory "$MOUNT_BASE"

sudo umount "$MOUNT_BASE"
sudo lvchange -an "$VG/$BASE_LV"

# ─── 5. rotate older baselines ─────────────────────────────────────────
if [[ $KEEP_OLD -eq 1 ]]; then
    echo "[5/5] --keep-old set; not rotating."
else
    echo "[5/5] Rotating out older chainstate baselines..."
    # All <prefix>* LVs strictly older (lex) than the new one.
    OLD_LVS=$(sudo lvs --noheadings -o lv_name "$VG" 2>/dev/null \
        | awk '{print $1}' \
        | grep -E "^${PREFIX}" \
        | awk -v cur="$BASE_LV" '$0 < cur')

    if [[ -z "$OLD_LVS" ]]; then
        echo "      nothing to rotate."
    fi
    for lv in $OLD_LVS; do
        # Refuse to remove an LV that has active snapshots — typically means
        # a benchmark run is still using it.
        snaps=$(sudo lvs --noheadings -o lv_name --select "origin=$lv" "$VG" 2>/dev/null \
            | awk '{print $1}' | tr '\n' ' ')
        if [[ -n "${snaps// /}" ]]; then
            echo "      skipping $VG/$lv (has active snapshots: $snaps)"
            continue
        fi
        echo "      removing $VG/$lv"
        sudo lvremove -y "$VG/$lv"
    done
fi

echo "Done. New chainstate base: $VG/$BASE_LV"
