#!/usr/bin/env bash
# Download the latest Stacks mainnet chainstate, verify SHA-256, extract into
# a fresh dated LVM-thin volume, and (by default) lvremove older chainstate
# baselines so the new one becomes what the worker picks up. The compressed
# bytes are hashed while ripcat streams them into the extractor.
#
# The worker selects the lexicographically-newest LV matching
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
BASE_SIZE=1T
CONNECTIONS=8
WINDOW_MIB=512
RETRIES=20
SPOOL_DIR=
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
                       worker config.)
  --base-size SIZE     Virtual size of the new base LV. Default: ${BASE_SIZE}.
  --connections N      Parallel ripcat range requests. Default: ${CONNECTIONS}.
  --window-mib N       Maximum temporary spool window in MiB. Default: ${WINDOW_MIB}.
  --retries N          Retries after the first request per range. Default: ${RETRIES}.
  --spool-dir DIR      Parent for ripcat's private temporary directory.
                        Default: the system temporary directory.
  --keep-old           Skip rotation — leave older <prefix>* LVs in place.
                       Default behaviour is to lvremove any chainstate LV
                       strictly older (lexicographic) than the new one,
                       SKIPPING any that have active snapshots (e.g. a
                       benchmark in flight).
  --url URL            Override archive URL. The .sha256 sidecar is
                       derived as URL_basename minus .tar.zst plus .sha256.
  -h, --help           Show this help and exit.

Safety:
  - The base is published only after extraction and final SHA-256 match.
  - Failed ranges retry without replaying already decoded chunks. Restarting
    the script still restarts extraction from byte zero.
  - Failed downloads, verification, or extraction remove the partial base LV.
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
        --connections)  CONNECTIONS=$2;       shift 2 ;;
        --window-mib)   WINDOW_MIB=$2;        shift 2 ;;
        --retries)      RETRIES=$2;           shift 2 ;;
        --spool-dir)    SPOOL_DIR=$2;         shift 2 ;;
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
MOUNT_BASE=/mnt/sbgh-base

# ─── pre-flight ────────────────────────────────────────────────────────
required_commands=(ripcat zstd tar curl awk grep sudo lvs lvcreate lvremove lvchange mkfs.xfs mount mountpoint umount mktemp mkfifo sha256sum tee)
for cmd in "${required_commands[@]}"; do
    command -v "$cmd" >/dev/null \
        || { echo "missing required command: $cmd" >&2; exit 1; }
done

[[ $CONNECTIONS =~ ^[1-9][0-9]*$ ]] \
    || { echo "--connections must be a positive integer" >&2; exit 2; }
[[ $WINDOW_MIB =~ ^[1-9][0-9]*$ ]] \
    || { echo "--window-mib must be a positive integer" >&2; exit 2; }
[[ $RETRIES =~ ^[0-9]+$ ]] \
    || { echo "--retries must be a non-negative integer" >&2; exit 2; }
if [[ -n $SPOOL_DIR && ! -d $SPOOL_DIR ]]; then
    echo "--spool-dir is not a directory: $SPOOL_DIR" >&2
    exit 2
fi

if sudo lvs --noheadings -o lv_name "$VG" 2>/dev/null \
    | awk '{print $1}' | grep -qx "$BASE_LV"; then
    echo "Base LV $VG/$BASE_LV already exists. Pick a different --date or remove it first." >&2
    exit 1
fi

# ─── cleanup trap ──────────────────────────────────────────────────────
# The base LV is the deliverable: keep on success, lvremove on failure.
# An empty/partial base LV would be selected by the worker. Publish only
# after extraction, verification, and unmount succeed.
BASE_CREATED=0
BASE_POPULATED=0
HASH_PID=
DOWNLOAD_DIR=
cleanup() {
    local rc=$?
    set +e
    if [[ -n $HASH_PID ]]; then
        kill "$HASH_PID" 2>/dev/null || true
        wait "$HASH_PID" 2>/dev/null || true
    fi
    mountpoint -q "$MOUNT_BASE"    2>/dev/null && sudo umount "$MOUNT_BASE"
    if [[ $BASE_CREATED -eq 1 && $BASE_POPULATED -eq 0 ]]; then
        echo "cleanup: removing partial base LV $VG/$BASE_LV (extract did not complete)" >&2
        sudo lvremove -y "$VG/$BASE_LV"
    elif [[ $BASE_CREATED -eq 1 ]]; then
        sudo lvchange -an "$VG/$BASE_LV" 2>/dev/null
    fi
    if [[ -n $DOWNLOAD_DIR ]]; then
        rm -f -- \
            "$DOWNLOAD_DIR/archive.sha256.fifo" \
            "$DOWNLOAD_DIR/archive.sha256.actual"
        rmdir -- "$DOWNLOAD_DIR" 2>/dev/null || true
    fi
    sudo rmdir "$MOUNT_BASE" 2>/dev/null
    return $rc
}
trap cleanup EXIT
trap 'exit 143' TERM
trap 'exit 130' INT

# ─── 1. expected SHA ───────────────────────────────────────────────────
echo "[1/4] Fetching expected SHA-256 from $SHA_URL..."
EXPECTED_SHA=$(curl --fail --silent --show-error --location "$SHA_URL" | awk '{print $1}')
[[ $EXPECTED_SHA =~ ^[0-9a-fA-F]{64}$ ]] \
    || { echo "invalid SHA-256 sidecar value: $EXPECTED_SHA" >&2; exit 1; }
echo "      expected: $EXPECTED_SHA"

# ─── 2. create the new base LV ─────────────────────────────────────────
echo "[2/4] Creating base LV $VG/$BASE_LV..."
sudo lvcreate -V "$BASE_SIZE" --thin --name "$BASE_LV" "$VG/$THINPOOL"
BASE_CREATED=1
sudo mkfs.xfs -q "/dev/$VG/$BASE_LV"
sudo mkdir -p "$MOUNT_BASE"
sudo mount "/dev/$VG/$BASE_LV" "$MOUNT_BASE"

# Pipefail (set above) makes any download, hash fan-out, decompression, or tar
# failure remove the half-populated base through the cleanup trap.
# ─── 3. stream, verify, and extract ────────────────────────────────────
echo "[3/4] Streaming, verifying, and extracting via ripcat..."
DOWNLOAD_DIR=$(mktemp -d /tmp/sbgh-chainstate-stream.XXXXXX)
mkfifo "$DOWNLOAD_DIR/archive.sha256.fifo"
sha256sum <"$DOWNLOAD_DIR/archive.sha256.fifo" \
    >"$DOWNLOAD_DIR/archive.sha256.actual" &
HASH_PID=$!

ripcat_args=(
    --connections "$CONNECTIONS"
    --window-mib "$WINDOW_MIB"
    --retries "$RETRIES"
)
if [[ -n $SPOOL_DIR ]]; then
    ripcat_args+=(--spool-dir "$SPOOL_DIR")
fi
ripcat "${ripcat_args[@]}" "$URL" \
    | tee "$DOWNLOAD_DIR/archive.sha256.fifo" \
    | zstd --decompress --stdout \
    | sudo tar --extract --file - --directory "$MOUNT_BASE"

wait "$HASH_PID"
HASH_PID=
ACTUAL_SHA=$(awk '{print $1}' "$DOWNLOAD_DIR/archive.sha256.actual")
if [[ ${ACTUAL_SHA,,} != "${EXPECTED_SHA,,}" ]]; then
    echo "SHA-256 mismatch after streaming extraction" >&2
    echo "  expected: $EXPECTED_SHA" >&2
    echo "  actual:   $ACTUAL_SHA" >&2
    exit 1
fi
echo "      verified: $ACTUAL_SHA"

sudo umount "$MOUNT_BASE"
# Every published chainstate is an immutable origin. Jobs receive only
# explicit read-write snapshots; the origin itself is never guest-attached.
sudo lvchange --permission r "$VG/$BASE_LV"
sudo lvchange -an "$VG/$BASE_LV"
# Mark the LV as a successful deliverable AFTER unmount completes;
# anything before this point is "partial" from the trap's perspective.
BASE_POPULATED=1

# ─── 4. rotate older baselines ─────────────────────────────────────────
if [[ $KEEP_OLD -eq 1 ]]; then
    echo "[4/4] --keep-old set; not rotating."
else
    echo "[4/4] Rotating out older chainstate baselines..."
    # All <prefix>* LVs strictly older (lex) than the new one.
    OLD_LVS=$(sudo lvs --noheadings -o lv_name "$VG" 2>/dev/null \
        | awk -v prefix="$PREFIX" -v cur="$BASE_LV" \
            'index($1, prefix) == 1 && $1 < cur {print $1}')

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
