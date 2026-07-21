#!/usr/bin/env bash
# Gracefully stop a managed stacks-node, take a tagged LVM-thin snapshot of its
# chainstate LV, write sbgh provenance metadata, and optionally restart it.
#
# This is an operator script for the 0052 design spike. It deliberately handles
# one cut point at a time: run it, snapshot, upgrade/reconfigure the node if
# needed, then run it again for the next cut.

set -euo pipefail

VG=vg0
BASE_LV=stacks-node-mainnet
SNAPSHOT_PREFIX=mainnet-node-
SERVICE=stacks-node.service
API_URL=http://127.0.0.1:20443/v2/info
METADATA_DIR=/var/lib/sbgh/chainstate-snapshots
POLL_SECS=30
RESTART_NODE=1
DRY_RUN=0
BASE_MOUNT=
QUIESCE=sync
VALIDATE_SNAPSHOT=1
ALLOW_EPOCH_DRIFT=0
# Burn-block margin used only to flag a cut as "boundary-adjacent" for the
# best-effort warning below. It is NOT a tolerance the guard enforces; the guard
# rejects only a tip that has already crossed the boundary at read time.
EPOCH_OVERSHOOT_MARGIN=8
SNAPSHOT_MOUNT_OPTIONS=ro,nouuid,norecovery

COMMAND=
EVENT_NAME=
REASON=manual
TARGET_BURN_HEIGHT=
TARGET_STACKS_HEIGHT=
TARGET_EPOCH=
BOUNDARY_ADJACENT=0
PRODUCER_RELEASE=auto
EXPECTED_PRODUCER_RELEASE=
NODE_STOPPED=0
BASE_FROZEN=0
BASE_UNMOUNTED=0
VALIDATION_STATUS=not_run
VALIDATION_NOTES=

usage() {
    cat <<EOF
Usage:
  $(basename "$0") snapshot-now [OPTIONS] --event NAME
  $(basename "$0") wait-cut     [OPTIONS] --event NAME --target-burn-height N

Gracefully stop a managed stacks-node, snapshot its writable base LV, tag the
snapshot with sbgh provenance, and write a JSON sidecar under:
  ${METADATA_DIR}

Commands:
  snapshot-now     Snapshot immediately using the current /v2/info payload.
  wait-cut         Poll /v2/info until a target height is reached, then snapshot.
  print-epoch-map  Print the built-in epoch activation map and exit.

Required:
  --event NAME                  Stable cut/snapshot name, e.g. rel-3.4.0.0.3.
  --target-burn-height N        wait-cut only; stop when burn height >= N.

Options:
  --target-stacks-height N      Also allow stop when stacks_tip_height >= N.
  --reason STR                  Cut reason: release, epoch, daily, manual.
                                Default: ${REASON}.
  --producer-release STR        Release that produced the chainstate. Default:
                                auto (parsed from /v2/info server_version).
  --expected-producer-release STR
                                Fail before snapshot if /v2/info reports a
                                different server_version release.
  --vg NAME                     Volume group. Default: ${VG}.
  --base-lv NAME                Writable managed node LV. Default: ${BASE_LV}.
  --snapshot-prefix STR         Snapshot LV name prefix. Default:
                                ${SNAPSHOT_PREFIX}.
  --service NAME                systemd unit to stop/start. Default: ${SERVICE}.
  --api-url URL                 stacks-node /v2/info URL. Default: ${API_URL}.
  --metadata-dir PATH           Metadata sidecar dir. Default: ${METADATA_DIR}.
  --base-mount PATH             Mountpoint for the writable base LV. Required
                                for --quiesce fsfreeze|umount.
  --quiesce MODE                sync, fsfreeze, or umount. Default: ${QUIESCE}.
                                sync is always run; fsfreeze/umount add a
                                stronger clean-snapshot boundary.
  --no-validate                 Skip read-only snapshot mount/path validation.
  --allow-epoch-drift           Snapshot even if the observed height overshot
                                the target height's epoch boundary. Without this
                                a boundary-crossing overshoot fails instead of
                                producing a mislabeled snapshot.
  --snapshot-mount-options OPTS Mount options for validation. Default:
                                ${SNAPSHOT_MOUNT_OPTIONS}
                                The default is XFS-snapshot-safe: nouuid avoids
                                origin/snapshot UUID clashes, and norecovery
                                avoids log replay on read-only validation.
  --poll-secs N                 wait-cut poll interval. Default: ${POLL_SECS}.
  --no-restart                  Leave node stopped after snapshot.
  --dry-run                     Print privileged commands instead of running.
  -h, --help                    Show this help.

Notes:
  - The snapshot is a thin snapshot of VG/BASE_LV, using the same LVM primitive
    as the daemon's per-job chainstate snapshots.
  - Snapshot tags are intentionally compact. Rich metadata lives in the JSON
    sidecar.
  - The "actual epoch" tag is derived from observed burn_block_height using the
    built-in mainnet epoch activation map.
EOF
}

epoch_for_burn_height() {
    local h=$1
    if (( h >= 943333 )); then echo "3.4"
    elif (( h >= 923222 )); then echo "3.3"
    elif (( h >= 907740 )); then echo "3.2"
    elif (( h >= 875000 )); then echo "3.1"
    elif (( h >= 867867 )); then echo "3.0"
    elif (( h >= 840360 )); then echo "2.5"
    elif (( h >= 791551 )); then echo "2.4"
    elif (( h >= 789751 )); then echo "2.3"
    elif (( h >= 787651 )); then echo "2.2"
    elif (( h >= 781551 )); then echo "2.1"
    elif (( h >= 713000 )); then echo "2.05"
    elif (( h >= 666050 )); then echo "2.0"
    else echo "pre-2.0"
    fi
}

print_epoch_map() {
    cat <<'EOF'
epoch,burn_height,notes
2.0,666050,Stacks 2.0 mainnet
2.05,713000,SIP-012 cost changes
2.1,781551,Stacks 2.1 activation
2.2,787651,SIP-022 emergency PoX fix boundary
2.3,789751,SIP-022/SIP-023-style fix boundary
2.4,791551,PoX re-enabled / SIP-024 activation
2.5,840360,PoX-4 / pre-Nakamoto
3.0,867867,Nakamoto
3.1,875000,SIP-029
3.2,907740,SIP-031
3.3,923222,Clarity 4
3.4,943333,Clarity 5 / at-block removal
EOF
}

die() {
    echo "ERROR: $*" >&2
    exit 1
}

cleanup() {
    local rc=${1:-$?}
    if [[ $BASE_FROZEN -eq 1 || $BASE_UNMOUNTED -eq 1 ]]; then
        echo "cleanup: restoring base LV mount state" >&2
        set +e
        restore_quiescence
    fi
    if [[ $rc -ne 0 && $NODE_STOPPED -eq 1 && $RESTART_NODE -eq 1 ]]; then
        echo "cleanup: restarting $SERVICE after failure" >&2
        set +e
        run sudo systemctl start "$SERVICE"
        NODE_STOPPED=0
    fi
    return "$rc"
}

signal_cleanup() {
    local rc=$1
    trap - EXIT INT TERM
    cleanup "$rc"
    exit "$rc"
}

trap cleanup EXIT
trap 'signal_cleanup 130' INT
trap 'signal_cleanup 143' TERM

need_cmds() {
    local missing=0
    for cmd in curl jq sudo systemctl lvcreate lvchange lvs date mktemp install sed tr mount umount mountpoint find; do
        if ! command -v "$cmd" >/dev/null 2>&1; then
            echo "missing required command: $cmd" >&2
            missing=1
        fi
    done
    if [[ "$QUIESCE" == "fsfreeze" ]] && ! command -v fsfreeze >/dev/null 2>&1; then
        echo "missing required command for --quiesce fsfreeze: fsfreeze" >&2
        missing=1
    fi
    [[ $missing -eq 0 ]] || exit 1
}

run() {
    if [[ $DRY_RUN -eq 1 ]]; then
        printf '+'
        printf ' %q' "$@"
        printf '\n'
        return 0
    fi
    "$@"
}

sanitize_slug() {
    # Keep names compatible with both LV names and LVM tags.
    echo "$1" \
        | tr '[:upper:]' '[:lower:]' \
        | sed -E 's/[^a-z0-9_.+-]+/-/g; s/^-+//; s/-+$//; s/-+/-/g'
}

tagify() {
    local tag
    tag=$(sanitize_slug "$1")
    # LVM tags are small labels; keep them comfortably short.
    echo "${tag:0:120}"
}

fetch_info() {
    curl --fail --silent --show-error --location "$API_URL"
}

jq_required() {
    local filter=$1
    local label=$2
    local value
    value=$(jq -er "$filter" <<<"$INFO_JSON") \
        || die "/v2/info missing required field: $label"
    echo "$value"
}

producer_from_info() {
    local server_version=$1
    if [[ "$server_version" =~ stacks-node[[:space:]]+([^[:space:]]+) ]]; then
        echo "${BASH_REMATCH[1]}"
    else
        echo "unknown"
    fi
}

metadata_json() {
    local snapshot_lv=$1
    local event_slug=$2
    local actual_epoch=$3
    local actual_producer=$4
    local observed_at=$5
    local active=$6

    jq -n \
        --arg event "$EVENT_NAME" \
        --arg event_slug "$event_slug" \
        --arg reason "$REASON" \
        --arg vg "$VG" \
        --arg base_lv "$BASE_LV" \
        --arg snapshot_lv "$snapshot_lv" \
        --arg snapshot_path "/dev/$VG/$snapshot_lv" \
        --arg service "$SERVICE" \
        --arg api_url "$API_URL" \
        --arg base_mount "$BASE_MOUNT" \
        --arg quiesce "$QUIESCE" \
        --arg observed_at "$observed_at" \
        --arg producer_release "$actual_producer" \
        --arg actual_epoch "$actual_epoch" \
        --arg validation_status "$VALIDATION_STATUS" \
        --arg validation_notes "$VALIDATION_NOTES" \
        --arg validation_mount_options "$SNAPSHOT_MOUNT_OPTIONS" \
        --arg target_epoch "$TARGET_EPOCH" \
        --argjson snapshot_active "$active" \
        --argjson target_burn_height "${TARGET_BURN_HEIGHT:-null}" \
        --argjson target_stacks_height "${TARGET_STACKS_HEIGHT:-null}" \
        --argjson allow_epoch_drift "$([[ $ALLOW_EPOCH_DRIFT -eq 1 ]] && echo true || echo false)" \
        --argjson boundary_adjacent "$([[ $BOUNDARY_ADJACENT -eq 1 ]] && echo true || echo false)" \
        --argjson info "$INFO_JSON" \
        '{
          schema_version: 1,
          tool: "scripts/managed-node-snapshot.sh",
          event: {
            name: $event,
            slug: $event_slug,
            reason: $reason,
            target_burn_height: $target_burn_height,
            target_stacks_height: $target_stacks_height
          },
          epoch_guard: {
            mode: "pre-stop-early-rejection",
            target_epoch: (if $target_epoch == "" then null else $target_epoch end),
            observed_epoch: $actual_epoch,
            observed_burn_height: ($info.burn_block_height),
            allow_epoch_drift: $allow_epoch_drift,
            boundary_adjacent: $boundary_adjacent,
            limitation: "Observed tip was read before the graceful stop; the node may advance across an epoch boundary during shutdown. This is a best-effort cut, not an authoritative epoch boundary, until node-level stop-at-height exists."
          },
          lvm: {
            vg: $vg,
            base_lv: $base_lv,
            snapshot_lv: $snapshot_lv,
            snapshot_path: $snapshot_path,
            active: $snapshot_active
          },
          quiescence: {
            mode: $quiesce,
            base_mount: (if $base_mount == "" then null else $base_mount end)
          },
          validation: {
            status: $validation_status,
            notes: $validation_notes,
            mount_options: $validation_mount_options
          },
          node: {
            service: $service,
            api_url: $api_url,
            producer_release: $producer_release,
            actual_epoch: $actual_epoch,
            observed_at: $observed_at,
            info: $info
          }
        }'
}

write_metadata() {
    local snapshot_lv=$1
    local event_slug=$2
    local actual_epoch=$3
    local actual_producer=$4
    local observed_at=$5
    local active=$6
    local tmp metadata_path

    tmp=$(mktemp)
    metadata_json "$snapshot_lv" "$event_slug" "$actual_epoch" "$actual_producer" "$observed_at" "$active" >"$tmp"
    metadata_path="${METADATA_DIR%/}/${snapshot_lv}.json"

    run sudo install -d -m 0755 "$METADATA_DIR"
    run sudo install -m 0644 "$tmp" "$metadata_path"
    rm -f "$tmp"
    echo "Metadata: $metadata_path"
}

check_base_lv_exists() {
    sudo lvs --noheadings -o lv_name "$VG" 2>/dev/null \
        | awk '{print $1}' \
        | grep -qx "$BASE_LV" \
        || die "base LV $VG/$BASE_LV does not exist"
}

check_snapshot_absent() {
    local snapshot_lv=$1
    if sudo lvs --noheadings -o lv_name "$VG" 2>/dev/null \
        | awk '{print $1}' \
        | grep -qx "$snapshot_lv"; then
        die "snapshot LV $VG/$snapshot_lv already exists"
    fi
}

stop_node() {
    echo "Stopping $SERVICE gracefully..."
    run sudo systemctl stop "$SERVICE"
    NODE_STOPPED=1
}

start_node() {
    [[ $RESTART_NODE -eq 1 ]] || return 0
    restore_quiescence
    echo "Restarting $SERVICE..."
    run sudo systemctl start "$SERVICE"
    NODE_STOPPED=0
}

quiesce_base_lv() {
    echo "Flushing filesystem writes..."
    run sync

    case "$QUIESCE" in
        sync)
            return 0
            ;;
        fsfreeze)
            [[ -n "$BASE_MOUNT" ]] || die "--base-mount is required for --quiesce fsfreeze"
            mountpoint -q "$BASE_MOUNT" || die "$BASE_MOUNT is not a mountpoint"
            echo "Freezing $BASE_MOUNT..."
            run sudo fsfreeze --freeze "$BASE_MOUNT"
            BASE_FROZEN=1
            ;;
        umount)
            [[ -n "$BASE_MOUNT" ]] || die "--base-mount is required for --quiesce umount"
            mountpoint -q "$BASE_MOUNT" || die "$BASE_MOUNT is not a mountpoint"
            echo "Unmounting $BASE_MOUNT..."
            run sudo umount "$BASE_MOUNT"
            BASE_UNMOUNTED=1
            ;;
        *)
            die "unknown --quiesce mode: $QUIESCE"
            ;;
    esac
}

restore_quiescence() {
    if [[ $BASE_FROZEN -eq 1 ]]; then
        echo "Unfreezing $BASE_MOUNT..."
        run sudo fsfreeze --unfreeze "$BASE_MOUNT"
        BASE_FROZEN=0
    fi
    if [[ $BASE_UNMOUNTED -eq 1 ]]; then
        echo "Remounting $BASE_MOUNT..."
        if ! run sudo mount "$BASE_MOUNT"; then
            run sudo mount "/dev/$VG/$BASE_LV" "$BASE_MOUNT"
        fi
        BASE_UNMOUNTED=0
    fi
}

validate_snapshot() {
    local snapshot_lv=$1
    if [[ $VALIDATE_SNAPSHOT -eq 0 ]]; then
        VALIDATION_STATUS=skipped
        VALIDATION_NOTES="operator disabled validation"
        return 0
    fi
    if [[ $DRY_RUN -eq 1 ]]; then
        VALIDATION_STATUS=dry_run
        VALIDATION_NOTES="not run in dry-run mode"
        return 0
    fi

    local mount_dir chainstate_dir
    mount_dir=$(mktemp -d)
    if ! sudo mount -o "$SNAPSHOT_MOUNT_OPTIONS" "/dev/$VG/$snapshot_lv" "$mount_dir"; then
        rmdir "$mount_dir" 2>/dev/null || true
        VALIDATION_STATUS=failed
        VALIDATION_NOTES="read-only mount failed with options: $SNAPSHOT_MOUNT_OPTIONS"
        return 1
    fi

    chainstate_dir=$(sudo find "$mount_dir" -maxdepth 3 -mindepth 1 -type d -name chainstate -print -quit 2>/dev/null || true)
    sudo umount "$mount_dir"
    rmdir "$mount_dir" 2>/dev/null || true

    if [[ -z "$chainstate_dir" ]]; then
        VALIDATION_STATUS=failed
        VALIDATION_NOTES="no chainstate directory found within depth 3"
        return 1
    fi

    VALIDATION_STATUS=ok
    VALIDATION_NOTES="read-only mount succeeded; found ${chainstate_dir#"$mount_dir"/}"
    return 0
}

snapshot_current_info() {
    local event_slug server_version actual_producer actual_epoch observed_at
    local burn_height stacks_height snapshot_lv observed_producer margin_epoch

    event_slug=$(sanitize_slug "$EVENT_NAME")
    [[ -n "$event_slug" ]] || die "--event produced an empty slug"

    # Everything that does not depend on the live tip runs before the final API
    # read, so the read->stop window (during which the node keeps advancing) is
    # as small as possible. See the epoch-guard limitation below.
    check_base_lv_exists

    INFO_JSON=$(fetch_info)
    burn_height=$(jq_required '.burn_block_height | numbers' burn_block_height)
    stacks_height=$(jq_required '.stacks_tip_height | numbers' stacks_tip_height)
    server_version=$(jq_required '.server_version | strings' server_version)
    observed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)

    # The mismatch guard always compares the release actually reported by the
    # running node against the expectation. --producer-release only overrides
    # the release recorded in provenance metadata; it must not stand in for the
    # observed value, or the check would compare the expectation to itself.
    observed_producer=$(producer_from_info "$server_version")
    if [[ -n "$EXPECTED_PRODUCER_RELEASE" && "$observed_producer" != "$EXPECTED_PRODUCER_RELEASE" ]]; then
        die "producer release mismatch: expected $EXPECTED_PRODUCER_RELEASE, observed $observed_producer (server_version: $server_version)"
    fi

    if [[ "$PRODUCER_RELEASE" == "auto" ]]; then
        actual_producer="$observed_producer"
    else
        actual_producer="$PRODUCER_RELEASE"
    fi

    actual_epoch=$(epoch_for_burn_height "$burn_height")

    # Pre-stop early-rejection guard -- NOT complete drift detection. It rejects
    # a tip that has ALREADY overshot the target epoch at read time. It cannot
    # see blocks the node processes during graceful shutdown, so a
    # boundary-adjacent cut may still cross the boundary after this check.
    # Authoritative epoch boundaries require node-level stop-at-height; until
    # then these snapshots are best-effort. See planning/design/0052.
    if [[ -n "$TARGET_BURN_HEIGHT" ]]; then
        TARGET_EPOCH=$(epoch_for_burn_height "$TARGET_BURN_HEIGHT")
        margin_epoch=$(epoch_for_burn_height "$((TARGET_BURN_HEIGHT + EPOCH_OVERSHOOT_MARGIN))")
        [[ "$TARGET_EPOCH" != "$margin_epoch" ]] && BOUNDARY_ADJACENT=1

        if [[ "$actual_epoch" != "$TARGET_EPOCH" && $ALLOW_EPOCH_DRIFT -eq 0 ]]; then
            die "epoch overshoot: target burn $TARGET_BURN_HEIGHT is epoch $TARGET_EPOCH, but the tip already reads burn $burn_height (epoch $actual_epoch). The node has passed the boundary; resuming this cut will not recover it -- restore an earlier chainstate checkpoint, or pass --allow-epoch-drift to snapshot the current (post-boundary) state anyway."
        fi
        if [[ $BOUNDARY_ADJACENT -eq 1 ]]; then
            echo "WARNING: boundary-adjacent cut. The tip is read while the node runs;" >&2
            echo "         it may still cross into the next epoch during graceful" >&2
            echo "         shutdown. This snapshot is best-effort, not an authoritative" >&2
            echo "         epoch boundary (needs node-level stop-at-height)." >&2
        fi
    fi

    snapshot_lv="${SNAPSHOT_PREFIX}${event_slug}-b${burn_height}-s${stacks_height}"
    snapshot_lv=$(sanitize_slug "$snapshot_lv")
    check_snapshot_absent "$snapshot_lv"

    echo "Cut event:       $EVENT_NAME"
    echo "Reason:          $REASON"
    echo "Observed burn:   $burn_height"
    echo "Observed stacks: $stacks_height"
    echo "Actual epoch:    $actual_epoch"
    echo "Producer:        $actual_producer"
    echo "Snapshot LV:     $VG/$snapshot_lv"

    stop_node
    quiesce_base_lv

    local tags=(
        "sbgh"
        "sbgh-chainstate"
        "managed-node"
        "network-mainnet"
        "event-$event_slug"
        "reason-$REASON"
        "epoch-$actual_epoch"
        "producer-$actual_producer"
    )
    local tag_args=()
    for tag in "${tags[@]}"; do
        tag_args+=(--addtag "$(tagify "$tag")")
    done

    echo "Creating read-only thin snapshot..."
    run sudo lvcreate \
        --snapshot \
        --name "$snapshot_lv" \
        --setactivationskip n \
        "${tag_args[@]}" \
        "$VG/$BASE_LV"
    run sudo lvchange --permission r "$VG/$snapshot_lv"
    if [[ $BASE_FROZEN -eq 1 ]]; then
        restore_quiescence
    fi

    local validation_ok=1
    validate_snapshot "$snapshot_lv" || validation_ok=0
    run sudo lvchange -an "$VG/$snapshot_lv"

    if [[ $BASE_UNMOUNTED -eq 1 ]]; then
        restore_quiescence
    fi

    write_metadata "$snapshot_lv" "$event_slug" "$actual_epoch" "$actual_producer" "$observed_at" false
    [[ $validation_ok -eq 1 ]] || die "snapshot validation failed: $VALIDATION_NOTES"
    start_node
    echo "Done. Snapshot: $VG/$snapshot_lv"
}

wait_until_cut() {
    [[ -n "$TARGET_BURN_HEIGHT" || -n "$TARGET_STACKS_HEIGHT" ]] \
        || die "wait-cut needs --target-burn-height or --target-stacks-height"

    while true; do
        INFO_JSON=$(fetch_info)
        local burn_height stacks_height server_version
        burn_height=$(jq_required '.burn_block_height | numbers' burn_block_height)
        stacks_height=$(jq_required '.stacks_tip_height | numbers' stacks_tip_height)
        server_version=$(jq_required '.server_version | strings' server_version)

        printf '[%s] burn=%s stacks=%s server=%s\n' \
            "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
            "$burn_height" \
            "$stacks_height" \
            "$server_version"

        if [[ -n "$TARGET_BURN_HEIGHT" && "$burn_height" -ge "$TARGET_BURN_HEIGHT" ]]; then
            echo "Reached target burn height: $burn_height >= $TARGET_BURN_HEIGHT"
            break
        fi
        if [[ -n "$TARGET_STACKS_HEIGHT" && "$stacks_height" -ge "$TARGET_STACKS_HEIGHT" ]]; then
            echo "Reached target stacks height: $stacks_height >= $TARGET_STACKS_HEIGHT"
            break
        fi
        sleep "$POLL_SECS"
    done

    snapshot_current_info
}

parse_args() {
    [[ $# -gt 0 ]] || { usage >&2; exit 2; }
    if [[ "$1" == "-h" || "$1" == "--help" ]]; then
        usage
        exit 0
    fi
    COMMAND=$1
    shift

    if [[ "$COMMAND" == "print-epoch-map" ]]; then
        print_epoch_map
        exit 0
    fi

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --event) EVENT_NAME=$2; shift 2 ;;
            --reason) REASON=$2; shift 2 ;;
            --target-burn-height) TARGET_BURN_HEIGHT=$2; shift 2 ;;
            --target-stacks-height) TARGET_STACKS_HEIGHT=$2; shift 2 ;;
            --producer-release) PRODUCER_RELEASE=$2; shift 2 ;;
            --expected-producer-release) EXPECTED_PRODUCER_RELEASE=$2; shift 2 ;;
            --vg) VG=$2; shift 2 ;;
            --base-lv) BASE_LV=$2; shift 2 ;;
            --snapshot-prefix) SNAPSHOT_PREFIX=$2; shift 2 ;;
            --service) SERVICE=$2; shift 2 ;;
            --api-url) API_URL=$2; shift 2 ;;
            --metadata-dir) METADATA_DIR=$2; shift 2 ;;
            --base-mount) BASE_MOUNT=$2; shift 2 ;;
            --quiesce) QUIESCE=$2; shift 2 ;;
            --no-validate) VALIDATE_SNAPSHOT=0; shift ;;
            --allow-epoch-drift) ALLOW_EPOCH_DRIFT=1; shift ;;
            --snapshot-mount-options) SNAPSHOT_MOUNT_OPTIONS=$2; shift 2 ;;
            --poll-secs) POLL_SECS=$2; shift 2 ;;
            --no-restart) RESTART_NODE=0; shift ;;
            --dry-run) DRY_RUN=1; shift ;;
            -h|--help) usage; exit 0 ;;
            *) die "unknown arg: $1" ;;
        esac
    done
}

validate_common_args() {
    [[ "$COMMAND" == "snapshot-now" || "$COMMAND" == "wait-cut" ]] \
        || die "unknown command: $COMMAND"
    [[ -n "$EVENT_NAME" ]] || die "--event is required"
    [[ "$REASON" == "$(sanitize_slug "$REASON")" ]] \
        || die "--reason must be tag-safe (letters, numbers, dot, underscore, plus, hyphen)"
    [[ "$QUIESCE" == "sync" || "$QUIESCE" == "fsfreeze" || "$QUIESCE" == "umount" ]] \
        || die "--quiesce must be sync, fsfreeze, or umount"
    [[ -n "$SNAPSHOT_MOUNT_OPTIONS" ]] \
        || die "--snapshot-mount-options must not be empty"
    [[ "$POLL_SECS" =~ ^[0-9]+$ && "$POLL_SECS" -gt 0 ]] \
        || die "--poll-secs must be a positive integer"
    if [[ -n "$TARGET_BURN_HEIGHT" && ! "$TARGET_BURN_HEIGHT" =~ ^[0-9]+$ ]]; then
        die "--target-burn-height must be an integer"
    fi
    if [[ -n "$TARGET_STACKS_HEIGHT" && ! "$TARGET_STACKS_HEIGHT" =~ ^[0-9]+$ ]]; then
        die "--target-stacks-height must be an integer"
    fi
}

INFO_JSON=
parse_args "$@"
validate_common_args
need_cmds

case "$COMMAND" in
    snapshot-now) snapshot_current_info ;;
    wait-cut) wait_until_cut ;;
esac
