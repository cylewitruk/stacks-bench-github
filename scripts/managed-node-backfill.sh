#!/usr/bin/env bash
# Drive a managed stacks-node through an ordered cut plan and delegate each
# quiesced LVM snapshot to managed-node-snapshot.sh.

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
PLAN=
PLAN_DIR=
COMMAND=
FROM_EVENT=
THROUGH_EVENT=
ONLY_EVENT=
DRY_RUN=0
SKIP_INSTALL=0
SKIP_START=0
RESTART_ON_FAILURE=0
SNAPSHOT_SCRIPT=
CURRENT_CUT=
LAST_COMPLETED_CUT=

CUT_EVENT=
CUT_REASON=
CUT_PRODUCER_RELEASE=
CUT_EXPECTED_PRODUCER_RELEASE=
CUT_TARGET_BURN_HEIGHT=
CUT_TARGET_STACKS_HEIGHT=
CUT_SERVICE=
CUT_NOTES=

usage() {
    cat <<'EOF'
Usage:
  managed-node-backfill.sh list  --plan FILE
  managed-node-backfill.sh check --plan FILE
  managed-node-backfill.sh run   --plan FILE [OPTIONS]

Commands:
  list       Print enabled cuts in execution order.
  check      Validate the cut-plan shape.
  run        Execute the cut plan.

Run options:
  --from EVENT       Skip cuts before EVENT.
  --through EVENT    Stop after EVENT.
  --only EVENT       Run only EVENT.
  --snapshot-script PATH
                     Override the snapshot helper path. Default comes from the
                     plan, or scripts/managed-node-snapshot.sh.
  --skip-install     Do not run the plan's install_release hook.
  --skip-start       Do not start the node before each cut.
  --restart-on-failure
                     Try to start the current cut's node service if the
                     backfill is interrupted or fails.
  --dry-run          Print commands without executing hooks or snapshots.
  -h, --help         Show this help.

Plan notes:
  - hooks are argv arrays, not shell snippets. No eval is used.
  - install_release is intentionally host-specific; point it at a local script
    that checks out/builds/installs the requested release safely.
  - Long backfills are expected to resume. After a TRANSIENT failure, fix the
    issue and rerun with --from EVENT to retry that cut, or the next cut if the
    snapshot completed before the failure. An epoch-overshoot failure is NOT
    transient: the node has already crossed the boundary, so resuming will not
    recover it -- restore an earlier chainstate checkpoint first.
  - supported hook placeholders:
      {{event}}, {{reason}}, {{producer_release}},
      {{expected_producer_release}}, {{target_burn_height}},
      {{target_stacks_height}}, {{service}}, {{notes}}
EOF
}

die() {
    echo "ERROR: $*" >&2
    exit 1
}

need_cmds() {
    if ! command -v jq >/dev/null 2>&1; then
        echo "missing required command: jq" >&2
        exit 1
    fi
}

run_cmd() {
    if [[ $# -eq 0 ]]; then
        return 0
    fi
    if [[ $DRY_RUN -eq 1 ]]; then
        printf '+'
        printf ' %q' "$@"
        printf '\n'
        return 0
    fi
    "$@"
}

start_current_service() {
    if [[ -z "$CUT_SERVICE" ]]; then
        return 0
    fi
    if hook_present start_node; then
        run_hook start_node
    else
        run_cmd sudo systemctl start "$CUT_SERVICE"
    fi
}

backfill_cleanup() {
    local rc=${1:-$?}
    if [[ "$COMMAND" == "run" && $rc -ne 0 ]]; then
        echo "Backfill interrupted or failed." >&2
        if [[ -n "$CURRENT_CUT" ]]; then
            echo "Current cut: $CURRENT_CUT" >&2
            echo "For a transient failure (build/install/disk), resume with: $0 run --plan $PLAN --from $CURRENT_CUT" >&2
            echo "NOTE: an epoch-overshoot failure is NOT recoverable by resuming -- the node has already passed the boundary. Restore an earlier chainstate checkpoint before retrying that cut." >&2
        fi
        if [[ -n "$LAST_COMPLETED_CUT" ]]; then
            echo "Last completed cut: $LAST_COMPLETED_CUT" >&2
        fi
        if [[ $RESTART_ON_FAILURE -eq 1 ]]; then
            echo "Attempting to start $CUT_SERVICE after failure." >&2
            set +e
            start_current_service
        else
            echo "Node restart on failure disabled; pass --restart-on-failure to opt in." >&2
        fi
    fi
    return "$rc"
}

signal_cleanup() {
    local rc=$1
    trap - EXIT INT TERM
    backfill_cleanup "$rc"
    exit "$rc"
}

trap 'backfill_cleanup $?' EXIT
trap 'signal_cleanup 130' INT
trap 'signal_cleanup 143' TERM

jq_plan() {
    jq -r "$1" "$PLAN"
}

jq_cut() {
    local idx=$1
    local filter=$2
    jq -r --argjson i "$idx" "$filter" "$PLAN"
}

sanitize_slug() {
    echo "$1" \
        | tr '[:upper:]' '[:lower:]' \
        | sed -E 's/[^a-z0-9_.+-]+/-/g; s/^-+//; s/-+$//; s/-+/-/g'
}

abs_dirname() {
    local path=$1
    (cd -- "$(dirname -- "$path")" && pwd -P)
}

resolve_helper_path() {
    local path=$1
    if [[ "$path" == /* ]]; then
        printf '%s\n' "$path"
        return
    fi
    if [[ -n "$PLAN_DIR" && -e "$PLAN_DIR/$path" ]]; then
        printf '%s\n' "$PLAN_DIR/$path"
        return
    fi
    if [[ -e "$path" ]]; then
        printf '%s/%s\n' "$(abs_dirname "$path")" "$(basename -- "$path")"
        return
    fi
    printf '%s/%s\n' "$SCRIPT_DIR" "$path"
}

render_arg() {
    local arg=$1
    arg=${arg//\{\{event\}\}/$CUT_EVENT}
    arg=${arg//\{\{reason\}\}/$CUT_REASON}
    arg=${arg//\{\{producer_release\}\}/$CUT_PRODUCER_RELEASE}
    arg=${arg//\{\{expected_producer_release\}\}/$CUT_EXPECTED_PRODUCER_RELEASE}
    arg=${arg//\{\{target_burn_height\}\}/$CUT_TARGET_BURN_HEIGHT}
    arg=${arg//\{\{target_stacks_height\}\}/$CUT_TARGET_STACKS_HEIGHT}
    arg=${arg//\{\{service\}\}/$CUT_SERVICE}
    arg=${arg//\{\{notes\}\}/$CUT_NOTES}
    printf '%s\n' "$arg"
}

hook_present() {
    local name=$1
    [[ "$(jq -r --arg name "$name" '(.hooks // {}) | has($name)' "$PLAN")" == "true" ]]
}

run_hook() {
    local name=$1
    local len arg
    hook_present "$name" || return 0
    len=$(jq -r --arg name "$name" '((.hooks // {})[$name] // []) | length' "$PLAN")
    [[ "$len" -gt 0 ]] || return 0

    local args=()
    for ((j = 0; j < len; j++)); do
        arg=$(jq -r --arg name "$name" --argjson j "$j" '(.hooks // {})[$name][$j]' "$PLAN")
        args+=("$(render_arg "$arg")")
    done

    echo "Hook $name: ${args[*]}"
    run_cmd "${args[@]}"
}

cut_count() {
    jq_plan '.cuts | length'
}

load_cut_context() {
    local idx=$1
    CUT_EVENT=$(jq_cut "$idx" ".cuts[\$i].event // empty")
    CUT_REASON=$(jq_cut "$idx" ".cuts[\$i].reason // .defaults.reason // \"manual\"")
    CUT_PRODUCER_RELEASE=$(jq_cut "$idx" ".cuts[\$i].producer_release // empty")
    CUT_EXPECTED_PRODUCER_RELEASE=$(jq_cut "$idx" ".cuts[\$i].expected_producer_release // .cuts[\$i].producer_release // empty")
    CUT_TARGET_BURN_HEIGHT=$(jq_cut "$idx" ".cuts[\$i].target_burn_height // empty")
    CUT_TARGET_STACKS_HEIGHT=$(jq_cut "$idx" ".cuts[\$i].target_stacks_height // empty")
    CUT_SERVICE=$(jq_cut "$idx" ".cuts[\$i].service // .defaults.service // \"stacks-node.service\"")
    CUT_NOTES=$(jq_cut "$idx" ".cuts[\$i].notes // empty")
}

cut_enabled() {
    local idx=$1
    [[ "$(jq_cut "$idx" ".cuts[\$i].enabled // true")" == "true" ]]
}

cut_command() {
    local idx=$1
    jq_cut "$idx" ".cuts[\$i].command // \"wait-cut\""
}

# Echo the plan index of the first enabled cut whose event matches, or return
# non-zero if none matches. Plan order is the execution order, so indices are
# directly comparable for range-ordering checks.
enabled_event_index() {
    local target=$1
    local count idx
    count=$(cut_count)
    for ((idx = 0; idx < count; idx++)); do
        cut_enabled "$idx" || continue
        if [[ "$(jq_cut "$idx" ".cuts[\$i].event // empty")" == "$target" ]]; then
            printf '%s\n' "$idx"
            return 0
        fi
    done
    return 1
}

should_run_cut() {
    local event=$1
    local started_ref=$2

    if [[ -n "$ONLY_EVENT" ]]; then
        [[ "$event" == "$ONLY_EVENT" ]]
        return
    fi
    if [[ -n "$FROM_EVENT" && "$started_ref" -eq 0 ]]; then
        [[ "$event" == "$FROM_EVENT" ]]
        return
    fi
    return 0
}

snapshot_default() {
    local key=$1
    local fallback=$2
    jq -r --arg key "$key" --arg fallback "$fallback" '.defaults[$key] // $fallback' "$PLAN"
}

build_snapshot_args() {
    local command=$1
    local args=("$SNAPSHOT_SCRIPT" "$command" "--event" "$CUT_EVENT" "--reason" "$CUT_REASON")

    local vg base_lv base_mount prefix api_url metadata_dir quiesce poll_secs mount_options
    vg=$(snapshot_default vg vg0)
    base_lv=$(snapshot_default base_lv stacks-node-mainnet)
    base_mount=$(snapshot_default base_mount "")
    prefix=$(snapshot_default snapshot_prefix mainnet-node-)
    api_url=$(snapshot_default api_url http://127.0.0.1:20443/v2/info)
    metadata_dir=$(snapshot_default metadata_dir /var/lib/sbgh/chainstate-snapshots)
    quiesce=$(snapshot_default quiesce sync)
    poll_secs=$(snapshot_default poll_secs 30)
    mount_options=$(snapshot_default snapshot_mount_options ro,nouuid,norecovery)

    args+=(--vg "$vg")
    args+=(--base-lv "$base_lv")
    args+=(--snapshot-prefix "$prefix")
    args+=(--service "$CUT_SERVICE")
    args+=(--api-url "$api_url")
    args+=(--metadata-dir "$metadata_dir")
    args+=(--quiesce "$quiesce")
    args+=(--poll-secs "$poll_secs")
    args+=(--snapshot-mount-options "$mount_options")

    if [[ -n "$base_mount" ]]; then
        args+=(--base-mount "$base_mount")
    fi
    if [[ -n "$CUT_PRODUCER_RELEASE" ]]; then
        args+=(--producer-release "$CUT_PRODUCER_RELEASE")
    fi
    if [[ -n "$CUT_EXPECTED_PRODUCER_RELEASE" ]]; then
        args+=(--expected-producer-release "$CUT_EXPECTED_PRODUCER_RELEASE")
    fi
    if [[ -n "$CUT_TARGET_BURN_HEIGHT" ]]; then
        args+=(--target-burn-height "$CUT_TARGET_BURN_HEIGHT")
    fi
    if [[ -n "$CUT_TARGET_STACKS_HEIGHT" ]]; then
        args+=(--target-stacks-height "$CUT_TARGET_STACKS_HEIGHT")
    fi

    if [[ "$(jq_plan '.defaults.validate_snapshot // true')" != "true" ]]; then
        args+=(--no-validate)
    fi
    if [[ "$(jq_plan '.defaults.allow_epoch_drift // false')" == "true" ]]; then
        args+=(--allow-epoch-drift)
    fi
    if [[ "$(jq_plan '.defaults.restart_after_snapshot // false')" != "true" ]]; then
        args+=(--no-restart)
    fi
    if [[ $DRY_RUN -eq 1 ]]; then
        args+=(--dry-run)
    fi

    printf '%s\0' "${args[@]}"
}

run_snapshot() {
    local command=$1
    local args=()
    while IFS= read -r -d '' arg; do
        args+=("$arg")
    done < <(build_snapshot_args "$command")
    run_cmd "${args[@]}"
}

start_node() {
    [[ $SKIP_START -eq 0 ]] || return 0
    start_current_service
}

list_plan() {
    local count idx enabled
    count=$(cut_count)
    printf 'event\treason\tproducer\ttarget_burn\ttarget_stacks\tnotes\n'
    for ((idx = 0; idx < count; idx++)); do
        enabled=$(jq_cut "$idx" ".cuts[\$i].enabled // true")
        [[ "$enabled" == "true" ]] || continue
        load_cut_context "$idx"
        printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
            "$CUT_EVENT" \
            "$CUT_REASON" \
            "$CUT_PRODUCER_RELEASE" \
            "$CUT_TARGET_BURN_HEIGHT" \
            "$CUT_TARGET_STACKS_HEIGHT" \
            "$CUT_NOTES"
    done
}

check_plan() {
    [[ -f "$PLAN" ]] || die "plan file not found: $PLAN"
    [[ "$(jq_plan '.schema_version // empty')" == "1" ]] \
        || die "schema_version must be 1"
    [[ "$(jq_plan '.cuts | type')" == "array" ]] \
        || die "cuts must be an array"

    local count idx command event reason last_burn burn
    count=$(cut_count)
    [[ "$count" -gt 0 ]] || die "cuts must not be empty"
    last_burn=

    for ((idx = 0; idx < count; idx++)); do
        load_cut_context "$idx"
        event=$CUT_EVENT
        reason=$CUT_REASON
        command=$(cut_command "$idx")
        [[ -n "$event" ]] || die "cut $idx missing event"
        [[ "$event" == "$(sanitize_slug "$event")" ]] \
            || die "cut $idx event must be LV/tag safe: $event"
        [[ "$reason" == "$(sanitize_slug "$reason")" ]] \
            || die "cut $idx reason must be tag safe: $reason"
        [[ "$command" == "wait-cut" || "$command" == "snapshot-now" ]] \
            || die "cut $idx command must be wait-cut or snapshot-now"
        if [[ "$command" == "wait-cut" ]]; then
            [[ -n "$CUT_TARGET_BURN_HEIGHT" || -n "$CUT_TARGET_STACKS_HEIGHT" ]] \
                || die "cut $idx wait-cut needs target_burn_height or target_stacks_height"
        fi
        if [[ -n "$CUT_TARGET_BURN_HEIGHT" ]]; then
            [[ "$CUT_TARGET_BURN_HEIGHT" =~ ^[0-9]+$ ]] \
                || die "cut $idx target_burn_height must be an integer"
            burn=$CUT_TARGET_BURN_HEIGHT
            if [[ -n "$last_burn" && "$burn" -lt "$last_burn" ]]; then
                die "cut $idx target_burn_height moved backwards: $burn < $last_burn"
            fi
            last_burn=$burn
        fi
    done

    echo "Plan OK: $PLAN ($count cuts)"
}

run_plan() {
    check_plan >/dev/null
    [[ -x "$SNAPSHOT_SCRIPT" ]] || die "snapshot script is not executable: $SNAPSHOT_SCRIPT"

    local from_idx='' through_idx=''
    if [[ -n "$FROM_EVENT" ]]; then
        from_idx=$(enabled_event_index "$FROM_EVENT") \
            || die "--from event not found among enabled cuts: $FROM_EVENT"
    fi
    if [[ -n "$THROUGH_EVENT" ]]; then
        through_idx=$(enabled_event_index "$THROUGH_EVENT") \
            || die "--through event not found among enabled cuts: $THROUGH_EVENT"
    fi
    if [[ -n "$ONLY_EVENT" ]]; then
        enabled_event_index "$ONLY_EVENT" >/dev/null \
            || die "--only event not found among enabled cuts: $ONLY_EVENT"
    fi
    if [[ -n "$from_idx" && -n "$through_idx" && "$through_idx" -lt "$from_idx" ]]; then
        die "--through $THROUGH_EVENT precedes --from $FROM_EVENT in the plan"
    fi

    local count idx started current_release command
    started=0
    current_release=
    count=$(cut_count)

    for ((idx = 0; idx < count; idx++)); do
        cut_enabled "$idx" || continue
        load_cut_context "$idx"
        should_run_cut "$CUT_EVENT" "$started" || continue
        started=1
        CURRENT_CUT=$CUT_EVENT

        echo
        echo "==> Cut $CUT_EVENT"
        if [[ "$(jq_cut "$idx" ".cuts[\$i].estimated // false")" == "true" ]]; then
            echo "    WARNING: this cut uses an estimated height; verify before relying on it as an exact release boundary."
        fi
        [[ -n "$CUT_NOTES" ]] && echo "    $CUT_NOTES"

        if [[ $SKIP_INSTALL -eq 0 && -n "$CUT_PRODUCER_RELEASE" ]]; then
            if [[ "$CUT_PRODUCER_RELEASE" != "$current_release" || "$(jq_cut "$idx" ".cuts[\$i].force_install // false")" == "true" ]]; then
                if hook_present install_release; then
                    run_hook install_release
                else
                    echo "No install_release hook configured; assuming producer $CUT_PRODUCER_RELEASE is already installed."
                fi
                current_release=$CUT_PRODUCER_RELEASE
            fi
        fi

        start_node
        command=$(cut_command "$idx")
        run_snapshot "$command"
        run_hook after_snapshot
        LAST_COMPLETED_CUT=$CURRENT_CUT
        CURRENT_CUT=

        if [[ -n "$THROUGH_EVENT" && "$CUT_EVENT" == "$THROUGH_EVENT" ]]; then
            break
        fi
    done

    [[ $started -eq 1 ]] || die "no cuts matched the requested range"
}

parse_args() {
    [[ $# -gt 0 ]] || { usage >&2; exit 2; }
    if [[ "$1" == "-h" || "$1" == "--help" ]]; then
        usage
        exit 0
    fi
    COMMAND=$1
    shift

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --plan) PLAN=$2; shift 2 ;;
            --from) FROM_EVENT=$2; shift 2 ;;
            --through) THROUGH_EVENT=$2; shift 2 ;;
            --only) ONLY_EVENT=$2; shift 2 ;;
            --snapshot-script) SNAPSHOT_SCRIPT=$2; shift 2 ;;
            --skip-install) SKIP_INSTALL=1; shift ;;
            --skip-start) SKIP_START=1; shift ;;
            --restart-on-failure) RESTART_ON_FAILURE=1; shift ;;
            --dry-run) DRY_RUN=1; shift ;;
            -h|--help) usage; exit 0 ;;
            *) die "unknown arg: $1" ;;
        esac
    done
}

parse_args "$@"
need_cmds

[[ "$COMMAND" == "list" || "$COMMAND" == "check" || "$COMMAND" == "run" ]] \
    || die "unknown command: $COMMAND"
[[ -n "$PLAN" ]] || die "--plan is required"
[[ -f "$PLAN" ]] || die "plan file not found: $PLAN"

if [[ -z "$SNAPSHOT_SCRIPT" ]]; then
    SNAPSHOT_SCRIPT=$(jq -r '.snapshot_script // "managed-node-snapshot.sh"' "$PLAN")
fi
PLAN_DIR=$(abs_dirname "$PLAN")
SNAPSHOT_SCRIPT=$(resolve_helper_path "$SNAPSHOT_SCRIPT")

case "$COMMAND" in
    list) check_plan >/dev/null; list_plan ;;
    check) check_plan ;;
    run) run_plan ;;
esac
