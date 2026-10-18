#!/usr/bin/env bash
# Safely deploy a wire-compatible worker update from a host with operator CLI
# access. Failures after draining leave the worker drained and, once quiescent,
# stopped.

set -Eeuo pipefail

repo_root=$(cd "$(dirname "$0")/.." && pwd)
profile=combined
timeout_secs=300
build_enabled=1

usage() {
    cat <<'EOF'
Usage: sudo ./scripts/update-worker-host.sh [OPTIONS]

Cordon one fleet worker, wait for its attempts and cleanup to quiesce, stop
the local instance, build and install sbgh-worker, run preflight and the
production registration check, then uncordon and return it online. A resident
replacement is first verified online while still cordoned.

Options:
  --profile NAME       Worker systemd instance/config name (default: combined)
  --timeout-secs N     Wait limit for quiescence and health (default: 300)
  --no-build           Install the existing release binary without rebuilding
  -h, --help           Show this help

The host must have authenticated sbgh-cli operator access. This workflow is for
wire-compatible worker-only updates; use the coordinated fleet runbook when the
daemon protocol or database schema also changes.
Requires the v35 CLI/daemon maintenance API. No unrelated worker is drained or
required to become idle. Legacy exit-after-idle workers remain supported.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --profile)
            profile=${2:?--profile needs a value}
            shift 2
            ;;
        --timeout-secs)
            timeout_secs=${2:?--timeout-secs needs a value}
            shift 2
            ;;
        --no-build)
            build_enabled=0
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

[[ "$profile" =~ ^[A-Za-z0-9_.-]+$ ]] || {
    echo "error: --profile contains unsupported characters: $profile" >&2
    exit 2
}
[[ "$timeout_secs" =~ ^[0-9]+$ && "$timeout_secs" -ge 10 && "$timeout_secs" -le 86400 ]] || {
    echo "error: --timeout-secs must be an integer between 10 and 86400" >&2
    exit 2
}
[[ ${EUID:-$(id -u)} -eq 0 ]] || {
    echo "error: must run as root (use sudo)" >&2
    exit 2
}
[[ -z "${DESTDIR:-}" ]] || {
    echo "error: DESTDIR is not supported by the live update workflow" >&2
    exit 2
}

cli=/usr/local/bin/sbgh-cli
worker=/usr/local/bin/sbgh-worker
worker_config="/etc/sbgh/worker/$profile.toml"
worker_unit="sbgh-worker@$profile.service"

for command in runuser systemctl systemd-analyze python3; do
    command -v "$command" >/dev/null || {
        echo "error: required command is missing: $command" >&2
        exit 1
    }
done
for executable in "$cli" "$worker" "$repo_root/scripts/install-worker.sh"; do
    [[ -x "$executable" ]] || {
        echo "error: required executable is missing: $executable" >&2
        exit 1
    }
done
[[ -r "$worker_config" ]] || {
    echo "error: worker config is not readable: $worker_config" >&2
    exit 1
}

operator() {
    runuser -u sbgh -- "$cli" "$@"
}

worker_command() {
    runuser -u sbgh-worker -- env RUST_LOG=warn \
        "$worker" --config "$worker_config" "$@"
}

maintenance_state() {
    python3 -c '
import json, sys
worker = json.load(sys.stdin)
print("\t".join((
    str(worker["enabled"]).lower(),
    str(worker["cordoned"]).lower(),
    worker.get("session_status") or "offline",
    worker.get("session_id") or "-",
    str(worker["session_live"]).lower(),
)))
'
}

json_field() {
    python3 -c 'import json, sys; value = json.load(sys.stdin)[sys.argv[1]]; print("-" if value is None else str(value).lower() if isinstance(value, bool) else value)' "$1"
}

wait_for_worker() {
    local worker_id=$1
    local expected_cordon=$2
    local deadline=$((SECONDS + timeout_secs))
    local values enabled draining session_status snapshot current_session live
    while (( SECONDS < deadline )); do
        if systemctl is-active --quiet "$worker_unit"; then
            snapshot=$(operator fleet worker-status --worker-id "$worker_id" --json) || return 1
            values=$(maintenance_state <<<"$snapshot") || return 1
            IFS=$'\t' read -r enabled draining session_status current_session live <<<"$values"
            if [[ "$enabled" == true && "$draining" == "$expected_cordon" \
                && "$live" == true && "$current_session" != - \
                && "$current_session" != "$previous_session" \
                && "$session_status" =~ ^(registered|idle|offered|running|draining)$ ]]; then
                echo "worker online: status=$session_status cordoned=$draining session=$current_session"
                return 0
            fi
        fi
        sleep 1
    done
    echo "error: $worker_unit did not establish a new live session with cordoned=$expected_cordon within ${timeout_secs}s" >&2
    return 1
}

deployment_started=0
safe_to_stop_worker=0
deployment_succeeded=0
worker_id=

recover_on_failure() {
    local exit_code=$?
    trap - EXIT
    if (( deployment_started && ! deployment_succeeded )); then
        set +e
        echo >&2
        echo "UPDATE FAILED — attempting fail-closed worker recovery." >&2
        if [[ -n "$worker_id" ]]; then
            operator fleet cordon --worker-id "$worker_id" \
                --reason 'worker updater: failed deployment' >/dev/null 2>&1 || \
                echo "WARNING: cordon could not be confirmed; inspect the worker policy before continuing." >&2
        fi
        if (( safe_to_stop_worker )); then
            systemctl stop "$worker_unit" >/dev/null 2>&1
        fi
        echo "Inspect:  systemctl status $worker_unit" >&2
        echo "Recover: sudo $repo_root/scripts/update-worker-host.sh --profile $profile --no-build" >&2
    fi
    exit "$exit_code"
}
trap recover_on_failure EXIT

echo "==> Pre-deployment checks"
systemctl is-enabled --quiet "$worker_unit" || {
    echo "error: $worker_unit must be enabled" >&2
    exit 1
}
operator status

registration=$(worker_command fleet check)
printf '%s\n' "$registration"
worker_id=$(sed -nE 's/.* registration=authorized worker_id=([^ ]+).*/\1/p' \
    <<<"$registration" | tail -n 1)
[[ "$worker_id" =~ ^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$ ]] || {
    echo "error: could not derive an authorized worker UUID from fleet check" >&2
    exit 1
}

operator fleet status
# Require the new read/wait surface before changing any policy.
initial_snapshot=$(operator fleet worker-status --worker-id "$worker_id" --json)
previous_session=$(json_field session_id <<<"$initial_snapshot")
operator fleet wait-worker --help >/dev/null
enabled=$(json_field enabled <<<"$initial_snapshot")
[[ "$enabled" == true ]] || {
    echo "error: worker $worker_id is disabled" >&2
    exit 1
}

echo "==> Cordon worker and wait for targeted quiescence"
deployment_started=1
cordon=$(operator fleet cordon --worker-id "$worker_id" \
    --reason 'worker updater: binary maintenance' --json)
generation=$(json_field generation <<<"$cordon")
quiescent=$(operator fleet wait-worker --worker-id "$worker_id" \
    --generation "$generation" --until quiescent --timeout-seconds "$timeout_secs" --json)
printf '%s\n' "$quiescent"
if [[ $(json_field session_live <<<"$quiescent") == true ]] && \
    python3 -c 'import json, sys; sys.exit("shutdown_after_idle" not in json.load(sys.stdin)["protocol_features"])' <<<"$quiescent"; then
    shutdown=$(operator fleet shutdown-after-idle --worker-id "$worker_id" \
        --reason 'worker updater: stop quiescent session' --json)
    operator fleet wait-worker --worker-id "$worker_id" \
        --generation "$(json_field generation <<<"$shutdown")" \
        --session-id "$(json_field target_session_id <<<"$shutdown")" \
        --until deregistered --timeout-seconds "$timeout_secs" --json
fi
safe_to_stop_worker=1
systemctl stop "$worker_unit"
systemctl is-active --quiet "$worker_unit" && {
    echo "error: failed to stop $worker_unit" >&2
    exit 1
}

echo "==> Build and install worker release"
installer_args=()
if (( ! build_enabled )); then
    installer_args+=(--no-build)
fi
"$repo_root/scripts/install-worker.sh" "${installer_args[@]}"
systemd-analyze verify /etc/systemd/system/sbgh-worker@.service

echo "==> Verify worker host and production fleet transport"
runuser -u sbgh-worker -- env RUST_LOG=info \
    "$worker" --config "$worker_config" --preflight-only
post_registration=$(worker_command fleet check)
printf '%s\n' "$post_registration"
grep -q "registration=authorized worker_id=$worker_id " <<<"$post_registration" || {
    echo "error: updated worker authenticated as a different worker" >&2
    exit 1
}
grep -q ' draining=true ' <<<" $post_registration " || {
    echo "error: worker policy was not fail-closed before reactivation" >&2
    exit 1
}

echo "==> Return worker to service"
if grep -q ' cordon_semantic=resident ' <<<" $post_registration "; then
    systemctl start "$worker_unit"
    wait_for_worker "$worker_id" true
fi
# Once admission reopens, an unrelated incoming job may already be executing.
# Failure recovery may cordon again, but must not blindly stop that execution.
safe_to_stop_worker=0
operator fleet uncordon --worker-id "$worker_id" \
    --reason 'worker updater: preflight and registration verified'
if ! grep -q ' cordon_semantic=resident ' <<<" $post_registration "; then
    systemctl start "$worker_unit"
fi
wait_for_worker "$worker_id" false

operator fleet status
systemctl is-active --quiet "$worker_unit"

deployment_succeeded=1
echo
echo "Update complete: worker $worker_id is updated, online, and uncordoned; scheduler and task admission policies still apply."
