#!/usr/bin/env bash
# Safely deploy a daemon-only update on the single-worker combined host.
# Failures after draining leave the worker drained and, once quiescent, stopped.

set -Eeuo pipefail

repo_root=$(cd "$(dirname "$0")/.." && pwd)
profile=combined
timeout_secs=300
build_enabled=1
backup_enabled=1

usage() {
    cat <<'EOF'
Usage: sudo ./scripts/update-combined-host.sh [OPTIONS]

Drain the only fleet worker, wait for quiescence, back up Postgres, install and
restart the daemon, verify its API and worker-facing TLS path, then return the
local worker to service.

Options:
  --profile NAME       Worker systemd instance/config name (default: combined)
  --timeout-secs N     Wait limit for quiescence and health (default: 300)
  --no-build           Install existing release binaries without rebuilding
  --skip-backup        Skip pg-backup.sh (only with a separately verified backup)
  -h, --help           Show this help

This command is intentionally limited to a fleet with exactly one registered
worker. Use the coordinated fleet runbook for protocol changes or multiple
workers.
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
        --skip-backup)
            backup_enabled=0
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
[[ "$timeout_secs" =~ ^[0-9]+$ && "$timeout_secs" -ge 10 ]] || {
    echo "error: --timeout-secs must be an integer >= 10" >&2
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
daemon_unit=sbgh-daemon.service
worker_unit="sbgh-worker@$profile.service"

for command in runuser systemctl systemd-analyze python3 sha256sum; do
    command -v "$command" >/dev/null || {
        echo "error: required command is missing: $command" >&2
        exit 1
    }
done
for executable in "$cli" "$worker" "$repo_root/scripts/install-daemon.sh"; do
    [[ -x "$executable" ]] || {
        echo "error: required executable is missing: $executable" >&2
        exit 1
    }
done
[[ -r "$worker_config" ]] || {
    echo "error: worker config is not readable: $worker_config" >&2
    exit 1
}
if (( backup_enabled )); then
    if ! systemctl cat sbgh-pg-backup.service >/dev/null 2>&1; then
        [[ -x "$repo_root/scripts/pg-backup.sh" ]] || {
            echo "error: backup service is absent and backup script is not executable: $repo_root/scripts/pg-backup.sh" >&2
            exit 1
        }
    fi
fi

operator() {
    runuser -u sbgh -- "$cli" "$@"
}

worker_command() {
    runuser -u sbgh-worker -- env RUST_LOG=warn \
        "$worker" --config "$worker_config" "$@"
}

metric() {
    local summary=$1
    local key=$2
    local pattern="(^|[[:space:]])${key}=([0-9]+)($|[[:space:]])"
    if [[ "$summary" =~ $pattern ]]; then
        printf '%s\n' "${BASH_REMATCH[2]}"
    else
        echo "error: fleet status omitted metric '$key': $summary" >&2
        return 1
    fi
}

fleet_status() {
    local status
    status=$(operator fleet status)
    printf '%s\n' "$status" >&2
    printf '%s\n' "$status"
}

assert_single_worker_fleet() {
    local status=$1
    local summary=${status%%$'\n'*}
    local workers gaps
    workers=$(metric "$summary" workers)
    gaps=$(metric "$summary" gaps)
    [[ "$workers" -eq 1 ]] || {
        echo "error: combined-host update requires exactly one registered worker (found $workers)" >&2
        return 1
    }
    [[ "$gaps" -eq 0 ]] || {
        echo "error: fleet has $gaps reliable-event gap(s); reconcile them before deployment" >&2
        return 1
    }
}

worker_state() {
    python3 -c '
import json, sys
worker = json.load(sys.stdin)["worker"]
print("\t".join((
    str(worker["enabled"]).lower(),
    str(worker["draining"]).lower(),
    worker.get("session_status") or "offline",
)))
'
}

wait_for_quiescence() {
    local deadline=$((SECONDS + timeout_secs))
    local last_state=
    local status summary attempts cleanup gaps state
    while (( SECONDS < deadline )); do
        if status=$(operator fleet status 2>/dev/null); then
            summary=${status%%$'\n'*}
            attempts=$(metric "$summary" attempts)
            cleanup=$(metric "$summary" cleanup)
            gaps=$(metric "$summary" gaps)
            state="attempts=$attempts cleanup=$cleanup gaps=$gaps"
            if [[ "$state" != "$last_state" ]]; then
                echo "fleet quiescence: $state"
                last_state=$state
            fi
            if [[ "$attempts" -eq 0 && "$cleanup" -eq 0 && "$gaps" -eq 0 ]]; then
                return 0
            fi
        fi
        sleep 2
    done
    echo "error: fleet did not become quiescent within ${timeout_secs}s" >&2
    return 1
}

wait_for_daemon() {
    local deadline=$((SECONDS + timeout_secs))
    while (( SECONDS < deadline )); do
        if systemctl is-active --quiet "$daemon_unit" && operator status >/dev/null 2>&1; then
            operator status
            return 0
        fi
        sleep 1
    done
    echo "error: daemon API did not become healthy within ${timeout_secs}s" >&2
    return 1
}

wait_for_worker() {
    local worker_id=$1
    local deadline=$((SECONDS + timeout_secs))
    local policy state enabled draining session_status
    while (( SECONDS < deadline )); do
        if systemctl is-active --quiet "$worker_unit" \
            && policy=$(operator fleet show-worker --worker-id "$worker_id" 2>/dev/null) \
            && state=$(worker_state <<<"$policy"); then
            IFS=$'\t' read -r enabled draining session_status <<<"$state"
            if [[ "$enabled" == true && "$draining" == false \
                && "$session_status" =~ ^(registered|idle|offered|running)$ ]]; then
                echo "worker online: status=$session_status draining=false"
                return 0
            fi
        fi
        sleep 1
    done
    echo "error: $worker_unit did not become an online, schedulable session within ${timeout_secs}s" >&2
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
        echo "UPDATE FAILED — leaving worker fail-closed." >&2
        if [[ -n "$worker_id" ]]; then
            operator fleet drain --worker-id "$worker_id" >/dev/null 2>&1
        fi
        if (( safe_to_stop_worker )); then
            systemctl stop "$worker_unit" >/dev/null 2>&1
        fi
        echo "Inspect:  systemctl status $daemon_unit $worker_unit" >&2
        echo "Recover: sudo $repo_root/scripts/update-combined-host.sh --profile $profile --no-build" >&2
    fi
    exit "$exit_code"
}
trap recover_on_failure EXIT

echo "==> Pre-deployment checks"
systemctl is-active --quiet "$daemon_unit" || {
    echo "error: $daemon_unit must be active before an update" >&2
    exit 1
}
systemctl is-enabled --quiet "$worker_unit" || {
    echo "error: $worker_unit must be enabled" >&2
    exit 1
}
operator status

registration=$(worker_command fleet check)
printf '%s\n' "$registration"
worker_id=$(sed -nE 's/.* registration=authorized worker_id=([^ ]+).*/\1/p' <<<"$registration" | tail -n 1)
[[ "$worker_id" =~ ^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$ ]] || {
    echo "error: could not derive an authorized worker UUID from fleet check" >&2
    exit 1
}

initial_fleet=$(fleet_status)
assert_single_worker_fleet "$initial_fleet"
grep -Eq "^${worker_id}[[:space:]]" <<<"$initial_fleet" || {
    echo "error: configured worker $worker_id is not the fleet's registered worker" >&2
    exit 1
}
initial_policy=$(operator fleet show-worker --worker-id "$worker_id")
IFS=$'\t' read -r enabled _ _ <<<"$(worker_state <<<"$initial_policy")"
[[ "$enabled" == true ]] || {
    echo "error: worker $worker_id is disabled" >&2
    exit 1
}

echo "==> Drain worker and wait for quiescence"
deployment_started=1
operator fleet drain --worker-id "$worker_id"
wait_for_quiescence
safe_to_stop_worker=1
systemctl stop "$worker_unit"
systemctl is-active --quiet "$worker_unit" && {
    echo "error: failed to stop $worker_unit" >&2
    exit 1
}

if (( backup_enabled )); then
    echo "==> Back up Postgres"
    if systemctl cat sbgh-pg-backup.service >/dev/null 2>&1; then
        systemctl start sbgh-pg-backup.service
        [[ $(systemctl show sbgh-pg-backup.service -p Result --value) == success ]] || {
            echo "error: sbgh-pg-backup.service did not complete successfully" >&2
            exit 1
        }
    else
        "$repo_root/scripts/pg-backup.sh"
    fi
else
    echo "==> Skipping Postgres backup (--skip-backup)"
fi

echo "==> Install daemon-only release"
worker_hash_before=$(sha256sum "$worker" | awk '{print $1}')
installer_args=(--no-start)
if (( ! build_enabled )); then
    installer_args+=(--no-build)
fi
"$repo_root/scripts/install-daemon.sh" "${installer_args[@]}"
worker_hash_after=$(sha256sum "$worker" | awk '{print $1}')
[[ "$worker_hash_before" == "$worker_hash_after" ]] || {
    echo "error: daemon installer unexpectedly changed $worker" >&2
    exit 1
}
systemd-analyze verify /etc/systemd/system/sbgh-daemon.service

echo "==> Restart daemon and verify control plane"
systemctl restart "$daemon_unit"
wait_for_daemon
post_restart_fleet=$(fleet_status)
assert_single_worker_fleet "$post_restart_fleet"
post_summary=${post_restart_fleet%%$'\n'*}
[[ $(metric "$post_summary" attempts) -eq 0 \
    && $(metric "$post_summary" cleanup) -eq 0 ]] || {
    echo "error: fleet is not quiescent after daemon restart" >&2
    exit 1
}

echo "==> Verify worker host and production fleet transport"
runuser -u sbgh-worker -- env RUST_LOG=info \
    "$worker" --config "$worker_config" --preflight-only
post_registration=$(worker_command fleet check)
printf '%s\n' "$post_registration"
grep -q "registration=authorized worker_id=$worker_id " <<<"$post_registration" || {
    echo "error: post-restart fleet check authorized a different worker" >&2
    exit 1
}
grep -q ' draining=true ' <<<" $post_registration " || {
    echo "error: worker policy was not fail-closed before reactivation" >&2
    exit 1
}

echo "==> Return worker to service"
operator fleet undrain --worker-id "$worker_id"
systemctl restart "$worker_unit"
wait_for_worker "$worker_id"

final_fleet=$(fleet_status)
assert_single_worker_fleet "$final_fleet"
systemctl is-active --quiet "$daemon_unit"
systemctl is-active --quiet "$worker_unit"

deployment_succeeded=1
echo
echo "Update complete: daemon healthy; worker $worker_id is online and schedulable."
