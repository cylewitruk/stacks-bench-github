set dotenv-load := false

default:
  @just --list

build *args:
    #!/usr/bin/env bash
    set -euo pipefail
    set -- {{args}}

    profile_args=()

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --release)
                profile_args=(--release)
                shift
                ;;
            --no-sccache)
                export RUSTC_WRAPPER=
                shift
                ;;
            -h|--help)
                printf '%s\n' \
                    'Usage: just build [--release] [--no-sccache]' \
                    '' \
                    'Options:' \
                    '  --release     Build with Cargo'\''s release profile. Defaults to dev.' \
                    '  --no-sccache  Clear RUSTC_WRAPPER for sandboxed agent runs.'
                exit 0
                ;;
            *)
                echo "error: unsupported build option: $1" >&2
                exit 1
                ;;
        esac
    done

    cargo --locked build --all-targets "${profile_args[@]}"

fmt:
    cargo --locked fmt --all

check-docs:
    python3 scripts/check-docs.py

check-architecture:
    python3 scripts/check-package-dag.py

lint *args:
    #!/usr/bin/env bash
    set -euo pipefail
    set -- {{args}}

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --no-sccache)
                export RUSTC_WRAPPER=
                shift
                ;;
            -h|--help)
                printf '%s\n' \
                    'Usage: just lint [--no-sccache]' \
                    '' \
                    'Options:' \
                    '  --no-sccache  Clear RUSTC_WRAPPER for sandboxed agent runs.'
                exit 0
                ;;
            *)
                echo "error: unsupported lint option: $1" >&2
                exit 1
                ;;
        esac
    done

    python3 scripts/check-docs.py
    python3 scripts/check-task-submission-names.py
    python3 scripts/check-reporting-boundaries.py
    python3 scripts/check-task-aware-intake.py
    python3 scripts/check-protobuf-boundary.py
    python3 scripts/check-worker-registry-boundary.py
    python3 scripts/check-package-dag.py
    python3 scripts/check-sandbox-network-assets.py \
        --xml network/sandbox-egress.xml \
        --nft network/sandbox-egress.nft \
        --protected-ipv4 network/protected-ipv4.conf.example
    python3 scripts/test-sandbox-network-assets.py
    python3 scripts/test-installers.py
    python3 scripts/test-update-combined-host.py
    python3 scripts/test-update-worker-host.py
    python3 scripts/test-block-validation-progress.py
    for script in scripts/*.sh; do bash -n "$script"; done
    cargo machete --with-metadata
    RUST_LOG=warn cargo --locked clippy --all-targets -- -D warnings
    cargo --locked fmt --all -- --check

fix *args:
    #!/usr/bin/env bash
    set -euo pipefail
    set -- {{args}}

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --no-sccache)
                export RUSTC_WRAPPER=
                shift
                ;;
            -h|--help)
                printf '%s\n' \
                    'Usage: just fix [--no-sccache]' \
                    '' \
                    'Options:' \
                    '  --no-sccache  Clear RUSTC_WRAPPER for sandboxed agent runs.'
                exit 0
                ;;
            *)
                echo "error: unsupported fix option: $1" >&2
                exit 1
                ;;
        esac
    done

    RUST_LOG=warn cargo --locked clippy --fix --all-targets --allow-dirty
    cargo --locked fmt --all

# Run workspace tests (use `just test --help` for modes and filters).
test *args:
    #!/usr/bin/env bash
    set -euo pipefail
    set -- {{args}}

    level="warn"
    level_explicit=0
    mode="full"
    no_sccache=0
    pkg_arg=()
    rest=()
    filters=()
    nocapture=()

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --trace|--debug|--info|--warn|--error)
                level="${1#--}"
                level_explicit=1
                shift
                ;;
            -p|--package)
                shift
                if [[ $# -eq 0 ]]; then
                    echo "error: -p/--package requires a package" >&2
                    exit 1
                fi
                pkg_arg=(-p "$1")
                shift
                ;;
            --nocapture|--no-capture)
                nocapture=(--no-capture)
                shift
                ;;
            --summary)
                mode="summary"
                shift
                ;;
            --results)
                mode="results"
                shift
                ;;
            --failures)
                mode="failures"
                shift
                ;;
            --no-sccache)
                no_sccache=1
                shift
                ;;
            -h|--help)
                printf '%s\n' \
                    'Usage:' \
                    '  just test [OPTIONS] [NEXTEST_FILTERS_OR_ARGS...]' \
                    '  just test-summary [--no-sccache] [NEXTEST_FILTERS_OR_ARGS...]' \
                    '  just test-failures [--no-sccache] [NEXTEST_FILTERS_OR_ARGS...]' \
                    '' \
                    'Wrapper options:' \
                    '  --summary       Suppress per-test output; print nextest header + summary.' \
                    '  --failures      Print failing tests and captured failure output at the end.' \
                    '  --results       Print per-test PASS/FAIL statuses without captured success output.' \
                    '  --no-sccache    Clear RUSTC_WRAPPER for sandboxed agent runs.' \
                    '  --trace|--debug|--info|--warn|--error' \
                    '                  Set RUST_LOG. Defaults to warn, or debug when filters are passed.' \
                    '  -p, --package   Forward a package selector to nextest.' \
                    '  --nocapture     Forward --no-capture to nextest.' \
                    '' \
                    'Examples:' \
                    '  just test' \
                    '  just test archive' \
                    '  just test --summary archive' \
                    '  just test --failures archive' \
                    '  just test --results -p sbgh-daemon runner' \
                    '  just test --no-sccache pull_rebase_with_auth_fast_forwards_against_local_remote'
                exit 0
                ;;
            --)
                shift
                (($#)) && filters+=("$@")
                break
                ;;
            -*)
                rest+=("$1")
                shift
                ;;
            *)
                filters+=("$1")
                shift
                ;;
        esac
    done

    if [[ ${#filters[@]} -gt 0 && $level_explicit -eq 0 ]]; then
        level="debug"
    fi
    export RUST_LOG="$level"

    if [[ $no_sccache -eq 1 ]]; then
        export RUSTC_WRAPPER=
    fi

    selection=(--workspace)
    if [[ ${#pkg_arg[@]} -gt 0 ]]; then
        selection=("${pkg_arg[@]}")
    fi

    cmd=(
        cargo --locked nextest run
        "${selection[@]}"
        --no-fail-fast
        --all-targets
        ${rest[@]+"${rest[@]}"}
        ${nocapture[@]+"${nocapture[@]}"}
    )

    case "$mode" in
        summary)
            cmd+=(
                --status-level none
                --final-status-level none
                --failure-output never
                --success-output never
                --show-progress none
                --cargo-quiet
            )
            ;;
        results)
            cmd+=(
                --status-level pass
                --final-status-level none
                --failure-output final
                --success-output never
                --show-progress none
            )
            ;;
        failures)
            cmd+=(
                --status-level fail
                --final-status-level fail
                --failure-output final
                --success-output never
                --show-progress none
            )
            ;;
    esac

    exec "${cmd[@]}" "${filters[@]+"${filters[@]}"}"

test-summary *args:
    @just test --summary {{args}}

test-failures *args:
    @just test --failures {{args}}
