set dotenv-load := false

default:
  @just --list

build *args:
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
                    'Usage: just build [--no-sccache]' \
                    '' \
                    'Options:' \
                    '  --no-sccache  Clear RUSTC_WRAPPER for sandboxed agent runs.'
                exit 0
                ;;
            *)
                echo "error: unsupported build option: $1" >&2
                exit 1
                ;;
        esac
    done

    cargo --locked build --all-targets --release

install *args:
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
                    'Usage: just install [--no-sccache]' \
                    '' \
                    'Options:' \
                    '  --no-sccache  Clear RUSTC_WRAPPER for sandboxed agent runs.'
                exit 0
                ;;
            *)
                echo "error: unsupported install option: $1" >&2
                exit 1
                ;;
        esac
    done

    cargo --locked install --path crates/stacks-bench-agent

fmt:
    cargo +nightly --locked fmt --all

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

    RUST_LOG=warn cargo --locked clippy --all-targets -- -D warnings
    cargo check --locked --all-targets
    cargo +nightly --locked fmt --all -- --check

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
    cargo +nightly --locked fmt --all

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
                    '  just test --results -p stacks-bench-agent archive' \
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

    cmd=(
        cargo --locked nextest run
        --workspace
        --no-fail-fast
        --all-targets
        ${pkg_arg[@]+"${pkg_arg[@]}"}
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
