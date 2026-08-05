#!/usr/bin/env bash
# Shared primitives for the daemon and worker installers. Source this file;
# it is not an installer entry point.

sbgh_install_init() {
    local repo_root=$1

    SBGH_INSTALL_REPO_ROOT=$repo_root
    SBGH_INSTALL_DESTDIR=${DESTDIR:-}
    if [[ -n "$SBGH_INSTALL_DESTDIR" ]]; then
        if [[ "$SBGH_INSTALL_DESTDIR" != /* || "$SBGH_INSTALL_DESTDIR" == "/" ]]; then
            echo "DESTDIR must be an absolute staging directory other than /." >&2
            return 2
        fi
        SBGH_INSTALL_DESTDIR=${SBGH_INSTALL_DESTDIR%/}
    elif [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
        echo "Must run as root (use sudo), or set DESTDIR for a staging install." >&2
        return 2
    fi
}

sbgh_install_path() {
    local absolute_path=$1
    printf '%s%s\n' "$SBGH_INSTALL_DESTDIR" "$absolute_path"
}

sbgh_require_file() {
    local path=$1
    local description=$2
    if [[ ! -f "$path" ]]; then
        echo "$description not found at $path." >&2
        return 1
    fi
}

sbgh_require_executable() {
    local path=$1
    local description=$2
    if [[ ! -x "$path" ]]; then
        echo "$description not found at $path after the build step." >&2
        return 1
    fi
}

sbgh_build_release() {
    local build_enabled=$1
    shift
    if [[ "$build_enabled" -eq 0 ]]; then
        echo "Skipping build (--no-build)."
        return
    fi

    local build_user=${SUDO_USER:-}
    if [[ -z "$build_user" || "$build_user" == "root" ]]; then
        echo "Refusing to build as root because that would change target/ ownership." >&2
        echo "Run through sudo from a non-root shell, or pass --no-build." >&2
        return 1
    fi

    local build_home cargo_bin
    build_home=$(getent passwd "$build_user" | cut -d: -f6)
    cargo_bin="$build_home/.cargo/bin/cargo"
    if [[ ! -x "$cargo_bin" ]]; then
        cargo_bin=$(command -v cargo || true)
    fi
    if [[ -z "$cargo_bin" || ! -x "$cargo_bin" ]]; then
        echo "cargo not found for build user $build_user." >&2
        return 1
    fi

    local cargo_args=(
        build
        --manifest-path "$SBGH_INSTALL_REPO_ROOT/Cargo.toml"
        --locked
        --release
    )
    local package
    for package in "$@"; do
        cargo_args+=(-p "$package")
    done

    echo "Building release packages as $build_user: $*"
    sudo -u "$build_user" -H "$cargo_bin" "${cargo_args[@]}"
}

sbgh_install_file() {
    local mode=$1
    local source=$2
    local absolute_destination=$3
    local destination parent
    destination=$(sbgh_install_path "$absolute_destination")
    parent=$(dirname "$destination")
    if [[ ! -d "$parent" ]]; then
        install -d -m 0755 "$parent"
    fi
    install -m "$mode" "$source" "$destination"
}

sbgh_install_directory() {
    local mode=$1
    local absolute_destination=$2
    local destination
    destination=$(sbgh_install_path "$absolute_destination")
    install -d -m "$mode" "$destination"
}

sbgh_reload_systemd() {
    if [[ -n "$SBGH_INSTALL_DESTDIR" ]]; then
        echo "Skipping systemd reload for staging root $SBGH_INSTALL_DESTDIR."
    else
        systemctl daemon-reload
    fi
}
