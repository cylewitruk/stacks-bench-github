#!/usr/bin/env bash
# Build a host-native launcher against the exact deployed predecessor source.
set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
revision=0f18950ad086799015fd1af0d66a7e672a3e5e27
git -C "$repo_root" cat-file -e "$revision^{commit}"
fixture_root=$(mktemp -d "${TMPDIR:-/tmp}/sbgh-revision-one.XXXXXX")
trap 'rm -rf -- "$fixture_root"' EXIT
git -C "$repo_root" archive "$revision" | tar -xf - -C "$fixture_root"
cmp "$fixture_root/proto/sbgh/fleet/v1/fleet.proto" \
    "$repo_root/fixtures/fleet/revision-1/proto/sbgh/fleet/v1/fleet.proto"
install -d "$fixture_root/crates/sbgh-worker/examples"
install -m 0644 "$repo_root/fixtures/fleet/revision-1/worker-probe.rs" \
    "$fixture_root/crates/sbgh-worker/examples/sbgh-previous-worker-probe.rs"
install -d "$fixture_root/crates/sbgh-postgres/examples"
install -m 0644 "$repo_root/fixtures/fleet/revision-1/schema-probe.rs" \
    "$fixture_root/crates/sbgh-postgres/examples/sbgh-previous-schema-probe.rs"

# Separate target root keeps this frozen-lock build out of the current workspace.
export CARGO_TARGET_DIR="$repo_root/target/previous-worker"
export RUSTC_WRAPPER=
cd "$fixture_root"
cargo --locked build -p sbgh-worker --example sbgh-previous-worker-probe
cargo --locked build -p sbgh-postgres --example sbgh-previous-schema-probe
printf 'previous_worker_revision=%s\n' "$revision"
printf 'previous_worker_binary=%s/debug/examples/sbgh-previous-worker-probe\n' "$CARGO_TARGET_DIR"
shasum -a 256 "$CARGO_TARGET_DIR/debug/examples/sbgh-previous-worker-probe"
shasum -a 256 "$CARGO_TARGET_DIR/debug/examples/sbgh-previous-schema-probe"
