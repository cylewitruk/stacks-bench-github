# Multi-stage build for the handler, cli, and smee binaries. Build the
# workspace once, then carve out a slim runtime image per binary via separate
# targets.
#
# Build either with:
#   docker build -f docker/Dockerfile --target handler -t sbgh-handler .
#   docker build -f docker/Dockerfile --target cli     -t sbgh-cli     .
#   docker build -f docker/Dockerfile --target smee    -t sbgh-smee    .
# Or via docker-compose, which picks the right `target` for each service.

# ─── 1. Build stage ─────────────────────────────────────────────────────
FROM rust:1.95.0-slim-trixie AS builder

# `slim-trixie` strips docs + non-essential bits from the official rust image
# but keeps gcc/ld, which is enough to link the C bits in `ring` (used by
# rustls + jsonwebtoken). Add packages here only if a future dep needs them.

WORKDIR /build

# Workspace + migrations. We copy the whole tree because the workspace has
# inter-crate path deps; trying to be clever with deps-only caching here
# would need cargo-chef and isn't worth it for a low-frequency build.
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY migrations ./migrations

# Build only the binaries packaged into the runtime images.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --release -p sbgh-handler -p sbgh-cli -p sbgh-smee && \
    cp target/release/sbgh-handler /usr/local/bin/sbgh-handler && \
    cp target/release/sbgh-cli     /usr/local/bin/sbgh-cli && \
    cp target/release/sbgh-smee    /usr/local/bin/sbgh-smee

# ─── 2. Common runtime layer ────────────────────────────────────────────
# Slim Debian + just the runtime libs reqwest/rustls/sqlx-postgres need.
FROM debian:trixie-slim AS runtime

# Container uid/gid for the in-container service account. Used by the
# handler and smee images. Must match the host user that owns the bind-
# mounted config dir for that service (see docker/.env: SBGH_UID/SBGH_GID
# defaults to 901 — the host sbgh-handler user). Deliberately distinct
# from:
#   - the postgres container's uid (default 900, see compose)
#   - the host orchestrator's uid (default 902, see docs/host-bringup.md §3)
# so a compromised handler container can't share the orchestrator's or
# postgres's on-disk identity. We pick numbers low in the system uid
# range (100–999) to avoid the common Ubuntu allocations near the top of
# that range (997 systemd-timesync, 998 systemd-network, 999 dnsmasq).
# Override at build time:
#   docker compose build --build-arg SBGH_UID=$(id -u sbgh-handler) ...
ARG SBGH_UID=901
ARG SBGH_GID=901

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid ${SBGH_GID} sbgh \
    && useradd  --system --uid ${SBGH_UID} --gid ${SBGH_GID} \
                --home-dir /home/sbgh --create-home \
                --shell /usr/sbin/nologin sbgh

USER sbgh
WORKDIR /home/sbgh

# ─── 3a. sbgh-cli image ─────────────────────────────────────────────────
# Operator CLI: `sbgh-cli migrate` (one-shot at deploy time, runs as the
# DB *owner* role to apply schema + (re)create runtime roles) and
# `sbgh-cli installer ...` (admin commands for the allowed_installer
# allowlist). Compose wires the handler + orchestrator to wait on the
# `migrate` subcommand completing successfully.
FROM runtime AS cli
COPY --from=builder /usr/local/bin/sbgh-cli /usr/local/bin/sbgh-cli
ENTRYPOINT ["/usr/local/bin/sbgh-cli"]

# ─── 3b. sbgh-handler image ─────────────────────────────────────────────
FROM runtime AS handler

COPY --from=builder /usr/local/bin/sbgh-handler /usr/local/bin/sbgh-handler

# The handler binds 0.0.0.0:8080 by default (overridable via SBGH_BIND_ADDR
# or [server].bind_addr in the TOML).
EXPOSE 8080

# Default config path inside the container. Compose bind-mounts the host's
# handler-owned config dir to /etc/sbgh/handler:ro so this resolves 1:1
# with the host path.
ENV SBGH_CONFIG=/etc/sbgh/handler/config.toml

ENTRYPOINT ["/usr/local/bin/sbgh-handler"]

# ─── 3c. sbgh-smee image ────────────────────────────────────────────────
FROM runtime AS smee

COPY --from=builder /usr/local/bin/sbgh-smee /usr/local/bin/sbgh-smee

# No EXPOSE — smee is outbound-only (SSE → smee.io, HTTP → handler).
# --channel and --target are passed as `command:` from docker-compose.

ENTRYPOINT ["/usr/local/bin/sbgh-smee"]
