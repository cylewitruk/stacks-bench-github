#!/usr/bin/env bash
# Pre-flight check for an orchestrator host. Verifies that every piece the
# benchmark driver will touch — LVM, libvirt, filesystem layout, sbgh user,
# sudoers, golden image, GitHub App secrets, Postgres reachability — is
# present and wired correctly. Read-only; never modifies system state.
#
# Run as root (or with sudo). Reads the same config the orchestrator will:
#   $SBGH_CONFIG  if set,
#   else /etc/sbgh/config.toml,
#   else $HOME/.config/sbgh/config.toml.
#
# Exit status:
#   0  — all checks passed (warnings allowed)
#   1  — at least one check failed
#   2  — script-level error (missing config, missing python, etc.)

set -uo pipefail

# ─── output helpers ────────────────────────────────────────────────────
RED=$(tput setaf 1 2>/dev/null || true)
GRN=$(tput setaf 2 2>/dev/null || true)
YLW=$(tput setaf 3 2>/dev/null || true)
BLD=$(tput bold    2>/dev/null || true)
RST=$(tput sgr0    2>/dev/null || true)

PASS_COUNT=0
FAIL_COUNT=0
WARN_COUNT=0

section() { echo; echo "${BLD}── $* ──${RST}"; }
pass()    { echo "  ${GRN}[PASS]${RST} $*"; PASS_COUNT=$((PASS_COUNT+1)); }
fail()    { echo "  ${RED}[FAIL]${RST} $*"; FAIL_COUNT=$((FAIL_COUNT+1)); }
warn()    { echo "  ${YLW}[WARN]${RST} $*"; WARN_COUNT=$((WARN_COUNT+1)); }
info()    { echo "         $*"; }

# ─── locate + parse config ─────────────────────────────────────────────
# Only the orchestrator config is parsed here — it has everything we need
# to validate (LVM, VM, GitHub credentials, DB URL, paths). The handler
# config is much narrower (webhook secret + allowlist) and is sanity-
# checked separately in §12 (presence + ownership only).
#
# Search order matches `OrchestratorConfig::load`:
#   $SBGH_CONFIG → /etc/sbgh/orchestrator/config.toml → $HOME/.config/sbgh/orchestrator/config.toml.
SBGH_HOME=$(getent passwd sbgh 2>/dev/null | cut -d: -f6)

locate_config() {
    if [[ -n "${SBGH_CONFIG:-}" ]] && [[ -r "$SBGH_CONFIG" ]]; then
        echo "$SBGH_CONFIG"; return
    fi
    if [[ -r /etc/sbgh/orchestrator/config.toml ]]; then
        echo /etc/sbgh/orchestrator/config.toml; return
    fi
    if [[ -n "$SBGH_HOME" ]] && [[ -r "$SBGH_HOME/.config/sbgh/orchestrator/config.toml" ]]; then
        echo "$SBGH_HOME/.config/sbgh/orchestrator/config.toml"; return
    fi
    if [[ -n "${HOME:-}" ]] && [[ -r "$HOME/.config/sbgh/orchestrator/config.toml" ]]; then
        echo "$HOME/.config/sbgh/orchestrator/config.toml"; return
    fi
    echo ""
}

CONFIG_PATH=$(locate_config)
if [[ -z "$CONFIG_PATH" ]]; then
    echo "${RED}No orchestrator config file found.${RST} Looked at:" >&2
    echo "  - \$SBGH_CONFIG (\"${SBGH_CONFIG:-unset}\")" >&2
    echo "  - /etc/sbgh/orchestrator/config.toml" >&2
    if [[ -n "$SBGH_HOME" ]]; then
        echo "  - $SBGH_HOME/.config/sbgh/orchestrator/config.toml" >&2
    fi
    echo "  - \$HOME/.config/sbgh/orchestrator/config.toml (\"${HOME:-unset}/.config/sbgh/orchestrator/config.toml\")" >&2
    exit 2
fi

if ! command -v python3 >/dev/null; then
    echo "${RED}python3 not found${RST} — sanity check needs python3 to parse the TOML config" >&2
    exit 2
fi

echo "${BLD}sbgh host sanity check${RST}"
echo "Orchestrator config: $CONFIG_PATH"

# Pull everything we need out of the TOML in one shot. tomllib is stdlib
# from Python 3.11+ (Debian 12 / Ubuntu 24.04 both ship 3.11+).
if ! CONFIG_VARS=$(python3 <<EOF 2>&1
import tomllib, shlex, sys
try:
    with open("$CONFIG_PATH", "rb") as f:
        d = tomllib.load(f)
except Exception as e:
    print(f"echo 'TOML parse error: {e}' >&2; exit 2")
    sys.exit(0)

def g(p, default=""):
    cur = d
    for k in p.split("."):
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k, default)
    return cur if cur is not None else default

vars = {
    "CFG_VG":            g("lvm.vg_name", "vg0"),
    "CFG_THINPOOL":      g("lvm.thinpool", "thinpool"),
    "CFG_PREFIX":        g("lvm.chainstate_base_prefix", "mainnet-"),
    "CFG_GOLDEN":        g("vm.golden_image"),
    "CFG_NETWORK":       g("vm.network", "default"),
    "CFG_VM_VCPUS":      g("vm.vcpus", 2),
    "CFG_VM_MEMORY":     g("vm.memory_gib", 8),
    "CFG_JOBS_DIR":      g("paths.jobs_dir", "/var/lib/sbgh/jobs"),
    "CFG_RESULTS_DIR":   g("paths.results_archive_dir", "/var/lib/sbgh/results"),
    "CFG_GIT_MIRROR":    g("paths.git_mirror", "/var/lib/sbgh/git/stacks-core.git"),
    "CFG_TMPFS_ROOT":    g("paths.results_tmpfs_root", "/run/sbgh/jobs"),
    "CFG_VIRSH":         g("paths.virsh_binary", "/usr/bin/virsh"),
    "CFG_SUDO":          g("paths.sudo_binary", "/usr/bin/sudo"),
    "CFG_QEMU_IMG":      g("paths.qemu_img_binary", "/usr/bin/qemu-img"),
    "CFG_CLOUD_LOCALDS": g("paths.cloud_localds_binary", "/usr/bin/cloud-localds"),
    "CFG_GIT_BIN":       g("paths.git_binary", "/usr/bin/git"),
    "CFG_PRIVATE_KEY":   g("github.private_key_path"),
    "CFG_CLIENT_ID":     g("github.client_id"),
    "CFG_DATABASE_URL":  g("server.database_url"),
    "CFG_SERVICE_USER":  g("server.service_user", "sbgh"),
}
for k, v in vars.items():
    print(f"{k}={shlex.quote(str(v))}")
EOF
); then
    echo "Failed to parse $CONFIG_PATH" >&2
    echo "$CONFIG_VARS" >&2
    exit 2
fi
eval "$CONFIG_VARS"

# Env can still override secrets, matching the orchestrator's loader semantics.
CFG_DATABASE_URL="${DATABASE_URL:-$CFG_DATABASE_URL}"
CFG_CLIENT_ID="${SBGH_GH_CLIENT_ID:-$CFG_CLIENT_ID}"
CFG_PRIVATE_KEY="${SBGH_GH_PRIVATE_KEY_PATH:-$CFG_PRIVATE_KEY}"

# ─── 1. Required tools ─────────────────────────────────────────────────
section "1. Required tools"
for cmd in virsh qemu-img cloud-localds git \
           lvcreate lvremove lvs lvchange \
           mkfs.ext4 mkfs.xfs losetup mount umount chown truncate \
           sudo curl awk grep sed psql; do
    if command -v "$cmd" >/dev/null; then
        pass "$cmd"
    else
        case "$cmd" in
            virsh|virt-*)         hint="apt install libvirt-clients libvirt-daemon-system" ;;
            qemu-img)             hint="apt install qemu-utils" ;;
            cloud-localds)        hint="apt install cloud-image-utils" ;;
            mkfs.xfs)             hint="apt install xfsprogs" ;;
            mkfs.ext4|losetup)    hint="apt install e2fsprogs util-linux" ;;
            lvcreate|lvremove|lvs|lvchange) hint="apt install lvm2" ;;
            psql)                 hint="apt install postgresql-client" ;;
            *) hint="" ;;
        esac
        fail "$cmd missing${hint:+  → $hint}"
    fi
done

# virtiofsd is special: libvirt invokes it directly (not via PATH), and on
# Debian/Ubuntu it lives at /usr/libexec/virtiofsd, not in $PATH. Check
# both locations so a /usr/libexec install passes.
if command -v virtiofsd >/dev/null || [[ -x /usr/libexec/virtiofsd ]]; then
    pass "virtiofsd (backs the virtio-fs results share)"
else
    fail "virtiofsd missing  → apt install virtiofsd  (without it, virsh start fails with 'Unable to find a satisfying virtiofsd')"
fi

# ─── 2. Virtualization ─────────────────────────────────────────────────
section "2. Virtualization"
if grep -qE 'vmx|svm' /proc/cpuinfo 2>/dev/null; then
    pass "CPU exposes virtualization extensions"
else
    fail "CPU lacks vmx/svm flag — KVM won't work"
fi
if [[ -c /dev/kvm ]]; then
    pass "/dev/kvm exists"
else
    fail "/dev/kvm not present — kvm kernel module not loaded?"
fi
if systemctl is-active --quiet libvirtd; then
    pass "libvirtd is running"
else
    fail "libvirtd is not running  → systemctl start libvirtd"
fi

# ─── 3. libvirt network ────────────────────────────────────────────────
section "3. libvirt network: $CFG_NETWORK"
if virsh net-info "$CFG_NETWORK" >/dev/null 2>&1; then
    pass "network '$CFG_NETWORK' is defined"
    if virsh net-info "$CFG_NETWORK" 2>/dev/null | awk '/^Active/{print $2}' | grep -q yes; then
        pass "network '$CFG_NETWORK' is active"
    else
        fail "network '$CFG_NETWORK' is inactive  → virsh net-start $CFG_NETWORK"
    fi
else
    fail "network '$CFG_NETWORK' not defined  → virsh net-define + virsh net-start"
fi

# ─── 4. LVM layout ─────────────────────────────────────────────────────
section "4. LVM"
if vgs --noheadings -o vg_name 2>/dev/null | awk '{print $1}' | grep -qx "$CFG_VG"; then
    pass "VG '$CFG_VG' exists"
    # Free space sanity (warn if low).
    vfree=$(vgs --noheadings --units g -o vg_free "$CFG_VG" 2>/dev/null | tr -d ' g')
    if [[ -n "$vfree" ]] && (( $(awk "BEGIN{print ($vfree>10)?1:0}") )); then
        pass "VG '$CFG_VG' has ${vfree}G free"
    else
        warn "VG '$CFG_VG' free space looks low: ${vfree:-unknown}G"
    fi
else
    fail "VG '$CFG_VG' not found  → vgs to list, fix [lvm].vg_name in config"
fi

if lvs --noheadings -o lv_name "$CFG_VG" 2>/dev/null | awk '{print $1}' | grep -qx "$CFG_THINPOOL"; then
    pass "thin pool '$CFG_VG/$CFG_THINPOOL' exists"
    pool_attr=$(lvs --noheadings -o lv_attr "$CFG_VG/$CFG_THINPOOL" 2>/dev/null | tr -d ' ')
    if [[ "$pool_attr" =~ ^t ]]; then
        pass "'$CFG_THINPOOL' is a thin-pool ($pool_attr)"
        pool_data=$(lvs --noheadings -o data_percent "$CFG_VG/$CFG_THINPOOL" 2>/dev/null | tr -d ' ')
        info "thin pool data utilization: ${pool_data:-unknown}%"
    else
        fail "'$CFG_THINPOOL' has attr $pool_attr, expected to start with 't' (thin pool)"
    fi
else
    fail "thin pool '$CFG_VG/$CFG_THINPOOL' not found"
fi

# Discover chainstate base LVs the orchestrator would consider.
mapfile -t bases < <(lvs --noheadings -o lv_name "$CFG_VG" 2>/dev/null \
    | awk '{print $1}' \
    | grep -E "^${CFG_PREFIX}" \
    | sort)
if (( ${#bases[@]} == 0 )); then
    fail "no base chainstate LV in '$CFG_VG' matching prefix '$CFG_PREFIX'"
    info "→ run scripts/download-chainstate.sh, or create one manually"
else
    pass "found ${#bases[@]} chainstate base(s) matching '$CFG_PREFIX'"
    for b in "${bases[@]}"; do info "  - $b"; done
    info "orchestrator will pick: ${bases[-1]} (lexicographically newest)"
fi

# ─── 5. Filesystem paths ───────────────────────────────────────────────
section "5. Filesystem paths"
for p in "$CFG_JOBS_DIR" "$CFG_RESULTS_DIR" "$CFG_TMPFS_ROOT"; do
    if [[ -d "$p" ]]; then
        owner=$(stat -c '%U:%G' "$p" 2>/dev/null)
        mode=$(stat -c '%a' "$p" 2>/dev/null)
        if [[ "$owner" == "$CFG_SERVICE_USER:$CFG_SERVICE_USER" ]]; then
            pass "$p (mode $mode, owner $owner)"
        else
            warn "$p exists but owner is $owner (expected $CFG_SERVICE_USER:$CFG_SERVICE_USER, mode $mode)"
        fi
    else
        fail "$p does not exist  → install -d -m 0755 -o $CFG_SERVICE_USER -g $CFG_SERVICE_USER $p"
    fi
done

# git mirror parent dir (the .git itself may not exist until first job)
git_mirror_parent=$(dirname "$CFG_GIT_MIRROR")
if [[ -d "$git_mirror_parent" ]]; then
    owner=$(stat -c '%U:%G' "$git_mirror_parent" 2>/dev/null)
    if [[ "$owner" == "$CFG_SERVICE_USER:$CFG_SERVICE_USER" ]]; then
        pass "git mirror parent $git_mirror_parent (owner $owner)"
    else
        warn "git mirror parent $git_mirror_parent owner is $owner (expected $CFG_SERVICE_USER)"
    fi
    if [[ -d "$CFG_GIT_MIRROR" ]]; then
        info "  bare mirror already present at $CFG_GIT_MIRROR"
    else
        info "  bare mirror not yet cloned — first job will create it"
    fi
else
    fail "git mirror parent $git_mirror_parent does not exist"
fi

# Config dirs — security boundary. Each must be owned by the right user
# (different uid each!) and mode 0700 so the other user can't read it.
for entry in "/etc/sbgh/handler:sbgh-handler" "/etc/sbgh/orchestrator:sbgh"; do
    dir="${entry%%:*}"
    expected_owner="${entry##*:}"
    if [[ -d "$dir" ]]; then
        owner=$(stat -c '%U' "$dir" 2>/dev/null)
        mode=$(stat -c '%a' "$dir" 2>/dev/null)
        if [[ "$owner" == "$expected_owner" ]] && [[ "$mode" == "700" ]]; then
            pass "$dir (mode $mode, owner $owner)"
        elif [[ "$owner" != "$expected_owner" ]]; then
            fail "$dir owner is $owner, expected $expected_owner"
        else
            warn "$dir owner is correct ($owner) but mode is $mode (expected 700)"
        fi
    else
        fail "$dir does not exist  → install -d -m 0700 -o $expected_owner -g $expected_owner $dir"
    fi
done

# ─── 6. Service user + groups ──────────────────────────────────────────
section "6. Service users"
# Two host users, one per service. See docs/host-bringup.md §3.
#   - sbgh-handler (uid 997): identity for the handler container
#   - sbgh         (uid 998): runs the orchestrator binary on the host

# Read handler uid override from docker/.env if present.
expected_handler_uid=901
expected_handler_gid=901
for candidate in \
        "$(dirname "$0")/../docker/.env" \
        "./docker/.env"; do
    if [[ -f "$candidate" ]]; then
        override_uid=$(grep -E '^SBGH_UID=' "$candidate" 2>/dev/null | tail -1 | cut -d= -f2- | tr -d '"' | tr -d "'" | tr -d ' ')
        override_gid=$(grep -E '^SBGH_GID=' "$candidate" 2>/dev/null | tail -1 | cut -d= -f2- | tr -d '"' | tr -d "'" | tr -d ' ')
        [[ -n "$override_uid" ]] && expected_handler_uid="$override_uid"
        [[ -n "$override_gid" ]] && expected_handler_gid="$override_gid"
        break
    fi
done

# --- sbgh-handler (uid 997) ---
if id sbgh-handler >/dev/null 2>&1; then
    pass "user 'sbgh-handler' exists"
    h_uid=$(id -u sbgh-handler)
    h_gid=$(id -g sbgh-handler)
    if [[ "$h_uid" == "$expected_handler_uid" ]] && [[ "$h_gid" == "$expected_handler_gid" ]]; then
        pass "sbgh-handler uid/gid $h_uid/$h_gid matches container expectation ($expected_handler_uid/$expected_handler_gid)"
    else
        fail "sbgh-handler uid/gid is $h_uid/$h_gid, but containers expect $expected_handler_uid/$expected_handler_gid"
        info "→ set SBGH_UID=$h_uid SBGH_GID=$h_gid in docker/.env and rebuild:"
        info "  docker compose -f docker/docker-compose.yml build --no-cache"
    fi
else
    fail "user 'sbgh-handler' missing  → groupadd --system --gid 901 sbgh-handler && useradd --system --uid 901 --gid 901 --shell /usr/sbin/nologin sbgh-handler"
fi

# --- sbgh (uid 998, orchestrator) ---
if id "$CFG_SERVICE_USER" >/dev/null 2>&1; then
    pass "user '$CFG_SERVICE_USER' exists"
    groups=$(id -nG "$CFG_SERVICE_USER")
    if [[ " $groups " == *" libvirt "* ]]; then
        pass "$CFG_SERVICE_USER is in 'libvirt' group"
    else
        warn "$CFG_SERVICE_USER not in 'libvirt' group — virsh access will need sudo"
        info "→ usermod -a -G libvirt $CFG_SERVICE_USER"
    fi

    s_uid=$(id -u "$CFG_SERVICE_USER")
    s_gid=$(id -g "$CFG_SERVICE_USER")
    if [[ "$s_uid" == "902" ]] && [[ "$s_gid" == "902" ]]; then
        pass "$CFG_SERVICE_USER uid/gid 902/902 (recommended)"
    else
        warn "$CFG_SERVICE_USER uid/gid is $s_uid/$s_gid (recommended: 902/902)"
    fi

    # Tripwire: the two service uids MUST differ — sharing one defeats
    # the filesystem boundary between handler and orchestrator config.
    if id sbgh-handler >/dev/null 2>&1; then
        if [[ "$(id -u sbgh-handler)" == "$s_uid" ]]; then
            fail "sbgh-handler and $CFG_SERVICE_USER share uid $s_uid — orchestrator config readable from handler container!"
        fi
    fi
else
    fail "user '$CFG_SERVICE_USER' does not exist  → groupadd --system --gid 902 sbgh && useradd --system --uid 902 --gid 902 --shell /usr/sbin/nologin sbgh"
fi

# ─── 7. Sudoers ────────────────────────────────────────────────────────
section "7. Sudoers (commands sbgh runs via sudo)"
# Group expected privileged commands. The orchestrator hits the first group;
# the chainstate refresh script also needs the second.
orchestrator_cmds=(
    /usr/sbin/lvcreate /usr/sbin/lvremove /usr/sbin/lvs
    /usr/sbin/mkfs.ext4 /usr/sbin/losetup
    /usr/bin/mount /usr/bin/umount /usr/bin/chown
    /usr/bin/virsh
)
chainstate_cmds=(
    /usr/sbin/mkfs.xfs /usr/sbin/lvchange
    /usr/bin/aria2c /usr/bin/mkdir /usr/bin/rmdir
)

check_sudo() {
    local label="$1" cmd="$2"
    # `sudo -u sbgh sudo -n -l <cmd>` succeeds when sbgh has NOPASSWD allowlist
    # for that exact path. Output goes nowhere if allowed.
    if sudo -u "$CFG_SERVICE_USER" sudo -n -l "$cmd" >/dev/null 2>&1; then
        pass "$label: $cmd"
    else
        fail "$label: $cmd  → add to /etc/sudoers.d/sbgh"
    fi
}

if ! id "$CFG_SERVICE_USER" >/dev/null 2>&1; then
    warn "service user missing — skipping sudoers checks"
else
    for c in "${orchestrator_cmds[@]}"; do check_sudo "orchestrator" "$c"; done
    for c in "${chainstate_cmds[@]}";    do check_sudo "chainstate" "$c"; done
fi

# ─── 8. Golden image ───────────────────────────────────────────────────
section "8. Golden image: $CFG_GOLDEN"
if [[ -z "$CFG_GOLDEN" ]]; then
    fail "[vm].golden_image is empty in config"
elif [[ ! -f "$CFG_GOLDEN" ]]; then
    fail "golden image not found at $CFG_GOLDEN  → run scripts/build-golden-image.sh"
else
    sz=$(du -h "$CFG_GOLDEN" | cut -f1)
    if qemu-img info "$CFG_GOLDEN" >/dev/null 2>&1; then
        fmt=$(qemu-img info --output=json "$CFG_GOLDEN" 2>/dev/null \
            | python3 -c "import json,sys; print(json.load(sys.stdin).get('format',''))" 2>/dev/null)
        if [[ "$fmt" == "qcow2" ]]; then
            pass "golden image is a qcow2 ($sz)"
        else
            warn "golden image format is '$fmt', expected qcow2"
        fi
    else
        fail "qemu-img can't read $CFG_GOLDEN — corrupted?"
    fi
    # Permission check from the perspective of the service user.
    if sudo -u "$CFG_SERVICE_USER" test -r "$CFG_GOLDEN" 2>/dev/null; then
        pass "service user '$CFG_SERVICE_USER' can read it"
    else
        fail "'$CFG_SERVICE_USER' cannot read $CFG_GOLDEN — fix permissions"
    fi
fi

# ─── 9. GitHub App private key ─────────────────────────────────────────
section "9. GitHub App credentials"
if [[ -z "$CFG_CLIENT_ID" ]]; then
    fail "[github].client_id is empty"
else
    if [[ "$CFG_CLIENT_ID" =~ ^Iv ]]; then
        pass "client_id looks well-formed (starts with 'Iv')"
    else
        warn "client_id '$CFG_CLIENT_ID' doesn't start with 'Iv' — that's the legacy App ID format, not Client ID"
    fi
fi

if [[ -z "$CFG_PRIVATE_KEY" ]]; then
    fail "[github].private_key_path is empty"
elif [[ ! -f "$CFG_PRIVATE_KEY" ]]; then
    fail "private key not found at $CFG_PRIVATE_KEY"
else
    mode=$(stat -c '%a' "$CFG_PRIVATE_KEY" 2>/dev/null)
    if [[ "$mode" == "600" ]]; then
        pass "private key permissions are 0600"
    else
        fail "private key mode is $mode, expected 0600  → chmod 0600 $CFG_PRIVATE_KEY"
    fi
    if head -1 "$CFG_PRIVATE_KEY" 2>/dev/null | grep -qE '^-----BEGIN (RSA )?PRIVATE KEY-----$'; then
        pass "private key looks like a PEM RSA key"
    else
        fail "private key doesn't start with the expected PEM header"
    fi
    if sudo -u "$CFG_SERVICE_USER" test -r "$CFG_PRIVATE_KEY" 2>/dev/null; then
        pass "service user '$CFG_SERVICE_USER' can read the private key"
    else
        fail "'$CFG_SERVICE_USER' cannot read the private key — fix ownership"
    fi
fi

# ─── 10. Network reachability ──────────────────────────────────────────
section "10. Network reachability (from host)"
for host in api.github.com github.com cloud-images.ubuntu.com archive.hiro.so; do
    if timeout 5 curl --fail --silent --head --max-time 5 "https://$host/" >/dev/null 2>&1; then
        pass "https://$host  reachable"
    else
        fail "https://$host  unreachable (curl --head failed)"
    fi
done

# ─── 11. Postgres + role split ─────────────────────────────────────────
section "11. Postgres: $CFG_DATABASE_URL"
if [[ -z "$CFG_DATABASE_URL" ]]; then
    fail "DATABASE_URL is unset (neither config nor env)"
elif ! command -v psql >/dev/null; then
    warn "psql not installed — skipping Postgres check (apt install postgresql-client)"
else
    # The orchestrator's DSN should use the narrow `sbgh_orch` role. If
    # it's still using the owner role `sbgh`, the Postgres half of the
    # boundary is wide open.
    db_user=""
    if [[ "$CFG_DATABASE_URL" =~ postgres://([^:@/]+)[:@] ]]; then
        db_user="${BASH_REMATCH[1]}"
    fi
    case "$db_user" in
        sbgh_orch)
            pass "orchestrator DSN uses the narrow 'sbgh_orch' role"
            ;;
        sbgh)
            fail "orchestrator DSN uses owner role 'sbgh' — role split bypassed"
            info "→ change [server].database_url to postgres://sbgh_orch:<SBGH_ORCH_DB_PASSWORD>@.../sbgh"
            ;;
        sbgh_handler)
            fail "orchestrator DSN uses 'sbgh_handler' role — that's the *handler*'s role, INSERT-only"
            ;;
        "")
            warn "couldn't parse user from DATABASE_URL"
            ;;
        *)
            warn "orchestrator DSN uses unexpected role '$db_user' (expected 'sbgh_orch')"
            ;;
    esac

    if PGCONNECT_TIMEOUT=5 psql "$CFG_DATABASE_URL" -tAc 'SELECT 1' >/dev/null 2>&1; then
        pass "can connect to Postgres as '$db_user'"

        if PGCONNECT_TIMEOUT=5 psql "$CFG_DATABASE_URL" -tAc \
                "SELECT to_regclass('public.jobs') IS NOT NULL" 2>/dev/null | grep -q '^t$'; then
            pass "'jobs' table exists"
            queued=$(PGCONNECT_TIMEOUT=5 psql "$CFG_DATABASE_URL" -tAc \
                "SELECT count(*) FROM jobs WHERE status='queued'" 2>/dev/null)
            running=$(PGCONNECT_TIMEOUT=5 psql "$CFG_DATABASE_URL" -tAc \
                "SELECT count(*) FROM jobs WHERE status='running'" 2>/dev/null)
            info "queued=${queued:-?}  running=${running:-?}"
        else
            warn "'jobs' table not present — sbgh-migrate hasn't run yet"
        fi

        # Verify the three roles exist and have the expected grants. Use
        # has_table_privilege so we don't need to query pg_authid (which
        # the orchestrator's role can't read).
        for role in sbgh sbgh_handler sbgh_orch; do
            if PGCONNECT_TIMEOUT=5 psql "$CFG_DATABASE_URL" -tAc \
                    "SELECT 1 FROM pg_roles WHERE rolname='$role'" 2>/dev/null | grep -q '^1$'; then
                pass "role '$role' exists"
            else
                fail "role '$role' missing — sbgh-migrate hasn't applied roles yet"
            fi
        done

        # Tripwire: sbgh_handler's grants on `jobs` are column-level — it
        # CAN read id + github_delivery_id (needed for INSERT ... ON
        # CONFLICT ... RETURNING), but MUST NOT be able to read or write
        # the sensitive columns (head_sha, args, requested_by, result,
        # status, …). Pick a representative column from each side to
        # check; if either side regresses the whole boundary is leaky.
        check_col_priv() {
            local col="$1" priv="$2" want="$3" label="$4"
            local got
            got=$(PGCONNECT_TIMEOUT=5 psql "$CFG_DATABASE_URL" -tAc \
                "SELECT has_column_privilege('sbgh_handler','public.jobs','$col','$priv')" \
                2>/dev/null | tr -d '[:space:]')
            case "$got" in
                "$want") pass "sbgh_handler $label" ;;
                f|t)     fail "sbgh_handler $label: got $got, expected $want" ;;
                *)       warn "couldn't probe sbgh_handler $priv on $col (got '$got')" ;;
            esac
        }
        # Must-have: needed for enqueue to actually work.
        check_col_priv id                 SELECT t "can SELECT id (for RETURNING)"
        check_col_priv github_delivery_id SELECT t "can SELECT github_delivery_id (for ON CONFLICT)"
        check_col_priv repository         INSERT t "can INSERT repository"
        check_col_priv github_delivery_id INSERT t "can INSERT github_delivery_id"
        # Must-NOT-have: leaks of orchestrator-owned state.
        check_col_priv head_sha    SELECT f "cannot SELECT head_sha (orchestrator-owned)"
        check_col_priv result      SELECT f "cannot SELECT result blobs"
        check_col_priv requested_by SELECT f "cannot SELECT requested_by"
        check_col_priv status      INSERT f "cannot INSERT status (no fabricated 'completed' rows)"
        check_col_priv result      INSERT f "cannot INSERT result"

        for priv in SELECT UPDATE; do
            orch_priv=$(PGCONNECT_TIMEOUT=5 psql "$CFG_DATABASE_URL" -tAc \
                "SELECT has_table_privilege('sbgh_orch','public.jobs','$priv')" 2>/dev/null \
                | tr -d '[:space:]')
            if [[ "$orch_priv" == "t" ]]; then
                pass "sbgh_orch has $priv on jobs"
            else
                fail "sbgh_orch missing $priv on jobs"
            fi
        done
    else
        fail "Postgres unreachable at $CFG_DATABASE_URL"
    fi
fi

# ─── 12. Docker stack (handler + smee + Postgres) ─────────────────────
section "12. Docker stack"
if command -v docker >/dev/null; then
    pass "docker CLI present"
    if docker info >/dev/null 2>&1; then
        pass "docker daemon reachable"
    else
        fail "docker daemon unreachable (started? user in docker group?)"
    fi

    compose_yml=""
    for candidate in \
            "$(dirname "$0")/../docker/docker-compose.yml" \
            "./docker/docker-compose.yml"; do
        if [[ -f "$candidate" ]]; then
            compose_yml=$(realpath "$candidate")
            break
        fi
    done

    if [[ -n "$compose_yml" ]]; then
        pass "compose file: $compose_yml"
        compose_dir=$(dirname "$compose_yml")

        # Small helper: pull `KEY=value` from compose_dir/.env (last wins).
        env_lookup() {
            grep -E "^$1=" "$compose_dir/.env" 2>/dev/null | tail -1 \
                | cut -d= -f2- | tr -d '"' | tr -d "'" | tr -d ' '
        }

        if [[ -f "$compose_dir/.env" ]]; then
            pass "$compose_dir/.env present"
            if grep -qE '^SMEE_CHANNEL=https?://smee\.io/' "$compose_dir/.env"; then
                pass "SMEE_CHANNEL set in .env"
            else
                warn "SMEE_CHANNEL not set (or placeholder) in $compose_dir/.env"
            fi
            for required in POSTGRES_OWNER_PASSWORD SBGH_HANDLER_DB_PASSWORD SBGH_ORCH_DB_PASSWORD; do
                val=$(env_lookup "$required")
                if [[ -n "$val" ]] && [[ "$val" != "REPLACE_ME" ]]; then
                    pass "$required set in .env"
                else
                    fail "$required missing or placeholder in $compose_dir/.env"
                fi
            done
        else
            warn "$compose_dir/.env not found  → cp $compose_dir/.env.example $compose_dir/.env"
        fi

        # Handler secrets.env — only file env_file references in compose.
        # Default /etc/sbgh/handler, honor SBGH_HANDLER_CONFIG_DIR override.
        handler_config_dir="/etc/sbgh/handler"
        if [[ -f "$compose_dir/.env" ]]; then
            override_dir=$(env_lookup SBGH_HANDLER_CONFIG_DIR)
            [[ -n "$override_dir" ]] && handler_config_dir="$override_dir"
        fi
        handler_secrets="$handler_config_dir/secrets.env"
        if [[ -f "$handler_secrets" ]]; then
            mode=$(stat -c '%a' "$handler_secrets" 2>/dev/null)
            owner=$(stat -c '%U:%G' "$handler_secrets" 2>/dev/null)
            if [[ "$mode" == "600" ]]; then
                pass "$handler_secrets (mode 0600, owner $owner)"
            else
                warn "$handler_secrets mode is $mode, expected 0600"
            fi
            if grep -qE '^SBGH_WEBHOOK_SECRET=' "$handler_secrets" 2>/dev/null; then
                pass "SBGH_WEBHOOK_SECRET set in $handler_secrets"
            else
                fail "SBGH_WEBHOOK_SECRET missing from $handler_secrets"
            fi
        else
            warn "secrets file not found at $handler_secrets (compose env_file will fail)"
        fi

        # Rootless Postgres bind-mount dir. The container runs as uid
        # ${POSTGRES_UID:-900} (we override the image's baked-in 999 to
        # dodge dnsmasq on most Ubuntu installs). The entrypoint skips
        # its usual chown when not root, so the host dir MUST already be
        # owned by that same uid or postgres fails to start.
        pg_uid_expected=900
        pg_gid_expected=900
        pg_data_dir="/var/lib/sbgh/postgres"
        if [[ -f "$compose_dir/.env" ]]; then
            override=$(env_lookup POSTGRES_UID)
            [[ -n "$override" ]] && pg_uid_expected="$override"
            override=$(env_lookup POSTGRES_GID)
            [[ -n "$override" ]] && pg_gid_expected="$override"
            override_pg_dir=$(env_lookup POSTGRES_DATA_DIR)
            [[ -n "$override_pg_dir" ]] && pg_data_dir="$override_pg_dir"
        fi
        if [[ -d "$pg_data_dir" ]]; then
            pg_uid=$(stat -c '%u' "$pg_data_dir" 2>/dev/null)
            pg_gid=$(stat -c '%g' "$pg_data_dir" 2>/dev/null)
            if [[ "$pg_uid" == "$pg_uid_expected" ]] && [[ "$pg_gid" == "$pg_gid_expected" ]]; then
                pass "$pg_data_dir owned by uid $pg_uid (matches POSTGRES_UID)"
            else
                fail "$pg_data_dir owned by $pg_uid:$pg_gid, expected $pg_uid_expected:$pg_gid_expected  → sudo chown -R $pg_uid_expected:$pg_gid_expected $pg_data_dir"
            fi
        else
            fail "$pg_data_dir does not exist  → sudo install -d -m 0700 -o $pg_uid_expected -g $pg_gid_expected $pg_data_dir"
        fi

        # Are the long-running containers up? Migrate is one-shot so we
        # only check it exited successfully if present.
        for svc in sbgh-postgres sbgh-handler sbgh-smee; do
            if docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "$svc"; then
                pass "$svc container is running"
            else
                warn "$svc container is NOT running (docker compose up -d ?)"
            fi
        done
        if docker ps -a --format '{{.Names}}\t{{.Status}}' 2>/dev/null | grep -q '^sbgh-migrate\b'; then
            migrate_status=$(docker ps -a --format '{{.Names}}\t{{.Status}}' | grep '^sbgh-migrate\b' | cut -f2)
            if [[ "$migrate_status" =~ ^Exited\ \(0\) ]]; then
                pass "sbgh-migrate completed successfully ($migrate_status)"
            else
                warn "sbgh-migrate status: $migrate_status (expected 'Exited (0) ...')"
            fi
        fi

        # Verify handler ≠ smee uid at runtime (defense-in-depth — they
        # share an image but compose pins each to a distinct numeric uid).
        if docker ps --format '{{.Names}}' 2>/dev/null | grep -qx sbgh-handler \
                && docker ps --format '{{.Names}}' 2>/dev/null | grep -qx sbgh-smee; then
            handler_runtime_uid=$(docker exec sbgh-handler id -u 2>/dev/null)
            smee_runtime_uid=$(docker exec sbgh-smee id -u 2>/dev/null)
            if [[ -n "$handler_runtime_uid" ]] && [[ -n "$smee_runtime_uid" ]]; then
                if [[ "$handler_runtime_uid" != "$smee_runtime_uid" ]]; then
                    pass "handler ($handler_runtime_uid) and smee ($smee_runtime_uid) run as distinct uids"
                else
                    fail "handler and smee both run as uid $handler_runtime_uid — defense-in-depth lost"
                fi
            fi
        fi
    else
        warn "docker-compose.yml not found near this script"
    fi
else
    warn "docker not installed  → apt install docker.io docker-compose-v2"
fi

# ─── Summary ───────────────────────────────────────────────────────────
echo
echo "${BLD}── Summary ──${RST}"
echo "  ${GRN}passed:${RST}  $PASS_COUNT"
echo "  ${YLW}warnings:${RST} $WARN_COUNT"
echo "  ${RED}failed:${RST}  $FAIL_COUNT"
echo
if (( FAIL_COUNT == 0 )); then
    echo "${GRN}${BLD}Host looks ready for the orchestrator.${RST}"
    exit 0
else
    echo "${RED}${BLD}Host is NOT ready — fix the failures above before running the orchestrator.${RST}"
    exit 1
fi
