#!/usr/bin/env bash
# Install a collector release tarball as the new active version.
#
# Usage:
#   sudo ./install.sh <release_tarball.tar.gz> [-y]
#
#   -y    don't prompt for confirmation (for automation)
#
# Layout under /opt/hft-collector:
#   current -> releases/<tag>/     symlink, swapped atomically
#   releases/<tag>/
#     bin/{collector,collector-run.sh,rollback.sh,gate-run.sh,alert.sh}
#     bin/{heartbeat.sh,day0_poller.py,funding_poller.py} and the units that
#     execute them — all optional, all installed but never enabled here
#     etc/{hft-collector@.service,hft-collector-gate@.service,
#          hft-collector-gate@.timer,hft-collector-alert@.service,
#          instance.env.example,alert.env.example}
#     tools/quality_report.py      the gate's only dependency; stdlib-only
#     README.md
#     RELEASE
#   etc/<instance>.env             operator-authored, never touched here
#   etc/alert.env                  operator-authored, root:600, see alert.sh
#   data/                          recorded .gz, never touched here
#   .previous                      rollback target
#
# What this does:
#   1. Extracts and validates the tarball (manifest + binary provenance).
#   2. Installs it into a new releases/<tag>/.
#   3. Refreshes the systemd template unit if it changed.
#   4. Atomically swaps `current`, then immediately records `.previous`.
#   5. Restarts each configured instance one at a time, verifying each.
#
# What this does NOT do:
#   - Never edits /opt/hft-collector/etc/*.env or touches data/.
#   - Does not roll back on failure. It stops at the first bad instance and
#     tells you to run rollback.sh, so a partially-restarted fleet is visible
#     rather than silently reverted.
#
# Restarting a collector drops its WebSocket and leaves a gap in the
# recording, typically a second or two per instance. There is no zero-gap
# deploy here: two processes writing the same .gz would interleave gzip
# members from different streams.

set -euo pipefail

INSTALL_ROOT=/opt/hft-collector
RELEASES_DIR="${INSTALL_ROOT}/releases"
ETC_DIR="${INSTALL_ROOT}/etc"
DATA_DIR="${INSTALL_ROOT}/data"
UNIT_NAME='hft-collector@.service'
UNIT_FILE="/etc/systemd/system/${UNIT_NAME}"
USER_NAME=hftcollector
VERIFY_SECONDS=10

ASSUME_YES=0
TARBALL=""
while [[ "$#" -gt 0 ]]; do
    case "$1" in
        -y|--yes) ASSUME_YES=1; shift ;;
        -*) echo "Unknown option: $1" >&2; exit 1 ;;
        *)
            # Refuse a second positional rather than silently installing the
            # last one — `install.sh /tmp/hft-collector-release-*.tar.gz` with
            # several matches must not pick one at random.
            if [[ -n "${TARBALL}" ]]; then
                echo "ERROR: more than one tarball given ('${TARBALL}' and '$1')." >&2
                exit 1
            fi
            TARBALL="$1"; shift ;;
    esac
done

# Ask the operator to confirm, failing closed when there is nobody to ask.
#
# `[[ -e /dev/tty ]]` is NOT a test for interactivity: /dev/tty is a device
# node that exists unconditionally, so the guard is always true. Under
# `ssh host 'sudo install.sh ...'` — the very command cross-build-linux.sh
# prints — there is no controlling terminal, the read fails, and the previous
# version of this function exited 0 having installed nothing. A deploy that
# did not happen must never report success.
confirm() {
    [[ "${ASSUME_YES}" -eq 1 ]] && return 0
    if ! exec 3< /dev/tty 2>/dev/null; then
        echo "" >&2
        echo "ERROR: no terminal available to confirm, and -y was not given." >&2
        echo "       Re-run with -y for non-interactive use." >&2
        exit 5
    fi
    printf "\nProceed? [y/N] "
    local reply=""
    read -r reply <&3 || reply=""
    exec 3<&-
    case "${reply}" in
        y|Y|yes|YES) return 0 ;;
        *) echo "Aborted." >&2; exit 5 ;;
    esac
}

if [[ -z "${TARBALL}" ]] || [[ ! -f "${TARBALL}" ]]; then
    echo "Usage: $0 <release_tarball.tar.gz> [-y]" >&2
    exit 1
fi
if [[ "$(id -u)" -ne 0 ]]; then
    echo "ERROR: must run as root (sudo)" >&2
    exit 1
fi
if ! command -v systemctl >/dev/null 2>&1; then
    echo "ERROR: systemctl not found. These scripts target a systemd Linux host." >&2
    exit 1
fi
if ! id "${USER_NAME}" >/dev/null 2>&1 || [[ ! -d "${RELEASES_DIR}" ]]; then
    echo "ERROR: host not bootstrapped (missing user '${USER_NAME}' or ${RELEASES_DIR})." >&2
    echo "Run once:  sudo ./bootstrap.sh" >&2
    exit 1
fi

# ---------------------------------------------------------------- extract

# Staged under releases/, not in /tmp. Two reasons: many hardened hosts mount
# /tmp `noexec`, which would make the version probe below fail with a message
# blaming the architecture; and staging on the install filesystem means the
# final publish is a rename within one filesystem, which is atomic.
TMP="${RELEASES_DIR}/.staging.$$"
rm -rf "${TMP}"
mkdir -p "${TMP}"
trap 'rm -rf "${TMP}"' EXIT

echo "==> Extracting ${TARBALL}"
tar -xzf "${TARBALL}" -C "${TMP}"

if [[ ! -f "${TMP}/RELEASE" ]]; then
    echo "ERROR: tarball has no RELEASE manifest — not a collector release?" >&2
    exit 2
fi

manifest() { grep "^$1=" "${TMP}/RELEASE" | head -n1 | cut -d= -f2- || true; }

TAG="$(manifest tag)"
COMPONENT="$(manifest component)"
if [[ -z "${TAG}" ]]; then
    echo "ERROR: no tag in RELEASE manifest" >&2
    exit 2
fi
if [[ -n "${COMPONENT}" ]] && [[ "${COMPONENT}" != "hft-collector" ]]; then
    echo "ERROR: this tarball is for component '${COMPONENT}', not hft-collector." >&2
    exit 2
fi
# Guard the tag before it is ever used to build a path: it comes from a file
# inside the tarball, and `releases/../../etc` would be an unpleasant surprise.
if [[ ! "${TAG}" =~ ^[A-Za-z0-9._-]+$ ]]; then
    echo "ERROR: refusing unsafe release tag: '${TAG}'" >&2
    exit 2
fi

NEW_RELEASE="${RELEASES_DIR}/${TAG}"
if [[ -e "${NEW_RELEASE}" ]]; then
    echo "ERROR: ${NEW_RELEASE} already exists." >&2
    LIVE=""
    [[ -L "${INSTALL_ROOT}/current" ]] && LIVE="$(readlink "${INSTALL_ROOT}/current")"
    if [[ "${LIVE}" == "${NEW_RELEASE}" ]]; then
        echo "       That directory is the CURRENTLY LIVE release. Do not delete it." >&2
        echo "       Rebuild with a different tag." >&2
    else
        echo "       Rebuild with a different tag, or — after confirming with" >&2
        echo "       'rollback.sh --list' that it is neither current nor .previous —" >&2
        echo "       remove it." >&2
    fi
    exit 3
fi
for f in bin/collector bin/collector-run.sh bin/rollback.sh etc/hft-collector@.service; do
    if [[ ! -f "${TMP}/${f}" ]]; then
        echo "ERROR: tarball is missing ${f}" >&2
        exit 2
    fi
done
# The daily gate is optional in the tarball rather than required, so this
# script still installs a release built before it existed. A release that has
# it gets the units refreshed below; one that does not is left alone, and the
# timer an operator already enabled keeps running the previous release's copy
# through `current/`.
GATE_FILES=(bin/gate-run.sh tools/quality_report.py
            etc/hft-collector-gate@.service etc/hft-collector-gate@.timer)
HAS_GATE=1
for f in "${GATE_FILES[@]}"; do
    [[ -f "${TMP}/${f}" ]] || HAS_GATE=0
done
if [[ "${HAS_GATE}" -eq 0 ]]; then
    echo "NOTE: this tarball carries no daily quality gate (bin/gate-run.sh +"
    echo "      tools/quality_report.py). Nothing is removed; an enabled timer"
    echo "      keeps running whatever the previous release installed."
fi
# The dead man and the two pollers, on the same optional terms as the gate: a
# tarball built before 2026-08-22 does not carry them, and such a release must
# still install — its units keep executing whatever the previous release left in
# `current/bin`. Each component is checked on its own so the message names the
# one that is missing rather than the group.
#
# Format: <name>|<file> <file> ... — one line each. No associative arrays, to
# keep this script readable on any bash a host happens to have.
TIMED_COMPONENTS="
heartbeat|bin/heartbeat.sh etc/hft-heartbeat.service etc/hft-heartbeat.timer
day0-poller|bin/day0_poller.py etc/hft-day0-poller.service etc/hft-day0-poller.timer
funding-poller|bin/funding_poller.py etc/hft-funding-poller.service etc/hft-funding-poller.timer
"
PRESENT_COMPONENTS=""
while IFS='|' read -r name files; do
    [[ -z "${name}" ]] && continue
    have=1
    for f in ${files}; do
        [[ -f "${TMP}/${f}" ]] || have=0
    done
    if [[ "${have}" -eq 1 ]]; then
        PRESENT_COMPONENTS="${PRESENT_COMPONENTS}${name}|${files}
"
    else
        echo "NOTE: this tarball carries no ${name}. Nothing is removed; an"
        echo "      enabled timer keeps running whatever the previous release"
        echo "      installed under current/bin."
    fi
done <<<"${TIMED_COMPONENTS}"

# The alert hook, on the same optional terms. Loud rather than silent when it
# is absent: every unit shipped here carries OnFailure=hft-collector-alert@%n
# unconditionally, so a release without it leaves those units naming a hook
# systemd cannot resolve — and the symptom is that nothing alerts, which is
# indistinguishable from nothing failing.
ALERT_FILES=(bin/alert.sh etc/hft-collector-alert@.service etc/alert.env.example)
HAS_ALERT=1
for f in "${ALERT_FILES[@]}"; do
    [[ -f "${TMP}/${f}" ]] || HAS_ALERT=0
done
if [[ "${HAS_ALERT}" -eq 0 ]]; then
    echo "WARNING: this tarball carries no alert hook (bin/alert.sh +"
    echo "         etc/hft-collector-alert@.service). The units' OnFailure= will"
    echo "         not resolve unless a previous release installed it, and a"
    echo "         failed collector will then be visible only in systemctl."
fi

# Cross-check the binary against the manifest. A tarball whose bin/ was staged
# from a stale target/ dir is the classic way to deploy code you did not build.
chmod 755 "${TMP}/bin/collector"
ACTUAL_VERSION="$("${TMP}/bin/collector" --version 2>/dev/null || echo "<cannot execute>")"
EXPECTED_VERSION="$(manifest binary_version)"
echo "==> Binary reports: ${ACTUAL_VERSION}"
if [[ "${ACTUAL_VERSION}" == "<cannot execute>" ]]; then
    echo "ERROR: bin/collector will not execute on this host." >&2
    echo "  Staged at ${TMP}/bin/collector" >&2
    echo "  Usual causes: wrong architecture or libc, a missing dynamic loader," >&2
    echo "  or ${RELEASES_DIR} mounted noexec. Diagnose with:" >&2
    echo "    file ${TMP}/bin/collector && ${TMP}/bin/collector --version" >&2
    exit 2
fi
if [[ -n "${EXPECTED_VERSION}" ]] && [[ "${EXPECTED_VERSION}" != "unknown" ]]; then
    if [[ "${EXPECTED_VERSION}" != "${ACTUAL_VERSION}" ]]; then
        echo "ERROR: manifest/binary mismatch." >&2
        echo "  manifest binary_version = ${EXPECTED_VERSION}" >&2
        echo "  actual  --version       = ${ACTUAL_VERSION}" >&2
        exit 2
    fi
else
    # A cross-built tarball cannot record `binary_version`: the build host
    # cannot execute a Linux binary to ask. The provenance is still checkable,
    # because build.rs bakes commit, branch and dirty state into `--version`
    # and the manifest records the same three independently. Comparing them
    # here restores a real stale-binary check for cross-built releases instead
    # of degrading to "the binary runs".
    for field in collector_commit collector_branch collector_dirty; do
        want="$(manifest "${field}")"
        [[ -z "${want}" || "${want}" == "unknown" ]] && continue
        if [[ "${ACTUAL_VERSION}" != *"${want}"* ]]; then
            echo "ERROR: manifest/binary provenance mismatch on ${field}." >&2
            echo "  manifest ${field} = ${want}" >&2
            echo "  actual  --version = ${ACTUAL_VERSION}" >&2
            echo "  The tarball's binary was not built from the commit the manifest claims." >&2
            exit 2
        fi
    done
fi
if [[ "${ACTUAL_VERSION}" == *dirty* ]]; then
    echo "WARNING: this binary was built from a dirty tree — not reproducible from a commit."
fi

# ------------------------------------------------- what will be restarted

# In scope for a restart = an instance that is **currently running**.
#
# Not "enabled or active": `systemctl restart` starts a stopped unit, so an
# enabled-but-deliberately-stopped instance would be silently brought up by a
# routine deploy — the opposite of what an operator who ran `systemctl stop`
# asked for. Enabled-but-inactive instances pick up the new release the next
# time they are started, which is the correct behaviour for a symlinked
# release layout.
INSTANCES=()
SKIPPED=()
shopt -s nullglob
for envfile in "${ETC_DIR}"/*.env; do
    name="$(basename "${envfile}" .env)"
    # systemd instance names are escaped tokens; a basename that is not a legal
    # one would silently never match a unit. Surface it instead of dropping it.
    if [[ ! "${name}" =~ ^[A-Za-z0-9:_.\\-]+$ ]]; then
        echo "WARNING: ignoring ${envfile} — '${name}' is not a valid systemd instance name." >&2
        continue
    fi
    active="$(systemctl is-active "hft-collector@${name}.service" 2>/dev/null || true)"
    enabled="$(systemctl is-enabled "hft-collector@${name}.service" 2>/dev/null || true)"
    if [[ "${active}" == "active" ]]; then
        INSTANCES+=("${name}")
    elif [[ "${enabled}" == enabled* ]]; then
        SKIPPED+=("${name}")
    fi
done
shopt -u nullglob

echo ""
echo "==> Release ${TAG}"
sed 's/^/    /' "${TMP}/RELEASE"
echo ""
if [[ "${#INSTANCES[@]}" -eq 0 ]]; then
    echo "==> No running instances. Nothing will be restarted."
else
    echo "==> Will restart ${#INSTANCES[@]} running instance(s), one at a time:"
    for i in "${INSTANCES[@]}"; do
        echo "      hft-collector@${i}   (recording gap of ~1-2s)"
    done
fi
if [[ "${#SKIPPED[@]}" -gt 0 ]]; then
    echo "==> Enabled but not running, left stopped: ${SKIPPED[*]}"
fi

confirm

# ---------------------------------------------------------------- install

echo ""
echo "==> Installing ${NEW_RELEASE}"
install -d -m 755 "${RELEASES_DIR}" "${ETC_DIR}"
# Create the data dir only if absent. Re-applying ownership and mode on every
# deploy would contradict this script's own "never touches data/" invariant and
# would quietly undo an operator's deliberate permission change.
[[ -d "${DATA_DIR}" ]] || install -d -o "${USER_NAME}" -g "${USER_NAME}" -m 755 "${DATA_DIR}"

# The tarball was already extracted into a staging directory under releases/,
# so publishing is a rename within one filesystem — atomic. `cp -a` straight
# into releases/<tag> would not be: a kill or ENOSPC midway leaves a partial
# directory that looks complete to rollback.sh, and the "already exists" check
# above then blocks every retry of the same tarball.
chown -R root:root "${TMP}"
chmod -R u+rwX,go+rX,go-w "${TMP}"
chmod 755 "${TMP}/bin/"*
sync -f "${TMP}" 2>/dev/null || true
mv -T "${TMP}" "${NEW_RELEASE}"
# Published: stop the EXIT trap from deleting what is now the live release.
trap - EXIT

# Keep a copy of the templates where an operator will look for them. Only the
# `.example` files: the real <instance>.env and alert.env are operator-authored
# and this script never touches them.
if [[ -f "${NEW_RELEASE}/etc/instance.env.example" ]]; then
    install -m 644 "${NEW_RELEASE}/etc/instance.env.example" "${ETC_DIR}/instance.env.example"
fi
if [[ -f "${NEW_RELEASE}/etc/binancefuturesum-day0.env.example" ]]; then
    install -m 644 "${NEW_RELEASE}/etc/binancefuturesum-day0.env.example" \
                   "${ETC_DIR}/binancefuturesum-day0.env.example"
fi
if [[ -f "${NEW_RELEASE}/etc/alert.env.example" ]]; then
    install -m 644 "${NEW_RELEASE}/etc/alert.env.example" "${ETC_DIR}/alert.env.example"
fi

if ! cmp -s "${NEW_RELEASE}/etc/hft-collector@.service" "${UNIT_FILE}" 2>/dev/null; then
    echo "==> Updating ${UNIT_FILE}"
    install -m 644 "${NEW_RELEASE}/etc/hft-collector@.service" "${UNIT_FILE}"
fi

# The gate's units, on the same terms. They are installed but never enabled
# here: install.sh does not start things nobody asked for, exactly as it
# refuses to start an enabled-but-stopped collector instance. Enabling is one
# command and it is in the README.
if [[ "${HAS_GATE}" -eq 1 ]]; then
    for unit in hft-collector-gate@.service hft-collector-gate@.timer; do
        if ! cmp -s "${NEW_RELEASE}/etc/${unit}" "/etc/systemd/system/${unit}" 2>/dev/null; then
            echo "==> Updating /etc/systemd/system/${unit}"
            install -m 644 "${NEW_RELEASE}/etc/${unit}" "/etc/systemd/system/${unit}"
        fi
    done
fi

# The dead man's and the pollers' units, on the gate's terms exactly: installed,
# never enabled here. Enabling is the operator's, and the DEPLOY notes beside
# this script say when.
#
# Refreshing them matters more than it looks: their ExecStart points INSIDE the
# swapped tree, so a unit left describing the previous release's flags while the
# tree underneath it changed is a poller that runs with the wrong arguments and
# says nothing about it.
while IFS='|' read -r name files; do
    [[ -z "${name}" ]] && continue
    for f in ${files}; do
        case "${f}" in
            etc/*)
                unit="${f#etc/}"
                if ! cmp -s "${NEW_RELEASE}/${f}" "/etc/systemd/system/${unit}" 2>/dev/null; then
                    echo "==> Updating /etc/systemd/system/${unit}"
                    install -m 644 "${NEW_RELEASE}/${f}" "/etc/systemd/system/${unit}"
                fi
                ;;
        esac
    done
done <<<"${PRESENT_COMPONENTS}"

# The alert unit, likewise. This one is not "enabled" at all — it is an
# OnFailure target, activated by the units that name it — so installing the
# file is the whole of wiring it up.
if [[ "${HAS_ALERT}" -eq 1 ]]; then
    unit=hft-collector-alert@.service
    if ! cmp -s "${NEW_RELEASE}/etc/${unit}" "/etc/systemd/system/${unit}" 2>/dev/null; then
        echo "==> Updating /etc/systemd/system/${unit}"
        install -m 644 "${NEW_RELEASE}/etc/${unit}" "/etc/systemd/system/${unit}"
    fi
fi

PREV_TARGET=""
if [[ -L "${INSTALL_ROOT}/current" ]]; then
    PREV_TARGET="$(readlink "${INSTALL_ROOT}/current")"
fi

# Record the rollback target BEFORE the flip, and write-then-rename so the file
# is never observed truncated. Writing it after the flip — as the myhft
# install.sh does, only after a successful restart — leaves a window where
# `current` points at the new release and nothing records where to go back to,
# exactly when rollback.sh is needed most.
if [[ -n "${PREV_TARGET}" ]]; then
    printf '%s\n' "${PREV_TARGET}" > "${INSTALL_ROOT}/.previous.new"
    chmod 600 "${INSTALL_ROOT}/.previous.new"
    mv -f "${INSTALL_ROOT}/.previous.new" "${INSTALL_ROOT}/.previous"
fi

echo "==> Swapping ${INSTALL_ROOT}/current -> ${NEW_RELEASE}"
ln -snf "${NEW_RELEASE}" "${INSTALL_ROOT}/current.new"
mv -Tf "${INSTALL_ROOT}/current.new" "${INSTALL_ROOT}/current"

# Reload unconditionally. Gating on `cmp` of the on-disk file makes the script
# non-idempotent under interruption: a run that installed the unit but died
# before reloading leaves systemd's in-memory state stale, and the retry then
# sees identical files and skips the reload forever. daemon-reload is cheap and
# has no effect on running units.
systemctl daemon-reload

# ---------------------------------------------------------------- restart

failed=""
for name in "${INSTANCES[@]}"; do
    unit="hft-collector@${name}.service"
    echo ""
    echo "==> Restarting ${unit}"
    systemctl reset-failed "${unit}" 2>/dev/null || true
    if ! systemctl restart "${unit}"; then
        failed="${name}"
        break
    fi
    ok=1
    for _ in $(seq "${VERIFY_SECONDS}"); do
        sleep 1
        if ! systemctl is-active --quiet "${unit}"; then
            ok=0
            break
        fi
    done
    if [[ "${ok}" -ne 1 ]]; then
        failed="${name}"
        break
    fi
    echo "    ${unit}: active after ${VERIFY_SECONDS}s"
done

if [[ -n "${failed}" ]]; then
    echo "" >&2
    echo "ERROR: hft-collector@${failed} did not stay active." >&2
    journalctl -u "hft-collector@${failed}.service" --since "2 min ago" -n 40 --no-pager >&2 || true
    echo "" >&2
    echo "Instances already restarted onto ${TAG} are still running it." >&2
    echo "To revert everything:  sudo ${INSTALL_ROOT}/current/bin/rollback.sh" >&2
    exit 4
fi

echo ""
echo "==> Installed ${TAG}"
[[ -n "${PREV_TARGET}" ]] && echo "    previous: ${PREV_TARGET}"
for name in "${INSTANCES[@]}"; do
    printf '    hft-collector@%-16s %s\n' "${name}" "$(systemctl is-active "hft-collector@${name}.service")"
done
shopt -s nullglob
CONFIGURED=("${ETC_DIR}"/*.env)
shopt -u nullglob
if [[ "${#SKIPPED[@]}" -gt 0 ]]; then
    echo ""
    echo "Left stopped (enabled but not running): ${SKIPPED[*]}"
    echo "They will pick up ${TAG} the next time they start."
fi
# Gated on whether any instance is *configured*, not on whether any is
# running — otherwise a deploy to a host whose instances are all stopped
# tells the operator to create the instance files that already exist.
if [[ "${#CONFIGURED[@]}" -eq 0 ]]; then
    echo ""
    echo "No instances configured yet. To add one:"
    echo "  sudo cp ${ETC_DIR}/instance.env.example ${ETC_DIR}/hyperliquid.env"
    echo "  sudo \$EDITOR ${ETC_DIR}/hyperliquid.env"
    echo "  sudo systemctl enable --now hft-collector@hyperliquid"
fi
if [[ "${HAS_GATE}" -eq 1 ]]; then
    GATE_ENABLED="$(systemctl is-enabled 'hft-collector-gate@all.timer' 2>/dev/null || true)"
    if [[ "${GATE_ENABLED}" != enabled* ]]; then
        echo ""
        echo "The daily quality gate is installed but not enabled. It checks"
        echo "yesterday's recordings at 00:35 UTC and fails the unit on a red day:"
        echo "  sudo systemctl enable --now hft-collector-gate@all.timer"
    fi
fi
echo ""
echo "Logs:  journalctl -u 'hft-collector@*' -f"
echo "Disk:  $(df -h "${DATA_DIR}" --output=avail 2>/dev/null | tail -1 | tr -d ' ') available in ${DATA_DIR}"
