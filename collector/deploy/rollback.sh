#!/usr/bin/env bash
# Roll the collector back to a previous release.
#
# Usage:
#   sudo ./rollback.sh              # go to /opt/hft-collector/.previous
#   sudo ./rollback.sh <tag>        # go to /opt/hft-collector/releases/<tag>
#   sudo ./rollback.sh --list       # show available releases and exit
#   sudo ./rollback.sh ... -y       # skip the confirmation prompt
#
# A copy of this script ships inside every release at
# /opt/hft-collector/current/bin/rollback.sh, so recovery never depends on
# having the source repo on the host.
#
# Effect:
#   1. Flips /opt/hft-collector/current to the target release.
#   2. Reinstates that release's systemd unit if it differs.
#   3. Restarts each enabled instance one at a time, verifying each.
#   4. Records the release you just left as the new .previous, so a second
#      rollback returns you to where you started rather than oscillating
#      into an unknown state.
#
# Recorded data under /opt/hft-collector/data is never touched. Rolling back
# does interrupt recording for a second or two per instance.

set -euo pipefail

INSTALL_ROOT=/opt/hft-collector
RELEASES_DIR="${INSTALL_ROOT}/releases"
ETC_DIR="${INSTALL_ROOT}/etc"
UNIT_FILE='/etc/systemd/system/hft-collector@.service'
VERIFY_SECONDS=10

ASSUME_YES=0
TARGET_TAG=""
LIST_ONLY=0
while [[ "$#" -gt 0 ]]; do
    case "$1" in
        -y|--yes)  ASSUME_YES=1; shift ;;
        -l|--list) LIST_ONLY=1; shift ;;
        -*) echo "Unknown option: $1" >&2; exit 1 ;;
        *)  TARGET_TAG="$1"; shift ;;
    esac
done

list_releases() {
    # Every lookup here must tolerate failure: --list is deliberately available
    # before the root check, and `.previous` is mode 600. Under `set -e` a bare
    # `[[ -f x ]] && cat x` as the last command of the list aborts the whole
    # script when the `cat` fails, so the one subcommand a non-root operator can
    # use during an incident would print nothing at all.
    local cur=""
    if [[ -L "${INSTALL_ROOT}/current" ]]; then
        cur="$(readlink "${INSTALL_ROOT}/current" 2>/dev/null || true)"
    fi
    local prev=""
    if [[ -f "${INSTALL_ROOT}/.previous" ]]; then
        prev="$(cat "${INSTALL_ROOT}/.previous" 2>/dev/null || echo '<unreadable: needs root>')"
    fi
    echo "Releases in ${RELEASES_DIR}:"
    local found=0
    for d in "${RELEASES_DIR}"/*/; do
        [[ -d "${d}" ]] || continue
        found=1
        d="${d%/}"
        local mark=""
        [[ "${d}" == "${cur}" ]]  && mark="${mark}  <- current"
        [[ "${d}" == "${prev}" ]] && mark="${mark}  <- .previous"
        local built
        built="$(grep '^built_at=' "${d}/RELEASE" 2>/dev/null | cut -d= -f2- || true)"
        printf '  %-28s %s%s\n' "$(basename "${d}")" "${built:-?}" "${mark}"
    done
    [[ "${found}" -eq 1 ]] || echo "  (none)"
    return 0
}

if [[ "${LIST_ONLY}" -eq 1 ]]; then
    list_releases
    exit 0
fi

if [[ "$(id -u)" -ne 0 ]]; then
    echo "ERROR: must run as root (sudo)" >&2
    exit 1
fi
if ! command -v systemctl >/dev/null 2>&1; then
    echo "ERROR: systemctl not found. These scripts target a systemd Linux host." >&2
    exit 1
fi

if [[ -n "${TARGET_TAG}" ]]; then
    if [[ ! "${TARGET_TAG}" =~ ^[A-Za-z0-9._-]+$ ]]; then
        echo "ERROR: refusing unsafe tag: '${TARGET_TAG}'" >&2
        exit 1
    fi
    TARGET="${RELEASES_DIR}/${TARGET_TAG}"
else
    if [[ ! -f "${INSTALL_ROOT}/.previous" ]]; then
        echo "ERROR: no .previous record — pass a tag explicitly." >&2
        echo "" >&2
        list_releases >&2
        exit 1
    fi
    TARGET="$(cat "${INSTALL_ROOT}/.previous")"
fi

# Constrain the target to releases/ regardless of where it came from. The tag
# regex above only guards the CLI argument; `.previous` is a file on disk and
# must not be able to point `current` at an arbitrary path.
case "${TARGET}" in
    "${RELEASES_DIR}"/*) ;;
    *)
        echo "ERROR: rollback target '${TARGET}' is not under ${RELEASES_DIR}; refusing." >&2
        exit 2 ;;
esac

if [[ ! -d "${TARGET}" ]]; then
    echo "ERROR: rollback target does not exist: ${TARGET}" >&2
    echo "" >&2
    list_releases >&2
    exit 2
fi
if [[ ! -x "${TARGET}/bin/collector" ]]; then
    echo "ERROR: ${TARGET}/bin/collector missing or not executable — refusing to roll back to a broken release." >&2
    exit 2
fi

CURRENT=""
if [[ -L "${INSTALL_ROOT}/current" ]]; then
    CURRENT="$(readlink "${INSTALL_ROOT}/current")"
fi

# In scope = enabled OR active OR failed.
#
# Wider than install.sh's "active only", deliberately: rollback runs after
# something went wrong, and the units that most need recovering are exactly the
# ones that are now `failed` (is-active reports `failed`, is-enabled may report
# `disabled` for an instance that was started but never enabled). Excluding
# them would leave the broken instances behind on the bad release.
INSTANCES=()
shopt -s nullglob
for envfile in "${ETC_DIR}"/*.env; do
    name="$(basename "${envfile}" .env)"
    [[ "${name}" =~ ^[A-Za-z0-9:_.\\-]+$ ]] || continue
    enabled="$(systemctl is-enabled "hft-collector@${name}.service" 2>/dev/null || true)"
    active="$(systemctl is-active "hft-collector@${name}.service" 2>/dev/null || true)"
    if [[ "${enabled}" == enabled* ]] || [[ "${active}" == "active" ]] || [[ "${active}" == "failed" ]]; then
        INSTANCES+=("${name}")
    fi
done
shopt -u nullglob

# Only short-circuit when the symlink is already right AND nothing is left
# broken. A rollback that stopped partway through its restart loop leaves
# `current` on the target with some instances still failed; refusing to re-run
# in that state would strand the operator with no tool to finish the job.
if [[ "${CURRENT}" == "${TARGET}" ]]; then
    stranded=0
    for name in "${INSTANCES[@]}"; do
        systemctl is-active --quiet "hft-collector@${name}.service" || stranded=1
    done
    if [[ "${stranded}" -eq 0 ]]; then
        echo "Already on ${TARGET} and all in-scope instances are active — nothing to do."
        exit 0
    fi
    echo "==> Already on ${TARGET}, but some instances are not active."
    echo "    Re-running the restart pass to finish an interrupted rollback."
fi

echo "==> Rolling back"
echo "    from: ${CURRENT:-<none>}"
echo "    to:   ${TARGET}"
if [[ -f "${TARGET}/RELEASE" ]]; then
    echo ""
    sed 's/^/    /' "${TARGET}/RELEASE"
fi
echo ""
if [[ "${#INSTANCES[@]}" -eq 0 ]]; then
    echo "==> No enabled instances; only the symlink will change."
else
    echo "==> Will restart ${#INSTANCES[@]} instance(s): ${INSTANCES[*]}"
fi

if [[ "${ASSUME_YES}" -ne 1 ]]; then
    # See install.sh: `[[ -e /dev/tty ]]` is not an interactivity test, and an
    # abort must not exit 0 — a rollback that silently did nothing during an
    # incident is the worst possible false success.
    if ! exec 3< /dev/tty 2>/dev/null; then
        echo "" >&2
        echo "ERROR: no terminal available to confirm, and -y was not given." >&2
        echo "       Re-run with -y for non-interactive use." >&2
        exit 5
    fi
    printf "\nProceed? [y/N] "
    reply=""
    read -r reply <&3 || reply=""
    exec 3<&-
    case "${reply}" in
        y|Y|yes|YES) ;;
        *) echo "Aborted." >&2; exit 5 ;;
    esac
fi

# Reinstate the target release's unit before flipping, so the restart below
# uses the unit that shipped with the code being restored. Reload
# unconditionally: an interrupted install may have written a unit file without
# reloading, and a `cmp`-gated reload would then never correct systemd's
# in-memory state.
if [[ -f "${TARGET}/etc/hft-collector@.service" ]]; then
    if ! cmp -s "${TARGET}/etc/hft-collector@.service" "${UNIT_FILE}" 2>/dev/null; then
        echo "==> Reinstating ${UNIT_FILE} from ${TARGET}"
        install -m 644 "${TARGET}/etc/hft-collector@.service" "${UNIT_FILE}"
    fi
fi
systemctl daemon-reload

echo "==> Flipping ${INSTALL_ROOT}/current"
ln -snf "${TARGET}" "${INSTALL_ROOT}/current.new"
mv -Tf "${INSTALL_ROOT}/current.new" "${INSTALL_ROOT}/current"

# Point .previous at what we just left, so an immediate second rollback is a
# return trip rather than a walk further back through history.
if [[ -n "${CURRENT}" ]] && [[ "${CURRENT}" != "${TARGET}" ]]; then
    printf '%s\n' "${CURRENT}" > "${INSTALL_ROOT}/.previous.new"
    chmod 600 "${INSTALL_ROOT}/.previous.new"
    mv -f "${INSTALL_ROOT}/.previous.new" "${INSTALL_ROOT}/.previous"
fi

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
    echo "ERROR: hft-collector@${failed} did not stay active after rollback." >&2
    echo "The symlink is already on ${TARGET}; this is a problem with the target" >&2
    echo "release or with that instance's config, not with the flip." >&2
    journalctl -u "hft-collector@${failed}.service" --since "2 min ago" -n 40 --no-pager >&2 || true
    echo "" >&2
    echo "Available releases:" >&2
    list_releases >&2
    exit 4
fi

echo ""
echo "==> Rollback complete: $(basename "${TARGET}")"
for name in "${INSTANCES[@]}"; do
    printf '    hft-collector@%-16s %s\n' "${name}" "$(systemctl is-active "hft-collector@${name}.service")"
done
echo ""
echo "Logs:  journalctl -u 'hft-collector@*' -f"
