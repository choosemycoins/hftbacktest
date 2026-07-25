#!/usr/bin/env bash
# One-time per-host bootstrap for the market-data collector.
# Run as root once, before the first install.sh.
#
# Usage:
#   sudo ./bootstrap.sh
#
# What this does (idempotent — safe to re-run):
#   1. Creates the system user/group `hftcollector` if missing.
#   2. Scaffolds the /opt/hft-collector tree with the right ownership.
#
# What this does NOT do:
#   - Does not install a release. Run install.sh afterwards.
#   - Does not create instance env files; those are operator-authored, and
#     their names determine the systemd instance names.
#   - Does not start or enable anything.
#
# Why this is separate from install.sh: install.sh runs on every deploy, and
# user/group management has no business executing that often. Keeping it here
# holds the blast radius of a routine deploy down to files under /opt.

set -euo pipefail

if [[ "$(id -u)" -ne 0 ]]; then
    echo "ERROR: must run as root (sudo)" >&2
    exit 1
fi

INSTALL_ROOT=/opt/hft-collector
USER_NAME=hftcollector
GROUP_NAME=hftcollector

if ! getent group "${GROUP_NAME}" >/dev/null 2>&1; then
    echo "==> Creating group ${GROUP_NAME}"
    groupadd --system "${GROUP_NAME}"
fi
if ! id "${USER_NAME}" >/dev/null 2>&1; then
    echo "==> Creating user ${USER_NAME}"
    useradd --system --no-create-home --shell /usr/sbin/nologin \
        --gid "${GROUP_NAME}" "${USER_NAME}"
fi

echo "==> Scaffolding ${INSTALL_ROOT}"
install -d -o root           -g root            -m 755 "${INSTALL_ROOT}"
install -d -o root           -g root            -m 755 "${INSTALL_ROOT}/releases"
install -d -o root           -g root            -m 755 "${INSTALL_ROOT}/etc"

# Recorded data is the one thing here that is expensive to lose and grows
# without bound. It lives outside the versioned releases/ tree precisely so a
# deploy or a rollback never touches it.
# `install -d` sets ownership on an existing directory too, so if a separate
# data volume is already mounted here this chowns the volume's root and no
# manual step is needed. That is the reason to mount BEFORE bootstrapping.
install -d -o "${USER_NAME}" -g "${GROUP_NAME}" -m 755 "${INSTALL_ROOT}/data"

if mountpoint -q "${INSTALL_ROOT}/data" 2>/dev/null; then
    echo "==> ${INSTALL_ROOT}/data is a mount point — ownership applied to the volume"
    findmnt -no SOURCE,FSTYPE,SIZE "${INSTALL_ROOT}/data" 2>/dev/null | sed 's/^/    /'
else
    echo "==> NOTE: ${INSTALL_ROOT}/data is on the root filesystem."
    echo "    If a separate volume is intended, mount it there and re-run this"
    echo "    script — mounting afterwards hides the directory just created and"
    echo "    leaves the volume owned by root."
fi

echo ""
echo "==> Bootstrap complete."
echo ""
echo "Disk check — recorded data accumulates here and is never pruned:"
df -h "${INSTALL_ROOT}/data" 2>/dev/null || true
echo ""
echo "Next:"
echo "  1. sudo ./install.sh /tmp/hft-collector-release-<tag>.tar.gz"
echo "  2. sudo cp ${INSTALL_ROOT}/current/etc/instance.env.example ${INSTALL_ROOT}/etc/hyperliquid.env"
echo "  3. sudo \$EDITOR ${INSTALL_ROOT}/etc/hyperliquid.env"
echo "  4. sudo systemctl enable --now hft-collector@hyperliquid"
echo ""
echo "The instance name is just the env file's basename — 'hyperliquid' above"
echo "matches hyperliquid.env and nothing else. Use one instance per collection"
echo "job; a second venue, or the same venue with a different symbol set, is a"
echo "second env file and a second 'systemctl enable --now'."
