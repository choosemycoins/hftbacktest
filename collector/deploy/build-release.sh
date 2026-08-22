#!/usr/bin/env bash
# Build a collector release tarball ready for install.sh.
#
# Run this on the target host, or on a machine with a matching glibc and
# architecture. To build for Linux from macOS, use cross-build-linux.sh.
#
# Usage:
#   ./build-release.sh [<release_tag>]
#
# Output: /tmp/hft-collector-release-<tag>.tar.gz containing
#   bin/collector
#   bin/collector-run.sh
#   bin/gate-run.sh                 daily quality gate, run by the timer below
#   bin/alert.sh                    OnFailure hook; ExecStart of the alert unit
#   bin/rollback.sh                 so a rollback needs nothing but /opt
#   bin/heartbeat.sh                dead man over every instance and poller
#   bin/day0_poller.py              day-zero listing poller
#   bin/funding_poller.py           cross-venue funding poller
#   bin/params_poller.py            venue administered-parameter poller
#   bin/positions_poller.py         Hyperliquid operator-position poller
#   etc/hft-collector@.service
#   etc/hft-collector-gate@.service
#   etc/hft-collector-gate@.timer
#   etc/hft-collector-alert@.service
#   etc/instance.env.example
#   etc/alert.env.example
#   tools/quality_report.py         the gate's only dependency; stdlib-only
#   README.md
#   RELEASE                         (manifest read by install.sh)
#
# Keep this list in step with the `install -m` block below — it is the one place
# an operator reads to know what a release contains. install.sh requires
# bin/{collector,collector-run.sh,rollback.sh} and etc/hft-collector@.service,
# and treats the gate set and the alert set as optional (a tarball without
# either installs, and says so). cross-build-linux.sh stages exactly the same
# set.

set -euo pipefail

DEPLOY_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${DEPLOY_DIR}/../.." && pwd)"
TAG="${1:-$(date -u +%Y%m%d-%H%M%S)}"
# Same character class install.sh enforces on the manifest tag. Validating here
# means an illegal tag fails in one second at build time, rather than after the
# tarball has been copied to a host and rejected at install time.
if [[ ! "${TAG}" =~ ^[A-Za-z0-9._-]+$ ]]; then
    echo "ERROR: tag must match [A-Za-z0-9._-]+ (install.sh rejects anything else): '${TAG}'" >&2
    exit 1
fi
BUILD_DIR="$(mktemp -d -t hft-collector-release-XXXXXX)"
TARBALL="/tmp/hft-collector-release-${TAG}.tar.gz"

cleanup() { rm -rf "${BUILD_DIR}"; }
trap cleanup EXIT

echo "==> Build tag:  ${TAG}"
echo "==> Repo root:  ${REPO_ROOT}"
echo "==> Build dir:  ${BUILD_DIR}"

if [[ ! -f "${REPO_ROOT}/collector/Cargo.toml" ]]; then
    echo "ERROR: ${REPO_ROOT}/collector/Cargo.toml not found — is this the hftbacktest workspace?" >&2
    exit 1
fi

# A dirty tree produces a binary that reports itself as dirty and cannot be
# reproduced from a commit. That is allowed for a hotfix but should never be
# silent, because the tag is all the operator sees later.
if ! git -C "${REPO_ROOT}" diff --quiet HEAD 2>/dev/null; then
    echo "WARNING: working tree has uncommitted tracked changes;"
    echo "         this release will be marked dirty and is not reproducible."
fi

echo ""
echo "==> Building collector (release)"
( cd "${REPO_ROOT}" && cargo build --release -p collector )

BIN="${REPO_ROOT}/target/release/collector"
if [[ ! -x "${BIN}" ]]; then
    echo "ERROR: expected binary not found at ${BIN}" >&2
    exit 2
fi

echo ""
echo "==> Staging release"
mkdir -p "${BUILD_DIR}/bin" "${BUILD_DIR}/etc" "${BUILD_DIR}/tools"
install -m 755 "${BIN}"                                   "${BUILD_DIR}/bin/collector"
install -m 755 "${DEPLOY_DIR}/collector-run.sh"           "${BUILD_DIR}/bin/collector-run.sh"
install -m 755 "${DEPLOY_DIR}/gate-run.sh"                "${BUILD_DIR}/bin/gate-run.sh"
install -m 644 "${DEPLOY_DIR}/hft-collector@.service"     "${BUILD_DIR}/etc/hft-collector@.service"
install -m 644 "${DEPLOY_DIR}/hft-collector-gate@.service" "${BUILD_DIR}/etc/hft-collector-gate@.service"
install -m 644 "${DEPLOY_DIR}/hft-collector-gate@.timer"  "${BUILD_DIR}/etc/hft-collector-gate@.timer"
install -m 644 "${DEPLOY_DIR}/instance.env.example"       "${BUILD_DIR}/etc/instance.env.example"
install -m 644 "${REPO_ROOT}/collector/README.md"         "${BUILD_DIR}/README.md"

# The alert hook. Every unit here carries OnFailure=hft-collector-alert@%n
# unconditionally, and that unit's ExecStart is current/bin/alert.sh — so a
# release without these three ships units naming a hook that does not exist,
# and the failure is exactly the one nobody sees: nothing alerts, and nothing
# says nothing alerted. alert.env.example travels because the README's install
# step copies it out of current/etc/; the credentials themselves are
# operator-authored and never in git.
install -m 755 "${DEPLOY_DIR}/alert.sh"                   "${BUILD_DIR}/bin/alert.sh"
install -m 644 "${DEPLOY_DIR}/hft-collector-alert@.service" "${BUILD_DIR}/etc/hft-collector-alert@.service"
install -m 644 "${DEPLOY_DIR}/alert.env.example"          "${BUILD_DIR}/etc/alert.env.example"

# The offline quality report, which the daily gate timer runs on the host.
#
# Only this one tool travels. It is stdlib-only by design and runs under any
# python3 the distribution ships, which is what makes an on-host gate possible
# at all; `build_dataset.py` and `backtest_first.py` need numpy and belong on
# the machine that assembles datasets, not on the box whose job is to record.
# Shipping them would put a dependency on the recording host that nothing there
# can satisfy.
install -m 644 "${REPO_ROOT}/collector/tools/quality_report.py" "${BUILD_DIR}/tools/quality_report.py"

# Ship rollback.sh inside the release so a rollback is possible from a host
# that has nothing but /opt — the operator should never need the source repo
# to recover. install.sh is deliberately NOT shipped: installing a release
# requires the tarball, which arrives with its own copy.
# The dead man and the two pollers. Staged here for one reason: until 2026-08-22
# they were not, and all three had been copied into the live release directory
# BY HAND. `install.sh` builds a fresh release directory and flips `current` at
# it, so the next ordinary release would have removed all three at once —
# silently, because their units keep pointing at `current/bin/...` and simply
# start failing. The heartbeat is the worst of the three to lose that way: it is
# the thing that would have told us the others were gone.
#
# Their units are staged too. A unit whose ExecStart points inside the swapped
# tree has to travel with the tree, or a release can leave the file and the unit
# describing different things.
install -m 755 "${REPO_ROOT}/collector/tools/day0_poller.py"    "${BUILD_DIR}/bin/day0_poller.py"
install -m 755 "${REPO_ROOT}/collector/tools/funding_poller.py" "${BUILD_DIR}/bin/funding_poller.py"
install -m 755 "${REPO_ROOT}/collector/tools/params_poller.py"  "${BUILD_DIR}/bin/params_poller.py"
install -m 755 "${REPO_ROOT}/collector/tools/positions_poller.py" "${BUILD_DIR}/bin/positions_poller.py"
install -m 755 "${DEPLOY_DIR}/heartbeat.sh"                     "${BUILD_DIR}/bin/heartbeat.sh"
install -m 644 "${DEPLOY_DIR}/hft-heartbeat.service"            "${BUILD_DIR}/etc/hft-heartbeat.service"
install -m 644 "${DEPLOY_DIR}/hft-heartbeat.timer"              "${BUILD_DIR}/etc/hft-heartbeat.timer"
install -m 644 "${DEPLOY_DIR}/hft-day0-poller.service"          "${BUILD_DIR}/etc/hft-day0-poller.service"
install -m 644 "${DEPLOY_DIR}/hft-day0-poller.timer"            "${BUILD_DIR}/etc/hft-day0-poller.timer"
install -m 644 "${DEPLOY_DIR}/hft-funding-poller.service"       "${BUILD_DIR}/etc/hft-funding-poller.service"
install -m 644 "${DEPLOY_DIR}/hft-funding-poller.timer"         "${BUILD_DIR}/etc/hft-funding-poller.timer"
install -m 644 "${DEPLOY_DIR}/hft-params-poller.service"         "${BUILD_DIR}/etc/hft-params-poller.service"
install -m 644 "${DEPLOY_DIR}/hft-params-poller.timer"           "${BUILD_DIR}/etc/hft-params-poller.timer"
install -m 644 "${DEPLOY_DIR}/hft-positions-poller.service"      "${BUILD_DIR}/etc/hft-positions-poller.service"
install -m 644 "${DEPLOY_DIR}/hft-positions-poller.timer"        "${BUILD_DIR}/etc/hft-positions-poller.timer"
install -m 644 "${DEPLOY_DIR}/operators_addrs.json.example"      "${BUILD_DIR}/etc/operators_addrs.json.example"
install -m 644 "${DEPLOY_DIR}/binancefuturesum-day0.env.example" \
               "${BUILD_DIR}/etc/binancefuturesum-day0.env.example"

install -m 755 "${DEPLOY_DIR}/rollback.sh"                "${BUILD_DIR}/bin/rollback.sh"

# `--version` is the binary's own account of its provenance; recording it
# alongside the git values lets install.sh detect a tarball whose manifest and
# binary disagree (mismatched staging, stale target/ dir).
BIN_VERSION="$("${BIN}" --version 2>/dev/null || echo unknown)"

cat > "${BUILD_DIR}/RELEASE" <<EOF
tag=${TAG}
component=hft-collector
built_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
built_on=$(uname -s) $(uname -m)
target=native
build_method=cargo build --release -p collector
binary_version=${BIN_VERSION}
collector_commit=$(git -C "${REPO_ROOT}" rev-parse --short HEAD 2>/dev/null || echo unknown)
collector_dirty=$(git -C "${REPO_ROOT}" diff --quiet HEAD 2>/dev/null && echo clean || echo dirty)
collector_branch=$(git -C "${REPO_ROOT}" rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)
host=$(hostname)
EOF
echo ""
cat "${BUILD_DIR}/RELEASE"

echo ""
echo "==> Packaging ${TARBALL}"
( cd "${BUILD_DIR}" && tar -czf "${TARBALL}" . )

echo ""
echo "==> Done"
ls -la "${TARBALL}"
echo ""
echo "Next:"
echo "  sudo ${DEPLOY_DIR}/install.sh ${TARBALL}"
