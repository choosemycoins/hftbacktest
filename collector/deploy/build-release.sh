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
#   etc/hft-collector@.service
#   etc/instance.env.example
#   README.md
#   RELEASE            (manifest read by install.sh)

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
mkdir -p "${BUILD_DIR}/bin" "${BUILD_DIR}/etc"
install -m 755 "${BIN}"                                   "${BUILD_DIR}/bin/collector"
install -m 755 "${DEPLOY_DIR}/collector-run.sh"           "${BUILD_DIR}/bin/collector-run.sh"
install -m 644 "${DEPLOY_DIR}/hft-collector@.service"     "${BUILD_DIR}/etc/hft-collector@.service"
install -m 644 "${DEPLOY_DIR}/instance.env.example"       "${BUILD_DIR}/etc/instance.env.example"
install -m 644 "${REPO_ROOT}/collector/README.md"         "${BUILD_DIR}/README.md"

# Ship rollback.sh inside the release so a rollback is possible from a host
# that has nothing but /opt — the operator should never need the source repo
# to recover. install.sh is deliberately NOT shipped: installing a release
# requires the tarball, which arrives with its own copy.
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
