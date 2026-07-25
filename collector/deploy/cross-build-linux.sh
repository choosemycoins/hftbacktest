#!/usr/bin/env bash
# Cross-build the collector for Linux x86_64 from macOS (or any host) using
# cargo-zigbuild. Produces a tarball in the same format as build-release.sh;
# install.sh accepts either.
#
# Prerequisites (one-time):
#   brew install zig
#   cargo install cargo-zigbuild
#   rustup target add x86_64-unknown-linux-gnu
#
# Usage:
#   ./cross-build-linux.sh [<release_tag>]
#
# Why zig: it supplies a complete C toolchain for the cross target, which the
# collector needs for its TLS stack (tokio-tungstenite/native-tls and reqwest
# both pull in C dependencies), without a Docker round trip. The resulting
# ELF targets a very low glibc ABI, so it runs on essentially any modern
# Linux.

set -euo pipefail

DEPLOY_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${DEPLOY_DIR}/../.." && pwd)"
TARGET="${COLLECTOR_LINUX_TARGET:-x86_64-unknown-linux-gnu}"
TAG="${1:-$(date -u +%Y%m%d-%H%M%S)-linux}"
if [[ ! "${TAG}" =~ ^[A-Za-z0-9._-]+$ ]]; then
    echo "ERROR: tag must match [A-Za-z0-9._-]+ (install.sh rejects anything else): '${TAG}'" >&2
    exit 1
fi
BUILD_DIR="$(mktemp -d -t hft-collector-cross-XXXXXX)"
TARBALL="/tmp/hft-collector-release-${TAG}.tar.gz"

cleanup() { rm -rf "${BUILD_DIR}"; }
trap cleanup EXIT

need() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "ERROR: missing $1. One-time setup:" >&2
        echo "  brew install zig" >&2
        echo "  cargo install cargo-zigbuild" >&2
        echo "  rustup target add ${TARGET}" >&2
        exit 1
    fi
}
need zig
need cargo-zigbuild
if ! rustup target list --installed | grep -qx "${TARGET}"; then
    echo "ERROR: rustup target ${TARGET} not installed. Run:" >&2
    echo "  rustup target add ${TARGET}" >&2
    exit 1
fi

echo "==> Cross-build for ${TARGET}, tag=${TAG}"
echo "==> Repo root: ${REPO_ROOT}"

if ! git -C "${REPO_ROOT}" diff --quiet HEAD 2>/dev/null; then
    echo "WARNING: working tree has uncommitted tracked changes;"
    echo "         this release will be marked dirty and is not reproducible."
fi

echo ""
echo "==> Building collector"
( cd "${REPO_ROOT}" && cargo zigbuild --target "${TARGET}" --release -p collector )

BIN="${REPO_ROOT}/target/${TARGET}/release/collector"
if [[ ! -f "${BIN}" ]]; then
    echo "ERROR: expected binary not found at ${BIN}" >&2
    exit 2
fi

echo ""
echo "==> Staging release"
mkdir -p "${BUILD_DIR}/bin" "${BUILD_DIR}/etc"
install -m 755 "${BIN}"                                 "${BUILD_DIR}/bin/collector"
install -m 755 "${DEPLOY_DIR}/collector-run.sh"         "${BUILD_DIR}/bin/collector-run.sh"
install -m 755 "${DEPLOY_DIR}/rollback.sh"              "${BUILD_DIR}/bin/rollback.sh"
install -m 644 "${DEPLOY_DIR}/hft-collector@.service"   "${BUILD_DIR}/etc/hft-collector@.service"
install -m 644 "${DEPLOY_DIR}/instance.env.example"     "${BUILD_DIR}/etc/instance.env.example"
install -m 644 "${REPO_ROOT}/collector/README.md"       "${BUILD_DIR}/README.md"

# A cross-built binary cannot be executed here, so `binary_version` is left
# unknown on purpose. install.sh treats that as "cannot cross-check" rather
# than as a mismatch — see the verification step there.
cat > "${BUILD_DIR}/RELEASE" <<EOF
tag=${TAG}
component=hft-collector
built_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
built_on=$(uname -s) $(uname -m)
target=${TARGET}
build_method=cargo-zigbuild
zig_version=$(zig version)
binary_version=unknown
collector_commit=$(git -C "${REPO_ROOT}" rev-parse --short HEAD 2>/dev/null || echo unknown)
collector_dirty=$(git -C "${REPO_ROOT}" diff --quiet HEAD 2>/dev/null && echo clean || echo dirty)
collector_branch=$(git -C "${REPO_ROOT}" rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)
host=$(hostname)
EOF
echo ""
cat "${BUILD_DIR}/RELEASE"

echo ""
echo "==> Binary verification"
FILE_OUT="$(file "${BUILD_DIR}/bin/collector")"
echo "  collector: ${FILE_OUT#*: }"
case "${TARGET}" in
    x86_64-*) EXPECT_ARCH="x86-64" ;;
    aarch64-*) EXPECT_ARCH="ARM aarch64" ;;
    *) EXPECT_ARCH="" ;;
esac
if [[ "${FILE_OUT}" != *"ELF 64-bit"* ]]; then
    echo "ERROR: collector is not a 64-bit ELF — wrong toolchain?" >&2
    exit 3
fi
if [[ -n "${EXPECT_ARCH}" ]] && [[ "${FILE_OUT}" != *"${EXPECT_ARCH}"* ]]; then
    echo "ERROR: collector is not ${EXPECT_ARCH}" >&2
    exit 3
fi

echo ""
echo "==> Packaging ${TARBALL}"
( cd "${BUILD_DIR}" && tar -czf "${TARBALL}" . )

echo ""
echo "==> Done"
ls -la "${TARBALL}"
echo ""
echo "Next:"
echo "  scp ${TARBALL} <user>@<host>:/tmp/"
echo "  # first install on a fresh host also needs bootstrap.sh + install.sh:"
echo "  scp ${DEPLOY_DIR}/bootstrap.sh ${DEPLOY_DIR}/install.sh <user>@<host>:/tmp/"
echo "  ssh <host> 'sudo /tmp/bootstrap.sh && sudo /tmp/install.sh /tmp/$(basename "${TARBALL}")'"
