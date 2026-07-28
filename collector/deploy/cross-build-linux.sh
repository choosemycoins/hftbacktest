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
mkdir -p "${BUILD_DIR}/bin" "${BUILD_DIR}/etc" "${BUILD_DIR}/tools"
install -m 755 "${BIN}"                                 "${BUILD_DIR}/bin/collector"
install -m 755 "${DEPLOY_DIR}/collector-run.sh"         "${BUILD_DIR}/bin/collector-run.sh"
install -m 755 "${DEPLOY_DIR}/gate-run.sh"              "${BUILD_DIR}/bin/gate-run.sh"
install -m 755 "${DEPLOY_DIR}/rollback.sh"              "${BUILD_DIR}/bin/rollback.sh"
install -m 644 "${DEPLOY_DIR}/hft-collector@.service"   "${BUILD_DIR}/etc/hft-collector@.service"
install -m 644 "${DEPLOY_DIR}/hft-collector-gate@.service" "${BUILD_DIR}/etc/hft-collector-gate@.service"
install -m 644 "${DEPLOY_DIR}/hft-collector-gate@.timer" "${BUILD_DIR}/etc/hft-collector-gate@.timer"
install -m 644 "${DEPLOY_DIR}/instance.env.example"     "${BUILD_DIR}/etc/instance.env.example"
install -m 644 "${REPO_ROOT}/collector/README.md"       "${BUILD_DIR}/README.md"

# The alert hook — see the note in build-release.sh. Every unit carries
# OnFailure=hft-collector-alert@%n unconditionally and that unit runs
# current/bin/alert.sh, so leaving these out ships units naming a hook that
# does not exist.
install -m 755 "${DEPLOY_DIR}/alert.sh"                 "${BUILD_DIR}/bin/alert.sh"
install -m 644 "${DEPLOY_DIR}/hft-collector-alert@.service" "${BUILD_DIR}/etc/hft-collector-alert@.service"
install -m 644 "${DEPLOY_DIR}/alert.env.example"        "${BUILD_DIR}/etc/alert.env.example"

# The offline quality report, run on the host by the daily gate timer. Only
# this one tool travels: it is stdlib-only and runs under any python3, while
# build_dataset.py and backtest_first.py need numpy and belong on the machine
# that assembles datasets rather than on the box that records them.
install -m 644 "${REPO_ROOT}/collector/tools/quality_report.py" "${BUILD_DIR}/tools/quality_report.py"

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

# Stage a complete upload set. The release tarball alone is not enough for a
# first install — bootstrap.sh and install.sh have to exist on the host before
# there is a release to run them from — and copying them as a separate step is
# easy to forget. One directory, one scp.
UPLOAD="/tmp/hft-collector-upload-${TAG}"
rm -rf "${UPLOAD}"
mkdir -p "${UPLOAD}"
cp "${TARBALL}" "${UPLOAD}/"
install -m 755 "${DEPLOY_DIR}/bootstrap.sh" "${UPLOAD}/bootstrap.sh"
install -m 755 "${DEPLOY_DIR}/install.sh"   "${UPLOAD}/install.sh"
install -m 644 "${DEPLOY_DIR}/instance.env.example" "${UPLOAD}/instance.env.example"

echo ""
echo "==> Done"
ls -la "${UPLOAD}"
echo ""
echo "Next:"
echo "  scp -r ${UPLOAD} <user>@<host>:/tmp/"
# `-y` is required, not optional: `ssh host 'cmd'` allocates no TTY, install.sh
# has no terminal to read a confirmation from, and it fails closed rather than
# installing unattended. sudo's `use_pty` does not help — it is a no-op when
# sudo itself has no terminal.
UP="/tmp/$(basename "${UPLOAD}")"
echo "  ssh <host> 'sudo ${UP}/bootstrap.sh && sudo ${UP}/install.sh ${UP}/$(basename "${TARBALL}") -y'"
echo ""
echo "Then, per collection job:"
echo "  ssh <host> 'sudo cp ${UP}/instance.env.example /opt/hft-collector/etc/hyperliquid.env'"
echo "  ssh -t <host> 'sudo \$EDITOR /opt/hft-collector/etc/hyperliquid.env'"
echo "  ssh <host> 'sudo systemctl enable --now hft-collector@hyperliquid'"
