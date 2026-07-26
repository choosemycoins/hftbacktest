#!/usr/bin/env bash
# Launch wrapper for one collector instance, invoked by hft-collector@.service.
#
# Its job is to turn the instance env file into a validated argv and then get
# out of the way. It `exec`s the collector so the binary becomes PID 1 of the
# cgroup — systemd's SIGTERM then reaches the process that knows how to flush
# the gzip streams, with no shell in between to swallow it.
#
# Required environment (from EnvironmentFile=/opt/hft-collector/etc/%i.env):
#   COLLECTOR_EXCHANGE    Venue name, e.g. bybit
#   COLLECTOR_SYMBOLS     Whitespace-separated symbol list
#
# Optional:
#   COLLECTOR_DATA_DIR    Output directory; defaults to one per instance
#
# Provided by the unit:
#   COLLECTOR_HOME        Install root (/opt/hft-collector/current)
#   COLLECTOR_INSTANCE    systemd instance name, for log context

set -euo pipefail

: "${COLLECTOR_HOME:?COLLECTOR_HOME must be set (normally by the unit)}"
: "${COLLECTOR_EXCHANGE:?COLLECTOR_EXCHANGE must be set in the instance env file}"
: "${COLLECTOR_SYMBOLS:?COLLECTOR_SYMBOLS must be set in the instance env file}"

INSTANCE="${COLLECTOR_INSTANCE:-unknown}"

# A directory of the instance's own by default. Two collectors may not share
# one — the binary takes an exclusive flock on <dir>/.collector.lock and refuses
# to start if another holds it, because sharing interleaves gzip members and
# destroys both recordings. Defaulting per instance is what keeps an env file
# copied between units from colliding when this line is the one not edited.
COLLECTOR_DATA_DIR="${COLLECTOR_DATA_DIR:-/opt/hft-collector/data/${INSTANCE}}"

BIN="${COLLECTOR_HOME}/bin/collector"

if [[ ! -x "${BIN}" ]]; then
    echo "collector-run[${INSTANCE}]: binary not executable: ${BIN}" >&2
    exit 2
fi

# Fail closed on an unwritable data directory. Without this the collector
# starts, connects, and only discovers the problem when the first message
# arrives — by which point systemd has already reported the unit as active
# and the operator believes recording is under way.
if [[ ! -d "${COLLECTOR_DATA_DIR}" ]]; then
    if ! mkdir -p "${COLLECTOR_DATA_DIR}" 2>/dev/null; then
        echo "collector-run[${INSTANCE}]: cannot create COLLECTOR_DATA_DIR=${COLLECTOR_DATA_DIR}" >&2
        echo "  Under ProtectSystem=strict the unit may write only to ReadWritePaths." >&2
        echo "  If this path is outside /opt/hft-collector/data, add it with:" >&2
        echo "    systemctl edit hft-collector@${INSTANCE}   # [Service] ReadWritePaths=..." >&2
        exit 3
    fi
fi
if [[ ! -w "${COLLECTOR_DATA_DIR}" ]]; then
    echo "collector-run[${INSTANCE}]: COLLECTOR_DATA_DIR not writable: ${COLLECTOR_DATA_DIR}" >&2
    echo "  Owner: $(stat -c '%U:%G %a' "${COLLECTOR_DATA_DIR}" 2>/dev/null || echo unknown)" >&2
    echo "  Expected writable by: $(id -un)" >&2
    exit 3
fi

# Deliberate word splitting: COLLECTOR_SYMBOLS is a whitespace-separated list
# and each symbol must become its own argv entry. `read -ra` keeps that
# explicit and shellcheck-clean instead of relying on an unquoted expansion.
read -ra SYMBOLS <<< "${COLLECTOR_SYMBOLS}"
if [[ "${#SYMBOLS[@]}" -eq 0 ]]; then
    echo "collector-run[${INSTANCE}]: COLLECTOR_SYMBOLS is empty" >&2
    exit 4
fi

# Optional flags. Each is omitted entirely when its variable is unset, so the
# binary's own defaults stay authoritative and this wrapper never has to be
# kept in sync with them.
OPTS=()
[[ -n "${COLLECTOR_MIN_FREE_GB:-}" ]]  && OPTS+=(--min-free-gb "${COLLECTOR_MIN_FREE_GB}")
[[ -n "${COLLECTOR_STALL_TIMEOUT_MIN:-}" ]] && OPTS+=(--stall-timeout-min "${COLLECTOR_STALL_TIMEOUT_MIN}")
[[ -n "${COLLECTOR_BYBIT_DEPTHS:-}" ]] && OPTS+=(--bybit-depths "${COLLECTOR_BYBIT_DEPTHS}")
[[ -n "${COLLECTOR_HL_L2_MODES:-}" ]]  && OPTS+=(--hl-l2-modes "${COLLECTOR_HL_L2_MODES}")
if [[ "${COLLECTOR_NO_SYMBOL_CHECK:-0}" == "1" ]]; then
    echo "collector-run[${INSTANCE}]: WARNING symbol validation disabled" >&2
    OPTS+=(--no-symbol-check)
fi

echo "collector-run[${INSTANCE}]: exchange=${COLLECTOR_EXCHANGE} symbols=${SYMBOLS[*]} data_dir=${COLLECTOR_DATA_DIR}"
echo "collector-run[${INSTANCE}]: opts=${OPTS[*]:-(defaults)}"
echo "collector-run[${INSTANCE}]: $("${BIN}" --version)"

# Positional argv order is fixed by clap: <path> <exchange> <symbols...>.
# Flags precede them so a symbol can never be mistaken for a flag value.
exec "${BIN}" "${OPTS[@]}" "${COLLECTOR_DATA_DIR}" "${COLLECTOR_EXCHANGE}" "${SYMBOLS[@]}"
