#!/usr/bin/env bash
# Unattended daily offload — the launchd entry point on the OPERATOR machine.
#
#   offload-daily.sh            # normally invoked by launchd, runnable by hand
#
# Wraps offload.sh (which does the actual rsync → verify → rm) with the three
# things an unattended run needs and an interactive one does not:
#
#   1. A dated log under <target>/offload-logs/, pruned after 60 days.
#   2. A Telegram message when the run FAILS — credentials come from the same
#      env file the operator already keeps for alerts (TG_BOT_TOKEN,
#      TG_CHAT_ID). Success is silent: the host-side dead-man already attests
#      the recording, and a daily "all fine" message trains people to ignore
#      the channel.
#   3. An early-warning when the run succeeds but the host volume is filling
#      anyway (offload not keeping up with the burn rate): a warning is sent
#      at >= WARN_USE_PCT (default 70%). The collector's own min-free-gb check
#      is the fatal backstop; this fires days earlier.
#
# What this does NOT cover: launchd never firing at all (machine off for
# days). If you want a dead man for the offload itself, create a second
# healthchecks.io check and set HFT_OFFLOAD_PING_URL — success then pings it,
# failure hits <url>/fail, and silence escalates by itself.
#
# Configuration (environment, normally from the launchd plist):
#   HFT_HOST                ssh destination            (default hft-collector-tokyo)
#   HFT_TARGET              local archive directory    (default ~/hft-data)
#   HFT_TG_ENV              telegram env file          (default ~/.config/hftbacktest-connector/telegram-alert.env)
#   HFT_OFFLOAD_PING_URL    optional healthchecks check for the offload itself
#   HFT_OFFLOAD_WARN_PCT    host volume use%% that warns (default 70)
#
# Deliberately bash 3.2 — this runs on a Mac, same rule as offload.sh.

set -euo pipefail

DEPLOY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HFT_HOST="${HFT_HOST:-hft-collector-tokyo}"
HFT_TARGET="${HFT_TARGET:-${HOME}/hft-data}"
TG_ENV="${HFT_TG_ENV:-${HOME}/.config/hftbacktest-connector/telegram-alert.env}"
PING_URL="${HFT_OFFLOAD_PING_URL:-}"
WARN_USE_PCT="${HFT_OFFLOAD_WARN_PCT:-70}"

LOG_DIR="${HFT_TARGET}/offload-logs"
mkdir -p "${LOG_DIR}"
LOG="${LOG_DIR}/$(date -u +%Y%m%dT%H%M%SZ).log"

telegram() {
    # telegram <text> — best effort, never changes the exit code.
    local text="$1"
    [[ -r "${TG_ENV}" ]] || { echo "offload-daily: no ${TG_ENV}, alert not sent" >>"${LOG}"; return 0; }
    # shellcheck disable=SC1090
    source "${TG_ENV}"
    [[ -n "${TG_BOT_TOKEN:-}" && -n "${TG_CHAT_ID:-}" ]] || { echo "offload-daily: TG_BOT_TOKEN/TG_CHAT_ID unset, alert not sent" >>"${LOG}"; return 0; }
    curl -fsS -m 10 --retry 2 -o /dev/null \
        --data-urlencode "chat_id=${TG_CHAT_ID}" \
        --data-urlencode "text=${text}" \
        "https://api.telegram.org/bot${TG_BOT_TOKEN}/sendMessage" \
        || echo "offload-daily: telegram delivery failed" >>"${LOG}"
}

rc=0
"${DEPLOY_DIR}/offload.sh" --host "${HFT_HOST}" --target "${HFT_TARGET}" >>"${LOG}" 2>&1 || rc=$?

# Prune old logs; kilobytes, but nothing else ever deletes them.
find "${LOG_DIR}" -name '*.log' -mtime +60 -delete 2>/dev/null || true

if [[ "${rc}" -ne 0 ]]; then
    telegram "🔴 offload ${HFT_HOST} failed (exit ${rc})
log: ${LOG}

$(tail -c 2500 "${LOG}")"
    [[ -n "${PING_URL}" ]] && curl -fsS -m 10 --retry 2 -o /dev/null \
        --data-raw "offload exit ${rc}" "${PING_URL}/fail" 2>/dev/null || true
    exit "${rc}"
fi

# Success. Check the host volume before going quiet: offload.sh prints
# `df -h` of the data volume as its last act ("free on the host now").
# offload.sh prints `df -h | tail -n +2`, so exactly one data line follows.
use_pct="$(awk '/free on the host now/{getline; for(i=1;i<=NF;i++) if ($i ~ /%$/) {gsub("%","",$i); print $i; exit}}' "${LOG}")"
if [[ -n "${use_pct}" ]] && (( use_pct >= WARN_USE_PCT )); then
    telegram "🟡 offload ${HFT_HOST}: ran fine, but the data volume is at ${use_pct}% AFTER the offload — the burn rate is outrunning the archive. Log: ${LOG}"
fi

[[ -n "${PING_URL}" ]] && curl -fsS -m 10 --retry 2 -o /dev/null "${PING_URL}" 2>/dev/null || true
exit 0
