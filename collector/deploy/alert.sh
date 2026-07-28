#!/usr/bin/env bash
# Telegram alert hook for hft-collector units.
#
# Wired via systemd:  OnFailure=hft-collector-alert@%n.service
# The instance name (%i of the alert unit) is the FULL name of the unit that
# failed, e.g. "hft-collector@binancefuturesum.service".
#
# Reads /opt/hft-collector/etc/alert.env (root:600):
#   TG_BOT_TOKEN=...
#   TG_CHAT_ID=...
#
# Design notes:
# - Rate limit: at most one message per failed unit per 5 minutes, tracked by a
#   stamp file under /run (tmpfs — resets on boot, which is what you want: the
#   first failure after a reboot always alerts). A suppressed alert still lands
#   in the journal, so nothing is lost — only the phone stays quiet.
# - Delivery: 3 attempts, 5s apart, 10s timeout each. If Telegram is down, the
#   alert is in the journal anyway; this hook must never hang or loop forever.
# - This script deliberately does NOT exit non-zero on delivery failure:
#   a failing alert unit spamming OnFailure of itself would be a loop.

set -uo pipefail

FAILED_UNIT="${1:-unknown-unit}"
ENV_FILE=/opt/hft-collector/etc/alert.env
STAMP_DIR=/run/hft-collector-alert
RATE_LIMIT_S=300

if [[ ! -r "${ENV_FILE}" ]]; then
    echo "alert.sh: ${ENV_FILE} missing or unreadable; alert not sent" >&2
    exit 0
fi
# shellcheck disable=SC1090
source "${ENV_FILE}"
if [[ -z "${TG_BOT_TOKEN:-}" || -z "${TG_CHAT_ID:-}" ]]; then
    echo "alert.sh: TG_BOT_TOKEN/TG_CHAT_ID not set; alert not sent" >&2
    exit 0
fi

mkdir -p "${STAMP_DIR}"
STAMP="${STAMP_DIR}/${FAILED_UNIT//\//_}"
now=$(date +%s)
if [[ -f "${STAMP}" ]]; then
    last=$(cat "${STAMP}" 2>/dev/null || echo 0)
    if (( now - last < RATE_LIMIT_S )); then
        echo "alert.sh: rate-limited (last alert $((now - last))s ago) for ${FAILED_UNIT}" >&2
        exit 0
    fi
fi
echo "${now}" > "${STAMP}"

HOST=$(hostname)
STATE=$(systemctl show "${FAILED_UNIT}" -p ActiveState,SubState,ExecMainStatus 2>/dev/null | tr '\n' ' ')
JOURNAL=$(journalctl -u "${FAILED_UNIT}" -n 12 --no-pager -o short-iso 2>/dev/null | tail -c 2500)

TEXT="🔴 ${FAILED_UNIT} failed on ${HOST}
${STATE}

${JOURNAL}"

for _ in 1 2 3; do
    if curl -s -m 10 -X POST \
        "https://api.telegram.org/bot${TG_BOT_TOKEN}/sendMessage" \
        -d chat_id="${TG_CHAT_ID}" \
        --data-urlencode "text=${TEXT}" | grep -q '"ok":true'; then
        exit 0
    fi
    sleep 5
done
echo "alert.sh: delivery to Telegram failed after 3 attempts for ${FAILED_UNIT}" >&2
exit 0
