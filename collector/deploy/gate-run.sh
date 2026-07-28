#!/usr/bin/env bash
# Run yesterday's data-quality gate over this host's recordings.
#
# Invoked by hft-collector-gate@.service; runnable by hand:
#   nice -n 19 ionice -c3 /opt/hft-collector/current/bin/gate-run.sh all
#
# The argument is an INSTANCE SET:
#   <name>   one collector instance, i.e. /opt/hft-collector/etc/<name>.env
#   all      every instance configured on this host (the default)
#
# For each instance's data directory it runs quality_report.py over ONE day and
# writes the result next to the data:
#
#   <data_dir>/gate/<YYYYMMDD>.txt     the operator's view
#   <data_dir>/gate/<YYYYMMDD>.json    quality-report-v1, what build_dataset.py reads
#
# Exit codes:  0 every instance green or yellow
#              1 at least one instance RED — the unit lands in `failed`, which
#                is what a journal entry and any OnFailure= hook hang off
#              2 could not run the check at all (no python3, no report script,
#                an unreadable directory)
#
# ## One report per directory, not one report for the host
#
# quality_report.py takes one directory per venue per report and refuses two
# directories of the same venue in one run ("one venue per report entry"). Two
# USD-M recordings on one host is exactly the configuration the signal union
# needs (build_dataset.py --binance-report-b), so a single combined run would
# fail on the very setup this exists to watch. One run per directory also makes
# "next to the data" well defined, and the per-instance JSON is directly what
# --binance-report-b consumes.
#
# ## It competes with the recording for 2 vCPU
#
# A day is gigabytes of gzip and the check decodes all of it, on a box whose
# only real job is to not fall behind. The unit runs it at Nice=19 with idle I/O
# priority so it yields to the collector whenever they are both runnable; a
# manual run should be prefixed as shown above. It will take longer under load.
# That is the intended trade — a late report costs nothing, a dropped frame is
# gone.
#
# ## What it will call red at 00:35 that is not actually wrong
#
# Files rotate lazily: yesterday's `.gz` gets its gzip trailer on the FIRST
# write after midnight (`file.rs`), so a symbol that has had no frame since
# 23:59 still has an unterminated member at 00:35, and an unterminated member on
# a finalized day is corruption as far as the report is concerned. Liquid feeds
# rotate within milliseconds of midnight and never see this. If an instance
# records something thin enough to go half an hour without a print, move its
# timer later with a drop-in rather than learning to ignore a red:
#   systemctl edit hft-collector-gate@<set>.timer   # [Timer] OnCalendar=...

set -euo pipefail

COLLECTOR_HOME="${COLLECTOR_HOME:-/opt/hft-collector/current}"
ETC_DIR="${COLLECTOR_ETC_DIR:-/opt/hft-collector/etc}"
DATA_ROOT="${COLLECTOR_DATA_ROOT:-/opt/hft-collector/data}"
REPORT_SCRIPT="${COLLECTOR_HOME}/tools/quality_report.py"
PYTHON="${GATE_PYTHON:-python3}"
PROFILE="${GATE_PROFILE:-mode-a-v1}"
SET="${1:-${COLLECTOR_GATE_SET:-all}}"

if ! command -v "${PYTHON}" >/dev/null 2>&1; then
    echo "gate-run: ${PYTHON} not found. quality_report.py is stdlib-only, so any" >&2
    echo "          python3 the distribution ships will do." >&2
    exit 2
fi
if [[ ! -f "${REPORT_SCRIPT}" ]]; then
    echo "gate-run: ${REPORT_SCRIPT} is missing — this release predates the gate." >&2
    echo "          Install a release built by build-release.sh at or after it." >&2
    exit 2
fi

# Yesterday, on this host's UTC clock — the same clock that named the files.
# GNU date first, BSD second, so the script also runs on a Mac for a spot check.
if [[ -n "${GATE_DAY:-}" ]]; then
    DAY="${GATE_DAY}"
elif DAY="$(date -u -d yesterday +%Y%m%d 2>/dev/null)"; then
    :
elif DAY="$(date -u -v-1d +%Y%m%d 2>/dev/null)"; then
    :
else
    echo "gate-run: cannot work out yesterday's date with this \`date\`." >&2
    exit 2
fi
if [[ ! "${DAY}" =~ ^[0-9]{8}$ ]]; then
    echo "gate-run: GATE_DAY must be YYYYMMDD, got '${DAY}'." >&2
    exit 2
fi

# The data directory of one instance, WITHOUT executing its env file.
#
# `source`ing an operator-authored file to read one variable is a shell
# injection with extra steps, and this runs on a timer with the collector's own
# privileges. The default matches collector-run.sh: one directory per instance,
# because the collector takes an exclusive lock on it.
data_dir_of() {
    local name="$1" env_file="${ETC_DIR}/$1.env" dir=""
    if [[ -r "${env_file}" ]]; then
        dir="$(sed -n 's/^[[:space:]]*COLLECTOR_DATA_DIR=//p' "${env_file}" \
               | tail -n1 | tr -d "\"'" | tr -d '[:space:]')"
    fi
    printf '%s\n' "${dir:-${DATA_ROOT}/${name}}"
}

INSTANCES=()
if [[ "${SET}" == "all" ]]; then
    shopt -s nullglob
    for env_file in "${ETC_DIR}"/*.env; do
        name="$(basename "${env_file}" .env)"
        # alert.env and friends are operator configuration, not collection jobs.
        [[ -d "$(data_dir_of "${name}")" ]] || continue
        INSTANCES+=("${name}")
    done
    shopt -u nullglob
    if [[ "${#INSTANCES[@]}" -eq 0 ]]; then
        echo "gate-run: no instance with a data directory under ${DATA_ROOT}." >&2
        exit 2
    fi
else
    INSTANCES=("${SET}")
fi

echo "gate-run: day=${DAY} profile=${PROFILE} instances=${INSTANCES[*]}"

worst=0
red=()
checked=0
for name in "${INSTANCES[@]}"; do
    dir="$(data_dir_of "${name}")"
    if [[ ! -d "${dir}" ]]; then
        echo "gate-run[${name}]: ${dir} is not a directory" >&2
        worst=2
        continue
    fi
    out_dir="${dir}/gate"
    if ! mkdir -p "${out_dir}" 2>/dev/null; then
        echo "gate-run[${name}]: cannot create ${out_dir}" >&2
        echo "  Under ProtectSystem=strict the unit may write only to ReadWritePaths." >&2
        worst=2
        continue
    fi

    txt="${out_dir}/${DAY}.txt"
    json="${out_dir}/${DAY}.json"
    # Written to a temporary name and renamed, so a reader — or the next
    # build_dataset.py run — never sees a half-written report.
    tmp_txt="${txt}.partial"
    tmp_json="${json}.partial"

    rc=0
    "${PYTHON}" "${REPORT_SCRIPT}" \
        --dir "${dir}" --day "${DAY}" --profile "${PROFILE}" \
        --json "${tmp_json}" > "${tmp_txt}" 2>&1 || rc=$?

    mv -f "${tmp_txt}" "${txt}"
    [[ -f "${tmp_json}" ]] && mv -f "${tmp_json}" "${json}"
    checked=$((checked + 1))

    case "${rc}" in
        0) echo "gate-run[${name}]: ok -> ${txt}" ;;
        1)
            echo "gate-run[${name}]: RED -> ${txt}" >&2
            # The findings themselves, in the journal: an operator reading a
            # failure notification should not have to ssh in to learn what it
            # was. The full report stays on disk beside the data.
            grep -E '^\s*\[(red|yellow)' "${txt}" | head -20 | sed "s/^/gate-run[${name}]:   /" >&2 || true
            red+=("${name}")
            [[ "${worst}" -lt 1 ]] && worst=1
            ;;
        *)
            echo "gate-run[${name}]: the check could not run (exit ${rc})" >&2
            tail -5 "${txt}" | sed "s/^/gate-run[${name}]:   /" >&2 || true
            worst=2
            ;;
    esac
done

echo "gate-run: checked ${checked} instance(s) for ${DAY}"
if [[ "${#red[@]}" -gt 0 ]]; then
    echo "gate-run: RED: ${red[*]}" >&2
fi

# Dead-man heartbeat (healthchecks.io). One successful ping a day attests, in a
# single bit: host up, volume mounted, recording ran, yesterday validated. A
# red day hits <url>/fail (instant alert) with the verdict as the body; a
# could-not-run day sends NOTHING — silence is precisely the signal a dead-man
# service exists to escalate, and pinging "ok" from a broken gate would defeat
# it. Best-effort: delivery failure must not change the exit code the alert
# hook keys on.
ALERT_ENV=/opt/hft-collector/etc/alert.env
if [[ -r "${ALERT_ENV}" ]]; then
    # shellcheck disable=SC1090
    source "${ALERT_ENV}"
    if [[ -n "${HEALTHCHECK_PING_URL:-}" ]]; then
        case "${worst}" in
            0) curl -fsS -m 10 --retry 2 -o /dev/null "${HEALTHCHECK_PING_URL}" \
                   || echo "gate-run: heartbeat delivery failed (gate result unaffected)" >&2 ;;
            1) curl -fsS -m 10 --retry 2 -o /dev/null \
                   --data-raw "RED ${DAY}: ${red[*]}" "${HEALTHCHECK_PING_URL}/fail" \
                   || echo "gate-run: fail-ping delivery failed (gate result unaffected)" >&2 ;;
            *) echo "gate-run: gate could not run; withholding the heartbeat so the dead-man fires" >&2 ;;
        esac
    fi
fi
exit "${worst}"
