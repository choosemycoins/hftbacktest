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
# Находки в журнал. КРАСНЫЕ ПЕЧАТАЮТСЯ ВСЕ И ВСЕГДА; режется только хвост
# жёлтых, и то с явной пометкой, сколько отброшено.
#
# Было: grep красных И жёлтых одним выражением, потом head -20. Двадцати жёлтых
# достаточно, чтобы красная находка не попала в вывод вовсе — так и случилось:
# единственный missing_required по VINE утонул под жёлтыми, оператор увидел
# «RED» без причины и пошёл искать её вручную. Гейт, который не называет
# причину своего отказа, стоит ровно столько же, сколько отсутствующий гейт.
emit_findings() {
    local txt="$1" name="$2" yellows dropped
    grep -E '^\s*\[red' "${txt}" | sed "s/^/gate-run[${name}]:   /" >&2 || true
    yellows="$(grep -cE '^\s*\[yellow' "${txt}" 2>/dev/null || echo 0)"
    grep -E '^\s*\[yellow' "${txt}" | head -20 | sed "s/^/gate-run[${name}]:   /" >&2 || true
    dropped=$(( yellows > 20 ? yellows - 20 : 0 ))
    if (( dropped > 0 )); then
        echo "gate-run[${name}]:   ... и ещё ${dropped} жёлтых, полный отчёт: ${txt}" >&2
    fi
}

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

# The dataset profile ONE instance is judged against, read the same way and for
# the same reason: never by sourcing an operator-authored file.
#
# Per instance rather than per host because "which streams are load-bearing" is
# a property of why that recording exists, not of the box it runs on. A signal
# instance under `mode-a-v1` may lose `@depth@0ms` to a warning; an instance
# recorded FOR the book — Binance publishes no USD-M bookTicker archive after
# 2024-04, and none at all for anything listed later — must go red for the same
# loss, or the one thing it exists for disappears behind a yellow the timer does
# not escalate. GATE_PROFILE in the environment still sets the default for
# instances that name none.
profile_of() {
    local name="$1" env_file="${ETC_DIR}/$1.env" profile=""
    if [[ -r "${env_file}" ]]; then
        profile="$(sed -n 's/^[[:space:]]*GATE_PROFILE=//p' "${env_file}" \
                   | tail -n1 | tr -d "\"'" | tr -d '[:space:]')"
    fi
    printf '%s\n' "${profile:-${PROFILE}}"
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
    profile="$(profile_of "${name}")"
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
        --dir "${dir}" --day "${DAY}" --profile "${profile}" \
        --json "${tmp_json}" > "${tmp_txt}" 2>&1 || rc=$?

    mv -f "${tmp_txt}" "${txt}"
    [[ -f "${tmp_json}" ]] && mv -f "${tmp_json}" "${json}"
    checked=$((checked + 1))

    case "${rc}" in
        0) echo "gate-run[${name}]: ok (profile ${profile}) -> ${txt}" ;;
        1)
            echo "gate-run[${name}]: RED -> ${txt}" >&2
            # The findings themselves, in the journal: an operator reading a
            # failure notification should not have to ssh in to learn what it
            # was. The full report stays on disk beside the data.
            emit_findings "${txt}" "${name}"
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

# --- каталоги поллеров -------------------------------------------------------
# Их не видит цикл выше: он перечисляет инстансы по etc/*.env, а у поллера .env
# нет. До 2026-08-22 это значило, что funding, params и positions не проверялись
# НИЧЕМ — ни разу за всё время их существования.
#
# Проверяет их отдельный инструмент, а не профиль внутри quality_report.py, и это
# осознанно: тот построен вокруг записей биржевых стримов (сайдкар _meta с
# session_start, набор стримов на символ, цепочки последовательности), а у
# поллера нет ничего из перечисленного — это снимки REST по таймеру. Общая
# абстракция поверх двух непохожих вещей стоила бы дороже двух инструментов.
#
# Каденция объявляется здесь, потому что она свойство ТАЙМЕРА, а не данных:
# ровно её и проверяем — молчащий таймер params стоил 23 часов ряда 12.08.
POLLER_DIRS="funding:300 params:3600 positions:3600"
POLLER_SCRIPT="${COLLECTOR_HOME}/tools/poller_report.py"
if [[ -f "${POLLER_SCRIPT}" ]]; then
    for spec in ${POLLER_DIRS}; do
        pname="${spec%%:*}"
        cadence="${spec##*:}"
        pdir="${DATA_ROOT}/${pname}"
        [[ -d "${pdir}" ]] || continue
        pout="${pdir}/gate"
        # Отказ mkdir здесь раньше уходил в /dev/null: сервис выходил с кодом 2,
        # не напечатав ни слова, и тревога становилась неотличимой от настоящего
        # RED. Инстансы двадцатью строками выше диагностируют этот же случай —
        # поллеры должны делать то же самое.
        if ! mkdir -p "${pout}" 2>/dev/null; then
            echo "gate-run[${pname}]: cannot create ${pout}" >&2
            echo "  Under ProtectSystem=strict the unit may write only to ReadWritePaths;" >&2
            echo "  и каталог должен принадлежать пользователю юнита (ls -ld ${pdir})." >&2
            worst=2
            continue
        fi
        ptxt="${pout}/${DAY}.txt"; pjson="${pout}/${DAY}.json"
        prc=0
        "${PYTHON}" "${POLLER_SCRIPT}" "${pdir}" --cadence-s "${cadence}" \
            --day "${DAY}" --json "${pjson}.partial" > "${ptxt}.partial" 2>&1 || prc=$?
        mv -f "${ptxt}.partial" "${ptxt}"
        [[ -f "${pjson}.partial" ]] && mv -f "${pjson}.partial" "${pjson}"
        checked=$((checked + 1))
        case "${prc}" in
            0) echo "gate-run[${pname}]: ok -> ${ptxt}" ;;
            1)
                echo "gate-run[${pname}]: RED -> ${ptxt}" >&2
                emit_findings "${ptxt}" "${pname}"
                red+=("${pname}")
                [[ "${worst}" -lt 1 ]] && worst=1
                ;;
            *)
                echo "gate-run[${pname}]: the check could not run (exit ${prc})" >&2
                tail -5 "${ptxt}" | sed "s/^/gate-run[${pname}]:   /" >&2 || true
                worst=2
                ;;
        esac
    done
else
    echo "gate-run: no ${POLLER_SCRIPT}; poller directories not checked" >&2
fi

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
