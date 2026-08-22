#!/usr/bin/env bash
# Rolling-window tier of the hybrid archive: the operator Mac keeps the last
# HFT_KEEP_DAYS days of recordings (datasets and backtests want them local),
# everything older MOVES to HFT_ARCHIVE_DIR — an external volume or any other
# mounted path. Called by offload-daily.sh before the offload (moving first is
# what frees the space the offload is about to need); runnable by hand:
#
#   HFT_ARCHIVE_DIR=/Volumes/T7/hft-archive collector/deploy/archive-rotate.sh
#
# Environment:
#   HFT_TARGET        local archive root                (default ~/hft-data)
#   HFT_ARCHIVE_DIR   where old days go; unset = tier disabled (exit 0, says so)
#   HFT_KEEP_DAYS     how many most-recent UTC days stay local (default 2)
#   HFT_ROTATE_TODAY  test override for "today", YYYYMMDD
#
# What moves: per-instance data files and _meta sidecars whose UTC day is
# STRICTLY older than today − HFT_KEEP_DAYS. What never moves: gate/ reports
# (kilobytes, and build_dataset reads them), reports/, offload-logs/, and
# dataset-* directories (derived artifacts, user-managed).
#
# Every move is copy → sha256 both sides → rm; a mismatch leaves the local
# file in place and fails the run. Same allowlist as offload.sh, including
# the colon for HL builder-dex names.
#
# Exit codes: 0 rotated (or nothing to do / tier disabled)
#             1 a move failed verification or the filesystem said no
#             2 HFT_ARCHIVE_DIR is set but not reachable (volume unmounted) —
#               nothing was touched; the caller decides how loud to be
#
# Deliberately bash 3.2 — this runs on a Mac, same rule as offload.sh.

set -euo pipefail

TARGET="${HFT_TARGET:-${HOME}/hft-data}"
ARCHIVE="${HFT_ARCHIVE_DIR:-}"
KEEP_DAYS="${HFT_KEEP_DAYS:-2}"

if [[ -z "${ARCHIVE}" ]]; then
    echo "archive-rotate: HFT_ARCHIVE_DIR is not set — tier disabled, nothing moves."
    exit 0
fi
if [[ ! "${KEEP_DAYS}" =~ ^[0-9]+$ ]]; then
    echo "archive-rotate: HFT_KEEP_DAYS must be a number, got '${KEEP_DAYS}'." >&2
    exit 1
fi
if [[ ! -d "${ARCHIVE}" ]]; then
    echo "archive-rotate: ${ARCHIVE} is not reachable (volume not mounted?); nothing was touched." >&2
    exit 2
fi

if [[ -n "${HFT_ROTATE_TODAY:-}" ]]; then
    TODAY="${HFT_ROTATE_TODAY}"
elif TODAY="$(date -u +%Y%m%d 2>/dev/null)"; then
    :
fi
if [[ ! "${TODAY}" =~ ^[0-9]{8}$ ]]; then
    echo "archive-rotate: cannot work out today's UTC date." >&2
    exit 1
fi
# The newest day that still MOVES: strictly older than today − KEEP_DAYS.
# BSD date first, GNU second — same order and reason as gate-run.sh.
if CUTOFF="$(date -u -v-"${KEEP_DAYS}"d -j -f %Y%m%d "${TODAY}" +%Y%m%d 2>/dev/null)"; then
    :
elif CUTOFF="$(date -u -d "${TODAY} -${KEEP_DAYS} days" +%Y%m%d 2>/dev/null)"; then
    :
else
    echo "archive-rotate: cannot compute the cutoff with this \`date\`." >&2
    exit 1
fi

# Same recognition rule as offload.sh, colon included; gate/ is excluded by
# construction because this only looks at an instance directory's top level.
day_of() {
    local name="$1"
    # Имя может нести ПОДКАТАЛОГ, и дату несёт только последний компонент.
    # Поллер параметров пишет `<площадка>/params_<площадка>_<источник>_<день>.gz`,
    # и прежняя редакция, якорившая шаблон на `^`, такое имя не признавала:
    # файл уходил в «unrecognised names, left alone», то есть НЕ доезжал до
    # архива и НЕ удалялся с хоста — молча, при зелёном пульсе. Замерено
    # 2026-08-22; тот же класс, что блокер #88 у funding-поллера.
    #
    # Послабление касается ровно одного случая — .gz в подкаталоге, — и потому
    # безопасно: отчёты гейта это .txt/.json и разбираются отдельной веткой,
    # которая НАМЕРЕННО требует буквального `gate/`, а сайдкары _meta лежат
    # только на первом уровне.
    local leaf="${name##*/}"
    if [[ "${leaf}" =~ ^[A-Za-z0-9._:-]+_([0-9]{8})\.gz$ ]] \
    || [[ "${leaf}" =~ ^_meta_[A-Za-z0-9._-]+_([0-9]{8})\.jsonl$ ]] \; then
        echo "${BASH_REMATCH[1]}"
        return 0
    fi
    return 1
}

sha_of() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | cut -d' ' -f1
    else
        shasum -a 256 "$1" | cut -d' ' -f1
    fi
}

echo "archive-rotate: keep ${KEEP_DAYS} day(s) (moving days < ${CUTOFF}) ${TARGET} -> ${ARCHIVE}"

moved=0
kept=0
failed=0
for inst_dir in "${TARGET}"/*/; do
    [[ -d "${inst_dir}" ]] || continue
    inst="$(basename "${inst_dir}")"
    case "${inst}" in
        reports|offload-logs|dataset-*) continue ;;
    esac
    for f in "${inst_dir}"*; do
        [[ -f "${f}" ]] || continue
        name="$(basename "${f}")"
        day="$(day_of "${name}")" || continue
        if (( 10#${day} >= 10#${CUTOFF} )); then
            kept=$((kept + 1))
            continue
        fi
        mkdir -p "${ARCHIVE}/${inst}"
        dest="${ARCHIVE}/${inst}/${name}"
        if ! cp "${f}" "${dest}.partial" || ! mv "${dest}.partial" "${dest}"; then
            echo "  ERROR: copy failed for ${inst}/${name}; local file untouched." >&2
            rm -f "${dest}.partial" 2>/dev/null || true
            failed=$((failed + 1))
            continue
        fi
        src_sha="$(sha_of "${f}")"
        dst_sha="$(sha_of "${dest}")"
        if [[ "${src_sha}" != "${dst_sha}" ]]; then
            echo "  MISMATCH ${inst}/${name}: local ${src_sha}, archive ${dst_sha}; local kept, archive copy removed." >&2
            rm -f "${dest}"
            failed=$((failed + 1))
            continue
        fi
        rm "${f}"
        moved=$((moved + 1))
        echo "  moved ${inst}/${name}"
    done
done

echo "archive-rotate: moved ${moved}, kept ${kept} (within window), failed ${failed}"
[[ "${failed}" -gt 0 ]] && exit 1
exit 0
