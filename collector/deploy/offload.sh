#!/usr/bin/env bash
# Move finalized recordings off a collector host onto the operator's machine.
#
# Run this FROM the operator machine, not on the host:
#   ./offload.sh --host user@box --target ~/marketdata [--instances a,b] [--dry-run]
#
# The host records 3-5 GB a day for a full USD-M symbol set and has a 53 GB
# volume, so it fills in under a fortnight. The collector will not overwrite or
# prune anything — it stops when free space hits --min-free-gb and marks the
# unit failed. Something has to take the data away, and this is that something.
#
# What it does, per instance directory, in this order:
#   1. Lists the host's FINALIZED files — yesterday and older, by the HOST's
#      UTC date. Today's are never touched: the gzip member being appended to
#      has no trailer yet, and a collector restart appends another member to
#      the same file.
#   2. sha256 on the HOST for exactly those files.
#   3. rsync them here.
#   4. sha256 HERE, and compare hash for hash.
#   5. gzip -t every .gz that arrived — a matching hash proves the copy, `gzip
#      -t` proves the thing copied was ever readable.
#   6. ONLY THEN rm on the host, and only the files that passed all of it.
#
# Idempotent. Interrupt it anywhere and run it again: rsync re-copies nothing
# it already has, verification runs from scratch, and only verified files are
# removed. A file already deleted on the host is simply not listed again.
#
# What it does NOT do:
#   - Never touches today's files, and asserts that a second time immediately
#     before the delete — that is the one irreversible step here.
#   - Never deletes anything it did not just verify byte for byte.
#   - Never deletes the gate reports (gate/*.txt|json). They are copied and
#     LEFT: kilobytes against gigabytes, and the host's own audit trail for a
#     day whose data has gone is worth keeping where the journal is.
#   - Never touches a file whose name it does not recognise. The allowlist is
#     also what makes the remote commands safe: only [A-Za-z0-9._-] and the
#     `gate/` prefix ever reach a remote shell.
#
# Requires on this machine: ssh, rsync, sha256sum or shasum, gzip.
# Requires on the host: GNU find, sha256sum, a shell. Nothing is installed.
#
# Deliberately written for bash 3.2 — no `mapfile`, no `${var@Q}`, no
# associative arrays. The operator machine here is a Mac, whose /bin/bash is
# 3.2, and a script that dies with a parse error before it can print "you need
# a newer bash" is not a better outcome than one that simply works.

set -euo pipefail

REMOTE_DATA_ROOT="${HFT_REMOTE_DATA_ROOT:-/opt/hft-collector/data}"
HOST="${HFT_HOST:-}"
TARGET="${HFT_TARGET:-}"
INSTANCES_ARG="${HFT_INSTANCES:-}"
DRY_RUN=0
KEEP_ON_HOST=0

usage() {
    cat >&2 <<'EOF'
Usage: offload.sh --host <[user@]host> --target <dir> [options]

  --host       <[user@]host>   ssh destination of the collector box   (env HFT_HOST)
  --target     <dir>           local directory to archive into        (env HFT_TARGET)
  --instances  <a,b,c>         instance dirs; default: all on the host (env HFT_INSTANCES)
  --remote-root <dir>          host data root (default /opt/hft-collector/data)
  --dry-run                    list what would move; copy nothing, delete nothing
  --keep-on-host               copy and verify, but never delete
  -h, --help

Files land in <target>/<instance>/, mirroring the host's layout.
EOF
}

while [[ "$#" -gt 0 ]]; do
    case "$1" in
        --host) HOST="${2:?--host needs a value}"; shift 2 ;;
        --target) TARGET="${2:?--target needs a value}"; shift 2 ;;
        --instances) INSTANCES_ARG="${2:?--instances needs a value}"; shift 2 ;;
        --remote-root) REMOTE_DATA_ROOT="${2:?--remote-root needs a value}"; shift 2 ;;
        --dry-run) DRY_RUN=1; shift ;;
        --keep-on-host) KEEP_ON_HOST=1; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "ERROR: unknown argument: $1" >&2; usage; exit 1 ;;
    esac
done

if [[ -z "${HOST}" ]] || [[ -z "${TARGET}" ]]; then
    echo "ERROR: --host and --target are both required." >&2
    usage
    exit 1
fi

for tool in ssh rsync gzip; do
    if ! command -v "${tool}" >/dev/null 2>&1; then
        echo "ERROR: ${tool} is not installed on this machine." >&2
        exit 1
    fi
done

# macOS ships `shasum`, Linux ships `sha256sum`; both print "<hash>  <name>".
if command -v sha256sum >/dev/null 2>&1; then
    SHA_LOCAL=(sha256sum)
elif command -v shasum >/dev/null 2>&1; then
    SHA_LOCAL=(shasum -a 256)
else
    echo "ERROR: neither sha256sum nor shasum found; cannot verify a transfer." >&2
    echo "       Copying without verifying is not something this script will do." >&2
    exit 1
fi

mkdir -p "${TARGET}"
TARGET="$(cd "${TARGET}" && pwd)"

WORK="$(mktemp -d -t hft-offload-XXXXXX)"
trap 'rm -rf "${WORK}"' EXIT

# ---------------------------------------------------------------- the host

# The HOST's UTC date decides which file is still being written, because the
# host's clock is what names the files (`file.rs` rotates on `Utc::now()`).
# Using this machine's date would be a guess about someone else's clock, and a
# guess an hour either side of midnight deletes a file that is still open.
REMOTE_TODAY="$(ssh -n "${HOST}" 'date -u +%Y%m%d')" || {
    echo "ERROR: cannot reach ${HOST} over ssh." >&2
    exit 2
}
LOCAL_TODAY="$(date -u +%Y%m%d)"
if [[ ! "${REMOTE_TODAY}" =~ ^[0-9]{8}$ ]]; then
    echo "ERROR: the host answered '${REMOTE_TODAY}' for its UTC date, which is not YYYYMMDD." >&2
    exit 2
fi
echo "==> Host ${HOST}, UTC date ${REMOTE_TODAY} (data root ${REMOTE_DATA_ROOT})"
if [[ "${REMOTE_TODAY}" != "${LOCAL_TODAY}" ]]; then
    # Not fatal: it is legitimate for a few minutes around midnight. It is
    # still worth saying, because the finalized set is decided by the host's
    # answer and not by this machine's.
    echo "    NOTE: this machine says ${LOCAL_TODAY}. The host's date is the one used."
fi

if [[ -n "${INSTANCES_ARG}" ]]; then
    IFS=', ' read -ra INSTANCES <<< "${INSTANCES_ARG}"
else
    INSTANCES=()
    while IFS= read -r line; do
        [[ -n "${line}" ]] && INSTANCES+=("${line}")
    done < <(ssh -n "${HOST}" \
        "find '${REMOTE_DATA_ROOT}' -mindepth 1 -maxdepth 1 -type d -printf '%f\n' 2>/dev/null | sort")
fi
if [[ "${#INSTANCES[@]}" -eq 0 ]]; then
    echo "ERROR: no instance directories under ${REMOTE_DATA_ROOT} on ${HOST}." >&2
    exit 2
fi
echo "==> Instances: ${INSTANCES[*]}"

# ---------------------------------------------------------------- helpers

# A file the collector wrote, and the UTC day it belongs to.
#
# Three shapes and no others (`file.rs`): the day's data, the day's sidecar,
# and the gate report written next to them. Anything else on the host is left
# exactly where it is — an unrecognised name is not something to delete.
#
# Nothing outside [A-Za-z0-9._-] and a literal `gate/` can match, which is what
# makes it safe to interpolate these names into a remote shell command below.
day_of() {
    local name="$1"
    if [[ "${name}" =~ ^[A-Za-z0-9._-]+_([0-9]{8})\.gz$ ]] \
    || [[ "${name}" =~ ^_meta_[A-Za-z0-9._-]+_([0-9]{8})\.jsonl$ ]] \
    || [[ "${name}" =~ ^gate/([0-9]{8})\.(txt|json)$ ]]; then
        echo "${BASH_REMATCH[1]}"
        return 0
    fi
    return 1
}

# Gate reports are copied and left behind; everything else is copied and taken.
is_gate_report() { [[ "$1" == gate/* ]]; }

human() {
    local bytes="$1"
    awk -v b="${bytes}" 'BEGIN {
        split("B KB MB GB TB", u, " "); i = 1
        while (b >= 1024 && i < 5) { b /= 1024; i++ }
        printf (i == 1 ? "%d %s" : "%.1f %s"), b, u[i]
    }'
}

TOTAL_VERIFIED=0
TOTAL_BYTES=0
TOTAL_DELETED=0
TOTAL_SKIPPED=0
FAILURES=0
SUMMARY=()

# ---------------------------------------------------------------- per instance

for instance in "${INSTANCES[@]}"; do
    if [[ ! "${instance}" =~ ^[A-Za-z0-9._-]+$ ]]; then
        echo "WARNING: skipping instance name '${instance}' — not [A-Za-z0-9._-]." >&2
        FAILURES=$((FAILURES + 1))
        continue
    fi
    remote_dir="${REMOTE_DATA_ROOT}/${instance}"
    local_dir="${TARGET}/${instance}"
    echo ""
    echo "=== ${instance}"

    listing="${WORK}/${instance}.listing"
    if ! ssh -n "${HOST}" \
        "find '${remote_dir}' -mindepth 1 -maxdepth 2 -type f -printf '%P\t%s\n' 2>/dev/null" \
        > "${listing}"; then
        echo "  ERROR: cannot list ${remote_dir}" >&2
        FAILURES=$((FAILURES + 1))
        continue
    fi

    move_list="${WORK}/${instance}.move"        # copied and then removed
    keep_list="${WORK}/${instance}.keep"        # copied and left in place
    : > "${move_list}"
    : > "${keep_list}"
    bytes=0
    live=0
    unknown=0
    while IFS=$'\t' read -r name size; do
        [[ -z "${name}" ]] && continue
        if ! day="$(day_of "${name}")"; then
            unknown=$((unknown + 1))
            continue
        fi
        # `10#` forces base 10: bash reads a leading zero as octal, and a
        # date is the one place that silently changes the answer.
        if (( 10#${day} >= 10#${REMOTE_TODAY} )); then
            live=$((live + 1))
            continue
        fi
        if is_gate_report "${name}"; then
            printf '%s\n' "${name}" >> "${keep_list}"
        else
            printf '%s\n' "${name}" >> "${move_list}"
            bytes=$((bytes + size))
        fi
    done < "${listing}"

    n_move="$(wc -l < "${move_list}" | tr -d ' ')"
    n_keep="$(wc -l < "${keep_list}" | tr -d ' ')"
    echo "  finalized: ${n_move} file(s), $(human "${bytes}"); gate reports: ${n_keep}"
    [[ "${live}" -gt 0 ]] && echo "  today (${REMOTE_TODAY}): ${live} file(s), left alone"
    [[ "${unknown}" -gt 0 ]] && echo "  unrecognised names: ${unknown}, left alone"

    if [[ "${n_move}" -eq 0 && "${n_keep}" -eq 0 ]]; then
        SUMMARY+=("${instance}: nothing to move")
        continue
    fi

    all_list="${WORK}/${instance}.all"
    cat "${move_list}" "${keep_list}" > "${all_list}"

    if [[ "${DRY_RUN}" -eq 1 ]]; then
        echo "  DRY RUN — would copy to ${local_dir}/ and then remove ${n_move} file(s) on the host:"
        # `head` first, `sed` second, and not the other way round. Under `set -o
        # pipefail` a `sed … | head -20` kills sed with SIGPIPE as soon as head
        # has its twenty lines, the pipeline reports 141, and `set -e` ends the
        # run — silently, with no summary and no further instances, on exactly
        # the listing large enough to need truncating. Measured: fine at 50
        # files, exit 141 at 3000. `head` reading the file directly writes to a
        # pipe nobody closes early.
        head -20 "${move_list}" | sed 's/^/    /'
        [[ "${n_move}" -gt 20 ]] && echo "    … and $((n_move - 20)) more"
        SUMMARY+=("${instance}: would move ${n_move} file(s), $(human "${bytes}")")
        TOTAL_BYTES=$((TOTAL_BYTES + bytes))
        continue
    fi

    mkdir -p "${local_dir}"

    # --files-from with a relative list keeps `gate/` nested where it belongs.
    # -c is deliberate: rsync's default quick check is size+mtime, and this
    # script's whole promise is that nothing is deleted on the strength of a
    # timestamp.
    echo "  copying…"
    if ! rsync -a --checksum --partial --human-readable \
        --files-from="${all_list}" "${HOST}:${remote_dir}/" "${local_dir}/"; then
        echo "  ERROR: rsync failed; nothing will be removed on the host." >&2
        FAILURES=$((FAILURES + 1))
        continue
    fi

    # --- verify: the host's hashes, ours, and a decode of every .gz ---------
    remote_sums="${WORK}/${instance}.remote.sha"
    if ! ssh "${HOST}" "cd '${remote_dir}' && xargs -d '\n' -r sha256sum --" \
        < "${all_list}" > "${remote_sums}"; then
        echo "  ERROR: could not checksum on the host; nothing will be removed." >&2
        FAILURES=$((FAILURES + 1))
        continue
    fi

    verified="${WORK}/${instance}.verified"
    : > "${verified}"
    bad=0
    while read -r want name; do
        [[ -z "${name}" ]] && continue
        local_file="${local_dir}/${name}"
        if [[ ! -f "${local_file}" ]]; then
            echo "  MISMATCH ${name}: did not arrive" >&2
            bad=$((bad + 1))
            continue
        fi
        got="$("${SHA_LOCAL[@]}" "${local_file}" | cut -d' ' -f1)"
        if [[ "${got}" != "${want}" ]]; then
            echo "  MISMATCH ${name}: host ${want}, here ${got}" >&2
            bad=$((bad + 1))
            continue
        fi
        # A hash proves the copy is faithful. It does not prove the original
        # was ever a readable gzip stream — a member truncated by a SIGKILL
        # copies perfectly. This is the only check that would notice.
        if [[ "${name}" == *.gz ]] && ! gzip -t "${local_file}" 2>/dev/null; then
            echo "  CORRUPT  ${name}: copied faithfully, but will not decode" >&2
            echo "           left on the host for investigation" >&2
            bad=$((bad + 1))
            continue
        fi
        is_gate_report "${name}" || printf '%s\n' "${name}" >> "${verified}"
    done < "${remote_sums}"

    n_verified="$(wc -l < "${verified}" | tr -d ' ')"
    # Two different counts, and the difference matters: `ver_total` is
    # everything that arrived intact, `n_verified` is the subset that is also
    # eligible for deletion (gate reports never are).
    ver_total=$((n_move + n_keep - bad))
    echo "  verified: ${ver_total} of $((n_move + n_keep)) file(s)"
    # "verified", not "copied": a file that failed `gzip -t` is here too, and
    # calling it copied in a summary about what was safe to delete would be the
    # one misreading that matters.
    TOTAL_VERIFIED=$((TOTAL_VERIFIED + ver_total))
    TOTAL_BYTES=$((TOTAL_BYTES + bytes))
    if [[ "${bad}" -gt 0 ]]; then
        FAILURES=$((FAILURES + 1))
    fi

    if [[ "${KEEP_ON_HOST}" -eq 1 ]]; then
        echo "  --keep-on-host: nothing removed"
        SUMMARY+=("${instance}: verified ${ver_total}, removed 0 (--keep-on-host)")
        TOTAL_SKIPPED=$((TOTAL_SKIPPED + n_verified))
        continue
    fi
    if [[ "${n_verified}" -eq 0 ]]; then
        SUMMARY+=("${instance}: verified ${ver_total}, removed 0")
        continue
    fi

    # The one irreversible step, so the date rule is asserted a second time
    # rather than trusted. A bug in the filter above is a deleted recording;
    # a bug here is an error message.
    while read -r name; do
        day="$(day_of "${name}")" || { echo "ERROR: unfiltered name in the delete list: '${name}'" >&2; exit 3; }
        if (( 10#${day} >= 10#${REMOTE_TODAY} )); then
            echo "ERROR: refusing to delete ${name}: day ${day} is not finalized on the host (${REMOTE_TODAY})." >&2
            exit 3
        fi
    done < "${verified}"

    echo "  removing ${n_verified} verified file(s) on the host…"
    # sudo is load-bearing: the recordings are owned by the hftcollector
    # system user and the directory is 0755, so the operator's ssh user can
    # READ everything (which is why download+verify succeed) but cannot unlink.
    # Found on the first real run — the shimmed test harness could not model
    # host permissions. Reads stay unprivileged; only the unlink escalates.
    if ! ssh "${HOST}" "cd '${remote_dir}' && sudo xargs -d '\n' -r rm -f --" < "${verified}"; then
        echo "  ERROR: remove failed on the host; the local copies are good." >&2
        FAILURES=$((FAILURES + 1))
        SUMMARY+=("${instance}: verified ${ver_total}, REMOVE FAILED")
        continue
    fi
    TOTAL_DELETED=$((TOTAL_DELETED + n_verified))
    SUMMARY+=("${instance}: verified ${ver_total}, removed ${n_verified}, $(human "${bytes}")")
done

# ---------------------------------------------------------------- summary

echo ""
echo "==> Summary"
# Guarded because bash 3.2 — the Mac's /bin/bash — treats "${arr[@]}" on an
# EMPTY array as an unbound variable under `set -u` and aborts. Reached
# whenever every instance was skipped, which is exactly the run that most needs
# to print its summary.
if [[ "${#SUMMARY[@]}" -eq 0 ]]; then
    echo "    nothing was examined"
else
    for line in "${SUMMARY[@]}"; do
        echo "    ${line}"
    done
fi
echo ""
if [[ "${DRY_RUN}" -eq 1 ]]; then
    echo "    DRY RUN: $(human "${TOTAL_BYTES}") would move. Nothing was copied or deleted."
else
    echo "    verified ${TOTAL_VERIFIED} file(s), $(human "${TOTAL_BYTES}") into ${TARGET}"
    echo "    removed ${TOTAL_DELETED} file(s) on ${HOST}"
    [[ "${TOTAL_SKIPPED}" -gt 0 ]] && echo "    left ${TOTAL_SKIPPED} verified file(s) on the host (--keep-on-host)"
    echo "    free on the host now:"
    ssh -n "${HOST}" "df -h '${REMOTE_DATA_ROOT}' | tail -n +2" 2>/dev/null | sed 's/^/      /' || true
fi

if [[ "${FAILURES}" -gt 0 ]]; then
    echo ""
    echo "ERROR: ${FAILURES} instance(s) had a problem; see above. Nothing unverified was deleted." >&2
    exit 4
fi
