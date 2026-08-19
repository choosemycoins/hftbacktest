#!/usr/bin/env bash
# Heartbeat: «всё норм» по коллекторам маркетдаты и поллерам, плюс детектор тишины.
#
# Зачем отдельно от OnFailure=. OnFailure ловит юнит, который УПАЛ. Он не ловит юнит,
# который жив и ничего не делает, и не ловит таймер, который перестал отстреливать.
# Измерено 2026-08-12: params-poller.timer молча не срабатывал 23 часа
# (`Trigger: n/a` после рестарта), юнит числился enabled, ни одного алерта не пришло,
# и поймал это человек случайным вопросом. Это тот класс отказа, который виден только
# по ОТСУТСТВИЮ свежих записей.
#
# Почему heartbeat, а не только тревоги. Если сообщения приходят только на проблему,
# тишина двусмысленна: либо всё хорошо, либо сломался сам сторож. Периодическое
# «всё норм» делает тишину однозначной — её отсутствие и есть тревога.
# Ровно та же логика, по которой поллеры пишут ПУЛЬС, а не только изменения.
#
# Дёшево по построению: смотрим ТОЛЬКО mtime новейшего файла (`find -newermt … -quit`,
# выход на первом попадании). Никакой распаковки. Хост сбора не должен нести нагрузку.
#
# Запускается СИСТЕМНЫМ юнитом от root — иначе не прочитать alert.env (root:600).
# Каталоги поллеров живут в домашнем каталоге сервисного пользователя, поэтому путь
# к ним задаётся явно (POLLER_HOME), а не через $HOME: у root он другой.
#
# Запуск:  heartbeat.sh [--dry-run]
# Конфиг:  /opt/hft-collector/etc/alert.env  (тот же, что у alert.sh)
#            TG_BOT_TOKEN, TG_CHAT_ID
#            HEARTBEAT_DIGEST_HOUR   час UTC для ежедневного «всё норм» (по умолчанию 9)
#            HEARTBEAT_STALE_RATE_S  не чаще одной тревоги в N секунд (по умолчанию 3600)

set -uo pipefail

DRY_RUN=0
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN=1

ENV_FILE=/opt/hft-collector/etc/alert.env
DATA_ROOT=/opt/hft-collector/data
ETC_DIR=/opt/hft-collector/etc
STAMP=/run/hft-collector-heartbeat.stamp
POLLER_HOME="${POLLER_HOME:-/home/ubuntu}"
HOST="$(hostname -s)"

# Порог свежести в МИНУТАХ по классам целей.
COLLECTOR_STALE_MIN=10   # пишут непрерывно; 10 мин — это уже точно не «тонкий символ»
POLLER_STALE_MIN=90      # часовой таймер + RandomizedDelaySec до 4 мин + запас

if [[ -r "${ENV_FILE}" ]]; then
    # shellcheck disable=SC1090
    source "${ENV_FILE}"
fi
DIGEST_HOUR="${HEARTBEAT_DIGEST_HOUR:-9}"
RATE_S="${HEARTBEAT_STALE_RATE_S:-3600}"

# --- гвард переполнения диска (задача #56) ------------------------------------
# Рантайм-пол в самом коллекторе есть (check_disk каждый минутный тик, чистая
# остановка с записью disk_exhausted в _meta), но он срабатывает у САМОГО пола,
# когда сбор уже умирает. Здесь — предупреждение ЗАРАНЕЕ, потому что измеренная
# 2026-08-19 цепочка отказа целиком тихая: машина оператора выключена → офлоад
# не забирает → диск заполняется ~10 ГБ/сутки → у пола останавливаются все
# инстансы разом. От 47 ГБ свободного до пола — четверо суток тишины.
# 15 ГБ ≈ полтора суток запаса: хватает проспать ночь и починить утром.
DISK_WARN_GB="${HEARTBEAT_DISK_WARN_GB:-15}"
DISK_CRIT_GB="${HEARTBEAT_DISK_CRIT_GB:-8}"
DISK_RATE_S="${HEARTBEAT_DISK_RATE_S:-21600}"   # warn: не чаще раза в 6 часов
DISK_STAMP=/run/hft-collector-heartbeat-disk.stamp
# Дайджест «всё норм» — не чаще раза в СУТКИ. Прежнее условие «час == 9» без
# штампа отправляло его дважды: таймер стреляет в :00 и :30, и оба прогона
# попадают в девятый час. Штамп хранит дату последней доставки.
DIGEST_STAMP=/run/hft-collector-heartbeat-digest.stamp

# --- проверка одной цели: есть ли файл свежее порога --------------------------
# Печатает "OK <имя> <возраст_мин>" либо "STALE <имя> <возраст_мин|nofiles>".
check_dir() {
    local name="$1" dir="$2" limit_min="$3"
    if [[ ! -d "${dir}" ]]; then
        echo "STALE ${name} nodir"; return
    fi
    if find "${dir}" -maxdepth 2 -name '*.gz' -newermt "-${limit_min} minutes" -print -quit \
        2>/dev/null | grep -q .; then
        echo "OK ${name}"
    else
        # возраст новейшего файла — только для текста сообщения, полный обход
        # каталога здесь допустим лишь потому, что мы уже знаем: свежих нет
        local newest age
        newest="$(find "${dir}" -maxdepth 2 -name '*.gz' -printf '%T@\n' 2>/dev/null \
                  | sort -rn | head -1)"
        if [[ -z "${newest}" ]]; then echo "STALE ${name} nofiles"; return; fi
        age=$(( ( $(date +%s) - ${newest%.*} ) / 60 ))
        echo "STALE ${name} ${age}m"
    fi
}

results=()

# коллекторы: по одному инстансу на <etc>/<name>.env (бэкапы .bak-* пропускаем)
for env_path in "${ETC_DIR}"/*.env; do
    [[ -e "${env_path}" ]] || continue
    inst="$(basename "${env_path}" .env)"
    [[ "${inst}" == "alert" ]] && continue
    results+=("$(check_dir "${inst}" "${DATA_ROOT}/${inst}" "${COLLECTOR_STALE_MIN}")")
done

# поллеры: пишут в домашний каталог сервисного пользователя
for p in "params:${POLLER_HOME}/params-data" "positions:${POLLER_HOME}/positions-data"; do
    results+=("$(check_dir "${p%%:*}" "${p#*:}" "${POLLER_STALE_MIN}")")
done

stale=(); ok=()
for r in "${results[@]}"; do
    read -r verdict name detail <<<"${r}"
    if [[ "${verdict}" == "OK" ]]; then ok+=("${name}"); else stale+=("${name}(${detail:-?})"); fi
done

now_hour="$(date -u +%-H)"
send=""
if (( ${#stale[@]} > 0 )); then
    send="🔴 ${HOST}: НЕТ СВЕЖИХ ЗАПИСЕЙ — ${stale[*]}"$'\n'"живы (${#ok[@]}): ${ok[*]}"
    # рейт-лимит только на тревогу; дайджест «всё норм» ходит по расписанию
    if [[ -f "${STAMP}" ]]; then
        last=$(cat "${STAMP}" 2>/dev/null || echo 0)
        (( $(date +%s) - last < RATE_S )) && send=""
    fi
    [[ -n "${send}" ]] && date +%s > "${STAMP}"
elif [[ "${now_hour}" == "${DIGEST_HOUR}" ]]; then
    # Раз в сутки, а не раз в прогон девятого часа: см. комментарий у DIGEST_STAMP.
    if [[ "$(cat "${DIGEST_STAMP}" 2>/dev/null)" != "$(date -u +%Y%m%d)" ]]; then
        digest_free="$(df -BG --output=avail "${DATA_ROOT}" 2>/dev/null | tail -1 | tr -d '[:space:]')"
        send="🟢 ${HOST}: всё норм — ${#ok[@]} источников пишут свежее, диск: ${digest_free:-?} свободно"$'\n'"${ok[*]}"
    fi
fi

# --- доставка ------------------------------------------------------------------
# Возвращает 0 только при подтверждённой доставке. Успех обязан оставлять след в
# журнале: без него «отправил» и «не запускался» неразличимы — ровно так дубль
# дайджеста жил незамеченным с 12.08 (в час дайджеста журнал был просто пуст).
send_tg() {
    local text="$1"
    if (( DRY_RUN )); then
        echo "--- DRY RUN, отправлено было бы: ---"; echo "${text}"; return 0
    fi
    if [[ -z "${TG_BOT_TOKEN:-}" || -z "${TG_CHAT_ID:-}" ]]; then
        echo "heartbeat: TG не настроен; сообщение только в журнал:" >&2
        echo "${text}" >&2
        return 1
    fi
    local attempt
    for attempt in 1 2 3; do
        if curl -sS -m 10 -o /dev/null \
            --data-urlencode "chat_id=${TG_CHAT_ID}" \
            --data-urlencode "text=${text}" \
            "https://api.telegram.org/bot${TG_BOT_TOKEN}/sendMessage"; then
            echo "heartbeat: отправлено: $(head -1 <<<"${text}")"
            return 0
        fi
        # След неудачной попытки обязателен: ретрай после таймаута, когда первый
        # запрос на самом деле ДОШЁЛ, — единственный наш механизм дубля в TG, и
        # без этой строки он неотличим от одиночной доставки (разбор 2026-08-19).
        echo "heartbeat: попытка ${attempt} не доставлена, повтор через 5 с" >&2
        sleep 5
    done
    # Недоставленный алерт остаётся в журнале. Ненулевой код процесса запустил бы
    # OnFailure самого сторожа — петля; поэтому наружу всегда выходим нулём.
    echo "heartbeat: доставка не удалась; текст остаётся в журнале:" >&2
    echo "${text}" >&2
    return 1
}

sent_something=0
if [[ -n "${send}" ]]; then
    send_tg "${send}" && sent_something=1
    # Штамп дайджеста пишется ТОЛЬКО после подтверждённой доставки и только вне
    # dry-run: упавший Telegram не должен съесть дайджест на сутки — следующий
    # получас довезёт.
    if [[ "${send}" == 🟢* ]] && (( sent_something )) && (( ! DRY_RUN )); then
        date -u +%Y%m%d > "${DIGEST_STAMP}"
    fi
fi

# --- гвард диска: считается ПОСЛЕ свежести, шлётся отдельным сообщением --------
free_bytes="$(df -B1 --output=avail "${DATA_ROOT}" 2>/dev/null | tail -1 | tr -d '[:space:]')"
if [[ "${free_bytes}" =~ ^[0-9]+$ ]]; then
    free_gb=$(( free_bytes / 1024 / 1024 / 1024 ))
    level=0
    (( free_gb < DISK_WARN_GB )) && level=1
    (( free_gb < DISK_CRIT_GB )) && level=2
    if (( level > 0 )); then
        last_level=0; last_epoch=0
        if [[ -f "${DISK_STAMP}" ]]; then
            read -r last_level last_epoch < "${DISK_STAMP}" 2>/dev/null || true
            [[ "${last_level}" =~ ^[0-9]+$ ]] || last_level=0
            [[ "${last_epoch}" =~ ^[0-9]+$ ]] || last_epoch=0
        fi
        rate="${DISK_RATE_S}"; (( level == 2 )) && rate="${RATE_S}"
        # Эскалация warn → crit пробивает рейт-лимит: ухудшение — новое событие.
        if (( level > last_level || $(date +%s) - last_epoch >= rate )); then
            if (( level == 2 )); then
                disk_msg="🔴 ${HOST}: ДИСК СБОРА ПОЧТИ ПОЛОН — свободно ${free_gb} ГБ (крит. порог ${DISK_CRIT_GB})."
            else
                disk_msg="🟡 ${HOST}: диск сбора заканчивается — свободно ${free_gb} ГБ (порог ${DISK_WARN_GB})."
            fi
            disk_msg+=$'\n'"У пола 5 ГБ все инстансы остановятся разом. Обычная причина: офлоад не забирает данные (машина оператора выключена или не в сети)."
            send_tg "${disk_msg}" && sent_something=1
            (( ! DRY_RUN )) && echo "${level} $(date +%s)" > "${DISK_STAMP}"
        fi
    else
        # Здоровый диск снимает штамп: следующая деградация снова тревожит сразу.
        rm -f "${DISK_STAMP}"
    fi
else
    echo "heartbeat: не смог прочитать свободное место в ${DATA_ROOT}" >&2
fi

if (( ! sent_something )); then
    echo "heartbeat: ok=${#ok[@]} stale=${#stale[@]} disk=${free_gb:-?}ГБ; сообщение не отправляется"
fi
exit 0
