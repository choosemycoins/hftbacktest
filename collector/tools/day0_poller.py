#!/usr/bin/env python3
"""Поллер листингов дня-0 Binance USDⓈ-M: ловит новые символы и заводит их в захват.

Зачем. Вердикт дня-0 (19.08) держится на ОДНОМ листинг-событии: клетка жива по
замороженной планке, но возраст символа коррелирует с настенным временем на 0.9936 —
разделить «край дня-0» и «край того дня» нечем. Снять неответ может только вторая
когорта, а листинги приходят волнами раз в ~2.1 суток БЕЗ предупреждения, и самое
дорогое в них — первые минуты (вал 0–5 мин +13.16 бп). Машина оператора измеренно
бывает выключена сутками, поэтому ручной режим не годится: поллер живёт на хосте
сбора и работает без человека.

Что он делает раз в 10 минут: читает публичный exchangeInfo, сравнивает с известным
состоянием, дописывает новые символы в env выделенного инстанса захвата, ставит на
момент старта торгов transient-таймер `systemctl restart`, шлёт оператору один TG-алерт
на волну и выводит отжившие 14 суток символы. Механика повторяет ручную шестёрку 14.08,
которая и дала вердикт, — она не изобретается заново.

Три вещи, которые здесь легко сделать неправильно (все три измерены, не выведены):

1. ДЕТЕКТОР — это членство ключа (symbol, onboardMs) в известном состоянии, а НЕ
   «onboardDate в будущем». В живом снапшоте 2026-08-19 символов с будущей датой было
   НОЛЬ, а единственный PENDING_TRADING (GAIBUSDT) нёс дату на 272 суток В ПРОШЛОМ.
   Поллер на дате не увидел бы ничего и молчал бы вечно.
2. ЧАСЫ — только локальные. `exchangeInfo.serverTime` стух на 87 минут и на шести
   пробах даже УБЫВАЛ, при верных локальных часах (проверено по /fapi/v1/time и
   стороннему Date). serverTime только логируется — ради измерения задержки самого
   документа, которая пока неизвестна.
3. СТОРОЖ ТИШИНЫ инстанса — 1080 минут, а не дефолтные 5. Подписка на ещё не
   торгующийся символ площадкой ПРИНИМАЕТСЯ (подтверждено WS-пробой), но не даёт ни
   одного кадра; сторож убивал шестёрку каждые 305 с — 10 падений за 45 минут. Тот же
   порог кроет сессионные TRADIFI-эквити (34 из 36 недавних листингов), которые молчат
   всю ночь биржи-источника.

Устройство. Все решения — чистые функции над словарём exchangeInfo и состоянием;
`plan_tick` возвращает `Plan` ДАННЫМИ, а `apply_plan` их исполняет через тонкий слой
`Host`, подменяемый в тестах. Идемпотентность обязательна: повторный тик над неизменным
миром не пишет env и не рестартит инстанс, потому что каждый рестарт рвёт запись живым
символам ровно в самом дорогом окне. Считается она по НАБЛЮДЁННЫМ эффектам, а не по
намерениям: в состоянии живёт журнал (`Entry.subscribed`, `Entry.timer_armed`), который
пишет только исполнитель и только по нулевому коду возврата systemd. Без него обрыв
между записью env и рестартом (или молча отвергнутый рестарт) давал символ, который
числится в захвате, лежит в COLLECTOR_SYMBOLS и не имеет ни одной подписки — а сторож
тишины инстанса 1080 мин по построению не заметил бы этого 18 часов. Fail-closed: любой
провал чтения оставляет мир нетронутым, серия провалов алертит, неисполненный эффект
алертит и переносится на следующий тик (прецедент: params-poller молчал 23 часа).

Дизайн и обоснования решений: scratchpad DESIGN.md задачи #74 (заморожен 19.08).

Запуск: day0_poller.py --once --data-dir /home/ubuntu/day0-data [--dry-run]
"""

from __future__ import annotations

import argparse
import dataclasses
import gzip
import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Protocol

# --- источник ----------------------------------------------------------------
EXCHANGE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
USER_AGENT = "hft-collector-day0/1.0"
# Измерено: ответ 0.41–1.92 с. Таймаут щедрый — тик редкий, и лучше дождаться, чем
# записать провал на медленном ответе.
HTTP_TIMEOUT_S = 30
# «Валидный» JSON на три символа — это сломанное происхождение, а не мир (в норме 872).
# Без пола усечённый ответ прочитался бы как массовый делистинг и вычистил бы env.
UNIVERSE_FLOOR = 100

# --- окна и пороги -----------------------------------------------------------
DAY_MS = 86_400_000
# Затухание края завершено за 10–12 суток по измеренной λ; хвост до 14 ещё измерим.
ROTATION_DAYS = 14
# Символ захватоспособен ⇔ он не был бы немедленно ротирован. ОДНА константа вместо
# двух убирает класс рассинхронов «захватили и тут же выкинули».
CAPTURE_MAX_AGE_DAYS = ROTATION_DAYS
# Арифметика запаса: stop инстанса до 30 с (TimeoutStopSec) + старт с подпиской ~5 с ⇒
# подписка стоит не позже чем за ~85 с до первой сделки.
TIMER_LEAD_S = 120
# ~1 час тишины. Ниже порога молчим: мигание сети не повод будить оператора.
FAIL_ALERT_AFTER = 6
# ~6 часов. Пока провалы длятся — напоминание, а не шторм (паттерн DISK_RATE_S heartbeat).
FAIL_REALERT_EVERY = 36

# --- контракты хоста (литералы ровно в одном месте) --------------------------
ENV_PATH = "/opt/hft-collector/etc/binancefuturesum-day0.env"
INSTANCE_UNIT = "hft-collector@binancefuturesum-day0.service"
TIMER_PREFIX = "hft-day0-restart-"
ALERT_ENV_PATH = "/opt/hft-collector/etc/alert.env"
DEFAULT_DATA_DIR = "/home/ubuntu/day0-data"
COLLECTOR_EXCHANGE = "binancefuturesum"
# Оба сторожа инстанса — см. пункт 3 шапки. Меняешь одно — меняй и второе.
STALL_TIMEOUT_MIN = 1080
LIVENESS_TIMEOUT_S = 64800

# --- словарь площадки --------------------------------------------------------
STATUS_TRADING = "TRADING"
STATUS_PENDING = "PENDING_TRADING"
# Живыми наблюдались ровно три статуса; документация знает больше (PRE_DELIVERING,
# DELIVERING, CLOSE…). Незнакомый читается fail-closed: не листинг.
LIVE_STATUSES = frozenset({STATUS_TRADING, STATUS_PENDING})
# Перевыпуск квартальной серии (BTCUSDT_261225 и пара к нему) — не день-0.
QUARTERLY_TYPES = frozenset({"CURRENT_QUARTER", "NEXT_QUARTER"})
TRADIFI_TYPE = "TRADIFI_PERPETUAL"

# --- фазы записи состояния ---------------------------------------------------
PHASE_REJECTED = "rejected"      # объявленная отбраковка; терминальна
PHASE_SEEDED = "seeded"          # мир как он есть; никаких действий никогда
PHASE_WATCH = "watch"            # PENDING, таймер поставить некуда — берём сейчас
PHASE_ARMED = "armed"            # PENDING с датой в будущем — таймер на onboard−2мин
PHASE_CAPTURING = "capturing"    # торгуется и моложе окна — пишем
PHASE_SHELVED = "shelved"        # watch, не начавший торговать за окно; флип всё ещё ловим
PHASE_ROTATED = "rotated"        # отработал своё, выведен из env
PHASE_SUPERSEDED = "superseded"  # листинг перенесён: ключ заменён новым

IN_ENV_PHASES = frozenset({PHASE_WATCH, PHASE_ARMED, PHASE_CAPTURING})
SCHEMA = 1


class Day0Refusal(RuntimeError):
    """Поллер отказался делать то, чего ему делать нельзя. Всегда баг, всегда громко."""


class Day0FetchError(RuntimeError):
    """Ответ площадки нечитаем или неправдоподобен — тик считается провалом целиком."""


# ==============================================================================
# Чистые функции: разбор
# ==============================================================================


def iso_ms(ms: int) -> str:
    return datetime.fromtimestamp(ms // 1000, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def on_calendar_utc(ms: int) -> str:
    """Форма OnCalendar для systemd-run. UTC явно: у хоста может быть любая зона."""
    return datetime.fromtimestamp(ms // 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def entry_key(symbol: str, onboard_ms: int) -> str:
    """Ключ состояния — ПАРА. По голому тикеру релистинг того же имени с новым
    onboardDate был бы неотличим от старой записи, и день-0 прошёл бы мимо."""
    return f"{symbol}@{onboard_ms}"


def timer_unit_name(symbol: str, onboard_ms: int) -> str:
    """Детерминизм имени и есть идемпотентность: повторная постановка того же таймера
    после обрыва читается systemd как «already exists», то есть как успех."""
    return f"{TIMER_PREFIX}{symbol}-{onboard_ms}"


def is_tradifi(raw: Any) -> bool:
    """TRADIFI по contractType, с перекрёстной проверкой по underlyingSubType.

    ТОТАЛЬНАЯ. `underlyingSubType` бывает пустым списком (34 символа из 872) и может
    отсутствовать вовсе — `sub[0]` уронил бы поллер на них. Признак
    «underlyingType != COIN» ЛОЖЕН в обе стороны: ANTHROPICUSDT — TRADIFI при INDEX,
    BTCDOMUSDT — обычный PERPETUAL при INDEX.
    """
    if not isinstance(raw, dict):
        return False
    if raw.get("contractType") == TRADIFI_TYPE:
        return True
    sub = raw.get("underlyingSubType")
    return isinstance(sub, list) and "TradFi" in sub


def parse_snapshot(payload: bytes) -> dict:
    """Байты ответа → документ. Любая неправдоподобность — провал, а не мир."""
    try:
        doc = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise Day0FetchError(f"нечитаемый ответ: {type(exc).__name__}") from exc
    if not isinstance(doc, dict):
        raise Day0FetchError("ответ не объект")
    symbols = doc.get("symbols")
    if not isinstance(symbols, list):
        raise Day0FetchError("в ответе нет массива symbols")
    if len(symbols) < UNIVERSE_FLOOR:
        # Пустой/усечённый ответ, прочитанный как мир, вычистил бы COLLECTOR_SYMBOLS,
        # а collector-run.sh на пустом списке выходит с кодом 4 и роняет юнит.
        raise Day0FetchError(f"вселенная {len(symbols)} < {UNIVERSE_FLOOR}")
    # Пол на сырой длине НЕ заменяет разбора: переименованное/переехавшее по типу поле
    # (ровно то, чем схема площадки и меняется) давало бы universe=0 при len=872, и тик
    # записывался бы успехом — оба канала тревоги молчат, детектор слеп навсегда
    # (доказано мутацией П1). Одна битая запись не мягче: ей может оказаться САМ новый
    # листинг (П1b: onboardDate=null у него одного), а каждый пропущенный невосполним.
    # Поэтому битая запись = провал тика целиком: мир не тронут, серия провалов копится
    # до штатного алерта, тишина пульса будит heartbeat. Живой снапшот 2026-08-19
    # (872 записи) разбирается без единой битой — строгость не стоит ложных провалов.
    malformed = sum(1 for raw in symbols if _fields(raw) is None)
    if malformed:
        raise Day0FetchError(
            f"битых записей {malformed} из {len(symbols)} — схема площадки уехала?"
        )
    return doc


def _fields(raw: Any) -> tuple | None:
    """Поля одной записи symbols. None — запись непригодна (считается счётчиком)."""
    if not isinstance(raw, dict):
        return None
    symbol = raw.get("symbol")
    status = raw.get("status")
    contract_type = raw.get("contractType")
    onboard = raw.get("onboardDate")
    if not isinstance(symbol, str) or not symbol or not isinstance(status, str):
        return None
    if not isinstance(contract_type, str) or isinstance(onboard, bool):
        return None
    try:
        onboard_ms = int(onboard)
    except (TypeError, ValueError):
        return None
    quote = raw.get("quoteAsset") if isinstance(raw.get("quoteAsset"), str) else "?"
    utype = raw.get("underlyingType") if isinstance(raw.get("underlyingType"), str) else "?"
    return symbol, onboard_ms, status, contract_type, quote, utype, is_tradifi(raw)


# ==============================================================================
# Состояние
# ==============================================================================


@dataclass
class Entry:
    """Одна запись состояния: что мы знаем про пару (symbol, onboardMs)."""

    symbol: str
    onboard_ms: int
    phase: str
    status_last_seen: str
    contract_type: str
    quote: str
    underlying_type: str
    tradifi: bool
    first_seen_local_ms: int
    # Оба времени первого наблюдения — это измерение задержки САМОГО документа
    # (serverTime стух на 87 мин; отстаёт ли вместе с ним содержимое — не измерено).
    first_seen_server_ms: int | None
    # Точка отсчёта свежести и ротации: onboardDate, если он был правдоподобен, и
    # время наблюдённого флипа — для пути через watch (там дата измеренно недостоверна).
    anchor_ms: int | None
    in_env: bool
    timer_unit: str | None
    phase_changed_local_ms: int
    dead: bool = False
    # ЖУРНАЛ СОБСТВЕННЫХ ЭФФЕКТОВ — то, что поллер НАБЛЮДАЛ, а не то, что намеревался.
    # `subscribed`: инстанс был успешно рестартован в момент, когда символ уже лежал в
    # env (или это сделал его подтверждённый таймер). Пока флага нет, символ числится
    # в COLLECTOR_SYMBOLS, но подписки может не быть вовсе, а сторож тишины инстанса
    # (1080 мин) этого по построению не заметит 18 часов.
    # `timer_armed`: `systemd-run` вернул ноль. Несостоявшаяся постановка иначе
    # неотличима от состоявшейся, а R-QUIET на флипе считает, что рестарт уже был.
    # Оба сбрасываются в False по умолчанию: утрата состояния обязана читаться как
    # «не наблюдали», а не как «всё уже сделано» — цена лишнего рестарта ≤35 с записи,
    # цена пропущенной переподписки — вся волна (следующая через ~2.1 суток).
    subscribed: bool = False
    timer_armed: bool = False


@dataclass
class State:
    known: dict[str, Entry] = field(default_factory=dict)
    consecutive_failures: int = 0
    failure_alerted: bool = False
    last_success_local_ms: int | None = None
    last_failure_reason: str = ""
    # Подряд идущие тики, чьи эффекты (рестарт/таймер) вернули ненулевой код. Живёт в
    # состоянии по той же причине, что и счётчик провалов чтения: тик — oneshot.
    effect_failures: int = 0


def load_state(text: str | None) -> State:
    """Битое или отсутствующее состояние читается как «ничего не знаем».

    Прецедент positions-поллера. Потеря состояния реальна без единого бага: state.json
    переписывается каждые 10 минут без fsync (жёсткий ребут даёт нулевую длину или
    мусор), а read_file глушит OSError. Плата за re-bootstrap мала НЕ сама по себе, а
    благодаря спасению по env в `_classify_new`: env-файл — второй, независимый носитель
    факта «мы это ловим», и символ из него не понижается в seeded, даже когда его
    onboardDate старше окна (класс GAIBUSDT: якорь — наблюдённый флип — утрачен вместе
    с состоянием). Поэтому env пересобирается из тех же символов, совпадает по байтам —
    ни записи, ни рестарта; продублируется максимум один алерт.
    """
    if not text:
        return State()
    try:
        doc = json.loads(text)
        if not isinstance(doc, dict) or doc.get("schema") != SCHEMA:
            return State()
        fields = {f.name for f in dataclasses.fields(Entry)}
        known = {
            k: Entry(**{n: v for n, v in raw.items() if n in fields})
            for k, raw in doc.get("known", {}).items()
            if isinstance(raw, dict)
        }
        return State(
            known=known,
            consecutive_failures=int(doc.get("consecutive_failures", 0)),
            failure_alerted=bool(doc.get("failure_alerted", False)),
            last_success_local_ms=doc.get("last_success_local_ms"),
            last_failure_reason=str(doc.get("last_failure_reason", "")),
            effect_failures=int(doc.get("effect_failures", 0)),
        )
    except (json.JSONDecodeError, TypeError, ValueError):
        return State()


def dump_state(state: State) -> str:
    return json.dumps(
        {
            "schema": SCHEMA,
            "known": {k: dataclasses.asdict(e) for k, e in sorted(state.known.items())},
            "consecutive_failures": state.consecutive_failures,
            "failure_alerted": state.failure_alerted,
            "last_success_local_ms": state.last_success_local_ms,
            "last_failure_reason": state.last_failure_reason,
            "effect_failures": state.effect_failures,
        },
        ensure_ascii=False,
        indent=1,
    )


# ==============================================================================
# Чистые функции: env
# ==============================================================================


ENV_HEADER = "# Сгенерировано day0_poller — не редактировать руками: перепишется следующей волной."


def env_text(symbols: list[str]) -> str:
    """Байты env инстанса захвата. Заголовок статичен (без времени генерации) — иначе
    каждый тик отличался бы от предыдущего и идемпотентность по байтам была бы ложью."""
    if not symbols:
        # Пояс безопасности: пустой список структурно недостижим (волна добавляет ≥1),
        # но если он всё же собран — это баг, а цена ошибки известна: exit 4 и падение
        # инстанса. Лучше не тронуть env вообще.
        raise Day0Refusal("COLLECTOR_SYMBOLS оказался бы пустым")
    return "\n".join([
        ENV_HEADER,
        f"COLLECTOR_EXCHANGE={COLLECTOR_EXCHANGE}",
        "COLLECTOR_SYMBOLS=" + " ".join(symbols),
        "RUST_LOG=info",
        "# Сессионные TRADIFI (34 из 36 листингов за 30 суток) молчат вне сессии",
        "# биржи-источника, и ровно так же молчит ЛЮБОЙ ещё не начавший торги символ:",
        "# подписка принимается, кадров нет. Дефолтный сторож 5 мин убивал шестёрку",
        "# каждые 305 с (измерено 14.08). 1080 минут кроет оба случая.",
        f"COLLECTOR_STALL_TIMEOUT_MIN={STALL_TIMEOUT_MIN}",
        f"COLLECTOR_LIVENESS_TIMEOUT_S={LIVENESS_TIMEOUT_S}",
        "",
    ])


def env_symbols_of(text: str | None) -> frozenset[str]:
    """Символы из COLLECTOR_SYMBOLS текущего env-файла. Терпимая к любому мусору:
    env — вторичный носитель, и нечитаемость не должна ронять тик."""
    if not text:
        return frozenset()
    for line in text.splitlines():
        if line.startswith("COLLECTOR_SYMBOLS="):
            return frozenset(line.removeprefix("COLLECTOR_SYMBOLS=").split())
    return frozenset()


# ==============================================================================
# План
# ==============================================================================


@dataclass(frozen=True)
class Plan:
    """Решения тика как ДАННЫЕ. Исполняет их `apply_plan`, и только он."""

    cancel_timers: list[str] = field(default_factory=list)
    env_write: tuple[str, str] | None = None          # (путь, содержимое)
    arm_timers: list[tuple[str, str]] = field(default_factory=list)  # (unit, on-calendar UTC)
    restart_now: bool = False                          # не более одного за тик
    alerts: list[str] = field(default_factory=list)
    counters: dict[str, int] = field(default_factory=dict)
    # Пульс пишется ТОЛЬКО на успешном тике: его отсутствие и есть второй, независимый
    # от Telegram канал тревоги (heartbeat увидит STALE через 90 минут).
    pulse: str | None = None


def _new_counters() -> dict[str, int]:
    """Отбраковка объявляется всегда, даже нулевая: молчаливый пропуск — это то, чего
    в этом поллере быть не должно ни в одном классе."""
    return {
        "universe": 0, "malformed_entry": 0, "rejected_quarterly": 0,
        "seeded_stale": 0, "seeded_unknown_status": 0,
        "wave": 0, "armed": 0, "watch": 0, "capturing": 0,
        "superseded": 0, "rotated": 0, "shelved": 0, "in_env": 0,
        "env_rescued": 0,
    }


def plan_tick(
    snapshot: dict,
    state: State,
    now_ms: int,
    *,
    env_now: str | None = None,
    hostname: str = "",
) -> tuple[Plan, State]:
    """Один тик: мир + состояние + локальные часы → (план, новое состояние).

    Чистая. `env_now` — текущее содержимое env, прочитанное вызывающим: сравнение по
    байтам и есть механизм идемпотентности, и держать его здесь честнее, чем прятать
    решение «писать или нет» в исполняющем слое.
    """
    known = {k: replace(e) for k, e in state.known.items()}
    counters = _new_counters()
    server_ms = snapshot.get("serverTime")
    server_ms = int(server_ms) if isinstance(server_ms, int) else None

    wave: list[str] = []      # ключи, ставшие в этом тике watch/armed/capturing
    flips: list[str] = []     # из них — наблюдённые PENDING→TRADING (R-FLIP)
    cancel: list[str] = []

    # env — второй, НЕЗАВИСИМЫЙ носитель факта «мы это ловим». Нужен, потому что
    # state.json переписывается каждые 10 минут без fsync и после жёсткого ребута
    # бывает пустым/битым, а load_state читает это как «ничего не знаем». Без него
    # флипнувший через watch символ (класс GAIBUSDT: onboardDate на 272 суток в
    # прошлом, якорь ротации — НАБЛЮДЁННЫЙ флип) переклассифицировался бы по своей
    # дате в «старьё» и молча вылетал из захвата — навсегда: следующий тик уже видит
    # его известным seeded. Пин: test_state_loss_never_drops_a_flipped_watch_symbol…
    env_carried = env_symbols_of(env_now)

    # Ключи, которые ещё могут быть перенесены (R-MOVE) — только они.
    open_by_symbol: dict[str, list[str]] = {}
    for k, e in known.items():
        if e.phase in (PHASE_ARMED, PHASE_WATCH):
            open_by_symbol.setdefault(e.symbol, []).append(k)

    for raw in snapshot.get("symbols", []):
        parsed = _fields(raw)
        if parsed is None:
            # Пояс безопасности: через tick() сюда не попасть — parse_snapshot читает
            # любую битую запись как провал тика (П1/П1b). Счётчик остаётся, чтобы
            # plan_tick был тотальным для прямых вызовов, но тревога живёт НЕ здесь.
            counters["malformed_entry"] += 1
            continue
        symbol, onboard_ms, status, contract_type, quote, utype, tradifi = parsed
        counters["universe"] += 1
        key = entry_key(symbol, onboard_ms)

        known_entry = known.get(key)
        if known_entry is not None:
            _advance(known_entry, key, status, now_ms, wave, flips)
            continue

        # R-MOVE: у известного ожидающего символа изменился onboardDate. Его таймер
        # выстрелил бы в пустоту и рванул бы запись живым символам — снимаем.
        for old_key in open_by_symbol.get(symbol, []):
            if old_key == key:
                continue
            old = known[old_key]
            if old.timer_unit:
                cancel.append(old.timer_unit)
            old.phase = PHASE_SUPERSEDED
            old.in_env = False
            old.timer_unit = None
            old.phase_changed_local_ms = now_ms
            counters["superseded"] += 1

        # R-NEW: символа нет в известном состоянии — это и есть первичный детектор.
        entry = _classify_new(
            symbol=symbol, onboard_ms=onboard_ms, status=status, contract_type=contract_type,
            quote=quote, utype=utype, tradifi=tradifi, now_ms=now_ms, server_ms=server_ms,
            counters=counters, in_env_now=symbol in env_carried,
        )
        known[key] = entry
        if entry.phase in IN_ENV_PHASES:
            wave.append(key)
            counters[entry.phase] += 1

    counters["wave"] = len(wave)
    new_state = State(
        known=known,
        consecutive_failures=0,
        failure_alerted=False,
        last_success_local_ms=now_ms,
        last_failure_reason="",
    )
    alerts: list[str] = []
    if state.failure_alerted:
        alerts.append(
            f"🟢 {hostname}: day0-поллер восстановился после "
            f"{state.consecutive_failures} провалов подряд"
        )

    if not wave:
        # Между волнами env не трогается вообще: ротация «сама по себе» была бы
        # невидимой мутацией конфига живого инстанса. Но НЕЗАКРЫТЫЙ долг по эффектам
        # (рестарт не состоялся, таймер не встал) добирается и здесь — иначе провал
        # ждал бы следующей волны, то есть ~2.1 суток.
        counters["in_env"] = sum(1 for e in known.values() if e.in_env)
        return (
            Plan(cancel_timers=cancel, arm_timers=_arm_timers(known, now_ms),
                 restart_now=_restart_owed(known), alerts=alerts, counters=counters,
                 pulse=_pulse(now_ms, server_ms, known, counters)),
            new_state,
        )

    rotated, shelved = _rotate(known, wave, now_ms, counters)
    symbols = [
        e.symbol
        for e in sorted((e for e in known.values() if e.in_env),
                        key=lambda e: (e.onboard_ms, e.symbol))
    ]
    try:
        content = env_text(symbols)
    except Day0Refusal as exc:
        # Состояние НЕ трогаем: следующий тик увидит ту же волну и попробует снова.
        return (
            Plan(alerts=[f"🔴 {hostname}: day0-поллер отказался писать env — {exc}"],
                 counters=counters),
            state,
        )
    counters["in_env"] = len(symbols)

    env_write = None if env_now == content else (ENV_PATH, content)
    arm = _arm_timers(known, now_ms)
    restart_now = _restart_owed(known)

    alerts.append(_wave_alert(hostname, known, wave, flips, rotated + shelved, now_ms, server_ms))
    return (
        Plan(cancel_timers=cancel, env_write=env_write, arm_timers=arm, restart_now=restart_now,
             alerts=alerts, counters=counters, pulse=_pulse(now_ms, server_ms, known, counters)),
        new_state,
    )


def _classify_new(*, symbol, onboard_ms, status, contract_type, quote, utype, tradifi,
                  now_ms, server_ms, counters, in_env_now: bool = False) -> Entry:
    if contract_type in QUARTERLY_TYPES:
        phase, anchor = PHASE_REJECTED, None
        counters["rejected_quarterly"] += 1
    elif status not in LIVE_STATUSES:
        phase, anchor = PHASE_SEEDED, None
        counters["seeded_unknown_status"] += 1
    elif status == STATUS_PENDING:
        # Дата в будущем и дальше упреждения — ставим таймер; иначе брать надо сейчас,
        # потому что таймер в прошлое поставить некуда (класс GAIBUSDT).
        if onboard_ms - TIMER_LEAD_S * 1000 > now_ms:
            phase, anchor = PHASE_ARMED, onboard_ms
        else:
            phase, anchor = PHASE_WATCH, None
    elif now_ms - onboard_ms > CAPTURE_MAX_AGE_DAYS * DAY_MS:
        if in_env_now:
            # СПАСЕНИЕ ПО ENV: символ старше окна по своей дате, но он уже стоит в
            # COLLECTOR_SYMBOLS — значит его дата была признана недостоверной раньше
            # (watch→флип), а состояние с истинным якорем утрачено. Понизить его в
            # seeded — молча оборвать живой захват без возврата. Якорь — от наблюдения,
            # как у R-FLIP: продлить захват максимум на окно ротации дешевле, чем
            # потерять листинг. Балласта вселенной это не касается — его в env нет.
            phase, anchor = PHASE_CAPTURING, now_ms
            counters["env_rescued"] += 1
        else:
            phase, anchor = PHASE_SEEDED, None
            counters["seeded_stale"] += 1
    else:
        phase, anchor = PHASE_CAPTURING, onboard_ms

    return Entry(
        symbol=symbol,
        onboard_ms=onboard_ms,
        phase=phase,
        status_last_seen=status,
        contract_type=contract_type,
        quote=quote,
        underlying_type=utype,
        tradifi=tradifi,
        first_seen_local_ms=now_ms,
        first_seen_server_ms=server_ms,
        anchor_ms=anchor,
        in_env=phase in IN_ENV_PHASES,
        timer_unit=timer_unit_name(symbol, onboard_ms) if phase == PHASE_ARMED else None,
        phase_changed_local_ms=now_ms,
    )


def _restart_owed(known: dict[str, Entry]) -> bool:
    """Рестарт причитается, пока в env лежит символ, которому запись нужна СЕЙЧАС, а
    подхват env инстансом ни разу не подтверждён нашим же наблюдённым рестартом.

    Считать по факту записи env было НЕЛЬЗЯ, и это измерено: тик, умерший между записью
    и рестартом, оставляет на диске env, который следующий тик находит совпавшим по
    байтам, — и рестарта не делает. Символ при этом числится capturing и лежит в
    COLLECTOR_SYMBOLS, но инстанс не переподписывался ни разу, кадров нет, а сторож
    тишины инстанса (1080 мин, и он такой не зря) молчит 18 часов. Чинилось это только
    следующей волной, то есть через ~2.1 суток — при том что вся ценность в первых часах.

    armed сюда не входит намеренно: его подписку подхватит рестарт собственного таймера,
    а лишний рестарт сейчас — это лишний разрыв записи живым символам.
    """
    return any(
        e.in_env and not e.subscribed and e.phase in (PHASE_WATCH, PHASE_CAPTURING)
        for e in known.values()
    )


def _arm_timers(known: dict[str, Entry], now_ms: int) -> list[tuple[str, str]]:
    """Таймеры, постановка которых ещё не подтверждена ненулевым кодом systemd-run.

    Повтор безопасен по построению: имя юнита детерминировано, и повторная постановка
    читается systemd как «already exists», то есть как успех (см. `systemd_run_timer`).
    Дата в прошлом не берётся вовсе — systemd-run туда либо стреляет немедленно, либо
    не стреляет никогда; этот случай ловит флип (`_advance`).
    """
    return [
        (e.timer_unit, on_calendar_utc(e.onboard_ms - TIMER_LEAD_S * 1000))
        for e in sorted(known.values(), key=lambda e: (e.onboard_ms, e.symbol))
        if e.phase == PHASE_ARMED and e.timer_unit and not e.timer_armed
        and e.onboard_ms - TIMER_LEAD_S * 1000 > now_ms
    ]


def _advance(e: Entry, key: str, status: str, now_ms: int,
             wave: list[str], flips: list[str]) -> None:
    """Переходы известного ключа. Мутирует копию записи, живущую в `known`."""
    e.status_last_seen = status
    if status == STATUS_TRADING and e.phase in (PHASE_WATCH, PHASE_SHELVED):
        # R-FLIP. Таймера у такого символа нет (его дата недостоверна), значит свежую
        # подписку после старта торгов обеспечивает сам флип.
        e.phase = PHASE_CAPTURING
        e.anchor_ms = now_ms          # часы ротации — от НАБЛЮДЁННОГО старта
        e.in_env = True
        e.dead = False
        e.timer_unit = None
        e.phase_changed_local_ms = now_ms
        # Подписка, сделанная ДО старта торгов, не считается: она принимается площадкой,
        # но кадров не даёт (урок day0-start). Долг закроет наблюдённый рестарт.
        e.subscribed = False
        wave.append(key)
        flips.append(key)
    elif status == STATUS_TRADING and e.phase == PHASE_ARMED:
        e.phase = PHASE_CAPTURING
        e.phase_changed_local_ms = now_ms
        if e.timer_armed:
            # R-QUIET: рестарт уже сделал таймер за 2 минуты до старта. Повтор здесь
            # стоил бы разрыва записи ровно в окне 0–5 мин, которое и есть самое дорогое.
            # Основание — НАБЛЮДЁННАЯ постановка таймера, а не то, что мы её задумали.
            e.subscribed = True
        else:
            # Таймер не встал (ненулевой код systemd-run) — переподписки не было ни
            # одной, и молчание здесь стоит всего окна. Ведём себя как R-FLIP.
            e.subscribed = False
            wave.append(key)
            flips.append(key)
        e.timer_unit = None
    elif status not in LIVE_STATUSES and e.phase in IN_ENV_PHASES:
        # Ушедший из торгов символ занимает подписку и место на диске, но кадров не даёт.
        # Помечаем; выведет следующая волна (env вне волны не трогаем).
        e.dead = True


def _rotate(known: dict[str, Entry], wave: list[str], now_ms: int,
            counters: dict[str, int]) -> tuple[list[str], list[str]]:
    """Вывод отживших — ТОЛЬКО внутри записи env волной. Возвращает (rotated, shelved)."""
    rotated: list[str] = []
    shelved: list[str] = []
    window = ROTATION_DAYS * DAY_MS
    for key, e in known.items():
        if not e.in_env or key in wave:
            continue
        if e.dead:
            e.phase, e.in_env = PHASE_ROTATED, False
            rotated.append(e.symbol)
        elif e.phase == PHASE_CAPTURING and e.anchor_ms is not None and now_ms - e.anchor_ms > window:
            e.phase, e.in_env = PHASE_ROTATED, False
            rotated.append(e.symbol)
        elif e.phase == PHASE_WATCH and now_ms - e.first_seen_local_ms > window:
            # Не «ротирован»: он ещё ни дня не торговал, и его флип продолжает ловиться.
            e.phase, e.in_env = PHASE_SHELVED, False
            shelved.append(e.symbol)
        else:
            continue
        e.phase_changed_local_ms = now_ms
    counters["rotated"] = len(rotated)
    counters["shelved"] = len(shelved)
    return rotated, shelved


def _wave_alert(hostname: str, known: dict[str, Entry], wave: list[str], flips: list[str],
                retired: list[str], now_ms: int, server_ms: int | None) -> str:
    lines = [f"🆕 {hostname}: волна листингов day-0 — {len(wave)}"]
    for key in sorted(wave, key=lambda k: (known[k].onboard_ms, known[k].symbol)):
        e = known[key]
        quote = f" — квота {e.quote}" if e.quote != "USDT" else ""
        if e.phase == PHASE_ARMED:
            action = f"таймер на {on_calendar_utc(e.onboard_ms - TIMER_LEAD_S * 1000)}"
        elif e.phase == PHASE_WATCH:
            action = "watch: дата в прошлом, подхвачен сейчас"
        elif key in flips:
            action = "начал торговать, переподписка сейчас"
        else:
            action = "уже торгуется, подхвачен сейчас"
        lines.append(
            f"• {e.symbol} — {e.contract_type}/{e.underlying_type}{quote} — "
            f"onboard {on_calendar_utc(e.onboard_ms)} — {action}"
        )
    if retired:
        lines.append(f"выведено ({len(retired)}): {', '.join(sorted(retired))}")
    # Хвост — измерительный крюк: отстаёт ли вместе с serverTime само СОДЕРЖИМОЕ
    # документа, не измерено ничем, кроме первого живого листинга.
    tail = f"локально {iso_ms(now_ms)}"
    if server_ms is not None:
        tail += f" · serverTime {iso_ms(server_ms)} (лаг {(now_ms - server_ms) / 60000:.1f} мин)"
    lines.append(tail)
    return "\n".join(lines)


def _pulse(now_ms: int, server_ms: int | None, known: dict[str, Entry],
           counters: dict[str, int]) -> str:
    return json.dumps(
        {
            "t": now_ms,
            "iso": iso_ms(now_ms),
            "known": len(known),
            "server_lag_s": None if server_ms is None else round((now_ms - server_ms) / 1000, 1),
            "counters": counters,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )


def plan_failure(state: State, reason: str, now_ms: int, *, hostname: str = "") -> tuple[Plan, State]:
    """Провал чтения: мир не меняется НИЧЕМ, кроме счётчика.

    Счётчик живёт в состоянии, а не в памяти процесса: тик — oneshot, и без этого
    серия провалов никогда бы не накопилась (ровно так params-poller молчал 23 часа).
    """
    n = state.consecutive_failures + 1
    due = n == FAIL_ALERT_AFTER or (n > FAIL_ALERT_AFTER and (n - FAIL_ALERT_AFTER) % FAIL_REALERT_EVERY == 0)
    alerts = []
    if due:
        alerts.append(
            f"🔴 {hostname}: day0-поллер не может прочитать exchangeInfo — "
            f"{n} провалов подряд. Последняя причина: {reason[:200]}"
        )
    new_state = replace(
        state,
        consecutive_failures=n,
        failure_alerted=state.failure_alerted or due,
        last_failure_reason=reason[:300],
    )
    return Plan(alerts=alerts, counters={"failures": n}), new_state


# ==============================================================================
# Исполняющий слой
# ==============================================================================


class Host(Protocol):
    """Шов между решениями и миром. В тестах подменяется записывающей подделкой."""

    state_path: str

    def now_ms(self) -> int: ...
    def fetch_exchange_info(self) -> bytes: ...
    def read_file(self, path: str) -> str | None: ...
    def write_atomic(self, path: str, text: str) -> None: ...
    def systemctl(self, *args: str) -> int: ...
    def systemd_run_timer(self, unit: str, on_calendar: str, argv: list[str]) -> int: ...
    def send_telegram(self, text: str) -> bool: ...
    def append_pulse(self, line: str) -> None: ...


def assert_allowed_systemctl(args: tuple[str, ...]) -> None:
    """Поллер живёт от root, поэтому список глаголов — часть контракта, а не стиль.

    Ему нужно ровно две вещи: рестарт СВОЕГО инстанса захвата и снятие СВОИХ
    transient-таймеров. `enable/disable/mask` — конфигурация загрузки, дело оператора;
    остановка чужого инстанса — молчаливая потеря чужой записи.
    """
    if len(args) == 2 and args[0] == "restart" and args[1] == INSTANCE_UNIT:
        return
    if len(args) == 2 and args[0] == "stop" and args[1].startswith(TIMER_PREFIX) \
            and args[1].endswith(".timer"):
        return
    raise Day0Refusal(f"запрещённая команда systemctl: {' '.join(args)}")


def read_alert_env(path: str = ALERT_ENV_PATH) -> tuple[str | None, str | None]:
    """(токен, чат) из того же alert.env, что у alert.sh и heartbeat.sh (root:600).

    Читается В МОМЕНТ ОТПРАВКИ: секрет не оседает ни в состоянии, ни в пульсе, ни в
    логе, а ротация токена не требует рестарта поллера.
    """
    token = chat = None
    try:
        text = Path(path).read_text(encoding="utf-8")
    except OSError:
        return None, None
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.removeprefix("export ").partition("=")
        value = value.strip().strip('"').strip("'")
        if key.strip() == "TG_BOT_TOKEN":
            token = value or None
        elif key.strip() == "TG_CHAT_ID":
            chat = value or None
    return token, chat


class RealHost:
    """Настоящий хост: сеть, диск, systemd. Никаких решений — только исполнение."""

    def __init__(
        self,
        data_dir: str | Path,
        *,
        url: str = EXCHANGE_INFO_URL,
        alert_env_path: str = ALERT_ENV_PATH,
        _open: Callable[..., Any] = urllib.request.urlopen,
        _run: Callable[..., Any] = subprocess.run,
        _now_ms: Callable[[], int] | None = None,
    ) -> None:
        self.data_dir = Path(data_dir)
        self.state_path = str(self.data_dir / "state.json")
        self._url = url
        self._alert_env_path = alert_env_path
        self._open = _open
        self._run = _run
        self._now_ms = _now_ms or (lambda: int(time.time() * 1000))

    def now_ms(self) -> int:
        return self._now_ms()

    def fetch_exchange_info(self) -> bytes:
        """Единственный GET за тик. gzip обязателен: 48 КБ на проводе против 1.05 МБ
        (22.1×), а urllib САМ не распаковывает — только по Content-Encoding."""
        req = urllib.request.Request(
            self._url, headers={"User-Agent": USER_AGENT, "Accept-Encoding": "gzip"},
        )
        with self._open(req, timeout=HTTP_TIMEOUT_S) as resp:
            body = resp.read()
            encoding = (resp.headers.get("Content-Encoding") or "").lower()
        return gzip.decompress(body) if "gzip" in encoding else body

    def read_file(self, path: str) -> str | None:
        try:
            return Path(path).read_text(encoding="utf-8")
        except OSError:
            return None

    def write_atomic(self, path: str, text: str) -> None:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.parent / (p.name + ".tmp")
        tmp.write_text(text, encoding="utf-8")
        os.replace(tmp, p)  # недописанный env — это юнит, который не стартует

    def systemctl(self, *args: str) -> int:
        assert_allowed_systemctl(args)
        return self._run(["systemctl", *args], capture_output=True, text=True).returncode

    def systemd_run_timer(self, unit: str, on_calendar: str, argv: list[str]) -> int:
        cmd = [
            "systemd-run", f"--unit={unit}", f"--on-calendar={on_calendar}",
            # Дефолтная точность systemd — МИНУТА; она съела бы весь запас упреждения.
            "--timer-property=AccuracySec=1s", "--collect", *argv,
        ]
        res = self._run(cmd, capture_output=True, text=True)
        blob = f"{getattr(res, 'stderr', '') or ''}{getattr(res, 'stdout', '') or ''}".lower()
        if res.returncode != 0 and "already exists" in blob:
            return 0  # тот же таймер уже стоит — это успех, а не отказ
        return res.returncode

    def send_telegram(self, text: str) -> bool:
        token, chat = read_alert_env(self._alert_env_path)
        if not token or not chat:
            print("day0_poller: TG не настроен; сообщение только в журнал:\n" + text, flush=True)
            return False
        data = urllib.parse.urlencode({"chat_id": chat, "text": text}).encode()
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data=data, headers={"User-Agent": USER_AGENT},
        )
        try:
            with self._open(req, timeout=15) as resp:
                resp.read()
            return True
        except (OSError, ValueError) as exc:
            # TODO(#88, панель): перечисление классов — тот же баг, что был у
            # funding_poller.py: http.client.IncompleteRead из resp.read() не
            # наследует OSError и вылетает наружу. Лечится зеркалом funding-фикса
            # (тотальный `except Exception` + пин send_telegram_reports_false…).
            # Только класс ошибки: её текст может нести URL, а в URL живёт токен.
            print(f"day0_poller: TG не доставлен ({type(exc).__name__})", flush=True)
            return False

    def append_pulse(self, line: str) -> None:
        """Пульс — ИМЕННО gzip-файл не глубже второго уровня: heartbeat ищет свежесть
        через `find -maxdepth 2 -name '*.gz'`, и обычный pulse.txt он не увидит никогда."""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        day = datetime.fromtimestamp(self.now_ms() // 1000, timezone.utc).strftime("%Y%m%d")
        with gzip.open(self.data_dir / f"pulse-{day}.gz", "at", encoding="utf-8") as fh:
            fh.write(line + "\n")


class DryRunHost:
    """Читает по-настоящему, но наружу не пишет ничего. Для предполётной проверки."""

    def __init__(self, inner: Host) -> None:
        self._inner = inner
        self.state_path = inner.state_path

    def now_ms(self) -> int:
        return self._inner.now_ms()

    def fetch_exchange_info(self) -> bytes:
        return self._inner.fetch_exchange_info()

    def read_file(self, path: str) -> str | None:
        return self._inner.read_file(path)

    def write_atomic(self, path: str, text: str) -> None:
        print(f"--- DRY RUN, записал бы {path}:\n{text}", flush=True)

    def systemctl(self, *args: str) -> int:
        assert_allowed_systemctl(args)
        print(f"--- DRY RUN, выполнил бы: systemctl {' '.join(args)}", flush=True)
        return 0

    def systemd_run_timer(self, unit: str, on_calendar: str, argv: list[str]) -> int:
        print(f"--- DRY RUN, поставил бы таймер {unit} на {on_calendar}: {' '.join(argv)}", flush=True)
        return 0

    def send_telegram(self, text: str) -> bool:
        print(f"--- DRY RUN, отправил бы:\n{text}", flush=True)
        return True

    def append_pulse(self, line: str) -> None:
        print(f"--- DRY RUN, пульс: {line}", flush=True)


def apply_plan(plan: Plan, new_state: State, host: Host, *, hostname: str = "") -> None:
    """Исполнение плана в порядке, выбранном под обрыв в любом месте.

    cancel → env → таймеры → рестарт → алерты → state → пульс. Всё, что стоит ДО
    сохранения состояния, идемпотентно: env сравнивается по байтам, имена таймеров
    детерминированы («already exists» = успех), а повторный рестарт стоит ≤35 с записи.
    Алерты идут до сохранения намеренно: дубль алерта безопаснее навсегда пропущенной
    волны.

    Исполнитель ЖУРНАЛИРУЕТ СВОИ ЭФФЕКТЫ в сохраняемое состояние — и потому мутирует
    `new_state`. Без этого коды возврата systemd отбрасывались, план коммитился как
    исполненный, и «рестарт не состоялся» становилось неотличимо от «состоялся»: символ
    числился capturing, лежал в COLLECTOR_SYMBOLS и не имел ни одной подписки, а сторож
    тишины инстанса (1080 мин) молчал 18 часов. Планировщик при этом остаётся чистым:
    он читает журнал, но не пишет его.
    """
    failures: list[str] = []

    for unit in plan.cancel_timers:
        host.systemctl("stop", unit if unit.endswith(".timer") else unit + ".timer")

    if plan.env_write is not None:
        path, content = plan.env_write
        if path != ENV_PATH:
            # Чужой env — это чужой инстанс сбора. Громкий отказ, а не запись.
            host.send_telegram(f"🔴 day0-поллер отказался писать чужой env: {path}")
            raise Day0Refusal(f"попытка записи вне своего env: {path}")
        host.write_atomic(path, content)

    for unit, calendar in plan.arm_timers:
        rc = host.systemd_run_timer(unit, calendar, ["systemctl", "restart", INSTANCE_UNIT])
        if rc == 0:
            _mark_timer_armed(new_state, unit)
        else:
            failures.append(f"таймер {unit} (systemd-run на {calendar}): rc={rc}")

    if plan.restart_now:
        rc = host.systemctl("restart", INSTANCE_UNIT)
        if rc == 0:
            # Рестарт перечитывает ВЕСЬ env разом, поэтому подтверждается сразу весь
            # его состав, а не один символ волны.
            _mark_subscribed(new_state)
        else:
            failures.append(f"systemctl restart {INSTANCE_UNIT}: rc={rc}")

    alerts = list(plan.alerts)
    if failures:
        n = new_state.effect_failures + 1
        new_state.effect_failures = n
        # Первый отказ — сразу, дальше напоминание раз в ~6 часов: молчать нельзя
        # (прецедент params-поллера, 23 часа), но и шторм при сломанном systemd не нужен.
        if n == 1 or n % FAIL_REALERT_EVERY == 0:
            alerts.append(
                f"🔴 {hostname}: day0-поллер — эффекты тика не исполнились ({n}-й тик подряд): "
                + "; ".join(failures)
            )
    else:
        new_state.effect_failures = 0

    for text in alerts:
        host.send_telegram(text)

    host.write_atomic(host.state_path, dump_state(new_state))

    if plan.pulse is not None:
        # Пульс — про живость самого поллера (он прочитал мир и записал решение), а не
        # про успех systemd: неисполненный эффект уже говорит за себя в Telegram, и
        # глушить им второй канал значило бы путать два разных отказа.
        host.append_pulse(plan.pulse)


def _mark_subscribed(state: State) -> None:
    """Подтверждение подхвата env: наблюдённый рестарт закрывает долг всем, кто в env."""
    for e in state.known.values():
        if e.in_env:
            e.subscribed = True


def _mark_timer_armed(state: State, unit: str) -> None:
    for e in state.known.values():
        if e.timer_unit == unit:
            e.timer_armed = True


def tick(host: Host, *, hostname: str = "") -> str:
    """Один тик поллера. Возвращает строку для журнала."""
    now_ms = host.now_ms()
    state = load_state(host.read_file(host.state_path))
    try:
        snapshot = parse_snapshot(host.fetch_exchange_info())
    except (Day0FetchError, OSError, TimeoutError, ValueError) as exc:
        # Fail closed: мир не меняется ничем, кроме счётчика, и пульс НЕ пишется —
        # тишина пульса и есть второй, независимый от Telegram канал тревоги.
        reason = f"{type(exc).__name__}: {exc}"
        plan, new_state = plan_failure(state, reason, now_ms, hostname=hostname)
        apply_plan(plan, new_state, host, hostname=hostname)
        return f"провал #{new_state.consecutive_failures}: {reason}"

    plan, new_state = plan_tick(
        snapshot, state, now_ms, env_now=host.read_file(ENV_PATH), hostname=hostname,
    )
    apply_plan(plan, new_state, host, hostname=hostname)
    c = plan.counters
    return (
        f"вселенная={c.get('universe', 0)} known={len(new_state.known)} "
        f"волна={c.get('wave', 0)} armed={c.get('armed', 0)} watch={c.get('watch', 0)} "
        f"capturing={c.get('capturing', 0)} ротировано={c.get('rotated', 0)} "
        f"полка={c.get('shelved', 0)} в_env={c.get('in_env', 0)} "
        f"отбраковка(кварт/статус/старые/битые)={c.get('rejected_quarterly', 0)}/"
        f"{c.get('seeded_unknown_status', 0)}/{c.get('seeded_stale', 0)}/"
        f"{c.get('malformed_entry', 0)}"
        + (" ВОЛНА" if c.get("wave", 0) else "")
    )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Поллер листингов дня-0 (Binance USDⓈ-M)")
    ap.add_argument("--data-dir", default=DEFAULT_DATA_DIR,
                    help="каталог состояния, пульса и лога (пульс ищет heartbeat)")
    ap.add_argument("--once", action="store_true", help="один тик и выход (для таймера)")
    ap.add_argument("--interval-s", type=int, default=600)
    ap.add_argument("--dry-run", action="store_true",
                    help="читать по-настоящему, но ничего не менять и никого не будить")
    args = ap.parse_args(argv)

    host: Host = RealHost(args.data_dir)
    if args.dry_run:
        host = DryRunHost(host)
    name = socket.gethostname().split(".")[0]

    while True:
        began = time.time()
        print(f"{iso_ms(host.now_ms())} {tick(host, hostname=name)}", flush=True)
        if args.once:
            return 0
        time.sleep(max(0.0, args.interval_s - (time.time() - began)))


if __name__ == "__main__":
    sys.exit(main())
