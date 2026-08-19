"""Тесты поллера листингов дня-0. Сеть, systemd и диск хоста не трогаем — исполняющий
слой подменяется записывающей подделкой.

Что здесь пиннуется и почему именно это. Вердикт дня-0 упирается в n=1 листинг-событие,
вторая когорта приходит без предупреждения раз в ~2 сут, и каждый пропущенный листинг
невосполним. Значит цена ошибки несимметрична: лишний рестарт стоит ≤35 с записи, а
пропущенная волна — недели ожидания. Поэтому пины держат три класса свойств:

1. ДЕТЕКЦИЯ. Событие — это отсутствие ключа (symbol, onboardMs) в известном состоянии,
   а НЕ «onboardDate в будущем»: в живом снапшоте 2026-08-19 таких символов было ноль,
   а единственный PENDING_TRADING нёс дату на 272 суток В ПРОШЛОМ (GAIBUSDT). Детектор
   на дате не увидел бы ничего.
2. ИДЕМПОТЕНТНОСТЬ. Тик над неизменным миром не смеет писать env и рестартить инстанс:
   каждый рестарт рвёт запись живым символам волны ровно в самое дорогое окно.
3. FAIL-CLOSED. Пустой/битый/усечённый ответ читается как провал, а не как «делистнули
   всё»: прочитанный как мир, он опустошил бы COLLECTOR_SYMBOLS, а collector-run.sh на
   пустом списке выходит с кодом 4 и роняет юнит.

Все времена — по локальным часам: serverTime внутри exchangeInfo измеренно стух на
87 минут и даже убывал, поэтому он только логируется (и это тоже пиннуто).
"""

from __future__ import annotations

import gzip
import inspect
import json
import subprocess
import urllib.error
import urllib.request
from pathlib import Path

import pytest

import day0_poller
from day0_poller import (
    ENV_PATH,
    FAIL_ALERT_AFTER,
    FAIL_REALERT_EVERY,
    INSTANCE_UNIT,
    LIVENESS_TIMEOUT_S,
    PHASE_ARMED,
    PHASE_CAPTURING,
    PHASE_REJECTED,
    PHASE_ROTATED,
    PHASE_SEEDED,
    PHASE_SHELVED,
    PHASE_SUPERSEDED,
    PHASE_WATCH,
    ROTATION_DAYS,
    STALL_TIMEOUT_MIN,
    TIMER_LEAD_S,
    UNIVERSE_FLOOR,
    Day0Refusal,
    Entry,
    Plan,
    RealHost,
    State,
    apply_plan,
    assert_allowed_systemctl,
    dump_state,
    entry_key,
    env_text,
    is_tradifi,
    load_state,
    on_calendar_utc,
    plan_tick,
    read_alert_env,
    tick,
    timer_unit_name,
)

DAY_MS = 86_400_000
# «Сейчас» тестов. Величина неважна, важно что все ожидания считаются от неё же.
NOW = 1_787_000_000_000
# Ровно то отставание serverTime, что измерено живьём 2026-08-19.
SERVER_LAG_MS = 87 * 60 * 1000
ROT_MS = ROTATION_DAYS * DAY_MS


# --- построители входа --------------------------------------------------------


def sym(
    symbol: str,
    onboard_ms: int,
    *,
    status: str = "TRADING",
    contract_type: str = "PERPETUAL",
    quote: str = "USDT",
    underlying_type: str = "COIN",
    sub: list | None = None,
) -> dict:
    """Одна запись массива symbols в форме, которую отдаёт площадка."""
    if sub is None:
        sub = ["TradFi"] if contract_type == "TRADIFI_PERPETUAL" else []
    return {
        "symbol": symbol,
        "pair": symbol,
        "contractType": contract_type,
        "deliveryDate": 4133404800000,
        "onboardDate": onboard_ms,
        "status": status,
        "quoteAsset": quote,
        "underlyingType": underlying_type,
        "underlyingSubType": sub,
    }


def doc(*symbols: dict, server_ms: int | None = None, pad: int = UNIVERSE_FLOOR + 20) -> dict:
    """Документ exchangeInfo: интересные символы плюс балласт старого мира.

    Балласт не украшение: вселенная ниже UNIVERSE_FLOOR читается как провал, и без
    него любой тест случайно проверял бы fail-closed вместо своей темы.
    """
    body = list(symbols)
    body += [sym(f"PAD{i}USDT", NOW - 400 * DAY_MS) for i in range(pad)]
    return {
        "serverTime": NOW - SERVER_LAG_MS if server_ms is None else server_ms,
        "symbols": body,
    }


def raw(d: dict) -> bytes:
    return json.dumps(d).encode()


def entry(
    symbol: str,
    onboard_ms: int,
    phase: str,
    *,
    anchor_ms: int | None = None,
    first_seen_local_ms: int = NOW,
    in_env: bool | None = None,
    timer_unit: str | None = None,
    status: str = "TRADING",
    dead: bool = False,
) -> Entry:
    return Entry(
        symbol=symbol,
        onboard_ms=onboard_ms,
        phase=phase,
        status_last_seen=status,
        contract_type="PERPETUAL",
        quote="USDT",
        underlying_type="COIN",
        tradifi=False,
        first_seen_local_ms=first_seen_local_ms,
        first_seen_server_ms=None,
        anchor_ms=anchor_ms,
        in_env=(phase in (PHASE_WATCH, PHASE_ARMED, PHASE_CAPTURING)) if in_env is None else in_env,
        timer_unit=timer_unit,
        phase_changed_local_ms=first_seen_local_ms,
        dead=dead,
    )


def state_of(*entries: Entry, **kw) -> State:
    return State(known={entry_key(e.symbol, e.onboard_ms): e for e in entries}, **kw)


def env_symbols(content: str) -> list[str]:
    for line in content.splitlines():
        if line.startswith("COLLECTOR_SYMBOLS="):
            return line.split("=", 1)[1].split()
    raise AssertionError("в env нет COLLECTOR_SYMBOLS")


class FakeHost:
    """Двойник исполняющего слоя: всё пишется в память, наружу ни байта."""

    def __init__(self, payload: bytes | Exception = b"", *, now_ms: int = NOW, files: dict | None = None):
        self.state_path = "/data/state.json"
        self.payload = payload
        self._now = now_ms
        self.files: dict[str, str] = dict(files or {})
        self.calls: list[tuple] = []
        self.telegrams: list[str] = []
        self.pulses: list[str] = []
        self.fetches = 0

    # --- часы и сеть
    def now_ms(self) -> int:
        return self._now

    def fetch_exchange_info(self) -> bytes:
        self.fetches += 1
        if isinstance(self.payload, Exception):
            raise self.payload
        return self.payload

    # --- диск
    def read_file(self, path: str) -> str | None:
        self.calls.append(("read", path))
        return self.files.get(path)

    def write_atomic(self, path: str, text: str) -> None:
        self.calls.append(("write", path))
        self.files[path] = text

    # --- systemd
    def systemctl(self, *args: str) -> int:
        self.calls.append(("systemctl",) + args)
        return 0

    def systemd_run_timer(self, unit: str, on_calendar: str, argv: list[str]) -> int:
        self.calls.append(("timer", unit, on_calendar, tuple(argv)))
        return 0

    # --- наружу
    def send_telegram(self, text: str) -> bool:
        self.telegrams.append(text)
        return True

    def append_pulse(self, line: str) -> None:
        self.pulses.append(line)

    # --- удобства теста
    def env(self) -> str | None:
        return self.files.get(ENV_PATH)

    def restarts(self) -> list[tuple]:
        return [c for c in self.calls if c[:2] == ("systemctl", "restart")]

    def timers(self) -> list[tuple]:
        return [c for c in self.calls if c[0] == "timer"]

    def env_writes(self) -> list[tuple]:
        return [c for c in self.calls if c == ("write", ENV_PATH)]


# ==============================================================================
# ДЕТЕКЦИЯ И BOOTSTRAP
# ==============================================================================


def test_bootstrap_seeds_the_stale_world_silently():
    """Первый запуск не имеет права объявить волной весь мир: 872 алерта и env на
    872 символа — это не запуск, а инцидент. Старый мир молча запоминается."""
    plan, new = plan_tick(doc(), State(), NOW)

    assert plan.env_write is None
    assert plan.arm_timers == []
    assert plan.restart_now is False
    assert plan.alerts == []
    assert plan.counters["seeded_stale"] == UNIVERSE_FLOOR + 20
    assert {e.phase for e in new.known.values()} == {PHASE_SEEDED}
    assert plan.pulse is not None  # тик успешен ⇒ след остаётся


def test_bootstrap_captures_symbols_younger_than_the_rotation_window():
    """Хост, пролежавший сутки, не теряет когорту: свежие символы образуют обычную
    волну, а не остаются за бортом «потому что первый запуск»."""
    plan, new = plan_tick(doc(sym("NEWUSDT", NOW - 2 * DAY_MS)), State(), NOW)

    assert plan.env_write is not None and plan.env_write[0] == ENV_PATH
    assert env_symbols(plan.env_write[1]) == ["NEWUSDT"]
    assert plan.restart_now is True
    assert len(plan.alerts) == 1
    assert new.known[entry_key("NEWUSDT", NOW - 2 * DAY_MS)].phase == PHASE_CAPTURING


def test_a_new_pending_symbol_with_a_future_onboard_date_is_armed_with_a_timer():
    onboard = NOW + 2 * 3600 * 1000
    plan, new = plan_tick(doc(sym("SOONUSDT", onboard, status="PENDING_TRADING")), State(), NOW)

    unit = timer_unit_name("SOONUSDT", onboard)
    assert plan.arm_timers == [(unit, on_calendar_utc(onboard - TIMER_LEAD_S * 1000))]
    assert env_symbols(plan.env_write[1]) == ["SOONUSDT"]  # подписка принимается заранее
    assert plan.restart_now is False  # ждём таймер, а не рвём запись сейчас
    assert new.known[entry_key("SOONUSDT", onboard)].phase == PHASE_ARMED


def test_a_new_pending_symbol_with_a_past_onboard_date_is_watched_env_now_no_timer():
    """Класс GAIBUSDT — живой на момент разведки: PENDING_TRADING с датой на 272 суток
    в прошлом. Таймер в прошлое поставить нельзя, значит символ берётся немедленно."""
    onboard = NOW - 272 * DAY_MS
    plan, new = plan_tick(doc(sym("GAIBUSDT", onboard, status="PENDING_TRADING")), State(), NOW)

    assert plan.arm_timers == []
    assert env_symbols(plan.env_write[1]) == ["GAIBUSDT"]
    assert plan.restart_now is True
    e = new.known[entry_key("GAIBUSDT", onboard)]
    assert e.phase == PHASE_WATCH
    # Часы ротации у watch идут от НАБЛЮДЕНИЯ: его onboardDate измеренно недостоверен.
    assert e.anchor_ms is None and e.first_seen_local_ms == NOW


def test_a_watched_symbol_flipping_to_trading_restarts_immediately():
    """R-FLIP. Урок day0-start: после начала торгов нужна свежая подписка, а таймера у
    watch-символа нет — значит рестарт делает сам флип. Env при этом НЕ меняется
    (символ уже в списке), и именно поэтому рестарт не имеет права зависеть от записи."""
    onboard = NOW - 272 * DAY_MS
    before = state_of(entry("GAIBUSDT", onboard, PHASE_WATCH, first_seen_local_ms=NOW - DAY_MS,
                            status="PENDING_TRADING"))
    content = env_text(["GAIBUSDT"])

    plan, new = plan_tick(doc(sym("GAIBUSDT", onboard)), before, NOW, env_now=content)

    assert plan.env_write is None          # байты те же
    assert plan.restart_now is True        # и всё равно рестарт
    assert plan.arm_timers == []
    e = new.known[entry_key("GAIBUSDT", onboard)]
    assert e.phase == PHASE_CAPTURING and e.anchor_ms == NOW  # якорь — время флипа
    assert len(plan.alerts) == 1


def test_a_shelved_symbol_still_captures_on_its_trading_flip():
    """«Не торговал 14 суток» — не приговор: PENDING может лежать месяцами (272 суток
    у GAIBUSDT), и его старт обязан быть пойман."""
    onboard = NOW - 300 * DAY_MS
    before = state_of(entry("GAIBUSDT", onboard, PHASE_SHELVED, in_env=False,
                            first_seen_local_ms=NOW - 20 * DAY_MS, status="PENDING_TRADING"))

    plan, new = plan_tick(doc(sym("GAIBUSDT", onboard)), before, NOW, env_now=env_text(["OTHERUSDT"]))

    assert env_symbols(plan.env_write[1]) == ["GAIBUSDT"]
    assert plan.restart_now is True
    assert new.known[entry_key("GAIBUSDT", onboard)].phase == PHASE_CAPTURING


def test_an_armed_symbol_flipping_to_trading_changes_nothing():
    """R-QUIET. Таймер уже сделал рестарт за 2 минуты до старта; повтор здесь стоил бы
    разрыва записи ровно в окне 0–5 мин, которое и есть самое дорогое."""
    onboard = NOW - 60_000
    unit = timer_unit_name("SOONUSDT", onboard)
    before = state_of(entry("SOONUSDT", onboard, PHASE_ARMED, anchor_ms=onboard,
                            timer_unit=unit, status="PENDING_TRADING"))

    plan, new = plan_tick(doc(sym("SOONUSDT", onboard)), before, NOW, env_now=env_text(["SOONUSDT"]))

    assert plan == Plan(counters=plan.counters, pulse=plan.pulse)  # ни одного действия
    e = new.known[entry_key("SOONUSDT", onboard)]
    assert e.phase == PHASE_CAPTURING and e.anchor_ms == onboard and e.in_env is True


def test_a_symbol_first_seen_trading_within_the_window_is_captured_now():
    """Все 36 листингов за 30 суток впервые видны уже как TRADING — окно PENDING
    проходит между тиками. Значит этот путь и есть основной, а не угол."""
    for age_days in (0, 3, ROTATION_DAYS):  # граница включительно: захват == ротация
        onboard = NOW - age_days * DAY_MS
        plan, new = plan_tick(doc(sym("FRESHUSDT", onboard)), State(), NOW)
        assert new.known[entry_key("FRESHUSDT", onboard)].phase == PHASE_CAPTURING, age_days
        assert plan.restart_now is True


def test_a_symbol_first_seen_trading_older_than_the_window_is_seeded_not_captured():
    onboard = NOW - ROT_MS - 1
    plan, new = plan_tick(doc(sym("OLDUSDT", onboard)), State(), NOW)

    assert new.known[entry_key("OLDUSDT", onboard)].phase == PHASE_SEEDED
    assert plan.env_write is None and plan.alerts == []
    assert plan.counters["seeded_stale"] == UNIVERSE_FLOOR + 21


def test_a_relisted_symbol_with_a_new_onboard_date_is_a_new_day_zero_event():
    """Ключ — пара (symbol, onboardMs). По голому тикеру релистинг был бы неотличим
    от старой записи и день-0 прошёл бы мимо."""
    old_onboard = NOW - 200 * DAY_MS
    new_onboard = NOW - 3600_000
    before = state_of(entry("PHOENIXUSDT", old_onboard, PHASE_ROTATED, in_env=False,
                            anchor_ms=old_onboard))

    plan, new = plan_tick(doc(sym("PHOENIXUSDT", new_onboard)), before, NOW)

    assert new.known[entry_key("PHOENIXUSDT", new_onboard)].phase == PHASE_CAPTURING
    assert new.known[entry_key("PHOENIXUSDT", old_onboard)].phase == PHASE_ROTATED  # старая не тронута
    assert env_symbols(plan.env_write[1]) == ["PHOENIXUSDT"]


def test_a_rescheduled_onboard_date_supersedes_and_cancels_only_its_own_timer():
    """Перенос листинга: старый таймер выстрелил бы в пустоту и рванул бы запись
    живым символам. Снимается ТОЛЬКО его таймер — соседний остаётся нетронутым."""
    old_onboard = NOW + 3600_000
    new_onboard = NOW + 7200_000
    mine = timer_unit_name("MOVEDUSDT", old_onboard)
    neighbour = timer_unit_name("STAYUSDT", NOW + 1800_000)
    before = state_of(
        entry("MOVEDUSDT", old_onboard, PHASE_ARMED, anchor_ms=old_onboard, timer_unit=mine,
              status="PENDING_TRADING"),
        entry("STAYUSDT", NOW + 1800_000, PHASE_ARMED, anchor_ms=NOW + 1800_000,
              timer_unit=neighbour, status="PENDING_TRADING"),
    )

    plan, new = plan_tick(
        doc(sym("MOVEDUSDT", new_onboard, status="PENDING_TRADING"),
            sym("STAYUSDT", NOW + 1800_000, status="PENDING_TRADING")),
        before, NOW,
    )

    assert plan.cancel_timers == [mine]
    assert new.known[entry_key("MOVEDUSDT", old_onboard)].phase == PHASE_SUPERSEDED
    assert new.known[entry_key("MOVEDUSDT", old_onboard)].in_env is False
    assert new.known[entry_key("MOVEDUSDT", new_onboard)].phase == PHASE_ARMED
    assert plan.arm_timers == [(timer_unit_name("MOVEDUSDT", new_onboard),
                                on_calendar_utc(new_onboard - TIMER_LEAD_S * 1000))]
    assert env_symbols(plan.env_write[1]) == ["STAYUSDT", "MOVEDUSDT"]  # без дубля, по дате
    assert new.known[entry_key("STAYUSDT", NOW + 1800_000)].timer_unit == neighbour


def test_a_dated_quarterly_future_is_rejected_with_a_declared_counter():
    """BTCUSDT_261225 и ETHUSDT_261225 приезжают парами по расписанию биржи. Это
    перевыпуск серии, а не день-0, и отбраковка обязана быть видимой."""
    plan, new = plan_tick(
        doc(sym("BTCUSDT_261225", NOW - 3600_000, contract_type="CURRENT_QUARTER"),
            sym("ETHUSDT_261225", NOW - 3600_000, contract_type="NEXT_QUARTER")),
        State(), NOW,
    )

    assert plan.env_write is None and plan.alerts == []
    assert plan.counters["rejected_quarterly"] == 2
    assert new.known[entry_key("BTCUSDT_261225", NOW - 3600_000)].phase == PHASE_REJECTED


def test_an_unknown_status_is_never_a_listing():
    """Живьём наблюдались три статуса, документация знает больше. Незнакомый читается
    fail-closed: не листинг, но и не молчаливый пропуск — со счётчиком."""
    plan, new = plan_tick(
        doc(sym("WEIRDUSDT", NOW - 3600_000, status="PRE_DELIVERING"),
            sym("GONEUSDT", NOW - 3600_000, status="SETTLING")),
        State(), NOW,
    )

    assert plan.env_write is None and plan.restart_now is False
    assert plan.counters["seeded_unknown_status"] == 2
    assert new.known[entry_key("WEIRDUSDT", NOW - 3600_000)].phase == PHASE_SEEDED


def test_a_non_usdt_quote_is_captured_and_named_in_the_alert():
    """Квоты U/USD1/USDC — 5 листингов за 90 суток. Каждый пропущенный невосполним,
    поэтому берём все, но класс называем: сегментировать можно в анализе."""
    plan, _ = plan_tick(doc(sym("BTCU", NOW - 3600_000, quote="U")), State(), NOW, hostname="tokyo")

    assert env_symbols(plan.env_write[1]) == ["BTCU"]
    assert "квота U" in plan.alerts[0]


def test_the_tradifi_classifier_survives_an_empty_underlying_subtype():
    """34 символа из 872 несут underlyingSubType == []. sub[0] уронил бы поллер на них."""
    assert is_tradifi(sym("SKHYNIXUSDT", NOW, contract_type="TRADIFI_PERPETUAL")) is True
    assert is_tradifi(sym("XUSDT", NOW, contract_type="TRADIFI_PERPETUAL", sub=[])) is True
    assert is_tradifi(sym("USDCUSDT", NOW, sub=[])) is False
    assert is_tradifi({"symbol": "NOFIELDS"}) is False
    assert is_tradifi(sym("YUSDT", NOW, sub=None)) is False
    # underlyingType как признак ЛОЖЕН в обе стороны: ANTHROPICUSDT — TRADIFI при INDEX,
    # а BTCDOMUSDT — обычный PERPETUAL при INDEX.
    assert is_tradifi(sym("ANTHROPICUSDT", NOW, contract_type="TRADIFI_PERPETUAL",
                          underlying_type="INDEX")) is True
    assert is_tradifi(sym("BTCDOMUSDT", NOW, underlying_type="INDEX")) is False


def test_the_alert_names_the_contract_class():
    """Класс решает экономику клетки: мейкер на TRADIFI_PERPETUAL измеренно 0.00 бп.
    Оператор обязан видеть класс из самого алерта, не открывая биржу."""
    plan, _ = plan_tick(
        doc(sym("SKHYNIXUSDT", NOW - 600_000, contract_type="TRADIFI_PERPETUAL",
                underlying_type="KR_EQUITY"),
            sym("MEMEUSDT", NOW - 600_000)),
        State(), NOW, hostname="tokyo",
    )

    text = plan.alerts[0]
    assert "TRADIFI_PERPETUAL/KR_EQUITY" in text
    assert "PERPETUAL/COIN" in text
    assert "tokyo" in text


def test_symbols_are_written_verbatim_from_exchange_info():
    """Коллектор binancefuturesum не валидирует символы ВООБЩЕ (fetch_symbol_list мёртв),
    значит опечатка молчалива 18 часов при стороже 1080 мин. Единственная защита —
    писать ровно ту строку, что отдала площадка."""
    odd = "1000BONKUSDT"
    lower = "TesTUSDT"  # ловит .upper()/.strip() в поллере: площадка отдаёт как есть
    plan, _ = plan_tick(doc(sym(odd, NOW - 600_000), sym(lower, NOW - 500_000)), State(), NOW)

    assert env_symbols(plan.env_write[1]) == [odd, lower]


# ==============================================================================
# ВОЛНА, ENV, РОТАЦИЯ
# ==============================================================================


def test_a_wave_writes_the_env_once_appending_to_live_symbols():
    live = NOW - 5 * DAY_MS
    fresh = NOW - 600_000
    before = state_of(entry("LIVEUSDT", live, PHASE_CAPTURING, anchor_ms=live))

    plan, _ = plan_tick(doc(sym("LIVEUSDT", live), sym("NEWUSDT", fresh)), before, NOW)

    assert env_symbols(plan.env_write[1]) == ["LIVEUSDT", "NEWUSDT"]  # порядок по onboardMs


def test_the_six_symbol_wave_plans_six_timers_one_env_write_and_at_most_one_restart():
    """Реальная форма 14.08: шесть символов с шагом РОВНО 5 минут. Решение — рестарт на
    каждый символ (так и была снята шестёрка, чей вердикт +13.16 бп получен С этими
    разрывами), но НЕМЕДЛЕННЫЙ рестарт при этом не имеет права размножаться."""
    base = NOW + 2 * 3600 * 1000
    wave = [sym(f"KRW{i}USDT", base + i * 5 * 60_000, status="PENDING_TRADING",
                contract_type="TRADIFI_PERPETUAL", underlying_type="KR_EQUITY")
            for i in range(6)]

    plan, _ = plan_tick(doc(*wave), State(), NOW)

    assert len(plan.arm_timers) == 6
    assert len(env_symbols(plan.env_write[1])) == 6
    assert plan.restart_now is False  # волна из одних будущих дат сама себя не рвёт
    assert len(plan.alerts) == 1

    # Та же волна, но первый символ поллер проспал и он уже торгует: рестарт РОВНО один.
    wave[0] = sym("KRW0USDT", NOW - 180_000, contract_type="TRADIFI_PERPETUAL",
                  underlying_type="KR_EQUITY")
    plan2, _ = plan_tick(doc(*wave), State(), NOW)
    assert len(plan2.arm_timers) == 5
    assert plan2.restart_now is True
    assert len(plan2.alerts) == 1


def test_an_armed_only_wave_does_not_restart_the_instance():
    """Уточнение (г) задания: рестарт «чтобы подхватить env» нужен лишь тому, кому
    подписка нужна СЕЙЧАС. У armed её подхватит рестарт собственного таймера."""
    plan, _ = plan_tick(
        doc(sym("AUSDT", NOW + 3600_000, status="PENDING_TRADING"),
            sym("BUSDT", NOW + 7200_000, status="PENDING_TRADING")),
        State(), NOW,
    )

    assert plan.restart_now is False
    assert len(plan.arm_timers) == 2


def test_the_env_carries_the_watchdog_overrides():
    """Дефолтный сторож тишины (5 мин) убивал шестёрку каждые 305 с: принятая подписка
    на ещё не торгующийся символ не даёт НИ ОДНОГО кадра (измерено на GAIBUSDT).
    Плюс сессионные KR-эквити молчат всю ночь. Оба случая кроет 1080 мин."""
    plan, _ = plan_tick(doc(sym("NEWUSDT", NOW - 600_000)), State(), NOW)
    content = plan.env_write[1]

    assert f"COLLECTOR_STALL_TIMEOUT_MIN={STALL_TIMEOUT_MIN}" in content
    assert f"COLLECTOR_LIVENESS_TIMEOUT_S={LIVENESS_TIMEOUT_S}" in content
    assert "COLLECTOR_EXCHANGE=binancefuturesum" in content
    assert STALL_TIMEOUT_MIN == 1080 and LIVENESS_TIMEOUT_S == 64800


def test_the_env_write_is_atomic_tmp_plus_rename(tmp_path, monkeypatch):
    """Недописанный env — это юнит, который не стартует: EnvironmentFile обязателен."""
    seen = []
    real_replace = day0_poller.os.replace

    def spy(src, dst):
        seen.append((str(src), str(dst)))
        real_replace(src, dst)

    monkeypatch.setattr(day0_poller.os, "replace", spy)
    host = RealHost(tmp_path)
    target = tmp_path / "etc" / "x.env"
    host.write_atomic(str(target), "COLLECTOR_SYMBOLS=A\n")

    assert target.read_text(encoding="utf-8") == "COLLECTOR_SYMBOLS=A\n"
    assert len(seen) == 1 and seen[0][0].endswith(".tmp") and seen[0][1] == str(target)
    assert not list(tmp_path.rglob("*.tmp"))


def test_an_unchanged_world_produces_no_effects_but_the_pulse():
    """Второй тик над тем же миром не пишет env и не рестартит: иначе поллер сам себе
    и есть разрыв записи. Пульс при этом обязан идти — иначе «не менялось» и «мы не
    смотрели» неразличимы (тот же принцип, что у params/positions-поллеров)."""
    host = FakeHost(raw(doc(sym("NEWUSDT", NOW - 600_000))))
    tick(host, hostname="tokyo")
    assert host.env_writes() and host.restarts() and len(host.telegrams) == 1

    host.calls.clear()
    host.telegrams.clear()
    tick(host, hostname="tokyo")

    assert host.env_writes() == []
    assert host.restarts() == []
    assert host.timers() == []
    assert host.telegrams == []
    assert len(host.pulses) == 2


def test_rotation_happens_only_inside_a_wave_write():
    """Между волнами env не трогается вообще: тишина поллера снаружи неотличима от
    его отсутствия, и ротация «сама по себе» была бы невидимой мутацией конфига."""
    old = NOW - 20 * DAY_MS
    before = state_of(entry("OLDUSDT", old, PHASE_CAPTURING, anchor_ms=old))

    quiet, after_quiet = plan_tick(doc(sym("OLDUSDT", old)), before, NOW,
                                   env_now=env_text(["OLDUSDT"]))
    assert quiet.env_write is None
    assert after_quiet.known[entry_key("OLDUSDT", old)].in_env is True

    wave, after_wave = plan_tick(doc(sym("OLDUSDT", old), sym("NEWUSDT", NOW - 600_000)),
                                 before, NOW, env_now=env_text(["OLDUSDT"]))
    assert env_symbols(wave.env_write[1]) == ["NEWUSDT"]
    assert after_wave.known[entry_key("OLDUSDT", old)].phase == PHASE_ROTATED


def test_rotation_ages_from_the_anchor_by_local_clock_boundary_exclusive():
    """Граница строго больше, и обе стороны проверены. Часы — локальные: serverTime
    измеренно стух на 87 минут и даже убывал, поэтому здесь он подставлен так, что
    по нему символ был бы «свежим», — и это ничего не меняет."""
    for age, expected in ((ROT_MS, PHASE_CAPTURING), (ROT_MS + 1, PHASE_ROTATED)):
        anchor = NOW - age
        before = state_of(entry("EDGEUSDT", anchor, PHASE_CAPTURING, anchor_ms=anchor))
        plan, after = plan_tick(
            doc(sym("EDGEUSDT", anchor), sym("NEWUSDT", NOW - 600_000), server_ms=anchor + DAY_MS),
            before, NOW, env_now=env_text(["EDGEUSDT"]),
        )
        assert after.known[entry_key("EDGEUSDT", anchor)].phase == expected, age
        assert ("EDGEUSDT" in env_symbols(plan.env_write[1])) == (expected == PHASE_CAPTURING)


def test_a_stale_onboard_date_never_rotates_a_symbol_before_it_traded():
    """У watch onboardDate измеренно недостоверен (272 суток в прошлом у живого
    PENDING). Ротация по нему выкинула бы символ ровно перед его листингом."""
    onboard = NOW - 272 * DAY_MS
    young = state_of(entry("GAIBUSDT", onboard, PHASE_WATCH, first_seen_local_ms=NOW - DAY_MS,
                           status="PENDING_TRADING"))
    plan, after = plan_tick(doc(sym("GAIBUSDT", onboard, status="PENDING_TRADING"),
                                sym("NEWUSDT", NOW - 600_000)), young, NOW)
    assert after.known[entry_key("GAIBUSDT", onboard)].phase == PHASE_WATCH
    assert "GAIBUSDT" in env_symbols(plan.env_write[1])

    # А вот 14 суток БЕЗ ТОРГОВ от первого наблюдения — уже полка, не ротация:
    # флип из shelved продолжает ловиться (см. отдельный пин).
    old = state_of(entry("GAIBUSDT", onboard, PHASE_WATCH,
                         first_seen_local_ms=NOW - ROT_MS - 1, status="PENDING_TRADING"))
    plan2, after2 = plan_tick(doc(sym("GAIBUSDT", onboard, status="PENDING_TRADING"),
                                  sym("NEWUSDT", NOW - 600_000)), old, NOW)
    assert after2.known[entry_key("GAIBUSDT", onboard)].phase == PHASE_SHELVED
    assert env_symbols(plan2.env_write[1]) == ["NEWUSDT"]


def test_a_dead_status_marks_the_symbol_for_rotation_regardless_of_age():
    """Ушедший в SETTLING символ занимает подписку и место на диске, но кадров не даёт."""
    live = NOW - DAY_MS
    before = state_of(entry("DYINGUSDT", live, PHASE_CAPTURING, anchor_ms=live))

    quiet, after_quiet = plan_tick(doc(sym("DYINGUSDT", live, status="SETTLING")), before, NOW,
                                   env_now=env_text(["DYINGUSDT"]))
    assert quiet.env_write is None                       # метка — не повод писать env
    assert after_quiet.known[entry_key("DYINGUSDT", live)].dead is True

    wave, after_wave = plan_tick(
        doc(sym("DYINGUSDT", live, status="SETTLING"), sym("NEWUSDT", NOW - 600_000)),
        after_quiet, NOW, env_now=env_text(["DYINGUSDT"]),
    )
    assert env_symbols(wave.env_write[1]) == ["NEWUSDT"]
    assert after_wave.known[entry_key("DYINGUSDT", live)].phase == PHASE_ROTATED

    # Выведенный однажды больше не выводится: иначе каждая следующая волна отчитывалась
    # бы о нём заново, и «выведено» в алерте перестало бы значить «произошло сейчас».
    third, _ = plan_tick(
        doc(sym("DYINGUSDT", live, status="SETTLING"), sym("NEWUSDT", NOW - 600_000),
            sym("SECONDUSDT", NOW - 300_000)),
        after_wave, NOW, env_now=wave.env_write[1],
    )
    assert third.counters["rotated"] == 0
    assert "выведено" not in third.alerts[0]


def test_rotation_can_never_empty_the_symbol_list():
    """collector-run.sh на пустом COLLECTOR_SYMBOLS выходит с кодом 4 и роняет юнит.
    Пустой список структурно недостижим (волна добавляет ≥1), поэтому здесь пинится
    сам пояс — и то, что волна вытесняет всё остальное, но не себя."""
    with pytest.raises(Day0Refusal):
        env_text([])

    old = NOW - 30 * DAY_MS
    before = state_of(entry("OLDUSDT", old, PHASE_CAPTURING, anchor_ms=old, dead=True))
    plan, _ = plan_tick(doc(sym("OLDUSDT", old, status="SETTLING"), sym("NEWUSDT", NOW - 600_000)),
                        before, NOW, env_now=env_text(["OLDUSDT"]))
    assert env_symbols(plan.env_write[1]) == ["NEWUSDT"]


def test_a_disappeared_symbol_is_not_an_event():
    """Исчезновение ключа — не наблюдение: пустой или усечённый ответ прочитался бы
    как «делистнули всё». Диффер обязан пережить отсутствие и ничего не менять."""
    live = NOW - DAY_MS
    before = state_of(entry("GHOSTUSDT", live, PHASE_CAPTURING, anchor_ms=live))

    plan, after = plan_tick(doc(), before, NOW, env_now=env_text(["GHOSTUSDT"]))

    assert plan.env_write is None and plan.restart_now is False and plan.alerts == []
    e = after.known[entry_key("GHOSTUSDT", live)]
    assert e.phase == PHASE_CAPTURING and e.in_env is True and e.dead is False


# ==============================================================================
# ТАЙМЕРЫ И РЕСТАРТЫ
# ==============================================================================


def test_timer_unit_names_are_deterministic_and_rearming_is_a_noop(tmp_path):
    """Детерминизм имени и есть идемпотентность: повторная постановка того же таймера
    после обрыва обязана читаться как успех, а не как отказ."""
    onboard = NOW + 3600_000
    assert timer_unit_name("AUSDT", onboard) == timer_unit_name("AUSDT", onboard)
    assert timer_unit_name("AUSDT", onboard) != timer_unit_name("AUSDT", onboard + 1)
    assert timer_unit_name("AUSDT", onboard).startswith("hft-day0-restart-")

    class _Res:
        def __init__(self, rc, err): self.returncode, self.stderr, self.stdout = rc, err, ""

    host = RealHost(tmp_path, _run=lambda *a, **k: _Res(1, "Unit hft-day0-restart-AUSDT-1.timer already exists."))
    assert host.systemd_run_timer("hft-day0-restart-AUSDT-1", "2026-08-20 01:58:00 UTC",
                                  ["systemctl", "restart", INSTANCE_UNIT]) == 0
    host_bad = RealHost(tmp_path, _run=lambda *a, **k: _Res(1, "Failed to start transient timer"))
    assert host_bad.systemd_run_timer("u", "c", ["systemctl", "restart", INSTANCE_UNIT]) == 1


def test_no_timer_is_ever_scheduled_into_the_past():
    """systemd-run на прошедшее время либо стреляет немедленно, либо не стреляет вовсе;
    и то и другое хуже честного «подхватить сейчас»."""
    plan, _ = plan_tick(
        doc(sym("PASTUSDT", NOW - 272 * DAY_MS, status="PENDING_TRADING"),
            sym("SOONUSDT", NOW + 60_000, status="PENDING_TRADING"),
            sym("FUTUREUSDT", NOW + 3600_000, status="PENDING_TRADING")),
        State(), NOW,
    )

    assert [u for u, _ in plan.arm_timers] == [timer_unit_name("FUTUREUSDT", NOW + 3600_000)]
    for _, calendar in plan.arm_timers:
        assert calendar > on_calendar_utc(NOW)


def test_a_timer_fires_two_minutes_before_onboard_with_one_second_accuracy(tmp_path):
    """Арифметика запаса: stop инстанса до 30 с (TimeoutStopSec) + старт с подпиской ~5 с,
    значит на −120 с подписка встаёт не позже чем за ~85 с до торгов. Дефолтная
    точность systemd — МИНУТА, она съела бы весь запас, поэтому AccuracySec=1s."""
    onboard = NOW + 3600_000
    plan, _ = plan_tick(doc(sym("SOONUSDT", onboard, status="PENDING_TRADING")), State(), NOW)
    unit, calendar = plan.arm_timers[0]
    assert calendar == on_calendar_utc(onboard - 120_000)
    assert calendar.endswith(" UTC") and TIMER_LEAD_S == 120

    seen = {}

    class _Res:
        returncode, stderr, stdout = 0, "", ""

    def runner(cmd, **kw):
        seen["cmd"] = cmd
        return _Res()

    RealHost(tmp_path, _run=runner).systemd_run_timer(unit, calendar,
                                                      ["systemctl", "restart", INSTANCE_UNIT])
    assert seen["cmd"] == [
        "systemd-run", f"--unit={unit}", f"--on-calendar={calendar}",
        "--timer-property=AccuracySec=1s", "--collect",
        "systemctl", "restart", INSTANCE_UNIT,
    ]


def test_a_pending_symbol_closer_than_the_lead_is_captured_now_not_timed():
    """onboard − 120 с уже в прошлом: таймер поставить некуда, а окно 0–5 мин стоит
    +13.16 бп — значит подписка ставится немедленно."""
    onboard = NOW + 60_000
    plan, new = plan_tick(doc(sym("EDGEUSDT", onboard, status="PENDING_TRADING")), State(), NOW)

    assert plan.arm_timers == []
    assert plan.restart_now is True
    assert new.known[entry_key("EDGEUSDT", onboard)].phase == PHASE_WATCH


# ==============================================================================
# FAIL-CLOSED
# ==============================================================================


def _failing_host(exc: Exception, *, known_env: str = "COLLECTOR_SYMBOLS=LIVEUSDT\n") -> FakeHost:
    live = NOW - DAY_MS
    st = state_of(entry("LIVEUSDT", live, PHASE_CAPTURING, anchor_ms=live))
    return FakeHost(exc, files={ENV_PATH: known_env, "/data/state.json": dump_state(st)})


def test_a_fetch_failure_changes_nothing_but_the_counter():
    host = _failing_host(urllib.error.URLError("венью молчит"))
    out = tick(host, hostname="tokyo")

    assert host.env_writes() == [] and host.restarts() == [] and host.timers() == []
    assert host.telegrams == [] and host.pulses == []   # пульса нет ⇒ сторож увидит тишину
    after = load_state(host.files["/data/state.json"])
    assert after.consecutive_failures == 1
    assert after.known[entry_key("LIVEUSDT", NOW - DAY_MS)].phase == PHASE_CAPTURING
    assert "провал" in out


def test_a_parse_failure_changes_nothing_but_the_counter():
    host = _failing_host(Exception("не используется"))
    host.payload = b"{\xd0\xbd\xd0\xb5 json"
    tick(host, hostname="tokyo")

    assert host.env_writes() == [] and host.pulses == []
    assert load_state(host.files["/data/state.json"]).consecutive_failures == 1


def test_an_empty_universe_is_a_failure_not_a_delisting():
    """Прочитанный как мир, пустой ответ опустошил бы COLLECTOR_SYMBOLS → exit 4 у
    collector-run.sh → падение инстанса. Пол вселенной ловит и усечённый ответ."""
    for payload in (
        raw({"serverTime": NOW, "symbols": []}),
        raw({"serverTime": NOW}),
        raw({"serverTime": NOW, "symbols": [sym("AUSDT", NOW)] * (UNIVERSE_FLOOR - 1)}),
        b"[]",
    ):
        host = _failing_host(Exception("не используется"))
        host.payload = payload
        tick(host, hostname="tokyo")
        assert host.env_writes() == [], payload[:40]
        assert host.pulses == []
        after = load_state(host.files["/data/state.json"])
        assert after.consecutive_failures == 1
        assert after.known[entry_key("LIVEUSDT", NOW - DAY_MS)].in_env is True


def test_six_consecutive_failures_alert_once_then_every_six_hours():
    """Прецедент: params-poller молча не срабатывал 23 часа. Порог — час тишины, дальше
    напоминание раз в ~6 часов, а не шторм на каждый тик."""
    host = _failing_host(urllib.error.URLError("венью молчит"))
    for _ in range(FAIL_ALERT_AFTER - 1):
        tick(host, hostname="tokyo")
    assert host.telegrams == []

    tick(host, hostname="tokyo")
    assert len(host.telegrams) == 1 and "tokyo" in host.telegrams[0]

    for _ in range(FAIL_REALERT_EVERY - 1):
        tick(host, hostname="tokyo")
    assert len(host.telegrams) == 1

    tick(host, hostname="tokyo")
    assert len(host.telegrams) == 2
    assert load_state(host.files["/data/state.json"]).consecutive_failures == \
        FAIL_ALERT_AFTER + FAIL_REALERT_EVERY


def test_recovery_after_an_alerted_outage_sends_one_all_clear_and_resets():
    host = _failing_host(urllib.error.URLError("венью молчит"))
    for _ in range(FAIL_ALERT_AFTER):
        tick(host, hostname="tokyo")
    assert len(host.telegrams) == 1

    host.payload = raw(doc(sym("LIVEUSDT", NOW - DAY_MS)))
    tick(host, hostname="tokyo")

    assert len(host.telegrams) == 2 and "восстановил" in host.telegrams[1]
    after = load_state(host.files["/data/state.json"])
    assert after.consecutive_failures == 0 and after.failure_alerted is False
    assert len(host.pulses) == 1  # пульс появляется только на успехе

    tick(host, hostname="tokyo")
    assert len(host.telegrams) == 2  # «восстановился» ровно один раз


def test_the_pulse_is_a_gz_file_touched_only_on_success(tmp_path):
    """Контракт сторожа: heartbeat ищет `find -maxdepth 2 -name '*.gz'`. Обычный
    pulse.txt он не увидит НИКОГДА и будет вечно рапортовать STALE nofiles."""
    host = RealHost(tmp_path, _now_ms=lambda: NOW)
    host.append_pulse(json.dumps({"t": NOW}))
    host.append_pulse(json.dumps({"t": NOW + 1}))

    files = list(tmp_path.rglob("*.gz"))
    assert len(files) == 1
    assert len(files[0].relative_to(tmp_path).parts) <= 2  # глубже сторож не смотрит
    with gzip.open(files[0], "rt", encoding="utf-8") as fh:
        lines = [json.loads(x) for x in fh if x.strip()]
    assert [r["t"] for r in lines] == [NOW, NOW + 1]


def test_a_crash_between_env_write_and_state_save_heals_idempotently():
    """Порядок применения выбран под обрыв: всё до сохранения state идемпотентно.
    Реплей обязан совпасть по байтам env, не рестартить второй раз и лишь продублировать
    алерт — дубль безопаснее навсегда пропущенной волны."""
    host = FakeHost(raw(doc(sym("NEWUSDT", NOW - 600_000))))
    before_state = host.files.get(host.state_path)
    tick(host, hostname="tokyo")
    assert host.env_writes() and host.restarts()

    # обрыв ровно после записи env: state не сохранился
    if before_state is None:
        host.files.pop(host.state_path, None)
    else:
        host.files[host.state_path] = before_state
    host.calls.clear()
    host.telegrams.clear()

    tick(host, hostname="tokyo")

    assert host.env_writes() == []      # байты совпали
    assert host.restarts() == []        # и потому рестарта нет
    assert len(host.telegrams) == 1     # дубль ровно один
    assert load_state(host.files[host.state_path]).known  # state доехал со второй попытки


def test_the_alert_goes_out_before_the_state_is_saved():
    """Порядок применения — не стиль, а выбор направления потери. Сохрани состояние
    ДО алерта — и обрыв между ними стирает волну НАВСЕГДА: следующий тик увидит её уже
    известной и промолчит, а листинг невосполним. Обратный порядок стоит максимум дубля."""

    class Dying(FakeHost):
        def send_telegram(self, text: str) -> bool:
            raise RuntimeError("процесс умер на отправке")

    host = Dying(raw(doc(sym("NEWUSDT", NOW - 600_000))))
    with pytest.raises(RuntimeError):
        tick(host, hostname="tokyo")

    assert host.state_path not in host.files   # состояние НЕ сохранено
    assert host.env_writes()                   # env уже записан — он идемпотентен по байтам

    # и следующий тик действительно повторяет волну, а не глотает её
    healthy = FakeHost(raw(doc(sym("NEWUSDT", NOW - 600_000))), files=dict(host.files))
    tick(healthy, hostname="tokyo")
    assert len(healthy.telegrams) == 1
    assert healthy.env_writes() == [] and healthy.restarts() == []


def test_corrupt_state_rebootstraps_without_restarting_anything():
    """Битое состояние читается как «ничего не знаем» (прецедент positions-поллера).
    Re-bootstrap не имеет права рвать живую запись: env уже верен, значит тишина."""
    fresh = NOW - 600_000
    host = FakeHost(raw(doc(sym("NEWUSDT", fresh))),
                    files={ENV_PATH: env_text(["NEWUSDT"]), "/data/state.json": "{не json"})

    tick(host, hostname="tokyo")

    assert host.env_writes() == [] and host.restarts() == []
    after = load_state(host.files["/data/state.json"])
    assert after.known[entry_key("NEWUSDT", fresh)].phase == PHASE_CAPTURING
    assert len(host.telegrams) == 1  # плата за re-bootstrap — один повторный алерт


# ==============================================================================
# ГРАНИЦЫ ИСПОЛНЯЮЩЕГО СЛОЯ
# ==============================================================================


def test_the_planner_does_no_io(monkeypatch):
    """Решения — данные. Планировщик, который ходит в сеть или в systemd, нельзя
    ни протестировать, ни прогнать в --dry-run."""
    def forbidden(*a, **k):
        raise AssertionError("планировщик полез в IO")

    monkeypatch.setattr(urllib.request, "urlopen", forbidden)
    monkeypatch.setattr(subprocess, "run", forbidden)
    host = FakeHost(raw(doc()))

    plan, _ = plan_tick(doc(sym("NEWUSDT", NOW - 600_000)), State(), NOW)

    assert plan.env_write is not None
    assert host.calls == [] and host.fetches == 0
    assert "host" not in inspect.signature(plan_tick).parameters


def test_exactly_one_get_per_tick():
    """Бюджет веса не проблема (1 из 2400/мин), проблема — незаметно завести второй
    источник истины в одном тике."""
    host = FakeHost(raw(doc(sym("NEWUSDT", NOW - 600_000))))
    tick(host, hostname="tokyo")
    assert host.fetches == 1


def test_the_poller_only_ever_writes_its_own_env_file():
    """Чужой env — это чужой инстанс сбора. План с любым другим путём — баг, и он
    обязан быть громким отказом, а не записью."""
    host = FakeHost(raw(doc()))
    bad = Plan(env_write=("/opt/hft-collector/etc/bybit.env", "COLLECTOR_SYMBOLS=X\n"))

    with pytest.raises(Day0Refusal):
        apply_plan(bad, State(), host)

    assert host.files == {}
    assert len(host.telegrams) == 1 and "bybit.env" in host.telegrams[0]


def test_the_only_systemctl_verbs_are_restart_and_own_timer_stop():
    """Поллер живёт от root. Всё, что ему нужно, — рестарт СВОЕГО инстанса и снятие
    СВОИХ transient-таймеров; enable/disable/mask — конфигурация загрузки, дело оператора."""
    assert_allowed_systemctl(("restart", INSTANCE_UNIT))
    assert_allowed_systemctl(("stop", "hft-day0-restart-AUSDT-1787000000000.timer"))

    for forbidden in (
        ("restart", "hft-collector@bybit.service"),
        ("stop", "hft-collector@binancefuturesum-day0.service"),
        ("enable", INSTANCE_UNIT),
        ("disable", INSTANCE_UNIT),
        ("mask", INSTANCE_UNIT),
        ("stop", "hft-heartbeat.timer"),
        ("daemon-reload",),
    ):
        with pytest.raises(Day0Refusal):
            assert_allowed_systemctl(forbidden)


def test_the_tg_token_is_read_at_send_time_and_never_persisted(tmp_path):
    """Секрет живёт в root:600 alert.env и читается в момент отправки: ни state, ни
    пульс, ни лог его не видят, а ротация токена не требует рестарта поллера."""
    alert_env = tmp_path / "alert.env"
    alert_env.write_text("# комментарий\nTG_BOT_TOKEN=111:AAA\nTG_CHAT_ID=-100777\n", encoding="utf-8")
    urls = []

    class _Resp:
        def read(self): return b"{}"
        def __enter__(self): return self
        def __exit__(self, *a): return False

    def opener(req, timeout=None):
        urls.append(req.full_url)
        return _Resp()

    host = RealHost(tmp_path, alert_env_path=str(alert_env), _open=opener)
    assert host.send_telegram("волна") is True
    assert urls[0].startswith("https://api.telegram.org/bot111:AAA/")

    alert_env.write_text("TG_BOT_TOKEN=222:BBB\nTG_CHAT_ID=-100777\n", encoding="utf-8")
    host.send_telegram("вторая волна")
    assert "222:BBB" in urls[1]  # прочитан заново, а не закеширован при старте

    assert read_alert_env(str(tmp_path / "missing.env")) == (None, None)

    # и ни один наш артефакт не несёт токена
    st = state_of(entry("AUSDT", NOW, PHASE_CAPTURING, anchor_ms=NOW))
    plan, _ = plan_tick(doc(sym("AUSDT", NOW)), st, NOW, env_now=env_text(["AUSDT"]))
    assert "111:AAA" not in dump_state(st) + (plan.pulse or "") + "".join(plan.alerts)


def test_the_wave_alert_records_local_and_server_time():
    """Единственный измерительный крюк по открытому вопросу: отстаёт ли вместе с
    serverTime само СОДЕРЖИМОЕ документа. Первый живой листинг это и измерит."""
    server = NOW - SERVER_LAG_MS
    plan, _ = plan_tick(doc(sym("NEWUSDT", NOW - 600_000), server_ms=server), State(), NOW,
                        hostname="tokyo")

    text = plan.alerts[0]
    assert day0_poller.iso_ms(NOW) in text
    assert day0_poller.iso_ms(server) in text
    assert "87." in text  # лаг назван числом, а не оставлен читателю
    assert json.loads(plan.pulse)["server_lag_s"] == pytest.approx(SERVER_LAG_MS / 1000)
