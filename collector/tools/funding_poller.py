#!/usr/bin/env python3
"""Поллер кросс-венью ставок фандинга: Hyperliquid, Lighter, aster, paradex (#88).

Зачем. Ретроспективной истории фандинга не отдаёт НИ ОДНА из этих площадок
(проверено в #73/#40) — каждый неснятый час потерян навсегда. Потребители:
#40-методика (фандинг как post-hoc стоимость на удержании), unlock-short
(HL платит шортам — попутный ветер), eventshort.

Binance сюда НЕ входит намеренно: его premiumIndex уже пишет Rust-коллектор
(`collector/src/binancefuturesum/mod.rs`, тик 10 с) — вторая запись была бы
дублем. У aster тот же Rust-поллер ЕСТЬ, но пишет только символы своего
инстанса захвата; venue-wide истории aster не существует — эта нога не дубль.

Контракт записи — контракт коллектора: сырые ответы площадки по ВСЕМ её
символам, без сведения и интерпретации на этапе захвата. Парсинг минимальный —
счётчик символов для лога/пульса и пол правдоподобия (усечённый ответ,
прочитанный как мир, оставил бы дыру в ряде молча). Формат: jsonl.gz на
площадку на сутки UTC, строка:

    {"t_local_ns": ..., "venue": ..., "endpoint": ..., "payload": <сырой ответ>}

Метка времени venue остаётся внутри payload там, где площадка её даёт (aster
`time`, paradex `created_at`); у Lighter её нет вовсе — поэтому `t_local_ns`
в конверте обязателен.

Каденс 5 минут: ставки меняются часами, 5 минут ловит путь predicted
(HL predictedFundings, aster nextFundingTime). Измерено 2026-08-20
(RECON.md задачи): суммарно ~247 КБ gz на тик ≈ 71 МБ/сут; лимиты не в игре
(aster вес 10 из 2400/мин, paradex 1 из 50/с, HL/Lighter без заголовков).

Fail-closed по НОГАМ: битый/пустой/усечённый ответ или отказ диска — провал
ноги, остальные пишутся как ни в чём не бывало. Серия из FAIL_ALERT_AFTER
провалов одной ноги подряд — собственный TG-алерт с именем ноги (счётчик живёт
в состоянии: тик — oneshot, прецедент params-поллера — 23 часа молчания).
Пульс пишется, если хоть одна нога жива, и несёт счётчики по всем ногам;
полная тишина пульса = все ноги мертвы либо мёртв сам поллер — её ловит
heartbeat (порог 20 мин = 3 пропущенных тика + запас).

Ретрая внутри тика нет намеренно (тот же выбор, что у day0): тик редкий, цена
провала — 5 минут ряда, а серия провалов алертит сама.

Устройство — day0-паттерн в миниатюре: чистые функции над состоянием, тонкий
слой Host, подменяемый в тестах; DryRunHost читает по-настоящему, но наружу
не пишет ничего.

Запуск: funding_poller.py --once [--dry-run]
        [--out-dir /opt/hft-collector/data/funding]
        [--pulse-dir /home/ubuntu/funding-data]
"""

from __future__ import annotations

import argparse
import dataclasses
import gzip
import json
import socket
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Protocol

USER_AGENT = "hft-collector-funding/1.0"
# Ответы 0.2–1.5 с (замерено пробой); таймаут щедрый — тик редкий, лучше
# дождаться, чем записать провал на медленном ответе.
HTTP_TIMEOUT_S = 30

# 6 провалов × 5 мин = полчаса тишины ноги: мигание сети — не событие.
FAIL_ALERT_AFTER = 6
# Пока провалы длятся — напоминание раз в ~6 часов, а не шторм.
FAIL_REALERT_EVERY = 72

DEFAULT_OUT_DIR = "/opt/hft-collector/data/funding"
DEFAULT_PULSE_DIR = "/home/ubuntu/funding-data"
ALERT_ENV_PATH = "/opt/hft-collector/etc/alert.env"

SCHEMA = 1


class FundingFetchError(RuntimeError):
    """Ответ площадки нечитаем или неправдоподобен — провал НОГИ, не тика."""


# ==============================================================================
# Ноги: что, откуда и как проверяется. Счётчики символов ТОТАЛЬНЫ: любая чужая
# форма — FundingFetchError, а не KeyError с трейсбеком в логе.
# ==============================================================================


def _count_hl_meta(doc: Any) -> int:
    """`[{"universe": [...]}, [ctx, ...]]`; ctx сшит с universe позиционно,
    расхождение длин = битый документ."""
    if (not isinstance(doc, list) or len(doc) != 2 or not isinstance(doc[0], dict)
            or not isinstance(doc[0].get("universe"), list)
            or not isinstance(doc[1], list)
            or len(doc[0]["universe"]) != len(doc[1])):
        raise FundingFetchError("metaAndAssetCtxs: чужая форма")
    return len(doc[1])


def _count_hl_predicted(doc: Any) -> int:
    """`[[symbol, [[venue, {...}], ...]], ...]`."""
    if not isinstance(doc, list) or not all(
            isinstance(row, list) and len(row) == 2 and isinstance(row[0], str)
            for row in doc):
        raise FundingFetchError("predictedFundings: чужая форма")
    return len(doc)


def _count_lighter(doc: Any) -> int:
    """`{"code": 200, "funding_rates": [...]}`; таблица уже кросс-venue."""
    if not isinstance(doc, dict) or not isinstance(doc.get("funding_rates"), list):
        raise FundingFetchError("funding-rates: чужая форма")
    return len(doc["funding_rates"])


def _count_aster(doc: Any) -> int:
    """Массив premiumIndex-элементов (бинанс-клон, без symbol = все разом)."""
    if not isinstance(doc, list) or not all(
            isinstance(row, dict) and "symbol" in row for row in doc):
        raise FundingFetchError("premiumIndex: чужая форма")
    return len(doc)


def _count_paradex(doc: Any) -> int:
    """`{"results": [...]}` — все рынки, включая опционы; фандинг у перпов."""
    if not isinstance(doc, dict) or not isinstance(doc.get("results"), list):
        raise FundingFetchError("markets/summary: чужая форма")
    return len(doc["results"])


@dataclass(frozen=True)
class Leg:
    """Одна нога тика: один HTTP-запрос, один файл назначения, один счётчик."""

    name: str        # имя ноги в состоянии, пульсе и алертах
    venue: str       # имя площадки = имя файла и поле `venue` записи
    endpoint: str    # поле `endpoint` записи (у HL два эндпоинта в одном файле)
    url: str
    body: bytes | None                 # None = GET, иначе POST application/json
    floor: int                         # пол правдоподобия по числу записей
    count: Callable[[Any], int]        # тотальный счётчик символов


# Формы и объёмы измерены живьём 2026-08-20 (RECON.md): HL 232 символа,
# lighter 723 строки (свои 212 + binance/bybit/hyperliquid — их зеркала НЕ
# выбрасываем, это бесплатная перекрёстная сверка), aster 702, paradex 1697
# записей (62 перпа, остальное опционы — пишем как есть). Полы — примерно
# четверть измеренного: ниже — это уже не «universe похудел», а битое
# происхождение.
LEGS: tuple[Leg, ...] = (
    Leg("hyperliquid.metaAndAssetCtxs", "hyperliquid", "metaAndAssetCtxs",
        "https://api.hyperliquid.xyz/info",
        json.dumps({"type": "metaAndAssetCtxs"}).encode(), 50, _count_hl_meta),
    Leg("hyperliquid.predictedFundings", "hyperliquid", "predictedFundings",
        "https://api.hyperliquid.xyz/info",
        json.dumps({"type": "predictedFundings"}).encode(), 50, _count_hl_predicted),
    Leg("lighter.funding-rates", "lighter", "funding-rates",
        "https://mainnet.zklighter.elliot.ai/api/v1/funding-rates",
        None, 100, _count_lighter),
    Leg("aster.premiumIndex", "aster", "premiumIndex",
        "https://fapi.asterdex.com/fapi/v1/premiumIndex",
        None, 100, _count_aster),
    Leg("paradex.markets-summary", "paradex", "markets-summary",
        "https://api.prod.paradex.trade/v1/markets/summary?market=ALL",
        None, 100, _count_paradex),
)


# ==============================================================================
# Состояние: счётчики провалов по ногам. Живут на диске — тик oneshot.
# ==============================================================================


@dataclass
class LegState:
    consecutive_failures: int = 0
    alerted: bool = False


@dataclass
class State:
    legs: dict[str, LegState] = field(default_factory=dict)


def load_state(text: str | None) -> State:
    """Битое или отсутствующее состояние читается как «ничего не знаем»:
    в худшем случае продублируется один алерт (прецедент positions-поллера)."""
    if not text:
        return State()
    try:
        doc = json.loads(text)
        if not isinstance(doc, dict) or doc.get("schema") != SCHEMA:
            return State()
        raw = doc.get("legs")
        if not isinstance(raw, dict):
            return State()
        return State(legs={
            name: LegState(int(v.get("consecutive_failures", 0)),
                           bool(v.get("alerted", False)))
            for name, v in raw.items() if isinstance(v, dict)
        })
    except (json.JSONDecodeError, TypeError, ValueError):
        return State()


def dump_state(state: State) -> str:
    return json.dumps(
        {"schema": SCHEMA,
         "legs": {n: dataclasses.asdict(s) for n, s in sorted(state.legs.items())}},
        ensure_ascii=False, indent=1)


# ==============================================================================
# Чистая логика тревог: (state, исходы) → (алерты, новое state)
# ==============================================================================


@dataclass(frozen=True)
class LegOutcome:
    ok: bool
    symbols: int | None = None
    reason: str = ""


def advance(state: State, outcomes: dict[str, LegOutcome],
            hostname: str) -> tuple[list[str], State]:
    alerts: list[str] = []
    new = State(legs={})
    for name, out in outcomes.items():
        prev = state.legs.get(name, LegState())
        if out.ok:
            if prev.alerted:
                alerts.append(
                    f"🟢 {hostname}: funding-поллер — нога {name} восстановилась "
                    f"после {prev.consecutive_failures} провалов подряд")
            new.legs[name] = LegState(0, False)
            continue
        n = prev.consecutive_failures + 1
        due = n == FAIL_ALERT_AFTER or (
            n > FAIL_ALERT_AFTER and (n - FAIL_ALERT_AFTER) % FAIL_REALERT_EVERY == 0)
        if due:
            alerts.append(
                f"🔴 {hostname}: funding-поллер — нога {name} не пишется, "
                f"{n} провалов подряд. Последняя причина: {out.reason[:200]}")
        new.legs[name] = LegState(n, prev.alerted or due)
    return alerts, new


# ==============================================================================
# Исполняющий слой
# ==============================================================================


class Host(Protocol):
    """Шов между решениями и миром. В тестах — записывающая подделка."""

    state_path: str

    def now_ns(self) -> int: ...
    def fetch(self, leg: Leg) -> bytes: ...
    def read_file(self, path: str) -> str | None: ...
    def write_atomic(self, path: str, text: str) -> None: ...
    def append_jsonl(self, venue: str, day: str, line: str) -> None: ...
    def append_pulse(self, line: str) -> None: ...
    def send_telegram(self, text: str) -> bool: ...


def read_alert_env(path: str = ALERT_ENV_PATH) -> tuple[str | None, str | None]:
    """(токен, чат) из того же alert.env, что у alert.sh/heartbeat.sh (root:600).
    Читается в момент отправки: секрет не оседает ни в состоянии, ни в логе."""
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
    """Настоящий хост: сеть и диск. Никаких решений — только исполнение."""

    def __init__(self, out_dir: str | Path, pulse_dir: str | Path, *,
                 alert_env_path: str = ALERT_ENV_PATH,
                 _open: Callable[..., Any] = urllib.request.urlopen,
                 _now_ns: Callable[[], int] | None = None) -> None:
        self.out_dir = Path(out_dir)
        self.pulse_dir = Path(pulse_dir)
        self.state_path = str(self.pulse_dir / "state.json")
        self._alert_env_path = alert_env_path
        self._open = _open
        self._now_ns = _now_ns or time.time_ns

    def now_ns(self) -> int:
        return self._now_ns()

    def fetch(self, leg: Leg) -> bytes:
        """Один запрос ноги. gzip запрашивается всегда, а разжимается по
        Content-Encoding: aster/paradex отдают gzip (8×/5.8× экономии на
        проводе), HL/Lighter отвечают identity — измерено 2026-08-20."""
        headers = {"User-Agent": USER_AGENT, "Accept-Encoding": "gzip"}
        if leg.body is not None:
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(leg.url, data=leg.body, headers=headers)
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
        tmp.replace(p)

    def append_jsonl(self, venue: str, day: str, line: str) -> None:
        """gzip-append: каждый тик — свой gzip-член, zcat читает подряд.
        Тот же приём, что у positions-поллера."""
        self.out_dir.mkdir(parents=True, exist_ok=True)
        with gzip.open(self.out_dir / f"{venue}-{day}.jsonl.gz", "at",
                       encoding="utf-8") as fh:
            fh.write(line + "\n")

    def append_pulse(self, line: str) -> None:
        """Пульс — ИМЕННО gzip-файл не глубже второго уровня: heartbeat ищет
        свежесть через `find -maxdepth 2 -name '*.gz'`."""
        self.pulse_dir.mkdir(parents=True, exist_ok=True)
        day = datetime.fromtimestamp(self.now_ns() // 1_000_000_000,
                                     timezone.utc).strftime("%Y%m%d")
        with gzip.open(self.pulse_dir / f"pulse-{day}.gz", "at",
                       encoding="utf-8") as fh:
            fh.write(line + "\n")

    def send_telegram(self, text: str) -> bool:
        token, chat = read_alert_env(self._alert_env_path)
        if not token or not chat:
            print("funding_poller: TG не настроен; сообщение только в журнал:\n"
                  + text, flush=True)
            return False
        data = urllib.parse.urlencode({"chat_id": chat, "text": text}).encode()
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data=data, headers={"User-Agent": USER_AGENT})
        try:
            with self._open(req, timeout=15) as resp:
                resp.read()
            return True
        except (OSError, ValueError) as exc:
            # Только класс ошибки: её текст может нести URL, а в URL — токен.
            print(f"funding_poller: TG не доставлен ({type(exc).__name__})",
                  flush=True)
            return False


class DryRunHost:
    """Читает по-настоящему (сеть, состояние), наружу не пишет ничего."""

    def __init__(self, inner: Host) -> None:
        self._inner = inner
        self.state_path = inner.state_path

    def now_ns(self) -> int:
        return self._inner.now_ns()

    def fetch(self, leg: Leg) -> bytes:
        return self._inner.fetch(leg)

    def read_file(self, path: str) -> str | None:
        return self._inner.read_file(path)

    def write_atomic(self, path: str, text: str) -> None:
        print(f"--- DRY RUN, записал бы {path}:\n{text}", flush=True)

    def append_jsonl(self, venue: str, day: str, line: str) -> None:
        print(f"--- DRY RUN, дописал бы {venue}-{day}.jsonl.gz: "
              f"{len(line)} байт, {line[:120]}…", flush=True)

    def append_pulse(self, line: str) -> None:
        print(f"--- DRY RUN, пульс: {line}", flush=True)

    def send_telegram(self, text: str) -> bool:
        print(f"--- DRY RUN, отправил бы:\n{text}", flush=True)
        return True


# ==============================================================================
# Тик
# ==============================================================================


def _run_leg(host: Host, leg: Leg) -> LegOutcome:
    """Одна нога: запрос → минимальный парсинг → пол → запись. Любой провал —
    провал ноги; ошибка записи (диск) — тоже провал ноги, а не тика."""
    t_ns = host.now_ns()
    try:
        raw = host.fetch(leg)
        doc = json.loads(raw.decode("utf-8"))
        n = leg.count(doc)
        if n < leg.floor:
            raise FundingFetchError(f"записей {n} < пола {leg.floor}")
        line = json.dumps(
            {"t_local_ns": t_ns, "venue": leg.venue, "endpoint": leg.endpoint,
             "payload": doc},
            ensure_ascii=False, separators=(",", ":"))
        day = datetime.fromtimestamp(t_ns // 1_000_000_000,
                                     timezone.utc).strftime("%Y%m%d")
        host.append_jsonl(leg.venue, day, line)
        return LegOutcome(ok=True, symbols=n)
    except Exception as exc:  # noqa: BLE001 — тотально, и это решение
        # Перечисление классов здесь было БАГОМ, а не строгостью: провод рвётся
        # и тем, что не наследует OSError — `http.client.IncompleteRead`
        # (оборванное тело), `EOFError` и `zlib.error` (усечённый или побитый
        # gzip; gzip получают ровно aster и paradex). Любой из них уносил весь
        # тик: остальные ноги не опрашивались, состояние не писалось — значит
        # счётчик провалов не рос и алерт по ноге не наступал НИКОГДА.
        # Провал не проглочен: класс ошибки едет в строку лога, в состояние и
        # через FAIL_ALERT_AFTER тиков — в Telegram. BaseException (KeyboardInterrupt,
        # SystemExit) сюда намеренно не попадает.
        return LegOutcome(ok=False, reason=f"{type(exc).__name__}: {exc}")


def tick(host: Host, *, hostname: str = "") -> str:
    """Один тик: все ноги, потом тревоги, потом состояние, потом пульс.
    Пульс последним и только при живой ноге: его тишина — второй, независимый
    от Telegram канал тревоги (heartbeat, порог 20 мин)."""
    state = load_state(host.read_file(host.state_path))
    outcomes = {leg.name: _run_leg(host, leg) for leg in LEGS}

    alerts, new_state = advance(state, outcomes, hostname)
    for text in alerts:
        host.send_telegram(text)
    host.write_atomic(host.state_path, dump_state(new_state))

    if any(o.ok for o in outcomes.values()):
        host.append_pulse(json.dumps(
            {"t_ns": host.now_ns(),
             "legs": {name: {"ok": o.ok, "symbols": o.symbols,
                             "consecutive_failures":
                                 new_state.legs[name].consecutive_failures}
                      for name, o in outcomes.items()}},
            ensure_ascii=False, separators=(",", ":")))

    parts = []
    for leg in LEGS:
        o = outcomes[leg.name]
        parts.append(f"{leg.name}={o.symbols}" if o.ok
                     else f"{leg.name}=ПРОВАЛ({o.reason[:80]})")
    return " ".join(parts)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Поллер кросс-венью ставок фандинга (HL/Lighter/aster/paradex)")
    ap.add_argument("--out-dir", default=DEFAULT_OUT_DIR,
                    help="куда пишутся jsonl.gz по площадкам (волюм данных)")
    ap.add_argument("--pulse-dir", default=DEFAULT_PULSE_DIR,
                    help="каталог пульса и состояния (его смотрит heartbeat)")
    ap.add_argument("--once", action="store_true", help="один тик и выход (для таймера)")
    ap.add_argument("--interval-s", type=int, default=300)
    ap.add_argument("--dry-run", action="store_true",
                    help="читать по-настоящему, но ничего не писать и никого не будить")
    args = ap.parse_args(argv)

    host: Host = RealHost(args.out_dir, args.pulse_dir)
    if args.dry_run:
        host = DryRunHost(host)
    name = socket.gethostname().split(".")[0]

    while True:
        began = time.time()
        stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        print(f"{stamp} {tick(host, hostname=name)}", flush=True)
        if args.once:
            return 0
        time.sleep(max(0.0, args.interval_s - (time.time() - began)))


if __name__ == "__main__":
    sys.exit(main())
