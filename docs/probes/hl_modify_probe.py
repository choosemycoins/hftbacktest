#!/usr/bin/env python3
"""Пробник HL testnet: что биржа сообщает при modify / batchModify (MON-4).

Дизайн и вопросы Q1–Q13 — docs/design-live-modify.md §9. Шлёт РЕАЛЬНЫЕ (тестнет) действия:
запускать только после ревью. CI его не запускает.

Режимы
  --self-test     офлайн: только стандартная библиотека (Python ≥ 3.10; без SDK, ключей и сети) —
                  гардрейлы, чистые функции и весь план S1–S21 на офлайн-бирже в процессе
                  (fake_hl_venue.py рядом).
  --golden        офлайн, нужен SDK: golden msgpack/подписи batchModify для Rust-тестов (T21, T22,
                  T50 — с expiresAfter и без).
  --dry-run       только /info (чтение): префлайт и план; ничего не подписывает и не шлёт.
  --sweep-only    снять оставшиеся ордера пробника (любого прогона) на монете — и только их.
  --fake-venue P  весь боевой путь против офлайн-фейка на 127.0.0.1:P (fake_hl_server.py:
                  HTTP + WS, подпись проверяется); ключ — публичный Hardhat #0.
  (без флага)     префлайт, добор хвостов, план S1–S21 на тестнете, очистка, сверка филлов/позиции.

Гардрейлы — в коде (assert_venue_url, refuse_mainnet_config, check_sdk, Ledger.owns,
check_order_guardrails, check_cross_guardrails, assert_action_safe, Budget, Probe.observe,
list_open_orders, sweep_targets, sweep, Stopper, say, acquire_lock, shared_wallet_conflicts):
  * только хост api.hyperliquid-testnet.xyz (с --fake-venue — только 127.0.0.1); подпись
    is_mainnet=False; ни HTTP (/exchange и /info), ни WS не идут по редиректам; файл ключей,
    упоминающий мейннет, — отказ; SDK — ровно 0.24.0 и обязан воспроизвести golden ордера из
    exchange.rs (check_sdk — в начале прогона, до первого запроса); часы не дальше 3 с от биржи
    (Q12) — иначе отказ;
  * только свои ордера: cloid = 0x0dd1 + 8 hex метки + 16 нулей + 4 hex счётчика; последний
    рубеж перед подписью (assert_action_safe) пускает цель правки и cancelByCloid только из
    реестра прогона, cancel по oid — только oid, который сам список биржи показывает с cloid этой
    формы на монете пробника; добор и очистка снимают только такие строки — oid из реестра (его
    кладёт ответ биржи, а он может соврать) целью не делает; cancel-all нет;
  * только ALO-покупки, цена ≤ 95 % от min(mid, best bid) по l2Book, перечитанному прямо перед
    каждым POST (после любой паузы); S1–S17 — 88–91 %, C4 и его правки (S18–S20) — не выше 92 %
    (цену там задаёт целое число лотов на $10.05–11: на тестнет-BTC ≈ 90 / 84 / 78 %).
    Единственное исключение — Q13 (S21): ALO-правка своей покупки на [best ask, best ask · 1.002],
    notional $10–11; биржа обязана отказать; встала — аборт по гардрейлу 5 %, исполнилась — аборт
    по филлу; дальше очистка;
  * notional ≤ $15 (план $10–14; намеренный ~$5 в S15/S16 — биржа обязана отказать);
  * бюджет — в запросах биржи, не в POST (batchModify×2 = 2; просроченное действие — 5):
    план 26 (+2 запаса), очистка 6, добор хвостов 3 — потолок прогона 37 (RUN_CEILING, одобрен
    лидом 06.10, нота §14 Р1); пулы раздельные, счёт до подписи, после шага — доначисление, если
    биржа списала больше;
  * аборт с очисткой на неожиданном ответе/состоянии (Probe.check_answer, Probe.observe) и на
    любом сигнале, который по умолчанию завершил бы процесс и приходит снаружи (Stopper.SIGNALS:
    INT/TERM/HUP/QUIT, USR1/USR2, ALRM/VTALRM/PROF, IO, PWR, XCPU/XFSZ); сигнал во время очистки
    её не прерывает; очистка принимает только список ордеров правильной формы, повторяет /info и
    ограничена по времени; «чисто» — два пустых списка подряд; хвосты прошлых прогонов
    снимаются в начале и в конце;
  * свёртка (очистка, сверка, запись результатов, печать) — каждый шаг сам по себе: сбой одного
    не отменяет следующих; печать не роняет её и после потери терминала (вывод уходит в
    <UTC>_hl_modify_probe_console.log рядом с результатами); код 4 — очистку не удалось проверить
    (или ответ биржи приписал прогону ордер, который список показывает без cloid: проверить руками);
  * один прогон за раз (flock); результаты — вне репозитория (~/.local/state/hl_modify_probe);
  * ключ — только из --config (TOML), не печатается и не пишется;
  * отказ, если работающий HL-коннектор подписывает тем же файлом ключей или тем же
    API-кошельком; предупреждение — если его файл ключей не определить или он на том же счёте.

Ключи (--config, режим 600): api_wallet_private_key, master_account_address
[, api_wallet_address — для сверки]. Нужен ОТДЕЛЬНЫЙ тестнет API-кошелёк, одобренный мастером.

Запуск (из корня репозитория; SDK закреплён — на 0.24.0 проверен --golden). Сеть нужна только
первому `$R` (uv ставит окружение из PyPI) и самому прогону; всё офлайн — под `unshare -rn`:
  python3 -B docs/probes/hl_modify_probe.py --self-test         # офлайн, только stdlib
  R="uv run --no-project --python 3.11 --with hyperliquid-python-sdk==0.24.0 --with coincurve --with pytest"
  $R python -c "import hyperliquid"                             # СЕТЬ: PyPI, ставит окружение в кэш uv
  unshare -rn sh -c "ip link set lo up && HL_PROBE_REQUIRE_NETNS=1 uv run --offline ${R#uv run } python -B -m pytest -q -p no:cacheprovider docs/probes"
  unshare -rn uv run --offline ${R#uv run } python -B docs/probes/hl_modify_probe.py --golden
  $R python -B docs/probes/hl_modify_probe.py --config <creds.toml> --dry-run
  $R python -B docs/probes/hl_modify_probe.py --config <creds.toml>
  $R python -B docs/probes/hl_modify_probe.py --config <creds.toml> --sweep-only   # после kill -9 / кода 4
"""
from __future__ import annotations

import argparse
import email.utils
import fcntl
import importlib.util
import json
import math
import os
import re
import secrets
import signal
import stat
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlparse


def _toml():
    """Парсер TOML — лениво: --self-test обходится стандартной библиотекой и на Python 3.10."""
    try:
        import tomllib
    except ModuleNotFoundError:  # Python 3.10: тот же парсер под старым именем
        import tomli as tomllib
    return tomllib


# --- Константы ----------------------------------------------------------------------------

TESTNET_HOST = "api.hyperliquid-testnet.xyz"
BASE_URL = f"https://{TESTNET_HOST}"
WS_URL = f"wss://{TESTNET_HOST}/ws"
# Подстрока, которой нет в тестнет-хосте ("hyperliquid-testnet.xyz"), но есть в мейннете.
MAINNET_MARKER = "api.hyperliquid.xyz"
LOOPBACK = "127.0.0.1"
SDK_PIN = "0.24.0"

PROBE_PREFIX = "0dd1"  # 4 hex; у коннектора по умолчанию a1f0
# Полная форма cloid пробника: случайный cloid совпадёт с ней с вероятностью 2^-80.
PROBE_CLOID_RE = re.compile(r"0x0dd1[0-9a-f]{8}0{16}[0-9a-f]{4}")
MAX_MINTS = 0xFFFF

MIN_DISTANCE = 0.05  # цена покупки ≤ (1 - 5 %) · min(mid, best bid)
MAX_NOTIONAL = 15.0
VENUE_MIN_NOTIONAL = 10.0  # правило биржи; ниже — отказ
NOTIONAL_FLOOR = 10.05  # запас над минимумом: округление не уведёт ниже $10
TARGET_NOTIONAL = 13.0
DOWN_NOTIONAL = 11.0
BAD_NOTIONAL = 5.0  # намеренно ниже минимума биржи (Q10)
SMALL_MAX_FRACTION = 0.92  # C4 и его правки (S18–S20): ≤ $11, не ближе 8 % к рынку
# Q12: expiresAfter = подпись + 10 с — как пошлёт коннектор (EXCHANGE_TIMEOUT, нота §5.1);
# просроченное — на 5 с в прошлом. Часы сверяются с биржей в префлайте по заголовку Date ответов
# /info (свежий на каждом ответе; `time` книги на тихом тестнете может стоять) — MAX_CLOCK_SKEW_MS.
EXPIRY_S = 10.0
EXPIRED_BY_S = 5.0
MAX_CLOCK_SKEW_MS = 3000
# Q13: правка C4 через спред — на best ask · (1 + 0.1 %) вверх на сетку, потолок — ask · 1.002;
# notional $10–11: выше минимума биржи (иначе отказ по минимуму заслонил бы ответ ALO), худший
# исход — тестнет-филл не больше $11 (решение лида).
CROSS_OVER = 0.001
CROSS_MAX_OVER = 0.002
CROSS_MAX_NOTIONAL = 11.0
CROSS_MIN_NOTIONAL = 10.02  # запас над минимумом: если биржа меряет его не по лимитной цене
MAX_OPEN_RUN_ORDERS = 3

# Бюджет — в запросах биржи (nRequestsUsed): ордер/отмена/batchModify из n — n, modify — 1 [F1];
# действие, отвергнутое за просроченный expiresAfter, — 5× (документация HL; Q12 это меряет).
EXPIRED_COST = 5
PLAN_COSTS = {
    "S1": 1, "S2": 1, "S3": 1, "S4": 1, "S5": 1, "S6": 2, "S7": 1, "S8": 1, "S9": 1, "S10": 1,
    "S11": 1, "S12": 1, "S13": 1, "S14": 1, "S15": 1, "S16": 1, "S17": 1, "S18": 1,
    "S19": 1, "S20": EXPIRED_COST, "S21": 1,
}
PLAN_ACTIONS = len(PLAN_COSTS)  # шагов (= подписанных POST) в плане
PLAN_SLACK = 2  # на доначисление по замеру (чужая активность счёта), чтобы шум не съел S21
PLAN_BUDGET = sum(PLAN_COSTS.values()) + PLAN_SLACK  # 26 + 2
CLEANUP_BUDGET = 6  # два полных круга по MAX_OPEN_RUN_ORDERS
SWEEP_BUDGET = 3  # хвосты прошлых прогонов в начале; больше — отказ, сначала --sweep-only
SWEEP_ONLY_BUDGET = 12
# Потолок одного прогона в запросах биржи; 37 одобрен лидом 06.10 (тестнет; нота §9, §14 Р1).
RUN_CEILING = PLAN_BUDGET + CLEANUP_BUDGET + SWEEP_BUDGET  # 28 + 6 + 3 = 37
ALLOWED_ACTIONS = ("order", "modify", "batchModify", "cancel", "cancelByCloid")
WIRE_KEYS = ("a", "b", "p", "s", "r", "t", "c")

# Тексты, после которых план не имеет смысла или опасен, — узко, известными формами. Широкие
# «nonce» / «rate limit» оборвали бы план на неизмеренном отказе S20 (просрочка) или S21 (ALO)
# до Q13. Сравнение без регистра, подстрокой по тексту ответа; до проверки expect.
ABORT_TEXTS = (
    "too many cumulative requests",  # адресный кап (econ, F1)
    "user or api wallet",  # «… does not exist»: не тот подписант / агент не одобрен (exchange.rs)
    "insufficient margin",  # exchange.rs
    "invalid nonce",  # документация HL; текст не мерен
)
# Широкие слова — не аборт, а пометка шага (`text_flags`): отказ, принятый за ответ (S15/S16,
# S20, S21 и `expect=any`), мог быть лимитом или nonce, а не ответом на вопрос — читать глазами.
FLAG_TEXTS = ("nonce", "rate limit", "too many")

# Golden: тот же ордер и тот же Hardhat-ключ, что в signing.rs / exchange.rs.
HARDHAT_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
HARDHAT_ADDRESS_1 = "0x70997970c51812dc3a010c7d01b50e0d17dc79c8"  # подмена адреса в рамках
FAKE_MASTER = "0x3c44cdddb6a900fa2b585dd299e03d12fa4293bc"  # Hardhat #2 — мастер fake_hl_server
GOLDEN_CLOID = "0xa1b2000000000000000000000000dead"
GOLDEN_OID = 57118842019
GOLDEN_NONCE = 1_700_000_000_002
GOLDEN_EXPIRES_AFTER = GOLDEN_NONCE + 10_000  # подпись + EXCHANGE_TIMEOUT, нота §5.1 (T50)
# exchange.rs::msgpack_bytes_match_the_official_sdk, order_alo_cloid — контроль среды SDK.
ORDER_MSGPACK_ANCHOR = (
    "83a474797065a56f72646572a66f72646572739187a16103a162c3a170a53330303030a173a5302e303031"
    "a172c2a17481a56c696d697481a3746966a3416c6fa163d9223078613162323030303030303030303030"
    "3030303030303030303030303064656164a867726f7570696e67a26e61"
)


@dataclass(frozen=True)
class Timing:
    pace_s: float  # минимум между действиями
    over_cap_pace_s: float  # сверх капа биржа пропускает одно не-отменное действие в 10 с
    observe_s: float  # окно сбора orderUpdates после ответа
    http_timeout_s: float
    ws_ready_s: float
    settle_s: float  # очистка: повторный взгляд — ордер после сбоя транспорта ложится позже
    retry_s: float  # пауза между повторами /info
    cleanup_deadline_s: float  # очистка заканчивается всегда
    stamp_skew_ms: int  # запись orderUpdates старше POST шага на столько — от прошлого шага


REAL = Timing(2.0, 11.0, 4.0, 15.0, 10.0, 2.0, 1.0, 120.0, 1000)
FAST = Timing(0.02, 1.1, 0.35, 5.0, 5.0, 0.3, 0.1, 30.0, 0)  # fake_hl_server на loopback


class Refuse(Exception):
    """Префлайт-отказ: ничего не отправлено."""


class Abort(Exception):
    """Остановить план, снять свои ордера, выйти с ненулевым кодом."""


class Guardrail(Abort):
    """Действие нарушило бы гардрейл — оно не отправляется."""


class Interrupted(Abort):
    """Сигнал: план остановлен в безопасной точке, дальше — очистка."""


# --- Чистые функции (покрыты --self-test и тестами рядом) ---------------------------------


def assert_testnet_url(url: str) -> None:
    """Только https/wss на тестнет-хост, порт по умолчанию. Всё прочее — отказ."""
    u = urlparse(url)
    host = (u.hostname or "").lower()
    if u.scheme not in ("https", "wss") or host != TESTNET_HOST or u.port not in (None, 443):
        raise Refuse(f"not the Hyperliquid testnet endpoint: {url!r}")
    if u.username or u.password:
        raise Refuse(f"credentials in the URL: {url!r}")


@dataclass(frozen=True)
class Venue:
    rest: str
    ws: str
    fake: bool = False


TESTNET = Venue(BASE_URL, WS_URL)


def fake_venue(port) -> Venue:
    """Офлайн-фейк (fake_hl_server.py) — только 127.0.0.1; другого адреса этот режим не знает."""
    if isinstance(port, bool) or not isinstance(port, int) or not 0 < port < 65536:
        raise Refuse(f"--fake-venue needs a TCP port, got {port!r}")
    return Venue(f"http://{LOOPBACK}:{port}", f"ws://{LOOPBACK}:{port}/ws", fake=True)


def assert_venue_url(url: str, venue: Venue) -> None:
    if not venue.fake:
        assert_testnet_url(url)
        return
    u = urlparse(url)
    if u.scheme not in ("http", "ws") or u.hostname != LOOPBACK or u.username or u.password:
        raise Refuse(f"the fake venue lives on {LOOPBACK} only: {url!r}")


def _leaves(value, key="<root>"):
    if isinstance(value, dict):
        for k, v in value.items():
            yield from _leaves(v, k)
    elif isinstance(value, list):
        for v in value:
            yield from _leaves(v, key)
    else:
        yield key, value


def refuse_mainnet_config(cfg: dict) -> None:
    """Отказ, если файл где-то говорит о мейннете — признак перепутанного файла.

    Сам тестнет держится не этим, а константой хоста и is_mainnet=False в sign().
    Значение в сообщение не попадает."""
    for key, value in _leaves(cfg):
        if key == "is_mainnet" and value is True:
            raise Refuse("the config says is_mainnet = true")
        if isinstance(value, str) and MAINNET_MARKER in value.lower():
            raise Refuse(f"config key {key!r} points at mainnet")


def is_abort_text(text: str) -> bool:
    t = (text or "").lower()
    return any(marker in t for marker in ABORT_TEXTS)


def text_flags(text: str) -> list[str]:
    """Широкие слова в тексте ответа, которые не аборт, но делают отказ подозрительным."""
    t = (text or "").lower()
    return [marker for marker in FLAG_TEXTS if marker in t]


def make_run_prefix(run_tag: int) -> str:
    """0x + 4 hex пробника + 8 hex метки прогона; остальные 20 hex — счётчик."""
    return f"0x{PROBE_PREFIX}{run_tag & 0xFFFFFFFF:08x}"


def is_probe_cloid(cloid) -> bool:
    """Cloid, выпущенный этим пробником в любом прогоне, — и ничем другим."""
    return isinstance(cloid, str) and PROBE_CLOID_RE.fullmatch(cloid.lower()) is not None


def _dec(x) -> Decimal | None:
    try:
        return Decimal(str(x)).normalize()
    except Exception:  # noqa: BLE001
        return None


def action_items(action: dict) -> int:
    """Ордеров в действии — столько запросов бюджета оно стоит [F1]."""
    t = action.get("type")
    if t == "order":
        return len(action["orders"])
    if t == "batchModify":
        return len(action["modifies"])
    if t in ("cancel", "cancelByCloid"):
        return len(action["cancels"])
    if t == "modify":
        return 1
    raise Guardrail(f"action type {t!r} is not allowed")


def venue_cost(action: dict, *, stale: bool = False) -> int:
    """Запросы бюджета биржи, которые действие спишет (stale: expiresAfter уже в прошлом)."""
    return action_items(action) * (EXPIRED_COST if stale else 1)


def action_wires(action: dict) -> list[dict]:
    t = action.get("type")
    if t == "order":
        return list(action["orders"])
    if t == "batchModify":
        return [m["order"] for m in action["modifies"]]
    if t == "modify":
        return [action["order"]]
    return []


class Budget:
    """Раздельные пулы в запросах биржи; списание — до подписи, доначисление — по замеру."""

    LIMITS = {"plan": PLAN_BUDGET, "cleanup": CLEANUP_BUDGET, "sweep": SWEEP_BUDGET}

    def __init__(self, limits: dict | None = None):
        self.limits = dict(limits or self.LIMITS)
        self.spent = {k: 0 for k in self.limits}

    def room(self, pool: str) -> int:
        return self.limits.get(pool, 0) - self.spent.get(pool, 0)

    def charge(self, pool: str, cost: int) -> None:
        if pool not in self.limits:
            raise Guardrail(f"no budget pool {pool!r}")
        if isinstance(cost, bool) or not isinstance(cost, int) or cost <= 0:
            raise Guardrail(f"bad cost {cost!r}")
        if self.spent[pool] + cost > self.limits[pool]:
            raise Guardrail(f"{pool} budget: {self.spent[pool]} + {cost} > {self.limits[pool]} venue requests")
        self.spent[pool] += cost

    def true_up(self, pool: str, extra: int) -> None:
        """Биржа списала больше предсказанного: доначислить, чтобы потолок держал факт."""
        if pool in self.spent and isinstance(extra, int) and extra > 0:
            self.spent[pool] += extra


class Ledger:
    """Реестр прогона: что пробник выпустил, что просил и какие oid биржа ему приписала."""

    def __init__(self, run_prefix: str):
        self.run_prefix = run_prefix.lower()
        self.cloids: set[str] = set()
        self.oids: set[int] = set()
        self.chains: dict[str, list[int]] = {}
        self.requested: dict[str, list[tuple[Decimal, Decimal]]] = {}
        self.n = 0

    def mint(self) -> str:
        self.n += 1
        if self.n > MAX_MINTS:
            raise Guardrail("cloid counter exhausted")
        cloid = f"{self.run_prefix}{self.n:020x}"
        if not is_probe_cloid(cloid):
            raise Guardrail(f"malformed cloid {cloid}")
        self.cloids.add(cloid)
        return cloid

    def is_run_cloid(self, cloid) -> bool:
        return isinstance(cloid, str) and cloid.lower().startswith(self.run_prefix)

    def note(self, cloid, oid) -> None:
        """Записывает oid, если он пришёл вместе с cloid этого прогона."""
        if isinstance(oid, bool) or not isinstance(oid, int) or not self.is_run_cloid(cloid):
            return
        self.oids.add(oid)
        chain = self.chains.setdefault(cloid.lower(), [])
        if oid not in chain:
            chain.append(oid)

    def note_answer(self, action: dict, statuses: list) -> None:
        """Ответ на НАШЕ действие: statuses[i] — про wire i. Oid приписывается по позиции, даже
        если биржа не вернула cloid: правка, сбросившая cloid, не уведёт ордер из очистки."""
        for wire, st in zip(action_wires(action), statuses):
            inner = st.get(status_kind(st)) if isinstance(st, dict) else None
            if isinstance(inner, dict):
                self.note(wire.get("c"), inner.get("oid"))
                self.note(inner.get("cloid"), inner.get("oid"))

    def note_request(self, action: dict) -> None:
        """Что просили: (p, s) — под cloid в wire и под cloid цели правки (биржа может оставить
        старый cloid при новом `c`, Q8, — это ответ, а не «цена, которой не просили»)."""
        t = action.get("type")
        targets = ([action.get("oid")] if t == "modify" else
                   [m.get("oid") for m in action.get("modifies", [])] if t == "batchModify" else [])
        for i, wire in enumerate(action_wires(action)):
            asked = (_dec(wire["p"]), _dec(wire["s"]))
            keys = {str(wire.get("c") or "").lower()}
            target = targets[i] if i < len(targets) else None
            if isinstance(target, str):
                keys.add(target.lower())
            elif isinstance(target, int) and not isinstance(target, bool):
                keys.update(c for c, chain in self.chains.items() if target in chain)
            for c in keys & self.cloids:
                self.requested.setdefault(c, []).append(asked)

    def size_verdict(self, cloid: str, px, sz) -> str:
        """ok — цена и объём из запрошенных; partial — цена наша, объёма меньше (частичный
        филл); unexpected — ни то ни другое."""
        asked = self.requested.get((cloid or "").lower(), [])
        p, s = _dec(px), _dec(sz)
        if (p, s) in asked:
            return "ok"
        if s is not None and any(p == ap and as_ is not None and s < as_ for ap, as_ in asked):
            return "partial"
        return "unexpected"

    def owns(self, target) -> bool:
        if isinstance(target, bool):
            return False
        if isinstance(target, int):
            return target in self.oids
        if isinstance(target, str):
            return target.lower() in self.cloids
        return False


def check_order_guardrails(px, sz, ref, *, tif, is_buy, allow_below_venue_min=False) -> None:
    if tif != "Alo":
        raise Guardrail("only post-only (Alo) orders")
    if is_buy is not True:
        raise Guardrail("only buys")
    if not (math.isfinite(px) and math.isfinite(sz) and px > 0 and sz > 0):
        raise Guardrail(f"price {px} / size {sz} is not a positive number")
    if not (math.isfinite(ref) and ref > 0):
        raise Guardrail("no reference price")
    if px > (1.0 - MIN_DISTANCE) * ref:
        raise Guardrail(f"price {px} is within {MIN_DISTANCE:.0%} of the reference {ref}")
    notional = px * sz
    if notional > MAX_NOTIONAL:
        raise Guardrail(f"notional ${notional:.2f} is above ${MAX_NOTIONAL}")
    if notional < VENUE_MIN_NOTIONAL and not allow_below_venue_min:
        raise Guardrail(f"notional ${notional:.2f} is below the venue minimum")


def check_cross_guardrails(px, sz, book, *, tif, is_buy) -> None:
    """Только Q13 (S21): ALO-покупка на/через тач, которую биржа обязана отвергнуть.

    Узкое исключение из правила 5 %: цена в [best ask, best ask · (1 + CROSS_MAX_OVER)],
    notional в [минимум биржи, CROSS_MAX_NOTIONAL]. Ниже best ask — мерить нечего; ниже
    минимума — отказ по минимуму заслонил бы ответ ALO; выше $11 — решение лида."""
    if tif != "Alo":
        raise Guardrail("only post-only (Alo) orders")
    if is_buy is not True:
        raise Guardrail("only buys")
    if not (math.isfinite(px) and math.isfinite(sz) and px > 0 and sz > 0):
        raise Guardrail(f"price {px} / size {sz} is not a positive number")
    ask = (book or {}).get("best_ask")
    if not (isinstance(ask, (int, float)) and not isinstance(ask, bool) and math.isfinite(ask) and ask > 0):
        raise Guardrail("no best ask")
    if px < ask:
        raise Guardrail(f"price {px} is below the best ask {ask}: it would not cross")
    if px > (1.0 + CROSS_MAX_OVER) * ask:
        raise Guardrail(f"price {px} is more than {CROSS_MAX_OVER:.1%} through the best ask {ask}")
    notional = px * sz
    if notional > CROSS_MAX_NOTIONAL:
        raise Guardrail(f"notional ${notional:.2f} is above ${CROSS_MAX_NOTIONAL} (Q13)")
    if notional < VENUE_MIN_NOTIONAL:
        raise Guardrail(f"notional ${notional:.2f} is below the venue minimum")


def _quantum(decimals: int) -> Decimal:
    return Decimal(1).scaleb(-decimals)  # decimals < 0 даёт 10, 100, …


def price_decimals(px: float, sz_decimals: int) -> int:
    """HL perps: ≤ 5 значащих цифр и ≤ 6 − szDecimals знаков после запятой."""
    return min(max(0, 6 - sz_decimals), 4 - int(math.floor(math.log10(px))))


def _round_px(px: float, sz_decimals: int, rounding) -> float:
    q = _quantum(price_decimals(px, sz_decimals))
    return float((Decimal(repr(px)) / q).to_integral_value(rounding=rounding) * q)


def floor_px(px: float, sz_decimals: int) -> float:
    """Вниз — для покупки это дальше от рынка, никогда не ближе."""
    return _round_px(px, sz_decimals, ROUND_FLOOR)


def ceil_px(px: float, sz_decimals: int) -> float:
    """Вверх — для Q13 (цена не ниже best ask) и для минимального notional C4."""
    return _round_px(px, sz_decimals, ROUND_CEILING)


def floor_sz(sz: float, sz_decimals: int) -> float:
    q = _quantum(sz_decimals)
    return float((Decimal(repr(sz)) / q).to_integral_value(rounding=ROUND_FLOOR) * q)


def ceil_sz(sz: float, sz_decimals: int) -> float:
    q = _quantum(sz_decimals)
    return float((Decimal(repr(sz)) / q).to_integral_value(rounding=ROUND_CEILING) * q)


def small_order(ref: float, szd: int, *, extra_lots: int = 0):
    """(px, sz) ALO-покупки на $10.05–11 с ценой ≤ 92 % ref — C4 и его правки (S18–S20).

    Наименьшее число лотов, при котором цена не выше 92 % ref, плюс extra_lots (другая цена и
    объём для каждой правки); цена — самая низкая, дающая ≥ $10.05. None — сетка не даёт."""
    lot = _quantum(szd)
    k = math.ceil(NOTIONAL_FLOOR / (SMALL_MAX_FRACTION * ref) / float(lot) - 1e-9) + extra_lots
    sz = float(lot * k)
    px = ceil_px(NOTIONAL_FLOOR / sz, szd)
    notional = px * sz
    if px > SMALL_MAX_FRACTION * ref or not VENUE_MIN_NOTIONAL <= notional <= CROSS_MAX_NOTIONAL:
        return None
    return px, sz


def cross_order(book: dict, szd: int):
    """(px, sz) для Q13: цена в [ask·(1 + 0.1 %), ask·(1 + 0.2 %)] на сетке, notional — наибольший
    в [$10.02, $11] (дальше от минимума биржи, чтобы отказ был про пересечение). None — сетка
    лотов не даёт."""
    ask = book["best_ask"]
    lo = max(ceil_px(ask * (1.0 + CROSS_OVER), szd), ceil_px(ask, szd))
    hi = floor_px(ask * (1.0 + CROSS_MAX_OVER), szd)
    lot = _quantum(szd)
    best = None
    k = max(1, math.ceil(CROSS_MIN_NOTIONAL / (hi * float(lot)) - 1e-9))
    while k * float(lot) * lo <= CROSS_MAX_NOTIONAL:
        sz = float(lot * k)
        px = min(hi, floor_px(CROSS_MAX_NOTIONAL / sz, szd))
        notional = px * sz
        if px >= lo and CROSS_MIN_NOTIONAL <= notional <= CROSS_MAX_NOTIONAL and (best is None or notional > best[0]):
            best = (notional, px, sz)
        k += 1
    return None if best is None else (best[1], best[2])


def rows_problem(value, *, what: str = "frontendOpenOrders") -> str | None:
    """None — ответ /info со списком ордеров (frontendOpenOrders, openOrders) годится: список
    объектов с целым `oid`, строковой `coin` и cloid — строкой или ничем. Иначе — что не так.

    `{}` иначе читался бы как «ордеров нет» (ложное «чисто»), а `{"error": …}` и `false`
    роняли бы очистку исключением мимо отчёта."""
    if not isinstance(value, list):
        return f"{what}: not a list but {type(value).__name__}: {json.dumps(value, default=repr)[:160]}"
    for row in value:
        if not isinstance(row, dict):
            return f"{what}: a row is not an object: {json.dumps(row, default=repr)[:160]}"
        oid, cloid = row.get("oid"), row.get("cloid")
        if isinstance(oid, bool) or not isinstance(oid, int):
            return f"{what}: a row without an integer oid: {json.dumps(row, default=repr)[:160]}"
        if not isinstance(row.get("coin"), str):
            return f"{what}: a row without a coin: {json.dumps(row, default=repr)[:160]}"
        if cloid is not None and not isinstance(cloid, str):
            return f"{what}: a row with a non-string cloid: {json.dumps(row, default=repr)[:160]}"
    return None


def sweep_targets(open_orders, coin: str) -> list[int]:
    """oid строк, которые список биржи показывает с cloid формы пробника (любой прогон) на монете, —
    и только их. Строка без cloid или с чужим cloid — не цель, даже если её oid есть в реестре
    прогона: oid в реестр кладёт ответ биржи на наше действие, а ответ может соврать (нота §9).
    Вход — список, прошедший rows_problem."""
    return [
        o["oid"] for o in open_orders
        if o.get("coin") == coin and type(o.get("oid")) is int and is_probe_cloid(o.get("cloid"))
    ]


def probe_form_oids(open_orders, coin: str) -> set[int]:
    """То же множеством: что рубеж (assert_action_safe) вправе отменить по oid."""
    return set(sweep_targets(open_orders, coin))


def disowned_rows(open_orders, coin: str, ledger_oids) -> list[dict]:
    """Строки на монете, которые ответ биржи приписал прогону (oid из реестра), а список показывает
    без cloid формы пробника — с чужим cloid или вовсе без него. Их не трогает никто: ответ и список
    биржи противоречат друг другу. Строка без cloid может быть и нашей (правка сбросила cloid) —
    её проверяют руками (код 4)."""
    return [
        {"coin": o.get("coin"), "oid": o.get("oid"), "cloid": o.get("cloid")} for o in open_orders
        if o.get("coin") == coin and type(o.get("oid")) is int and o["oid"] in ledger_oids
        and not is_probe_cloid(o.get("cloid"))
    ]


def describe_disowned(rows) -> str:
    """Причина словами: ответ биржи дал ордеру прогона oid, который её же список показывает иначе."""
    parts = [
        f"oid {o.get('oid')}, but its open-order list shows that oid "
        + (f"under cloid {o['cloid']}" if o.get("cloid") else "without a cloid") + f" on {o.get('coin')}"
        for o in rows
    ]
    return ("the venue's answer gave this run's order " + "; and ".join(parts)
            + ": the answer and the list disagree — the probe never touches such an order")


def response_shape(body):
    """('statuses', list) | ('default', None) | ('err', text) | ('other', body)."""
    if not isinstance(body, dict):
        return "other", body
    if body.get("status") == "err":
        return "err", str(body.get("response"))
    if body.get("status") == "ok":
        response = body.get("response")
        if isinstance(response, dict):
            statuses = (response.get("data") or {}).get("statuses")
            if isinstance(statuses, list):
                return "statuses", statuses
            if response.get("type") == "default":
                return "default", None
    return "other", body


def status_kind(status) -> str:
    if status == "success":
        return "success"
    if isinstance(status, dict):
        for kind in ("resting", "filled", "error"):
            if kind in status:
                return kind
    return "other"


def assert_action_safe(action: dict, asset: int, owns, sweepable=frozenset()) -> None:
    """Последний рубеж перед подписью — независимо от того, как действие собрано.

    Единственное место, где проверяются ЦЕЛИ: правка (modify/batchModify) и cancelByCloid — только
    то, что принадлежит прогону (owns: выпущенный им cloid или oid, который биржа приписала его
    cloid); cancel по oid — только oid из `sweepable`: список биржи сам показывает этот ордер с
    cloid формы пробника на монете (добор и очистка, probe_form_oids). Своего oid из реестра для
    отмены мало: oid в реестр кладёт ответ биржи, а ответ может соврать (нота §9)."""
    t = action.get("type")
    if t not in ALLOWED_ACTIONS:
        raise Guardrail(f"action type {t!r} is never signed by this probe")
    action_items(action)
    for w in action_wires(action):
        if tuple(w) != WIRE_KEYS:
            raise Guardrail(f"order wire keys {tuple(w)}")
        if w["a"] != asset or w["b"] is not True or w["r"] is not False or w["t"] != {"limit": {"tif": "Alo"}}:
            raise Guardrail("only post-only buys on the probe coin")
        if not owns(w["c"]):
            raise Guardrail(f"cloid {w['c']} was not minted by this run")
        if Decimal(w["p"]) * Decimal(w["s"]) > Decimal(repr(MAX_NOTIONAL)):
            raise Guardrail(f"notional {w['p']}×{w['s']} is above ${MAX_NOTIONAL}")
    targets = ([action.get("oid")] if t == "modify" else
               [m.get("oid") for m in action["modifies"]] if t == "batchModify" else [])
    for target in targets:
        if not owns(target):
            raise Guardrail(f"{t}: target {target!r} is not this run's")
    if t == "order" and action.get("grouping") != "na":
        raise Guardrail("grouping must be 'na'")
    if t == "cancel":
        for c in action["cancels"]:
            oid = c.get("o")
            if c.get("a") != asset or type(oid) is not int:
                raise Guardrail("cancel: only oids on the probe coin")
            if oid not in sweepable:
                raise Guardrail(f"cancel: the venue's own list does not show oid {oid} under a probe cloid on the coin")
    if t == "cancelByCloid" and not all(c.get("asset") == asset and owns(c.get("cloid")) for c in action["cancels"]):
        raise Guardrail("cancelByCloid: only this run's cloids on the probe coin")


def parse_rate_limit(rl) -> dict:
    """userRateLimit → {used, cap}; другая форма — Abort (а не KeyError мимо отчёта)."""
    try:
        used, cap = rl["nRequestsUsed"], rl["nRequestsCap"]
        if isinstance(used, bool) or isinstance(cap, bool):
            raise TypeError("bool")
        return {"used": int(used), "cap": int(cap)}
    except (KeyError, TypeError, ValueError) as error:
        raise Abort(f"/info userRateLimit of an unexpected shape ({error!r}): {json.dumps(rl, default=repr)[:160]}") from None


def position_of(state, coin: str) -> str | None:
    """szi по монете из clearinghouseState; нет записи — "0" (плоско, fixtures.rs)."""
    if not isinstance(state, dict) or not isinstance(state.get("assetPositions"), list):
        return None
    for entry in state["assetPositions"]:
        pos = (entry or {}).get("position") or {}
        if pos.get("coin") == coin:
            return str(pos.get("szi"))
    return "0"


def action_preimage(packed: bytes, nonce: int, expires_after: int | None = None) -> bytes:
    """Прообраз connection_id: msgpack ‖ nonce:u64be ‖ 0x00 (без vault) [‖ 0x00 ‖ expires_after:u64be].

    HL-нота §5.7; хвост — только при expires_after. Нота design-live-modify §5.1.
    """
    out = packed + nonce.to_bytes(8, "big") + b"\x00"
    if expires_after is not None:
        out += b"\x00" + expires_after.to_bytes(8, "big")
    return out


def accepted_modifies(step) -> list[dict]:
    """Ордера (wire), которые правка этого шага по ответу биржи приняла (пропущенный шаг — нет)."""
    action = step.get("action") or {}
    if action.get("type") == "modify":
        return [action["order"]] if step.get("shape") == "default" else []
    if action.get("type") != "batchModify":
        return []
    statuses = step.get("statuses") or []
    return [m["order"] for m, st in zip(action["modifies"], statuses) if status_kind(st) == "resting"]


def oid_chains(steps) -> dict[str, list[int]]:
    """cloid → oid по порядку шагов, по frontendOpenOrders после каждого шага."""
    chains: dict[str, list[int]] = {}
    for step in steps:
        for cloid, entry in (step.get("live_after") or {}).items():
            chain = chains.setdefault(cloid, [])
            if not chain or chain[-1] != entry["oid"]:
                chain.append(entry["oid"])
    return chains


def oid_monotonic(steps) -> dict:
    """P3 (нота §1.1): в каждой цепочке cloid oid строго растёт."""
    chains = oid_chains(steps)
    replaced = {c: o for c, o in chains.items() if len(o) > 1}
    return {
        "chains": replaced,
        "strictly_increasing": all(a < b for o in replaced.values() for a, b in zip(o, o[1:]))
        if replaced
        else None,
    }


def replacement_sizes(steps) -> list[dict]:
    """origSz замены против отправленного s — для каждой принятой правки (нота §3.1, A1)."""
    out = []
    for step in steps:
        rows = {str(o.get("cloid") or "").lower(): o for o in step.get("frontend_open") or []}
        for wire_ in accepted_modifies(step):
            row = rows.get(str(wire_.get("c") or "").lower())
            if row is None:
                continue
            out.append(
                {
                    "step": step["step"],
                    "cloid": wire_.get("c"),
                    "s": wire_["s"],
                    "origSz": row.get("origSz"),
                    "sz": row.get("sz"),
                    "equal": row.get("origSz") is not None and float(row["origSz"]) == float(wire_["s"]),
                }
            )
    return out


def kept_untouched(step) -> bool | None:
    """Все ордера, открытые до шага, после него — те же (oid, цена, объём)."""
    if step is None or "live_after" not in step:
        return None
    before, after = step.get("live_before") or {}, step["live_after"]
    keep = ("oid", "px", "sz")
    return bool(before) and all(
        c in after and all(after[c].get(k) == v.get(k) for k in keep) for c, v in before.items()
    )


def same_file(a, b) -> bool:
    try:
        return os.path.samefile(a, b)
    except (OSError, TypeError):
        return False


def running_hl_connectors(proc_root: Path = Path("/proc")):
    """Процессы `…connector… <name> hyperliquid <config>`: файл ключей, счёт и монеты.

    None — список процессов недоступен (не Linux). Ключи здесь не читаются. `unresolved` —
    почему файл ключей не определён (env/cwd другого пользователя, нечитаемый конфиг)."""
    if not proc_root.is_dir():
        return None
    found = []
    for p in proc_root.iterdir():
        if not p.name.isdigit():
            continue
        try:
            raw = (p / "cmdline").read_bytes()
        except OSError:
            continue
        argv = [a.decode(errors="replace") for a in raw.split(b"\0") if a]
        # clap: три позиционных аргумента — name, connector, config.
        if len(argv) < 4 or argv[2] != "hyperliquid":
            continue
        names = {os.path.basename(argv[0])}
        try:
            names.add(os.path.basename(os.readlink(p / "exe")))
        except OSError:
            pass
        if not any("connector" in n for n in names):
            continue
        unresolved = []
        try:
            cwd = Path(os.readlink(p / "cwd"))
        except OSError:
            cwd = None
        cfg_arg = Path(argv[3])
        cfg_path = cfg_arg if cfg_arg.is_absolute() else (cwd / cfg_arg if cwd else None)
        env = None
        try:
            env = {}
            for item in (p / "environ").read_bytes().split(b"\0"):
                key, sep, value = item.partition(b"=")
                if sep:
                    env[key.decode(errors="replace")] = value.decode(errors="replace")
        except OSError:
            env = None
            unresolved.append("its environment is unreadable (HYPERLIQUID_CREDENTIALS may override)")
        cfg = {}
        if cfg_path is None:
            unresolved.append("relative config and unreadable cwd")
        else:
            toml = _toml()
            try:
                cfg = toml.loads(cfg_path.read_text())
            except (OSError, toml.TOMLDecodeError, UnicodeDecodeError):
                unresolved.append(f"config {cfg_path} is unreadable")
        creds = ((env or {}).get("HYPERLIQUID_CREDENTIALS") or "").strip() or str(
            cfg.get("credentials_path") or ""
        ).strip()
        creds_path = None
        if creds:
            cp = Path(creds)  # коннектор не раскрывает "~" — и мы не раскрываем
            if cp.is_absolute():
                creds_path = cp
            elif cwd is not None:
                creds_path = cwd / cp
            else:
                unresolved.append("relative credentials path and unreadable cwd")
        elif not unresolved:
            unresolved.append("no credentials path in its environment or config")
        found.append(
            {
                "pid": int(p.name),
                "config": str(cfg_path or argv[3]),
                "credentials": str(creds_path) if creds_path else None,
                "coins": list(cfg.get("coins") or []),
                "account_address": cfg.get("account_address"),
                "unresolved": "; ".join(unresolved) or None,
            }
        )
    return found


def shared_wallet_conflicts(config_path, agent, master, coin, connectors, keys_of):
    """(отказы, предупреждения). keys_of(path) → (адрес API-кошелька, адрес мастера) файла
    ключей или (None, None)."""
    refusals, warnings = [], []
    if connectors is None:
        warnings.append(
            "could not list running processes (no /proc): make sure no live connector "
            "signs with this API wallet or trades this account"
        )
        return refusals, warnings
    agent, master = agent.lower(), master.lower()
    for c in connectors:
        tag = f"pid {c['pid']} (connector … hyperliquid {c['config']})"
        cred = c.get("credentials")
        if cred and config_path and same_file(config_path, cred):
            refusals.append(
                f"{tag} uses the same credentials file {cred}: two signers on one API wallet "
                "fight over its nonce set and the live connector's orders get refused"
            )
            continue
        other_agent, other_master = keys_of(cred) if cred else (None, None)
        if other_agent and other_agent.lower() == agent:
            refusals.append(f"{tag} signs with the same API wallet {agent} from another file")
            continue
        if c.get("unresolved") or not other_agent:
            why = c.get("unresolved") or f"credentials file {cred} is unreadable"
            warnings.append(
                f"{tag}: cannot tell which API wallet it signs with ({why}); if it is {agent}, "
                "stop it first — a shared signer gets its orders refused"
            )
        account = str(c.get("account_address") or other_master or "").lower()
        if account == master:
            trades = f", configured coins {c.get('coins')}" if c.get("coins") else ""
            warnings.append(
                f"{tag} trades this account {master}{trades}: its sweep/cancel-all/"
                f"reconciliation may cancel probe orders on {coin} (the probe then aborts), and "
                "its requests pollute d_used"
            )
        elif not account:
            warnings.append(f"{tag}: its account is unknown — if it is {master}, see above")
    return refusals, warnings


def redact(text: str, master: str) -> str:
    return re.sub(re.escape(master), HARDHAT_ADDRESS_1, text, flags=re.IGNORECASE) if master else text


def default_state_dir() -> Path:
    base = os.environ.get("XDG_STATE_HOME") or str(Path.home() / ".local" / "state")
    return Path(base) / "hl_modify_probe"


def acquire_lock(state_dir: Path):
    """Один прогон за раз на машине: два прогона делили бы очистку и бюджет."""
    state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    handle = open(state_dir / "hl_modify_probe.lock", "a+")  # noqa: SIM115 — держится до выхода
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        handle.close()
        raise Refuse(f"another hl_modify_probe run holds {state_dir / 'hl_modify_probe.lock'}") from None
    handle.seek(0)
    handle.truncate()
    handle.write(f"{os.getpid()}\n")
    handle.flush()
    return handle


# --- Вывод и сигналы ------------------------------------------------------------------------

# Куда уходит вывод, если терминал пропал: рядом с результатами прогона (run() задаёт путь).
_console: dict = {"log": None, "detached": {}}  # detached: fd → куда переведён


def _say(text: str) -> None:
    """Из обработчика сигнала: прямой write(2) — print мог бы войти в буфер повторно."""
    try:
        os.write(2, (text + "\n").encode())
    except OSError:
        pass


def console_log_to(path) -> None:
    _console["log"], _console["detached"] = path, {}


def console_log_used() -> str | None:
    """Путь лога, если хоть один поток вывода в него переведён."""
    used = [t for t in _console["detached"].values() if t != os.devnull]
    return used[0] if used else None


def console_detach(fd: int, why: str) -> None:
    """Поток вывода (fd 1 или 2), запись в который упала, → лог рядом с результатами (или
    /dev/null, если лог не открыть). Каждый fd — один раз; здоровый поток не трогается.

    После обрыва SSH или закрытия терминала любой write в tty даёт EIO: print из свёртки оборвал
    бы очистку и запись результатов, а несброшенный буфер при выходе дал бы код 120 вместо
    нашего. Строка, на которой вывод оборвался, может повториться в логе."""
    if fd in _console["detached"]:
        return
    _console["detached"][fd] = os.devnull
    target = None
    if _console["log"] is not None:
        try:
            Path(_console["log"]).parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            target = os.open(_console["log"], os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
            _console["detached"][fd] = str(_console["log"])
        except OSError:
            target = None
    try:
        if target is None:
            target = os.open(os.devnull, os.O_WRONLY)
        os.dup2(target, fd)
        os.close(target)
        stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        os.write(fd, f"--- {stamp} fd {fd}: the terminal is gone ({why}); output continues here ---\n".encode())
    except OSError:
        pass


def say(text: str = "", *, err: bool = False) -> None:
    """print, который не бросает: ошибка вывода (EIO после обрыва терминала, EPIPE) переводит
    этот поток в лог (console_detach) и повторяет строку туда. Весь вывод боевого пути — здесь."""
    for _ in range(2):
        try:
            print(text, file=sys.stderr if err else sys.stdout, flush=True)
            return
        except OSError:
            console_detach(2 if err else 1, "output failed")
        except ValueError:  # поток закрыт: терминал тут ни при чём — строка теряется, свёртка нет
            return


def warn(text: str) -> None:
    say(text, err=True)


class Stopper:
    """Сигнал, который по умолчанию завершил бы процесс и приходит снаружи (signal(7)), → флаг; план
    останавливается в безопасной точке, дальше — очистка. Во время очистки сигналы игнорируются: она
    ограничена по времени и кончается сама. Мимо очистки уходят только SIGKILL (kill -9, OOM-killer),
    сигналы аварии самого интерпретатора (SIGSEGV, SIGBUS, SIGILL, SIGFPE, SIGABRT, SIGTRAP, SIGSYS:
    их обработчик на Python вернул бы управление в упавшую инструкцию) и сигналы без имени в модуле
    signal (SIGSTKFLT, real-time — их шлют только руками) — потом --sweep-only. Стоп-сигналы (Ctrl-Z)
    не завершают, а замораживают процесс — вместе с его ордерами."""

    SIGNALS = (
        "SIGINT", "SIGTERM", "SIGHUP", "SIGQUIT",  # Ctrl-C, kill, обрыв терминала, Ctrl-\
        "SIGUSR1", "SIGUSR2", "SIGALRM", "SIGVTALRM", "SIGPROF", "SIGIO", "SIGPWR",  # kill -USR1 и т. п.
        "SIGXCPU", "SIGXFSZ",  # rlimit: процессор, размер файла
    )

    def __init__(self):
        self.reason: str | None = None
        self.cleaning = False
        self.ignored: list[str] = []

    def install(self) -> None:
        for name in self.SIGNALS:
            sig = getattr(signal, name, None)  # SIGPWR — только Linux
            if sig is not None:
                signal.signal(sig, self._on_signal)

    def _on_signal(self, signum, _frame) -> None:
        name = signal.Signals(signum).name
        if self.cleaning:
            self.ignored.append(name)
            _say(f"{name} ignored: cleanup of probe orders is running and ends by itself "
                 "(kill -9 skips it — then run --sweep-only)")
            return
        if self.reason is None:
            self.reason = name
            _say(f"{name}: stopping after the current request, then cleanup")
        else:
            _say(f"{name}: already stopping")

    @property
    def requested(self) -> bool:
        return self.reason is not None

    def check(self) -> None:
        if self.reason is not None:
            raise Interrupted(f"stopped by {self.reason}")

    def sleep(self, seconds: float) -> None:
        """Пауза, которую сигнал обрывает (шагом 0.1 с); затем check()."""
        left = seconds
        while left > 0 and self.reason is None:
            chunk = min(left, 0.1)
            time.sleep(chunk)
            left -= chunk
        self.check()


# --- SDK (ленивый импорт: --self-test без него) --------------------------------------------


def load_sdk():
    try:
        import importlib.metadata as md

        import msgpack
        import requests
        import websocket  # websocket-client, зависимость SDK
        from eth_account import Account
        from eth_utils import keccak
        from hyperliquid.utils.signing import (
            float_to_wire,
            order_request_to_order_wire,
            order_wires_to_order_action,
            sign_l1_action,
        )
        from hyperliquid.utils.types import Cloid
    except ImportError as error:
        raise Refuse(f"the official SDK is required: {error} (docs/design-live-modify.md §9)") from None
    try:
        version = md.version("hyperliquid-python-sdk")
    except md.PackageNotFoundError:
        version = "unknown"
    return SimpleNamespace(
        Account=Account,
        Cloid=Cloid,
        float_to_wire=float_to_wire,
        order_request_to_order_wire=order_request_to_order_wire,
        order_wires_to_order_action=order_wires_to_order_action,
        sign_l1_action=sign_l1_action,
        msgpack=msgpack,
        keccak=keccak,
        requests=requests,
        websocket=websocket,
        version=version,
    )


def golden_order(sdk) -> dict:
    return {
        "coin": "BTC", "is_buy": True, "sz": 0.001, "limit_px": 30000.0,
        "order_type": {"limit": {"tif": "Alo"}}, "reduce_only": False,
        "cloid": sdk.Cloid.from_str(GOLDEN_CLOID),
    }


def check_sdk(sdk) -> None:
    """Пин версии и контроль среды: SDK обязан воспроизвести golden ордера из exchange.rs.
    0.24.0 подписывает expiresAfter (sign_l1_action с 6 аргументами) — старее не годится."""
    if sdk.version != SDK_PIN:
        raise Refuse(f"hyperliquid-python-sdk {sdk.version}; the probe is pinned to {SDK_PIN} (golden-verified)")
    wire = sdk.order_request_to_order_wire(golden_order(sdk), 3)
    if sdk.msgpack.packb(sdk.order_wires_to_order_action([wire])).hex() != ORDER_MSGPACK_ANCHOR:
        raise Refuse("this SDK does not reproduce the exchange.rs order golden")


def sign(sdk, wallet, action, nonce, expires_after=None):
    """Единственное место, где подписывается действие, — и is_mainnet=False зашит здесь."""
    return sdk.sign_l1_action(wallet, action, None, nonce, expires_after, False)


def connection_id(sdk, action, nonce, expires_after=None) -> str:
    packed = sdk.msgpack.packb(action)
    return bytes(sdk.keccak(action_preimage(packed, nonce, expires_after))).hex()


# --- Сеть -----------------------------------------------------------------------------------


class Client:
    def __init__(self, sdk, wallet, master: str, venue: Venue = TESTNET, timing: Timing = REAL, *,
                 dry_run: bool, budget: Budget | None = None):
        assert_venue_url(venue.rest, venue)
        self.sdk = sdk
        self.wallet = wallet
        self.master = master
        self.venue = venue
        self.timing = timing
        self.dry_run = dry_run
        self.budget = budget or Budget()
        self.http = sdk.requests.Session()
        self.n_actions = 0  # подписанных POST (справочно; потолок — в запросах биржи)
        self.last_nonce = 0
        self.asset = None
        self.coin = None
        self.owns = None
        self.clock_lead_ms: int | None = None  # наши часы − Date ответа (середина секунды)

    def bind(self, asset: int, owns, coin: str) -> None:
        self.asset, self.owns, self.coin = asset, owns, coin

    def info(self, body, *, retries: int = 0, sleep=None):
        sleep = sleep or time.sleep
        last = None
        for attempt in range(retries + 1):
            if attempt:
                sleep(self.timing.retry_s * attempt)
            try:
                r = self.http.post(
                    self.venue.rest + "/info", json=body, timeout=self.timing.http_timeout_s,
                    allow_redirects=False,
                )
            except Exception as error:  # noqa: BLE001
                last = f"/info {body.get('type')}: transport failure {error!r}"
                continue
            if r.status_code != 200:
                last = f"/info {body.get('type')} answered HTTP {r.status_code}: {r.text[:200]}"
                continue
            self._note_date(r)
            try:
                return r.json()
            except ValueError:
                last = f"/info {body.get('type')}: the answer is not JSON: {r.text[:200]}"
        raise Abort(last)

    def _note_date(self, response) -> None:
        """Перекос часов по заголовку Date (секундная точность): наши − биржевые − 500 мс."""
        try:
            date = (getattr(response, "headers", None) or {}).get("Date")
            venue_ms = int(email.utils.parsedate_to_datetime(date).timestamp() * 1000) if date else None
        except (TypeError, ValueError):
            venue_ms = None
        if venue_ms is not None:
            self.clock_lead_ms = int(time.time() * 1000) - venue_ms - 500

    def exchange(self, action, *, pool: str = "plan", cost: int | None = None, expires_in_s=None,
                 listing=None) -> dict:
        """Подписывает (тестнет) и шлёт. Сбой транспорта — в записи, не исключением.

        expires_in_s: None — без expiresAfter; иначе expiresAfter = момент подписи + столько
        (Q12; отрицательное — уже просрочено). Одно значение — и в подписи, и в теле.
        cost — запросы биржи (по умолчанию venue_cost); списывается из пула до подписи: действие,
        на которое запросов нет, не подписывается и не уходит.
        listing — список биржи, по которому собран cancel добора хвостов и очистки: из него рубеж
        сам берёт oid формы пробника на монете. Без него cancel по oid не подписывается (план
        снимает свои ордера только cancelByCloid)."""
        if self.dry_run:
            raise Guardrail("dry run: nothing is signed or sent")
        if self.asset is None or self.coin is None:
            raise Guardrail("the client is not bound to the probe coin")
        sweepable = frozenset(probe_form_oids(listing, self.coin)) if listing is not None else frozenset()
        assert_action_safe(action, self.asset, self.owns, sweepable)
        if cost is None:
            cost = venue_cost(action, stale=expires_in_s is not None and expires_in_s < 0)
        self.budget.charge(pool, cost)
        t_sign_ms = int(time.time() * 1000)
        nonce = max(t_sign_ms, self.last_nonce + 1)
        self.last_nonce = nonce
        expires_after = None if expires_in_s is None else t_sign_ms + int(round(expires_in_s * 1000))
        payload = {
            "action": action,
            "nonce": nonce,
            "signature": sign(self.sdk, self.wallet, action, nonce, expires_after),
            "vaultAddress": None,
            "expiresAfter": expires_after,
        }
        self.n_actions += 1
        out = {
            "nonce": nonce, "t_sign_ms": t_sign_ms, "expires_after": expires_after, "pool": pool,
            "cost": cost, "post_no": self.n_actions, "t_send_ns": time.time_ns(),
        }
        try:
            # Без редиректов: подписанное действие уходит только на свой хост.
            r = self.http.post(
                self.venue.rest + "/exchange", json=payload, timeout=self.timing.http_timeout_s,
                allow_redirects=False,
            )
        except Exception as error:  # noqa: BLE001 — любой сбой транспорта = неопределённость
            out.update(t_resp_ns=time.time_ns(), transport_error=repr(error))
            return out
        out.update(t_resp_ns=time.time_ns(), http_status=r.status_code, text=r.text[:2000])
        try:
            out["body"] = r.json()
        except ValueError:
            out["body"] = None
        return out


class Stream:
    """orderUpdates + userFills мастера; каждая рамка с локальным временем приёма и номером."""

    def __init__(self, websocket_module, master: str, venue: Venue = TESTNET):
        assert_venue_url(venue.ws, venue)
        self.ws_mod = websocket_module
        # Без редиректов: websocket-client по умолчанию проходит до 3 редиректов рукопожатия, а
        # assert_venue_url видел только исходный URL. 1.9.2 на redirect_limit=0 бросает, 1.9.0
        # молча «подключается» с ответом 3xx — поэтому ещё и статус: только 101 с этого хоста.
        self.ws = websocket_module.create_connection(venue.ws, timeout=10, redirect_limit=0)
        status = self.ws.getstatus()
        if status != 101:
            try:
                self.ws.close()
            except Exception:  # noqa: BLE001
                pass
            raise Refuse(f"the account stream answered HTTP {status}, not 101: redirects are never followed")
        self.master = master
        self.frames: list[dict] = []
        self.lock = threading.Lock()
        self.dead = None
        self.stop = threading.Event()
        self.seq = 0
        for kind in ("orderUpdates", "userFills"):
            self.ws.send(
                json.dumps({"method": "subscribe", "subscription": {"type": kind, "user": master}})
            )
        threading.Thread(target=self._read, daemon=True).start()
        threading.Thread(target=self._ping, daemon=True).start()

    def _read(self):
        self.ws.settimeout(1.0)
        while not self.stop.is_set():
            try:
                text = self.ws.recv()
            except self.ws_mod.WebSocketTimeoutException:
                continue
            except Exception as error:  # noqa: BLE001
                if not self.stop.is_set():
                    self.dead = repr(error)
                return
            if not text:
                if not self.stop.is_set() and not self.ws.connected:
                    self.dead = "closed by the venue"
                    return
                continue
            if isinstance(text, bytes):
                text = text.decode("utf-8", "replace")
            try:
                channel = json.loads(text).get("channel")
            except (ValueError, AttributeError):
                channel = None
            if channel == "pong":
                continue
            with self.lock:
                self.seq += 1
                self.frames.append(
                    {"seq": self.seq, "t_ns": time.time_ns(), "channel": channel, "text": text}
                )

    def _ping(self):
        while not self.stop.wait(20):
            try:
                self.ws.send(json.dumps({"method": "ping"}))
            except Exception as error:  # noqa: BLE001
                if not self.stop.is_set():
                    self.dead = repr(error)
                return

    def last_seq(self) -> int:
        with self.lock:
            return self.seq

    def since(self, seq0: int) -> list[dict]:
        with self.lock:
            return [f for f in self.frames if f["seq"] > seq0]

    def wait_ready(self, timeout_s: float) -> None:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            acked = set()
            for f in self.since(0):
                if f["channel"] == "subscriptionResponse":
                    sub = (json.loads(f["text"]).get("data") or {}).get("subscription") or {}
                    acked.add(sub.get("type"))
            if {"orderUpdates", "userFills"} <= acked:
                return
            if self.dead:
                break
            time.sleep(0.05)
        raise Refuse(f"the account stream did not acknowledge both subscriptions ({self.dead})")

    def close(self):
        self.stop.set()
        try:
            self.ws.close()
        except Exception:  # noqa: BLE001
            pass


def list_open_orders(client: Client, master: str, *, kind="frontendOpenOrders", retries=0, sleep=None) -> list[dict]:
    """Открытые ордера счёта — только список правильной формы (rows_problem), иначе Abort.
    Единственный читатель frontendOpenOrders/openOrders: план, префлайт, добор и очистка."""
    rows = client.info({"type": kind, "user": master}, retries=retries, sleep=sleep)
    problem = rows_problem(rows, what=kind)
    if problem:
        raise Abort(f"/info {problem}")
    return rows


def fetch_book(client: Client, coin: str, *, sleep=None) -> dict:
    book = client.info({"type": "l2Book", "coin": coin}, retries=1, sleep=sleep)
    try:
        levels = book.get("levels") or [[], []]
        if len(levels) != 2 or not levels[0] or not levels[1]:
            raise Abort("the book is empty or one-sided")
        best_bid, best_ask = float(levels[0][0]["px"]), float(levels[1][0]["px"])
        mid = float(client.info({"type": "allMids"}, retries=1, sleep=sleep)[coin])
    except (AttributeError, KeyError, IndexError, TypeError, ValueError) as error:
        raise Abort(f"l2Book/allMids of an unexpected shape: {error!r}") from None
    if not (math.isfinite(best_bid) and math.isfinite(best_ask) and math.isfinite(mid)):
        raise Abort(f"non-finite prices {best_bid}/{best_ask}/{mid}")
    if not best_bid < best_ask:
        raise Abort(f"crossed book {best_bid}/{best_ask}")
    venue_ms = book.get("time")
    return {
        "mid": mid, "best_bid": best_bid, "best_ask": best_ask, "ref": min(mid, best_bid),
        # наши часы минус `time` книги: задержка + возраст книги + перекос — только для записи
        "book_age_ms": int(time.time() * 1000) - venue_ms if isinstance(venue_ms, int) else None,
    }


# --- Прогон ---------------------------------------------------------------------------------


class Probe:
    def __init__(self, client, stream, ledger, coin, asset, szd, master, timing=None, stopper=None):
        self.client, self.stream, self.ledger = client, stream, ledger
        self.coin, self.asset, self.szd, self.master = coin, asset, szd, master
        self.timing = timing or client.timing
        self.stopper = stopper or Stopper()
        client.bind(asset, ledger.owns, coin)
        self.steps: list[dict] = []
        self.live: dict[str, dict] = {}  # cloid -> {"oid","px","sz","origSz"} по frontendOpenOrders
        self.book: dict | None = None
        self.over_cap = False
        self.last_action_t = 0.0
        self.cursor = 0  # последняя просмотренная рамка потока: ни одна не теряется
        self.pre_frames: list[dict] = []  # до S1 (подписка, добор хвостов)
        self.after_frames: list[dict] = []  # после плана (очистка)
        self.start_report: dict | None = None  # добор хвостов в начале (start_sweep)
        self.t_start_ms = int(time.time() * 1000)

    # Цены и ордера ---------------------------------------------------------------------------

    def refresh_book(self) -> dict:
        self.book = fetch_book(self.client, self.coin, sleep=self.stopper.sleep)
        return self.book

    def px(self, fraction: float) -> float:
        return floor_px(fraction * self.book["ref"], self.szd)

    def size(self, notional: float, px: float) -> float:
        return ceil_sz(notional / px, self.szd)

    def bad_size(self, px: float) -> float:
        sz = floor_sz(BAD_NOTIONAL / px, self.szd)
        if sz <= 0:
            raise Guardrail("the lot is too coarse for a sub-minimum test size")
        return sz

    def small(self, extra_lots: int = 0):
        got = small_order(self.book["ref"], self.szd, extra_lots=extra_lots)
        if got is None:
            raise Guardrail("no $10–11 order fits this lot grid at this price")
        return got

    def _wire(self, cloid, px, sz) -> dict:
        f2w = self.client.sdk.float_to_wire
        return {"a": self.asset, "b": True, "p": f2w(px), "s": f2w(sz), "r": False,
                "t": {"limit": {"tif": "Alo"}}, "c": cloid}

    def wire(self, cloid: str, px: float, sz: float, *, allow_below_min=False) -> dict:
        """Гардрейлы — против self.book, перечитанной в act() прямо перед этим POST."""
        if not self.ledger.owns(cloid):
            raise Guardrail(f"cloid {cloid} is not this run's")
        check_order_guardrails(
            px, sz, self.book["ref"], tif="Alo", is_buy=True, allow_below_venue_min=allow_below_min
        )
        return self._wire(cloid, px, sz)

    def cross_wire(self, cloid: str) -> dict:
        """Q13 (S21): ALO-покупка на/через best ask — единственное исключение из 5 %."""
        if not self.ledger.owns(cloid):
            raise Guardrail(f"cloid {cloid} is not this run's")
        got = cross_order(self.book, self.szd)
        if got is None:
            raise Guardrail("Q13: no $10–11 order through the ask fits this lot grid")
        px, sz = got
        check_cross_guardrails(px, sz, self.book, tif="Alo", is_buy=True)
        return self._wire(cloid, px, sz)

    def place(self, cloid, px, sz):
        return {"type": "order", "orders": [self.wire(cloid, px, sz)], "grouping": "na"}

    def modify_one(self, target, wire):
        return {"type": "modify", "oid": target, "order": wire}

    def batch_modify(self, pairs):
        return {"type": "batchModify", "modifies": [{"oid": t, "order": w} for t, w in pairs]}

    def cancel_by_cloid(self, cloids):
        return {
            "type": "cancelByCloid",
            "cancels": [{"asset": self.asset, "cloid": c} for c in cloids],
        }

    # Поток: курсор, поздние рамки ------------------------------------------------------------

    def examine(self) -> list[dict]:
        """Рамки после курсора; курсор двигается — каждая рамка просматривается ровно раз."""
        frames = self.stream.since(self.cursor)
        if frames:
            self.cursor = frames[-1]["seq"]
        return frames

    def attribute_late(self, frames, *, raise_on_fill=True, bucket=None) -> None:
        """Рамки, пришедшие после окна шага и до POST следующего, — прошлому шагу, с меткой late."""
        if not frames:
            return
        entries = self.run_entries(frames, late=True)
        fills = self.run_fills(frames)
        kept = self.keep_frames(frames, entries)
        target = None
        if bucket is not None:
            bucket.extend(kept)
        else:
            target = next((s for s in reversed(self.steps) if "skipped" not in s), None)
            if target is None:
                self.pre_frames.extend(kept)
            else:
                target.setdefault("late_ws_entries", []).extend(entries)
                target.setdefault("late_ws_frames", []).extend(kept)
                if fills:
                    target.setdefault("late_fills", []).extend(fills)
        if fills and raise_on_fill:
            where = target["step"] if target else "start"
            raise Abort(f"a probe order FILLED (frame seen late, after {where}): {fills}")

    def run_entries(self, frames, *, late=False) -> list[dict]:
        out = []
        for f in frames:
            if f["channel"] != "orderUpdates":
                continue
            data = json.loads(f["text"]).get("data") or []
            for i, e in enumerate(data):
                o = e.get("order") or {}
                if self.ledger.is_run_cloid(o.get("cloid")) or o.get("oid") in self.ledger.oids:
                    self.ledger.note(o.get("cloid"), o.get("oid"))
                    out.append(
                        {
                            "frame": f["seq"], "idx": i, "n_in_frame": len(data),
                            "oid": o.get("oid"), "cloid": o.get("cloid"), "status": e.get("status"),
                            "statusTimestamp": e.get("statusTimestamp"), "limitPx": o.get("limitPx"),
                            "sz": o.get("sz"), "origSz": o.get("origSz"), "t_ns": f["t_ns"],
                            "late": late,
                        }
                    )
        return out

    def run_fills(self, frames) -> list[dict]:
        fills = []
        for f in frames:
            if f["channel"] != "userFills":
                continue
            data = json.loads(f["text"]).get("data") or {}
            if data.get("isSnapshot"):
                continue
            for x in data.get("fills") or []:
                if is_probe_cloid(x.get("cloid")) or x.get("oid") in self.ledger.oids:
                    fills.append(x)
        return fills

    def keep_frames(self, frames, entries) -> list[dict]:
        """Дословно — для фикстур: рамки с записями пробника, его филлы и ошибки потока."""
        ours = {e["frame"] for e in entries}
        keep = []
        for f in frames:
            if f["seq"] in ours or f["channel"] == "error" or (
                f["channel"] == "userFills" and self.run_fills([f])
            ):
                keep.append({"seq": f["seq"], "t_ns": f["t_ns"], "text": redact(f["text"], self.master)})
        return keep

    # Действие и наблюдение -------------------------------------------------------------------

    def budget(self) -> dict:
        rl = self.client.info({"type": "userRateLimit", "user": self.master}, retries=2, sleep=self.stopper.sleep)
        return parse_rate_limit(rl)

    def targets_of(self, action) -> list:
        if action["type"] == "modify":
            return [action["oid"]]
        if action["type"] == "batchModify":
            return [m["oid"] for m in action["modifies"]]
        if action["type"] == "cancelByCloid":
            return [c["cloid"] for c in action["cancels"]]
        return []

    def skip(self, step: str, reason: str) -> None:
        self.steps.append({"step": step, "skipped": reason})
        say(f"{step:>4} skipped: {reason}")

    def act(self, step, purpose, kind, build, *, expect, refs=(), may_kill=(), may_create=(), expires_in_s=None):
        """Один подписанный POST. expect: resting | success | modified | refused | any.

        build() зовётся после паузы и после свежей книги: цена считается и проверяется
        гардрейлами прямо перед отправкой. Постановка сама разрешает появление своих cloid."""
        self.stopper.check()
        self.attribute_late(self.examine())
        if self.stream.dead:
            raise Abort(f"{step}: the account stream died: {self.stream.dead}")
        b0 = self.budget()
        over = b0["used"] >= b0["cap"]
        self.over_cap |= over
        is_cancel = kind in ("cancel", "cancelByCloid")
        gap = self.timing.over_cap_pace_s if over and not is_cancel else self.timing.pace_s
        self.stopper.sleep(self.last_action_t + gap - time.time())
        self.refresh_book()  # свежая книга прямо перед POST — после любой паузы
        action = build()
        if action.get("type") != kind:
            raise Guardrail(f"{step}: built {action.get('type')!r}, the plan says {kind!r}")
        # Цели правок и отмен проверяет assert_action_safe в Client.exchange — единственное место.
        cost = venue_cost(action, stale=expires_in_s is not None and expires_in_s < 0)
        if cost != PLAN_COSTS.get(step):
            raise Guardrail(f"{step}: costs {cost} venue requests, the plan reserved {PLAN_COSTS.get(step)}")
        may_create = {c.lower() for c in may_create}
        if kind == "order":
            may_create |= {w["c"].lower() for w in action["orders"]}
        may_kill = {c.lower() for c in may_kill}
        self.ledger.note_request(action)
        self.attribute_late(self.examine())  # пришедшее за паузу и сборку — прошлому шагу
        live_before = {c: dict(v) for c, v in self.live.items()}
        res = self.client.exchange(action, pool="plan", cost=cost, expires_in_s=expires_in_s)
        self.last_action_t = time.time()
        shape, payload = response_shape(res.get("body"))
        rec = {
            "step": step, "purpose": purpose, "action": action, **res, "shape": shape,
            "expires_in_s": expires_in_s, "budget_before": b0, "over_cap": over,
            "live_before": live_before, "book_at_send": dict(self.book),
        }
        if shape == "statuses":
            rec["statuses"] = payload
            self.ledger.note_answer(action, payload)
        self.steps.append(rec)
        try:
            self.stopper.check()  # сигнал во время POST: наблюдение пропускается, сразу очистка
            answer_error = None
            try:
                self.check_answer(rec, expect)
            except Abort as error:
                answer_error = error
                rec["answer_abort"] = str(error)
            try:
                self.observe(rec, refs, may_kill, may_create)
            except Interrupted:
                raise
            except Abort as error:
                rec["observe_abort"] = str(error)
                if answer_error is None:
                    raise
            if answer_error is not None:
                raise answer_error
        finally:
            self.print_step(rec)
        return rec

    def check_answer(self, rec, expect):
        step = rec["step"]
        if "transport_error" in rec:
            raise Abort(f"{step}: transport failure {rec['transport_error']}")
        if rec.get("http_status") != 200:
            raise Abort(f"{step}: HTTP {rec.get('http_status')}: {rec.get('text', '')[:200]}")
        if rec.get("body") is None:
            raise Abort(f"{step}: the answer is not JSON")
        if is_abort_text(rec.get("text", "")):
            raise Abort(f"{step}: {rec.get('text', '')[:200]}")
        flags = text_flags(rec.get("text", ""))
        if flags:  # не аборт: ответ записан, но читать его глазами (FLAG_TEXTS)
            rec["text_flags"] = flags
        shape, statuses = rec["shape"], rec.get("statuses") or []
        text = rec.get("text", "")[:200]
        if shape == "other":
            raise Abort(f"{step}: an answer of unknown shape: {text}")
        if shape == "statuses" and len(statuses) != action_items(rec["action"]):
            raise Abort(f"{step}: {len(statuses)} statuses for {action_items(rec['action'])} orders: {text}")
        kinds = [status_kind(s) for s in statuses]
        if "filled" in kinds:
            raise Abort(f"{step}: the answer says a probe order FILLED: {text}")
        if "other" in kinds:
            raise Abort(f"{step}: a status of unknown shape: {text}")
        if expect == "resting" and not (shape == "statuses" and kinds == ["resting"]):
            raise Abort(f"{step}: the placement did not rest: {text}")
        if expect == "success" and not (shape == "statuses" and kinds and set(kinds) == {"success"}):
            raise Abort(f"{step}: the cancel was not confirmed: {text}")
        if expect == "modified":
            ok_default = rec["action"]["type"] == "modify" and shape == "default"
            ok_statuses = shape == "statuses" and kinds and set(kinds) == {"resting"}
            if not (ok_default or ok_statuses):
                raise Abort(f"{step}: the modify was refused: {text}")
        if expect == "refused":
            refused = shape == "err" or (shape == "statuses" and kinds and set(kinds) == {"error"})
            if not refused:
                raise Abort(f"{step}: the venue was obliged to refuse and did not: {text}")

    def observe(self, rec, refs, may_kill, may_create):
        step = rec["step"]
        self.stopper.sleep(self.timing.observe_s)
        b1 = self.budget()
        rec["budget_after"] = b1
        rec["d_used"] = b1["used"] - rec["budget_before"]["used"]
        if isinstance(rec.get("cost"), int) and rec["d_used"] > rec["cost"]:
            rec["charged_over_predicted"] = rec["d_used"] - rec["cost"]
            self.client.budget.true_up("plan", rec["charged_over_predicted"])
        frames = self.examine()
        send_ms = rec["t_send_ns"] // 1_000_000
        entries = self.run_entries(frames)
        for e in entries:  # запись, проштампованная раньше этого POST, — от прошлого шага
            ts = e.get("statusTimestamp")
            e["stamped_before_send"] = isinstance(ts, int) and ts < send_ms - self.timing.stamp_skew_ms
        rec["ws_entries"] = entries
        rec["ws_frames"] = self.keep_frames(frames, entries)
        fills = self.run_fills(frames)
        if fills:
            rec["fills"] = fills
            raise Abort(f"{step}: a probe order FILLED: {fills}")
        if self.stream.dead:
            raise Abort(f"{step}: the account stream died: {self.stream.dead}")

        sleep = self.stopper.sleep
        open_all = list_open_orders(self.client, self.master, retries=2, sleep=sleep)
        on_coin = [o for o in open_all if o.get("coin") == self.coin]
        run_open = [o for o in on_coin if self.ledger.is_run_cloid(o.get("cloid")) or o.get("oid") in self.ledger.oids]
        for o in run_open:
            self.ledger.note(o.get("cloid"), o.get("oid"))
        rec["frontend_open"] = run_open
        # oid в реестре — со слов ответа биржи на наше действие. Строка с таким oid без cloid прогона
        # (на любой монете) — ответ и список противоречат друг другу: ответ соврал или правка сбросила
        # cloid. План дальше не идёт (цели правок по oid берутся из реестра), очистка такую строку не
        # тронет (sweep_targets), а причина называется прямо — не «префикс прогона».
        disowned = [
            {"coin": o.get("coin"), "oid": o.get("oid"), "cloid": o.get("cloid")} for o in open_all
            if o.get("oid") in self.ledger.oids and not self.ledger.is_run_cloid(o.get("cloid"))
        ]
        if disowned:
            rec["disowned"] = disowned
            raise Abort(f"{step}: {describe_disowned(disowned)}")
        alien = [o for o in on_coin if is_probe_cloid(o.get("cloid")) and o not in run_open]
        if alien:
            raise Abort(f"{step}: another probe run's orders are open on this account: {alien}")
        oo = list_open_orders(self.client, self.master, kind="openOrders", retries=2, sleep=sleep)
        rec["open_orders"] = [o for o in oo if o.get("coin") == self.coin and o.get("oid") in self.ledger.oids]

        query = list(refs)
        for target in self.targets_of(rec["action"]):
            query.append(target)
            if isinstance(target, str):
                query.extend(self.ledger.chains.get(target.lower(), []))
        seen, statuses = set(), {}
        for ref in query:
            key = str(ref).lower()
            if key in seen:
                continue
            seen.add(key)
            statuses[str(ref)] = self.client.info(
                {"type": "orderStatus", "user": self.master, "oid": ref}, retries=1, sleep=sleep
            )
        rec["order_status"] = statuses

        # Состояние после шага — и его инварианты.
        self.refresh_book()
        live = {}
        for o in run_open:
            cloid = (o.get("cloid") or "").lower()
            key = cloid or f"oid:{o.get('oid')}"
            if cloid and cloid not in self.ledger.cloids:
                raise Abort(f"{step}: an open order with this run's prefix the probe never minted: {o}")
            if key in live:
                rec["duplicate_cloid"] = cloid
                raise Abort(f"{step}: two open orders share cloid {cloid}")
            px, sz = float(o["limitPx"]), float(o["sz"])
            if o.get("side") != "B" or px > (1.0 - MIN_DISTANCE) * self.book["ref"] or px * sz > MAX_NOTIONAL:
                raise Abort(f"{step}: a probe order breaks a guardrail now: {o}")
            verdict = self.ledger.size_verdict(cloid, o["limitPx"], o["sz"]) if cloid else "unexpected"
            if verdict == "partial":
                raise Abort(f"{step}: a probe order is smaller than asked — partially FILLED? {o}")
            if verdict != "ok":
                raise Abort(f"{step}: a probe order rests at a price/size the probe never asked for: {o}")
            live[key] = {"oid": o["oid"], "px": px, "sz": sz, "origSz": o.get("origSz")}
        if len(live) > MAX_OPEN_RUN_ORDERS:
            raise Abort(f"{step}: {len(live)} probe orders are open")
        before = set(rec["live_before"])
        vanished = before - set(live)
        appeared = set(live) - before
        rec["live_after"] = live
        rec["vanished"] = sorted(vanished)
        rec["appeared"] = sorted(appeared)
        self.live = live
        if vanished - may_kill:
            raise Abort(f"{step}: probe orders vanished unasked: {sorted(vanished - may_kill)}")
        if appeared - may_create:
            raise Abort(f"{step}: probe orders appeared unasked: {sorted(appeared - may_create)}")

    def print_step(self, rec):
        st = rec.get("statuses")
        what = rec["shape"] if st is None else [status_kind(s) for s in st]
        note = rec.get("answer_abort") or rec.get("observe_abort") or ""
        flags = rec.get("text_flags")
        say(
            f"{rec['step']:>4} #{rec.get('post_no')} {rec['action']['type']:<13} -> {what} "
            f"cost={rec.get('cost')} d_used={rec.get('d_used')}{' OVER-CAP' if rec['over_cap'] else ''}"
            f"  {rec['purpose']}{'  ?? the answer mentions ' + ', '.join(flags) if flags else ''}"
            f"{'  !! ' + note if note else ''}"
        )

    def oid_of(self, cloid: str) -> int:
        entry = self.live.get(cloid.lower())
        if entry is None:
            raise Abort(f"{cloid} is not resting")
        return entry["oid"]


def run_plan(p: Probe) -> None:
    """S1–S21. Каждый шаг — один подписанный POST; таблица Q — нота §9."""
    led = p.ledger
    v: dict = {}  # решённое на шаге и нужное дальше

    c1 = led.mint()

    def s1():
        v["px1"] = p.px(0.90)
        v["sz"] = p.size(TARGET_NOTIONAL, v["px1"])
        return p.place(c1, v["px1"], v["sz"])

    rec = p.act("S1", "place ALO C1", "order", s1, expect="resting", refs=[c1])
    o1 = rec["statuses"][0]["resting"]["oid"]

    # Q1, Q2, Q4: одиночный modify по oid, cloid сохранён.
    p.act("S2", "modify (single) by oid, cloid kept", "modify",
          lambda: p.modify_one(o1, p.wire(c1, p.px(0.89), v["sz"])), expect="modified", refs=[o1, c1])

    # Q2, Q3, Q4: batchModify по cloid.
    def s3():
        v["px3"] = p.px(0.88)
        return p.batch_modify([(c1, p.wire(c1, v["px3"], v["sz"]))])

    p.act("S3", "batchModify by cloid, cloid kept", "batchModify", s3, expect="modified", refs=[c1])

    # Q5: та же цена, меньше объём, цель — текущий oid.
    def s4():
        v["sz_down"] = p.size(DOWN_NOTIONAL, v["px3"])
        return p.batch_modify([(p.oid_of(c1), p.wire(c1, v["px3"], v["sz_down"]))])

    p.act("S4", "batchModify by current oid: same price, smaller size", "batchModify", s4,
          expect="modified", refs=[c1])

    # Q6: второй ордер и batchModify из двух.
    c2 = led.mint()

    def s5():
        v["px5"] = p.px(0.91)
        v["sz5"] = p.size(TARGET_NOTIONAL, v["px5"])
        return p.place(c2, v["px5"], v["sz5"])

    p.act("S5", "place ALO C2", "order", s5, expect="resting", refs=[c2])
    p.act("S6", "batchModify of two orders", "batchModify",
          lambda: p.batch_modify([(c1, p.wire(c1, p.px(0.90), v["sz_down"])),
                                  (c2, p.wire(c2, p.px(0.89), v["sz5"]))]),
          expect="modified", refs=[c1, c2])

    # Q7: отказы, живые ордера не задеты.
    phantom = led.mint()  # никогда не ставился
    p.act("S7", "modify (single) of a never-placed cloid", "modify",
          lambda: p.modify_one(phantom, p.wire(phantom, p.px(0.885), v["sz"])),
          expect="any", refs=[phantom], may_create=[phantom])
    p.act("S8", "batchModify of a never-placed cloid", "batchModify",
          lambda: p.batch_modify([(phantom, p.wire(phantom, p.px(0.885), v["sz"]))]),
          expect="any", refs=[phantom], may_create=[phantom])
    p.act("S9", "modify (single) by a superseded oid", "modify",
          lambda: p.modify_one(o1, p.wire(c1, p.px(0.885), v["sz_down"])), expect="any", refs=[o1, c1])
    p.act("S10", "batchModify by a superseded oid", "batchModify",
          lambda: p.batch_modify([(o1, p.wire(c1, p.px(0.885), v["sz_down"]))]), expect="any", refs=[o1, c1])

    # Q8: новый cloid в правке.
    c3 = led.mint()
    p.act("S11", "batchModify of C2 carrying a NEW cloid C3", "batchModify",
          lambda: p.batch_modify([(c2, p.wire(c3, p.px(0.895), v["sz5"]))]),
          expect="any", refs=[c2, c3], may_kill=[c2], may_create=[c3])
    second = c3 if c3 in p.live else c2

    # Q9: отмена по cloid после правки.
    p.act("S12", "cancelByCloid after a modify", "cancelByCloid", lambda: p.cancel_by_cloid([second]),
          expect="success", refs=[c2, c3], may_kill=[second])

    # Q7: правка снятого ордера.
    p.act("S13", "modify (single) of a canceled order", "modify",
          lambda: p.modify_one(second, p.wire(second, p.px(0.89), v["sz5"])),
          expect="any", refs=[second], may_create=[second])
    p.act("S14", "batchModify of a canceled order", "batchModify",
          lambda: p.batch_modify([(second, p.wire(second, p.px(0.89), v["sz5"]))]),
          expect="any", refs=[second], may_create=[second])

    # Q10: замена ниже минимума биржи — биржа ОБЯЗАНА отказать; жив ли старый ордер.
    def below_min(builder):
        def build():
            pxb = p.px(0.89)
            return builder(p.wire(c1, pxb, p.bad_size(pxb), allow_below_min=True))
        return build

    if c1 in p.live:
        p.act("S15", "batchModify to a size below the venue minimum (atomicity)", "batchModify",
              below_min(lambda w: p.batch_modify([(c1, w)])), expect="refused", refs=[c1], may_kill=[c1])
    else:
        p.skip("S15", "C1 is not resting")
    if c1 in p.live:
        p.act("S16", "modify (single) to a size below the venue minimum (atomicity)", "modify",
              below_min(lambda w: p.modify_one(c1, w)), expect="refused", refs=[c1], may_kill=[c1])
    else:
        p.skip("S16", "C1 is not resting")

    # Q9: отмена по cloid после нескольких правок.
    if c1 in p.live:
        p.act("S17", "cancelByCloid after modifies", "cancelByCloid", lambda: p.cancel_by_cloid([c1]),
              expect="success", refs=[c1], may_kill=[c1])
    else:
        p.skip("S17", "C1 is not resting")

    # Q12, Q13 — на свежем ордере C4 не дороже $11 (C1 к этому моменту снят или умер).
    c4 = led.mint()
    p.act("S18", "place ALO C4 of at most $11 (for Q12, Q13)", "order",
          lambda: p.place(c4, *p.small(0)), expect="resting", refs=[c4])
    # Q12+: expiresAfter = подпись + 10 с, как пошлёт коннектор (нота §5.1) — обязан быть принят.
    p.act("S19", "batchModify with expiresAfter = signing + 10 s", "batchModify",
          lambda: p.batch_modify([(c4, p.wire(c4, *p.small(1)))]),
          expect="modified", refs=[c4], expires_in_s=EXPIRY_S)
    # Q12−: expiresAfter уже 5 с в прошлом — форма отказа, Δ бюджета, цел ли C4 (P1, нота §1.1).
    # Исполнится вопреки P1 — новая цена и объём всё равно внутри гардрейлов и ≤ $11.
    p.act("S20", "batchModify with expiresAfter 5 s in the past", "batchModify",
          lambda: p.batch_modify([(c4, p.wire(c4, *p.small(2)))]),
          expect="any", refs=[c4], may_kill=[c4], expires_in_s=-EXPIRED_BY_S)
    # Q13: ALO-правка через спред. Отказ с живым C4 или отказ с убитым — оба ответа; встала —
    # гардрейл 5 % в observe, исполнилась — филл: аборт, очистка снимает остальное.
    if c4 in p.live:
        p.act("S21", "batchModify of an ALO buy through the best ask (must be refused)", "batchModify",
              lambda: p.batch_modify([(c4, p.cross_wire(c4))]),
              expect="refused", refs=[c4], may_kill=[c4], expires_in_s=EXPIRY_S)
    else:
        p.skip("S21", "C4 is not resting")


# --- Добор хвостов и очистка ----------------------------------------------------------------


def _errtext(error: BaseException) -> str:
    return str(error) if isinstance(error, Abort) else repr(error)


def _list_open(client, master, timing, deadline, report, tries=6):
    """frontendOpenOrders с повторами — только список правильной формы (list_open_orders).
    None — не удалось до дедлайна или за `tries` попыток; каждая неудача — в отчёте."""
    for attempt in range(tries):
        if time.monotonic() > deadline:
            report["errors"].append("deadline")
            return None
        try:
            return list_open_orders(client, master)
        except Exception as error:  # noqa: BLE001 — транспорт, HTTP, форма ответа и прочее
            report["errors"].append(_errtext(error))
        if attempt + 1 < tries:
            time.sleep(timing.retry_s * (attempt + 1))
    return None


def sweep(client, master, coin, asset, ledger_oids, *, pool, timing, label, max_cancels=3) -> dict:
    """Снимает по oid строки с cloid формы пробника на монете (sweep_targets) — и ничего больше.

    ledger_oids — только для отчёта: строку с oid из реестра без cloid формы пробника не снимает
    никто, она уходит в `disowned` (disowned_rows). Не бросает: любой сбой — в errors; left = None —
    проверить не удалось, > 0 — не снято. Сигналы её не прерывают (обработчик только ставит флаг);
    /info повторяется, список принимается только правильной формы; «чисто» — два пустых списка
    подряд с паузой settle (ордер после сбоя транспорта ложится позже); дедлайн — очистка кончается
    всегда."""
    report = {"label": label, "cancels": [], "left": None, "errors": [], "other_open": None, "disowned": []}
    deadline = time.monotonic() + timing.cleanup_deadline_s
    empty_seen = False
    while True:
        open_all = _list_open(client, master, timing, deadline, report)
        if open_all is None:
            report["left"] = None
            break
        targets = sweep_targets(open_all, coin)
        report["disowned"] = disowned_rows(open_all, coin, ledger_oids)
        report["other_open"] = len(open_all) - len(targets)
        report["left"] = len(targets)
        if not targets:
            if empty_seen:
                break
            empty_seen = True
            time.sleep(timing.settle_s)
            continue
        empty_seen = False
        if max_cancels <= 0:
            report["errors"].append("cancel rounds exhausted")
            break
        batch = targets[: max(0, client.budget.room(pool))]
        if not batch:
            report["errors"].append(f"{pool} budget exhausted")
            break
        max_cancels -= 1
        action = {"type": "cancel", "cancels": [{"a": asset, "o": oid} for oid in batch]}
        try:
            # listing: рубеж (assert_action_safe) сам сверяет каждый oid со списком биржи.
            res = client.exchange(action, pool=pool, cost=len(batch), listing=open_all)
        except Exception as error:  # noqa: BLE001 — гардрейл рубежа и прочее: не снято, left > 0
            report["errors"].append(_errtext(error))
            break
        report["cancels"].append({"oids": batch, **{k: v for k, v in res.items() if k != "text"}})
        time.sleep(timing.settle_s)
    return report


def start_sweep(p: Probe) -> dict:
    """Хвосты прошлых прогонов (та же форма cloid) на монете — до первого шага плана."""
    rep = sweep(p.client, p.master, p.coin, p.asset, set(), pool="sweep", timing=p.timing, label="start")
    p.start_report = rep  # в результаты — и когда добор не удался и план не начался
    p.attribute_late(p.examine(), raise_on_fill=False, bucket=p.pre_frames)
    if rep["cancels"]:
        say(f"start sweep: cancelled {sum(len(c['oids']) for c in rep['cancels'])} leftover probe order(s)")
    if rep["left"] != 0:
        raise Abort(f"leftover probe orders could not be cleared before the plan: {rep}")
    return rep


def cleanup(p: Probe) -> dict:
    """Снимает ордера пробника (этот прогон и хвосты любых прошлых) на монете, по oid — только строки
    с cloid формы пробника; строки, которые ответ биржи приписал прогону без такого cloid, — в отчёт."""
    if p.client.dry_run:
        return {"left": 0, "cancels": [], "errors": [], "other_open": None, "disowned": []}
    rep = sweep(p.client, p.master, p.coin, p.asset, set(p.ledger.oids), pool="cleanup", timing=p.timing, label="end")
    say(
        f"probe orders still open on {p.coin}: {rep['left']}; other open orders on the account: "
        f"{rep['other_open']} (untouched){'; errors: ' + '; '.join(rep['errors']) if rep['errors'] else ''}"
    )
    if rep["disowned"]:
        warn(f"never touched: {describe_disowned(rep['disowned'])}")
    return rep


def final_check(p: Probe, pre: dict) -> dict:
    """После очистки: филлы пробника по монете (userFills) и позиция по монете против старта.
    Не бросает: сбой любой части — в errors (это код 2, а свёртка идёт дальше)."""
    out = {"errors": [], "probe_fills": None, "position_start": pre.get("position"), "position_end": None,
           "position_changed": None}
    since = p.t_start_ms - 60_000
    try:
        fills = p.client.info({"type": "userFills", "user": p.master}, retries=3)
        if not isinstance(fills, list) or not all(isinstance(f, dict) for f in fills):
            raise Abort(f"/info userFills of an unexpected shape: {json.dumps(fills, default=repr)[:160]}")
        out["probe_fills"] = [
            f for f in fills
            if f.get("coin") == p.coin and int(f.get("time") or 0) >= since
            and (is_probe_cloid(f.get("cloid")) or f.get("oid") in p.ledger.oids)
        ]
    except Exception as error:  # noqa: BLE001
        out["errors"].append(f"userFills: {_errtext(error)}")
    try:
        state = p.client.info({"type": "clearinghouseState", "user": p.master}, retries=3)
        out["position_end"] = position_of(state, p.coin)
        if out["position_end"] is None:
            raise Abort(f"/info clearinghouseState of an unexpected shape: {json.dumps(state, default=repr)[:160]}")
        if out["position_start"] is not None:
            out["position_changed"] = Decimal(out["position_end"]) != Decimal(out["position_start"])
    except Exception as error:  # noqa: BLE001
        out["errors"].append(f"clearinghouseState: {_errtext(error)}")
    if out["probe_fills"]:
        warn(f"PROBE FILLS on {p.coin}: {out['probe_fills']}")
    if out["position_changed"]:
        warn(f"POSITION on {p.coin} changed during the run: {out['position_start']} -> {out['position_end']}")
    if out["errors"]:
        warn(f"final check incomplete: {'; '.join(out['errors'])}")
    return out


# --- Сводка ---------------------------------------------------------------------------------


def brief_status(value):
    """orderStatus → {status, oid, cloid} | 'unknownOid' | как есть."""
    if not isinstance(value, dict):
        return value
    if value.get("status") != "order":
        return value.get("status")
    order = value.get("order") or {}
    inner = order.get("order") or {}
    return {"status": order.get("status"), "oid": inner.get("oid"), "cloid": inner.get("cloid")}


def summarize(steps: list[dict], ledger: Ledger | None = None) -> dict:
    by = {s["step"]: s for s in steps if "skipped" not in s}

    def brief(sid):
        s = by.get(sid)
        if s is None:
            skipped = next((x["skipped"] for x in steps if x["step"] == sid and "skipped" in x), None)
            return {"skipped": skipped} if skipped else None
        return {
            "shape": s["shape"],
            "statuses": s.get("statuses"),
            "body": s.get("body") if s["shape"] != "statuses" else None,
            "transport_error": s.get("transport_error"),
            "cost": s.get("cost"),
            "d_used": s.get("d_used"),
            "ws": [(e["frame"], e["idx"], e["n_in_frame"], e["oid"], e["cloid"], e["status"],
                    e.get("stamped_before_send")) for e in s.get("ws_entries", [])],
            "late_ws": [(e["frame"], e["oid"], e["cloid"], e["status"]) for e in s.get("late_ws_entries", [])],
            "order_status": {k: brief_status(v) for k, v in (s.get("order_status") or {}).items()},
            "live_before": s.get("live_before"),
            "live_after": s.get("live_after"),
            "vanished": s.get("vanished"),
            "appeared": s.get("appeared"),
            "abort": s.get("answer_abort") or s.get("observe_abort"),
            "text_flags": s.get("text_flags"),
        }

    def accepted(sid):
        s = by.get(sid)
        if s is None:
            return None
        return s["shape"] == "statuses" and [status_kind(x) for x in s.get("statuses") or []] == ["resting"]

    def refused(sid):
        """Отказ целиком: top-level err или error в каждом статусе. filled — не отказ."""
        s = by.get(sid)
        if s is None or "transport_error" in s:
            return None
        kinds = [status_kind(x) for x in s.get("statuses") or []]
        return s["shape"] == "err" or (s["shape"] == "statuses" and bool(kinds) and set(kinds) == {"error"})

    def old_order_fate(sid):
        """Чем кончился ордер, который шаг правил: тот же oid стоит / другой oid / нет его."""
        s = by.get(sid)
        if s is None or not s.get("live_before"):
            return None
        target = str(s["action"]["modifies"][0]["oid"]).lower() if s["action"]["type"] == "batchModify" else None
        old = (s.get("live_before") or {}).get(target) or {}
        old_oid = old.get("oid")
        after = (s.get("live_after") or {}).get(target)
        return {
            "old_oid": old_oid,
            "order_status_of_old_oid": brief_status((s.get("order_status") or {}).get(str(old_oid))),
            "resting_same_oid": bool(after and after.get("oid") == old_oid),
            "resting_new_oid": after.get("oid") if after and after.get("oid") != old_oid else None,
            "ws_for_old_oid": [(e["status"], e["frame"], e["late"]) for e in
                               s.get("ws_entries", []) + s.get("late_ws_entries", []) if e["oid"] == old_oid],
        }

    budget = {}
    for s in steps:
        if "skipped" not in s:
            budget.setdefault(s["action"]["type"], []).append((s["step"], s.get("cost"), s.get("d_used")))
    s20 = by.get("S20")
    return {
        "Q1_single_modify_ack": brief("S2"),
        "Q2_Q3_Q4_modify_streams": {sid: brief(sid) for sid in ("S2", "S3", "S4", "S6")},
        "Q5_size_down_same_price": brief("S4"),
        "Q6_batch_of_two": brief("S6"),
        "Q7_refusals": {sid: brief(sid) for sid in ("S7", "S8", "S9", "S10", "S13", "S14")},
        "Q8_new_cloid": brief("S11"),
        "Q9_cancel_after_modify": {sid: brief(sid) for sid in ("S12", "S17")},
        "Q10_atomicity": {sid: brief(sid) for sid in ("S15", "S16")},
        "Q11_cost_vs_d_used_by_action": budget,
        # P1 (нота §1.1): с expiresAfter в будущем принят, в прошлом — отвергнут, ордер цел.
        "Q12_expires_after": {
            "future_accepted": accepted("S19"),
            "past_refused": refused("S20"),
            "past_order_untouched": kept_untouched(s20),
            "past_charged_d_used": s20.get("d_used") if s20 else None,
            "past_predicted_cost": EXPIRED_COST,
            "S19": brief("S19"),
            "S20": brief("S20"),
        },
        # Ветки нота §10: (А) отказ, ордер цел; (Б) отказ, ордер снят; (В)/(Г) — аборт пробника.
        "Q13_alo_cross": {
            "refused": refused("S21"),
            "old_order_untouched": kept_untouched(by.get("S21")),
            "old_order": old_order_fate("S21"),
            "S21": brief("S21"),
        },
        "P3_oid_monotonic": oid_monotonic(steps),
        "replacement_sizes": replacement_sizes(steps),
        "ledger_oid_chains": {c: o for c, o in ledger.chains.items()} if ledger else None,
        "late_frames_by_step": {s["step"]: len(s["late_ws_entries"]) for s in steps if s.get("late_ws_entries")},
        "over_cap_any": any(s.get("over_cap") for s in steps if "skipped" not in s),
    }


def print_summary(summary: dict) -> None:
    def one(label, b):
        if not b:
            return f"{label}: —"
        if b.get("skipped"):
            return f"{label}: skipped ({b['skipped']})"
        st = b.get("statuses")
        what = [status_kind(x) for x in st] if st else b.get("shape")
        body = b.get("body")
        flags = f" ?? mentions {', '.join(b['text_flags'])}" if b.get("text_flags") else ""
        return (f"{label}: {what}{' ' + json.dumps(body)[:160] if body else ''} cost={b.get('cost')} "
                f"d_used={b.get('d_used')}{flags}")

    say("--- summary ---")
    say(one("Q1  S2  modify", summary["Q1_single_modify_ack"]))
    say(one("Q5  S4  same px, smaller", summary["Q5_size_down_same_price"]))
    say(one("Q6  S6  batch of two", summary["Q6_batch_of_two"]))
    for sid, b in summary["Q7_refusals"].items():
        say(one(f"Q7  {sid}", b))
    say(one("Q8  S11 new cloid", summary["Q8_new_cloid"]))
    for sid, b in summary["Q10_atomicity"].items():
        say(one(f"Q10 {sid}", b))
    q12 = summary["Q12_expires_after"]
    say(one("Q12 S19 expiresAfter +10 s", q12["S19"]))
    say(one("Q12 S20 expiresAfter -5 s", q12["S20"]) + f" (predicted {q12['past_predicted_cost']})")
    say(f"Q12 future_accepted={q12['future_accepted']} past_refused={q12['past_refused']} "
        f"past_order_untouched={q12['past_order_untouched']}")
    q13 = summary["Q13_alo_cross"]
    say(one("Q13 S21 ALO through the ask", q13["S21"]))
    say(f"Q13 refused={q13['refused']} old_order_untouched={q13['old_order_untouched']} old_order={q13['old_order']}")
    p3 = summary["P3_oid_monotonic"]
    say(f"P3  oid strictly increasing per cloid chain: {p3['strictly_increasing']} {p3['chains']}")
    for r in summary["replacement_sizes"]:
        say(f"origSz {r['step']} {str(r['cloid'])[-6:]}: sent s={r['s']} origSz={r['origSz']} sz={r['sz']} equal={r['equal']}")
    if summary["late_frames_by_step"]:
        say(f"late orderUpdates entries by step: {summary['late_frames_by_step']}")


# --- Ключи, префлайт ------------------------------------------------------------------------


def read_credentials(path: Path) -> dict:
    try:
        st = path.stat()
    except OSError as error:
        raise Refuse(f"cannot read the credentials file: {error}") from None
    if st.st_mode & (stat.S_IRWXG | stat.S_IRWXO):
        warn(f"WARNING: {path} is readable by group/others; chmod 600 it")
    try:
        toml = _toml()
    except ModuleNotFoundError:
        raise Refuse("reading the credentials needs tomllib (Python ≥ 3.11) or tomli") from None
    try:
        text = path.read_text()
    except OSError as error:
        raise Refuse(f"cannot read the credentials file: {error}") from None
    try:
        cfg = toml.loads(text)
    except (toml.TOMLDecodeError, UnicodeDecodeError):
        # Сообщение парсера процитировало бы строку с ключом — и в цепочке исключений тоже.
        raise Refuse(f"{path} is not valid TOML (the parser's message is withheld: it would quote the key)") from None
    refuse_mainnet_config(cfg)
    for key in ("api_wallet_private_key", "master_account_address"):
        if not isinstance(cfg.get(key), str) or not cfg[key].strip():
            raise Refuse(f"{key} is missing from {path}")
    return cfg


def keys_of_credentials(sdk, path):
    """(адрес API-кошелька, адрес мастера) чужого файла ключей — только для сравнения."""
    try:
        cfg = _toml().loads(Path(path).read_text())
        agent = sdk.Account.from_key(cfg["api_wallet_private_key"].strip()).address.lower()
        return agent, str(cfg.get("master_account_address") or "").strip().lower() or None
    except Exception:  # noqa: BLE001
        return None, None


def preflight(client: Client, coin: str, agent: str, *, for_plan: bool = True) -> dict:
    """Только /info. Невыполнимый план — Guardrail/Refuse до любого действия. for_plan=False
    (--sweep-only): только то, что нужно снять хвосты, — выполнимость плана и часы не мешают."""
    meta = client.info({"type": "meta"}, retries=1)
    names = [u["name"] for u in meta["universe"]]
    if coin not in names:
        raise Refuse(f"{coin} is not in the testnet perp universe")
    asset = names.index(coin)
    szd = int(meta["universe"][asset]["szDecimals"])
    agents = client.info({"type": "extraAgents", "user": client.master}, retries=1) or []
    if not any(str(a.get("address", "")).lower() == agent for a in agents):
        raise Refuse("the API wallet is not an approved agent of the master account (approve it in the testnet UI)")
    rl = parse_rate_limit(client.info({"type": "userRateLimit", "user": client.master}, retries=1))
    open_all = list_open_orders(client, client.master, retries=1)
    state = client.info({"type": "clearinghouseState", "user": client.master}, retries=1)
    book = fetch_book(client, coin)
    base = {
        "asset": asset,
        "szDecimals": szd,
        "budget": rl,
        "other_open_orders": len(open_all),
        "leftovers": sweep_targets(open_all, coin),
        "position": position_of(state, coin),
        "book": book,
        "clock_lead_ms": client.clock_lead_ms,
    }
    if not for_plan:
        return base
    lead = client.clock_lead_ms
    if lead is None:
        warn("WARNING: /info answers carry no Date header: our clock is not checked against the venue (Q12)")
    elif abs(lead) > MAX_CLOCK_SKEW_MS:
        raise Refuse(f"our clock is {lead} ms off the venue's (HTTP Date): Q12's +{EXPIRY_S:g}/-{EXPIRED_BY_S:g} s "
                     "expiry windows would not mean what they say; sync the clock")
    # План выполним на этой сетке лотов?
    for fraction, notional in ((0.88, TARGET_NOTIONAL), (0.91, TARGET_NOTIONAL), (0.88, DOWN_NOTIONAL)):
        px = floor_px(fraction * book["ref"], szd)
        check_order_guardrails(px, ceil_sz(notional / px, szd), book["ref"], tif="Alo", is_buy=True)
    if floor_sz(BAD_NOTIONAL / floor_px(0.89 * book["ref"], szd), szd) <= 0:
        raise Refuse("the lot is too coarse for the sub-minimum step; pick another --coin")
    small = [small_order(book["ref"], szd, extra_lots=k) for k in (0, 1, 2)]
    cross = cross_order(book, szd)
    if None in small or cross is None:
        raise Guardrail(f"S18–S21 need $10–11 orders (Q13: through the ask) and {coin}'s lot grid does not "
                        "allow them at this price; pick a coin with a finer lot (e.g. --coin ETH)")
    check_cross_guardrails(*cross, book, tif="Alo", is_buy=True)
    for px, sz in small:
        check_order_guardrails(px, sz, book["ref"], tif="Alo", is_buy=True)
    return {**base, "small": small, "cross": cross}


def write_results(out_dir: Path, name: str, result: dict) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = out_dir / f"{stamp}_{name}.json"
    path.write_text(json.dumps(result, indent=1, default=str))
    return path


def run(args, out_dir: Path) -> int:
    venue = fake_venue(args.fake_venue) if args.fake_venue is not None else TESTNET
    assert_venue_url(venue.rest, venue)
    assert_venue_url(venue.ws, venue)
    timing = FAST if venue.fake else REAL
    lock = acquire_lock(Path(args.state_dir).expanduser())  # noqa: F841 — держится до выхода
    # Терминал может пропасть посреди прогона (обрыв SSH): тогда вывод — сюда, рядом с результатами.
    console_log_to(out_dir / f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}_hl_modify_probe_console.log")
    try:  # символ, которого не знает кодировка терминала, — заменой, а не потерянной строкой
        sys.stdout.reconfigure(errors="backslashreplace")
    except (AttributeError, ValueError):
        pass
    sdk = load_sdk()
    check_sdk(sdk)
    if venue.fake:
        key, master, cfg_path, creds = HARDHAT_KEY, FAKE_MASTER, None, {}
        say("fake venue on 127.0.0.1: public test key, no real funds; shared-wallet check skipped")
    else:
        cfg_path = Path(args.config).expanduser()
        creds = read_credentials(cfg_path)
        key, master = creds["api_wallet_private_key"].strip(), creds["master_account_address"].strip().lower()
    wallet = sdk.Account.from_key(key)
    agent = wallet.address.lower()
    if not re.fullmatch(r"0x[0-9a-f]{40}", master):
        raise Refuse("master_account_address is not an address")
    if master == agent:
        raise Refuse("master_account_address is the API wallet itself; queries would return nothing")
    if not venue.fake:
        claimed = str(creds.get("api_wallet_address") or "").strip().lower()
        if claimed and claimed != agent:
            raise Refuse("api_wallet_address does not match the key")
        refusals, warnings = shared_wallet_conflicts(
            cfg_path, agent, master, args.coin, running_hl_connectors(),
            lambda path: keys_of_credentials(sdk, path),
        )
        for w in warnings + refusals:
            warn(f"WARNING: {w}")
        if refusals:
            raise Refuse("this API wallet is in use by a running connector; use a dedicated one")

    stopper = Stopper()
    stopper.install()
    budget = Budget({"plan": 0, "cleanup": 0, "sweep": SWEEP_ONLY_BUDGET} if args.sweep_only else None)
    client = Client(sdk, wallet, master, venue, timing, dry_run=args.dry_run, budget=budget)
    try:
        pre = preflight(client, args.coin, agent, for_plan=not args.sweep_only)
    except Refuse:
        raise
    except Exception as error:  # noqa: BLE001 — только /info, ничего ещё не отправлено: отказ, не аборт
        raise Refuse(_errtext(error)) from None
    if stopper.requested:
        raise Refuse(f"stopped by {stopper.reason} before anything was sent")
    ledger = Ledger(make_run_prefix(secrets.randbits(32)))
    client.bind(pre["asset"], ledger.owns, args.coin)
    say(
        f"{'FAKE' if venue.fake else 'testnet'} {args.coin} asset={pre['asset']} szDecimals={pre['szDecimals']} "
        f"mid={pre['book']['mid']} ref={pre['book']['ref']} ask={pre['book']['best_ask']} "
        f"clock_lead_ms={pre['clock_lead_ms']} budget={pre['budget']} position={pre['position']} "
        f"other open orders={pre['other_open_orders']} leftover probe orders={len(pre['leftovers'])} "
        f"run prefix={ledger.run_prefix} sdk={sdk.version}"
    )
    if pre["budget"]["used"] >= pre["budget"]["cap"]:
        say("over the request cap: non-cancel actions are paced; Q11 is measured over the cap")

    if args.dry_run:
        ref, szd = pre["book"]["ref"], pre["szDecimals"]
        for fraction in (0.91, 0.90, 0.895, 0.89, 0.885, 0.88):
            px = floor_px(fraction * ref, szd)
            say(f"  price {fraction:.3f}·ref = {px}  size@${TARGET_NOTIONAL} = {ceil_sz(TARGET_NOTIONAL / px, szd)}")
        px21, sz21 = pre["cross"]
        say(f"  S18–S20 C4 (px, sz) = {pre['small']}; S21 through the ask {pre['book']['best_ask']}: "
            f"{sz21} @ {px21} (${px21 * sz21:.2f})")
        say(f"dry run: steps S1–S21 as in run_plan(); venue requests at most {RUN_CEILING} (plan {PLAN_BUDGET} + "
            f"cleanup {CLEANUP_BUDGET} + sweep {SWEEP_BUDGET}); nothing was signed or sent")
        return 0

    if args.sweep_only:
        stopper.cleaning = True
        rep = sweep(client, master, args.coin, pre["asset"], set(), pool="sweep", timing=timing,
                    label="sweep-only", max_cancels=4)
        say(f"sweep-only: probe orders left on {args.coin}: {rep['left']}; cancels: {len(rep['cancels'])}; "
            f"errors: {rep['errors']}")
        path = None
        try:
            path = write_results(out_dir, "hl_modify_probe_sweep",
                                 {"meta": {"coin": args.coin, "venue": venue.rest}, "sweep": rep})
            say(f"saved {path}")
        except Exception as error:  # noqa: BLE001
            warn(f"RESULTS NOT WRITTEN: {error!r}")
        if rep["left"] != 0:
            warn(f"NOT CLEAN: probe orders left on {args.coin}: {rep['left']} — run --sweep-only again or cancel by hand")
            return 4
        return 0 if path else 2

    if len(pre["leftovers"]) > SWEEP_BUDGET:
        raise Refuse(f"{len(pre['leftovers'])} leftover probe orders on {args.coin} (> {SWEEP_BUDGET}): "
                     "run --sweep-only first")
    try:
        stream = Stream(sdk.websocket, master, venue)
        stream.wait_ready(timing.ws_ready_s)
    except Refuse:
        raise
    except Exception as error:  # noqa: BLE001 — до первого действия: отказ, не аборт
        raise Refuse(f"cannot open the account stream: {error!r}") from None
    probe = Probe(client, stream, ledger, args.coin, pre["asset"], pre["szDecimals"], master, timing, stopper)
    aborted = None
    try:
        start_sweep(probe)
        run_plan(probe)
    except BaseException as error:  # noqa: BLE001 — очистка обязана случиться при любом исходе
        aborted = f"{type(error).__name__}: {error}"
        warn(f"ABORT: {aborted}")

    # Свёртка. Каждый шаг — сам по себе: сбой одного не отменяет следующих, результаты пишутся
    # всегда и до печати, печать не бросает (say). Очистку не удалось проверить — код 4.
    stopper.cleaning = True
    failures: list[str] = []

    def guarded(label, fn, default=None):
        try:
            return fn()
        except BaseException as error:  # noqa: BLE001
            failures.append(f"{label}: {error!r}")
            warn(f"{label} FAILED: {error!r}")
            return default

    guarded("late frames", lambda: probe.attribute_late(probe.examine(), raise_on_fill=False))
    cleanup_rep = guarded("cleanup", lambda: cleanup(probe),
                          {"left": None, "cancels": [], "errors": ["cleanup raised"], "other_open": None,
                           "disowned": []})
    final = guarded("final check", lambda: final_check(probe, pre),
                    {"errors": ["final check raised"], "probe_fills": None, "position_changed": None})
    guarded("pause", lambda: time.sleep(timing.observe_s))
    guarded("late frames after cleanup",
            lambda: probe.attribute_late(probe.examine(), raise_on_fill=False, bucket=probe.after_frames))
    guarded("stream close", stream.close)
    summary = guarded("summary", lambda: summarize(probe.steps, ledger))
    path = None
    result = guarded("results", lambda: {
        "meta": {
            "network": "fake" if venue.fake else "testnet",
            "base": venue.rest,
            "coin": args.coin,
            "asset": pre["asset"],
            "szDecimals": pre["szDecimals"],
            "book_at_start": pre["book"],
            "clock_lead_ms_at_start": pre["clock_lead_ms"],
            "budget_at_start": pre["budget"],
            "position_at_start": pre["position"],
            "over_cap_any": probe.over_cap,
            "run_prefix": ledger.run_prefix,
            "sdk_version": sdk.version,
            "master": f"0x…{master[-4:]}",
            "venue_requests_spent": dict(client.budget.spent),
            "venue_requests_limits": dict(client.budget.limits),
            "posts": client.n_actions,
            "aborted": aborted,
            "signals_ignored_during_cleanup": stopper.ignored,
            "start_sweep": probe.start_report,
            "cleanup": cleanup_rep,
            "final_check": final,
            "wind_down_failures": list(failures),
            "console_log": console_log_used(),
            "note": "frame text: master address replaced by Hardhat #1, as in fixtures.rs",
        },
        "summary": summary if summary is not None else {"error": "the summary failed; the raw steps are saved"},
        "steps": probe.steps,
        "frames_before_plan": probe.pre_frames,
        "frames_after_plan": probe.after_frames,
    })
    if result is not None:
        path = guarded("results", lambda: write_results(out_dir, f"hl_modify_probe_{ledger.run_prefix[6:]}", result))
    if summary is not None:
        guarded("summary printing", lambda: print_summary(summary))
    say(f"venue requests spent: {client.budget.spent} (limits {client.budget.limits}, ceiling {RUN_CEILING}); "
        f"POSTs: {client.n_actions}")
    if path is not None:
        say(f"saved {path}")
    else:
        warn("RESULTS NOT WRITTEN — see the errors above")
    left = cleanup_rep.get("left")
    # Строка без cloid, которую ответ биржи приписал прогону, может быть и нашей (правка сбросила
    # cloid): пробник её не отменяет — опознать нечем, — значит, «чисто» не доказано.
    unnamed = [d["oid"] for d in cleanup_rep.get("disowned") or [] if not d.get("cloid")]
    if left != 0 or unnamed:
        if left != 0:
            warn(f"NOT CLEAN: probe orders left on {args.coin}: {left} — run --sweep-only or cancel by hand")
        if unnamed:
            warn(f"NOT CLEAN: the venue tied oid(s) {unnamed} on {args.coin} to this run, but lists them without a "
                 "cloid — the probe never cancels an order it cannot identify: check them by hand")
        return 4
    if (aborted or final.get("probe_fills") or final.get("position_changed") or final.get("errors") or failures
            or path is None or cleanup_rep.get("disowned")):
        return 2
    return 0


# --- Golden ---------------------------------------------------------------------------------


def golden(out_dir: Path) -> int:
    """Офлайн. Байты msgpack и подписи batchModify от ОФИЦИАЛЬНОГО SDK — для T21, T22, T50.

    Каждый вектор — без expiresAfter (sig_*) и с ним (sig_*_expires, expiresAfter =
    nonce + 10 000 мс, как пошлёт коннектор): нота design-live-modify §5.1.
    """
    sdk = load_sdk()
    try:
        check_sdk(sdk)
    except Refuse as error:
        print(f"REFUSED: {error}; vectors would not be comparable", file=sys.stderr)
        return 3
    try:
        from hyperliquid.exchange import Exchange
        from hyperliquid.utils.constants import TESTNET_API_URL
    except ImportError as error:
        raise Refuse(f"this SDK lacks a golden builder: {error}") from None
    try:  # хэш действия самого SDK — сверка нашего прообраза (action_preimage)
        from hyperliquid.utils.signing import action_hash as sdk_action_hash
    except ImportError:
        sdk_action_hash = None

    wallet = sdk.Account.from_key(HARDHAT_KEY)
    order_a = golden_order(sdk)
    order_b = {  # без cloid: `c` должен отсутствовать, не быть nil
        "coin": "BTC", "is_buy": False, "sz": 1.5, "limit_px": 2000.5,
        "order_type": {"limit": {"tif": "Gtc"}}, "reduce_only": True, "cloid": None,
    }
    captured: list[dict] = []

    class _Info:
        def name_to_asset(self, name):
            return {"BTC": 3}[name]

    class _Exchange:  # ровно то, что читает Exchange.bulk_modify_orders_new; сети нет
        info = _Info()
        vault_address = None
        expires_after = None
        account_address = None
        base_url = TESTNET_API_URL

        def __init__(self):
            self.wallet = wallet

        def _post_action(self, action, *_rest):
            captured.append(action)
            return {"status": "captured"}

    variants = [
        ("batch_modify_by_cloid", [{"oid": sdk.Cloid.from_str(GOLDEN_CLOID), "order": order_a}]),
        ("batch_modify_by_oid", [{"oid": GOLDEN_OID, "order": order_a}]),
        ("batch_modify_two", [{"oid": sdk.Cloid.from_str(GOLDEN_CLOID), "order": order_a}, {"oid": GOLDEN_OID, "order": order_b}]),
    ]
    vectors = []
    for name, requests_ in variants:
        captured.clear()
        try:
            Exchange.bulk_modify_orders_new(_Exchange(), requests_)
            action, built_by = captured[-1], "sdk:Exchange.bulk_modify_orders_new"
        except Exception as error:  # noqa: BLE001
            print(f"REFUSED: the SDK path failed for {name}: {error!r}", file=sys.stderr)
            return 3
        vectors.append((name, action, built_by))
    # Одиночного modify у SDK нет (modify_order шлёт batchModify) — собран по документации API.
    vectors.append((
        "single_modify_by_oid",
        {"type": "modify", "oid": GOLDEN_OID, "order": sdk.order_request_to_order_wire(order_a, 3)},
        "hand-built per the API docs (the SDK has no single-modify builder)",
    ))

    out = {"nonce": GOLDEN_NONCE, "expires_after": GOLDEN_EXPIRES_AFTER, "key": "Hardhat #0 (public test key)",
           "sdk_version": sdk.version, "order_anchor_matches_exchange_rs": True,
           "preimage_checked_against_sdk_action_hash": sdk_action_hash is not None, "vectors": []}
    for name, action, built_by in vectors:
        entry = {
            "name": name,
            "built_by": built_by,
            "action_json": action,
            "msgpack_hex": sdk.msgpack.packb(action).hex(),
            "connection_id": connection_id(sdk, action, GOLDEN_NONCE),
            "connection_id_expires": connection_id(sdk, action, GOLDEN_NONCE, GOLDEN_EXPIRES_AFTER),
        }
        if sdk_action_hash is not None:
            for key, expires in (("connection_id", None), ("connection_id_expires", GOLDEN_EXPIRES_AFTER)):
                try:
                    theirs = bytes(sdk_action_hash(action, None, GOLDEN_NONCE, expires)).hex()
                except TypeError:  # другая сигнатура action_hash — сверки нет, подписи всё равно от SDK
                    out["preimage_checked_against_sdk_action_hash"] = False
                    break
                if theirs != entry[key]:
                    print(f"REFUSED: our preimage disagrees with the SDK's action_hash for {name} ({key})", file=sys.stderr)
                    return 3
        for net, is_mainnet in (("testnet", False), ("mainnet", True)):
            # Офлайн-векторы публичным ключом; боевые действия подписывает только sign().
            for suffix, expires in (("", None), ("_expires", GOLDEN_EXPIRES_AFTER)):
                sig = sdk.sign_l1_action(wallet, action, None, GOLDEN_NONCE, expires, is_mainnet)
                entry[f"sig_{net}{suffix}"] = {"r": sig["r"], "s": sig["s"], "v": sig["v"]}
        out["vectors"].append(entry)
    out_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    path = out_dir / "golden_batch_modify.json"
    path.write_text(json.dumps(out, indent=1, default=str))
    print(json.dumps(out, indent=1, default=str))
    print(f"saved {path}")
    return 0


# --- Self-test ------------------------------------------------------------------------------


def self_test() -> int:
    def refused(fn, *a, **kw):
        try:
            fn(*a, **kw)
        except (Refuse, Abort):
            return True
        return False

    # Только тестнет; фейк — только loopback.
    assert_testnet_url(BASE_URL)
    assert_testnet_url(WS_URL)
    for bad in (
        "https://api.hyperliquid.xyz", "wss://api.hyperliquid.xyz/ws", "https://API.HYPERLIQUID.XYZ/info",
        "http://api.hyperliquid-testnet.xyz", "https://api.hyperliquid-testnet.xyz.evil.example",
        "https://api.hyperliquid-testnet.xyz:8443", "https://user@evil.example",
        "https://user@api.hyperliquid-testnet.xyz",
    ):
        assert refused(assert_testnet_url, bad), bad
    fv = fake_venue(18545)
    assert_venue_url(fv.rest, fv)
    assert_venue_url(fv.ws, fv)
    for bad in ("http://localhost:1", "http://10.0.0.1:1", "https://api.hyperliquid.xyz", "http://u@127.0.0.1:1"):
        assert refused(assert_venue_url, bad, fv), bad
    assert refused(assert_venue_url, fv.rest, TESTNET)
    for bad_port in (0, 65536, "1", True, None):
        assert refused(fake_venue, bad_port), bad_port
    assert refused(refuse_mainnet_config, {"is_mainnet": True})
    assert refused(refuse_mainnet_config, {"rest_url": "https://api.hyperliquid.xyz"})
    assert refused(refuse_mainnet_config, {"x": {"y": ["wss://api.hyperliquid.xyz/ws"]}})
    refuse_mainnet_config({"api_wallet_private_key": "0x" + "1" * 64, "master_account_address": "0x" + "2" * 40,
                           "rest_url": "https://api.hyperliquid-testnet.xyz", "is_mainnet": False})

    # Гардрейлы ордера: ref 100 → потолок цены 95.
    check_order_guardrails(90.0, 0.13, 100.0, tif="Alo", is_buy=True)
    check_order_guardrails(95.0, 0.12, 100.0, tif="Alo", is_buy=True)
    for args, kw in (
        ((95.01, 0.12, 100.0), {}),                      # ближе 5 %
        ((90.0, 0.13, 100.0), {"tif": "Gtc"}),           # не post-only
        ((90.0, 0.13, 100.0), {"is_buy": False}),        # продажа
        ((90.0, 0.17, 100.0), {}),                       # $15.3 > $15
        ((90.0, 0.10, 100.0), {}),                       # $9 < минимума биржи
        ((float("nan"), 0.1, 100.0), {}),
        ((90.0, 0.13, float("nan")), {}),
    ):
        full = {"tif": "Alo", "is_buy": True, **kw}
        assert refused(check_order_guardrails, *args, **full), (args, kw)
    check_order_guardrails(90.0, 0.05, 100.0, tif="Alo", is_buy=True, allow_below_venue_min=True)
    assert refused(check_order_guardrails, 90.0, 0.17, 100.0, tif="Alo", is_buy=True, allow_below_venue_min=True)

    # Q13: исключение из 5 % — узкое. best ask 100 → цена [100, 100.2], notional [$10, $11].
    book = {"best_ask": 100.0}
    check_cross_guardrails(100.0, 0.11, book, tif="Alo", is_buy=True)
    check_cross_guardrails(100.2, 0.1, book, tif="Alo", is_buy=True)
    for args, kw in (
        ((99.99, 0.11, book), {}),                       # не пересекает — мерить нечего
        ((100.21, 0.1, book), {}),                       # дальше 0.2 % за тачем
        ((100.0, 0.111, book), {}),                      # $11.1 > $11 (решение лида)
        ((100.0, 0.09, book), {}),                       # $9: отказ по минимуму заслонил бы ALO
        ((100.0, 0.11, book), {"tif": "Gtc"}),           # не post-only
        ((100.0, 0.11, book), {"is_buy": False}),        # продажа
        ((100.0, 0.11, {}), {}),                         # нет best ask
        ((100.0, 0.11, {"best_ask": True}), {}),
        ((float("nan"), 0.11, book), {}),
    ):
        full = {"tif": "Alo", "is_buy": True, **kw}
        assert refused(check_cross_guardrails, *args, **full), (args, kw)

    # Округление цены — вниз/вверх, ≤ 5 значащих цифр, сетка szDecimals.
    for px, szd in ((85123.7, 5), (123456.0, 5), (1888.88, 4), (0.123456, 0), (9.87654, 2)):
        r = floor_px(px, szd)
        assert r <= px <= ceil_px(px, szd), (px, r)
        digits = Decimal(repr(r)).normalize().as_tuple().digits
        assert len(digits) <= 5, (px, r)
        assert -Decimal(repr(r)).normalize().as_tuple().exponent <= max(0, 6 - szd) or r == int(r), (px, r)
    assert floor_px(123456.0, 5) == 123450.0
    assert floor_px(85123.7, 5) == 85123.0
    assert ceil_px(85123.2, 5) == 85124.0 and ceil_px(100110.01, 5) == 100120.0
    assert ceil_px(100.0, 2) == 100.0 and ceil_px(77307.69, 5) == 77308.0
    assert ceil_sz(0.000151, 5) == 0.00016 and floor_sz(0.000159, 5) == 0.00015

    # C4 (≤ $11) и Q13 на тестнет-BTC (85.9k, econ 2026-10-05) и на ETH.
    btc = {"best_bid": 85918.0, "best_ask": 85919.0, "mid": 85918.5, "ref": 85918.0}
    eth = {"best_bid": 3999.5, "best_ask": 4000.0, "mid": 3999.75, "ref": 3999.5}
    for book_, szd in ((btc, 5), (eth, 4)):
        orders = [small_order(book_["ref"], szd, extra_lots=k) for k in (0, 1, 2)]
        assert len(set(orders)) == 3, orders  # у каждой правки C4 своя цена и объём
        for px, sz in orders:
            check_order_guardrails(px, sz, book_["ref"], tif="Alo", is_buy=True)
            assert px <= SMALL_MAX_FRACTION * book_["ref"] and px * sz <= CROSS_MAX_NOTIONAL, (px, sz)
        cpx, csz = cross_order(book_, szd)
        check_cross_guardrails(cpx, csz, book_, tif="Alo", is_buy=True)
    assert cross_order({"best_ask": 300000.0}, 5) is None  # лот $3: в $10–11 не попасть

    # Прообраз подписи: хвост 0x00 ‖ u64be — только при expiresAfter (нота §5.1, HL-нота §5.7).
    assert action_preimage(b"\x80", 1) == b"\x80" + (1).to_bytes(8, "big") + b"\x00"
    assert action_preimage(b"\x80", 1, 2) == b"\x80" + (1).to_bytes(8, "big") + b"\x00\x00" + (2).to_bytes(8, "big")

    # Бюджет — в запросах биржи, пулы раздельные; потолок прогона — 37, одобрен лидом (нота §14 Р1).
    assert sum(PLAN_COSTS.values()) == 26 and PLAN_BUDGET == 28 and PLAN_ACTIONS == 21
    assert RUN_CEILING == 37 == PLAN_BUDGET + CLEANUP_BUDGET + SWEEP_BUDGET
    assert venue_cost({"type": "batchModify", "modifies": [1, 2]}) == 2
    assert venue_cost({"type": "modify", "oid": 1, "order": {}}) == 1
    assert venue_cost({"type": "batchModify", "modifies": [1]}, stale=True) == EXPIRED_COST
    assert venue_cost({"type": "cancel", "cancels": [1, 2, 3]}) == 3
    assert refused(venue_cost, {"type": "scheduleCancel"})
    b = Budget()
    b.charge("plan", PLAN_BUDGET)
    assert refused(b.charge, "plan", 1) and refused(b.charge, "nope", 1) and refused(b.charge, "plan", 0)
    b.charge("cleanup", CLEANUP_BUDGET)  # резерв очистки план не трогает
    assert refused(b.charge, "cleanup", 1)
    b2 = Budget()
    b2.charge("plan", PLAN_BUDGET - 2)
    b2.true_up("plan", 2)  # биржа списала на 2 больше — потолок держит факт
    assert refused(b2.charge, "plan", 1)

    # cloid и реестр.
    prefix = make_run_prefix(0x6AC41BE9)
    assert prefix == "0x0dd16ac41be9"
    led = Ledger(prefix)
    c = led.mint()
    assert re.fullmatch(r"0x0dd16ac41be9[0-9a-f]{20}", c) and is_probe_cloid(c), c
    assert not is_probe_cloid("0x0dd16ac41be9" + "1" * 20) and not is_probe_cloid("0xa1f0" + "0" * 28)
    assert led.owns(c) and not led.owns("0xa1f0" + "0" * 28) and not led.owns(12345) and not led.owns(True)
    led.note(c.upper().replace("0X", "0x"), 777)
    led.note("0xa1f0" + "0" * 28, 888)  # чужой cloid — oid не наш
    assert led.owns(777) and not led.owns(888)
    led.note_answer({"type": "order", "orders": [{"c": c}]}, [{"resting": {"oid": 779}}])  # без cloid в ответе
    assert led.owns(779) and led.chains[c] == [777, 779]
    led.note_request({"type": "order", "orders": [{"c": c, "p": "90.0", "s": "0.13"}]})
    assert led.size_verdict(c, "90", "0.13") == "ok"
    c_new = led.mint()  # правка c с новым cloid: биржа может оставить старый — тоже «просили»
    led.note_request({"type": "batchModify", "modifies": [{"oid": c, "order": {"c": c_new, "p": "89", "s": "0.14"}}]})
    assert led.size_verdict(c, "89", "0.14") == "ok" and led.size_verdict(c_new, "89", "0.14") == "ok"
    led.note_request({"type": "modify", "oid": 779, "order": {"c": c, "p": "88", "s": "0.15"}})  # цель — oid цепочки c
    assert led.size_verdict(c, "88", "0.15") == "ok"
    assert led.size_verdict(c, "90.0", "0.12") == "partial"
    assert led.size_verdict(c, "91", "0.13") == "unexpected"

    # Добор/очистка: только строки с cloid формы пробника (любой прогон) на монете. oid из реестра
    # целью не делает: строка с ним без такого cloid — в отчёт (disowned), а не в отмену.
    other_run = make_run_prefix(0x6AC41BEA) + "0" * 16 + "0002"
    orders = [
        {"coin": "BTC", "oid": 1, "cloid": c},
        {"coin": "BTC", "oid": 2, "cloid": "0xa1f0" + "1" * 28},
        {"coin": "BTC", "oid": 3, "cloid": None},
        {"coin": "ETH", "oid": 4, "cloid": c},
        {"coin": "BTC", "oid": 5, "cloid": other_run},
        {"coin": "BTC", "oid": "6", "cloid": c},
        {"coin": "BTC", "oid": 7, "cloid": "0x0dd1" + "f" * 28},  # префикс есть, формы нет
        {"coin": "BTC", "oid": 8},  # без cloid, но oid наш
    ]
    assert sweep_targets(orders, "BTC") == [1, 5], sweep_targets(orders, "BTC")
    assert probe_form_oids(orders, "BTC") == {1, 5}
    assert [d["oid"] for d in disowned_rows(orders, "BTC", {1, 2, 3, 4, 7, 8})] == [2, 3, 7, 8]

    # Список открытых ордеров принимается только правильной формы: {} — не «ордеров нет».
    assert rows_problem([]) is None
    assert rows_problem([{"coin": "BTC", "oid": 1, "cloid": c}, {"coin": "BTC", "oid": 2}]) is None
    for bad in ({}, {"error": "maintenance"}, False, None, "[]", [1], [{"coin": "BTC", "oid": "1"}],
                [{"coin": "BTC", "oid": True}], [{"oid": 1}], [{"coin": "BTC", "oid": 1, "cloid": 5}]):
        assert rows_problem(bad), bad
    assert parse_rate_limit({"nRequestsUsed": 5, "nRequestsCap": 10}) == {"used": 5, "cap": 10}
    for bad in ({}, None, {"nRequestsUsed": "x", "nRequestsCap": 1}, {"nRequestsUsed": True, "nRequestsCap": 1}):
        assert refused(parse_rate_limit, bad), bad

    # Последний рубеж перед подписью.
    w = {"a": 3, "b": True, "p": "90", "s": "0.13", "r": False, "t": {"limit": {"tif": "Alo"}}, "c": c}
    assert_action_safe({"type": "order", "orders": [w], "grouping": "na"}, 3, led.owns)
    for bad in (
        {"type": "order", "orders": [{**w, "b": False}], "grouping": "na"},
        {"type": "order", "orders": [{**w, "t": {"limit": {"tif": "Gtc"}}}], "grouping": "na"},
        {"type": "order", "orders": [{**w, "a": 4}], "grouping": "na"},
        {"type": "order", "orders": [{**w, "c": "0xa1f0" + "0" * 28}], "grouping": "na"},
        {"type": "order", "orders": [{**w, "s": "0.2"}], "grouping": "na"},
        {"type": "scheduleCancel", "time": None},
        {"type": "cancelByCloid", "cancels": [{"asset": 3, "cloid": "0xa1f0" + "0" * 28}]},
        {"type": "cancel", "cancels": [{"a": 4, "o": 1}]},
    ):
        assert refused(assert_action_safe, bad, 3, led.owns), bad
    # Цели — только здесь: правка — своя (cloid прогона или его oid), cancel — только oid, который
    # список биржи показывает под cloid формы пробника (sweepable); свой oid из реестра — мало.
    for good in (
        {"type": "modify", "oid": c, "order": w},
        {"type": "modify", "oid": 777, "order": w},
        {"type": "batchModify", "modifies": [{"oid": c, "order": w}, {"oid": 779, "order": w}]},
    ):
        assert_action_safe(good, 3, led.owns)
    assert_action_safe({"type": "cancel", "cancels": [{"a": 3, "o": 999}]}, 3, led.owns, frozenset({999}))
    for bad in (
        {"type": "modify", "oid": 888, "order": w},  # oid чужого cloid
        {"type": "modify", "oid": "0xa1f0" + "0" * 28, "order": w},
        {"type": "modify", "oid": True, "order": w},
        {"type": "batchModify", "modifies": [{"oid": c, "order": w}, {"oid": 888, "order": w}]},
        {"type": "cancel", "cancels": [{"a": 3, "o": 777}]},  # свой oid, списка биржи нет
    ):
        assert refused(assert_action_safe, bad, 3, led.owns), bad
    assert refused(assert_action_safe, {"type": "cancel", "cancels": [{"a": 3, "o": 777}, {"a": 3, "o": 999}]},
                   3, led.owns, frozenset({999}))  # 777 — свой, но список его под формой пробника не показал

    # SDK — ровно 0.24.0 и обязан воспроизвести golden ордера exchange.rs (заглушка воспроизводит).
    def sdk_(version, anchor=ORDER_MSGPACK_ANCHOR):
        return SimpleNamespace(version=version, Cloid=SimpleNamespace(from_str=str),
                               order_request_to_order_wire=lambda order, asset: order,
                               order_wires_to_order_action=lambda wires: wires,
                               msgpack=SimpleNamespace(packb=lambda _a: bytes.fromhex(anchor)))

    check_sdk(sdk_(SDK_PIN))
    assert refused(check_sdk, sdk_("0.23.0")) and refused(check_sdk, sdk_("unknown"))
    assert refused(check_sdk, sdk_(SDK_PIN, "c0"))

    # Тексты аборта узкие: кап/кошелёк/маржа/«invalid nonce» — да; отказ, лишь упомянувший nonce
    # или лимит (неизмеренный S20/S21), — нет, но помечается.
    assert is_abort_text("Too many cumulative requests sent (19540 > 19470)")
    assert is_abort_text("User or API Wallet 0xabc does not exist.")
    assert is_abort_text("Insufficient margin to place order.")
    assert is_abort_text("Invalid nonce: duplicate nonce")
    assert not is_abort_text("Order was never placed, already canceled, or filled. asset=3")
    assert not is_abort_text("Post only order would have immediately matched")
    assert not is_abort_text("Action expired: nonce 1 is older than expiresAfter 2")
    assert not is_abort_text("rate limit")
    assert text_flags("Action expired: NONCE 1") == ["nonce"] and text_flags("Post only order") == []
    # Каждый сигнал, который по умолчанию завершил бы процесс и приходит снаружи, — через очистку.
    assert {"SIGINT", "SIGTERM", "SIGHUP", "SIGQUIT", "SIGUSR1", "SIGUSR2", "SIGALRM", "SIGVTALRM", "SIGPROF",
            "SIGIO", "SIGPWR", "SIGXCPU", "SIGXFSZ"} <= set(Stopper.SIGNALS), Stopper.SIGNALS

    # Формы ответа.
    assert response_shape({"status": "ok", "response": {"type": "default"}})[0] == "default"
    assert response_shape({"status": "err", "response": "nope"}) == ("err", "nope")
    shape, st = response_shape({"status": "ok", "response": {"type": "order", "data": {"statuses": [{"resting": {"oid": 1}}]}}})
    assert shape == "statuses" and status_kind(st[0]) == "resting"
    assert status_kind("success") == "success" and status_kind({"error": "x"}) == "error"
    assert response_shape("<html>")[0] == "other"
    assert position_of({"assetPositions": []}, "BTC") == "0"
    assert position_of({"assetPositions": [{"position": {"coin": "BTC", "szi": "-0.1"}}]}, "BTC") == "-0.1"
    assert position_of(None, "BTC") is None

    # Сводка: P3 и размеры замен по синтетическим шагам.
    def step_(sid, live, statuses=None, action=None, frontend=None):
        return {"step": sid, "live_after": live, "statuses": statuses, "shape": "statuses",
                "action": action or {"type": "order"}, "frontend_open": frontend or []}

    mono = [step_("S1", {"x": {"oid": 5}}), step_("S2", {"x": {"oid": 9}}), step_("S3", {"x": {"oid": 9}, "y": {"oid": 6}})]
    assert oid_monotonic(mono) == {"chains": {"x": [5, 9]}, "strictly_increasing": True}
    assert oid_monotonic(mono + [step_("S4", {"x": {"oid": 7}})])["strictly_increasing"] is False
    assert oid_monotonic(mono[:1])["strictly_increasing"] is None
    bm = {"type": "batchModify", "modifies": [{"oid": "x", "order": {"c": "x", "s": "0.0002", "p": "9"}},
                                              {"oid": "y", "order": {"c": "y", "s": "0.0003", "p": "9"}}]}
    rows = [{"cloid": "x", "origSz": "0.0002", "sz": "0.0002"}, {"cloid": "y", "origSz": "0.0005", "sz": "0.0005"}]
    sizes = replacement_sizes([step_("S6", {}, [{"resting": {"oid": 1}}, {"error": "no"}], bm, rows)])
    assert [(r["cloid"], r["equal"]) for r in sizes] == [("x", True)], sizes  # отвергнутая y не сравнивается
    assert kept_untouched({"live_before": {"x": {"oid": 1}}, "live_after": {"x": {"oid": 1}}}) is True
    assert kept_untouched({"live_before": {"x": {"oid": 1}}, "live_after": {"x": {"oid": 2}}}) is False
    assert kept_untouched({"live_before": {"x": {"oid": 1}}}) is None

    # Общий кошелёк с живым коннектором.
    with tempfile.TemporaryDirectory() as d:
        a = Path(d) / "a.toml"
        bf = Path(d) / "b.toml"
        link = Path(d) / "link.toml"
        a.write_text("x = 1\n")
        bf.write_text("x = 2\n")
        link.symlink_to(a)
        agent, master = "0x" + "a" * 40, "0x" + "b" * 40
        base = {"config": "c.toml", "coins": [], "account_address": None, "unresolved": None}
        ref, warn = shared_wallet_conflicts(a, agent, master, "BTC", [{**base, "pid": 1, "credentials": str(link)}],
                                            lambda p: (None, None))
        assert ref and not warn, (ref, warn)
        conns = [{**base, "pid": 2, "credentials": str(bf), "coins": ["BTC"], "account_address": master}]
        ref, warn = shared_wallet_conflicts(a, agent, master, "BTC", conns, lambda p: (agent.upper(), None))
        assert ref, ref
        ref, warn = shared_wallet_conflicts(a, agent, master, "BTC", conns, lambda p: ("0x" + "c" * 40, None))
        assert not ref and any("trades this account" in x for x in warn), (ref, warn)
        unres = [{**base, "pid": 3, "credentials": None, "unresolved": "its environment is unreadable"}]
        ref, warn = shared_wallet_conflicts(a, agent, master, "ETH", unres, lambda p: (None, None))
        assert not ref and any("cannot tell" in x for x in warn), warn
        same_acct = [{**base, "pid": 4, "credentials": str(bf)}]
        ref, warn = shared_wallet_conflicts(a, agent, master, "ETH", same_acct, lambda p: ("0x" + "c" * 40, master))
        assert not ref and any("trades this account" in x for x in warn), warn
        ref, warn = shared_wallet_conflicts(a, agent, master, "BTC", None, lambda p: (None, None))
        assert not ref and warn
        assert same_file(a, link) and not same_file(a, bf) and not same_file(a, Path(d) / "missing")

    # Редакция адреса.
    assert redact('{"user":"0xABCDEF"}', "0xabcdef") == '{"user":"' + HARDHAT_ADDRESS_1 + '"}'

    # Весь план S1–S21 на офлайн-бирже в процессе (без SDK и сети): доходит до конца и ничего
    # не оставляет. Сбои и сигналы — в test_hl_modify_probe_e2e.py против fake_hl_server.py.
    fake_path = Path(__file__).resolve().with_name("fake_hl_venue.py")
    spec = importlib.util.spec_from_file_location("fake_hl_venue", fake_path)
    fake = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(fake)
    me = sys.modules.get(__name__) or SimpleNamespace(**globals())
    outcome = fake.run_offline(me, "atomic", quiet=True)
    assert outcome.aborted is None, outcome.aborted
    assert [s["step"] for s in outcome.probe.steps] == [f"S{i}" for i in range(1, PLAN_ACTIONS + 1)]
    assert outcome.cleanup["left"] == 0 and not outcome.venue.run_orders_open()

    print("self-test ok")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--config", help="credentials TOML of a DEDICATED testnet API wallet")
    ap.add_argument("--coin", default="BTC")
    ap.add_argument("--state-dir", default=str(default_state_dir()), help="lock and default results (outside the repo)")
    ap.add_argument("--out", help="results directory (default: <state-dir>/results)")
    ap.add_argument("--fake-venue", type=int, metavar="PORT", help="offline fake on 127.0.0.1:PORT (fake_hl_server.py)")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--self-test", action="store_true")
    mode.add_argument("--golden", action="store_true")
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--sweep-only", action="store_true")
    args = ap.parse_args(argv)
    out_dir = Path(args.out).expanduser() if args.out else Path(args.state_dir).expanduser() / "results"
    try:
        if args.self_test:
            return self_test()
        if args.golden:
            return golden(out_dir)
        if args.fake_venue is not None and args.config:
            ap.error("--fake-venue signs with a public test key: drop --config")
        if args.fake_venue is None and not args.config:
            ap.error("--config is required (a dedicated testnet API wallet)")
        return run(args, out_dir)
    except Refuse as error:
        warn(f"REFUSED (nothing was sent): {error}")
        return 3


if __name__ == "__main__":
    sys.exit(main())
