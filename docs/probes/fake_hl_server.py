#!/usr/bin/env python3
"""Офлайн-фейк HL testnet по-настоящему: HTTP + WS на loopback, подпись SDK (MON-4, нота §9).

Один loopback-порт: POST /info, POST /exchange и WS /ws (orderUpdates, userFills). Наружу не
ходит ничего; слушает только 127.0.0.1.

Воспроизводит то, на что опирается нота (§1, F1–F8) и что снято в econ/fixtures.rs: ack
`modify` = `default` без oid; ack `batchModify` = новый oid при том же cloid; старый oid после
правки — `canceled`; `orderStatus(cloid)` отдаёт старый oid (F5); `cancelByCloid` бьёт в замену;
бюджет — по ордеру в действии, отвергнутые тоже списываются; ALO через рынок — «Post only order
would have immediately matched»; минимум $10; сверх капа — одно не-отменное действие в окно.
Подпись проверяется по-настоящему (SDK): подписант — одобренный агент мастера, источник 'b'
(тестнет), nonce растёт, просроченный `expiresAfter` — отказ.

Чего биржа не показала, задаёт Profile — тесты гоняют пробник по вариантам. Сбои — хуки
`on_exchange(ctx)` / `on_info(ctx)` / `on_ws(ctx)` (в том числе 307 на чужой хост на каждом из
трёх путей) и методы `fill`, `kill_streams`.

Запуск вручную (для глаз):  python fake_hl_server.py --port 18545 [--profile atomic_refusal=false]
"""
from __future__ import annotations

import argparse
import base64
import email.utils
import hashlib
import json
import socket
import struct
import threading
import time
from dataclasses import dataclass, fields
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# Hardhat #0 — агент пробника в режиме --fake-venue; мастер — Hardhat #2. Публичные тестовые
# ключи: подпись настоящая, но деньгам негде взяться.
FAKE_AGENT = "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266"
FAKE_MASTER = "0x3c44cdddb6a900fa2b585dd299e03d12fa4293bc"

WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
OID_START = 61_934_000_000  # порядок величины тестнета (econ, 2026-10-05)

# Порядок вселенной как на тестнете: BTC — asset 3 (econ, exchange.rs).
UNIVERSE = [("SOL", 2), ("APT", 2), ("ATOM", 2), ("BTC", 5), ("ETH", 4)]
BOOKS = {"BTC": ("85918", "85919"), "ETH": ("3999.5", "4000")}

NEVER_PLACED = "Order was never placed, already canceled, or filled. asset={a}"
CANNOT_MODIFY = "Cannot modify canceled or filled order"
MIN_VALUE = "Order must have minimum value of $10. asset={a}"
POST_ONLY = "Post only order would have immediately matched, bbo was {bid}@{ask}. asset={a}"
TICK = "Price must be divisible by tick size. asset={a}"
LOT = "Order has invalid size. asset={a}"
EXPIRED = "Action expired: expiresAfter {exp} is before {now}"
OVER_CAP = (
    "Too many cumulative requests sent ({used} > {cap}) for cumulative volume traded $9470.96. "
    "Place taker orders to free up 1 request per USDC traded."
)


@dataclass(frozen=True)
class Profile:
    """Неизмеренное поведение биржи — варианты; по умолчанию то, что предполагает нота."""

    # Записи orderUpdates при правке: old=canceled старого oid, new=open замены.
    stream_order: str = "old_new_one_frame"  # | new_old_one_frame | old_new_two_frames |
    #                                            new_old_two_frames | old_only | none
    q5_keeps_oid: bool = False  # та же цена, меньше объём: oid сохраняется (Q5)
    single_modify_refusal: str = "err"  # отказ одиночного modify: top-level err | statuses
    atomic_refusal: bool = True  # отвергнутая правка оставляет старый ордер (Q10, Q13)
    new_cloid: str = "accept"  # новый cloid в правке: accept | refuse | keep_old (Q8)
    expired_cost: int = 5  # множитель бюджета за просроченный expiresAfter (доки: 5x) (Q12)
    expired_refusal: str = "err"  # err | statuses
    alo_modify_cross: str = "refuse"  # refuse | fill — сломанная биржа: правка через рынок исполнена
    order_status_by_cloid: str = "first"  # F5: старейший oid с этим cloid | latest
    over_cap: bool = False  # nRequestsUsed ≥ cap с самого начала
    throttle_s: float = 10.0  # сверх капа: одно не-отменное действие в это окно
    frame_delay_s: float = 0.0  # задержка рамок orderUpdates/userFills (поздние рамки)
    min_notional: float = 10.0  # 0 — сломанная биржа: правка ниже $10 принимается (S15 обязан абортить)
    # >0 — рамки придерживаются до N-го запроса userRateLimit после них и уходят ДО ответа на
    # него: при N = 2 это ровно щель «после окна шага, до POST следующего» (поздние рамки).
    frames_released_on_rate_limit: int = 0

    @classmethod
    def parse(cls, items: list[str]) -> "Profile":
        kinds = {f.name: f.type for f in fields(cls)}
        out = {}
        for item in items:
            key, _, raw = item.partition("=")
            if key not in kinds:
                raise SystemExit(f"unknown profile key {key!r}")
            t = kinds[key]
            out[key] = (raw.lower() in ("1", "true", "yes")) if t == "bool" else (
                int(raw) if t == "int" else float(raw) if t == "float" else raw
            )
        return cls(**out)


@dataclass
class Order:
    coin: str
    asset: int
    side: str
    limit_px: Decimal
    sz: Decimal
    orig_sz: Decimal
    oid: int
    cloid: str | None
    timestamp: int
    tif: str
    status: str = "open"
    status_ts: int = 0

    def wire(self, frontend: bool) -> dict:
        out = {
            "coin": self.coin,
            "side": self.side,
            "limitPx": _s(self.limit_px),
            "sz": _s(self.sz),
            "oid": self.oid,
            "timestamp": self.timestamp,
            "origSz": _s(self.orig_sz),
        }
        if frontend:
            out.update(
                triggerCondition="N/A", isTrigger=False, triggerPx="0.0", children=[],
                isPositionTpsl=False, reduceOnly=False, orderType="Limit", tif=self.tif,
            )
        if self.cloid is not None:
            out["cloid"] = self.cloid
        return out


def _s(x: Decimal) -> str:
    """Как биржа: '85919.0', '0.00013'."""
    text = format(x.normalize(), "f")
    return text if "." in text else text + ".0"


def _now_ms() -> int:
    return int(time.time() * 1000)


@dataclass
class Ctx:
    """Что видит хук: номер запроса этого вида, тело, фаза и сам фейк."""

    n: int
    body: dict
    fake: "FakeHL"
    phase: str = "before"  # для /exchange: before (до обработки) | after (обработано, ответа ещё нет)
    response: dict | None = None
    kind: str = ""


class FakeHL:
    def __init__(self, profile: Profile | None = None, *, port: int = 0, cap: int = 10_000, used: int = 100):
        self.profile = profile or Profile()
        self.lock = threading.RLock()
        self.agents = {FAKE_AGENT: FAKE_MASTER}
        self.books = dict(BOOKS)
        self.orders: dict[int, Order] = {}  # живые
        self.history: dict[int, Order] = {}  # все, включая мёртвые
        self.cloid_first: dict[str, int] = {}
        self.next_oid = OID_START
        self.fills: list[dict] = []
        self.position: dict[str, Decimal] = {}
        self.cap = cap
        self.used = max(used, cap + 64) if self.profile.over_cap else used
        self.last_nonce: dict[str, int] = {}
        self.last_noncancel_t = 0.0
        self.sessions: list[_WsSession] = []
        self.exchange_log: list[dict] = []
        self.info_log: list[dict] = []
        self.timeline: list[tuple[str, str, float]] = []  # (info|exchange, тип, monotonic) по приходу
        self.push_log: list[tuple[str, object]] = []  # (канал, data) — то, что реально ушло в поток
        self.held: list[list] = []  # [text, channel, user, data, сколько userRateLimit прошло]
        self.n_exchange = 0
        self.n_info = 0
        self.on_exchange = None  # Callable[[Ctx], …]: None | drop | http500 | ("redirect", url)
        # Callable[[Ctx], …]: None | drop | http500 | garbage | ("body", b"…") — 200 с этим телом |
        # ("redirect", url) — 307 на чужой хост
        self.on_info = None
        self.on_ws = None  # Callable[[Ctx], …]: None | ("redirect", url) — 307 на рукопожатие
        self.ws_log: list[str] = []  # пути рукопожатий WS, пришедших сюда
        self.probe_pid: int | None = None
        self.date_offset_s = 0.0  # сдвиг заголовка Date: часы биржи «уходят» от наших
        self.server = ThreadingHTTPServer(("127.0.0.1", port), _make_handler(self))
        self.server.daemon_threads = True
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    # Жизненный цикл ----------------------------------------------------------------------

    def start(self) -> "FakeHL":
        self.thread.start()
        return self

    def stop(self) -> None:
        self.kill_streams()
        self.server.shutdown()
        self.server.server_close()

    # Засев и инспекция (для тестов) -------------------------------------------------------

    def asset_of(self, coin: str) -> int:
        return [name for name, _ in UNIVERSE].index(coin)

    def sz_decimals(self, coin: str) -> int:
        return dict(UNIVERSE)[coin]

    def seed_order(self, coin: str, px: str, sz: str, cloid: str | None, side: str = "B") -> int:
        """Чужой или «оставшийся» ордер — кладётся мимо /exchange, без рамок."""
        with self.lock:
            o = self._new_order(coin, side, Decimal(px), Decimal(sz), cloid, "Gtc", push=False)
            return o.oid

    def open_orders(self) -> list[Order]:
        with self.lock:
            return list(self.orders.values())

    def set_book(self, coin: str, bid: str, ask: str) -> None:
        with self.lock:
            self.books[coin] = (bid, ask)

    def fill(self, oid: int, sz: str | None = None, *, silent: bool = False) -> dict:
        """Исполняет живой ордер (целиком или часть) как мейкер по его цене."""
        with self.lock:
            o = self.orders[oid]
            qty = Decimal(sz) if sz is not None else o.sz
            return self._fill(o, qty, o.limit_px, silent=silent)

    def kill_streams(self) -> None:
        for s in list(self.sessions):
            s.close()

    # Внутреннее состояние ------------------------------------------------------------------

    def _new_order(self, coin, side, px, sz, cloid, tif, *, push=True) -> Order:
        self.next_oid += 1
        now = _now_ms()
        o = Order(coin, self.asset_of(coin), side, px, sz, sz, self.next_oid, cloid, now, tif, "open", now)
        self.orders[o.oid] = o
        self.history[o.oid] = o
        if cloid is not None:
            self.cloid_first.setdefault(cloid.lower(), o.oid)
        if push:
            self._push_order_updates([o])
        return o

    def _end(self, o: Order, status: str, *, push=True) -> None:
        self.orders.pop(o.oid, None)
        o.status, o.status_ts = status, _now_ms()
        if push:
            self._push_order_updates([o])

    def _fill(self, o: Order, qty: Decimal, px: Decimal, *, silent: bool) -> dict:
        start = self.position.get(o.coin, Decimal(0))
        signed = qty if o.side == "B" else -qty
        self.position[o.coin] = start + signed
        o.sz -= qty
        fill = {
            "coin": o.coin, "px": _s(px), "sz": _s(qty), "side": o.side, "time": _now_ms(),
            "startPosition": _s(start), "dir": "Open Long" if o.side == "B" else "Open Short",
            "closedPnl": "0.0", "hash": "0x" + "0" * 64, "oid": o.oid, "crossed": False,
            "fee": "0.0", "tid": 100_000 + len(self.fills), "feeToken": "USDC",
        }
        if o.cloid is not None:
            fill["cloid"] = o.cloid
        self.fills.append(fill)
        if o.sz <= 0:
            self._end(o, "filled", push=not silent)
        if not silent:
            self._push("userFills", FAKE_MASTER, {"isSnapshot": False, "user": FAKE_MASTER, "fills": [fill]})
        return fill

    def _find_live(self, target) -> Order | None:
        if isinstance(target, bool):
            return None
        if isinstance(target, int):
            return self.orders.get(target)
        if isinstance(target, str):
            for o in self.orders.values():
                if (o.cloid or "").lower() == target.lower():
                    return o
        return None

    def _known(self, target) -> bool:
        if isinstance(target, int) and not isinstance(target, bool):
            return target in self.history
        if isinstance(target, str):
            return target.lower() in self.cloid_first
        return False

    # WS -----------------------------------------------------------------------------------

    def _push(self, channel: str, user: str, data) -> None:
        text = json.dumps({"channel": channel, "data": data})
        if self.profile.frames_released_on_rate_limit > 0:
            self.held.append([text, channel, user, data, 0])
        elif self.profile.frame_delay_s > 0:
            threading.Timer(self.profile.frame_delay_s, self._deliver, args=(text, channel, user, data)).start()
        else:
            self._deliver(text, channel, user, data)

    def _deliver(self, text: str, channel: str, user: str, data) -> None:
        with self.lock:
            sent = [s.send_text(text) for s in list(self.sessions) if s.wants(channel, user)]
            if any(sent):
                self.push_log.append((channel, data))

    def _release_held(self) -> None:
        keep = []
        for item in self.held:
            item[4] += 1
            if item[4] >= self.profile.frames_released_on_rate_limit:
                self._deliver(*item[:4])
            else:
                keep.append(item)
        self.held = keep

    def _push_order_updates(self, orders: list[Order]) -> None:
        data = [{"order": o.wire(False), "status": o.status, "statusTimestamp": o.status_ts} for o in orders]
        if data:
            self._push("orderUpdates", FAKE_MASTER, data)

    def _push_modify(self, old: Order, new: Order) -> None:
        mode = self.profile.stream_order
        if mode == "none":
            return
        if mode == "old_only":
            self._push_order_updates([old])
        elif mode == "old_new_one_frame":
            self._push_order_updates([old, new])
        elif mode == "new_old_one_frame":
            self._push_order_updates([new, old])
        elif mode == "old_new_two_frames":
            self._push_order_updates([old])
            self._push_order_updates([new])
        elif mode == "new_old_two_frames":
            self._push_order_updates([new])
            self._push_order_updates([old])
        else:
            raise ValueError(mode)

    # /info --------------------------------------------------------------------------------

    def info(self, body: dict):
        t = body.get("type")
        with self.lock:
            if t == "meta":
                return {"universe": [{"szDecimals": d, "name": n, "maxLeverage": 40} for n, d in UNIVERSE]}
            if t == "extraAgents":
                return [{"name": "probe", "address": a, "validUntil": 1_900_000_000_000}
                        for a, m in self.agents.items() if m == body.get("user", "").lower()]
            if t == "userRateLimit":
                self._release_held()
                return {"cumVlm": "9470.96", "nRequestsUsed": self.used, "nRequestsCap": self.cap,
                        "nRequestsSurplus": 0}
            if t == "l2Book":
                bid, ask = self.books[body["coin"]]
                lv = lambda px: [{"px": px, "sz": "0.5", "n": 3}]  # noqa: E731
                return {"coin": body["coin"], "time": _now_ms(), "levels": [lv(bid), lv(ask)]}
            if t == "allMids":
                return {c: _s((Decimal(b) + Decimal(a)) / 2) for c, (b, a) in self.books.items()}
            if t == "frontendOpenOrders":
                return [o.wire(True) for o in self._mine()]
            if t == "openOrders":
                return [{k: v for k, v in o.wire(False).items() if k not in ("cloid", "origSz")} for o in self._mine()]
            if t == "orderStatus":
                return self._order_status(body.get("oid"))
            if t == "userFills":
                return list(reversed(self.fills))
            if t == "clearinghouseState":
                positions = [
                    {"type": "oneWay", "position": {"coin": c, "szi": _s(q), "entryPx": "0.0"}}
                    for c, q in self.position.items() if q != 0
                ]
                return {"marginSummary": {"accountValue": "1000.0"}, "assetPositions": positions,
                        "time": _now_ms()}
        return None  # → HTTP 422, как биржа на неизвестный тип

    def _mine(self) -> list[Order]:
        return sorted(self.orders.values(), key=lambda o: o.oid, reverse=True)

    def _order_status(self, ref):
        o = None
        if isinstance(ref, int) and not isinstance(ref, bool):
            o = self.history.get(ref)
        elif isinstance(ref, str):
            key = ref.lower()
            if self.profile.order_status_by_cloid == "first":
                oid = self.cloid_first.get(key)
                o = self.history.get(oid) if oid is not None else None
            else:
                same = [x for x in self.history.values() if (x.cloid or "").lower() == key]
                o = max(same, key=lambda x: x.oid) if same else None
        if o is None:
            return {"status": "unknownOid"}
        return {"status": "order", "order": {"order": o.wire(True), "status": o.status, "statusTimestamp": o.status_ts}}

    # /exchange ----------------------------------------------------------------------------

    def exchange(self, payload: dict) -> dict:
        from hyperliquid.utils.signing import recover_agent_or_user_from_l1_action

        action = payload.get("action") or {}
        nonce = payload.get("nonce")
        expires = payload.get("expiresAfter")
        with self.lock:
            try:
                signer = recover_agent_or_user_from_l1_action(
                    action, payload["signature"], payload.get("vaultAddress"), nonce, expires, False
                ).lower()
            except Exception as error:  # noqa: BLE001
                return self._log(action, None, {"status": "err", "response": f"bad signature: {error}"})
            master = self.agents.get(signer)
            if master is None:
                return self._log(action, signer, {"status": "err", "response": f"User or API Wallet {signer} does not exist."})
            if not isinstance(nonce, int) or nonce <= self.last_nonce.get(signer, 0):
                return self._log(action, signer, {"status": "err", "response": "Invalid nonce: duplicate nonce"})
            self.last_nonce[signer] = nonce
            t = action.get("type")
            n_items = {"order": "orders", "batchModify": "modifies", "cancel": "cancels", "cancelByCloid": "cancels"}
            cost = len(action.get(n_items[t]) or []) if t in n_items else 1
            now = _now_ms()
            if expires is not None and expires < now:
                self.used += cost * self.profile.expired_cost
                text = EXPIRED.format(exp=expires, now=now)
                if self.profile.expired_refusal == "statuses":
                    return self._log(action, signer, _ok_statuses("order", [{"error": text}] * max(cost, 1)))
                return self._log(action, signer, {"status": "err", "response": text})
            self.used += cost
            is_cancel = t in ("cancel", "cancelByCloid")
            if self.used > self.cap and not is_cancel:
                if time.time() - self.last_noncancel_t < self.profile.throttle_s:
                    return self._log(action, signer, {"status": "err", "response": OVER_CAP.format(used=self.used, cap=self.cap)})
            if not is_cancel:
                self.last_noncancel_t = time.time()
            if t == "order":
                return self._log(action, signer, _ok_statuses("order", [self._place(w) for w in action["orders"]]))
            if t == "batchModify":
                sts = [self._modify(m["oid"], m["order"]) for m in action["modifies"]]
                return self._log(action, signer, _ok_statuses("order", sts))
            if t == "modify":
                st = self._modify(action["oid"], action["order"])
                if "error" not in st:
                    return self._log(action, signer, {"status": "ok", "response": {"type": "default"}})
                if self.profile.single_modify_refusal == "statuses":
                    return self._log(action, signer, _ok_statuses("order", [st]))
                return self._log(action, signer, {"status": "err", "response": st["error"]})
            if t == "cancel":
                sts = [self._cancel(self.orders.get(c["o"]), c["a"]) for c in action["cancels"]]
                return self._log(action, signer, _ok_statuses("cancel", sts))
            if t == "cancelByCloid":
                sts = [self._cancel(self._find_live(c["cloid"]), c["asset"]) for c in action["cancels"]]
                return self._log(action, signer, _ok_statuses("cancel", sts))
            return self._log(action, signer, {"status": "err", "response": f"Unknown action {t}"})

    def _log(self, action, signer, response) -> dict:
        self.exchange_log.append({"type": action.get("type"), "action": action, "signer": signer, "response": response})
        return response

    def _validate(self, w: dict) -> tuple[str | None, str, Decimal, Decimal]:
        a = w.get("a")
        coin = UNIVERSE[a][0] if isinstance(a, int) and 0 <= a < len(UNIVERSE) else None
        if coin is None:
            return "Invalid asset", "", Decimal(0), Decimal(0)
        szd = dict(UNIVERSE)[coin]
        px, sz = Decimal(w["p"]), Decimal(w["s"])
        if px <= 0 or sz <= 0:
            return LOT.format(a=a), coin, px, sz
        if px != px.to_integral_value():  # целые цены разрешены всегда
            digits = len(px.normalize().as_tuple().digits)
            decimals = -px.normalize().as_tuple().exponent
            if digits > 5 or decimals > 6 - szd:
                return TICK.format(a=a), coin, px, sz
        if sz != sz.quantize(Decimal(1).scaleb(-szd)):
            return LOT.format(a=a), coin, px, sz
        if px * sz < Decimal(str(self.profile.min_notional)):
            return MIN_VALUE.format(a=a), coin, px, sz
        return None, coin, px, sz

    def _crosses(self, coin: str, side: str, px: Decimal) -> bool:
        bid, ask = (Decimal(x) for x in self.books[coin])
        return px >= ask if side == "B" else px <= bid

    def _post_only_text(self, coin: str, a: int) -> str:
        bid, ask = self.books[coin]
        return POST_ONLY.format(bid=bid, ask=ask, a=a)

    def _place(self, w: dict) -> dict:
        err, coin, px, sz = self._validate(w)
        if err:
            return {"error": err}
        side = "B" if w["b"] else "A"
        tif = w["t"]["limit"]["tif"]
        if tif == "Alo" and self._crosses(coin, side, px):
            return {"error": self._post_only_text(coin, w["a"])}
        o = self._new_order(coin, side, px, sz, w.get("c"), tif)
        return {"resting": {"oid": o.oid, **({"cloid": o.cloid} if o.cloid else {})}}

    def _modify(self, target, w: dict) -> dict:
        old = self._find_live(target)
        if old is None:
            return {"error": CANNOT_MODIFY if self._known(target) else NEVER_PLACED.format(a=w.get("a"))}
        err, coin, px, sz = self._validate(w)
        side = "B" if w.get("b") else "A"
        tif = w["t"]["limit"]["tif"]
        crosses = not err and tif == "Alo" and self._crosses(coin, side, px)
        if crosses and self.profile.alo_modify_cross == "refuse":
            err = self._post_only_text(coin, w["a"])
        cloid = w.get("c")
        if not err and cloid is not None and (old.cloid or "").lower() != cloid.lower() and self.profile.new_cloid == "refuse":
            err = "Cannot change cloid with modify"
        if err:
            if not self.profile.atomic_refusal:
                self._end(old, "canceled")
            return {"error": err}
        if self.profile.q5_keeps_oid and px == old.limit_px and sz < old.sz:
            old.sz = old.orig_sz = sz
            old.status_ts = _now_ms()
            self._push_order_updates([old])
            return {"resting": {"oid": old.oid, **({"cloid": old.cloid} if old.cloid else {})}}
        self._end(old, "canceled", push=False)
        keep = cloid is None or self.profile.new_cloid == "keep_old"
        new = self._new_order(coin, side, px, sz, old.cloid if keep else cloid, tif, push=False)
        self._push_modify(old, new)
        if crosses:  # сломанная биржа: ALO-правка через рынок исполнилась
            ask = Decimal(self.books[coin][1])
            self._fill(new, new.sz, ask, silent=False)
            return {"filled": {"totalSz": _s(sz), "avgPx": _s(ask), "oid": new.oid, **({"cloid": new.cloid} if new.cloid else {})}}
        return {"resting": {"oid": new.oid, **({"cloid": new.cloid} if new.cloid else {})}}

    def _cancel(self, o: Order | None, a) -> object:
        if o is None:
            return {"error": NEVER_PLACED.format(a=a)}
        self._end(o, "canceled")
        return "success"


def _ok_statuses(kind: str, statuses: list) -> dict:
    return {"status": "ok", "response": {"type": kind, "data": {"statuses": statuses}}}


# --- HTTP и WS ----------------------------------------------------------------------------


def _make_handler(fake: FakeHL):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):  # тишина
            pass

        def date_time_string(self, timestamp=None):  # Date — по часам фейка со сдвигом
            return email.utils.formatdate((timestamp or time.time()) + fake.date_offset_s, usegmt=True)

        def _reply(self, code: int, body) -> None:
            raw = body if isinstance(body, bytes) else json.dumps(body).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def _drop(self) -> None:
            self.close_connection = True
            try:
                self.connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

        def _redirect(self, url: str) -> None:
            """307: клиент, идущий по редиректам, повторит тот же POST с тем же телом по url."""
            self.send_response(307)
            self.send_header("Location", url)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_POST(self):  # noqa: N802
            length = int(self.headers.get("Content-Length") or 0)
            try:
                body = json.loads(self.rfile.read(length) or b"{}")
            except ValueError:
                return self._reply(400, {"error": "bad json"})
            if self.path == "/info":
                with fake.lock:
                    fake.n_info += 1
                    ctx = Ctx(fake.n_info, body, fake, kind=str(body.get("type")))
                    fake.info_log.append({"n": ctx.n, "type": ctx.kind})
                    fake.timeline.append(("info", ctx.kind, time.monotonic()))
                verdict = fake.on_info(ctx) if fake.on_info else None
                if verdict == "drop":
                    return self._drop()
                if verdict == "http500":
                    return self._reply(500, b"internal error")
                if verdict == "garbage":
                    return self._reply(200, b"<html>gateway</html>")
                if isinstance(verdict, tuple) and verdict[0] == "body":  # 200, но тело не то
                    return self._reply(200, verdict[1])
                if isinstance(verdict, tuple) and verdict[0] == "redirect":
                    return self._redirect(verdict[1])
                out = fake.info(body)
                return self._reply(422, b"Failed to deserialize the JSON body") if out is None else self._reply(200, out)
            if self.path == "/exchange":
                with fake.lock:
                    fake.n_exchange += 1
                    ctx = Ctx(fake.n_exchange, body, fake, kind=str((body.get("action") or {}).get("type")))
                    fake.timeline.append(("exchange", ctx.kind, time.monotonic()))
                verdict = fake.on_exchange(ctx) if fake.on_exchange else None
                if verdict == "drop":  # до обработки: действие не дошло
                    return self._drop()
                if verdict == "http500":
                    return self._reply(500, b"internal error")
                if isinstance(verdict, tuple) and verdict[0] == "redirect":  # 307 сохраняет POST и тело
                    return self._redirect(verdict[1])
                out = fake.exchange(body)
                ctx.phase, ctx.response = "after", out
                verdict = fake.on_exchange(ctx) if fake.on_exchange else None
                if verdict == "drop":  # обработано, ответ потерян — неопределённость
                    return self._drop()
                return self._reply(200, out)
            return self._reply(404, {"error": "not found"})

        def do_GET(self):  # noqa: N802
            if self.path != "/ws" or "websocket" not in (self.headers.get("Upgrade") or "").lower():
                return self._reply(404, {"error": "not found"})
            with fake.lock:
                fake.ws_log.append(self.path)
            verdict = fake.on_ws(Ctx(len(fake.ws_log), {}, fake, kind="ws")) if fake.on_ws else None
            if isinstance(verdict, tuple) and verdict[0] == "redirect":
                return self._redirect(verdict[1])
            key = self.headers.get("Sec-WebSocket-Key") or ""
            accept = base64.b64encode(hashlib.sha1((key + WS_GUID).encode()).digest()).decode()
            self.send_response(101, "Switching Protocols")
            self.send_header("Upgrade", "websocket")
            self.send_header("Connection", "Upgrade")
            self.send_header("Sec-WebSocket-Accept", accept)
            self.end_headers()
            self.close_connection = True
            _WsSession(fake, self.connection, self.rfile).serve()

    return Handler


class _WsSession:
    def __init__(self, fake: FakeHL, conn, rfile):
        self.fake, self.conn, self.rfile = fake, conn, rfile
        self.subs: set[tuple[str, str]] = set()
        self.send_lock = threading.Lock()
        self.closed = False

    def wants(self, channel: str, user: str) -> bool:
        return not self.closed and (channel, user.lower()) in self.subs

    def send_text(self, text: str) -> None:
        payload = text.encode()
        n = len(payload)
        head = bytes([0x81]) + (
            bytes([n]) if n < 126 else bytes([126]) + struct.pack(">H", n) if n < 65536 else bytes([127]) + struct.pack(">Q", n)
        )
        with self.send_lock:
            if self.closed:
                return False
            try:
                self.conn.sendall(head + payload)
            except OSError:
                self.closed = True
                return False
            return True

    def close(self) -> None:
        self.closed = True
        try:
            self.conn.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

    def _read_exact(self, n: int) -> bytes:
        data = self.rfile.read(n)
        if data is None or len(data) < n:
            raise EOFError
        return data

    def serve(self) -> None:
        with self.fake.lock:
            self.fake.sessions.append(self)
        try:
            while not self.closed:
                b0, b1 = self._read_exact(2)
                opcode, n = b0 & 0x0F, b1 & 0x7F
                if n == 126:
                    n = struct.unpack(">H", self._read_exact(2))[0]
                elif n == 127:
                    n = struct.unpack(">Q", self._read_exact(8))[0]
                mask = self._read_exact(4) if b1 & 0x80 else b"\0\0\0\0"
                payload = bytes(c ^ mask[i % 4] for i, c in enumerate(self._read_exact(n)))
                if opcode == 0x8:
                    break
                if opcode == 0x9:
                    with self.send_lock:
                        self.conn.sendall(bytes([0x8A, len(payload)]) + payload)
                    continue
                if opcode == 0x1:
                    self._on_text(payload.decode())
        except (EOFError, OSError, ValueError):
            pass
        finally:
            self.closed = True
            with self.fake.lock:
                if self in self.fake.sessions:
                    self.fake.sessions.remove(self)

    def _on_text(self, text: str) -> None:
        msg = json.loads(text)
        if msg.get("method") == "ping":
            self.send_text(json.dumps({"channel": "pong"}))
            return
        if msg.get("method") != "subscribe":
            self.send_text(json.dumps({"channel": "error", "data": f"Error parsing JSON into valid websocket request: {text}"}))
            return
        sub = dict(msg.get("subscription") or {})
        kind, user = sub.get("type"), str(sub.get("user") or "").lower()
        if kind == "userFills":
            sub["aggregateByTime"] = False
        self.send_text(json.dumps({"channel": "subscriptionResponse", "data": {"method": "subscribe", "subscription": sub}}))
        with self.fake.lock:
            self.subs.add((kind, user))
            if kind == "userFills":
                snapshot = [f for f in self.fake.fills] if user == FAKE_MASTER else []
                self.send_text(json.dumps({"channel": "userFills", "data": {"isSnapshot": True, "user": user, "fills": snapshot}}))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--port", type=int, default=18545)
    ap.add_argument("--profile", action="append", default=[], metavar="KEY=VALUE")
    ap.add_argument("--leftover", action="store_true", help="seed a leftover probe order from a 'previous run'")
    args = ap.parse_args(argv)
    fake = FakeHL(Profile.parse(args.profile), port=args.port)
    if args.leftover:
        fake.seed_order("BTC", "70000", "0.0002", "0x0dd1deadbeef" + "0" * 16 + "0002")
    fake.start()
    print(f"fake HL on 127.0.0.1:{fake.port} profile={fake.profile}", flush=True)
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        fake.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
