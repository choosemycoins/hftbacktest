"""Офлайн-биржа для hl_modify_probe.py: без сети, без SDK, без ключей.

Подменяет только то, что пробник берёт снаружи: HTTP-сессию (/info, /exchange), подпись SDK и
поток аккаунта. Всё остальное — настоящий пробник: Client.exchange (потолок действий, nonce,
expiresAfter в подписи и в теле), Probe.act/observe, run_plan, cleanup, summarize.

Поведение биржи там, где оно не измерено, — допущение фейка, а не знание о HL: формы отказа
просроченного действия (Q12) и ALO-правки через спред (Q13) взяты по образцу измеренной
ALO-постановки (F10 ноты docs/design-live-modify.md). VARIANTS — исходы, которые пробник обязан
пережить (это ответ на вопрос) или на которых обязан абортить и убрать за собой.
"""
from __future__ import annotations

import contextlib
import io
import json
import time
from decimal import Decimal
from types import SimpleNamespace

COIN = "BTC"
ASSET = 3
SZ_DECIMALS = 5
BEST_BID, BEST_ASK, MID = 99990.0, 100010.0, 100000.0
MASTER = "0x" + "b" * 40
AGENT = "0x" + "c" * 40
RUN_PREFIX = "0x0dd1"
# Чужие ордера на том же счёте: пробник обязан их не тронуть.
FOREIGN = (
    {"oid": 61_000_000_001, "cloid": "0xa1f0" + "0" * 27 + "1", "px": "90000", "sz": "0.0002"},
    {"oid": 61_000_000_002, "cloid": None, "px": "89000", "sz": "0.0002"},
)

# имя: (ALO-правка через спред, действие с просроченным expiresAfter)
VARIANTS = {
    "atomic": ("refuse_keep", "refuse"),           # ожидаемое: оба отказа, ордер цел
    "cross_kills_old": ("refuse_kill", "refuse"),  # Q13(Б): отказ, старый ордер снят
    "expiry_ignored": ("refuse_keep", "execute"),  # P1 ложна: просроченное исполнено
    "cross_rests": ("rest", "refuse"),             # Q13(Г): рынок ушёл, правка встала у тача
    "cross_fills": ("fill", "refuse"),             # Q13(В): ALO на правке не соблюдена, филл
    "expiry_mentions_nonce": ("refuse_keep", "refuse_nonce"),  # отказ S20 упоминает nonce: не аборт
}

POST_ONLY = f"Post only order would have immediately matched, bbo was {BEST_BID:g}@{BEST_ASK:g}. asset={ASSET}"
GONE = f"Order was never placed, already canceled, or filled. asset={ASSET}"
MIN_VALUE = f"Order must have minimum value of $10. asset={ASSET}"
EXPIRED = "Action expired: expiresAfter is in the past"  # форма не измерена — Q12
EXPIRED_NONCE = "Action expired: nonce is older than expiresAfter"  # то же, но со словом «nonce»


def float_to_wire(x: float) -> str:
    """Как hyperliquid.utils.signing.float_to_wire."""
    rounded = f"{x:.8f}"
    if abs(float(rounded) - x) >= 1e-12:
        raise ValueError(f"float_to_wire causes rounding: {x}")
    return f"{Decimal(rounded).normalize():f}"


def _ok(kind, statuses):
    return {"status": "ok", "response": {"type": kind, "data": {"statuses": statuses}}}


class FakeVenue:
    def __init__(self, cross="refuse_keep", expired="refuse"):
        self.cross, self.expired = cross, expired
        self.next_oid = 61_940_000_000
        self.next_tid = 1
        self.open: dict[int, dict] = {}      # oid → строка frontendOpenOrders
        self.rows: dict[int, dict] = {}      # oid → строка, и после снятия
        self.status: dict[int, str] = {}     # oid → последний статус
        self.frames: list[tuple[str, object]] = []
        self.used, self.cap = 100, 10_000
        self.payloads: list[dict] = []       # тела /exchange, как отправлены
        self.signed: list[dict] = []         # что подписал «SDK»
        self.fills: list[dict] = []
        self.position = 0.0
        for f in FOREIGN:
            row = self._row(f["oid"], f["cloid"], f["px"], f["sz"])
            self.open[f["oid"]] = self.rows[f["oid"]] = row
            self.status[f["oid"]] = "open"

    @staticmethod
    def now_ms() -> int:
        return int(time.time() * 1000)

    # --- состояние ------------------------------------------------------------------------

    def _row(self, oid, cloid, px, sz):
        return {
            "coin": COIN, "side": "B", "limitPx": px, "sz": sz, "origSz": sz, "oid": oid, "cloid": cloid,
            "timestamp": self.now_ms(), "orderType": "Limit", "tif": "Alo", "reduceOnly": False,
            "isTrigger": False, "triggerPx": "0.0", "triggerCondition": "N/A", "isPositionTpsl": False,
            "children": [],
        }

    def _emit(self, row, status):
        self.status[row["oid"]] = status
        order = {k: row[k] for k in ("coin", "side", "limitPx", "sz", "oid", "timestamp", "origSz", "cloid")}
        self.frames.append(("orderUpdates", [{"order": order, "status": status, "statusTimestamp": self.now_ms()}]))

    def _rest(self, w) -> int:
        oid = self.next_oid
        self.next_oid += 1
        row = self._row(oid, w.get("c"), w["p"], w["s"])
        self.open[oid] = self.rows[oid] = row
        self._emit(row, "open")
        return oid

    def _cancel(self, oid):
        self._emit(self.open.pop(oid), "canceled")

    def _fill(self, w) -> int:
        """Тейкер: исполнение по best ask целиком, ордер не встаёт."""
        oid = self.next_oid
        self.next_oid += 1
        row = self._row(oid, w.get("c"), w["p"], w["s"])
        self.rows[oid] = row
        self._emit(row, "filled")
        fill = {
            "coin": COIN, "px": f"{BEST_ASK:g}", "sz": w["s"], "side": "B", "time": self.now_ms(),
            "startPosition": f"{self.position:g}", "dir": "Open Long", "closedPnl": "0.0",
            "hash": "0x" + "0" * 64, "oid": oid, "crossed": True, "fee": "0.0", "tid": self.next_tid,
            "cloid": w.get("c"), "feeToken": "USDC",
        }
        self.next_tid += 1
        self.position += float(w["s"])
        self.fills.append(fill)
        self.frames.append(("userFills", {"user": MASTER, "fills": [fill]}))
        return oid

    def _find_open(self, target):
        if isinstance(target, bool):
            return None
        if isinstance(target, int):
            return target if target in self.open else None
        if isinstance(target, str):
            for oid, row in self.open.items():
                if (row["cloid"] or "").lower() == target.lower():
                    return oid
        return None

    def run_orders_open(self) -> list[dict]:
        return [r for r in self.open.values() if (r["cloid"] or "").startswith(RUN_PREFIX)]

    def foreign_untouched(self) -> bool:
        return all(
            f["oid"] in self.open and self.open[f["oid"]]["limitPx"] == f["px"] and self.open[f["oid"]]["sz"] == f["sz"]
            for f in FOREIGN
        )

    # --- /exchange ------------------------------------------------------------------------

    def _place(self, w):
        px, sz = float(w["p"]), float(w["s"])
        if px * sz < 10:
            return {"error": MIN_VALUE}
        if w["b"] and px >= BEST_ASK:  # пробник ставит только ALO-покупки
            return {"error": POST_ONLY}
        status = {"oid": self._rest(w)}
        if w.get("c"):
            status["cloid"] = w["c"]
        return {"resting": status}

    def _modify(self, target, w):
        oid = self._find_open(target)
        if oid is None:
            return {"error": GONE}
        px, sz = float(w["p"]), float(w["s"])
        if px * sz < 10:
            return {"error": MIN_VALUE}  # атомарно — допущение фейка; Q10 меряет
        if w["b"] and px >= BEST_ASK:
            if self.cross == "refuse_keep":
                return {"error": POST_ONLY}
            if self.cross == "refuse_kill":
                self._cancel(oid)
                return {"error": POST_ONLY}
            if self.cross == "fill":
                self._cancel(oid)
                new = self._fill(w)
                return {"filled": {"totalSz": w["s"], "avgPx": f"{BEST_ASK:g}", "oid": new, "cloid": w.get("c")}}
            # "rest": рынок ушёл, правка встаёт как обычная
        self._cancel(oid)
        return {"resting": {"oid": self._rest(w), "cloid": w.get("c")}}

    def exchange(self, payload):
        self.payloads.append(payload)
        action = payload["action"]
        kind = action["type"]
        items = {"order": "orders", "batchModify": "modifies", "cancel": "cancels", "cancelByCloid": "cancels"}
        self.used += len(action.get(items.get(kind, ""), [])) or 1
        expires = payload.get("expiresAfter")
        if expires is not None and expires < self.now_ms() and self.expired in ("refuse", "refuse_nonce"):
            return {"status": "err", "response": EXPIRED_NONCE if self.expired == "refuse_nonce" else EXPIRED}
        if kind == "order":
            return _ok("order", [self._place(w) for w in action["orders"]])
        if kind == "batchModify":
            return _ok("order", [self._modify(m["oid"], m["order"]) for m in action["modifies"]])
        if kind == "modify":
            result = self._modify(action["oid"], action["order"])
            if "error" in result:
                return {"status": "err", "response": result["error"]}
            return {"status": "ok", "response": {"type": "default"}}
        if kind == "cancelByCloid":
            out = []
            for c in action["cancels"]:
                oid = self._find_open(c["cloid"])
                if oid is None:
                    out.append({"error": GONE})
                else:
                    self._cancel(oid)
                    out.append("success")
            return _ok("cancel", out)
        if kind == "cancel":
            out = []
            for c in action["cancels"]:
                if c["o"] in self.open:
                    self._cancel(c["o"])
                    out.append("success")
                else:
                    out.append({"error": GONE})
            return _ok("cancel", out)
        raise AssertionError(f"the probe sent an action the fake does not know: {kind}")

    # --- /info ----------------------------------------------------------------------------

    def _order_status(self, ref):
        oid = None
        if isinstance(ref, int) and ref in self.rows:
            oid = ref
        elif isinstance(ref, str):
            # F5: по cloid биржа отдаёт ПЕРВЫЙ (старый) oid цепочки.
            matching = sorted(o for o, r in self.rows.items() if (r["cloid"] or "").lower() == ref.lower())
            oid = matching[0] if matching else None
        if oid is None:
            return {"status": "unknownOid"}
        return {"status": "order", "order": {"order": dict(self.rows[oid]), "status": self.status[oid],
                                              "statusTimestamp": self.now_ms()}}

    def info(self, body):
        kind = body["type"]
        if kind == "l2Book":
            return {"coin": COIN, "time": self.now_ms(), "levels": [
                [{"px": f"{BEST_BID:g}", "sz": "1.0", "n": 1}], [{"px": f"{BEST_ASK:g}", "sz": "1.0", "n": 1}]]}
        if kind == "allMids":
            return {COIN: f"{MID:g}"}
        if kind == "userRateLimit":
            return {"cumVlm": "0.0", "nRequestsUsed": self.used, "nRequestsCap": self.cap}
        if kind == "frontendOpenOrders":
            return [dict(r) for r in self.open.values()]
        if kind == "openOrders":
            keys = ("coin", "limitPx", "oid", "side", "sz", "timestamp")
            return [{k: r[k] for k in keys} for r in self.open.values()]
        if kind == "orderStatus":
            return self._order_status(body["oid"])
        if kind == "meta":
            names = (("ETH", 4), ("SOL", 2), ("ARB", 1), (COIN, SZ_DECIMALS))
            return {"universe": [{"name": n, "szDecimals": d} for n, d in names]}
        if kind == "extraAgents":
            return [{"name": "probe", "address": AGENT, "validUntil": self.now_ms() + 86_400_000}]
        if kind == "clearinghouseState":  # префлайт и итоговая сверка позиции
            pos = [{"type": "oneWay", "position": {"coin": COIN, "szi": f"{self.position:g}"}}] if self.position else []
            return {"assetPositions": pos, "time": self.now_ms()}
        if kind == "userFills":
            return list(reversed(self.fills))
        raise AssertionError(f"the probe asked /info for something the fake does not know: {kind}")


class FakeResponse:
    def __init__(self, body, status_code=200):
        self.status_code = status_code
        self.text = json.dumps(body)
        self._body = body

    def json(self):
        return self._body


class FakeSession:
    """requests.Session для пробника: принимает только тестнет-хост; в сеть не ходит ничего.

    fail_on(payload) → None | "lost_request" | "lost_response": сбой транспорта на этом
    /exchange — до биржи или после того, как она действие применила.
    rewrite(payload, body) → body: биржа, которая отвечает не то, что сделала (врущий ответ).
    """

    def __init__(self, venue, fail_on=None, rewrite=None):
        self.venue = venue
        self.fail_on = fail_on
        self.rewrite = rewrite
        self.posts: list[tuple[str, bool]] = []

    def post(self, url, json=None, timeout=None, allow_redirects=True):  # noqa: A002 — как requests
        if not url.startswith("https://api.hyperliquid-testnet.xyz/"):
            raise AssertionError(f"the probe addressed a non-testnet URL: {url}")
        self.posts.append((url, allow_redirects))
        if url.endswith("/info"):
            return FakeResponse(self.venue.info(json))
        if url.endswith("/exchange"):
            failure = self.fail_on(json) if self.fail_on else None
            if failure == "lost_request":
                raise ConnectionError("fake transport failure before the venue")
            body = self.venue.exchange(json)
            if failure == "lost_response":
                raise ConnectionError("fake transport failure after the venue applied the action")
            if self.rewrite is not None:
                body = self.rewrite(json, body)
            return FakeResponse(body)
        raise AssertionError(url)


class FakeStream:
    """Поток аккаунта: рамки биржи с номерами, как Stream пробника."""

    def __init__(self, venue):
        self.venue = venue
        self.dead = None

    def last_seq(self) -> int:
        return len(self.venue.frames)

    def since(self, seq0: int) -> list[dict]:
        return [
            {"seq": i + 1, "t_ns": 0, "channel": ch, "text": json.dumps({"channel": ch, "data": data})}
            for i, (ch, data) in enumerate(self.venue.frames)
            if i + 1 > seq0
        ]

    def close(self):
        pass


def fake_sdk(venue, session):
    def sign_l1_action(wallet, action, active_pool, nonce, expires_after, is_mainnet):
        if is_mainnet is not False:
            raise AssertionError("the probe asked to sign for mainnet")
        venue.signed.append({"action": action, "nonce": nonce, "expires_after": expires_after})
        return {"r": "0x" + "1" * 64, "s": "0x" + "2" * 64, "v": 27}

    return SimpleNamespace(
        requests=SimpleNamespace(Session=lambda: session),
        float_to_wire=float_to_wire,
        sign_l1_action=sign_l1_action,
        version="fake",
        expires_after_ok=True,
    )


def run_offline(probe, variant="atomic", *, fail_on=None, rewrite=None, quiet=False):
    """run_plan + cleanup + summarize пробника на фейке. Паузы пробника на время прогона — no-op."""
    cross, expired = VARIANTS[variant]
    venue = FakeVenue(cross, expired)
    session = FakeSession(venue, fail_on, rewrite)
    client = probe.Client(fake_sdk(venue, session), None, MASTER, dry_run=False)
    ledger = probe.Ledger(probe.make_run_prefix(0x6AC4F00D))
    p = probe.Probe(client, FakeStream(venue), ledger, COIN, ASSET, SZ_DECIMALS, MASTER)
    real_sleep = probe.time.sleep
    probe.time.sleep = lambda _s: None
    aborted = None
    sink = io.StringIO()
    try:
        with contextlib.redirect_stdout(sink) if quiet else contextlib.nullcontext():
            try:
                probe.run_plan(p)
            except Exception as error:  # noqa: BLE001 — как run(): любой исход, затем очистка
                aborted = f"{type(error).__name__}: {error}"
            report = probe.cleanup(p)
    finally:
        probe.time.sleep = real_sleep
    return SimpleNamespace(
        probe=p, venue=venue, session=session, aborted=aborted, cleanup=report,
        summary=probe.summarize(p.steps), stdout=sink.getvalue(),
    )
