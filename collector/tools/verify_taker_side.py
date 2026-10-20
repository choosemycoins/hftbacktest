#!/usr/bin/env python3
"""Verify the taker-side convention of a venue's trade tag against the book.

Spec: `docs/spec-mrr-gate.md` §1.4 (procedure and acceptance), §10 (per-venue
field pins). This is the §14.3 blocker: the script must cover all six venues
before the freeze; a venue it cannot verify goes `SIGN_UNVERIFIED` as a
recorded outcome, not something to fix on screening day.

For every print, take the best bid/ask whose **exchange** timestamp is strictly
less than the print's exchange timestamp and classify the aggressor
geometrically:

    price >= best_ask  -> the taker BOUGHT
    price <= best_bid  -> the taker SOLD
    strictly inside    -> ambiguous, not counted either way

Then cross-tabulate against the venue's own tag. Acceptance (§1.4):
`P(taker_bought|BUY) >= 0.98`, `P(taker_bought|SELL) <= 0.02`, over `>= 1000`
classified prints. Anything else is `SIGN_UNVERIFIED` for every cell of the
venue.

Populations follow §10: bybit `BT == true` excluded, `RPI` kept and counted;
aster `X != "MARKET"` excluded and counted; lighter `liquidation_trades` kept
and counted; paradex only `trade_type == "FILL"` enters the verdict (RPI sign
unconfirmed, §10.6) — other populations are still cross-tabulated as
diagnostics. A print whose sign field is missing or unrecognised is thrown
away and counted (§1.4).

Clocks follow §10 (the µs trap of #63): hyperliquid/bybit/aster/extended/
paradex are ms; lighter quotes use `last_updated_at` (µs) and lighter trades
use `transaction_time` (µs). bybit picks `cts` per file when present in
>= 99% of `orderbook.1` frames, else `ts`; the choice is printed.

Read-only on the data root. wf71-mrr.

usage: verify_taker_side.py <venue> <path.gz>... [--limit N] [--out FILE]
  venue in {hyperliquid, bybit, lighter, aster, extended, paradex}
"""

from __future__ import annotations

import argparse
import gzip
import json
import sys

# ---------------------------------------------------------------------------
# frozen constants (spec §1.4, §10)
# ---------------------------------------------------------------------------

MS = 1_000_000  #: ns per millisecond
US = 1_000  #: ns per microsecond

ACCEPT_P_BUY = 0.98
ACCEPT_P_SELL = 0.02
MIN_CLASSIFIED = 1000
#: §10.2: per-file rule — `cts` if present in >= 99% of orderbook.1 frames.
BYBIT_CTS_MIN_FRAC = 0.99
#: quote history kept for the strictly-earlier search (receive order).
HIST_MAX = 8192

VENUES = ("hyperliquid", "bybit", "lighter", "aster", "extended", "paradex")

#: raw tag -> canonical side, only for tags that enter the verdict (§1.4 table).
ELIGIBLE = {
    "hyperliquid": {"B": "BUY", "A": "SELL"},
    "bybit": {"Buy": "BUY", "Sell": "SELL"},
    "lighter": {"maker_ask": "BUY", "maker_bid": "SELL"},
    "aster": {"m=false": "BUY", "m=true": "SELL"},
    "extended": {"BUY": "BUY", "SELL": "SELL"},
    "paradex": {"BUY/FILL": "BUY", "SELL/FILL": "SELL"},
}


def _payload(line: str) -> str:
    return line[line.find(" ") + 1 :]


# ---------------------------------------------------------------------------
# per-venue readers -> ("quote", ts_ns, bid, ask) | ("trade", ts_ns, px, tag, uid)
#                      | ("drop", reason) | ("note", key)
# ---------------------------------------------------------------------------


def read_hyperliquid(path, limit=None):
    """§10.1: touch = `bbo` channel (`data.time` ms); trades tagged by `side`."""
    with gzip.open(path, "rt") as f:
        for i, line in enumerate(f):
            if limit is not None and i >= limit:
                return
            p = _payload(line)
            if '"channel":"bbo"' in p:
                d = json.loads(p)["data"]
                try:
                    bbo = d["bbo"]
                    yield (
                        "quote",
                        int(d["time"]) * MS,
                        float(bbo[0]["px"]),
                        float(bbo[1]["px"]),
                    )
                except (KeyError, IndexError, TypeError, ValueError):
                    yield ("drop", "quote_unparseable")
            elif '"channel":"trades"' in p:
                d = json.loads(p)["data"]
                for r in d if isinstance(d, list) else [d]:
                    side = r.get("side")
                    if side not in ("B", "A"):
                        yield ("drop", "unrecognised_tag")
                        continue
                    yield (
                        "trade",
                        int(r["time"]) * MS,
                        float(r["px"]),
                        side,
                        r.get("tid"),
                    )


def bybit_clock_field(path, limit=None):
    """§10.2 per-file rule: `cts` if in >= 99% of orderbook.1 frames, else `ts`."""
    frames = with_cts = 0
    with gzip.open(path, "rt") as f:
        for i, line in enumerate(f):
            if limit is not None and i >= limit:
                break
            p = _payload(line)
            if '"topic":"orderbook.1.' not in p:
                continue
            frames += 1
            if '"cts":' in p:
                with_cts += 1
    frac = with_cts / frames if frames else 0.0
    return ("cts" if frac >= BYBIT_CTS_MIN_FRAC else "ts"), frames, frac


def read_bybit(path, limit=None, clock="cts"):
    """§10.2: touch = last `orderbook.1` snapshot frame (no reconstruction);
    trades = `publicTrade`, `BT == true` excluded, `RPI` kept and counted."""
    with gzip.open(path, "rt") as f:
        for i, line in enumerate(f):
            if limit is not None and i >= limit:
                return
            p = _payload(line)
            if '"topic":"orderbook.1.' in p:
                j = json.loads(p)
                if j.get("type") != "snapshot":
                    # §10.2 MEASURED 100% snapshot; a delta here is counted,
                    # not silently reconstructed (fail closed).
                    yield ("note", "orderbook1_delta_frame")
                    continue
                d = j["data"]
                b, a = d.get("b") or [], d.get("a") or []
                if not b or not a:
                    yield ("drop", "one_sided_book_frame")
                    continue
                ts = j.get(clock)
                if ts is None:
                    ts = j["ts"]  # frame outside the >=99% majority
                yield ("quote", int(ts) * MS, float(b[0][0]), float(a[0][0]))
            elif '"topic":"publicTrade.' in p:
                for r in json.loads(p)["data"]:
                    if r.get("BT") is True:
                        yield ("drop", "block_trade_excluded")
                        continue
                    if r.get("RPI") is True:
                        yield ("note", "rpi_kept")
                    tag = r.get("S")
                    if tag not in ("Buy", "Sell"):
                        yield ("drop", "unrecognised_tag")
                        continue
                    yield ("trade", int(r["T"]) * MS, float(r["p"]), tag, r.get("i"))


def read_lighter(path, limit=None):
    """§10.3: touch = `ticker` (`last_updated_at` µs); trades keyed by
    `is_maker_ask`, clock `transaction_time` µs; liquidations stay in."""
    with gzip.open(path, "rt") as f:
        for i, line in enumerate(f):
            if limit is not None and i >= limit:
                return
            p = _payload(line)
            if '"channel":"ticker:' in p:
                j = json.loads(p)
                t = j["ticker"]
                ts = j.get("last_updated_at") or t.get("last_updated_at")
                try:
                    yield (
                        "quote",
                        int(ts) * US,
                        float(t["b"]["price"]),
                        float(t["a"]["price"]),
                    )
                except (KeyError, TypeError, ValueError):
                    yield ("drop", "quote_unparseable")
            elif '"channel":"trade:' in p:
                j = json.loads(p)
                for arr, note in (
                    ("trades", None),
                    ("liquidation_trades", "liquidation_kept"),
                ):
                    for r in j.get(arr) or []:
                        if note:
                            yield ("note", note)
                        ima = r.get("is_maker_ask")
                        if ima is True:
                            tag = "maker_ask"
                        elif ima is False:
                            tag = "maker_bid"
                        else:
                            yield ("drop", "unrecognised_tag")
                            continue
                        yield (
                            "trade",
                            int(r["transaction_time"]) * US,
                            float(r["price"]),
                            tag,
                            r.get("trade_id"),
                        )


def read_aster(path, limit=None):
    """§10.4: touch = `bookTicker` (`T` ms, transaction); trades = `@trade`,
    `X != "MARKET"` excluded and counted."""
    with gzip.open(path, "rt") as f:
        for i, line in enumerate(f):
            if limit is not None and i >= limit:
                return
            p = _payload(line)
            if "@bookTicker" in p:
                d = json.loads(p)["data"]
                try:
                    yield (
                        "quote",
                        int(d["T"]) * MS,
                        float(d["b"]),
                        float(d["a"]),
                    )
                except (KeyError, TypeError, ValueError):
                    yield ("drop", "quote_unparseable")
            elif "@trade" in p:
                r = json.loads(p)["data"]
                if r.get("X") != "MARKET":
                    yield ("drop", "non_market_excluded")
                    continue
                m = r.get("m")
                if m is True:
                    tag = "m=true"
                elif m is False:
                    tag = "m=false"
                else:
                    yield ("drop", "unrecognised_tag")
                    continue
                yield ("trade", int(r["T"]) * MS, float(r["p"]), tag, r.get("t"))


def read_extended(path, limit=None):
    """§10.5: book rebuilt from SNAPSHOT + DELTA (`c` absolute, 0 = removal),
    external `ts` ms; the best after each frame is emitted as the quote.
    Trades are the `"tT":"TRADE"` rows; MP frames (`ts = 0`) are not read."""
    bids: dict = {}
    asks: dict = {}
    snap_seen = False
    with gzip.open(path, "rt") as f:
        for i, line in enumerate(f):
            if limit is not None and i >= limit:
                return
            p = _payload(line)
            if '"t":"SNAPSHOT"' in p:
                j = json.loads(p)
                d = j["data"]
                bids.clear()
                asks.clear()
                for lv in d.get("b") or []:
                    bids[float(lv["p"])] = float(lv["q"])
                for lv in d.get("a") or []:
                    asks[float(lv["p"])] = float(lv["q"])
                snap_seen = True
                yield ("note", "book_snapshot")
            elif '"t":"DELTA"' in p:
                if not snap_seen:
                    continue
                j = json.loads(p)
                d = j["data"]
                for lv in d.get("b") or []:
                    px, c = float(lv["p"]), float(lv["c"])
                    if c == 0.0:
                        bids.pop(px, None)
                    else:
                        bids[px] = c
                for lv in d.get("a") or []:
                    px, c = float(lv["p"]), float(lv["c"])
                    if c == 0.0:
                        asks.pop(px, None)
                    else:
                        asks[px] = c
            elif '"tT":"TRADE"' in p:
                d = json.loads(p)["data"]
                for r in d if isinstance(d, list) else [d]:
                    if r.get("tT") != "TRADE":
                        continue
                    tag = r.get("S")
                    if tag not in ("BUY", "SELL"):
                        yield ("drop", "unrecognised_tag")
                        continue
                    yield ("trade", int(r["T"]) * MS, float(r["p"]), tag, r.get("i"))
                continue
            else:
                continue
            if snap_seen and bids and asks:
                ts = int(j.get("ts", 0))
                if ts:  # MP-style ts=0 frames carry no usable clock
                    yield ("quote", ts * MS, max(bids), min(asks))


def read_paradex(path, limit=None):
    """§10.6: touch = `bbo` channel; only `trade_type == "FILL"` enters the
    verdict — other populations (RPI) are cross-tabbed as diagnostics."""
    with gzip.open(path, "rt") as f:
        for i, line in enumerate(f):
            if limit is not None and i >= limit:
                return
            p = _payload(line)
            if '"channel":"bbo.' in p:
                d = json.loads(p)["params"]["data"]
                try:
                    yield (
                        "quote",
                        int(d["last_updated_at"]) * MS,
                        float(d["bid"]),
                        float(d["ask"]),
                    )
                except (KeyError, TypeError, ValueError):
                    yield ("drop", "quote_unparseable")
            elif '"channel":"trades.' in p:
                d = json.loads(p)["params"]["data"]
                for r in d if isinstance(d, list) else [d]:
                    side = r.get("side")
                    if side not in ("BUY", "SELL"):
                        yield ("drop", "unrecognised_tag")
                        continue
                    tag = f"{side}/{r.get('trade_type')}"
                    yield (
                        "trade",
                        int(r["created_at"]) * MS,
                        float(r["price"]),
                        tag,
                        r.get("id"),
                    )


READERS = {
    "hyperliquid": read_hyperliquid,
    "bybit": read_bybit,
    "lighter": read_lighter,
    "aster": read_aster,
    "extended": read_extended,
    "paradex": read_paradex,
}


# ---------------------------------------------------------------------------
# engine
# ---------------------------------------------------------------------------


def verify(venue, paths, limit=None):
    """Run the §1.4 procedure over `paths` (pooled) and return the report dict."""
    if venue not in VENUES:
        raise SystemExit(f"unknown venue {venue}; expected one of {VENUES}")

    tab: dict = {}
    notes: dict = {}
    drops: dict = {}
    seen: set = set()
    hist: list = []
    prints = amb = no_book = dup = 0
    clock_by_file = {}

    for path in paths:
        hist.clear()  # §1.6: the chain (and the book) breaks at the file edge
        if venue == "bybit":
            clock, frames, frac = bybit_clock_field(path, limit)
            clock_by_file[str(path)] = {
                "clock": clock,
                "orderbook1_frames": frames,
                "cts_frac": round(frac, 6),
            }
            events = read_bybit(path, limit, clock=clock)
        else:
            events = READERS[venue](path, limit)

        for ev in events:
            kind = ev[0]
            if kind == "note":
                notes[ev[1]] = notes.get(ev[1], 0) + 1
            elif kind == "drop":
                drops[ev[1]] = drops.get(ev[1], 0) + 1
            elif kind == "quote":
                _, ts, bid, ask = ev
                hist.append((ts, bid, ask))
                if len(hist) > HIST_MAX:
                    del hist[: HIST_MAX // 2]
            else:
                _, ts, px, tag, uid = ev
                if uid is not None:
                    key = (str(path), uid) if venue == "aster" else uid
                    if key in seen:
                        dup += 1
                        continue
                    seen.add(key)
                prints += 1
                bb = ba = None
                # the quote in force strictly BEFORE the print, exchange clock
                for ts_q, q_b, q_a in reversed(hist):
                    if ts_q < ts:
                        bb, ba = q_b, q_a
                        break
                if bb is None or ba is None or not (0 < bb < ba):
                    no_book += 1
                    continue
                t = tab.setdefault(tag, {"taker_bought": 0, "taker_sold": 0})
                if px >= ba:
                    t["taker_bought"] += 1
                elif px <= bb:
                    t["taker_sold"] += 1
                else:
                    amb += 1

    return _report(venue, paths, tab, notes, drops, prints, amb, no_book, dup,
                   clock_by_file)


def _report(venue, paths, tab, notes, drops, prints, amb, no_book, dup,
            clock_by_file):
    eligible = ELIGIBLE[venue]
    n = {"BUY": 0, "SELL": 0}
    bought = {"BUY": 0, "SELL": 0}
    for tag, v in tab.items():
        side = eligible.get(tag)
        if side is None:
            continue
        n[side] += v["taker_bought"] + v["taker_sold"]
        bought[side] += v["taker_bought"]
    n_classified = n["BUY"] + n["SELL"]
    p_buy = bought["BUY"] / n["BUY"] if n["BUY"] else None
    p_sell = bought["SELL"] / n["SELL"] if n["SELL"] else None

    reasons = []
    if n_classified < MIN_CLASSIFIED:
        reasons.append(
            f"n_classified {n_classified} < {MIN_CLASSIFIED} (spec 1.4 floor)"
        )
    if p_buy is None:
        reasons.append("no classified BUY-tagged prints")
    elif p_buy < ACCEPT_P_BUY:
        reasons.append(
            f"P(taker_bought|BUY) {p_buy:.4f} < {ACCEPT_P_BUY} (spec 1.4)"
        )
    if p_sell is None:
        reasons.append("no classified SELL-tagged prints")
    elif p_sell > ACCEPT_P_SELL:
        reasons.append(
            f"P(taker_bought|SELL) {p_sell:.4f} > {ACCEPT_P_SELL} (spec 1.4)"
        )
    passed = not reasons

    out = {
        "venue": venue,
        "files": [str(p) for p in paths],
        "prints_seen": prints,
        "prints_deduped": dup,
        "prints_with_no_book": no_book,
        "prints_ambiguous_inside": amb,
        "prints_classified": n_classified,
        "n_buy_tag": n["BUY"],
        "n_sell_tag": n["SELL"],
        "P_taker_bought_given_BUY": round(p_buy, 6) if p_buy is not None else None,
        "P_taker_bought_given_SELL": (
            round(p_sell, 6) if p_sell is not None else None
        ),
        "crosstab": {k: dict(v) for k, v in sorted(tab.items())},
        "populations": {**{k: v for k, v in sorted(notes.items())},
                        **{k: v for k, v in sorted(drops.items())}},
        "passed": passed,
        "verdict": "VERIFIED" if passed else "SIGN_UNVERIFIED",
        "reasons": reasons,
    }
    if clock_by_file:
        out["clock_by_file"] = clock_by_file
    return out


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("venue", choices=VENUES)
    ap.add_argument("files", nargs="+")
    ap.add_argument("--limit", type=int, default=None, help="max lines per file")
    ap.add_argument("--out", default=None, help="also write the JSON here")
    args = ap.parse_args(argv)
    res = verify(args.venue, args.files, args.limit)
    text = json.dumps(res, indent=2)
    if args.out:
        with open(args.out, "w") as f:
            f.write(text + "\n")
    print(text)
    return 0 if res["passed"] else 3


if __name__ == "__main__":
    sys.exit(main())
