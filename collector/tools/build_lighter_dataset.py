#!/usr/bin/env python3
"""Lighter market-data dataset builder — profile ``lighter-market-data-v1``.

Turns this repository's own Lighter recordings (``<symbol>_<YYYYMMDD>.gz``, the
``<local_ts_ns> <venue_json>`` line format of every collector) into the event
``.npz`` a :class:`hftbacktest` backtest reads: a kind-1 depth stream plus a
trade tape, tagged ``EXCH_EVENT | LOCAL_EVENT`` and ordered on both timelines.

Unlike ``build_hl_dataset.py`` there is no native converter to lean on — the
Hyperliquid path calls ``hftbacktest.data.utils.hyperliquid.convert`` in Rust,
and Lighter has none. So the **connector's proven parsing semantics are ported
here**, frame for frame, from ``connector/src/lighter/`` (Phase 1):

* ``msg.rs`` — a subscription is asked for as ``order_book/36`` and every frame
  comes back as ``order_book:36``; the market id is read from that **colon**
  spelling. ``begin_nonce`` lives inside ``order_book`` and is ``0`` on a
  snapshot; ``offset`` is not read. Prices and sizes are JSON strings and a size
  that fails to parse is an error, never a zero (a zero is a level deletion).
* ``depth.rs`` — the book is a full mirror. A ``subscribed/order_book`` frame is
  the whole book *diffed* against what is held; an ``update/order_book`` frame
  overlays only the levels it names, and ``"size":"0.0"`` deletes one.
  **Deletions precede inserts in every frame.** A frame that would publish a
  crossed book is refused whole; one older than the last applied is refused;
  ``last_updated_at`` is **microseconds** (the ``timestamp`` beside it is ms),
  so a µs→ns conversion is ``* 1000`` and a value that leads the local clock by
  more than a minute is refused rather than latched.
* ``trades.rs`` — both the ``trades`` and ``liquidation_trades`` arrays are
  genuine prints; the side is the **aggressor's**, so ``is_maker_ask`` (the
  *resting* side was the ask) means the taker bought. ``transaction_time`` is
  microseconds. The venue replays the last 50 prints on every (re)subscribe, so
  prints are de-duplicated by ``trade_id`` on a bounded per-market window.

Two deliberate departures from the connector, both because this is a dataset
and not a live feed:

1. **The tick is MEASURED from the recording**, not derived from the catalog's
   ``supported_price_decimals`` — the same discipline ``build_dataset.py`` uses
   for Hyperliquid (:func:`build_dataset.reconcile_tick`), and for the same
   reason: a decimals field is a claim, the recording is the evidence. The
   catalog decimals become the cross-check and the **lot** (``10^-size_decimals``).
2. **market_id and decimals come from the catalog** — the collector's
   ``_collector:"universe"`` sidecar record, the offline stand-in for
   ``GET /api/v1/orderBooks`` that ``connector/src/lighter/rest.rs`` resolves
   against. The match is exact, case included, like the connector's.

The gate is a **minimal Lighter profile** computed from the scan (there is no
Phase-2 ``quality-report-v1`` for this venue): a day with no order-book stream,
a negative feed latency (the one thing that would force a silent time shift), a
window that never quotes enough distinct prices to measure a tick from, or a
symbol the catalog does not list, is **red** and refuses the build. Gaps, chain
breaks, refused frames and a tick that disagrees with the catalog are
**yellow** — recorded and warned about, built over. It reuses the gap
thresholds ``quality_report.py`` already defines for this venue so the two
cannot drift.

Nothing here talks to the network, and nothing overwrites a recording.

Usage::

    build_lighter_dataset.py --data-dir /home/storage/hft-data/lighter \\
        --symbol CRV --out-dir dataset-crv/
"""

from __future__ import annotations

import argparse
import gzip
import json
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Callable, Iterator, Optional, Sequence

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))

from build_dataset import (  # noqa: E402
    EVENT_DTYPE,
    NS_PER_HOUR,
    BuildError,
    PriceGrid,
    Window,
    _decimal_hours,
    _step,
    _utc_str,
    _warn,
    file_fingerprint,
    iter_records,
    tick_from_exponent,
)

# ---------------------------------------------------------------------------
# constants
# ---------------------------------------------------------------------------

LIGHTER_VENUE = 'lighter'
PROFILE = 'lighter-market-data-v1'
MANIFEST_SCHEMA = 'lighter-dataset-v1'

NS_PER_US = 1_000
NS_PER_SEC = 1_000_000_000
INT64_MAX = (1 << 63) - 1

#: The event-flag bits, mirrored from ``hftbacktest/src/types.rs`` so this module
#: (and its tests) do not need the native extension. The raw stream is emitted
#: with the base kind + side bits only; :func:`default_finalize_fn` ORs in
#: ``EXCH_EVENT``/``LOCAL_EVENT`` exactly as ``data/validation.py`` does.
DEPTH_EVENT = 1
TRADE_EVENT = 2
BUY_EVENT = 1 << 29
SELL_EVENT = 1 << 28
EXCH_EVENT = 1 << 31
LOCAL_EVENT = 1 << 30

BID_DEPTH = DEPTH_EVENT | BUY_EVENT
ASK_DEPTH = DEPTH_EVENT | SELL_EVENT
BUY_TRADE = TRADE_EVENT | BUY_EVENT
SELL_TRADE = TRADE_EVENT | SELL_EVENT

#: ``depth.rs`` ``MAX_CLOCK_LEAD_NS`` — how far the venue clock may lead the
#: local one before a frame is unusable. Generous enough that ordinary skew
#: never trips it (measured frame age: 19.6 ms median, 425 ms max) and tight
#: enough that a millisecond field read as microseconds does.
MAX_CLOCK_LEAD_NS = 60_000_000_000

#: Too few distinct prices to measure a tick from: one price lands on a grid as
#: coarse as its own last non-zero digit, so any answer would be a property of
#: that number, not the instrument. Same bar as ``build_dataset.MIN_GRID_PRICES``.
MIN_GRID_PRICES = 8

#: Largest hole in a Lighter stream that is still the feed working normally, in
#: nanoseconds — the numbers ``quality_report.py`` derives and documents for
#: this venue (``MAX_GAP_NS`` for ``lighter``). Kept in sync by intent: a hole
#: past these is a yellow, never a refusal.
MAX_GAP_NS = {
    'order_book': 30 * NS_PER_SEC,
    'ticker': 20 * NS_PER_SEC,
    'trade': 120 * NS_PER_SEC,
    'market_stats': 30 * NS_PER_SEC,
}

VERDICT_ORDER = {'green': 0, 'yellow': 1, 'red': 2}


# ---------------------------------------------------------------------------
# catalog (the collector's ``universe`` sidecar record == GET /api/v1/orderBooks)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MarketInfo:
    """What the catalog says about one market — ``rest.rs`` ``MarketInfo``."""

    symbol: str
    market_id: int
    price_decimals: int
    size_decimals: int

    def catalog_tick(self) -> float:
        """The venue's declared price grid, ``10^-price_decimals``.

        The cross-check, **not** the tick registered: that one is measured
        (:func:`reconcile_tick_lighter`).
        """
        return float(Decimal(1).scaleb(-int(self.price_decimals)))

    def lot_size(self) -> float:
        """The venue's size grid, ``10^-size_decimals`` — registered as the lot."""
        return float(Decimal(1).scaleb(-int(self.size_decimals)))


def load_catalog(meta_files: Sequence[Path]) -> dict[str, MarketInfo]:
    """The latest ``_collector:"universe"`` record across ``meta_files``.

    The collector snapshots ``GET /api/v1/orderBooks`` into the sidecar as a
    ``universe`` record; market_id and the decimals do not change day to day, so
    a day whose own sidecar carries none (measured: 2026-07-30 has no universe
    record) is served by an adjacent day's. The **last** record wins, matching
    the connector, which re-resolves on every housekeeping tick.
    """
    catalog: dict[str, MarketInfo] = {}
    seen_any = False
    for path in meta_files:
        p = Path(path)
        if not p.exists():
            continue
        for _line_no, _local_ts, payload, _raw in _iter_meta(p):
            try:
                obj = json.loads(payload)
            except (json.JSONDecodeError, ValueError):
                continue
            if obj.get('_collector') != 'universe':
                continue
            seen_any = True
            for row in obj.get('markets') or []:
                symbol = row.get('symbol')
                if symbol is None:
                    continue
                catalog[symbol] = MarketInfo(
                    symbol=symbol,
                    market_id=int(row['market_id']),
                    price_decimals=int(row.get('supported_price_decimals', 0)),
                    size_decimals=int(row.get('supported_size_decimals', 0)),
                )
    if not seen_any:
        raise BuildError(
            'no _collector:"universe" record in any of the meta files %s. The '
            'catalog is how a symbol becomes a market_id and its decimals; '
            'without one there is nothing to address. Point --catalog at a meta '
            'file that has one (an adjacent day usually does).'
            % ', '.join(str(p) for p in meta_files)
        )
    return catalog


def resolve_symbol(catalog: dict[str, MarketInfo], symbol: str) -> MarketInfo:
    """Exact, case-included match — ``rest.rs`` ``resolve_one``.

    Everything downstream keys on the symbol as spelled; a case-folded match
    would address a market under a name nothing registered. So a near miss that
    differs only in case is named rather than quietly corrected.
    """
    info = catalog.get(symbol)
    if info is not None:
        return info
    fold = {s.upper(): s for s in catalog}
    hint = fold.get(symbol.upper())
    if hint is not None:
        raise BuildError(
            '%r is not how the Lighter catalog spells it: the venue\'s spelling '
            'is %r, and the match is exact.' % (symbol, hint)
        )
    raise BuildError(
        '%r is not in the Lighter catalog (it has: %s).'
        % (symbol, ', '.join(sorted(catalog)) or 'nothing')
    )


# ---------------------------------------------------------------------------
# frame parsing (ported from msg.rs)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Level:
    px: float
    qty: float


@dataclass(frozen=True)
class Book:
    market_id: int
    bids: list
    asks: list
    nonce: int
    begin_nonce: int
    last_updated_at_us: int


@dataclass(frozen=True)
class Trade:
    market_id: int
    trade_id: int
    px: float
    qty: float
    is_maker_ask: bool
    transaction_time_us: int


def market_id_of(channel: Optional[str]) -> Optional[int]:
    """The market id out of a channel in its **response** spelling, ``<name>:<id>``.

    ``None`` for a channel naming no single market and for the request spelling
    (``order_book/36``), which must never route.
    """
    if not channel or ':' not in channel:
        return None
    try:
        return int(channel.rsplit(':', 1)[1])
    except ValueError:
        return None


def _number(text) -> float:
    """One of the venue's stringly-typed numbers — ``msg.rs`` ``parse_number``.

    A field that cannot be read is an error rather than a zero: a size silently
    read as zero is a level *deletion*.
    """
    try:
        return float(text)
    except (TypeError, ValueError):
        raise BuildError('not a number: %r' % (text,))


def _levels(raw) -> list:
    return [Level(_number(lv['price']), _number(lv['size'])) for lv in raw or []]


def parse_frame(obj: dict):
    """One frame → ``('book_snapshot'|'book_update', Book)``,
    ``('trades', [Trade])``, or ``('other', label)``.

    ``obj`` is the already-decoded venue JSON (the part after the timestamp).
    Malformed structure — a book frame whose channel names no market, a price or
    size that is not a number — raises, exactly as the connector refuses it.
    """
    kind = obj.get('type')
    if not isinstance(kind, str) or '/' not in kind:
        return ('other', kind if isinstance(kind, str) else '<no type>')
    prefix, channel_kind = kind.split('/', 1)
    channel = obj.get('channel')

    if channel_kind == 'order_book' and prefix in ('subscribed', 'update'):
        market_id = market_id_of(channel)
        if market_id is None:
            raise BuildError('an order_book frame with no market id: %r' % channel)
        ob = obj.get('order_book') or {}
        book = Book(
            market_id=market_id,
            bids=_levels(ob.get('bids')),
            asks=_levels(ob.get('asks')),
            nonce=int(ob.get('nonce', 0)),
            begin_nonce=int(ob.get('begin_nonce', 0)),
            last_updated_at_us=int(ob['last_updated_at']),
        )
        return ('book_snapshot' if prefix == 'subscribed' else 'book_update', book)

    if channel_kind == 'trade' and prefix in ('subscribed', 'update'):
        trades = []
        for arr in ('trades', 'liquidation_trades'):
            for tr in obj.get(arr) or []:
                trades.append(Trade(
                    market_id=int(tr['market_id']),
                    trade_id=int(tr['trade_id']),
                    px=_number(tr['price']),
                    qty=_number(tr['size']),
                    is_maker_ask=bool(tr['is_maker_ask']),
                    transaction_time_us=int(tr['transaction_time']),
                ))
        return ('trades', trades)

    return ('other', kind)


# ---------------------------------------------------------------------------
# trade de-duplication (ported from trades.rs)
# ---------------------------------------------------------------------------


TRADE_ID_HISTORY = 96


class TradeDedup:
    """The venue replays the last 50 prints on every (re)subscribe; a bounded
    per-market window of remembered ids drops them. ``trades.rs`` ``TradeDedup``.
    """

    def __init__(self) -> None:
        self._per_market: dict[int, tuple[list, set]] = {}
        self.dropped = 0

    def accept(self, market_id: int, trade_id: int) -> bool:
        queue, seen = self._per_market.setdefault(market_id, ([], set()))
        if trade_id in seen:
            self.dropped += 1
            return False
        seen.add(trade_id)
        queue.append(trade_id)
        if len(queue) > TRADE_ID_HISTORY:
            seen.discard(queue.pop(0))
        return True


# ---------------------------------------------------------------------------
# the book mirror (ported from depth.rs DepthMirror)
# ---------------------------------------------------------------------------


REFUSAL_IMPLAUSIBLE = 'implausible'
REFUSAL_STALE = 'stale'
REFUSAL_CROSSED = 'crossed'


@dataclass
class MirrorCounts:
    stale: int = 0
    implausible: int = 0
    crossed: int = 0
    collapsed: int = 0
    sub_lot: int = 0


class LighterBook:
    """The connector's full-book mirror, emitting kind-1 depth diff events.

    Keyed by price **tick** (``round(px / tick)``) exactly as ``depth.rs`` keys
    it, so two spellings of one price cannot become two levels. The registered
    tick is the *measured* one; the lot is the catalog's. ``px`` is kept as the
    venue sent it, so every event carries the venue's own value rather than one
    reconstructed from the tick index.

    Every method returns ``(events, refusal)``: on success a list of
    ``(ev, exch_ts_ns, local_ts_ns, px, qty)`` tuples and ``None``; on a refusal
    an empty list and the reason. A refusal means the mirror no longer describes
    the venue's book — the caller counts it and, on a live feed, would repair.
    """

    def __init__(self, tick_size: float, lot_size: float) -> None:
        self.tick_size = tick_size
        self.lot_size = lot_size
        self.bids: dict[int, Level] = {}
        self.asks: dict[int, Level] = {}
        self.last_exch_ts = -(1 << 63)
        self.counts = MirrorCounts()

    # -- queries -----------------------------------------------------------
    def _tick(self, px: float) -> int:
        return int(round(px / self.tick_size))

    def _lots(self, qty: float) -> int:
        return int(round(qty / self.lot_size))

    def best_bid(self) -> Optional[float]:
        if not self.bids:
            return None
        return self.bids[max(self.bids)].px

    def best_ask(self) -> Optional[float]:
        if not self.asks:
            return None
        return self.asks[min(self.asks)].px

    def depth(self) -> tuple[int, int]:
        return (len(self.bids), len(self.asks))

    # -- application -------------------------------------------------------
    def on_snapshot(self, bids, asks, exch_ts_us, local_ts):
        return self._apply(self._side_of(bids), self._side_of(asks),
                           exch_ts_us, local_ts)

    def on_delta(self, bids, asks, exch_ts_us, local_ts):
        new_bids = dict(self.bids)
        new_asks = dict(self.asks)
        self._overlay(new_bids, bids)
        self._overlay(new_asks, asks)
        return self._apply(new_bids, new_asks, exch_ts_us, local_ts)

    def _apply(self, new_bids, new_asks, exch_ts_us, local_ts):
        # Crossed first, and BEFORE the monotonic gate latches: a refused frame
        # must not poison the timestamp the next good frame is compared to.
        if new_bids and new_asks and min(new_asks) <= max(new_bids):
            self.counts.crossed += 1
            return ([], REFUSAL_CROSSED)
        exch_ts, refusal = self._accept(exch_ts_us, local_ts)
        if refusal is not None:
            return ([], refusal)
        return (self._emit(new_bids, new_asks, exch_ts, local_ts), None)

    def _accept(self, exch_ts_us, local_ts):
        """``(exch_ts_ns, None)`` if the frame's time is usable, else ``(None, reason)``."""
        exch_ts = exch_ts_us * NS_PER_US  # µs → ns; §4.4
        if exch_ts > INT64_MAX or exch_ts < -INT64_MAX:
            self.counts.implausible += 1
            return (None, REFUSAL_IMPLAUSIBLE)
        if local_ts > 0 and exch_ts - local_ts > MAX_CLOCK_LEAD_NS:
            self.counts.implausible += 1
            return (None, REFUSAL_IMPLAUSIBLE)
        if exch_ts < self.last_exch_ts:
            self.counts.stale += 1
            return (None, REFUSAL_STALE)
        self.last_exch_ts = exch_ts
        return (exch_ts, None)

    def _side_of(self, levels) -> dict[int, Level]:
        out: dict[int, Level] = {}
        self._fold(out, levels)
        return {tick: lv for tick, lv in out.items() if self._lots(lv.qty) > 0}

    def _overlay(self, side: dict[int, Level], levels) -> None:
        changed: dict[int, Level] = {}
        self._fold(changed, levels)
        for tick, lv in changed.items():
            if self._lots(lv.qty) > 0:
                side[tick] = lv
            else:
                # Both an explicit "0.0" (a deletion) and a positive size that
                # rounds below one lot land here. Only the second says anything
                # about the venue, so only it is counted.
                if lv.qty > 0.0:
                    self.counts.sub_lot += 1
                side.pop(tick, None)

    def _fold(self, out: dict[int, Level], levels) -> None:
        for lv in levels:
            tick = self._tick(lv.px)
            if tick in out:
                prev = out[tick]
                out[tick] = Level(prev.px, prev.qty + lv.qty)
                self.counts.collapsed += 1
            else:
                out[tick] = lv

    def _emit(self, new_bids, new_asks, exch_ts, local_ts):
        events = []
        # Deletions for both sides first — see depth.rs §4.6 — then the levels
        # the new state introduced or resized (compared in lots). Walked in
        # ascending tick order, as the connector's BTreeMap is, so a frame emits
        # byte-for-byte what the proven live path does.
        for tick in sorted(self.bids):
            if tick not in new_bids:
                events.append((BID_DEPTH, exch_ts, local_ts, self.bids[tick].px, 0.0))
        for tick in sorted(self.asks):
            if tick not in new_asks:
                events.append((ASK_DEPTH, exch_ts, local_ts, self.asks[tick].px, 0.0))
        for tick in sorted(new_bids):
            lv = new_bids[tick]
            prev = self.bids.get(tick)
            if prev is None or self._lots(prev.qty) != self._lots(lv.qty):
                events.append((BID_DEPTH, exch_ts, local_ts, lv.px, lv.qty))
        for tick in sorted(new_asks):
            lv = new_asks[tick]
            prev = self.asks.get(tick)
            if prev is None or self._lots(prev.qty) != self._lots(lv.qty):
                events.append((ASK_DEPTH, exch_ts, local_ts, lv.px, lv.qty))
        self.bids = new_bids
        self.asks = new_asks
        return events


# ---------------------------------------------------------------------------
# tick reconciliation (measured wins; catalog decimals cross-check)
# ---------------------------------------------------------------------------


def reconcile_tick_lighter(grid: PriceGrid, price_decimals: int,
                           symbol: Optional[str] = None) -> dict:
    """The measured tick, cross-checked against the catalog's price decimals.

    Lighter's tick is a fixed decimal count per market (``10^-price_decimals``),
    independent of price magnitude — so, unlike Hyperliquid's five-significant-
    figure rule, there is no decade-crossing case: one measured tick describes
    the whole window. The measurement wins where the two disagree (the recording
    is the evidence, the catalog is a claim), and the disagreement is warned
    about rather than smoothed over:

    * **coarser than the catalog**: the window never used the finest legal
      increment. Every recorded price is still on the measured grid, so the
      dataset is intact; the strategy is held to the levels the day had.
    * **finer than the catalog**: the recording quotes past the precision the
      catalog advertises — the catalog record is stale or wrong for this market.
      The measurement is used (the coarser catalog tick would round an observed
      price onto a level that never existed), and it is a loud yellow.

    Refuses only when there is no honest answer: too few distinct prices to
    measure a tick from at all.
    """
    where = '' if symbol is None else ' for %s' % symbol
    catalog_exp = -int(price_decimals)
    if grid.unparsed:
        _warn('%d Lighter price(s)%s did not parse and were left out of the tick '
              'measurement' % (grid.unparsed, where))
    if grid.distinct_prices < MIN_GRID_PRICES:
        raise BuildError(
            'the window holds %d distinct Lighter price(s)%s (%d observed) — too '
            'few to measure a tick from: one price lands on a grid as coarse as '
            'its own last non-zero digit. Widen the window or pass '
            '--tick-size/--lot-size explicitly.'
            % (grid.distinct_prices, where, grid.prices)
        )
    measured_exp = grid.tick_exponent
    decades = measured_exp - catalog_exp
    if decades == 0:
        cross_check = 'agree'
    elif decades > 0:
        cross_check = 'measured_coarser_than_catalog'
        _warn('the measured tick%s is %r, %d decade(s) coarser than the catalog '
              '%r (supported_price_decimals=%d). Every recorded price lands on '
              'the measured grid, so the dataset is intact — this window never '
              'used the finest legal increment (%d distinct prices).'
              % (where, tick_from_exponent(measured_exp), decades,
                 tick_from_exponent(catalog_exp), price_decimals,
                 grid.distinct_prices))
    else:
        cross_check = 'measured_finer_than_catalog'
        _warn('the measured tick%s is %r, finer than the catalog %r '
              '(supported_price_decimals=%d). The recording quotes past the '
              'precision the catalog advertises, so the catalog record is stale '
              'or wrong for this market. Using the measurement: the coarser '
              'catalog tick would round an observed price onto a level that '
              'never existed.'
              % (where, tick_from_exponent(measured_exp),
                 tick_from_exponent(catalog_exp), price_decimals))
    return {
        'value': tick_from_exponent(measured_exp),
        'source': 'measured',
        'measured': tick_from_exponent(measured_exp),
        'exponent': measured_exp,
        'catalog': tick_from_exponent(catalog_exp),
        'catalog_exponent': catalog_exp,
        'cross_check': cross_check,
        'decades_from_catalog': decades,
        'price_decimals': int(price_decimals),
        'distinct_prices': grid.distinct_prices,
        'note': 'tick = coarsest power-of-ten grid every quoted price lands on; '
                'the catalog supported_price_decimals is the cross-check and the '
                'lot, not the tick (build_dataset.reconcile_tick discipline).',
    }


def resolve_tick_lot(grid: PriceGrid, info: MarketInfo, *,
                     cli_tick: Optional[float], cli_lot: Optional[float],
                     symbol: str) -> tuple[float, float, dict]:
    """Tick and lot for the build: measured tick + catalog lot, or a CLI override."""
    if (cli_tick is None) != (cli_lot is None):
        raise BuildError('give both --tick-size and --lot-size, or neither')
    if cli_tick is not None:
        measured = None
        try:
            measured = tick_from_exponent(grid.tick_exponent)
        except BuildError:
            pass
        return cli_tick, cli_lot, {
            'kind': 'cli', 'tick': cli_tick, 'lot': cli_lot,
            'measured_tick': measured,
            'catalog_tick': info.catalog_tick(),
            'price_decimals': info.price_decimals,
            'size_decimals': info.size_decimals,
        }
    tick = reconcile_tick_lighter(grid, info.price_decimals, symbol)
    lot = info.lot_size()
    return tick['value'], lot, {
        'kind': 'measured', 'tick': tick, 'lot': lot,
        'size_decimals': info.size_decimals,
        'catalog_tick': info.catalog_tick(),
    }


# ---------------------------------------------------------------------------
# scan (pass 1): time policy, price grid, stream health, the gate's evidence
# ---------------------------------------------------------------------------


def _iter_meta(path: Path) -> Iterator[tuple[int, int, bytes, bytes]]:
    """``iter_records`` over a plain (un-gzipped) sidecar ``.jsonl``."""
    with open(path, 'rb') as f:
        for line_no, raw in enumerate(f, start=1):
            if not raw.strip():
                continue
            head, sep, payload = raw.partition(b' ')
            if not sep:
                continue
            try:
                local_ts = int(head)
            except ValueError:
                continue
            yield (line_no, local_ts, payload.rstrip(b'\n'), raw)


@dataclass
class LighterScan:
    """What one raw day's scan found — the gate's evidence and the manifest's."""

    path: str
    day: str
    market_id: int
    first_local_ts: Optional[int] = None
    last_local_ts: Optional[int] = None
    lines: int = 0
    lines_in_window: int = 0
    order_book_frames: int = 0
    snapshots: int = 0
    updates: int = 0
    trade_frames: int = 0
    trade_prints: int = 0
    ticker_frames: int = 0
    market_stats_frames: int = 0
    other_frames: int = 0
    converted_book_frames: int = 0   # book frames with a market-id match in window
    converted_trade_prints: int = 0
    chain_breaks: int = 0
    min_local_minus_exch_ns: Optional[int] = None
    price_grid: PriceGrid = field(default_factory=PriceGrid)
    max_gap_ns: dict = field(default_factory=dict)  # stream -> largest gap seen
    _last_ts: dict = field(default_factory=dict)

    def coverage(self) -> Optional[tuple[int, int]]:
        if self.first_local_ts is None:
            return None
        return (self.first_local_ts, self.last_local_ts)

    def summary(self) -> dict:
        return {
            'path': self.path, 'day': self.day, 'market_id': self.market_id,
            'lines': self.lines, 'lines_in_window': self.lines_in_window,
            'order_book_frames': self.order_book_frames,
            'snapshots': self.snapshots, 'updates': self.updates,
            'trade_frames': self.trade_frames, 'trade_prints': self.trade_prints,
            'ticker_frames': self.ticker_frames,
            'market_stats_frames': self.market_stats_frames,
            'converted_book_frames': self.converted_book_frames,
            'converted_trade_prints': self.converted_trade_prints,
            'chain_breaks': self.chain_breaks,
            'min_local_minus_exch_ns': self.min_local_minus_exch_ns,
            'coverage': self.coverage(),
            'max_gap_ns': {k: int(v) for k, v in self.max_gap_ns.items()},
            'tick_grid': self.price_grid.summary(),
        }


def _observe_latency(scan: LighterScan, local_ts: int, exch_ts_us) -> None:
    if exch_ts_us is None:
        return
    lat = local_ts - int(exch_ts_us) * NS_PER_US
    if scan.min_local_minus_exch_ns is None or lat < scan.min_local_minus_exch_ns:
        scan.min_local_minus_exch_ns = lat


def _observe_gap(scan: LighterScan, stream: str, local_ts: int) -> None:
    prev = scan._last_ts.get(stream)
    if prev is not None:
        gap = local_ts - prev
        if gap > scan.max_gap_ns.get(stream, 0):
            scan.max_gap_ns[stream] = gap
    scan._last_ts[stream] = local_ts


def scan_lighter_file(path: Path, market_id: int, window: Optional[Window],
                      symbol: str) -> LighterScan:
    """Pass 1 over one raw day: time policy, price grid, stream health, chain."""
    day = _day_of(path)
    scan = LighterScan(path=str(path), day=day, market_id=market_id)
    prev_nonce: Optional[int] = None
    for rec in iter_records(path):
        scan.lines += 1
        if scan.first_local_ts is None:
            scan.first_local_ts = rec.local_ts
        scan.last_local_ts = rec.local_ts
        in_window = window is None or window.contains(rec.local_ts)
        if in_window:
            scan.lines_in_window += 1
        try:
            obj = json.loads(rec.payload)
        except (json.JSONDecodeError, ValueError):
            continue
        kind = obj.get('type')
        if not isinstance(kind, str) or '/' not in kind:
            scan.other_frames += 1
            continue
        prefix, channel_kind = kind.split('/', 1)
        frame_mid = market_id_of(obj.get('channel'))

        if channel_kind == 'order_book' and prefix in ('subscribed', 'update'):
            scan.order_book_frames += 1
            _observe_gap(scan, 'order_book', rec.local_ts)
            ob = obj.get('order_book') or {}
            _observe_latency(scan, rec.local_ts, ob.get('last_updated_at'))
            is_snapshot = prefix == 'subscribed'
            nonce = ob.get('nonce')
            begin = ob.get('begin_nonce', 0)
            if is_snapshot:
                scan.snapshots += 1
            else:
                scan.updates += 1
                if prev_nonce is not None and begin != prev_nonce:
                    scan.chain_breaks += 1
            if nonce is not None:
                prev_nonce = nonce
            if frame_mid == market_id:
                if in_window:
                    scan.converted_book_frames += 1
                    for side in ('bids', 'asks'):
                        for lv in ob.get(side) or []:
                            px = lv.get('price')
                            if px is not None:
                                scan.price_grid.observe(px)
        elif channel_kind == 'trade' and prefix in ('subscribed', 'update'):
            scan.trade_frames += 1
            _observe_gap(scan, 'trade', rec.local_ts)
            prints = 0
            for arr in ('trades', 'liquidation_trades'):
                for tr in obj.get(arr) or []:
                    prints += 1
                    _observe_latency(scan, rec.local_ts, tr.get('transaction_time'))
            scan.trade_prints += prints
            if frame_mid == market_id and in_window:
                scan.converted_trade_prints += prints
        elif channel_kind == 'ticker':
            scan.ticker_frames += 1
            _observe_gap(scan, 'ticker', rec.local_ts)
        elif channel_kind == 'market_stats':
            scan.market_stats_frames += 1
            _observe_gap(scan, 'market_stats', rec.local_ts)
        else:
            scan.other_frames += 1
    return scan


# ---------------------------------------------------------------------------
# the gate (a minimal Lighter profile)
# ---------------------------------------------------------------------------


def lighter_verdict(scan: LighterScan) -> tuple[str, list, list]:
    """``(verdict, red_reasons, yellow_reasons)`` for one day.

    Red — the dataset would be structurally unusable, so the build refuses:

    * no order-book frames in the window (depth comes from nowhere else);
    * a negative feed latency (``min(local_ts - exch_ts) < 0``) — the one thing
      that would force a silent time shift, which the whole time policy forbids;
    * no trade prints in the window (a passive-grid backtest fills resting
      orders off the tape; with none, every fill is fantasy and the run is a
      flat line dressed up as a result).

    Yellow — recorded and warned about, built over: stream gaps past the
    ``quality_report.py`` thresholds, chain breaks, and (folded in later) the
    tick disagreeing with the catalog and any frames the conversion refused.
    """
    red: list = []
    yellow: list = []
    if scan.converted_book_frames == 0:
        red.append('no order_book frames for market_id %d inside the window; '
                   'depth comes only from order_book, so there is no book to '
                   'build.' % scan.market_id)
    if scan.min_local_minus_exch_ns is not None and scan.min_local_minus_exch_ns < 0:
        red.append('feed latency min(local_ts - exch_ts) = %d ns < 0. A negative '
                   'latency would force a silent whole-file time shift, which the '
                   'time policy forbids — this is data to investigate, not to '
                   'correct.' % scan.min_local_minus_exch_ns)
    if scan.converted_trade_prints == 0:
        red.append('no trade prints for market_id %d inside the window. A passive '
                   'grid is filled off the tape; with no trades the backtest '
                   'produces no fills and any PnL is an artefact.'
                   % scan.market_id)
    for stream, limit in MAX_GAP_NS.items():
        gap = scan.max_gap_ns.get(stream)
        if gap is not None and gap > limit:
            yellow.append('%s has a %.3fs gap (limit %.0fs) — a reconnect-sized '
                          'hole in the feed.' % (stream, gap / NS_PER_SEC,
                                                  limit / NS_PER_SEC))
    if scan.chain_breaks:
        yellow.append('%d begin_nonce chain break(s): order_book updates were '
                      'dropped between frames, so the book is missing whatever '
                      'changed across each break until the next snapshot reseeds '
                      'it.' % scan.chain_breaks)
    verdict = 'red' if red else ('yellow' if yellow else 'green')
    return verdict, red, yellow


# ---------------------------------------------------------------------------
# convert (pass 2): drive the mirror + dedup, tag + order, save
# ---------------------------------------------------------------------------


def default_finalize_fn(data: np.ndarray) -> np.ndarray:
    """Tag events ``EXCH_EVENT``/``LOCAL_EVENT`` and order both timelines.

    Reuses ``hftbacktest.data.validation.correct_event_order`` — the same
    finalization the Hyperliquid dataset path runs — so a Lighter dataset is
    byte-compatible with the reader and the two cannot diverge about what a
    valid event array is. ``base_latency`` is 0 and no ``correct_local_timestamp``
    shift is applied: the scan already refused any day with a negative latency.
    """
    try:
        from hftbacktest.data.validation import (
            correct_event_order,
            validate_event_order,
        )
    except Exception as e:  # pragma: no cover - depends on the native build
        raise BuildError(
            'cannot import hftbacktest.data.validation (%s). Build the package '
            'first: maturin develop --release -m py-hftbacktest/Cargo.toml' % e
        )
    if len(data) == 0:
        return data
    sorted_exch = np.argsort(data['exch_ts'], kind='stable')
    sorted_local = np.argsort(data['local_ts'], kind='stable')
    final = correct_event_order(data, sorted_exch, sorted_local)
    validate_event_order(final)
    return final


def convert_lighter_day(src: Path, out_npz: Path, *, day: str, window: Window,
                        scan: LighterScan, market_id: int, tick_size: float,
                        lot_size: float, clock_correction_ns: int,
                        finalize_fn: Callable, book: 'LighterBook',
                        dedup: 'TradeDedup') -> Optional[dict]:
    """Drive the mirror + dedup over one day inside the window, tag/order, save.

    ``book`` and ``dedup`` are **carried in** from the previous day and mutated
    here, because the backtest concatenates the window's day files into one
    continuous depth (``L2AssetBuilder.data(vec![...])`` — the book is not reset
    between files, ``myhft`` ``grid_backtest_protected.rs``). A fresh book per
    day would compute each day's diffs against an empty book, so a level that
    existed at the previous day's close and is *deleted* early the next day —
    with no prior resize to re-introduce it — would never get its deletion
    emitted, and would rest stale in the concatenated book. Carrying the mirror
    diffs day N against day N-1's real close. The de-dup carries for the same
    reason: a reconnect replay that straddles midnight must still be dropped.
    """
    ev_list: list = []
    exch_list: list = []
    local_list: list = []
    px_list: list = []
    qty_list: list = []
    refused = {REFUSAL_CROSSED: 0, REFUSAL_STALE: 0, REFUSAL_IMPLAUSIBLE: 0}
    counts_before = vars(book.counts).copy()
    dropped_before = dedup.dropped
    book_frames = 0
    trade_prints = 0

    def push(events):
        for ev, exch_ts, local_ts, px, qty in events:
            ev_list.append(ev)
            exch_list.append(exch_ts)
            local_list.append(local_ts)
            px_list.append(px)
            qty_list.append(qty)

    for rec in iter_records(src):
        if not window.contains(rec.local_ts):
            continue
        try:
            obj = json.loads(rec.payload)
        except (json.JSONDecodeError, ValueError):
            continue
        tag, payload = parse_frame(obj)
        if tag in ('book_snapshot', 'book_update'):
            if payload.market_id != market_id:
                continue
            book_frames += 1
            if tag == 'book_snapshot':
                events, refusal = book.on_snapshot(
                    payload.bids, payload.asks, payload.last_updated_at_us,
                    rec.local_ts)
            else:
                events, refusal = book.on_delta(
                    payload.bids, payload.asks, payload.last_updated_at_us,
                    rec.local_ts)
            if refusal is not None:
                refused[refusal] += 1
            else:
                push(events)
        elif tag == 'trades':
            for tr in payload:
                if tr.market_id != market_id:
                    continue
                if not dedup.accept(tr.market_id, tr.trade_id):
                    continue
                exch_ts = tr.transaction_time_us * NS_PER_US
                if exch_ts > INT64_MAX:
                    continue
                trade_prints += 1
                ev = BUY_TRADE if tr.is_maker_ask else SELL_TRADE
                ev_list.append(ev)
                exch_list.append(exch_ts)
                local_list.append(rec.local_ts)
                px_list.append(tr.px)
                qty_list.append(tr.qty)

    if not ev_list:
        _warn('%s produced no events inside the window; skipping' % src)
        return None

    data = np.zeros(len(ev_list), dtype=EVENT_DTYPE)
    data['ev'] = np.array(ev_list, dtype=np.uint64)
    data['exch_ts'] = np.array(exch_list, dtype=np.int64)
    data['local_ts'] = np.array(local_list, dtype=np.int64)
    data['px'] = np.array(px_list, dtype=np.float64)
    data['qty'] = np.array(qty_list, dtype=np.float64)

    if clock_correction_ns:
        data['local_ts'] += np.int64(clock_correction_ns)

    raw_min = int((data['local_ts'] - data['exch_ts']).min())
    if raw_min < 0:
        # The scan gates this red before conversion; a negative here means the
        # window's trade tape carried a latency the book scan did not sample.
        raise BuildError(
            '%s: min(local_ts - exch_ts) = %d ns < 0 after conversion. Refusing '
            'to shift the file — this is the time policy the scan enforces, and a '
            'trade timestamp reached it that the order-book scan did not.'
            % (src, raw_min))

    final = finalize_fn(data)

    Path(out_npz).parent.mkdir(parents=True, exist_ok=True)
    # The Rust reader hard-codes the array key `data` (reader.rs).
    np.savez_compressed(out_npz, data=final)
    return {
        'day': day,
        'path': str(out_npz),
        'rows': int(len(final)),
        'raw_events': len(ev_list),
        'book_frames': book_frames,
        'trade_prints': trade_prints,
        'trades_deduplicated': int(dedup.dropped - dropped_before),
        'refused': dict(refused),
        # Per-day deltas of the carried mirror's cumulative counters.
        'mirror_counts': {k: vars(book.counts)[k] - counts_before[k]
                          for k in counts_before},
        'final_depth': list(book.depth()),
        'source': str(src),
        'lines_in_window': scan.lines_in_window,
        'min_local_minus_exch_ns': raw_min,
    }


# ---------------------------------------------------------------------------
# build
# ---------------------------------------------------------------------------


def _day_of(path: Path) -> str:
    """``ondo_20260729.gz`` → ``20260729``."""
    stem = Path(path).name
    for part in stem.replace('.gz', '').split('_'):
        if len(part) == 8 and part.isdigit():
            return part
    raise BuildError('cannot read a YYYYMMDD day out of %r' % stem)


def find_symbol_files(data_dir: Path, symbol: str,
                      days: Optional[Sequence[str]]) -> list[tuple[str, Path]]:
    """``<symbol-lowercase>_<YYYYMMDD>.gz`` in ``data_dir``, sorted by day."""
    out = []
    for path in sorted(Path(data_dir).glob('%s_*.gz' % symbol.lower())):
        try:
            day = _day_of(path)
        except BuildError:
            continue
        if days is None or day in days:
            out.append((day, path))
    return out


def find_meta_files(data_dir: Path) -> list[Path]:
    return sorted(Path(data_dir).glob('_meta_lighter_*.jsonl'))


def build(args: argparse.Namespace, finalize_fn: Optional[Callable] = None) -> dict:
    """Assemble the dataset and return the manifest it wrote."""
    finalize_fn = finalize_fn or default_finalize_fn
    data_dir = Path(args.data_dir).resolve()
    if not data_dir.is_dir():
        raise BuildError('--data-dir %s is not a directory' % data_dir)

    # --- catalog: market_id + decimals --------------------------------------
    if args.catalog:
        meta_files = [Path(args.catalog).resolve()]
    else:
        meta_files = find_meta_files(data_dir)
        if not meta_files:
            raise BuildError(
                'no _meta_lighter_*.jsonl sidecar in %s to read the catalog from. '
                'Pass --catalog explicitly.' % data_dir)
    catalog = load_catalog(meta_files)
    info = resolve_symbol(catalog, args.symbol)
    _step('resolved %s -> market_id=%d price_decimals=%d size_decimals=%d '
          '(catalog tick %g, lot %g)'
          % (info.symbol, info.market_id, info.price_decimals,
             info.size_decimals, info.catalog_tick(), info.lot_size()))

    # --- the days --------------------------------------------------------------
    days = None
    if args.days:
        days = [d.strip() for d in args.days.split(',') if d.strip()]
    files = find_symbol_files(data_dir, args.symbol, days)
    if not files:
        raise BuildError(
            'no %s_<day>.gz recordings in %s%s'
            % (args.symbol.lower(), data_dir,
               '' if days is None else ' for days %s' % ', '.join(days)))

    # --- scan every day (pass 1), gate, then convert -------------------------
    _step('scanning %d raw day file(s)' % len(files))
    scans = [scan_lighter_file(p, info.market_id, None, args.symbol)
             for _day, p in files]
    coverages = [s.coverage() for s in scans if s.coverage() is not None]
    if not coverages:
        raise BuildError('none of the %s recordings carried a timestamped line'
                         % args.symbol)
    window = Window(min(c[0] for c in coverages), max(c[1] for c in coverages))
    min_window_ns = int(args.min_window_hours * NS_PER_HOUR)
    if window.duration_ns < min_window_ns:
        raise BuildError(
            'window [%d, %d] is %.3f h, shorter than the required %.3f h'
            % (window.start_ns, window.end_ns, window.duration_ns / NS_PER_HOUR,
               min_window_ns / NS_PER_HOUR))
    days_list = [d for d, _ in files]
    _step('window %s .. %s (%.3f h), days: %s'
          % (_utc_str(window.start_ns), _utc_str(window.end_ns),
             window.duration_ns / NS_PER_HOUR, ', '.join(days_list)))

    # Gate: any red day refuses the whole build (like build_hl_dataset).
    day_verdicts = {}
    worst = 'green'
    for (day, _src), scan in zip(files, scans):
        verdict, red, yellow = lighter_verdict(scan)
        day_verdicts[day] = {'verdict': verdict, 'red': red, 'yellow': yellow,
                             'scan': scan.summary()}
        if VERDICT_ORDER[verdict] > VERDICT_ORDER[worst]:
            worst = verdict
        for line in yellow:
            _warn('%s %s: %s' % (args.symbol, day, line))
    reds = {d: v['red'] for d, v in day_verdicts.items() if v['verdict'] == 'red'}
    if reds:
        raise BuildError(
            'the Lighter quality gate marks %d day(s) red — refusing to build:\n  %s'
            % (len(reds), '\n  '.join('%s: %s' % (d, '; '.join(r))
                                      for d, r in reds.items())))

    # --- tick / lot: measured, cross-checked, overridable --------------------
    combined = PriceGrid()
    for s in scans:
        combined.merge(s.price_grid)
    tick_size, lot_size, tick_lot = resolve_tick_lot(
        combined, info, cli_tick=args.tick_size, cli_lot=args.lot_size,
        symbol=args.symbol)
    _step('tick_size=%r lot_size=%r (%s)' % (tick_size, lot_size, tick_lot['kind']))

    # --- conversion ----------------------------------------------------------
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    scan_by_path = {s.path: s for s in scans}
    outputs = []
    skipped = []
    # One mirror and one de-dup carried across every day of the window: the
    # backtest concatenates the day files into one continuous book, so day N is
    # diffed against day N-1's close (see convert_lighter_day).
    book = LighterBook(tick_size, lot_size)
    dedup = TradeDedup()
    for day, src in files:
        out_npz = out_dir / ('lighter_%s_%s.npz' % (args.symbol.lower(), day))
        result = convert_lighter_day(
            src, out_npz, day=day, window=window, scan=scan_by_path[str(src)],
            market_id=info.market_id, tick_size=tick_size, lot_size=lot_size,
            clock_correction_ns=args.clock_correction_ns, finalize_fn=finalize_fn,
            book=book, dedup=dedup)
        if result is not None:
            outputs.append(result)
        else:
            skipped.append({'day': day, 'source': str(src),
                            'reason': 'no events inside the window'})
    if not outputs:
        raise BuildError('no Lighter day produced any events inside the window')

    # Fold conversion-derived anomalies into the day verdicts as yellows.
    for out in outputs:
        anomalies = []
        for reason, n in out['refused'].items():
            if n:
                anomalies.append('%d %s-refused frame(s)' % (n, reason))
        for name in ('sub_lot', 'collapsed'):
            if out['mirror_counts'].get(name):
                anomalies.append('%d %s' % (out['mirror_counts'][name], name))
        if anomalies:
            dv = day_verdicts[out['day']]
            dv['yellow'] = list(dv.get('yellow', [])) + anomalies
            if dv['verdict'] == 'green':
                dv['verdict'] = 'yellow'
                if worst == 'green':
                    worst = 'yellow'
            for a in anomalies:
                _warn('%s %s (conversion): %s' % (args.symbol, out['day'], a))
    if tick_lot['kind'] == 'measured' \
            and tick_lot['tick']['cross_check'] != 'agree':
        if worst == 'green':
            worst = 'yellow'

    names = [Path(o['path']).name for o in outputs]
    corrected = Window(window.start_ns + int(args.clock_correction_ns),
                       window.end_ns + int(args.clock_correction_ns))
    manifest = {
        'schema': MANIFEST_SCHEMA,
        'profile': PROFILE,
        'built_by': 'hftbacktest collector/tools/build_lighter_dataset.py',
        'built_at_utc': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'rebuild_cmd': _rebuild_cmd(args, tick_size, lot_size),
        'instruments': {
            'execution': {'venue': LIGHTER_VENUE, 'symbol': args.symbol,
                          'market_id': info.market_id, 'role': 'BacktestAsset'},
        },
        'tick_size': tick_size,
        'lot_size': lot_size,
        'tick_lot': tick_lot,
        'catalog': {'symbol': info.symbol, 'market_id': info.market_id,
                    'price_decimals': info.price_decimals,
                    'size_decimals': info.size_decimals,
                    'catalog_tick': info.catalog_tick(),
                    'catalog_lot': info.lot_size(),
                    'source': [str(p) for p in meta_files]},
        'clock_correction_ns': int(args.clock_correction_ns),
        'min_window_hours': float(args.min_window_hours),
        'window': {
            'days': days_list,
            'start_ns': corrected.start_ns, 'end_ns': corrected.end_ns,
            'duration_ns': corrected.duration_ns,
            'start_utc': _utc_str(corrected.start_ns),
            'end_utc': _utc_str(corrected.end_ns),
            'raw_start_ns': window.start_ns, 'raw_end_ns': window.end_ns,
            'note': 'the recorded coverage of this symbol\'s own order_book '
                    'stream across its day files, not 00:00-24:00.',
        },
        'files': {
            'execution_npz': names[0] if len(names) == 1 else None,
            'execution_npz_all': names,
            'sources': [o['source'] for o in outputs],
            'out_dir': str(out_dir),
        },
        'inputs': {
            LIGHTER_VENUE: {
                'data_dir': str(data_dir),
                'data': [file_fingerprint(p, day) for day, p in files],
                'meta': [file_fingerprint(p) for p in meta_files if Path(p).exists()],
            },
        },
        'converter': {
            'ported_from': 'connector/src/lighter (msg.rs, depth.rs, trades.rs)',
            'exch_ts_multiplier_ns': NS_PER_US,
            'trade_id_history': TRADE_ID_HISTORY,
            'total_rows': sum(o['rows'] for o in outputs),
            'total_book_frames': sum(o['book_frames'] for o in outputs),
            'total_trade_prints': sum(o['trade_prints'] for o in outputs),
            'total_trades_deduplicated': sum(o['trades_deduplicated'] for o in outputs),
            'per_day': outputs,
        },
        'quality_gate': {
            'profile': 'lighter-market-data gate (build_lighter_dataset.py); '
                       'gap thresholds shared with quality_report.py',
            'verdict': worst,
            'gap_limits_ns': MAX_GAP_NS,
            'days': day_verdicts,
            'note': 'no Phase-2 quality-report-v1 exists for Lighter; this gate '
                    'is computed inline from the scan. A red day refuses the '
                    'build; yellows are recorded and built over.',
        },
        'initial_snapshot': False,
        'limitations': [
            'Market data only: no live fills. A passive-grid fill is modelled '
            'off the recorded trade tape by the backtest, not observed.',
            'No initial depth snapshot — the recordings begin mid-session (the '
            'collector was already subscribed at the file boundary), so the book '
            'starts empty and warms up from the window\'s first deltas. The '
            'strategy\'s warm-up guard must be correct from the first callback.',
            '%d day(s); the window is the recorded coverage of %s, not 00:00-24:00.'
            % (len(days_list), args.symbol),
        ],
    }
    manifest_path = out_dir / 'manifest.json'
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + '\n')
    _step('manifest -> %s (gate: %s)' % (manifest_path, worst))
    return manifest


def _rebuild_cmd(args: argparse.Namespace, tick_size: float,
                 lot_size: float) -> list:
    cmd = [
        sys.executable, str(Path(__file__).resolve()),
        '--data-dir', str(Path(args.data_dir).resolve()),
        '--symbol', args.symbol,
        '--out-dir', str(Path(args.out_dir).resolve()),
        '--min-window-hours', str(args.min_window_hours),
        '--clock-correction-ns', str(args.clock_correction_ns),
    ]
    if args.days:
        cmd += ['--days', args.days]
    if args.catalog:
        cmd += ['--catalog', str(Path(args.catalog).resolve())]
    if args.tick_size is not None and args.lot_size is not None:
        cmd += ['--tick-size', repr(tick_size), '--lot-size', repr(lot_size)]
    return cmd


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog='build_lighter_dataset.py',
        description='Assemble a Lighter market-data backtest dataset (profile %s) '
                    'from the collector recordings, gated on an inline Lighter '
                    'quality profile.' % PROFILE)
    p.add_argument('--data-dir', required=True, type=Path,
                   help='Directory of <symbol>_<YYYYMMDD>.gz and '
                        '_meta_lighter_<day>.jsonl.')
    p.add_argument('--symbol', required=True,
                   help='Lighter wire name, e.g. CRV (exact, case included).')
    p.add_argument('--out-dir', required=True, type=Path)
    p.add_argument('--days', default=None,
                   help='Comma-separated YYYYMMDD; default: every day with a file.')
    p.add_argument('--catalog', default=None, type=Path,
                   help='Meta .jsonl carrying the universe record; default: search '
                        '--data-dir.')
    p.add_argument('--tick-size', type=float, default=None)
    p.add_argument('--lot-size', type=float, default=None,
                   help='Give both --tick-size and --lot-size, or neither and let '
                        'the tick be measured and the lot come from the catalog.')
    p.add_argument('--min-window-hours', type=_decimal_hours, default=Decimal('1'))
    p.add_argument('--clock-correction-ns', type=int, default=0)
    return p.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None,
         finalize_fn: Optional[Callable] = None) -> int:
    args = parse_args(argv)
    try:
        build(args, finalize_fn=finalize_fn)
    except BuildError as e:
        print('error: %s' % e, file=sys.stderr)
        sys.exit(1)
    return 0


if __name__ == '__main__':
    sys.exit(main())
