import gzip
import json
from typing import Optional

import numpy as np
from numpy.typing import NDArray

from hftbacktest.data.utils.difforderbooksnapshot import (
    DiffOrderBookSnapshot,
    CHANGED,
    INSERTED, IN_THE_BOOK_DELETION,
    OUT_OF_BOOK_DELETION_ABOVE, OUT_OF_BOOK_DELETION_BELOW,
)
from hftbacktest.data.validation import correct_event_order, correct_local_timestamp, validate_event_order
from ...types import (
    DEPTH_EVENT,
    TRADE_EVENT,
    BUY_EVENT,
    SELL_EVENT,
    event_dtype
)

#: Number of trade ids remembered per coin when dropping replayed trades.
#:
#: Hyperliquid replays the last 30 fills of a coin in a single ``trades`` frame
#: on every (re)subscribe, so any window above 30 covers a whole replay; the
#: margin absorbs a replay that arrives interleaved with fresh fills.
#:
#: The window is bounded on purpose. A liquid coin sees hundreds of thousands of
#: fills a day, and remembering every id would make the converter's memory grow
#: with the recording — a multi-day conversion would pay for that with nothing
#: in return, since a replay never reaches further back than those 30 fills.
TRADE_TID_HISTORY = 64

#: Levels a side each depth stream delivers, i.e. the ``num_levels`` each
#: ``book_mode`` must be paired with. Measured over eight recordings (4 coins x
#: 2 days, 2026-07-26/27): every ``fast`` frame carried exactly 5 levels a side
#: and every plain one exactly 20 — Hyperliquid never sent a short book.
#:
#: The pairing is enforced rather than assumed. :class:`DiffOrderBookSnapshot`
#: preallocates exactly ``num_levels`` rows and writes ``len(bid_px)`` into them
#: without a bounds check (``difforderbooksnapshot.py:35-41,67-72``), so
#: :func:`convert` truncates every snapshot to ``num_levels``; without the check
#: a mispaired call would quietly return a half-depth book instead of raising.
BOOK_MODE_LEVELS = {'slow': 20, 'fast': 5, 'bbo+fast': 5}

#: The depth streams :func:`convert` can build a book from.
#:
#: ``'slow'`` and ``'fast'`` each take one ``l2Book`` cadence and ignore the
#: other. ``'bbo+fast'`` fuses the ``bbo`` touch feed into the ``fast`` cadence;
#: see :func:`convert` for what that means for the emitted rows.
BOOK_MODES = tuple(BOOK_MODE_LEVELS)

#: Book modes that fuse the ``bbo`` touch feed into an ``l2Book`` cadence.
BBO_BOOK_MODES = frozenset({'bbo+fast'})

#: Every deletion kind :class:`DiffOrderBookSnapshot` reports. A fusing mode
#: emits all of them; see the ``delete_out_of_book`` check in :func:`convert`.
ALL_DELETIONS = (IN_THE_BOOK_DELETION,
                 OUT_OF_BOOK_DELETION_ABOVE, OUT_OF_BOOK_DELETION_BELOW)


def convert(
        input_filename: str,
        tick_size: float,
        lot_size: float,
        num_levels: int = 20,
        output_filename: Optional[str] = None,
        base_latency: float = 0,
        buffer_size: int = 100_000_000,
        exch_ts_multiplier: int = 1_000_000,
        delete_out_of_book: bool = True,
        book_mode: str = 'slow',
        stats: Optional[dict] = None
) -> NDArray:
    r"""
    Converts raw Hyperliquid feed stream file into a format compatible with HftBacktest.
    If you encounter an ``IndexError`` due to an out-of-bounds, try increasing the ``buffer_size``.

    **File Format:**

    .. code-block::

        local_timestamp raw_stream
        1736682893953732482 {"channel":"trades","data":[{"coin":"HYPE","side":"A","px":"21.269","sz":"7.78","time":1736682877317,"hash":"0x0190932c67e6b94b2bc6041b482c76013b00c5a4dc99c1611ca5bdc1edcd2786","tid":185481619581124,"users":["0x010461c14e146ac35fe42271bdc1134ee31c703a","0x475d0900e45e9d9f4661f30e616e2395da592720"]},{"coin":"HYPE","side":"B","px":"21.289","sz":"3.29","time":1736682878040,"hash":"0x0000000000000000000000000000000000000000000000000000000000000000","tid":680159081286628,"users":["0x6844b345852a97d87c2bed4e0c123ee5c935c000","0xa1cb16d2b17202c336138f765559dfc73830b1fb"]},{"coin":"HYPE","side":"B","px":"21.289","sz":"0.55","time":1736682878040,"hash":"0x0000000000000000000000000000000000000000000000000000000000000000","tid":430781595823972,"users":["0xdbe452f1a0d0190e161b5fae8ee3a530f099d9a3","0xa1cb16d2b17202c336138f765559dfc73830b1fb"]},{"coin":"HYPE","side":"B","px":"21.289","sz":"4.65","time":1736682880335,"hash":"0xa105b47441ecc649b68c041b482ca5013f00c4ba6114fe23110f14dd364eda0a","tid":679380818995751,"users":["0x43205e893e4a958fb993c9a0df436530f47aefb8","0x739f081428592c9c4b0a6a88dec5f0308e6788a0"]},{"coin":"HYPE","side":"B","px":"21.289","sz":"2.41","time":1736682880518,"hash":"0x3fc2541a75d20cb2f607041b482ca70155001953351c0f0610392ac90a283353","tid":471300372581745,"users":["0x38b9a6c32685df04afb3282ecc103cbdba7ab9ff","0xa554e2958a076e273552cbaaa795494089e2308c"]},{"coin":"HYPE","side":"B","px":"21.289","sz":"2.24","time":1736682880518,"hash":"0x3fc2541a75d20cb2f607041b482ca70155001953351c0f0610392ac90a283353","tid":824956871283458,"users":["0x38b9a6c32685df04afb3282ecc103cbdba7ab9ff","0x739f081428592c9c4b0a6a88dec5f0308e6788a0"]},{"coin":"HYPE","side":"A","px":"21.279","sz":"18.28","time":1736682881724,"hash":"0xd65c56f65c3d8b89e085041b482cbb012c006342f8cbcc361981dfd6f5d8f4cd","tid":109504214426929,"users":["0x924b0b9147f3562065d180d6995dd30c95c12eac","0x972baee8b44ad0b1e73dee6c20f34e17dd01fe47"]},{"coin":"HYPE","side":"A","px":"21.273","sz":"68.65","time":1736682881724,"hash":"0xd65c56f65c3d8b89e085041b482cbb012c006342f8cbcc361981dfd6f5d8f4cd","tid":170899955875792,"users":["0x31ca8395cf837de08b24da3f660e77761dfb974b","0x972baee8b44ad0b1e73dee6c20f34e17dd01fe47"]},{"coin":"HYPE","side":"B","px":"21.289","sz":"19.74","time":1736682882067,"hash":"0x0000000000000000000000000000000000000000000000000000000000000000","tid":663835145561653,"users":["0x2657a9d65dc2e832dd0f6e05a556408353a149bc","0x739f081428592c9c4b0a6a88dec5f0308e6788a0"]},{"coin":"HYPE","side":"B","px":"21.291","sz":"4.7","time":1736682882067,"hash":"0x0000000000000000000000000000000000000000000000000000000000000000","tid":68532102663456,"users":["0x2657a9d65dc2e832dd0f6e05a556408353a149bc","0xe72d9d94337cc12659ee9d1f3aea5475035fec5a"]},{"coin":"HYPE","side":"B","px":"21.292","sz":"15.58","time":1736682882067,"hash":"0x0000000000000000000000000000000000000000000000000000000000000000","tid":43588775356743,"users":["0x2657a9d65dc2e832dd0f6e05a556408353a149bc","0xa1cb16d2b17202c336138f765559dfc73830b1fb"]},{"coin":"HYPE","side":"B","px":"21.292","sz":"42.63","time":1736682882067,"hash":"0x0000000000000000000000000000000000000000000000000000000000000000","tid":309212071809688,"users":["0x2657a9d65dc2e832dd0f6e05a556408353a149bc","0x2994bf69adee67aba8e3b28bec460ed2001df759"]},{"coin":"HYPE","side":"B","px":"21.294","sz":"2.87","time":1736682882930,"hash":"0x30b9df8c28987268a1a3041b482cce01440090b85064d8547ec0e75c8bab188e","tid":651550759705720,"users":["0x1341c77a124edb0c90bd98442f6a4d0baa5ba824","0x9d9cbc11c85848a3ebf181940f2bec9f5fc75396"]},{"coin":"HYPE","side":"B","px":"21.294","sz":"1.78","time":1736682882930,"hash":"0x30b9df8c28987268a1a3041b482cce01440090b85064d8547ec0e75c8bab188e","tid":701267030078115,"users":["0x1341c77a124edb0c90bd98442f6a4d0baa5ba824","0xe72d9d94337cc12659ee9d1f3aea5475035fec5a"]},{"coin":"HYPE","side":"B","px":"21.293","sz":"29.51","time":1736682883935,"hash":"0x3a8d19c41d366f4fd1c8041b482cde015a00275e602c0c3c97410f75181c87fd","tid":73469641722474,"users":["0x28547f2cce3bc73d5da5356745b6baeb6c53ed93","0x2994bf69adee67aba8e3b28bec460ed2001df759"]},{"coin":"HYPE","side":"B","px":"21.294","sz":"2.92","time":1736682883935,"hash":"0x3a8d19c41d366f4fd1c8041b482cde015a00275e602c0c3c97410f75181c87fd","tid":907055023121248,"users":["0x28547f2cce3bc73d5da5356745b6baeb6c53ed93","0xe72d9d94337cc12659ee9d1f3aea5475035fec5a"]},{"coin":"HYPE","side":"B","px":"21.299","sz":"203.8","time":1736682883935,"hash":"0x3a8d19c41d366f4fd1c8041b482cde015a00275e602c0c3c97410f75181c87fd","tid":114855171699084,"users":["0x28547f2cce3bc73d5da5356745b6baeb6c53ed93","0x023a3d058020fb76cca98f01b3c48c8938a22355"]},{"coin":"HYPE","side":"B","px":"21.3","sz":"0.84","time":1736682885011,"hash":"0x0000000000000000000000000000000000000000000000000000000000000000","tid":414458082088382,"users":["0x6844b345852a97d87c2bed4e0c123ee5c935c000","0x9d9cbc11c85848a3ebf181940f2bec9f5fc75396"]},{"coin":"HYPE","side":"B","px":"21.3","sz":"2.29","time":1736682886016,"hash":"0x0000000000000000000000000000000000000000000000000000000000000000","tid":516578515032737,"users":["0x2657a9d65dc2e832dd0f6e05a556408353a149bc","0x9d9cbc11c85848a3ebf181940f2bec9f5fc75396"]},{"coin":"HYPE","side":"B","px":"21.304","sz":"1.18","time":1736682886016,"hash":"0x0000000000000000000000000000000000000000000000000000000000000000","tid":977487737665395,"users":["0x2657a9d65dc2e832dd0f6e05a556408353a149bc","0xe72d9d94337cc12659ee9d1f3aea5475035fec5a"]},{"coin":"HYPE","side":"B","px":"21.304","sz":"3.28","time":1736682892095,"hash":"0x0000000000000000000000000000000000000000000000000000000000000000","tid":725923874460472,"users":["0x6844b345852a97d87c2bed4e0c123ee5c935c000","0xe72d9d94337cc12659ee9d1f3aea5475035fec5a"]},{"coin":"HYPE","side":"A","px":"21.286","sz":"2.45","time":1736682893100,"hash":"0x0000000000000000000000000000000000000000000000000000000000000000","tid":534132059665492,"users":["0xa1cb16d2b17202c336138f765559dfc73830b1fb","0xce43a7498e65b6f6c8741fd9a7efc3f78efc78c4"]},{"coin":"HYPE","side":"A","px":"21.286","sz":"15.97","time":1736682893100,"hash":"0x051810961666d7bb85c4041b482d72011f0031d0f6508508e450796bb5be327c","tid":1025215867692119,"users":["0xa1cb16d2b17202c336138f765559dfc73830b1fb","0xe30dc1eba82267d5e55391d3cdc7d6f72de9c227"]},{"coin":"HYPE","side":"A","px":"21.285","sz":"237.82","time":1736682893100,"hash":"0x051810961666d7bb85c4041b482d72011f0031d0f6508508e450796bb5be327c","tid":1028589439832641,"users":["0x023a3d058020fb76cca98f01b3c48c8938a22355","0xe30dc1eba82267d5e55391d3cdc7d6f72de9c227"]},{"coin":"HYPE","side":"A","px":"21.28","sz":"4.7","time":1736682893100,"hash":"0x051810961666d7bb85c4041b482d72011f0031d0f6508508e450796bb5be327c","tid":864606086107572,"users":["0xe72d9d94337cc12659ee9d1f3aea5475035fec5a","0xe30dc1eba82267d5e55391d3cdc7d6f72de9c227"]},{"coin":"HYPE","side":"A","px":"21.277","sz":"41.05","time":1736682893100,"hash":"0x051810961666d7bb85c4041b482d72011f0031d0f6508508e450796bb5be327c","tid":999086141165990,"users":["0x31ca8395cf837de08b24da3f660e77761dfb974b","0xe30dc1eba82267d5e55391d3cdc7d6f72de9c227"]},{"coin":"HYPE","side":"B","px":"21.304","sz":"0.23","time":1736682893702,"hash":"0x75013367f492e899e498041b482d7b0145005169149be7e7181a0ca2aecc305e","tid":185973078898076,"users":["0xc1995c6b6a101cf9ac7547688908afaf88b01302","0xe72d9d94337cc12659ee9d1f3aea5475035fec5a"]},{"coin":"HYPE","side":"B","px":"21.304","sz":"0.56","time":1736682893702,"hash":"0x75013367f492e899e498041b482d7b0145005169149be7e7181a0ca2aecc305e","tid":98226337118421,"users":["0xc1995c6b6a101cf9ac7547688908afaf88b01302","0xb6bf1eab724da50cc99e9489f4523de277d815b7"]},{"coin":"HYPE","side":"B","px":"21.305","sz":"18.42","time":1736682893702,"hash":"0x75013367f492e899e498041b482d7b0145005169149be7e7181a0ca2aecc305e","tid":627730381177664,"users":["0xc1995c6b6a101cf9ac7547688908afaf88b01302","0xa1cb16d2b17202c336138f765559dfc73830b1fb"]},{"coin":"HYPE","side":"B","px":"21.306","sz":"906.75","time":1736682893702,"hash":"0x75013367f492e899e498041b482d7b0145005169149be7e7181a0ca2aecc305e","tid":996720750934958,"users":["0xc1995c6b6a101cf9ac7547688908afaf88b01302","0xb9cd6cde81e55e1e6714d21b4f228faf98999eed"]}]}
        1736682893953758185 {"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"l2Book","coin":"HYPE","nSigFigs":null,"mantissa":null}}}
        1736682893953775297 {"channel":"l2Book","data":{"coin":"HYPE","time":1736682893796,"levels":[[{"px":"21.277","sz":"80.29","n":2},{"px":"21.276","sz":"317.1","n":2},{"px":"21.273","sz":"4.7","n":1},{"px":"21.271","sz":"1.0","n":1},{"px":"21.27","sz":"3.1","n":1},{"px":"21.268","sz":"94.03","n":1},{"px":"21.266","sz":"424.29","n":3},{"px":"21.265","sz":"97.96","n":3},{"px":"21.264","sz":"2.9","n":1},{"px":"21.263","sz":"23.74","n":2},{"px":"21.262","sz":"74.81","n":3},{"px":"21.261","sz":"494.98","n":2},{"px":"21.26","sz":"193.82","n":1},{"px":"21.259","sz":"54.7","n":2},{"px":"21.258","sz":"127.74","n":3},{"px":"21.255","sz":"194.67","n":2},{"px":"21.254","sz":"187.49","n":3},{"px":"21.251","sz":"0.5","n":1},{"px":"21.249","sz":"81.6","n":2},{"px":"21.247","sz":"28.43","n":1}],[{"px":"21.306","sz":"223.2","n":2},{"px":"21.309","sz":"43.18","n":1},{"px":"21.311","sz":"125.23","n":1},{"px":"21.312","sz":"2.99","n":1},{"px":"21.313","sz":"1.0","n":1},{"px":"21.317","sz":"60.47","n":1},{"px":"21.318","sz":"21.3","n":2},{"px":"21.319","sz":"339.71","n":2},{"px":"21.322","sz":"35.0","n":1},{"px":"21.323","sz":"227.41","n":1},{"px":"21.324","sz":"12.38","n":2},{"px":"21.33","sz":"46.18","n":2},{"px":"21.335","sz":"1.0","n":1},{"px":"21.336","sz":"120.23","n":2},{"px":"21.342","sz":"2.88","n":1},{"px":"21.344","sz":"1562.36","n":3},{"px":"21.345","sz":"391.37","n":2},{"px":"21.347","sz":"292.91","n":1},{"px":"21.348","sz":"13.41","n":3},{"px":"21.349","sz":"669.92","n":3}]]}}

    Args:
        input_filename: Input filename with path.
        tick_size: Tick size, minimum price increment.
        lot_size: Lot size, minimum quantity increment.
        num_levels: Number of market depth snapshot levels delivered by the ``l2Book`` channel. Default: ``20``.
        output_filename: If provided, the converted data will be saved to the specified filename in ``npz`` format.
        base_latency: The value to be added to the feed latency.
                      See :func:`.correct_local_timestamp`.
        buffer_size: Sets a preallocated row size for the buffer.
        exch_ts_multiplier: Multiplier to convert exchange timestamps to nanoseconds. Default: ``1_000_000``.
        delete_out_of_book: Whether to insert a market depth delete event when an existing level moves out of the book
                            (i.e., beyond the the given number of market depth snapshot levels)
        book_mode: Which depth stream to build the book from; one of :data:`BOOK_MODES`. See **Book modes** below.
                   Default: ``'slow'``.
        stats: If provided, the mapping is updated with counters describing this conversion:
               ``deduplicated_trades`` — the number of replayed trades dropped, see below;
               ``bbo_depth_frames`` — ``bbo`` frames that changed the book, ``0`` outside ``'bbo+fast'``.

    Returns:
        Converted data compatible with HftBacktest.

    **Book modes:**

    A recording interleaves three depth feeds, and they cannot simply be concatenated:
    ``l2Book`` is a full top-N snapshot of a *different* N per cadence — 20 levels every
    ~5.4 s, 5 levels every ~0.54 s with ``fast: true`` — and ``bbo`` is a one-level touch
    feed, event-driven, median 86 ms between frames. Feeding a 5-level snapshot to a
    20-level differ would delete levels 6..20 on every fast frame and restore them on the
    next slow one, oscillating the book.

    * ``'slow'`` — the plain ``l2Book`` cadence alone, ``num_levels=20``. Reproduces the
      behaviour of recordings made before the fast feed existed.
    * ``'fast'`` — the ``fast: true`` cadence alone, ``num_levels=5``.
    * ``'bbo+fast'`` — the ``fast`` cadence for depth, with the touch additionally updated
      by every ``bbo`` frame in between; ``num_levels`` must be ``5``.

    ``num_levels`` is not free to choose: it must be the level count of the chosen
    cadence, per :data:`BOOK_MODE_LEVELS`, and a mispairing raises.

    ``'bbo+fast'`` exists because the live connector is specified on ``bbo`` + ``l2Book
    fast``: without it the backtest runs on a strictly coarser book than the bot it is
    meant to predict. It emits ordinary ``DEPTH_EVENT`` rows, never ``DEPTH_BBO_EVENT``,
    which the backtest's ``Local`` processor ignores.

    The fused book is a **top-5 window**, and only inside that window is it meaningful:

    * ``bbo`` is authoritative about the touch. Mirrored levels above the new best bid, or
      below the new best ask, are gone and are emitted as deletions — which is also what
      keeps the book uncrossed when a ``bbo`` arrives through a mirror up to ~0.54 s stale.
    * Each ``fast`` snapshot is diffed against the book **as the interleaved ``bbo`` frames
      left it**, not against the previous snapshot. Diffing against the previous snapshot
      would silently swallow a level the ``bbo`` moved and the snapshot moved back.
    * Within a frame every deletion is emitted before every insert, so the book is
      uncrossed after each individual row rather than only at the end of the frame.
    * A ``bbo`` side arriving as ``null`` leaves that side of the book untouched — it is
      "no news", not "empty". Measured over eight recordings: Hyperliquid never sent one.
    * ``delete_out_of_book=False`` is **refused** for this mode. Suppressing a truncation
      deletion is safe only within the frame that produces it: the level is dropped from
      the fused book's mirror at the same moment, so no later diff can ever delete it, and
      it crosses as soon as the market moves through it. Measured on ``btc_20260727``,
      running the mode with the flag off left 1 684 955 of 1 685 014 depth rows crossed and
      grew the book to 569 x 1562 levels. (The flag is equally unsound for ``'slow'`` and
      ``'fast'``, where it long predates this mode and is left as it was.)

    Depth below the touch is thin and intermittent, and must not be read as a queue.
    A ``bbo`` that lifts the best bid pushes the deepest level out of the window; a ``bbo``
    that moves the touch *through* mirrored levels deletes every one of them, and nothing
    restores them until the next ``fast`` snapshot — median 540 ms, p99.9 930 ms, max 16.3 s
    measured. Time-weighted over ``btc_20260727`` the fused book holds 5 levels a side for
    96.9 % of the day, 4 for 1.5 %, 3 for 0.6 %, 2 for 0.4 % and **one for 0.64 %** — about
    nine minutes. ``'fast'`` alone is 5 levels for 100 % of the same span. This is the price
    of a one-level touch feed on a top-N window, not a defect of the fusion.

    **Replayed trades:**

    Hyperliquid replays the last 30 fills of a coin in a single ``trades`` frame on every
    (re)subscribe, and a replayed fill carries the same ``tid`` as the original. A recording that
    reconnected therefore holds trades that never happened twice, which would feed phantom
    ``TRADE_EVENT`` rows into the backtest and bias any model that counts trades. Trades whose
    ``tid`` was already emitted are dropped; the count is printed and reported through ``stats``.
    Entries carrying no ``tid`` are passed through unchanged — two fills of the same size at the
    same price and time are legitimate, so there is nothing to tell them apart by.

    The guard spans one call, i.e. one recording file. A resubscribe that lands within 30 fills
    of a file rotation replays fills recorded in the *previous* file; nothing here remembers
    them, so a dataset assembled from consecutive days can still hold that one replay twice and
    the reported count will not mention it. Deduplicating across files would need the caller to
    carry the window between calls.
    """


    if book_mode not in BOOK_MODES:
        raise ValueError(
            'book_mode must be one of %s, got %r.'
            % (', '.join(repr(m) for m in BOOK_MODES), book_mode)
        )
    fuse_bbo = book_mode in BBO_BOOK_MODES
    expected_levels = BOOK_MODE_LEVELS[book_mode]
    if num_levels != expected_levels:
        raise ValueError(
            f"book_mode={book_mode!r} builds the book from a cadence that delivers "
            f"exactly {expected_levels} levels a side, so num_levels must be "
            f"{expected_levels}; got {num_levels}. Every snapshot is truncated to "
            f"num_levels because DiffOrderBookSnapshot preallocates exactly that many "
            f"rows and writes len(bid_px) into them without a bounds check "
            f"(difforderbooksnapshot.py:35-41,67-72), so a mispaired call would "
            f"return a book of the wrong depth rather than raise."
        )
    if fuse_bbo and not delete_out_of_book:
        # Fail closed. Suppressing a truncation deletion looks safe inside the
        # frame that produces it — the kept level is below the new lowest bid,
        # so it does not cross yet — but the level is gone from the mirror the
        # moment it is suppressed, so no later diff can delete it, and it
        # crosses as soon as the market moves through it. Measured on
        # `btc_20260727`: 1_684_955 of 1_685_014 depth rows left the book
        # crossed and it grew to 569 x 1562 levels.
        raise ValueError(
            f"book_mode={book_mode!r} requires delete_out_of_book=True. The mode's "
            f"output invariant is an uncrossed book after every single row, and a "
            f"suppressed truncation deletion breaks it: the level is dropped from "
            f"the fused book's mirror, so nothing can ever delete it afterwards, and "
            f"it crosses as soon as the touch moves past it."
        )

    tmp = np.empty(buffer_size, event_dtype)
    row_num = 0
    timestamp_slice = 19
    diff = DiffOrderBookSnapshot(num_levels, tick_size, lot_size)

    # The top-`num_levels` book the emitted rows have built so far, as `[px, qty]`
    # pairs, most aggressive level first. Only `bbo+fast` keeps one: it is what a
    # `bbo` frame edits, and — through `diff`, whose previous state is always the
    # last mirror fed to it — what the next snapshot is diffed against.
    #
    # One mirror, not one per coin, matching what `diff` has always done. The
    # collector writes one coin per file, so this only matters to a caller that
    # concatenates coins into one recording, and such a caller already had a
    # book built from two coins' snapshots.
    mirror_bids = []
    mirror_asks = []
    bbo_depth_frames = 0

    def emit_updates(entries, count, side_ev, exch_ts, local_ts):
        """Insertions and size changes, `count` levels of `entries`.

        `DiffOrderBookSnapshot.snapshot` returns its whole preallocated array,
        not the part it just filled, and it swaps two buffers between calls — so
        rows past `count` hold what the buffer held two calls ago, complete with
        their update flags. They are only harmless while every call carries the
        same number of levels, which is exactly what fusing `bbo` breaks.
        """
        nonlocal row_num
        for i in range(count):
            entry = entries[i]
            if entry[2] == INSERTED or entry[2] == CHANGED:
                tmp[row_num] = (
                    DEPTH_EVENT | side_ev,
                    exch_ts,
                    local_ts,
                    entry[0],
                    entry[1],
                    0,
                    0,
                    0
                )
                row_num += 1

    def emit_deletions(entries, side_ev, exch_ts, local_ts, unconditional):
        """Levels that left the book, as zero-quantity rows.

        `unconditional` lists the deletion kinds that are emitted whatever
        `delete_out_of_book` says; the rest are truncation, i.e. a level that
        left the observed window rather than the book.
        """
        nonlocal row_num
        for entry in entries:
            if entry[1] in unconditional or delete_out_of_book:
                tmp[row_num] = (
                    side_ev | DEPTH_EVENT,
                    exch_ts,
                    local_ts,
                    entry[0],
                    0,
                    0,
                    0,
                    0
                )
                row_num += 1

    def emit_fused(bids, asks, exch_ts, local_ts):
        """Diff the fused book against what was emitted before, and emit that.

        `bids`/`asks` are the new state of the mirror, as `[px, qty]` pairs.
        """
        bid_px = np.array([lv[0] for lv in bids], float)
        bid_qty = np.array([lv[1] for lv in bids], float)
        ask_px = np.array([lv[0] for lv in asks], float)
        ask_qty = np.array([lv[1] for lv in asks], float)

        bid, ask, bid_del, ask_del = diff.snapshot(bid_px, bid_qty, ask_px, ask_qty)

        # Every deletion, of every kind — `delete_out_of_book=False` is refused
        # for this mode, see `convert`. Suppressing the truncation ones would
        # strand a level the mirror has already forgotten, and it would cross
        # the book the moment the touch moved past it.
        #
        # Deletions first. Every level the new state does not keep is gone
        # before any of the levels it does keep arrives, so the book is
        # uncrossed after each individual row and not merely at the end of the
        # frame — the new state is uncrossed, and a subset of an uncrossed book
        # cannot cross.
        emit_deletions(bid_del, BUY_EVENT, exch_ts, local_ts, ALL_DELETIONS)
        emit_deletions(ask_del, SELL_EVENT, exch_ts, local_ts, ALL_DELETIONS)
        emit_updates(bid, len(bids), BUY_EVENT, exch_ts, local_ts)
        emit_updates(ask, len(asks), SELL_EVENT, exch_ts, local_ts)

    def apply_bbo(bid_lv, ask_lv):
        """The mirror as this `bbo` frame leaves it. Either side may be `None`.

        `bbo` states the whole touch: no level rests above the best bid, none
        below the best ask, and none on the far side at or through either. A
        side that did not arrive says nothing and changes nothing.
        """
        # Copied, not aliased: `apply_bbo` must not be able to edit the mirror
        # before `emit_fused` has diffed against it.
        bids, asks = list(mirror_bids), list(mirror_asks)
        if bid_lv is not None:
            px = float(bid_lv['px'])
            bids = [lv for lv in bids if lv[0] < px]
            asks = [lv for lv in asks if lv[0] > px]
            bids.insert(0, [px, float(bid_lv['sz'])])
        if ask_lv is not None:
            px = float(ask_lv['px'])
            asks = [lv for lv in asks if lv[0] > px]
            bids = [lv for lv in bids if lv[0] < px]
            asks.insert(0, [px, float(ask_lv['sz'])])
        return bids[:num_levels], asks[:num_levels]

    # Recently emitted trade ids, per coin. Each `dict` is an insertion-ordered
    # set, so the oldest id is its first key and eviction stays O(1) without a
    # second structure. Keyed by coin because a recording may interleave several
    # of them, and one busy coin must not evict another's ids before its replay
    # arrives. See `TRADE_TID_HISTORY` for why the window is bounded.
    recent_tids = {}
    deduplicated_trades = 0

    with gzip.open(input_filename, 'r') as f:
        while True:
            line = f.readline()
            if not line:
                break

            local_ts = int(line[:timestamp_slice])
            message = json.loads(line[timestamp_slice + 1:])
            if message.get("channel") == "trades":
                trades_data = message.get("data", [])
                for trade in trades_data:
                    tid = trade.get("tid")
                    if tid is not None:
                        emitted = recent_tids.setdefault(trade.get("coin"), {})
                        if tid in emitted:
                            # A replay from a (re)subscribe: this fill is
                            # already in the output under the same tid.
                            deduplicated_trades += 1
                            continue
                        if len(emitted) >= TRADE_TID_HISTORY:
                            del emitted[next(iter(emitted))]
                        emitted[tid] = None
                    # An entry without a tid carries no evidence either way, so
                    # it is emitted as is rather than guessed at.

                    exch_ts = trade.get("time") * exch_ts_multiplier

                    tmp[row_num] = (
                        TRADE_EVENT | (SELL_EVENT if trade.get("side") == "A" else BUY_EVENT), # trade initiator's side
                        exch_ts,
                        int(local_ts),
                        float(trade.get("px")),
                        float(trade.get("sz")),
                        0,
                        0,
                        0
                    )
                    row_num += 1

            elif message.get("channel") == "l2Book":
                depth_data = message.get("data", {})

                # A recording may interleave two l2Book cadences: the plain feed
                # (20 levels/side, ~5s) and the `fast` one (5 levels/side,
                # ~0.5s), which the collector subscribes to together. They carry
                # different depths, and `DiffOrderBookSnapshot` treats every
                # message as a full snapshot of `num_levels` — so mixing them
                # would delete levels 6..20 on every fast message and restore
                # them on the next slow one, oscillating the book.
                #
                # Select exactly one cadence. Messages from the plain feed have
                # no `fast` key at all, so `book_mode='slow'` reproduces the
                # behaviour of recordings made before the fast feed existed.
                # `'bbo+fast'` takes the same cadence as `'fast'`; only the
                # `bbo` frames in between are extra.
                is_fast = bool(depth_data.get("fast", False))
                if is_fast != (book_mode != 'slow'):
                    continue

                exch_ts = depth_data.get("time") * exch_ts_multiplier
                levels = depth_data.get("levels")
                # Capped because `DiffOrderBookSnapshot` writes `len(bid_px)`
                # rows into a `num_levels`-row buffer with no bounds check, so a
                # deeper snapshot than the mode expects would corrupt memory
                # rather than raise. Measured over eight recordings, Hyperliquid
                # sends exactly the levels the cadence promises, so this only
                # ever fires on a mispaired book_mode/num_levels.
                bids = levels[0][:num_levels]
                asks = levels[1][:num_levels]

                if fuse_bbo:
                    # A snapshot re-states the whole window, so it replaces the
                    # mirror outright. What it is *diffed* against is the mirror
                    # as the interleaved `bbo` frames left it — not the previous
                    # snapshot, which would call a level the `bbo` moved and
                    # this snapshot moved back "unchanged" and emit nothing,
                    # stranding the backtest on the `bbo`'s size.
                    new_bids = [[float(b["px"]), float(b["sz"])] for b in bids]
                    new_asks = [[float(a["px"]), float(a["sz"])] for a in asks]
                    emit_fused(new_bids, new_asks, exch_ts, int(local_ts))
                    mirror_bids, mirror_asks = new_bids, new_asks
                    continue

                bid_px = np.array([float(b["px"]) for b in bids])
                bid_qty = np.array([float(b["sz"]) for b in bids])
                ask_px = np.array([float(a["px"]) for a in asks])
                ask_qty = np.array([float(a["sz"]) for a in asks])

                bid, ask, bid_del, ask_del = diff.snapshot(bid_px, bid_qty, ask_px, ask_qty)

                emit_updates(bid, len(bid_px), BUY_EVENT, exch_ts, int(local_ts))
                emit_updates(ask, len(ask_px), SELL_EVENT, exch_ts, int(local_ts))
                emit_deletions(bid_del, BUY_EVENT, exch_ts, int(local_ts),
                               (IN_THE_BOOK_DELETION,))
                emit_deletions(ask_del, SELL_EVENT, exch_ts, int(local_ts),
                               (IN_THE_BOOK_DELETION,))

            elif fuse_bbo and message.get("channel") == "bbo":
                bbo_data = message.get("data", {})
                exch_ts = bbo_data.get("time") * exch_ts_multiplier
                # `[bid, ask]`, either of which the venue types as nullable.
                sides = bbo_data.get("bbo") or ()
                bids, asks = apply_bbo(
                    sides[0] if len(sides) > 0 else None,
                    sides[1] if len(sides) > 1 else None,
                )

                # Most `bbo` frames restate a touch that has not moved. Diffing
                # an unchanged book would emit nothing anyway; skipping keeps
                # ~656k frames a day out of the differ.
                if bids == mirror_bids and asks == mirror_asks:
                    continue

                emit_fused(bids, asks, exch_ts, int(local_ts))
                mirror_bids, mirror_asks = bids, asks
                bbo_depth_frames += 1

    # Printed unconditionally: a converter that says nothing and a recording
    # with nothing to drop must not look the same to whoever reads the log.
    print('deduplicated %d replayed trades' % deduplicated_trades)
    if fuse_bbo:
        # The whole reason the mode exists is that these frames used to be
        # dropped in silence. A conversion that applied none of them fused
        # nothing, and must not look like one that did.
        print('applied %d bbo frames to the book' % bbo_depth_frames)
    if stats is not None:
        stats['deduplicated_trades'] = deduplicated_trades
        stats['bbo_depth_frames'] = bbo_depth_frames

    tmp = tmp[:row_num]

    if row_num == 0:
        # Fail closed. A file with no market data still converts cleanly to a
        # zero-row npz that is indistinguishable from a day the venue was
        # silent, and it lands in the dataset directory looking legitimate.
        # The common causes are all mistakes worth surfacing: the collector's
        # `_meta_*.gz` sidecar passed in by a wildcard, a recording from a
        # different venue, a truncated file, or a book_mode that matched
        # nothing in this recording.
        raise ValueError(
            f'{input_filename!r} yielded no market-data records. '
            f'Check it is a Hyperliquid symbol recording (not a _meta sidecar '
            f'or another venue), and that book_mode={book_mode!r} matches a '
            f'cadence present in the file.'
        )

    print('Correcting the latency')
    tmp = correct_local_timestamp(tmp, base_latency)

    print('Correcting the event order')
    data = correct_event_order(
        tmp,
        np.argsort(tmp['exch_ts'], kind='mergesort'),
        np.argsort(tmp['local_ts'], kind='mergesort')
    )

    validate_event_order(data)

    if output_filename is not None:
        print('Saving to %s' % output_filename)
        np.savez_compressed(output_filename, data=data)

    return data
