"""Tests for ``hftbacktest.data.utils.hyperliquid.convert``.

Every recording here is synthetic: a tiny gzip file with known frames written
into ``tmp_path``. No network, no real recording.

The subject is the trade de-duplication. Hyperliquid replays the last 30 fills
per coin in a single ``trades`` frame on every (re)subscribe, and a replayed
fill carries the SAME ``tid`` as the original, so a recording with N reconnects
holds phantom trades that would feed extra ``TRADE_EVENT``s into the backtest.
"""

import gzip
import json
from collections import Counter

import numpy as np

from hftbacktest.data.utils import hyperliquid
from hftbacktest.types import EXCH_EVENT, TRADE_EVENT

MS = 1_000_000

# 2026-07-26T00:00:00Z in milliseconds; nanosecond local timestamps derived from
# it are 19 digits wide, which is what the reader's fixed-width slice expects.
BASE_MS = 1_785_024_000_000


def trade(tid, seq, coin='BTC', side='B'):
    """One fill. ``seq`` fixes both its venue time and its (unique) price."""
    entry = {
        'coin': coin,
        'side': side,
        'px': str(1000 + seq),
        'sz': '1.0',
        'time': BASE_MS + seq * 1000,
    }
    if tid is not None:
        entry['tid'] = tid
    return entry


def write_recording(path, frames):
    """``frames`` is an iterable of lists of trade entries; one frame per line.

    The local timestamp of a frame is half a millisecond after the newest venue
    timestamp it carries, so the feed latency is positive throughout and
    ``correct_local_timestamp`` never shifts anything.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, 'wb') as f:
        for entries in frames:
            local_ts = max(e['time'] for e in entries) * MS + 500_000
            payload = json.dumps({'channel': 'trades', 'data': entries})
            f.write(('%d %s\n' % (local_ts, payload)).encode())
    return path


def convert(path, **kwargs):
    return hyperliquid.convert(
        str(path), tick_size=0.1, lot_size=0.001, buffer_size=10_000, **kwargs
    )


def trade_prices(data):
    """Prices of the emitted trades, one entry per converted row.

    ``correct_event_order`` splits a row whose exchange order and local order
    disagree into an exchange-side row and a local-side row — which is exactly
    what a replayed fill (old ``time``, new local timestamp) causes. Counting
    the exchange side only therefore counts each emitted trade once.
    """
    is_trade = (data['ev'] & 0xff) == TRADE_EVENT
    is_exch = (data['ev'] & EXCH_EVENT) == EXCH_EVENT
    return Counter(data['px'][is_trade & is_exch].tolist())


def test_a_resubscribe_replay_is_dropped_and_counted(tmp_path):
    """3 of the previous fills come back on resubscribe; each tid stays unique."""
    frames = [[trade(tid=n, seq=n)] for n in range(1, 31)]
    # The resubscribe frame: three already-seen fills, then three new ones.
    frames.append([trade(tid=n, seq=n) for n in (28, 29, 30)]
                  + [trade(tid=n, seq=n) for n in (31, 32, 33)])

    stats = {}
    data = convert(write_recording(tmp_path / 'btc.gz', frames), stats=stats)

    prices = trade_prices(data)
    assert sum(prices.values()) == 33
    assert set(prices.values()) == {1}, 'a tid must be emitted exactly once'
    assert stats['deduplicated_trades'] == 3


def test_the_dedup_window_is_deeper_than_the_thirty_replayed_fills(tmp_path):
    """A tid last seen 31 fills ago is still recognised as a replay."""
    assert hyperliquid.TRADE_TID_HISTORY >= 30, 'the replay is 30 fills per coin'
    assert hyperliquid.TRADE_TID_HISTORY == 64, 'pinned; changing it changes the guarantee'

    frames = [[trade(tid=n, seq=n)] for n in range(1, 32)]
    frames.append([trade(tid=1, seq=1)])

    stats = {}
    data = convert(write_recording(tmp_path / 'btc.gz', frames), stats=stats)

    assert sum(trade_prices(data).values()) == 31
    assert stats['deduplicated_trades'] == 1


def test_a_tid_older_than_the_window_is_not_deduplicated(tmp_path):
    """The bound is finite and deliberate: memory over multi-day converts.

    Beyond ``TRADE_TID_HISTORY`` fills the converter no longer remembers a tid,
    so a replay that far back passes through. Hyperliquid replays 30, so this
    cannot happen on a real recording — the test states the trade-off rather
    than asserting an unbounded guarantee the code does not make.
    """
    depth = hyperliquid.TRADE_TID_HISTORY
    frames = [[trade(tid=n, seq=n)] for n in range(1, depth + 2)]
    frames.append([trade(tid=1, seq=1)])

    stats = {}
    data = convert(write_recording(tmp_path / 'btc.gz', frames), stats=stats)

    prices = trade_prices(data)
    assert prices[1001.0] == 2, 'evicted from the window, so emitted again'
    assert sum(prices.values()) == depth + 2
    assert stats['deduplicated_trades'] == 0


def test_the_window_does_not_survive_across_files(tmp_path):
    """The scope of the guard is one ``convert`` call, i.e. one recording file.

    The collector rotates a new file at every UTC midnight while the socket
    stays up, and ``build_dataset`` converts each day on its own before feeding
    the days to a single asset. A resubscribe landing within 30 fills of the
    rotation therefore replays fills that live in the *previous* day's file: the
    window is empty for them, they are emitted, and the dataset holds them
    twice while the reported count says nothing about it.

    Measured 2026-07-27: zero cross-file overlap in the current recordings (the
    rotation and the reconnects did not coincide), and the exposed window is
    ~8 s for BTC and ~2 min for ENA per rotation. Pinned so the bound on the
    manifest's ``deduplicated_trades`` stays a stated one.
    """
    day1 = write_recording(tmp_path / 'btc_day1.gz', [[trade(tid=n, seq=n)] for n in (1, 2, 3)])
    day2 = write_recording(tmp_path / 'btc_day2.gz',
                           [[trade(tid=3, seq=3), trade(tid=4, seq=4)]])

    stats1, stats2 = {}, {}
    convert(day1, stats=stats1)
    data2 = convert(day2, stats=stats2)

    assert stats1['deduplicated_trades'] == 0
    assert stats2['deduplicated_trades'] == 0, 'tid 3 is not remembered across files'
    assert trade_prices(data2)[1003.0] == 1, 'and day 1 already holds the same fill'


def test_entries_without_a_tid_pass_through_unchanged(tmp_path):
    """No tid, no evidence. Two identical fills are legitimate, so never guess."""
    frames = [
        [trade(tid=None, seq=1)],
        [trade(tid=None, seq=1)],
        [trade(tid=None, seq=2), trade(tid=7, seq=3)],
        [trade(tid=7, seq=3)],
    ]

    stats = {}
    data = convert(write_recording(tmp_path / 'btc.gz', frames), stats=stats)

    prices = trade_prices(data)
    assert prices[1001.0] == 2, 'both untagged fills are kept'
    assert prices[1002.0] == 1
    assert prices[1003.0] == 1, 'the tagged replay next to them is still dropped'
    assert stats['deduplicated_trades'] == 1


def test_the_window_is_per_coin(tmp_path):
    """A file may interleave coins; one coin's fills must not evict another's."""
    frames = []
    for n in range(1, 31):
        frames.append([trade(tid=n, seq=2 * n, coin='BTC'),
                       trade(tid=1000 + n, seq=2 * n + 1, coin='ENA')])
    frames.append([trade(tid=1, seq=2, coin='BTC'),
                   trade(tid=1001, seq=3, coin='ENA')])

    stats = {}
    data = convert(write_recording(tmp_path / 'mixed.gz', frames), stats=stats)

    assert sum(trade_prices(data).values()) == 60
    assert stats['deduplicated_trades'] == 2


def test_the_dedup_count_is_printed_even_when_it_is_zero(tmp_path, capsys):
    """One summary line, always — silence and zero must not look the same."""
    write_recording(tmp_path / 'clean.gz', [[trade(tid=n, seq=n)] for n in range(1, 4)])
    convert(tmp_path / 'clean.gz')
    assert 'deduplicated 0 replayed trades' in capsys.readouterr().out

    frames = [[trade(tid=n, seq=n)] for n in range(1, 4)]
    frames.append([trade(tid=2, seq=2)])
    write_recording(tmp_path / 'dirty.gz', frames)
    convert(tmp_path / 'dirty.gz')
    assert 'deduplicated 1 replayed trades' in capsys.readouterr().out


def test_stats_is_optional(tmp_path):
    """The out-param is additive: the existing call sites pass nothing."""
    frames = [[trade(tid=n, seq=n)] for n in range(1, 4)]
    frames.append([trade(tid=2, seq=2)])
    data = convert(write_recording(tmp_path / 'btc.gz', frames))
    assert sum(trade_prices(data).values()) == 3


def test_the_npz_output_holds_the_deduplicated_rows(tmp_path):
    """Saving is the real call path in build_dataset's sibling tools."""
    frames = [[trade(tid=n, seq=n)] for n in range(1, 4)]
    frames.append([trade(tid=1, seq=1), trade(tid=4, seq=4)])
    out = tmp_path / 'out.npz'

    convert(write_recording(tmp_path / 'btc.gz', frames), output_filename=str(out))

    saved = np.load(out)['data']
    assert sum(trade_prices(saved).values()) == 4
