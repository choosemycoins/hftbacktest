#!/usr/bin/env python3
"""Daily snapshot of the Lighter-on-Robinhood-Chain points programme.

Appends one JSON line per run to ``<--out-dir>/points_<UTC YYYYMMDD>.gz``,
each line its own gzip member, so the file is valid gzip after every run and
matches the ``<name>_<YYYYMMDD>.gz`` shape ``offload.sh`` moves off the host
once the day is over. Public, unauthenticated
GETs only, against ``https://api.rh.lighter.xyz``; nothing here logs in, signs
or holds a key.

What the instance publishes without auth, checked 2026-10-08:

* ``/api/v1/leaderboard?type=all`` — the points leaderboard, **top 10 only**,
  addresses masked to their first two hex digits (``0xa4****…``), points as a
  float. ``limit`` is ignored. ``type=weekly`` answered ``{"entries":[]}`` on a
  Thursday; whether it fills after the Friday drop is one of the things these
  snapshots exist to find out.
* ``/api/v1/pnlLeaderboard`` — every account that has traded (``total`` = 3554
  on 2026-10-08), with ``volume``, ``pnl``, ``account_value``; pages of at most
  100, sortable by volume; refreshed hourly (``updated_at``).
* Per-account points (``/api/v1/livePoints/total``, ``/api/v1/referral/points``)
  require auth and are **not** used.

So points are visible for ten accounts and volume for all of them. A day's
emission and its concentration are estimated offline from consecutive
snapshots: the top-10 points delta, the all-account volume delta, and the
points-per-dollar of the top 10 (matched to their volume rows by the unmasked
two hex digits, where that is unambiguous). This script only records; it
computes nothing that a later reader could not recompute from the line.

Fail closed: a page that fails or a catalogue that changes shape stops the run
with exit 1 and **no line is written** — a partial snapshot would read as a
smaller population. ``volume_complete`` records whether the pages covered
``total`` (accounts can appear between pages); it is false rather than a
failure, because a few accounts short is still a usable snapshot, and the
exit code is then 3 so the timer's journal shows it.

Usage::

    lighter_rh_points.py --out-dir /opt/hft-collector/data/lighter-rh-points
"""

from __future__ import annotations

import argparse
import gzip
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

BASE_URL = "https://api.rh.lighter.xyz"
PAGE_LIMIT = 100  # the documented maximum; 1000 is refused with code 20001
PAGE_PAUSE_S = 3.0  # ~383 pages (38k accounts on 08.10), ~19 min a run; the REST budget is not published and
# is shared per IP with the collectors — 0.5 s drew a 429 on the first deploy
BACKOFF_S = (15, 60, 180)  # waits before each retry of a 429, then give up
MAX_PAGES = 1000  # 100k accounts — a runaway guard, not an expectation
MAX_WALKS = 2  # a walk is ~21 min and the table refreshes hourly, so the second walk sits inside one refresh
TIMEOUT_S = 20
SCHEMA = "lighter-rh-points-v1"

Fetch = Callable[[str, dict], dict]


class SnapshotError(RuntimeError):
    """The venue answered something this snapshot cannot be built from."""


def http_get(path: str, params: dict) -> dict:
    url = f"{BASE_URL}{path}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": "hft-collector-points/1"})
    with urllib.request.urlopen(req, timeout=TIMEOUT_S) as resp:
        return json.load(resp)


def with_backoff(fetch: Fetch, pause: Callable[[float], None] = time.sleep) -> Fetch:
    """``fetch`` that waits out a 429 and asks again, ``BACKOFF_S`` times.

    Only a rate limit is retried: any other refusal says something about the
    request or the venue, and a retry would only hide it. A venue still
    refusing after the last wait fails the run as before.
    """
    def patient(path: str, params: dict) -> dict:
        for wait in BACKOFF_S:
            try:
                return fetch(path, params)
            except urllib.error.HTTPError as error:
                if error.code != 429:
                    raise
                print(f"lighter_rh_points: 429 on {path}, retrying in {wait} s",
                      file=sys.stderr)
                pause(wait)
        return fetch(path, params)
    return patient


def leaderboard(fetch: Fetch, kind: str) -> list:
    """The points leaderboard of one ``type``, validated row by row."""
    body = fetch("/api/v1/leaderboard", {"type": kind})
    entries = body.get("entries")
    if not isinstance(entries, list):
        raise SnapshotError(f"leaderboard type={kind}: no entries array: {body!r:.300}")
    out = []
    for row in entries:
        try:
            out.append({
                "l1_address": str(row["l1_address"]),
                "points": float(row["points"]),
                "entry": int(row["entry"]),
            })
        except (KeyError, TypeError, ValueError) as error:
            raise SnapshotError(f"leaderboard type={kind}: bad row {row!r}: {error}") from None
    return out


def volume_table(fetch: Fetch, pause: Callable[[float], None] = time.sleep) -> dict:
    """Every account's all-time volume, walked again if the first walk mixed
    two hourly refreshes.

    A refresh landing mid-walk re-ranks the table under the cursor: rows that
    slide back past it are never served. Measured 2026-10-16 20:01Z (the
    Friday run starts on the hour): 48 of 42115 accounts missed, 47 of them
    present in the morning snapshot — not newcomers. Keeping the rows of the
    first walk would mix two hours, so a repeat walk replaces it whole.
    """
    for walk in range(1, MAX_WALKS + 1):
        table = _walk_volume(fetch, pause)
        table["walks"] = walk
        if len(table["updated_at"]) <= 1:
            break
        if walk < MAX_WALKS:
            print(f"lighter_rh_points: refresh landed mid-walk {table['updated_at']}, walking again",
                  file=sys.stderr)
            pause(PAGE_PAUSE_S)
    return table


def _walk_volume(fetch: Fetch, pause: Callable[[float], None]) -> dict:
    """One walk of every account's all-time volume, paged out of ``pnlLeaderboard``.

    Sorted by volume descending so that if the population grows mid-walk the
    newcomers (small volume) land at the end, where they are least likely to
    shift a page boundary over a large account. Rows are keyed by address, so
    a row seen twice across a shifted boundary is counted once.
    """
    rows: dict = {}
    total = None
    updated_at = set()
    for page in range(MAX_PAGES):
        body = fetch("/api/v1/pnlLeaderboard", {
            "time_window": "all", "sort_by": "volume", "sort_dir": "desc",
            "limit": PAGE_LIMIT, "offset": page * PAGE_LIMIT,
        })
        if body.get("code") != 200 or not isinstance(body.get("entries"), list):
            raise SnapshotError(f"pnlLeaderboard offset={page * PAGE_LIMIT}: {body!r:.300}")
        total = int(body["total"])
        updated_at.add(int(body["updated_at"]))
        for row in body["entries"]:
            try:
                rows[str(row["l1_address"])] = {
                    "volume": float(row["volume"]),
                    "pnl": float(row["pnl"]),
                    "account_value": float(row["account_value"]),
                }
            except (KeyError, TypeError, ValueError) as error:
                raise SnapshotError(f"pnlLeaderboard: bad row {row!r}: {error}") from None
        if len(body["entries"]) < PAGE_LIMIT:
            break
        pause(PAGE_PAUSE_S)
    else:
        raise SnapshotError(f"pnlLeaderboard: more than {MAX_PAGES} pages")
    return {
        "total": total,
        # More than one value means the hourly refresh landed mid-walk and the
        # pages mix two hours; ``volume_table`` walks again.
        "updated_at": sorted(updated_at),
        "complete": total is not None and len(rows) >= total,
        "accounts": rows,
    }


def snapshot(fetch: Fetch, now: Optional[datetime] = None,
             pause: Callable[[float], None] = time.sleep) -> dict:
    now = now or datetime.now(timezone.utc)
    top_all = leaderboard(fetch, "all")
    top_weekly = leaderboard(fetch, "weekly")
    volume = volume_table(fetch, pause)
    return {
        "schema": SCHEMA,
        "taken_at": now.isoformat(timespec="seconds"),
        "base_url": BASE_URL,
        "leaderboard_all": top_all,
        "leaderboard_weekly": top_weekly,
        "top_all_points_sum": round(sum(r["points"] for r in top_all), 6),
        "volume_total_accounts": volume["total"],
        "volume_updated_at": volume["updated_at"],
        "volume_complete": volume["complete"],
        "volume_walks": volume["walks"],
        "volume_sum": round(sum(r["volume"] for r in volume["accounts"].values()), 6),
        "accounts": volume["accounts"],
    }


def out_path(out_dir: str, now: datetime) -> Path:
    return Path(out_dir) / f"points_{now.strftime('%Y%m%d')}.gz"


def append_line(path: Path, record: dict) -> None:
    """One line, one gzip member, one write: a reader never sees half a
    snapshot, and the file stays valid gzip between runs."""
    line = json.dumps(record, separators=(",", ":"), sort_keys=True) + "\n"
    member = gzip.compress(line.encode("utf-8"))
    with open(path, "ab") as f:
        f.write(member)
        f.flush()


def main(argv=None, fetch: Fetch = http_get) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out-dir", required=True,
                        help="directory for points_<YYYYMMDD>.gz (must exist)")
    args = parser.parse_args(argv)
    if not Path(args.out_dir).is_dir():
        print(f"lighter_rh_points: {args.out_dir} is not a directory", file=sys.stderr)
        return 2
    now = datetime.now(timezone.utc)
    try:
        record = snapshot(with_backoff(fetch), now=now)
    except (SnapshotError, OSError, ValueError) as error:
        print(f"lighter_rh_points: no snapshot written: {error}", file=sys.stderr)
        return 1
    append_line(out_path(args.out_dir, now), record)
    print(
        f"lighter_rh_points: top10={record['top_all_points_sum']:.1f} "
        f"weekly_rows={len(record['leaderboard_weekly'])} "
        f"accounts={len(record['accounts'])}/{record['volume_total_accounts']} "
        f"volume=${record['volume_sum'] / 1e9:.3f}B walks={record['volume_walks']} "
        f"complete={record['volume_complete']}"
    )
    return 0 if record["volume_complete"] else 3


if __name__ == "__main__":
    sys.exit(main())
