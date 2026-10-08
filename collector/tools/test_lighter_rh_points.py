"""Tests for lighter_rh_points.py, on responses captured from api.rh.lighter.xyz
on 2026-10-08 (testdata/lighter_rh_points/)."""

import gzip
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

import lighter_rh_points as lp

DATA = Path(__file__).parent / "testdata" / "lighter_rh_points"
NOW = datetime(2026, 10, 8, 12, 0, tzinfo=timezone.utc)


def load(name):
    return json.loads((DATA / name).read_text())


def real_fetch(path, params):
    """Serves the captured responses: the leaderboards whole, and the one
    captured volume page (5 rows of 3554) at offset 0, empty past it."""
    if path == "/api/v1/leaderboard":
        return load(f"leaderboard_{params['type']}.json")
    if path == "/api/v1/pnlLeaderboard":
        if params["offset"] == 0:
            return load("pnl_all_volume_offset0_limit5.json")
        return load("pnl_all_volume_past_end.json")
    raise AssertionError(path)


def no_pause(_):
    pass


def test_a_snapshot_reads_the_real_responses():
    snap = lp.snapshot(real_fetch, now=NOW, pause=no_pause)
    top = snap["leaderboard_all"]
    assert len(top) == 10
    assert [r["entry"] for r in top] == list(range(1, 11))
    assert top[0]["l1_address"].startswith("0xa4")
    assert top[0]["points"] > top[-1]["points"] > 0
    assert snap["top_all_points_sum"] == pytest.approx(sum(r["points"] for r in top))
    # Empty on a Thursday, and recorded as such rather than dropped.
    assert snap["leaderboard_weekly"] == []
    assert snap["volume_total_accounts"] == 3554
    assert len(snap["accounts"]) == 5
    # Five of 3554: the snapshot says so instead of passing for the population.
    assert snap["volume_complete"] is False
    assert snap["schema"] == lp.SCHEMA
    assert snap["taken_at"] == "2026-10-08T12:00:00+00:00"


def test_the_volume_walk_pages_until_a_short_page_and_counts_each_account_once(monkeypatch):
    monkeypatch.setattr(lp, "PAGE_LIMIT", 2)
    rows = load("pnl_all_volume_offset0_limit5.json")["entries"]
    calls, pauses = [], []

    def fetch(path, params):
        calls.append(params["offset"])
        # Pages 0-1, 2-3, then a page whose boundary shifted by one: row 3
        # is served twice, as when an account is inserted mid-walk.
        pages = {0: rows[0:2], 2: rows[2:4], 4: rows[3:4]}
        return {"code": 200, "entries": pages[params["offset"]], "total": 4,
                "updated_at": 1791450000}

    table = lp.volume_table(fetch, pause=pauses.append)
    assert calls == [0, 2, 4]
    assert len(pauses) == 2, "a pause between pages, none after the last"
    assert len(table["accounts"]) == 4
    assert table["complete"] is True
    assert table["updated_at"] == [1791450000]


def test_a_refused_page_is_an_error_not_a_short_population():
    def fetch(path, params):
        if path == "/api/v1/pnlLeaderboard":
            return {"code": 20001, "message": "invalid param "}
        return real_fetch(path, params)

    with pytest.raises(lp.SnapshotError):
        lp.snapshot(fetch, now=NOW, pause=no_pause)


def test_a_leaderboard_row_of_another_shape_is_refused():
    def fetch(path, params):
        if path == "/api/v1/leaderboard":
            return {"entries": [{"l1_address": "0xa4**", "score": 1.0}]}
        return real_fetch(path, params)

    with pytest.raises(lp.SnapshotError):
        lp.snapshot(fetch, now=NOW, pause=no_pause)


def test_main_writes_nothing_when_the_snapshot_fails(tmp_path):
    def fetch(path, params):
        raise OSError("connection refused")

    assert lp.main(["--out-dir", str(tmp_path)], fetch=fetch) == 1
    assert list(tmp_path.iterdir()) == []


def test_main_refuses_a_missing_directory(tmp_path):
    assert lp.main(["--out-dir", str(tmp_path / "nope")], fetch=real_fetch) == 2


def test_main_appends_one_line_per_run_and_flags_an_incomplete_walk(tmp_path, monkeypatch):
    monkeypatch.setattr(lp.time, "sleep", no_pause)
    assert lp.main(["--out-dir", str(tmp_path)], fetch=real_fetch) == 3
    assert lp.main(["--out-dir", str(tmp_path)], fetch=real_fetch) == 3
    (out,) = tmp_path.iterdir()
    # The shape offload.sh moves: <name>_<YYYYMMDD>.gz, valid gzip throughout.
    assert out.name.startswith("points_") and out.name.endswith(".gz")
    assert len(out.name) == len("points_20261008.gz")
    lines = gzip.decompress(out.read_bytes()).decode().splitlines()
    assert len(lines) == 2
    record = json.loads(lines[0])
    assert record["volume_complete"] is False
    assert len(record["leaderboard_all"]) == 10


def http_429():
    import urllib.error
    return urllib.error.HTTPError("https://api.rh.lighter.xyz/x", 429, "Too Many Requests", {}, None)


def test_a_rate_limited_request_is_retried_after_a_growing_wait():
    # The first deployed run (2026-10-08 12:16Z) died on a 429 a few pages
    # into the walk: the venue's REST budget is unpublished and shared per IP.
    answers = [http_429(), http_429(), {"ok": 1}]
    waits = []

    def fetch(path, params):
        answer = answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer

    patient = lp.with_backoff(fetch, pause=waits.append)
    assert patient("/p", {}) == {"ok": 1}
    assert waits == list(lp.BACKOFF_S[:2])
    assert waits[0] < waits[1]


def test_a_venue_that_keeps_refusing_still_fails_the_run():
    waits = []

    def fetch(path, params):
        raise http_429()

    patient = lp.with_backoff(fetch, pause=waits.append)
    with pytest.raises(OSError):
        patient("/p", {})
    assert waits == list(lp.BACKOFF_S)


def test_only_a_rate_limit_is_retried():
    import urllib.error
    waits = []

    def fetch(path, params):
        raise urllib.error.HTTPError("u", 500, "boom", {}, None)

    with pytest.raises(OSError):
        lp.with_backoff(fetch, pause=waits.append)("/p", {})
    assert waits == []
