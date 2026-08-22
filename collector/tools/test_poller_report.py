"""Тесты гейта каталогов поллеров.

Каждый тест отвечает измеренному отказу, а не воображаемому: обрыв gzip
(`bybit/hftusdt` 21.08), молчащий таймер (params, 23 часа 12.08), исчезнувшая
нога (её не видно вовсе, потому что файла нет) и записи об ошибке, которые все
три поллера пишут, а никто не читает.
"""

from __future__ import annotations

import gzip
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from poller_report import GREEN, RED, YELLOW, build_report, expected_ticks, leg_name

DAY = "20260821"
PREV = "20260820"
CADENCE = 3600.0
# Полдень следующих суток: день закончен, поправка на незавершённость не мешает.
AFTER = datetime(2026, 8, 22, 12, 0, tzinfo=timezone.utc)


def write(root: Path, rel: str, records: list[dict]) -> Path:
    p = root / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(p, "wt", encoding="utf-8") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")
    return p


def hourly(day: str, n: int, kind: str = "pulse", start_hour: int = 0) -> list[dict]:
    out = []
    for h in range(start_hour, start_hour + n):
        ts = datetime(int(day[:4]), int(day[4:6]), int(day[6:]), h % 24,
                      tzinfo=timezone.utc).isoformat()
        out.append({"ts": ts, "venue": "bybit", "kind": kind})
    return out


def test_a_full_day_on_every_leg_is_green(tmp_path):
    for venue in ("bybit", "aster"):
        write(tmp_path, f"{venue}/params_{venue}_x_{DAY}.gz", hourly(DAY, 24))
        write(tmp_path, f"{venue}/params_{venue}_x_{PREV}.gz", hourly(PREV, 24))
    r = build_report(tmp_path, DAY, CADENCE, now=AFTER)
    assert r["verdict"] == GREEN, r["issues"]
    assert r["legs"]["bybit/params_bybit_x"]["ticks"] == 24


def test_a_silent_timer_is_red(tmp_path):
    """12.08: таймер params не сработал 23 часа, следом была только тишина."""
    write(tmp_path, f"bybit/params_bybit_x_{DAY}.gz", hourly(DAY, 1))
    r = build_report(tmp_path, DAY, CADENCE, now=AFTER)
    assert r["verdict"] == RED
    assert any(i["kind"] == "tick_coverage" for i in r["issues"])


def test_a_few_missed_ticks_are_yellow_not_red(tmp_path):
    """Рестарт или джиттер таймера — это не отказ, и красным быть не должен."""
    write(tmp_path, f"bybit/params_bybit_x_{DAY}.gz", hourly(DAY, 20))
    r = build_report(tmp_path, DAY, CADENCE, now=AFTER)
    assert r["verdict"] == YELLOW


def test_a_truncated_member_is_red(tmp_path):
    """Файл, дописывавшийся в момент нечистой смерти, теряет трейлер."""
    p = write(tmp_path, f"bybit/params_bybit_x_{DAY}.gz", hourly(DAY, 24))
    data = p.read_bytes()
    p.write_bytes(data[: len(data) - 8])          # срезаем трейлер члена
    r = build_report(tmp_path, DAY, CADENCE, now=AFTER)
    assert r["verdict"] == RED
    assert any(i["kind"] == "gzip_integrity" for i in r["issues"])


def test_a_leg_that_vanished_since_yesterday_is_red(tmp_path):
    """Площадка, которую перестали опрашивать, не видна НИЧЕМ, кроме сверки."""
    write(tmp_path, f"bybit/params_bybit_x_{PREV}.gz", hourly(PREV, 24))
    write(tmp_path, f"aster/params_aster_x_{PREV}.gz", hourly(PREV, 24))
    write(tmp_path, f"bybit/params_bybit_x_{DAY}.gz", hourly(DAY, 24))
    r = build_report(tmp_path, DAY, CADENCE, now=AFTER)
    assert r["verdict"] == RED
    assert [i["kind"] for i in r["issues"] if i["kind"] == "leg_vanished"]
    assert "aster" in " ".join(i["text"] for i in r["issues"])


def test_error_records_are_reported_not_swallowed(tmp_path):
    recs = hourly(DAY, 24)
    recs[3]["kind"] = "error"
    write(tmp_path, f"bybit/params_bybit_x_{DAY}.gz", recs)
    write(tmp_path, f"bybit/params_bybit_x_{PREV}.gz", hourly(PREV, 24))
    r = build_report(tmp_path, DAY, CADENCE, now=AFTER)
    assert r["verdict"] == YELLOW
    assert r["legs"]["bybit/params_bybit_x"]["errors"] == 1


def test_a_long_hole_is_flagged_even_when_coverage_looks_fine(tmp_path):
    """Половина суток без опроса при формально приличном покрытии."""
    # Девять часов, одиннадцатичасовая дыра, четыре часа. Покрытие 54% — жёлтое,
    # то есть само по себе о дыре не говорит; о ней говорит только разрыв.
    recs = hourly(DAY, 9) + hourly(DAY, 4, start_hour=20)
    write(tmp_path, f"bybit/params_bybit_x_{DAY}.gz", recs)
    write(tmp_path, f"bybit/params_bybit_x_{PREV}.gz", hourly(PREV, 24))
    r = build_report(tmp_path, DAY, CADENCE, now=AFTER)
    assert any(i["kind"] == "tick_gap" for i in r["issues"]), r["issues"]


def test_records_of_one_tick_are_not_counted_as_several_ticks(tmp_path):
    """positions пишет на один тик и role, и funding; funding — по эндпоинту."""
    recs = []
    for r0 in hourly(DAY, 24):
        recs.append({**r0, "kind": "role"})
        recs.append({**r0, "kind": "funding"})
    write(tmp_path, f"0xabc/positions_0xabc_{DAY}.gz", recs)
    write(tmp_path, f"0xabc/positions_0xabc_{PREV}.gz", hourly(PREV, 24))
    rep = build_report(tmp_path, DAY, CADENCE, now=AFTER)
    assert rep["legs"]["0xabc/positions_0xabc"]["ticks"] == 24
    assert rep["legs"]["0xabc/positions_0xabc"]["records"] == 48
    assert rep["verdict"] == GREEN


def test_nanosecond_stamps_are_understood(tmp_path):
    """funding-поллер метит t_local_ns, а не ISO — обе формы настоящие."""
    base = datetime(2026, 8, 21, tzinfo=timezone.utc).timestamp()
    recs = [{"t_local_ns": int((base + h * 3600) * 1e9), "venue": "hyperliquid"}
            for h in range(24)]
    write(tmp_path, f"funding_hyperliquid_{DAY}.gz", recs)
    write(tmp_path, f"funding_hyperliquid_{PREV}.gz",
          [{"t_local_ns": int((base - 86400 + h * 3600) * 1e9)} for h in range(24)])
    r = build_report(tmp_path, DAY, CADENCE, now=AFTER)
    assert r["legs"]["funding_hyperliquid"]["ticks"] == 24
    assert r["verdict"] == GREEN


def test_an_unfinished_day_is_not_condemned_for_being_unfinished(tmp_path):
    """В полдень прошла половина ожидаемых тиков, и это не отказ."""
    write(tmp_path, f"bybit/params_bybit_x_{DAY}.gz", hourly(DAY, 12))
    noon = datetime(2026, 8, 21, 12, 0, tzinfo=timezone.utc)
    assert expected_ticks(CADENCE, DAY, noon) == 12
    r = build_report(tmp_path, DAY, CADENCE, now=noon)
    assert r["verdict"] == GREEN


def test_an_empty_directory_is_red_not_green(tmp_path):
    """Отсутствие данных обязано быть красным: «нечего проверять» — не «чисто»."""
    assert build_report(tmp_path, DAY, CADENCE, now=AFTER)["verdict"] == RED


def test_the_leg_name_survives_every_layout_the_pollers_use(tmp_path):
    assert leg_name(Path(f"funding_aster_{DAY}.gz"), DAY) == "funding_aster"
    assert leg_name(Path(f"bybit/params_bybit_risk-limit_{DAY}.gz"), DAY) \
        == "bybit/params_bybit_risk-limit"
    assert leg_name(Path(f"0xabc/positions_0xabc_{DAY}.gz"), DAY) == "0xabc/positions_0xabc"
