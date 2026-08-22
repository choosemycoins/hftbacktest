"""Тесты поллера позиций операторов HL. Сеть не трогаем — источник подменяется двойником.

Проверяем ровно то, ради чего поллер существует: что новые часовые записи позиции
дописываются, что уже виденные НЕ дублируются, что отсутствие новых записей всё равно
оставляет след, и что беда одного адреса не уносит остальные.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest

from positions_poller import (
    RECORD_LIMIT,
    load_state,
    poll_once,
    resolve_master,
    save_state,
)


def read_records(out_dir: Path, kind: str | None = None) -> list[dict]:
    out: list[dict] = []
    for p in sorted(out_dir.rglob("*.gz")):
        with gzip.open(p, "rt", encoding="utf-8") as fh:
            out.extend(json.loads(line) for line in fh if line.strip())
    return [r for r in out if kind is None or r["kind"] == kind]


def funding(ts: int, coin: str = "HYPE", szi: str = "100.0") -> dict:
    return {"time": ts, "delta": {"type": "funding", "coin": coin, "szi": szi,
                                  "fundingRate": "0.0000125", "usdc": "1.0"}}


@pytest.fixture()
def addrs(tmp_path):
    p = tmp_path / "addrs.json"
    p.write_text(json.dumps(["0xaaa"]), encoding="utf-8")
    return p


def test_new_hourly_records_are_appended(tmp_path, addrs):
    """Ряд позиций — это и есть продукт; новые часы обязаны попасть на диск как есть."""
    def fetch(kind, addr, start=None):
        if kind == "userFunding":
            return [funding(1000), funding(2000)]
        return {"role": "user"}

    poll_once(tmp_path, addrs, fetch=fetch)
    recs = read_records(tmp_path, "funding")
    assert [r["time"] for r in recs] == [1000, 2000]
    assert [float(r["delta"]["szi"]) for r in recs] == [100.0, 100.0]


def test_already_seen_records_are_not_duplicated(tmp_path, addrs):
    """Второй прогон обязан дописать ТОЛЬКО то, чего ещё не было: иначе ряд
    распухает копиями и любая агрегация по нему врёт."""
    state = {"n": 0}

    def fetch(kind, addr, start=None):
        if kind != "userFunding":
            return {"role": "user"}
        state["n"] += 1
        if state["n"] == 1:
            return [funding(1000), funding(2000)]
        # площадка отдаёт от переданного start; двойник это уважает
        assert start is not None and start > 2000
        return [funding(3000)]

    poll_once(tmp_path, addrs, fetch=fetch)
    poll_once(tmp_path, addrs, fetch=fetch)
    assert [r["time"] for r in read_records(tmp_path, "funding")] == [1000, 2000, 3000]


def test_no_new_records_still_leaves_a_pulse(tmp_path, addrs):
    """Без пульса «позиция не менялась» и «мы не смотрели» неразличимы — та же
    причина, по которой пульс есть у поллера параметров."""
    def fetch(kind, addr, start=None):
        return [] if kind == "userFunding" else {"role": "user"}

    poll_once(tmp_path, addrs, fetch=fetch)
    pulses = read_records(tmp_path, "pulse")
    assert len(pulses) == 1 and pulses[0]["user"] == "0xaaa"


def test_one_bad_address_does_not_lose_the_others(tmp_path):
    """Аутэдж по одному адресу не должен уносить весь проход: урок #75, где чужой
    503 убил сбор насовсем."""
    p = tmp_path / "a.json"
    p.write_text(json.dumps(["0xbad", "0xgood"]), encoding="utf-8")

    def fetch(kind, addr, start=None):
        if addr == "0xbad":
            raise TimeoutError("венью молчит")
        return [funding(1000)] if kind == "userFunding" else {"role": "user"}

    poll_once(tmp_path, p, fetch=fetch)
    assert [r["user"] for r in read_records(tmp_path, "error")] == ["0xbad"]
    assert [r["user"] for r in read_records(tmp_path, "funding")] == ["0xgood"]


def test_subaccount_is_resolved_to_its_master(tmp_path, addrs):
    """8 из 12 топ-мейкеров HYPE — сабаккаунты, трое под одним мастером. Без
    разрешения ранжирование и роли систематически врут, поэтому мастер пишется рядом."""
    def fetch(kind, addr, start=None):
        if kind == "userRole":
            return {"role": "subAccount", "data": {"master": "0xMASTER"}}
        return [funding(1000)]

    poll_once(tmp_path, addrs, fetch=fetch)
    roles = read_records(tmp_path, "role")
    assert roles[0]["role"] == "subAccount" and roles[0]["master"] == "0xMASTER"
    # и запись позиции несёт мастера, чтобы агрегация не требовала второго джойна
    assert read_records(tmp_path, "funding")[0]["master"] == "0xMASTER"


def test_resolve_master_is_total():
    """Неизвестная форма ответа не должна валить проход — fail closed на разборе."""
    assert resolve_master({"role": "user"}) == (None, "user")
    assert resolve_master({"role": "vault"}) == (None, "vault")
    assert resolve_master({"role": "subAccount", "data": {"master": "0xM"}}) == ("0xM", "subAccount")
    assert resolve_master({"role": "subAccount"}) == (None, "subAccount")
    assert resolve_master({}) == (None, "unknown")
    assert resolve_master(None) == (None, "unknown")


def test_broken_state_is_read_as_knowing_nothing(tmp_path):
    """Битое состояние хуже отсутствующего только если оно врёт; читаем как «не знаем»,
    в худшем случае перекачаем лишнее."""
    (tmp_path / ".positions_state.json").write_text("{не json", encoding="utf-8")
    assert load_state(tmp_path) == {}


def test_state_survives_a_roundtrip(tmp_path):
    save_state(tmp_path, {"0xaaa": 12345})
    assert load_state(tmp_path) == {"0xaaa": 12345}


def test_pagination_advances_past_the_last_seen_time(tmp_path, addrs):
    """Площадка отдаёт по RECORD_LIMIT; полный ответ означает «есть ещё», и поллер
    обязан дозапросить, иначе на активном адресе ряд будет с дырами."""
    calls = []

    def fetch(kind, addr, start=None):
        if kind != "userFunding":
            return {"role": "user"}
        calls.append(start)
        if len(calls) == 1:
            return [funding(1000 + i) for i in range(RECORD_LIMIT)]
        return [funding(9000)]

    poll_once(tmp_path, addrs, fetch=fetch)
    times = [r["time"] for r in read_records(tmp_path, "funding")]
    assert len(times) == RECORD_LIMIT + 1 and times[-1] == 9000
    assert calls[1] == 1000 + RECORD_LIMIT - 1 + 1  # строго за последним виденным


# --- ретрай на рейт-лимит -------------------------------------------------
# Первый живой бэкфилл по 60 адресам получил HTTP 429 на 37 из них. Часовой поллер,
# теряющий 60% адресов каждый прогон, даёт ряд с РАЗНЫМИ дырами каждый час — то есть
# невосстановимый даже задним числом. Ретрай тут не удобство, а условие целостности.

import urllib.error

from positions_poller import BACKOFF_S, RETRIES, fetch_info


class _Resp:
    def __init__(self, payload): self._p = payload
    def read(self): return json.dumps(self._p).encode()
    def __enter__(self): return self
    def __exit__(self, *a): return False


def _http(code):
    return urllib.error.HTTPError("u", code, "boom", {}, None)


def test_rate_limit_is_retried_and_then_succeeds():
    calls, slept = {"n": 0}, []

    def opener(req, timeout=None):
        calls["n"] += 1
        if calls["n"] < 3:
            raise _http(429)
        return _Resp([{"time": 1}])

    got = fetch_info("userFunding", "0xa", _open=opener, _sleep=slept.append)
    assert got == [{"time": 1}] and calls["n"] == 3
    assert slept == [BACKOFF_S, BACKOFF_S * 2]  # экспоненциальное отступление


def test_retries_are_bounded_and_the_error_still_surfaces():
    """Исчерпав попытки, ошибку НЕ глотаем: вызывающий обязан записать её как
    наблюдение, иначе потеря адреса станет невидимой."""
    slept = []

    def opener(req, timeout=None):
        raise _http(429)

    with pytest.raises(urllib.error.HTTPError):
        fetch_info("userFunding", "0xa", _open=opener, _sleep=slept.append)
    assert len(slept) == RETRIES


def test_a_non_retryable_status_fails_immediately():
    """404 повторять бессмысленно — это не перегрузка, а ответ по существу."""
    slept = []

    def opener(req, timeout=None):
        raise _http(404)

    with pytest.raises(urllib.error.HTTPError):
        fetch_info("userFunding", "0xa", _open=opener, _sleep=slept.append)
    assert slept == []


# --- контракт с offload.sh -------------------------------------------------
# Тот же приём, что в test_funding_poller.py и test_params_poller.py: вырезаем
# НАСТОЯЩУЮ day_of из offload.sh и гоняем её. Два совпадающих литерала в двух
# файлах доказывали бы только собственное совпадение.
import subprocess

OFFLOAD_SH = Path(__file__).parent.parent / "deploy" / "offload.sh"


def offload_day_of(name: str) -> str | None:
    text = OFFLOAD_SH.read_text(encoding="utf-8")
    start = text.index("day_of() {")
    func = text[start : text.index("\n}\n", start) + 3]
    r = subprocess.run(
        ["bash", "-c", func + '\nday_of "$1"', "day_of", name],
        capture_output=True, text=True,
    )
    return r.stdout.strip() if r.returncode == 0 else None


def test_every_data_file_is_recognised_and_dated_by_offload(tmp_path, addrs):
    """Ряд позиций обязан УЕЗЖАТЬ с хоста, а не копиться на нём вечно.

    Поллер пишет в ПОДКАТАЛОГ адреса, то есть offload.sh видит имя вида
    `0xaaa/positions_0xaaa_20260822.gz`. Замерено 2026-08-22: до правки day_of
    якорилась на `^`, такое имя не признавала, и файл уходил в «unrecognised
    names, left alone» — НЕ доезжал до архива и НЕ удалялся с хоста, при
    зелёном пульсе и молчащем TG. Ровно тот же класс, что блокер панели #88
    у funding-поллера. Здесь это стоило бы 188 МБ на корневом диске в 6.7 ГБ.
    """
    def fetch(kind, addr, start=None):
        if kind == "userFunding":
            return [funding(1000)]
        return {"role": "user"}

    poll_once(tmp_path, addrs, fetch=fetch)
    written = sorted(tmp_path.rglob("positions_*.gz"))
    assert written, "поллер обязан что-то записать"
    for path in written:
        rel = path.relative_to(tmp_path).as_posix()
        assert "/" in rel, "имя, которое увидит offload.sh, несёт подкаталог адреса"
        day_in_name = path.stem.rsplit("_", 1)[1]
        assert offload_day_of(rel) == day_in_name, rel
