"""Тесты поллера параметров. Сеть не трогаем — источник подменяется двойником.

Проверяем ровно то, ради чего поллер существует: что изменение параметра
фиксируется, что неизменность доказывается пульсом, и что дышащие поля
(объём, цена, время сервера) НЕ считаются изменением параметра.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest

from params_poller import SOURCES, poll_once, stable_hash, strip_volatile


def read_records(out_dir: Path) -> list[dict]:
    out = []
    for p in sorted(out_dir.rglob("params_*.gz")):
        with gzip.open(p, "rt", encoding="utf-8") as fh:
            out.extend(json.loads(line) for line in fh if line.strip())
    return out


@pytest.fixture()
def one_source(monkeypatch):
    """Оставляем ровно один источник, чтобы тесты читались."""
    monkeypatch.setitem(SOURCES, "__test__", [("cat", "https://example.invalid/cat")])
    for venue in list(SOURCES):
        if venue != "__test__":
            monkeypatch.delitem(SOURCES, venue)


def test_a_breathing_field_is_not_a_parameter_change(tmp_path, one_source):
    """Объём и время сервера меняются каждый опрос. Если считать их изменением
    параметра, ряд изменений утонет в шуме и станет бесполезен."""
    state = {"n": 0}

    def fetch(_url):
        state["n"] += 1
        return {"symbol": "ETH", "tickSize": "0.01", "volume": state["n"], "serverTime": state["n"]}

    poll_once(tmp_path, fetch=fetch)
    poll_once(tmp_path, fetch=fetch)
    kinds = [r["kind"] for r in read_records(tmp_path)]
    assert kinds == ["snapshot", "pulse"], kinds


def test_a_real_parameter_change_is_recorded_with_the_previous_hash(tmp_path, one_source):
    """Смена тика — то самое событие, ради которого всё затевалось; и она
    обязана нести хеш предыдущего состояния, иначе «до» не восстановить."""
    state = {"tick": "0.01"}

    def fetch(_url):
        return {"symbol": "ETH", "tickSize": state["tick"], "volume": 1}

    poll_once(tmp_path, fetch=fetch)
    state["tick"] = "0.0025"  # ровно тот случай из корпуса, что «заректило за ночь»
    poll_once(tmp_path, fetch=fetch)

    recs = read_records(tmp_path)
    assert [r["kind"] for r in recs] == ["snapshot", "snapshot"]
    assert recs[1]["prev_sha256"] == recs[0]["sha256"]
    assert recs[1]["payload"]["tickSize"] == "0.0025"


def test_an_unchanged_catalog_still_leaves_a_pulse(tmp_path, one_source):
    """Без пульса «не изменилось» и «мы не смотрели» неразличимы."""
    poll_once(tmp_path, fetch=lambda _u: {"a": 1})
    poll_once(tmp_path, fetch=lambda _u: {"a": 1})
    recs = read_records(tmp_path)
    assert recs[1]["kind"] == "pulse"
    assert recs[1]["sha256"] == recs[0]["sha256"]


def test_an_unreachable_venue_is_recorded_not_swallowed(tmp_path, one_source):
    """Недоступность площадки — само по себе наблюдение (линия «здоровье
    площадок»), поэтому она пишется, а не теряется."""
    def fetch(_url):
        raise TimeoutError("венью молчит")

    poll_once(tmp_path, fetch=fetch)
    recs = read_records(tmp_path)
    assert recs[0]["kind"] == "error"
    assert "венью молчит" in recs[0]["error"]


def test_an_error_does_not_poison_the_known_hash(tmp_path, one_source):
    """После сбоя следующий успешный опрос не должен выглядеть как изменение,
    если параметры на самом деле те же."""
    calls = {"n": 0}

    def fetch(_url):
        calls["n"] += 1
        if calls["n"] == 2:
            raise TimeoutError("моргнуло")
        return {"a": 1}

    poll_once(tmp_path, fetch=fetch)   # snapshot
    poll_once(tmp_path, fetch=fetch)   # error
    poll_once(tmp_path, fetch=fetch)   # обязан быть pulse, а не snapshot
    assert [r["kind"] for r in read_records(tmp_path)] == ["snapshot", "error", "pulse"]


def test_key_order_does_not_fake_a_change(tmp_path, one_source):
    """Площадка вправе переставить ключи в ответе; это не изменение параметра."""
    seq = [{"a": 1, "b": 2}, {"b": 2, "a": 1}]

    def fetch(_url):
        return seq.pop(0)

    poll_once(tmp_path, fetch=fetch)
    poll_once(tmp_path, fetch=fetch)
    assert [r["kind"] for r in read_records(tmp_path)] == ["snapshot", "pulse"]


def test_volatile_stripping_is_recursive_and_keeps_parameters():
    payload = {"symbols": [{"symbol": "ETH", "tickSize": "0.01", "lastPrice": "1", "filters": [{"minQty": "5", "volume": 9}]}]}
    stripped = strip_volatile(payload)
    s = stripped["symbols"][0]
    assert s["tickSize"] == "0.01" and s["filters"][0]["minQty"] == "5"
    assert "lastPrice" not in s and "volume" not in s["filters"][0]


def test_hash_is_stable_across_equal_payloads():
    assert stable_hash({"a": [1, {"b": 2}]}) == stable_hash({"a": [1, {"b": 2}]})
    assert stable_hash({"a": 1}) != stable_hash({"a": 2})


def test_reordered_catalog_is_not_a_change(tmp_path, one_source):
    """Замерено вживую 11.08: Lighter и Paradex вернули те же рынки в другом
    порядке через 11 секунд. Позиционное сравнение объявило «изменились» 46
    параметров. Без канонизации порядка ряд изменений — шум на 100%."""
    seq = [
        {"markets": [{"s": "ETH", "tick": "0.01"}, {"s": "BTC", "tick": "0.1"}]},
        {"markets": [{"s": "BTC", "tick": "0.1"}, {"s": "ETH", "tick": "0.01"}]},
    ]
    poll_once(tmp_path, fetch=lambda _u: seq.pop(0))
    poll_once(tmp_path, fetch=lambda _u: seq.pop(0))
    assert [r["kind"] for r in read_records(tmp_path)] == ["snapshot", "pulse"]


def test_a_change_inside_a_reordered_catalog_is_still_caught(tmp_path, one_source):
    """Канонизация порядка не имеет права прятать настоящее изменение."""
    seq = [
        {"markets": [{"s": "ETH", "tick": "0.01"}, {"s": "BTC", "tick": "0.1"}]},
        {"markets": [{"s": "BTC", "tick": "0.1"}, {"s": "ETH", "tick": "0.0025"}]},
    ]
    poll_once(tmp_path, fetch=lambda _u: seq.pop(0))
    poll_once(tmp_path, fetch=lambda _u: seq.pop(0))
    assert [r["kind"] for r in read_records(tmp_path)] == ["snapshot", "snapshot"]
