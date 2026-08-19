"""Тесты поллера кросс-венью фандинга (#88).

Фикстуры в testdata/funding/ — усечённые ЖИВЫЕ ответы площадок (сняты 2026-08-20,
см. RECON.md задачи): форма пиннется бытием, не выдумкой. Для тиков, где нужен
объём выше пола правдоподобия, записи фикстуры тиражируются — форма та же.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest

import funding_poller as fp

TESTDATA = Path(__file__).parent / "testdata" / "funding"


def fixture_bytes(name: str) -> bytes:
    return (TESTDATA / name).read_bytes()


def inflate(name: str) -> bytes:
    """Фикстура, растиражированная выше пола правдоподобия своей ноги."""
    doc = json.loads(fixture_bytes(name))
    if name.startswith("hl_metaAndAssetCtxs"):
        n = 120
        doc = [{"universe": doc[0]["universe"] * n}, doc[1] * n]
    elif name.startswith("hl_predictedFundings"):
        doc = doc * 120
    elif name.startswith("lighter"):
        doc = {"code": doc["code"], "funding_rates": doc["funding_rates"] * 120}
    elif name.startswith("aster"):
        doc = doc * 120
    elif name.startswith("paradex"):
        doc = {"results": doc["results"] * 120}
    else:  # pragma: no cover — новая фикстура без ветки — это ошибка теста
        raise AssertionError(name)
    return json.dumps(doc).encode()


GOOD = {
    "hyperliquid.metaAndAssetCtxs": inflate("hl_metaAndAssetCtxs.trunc.json"),
    "hyperliquid.predictedFundings": inflate("hl_predictedFundings.trunc.json"),
    "lighter.funding-rates": inflate("lighter_funding_rates.trunc.json"),
    "aster.premiumIndex": inflate("aster_premiumIndex.trunc.json"),
    "paradex.markets-summary": inflate("paradex_markets_summary.trunc.json"),
}


class FakeHost:
    """Записывающая подделка Host: ни сети, ни диска, ни systemd."""

    def __init__(self, responses=None, *, now_ns=1_787_200_000_000_000_000,
                 state_text=None, broken_venues=()):
        self.responses = dict(GOOD if responses is None else responses)
        self._now_ns = now_ns
        self.state_path = "/fake/state.json"
        self._files = {self.state_path: state_text} if state_text else {}
        self.jsonl = []          # (venue, day, line)
        self.pulses = []
        self.telegrams = []
        self.written = {}
        self.broken_venues = set(broken_venues)  # append_jsonl кидает OSError

    def now_ns(self):
        return self._now_ns

    def fetch(self, leg):
        body = self.responses[leg.name]
        if isinstance(body, Exception):
            raise body
        return body

    def read_file(self, path):
        return self._files.get(path)

    def write_atomic(self, path, text):
        self.written[path] = text
        self._files[path] = text

    def append_jsonl(self, venue, day, line):
        if venue in self.broken_venues:
            raise OSError(28, "No space left on device")
        self.jsonl.append((venue, day, line))

    def append_pulse(self, line):
        self.pulses.append(line)

    def send_telegram(self, text):
        self.telegrams.append(text)
        return True


# ==============================================================================
# Форма ног: URL-ы и методы — это измеренный контракт, не конфиг
# ==============================================================================


def test_legs_pin_the_measured_endpoints():
    by_name = {leg.name: leg for leg in fp.LEGS}
    assert set(by_name) == set(GOOD)

    hl_meta = by_name["hyperliquid.metaAndAssetCtxs"]
    assert hl_meta.url == "https://api.hyperliquid.xyz/info"
    assert json.loads(hl_meta.body) == {"type": "metaAndAssetCtxs"}

    hl_pred = by_name["hyperliquid.predictedFundings"]
    assert hl_pred.url == "https://api.hyperliquid.xyz/info"
    assert json.loads(hl_pred.body) == {"type": "predictedFundings"}

    assert by_name["lighter.funding-rates"].url == \
        "https://mainnet.zklighter.elliot.ai/api/v1/funding-rates"
    assert by_name["lighter.funding-rates"].body is None

    assert by_name["aster.premiumIndex"].url == \
        "https://fapi.asterdex.com/fapi/v1/premiumIndex"
    assert by_name["aster.premiumIndex"].body is None

    assert by_name["paradex.markets-summary"].url == \
        "https://api.prod.paradex.trade/v1/markets/summary?market=ALL"
    assert by_name["paradex.markets-summary"].body is None

    # Binance premiumIndex уже пишет Rust-коллектор — ноги на fapi.binance.com
    # здесь быть НЕ должно (дубль записи).
    assert not any("binance.com" in leg.url for leg in fp.LEGS)


@pytest.mark.parametrize("name,fixture,expected", [
    ("hyperliquid.metaAndAssetCtxs", "hl_metaAndAssetCtxs.trunc.json", 2),
    ("hyperliquid.predictedFundings", "hl_predictedFundings.trunc.json", 2),
    ("lighter.funding-rates", "lighter_funding_rates.trunc.json", 2),
    ("aster.premiumIndex", "aster_premiumIndex.trunc.json", 2),
    ("paradex.markets-summary", "paradex_markets_summary.trunc.json", 2),
])
def test_symbol_counter_reads_each_live_shape(name, fixture, expected):
    leg = next(leg for leg in fp.LEGS if leg.name == name)
    assert leg.count(json.loads(fixture_bytes(fixture))) == expected


def test_symbol_counter_rejects_alien_shapes():
    # Пустой-но-валидный `[]` сюда не входит: пустоту ловит пол правдоподобия
    # (см. test_a_broken_leg_does_not_stop_the_tick), а здесь — чужие формы.
    for leg in fp.LEGS:
        for alien in ({}, {"error": "x"}, "string", None, 42):
            with pytest.raises(fp.FundingFetchError):
                leg.count(alien)


def test_hl_meta_counter_rejects_desynced_universe():
    # ctx позиционно сшит с universe; расхождение длин = битый документ,
    # а не «сколько-то символов».
    doc = json.loads(fixture_bytes("hl_metaAndAssetCtxs.trunc.json"))
    doc[0]["universe"] = doc[0]["universe"][:1]
    leg = next(leg for leg in fp.LEGS if leg.name == "hyperliquid.metaAndAssetCtxs")
    with pytest.raises(fp.FundingFetchError):
        leg.count(doc)


def test_floors_are_below_the_measured_universe():
    # Измерено 2026-08-20: HL 232, lighter 723, aster 702, paradex 1697.
    measured = {
        "hyperliquid.metaAndAssetCtxs": 232,
        "hyperliquid.predictedFundings": 232,
        "lighter.funding-rates": 723,
        "aster.premiumIndex": 702,
        "paradex.markets-summary": 1697,
    }
    for leg in fp.LEGS:
        assert 0 < leg.floor < measured[leg.name], leg.name


# ==============================================================================
# Тик: запись
# ==============================================================================


def test_a_good_tick_writes_one_line_per_leg_into_per_venue_day_files():
    host = FakeHost()
    fp.tick(host, hostname="tokyo")
    assert len(host.jsonl) == 5
    # 2026-08-20 05:46:40 UTC — день из now_ns, не из площадки
    assert {(v, d) for v, d, _ in host.jsonl} == {
        ("hyperliquid", "20260820"), ("lighter", "20260820"),
        ("aster", "20260820"), ("paradex", "20260820"),
    }
    hl_lines = [json.loads(l) for v, _, l in host.jsonl if v == "hyperliquid"]
    assert {l["endpoint"] for l in hl_lines} == \
        {"metaAndAssetCtxs", "predictedFundings"}


def test_written_line_carries_the_raw_payload_untouched():
    host = FakeHost()
    fp.tick(host, hostname="tokyo")
    for venue, _day, line in host.jsonl:
        rec = json.loads(line)
        assert set(rec) == {"t_local_ns", "venue", "endpoint", "payload"}
        assert rec["venue"] == venue
        assert rec["t_local_ns"] == host.now_ns()
        leg = next(l for l in fp.LEGS
                   if l.venue == venue and l.endpoint == rec["endpoint"])
        assert rec["payload"] == json.loads(host.responses[leg.name])


def test_line_is_single_line_json():
    host = FakeHost()
    fp.tick(host, hostname="tokyo")
    for _v, _d, line in host.jsonl:
        assert "\n" not in line


# ==============================================================================
# Fail-closed: провал ноги, не тика
# ==============================================================================


BAD_BODIES = [
    b"",                          # пустой ответ
    b"<html>502 Bad Gateway</html>",  # не-JSON
    b"{}",                        # чужая форма
    b'{"code":200,"funding_rates":[]}',  # пустая таблица (ниже пола)
]


@pytest.mark.parametrize("bad", BAD_BODIES)
def test_a_broken_leg_does_not_stop_the_tick(bad):
    host = FakeHost({**GOOD, "lighter.funding-rates": bad})
    fp.tick(host, hostname="tokyo")
    venues = [v for v, _, _ in host.jsonl]
    assert "lighter" not in venues
    assert sorted(venues) == ["aster", "hyperliquid", "hyperliquid", "paradex"]


def test_a_network_error_is_a_leg_failure_too():
    host = FakeHost({**GOOD, "paradex.markets-summary": OSError("timed out")})
    fp.tick(host, hostname="tokyo")
    assert "paradex" not in [v for v, _, _ in host.jsonl]
    assert len(host.jsonl) == 4


def test_an_undersized_response_is_a_leg_failure():
    # Форма валидная, но записей меньше пола: усечённый ответ, прочитанный как мир,
    # оставил бы дыру в ряде молча. Усечённая фикстура (2 записи) сама по себе бита.
    host = FakeHost({**GOOD,
                     "aster.premiumIndex": fixture_bytes("aster_premiumIndex.trunc.json")})
    fp.tick(host, hostname="tokyo")
    assert "aster" not in [v for v, _, _ in host.jsonl]


def test_a_disk_failure_on_one_venue_does_not_stop_the_tick():
    host = FakeHost(broken_venues={"hyperliquid"})
    fp.tick(host, hostname="tokyo")
    assert sorted(v for v, _, _ in host.jsonl) == ["aster", "lighter", "paradex"]
    state = json.loads(host.written[host.state_path])
    assert state["legs"]["hyperliquid.metaAndAssetCtxs"]["consecutive_failures"] == 1


# ==============================================================================
# Серия провалов ноги → собственный алерт с именем площадки
# ==============================================================================


def run_ticks(n, responses, state_text=None):
    """n тиков подряд с переносом состояния; возвращает последний host."""
    for _ in range(n):
        host = FakeHost(responses, state_text=state_text)
        fp.tick(host, hostname="tokyo")
        state_text = host.written[host.state_path]
        yield host


def test_leg_failure_series_alerts_with_the_venue_name():
    bad = {**GOOD, "lighter.funding-rates": b"{}"}
    hosts = list(run_ticks(fp.FAIL_ALERT_AFTER, bad))
    for host in hosts[:-1]:
        assert host.telegrams == []
    final = hosts[-1].telegrams
    assert len(final) == 1
    assert "lighter.funding-rates" in final[0]
    assert "🔴" in final[0]


def test_no_alert_below_the_threshold_and_counter_survives_restarts():
    bad = {**GOOD, "lighter.funding-rates": b"{}"}
    hosts = list(run_ticks(fp.FAIL_ALERT_AFTER - 1, bad))
    assert all(h.telegrams == [] for h in hosts)
    state = json.loads(hosts[-1].written[hosts[-1].state_path])
    assert state["legs"]["lighter.funding-rates"]["consecutive_failures"] == \
        fp.FAIL_ALERT_AFTER - 1


def test_realert_happens_on_cadence_not_every_tick():
    bad = {**GOOD, "lighter.funding-rates": b"{}"}
    n = fp.FAIL_ALERT_AFTER + fp.FAIL_REALERT_EVERY
    hosts = list(run_ticks(n, bad))
    alerted = [i for i, h in enumerate(hosts) if h.telegrams]
    assert alerted == [fp.FAIL_ALERT_AFTER - 1, n - 1]


def test_recovery_after_an_alert_sends_a_green_and_resets_the_counter():
    bad = {**GOOD, "lighter.funding-rates": b"{}"}
    hosts = list(run_ticks(fp.FAIL_ALERT_AFTER, bad))
    state_text = hosts[-1].written[hosts[-1].state_path]

    host = FakeHost(GOOD, state_text=state_text)
    fp.tick(host, hostname="tokyo")
    assert len(host.telegrams) == 1
    assert "🟢" in host.telegrams[0]
    assert "lighter.funding-rates" in host.telegrams[0]
    state = json.loads(host.written[host.state_path])
    assert state["legs"]["lighter.funding-rates"]["consecutive_failures"] == 0
    assert state["legs"]["lighter.funding-rates"]["alerted"] is False


def test_recovery_without_a_prior_alert_is_silent():
    bad = {**GOOD, "lighter.funding-rates": b"{}"}
    hosts = list(run_ticks(2, bad))
    state_text = hosts[-1].written[hosts[-1].state_path]
    host = FakeHost(GOOD, state_text=state_text)
    fp.tick(host, hostname="tokyo")
    assert host.telegrams == []


# ==============================================================================
# Пульс: наблюдаем эффект, тишина = второй канал тревоги
# ==============================================================================


def test_pulse_carries_per_leg_counters():
    host = FakeHost({**GOOD, "lighter.funding-rates": b"{}"})
    fp.tick(host, hostname="tokyo")
    assert len(host.pulses) == 1
    pulse = json.loads(host.pulses[0])
    assert pulse["legs"]["lighter.funding-rates"]["ok"] is False
    assert pulse["legs"]["aster.premiumIndex"]["ok"] is True
    assert pulse["legs"]["aster.premiumIndex"]["symbols"] == 240  # 2 × 120
    assert pulse["legs"]["lighter.funding-rates"]["consecutive_failures"] == 1


def test_no_pulse_when_every_leg_failed():
    host = FakeHost({name: b"" for name in GOOD})
    fp.tick(host, hostname="tokyo")
    assert host.pulses == []
    # но состояние (счётчики) записано — иначе серия никогда не накопится
    assert host.state_path in host.written


# ==============================================================================
# Состояние
# ==============================================================================


def test_broken_or_missing_state_reads_as_empty():
    assert fp.load_state(None) == fp.State()
    assert fp.load_state("") == fp.State()
    assert fp.load_state("{garbage") == fp.State()
    assert fp.load_state('{"schema": 999}') == fp.State()
    assert fp.load_state('{"schema": 1, "legs": "no"}') == fp.State()


def test_state_roundtrip():
    state = fp.State(legs={"lighter.funding-rates": fp.LegState(3, True)})
    assert fp.load_state(fp.dump_state(state)) == state


# ==============================================================================
# Исполняющий слой
# ==============================================================================


def test_dry_run_host_reads_but_never_writes(capsys):
    inner = FakeHost()
    dry = fp.DryRunHost(inner)
    fp.tick(dry, hostname="tokyo")
    assert inner.jsonl == []
    assert inner.pulses == []
    assert inner.telegrams == []
    assert inner.written == {}
    out = capsys.readouterr().out
    assert "DRY RUN" in out


def test_real_host_appends_gzip_jsonl_per_venue_day(tmp_path):
    host = fp.RealHost(tmp_path / "data", tmp_path / "pulse")
    host.append_jsonl("aster", "20260820", '{"x":1}')
    host.append_jsonl("aster", "20260820", '{"x":2}')
    p = tmp_path / "data" / "aster-20260820.jsonl.gz"
    assert p.exists()
    with gzip.open(p, "rt", encoding="utf-8") as fh:
        assert fh.read() == '{"x":1}\n{"x":2}\n'


def test_real_host_pulse_is_a_gz_file_heartbeat_can_find(tmp_path):
    host = fp.RealHost(tmp_path / "data", tmp_path / "pulse",
                       _now_ns=lambda: 1_787_200_000_000_000_000)
    host.append_pulse('{"t":1}')
    files = list((tmp_path / "pulse").glob("pulse-*.gz"))
    assert len(files) == 1
    assert files[0].name == "pulse-20260820.gz"


def test_real_host_gunzips_by_content_encoding(tmp_path):
    class Resp:
        def __init__(self, body, enc):
            self._body, self.headers = body, {"Content-Encoding": enc}
        def read(self):
            return self._body
        def __enter__(self):
            return self
        def __exit__(self, *a):
            return False
        class headers:  # noqa — заменяется в __init__
            pass

    class H:
        def __init__(self, body, enc):
            self.body, self.enc = body, enc
        def __call__(self, req, timeout):
            r = Resp(self.body, self.enc)
            r.headers = {"Content-Encoding": self.enc} if self.enc else {}
            r.headers = type("Hs", (), {"get": lambda s, k, d=None: (
                {"Content-Encoding": self.enc}.get(k, d))})()
            return r

    leg = fp.LEGS[0]
    raw = b'{"ok": 1}'
    host = fp.RealHost(tmp_path / "d", tmp_path / "p",
                       _open=H(gzip.compress(raw), "gzip"))
    assert host.fetch(leg) == raw
    host = fp.RealHost(tmp_path / "d", tmp_path / "p", _open=H(raw, ""))
    assert host.fetch(leg) == raw
