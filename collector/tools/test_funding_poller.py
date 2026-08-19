"""Тесты поллера кросс-венью фандинга (#88).

Фикстуры в testdata/funding/ — усечённые ЖИВЫЕ ответы площадок (сняты 2026-08-20,
см. RECON.md задачи): форма пиннется бытием, не выдумкой. Для тиков, где нужен
объём выше пола правдоподобия, записи фикстуры тиражируются — форма та же.
"""

from __future__ import annotations

import gzip
import http.client
import json
import os
import subprocess
import time
import zlib
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
    # 2026-08-20 04:26:40 UTC — день из now_ns, не из площадки
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


# Классы, которыми провод рвётся НА САМОМ ДЕЛЕ. Три из четырёх не наследуют
# OSError — измерено на python 3.13: IncompleteRead → HTTPException → Exception,
# EOFError и zlib.error — прямые наследники Exception. Пока `_run_leg` ловил
# перечисление, любой из них уносил ВЕСЬ тик: остальные ноги не опрашивались,
# состояние не писалось (счётчик провалов не рос, значит алерт по ноге не
# наступал никогда), пульса не было. gzip получают ровно те две ноги, что и
# отдают его — aster и paradex.
TRANSPORT_FAILURES = [
    OSError("timed out"),                                   # сокет
    http.client.IncompleteRead(b"{\"resu"),                 # тело оборвалось
    EOFError("Compressed file ended before the end-of-stream marker"),
    zlib.error("Error -3 while decompressing data"),        # gzip побит в пути
]


@pytest.mark.parametrize(
    "exc", TRANSPORT_FAILURES,
    ids=lambda e: f"{type(e).__module__}.{type(e).__name__}")
def test_any_transport_failure_costs_one_leg_and_no_more(exc):
    host = FakeHost({**GOOD, "paradex.markets-summary": exc})
    fp.tick(host, hostname="tokyo")
    assert sorted(v for v, _, _ in host.jsonl) == \
        ["aster", "hyperliquid", "hyperliquid", "lighter"]
    # Провал посчитан — иначе серия не накопится и алерт по ноге не наступит.
    state = json.loads(host.written[host.state_path])
    assert state["legs"]["paradex.markets-summary"]["consecutive_failures"] == 1
    assert len(host.pulses) == 1


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
# Сутки файла — UTC, а не хостовые
# ==============================================================================


@pytest.fixture
def tokyo_tz():
    """Часовой пояс процесса — токийский: там, где стоит хост сбора, локальные
    сутки уже наступили, а UTC-е ещё нет. Без этого пин вакуумен на UTC-машине."""
    if not hasattr(time, "tzset"):  # pragma: no cover — не Unix
        pytest.skip("tzset только на Unix")
    import os
    previous = os.environ.get("TZ")
    os.environ["TZ"] = "Asia/Tokyo"
    time.tzset()
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = previous
        time.tzset()


# 2026-08-20 23:59:30 UTC — в Токио это уже 21-е.
NEAR_UTC_MIDNIGHT_NS = 1_787_270_370_000_000_000


def test_the_file_day_is_the_utc_day_not_the_hosts(tokyo_tz):
    # Ряд режется по UTC-суткам: иначе один и тот же час лежал бы в разных
    # файлах у хостов в разных поясах, а потребитель (#40) склеивает их вслепую.
    host = FakeHost(now_ns=NEAR_UTC_MIDNIGHT_NS)
    fp.tick(host, hostname="tokyo")
    assert {d for _, d, _ in host.jsonl} == {"20260820"}


def test_the_pulse_file_day_is_the_utc_day_too(tokyo_tz, tmp_path):
    host = fp.RealHost(tmp_path / "data", tmp_path / "pulse",
                       _now_ns=lambda: NEAR_UTC_MIDNIGHT_NS)
    host.append_pulse('{"t":1}')
    assert [p.name for p in (tmp_path / "pulse").glob("pulse-*.gz")] == \
        ["pulse-20260820.gz"]


# ==============================================================================
# Исполняющий слой
# ==============================================================================


def test_alert_env_is_read_the_way_alert_sh_writes_it(tmp_path):
    # Тот же файл, что у alert.sh/heartbeat.sh: строки с `export`, кавычки,
    # комментарии. Прочитанный неверно токен — это молчащий TG при живом алерте.
    path = tmp_path / "alert.env"
    path.write_text(
        "# алертинг коллектора\n"
        "\n"
        'export TG_BOT_TOKEN="123:abc"\n'
        "TG_CHAT_ID='-1001'\n"
        "мусор без знака равенства\n",
        encoding="utf-8")
    assert fp.read_alert_env(str(path)) == ("123:abc", "-1001")


def test_missing_or_empty_alert_env_reads_as_not_configured(tmp_path):
    assert fp.read_alert_env(str(tmp_path / "нет-такого")) == (None, None)
    path = tmp_path / "alert.env"
    path.write_text("TG_BOT_TOKEN=\nTG_CHAT_ID=\n", encoding="utf-8")
    assert fp.read_alert_env(str(path)) == (None, None)


@pytest.mark.parametrize(
    "exc", TRANSPORT_FAILURES,
    ids=lambda e: f"{type(e).__module__}.{type(e).__name__}")
def test_send_telegram_reports_false_on_any_transport_failure(tmp_path, capsys, exc):
    # Зеркало пина _run_leg (03f12ac), тот же класс бага на втором сетевом
    # вызове того же файла: перечисление (OSError, ValueError) пропускало
    # наружу http.client.IncompleteRead из resp.read() — РОВНО во время
    # инцидента (алерт по ноге due) и ДО записи состояния и пульса. Устойчивый
    # обрыв пути к api.telegram.org давал «данные пишутся, state замер, пульса
    # нет» — сторож кричал про мёртвый поллер при живой записи. Канал тревоги
    # не смеет ронять канал данных: send_telegram не кидает НИКОГДА.
    env = tmp_path / "alert.env"
    env.write_text("TG_BOT_TOKEN=secret-token\nTG_CHAT_ID=-1001\n",
                   encoding="utf-8")

    def opener(req, timeout):
        raise exc

    host = fp.RealHost(tmp_path / "d", tmp_path / "p",
                       alert_env_path=str(env), _open=opener)
    assert host.send_telegram("probe") is False
    out = capsys.readouterr().out
    assert "secret-token" not in out     # текст ошибки может нести URL с токеном
    assert type(exc).__name__ in out     # но класс отказа оператору виден


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


# ------------------------------------------------------------------------
# Имя файла данных — контракт с offload.sh, и держит его НАСТОЯЩАЯ day_of,
# вырезанная из скрипта, а не пересказ её регекспа: разъедутся — упадёт тест,
# а не доставка.
# ------------------------------------------------------------------------

OFFLOAD_SH = Path(__file__).parent.parent / "deploy" / "offload.sh"


def offload_day_of(name: str) -> str | None:
    text = OFFLOAD_SH.read_text(encoding="utf-8")
    start = text.index("day_of() {")
    func = text[start:text.index("\n}", start) + 2]
    proc = subprocess.run(
        ["bash", "-c", func + '\nday_of "$1"', "day_of", name],
        capture_output=True, text=True, check=False)
    return proc.stdout.strip() if proc.returncode == 0 else None


@pytest.mark.parametrize("venue", sorted({leg.venue for leg in fp.LEGS}))
def test_every_data_file_name_is_recognised_and_dated_by_offload(tmp_path, venue):
    # Блокер панели #88: ряд, ради невосполнимости которого поллер написан,
    # обязан УЕЗЖАТЬ с хоста. offload.sh (единственный путь доставки; его же
    # оборачивает offload-daily.sh) признаёт имена функцией day_of; всё
    # нераспознанное он молча оставляет лежать вечно («unrecognised names: N,
    # left alone») — не доезжает до оператора И не удаляется с хоста, при
    # зелёном пульсе и молчащем TG. Прежнее имя <venue>-YYYYMMDD.jsonl.gz не
    # проходило: day_of требует подчёркивание перед датой и .gz СРАЗУ после.
    # Сегодняшний файл офлоад не трогает сам (day >= UTC-сегодня хоста), так
    # что распознаваемость не грозит файлу, в который ещё дописывают.
    host = fp.RealHost(tmp_path / "data", tmp_path / "pulse")
    host.append_jsonl(venue, "20260820", '{"x":1}')
    (written,) = (tmp_path / "data").iterdir()
    assert offload_day_of(written.name) == "20260820"


def test_real_host_appends_gzip_jsonl_per_venue_day(tmp_path):
    host = fp.RealHost(tmp_path / "data", tmp_path / "pulse")
    host.append_jsonl("aster", "20260820", '{"x":1}')
    host.append_jsonl("aster", "20260820", '{"x":2}')
    p = tmp_path / "data" / "funding_aster_20260820.gz"
    assert p.exists()
    with gzip.open(p, "rt", encoding="utf-8") as fh:
        assert fh.read() == '{"x":1}\n{"x":2}\n'


def _spy_fs(monkeypatch, watched_dir):
    """Подсматривает os.open/os.write/os.fsync по файлам внутри watched_dir.
    Фильтр по fd обязателен: os.* глобальны, и без него в события попадали бы
    чужие записи (capture самого pytest)."""
    events, fds = [], set()
    real = {"open": os.open, "write": os.write, "fsync": os.fsync}

    def spy_open(path, flags, *a, **kw):
        fd = real["open"](path, flags, *a, **kw)
        if str(path).startswith(str(watched_dir)):
            fds.add(fd)
        return fd

    def spy_write(fd, data):
        if fd in fds:
            events.append(("write", bytes(data)))
        return real["write"](fd, data)

    def spy_fsync(fd):
        if fd in fds:
            events.append(("fsync", b""))
        return real["fsync"](fd)

    monkeypatch.setattr(os, "open", spy_open)
    monkeypatch.setattr(os, "write", spy_write)
    monkeypatch.setattr(os, "fsync", spy_fsync)
    return events


def test_an_append_is_one_complete_gzip_member_in_a_single_write(tmp_path, monkeypatch):
    # Панель #88 (major): буферизованный gzip.open(.., 'at') писал член серией
    # write и закрывал его трейлером только на close. Убийство процесса посреди
    # дописывания (ребут, OOM, systemctl stop на 200-КБ записи paradex)
    # оставляло усечённый член, а усечённый член в СЕРЕДИНЕ файла делает
    # нечитаемым для gzip/zcat весь остаток суток площадки — до 288 записей,
    # не одну строку. И отказ невидим всем трём каналам тревоги: append к битому
    # хвосту продолжается, пульс зелёный, OnFailure молчит. Член обязан ложиться
    # на диск ПОЛНЫМ, одним os.write, и быть fsync-нут до возврата: окно
    # разрыва — один syscall вместо серии буферизованных write.
    events = _spy_fs(monkeypatch, tmp_path / "data")
    host = fp.RealHost(tmp_path / "data", tmp_path / "pulse")
    host.append_jsonl("paradex", "20260820", '{"x":1}')

    writes = [d for kind, d in events if kind == "write"]
    assert len(writes) == 1
    assert gzip.decompress(writes[0]) == b'{"x":1}\n'   # член полон уже в write
    assert [k for k, _ in events] == ["write", "fsync"]  # и на диске до возврата


def test_the_pulse_append_is_a_single_complete_member_too(tmp_path, monkeypatch):
    # Тот же обрыв в pulse-файле ослеплял бы разбор инцидента задним числом.
    events = _spy_fs(monkeypatch, tmp_path / "pulse")
    host = fp.RealHost(tmp_path / "data", tmp_path / "pulse",
                       _now_ns=lambda: 1_787_200_000_000_000_000)
    host.append_pulse('{"t":1}')
    writes = [d for kind, d in events if kind == "write"]
    assert len(writes) == 1
    assert gzip.decompress(writes[0]) == b'{"t":1}\n'


def test_ticks_after_a_torn_tail_are_recoverable_by_gzip_magic_resync(tmp_path):
    # Радиус поражения обрыва, честно: наивный читатель (gzip.open/zcat)
    # останавливается на рваном члене, но каждый наш член самодостаточен —
    # читалка с ресинком по магии 1f 8b 08 достаёт всё, что записано ПОСЛЕ
    # обрыва. Пин держит самодостаточность членов: без неё после разрыва
    # (например, от отказа питания ниже уровня syscall) спасать было бы нечего.
    host = fp.RealHost(tmp_path / "data", tmp_path / "pulse")
    host.append_jsonl("aster", "20260820", '{"tick":1}')
    (path,) = (tmp_path / "data").iterdir()
    torn = gzip.compress(b'{"tick":2}\n')
    with open(path, "ab") as fh:
        fh.write(torn[: len(torn) // 2])        # запись оборвана на середине
    host.append_jsonl("aster", "20260820", '{"tick":3}')

    raw = path.read_bytes()
    with pytest.raises((EOFError, zlib.error, gzip.BadGzipFile, OSError)):
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            fh.read()                            # наивный читатель падает
    last = raw.rindex(b"\x1f\x8b\x08")
    assert gzip.decompress(raw[last:]) == b'{"tick":3}\n'


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
