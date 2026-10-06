"""Офлайн-сценарии пробника на фейковой бирже (fake_hl_venue.py). Без сети, SDK и ключей —
только стандартная библиотека, Python ≥ 3.10.

  python3 -B docs/probes/test_hl_modify_probe.py              # все сценарии
  unshare -rn python3 -B docs/probes/test_hl_modify_probe.py  # то же — без сетевого стека
  (или pytest -p no:cacheprovider docs/probes/test_hl_modify_probe.py)

Каждый тест пиннит одно свойство пробника; в скобках — что он ловит.
"""
from __future__ import annotations

import importlib.util
import signal
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent


def _load(name: str, file: str):
    spec = importlib.util.spec_from_file_location(name, HERE / file)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


probe = _load("hl_modify_probe", "hl_modify_probe.py")
fake = _load("fake_hl_venue", "fake_hl_venue.py")

ALL_STEPS = [f"S{i}" for i in range(1, probe.PLAN_ACTIONS + 1)]


def by_step(outcome) -> dict:
    return {s["step"]: s for s in outcome.probe.steps}


def test_the_plan_runs_to_the_end_and_leaves_nothing_open():
    """(S1/S5 абортили «appeared unasked»: постановка не разрешала свой же cloid.)"""
    r = fake.run_offline(probe, "atomic", quiet=True)
    assert r.aborted is None, r.aborted
    assert [s["step"] for s in r.probe.steps] == ALL_STEPS
    assert r.cleanup["left"] == 0 and not r.venue.run_orders_open()
    assert r.venue.foreign_untouched(), "a foreign order on the account was touched"
    assert r.cleanup["other_open"] == len(fake.FOREIGN)


def test_the_action_cap_holds_with_cleanup():
    """(потолок — в запросах биржи, не в POST; пулы плана и очистки раздельные)"""
    r = fake.run_offline(probe, "atomic", quiet=True)
    plan = [p for p in r.venue.payloads if p["action"]["type"] != "cancel"]
    assert len(plan) == probe.PLAN_ACTIONS
    assert r.probe.client.n_actions == len(r.venue.payloads)
    spent = r.probe.client.budget.spent
    assert spent["plan"] == sum(probe.PLAN_COSTS.values()) <= probe.PLAN_BUDGET  # S6 = 2, S20 = 5
    assert 0 < spent["cleanup"] <= probe.CLEANUP_BUDGET


def test_expiry_is_signed_and_sent_only_where_the_plan_asks():
    """(Q12: expiresAfter в подписи = в теле; +10 с у S19/S21, −5 с у S20, у прочих — нет)"""
    r = fake.run_offline(probe, "atomic", quiet=True)
    assert len(r.venue.signed) == len(r.venue.payloads)
    for signed, payload in zip(r.venue.signed, r.venue.payloads):
        assert signed["expires_after"] == payload["expiresAfter"]
        assert signed["nonce"] == payload["nonce"]
    expected = {"S19": 10_000, "S20": -5_000, "S21": 10_000}
    for sid, step in by_step(r).items():
        if sid in expected:
            assert step["expires_after"] - step["t_sign_ms"] == expected[sid], sid
        else:
            assert step["expires_after"] is None, sid


def test_q12_and_q13_answers_on_a_venue_that_behaves_as_assumed():
    """(сводка: P1 подтверждена, ALO через спред отвергнута с живым ордером, P3, размеры замен)"""
    r = fake.run_offline(probe, "atomic", quiet=True)
    q12, q13 = r.summary["Q12_expires_after"], r.summary["Q13_alo_cross"]
    assert q12["future_accepted"] is True
    assert q12["past_refused"] is True and q12["past_order_untouched"] is True
    assert q13["refused"] is True and q13["old_order_untouched"] is True
    assert r.summary["P3_oid_monotonic"]["strictly_increasing"] is True
    sizes = r.summary["replacement_sizes"]
    assert sizes and all(x["equal"] for x in sizes), sizes


def test_a_venue_that_ignores_expiry_is_reported_not_hidden():
    """(P1 ложна → past_refused = False; план идёт дальше, всё в гардрейлах)"""
    r = fake.run_offline(probe, "expiry_ignored", quiet=True)
    assert r.aborted is None, r.aborted
    q12 = r.summary["Q12_expires_after"]
    assert q12["past_refused"] is False and q12["past_order_untouched"] is False
    assert r.cleanup["left"] == 0


def test_a_cross_refusal_that_kills_the_old_order_is_an_answer():
    """(Q13(Б): ордер исчез — разрешено, не аборт)"""
    r = fake.run_offline(probe, "cross_kills_old", quiet=True)
    assert r.aborted is None, r.aborted
    s21 = by_step(r)["S21"]
    assert s21["vanished"] and s21["shape"] == "statuses"
    q13 = r.summary["Q13_alo_cross"]
    assert q13["refused"] is True and q13["old_order_untouched"] is False
    assert r.cleanup["left"] == 0


def test_a_cross_that_rests_aborts_and_is_cleaned_up():
    """(Q13(Г): правка встала у тача — отказ обязателен, плюс гардрейл 5 % в observe; очистка)"""
    r = fake.run_offline(probe, "cross_rests", quiet=True)
    assert r.aborted and "obliged to refuse" in r.aborted, r.aborted
    s21 = r.probe.steps[-1]
    assert s21["step"] == "S21" and "breaks a guardrail" in s21["observe_abort"]
    assert r.cleanup["left"] == 0 and not r.venue.run_orders_open()


def test_a_cross_that_fills_aborts_and_is_cleaned_up():
    """(Q13(В): филл на правке — аборт; продаж пробник не делает, позиция остаётся)"""
    r = fake.run_offline(probe, "cross_fills", quiet=True)
    assert r.aborted and "FILLED" in r.aborted, r.aborted
    assert r.cleanup["left"] == 0 and not r.venue.run_orders_open()
    assert r.venue.position > 0 and all(f["side"] == "B" for f in r.venue.fills)
    assert all(p["action"]["type"] in ("order", "modify", "batchModify", "cancel", "cancelByCloid")
               for p in r.venue.payloads)


def test_a_lost_response_aborts_and_still_cleans_up():
    """(сбой транспорта после того, как биржа применила правку: аборт, очистка находит замену)"""
    def fail(payload):
        return "lost_response" if payload.get("expiresAfter") and payload["expiresAfter"] > fake.FakeVenue.now_ms() else None

    r = fake.run_offline(probe, "atomic", fail_on=fail, quiet=True)
    assert r.aborted and "transport failure" in r.aborted, r.aborted
    assert by_step(r)["S19"].get("transport_error")
    assert r.cleanup["left"] == 0 and not r.venue.run_orders_open()


def test_no_post_follows_a_redirect_and_every_one_stays_on_testnet():
    """(подписанное действие не уходит по редиректу; MX2: /info шёл по редиректам, а его список —
    sweepable очистки: ответ чужого хоста назначал бы цели отмены)"""
    r = fake.run_offline(probe, "atomic", quiet=True)
    kinds = {url.rsplit("/", 1)[-1] for url, _ in r.session.posts}
    assert kinds == {"info", "exchange"}, kinds
    assert all(allow is False for _, allow in r.session.posts), [p for p in r.session.posts if p[1] is not False]


def test_the_cross_wire_is_the_only_one_through_the_touch():
    """(исключение из 5 % — только S21)"""
    r = fake.run_offline(probe, "atomic", quiet=True)
    for step in r.probe.steps:
        action = step["action"]
        wires = action.get("orders") or [m["order"] for m in action.get("modifies", [])]
        if action["type"] == "modify":
            wires = [action["order"]]
        for w in wires:
            through = float(w["p"]) >= fake.BEST_ASK
            assert through == (step["step"] == "S21"), (step["step"], w["p"])


def test_preflight_refuses_a_book_where_q13_cannot_be_measured():
    """(префлайт: S21 за потолком гардрейла — отказ до любого действия)"""
    venue = fake.FakeVenue()
    session = fake.FakeSession(venue)
    client = probe.Client(fake.fake_sdk(venue, session), None, fake.MASTER, dry_run=True)
    pre = probe.preflight(client, fake.COIN, fake.AGENT)
    assert pre["asset"] == fake.ASSET and not venue.payloads
    real_ask = fake.BEST_ASK
    try:
        fake.BEST_ASK = 3 * fake.BEST_BID  # notional правки через спред > $15
        try:
            probe.preflight(client, fake.COIN, fake.AGENT)
        except probe.Abort:
            pass
        else:
            raise AssertionError("preflight accepted an infeasible Q13 step")
    finally:
        fake.BEST_ASK = real_ask
    assert not venue.payloads


def test_the_last_line_refuses_any_target_this_run_does_not_own():
    """(M1: цели правок и отмен проверял только act(); рубеж перед подписью — нет. Теперь
    assert_action_safe — единственное место, и оно само сверяет cancel со списком биржи. Раунд 3:
    свой oid из реестра отмену больше не разрешает — oid в реестр кладёт ответ биржи, а он может
    соврать; отменяется только то, что список биржи показывает под cloid формы пробника)"""
    led = probe.Ledger(probe.make_run_prefix(0x5EED))
    c = led.mint()
    led.note(c, 777)
    w = {"a": 3, "b": True, "p": "90", "s": "0.13", "r": False, "t": {"limit": {"tif": "Alo"}}, "c": c}
    foreign_cloid = "0xa1f0" + "0" * 28
    for good in ({"type": "modify", "oid": c, "order": w}, {"type": "modify", "oid": 777, "order": w},
                 {"type": "batchModify", "modifies": [{"oid": 777, "order": w}]}):
        probe.assert_action_safe(good, 3, led.owns)
    for bad in ({"type": "modify", "oid": 888, "order": w},
                {"type": "modify", "oid": foreign_cloid, "order": w},
                {"type": "batchModify", "modifies": [{"oid": c, "order": w}, {"oid": foreign_cloid, "order": w}]},
                {"type": "batchModify", "modifies": [{"oid": 888, "order": w}]},
                {"type": "cancel", "cancels": [{"a": 3, "o": 888}]},
                {"type": "cancel", "cancels": [{"a": 3, "o": 777}]}):  # свой oid, но список биржи не показан
        try:
            probe.assert_action_safe(bad, 3, led.owns)
        except probe.Guardrail:
            continue
        raise AssertionError(f"the last line signed {bad}")

    # cancel по oid — только если список биржи сам показывает его под cloid формы пробника на монете.
    venue = fake.FakeVenue()
    session = fake.FakeSession(venue)
    client = probe.Client(fake.fake_sdk(venue, session), None, fake.MASTER, dry_run=False)
    client.bind(3, led.owns, "BTC")
    other_run = probe.make_run_prefix(0xD00D) + "0" * 16 + "0001"
    listing = [
        {"coin": "BTC", "oid": 501, "cloid": other_run},       # хвост прошлого прогона — можно
        {"coin": "BTC", "oid": 502, "cloid": foreign_cloid},   # чужой префикс
        {"coin": "BTC", "oid": 503, "cloid": None},            # без cloid
        {"coin": "ETH", "oid": 504, "cloid": other_run},       # форма пробника, другая монета
        {"coin": "BTC", "oid": 777, "cloid": foreign_cloid},   # oid из реестра — под чужим cloid
        {"coin": "BTC", "oid": 505, "cloid": c},               # cloid прогона — можно
    ]
    for oid in (501, 505):
        client.exchange({"type": "cancel", "cancels": [{"a": 3, "o": oid}]}, pool="cleanup", cost=1, listing=listing)
    for oid in (502, 503, 504, 777):
        try:
            client.exchange({"type": "cancel", "cancels": [{"a": 3, "o": oid}]}, pool="cleanup", cost=1,
                            listing=listing)
        except probe.Guardrail:
            continue
        raise AssertionError(f"oid {oid} was cancelled")
    assert [p["action"]["cancels"][0]["o"] for p in venue.payloads] == [501, 505]


def _answer_with_oid(oid):
    """Врущая биржа: ответ на первую постановку (S1) несёт чужой oid и без cloid."""
    state = {"done": False}

    def rewrite(payload, body):
        if not state["done"] and payload["action"]["type"] == "order":
            state["done"] = True
            body["response"]["data"]["statuses"][0] = {"resting": {"oid": oid}}
        return body

    return rewrite


def test_an_answer_naming_a_foreign_order_stops_the_plan_and_that_order_is_never_touched():
    """(врущая биржа: ответ на S1 несёт oid чужого ордера; реестр брал его по позиции, очистка
    снимала этот чужой ордер, а аборт винил «префикс прогона, которого пробник не выпускал». Теперь
    план встаёт на S1 с настоящей причиной, очистка снимает только строки с cloid формы пробника,
    чужая строка — в отчёте `disowned`)"""
    for foreign in fake.FOREIGN:
        r = fake.run_offline(probe, "atomic", rewrite=_answer_with_oid(foreign["oid"]), quiet=True)
        assert r.aborted and r.aborted.startswith("Abort: S1: ") and f"oid {foreign['oid']}" in r.aborted, r.aborted
        where = f"under cloid {foreign['cloid']}" if foreign["cloid"] else "without a cloid"
        assert where in r.aborted and "never minted" not in r.aborted, r.aborted
        assert [s["step"] for s in r.probe.steps] == ["S1"]
        assert r.venue.foreign_untouched(), "the cleanup cancelled a foreign order"
        assert r.cleanup["left"] == 0 and not r.venue.run_orders_open(), r.cleanup
        assert [d["oid"] for d in r.cleanup["disowned"]] == [foreign["oid"]], r.cleanup
        cancelled = [c["o"] for p in r.venue.payloads if p["action"]["type"] == "cancel" for c in p["action"]["cancels"]]
        assert cancelled and foreign["oid"] not in cancelled, cancelled


def test_a_sweep_acts_only_on_rows_with_a_probe_cloid():
    """(очистка брала целью любой oid из реестра — и чужой ордер под чужим cloid, и ордер без cloid,
    если ответ биржи приписал его oid прогону. Цель — только строка с cloid формы пробника; прочие с
    oid из реестра — в отчёт `disowned`, без отмены)"""
    rows = [dict(PROBE_ROW), {"coin": "BTC", "oid": 701, "cloid": "0xa1f0" + "1" * 28},
            {"coin": "BTC", "oid": 702, "cloid": None}, {"coin": "BTC", "oid": 703},
            {"coin": "ETH", "oid": 704, "cloid": None}]
    sent = []

    def exchange(action, **_kw):
        sent.append(action)
        gone = {c["o"] for c in action["cancels"]}
        rows[:] = [r for r in rows if r["oid"] not in gone]
        return {"http_status": 200}

    client = _StubClient(lambda _b: [dict(r) for r in rows], exchange)
    rep = probe.sweep(client, fake.MASTER, "BTC", 3, {601, 701, 702, 703, 704}, pool="cleanup", timing=TINY,
                      label="t")
    assert [c["o"] for a in sent for c in a["cancels"]] == [PROBE_ROW["oid"]], sent
    assert rep["left"] == 0 and rep["errors"] == [], rep
    assert [d["oid"] for d in rep["disowned"]] == [701, 702, 703], rep  # ETH — не монета пробника


def test_the_budget_is_charged_before_anything_is_signed():
    """(MX1: списание после POST — действие сверх потолка подписано и ушло на биржу, а гардрейл
    падал уже потом; после доначисления по замеру так уходил S21)"""
    venue = fake.FakeVenue()
    session = fake.FakeSession(venue)
    client = probe.Client(fake.fake_sdk(venue, session), None, fake.MASTER, dry_run=False,
                          budget=probe.Budget({"plan": 2, "cleanup": 0, "sweep": 0}))
    led = probe.Ledger(probe.make_run_prefix(0xB0D6E7))
    client.bind(fake.ASSET, led.owns, fake.COIN)

    def order():
        w = {"a": fake.ASSET, "b": True, "p": "90000", "s": "0.00013", "r": False,
             "t": {"limit": {"tif": "Alo"}}, "c": led.mint()}
        return {"type": "order", "orders": [w], "grouping": "na"}

    client.exchange(order(), pool="plan", cost=1)
    client.budget.true_up("plan", 1)  # биржа списала на 1 больше предсказанного — пул исчерпан
    try:
        client.exchange(order(), pool="plan", cost=1)
    except probe.Guardrail as error:
        assert "plan budget" in str(error), error
    else:
        raise AssertionError("an action over the cap was accepted")
    assert len(venue.signed) == 1 and len(venue.payloads) == 1, "an action over the cap was signed or sent"


def _sdk(version, anchor_hex=probe.ORDER_MSGPACK_ANCHOR):
    """SDK-заглушка: воспроизводит (или нет) golden ордера; версия — любая."""
    return SimpleNamespace(
        version=version,
        Cloid=SimpleNamespace(from_str=lambda s: s),
        order_request_to_order_wire=lambda order, asset: {"order": order, "asset": asset},
        order_wires_to_order_action=lambda wires: {"type": "order", "orders": wires},
        msgpack=SimpleNamespace(packb=lambda _action: bytes.fromhex(anchor_hex)),
    )


def test_only_the_pinned_sdk_is_accepted():
    """(MX3: пин hyperliquid-python-sdk==0.24.0 не держал ни один тест — проверку версии можно было
    снять молча; другая версия, даже воспроизводящая golden, — отказ)"""
    probe.check_sdk(_sdk(probe.SDK_PIN))
    for version in ("0.23.0", "0.24.1", "0.24", "unknown", "fake"):
        try:
            probe.check_sdk(_sdk(version))
        except probe.Refuse as error:
            assert f"pinned to {probe.SDK_PIN}" in str(error), error
        else:
            raise AssertionError(f"SDK {version} was accepted")
    try:
        probe.check_sdk(_sdk(probe.SDK_PIN, anchor_hex="c0"))
    except probe.Refuse as error:
        assert "golden" in str(error), error
    else:
        raise AssertionError("an SDK that does not reproduce the exchange.rs golden was accepted")


# Сигналы, которые по умолчанию завершают процесс и приходят снаружи (signal(7)): каждый обязан
# пройти через очистку. Мимо неё — только SIGKILL и сигналы аварии самого интерпретатора.
ENDING_SIGNALS = [n for n in ("SIGINT", "SIGTERM", "SIGHUP", "SIGQUIT", "SIGUSR1", "SIGUSR2", "SIGALRM",
                              "SIGVTALRM", "SIGPROF", "SIGIO", "SIGPWR", "SIGXCPU", "SIGXFSZ") if hasattr(signal, n)]


def test_every_signal_that_would_end_the_run_stops_it_for_the_cleanup():
    """(Stopper ловил четыре сигнала: SIGUSR1/2, SIGALRM, SIGPWR, SIGXCPU/SIGXFSZ от rlimit и
    прочие убивали пробник мимо очистки, а нота обещала «без очистки — только kill -9»; отдельный
    процесс — неперехваченный сигнал убил бы сам pytest)"""
    code = (
        "import os, resource, signal, sys, time\n"
        "resource.setrlimit(resource.RLIMIT_CORE, (0, 0))\n"
        f"sys.path.insert(0, {str(HERE)!r})\n"
        "import hl_modify_probe as hp\n"
        "for name in sys.argv[1:]:\n"
        "    stopper = hp.Stopper()\n"
        "    stopper.install()\n"
        "    os.kill(os.getpid(), getattr(signal, name))\n"
        "    deadline = time.monotonic() + 5\n"
        "    while stopper.reason is None and time.monotonic() < deadline:\n"
        "        time.sleep(0.001)\n"
        "    print(name, stopper.reason, flush=True)\n"
    )
    proc = subprocess.run([sys.executable, "-I", "-S", "-B", "-c", code, *ENDING_SIGNALS],
                          capture_output=True, text=True, timeout=60)
    assert proc.returncode == 0, f"a signal ended the probe past its Stopper: rc={proc.returncode}, {proc.stdout!r}"
    assert proc.stdout.splitlines() == [f"{n} {n}" for n in ENDING_SIGNALS], proc.stdout


class _StubClient:
    """Клиент для sweep(): /info и /exchange — функции теста; бюджет — настоящий."""

    def __init__(self, info, exchange=None):
        self._info, self._exchange = info, exchange
        self.budget = probe.Budget()
        self.sent = []

    def info(self, body, **_kw):
        return self._info(body)

    def exchange(self, action, **kw):
        self.sent.append(action)
        return self._exchange(action, **kw) if self._exchange else {"http_status": 200}


TINY = probe.Timing(0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 30.0, 0)
PROBE_ROW = {"coin": "BTC", "oid": 601, "cloid": probe.make_run_prefix(0xD00D) + "0" * 16 + "0002"}


def test_an_open_order_list_of_the_wrong_shape_is_never_read_as_clean():
    """({} давал ложное «чисто», {"error": …} и false роняли очистку мимо отчёта)"""
    for payload in ({}, {"error": "maintenance"}, False, None, [{"coin": "BTC", "oid": "601"}]):
        client = _StubClient(lambda _b, p=payload: p)
        rep = probe.sweep(client, fake.MASTER, "BTC", 3, set(), pool="cleanup", timing=TINY, label="t")
        assert rep["left"] is None, (payload, rep)
        assert len(rep["errors"]) == 6 and all("frontendOpenOrders" in e for e in rep["errors"]), rep
        assert client.sent == [], "a cancel went out on a list the probe could not read"


def test_a_sweep_never_raises_and_reports_what_it_could_not_do():
    """(любое исключение в очистке — отчёт и код 4, а не трейсбек из свёртки)"""
    def boom(_body):
        raise RuntimeError("boom")

    rep = probe.sweep(_StubClient(boom), fake.MASTER, "BTC", 3, set(), pool="cleanup", timing=TINY, label="t")
    assert rep["left"] is None and any("boom" in e for e in rep["errors"]), rep

    def refuse(_action, **_kw):
        raise RuntimeError("signer gone")

    rep = probe.sweep(_StubClient(lambda _b: [PROBE_ROW], refuse), fake.MASTER, "BTC", 3, set(), pool="cleanup",
                      timing=TINY, label="t")
    assert rep["left"] == 1 and any("signer gone" in e for e in rep["errors"]), rep


def test_an_expiry_refusal_that_mentions_nonce_is_flagged_not_fatal():
    """(широкое «nonce» в ABORT_TEXTS обрывало план на неизмеренном отказе S20 — до Q13)"""
    r = fake.run_offline(probe, "expiry_mentions_nonce", quiet=True)
    assert r.aborted is None, r.aborted
    steps = by_step(r)
    assert list(steps) == ALL_STEPS
    assert steps["S20"]["text_flags"] == ["nonce"] and r.summary["Q12_expires_after"]["S20"]["text_flags"] == ["nonce"]
    assert r.summary["Q13_alo_cross"]["refused"] is True
    assert "?? the answer mentions nonce" in r.stdout


def test_the_account_stream_takes_only_a_101_from_its_own_host():
    """(websocket-client шёл по редиректам рукопожатия; 1.9.0 на redirect_limit=0 «подключается»
    с ответом 307 — поэтому ещё и статус)"""
    calls = []

    class _Ws:
        def __init__(self, status):
            self.status, self.closed, self.sent = status, False, []

        def getstatus(self):
            return self.status

        def close(self):
            self.closed = True

        def send(self, text):
            self.sent.append(text)

        def settimeout(self, _t):
            pass

        def recv(self):
            raise ConnectionError("stub")

    class _Module:
        WebSocketTimeoutException = TimeoutError

        def __init__(self, status):
            self.status = status

        def create_connection(self, url, **kw):
            calls.append((url, kw))
            self.ws = _Ws(self.status)
            return self.ws

    venue = probe.fake_venue(18545)
    module = _Module(307)
    try:
        probe.Stream(module, fake.MASTER, venue)
    except probe.Refuse as error:
        assert "307" in str(error)
    else:
        raise AssertionError("a 307 handshake was taken for a stream")
    assert module.ws.closed and module.ws.sent == [], "subscribed on a redirect"
    assert calls[-1] == (venue.ws, {"timeout": 10, "redirect_limit": 0}), calls


def test_the_self_test_needs_nothing_but_the_standard_library():
    """(нота §9 звала `$R … --self-test` «офлайн», а uv тянул SDK из PyPI; tomllib/tomli
    импортировался на верхнем уровне и валил Python 3.10 без tomli)"""
    import subprocess

    proc = subprocess.run([sys.executable, "-I", "-S", "-B", str(HERE / "hl_modify_probe.py"), "--self-test"],
                          capture_output=True, text=True, timeout=300)
    assert proc.returncode == 0 and "self-test ok" in proc.stdout, proc.stderr[-2000:]


def test_an_unencodable_line_does_not_take_the_terminal_away():
    """(ошибка кодировки — ValueError, а не «терминал пропал»: say() не уводит здоровый stdout в
    лог и не бросает; отдельный процесс — перевод fd в тесте увёл бы вывод самого pytest)"""
    import subprocess

    code = (
        "import io, os, sys\n"
        f"sys.path.insert(0, {str(HERE)!r})\n"
        "import hl_modify_probe as hp\n"
        "sys.stdout = io.TextIOWrapper(io.BytesIO(), encoding='ascii')\n"
        "hp.say('\\u2014 not ascii')\n"
        "os.write(1, b'fd1-still-here\\n')\n"
    )
    proc = subprocess.run([sys.executable, "-I", "-S", "-B", "-c", code], capture_output=True, text=True, timeout=60)
    assert proc.returncode == 0 and "fd1-still-here" in proc.stdout, (proc.stdout, proc.stderr)


def test_the_self_test_passes():
    assert probe.self_test() == 0


def main() -> int:
    tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)]
    failed = 0
    for name, fn in tests:
        try:
            fn()
        except Exception as error:  # noqa: BLE001
            failed += 1
            print(f"FAIL {name}: {type(error).__name__}: {error}")
        else:
            print(f"ok   {name}")
    print(f"{len(tests) - failed}/{len(tests)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
