"""Сквозные тесты пробника: настоящий процесс (`--fake-venue`) против fake_hl_server.py.

HTTP + WS на 127.0.0.1, подпись каждого действия проверяется SDK (подписант — одобренный агент,
источник «b» — тестнет). Весь план S1–S21, варианты поведения биржи, сбои транспорта и /info (в
том числе ответы не той формы), сигналы, обрыв терминала (настоящий pty), редиректы /exchange,
/info и WS, филлы (между шагами и в окне шага), врущий ответ биржи (чужой oid), потолок бюджета до
подписи, пин SDK. В конце КАЖДОГО сценария (если надо — после --sweep-only): на бирже нет ни одного
ордера пробника на монете, чужие ордера (другой префикс, без cloid, форма пробника на другой
монете, префикс 0dd1 без формы) не тронуты, хвост прошлого прогона снят.

Нужен hyperliquid-python-sdk==0.24.0 и pytest; сеть не нужна. Гонять так:
  unshare -rn sh -c 'ip link set lo up && HL_PROBE_REQUIRE_NETNS=1 python -B -m pytest -q -p no:cacheprovider docs/probes'
С HL_PROBE_REQUIRE_NETNS=1 тесты сами проверяют, что в пространстве имён нет ничего, кроме lo.
"""
from __future__ import annotations

import fcntl
import json
import os
import pty
import select
import signal
import socket
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

import pytest

pytest.importorskip("hyperliquid", reason="hyperliquid-python-sdk==0.24.0 is needed to sign and verify")

HERE = Path(__file__).resolve().parent
PROBE = HERE / "hl_modify_probe.py"
sys.path.insert(0, str(HERE))

import fake_hl_server as fh  # noqa: E402
import hl_modify_probe as hp  # noqa: E402

LEFTOVER = "0x0dd1deadbeef" + "0" * 16 + "0002"  # прошлый прогон: снять в начале
FOREIGN = {
    "a1f0_btc": ("BTC", "70000", "0.0002", "0xa1f0" + "1" * 28),
    "no_cloid_btc": ("BTC", "69000", "0.0002", None),
    "probe_shape_eth": ("ETH", "3000", "0.004", "0x0dd1cafebabe" + "0" * 16 + "0001"),
    "prefix_not_shape_btc": ("BTC", "68000", "0.0002", "0x0dd1" + "f" * 28),
}


@pytest.fixture(scope="session", autouse=True)
def offline_only():
    if os.environ.get("HL_PROBE_REQUIRE_NETNS") == "1":
        ifaces = {name for _, name in socket.if_nameindex()}  # интерфейсы ЭТОГО netns
        assert ifaces == {"lo"}, f"not an isolated network namespace: {ifaces}"


@pytest.fixture
def make_fake():
    fakes = []

    def make(profile: fh.Profile | None = None, *, leftovers=(LEFTOVER,)):
        fake = fh.FakeHL(profile or fh.Profile())
        fake.foreign = {name: fake.seed_order(coin, px, sz, cloid) for name, (coin, px, sz, cloid) in FOREIGN.items()}
        fake.leftovers = [fake.seed_order("BTC", "71000", "0.0002", c) for c in leftovers]
        fakes.append(fake.start())
        return fake

    yield make
    for fake in fakes:
        fake.stop()


def probe_cmd(fake, tmp_path, *extra, out=None):
    out = out or tmp_path / "out"
    return [sys.executable, "-B", str(PROBE), "--fake-venue", str(fake.port), "--state-dir", str(tmp_path / "state"),
            "--out", str(out), *extra]


def read_result(out):
    results = sorted(out.glob("*_hl_modify_probe_*.json")) if out.is_dir() else []
    return json.loads(results[-1].read_text()) if results else None


def run_probe(fake, tmp_path, *extra, timeout=240, out=None):
    out = out or tmp_path / "out"
    proc = subprocess.Popen(probe_cmd(fake, tmp_path, *extra, out=out), stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True, env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"})
    fake.probe_pid = proc.pid
    stdout, stderr = proc.communicate(timeout=timeout)
    return proc.returncode, stdout, stderr, read_result(out)


def run_probe_on_a_terminal_that_hangs_up(fake, tmp_path, after: bytes, timeout=240):
    """Пробник на настоящем pty, лидер своей сессии. Увидев `after`, «терминал» (master pty)
    закрывается — как при обрыве SSH: SIGHUP сессии, дальше любой write в tty даёт EIO."""
    pid, master = pty.fork()
    if pid == 0:  # ребёнок: pty — его управляющий терминал
        try:
            os.execv(sys.executable, probe_cmd(fake, tmp_path))
        finally:
            os._exit(127)
    fake.probe_pid = pid
    seen = b""
    deadline = time.monotonic() + timeout
    while after not in seen and time.monotonic() < deadline:
        ready, _, _ = select.select([master], [], [], 0.5)
        if not ready:
            continue
        try:
            chunk = os.read(master, 4096)
        except OSError:
            break
        if not chunk:
            break
        seen += chunk
    os.close(master)
    while time.monotonic() < deadline:
        done, status = os.waitpid(pid, os.WNOHANG)
        if done:
            return os.waitstatus_to_exitcode(status), seen, read_result(tmp_path / "out")
        time.sleep(0.05)
    os.kill(pid, signal.SIGKILL)
    os.waitpid(pid, 0)
    pytest.fail(f"the probe did not exit after the hangup; it printed {seen[-300:]!r}")


def probe_orders_open(fake) -> list:
    return [o for o in fake.open_orders() if o.coin == "BTC" and hp.is_probe_cloid(o.cloid)]


def assert_clean(fake):
    """Ни одного ордера пробника на монете; чужие на месте как были; хвост прошлого снят."""
    assert probe_orders_open(fake) == [], [o.wire(True) for o in probe_orders_open(fake)]
    open_oids = {o.oid for o in fake.open_orders()}
    for name, oid in fake.foreign.items():
        assert oid in open_oids, f"foreign order {name} was touched"
        coin, px, sz, cloid = FOREIGN[name]
        o = fake.orders[oid]
        assert (str(o.limit_px), str(o.sz), o.cloid) == (px, sz, cloid), name
    for oid in fake.leftovers:
        assert oid not in open_oids, "a leftover probe order of a previous run survived"


def assert_wire_discipline(fake):
    """Каждое действие подписано одобренным агентом для тестнета; типы — только разрешённые; ни
    одно действие не целится в чужой ордер."""
    foreign_oids = set(fake.foreign.values())
    for entry in fake.exchange_log:
        assert entry["signer"] == fh.FAKE_AGENT, entry["response"]
        action = entry["action"]
        assert action["type"] in hp.ALLOWED_ACTIONS, action["type"]
        if action["type"] == "cancel":
            assert not {c["o"] for c in action["cancels"]} & foreign_oids
        for wire in hp.action_wires(action):
            assert wire["b"] is True and wire["t"] == {"limit": {"tif": "Alo"}} and hp.is_probe_cloid(wire["c"])
        for target in ([action["oid"]] if action["type"] == "modify" else
                       [m["oid"] for m in action.get("modifies", [])]):
            assert target not in foreign_oids and (isinstance(target, int) or hp.is_probe_cloid(target))


class PlanHook:
    """on_exchange: номер шага плана = число не-`cancel` POST (добор и очистка шлют `cancel`)."""

    def __init__(self, at_step: int, action=None, phase="before", info_after=None):
        self.at_step, self.action, self.phase, self.info_after = at_step, action, phase, info_after
        self.step = 0
        self.fired = False
        self.lock = threading.Lock()

    def __call__(self, ctx):
        with self.lock:
            if ctx.phase == "before" and ctx.kind != "cancel":
                self.step += 1
            if not self.fired and self.step == self.at_step and ctx.phase == self.phase and ctx.kind != "cancel":
                self.fired = True
                return self.action(ctx) if self.action else None
        return None


def steps_of(result) -> list[str]:
    return [s["step"] for s in result["steps"]]


def assert_every_frame_seen_once(fake, result):
    """Курсор: каждая запись orderUpdates по ордерам прогона, отправленная биржей, учтена
    пробником ровно один раз — в окне шага, поздней у прошлого шага или в рамках до/после плана."""
    prefix = result["meta"]["run_prefix"]
    pushed = sorted(
        (e["order"]["oid"], e["status"])
        for channel, data in fake.push_log if channel == "orderUpdates"
        for e in data if str(e["order"].get("cloid") or "").startswith(prefix)
    )
    seen = []
    for step in result["steps"]:
        for e in step.get("ws_entries", []) + step.get("late_ws_entries", []):
            seen.append((e["oid"], e["status"]))
    for frame in result["frames_before_plan"] + result["frames_after_plan"]:
        msg = json.loads(frame["text"])
        if msg.get("channel") == "orderUpdates":
            seen += [(e["order"]["oid"], e["status"]) for e in msg["data"]
                     if str(e["order"].get("cloid") or "").startswith(prefix)]
    assert sorted(seen) == pushed, (len(seen), len(pushed))


def assert_book_read_right_before_each_post(fake, max_gap_s=None):
    """Перед каждым POST плана последнее чтение l2Book — после userRateLimit шага (после паузы
    бюджета); max_gap_s — и не раньше чем за столько до POST (пауза сверх капа — 1.1 с)."""
    last_book = last_budget = None
    for i, (kind, what, t) in enumerate(fake.timeline):
        if kind == "info" and what == "l2Book":
            last_book = (i, t)
        elif kind == "info" and what == "userRateLimit":
            last_budget = (i, t)
        elif kind == "exchange" and what != "cancel":
            assert last_book and last_budget and last_book[0] > last_budget[0], f"POST #{i}: book read before the wait"
            if max_gap_s is not None:
                assert t - last_book[1] < max_gap_s, f"POST #{i}: the book is {t - last_book[1]:.2f} s old"
            last_book = last_budget = None


# --- Полный план ------------------------------------------------------------------------------


def test_the_whole_plan_runs_and_leaves_nothing(make_fake, tmp_path):
    fake = make_fake()
    rc, out, err, result = run_probe(fake, tmp_path)
    assert rc == 0, err
    assert steps_of(result) == [f"S{i}" for i in range(1, 22)]
    meta, summary = result["meta"], result["summary"]
    assert meta["aborted"] is None and meta["cleanup"]["left"] == 0
    assert meta["start_sweep"]["cancels"][0]["oids"] == fake.leftovers
    assert meta["final_check"]["probe_fills"] == [] and meta["final_check"]["position_changed"] is False
    q12, q13 = summary["Q12_expires_after"], summary["Q13_alo_cross"]
    assert q12["future_accepted"] is True and q12["past_refused"] is True and q12["past_order_untouched"] is True
    assert q12["past_charged_d_used"] == hp.EXPIRED_COST  # фейк списывает 5×, как в документации
    assert q13["refused"] is True and q13["old_order_untouched"] is True
    assert q13["old_order"]["resting_same_oid"] is True
    assert summary["P3_oid_monotonic"]["strictly_increasing"] is True
    assert summary["replacement_sizes"] and all(r["equal"] for r in summary["replacement_sizes"])
    assert meta["venue_requests_spent"]["plan"] <= hp.PLAN_BUDGET
    # Q2: у каждой правки есть записи старого и нового oid в потоке.
    for sid in ("S3", "S4", "S6"):
        statuses = {e[5] for e in summary["Q2_Q3_Q4_modify_streams"][sid]["ws"]}
        assert {"open", "canceled"} <= statuses, (sid, statuses)
    assert "--- summary ---" in out and "P3" in out
    assert_clean(fake)
    assert_wire_discipline(fake)
    assert_every_frame_seen_once(fake, result)
    assert_book_read_right_before_each_post(fake)


@pytest.mark.parametrize(
    "profile",
    [
        fh.Profile(stream_order="new_old_one_frame"),
        fh.Profile(stream_order="old_new_two_frames"),
        fh.Profile(stream_order="new_old_two_frames"),
        fh.Profile(stream_order="old_only"),
        fh.Profile(stream_order="none"),
        fh.Profile(q5_keeps_oid=True),
        fh.Profile(single_modify_refusal="statuses"),
        fh.Profile(new_cloid="refuse"),
        fh.Profile(new_cloid="keep_old"),
        fh.Profile(expired_cost=1, expired_refusal="statuses"),
        fh.Profile(order_status_by_cloid="latest"),
    ],
    ids=lambda p: ",".join(f"{k}={v}" for k, v in vars(p).items() if v != getattr(fh.Profile(), k)),
)
def test_unmeasured_venue_behaviour_is_recorded_not_fatal(make_fake, tmp_path, profile):
    fake = make_fake(profile)
    rc, _, err, result = run_probe(fake, tmp_path)
    assert rc == 0, err
    assert steps_of(result) == [f"S{i}" for i in range(1, 22)]
    assert_clean(fake)
    assert_wire_discipline(fake)


def test_non_atomic_refusals_are_answers(make_fake, tmp_path):
    """Q10/Q13(Б): отказ правки снимает старый ордер — ответ, а не аборт; S16/S17 пропущены."""
    fake = make_fake(fh.Profile(atomic_refusal=False))
    rc, _, err, result = run_probe(fake, tmp_path)
    assert rc == 0, err
    skipped = {s["step"]: s["skipped"] for s in result["steps"] if "skipped" in s}
    assert set(skipped) == {"S16", "S17"}, skipped
    q13 = result["summary"]["Q13_alo_cross"]
    assert q13["refused"] is True and q13["old_order_untouched"] is False
    assert q13["old_order"]["order_status_of_old_oid"]["status"] == "canceled"
    assert_clean(fake)


def test_over_the_cap_the_plan_is_paced_not_refused(make_fake, tmp_path):
    fake = make_fake(fh.Profile(over_cap=True, throttle_s=1.0))
    rc, _, err, result = run_probe(fake, tmp_path, timeout=400)
    assert rc == 0, err
    assert result["meta"]["over_cap_any"] is True
    assert not any("Too many" in json.dumps(e["response"]) for e in fake.exchange_log)
    assert_book_read_right_before_each_post(fake, max_gap_s=0.3)
    assert_clean(fake)


# --- Сбои ------------------------------------------------------------------------------------


@pytest.mark.parametrize("phase", ["before", "after"])
def test_a_transport_failure_mid_plan_aborts_and_cleans_up(make_fake, tmp_path, phase):
    """after — биржа применила правку двух ордеров, ответ потерян: замены знает только очистка."""
    fake = make_fake()
    fake.on_exchange = PlanHook(6, lambda ctx: "drop", phase=phase)
    rc, _, err, result = run_probe(fake, tmp_path)
    assert rc == 2, err
    assert "S6: transport failure" in result["meta"]["aborted"]
    assert steps_of(result)[-1] == "S6"
    assert_clean(fake)
    assert_wire_discipline(fake)


def test_a_redirect_is_never_followed(make_fake, tmp_path):
    hits = []

    class Trap(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802
            hits.append(self.path)
            self.send_response(200)
            self.end_headers()

        def log_message(self, *a):
            pass

    trap = ThreadingHTTPServer(("127.0.0.1", 0), Trap)
    threading.Thread(target=trap.serve_forever, daemon=True).start()
    fake = make_fake()

    def redirect(ctx):
        return ("redirect", f"http://127.0.0.1:{trap.server_address[1]}/exchange")

    fake.on_exchange = PlanHook(2, redirect)
    try:
        rc, _, err, result = run_probe(fake, tmp_path)
    finally:
        trap.shutdown()
    assert rc == 2 and "S2: HTTP 307" in result["meta"]["aborted"], err
    assert hits == [], "the signed action followed a redirect"
    assert_clean(fake)


@pytest.mark.parametrize(
    "sig", [signal.SIGTERM, signal.SIGHUP, signal.SIGINT, signal.SIGQUIT, signal.SIGUSR1, signal.SIGALRM]
)
def test_a_signal_mid_plan_stops_and_cleans_up(make_fake, tmp_path, sig):
    fake = make_fake()
    fake.on_exchange = PlanHook(9, lambda ctx: os.kill(ctx.fake.probe_pid, sig))
    rc, _, err, result = run_probe(fake, tmp_path)
    assert rc == 2, err
    assert result["meta"]["aborted"] == f"Interrupted: stopped by {sig.name}"
    assert steps_of(result)[-1] == "S9"
    assert_clean(fake)


def test_signals_during_cleanup_do_not_stop_it_and_info_failures_are_retried(make_fake, tmp_path):
    """SIGTERM посреди плана; во время очистки — ещё SIGINT и SIGTERM, а /info трижды падает."""
    fake = make_fake()
    stopped = threading.Event()

    def term(ctx):
        os.kill(ctx.fake.probe_pid, signal.SIGTERM)
        stopped.set()

    failures = iter(["http500", "drop", "http500"])
    sent = []

    def info(ctx):
        if not stopped.is_set() or ctx.kind != "frontendOpenOrders":
            return None
        if not sent:
            for s in (signal.SIGINT, signal.SIGTERM):
                os.kill(ctx.fake.probe_pid, s)
                sent.append(s)
        return next(failures, None)

    fake.on_exchange = PlanHook(12, term)
    fake.on_info = info
    rc, _, err, result = run_probe(fake, tmp_path)
    assert rc == 2, err
    cleanup = result["meta"]["cleanup"]
    assert cleanup["left"] == 0 and len(cleanup["errors"]) == 3, cleanup
    assert sorted(result["meta"]["signals_ignored_during_cleanup"]) == ["SIGINT", "SIGTERM"]
    assert "ignored: cleanup of probe orders is running" in err
    assert_clean(fake)


def test_cleanup_that_cannot_verify_says_so_and_sweep_only_finishes_it(make_fake, tmp_path):
    fake = make_fake()
    stopped = threading.Event()

    def term(ctx):
        os.kill(ctx.fake.probe_pid, signal.SIGTERM)
        stopped.set()

    fake.on_exchange = PlanHook(6, term)
    fake.on_info = lambda ctx: "drop" if stopped.is_set() and ctx.kind == "frontendOpenOrders" else None
    rc, _, err, result = run_probe(fake, tmp_path, timeout=300)
    assert rc == 4 and "NOT CLEAN" in err, err
    assert result["meta"]["cleanup"]["left"] is None
    assert probe_orders_open(fake), "the scenario needs live probe orders"
    fake.on_info = None
    fake.on_exchange = None
    rc, out, err, _ = run_probe(fake, tmp_path / "sweep", "--sweep-only")
    assert rc == 0, err
    assert_clean(fake)


def stop_at(step: int):
    """on_exchange: SIGTERM пробнику на POST шага `step`; Event — стоп отправлен."""
    stopped = threading.Event()

    def term(ctx):
        os.kill(ctx.fake.probe_pid, signal.SIGTERM)
        stopped.set()

    return PlanHook(step, term), stopped


@pytest.mark.parametrize(
    "payload",
    [b"{}", b'{"error": "maintenance"}', b"false", b"null", b'[{"coin": "BTC", "oid": "61934000005"}]'],
    ids=["empty_object", "error_object", "false", "null", "string_oid"],
)
def test_an_open_order_list_of_the_wrong_shape_in_cleanup_is_never_clean(make_fake, tmp_path, payload):
    """frontendOpenOrders в очистке отвечает 200, но не списком ордеров: {} давал ложное «чисто»
    (код 2, ордера на бирже), {"error"}/false роняли свёртку (код 1, без результатов). Теперь —
    код 4, NOT CLEAN, результаты и итоговая сверка есть; --sweep-only добирает."""
    fake = make_fake()
    fake.on_exchange, stopped = stop_at(6)
    fake.on_info = lambda ctx: ("body", payload) if stopped.is_set() and ctx.kind == "frontendOpenOrders" else None
    rc, _, err, result = run_probe(fake, tmp_path)
    assert rc == 4 and "NOT CLEAN" in err, err
    meta = result["meta"]
    assert meta["cleanup"]["left"] is None and meta["cleanup"]["cancels"] == []
    assert meta["cleanup"]["errors"] and all("frontendOpenOrders" in e for e in meta["cleanup"]["errors"])
    assert meta["final_check"]["position_changed"] is False and meta["wind_down_failures"] == []
    assert probe_orders_open(fake), "the scenario needs live probe orders"
    fake.on_info = fake.on_exchange = None
    rc, _, err, _ = run_probe(fake, tmp_path / "sweep", "--sweep-only")
    assert rc == 0, err
    assert_clean(fake)


def test_an_open_order_list_of_the_wrong_shape_mid_plan_aborts_instead_of_being_read(make_fake, tmp_path):
    """`{}` в окне шага читался бы как «ордеров нет»: при may_kill — ложный ответ «ордер снят»."""
    fake = make_fake()
    state = {"posts": 0, "served": False}

    def on_exchange(ctx):
        if ctx.phase == "after" and ctx.kind != "cancel":
            state["posts"] += 1

    def on_info(ctx):
        if state["posts"] == 3 and not state["served"] and ctx.kind == "frontendOpenOrders":
            state["served"] = True
            return ("body", b"{}")
        return None

    fake.on_exchange, fake.on_info = on_exchange, on_info
    rc, _, err, result = run_probe(fake, tmp_path)
    assert rc == 2, err
    assert state["served"] and steps_of(result) == ["S1", "S2", "S3"]
    assert "frontendOpenOrders: not a list" in result["steps"][-1]["observe_abort"], result["steps"][-1]
    assert "frontendOpenOrders: not a list" in result["meta"]["aborted"]
    assert result["meta"]["cleanup"]["left"] == 0
    assert_clean(fake)


def test_a_final_check_that_cannot_read_fills_does_not_lose_the_results(make_fake, tmp_path):
    """Сбой шага свёртки (userFills не той формы) не отменяет остальных: очистка проверена,
    результаты записаны, код 2 — а не трейсбек из finally и код 1."""
    fake = make_fake()
    fake.on_info = lambda ctx: ("body", b'{"error": "x"}') if ctx.kind == "userFills" else None
    rc, _, err, result = run_probe(fake, tmp_path)
    assert rc == 2, err
    meta = result["meta"]
    assert meta["aborted"] is None and meta["cleanup"]["left"] == 0
    assert any("userFills" in e for e in meta["final_check"]["errors"]), meta["final_check"]
    assert "final check incomplete" in err
    assert_clean(fake)


def test_results_that_cannot_be_written_do_not_skip_the_cleanup(make_fake, tmp_path):
    fake = make_fake()
    blocker = tmp_path / "not_a_dir"
    blocker.write_text("x")
    rc, out, err, _ = run_probe(fake, tmp_path, out=blocker / "results")
    assert rc == 2 and "RESULTS NOT WRITTEN" in err, err
    assert "probe orders still open on BTC: 0" in out
    assert_clean(fake)


def test_a_terminal_hangup_mid_plan_still_cleans_up_and_writes_results(make_fake, tmp_path):
    """Обрыв SSH / закрытый терминал: настоящий SIGHUP, после него каждый write в tty — EIO.
    Раньше print очистки бросал из finally: сверка пропущена, результатов нет, код 120."""
    fake = make_fake()
    rc, seen, result = run_probe_on_a_terminal_that_hangs_up(fake, tmp_path, after=b"S5 #")
    assert b"S5 #" in seen, seen[-300:]
    assert rc == 2, (rc, seen[-300:])
    meta = result["meta"]
    assert meta["aborted"] == "Interrupted: stopped by SIGHUP" and meta["cleanup"]["left"] == 0
    assert meta["final_check"]["errors"] == [] and meta["wind_down_failures"] == []
    log = Path(meta["console_log"])
    assert log.parent == tmp_path / "out" and log.name.endswith("_hl_modify_probe_console.log"), log
    text = log.read_text()
    assert "the terminal is gone" in text and "probe orders still open on BTC: 0" in text and "saved " in text, text
    assert_clean(fake)


def test_the_account_stream_never_follows_a_redirect(make_fake, tmp_path):
    """307 на рукопожатие /ws: websocket-client шёл по нему (до 3 раз), и «филл → аборт до
    следующего POST» смотрел бы в чужой поток. Теперь — отказ до первого действия."""
    trap = make_fake()  # хост, которого пробнику не давали
    fake = make_fake()
    fake.on_ws = lambda ctx: ("redirect", f"ws://127.0.0.1:{trap.port}/ws")
    rc, _, err, result = run_probe(fake, tmp_path)
    assert rc == 3 and "account stream" in err, err
    assert fake.ws_log == ["/ws"] and trap.ws_log == [], (fake.ws_log, trap.ws_log)
    assert fake.exchange_log == [] and result is None


def test_an_order_landing_after_the_first_empty_list_is_still_swept(make_fake, tmp_path):
    """M2: «чисто» — два пустых списка подряд с паузой. Ордер, легший сразу после первого
    пустого (поздняя постановка после сбоя транспорта), снимается следующим кругом."""
    fake = make_fake()
    late_cloid = "0x0dd1facefeed" + "0" * 16 + "0001"
    state = {"plan_done": False, "late": None}
    fake.on_exchange = PlanHook(21, lambda ctx: state.update(plan_done=True), phase="after")
    real_info = fake.info

    def info(body):
        out = real_info(body)
        empty = body.get("type") == "frontendOpenOrders" and not any(
            o["coin"] == "BTC" and hp.is_probe_cloid(o.get("cloid")) for o in out)
        if state["plan_done"] and state["late"] is None and empty:
            state["late"] = fake.seed_order("BTC", "71500", "0.0002", late_cloid)  # легла после ответа
        return out

    fake.info = info
    rc, _, err, result = run_probe(fake, tmp_path)
    assert rc == 0, err
    assert state["late"] is not None, "the scenario needs an empty list during cleanup"
    assert any(state["late"] in c["oids"] for c in result["meta"]["cleanup"]["cancels"]), result["meta"]["cleanup"]
    assert_clean(fake)
    assert state["late"] not in {o.oid for o in fake.open_orders()}


def test_a_fill_on_a_probe_order_aborts_and_is_reported(make_fake, tmp_path):
    fake = make_fake()

    def fill(ctx):
        oid = ctx.response["response"]["data"]["statuses"][0]["resting"]["oid"]
        ctx.fake.fill(oid, "0.00005")

    fake.on_exchange = PlanHook(3, fill, phase="after")
    rc, _, err, result = run_probe(fake, tmp_path)
    assert rc == 2, err
    assert "FILLED" in result["meta"]["aborted"]
    final = result["meta"]["final_check"]
    assert len(final["probe_fills"]) == 1 and final["position_changed"] is True
    assert_clean(fake)


def test_a_silent_partial_fill_is_caught_by_the_size_check(make_fake, tmp_path):
    """Рамки userFills нет (поток потерял её) — объём на бирже меньше запрошенного."""
    fake = make_fake()

    def fill(ctx):
        oid = ctx.response["response"]["data"]["statuses"][0]["resting"]["oid"]
        ctx.fake.fill(oid, "0.00005", silent=True)

    fake.on_exchange = PlanHook(5, fill, phase="after")
    rc, _, err, result = run_probe(fake, tmp_path)
    assert rc == 2, err
    assert "partially FILLED" in result["meta"]["aborted"]
    assert len(result["meta"]["final_check"]["probe_fills"]) == 1
    assert_clean(fake)


def test_late_frames_are_attributed_to_the_previous_step(make_fake, tmp_path):
    fake = make_fake(fh.Profile(frame_delay_s=0.5))  # окно наблюдения FAST — 0.35 с
    rc, _, err, result = run_probe(fake, tmp_path)
    assert rc == 0, err
    late = result["summary"]["late_frames_by_step"]
    stamped = [e for s in result["steps"] if "skipped" not in s for e in s.get("ws_entries", [])
               if e.get("stamped_before_send")]
    assert late or stamped, "late frames were neither attributed nor flagged"
    for s in result["steps"]:
        for e in s.get("late_ws_entries", []):
            assert e["late"] is True
    assert_every_frame_seen_once(fake, result)
    assert_clean(fake)


def test_frames_landing_between_the_window_and_the_next_post_go_to_the_previous_step(make_fake, tmp_path):
    """Рамки шага N приходят ровно в щель: после окна N, до POST N+1 (не в окно N+1)."""
    fake = make_fake(fh.Profile(frames_released_on_rate_limit=2))
    rc, _, err, result = run_probe(fake, tmp_path)
    assert rc == 0, err
    late = result["summary"]["late_frames_by_step"]
    assert set(late) >= {"S2", "S3", "S4", "S6"}, late
    for s in result["steps"]:
        assert not any(e.get("stamped_before_send") for e in s.get("ws_entries", [])), s["step"]
    assert_every_frame_seen_once(fake, result)
    assert_clean(fake)


def test_a_fill_seen_between_steps_stops_the_plan_before_the_next_post(make_fake, tmp_path):
    """Филл после окна S3 (лист открытых ордеров уже снят) — S4 не отправляется."""
    fake = make_fake()
    state = {"posts": 0, "rl_after_s3": 0, "oid": None}

    def on_exchange(ctx):
        if ctx.phase == "after" and ctx.kind != "cancel":
            state["posts"] += 1
            if state["posts"] == 3:
                state["oid"] = ctx.response["response"]["data"]["statuses"][0]["resting"]["oid"]

    def on_info(ctx):
        if state["oid"] and ctx.kind == "userRateLimit":
            state["rl_after_s3"] += 1
            if state["rl_after_s3"] == 2:  # b0 шага S4: окно S3 и его листинг позади
                ctx.fake.fill(state["oid"], "0.00005")

    fake.on_exchange, fake.on_info = on_exchange, on_info
    rc, _, err, result = run_probe(fake, tmp_path)
    assert rc == 2, err
    assert "FILLED (frame seen late, after S3)" in result["meta"]["aborted"]
    assert steps_of(result) == ["S1", "S2", "S3"]
    assert len([e for e in fake.exchange_log if e["type"] != "cancel"]) == 3, "a POST went out after the fill"
    assert_clean(fake)


def test_the_q13_wire_rechecks_its_own_guardrail(monkeypatch):
    """Защита в глубину: даже если расчёт цены Q13 ошибётся, cross_wire не соберёт wire."""
    led = hp.Ledger(hp.make_run_prefix(1))
    c = led.mint()
    client = type("C", (), {"timing": hp.FAST, "bind": lambda self, *_a: None,
                            "sdk": type("S", (), {"float_to_wire": staticmethod(str)})()})()
    p = hp.Probe(client, None, led, "BTC", 3, 5, "0x" + "b" * 40)
    p.book = {"best_bid": 99990.0, "best_ask": 100010.0, "mid": 100000.0, "ref": 99990.0}
    assert p.cross_wire(c)["p"]
    for wrong in ((101100.0, 0.0001), (99900.0, 0.0001), (100100.0, 0.00012)):  # +1 %, ниже ask, $12
        monkeypatch.setattr(hp, "cross_order", lambda book, szd, w=wrong: w)
        with pytest.raises(hp.Guardrail):
            p.cross_wire(c)


def test_a_dead_account_stream_aborts_and_cleans_up(make_fake, tmp_path):
    fake = make_fake()
    fake.on_exchange = PlanHook(7, lambda ctx: ctx.fake.kill_streams(), phase="after")
    rc, _, err, result = run_probe(fake, tmp_path)
    assert rc == 2, err
    assert "stream died" in result["meta"]["aborted"]
    assert_clean(fake)


def test_a_venue_that_accepts_a_sub_minimum_modify_aborts(make_fake, tmp_path):
    """S15 обязан получить отказ (гардрейл «~$5 — биржа обязана отвергнуть»)."""
    fake = make_fake(fh.Profile(min_notional=0.0))
    rc, _, err, result = run_probe(fake, tmp_path)
    assert rc == 2, err
    assert steps_of(result)[-1] == "S15" and "obliged to refuse" in result["meta"]["aborted"]
    assert_clean(fake)


def test_a_charge_above_the_prediction_counts_against_the_cap(make_fake, tmp_path):
    """Биржа (или чужая активность счёта) списала на S3 на 3 больше: потолок держит факт — и держит
    его ДО подписи (MX1): на S21 запросов плана уже нет, и он на биржу не уходит."""
    fake = make_fake()

    def overcharge(ctx):
        with ctx.fake.lock:
            ctx.fake.used += 3

    fake.on_exchange = PlanHook(3, overcharge, phase="after")
    rc, _, err, result = run_probe(fake, tmp_path)
    assert rc == 2, err
    assert "plan budget: 28 + 1 > 28" in result["meta"]["aborted"], result["meta"]["aborted"]
    assert result["steps"][2]["charged_over_predicted"] == 3
    assert steps_of(result)[-1] == "S20"
    plan = [e for e in fake.exchange_log if e["type"] != "cancel"]
    assert len(plan) == 20, "an action the plan budget could not pay for reached the venue"
    assert_clean(fake)


def test_a_fill_inside_a_step_window_stops_the_plan_at_that_step(make_fake, tmp_path):
    """MX26: C1 исполнен целиком сразу после S15 — шага, которому разрешено убить C1 (may_kill). Без
    аборта по филлу в окне исчезновение C1 сошло бы за ответ: S18–S21 ушли бы на биржу, а о филле
    сказала бы только итоговая сверка."""
    fake = make_fake()
    state = {}

    def fill_c1(ctx):
        state["open"] = [o.oid for o in ctx.fake.open_orders() if o.coin == "BTC" and hp.is_probe_cloid(o.cloid)]
        state["posts"] = len([e for e in ctx.fake.exchange_log if e["type"] != "cancel"])
        if len(state["open"]) == 1:
            ctx.fake.fill(state["open"][0])

    fake.on_exchange = PlanHook(15, fill_c1, phase="after")
    rc, _, err, result = run_probe(fake, tmp_path)
    assert len(state["open"]) == 1 and state["posts"] == 15, state
    assert rc == 2, err
    assert result["meta"]["aborted"].startswith("Abort: S15: a probe order FILLED"), result["meta"]["aborted"]
    assert steps_of(result)[-1] == "S15"
    assert len([e for e in fake.exchange_log if e["type"] != "cancel"]) == 15, "a POST went out after the fill"
    final = result["meta"]["final_check"]
    assert len(final["probe_fills"]) == 1 and final["position_changed"] is True
    assert_clean(fake)


@pytest.mark.parametrize("foreign, code", [("a1f0_btc", 2), ("no_cloid_btc", 4), ("probe_shape_eth", 2)])
def test_an_answer_naming_a_foreign_order_stops_the_plan_and_that_order_is_never_touched(
    make_fake, tmp_path, foreign, code
):
    """Врущая биржа: ответ на S1 несёт oid чужого ордера счёта (под cloid a1f0, без cloid, или на
    другой монете под формой пробника), а реестр берёт oid ответа по позиции. Раньше очистка сняла
    чужой ордер, а аборт винил «префикс прогона»; ордер на другой монете не замечался вовсе, и S2
    правил его по oid. Теперь: аборт на S1 с настоящей причиной, очистка снимает только строки с
    cloid формы пробника на монете, чужая строка — в отчёте. Без cloid это мог быть и наш ордер
    (правка сбросила cloid): пробник его не трогает и выходит с кодом 4 — проверить руками."""
    fake = make_fake()
    foreign_oid = fake.foreign[foreign]

    def lie(ctx):
        ctx.response["response"]["data"]["statuses"][0] = {"resting": {"oid": foreign_oid}}

    fake.on_exchange = PlanHook(1, lie, phase="after")
    rc, _, err, result = run_probe(fake, tmp_path)
    assert rc == code, err
    aborted = result["meta"]["aborted"]
    coin, _, _, cloid = FOREIGN[foreign]
    where = (f"under cloid {cloid}" if cloid else "without a cloid") + f" on {coin}"
    assert aborted.startswith("Abort: S1: ") and f"oid {foreign_oid}" in aborted and where in aborted, aborted
    assert "never minted" not in aborted, aborted
    assert steps_of(result) == ["S1"]
    cleanup = result["meta"]["cleanup"]
    expected = [foreign_oid] if coin == "BTC" else []  # очистка смотрит только на монету пробника
    assert cleanup["left"] == 0 and [d["oid"] for d in cleanup["disowned"]] == expected, cleanup
    if code == 4:
        assert "NOT CLEAN" in err and "by hand" in err and str(foreign_oid) in err, err
    assert_clean(fake)
    assert_wire_discipline(fake)


def test_an_info_redirect_is_never_followed(make_fake, tmp_path):
    """MX2: /info тоже не идёт по редиректам. Его список — sweepable очистки: ответ, пришедший по
    307 с чужого хоста, назначал бы цели отмены. Ловушка показывает чужой ордер a1f0 под cloid
    формы пробника; 307 — ошибка ответа, как на /exchange: «не проверено», код 4, --sweep-only."""
    hits = []
    fake = make_fake()
    disguised = json.dumps([{
        "coin": "BTC", "side": "B", "limitPx": "70000.0", "sz": "0.0002", "origSz": "0.0002", "timestamp": 1,
        "oid": fake.foreign["a1f0_btc"], "cloid": "0x0dd1abababab" + "0" * 16 + "0001",
    }]).encode()

    class Trap(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802
            hits.append(self.path)
            self.rfile.read(int(self.headers.get("Content-Length") or 0))
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(disguised)))
            self.end_headers()
            self.wfile.write(disguised)

        def log_message(self, *a):
            pass

    trap = ThreadingHTTPServer(("127.0.0.1", 0), Trap)
    threading.Thread(target=trap.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{trap.server_address[1]}/info"
    fake.on_exchange, stopped = stop_at(6)
    fake.on_info = lambda ctx: ("redirect", url) if stopped.is_set() and ctx.kind == "frontendOpenOrders" else None
    try:
        rc, _, err, result = run_probe(fake, tmp_path)
    finally:
        trap.shutdown()
    assert hits == [], "an /info answer was taken from a host the probe was never given"
    assert rc == 4 and "NOT CLEAN" in err, err
    cleanup = result["meta"]["cleanup"]
    assert cleanup["left"] is None and cleanup["cancels"] == [], cleanup
    assert cleanup["errors"] and all("HTTP 307" in e for e in cleanup["errors"]), cleanup
    assert probe_orders_open(fake), "the scenario needs live probe orders"
    fake.on_info = fake.on_exchange = None
    rc, _, err, _ = run_probe(fake, tmp_path / "sweep", "--sweep-only")
    assert rc == 0, err
    assert_clean(fake)


def test_a_run_on_any_sdk_but_the_pinned_one_is_refused_before_any_request(make_fake, tmp_path, monkeypatch, capsys):
    """MX3: пин hyperliquid-python-sdk==0.24.0 проверяется в начале прогона, до первого запроса к
    бирже. Тот же SDK под другим номером версии — отказ, код 3, на биржу не ушло ничего."""
    fake = make_fake()
    real = hp.load_sdk()
    monkeypatch.setattr(hp, "load_sdk", lambda: SimpleNamespace(**{**vars(real), "version": "0.23.0"}))
    monkeypatch.setattr(hp.Stopper, "install", lambda self: None)  # если план всё же пойдёт — не в сигналах pytest
    rc = hp.main(["--fake-venue", str(fake.port), "--state-dir", str(tmp_path / "state"), "--out", str(tmp_path / "out")])
    err = capsys.readouterr().err
    assert rc == 3 and f"pinned to {hp.SDK_PIN}" in err, err
    assert fake.info_log == [] and fake.exchange_log == [], "the probe reached the venue on an unpinned SDK"


def test_another_runs_order_appearing_mid_plan_aborts_and_is_swept(make_fake, tmp_path):
    fake = make_fake()
    other = "0x0dd1feedface" + "0" * 16 + "0001"
    fake.on_exchange = PlanHook(8, lambda ctx: ctx.fake.seed_order("BTC", "72000", "0.0002", other), phase="after")
    rc, _, err, result = run_probe(fake, tmp_path)
    assert rc == 2, err
    assert "another probe run" in result["meta"]["aborted"]
    assert_clean(fake)  # снят и чужой-прогонный ордер той же формы


# --- Q13: исключение из 5 % ------------------------------------------------------------------


def test_q13_a_venue_that_fills_the_cross_aborts_and_reports_the_fill(make_fake, tmp_path):
    fake = make_fake(fh.Profile(alo_modify_cross="fill"))
    rc, _, err, result = run_probe(fake, tmp_path)
    assert rc == 2, err
    assert steps_of(result)[-1] == "S21" and "FILLED" in result["meta"]["aborted"]
    fills = result["meta"]["final_check"]["probe_fills"]
    assert len(fills) == 1 and float(fills[0]["px"]) * float(fills[0]["sz"]) <= hp.CROSS_MAX_NOTIONAL
    assert_clean(fake)


def test_q13_a_cross_that_rests_because_the_market_moved_aborts(make_fake, tmp_path):
    fake = make_fake()

    def move(ctx):  # ask ушёл вверх между чтением книги и обработкой правки
        ctx.fake.set_book("BTC", "86200", "86300")

    fake.on_exchange = PlanHook(21, move)
    rc, _, err, result = run_probe(fake, tmp_path)
    assert rc == 2, err
    s21 = result["steps"][-1]
    assert s21["step"] == "S21" and "obliged to refuse" in s21["answer_abort"]
    assert "breaks a guardrail" in s21["observe_abort"]
    assert_clean(fake)


def test_q13_is_the_only_wire_through_the_touch_and_stays_within_11_usd(make_fake, tmp_path):
    fake = make_fake()
    rc, _, err, _ = run_probe(fake, tmp_path)
    assert rc == 0, err
    bid, ask = (float(x) for x in fh.BOOKS["BTC"])
    plan = [e for e in fake.exchange_log if e["type"] != "cancel"]
    for n, entry in enumerate(plan, start=1):
        for w in hp.action_wires(entry["action"]):
            px, sz = float(w["p"]), float(w["s"])
            if n == 21:
                assert ask <= px <= ask * 1.002 and 10 <= px * sz <= 11, (px, sz)
            else:
                assert px <= 0.95 * bid, (n, px)


# --- До первого действия -------------------------------------------------------------------


def test_too_many_leftovers_refuse_then_sweep_only_clears_them(make_fake, tmp_path):
    leftovers = [f"0x0dd1deadbeef{'0' * 16}{i:04x}" for i in range(1, 5)]
    fake = make_fake(leftovers=leftovers)
    rc, _, err, result = run_probe(fake, tmp_path)
    assert rc == 3 and "run --sweep-only first" in err, err
    assert fake.exchange_log == [] and result is None
    rc, _, err, _ = run_probe(fake, tmp_path / "sweep", "--sweep-only")
    assert rc == 0, err
    assert_clean(fake)


def test_a_second_run_is_refused_while_the_lock_is_held(make_fake, tmp_path):
    fake = make_fake()
    state = tmp_path / "state"
    state.mkdir()
    with open(state / "hl_modify_probe.lock", "a+") as held:
        fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
        rc, _, err, _ = run_probe(fake, tmp_path)
    assert rc == 3 and "holds" in err, err
    assert fake.exchange_log == [] and fake.info_log == []


def test_dry_run_signs_nothing(make_fake, tmp_path):
    fake = make_fake()
    rc, out, err, _ = run_probe(fake, tmp_path, "--dry-run")
    assert rc == 0, err
    assert fake.exchange_log == [] and "nothing was signed or sent" in out


def test_a_terminal_that_cannot_encode_a_line_gets_it_escaped(make_fake, tmp_path, monkeypatch):
    """Не-UTF-8 терминал: строка с «·» не роняет пробник и не теряется — она экранируется."""
    monkeypatch.setenv("PYTHONIOENCODING", "ascii")
    fake = make_fake()
    rc, out, err, _ = run_probe(fake, tmp_path, "--dry-run")
    assert rc == 0, err
    assert "\\xb7ref = " in out and "nothing was signed or sent" in out, out


def test_an_infeasible_q13_book_is_refused_before_anything_is_sent(make_fake, tmp_path):
    fake = make_fake()
    fake.set_book("BTC", "299990", "300000")  # лот $3: в $10–11 через тач не попасть
    rc, _, err, _ = run_probe(fake, tmp_path)
    assert rc == 3 and "lot grid" in err, err
    assert fake.exchange_log == []


def test_a_clock_off_the_venue_is_refused_but_a_stale_book_time_is_not(make_fake, tmp_path):
    fake = make_fake()
    real = fake.info

    def stale_book(body):  # тихий тестнет: `time` книги стоит минуту — это не перекос часов
        out = real(body)
        if body.get("type") == "l2Book":
            out["time"] -= 60_000
        return out

    fake.info = stale_book
    fake.date_offset_s = -10.0  # а вот часы биржи на 10 с позади наших — отказ
    rc, _, err, _ = run_probe(fake, tmp_path)
    assert rc == 3 and "off the venue" in err, err
    assert fake.exchange_log == []
    fake.date_offset_s = 0.0
    rc, _, err, result = run_probe(fake, tmp_path / "ok")
    assert rc == 0, err
    assert abs(result["meta"]["clock_lead_ms_at_start"]) < 1500
    assert result["meta"]["book_at_start"]["book_age_ms"] >= 60_000
    assert_clean(fake)


def test_sweep_only_does_not_depend_on_the_plan(make_fake, tmp_path):
    """Невыполнимый Q13 и врущие часы не мешают снять хвосты."""
    fake = make_fake(leftovers=[LEFTOVER, "0x0dd1deadbeef" + "0" * 16 + "0003"])
    fake.set_book("BTC", "299990", "300000")
    fake.date_offset_s = -10.0
    rc, out, err, _ = run_probe(fake, tmp_path, "--sweep-only")
    assert rc == 0, err
    assert_clean(fake)


def test_the_fake_venue_mode_refuses_a_real_key(tmp_path):
    proc = subprocess.run([sys.executable, "-B", str(PROBE), "--fake-venue", "18545", "--config", "x.toml"],
                          capture_output=True, text=True)
    assert proc.returncode == 2 and "public test key" in proc.stderr


# --- Детектор общего кошелька: фальшивый /proc -----------------------------------------------


def _proc(root: Path, pid: int, argv, *, cwd=None, environ=None, exe=None):
    d = root / str(pid)
    d.mkdir(parents=True)
    (d / "cmdline").write_bytes(b"\0".join(a.encode() for a in argv) + b"\0")
    if cwd is not None:
        (d / "cwd").symlink_to(cwd)
    if environ is not None:
        (d / "environ").write_bytes(b"\0".join(f"{k}={v}".encode() for k, v in environ.items()))
    if exe is not None:
        (d / "exe").symlink_to(exe)


def test_the_shared_wallet_detector_on_a_fake_proc(tmp_path):
    root, home = tmp_path / "proc", tmp_path / "home"
    home.mkdir()
    mine = home / "probe.toml"
    mine.write_text("x = 1\n")
    other_creds = home / "other.toml"
    other_creds.write_text("x = 2\n")
    (home / "a.toml").write_text(f'credentials_path = "{mine}"\ncoins = ["BTC"]\n')
    (home / "b.toml").write_text('credentials_path = "other.toml"\n')
    (home / "c.toml").write_text(f'account_address = "{"0x" + "b" * 40}"\ncoins = ["ETH"]\n')
    _proc(root, 101, ["/opt/connector", "hl", "hyperliquid", str(home / "a.toml")], cwd=home, environ={})
    _proc(root, 102, ["./connector-v2", "hl2", "hyperliquid", "b.toml"], cwd=home, environ={})
    # чужой uid: ни cwd, ни environ не читаются, бинарь переименован — exe всё равно «connector»
    _proc(root, 103, ["/usr/bin/x", "hl3", "hyperliquid", str(home / "c.toml")], exe="/opt/bin/connector")
    _proc(root, 106, ["connector", "hl4", "hyperliquid", "d.toml"])  # относительный конфиг, cwd не читается
    _proc(root, 104, ["/opt/connector", "bb", "bybit", "bybit.toml"], cwd=home, environ={})
    _proc(root, 105, ["/usr/bin/python3", "x", "hyperliquid", "y"], cwd=home, environ={})
    found = {c["pid"]: c for c in hp.running_hl_connectors(root)}
    assert set(found) == {101, 102, 103, 106}
    assert found[101]["credentials"] == str(mine) and found[101]["unresolved"] is None
    assert found[102]["credentials"] == str(other_creds)
    assert found[103]["credentials"] is None and "environment is unreadable" in found[103]["unresolved"]

    agent, master = "0x" + "a" * 40, "0x" + "b" * 40
    keys = {str(other_creds): ("0x" + "c" * 40, master)}
    refusals, warnings = hp.shared_wallet_conflicts(mine, agent, master, "BTC", list(found.values()),
                                                    lambda p: keys.get(str(p), (None, None)))
    assert len(refusals) == 1 and "pid 101" in refusals[0] and "same credentials file" in refusals[0]
    assert any("pid 102" in w and "trades this account" in w for w in warnings), warnings
    assert any("pid 103" in w and "cannot tell" in w for w in warnings), warnings
    assert any("pid 103" in w and "trades this account" in w for w in warnings), warnings
    assert any("pid 106" in w and "cannot tell" in w for w in warnings), warnings
    assert any("pid 106" in w and "account is unknown" in w for w in warnings), warnings
    keys[str(other_creds)] = (agent, master)
    refusals, _ = hp.shared_wallet_conflicts(home / "elsewhere.toml", agent, master, "BTC", list(found.values()),
                                             lambda p: keys.get(str(p), (None, None)))
    assert any("pid 102" in r and "same API wallet" in r for r in refusals), refusals


def test_results_and_lock_live_outside_the_repository(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "xdg"))
    assert hp.default_state_dir() == tmp_path / "xdg" / "hl_modify_probe"
    monkeypatch.delenv("XDG_STATE_HOME")
    assert HERE not in hp.default_state_dir().parents
    first = hp.acquire_lock(tmp_path / "s")
    try:
        with pytest.raises(hp.Refuse):
            hp.acquire_lock(tmp_path / "s")
    finally:
        first.close()
    hp.acquire_lock(tmp_path / "s").close()


def test_the_stopper_ignores_signals_during_cleanup():
    stopper = hp.Stopper()
    old = {s: signal.getsignal(getattr(signal, s)) for s in hp.Stopper.SIGNALS if hasattr(signal, s)}
    try:
        stopper.install()
        os.kill(os.getpid(), signal.SIGTERM)
        time.sleep(0.05)
        assert stopper.reason == "SIGTERM"
        with pytest.raises(hp.Interrupted):
            stopper.sleep(5)  # обрывается сразу: стоп уже запрошен
        stopper.cleaning = True
        os.kill(os.getpid(), signal.SIGINT)
        time.sleep(0.05)
        assert stopper.ignored == ["SIGINT"] and stopper.reason == "SIGTERM"
    finally:
        for s, h in old.items():
            signal.signal(getattr(signal, s), h)
