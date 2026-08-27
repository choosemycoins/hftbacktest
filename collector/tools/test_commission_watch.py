"""Tests for `commission_watch.py`.

No network, no key, no chat: the poll, the clock and the alert channel are
parameters of `run`, exactly as the collector's own pollers take their `fetch`
as one.

The subject is a promise about a number nobody publishes any more. Binance
edits the USD-M fee page in place with no feed and no date (since 2026-02), so
this signed endpoint is the only instrument that says what WE are charged, and
every USDC measurement is conditioned on it answering zero.
"""

import json
from pathlib import Path

import pytest

import commission_watch as cw


#: What the venue actually answered for our key on 2026-08-26 — the zero that
#: the whole USDC class rests on, and the USDT control beside it.
USDC_ANSWER = {"symbol": "BTCUSDC", "makerCommissionRate": "0.00000000", "takerCommissionRate": "0.00040000"}
USDT_ANSWER = {"symbol": "BTCUSDT", "makerCommissionRate": "0.00020000", "takerCommissionRate": "0.00050000"}


@pytest.fixture
def credentials(tmp_path):
    """A key file shaped like the real one, which is TOML with comments."""
    path = tmp_path / "binanceusdm.toml"
    path.write_text(
        "# Ключи Binance USDⓈ-M futures.\n"
        'api_key = "the-key"\n'
        "secret  = 'the-secret'\n"
        'rest_base = "https://fapi.binance.com"\n'
    )
    return path


def args(tmp_path, credentials, symbols):
    return cw.build_parser().parse_args(
        ["--symbols", *symbols, "--out", str(tmp_path / "series"), "--credentials", str(credentials)]
    )


def answers(mapping):
    """A fetch that answers from a dict and records what it was asked."""
    asked = []

    def fetch(base, api_key, secret, symbol):
        asked.append(symbol)
        answer = mapping[symbol]
        if isinstance(answer, Exception):
            raise answer
        return answer

    return fetch, asked


def recorder():
    sent = []

    def send(text, token, chat_id):
        sent.append(text)
        return True

    return send, sent


def series_rows(out_dir: Path):
    rows = []
    for path in sorted(Path(out_dir).glob("commission_*.jsonl")):
        rows += [json.loads(line) for line in path.read_text().splitlines()]
    return rows


def test_the_signature_covers_the_query_that_is_sent():
    """Binance rejects a signature computed over anything but the exact query
    string, so the query is built once and signed as text.

    The vector is checked against an independent HMAC rather than against
    itself: a signature routine that agrees only with its own re-implementation
    of the query encoding is the bug this catches.
    """
    import hashlib
    import hmac

    query = cw.signed_query({"symbol": "BTCUSDC", "timestamp": 1787788800000}, "the-secret")
    body, _, signature = query.rpartition("&signature=")

    assert body == "symbol=BTCUSDC&timestamp=1787788800000", "order and encoding as sent"
    assert (
        signature
        == hmac.new(b"the-secret", body.encode(), hashlib.sha256).hexdigest()
    )


def test_the_credentials_never_reach_the_series_or_an_alert(tmp_path, credentials):
    """The one thing this tool must never do. It holds a live key and writes
    two artefacts an operator will paste around."""
    fetch, _ = answers({"BTCUSDC": USDC_ANSWER, "BTCUSDT": USDT_ANSWER})
    send, sent = recorder()

    cw.run(
        args(tmp_path, credentials, ["BTCUSDC", "BTCUSDT:2"]),
        fetch=fetch,
        now=1787788800.0,
        send=send,
    )

    written = "\n".join(
        p.read_text() for p in (tmp_path / "series").iterdir()
    ) + "\n".join(sent)
    assert "the-secret" not in written
    assert "the-key" not in written


def test_a_promotion_still_in_force_writes_the_series_and_says_nothing(tmp_path, credentials, monkeypatch):
    monkeypatch.setenv("TG_BOT_TOKEN", "t")
    monkeypatch.setenv("TG_CHAT_ID", "c")
    fetch, asked = answers({"BTCUSDC": USDC_ANSWER, "BTCUSDT": USDT_ANSWER})
    send, sent = recorder()

    code = cw.run(
        args(tmp_path, credentials, ["BTCUSDC", "BTCUSDT:2"]),
        fetch=fetch,
        now=1787788800.0,
        send=send,
    )

    assert code == 0
    assert sent == [], "a rate that has not moved is not news"
    rows = series_rows(tmp_path / "series")
    assert [r["symbol"] for r in rows] == ["BTCUSDC", "BTCUSDT"]
    assert rows[0]["maker_bps"] == 0.0 and rows[0]["taker_bps"] == 4.0
    assert rows[0]["maker_raw"] == "0.00000000", "the venue's own string is kept"
    assert rows[1]["maker_bps"] == 2.0
    assert asked == ["BTCUSDC", "BTCUSDT"]


def test_the_promotion_ending_is_an_alert_a_non_zero_exit_and_a_row(tmp_path, credentials, monkeypatch):
    """The event this exists for: USDC starts charging and the control does not.

    Non-zero exit as well as the message, so a unit's `OnFailure=` sees it even
    if Telegram is down — the two paths fail independently.
    """
    monkeypatch.setenv("TG_BOT_TOKEN", "t")
    monkeypatch.setenv("TG_CHAT_ID", "c")
    ended = dict(USDC_ANSWER, makerCommissionRate="0.00020000")
    fetch, _ = answers({"BTCUSDC": ended, "BTCUSDT": USDT_ANSWER})
    send, sent = recorder()

    code = cw.run(
        args(tmp_path, credentials, ["BTCUSDC", "BTCUSDT:2"]),
        fetch=fetch,
        now=1787788800.0,
        send=send,
    )

    assert code == 1
    assert len(sent) == 1
    assert "BTCUSDC" in sent[0] and "+2.000" in sent[0]
    assert "BTCUSDT" not in sent[0], "the control matched; it is not part of the finding"
    rows = series_rows(tmp_path / "series")
    assert rows[0]["maker_bps"] == 2.0


def test_the_alert_goes_quiet_but_the_series_does_not(tmp_path, credentials, monkeypatch):
    """A fee change is permanent until it is not. One message an hour for a
    week is how a channel gets muted — which is the failure this watch exists
    to avoid — but the series must keep a row per poll regardless."""
    monkeypatch.setenv("TG_BOT_TOKEN", "t")
    monkeypatch.setenv("TG_CHAT_ID", "c")
    ended = dict(USDC_ANSWER, makerCommissionRate="0.00020000")
    fetch, _ = answers({"BTCUSDC": ended})
    send, sent = recorder()
    argv = args(tmp_path, credentials, ["BTCUSDC"])

    base = 1787788800.0
    for hour in range(4):
        cw.run(argv, fetch=fetch, now=base + hour * 3600, send=send)

    assert len(sent) == 1, "quiet for six hours after speaking"
    assert len(series_rows(tmp_path / "series")) == 4

    cw.run(argv, fetch=fetch, now=base + 7 * 3600, send=send)
    assert len(sent) == 2, "and speaks again once the quiet period is over"


def test_one_unreachable_symbol_is_recorded_and_does_not_stop_the_others(tmp_path, credentials):
    fetch, _ = answers({"BTCUSDC": USDC_ANSWER, "ZECUSDC": RuntimeError("HTTP Error 418")})
    send, sent = recorder()

    code = cw.run(
        args(tmp_path, credentials, ["BTCUSDC", "ZECUSDC"]),
        fetch=fetch,
        now=1787788800.0,
        send=send,
    )

    assert code == 0, "the promise held everywhere it could be checked"
    rows = series_rows(tmp_path / "series")
    assert [r["symbol"] for r in rows] == ["BTCUSDC", "ZECUSDC"]
    assert "418" in rows[1]["error"], "the hole in the series says why it is there"
    assert "maker_bps" not in rows[1], "and claims no reading"


def test_a_watch_that_has_been_blind_for_six_hours_says_so(tmp_path, credentials, monkeypatch):
    """The other half of "fail closed": a key that expired, an IP allowlist that
    changed, a venue that started refusing — all of them leave the assumption
    unwatched while the timer keeps reporting success."""
    monkeypatch.setenv("TG_BOT_TOKEN", "t")
    monkeypatch.setenv("TG_CHAT_ID", "c")
    fetch, _ = answers({"BTCUSDC": RuntimeError("HTTP Error 401: Unauthorized")})
    send, sent = recorder()
    argv = args(tmp_path, credentials, ["BTCUSDC"])

    base = 1787788800.0
    for hour in range(cw.FAILURES_BEFORE_ALERT - 1):
        assert cw.run(argv, fetch=fetch, now=base + hour * 3600, send=send) == 2
    assert sent == [], "one 502 is ordinary and costs one sample"

    cw.run(argv, fetch=fetch, now=base + cw.FAILURES_BEFORE_ALERT * 3600, send=send)
    assert len(sent) == 1 and "failed" in sent[0]

    # And one good poll re-arms it, so a second outage is a second message.
    good, _ = answers({"BTCUSDC": USDC_ANSWER})
    cw.run(argv, fetch=good, now=base + 10 * 3600, send=send)
    state = json.loads((tmp_path / "series" / "state.json").read_text())
    assert state["consecutive_failures"] == 0


def test_the_verdict_names_every_symbol_that_moved():
    """Which symbols moved is the content: USDC alone means the promotion
    ended, USDC and the USDT control together mean our tier moved."""
    readings = {
        "BTCUSDC": {"maker_bps": 2.0, "taker_bps": 5.0},
        "ZECUSDC": {"maker_bps": 0.0, "taker_bps": 4.0},
        "BTCUSDT": {"maker_bps": 2.0, "taker_bps": 5.0},
    }
    changed = cw.verdict(readings, {"BTCUSDC": 0.0, "ZECUSDC": 0.0, "BTCUSDT": 2.0})
    assert [c[0] for c in changed] == ["BTCUSDC"]


def test_a_rate_file_without_a_key_is_a_usage_error_not_a_silent_zero(tmp_path):
    """Fail closed: a watch that cannot read its key must not report success."""
    path = tmp_path / "empty.toml"
    path.write_text("# nothing here\n")
    with pytest.raises(ValueError):
        cw.read_credentials(path)

    code = cw.run(args(tmp_path, path, ["BTCUSDC"]), fetch=None, now=0.0)
    assert code == 2
