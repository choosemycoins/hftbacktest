"""Masking rules added in v2 (DESIGN.md §10.2): each with its shape and a
false-positive guard. Synthetic, never-used values."""

import pytest

from kb.redact import redact, redact_obj

PEM = ("-----BEGIN OPENSSH PRIVATE KEY-----\n"
       "b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW\n"
       "QyNTUxOQAAACDxKZ8tLc0Q2cfp3fake0nly4tests0nlyAAAAAAAAAAAAAAAAAAAAAAAAAA\n"
       "-----END OPENSSH PRIVATE KEY-----")
TG_BOT = "123456789:AAH4bX9fakeTokenForTests_only-abc12"   # 35 after the colon
B58 = "4" + "Wq9ZdYx3kL7vRmT2nP5cJ8hF6gB1eA" * 3          # 91 chars of the base58 alphabet
XAI = "xai-" + "Ab3dEf6hIj9kLm2nOp5qRs8tUv1wXy4z"
HF = "hf_" + "AbCdEfGhIjKlMnOpQrStUvWxYz012345"
GOOGLE = "AIza" + "SyD-fake_Key1234567890abcdefghijklm"
BINANCE = "vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A"
BYBIT_SECRET = "Xk3pL9qWm2Rt7Yv4Bn6Hc8Jd1Fs5Ga0Ze3Ux"


@pytest.mark.parametrize("secret,text,tag", [
    (PEM, f"ключ агента:\n{PEM}\nконец", "pem"),
    (TG_BOT, f"бот токен {TG_BOT} для алертов", "tg"),
    (B58, f"solana private key {B58} импортирован", "b58"),
    (XAI, f"export XAI {XAI}", "token"),
    (HF, f"HF {HF} read-only", "token"),
    (GOOGLE, f"maps {GOOGLE} квота", "token"),
    (BINANCE, f"Binance API secret: {BINANCE}", None),
    (BYBIT_SECRET, f"bybit {BYBIT_SECRET} testnet", "entropy"),
])
def test_new_secret_shapes_are_masked(secret, text, tag):
    out = redact(text)
    assert secret not in out, out
    assert "redacted" in out
    if tag:
        assert f"[redacted:{tag}]" in out


def test_truncated_pem_is_masked_to_the_end():
    # short body lines: no generic rule would catch them one by one
    body = "\n".join(["b3BlbnNzaC1rZXktdjEAAA", "AABG5vbmUAAAAEbm9uZQAA", "QyNTUxOQAAACDxKZ8tLc0Q"])
    cut = "-----BEGIN OPENSSH PRIVATE KEY-----\n" + body   # a chunk boundary cut the END line off
    out = redact("до\n" + cut)
    assert "b3BlbnNzaC1rZXktdjEA" not in out and "QyNTUxOQ" not in out
    assert out == "до\n[redacted:pem]"


@pytest.mark.parametrize("text", [
    "commit 4f37797443c95274b57f3cfc3c08c5b6c12f9d73 fixed tick rounding",
    "session 7a306fe8-d654-4780-9776-816dcfe1d94c resumed",
    "config key subscription_tracker_unmark_symbol_after_reject_v2 set",
    "/home/andrew/.claude/projects/-home-andrew-RustroverProjects-hftbacktest/memory/x.md",
    "/home/andrew/RustroverProjects/hftbacktest/collector/tools/build_dataset.py:2124",
    "Бот работает 24/7, задержка 200 мс, 60 sendTx в минуту, BTCUSDT ETHUSDT SOLUSDT",
    "connector/src/bybit/public_stream.rs SubscriptionTracker unmark",
    "кошелёк 0x не нужен, вывод 10006 Too many visits",
])
def test_ordinary_text_hashes_ids_and_paths_survive(text):
    assert redact(text) == text


def test_snippet_marks_cannot_split_a_secret_past_the_rules():
    from kb.search import make_matcher, parse_query, snippet
    text = f"новый ключ {BINANCE} выдан, ключ от тестнета"
    out = snippet(text, make_matcher(parse_query("ключ")))
    assert BINANCE not in out and BINANCE[:20] not in out and "[ключ]" in out


def test_redact_obj_masks_keys_values_and_nested_lists():
    obj = {"title": f"T {XAI}", "results": [{"snippet": f"s {HF}", "n": 3}],
           f"k {GOOGLE}": [TG_BOT, None, 1.5]}
    out = redact_obj(obj)
    flat = repr(out)
    for s in (XAI, HF, GOOGLE, TG_BOT):
        assert s not in flat
    assert out["results"][0]["n"] == 3
