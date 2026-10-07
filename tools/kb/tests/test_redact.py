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


# --- named secrets: the NAME only has to contain the word; URL passwords ----------
# `\bpassword\b` missed every real-world name: DB_PASSWORD, BYBIT_API_KEY,
# apiSecret, x-api-key (`_`, `-` and camelCase glue the word to the rest), and
# a value shorter than 6 or quoted with spaces went out (in part) in clear.

@pytest.mark.parametrize("text,want", [
    ("export DB_PASSWORD=hunter2xyz", "export DB_PASSWORD=[redacted]"),
    ("BYBIT_API_KEY = AbCdEf123456", "BYBIT_API_KEY = [redacted]"),
    ("GITHUB_TOKEN: ghx_abcdefabcdef", "GITHUB_TOKEN: [redacted]"),
    ("apiSecret: s3cr3tvalue", "apiSecret: [redacted]"),
    ('{"client_secret": "Zz9-yy8_xx7"}', '{"client_secret": [redacted]}'),
    ("{'Refresh_Token': 'rt-0001'}", "{'Refresh_Token': [redacted]}"),
    ("x-api-key: k3y-value-q", "x-api-key: [redacted]"),
    ("HL_PRIVATE_KEY=deadbeefcafe", "HL_PRIVATE_KEY=[redacted]"),
    ("mysql.passwd: qwerty", "mysql.passwd: [redacted]"),
    ("password=abc12", "password=[redacted]"),                       # short values too
    ("пароль password:\xa0Hunter2Hunter", "пароль password:\xa0[redacted]"),   # Telegram's &nbsp;
    ("Password := pw", "Password := [redacted]"),
    ("SECRET_KEY='correct horse battery staple' # prod", "SECRET_KEY=[redacted] # prod"),
    ('WALLET_MNEMONIC="abandon ability able about above absent"', "WALLET_MNEMONIC=[redacted]"),
    ('token = "cut off mid-value\nnext line', "token = [redacted]\nnext line"),   # unclosed: to the line end
])
def test_a_named_secret_is_masked_and_its_name_kept(text, want):
    assert redact(text) == want


@pytest.mark.parametrize("text,want", [
    ("DATABASE_URL=postgres://admin:hunter2@db.example.com:5432/x",
     "DATABASE_URL=postgres://admin:[redacted]@db.example.com:5432/x"),
    ("redis://:Sup3rPass@127.0.0.1:6379/0", "redis://:[redacted]@127.0.0.1:6379/0"),
    ("clone https://user:p@ss@host.io/repo.git", "clone https://user:[redacted]@host.io/repo.git"),
    ("AMQP://Guest:gu3st@rabbit", "AMQP://Guest:[redacted]@rabbit"),
])
def test_a_url_password_is_masked_user_and_host_kept(text, want):
    assert redact(text) == want


@pytest.mark.parametrize("text", [
    "https://example.com:8080/path?q=1",                 # a port is not a password
    "ssh://git@github.com/org/repo.git",                 # a user without a password
    "git@github.com:org/repo.git",
    "if token == expected: return",                      # a comparison, not an assignment
    "assert secret != other and password <= limit",
    "tokenizer = AutoTokenizer.from_pretrained(path)",   # tokeniz… is not a token
    "tokenize='unicode61 remove_diacritics 2'",
    "the token bucket refills at 10/s; secret sauce",    # no separator at all
])
def test_named_and_url_rules_leave_ordinary_text_alone(text):
    assert redact(text) == text


@pytest.mark.parametrize("text", [
    "export DB_PASSWORD=hunter2xyz", '{"client_secret": "Zz9-yy8_xx7"}',
    "SECRET_KEY='correct horse battery staple'", "redis://:Sup3rPass@127.0.0.1:6379/0",
    "clone https://user:p@ss@host.io/repo.git",
])
def test_masking_twice_changes_nothing(text):
    """Output is masked more than once (snippet, the JSON, the printed line)."""
    once = redact(text)
    assert redact(once) == once


def test_a_long_run_of_name_characters_costs_linear_time():
    """The name is bounded: unbounded, every credential word inside one long
    run of name characters re-scanned the run (12 000 × "token": 1 s; 30 000:
    ~6 s). Bounded, 30 000 take milliseconds — a margin no machine load eats."""
    import time
    text = "token" * 6000
    t0 = time.perf_counter()
    redact(text)
    assert time.perf_counter() - t0 < 1.0
