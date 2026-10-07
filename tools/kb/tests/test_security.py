"""Security invariants across commands: modes of everything created, the
offline self-check, no HF import before the offline variables, masked output
on every CLI path."""

import json
import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest

from kb import cli, store

KB_ROOT = Path(__file__).resolve().parents[1]
FAKE_KEY = "0x" + "7a" * 32
BINANCE = "vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A"


def eth0(tmp_path, monkeypatch):
    p = tmp_path / "net_dev_eth"
    p.write_text("Inter-|\n face |\n    lo: 0 0\n  eth0: 1 2\n")
    monkeypatch.setenv("KB_PROC_NET_DEV", str(p))


def c(corpus):
    return ["--config", str(corpus["config"])]


def test_model_loading_commands_refuse_a_visible_network(corpus, tmp_path, monkeypatch, capsys):
    eth0(tmp_path, monkeypatch)
    assert cli.main(c(corpus) + ["update", "--embed", "--lock-timeout", "0"]) == 2
    err = capsys.readouterr().err
    assert "eth0" in err and "unshare -rn" in err
    assert not corpus["db"].exists(), "refused before any write"
    assert cli.main(c(corpus) + ["update", "--lock-timeout", "0"]) == 0, \
        "a plain update loads no model and needs no namespace"
    assert cli.main(c(corpus) + ["embed"]) == 2
    assert cli.main(c(corpus) + ["update", "--require-offline", "--lock-timeout", "0"]) == 2
    assert cli.main(c(corpus) + ["embed", "--allow-network"]) == 0


def test_importing_kb_pulls_in_no_hf_code_and_main_forces_offline(corpus):
    code = (
        "import os, sys\n"
        f"sys.path.insert(0, {str(KB_ROOT)!r})\n"
        "for v in ('HF_HUB_OFFLINE', 'TRANSFORMERS_OFFLINE'): os.environ.pop(v, None)\n"
        "import kb.cli, kb.search, kb.update, kb.embed, kb.semantic, kb.mcp_server, kb.evaluate\n"
        "bad = [m for m in ('huggingface_hub', 'transformers', 'torch') if m in sys.modules]\n"
        "assert not bad, bad\n"
        f"kb.cli.main(['--config', {str(corpus['config'])!r}, 'update', '--lock-timeout', '0'])\n"
        "assert os.environ['HF_HUB_OFFLINE'] == '1' and os.environ['TRANSFORMERS_OFFLINE'] == '1'\n"
        "import huggingface_hub.constants as k\n"
        "assert k.HF_HUB_OFFLINE, 'the variables were in place before the import'\n"
        "print('OK')\n")
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=120,
                       env=dict(os.environ, KB_PROC_NET_DEV=os.environ["KB_PROC_NET_DEV"]))
    assert r.stdout.strip().endswith("OK"), r.stderr[-2000:]


def test_cli_sets_umask_077(corpus):
    old = os.umask(0o022)
    try:
        cli.main(c(corpus) + ["update", "--lock-timeout", "0"])
        assert os.umask(0o022) == 0o077
    finally:
        os.umask(old)


def test_journal_inherits_the_database_mode_not_the_umask(tmp_path):
    db = tmp_path / "kb.db"
    conn = store.open_rw(db)
    store.init_schema(conn)
    os.chmod(db, 0o600)
    old = os.umask(0o022)
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("INSERT INTO meta VALUES ('x', ?)", ("y" * 100000,))
        journal = Path(f"{db}-journal")
        assert journal.exists()
        assert stat.S_IMODE(os.stat(journal).st_mode) == 0o600
        conn.execute("ROLLBACK")
    finally:
        os.umask(old)
        conn.close()


def test_everything_in_the_db_dir_is_private_after_a_full_cycle(corpus, tmp_path, capsys):
    golden = tmp_path / "g.jsonl"
    golden.write_text(json.dumps({"id": "q", "query": "латентность", "tags": [],
                                  "expect": [str(corpus["slug"] / "s1.jsonl")],
                                  "must_contain": ["латентность"]}, ensure_ascii=False) + "\n")
    old = os.umask(0o022)       # even a careless umask in the caller
    try:
        assert cli.main(c(corpus) + ["update", "--embed", "--lock-timeout", "0"]) == 0
        assert cli.main(c(corpus) + ["eval", "--golden", str(golden), "--modes", "fts,hybrid"]) == 0
        assert cli.main(c(corpus) + ["gc"]) == 0
    finally:
        os.umask(old)
    root = corpus["db"].parent
    for p in [root, *root.rglob("*")]:
        mode = stat.S_IMODE(os.stat(p).st_mode)
        assert mode == (0o700 if p.is_dir() else 0o600), (p, oct(mode))


def test_a_foreign_directory_keeps_its_mode(corpus, tmp_path, capsys):
    victim = tmp_path / "shared"
    victim.mkdir()
    (victim / "notes.txt").write_text("someone else's")
    os.chmod(victim, 0o755)
    assert cli.main(c(corpus) + ["--db", str(victim / "kb.db"), "update", "--lock-timeout", "0"]) == 0
    assert stat.S_IMODE(os.stat(victim).st_mode) == 0o755
    assert stat.S_IMODE(os.stat(victim / "kb.db").st_mode) == 0o600
    assert "unrelated files" in capsys.readouterr().err


def test_every_cli_output_path_is_masked(corpus, capsys):
    (corpus["slug"] / "k.jsonl").write_text(json.dumps({
        "type": "user", "uuid": "k", "timestamp": "2026-07-28T00:00:00Z",
        "message": {"role": "user", "content": f"секрет биржи {BINANCE} и ключ {FAKE_KEY}"}}) + "\n")
    (corpus["research"] / "h.md").write_text(f"# Заголовок {BINANCE}\n\nсекрет биржи в заголовке\n")
    assert cli.main(c(corpus) + ["update", "--embed", "--lock-timeout", "0"]) == 0
    capsys.readouterr()
    outs = []
    for args in (["search", "секрет биржи"], ["search", "секрет биржи", "--json"],
                 ["search", "секрет биржи", "--mode", "semantic", "--json"]):
        assert cli.main(c(corpus) + args) == 0
        outs.append(capsys.readouterr().out)
    ids = {r["id"] for r in json.loads(outs[1])["results"]}
    for kid in ids:
        for args in (["read", kid, "--context", "2"], ["read", kid, "--json"]):
            assert cli.main(c(corpus) + args) == 0
            outs.append(capsys.readouterr().out)
    blob = "\n".join(outs)
    assert BINANCE not in blob and FAKE_KEY not in blob
    assert "redacted" in blob


def test_model_verify_refuses_pickles_and_size_mismatches(tmp_path):
    """The runtime never unpickles: a .bin next to the weights is a refusal."""
    from kb import models
    manifest = {"e5b": {"revision": "r", "files": {"model.safetensors": {"size": 1, "sha256": "0"}}}}
    d = tmp_path / "hub" / "models--intfloat--multilingual-e5-base" / "snapshots" / "r"
    d.mkdir(parents=True)
    (d / "model.safetensors").write_bytes(b"x")
    assert models.verify(tmp_path, "e5b", manifest=manifest) == d
    with pytest.raises(models.ModelError, match="sha256"):
        models.verify(tmp_path, "e5b", full=True, manifest=manifest)
    (d / "pytorch_model.bin").write_bytes(b"pickle")
    with pytest.raises(models.ModelError, match="pickle"):
        models.verify(tmp_path, "e5b", manifest=manifest)
    (d / "pytorch_model.bin").unlink()
    (d / "model.safetensors").write_bytes(b"xy")
    with pytest.raises(models.ModelError, match="size"):
        models.verify(tmp_path, "e5b", manifest=manifest)


def test_cli_errors_are_masked_too(corpus, capsys):
    """An error echoes the query back (bad raw: syntax): stderr is an output
    path like any other — a key pasted into a query is masked there too."""
    assert cli.main(c(corpus) + ["update", "--lock-timeout", "0"]) == 0
    capsys.readouterr()
    assert cli.main(c(corpus) + ["search", f'raw:"unbalanced {FAKE_KEY} {BINANCE}', "--mode", "fts"]) == 2
    err = capsys.readouterr().err
    assert "bad query" in err, err
    assert FAKE_KEY not in err and BINANCE not in err and "redacted" in err


# --- snippets: the whole chunk is masked before any cut ---------------------------
# A bot token is one secret but several FTS tokens (`:`, `-`, `_` split them);
# a window cut at a token boundary inside it, or the semantic head cut at
# character 600, leaves a tail no rule recognises. Synthetic, never-used value.

TG_SECRET = "AAHdq-TcvCH1vGWJxfSe_ofSAs0K5PALDs1"      # the 35-char secret half
TG_TOKEN = f"1234567890:{TG_SECRET}"


def test_precondition_the_whole_bot_token_is_masked():
    from kb.redact import redact
    assert len(TG_SECRET) == 35 and TG_SECRET not in redact(f"bot {TG_TOKEN}")


def test_a_snippet_window_never_starts_inside_a_secret():
    from kb.search import make_matcher, parse_query, snippet
    filler = " ".join(f"w{i}" for i in range(33))
    text = f"intro {TG_TOKEN} {filler} alert bot alert bot"
    out = snippet(text, make_matcher(parse_query("alert bot")))
    assert "[alert]" in out
    assert TG_SECRET[:12] not in out and TG_SECRET[-12:] not in out, out


def test_the_semantic_head_never_cuts_a_secret_in_two():
    from kb.search import SEMANTIC_HEAD, snippet
    text = "x" + "\n" * 553 + " " + TG_TOKEN + " tail"
    assert text[:SEMANTIC_HEAD * 2].endswith(TG_TOKEN[:45]), "precondition: the cut lands inside it"
    out = snippet(text, None, semantic_only=True)
    assert TG_SECRET[:12] not in out, out


# --- query.terms: a pasted secret, lowercased and cut into pieces ------------------
# Terms are stems / normalized words: lowercased and split at `:`, `-`, `_`.
# Each piece passed `redact` on its own and no shape survives that (the mixed
# case of a Binance key is gone, a bot token is four pieces), so `--json` and
# MCP answered with the key spelled out in `query.terms`.

JWT = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U"


@pytest.mark.parametrize("query,secret,kept", [
    (f"ключ {BINANCE} выдан", BINANCE, "ключ"),
    (f"бот {TG_TOKEN} алерты", TG_TOKEN, "бот"),
    (f"сессия {JWT} истекла", JWT, "сесс"),
    ("export DB_PASSWORD=hunter2xyz", "hunter2xyz", "password"),
    (f"raw:ключ AND {BINANCE}", BINANCE, "ключ"),
    ("raw:password=hunterxyz*", "hunterxyz", "password"),
])
def test_query_terms_never_spell_a_pasted_secret(query, secret, kept):
    from kb.search import parse_query, shown_terms
    from kb.stem import TOKEN_RE, normalize, stem_word
    shown = shown_terms(parse_query(query))
    pieces = {p for p in TOKEN_RE.findall(normalize(secret)) if len(p) >= 4}
    pieces |= {stem_word(p) for p in pieces}
    assert not pieces & {t.rstrip("*") for t in shown}, shown
    assert "[redacted]" in shown and kept in shown, shown


def test_query_terms_without_a_secret_are_the_terms():
    from kb.search import parse_query, shown_terms
    for query in ("почему прод терял деньги на комиссиях", "raw:теря* AND szDecimals"):
        q = parse_query(query)
        assert shown_terms(q) == q.terms + [p + "*" for p in q.prefixes]


def test_cli_json_query_terms_carry_no_pasted_secret(corpus, capsys):
    assert cli.main(c(corpus) + ["update", "--lock-timeout", "0"]) == 0
    capsys.readouterr()
    assert cli.main(c(corpus) + ["search", f"ключ {BINANCE}", "--mode", "fts", "--json"]) == 0
    out = capsys.readouterr().out
    assert BINANCE.lower() not in out and BINANCE not in out
    assert json.loads(out)["query"]["terms"] == ["ключ", "[redacted]"]
