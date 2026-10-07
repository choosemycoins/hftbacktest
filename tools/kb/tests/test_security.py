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
