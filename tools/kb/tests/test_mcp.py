"""The MCP server: in-process client for the tools, a real stdio subprocess
for the transport, masking on every path out, no network."""

import json
import os
import subprocess
import sys
from pathlib import Path

import anyio
import pytest

from kb import embed, mcp_server, semantic, store, update
from kb import config as cfgmod
from kb.embed import FakeEmbedder
from kb.search import Index

FAKE_KEY = "0x" + "7a" * 32
FAKE_BINANCE = "vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A"
KB_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture()
def served(corpus):
    (corpus["slug"] / "keys.jsonl").write_text(json.dumps({
        "type": "user", "uuid": "k1", "timestamp": "2026-07-28T14:51:15Z",
        "message": {"role": "user", "content":
                    f"ключи API Wallet {FAKE_KEY} и Binance secret {FAKE_BINANCE} готово"}}) + "\n")
    big = corpus["research"] / "huge.md"
    big.write_text("".join(f"# Раздел {i}\n\n" + ("огромный%d " % i) * 160 + "\n\n" for i in range(60)))
    cfg = cfgmod.load_config(corpus["config"])
    update.update(cfg, corpus["db"], warn=lambda m: None, lock_timeout=0)
    fe = FakeEmbedder()
    conn = store.open_rw(corpus["db"])
    embed.embed_pass(conn, corpus["db"], fe)
    conn.close()
    idx = Index(corpus["db"])
    eng = semantic.Engine(idx, "fake", fe.dim, embedder=fe)
    return mcp_server.build_server(idx, eng), corpus


def call(server, name, args):
    from mcp import Client

    async def main():
        async with Client(server) as c:
            return await c.call_tool(name, args)
    return anyio.run(main)


def payload(result):
    assert not result.is_error, result.content[0].text
    return json.loads(result.content[0].text)


def test_exactly_two_tools(served):
    from mcp import Client
    server, _ = served

    async def main():
        async with Client(server) as c:
            return await c.list_tools()
    tools = anyio.run(main)
    assert sorted(t.name for t in tools.tools) == ["kb_read", "kb_search"]
    schema = {t.name: t.input_schema for t in tools.tools}
    assert set(schema["kb_search"]["properties"]) == {"query", "mode", "kind", "project", "since", "limit"}
    assert set(schema["kb_read"]["properties"]) == {"pointer", "context"}


def test_search_answers_with_pointers_coverage_and_notice(served):
    server, _ = served
    resp = payload(call(server, "kb_search", {"query": "монетизирует латентность"}))
    assert "pointers, verify at source" in resp["notice"]
    assert resp["query"]["used"] == "hybrid"
    assert resp["coverage"]["in_scope"] > 0
    assert resp["results"][0]["id"].startswith("kb:")


def test_every_string_out_of_search_and_read_is_masked(served):
    server, _ = served
    raw = call(server, "kb_search", {"query": "ключи API Wallet Binance secret", "mode": "fts"})
    text = raw.content[0].text
    assert FAKE_KEY not in text and FAKE_BINANCE not in text and "redacted" in text
    hit = json.loads(text)["results"][0]
    read = call(server, "kb_read", {"pointer": hit["id"], "context": 2}).content[0].text
    assert FAKE_KEY not in read and FAKE_BINANCE not in read
    assert "redacted" in read


def test_read_refuses_what_is_not_indexed(served):
    server, corpus = served
    for bad in ("/etc/passwd", f"{corpus['repo']}/../../../etc/passwd#L1-1", "~/.ssh/id_rsa",
                f"{corpus['home']}/.config/kb/kb.toml#L1-5", "kb:ffffffffffffffff"):
        r = call(server, "kb_read", {"pointer": bad})
        assert r.is_error and "not an indexed pointer" in r.content[0].text


def test_read_is_capped_and_neighbours_are_cut_first(served, monkeypatch):
    from kb import search
    server, _ = served
    hit = payload(call(server, "kb_search", {"query": "огромный30", "mode": "fts"}))["results"][0]
    full = payload(call(server, "kb_read", {"pointer": hit["id"], "context": 0}))["text"]
    r = payload(call(server, "kb_read", {"pointer": hit["id"], "context": 5}))
    total = len(r["text"]) + sum(map(len, r["before"])) + sum(map(len, r["after"]))
    assert total <= search.READ_CAP and len(r["before"]) == len(r["after"]) == 5
    monkeypatch.setattr(search, "READ_CAP", len(full) + 500)
    r = payload(call(server, "kb_read", {"pointer": hit["id"], "context": 5}))
    total = len(r["text"]) + sum(map(len, r["before"])) + sum(map(len, r["after"]))
    assert r["text"] == full, "the chunk itself is never cut for its neighbours"
    assert total <= len(full) + 500


def test_bad_arguments_are_clean_masked_errors(served):
    server, _ = served
    r = call(server, "kb_search", {"query": f'raw:"unbalanced {FAKE_KEY}'})
    assert r.is_error and FAKE_KEY not in r.content[0].text
    r = call(server, "kb_search", {"query": "x", "kind": "nonsense"})
    assert r.is_error and "unknown kind" in r.content[0].text
    big = payload(call(server, "kb_search", {"query": "огромный1", "limit": 500, "mode": "fts"}))
    assert len(big["results"]) <= 50


def test_the_module_has_no_network_transport():
    src = Path(mcp_server.__file__).read_text()
    for needle in ('"sse"', "streamable", "uvicorn", "host=", "port=", "run_sse", "http"):
        assert needle not in src, needle


# --- the real process -----------------------------------------------------------

def lo_only(tmp_path):
    p = tmp_path / "net_dev"
    p.write_text("Inter-|   Receive\n face |bytes\n    lo: 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0\n")
    return p


def kb_proc(corpus, tmp_path, net_dev, extra_env=None):
    env = dict(os.environ, KB_PROC_NET_DEV=str(net_dev), PYTHONPATH=str(KB_ROOT), **(extra_env or {}))
    return subprocess.Popen([sys.executable, "-m", "kb", "--config", str(corpus["config"]), "mcp"],
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            env=env, cwd=str(tmp_path))


def exchange(p, msgs, until_id, timeout=120) -> bytes:
    """Send all requests, keep stdin open until the response `until_id`
    arrives (EOF on stdin ends the session), then close and drain."""
    import threading
    got = []
    done = threading.Event()

    def reader():
        for line in p.stdout:
            got.append(line)
            try:
                if json.loads(line).get("id") == until_id:
                    done.set()
            except ValueError:
                pass
        done.set()
    t = threading.Thread(target=reader, daemon=True)
    t.start()
    p.stdin.write("".join(json.dumps(m) + "\n" for m in msgs).encode())
    p.stdin.flush()
    done.wait(timeout)
    p.stdin.close()
    p.wait(timeout=60)
    t.join(10)
    return b"".join(got)


def test_stdio_server_speaks_only_json_rpc_on_stdout(served, tmp_path):
    _, corpus = served
    p = kb_proc(corpus, tmp_path, lo_only(tmp_path))
    msgs = [
        {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
            "protocolVersion": "2025-06-18", "capabilities": {},
            "clientInfo": {"name": "t", "version": "0"}}},
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
        {"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
        {"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {
            "name": "kb_search", "arguments": {"query": "ключи API Wallet", "mode": "fts"}}},
    ]
    out = exchange(p, msgs, until_id=3)
    lines = [l for l in out.decode().splitlines() if l.strip()]
    frames = [json.loads(l) for l in lines]           # every stdout line parses
    assert all(f.get("jsonrpc") == "2.0" for f in frames)
    by_id = {f.get("id"): f for f in frames}
    assert {t["name"] for t in by_id[2]["result"]["tools"]} == {"kb_search", "kb_read"}
    text = by_id[3]["result"]["content"][0]["text"]
    assert "redacted" in text and FAKE_KEY not in out.decode()


def test_stray_writes_after_protection_go_to_stderr(tmp_path):
    script = tmp_path / "s.py"
    script.write_text(
        "import os, sys\n"
        f"sys.path.insert(0, {str(KB_ROOT)!r})\n"
        "from kb.mcp_server import protect_stdio\n"
        "wire_in, wire_out = protect_stdio()\n"
        "print('STRAY PRINT')\n"
        "os.write(1, b'STRAY FD1\\n')\n"
        "wire_out.write(b'{\"jsonrpc\": \"2.0\"}\\n')\n")
    r = subprocess.run([sys.executable, str(script)], capture_output=True, timeout=60)
    assert r.stdout == b'{"jsonrpc": "2.0"}\n'
    assert b"STRAY PRINT" in r.stderr and b"STRAY FD1" in r.stderr


def test_refuses_to_start_with_a_network(served, tmp_path):
    _, corpus = served
    net = tmp_path / "net_dev_eth"
    net.write_text("Inter-|\n face |\n    lo: 0 0\n  eth0: 1 2\n")
    p = kb_proc(corpus, tmp_path, net)
    out, err = p.communicate(b"", timeout=60)
    assert p.returncode == 2
    assert b"unshare -rn" in err and b"eth0" in err
    assert out == b""


def test_titles_and_paths_are_masked_too_not_only_snippets(corpus):
    """The snippet is masked inside search(); a secret in a HEADING reaches the
    caller through `title` — only the outer mask stops it."""
    (corpus["research"] / "leak.md").write_text(f"# Ключ {FAKE_KEY}\n\nтекст про ротацию ключей агента\n")
    cfg = cfgmod.load_config(corpus["config"])
    update.update(cfg, corpus["db"], warn=lambda m: None, lock_timeout=0)
    server = mcp_server.build_server(Index(corpus["db"]), None)
    text = call(server, "kb_search", {"query": "ротацию ключей агента", "mode": "fts"}).content[0].text
    assert "leak.md" in text and FAKE_KEY not in text


def test_sdk_stdio_client_smoke(served, tmp_path):
    """The SDK's own stdio client against `python -m kb mcp`."""
    from mcp import Client, StdioServerParameters
    _, corpus = served
    params = StdioServerParameters(
        command=sys.executable, args=["-m", "kb", "--config", str(corpus["config"]), "mcp"],
        env=dict(os.environ, KB_PROC_NET_DEV=str(lo_only(tmp_path)), PYTHONPATH=str(KB_ROOT)),
        cwd=str(tmp_path))

    async def main():
        async with Client(params) as c:
            tools = await c.list_tools()
            r = await c.call_tool("kb_search", {"query": "matching engine", "mode": "fts"})
            return tools, r
    tools, r = anyio.run(main)
    assert {t.name for t in tools.tools} == {"kb_search", "kb_read"}
    assert not r.is_error and "deribit-ru" in r.content[0].text


def test_a_secret_split_by_the_snippet_window_does_not_leak_through_kb_search(corpus):
    """End to end: the window of an fts hit starts right after the bot id."""
    secret = "AAHdq-TcvCH1vGWJxfSe_ofSAs0K5PALDs1"
    filler = " ".join(f"w{i}" for i in range(33))
    (corpus["slug"] / "tg.jsonl").write_text(json.dumps({
        "type": "user", "uuid": "tg1", "timestamp": "2026-08-01T00:00:00Z",
        "message": {"role": "user", "content": f"intro 1234567890:{secret} {filler} alert bot alert bot"}})
        + "\n")
    cfg = cfgmod.load_config(corpus["config"])
    update.update(cfg, corpus["db"], warn=lambda m: None, lock_timeout=0)
    server = mcp_server.build_server(Index(corpus["db"]), None)
    text = call(server, "kb_search", {"query": "alert bot", "mode": "fts"}).content[0].text
    assert "tg.jsonl" in text
    assert secret[:12] not in text and secret[-12:] not in text
