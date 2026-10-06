"""Full build of a synthetic corpus, then search / read / stats through the API
and the CLI."""

import json
import os
import stat

import pytest

from kb import cli, search, update
from kb import config as cfgmod
from kb.search import Index, QueryError

FAKE_KEY = "0x" + "7a" * 32


@pytest.fixture()
def built(corpus):
    cfg = cfgmod.load_config(corpus["config"])
    st = update.build_full(cfg, corpus["db"], warn=lambda m: None)
    return cfg, corpus["db"], st


def test_build_indexes_every_kind(built):
    _, db, st = built
    kinds = st["kinds"]
    for kind in ("transcript", "memory", "doc", "export", "note", "course", "chat", "commit"):
        assert kinds[kind]["added"] > 0, kind
    assert st["missing_roots"] == []
    assert db.exists()


def test_db_is_0600_and_dir_0700(built):
    _, db, _ = built
    assert stat.S_IMODE(os.stat(db).st_mode) == 0o600
    assert stat.S_IMODE(os.stat(db.parent).st_mode) == 0o700


def test_build_is_atomic_old_db_survives_a_failure(built, monkeypatch):
    cfg, db, _ = built
    before = db.read_bytes()

    def boom(*a, **kw):
        raise RuntimeError("source exploded")
    monkeypatch.setattr(update.sources, "scan_all", boom)
    with pytest.raises(RuntimeError):
        update.build_full(cfg, db, warn=lambda m: None)
    assert db.read_bytes() == before
    leftovers = [p.name for p in db.parent.iterdir() if p.name not in (db.name, ".kbindex-dir")]
    assert leftovers == []


def test_one_unparsable_unit_does_not_sink_the_build(corpus, monkeypatch):
    bad = corpus["research"] / "broken.md"
    bad.write_text("# fine\n\nbut the parser will explode on this file\n")
    real = update.sources._parse_md

    def flaky(path, name):
        if name == "broken.md":
            raise UnicodeDecodeError("utf-8", b"x", 0, 1, "boom")
        return real(path, name)
    monkeypatch.setattr(update.sources, "_parse_md", flaky)
    warnings = []
    cfg = cfgmod.load_config(corpus["config"])
    st = update.build_full(cfg, corpus["db"], warn=warnings.append)
    assert st["kinds"]["note"]["failed"] == 1
    assert any("broken.md" in w for w in warnings)
    assert st["kinds"]["note"]["added"] > 0


def S(db, q, **kw):
    return search.search(Index(db), q, **kw)


def wheres(resp):
    return [r["where"] for r in resp["results"]]


def test_natural_query_finds_whole_words_across_the_corpus(built):
    _, db, _ = built
    resp = S(db, "монетизирует латентность")
    assert resp["results"]
    assert resp["results"][0]["kind"] == "transcript"
    assert "[монетизирует]" in resp["results"][0]["snippet"]


def test_raw_query_is_fts5_syntax_over_title_and_text(built):
    _, db, _ = built
    assert S(db, 'raw:"монетизирует латентность"')["results"]
    assert not S(db, 'raw:"латентность монетизирует"')["results"]
    assert S(db, "raw:монетиз*")["results"]
    with pytest.raises(QueryError):
        S(db, 'raw:unbalanced "quote')


def test_filters_apply_inside_the_ranking(built):
    _, db, _ = built
    resp = S(db, "Lighter латентность комиссия троттл", kind="memory")
    assert resp["results"] and all(r["kind"] == "memory" for r in resp["results"])
    resp = S(db, "троттл", kind="export,memory")
    assert {r["kind"] for r in resp["results"]} <= {"export", "memory"}
    assert all(r["project"] == "deribit-ru" for r in S(db, "matching", project="deribit-ru")["results"])
    assert not S(db, "монетизирует", since="2026-08-10")["results"]
    with pytest.raises(QueryError):
        S(db, "x", kind="nonsense")


def test_where_points_at_the_source(built, corpus):
    _, db, _ = built
    r = S(db, "matching engine", kind="chat")["results"][0]
    assert r["where"] == f"{corpus['tg']}/ChatExport_2026-09-14 (1)/messages.html#message213023"
    c = S(db, "добавил коллектор", kind="commit")["results"][0]
    assert c["where"].startswith(f"{corpus['repo']}@") and len(c["where"].rsplit("@", 1)[1]) == 40
    d = S(db, "truncation trap", kind="doc")["results"][0]
    assert d["where"] == f"{corpus['repo']}/docs/design-x.md#L1-3"
    a = S(db, "фиксируем позиции", kind="course")["results"][0]
    assert a["where"].endswith("13.Риск-менеджмент.json#t=01:35-01:39")


def test_read_resolves_ids_and_wheres_only_through_the_index(built, corpus):
    _, db, _ = built
    idx = Index(db)
    hit = S(db, "matching engine")["results"][0]
    by_id = search.read_pointer(idx, hit["id"])
    by_where = search.read_pointer(idx, hit["where"])
    assert by_id["text"] == by_where["text"]
    assert "5/s" in by_id["text"]
    assert by_id["stale"] is None, "chats have no single-file stat"
    for bad in ("/etc/passwd", f"{corpus['repo']}/../../etc/passwd#L1-1", "~/.ssh/id_rsa#L1",
                "kb:0000000000000000"):
        with pytest.raises(QueryError):
            search.read_pointer(idx, bad)


def test_read_marks_a_changed_source_stale_and_returns_neighbours(built, corpus):
    _, db, _ = built
    idx = Index(db)
    hit = S(db, "Коллектор мержит", kind="doc")["results"][0]
    assert search.read_pointer(idx, hit["id"])["stale"] is False
    agents = corpus["repo"] / "AGENTS.md"
    agents.write_text(agents.read_text() + "\nновая строка\n")
    assert search.read_pointer(idx, hit["id"])["stale"] is True


def test_read_caps_the_answer_and_cuts_neighbours_first(corpus):
    big = corpus["research"] / "big-notes.md"
    big.write_text("".join(f"# Раздел {i}\n\n" + ("слово%d " % i) * 200 + "\n\n" for i in range(40)))
    cfg = cfgmod.load_config(corpus["config"])
    update.build_full(cfg, corpus["db"], warn=lambda m: None)
    idx = Index(corpus["db"])
    hit = S(corpus["db"], "слово20", kind="note")["results"][0]
    r = search.read_pointer(idx, hit["id"], context=5)
    total = len(r["text"]) + sum(map(len, r["before"])) + sum(map(len, r["after"]))
    assert total <= search.READ_CAP
    assert len(r["before"]) <= 5 and len(r["after"]) <= 5
    assert "слово20" in r["text"]


def test_dedup_by_text_sha_and_at_most_three_per_unit(corpus):
    slug = corpus["slug"]
    lines = "".join(json.dumps({"type": "user", "uuid": f"d{i}", "timestamp": f"2026-08-0{i % 9 + 1}T00:00:00Z",
                                "message": {"role": "user", "content": f"повтор дубль {i}"}}) + "\n"
                    for i in range(8))
    (slug / "many.jsonl").write_text(lines)
    (slug / "dup.jsonl").write_text(json.dumps({"type": "user", "uuid": "x", "timestamp": "2026-08-01T00:00:00Z",
                                                "message": {"role": "user", "content": "почему прод терял деньги на комиссиях?"}}) + "\n")
    cfg = cfgmod.load_config(corpus["config"])
    update.build_full(cfg, corpus["db"], warn=lambda m: None)
    resp = S(corpus["db"], "повтор дубль", limit=50)
    per_unit = {}
    for r in resp["results"]:
        per_unit[r["where"].split("#")[0]] = per_unit.get(r["where"].split("#")[0], 0) + 1
    assert max(per_unit.values()) <= 3
    same = S(corpus["db"], "почему прод терял деньги на комиссиях", kind="transcript", limit=50)
    texts = [r["snippet"] for r in same["results"] if "терял" in r["snippet"]]
    assert len(texts) == 1, "the same text twice collapses to its earliest occurrence"


def test_response_shape_and_coverage(built):
    _, db, _ = built
    resp = S(db, "троттл")
    assert set(resp) >= {"notice", "index", "query", "coverage", "degraded", "results"}
    assert "pointers, verify at source" in resp["notice"]
    assert resp["coverage"]["in_scope"] > 0
    assert resp["index"]["updated_at"]


# --- CLI ----------------------------------------------------------------------

def test_cli_build_search_read_stats(corpus, capsys):
    rc = cli.main(["--config", str(corpus["config"]), "build", "--full"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "transcript" in out and "never commit" in out
    assert cli.main(["--config", str(corpus["config"]), "search", "matching engine"]) == 0
    out = capsys.readouterr().out
    assert "pointers, verify at source" in out and "chat | deribit-ru" in out
    kid = out.split("(kb:")[1].split(")")[0]
    assert cli.main(["--config", str(corpus["config"]), "read", f"kb:{kid}", "--json"]) == 0
    assert "5/s" in json.loads(capsys.readouterr().out)["text"]
    assert cli.main(["--config", str(corpus["config"]), "stats", "--check"]) == 0
    out = capsys.readouterr().out
    assert "FTS integrity ok" in out
    assert "ChatExport_2026-10-01" in out, "unclassified exports are visible"


def test_cli_output_is_masked_but_the_index_is_not(corpus, capsys):
    (corpus["slug"] / "key.jsonl").write_text(json.dumps({
        "type": "user", "uuid": "k", "timestamp": "2026-07-28T14:51:15Z",
        "message": {"role": "user", "content": f"ключи API Wallet {FAKE_KEY} готово"}}) + "\n")
    assert cli.main(["--config", str(corpus["config"]), "build", "--full"]) == 0
    capsys.readouterr()
    for args in (["search", "ключи API Wallet"], ["search", "ключи API Wallet", "--json"]):
        assert cli.main(["--config", str(corpus["config"]), *args]) == 0
        out = capsys.readouterr().out
        assert FAKE_KEY not in out and "redacted" in out
    resp = S(corpus["db"], "ключи API Wallet")
    hit = search.read_pointer(Index(corpus["db"]), resp["results"][0]["id"])
    assert FAKE_KEY in hit["text"], "masking is a presentation rule; the API stays honest"


def test_cli_bad_query_and_v1_db_are_clean_errors(corpus, tmp_path, capsys):
    import sqlite3
    old = tmp_path / "v1.db"
    c = sqlite3.connect(old)
    c.execute("CREATE VIRTUAL TABLE kb USING fts5(text)")
    c.close()
    assert cli.main(["--config", str(corpus["config"]), "--db", str(old), "search", "x"]) == 2
    assert "kb build --full" in capsys.readouterr().err
    assert cli.main(["--config", str(corpus["config"]), "--db", str(tmp_path / "none.db"),
                     "search", "x"]) == 2


def test_cli_missing_root_exits_3(corpus):
    import shutil
    shutil.rmtree(corpus["asr"])
    assert cli.main(["--config", str(corpus["config"]), "build", "--full"]) == 3


def test_help_documents_the_contract():
    doc = cli.__doc__.lower()
    for needle in ("pointer", "0600", "0700", "never commit", "mask", "not a guarantee",
                   "fetch-model", "raw:", "not evidence", "allowlist"):
        assert needle in doc, needle
