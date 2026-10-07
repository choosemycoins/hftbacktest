"""Embedding store and pass, on the deterministic fake embedder (offline)."""

import os
import stat

import numpy as np
import pytest

from kb import embed, store, update
from kb import config as cfgmod
from kb.chunking import Piece
from kb.embed import FakeEmbedder, MatrixStore
from kb.sources import Candidate


def cand(rel, kind="note"):
    return Candidate("hft-research", rel, kind, "p", f"/x/{rel}", [1, 1], "2026-01-01T00:00:00Z",
                     parse=lambda: ([], {}), files=(), title=rel)


@pytest.fixture()
def db(tmp_path):
    d = tmp_path / "kbdir"
    d.mkdir()
    path = d / "kb.db"
    conn = store.open_rw(path)
    store.init_schema(conn)
    yield path, conn
    conn.close()


def emb_rows(conn, key="fake"):
    return dict(conn.execute("SELECT text_sha, row FROM emb_row WHERE model_key=?", (key,)))


def test_same_text_is_embedded_once(db):
    path, conn = db
    store.write_unit(conn, cand("a.md"), [Piece("x", "T", "одинаковый текст", "L1-1")], None)
    store.write_unit(conn, cand("b.md"), [Piece("x", "T", "одинаковый текст", "L1-1"),
                                          Piece("y", "T", "другой текст", "L2-2")], None)
    st = embed.embed_pass(conn, path, FakeEmbedder())
    assert st["embedded"] == 2
    assert len(emb_rows(conn)) == 2
    assert embed.embed_pass(conn, path, FakeEmbedder())["embedded"] == 0, "nothing pending"


def test_vectors_read_back_by_content_address(db):
    path, conn = db
    fe = FakeEmbedder()
    store.write_unit(conn, cand("a.md"), [Piece(f"s{i}", "T", f"текст номер {i} про комиссию", "L1-1")
                                          for i in range(5)], None)
    embed.embed_pass(conn, path, fe)
    ms = MatrixStore(path, "fake", fe.dim)
    m = ms.load(conn)
    for sha, row in emb_rows(conn).items():
        title, text = conn.execute("SELECT title, text FROM chunk WHERE text_sha=?", (sha,)).fetchone()
        want = fe.embed_passages([embed.passage_text(title, text)])[0]
        assert np.allclose(np.asarray(m[row], dtype=np.float32), want, atol=1e-3)


def test_an_interrupted_append_leaves_garbage_not_wrong_rows(db):
    path, conn = db
    fe = FakeEmbedder()
    store.write_unit(conn, cand("a.md"), [Piece("a", "T", "первый", "L1-1")], None)
    embed.embed_pass(conn, path, fe)
    ms = MatrixStore(path, "fake", fe.dim)
    with open(ms.path(0), "ab") as fh:           # crash: 1.5 rows written, nothing mapped
        fh.write(b"\x7f" * int(ms.row_bytes * 1.5))
    store.write_unit(conn, cand("b.md"), [Piece("b", "T", "второй", "L1-1")], None)
    embed.embed_pass(conn, path, fe)
    m = ms.load(conn)
    rows = emb_rows(conn)
    sha_b = conn.execute("SELECT text_sha FROM chunk WHERE text='второй'").fetchone()[0]
    assert rows[sha_b] == 2, "base = whole rows actually in the file (the cut row is dropped)"
    assert np.allclose(np.asarray(m[rows[sha_b]], np.float32),
                       fe.embed_passages([embed.passage_text("T", "второй")])[0], atol=1e-3)
    junk, total = ms.garbage(conn)
    assert (junk, total) == (1, 3)


def make_garbage(conn, path, n_dead=30, n_live=10):
    fe = FakeEmbedder()
    store.write_unit(conn, cand("dead.md"), [Piece(f"d{i}", "T", f"мёртвый {i}", "L1-1") for i in range(n_dead)], None)
    store.write_unit(conn, cand("live.md"), [Piece(f"l{i}", "T", f"живой {i}", "L1-1") for i in range(n_live)], None)
    embed.embed_pass(conn, path, fe)
    uid = conn.execute("SELECT unit_id FROM unit WHERE rel='dead.md'").fetchone()[0]
    store.delete_unit(conn, uid)
    return fe


def live_vectors_ok(conn, path, fe):
    ms = MatrixStore(path, "fake", fe.dim)
    m = ms.load(conn)
    for sha, row in emb_rows(conn).items():
        r = conn.execute("SELECT title, text FROM chunk WHERE text_sha=?", (sha,)).fetchone()
        if r is None:
            continue
        assert np.allclose(np.asarray(m[row], np.float32),
                           fe.embed_passages([embed.passage_text(*r)])[0], atol=1e-3)


def test_gc_writes_a_new_generation_with_live_rows_only(db):
    path, conn = db
    fe = make_garbage(conn, path)
    ms = MatrixStore(path, "fake", fe.dim)
    assert ms.gc(conn, force=True) == 30
    assert ms.gen(conn) == 1 and ms.path(1).exists() and not ms.path(0).exists()
    assert ms.load(conn).shape[0] == 10
    live_vectors_ok(conn, path, fe)


def test_gc_crash_before_commit_keeps_the_old_generation(db, monkeypatch):
    path, conn = db
    fe = make_garbage(conn, path)
    ms = MatrixStore(path, "fake", fe.dim)
    real = store.transaction

    def boom(c):
        raise RuntimeError("power cut")
    monkeypatch.setattr(store, "transaction", boom)
    with pytest.raises(RuntimeError):
        ms.gc(conn, force=True)
    monkeypatch.setattr(store, "transaction", real)
    assert ms.gen(conn) == 0
    live_vectors_ok(conn, path, fe)
    assert ms.clean_orphans(conn) == ["g1.f16"], "the half-made generation is an orphan"


def test_gc_crash_after_commit_leaves_only_an_orphan(db, monkeypatch):
    path, conn = db
    fe = make_garbage(conn, path)
    ms = MatrixStore(path, "fake", fe.dim)
    monkeypatch.setattr(type(ms.path(0)), "unlink", lambda self, *a: (_ for _ in ()).throw(OSError("crash")))
    with pytest.raises(OSError):
        ms.gc(conn, force=True)
    monkeypatch.undo()
    assert ms.gen(conn) == 1
    live_vectors_ok(conn, path, fe)
    assert ms.clean_orphans(conn) == ["g0.f16"]


def test_budget_and_max_chunks_stop_cleanly(db):
    path, conn = db
    store.write_unit(conn, cand("a.md"), [Piece(f"s{i}", "T", f"кусок {i}", "L1-1") for i in range(30)], None)
    st = embed.embed_pass(conn, path, FakeEmbedder(), budget_minutes=0)
    assert st["embedded"] == 0 and st["remaining"] == 30
    st = embed.embed_pass(conn, path, FakeEmbedder(), max_chunks=10)
    assert st["embedded"] == 10 and st["remaining"] == 20


def test_pending_follows_the_d14_order_newest_first(db):
    path, conn = db
    for i, kind in enumerate(("chat", "transcript", "note", "memory", "doc")):
        c = cand(f"{kind}.md", kind=kind)
        store.write_unit(conn, c, [Piece("old", "T", f"{kind} старый", "L1-1", date="2026-01-01T00:00:00Z"),
                                   Piece("new", "T", f"{kind} новый", "L1-1", date="2026-09-01T00:00:00Z")], None)
    order = [conn.execute("SELECT kind, text FROM chunk WHERE rowid=?", (r,)).fetchone()
             for _, r in embed.pending(conn, "fake")]
    assert [k for k, _ in order] == ["memory", "memory", "doc", "doc", "note", "note",
                                     "transcript", "transcript", "chat", "chat"]
    assert order[0][1] == "memory новый"


def test_matrix_and_dirs_are_private(db):
    path, conn = db
    store.write_unit(conn, cand("a.md"), [Piece("a", "T", "x", "L1-1")], None)
    old = os.umask(0o022)          # a permissive umask must not leak into the modes
    try:
        embed.embed_pass(conn, path, FakeEmbedder())
    finally:
        os.umask(old)
    ms = MatrixStore(path, "fake", 64)
    assert stat.S_IMODE(os.stat(ms.path(0)).st_mode) == 0o600
    assert stat.S_IMODE(os.stat(ms.dir).st_mode) == 0o700
    assert stat.S_IMODE(os.stat(ms.dir.parent).st_mode) == 0o700


def test_reparse_and_full_rebuild_reuse_vectors(corpus):
    cfg = cfgmod.load_config(corpus["config"])
    update.update(cfg, corpus["db"], warn=lambda m: None, lock_timeout=0)
    conn = store.open_rw(corpus["db"])
    first = embed.embed_pass(conn, corpus["db"], FakeEmbedder())
    assert first["embedded"] > 0
    conn.close()
    update.build_full(cfg, corpus["db"], warn=lambda m: None)
    conn = store.open_rw(corpus["db"])
    assert embed.embed_pass(conn, corpus["db"], FakeEmbedder())["embedded"] == 0, \
        "build --full carries emb_row over: no vector is recomputed"
    conn.close()


def test_parser_version_bump_does_not_re_embed(corpus, monkeypatch):
    from kb import sources
    cfg = cfgmod.load_config(corpus["config"])
    update.update(cfg, corpus["db"], warn=lambda m: None, lock_timeout=0)
    conn = store.open_rw(corpus["db"])
    embed.embed_pass(conn, corpus["db"], FakeEmbedder())
    conn.close()
    monkeypatch.setitem(sources.PARSER_VERSIONS, "note", 77)
    st = update.update(cfg, corpus["db"], warn=lambda m: None, lock_timeout=0)
    assert st["kinds"]["note"]["units_changed"] >= 4
    conn = store.open_rw(corpus["db"])
    assert embed.pending(conn, "fake") == [], "same text -> same text_sha -> the old vector"
    conn.close()


def test_cli_budget_exhausted_is_a_clean_exit_with_the_remainder(corpus, capsys):
    from kb import cli
    c = ["--config", str(corpus["config"])]
    assert cli.main(c + ["update", "--lock-timeout", "0"]) == 0
    capsys.readouterr()
    assert cli.main(c + ["embed", "--budget-minutes", "0"]) == 0
    out = capsys.readouterr().out
    assert "+0 in" in out and "remaining 0" not in out
