"""Schema v2: units, chunks, external-content FTS kept in sync by triggers."""

import hashlib
import sqlite3

import pytest

from kb import store
from kb.chunking import Piece
from kb.sources import Candidate


def cand(rel="a.md", sig=(1, 1), kind="note", project="p", source="hft-research"):
    return Candidate(source, rel, kind, project, f"/x/{rel}", list(sig), "2026-01-01T00:00:00Z",
                     parse=lambda: ([], {}), files=(f"/x/{rel}",), title=rel)


def P(anchor, text, loc="L1-1", title="t"):
    return Piece(anchor, title, text, loc)


@pytest.fixture()
def conn(tmp_path):
    c = store.open_rw(tmp_path / "kb.db")
    store.init_schema(c)
    yield c
    c.close()


def fts(conn, q):
    return [r[0] for r in conn.execute(
        "SELECT c.text FROM chunk_fts JOIN chunk c ON c.rowid = chunk_fts.rowid "
        "WHERE chunk_fts MATCH ?", (q,))]


def integrity(conn):
    # rank=1: also compare the index with the content table (catches a missing trigger)
    conn.execute("INSERT INTO chunk_fts(chunk_fts, rank) VALUES('integrity-check', 1)")


def test_schema_version_and_rollback_journal(conn, tmp_path):
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 2
    assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "delete"


def test_chunk_id_is_sha_of_source_rel_anchor():
    want = hashlib.sha256("repo:x\x1fdocs/a.md\x1fH1 › H2".encode()).hexdigest()[:16]
    assert store.chunk_id("repo:x", "docs/a.md", "H1 › H2") == want


def test_insert_then_diff_update_delete_keeps_fts_in_sync(conn):
    c = cand()
    store.write_unit(conn, c, [P("a", "альфа один"), P("b", "бета два"), P("c", "гамма три")], "sha1")
    rowids = dict(conn.execute("SELECT anchor, rowid FROM chunk"))
    stats = store.write_unit(conn, c, [P("a", "альфа один"), P("b", "бета ИЗМЕНЁН"), P("d", "дельта")], "sha2")
    assert {k: stats[k] for k in ("added", "updated", "deleted", "unchanged")} == \
        {"added": 1, "updated": 1, "deleted": 1, "unchanged": 1}
    after = dict(conn.execute("SELECT anchor, rowid FROM chunk"))
    assert after["a"] == rowids["a"] and after["b"] == rowids["b"], "same id -> same row"
    assert "c" not in after
    integrity(conn)
    assert fts(conn, "гамма") == []
    assert fts(conn, "ИЗМЕНЁН") == ["бета ИЗМЕНЁН"]
    assert fts(conn, "два") == []


def test_metadata_only_change_updates_the_row(conn):
    c = cand()
    store.write_unit(conn, c, [P("a", "текст", loc="L1-2")], "s")
    st = store.write_unit(conn, c, [P("a", "текст", loc="L5-6")], "s2")
    assert st["updated"] == 1
    assert conn.execute("SELECT loc FROM chunk").fetchone()[0] == "L5-6"
    integrity(conn)


def test_delete_unit_removes_chunks_and_fts_rows(conn):
    st = store.write_unit(conn, cand(), [P("a", "уникальное слово")], "s")
    store.delete_unit(conn, st["unit_id"])
    assert conn.execute("SELECT count(*) FROM chunk").fetchone()[0] == 0
    assert conn.execute("SELECT count(*) FROM unit").fetchone()[0] == 0
    integrity(conn)
    assert fts(conn, "уникальное") == []


def test_duplicate_anchor_in_one_unit_is_refused(conn):
    with pytest.raises(ValueError, match="duplicate"):
        store.write_unit(conn, cand(), [P("a", "x"), P("a", "y")], "s")
    assert conn.execute("SELECT count(*) FROM unit").fetchone()[0] == 0, "nothing half-written"


def test_text_sha_covers_title_and_text(conn):
    store.write_unit(conn, cand(), [P("a", "same", title="T1"), P("b", "same", title="T2")], "s")
    shas = [r[0] for r in conn.execute("SELECT text_sha FROM chunk ORDER BY anchor")]
    assert shas[0] != shas[1]
    assert shas[0] == hashlib.sha256("T1\x1fsame".encode()).hexdigest()


def test_unit_and_chunk_metadata_are_stored(conn):
    c = cand(kind="memory", project="hftbacktest")
    store.write_unit(conn, c, [Piece("a", "t", "body", "L3-4", date="2026-05-05T00:00:00Z")], "sha")
    row = conn.execute("SELECT kind, project, date, loc, ordinal, n_chars FROM chunk").fetchone()
    assert row == ("memory", "hftbacktest", "2026-05-05T00:00:00Z", "L3-4", 0, 4)
    u = conn.execute("SELECT source_id, rel, abs_path, signature, content_sha, parser_ver FROM unit").fetchone()
    assert u[:3] == ("hft-research", "a.md", "/x/a.md")
    assert u[3] == "[1, 1]" and u[4] == "sha" and u[5] >= 1


def test_v1_database_is_a_clean_error(tmp_path):
    p = tmp_path / "old.db"
    c = sqlite3.connect(p)
    c.execute("CREATE VIRTUAL TABLE kb USING fts5(text)")
    c.close()
    with pytest.raises(store.SchemaError, match="kb build --full"):
        store.check_schema(store.open_ro(p))


def test_readonly_connection_cannot_write(tmp_path, conn):
    ro = store.open_ro(tmp_path / "kb.db")
    with pytest.raises(sqlite3.OperationalError):
        ro.execute("INSERT INTO meta VALUES ('x', 'y')")
