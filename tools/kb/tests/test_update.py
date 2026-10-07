"""Incremental `kb update`: signatures, sha, diff by chunk_id, deletions and
their guards, the writer lock and the resource gates."""

import fcntl
import json
import os
import shutil
import sqlite3
import time

import pytest

from kb import cli, store, update
from kb import config as cfgmod
from kb import sources


def cfg_of(corpus):
    return cfgmod.load_config(corpus["config"])


def run(corpus, **kw):
    kw.setdefault("warn", lambda m: None)
    kw.setdefault("lock_timeout", 0)
    return update.update(cfg_of(corpus), corpus["db"], **kw)


def chunks(db, sql="SELECT text FROM chunk", args=()):
    c = store.open_ro(db)
    try:
        return c.execute(sql, args).fetchall()
    finally:
        c.close()


def fts(db, q):
    return [r[0] for r in chunks(db, "SELECT c.text FROM chunk_fts JOIN chunk c ON c.rowid = chunk_fts.rowid "
                                     "WHERE chunk_fts MATCH ?", (q,))]


def integrity(db):
    c = store.open_rw(db)
    c.execute("INSERT INTO chunk_fts(chunk_fts, rank) VALUES('integrity-check', 1)")
    c.close()


@pytest.fixture()
def counted(monkeypatch):
    """Count parses per relative path (wraps every candidate's parse)."""
    seen = []
    real = sources.scan_all

    def spy_scan_all(cfg):
        scans = real(cfg)
        for scan in scans:
            for c in scan.candidates:
                def parse(inner=c.parse, rel=c.rel):
                    seen.append(rel)
                    return inner()
                c.parse = parse
        return scans
    monkeypatch.setattr(sources, "scan_all", spy_scan_all)
    return seen


def test_the_parse_spy_sees_parses(corpus, counted):
    run(corpus)
    assert "book-notes.md" in counted, "guards the spy itself: the skip tests are not vacuous"


def test_first_update_on_an_empty_db_indexes_everything(corpus):
    st = run(corpus)
    assert st["exit"] == 0
    assert st["kinds"]["note"]["units_new"] >= 4
    assert fts(corpus["db"], "латентность")
    integrity(corpus["db"])


def test_unchanged_units_are_skipped_without_parsing(corpus, counted):
    run(corpus)
    counted.clear()
    st = run(corpus)
    assert counted == [], "same (size, mtime_ns) and parser_ver -> no parse at all"
    assert st["kinds"]["note"]["units_skipped"] >= 4


def test_touched_file_with_same_bytes_is_not_reparsed(corpus, counted):
    run(corpus)
    f = corpus["research"] / "book-notes.md"
    os.utime(f, ns=(time.time_ns(), time.time_ns() + 10**9))
    counted.clear()
    st = run(corpus)
    assert "book-notes.md" not in counted, "sha256 equal -> signature refreshed, no parse"
    assert st["kinds"]["note"]["units_touched"] == 1
    sig = chunks(corpus["db"], "SELECT signature FROM unit WHERE rel='book-notes.md'")[0][0]
    stt = os.stat(f)
    assert json.loads(sig) == [stt.st_size, stt.st_mtime_ns]


def test_changed_file_is_diffed_by_chunk_id(corpus):
    run(corpus)
    f = corpus["research"] / "book-notes.md"
    f.write_text("# Book\n\nanytime-valid methods are absent. ДОБАВЛЕНО НОВОЕ\n")
    before = dict(chunks(corpus["db"], "SELECT anchor, rowid FROM chunk JOIN unit USING(unit_id) "
                                       "WHERE rel='book-notes.md'"))
    st = run(corpus)
    after = dict(chunks(corpus["db"], "SELECT anchor, rowid FROM chunk JOIN unit USING(unit_id) "
                                      "WHERE rel='book-notes.md'"))
    assert st["kinds"]["note"]["units_changed"] == 1 and st["kinds"]["note"]["updated"] == 1
    assert before == after, "same anchor -> same row (and the same kb:<id>)"
    assert fts(corpus["db"], "ДОБАВЛЕНО")
    integrity(corpus["db"])


def test_deleted_file_is_deleted_from_the_index(corpus):
    run(corpus)
    (corpus["research"] / "book-notes.md").unlink()
    st = run(corpus)
    assert st["kinds"]["note"]["units_deleted"] == 1
    assert not fts(corpus["db"], "anytime")
    integrity(corpus["db"])


def test_missing_root_deletes_nothing_and_exits_3(corpus):
    run(corpus)
    n = len(chunks(corpus["db"], "SELECT 1 FROM chunk WHERE kind='course'"))
    shutil.rmtree(corpus["asr"])
    st = run(corpus)
    assert st["exit"] == 3
    assert len(chunks(corpus["db"], "SELECT 1 FROM chunk WHERE kind='course'")) == n
    assert any("transcripts" in m for m in st["missing_roots"])


def test_root_that_lost_every_unit_counts_as_missing(corpus):
    run(corpus)
    for p in corpus["asr"].iterdir():
        p.unlink()
    st = run(corpus)
    assert st["exit"] == 3
    assert chunks(corpus["db"], "SELECT 1 FROM unit WHERE source_id='course:cryptology10'")


def test_mass_delete_is_refused_without_the_flag(corpus):
    d = corpus["research"] / "many"
    d.mkdir()
    for i in range(120):
        (d / f"n{i}.md").write_text(f"# Note {i}\n\nтекст заметки номер {i} достаточно длинный\n")
    run(corpus)
    for i in range(60):
        (d / f"n{i}.md").unlink()
    st = run(corpus)
    assert st["exit"] == 3 and "hft-research" in st["refused"]
    assert len(chunks(corpus["db"], "SELECT 1 FROM unit WHERE rel LIKE 'many/%'")) == 120
    st = run(corpus, allow_mass_delete=True)
    assert st["exit"] == 0
    assert len(chunks(corpus["db"], "SELECT 1 FROM unit WHERE rel LIKE 'many/%'")) == 60


def test_chat_removed_from_the_allowlist_is_deleted_without_a_guard(corpus):
    run(corpus)
    assert chunks(corpus["db"], "SELECT 1 FROM chunk WHERE kind='chat'")
    p = corpus["config"]
    p.write_text(p.read_text().replace(
        'allow = [{ id = "deribit-ru", exports = ["ChatExport_2026-09-14 (1)"] }]', 'allow = []'))
    st = run(corpus)
    assert st["exit"] == 0
    assert not chunks(corpus["db"], "SELECT 1 FROM chunk WHERE kind='chat'")
    assert st["removed_sources"] == ["chat:deribit-ru"]


def test_parser_version_bump_rereads_the_kind(corpus, counted, monkeypatch):
    run(corpus)
    counted.clear()
    monkeypatch.setitem(sources.PARSER_VERSIONS, "memory", 99)
    run(corpus)
    assert counted == ["-home-andrew-RustroverProjects-hftbacktest/memory/reference_kbindex.md"]
    assert chunks(corpus["db"], "SELECT DISTINCT parser_ver FROM unit WHERE kind='memory'") == [(99,)]


def test_transcript_moved_to_the_mirror_keeps_its_id(corpus, tmp_path, counted):
    mirror = tmp_path / "mirror"
    p = corpus["config"]
    p.write_text(p.read_text().replace(f'root = "{corpus["claude"]}"',
                                       f'root = "{corpus["claude"]}"\nretain_from = "{mirror}"'))
    src = corpus["slug"] / "s1.jsonl"
    dst = mirror / corpus["slug"].name / "s1.jsonl"
    dst.parent.mkdir(parents=True)
    shutil.copy2(src, dst)
    run(corpus)
    ids = chunks(corpus["db"], "SELECT chunk_id FROM chunk JOIN unit USING(unit_id) WHERE rel LIKE '%/s1.jsonl'")
    src.unlink()                                # Claude Code cleanup
    counted.clear()
    st = run(corpus)
    assert st["exit"] == 0
    assert chunks(corpus["db"], "SELECT chunk_id FROM chunk JOIN unit USING(unit_id) "
                                "WHERE rel LIKE '%/s1.jsonl'") == ids
    assert chunks(corpus["db"], "SELECT abs_path FROM unit WHERE rel LIKE '%/s1.jsonl'")[0][0] == str(dst)
    assert not any(r.endswith("/s1.jsonl") for r in counted), "same bytes: no re-parse"


def test_alias_change_moves_chunks_to_the_new_project(corpus):
    run(corpus)
    p = corpus["config"]
    p.write_text(p.read_text().replace('= "eacc-review"', '= "eacc-forecasts"'))
    run(corpus)
    assert chunks(corpus["db"], "SELECT DISTINCT project FROM chunk WHERE text LIKE '%e/acc%'") == \
        [("eacc-forecasts",)]


def test_a_second_writer_does_not_wait_when_told_not_to(corpus):
    run(corpus)
    lock = corpus["db"].parent / "kb.lock"
    fd = os.open(lock, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(fd, fcntl.LOCK_EX)
    try:
        st = run(corpus, lock_timeout=0)
        assert st["exit"] == 0 and st["busy"] is True
    finally:
        os.close(fd)


def test_free_space_guard_stops_before_any_write(corpus, monkeypatch):
    class V:
        f_bavail, f_frsize = 1, 4096
    monkeypatch.setattr(update.os, "statvfs", lambda p: V)
    st = run(corpus, min_free_gb=5)
    assert st["exit"] == 3 and "free" in st["reason"]
    assert not corpus["db"].exists()


def test_psi_gate_defers_with_75(corpus, tmp_path):
    psi = tmp_path / "memory.pressure"
    psi.write_text("some avg10=63.10 avg60=40.00 avg300=10.00 total=1\nfull avg10=1.0 avg60=0 avg300=0 total=1\n")
    st = run(corpus, max_psi=20, psi_wait_minutes=0, psi_path=psi)
    assert st["exit"] == 75
    assert not corpus["db"].exists()
    psi.write_text("some avg10=3.10 avg60=4.00 avg300=1.00 total=1\n")
    assert run(corpus, max_psi=20, psi_wait_minutes=0, psi_path=psi)["exit"] == 0


def test_summary_never_carries_chunk_text(corpus, capsys):
    assert cli.main(["--config", str(corpus["config"]), "update", "--lock-timeout", "0"]) == 0
    out = capsys.readouterr()
    assert "латентность" not in out.out + out.err
    assert "note" in out.out


def test_lock_file_is_0600(corpus):
    import stat
    run(corpus)
    assert stat.S_IMODE(os.stat(corpus["db"].parent / "kb.lock").st_mode) == 0o600


def test_unmounted_mirror_keeps_mirror_only_transcripts(corpus, tmp_path):
    """Live root present, retain_from missing: the source still has candidates,
    so only the root check stands between an unmounted disk and deletion."""
    mirror = tmp_path / "mirror"
    p = corpus["config"]
    p.write_text(p.read_text().replace(f'root = "{corpus["claude"]}"',
                                       f'root = "{corpus["claude"]}"\nretain_from = "{mirror}"'))
    only = mirror / corpus["slug"].name / "old-session.jsonl"
    only.parent.mkdir(parents=True)
    only.write_text(json.dumps({"type": "user", "uuid": "o", "timestamp": "2026-01-01T00:00:00Z",
                                "message": {"role": "user", "content": "только в зеркале"}}) + "\n")
    run(corpus)
    shutil.rmtree(mirror)
    st = run(corpus)
    assert st["exit"] == 3
    assert fts(corpus["db"], "зеркале"), "a transcript kept only by the mirror must survive an unmount"


def test_a_new_commit_touches_no_existing_commit_row(corpus):
    """`git log --all` lists newest first; a new commit must not shift the
    ordinal (and so rewrite the row) of every older one."""
    from conftest import git
    run(corpus)
    (corpus["repo"] / "n.txt").write_text("n")
    git(corpus["repo"], "add", "n.txt")
    git(corpus["repo"], "commit", "-q", "-m", "третий коммит")
    st = run(corpus)
    assert st["kinds"]["commit"]["added"] == 1
    assert st["kinds"]["commit"].get("updated", 0) == 0


def test_stats_reports_refusals_and_an_open_config(corpus, capsys):
    d = corpus["research"] / "many"
    d.mkdir()
    for i in range(120):
        (d / f"n{i}.md").write_text(f"# Note {i}\n\nтекст заметки номер {i} достаточно длинный\n")
    run(corpus)
    for i in range(60):
        (d / f"n{i}.md").unlink()
    run(corpus)
    os.chmod(corpus["config"], 0o644)
    assert cli.main(["--config", str(corpus["config"]), "stats"]) == 3
    out = capsys.readouterr().out
    assert "MASS DELETE REFUSED (last run): hft-research" in out
    assert "chmod 600" in out
    os.chmod(corpus["config"], 0o600)
    assert cli.main(["--config", str(corpus["config"]), "stats"]) == 0


def test_psi_path_comes_from_the_cgroup_not_the_uid():
    """Under `unshare -r` getuid() is 0; the cgroup still names user@1000."""
    text = "0::/user.slice/user-1000.slice/user@1000.service/app.slice/kb-update.service\n"
    assert str(update.psi_path_from_cgroup(text)) == \
        "/sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/memory.pressure"
    assert update.psi_path_from_cgroup("0::/system.slice/cron.service\n") is None


def test_build_and_gc_report_a_held_lock_instead_of_a_traceback(corpus, capsys):
    run(corpus)
    lock = corpus["db"].parent / "kb.lock"
    fd = os.open(lock, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(fd, fcntl.LOCK_EX)
    try:
        c = ["--config", str(corpus["config"])]
        assert cli.main(c + ["build", "--full", "--lock-timeout", "0"]) == 0
        assert cli.main(c + ["gc", "--lock-timeout", "0"]) == 0
        assert capsys.readouterr().err.count("another writer holds the lock") == 2
    finally:
        os.close(fd)


# --- an interrupted writer, a busy database: what readers are told, what --full does ---

def _embedded(corpus):
    """A database with vectors, as after `kb update --embed` (fake model)."""
    from kb import embed
    from kb.embed import FakeEmbedder
    run(corpus)
    conn = store.open_rw(corpus["db"])
    embed.embed_pass(conn, corpus["db"], FakeEmbedder())
    conn.close()
    n = emb_rows(corpus["db"])
    assert n > 0
    return n


def emb_rows(db):
    return chunks(db, "SELECT count(*) FROM emb_row")[0][0]


def test_a_hot_journal_sends_readers_to_kb_update_and_kb_update_heals_it(corpus):
    from conftest import kill_a_writer_mid_transaction
    from kb.search import Index, QueryError, search
    n = _embedded(corpus)
    long_lived = Index(corpus["db"])
    assert search(long_lived, "латентность", mode="fts")["results"]
    kill_a_writer_mid_transaction(corpus["db"])
    for idx in (Index(corpus["db"]), long_lived):            # a fresh reader and an open one
        with pytest.raises(QueryError) as e:
            search(idx, "латентность", mode="fts")
        assert "kb update" in str(e.value) and "build --full" not in str(e.value), str(e.value)
    st = run(corpus)
    assert st["exit"] == 0 and emb_rows(corpus["db"]) == n
    assert search(long_lived, "латентность", mode="fts")["results"]
    integrity(corpus["db"])


def test_build_full_over_a_hot_journal_keeps_the_vectors_and_a_whole_file(corpus):
    """--full used to read the old file read-only: the hot journal made it
    'unreadable' -> 0 vectors carried; os.replace left that journal next to
    the NEW file, and the next writer played it into it (malformed)."""
    from conftest import kill_a_writer_mid_transaction
    n = _embedded(corpus)
    kill_a_writer_mid_transaction(corpus["db"])
    st = update.build_full(cfg_of(corpus), corpus["db"], warn=lambda m: None)
    assert st["carried_embeddings"] == n
    assert not os.path.exists(f"{corpus['db']}-journal")
    assert run(corpus)["exit"] == 0 and emb_rows(corpus["db"]) == n
    integrity(corpus["db"])


def test_build_full_never_drops_vectors_it_could_not_read(corpus, monkeypatch):
    """A v2 database held by an outside writer is not 'nothing to carry': the
    build stops and the old file (with its vectors) stays as it was."""
    import sqlite3
    n = _embedded(corpus)
    monkeypatch.setattr(store, "BUSY_MS", 200)
    before = corpus["db"].read_bytes()
    holder = sqlite3.connect(corpus["db"], isolation_level=None)
    holder.execute("BEGIN EXCLUSIVE")
    try:
        with pytest.raises(store.SchemaError, match="busy"):
            update.build_full(cfg_of(corpus), corpus["db"], warn=lambda m: None)
    finally:
        holder.execute("ROLLBACK")
        holder.close()
    assert corpus["db"].read_bytes() == before and emb_rows(corpus["db"]) == n
    assert sorted(p.name for p in corpus["db"].parent.iterdir() if p.name.startswith(".kb.db.tmp")) == []


def test_a_busy_index_tells_readers_to_retry_not_to_rebuild(corpus, monkeypatch, capsys):
    import sqlite3
    from kb.search import Index, QueryError, search
    run(corpus)
    monkeypatch.setattr(store, "BUSY_MS", 200)
    long_lived = Index(corpus["db"])
    assert search(long_lived, "латентность", mode="fts")["results"]
    holder = sqlite3.connect(corpus["db"], isolation_level=None)
    holder.execute("BEGIN EXCLUSIVE")
    try:
        for idx in (Index(corpus["db"]), long_lived):
            with pytest.raises(QueryError) as e:
                search(idx, "латентность", mode="fts")
            assert "busy" in str(e.value) and "build --full" not in str(e.value), str(e.value)
        assert cli.main(["--config", str(corpus["config"]), "search", "латентность", "--mode", "fts"]) == 2
        assert "busy" in capsys.readouterr().err
    finally:
        holder.execute("ROLLBACK")
        holder.close()


def test_cli_build_reports_a_refusal_instead_of_a_traceback(corpus, monkeypatch, capsys):
    import sqlite3
    _embedded(corpus)
    monkeypatch.setattr(store, "BUSY_MS", 200)
    holder = sqlite3.connect(corpus["db"], isolation_level=None)
    holder.execute("BEGIN EXCLUSIVE")
    try:
        assert cli.main(["--config", str(corpus["config"]), "build", "--full"]) == 3
    finally:
        holder.execute("ROLLBACK")
        holder.close()
    assert "busy" in capsys.readouterr().err


def test_build_full_refuses_to_drop_vectors_whose_tables_it_cannot_read(corpus):
    import sqlite3
    _embedded(corpus)
    c = sqlite3.connect(corpus["db"], isolation_level=None)     # foreign keys off, as sqlite3 is
    c.execute("DROP TABLE emb_model")
    c.close()
    before = corpus["db"].read_bytes()
    with pytest.raises(store.SchemaError, match="cannot carry the vectors"):
        update.build_full(cfg_of(corpus), corpus["db"], warn=lambda m: None)
    assert corpus["db"].read_bytes() == before


# --- one odd unit never sinks the run -----------------------------------------------

FILLER = " ".join(["слово"] * 70)                 # > SMALL: no section merges into another
LONG_Y = "\n\n".join(f"абзац {i} МЕТКАДЛИННЫЙ{i} " + "текст " * 40 for i in range(9))
ODD_HEADINGS = (f"# X\n\nМЕТКАПЕРВЫЙ {FILLER}\n\n# X\n\nМЕТКАПОВТОР {FILLER}\n\n"
                f"# X#2\n\nМЕТКАБУКВАЛЬНЫЙ {FILLER}\n\n"
                f"# Y\n\n{LONG_Y}\n\n# Y/p2\n\nМЕТКАЧАСТЬ {FILLER}\n")


def test_a_heading_equal_to_a_generated_anchor_does_not_sink_the_run(corpus):
    """«X#2» is what the chunker calls the second «X», «Y/p2» the second part
    of a long «Y». A heading spelled exactly so raised ValueError (duplicate
    anchor) out of `kb update` and `kb build --full`: one note took every
    unit of every source down with it."""
    (corpus["research"] / "odd-headings.md").write_text(ODD_HEADINGS, encoding="utf-8")
    st = run(corpus)
    assert st["exit"] == 0
    assert st["kinds"]["note"]["anchor_collisions"] == 2
    for word in ("МЕТКАПЕРВЫЙ", "МЕТКАПОВТОР", "МЕТКАБУКВАЛЬНЫЙ", "МЕТКАДЛИННЫЙ0", "МЕТКАДЛИННЫЙ8",
                 "МЕТКАЧАСТЬ"):
        assert fts(corpus["db"], word), word
    sql = ("SELECT anchor, chunk_id FROM chunk JOIN unit USING(unit_id) "
           "WHERE rel='odd-headings.md' ORDER BY ordinal")
    ids = chunks(corpus["db"], sql)
    assert [a for a, _ in ids] == ["X", "X#2", "X#2#2", "Y", "Y/p2", "Y/p2#2"]
    st = update.build_full(cfg_of(corpus), corpus["db"], warn=lambda m: None)
    assert st["exit"] == 0
    assert chunks(corpus["db"], sql) == ids, "a fresh parse resolves the same way: the same ids"


def test_one_unit_that_cannot_be_written_does_not_sink_the_run(corpus):
    """A transcript line with an escaped lone surrogate (JSON.stringify writes
    one for a string cut inside an emoji) parses, but no UTF-8 encoder takes
    it: the UnicodeEncodeError escaped `kb update` / `kb build --full` and
    stopped every unit after it. Now that unit is counted and named (its
    path, never its text), everything else is indexed, the run exits 3."""
    (corpus["slug"] / "cut-emoji.jsonl").write_text(json.dumps({
        "type": "user", "uuid": "ce1", "timestamp": "2026-08-01T00:00:00Z",
        "message": {"role": "user", "content": "МЕТКАОБРЕЗКИ эмодзи \ud83d обрезан"}}) + "\n")
    for go in (lambda warn: run(corpus, warn=warn),
               lambda warn: update.build_full(cfg_of(corpus), corpus["db"], warn=warn)):
        warnings = []
        st = go(warnings.append)
        assert st["exit"] == 3 and st["failed"] == 1
        assert st["kinds"]["transcript"]["failed"] == 1
        assert any("cut-emoji.jsonl" in w for w in warnings), warnings
        assert not any("МЕТКАОБРЕЗКИ" in w for w in warnings), "paths in the journal, never text"
        assert fts(corpus["db"], "латентность"), "the next transcript of the same source"
        assert fts(corpus["db"], "anytime"), "and the sources after it"
        integrity(corpus["db"])


@pytest.mark.parametrize("exc", [sqlite3.OperationalError("database is locked"),
                                 sqlite3.DatabaseError("database disk image is malformed")])
def test_a_failing_database_still_stops_the_run(corpus, monkeypatch, exc):
    """Isolation is for a unit's own data. A busy, full, unreadable or corrupt
    database fails every unit after it the same way (busy: 5 s each) — the
    run stops at once, as before, instead of counting thousands of failures."""
    def broken(*a, **kw):
        raise exc
    monkeypatch.setattr(store, "write_unit", broken)
    with pytest.raises(type(exc)):
        run(corpus)
