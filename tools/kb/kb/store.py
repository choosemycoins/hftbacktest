"""Schema v2 (DESIGN.md §3): unit -> chunk -> chunk_fts (external content).

Rollback journal, never WAL: `kb build --full` swaps the database file with
an atomic rename, and a WAL file left by a live reader of the old file would
be applied to the new one.

Deletion is explicit (chunks, then the unit, one transaction); FTS rows follow
through triggers written exactly like the "External Content Tables" example of
the FTS5 documentation. Metadata-only updates (loc, ordinal, date) do not touch
the FTS index: the update trigger fires only on the indexed columns.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from . import sources
from .stem import STEM_VERSION, stem_text

SCHEMA_VERSION = 2
SEP = "\x1f"
BUSY_MS = 5000          # how long any handle waits for another one's lock

SCHEMA = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);

CREATE TABLE unit (
  unit_id     INTEGER PRIMARY KEY,
  source_id   TEXT NOT NULL,
  rel         TEXT NOT NULL,
  kind        TEXT NOT NULL CHECK (kind IN
               ('transcript','export','memory','doc','note','chat','course','commit')),
  project     TEXT NOT NULL,
  abs_path    TEXT NOT NULL,
  signature   TEXT NOT NULL,
  content_sha TEXT,
  parser_ver  INTEGER NOT NULL,
  indexed_at  TEXT NOT NULL,
  title       TEXT NOT NULL DEFAULT '',
  UNIQUE (source_id, rel)
);

CREATE TABLE chunk (
  rowid      INTEGER PRIMARY KEY,
  chunk_id   TEXT NOT NULL UNIQUE,
  unit_id    INTEGER NOT NULL REFERENCES unit(unit_id),
  ordinal    INTEGER NOT NULL,
  anchor     TEXT NOT NULL,
  kind       TEXT NOT NULL,
  project    TEXT NOT NULL,
  date       TEXT NOT NULL,
  loc        TEXT NOT NULL,
  file       TEXT,
  title      TEXT NOT NULL DEFAULT '',
  text       TEXT NOT NULL,          -- raw, UNmasked (masking happens on output)
  stem       TEXT NOT NULL,          -- stems of title + text, in order (kb.stem)
  text_sha   TEXT NOT NULL,          -- sha256(title ␟ text): the embedding key
  n_chars    INTEGER NOT NULL
);
CREATE INDEX chunk_unit  ON chunk(unit_id, ordinal);
CREATE INDEX chunk_scope ON chunk(kind, project, date);
CREATE INDEX chunk_sha   ON chunk(text_sha);

CREATE VIRTUAL TABLE chunk_fts USING fts5(
  title, text, stem,
  content='chunk', content_rowid='rowid',
  tokenize='unicode61 remove_diacritics 2'
);
CREATE TRIGGER chunk_ai AFTER INSERT ON chunk BEGIN
  INSERT INTO chunk_fts(rowid, title, text, stem) VALUES (new.rowid, new.title, new.text, new.stem);
END;
CREATE TRIGGER chunk_ad AFTER DELETE ON chunk BEGIN
  INSERT INTO chunk_fts(chunk_fts, rowid, title, text, stem)
    VALUES ('delete', old.rowid, old.title, old.text, old.stem);
END;
CREATE TRIGGER chunk_au AFTER UPDATE OF title, text, stem ON chunk BEGIN
  INSERT INTO chunk_fts(chunk_fts, rowid, title, text, stem)
    VALUES ('delete', old.rowid, old.title, old.text, old.stem);
  INSERT INTO chunk_fts(rowid, title, text, stem) VALUES (new.rowid, new.title, new.text, new.stem);
END;

CREATE TABLE emb_model (
  model_key   TEXT PRIMARY KEY,
  repo_id     TEXT NOT NULL,
  revision    TEXT NOT NULL,
  dim         INTEGER NOT NULL,
  max_tokens  INTEGER NOT NULL,
  query_prefix TEXT NOT NULL,
  passage_prefix TEXT NOT NULL,
  gen         INTEGER NOT NULL,
  created_at  TEXT NOT NULL
);
CREATE TABLE emb_row (
  model_key TEXT NOT NULL REFERENCES emb_model(model_key),
  text_sha  TEXT NOT NULL,
  row       INTEGER NOT NULL,
  PRIMARY KEY (model_key, text_sha)
);
"""


class SchemaError(Exception):
    pass


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def chunk_id(source_id: str, rel: str, anchor: str) -> str:
    return hashlib.sha256(SEP.join((source_id, rel, anchor)).encode("utf-8")).hexdigest()[:16]


def text_sha(title: str, text: str) -> str:
    return hashlib.sha256(f"{title}{SEP}{text}".encode("utf-8")).hexdigest()


def open_rw(path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path), isolation_level=None, timeout=BUSY_MS / 1000)
    conn.execute(f"PRAGMA busy_timeout={BUSY_MS}")
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=FULL")
    # Big units (a chat is one unit) must not spill to the file mid-transaction:
    # a spill takes the EXCLUSIVE lock and blocks readers until commit.
    conn.execute("PRAGMA cache_size=-262144")
    return conn


def open_ro(path) -> sqlite3.Connection:
    uri = Path(os.path.abspath(path)).as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True, isolation_level=None, timeout=BUSY_MS / 1000,
                           check_same_thread=False)
    conn.execute(f"PRAGMA busy_timeout={BUSY_MS}")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
    set_meta(conn, schema=str(SCHEMA_VERSION), built_at=now_iso(), emb_version="0",
             parser_versions=json.dumps(sources.PARSER_VERSIONS, sort_keys=True),
             stem_version=STEM_VERSION)


def explain(exc: sqlite3.DatabaseError) -> str:
    """What a database error means for the reader and what to do. Only a file
    that is not a kb database calls for `kb build --full`: on a merely busy
    one it would race the writer, and next to a hot journal it used to drop
    every vector (DESIGN.md §8.5)."""
    name = getattr(exc, "sqlite_errorname", "") or ""
    if name.startswith(("SQLITE_BUSY", "SQLITE_LOCKED")):
        return "the index is busy (a writer is committing) — retry in a few seconds"
    if name == "SQLITE_READONLY_ROLLBACK":
        return ("an interrupted write left a hot journal next to the index; a reader cannot "
                "roll it back — run: kb update (it does, and keeps every vector)")
    return f"not a kb database ({exc}) — run: kb build --full"


def check_schema(conn: sqlite3.Connection) -> None:
    try:
        version = conn.execute("PRAGMA user_version").fetchone()[0]
    except sqlite3.DatabaseError as exc:
        raise SchemaError(explain(exc)) from exc
    if version != SCHEMA_VERSION:
        raise SchemaError(f"schema v{version} (need v{SCHEMA_VERSION}) — run: kb build --full")


def recover(path) -> int | None:
    """Open the live database read-write once: its first read rolls back a hot
    journal an interrupted writer left (read-only handles cannot, so every
    reader fails until a writer does). user_version, or None when the file is
    not a SQLite database at all; any other failure (busy, read-only file)
    raises SchemaError — a database that could not be read is never taken
    for one that has nothing in it."""
    conn = sqlite3.connect(str(path), isolation_level=None, timeout=BUSY_MS / 1000)
    try:
        conn.execute(f"PRAGMA busy_timeout={BUSY_MS}")
        return conn.execute("PRAGMA user_version").fetchone()[0]
    except sqlite3.DatabaseError as exc:
        if getattr(exc, "sqlite_errorname", "") == "SQLITE_NOTADB":
            return None
        raise SchemaError(explain(exc)) from exc
    finally:
        conn.close()


@contextmanager
def transaction(conn: sqlite3.Connection):
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    conn.execute("COMMIT")


@contextmanager
def snapshot(conn: sqlite3.Connection):
    """One read transaction: every statement inside sees the same committed
    state (a SHARED lock; a writer's commit waits for its end, busy_timeout).
    Nested use joins the outer one. Keep it short: nothing slow (a model load,
    a query embedding) belongs inside."""
    if conn.in_transaction:
        yield
        return
    conn.execute("BEGIN")
    try:
        yield
    finally:
        if conn.in_transaction:         # some errors end the transaction themselves
            conn.execute("COMMIT")      # read-only: only drops the lock


def get_meta(conn: sqlite3.Connection, key: str, default=None):
    row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return row[0] if row else default


def set_meta(conn: sqlite3.Connection, **values) -> None:
    conn.executemany("INSERT INTO meta(key, value) VALUES (?, ?) "
                     "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                     [(k, str(v)) for k, v in values.items()])


def units_of(conn: sqlite3.Connection, source_id: str) -> dict:
    return {r[1]: {"unit_id": r[0], "signature": r[2], "content_sha": r[3], "parser_ver": r[4],
                   "abs_path": r[5], "project": r[6], "kind": r[7]}
            for r in conn.execute(
                "SELECT unit_id, rel, signature, content_sha, parser_ver, abs_path, project, kind "
                "FROM unit WHERE source_id = ?", (source_id,))}


def _chunk_rows(cand, pieces):
    """Rows of one unit. `loc` is made unique within the unit here, for every
    chunker: parts of one cut piece share their source position (a transcript
    line, a commit sha, a docstring, an ASR window, a chat window, one long
    markdown line), so the k-th chunk with an already seen loc gets '/p<k>' —
    `where` (built from loc) then names exactly one chunk, and kb read of a
    hit's `where` reads that hit."""
    rows, seen, locs = [], set(), {}
    for ordinal, p in enumerate(pieces):
        cid = chunk_id(cand.source_id, cand.rel, p.anchor)
        if cid in seen:
            raise ValueError(f"duplicate anchor {p.anchor!r} in {cand.source_id}:{cand.rel}")
        seen.add(cid)
        locs[p.loc] = locs.get(p.loc, 0) + 1
        loc = p.loc if locs[p.loc] == 1 else f"{p.loc}/p{locs[p.loc]}"
        rows.append({
            "chunk_id": cid, "ordinal": ordinal, "anchor": p.anchor, "kind": cand.kind,
            "project": cand.project, "date": p.date or cand.date, "loc": loc, "file": p.file,
            "title": p.title, "text": p.text, "text_sha": text_sha(p.title, p.text),
            "stem": None,                   # computed only for rows that get written
            "n_chars": len(p.text),
        })
    return rows


_COMPARED = ("ordinal", "anchor", "kind", "project", "date", "loc", "file", "title", "text_sha")


def write_unit(conn: sqlite3.Connection, cand, pieces, content_sha: str | None,
               parser_ver: int | None = None) -> dict:
    """Make the unit's chunks equal `pieces`: diff by chunk_id. One transaction."""
    rows = _chunk_rows(cand, pieces)
    parser_ver = parser_ver if parser_ver is not None else sources.PARSER_VERSIONS[cand.kind]
    stats = {"added": 0, "updated": 0, "deleted": 0, "unchanged": 0}
    with transaction(conn):
        conn.execute(
            "INSERT INTO unit(source_id, rel, kind, project, abs_path, signature, content_sha, "
            "parser_ver, indexed_at, title) VALUES (?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(source_id, rel) DO UPDATE SET kind=excluded.kind, "
            "project=excluded.project, abs_path=excluded.abs_path, signature=excluded.signature, "
            "content_sha=excluded.content_sha, parser_ver=excluded.parser_ver, "
            "indexed_at=excluded.indexed_at, title=excluded.title",
            (cand.source_id, cand.rel, cand.kind, cand.project, cand.abs_path,
             json.dumps(cand.signature), content_sha, parser_ver, now_iso(), cand.title))
        unit_id = conn.execute("SELECT unit_id FROM unit WHERE source_id=? AND rel=?",
                               (cand.source_id, cand.rel)).fetchone()[0]
        old = {r[0]: dict(zip(("rowid",) + _COMPARED, r[1:])) for r in conn.execute(
            "SELECT chunk_id, rowid, " + ", ".join(_COMPARED) + " FROM chunk WHERE unit_id = ?",
            (unit_id,))}
        new_ids = {r["chunk_id"] for r in rows}
        gone = [old[cid]["rowid"] for cid in old if cid not in new_ids]
        conn.executemany("DELETE FROM chunk WHERE rowid = ?", [(r,) for r in gone])
        stats["deleted"] = len(gone)
        for r in rows:
            prev = old.get(r["chunk_id"])
            if prev is None:
                conn.execute(
                    "INSERT INTO chunk(chunk_id, unit_id, ordinal, anchor, kind, project, date, "
                    "loc, file, title, text, stem, text_sha, n_chars) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (r["chunk_id"], unit_id, r["ordinal"], r["anchor"], r["kind"], r["project"],
                     r["date"], r["loc"], r["file"], r["title"], r["text"],
                     stem_text(r["title"], r["text"]), r["text_sha"], r["n_chars"]))
                stats["added"] += 1
            elif any(prev[k] != r[k] for k in _COMPARED):
                if prev["text_sha"] != r["text_sha"]:
                    conn.execute(
                        "UPDATE chunk SET ordinal=?, anchor=?, kind=?, project=?, date=?, loc=?, "
                        "file=?, title=?, text=?, stem=?, text_sha=?, n_chars=? WHERE rowid=?",
                        (r["ordinal"], r["anchor"], r["kind"], r["project"], r["date"], r["loc"],
                         r["file"], r["title"], r["text"], stem_text(r["title"], r["text"]),
                         r["text_sha"], r["n_chars"], prev["rowid"]))
                else:          # metadata only: the FTS row stays as it is
                    conn.execute(
                        "UPDATE chunk SET ordinal=?, anchor=?, kind=?, project=?, date=?, loc=?, "
                        "file=? WHERE rowid=?",
                        (r["ordinal"], r["anchor"], r["kind"], r["project"], r["date"], r["loc"],
                         r["file"], prev["rowid"]))
                stats["updated"] += 1
            else:
                stats["unchanged"] += 1
    stats["unit_id"] = unit_id
    return stats


def touch_unit(conn: sqlite3.Connection, unit_id: int, cand) -> None:
    """Same bytes, new stat (or the file moved live -> mirror): metadata only."""
    with transaction(conn):
        conn.execute("UPDATE unit SET signature=?, abs_path=?, indexed_at=? WHERE unit_id=?",
                     (json.dumps(cand.signature), cand.abs_path, now_iso(), unit_id))


def delete_unit(conn: sqlite3.Connection, unit_id: int) -> int:
    with transaction(conn):
        n = conn.execute("DELETE FROM chunk WHERE unit_id = ?", (unit_id,)).rowcount
        conn.execute("DELETE FROM unit WHERE unit_id = ?", (unit_id,))
    return n


def restem(conn: sqlite3.Connection) -> int:
    """Recompute the stem column (the stemmer changed). Triggers re-index FTS."""
    n = 0
    with transaction(conn):
        rows = conn.execute("SELECT rowid, title, text FROM chunk").fetchall()
        for rowid, title, text in rows:
            conn.execute("UPDATE chunk SET stem = ? WHERE rowid = ?", (stem_text(title, text), rowid))
            n += 1
        set_meta(conn, stem_version=STEM_VERSION)
    return n
