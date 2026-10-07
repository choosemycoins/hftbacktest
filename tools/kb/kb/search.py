"""Search: FTS (natural or `raw:`), and the shape of every answer.

Every result is a POINTER (id, kind, project, date, where) plus a masked
snippet for orientation — open the source before believing anything.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from . import store
from .redact import redact
from .stem import TOKEN_RE, normalize, stem_word, tokens

NOTICE = ("kb: pointers, verify at source — locations of past discussion, not answers. "
          "Snippets are masked best-effort; keep this output local.")
DEPTH = 100
MAX_LIMIT = 50
MAX_QUERY = 2000
MAX_TERMS = 32
PER_KEY = 3
SNIPPET_TOKENS = 40
SNIPPET_CHARS = 400
SEMANTIC_HEAD = 300

STOPWORDS = frozenset("""
и в во не что он на я с со как а то все она так его но да ты к у же вы за бы по только ее её мне
было вот от меня еще ещё нет о из ему теперь когда даже ну вдруг ли если уже или ни быть был него до
вас нибудь опять уж вам ведь там потом себя ничего ей может они тут где есть надо ней для мы тебя их
чем была сам чтоб без будто чего раз тоже себе под будет ж тогда кто этот того потому этого какой
совсем ним здесь этом один почти мой тем чтобы нее сейчас были куда зачем всех никогда можно при
наконец два об другой хоть после над больше тот через эти нас про всего них какая много разве три
эту моя впрочем хорошо свою этой перед иногда лучше чуть том нельзя такой им более всегда конечно
всю между это ли такое какие каких какую сколько почему
a an the and or but if then else of to in on at by for with from into onto over under about as is are
was were be been being do does did doing have has had having i you he she it we they me him her us
them my your his its our their this that these those what which who whom whose how why when where
not no nor so than too very can will just should would could there here all any both each few more
most other some such only own same s t don now
""".split())


class QueryError(Exception):
    pass


def fts_quote(term: str) -> str:
    return '"' + term.replace('"', '""') + '"'


@dataclass
class Query:
    text: str
    raw: bool
    body: str
    terms: list[str] = field(default_factory=list)       # stems (natural) / words (raw)
    prefixes: list[str] = field(default_factory=list)    # raw `foo*` terms
    fts: str = ""                                        # FTS5 MATCH expression
    stemmed: bool = False                                # terms are stems


def parse_query(text: str) -> Query:
    text = (text or "").strip()
    if not text:
        raise QueryError("empty query")
    if len(text) > MAX_QUERY:
        raise QueryError(f"query longer than {MAX_QUERY} characters")
    if text.startswith("raw:"):
        body = text[4:].strip()
        if not body:
            raise QueryError("empty raw: query")
        words = [w for w in re.findall(r"[^\W_]+\*?", body)
                 if w.rstrip("*") not in ("AND", "OR", "NOT", "NEAR")]
        terms = [normalize(w) for w in words if not w.endswith("*")]
        prefixes = [normalize(w[:-1]) for w in words if w.endswith("*")]
        return Query(text, True, body, terms, prefixes, "{title text} : (" + body + ")")
    words = _query_words(text)
    stems = list(dict.fromkeys(stem_word(w) for w in words))[:MAX_TERMS]
    return Query(text, False, text, stems, [],
                 "stem : (" + " OR ".join(fts_quote(t) for t in stems) + ")", stemmed=True)


def _query_words(text: str) -> list[str]:
    toks = tokens(text)
    kept = [t for t in toks if t not in STOPWORDS] or toks
    if not kept:
        raise QueryError("query has no searchable words")
    return kept


def raw_or_query(text: str) -> Query:
    """Eval baseline `fts_raw_or`: the same words OR-ed over the RAW columns,
    no stemming — the measure of what stemming adds."""
    if text.strip().startswith("raw:"):
        return parse_query(text)
    import unicodedata
    toks = TOKEN_RE.findall(unicodedata.normalize("NFKC", text).lower())   # ё kept: raw columns keep it
    kept = [t for t in toks if t not in STOPWORDS and t.replace("ё", "е") not in STOPWORDS] or toks
    if not kept:
        raise QueryError("query has no searchable words")
    words = list(dict.fromkeys(kept))[:MAX_TERMS]
    return Query(text, False, text, words, [],
                 "{title text} : (" + " OR ".join(fts_quote(t) for t in words) + ")")


# ---------------------------------------------------------------------------
# results
# ---------------------------------------------------------------------------

@dataclass
class Hit:
    rowid: int
    chunk_id: str
    unit_id: int
    kind: str
    project: str
    date: str
    title: str
    text: str
    loc: str
    file: str | None
    abs_path: str
    text_sha: str
    ordinal: int
    ranks: dict = field(default_factory=dict)
    score: float = 0.0

    @property
    def where(self) -> str:
        return where_of(self.abs_path, self.kind, self.loc, self.file)


def where_of(abs_path: str, kind: str, loc: str, file: str | None) -> str:
    if kind == "commit":
        return f"{abs_path}@{loc}"
    if kind == "chat":
        first = loc.split("..", 1)[0]
        return f"{abs_path}/{file}#{first}"
    return f"{abs_path}#{loc}"


HIT_SQL = ("SELECT c.rowid, c.chunk_id, c.unit_id, c.kind, c.project, c.date, c.title, c.text, "
           "c.loc, c.file, u.abs_path, c.text_sha, c.ordinal FROM chunk c JOIN unit u USING (unit_id) ")


def fetch_hits(conn: sqlite3.Connection, rowids: list[int]) -> dict[int, Hit]:
    out = {}
    for i in range(0, len(rowids), 500):
        part = rowids[i:i + 500]
        sql = HIT_SQL + f"WHERE c.rowid IN ({','.join('?' * len(part))})"
        for r in conn.execute(sql, part):
            out[r[0]] = Hit(*r)
    return out


@dataclass
class Scope:
    kinds: tuple[str, ...] = ()
    project: str | None = None
    since: str | None = None

    def sql(self, alias="c"):
        where, params = [], []
        if self.kinds:
            where.append(f"{alias}.kind IN ({','.join('?' * len(self.kinds))})")
            params += list(self.kinds)
        if self.project:
            where.append(f"{alias}.project = ?")
            params.append(self.project)
        if self.since:
            where.append(f"{alias}.date >= ?")
            params.append(self.since)
        return where, params


def make_scope(kind=None, project=None, since=None) -> Scope:
    from .sources import KINDS
    kinds = ()
    if kind:
        kinds = tuple(k.strip() for k in (kind if isinstance(kind, (list, tuple)) else
                                           str(kind).split(",")) if k.strip())
        bad = [k for k in kinds if k not in KINDS]
        if bad:
            raise QueryError(f"unknown kind {', '.join(bad)} (one of {', '.join(KINDS)})")
    if since:
        since = str(since).strip()
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}(?:T[\d:]+Z?)?", since):
            raise QueryError("since must be an ISO date, e.g. 2026-09-01")
    return Scope(kinds, project or None, since or None)


def fts_ranked(conn, q: Query, scope: Scope, depth=DEPTH, restrict_table=None) -> list[int]:
    where, params = scope.sql()
    sql = ("SELECT c.rowid FROM chunk_fts JOIN chunk c ON c.rowid = chunk_fts.rowid "
           + (f"JOIN {restrict_table} r ON r.rowid = c.rowid " if restrict_table else "")
           + "WHERE chunk_fts MATCH ? " + "".join(f"AND {w} " for w in where)
           + "ORDER BY bm25(chunk_fts) LIMIT ?")
    try:
        return [r[0] for r in conn.execute(sql, [q.fts, *params, depth])]
    except sqlite3.OperationalError as exc:
        raise QueryError(f"bad query {q.body!r}: {exc}") from exc


def dedup_diversify(hits: list[Hit], limit: int) -> list[Hit]:
    """Same text -> keep the earliest by date (the list order otherwise); at
    most PER_KEY results per unit (per chat day). The key is the text alone,
    not text_sha: text_sha includes the title, and the same message pasted into
    two sessions has two titles (session/agent) — exactly the duplicate to drop."""
    first: dict[bytes, Hit] = {}
    for h in hits:
        key = hashlib.sha1(h.text.encode("utf-8")).digest()
        cur = first.get(key)
        if cur is None or h.date < cur.date:
            first[key] = h
    keep = {id(h) for h in first.values()}
    out, per = [], {}
    for h in hits:
        if id(h) not in keep:
            continue
        key = (h.unit_id, h.date[:10]) if h.kind == "chat" else (h.unit_id,)
        if per.get(key, 0) >= PER_KEY:
            continue
        per[key] = per.get(key, 0) + 1
        out.append(h)
        if len(out) >= limit:
            break
    return out


# ---------------------------------------------------------------------------
# snippets
# ---------------------------------------------------------------------------

def _flat(text: str) -> str:
    return " ".join(text.split())


def make_matcher(q: Query):
    terms = set(q.terms)
    prefixes = tuple(q.prefixes)
    if q.stemmed:
        def match(token: str) -> bool:
            return stem_word(token) in terms
        return match

    def match(token: str) -> bool:
        t = normalize(token)
        return t in terms or (bool(prefixes) and t.startswith(prefixes))
    return match


def snippet(text: str, match, semantic_only=False) -> str:
    """One line, <= SNIPPET_CHARS: the SNIPPET_TOKENS-token window with the most
    matches, matches in [ ]. The window is masked BEFORE marking, so a marker
    never splits a secret into pieces the masking rules cannot see."""
    if semantic_only or match is None:
        head = redact(_flat(text[: SEMANTIC_HEAD * 2]))
        return head[:SEMANTIC_HEAD] + ("…" if len(head) > SEMANTIC_HEAD or len(text) > SEMANTIC_HEAD * 2 else "")
    toks = [(m.start(), m.end(), match(m.group())) for m in TOKEN_RE.finditer(text)]
    if not toks:
        return redact(_flat(text))[:SNIPPET_CHARS]
    best, best_i, run = -1, 0, 0
    for i, t in enumerate(toks):
        run += t[2]
        if i >= SNIPPET_TOKENS:
            run -= toks[i - SNIPPET_TOKENS][2]
        start = max(0, i - SNIPPET_TOKENS + 1)
        if run > best:
            best, best_i = run, start
    lo = toks[best_i][0]
    hi = toks[min(len(toks), best_i + SNIPPET_TOKENS) - 1][1]
    window = redact(text[lo:hi])
    marked = TOKEN_RE.sub(lambda m: f"[{m.group()}]" if match(m.group()) else m.group(), window)
    out = _flat(marked)
    if len(out) > SNIPPET_CHARS:
        out = out[:SNIPPET_CHARS].rstrip() + "…"
    return ("… " if lo > 0 else "") + out + (" …" if hi < len(text) else "")


# ---------------------------------------------------------------------------
# the index (read-only, reopened when the file is replaced)
# ---------------------------------------------------------------------------

class Index:
    def __init__(self, db_path):
        self.path = Path(db_path)
        self._conn = None
        self._ino = None

    def conn(self) -> sqlite3.Connection:
        try:
            st = os.stat(self.path)
        except OSError as exc:
            raise QueryError(f"no index at {self.path} — run: kb build --full") from exc
        if self._conn is None or st.st_ino != self._ino:
            if self._conn is not None:
                self._conn.close()
            conn = store.open_ro(self.path)
            try:
                store.check_schema(conn)
            except store.SchemaError as exc:
                conn.close()
                raise QueryError(str(exc)) from exc
            self._conn, self._ino = conn, st.st_ino
        return self._conn

    def meta(self) -> dict:
        return dict(self.conn().execute("SELECT key, value FROM meta"))

    def close(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None


def index_info(meta: dict) -> dict:
    updated = meta.get("updated_at") or meta.get("built_at")
    age = None
    if updated:
        try:
            dt = datetime.strptime(updated, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            age = round((datetime.now(timezone.utc) - dt).total_seconds() / 3600, 1)
        except ValueError:
            pass
    return {"updated_at": updated, "age_hours": age, "model": None}


def count_scope(conn, scope: Scope) -> int:
    where, params = scope.sql()
    sql = "SELECT count(*) FROM chunk c" + (" WHERE " + " AND ".join(where) if where else "")
    return conn.execute(sql, params).fetchone()[0]


def hit_json(h: Hit, match, semantic_only: bool) -> dict:
    return {"id": f"kb:{h.chunk_id}", "kind": h.kind, "project": h.project, "date": h.date,
            "title": h.title, "where": h.where,
            "snippet": snippet(h.text, match, semantic_only=semantic_only),
            "score": round(h.score, 6), "ranks": h.ranks}


def _restrict_table(index, conn, restrict) -> str | None:
    """Eval: rank inside a sub-corpus (a TEMP table works on a read-only handle)."""
    if restrict is None:
        return None
    key = (id(conn), id(restrict), len(restrict))
    if getattr(index, "_restrict_key", None) != key:
        conn.execute("CREATE TEMP TABLE IF NOT EXISTS kb_restrict(rowid INTEGER PRIMARY KEY)")
        conn.execute("DELETE FROM kb_restrict")
        conn.executemany("INSERT INTO kb_restrict VALUES (?)", [(int(r),) for r in restrict])
        index._restrict_key = key
    return "kb_restrict"


def search(index: Index, query: str, mode: str = "hybrid", kind=None, project=None, since=None,
           limit: int = 10, engine=None, wait: float = 60.0, restrict=None,
           fts_query=None) -> dict:
    """The answer as one JSON-able dict (the same for CLI --json and MCP).
    `engine` (kb.semantic.Engine) supplies vectors; without it, or while its
    model is unavailable, semantic/hybrid degrade to fts and say so."""
    from .semantic import rrf, semantic_text
    t0 = time.monotonic()
    if mode not in ("fts", "semantic", "hybrid"):
        raise QueryError("mode must be hybrid, fts or semantic")
    limit = max(1, min(int(limit), MAX_LIMIT))
    scope = make_scope(kind, project, since)
    q = fts_query(query) if fts_query else parse_query(query)
    conn = index.conn()
    meta = index.meta()
    table = _restrict_table(index, conn, restrict)
    degraded = None
    sem: list[tuple[int, float]] = []
    want_sem = mode in ("semantic", "hybrid")
    if want_sem:
        emb, why = engine.embedder(wait) if engine is not None else (None, "no embedding model configured")
        if emb is None:
            degraded = f"semantic unavailable ({why}); fts only"
            mode_used = "fts"
        else:
            qv = emb.embed_query(semantic_text(q))
            sem = engine.sem.ranked(qv, scope, restrict=restrict)
            mode_used = mode
    else:
        mode_used = "fts"
    fts = fts_ranked(conn, q, scope, restrict_table=table) if mode_used in ("fts", "hybrid") else []
    sem_ids = [r for r, _ in sem]
    hits = fetch_hits(conn, list(dict.fromkeys(fts + sem_ids)))
    if mode_used == "hybrid":
        fused = rrf(fts, sem_ids, {r: h.date for r, h in hits.items()})
        ordered = []
        for r, score, ranks in fused:
            h = hits[r]
            h.score, h.ranks = score, ranks
            ordered.append(h)
    elif mode_used == "semantic":
        ordered = []
        for i, (r, score) in enumerate(sem, 1):
            h = hits[r]
            h.score, h.ranks = score, {"sem": i}
            ordered.append(h)
    else:
        ordered = []
        for i, r in enumerate(fts, 1):
            h = hits[r]
            h.score, h.ranks = 1.0 / i, {"fts": i}
            ordered.append(h)
    final = dedup_diversify(ordered, limit)
    match = make_matcher(q)
    if engine is not None:
        coverage = engine.sem.coverage(scope, restrict)
    else:
        coverage = {"embedded": 0, "in_scope": count_scope(conn, scope)}
    info = index_info(meta)
    info["model"] = engine.model_key if engine is not None else None
    return {
        "notice": NOTICE,
        "index": info,
        "query": {"mode": mode, "used": mode_used, "raw": q.raw,
                  "terms": q.terms + [p + "*" for p in q.prefixes]},
        "coverage": coverage,
        "degraded": degraded,
        "results": [hit_json(h, match, "fts" not in h.ranks) for h in final],
        "seconds": round(time.monotonic() - t0, 3),
    }


# ---------------------------------------------------------------------------
# kb read
# ---------------------------------------------------------------------------

READ_CAP = 20_000
MAX_CONTEXT = 5


def _resolve(conn, pointer: str) -> int | None:
    p = (pointer or "").strip()
    m = re.fullmatch(r"(?:kb:)?([0-9a-f]{16})", p)
    if m:
        row = conn.execute("SELECT rowid FROM chunk WHERE chunk_id = ?", (m.group(1),)).fetchone()
        return row[0] if row else None
    if "@" in p:
        path, _, sha = p.rpartition("@")
        if re.fullmatch(r"[0-9a-f]{7,40}", sha):
            row = conn.execute(
                "SELECT c.rowid FROM chunk c JOIN unit u USING (unit_id) WHERE c.kind='commit' "
                "AND u.abs_path = ? AND c.loc LIKE ? ORDER BY c.ordinal LIMIT 1",
                (path, sha + "%")).fetchone()
            if row:
                return row[0]
    if "#" in p:
        path, _, frag = p.rpartition("#")
        row = conn.execute(
            "SELECT c.rowid FROM chunk c JOIN unit u USING (unit_id) WHERE u.abs_path = ? "
            "AND c.kind != 'chat' AND c.loc = ? ORDER BY c.ordinal LIMIT 1", (path, frag)).fetchone()
        if row:
            return row[0]
        if frag.startswith("message"):
            for (rowid,) in conn.execute(
                    "SELECT c.rowid FROM chunk c JOIN unit u USING (unit_id) WHERE c.kind='chat' "
                    "AND (u.abs_path || '/' || c.file) = ? AND c.loc LIKE ? ORDER BY c.ordinal LIMIT 1",
                    (path, frag + "..%")):
                return rowid
    return None


def _stale(conn, unit_id: int, abs_path: str, kind: str):
    if kind in ("chat", "commit"):
        return None
    sig = conn.execute("SELECT signature FROM unit WHERE unit_id=?", (unit_id,)).fetchone()[0]
    try:
        st = os.stat(abs_path)
    except OSError:
        return True
    return json.loads(sig) != [st.st_size, st.st_mtime_ns]


def read_pointer(index: Index, pointer: str, context: int = 1) -> dict:
    """Resolve a pointer THROUGH THE INDEX ONLY; never read arbitrary files."""
    conn = index.conn()
    rowid = _resolve(conn, pointer)
    if rowid is None:
        raise QueryError("not an indexed pointer (use a kb:<id> or a `where` from kb search)")
    h = fetch_hits(conn, [rowid])[rowid]
    context = 0 if h.kind == "commit" else max(0, min(int(context), MAX_CONTEXT))
    text = h.text[:READ_CAP]
    budget = READ_CAP - len(text)
    before, after = [], []
    if context:
        rows = conn.execute(
            "SELECT ordinal, text FROM chunk WHERE unit_id=? AND ordinal BETWEEN ? AND ? "
            "AND ordinal != ? ORDER BY ordinal",
            (h.unit_id, h.ordinal - context, h.ordinal + context, h.ordinal)).fetchall()
        near = sorted(rows, key=lambda r: (abs(r[0] - h.ordinal), r[0]))
        for ordinal, t in near:
            if budget <= 0:
                break
            piece = t[:budget]
            budget -= len(piece)
            (before if ordinal < h.ordinal else after).append((ordinal, piece))
    return {"id": f"kb:{h.chunk_id}", "where": h.where, "kind": h.kind, "project": h.project,
            "date": h.date, "title": h.title, "stale": _stale(conn, h.unit_id, h.abs_path, h.kind),
            "text": text, "before": [t for _, t in sorted(before)],
            "after": [t for _, t in sorted(after)]}
