"""Semantic and hybrid ranking (DESIGN.md §7).

semantic  query vector -> cosine against the chunks in scope that have a
          vector (scope masks come from metadata held in memory) -> top 100.
          Chunks without a vector do not take part; `coverage` says how many.
hybrid    reciprocal-rank fusion of the fts and semantic lists:
          score = Σ 1/(60 + rank), depth 100 + 100, weights 1:1 (not tuned on
          the golden set); ties -> better fts rank, then newer date.
"""

from __future__ import annotations

import re
import warnings

import numpy as np

from . import store
from .embed import MatrixStore

RRF_K = 60
DEPTH = 100
BLOCK = 32768
FAST_MIN = 20_000        # a scope this big is pre-ranked by one float16 product over the matrix
PREFILTER = 1_000        # ... and only its best PREFILTER are re-scored exactly in float32


class SemanticIndex:
    """Metadata arrays + the float16 matrix of one model, reloaded whenever
    the database changed under them: the stamp is (connection generation,
    PRAGMA data_version), and data_version moves on EVERY commit of another
    connection — `kb update` rewriting chunks in place, an embedding append,
    a gc — not only on the ones that remember to bump a counter. Loaded inside
    the caller's read snapshot, so the arrays match what the same search
    fetches."""

    def __init__(self, index, model_key: str, dim: int):
        self.index = index
        self.key = model_key
        self.dim = dim
        self._stamp = None

    def _load(self):
        conn = self.index.conn()
        with store.snapshot(conn):
            stamp = (self.index.generation, conn.execute("PRAGMA data_version").fetchone()[0])
            if stamp != self._stamp:
                self._fill(conn)
                self._stamp = stamp

    def _fill(self, conn):
        rows = conn.execute(
            "SELECT c.rowid, c.kind, c.project, c.date, COALESCE(r.row, -1) FROM chunk c "
            "LEFT JOIN emb_row r ON r.model_key = ? AND r.text_sha = c.text_sha", (self.key,)).fetchall()
        self.rowids = np.fromiter((r[0] for r in rows), dtype=np.int64, count=len(rows))
        kinds = sorted({r[1] for r in rows})
        self.kind_code = {k: i for i, k in enumerate(kinds)}
        self.kinds = np.fromiter((self.kind_code[r[1]] for r in rows), dtype=np.int16, count=len(rows))
        projects = sorted({r[2] for r in rows})
        self.project_code = {p: i for i, p in enumerate(projects)}
        self.projects = np.fromiter((self.project_code[r[2]] for r in rows), dtype=np.int32,
                                    count=len(rows))
        self.dates = np.array([r[3] for r in rows], dtype="U20")
        self.rows = np.fromiter((r[4] for r in rows), dtype=np.int64, count=len(rows))
        self.matrix = MatrixStore(self.index.path, self.key, self.dim).load(conn)
        self.rows[self.rows >= len(self.matrix)] = -1          # never trust a short (or empty) file

    def mask(self, scope, restrict=None) -> np.ndarray:
        self._load()
        m = np.ones(len(self.rowids), dtype=bool)
        if scope.kinds:
            codes = [self.kind_code[k] for k in scope.kinds if k in self.kind_code]
            m &= np.isin(self.kinds, codes)
        if scope.project:
            code = self.project_code.get(scope.project, -1)
            m &= self.projects == code
        if scope.since:
            m &= self.dates >= scope.since
        if restrict is not None:
            m &= np.isin(self.rowids, restrict)
        return m

    def coverage(self, scope, restrict=None) -> dict:
        m = self.mask(scope, restrict)
        return {"embedded": int((m & (self.rows >= 0)).sum()), "in_scope": int(m.sum())}

    def ranked(self, qvec: np.ndarray, scope, depth=DEPTH, restrict=None) -> list[tuple[int, float]]:
        m = self.mask(scope, restrict) & (self.rows >= 0)
        idx = np.nonzero(m)[0]
        if not len(idx):
            return []
        pos, scores = top_rows(self.matrix, self.rows[idx], np.asarray(qvec, dtype=np.float32), depth)
        return [(int(self.rowids[idx[p]]), float(s)) for p, s in zip(pos, scores)]


def _exact_scores(matrix, rows: np.ndarray, q: np.ndarray) -> np.ndarray:
    scores = np.empty(len(rows), dtype=np.float32)
    for i in range(0, len(rows), BLOCK):
        scores[i:i + BLOCK] = np.asarray(matrix[rows[i:i + BLOCK]], dtype=np.float32) @ q
    return scores


def _fp16_scores(matrix, q: np.ndarray) -> np.ndarray | None:
    """Cosine of every matrix row in one float16 matrix-vector product (torch
    reads the float16 memmap as is; numpy has no fast float16 path). None
    when torch is unavailable — the caller then scores exactly."""
    try:
        import torch
    except ImportError:
        return None
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)     # a read-only memmap: only read here
        m = torch.from_numpy(np.asarray(matrix))
    return torch.mv(m, torch.from_numpy(q.astype("<f2"))).float().numpy()


def top_rows(matrix, rows: np.ndarray, q: np.ndarray, depth: int = DEPTH):
    """(positions in `rows`, float32 scores) of the `depth` best cosines, best
    first, ties by position — the exact ranking. `rows[i]` is the matrix row
    of candidate i (chunks with one text share a row). A scope of FAST_MIN or
    more is pre-ranked by `_fp16_scores` over the whole matrix and only its
    best PREFILTER candidates are re-scored in float32: gathering and
    converting 194k×1024 rows in numpy costs ~900 ms a query, the float16
    product ~25 ms [measured 07.10]; float16 alone misorders near-ties."""
    pos = np.arange(len(rows))
    if len(rows) >= FAST_MIN and len(rows) > max(PREFILTER, depth):
        pre = _fp16_scores(matrix, q)
        if pre is not None:
            k = max(PREFILTER, depth)
            pos = np.sort(np.argpartition(-pre[rows], k - 1)[:k])
    scores = _exact_scores(matrix, rows[pos], q)
    order = np.lexsort((pos, -scores))[:depth]
    return pos[order], scores[order]


def rrf(fts: list[int], sem: list[int], dates: dict[int, str], k: int = RRF_K):
    """[(rowid, score, ranks)] best first."""
    fr = {r: i for i, r in enumerate(fts, 1)}
    sr = {r: i for i, r in enumerate(sem, 1)}
    out = []
    for r in set(fr) | set(sr):
        score = (1.0 / (k + fr[r]) if r in fr else 0.0) + (1.0 / (k + sr[r]) if r in sr else 0.0)
        ranks = {}
        if r in fr:
            ranks["fts"] = fr[r]
        if r in sr:
            ranks["sem"] = sr[r]
        out.append((r, score, ranks))
    out.sort(key=lambda x: (-x[1], x[2].get("fts", 10**9), _neg_date(dates.get(x[0], ""))))
    return out


def _neg_date(d: str):
    return tuple(-ord(c) for c in d)


_OPS = re.compile(r"\b(?:AND|OR|NOT|NEAR)\b|[\"*():{}^+]")


def semantic_text(q) -> str:
    """Text for the query embedding: a raw FTS5 query without its operators."""
    if not q.raw:
        return q.text
    return " ".join(_OPS.sub(" ", q.body).split()) or q.body


class Engine:
    """The semantic half of a search: an embedder (loaded now, in the
    background, or never) and the in-memory SemanticIndex. Coverage needs
    only the index, never the model."""

    def __init__(self, index, model_key: str, dim: int, loader=None, embedder=None):
        import threading
        self.index = index
        self.model_key = model_key
        self.sem = SemanticIndex(index, model_key, dim)
        self._loader = loader
        self._emb = embedder
        self.error = None if (embedder is not None or loader is not None) else "no embedding model"
        self._ready = threading.Event()
        self._thread = None
        if embedder is not None or loader is None:
            self._ready.set()

    def _load(self):
        try:
            self._emb = self._loader()
        except Exception as exc:          # noqa: BLE001 — reported as `degraded`
            self.error = f"{type(exc).__name__}: {exc}"
        finally:
            self._ready.set()

    def preload(self):
        """Load the metadata arrays now: done in the background next to the
        model import they lose the GIL and the first query waits ~13 s
        (measured); done first they cost ~1 s of start-up."""
        try:
            self.sem._load()
        except Exception:          # noqa: BLE001 — the first query reports it
            pass

    def start(self):
        """Load the model in a background thread (MCP: right after start-up)."""
        import threading
        if self._thread is None and not self._ready.is_set():
            self._thread = threading.Thread(target=self._load, name="kb-model", daemon=True)
            self._thread.start()

    def embedder(self, wait: float = 60.0):
        """(embedder, None) or (None, reason)."""
        if not self._ready.is_set() and self._thread is None:
            self._load()                   # CLI: synchronous
        if not self._ready.wait(wait):
            return None, f"model still loading after {wait:.0f} s"
        return (self._emb, None) if self._emb is not None else (None, self.error)


def engine_for(index, cfg, threads: int = 4, embedder=None, background=False):
    """Engine for the configured model, or None when none is configured."""
    key = cfg.model_active
    if not key:
        return None
    from .embed import load_embedder
    from .models import SPECS
    dim = SPECS[key].dim if key in SPECS else 64
    try:
        row = index.conn().execute("SELECT dim FROM emb_model WHERE model_key=?", (key,)).fetchone()
        if row:
            dim = row[0]
    except Exception:           # noqa: BLE001 — a missing index is reported by search()
        pass
    eng = Engine(index, key, dim, embedder=embedder,
                 loader=None if embedder is not None else
                 (lambda: load_embedder(key, cfg.hf_home, threads=threads)))
    if background:
        eng.preload()
        eng.start()
    return eng
