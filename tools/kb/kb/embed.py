"""Embeddings (DESIGN.md §6): the embedder protocol, a deterministic fake for
tests, the transformers backend, the float16 matrix store and the
incremental, budgeted embedding pass.

Storage: <db_dir>/emb/<model_key>/g<gen>.f16 — raw little-endian float16,
row-major, no header; rows = size / (2·dim). Rows are addressed by CONTENT
(emb_row: text_sha -> row), so the same text in two places is embedded once,
a re-parse that keeps the text costs nothing, and `kb build --full` carries
the vectors over. The file only grows; garbage (vectors of texts no longer in
the index, tails of interrupted appends) is dropped by writing generation
g+1 and switching to it in one transaction. Vectors are partly invertible to
text: the matrix is as secret as the database (0600 in a 0700 directory).
"""

from __future__ import annotations

import hashlib
import os
import time
from pathlib import Path
from typing import Protocol

import numpy as np

from . import store
from .stem import stems

KIND_ORDER = ("memory", "doc", "note", "course", "commit", "export", "transcript", "chat")
GROUP = 1000                 # chunks per append/transaction
BATCH = 32                   # chunks per forward pass
GC_SHARE, GC_MIN_ROWS = 0.25, 10_000


class Embedder(Protocol):
    model_key: str
    dim: int

    def embed_passages(self, texts: list[str]) -> np.ndarray: ...

    def embed_query(self, text: str) -> np.ndarray: ...


def passage_text(title: str, text: str) -> str:
    return f"{title}\n{text}" if title else text


# ---------------------------------------------------------------------------
# fake (tests): stems -> hashed signed buckets
# ---------------------------------------------------------------------------

class FakeEmbedder:
    """Deterministic and offline: each stem adds ±1 to a hashed bucket. An
    optional synonym table maps stems to one canonical token, so a test can
    show the semantic list finding a chunk with no word in common."""

    revision = "fake"
    repo_id = "fake"
    max_tokens = 512
    query_prefix = passage_prefix = ""

    def __init__(self, dim: int = 64, synonyms: dict[str, str] | None = None, key: str = "fake"):
        self.dim = dim
        self.model_key = key
        self.synonyms = {}
        for a, b in (synonyms or {}).items():
            canon = stems(b)[0] if stems(b) else b
            for w in (a, b):
                for s in stems(w):
                    self.synonyms[s] = canon

    def _vec(self, text: str) -> np.ndarray:
        v = np.zeros(self.dim, dtype=np.float32)
        for s in stems(text):
            s = self.synonyms.get(s, s)
            h = hashlib.blake2b(s.encode("utf-8"), digest_size=8).digest()
            idx = int.from_bytes(h[:4], "little") % self.dim
            v[idx] += 1.0 if h[4] & 1 else -1.0
        n = np.linalg.norm(v)
        return v / n if n else v

    def embed_passages(self, texts):
        return np.stack([self._vec(t) for t in texts]) if texts else np.zeros((0, self.dim), np.float32)

    def embed_query(self, text):
        return self._vec(text)


# ---------------------------------------------------------------------------
# transformers backend (CPU by default)
# ---------------------------------------------------------------------------

class TransformerEmbedder:
    def __init__(self, key: str, model_dir: Path, revision: str, threads: int = 16,
                 device: str = "cpu"):
        from .netguard import force_offline_env
        force_offline_env()
        import torch
        from transformers import AutoModel, AutoTokenizer
        from transformers.utils import logging as tlog
        from .models import SPECS
        tlog.set_verbosity_error()
        tlog.disable_progress_bar()
        spec = SPECS[key]
        self.spec = spec
        self.model_key = key
        self.dim = spec.dim
        self.max_tokens = spec.max_tokens
        self.query_prefix, self.passage_prefix = spec.query_prefix, spec.passage_prefix
        self.repo_id, self.revision = spec.repo_id, revision
        self.device = device
        torch.set_num_threads(threads)
        self._torch = torch
        self.tok = AutoTokenizer.from_pretrained(str(model_dir), local_files_only=True)
        self.model = AutoModel.from_pretrained(str(model_dir), local_files_only=True,
                                               use_safetensors=True, trust_remote_code=False)
        self.model.eval().to(device)

    def _encode(self, texts: list[str]) -> np.ndarray:
        torch = self._torch
        out = []
        order = sorted(range(len(texts)), key=lambda i: len(texts[i]))
        with torch.inference_mode():
            for i in range(0, len(order), BATCH):
                idx = order[i:i + BATCH]
                enc = self.tok([texts[j] for j in idx], padding=True, truncation=True,
                               max_length=self.max_tokens, return_tensors="pt").to(self.device)
                h = self.model(**enc).last_hidden_state
                if self.spec.pooling == "cls":
                    e = h[:, 0]
                else:
                    m = enc["attention_mask"].unsqueeze(-1).to(h.dtype)
                    e = (h * m).sum(1) / m.sum(1).clamp(min=1e-9)
                e = torch.nn.functional.normalize(e.float(), dim=-1)
                out.append((idx, e.cpu().numpy()))
        res = np.zeros((len(texts), self.dim), dtype=np.float32)
        for idx, e in out:
            res[idx] = e
        return res

    def embed_passages(self, texts):
        return self._encode([self.passage_prefix + t for t in texts])

    def embed_query(self, text):
        return self._encode([self.query_prefix + text])[0]

    def count_tokens(self, texts: list[str]) -> int:
        return sum(min(len(ids), self.max_tokens) for ids in self.tok(
            [self.passage_prefix + t for t in texts], truncation=False)["input_ids"])


def load_embedder(key: str, hf_home: Path, threads: int = 16, device: str = "cpu",
                  full_verify: bool = False):
    """The configured model, verified against models.toml (sizes; sha256 too
    when `full_verify`). key 'fake' -> FakeEmbedder (tests, smoke runs)."""
    if key == "fake":
        return FakeEmbedder()
    from .models import load_manifest, verify
    manifest = load_manifest()
    d = verify(hf_home, key, full=full_verify, manifest=manifest)
    return TransformerEmbedder(key, d, manifest[key]["revision"], threads=threads, device=device)


# ---------------------------------------------------------------------------
# matrix store
# ---------------------------------------------------------------------------

class MatrixStore:
    def __init__(self, db_path: Path, model_key: str, dim: int):
        self.db_path = Path(db_path)
        self.key = model_key
        self.dim = dim
        self.row_bytes = 2 * dim
        self.dir = self.db_path.parent / "emb" / model_key

    def path(self, gen: int) -> Path:
        return self.dir / f"g{gen}.f16"

    def _ensure_dir(self):
        for d in (self.dir.parent, self.dir):
            d.mkdir(mode=0o700, exist_ok=True)
            os.chmod(d, 0o700)

    def ensure_model(self, conn, emb) -> int:
        row = conn.execute("SELECT gen, dim FROM emb_model WHERE model_key=?", (self.key,)).fetchone()
        if row:
            if row[1] != self.dim:
                raise ValueError(f"{self.key}: dim {self.dim} != stored {row[1]}")
            return row[0]
        with store.transaction(conn):
            conn.execute("INSERT INTO emb_model VALUES (?,?,?,?,?,?,?,?,?)",
                         (self.key, emb.repo_id, emb.revision, self.dim, emb.max_tokens,
                          emb.query_prefix, emb.passage_prefix, 0, store.now_iso()))
        return 0

    def gen(self, conn) -> int:
        row = conn.execute("SELECT gen FROM emb_model WHERE model_key=?", (self.key,)).fetchone()
        return row[0] if row else 0

    def clean_orphans(self, conn) -> list[str]:
        """Matrix files of other generations: leftovers of an interrupted gc."""
        if not self.dir.is_dir():
            return []
        cur = self.path(self.gen(conn)).name
        gone = []
        for p in self.dir.glob("g*.f16*"):
            if p.name != cur:
                p.unlink()
                gone.append(p.name)
        return gone

    def append(self, conn, shas: list[str], vecs: np.ndarray) -> None:
        """Append rows -> fsync -> map them in one transaction. A crash before
        the commit leaves an unreferenced tail (garbage), never a wrong row."""
        assert vecs.shape == (len(shas), self.dim)
        self._ensure_dir()
        path = self.path(self.gen(conn))
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            size = os.fstat(fd).st_size
            whole = size - size % self.row_bytes
            if whole != size:
                os.ftruncate(fd, whole)            # a row cut short by a crash
            base = whole // self.row_bytes
            os.lseek(fd, whole, os.SEEK_SET)
            data = np.ascontiguousarray(vecs, dtype="<f2").tobytes()
            view = memoryview(data)
            while view:
                n = os.write(fd, view)
                view = view[n:]
            os.fsync(fd)
        finally:
            os.close(fd)
        with store.transaction(conn):
            conn.executemany("INSERT OR REPLACE INTO emb_row(model_key, text_sha, row) VALUES (?,?,?)",
                             [(self.key, s, base + i) for i, s in enumerate(shas)])
            v = int(store.get_meta(conn, "emb_version", "0")) + 1
            store.set_meta(conn, emb_version=str(v))

    def load(self, conn) -> np.ndarray:
        path = self.path(self.gen(conn))
        if not path.exists():
            return np.zeros((0, self.dim), dtype=np.float16)
        n = path.stat().st_size // self.row_bytes
        if n == 0:
            return np.zeros((0, self.dim), dtype=np.float16)
        return np.memmap(path, dtype="<f2", mode="r", shape=(n, self.dim))

    def garbage(self, conn) -> tuple[int, int]:
        path = self.path(self.gen(conn))
        rows = path.stat().st_size // self.row_bytes if path.exists() else 0
        live = conn.execute(
            "SELECT count(*) FROM emb_row r WHERE r.model_key=? AND EXISTS "
            "(SELECT 1 FROM chunk c WHERE c.text_sha = r.text_sha)", (self.key,)).fetchone()[0]
        return rows - live, rows

    def gc(self, conn, force: bool = False) -> int:
        junk, rows = self.garbage(conn)
        if not force and not (junk > GC_SHARE * rows and junk > GC_MIN_ROWS):
            return 0
        live = conn.execute(
            "SELECT r.text_sha, r.row FROM emb_row r WHERE r.model_key=? AND EXISTS "
            "(SELECT 1 FROM chunk c WHERE c.text_sha = r.text_sha) ORDER BY r.row",
            (self.key,)).fetchall()
        old_gen = self.gen(conn)
        src = self.load(conn)
        new_path = self.path(old_gen + 1)
        self._ensure_dir()
        fd = os.open(new_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            for i in range(0, len(live), 32768):
                part = np.asarray(src[[r for _, r in live[i:i + 32768]]], dtype="<f2")
                os.write(fd, part.tobytes())
            os.fsync(fd)
        finally:
            os.close(fd)
        with store.transaction(conn):
            conn.execute("DELETE FROM emb_row WHERE model_key=?", (self.key,))
            conn.executemany("INSERT INTO emb_row(model_key, text_sha, row) VALUES (?,?,?)",
                             [(self.key, s, i) for i, (s, _) in enumerate(live)])
            conn.execute("UPDATE emb_model SET gen=? WHERE model_key=?", (old_gen + 1, self.key))
            store.set_meta(conn, emb_version=str(int(store.get_meta(conn, "emb_version", "0")) + 1))
        del src
        old = self.path(old_gen)
        if old.exists():
            old.unlink()
        return junk


# ---------------------------------------------------------------------------
# the pass
# ---------------------------------------------------------------------------

def pending(conn, key: str, restrict: set[int] | None = None) -> list[tuple[str, int]]:
    """[(text_sha, rowid of one chunk with that text)] not yet embedded,
    in D14 order: memory → doc → note → course → commit → export →
    transcript → chat, newest first inside a kind."""
    rows = conn.execute(
        "SELECT c.text_sha, c.rowid, c.kind, c.date FROM chunk c LEFT JOIN emb_row r "
        "ON r.model_key = ? AND r.text_sha = c.text_sha WHERE r.text_sha IS NULL", (key,)).fetchall()
    rank = {k: i for i, k in enumerate(KIND_ORDER)}
    rows.sort(key=lambda r: r[3], reverse=True)
    rows.sort(key=lambda r: rank.get(r[2], len(rank)))
    seen, out = set(), []
    for sha, rowid, _, _ in rows:
        if sha in seen or (restrict is not None and rowid not in restrict):
            continue
        seen.add(sha)
        out.append((sha, rowid))
    return out


def embed_pass(conn, db_path: Path, emb, budget_minutes: float | None = None,
               max_chunks: int | None = None, restrict: set[int] | None = None, log=None) -> dict:
    log = log or (lambda m: None)
    t0 = time.monotonic()
    ms = MatrixStore(db_path, emb.model_key, emb.dim)
    ms.ensure_model(conn, emb)
    ms.clean_orphans(conn)
    todo = pending(conn, emb.model_key, restrict)
    total = len(todo)
    if max_chunks is not None:
        todo = todo[:max_chunks]
    deadline = None if budget_minutes is None else t0 + budget_minutes * 60
    done = 0
    for i in range(0, len(todo), GROUP):
        if deadline is not None and time.monotonic() >= deadline:
            break
        group = todo[i:i + GROUP]
        texts = {}
        for j in range(0, len(group), 500):
            part = group[j:j + 500]
            q = f"SELECT rowid, title, text FROM chunk WHERE rowid IN ({','.join('?' * len(part))})"
            for rowid, title, text in conn.execute(q, [r for _, r in part]):
                texts[rowid] = passage_text(title, text)
        vecs = emb.embed_passages([texts[r] for _, r in group])
        ms.append(conn, [s for s, _ in group], vecs)
        done += len(group)
        rate = done / max(1e-9, time.monotonic() - t0)
        log(f"  embedded {done}/{total} ({rate:.1f}/s)")
    collected = ms.gc(conn)
    store.set_meta(conn, active_model=emb.model_key)
    return {"model": emb.model_key, "embedded": done, "remaining": total - done,
            "garbage_collected": collected, "seconds": round(time.monotonic() - t0, 1)}
