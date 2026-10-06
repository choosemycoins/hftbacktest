"""Building the index: `kb build --full` (fresh file, atomic swap)."""

from __future__ import annotations

import hashlib
import json
import os
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

from . import secure, sources, store
from .config import Config


def content_sha(files) -> str | None:
    if not files:
        return None
    h = hashlib.sha256()
    for f in files:
        with open(f, "rb") as fh:
            for block in iter(lambda: fh.read(1 << 20), b""):
                h.update(block)
    return h.hexdigest()


def _warn_default(msg: str) -> None:
    print(f"kb: warning: {msg}", file=sys.stderr)


def _fsync(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _index_unit(conn, cand, totals, warn) -> None:
    kind = cand.kind
    try:
        pieces, st = cand.parse()
        sha = content_sha(cand.files)
    except Exception as exc:          # one unreadable unit must not sink the build
        totals[kind]["failed"] += 1
        warn(f"{kind}: cannot parse {cand.abs_path}: {type(exc).__name__}")
        return
    res = store.write_unit(conn, cand, pieces, sha)
    for k in ("added", "updated", "deleted"):
        totals[kind][k] += res[k]
    totals[kind]["units"] += 1
    totals[kind]["malformed"] += st.get("malformed", 0)
    totals[kind]["unparsable"] += st.get("unparsable", 0)


def build_full(cfg: Config, db_path=None, log=None, warn=None) -> dict:
    """Index everything into a temp file next to the database, then rename it
    over the old one: an interrupted or failing build leaves the old intact."""
    db_path = Path(db_path or cfg.db)
    log = log or (lambda m: None)
    warn = warn or _warn_default
    started = time.monotonic()
    dir_managed = secure._secure_dir(db_path, warn)
    tmp = db_path.parent / f".{db_path.name}.tmp-{os.getpid()}"
    for leftover in (tmp, Path(f"{tmp}-journal")):
        if leftover.exists():
            leftover.unlink()
    totals: dict = defaultdict(Counter)
    missing: list[str] = []
    conn = None
    try:
        conn = store.open_rw(tmp)
        os.chmod(tmp, secure.DB_MODE)
        conn.execute("PRAGMA synchronous=OFF")     # not live until fsync + rename
        store.init_schema(conn)
        for scan in sources.scan_all(cfg):
            t0 = time.monotonic()
            for item in scan.missing:
                warn(f"{scan.source_id}: source root not indexed: {item}")
            missing += scan.missing
            for note in scan.notes:
                warn(f"{scan.source_id}: {note}")
            for cand in scan.candidates:
                _index_unit(conn, cand, totals, warn)
            for k, v in scan.skipped.items():
                totals["_skipped"][k] += v
            log(f"  {scan.source_id:<28} units={len(scan.candidates):<6} "
                f"{time.monotonic() - t0:7.1f}s")
        elapsed = round(time.monotonic() - started, 1)
        store.set_meta(conn, updated_at=store.now_iso(), update_seconds=elapsed,
                       config_sha=cfg.sha, last_run=json.dumps(
                           {"mode": "full", "missing_roots": missing,
                            "kinds": {k: dict(v) for k, v in totals.items()}}, ensure_ascii=False))
        conn.execute("INSERT INTO chunk_fts(chunk_fts) VALUES('optimize')")
        conn.close()
        conn = None
        _fsync(tmp)
        os.chmod(tmp, secure.DB_MODE)
        os.replace(tmp, db_path)
    except BaseException:
        if conn is not None:
            conn.close()
        for leftover in (tmp, Path(f"{tmp}-journal")):
            try:
                leftover.unlink()
            except OSError:
                pass
        raise
    os.chmod(db_path, secure.DB_MODE)
    if dir_managed:
        os.chmod(db_path.parent, secure.DIR_MODE)
    return {"kinds": {k: dict(v) for k, v in totals.items()}, "missing_roots": missing,
            "dir_managed": dir_managed, "db_bytes": db_path.stat().st_size,
            "elapsed": round(time.monotonic() - started, 1)}
