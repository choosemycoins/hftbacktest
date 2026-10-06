"""`kb stats`: what is indexed, what is missing, what nobody classified."""

from __future__ import annotations

import json
from pathlib import Path

from . import sources, store
from .config import Config
from .search import Index, index_info


def stats_report(cfg: Config, db_path: Path, check: bool = False) -> tuple[str, int]:
    idx = Index(db_path)
    conn = idx.conn()
    meta = idx.meta()
    info = index_info(meta)
    out = [f"kb stats — {db_path}",
           f"  updated {info['updated_at']} ({info['age_hours']} h ago), "
           f"last run {meta.get('update_seconds', '?')} s, schema v{meta.get('schema')}"]
    rows = conn.execute(
        "SELECT kind, count(DISTINCT unit_id), count(*), sum(n_chars) FROM chunk GROUP BY kind "
        "ORDER BY kind").fetchall()
    out.append(f"  {'kind':<11} {'units':>7} {'chunks':>8} {'chars':>12}")
    for kind, units, chunks, chars in rows:
        out.append(f"  {kind:<11} {units:>7} {chunks:>8} {chars or 0:>12}")
    rc = 0
    last = json.loads(meta.get("last_run") or "{}")
    for m in last.get("missing_roots", []):
        out.append(f"  MISSING ROOT (last run): {m}")
    for k, v in sorted(last.get("kinds", {}).items()):
        bad = {x: v[x] for x in ("malformed", "failed", "unparsable") if v.get(x)}
        if bad:
            out.append(f"  {k}: " + " ".join(f"{x}={n}" for x, n in bad.items()))
    for course in cfg.courses:
        done, total = sources.asr_progress(course)
        errs = len(list(course.asr.glob("*.err"))) if course.asr and course.asr.is_dir() else 0
        out.append(f"  ASR {course.id}: {done} of {total if total is not None else '?'} videos"
                   + (f", {errs} .err" if errs else ""))
    unclassified = sources.unclassified_exports(cfg)
    if unclassified:
        out.append(f"  Telegram exports NOT CLASSIFIED (neither allow nor ignore, not indexed): "
                   + ", ".join(unclassified))
    size = db_path.stat().st_size
    out.append(f"  db {size / 1e6:.1f} MB")
    if check:
        try:
            # the check is an INSERT-shaped command: it needs a read-write handle
            # (it writes nothing); rank=1 also compares the index with the content
            conn_rw = store.open_rw(db_path)
            try:
                conn_rw.execute("INSERT INTO chunk_fts(chunk_fts, rank) VALUES('integrity-check', 1)")
            finally:
                conn_rw.close()
            out.append("  check: FTS integrity ok")
        except Exception as exc:          # noqa: BLE001 — reported, exit 3
            out.append(f"  check: FTS integrity FAILED: {exc}")
            rc = 3
    idx.close()
    return "\n".join(out), rc
