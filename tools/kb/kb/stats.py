"""`kb stats`: what is indexed, what is missing, what nobody classified."""

from __future__ import annotations

import json
from pathlib import Path

from . import sources, store
from .config import Config
from .search import Index, index_info


def stats_report(cfg: Config, db_path: Path, check: bool = False) -> tuple[str, int]:
    rc = 0
    idx = Index(db_path)
    conn = idx.conn()
    meta = idx.meta()
    info = index_info(meta)
    out = [f"kb stats — {db_path}",
           f"  updated {info['updated_at']} ({info['age_hours']} h ago), "
           f"last run {meta.get('update_seconds', '?')} s, schema v{meta.get('schema')}"]
    model = cfg.model_active
    rows = conn.execute(
        "SELECT c.kind, count(DISTINCT c.unit_id), count(*), sum(c.n_chars), count(r.row) "
        "FROM chunk c LEFT JOIN emb_row r ON r.model_key = ? AND r.text_sha = c.text_sha "
        "GROUP BY c.kind ORDER BY c.kind", (model or "",)).fetchall()
    out.append(f"  {'kind':<11} {'units':>7} {'chunks':>8} {'chars':>12} {'embedded':>9}  ({model or 'no model'})")
    tot = [0, 0, 0, 0]
    for kind, units, chunks, chars, emb in rows:
        out.append(f"  {kind:<11} {units:>7} {chunks:>8} {chars or 0:>12} {emb:>9}  {emb / chunks:5.1%}")
        tot = [tot[0] + units, tot[1] + chunks, tot[2] + (chars or 0), tot[3] + emb]
    if rows:
        out.append(f"  {'total':<11} {tot[0]:>7} {tot[1]:>8} {tot[2]:>12} {tot[3]:>9}  "
                   f"{tot[3] / max(tot[1], 1):5.1%}")
    from .embed import MatrixStore
    for key, dim in conn.execute("SELECT model_key, dim FROM emb_model ORDER BY model_key"):
        ms = MatrixStore(db_path, key, dim)
        junk, total = ms.garbage(conn)
        path = ms.path(ms.gen(conn))
        size = path.stat().st_size if path.exists() else 0
        out.append(f"  vectors {key}: {total} rows, garbage {junk} ({junk / max(total, 1):.0%}), "
                   f"{size / 1e6:.1f} MB, generation {ms.gen(conn)}")
        if check:
            top = conn.execute("SELECT max(row) FROM emb_row WHERE model_key=?", (key,)).fetchone()[0]
            if top is not None and top >= total:
                out.append(f"  check: vectors {key}: emb_row points past the matrix ({top} >= {total})")
                rc = 3
    last = json.loads(meta.get("last_run") or "{}")
    for m in last.get("missing_roots", []):
        out.append(f"  MISSING ROOT (last run): {m}")
    for src in last.get("refused", []):
        out.append(f"  MASS DELETE REFUSED (last run): {src} — rerun with --allow-mass-delete if intended")
    if cfg.path is not None and cfg.path.exists():
        mode = cfg.path.stat().st_mode & 0o777
        if mode & 0o077:
            out.append(f"  WARNING: {cfg.path} is {mode:04o}: it names private chats — chmod 600")
            rc = 3
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
    if check and model and model != "fake":
        from .models import ModelError, verify
        try:
            verify(cfg.hf_home, model, full=True)
            out.append(f"  check: model {model} files match models.toml (sha256)")
        except ModelError as exc:
            out.append(f"  check: model {model}: {exc}")
            rc = 3
    idx.close()
    return "\n".join(out), rc
