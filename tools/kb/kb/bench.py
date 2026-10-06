"""`kb bench-embed`: embedding speed on a fixed, kind-stratified sample of the
real chunks (seed 20261007) -> chunks/s, tokens/s, peak RSS, and from them
the full backfill and the daily delta. Reports go to <db_dir>/eval/ (0600);
they hold numbers and model names, never text."""

from __future__ import annotations

import json
import os
import random
import resource
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from . import store
from .embed import load_embedder, passage_text

SEED = 20261007


def private_dir(path: Path) -> Path:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path, 0o700)
    return path


def write_report(db_path: Path, name: str, data: dict) -> Path:
    d = private_dir(Path(db_path).parent / "eval")
    p = d / name
    fd = os.open(p, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=1)
    os.chmod(p, 0o600)
    return p


def stratified_sample(conn, n: int, seed: int = SEED) -> list[int]:
    counts = dict(conn.execute("SELECT kind, count(*) FROM chunk GROUP BY kind"))
    total = sum(counts.values())
    rng = random.Random(seed)
    out = []
    for kind in sorted(counts):
        k = max(1, round(n * counts[kind] / total))
        ids = [r[0] for r in conn.execute("SELECT rowid FROM chunk WHERE kind=? ORDER BY rowid", (kind,))]
        out += rng.sample(ids, min(k, len(ids)))
    return sorted(out)


def daily_new_chunks(conn, days: int = 14) -> float:
    since = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")
    n = conn.execute("SELECT count(*) FROM chunk WHERE date >= ?", (since,)).fetchone()[0]
    return n / days


def bench_embed(cfg, db_path: Path, model: str, n: int, threads: int, device: str, log=print) -> dict:
    conn = store.open_ro(db_path)
    try:
        store.check_schema(conn)
        ids = stratified_sample(conn, n)
        texts = [passage_text(t, x) for t, x in conn.execute(
            f"SELECT title, text FROM chunk WHERE rowid IN ({','.join('?' * len(ids))})", ids)]
        total = conn.execute("SELECT count(*) FROM chunk").fetchone()[0]
        daily = daily_new_chunks(conn)
    finally:
        conn.close()
    t0 = time.monotonic()
    emb = load_embedder(model, cfg.hf_home, threads=threads, device=device)
    load_s = time.monotonic() - t0
    emb.embed_passages(texts[:8])                        # warm-up
    tokens = emb.count_tokens(texts) if hasattr(emb, "count_tokens") else 0
    log(f"bench-embed {model}: {len(texts)} chunks, {tokens} tokens, {threads} threads, {device}")
    t0 = time.monotonic()
    emb.embed_passages(texts)
    secs = time.monotonic() - t0
    q0 = time.monotonic()
    for q in ("комиссия мейкера на Lighter", "Hyperliquid request budget", "почему бот упал"):
        emb.embed_query(q)
    query_ms = (time.monotonic() - q0) / 3 * 1000
    rate = len(texts) / secs
    rss_gb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1e6
    report = {
        "model": model, "device": device, "threads": threads, "n": len(texts), "seed": SEED,
        "tokens": tokens, "seconds": round(secs, 1), "chunks_per_s": round(rate, 2),
        "tokens_per_s": round(tokens / secs, 1), "load_seconds": round(load_s, 1),
        "query_ms": round(query_ms, 1), "peak_rss_gb": round(rss_gb, 2),
        "corpus_chunks": total, "backfill_hours": round(total / rate / 3600, 1),
        "daily_new_chunks": round(daily, 1), "daily_delta_minutes": round(daily / rate / 60, 1),
        "at": store.now_iso(), "load_avg": os.getloadavg(),
    }
    p = write_report(db_path, f"bench-{model}-{report['at'].replace(':', '')}.json", report)
    report["report"] = str(p)
    return report
