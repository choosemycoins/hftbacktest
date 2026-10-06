"""Golden-set evaluation (DESIGN.md §13) and the model choice (§6.3).

Matching rules for an expected entry:
  ends with '/'   directory prefix (a chat export directory)
  'repo@sha'      a commit chunk of that repository whose sha starts with sha
  otherwise       the exact file (after expanduser)
hit@k     a chunk of an expected source is among the first k results
strict@k  ... and its text (case- and whitespace-insensitive) contains one of
          must_contain
MRR@10    1 / rank of the first hit
Before counting, a drift check moves a query whose expected sources vanished or
lost every must_contain literal into `drifted` — not counted as a miss.

Modes: fts_raw_or (the query's words OR-ed over the raw text: no stemming),
fts (stemmed), semantic, hybrid. Reports go to <db_dir>/eval (0600): ids,
ranks and metrics only, never chunk text.
"""

from __future__ import annotations

import html
import json
import os
import random
import re
import subprocess
from collections import defaultdict
from pathlib import Path

import numpy as np

from . import search as S
from .bench import SEED, write_report

MODES = ("fts_raw_or", "fts", "semantic", "hybrid")
K = (5, 10)


def norm(s: str) -> str:
    return "".join(s.lower().split())


def load_golden(path: Path) -> list[dict]:
    out = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if line.strip():
            out.append(json.loads(line))
    return out


def expand(entry: str) -> str:
    if "@" in entry and not entry.endswith("/"):
        repo, _, sha = entry.rpartition("@")
        return f"{os.path.expanduser(repo)}@{sha}"
    return os.path.expanduser(entry)


# --- drift check ------------------------------------------------------------------

_TAG = re.compile(r"<[^>]+>")


def _strings(obj, out):
    if isinstance(obj, str):
        out.append(obj)
    elif isinstance(obj, dict):
        for v in obj.values():
            _strings(v, out)
    elif isinstance(obj, list):
        for v in obj:
            _strings(v, out)


def source_text(entry: str) -> str | None:
    """Normalized text of an expected source, or None if it does not exist."""
    e = expand(entry)
    if "@" in e and not e.endswith("/"):
        repo, _, sha = e.rpartition("@")
        try:
            out = subprocess.run(["git", "-C", repo, "show", "-s", "--format=%B", sha],
                                 capture_output=True, text=True, check=True).stdout
        except (subprocess.CalledProcessError, OSError):
            return None
        return norm(out)
    p = Path(e)
    if e.endswith("/"):
        if not p.is_dir():
            return None
        parts = []
        for f in sorted(p.glob("messages*.html")):
            parts.append(norm(html.unescape(_TAG.sub(" ", f.read_text(encoding="utf-8", errors="replace")))))
        if (p / "result.json").is_file():
            acc = []
            _strings(json.loads((p / "result.json").read_text(encoding="utf-8")), acc)
            parts.append(norm(" ".join(acc)))
        return "".join(parts)
    if not p.is_file():
        return None
    raw = p.read_text(encoding="utf-8", errors="replace")
    if p.suffix in (".json", ".jsonl"):
        acc = []
        for line in (raw.splitlines() if p.suffix == ".jsonl" else [raw]):
            try:
                _strings(json.loads(line), acc)
            except ValueError:
                continue
        return norm(" ".join(acc))
    return norm(raw)


def drift_check(golden: list[dict]) -> tuple[list[dict], list[dict]]:
    ok, drifted = [], []
    cache: dict[str, str | None] = {}
    for g in golden:
        texts = []
        for e in g["expect"]:
            if e not in cache:
                cache[e] = source_text(e)
            texts.append(cache[e])
        present = [t for t in texts if t is not None]
        literal = any(norm(m) in t for m in g["must_contain"] for t in present)
        if not present or not literal:
            drifted.append({"id": g["id"], "reason": "source missing" if not present else "literal lost"})
        else:
            ok.append(g)
    return ok, drifted


# --- matching -----------------------------------------------------------------------

def chunk_path(hit) -> str:
    if hit.kind == "chat":
        return f"{hit.abs_path}/{hit.file}"
    return hit.abs_path


def matches(hit, entry: str) -> bool:
    e = expand(entry)
    if e.endswith("/"):
        return chunk_path(hit).startswith(e)
    if "@" in e:
        repo, _, sha = e.rpartition("@")
        return hit.kind == "commit" and hit.abs_path == repo and hit.loc.startswith(sha)
    return hit.abs_path == e and hit.kind != "commit"


def judge(hits: list, g: dict) -> dict:
    lits = [norm(m) for m in g["must_contain"]]
    first_hit = first_strict = None
    for rank, h in enumerate(hits, 1):
        if any(matches(h, e) for e in g["expect"]):
            first_hit = first_hit or rank
            if first_strict is None and any(m in norm(h.text) for m in lits):
                first_strict = rank
    return {"hit": first_hit, "strict": first_strict}


# --- running ------------------------------------------------------------------------

def run_query(index, engine, g: dict, mode: str, restrict, limit=10) -> list:
    f = g.get("filters") or {}
    kw = dict(kind=f.get("kind"), project=f.get("project"), since=f.get("since"), limit=limit,
              engine=engine, restrict=restrict)
    if mode == "fts_raw_or":
        resp = S.search(index, g["query"], mode="fts", fts_query=S.raw_or_query, **kw)
    else:
        resp = S.search(index, g["query"], mode=mode, **kw)
        if mode != "fts" and resp["query"]["used"] != mode:
            raise RuntimeError(f"{mode} degraded: {resp['degraded']}")
    conn = index.conn()
    ids = [r["id"][3:] for r in resp["results"]]
    rows = {r[0]: r[1] for r in conn.execute(
        f"SELECT chunk_id, rowid FROM chunk WHERE chunk_id IN ({','.join('?' * len(ids))})", ids)} if ids else {}
    hits = S.fetch_hits(conn, list(rows.values()))
    return [hits[rows[i]] for i in ids]


def metrics(rows: list[dict]) -> dict:
    n = len(rows)
    out = {"n": n}
    for k in K:
        out[f"hit@{k}"] = sum(1 for r in rows if r["hit"] and r["hit"] <= k)
        out[f"strict@{k}"] = sum(1 for r in rows if r["strict"] and r["strict"] <= k)
    out["mrr@10"] = round(sum(1 / r["hit"] for r in rows if r["hit"] and r["hit"] <= 10) / max(n, 1), 3)
    return out


def evaluate(index, golden: list[dict], modes, engine=None, restrict=None, log=print) -> dict:
    ok, drifted = drift_check(golden)
    per = defaultdict(list)
    for g in ok:
        for mode in modes:
            hits = run_query(index, engine, g, mode, restrict)
            j = judge(hits, g)
            per[mode].append({"id": g["id"], "tags": g.get("tags", []), **j,
                              "top": [f"kb:{h.chunk_id}" for h in hits[:3]]})
    report = {"queries": len(golden), "counted": len(ok), "drifted": drifted, "modes": {}}
    for mode in modes:
        rows = per[mode]
        tags = sorted({t for r in rows for t in r["tags"]})
        report["modes"][mode] = {"all": metrics(rows),
                                 "by_tag": {t: metrics([r for r in rows if t in r["tags"]]) for t in tags},
                                 "rows": rows}
    return report


def table(report: dict, title: str = "") -> str:
    lines = [title] if title else []
    lines.append(f"queries {report['queries']}, counted {report['counted']}, drifted "
                 f"{len(report['drifted'])}")
    lines.append(f"{'mode':<12} {'hit@5':>6} {'hit@10':>7} {'strict@5':>9} {'strict@10':>10} {'MRR@10':>7}")
    for mode, r in report["modes"].items():
        a = r["all"]
        lines.append(f"{mode:<12} {a['hit@5']:>6} {a['hit@10']:>7} {a['strict@5']:>9} "
                     f"{a['strict@10']:>10} {a['mrr@10']:>7}")
    return "\n".join(lines)


# --- sub-corpus for the model comparison (§6.3) --------------------------------------

def subcorpus(conn, golden: list[dict], distractors: int, seed: int = SEED,
              per_chat: int = 200) -> np.ndarray:
    """All chunks of the expected sources (chat dirs: windows holding a
    must_contain literal, at most `per_chat`) + kind-stratified random
    distractors."""
    keep: set[int] = set()
    for g in golden:
        lits = [norm(m) for m in g["must_contain"]]
        for e in g["expect"]:
            x = expand(e)
            if x.endswith("/"):
                rows = conn.execute(
                    "SELECT c.rowid, c.text FROM chunk c JOIN unit u USING(unit_id) "
                    "WHERE c.kind='chat' AND substr(u.abs_path || '/' || c.file, 1, ?) = ? "
                    "ORDER BY c.rowid", (len(x), x)).fetchall()
                hits = [r for r, t in rows if any(m in norm(t) for m in lits)]
                keep.update(hits[:per_chat])
            elif "@" in x:
                repo, _, sha = x.rpartition("@")
                keep.update(r[0] for r in conn.execute(
                    "SELECT c.rowid FROM chunk c JOIN unit u USING(unit_id) WHERE c.kind='commit' "
                    "AND u.abs_path=? AND substr(c.loc, 1, ?) = ?", (repo, len(sha), sha)))
            else:
                keep.update(r[0] for r in conn.execute(
                    "SELECT c.rowid FROM chunk c JOIN unit u USING(unit_id) WHERE u.abs_path=? "
                    "AND c.kind != 'commit'", (x,)))
    counts = dict(conn.execute("SELECT kind, count(*) FROM chunk GROUP BY kind"))
    total = sum(counts.values())
    rng = random.Random(seed)
    for kind in sorted(counts):
        ids = [r[0] for r in conn.execute("SELECT rowid FROM chunk WHERE kind=? ORDER BY rowid", (kind,))
               if r[0] not in keep]
        k = min(len(ids), round(distractors * counts[kind] / total))
        keep.update(rng.sample(ids, k))
    return np.array(sorted(keep), dtype=np.int64)


def discordant(a: dict, b: dict, mode="hybrid", metric="strict", k=10) -> dict:
    """Queries model b gets right (metric@k) and a does not, and vice versa."""
    ra = {r["id"]: r for r in a["modes"][mode]["rows"]}
    rb = {r["id"]: r for r in b["modes"][mode]["rows"]}
    ok = lambda r: bool(r[metric]) and r[metric] <= k
    b_only = sorted(i for i in ra if ok(rb[i]) and not ok(ra[i]))
    a_only = sorted(i for i in ra if ok(ra[i]) and not ok(rb[i]))
    return {"b_wins": b_only, "a_wins": a_only, "net_b": len(b_only) - len(a_only)}


def decide(report_e5b: dict, report_m3: dict, delta_minutes_m3: float | None) -> dict:
    """§6.3 rule, written before counting: m3 if it wins >= 4 more hybrid
    strict@10 queries than it loses AND its CPU daily delta <= 60 min; else e5b."""
    d = discordant(report_e5b, report_m3)
    speed_ok = delta_minutes_m3 is not None and delta_minutes_m3 <= 60
    choice = "m3" if d["net_b"] >= 4 and speed_ok else "e5b"
    return {"choice": choice, "net_m3_minus_e5b": d["net_b"], "m3_wins": d["b_wins"],
            "e5b_wins": d["a_wins"], "m3_daily_delta_minutes": delta_minutes_m3,
            "speed_ok": speed_ok}


def save(db_path: Path, name: str, data: dict) -> Path:
    return write_report(db_path, name, data)
