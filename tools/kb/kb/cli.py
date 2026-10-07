"""kb — a local index over everything that was DISCUSSED but never distilled:
Claude session transcripts and /export files, memory, every markdown doc of
the repos (hftbacktest, myhft, eventshort, jansen), ~/hft-research notes,
course transcripts and notes, allowlisted Telegram chats, git commits.

It answers "where was this talked about?", not "what is the answer?". Every
result is a POINTER — id (kb:<16 hex>), kind, project, date, where (path +
line range / time / message range / sha) — plus a snippet for orientation.
Open the source and read it before believing anything.

THE DATABASE IS A SECRET, AND SO IS THIS TOOL'S OUTPUT
  Transcripts and chats routinely contain pasted keys and addresses. The
  database and the embedding matrices are 0600; their directory is 0700 when
  kb owns it (created it, found it empty, or finds the .kbindex-dir marker) —
  a directory holding unrelated files is never re-chmodded. Never commit,
  sync, upload or copy them off this machine; they are derived artifacts:
  delete and rebuild. Everything printed (CLI, MCP) is masked best-effort
  (keys, addresses, tokens); masking is a net, NOT a guarantee — keep the
  output local.

SEARCH
  natural (default)  words of the query, minus stop words, STEMMED (Snowball
                     ru/en, ё = е) and OR-ed over a stemmed shadow column,
                     ranked by BM25: `терять` finds `терял`, `комиссиях`.
                     Not every form converges (`отмена`/`отменить`,
                     `котировать`/`котировки`) — that is what semantic is for.
  raw:<FTS5 query>   passed to SQLite FTS5 over the RAW title+text: exact
                     forms, "phrase", prefix*, AND/OR/NOT, NEAR(a b, 10).
                     Tokenizer unicode61 (case-folding, diacritics removed;
                     ё is NOT folded in raw mode).
  Filters: --kind (comma list), --project, --since YYYY-MM-DD.

LIMITATIONS worth knowing before you trust an empty result
  * An empty result is not evidence the topic was never discussed; the
    coverage line says how much of the scope each mode could see.
  * The session you are in right now is indexed too once the index updates,
    so a phrase you just typed matches itself.
  * Dates of files are their mtime (an approximation); transcript messages,
    chat windows and commits carry their own timestamps.
  * Only allowlisted Telegram chats are indexed; `kb stats` lists exports
    that are neither allowed nor ignored.

NETWORK
  Only `kb fetch-model` touches the network (one-time model download).

EXIT CODES  0 ok · 2 bad query/usage · 3 incomplete run (missing root, guard,
            parse failure) · 75 deferred (memory pressure, lock held while waiting).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from . import config as cfgmod


def _db(args, cfg) -> Path:
    return Path(args.db).expanduser() if args.db else cfg.db


def _print_hits(resp: dict, query: str) -> str:
    from .redact import redact
    out = [resp["notice"]]
    idx = resp["index"]
    cov = resp["coverage"]
    age = f" ({idx['age_hours']} h ago)" if idx.get("age_hours") is not None else ""
    out.append(f"index: updated {idx.get('updated_at') or '?'}{age} · mode {resp['query']['mode']}"
               f" · model {idx.get('model') or '-'} · embedded {cov['embedded']}/{cov['in_scope']}"
               " in scope")
    if resp.get("degraded"):
        out.append(f"degraded: {resp['degraded']}")
    out.append(f"query: {redact(query)}")
    if not resp["results"]:
        out.append("(no matches — not evidence of absence; try other words, raw: prefix*, "
                   "or a wider scope)")
        return "\n".join(out)
    out.append("")
    for i, r in enumerate(resp["results"], 1):
        out.append(f"{i:>3}. {r['kind']} | {r['project']} | {r['date'][:10]} | {r['where']}  ({r['id']})")
        out.append(f"     {r['snippet']}")
    return "\n".join(redact(line) for line in out)


def cmd_search(args, cfg) -> int:
    from .redact import redact_obj
    from .search import Index, QueryError, search
    from .semantic import engine_for
    idx = Index(_db(args, cfg))
    try:
        engine = engine_for(idx, cfg, threads=args.threads) if idx.path.exists() else None
        resp = search(idx, args.query, mode=args.mode, kind=args.kind, project=args.project,
                      since=args.since, limit=args.limit, engine=engine)
    except QueryError as exc:
        print(f"kb: {exc}", file=sys.stderr)
        return 2
    resp = redact_obj(resp)
    if args.json:
        print(json.dumps(resp, ensure_ascii=False, indent=1))
    else:
        print(_print_hits(resp, args.query))
    return 0


def cmd_read(args, cfg) -> int:
    from .redact import redact_obj
    from .search import Index, QueryError, read_pointer
    try:
        resp = redact_obj(read_pointer(Index(_db(args, cfg)), args.pointer, args.context))
    except QueryError as exc:
        print(f"kb: {exc}", file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps(resp, ensure_ascii=False, indent=1))
        return 0
    print(f"{resp['id']} | {resp['kind']} | {resp['project']} | {resp['date']} | {resp['where']}"
          + (" | STALE: the source changed since indexing" if resp["stale"] else ""))
    print(f"title: {resp['title']}")
    for t in resp["before"]:
        print("--- before ---\n" + t)
    print("=== chunk ===\n" + resp["text"])
    for t in resp["after"]:
        print("--- after ---\n" + t)
    return 0


def _log(msg: str) -> None:
    print(msg, flush=True)


def _summary(st: dict) -> None:
    for kind, c in sorted(st.get("kinds", {}).items()):
        print(f"  {kind:<11} " + " ".join(f"{k}={v}" for k, v in sorted(c.items())), flush=True)
    if "db_bytes" in st:
        print(f"  db={st['db_bytes'] / 1e6:.1f} MB elapsed={st.get('elapsed')}s")
        where = "in a 0700 dir kb owns" if st.get("dir_managed") else \
            "in a directory kb did not lock down (see warning above)"
        print(f"  db is 0600 {where}: it holds pasted secrets — never commit or copy it off "
              "this machine")
    for m in st.get("missing_roots", []):
        print(f"  INCOMPLETE: missing root: {m}", file=sys.stderr)
    for s in st.get("refused", []):
        print(f"  INCOMPLETE: mass delete refused for {s} (--allow-mass-delete)", file=sys.stderr)
    if st.get("failed"):
        print(f"  INCOMPLETE: {st['failed']} unit(s) failed to parse (see warnings)", file=sys.stderr)
    if st.get("reason"):
        print(f"  {st['reason']}", file=sys.stderr)


def cmd_build(args, cfg) -> int:
    from .update import build_full
    if not args.full:
        print("kb build needs --full (use `kb update` for an incremental run)", file=sys.stderr)
        return 2
    db = _db(args, cfg)
    print(f"kb build --full -> {db}", flush=True)
    st = build_full(cfg, db, log=_log, lock_timeout=args.lock_timeout)
    _summary(st)
    return st["exit"]


def _embed_after(cfg, db, args):
    """The embedding pass, run under the writer lock of `kb update`."""
    def after(conn):
        from .embed import embed_pass, load_embedder
        from .models import ModelError
        key = getattr(args, "model", None) or cfg.model_active
        if not key:
            return {"skipped": "no [models] active in the config"}
        try:
            emb = load_embedder(key, cfg.hf_home, threads=args.threads, device=args.device,
                                full_verify=True)
        except ModelError as exc:
            print(f"kb: embeddings skipped: {exc}", file=sys.stderr, flush=True)
            return {"skipped": str(exc)}
        st = embed_pass(conn, db, emb, budget_minutes=args.budget_minutes,
                        max_chunks=args.max_chunks, log=_log)
        print(f"  embeddings {st['model']}: +{st['embedded']} in {st['seconds']} s, "
              f"remaining {st['remaining']}", flush=True)
        return st
    return after


def cmd_update(args, cfg) -> int:
    from .update import update
    db = _db(args, cfg)
    print(f"kb update -> {db}", flush=True)
    st = update(cfg, db, allow_mass_delete=args.allow_mass_delete, lock_timeout=args.lock_timeout,
                min_free_gb=args.min_free_gb, max_psi=args.max_psi,
                psi_wait_minutes=args.psi_wait_minutes, log=_log,
                after=_embed_after(cfg, db, args) if args.embed else None)
    _summary(st)
    return st["exit"]


def cmd_embed(args, cfg) -> int:
    from . import store
    from .update import Busy, WriterLock
    db = _db(args, cfg)
    if not db.exists():
        print(f"kb: no index at {db} — run: kb build --full", file=sys.stderr)
        return 2
    try:
        with WriterLock(db.parent, args.lock_timeout):
            conn = store.open_rw(db)
            try:
                store.check_schema(conn)
                st = _embed_after(cfg, db, args)(conn)
            finally:
                conn.close()
    except Busy:
        print("kb: another writer holds the lock — nothing done", file=sys.stderr)
        return 0 if args.lock_timeout <= 0 else 75
    return 3 if "skipped" in st else 0


def cmd_gc(args, cfg) -> int:
    import shutil
    from . import store
    from .embed import MatrixStore
    from .update import WriterLock
    db = _db(args, cfg)
    with WriterLock(db.parent, args.lock_timeout):
        conn = store.open_rw(db)
        try:
            models = conn.execute("SELECT model_key, dim FROM emb_model").fetchall()
            for key, dim in models:
                ms = MatrixStore(db, key, dim)
                if args.drop_model == key:
                    with store.transaction(conn):
                        conn.execute("DELETE FROM emb_row WHERE model_key=?", (key,))
                        conn.execute("DELETE FROM emb_model WHERE model_key=?", (key,))
                    shutil.rmtree(ms.dir, ignore_errors=True)
                    print(f"  {key}: dropped")
                    continue
                orphans = ms.clean_orphans(conn)
                n = ms.gc(conn, force=True)
                print(f"  {key}: {n} garbage rows dropped, {len(orphans)} orphan file(s) removed")
        finally:
            conn.close()
    return 0


def cmd_fetch_model(args, cfg) -> int:
    from .fetch import fetch_model
    from .models import ModelError
    try:
        fetch_model(args.model, cfg.hf_home, revision=args.revision, log=_log)
    except ModelError as exc:
        print(f"kb: {exc}", file=sys.stderr)
        return 2
    return 0


def cmd_mcp(args, cfg) -> int:
    from .mcp_server import serve
    return serve(cfg, _db(args, cfg), proc_net_dev=args.proc_net_dev, threads=args.threads)


GOLDEN = Path(__file__).resolve().parent.parent / "eval" / "golden.jsonl"


def cmd_eval(args, cfg) -> int:
    """Golden eval. Plain: the chosen modes on the whole index (semantic and
    hybrid use the active model). --compare-models A,B: both models on the
    same sub-corpus (expected sources + stratified distractors), embedding
    the sub-corpus first, then the §6.3 decision."""
    import glob
    from . import evaluate as E
    from . import store
    from .embed import embed_pass, load_embedder
    from .search import Index
    from .semantic import Engine, engine_for
    from .update import WriterLock
    db = _db(args, cfg)
    golden = E.load_golden(Path(args.golden).expanduser())
    modes = [m for m in args.modes.split(",") if m]
    bad = [m for m in modes if m not in E.MODES]
    if bad:
        print(f"kb: unknown mode(s) {bad}; use {','.join(E.MODES)}", file=sys.stderr)
        return 2
    idx = Index(db)
    stamp = store.now_iso().replace(":", "")
    if not args.compare_models:
        engine = engine_for(idx, cfg, threads=args.threads) \
            if any(m in ("semantic", "hybrid") for m in modes) else None
        rep = E.evaluate(idx, golden, modes, engine=engine)
        print(E.table(rep, f"kb eval — whole index ({db})"))
        p = E.save(db, f"eval-{stamp}.json", rep)
        print(f"report: {p}")
        return 0
    keys = args.compare_models.split(",")
    sub = E.subcorpus(idx.conn(), golden, args.distractors, seed=args.seed)
    print(f"sub-corpus: {len(sub)} chunks (expected sources + {args.distractors} distractors, "
          f"seed {args.seed})", flush=True)
    reports = {}
    for key in keys:
        emb = load_embedder(key, cfg.hf_home, threads=args.threads, device=args.device)
        with WriterLock(db.parent, 600):
            conn = store.open_rw(db)
            try:
                st = embed_pass(conn, db, emb, restrict=set(int(r) for r in sub), log=_log)
            finally:
                conn.close()
        print(f"  {key}: embedded {st['embedded']} sub-corpus chunks in {st['seconds']} s", flush=True)
        engine = Engine(idx, key, emb.dim, embedder=emb)
        rep = E.evaluate(idx, golden, modes, engine=engine, restrict=sub)
        rep["embed_seconds"] = st["seconds"]
        reports[key] = rep
        print(E.table(rep, f"kb eval — {key}, sub-corpus of {len(sub)}"), flush=True)
    out = {"subcorpus": len(sub), "seed": args.seed, "distractors": args.distractors,
           "reports": reports}
    if set(keys) == {"e5b", "m3"}:
        benches = sorted(glob.glob(str(db.parent / "eval" / "bench-m3-*.json")))
        delta = json.loads(Path(benches[-1]).read_text())["daily_delta_minutes"] if benches else None
        out["decision"] = E.decide(reports["e5b"], reports["m3"], delta)
        d = out["decision"]
        print(f"decision (DESIGN §6.3): {d['choice']} — net m3−e5b on hybrid strict@10 = "
              f"{d['net_m3_minus_e5b']} (m3 wins {len(d['m3_wins'])}, e5b wins {len(d['e5b_wins'])}); "
              f"m3 daily delta {d['m3_daily_delta_minutes']} min (≤ 60: {d['speed_ok']})")
    p = E.save(db, f"compare-{'-'.join(keys)}-{stamp}.json", out)
    print(f"report: {p}")
    return 0


def cmd_bench(args, cfg) -> int:
    from .bench import bench_embed
    print(json.dumps(bench_embed(cfg, _db(args, cfg), args.model, args.n, args.threads,
                                 args.device, log=_log), indent=1))
    return 0


def cmd_stats(args, cfg) -> int:
    from .stats import stats_report
    from .search import QueryError
    try:
        text, rc = stats_report(cfg, _db(args, cfg), check=args.check)
    except QueryError as exc:
        print(f"kb: {exc}", file=sys.stderr)
        return 2
    print(text)
    return rc


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="kb", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--config", help="kb.toml (default ~/.config/kb/kb.toml, else built-in)")
    p.add_argument("--db", help="database path (default from the config)")
    sub = p.add_subparsers(dest="command", required=True)

    s = sub.add_parser("search", help="where was this discussed?")
    s.add_argument("query")
    s.add_argument("--mode", default="hybrid", choices=["hybrid", "fts", "semantic"])
    s.add_argument("--kind")
    s.add_argument("--project")
    s.add_argument("--since")
    s.add_argument("--limit", type=int, default=10)
    s.add_argument("--json", action="store_true")
    s.add_argument("--threads", type=int, default=4, help="CPU threads for the query embedding")

    r = sub.add_parser("read", help="a chunk (and neighbours) by kb:<id> or `where`")
    r.add_argument("pointer")
    r.add_argument("--context", type=int, default=1)
    r.add_argument("--json", action="store_true")

    b = sub.add_parser("build", help="full rebuild into a temp file, then atomic rename")
    b.add_argument("--full", action="store_true")
    b.add_argument("--lock-timeout", type=float, default=600)

    u = sub.add_parser("update", help="incremental update (what changed since the last run)")
    u.add_argument("--allow-mass-delete", action="store_true",
                   help="accept losing > max(50, 20%%) of a source's units in one run")
    u.add_argument("--lock-timeout", type=float, default=600,
                   help="seconds to wait for another writer; 0 = exit 0 at once (timer)")
    u.add_argument("--min-free-gb", type=float, default=5.0)
    u.add_argument("--max-psi", type=float, default=None,
                   help="defer while memory PSI some avg10 of user@.service is above this")
    u.add_argument("--psi-wait-minutes", type=float, default=60)
    u.add_argument("--embed", action="store_true", help="then embed what has no vector yet")
    _embed_args(u)

    e = sub.add_parser("embed", help="embed chunks that have no vector yet (budgeted)")
    e.add_argument("--model", help="model key (default: [models] active)")
    e.add_argument("--lock-timeout", type=float, default=600)
    _embed_args(e)

    g = sub.add_parser("gc", help="drop embedding garbage (new matrix generation)")
    g.add_argument("--drop-model", help="delete one model's vectors entirely")
    g.add_argument("--lock-timeout", type=float, default=600)

    f = sub.add_parser("fetch-model", help="ONE-TIME model download (the only network command)")
    f.add_argument("--model", required=True)
    f.add_argument("--revision", help="commit sha (default: the repo's current one, then pinned)")

    m = sub.add_parser("mcp", help="stdio MCP server (kb_search, kb_read); run under unshare -rn")
    m.add_argument("--threads", type=int, default=4)
    m.add_argument("--proc-net-dev", default=None, help=argparse.SUPPRESS)

    ev = sub.add_parser("eval", help="golden-set recall per mode; --compare-models for §6.3")
    ev.add_argument("--golden", default=str(GOLDEN))
    ev.add_argument("--modes", default="fts_raw_or,fts,semantic,hybrid")
    ev.add_argument("--compare-models", help="e.g. e5b,m3: both on one sub-corpus")
    ev.add_argument("--distractors", type=int, default=10000)
    ev.add_argument("--seed", type=int, default=20261007)
    ev.add_argument("--threads", type=int, default=16)
    ev.add_argument("--device", default="cpu", choices=["cpu", "cuda"])

    bn = sub.add_parser("bench-embed", help="embedding speed on a stratified sample of real chunks")
    bn.add_argument("--model", required=True)
    bn.add_argument("--n", type=int, default=2000)
    bn.add_argument("--threads", type=int, default=16)
    bn.add_argument("--device", default="cpu", choices=["cpu", "cuda"])

    st = sub.add_parser("stats", help="what is indexed, what is missing")
    st.add_argument("--check", action="store_true", help="FTS integrity check")
    return p


COMMANDS = {"search": cmd_search, "read": cmd_read, "build": cmd_build, "update": cmd_update,
            "stats": cmd_stats, "embed": cmd_embed, "gc": cmd_gc, "fetch-model": cmd_fetch_model,
            "bench-embed": cmd_bench, "eval": cmd_eval, "mcp": cmd_mcp}


def _embed_args(p) -> None:
    p.add_argument("--budget-minutes", type=float, default=None)
    p.add_argument("--max-chunks", type=int, default=None)
    p.add_argument("--threads", type=int, default=16)
    p.add_argument("--device", default="cpu", choices=["cpu", "cuda"])


def main(argv=None) -> int:
    os.umask(0o077)       # everything kb creates (db, journal, lock, matrices) is 0600/0700
    args = build_parser().parse_args(argv)
    if args.command != "fetch-model":
        from .netguard import force_offline_env
        force_offline_env()   # before anything imports huggingface_hub / transformers
    try:
        cfg = cfgmod.load_config(args.config)
    except cfgmod.ConfigError as exc:
        print(f"kb: config: {exc}", file=sys.stderr)
        return 2
    return COMMANDS[args.command](args, cfg)


if __name__ == "__main__":
    sys.exit(main())
