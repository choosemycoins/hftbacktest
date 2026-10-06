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
    idx = Index(_db(args, cfg))
    try:
        resp = search(idx, args.query, mode=args.mode, kind=args.kind, project=args.project,
                      since=args.since, limit=args.limit)
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


def cmd_update(args, cfg) -> int:
    from .update import update
    db = _db(args, cfg)
    print(f"kb update -> {db}", flush=True)
    st = update(cfg, db, allow_mass_delete=args.allow_mass_delete, lock_timeout=args.lock_timeout,
                min_free_gb=args.min_free_gb, max_psi=args.max_psi,
                psi_wait_minutes=args.psi_wait_minutes, log=_log)
    _summary(st)
    return st["exit"]


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
    s.add_argument("--mode", default="fts", choices=["hybrid", "fts", "semantic"])
    s.add_argument("--kind")
    s.add_argument("--project")
    s.add_argument("--since")
    s.add_argument("--limit", type=int, default=10)
    s.add_argument("--json", action="store_true")

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

    st = sub.add_parser("stats", help="what is indexed, what is missing")
    st.add_argument("--check", action="store_true", help="FTS integrity check")
    return p


COMMANDS = {"search": cmd_search, "read": cmd_read, "build": cmd_build, "update": cmd_update,
            "stats": cmd_stats}


def main(argv=None) -> int:
    os.umask(0o077)       # everything kb creates (db, journal, lock, matrices) is 0600/0700
    args = build_parser().parse_args(argv)
    try:
        cfg = cfgmod.load_config(args.config)
    except cfgmod.ConfigError as exc:
        print(f"kb: config: {exc}", file=sys.stderr)
        return 2
    return COMMANDS[args.command](args, cfg)


if __name__ == "__main__":
    sys.exit(main())
