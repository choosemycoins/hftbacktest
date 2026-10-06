#!/usr/bin/env python3
"""kbindex — compatibility shim. The index moved to the `kb` package
(tools/kb; design in tools/kb/DESIGN.md when present): chunked FTS with
Russian stemming, local embeddings, hybrid search, a stdio MCP server.

  kbindex.py build                 -> kb update
  kbindex.py search QUERY [...]    -> kb search "raw:QUERY" --mode fts [...]
                                      (v1 semantics: QUERY is a raw FTS5 query)

Everything else: run `kb --help` (venv /home/storage/hft-scratch/kb-venv).
The database and this tool's output are secrets: 0600 files, masked output,
never commit or copy them off this machine.
"""

import os
import sys

KB = os.environ.get("KB_BIN", "/home/storage/hft-scratch/kb-venv/bin/kb")


def translate(argv):
    """v1 argv -> kb argv."""
    if not argv or argv[0] in ("-h", "--help"):
        return ["--help"]
    cmd, rest = argv[0], list(argv[1:])
    head = []
    if "--db" in rest:
        i = rest.index("--db")
        head = ["--db", rest[i + 1]]
        del rest[i:i + 2]
    if cmd == "build":
        if any(a.startswith("--add-root") for a in rest):
            raise SystemExit("kbindex: --add-root is gone; roots live in ~/.config/kb/kb.toml "
                             "(see tools/kb/kb.example.toml)")
        return head + ["update", *rest]
    if cmd == "search":
        if not rest:
            raise SystemExit("kbindex: search needs a query")
        query, opts = rest[0], rest[1:]
        return head + ["search", f"raw:{query}", "--mode", "fts", *opts]
    raise SystemExit(f"kbindex: unknown command {cmd!r}; use build | search (or kb --help)")


def main(argv=None):
    args = translate(sys.argv[1:] if argv is None else argv)
    if not os.path.exists(KB):
        raise SystemExit(f"kbindex: {KB} not found — create the venv (tools/kb/DESIGN.md §11.1)")
    os.execv(KB, [KB, *args])


if __name__ == "__main__":
    main()
