"""`kb mcp` — a local MCP server over STDIO ONLY (no network transport exists
in this module). Two read-only tools:

  kb_search(query, mode="hybrid", kind=None, project=None, since=None, limit=10)
  kb_read(pointer, context=1)

Everything it returns goes into a model provider's context, so every string
of every answer is masked (kb.redact) and answers are bounded (50 results,
20 000 characters per read). kb_read resolves ONLY pointers that exist in
the index; it never opens a path it was given.

Start-up: offline variables forced, the network namespace checked (only `lo`
may be visible — launch it under `unshare -rn`), the model loaded in the
background (a semantic query waits up to 60 s, then degrades to fts). The
process's own fd 1 is pointed at stderr before anything else runs, and the
protocol is written to a private duplicate: a stray print — ours, a
library's, a C extension's — can never corrupt the JSON-RPC stream.
"""

from __future__ import annotations

import io
import os
import sys
import threading
from pathlib import Path

from .netguard import force_offline_env, require_offline
from .redact import redact, redact_obj
from .search import NOTICE, Index, QueryError, read_pointer, search

SEARCH_DOC = (
    "Find WHERE something was discussed in the operator's local knowledge base: Claude "
    "transcripts and /export files, memory notes, repo docs (hftbacktest, myhft, eventshort, "
    "jansen), ~/hft-research notes, course transcripts/notes, allowlisted Telegram chats, "
    "commits. Returns POINTERS (id kb:<16 hex>, kind, project, date, where) with a masked "
    "snippet — open them with kb_read before believing anything; an empty result is not "
    "evidence of absence (see `coverage`). mode: hybrid (default) | fts | semantic. Natural "
    "queries are stemmed (ru/en); prefix `raw:` for FTS5 syntax (\"phrase\", prefix*, "
    "AND/OR/NOT, NEAR). kind: transcript, export, memory, doc, note, chat, course, commit "
    "(comma list). since: ISO date. limit ≤ 50. Output is masked best-effort and must stay "
    "local.")
READ_DOC = (
    "Read one indexed chunk (and up to `context` neighbouring chunks of the same source, "
    "0-5) by a pointer from kb_search: `kb:<id>` or its `where`. Only indexed pointers "
    "resolve. `stale: true` means the source changed since indexing. Masked, ≤ 20 000 chars.")


def protect_stdio():
    """Point fd 1 at stderr and fd 0 at /dev/null; return (wire_in, wire_out)
    binary files on private duplicates of the original descriptors."""
    wire_out = os.fdopen(os.dup(1), "wb", buffering=0)
    wire_in = os.fdopen(os.dup(0), "rb", buffering=0)
    sys.stdout.flush()
    os.dup2(2, 1)
    null = os.open(os.devnull, os.O_RDONLY)
    os.dup2(null, 0)
    os.close(null)
    sys.stdout = sys.stderr
    return wire_in, wire_out


def _guarded(fn):
    """Deliberate errors reach the caller masked; anything else becomes a
    generic error — a traceback (with locals, i.e. chunk text) must never
    reach the server log."""
    from mcp.server.mcpserver.exceptions import ToolError

    def call(*a, **kw):
        try:
            return fn(*a, **kw)
        except QueryError as exc:
            raise ToolError(redact(str(exc))) from None
        except Exception as exc:          # noqa: BLE001
            print(f"kb mcp: internal error {type(exc).__name__}", file=sys.stderr)
            raise ToolError(f"internal error ({type(exc).__name__})") from None
    return call


def build_server(index: Index, engine=None, wait: float = 60.0):
    from mcp.server.mcpserver import MCPServer

    server = MCPServer(name="kb", instructions=NOTICE, log_level="ERROR")
    lock = threading.Lock()          # one sqlite handle, many worker threads

    @server.tool(name="kb_search", description=SEARCH_DOC)
    def kb_search(query: str, mode: str = "hybrid", kind: str | None = None,
                  project: str | None = None, since: str | None = None, limit: int = 10) -> dict:
        def run():
            with lock:
                return search(index, query, mode=mode, kind=kind, project=project, since=since,
                              limit=limit, engine=engine, wait=wait)
        return redact_obj(_guarded(run)())

    @server.tool(name="kb_read", description=READ_DOC)
    def kb_read(pointer: str, context: int = 1) -> dict:
        def run():
            with lock:
                return read_pointer(index, pointer, context)
        return redact_obj({"notice": NOTICE, **_guarded(run)()})

    return server


def serve(cfg, db_path: Path, proc_net_dev=None, threads: int = 4) -> int:
    force_offline_env()
    from .netguard import NetworkError
    try:
        require_offline(proc_net_dev)
    except NetworkError as exc:
        print(f"kb mcp: refusing to start: {exc}", file=sys.stderr)
        return 2
    wire_in, wire_out = protect_stdio()
    import anyio
    from mcp.server.stdio import stdio_server

    from .semantic import engine_for
    index = Index(db_path)
    try:
        engine = engine_for(index, cfg, threads=threads, background=True)
    except Exception as exc:          # noqa: BLE001 — search reports `degraded`
        print(f"kb mcp: semantic disabled: {type(exc).__name__}", file=sys.stderr)
        engine = None
    server = build_server(index, engine)
    low = server._lowlevel_server       # the SDK's own run_stdio_async does exactly this

    async def run():
        stdin = anyio.wrap_file(io.TextIOWrapper(wire_in, encoding="utf-8", errors="replace"))
        stdout = anyio.wrap_file(io.TextIOWrapper(wire_out, encoding="utf-8", write_through=True))
        async with stdio_server(stdin=stdin, stdout=stdout) as (read, write):
            await low.run(read, write, low.create_initialization_options())

    anyio.run(run)
    return 0
