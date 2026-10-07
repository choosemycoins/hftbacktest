"""Building and refreshing the index (DESIGN.md §8).

`kb update` — incremental, in place, one transaction per unit:
  unit not in the db              -> parse, insert
  same (size, mtime_ns) & parser  -> skip (no parse at all)
  stat changed, sha256 the same   -> refresh the signature (touch)
  bytes changed                   -> parse, diff by chunk_id
  chats / git                     -> signature over all export files / refs
Deletions follow the source, with guards: a missing (or emptied) root deletes
nothing and exits 3; losing more than max(50, 20 %) of a source's units in one
run needs --allow-mass-delete; a source removed FROM THE CONFIG is deleted
without a guard (that is an explicit act), and logged.

`kb build --full` — the same pass into a fresh temp file, then fsync and an
atomic rename; embeddings of the old file are carried over (they are keyed by
text, never recomputed).

Never printed or logged: chunk text. Only counts, ids and paths.
"""

from __future__ import annotations

import errno
import fcntl
import hashlib
import json
import os
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

from . import secure, sources, store
from .config import Config
from .stem import STEM_VERSION

LOCK_NAME = "kb.lock"
MASS_DELETE_MIN = 50
MASS_DELETE_SHARE = 0.20
EXIT_OK, EXIT_INCOMPLETE, EXIT_DEFERRED = 0, 3, 75


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
    print(f"kb: warning: {msg}", file=sys.stderr, flush=True)


def _fsync(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


# ---------------------------------------------------------------------------
# writer lock and resource gates
# ---------------------------------------------------------------------------

class Busy(Exception):
    pass


class WriterLock:
    """flock on <db_dir>/kb.lock; `timeout` 0 = do not wait."""

    def __init__(self, db_dir: Path, timeout: float):
        self.path = db_dir / LOCK_NAME
        self.timeout = timeout
        self.fd = None

    def __enter__(self):
        self.fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        os.chmod(self.path, 0o600)
        deadline = time.monotonic() + max(0.0, self.timeout)
        while True:
            try:
                fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return self
            except OSError as exc:
                if exc.errno not in (errno.EAGAIN, errno.EACCES):
                    raise
                if time.monotonic() >= deadline:
                    os.close(self.fd)
                    self.fd = None
                    raise Busy(str(self.path)) from None
                time.sleep(0.5)

    def __exit__(self, *exc):
        if self.fd is not None:
            fcntl.flock(self.fd, fcntl.LOCK_UN)
            os.close(self.fd)
            self.fd = None


def psi_path_from_cgroup(cgroup_text: str) -> Path | None:
    """memory.pressure of the user manager (user@<uid>.service) this process
    runs under, from /proc/self/cgroup. Works inside a user namespace
    (`unshare -r` makes getuid() 0, the cgroup path keeps the real uid)."""
    for line in cgroup_text.splitlines():
        path = line.split(":", 2)[-1]
        parts = path.strip("/").split("/")
        for i, part in enumerate(parts):
            if part.startswith("user@") and part.endswith(".service"):
                return Path("/sys/fs/cgroup", *parts[:i + 1], "memory.pressure")
    return None


def default_psi_path() -> Path:
    try:
        found = psi_path_from_cgroup(Path("/proc/self/cgroup").read_text())
    except OSError:
        found = None
    if found is not None:
        return found
    uid = os.getuid()
    return Path(f"/sys/fs/cgroup/user.slice/user-{uid}.slice/user@{uid}.service/memory.pressure")


def psi_some_avg10(path: Path) -> float | None:
    try:
        for line in Path(path).read_text().splitlines():
            if line.startswith("some "):
                return float(line.split("avg10=")[1].split()[0])
    except (OSError, IndexError, ValueError):
        return None
    return None


def free_gb(path: Path) -> float:
    st = os.statvfs(path)
    return st.f_bavail * st.f_frsize / 1e9


def _gates(db_dir: Path, min_free_gb, max_psi, psi_wait_minutes, psi_path, warn):
    """None if clear to write, else (exit code, reason)."""
    if min_free_gb:
        free = free_gb(db_dir)
        if free < min_free_gb:
            return EXIT_INCOMPLETE, f"only {free:.1f} GB free on the database filesystem (< {min_free_gb})"
    if max_psi is not None:
        path = psi_path or default_psi_path()
        deadline = time.monotonic() + psi_wait_minutes * 60
        while True:
            value = psi_some_avg10(path)
            if value is None:
                warn(f"memory pressure unreadable at {path}; gate skipped")
                break
            if value <= max_psi:
                break
            if time.monotonic() >= deadline:
                return EXIT_DEFERRED, f"memory pressure some avg10={value} > {max_psi}"
            time.sleep(30)
    return None


# ---------------------------------------------------------------------------
# the pass
# ---------------------------------------------------------------------------

def _empty_stats():
    return Counter()


def _process_scan(conn, scan, totals, allow_mass_delete, warn, refused, missing):
    existing = store.units_of(conn, scan.source_id)
    seen = set()
    for cand in scan.candidates:
        seen.add(cand.rel)
        t = totals[cand.kind]
        cur = existing.get(cand.rel)
        pv = sources.PARSER_VERSIONS[cand.kind]
        sig = json.dumps(cand.signature)
        if cur is not None and cur["parser_ver"] == pv and cur["kind"] == cand.kind \
                and cur["project"] == cand.project:
            if cur["signature"] == sig:
                if cur["abs_path"] != cand.abs_path:
                    store.touch_unit(conn, cur["unit_id"], cand)
                    t["units_touched"] += 1
                else:
                    t["units_skipped"] += 1
                continue
            if cand.files:
                try:
                    sha = content_sha(cand.files)
                except OSError as exc:
                    t["failed"] += 1
                    warn(f"{cand.kind}: cannot read {cand.abs_path}: {type(exc).__name__}")
                    continue
                if sha == cur["content_sha"]:
                    store.touch_unit(conn, cur["unit_id"], cand)
                    t["units_touched"] += 1
                    continue
        try:
            pieces, st = cand.parse()
            sha = content_sha(cand.files)
        except Exception as exc:          # one unreadable unit must not sink the run
            t["failed"] += 1
            warn(f"{cand.kind}: cannot parse {cand.abs_path}: {type(exc).__name__}")
            continue
        res = store.write_unit(conn, cand, pieces, sha, pv)
        for k in ("added", "updated", "deleted"):
            t[k] += res[k]
        t["units_new" if cur is None else "units_changed"] += 1
        t["malformed"] += st.get("malformed", 0)
        t["unparsable"] += st.get("unparsable", 0)

    gone = {rel: u for rel, u in existing.items() if rel not in seen}
    if not gone:
        return
    if not scan.roots_ok:
        return
    if existing and not scan.candidates and not allow_mass_delete:
        warn(f"{scan.source_id}: no units found under an existing root — treated as "
             f"missing; {len(gone)} unit(s) kept (--allow-mass-delete to accept)")
        missing.append(f"{scan.source_id}: root present but empty")
        return
    limit = max(MASS_DELETE_MIN, MASS_DELETE_SHARE * len(existing))
    if len(gone) > limit and not allow_mass_delete:
        refused.append(scan.source_id)
        warn(f"{scan.source_id}: {len(gone)} of {len(existing)} units vanished — mass delete "
             "refused; rerun with --allow-mass-delete if this is intended")
        return
    for rel, u in gone.items():
        n = store.delete_unit(conn, u["unit_id"])
        totals[u["kind"]]["units_deleted"] += 1
        totals[u["kind"]]["deleted"] += n


def _pass(conn, cfg: Config, allow_mass_delete: bool, log, warn) -> dict:
    totals: dict = defaultdict(_empty_stats)
    missing: list[str] = []
    refused: list[str] = []
    for scan in sources.scan_all(cfg):
        t0 = time.monotonic()
        for item in scan.missing:
            warn(f"{scan.source_id}: source root not indexed: {item}")
        missing += [f"{scan.source_id}: {m}" for m in scan.missing]
        for note in scan.notes:
            warn(f"{scan.source_id}: {note}")
        _process_scan(conn, scan, totals, allow_mass_delete, warn, refused, missing)
        for k, v in scan.skipped.items():
            totals["_skipped"][k] += v
        log(f"  {scan.source_id:<30} units={len(scan.candidates):<6} {time.monotonic() - t0:7.1f}s")
    configured = sources.configured_source_ids(cfg)
    removed = sorted({r[0] for r in conn.execute("SELECT DISTINCT source_id FROM unit")} - configured)
    for source_id in removed:
        units = conn.execute("SELECT unit_id, kind FROM unit WHERE source_id = ?", (source_id,)).fetchall()
        for unit_id, kind in units:
            totals[kind]["deleted"] += store.delete_unit(conn, unit_id)
            totals[kind]["units_deleted"] += 1
        log(f"  {source_id}: removed from the config — {len(units)} unit(s) deleted")
    failed = sum(v.get("failed", 0) for v in totals.values())
    return {"kinds": {k: dict(v) for k, v in totals.items()}, "missing_roots": missing,
            "refused": refused, "removed_sources": removed, "failed": failed}


def _finish(conn, cfg, result, started, mode):
    elapsed = round(time.monotonic() - started, 1)
    store.set_meta(conn, updated_at=store.now_iso(), update_seconds=elapsed, config_sha=cfg.sha,
                   last_run=json.dumps({"mode": mode, **result}, ensure_ascii=False))
    changed = sum(v.get("added", 0) + v.get("updated", 0) + v.get("deleted", 0)
                  for v in result["kinds"].values())
    if mode == "full" or changed > 10_000:
        conn.execute("INSERT INTO chunk_fts(chunk_fts) VALUES('optimize')")
    elif changed:
        conn.execute("INSERT INTO chunk_fts(chunk_fts, rank) VALUES('merge', 500)")
    return elapsed


def _exit_code(result) -> int:
    return EXIT_INCOMPLETE if (result["missing_roots"] or result["refused"] or result["failed"]) \
        else EXIT_OK


def update(cfg: Config, db_path=None, *, allow_mass_delete=False, lock_timeout=600.0,
           min_free_gb=5.0, max_psi=None, psi_wait_minutes=60, psi_path=None,
           log=None, warn=None, after=None) -> dict:
    """Incremental pass over the live database. `after(conn)` runs under the
    same writer lock (the embedding pass)."""
    db_path = Path(db_path or cfg.db)
    log = log or (lambda m: None)
    warn = warn or _warn_default
    started = time.monotonic()
    dir_managed = secure._secure_dir(db_path, warn)
    gate = _gates(db_path.parent, min_free_gb, max_psi, psi_wait_minutes, psi_path, warn)
    if gate:
        return {"exit": gate[0], "reason": gate[1], "busy": False, "kinds": {}}
    try:
        with WriterLock(db_path.parent, lock_timeout):
            fresh = not db_path.exists()
            conn = store.open_rw(db_path)
            try:
                os.chmod(db_path, secure.DB_MODE)
                if fresh:
                    store.init_schema(conn)
                store.check_schema(conn)
                restemmed = 0
                if store.get_meta(conn, "stem_version") != STEM_VERSION:
                    log(f"  stemmer changed -> re-stemming every chunk ({STEM_VERSION})")
                    restemmed = store.restem(conn)
                result = _pass(conn, cfg, allow_mass_delete, log, warn)
                result["restemmed"] = restemmed
                result["elapsed"] = _finish(conn, cfg, result, started, "update")
                if after is not None:
                    result["after"] = after(conn)
            finally:
                conn.close()
    except Busy:
        msg = "another writer holds the lock (kb update/build) — nothing done"
        print(f"kb: {msg}", file=sys.stderr, flush=True)
        return {"exit": EXIT_OK if lock_timeout <= 0 else EXIT_DEFERRED, "busy": True,
                "reason": msg, "kinds": {}}
    for leftover in (Path(f"{db_path}-journal"),):
        if leftover.exists():
            os.chmod(leftover, secure.DB_MODE)
    if dir_managed:
        os.chmod(db_path.parent, secure.DIR_MODE)
    result.update(exit=_exit_code(result), busy=False, dir_managed=dir_managed,
                  db_bytes=db_path.stat().st_size)
    return result


def copy_embeddings(src_db: Path, conn) -> int:
    """Carry emb_model/emb_row over from the live database (same matrices)."""
    if not src_db.exists():
        return 0
    try:
        old = store.open_ro(src_db)
        store.check_schema(old)
    except Exception:          # v1 or unreadable: nothing to carry
        return 0
    try:
        models = old.execute("SELECT * FROM emb_model").fetchall()
        rows = old.execute("SELECT * FROM emb_row").fetchall()
        emb_version = store.get_meta(old, "emb_version", "0")
        active = store.get_meta(old, "active_model")
    finally:
        old.close()
    with store.transaction(conn):
        conn.executemany("INSERT INTO emb_model VALUES (?,?,?,?,?,?,?,?,?)", models)
        conn.executemany("INSERT INTO emb_row VALUES (?,?,?)", rows)
        store.set_meta(conn, emb_version=str(int(emb_version) + 1))
        if active:
            store.set_meta(conn, active_model=active)
    return len(rows)


def build_full(cfg: Config, db_path=None, log=None, warn=None, lock_timeout=600.0) -> dict:
    """The same pass into a temp file next to the database, then fsync and an
    atomic rename: an interrupted or failing build leaves the old one intact."""
    db_path = Path(db_path or cfg.db)
    log = log or (lambda m: None)
    warn = warn or _warn_default
    started = time.monotonic()
    dir_managed = secure._secure_dir(db_path, warn)
    tmp = db_path.parent / f".{db_path.name}.tmp-{os.getpid()}"
    with WriterLock(db_path.parent, lock_timeout):
        for leftover in (tmp, Path(f"{tmp}-journal")):
            if leftover.exists():
                leftover.unlink()
        conn = None
        try:
            conn = store.open_rw(tmp)
            os.chmod(tmp, secure.DB_MODE)
            conn.execute("PRAGMA synchronous=OFF")     # not live until fsync + rename
            store.init_schema(conn)
            result = _pass(conn, cfg, True, log, warn)
            result["carried_embeddings"] = copy_embeddings(db_path, conn)
            _finish(conn, cfg, result, started, "full")
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
    result.update(exit=_exit_code(result), dir_managed=dir_managed,
                  db_bytes=db_path.stat().st_size, elapsed=round(time.monotonic() - started, 1))
    return result
