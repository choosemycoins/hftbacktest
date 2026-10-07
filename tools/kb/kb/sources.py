"""Sources: what each kind takes, what it leaves, and how a unit is parsed.

A *unit* is the thing whose change triggers a re-parse: a file, a whole chat
(all its exports), a git repository. A scan lists the units a source has right
now, cheaply (stat, git refs); parsing happens only for units that changed.

Never read: files whose NAME looks like a secret (`*.pem`, `.env*`,
`*secret*`, ...), tool calls/outputs of transcripts and exports, chats that are
not on the allowlist, anything under ~/.config or ~/.ssh (not a root).
"""

from __future__ import annotations

import fnmatch
import glob as globmod
import hashlib
import html
import json
import os
import re
import subprocess
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from . import chunking as ch
from .config import Config
from .extract import extract_message_text

# Bump a kind's version when its parser changes what it emits: every unit of
# that kind is re-read once; ids are anchors and vectors are keyed by text, so
# nothing is re-embedded unless the text itself changed.
PARSER_VERSIONS = {"transcript": 1, "memory": 1, "doc": 1, "export": 2, "note": 1,
                   "course": 1, "chat": 1, "commit": 2}
# export 2: inline tool lines ("Stop Task  ⎿ …") dropped; commit 2: oldest-first ordinals
KINDS = tuple(PARSER_VERSIONS)

MAX_BYTES = 2 * 1024 * 1024
MIN_NOTE_BYTES = 40
SERVICE_DIRS = {".git", ".venv", "venv", "site-packages", "node_modules", "__pycache__",
                ".pytest_cache", "target", "build", "dist", ".idea", ".gradle", ".mypy_cache",
                ".ruff_cache", ".tox"}
NOTE_TXT_EXCLUDED_DIRS = {"out", "raw", "data", "cache"}
_SECRET_NAME = re.compile(
    r"(?i)(?:\.(?:pem|key|p12|pfx)$|^\.env|^id_rsa|^id_ed25519|secret|credential|password)")
_EXPORT_NAME = re.compile(r"^(?:\d{4}-\d{2}-\d{2}-\d{6}-.*|conversation-\d{4}-\d{2}-\d{2}-\d{6}.*)\.txt$")
_NOTE_META_TXT = re.compile(r"(?i)^(?:requirements.*\.txt|sha256sums.*|fetched_at\.txt)$")


def secret_named(name: str) -> bool:
    return bool(_SECRET_NAME.search(name))


def iso_mtime(st) -> str:
    return ch.iso_utc(st.st_mtime)


def iso_any(value: str) -> str | None:
    """'2026-08-09T10:00:00.123Z' / '+02:00' offsets -> '2026-08-09T10:00:00Z'."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class Candidate:
    source_id: str
    rel: str
    kind: str
    project: str
    abs_path: str
    signature: list
    date: str
    parse: Callable[[], tuple[list[ch.Piece], dict]]
    files: tuple[str, ...] = ()
    title: str = ""


@dataclass
class Scan:
    source_id: str
    candidates: list[Candidate] = field(default_factory=list)
    roots_ok: bool = True
    missing: list[str] = field(default_factory=list)
    skipped: Counter = field(default_factory=Counter)
    notes: list[str] = field(default_factory=list)


def _file_sig(st) -> list:
    return [st.st_size, st.st_mtime_ns]


def _read(path: str) -> str:
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        return fh.read()


def _walk(root: Path, prune_nested_repos: bool):
    """Yield (dirpath, filename) under root; service dirs pruned, symlinks
    not followed, nested git repos/worktrees pruned when asked."""
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        keep = []
        for d in dirnames:
            if d in SERVICE_DIRS:
                continue
            full = os.path.join(dirpath, d)
            if os.path.islink(full):
                continue
            if prune_nested_repos and os.path.exists(os.path.join(full, ".git")):
                continue
            keep.append(d)
        dirnames[:] = sorted(keep)
        for f in sorted(filenames):
            yield dirpath, f


def _file_candidate(scan: Scan, root: Path, full: str, kind: str, project: str,
                    parse: Callable[[str], tuple[list, dict]], min_bytes=1) -> None:
    name = os.path.basename(full)
    if os.path.islink(full) or not os.path.isfile(full):
        return
    if secret_named(name):
        scan.skipped["secret_name"] += 1
        return
    st = os.stat(full)
    if st.st_size > MAX_BYTES:
        scan.skipped["too_big"] += 1
        return
    if st.st_size < min_bytes:
        scan.skipped["too_small"] += 1
        return
    rel = os.path.relpath(full, root)
    scan.candidates.append(Candidate(
        source_id=scan.source_id, rel=rel, kind=kind, project=project, abs_path=full,
        signature=_file_sig(st), date=iso_mtime(st), files=(full,),
        parse=lambda full=full, name=name: parse(full, name), title=name))


def project_of_slug(slug: str, aliases: dict) -> str:
    if slug in aliases:
        return aliases[slug]
    parts = [p for p in slug.split("-") if p]
    return parts[-1] if parts else slug


# ---------------------------------------------------------------------------
# Claude transcripts and memory
# ---------------------------------------------------------------------------

def parse_transcript(path: str, rel: str):
    parts = rel.split("/")
    if len(parts) <= 2:
        session, agent = Path(parts[-1]).stem, "main"
    else:
        session, agent = parts[1], Path(parts[-1]).stem
    fallback = ch.iso_utc(os.stat(path).st_mtime)
    pieces, malformed = [], 0
    seen: dict[str, int] = {}
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        for lineno, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except (ValueError, RecursionError):
                malformed += 1
                continue
            text = extract_message_text(obj)
            if text is None:
                continue
            role = obj.get("type", "?")
            uuid = obj.get("uuid") if isinstance(obj.get("uuid"), str) else None
            if uuid:
                seen[uuid] = seen.get(uuid, 0) + 1
                if seen[uuid] > 1:
                    uuid = f"{uuid}#{seen[uuid]}"
            date = iso_any(obj.get("timestamp")) or fallback
            pieces.extend(ch.chunk_message(text, lineno, uuid, f"{session[:8]}/{agent} · {role}", date))
    return pieces, {"malformed": malformed}


def scan_transcripts(cfg: Config) -> Scan:
    scan = Scan("claude")
    roots = [cfg.claude_root] + ([cfg.retain_from] if cfg.retain_from else [])   # live first: it wins
    seen: set[str] = set()
    for root in roots:
        if root is None:
            continue
        if not root.is_dir():
            scan.roots_ok = False
            scan.missing.append(str(root))
            continue
        for slug_dir in sorted(p for p in root.iterdir() if p.is_dir()):
            project = project_of_slug(slug_dir.name, cfg.aliases)
            for dirpath, f in _walk(slug_dir, prune_nested_repos=False):
                if not f.endswith(".jsonl"):
                    continue
                full = os.path.join(dirpath, f)
                rel = os.path.relpath(full, root)
                if rel in seen or os.path.islink(full):
                    continue
                seen.add(rel)
                st = os.stat(full)
                scan.candidates.append(Candidate(
                    "claude", rel, "transcript", project, full, _file_sig(st), iso_mtime(st),
                    parse=lambda full=full, rel=rel: parse_transcript(full, rel), files=(full,),
                    title=f))
    if cfg.claude_root is None:
        scan.roots_ok = False
        scan.missing.append("[claude] root not configured")
    return scan


def _parse_md(path: str, name: str):
    return ch.chunk_markdown(_read(path), name), {}


def scan_memory(cfg: Config) -> Scan:
    scan = Scan("claude-memory")
    root = cfg.claude_root
    if root is None or not root.is_dir():
        scan.roots_ok = False
        scan.missing.append(str(root))
        return scan
    for slug_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        mem = slug_dir / "memory"
        if not mem.is_dir():
            continue
        project = project_of_slug(slug_dir.name, cfg.aliases)
        for f in sorted(os.listdir(mem)):
            if f.endswith(".md"):
                _file_candidate(scan, root, str(mem / f), "memory", project, _parse_md)
    return scan


# ---------------------------------------------------------------------------
# repositories: md docs, rst under docs/, docstrings, /export transcripts
# ---------------------------------------------------------------------------

def _parse_plain(path: str, name: str):
    return ch.chunk_plain(_read(path), name), {}


def _parse_docstring(path: str, name: str):
    pieces = ch.chunk_docstring(_read(path), name)
    if pieces is None:
        return [], {"unparsable": 1}
    return pieces, {}


def _parse_export(path: str, name: str):
    return ch.chunk_export(_read(path), name), {}


def export_date(name: str, st) -> str:
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})-(\d{2})(\d{2})(\d{2})", name)
    if not m:
        return iso_mtime(st)
    y, mo, d, h, mi, s = (int(x) for x in m.groups())
    local = time.mktime((y, mo, d, h, mi, s, 0, 0, -1))   # the export names local time
    return ch.iso_utc(local)


def scan_repo(cfg: Config, repo) -> Scan:
    scan = Scan(f"repo:{repo.name}")
    root = repo.path
    if not root.is_dir():
        scan.roots_ok = False
        scan.missing.append(str(root))
        return scan
    for dirpath, f in _walk(root, prune_nested_repos=True):
        full = os.path.join(dirpath, f)
        rel = os.path.relpath(full, root)
        if f.endswith(".md"):
            _file_candidate(scan, root, full, "doc", repo.name, _parse_md)
        elif f.endswith(".rst") and rel.split(os.sep)[0] == "docs":
            _file_candidate(scan, root, full, "doc", repo.name, _parse_plain)
        elif dirpath == str(root) and _EXPORT_NAME.match(f):
            before = len(scan.candidates)
            _file_candidate(scan, root, full, "export", repo.name, _parse_export)
            if len(scan.candidates) > before:
                c = scan.candidates[-1]
                c.date = export_date(f, os.stat(full))
    seen = {c.rel for c in scan.candidates}
    for d in repo.docstring_dirs:
        base = (root / d).resolve()
        if not base.is_dir():
            scan.notes.append(f"docstring dir missing: {base}")
            continue
        for f in sorted(os.listdir(base)):
            full = str(base / f)
            rel = os.path.relpath(full, root)
            if f.endswith(".py") and rel not in seen:
                seen.add(rel)
                _file_candidate(scan, root, full, "doc", repo.name, _parse_docstring)
    return scan


# ---------------------------------------------------------------------------
# research notes, course
# ---------------------------------------------------------------------------

def scan_research(cfg: Config) -> Scan:
    scan = Scan("hft-research")
    root = cfg.research_root
    if root is None:
        return scan
    if not root.is_dir():
        scan.roots_ok = False
        scan.missing.append(str(root))
        return scan
    for dirpath, f in _walk(root, prune_nested_repos=False):
        full = os.path.join(dirpath, f)
        rel = os.path.relpath(full, root)
        if any(fnmatch.fnmatch(rel, pat) for pat in cfg.research_exclude):
            continue
        parts = rel.split(os.sep)
        project = parts[0] if len(parts) > 1 else "hft-research"
        if f.endswith(".md"):
            _file_candidate(scan, root, full, "note", project, _parse_md, MIN_NOTE_BYTES)
        elif f.endswith(".txt"):
            if set(parts[:-1]) & NOTE_TXT_EXCLUDED_DIRS or _NOTE_META_TXT.match(f):
                scan.skipped["excluded_txt"] += 1
                continue
            _file_candidate(scan, root, full, "note", project, _parse_plain, MIN_NOTE_BYTES)
    return scan


def _parse_asr(course: str):
    def parse(path: str, name: str):
        try:
            data = json.loads(_read(path))
            segments = data["segments"]
        except (ValueError, KeyError, TypeError):
            return [], {"malformed": 1}
        return ch.chunk_asr(segments, course, Path(name).stem), {}
    return parse


def scan_course_asr(cfg: Config, course) -> Scan:
    scan = Scan(f"course:{course.id}")
    root = course.asr
    if root is None:
        return scan
    if not root.is_dir():
        scan.roots_ok = False
        scan.missing.append(str(root))
        return scan
    for f in sorted(os.listdir(root)):
        if f.endswith(".json"):
            _file_candidate(scan, root, str(root / f), "course", course.id, _parse_asr(course.id))
    return scan


def scan_course_notes(cfg: Config, course) -> Scan:
    scan = Scan(f"course:{course.id}-notes")
    root = course.notes
    if root is None:
        return scan
    if not root.is_dir():
        scan.roots_ok = False
        scan.missing.append(str(root))
        return scan
    for dirpath, f in _walk(root, prune_nested_repos=False):
        if f.endswith(".md"):
            _file_candidate(scan, root, os.path.join(dirpath, f), "course", course.id, _parse_md)
    return scan


def asr_progress(course) -> tuple[int, int | None]:
    """(transcribed, videos) for kb stats; videos None when unknown."""
    done = len(list(course.asr.glob("*.json"))) if course.asr and course.asr.is_dir() else 0
    total = None
    if course.videos and course.videos.is_dir():
        total = sum(1 for p in course.videos.rglob("*")
                    if p.suffix.lower() in (".mp4", ".mkv", ".webm", ".avi", ".mov"))
    return done, total


# ---------------------------------------------------------------------------
# Telegram chats (allowlist only)
# ---------------------------------------------------------------------------

_MSG_RE = re.compile(
    r'<div class="message (default clearfix(?: joined)?|service)" id="message(-?\d+)">(.*?)'
    r'(?=<div class="message (?:default clearfix(?: joined)?|service)" id="message|'
    r'</div>\s*</div>\s*</div>\s*</body>)', re.S)
_DATE_RE = re.compile(r'<div class="pull_right date details" title="([^"]+)"')
_FROM_RE = re.compile(r'<div class="from_name">\s*(.*?)\s*</div>', re.S)
_TEXT_RE = re.compile(r'<div class="text">\s*(.*?)\s*</div>', re.S)
_TITLE_RE = re.compile(r'<div class="page_header">.*?<div class="text bold">\s*(.*?)\s*</div>', re.S)
_TAG_RE = re.compile(r"<[^>]+>")


def _html_text(fragment: str) -> str:
    text = re.sub(r"<br\s*/?>", "\n", fragment)
    return html.unescape(_TAG_RE.sub("", text)).strip()


def _tg_date(value: str) -> float | None:
    for fmt in ("%d.%m.%Y %H:%M:%S UTC%z", "%d.%m.%Y %H:%M UTC%z"):
        try:
            return datetime.strptime(value.strip(), fmt).timestamp()
        except ValueError:
            continue
    return None


def _html_files(d: Path) -> list[Path]:
    return sorted(d.glob("messages*.html"), key=lambda p: (len(p.name), p.name))


def parse_html_export(d: Path, export: str):
    """-> (title, {id: message}, malformed)."""
    msgs, title, bad, last_from = {}, None, 0, None
    for f in _html_files(d):
        s = _read(str(f))
        if title is None:
            m = _TITLE_RE.search(s)
            title = _html_text(m.group(1)) if m else None
        for m in _MSG_RE.finditer(s):
            kind, mid, body = m.group(1), int(m.group(2)), m.group(3)
            if kind == "service":
                continue
            own = body.split('class="forwarded body"', 1)[0]
            fr = _FROM_RE.search(own)
            if fr:
                last_from = _html_text(fr.group(1))
            t = _TEXT_RE.search(body)
            text = _html_text(t.group(1)) if t else ""
            if not text:
                continue
            d_ = _DATE_RE.search(body)
            ts = _tg_date(d_.group(1)) if d_ else None
            if ts is None:
                bad += 1
                continue
            msgs[mid] = {"id": mid, "ts": ts, "sender": last_from, "text": text,
                         "file": f"{export}/{f.name}"}
    return title, msgs, bad


def _json_text(value) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return "".join(v if isinstance(v, str) else str(v.get("text", "")) for v in value
                       if isinstance(v, (str, dict)))
    return ""


def parse_json_export(d: Path, export: str):
    data = json.loads(_read(str(d / "result.json")))
    msgs, bad = {}, 0
    for m in data.get("messages", []):
        if m.get("type") != "message":
            continue
        text = _json_text(m.get("text")).strip()
        if not text:
            continue
        try:
            ts = float(m["date_unixtime"])
        except (KeyError, TypeError, ValueError):
            bad += 1
            continue
        msgs[int(m["id"])] = {"id": int(m["id"]), "ts": ts,
                              "sender": m.get("from") or m.get("actor"), "text": text,
                              "file": f"{export}/result.json"}
    return data.get("name"), msgs, bad


def _export_files(d: Path) -> list[Path]:
    files = _html_files(d)
    if (d / "result.json").is_file():
        files.append(d / "result.json")
    return files


def parse_chat(root: Path, allow) -> tuple[list[ch.Piece], dict]:
    merged: dict[int, dict] = {}
    title, bad = None, 0
    for export in allow.exports:                    # later exports win per message id
        d = root / export
        if (d / "result.json").is_file() and not _html_files(d):
            t, msgs, b = parse_json_export(d, export)
        else:
            t, msgs, b = parse_html_export(d, export)
        title = t or title
        bad += b
        merged.update(msgs)
    return ch.chunk_chat(list(merged.values()), title or allow.id), \
        {"malformed": bad, "messages": len(merged)}


def scan_chat(cfg: Config, allow) -> Scan:
    scan = Scan(f"chat:{allow.id}")
    root = cfg.chats_root
    if root is None or not root.is_dir():
        scan.roots_ok = False
        scan.missing.append(str(root))
        return scan
    sig = []
    for export in allow.exports:
        d = root / export
        files = _export_files(d) if d.is_dir() else []
        if not files:
            scan.roots_ok = False
            scan.missing.append(str(d))
            continue
        for f in files:
            st = f.stat()
            sig.append([f"{export}/{f.name}", st.st_size, st.st_mtime_ns])
    if not scan.roots_ok:
        return scan
    sig.sort()
    newest = max(s[2] for s in sig) / 1e9
    scan.candidates.append(Candidate(
        scan.source_id, "", "chat", allow.id, str(root), sig, ch.iso_utc(newest),
        parse=lambda: parse_chat(root, allow), files=(), title=allow.id))
    return scan


def unclassified_exports(cfg: Config) -> list[str]:
    root = cfg.chats_root
    if root is None or not root.is_dir():
        return []
    known = set(cfg.chats_ignore) | {e for a in cfg.chats_allow for e in a.exports}
    return sorted(p.name for p in root.iterdir()
                  if p.is_dir() and p.name.startswith("ChatExport") and p.name not in known)


# ---------------------------------------------------------------------------
# git
# ---------------------------------------------------------------------------

GIT_FORMAT = "%H%x00%aI%x00%s%x00%b%x1e"


def _git(repo: Path, *args) -> str | None:
    try:
        return subprocess.run(["git", *args], cwd=str(repo), check=True, capture_output=True,
                              text=True, errors="replace").stdout
    except (subprocess.CalledProcessError, OSError):
        return None


def git_signature(repo: Path) -> str | None:
    refs = _git(repo, "for-each-ref", "--format=%(objectname) %(refname)")
    head = _git(repo, "rev-parse", "HEAD")
    if refs is None:
        return None
    return hashlib.sha256((refs + "\n" + (head or "")).encode()).hexdigest()


def parse_git(repo: Path):
    # oldest first: a new commit appends, older rows keep their ordinal
    out = _git(repo, "log", "--all", "--reverse", "--format=" + GIT_FORMAT)
    if out is None:
        raise RuntimeError(f"git log failed in {repo}")
    pieces = []
    for record in out.split("\x1e"):
        record = record.strip("\n")
        if not record.strip():
            continue
        fields = record.split("\x00")
        if len(fields) < 3:
            continue
        sha, date, subject = fields[0].strip(), fields[1], fields[2]
        body = fields[3] if len(fields) > 3 else ""
        pieces.extend(ch.chunk_commit(sha, subject, body, iso_any(date) or ""))
    return pieces, {}


def _git_candidate(scan: Scan, repo: Path, rel: str, project: str) -> None:
    sig = git_signature(repo)
    if sig is None:
        scan.roots_ok = False
        scan.missing.append(f"{repo} (not a git repository)")
        return
    st = os.stat(repo)
    scan.candidates.append(Candidate(scan.source_id, rel, "commit", project, str(repo), [sig],
                                     iso_mtime(st), parse=lambda: parse_git(repo), title=project))


def scan_git_repo(cfg: Config, repo) -> Scan:
    scan = Scan(f"git:{repo.name}")
    if not repo.path.is_dir():
        scan.roots_ok = False
        scan.missing.append(str(repo.path))
        return scan
    _git_candidate(scan, repo.path, "", repo.name)
    return scan


def scan_git_discovered(cfg: Config) -> Scan | None:
    if not cfg.git_discover:
        return None
    scan = Scan("git:hft-research")
    base = Path(cfg.git_discover.split("*", 1)[0]).expanduser()
    if not base.is_dir():
        scan.roots_ok = False
        scan.missing.append(str(base))
        return scan
    for dot_git in sorted(globmod.glob(cfg.git_discover)):
        repo = Path(dot_git).parent
        _git_candidate(scan, repo, os.path.relpath(repo, base), repo.name)
    return scan


# ---------------------------------------------------------------------------

def configured_source_ids(cfg: Config) -> set[str]:
    ids = {"claude", "claude-memory"} if cfg.claude_root else set()
    for r in cfg.repos:
        ids |= {f"repo:{r.name}", f"git:{r.name}"}
    if cfg.research_root:
        ids.add("hft-research")
    if cfg.git_discover:
        ids.add("git:hft-research")
    for c in cfg.courses:
        if c.asr:
            ids.add(f"course:{c.id}")
        if c.notes:
            ids.add(f"course:{c.id}-notes")
    for a in cfg.chats_allow:
        ids.add(f"chat:{a.id}")
    return ids


def scan_all(cfg: Config) -> list[Scan]:
    out = []
    if cfg.claude_root:
        out += [scan_transcripts(cfg), scan_memory(cfg)]
    for r in cfg.repos:
        out += [scan_repo(cfg, r), scan_git_repo(cfg, r)]
    if cfg.research_root:
        out.append(scan_research(cfg))
    g = scan_git_discovered(cfg)
    if g is not None:
        out.append(g)
    for c in cfg.courses:
        if c.asr:
            out.append(scan_course_asr(cfg, c))
        if c.notes:
            out.append(scan_course_notes(cfg, c))
    for a in cfg.chats_allow:
        out.append(scan_chat(cfg, a))
    return out
