#!/usr/bin/env python3
"""kbindex — a local full-text index over everything that was *discussed* but
never distilled: Claude session transcripts, memory files, study/tool module
docstrings and git commit messages of the three projects (jansen, myhft,
hftbacktest).

It answers "where was this talked about?", not "what is the answer?". Every
result is a POINTER — kind, project, date, file, session/commit ref — plus a
snippet for orientation. Open the source and read it before believing anything.

WHAT IS INDEXED
  transcript  ~/.claude/projects/<slug>/**/*.jsonl  — one row per message.
              Only human-readable conversation is kept: user messages that are
              plain strings, and assistant `text` content parts. Tool calls,
              tool results, thinking blocks, images, attachments, slash-command
              echoes and `<system-reminder>` blocks are dropped. Nested
              subagent transcripts are indexed too — that is where most of the
              real discussion lives.
  memory      ~/.claude/projects/<slug>/memory/*.md — one row per file, dated
              by file mtime.
  doc         module-level docstrings of the study/tool directories, read with
              ast.get_docstring — the files are parsed, never imported or
              executed.
  commit      `git log --all` of the three repos — one row per commit, subject
              plus full body.

SEARCH SYNTAX
  The query string is passed straight to SQLite FTS5, so its syntax applies:
      tick szDecimals          all terms (implicit AND)
      "szDecimals rounding"    exact phrase
      szDecim*                 prefix match (the `*` suffix is the only wildcard)
      tick NOT collector       boolean NOT / OR / AND (uppercase)
      NEAR(tick decimals, 10)  proximity
  Terms containing FTS5 punctuation must be quoted: "hl_bn_md.pem".

  The tokenizer is unicode61 — Unicode-aware, case-folding, NO STEMMING and no
  language rules. Russian therefore matches WHOLE WORDS ONLY: `терял` will not
  find `терять` or `теряли`. Use a prefix query (`теря*`) to cover a paradigm.
  Only the `text` column is searchable; kind/project/path/ref/date are metadata,
  filtered with --kind/--project instead of matched by the query.

THE DATABASE IS A SECRET, AND SO IS THIS TOOL'S OUTPUT
  Default location ~/.local/share/kbindex/kb.db. Transcripts routinely contain
  pasted API keys, private endpoints and account data. The database file is
  always chmod 0600. It must NEVER be committed to git, synced, uploaded or
  copied off this machine. It is a disposable derived artifact: delete it and
  rebuild.

  Directory mode: kbindex chmods the database's parent directory to 0700 only
  when that directory belongs to it — one it created, or an existing empty one
  it then marks with a `.kbindex-dir` file. A directory that holds
  unrelated files is NEVER re-chmodded (`--db ~/kb.db` will not lock down
  $HOME); the build warns instead, and the database is still 0600.

  Search output: snippets are printed with obvious secrets masked (long hex
  keys and 0x-values, bearer/JWT/provider tokens, `password=`-shaped
  assignments). Masking is best-effort pattern matching, NOT a guarantee — a
  secret in an unrecognised shape will be printed verbatim. Treat search
  output like the database itself: local, private, not for pasting elsewhere.

LIMITATIONS worth knowing before you trust an empty result
  * No stemming (see above): Russian matches whole word forms only.
  * The session you are in right now is indexed too, so a phrase you just
    typed matches itself and can crowd out the older discussion you meant.
    Add `NOT <a word unique to today>` or filter by --project.
  * A project with no local `~/.claude/projects/<slug>/*.jsonl` contributes no
    transcript rows at all; its history survives only as memory files and
    commits. Absence of hits is not evidence the topic was never discussed.
  * Missing source roots are reported as warnings and make `build` exit 3.

USAGE
  kbindex.py build  [--db PATH] [--add-root KIND=PATH]...
      Full rebuild into a temp file next to the target, then an atomic rename,
      so an interrupted or failing build leaves the previous database intact.
      Prints per-source row counts, skipped malformed lines, size and elapsed.
      KIND is one of claude|docs|git; bare paths are treated as claude roots.
      Exit 0 on a clean build, 3 if any source root was missing.
  kbindex.py search QUERY [--kind K] [--project P] [--limit N] [--db PATH]
"""

import argparse
import ast
import json
import os
import re
import sqlite3
import stat as stat_mod
import subprocess
import sys
import time
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path

HOME = Path.home()

DEFAULT_DB = HOME / ".local" / "share" / "kbindex" / "kb.db"

KINDS = ("transcript", "memory", "doc", "commit")

DB_MODE = 0o600
DIR_MODE = 0o700

# Written into a directory whose mode kbindex manages. Its presence — and
# nothing else — authorises re-chmodding that directory on later builds.
DIR_MARKER = ".kbindex-dir"
DIR_MARKER_TEXT = (
    "This directory is managed by tools/kbindex.py: it is kept 0700 because\n"
    "kb.db holds secrets pasted into session transcripts. Delete this marker\n"
    "to stop kbindex from touching the directory mode.\n"
)

POINTER_HEADER = (
    "kbindex: pointers, verify at source — these are locations of past "
    "discussion, not answers.\n"
    "         snippets come from transcripts that contain pasted secrets; "
    "obvious ones are masked, masking is best-effort — keep this output local."
)


@dataclass(frozen=True)
class Config:
    """Where to look. Hardcoded defaults below; extend with --add-root."""

    claude_roots: list = field(default_factory=list)
    doc_dirs: list = field(default_factory=list)
    git_repos: list = field(default_factory=list)


DEFAULT_CONFIG = Config(
    claude_roots=[HOME / ".claude" / "projects"],
    doc_dirs=[
        HOME / "PycharmProjects" / "jansen" / "studies",
        HOME / "RustroverProjects" / "myhft" / "scripts",
        HOME / "RustroverProjects" / "hftbacktest" / "collector" / "tools",
    ],
    git_repos=[
        HOME / "PycharmProjects" / "jansen",
        HOME / "RustroverProjects" / "myhft",
        HOME / "RustroverProjects" / "hftbacktest",
    ],
)


class QueryError(Exception):
    """A bad search query, or a database that cannot be read."""


@dataclass(frozen=True)
class Row:
    kind: str
    project: str
    path: str
    ref: str
    date: str
    text: str


@dataclass(frozen=True)
class Hit:
    kind: str
    project: str
    path: str
    ref: str
    date: str
    text: str
    snippet: str


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def iso_date(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat(timespec="seconds")


def project_of_slug(slug: str) -> str:
    """`-home-andrew-RustroverProjects-myhft` -> `myhft`."""
    parts = [p for p in slug.split("-") if p]
    return parts[-1] if parts else slug


def project_of_path(path: Path) -> str:
    """Name of the git repo owning `path`, so docstrings and commits of the
    same project share a --project value. Falls back to the parent directory."""
    for parent in [path] + list(path.parents):
        if (parent / ".git").exists():
            return parent.name
    return path.parent.name or path.name


# ---------------------------------------------------------------------------
# transcript message extraction
# ---------------------------------------------------------------------------

_SYSTEM_REMINDER = re.compile(r"<system-reminder>.*?</system-reminder>", re.S | re.I)
_TASK_NOTIFICATION = re.compile(r"<task-notification>(.*?)(?:</task-notification>|\Z)", re.S | re.I)
_SUMMARY = re.compile(r"<summary>(.*?)</summary>", re.S | re.I)
_COMMAND_BLOCK = re.compile(
    r"<(command-name|command-message|command-args|local-command-stdout|"
    r"local-command-stderr|local-command-caveat)>.*?</\1>",
    re.S | re.I,
)
_OPEN_COMMAND_TAG = re.compile(
    r"</?(command-name|command-message|command-args|local-command-stdout|"
    r"local-command-stderr|local-command-caveat)>",
    re.I,
)
_DATA_URI = re.compile(r"data:[\w.+-]+/[\w.+-]+;base64,[A-Za-z0-9+/=]+")
_LONG_B64 = re.compile(r"\b[A-Za-z0-9+/]{200,}={0,2}\b")

MIN_TEXT_LEN = 2


def clean_text(text: str) -> str:
    """Strip machine noise from a raw message string."""
    if not text:
        return ""
    m = _TASK_NOTIFICATION.search(text)
    if m:
        summary = _SUMMARY.search(m.group(1))
        text = summary.group(1) if summary else ""
    text = _SYSTEM_REMINDER.sub(" ", text)
    text = _COMMAND_BLOCK.sub(" ", text)
    text = _OPEN_COMMAND_TAG.sub(" ", text)
    text = _DATA_URI.sub(" ", text)
    text = _LONG_B64.sub(" ", text)
    return text.strip()


def extract_message_text(obj):
    """Human-readable conversation text of one transcript line, or None.

    Kept: user messages whose content is a plain string, and assistant `text`
    content parts. Everything else — tool_use, tool_result, thinking, images,
    attachments, session bookkeeping, meta/slash-command lines — is noise.
    """
    if not isinstance(obj, dict):
        return None
    if obj.get("type") not in ("user", "assistant"):
        return None
    if obj.get("isMeta"):
        return None
    message = obj.get("message")
    if not isinstance(message, dict):
        return None
    content = message.get("content")

    if isinstance(content, str):
        parts = [content]
    elif isinstance(content, list):
        parts = [
            p.get("text") or ""
            for p in content
            if isinstance(p, dict) and p.get("type") == "text"
        ]
    else:
        return None

    cleaned = [c for c in (clean_text(p) for p in parts) if len(c) >= MIN_TEXT_LEN]
    if not cleaned:
        return None
    return "\n".join(cleaned)


def collect_transcript_rows(claude_roots):
    rows, malformed, files, missing = [], 0, 0, []
    for root in claude_roots:
        root = Path(root)
        if not root.is_dir():
            missing.append(str(root))
            continue
        for slug_dir in sorted(p for p in root.iterdir() if p.is_dir()):
            project = project_of_slug(slug_dir.name)
            for path in sorted(slug_dir.rglob("*.jsonl")):
                if not path.is_file():
                    continue
                files += 1
                ref = path.stem
                with path.open("r", encoding="utf-8", errors="replace") as fh:
                    for line in fh:
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
                        rows.append(Row(
                            kind="transcript",
                            project=project,
                            path=str(path),
                            ref=ref,
                            date=str(obj.get("timestamp") or ""),
                            text=text,
                        ))
    return rows, {"files": files, "malformed_lines": malformed, "missing": missing}


# ---------------------------------------------------------------------------
# memory
# ---------------------------------------------------------------------------

def collect_memory_rows(claude_roots):
    rows, files, missing = [], 0, []
    for root in claude_roots:
        root = Path(root)
        if not root.is_dir():
            missing.append(str(root))
            continue
        for slug_dir in sorted(p for p in root.iterdir() if p.is_dir()):
            project = project_of_slug(slug_dir.name)
            mem_dir = slug_dir / "memory"
            if not mem_dir.is_dir():
                continue
            for path in sorted(mem_dir.glob("*.md")):
                if not path.is_file():
                    continue
                files += 1
                rows.append(Row(
                    kind="memory",
                    project=project,
                    path=str(path),
                    ref="",
                    date=iso_date(path.stat().st_mtime),
                    text=path.read_text(encoding="utf-8", errors="replace"),
                ))
    return rows, {"files": files, "missing": missing}


# ---------------------------------------------------------------------------
# docstrings
# ---------------------------------------------------------------------------

def collect_docstring_rows(doc_dirs):
    rows, unparsable, files, missing = [], 0, 0, []
    for d in doc_dirs:
        d = Path(d)
        if not d.is_dir():
            missing.append(str(d))
            continue
        project = project_of_path(d)
        for path in sorted(d.glob("*.py")):
            if not path.is_file():
                continue
            files += 1
            source = path.read_text(encoding="utf-8", errors="replace")
            try:
                tree = ast.parse(source)
            except SyntaxError:
                unparsable += 1
                continue
            doc = ast.get_docstring(tree)
            if not doc or not doc.strip():
                continue
            rows.append(Row(
                kind="doc",
                project=project,
                path=str(path),
                ref="",
                date=iso_date(path.stat().st_mtime),
                text=f"{path.name}\n{doc.strip()}",
            ))
    return rows, {"files": files, "unparsable": unparsable, "missing": missing}


# ---------------------------------------------------------------------------
# git
# ---------------------------------------------------------------------------

# %x1e terminates each record: commit bodies contain newlines, so line
# splitting alone would shred them.
GIT_FORMAT = "%H%x00%aI%x00%s%x00%b%x1e"


def _git(repo: Path, *args):
    """Run git in `repo`; return stdout, or None if git failed."""
    try:
        out = subprocess.run(["git", *args], cwd=str(repo), check=True,
                             capture_output=True, text=True, errors="replace")
    except (subprocess.CalledProcessError, OSError):
        return None
    return out.stdout


def resolve_repo(repo: Path):
    """`repo` -> (root, note) with root None when it is not a git repository.

    Asking git beats looking for a literal `.git` entry: it also recognises
    bare repos, worktrees, and a path *inside* a repo (whose commits then get
    attributed to the enclosing repo, with a note, instead of silently
    vanishing or being filed under a subdirectory name).
    """
    repo = Path(repo)
    if not repo.is_dir():
        return None, "does not exist"
    if _git(repo, "rev-parse", "--absolute-git-dir") is None:
        return None, "not a git repository"
    top = _git(repo, "rev-parse", "--show-toplevel")
    if not top or not top.strip():
        return repo, "bare repository"          # bare: no work tree to report
    root = Path(top.strip())
    if root != repo.resolve():
        return root, f"is inside {root} — indexing that repository"
    return root, ""


def collect_git_rows(git_repos):
    rows, repos, missing, notes, seen = [], 0, [], [], set()
    for given in git_repos:
        repo, note = resolve_repo(Path(given))
        if repo is None:
            missing.append(f"{given} ({note})")
            continue
        if note:
            notes.append(f"{given} {note}")
        if str(repo) in seen:
            continue
        seen.add(str(repo))
        try:
            out = subprocess.run(
                ["git", "log", "--all", "--format=" + GIT_FORMAT],
                cwd=str(repo), check=True, capture_output=True, text=True,
                errors="replace",
            ).stdout
        except (subprocess.CalledProcessError, OSError) as exc:
            missing.append(f"{repo} (git log failed: {exc})")
            continue
        repos += 1
        project = repo.name[:-4] if repo.name.endswith(".git") else repo.name
        for record in out.split("\x1e"):
            record = record.strip("\n")
            if not record.strip():
                continue
            fields = record.split("\x00")
            if len(fields) < 3:
                continue
            sha, date, subject = fields[0], fields[1], fields[2]
            body = fields[3] if len(fields) > 3 else ""
            text = subject if not body.strip() else f"{subject}\n{body.strip()}"
            rows.append(Row(
                kind="commit",
                project=project,
                path=str(repo),
                ref=sha.strip(),
                date=date,
                text=text,
            ))
    return rows, {"repos": repos, "missing": missing, "notes": notes}


# ---------------------------------------------------------------------------
# build
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE VIRTUAL TABLE kb USING fts5(
    kind UNINDEXED,
    project UNINDEXED,
    path UNINDEXED,
    ref UNINDEXED,
    date UNINDEXED,
    text,
    tokenize='unicode61'
);
"""


def _stat_repr(value):
    """Lists of paths are logged as a count; the paths themselves are warned."""
    return len(value) if isinstance(value, list) else value


def _claim_dir(path: Path) -> None:
    os.chmod(path, DIR_MODE)
    marker = path / DIR_MARKER
    if not marker.exists():
        marker.write_text(DIR_MARKER_TEXT, encoding="utf-8")
        os.chmod(marker, DB_MODE)


def _secure_dir(db_path: Path, warn=None) -> bool:
    """Prepare the directory that will hold `db_path`; True if kbindex owns it.

    Ownership — and only ownership — licenses changing the mode of a directory
    the user pointed us at. `--db ~/kb.db` must not turn $HOME into 0700, and
    `--db /srv/shared/kb.db` must not lock the other users of a shared
    directory out. A directory is ours if we created it, if it is empty, or if
    it carries our marker from an earlier build; otherwise its mode is left
    exactly as found and the caller is warned. The database file itself is
    0600 either way.
    """
    warn = warn or (lambda msg: None)
    path = db_path.parent
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
        _claim_dir(path)
        return True
    if (path / DIR_MARKER).is_file():
        _claim_dir(path)
        return True
    strangers = [p.name for p in path.iterdir()
                 if p.name != db_path.name
                 and not p.name.startswith(f".{db_path.name}.tmp-")]
    if not strangers:
        _claim_dir(path)
        return True
    mode = stat_mod.S_IMODE(path.stat().st_mode)
    warn(f"{path} holds unrelated files ({len(strangers)}), so its mode "
         f"{mode:04o} is left alone — the database will be 0600 but anyone who "
         f"can enter this directory can read it. Prefer {DEFAULT_DB.parent}.")
    return False


def build(db_path, config=None, log=None, warn=None):
    """Full rebuild into a temp file next to `db_path`, then atomic rename."""
    config = config or DEFAULT_CONFIG
    db_path = Path(db_path)
    log = log or (lambda msg: None)
    warn = warn or (lambda msg: print(f"kbindex: warning: {msg}", file=sys.stderr))
    started = time.monotonic()

    dir_managed = _secure_dir(db_path, warn)
    tmp_path = db_path.parent / f".{db_path.name}.tmp-{os.getpid()}"
    if tmp_path.exists():
        tmp_path.unlink()

    sources = (
        ("transcript", lambda: collect_transcript_rows(config.claude_roots)),
        ("memory", lambda: collect_memory_rows(config.claude_roots)),
        ("doc", lambda: collect_docstring_rows(config.doc_dirs)),
        ("commit", lambda: collect_git_rows(config.git_repos)),
    )

    counts, source_stats, chars = {}, {}, {}
    conn = None
    try:
        conn = sqlite3.connect(str(tmp_path))
        os.chmod(tmp_path, DB_MODE)
        conn.executescript(SCHEMA)
        for kind, collect in sources:
            t0 = time.monotonic()
            rows, stats = collect()
            conn.executemany(
                "INSERT INTO kb (kind, project, path, ref, date, text) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                [(r.kind, r.project, r.path, r.ref, r.date, r.text) for r in rows],
            )
            counts[kind] = len(rows)
            chars[kind] = sum(len(r.text) for r in rows)
            stats["seconds"] = round(time.monotonic() - t0, 2)
            source_stats[kind] = stats
            log(f"  {kind:<11} rows={len(rows):<7} chars={chars[kind]:<10} "
                + " ".join(f"{k}={_stat_repr(v)}" for k, v in stats.items()))
            for item in stats.get("missing", ()):
                warn(f"{kind}: source root not indexed: {item}")
            for item in stats.get("notes", ()):
                warn(f"{kind}: {item}")
        conn.commit()
        conn.execute("INSERT INTO kb(kb) VALUES('optimize')")
        conn.commit()
        conn.close()
        conn = None
        os.chmod(tmp_path, DB_MODE)
        os.replace(str(tmp_path), str(db_path))
    except BaseException:
        if conn is not None:
            conn.close()
        for leftover in (tmp_path, Path(str(tmp_path) + "-journal"),
                         Path(str(tmp_path) + "-wal"), Path(str(tmp_path) + "-shm")):
            try:
                leftover.unlink()
            except OSError:
                pass
        raise

    os.chmod(db_path, DB_MODE)
    if dir_managed:
        os.chmod(db_path.parent, DIR_MODE)
    missing = [item for stats in source_stats.values()
               for item in stats.get("missing", ())]
    if missing:
        warn(f"{len(missing)} source root(s) contributed nothing — "
             "this build is incomplete")
    return {
        "counts": counts,
        "chars": chars,
        "sources": source_stats,
        "missing_roots": missing,
        "dir_managed": dir_managed,
        "db_bytes": db_path.stat().st_size,
        "elapsed": round(time.monotonic() - started, 2),
    }


# ---------------------------------------------------------------------------
# search
# ---------------------------------------------------------------------------

SNIPPET_TOKENS = 16


def search(db_path, query, kind=None, project=None, limit=20):
    db_path = Path(db_path)
    if not db_path.is_file():
        raise QueryError(f"no index at {db_path} — run: kbindex.py build")
    sql = (
        "SELECT kind, project, path, ref, date, text, "
        f"snippet(kb, 5, '[', ']', ' … ', {SNIPPET_TOKENS}) "
        "FROM kb WHERE kb MATCH ?"
    )
    params = [query]
    if kind:
        sql += " AND kind = ?"
        params.append(kind)
    if project:
        sql += " AND project = ?"
        params.append(project)
    sql += " ORDER BY rank LIMIT ?"
    params.append(int(limit))
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    except sqlite3.Error as exc:
        raise QueryError(f"cannot open {db_path}: {exc}") from exc
    try:
        rows = conn.execute(sql, params).fetchall()
    except sqlite3.Error as exc:
        raise QueryError(f"bad FTS5 query {query!r}: {exc}") from exc
    finally:
        conn.close()
    return [Hit(*r) for r in rows]


def _one_line(text, width=300):
    flat = " ".join(text.split())
    return flat[:width] + ("…" if len(flat) > width else "")


# Printing a snippet takes text out of a 0600 database and puts it on a
# terminal, into scrollback and into whatever the operator pastes next.
# Transcripts contain pasted private keys, wallet addresses and tokens, so the
# shapes below are masked on the way out. This is a net, not a guarantee: a
# secret in an unrecognised shape still gets printed — hence the header
# warning. `[`/`]` around a term are FTS5 match markers, so the patterns
# tolerate them inside a run.
_HEXISH = r"[0-9a-fA-F\[\]]"
REDACTIONS = (
    # 0x-prefixed values: EVM/HL addresses and private keys alike
    (re.compile(rf"\b0x{_HEXISH}{{16,}}"), "[redacted:0x]"),
    # bare long hex: 64-char keys and hashes. 40 is left alone so that git
    # SHAs — the whole point of a commit pointer — stay readable.
    (re.compile(rf"\b{_HEXISH}{{48,}}\b"), "[redacted:hex]"),
    (re.compile(r"\beyJ[A-Za-z0-9_\-\[\]]{8,}\.[A-Za-z0-9_\-\[\]]{8,}\.[A-Za-z0-9_\-\[\]]{5,}"),
     "[redacted:jwt]"),
    (re.compile(r"(?i)\b(?:bearer|basic)\s+[A-Za-z0-9+/=._\-\[\]]{12,}"), "[redacted:auth]"),
    (re.compile(r"\b(?:sk|pk|rk)-[A-Za-z0-9_\-\[\]]{16,}"), "[redacted:token]"),
    (re.compile(r"\bgh[pousr]_[A-Za-z0-9\[\]]{20,}"), "[redacted:token]"),
    (re.compile(r"\bxox[abposr]-[A-Za-z0-9\-\[\]]{10,}"), "[redacted:token]"),
    (re.compile(r"\bAKIA[0-9A-Z\[\]]{16}\b"), "[redacted:token]"),
    # key = value / "key": "value" for credential-shaped names
    (re.compile(r"(?i)\b(api[_-]?keys?|secrets?|passwords?|passwd|passphrase|"
                r"tokens?|private[_-]?keys?|mnemonic|seed[_-]?phrase|"
                r"credentials?)\b(\"?\s*[:=]\s*)\"?'?[^\s\"',;]{6,}"),
     r"\1\2[redacted]"),
)


def redact(text):
    """Mask recognisable secrets in text that is about to be printed."""
    if not text:
        return text
    for pattern, replacement in REDACTIONS:
        text = pattern.sub(replacement, text)
    return text


def format_hits(hits, query=None):
    out = [POINTER_HEADER]
    if query is not None:
        out.append(f"query: {query}")
    if not hits:
        out.append("(no matches — unicode61 does not stem; try a prefix query like слов*)")
        return "\n".join(out)
    out.append("")
    for i, h in enumerate(hits, 1):
        loc = f"{h.path}#{h.ref}" if h.ref else f"{h.path}#"
        out.append(f"{i:>3}. {h.kind} | {h.project} | {h.date or '-'} | {loc}")
        out.append(f"     {redact(_one_line(h.snippet))}")
    return "\n".join(out)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _apply_add_roots(config, add_roots):
    claude = list(config.claude_roots)
    docs = list(config.doc_dirs)
    repos = list(config.git_repos)
    for spec in add_roots or []:
        if "=" in spec:
            kind, _, path = spec.partition("=")
        else:
            kind, path = "claude", spec
        kind = kind.strip().lower()
        target = {"claude": claude, "docs": docs, "git": repos}.get(kind)
        if target is None:
            raise SystemExit(f"--add-root: unknown kind {kind!r}; use claude|docs|git")
        target.append(Path(path).expanduser())
    return replace(config, claude_roots=claude, doc_dirs=docs, git_repos=repos)


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="kbindex.py",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_build = sub.add_parser("build", help="full rebuild of the index",
                             description=__doc__,
                             formatter_class=argparse.RawDescriptionHelpFormatter)
    p_build.add_argument("--db", default=str(DEFAULT_DB))
    p_build.add_argument("--add-root", action="append", metavar="KIND=PATH",
                         help="extra source root; KIND is claude|docs|git "
                              "(bare path = claude)")

    p_search = sub.add_parser("search", help="find where something was discussed",
                              description=__doc__,
                              formatter_class=argparse.RawDescriptionHelpFormatter)
    p_search.add_argument("query", help="FTS5 query; see the syntax notes above")
    p_search.add_argument("--db", default=str(DEFAULT_DB))
    p_search.add_argument("--kind", choices=KINDS)
    p_search.add_argument("--project")
    p_search.add_argument("--limit", type=int, default=20)

    args = parser.parse_args(argv)

    if args.command == "build":
        config = _apply_add_roots(DEFAULT_CONFIG, args.add_root)
        print(f"kbindex build -> {args.db}")
        stats = build(args.db, config, log=print)
        total = sum(stats["counts"].values())
        print(f"  total rows={total} db={stats['db_bytes'] / 1e6:.1f} MB "
              f"elapsed={stats['elapsed']}s")
        where = "in a 0700 dir kbindex owns" if stats["dir_managed"] \
            else "in a directory kbindex did not lock down (see warning above)"
        print(f"  db is 0600 {where}: it holds pasted secrets — never commit "
              "or copy it off this machine")
        if stats["missing_roots"]:
            print(f"  INCOMPLETE: {len(stats['missing_roots'])} source root(s) "
                  "missing — see warnings above", file=sys.stderr)
            return 3
        return 0

    try:
        hits = search(args.db, args.query, kind=args.kind,
                      project=args.project, limit=args.limit)
    except QueryError as exc:
        print(f"kbindex: {exc}", file=sys.stderr)
        return 2
    print(format_hits(hits, query=args.query))
    return 0


if __name__ == "__main__":
    sys.exit(main())
