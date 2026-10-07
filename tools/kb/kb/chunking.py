"""Chunkers: the text of one unit (file, chat, repo) -> list[Piece].

Every piece carries
  anchor  a LOGICAL position that survives edits elsewhere in the unit
          (heading path, message uuid, first chat message id, 90-s grid slot,
          commit sha). chunk_id = sha256(source_id ␟ rel ␟ anchor)[:16], so a
          pointer stays valid while its own text changes.
  loc     where it is in the source right now ('L12-40', 'L377',
          'message5..9', 't=12:30-14:00', a sha) — metadata, not identity.
  title   context (file › H1 › H2, chat · day, lesson · mm:ss, subject).
  text    as found in the source, never rewritten (masking happens on output).

Pieces are at most CAP characters; a long unit is cut at a paragraph, then a
line, then a space, and consecutive parts overlap by ~OVERLAP characters. The
first part keeps the bare anchor and later parts get '/p<k>', so a section that
grows past CAP keeps the id of its first part.
"""

from __future__ import annotations

import ast
import bisect
import re
from dataclasses import dataclass
from datetime import datetime, timezone

CAP = 1500
OVERLAP = 150
SMALL = 300            # markdown sections shorter than this merge forward
CHAT_GAP_SECONDS = 15 * 60
ASR_WINDOW_SECONDS = 90


@dataclass(frozen=True)
class Piece:
    anchor: str
    title: str
    text: str
    loc: str
    date: str | None = None       # overrides the unit's date (messages, windows, commits)
    file: str | None = None       # chat: '<export dir>/messagesN.html'


# ---------------------------------------------------------------------------
# splitting
# ---------------------------------------------------------------------------

def _best_break(text: str, lo: int, hi: int) -> int:
    """End offset for a piece that must end in (lo, hi]: after a blank line,
    else after a newline, else after a space, else hi (hard cut)."""
    for sep in ("\n\n", "\n", " "):
        i = text.rfind(sep, lo, hi)
        if i != -1:
            return i + len(sep)
    return hi


def _align_start(text: str, pos: int, limit: int) -> int:
    """Move `pos` forward to the start of a word (after whitespace), not past `limit`."""
    if pos <= 0 or text[pos - 1].isspace():
        return pos
    for i in range(pos, limit):
        if text[i].isspace():
            return i + 1
    return pos


def split_text(text: str, cap: int = CAP, overlap: int = OVERLAP) -> list[tuple[int, int]]:
    """[(start, end)] covering `text`; each piece <= cap, consecutive pieces overlap."""
    n = len(text)
    if n <= cap:
        return [(0, n)]
    spans = []
    start = 0
    while n - start > cap:
        end = _best_break(text, start + cap // 2, start + cap)
        spans.append((start, end))
        nxt = _align_start(text, max(end - overlap, start + 1), end)
        start = nxt if nxt < end else end
    spans.append((start, n))
    return spans


def _strip_span(text: str, a: int, b: int) -> tuple[int, int]:
    while a < b and text[a].isspace():
        a += 1
    while b > a and text[b - 1].isspace():
        b -= 1
    return a, b


class _Lines:
    """Offset -> 1-based line number of one source text."""

    def __init__(self, text: str, numbers: list[int] | None = None):
        self.text = text
        self.starts = [0] + [m.end() for m in re.finditer("\n", text)]
        self.numbers = numbers      # line index -> original line number

    def line(self, off: int) -> int:
        idx = bisect.bisect_right(self.starts, off) - 1
        return self.numbers[idx] if self.numbers is not None else idx + 1

    def loc(self, a: int, b: int) -> str:
        return f"L{self.line(a)}-{self.line(max(a, b - 1))}"


def _span_pieces(src: _Lines, a: int, b: int, anchor: str, title: str, **extra) -> list[Piece]:
    """Pieces of src.text[a:b] (stripped), split when longer than CAP."""
    a, b = _strip_span(src.text, a, b)
    if a >= b:
        return []
    out = []
    body = src.text[a:b]
    for k, (x, y) in enumerate(split_text(body)):
        x, y = _strip_span(body, x, y)
        if x >= y:
            continue
        anc = anchor if not out else f"{anchor}/p{len(out) + 1}"
        out.append(Piece(anc, title, body[x:y], src.loc(a + x, a + y), **extra))
    return out


def _parts(text: str, anchor: str, title: str, loc: str, **extra) -> list[Piece]:
    """Pieces of one text whose loc does not depend on the cut (messages, windows)."""
    out = []
    for x, y in split_text(text):
        x, y = _strip_span(text, x, y)
        if x >= y:
            continue
        anc = anchor if not out else f"{anchor}/p{len(out) + 1}"
        out.append(Piece(anc, title, text[x:y], loc, **extra))
    return out


# ---------------------------------------------------------------------------
# markdown
# ---------------------------------------------------------------------------

_FENCE = re.compile(r"^ {0,3}(`{3,}|~{3,})")
_ATX = re.compile(r"^ {0,3}(#{1,6})(?:[ \t]+(.*?))?[ \t]*$")
TOP_ANCHOR = "(top)"
SEP = " › "


def _heading(line: str):
    m = _ATX.match(line)
    if not m:
        return None
    title = re.sub(r"[ \t]+#+$", "", (m.group(2) or "")).strip()
    if re.fullmatch(r"#*", title):
        title = ""
    return len(m.group(1)), title


def _md_sections(text: str):
    """[(heading path, start offset, end offset)] — the preamble (with any
    frontmatter) has the empty path. Headings inside fenced blocks are text."""
    lines = text.split("\n")
    offs, pos = [], 0
    for line in lines:
        offs.append(pos)
        pos += len(line) + 1
    first = 0
    if lines and lines[0].rstrip() == "---":
        for j in range(1, len(lines)):
            if lines[j].rstrip() in ("---", "..."):
                first = j + 1
                break
    sections, stack, cur_path, cur_start, fence = [], [], (), 0, None
    for i in range(first, len(lines)):
        line = lines[i]
        if fence:
            if re.fullmatch(r" {0,3}" + re.escape(fence[0]) + "{%d,}[ \t]*" % len(fence), line):
                fence = None
            continue
        fm = _FENCE.match(line)
        if fm:
            fence = fm.group(1)
            continue
        h = _heading(line)
        if h is None:
            continue
        level, title = h
        if offs[i] > cur_start:
            sections.append((cur_path, cur_start, offs[i]))
        while stack and stack[-1][0] >= level:
            stack.pop()
        stack.append((level, title))
        cur_path = tuple(t for _, t in stack)
        cur_start = offs[i]
    sections.append((cur_path, cur_start, len(text)))
    return sections


def _heading_only(chunk: str) -> bool:
    lines = [ln for ln in chunk.strip().split("\n") if ln.strip()]
    return len(lines) == 1 and _heading(lines[0]) is not None


def chunk_markdown(text: str, filename: str) -> list[Piece]:
    src = _Lines(text)
    secs = []
    for path, a, b in _md_sections(text):
        if text[a:b].strip():
            secs.append((path, a, b))
    # anchors: heading path, numbered on repeats of the same anchor string
    seen: dict[str, int] = {}
    anchored = []
    for path, a, b in secs:
        base = SEP.join(path) if path else TOP_ANCHOR
        seen[base] = seen.get(base, 0) + 1
        anchor = base if seen[base] == 1 else f"{base}#{seen[base]}"
        anchored.append((path, anchor, a, b))
    pieces: list[Piece] = []
    i = 0
    while i < len(anchored):
        path, anchor, a, b = anchored[i]
        size = len(text[a:b].strip())
        while size < SMALL and i + 1 < len(anchored):
            nb = anchored[i + 1][3]
            nsize = len(text[a:nb].strip())
            if nsize > CAP:
                break
            i += 1
            b, size = nb, nsize
        i += 1
        if _heading_only(text[a:b]):
            continue        # its heading is in the next section's title anyway
        title = SEP.join((filename,) + path) if path else filename
        pieces.extend(_span_pieces(src, a, b, anchor, title))
    return pieces


# ---------------------------------------------------------------------------
# plain text, docstrings
# ---------------------------------------------------------------------------

def chunk_plain(text: str, title: str, prefix: str = "") -> list[Piece]:
    src = _Lines(text)
    out = []
    a, b = _strip_span(text, 0, len(text))
    body = text[a:b]
    for x, y in split_text(body):
        x, y = _strip_span(body, x, y)
        if x < y:
            out.append(Piece(f"{prefix}p{len(out) + 1}", title, body[x:y], src.loc(a + x, a + y)))
    return out


def chunk_docstring(source: str, filename: str) -> list[Piece] | None:
    """Module docstring as pieces; [] if there is none; None if unparsable.
    The file is parsed with ast — never imported or executed."""
    try:
        tree = ast.parse(source)
    except (SyntaxError, ValueError):
        return None
    doc = ast.get_docstring(tree)
    if not doc or not doc.strip():
        return []
    node = tree.body[0]
    loc = f"L{node.lineno}-{node.end_lineno or node.lineno}"
    return [Piece(f"docstring/p{k + 1}", filename, p.text, loc)
            for k, p in enumerate(chunk_plain(doc.strip(), filename))]


# ---------------------------------------------------------------------------
# Claude /export
# ---------------------------------------------------------------------------

_TOOL_CALL = re.compile(
    r"^[●⏺] (?:[A-Z][A-Za-z0-9_]*|mcp__[\w-]+|[\w.-]+ - [\w .-]+ \(MCP\))\(")
_SUMMARY = re.compile(r"\(ctrl\+o to (?:expand|see)")
_TOOL_NAME = re.compile(r"[A-Z][A-Za-z]*(?: [A-Z][A-Za-z]*){0,3}")   # "Stop Task", "Read"
_BARE_COMMAND = re.compile(r"^/[\w:.-]+\s*$")


def _export_turns(text: str):
    """[(role, [(line number, text)])] with tool calls/outputs, banner,
    status lines and collapsed tool summaries removed."""
    turns: list[tuple[str, list]] = []
    cur = None
    state = None             # 'user' | 'assistant' | 'skip' | None
    banner = False
    header = None            # last "● Name" line: a tool header if ⎿ follows at once
    for no, raw in enumerate(text.split("\n"), 1):
        line = raw.rstrip()
        if banner:
            if line.startswith("╰"):
                banner = False
            continue
        if line.startswith("╭"):           # the welcome banner (carries the account e-mail);
            banner = True                   # prose tables are indented, never at column 0
            continue
        if line.startswith("❯"):
            cur = ("user", [(no, line[1:].strip())])
            turns.append(cur)
            state = "user"
            continue
        if line[:1] in ("●", "⏺"):
            if _TOOL_CALL.match(line) or "⎿" in line:      # "● Stop Task  ⎿  …"
                state = "skip"
                continue
            content = line[1:].strip()
            if cur is not None and cur[0] == "assistant":
                cur[1].append((no, content))
            else:
                cur = ("assistant", [(no, content)])
                turns.append(cur)
            state = "assistant"
            header = (no, content) if _TOOL_NAME.fullmatch(content) else None
            continue
        if line.startswith("✻"):
            continue
        if "⎿" in line:                    # tool output, also inline ("  Stop Task  ⎿  …")
            if header is not None and cur is not None and cur[1] and cur[1][-1] == header:
                cur[1].pop()               # "● Stop Task" was a tool header, not prose
            header = None
            state = "skip"
            continue
        if line.strip():
            header = None
        if _SUMMARY.search(line):
            continue
        if state == "skip":
            if not line.strip():
                continue
            indent = len(raw) - len(raw.lstrip(" "))
            if indent >= 3:              # prose sits at 2; tool text continues at 3+ (inline) / 5+
                continue
            state = cur[0] if cur is not None else None
        if state in ("user", "assistant") and cur is not None:
            cur[1].append((no, line[2:] if line.startswith("  ") else line))
    return turns


def chunk_export(text: str, filename: str) -> list[Piece]:
    stem = filename.rsplit(".", 1)[0][:40]
    out: list[Piece] = []
    n = 0
    for role, lines in _export_turns(text):
        while lines and not lines[-1][1].strip():
            lines.pop()
        while lines and not lines[0][1].strip():
            lines.pop(0)
        if not lines:
            continue
        body = "\n".join(t for _, t in lines)
        if role == "user" and _BARE_COMMAND.match(body):
            continue
        n += 1
        src = _Lines(body, [no for no, _ in lines])
        out.extend(_span_pieces(src, 0, len(body), f"t{n}", f"{stem} · {role}"))
    return out


# ---------------------------------------------------------------------------
# transcript messages, commits
# ---------------------------------------------------------------------------

def chunk_message(text: str, lineno: int, uuid: str | None, title: str, date: str) -> list[Piece]:
    anchor = uuid or f"L{lineno}"
    return _parts(text, anchor, title, f"L{lineno}", date=date)


def chunk_commit(sha: str, subject: str, body: str, date: str) -> list[Piece]:
    text = subject if not body.strip() else f"{subject}\n{body.strip()}"
    return _parts(text, sha, subject[:200], sha, date=date)


# ---------------------------------------------------------------------------
# chat windows
# ---------------------------------------------------------------------------

def iso_utc(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def chunk_chat(messages: list[dict], chat_title: str) -> list[Piece]:
    """messages: {id, ts (epoch s), sender, text, file}. Windows break on a
    gap > 15 min or when the window would exceed CAP; a message longer than
    CAP is its own window, cut in parts."""
    msgs = sorted(messages, key=lambda m: (m["ts"], m["id"]))
    out: list[Piece] = []
    win: list[tuple[dict, str]] = []
    size = 0

    def flush():
        nonlocal win, size
        if not win:
            return
        first, last = win[0][0], win[-1][0]
        date = iso_utc(first["ts"])
        text = "\n".join(line for _, line in win)
        out.extend(_parts(text, f"m{first['id']}", f"{chat_title} · {date[:10]}",
                          f"message{first['id']}..{last['id']}", date=date, file=first.get("file")))
        win, size = [], 0

    prev_ts = None
    for m in msgs:
        hhmm = datetime.fromtimestamp(m["ts"], tz=timezone.utc).strftime("%H:%M")
        line = f"{hhmm} {m.get('sender') or '?'}: {m['text']}"
        gap = prev_ts is not None and m["ts"] - prev_ts > CHAT_GAP_SECONDS
        if win and (gap or size + 1 + len(line) > CAP):
            flush()
        win.append((m, line))
        size += len(line) + (1 if size else 0)
        if len(line) > CAP:
            flush()
        prev_ts = m["ts"]
    flush()
    return out


# ---------------------------------------------------------------------------
# ASR
# ---------------------------------------------------------------------------

def mmss(seconds: float) -> str:
    s = int(seconds)
    return f"{s // 60:02d}:{s % 60:02d}"


def chunk_asr(segments: list[dict], course: str, lesson: str) -> list[Piece]:
    windows: dict[int, list[dict]] = {}
    for seg in segments:
        text = str(seg.get("t") or "").strip()
        if not text:
            continue
        k = int(float(seg["s"]) // ASR_WINDOW_SECONDS)
        windows.setdefault(k, []).append(seg)
    out = []
    for k in sorted(windows):
        segs = windows[k]
        text = " ".join(str(s["t"]).strip() for s in segs)
        start = k * ASR_WINDOW_SECONDS
        loc = f"t={mmss(float(segs[0]['s']))}-{mmss(float(segs[-1]['e']))}"
        out.extend(_parts(text, f"t{start}", f"{course} · {lesson} · {mmss(start)}", loc))
    return out
