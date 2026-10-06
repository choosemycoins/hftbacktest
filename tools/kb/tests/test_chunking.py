"""Chunkers: markdown, plain text, Claude /export, transcript messages, chat
windows, ASR grid, commits. Synthetic inputs only."""

import pytest

from kb import chunking as ch
from kb.chunking import CAP, OVERLAP


def lines_of(text: str, loc: str) -> str:
    """Source lines a 'L<a>-<b>' / 'L<a>' loc points at."""
    body = loc[1:]
    a, _, b = body.partition("-")
    a, b = int(a), int(b or a)
    return "\n".join(text.split("\n")[a - 1:b])


# ---------------------------------------------------------------------------
# split_text
# ---------------------------------------------------------------------------

def test_split_short_text_is_one_piece():
    assert ch.split_text("abc") == [(0, 3)]


def test_split_long_text_respects_cap_overlap_and_covers_everything():
    words = " ".join(f"w{i:05d}" for i in range(2000))          # ~14k chars
    spans = ch.split_text(words)
    assert len(spans) > 5
    assert all(b - a <= CAP for a, b in spans)
    assert spans[0][0] == 0 and spans[-1][1] == len(words)
    for (a1, b1), (a2, b2) in zip(spans, spans[1:]):
        assert a2 < b1, "consecutive pieces must overlap"
        assert b1 - a2 <= OVERLAP + 10
        assert words[a2 - 1] == " ", "a piece starts at a word boundary"


def test_split_prefers_paragraph_then_line_breaks():
    para = ("x" * 99 + "\n") * 9                       # 900 chars of lines
    text = para + "\n" + para                          # paragraph break at 901
    spans = ch.split_text(text)
    assert text[spans[0][1] - 2:spans[0][1]] == "\n\n" or text[spans[0][1] - 1] == "\n"


def test_split_never_returns_empty_or_stalls_on_unbreakable_runs():
    blob = "y" * 5000
    spans = ch.split_text(blob)
    assert all(b > a for a, b in spans)
    assert all(b - a <= CAP for a, b in spans)
    assert spans[-1][1] == 5000


# ---------------------------------------------------------------------------
# markdown
# ---------------------------------------------------------------------------

MD = """---
name: lighter-standard
description: "Standard: 200 мс задержка, 60 sendTx в минуту"
---

# Lighter

Вступление про площадку, достаточно длинное, чтобы раздел не сливался со
следующим: """ + "контекст " * 40 + """

## Тарифы

| тариф | мейкер | тейкер |
|---|---|---|
| Standard | 0 | 0 |

```python
# not a heading inside a fence
x = 1
```

## Тарифы

Второй раздел с тем же путём заголовков.
"""


def test_frontmatter_is_part_of_the_first_chunk():
    pieces = ch.chunk_markdown(MD, "note.md")
    assert pieces[0].text.startswith("---\nname: lighter-standard")
    assert "60 sendTx" in pieces[0].text


def test_heading_inside_a_fenced_block_does_not_split():
    pieces = ch.chunk_markdown(MD, "note.md")
    fenced = [p for p in pieces if "x = 1" in p.text]
    assert len(fenced) == 1
    assert "# not a heading inside a fence" in fenced[0].text
    assert all("not a heading" not in p.title for p in pieces)


def test_title_is_filename_and_heading_path():
    pieces = ch.chunk_markdown(MD, "note.md")
    tariffs = [p for p in pieces if "| Standard |" in p.text][0]
    assert tariffs.title == "note.md › Lighter › Тарифы"


def test_duplicate_heading_paths_get_numbered_anchors():
    big = "# A\n\n## B\n\n" + "слово " * 300 + "\n\n## B\n\n" + "другое " * 300 + "\n"
    pieces = ch.chunk_markdown(big, "f.md")
    anchors = [p.anchor for p in pieces]
    assert len(set(anchors)) == len(anchors)
    assert any(a.endswith("#2") for a in anchors), anchors


def test_small_sections_merge_forward_and_keep_the_first_anchor():
    text = "# A\n\nshort a\n\n# B\n\nshort b\n\n# C\n\n" + "long c " * 100 + "\n"
    pieces = ch.chunk_markdown(text, "f.md")
    first = pieces[0]
    assert "short a" in first.text and "short b" in first.text
    assert first.title == "f.md › A"


def test_long_section_is_split_with_overlap_and_no_empty_pieces():
    text = "# Big\n\n" + "\n\n".join("абзац номер %d " % i + "текст " * 40 for i in range(30)) + "\n"
    pieces = ch.chunk_markdown(text, "f.md")
    assert len(pieces) > 3
    assert all(p.text.strip() for p in pieces)
    assert all(len(p.text) <= CAP for p in pieces)
    assert pieces[0].anchor == "Big"
    assert pieces[1].anchor == "Big/p2", "parts after the first get /p<k>; the first keeps the bare anchor"


def test_loc_points_at_the_real_source_lines():
    pieces = ch.chunk_markdown(MD, "note.md")
    for p in pieces:
        src = lines_of(MD, p.loc)
        assert p.text.splitlines()[0] in src
        assert p.text.splitlines()[-1] in src


def test_editing_another_section_keeps_ids_editing_own_changes_text_only():
    base = "# A\n\n" + "альфа " * 80 + "\n\n# B\n\n" + "бета " * 80 + "\n"
    edited_other = base.replace("альфа альфа", "альфа ОМЕГА альфа", 1)
    edited_own = base.replace("бета бета", "бета ГАММА бета", 1)
    p0 = {p.anchor: p.text for p in ch.chunk_markdown(base, "f.md")}
    p1 = {p.anchor: p.text for p in ch.chunk_markdown(edited_other, "f.md")}
    p2 = {p.anchor: p.text for p in ch.chunk_markdown(edited_own, "f.md")}
    assert set(p0) == set(p1) == set(p2) == {"A", "B"}
    assert p1["B"] == p0["B"]
    assert p2["B"] != p0["B"] and p2["A"] == p0["A"]


def test_empty_markdown_gives_no_pieces():
    assert ch.chunk_markdown("   \n\n", "f.md") == []


# ---------------------------------------------------------------------------
# plain text and docstrings
# ---------------------------------------------------------------------------

def test_plain_text_packs_paragraphs_with_p_anchors():
    text = "\n\n".join("параграф %d " % i + "слово " * 60 for i in range(20))
    pieces = ch.chunk_plain(text, "notes.txt")
    assert [p.anchor for p in pieces][:3] == ["p1", "p2", "p3"]
    assert all(len(p.text) <= CAP for p in pieces)
    assert all(p.title == "notes.txt" for p in pieces)


def test_docstring_chunks_use_docstring_prefix_and_module_title():
    src = '"""Study: unlock tail.\n\nSecond line."""\nX = 1\n'
    pieces = ch.chunk_docstring(src, "study.py")
    assert len(pieces) == 1
    assert pieces[0].anchor == "docstring/p1"
    assert pieces[0].title == "study.py"
    assert "unlock tail" in pieces[0].text
    assert pieces[0].loc == "L1-3"


def test_docstring_of_unparsable_or_undocumented_module_is_none():
    assert ch.chunk_docstring("def (:\n", "x.py") is None
    assert ch.chunk_docstring("X = 1\n", "x.py") == []


# ---------------------------------------------------------------------------
# Claude /export
# ---------------------------------------------------------------------------

EXPORT = """╭─── Claude Code v2.1.179 ─────────────────────────────╮
│                Welcome back Andrew!                   │
│  Opus 4.8 · Claude Max · someone@example.com's Org    │
╰──────────────────────────────────────────────────────╯


✻ Conversation compacted (ctrl+o for history)


❯ /compact
  ⎿  Compacted (ctrl+o to see full summary)
  ⎿  Read crypto_squeeze_clf.py (136 lines)

❯ для памп детектора ты фитил бустинг на каких фичах?

● Let me pull up the exact feature list.

  Read 1 file, listed 1 directory (ctrl+o to expand)

● Bash(grep -n "FEATS = " crypto_pump_bybit.py; echo
      "--- dex full FEATS ---"; cat SECRETTOOLCALL)
  ⎿  --- dex full FEATS ---
     FEATS = ["ret_1", "SECRETTOOLOUTPUT"]
     … +5 lines (ctrl+o to expand)

  Searched for 1 pattern (ctrl+o to expand)

● Вот точный список фич для памп-детектора, на которых фитился
  LightGBM. Все каузальны.

  - ret_1, ret_5 — доходность
  │ таблица │ внутри │

❯ спасибо
"""


def test_export_drops_banner_tool_io_status_and_summaries():
    pieces = ch.chunk_export(EXPORT, "2026-06-18-005010-x.txt")
    blob = "\n".join(p.text for p in pieces)
    for gone in ("someone@example.com", "Welcome back", "SECRETTOOLCALL", "SECRETTOOLOUTPUT",
                 "ctrl+o", "Compacted", "Conversation compacted", "crypto_squeeze_clf.py"):
        assert gone not in blob, gone


def test_export_banner_is_dropped_even_after_a_turn():
    text = "● ответ\n\n" + EXPORT.split("\n\n\n")[0] + "\n\n❯ дальше\n"
    pieces = ch.chunk_export(text, "e.txt")
    blob = "\n".join(p.text for p in pieces)
    assert "someone@example.com" not in blob and "Welcome back" not in blob
    assert "ответ" in blob and "дальше" in blob


def test_heading_inside_a_long_fenced_block_does_not_start_a_section():
    code = "\n".join(f"x{i} = {i}  # строка кода" for i in range(40))
    text = "# Код\n\n```python\n" + code + "\n# fenced comment\n" + code + "\n```\n"
    pieces = ch.chunk_markdown(text, "f.md")
    assert {p.anchor for p in pieces} <= {"Код", "Код/p2", "Код/p3"}, [p.anchor for p in pieces]
    assert all(p.title == "f.md › Код" for p in pieces)


def test_export_keeps_operator_and_assistant_turns():
    pieces = ch.chunk_export(EXPORT, "2026-06-18-005010-x.txt")
    roles = [p.title.rsplit(" · ", 1)[1] for p in pieces]
    assert roles == ["user", "assistant", "user"], roles
    assert "памп детектора" in pieces[0].text
    assistant = pieces[1].text
    assert "Let me pull up" in assistant and "LightGBM. Все каузальны." in assistant
    assert "│ таблица │ внутри │" in assistant, "box tables inside prose are prose, not banner"
    assert [p.anchor for p in pieces] == ["t1", "t2", "t3"]


def test_export_loc_points_at_file_lines():
    pieces = ch.chunk_export(EXPORT, "e.txt")
    first = pieces[0]
    assert "памп детектора" in lines_of(EXPORT, first.loc)


def test_export_bare_slash_command_turn_is_dropped():
    pieces = ch.chunk_export("❯ /effort\n  ⎿  Set effort\n\n❯ /goal собрать индекс\n", "e.txt")
    assert [p.text for p in pieces] == ["/goal собрать индекс"]


# ---------------------------------------------------------------------------
# transcript messages
# ---------------------------------------------------------------------------

def test_transcript_message_short_is_one_piece_with_uuid_anchor():
    p = ch.chunk_message("короткое сообщение", lineno=377, uuid="u-1",
                         title="0cafbaca/main · user", date="2026-06-01T10:01:00Z")
    assert len(p) == 1
    assert p[0].anchor == "u-1" and p[0].loc == "L377" and p[0].date == "2026-06-01T10:01:00Z"


def test_transcript_message_without_uuid_uses_line_and_long_one_is_split():
    long = "слово " * 1000
    p = ch.chunk_message(long, lineno=12, uuid=None, title="t", date="d")
    assert p[0].anchor == "L12" and p[1].anchor == "L12/p2"
    assert all(x.loc == "L12" for x in p)
    assert all(len(x.text) <= CAP for x in p)


# ---------------------------------------------------------------------------
# chat windows
# ---------------------------------------------------------------------------

def msg(i, ts, text, sender="Alice", file="messages.html"):
    return {"id": i, "ts": ts, "sender": sender, "text": text, "file": file}


T0 = 1_757_000_000.0     # 2025-09-04T15:33:20Z


def test_chat_window_breaks_on_a_15_minute_gap():
    ms = [msg(1, T0, "a"), msg(2, T0 + 60, "b"), msg(3, T0 + 60 + 901, "c")]
    pieces = ch.chunk_chat(ms, "Chat")
    assert [p.loc for p in pieces] == ["message1..2", "message3..3"]
    assert pieces[0].anchor == "m1" and pieces[1].anchor == "m3"


def test_chat_window_breaks_on_the_size_cap():
    ms = [msg(i, T0 + i, "x" * 600) for i in range(1, 6)]
    pieces = ch.chunk_chat(ms, "Chat")
    assert len(pieces) >= 3
    assert all(len(p.text) <= CAP for p in pieces)


def test_chat_window_line_format_title_date_and_file():
    ms = [msg(7, T0, "привет", sender="Bob", file="messages3.html"), msg(8, T0 + 30, "ок")]
    p = ch.chunk_chat(ms, "Deribit RU")[0]
    assert p.text.splitlines()[0] == "15:33 Bob: привет"
    assert p.title == "Deribit RU · 2025-09-04"
    assert p.date == "2025-09-04T15:33:20Z"
    assert p.file == "messages3.html"


def test_chat_long_message_is_its_own_window_split_in_parts():
    ms = [msg(1, T0, "до"), msg(2, T0 + 5, "длинно " * 600), msg(3, T0 + 10, "после")]
    pieces = ch.chunk_chat(ms, "C")
    anchors = [p.anchor for p in pieces]
    assert anchors[0] == "m1" and "m2" in anchors and "m2/p2" in anchors and anchors[-1] == "m3"


def test_chat_appending_messages_only_touches_the_tail():
    ms = [msg(i, T0 + i * 1000, "m%d" % i) for i in range(1, 6)]
    before = {p.anchor: p.text for p in ch.chunk_chat(ms, "C")}
    after = {p.anchor: p.text for p in ch.chunk_chat(ms + [msg(6, T0 + 5000 + 30, "new")], "C")}
    changed = {a for a in after if before.get(a) != after[a]}
    assert changed == {"m5"}


# ---------------------------------------------------------------------------
# ASR grid
# ---------------------------------------------------------------------------

SEGS = [
    {"s": 0.05, "e": 23.7, "t": "друзья всем привет"},
    {"s": 23.7, "e": 89.0, "t": "введение"},
    {"s": 89.9, "e": 95.0, "t": "конец первой минуты"},
    {"s": 95.0, "e": 181.5, "t": "вторая полторы"},
    {"s": 200.0, "e": 210.0, "t": "третья"},
]


def test_asr_grid_is_90_seconds_by_segment_start():
    pieces = ch.chunk_asr(SEGS, "cryptology10", "13.Риск")
    assert [p.anchor for p in pieces] == ["t0", "t90", "t180"]
    assert pieces[0].text == "друзья всем привет введение конец первой минуты"
    assert pieces[1].text == "вторая полторы"


def test_asr_loc_runs_from_first_start_to_last_end():
    pieces = ch.chunk_asr(SEGS, "c", "l")
    assert pieces[0].loc == "t=00:00-01:35"
    assert pieces[1].loc == "t=01:35-03:01"
    assert pieces[0].title == "c · l · 00:00"


def test_asr_retranscription_keeps_anchors():
    other = [dict(s, t=s["t"].upper()) for s in SEGS]
    assert [p.anchor for p in ch.chunk_asr(SEGS, "c", "l")] == \
           [p.anchor for p in ch.chunk_asr(other, "c", "l")]


# ---------------------------------------------------------------------------
# commits
# ---------------------------------------------------------------------------

def test_commit_is_one_piece_with_sha_anchor_and_subject_title():
    sha = "9840d4a" + "0" * 33
    p = ch.chunk_commit(sha, "connector/hl: отмена перестала платить 290 мс", "тело\nвторая строка",
                        "2026-09-30T10:00:00Z")
    assert len(p) == 1
    assert p[0].anchor == sha and p[0].loc == sha
    assert p[0].title.startswith("connector/hl")
    assert "вторая строка" in p[0].text and p[0].date == "2026-09-30T10:00:00Z"


@pytest.mark.parametrize("fn", ["chunk_markdown", "chunk_plain", "chunk_export"])
def test_every_piece_is_nonempty_and_capped(fn):
    text = "# T\n\n" + ("строка текста " * 30 + "\n") * 80
    if fn == "chunk_export":        # an export is a sequence of turns
        text = "● " + text.replace("\n", "\n  ")
    pieces = getattr(ch, fn)(text, "f")
    assert pieces and all(p.text.strip() and len(p.text) <= CAP for p in pieces)
