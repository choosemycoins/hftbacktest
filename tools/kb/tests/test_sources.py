"""Source enumeration and parsing: what each kind takes and what it leaves."""

import json
import os
from pathlib import Path

from conftest import html_message, write_html_export

from kb import config as cfgmod
from kb import sources


def scans(corpus):
    cfg = cfgmod.load_config(corpus["config"])
    return {s.source_id: s for s in sources.scan_all(cfg)}, cfg


def parse_all(scan):
    out = {}
    for c in scan.candidates:
        pieces, _ = c.parse()
        out[c.rel] = (c, pieces)
    return out


# --- transcripts --------------------------------------------------------------

def test_transcripts_take_conversation_text_only(corpus):
    s, _ = scans(corpus)
    t = parse_all(s["claude"])
    main = t["-home-andrew-RustroverProjects-hftbacktest/s1.jsonl"]
    cand, pieces = main
    assert cand.kind == "transcript" and cand.project == "hftbacktest"
    texts = [p.text for p in pieces]
    assert texts == ["почему прод терял деньги на комиссиях?",
                     "Lighter монетизирует латентность, а не спред."]
    assert [p.anchor for p in pieces] == ["u1", "u2"]
    assert [p.loc for p in pieces] == ["L1", "L2"]
    assert pieces[0].date == "2026-08-09T10:00:00Z"
    assert pieces[0].title == "s1/main · user"
    blob = json.dumps([p.text for _, ps in t.values() for p in ps], ensure_ascii=False)
    assert "SECRETTOOLUSE" not in blob and "SECRETTOOLRESULT" not in blob


def test_subagent_transcripts_are_units_with_session_and_agent_in_title(corpus):
    s, _ = scans(corpus)
    t = parse_all(s["claude"])
    cand, pieces = t["-home-andrew-RustroverProjects-hftbacktest/s1/subagents/agent-abc.jsonl"]
    assert pieces[0].title == "s1/agent-abc · user"


def test_slug_alias_overrides_the_last_component(corpus):
    s, _ = scans(corpus)
    projects = {c.rel.split("/")[0]: c.project for c in s["claude"].candidates}
    assert projects["-home-andrew----------Telegram-Desktop-ChatExport-2026-09-26"] == "eacc-review"


def test_mirror_adds_only_what_is_missing_live(corpus, tmp_path):
    mirror = tmp_path / "mirror"
    gone = mirror / "-home-andrew-RustroverProjects-hftbacktest" / "old.jsonl"
    gone.parent.mkdir(parents=True)
    gone.write_text(json.dumps({"type": "user", "uuid": "o1", "timestamp": "2026-01-01T00:00:00Z",
                                "message": {"role": "user", "content": "очищено Claude Code"}}) + "\n")
    stale = mirror / "-home-andrew-RustroverProjects-hftbacktest" / "s1.jsonl"
    stale.write_text("{}\n")
    text = corpus["config"].read_text().replace(
        f'root = "{corpus["claude"]}"', f'root = "{corpus["claude"]}"\nretain_from = "{mirror}"')
    corpus["config"].write_text(text)
    s, _ = scans(corpus)
    by_rel = {c.rel: c for c in s["claude"].candidates}
    assert by_rel["-home-andrew-RustroverProjects-hftbacktest/old.jsonl"].abs_path == str(gone)
    assert by_rel["-home-andrew-RustroverProjects-hftbacktest/s1.jsonl"].abs_path == \
        str(corpus["slug"] / "s1.jsonl"), "the live copy wins"
    assert s["claude"].roots_ok


def test_missing_mirror_root_forbids_deletions_for_transcripts(corpus, tmp_path):
    text = corpus["config"].read_text().replace(
        f'root = "{corpus["claude"]}"', f'root = "{corpus["claude"]}"\nretain_from = "{tmp_path / "nope"}"')
    corpus["config"].write_text(text)
    s, _ = scans(corpus)
    assert not s["claude"].roots_ok
    assert any("nope" in m for m in s["claude"].missing)


def test_malformed_transcript_lines_are_counted(corpus):
    f = corpus["slug"] / "bad.jsonl"
    f.write_text("{not json\n" + json.dumps({"type": "user", "uuid": "z", "timestamp": "t",
                 "message": {"role": "user", "content": "survivor"}}) + "\n{\"cut\": ")
    s, _ = scans(corpus)
    c = next(c for c in s["claude"].candidates if c.rel.endswith("bad.jsonl"))
    pieces, stats = c.parse()
    assert [p.text for p in pieces] == ["survivor"]
    assert stats["malformed"] == 2


# --- memory -------------------------------------------------------------------

def test_memory_is_chunked_markdown_with_frontmatter_first(corpus):
    s, _ = scans(corpus)
    m = parse_all(s["claude-memory"])
    cand, pieces = m["-home-andrew-RustroverProjects-hftbacktest/memory/reference_kbindex.md"]
    assert cand.kind == "memory" and cand.project == "hftbacktest"
    assert pieces[0].text.startswith("---\nname: kbindex")


# --- repo docs, exports, docstrings ---------------------------------------------

def test_repo_takes_every_md_rst_under_docs_and_listed_docstrings(corpus):
    s, _ = scans(corpus)
    rels = {c.rel: c.kind for c in s["repo:hftbacktest"].candidates}
    assert rels["AGENTS.md"] == "doc"
    assert rels["docs/design-x.md"] == "doc"
    assert rels["src/README.md"] == "doc"
    assert rels["docs/api.rst"] == "doc"
    assert rels["collector/tools/commission_watch.py"] == "doc"
    assert rels["2026-08-12-115020-this-session.txt"] == "export"
    for absent in ("CLAUDE.md", "target/junk.md", "secret_notes.md", "big.md", "notes.rst",
                   ".claude/worktrees/agent-1/AGENTS.md"):
        assert absent not in rels, absent
    assert s["repo:hftbacktest"].skipped["secret_name"] == 1
    assert s["repo:hftbacktest"].skipped["too_big"] == 1


def test_export_parse_drops_nothing_but_noise(corpus):
    s, _ = scans(corpus)
    c = next(c for c in s["repo:hftbacktest"].candidates if c.kind == "export")
    pieces, _ = c.parse()
    assert [p.text for p in pieces] == ["вопрос оператора про троттл", "Ответ ассистента."]
    assert c.date.startswith("2026-08-12T")


def test_docstring_unit_parses_without_import(corpus):
    s, _ = scans(corpus)
    c = next(c for c in s["repo:hftbacktest"].candidates if c.rel.endswith("commission_watch.py"))
    pieces, _ = c.parse()
    assert "commissionRate" in pieces[0].text and pieces[0].anchor == "docstring/p1"


# --- notes ----------------------------------------------------------------------

def test_notes_rules(corpus):
    s, _ = scans(corpus)
    rels = {c.rel: c for c in s["hft-research"].candidates}
    assert "lighter-race-20260921/RESULT.md" in rels
    assert "lighter-race-20260921/raw/venue-doc.md" in rels, "md is taken even under raw/"
    assert "lighter-race-20260921/notes.txt" in rels
    assert "book-notes.md" in rels
    for absent in ("lighter-race-20260921/raw/dump.txt", "lighter-race-20260921/requirements.txt",
                   "lighter-race-20260921/tiny.txt", "cryptology10-review/pdf/13.md",
                   "prereg-sub/.git/HEAD"):
        assert absent not in rels, absent
    assert rels["lighter-race-20260921/RESULT.md"].project == "lighter-race-20260921"
    assert rels["book-notes.md"].project == "hft-research"
    assert rels["prereg-sub/PREREG.md"].kind == "note", "a research subrepo is still notes"


# --- course ---------------------------------------------------------------------

def test_course_asr_takes_json_only_on_a_90s_grid(corpus):
    s, _ = scans(corpus)
    a = parse_all(s["course:cryptology10"])
    assert list(a) == ["13.Риск-менеджмент.json"]
    cand, pieces = a["13.Риск-менеджмент.json"]
    assert cand.kind == "course" and cand.project == "cryptology10"
    assert [p.anchor for p in pieces] == ["t0", "t90"]
    assert pieces[1].text == "фиксируем 50 процентов позиции"


def test_course_notes_are_their_own_source(corpus):
    s, _ = scans(corpus)
    n = parse_all(s["course:cryptology10-notes"])
    cand, pieces = n["pdf/13.md"]
    assert cand.kind == "course" and "0.035%" in pieces[0].text


# --- chats ----------------------------------------------------------------------

def test_chat_html_parse_joined_sender_br_entities_service_media(corpus):
    s, _ = scans(corpus)
    cand = s["chat:deribit-ru"].candidates[0]
    assert cand.kind == "chat" and cand.project == "deribit-ru" and cand.rel == ""
    pieces, stats = cand.parse()
    assert len(pieces) == 1
    p = pieces[0]
    assert p.text.splitlines() == ["23:07 Support: matching engine: 5/s на сабаккаунт",
                                   "burst 20 & больше",
                                   "23:08 Support: спасибо"]
    assert p.anchor == "m213023" and p.loc == "message213023..213024"
    assert p.file == "ChatExport_2026-09-14 (1)/messages.html"
    assert p.title == "Deribit RU · 2018-07-11"
    assert p.date == "2018-07-11T23:07:13Z"


def test_only_allowlisted_chats_are_sources_and_unknown_exports_are_reported(corpus):
    s, cfg = scans(corpus)
    chats = [k for k in s if k.startswith("chat:")]
    assert chats == ["chat:deribit-ru"]
    assert sources.unclassified_exports(cfg) == ["ChatExport_2026-10-01"]


def test_result_json_with_entity_lists_and_merge_newer_export_wins(corpus):
    tg = corpus["tg"]
    j = tg / "ChatExport_2026-09-20"
    j.mkdir()
    (j / "result.json").write_text(json.dumps({"name": "Deribit RU", "messages": [
        {"id": 213024, "type": "message", "date": "2018-07-12T01:08:00", "date_unixtime": "1531350480",
         "from": "Support", "text": ["исправлено: ", {"type": "bold", "text": "спасибо!"}]},
        {"id": 213026, "type": "service", "date_unixtime": "1531350500", "action": "pin_message"},
        {"id": 213027, "type": "message", "date_unixtime": "1531350540", "from": "Bob", "text": "новое"},
    ]}, ensure_ascii=False), encoding="utf-8")
    cfgp = corpus["config"]
    cfgp.write_text(cfgp.read_text().replace(
        'exports = ["ChatExport_2026-09-14 (1)"]', 'exports = ["ChatExport_2026-09-14 (1)", "ChatExport_2026-09-20"]'))
    s, _ = scans(corpus)
    pieces, _ = s["chat:deribit-ru"].candidates[0].parse()
    text = "\n".join(p.text for p in pieces)
    assert "исправлено: спасибо!" in text, "the later export wins for the same message id"
    assert "23:08 Support: спасибо\n" not in text + "\n"
    assert "Bob: новое" in text
    assert "pin_message" not in text


def test_missing_export_dir_of_an_allowed_chat_forbids_deletions(corpus):
    cfgp = corpus["config"]
    cfgp.write_text(cfgp.read_text().replace('"ChatExport_2026-09-14 (1)"]', '"ChatExport_2026-09-14 (1)", "Gone"]'))
    s, _ = scans(corpus)
    assert not s["chat:deribit-ru"].roots_ok


def test_chat_signature_tracks_every_export_file(corpus):
    s, _ = scans(corpus)
    sig1 = s["chat:deribit-ru"].candidates[0].signature
    write_html_export(corpus["tg"] / "ChatExport_2026-09-14 (1)", "Deribit RU",
                      [html_message(5, "12.07.2018 01:07:13 UTC+02:00", "x", sender="A")],
                      name="messages2.html")
    s, _ = scans(corpus)
    assert s["chat:deribit-ru"].candidates[0].signature != sig1


# --- git ------------------------------------------------------------------------

def test_git_repo_and_discovered_research_subrepos(corpus):
    s, _ = scans(corpus)
    repo = s["git:hftbacktest"].candidates[0]
    pieces, _ = repo.parse()
    assert repo.kind == "commit" and repo.rel == ""
    assert any("добавил коллектор" in p.text for p in pieces)
    assert all(len(p.anchor) == 40 and p.loc == p.anchor for p in pieces)
    sub = {c.rel: c for c in s["git:hft-research"].candidates}
    assert list(sub) == ["prereg-sub"]
    sp, _ = sub["prereg-sub"].parse()
    assert sp[0].text.startswith("prereg: заморозка")
    assert sub["prereg-sub"].project == "prereg-sub"


def test_git_signature_changes_with_a_new_commit(corpus):
    from conftest import git
    s, _ = scans(corpus)
    before = s["git:hftbacktest"].candidates[0].signature
    (corpus["repo"] / "new.txt").write_text("n")
    git(corpus["repo"], "add", "new.txt")
    git(corpus["repo"], "commit", "-q", "-m", "second")
    s, _ = scans(corpus)
    assert s["git:hftbacktest"].candidates[0].signature != before


def test_file_signature_is_size_and_mtime_ns(corpus):
    s, _ = scans(corpus)
    c = next(c for c in s["hft-research"].candidates if c.rel == "book-notes.md")
    st = os.stat(c.abs_path)
    assert c.signature == [st.st_size, st.st_mtime_ns]
    assert c.files == (c.abs_path,)


def test_missing_root_is_reported_not_silent(corpus):
    import shutil
    shutil.rmtree(corpus["asr"])
    s, _ = scans(corpus)
    assert not s["course:cryptology10"].roots_ok
    assert s["course:cryptology10"].candidates == []
