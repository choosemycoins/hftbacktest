"""Tests for kbindex — extractors, FTS round-trip, build safety.

Everything runs on synthetic fixtures in tmp_path; no test touches the real
~/.claude, the real repos, or the real database.
"""

import json
import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import kbindex  # noqa: E402


# --------------------------------------------------------------------------
# fixtures
# --------------------------------------------------------------------------

def _line(obj):
    return json.dumps(obj, ensure_ascii=False) + "\n"


USER_TEXT = "почему прод терял деньги в июне?"
ASSISTANT_TEXT = "Разбор: adverse selection плюс комиссии, funding ни при чём."


def write_transcript(path: Path) -> None:
    """A transcript with real content plus every flavour of noise we exclude."""
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        # session bookkeeping, not a message
        _line({"type": "mode", "mode": "normal"}),
        # meta / local-command noise
        _line({
            "type": "user", "isMeta": True, "timestamp": "2026-06-01T10:00:00Z",
            "message": {"role": "user", "content": "<local-command-caveat>Caveat: ...</local-command-caveat>"},
        }),
        _line({
            "type": "user", "timestamp": "2026-06-01T10:00:01Z",
            "message": {"role": "user", "content": "<command-name>/login</command-name>\n<command-args></command-args>"},
        }),
        _line({
            "type": "user", "timestamp": "2026-06-01T10:00:02Z",
            "message": {"role": "user", "content": "<local-command-stdout>Login successful</local-command-stdout>"},
        }),
        # a real user message
        _line({
            "type": "user", "timestamp": "2026-06-01T10:01:00Z",
            "message": {"role": "user", "content": USER_TEXT},
        }),
        # tool results must never be indexed
        _line({
            "type": "user", "timestamp": "2026-06-01T10:01:05Z",
            "message": {"role": "user", "content": [
                {"type": "tool_result", "content": "SECRETTOOLRESULT rows=42"},
            ]},
        }),
        # assistant: thinking + tool_use are skipped, text is kept
        _line({
            "type": "assistant", "timestamp": "2026-06-01T10:02:00Z",
            "message": {"role": "assistant", "content": [
                {"type": "thinking", "thinking": "SECRETTHINKING let me grep"},
                {"type": "tool_use", "name": "Bash", "input": {"command": "SECRETTOOLUSE ls"}},
                {"type": "text", "text": ASSISTANT_TEXT},
            ]},
        }),
        # a pasted data URI inside otherwise real text
        _line({
            "type": "assistant", "timestamp": "2026-06-01T10:03:00Z",
            "message": {"role": "assistant", "content": [
                {"type": "text", "text": "chart: data:image/png;base64,QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVowMTIzNDU2Nzg5 done"},
            ]},
        }),
        # image part in a user message
        _line({
            "type": "user", "timestamp": "2026-06-01T10:04:00Z",
            "message": {"role": "user", "content": [
                {"type": "image", "source": {"type": "base64", "data": "SECRETIMAGEBLOB"}},
            ]},
        }),
        # attachments / task notifications are machine noise
        _line({"type": "attachment", "attachment": {"type": "skill_listing", "content": "SECRETSKILLLISTING"}}),
        _line({
            "type": "user", "timestamp": "2026-06-01T10:05:00Z",
            "message": {"role": "user", "content": "<task-notification>\n<task-id>abc</task-id>\n<summary>szDecimals tick fix verified</summary>\n</task-notification>"},
        }),
        # empty message
        _line({"type": "user", "timestamp": "2026-06-01T10:06:00Z", "message": {"role": "user", "content": ""}}),
    ]
    path.write_text("".join(lines), encoding="utf-8")


@pytest.fixture()
def claude_root(tmp_path: Path) -> Path:
    """A fake ~/.claude/projects with two slugs, sessions and memory."""
    root = tmp_path / "projects"
    slug = root / "-home-andrew-RustroverProjects-myhft"
    write_transcript(slug / "0cafbaca-be02-4dcd-b784-e1ec09287e87.jsonl")
    # a nested subagent transcript
    write_transcript(slug / "0cafbaca" / "subagents" / "agent-deadbeef.jsonl")
    mem = slug / "memory"
    mem.mkdir(parents=True, exist_ok=True)
    (mem / "live-loss-mechanism.md").write_text(
        "# live loss\n\nAdverse selection dominated; funding negligible.\n", encoding="utf-8"
    )
    other = root / "-home-andrew-PycharmProjects-jansen"
    other.mkdir(parents=True, exist_ok=True)
    (other / "s1.jsonl").write_text(
        _line({"type": "user", "timestamp": "2026-05-05T00:00:00Z",
               "message": {"role": "user", "content": "unlock tail correlation study"}}),
        encoding="utf-8",
    )
    return root


@pytest.fixture()
def git_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "tinyrepo"
    repo.mkdir()
    env = dict(os.environ, GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@e",
               GIT_COMMITTER_NAME="t", GIT_COMMITTER_EMAIL="t@e")
    run = lambda *a: subprocess.run(a, cwd=repo, env=env, check=True,
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    run("git", "init", "-q")
    (repo / "a.txt").write_text("a\n")
    run("git", "add", ".")
    run("git", "commit", "-q", "-m", "fix: szDecimals tick rounding",
        "-m", "Body spans\nmultiple lines with tick detail.")
    (repo / "b.txt").write_text("b\n")
    run("git", "add", ".")
    run("git", "commit", "-q", "-m", "feat: добавил коллектор")
    return repo


# --------------------------------------------------------------------------
# message extraction
# --------------------------------------------------------------------------

def test_user_string_message_is_extracted():
    obj = {"type": "user", "message": {"role": "user", "content": USER_TEXT}}
    assert kbindex.extract_message_text(obj) == USER_TEXT


def test_assistant_text_parts_only():
    obj = {"type": "assistant", "message": {"role": "assistant", "content": [
        {"type": "thinking", "thinking": "NOPE"},
        {"type": "tool_use", "name": "Bash", "input": {"command": "NOPE"}},
        {"type": "text", "text": "kept one"},
        {"type": "text", "text": "kept two"},
    ]}}
    out = kbindex.extract_message_text(obj)
    assert "kept one" in out and "kept two" in out
    assert "NOPE" not in out


def test_tool_result_and_image_parts_are_skipped():
    tool = {"type": "user", "message": {"role": "user", "content": [
        {"type": "tool_result", "content": "NOPE"}]}}
    img = {"type": "user", "message": {"role": "user", "content": [
        {"type": "image", "source": {"type": "base64", "data": "NOPE"}}]}}
    assert kbindex.extract_message_text(tool) is None
    assert kbindex.extract_message_text(img) is None


def test_non_message_lines_are_skipped():
    for obj in ({"type": "mode", "mode": "normal"},
                {"type": "attachment", "attachment": {"content": "NOPE"}},
                {"type": "file-history-snapshot", "snapshot": {}},
                {"type": "system", "content": "NOPE"}):
        assert kbindex.extract_message_text(obj) is None


def test_meta_and_local_command_noise_is_dropped():
    meta = {"type": "user", "isMeta": True,
            "message": {"role": "user", "content": "<local-command-caveat>x</local-command-caveat>"}}
    cmd = {"type": "user", "message": {"role": "user",
           "content": "<command-name>/login</command-name>\n<command-args></command-args>"}}
    out = {"type": "user", "message": {"role": "user",
           "content": "<local-command-stdout>Login successful</local-command-stdout>"}}
    empty = {"type": "user", "message": {"role": "user", "content": ""}}
    for obj in (meta, cmd, out, empty):
        assert kbindex.extract_message_text(obj) is None


def test_system_reminder_block_is_stripped_but_text_kept():
    obj = {"type": "user", "message": {"role": "user",
           "content": "real question here\n<system-reminder>NOPE internal</system-reminder>"}}
    got = kbindex.extract_message_text(obj)
    assert got == "real question here"


def test_task_notification_keeps_only_summary():
    obj = {"type": "user", "message": {"role": "user", "content":
           "<task-notification>\n<task-id>abc</task-id>\n<output-file>/tmp/NOPE</output-file>"
           "\n<summary>szDecimals tick fix verified</summary>\n</task-notification>"}}
    got = kbindex.extract_message_text(obj)
    assert got == "szDecimals tick fix verified"
    bare = {"type": "user", "message": {"role": "user",
            "content": "<task-notification><task-id>abc</task-id></task-notification>"}}
    assert kbindex.extract_message_text(bare) is None


def test_base64_data_uri_is_dropped_from_text():
    obj = {"type": "assistant", "message": {"role": "assistant", "content": [
        {"type": "text", "text": "before data:image/png;base64,QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVo= after"}]}}
    got = kbindex.extract_message_text(obj)
    assert "QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVo" not in got
    assert "before" in got and "after" in got


# --------------------------------------------------------------------------
# transcript source
# --------------------------------------------------------------------------

def test_transcript_rows_carry_project_session_and_date(claude_root: Path):
    rows, stats = kbindex.collect_transcript_rows([claude_root])
    myhft = [r for r in rows if r.project == "myhft"]
    assert myhft, "project slug must be shortened to the trailing component"
    texts = "\n".join(r.text for r in myhft)
    assert USER_TEXT in texts
    assert ASSISTANT_TEXT in texts
    for marker in ("SECRETTOOLRESULT", "SECRETTHINKING", "SECRETTOOLUSE",
                   "SECRETIMAGEBLOB", "SECRETSKILLLISTING"):
        assert marker not in texts, marker
    r = next(r for r in myhft if r.text == USER_TEXT and "subagents" not in r.path)
    assert r.kind == "transcript"
    assert r.ref == "0cafbaca-be02-4dcd-b784-e1ec09287e87"
    assert r.date == "2026-06-01T10:01:00Z"
    assert r.path.endswith(".jsonl")
    assert {r.project for r in rows} == {"myhft", "jansen"}


def test_nested_subagent_transcripts_are_indexed(claude_root: Path):
    rows, _ = kbindex.collect_transcript_rows([claude_root])
    refs = {r.ref for r in rows}
    assert "agent-deadbeef" in refs, "subagent transcripts hold most of the discussion"


def test_malformed_lines_are_counted_and_skipped(tmp_path: Path):
    root = tmp_path / "projects"
    f = root / "-x-proj" / "s.jsonl"
    f.parent.mkdir(parents=True)
    f.write_text(
        "{not json at all\n"
        + _line({"type": "user", "timestamp": "t", "message": {"role": "user", "content": "survivor"}})
        + "\n"                      # blank line: ignored, not counted as malformed
        + "{\"unterminated\": \n",
        encoding="utf-8",
    )
    rows, stats = kbindex.collect_transcript_rows([root])
    assert [r.text for r in rows] == ["survivor"]
    assert stats["malformed_lines"] == 2
    assert stats["files"] == 1


# --------------------------------------------------------------------------
# memory source
# --------------------------------------------------------------------------

def test_memory_rows_use_mtime_and_full_text(claude_root: Path):
    rows, _ = kbindex.collect_memory_rows([claude_root])
    assert len(rows) == 1
    r = rows[0]
    assert r.kind == "memory"
    assert r.project == "myhft"
    assert r.ref == ""
    assert "Adverse selection" in r.text
    mtime = os.stat(r.path).st_mtime
    assert r.date.startswith(kbindex.iso_date(mtime)[:10])


# --------------------------------------------------------------------------
# docstring source
# --------------------------------------------------------------------------

def test_docstrings_are_read_without_executing(tmp_path: Path):
    d = tmp_path / "studies"
    d.mkdir()
    (d / "good.py").write_text('"""Study: unlock tail correlation.\n\nSecond line.\n"""\nX = 1\n')
    (d / "nodoc.py").write_text("X = 1\n")
    (d / "broken.py").write_text("def (:\n")
    (d / "boom.py").write_text('"""has doc"""\nraise SystemExit("MUST NOT RUN")\n')
    rows, stats = kbindex.collect_docstring_rows([d])
    by_name = {Path(r.path).name: r for r in rows}
    assert set(by_name) == {"good.py", "boom.py"}
    assert by_name["good.py"].kind == "doc"
    assert "unlock tail correlation" in by_name["good.py"].text
    assert stats["unparsable"] == 1


# --------------------------------------------------------------------------
# git source
# --------------------------------------------------------------------------

def test_docstring_project_is_the_owning_repo_not_the_parent_dir(git_repo: Path):
    nested = git_repo / "collector" / "tools"
    nested.mkdir(parents=True)
    (nested / "quality_report.py").write_text('"""Quality report for collected days."""\n')
    rows, _ = kbindex.collect_docstring_rows([nested])
    assert [r.project for r in rows] == ["tinyrepo"], \
        "docstrings must be attributed to the repo so --project matches commits"


def test_docstring_project_falls_back_to_parent_dir_outside_a_repo(tmp_path: Path):
    d = tmp_path / "jansen" / "studies"
    d.mkdir(parents=True)
    (d / "s.py").write_text('"""doc"""\n')
    rows, _ = kbindex.collect_docstring_rows([d])
    assert [r.project for r in rows] == ["jansen"]


def test_git_rows_include_subject_and_multiline_body(git_repo: Path):
    rows, stats = kbindex.collect_git_rows([git_repo])
    assert len(rows) == 2
    assert stats["repos"] == 1
    r = next(r for r in rows if "szDecimals" in r.text)
    assert r.kind == "commit"
    assert r.project == "tinyrepo"
    assert len(r.ref) == 40
    assert "multiple lines with tick detail." in r.text, "body must not be truncated at newlines"
    assert r.date.startswith("20")
    assert any("добавил коллектор" in r.text for r in rows)


def test_missing_git_repo_is_skipped_but_reported(tmp_path: Path):
    rows, stats = kbindex.collect_git_rows([tmp_path / "does-not-exist"])
    assert rows == []
    assert stats["repos"] == 0
    assert len(stats["missing"]) == 1, "a typo'd root must never look like a clean build"
    assert "does-not-exist" in stats["missing"][0]


def test_non_repo_directory_is_reported_as_missing(tmp_path: Path):
    plain = tmp_path / "not-a-repo"
    plain.mkdir()
    rows, stats = kbindex.collect_git_rows([plain])
    assert rows == []
    assert stats["missing"] and "not a git repository" in stats["missing"][0]


def test_path_inside_a_repo_indexes_the_enclosing_repo_with_a_note(git_repo: Path):
    inside = git_repo / "collector" / "tools"
    inside.mkdir(parents=True)
    rows, stats = kbindex.collect_git_rows([inside])
    assert stats["repos"] == 1
    assert stats["missing"] == []
    assert stats["notes"], "the substitution must be visible, not silent"
    assert {r.project for r in rows} == {"tinyrepo"}


def test_bare_repo_is_indexed(tmp_path: Path, git_repo: Path):
    bare = tmp_path / "mirror.git"
    subprocess.run(["git", "clone", "--bare", "-q", str(git_repo), str(bare)],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    rows, stats = kbindex.collect_git_rows([bare])
    assert stats["repos"] == 1 and stats["missing"] == []
    assert {r.project for r in rows} == {"mirror"}, "`.git` suffix is not a project name"


def test_missing_source_roots_are_reported_by_every_collector(tmp_path: Path):
    gone = tmp_path / "gone"
    _, t = kbindex.collect_transcript_rows([gone])
    _, m = kbindex.collect_memory_rows([gone])
    _, d = kbindex.collect_docstring_rows([gone])
    assert t["missing"] == m["missing"] == d["missing"] == [str(gone)]


# --------------------------------------------------------------------------
# build + search
# --------------------------------------------------------------------------

@pytest.fixture()
def built(tmp_path: Path, claude_root: Path, git_repo: Path):
    docs = tmp_path / "studies"
    docs.mkdir()
    (docs / "study.py").write_text('"""Study of the tick szDecimals rounding bug."""\n')
    cfg = kbindex.Config(claude_roots=[claude_root], doc_dirs=[docs], git_repos=[git_repo])
    db = tmp_path / "share" / "kbindex" / "kb.db"
    stats = kbindex.build(db, cfg)
    return db, cfg, stats


def test_build_reports_counts_per_source(built):
    db, _, stats = built
    assert stats["counts"]["transcript"] > 0
    assert stats["counts"]["memory"] == 1
    assert stats["counts"]["doc"] == 1
    assert stats["counts"]["commit"] == 2
    assert stats["elapsed"] >= 0
    assert stats["db_bytes"] > 0
    assert db.exists()


def test_fts_roundtrip_english(built):
    db, _, _ = built
    hits = kbindex.search(db, "adverse selection")
    assert hits
    assert any(h.kind == "memory" for h in hits)


def test_fts_roundtrip_russian_whole_word(built):
    db, _, _ = built
    hits = kbindex.search(db, "почему прод терял")
    assert hits, "unicode61 must tokenize Cyrillic"
    assert USER_TEXT in hits[0].text


def test_no_stemming_but_prefix_syntax_works(built):
    db, _, _ = built
    assert kbindex.search(db, "терял")
    assert not kbindex.search(db, "терять"), "unicode61 has no stemmer; document this"
    assert kbindex.search(db, "теря*")


def test_quoted_phrase_query_passthrough(built):
    db, _, _ = built
    assert kbindex.search(db, '"szDecimals tick rounding"')
    assert not kbindex.search(db, '"rounding szDecimals tick"')


def test_kind_and_project_and_limit_filters(built):
    db, _, _ = built
    assert all(h.kind == "commit" for h in kbindex.search(db, "tick", kind="commit"))
    assert all(h.project == "myhft" for h in kbindex.search(db, "loss OR терял", project="myhft"))
    assert len(kbindex.search(db, "tick OR терял OR selection", limit=1)) == 1


def test_metadata_columns_are_not_searchable_as_text(built):
    db, _, _ = built
    # a bare word that only appears in the kind column must not match everything
    assert not kbindex.search(db, "commit")


def test_hits_render_as_pointers_with_snippet(built):
    db, _, _ = built
    out = kbindex.format_hits(kbindex.search(db, "adverse selection"))
    assert "pointers, verify at source" in out
    lines = out.splitlines()
    body = [l for l in lines if "|" in l]
    assert body
    assert "memory | myhft |" in body[0]
    assert "#" in body[0]
    assert any("adverse" in l.lower() for l in lines)


# --------------------------------------------------------------------------
# safety: what search prints
# --------------------------------------------------------------------------

# Synthetic, never-used values shaped like the real thing.
FAKE_KEY = "0x" + "7a" * 32
FAKE_ADDR = "0x9e557fcE01eaF817515f5a3642192694a73b1E17"
FAKE_BARE_HEX = "b" * 64
FAKE_SHA = "4f37797443c95274b57f3cfc3c08c5b6c12f9d73"


@pytest.mark.parametrize("secret,text", [
    (FAKE_KEY, f"ключи API Wallet Name api1 {FAKE_KEY} public"),
    (FAKE_ADDR, f"адрес кошелька {FAKE_ADDR} привязан"),
    (FAKE_BARE_HEX, f"agent key {FAKE_BARE_HEX} rotated"),
    ("hunter2hunter2", "export API_KEY=hunter2hunter2"),
    ("s3cr3t-value-x", 'password: "s3cr3t-value-x"'),
    ("AKIAIOSFODNN7EXAMPLE", "creds AKIAIOSFODNN7EXAMPLE here"),
    ("xoxb-1234567890-abcdef", "slack xoxb-1234567890-abcdef posted"),
])
def test_redact_masks_secret_shapes(secret, text):
    out = kbindex.redact(text)
    assert secret not in out, out
    assert "redacted" in out


def test_redact_keeps_ordinary_text_and_git_shas_readable():
    text = f"commit {FAKE_SHA} fixed the szDecimals tick rounding on Bybit"
    assert kbindex.redact(text) == text


def test_search_output_masks_a_pasted_key(tmp_path, git_repo):
    root = tmp_path / "projects" / "-home-andrew-RustroverProjects-myhft"
    root.mkdir(parents=True)
    (root / "s.jsonl").write_text(_line({
        "type": "user", "timestamp": "2026-07-28T14:51:15Z",
        "message": {"role": "user",
                    "content": f"ключи API Wallet Address {FAKE_ADDR} {FAKE_KEY} готово"},
    }), encoding="utf-8")
    cfg = kbindex.Config(claude_roots=[tmp_path / "projects"], doc_dirs=[], git_repos=[])
    db = tmp_path / "share" / "kb.db"
    kbindex.build(db, cfg)
    hits = kbindex.search(db, '"API Wallet Address"')
    assert hits, "the row is still findable"
    out = kbindex.format_hits(hits, query="API Wallet Address")
    assert FAKE_KEY not in out
    assert FAKE_ADDR not in out
    assert "redacted" in out
    assert "keep this output local" in out, "the header must not read as safe-to-paste"


def test_hit_text_is_unredacted_for_callers_but_stdout_is_not(tmp_path, capsys):
    """Masking is a presentation rule; the API stays honest about what is stored."""
    root = tmp_path / "projects" / "-x-proj"
    root.mkdir(parents=True)
    (root / "s.jsonl").write_text(_line({
        "type": "user", "timestamp": "t",
        "message": {"role": "user", "content": f"token {FAKE_BARE_HEX} pasted"},
    }), encoding="utf-8")
    cfg = kbindex.Config(claude_roots=[tmp_path / "projects"], doc_dirs=[], git_repos=[])
    db = tmp_path / "share" / "kb.db"
    kbindex.build(db, cfg)
    hits = kbindex.search(db, "pasted")
    assert FAKE_BARE_HEX in hits[0].text
    kbindex.main(["search", "pasted", "--db", str(db)])
    assert FAKE_BARE_HEX not in capsys.readouterr().out


def test_bad_fts_query_is_a_clean_error(built):
    db, _, _ = built
    with pytest.raises(kbindex.QueryError):
        kbindex.search(db, 'unbalanced "quote')


# --------------------------------------------------------------------------
# safety: permissions and atomicity
# --------------------------------------------------------------------------

def test_db_is_0600_and_dir_0700(built):
    db, _, _ = built
    assert stat.S_IMODE(os.stat(db).st_mode) == 0o600
    assert stat.S_IMODE(os.stat(db.parent).st_mode) == 0o700


def test_permissions_are_reapplied_on_rebuild_of_our_own_dir(built):
    db, cfg, _ = built
    assert (db.parent / kbindex.DIR_MARKER).is_file(), "the dir we created is marked ours"
    os.chmod(db, 0o644)
    os.chmod(db.parent, 0o755)
    stats = kbindex.build(db, cfg)
    assert stats["dir_managed"] is True
    assert stat.S_IMODE(os.stat(db).st_mode) == 0o600
    assert stat.S_IMODE(os.stat(db.parent).st_mode) == 0o700


def test_unrelated_directory_mode_is_never_changed(tmp_path, claude_root):
    """--db ~/kb.db must not turn $HOME into 0700."""
    victim = tmp_path / "victim"
    victim.mkdir()
    (victim / "someone-elses-notes.txt").write_text("keep me readable\n")
    os.chmod(victim, 0o755)
    cfg = kbindex.Config(claude_roots=[claude_root], doc_dirs=[], git_repos=[])
    warnings = []
    stats = kbindex.build(victim / "kb.db", cfg, warn=warnings.append)
    assert stat.S_IMODE(os.stat(victim).st_mode) == 0o755, "mode of a dir we do not own"
    assert stats["dir_managed"] is False
    assert not (victim / kbindex.DIR_MARKER).exists()
    assert any(str(victim) in w for w in warnings), "silence would be the bug"
    assert stat.S_IMODE(os.stat(victim / "kb.db").st_mode) == 0o600, "db is 0600 regardless"


def test_existing_empty_directory_is_claimed_and_locked_down(tmp_path, claude_root):
    d = tmp_path / "empty"
    d.mkdir()
    os.chmod(d, 0o755)
    cfg = kbindex.Config(claude_roots=[claude_root], doc_dirs=[], git_repos=[])
    stats = kbindex.build(d / "kb.db", cfg)
    assert stats["dir_managed"] is True
    assert stat.S_IMODE(os.stat(d).st_mode) == 0o700
    assert (d / kbindex.DIR_MARKER).is_file()


def test_marker_removal_hands_the_directory_back(tmp_path, claude_root):
    d = tmp_path / "own"
    cfg = kbindex.Config(claude_roots=[claude_root], doc_dirs=[], git_repos=[])
    kbindex.build(d / "kb.db", cfg)
    (d / kbindex.DIR_MARKER).unlink()
    (d / "unrelated.txt").write_text("x")
    os.chmod(d, 0o755)
    stats = kbindex.build(d / "kb.db", cfg)
    assert stats["dir_managed"] is False
    assert stat.S_IMODE(os.stat(d).st_mode) == 0o755


def test_build_is_atomic_old_db_survives_failure(built, monkeypatch):
    db, cfg, _ = built
    before = db.read_bytes()

    def boom(*a, **kw):
        raise RuntimeError("source exploded")

    monkeypatch.setattr(kbindex, "collect_git_rows", boom)
    with pytest.raises(RuntimeError):
        kbindex.build(db, cfg)
    assert db.read_bytes() == before
    leftovers = [p.name for p in db.parent.iterdir()
                 if p.name not in (db.name, kbindex.DIR_MARKER)]
    assert leftovers == [], f"temp files left behind: {leftovers}"


def test_build_writes_through_a_temp_file_then_renames(tmp_path, claude_root, monkeypatch):
    cfg = kbindex.Config(claude_roots=[claude_root], doc_dirs=[], git_repos=[])
    db = tmp_path / "kb.db"
    seen = {}
    real_replace = os.replace

    def spy(src, dst):
        seen["src"] = str(src)
        seen["dst"] = str(dst)
        return real_replace(src, dst)

    monkeypatch.setattr(kbindex.os, "replace", spy)
    kbindex.build(db, cfg)
    assert seen["dst"] == str(db)
    assert seen["src"] != str(db)
    assert Path(seen["src"]).parent == db.parent, "rename must be same-filesystem"


def test_search_against_missing_db_is_a_clean_error(tmp_path):
    with pytest.raises(kbindex.QueryError):
        kbindex.search(tmp_path / "nope.db", "anything")


# --------------------------------------------------------------------------
# CLI surface
# --------------------------------------------------------------------------

def test_cli_search_prints_pointer_header(built, capsys):
    db, _, _ = built
    rc = kbindex.main(["search", "adverse selection", "--db", str(db)])
    assert rc == 0
    out = capsys.readouterr().out
    assert "pointers, verify at source" in out


def test_cli_build_prints_per_source_counts(tmp_path, claude_root, capsys, monkeypatch):
    monkeypatch.setattr(kbindex, "DEFAULT_CONFIG",
                        kbindex.Config(claude_roots=[claude_root], doc_dirs=[], git_repos=[]))
    db = tmp_path / "kb.db"
    rc = kbindex.main(["build", "--db", str(db)])
    assert rc == 0
    out = capsys.readouterr().out
    assert "transcript" in out and "memory" in out
    assert "elapsed" in out


def test_cli_add_root_extends_defaults(tmp_path, claude_root, capsys, monkeypatch):
    monkeypatch.setattr(kbindex, "DEFAULT_CONFIG",
                        kbindex.Config(claude_roots=[], doc_dirs=[], git_repos=[]))
    db = tmp_path / "kb.db"
    rc = kbindex.main(["build", "--db", str(db), "--add-root", f"claude={claude_root}"])
    assert rc == 0
    assert kbindex.search(db, "терял")


def test_cli_build_exits_nonzero_and_warns_on_a_missing_root(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr(kbindex, "DEFAULT_CONFIG",
                        kbindex.Config(claude_roots=[], doc_dirs=[], git_repos=[]))
    db = tmp_path / "kbdir" / "kb.db"
    rc = kbindex.main(["build", "--db", str(db),
                       "--add-root", f"docs={tmp_path / 'no-such-dir'}",
                       "--add-root", f"git={tmp_path / 'no-such-repo'}"])
    assert rc == 3, "a build that indexed nothing must not look healthy"
    err = capsys.readouterr().err
    assert "no-such-dir" in err and "no-such-repo" in err


def test_build_stats_list_every_missing_root(tmp_path):
    cfg = kbindex.Config(claude_roots=[tmp_path / "a"], doc_dirs=[tmp_path / "b"],
                         git_repos=[tmp_path / "c"])
    stats = kbindex.build(tmp_path / "kbdir" / "kb.db", cfg, warn=lambda m: None)
    # claude root counts twice: transcripts and memory
    assert len(stats["missing_roots"]) == 4


def test_help_documents_tokenizer_and_secrecy():
    text = kbindex.__doc__ or ""
    low = text.lower()
    assert "unicode61" in low
    assert "stem" in low
    assert "never" in low and "git" in low
    assert "0600" in text or "0o600" in text
    assert "0700" in text and "unrelated files" in low, "document the directory-mode rule"
    assert "mask" in low, "document that output is masked and masking is best-effort"


def test_help_documents_the_recall_limitations():
    low = (kbindex.__doc__ or "").lower()
    assert "session you are in right now is indexed" in low, \
        "self-matching current-session hits surprise every user once"
    assert "no local" in low and "transcript rows" in low, \
        "a project without transcripts must not read as a project without history"
