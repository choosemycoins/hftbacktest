"""Shared synthetic fixtures. No test touches the real ~/.claude, repos,
Telegram exports, models or the real database."""

import json
import os
import subprocess
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def offline_netns(tmp_path_factory, monkeypatch):
    """Tests never depend on the host's network: the offline self-check reads
    a /proc/net/dev that lists only `lo`."""
    p = tmp_path_factory.getbasetemp() / "net_dev_lo"
    if not p.exists():
        p.write_text("Inter-|   Receive\n face |bytes\n    lo: 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0\n")
    monkeypatch.setenv("KB_PROC_NET_DEV", str(p))


def jline(obj) -> str:
    return json.dumps(obj, ensure_ascii=False) + "\n"


def git(repo: Path, *args):
    env = dict(os.environ, GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@e",
               GIT_COMMITTER_NAME="t", GIT_COMMITTER_EMAIL="t@e",
               GIT_AUTHOR_DATE="2026-09-30T10:00:00+00:00",
               GIT_COMMITTER_DATE="2026-09-30T10:00:00+00:00")
    subprocess.run(["git", *args], cwd=repo, env=env, check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def make_repo(root: Path, name: str, files: dict[str, str], message="init") -> Path:
    repo = root / name
    repo.mkdir(parents=True)
    git(repo, "init", "-q")
    for rel, text in files.items():
        p = repo / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text, encoding="utf-8")
    git(repo, "add", "-A")
    git(repo, "commit", "-q", "-m", message)
    return repo


HTML_HEAD = """<!DOCTYPE html>
<html><head><meta charset="utf-8"/><title>Exported Data</title></head>
<body><div class="page_wrap"><div class="page_header"><div class="content">
<div class="text bold">
{title}
</div></div></div>
<div class="page_body chat_page"><div class="history">
"""
HTML_TAIL = """
</div></div></div></body></html>
"""


def html_message(mid, date_title, text=None, sender=None, joined=False, media=False):
    cls = "message default clearfix joined" if joined else "message default clearfix"
    parts = [f'<div class="{cls}" id="message{mid}">', '<div class="body">',
             f'<div class="pull_right date details" title="{date_title}">\n00:00\n</div>']
    if sender:
        parts.append(f'<div class="from_name">\n{sender} \n</div>')
    if media:
        parts.append('<div class="media_wrap clearfix"><a class="photo_wrap clearfix pull_left" href="p.jpg"></a></div>')
    if text is not None:
        parts.append(f'<div class="text">\n{text}\n</div>')
    parts.append("</div>\n</div>")
    return "\n".join(parts)


def html_service(mid, text):
    return f'<div class="message service" id="message{mid}">\n<div class="body details">\n{text}\n</div>\n</div>'


def write_html_export(d: Path, title: str, messages: list[str], name="messages.html"):
    d.mkdir(parents=True, exist_ok=True)
    (d / name).write_text(HTML_HEAD.format(title=title) + "\n".join(messages) + HTML_TAIL,
                          encoding="utf-8")


@pytest.fixture()
def corpus(tmp_path: Path):
    """A small home directory with one instance of every source kind."""
    home = tmp_path / "home"
    claude = home / ".claude" / "projects"
    slug = claude / "-home-andrew-RustroverProjects-hftbacktest"
    slug.mkdir(parents=True)
    (slug / "s1.jsonl").write_text(
        jline({"type": "user", "uuid": "u1", "timestamp": "2026-08-09T10:00:00.123Z",
               "message": {"role": "user", "content": "почему прод терял деньги на комиссиях?"}})
        + jline({"type": "assistant", "uuid": "u2", "timestamp": "2026-08-09T10:01:00Z",
                 "message": {"role": "assistant", "content": [
                     {"type": "tool_use", "name": "Bash", "input": {"command": "SECRETTOOLUSE"}},
                     {"type": "text", "text": "Lighter монетизирует латентность, а не спред."}]}})
        + jline({"type": "user", "uuid": "u3", "timestamp": "2026-08-09T10:02:00Z",
                 "message": {"role": "user", "content": [
                     {"type": "tool_result", "content": "SECRETTOOLRESULT"}]}}),
        encoding="utf-8")
    sub = slug / "s1" / "subagents"
    sub.mkdir(parents=True)
    (sub / "agent-abc.jsonl").write_text(
        jline({"type": "user", "uuid": "u9", "timestamp": "2026-08-10T00:00:00Z",
               "message": {"role": "user", "content": "subagent prompt about szDecimals"}}),
        encoding="utf-8")
    mem = slug / "memory"
    mem.mkdir()
    (mem / "reference_kbindex.md").write_text(
        "---\nname: kbindex\ndescription: полнотекстовый поиск\n---\n\n# kbindex\n\nТроттл, а не комиссия.\n",
        encoding="utf-8")
    eacc = claude / "-home-andrew----------Telegram-Desktop-ChatExport-2026-09-26"
    eacc.mkdir()
    (eacc / "s.jsonl").write_text(
        jline({"type": "user", "uuid": "e1", "timestamp": "2026-09-26T00:00:00Z",
               "message": {"role": "user", "content": "разбор прогнозов e/acc"}}), encoding="utf-8")

    repos = home / "RustroverProjects"
    repo = make_repo(repos, "hftbacktest", {
        "AGENTS.md": "# AGENTS\n\n## Ловушки\n\nКоллектор ничего не мержит.\n",
        "docs/design-x.md": "# Design X\n\nThe l2Book truncation trap.\n",
        "docs/api.rst": "API reference\n=============\n\nrst body text\n",
        "collector/tools/commission_watch.py": '"""Watches commissionRate of Binance USDC."""\n',
        "notes.rst": "root rst is not a doc\n",
        "src/README.md": "nested readme\n",
    }, message="feat: добавил коллектор")
    (repo / "CLAUDE.md").symlink_to(repo / "AGENTS.md")
    (repo / "target").mkdir()
    (repo / "target" / "junk.md").write_text("build output md\n")
    (repo / "secret_notes.md").write_text("should never be read\n")
    (repo / "big.md").write_text("x" * (2 * 1024 * 1024 + 1))
    nested = repo / ".claude" / "worktrees" / "agent-1"
    nested.mkdir(parents=True)
    (nested / ".git").write_text("gitdir: /elsewhere\n")
    (nested / "AGENTS.md").write_text("worktree copy\n")
    (repo / "2026-08-12-115020-this-session.txt").write_text(
        "❯ вопрос оператора про троттл\n\n● Ответ ассистента.\n", encoding="utf-8")

    research = home / "hft-research"
    (research / "lighter-race-20260921" / "raw").mkdir(parents=True)
    (research / "lighter-race-20260921" / "RESULT.md").write_text(
        "# Result\n\nТейкер −1.86 бп после задержки.\n", encoding="utf-8")
    (research / "lighter-race-20260921" / "raw" / "venue-doc.md").write_text(
        "# Venue doc copy\n\nraw md is indexed even under raw/, data/ and out/\n", encoding="utf-8")
    (research / "lighter-race-20260921" / "raw" / "dump.txt").write_text("raw txt is excluded " * 5)
    (research / "lighter-race-20260921" / "notes.txt").write_text("заметка про очередь " * 5, encoding="utf-8")
    (research / "lighter-race-20260921" / "requirements.txt").write_text("numpy==2.0\n" * 10)
    (research / "lighter-race-20260921" / "tiny.txt").write_text("short")
    (research / "book-notes.md").write_text("# Book\n\nanytime-valid methods are absent.\n")
    (research / "cryptology10-review" / "pdf").mkdir(parents=True)
    (research / "cryptology10-review" / "pdf" / "13.md").write_text(
        "# Риск-менеджмент\n\nКомиссия WhiteBIT 0.035%.\n", encoding="utf-8")
    sub_repo = make_repo(research, "prereg-sub", {"PREREG.md": "# Prereg\n\nfrozen before any count, sha256 recorded\n"},
                         message="prereg: заморозка")

    asr = home / "storage" / "cryptology10" / "transcripts"
    asr.mkdir(parents=True)
    (asr / "13.Риск-менеджмент.json").write_text(json.dumps({
        "file": "13.mp4", "model": "medium", "device": "cpu", "duration": 200.0, "secs": 10,
        "segments": [{"s": 1.0, "e": 5.0, "t": "рискуем двумя процентами"},
                     {"s": 95.0, "e": 99.0, "t": "фиксируем 50 процентов позиции"}]},
        ensure_ascii=False), encoding="utf-8")
    (asr / "13.Риск-менеджмент.txt").write_text("duplicate of json")
    (asr / "14.x.lock").write_text("lock")
    (asr / "15.y.json.tmp").write_text("{")

    tg = home / "Downloads" / "Telegram Desktop"
    write_html_export(tg / "ChatExport_2026-09-14 (1)", "Deribit RU", [
        html_service(-1, "12 July 2018"),
        html_message(213023, "12.07.2018 01:07:13 UTC+02:00",
                     "matching engine: 5/s на сабаккаунт<br>burst 20 &amp; больше", sender="Support"),
        html_message(213024, "12.07.2018 01:08:00 UTC+02:00", "спасибо", joined=True),
        html_message(213025, "12.07.2018 01:09:00 UTC+02:00", None, sender="Bob", media=True),
    ])
    write_html_export(tg / "ChatExport_2023-11-01", "Private dialog", [
        html_message(1, "01.11.2023 10:00:00 UTC+02:00", "личное", sender="Friend")])
    write_html_export(tg / "ChatExport_2026-10-01", "Unknown new", [
        html_message(1, "01.10.2026 10:00:00 UTC+02:00", "новый экспорт", sender="X")])

    config = home / ".config" / "kb" / "kb.toml"
    config.parent.mkdir(parents=True)
    config.write_text(f'''
db = "{tmp_path / 'share' / 'kbindex' / 'kb.db'}"

[models]
active = "fake"
hf_home = "{tmp_path / 'hf'}"

[projects.alias]
"-home-andrew----------Telegram-Desktop-ChatExport-2026-09-26" = "eacc-review"

[claude]
root = "{claude}"

[[repo]]
name = "hftbacktest"
path = "{repo}"
docstring_dirs = ["collector/tools"]

[research]
root = "{research}"
exclude = ["cryptology10-review/**"]
git_discover = "{research}/*/.git"

[[course]]
id = "cryptology10"
asr = "{asr}"
notes = "{research / 'cryptology10-review'}"

[chats]
exports_root = "{tg}"
ignore = ["ChatExport_2023-11-01"]
allow = [{{ id = "deribit-ru", exports = ["ChatExport_2026-09-14 (1)"] }}]
''', encoding="utf-8")
    return {"home": home, "claude": claude, "slug": slug, "repo": repo, "research": research,
            "asr": asr, "tg": tg, "config": config, "sub_repo": sub_repo,
            "db": tmp_path / "share" / "kbindex" / "kb.db", "tmp": tmp_path}
