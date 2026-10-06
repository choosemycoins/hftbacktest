"""kb.toml — where to look. Fail closed: an unknown key is an error, not a
silently dropped source."""

from __future__ import annotations

import hashlib
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

HOME = Path.home()
DEFAULT_CONFIG_PATH = HOME / ".config" / "kb" / "kb.toml"
DEFAULT_DB = HOME / ".local" / "share" / "kbindex" / "kb.db"
DEFAULT_HF_HOME = Path("/home/storage/hft-scratch/hf-cache")


class ConfigError(Exception):
    pass


@dataclass(frozen=True)
class RepoCfg:
    name: str
    path: Path
    docstring_dirs: tuple[str, ...] = ()


@dataclass(frozen=True)
class CourseCfg:
    id: str
    asr: Path | None = None
    notes: Path | None = None
    videos: Path | None = None


@dataclass(frozen=True)
class ChatAllow:
    id: str
    exports: tuple[str, ...]


@dataclass(frozen=True)
class Config:
    db: Path = DEFAULT_DB
    model_active: str | None = None
    hf_home: Path = DEFAULT_HF_HOME
    aliases: dict = field(default_factory=dict)
    claude_root: Path | None = None
    retain_from: Path | None = None
    repos: tuple[RepoCfg, ...] = ()
    research_root: Path | None = None
    research_exclude: tuple[str, ...] = ()
    git_discover: str | None = None
    courses: tuple[CourseCfg, ...] = ()
    chats_root: Path | None = None
    chats_ignore: tuple[str, ...] = ()
    chats_allow: tuple[ChatAllow, ...] = ()
    path: Path | None = None
    sha: str = ""

    def sources_repr(self):
        """Everything that decides what gets indexed (not where the file was)."""
        return (str(self.db), self.model_active, str(self.hf_home), sorted(self.aliases.items()),
                self.claude_root, self.retain_from, self.repos, self.research_root,
                self.research_exclude, self.git_discover, self.courses, self.chats_root,
                self.chats_ignore, self.chats_allow)


_TOP = {"db", "models", "projects", "claude", "repo", "research", "course", "chats"}
_KEYS = {
    "models": {"active", "hf_home"},
    "projects": {"alias"},
    "claude": {"root", "retain_from"},
    "repo": {"name", "path", "docstring_dirs"},
    "research": {"root", "exclude", "git_discover"},
    "course": {"id", "asr", "notes", "videos"},
    "chats": {"exports_root", "ignore", "allow"},
    "chats.allow": {"id", "exports"},
}


def _path(value) -> Path | None:
    return None if value in (None, "") else Path(str(value)).expanduser()


def _check(table: dict, allowed: set[str], where: str):
    unknown = sorted(set(table) - allowed)
    if unknown:
        raise ConfigError(f"{where}: unknown key(s) {', '.join(unknown)}")


def parse_config(raw: dict, path: Path | None = None, sha: str = "") -> Config:
    _check(raw, _TOP, "kb.toml")
    models = raw.get("models", {})
    _check(models, _KEYS["models"], "[models]")
    projects = raw.get("projects", {})
    _check(projects, _KEYS["projects"], "[projects]")
    claude = raw.get("claude", {})
    _check(claude, _KEYS["claude"], "[claude]")
    repos = []
    for r in raw.get("repo", []):
        _check(r, _KEYS["repo"], "[[repo]]")
        if "name" not in r or "path" not in r:
            raise ConfigError("[[repo]] needs name and path")
        repos.append(RepoCfg(str(r["name"]), _path(r["path"]),
                             tuple(str(d) for d in r.get("docstring_dirs", []))))
    if len({r.name for r in repos}) != len(repos):
        raise ConfigError("[[repo]]: duplicate name")
    research = raw.get("research", {})
    _check(research, _KEYS["research"], "[research]")
    courses = []
    for c in raw.get("course", []):
        _check(c, _KEYS["course"], "[[course]]")
        if "id" not in c:
            raise ConfigError("[[course]] needs id")
        courses.append(CourseCfg(str(c["id"]), _path(c.get("asr")), _path(c.get("notes")),
                                 _path(c.get("videos"))))
    chats = raw.get("chats", {})
    _check(chats, _KEYS["chats"], "[chats]")
    allow = []
    for a in chats.get("allow", []):
        _check(a, _KEYS["chats.allow"], "[chats] allow entry")
        if "id" not in a or not a.get("exports"):
            raise ConfigError("[chats] allow entry needs id and exports")
        allow.append(ChatAllow(str(a["id"]), tuple(str(e) for e in a["exports"])))
    ids = [a.id for a in allow]
    if len(set(ids)) != len(ids):
        raise ConfigError("[chats] allow: duplicate chat id")
    ignore = tuple(str(e) for e in chats.get("ignore", []))
    both = sorted({e for a in allow for e in a.exports} & set(ignore))
    if both:
        raise ConfigError(f"[chats]: exports both allowed and ignored: {', '.join(both)}")
    exports_seen = [e for a in allow for e in a.exports]
    if len(set(exports_seen)) != len(exports_seen):
        raise ConfigError("[chats] allow: an export is listed under two chats")
    return Config(
        db=_path(raw.get("db")) or DEFAULT_DB,
        model_active=models.get("active"),
        hf_home=_path(models.get("hf_home")) or DEFAULT_HF_HOME,
        aliases=dict(projects.get("alias", {})),
        claude_root=_path(claude.get("root")),
        retain_from=_path(claude.get("retain_from")),
        repos=tuple(repos),
        research_root=_path(research.get("root")),
        research_exclude=tuple(str(x) for x in research.get("exclude", [])),
        git_discover=(str(Path(research["git_discover"]).expanduser())
                      if research.get("git_discover") else None),
        courses=tuple(courses),
        chats_root=_path(chats.get("exports_root")),
        chats_ignore=ignore,
        chats_allow=tuple(allow),
        path=path,
        sha=sha,
    )


BUILTIN_TOML = """
db = "~/.local/share/kbindex/kb.db"
[models]
active = "e5b"
hf_home = "/home/storage/hft-scratch/hf-cache"
[projects.alias]
"-home-andrew----------Telegram-Desktop-ChatExport-2026-09-26" = "eacc-review"
[claude]
root = "~/.claude/projects"
retain_from = "/home/andrew/storage_2tb/mirror/.claude/projects"
[[repo]]
name = "hftbacktest"
path = "~/RustroverProjects/hftbacktest"
docstring_dirs = ["collector/tools", "collector", "docs", "."]
[[repo]]
name = "myhft"
path = "~/RustroverProjects/myhft"
docstring_dirs = ["scripts", "docs", "."]
[[repo]]
name = "eventshort"
path = "~/RustroverProjects/eventshort"
docstring_dirs = []
[[repo]]
name = "jansen"
path = "~/PycharmProjects/jansen"
docstring_dirs = ["studies", "."]
[research]
root = "~/hft-research"
exclude = ["cryptology10-review/**"]
git_discover = "~/hft-research/*/.git"
[[course]]
id = "cryptology10"
asr = "/home/storage/hft-scratch/cryptology10/transcripts"
notes = "~/hft-research/cryptology10-review"
videos = "/home/storage/torrent/[Cryptology School] Cryptology 10 (2023)"
[[course]]
id = "workshop6"
asr = "/home/storage/hft-scratch/workshop6/transcripts"
videos = "/home/storage/torrent/[Cryptology School] Cryptology Work Shop v6.0 (2024)"
[chats]
exports_root = "~/Загрузки/Telegram Desktop"
ignore = []
allow = []
"""


def builtin_config() -> Config:
    data = BUILTIN_TOML.encode("utf-8")
    return parse_config(tomllib.loads(BUILTIN_TOML), None, hashlib.sha256(data).hexdigest())


def load_config(path: Path | str | None = None) -> Config:
    """`path` None -> ~/.config/kb/kb.toml, or the built-in default if absent."""
    if path is None:
        if not DEFAULT_CONFIG_PATH.is_file():
            return builtin_config()
        path = DEFAULT_CONFIG_PATH
    path = Path(path).expanduser()
    try:
        data = path.read_bytes()
    except OSError as exc:
        raise ConfigError(f"cannot read {path}: {exc}") from exc
    try:
        raw = tomllib.loads(data.decode("utf-8"))
    except (tomllib.TOMLDecodeError, UnicodeDecodeError) as exc:
        raise ConfigError(f"{path}: {exc}") from exc
    return parse_config(raw, path, hashlib.sha256(data).hexdigest())
