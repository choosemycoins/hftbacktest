"""kb.toml: parsing, defaults, fail-closed on typos."""

import tomllib
from pathlib import Path

import pytest

from kb import config as cfgmod

EXAMPLE = Path(__file__).resolve().parents[1] / "kb.example.toml"


def test_example_config_is_valid_toml_and_matches_the_builtin_defaults():
    raw = tomllib.loads(EXAMPLE.read_text(encoding="utf-8"))
    assert raw["chats"]["allow"] == [], "the example in the repo never names a chat"
    from_file = cfgmod.load_config(EXAMPLE)
    builtin = cfgmod.builtin_config()
    assert from_file.sources_repr() == builtin.sources_repr()


def test_paths_are_expanded_and_aliases_loaded(corpus):
    cfg = cfgmod.load_config(corpus["config"])
    assert cfg.claude_root == corpus["claude"]
    assert cfg.aliases["-home-andrew----------Telegram-Desktop-ChatExport-2026-09-26"] == "eacc-review"
    assert [r.name for r in cfg.repos] == ["hftbacktest"]
    assert cfg.chats_allow[0].id == "deribit-ru"
    assert cfg.chats_allow[0].exports == ("ChatExport_2026-09-14 (1)",)
    assert cfg.model_active == "fake"


def test_unknown_key_is_an_error_not_a_silently_dropped_source(tmp_path):
    p = tmp_path / "kb.toml"
    p.write_text('[[repo]]\nname = "x"\npath = "/x"\ndocstirng_dirs = []\n')
    with pytest.raises(cfgmod.ConfigError, match="docstirng_dirs"):
        cfgmod.load_config(p)


def test_a_chat_both_allowed_and_ignored_is_an_error(tmp_path):
    p = tmp_path / "kb.toml"
    p.write_text('[chats]\nexports_root = "/t"\nignore = ["E1"]\n'
                 'allow = [{ id = "c", exports = ["E1"] }]\n')
    with pytest.raises(cfgmod.ConfigError, match="E1"):
        cfgmod.load_config(p)


def test_duplicate_chat_ids_are_an_error(tmp_path):
    p = tmp_path / "kb.toml"
    p.write_text('[chats]\nexports_root = "/t"\n'
                 'allow = [{ id = "c", exports = ["E1"] }, { id = "c", exports = ["E2"] }]\n')
    with pytest.raises(cfgmod.ConfigError, match="duplicate"):
        cfgmod.load_config(p)


def test_config_sha_changes_with_content(tmp_path):
    p = tmp_path / "kb.toml"
    p.write_text('db = "/a/kb.db"\n')
    a = cfgmod.load_config(p).sha
    p.write_text('db = "/b/kb.db"\n')
    assert cfgmod.load_config(p).sha != a
