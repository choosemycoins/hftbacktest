"""tools/kbindex.py is a shim: v1 commands -> kb commands, v1 query semantics."""

import importlib.util
from pathlib import Path

import pytest

SHIM = Path(__file__).resolve().parents[2] / "kbindex.py"


@pytest.fixture(scope="module")
def shim():
    spec = importlib.util.spec_from_file_location("kbindex_shim", SHIM)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_build_maps_to_update(shim):
    assert shim.translate(["build"]) == ["update"]
    assert shim.translate(["build", "--db", "/x/kb.db"]) == ["--db", "/x/kb.db", "update"]


def test_search_keeps_v1_raw_fts_semantics(shim):
    assert shim.translate(["search", "теря*", "--kind", "memory", "--limit", "5"]) == \
        ["search", "raw:теря*", "--mode", "fts", "--kind", "memory", "--limit", "5"]
    assert shim.translate(["search", '"szDecimals tick"', "--db", "/d.db"]) == \
        ["--db", "/d.db", "search", 'raw:"szDecimals tick"', "--mode", "fts"]


def test_removed_options_and_unknown_commands_fail_loudly(shim):
    with pytest.raises(SystemExit, match="kb.toml"):
        shim.translate(["build", "--add-root", "claude=/x"])
    with pytest.raises(SystemExit):
        shim.translate(["frobnicate"])
