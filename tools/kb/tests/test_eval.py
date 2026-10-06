"""Golden-set metrics on a toy golden set over the synthetic corpus."""

import json

import numpy as np
import pytest

from kb import embed, evaluate, semantic, store, update
from kb import config as cfgmod
from kb.embed import FakeEmbedder
from kb.search import Index


@pytest.fixture()
def setup(corpus, tmp_path):
    cfg = cfgmod.load_config(corpus["config"])
    update.update(cfg, corpus["db"], warn=lambda m: None, lock_timeout=0)
    fe = FakeEmbedder(synonyms={"троттл": "throttle"})
    conn = store.open_rw(corpus["db"])
    embed.embed_pass(conn, corpus["db"], fe)
    conn.close()
    idx = Index(corpus["db"])
    eng = semantic.Engine(idx, "fake", fe.dim, embedder=fe)
    sha = [r[0] for r in idx.conn().execute("SELECT loc FROM chunk WHERE kind='commit' AND text LIKE 'feat:%'")][0]
    golden = [
        {"id": "q1", "query": "монетизирует латентность", "tags": ["exact"],
         "expect": [str(corpus["slug"] / "s1.jsonl")], "must_contain": ["монетизирует латентность"]},
        {"id": "q2", "query": "matching engine burst", "tags": ["chat"],
         "expect": [str(corpus["tg"] / "ChatExport_2026-09-14 (1)") + "/"], "must_contain": ["5/s"]},
        {"id": "q3", "query": "добавил коллектор", "tags": ["kind:commit"],
         "expect": [f"{corpus['repo']}@{sha[:7]}"], "must_contain": ["коллектор"]},
        {"id": "q4", "query": "throttle", "tags": ["xling"],
         "expect": [str(corpus["slug"] / "memory" / "reference_kbindex.md")], "must_contain": ["Троттл"]},
        {"id": "q5", "query": "что-то", "tags": [], "expect": [str(tmp_path / "gone.md")],
         "must_contain": ["x"]},
        {"id": "q6", "query": "anytime", "tags": [], "expect": [str(corpus["research"] / "book-notes.md")],
         "must_contain": ["литерал которого нет"]},
    ]
    return idx, eng, golden, corpus


def test_drift_check_moves_missing_files_and_lost_literals_aside(setup):
    _, _, golden, _ = setup
    ok, drifted = evaluate.drift_check(golden)
    assert {d["id"]: d["reason"] for d in drifted} == {"q5": "source missing", "q6": "literal lost"}
    assert [g["id"] for g in ok] == ["q1", "q2", "q3", "q4"]


def test_hit_strict_and_mrr_across_modes(setup):
    idx, eng, golden, _ = setup
    rep = evaluate.evaluate(idx, golden, evaluate.MODES, engine=eng)
    assert rep["counted"] == 4 and len(rep["drifted"]) == 2
    fts = {r["id"]: r for r in rep["modes"]["fts"]["rows"]}
    assert fts["q1"]["hit"] == 1 and fts["q1"]["strict"] == 1
    assert fts["q2"]["hit"] == 1, "a directory prefix matches the chat export"
    assert fts["q3"]["hit"] == 1, "repo@sha matches the commit"
    assert fts["q4"]["hit"] is None, "no lexical overlap"
    sem = {r["id"]: r for r in rep["modes"]["semantic"]["rows"]}
    assert sem["q4"]["hit"] is not None
    a = rep["modes"]["fts"]["all"]
    assert a["hit@10"] == 3 and a["strict@10"] == 3 and a["n"] == 4
    assert rep["modes"]["fts"]["by_tag"]["chat"]["hit@10"] == 1


def test_strict_requires_the_literal_in_the_hit_chunk(setup):
    idx, eng, golden, _ = setup
    g = dict(golden[0], must_contain=["слово, которого в чанке нет"], id="qx")
    hits = evaluate.run_query(idx, eng, g, "fts", None)
    j = evaluate.judge(hits, g)
    assert j["hit"] == 1 and j["strict"] is None


def test_subcorpus_and_restricted_ranking(setup):
    idx, eng, golden, _ = setup
    sub = evaluate.subcorpus(idx.conn(), golden[:4], distractors=3)
    assert len(sub) >= 4
    rep = evaluate.evaluate(idx, golden[:4], ("fts", "hybrid"), engine=eng, restrict=sub)
    assert rep["modes"]["hybrid"]["all"]["hit@10"] >= 3


def test_decision_rule_needs_net_four_wins_and_a_fast_enough_delta():
    def rep(ok_ids):
        rows = [{"id": f"q{i}", "strict": 1 if f"q{i}" in ok_ids else None, "hit": 1, "tags": []}
                for i in range(10)]
        return {"modes": {"hybrid": {"rows": rows}}}
    e5 = rep({"q0", "q1"})
    m3 = rep({"q0", "q1", "q2", "q3", "q4", "q5"})
    assert evaluate.decide(e5, m3, 30)["choice"] == "m3"
    assert evaluate.decide(e5, m3, 61)["choice"] == "e5b", "too slow on CPU"
    m3b = rep({"q0", "q2", "q3", "q4", "q5"})
    assert evaluate.decide(e5, m3b, 30)["choice"] == "e5b", "4 wins - 1 loss = net 3 < 4"


def test_eval_report_holds_no_chunk_text(setup):
    idx, eng, golden, corpus = setup
    rep = evaluate.evaluate(idx, golden[:4], ("fts",), engine=eng)
    p = evaluate.save(corpus["db"], "eval-test.json", rep)
    text = p.read_text()
    assert "монетизирует" not in text and "латентность" not in text
    import os, stat
    assert stat.S_IMODE(os.stat(p).st_mode) == 0o600
    assert stat.S_IMODE(os.stat(p.parent).st_mode) == 0o700


def test_cli_eval_whole_index_and_compare(setup, tmp_path, capsys):
    from kb import cli
    _, _, golden, corpus = setup
    g = tmp_path / "golden.jsonl"
    g.write_text("".join(json.dumps(x, ensure_ascii=False) + "\n" for x in golden[:4]))
    c = ["--config", str(corpus["config"])]
    assert cli.main(c + ["eval", "--golden", str(g), "--modes", "fts_raw_or,fts,hybrid"]) == 0
    out = capsys.readouterr().out
    assert "fts_raw_or" in out and "hybrid" in out and "report:" in out
    assert cli.main(c + ["eval", "--golden", str(g), "--modes", "fts,semantic,hybrid",
                         "--compare-models", "fake", "--distractors", "2"]) == 0
    out = capsys.readouterr().out
    assert "sub-corpus:" in out and "kb eval — fake" in out
