"""Semantic and hybrid search on the fake embedder."""

import numpy as np
import pytest

from kb import embed, search, semantic, store, update
from kb import config as cfgmod
from kb.embed import FakeEmbedder
from kb.search import Index


@pytest.fixture()
def indexed(corpus):
    cfg = cfgmod.load_config(corpus["config"])
    update.update(cfg, corpus["db"], warn=lambda m: None, lock_timeout=0)
    fe = FakeEmbedder(synonyms={"троттл": "throttle", "латентность": "latency"})
    conn = store.open_rw(corpus["db"])
    embed.embed_pass(conn, corpus["db"], fe)
    conn.close()
    idx = Index(corpus["db"])
    eng = semantic.Engine(idx, "fake", fe.dim, embedder=fe)
    return idx, eng, corpus


def S(idx, eng, q, **kw):
    return search.search(idx, q, engine=eng, **kw)


def test_semantic_finds_a_chunk_without_a_shared_word(indexed):
    idx, eng, _ = indexed
    assert not S(idx, eng, "throttle", mode="fts")["results"], "no lexical overlap"
    res = S(idx, eng, "throttle", mode="semantic")["results"]
    assert res and "троттл" in search.read_pointer(idx, res[0]["id"])["text"].lower()
    assert res[0]["ranks"] == {"sem": 1}


def test_hybrid_fuses_both_lists(indexed):
    idx, eng, _ = indexed
    resp = S(idx, eng, "latency монетизирует", mode="hybrid")
    assert resp["query"]["used"] == "hybrid"
    top = resp["results"][0]
    assert set(top["ranks"]) == {"fts", "sem"}, "found by both lists -> fused first"


def test_rrf_on_given_ranks():
    fused = semantic.rrf([1, 2, 3], [3, 4], {1: "2026-01-01", 2: "2026-01-02", 3: "2026", 4: "2026"})
    scores = {r: s for r, s, _ in fused}
    assert scores[3] == pytest.approx(1 / 63 + 1 / 61)
    assert scores[1] == pytest.approx(1 / 61)
    assert [r for r, _, _ in fused][0] == 3
    assert fused[1][0] == 1, "equal score: the better fts rank wins"
    assert fused[2][0] == 4 or fused[2][0] == 2


def test_filters_apply_before_top_k_in_the_semantic_list(indexed):
    idx, eng, _ = indexed
    res = S(idx, eng, "троттл комиссия латентность", mode="semantic", kind="commit", limit=50)["results"]
    assert res and all(r["kind"] == "commit" for r in res)


def test_chunks_without_vectors_are_in_fts_and_in_coverage(indexed):
    idx, eng, corpus = indexed
    (corpus["research"] / "fresh.md").write_text("# Fresh\n\nсвежая заметка без вектора про очередь\n")
    cfg = cfgmod.load_config(corpus["config"])
    update.update(cfg, corpus["db"], warn=lambda m: None, lock_timeout=0)
    resp = S(idx, eng, "свежая заметка", mode="hybrid")
    assert any("Fresh" in r["title"] for r in resp["results"])
    cov = resp["coverage"]
    assert cov["embedded"] == cov["in_scope"] - 1


def test_no_model_degrades_to_fts_and_says_so(indexed):
    idx, _, _ = indexed
    broken = semantic.Engine(idx, "fake", 64, loader=lambda: (_ for _ in ()).throw(OSError("no model")))
    resp = search.search(idx, "троттл", mode="hybrid", engine=broken)
    assert resp["query"]["used"] == "fts" and "no model" in resp["degraded"]
    assert resp["results"]
    resp = search.search(idx, "троттл", mode="semantic", engine=None)
    assert resp["degraded"] and resp["results"]


def test_raw_query_in_hybrid_embeds_the_text_without_operators():
    q = search.parse_query('raw:"монетизирует латентность" AND NOT спред*')
    assert semantic.semantic_text(q) == "монетизирует латентность спред"


def test_semantic_index_reloads_when_vectors_change(indexed):
    idx, eng, corpus = indexed
    before = eng.sem.coverage(search.make_scope())
    (corpus["research"] / "fresh2.md").write_text("# F2\n\nещё одна свежая заметка про ребейты\n")
    cfg = cfgmod.load_config(corpus["config"])
    update.update(cfg, corpus["db"], warn=lambda m: None, lock_timeout=0)
    conn = store.open_rw(corpus["db"])
    embed.embed_pass(conn, corpus["db"], eng._emb)
    conn.close()
    after = eng.sem.coverage(search.make_scope())
    assert after["embedded"] == before["embedded"] + 1


def test_semantic_restriction_ranks_inside_the_subset(indexed):
    idx, eng, _ = indexed
    conn = idx.conn()
    commits = np.array([r[0] for r in conn.execute("SELECT rowid FROM chunk WHERE kind='commit'")])
    res = S(idx, eng, "троттл", mode="hybrid", restrict=commits, limit=50)["results"]
    assert res and all(r["kind"] == "commit" for r in res)


def test_a_chunk_without_a_vector_never_enters_the_semantic_list(indexed):
    """row -1 must not index the matrix from the end (another chunk's vector)."""
    idx, eng, corpus = indexed
    (corpus["research"] / "novec.md").write_text("# NoVec\n\nзаметка без вектора номер один\n")
    cfg = cfgmod.load_config(corpus["config"])
    update.update(cfg, corpus["db"], warn=lambda m: None, lock_timeout=0)
    res = S(idx, eng, "заметка без вектора", mode="semantic", limit=50)["results"]
    assert all(r["title"] != "novec.md › NoVec" for r in res)


# --- CLI (config model 'fake') -------------------------------------------------

def test_cli_update_embed_then_hybrid_search_and_gc(corpus, capsys):
    import json as _json
    from kb import cli
    c = ["--config", str(corpus["config"])]
    assert cli.main(c + ["update", "--embed", "--lock-timeout", "0"]) == 0
    out = capsys.readouterr().out
    assert "embeddings fake: +" in out and "remaining 0" in out
    assert cli.main(c + ["search", "латентность", "--json"]) == 0
    resp = _json.loads(capsys.readouterr().out)
    assert resp["query"]["used"] == "hybrid" and resp["degraded"] is None
    assert resp["coverage"]["embedded"] == resp["coverage"]["in_scope"] > 0
    assert cli.main(c + ["embed", "--max-chunks", "5"]) == 0
    assert cli.main(c + ["gc"]) == 0
    assert "garbage rows dropped" in capsys.readouterr().out


def test_cli_embed_without_an_index_is_a_clean_error(corpus, capsys):
    from kb import cli
    assert cli.main(["--config", str(corpus["config"]), "embed"]) == 2
    assert "kb build --full" in capsys.readouterr().err


def test_stats_shows_vector_coverage_and_checks_the_matrix(corpus, capsys):
    from kb import cli
    c = ["--config", str(corpus["config"])]
    assert cli.main(c + ["update", "--embed", "--max-chunks", "3", "--lock-timeout", "0"]) == 0
    capsys.readouterr()
    assert cli.main(c + ["stats", "--check"]) == 0
    out = capsys.readouterr().out
    assert "embedded" in out and "vectors fake:" in out and "FTS integrity ok" in out
    total = [l for l in out.splitlines() if l.strip().startswith("total")][0]
    assert total.split()[4] == "3"
    conn = store.open_rw(corpus["db"])
    conn.execute("UPDATE emb_row SET row = 999 WHERE rowid = (SELECT min(rowid) FROM emb_row)")
    conn.close()
    assert cli.main(c + ["stats", "--check"]) == 3
    assert "points past the matrix" in capsys.readouterr().out


# --- the scan: exact ranking, fast on a big scope ---------------------------------

def _near_ties(n=3000, dim=64, seed=7):
    """Unit vectors whose cosines to `q` differ by less than a float16 ulp
    near the top, and several chunks sharing one matrix row (same text)."""
    rng = np.random.default_rng(seed)
    q = rng.standard_normal(dim).astype(np.float32)
    q /= np.linalg.norm(q)
    noise = rng.standard_normal((n, dim)).astype(np.float32)
    noise -= np.outer(noise @ q, q)                       # orthogonal to q
    noise /= np.linalg.norm(noise, axis=1, keepdims=True)
    cos = 0.9 + 0.0004 * rng.standard_normal(n).astype(np.float32)   # crowded top
    m = cos[:, None] * q + np.sqrt(1 - cos ** 2)[:, None] * noise
    matrix = m.astype("<f2")
    rows = np.concatenate([np.arange(n), rng.integers(0, n, 400)])  # duplicates share a row
    return matrix, rows, q


def _exact(matrix, rows, q, depth):
    s = np.asarray(matrix[rows], dtype=np.float32) @ q
    order = np.lexsort((np.arange(len(rows)), -s))[:depth]
    return order, s[order]


def test_a_big_scope_is_preranked_in_fp16_and_rescored_exactly(monkeypatch):
    """Above FAST_MIN the whole matrix is scored once in float16 (one fast
    matrix-vector product), and only the best PREFILTER are re-scored in
    float32: the answer equals the exact float32 ranking, ties by position."""
    import torch
    matrix, rows, q = _near_ties()
    fp16_order = np.argsort(-(torch.mv(torch.from_numpy(matrix), torch.from_numpy(q.astype("<f2")))
                               .float().numpy()[rows]), kind="stable")[:100]
    ref, ref_scores = _exact(matrix, rows, q, 100)
    assert not np.array_equal(fp16_order, ref), "precondition: float16 alone misorders the top"
    calls = []
    real = semantic._fp16_scores
    monkeypatch.setattr(semantic, "_fp16_scores", lambda m, v: calls.append(1) or real(m, v))
    monkeypatch.setattr(semantic, "FAST_MIN", 1000)
    monkeypatch.setattr(semantic, "PREFILTER", 400)
    pos, scores = semantic.top_rows(matrix, rows, q, 100)
    assert calls, "the big scope took the fp16 pre-ranking"
    assert np.array_equal(pos, ref)
    assert np.allclose(scores, ref_scores, atol=1e-6)


def test_a_small_scope_is_scored_exactly_without_the_prefilter(monkeypatch):
    matrix, rows, q = _near_ties(n=500)
    monkeypatch.setattr(semantic, "_fp16_scores", lambda m, v: pytest.fail("no prefilter below FAST_MIN"))
    pos, scores = semantic.top_rows(matrix, rows, q, 100)
    ref, ref_scores = _exact(matrix, rows, q, 100)
    assert np.array_equal(pos, ref) and np.allclose(scores, ref_scores, atol=1e-6)


@pytest.mark.parametrize("active", ['"e5b"', '""'])
def test_update_embed_that_skips_the_embedding_pass_is_incomplete(corpus, capsys, active):
    """The nightly unit runs `kb update --embed`: a pass skipped for a missing
    or unverifiable model (or no active model) must not look like success —
    otherwise the unit is green every night while coverage stops growing.
    Same verdict as `kb embed` on the same config."""
    from kb import cli
    p = corpus["config"]
    p.write_text(p.read_text().replace('active = "fake"', f"active = {active}"))
    c = ["--config", str(p)]
    assert cli.main(c + ["update", "--embed", "--lock-timeout", "0"]) == 3
    err = capsys.readouterr().err
    assert "embeddings skipped" in err and "INCOMPLETE" in err
    assert cli.main(c + ["embed"]) == 3
    assert cli.main(c + ["update", "--lock-timeout", "0"]) == 0, "without --embed nothing was skipped"
