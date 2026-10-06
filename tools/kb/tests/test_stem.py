"""Russian/English stemming: a shadow column searched with the query stemmed
by the same code; raw text kept for display and `raw:` queries."""

import json

import pytest

from kb import search, stem, store, update
from kb import config as cfgmod
from kb.search import Index


def test_russian_paradigm_collapses_to_one_stem():
    assert len({stem.stem_word(w) for w in ("терял", "теряли", "терять")}) == 1
    assert len({stem.stem_word(w) for w in ("комиссия", "комиссии", "комиссию")}) == 1


def test_yo_is_folded_before_stemming():
    assert stem.stems("ещё") == stem.stems("еще")
    assert stem.stems("Отменённый") == stem.stems("отмененный")


def test_tokenization_matches_fts5_unicode61():
    assert stem.tokens("hl_bn_md.pem") == ["hl", "bn", "md", "pem"], "underscore separates, as in FTS5"
    assert stem.tokens("L2Book 10006 x-y") == ["l2book", "10006", "x", "y"]


def test_each_script_gets_its_own_stemmer_and_mixed_tokens_stay():
    assert stem.stem_word("cancelled") == stem.stem_word("cancels") == "cancel"
    assert stem.stem_word("ликвидации") == stem.stem_word("ликвидация")
    assert stem.stem_word("l2book") == "l2book"
    assert stem.stem_word("10006") == "10006"


def test_query_and_index_produce_the_same_stems():
    text = "Ребейты мейкера и троттлинг: Lighter монетизирует латентность"
    q = search.parse_query("ребейт троттлинга латентностью")
    indexed = set(stem.stems(text))
    assert set(q.terms) <= indexed


def test_stem_column_keeps_word_order_for_phrases():
    assert stem.stem_text("Заголовок", "быстрые отмены ордеров") == \
        " ".join(stem.stems("Заголовок") + stem.stems("быстрые отмены ордеров"))


def test_query_drops_stopwords_unless_nothing_else_is_left():
    q = search.parse_query("почему это не работает")
    assert stem.stem_word("работает") in q.terms and stem.stem_word("почему") not in q.terms
    assert search.parse_query("что это").terms, "an all-stopword query still searches"


def test_query_is_capped_at_32_terms():
    q = search.parse_query(" ".join(f"слово{i}" for i in range(60)))
    assert len(q.terms) == 32


# --- through the index ----------------------------------------------------------

@pytest.fixture()
def built(corpus):
    cfg = cfgmod.load_config(corpus["config"])
    update.build_full(cfg, corpus["db"], warn=lambda m: None)
    return corpus["db"]


def S(db, q, **kw):
    return search.search(Index(db), q, **kw)["results"]


def test_natural_query_finds_another_word_form_raw_does_not(built):
    assert S(built, "терять комиссии"), "stemmed: терял/комиссиях match"
    assert not S(built, "raw:терять"), "raw is exact word forms"
    assert S(built, "raw:теря*"), "raw keeps FTS5 prefix syntax"


def test_raw_query_finds_an_exact_form_natural_would_also_find(built):
    assert S(built, 'raw:"монетизирует латентность"')
    assert S(built, "монетизировать латентности")


def test_snippet_marks_word_forms_of_the_query(built):
    hit = S(built, "терять деньги")[0]
    assert "[терял]" in hit["snippet"] and "[деньги]" in hit["snippet"]


def test_stemmer_change_restems_on_update(corpus, monkeypatch):
    cfg = cfgmod.load_config(corpus["config"])
    update.update(cfg, corpus["db"], warn=lambda m: None, lock_timeout=0)
    conn = store.open_rw(corpus["db"])
    store.set_meta(conn, stem_version="0-old")
    conn.execute("UPDATE chunk SET stem = 'мусор' WHERE kind = 'transcript'")
    conn.close()
    st = update.update(cfg, corpus["db"], warn=lambda m: None, lock_timeout=0)
    assert st["restemmed"] > 0
    assert S(corpus["db"], "терять комиссии")
    c = store.open_rw(corpus["db"])
    c.execute("INSERT INTO chunk_fts(chunk_fts, rank) VALUES('integrity-check', 1)")
    assert store.get_meta(c, "stem_version") == stem.STEM_VERSION
    c.close()
