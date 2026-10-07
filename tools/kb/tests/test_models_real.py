"""Real models from the local HF cache (pytest -m model). Skipped by default:
they need `kb fetch-model` first and take seconds to load. Offline."""

import os
from pathlib import Path

import numpy as np
import pytest

from kb import models
from kb.embed import load_embedder
from kb.netguard import force_offline_env

HF_HOME = Path(os.environ.get("KB_TEST_HF_HOME", "/home/storage/hft-scratch/hf-cache"))
pytestmark = pytest.mark.model


@pytest.mark.parametrize("key", ["e5b", "m3"])
def test_pinned_model_loads_offline_and_ranks_a_paraphrase_first(key):
    force_offline_env()
    try:
        models.verify(HF_HOME, key)
    except models.ModelError as exc:
        pytest.skip(str(exc))
    emb = load_embedder(key, HF_HOME, threads=4)
    passages = emb.embed_passages([
        "Lighter Standard: задержка 200 мс на выставление и отмену мейкера, 60 sendTx в минуту",
        "рецепт борща со свёклой и капустой",
        "Hyperliquid per-address request budget: 10000 + cumulative volume",
    ])
    assert passages.shape == (3, emb.dim)
    assert np.allclose(np.linalg.norm(passages, axis=1), 1.0, atol=1e-3)
    q = emb.embed_query("what artificial delay does Lighter add for makers?")
    scores = passages @ q
    assert scores.argmax() == 0, scores
