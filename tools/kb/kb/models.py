"""Embedding models: specs, the pinned manifest (models.toml), local loading.

The manifest pins each model to a commit sha of its Hugging Face repo and to
the sha256 and size of every file kb uses. Weights are safetensors only —
the runtime never unpickles anything (bge-m3 ships only a pickle; `kb
fetch-model` converts it once with torch.load(weights_only=True)).
"""

from __future__ import annotations

import hashlib
import os
import tomllib
from dataclasses import dataclass
from pathlib import Path

MANIFEST = Path(__file__).resolve().parent.parent / "models.toml"


@dataclass(frozen=True)
class ModelSpec:
    key: str
    repo_id: str
    dim: int
    max_tokens: int
    query_prefix: str
    passage_prefix: str
    pooling: str                     # 'mean' | 'cls'
    files: tuple[str, ...]           # what fetch-model downloads (besides the weights)
    weights: str                     # file in the repo holding the weights


TOKENIZER_FILES = ("config.json", "sentencepiece.bpe.model", "special_tokens_map.json",
                   "tokenizer.json", "tokenizer_config.json")

SPECS = {
    "e5b": ModelSpec("e5b", "intfloat/multilingual-e5-base", 768, 512, "query: ", "passage: ",
                     "mean", TOKENIZER_FILES, "model.safetensors"),
    "m3": ModelSpec("m3", "BAAI/bge-m3", 1024, 512, "", "", "cls",
                    TOKENIZER_FILES, "pytorch_model.bin"),
}


class ModelError(Exception):
    pass


def load_manifest(path: Path = MANIFEST) -> dict:
    if not path.is_file():
        return {}
    return tomllib.loads(path.read_text(encoding="utf-8")).get("model", {})


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 22), b""):
            h.update(block)
    return h.hexdigest()


def model_dir(hf_home: Path, key: str, manifest: dict | None = None) -> Path:
    manifest = load_manifest() if manifest is None else manifest
    entry = manifest.get(key)
    if not entry:
        raise ModelError(f"model {key!r} is not in models.toml — run: kb fetch-model --model {key}")
    spec = SPECS[key]
    return (Path(hf_home) / "hub" / ("models--" + spec.repo_id.replace("/", "--")) / "snapshots"
            / entry["revision"])


def verify(hf_home: Path, key: str, full: bool = False, manifest: dict | None = None) -> Path:
    """Size check of every pinned file (cheap); sha256 too when `full`.
    Returns the model directory; raises ModelError with the reason."""
    manifest = load_manifest() if manifest is None else manifest
    d = model_dir(hf_home, key, manifest)
    files = manifest[key].get("files", {})
    if "model.safetensors" not in files:
        raise ModelError(f"{key}: no safetensors weights pinned in models.toml")
    for name, meta in files.items():
        p = d / name
        try:
            size = os.stat(p).st_size
        except OSError:
            raise ModelError(f"{key}: missing {p} — run: kb fetch-model --model {key}") from None
        if size != int(meta["size"]):
            raise ModelError(f"{key}: {name} has size {size}, models.toml pins {meta['size']}")
        if full and sha256_file(p) != meta["sha256"]:
            raise ModelError(f"{key}: {name} sha256 differs from models.toml")
    for p in d.iterdir():
        if p.suffix in (".bin", ".pt", ".pkl", ".pickle"):
            raise ModelError(f"{key}: pickle file {p.name} next to the weights — refusing")
    return d
