"""`kb fetch-model` — the ONLY command that touches the network.

Downloads the pinned files of one model into HF_HOME (configs, tokenizer,
weights), converts a pickle-only repo (bge-m3) to safetensors exactly once
with torch.load(weights_only=True), deletes the pickle, and writes the
revision sha + sha256 + size of every file into tools/kb/models.toml.
"""

from __future__ import annotations

import os
from pathlib import Path

from .models import MANIFEST, SPECS, ModelError, load_manifest, sha256_file


def _toml_escape(s: str) -> str:
    return s.replace("\\", "\\\\").replace('"', '\\"')


def write_manifest(entries: dict, path: Path = MANIFEST) -> None:
    lines = ["# Pinned embedding models (written by `kb fetch-model`; do not edit by hand).",
             "# revision = commit sha of the Hugging Face repo; every file has a sha256 + size;",
             "# weights are safetensors only (pickles are converted once and deleted).", ""]
    for key in sorted(entries):
        e = entries[key]
        lines += [f"[model.{key}]", f'repo_id = "{_toml_escape(e["repo_id"])}"',
                  f'revision = "{e["revision"]}"']
        if e.get("converted_from"):
            lines.append(f'converted_from = "{_toml_escape(e["converted_from"])}"')
        lines.append("")
        for name in sorted(e["files"]):
            meta = e["files"][name]
            lines += [f'[model.{key}.files."{_toml_escape(name)}"]',
                      f'sha256 = "{meta["sha256"]}"', f"size = {int(meta['size'])}", ""]
    path.write_text("\n".join(lines), encoding="utf-8")


def _unlink_chain(path: Path) -> None:
    """Remove a symlink chain and the file it ends in (the HF cache links
    snapshot -> per-repo blob -> shared blob)."""
    p = Path(path)
    while p.is_symlink():
        nxt = (p.parent / os.readlink(p))
        p.unlink()
        p = nxt
    if p.exists():
        p.unlink()


def convert_pickle(bin_path: Path, out: Path) -> None:
    import torch
    from safetensors.torch import save_file
    state = torch.load(bin_path, map_location="cpu", weights_only=True)
    tensors = {k: v.contiguous().clone() for k, v in state.items() if hasattr(v, "contiguous")}
    save_file(tensors, str(out), metadata={"format": "pt"})


def fetch_model(key: str, hf_home: Path, revision: str | None = None, log=print) -> dict:
    if key not in SPECS:
        raise ModelError(f"unknown model {key!r} (one of {', '.join(SPECS)})")
    for var in ("HF_HUB_OFFLINE", "TRANSFORMERS_OFFLINE"):
        os.environ.pop(var, None)
    os.environ["HF_HOME"] = str(hf_home)
    os.environ.setdefault("HF_HUB_DISABLE_TELEMETRY", "1")
    from huggingface_hub import HfApi, hf_hub_download

    spec = SPECS[key]
    rev = revision or HfApi().model_info(spec.repo_id).sha
    log(f"kb fetch-model {key}: {spec.repo_id}@{rev}")
    files = {}
    snap = None
    for name in (*spec.files, spec.weights):
        p = Path(hf_hub_download(spec.repo_id, name, revision=rev, cache_dir=str(Path(hf_home) / "hub")))
        snap = p.parent
        files[name] = p
        log(f"  {name}: {p.stat().st_size} bytes")
    converted_from = None
    if spec.weights != "model.safetensors":
        out = snap / "model.safetensors"
        log(f"  converting {spec.weights} -> model.safetensors (torch.load weights_only=True)")
        convert_pickle(files[spec.weights], out)
        _unlink_chain(files[spec.weights])     # snapshot link -> repo blob link -> blob
        del files[spec.weights]
        files["model.safetensors"] = out
        converted_from = spec.weights
    entry = {"repo_id": spec.repo_id, "revision": rev, "converted_from": converted_from,
             "files": {name: {"sha256": sha256_file(p), "size": p.stat().st_size}
                       for name, p in files.items()}}
    manifest = load_manifest()
    manifest[key] = entry
    write_manifest(manifest)
    log(f"  pinned in {MANIFEST}")
    return entry
