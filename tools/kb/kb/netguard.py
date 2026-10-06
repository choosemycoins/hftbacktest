"""No network outside `kb fetch-model` (DESIGN.md §10.3).

1. Offline variables are FORCED (not setdefault) before anything imports
   huggingface_hub/transformers — those read them at import time.
2. Structurally the process is expected to run without a network: the MCP
   server under `unshare -rn`, the nightly unit with PrivateUsers=yes +
   PrivateNetwork=yes. `require_offline()` checks that from the inside: the
   interfaces in /proc/net/dev (per network namespace) must be exactly {lo}.
   The check itself opens no socket.
"""

from __future__ import annotations

import os
from pathlib import Path

OFFLINE_ENV = {
    "HF_HUB_OFFLINE": "1",
    "TRANSFORMERS_OFFLINE": "1",
    "HF_DATASETS_OFFLINE": "1",
    "HF_HUB_DISABLE_TELEMETRY": "1",
    "HF_HUB_DISABLE_PROGRESS_BARS": "1",
    "HF_HUB_DISABLE_IMPLICIT_TOKEN": "1",
    "TQDM_DISABLE": "1",
    "TRANSFORMERS_VERBOSITY": "error",
    "TOKENIZERS_PARALLELISM": "false",
}

LAUNCH_HINT = ("run it without a network: MCP — `/usr/bin/unshare -rn …/kb mcp`; the nightly "
               "unit — PrivateUsers=yes + PrivateNetwork=yes (see tools/kb/units)")


class NetworkError(Exception):
    pass


def force_offline_env() -> None:
    os.environ.update(OFFLINE_ENV)


def interfaces(path: str | Path = "/proc/net/dev") -> set[str]:
    names = set()
    for line in Path(path).read_text().splitlines()[2:]:
        if ":" in line:
            names.add(line.split(":", 1)[0].strip())
    return names


def require_offline(path: str | Path | None = None) -> None:
    path = path or os.environ.get("KB_PROC_NET_DEV", "/proc/net/dev")
    try:
        found = interfaces(path)
    except OSError as exc:
        raise NetworkError(f"cannot read {path}: {exc}") from exc
    extra = sorted(found - {"lo"})
    if extra:
        raise NetworkError(f"network interfaces visible ({', '.join(extra)}): {LAUNCH_HINT}")
