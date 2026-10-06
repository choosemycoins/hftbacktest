"""Modes of the database directory and files.

Moved verbatim from tools/kbindex.py (v1): the database is a secret (0600);
its directory is chmodded 0700 only when kb owns it (created it, found it
empty, or finds its marker there).
"""

import os
import stat as stat_mod
from pathlib import Path

HOME = Path.home()

DEFAULT_DB = HOME / ".local" / "share" / "kbindex" / "kb.db"

DB_MODE = 0o600
DIR_MODE = 0o700

# Written into a directory whose mode kbindex manages. Its presence — and
# nothing else — authorises re-chmodding that directory on later builds.
DIR_MARKER = ".kbindex-dir"
DIR_MARKER_TEXT = (
    "This directory is managed by kb (tools/kb, formerly tools/kbindex.py): it\n"
    "is kept 0700 because kb.db and the embedding matrices hold secrets pasted\n"
    "into session transcripts and chats. Delete this marker to stop kb from\n"
    "touching the directory mode.\n"
)


def _claim_dir(path: Path) -> None:
    os.chmod(path, DIR_MODE)
    marker = path / DIR_MARKER
    if not marker.exists():
        marker.write_text(DIR_MARKER_TEXT, encoding="utf-8")
        os.chmod(marker, DB_MODE)


def _secure_dir(db_path: Path, warn=None) -> bool:
    """Prepare the directory that will hold `db_path`; True if kbindex owns it.

    Ownership — and only ownership — licenses changing the mode of a directory
    the user pointed us at. `--db ~/kb.db` must not turn $HOME into 0700, and
    `--db /srv/shared/kb.db` must not lock the other users of a shared
    directory out. A directory is ours if we created it, if it is empty, or if
    it carries our marker from an earlier build; otherwise its mode is left
    exactly as found and the caller is warned. The database file itself is
    0600 either way.
    """
    warn = warn or (lambda msg: None)
    path = db_path.parent
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
        _claim_dir(path)
        return True
    if (path / DIR_MARKER).is_file():
        _claim_dir(path)
        return True
    strangers = [p.name for p in path.iterdir()
                 if p.name != db_path.name
                 and not p.name.startswith(f".{db_path.name}.tmp-")]
    if not strangers:
        _claim_dir(path)
        return True
    mode = stat_mod.S_IMODE(path.stat().st_mode)
    warn(f"{path} holds unrelated files ({len(strangers)}), so its mode "
         f"{mode:04o} is left alone — the database will be 0600 but anyone who "
         f"can enter this directory can read it. Prefer {DEFAULT_DB.parent}.")
    return False
