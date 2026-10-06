"""Tokens and stems (DESIGN.md §5).

Tokenization mirrors FTS5 unicode61: runs of letters/digits (`[^\\W_]+` —
the underscore separates), NFKC, lower case, and ё → е (FTS5's
remove_diacritics does NOT fold ё: `еще` never finds `ещё`). A token that is
all Cyrillic goes to the Snowball Russian stemmer, all Latin to the English
one; digits and mixed tokens (`l2book`, `10006`) stay as they are.

The index keeps a `stem` column (stems of title + text, in order, so phrases
and NEAR work on stems); queries are stemmed by the same code. Snowball is
pure Python (~25k words/s), so every word is cached once per process.
"""

from __future__ import annotations

import re
import unicodedata
from importlib.metadata import version

import snowballstemmer

TOKEN_RE = re.compile(r"[^\W_]+")
_CYR = re.compile(r"[а-я]+")
_LAT = re.compile(r"[a-z]+")
_RU = snowballstemmer.stemmer("russian")
_EN = snowballstemmer.stemmer("english")
STEM_VERSION = f"snowball-{version('snowballstemmer')}-r1"
_CACHE: dict[str, str] = {}
_CACHE_MAX = 3_000_000


def normalize(text: str) -> str:
    return unicodedata.normalize("NFKC", text).lower().replace("ё", "е")


def tokens(text: str) -> list[str]:
    return TOKEN_RE.findall(normalize(text))


def stem_word(token: str) -> str:
    """`token` already normalized (tokens() output) or not — both work."""
    s = _CACHE.get(token)
    if s is None:
        t = normalize(token)
        if _CYR.fullmatch(t):
            s = _RU.stemWord(t)
        elif _LAT.fullmatch(t):
            s = _EN.stemWord(t)
        else:
            s = t
        if len(_CACHE) < _CACHE_MAX:
            _CACHE[token] = s
    return s


def stems(text: str) -> list[str]:
    return [stem_word(t) for t in tokens(text)]


def stem_text(title: str, text: str) -> str:
    return " ".join(stems(title) + stems(text))
