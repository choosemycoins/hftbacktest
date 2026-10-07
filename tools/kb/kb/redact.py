"""Masking of recognisable secrets in text that leaves the database.

Moved from tools/kbindex.py (v1); v2 widened the named-secret rule and added
its own (DESIGN.md §10.2). A net, not a guarantee.
"""

import re

# Printing a snippet takes text out of a 0600 database and puts it on a
# terminal, into scrollback and into whatever the operator pastes next.
# Transcripts contain pasted private keys, wallet addresses and tokens, so the
# shapes below are masked on the way out. This is a net, not a guarantee: a
# secret in an unrecognised shape still gets printed — hence the header
# warning. `[`/`]` around a term are FTS5 match markers, so the patterns
# tolerate them inside a run.
_HEXISH = r"[0-9a-fA-F\[\]]"
# NAME=value, NAME: value, NAME = value, "name": "value". The name only has
# to CONTAIN a credential word — DB_PASSWORD, BYBIT_API_KEY, apiSecret,
# x-api-key (`\bpassword\b` missed them all: `_`, `-` and camelCase glue the
# word to the rest; `tokeniz…` is no token). The value goes whatever its
# length, a quoted one up to its closing quote (a mnemonic is twelve words)
# or, unclosed, to the end of the line; the name stays. One line only, and
# `==` is a comparison, not an assignment. An earlier mask is taken whole and
# nothing after it: output is masked more than once (snippet, JSON, line).
# The name is bounded (64 around the word): unbounded, a long run of name
# characters cost quadratic time (12 000 × "token": 1 s).
_CREDENTIAL = (r"(?:password|passwd|passphrase|secret|token(?!iz)|api[_-]?key|private[_-]?key|"
               r"mnemonic|seed[_-]?phrase|credential)")
_NAMED_SECRET = re.compile(
    r"(?i)((?<![\w.-])[\w.-]{0,64}?" + _CREDENTIAL + r"[\w.-]{0,64})"   # the name: kept
    r"([\"']?[^\S\n]*(?::=?|=(?!=))[^\S\n]*)"                 # the separator (NBSP too): kept
    r"(?:\[redacted[^\]\s]*\]|\"[^\"\n]*\"?|'[^'\n]*'?|[^\s\"',;]+)")   # the value: masked
REDACTIONS = (
    # 0x-prefixed values: EVM/HL addresses and private keys alike
    (re.compile(rf"\b0x{_HEXISH}{{16,}}"), "[redacted:0x]"),
    # bare long hex: 64-char keys and hashes. 40 is left alone so that git
    # SHAs — the whole point of a commit pointer — stay readable.
    (re.compile(rf"\b{_HEXISH}{{48,}}\b"), "[redacted:hex]"),
    (re.compile(r"\beyJ[A-Za-z0-9_\-\[\]]{8,}\.[A-Za-z0-9_\-\[\]]{8,}\.[A-Za-z0-9_\-\[\]]{5,}"),
     "[redacted:jwt]"),
    (re.compile(r"(?i)\b(?:bearer|basic)\s+[A-Za-z0-9+/=._\-\[\]]{12,}"), "[redacted:auth]"),
    (re.compile(r"\b(?:sk|pk|rk)-[A-Za-z0-9_\-\[\]]{16,}"), "[redacted:token]"),
    (re.compile(r"\bgh[pousr]_[A-Za-z0-9\[\]]{20,}"), "[redacted:token]"),
    (re.compile(r"\bxox[abposr]-[A-Za-z0-9\-\[\]]{10,}"), "[redacted:token]"),
    (re.compile(r"\bAKIA[0-9A-Z\[\]]{16}\b"), "[redacted:token]"),
    # key = value / "key": "value" for credential-shaped names (widened in v2)
    (_NAMED_SECRET, r"\1\2[redacted]"),
)


# v2 (DESIGN.md §10.2). Order matters: whole blocks and named shapes first,
# the generic high-entropy rule last, so it never eats a more specific mask.
_PEM = re.compile(r"-----BEGIN[^-\n]*PRIVATE KEY-----[\s\S]*?"
                  r"(?:-----END[^-\n]*PRIVATE KEY-----|\Z)")
REDACTIONS_V2 = (
    # scheme://user:password@host — the password goes, the user and the host
    # stay; the LAST '@' before the host ends it (`p@ss` unencoded happens)
    (re.compile(r"(?i)\b([a-z][a-z0-9+.-]*://[^\s:/?#@]*:)([^\s/?#]+)(@[^\s/?#@]+)"),
     r"\1[redacted]\3"),
    # Telegram bot token: <bot id>:<35 chars>
    (re.compile(r"(?<![0-9])\d{8,10}:[A-Za-z0-9_-]{35}(?![A-Za-z0-9_-])"), "[redacted:tg]"),
    (re.compile(r"\bxai-[A-Za-z0-9]{20,}"), "[redacted:token]"),
    (re.compile(r"\bhf_[A-Za-z0-9]{30,}"), "[redacted:token]"),
    (re.compile(r"\bAIza[0-9A-Za-z_-]{35}(?![0-9A-Za-z_-])"), "[redacted:token]"),
    # base58 >= 80: Solana private keys (88) and their kin
    (re.compile(r"(?<![A-Za-z0-9])[1-9A-HJ-NP-Za-km-z]{80,}(?![A-Za-z0-9])"), "[redacted:b58]"),
)
_RUN = re.compile(r"(?<![A-Za-z0-9+/])[A-Za-z0-9+/]{32,}={0,2}(?![A-Za-z0-9+/=])")


def _mixed(s: str) -> bool:
    return (any("a" <= c <= "z" for c in s) and any("A" <= c <= "Z" for c in s)
            and any(c.isdigit() for c in s))


def _entropy(m):
    """>= 32 of [A-Za-z0-9+/] with lower, upper AND digits: API keys/secrets
    (Binance 64, Bybit 36, base64 blobs). Not touched: git SHAs (no upper
    case), uuids and snake_case (separators break the run), and paths — a run
    with '/' is masked only when one of its '/'-free parts is itself a
    >= 20-char mixed token."""
    run = m.group(0)
    core = run.rstrip("=")
    if "/" in core:
        if not any(len(part) >= 20 and _mixed(part) for part in core.split("/")):
            return run
    elif not _mixed(core):
        return run
    return "[redacted:entropy]"


def redact(text):
    """Mask recognisable secrets in text that is about to be printed."""
    if not text:
        return text
    text = _PEM.sub("[redacted:pem]", text)
    for pattern, replacement in REDACTIONS:
        text = pattern.sub(replacement, text)
    for pattern, replacement in REDACTIONS_V2:
        text = pattern.sub(replacement, text)
    return _RUN.sub(_entropy, text)


def redact_obj(obj):
    """Mask every string inside a JSON-like structure (keys included): what
    leaves through the CLI or MCP is masked line by line, not field by field."""
    if isinstance(obj, str):
        return redact(obj)
    if isinstance(obj, dict):
        return {redact_obj(k): redact_obj(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [redact_obj(v) for v in obj]
    return obj
