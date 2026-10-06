"""Masking of recognisable secrets in text that leaves the database.

Moved verbatim from tools/kbindex.py (v1). A net, not a guarantee.
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
    # key = value / "key": "value" for credential-shaped names
    (re.compile(r"(?i)\b(api[_-]?keys?|secrets?|passwords?|passwd|passphrase|"
                r"tokens?|private[_-]?keys?|mnemonic|seed[_-]?phrase|"
                r"credentials?)\b(\"?\s*[:=]\s*)\"?'?[^\s\"',;]{6,}"),
     r"\1\2[redacted]"),
)


def redact(text):
    """Mask recognisable secrets in text that is about to be printed."""
    if not text:
        return text
    for pattern, replacement in REDACTIONS:
        text = pattern.sub(replacement, text)
    return text
