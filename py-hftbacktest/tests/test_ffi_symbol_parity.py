"""FFI symbol parity between the Python ctypes bindings and the Rust externs.

Every ``lib.<name>`` referenced in ``hftbacktest/binding.py`` must resolve to a
``#[no_mangle] pub extern "C" fn <name>`` exported by ``py-hftbacktest/src/*.rs``.

Why this needs its own gate: a referenced-but-unexported symbol raises
``AttributeError`` at IMPORT time under a ``--features live`` wheel (the ctypes
lookup ``lib.<name>`` fails while the module body runs), so the Python live bot
does not load at all. Nothing else catches it — ``cargo check --features live``
compiles the Rust side fine because no Rust code names the missing symbol, and
the rest of pytest never imports ``binding.py``. This is a pure-source check: it
parses both sides and never builds or loads the extension.

"Exported" means specifically ``#[unsafe(no_mangle)] pub extern "C" fn``: a
``pub extern "C" fn`` *without* ``#[unsafe(no_mangle)]`` keeps Rust name
mangling, so ``lib.<name>`` would still miss it. The exported-symbol pattern
therefore requires the attribute to immediately precede the function — matching
the signature alone would call a mangled symbol "exported" and pass falsely.
"""

import re
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]  # py-hftbacktest/
_BINDING = _ROOT / "hftbacktest" / "binding.py"
_SRC = _ROOT / "src"

_REFERENCED_RE = re.compile(r"\blib\.([A-Za-z0-9_]+)")
# The attribute is load-bearing: without #[unsafe(no_mangle)] the symbol is
# mangled and ctypes cannot find it, so it must be part of what counts as exported.
_EXPORTED_RE = re.compile(
    r'#\[unsafe\(no_mangle\)\]\s*\npub extern "C" fn\s+([A-Za-z0-9_]+)'
)


def _referenced_symbols() -> set[str]:
    return set(_REFERENCED_RE.findall(_BINDING.read_text()))


def _exported_symbols() -> set[str]:
    exported: set[str] = set()
    for rs in sorted(_SRC.glob("*.rs")):
        exported |= set(_EXPORTED_RE.findall(rs.read_text()))
    return exported


def test_every_referenced_ffi_symbol_is_exported():
    missing = sorted(_referenced_symbols() - _exported_symbols())
    assert not missing, (
        "binding.py references ctypes symbols that no `pub extern \"C\" fn` in "
        "py-hftbacktest/src/*.rs exports; under a --features live wheel each one "
        f"raises AttributeError at import time: {missing}"
    )
