"""Two-sided parity of the StateValues layout: Rust struct <-> numpy dtype.

The Rust ``the_state_values_layout_the_python_dtype_mirrors_is_unchanged`` pin
(``hftbacktest/src/types.rs``) guards the *Rust* side — offsets, size, alignment.
But nothing connected the two definitions, so a reorder or retype on the *numpy*
side (``state_values_dtype`` in ``hftbacktest/types.py``) was silent, and a
same-size retype on the Rust side (``f64`` <-> ``i64``) slipped past an
offset-only check. numba.carray reads this dtype over the raw ``StateValues``
bytes at four FFI entry points (AGENTS.md 4.7), so a mismatch is plausible-wrong
PnL, not an error.

This is a pure-source check: it parses both files as text and asserts the field
order, names, and Rust-type <-> numpy-format mapping agree, plus that
``StateValues`` still carries ``#[repr(C)]`` (the actual FFI contract — without
it the field order is only incidentally what the dtype expects).
"""

import re
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]  # py-hftbacktest/
_TYPES_PY = _ROOT / "hftbacktest" / "types.py"
_TYPES_RS = _ROOT.parent / "hftbacktest" / "src" / "types.rs"

# Rust scalar -> numpy format, for the types StateValues actually uses. Both are
# 8-byte here; the map is what makes a same-size retype (f64<->i64) a failure.
_RUST_TO_NUMPY = {"f64": "f8", "i64": "i8", "f32": "f4", "i32": "i4"}
_NUMPY_SIZE = {"f8": 8, "i8": 8, "f4": 4, "i4": 4}


def _numpy_fields() -> list[tuple[str, str]]:
    """Ordered (name, format) from the state_values_dtype literal."""
    src = _TYPES_PY.read_text()
    body = re.search(r"state_values_dtype\s*=\s*np\.dtype\(\s*\[(.*?)\]", src, re.S)
    assert body, "state_values_dtype list not found in types.py"
    return re.findall(r"\(\s*'([A-Za-z0-9_]+)'\s*,\s*'([A-Za-z0-9_]+)'\s*\)", body.group(1))


def _rust_struct() -> tuple[list[tuple[str, str]], bool]:
    """Ordered (name, type) of StateValues fields, and whether #[repr(C)] precedes it."""
    src = _TYPES_RS.read_text()
    m = re.search(
        r"(#\[repr\(C\)\]\s*\n(?:#\[[^\]]*\]\s*\n)*)?pub struct StateValues\s*\{(.*?)\}",
        src,
        re.S,
    )
    assert m, "pub struct StateValues not found in types.rs"
    has_repr_c = bool(m.group(1)) and "#[repr(C)]" in m.group(1)
    fields = re.findall(r"pub\s+([A-Za-z0-9_]+)\s*:\s*([A-Za-z0-9_]+)\s*,", m.group(2))
    return fields, has_repr_c


def test_state_values_struct_matches_the_numpy_dtype():
    rust_fields, has_repr_c = _rust_struct()
    numpy_fields = _numpy_fields()

    assert has_repr_c, (
        "StateValues lost #[repr(C)]; without it the field order the numpy dtype "
        "reads is only incidental, not guaranteed"
    )
    assert [n for n, _ in rust_fields] == [n for n, _ in numpy_fields], (
        f"field order/names diverge: Rust {[n for n, _ in rust_fields]} vs "
        f"numpy {[n for n, _ in numpy_fields]}"
    )
    for (rname, rtype), (_, nfmt) in zip(rust_fields, numpy_fields):
        assert _RUST_TO_NUMPY.get(rtype) == nfmt, (
            f"StateValues::{rname} is Rust `{rtype}` but the dtype reads `{nfmt}` "
            f"(expected `{_RUST_TO_NUMPY.get(rtype)}`) — a retype numba.carray would "
            f"misread"
        )

    itemsize = sum(_NUMPY_SIZE[nfmt] for _, nfmt in numpy_fields)
    assert itemsize == 48, f"state_values_dtype itemsize drifted from 48 to {itemsize}"
