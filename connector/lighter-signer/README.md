# Lighter signer sidecar (Phase 2, order path)

Out-of-process signer for the Lighter (zkLighter) backend. Design record:
[`docs/design-lighter-connector.md`](../../docs/design-lighter-connector.md) §3.2
(signing), §3.3 (nonce), §3.4 (auth token). This directory is the authoritative
implementation note for the signer; the connector-side client is
[`connector/src/lighter/signer.rs`](../src/lighter/signer.rs).

## Why a sidecar (not a Rust port, not linked in-process)

The signer is Schnorr over the ECgFp5 curve with a Poseidon2‑Goldilocks (Plonky2
variant) hash — **not** a composition of ready primitives (contrast HL's EIP‑712).
§3.2 rejected a Rust port for Phase 2: it would need the Goldilocks field, GF(p⁵),
ECgFp5 group arithmetic, Poseidon2 with Plonky2 constants, Schnorr, plus the field
vectors for ~22 tx types and the attribute layer — all byte‑exact. The vendor ships
it as a native Go c‑archive and a Python SDK that already solves the ctypes struct
layout and the mandatory “free the returned string with the library's own `Free`”
ownership. This sidecar reuses that path, so the signing the connector relies on is
the vendor's — the exact path the golden‑vector test pins.

Process boundary = key isolation: the private key lives **only** in the sidecar,
which reads it from a config file it opens itself. It never rides on argv, never
enters the connector's address space, and is never logged.

## The native signer on THIS platform (Linux x86_64)

Confirmed present and validated (the environment note's blocking prerequisite):

| | |
|---|---|
| SDK | `lighter-sdk == 1.1.2` (PyPI) |
| library | `lighter/signers/lighter-signer-linux-amd64.so` (ELF x86‑64, ~11.7 MB) |
| built from | `github.com/elliottech/lighter-go` rev `6d453d1cc2a2b5e6bba7658dc2381c8f789ba3eb` (2026‑06‑08) |
| hash deps | `poseidon_crypto v0.0.15`, `gnark-crypto/goldilocks v0.14.0` (`go version -m` on the `.so`) |
| all targets bundled | linux‑{amd64,arm64}, darwin‑{amd64,arm64}, windows‑amd64 |

Phase‑0 generated its golden vectors on `darwin-arm64`. The Goldilocks/Poseidon2 hash
is pure `uint64` integer math — platform‑invariant by construction — and this was
**measured**, not assumed (see the golden‑vector test below): the Linux `.so`
reproduces every Phase‑0 hash byte‑for‑byte.

Signing costs ~3.8 ms/call on this box (cgo + Poseidon2). That is well inside the
one‑in‑flight‑per‑slot regime (§4.12); if it ever isn't, §3.2's fallback is the Rust
port, and the hash is differentially testable against this `.so` in any volume.

## Protocol

Line‑delimited JSON on stdin/stdout, one response per request, answered in order.

Request: `{"id": <int>, "op": <op>, "api_key_index": <int>, "nonce": <int>, ...}`
(`nonce` omitted for `auth_token`/`ping`). Ops and their extra fields:

| op | fields | response on `ok:true` |
|---|---|---|
| `create_order` | `market_index, client_order_index, base_amount, price, is_ask, order_type, time_in_force, reduce_only, trigger_price, order_expiry` (+ optional integrator/self‑trade/`skip_nonce`) | `tx_type=14, tx_info, tx_hash` |
| `cancel_order` | `market_index, order_index` | `tx_type=15, tx_info, tx_hash` |
| `cancel_all` | `time_in_force` (0 IMMEDIATE→`time_ms` must be 0; 1 SCHEDULED; 2 ABORT), `time_ms`, `market_index` (255 = all) | `tx_type=16, tx_info, tx_hash` |
| `auth_token` | `deadline` (absolute unix **seconds**; server ceiling 8 h → refresh before expiry, §3.4) | `auth` |
| `ping` | — | — |

Response: `{"id": <int>, "ok": true, ...}` or `{"id": <int>, "ok": false, "error": "..."}`.

Only **signing refusals** come back as `ok:false` (recoverable — the caller decides
policy). A **setup failure** (missing key, unreadable library, unconfigured slot at
startup, a slot whose key the venue rejects) exits non‑zero, so the connector sees the
pipe close and fails closed (AGENTS.md §1.1).

### What a response is NOT

`tx_hash` is a pure function of the tx fields incl. `ExpiredAt`; the **signature is
non‑deterministic** (fresh random `k`) — never key anything off the signature. And a
`sendTx` HTTP 200 is not a verdict (§4.11): a rejected tx still gets 200, spends a
nonce and produces nothing on the private channel. Acceptance is `event_info.ae` on
the private channel, decided by the order manager — not by this sidecar and not by the
HTTP layer. This process only signs.

`ExpiredAt` is set from the clock **inside** the native library (a 10‑minute tx
lifetime) and cannot be supplied through the shim (§3.2). It comes back inside
`tx_info`.

## Running

Sidecar (its venv must have `lighter-sdk` importable; `LIGHTER_SDK_PATH` can point at a
non‑default install for tests):

```
python sidecar.py <config.toml>          # or set LIGHTER_SIGNER_CONFIG
```

Config format: [`signer.example.toml`](signer.example.toml). The connector spawns it
via `connector::lighter::signer::python_sidecar_command(python, script, config)` and
talks through `SignerClient`.

## Golden‑vector test — `python golden_vector_test.py`

Pins the **hash** (never the signature) against `docs/lighter-phase0-artifacts/`.
FULLY OFFLINE; uses the **fictional** key from the artifacts (`0x1122…`), never a real
key. Two layers, because `ExpiredAt` is clock‑sourced and not settable through the
`.so` (so a specific historical hash cannot be reproduced through the shipped library
alone):

1. **Property layer — shipped `.so`, always runs.** Reproduces the Phase‑0 methodology
   on this platform: hash **purity** (a pure function of the fields; 0 violations),
   full **15‑field sensitivity** (every `CreateOrder` field changes the hash), and
   hash/signature **structure** (40‑byte hash, 80‑byte sig, non‑deterministic). At
   ~3.8 ms/sign, same‑millisecond `ExpiredAt` collisions — which purity and sensitivity
   need — are produced by **concurrent** signing (Phase‑0's Darwin box was fast enough
   to get them serially; this one is not).
2. **Exact + differential layer — needs a Go toolchain, skipped otherwise.** The Go
   reference in [`pin/`](pin/), built from the **same** `lighter-go` revision the `.so`
   was compiled from, sets `ExpiredAt` explicitly and reproduces every Phase‑0 hash
   byte‑for‑byte; the differential then shows the shipped `.so` agrees with that
   reference at the clock‑chosen `ExpiredAt` the `.so` picks.

Established proof chain (measured, not argued):

```
shipped .so  ==(differential, per-ExpiredAt)==  lighter-go@6d453d1  ==(exact, 8/8)==  Phase-0 (Darwin) artifacts
```

The Go reference (`pin/`) is a self‑contained module pinned to the exact revision +
`poseidon_crypto v0.0.15`; `go run . ` prints the byte‑for‑byte reproduction of the
artifact hashes, `go run . serve` answers `<create|cancel> <expiredAt> <nonce>` lines
for the differential.

## Security

- Never print, log, or commit a private key. The sidecar logs only `chain_id`,
  `account_index`, and the configured api‑key **slots** — never key material.
- Testnet keys are testnet‑only (chain 300). Never point them at mainnet (chain 304).
- The example config carries a placeholder key; the real testnet config lives at
  `~/.config/hftbacktest-connector/lighter-testnet.toml` (mode 0600), referenced by
  path, never copied here.
