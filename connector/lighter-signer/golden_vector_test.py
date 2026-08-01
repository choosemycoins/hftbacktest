#!/usr/bin/env python3
"""Golden-vector test for the Lighter signer sidecar (design note §3.2).

Pins the native signer's HASH (never the signature — it is non-deterministic) against
the Phase-0 artifacts in ``docs/lighter-phase0-artifacts/``. FULLY OFFLINE and uses the
FICTIONAL key from the artifacts (0x1122…), never the real testnet key.

The tx hash is a pure function of the tx fields incl. ``ExpiredAt``. ``ExpiredAt`` is
set from the clock inside the native library and is NOT a shim parameter (§3.2), so a
specific historical hash cannot be reproduced through the ``.so`` alone. Two layers cope
with that:

  * PROPERTY layer (``.so`` only, always runs): reproduces the exact Phase-0 methodology
    — hash purity (a pure function of the fields), full 15-field sensitivity, and hash/
    signature structure. On this Linux box signing is ~3.8 ms, so same-millisecond
    collisions (which purity/sensitivity need) are produced by CONCURRENT signing.
  * EXACT + DIFFERENTIAL layer (needs a Go toolchain; skipped otherwise): a Go reference
    built from the SAME lighter-go revision the ``.so`` was compiled from sets ExpiredAt
    explicitly and reproduces every Phase-0 hash byte-for-byte; the differential then
    shows the shipped ``.so`` agrees with that reference at the clock-chosen ExpiredAt the
    ``.so`` picks. Chain: shipped .so == lighter-go@6d453d1 == Phase-0 (Darwin) artifacts.

Run:  python golden_vector_test.py    (exit 0 = pass)
"""
import base64
import json
import os
import shutil
import subprocess
import sys
import threading
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
ARTIFACTS = os.environ.get(
    "LIGHTER_PHASE0_ARTIFACTS",
    os.path.normpath(os.path.join(HERE, "..", "..", "docs", "lighter-phase0-artifacts")),
)
PIN_DIR = os.path.join(HERE, "pin")

_extra = os.environ.get("LIGHTER_SDK_PATH")
if _extra:
    sys.path.insert(0, _extra)
from lighter import signer_client as sc  # noqa: E402

# fictional golden key + base fields (docs/lighter-phase0-artifacts/golden_vector.json)
KEY = "11223344556677889900112233445566778899001122334455667788990011223344556677889900"
CHAIN, AKI, ACC = 300, 2, 1
URL = b"https://testnet.zklighter.elliot.ai"
BASE = dict(market_index=1, client_order_index=123456789, base_amount=1000, price=6500000,
            is_ask=0, order_type=0, time_in_force=2, reduce_only=0, trigger_price=0,
            order_expiry=1800000000000, integrator_account_index=0, integrator_taker_fee=0,
            integrator_maker_fee=0, self_trade_behavior_mode=0, self_trade_equality_mode=0,
            skip_nonce=0, nonce=42, api_key_index=AKI, account_index=ACC)

signer = sc.get_signer()
sc.decode_and_free(signer.CreateClient(URL, KEY.encode(), CHAIN, AKI, ACC))
sc.decode_and_free(signer.CreateClient(URL, KEY.encode(), CHAIN, AKI + 1, ACC))   # for api_key_index tweak
sc.decode_and_free(signer.CreateClient(URL, KEY.encode(), CHAIN, AKI, ACC + 1))   # for account_index tweak

FAILURES = []


def check(name, ok, detail=""):
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}{(' — ' + detail) if detail else ''}")
    if not ok:
        FAILURES.append(name)


def sign_create(d):
    res = signer.SignCreateOrder(
        d["market_index"], d["client_order_index"], d["base_amount"], d["price"], int(d["is_ask"]),
        d["order_type"], d["time_in_force"], d["reduce_only"], d["trigger_price"], d["order_expiry"],
        d["integrator_account_index"], d["integrator_taker_fee"], d["integrator_maker_fee"],
        d["self_trade_behavior_mode"], d["self_trade_equality_mode"], d["skip_nonce"],
        d["nonce"], d["api_key_index"], d["account_index"])
    e = sc.decode_and_free(res.err); ti = sc.decode_and_free(res.txInfo)
    th = sc.decode_and_free(res.txHash); sc.decode_and_free(res.messageToSign)
    assert e is None, e
    return json.loads(ti), th


def concurrent_sign(field_map, threads=16, per=90):
    """Sign each variant in field_map (name -> dict) concurrently; return name -> {ea: hash}."""
    out = {k: {} for k in field_map}
    locks = {k: threading.Lock() for k in field_map}

    def work(name, d):
        local = []
        for _ in range(per):
            j, th = sign_create(d)
            local.append((j["ExpiredAt"], th))
        with locks[name]:
            for ea, th in local:
                out[name][ea] = th

    ts = []
    for i in range(threads):
        for name, d in field_map.items():
            t = threading.Thread(target=work, args=(name, d)); t.start(); ts.append(t)
    for t in ts:
        t.join()
    return out


# ------------------------------------------------------------------ STRUCTURE
def test_structure():
    print("STRUCTURE (hash 40B, sig 80B, sig non-deterministic)")
    hashes, sigs, hlen, slen = set(), set(), set(), set()
    for _ in range(64):
        j, th = sign_create(BASE)
        hashes.add(th); sigs.add(j["Sig"]); hlen.add(len(th))
        slen.add(len(base64.b64decode(j["Sig"])))
    check("hash is 40 bytes (80 hex)", hlen == {80}, str(hlen))
    check("signature is 80 bytes", slen == {80}, str(slen))
    check("signature is non-deterministic", len(sigs) == 64, f"{len(sigs)}/64 distinct")


# ------------------------------------------------------------------ PURITY
def test_purity():
    print("PURITY (hash is a pure function of fields incl. ExpiredAt) — matches golden2_out.json")
    g2 = json.load(open(os.path.join(ARTIFACTS, "golden2_out.json")))
    # Collect EVERY (ea, hash) pair (not deduped) from a concurrent run so a millisecond
    # can be sampled more than once — that is what actually tests purity at ~3.8 ms/sign.
    pairs, lock = [], threading.Lock()

    def work():
        loc = [(j["ExpiredAt"], th) for j, th in (sign_create(BASE) for _ in range(120))]
        with lock:
            pairs.extend(loc)
    ts = [threading.Thread(target=work) for _ in range(20)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    buckets = defaultdict(set)
    for ea, th in pairs:
        buckets[ea].add(th)
    violations = sum(1 for hs in buckets.values() if len(hs) > 1)
    counts = defaultdict(int)
    for ea, _ in pairs:
        counts[ea] += 1
    multi = sum(1 for v in counts.values() if v >= 2)
    check("golden2 recorded hash is a pure function of fields", g2.get("hash_is_pure_function_of_fields") is True)
    check("no ExpiredAt maps to two hashes (purity)", violations == 0, f"{violations} violations")
    check("purity actually exercised (a millisecond sampled >=2x)", multi >= 1,
          f"{multi} ExpiredAt buckets sampled >=2x of {len(buckets)}")


# ------------------------------------------------------------------ SENSITIVITY
def _tweak(field):
    d = dict(BASE)
    if field in ("is_ask", "reduce_only", "skip_nonce"):
        d[field] = 1
    elif field == "time_in_force":
        d[field] = 1  # GTT
    else:
        d[field] = d[field] + 1
    return d


def test_sensitivity():
    print("SENSITIVITY (every CreateOrder field changes the hash) — matches sensitivity.json")
    sens = json.load(open(os.path.join(ARTIFACTS, "sensitivity.json")))
    fields = sens["fields_that_change_the_hash"]
    changed, inconclusive = [], []
    for f in fields:
        maps = concurrent_sign({"base": dict(BASE), "tw": _tweak(f)}, threads=16, per=110)
        shared = set(maps["base"]) & set(maps["tw"])
        if not shared:
            inconclusive.append(f); continue
        ea = sorted(shared)[0]
        if maps["base"][ea] != maps["tw"][ea]:
            changed.append(f)
    check("sensitivity.json lists all 15 CreateOrder fields", len(fields) == 15, str(len(fields)))
    check("every listed field changes the hash on the .so",
          set(changed) == set(fields) and not inconclusive,
          f"changed={len(changed)}/{len(fields)} inconclusive={inconclusive}")


# ------------------------------------------------------------------ EXACT + DIFFERENTIAL (Go)
def _go_available():
    return shutil.which("go") is not None


def _build_pin():
    env = dict(os.environ)
    env.setdefault("GOFLAGS", "-mod=mod")
    r = subprocess.run(["go", "build", "-o", "pin", "."], cwd=PIN_DIR, env=env,
                       capture_output=True, text=True, timeout=300)
    if r.returncode != 0:
        print("    (go build failed:\n" + r.stderr[-800:] + ")")
        return False
    return True


def test_exact_and_differential():
    print("EXACT + DIFFERENTIAL (Go reference @ the .so's own revision) — byte-for-byte vs artifacts")
    if not _go_available():
        print("  [SKIP] no Go toolchain — exact byte-for-byte pin needs it (§3.2). "
              "Property layer above still pins the .so.")
        return
    if not _build_pin():
        check("go reference builds", False)
        return
    proc = subprocess.Popen([os.path.join(PIN_DIR, "pin"), "serve"], stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE, text=True, bufsize=1)

    def ref(kind, ea, nonce):
        proc.stdin.write(f"{kind} {ea} {nonce}\n"); proc.stdin.flush()
        return proc.stdout.readline().strip()

    # EXACT: reproduce every recorded (ExpiredAt -> txHash) from the artifacts.
    gv = json.load(open(os.path.join(ARTIFACTS, "golden_vector.json")))
    g2 = json.load(open(os.path.join(ARTIFACTS, "golden2_out.json")))
    exact_ok = True
    bv = gv["base_vector"]
    got = ref("create", bv["inputs"]["ExpiredAt"], bv["inputs"]["nonce"])
    exact_ok &= (got == bv["expected_txHash_40b_hex"])
    for v in g2["golden_vectors"]:
        exact_ok &= (ref("create", v["ExpiredAt"], 42) == v["txHash"])
    # cancel vector from golden_out.json
    go = json.load(open(os.path.join(ARTIFACTS, "golden_out.json")))
    cancel = go["cancel_order_runs"][0]
    cti = json.loads(cancel["txInfo"])
    exact_ok &= (ref("cancel", cti["ExpiredAt"], cti["Nonce"]) == cancel["txHash"])
    check("Go reference reproduces every Phase-0 golden hash byte-for-byte", exact_ok)

    # DIFFERENTIAL: shipped .so vs reference at the .so's own (clock-chosen) ExpiredAt.
    mism = 0
    for _ in range(120):
        j, th = sign_create(BASE)
        if ref("create", j["ExpiredAt"], j["Nonce"]) != th:
            mism += 1
    proc.stdin.close(); proc.wait()
    check("shipped .so hash == Go reference at the .so's ExpiredAt (120 samples)", mism == 0,
          f"{mism} mismatches")


def main():
    print(f"artifacts: {ARTIFACTS}\n")
    test_structure()
    test_purity()
    test_sensitivity()
    test_exact_and_differential()
    print()
    if FAILURES:
        print(f"FAILED: {FAILURES}")
        return 1
    print("ALL GOLDEN-VECTOR CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
