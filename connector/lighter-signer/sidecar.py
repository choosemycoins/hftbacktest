#!/usr/bin/env python3
"""Lighter signer sidecar — Phase 2 (design note docs/design-lighter-connector.md §3.2).

A standalone process that wraps the lighter-sdk native signer
(``lighter/signers/lighter-signer-<os>-<arch>.{so,dylib,dll}``, a Go c-archive over
Schnorr/ECgFp5 + Poseidon2-Goldilocks). The private key lives ONLY in this process:
it is read from a TOML file this process opens itself, never passed on the command
line or through the connector, and never logged.

Why a sidecar and not a Rust port (§3.2): the signer is not a composition of ready
primitives (Goldilocks field, GF(p^5), ECgFp5 group, Poseidon2 with Plonky2
constants, Schnorr) — the vendor ships it as a native library and a Python SDK that
already solves the ctypes struct layout and the mandatory ``Free``-the-string memory
ownership. This sidecar reuses that path so the signing the connector relies on is
the vendor's, byte-for-byte the same path the golden-vector test pins.

Protocol: line-delimited JSON on stdin/stdout, one response per request, in order.

  request   {"id": <int>, "op": "create_order"|"cancel_order"|"cancel_all"|"auth_token"|"ping", ...}
  response  {"id": <int>, "ok": true,  ...}            on success
            {"id": <int>, "ok": false, "error": "..."} on a signing refusal (recoverable)

Only signing refusals come back as ``ok:false``; the connector decides policy. A
setup failure (missing key, unreadable library, unconfigured slot at startup) exits
non-zero so the connector sees the pipe close and fails closed (AGENTS.md §1.1).

The tx hash is NON-signature: the signature is non-deterministic (fresh random k per
signature), the hash is a pure function of the tx fields incl. ExpiredAt. ExpiredAt is
set inside the native library from the clock (10-min tx lifetime) and cannot be passed
in (§3.2); it comes back inside ``tx_info``.
"""
import ctypes
import json
import os
import sys

# The connector spawns us with the SDK importable (its venv). Allow an explicit path
# override for tests / non-default installs, without ever putting a key on argv.
_extra = os.environ.get("LIGHTER_SDK_PATH")
if _extra:
    sys.path.insert(0, _extra)

from lighter import signer_client as sc  # noqa: E402


def log(msg: str) -> None:
    """Operator-visible line on stderr. NEVER pass key material here."""
    print(f"[lighter-signer] {msg}", file=sys.stderr, flush=True)


# ---- flat-TOML config reader (py3.10 has no tomllib; lighter-sdk pulls no toml lib) --
def read_config(path: str) -> dict:
    """Read the flat testnet/mainnet config. Keys: url?, chain_id?, account_index,
    api_key_index, private_key. A [[key]] table list is also accepted for several
    api-key slots (§3.3: parallelism is more slots, not a nonce window).
    """
    cfg: dict = {"keys": {}}  # api_key_index -> private_key
    account_index = None
    cur_key_idx = None
    with open(path, "r") as fh:
        for raw in fh:
            line = raw.split("#", 1)[0].strip()
            if not line:
                continue
            if line == "[[key]]":
                cur_key_idx = None
                continue
            if "=" not in line:
                continue
            k, v = (s.strip() for s in line.split("=", 1))
            if v and v[0] in "\"'":
                v = v.strip("\"'")
                val: object = v
            else:
                try:
                    val = int(v)
                except ValueError:
                    val = v
            if k == "url":
                cfg["url"] = val
            elif k == "chain_id":
                cfg["chain_id"] = int(val)
            elif k == "account_index":
                account_index = int(val)
            elif k == "api_key_index":
                cur_key_idx = int(val)
            elif k == "private_key":
                idx = cur_key_idx if cur_key_idx is not None else cfg.get("_last_aki", 0)
                cfg["keys"][idx] = str(val)
                cfg["_last_aki"] = idx
    if account_index is None or not cfg["keys"]:
        raise ValueError("config must define account_index and at least one api_key_index/private_key")
    cfg["account_index"] = account_index
    cfg.pop("_last_aki", None)
    return cfg


class Signer:
    def __init__(self, cfg: dict):
        self.signer = sc.get_signer()  # loads the platform-appropriate native library
        self.account_index = int(cfg["account_index"])
        url = cfg.get("url", "https://testnet.zklighter.elliot.ai")
        chain_id = cfg.get("chain_id")
        if chain_id is None:
            chain_id = 304 if "mainnet.zklighter" in url else 300
        self.chain_id = int(chain_id)
        self.url = url
        self.slots = set()
        for aki, priv in cfg["keys"].items():
            key = priv[2:] if priv.startswith("0x") else priv
            err = sc.decode_and_free(self.signer.CreateClient(
                url.encode(), key.encode(), self.chain_id, int(aki), self.account_index))
            if err is not None:
                # Fail closed: a slot that cannot be created must not start.
                raise RuntimeError(f"CreateClient failed for api_key_index={aki}: {err}")
            self.slots.add(int(aki))
        log(f"ready: chain_id={self.chain_id} account_index={self.account_index} "
            f"api_key_slots={sorted(self.slots)} url={url}")

    def _aki(self, req: dict) -> int:
        aki = int(req["api_key_index"])
        if aki not in self.slots:
            raise KeyError(f"api_key_index {aki} is not a configured slot {sorted(self.slots)}")
        return aki

    @staticmethod
    def _decode(res) -> dict:
        err = sc.decode_and_free(res.err)
        tx_info = sc.decode_and_free(res.txInfo)
        tx_hash = sc.decode_and_free(res.txHash)
        sc.decode_and_free(res.messageToSign)
        if err:
            return {"ok": False, "error": err}
        return {"ok": True, "tx_type": res.txType, "tx_info": tx_info, "tx_hash": tx_hash}

    def create_order(self, r: dict) -> dict:
        return self._decode(self.signer.SignCreateOrder(
            int(r["market_index"]), int(r["client_order_index"]), int(r["base_amount"]),
            int(r["price"]), int(r["is_ask"]), int(r.get("order_type", 0)),
            int(r["time_in_force"]), int(r.get("reduce_only", 0)),
            int(r.get("trigger_price", 0)), int(r.get("order_expiry", -1)),
            int(r.get("integrator_account_index", 0)), int(r.get("integrator_taker_fee", 0)),
            int(r.get("integrator_maker_fee", 0)), int(r.get("self_trade_behavior_mode", 0)),
            int(r.get("self_trade_equality_mode", 0)), int(r.get("skip_nonce", 0)),
            int(r["nonce"]), self._aki(r), self.account_index))

    def cancel_order(self, r: dict) -> dict:
        return self._decode(self.signer.SignCancelOrder(
            int(r["market_index"]), int(r["order_index"]), int(r.get("skip_nonce", 0)),
            int(r["nonce"]), self._aki(r), self.account_index))

    def cancel_all(self, r: dict) -> dict:
        return self._decode(self.signer.SignCancelAllOrders(
            int(r.get("time_in_force", 0)), int(r["time_ms"]),
            int(r.get("market_index", 255)), int(r.get("skip_nonce", 0)),
            int(r["nonce"]), self._aki(r), self.account_index))

    def auth_token(self, r: dict) -> dict:
        # deadline is an ABSOLUTE unix-second timestamp (server ceiling 8h; §3.4).
        res = self.signer.CreateAuthToken(int(r["deadline"]), self._aki(r), self.account_index)
        auth = sc.decode_and_free(res.str)
        err = sc.decode_and_free(res.err)
        if err:
            return {"ok": False, "error": err}
        return {"ok": True, "auth": auth}

    def handle(self, req: dict) -> dict:
        op = req.get("op")
        if op == "ping":
            return {"ok": True}
        fn = {
            "create_order": self.create_order,
            "cancel_order": self.cancel_order,
            "cancel_all": self.cancel_all,
            "auth_token": self.auth_token,
        }.get(op)
        if fn is None:
            return {"ok": False, "error": f"unknown op: {op!r}"}
        return fn(req)


def main() -> int:
    path = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("LIGHTER_SIGNER_CONFIG")
    if not path:
        log("usage: sidecar.py <config.toml>  (or set LIGHTER_SIGNER_CONFIG)")
        return 2
    try:
        cfg = read_config(path)
    except Exception as e:  # noqa: BLE001 — any config failure is fatal, fail closed
        log(f"fatal: cannot read config: {e}")
        return 2
    try:
        signer = Signer(cfg)
    except Exception as e:  # noqa: BLE001 — a slot that cannot sign must not start
        log(f"fatal: signer init: {e}")
        return 3

    out = sys.stdout
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        rid = None
        try:
            req = json.loads(line)
            rid = req.get("id")
            resp = signer.handle(req)
        except Exception as e:  # noqa: BLE001 — a bad request must not kill the sidecar
            resp = {"ok": False, "error": f"{type(e).__name__}: {e}"}
        resp["id"] = rid
        out.write(json.dumps(resp) + "\n")
        out.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main())
