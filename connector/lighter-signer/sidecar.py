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

``ping`` answers with what this process signs for — ``chain_id``, ``url``,
``account_index``, ``api_key_slots`` — so the connector can refuse a signer that is
not on the network it dials (connector/src/lighter/signer.rs, ``verify_identity``).

Network. The chain id every transaction is hashed with is resolved from the network
URL — the config's ``url``, or ``--network-url`` from the connector, which must agree
when both are given — through ``KNOWN_CHAIN_IDS``; an unknown URL needs an explicit
``chain_id``, a ``chain_id`` that contradicts a known URL is refused, and so is a
mismatch with ``--expect-chain-id``. There is **no default**: the sidecar used to
infer 300 (testnet) for any URL that was not mainnet's, which on Robinhood Chain
(466324) signs every transaction for the wrong chain while the auth token — it
carries no chain id — keeps the private stream looking healthy.

Only signing refusals come back as ``ok:false``; the connector decides policy. A
setup failure (missing key, unreadable library, unconfigured slot at startup) exits
non-zero so the connector sees the pipe close and fails closed (AGENTS.md §1.1).

The tx hash is NON-signature: the signature is non-deterministic (fresh random k per
signature), the hash is a pure function of the tx fields incl. ExpiredAt. ExpiredAt is
set inside the native library from the clock (10-min tx lifetime) and cannot be passed
in (§3.2); it comes back inside ``tx_info``.
"""
import argparse
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


# ---- the network a transaction is signed for -------------------------------------------
# REST base -> the chain id its transactions are hashed with. Mirrors lighter-sdk's
# ``lighter.endpoint_profiles`` (test_sidecar.py checks the two agree) and
# ``known_chain_id`` in connector/src/lighter/signer.rs.
KNOWN_CHAIN_IDS = {
    "https://mainnet.zklighter.elliot.ai": 304,
    "https://testnet.zklighter.elliot.ai": 300,
    "https://api.rh.lighter.xyz": 466324,
    "https://api.rh-testnet.lighter.xyz": 300,
}


def normalize_url(url: str) -> str:
    return url.strip().rstrip("/")


def resolve_network(cfg_url, cfg_chain_id, network_url, expect_chain_id):
    """The ``(url, chain_id)`` to sign for. Raises ``ValueError`` on anything ambiguous.

    ``cfg_*`` come from the sidecar's own config, ``network_url`` and
    ``expect_chain_id`` from the connector's command line. Fail closed: no URL at
    all, two URLs that disagree, an unknown URL without a chain id, a chain id that
    contradicts a known URL, or a resolved chain id other than the expected one.
    """
    urls = {normalize_url(u) for u in (cfg_url, network_url) if u}
    if not urls:
        raise ValueError("no network: set `url` in the signer config or pass --network-url "
                         "(there is no default network)")
    if len(urls) > 1:
        raise ValueError(f"the signer config's url {cfg_url!r} and the connector's "
                         f"--network-url {network_url!r} name different networks")
    url = urls.pop()
    known = KNOWN_CHAIN_IDS.get(url)
    if cfg_chain_id is not None:
        chain_id = int(cfg_chain_id)
        if known is not None and chain_id != known:
            raise ValueError(f"chain_id {chain_id} contradicts {url}, whose chain id is {known}")
    elif known is not None:
        chain_id = known
    else:
        raise ValueError(f"unknown network {url}: set chain_id explicitly in the signer config")
    if expect_chain_id is not None and chain_id != int(expect_chain_id):
        raise ValueError(f"this signer would sign for chain {chain_id} on {url}, but the "
                         f"connector expects chain {int(expect_chain_id)}")
    return url, chain_id


# ---- flat-TOML config reader (py3.10 has no tomllib; lighter-sdk pulls no toml lib) --
def read_config(path: str) -> dict:
    """Read the flat signer config. Keys: url?, chain_id?, account_index,
    api_key_index, private_key (url/chain_id are resolved by resolve_network). A [[key]] table list is also accepted for several
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
    def __init__(self, cfg: dict, url: str, chain_id: int):
        """``url``/``chain_id`` come from :func:`resolve_network` — never a default."""
        self.signer = sc.get_signer()  # loads the platform-appropriate native library
        self.account_index = int(cfg["account_index"])
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
            # What this process signs for — the connector checks it (verify_identity).
            return {"ok": True, "chain_id": self.chain_id, "url": self.url,
                    "account_index": self.account_index, "api_key_slots": sorted(self.slots)}
        fn = {
            "create_order": self.create_order,
            "cancel_order": self.cancel_order,
            "cancel_all": self.cancel_all,
            "auth_token": self.auth_token,
        }.get(op)
        if fn is None:
            return {"ok": False, "error": f"unknown op: {op!r}"}
        return fn(req)


def parse_args(argv):
    parser = argparse.ArgumentParser(
        prog="sidecar.py", description="Lighter signer sidecar (line-delimited JSON on stdio).")
    parser.add_argument("config", nargs="?", default=os.environ.get("LIGHTER_SIGNER_CONFIG"),
                        help="the signer's own key config (or LIGHTER_SIGNER_CONFIG)")
    parser.add_argument("--network-url", default=None,
                        help="the REST base the connector dials; must agree with the config's url")
    parser.add_argument("--expect-chain-id", type=int, default=None,
                        help="refuse to start unless the resolved chain id is this one")
    return parser.parse_args(argv)


def main() -> int:
    args = parse_args(sys.argv[1:])
    path = args.config
    if not path:
        log("usage: sidecar.py <config.toml> [--network-url URL] [--expect-chain-id N]  "
            "(or set LIGHTER_SIGNER_CONFIG)")
        return 2
    try:
        cfg = read_config(path)
    except Exception as e:  # noqa: BLE001 — any config failure is fatal, fail closed
        log(f"fatal: cannot read config: {e}")
        return 2
    try:
        url, chain_id = resolve_network(cfg.get("url"), cfg.get("chain_id"),
                                        args.network_url, args.expect_chain_id)
    except ValueError as e:  # the wrong chain signs nothing the venue will take
        log(f"fatal: network: {e}")
        return 3
    try:
        signer = Signer(cfg, url, chain_id)
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
