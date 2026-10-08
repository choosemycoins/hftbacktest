#!/usr/bin/env python3
"""Tests for the signer sidecar's network resolution and identity handshake.

Offline. Uses the FICTIONAL golden key (0x1122…), never a real one. Run with the
sidecar's own interpreter (the venv that has lighter-sdk):

    <venv>/bin/python -m unittest connector/lighter-signer/test_sidecar.py

What this pins (design note §3.2; `lighter-rh` in connector/src/lighter/mod.rs):

* the chain id a transaction is hashed with comes from the NETWORK, never from a
  default — the sidecar used to infer 300 (testnet) for every URL that was not
  mainnet's, so on Robinhood Chain (466324) every transaction would have been
  refused while the auth token (which carries no chain id) kept the private stream
  looking healthy;
* the connector names the network it dials (``--network-url``) and the chain id it
  expects (``--expect-chain-id``); any disagreement refuses to start;
* ``ping`` reports what the sidecar signs for, so the connector can check it;
* end to end: a transaction signed by a sidecar on Robinhood Chain hashes with
  466324 — checked against a Go reference built from the same lighter-go revision as
  the shipped ``.so`` (skipped without a Go toolchain).
"""
import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import sidecar  # noqa: E402

RH = "https://api.rh.lighter.xyz"
RH_TESTNET = "https://api.rh-testnet.lighter.xyz"
MAINNET = "https://mainnet.zklighter.elliot.ai"
TESTNET = "https://testnet.zklighter.elliot.ai"

# The fictional golden key and slot the Go reference (pin/pin.go) is built with.
KEY = "0x11223344556677889900112233445566778899001122334455667788990011223344556677889900"
ACC, AKI = 1, 2
BASE = dict(market_index=1, client_order_index=123456789, base_amount=1000, price=6500000,
            is_ask=0, order_type=0, time_in_force=2, reduce_only=0, trigger_price=0,
            order_expiry=1800000000000)


class ResolveNetwork(unittest.TestCase):
    def test_robinhood_chain_resolves_to_466324(self):
        self.assertEqual(sidecar.resolve_network(None, None, RH, None), (RH, 466324))
        self.assertEqual(sidecar.resolve_network(RH, None, None, None), (RH, 466324))

    def test_every_known_network_has_its_own_chain_id(self):
        self.assertEqual(sidecar.resolve_network(None, None, MAINNET, None)[1], 304)
        self.assertEqual(sidecar.resolve_network(None, None, TESTNET, None)[1], 300)
        self.assertEqual(sidecar.resolve_network(None, None, RH_TESTNET, None)[1], 300)

    def test_there_is_no_default_network(self):
        # The old default was testnet: a config with no url signed for chain 300.
        with self.assertRaises(ValueError):
            sidecar.resolve_network(None, None, None, None)

    def test_an_unknown_url_needs_an_explicit_chain_id(self):
        with self.assertRaises(ValueError):
            sidecar.resolve_network("https://api.example.xyz", None, None, None)
        self.assertEqual(
            sidecar.resolve_network("https://api.example.xyz", 777, None, None),
            ("https://api.example.xyz", 777))

    def test_a_chain_id_that_contradicts_a_known_url_is_refused(self):
        with self.assertRaises(ValueError):
            sidecar.resolve_network(RH, 300, None, None)
        with self.assertRaises(ValueError):
            sidecar.resolve_network(MAINNET, 466324, None, None)

    def test_the_config_and_the_connector_must_name_the_same_network(self):
        with self.assertRaises(ValueError):
            sidecar.resolve_network(MAINNET, None, RH, None)
        # Agreement, trailing slash and all, is fine.
        self.assertEqual(sidecar.resolve_network(RH + "/", None, RH, None), (RH, 466324))

    def test_the_expected_chain_id_must_match(self):
        with self.assertRaises(ValueError):
            sidecar.resolve_network(None, None, TESTNET, 466324)
        self.assertEqual(sidecar.resolve_network(None, None, RH, 466324), (RH, 466324))

    def test_the_shipped_rh_example_signs_for_robinhood_chain(self):
        cfg = sidecar.read_config(os.path.join(HERE, "signer.rh.example.toml"))
        self.assertEqual(sidecar.resolve_network(cfg.get("url"), cfg.get("chain_id"), RH, 466324),
                         (RH, 466324))
        self.assertGreaterEqual(min(cfg["keys"]), 4, "slots 0-3 belong to the venue front-ends")

    def test_the_table_agrees_with_the_sdk_profiles(self):
        try:
            from lighter import endpoint_profiles as ep
        except ImportError:
            self.skipTest("this lighter-sdk has no endpoint_profiles")
        for profile in ep.ENDPOINT_PROFILES.values():
            self.assertEqual(sidecar.KNOWN_CHAIN_IDS[sidecar.normalize_url(profile.api_url)],
                             profile.chain_id, profile.name)


def _write_config(text):
    fd, path = tempfile.mkstemp(suffix=".toml")
    with os.fdopen(fd, "w") as fh:
        fh.write(text)
    os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
    return path


class Process(unittest.TestCase):
    """The real sidecar process, fictional key, no network traffic (CreateClient is local)."""

    def setUp(self):
        self.config = _write_config(
            f"account_index = {ACC}\napi_key_index = {AKI}\nprivate_key = \"{KEY}\"\n")

    def tearDown(self):
        os.unlink(self.config)

    def spawn(self, *args):
        return subprocess.Popen([sys.executable, os.path.join(HERE, "sidecar.py"), self.config, *args],
                                stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, text=True)

    def ask(self, proc, request):
        proc.stdin.write(json.dumps(request) + "\n")
        proc.stdin.flush()
        return json.loads(proc.stdout.readline())

    def test_ping_reports_the_network_the_sidecar_signs_for(self):
        proc = self.spawn("--network-url", RH, "--expect-chain-id", "466324")
        try:
            resp = self.ask(proc, {"id": 1, "op": "ping"})
        finally:
            proc.stdin.close(); proc.wait(timeout=30)
            proc.stdout.close(); proc.stderr.close()
        self.assertEqual(resp["id"], 1)
        self.assertTrue(resp["ok"])
        self.assertEqual(resp["chain_id"], 466324)
        self.assertEqual(resp["url"], RH)
        self.assertEqual(resp["account_index"], ACC)
        self.assertEqual(resp["api_key_slots"], [AKI])

    def test_a_chain_mismatch_refuses_to_start(self):
        proc = self.spawn("--network-url", RH, "--expect-chain-id", "300")
        proc.stdin.close()
        code = proc.wait(timeout=30)
        err = proc.stderr.read()
        proc.stdout.close(); proc.stderr.close()
        self.assertNotEqual(code, 0)
        self.assertIn("chain", err)
        self.assertNotIn("1122334455", err, "the key must never reach the log")

    def test_no_network_refuses_to_start(self):
        proc = self.spawn()
        proc.stdin.close()
        code = proc.wait(timeout=30)
        proc.stdout.close(); proc.stderr.close()
        self.assertNotEqual(code, 0)


PIN_DIR = os.path.join(HERE, "pin")


def _pin_binary():
    """Build the Go reference once; None when there is no Go toolchain."""
    if shutil.which("go") is None:
        return None
    env = dict(os.environ)
    env.setdefault("GOFLAGS", "-mod=mod")
    r = subprocess.run(["go", "build", "-o", "pin", "."], cwd=PIN_DIR, env=env,
                       capture_output=True, text=True, timeout=600)
    if r.returncode != 0:
        raise RuntimeError("go build of the reference failed:\n" + r.stderr[-800:])
    return os.path.join(PIN_DIR, "pin")


class ChainInTheHash(unittest.TestCase):
    """A Robinhood Chain signature is over a hash that carries 466324 — not 300, not 304."""

    def test_a_robinhood_chain_transaction_hashes_with_466324(self):
        pin = _pin_binary()
        if pin is None:
            self.skipTest("no Go toolchain: the exact reference needs one")
        config = _write_config(
            f"account_index = {ACC}\napi_key_index = {AKI}\nprivate_key = \"{KEY}\"\n")
        side = subprocess.Popen([sys.executable, os.path.join(HERE, "sidecar.py"), config,
                                 "--network-url", RH, "--expect-chain-id", "466324"],
                                stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, text=True)
        ref = subprocess.Popen([pin, "serve"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)

        def reference(ea, nonce, chain):
            ref.stdin.write(f"create {ea} {nonce} {chain}\n"); ref.stdin.flush()
            return ref.stdout.readline().strip()

        try:
            for i in range(5):
                side.stdin.write(json.dumps({"id": i, "op": "create_order", "api_key_index": AKI,
                                             "nonce": 42 + i, **BASE}) + "\n")
                side.stdin.flush()
                resp = json.loads(side.stdout.readline())
                self.assertTrue(resp["ok"], resp)
                info = json.loads(resp["tx_info"])
                ea, nonce = info["ExpiredAt"], info["Nonce"]
                self.assertEqual(reference(ea, nonce, 466324), resp["tx_hash"])
                self.assertNotEqual(reference(ea, nonce, 300), resp["tx_hash"])
                self.assertNotEqual(reference(ea, nonce, 304), resp["tx_hash"])
        finally:
            side.stdin.close(); side.wait(timeout=30)
            side.stdout.close(); side.stderr.close()
            ref.stdin.close(); ref.wait(timeout=30); ref.stdout.close()
            os.unlink(config)


if __name__ == "__main__":
    unittest.main()
