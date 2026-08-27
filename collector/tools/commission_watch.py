#!/usr/bin/env python3
"""Watches the maker commission Binance charges US, and says when it changes.

## Why this exists

The Binance USD-M **USDC** perpetuals have charged a maker commission of
exactly **0** since 2024-04-03. Confirmed for our own account on 2026-08-26
through `GET /fapi/v1/commissionRate` (USDT pays +2.0 bp on the same account),
and the venue calls it a promotion "until further notice" — no end date.

Every measurement we make on this class is conditioned on that zero. The
problem is finding out when it stops:

* since 2026-02 Binance has not published announcements for it. The fee page
  is edited **in place**, with no feed, no changelog and no date, so watching
  an announcement stream is not a weaker signal — it is no signal at all;
* the number is per account and per symbol. A tier change, a promo ending for
  one quote asset, or a change that applies to new accounts only are all
  invisible in anything public.

So the only instrument that answers the question for us is the signed endpoint
that states what WE are charged, and the only way not to learn about a change
after the fact is to ask on a timer and keep the answers.

## What it does

One signed request per watched symbol, one JSON line appended per poll —
`{"ts":…, "symbol":…, "maker":…, "taker":…}` — and a Telegram message the
first time a maker rate is not what the watch expects (`--expect-maker`,
default 0 for USDC symbols). The series is the point as much as the alarm: a
rate that moved and moved back would otherwise leave nothing behind.

USDT symbols are watched as a **control**, with their own expectation, so that
"the promo ended" and "our whole fee tier moved" are distinguishable, and so a
poll that silently starts answering nonsense is visible.

## What it does NOT do

It does not trade, cancel, or read positions, and it needs no permission
beyond Reading. The key is read from a file (mode 600, never in git) and never
logged, never written into the series, and never put in an alert.

Run it from a timer:

    commission_watch.py --out /path/to/series --symbols BTCUSDC:0 BTCUSDT:2

Exit code 0 when every watched symbol matched its expectation, 1 when one did
not (so `OnFailure=` sees it too), 2 when the check could not run at all.
"""

import argparse
import hashlib
import hmac
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

#: Basis points per unit of the rate the venue returns. `commissionRate`
#: answers `"0.00020000"` for the 2 bp USDT maker fee, i.e. a fraction of
#: notional; every number this tool prints is in basis points, because that is
#: the unit every measurement it guards is in.
BPS = 10_000.0

#: How long the alert stays quiet after firing for the same symbol. A fee
#: change is permanent until it is not: one message an hour for a week is how
#: an alert channel gets muted, which is the failure this exists to avoid.
ALERT_QUIET_S = 6 * 3600

#: Consecutive failed polls before the failure itself is worth a message. At an
#: hourly cadence this is six hours: past any maintenance window, and short
#: enough that a key that expired is not discovered by a backtest.
FAILURES_BEFORE_ALERT = 6

RECV_WINDOW_MS = 5000
REQUEST_TIMEOUT_S = 15


def read_credentials(path: Path) -> tuple:
    """`(api_key, secret, rest_base)` out of the TOML-shaped key file.

    Parsed by hand rather than with `tomllib` so this runs under any python3 a
    distribution ships (the same rule `quality_report.py` is written under: the
    box that records must not need a virtualenv). The file is three scalar
    assignments; anything cleverer in it is not ours.
    """
    values = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, raw = line.partition("=")
        values[key.strip()] = raw.strip().strip('"').strip("'")
    missing = [k for k in ("api_key", "secret") if not values.get(k)]
    if missing:
        raise ValueError(f"{path}: no {', '.join(missing)} in it")
    return (
        values["api_key"],
        values["secret"],
        values.get("rest_base", "https://fapi.binance.com").rstrip("/"),
    )


def signed_query(params: dict, secret: str) -> str:
    """Binance's signature: HMAC-SHA256 of the query string, appended to it.

    Order matters — the signature covers the string that is actually sent, so
    the query is built once and signed as text rather than re-encoded after.
    """
    query = urllib.parse.urlencode(params)
    signature = hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()
    return f"{query}&signature={signature}"


def fetch_commission(base: str, api_key: str, secret: str, symbol: str) -> dict:
    """`GET /fapi/v1/commissionRate` for one symbol. Weight 20."""
    query = signed_query(
        {
            "symbol": symbol,
            "recvWindow": RECV_WINDOW_MS,
            "timestamp": int(time.time() * 1000),
        },
        secret,
    )
    request = urllib.request.Request(
        f"{base}/fapi/v1/commissionRate?{query}",
        headers={"X-MBX-APIKEY": api_key, "Accept": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_S) as response:
        return json.loads(response.read().decode())


def parse_watch(spec: str) -> tuple:
    """`SYMBOL[:EXPECTED_MAKER_BPS]` -> `(symbol, expected_bps)`."""
    symbol, _, expected = spec.partition(":")
    return symbol.upper(), (float(expected) if expected else 0.0)


def reading(record: dict) -> dict:
    """The venue's answer as basis points, with its own strings kept.

    Both are stored: the floats are what the check reads, the strings are what
    the venue actually said, and a series that only kept the floats could not
    answer later whether a change was in the number or in our parsing of it.
    """
    maker = float(record["makerCommissionRate"])
    taker = float(record["takerCommissionRate"])
    return {
        "maker_bps": maker * BPS,
        "taker_bps": taker * BPS,
        "maker_raw": record["makerCommissionRate"],
        "taker_raw": record["takerCommissionRate"],
    }


def verdict(readings: dict, expectations: dict, tolerance_bps: float = 1e-9) -> list:
    """Every symbol whose maker rate is not what the watch expects.

    A list rather than a bool: which symbol moved is the whole content of the
    finding. USDC going to +2 and the USDT control going with it says the tier
    moved; USDC alone says the promotion ended.
    """
    changed = []
    for symbol, expected in sorted(expectations.items()):
        got = readings.get(symbol)
        if got is None:
            continue
        if abs(got["maker_bps"] - expected) > tolerance_bps:
            changed.append((symbol, expected, got["maker_bps"]))
    return changed


def load_state(path: Path) -> dict:
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return {}


def save_state(path: Path, state: dict) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=1, sort_keys=True))
    tmp.replace(path)


def due_to_alert(state: dict, key: str, now: float, quiet_s: float = ALERT_QUIET_S) -> bool:
    """Whether this alert key has been quiet long enough to speak again."""
    return now - float(state.get("last_alert", {}).get(key, 0.0)) >= quiet_s


def mark_alerted(state: dict, key: str, now: float) -> None:
    state.setdefault("last_alert", {})[key] = now


def telegram(text: str, token: str, chat_id: str) -> bool:
    """Best effort: a failed alert must not stop the series being written."""
    payload = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode()
    try:
        with urllib.request.urlopen(
            urllib.request.Request(
                f"https://api.telegram.org/bot{token}/sendMessage", data=payload
            ),
            timeout=REQUEST_TIMEOUT_S,
        ) as response:
            return response.status == 200
    except Exception as error:  # noqa: BLE001 - the reason goes to the journal
        print(f"commission-watch: telegram failed: {type(error).__name__}", file=sys.stderr)
        return False


def alert_text(changed: list, host: str) -> str:
    lines = ["🔴 Binance maker commission CHANGED", f"host {host}"]
    for symbol, expected, got in changed:
        lines.append(f"{symbol}: maker {got:+.3f} bps, expected {expected:+.3f}")
    lines.append("")
    lines.append(
        "Every USDC-perp measurement assumes maker = 0. Re-read "
        "/home/storage/binance-usdc/WHY-ZERO-FEE.md before trading on it."
    )
    return "\n".join(lines)


def run(args, fetch=fetch_commission, now=None, send=telegram) -> int:
    """One poll of every watched symbol. `fetch`/`send`/`now` are parameters so
    the policy can be tested without the venue, a key, or a chat."""
    now = time.time() if now is None else now
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    state_path = out_dir / "state.json"
    state = load_state(state_path)

    expectations = dict(parse_watch(spec) for spec in args.symbols)

    try:
        api_key, secret, base = read_credentials(Path(args.credentials).expanduser())
    except (OSError, ValueError) as error:
        print(f"commission-watch: {error}", file=sys.stderr)
        return 2

    readings, errors = {}, {}
    for symbol in expectations:
        try:
            readings[symbol] = reading(fetch(base, api_key, secret, symbol))
        except Exception as error:  # noqa: BLE001 - one bad symbol is not the run
            # The reason, never the request: a signed URL carries the key.
            errors[symbol] = f"{type(error).__name__}: {error}"

    stamp = datetime.fromtimestamp(now, timezone.utc)
    series = out_dir / f"commission_{stamp:%Y%m}.jsonl"
    with open(series, "a") as f:
        for symbol, got in sorted(readings.items()):
            f.write(
                json.dumps(
                    {
                        "ts": stamp.isoformat().replace("+00:00", "Z"),
                        "symbol": symbol,
                        "expected_maker_bps": expectations[symbol],
                        **got,
                    },
                    sort_keys=True,
                )
                + "\n"
            )
        for symbol, error in sorted(errors.items()):
            f.write(
                json.dumps(
                    {
                        "ts": stamp.isoformat().replace("+00:00", "Z"),
                        "symbol": symbol,
                        "error": error,
                    },
                    sort_keys=True,
                )
                + "\n"
            )

    token = os.environ.get("TG_BOT_TOKEN", "")
    chat_id = os.environ.get("TG_CHAT_ID", "")
    host = os.uname().nodename

    changed = verdict(readings, expectations)
    for symbol, expected, got in changed:
        print(f"commission-watch: {symbol} maker {got:+.4f} bps, expected {expected:+.4f}")
    if changed and token and chat_id and due_to_alert(state, "changed", now):
        if send(alert_text(changed, host), token, chat_id):
            mark_alerted(state, "changed", now)

    if errors:
        state["consecutive_failures"] = int(state.get("consecutive_failures", 0)) + 1
        for symbol, error in sorted(errors.items()):
            print(f"commission-watch: {symbol}: {error}", file=sys.stderr)
    else:
        state["consecutive_failures"] = 0

    if (
        state["consecutive_failures"] >= FAILURES_BEFORE_ALERT
        and token
        and chat_id
        and due_to_alert(state, "failing", now)
    ):
        if send(
            f"🟠 commission-watch on {host} has failed "
            f"{state['consecutive_failures']} polls in a row; the maker-fee "
            f"series has a hole and the zero-fee assumption is unwatched.",
            token,
            chat_id,
        ):
            mark_alerted(state, "failing", now)

    state["last_poll"] = stamp.isoformat().replace("+00:00", "Z")
    state["last_readings"] = {s: r["maker_bps"] for s, r in sorted(readings.items())}
    save_state(state_path, state)

    if changed:
        return 1
    return 2 if not readings else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="commission_watch.py",
        description="Poll our own maker/taker commission and alarm when it moves.",
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        required=True,
        metavar="SYMBOL[:EXPECTED_MAKER_BPS]",
        help="Symbols to watch and what each is expected to charge, in basis "
        "points (default 0). e.g. BTCUSDC ZECUSDC BTCUSDT:2",
    )
    parser.add_argument("--out", required=True, help="Directory for the series and the state.")
    parser.add_argument(
        "--credentials",
        default="~/.config/eventshort/binanceusdm.toml",
        help="File holding api_key/secret/rest_base (mode 600, never in git).",
    )
    return parser


def main(argv=None) -> int:
    return run(build_parser().parse_args(argv))


if __name__ == "__main__":
    sys.exit(main())
