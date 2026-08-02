#!/usr/bin/env bash
#
# The whole Definition-of-Done check set, in one command.
#
# This exists because the routine `--lib --bins` pair has a blind spot that has now
# bitten twice, and both times a fully green routine run is what let the breakage land:
#
#   * `hftbacktest/examples/` are not built by `--lib --bins`. They are the only consumer
#     of the public API from *outside* the crate, so they are what catches a signature
#     change — and they went stale in 2026-07-30 and again in the money-path-invariants
#     tier (`AGENTS.md` §5).
#   * `py-hftbacktest/src/live.rs` sits behind the non-default `live` feature, so
#     `--workspace` does not compile it at all. Adding `BotError::Unsupported` broke three
#     exhaustive matches there under a green workspace run; `OrderId` broke eight more one
#     commit later (`AGENTS.md` §5, §4.7).
#
# Neither blind spot is a bug in the routine command — it is the fast one and stays. This
# is the one to run before calling a change done, and the one to run whenever the public
# API, `trait Bot`, `BotError` or `ElapseResult` moved.
#
# There is no CI in this repository (`AGENTS.md` §5): a local run is the only gate there is.

set -euo pipefail

cd "$(dirname "$0")/.."

echo "==> cargo check --workspace --all-targets   (examples: the public API from outside)"
cargo check --workspace --all-targets

echo "==> cargo check -p py-hftbacktest --features live   (not built by --workspace)"
cargo check -p py-hftbacktest --features live

echo "==> cargo test --workspace --lib --bins"
# `--test-threads=1` is not paranoia: two bybit subscribe-rejection pins capture `error!`
# through a thread-local `tracing` layer, and `tracing`'s callsite-interest cache is
# global, so a parallel test without a subscriber can switch the callsite off for them.
# Measured at 3 failures in 60 parallel runs, 0 in 60 serial ones (`AGENTS.md` §4.2). The
# flake is in the harness, not in the code under test — serialising is the cheap way to
# keep this script's signal honest until the harness gets one process-wide subscriber.
cargo test --workspace --lib --bins -- --test-threads=1

echo "==> cargo clippy --workspace --lib --bins"
# `-D warnings` is unattainable here (~89 pre-existing). The bar is "no new warnings in
# the files you touched" — this prints, it does not judge (`AGENTS.md` §5).
cargo clippy --workspace --lib --bins

echo
echo "All green. Not covered here and still on you: cargo +nightly fmt (nightly, not"
echo "stable — the rustfmt.toml options are nightly-only), and .venv/bin/pytest"
echo "collector/tools/ when the Python tooling moved."
