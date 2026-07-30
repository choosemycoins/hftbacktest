# Design Note: `LiveEvent::SnapshotComplete` marker

Status: Accepted — implemented on branch `feat/snapshot-marker`
Date: 2026-04-24
Scope: connector protocol (wire) + `hftbacktest` receiver API
Trigger: [`myhft/docs/task-brief-connector-snapshot-marker.md`](../../myhft/docs/task-brief-connector-snapshot-marker.md)

## Problem

A live bot that talks to the connector via iceoryx has no signal telling it
when the connector has finished delivering the initial state (existing open
orders, position, depth snapshot) for an instrument it just registered. The
connector already frames that initial state with `BatchStart` / `BatchEnd`
(`connector/src/main.rs:134–185`), but those same events are reused by
market-data streams to group feed batches, so the receiver cannot tell the
registration batch apart from any other batch.

The practical consequence: after a `myhft` restart the bot can start
submitting orders before the connector has delivered the existing open orders
from the exchange, creating duplicates on top of live orders left from the
previous process. This is the incident pattern documented in RFC-A v2 §4,
path 1.

## Contract

After a consumer receives `LiveEvent::SnapshotComplete { symbol,
snapshot_time_ns }` for the asset it registered, `Bot::position(asset_no)`
and `Bot::orders(asset_no)` reflect the connector's cached view of the
exchange state at some timestamp `T = snapshot_time_ns`. From that point on
it is safe to make submit/cancel decisions based on those values.

The contract deliberately says "the connector's cached view", not "the
exchange's state". See the known gap below.

## Design choices

### New `LiveEvent` variant, not reused `BatchStart`/`BatchEnd`

`BatchStart`/`BatchEnd` are already used by market-data streams
(`PublishEvent::BatchStart(TO_ALL)` in `connector/src/{binancefutures,
binancespot}/market_data_stream.rs`). The receiver side cannot tell a
registration batch from a feed batch after the fact. Reusing them would
require adding an asset/kind field, which breaks existing senders and
doesn't buy anything over a dedicated variant. A dedicated variant isolates
the concern and keeps backward reasoning local.

### Minimal payload: `symbol` + `snapshot_time_ns`

The brief §Q4 left `num_orders` / `position` as optional cross-check fields.
They were dropped: the receiver would have to maintain a parallel count to
validate, duplicating the authoritative state. If consistency drifts, the
primary defect is in the order/position stream, not in the marker — and the
marker would have to fail the snapshot or silently log, neither of which is
obviously right. YAGNI wins; add the fields only when a concrete failure
mode demands them. `snapshot_time_ns` stays because consumers need it to
reason about freshness (e.g. `myhft` startup gate logging).

### `snapshot_ready(asset_no) -> bool` on `Bot`, not a new `ElapseResult`

A method is trivial to poll in a startup gate:

```rust
while !hbt.snapshot_ready(0) {
    hbt.elapse(100_000_000)?;  // drain events; the marker will flip the flag
}
```

A new `ElapseResult::SnapshotReady` variant would force every consumer of
`elapse()` to handle a new case they don't care about. The flag shape also
composes naturally with multiple assets: wait until all are ready before
trading. If a push-style signal is later needed, it can be added without
breaking the poll-style API.

### `snapshot_ready` starts `false` and is per-asset

A freshly built `LiveBot` has every `Instrument::snapshot_ready = false`.
That is the gate a restarted `myhft` uses: if the marker has not yet
arrived, no orders go out. The flag does not reset on
`LiveEvent::Error(ConnectionInterrupted)` — see the reconnect discussion
below. In the current `LiveBot` design, `RegisterInstrument` is only sent
once, at `LiveBotBuilder::build()`; the flag is already `false` at that
point by construction. No explicit reset logic exists because no code path
sends `RegisterInstrument` a second time within a single `LiveBot`
lifetime.

### `Backtest::snapshot_ready(_) = true`

There is no registration-time snapshot phase in backtest, so the flag is
trivially `true`. This keeps the trait usable as a drop-in between live and
backtest callers.

## Wire format and routing

- `LiveEvent` is serialized via `bincode` with the standard config
  (`hftbacktest/src/live/ipc/iceoryx.rs:174, 203`).
- The new variant is appended to the end of the enum, so decoding of all
  existing variants stays byte-identical. A receiver built against an older
  `LiveEvent` would fail to decode the new variant — upgrade both ends
  together. There is no bot consuming this protocol other than `myhft`, so
  pair-upgrade is sufficient.
- Routing: `IceoryxUnifiedChannel::recv_timeout` dispatches `SnapshotComplete`
  by `symbol → inst_no` in the same arm as `Feed`/`Order`/`Position`
  (`hftbacktest/src/live/ipc/iceoryx.rs:354–360`), so the bot receives
  `(inst_no, ev)` with the correct asset index.

## Reconnect semantics

The Bybit connector wraps each stream (public/private/trade) in a `Retry`
with `ExponentialBackoff`. On disconnect the stream emits
`LiveEvent::Error(ConnectionInterrupted)` and reconnects. The connector
does **not** re-emit `PublishEvent::RegisterInstrument` for registered
symbols, so no new snapshot frame (and no new `SnapshotComplete`) is sent
on reconnect. `order_manager`'s in-memory state persists across a
reconnect.

Consequence: `snapshot_ready` stays `true` through connector-side
reconnects. That matches RFC-A v2 §4 path 1: resilience on
`ConnectionInterrupted` is the consumer's job — `myhft` responds with a
fatal exit and supervisor restart, which triggers a fresh `LiveBot::build()`
and a fresh `false → true` cycle for each asset. No connector-side reconnect
marker is required.

## Known gap: the marker can outrun the venue-side sweep

> Corrected 2026-07-30. An earlier revision of this section claimed the
> REST-prefetch path was commented out and `cancel_all`/`get_all_position`
> unimplemented. That described a dead alternative (the commented block in
> `bybit/mod.rs` referencing methods that never existed on `PrivateStream`);
> the working path lives in `bybit/private_stream.rs` and is **active**.

What the Bybit connector actually does about pre-existing exchange state
is a *policy*, and the policy is: **cancel everything and start clean.**
On the private stream's subscribe-ack it walks every registered symbol,
and on each later registration it repeats for that symbol
(`private_stream.rs`, both call sites): REST `cancel_all_orders` on the
venue, then `get_position`, whose result is published to the bot. A bot
restart therefore cannot duplicate a live grid — the old orders are swept
at the venue, not adopted. `SnapshotComplete` then correctly describes an
`order_manager` that holds no resting orders, because the venue was just
told to hold none either.

The real gap is a **race**, and it is load-bearing knowledge for any
consumer:

- both the sweep and the position fetch run inside `tokio::spawn` — the
  subscribe-ack handler and the registration path do not await them;
- `SnapshotComplete` is published by the publish task immediately after
  the registration's `BatchEnd`, synchronously with `RegisterInstrument`
  processing (`connector/src/main.rs`), and registration sends
  `PublishEvent::RegisterInstrument` *before* calling
  `connector.register(symbol)`.

So `snapshot_ready(asset_no) == true` guarantees the marker's formal
contract — the registration round-trip finished and the state batch was
flushed — but **not** that the REST round-trip to the venue has completed.
For a short window after the marker a consumer may still observe: fills
from a not-yet-swept order of a previous incarnation, cancel
acknowledgements for orders it never placed, and a `Position` update
arriving strictly after the marker. A strategy gating on `snapshot_ready`
must tolerate those, or wait additionally for the first `Position` event.
(Separately, the marker never promised a populated book — see AGENTS.md
§4.1.)

Possible follow-ups (not in scope of this change):

1. Sequence the marker after the sweep: plumb the spawned round-trip's
   completion back into the registration path so `SnapshotComplete` is
   only published once `cancel_all` + `get_position` for that symbol have
   returned. Closes the race at the cost of coupling marker latency to
   venue REST latency.
2. Failure loudness: today an error inside the spawned sweep is logged
   (`error!`) and the marker is published regardless; publishing a
   `LiveEvent::Error` on sweep failure would let the consumer's
   `error_handler` decide instead of trading on an unswept book.

Either approach does not change the wire contract defined here; it
strengthens its content.

## Consumer snippet

```rust
// Startup gate in myhft's grid loop. Waits until the connector has
// delivered the initial state for every asset before the strategy is
// allowed to place orders.
fn wait_for_snapshot<MD, B: Bot<MD>>(hbt: &mut B, poll_ns: i64) -> Result<(), B::Error>
where
    MD: hftbacktest::depth::MarketDepth,
{
    loop {
        let ready = (0..hbt.num_assets()).all(|i| hbt.snapshot_ready(i));
        if ready {
            return Ok(());
        }
        hbt.elapse(poll_ns)?;
    }
}
```

Consumers that want a timeout should call `elapse` with a bounded total
budget and fail closed (log + exit) if any asset is still not ready. That
timeout logic lives in the consumer, not in `hftbacktest` — keeping it
policy-free on the receiver side.

## Test coverage

`hftbacktest/src/live/bot.rs` tests with an in-memory `MockChannel`:

- `snapshot_ready_starts_false`
- `snapshot_complete_flips_ready`
- `snapshot_complete_is_per_asset`
- `orders_without_complete_leave_ready_false`
- `orders_then_complete_exposes_all_orders_at_ready_time`
- `empty_snapshot_still_signals_ready`
- `timeout_without_complete_keeps_ready_false`

## Files touched

- `hftbacktest/src/types.rs` — new `LiveEvent::SnapshotComplete` variant;
  new trait method `Bot::snapshot_ready`.
- `hftbacktest/src/live/mod.rs` — `snapshot_ready: bool` field on
  `Instrument`, default `false`.
- `hftbacktest/src/live/bot.rs` — `process_event` sets the flag;
  `LiveBot::snapshot_ready` impl; tests.
- `hftbacktest/src/live/ipc/iceoryx.rs` — route `SnapshotComplete` by
  symbol.
- `hftbacktest/src/backtest/mod.rs` — `Backtest::snapshot_ready` returns
  `true`.
- `connector/src/main.rs` — emit `SnapshotComplete` after the registration
  frame's `BatchEnd`.
