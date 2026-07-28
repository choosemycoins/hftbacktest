# Design: Hyperliquid connector

Status: **Phase 1 (public market data) implemented**; Phases 2–5 still draft
Date: 2026-07-25, Phase 1 built and measured 2026-07-28
Scope: a new `hyperliquid` backend in the `connector/` crate (market data + order management)
Related: [`snapshot-complete-marker.md`](snapshot-complete-marker.md), `AGENTS.md` §4 (codebase traps)

**What exists in the tree today** is `connector/src/hyperliquid/`: config, universe
resolution, the public WebSocket, snapshot→delta synthesis, the trade-replay guard, and the
reconnect policy. It holds no key and **rejects every order** with `ErrorKind::OrderError`.
§10 records what the build changed about this document; read it before trusting §5.2's
first formulation.

API facts in this document were verified on **2026-07-25**, several by live measurement
against mainnet. Hyperliquid's docs were stale in at least four places at that date, so
treat every number here as perishable and re-measure before implementation. Statements
that could not be confirmed are marked **UNVERIFIED**.

---

## 1. Why this is not a routine connector

The three existing backends (`bybit`, `binancefutures`, `binancespot`) all share a shape:
a venue publishes **incremental order-book deltas**, the connector reshapes them into
`LiveEvent::Feed` events, and `LiveBot` applies them to its depth. Hyperliquid breaks
that shape at the first step, and the break is not cosmetic.

**Hyperliquid has no incremental depth channel on the public API.** `l2Book` delivers a
complete top-N snapshot on every message. There is no sequence number, no `U`/`u` range,
no checksum, and nothing to detect a gap with — because there are no gaps to detect, only
snapshots that supersede each other. Hyperliquid's own guidance is that automated traders
should run a non-validating node for real-time update streams.

That collides with a hard property of this repository, documented in `AGENTS.md` §4.1 and
re-confirmed independently during this research:

`Event::is()` (`hftbacktest/src/types.rs:355`) requires the low byte to match exactly, and
`LiveBot::process_event` (`hftbacktest/src/live/bot.rs:213–225`) only ever tests
`LOCAL_BID_DEPTH_EVENT` / `LOCAL_ASK_DEPTH_EVENT`, whose kind is `DEPTH_EVENT = 1`.
Events of kind `DEPTH_SNAPSHOT_EVENT = 4`, `DEPTH_CLEAR_EVENT = 3` and
`DEPTH_BBO_EVENT = 5` are **silently discarded by the bot**.

So the natural encoding — "it's a snapshot, send `DEPTH_SNAPSHOT_EVENT`" — produces a bot
whose order book stays permanently empty, with no error anywhere. The connector has no
choice: **it must synthesise incremental deltas itself and emit only kind-1 events.**

Everything else in this document follows from that, plus two secondary mismatches:
Hyperliquid has no fixed tick size (§5.3), and its rate limiting is denominated in traded
volume rather than time (§5.9).

---

## 2. Scope

**In scope (v1)**
- Perpetuals only.
- Market data: depth and trades, from the public WebSocket.
- Order management: submit, cancel, and the order/position event stream.
- The `Connector` + `ConnectorBuilder` traits as they exist today
  (`connector/src/connector.rs`).

**Out of scope (v1)**
- Spot. Spot has balances, not positions, so `LiveEvent::Position { qty }` has no honest
  meaning there. Adding it later is additive.
- `l4Book` via a self-hosted node (§5.1) — designed for, not built.
- Modify/amend. `Connector` has no `modify` method and `LiveBot::modify` is `todo!()`
  (`live/bot.rs:558`); adding amend support is a separate cross-cutting change.
- Builder codes, vaults, sub-accounts, TWAP orders.

---

## 3. What the connector must satisfy

From `connector/src/connector.rs`:

```rust
pub trait Connector {
    fn register(&mut self, symbol: String);
    fn order_manager(&self) -> Arc<Mutex<dyn GetOrders + Send + 'static>>;
    fn run(&mut self, tx: UnboundedSender<PublishEvent>);
    fn submit(&self, symbol: String, order: Order, tx: UnboundedSender<PublishEvent>);
    fn cancel(&self, symbol: String, order: Order, tx: UnboundedSender<PublishEvent>);
}
```

`run`, `submit` and `cancel` must not block; everything comes back asynchronously as
`PublishEvent`. `register` can be called at an arbitrary time, whenever a bot registers an
instrument.

Two repository-wide rules apply and are easy to violate:

- **Emit only kind-1 depth events** (§1).
- **Re-subscribe after every reconnect.** `AGENTS.md` §4.2 documents an upstream bug the
  other three backends all share: the symbol list is broadcast exactly once over a
  `tokio::sync::broadcast`, and each reconnect creates a fresh receiver that never sees it,
  so a reconnected public stream is connected but subscribed to nothing. The Hyperliquid
  backend must hold the `SharedSymbolSet` and re-subscribe from it on every connect.

  **There is no existing precedent for this in the repo, and the two halves must be
  composed by hand.** Bybit's private stream re-*reads* the shared set on connect
  (`bybit/private_stream.rs:100–108`) but only to drive per-symbol REST calls — the
  subscribe frame it sends is a fixed, account-wide `["order","position","execution"]`
  (`:80–93`) with no symbol in it. Symbol→subscribe-frame construction exists separately,
  in the *public* stream (`bybit/public_stream.rs:198–210`), but is fed from the
  broadcast receiver rather than the set — which is exactly the bug. Hyperliquid will be
  the first stream here that builds subscribe frames from the shared set on reconnect, so
  it needs its own testnet rehearsal (§7).

---

## 4. Hyperliquid surface, in brief

Endpoints: `wss://api.hyperliquid.xyz/ws`, `https://api.hyperliquid.xyz` (`POST /info`,
`POST /exchange`); testnet at `…hyperliquid-testnet.xyz`.

Feeds relevant here, with measured cadence on BTC (2026-07-25, mainnet):

| Feed | Content | Median gap | Note |
|---|---|---|---|
| `bbo` | best bid/ask, `[bid\|null, ask\|null]` | **0.21 s** | event-driven; either side may be `null` |
| `l2Book` `fast:true` | 5 levels/side, full snapshot | **0.54 s** | |
| `l2Book` default | 20 levels/side, full snapshot | **5.39 s** | docs still claim 0.5 s — stale |
| `trades` | array, aggressor side, `tid` | 0.65 s | |
| `orderUpdates` | order lifecycle | event | authoritative for status |
| `userFills` | executions | event | sends `isSnapshot:true` batch on subscribe |

Operational facts that shape the design:

- Liveness is an **application-level** `{"method":"ping"}` → `{"channel":"pong"}` on a
  <60 s interval, not RFC6455 ping frames. The existing collector code
  (`collector/src/hyperliquid/http.rs:37–52`) already does this correctly.
- **Subscribing to an unknown coin silently closes the whole connection**, taking every
  other subscription with it, with no error frame. Coins must be validated against `meta`
  before subscribing.
- An unknown subscription *type* does produce `{"channel":"error", …}` and the connection
  survives. Two different failure modes for one class of mistake.
- `webData2` no longer exists on mainnet (use `webData3`); `userEvents` replies on channel
  **`"user"`**, not `"userEvents"`.
- Resting orders are on-chain and survive disconnects, restarts and API-wallet expiry.
  There is no cancel-on-disconnect.

---

## 5. Design decisions

### 5.1 Market-data source is pluggable; v1 uses the public WebSocket

**Decision.** Define the market-data producer behind an internal trait with one v1
implementation (`PublicWsSource`) and a documented second (`L4NodeSource`).

Hyperliquid publishes [`order_book_server`](https://github.com/hyperliquid-dex/order_book_server),
which consumes a non-validating node and serves genuine per-block order diffs (`l4Book`:
`New{sz, insertBefore}` / `Update{origSz,newSz}` / `Remove` per order). That maps almost
exactly onto hftbacktest's L3 model and is the only path that yields true queue position.
It also serves `l2book` with `n_levels` up to 100, against the public feed's 20.

**What the node path actually costs** (from
[`hyperliquid-dex/node`](https://github.com/hyperliquid-dex/node), read 2026-07-25):

| | |
|---|---|
| Machine | 16 vCPU, 128 GB RAM, 500 GB SSD at 500 MB/s |
| OS | *"Currently only Ubuntu 24.04 is supported"* |
| Network | ports 4001 and 4002 open to the public, for gossip |
| Admission | permissionless — no staking, no allowlisting |
| Location | *"For lowest latency, run the node in Tokyo, Japan"* |
| Mainnet | needs at least one seed peer IP in `~/override_gossip_config.json` |

The optimizing-latency page states 32 logical cores where the node README states 16
vCPU; size for the larger.

**The operational cost is disk, not compute.** *"The network will generate around 100 GB
of logs per day, so it is recommended to archive or delete old files."* The stated 500 GB
lasts five days unpruned, so log rotation or S3 offload is part of the build, not an
afterthought. For scale: the collector records ~22 MB/day/symbol from the public feed.

Run with `--batch-by-block --write-fills --write-order-statuses --write-raw-book-diffs
--serve-info`; output lands under `~/hl/data/`.

**Caveats that are not negotiable.** `order_book_server` does not support spot order
books, does not show untriggered trigger orders, batches by block (milliseconds of added
latency), exits after 5 seconds without node events or on state divergence, and is
explicitly *"a standalone educational project"* with *"no commitment… to maintain, update,
or fix any issues"*. Adopting it means owning it. **UNVERIFIED:** whether the `hl-visor`
binary is published for arm64 — assume x86_64 until confirmed, since it decides the
instance family.

Order-of-magnitude: a `r7i.4xlarge`-class host plus provisioned disk throughput is roughly
two orders of magnitude more expensive per month than the `t4g.small` the public-feed
collector runs on.

v1 should not require running a node. But the seam costs almost nothing now and is
expensive to retrofit, and the accuracy ceiling of the public feed is low enough (§5.2)
that anyone serious will want the node path.

*Rejected:* node-only v1 — too high a barrier to first use.
*Rejected:* public-only with no seam — the diff-synthesis code in §5.2 is exactly the code
that becomes dead weight later; keeping it behind a trait makes its removal a config change.

### 5.2 Snapshot→delta synthesis, with a strict window policy

**Decision.** Maintain a per-coin mirror of the last observed book. On each `l2Book`
message, diff against the mirror and emit `LOCAL_BID_DEPTH_EVENT` /
`LOCAL_ASK_DEPTH_EVENT` (kind 1) per changed level, with `qty: 0.0` for a level that
disappeared. Wrap each snapshot's derived events in
`PublishEvent::BatchStart(TO_ALL)` / `BatchEnd(TO_ALL)`.

This is the same shape the Binance backend already uses when it replays a REST snapshot
(`connector/src/binancefutures/market_data_stream.rs:197,215`), so there is precedent to
follow rather than invent.

**The truncation trap, and the policy that avoids it.** Only the top *N* levels are
visible. A level that leaves the window is indistinguishable from a level that was
cancelled. Naively emitting `qty: 0.0` for everything absent from the new snapshot
corrupts the book on every volatile tick — the bot would see depth evaporate and reappear.

Policy as first written here: *the mirror is authoritative only inside the observed
window*, emitting deletions **only** for mirrored levels inside `[deepest_bid, best_bid]`
(resp. `[best_ask, deepest_ask]`) and leaving levels beyond the window untouched.

> **Superseded by the build — see §10.1.** That rule is incomplete in a way that breaks the
> book: it says nothing about mirrored levels **above** the new best bid, which is exactly
> where a falling touch leaves them. Under the rule as written, a bid at 100 that the
> market has left behind stays in the bot's book for ever while the venue's best bid is 95
> — a permanently crossed book. The implemented rule deletes every mirrored level absent
> from the snapshot, including truncation; §10.1 has the reasoning and the alternative it
> was weighed against.

Either way, the accuracy limit is real and belongs in the connector's docs rather than
being treated as an implementation detail: **with the public feed, only the top N levels
are trustworthy.**

**Feed selection.** Subscribe to `bbo` *and* `l2Book fast:true`:

- `bbo` at ~0.2 s is the only feed fast enough to quote on. Convert it into kind-1 depth
  events for the two touch levels — **not** `DEPTH_BBO_EVENT`, which the bot drops.
- `l2Book fast:true` at ~0.5 s supplies the 5 levels of context and corrects the mirror.

Default `l2Book` at ~5 s is a reconciliation feed at best; do not build on it.

**Crossing: measured, and answered offline.** The question was whether interleaving `bbo`
and `l2Book` into one mirror can transiently cross the book. It can, and often: on
`btc_20260727`, **~4.6 % of `bbo` frames** (30 410 bids, 30 700 asks of 655 873) arrive
through the last `l2Book fast` snapshot's touch — the mirror is up to ~0.54 s stale, which
is many ticks.

The offline converter now implements this fusion (`hyperliquid.convert`,
`book_mode='bbo+fast'`; Phase 5b of
[`design-multi-venue-collection.md`](design-multi-venue-collection.md)), and the rule that
resolves it is worth reusing verbatim: **`bbo` is authoritative about the touch.** Delete
every mirrored bid above the new best bid and every mirrored ask below the new best ask,
emit those deletions *before* the new touch, and the book is uncrossed after every
individual event rather than only at the end of a batch. Verified by replaying a whole
converted day row by row: 1 764 199 depth rows, zero crossings, on both the exchange and
the local ordering.

Two supporting measurements from the same day: `bbo` never sent a locked or crossed quote
of its own (655 873/655 873 had bid < ask), and never sent a `null` side — though the
field is typed nullable, so a null must be treated as "no news about that side", not as an
empty one.

The connector-side decision is still open: reuse the existing per-symbol
`FusedHashMapMarketDepth` in `main.rs`, or port the converter's mirror. Note that fusion's
own generated deletion events lack the `LOCAL_EVENT` bit (`AGENTS.md` §4.7), which would
have to be fixed for that path to work — the Python converter sidesteps it because
`correct_event_order` stamps both bits on every row on the way out, and nothing equivalent
runs live.

### 5.3 Register the finest tick; enforce the significant-figure rule at submit

**Decision.** `RegisterInstrument` gets `tick_size = 10^-(MAX_DECIMALS - szDecimals)`
(with `MAX_DECIMALS = 6` for perps) and `lot_size = 10^-szDecimals`. The 5-significant-figure
constraint is enforced only when formatting an outgoing order price.

Hyperliquid has **no tick size field**. Prices are legal if they have at most 5 significant
figures *and* at most `MAX_DECIMALS - szDecimals` decimal places; integer prices are always
legal. The effective tick is therefore **price-dependent**: 1.0 at BTC ≈ 64,000, but 0.1 at
9,999.

hftbacktest's depth is integer-ticked throughout, and `tick_size` is fixed at registration.
There is no way to express a price-dependent tick in the current model.

*Rejected:* pick the tick for the current price band and re-register on band crossing.
Re-registration is not supported (`LiveBotBuilder::build` sends `RegisterInstrument` exactly
once, `AGENTS.md` §4.4), and a stale tick would silently reject every order at the worst
possible moment.

The cost of the chosen option: the bot's price grid is finer than what the venue accepts,
so the connector must round at submit time and the bot's intended price and the resting
price can differ by up to one venue tick. That must be surfaced — the resulting
`LiveEvent::Order` carries the **accepted** price, so the bot's state stays truthful even
when its request was adjusted.

**This is the largest impedance mismatch between Hyperliquid and this codebase and should
be explicitly signed off before implementation starts.**

### 5.4 Order identity: a `cloid` with an embedded prefix

**Decision.** Generate `cloid = "0x" + 32 hex chars`, where the first 4 hex chars are a
configured prefix (`order_prefix`) and the remaining 28 are random. Keep the authoritative
mapping in a local `HashMap<Cloid, OrderExt>` exactly as
`connector/src/bybit/ordermanager.rs` does with `order_link_id`.

Bybit's `orderLinkId` is a free-form string, so the existing code can do
`order_link_id.starts_with(&self.prefix)` to ignore orders belonging to other bots or to a
human trader on the same account. Hyperliquid's `cloid` is a fixed 128-bit hex value —
exactly 32 hex chars, no more, no less — so a readable prefix is not available.

Encoding the prefix in the high nibbles preserves the property that matters: an order
arriving on `orderUpdates` whose `cloid` prefix does not match is **not ours** and must be
ignored rather than treated as an unknown-order error.

Orders placed without a `cloid` (by a human, or by another system) surface with
`cloid: null` and are likewise ignored.

### 5.5 `orderUpdates` is authoritative; unknown statuses are terminal

**Decision.** Subscribe to `orderUpdates` + `userFills`. Use `userEvents` (channel
`"user"`) only for `liquidation` and `nonUserCancel`, which have no other stream.

`orderUpdates` carries ~30 documented status strings — `open`, `filled`, `canceled`,
`rejected`, `marginCanceled`, `badAloPxRejected`, `reduceOnlyRejected`, … — and the set
**grows over time**. Hummingbot shipped a production bug precisely because it hardcoded the
list and Hyperliquid later added `perpMarginRejected`.

Parse into a known enum with a catch-all `Unknown(String)` arm, and **treat `Unknown` as
terminal**: log it loudly, mark the order `Status::Expired`, drop it from the order manager.
Per `AGENTS.md` §1.1, an order in an unrecognised state must fail closed — assuming it is
still resting is how a bot ends up quoting against orders that no longer exist.

Fills come from `userFills`, which sends an `isSnapshot: true` batch on subscribe and
increments thereafter. That snapshot boundary maps directly onto
`LiveEvent::SnapshotComplete` (§5.10).

### 5.6 Orders over REST in v1; the WS `post` transport behind a config flag

**Decision.** `POST /exchange` in v1. Add `ws_api_url` later as a config switch, mirroring
`connector/src/binancespot`'s existing `ws_api_url` pattern.

WS `post` avoids a TLS/TCP handshake per order and shares the market-data connection, so
order/feed ordering is observable. Against it: max 100 inflight posts, manual `id`
correlation, and the 2000-messages-per-minute cap is shared with subscribe traffic.

The decisive point is that **the signed payload is byte-identical either way**, so the
transport is genuinely a late-binding choice and building REST first costs nothing.

### 5.7 Signing: EIP-712 phantom agent over a msgpack action hash

L1 actions (`order`, `cancel`, `modify`, `scheduleCancel`) are signed as:

```
hash   = keccak( msgpack(action)
                 ‖ nonce:u64be
                 ‖ 0x00               (or 0x01 ‖ vault_address)
                 ‖ [ 0x00 ‖ expires_after:u64be ]   if expires_after is set )
agent  = { source: "a" (mainnet) | "b" (testnet), connectionId: hash }
sig    = EIP712( domain{ name:"Exchange", version:"1", chainId:1337,
                         verifyingContract: 0x0 }, Agent, agent )
```

Three footguns, all silent failures, all worth a test each:

1. **`chainId` is 1337 for both networks.** Mainnet vs testnet is encoded *only* in
   `source`. Getting it wrong yields a well-formed signature that recovers to the wrong
   address — `{"status":"err","response":"Unable to recover signer."}`.
2. **The hash is over msgpack**, so struct field order is part of the protocol: `a,b,p,s,r,t,c`
   for an order, `type,orders,grouping` for the action. Rust struct definition order must
   match the SDK's; a `#[serde(...)]` reordering breaks signing with no compile error.
3. **Numbers are strings with trailing zeros stripped** — `"50000"`, never `"50000.0"`.
   A trailing zero changes the msgpack bytes and therefore the hash.
   Also: the `f` flag on `cancel` must be **omitted** when false, not serialised as `false`.

**Key management.** Hyperliquid's API wallet ("agent wallet") signs on behalf of a master
account. The asymmetry that catches people: **actions are signed by the agent key, but all
state queries must use the master address** — `clearinghouseState`, `openOrders`,
`userFills` keyed by the agent address return nothing. The config therefore needs both:

```toml
# connector/examples/hyperliquid.toml
public_url      = "wss://api.hyperliquid.xyz/ws"
rest_url        = "https://api.hyperliquid.xyz"
is_mainnet      = true          # selects phantom-agent source "a" / "b"
account_address = "0x..."       # master account — used for ALL info queries
agent_key       = "0x..."       # API wallet private key — signs actions only
order_prefix    = "a1b2"        # 4 hex chars, embedded in cloid (§5.4)
```

New dependencies are required and none are in the tree today: `rmp-serde` for msgpack, and
keccak + secp256k1 EIP-712 signing (`alloy`, or `k256` + `sha3`). `connector/src/utils.rs`
currently provides only HMAC-SHA256 and Ed25519.

### 5.8 Nonces: a monotonic counter, not a wall clock

**Decision.** Seed a per-signer `AtomicU64` from `now_ms()` at startup; each action takes
`max(now_ms(), last + 1)`.

The chain retains the **100 highest nonces per signer address**. A new nonce must exceed
the smallest retained one and must never repeat. The valid window is generous
(`T - 2 days .. T + 1 day`), so modest clock skew is survivable — but raw `now_ms()` breaks
in two ways: a burst of more than 100 orders inside one millisecond collides, and a clock
that steps backwards across a restart replays into the retained set.

Corollary: **one API wallet per connector process.** The nonce set is per signer, so two
processes sharing a key will fight, and the loser's orders are rejected.

### 5.9 Rate limiting is denominated in traded volume — budget it explicitly

This is the constraint most likely to break a grid strategy, and it has no analogue on
Bybit or Binance.

- IP-based: 1200 weight/minute. An exchange action costs `1 + floor(batch_len / 40)`.
- **Address-based: 1 request per 1 USDC of cumulative volume traded**, with an initial
  buffer of 10,000 requests. When exhausted: **one request per 10 seconds.**
- Cancels get a higher ceiling: `min(limit + 100000, limit * 2)`.
- Open-order cap: 1,000 (rising with volume, max 5,000).

A market-making bot that quotes and re-quotes without filling **consumes budget it never
replenishes**. Ten thousand requests at even a modest re-quote rate is hours, not days,
after which the connector is throttled to 0.1 requests/second — which for a live quoting
bot is indistinguishable from being down.

Design consequences:
1. Prefer `batchModify` over cancel-then-place: one request instead of two, and the whole
   batch costs one address-limit unit per order but one IP-weight unit per 40.
2. The connector must **track its own request budget** and publish a
   `LiveEvent::Error(ErrorKind::CriticalConnectionError)` — or a dedicated kind — when it
   crosses a configured floor, so the bot can fail closed rather than discover the limit as
   a wall of 429s.
3. Throttling surfaces as HTTP **429**. Match on the status code; the body text is
   documented only in prose and is **UNVERIFIED**.
4. Malformed bodies return HTTP **422** with a **plain-text** body
   (`Failed to deserialize the JSON body into the target type`), not JSON. A strict
   `serde_json` error path will itself fail on it.

### 5.10 Reconnect: re-subscribe, then re-query, then mark the snapshot

**Decision.** On every (re)connect: re-subscribe every registered coin from the
`SharedSymbolSet`, re-query `openOrders` and `clearinghouseState` via REST, and reconcile
the order manager.

**This gives the bot no post-reconnect gate, and the design must not pretend otherwise.**
`SnapshotComplete` is emitted by the shared publish task synchronously after the
registration `BatchEnd` (`connector/src/main.rs:186–196`), and `run_receive_task` sends
`PublishEvent::RegisterInstrument` *before* calling `connector.register(symbol)`
(`main.rs:96–105`) — so the marker fires before this backend has even subscribed. Worse,
`snapshot_ready` is a write-once latch (`live/bot.rs:283–284`, no reset anywhere), so a
second marker after a reconnect changes nothing observable. Both facts are the
pre-existing trap in `AGENTS.md` §4.4; Hyperliquid inherits it rather than introducing it.

Consequence: the reconciliation below makes the *connector's* view correct, and because a
connector process starts before any bot registers, a cold start does produce an honest
marker. But **after a reconnect the bot has no signal at all** — fail-closed on
`ConnectionInterrupted` remains the consumer's job (supervisor restart), exactly as
RFC-A v2 specifies. Making the marker reconnect-aware needs a `snapshot_ready` reset and a
change to when `main.rs` emits it; that is a protocol change to be scoped separately, not
something Phase 4 can deliver on the current wire format.

No *order state* is replayed on reconnect. `userFills` re-sends its `isSnapshot` batch and
`l2Book` sends a fresh snapshot on the next tick, but **`orderUpdates` gaps are
unrecoverable from the stream** — the only way back to a correct view is a REST query.

The public `trades` channel is the opposite case and needs a guard. Measured on
2026-07-27 and checked against HL's own `candleSnapshot` to the unit: every (re)subscribe
replays **the last 30 fills of the coin in one frame**, each carrying the same `tid` as the
original. One day with 10 resubscribes injected 223 duplicate fills for BTC and 299 for ENA
(+0.066% and +1.36% of trade rows). A backend that forwards `trades` straight through would
push those into `LiveBot`'s `last_trades` as genuine `TRADE_EVENT`s after every blip, so it
must drop a fill whose `tid` it has already emitted — a bounded per-coin ring of recent ids
is enough, since the replay never reaches further back than 30. The offline converter now
does exactly this (`py-hftbacktest/hftbacktest/data/utils/hyperliquid.py`,
`TRADE_TID_HISTORY`); the live backend should reuse the same rule rather than invent one.

This is exactly the gap `snapshot-complete-marker.md` documents for Bybit, where the marker
promises "the connector's cached view" rather than exchange state. Hyperliquid should not
inherit that weakness: because open orders survive everything, a Hyperliquid connector that
does not re-query starts up believing the account is flat while it holds live orders.

Note that this differs from Bybit's actual behaviour, which is to `cancel_all` on connect
(`bybit/private_stream.rs:117,284`) and start clean. For Hyperliquid, **reconcile rather
than cancel**: cancelling on every reconnect turns a transient network blip into a
round-trip through an empty book, and on a venue where a reconnect costs rate-limit budget
that is a bad trade. Make it configurable (`cancel_all_on_connect = false` by default) so
an operator can choose the Bybit behaviour.

### 5.11 Arm the dead-man's switch

**Decision.** If `schedule_cancel_secs` is configured, call `scheduleCancel` on connect and
refresh it on a timer at half that interval.

`scheduleCancel { time }` cancels every open order at `time` unless refreshed. The time must
be ≥5 s in the future, and there is a cap of **10 triggers per UTC day** — so the refresh
interval must be minutes, not seconds, and the cap must be respected or the switch silently
stops arming.

Since Hyperliquid never cancels on disconnect, this is the only exchange-side protection
against a connector that dies while holding quotes. `AGENTS.md` §1.1 asks for exactly this
kind of fail-closed behaviour. Default it **on**.

---

## 6. Module layout

Mirrors `connector/src/bybit/`:

```
connector/src/hyperliquid/
  mod.rs             Hyperliquid struct, Connector + ConnectorBuilder impls, Config
  public_stream.rs   bbo + l2Book + trades; owns SharedSymbolSet; re-subscribes on connect
  private_stream.rs  orderUpdates + userFills + user; reconciliation on connect
  ordermanager.rs    cloid <-> (symbol, OrderId) map; mirrors bybit/ordermanager.rs
  rest.rs            POST /info and /exchange; meta cache; rate-limit budget
  sign.rs            msgpack action hash + EIP-712 phantom agent
  depth.rs           snapshot -> kind-1 delta synthesis (§5.2)
  msg.rs             wire types
```

Wiring: a `hyperliquid = []` feature in `connector/Cargo.toml` (added to `default`), a
`#[cfg(feature = "hyperliquid")] pub mod hyperliquid;` alongside the others in
`connector/src/main.rs`, and one arm in the connector-name `match`.

**Reuse from `collector/src/hyperliquid/`, with one structural fix.** The ping/pong
handling (`http.rs:37–52`) is correct and should be lifted as-is. But `connect()` splits
the WebSocket and **moves the write half into the ping task** after the initial subscribe
batch — after which nothing can ever be sent again. A connector needs to subscribe on
`register()` at arbitrary times, so the writer must become a task fed by an
`UnboundedSender<Message>` command channel. Also replace the hand-rolled backoff ladder
with the `Retry`/`ExponentialBackoff` helpers in `connector/src/utils.rs`.

---

## 7. Testing

Per `AGENTS.md` §1.3, the money-touching and protocol-touching parts come with tests first.
`connector/` currently has 5 tests total, all in `utils.rs`; this should not add to that
deficit.

**Must be TDD, all pure functions over fixture strings — no network:**

1. **Signing.** Golden vectors from the official Python SDK: a known action + nonce + key
   must produce a known `r/s/v`. This is the single highest-value test in the whole
   connector — every signing footgun in §5.7 is silent, and a golden vector catches all
   three.
2. **Price formatting.** The 5-significant-figure and decimal-place rules, including the
   documented cases (`1234.5` ok, `1234.56` rejected, `0.001234` ok, `0.0012345` rejected)
   and trailing-zero stripping.
3. **Depth synthesis** (§5.2). Feed two snapshots, assert the emitted event set — including
   that a level leaving the visible window produces **no** deletion, and that a level
   cancelled inside the window produces `qty: 0.0`.
4. **Event kind.** Assert every emitted depth event satisfies
   `event.is(LOCAL_BID_DEPTH_EVENT)` or `is(LOCAL_ASK_DEPTH_EVENT)`. This is a one-line
   test that permanently prevents the §1 failure mode.
5. **Status mapping.** Every documented `orderUpdates` status string maps to the intended
   `Status`, and an invented string maps to terminal-unknown rather than "still resting".
6. **cloid round-trip.** Prefix encode/decode; a foreign prefix and a null cloid are both
   ignored rather than erroring.
7. **Nonce monotonicity**, including a clock that steps backwards.

**Not unit-testable, needs testnet rehearsal:** reconnect/re-subscribe behaviour, the
invalid-coin disconnect, rate-limit behaviour, and the `scheduleCancel` cap.

---

## 8. Phasing

| Phase | Deliverable | Gate |
|---|---|---|
| 1 | `meta` cache, config, wire types, signing + golden vectors | Signing tests green |
| 2 | Public stream: subscribe, re-subscribe, depth synthesis | Depth tests green; bot's book non-empty against testnet |
| 3 | Order path: submit/cancel over REST, order manager, `orderUpdates` | Round-trip an order on testnet |
| 4 | Reconciliation on connect + `SnapshotComplete`; `scheduleCancel` | Kill -9 the connector, restart, verify state matches the exchange |
| 5 | Rate-limit budget tracking and fail-closed signalling | Budget exhaustion produces a bot-visible error |

Phase 2 is the risky one: if the fusion approach in §5.2 does not resolve cleanly, the
depth design needs revisiting before anything downstream is built.

---

## 9. Open questions

1. **§5.3 tick model** — does registering the finest tick and rounding at submit time
   produce acceptable behaviour for the intended strategy, or does the price-dependent tick
   need to be represented properly upstream? Needs a decision before Phase 2.
2. **§5.2 fusion vs. a private mirror** — can `bbo` and `l2Book` be fed into the existing
   `FusedHashMapMarketDepth` without transient crossing, and is the missing `LOCAL_EVENT`
   bit in fusion's deletion events (`AGENTS.md` §4.7) fixable without disturbing Bybit?
3. **Rate-limit budget** — what is the actual expected quote rate, and does it survive the
   10,000-request buffer? This may make Hyperliquid unsuitable for a high-frequency grid
   without the volume to replenish, and that is worth knowing before Phase 3, not after.
4. **`l4Book` node** — no longer a question of feasibility; the requirements are now in
   §5.1 and admission is permissionless. It is a question of whether the strategy needs
   queue position enough to justify the cost: a 16 vCPU / 128 GB host in Tokyo, ~100 GB of
   logs a day to rotate, and taking ownership of an explicitly unmaintained
   `order_book_server`. Roughly two orders of magnitude more expensive than the
   public-feed collector.

   Decide by measuring against the public feed's ceiling — 5 levels every ~0.54s and BBO
   every ~0.14s, with no queue position at all. If quoting at the touch performs
   acceptably within that, the node buys precision the strategy does not use. If it does
   not, there is no alternative: no amount of work on §5.2 can synthesise queue position
   that the public feed never transmits.

   Answering this changes the phasing. If the node is adopted, most of §5.2 becomes
   throwaway and Phase 2 should be skipped rather than built and discarded.
5. **UNVERIFIED: does `orderUpdates` send a snapshot on subscribe?** Could not be settled
   during research. The design assumes **no** and re-queries via REST, which is correct
   either way; confirm on testnet and simplify if it does.
6. **UNVERIFIED: `mantissa` legal values** — docs are silent and two sources conflict
   (`{1,2,5}` vs `{2,5}`). Only matters if `nSigFigs` aggregation is used; v1 does not use it.
7. **UNVERIFIED: max order batch size** — undocumented. Affects `batchModify` sizing (§5.9).
8. **UNVERIFIED: testnet feed cadence and asset indices** — mainnet only was measured, and
   indices are known to differ between networks.

---

## 10. Phase 1 as built (2026-07-28)

Phase 1 is `connector/src/hyperliquid/`: `mod.rs` (config, `Connector` impl), `rest.rs`
(universe), `msg.rs` (wire types), `depth.rs` (snapshot→delta), `trades.rs` (replay guard),
`public_stream.rs` (the connection). 68 tests, every fixture a byte-for-byte capture from
testnet. What follows is where the build departed from this document, and why. Where the
two disagree, **the build is right and this section says so** — the earlier text is kept so
the reasoning is auditable.

### 10.1 Deletion policy: everything absent from the snapshot goes

§5.2's rule covers only the window's interior. Two cases it leaves undefined decide whether
the book is usable at all:

* **Above the new best bid** (below the new best ask). A falling touch leaves mirrored
  levels there. Not deleting them leaves the bot's book permanently crossed with no error
  anywhere. These deletions are mandatory, and `bbo` — which restates the whole touch —
  produces them between snapshots too.
* **Beyond the deepest observed level** — genuine truncation, genuinely ambiguous.

The build deletes both, so the invariant is one sentence: **the bot's book is exactly the
last observed window**, plus any touch levels `bbo` has added since. The alternative
(keeping truncated levels) was rejected on two grounds: the residue never expires, so the
book accumulates levels of unknown age for as long as the process runs and any
depth-derived number is silently part stale; and the offline converter deletes them by
default (`delete_out_of_book=True`), so keeping them would make a backtest of this feed
disagree with the live bot it is meant to predict.

**The mirror is not capped at N.** A `bbo` may push it to N+1 levels between snapshots; the
next snapshot brings it back. Capping would delete a real level and re-insert it on the
next frame — churn for nothing.

**What the retained touch levels actually are.** The invariant above reads as benign and is
not. A `bbo` touch level was accurate only at the instant it *was* the touch; once the touch
moves past it, its size is **unverified resting size** until a snapshot reconciles, and any
depth-derived quantity computed in between includes it. Measured: 1 000 upward `bbo` ticks
with no intervening snapshot leave 999 such levels, and the next snapshot clears them in one
batch. Bounded in practice by the snapshot cadence — roughly 4 levels at `l2_book = "fast"`
(0.54 s against a ~0.14 s `bbo`), roughly 38 at `"slow"` (5.4 s). That ratio is another
reason `"fast"` is the default, and it is now stated in `depth.rs`'s module comment rather
than left to be rediscovered.

### 10.2 One unlistable coin must not blind the others

Resolution is per coin, not all-or-nothing. A coin the venue does not list is refused with
a `LiveEvent::Error(CriticalConnectionError)` naming it, and the remaining coins subscribe
normally. All-or-nothing resolution had a failure mode worth recording: the subscribe step
fails, the connection is torn down and retried, the bad coin is still in the shared symbol
set — an endless reconnect loop with no market data for anyone, caused by one bot
registering a typo.

Measured on testnet 2026-07-28: `POST /info {"type":"meta","dex":"nope_xyz"}` answers
**HTTP 500 with a `null` body**. Verified live: with
`coins = ["BTC","ETH","NOPE_XYZ","nope:ABC"]`, both bad coins produced a bot-visible error
and BTC/ETH kept flowing.

> **Corrected after review.** The first build read *any* status-bearing error as "that dex
> does not exist". `/info` is weight-20 against a 1200/min IP budget and the venue sits
> behind a CDN, so a 429 or a 502 on the **canonical** dex would have left its universe
> unfetched, every bare coin refused as unlistable — and then written off, because the
> subscription tracker marked the whole pending batch. The socket stays up (the keepalive
> feeds the idle detector), no later registration re-asks (`pending` is empty), and the
> connector runs for hours connected and publishing nothing; a supervisor restart does not
> help, because the bot re-registers the same coins into the same window. It is the
> `AGENTS.md` §4.2 failure reached by a different route.
>
> Two rules now close it: only the **measured signature** (HTTP 500, body `null`) means "no
> such dex" and everything else propagates so the connection is retried; and only coins the
> venue gave a **listing verdict** on are marked subscribed
> (`HyperliquidError::is_listing_verdict`). A coin left unresolved because the venue would
> not answer stays pending and is asked about again on the next wake-up. A connection that
> resolved nothing now says so at `error!` instead of logging `coins = []` at `info!`.

### 10.3 Frames older than the last applied one are dropped

The fused depth in `main.rs` rejects any event whose timestamp precedes the state it would
update (`fuse.rs:80,196`), so an out-of-order frame applied to this backend's mirror would
be dropped downstream and the mirror would stop describing the bot's book. Frames are
therefore required to be non-decreasing in exchange time per coin; older ones are counted
(`FeedCounts::stale_frames`) and ignored. Measured: **zero** out-of-order frames in 644
interleaved `bbo`/`l2Book` frames across two coins, and zero in every live run since.

**The gate is shared by both channels, on purpose.** `bbo` and `l2Book` are produced by
different venue subsystems and nothing promises their `time` fields are monotonic in
socket-send order, so a snapshot stamped behind the last `bbo` is discarded whole — costing
up to one snapshot interval (~0.54 s at `"fast"`). Per-channel gates were considered and
rejected: fusion would then reject that snapshot's write to the level the `bbo` had just
stamped, leaving the mirror believing it published something the bot never received, which
is the one failure this whole module exists to prevent.

That leaves a tail risk the 644-frame testnet sample is too thin to rule out: if the venue's
`bbo` clock ran systematically ahead of its `l2Book` clock, most snapshots would be refused
and the bot would run on a touch-only book with the deeper levels frozen, indefinitely. So
the assumption is made **self-reporting** rather than assumed: refusals are measured as a
*rate* over each reporting interval (`public_stream::degraded`) and logged at `warn!` above
5 % of the interval's depth frames, instead of only appearing as a cumulative count in an
`info!` line.

**Two ways the latch could be poisoned, both closed.** The gate is latched, which makes the
timestamp it latches safety-critical. `exch_ts_ms * 1_000_000` was unchecked: `overflow-checks`
is on for `dev`/`test` and **off** for `release`, so any `time > 9_223_372_036_854` panicked
in a test and wrapped silently in production (`9_999_999_999_999_999` →
`1_864_712_049_422_024_128`). And nothing bounded the value even when it did fit: one frame
stamped year 2100 refused **1 000 out of 1 000** well-formed frames that followed, with no
recovery. Now the multiply is checked and the value is sanity-checked against the local
receive clock (`MAX_CLOCK_LEAD_NS`, 60 s); either refusal drops the frame and leaves the
gate where it was.

### 10.4 §4.7 is answered by construction, and pinned by a test

Open question 2 asked whether `bbo` and `l2Book` can be fed into the existing
`FusedHashMapMarketDepth` without transient crossing, given that fusion's own deletion
events lack the `LOCAL_EVENT` bit and are dropped by `LiveBot`.

Answer: a **private mirror**, with fusion left in place downstream. Because deletions are
always emitted before the levels that displace them, the published stream is uncrossed
after every individual event, so fusion never reaches the branch that generates a deletion
of its own. `fusion_never_generates_a_bitless_event_from_this_stream` replays real frames
through the same fusion `main.rs` uses and asserts it returns exactly what it was given.
Inverting the emission order makes that test fail with three `ev: 0x10000001` events —
`SELL_EVENT | DEPTH_EVENT` and no `LOCAL_EVENT` — which is the §4.7 trap itself. Nothing in
`fuse.rs` had to change.

> **The claim was conditional; it is now unconditional.** "Deletions before inserts" keeps
> every intermediate state uncrossed *only while the new state is uncrossed*. `on_bbo`
> resolved a self-crossed touch; `on_snapshot` did not, so a crossed `l2Book` — which the
> venue has never sent, but nothing forbids — would have put a low ask into fusion under a
> mirrored bid and produced exactly the bitless events this section says are unreachable.
> Measured: two `ev = 0x20000001` events out of fusion from one constructed frame. A crossed
> snapshot is now **refused whole and counted** (it cannot be resolved in favour of either
> side without inventing a book), before the monotonic gate latches, so the next good frame
> still lands.
>
> Two smaller versions of the same rule — *the mirror may only record what it published* —
> were closed with it. A level shrinking below one lot was emitted with a non-zero quantity
> that fusion rounded to zero and **deleted**, leaving the mirror holding a level the bot did
> not have and no way to re-emit it; it is now mirrored as absent and emitted as a deletion.
> Two venue prices rounding to one tick collapsed last-writer-wins, discarding the displaced
> level's size; they are now summed. Neither is reachable while `szDecimals` is right, which
> is why both are counted (`collapsed_levels`, `sub_lot_levels`) — a non-zero count is the
> only evidence that it is not.

### 10.5 The bot, not the connector, declares tick and lot

§5.3 says `RegisterInstrument` gets `tick_size = 10^-(6-szDecimals)`. In the current trait
that value is **not the connector's to set**: `LiveRequest::RegisterInstrument` carries the
tick and lot the *bot* chose, and `Connector::register(&mut self, symbol)` never sees them.
The backend derives them from `szDecimals` for its own mirror and logs them per coin
("Resolved a Hyperliquid instrument…"), and `connector/examples/hyperliquid.toml` says to
copy those numbers. Closing that hole means passing tick/lot to `register`, which is a trait
change for all four backends — separate work, not Phase 1.

**The consequence, measured, is worse than "collapses two price levels into one".** With
venue lot `1e-5` (BTC) and a bot that registered `1e-3`, `FusedHashMapMarketDepth` computes
`qty_lot = round(qty / lot) == 0` for every level below the bot's lot, takes the
`Entry::Vacant` branch, inserts nothing and returns an empty result: 3 of 10 events vanish
before the bot sees them, and the bot holds 3 bid levels against the mirror's 5.

It is **not** repairable by the connector, and it is worth recording why, because the
obvious remedy does not work. Re-sending the full book does not help: a size that rounds to
zero in the bot's lot grid rounds to zero every time. Measured both ways — 50 identical
snapshots through the diff, and 50 full restatements through a stateless relay — the bot
ends with 3 levels in both. The connector cannot see the bot's lot, and no level below it is
representable in the bot's book at all. This is a registration error whose only fix is at the
registration, which is what §10.5 is about. What *was* fixed is the adjacent, connector-side
half: the bot now gets everything it can represent immediately on registration (§10.9)
rather than only as sizes happen to change.

### 10.6 Phase 1 refuses orders rather than pretending

`submit`/`cancel` publish `LiveEvent::Error(ErrorKind::OrderError)` and do nothing else,
and `order_manager()` returns an always-empty implementation. Note what that means for
`SnapshotComplete`, which `main.rs` emits for every registration: for this backend it
truthfully says "no orders, no position, because this connector has none", **not** "the
exchange is flat". Hyperliquid's resting orders survive disconnects and restarts (§5.10),
so Phase 2 must re-query before that marker means anything about the account.

### 10.7 Smoke, on testnet

Connector plus a `LiveBot` registering BTC and ETH, 2026-07-28:

* Both instruments reached `snapshot_ready = true` on the first `elapse`.
* The bot's book filled within ~3 s and held the window: 5/5 levels a side, transiently
  6–7 after a `bbo` touch move. **74/74 samples uncrossed**; no empty side.
* 60 s of feed: 230 snapshots, 180 `bbo` frames, 921 depth events, 71 trades, 0 stale
  frames, 0 untracked-coin frames, 0 venue errors.
* Reconnects forced with a local relay that drops the socket every 20–25 s: every drop was
  followed by a full re-subscribe of both coins from the shared symbol set — the `AGENTS.md`
  §4.2 bug, which would have left the connection subscribed to nothing.
* **The replay guard, live:** `replayed_trades` went 0 → 60 → 120 across three forced
  reconnects — exactly 30 fills per coin per reconnect, dropped — while genuinely new
  trades continued to be published. Unguarded, those 120 phantom fills would have entered
  the bot's `last_trades`.

Feed counters are logged every 60 s **and on every disconnect**: the periodic tick lives
inside a connection, so on a venue that drops sockets this often it would otherwise never
fire, and the feed would be unobservable exactly when it matters.

### 10.8 Two things this document asks for that were deliberately not built

* **The `PublicWsSource` / `L4NodeSource` trait seam (§5.1).** There is one implementation
  and no second in sight; a trait with one implementor is an abstraction bought before it
  is needed (`AGENTS.md` §1.2). The seam it protects is small anyway — `MarketState` and
  `DepthMirror` are already independent of the transport, and open question 4 may delete
  §5.2 entirely rather than add a second source.
* **A `nSigFigs`/`mantissa` choice.** Not used: the default full-precision book is what the
  mirror wants, so open question 6 stays unanswered and harmless.

**Phase mapping.** The delivery called "Phase 1" in the task brief is this document's Phase
1 *minus signing* plus its Phase 2 (public stream, depth synthesis, testnet rehearsal).
Signing and the golden vectors move into the order-path phase, where they are first needed.

### 10.9 A bot that registers late is handed the book (review fix)

The first build's diff suppressed every level whose size was unchanged — correct, and the
whole point of synthesising deltas from a feed that restates the book twice a second. What
it missed is that the mirror is then **the only record of what a bot holds**, and a bot that
registers against an already-running connector holds nothing.

`main.rs` gives a newly registered instrument a *fresh* `FusedHashMapMarketDepth` (the
`Entry::Vacant` arm) and publishes `SnapshotComplete` regardless. The one path that replays
an existing book — `Entry::Occupied` → `depth_.snapshot()` — emits `DEPTH_SNAPSHOT_EVENT`,
which `LiveBot` drops without a word (`AGENTS.md` §4.1); measured, 10 events out, 10 dropped.
So the bot got an affirmative go signal and an empty book, and filled it only from levels
that happened to *change size*. Measured: a mirror primed by one snapshot, then the same
snapshot again, produces **0 events**; the bot's best bid and best ask are both `NaN`, and a
run in which only the bid side moved left `best_bid = 100.1, best_ask = NaN` after 100
snapshots — so `(bid + ask) / 2` is `NaN` with no bound on how long that lasts.

Three ordinary situations reach it: the shipped `coins = ["BTC", "ETH"]` primes the mirror at
process start before any bot exists; a bot restarting against a long-running connector (the
`myhft` deployment shape); any second bot on the same symbol.

**Fix.** `Hyperliquid::register` already broadcasts the coin unconditionally on every
registration, including re-registrations. The stream now uses that payload — previously
discarded — to publish `DepthMirror::restate`: every mirrored level as a kind-1 insert,
bids then asks so the sequence is never crossed, stamped with the exchange time the mirror
was built from so nothing downstream sees a rewind. A registration for an already-subscribed
coin therefore costs one batch of events and **no** REST call. If the broadcast lagged and
the coin names were lost, every tracked coin is restated instead; a restatement is
idempotent, so that is cheaper than guessing.

Two cases that look alike and are not, and are deliberately different code paths:
`MarketState::track` **keeps** an existing mirror (reconnect: the bot's book is undisturbed,
a fresh mirror would leave every level above the new touch in place), while
`MarketState::restate` **re-publishes** it (registration: the bot's book is empty and the
mirror is right). Both are pinned by tests.

### 10.10 Four more things the review found

* **The same unchecked millisecond→nanosecond multiply, in the trade path.** `trades.rs`
  carried it too. It is two different bugs in the two profiles this crate is built with:
  `overflow-checks` is `true` for `dev`/`test` and `false` for `release`, so an absurd `time`
  panics a test and wraps silently in production — and a panic in the connector is `exit(1)`
  under its own hook, market data down for every bot. `trade_event` now returns `Option` and
  a fill that cannot be timestamped is refused and counted (`FeedCounts::malformed_trades`).


* **An unbounded error string kills the connector.** `LiveEvent` is bincode-encoded into a
  fixed `MAX_PAYLOAD_SIZE = 512` byte slice, and an encode that does not fit propagates out
  of `run_publish_task` into a `.unwrap()` under the `exit(1)` panic hook — the process dies,
  taking every bot's market data with it, and dies again on restart because the bot
  re-registers the same symbol. Measured: 506 characters encode to exactly 512 bytes, 510 do
  not fit; and the unlisted-coin message's "did you mean" list was unbounded — a symbol whose
  portion after the last `:` is empty matched every name in a 103-name universe and produced
  780 characters. This is the first backend to compose its own error text rather than relay
  the venue's short one, so it is the first that can reach the ceiling. Every published
  message is now clamped to 400 bytes on a character boundary in `HyperliquidError::to_value`
  — one choke point, so no future call site can miss it — and the suggestion list is capped
  at five names and skipped for prefixes shorter than two characters.

* **A rejected order left a phantom.** `LiveBot::submit_order` inserts the order into its own
  map as `Status::New` *before* the request leaves, and nothing in `process_event` removes it
  on an `Error` event. Phase 1's `reject_order` published only the error, so a bot that
  mistakenly pointed at this connector was left holding a live order that exists nowhere and
  an `order_id` that returns `OrderIdExist` for the life of the process. It now publishes the
  order back as `Status::Expired` first — the same answer `binancefutures/mod.rs` and
  `bybit/ordermanager.rs` give an unsendable request — and the error second, so the bot's
  state is clean before an error handler that may abort the `elapse` ever runs.

* **The `/info` round trip stalled the keepalive.** It is awaited inside the read loop, so
  while it is pending no other `select!` arm runs — including the 30 s keepalive, against a
  venue that drops a connection after 60 s of silence. `rest::INFO_TIMEOUT` bounds one dex at
  15 s, so two referenced perp dexes (a bot registering a HIP-3 coin mid-session is enough)
  put the worst case at the edge. The resolve is now bounded as a whole (`RESOLVE_BUDGET`,
  20 s) and preceded by a keepalive of its own, so the silence it can cause is the stall
  itself rather than the stall plus a missed interval.

Also in this pass: `SubscriptionTracker::reset` was deleted. It had exactly one caller — the
§4.2 regression test — so that test asserted the resubscribe property against a code path
production never took, and a change that hoisted the tracker out of `connect` would have kept
it green. The test now drives `apply_resolution` through a recording sink and models a
reconnect the way `connect` does it, with a fresh tracker.
