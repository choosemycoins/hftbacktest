# Design: Hyperliquid connector

Status: Draft — for review, not yet approved
Date: 2026-07-25
Scope: a new `hyperliquid` backend in the `connector/` crate (market data + order management)
Related: [`snapshot-complete-marker.md`](snapshot-complete-marker.md), `AGENTS.md` §4 (codebase traps)

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

Policy: **the mirror is authoritative only inside the observed window.**

- Let `deepest_bid` / `deepest_ask` be the furthest prices in the incoming snapshot.
- Emit deletions **only** for mirrored levels that are inside `[deepest_bid, best_bid]`
  (resp. `[best_ask, deepest_ask]`) and absent from the new snapshot.
- Levels beyond the window are left untouched in the bot's book. They are stale by
  construction, and the bot must not treat depth beyond level *N* as meaningful.

This must be stated in the connector's docs, because it is a real accuracy limit, not an
implementation detail: **with the public feed, only the top N levels are trustworthy.**

**Feed selection.** Subscribe to `bbo` *and* `l2Book fast:true`:

- `bbo` at ~0.2 s is the only feed fast enough to quote on. Convert it into kind-1 depth
  events for the two touch levels — **not** `DEPTH_BBO_EVENT`, which the bot drops.
- `l2Book fast:true` at ~0.5 s supplies the 5 levels of context and corrects the mirror.

Default `l2Book` at ~5 s is a reconciliation feed at best; do not build on it.

**UNVERIFIED / needs a decision at implementation time:** whether interleaving `bbo` and
`l2Book` into one mirror can transiently cross the book (a `bbo` ask below a mirrored bid).
The connector already owns a `FusedHashMapMarketDepth` per symbol in `main.rs`, which has
crossing-resolution logic; the cleanest answer is probably to feed both streams into the
existing fusion rather than hand-rolling a second mirror. That needs prototyping — note
that fusion's own generated deletion events lack the `LOCAL_EVENT` bit (`AGENTS.md` §4.7),
which would have to be fixed for this path to work.

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

Nothing is replayed on reconnect. `userFills` re-sends its `isSnapshot` batch and `l2Book`
sends a fresh snapshot on the next tick, but **`orderUpdates` gaps are unrecoverable from
the stream** — the only way back to a correct view is a REST query.

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
