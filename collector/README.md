# collector — market-data recorder

Records raw exchange WebSocket feeds to disk, in the line format the
`hftbacktest` data converters expect. It is the first step of the pipeline:

```
collector  ──▶  *.gz  ──▶  hftbacktest.data.utils.<venue>.convert*  ──▶  *.npz  ──▶  backtest
```

The collector does no parsing, normalisation or validation beyond routing a
message to the right file. It writes down what the venue said and when the
process received it. Everything else is a later stage.

---

## Quick start

```bash
cargo build --release -p collector

mkdir -p /tmp/mktdata
./target/release/collector /tmp/mktdata bybit BTCUSDT ETHUSDT
# Ctrl-C to stop; files are flushed and closed on the way out.
```

Arguments are positional and order matters:

```
collector <output_dir> <exchange> <symbol>...
```

| Argument | Meaning |
|---|---|
| `<output_dir>` | Directory for the `.gz` files. Must already exist. |
| `<exchange>` | One of the venue names in the table below. |
| `<symbol>...` | One or more symbols, in the venue's own notation. |

`collector --version` prints the package version plus the git commit, branch
and clean/dirty state it was built from. The same values are logged at
startup as the `collector_starting` event, so a recording can always be traced
back to the code that produced it.

Log verbosity is `RUST_LOG` (standard `EnvFilter` syntax). The default is
`info`, which gives startup provenance, connect/disconnect and date-rotation
events; `RUST_LOG=debug` is very noisy at tick rates.

The process exits **non-zero** if recording stops for any reason other than a
signal — a write failure, an internal hand-off filling up, a feed that has gone
silent, or the collection task ending. A clean `systemctl stop` exits 0.

---

## Supported venues

The stream set per venue is fixed in `src/main.rs` — it is not configurable
from the command line, because the downstream converters are written against
exactly these streams.

| `<exchange>` | Streams / topics recorded |
|---|---|
| `binancefutures`, `binancefuturesum` | `@trade`, `@bookTicker`, `@depth@0ms`, **+ REST `premiumIndex` every 10 s** |
| `binancefuturescm` | `@trade`, `@bookTicker`, `@depth@0ms`, `@markPrice@1s` |
| `binance`, `binancespot` | `@trade`, `@bookTicker`, `@depth@100ms` |
| `bybit` | `orderbook.{--bybit-depths}`, `publicTrade` |
| `hyperliquid` | `trades`, `bbo`, `activeAssetCtx`, `l2Book` × `{--hl-l2-modes}` |
| `lighter` | `order_book`, `ticker`, `trade`, `market_stats` — all four per market |

USD-M is the odd row: it carries no `@markPrice@1s`, because **that class
lives on fstream's routed `/market` path** (Binance split fstream into
`/public`/`/market`/`/private` on 2026-03-06; the collector dials `/public`,
and reaching `/market` would mean a second socket with its own lifecycle) —
see [Index, oracle and funding](#index-oracle-and-funding). COIN-M's `dstream`
has not migrated yet, still serves it unrouted, and keeps it.

Two venues take a flag because their defaults are load-bearing:

**`--bybit-depths` (default `1,50`).** Bybit fails the *entire* subscribe batch
if one topic is unknown, and the connection then stays open, ping-ponging, with
no subscription — recording nothing while looking perfectly healthy. Measured
against `stream.bybit.com/v5/public/linear` on 2026-07-25: `1`, `50` and `200`
are accepted for BTCUSDT, ETHUSDT and SOLUSDT; **`500` is rejected for all
three** (`error:handler not found`) despite still being documented. The
collector previously hardcoded `1,50,500` and therefore recorded zero bytes.
A rejected subscribe is now fatal — the process exits non-zero rather than
idling silently.

**`--hl-l2-modes` (default `slow,fast`).** Hyperliquid's `l2Book` has two
cadences and neither is sufficient alone. Measured on mainnet 2026-07-25:

| Feed | Levels/side | Median interval |
|---|---|---|
| `bbo` | 1 | 0.14 s |
| `l2Book` `fast` | 5 | 0.54 s |
| `l2Book` plain | 20 | 5.41 s |
| `trades` | — | 0.60 s |

The plain feed updates the book roughly three times a minute, which is not
usable for backtesting an HFT strategy; the fast feed is only five levels deep.
The venue accepts both subscriptions on one connection, so the collector records
both, side by side and unmerged. The converter selects one with its `book_mode`
argument and drops the other, or — with `book_mode='bbo+fast'` — folds the `bbo`
touch feed into the fast cadence, which is the pairing the live connector is
specified on. Recording all three is what keeps the choice open, and
revisitable. Messages from the fast feed carry `"fast": true` in their payload,
which is how they are told apart on the way back out.

Symbol case is passed through verbatim to the venue. Binance stream names are
lowercase (`btcusdt`), Bybit and Hyperliquid want uppercase (`BTCUSDT`, `BTC`).
Output filenames are lowercased regardless of what you pass in.

### Index, oracle and funding

Some of what is recorded is not order flow. Each venue's index basket, mark
price and funding rate are recorded because **they carry the spot basket that
venue's funding is priced against** — the one thing about a perpetual that its
own book and tape cannot tell you afterwards:

| Venue | Source | The field that matters | Also carries |
|---|---|---|---|
| Binance USD-M | **REST** `GET /fapi/v1/premiumIndex`, polled every 10 s | `indexPrice` — the Binance index, aggregated across its constituent spot exchanges | `markPrice`, `lastFundingRate`, `estimatedSettlePrice`, `interestRate`, `nextFundingTime` |
| Binance COIN-M | WS `<symbol>@markPrice@1s` | `i` — the same index | `p` mark price, `r` funding rate, `T` next funding time |
| Hyperliquid | WS `activeAssetCtx` | `ctx.oraclePx` — Hyperliquid's own spot basket, the direct input to its funding calculation | `ctx.markPx`, `ctx.midPx`, `ctx.premium`, `ctx.funding`, `ctx.openInterest` |
| Lighter | WS `market_stats/{market_id}` | `index_price` — the venue's own index | `mark_price`, `current_funding_rate`, `funding_rate`, `funding_timestamp`, `open_interest` |

Recording the constituent spot books instead would cost orders of magnitude
more and still only approximate what the venue actually used. These are the
venues' own aggregates, at a few hundred bytes a second per instrument.
Measured 2026-07-28 over a five-minute recording made by this collector,
~295 frames each:

| Venue / stream | Median interval | Frame | Gzip | Cost |
|---|---|---|---|---|
| Binance COIN-M `btcusd_perp@markPrice@1s` | 1.000 s | 231 B | 8.2× | 2.7 MB/day/symbol |
| Hyperliquid BTC `activeAssetCtx` | 1.018 s | 312 B | 11.9× | 2.4 MB/day/coin |

Both are periodic rather than event-driven: consecutive frames are frequently
byte-identical, which is why they compress an order of magnitude and why a
volatile session costs no more than a quiet one. Neither is behind a flag —
there is no trade-off to expose, and a recording made without them cannot be
repaired afterwards.

Lighter's is the exception to "periodic": `market_stats` arrives on change,
several times a second on a liquid market (0.12–0.25 s median, 1.77 s worst
over a 40 s window, 2026-07-28) and not at all on a still one. It is recorded
for the same reason — the index and funding it carries cannot be recovered from
the book afterwards — but its silence is not evidence on its own.

The Hyperliquid subscription takes the coin exactly as `l2Book` does, prefix
and all (`{"type":"activeAssetCtx","coin":"xyz:GOLD"}`), so a builder-dex
instrument's context lands in the same file as its book. Verified against
mainnet.

#### USD-M: the markPrice class moved behind the routed `/market` path, so it is polled instead

Binance split `fstream` into routed connection classes on 2026-03-06 —
`/public` (bookTicker, depth), `/market` (aggTrade, markPrice, kline, ...),
`/private` (user data) — and since the legacy decommission date, 2026-04-23,
an unrouted connection is a degraded alias of `/public`: a `/market`-class
subscription is acked and then never served, with no error anywhere.
**Measured 2026-07-28, from a Tokyo host and a filtered local path
independently, before the docs explained it:** `<symbol>@markPrice@1s`,
`<symbol>@markPrice`, `!markPrice@arr`, `!markPrice@arr@1s` and
`<symbol>@indexPrice@1s` each delivered **zero** frames, on the same connection
and in the same eight seconds that `<symbol>@trade` and `!bookTicker` delivered
**802**. Two independent network paths agreeing rules out the vantage point,
which an earlier single-path measurement could not. `dstream.binance.com`
(COIN-M) served `markPriceUpdate` throughout, which is why the sibling backend
keeps its stream and this one does not.

`@markPrice@1s` sat in the USD-M stream list for part of that day, subscribed
and silent: acked by the venue, no frames, no error, nothing in the recording
and nothing in the logs. Harmless, useless, and actively misleading in the list
— it read as evidence that index and funding data were being recorded.

So USD-M polls instead:

- **`GET /fapi/v1/premiumIndex` with no `symbol` parameter**, every 10 s. One
  request covers the whole venue: 851 elements, 188 KB, measured 2026-07-28.
  The elements for the recorded symbols are written into their symbol files
  **verbatim** — the venue's own JSON object, key order and all — exactly as a
  WebSocket frame would be. The rest are dropped.
- **Ten seconds is a sampling choice**, not a cadence the venue imposes. The
  underlying numbers move at 1/s and the funding rate itself every eight hours;
  a basis series does not need per-second resolution.
- **Ingress, not disk, is the cost.** The whole-venue response is fetched
  whatever the symbol count, so this is ~1.6 GB/day of download regardless of
  how many symbols an instance records, against ~2.1 MB/day/symbol of raw lines
  written (~0.3 MB/day compressed, extrapolating the COIN-M ratio — the raw
  figure is arithmetic, the compressed one is not yet measured). On a metered
  link that ratio is the thing to look at, not the file sizes.
- **A failed poll is a warning and a skipped cycle, never fatal.** This is
  auxiliary data; ending a day of book and tape because a REST endpoint
  returned 502 would be out of all proportion. Every other failure path in this
  collector stops the process, and this one deliberately does not.
- **But it is never silent.** After 30 consecutive failures — five minutes —
  the sidecar gets one record, so the offline gate can see it:

  ```json
  {"_collector":"poller_degraded","poller":"premiumIndex",
   "consecutive_failures":30,"interval_s":10,"error":"…"}
  ```

  Once per outage, not once per failure: at this cadence a record every ten
  seconds would be a sidecar nobody finishes reading. A successful poll clears
  the count and re-arms it, so a second outage is reported as a second outage.
  Every individual failure is still a `WARN` in the journal.

Handing the element to the writer is the one part that follows the ordinary
fatal contract: a hand-off that cannot take a record means the recording has
broken, and the collector stops with the files closed, exactly as everywhere
else.

One consequence for anything reading these files: the poller is a **second
producer**, and the only one with a hand-off of its own (`POLLER_HOP`). A
WebSocket frame queues through the socket hop and then the writer hop; a poll
crosses neither, so a `premiumIndex` line can be written ahead of a book tick
stamped earlier — by however long that tick was waiting in the two of them. That
is an interleave, not a defect, and the offline report grades it as its own
finding, yellow at any size (`interleave_kind` in `tools/quality_report.py`,
where the reasoning and the rejected alternatives are). Only in that direction,
though: a poll written *after* a frame stamped later waited on its own hop and
nowhere else, and that keeps a bound — it is the one place a `premiumIndex`
stamp taken anywhere but at receive would show. The REST depth snapshot is the
milder version of the same thing: it skips the socket hop but shares the writer
hop, so one queue still describes the pair and it keeps a bound either way.

The Python converter (`hftbacktest.data.utils.binancefutures`) skips these lines
the same way it already skips the REST depth snapshots: at its default
`combined_stream=True`, which is the correct setting for these files, a line
with no `data` envelope is passed over without an event type being read out of
it. Turning them into rows is not implemented — `opt='m'` converts a
`markPriceUpdate` frame, i.e. COIN-M's.

### Hyperliquid symbol names

Perps have **no quote suffix** — `BTC`, not `BTCUSDT`. The quote shown in the
web UI is the dex's collateral token, not part of the instrument name.

HIP-3 lets third parties deploy their own perp dexes, each with its own
universe and its own collateral. Their instrument names carry a `dex:` prefix,
and that prefixed name *is* the wire name — do not concatenate it yourself:

| Wire name | Dex | Collateral |
|---|---|---|
| `ENA` | canonical | USDC |
| `hyna:ENA` | hyna | USDE |
| `xyz:GOLD` | xyz | USDC |

`ENA` and `hyna:ENA` are different instruments with separate books; recording
both is legitimate and they land in `ena_*.gz` and `hyna:ena_*.gz`. Some assets
exist only on builder dexes — `GOLD` has no canonical listing at all, so a dex
must be chosen deliberately (measured 2026-07-25: it is on `xyz`, `flx`, `hyna`,
`km`, `cash` and `mkts`, across four different collateral tokens).

Every name is checked against the venue at startup and an unknown one is a
refusal to start, with a hint at the likely mistake:

```
Error: unknown Hyperliquid perp symbol(s): hyna:hyna:ENA (did you mean:
hyna:ENA?). Names are case-sensitive; the canonical dex takes a bare coin
(BTC, not BTCUSDT) and a builder dex takes its prefixed name (xyz:GOLD).
```

The resolved set — wire name, dex, collateral, `szDecimals`, `maxLeverage` — is
written to the sidecar as a `{"_collector":"universe"}` record. `szDecimals` is
what a converter needs for lot size, and the collateral is the only thing that
distinguishes two same-asset instruments after the fact.

### Lighter: markets are integers

Lighter subscribes by **market id**, not by symbol: `order_book/0` is ETH and
`order_book/1` is BTC. Nothing on the wire ever names the instrument, so the
collector resolves every requested symbol against
`GET /api/v1/orderBooks` at startup and refuses to run if one does not resolve.

That check is not optional here, and `--no-symbol-check` is **refused** rather
than honoured: on the other venues the flag skips a lookup that only validates,
but on this one the lookup *is* the addressing — there is nothing to subscribe
to without it.

Symbols are the venue's own bare names (`ETH`, `BTC`), matched case-insensitively
and recorded under the venue's spelling. Three things are refused, each because
it produces a silent failure rather than a loud one:

| Refused | Why |
|---|---|
| a symbol not in the catalog | subscribing to a market id the venue does not know is answered with `{"error":{"code":30005,...}}` and **the socket stays open** — a healthy connection recording nothing |
| a market whose `status` is not `active` | 18 of the venue's 227 markets were `inactive` on 2026-07-28; they are listed, subscribable and silent |
| a symbol that cannot be a filename | the venue's spot markets are named `ETH/USDC`, and the collector names files after the symbol — `<dir>/eth/usdc_<date>.gz` is a directory that does not exist, so the first frame would end the recording with `ENOENT` minutes after a clean start |

The resolved catalog — symbol, `market_id`, `market_type`, price and size
decimals, minimums — is written to the sidecar as a `{"_collector":"universe"}`
record, the same record name Hyperliquid's instrument metadata uses. The
symbol → id map is *also* stamped into `session_start` as `lighter_markets`,
because the recorded payloads are keyed by the integer and the ids are venue
configuration rather than constants: a day's files cannot be read back without
it.

**The four channels, measured on mainnet 2026-07-28** (ETH and BTC, 40 s):

| Channel | What it is | Median interval | Notes |
|---|---|---|---|
| `order_book` | full snapshot on subscribe, then diffs | 0.050 s | batched on a ~50 ms timer; a level with size `"0.0000"` is a **deletion** |
| `ticker` | top of book, event-driven per engine nonce | 0.0095 s | the low-latency touch, ten times finer than the book batches |
| `trade` | prints, per block | 0.07–0.18 s | the subscribe frame replays the **last 50 trades** — the first frame is history |
| `market_stats` | mark, index, funding, open interest | 0.12–0.25 s | see [Index, oracle and funding](#index-oracle-and-funding) |

All four are recorded for every market and none is behind a flag: they are not
substitutes for one another, and the book alone is neither fast enough at the
touch nor able to say what funding was priced against.

#### The nonce chain, and what the collector does about a break

Every `order_book` frame carries `begin_nonce`..`nonce` — matching-engine
sequence numbers — and `begin_nonce(N+1)` must equal `nonce(N)`. The snapshot
seeds the chain (`begin_nonce: 0`) and the diffs continue it, so the check
spans both frame types.

A break means the venue advanced without us. It is invisible everywhere else in
the recording: the frames keep arriving on time, so there is no hole in
`local_ts` for the offline report to measure, and only the numbers inside the
payloads say a batch is missing. The collector therefore:

1. writes `{"_collector":"sequence_gap","channel":"order_book","market":0,
   "symbol":"ETH","expected_begin_nonce":…,"begin_nonce":…,"count":N}` to the
   sidecar, immediately behind the frame that revealed it;
2. repairs **that market's book only**, by sending `unsubscribe` and then
   `subscribe` on `order_book/<id>`;
3. counts the breaks per market for the life of the process.

**The repair is two frames, and the obvious one on its own does nothing.**
Re-sending `subscribe` for a channel the connection already holds is answered
`{"error":{"code":30003,"message":"Already Subscribed to : order_book:0"}}` and
**no snapshot** — measured against mainnet twice on 2026-07-28, snapshots after
the duplicate: 0. A repair built that way would leave the book on a broken chain
until the socket happened to drop for some other reason, possibly hours, with
nothing in the recording saying the repair never happened. The unsubscribe is
what makes the subscribe a fresh one; the pair is honoured sent back to back,
ack at +267 ms and a fresh 105 KB snapshot with `begin_nonce: 0` at +522 ms.
Both of the venue's answers are recorded: the `{"type":"unsubscribed",…}` ack
names the market's channel, so it lands in that symbol's file, marking the exact
point the diff chain was deliberately broken.

A market is repaired **at most once a minute**. A venue that has genuinely lost
us breaks the chain on every frame that follows, and at ~20 book frames a second
one repair per frame would spend the venue's whole minute budget in five
seconds. Nor is "one repair outstanding, cleared by its snapshot" enough: that
makes the repair rate the repair's own 522 ms round trip, so a persistently
lossy market would cycle break → repair → snapshot → break at ~4 client messages
a second and drag a fresh 105–141 KB snapshot through the writer hop each time.
The cooldown is on the **attempt**, not the outcome, which also means a repair
the venue refuses cannot disarm a market permanently. Subsequent breaks are
still recorded and counted; only the repair is suppressed.

At 25 markets that is at worst 50 client messages a minute for repairs, which
leaves room for the keepalives and for a reconnect's whole subscribe set inside
the same minute.

**`offset` is not a sequence number.** The venue sends one on every book frame
and it is tempting; it is API-server-local and jumps on reconnect, so a chain
built on it would report a break for every reconnect and miss the losses that
matter.

#### Message budget, and why the market count is capped

The venue allows **200 client messages per minute** per connection, 500
subscriptions per connection and 255 connections per IP. Exceeding the message
budget is not answered with an error — the connection is throttled, which from
this side is a socket that goes quiet, i.e. the one failure this collector is
least able to tell from a quiet market.

So one connection's subscribe set is held to **half** the budget, leaving room
for the keepalives, the per-market repairs and a reconnect inside the same
minute. Four channels per market makes that **25 markets**, and
`match_catalog` refuses more with a message pointing at a second instance.
Subscribes go out in chunks of 8 with 250 ms between them; the pacing is logged
at every connect:

```
connecting to the Lighter WebSocket subscriptions=8 markets=2 chunk=8
  chunk_delay_ms=250 paced_over_ms=0 budget_per_min=100 venue_limit_per_min=200
```

Keepalive is a **protocol-level Ping every 30 s**. The venue sends no pings of
its own and requires a client frame at least every two minutes; the protocol
ping is used rather than the app-level `{"type":"ping"}` because it does not
enter the recording as a frame. A connection that delivers nothing at all for
90 s is torn down and redialled — an unknown market id is answered with an error
frame and *not* a close, so a silent socket has to be found from this side.

#### Geoblocking is checked before recording starts

`/stream` sits behind a CloudFront jurisdiction check. From a restricted region
the **upgrade** fails as `Protocol(ResetWithoutClosingHandshake)` —
indistinguishable from a network error — while REST keeps working, so a catalog
fetch that succeeded proves nothing about the socket. The collector probes the
upgrade once at startup with a 5 s timeout and refuses to start if it fails,
rather than reconnecting for ever and exiting 0. The error names the likely
cause and mentions the `?readonly=true` endpoint that exists for restricted
regions — deliberately not used, because it is a different data guarantee and a
recording made against it would not be comparable.

The refusal is recorded as `{"_collector":"probe_failed","url":…,"error":…}`,
under its own name rather than `symbol_check_failed`: by the time the probe
runs every symbol has already resolved against REST, so annotating the hole
with a symbol problem would send whoever reads it to check a list that was
never wrong.

#### Compression: none, measured

The venue offers `permessage-deflate`; a Python `websockets` client negotiates
it. This collector does not: tungstenite 0.27 implements no such extension and
never sends `Sec-WebSocket-Extensions`, so the server has nothing to accept and
every frame arrives as plain text. Recorded lines are therefore ordinary JSON
either way — what reaches the file is always the decompressed text — but the
bandwidth is not saved. The startup probe logs what the server answered, so the
day the stack grows the extension the journal says so rather than the bytes:

```
the venue is reachable from this host took_ms=941 extensions=None
```

#### Volume

Measured over a 4-minute mainnet recording of ETH and BTC, 2026-07-28:

| | Lines | Gzipped |
|---|---|---|
| ETH | 17 150 in 243 s (~71/s) | 1.27 MB → **~450 MB/day** |
| BTC | 16 346 in 243 s (~67/s) | 1.47 MB → **~520 MB/day** |

That is one to two orders of magnitude above Hyperliquid (~22 MB/day/coin) and
comparable to Bybit's busiest symbols — `ticker` alone is 60% of the lines.
Plan disk accordingly: a ten-market instance is roughly 5 GB/day.

One process handles one venue. Recording two venues means two processes — see
[Deployment](#deployment), where that maps onto one systemd instance each.

---

## Output format

One file per symbol per UTC day, plus one sidecar:

```
<output_dir>/<symbol-lowercase>_<YYYYMMDD>.gz        gzipped market data
<output_dir>/_meta_<exchange>_<YYYYMMDD>.jsonl       plain text, flushed per record
<output_dir>/.collector.lock                         which process owns this directory
```

The sidecar carries the exchange name so that days from several instances can
be gathered into one directory for conversion without their sidecars
overwriting each other.

**One collector per output directory** — this is enforced, not advised. The
first process takes an exclusive `flock` on `.collector.lock` and writes its
instance and pid there; a second one exits non-zero naming the holder:

```
Error: /opt/hft-collector/data/bybit is already being recorded into, by
instance=bybit pid=4711 since=2026-07-26T09:14:02+00:00. …
```

Sharing would corrupt the recording rather than merely mix it up: both
processes append to the same `<symbol>_<date>.gz` — Bybit `BTCUSDT` and Binance
`btcusdt` are one filename after lowercasing — and their gzip members
interleave into something no decoder can read, losing the day for both. The
lock lives on the open file description, so the kernel releases it however the
process dies, SIGKILL included; the file is left behind on purpose and its
presence alone means nothing.

It is **not** compressed, on purpose. Its value is being readable while the
collector runs, and gzip cannot provide that: `GzEncoder::flush()` emits a
deflate sync point but no member trailer, so a reader still rejects the file
until the member is closed at shutdown. Measured over a 12-minute run, a
gzipped meta stream stayed at 10 bytes on disk the whole time and materialised
only on exit — no use for diagnosing a live problem, and lost entirely to a
SIGKILL. It does not need compressing either: the four minutely
[host gauges](#host-gauges-clock-cpu-and-per-symbol-liveness) dominate its steady
state at ~530–600 bytes a minute — `disk` ~112 B, `clock` ~129 B, `cpu` ~110 B,
`liveness` ~180 B for thirteen symbols, measured 2026-07-28 — which is ~0.8 MB
a day against 22 MB a day for one Hyperliquid coin. Lifecycle records add to that only
when something happens. The different extension also keeps it out of the
`*_<date>.gz` wildcards that feed the converters.

```bash
tail -f /opt/hft-collector/data/hyperliquid/_meta_hyperliquid_$(date -u +%Y%m%d).jsonl
```

Each decompressed line is a receive timestamp, a single space, then the raw
message exactly as it came off the wire:

```
1758841137168651303 {"topic":"orderbook.1.BTCUSDT","type":"snapshot","ts":...}
```

- The timestamp is **nanoseconds since the Unix epoch, UTC**, taken when the
  process received the message. It is the `local_timestamp` of the backtest;
  the exchange timestamp lives inside the payload.
- Files roll over at UTC midnight, driven by the timestamp of the message
  being written — not by a timer. A symbol that goes quiet across midnight
  rolls over when its next message arrives.
- Nothing is ever pruned. Budget disk accordingly and add your own retention.

### The `_meta` sidecar

Symbol files hold only market data. Everything else the collector observes —
which cannot be attributed to a symbol — goes to
`_meta_<exchange>_<date>.jsonl` in the same line format, so a recording
explains itself instead of having to be guessed at:

**The connection lifecycle is written by every backend, in one format.** The
records tagged `_collector` are the collector's own account of the session;
the untagged ones are venue frames forwarded verbatim, and those are as uneven
as the venues themselves:

| Record | Meaning | Venues |
|---|---|---|
| `{"_collector":"session_start", …}` | collector version, commit, exchange, symbols, flags | all |
| `{"_collector":"subscribe", …}` | `url`, `attempt`, and the exact set that was requested | all |
| `{"_collector":"connected", …}` | `url` — the socket came up | all |
| `{"_collector":"disconnected", …}` | an established socket went away: `error`, and `connected_for_ms` | all |
| `{"_collector":"dial_failed", …}` | the socket never came up: `error`, and `dialling_for_ms` | all |
| `{"_collector":"stream_ended", …}` | clean end of stream, with `connected_for_ms` | all |
| `{"_collector":"queue_overflow", …}` | an internal hand-off filled up; the collector is stopping | all |
| `{"_collector":"hand_off_closed", …}` | an internal hand-off lost its consumer, i.e. the collection task had already ended; the collector is stopping | all |
| `{"_collector":"stalled", …}` | nothing was recorded for the whole watchdog window; the collector is stopping | all |
| `{"_collector":"poller_degraded", …}` | a REST poller has failed `consecutive_failures` times running, `interval_s` apart, with `error`. The collector is **not** stopping — see below | binancefuturesum |
| `{"_collector":"sequence_gap", …}` | a venue sequence number skipped ahead, so frames were lost: `channel`, `market`, `symbol`, `expected_begin_nonce`, `begin_nonce`, and the market's running `count`. The collector is **not** stopping — it repairs that market's book, at most once a minute per market | lighter |
| `{"_collector":"probe_failed", …}` | the venue refused the WebSocket upgrade from this host before anything was recorded: `url`, `error`. The collector **is** stopping, and this is not a symbol problem — see [Geoblocking is checked before recording starts](#geoblocking-is-checked-before-recording-starts) | lighter |
| `{"channel":"subscriptionResponse", …}` | the venue's ack, echoing its normalised parameters | hyperliquid |
| `{"channel":"error", …}` | venue rejections | hyperliquid |
| `{"channel":"pong", …}` | liveness during a stretch with no market data | hyperliquid |
| `{"success":…,"ret_msg":…}` | subscribe ack, successful or not | bybit |
| `{"type":"connected","session_id":…}` | the session handshake | lighter |
| `{"error":{"code":30005,…}}` | a subscription the venue would not serve | lighter |
| `{"error":{"code":30003,…}}` | a subscribe for a channel this connection already holds. It names no channel, so it can only land here | lighter |
| `{"channel":"height",…}`, `{"channel":"market_stats:all",…}` | channels that name no single market, if they ever arrive | lighter |

A `subscribe` followed by `dial_failed` is a socket that never came up. A
`connected` with no market data behind it is a subscription the venue accepted
and never served. Neither was distinguishable from a quiet market before.

`connected_for_ms` appears only on records for a connection that existed, which
is why a failed dial is its own event rather than a `disconnected` reporting how
long a DNS or TLS stall took. A refused internal hand-off writes no end-of-stream
record at all: it would have to travel the hop that just refused a market-data
frame, and `queue_overflow`/`hand_off_closed` already name it from the other end.

`poller_degraded` and `sequence_gap` are the two records that report a fault the
collector then carries on through. Every other `_collector` record above is
either routine or terminal. `poller_degraded` says a feed is missing while the
recording continues, because the feed in question is auxiliary and stopping over
it would be out of all proportion (see
[Index, oracle and funding](#index-oracle-and-funding)); it is written once per
outage rather than once per failure, and a successful poll re-arms it.
`sequence_gap` says a stretch of book updates was lost — the one damage that
leaves no trace in the data itself, since the frames either side of it arrive
perfectly on time (see [the nonce chain](#the-nonce-chain-and-what-the-collector-does-about-a-break)).

Binance acks nothing at all — the subscription is the URL it is dialled with —
so on those three venues the `_collector` records are the only account of the
session there is.

Alongside those events the sidecar carries the collector's **gauges** —
`disk`, `clock`, `cpu` and `liveness`, all written on one minute timer, plus
`universe` once at startup on Hyperliquid and Lighter. They are measurements of the host,
not events in the recording's life, and the difference is load-bearing: a
minutely gauge lands inside every hole longer than a minute whatever caused it,
so `quality_report.py` refuses to let one explain a gap. See
[Running out of space](#running-out-of-space) and
[Host gauges](#host-gauges-clock-cpu-and-per-symbol-liveness).

This is what turns an unexplained gap into a diagnosable one. A recording made
against a deliberately invalid symbol now reads:

```
session_start hyperliquid ['BTC','NOPE_XYZ'] modes=['slow','fast']
subscribe attempt=0 n=8
connected wss://api.hyperliquid.xyz/ws
subscriptionResponse ×4          <- only BTC's four were acked
disconnected after=1194ms err=Connection reset without closing handshake
subscribe attempt=1 n=8
...
```

Previously the same run produced a symbol file with seven unexplained
1.5–2 second holes and nothing else.

**Nothing is dropped.** Any frame that cannot be filed under a symbol — an
empty trades array, an unrecognised channel, a `pong` — is written to `_meta`
rather than discarded, so the recording stays a faithful record of the session.

The collector performs no merging of its own. Feeds of different depths and
rates are recorded side by side exactly as received; reconciling them is a
policy decision with no single right answer, and making it at capture time
would leave the recording unable to answer any other question. Keeping it
downstream is what allows several merge policies to be run over the same bytes
and compared.

### Multi-member gzip

A file is opened in **append** mode, so each collector session adds a new gzip
member to the same day's file. Restarting mid-day therefore preserves what was
already recorded.

This is transparent to `gunzip`, `zcat`, and Python's `gzip` module — which is
what every converter in `py-hftbacktest/hftbacktest/data/utils/` uses. It is
*not* transparent to `flate2::read::GzDecoder` in Rust, which stops after the
first member; use `flate2::read::MultiGzDecoder`.

Sanity-check a file:

```bash
zcat btcusdt_20260725.gz | wc -l
zcat btcusdt_20260725.gz | head -1
```

---

## Converting to backtest data

```python
from hftbacktest.data.utils import bybit

# Bybit: fuse orderbook.1 / .50 / .500 into one depth stream
data = bybit.convert_fused(
    'btcusdt_20260725.gz',
    output_filename='btcusdt_20260725.npz',
    tick_size=0.1,
    lot_size=0.001,
)

# or process a single depth level
data = bybit.convert_depth('btcusdt_20260725.gz')
```

For Hyperliquid, pick which depth stream to convert:

```python
from hftbacktest.data.utils import hyperliquid

data = hyperliquid.convert(
    'btc_20260725.gz', tick_size=1.0, lot_size=0.00001,
    num_levels=5, book_mode='bbo+fast',   # or 20/'slow', or 5/'fast'
)
```

| `book_mode` | `num_levels` | depth from | top of book from |
|---|---|---|---|
| `slow` | 20 | `l2Book` plain, ~5.4 s | the same |
| `fast` | 5 | `l2Book` `fast`, ~0.54 s | the same |
| `bbo+fast` | 5 | `l2Book` `fast`, ~0.54 s | `bbo`, median 86 ms |

The pair is not free to choose: `DiffOrderBookSnapshot` preallocates exactly
`num_levels` rows, so a mismatch is refused.

**Do not derive `tick_size` from `szDecimals`.** `10^-(6 - szDecimals)` is the
*lower bound* of the Hyperliquid tick, not the tick: a price is legal at ≤5
significant figures **and** ≤ `6 - szDecimals` decimals, so the effective tick is
the coarser of the two and therefore depends on the price. Measured on
2026-07-29: ONDO quoted 0.39040..0.41209 with `szDecimals=0`, every one of 2.76M
recorded prices on a 1e-5 grid, while the formula says 1e-6 — a backtest built on
it quotes nine phantom levels between every real one, and ONDO's PnL changed sign
when it was corrected. The step runs the other way too: above 100 000 five
figures make the effective tick 10 (testnet, 2026-07-28 — `123456` is rejected
and rounds to `123460`), so BTC is the same trap one decade up.
`tools/build_dataset.py` (mode A) and `tools/build_hl_dataset.py` (Hyperliquid
only, for a day whose signal recording is broken) measure the tick from the
recording — over the frames `--book-mode` actually converts, since the cadence it
skips never reaches the dataset — cross-check it against the rule, record both in
the manifest, and refuse a window that crosses a price decade or that quotes
finer than five significant figures. Converting by hand, take the tick from a
built manifest rather than from a formula.

The sidecar is `.jsonl`, so a `<dir>/*_<date>.gz` loop will not pick it up.
Independently of that, `hyperliquid.convert` raises on a recording that yields zero rows rather
than writing an empty `.npz` that looks like a legitimately silent day. That
guard also catches a truncated file, the wrong venue, and a `book_mode` that
matched no message in the recording.

`convert` also drops replayed trades. Hyperliquid resends the last 30 fills of a
coin in one `trades` frame on every (re)subscribe, and a replayed fill carries
the same `tid` as the original — measured on 2026-07-27, a day with 10
reconnects held 223 phantom rows for BTC and 299 for ENA. They would become
extra `TRADE_EVENT`s and bias the queue model, so a `tid` already emitted is
skipped; the converter prints `deduplicated N replayed trades` and fills
`stats['deduplicated_trades']` if a mapping is passed as `stats=`. Entries with
no `tid` pass through untouched. Binance recordings need none of this (verified:
zero duplicates).

The window lives inside one `convert` call, so it stops at a file boundary: a
resubscribe within 30 fills of the daily rotation replays fills belonging to the
previous day's file, and a dataset built from both days keeps that one replay.
Measured on 2026-07-26/27 the two never coincided (zero cross-file `tid`
overlap), and the exposed window per rotation is ~8 s for BTC and ~2 min for ENA.

`convert` builds a `DiffOrderBookSnapshot` of a fixed depth and treats every
`l2Book` message as a complete snapshot of that depth, so feeding it the
interleaved stream would delete levels 6–20 on every fast message and restore
them on the next slow one. `book_mode` selects one cadence and drops the other;
it defaults to `'slow'`, which is exactly how recordings made before the fast
feed existed behave.

The two `l2Book` cadences still cannot be mixed with each other — nothing has
changed there. `'bbo+fast'` fuses a different pair: the five-level `fast`
cadence with the one-level `bbo` touch feed, which the earlier converter dropped
in silence even though it is the majority of frames by count (655 873 of 978 751
on `btc_20260727`). The fused book is a top-5 window in which `bbo` is
authoritative about the touch — mirrored levels past a new best are emitted as
deletions, which is also what keeps the book uncrossed when a `bbo` arrives
through a mirror up to ~0.54 s stale — and each `fast` snapshot is diffed
against the book *as the `bbo` frames left it*, so a level the touch moved and
the snapshot moved back is not silently swallowed. Rows are ordinary
`DEPTH_EVENT`s: the backtest's `Local` processor ignores `DEPTH_BBO_EVENT`.
Measured on `btc_20260727`: 9.6 s to convert the day against 5.6 s for `fast`
alone, 2 110 186 rows against 1 478 946, and the gap between emitted depth rows
drops from a 540 ms median to 81 ms.

Two things about the fused book are easy to get wrong. `delete_out_of_book=False`
is **refused** for `'bbo+fast'`: suppressing a truncation deletion drops the
level from the fused book's mirror at the same moment, so nothing can ever
delete it afterwards and it crosses the book as soon as the market moves through
it — 1 684 955 of 1 685 014 depth rows left the book crossed when measured with
the flag off. And the book is thin more often than "a top-N window jitters at
the deepest level" suggests: a `bbo` that moves the touch *through* mirrored
levels deletes all of them, and nothing restores them until the next `fast`
snapshot. Time-weighted over that day the fused book holds five levels a side
96.9 % of the time and **one level 0.64 %** of it, about nine minutes; `fast`
alone is five levels 100 % of the same span. `num_levels` must match the chosen
cadence (`slow`=20, `fast`=5, `bbo+fast`=5) — a mispairing raises rather than
returning a book of the wrong depth.

The index/funding feeds are **skipped by every converter unless asked for**,
so adding them changed no existing output. `binancefutures.convert` has a
long-standing `opt` flag for exactly this stream, which until now had nothing to
read:

```python
from hftbacktest.data.utils import binancefutures

# opt='m' turns each markPriceUpdate into three rows with custom event ids:
#   100 = index price (the spot basket)   101 = mark price   102 = funding rate
data = binancefutures.convert('btcusd_perp_20260728.gz', opt='m')
```

**That is COIN-M only.** USD-M's index and funding data no longer arrives as
`markPriceUpdate` at all — it is the REST poller's `premiumIndex` elements, a
bare object with no `data` envelope, which `convert` passes over exactly as it
passes over the REST depth snapshots. Nothing is lost and nothing breaks;
`opt='m'` simply finds no rows in a USD-M file. Reading the poller's lines
directly is a `zcat`-and-`json.loads` away:

```bash
zcat btcusdt_20260728.gz | grep '"indexPrice"' | head -1
```

`hyperliquid.convert` has no equivalent yet: its loop handles `trades` and
`l2Book` and silently ignores every other channel, so `activeAssetCtx` is
recorded and preserved but not converted. Read it straight out of the `.gz` in
the meantime — `ctx.oraclePx`, `ctx.markPx` and `ctx.funding` need no
reconstruction.

Other converters that match what this collector records: `binancefutures.convert`
(for `binancefutures`/`binancefuturesum`/`binancefuturescm`) and
`hyperliquid.convert`. There is **no converter for Binance spot** — the
`binance`/`binancespot` collector modes record data nothing in this repo can
currently turn into `.npz`. `mexc.convert` and `tardis.convert` exist but are
for data this collector does not produce.

Two things routinely bite here:

- **`IndexError` during conversion** means the preallocated row buffer was too
  small. Raise `buffer_size` (default 100,000,000 rows ≈ 6.4 GB of virtual
  allocation — it is a reservation, not resident memory).
- **`.npz` key.** The Rust reader hard-codes the array key `data`
  (`hftbacktest/src/backtest/data/reader.rs`). Saving with
  `np.savez_compressed(f, arr)` produces the key `arr_0` and fails at load
  time; use `np.savez_compressed(f, data=arr)`. The `convert*` helpers already
  do the right thing when you pass `output_filename`.

An initial depth snapshot is usually also needed —
see `hftbacktest.data.utils.snapshot.create_last_snapshot` and the
`Data Preparation.ipynb` notebook in the repo root's `examples/`.

---

## Deployment

Scripts live in [`deploy/`](deploy/) and follow the same versioned-release,
symlink-swap pattern as the `myhft` bot: build a tarball, install it into
`releases/<tag>/`, flip an atomic `current` symlink, restart, verify — and
flip back on rollback.

```
/opt/hft-collector/
  current -> releases/<tag>/     atomically swapped
  releases/<tag>/
    bin/{collector,collector-run.sh,rollback.sh}
    etc/{hft-collector@.service,instance.env.example}
    RELEASE                      build manifest
  etc/<instance>.env             operator-authored, never touched by deploys
  data/                          recorded .gz, never touched by deploys
  .previous                      rollback target
```

### One-off host setup

If the recordings get their own volume — recommended, so a full data disk
cannot take the OS with it — **mount it before bootstrapping**. `bootstrap.sh`
sets ownership on `/opt/hft-collector/data`, and `install -d` applies that to
an existing directory, so a volume already mounted there gets chowned by the
script and there is no manual step to forget. Mount afterwards and you hide
the directory it just created, leaving the volume owned by root.

`/opt/hft-collector/data` is the path to use: it is already in the unit's
`ReadWritePaths` and `RequiresMountsFor`, so no drop-in is needed.

```bash
lsblk                                   # find the device; Nitro shows nvme1n1, not xvdf
sudo mkfs.ext4 -L hftdata /dev/nvme1n1  # ONLY if the volume is new and empty
sudo blkid /dev/nvme1n1                 # copy the UUID

sudo mkdir -p /opt/hft-collector/data
echo 'UUID=<uuid> /opt/hft-collector/data ext4 defaults,noatime,nofail 0 2' \
    | sudo tee -a /etc/fstab
sudo systemctl daemon-reload            # fstab changed; refresh the mount units
sudo mount -a
findmnt /opt/hft-collector/data         # confirm before continuing

sudo ./deploy/bootstrap.sh              # creates the user, chowns the volume
```

`bootstrap.sh` reports whether that path is a mount point, so the output tells
you which case you are in. It is idempotent: if the volume was mounted after an
earlier run, just run it again to fix the ownership.

Doing it the other way round is recoverable — mount, then
`sudo chown hftcollector:hftcollector /opt/hft-collector/data` by hand, or
simply re-run `bootstrap.sh`.

`noatime` avoids a metadata write per read on a volume that is append-only in
practice. Mount by UUID rather than `/dev/nvme1n1`: NVMe device names are not
stable across reboots. `nofail` keeps the host bootable and reachable when the
volume is missing — and the unit's `RequiresMountsFor=/opt/hft-collector/data`
is what stops the collector recording in that state.

**That pairing is load-bearing.** Without it, an unmounted volume is worse than
an outage: `collector-run.sh` would create the data directory on the root
filesystem, the free-space check would measure the root filesystem and pass,
recording would look healthy — and everything written would vanish under the
mount point the moment the volume came back.

To mount somewhere else instead, point `COLLECTOR_DATA_DIR` at it and widen
both directives:

```bash
sudo systemctl edit hft-collector@hyperliquid
# [Unit]
# RequiresMountsFor=/mnt/marketdata
# [Service]
# ReadWritePaths=/mnt/marketdata
```

### Build and install

```bash
# on the target host (or a matching Linux box)
./deploy/build-release.sh                 # -> /tmp/hft-collector-release-<tag>.tar.gz

# or cross-compile from macOS — needs zig + cargo-zigbuild
./deploy/cross-build-linux.sh v0.1.0                          # x86_64
COLLECTOR_LINUX_TARGET=aarch64-unknown-linux-gnu \
    ./deploy/cross-build-linux.sh v0.1.0-arm                  # Graviton

sudo ./deploy/install.sh /tmp/hft-collector-release-<tag>.tar.gz
```

Both targets are known to build and produce a correct ELF; `rustup target add`
the one you need first. Prefer **arm64/Graviton** unless something else on the
host requires x86: the collector is idle on either — 12 MB RSS and ~50 kbit/s
for three symbols — so the only difference that survives is instance price.

The cross-build stages a complete upload set at
`/tmp/hft-collector-upload-<tag>/`: the tarball plus `bootstrap.sh`,
`install.sh` and `instance.env.example`, since the tarball alone cannot
bootstrap a host.

`install.sh` runs the tarball's binary and refuses to proceed if its
`--version` disagrees with the `binary_version` in the `RELEASE` manifest —
which is what catches a tarball staged from a stale `target/` directory. A
cross-built tarball records `binary_version=unknown` (the build host cannot
execute a Linux binary), so for those the check degrades to "the binary must at
least run here".

### Instances

One systemd instance per collection job, named after its env file:

```bash
sudo cp /opt/hft-collector/etc/instance.env.example /opt/hft-collector/etc/bybit.env
sudo $EDITOR /opt/hft-collector/etc/bybit.env      # exchange, symbols
sudo systemctl enable --now hft-collector@bybit
```

`COLLECTOR_DATA_DIR` is optional: left out it becomes
`/opt/hft-collector/data/<instance>`, so each instance gets a directory of its
own without anyone remembering to change that line in a copied env file. Set it
only to record somewhere else, and never to a directory another instance is
using — see [Output format](#output-format) for what the lock does about it.

`install.sh` restarts only instances that are **currently running**; an
instance you deliberately stopped stays stopped and picks up the new release
next time it starts. `rollback.sh` casts a wider net — enabled, active *or*
failed — because after a bad deploy the units that most need recovering are
exactly the failed ones.

Both refuse to run without a terminal unless given `-y`, and both exit non-zero
when they abort. A deploy or rollback that did not happen never reports
success.

### Rollback

```bash
sudo /opt/hft-collector/current/bin/rollback.sh            # previous release
sudo /opt/hft-collector/current/bin/rollback.sh --list     # what's available
sudo /opt/hft-collector/current/bin/rollback.sh <tag>      # a specific one
```

A copy of `rollback.sh` ships inside every release, so recovery never requires
the source repo on the host.

### Operating

```bash
journalctl -u 'hft-collector@*' -f                  # all instances
systemctl status hft-collector@bybit
df -h /opt/hft-collector/data                       # this is the one that bites
ls -la /opt/hft-collector/data/bybit | tail
```

**Do not health-check by file mtime.** The gzip encoder writes in ~48 KB
blocks, so a perfectly healthy symbol file sits untouched for minutes between
flushes — measured on a 12-minute run, BTC flushed every 3–4 minutes and SOL
every 5, meaning a `-mmin -5` check would have reported SOL dead while it was
recording normally. It fails the other way too: a stalled collector keeps the
mtime of its last flush.

**A thin symbol makes that much worse, and it caused a false alarm on
2026-07-28.** The block is 48 KB *compressed*, and these feeds compress an
order of magnitude, so the thinner the symbol the longer the interval: a quiet
instrument trickling a few hundred bytes a second can leave its `.gz` mtime
untouched for **~10 minutes** and still be recording every frame. "The file is
not growing" is not "the collector is not recording" — at these compression
ratios the two are barely related. Reading the file while it is open shows the
same thing from the other side: only the blocks that have been flushed are
there, so the tail of a live recording is always missing however healthy it is.

What to check instead, in order:

1. the journal — `journalctl -u hft-collector@<instance> -n 50`;
2. the sidecar, below, which is flushed per record;
3. the stall watchdog, which is the process's own answer to this question and
   ends it if the answer is bad (see [Going silent](#going-silent)).

Check the sidecar instead of the mtime. It is plain text flushed per record, so
it is always current:

```bash
# what this instance is doing right now
tail -5 /opt/hft-collector/data/hyperliquid/_meta_hyperliquid_$(date -u +%Y%m%d).jsonl

# disconnects today
grep -c '"_collector":"disconnected"' .../_meta_hyperliquid_$(date -u +%Y%m%d).jsonl
```

For the data itself, growth over a window longer than the flush interval is the
honest signal — and `-mmin -10` is now the *floor* of that window, not a safe
value, since a thin symbol has been seen going ten minutes between flushes.
Give it real headroom:

```bash
find /opt/hft-collector/data -name "*_$(date -u +%Y%m%d).gz" -mmin -20
```

### Capacity

Measured over a 12-minute run, both book cadences plus bbo and trades:

| Venue / symbol | msg/s | compressed |
|---|---|---|
| Hyperliquid BTC | 7.4 | 22 MB/day |
| Hyperliquid ETH | 6.7 | 23 MB/day |
| Hyperliquid SOL | 5.6 | 15 MB/day |
| Bybit BTCUSDT, depths `1,50` | 32 | 86 MB/day |
| Bybit BTCUSDT, depths `1,50,200` | 43 | 159 MB/day |

Hyperliquid is an order of magnitude lighter than Bybit, and that is a property
of the venue rather than of this collector. Bybit streams an incremental delta
on **every** book change across up to 500 levels; Hyperliquid publishes no
incremental depth channel at all, only throttled snapshots — 20 levels every
~5.4s, 5 levels every ~0.54s, plus BBO at ~0.14s. The venue conflates before it
sends. Treat the small footprint as a warning about resolution, not a saving.

These are quiet-period figures from one 12-minute window. `bbo` and `trades`
are event-driven, so a volatile session costs several times more; size the
volume with headroom.

They also predate the index/funding feeds. Add:

| Feed | Cost | Measured? |
|---|---|---|
| Hyperliquid `activeAssetCtx` | 2.4 MB/day/coin | yes, 2026-07-28 |
| Binance COIN-M `@markPrice@1s` | 2.7 MB/day/symbol | yes, 2026-07-28 |
| Binance USD-M `premiumIndex` poller | ~2.1 MB/day/symbol raw, ~0.3 MB/day compressed | raw is arithmetic; the ratio is extrapolated from the COIN-M row |

About a tenth of a Hyperliquid coin's total, and under 4% of a Bybit symbol's.
Unlike everything else in the table these figures are flat: every one of them is
periodic and does not grow with volatility.

The USD-M poller has a cost the others do not, and it is not on disk.
`GET /fapi/v1/premiumIndex` is fetched **unfiltered** — one request covering all
851 symbols, 188 KB — so it costs **~1.6 GB/day of ingress regardless of how
many symbols the instance records**, five hundred times what it writes. On a
metered or shared link that is the number that matters. See
[Index, oracle and funding](#index-oracle-and-funding).

How much more has since been measured, on Binance UM rather than these two
venues. On 2026-07-26 at 22:00:00 UTC four symbols on `@trade` + `@bookTicker` +
`@depth@0ms` went from ~50 msg/s each to **4402/s and then 5514/s on btcusdt
alone**, ~20 000/s across the four — a hundredfold, sustained over seconds. Size
for the peak, not the average; it is also what the queue capacities in
[Falling behind](#falling-behind) are derived from.

### Running out of space

`--min-free-gb` (default 5) is checked at startup and every minute after.
Below the floor the collector closes its files cleanly and exits non-zero, so
systemd marks the unit failed — rather than writing until the filesystem is
full and leaving a half-written gzip member. `0` disables it.

Free space is also written to the sidecar every minute
(`{"_collector":"disk","free_bytes":…}`), which gives each recording its own
capacity history with no metrics agent involved.

For remote alerting, hang `OnFailure=` off the unit: every fail-closed path in
the collector ends in a non-zero exit, so unit failure is the single signal to
watch. See the commented example in `deploy/hft-collector@.service`.

**Restarting always costs a gap** of a second or two per instance while the
WebSocket reconnects, and both deploy and rollback restart. There is no
zero-gap path: two processes writing the same file would interleave gzip
members from different streams.

### Taking finished days off the host

Nothing on the host prunes anything, by design — the collector stops rather
than overwrite. A full USD-M symbol set burns 3-5 GB a day against a 53 GB
volume, so something has to move the data, and that something is
`deploy/offload.sh`. It runs **from the operator machine**:

```bash
deploy/offload.sh --host user@box --target ~/marketdata --dry-run
deploy/offload.sh --host user@box --target ~/marketdata
```

Per instance directory, in this order: list the host's finalized files → sha256
**on the host** → rsync here → sha256 **here** and compare → `gzip -t` every
`.gz` that arrived → *only then* `rm` on the host. Nothing is deleted that was
not just verified byte for byte, and a `.gz` that copies perfectly but will not
decode is left on the host and reported.

Three things it will not do:

* **Touch today's files.** Finalized means "older than the day the HOST says it
  is", asked over ssh rather than assumed — the host's clock is what names the
  files, and a guess an hour either side of midnight deletes one that is still
  open. The date rule is asserted a second time immediately before the delete,
  because that is the one irreversible step.
* **Touch a name it does not recognise.** Three shapes are known —
  `<symbol>_<day>.gz`, `_meta_<exchange>_<day>.jsonl`, `gate/<day>.{txt,json}`
  — and everything else is left alone and counted. The same allowlist is what
  keeps the remote commands safe: only `[A-Za-z0-9._-]` and a literal `gate/`
  ever reach a remote shell.
* **Delete the gate reports.** They are copied and left: kilobytes against
  gigabytes, and a day whose data has gone is exactly when its report is worth
  having on the box beside the journal.

It is idempotent. Interrupt it anywhere and run it again — rsync re-copies
nothing it already has, verification always runs from scratch, and a file
already removed is simply not listed. `--keep-on-host` copies and verifies
without deleting; `--instances a,b` narrows it. `--dry-run` prints the plan and
the byte count.

### The daily quality gate

`quality_report.py` answers the one question the collector process cannot
answer about itself — *did we actually get everything we asked for, and is it
readable?* — and the gate timer runs it on the host every night so the answer
arrives while yesterday can still be acted on.

```bash
sudo systemctl enable --now hft-collector-gate@all.timer
systemctl list-timers 'hft-collector-gate@*'
```

At **00:35 UTC** it checks **yesterday** for every instance data directory on
the host and writes the report next to the data:

```
/opt/hft-collector/data/<instance>/gate/<YYYYMMDD>.txt     the operator's view
/opt/hft-collector/data/<instance>/gate/<YYYYMMDD>.json    quality-report-v1
```

A **red day exits non-zero**, which lands `hft-collector-gate@<set>.service` in
`failed` — visible in `systemctl list-units --failed` and the hook any
`OnFailure=` alerting hangs off, the same signal the collector's own
fail-closed paths produce. The findings are echoed into the journal too, so a
notification does not have to be followed by an ssh session to learn what it
said. The instance token is a *set*: `all` is every instance here, or name one.

Two things worth knowing before you enable it:

* **It competes with the recording for two vCPUs.** A day is gigabytes of gzip
  and the gate decodes every byte, on a box whose only job is to not fall
  behind — and on a burstable instance that CPU is metered. So it is run at
  `Nice=19` with idle I/O priority: whenever the collector and the gate both
  want the CPU, the collector gets it and the gate simply takes longer. That is
  the intended trade; a late report costs nothing and a dropped frame is gone.
  A hard `CPUQuota=` is available as a drop-in and is deliberately not the
  default.
* **One report per directory, not one per host.** `quality_report.py` takes one
  directory per venue and refuses two of the same venue in one run — which is
  exactly the configuration a duplicated USD-M recording creates (see the
  signal union below). One run per directory sidesteps that, makes "next to the
  data" well defined, and produces the per-instance JSON that
  `build_dataset.py --binance-report-b` consumes directly.

A caveat the timer cannot design away: files rotate **lazily**, so yesterday's
`.gz` gets its gzip trailer on the first write after midnight. A liquid feed
rotates within milliseconds and never notices; a symbol thin enough to go from
23:59 to 00:35 without a print still has an unterminated member when the gate
looks, and an unterminated member on a finalized day is corruption as far as
the report is concerned. Move that instance's timer later with a drop-in rather
than learning to ignore a red.

### Recording the signal twice

Measured on the hosts: one Binance USD-M socket loses **0.2-0.4% of the day**
to reconnects, in clusters of 0.5-0.8 s, 10-19 times a day — and two sockets to
the same venue drop at **uncorrelated** times. So the signal is worth recording
twice, as two ordinary instances with different names and different data
directories, and joining them offline:

```bash
build_dataset.py --quality-report combined.json \
    --binance-report-b /data/um_b/gate/20260728.json \
    --hl-symbol BTC --binance-symbol BTCUSDT --out-dir dataset/
```

The signal array is then the **union** of both recordings' `@bookTicker`
frames, deduplicated by the venue's own update id `u`, keeping the earliest
`local_ts` for each — the recovering socket is the one that was up. Coverage is
the union of the two intervals and a gap survives only where **both** were
dark. The manifest records both inputs, each source's contribution, and how
many frames the union recovered.

Hyperliquid is deliberately **not** duplicated: its reconnect losses are
already mitigated by the 30-trade replay and the `bbo` fusion.

The secondary is **additive only**: the window, the day set and the
required-stream gate all come from the primary report, so a red primary still
refuses the build and a red secondary cannot.

Two things the union checks rather than assumes, because both fail silently:

* **One clock.** "Earliest `local_ts` wins" reads as *the socket that was up*
  only while both sockets stamp their frames against the same clock. Put the
  second recording on a second host whose clock is behind and it wins **every**
  update the two share, moving the whole signal timeline by the skew — and a
  skewed recording that also recovers a few frames looks exactly like a healthy
  one. So the build measures it: the median of *secondary receive time −
  primary receive time* over the update ids both recordings saw. On one host
  that is the difference in socket receive latency, single-digit milliseconds;
  it is written to the manifest as `signal.union.clock_offset_ns` either way,
  and one larger than the whole `--max-signal-age-ms` freshness window refuses
  the build. Mode A selects *the last row with `local_ts <= now`*, so a skew
  carried into the dataset lands in every decision the backtest makes. When the
  two recordings share **no** update id there is nothing to measure it with, and
  that is said out loud rather than passed: two sockets to one venue and one
  symbol see the same book updates, so it means the *primary* has no frame
  inside the window and the signal rests on the second recording alone. Legal —
  it is what the union is for — but until the warning existed it was also the
  one shape in which every number stayed healthy: `clock_offset_ns` null,
  `recovered_rows == rows`, `primary_only_rows` zero.
* **One `u`, one book.** The dedup key is only sound while one update id means
  one book state. Where it does not — a matching engine that restarted its
  counter inside the window is the realistic way — the dict would keep whichever
  frame arrived first and the other simply would not be in the dataset, with
  `rows`, `recovered_rows` and every coverage number still looking right. Two
  frames claiming one `u` with different prices therefore refuse the build; two
  claiming it with the *same* prices are the ordinary case and are just
  deduplicated.

### Falling behind

The two internal hand-offs are bounded (`src/queue.rs`), and since they fail for
different reasons they are bounded differently:

| Hop | Capacity | Consumer | What a full queue means |
|---|---|---|---|
| socket reader → parser | 16 384 | JSON parse, no syscalls | the parser is not keeping up with the tape |
| parser → writer | 32 768 | gzip + `write(2)` | the writer stalled or stopped |

If either fills, the collector stops: `{"_collector":"queue_overflow", …}` goes
to the sidecar, whatever is still queued is written, the files are closed, and
the exit is non-zero. It never waits for room, and it never drops a message to
make some.

A hand-off can also lose its consumer outright, which is what an already-ended
collection task looks like from the producer side. That stops the collector the
same way but is recorded as `{"_collector":"hand_off_closed", …}`: nothing was
full, and reading it as an overflow would send a gap investigation after a queue
depth that was never reached.

That is deliberate. An unbounded queue turns a stalled writer into unbounded
memory growth while every outward sign — connected, receiving, no errors — still
looks healthy, and what survives the OOM killer is a set of unterminated gzip
members. A queue deep enough to absorb a rotation but not a stall turns the same
fault into a failed unit instead.

**The writer capacity is measured, not proposed.** On 2026-07-26 at 22:00:00 UTC
a burst took four Binance UM symbols (`@trade` + `@bookTicker` + `@depth@0ms`)
from ~50 msg/s each to ~20 000 msg/s aggregate — 4402/s, then 5514/s, on btcusdt
alone. The parser → writer hop, then 4096 deep, filled and the collector exited
1; systemd restarted it 5s later and the restarted process rode out the *same
burst still accelerating* (6118/s at 22:00:11) without filling anything. One
overflow in 12 hours, a ~5s hole.

That second half is what sized the fix. The writer keeps up with the rate; 4096
simply is not enough buffer to ride out one stall of its own, because 4096 ÷
20 000 is 205 ms and the writer is the only stage here that can block in the
kernel — writeback, a gzip flush, the UTC-day rotation. 32 768 buys 1.6s at that
peak, and 2.7 minutes at the background rate, so a full queue still means the
writer has stopped rather than that traffic was briefly bursty.

The memory that buys: a typical UM frame is 300–600 B, so a full writer hop is
~20 MB. The worst case is the ~50 KB REST depth snapshots, but those are
throttled to 100/min, and a queue only stays full while nothing is being
dequeued — which is also what trips `--stall-timeout-min` and ends the process
inside five minutes. At most ~500 of them, so ~25 MB. Call the hop **under
50 MB**, and under 61 MB with both hops full, against 2 GB of host. Tokio's
bounded channels do not preallocate, so this is a ceiling and not a resting
cost. Memory is not what limits the number; the stall budget is.

Two caveats on that number, neither of them a risk at this scale. Setting
`COLLECTOR_STALL_TIMEOUT_MIN=0` disarms the watchdog, and with it the ~25 MB
half of the cap. And the frame sizes are Binance UM's, while the capacity is
shared by every backend: a Bybit instance carrying 1–2 KB `orderbook.200`
deltas is nearer **65 MB** — ~98 MB with its socket hop full alongside — and its
snapshots come over the WebSocket, so the 100/min throttle, which gates only the
Binance REST refetch, does not bound them. Four such instances wedged at once is
under 400 MB on the 2 GB host, so the conclusion holds and the number does not.

**The socket capacity is measured too, since 2026-07-29.** It stayed at 4096
when the writer hop was raised, on the argument that its consumer parses JSON
and hands over without ever making a syscall, so it can only stall on losing the
CPU — tens of milliseconds, against the 205 ms that 4096 buys at the measured
peak.

At 03:30:02 UTC on 2026-07-29 both `binancefuturesum` instances — separate
processes, separate sockets — filled this hop within 100 ms of each other and
exited, each leaving a ~6s hole and restarting clean. The `cpu` gauge either
side of it reads ~85% idle and 1.4% steal, so neither had lost the CPU, and
neither lost it at the same instant as the other by coincidence: the market
moved. The argument was too narrow. Two `serde_json` parsers on 2 vCPUs have a
*throughput*, and a tape peak above it fills this hop at the difference between
the two rates for as long as the excursion lasts — nothing has to stall.

So it is now sized by the duration of an excursion, as the writer hop is sized
by the duration of a stall: **16 384**, or 0.82s at the measured peak. The
budget is stated as a multiple of the depth that failed rather than of the
excursion, because nothing measured the excursion — both processes died 205 ms
into it. The writer hop is untouched: it held through the same event with 8106
and 9064 records queued, a quarter of its depth, and both backlogs reached disk
on the way out.

Two soft spots in that arithmetic, both written out in `queue.rs` and neither
closeable without another measurement. The 0.82s divides by the arrival rate,
which is the fill time for a parser that has stopped dead rather than one that
is merely out-run, so it is a floor — as is the 205 ms it is compared against,
computed the same way, which is why what the pair really decides is the 4×
ratio. And *the rate is 2026-07-26's*: nobody counted frames at 03:30 on the
29th, so if that excursion was faster than 20 000 msg/s every figure here
shrinks in proportion. A third burst is worth more than another round of
reasoning.

One cost the depth does carry: `drain_backlog` empties the **writer** hop on
the way out and nothing drains this one, so whatever is sitting here when the
process stops is lost. That went from ~4096 unparsed frames to ~16 384 — about
0.6s more of the ~6s hole a restart leaves anyway, which is why it did not
change the choice.

It cannot simply keep growing. This hop's overflow is the only *specific*
diagnosis the process has for a parser that cannot keep up, and it races the
stall watchdog — a parser that has stopped dequeuing produces no writes either,
so whichever fires first is what the operator gets, and the watchdog can only
say "nothing for five minutes". 16 384 reports in ~82s at the background rate,
or ~164s if the market is half as busy as when it was measured; a hop as deep as
the writer's would lose that race outright. Both bounds are pinned by tests
(`queue.rs` and `main.rs`), and 16 384 is the only power of two between them.
The same arithmetic puts a **floor under `--stall-timeout-min` of 3 minutes** —
it was 1 while the hop was 4096. Set it lower and the watchdog wins the race,
so the operator gets "silence" instead of the hop that broke.
One consequence reaches the offline gate: the deeper hop widens the largest
honest cross-stream overtake to 819 ms, so `quality_report.py`'s interleave
tolerance moved from 250 ms to 1s with it. That tolerance covers the pair this
hop actually separates — a REST depth snapshot against a WebSocket frame. A
WebSocket frame written behind a `premiumIndex` poll waited in this hop and the
writer's alike, so nothing over these capacities bounds it and the gate grades
it without one. The other way round — a poll written behind a frame stamped
later — keeps a ceiling of the same 1s, but a constant of its own
(`POLLER_HOP_CEILING_NS`), read off what the poller's hand-off has actually been
seen doing: raising this capacity again must not widen that one.

One case escapes all of it. If a write blocks for ever in the kernel — a hung
mount, a device that stops answering — the main loop never gets back to notice
the signal, so the process stops recording without exiting. Memory still stays
bounded and the reason is in `journalctl`, but only an external watchdog
(systemd `WatchdogSec`) turns that into a restart.

### Starting up into a network blip

Two venues make REST calls before anything is recorded: Hyperliquid resolves
every coin against `/info` (plus `spotMeta` for the collateral name), and
Lighter reads the market catalog it has no addressing without. Both refuse to
start when the call fails, which is right — a collector that cannot name what it
is recording should not record — but until 2026-07-29 each of them concluded
that from a **single** attempt.

At 06:09:48 that day a Hyperliquid instance restarted, met one `error sending
request` on `/info`, and exited a second after starting. The next systemd
restart succeeded immediately, which is what says the fault was the network and
not the venue.

So a startup resolve is now tried up to **three times, waiting 2s and then 4s**,
with a `WARN` naming the endpoint on each failed attempt. Every attempt still
carries its own timeout (15s for `/info`, 15s for the catalog), so a venue that
hangs rather than refuses is still bounded. After the last attempt the behaviour
is exactly what it was: the venue's own error, `{"_collector":
"symbol_check_failed", …}` in the sidecar, and a non-zero exit.

The bound is bigger than the ladder, and worth knowing before you read a slow
start as a hang. A venue that **refuses** costs only the 6s of backoff. A venue
that **hangs** costs 3 × 15s + 6s = 51s per endpoint, and Hyperliquid makes one
call for `spotMeta` plus one per referenced dex — so the shipped example config
takes ~153s to refuse to start where it used to take ~45s. That is still inside
`StartLimitIntervalSec=3600`, so ten failed starts still land the unit in
`failed` and fire the alert (~26 minutes rather than ~6). It stops being inside
the hour at **six** referenced dexes — seven endpoints is 362s a cycle against
the 360s that ten starts allow — and past that the unit would restart for ever
without ever reaching `failed`, which is the silent crash-loop
`StartLimitIntervalSec` exists to prevent. A config that wide wants a
`TimeoutStartSec` on the unit.

The ladder is deliberately short and deliberately not the reconnect ladder
(`src/backoff.rs` holds both, side by side, for that reason). A *running*
collector reconnects for as long as the venue is away, because the alternative
is to stop recording over a blip. A *starting* one has recorded nothing, so
retrying for ever would leave systemd believing the unit is coming up while
nothing is captured and nothing has failed.

What is **not** retried: mid-run REST — the Binance depth-snapshot refetch and
the `premiumIndex` poller — which have their own policies, because a snapshot is
worthless once stale and a poll is superseded by the next one. Nor Lighter's
`--no-symbol-check` refusal, which is a configuration error with the same answer
every time. Nor Lighter's WebSocket geoblocking probe, whose failure is usually
a jurisdiction rather than a hiccup.

### Going silent

`--stall-timeout-min` (default 5, `0` disables) stops the collector when **no
market data at all** has been written for that long: a `{"_collector":"stalled",
…}` record goes to the sidecar, the files are closed and the exit is non-zero.

It exists for the failures nothing else reports. A venue that accepts a
subscription and then sends nothing, a reconnect loop that reconnects but never
resubscribes, a frame parsed into no stream at all — none of those raise an
error, and every outward sign stays healthy while the recording is empty. The
clock starts at startup, so a collector that never records anything is caught
too.

Sidecar records do not count as data. That matters most during a reconnect
storm: every retry writes `subscribe`, `connected`/`dial_failed` and
`disconnected` to `_meta`, and with the 500 ms backoff floor a venue refusing
connections produces a couple a second for as long as it stays down. Counting
those would leave the watchdog satisfied by a collector recording nothing.

**5 minutes is a proposal, not a measurement** — open decision 2 of
`docs/design-multi-venue-collection.md`. For scale, the slowest legitimate feed
is Hyperliquid's plain `l2Book` at ~5.4s, so the default sits roughly fiftyfold
above anything real. Lower it once a quiet-period gap has actually been
measured.

**What an operator sees.** The exit is non-zero, so systemd restarts the
instance (`Restart=always`, `RestartSec=5s`) and it trips again ~305 s later.
The unit therefore reaches `failed` — and fires `OnFailure=` — only once the
start limit is hit: ten restarts, so roughly **50 minutes** after the feed went
quiet, with a fresh gzip member and a `session_start` record per attempt. That
is the shipped `StartLimitBurst=10` / `StartLimitIntervalSec=3600`; the interval
has to stay well above ten stall periods or the limit is never reached and the
unit restarts for ever without ever failing.

**It counts what the venue sent, not what the process wrote.** The distinction
did not exist until USD-M got a REST poller: every other line in a symbol file
arrives because a socket delivered something, but a `premiumIndex` element
arrives because a timer fired, and it is filed under `BTCUSDT` exactly as a
`bookTicker` frame is. Counting one would have disarmed this guard outright —
measured 2026-07-28 with `fstream` blackholed, a USD-M instance ran 120 s past a
60 s stall timeout on index samples alone and would have run all day, with
systemd reporting it healthy. So the poller's records travel a hop of their own
(`queue::POLLER_HOP`) and the watchdog is fed from the venue hop only; see
`watchdog::Source`. The samples are still written — they are data — they just do
not vouch for anything.

**Total silence now means more than it used to.** Every venue records at least
one *periodic* feed — Hyperliquid's `activeAssetCtx` at 1/s, COIN-M's
`@markPrice@1s` at 1/s — and those arrive whether or not the market moves and
whether or not the book changes. So on Hyperliquid and Binance futures the
watchdog tripping no longer means "the market went quiet"; it means the
always-on feed died too, which narrows the diagnosis considerably:

| Venue | What a trip rules out |
|---|---|
| `hyperliquid` | the `activeAssetCtx` subscription, on every coin at once — so the socket or the process, not one channel |
| `binancefuturescm` | the same, via `markPriceUpdate` |
| `binancefuturesum` | the socket. The `premiumIndex` poller is deliberately not counted, so a trip says the WebSocket stopped and says nothing about REST — check `_meta` for `poller_degraded` to learn whether the venue or this host was the problem |
| `lighter` | every market's book, tape, ticker **and** `market_stats` at once. None of the four is periodic — `market_stats` is event-driven, so its silence is not evidence on its own — but four channels across every market going quiet together is the socket or the process, not a still market. The connection's own 90 s idle check normally reaches that first and reconnects |
| `binance`, `binancespot`, `bybit` | nothing extra; these record order flow only, and a genuinely dead market still trips it |

The USD-M row is the useful one, but for the opposite reason to the others: the
two paths are reported separately on purpose. A trip with no `poller_degraded`
beside it is a WebSocket fault with REST still answering; both together are the
host or the network.

**What it still does not catch**, and must not be mistaken for:

- a dead depth stream while trades keep arriving, or the reverse;
- one Hyperliquid cadence stopping while the other two continue;
- a partially accepted subscription — indistinguishable from a full one here.

"One symbol of ten that stopped" used to be on that list. It is now the
[per-symbol liveness gauge](#host-gauges-clock-cpu-and-per-symbol-liveness) below,
which warns but deliberately does not stop the collector.

Answering *did we get everything we asked for* is an offline report over
finished files, not a decision this process can make about itself.

### Host gauges: clock, CPU and per-symbol liveness

Three measurements written to the sidecar on the **same one-minute timer as the
disk gauge**, for the same reason: a recording that carries its own history
needs no metrics agent, and `_meta` is the one file an operator can tail live.
None of them ever stops the collector.

| Record | Says | Warns when |
|---|---|---|
| `{"_collector":"clock", …}` | `sync`, `est_error_us`, `max_error_us`, `offset_us`, `freq_ppm` | the kernel reports `STA_UNSYNC`, or `max_error_us` > 4 000 000 |
| `{"_collector":"cpu", …}` | `steal_pct`, `user_pct`, `system_pct`, `idle_pct` over the last minute | `steal_pct` > 10 |
| `{"_collector":"liveness", …}` | `threshold_s`, and `ages_s` — seconds since anything was recorded, per symbol | one symbol's age passes `--liveness-timeout-s` |

All three warnings are **edge-triggered**: once when the fault starts, once
when it clears. A fault that lasts hours is one line in the journal, not sixty
an hour.

#### CPU, and how much of it the hypervisor took

The collector's failure mode is always the same sentence — *the writer cannot
keep up* — and it has two completely different causes. Either the venue is
sending more than this process can gzip, or **the host is not being given the
CPU it thinks it has**. From inside the process those are indistinguishable:
the queue fills, `queue_overflow` is written, the run ends, and every other
gauge reads the same in both cases.

The recording boxes are burstable (t4-class), where the second cause is not
hypothetical. Such an instance is entitled to a baseline fraction of a vCPU and
banks credits while it stays under it; spend the credits and the hypervisor
throttles it back to that baseline, accounting the difference as `steal`. So
the failure that looks most like "the venue flooded us" is the one the venue
had no part in, and the only witness is a counter in `/proc/stat`.

The four numbers are deltas over the minute, not averages since boot, and they
**sum to 100** — `nice` is folded into user, `irq`/`softirq` into system, and
`iowait` into idle, so an operator who reads three of them can name the fourth.
`guest` is not added to the total: Linux already counts it inside `user`, and
double-counting it would inflate the denominator and quietly shrink the one
number this gauge is for.

The **10% steal threshold** sits between two measured ends. Ordinary shared
tenancy costs a fraction of a percent, so anything inside that band would fire
on healthy hosts. A burstable instance out of credits is capped at a 10-40%
baseline, so a process that wants a whole vCPU is handed back 60-90% — the
fault does not arrive gently at 12%, it arrives as most of the machine. 10% is
an order of magnitude above the noise and well below the fault, and it fires
*early*: at 10% the collector is still keeping up, so the warning arrives while
there is time to resize or shed a symbol, rather than alongside the
`queue_overflow` it was supposed to explain.

Nothing here ever stops the collector. A throttled host is the one failure the
collector can do nothing about, and exiting over it would take the recording
down for the duration of a hypervisor's mood.

Off Linux there is no `/proc/stat`, so the record is
`{"_collector":"cpu","unsupported":true,"platform":"macos"}` rather than a
host with no steal — and the first sample of any run records
`{"first_sample":true}`, because cumulative counters have nothing to subtract
from yet and a zeroed reading there would open every recording with a minute of
"no steal" that was never measured.

**What an operator does.** `journalctl` says *"this host is not getting the CPU
it asked for"*. Check the instance's CPU credit balance; if it is at zero the
answer is a larger instance or fewer symbols, not a restart. Cross-check
`grep '"_collector":"cpu"' _meta_*.jsonl` against the same minutes in the
sidecar's `disk` and queue records — steal high and idle low at the moment the
queue filled is the throttling case, and steal near zero with idle near zero is
genuinely too much data for this box.

#### Clock discipline

Every line in every file is stamped with the host's clock at receive time, and
nothing checked that the host's clock meant anything. On 2026-07-27 a box came
back from a reboot undisciplined, recorded a full day, and the offline time
policy rejected all of it on a −7.04 ms local-versus-exchange skew — discovered
at assembly time, a day after the only moment it could have been fixed.

The gauge reads `adjtimex(2)`, which is the **kernel's** view of the clock
`Utc::now()` actually reads. Not `chronyc tracking`: that is chrony's model of
the correction it is about to apply, one step removed from what stamped the
line, and a chrony running happily while failing to discipline the kernel is
exactly the reboot case. It is also one unprivileged syscall rather than a
subprocess a minute, and it works identically under `systemd-timesyncd`.

The **4 s `max_error_us` threshold** is set where a false alarm is impossible,
not where a small skew is caught. The kernel grows `maxerror` by 500 µs/s
between updates and every daemon resets it on a successful poll, so the widest
legitimate excursion is `maxpoll × 500 µs` — ~512 ms under chrony's default
1024 s `maxpoll`, but **~1.024 s under `systemd-timesyncd`**, whose
`PollIntervalMaxSec` is 2048 s. Both are read here, so the threshold has to
clear both: 4 s is a quarter of the kernel's own saturation at 16 s, so it still
reports four times sooner than the kernel would, with room for a few missed
polls. A 7 ms skew is **not** what this warns about — no threshold that caught
7 ms could stay quiet on a healthy host. The five recorded numbers are how a
7 ms skew is found, exactly and after the fact.

Off Linux there is no `adjtimex`, so the record is
`{"_collector":"clock","unsupported":true,"platform":"macos"}` rather than a
healthy-looking reading. A dev run must not be able to produce a recording that
claims a disciplined clock.

**What an operator does.** `journalctl` says *"the host clock is not
disciplined"*. Check `chronyc tracking` / `timedatectl show-timesync`, and
whether the time service came up at all after the last reboot. The data being
recorded is still worth keeping — fix the clock and the next sample clears the
alarm — but note the window: everything stamped inside it is suspect, and
`quality_report.py` raises a yellow `clock_unsynced` note over exactly that
window when the day is checked.

#### Per-symbol liveness

`--stall-timeout-min` fires only on **total** silence, so nine symbols arriving
and one silent looks perfectly healthy. `ages_s` is seconds since anything was
last written for each symbol, and a symbol whose age passes the threshold gets
one warning.

It is **seeded with the symbols that were asked for**, aged from startup. The
worst version of the fault is a subscription the venue accepted for nine of ten
symbols: the tenth never produces a single record, so a gauge that only knew
about symbols it had seen would have nothing to report.

`--liveness-timeout-s` defaults to the slowest feed the venue serves *per
symbol*, since any one of a symbol's streams resets its clock:

| Venue | Default | Why |
|---|---|---|
| `hyperliquid` | 60 s | `activeAssetCtx` is per coin at ~1/s and always on |
| `binancefuturescm` | 60 s | the same, via `$symbol@markPrice@1s` |
| `lighter` | 60 s | four channels a market and any one resets the clock; the slowest, `market_stats`, ran at a 0.25 s median and a 1.77 s worst interval (2026-07-28). Event-driven rather than periodic, so this is thirty times a measured worst case rather than sixty times a fixed period |
| everything else | 300 s | order flow only, where a thin symbol is legitimately quiet; USD-M's `premiumIndex` is the collector's own poller and does not count |

`0` disables the **warning** and not the measurement: `ages_s` is still
recorded, because an operator silencing a noisy alarm has not asked to stop
recording what the collector saw.

**It counts what the venue sent**, on exactly the same terms as the stall
watchdog — sidecar records under `_meta` do not count, and neither do the
`premiumIndex` poller's, which file under `BTCUSDT` like any WebSocket frame and
keep arriving with the socket dead.

**It is per symbol, not per symbol × stream.** The writer only ever sees
`(timestamp, symbol, payload)`, and the stream is inside the payload. Each
backend already knows it — it parses every frame anyway — so the cost is not a
parse but carrying the name: an extra owned string per record across the writer
hop at up to ~20 000 msg/s, for a number read once a minute, or an interning
scheme threaded through five parsers and the queue's element type. Until that is
done, "depth died while trades flow" stays a limitation of this process and a
job for the offline report. "One coin went quiet" is caught here.

**What an operator does.** `journalctl` names the symbol and its age. Check
`_meta` for a `disconnected`/`connected` pair near that minute — a reconnect
that failed to resubscribe hits every symbol, so one symbol alone points at the
venue dropping a subscription instead. Restarting the instance re-subscribes
everything and costs the usual second or two of gap; that is the fix in almost
every case. If it is one thin symbol in a quiet market, raise
`--liveness-timeout-s` for that instance rather than learning to ignore the
warning.

---

## Known limitations

Worth knowing before you trust a dataset.

- **Reconnects are silent in the *symbol* files on every venue.** The gap can
  be attributed — the sidecar carries `subscribe`/`connected`/`disconnected`
  markers on all five backends — but only by lining the two files up on
  timestamps; nothing marks the hole where it happens. Sequence numbers in the
  payloads (Binance `pu`/`U`, Bybit `u`/`seq`) detect one after the fact,
  independently of the sidecar.
- **The reconnect backoff ladder only measures consecutive fast failures.**
  All five backends now share `src/backoff.rs` — 500 ms, 1 s, 5 s, 10 s, with
  every rung reachable and a floor where four of them used to retry with no
  delay at all. But `error_count` is reset in the same iteration it is
  incremented whenever the connection survived 30 s, so a venue that accepts a
  connection and drops it after a minute, forever, is retried at the floor
  forever.
- **A Binance stream name that does not exist is not an error.** It is accepted
  in the combined-stream URL, acked by `SUBSCRIBE`, and then never served —
  measured 2026-07-28, `btcusdt@totalnonsense` was indistinguishable from a
  stream that exists and is quiet. A typo in the stream list therefore costs a
  feed silently, which is what happened to `@markPrice@1s` while it sat
  commented out as `@@markPrice@1s`. The names now live in
  `binancefutures{um,cm}::STREAMS` with a test on their shape, because nothing
  at runtime will ever object.
- **A stream name that exists but belongs to another connection class is not
  an error either**, and that is the worse half. USD-M's whole markPrice class
  behaves exactly like the typo above on a `/public` (or legacy unrouted)
  connection: acked, silent, no error, indefinitely (measured 2026-07-28, and
  then explained by Binance's 2026-03-06 routed-path split — see
  [Index, oracle and funding](#index-oracle-and-funding)). There is no
  subscription-level check that can catch it, so a feed disappearing from a
  venue is only ever visible as its absence from a finished recording. That is
  what `tools/quality_report.py` is for, and it is why an *informational* stream
  absent from a day is recorded as a fact rather than raised as a warning:
  neither the operator nor the collector could have done anything about it.
- **A write is not evidence unless the venue caused it.** The stall watchdog
  counts records off the venue hop only, because USD-M's REST poller writes into
  the symbol files on a timer of its own and would otherwise have kept the
  watchdog satisfied through a completely dead socket — measured, 120 s past a
  60 s timeout and still going. Any future producer that is not a venue feed has
  to travel `queue::POLLER_HOP` for the same reason; putting it on the writer
  hop would silently disarm the guard again, and nothing would say so.
- **Symbol validation is Hyperliquid and Lighter only.** On both it is on by
  default and refuses to start on an unknown name — one bad coin closes
  Hyperliquid's whole WebSocket, and Lighter cannot address a market without
  resolving it at all. Bybit and Binance have no equivalent check yet; a typo
  there still produces a partial recording.
- **Lighter has no converter.** Nothing in `py-hftbacktest` reads these files
  yet, so a Lighter recording is raw material rather than a dataset, and
  `quality_report.py` treats every one of its streams as checked-but-not-
  required for that reason. The diffs are recorded exactly as sent, deletions
  (`"size":"0.0000"`) included, and no book is maintained at capture time —
  which is what leaves the merge policy open, and what makes writing that
  converter the next piece of work rather than a rerun.
- **A recovered Lighter book is not a complete one.** The repair after a nonce
  break gets a fresh snapshot, so the recording resumes correctly, but the
  updates lost between the break and the snapshot are gone — the sidecar's
  `sequence_gap` says how far the chain jumped, and that is all anyone will
  ever know about what was in them. Two further limits on the repair itself:
  a market is repaired at most once a minute, so a persistently lossy market
  spends most of that minute on a chain known to be broken; and nothing in the
  process reads the venue's answer, so if the venue ever stops honouring
  `unsubscribe`+`subscribe` the evidence will be a `sequence_gap` `count` that
  climbs without a recovery rather than an error. The wire sequence is pinned
  by a test against captured frames, not merely by this paragraph.
- **Lighter replays the last 50 trades on every subscribe, and nothing
  deduplicates them.** One `subscribed/trade` frame per market carries 50
  prints spanning ~12 s of history (measured: ~94 KB), and one arrives on every
  reconnect. Recording it verbatim is correct and it does not disturb the
  offline report — `classify` keys on the channel head and nothing iterates the
  array — but a converter that expands `trades` will emit phantom prints unless
  it skips a `trade_id` it has already seen. This is the same trap Hyperliquid's
  `tid` guard exists for (30 fills there, 223 phantom rows measured for BTC on a
  10-reconnect day); Lighter's exposure is larger, and `trade_id` is present on
  every print.
- **`binancefuturesum` and `binancefuturescm` are the same module twice**,
  differing only in their endpoints, their test fixtures and one `handle`
  branch. `binance` (spot) is a third near-copy. A fix to one needs applying to
  the others; only the socket loop is shared (`src/pump.rs`).
- **Throttler is off by one.** `Throttler` compares `len() > rate_limit`, so it
  permits `rate_limit + 1` calls per 60s window.
- **Spot vs futures gap detection differ.** Binance USD-M/COIN-M use
  `pu != prev_u`; spot uses `U != prev_u + 1`. Easy to copy wrong when adding
  a venue.
