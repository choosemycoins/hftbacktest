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
both, side by side and unmerged. The converter then selects exactly one with its
`book_mode` argument and drops the other; **fusing the two cadences is not
implemented** and `hyperliquid.convert` raises on any other value. Recording
both is what keeps the choice open, and revisitable. Messages from the fast feed
carry `"fast": true` in their payload, which is how they are told apart on the
way back out.

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
producer**. It writes straight to the writer while WebSocket frames queue
through the socket hop first, so a `premiumIndex` line can be written ahead of a
book tick stamped microseconds earlier. That is an interleave, not a defect, and
the offline report knows it (`_SECOND_PRODUCER` in `tools/quality_report.py`) —
it is the same situation the REST depth snapshot has always been in, but now at
8640 writes a day instead of a dozen.

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
SIGKILL. At ~90 KB/day it does not need compressing. The different extension
also keeps it out of the `*_<date>.gz` wildcards that feed the converters.

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
| `{"channel":"subscriptionResponse", …}` | the venue's ack, echoing its normalised parameters | hyperliquid |
| `{"channel":"error", …}` | venue rejections | hyperliquid |
| `{"channel":"pong", …}` | liveness during a stretch with no market data | hyperliquid |
| `{"success":…,"ret_msg":…}` | subscribe ack, successful or not | bybit |

A `subscribe` followed by `dial_failed` is a socket that never came up. A
`connected` with no market data behind it is a subscription the venue accepted
and never served. Neither was distinguishable from a quiet market before.

`connected_for_ms` appears only on records for a connection that existed, which
is why a failed dial is its own event rather than a `disconnected` reporting how
long a DNS or TLS stall took. A refused internal hand-off writes no end-of-stream
record at all: it would have to travel the hop that just refused a market-data
frame, and `queue_overflow`/`hand_off_closed` already name it from the other end.

`poller_degraded` is the one record that reports a fault the collector then
carries on through. Every other `_collector` record above is either routine or
terminal; this one says a feed is missing while the recording continues, because
the feed in question is auxiliary and stopping over it would be out of all
proportion (see [Index, oracle and funding](#index-oracle-and-funding)). It is
written once per outage rather than once per failure, and a successful poll
re-arms it.

Binance acks nothing at all — the subscription is the URL it is dialled with —
so on those three venues the `_collector` records are the only account of the
session there is.

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

For Hyperliquid, pick which book cadence to convert — the two cannot be mixed:

```python
from hftbacktest.data.utils import hyperliquid

data = hyperliquid.convert(
    'btc_20260725.gz', tick_size=1.0, lot_size=0.00001,
    num_levels=20, book_mode='slow',   # or num_levels=5, book_mode='fast'
)
```

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
feed existed behave. **Genuine fusion of the two cadences is not implemented
yet** — `bybit.convert_fused` plus `FuseMarketDepth` is the pattern it would
follow.

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

### Falling behind

The two internal hand-offs are bounded (`src/queue.rs`), and since they fail for
different reasons they are bounded differently:

| Hop | Capacity | Consumer | What a full queue means |
|---|---|---|---|
| socket reader → parser | 4096 | JSON parse, no syscalls | the process is out of CPU |
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
50 MB**, and under 55 MB with both hops full, against 2 GB of host. Tokio's
bounded channels do not preallocate, so this is a ceiling and not a resting
cost. Memory is not what limits the number; the stall budget is.

Two caveats on that number, neither of them a risk at this scale. Setting
`COLLECTOR_STALL_TIMEOUT_MIN=0` disarms the watchdog, and with it the ~25 MB
half of the cap. And the frame sizes are Binance UM's, while the capacity is
shared by every backend: a Bybit instance carrying 1–2 KB `orderbook.200`
deltas is nearer **65 MB**, and its snapshots come over the WebSocket, so the
100/min throttle — which gates only the Binance REST refetch — does not bound
them. Still an order of magnitude inside the host.

The socket hop stayed at 4096 on purpose. Its consumer parses JSON and hands
over without ever making a syscall, so it can only stall on losing the CPU —
and the burst proves it was keeping up, since the writer hop could not have
filled unless the parser was running flat out to fill it. A backlog there means
something different, and burying it under a deeper buffer would only delay the
report.

One case escapes all of it. If a write blocks for ever in the kernel — a hung
mount, a device that stops answering — the main loop never gets back to notice
the signal, so the process stops recording without exiting. Memory still stays
bounded and the reason is in `journalctl`, but only an external watchdog
(systemd `WatchdogSec`) turns that into a restart.

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
| `binance`, `binancespot`, `bybit` | nothing extra; these record order flow only, and a genuinely dead market still trips it |

The USD-M row is the useful one, but for the opposite reason to the others: the
two paths are reported separately on purpose. A trip with no `poller_degraded`
beside it is a WebSocket fault with REST still answering; both together are the
host or the network.

**What it still does not catch**, and must not be mistaken for:

- a dead depth stream while trades keep arriving, or the reverse;
- one symbol of ten that stopped;
- one Hyperliquid cadence stopping while the other two continue;
- a partially accepted subscription — indistinguishable from a full one here.

Answering *did we get everything we asked for* is an offline report over
finished files, not a decision this process can make about itself.

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
- **Symbol validation is Hyperliquid-only.** There it is on by default and
  refuses to start on an unknown coin, because one bad name closes the whole
  WebSocket and takes every valid subscription with it. Bybit and Binance have
  no equivalent check yet; a typo there still produces a partial recording.
- **`binancefuturesum` and `binancefuturescm` are the same module twice**,
  differing only in their endpoints, their test fixtures and one `handle`
  branch. `binance` (spot) is a third near-copy. A fix to one needs applying to
  the others; only the socket loop is shared (`src/pump.rs`).
- **Throttler is off by one.** `Throttler` compares `len() > rate_limit`, so it
  permits `rate_limit + 1` calls per 60s window.
- **Spot vs futures gap detection differ.** Binance USD-M/COIN-M use
  `pu != prev_u`; spot uses `U != prev_u + 1`. Easy to copy wrong when adding
  a venue.
