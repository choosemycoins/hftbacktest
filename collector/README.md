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
| `binancefutures`, `binancefuturesum` | `@trade`, `@bookTicker`, `@depth@0ms` |
| `binancefuturescm` | `@trade`, `@bookTicker`, `@depth@0ms` |
| `binance`, `binancespot` | `@trade`, `@bookTicker`, `@depth@100ms` |
| `bybit` | `orderbook.{--bybit-depths}`, `publicTrade` |
| `hyperliquid` | `trades`, `bbo`, `l2Book` × `{--hl-l2-modes}` |

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

`convert` builds a `DiffOrderBookSnapshot` of a fixed depth and treats every
`l2Book` message as a complete snapshot of that depth, so feeding it the
interleaved stream would delete levels 6–20 on every fast message and restore
them on the next slow one. `book_mode` selects one cadence and drops the other;
it defaults to `'slow'`, which is exactly how recordings made before the fast
feed existed behave. **Genuine fusion of the two cadences is not implemented
yet** — `bybit.convert_fused` plus `FuseMarketDepth` is the pattern it would
follow.

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

Check the sidecar instead. It is plain text flushed per record, so it is always
current:

```bash
# what this instance is doing right now
tail -5 /opt/hft-collector/data/hyperliquid/_meta_hyperliquid_$(date -u +%Y%m%d).jsonl

# disconnects today
grep -c '"_collector":"disconnected"' .../_meta_hyperliquid_$(date -u +%Y%m%d).jsonl
```

For the data itself, growth over a window longer than the flush interval is the
honest signal:

```bash
find /opt/hft-collector/data -name "*_$(date -u +%Y%m%d).gz" -mmin -10
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

The two internal hand-offs — socket reader → parser, parser → writer — are
bounded at 4096 messages each (`src/queue.rs`). If one fills, the collector
stops: `{"_collector":"queue_overflow", …}` goes to the sidecar, whatever is
still queued is written, the files are closed, and the exit is non-zero. It
never waits for room, and it never drops a message to make some.

A hand-off can also lose its consumer outright, which is what an already-ended
collection task looks like from the producer side. That stops the collector the
same way but is recorded as `{"_collector":"hand_off_closed", …}`: nothing was
full, and reading it as an overflow would send a gap investigation after a queue
depth that was never reached.

That is deliberate. An unbounded queue turns a stalled writer into unbounded
memory growth while every outward sign — connected, receiving, no errors — still
looks healthy, and what survives the OOM killer is a set of unterminated gzip
members. A queue deep enough to absorb a rotation but not a stall turns the same
fault into a failed unit instead. The capacity is a starting point rather than a
measurement: at the rates in [Capacity](#capacity) a full queue is on the order
of a minute of traffic, and it should be recomputed once a peak has actually
been observed.

One case escapes it. If a write blocks for ever in the kernel — a hung mount, a
device that stops answering — the main loop never gets back to notice the
signal, so the process stops recording without exiting. Memory still stays
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

**What it does not catch**, and must not be mistaken for:

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
