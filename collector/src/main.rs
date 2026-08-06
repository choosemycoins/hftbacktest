use std::time::Duration;

use anyhow::anyhow;
use chrono::Utc;
use clap::Parser;
use tokio::{self, select};
use tracing::{error, info, warn};

use crate::{
    file::Writer,
    queue::{POLLER_HOP, POLLER_QUEUE_CAPACITY, WRITER_HOP, WRITER_QUEUE_CAPACITY},
    watchdog::Source,
};

mod aster;
mod backoff;
mod binance;
mod binancefuturescm;
mod binancefuturesum;
mod bybit;
mod clock;
mod cpu;
mod disk;
mod error;
mod extended;
mod file;
mod hyperliquid;
mod lighter;
mod liveness;
mod lock;
mod meta;
mod paradex;
mod pump;
mod queue;
mod throttler;
mod watchdog;

/// Build provenance, baked in by `build.rs`. Shown by `--version` and logged
/// at startup so a running instance and a recorded dataset can both be traced
/// back to an exact commit.
const VERSION: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    " (",
    env!("COLLECTOR_GIT_SHORT"),
    " ",
    env!("COLLECTOR_GIT_BRANCH"),
    ", ",
    env!("COLLECTOR_GIT_DIRTY"),
    ")"
);

#[derive(Parser, Debug)]
#[command(version = VERSION, about, long_about = None)]
struct Args {
    /// Path for the files where collected data will be written.
    path: String,

    /// Name of the exchange
    exchange: String,

    /// Symbols for which data will be collected.
    symbols: Vec<String>,

    /// Bybit only: orderbook depth levels to subscribe to, comma-separated.
    ///
    /// Bybit fails the ENTIRE subscribe batch if any single topic is unknown,
    /// and `orderbook.500` is rejected for most symbols — mainnet answers
    /// `error:handler not found,topic:orderbook.500.BTCUSDT` even for BTCUSDT.
    /// The default is the conservative set every linear symbol accepts. Widen
    /// it only after checking that the venue accepts the extra depth for the
    /// symbols you are recording.
    #[arg(long, value_delimiter = ',', default_value = "1,50")]
    bybit_depths: Vec<u32>,

    /// Hyperliquid only: which `l2Book` cadences to record, comma-separated.
    ///
    /// `slow` = 20 levels/side at roughly 5s. `fast` = 5 levels/side at roughly
    /// 0.5s. `none` records no book at all (trades and bbo only). The default
    /// records both, and the two are kept side by side rather than merged: the
    /// converter picks exactly one with its `book_mode` argument, and fusing
    /// them is explicitly not implemented (`hyperliquid.convert` raises on any
    /// other value). Recording both is what lets the choice be made — and
    /// revisited, or fused by some later converter — after the fact.
    #[arg(long, value_delimiter = ',', default_value = "slow,fast")]
    hl_l2_modes: Vec<String>,

    /// Stop recording when free space on the output filesystem drops below
    /// this many gigabytes. `0` disables the check.
    ///
    /// Checked at startup and every minute after. Crossing the floor is a
    /// clean, non-zero exit: the files are closed properly and systemd marks
    /// the unit failed, which is a far better outcome than writes beginning to
    /// fail at zero bytes free with a half-written gzip member.
    #[arg(long, default_value_t = 5)]
    min_free_gb: u64,

    /// Stop when no market data at all has been written for this many minutes.
    /// `0` disables the check.
    ///
    /// A last-resort guard against silent nothing: a venue that accepts a
    /// subscription and never sends, a reconnect loop that never resubscribes,
    /// a frame that is parsed into no stream. None of those raise an error, and
    /// the process looks healthy while it records an empty day.
    ///
    /// The default of 5 minutes is a PROPOSAL, not a measurement (open decision
    /// 2 of docs/design-multi-venue-collection.md). For scale, the slowest
    /// legitimate feed is Hyperliquid's plain `l2Book` at ~5.4s, so the margin
    /// is roughly fifty-fold — raise it if you record something slower, and
    /// lower it once a real quiet-period gap has been measured.
    ///
    /// **Do not lower it below 3.** This is the slower of two guards on
    /// purpose: a starved parser fills the socket hop in ~82s at the measured
    /// background rate (~164s if the market is half as busy), and that overflow
    /// is the only report that NAMES the fault — this one can say no more than
    /// "silence". Below 3 minutes the watchdog wins the race and the diagnosis
    /// is lost. The margin used to run down to 1; raising the socket hop to
    /// 16 384 on 2026-07-29 raised the floor with it. See
    /// `queue::WS_QUEUE_CAPACITY` and
    /// `the_socket_hop_reports_a_starved_parser_before_the_stall_watchdog_does`,
    /// which pins the ordering against the default here.
    ///
    /// It only ever catches TOTAL silence: not a dead depth stream while trades
    /// still arrive, not one Hyperliquid cadence out of three. Sidecar records
    /// do not count as data. One symbol of ten that stopped is
    /// `--liveness-timeout-s`, which warns rather than stopping.
    #[arg(long, default_value_t = 5)]
    stall_timeout_min: u64,

    /// Warn when one symbol has recorded nothing for this many seconds. `0`
    /// disables the warning; leave it unset to derive it from the venue.
    ///
    /// The per-symbol half of `--stall-timeout-min`, which only ever fires on
    /// total silence and so cannot see one coin of ten going quiet. It never
    /// stops the collector: partial silence is ambiguous where total silence is
    /// not — a thin symbol in a quiet hour really can go a minute without a
    /// print — so it warns, records, and leaves the decision to the operator.
    ///
    /// The default is derived from the slowest feed the venue serves per
    /// symbol: 60s where a periodic ~1/s per-symbol feed exists (Hyperliquid's
    /// `activeAssetCtx`, COIN-M's `@markPrice@1s`), 300s where the venue
    /// records order flow only. See `liveness::default_threshold_s`.
    #[arg(long)]
    liveness_timeout_s: Option<u64>,

    /// Skip the startup check that every requested symbol exists on the venue.
    ///
    /// Only Hyperliquid implements the check today. Leave it on: an unknown
    /// coin there closes the whole WebSocket, taking every valid subscription
    /// with it, and the collector then reconnects forever writing partial data
    /// while looking healthy.
    #[arg(long)]
    no_symbol_check: bool,
}

/// Writes whatever the writer hop still holds, and reports how much that was.
///
/// Called once the main loop has decided to stop. Every record still queued has
/// already been reported to its producer as accepted — that is the promise
/// `queue.rs` makes when it treats a refused hand-off as fatal rather than
/// dropping it — so destroying the channel with them in it would break that
/// promise at the last possible moment. On an overflow the backlog is by
/// definition the full capacity, and it is the newest data in the recording:
/// the window around whatever went wrong.
///
/// Bounded rather than run to exhaustion. Producers do not stop because the
/// consumer decided to: the socket keeps delivering, and an unbounded drain
/// could be fed for as long as the venue stays up. The cap is
/// [`WRITER_QUEUE_CAPACITY`] — **this hop's** capacity, not the socket hop's,
/// which is half the size — so everything that was waiting is written and
/// nothing that arrives afterwards can extend the shutdown. Capping it with the
/// wrong constant would silently discard half of a full backlog, which
/// `the_drain_is_bounded_even_if_the_producers_keep_pushing` exists to catch.
///
/// The full backlog is now ~13 MB of gzip work (32 768 records at a few hundred
/// bytes) against the unit's `TimeoutStopSec=30s`, which is a sub-second job at
/// the compression level in use — the deeper queue does not put the clean
/// shutdown at risk.
fn drain_backlog(
    rx: &mut tokio::sync::mpsc::Receiver<queue::Record>,
    mut write: impl FnMut(queue::Record) -> Result<(), anyhow::Error>,
) -> usize {
    let mut written = 0;
    for _ in 0..WRITER_QUEUE_CAPACITY {
        let Ok(record) = rx.try_recv() else { break };
        if let Err(error) = write(record) {
            // The same fault the loop was already leaving on, most likely. The
            // remaining records cannot be written either, and trying would only
            // delay the gzip flush that may still succeed.
            error!(
                ?error,
                written, "couldn't write the queued backlog on the way out"
            );
            break;
        }
        written += 1;
    }
    written
}

/// Writes one of `main`'s own records to the sidecar.
///
/// Straight to the `Writer` rather than through `writer_tx`, and that is not an
/// optimisation: a sender held here would stop `writer_rx.recv()` ever
/// returning `None`, which is how a dead collection task is noticed. The cost
/// is that `_meta` is not ordered by `local_ts` while the queue has a backlog —
/// see `meta.rs`, which documents the same trade for the same reason.
///
/// The result is discarded because every caller is either on its way out
/// already or on a periodic tick whose failure the next write will raise
/// anyway; a gauge must not be able to end a recording.
///
/// It also means none of these records can reach
/// [`watchdog::StallWatchdog::record_write`]
/// or [`liveness::LivenessGauge::record_write`], which is what keeps a gauge
/// from vouching for a feed that has stopped — the trap `watchdog::Source`
/// documents for the `premiumIndex` poller. Belt and braces: they are filed
/// under [`file::META_STREAM`], which both of those exclude by name anyway, so
/// routing this through `writer_tx` some day would break the ownership rule
/// above long before it disarmed either guard.
fn write_meta(writer: &mut Writer, record: serde_json::Value) {
    let _ = writer.write(
        Utc::now(),
        file::META_STREAM.to_string(),
        record.to_string(),
    );
}

/// Reports a change in the host clock's discipline, and nothing when there was
/// none.
///
/// One function because the gauge is consulted twice — once at startup and once
/// a minute after — and the two must say the same thing. The alarm is
/// edge-triggered inside [`clock::ClockGauge`], so whichever of the two sees the
/// fault first is the one that reports it.
fn log_clock_alarm(alarm: Option<clock::Alarm>) {
    match alarm {
        // Not fatal. The data is still worth having, and which window a skewed
        // clock invalidates is a decision for the offline gate — not for a
        // process that cannot see the day it is halfway through.
        Some(clock::Alarm::Raised(reason)) => warn!(
            %reason,
            "the host clock is not disciplined; local timestamps in this recording may \
             be rejected downstream"
        ),
        Some(clock::Alarm::Cleared) => info!("the host clock is disciplined again"),
        None => {}
    }
}

/// Reports a change in how much CPU the host is actually being given.
///
/// One function for the same reason [`log_clock_alarm`] is one: the gauge is
/// consulted at startup and once a minute after, and the two must say the same
/// thing.
fn log_cpu_alarm(alarm: Option<cpu::Alarm>) {
    match alarm {
        // Not fatal, and deliberately so. A throttled host is the one failure
        // the collector can do nothing about, and exiting over it would take
        // the recording down for the duration of a hypervisor's mood. The
        // warning exists so the `queue_overflow` that may follow is read as
        // "this box ran out of CPU" rather than as "the venue flooded us".
        Some(cpu::Alarm::Raised(reason)) => warn!(
            %reason,
            "this host is not getting the CPU it asked for; a writer that falls behind now \
             is the instance being throttled, not the venue"
        ),
        Some(cpu::Alarm::Cleared) => info!("the host is getting its CPU again"),
        None => {}
    }
}

/// Reports free space, and refuses to continue below the floor.
fn check_disk(path: &str, min_free_gb: u64) -> Result<u64, anyhow::Error> {
    let free = disk::available_bytes(path)?;
    let floor = min_free_gb.saturating_mul(1024 * 1024 * 1024);
    if min_free_gb > 0 && free < floor {
        return Err(anyhow!(
            "only {:.1} GB free on {path}, below the --min-free-gb floor of {min_free_gb} GB",
            free as f64 / 1e9
        ));
    }
    Ok(free)
}

/// Listens for the signals that mean "stop recording and close the files".
///
/// systemd's default `KillSignal` is SIGTERM, so listening only for ctrl-c
/// (SIGINT) meant every service stop, restart, deploy and rollback killed the
/// collector outright. The `Writer` was then never dropped, `GzEncoder::finish`
/// never ran, and the day's `.gz` was left with an unterminated deflate stream.
///
/// The listeners are constructed **once** and then borrowed each time round the
/// select loop. Constructing them inside the loop instead would register and
/// tear down the handler on every message — at tick rates that is thousands of
/// times a second, and a signal arriving while no listener was registered would
/// be dropped, leaving the process ignoring `systemctl stop` until the
/// `TimeoutStopSec` SIGKILL truncated the files.
struct Shutdown {
    #[cfg(unix)]
    sigint: tokio::signal::unix::Signal,
    #[cfg(unix)]
    sigterm: tokio::signal::unix::Signal,
}

impl Shutdown {
    #[cfg(unix)]
    fn new() -> Result<Self, std::io::Error> {
        use tokio::signal::unix::{SignalKind, signal as unix_signal};
        Ok(Self {
            sigint: unix_signal(SignalKind::interrupt())?,
            sigterm: unix_signal(SignalKind::terminate())?,
        })
    }

    #[cfg(not(unix))]
    fn new() -> Result<Self, std::io::Error> {
        Ok(Self {})
    }

    #[cfg(unix)]
    async fn recv(&mut self) -> &'static str {
        select! {
            _ = self.sigint.recv() => "SIGINT",
            _ = self.sigterm.recv() => "SIGTERM",
        }
    }

    #[cfg(not(unix))]
    async fn recv(&mut self) -> &'static str {
        // `ctrl_c()` yields `io::Result`; treating an Err as "shutdown" would
        // turn a registration failure into an immediate silent exit, so hold
        // the task instead and let the operator kill the process.
        match tokio::signal::ctrl_c().await {
            Ok(()) => "ctrl-c",
            Err(error) => {
                error!(?error, "couldn't listen for ctrl-c");
                std::future::pending().await
            }
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();

    // `fmt::init()` alone would resolve to `EnvFilter::from_default_env()`,
    // whose fallback when RUST_LOG is unset is ERROR — which would hide the
    // startup provenance line and the date-rotation events for anyone running
    // the binary directly. Keep INFO as the default and let RUST_LOG override.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    info!(
        version = env!("CARGO_PKG_VERSION"),
        commit = env!("COLLECTOR_GIT_COMMIT"),
        branch = env!("COLLECTOR_GIT_BRANCH"),
        dirty = env!("COLLECTOR_GIT_DIRTY"),
        exchange = %args.exchange,
        path = %args.path,
        symbols = ?args.symbols,
        "collector_starting"
    );

    // Fail before opening a single file rather than partway through the day.
    let free_at_start = check_disk(&args.path, args.min_free_gb)?;
    info!(
        free_gb = format!("{:.1}", free_at_start as f64 / 1e9),
        min_free_gb = args.min_free_gb,
        "disk space at startup"
    );

    // Claim the directory before anything opens a file in it. A second
    // collector here would interleave its gzip members with this one's and
    // leave the day undecodable for both — see `lock.rs`. Declared before the
    // `Writer` so it is dropped after it: the directory is released only once
    // every gzip member has been finished.
    let dir_lock = lock::acquire(&args.path, &args.exchange)?;
    info!(lock = %dir_lock.path().display(), "output directory locked");

    // Constructed here rather than beside the main loop, which is the only
    // place it is used from afterwards. `Writer::new` opens no file — it does
    // that on the first write — so this costs nothing, and it is what lets the
    // symbol resolution below record a refusal to start in the sidecar. Every
    // other startup failure happens before there is anything to explain; that
    // one happens after a previous session may have left a day's file behind,
    // and "we refused to start" is the clearest explanation a gap can have.
    let mut writer = Writer::new(&args.path, &args.exchange);

    // Lighter subscribes by integer market id, and the map from symbol to id
    // is a runtime fact of the venue rather than a constant. It is resolved
    // here, before anything is recorded, for two reasons that neither of the
    // other venues has: nothing can be subscribed without it, and it has to be
    // stamped into `session_start` below — the recorded payloads are keyed by
    // the integer, so a recording that does not carry the key cannot be read
    // back after the venue relists a market.
    let lighter_markets = if args.exchange == "lighter" {
        match lighter::resolve_markets(
            &args.symbols,
            lighter::REST_URL.as_str(),
            !args.no_symbol_check,
        )
        .await
        {
            Ok(markets) => Some(markets),
            Err(error) => {
                write_meta(
                    &mut writer,
                    serde_json::json!({
                        "_collector": "symbol_check_failed",
                        "error": error.to_string(),
                        "symbols": args.symbols,
                    }),
                );
                error!(%error, "refusing to start");
                return Err(error);
            }
        }
    } else {
        None
    };

    // Bounded, with `full => fatal` as the policy — see `queue.rs`. The fatal
    // channel is how a producer that has no error path of its own (the
    // detached REST snapshot tasks) reaches this loop.
    let (fatal_tx, mut fatal_rx) = queue::fatal_channel();
    let (writer_tx, mut writer_rx) =
        queue::bounded(WRITER_HOP, WRITER_QUEUE_CAPACITY, fatal_tx.clone());

    // The collector's own periodic output travels its own hop. Not for capacity
    // — see `POLLER_QUEUE_CAPACITY` — but because once two records are in one
    // queue nothing tells them apart, and `main` has to: a `premiumIndex`
    // element is filed under `BTCUSDT` exactly as a `bookTicker` frame is, and
    // only one of the two means the venue is still sending. The hop is the
    // distinction, and it is consumed by an arm that does not pet the stall
    // watchdog.
    let (poller_tx, mut poller_rx) = queue::bounded(POLLER_HOP, POLLER_QUEUE_CAPACITY, fatal_tx);
    // `Option` so the backends that have no poller release the sender in one
    // place instead of every arm remembering to. A retained clone here would
    // hold the hop open for ever, exactly as one of `writer_tx` would.
    let mut poller_tx = Some(poller_tx);

    // Open the recording with a record of what produced it. The scoped clone
    // is dropped immediately: keeping a sender alive here would stop
    // `writer_rx.recv()` ever returning `None`, and that is what tells the
    // main loop the collection task has died.
    {
        let meta_tx = writer_tx.clone();
        let mut session_start = serde_json::json!({
            "_collector": "session_start",
            "version": env!("CARGO_PKG_VERSION"),
            "commit": env!("COLLECTOR_GIT_COMMIT"),
            "branch": env!("COLLECTOR_GIT_BRANCH"),
            "dirty": env!("COLLECTOR_GIT_DIRTY"),
            "exchange": args.exchange,
            "symbols": args.symbols,
            "bybit_depths": args.bybit_depths,
            "hl_l2_modes": args.hl_l2_modes,
        });
        // Added only where it means something, rather than as a null on four
        // other venues. Lighter's frames name a market by integer and never by
        // symbol, so this map is what makes the day's files readable: the ids
        // are venue configuration and can be reassigned between recordings.
        // The fuller catalog — tick sizes, minimums, market type — travels in
        // the `universe` record the backend writes next.
        if let Some(markets) = &lighter_markets {
            session_start["lighter_markets"] = markets
                .iter()
                .map(|m| (m.symbol.clone(), serde_json::Value::from(m.market_id)))
                .collect::<serde_json::Map<String, serde_json::Value>>()
                .into();
        }
        // Nothing has been enqueued yet, so this cannot fail; propagating
        // rather than discarding keeps the rule that a hand-off result is
        // never ignored.
        meta_tx.send((
            chrono::Utc::now(),
            file::META_STREAM.to_string(),
            session_start.to_string(),
        ))?;
    }

    // Cloned before the backends take ownership. The per-symbol liveness gauge
    // has to be seeded with what was ASKED for rather than with what arrives:
    // a symbol whose subscription the venue silently dropped never reaches the
    // writer at all, and that is the version of the fault worth catching.
    let requested_symbols = args.symbols.clone();

    let collection_task = match args.exchange.as_str() {
        // Two spellings, one backend — but `session_start` above stamps
        // `args.exchange` verbatim, so the recording remembers which word the
        // operator typed. The offline tools canonicalise it in exactly one
        // place, `canonical_exchange` in `collector/tools/quality_report.py`;
        // adding an alias here means adding it there too, or the same bytes
        // become buildable or unbuildable depending on the spelling.
        //
        // The stream sets themselves live next to the `handle` that parses
        // their frames (`binancefutures{um,cm}::STREAMS`) rather than here: a
        // misspelled stream name is silent at runtime — the venue acks it and
        // never serves it — so only a test can catch one.
        "binancefutures" | "binancefuturesum" => {
            let streams = binancefuturesum::STREAMS
                .iter()
                .map(|stream| stream.to_string())
                .collect();

            tokio::spawn(binancefuturesum::run_collection(
                streams,
                args.symbols,
                writer_tx,
                // The one backend with a producer of its own. Nothing else
                // takes this, so for every other exchange the hop is closed
                // before the loop starts and its arm never fires.
                poller_tx.take().expect("the poller hop is claimed once"),
            ))
        }
        "aster" => {
            let streams = aster::STREAMS
                .iter()
                .map(|stream| stream.to_string())
                .collect();

            tokio::spawn(aster::run_collection(
                streams,
                args.symbols,
                writer_tx,
                poller_tx.take().expect("the poller hop is claimed once"),
            ))
        }
        "binancefuturescm" => {
            let streams = binancefuturescm::STREAMS
                .iter()
                .map(|stream| stream.to_string())
                .collect();

            tokio::spawn(binancefuturescm::run_collection(
                streams,
                args.symbols,
                writer_tx,
            ))
        }
        "binance" | "binancespot" => {
            let streams = ["$symbol@trade", "$symbol@bookTicker", "$symbol@depth@100ms"]
                .iter()
                .map(|stream| stream.to_string())
                .collect();

            tokio::spawn(binance::run_collection(streams, args.symbols, writer_tx))
        }
        "bybit" => {
            if args.bybit_depths.is_empty() {
                return Err(anyhow!("--bybit-depths must list at least one depth level"));
            }
            let mut topics: Vec<String> = args
                .bybit_depths
                .iter()
                .map(|d| format!("orderbook.{d}.$symbol"))
                .collect();
            topics.push("publicTrade.$symbol".to_string());
            info!(depths = ?args.bybit_depths, "bybit orderbook depths");

            tokio::spawn(bybit::run_collection(topics, args.symbols, writer_tx))
        }
        "hyperliquid" => {
            use hyperliquid::SubscriptionSpec;

            // `trades`, `bbo` and `activeAssetCtx` — see `hyperliquid::ALWAYS_ON`
            // for why the funding/oracle feed is among them and unconditional.
            let mut subscriptions: Vec<SubscriptionSpec> = hyperliquid::ALWAYS_ON
                .iter()
                .map(|kind| SubscriptionSpec::plain(kind))
                .collect();
            // Record both book cadences; the converter selects one of them.
            // Measured on mainnet 2026-07-25: the plain feed is 20 levels every
            // ~5.3s, `fast` is 5 levels every ~0.5s. Recording only the plain
            // one — as this collector used to — yields a book that updates
            // three times a minute, which is not usable for backtesting an HFT
            // strategy no matter how good the converter is. `hyperliquid.convert`
            // reads whichever cadence `book_mode` names and drops the other;
            // fusing depth from the slow feed with the frequency of the fast one
            // is not implemented. Both are written so that choice stays open.
            for mode in &args.hl_l2_modes {
                match mode.as_str() {
                    "slow" => subscriptions.push(SubscriptionSpec::l2_book(false)),
                    "fast" => subscriptions.push(SubscriptionSpec::l2_book(true)),
                    "none" => {}
                    other => {
                        return Err(anyhow!(
                            "--hl-l2-modes: expected slow|fast|none, got '{other}'"
                        ));
                    }
                }
            }
            info!(modes = ?args.hl_l2_modes, "hyperliquid l2Book modes");

            tokio::spawn(hyperliquid::run_collection(
                subscriptions,
                args.symbols,
                writer_tx,
                !args.no_symbol_check,
            ))
        }
        "lighter" => {
            // Resolved before `session_start`, which is the only place it can
            // be: the venue has no symbol addressing at all, so there is
            // nothing to hand this backend until the catalog has been read.
            let markets = lighter_markets.expect("lighter markets are resolved for this exchange");
            info!(
                markets = markets.len(),
                channels = ?lighter::CHANNELS,
                subscriptions = markets.len() * lighter::CHANNELS.len(),
                "lighter markets"
            );

            tokio::spawn(lighter::run_collection(markets, writer_tx))
        }
        "paradex" => {
            // Markets are addressed by symbol (BTC-USD-PERP), so no catalog
            // pre-resolution is needed the way Lighter's integer market ids are;
            // `run_collection` confirms each exists and records the universe.
            info!(
                markets = args.symbols.len(),
                channels = ?paradex::CHANNELS,
                subscriptions = args.symbols.len() * paradex::CHANNELS.len(),
                "paradex markets"
            );

            tokio::spawn(paradex::run_collection(
                args.symbols,
                writer_tx,
                !args.no_symbol_check,
            ))
        }
        "extended" => {
            // URL-based public feed: no subscribe frames, one socket per
            // channel. `--no-symbol-check` skips the catalog lookup that
            // resolves market type and the RFQ flag; leave it on so an unknown
            // symbol refuses to start and RFQ markets get their executable book.
            tokio::spawn(extended::run_collection(
                args.symbols,
                writer_tx,
                !args.no_symbol_check,
            ))
        }
        exchange => {
            return Err(anyhow!("{exchange} is not supported."));
        }
    };

    // `None` means the arm above took it, i.e. this backend has a poller.
    // Whatever is left is dropped rather than held: a sender kept here would
    // hold the hop open for ever, and the arm in the loop below would wait on a
    // channel nothing can ever send to.
    let has_poller = poller_tx.is_none();
    drop(poller_tx);

    let mut shutdown = Shutdown::new()?;

    // One timer for all four gauges. Disk is sampled rather than checked per
    // write because a statvfs on the hot path would be a syscall per message,
    // and a minute is far shorter than the time it takes to consume the 5 GB
    // default floor at the observed ~20 MB/day/symbol. The clock, CPU and
    // liveness gauges share it rather than bringing timers of their own: they
    // are the same kind of thing — a measurement of the host, not an event —
    // and one tick keeps a minute's four records together in the sidecar.
    let mut gauges = tokio::time::interval(Duration::from_secs(60));
    gauges.tick().await; // the first tick is immediate; startup already checked

    // The guard against silent nothing. Armed from here, so a venue that never
    // sends is caught even though not one record ever arrives — see
    // `watchdog.rs`, including what it deliberately does not catch.
    let mut watchdog = watchdog::StallWatchdog::new(args.stall_timeout_min);
    if args.stall_timeout_min == 0 {
        warn!("stall watchdog disabled; a silent feed will not stop the collector");
    } else {
        info!(
            minutes = args.stall_timeout_min,
            // Named in the same line as the guard it qualifies. On a backend
            // with a poller the watchdog is armed against the *feed* going
            // silent and not against the process going silent, and an operator
            // reading the journal after an outage needs to know which.
            poller = has_poller,
            "stall watchdog armed on total silence from the venue"
        );
    }

    // Is the host clock being disciplined? Sampled on the same tick and written
    // to the sidecar, so an undisciplined clock is visible in the minute it
    // happens rather than at assembly time — and so a dataset built from these
    // files can prove the clock over its own window instead of assuming it. See
    // `clock.rs` for why this reads `adjtimex(2)` and not chrony.
    let mut clock_gauge = clock::ClockGauge::new();
    {
        let at_startup = clock::sample();
        match &at_startup {
            clock::Sample::Kernel(d) => info!(
                sync = d.synchronized,
                max_error_us = d.max_error_us,
                offset_us = d.offset_us,
                freq_ppm = d.freq_ppm,
                "clock discipline at startup"
            ),
            // Not a warning. It is the expected state off Linux, and the record
            // says `unsupported` rather than claiming a healthy clock.
            clock::Sample::Unsupported => info!(
                platform = std::env::consts::OS,
                "no adjtimex here; clock discipline will be recorded as unsupported"
            ),
            clock::Sample::Unavailable(error) => {
                warn!(%error, "couldn't read clock discipline")
            }
        }
        // Recorded and judged now rather than at the first tick a minute from
        // now. The state this gauge exists for — a host that came back from a
        // reboot with nothing disciplining its clock — is present from the
        // first second, and a recording that starts and dies inside the minute
        // would otherwise carry no clock evidence at all.
        write_meta(&mut writer, clock::record(&at_startup));
        log_clock_alarm(clock_gauge.observe(&at_startup));
    }

    // Is the hypervisor giving this host the CPU it thinks it has? Sampled on
    // the same tick, because the failure it explains — the writer falling
    // behind — is indistinguishable from a venue flood without it, and the
    // recording boxes are burstable instances where running out of CPU credits
    // is an ordinary Tuesday. See `cpu.rs`.
    //
    // Primed here rather than left to the first tick. The counters are
    // cumulative since boot, so the first sample can only ever establish a
    // baseline; taking it now means the first *measured* minute is the first
    // tick a minute from now, instead of the second one two minutes in.
    let mut cpu_gauge = cpu::CpuGauge::new();
    {
        let (reading, _) = cpu_gauge.observe(&cpu::sample());
        match &reading {
            // Expected off Linux, and not a warning: the record says
            // `unsupported` rather than claiming a host with no steal.
            cpu::Reading::Unsupported => info!(
                platform = std::env::consts::OS,
                "no /proc/stat here; cpu utilisation will be recorded as unsupported"
            ),
            cpu::Reading::Unavailable(error) => warn!(%error, "couldn't read cpu utilisation"),
            // Spelled out rather than caught with `_`, so a variant added later
            // has to be considered here instead of silently reading as "armed".
            // `Interval` cannot occur on the first sample; it is grouped because
            // it would mean the same thing if it ever did.
            cpu::Reading::FirstSample | cpu::Reading::Interval(_) => info!(
                steal_warn_pct = cpu::STEAL_WARN_PCT,
                "cpu gauge armed; the first measured interval lands on the next tick"
            ),
        }
        write_meta(&mut writer, cpu::record(&reading));
    }

    // The per-symbol half of the stall watchdog. Seeded with the requested
    // symbols so one that never arrives is caught, not just one that stops.
    let liveness_timeout_s = args
        .liveness_timeout_s
        .unwrap_or_else(|| liveness::default_threshold_s(&args.exchange));
    let mut liveness_gauge = liveness::LivenessGauge::new(liveness_timeout_s, &requested_symbols);
    if liveness_timeout_s == 0 {
        warn!("per-symbol liveness warning disabled; a single silent symbol will not be reported");
    } else {
        info!(
            seconds = liveness_timeout_s,
            derived = args.liveness_timeout_s.is_none(),
            "per-symbol liveness armed"
        );
    }

    // Disarms the poller arm once its hop closes: for the backends that never
    // claimed it that is immediately, and a closed channel resolves `recv()`
    // straight away, which without the guard would spin this loop at full tilt.
    let mut poller_open = has_poller;

    // Distinguishes "asked to stop" from "stopped because recording broke".
    // Exiting 0 in both cases would make an unrecordable host look healthy:
    // systemd reports `Deactivated successfully`, the unit is never marked
    // failed, and nothing surfaces that the data has stopped arriving.
    let mut fatal: Option<anyhow::Error> = None;

    loop {
        select! {
            sig = shutdown.recv() => {
                info!(signal = sig, "shutdown signal received");
                break;
            }
            _ = gauges.tick() => {
                // The three host gauges first, then disk. Disk is the only one
                // of the four that can end the recording, and a minute whose
                // clock, CPU and liveness readings were dropped because the
                // disk ran out is a minute of missing evidence about the very
                // moment things went wrong.
                let (clock_record, clock_alarm) = clock_gauge.tick();
                write_meta(&mut writer, clock_record);
                // Edge-triggered inside the gauge, so this is once per fault
                // and not once a minute for as long as one lasts.
                log_clock_alarm(clock_alarm);

                let (cpu_record, cpu_alarm) = cpu_gauge.tick();
                write_meta(&mut writer, cpu_record);
                log_cpu_alarm(cpu_alarm);

                let (liveness_record, went) = liveness_gauge.sample();
                write_meta(&mut writer, liveness_record);
                for transition in went {
                    match transition {
                        liveness::Transition::WentQuiet { symbol, age_s } => warn!(
                            %symbol,
                            age_s,
                            threshold_s = liveness_timeout_s,
                            "nothing has been recorded for this symbol; the socket may be \
                             up with its subscription silently gone"
                        ),
                        liveness::Transition::Resumed { symbol } => {
                            info!(%symbol, "records are arriving for this symbol again")
                        }
                    }
                }

                match check_disk(&args.path, args.min_free_gb) {
                    Ok(free) => {
                        // Written to the sidecar, not just the log: it makes a
                        // recording carry its own capacity history, and it is
                        // the one file an operator can tail live.
                        write_meta(&mut writer, serde_json::json!({
                            "_collector": "disk",
                            "free_bytes": free,
                            "path": args.path,
                        }));
                    }
                    Err(error) => {
                        error!(%error, "stopping: not enough free disk space");
                        write_meta(&mut writer, serde_json::json!({
                            "_collector": "disk_exhausted",
                            "error": error.to_string(),
                        }));
                        fatal = Some(error);
                        break;
                    }
                }
            }
            silence = watchdog.stalled() => {
                // Nothing has reached disk for the whole timeout. The venue may
                // be up and the socket connected — that is precisely the state
                // no other guard reports, and an empty recording is worth no
                // more than a failed one, so it ends the same way.
                let error = anyhow!(
                    "nothing has been recorded for {:.0}s, the --stall-timeout-min limit \
                     is {} minute(s); the feed is silent",
                    silence.as_secs_f64(),
                    args.stall_timeout_min
                );
                error!(%error, "stopping: no data is reaching disk");
                write_meta(&mut writer, serde_json::json!({
                    "_collector": "stalled",
                    "silent_for_s": silence.as_secs(),
                    "stall_timeout_min": args.stall_timeout_min,
                }));
                fatal = Some(error);
                break;
            }
            report = fatal_rx.recv() => {
                // A queue filled up, or lost its receiver. Either way frames are
                // no longer reaching disk, and the producers have stopped rather
                // than drop them silently. Breaking here is what runs the
                // writer's `Drop` — finishing the gzip members — before the
                // non-zero exit; the producers cannot do that from where they
                // are, which is the whole reason the signal comes back here.
                //
                // The record is named after which of the two happened. They are
                // not interchangeable: a hop losing its receiver is what an
                // ending collection task looks like from the other side, and
                // filing that as an overflow would point Phase 2's gap
                // attribution at a queue depth that was never reached.
                error!(event = report.event, reason = report.reason, "stopping: a data hand-off failed");
                write_meta(&mut writer, serde_json::json!({
                    "_collector": report.event,
                    "error": &report.reason,
                }));
                fatal = Some(anyhow!(report.reason));
                break;
            }
            r = poller_rx.recv(), if poller_open => match r {
                Some((recv_time, symbol, data)) => {
                    // Offered to the watchdog exactly as the venue arm's record
                    // is, and refused there rather than withheld here: the rule
                    // about what counts as life belongs in one place, with a
                    // test on it, not in whether a call site remembered to make
                    // a call. `Source::Collector` says the collector produced
                    // this on a timer of its own, so it is evidence the timer is
                    // running and none at all about the venue — see
                    // `watchdog::Source` for what conflating the two measured.
                    watchdog.record_write(Source::Collector, &symbol);
                    // Offered on the same terms and refused for the same
                    // reason. A `premiumIndex` element is filed under `BTCUSDT`
                    // exactly as a `bookTicker` frame is, so a gauge that took
                    // it would report every symbol alive with the socket dead.
                    liveness_gauge.record_write(Source::Collector, &symbol);
                    if let Err(error) = writer.write(recv_time, symbol, data) {
                        error!(?error, "write error");
                        fatal = Some(error);
                        break;
                    }
                }
                // The poller has gone, which on this backend means the
                // collection task has ended; `writer_rx` reports that, and
                // reporting it twice would only race the better message.
                None => poller_open = false,
            },
            r = writer_rx.recv() => match r {
                Some((recv_time, symbol, data)) => {
                    // Counted here, as the record leaves the queue for the
                    // writer, and never where it was enqueued: messages piling
                    // up behind a stalled writer must not read as life. A write
                    // that then fails ends the loop anyway.
                    watchdog.record_write(Source::Venue, &symbol);
                    liveness_gauge.record_write(Source::Venue, &symbol);
                    if let Err(error) = writer.write(recv_time, symbol, data) {
                        error!(?error, "write error");
                        fatal = Some(error);
                        break;
                    }
                }
                None => {
                    // Every sender is gone, i.e. the collection task ended.
                    // Nothing further will ever be recorded, so this is a
                    // failure however tidily it happened. The task's own error
                    // is recovered below, since "the task ended" on its own
                    // tells an operator nothing about why.
                    fatal = Some(anyhow!(
                        "the collection task ended; no further data will be recorded"
                    ));
                    break;
                }
            }
        }
    }

    // Every break above leaves records behind in the queue, and on the fatal
    // paths that is the last data the collector ever captured. Write it before
    // the files are closed rather than letting the channel be destroyed with
    // it — see `drain_backlog` for why this is bounded.
    // Both hops: the promise `queue.rs` makes — a record reported as accepted
    // is never dropped — is made by `Tx::send`, so it covers whichever hop the
    // sender belonged to. The poller hop is drained second because it is the
    // smaller and the less urgent of the two.
    let recovered = drain_backlog(&mut writer_rx, |(recv_time, symbol, data)| {
        writer.write(recv_time, symbol, data)
    }) + drain_backlog(&mut poller_rx, |(recv_time, symbol, data)| {
        writer.write(recv_time, symbol, data)
    });
    if recovered > 0 {
        info!(
            records = recovered,
            "wrote the queued backlog before closing"
        );
    }

    // Drop the writer explicitly rather than letting it fall off the end of
    // `main`: `RotatingFile::drop` is what calls `GzEncoder::finish`. Doing it
    // here means the "stopped" log line is emitted after the flush has been
    // attempted. A failed flush is logged by `Drop` itself and cannot be
    // reported through this return value.
    drop(writer);

    // Prefer the collection task's own error. It knows the real cause — an
    // unknown symbol, an unrecoverable stream failure — where the main loop
    // only ever sees the channel close. Without this the process exits with a
    // message that describes the symptom and not the fault.
    if fatal.is_some()
        && collection_task.is_finished()
        && let Ok(Err(task_error)) = collection_task.await
    {
        fatal = Some(task_error);
    }

    match fatal {
        None => {
            info!("collector_stopped");
            Ok(())
        }
        Some(error) => {
            error!(%error, "collector_stopped_with_error");
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::Record;

    fn record(payload: &str) -> Record {
        (Utc::now(), "BTC".to_string(), payload.to_string())
    }

    /// Everything still in the queue when the loop stops was already reported
    /// to its producer as accepted — `queue.rs` promises records are "never
    /// dropped", and the producers stopped rather than lose them. Leaving the
    /// channel to be destroyed with `main` would break that promise at the
    /// process level, and on an overflow it would discard a full
    /// `WRITER_QUEUE_CAPACITY` of records: the newest data in the recording,
    /// which is the window around the fault.
    #[test]
    fn the_queued_backlog_is_written_before_the_files_are_closed() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Record>(8);
        for payload in ["first", "second", "third"] {
            tx.try_send(record(payload)).unwrap();
        }

        let mut written = Vec::new();
        let count = drain_backlog(&mut rx, |(_, _, data)| {
            written.push(data);
            Ok(())
        });

        assert_eq!(count, 3);
        assert_eq!(written, ["first", "second", "third"]);
    }

    /// The drain runs after the decision to stop, so it must not become a
    /// reason not to. Producers may still be pushing — the socket does not stop
    /// because the writer did — so it is capped rather than run to exhaustion.
    ///
    /// Since the two hops stopped sharing a capacity it guards the wiring as
    /// well: capped with the socket hop's constant instead, this would recover
    /// 16 384 of 32 768 and throw away the rest of the last data the collector
    /// captured. Checked by temporarily swapping the constant — it fails.
    #[test]
    fn the_drain_is_bounded_even_if_the_producers_keep_pushing() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Record>(WRITER_QUEUE_CAPACITY * 2);
        for _ in 0..(WRITER_QUEUE_CAPACITY + 100) {
            tx.try_send(record("still arriving")).unwrap();
        }

        assert_eq!(drain_backlog(&mut rx, |_| Ok(())), WRITER_QUEUE_CAPACITY);
    }

    /// The socket hop is the one whose overflow genuinely races the stall
    /// watchdog, and it has to win that race.
    ///
    /// A starved parser produces no dequeues, so `watchdog.record_write` is
    /// never reached and the watchdog becomes the only other thing that would
    /// ever report the fault. It can say no more than "nothing has been
    /// recorded for five minutes"; the overflow names the hop and files
    /// `queue_overflow` in the sidecar, which is what Phase 2's gap attribution
    /// reads. The specific diagnosis has to arrive first.
    ///
    /// Bounded from both sides, or the rule has no teeth: merely beating the
    /// five-minute watchdog is still satisfied at ~60 000 messages, four times
    /// the shipped depth. What the upper bound is stated as changed when the hop
    /// was raised to 16 384 on 2026-07-29 (see `queue::burst`), and the old
    /// version is worth recording because it was the tighter of the two: **a
    /// diagnosis inside a minute** at the measured background rate, which capped
    /// the hop near 12 000 and which 16 384 (~82s) does not meet.
    ///
    /// That absolute was never the invariant, though — it was a proxy for one,
    /// and a decision about how quickly an operator should hear about a fault
    /// that has already stopped the recording either way. The invariant is the
    /// **ordering**, and what it needs is not speed but margin, because the
    /// number it is computed from is an estimate: `BACKGROUND_MSG_PER_S` is what
    /// four Binance UM symbols ran at when they were measured, and a quiet hour,
    /// a thinner instance or a venue between listings all make it smaller —
    /// which makes the hop slower to fill and the race closer. So the
    /// requirement is that the overflow still lands first **at half the measured
    /// background rate**. At 16 384 that is 164s against 300s; a
    /// `WRITER_QUEUE_CAPACITY`-sized hop (32 768) would be 328s and lose. The
    /// cap that follows is ~30 000 messages, the floor from
    /// `the_socket_hop_outlasts_the_excursion_that_overflowed_it` in `queue.rs`
    /// is 10 000, and 16 384 is the only power of two in between.
    ///
    /// The writer hop is deliberately not held to the same rule. Every dequeue
    /// pets the watchdog, so a writer that is slow rather than stopped keeps it
    /// satisfied indefinitely while the queue grows; there the bound is the only
    /// guard there is, not the faster of two.
    ///
    /// The watchdog default is read from the shipped `Args` rather than copied,
    /// so lowering the flag's default re-checks this instead of quietly
    /// invalidating it.
    #[test]
    fn the_socket_hop_reports_a_starved_parser_before_the_stall_watchdog_does() {
        let shipped = Args::parse_from(["collector", "/tmp/recording", "binancefuturesum"]);
        assert!(
            shipped.stall_timeout_min > 0,
            "the shipped default disables the stall watchdog; this invariant is about which \
             of two armed guards reports first"
        );
        let watchdog_ms = (shipped.stall_timeout_min as usize) * 60_000;

        let fill_ms = queue::burst::fill_time_ms(
            queue::WS_QUEUE_CAPACITY,
            queue::burst::BACKGROUND_MSG_PER_S,
        );
        assert!(
            fill_ms < watchdog_ms,
            "a starved parser fills the websocket->parser hop in {fill_ms} ms at the measured \
             background rate, which is no sooner than the {watchdog_ms} ms stall watchdog; the \
             collector would then report silence instead of naming the hop"
        );

        // The margin, and where the teeth are. The background rate is an
        // estimate from one venue on one day; the ordering has to survive it
        // being wrong in the direction that hurts.
        let thin_market_ms = queue::burst::fill_time_ms(
            queue::WS_QUEUE_CAPACITY,
            queue::burst::BACKGROUND_MSG_PER_S / 2,
        );
        assert!(
            thin_market_ms < watchdog_ms,
            "the websocket->parser hop ({} messages) takes {thin_market_ms} ms to report a \
             starved parser at half the measured background rate, and the {watchdog_ms} ms \
             stall watchdog would get there first — the recording would then be explained by \
             \"silence\" rather than by the hop that broke. That caps this hop near {} \
             messages",
            queue::WS_QUEUE_CAPACITY,
            watchdog_ms * (queue::burst::BACKGROUND_MSG_PER_S / 2) / 1000,
        );
    }

    /// A write that fails on the way out is the same fault the loop was already
    /// leaving on. Retrying the rest would just fail once per queued record and
    /// delay the flush that still has a chance of succeeding.
    #[test]
    fn a_failing_write_stops_the_drain() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Record>(8);
        tx.try_send(record("first")).unwrap();
        tx.try_send(record("second")).unwrap();

        let mut seen = 0;
        let count = drain_backlog(&mut rx, |_| {
            seen += 1;
            Err(anyhow!("the device stopped answering"))
        });

        assert_eq!(count, 0);
        assert_eq!(
            seen, 1,
            "the drain must not keep trying after a write failed"
        );
    }
}
