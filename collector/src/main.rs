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

mod backoff;
mod binance;
mod binancefuturescm;
mod binancefuturesum;
mod bybit;
mod clock;
mod cpu;
mod disk;
mod error;
mod file;
mod hyperliquid;
mod lighter;
mod liveness;
mod lock;
mod meta;
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

/// How long the producers get to stop before the wind-down goes on without
/// them.
///
/// It is a guard against a wedged task, not a budget anything is expected to
/// use: the collection task's cancellation lands at its next `.await`, which on
/// every backend is a socket read. Generous against the unit's
/// `TimeoutStopSec=30s` — the drain that follows still has to gzip whatever is
/// queued — and short enough that a task stuck in a blocking call cannot be what
/// SIGKILLs the process and truncates the day's file.
const PRODUCER_STOP_GRACE: Duration = Duration::from_secs(5);

/// Stops the producers, and recovers the collection task's own error if it had
/// one.
///
/// The drain that follows can only be honest if nothing is still handing records
/// over, and the only handle `main` holds on five backends' worth of read loops
/// is this one. Cancellation lands at the task's next `.await` — a socket read
/// in every case — after which its senders are dropped with it.
///
/// What it does **not** stop is a detached child: the REST snapshot fetchers
/// (`binancefutures*`, `bybit`) hold a `writer_tx` clone of their own, and
/// `pump`'s producer task holds the socket hop. Those are why the drain closes
/// the hand-off rather than trusting this to have emptied the field — see
/// [`drain_to_completion`] — and why `wind_down` declares the collector stopping
/// before calling this: cancelling `pump` is itself what orphans its reader.
///
/// # Known limit: the socket hop is cut, not drained
///
/// The collection task **is** `pump`, which owns the socket hop's receiver, so
/// cancelling it destroys whatever that hop still held — frames the reader had
/// already been told were accepted and the parser had not reached yet. Measured
/// on this wiring: of 500 frames accepted into the socket hop, 244 went with the
/// cancellation. Silently — a dropped receiver refuses nothing, so no producer
/// was told and nothing counted them.
///
/// It is the promise `drain_to_completion` keeps for the writer hop, broken one
/// hop upstream, and it predates the wind-down: before it, `main` returned and
/// destroyed both hops' backlogs together. Normally the loss is nil — the parser
/// is a string copy and a route, so the hop is empty unless the tape is
/// out-running it — but that is exactly the state a stop can land in, and the
/// 2026-07-29 excursion filled this hop to 16 384 (`queue::burst`).
///
/// Closing it is a cascade, not a patch: the **reader** would have to be what
/// stops, so that `pump` sees its sender dropped, drains what is left into the
/// writer hop and returns on its own. That means a cancellation path through
/// five backends' `keep_connection` loops — its own change, with its own tests.
///
/// Returns the error the task returned, when it had already finished on its own.
/// A cancelled task has none, and neither has one that outlived the grace: both
/// leave the loop's own diagnosis in place, which is the honest answer when the
/// task never got to say anything.
async fn stop_collecting(
    task: tokio::task::JoinHandle<Result<(), anyhow::Error>>,
    grace: Duration,
) -> Option<anyhow::Error> {
    // Only a task that is still running is cancelled. Aborting one that has
    // already returned discards its result — the error the loop is about to
    // report — and that error is the whole reason this function returns one.
    if !task.is_finished() {
        task.abort();
    }
    match tokio::time::timeout(grace, task).await {
        Ok(Ok(Err(error))) => Some(error),
        Ok(_) => None,
        Err(_) => {
            // Nothing here can force it. The drain still runs and the files are
            // still finished; the record says the shutdown went ahead without
            // it, which is the only part that is actionable.
            warn!(
                grace_s = grace.as_secs(),
                "the collection task did not stop in time; winding down without it"
            );
            None
        }
    }
}

/// Writes everything the hand-off ever accepted, and reports how much that was.
///
/// Called once the main loop has decided to stop. Every record still queued has
/// already been reported to its producer as accepted — that is the promise
/// `queue.rs` makes when it treats a refused hand-off as fatal rather than
/// dropping it — so destroying the channel with them in it would break that
/// promise at the last possible moment. On an overflow the backlog is by
/// definition the full capacity, and it is the newest data in the recording:
/// the window around whatever went wrong.
///
/// **It closes the hand-off first, and that is what makes it correct.** The
/// previous version drained with `try_recv` until the queue was empty, which is
/// not the same question: producers do not stop because the consumer decided to,
/// so an empty queue means "nothing this instant", not "nothing further". A
/// producer that handed a record over microseconds later had it accepted into a
/// channel that was then destroyed — silently, with no error anywhere, on the
/// ordinary `systemctl stop` path. [`stop_collecting`] narrows that window and
/// cannot close it, because the detached REST snapshot tasks hold senders it has
/// no handle on.
///
/// `Receiver::close` closes it from this end: further hand-offs are **refused**
/// — `Tx::send` returns `Closed`, so the producer is told rather than left
/// believing the record was taken — while everything already accepted stays
/// readable. That turns the invariant into one with two outcomes and no third:
/// a record is written, or its hand-off is refused.
///
/// A refusal here is counted, not raised: `wind_down` has already set
/// [`queue::Stopping`], and the refusals it causes are the mechanism working.
/// Reported as faults they were an `error!` on every clean stop with a backlog
/// to drain — see that type for why the distinction is scoped rather than a way
/// of hiding one. It is also the close that bounds the drain.
/// `recv()` yields `None` once the channel is closed and every record sent
/// before it was closed has been handed over, so the loop terminates by
/// construction rather than by a cap — at most this hop's capacity of records,
/// ~13 MB of gzip work against the unit's `TimeoutStopSec=30s`, which is a
/// sub-second job at the compression level in use.
async fn drain_to_completion(
    rx: &mut tokio::sync::mpsc::Receiver<queue::Record>,
    mut write: impl FnMut(queue::Record) -> Result<(), anyhow::Error>,
) -> usize {
    rx.close();
    let mut written = 0;
    while let Some(record) = rx.recv().await {
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

/// The wind-down, in the one order that keeps the hand-off's promise.
///
/// 1. **Say that the collector is stopping**, so that a hand-off refused from
///    here on is read as the shutdown working rather than as a broken
///    recording. First, because step 2 is itself a cause of refusals: the abort
///    drops the socket hop's receiver while `pump`'s reader — a task of its own,
///    which nothing here has a handle on — is still holding the sender.
/// 2. **Stop the producers.** Nothing can be handed over by a task that is no
///    longer running. What that reaches is one task per backend; what it does
///    not reach is every child they spawned, which is why step 1 comes before it
///    and not after step 3.
/// 3. **Close both hands-off and drain them to completion.** Anything the
///    detached children still hand over is refused rather than accepted and
///    destroyed, and the drain ends because the channel is closed rather than
///    because it was briefly empty.
///
/// The writer hop is drained first because it is the larger and the more urgent
/// of the two; the poller hop carries the collector's own periodic output.
///
/// One function so the order is testable at all: everything around it in `main`
/// dials a socket. The latch is set here rather than at the `break` for the same
/// reason — a step of the wind-down belongs where the wind-down is tested, not
/// in the loop that cannot be.
///
/// Where exactly it is set *within* this function is not pinned, and the window
/// it covers is why: between the abort landing and the join returning there is
/// an instant in which a child on another worker thread can be refused. A test
/// cannot schedule inside it — the abort and the join resolve together — so the
/// placement is an argument, and `stopping.begin()` being the first statement is
/// what makes the argument hold.
async fn wind_down(
    stopping: &queue::Stopping,
    collection_task: tokio::task::JoinHandle<Result<(), anyhow::Error>>,
    grace: Duration,
    writer_rx: &mut tokio::sync::mpsc::Receiver<queue::Record>,
    poller_rx: &mut tokio::sync::mpsc::Receiver<queue::Record>,
    mut write: impl FnMut(queue::Record) -> Result<(), anyhow::Error>,
) -> (Option<anyhow::Error>, usize) {
    stopping.begin();
    let task_error = stop_collecting(collection_task, grace).await;
    let recovered = drain_to_completion(writer_rx, &mut write).await
        + drain_to_completion(poller_rx, &mut write).await;
    (task_error, recovered)
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
        match lighter::resolve_markets(&args.symbols, lighter::REST_URL, !args.no_symbol_check)
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
    // Handed to `wind_down`, which sets it. Until then every hand-off reports a
    // refusal exactly as it always has.
    let stopping = fatal_rx.stopping();
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
    // paths that is the last data the collector ever captured. The producers
    // are stopped and both hands-off are then drained to completion rather than
    // to the first instant they happen to be empty — see `wind_down`, and
    // `drain_to_completion` for why closing the channel is what makes the
    // promise `queue.rs` makes survive the shutdown.
    let (task_error, recovered) = wind_down(
        &stopping,
        collection_task,
        PRODUCER_STOP_GRACE,
        &mut writer_rx,
        &mut poller_rx,
        |(recv_time, symbol, data)| writer.write(recv_time, symbol, data),
    )
    .await;
    if recovered > 0 {
        info!(
            records = recovered,
            "wrote the queued backlog before closing"
        );
    }

    // What the close refused: records the venue sent after the decision to
    // stop, which is the definition of stopping rather than a broken promise —
    // none of them was ever reported to its producer as accepted. Stated as a
    // number because nothing else states it: the reports themselves are
    // suppressed for the duration, or every clean stop would log a fault.
    let refused = stopping.refusals();
    if refused > 0 {
        info!(
            records = refused,
            "hand-offs refused after the wind-down began; those records are not on disk"
        );
    }

    // Prefer the collection task's own error. It knows the real cause — an
    // unknown symbol, an unrecoverable stream failure — where the main loop
    // only ever sees the channel close. Without this the process exits with a
    // message that describes the symptom and not the fault. Only on a path that
    // was already failing: a task cancelled by the wind-down has no error of its
    // own, and a signal that raced a dying task is still a signal.
    if fatal.is_some()
        && let Some(error) = task_error
    {
        fatal = Some(error);
    }

    // Close the files here, explicitly, and let the failure through. A gzip
    // member whose trailer was never written is not a shorter recording but an
    // unreadable one, and the device that refuses those last bytes refuses them
    // at the one moment nothing else is watching. Left to `Drop` — which has
    // nowhere to return to — the process exited 0 over a corrupt day and
    // systemd reported `Deactivated successfully`.
    //
    // Second to whatever stopped the recording, which is the more useful
    // diagnosis of the two; both reach the journal either way.
    if let Err(error) = writer.finish() {
        error!(%error, "the recording may be truncated");
        fatal.get_or_insert(error);
    }

    // `Drop` stays as the backstop for the paths that never reach here — a
    // panic, an early return. `finish` emptied the writer, so this closes
    // nothing twice.
    drop(writer);

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
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    use tokio::sync::Notify;
    use tokio_tungstenite::tungstenite::Utf8Bytes;

    use super::*;
    use crate::{
        error::ConnectorError,
        queue::{Frame, Record, Tx},
    };

    fn record(payload: &str) -> Record {
        (Utc::now(), "BTC".to_string(), payload.to_string())
    }

    const FRAME: &str = r#"{"channel":"l2Book","data":{"coin":"BTC","time":1,"levels":[[],[]]}}"#;

    /// The producers the wind-down cannot reach are the ordinary case, and a
    /// clean stop must not report them as a broken recording.
    ///
    /// Wired the way `main` wires every backend: the collection task **is**
    /// [`pump::pump`], which owns the socket hop's receiver and spawns the
    /// socket reader as a task of its own, holding only a join handle. So
    /// `stop_collecting`'s cancellation lands on `pump` — dropping that
    /// receiver — while the reader is still there with the sender, and the
    /// reader's next hand-off meets a channel with no receiver left. The REST
    /// snapshot fetchers (`binancefutures*`, `bybit`) are the same shape one hop
    /// further down: a detached task holding a `writer_tx` clone.
    ///
    /// Reported as a fault, that is `error!: a data hand-off failed; the
    /// collector will stop` on the ordinary `systemctl stop` — measured on this
    /// wiring at a few hundred queued records, which is any stop that has a
    /// backlog to drain. The exit code stays 0 and no `_meta` record is written,
    /// so it is the journal that is wrong and not the data; an operator who sees
    /// it on every clean stop stops reading it.
    ///
    /// Both halves are the pin, because suppressing the report must not become
    /// accepting the record. The reader's hand-off is still **refused** — it
    /// gets an error back and knows the record did not land — and the refusal is
    /// counted, so the shutdown can say how many there were.
    #[tokio::test]
    async fn a_hand_off_the_wind_down_cannot_reach_is_refused_without_reporting_a_fault() {
        let (writer_tx, mut writer_rx, mut fatal) = queue::test_bounded::<Record>(WRITER_HOP, 128);
        let (_poller_tx, mut poller_rx, _poller_fatal) =
            queue::test_bounded::<Record>(POLLER_HOP, 8);
        // The socket hop inherits this one — `pump` builds it from
        // `writer_tx.fatal()` — so one latch covers both hops, exactly as the
        // single fatal channel in `main` does.
        let stopping = fatal.stopping();

        let released = Arc::new(Notify::new());
        let reader_done = Arc::new(Notify::new());
        let refused = Arc::new(AtomicBool::new(false));
        let forwarded = Arc::new(AtomicUsize::new(0));

        let collection = tokio::spawn(pump::pump(
            writer_tx,
            {
                let released = released.clone();
                let reader_done = reader_done.clone();
                let refused = refused.clone();
                // The socket reader. It hands one frame over, then waits —
                // standing in for a read that has not returned when the signal
                // arrives — and hands over the frame that was in flight after
                // the wind-down has run. Sequenced rather than raced, because
                // the outcome must not depend on which instant it wakes in.
                move |ws_tx: Tx<Frame>| async move {
                    ws_tx.send((Utc::now(), Utf8Bytes::from(FRAME))).unwrap();
                    released.notified().await;
                    refused.store(
                        ws_tx.send((Utc::now(), Utf8Bytes::from(FRAME))).is_err(),
                        Ordering::SeqCst,
                    );
                    reader_done.notify_one();
                }
            },
            {
                let forwarded = forwarded.clone();
                move |writer_tx: &Tx<Record>, recv_time, data: Utf8Bytes| {
                    writer_tx.send((recv_time, "BTC".to_string(), data.to_string()))?;
                    forwarded.fetch_add(1, Ordering::SeqCst);
                    Ok::<(), ConnectorError>(())
                }
            },
        ));

        // So this is a wind-down of a pipeline that is running, not of one that
        // never started.
        while forwarded.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }

        let mut written = 0;
        let (_task_error, recovered) = wind_down(
            &stopping,
            collection,
            PRODUCER_STOP_GRACE,
            &mut writer_rx,
            &mut poller_rx,
            |_| {
                written += 1;
                Ok(())
            },
        )
        .await;

        released.notify_one();
        reader_done.notified().await;

        assert_eq!(
            recovered, 1,
            "the frame the reader handed over before the stop is owed to the recording"
        );
        assert!(
            refused.load(Ordering::SeqCst),
            "the reader outlived the cancellation — that is the point — and its hand-off \
             was accepted into a channel with no receiver left, which is the silent loss \
             the close exists to prevent"
        );
        assert_eq!(
            stopping.refusals(),
            1,
            "the refusal must be counted; it is a record that never reached disk and \
             nothing else reports it"
        );
        // The direct observable, and the one that survives into production: on
        // the signal path `main` discards the task's result, so a refusal shows
        // up only as `Tx::send` raising the fatal signal and logging `a data
        // hand-off failed`. A clean stop must raise none.
        //
        // A deadline of zero is a single poll of an empty channel: it cannot
        // wait, so it cannot flake, and the keepalive in `FatalRx` means the
        // alternative — `recv()` on a channel with no sender left — would hang
        // rather than answer.
        assert!(
            tokio::time::timeout(Duration::ZERO, fatal.recv())
                .await
                .is_err(),
            "a clean shutdown reported a broken hand-off; every `systemctl stop` with a \
             backlog would log one and an operator would learn to ignore the one that matters"
        );
    }

    /// Everything still in the queue when the loop stops was already reported
    /// to its producer as accepted — `queue.rs` promises records are "never
    /// dropped", and the producers stopped rather than lose them. Leaving the
    /// channel to be destroyed with `main` would break that promise at the
    /// process level, and on an overflow it would discard a full
    /// `WRITER_QUEUE_CAPACITY` of records: the newest data in the recording,
    /// which is the window around the fault.
    #[tokio::test]
    async fn the_queued_backlog_is_written_before_the_files_are_closed() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Record>(8);
        for payload in ["first", "second", "third"] {
            tx.try_send(record(payload)).unwrap();
        }
        drop(tx);

        let mut written = Vec::new();
        let count = drain_to_completion(&mut rx, |(_, _, data)| {
            written.push(data);
            Ok(())
        })
        .await;

        assert_eq!(count, 3);
        assert_eq!(written, ["first", "second", "third"]);
    }

    /// The drain runs after the decision to stop, so it must not become a
    /// reason not to. Producers do not stop because the consumer did — the
    /// socket keeps delivering — so a drain that ran until the queue stayed
    /// empty could be fed for as long as the venue stays up.
    ///
    /// It ends because the hand-off is **closed**, not because a counter ran
    /// out. That is what makes the bound and the invariant the same mechanism:
    /// the cap this used to carry bounded the shutdown by throwing accepted
    /// records away, and closing the channel bounds it by refusing new ones.
    ///
    /// Deliberately over-full: the sender here holds twice this hop's capacity
    /// so that a version which kept a cap of `WRITER_QUEUE_CAPACITY` — or, the
    /// old wiring hazard, of the socket hop's half-size constant — would stop
    /// early and be caught.
    #[tokio::test]
    async fn the_drain_is_bounded_even_if_the_producers_keep_pushing() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Record>(WRITER_QUEUE_CAPACITY * 2);
        let queued = WRITER_QUEUE_CAPACITY + 100;
        for _ in 0..queued {
            tx.try_send(record("still arriving")).unwrap();
        }

        // Never dropped, and still pushing when the drain starts.
        let producer = tokio::spawn(async move {
            while tx.try_send(record("still arriving")).is_ok() {
                tokio::task::yield_now().await;
            }
            tx
        });

        let written = drain_to_completion(&mut rx, |_| Ok(())).await;
        let tx = producer.await.unwrap();

        assert!(
            written >= queued,
            "the drain stopped at {written} records with {queued} already accepted"
        );
        assert!(
            tx.try_send(record("too late")).is_err(),
            "the drain must leave the hand-off closed, so a straggler is refused rather \
             than accepted into a channel that is about to be destroyed"
        );
    }

    /// The promise `queue.rs` makes is that a record **reported as accepted** is
    /// never dropped, and the shutdown is the last place that promise can be
    /// broken. A producer does not stop because the main loop decided to: the
    /// socket keeps delivering while the drain runs, so a record can be accepted
    /// after the drain has looked and before the receiver is destroyed. Once
    /// that receiver is dropped, whatever the queue still holds goes with it —
    /// silently, and it is the newest data in the recording.
    ///
    /// So every accepted record must have one of two fates and no third:
    /// **written**, or **refused at the hand-off** so the producer knows. The
    /// count is the invariant; which of the two a given record gets is a matter
    /// of timing and neither is a loss.
    ///
    /// A real runtime with a thread to spare, because the race is the subject:
    /// the producer has to be able to hand a record over *while* the drain is
    /// running, which is exactly what a single-threaded test cannot arrange.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn every_record_the_hand_off_accepted_is_written_or_refused() {
        let (tx, mut rx, _fatal) = queue::test_bounded::<Record>(WRITER_HOP, 64);
        let accepted = Arc::new(AtomicUsize::new(0));
        let counted = accepted.clone();
        let (started, running) = tokio::sync::oneshot::channel();

        // The tape does not stop for the shutdown. This keeps handing records
        // over for exactly as long as the hand-off keeps taking them.
        let producer = tokio::spawn(async move {
            let mut started = Some(started);
            while tx.send(record("still arriving")).is_ok() {
                counted.fetch_add(1, Ordering::SeqCst);
                if let Some(started) = started.take() {
                    let _ = started.send(());
                }
                tokio::task::yield_now().await;
            }
        });
        // So the drain is a wind-down of something live, not of a task that
        // never ran.
        running.await.unwrap();

        let mut written = 0;
        let drained = drain_to_completion(&mut rx, |_| {
            written += 1;
            Ok(())
        })
        .await;
        producer.await.unwrap();

        assert_eq!(drained, written);
        assert!(written > 0, "the producer never got to hand anything over");
        assert_eq!(
            written,
            accepted.load(Ordering::SeqCst),
            "the shutdown destroyed records the hand-off had already accepted"
        );
    }

    /// The other half of the same invariant, and the reason the order in
    /// [`wind_down`] is what it is.
    ///
    /// Closing the hand-off keeps an accepted record from being destroyed, but
    /// closing it *under a live producer* would refuse the tape still arriving.
    /// Stopping the producers first is what makes the record handed over while
    /// the collector is winding down reach the disk rather than be refused at
    /// the door — the two outcomes are not equivalent, and this is the one the
    /// order exists to choose.
    ///
    /// **What this pin does not see**, and the reason
    /// `a_hand_off_the_wind_down_cannot_reach_is_refused_without_reporting_a_
    /// fault` exists: the producer here is the collection task itself, so the
    /// cancellation reaches it. Every real one spawns children the cancellation
    /// cannot reach, and for a long time this pin was cited as proof that a
    /// clean stop raises no fatal signal. It proves that only of a producer with
    /// no children at all.
    #[tokio::test]
    async fn a_producer_still_delivering_when_the_loop_stops_is_written_not_refused() {
        let (tx, mut writer_rx, mut fatal) = queue::test_bounded::<Record>(WRITER_HOP, 128);
        let (_poller_tx, mut poller_rx, _poller_fatal) =
            queue::test_bounded::<Record>(POLLER_HOP, 8);
        let stopping = fatal.stopping();
        let accepted = Arc::new(AtomicUsize::new(0));
        let counted = accepted.clone();

        // Stands in for a collection task: hands records over until it is
        // stopped, and returns an error only if one is refused.
        let collection = tokio::spawn(async move {
            loop {
                tx.send(record("live tape"))?;
                counted.fetch_add(1, Ordering::SeqCst);
                // The await point cancellation lands on, exactly as a socket
                // read is on every backend.
                tokio::task::yield_now().await;
            }
        });
        // Let it get going, so this is a wind-down of something live rather
        // than of a task that never ran.
        tokio::task::yield_now().await;

        let mut written = 0;
        let (task_error, recovered) = wind_down(
            &stopping,
            collection,
            PRODUCER_STOP_GRACE,
            &mut writer_rx,
            &mut poller_rx,
            |_| {
                written += 1;
                Ok(())
            },
        )
        .await;

        assert!(written > 0, "the producer never got to hand anything over");
        assert_eq!(recovered, written);
        assert_eq!(
            written,
            accepted.load(Ordering::SeqCst),
            "a record the producer handed over before it was stopped was dropped"
        );
        assert!(
            task_error.is_none(),
            "a cancelled producer has no error of its own, and reporting a refusal here \
             would make every clean stop look like a broken recording: {task_error:?}"
        );
        // Nothing was refused at all, which is the stronger statement and the
        // one the order buys: a producer the cancellation reaches stops handing
        // records over before the close can turn any of them away. The latch
        // would have suppressed the report either way, so the count is what
        // distinguishes "no refusal" from "a refusal nobody heard".
        assert_eq!(
            stopping.refusals(),
            0,
            "a record was refused although the producer had been stopped first"
        );
        // A deadline of zero is a single poll of an empty channel: it cannot
        // wait, so it cannot flake, and the keepalive in `FatalRx` means the
        // alternative — `recv()` on a channel with no sender left — would hang
        // rather than answer.
        assert!(
            tokio::time::timeout(Duration::ZERO, fatal.recv())
                .await
                .is_err(),
            "a clean shutdown raised the fatal signal"
        );
    }

    /// A collection task that had already failed knows the real cause, and the
    /// wind-down must not throw it away by cancelling a task that has already
    /// returned.
    #[tokio::test]
    async fn the_collection_tasks_own_error_survives_the_wind_down() {
        let (tx, mut writer_rx, fatal) = queue::test_bounded::<Record>(WRITER_HOP, 8);
        let (_poller_tx, mut poller_rx, _poller_fatal) =
            queue::test_bounded::<Record>(POLLER_HOP, 8);
        let stopping = fatal.stopping();

        let collection = tokio::spawn(async move {
            tx.send(record("the last frame before it broke")).unwrap();
            Err::<(), _>(anyhow!("BTCUSDT is not listed on this venue"))
        });
        // The ordering `main` actually sees: the writer hop only reports itself
        // closed once the task has returned and its sender went with it, so by
        // the time the loop breaks on that the task is finished and has an
        // error to give.
        while !collection.is_finished() {
            tokio::task::yield_now().await;
        }

        let mut written = 0;
        let (task_error, _) = wind_down(
            &stopping,
            collection,
            PRODUCER_STOP_GRACE,
            &mut writer_rx,
            &mut poller_rx,
            |_| {
                written += 1;
                Ok(())
            },
        )
        .await;

        assert_eq!(
            task_error.map(|error| error.to_string()),
            Some("BTCUSDT is not listed on this venue".to_string())
        );
        assert_eq!(
            written, 1,
            "the last record it handed over before failing is the window around the fault"
        );
    }

    /// The grace is a guard, not a budget: a producer whose cancellation has
    /// not landed must not be able to hold the shutdown open until systemd's
    /// `TimeoutStopSec` SIGKILLs the process and truncates the day's gzip
    /// member. The wind-down goes on without it, and what it had already handed
    /// over is still written — the close is what makes that safe, since the
    /// straggler can no longer put anything into a channel about to be dropped.
    ///
    /// A grace of zero is the degenerate case of the same thing and the only
    /// one a test can reach deterministically: cancellation is delivered by the
    /// scheduler, so a task that is pending when `stop_collecting` is called
    /// cannot possibly have joined before a deadline that has already passed.
    /// Virtual time, so it costs nothing and cannot flake on a busy machine.
    #[tokio::test(start_paused = true)]
    async fn a_producer_that_has_not_stopped_yet_does_not_hold_the_shutdown_open() {
        let (tx, mut writer_rx, fatal) = queue::test_bounded::<Record>(WRITER_HOP, 8);
        let (_poller_tx, mut poller_rx, _poller_fatal) =
            queue::test_bounded::<Record>(POLLER_HOP, 8);
        let stopping = fatal.stopping();

        tx.send(record("accepted before it stalled")).unwrap();
        let stalled = tokio::spawn(async move {
            let _held = tx;
            std::future::pending::<()>().await;
            Ok(())
        });

        let mut written = 0;
        let (task_error, recovered) = wind_down(
            &stopping,
            stalled,
            Duration::ZERO,
            &mut writer_rx,
            &mut poller_rx,
            |_| {
                written += 1;
                Ok(())
            },
        )
        .await;

        assert!(task_error.is_none());
        assert_eq!(
            recovered, 1,
            "what it handed over before it stalled is still owed to the recording"
        );
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
    #[tokio::test]
    async fn a_failing_write_stops_the_drain() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Record>(8);
        tx.try_send(record("first")).unwrap();
        tx.try_send(record("second")).unwrap();

        let mut seen = 0;
        let count = drain_to_completion(&mut rx, |_| {
            seen += 1;
            Err(anyhow!("the device stopped answering"))
        })
        .await;

        assert_eq!(count, 0);
        assert_eq!(
            seen, 1,
            "the drain must not keep trying after a write failed"
        );
    }
}
