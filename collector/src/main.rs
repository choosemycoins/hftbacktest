use std::time::Duration;

use anyhow::anyhow;
use chrono::Utc;
use clap::Parser;
use tokio::{self, select};
use tracing::{error, info, warn};

use crate::{
    file::Writer,
    queue::{QUEUE_CAPACITY, WRITER_HOP},
};

mod backoff;
mod binance;
mod binancefuturescm;
mod binancefuturesum;
mod bybit;
mod disk;
mod error;
mod file;
mod hyperliquid;
mod lock;
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
    /// It only ever catches TOTAL silence: not a dead depth stream while trades
    /// still arrive, not one symbol of ten that stopped, not one Hyperliquid
    /// cadence out of three. Sidecar records do not count as data.
    #[arg(long, default_value_t = 5)]
    stall_timeout_min: u64,

    /// Skip the startup check that every requested symbol exists on the venue.
    ///
    /// Only Hyperliquid implements the check today. Leave it on: an unknown
    /// coin there closes the whole WebSocket, taking every valid subscription
    /// with it, and the collector then reconnects forever writing partial data
    /// while looking healthy.
    #[arg(long)]
    no_symbol_check: bool,
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

    // Bounded, with `full => fatal` as the policy — see `queue.rs`. The fatal
    // channel is how a producer that has no error path of its own (the
    // detached REST snapshot tasks) reaches this loop.
    let (fatal_tx, mut fatal_rx) = queue::fatal_channel();
    let (writer_tx, mut writer_rx) = queue::bounded(WRITER_HOP, QUEUE_CAPACITY, fatal_tx);

    // Open the recording with a record of what produced it. The scoped clone
    // is dropped immediately: keeping a sender alive here would stop
    // `writer_rx.recv()` ever returning `None`, and that is what tells the
    // main loop the collection task has died.
    {
        let meta_tx = writer_tx.clone();
        // Nothing has been enqueued yet, so this cannot fail; propagating
        // rather than discarding keeps the rule that a hand-off result is
        // never ignored.
        meta_tx.send((
            chrono::Utc::now(),
            file::META_STREAM.to_string(),
            serde_json::json!({
                "_collector": "session_start",
                "version": env!("CARGO_PKG_VERSION"),
                "commit": env!("COLLECTOR_GIT_COMMIT"),
                "branch": env!("COLLECTOR_GIT_BRANCH"),
                "dirty": env!("COLLECTOR_GIT_DIRTY"),
                "exchange": args.exchange,
                "symbols": args.symbols,
                "bybit_depths": args.bybit_depths,
                "hl_l2_modes": args.hl_l2_modes,
            })
            .to_string(),
        ))?;
    }

    let collection_task = match args.exchange.as_str() {
        "binancefutures" | "binancefuturesum" => {
            let streams = [
                "$symbol@trade",
                "$symbol@bookTicker",
                "$symbol@depth@0ms",
                // "$symbol@@markPrice@1s"
            ]
            .iter()
            .map(|stream| stream.to_string())
            .collect();

            tokio::spawn(binancefuturesum::run_collection(
                streams,
                args.symbols,
                writer_tx,
            ))
        }
        "binancefuturescm" => {
            let streams = [
                "$symbol@trade",
                "$symbol@bookTicker",
                "$symbol@depth@0ms",
                // "$symbol@@markPrice@1s"
            ]
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

            let mut subscriptions = vec![
                SubscriptionSpec::plain("trades"),
                SubscriptionSpec::plain("bbo"),
            ];
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
        exchange => {
            return Err(anyhow!("{exchange} is not supported."));
        }
    };

    let mut shutdown = Shutdown::new()?;
    let mut writer = Writer::new(&args.path, &args.exchange);

    // Sampled rather than checked per write: a statvfs on the hot path would be
    // a syscall per message. A minute is far shorter than the time it takes to
    // consume the 5 GB default floor at the observed ~20 MB/day/symbol.
    let mut disk_check = tokio::time::interval(Duration::from_secs(60));
    disk_check.tick().await; // the first tick is immediate; startup already checked

    // The guard against silent nothing. Armed from here, so a venue that never
    // sends is caught even though not one record ever arrives — see
    // `watchdog.rs`, including what it deliberately does not catch.
    let mut watchdog = watchdog::StallWatchdog::new(args.stall_timeout_min);
    if args.stall_timeout_min == 0 {
        warn!("stall watchdog disabled; a silent feed will not stop the collector");
    } else {
        info!(
            minutes = args.stall_timeout_min,
            "stall watchdog armed on total silence"
        );
    }

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
            _ = disk_check.tick() => {
                match check_disk(&args.path, args.min_free_gb) {
                    Ok(free) => {
                        // Written to the sidecar, not just the log: it makes a
                        // recording carry its own capacity history, and it is
                        // the one file an operator can tail live.
                        let _ = writer.write(
                            Utc::now(),
                            file::META_STREAM.to_string(),
                            serde_json::json!({
                                "_collector": "disk",
                                "free_bytes": free,
                                "path": args.path,
                            })
                            .to_string(),
                        );
                    }
                    Err(error) => {
                        error!(%error, "stopping: not enough free disk space");
                        let _ = writer.write(
                            Utc::now(),
                            file::META_STREAM.to_string(),
                            serde_json::json!({
                                "_collector": "disk_exhausted",
                                "error": error.to_string(),
                            })
                            .to_string(),
                        );
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
                let _ = writer.write(
                    Utc::now(),
                    file::META_STREAM.to_string(),
                    serde_json::json!({
                        "_collector": "stalled",
                        "silent_for_s": silence.as_secs(),
                        "stall_timeout_min": args.stall_timeout_min,
                    })
                    .to_string(),
                );
                fatal = Some(error);
                break;
            }
            reason = fatal_rx.recv() => {
                // A queue filled up, or lost its receiver. Either way frames are
                // no longer reaching disk, and the producers have stopped rather
                // than drop them silently. Breaking here is what runs the
                // writer's `Drop` — finishing the gzip members — before the
                // non-zero exit; the producers cannot do that from where they
                // are, which is the whole reason the signal comes back here.
                error!(reason, "stopping: a data hand-off failed");
                let _ = writer.write(
                    Utc::now(),
                    file::META_STREAM.to_string(),
                    serde_json::json!({
                        "_collector": "queue_overflow",
                        "error": &reason,
                    })
                    .to_string(),
                );
                fatal = Some(anyhow!(reason));
                break;
            }
            r = writer_rx.recv() => match r {
                Some((recv_time, symbol, data)) => {
                    // Counted here, as the record leaves the queue for the
                    // writer, and never where it was enqueued: messages piling
                    // up behind a stalled writer must not read as life. A write
                    // that then fails ends the loop anyway.
                    watchdog.record_write(&symbol);
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
