//! The connector process: one bot-facing IPC endpoint, one exchange backend.
//!
//! Two things in here are generic supervision rather than plumbing, and both were written
//! against a measured live session (design note §11.14, `AGENTS.md` §4.7):
//!
//! # The orderly stop
//!
//! Stopping has an order, and it is the reverse of starting. The signal handler does one
//! thing — set `stopping` — and everything after it is sequenced here:
//!
//! 1. [`run_receive_task`] stops accepting bot requests and returns, dropping its
//!    [`PublishEvent`] sender. A registration already on the queue is no longer answered
//!    either ([`supervision::publishable_while_stopping`]).
//! 2. The registered instruments are swept, if configured to be — a stopping connector
//!    leaves no unattended maker — and the stop **waits for that sweep**, bounded by
//!    `shutdown.sweep_timeout_ms`. The publish task is still in its steady-state loop
//!    throughout, so the cancels it confirms reach whichever bots are still listening.
//! 3. `Connector::shutdown` winds the backend's stream tasks down, dropping theirs.
//! 4. Only then is the publish task told to stop, and it stops only once **every** sender is
//!    gone ([`supervision::drain_publish`]) or `shutdown.grace_ms` has passed.
//!
//! That ordering is the entire fix for "a clean SIGINT always ends in a panic". The cause
//! was never the `.unwrap()` on `pub_tx.send(..)` that appears all over this crate — it was
//! that the publish task used to return *first*, leaving those senders pointing at a closed
//! channel. Those `unwrap`s are the supervisor contract for **unexpected** death and they
//! are deliberately untouched: an unexpected publish-task failure still panics into
//! `exit(1)`, while the orderly path reaches [`exit`] with the receiver still held, so a
//! send-after-close cannot happen on it at all.
//!
//! **`exit(0)` is not automatic**, and that is the other half of the contract: it is
//! reserved for a stop that did what it promised. A sweep that ran out of time, a publish
//! task that failed or vanished, and a receive loop that ended on its own each exit 1 —
//! [`supervision::exit_code`] decides, from a [`supervision::StopReport`].
//!
//! # Bot liveness
//!
//! A bot's orders outlive the bot. Measured: a fill landed 3 s after the bot's `SIGINT` and
//! moved the position, and the connector only sweeps on *re-registration*, so until the
//! supervisor restarted the bot the venue was hosting an unattended market maker.
//!
//! **Why an application-level heartbeat and not iceoryx's own peer-death detection.**
//! iceoryx2 0.6.1 does offer it — `Node::list()` reports `NodeState::Dead` from a file-lock
//! monitoring token, `Node::cleanup_dead_nodes()` reaps it, and a publish-subscribe service's
//! `dynamic_config()` will report `number_of_subscribers()` and list each subscriber's
//! `NodeId`. None of it answers the question this policy asks:
//!
//! * **It is not attributable.** The action is "cancel the orders of the instruments *this*
//!   bot registered", and the connector's key for that is the bot id in the IPC user header —
//!   a random `u64` the bot mints for itself. Nothing iceoryx exposes carries it; a
//!   `SubscriberDetails` carries a `NodeId`, and no message carries a `NodeId`. With two bots
//!   on one connector, node liveness can only say "somebody left".
//! * **It cannot see a wedged bot.** A live process whose `elapse` loop has stopped turning
//!   still holds a perfectly healthy node and subscriber, and its resting orders are exactly
//!   as unattended as a dead one's. `LiveRequest::Heartbeat` is sent *from inside `elapse`*,
//!   so it reports the loop, not the process.
//! * **Detection would mean system-wide GC on a timer.** `Node::list`/`cleanup_dead_nodes`
//!   take cleaner locks and remove other processes' stale resources, and their latency is not
//!   ours to bound. That is a strange side effect for a trading-safety decision.
//!
//! So the bot says so itself, and the connector arms a bot only once it has heard from it —
//! which is what keeps a bot too old to know the variant from ever being swept.
//!
//! **What the sweep can and cannot be scoped to.** A venue cancel reaches every order the
//! *account* holds on a symbol; nothing on the venue records which bot asked for an order,
//! because [`Connector::submit`] is never told. So "cancel the dead bot's orders" only
//! exists for symbols no other live bot is quoting, and the registry hands out exactly
//! those (`supervision::DeadBot`). A symbol two bots share is named in the log and left
//! alone until the last of them goes: flattening a healthy market maker to tidy up after a
//! dead one is the worse of the two failures, and it is the one that is invisible to the
//! bot it happens to.

use std::{
    collections::{HashMap, hash_map::Entry},
    fs::read_to_string,
    panic,
    process::exit,
    sync::{
        Arc,
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use chrono::Utc;
use clap::Parser;
use hftbacktest::{
    live::ipc::{
        TO_ALL,
        iceoryx::{ChannelError, IceoryxBuilder, IceoryxSender},
    },
    prelude::*,
};
use iceoryx2::{
    node::NodeBuilder,
    prelude::{SignalHandlingMode, ipc},
};
use tokio::{
    runtime::Builder,
    select,
    signal,
    sync::{
        Notify,
        mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
        oneshot,
    },
};
use tracing::{error, info, warn};

use crate::{
    binancefutures::BinanceFutures,
    binancespot::BinanceSpot,
    bybit::Bybit,
    connector::{Connector, ConnectorBuilder, GetOrders, PublishEvent, SweepReason},
    hyperliquid::Hyperliquid,
    lighter::Lighter,
    supervision::{
        BotRegistry,
        DrainOutcome,
        LIVENESS_POLL_INTERVAL,
        StopKind,
        StopReport,
        SupervisionConfig,
        drain_publish,
        exit_code,
        publishable_while_stopping,
    },
};

#[cfg(feature = "binancefutures")]
pub mod binancefutures;
#[cfg(feature = "binancespot")]
pub mod binancespot;
#[cfg(feature = "bybit")]
pub mod bybit;
#[cfg(feature = "hyperliquid")]
pub mod hyperliquid;
#[cfg(feature = "lighter")]
pub mod lighter;

mod connector;
//mod fuse;
mod supervision;
mod utils;

struct Position {
    qty: f64,
    exch_ts: i64,
}

/// Serves the bots: their registrations, their orders, and their heartbeats.
///
/// Runs on the main thread and blocks, so its return is what begins the orderly stop. Two
/// things end it: `stopping`, set by the signal handler, and a node failure.
///
/// It also owns `registry`, and therefore the bot-liveness policy — this is the one place
/// that sees both which bot registered what and which bots are still talking. `registry` is
/// borrowed rather than shared because there is exactly one reader and one writer, and both
/// are this function; `main` reads it afterwards, when this has returned.
fn run_receive_task(
    name: &str,
    tx: UnboundedSender<PublishEvent>,
    connector: &mut Box<dyn Connector>,
    registry: &mut BotRegistry,
    supervision: &SupervisionConfig,
    stopping: &AtomicBool,
) -> Result<(), ChannelError> {
    let node = NodeBuilder::new()
        .signal_handling_mode(SignalHandlingMode::Disabled)
        .create::<ipc::Service>()
        .map_err(|error| ChannelError::BuildError(error.to_string()))?;
    let bot_rx = IceoryxBuilder::new(name).bot(false).receiver()?;
    let liveness_timeout = supervision.bot_liveness.timeout();
    let mut last_liveness_poll = Instant::now();
    loop {
        // Checked before receiving, not after: once the stop has begun, a request that is
        // already on the queue must not be acted on. Submitting an order into a connector
        // that is about to sweep and exit is the one thing worse than dropping the request.
        if stopping.load(Ordering::Relaxed) {
            info!("No longer accepting bot requests; the connector is stopping.");
            break;
        }
        let cycle_time = Duration::from_nanos(1000);
        match node.wait(cycle_time) {
            Ok(()) => {
                while let Some((id, ev)) = bot_rx.receive()? {
                    match ev {
                        LiveRequest::Order {
                            symbol: asset,
                            order,
                        } => match order.req {
                            Status::New => {
                                // Requests to the Connector submit the new order.
                                connector.submit(asset, order, tx.clone());
                            }
                            Status::Canceled => {
                                // Requests to the Connector cancel the order.
                                connector.cancel(asset, order, tx.clone());
                            }
                            status => {
                                error!(?status, "An invalid request was received from the bot.");
                            }
                        },
                        LiveRequest::RegisterInstrument {
                            symbol,
                            tick_size,
                            lot_size,
                        } => {
                            registry.register(id, symbol.clone());
                            // Makes prepare the publisher thread to also add the instrument.
                            tx.send(PublishEvent::RegisterInstrument {
                                id,
                                symbol: symbol.clone(),
                                tick_size,
                                lot_size,
                            })
                            .unwrap();
                            // Requests to the Connector subscribe to the necessary feeds for the
                            // instrument.
                            connector.register(symbol);
                        }
                        // Deliberately silent. It arrives once a second per instrument per
                        // bot, and the only thing it changes is a timestamp.
                        LiveRequest::Heartbeat => {
                            if registry.heartbeat(id, Instant::now()) {
                                warn!(
                                    bot_id = id,
                                    "A bot presumed dead is heartbeating again. Its orders \
                                     were already cancelled; the liveness window is shorter \
                                     than this bot manages to report at — raise \
                                     `bot_liveness.timeout_ms`, or lower the bot's \
                                     heartbeat interval."
                                );
                            }
                        }
                    }
                }

                if supervision.bot_liveness.enabled
                    && last_liveness_poll.elapsed() >= LIVENESS_POLL_INTERVAL
                {
                    last_liveness_poll = Instant::now();
                    for dead in registry.take_dead(last_liveness_poll, liveness_timeout) {
                        // Named separately because they are two different situations for
                        // whoever reads this at 3am, and because three of the four backends
                        // implement `sweep` as a documented no-op: this says what was asked
                        // for, and the backend says what it did about it.
                        if !dead.shared.is_empty() {
                            error!(
                                bot_id = dead.id,
                                shared = ?dead.shared,
                                "A bot stopped heartbeating, but another live bot is quoting \
                                 the same instruments. Its orders there are LEFT RESTING: a \
                                 cancel reaches every order the account holds on a symbol, \
                                 so clearing them would flatten the live bot's grid too. \
                                 They are cleared when the last bot on those instruments \
                                 goes, or when this connector stops."
                            );
                        }
                        if dead.symbols.is_empty() {
                            continue;
                        }
                        error!(
                            bot_id = dead.id,
                            symbols = ?dead.symbols,
                            ?liveness_timeout,
                            "A bot stopped heartbeating. Asking the backend to cancel the \
                             orders it left resting: until it is restarted nothing is \
                             managing them, and a fill would move a position no one is \
                             watching. The connector stays up for it to reconnect to."
                        );
                        // Detached on purpose, unlike the shutdown sweep: nothing is waiting
                        // on this one, the connector keeps running, and dropping a
                        // `JoinHandle` does not cancel its task.
                        drop(connector.sweep(
                            dead.symbols,
                            SweepReason::BotDied { id: dead.id },
                            tx.clone(),
                        ));
                    }
                }
            }
            Err(error) => {
                // The one thing that ends this loop without anybody asking, and the sole
                // trigger for `StopKind::ReceiveLoopFailed`. Logged here because the caller
                // only learns *that* it happened: the loop returns `Ok(())` so that the stop
                // still runs in order.
                error!(
                    ?error,
                    "The iceoryx node failed; this connector can no longer receive from any \
                     bot."
                );
                break;
            }
        }
    }
    Ok(())
}

/// Publishes one [`PublishEvent`] to the bots.
///
/// Extracted so the steady-state loop and [`drain_publish`] run **the same** handler: a
/// cancel the venue confirms during the shutdown sweep has to reach whichever bots are still
/// listening, and a shutdown path with its own quieter handler is a shutdown path nobody
/// tests.
///
/// `stopping` is the one thing the two paths do differ on, and it is a refusal rather than a
/// second handler: see [`publishable_while_stopping`].
fn handle_publish_event(
    msg: PublishEvent,
    bot_tx: &IceoryxSender<LiveEvent>,
    order_manager: &Arc<Mutex<dyn GetOrders>>,
    depth: &mut HashMap<String, FusedHashMapMarketDepth>,
    position: &mut HashMap<String, Position>,
    stopping: &AtomicBool,
) -> Result<(), ChannelError> {
    if stopping.load(Ordering::Relaxed) && !publishable_while_stopping(&msg) {
        if let PublishEvent::RegisterInstrument { id, symbol, .. } = &msg {
            warn!(
                bot_id = id,
                %symbol,
                "Not answering an instrument registration: the connector is stopping and \
                 would be telling the bot its state is settled and it may submit — into a \
                 connector that has stopped reading requests."
            );
        }
        return Ok(());
    }
    match msg {
        PublishEvent::RegisterInstrument {
            id,
            symbol,
            tick_size,
            lot_size,
        } => {
            // Sends the current state (orders, position, and market depth) to the bot that
            // requested to add this instrument in batch mode.
            bot_tx.send(id, &LiveEvent::BatchStart)?;

            for order in order_manager.lock().unwrap().orders(Some(symbol.clone())) {
                bot_tx.send(
                    id,
                    &LiveEvent::Order {
                        symbol: symbol.clone(),
                        order,
                    },
                )?;
            }

            if let Some(position) = position.get(&symbol) {
                bot_tx.send(
                    id,
                    &LiveEvent::Position {
                        symbol: symbol.clone(),
                        qty: position.qty,
                        exch_ts: position.exch_ts,
                    },
                )?;
            }

            match depth.entry(symbol.clone()) {
                Entry::Occupied(mut entry) => {
                    let depth_: &mut FusedHashMapMarketDepth = entry.get_mut();
                    let snapshot = depth_.snapshot();
                    for event in snapshot {
                        bot_tx.send(
                            id,
                            &LiveEvent::Feed {
                                symbol: entry.key().clone(),
                                event,
                            },
                        )?;
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert(FusedHashMapMarketDepth::new(tick_size, lot_size));
                }
            }

            bot_tx.send(id, &LiveEvent::BatchEnd)?;

            // Mark the end of the initial state snapshot for this asset. After this
            // the bot is free to submit/cancel orders based on orders/position.
            // See `LiveEvent::SnapshotComplete` docs for the exact contract.
            bot_tx.send(
                id,
                &LiveEvent::SnapshotComplete {
                    symbol,
                    snapshot_time_ns: Utc::now().timestamp_nanos_opt().unwrap_or(0),
                },
            )?;
        }
        PublishEvent::LiveEvent(ev) => {
            // The live event will only be published if the result is true.
            for ev in handle_ev(ev, depth, position) {
                bot_tx.send(TO_ALL, &ev)?;
            }
        }
        PublishEvent::BatchStart(id) => {
            bot_tx.send(id, &LiveEvent::BatchStart)?;
        }
        PublishEvent::BatchEnd(id) => {
            bot_tx.send(id, &LiveEvent::BatchEnd)?;
        }
    }
    Ok(())
}

/// Publishes to the bots, and — on an orderly stop — is the **last** thing in the process to
/// stop doing so.
///
/// The `Err` return is the unexpected-death path: it is unwrapped by the caller under the
/// `exit(1)` panic hook, which is the supervisor contract `AGENTS.md` §4.7 describes. This
/// function never returns on the orderly path; see the parking comment at the end.
async fn run_publish_task(
    name: &str,
    order_manager: Arc<Mutex<dyn GetOrders>>,
    mut rx: UnboundedReceiver<PublishEvent>,
    shutdown_signal: Arc<Notify>,
    grace: Duration,
    stopping: Arc<AtomicBool>,
    drained: oneshot::Sender<DrainOutcome>,
) -> Result<(), ChannelError> {
    let mut depth = HashMap::new();
    let mut position: HashMap<String, Position> = HashMap::new();
    let bot_tx = IceoryxBuilder::new(name).bot(false).sender()?;

    loop {
        select! {
            // Notified by `main`, and **not** by the signal handler: the sweep runs before
            // this, publishing as it goes, and it must not be publishing into a drain that
            // is counting down a deadline meant for something else.
            _ = shutdown_signal.notified() => {
                break;
            }
            Some(msg) = rx.recv() => {
                handle_publish_event(
                    msg,
                    &bot_tx,
                    &order_manager,
                    &mut depth,
                    &mut position,
                    &stopping,
                )?;
            }
        }
    }

    // The orderly path. Everything still in flight goes out, and then this waits for the
    // senders themselves — the backend's stream tasks', and anything the sweep left behind —
    // to be dropped.
    let outcome = drain_publish(&mut rx, grace, |msg| {
        handle_publish_event(
            msg,
            &bot_tx,
            &order_manager,
            &mut depth,
            &mut position,
            &stopping,
        )
    })
    .await;
    match &outcome {
        DrainOutcome::SendersDropped { published } => {
            info!(
                published,
                "Drained the connector's publish queue; nothing can send now."
            );
        }
        DrainOutcome::GraceElapsed { published } => {
            // The sweep has already finished by the time this runs — `main` waits for it
            // before waking this task — so what is still holding a sender is a stream task.
            // While the two shared one deadline this line named the wrong cause on the only
            // backend that has both.
            warn!(
                published,
                ?grace,
                "Stopping with stream senders still alive: this backend does not wind its \
                 tasks down. Exiting without dropping the receiver, so nothing can panic on \
                 a closed channel."
            );
        }
        DrainOutcome::PublishFailed { published } => {
            // Not fatal, deliberately. The connector is already stopping, and turning a
            // clean stop into exit(1) is the whole bug being fixed here.
            warn!(
                published,
                "Couldn't publish while draining; stopping anyway."
            );
        }
    }
    let _ = drained.send(outcome);

    // **`rx` is never dropped.** `main` calls `exit(0)` once it has the outcome above, and
    // until it does, every `pub_tx.send(..).unwrap()` left anywhere in the process must
    // still find a live receiver. Returning here — which is what this function used to do
    // the instant the shutdown signal arrived — is precisely what made every clean SIGINT
    // end in a panic and exit 1.
    std::future::pending::<()>().await;
    unreachable!("std::future::pending never completes")
}

/// Maintains the market depth for all added instruments, allowing another bot to request the same
/// instrument and publishing the market depth snapshot, and fuses the market depth from different
/// streams, such as L1 or L2 with varying depths and update frequencies, to provide the most
/// granular and frequent updates.
///
/// Returns true when the received live event needs to be published; otherwise, it does not.
/// For example, publication is unnecessary if the received market depth data is outdated by more
/// recent data from a different stream due to fusion.
fn handle_ev(
    ev: LiveEvent,
    depth: &mut HashMap<String, FusedHashMapMarketDepth>,
    position: &mut HashMap<String, Position>,
) -> Vec<LiveEvent> {
    match &ev {
        LiveEvent::Feed { symbol, event } => {
            if event.is(BUY_EVENT | DEPTH_EVENT) {
                let depth_ = {
                    match depth.get_mut(symbol) {
                        Some(d) => d,
                        None => return vec![],
                    }
                };
                return depth_
                    .update_bid_depth(event.clone())
                    .iter()
                    .map(|event| LiveEvent::Feed {
                        symbol: symbol.clone(),
                        event: event.clone(),
                    })
                    .collect();
            } else if event.is(SELL_EVENT | DEPTH_EVENT) {
                let depth_ = {
                    match depth.get_mut(symbol) {
                        Some(d) => d,
                        None => return vec![],
                    }
                };
                return depth_
                    .update_ask_depth(event.clone())
                    .iter()
                    .map(|event| LiveEvent::Feed {
                        symbol: symbol.clone(),
                        event: event.clone(),
                    })
                    .collect();
            } else if event.is(BUY_EVENT | DEPTH_BBO_EVENT) {
                let depth_ = {
                    match depth.get_mut(symbol) {
                        Some(d) => d,
                        None => return vec![],
                    }
                };
                return depth_
                    .update_best_bid(event.clone())
                    .iter()
                    .map(|event| LiveEvent::Feed {
                        symbol: symbol.clone(),
                        event: event.clone(),
                    })
                    .collect();
            } else if event.is(SELL_EVENT | DEPTH_BBO_EVENT) {
                let depth_ = {
                    match depth.get_mut(symbol) {
                        Some(d) => d,
                        None => return vec![],
                    }
                };
                return depth_
                    .update_best_ask(event.clone())
                    .iter()
                    .map(|event| LiveEvent::Feed {
                        symbol: symbol.clone(),
                        event: event.clone(),
                    })
                    .collect();
            } else if event.is(DEPTH_CLEAR_EVENT) {
                let depth_ = {
                    match depth.get_mut(symbol) {
                        Some(d) => d,
                        None => return vec![],
                    }
                };
                depth_.clear_depth(Side::None, 0.0, 0);
            }
        }
        LiveEvent::Position {
            symbol,
            qty,
            exch_ts,
        } => {
            if position.contains_key(symbol) {
                let position = position.get_mut(symbol).unwrap();
                return if *exch_ts >= position.exch_ts {
                    position.qty = *qty;
                    vec![ev]
                } else {
                    vec![]
                };
            } else {
                position.insert(
                    symbol.clone(),
                    Position {
                        qty: *qty,
                        exch_ts: *exch_ts,
                    },
                );
                return vec![ev];
            }
        }
        _ => {}
    }
    vec![ev]
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Name of the connector, used when connecting the bot to the connector.
    name: String,

    /// Connector
    /// * binancefutures: Binance USD-m Futures
    /// * binancespot: Binance Spot
    /// * bybit: Bybit Linear Futures
    /// * hyperliquid: Hyperliquid perpetuals (market data only; orders are rejected)
    /// * lighter: Lighter perpetuals (market data only; orders are rejected)
    connector: String,

    /// Connector's configuration file path.
    config: String,
}

/// At least two runtime workers, whatever the host has.
///
/// [`run_receive_task`] is synchronous and busy-loops for the connector's whole life, so it
/// holds a worker the entire time. On a single-worker runtime — a 1-vCPU host, or
/// `TOKIO_WORKER_THREADS=1` — that is *the* worker: the bot-death sweep is `tokio::spawn`ed
/// from inside that loop and would never be polled, so a dead bot's orders would be logged
/// as swept and not cancelled, and the signal handler would never run either, leaving the
/// connector ignoring its own `SIGTERM`. An explicit floor also overrides the environment
/// variable, which is the other way a deployment gets there by accident.
const MIN_WORKER_THREADS: usize = 2;

/// Picks the process-wide rustls provider before anything opens a TLS connection.
///
/// A `cargo build --workspace` binary links TWO providers: this crate's deps enable
/// `rustls/ring`, while `py-hftbacktest → hftbacktest/s3 → aws-config` unifies in
/// `rustls/aws-lc-rs`. With both present rustls refuses to guess, and the first
/// handshake panics with "Could not automatically determine the process-level
/// CryptoProvider" — through the panic hook that is `exit(1)` on the first message
/// of every venue (AGENTS.md §4.7, reproduced on bybit 2026-07-28). Installing
/// explicitly makes the workspace build behave like `cargo build -p connector`.
///
/// Idempotent so a test harness that already installed one — or a future second
/// call — is a no-op rather than a startup panic.
fn install_crypto_provider() {
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        // A racing install between the check and here can only be the same provider;
        // losing that race is fine, so the error is deliberately ignored.
        let _ = rustls::crypto::ring::default_provider().install_default();
    }
}

fn main() {
    install_crypto_provider();
    let workers = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(MIN_WORKER_THREADS)
        .max(MIN_WORKER_THREADS);
    Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
        .unwrap()
        .block_on(run());
}

#[cfg(test)]
mod crypto_provider_tests {
    use super::install_crypto_provider;

    /// Pins the §4.7 fix: after startup installation a default provider exists, and a
    /// second call — or one racing a provider someone else installed — does not panic.
    #[test]
    fn installing_the_crypto_provider_is_effective_and_idempotent() {
        install_crypto_provider();
        assert!(rustls::crypto::CryptoProvider::get_default().is_some());
        install_crypto_provider();
    }
}

async fn run() {
    // Ensures that the main thread will terminate if any of its child threads panics.
    let orig_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        orig_hook(panic_info);
        exit(1);
    }));

    let args = Args::parse();

    tracing_subscriber::fmt::init();

    // The publish task is woken by `main`, at the end of the stop — see [`run`]'s ordering
    // below and the module docs. `notify_one`, not `notify_waiters`: it stores a permit when
    // nobody is waiting yet, so a wake that arrives before the publish task reaches its
    // `select!` is not dropped on the floor. There is exactly one waiter.
    let shutdown_signal = Arc::new(Notify::new());
    let shutdown_signal_for_drain = shutdown_signal.clone();
    // Read by the receive loop and the publish task without a lock: a `bool` the signal
    // handler sets once and everyone else only reads.
    let stopping = Arc::new(AtomicBool::new(false));
    let stopping_ = stopping.clone();
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            // Wait for either SIGINT (CTRL+C) or SIGTERM on Unix.
            let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
                .expect("failed to install SIGTERM handler");
            select! {
                _ = signal::ctrl_c() => {},
                _ = sigterm.recv() => {},
            }
        }
        #[cfg(not(unix))]
        {
            // Non-Unix platforms only has SIGINT.
            if let Err(error) = signal::ctrl_c().await {
                error!(?error, "Couldn't listen for shutdown signal.");
            }
        }
        // This is the whole of the handler. It says "stop", and everything that follows is
        // ordered by `main`: the receive loop returns, the sweep runs — publishing its
        // cancels through a publish task that is still in its steady-state loop — and only
        // then is the publish task told to drain. Waking the publish task here instead is
        // what put the sweep and the stream wind-down on one 5 s deadline, so a sweep whose
        // per-symbol allowance is 20 s could be cut off mid-POST by a stop that then
        // reported success.
        info!("Shutting down: no new bot requests will be accepted.");
        stopping_.store(true, Ordering::Relaxed);
    });

    let (pub_tx, pub_rx) = unbounded_channel();

    let config = read_to_string(&args.config)
        .map_err(|error| {
            error!(
                ?error,
                config = args.config,
                "An error occurred while reading the configuration file."
            );
        })
        .unwrap();

    // Generic supervision policy, read from the backend's own file. Refused rather than
    // defaulted: silently falling back would turn a typo in `timeout_ms` into a connector
    // that looks protected and is not.
    let supervision: SupervisionConfig = toml::from_str(&config)
        .map_err(|error| {
            error!(
                ?error,
                config = args.config,
                "Couldn't read the `[shutdown]` / `[bot_liveness]` supervision settings."
            );
        })
        .unwrap();
    info!(
        sweep_on_stop = supervision.shutdown.sweep_orders,
        shutdown_grace = ?supervision.shutdown.grace(),
        bot_liveness = supervision.bot_liveness.enabled,
        bot_timeout = ?supervision.bot_liveness.timeout(),
        "Supervision policy."
    );

    let mut connector: Box<dyn Connector> = match args.connector.as_str() {
        "binancefutures" => {
            let mut connector = BinanceFutures::build_from(&config)
                .map_err(|error| {
                    error!(?error, "Couldn't build the BinanceFutures connector.");
                })
                .unwrap();
            connector.run(pub_tx.clone());
            Box::new(connector)
        }
        "bybit" => {
            let mut connector = Bybit::build_from(&config)
                .map_err(|error| {
                    error!(?error, "Couldn't build the Bybit connector.");
                })
                .unwrap();
            connector.run(pub_tx.clone());
            Box::new(connector)
        }
        "binancespot" => {
            let mut connector = BinanceSpot::build_from(&config)
                .map_err(|error| {
                    error!(?error, "Couldn't build the Bybit connector.");
                })
                .unwrap();
            connector.run(pub_tx.clone());
            Box::new(connector)
        }
        "hyperliquid" => {
            let mut connector = Hyperliquid::build_from(&config)
                .map_err(|error| {
                    error!(?error, "Couldn't build the Hyperliquid connector.");
                })
                .unwrap();
            connector.run(pub_tx.clone());
            Box::new(connector)
        }
        "lighter" => {
            let mut connector = Lighter::build_from(&config)
                .map_err(|error| {
                    error!(?error, "Couldn't build the Lighter connector.");
                })
                .unwrap();
            connector.run(pub_tx.clone());
            Box::new(connector)
        }
        connector => {
            error!(%connector, "This connector doesn't exist.");
            exit(1);
        }
    };

    let name = args.name.clone();
    let order_manager = connector.order_manager();
    let grace = supervision.shutdown.grace();
    let stopping_for_publish = stopping.clone();
    let (drained_tx, drained_rx) = oneshot::channel();
    let _handle = thread::spawn(move || {
        let rt = Builder::new_current_thread().enable_all().build().unwrap();

        rt.block_on(async move {
            // Untouched: this `unwrap` under the process-wide panic hook is how an
            // *unexpected* publish-task death becomes exit(1). `AGENTS.md` §4.7.
            run_publish_task(
                &name,
                order_manager,
                pub_rx,
                shutdown_signal,
                grace,
                stopping_for_publish,
                drained_tx,
            )
            .await
            .map_err(|error: ChannelError| {
                error!(
                    ?error,
                    "An error occurred while sending a live event to the bots."
                );
            })
            .unwrap();
        });
    });

    // Kept back from the move below so the shutdown sweep has a sender of its own. Holding
    // it is what stops the publish task draining out from under the sweep.
    let sweep_tx = pub_tx.clone();
    let mut registry = BotRegistry::new();

    let name = args.name;
    run_receive_task(
        &name,
        pub_tx,
        &mut connector,
        &mut registry,
        &supervision,
        &stopping,
    )
    .map_err(|error| {
        error!(
            ?error,
            "An error occurred while receiving a request from the bots."
        );
    })
    .unwrap();

    // The receive loop also ends when its node fails, which nobody asked for. Both endings
    // stop the connector and both sweep — an unserved bot's orders are as unattended as a
    // dead bot's — but only one of them is a clean stop.
    let kind = if stopping.load(Ordering::Relaxed) {
        StopKind::Orderly
    } else {
        error!(
            "The bot-facing receive loop ended without a shutdown signal; this connector can \
             no longer serve any bot. Stopping as a failure."
        );
        // Everything below refuses to answer a registration once this is set, and the stop
        // is otherwise identical.
        stopping.store(true, Ordering::Relaxed);
        StopKind::ReceiveLoopFailed
    };

    // From here on this is the orderly stop, in order. See the module docs.
    let symbols = registry.registered_symbols();
    let sweep_timeout = supervision.shutdown.sweep_timeout();
    let sweep = if supervision.shutdown.sweep_orders && !symbols.is_empty() {
        info!(
            ?symbols,
            watched_bots = registry.watched(),
            ?sweep_timeout,
            "Cancelling the registered instruments' orders before stopping; a connector \
             that is going away cannot manage a resting order. Set `shutdown.sweep_orders \
             = false` to leave them."
        );
        connector.sweep(symbols, SweepReason::ConnectorStopping, sweep_tx)
    } else {
        drop(sweep_tx);
        None
    };

    // **Waited for, not merely started.** The publish task is still in its steady-state loop
    // while this runs, so the cancels it confirms reach whichever bots are still listening;
    // and nothing is counting down a deadline that was never meant to bound a sweep. A stop
    // that gives up here has left orders resting, which is the failure the sweep exists to
    // prevent, so it is loud and it is not exit 0.
    let sweep_finished = match sweep {
        Some(handle) => match tokio::time::timeout(sweep_timeout, handle).await {
            Ok(Ok(())) => true,
            Ok(Err(error)) => {
                error!(?error, "The shutdown sweep task failed.");
                false
            }
            Err(_) => {
                error!(
                    ?sweep_timeout,
                    "The shutdown sweep did not finish in time. Orders may still be resting \
                     on the venue with nothing managing them — check the venue before \
                     restarting, and raise `shutdown.sweep_timeout_ms` if this is normal for \
                     this deployment."
                );
                false
            }
        },
        None => true,
    };

    // After the sweep, so a sweep in flight is not aborted along with the streams.
    connector.shutdown();
    // ...and only now does the publish task stop serving. Every sender that could still have
    // something to say has said it.
    shutdown_signal_for_drain.notify_one();

    // The publish task holds the receiver until this returns, and `exit` does not run its
    // destructor, so nothing in the process can meet a closed channel.
    // Bounded. The drain has its own deadline, so this only fires if the publish task is
    // wedged somewhere it should not be — and a connector that hangs on SIGTERM is worse
    // than either exit code, because the supervisor ends up sending SIGKILL and learns
    // nothing at all.
    let drain = match tokio::time::timeout(grace + Duration::from_secs(2), drained_rx).await {
        Ok(Ok(outcome)) => Some(outcome),
        // The publish task went away without reporting. It cannot do so on the orderly path,
        // so this means it failed — and it will already have panicked into exit(1) under the
        // hook. Reaching here at all means the race went the other way; agreeing with it is
        // what stops the race from deciding what the supervisor is told.
        Ok(Err(_)) => {
            error!("The publish task ended without reporting a drain outcome.");
            None
        }
        Err(_) => {
            error!(
                ?grace,
                "The publish task did not finish draining in time; stopping without it."
            );
            None
        }
    };

    let report = StopReport {
        kind,
        sweep_finished,
        drain,
    };
    let code = exit_code(&report);
    info!(?report, code, "Connector stopped.");
    exit(code);
}
