//! Backend-agnostic supervision: the orderly stop, and bot-liveness.
//!
//! Both mechanisms here exist because of one measured session (design note §11.14):
//!
//! 1. **A clean SIGINT always ended in a panic and `exit(1)`**, so a supervisor could not
//!    tell a stop from a crash. The cause is an ordering bug, not the `unwrap`s: the publish
//!    task returned on the shutdown signal, its receiver dropped, and the next
//!    `pub_tx.send(..).unwrap()` from a still-running stream task panicked. The fix is
//!    [`drain_publish`] — the publish task stops *last*, after every sender is gone — which
//!    leaves `AGENTS.md` §4.7's supervisor contract (an *unexpected* publish-task death
//!    still panics into `exit(1)`) exactly as it was.
//! 2. **Orders outlived the bot and kept trading.** A fill landed 3 s after the bot's
//!    SIGINT and moved the position. Between a bot's death and its supervisor's restart the
//!    venue hosts an unattended market maker. [`BotRegistry`] is how the connector notices.

use std::{
    collections::{BTreeSet, HashMap},
    time::{Duration, Instant},
};

use hftbacktest::live::ipc::iceoryx::ChannelError;
use serde::Deserialize;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::connector::{PublishEvent, SweepOutcome};

/// How long the publish task waits for every [`PublishEvent`] sender to be dropped before it
/// stops anyway.
///
/// Only the Hyperliquid backend winds its stream tasks down (`Connector::shutdown`); the
/// others leave theirs running, so on those the drain always runs to this deadline. That is
/// the reason it is a deadline and not a wait: a backend that never drops its senders must
/// still produce `exit(0)`, or the whole fix is worth nothing on three of four backends.
///
/// **It does not bound the sweep** — [`ShutdownConfig::sweep_timeout_ms`] does. The drain
/// does not begin until the sweep has finished, so five seconds here is what a stop spends
/// waiting for stream tasks that are never going to let go, and nothing else. It used to
/// bound both, which put a 5 s ceiling on a sweep whose per-symbol allowance is 20 s: a stop
/// could exit 0 having cancelled half a grid.
const DEFAULT_SHUTDOWN_GRACE_MS: u64 = 5_000;

/// How long an orderly stop may spend cancelling before it gives up on the sweep and says so.
///
/// The sweep is sequential over symbols and each one is several venue round trips — the
/// Hyperliquid backend allows itself up to 20 s per symbol — so this is sized for a handful
/// of symbols at ordinary latency, not for the worst case of many. It exists to stop a
/// connector hanging on `SIGTERM` against an unreachable venue: whatever it did not finish
/// is reported and the process exits **non-zero**, because a stop that left orders resting
/// is not a clean stop.
const DEFAULT_SWEEP_TIMEOUT_MS: u64 = 30_000;

/// Silence after which a bot **that has been heartbeating** is presumed dead.
///
/// What the window has to clear is the bot's *heartbeat interval*, not the duration of its
/// `elapse` calls. Those are different numbers, and the difference is the whole hazard: the
/// heartbeat is sent from inside `elapse`, and a bot that receives nothing at all — a stalled
/// public feed (`AGENTS.md` §4.2), or `wait_order_response`, which hardcodes 60 s — spends
/// that time inside a single receive. `LiveBot` caps each receive at its heartbeat interval
/// so that it keeps reporting throughout; against the default of one second, 10 s here is an
/// order of magnitude of headroom.
const DEFAULT_BOT_TIMEOUT_MS: u64 = 10_000;

/// How often the receive loop asks whether a bot has gone quiet.
///
/// The loop itself cycles at 1 µs; asking a `HashMap` that often would burn a core to
/// answer "no" a million times a second.
pub const LIVENESS_POLL_INTERVAL: Duration = Duration::from_millis(250);

fn default_true() -> bool {
    true
}

fn default_grace_ms() -> u64 {
    DEFAULT_SHUTDOWN_GRACE_MS
}

fn default_bot_timeout_ms() -> u64 {
    DEFAULT_BOT_TIMEOUT_MS
}

fn default_sweep_timeout_ms() -> u64 {
    DEFAULT_SWEEP_TIMEOUT_MS
}

/// The supervision settings, read from the **same** config file as the backend's own.
///
/// Deliberately parsed separately rather than added to each backend's `Config`: the policy
/// is generic, `main.rs` owns it, and a backend that has not thought about it should not be
/// able to omit it. Serde ignores the backend's own keys, and the backend's parser ignores
/// these, so one file serves both.
#[derive(Deserialize, Debug, Default)]
pub struct SupervisionConfig {
    #[serde(default)]
    pub shutdown: ShutdownConfig,
    #[serde(default)]
    pub bot_liveness: BotLivenessConfig,
}

#[derive(Deserialize, Debug)]
pub struct ShutdownConfig {
    /// Cancel the venue orders of every instrument a bot registered, on an orderly stop.
    ///
    /// **Defaults to on, and that is the fail-closed choice**: a connector that is stopping
    /// cannot manage a resting order, so leaving one behind means an unattended maker for
    /// however long the restart takes. Turning it off is supported — a deployment that
    /// restarts the connector under a live bot may prefer the orders to survive — but it is
    /// a decision, not a default.
    #[serde(default = "default_true")]
    pub sweep_orders: bool,
    /// How long to wait for the backends' senders to go away before stopping anyway.
    #[serde(default = "default_grace_ms")]
    pub grace_ms: u64,
    /// How long the stop may spend cancelling before it gives up on the sweep.
    #[serde(default = "default_sweep_timeout_ms")]
    pub sweep_timeout_ms: u64,
}

impl Default for ShutdownConfig {
    fn default() -> Self {
        Self {
            sweep_orders: true,
            grace_ms: DEFAULT_SHUTDOWN_GRACE_MS,
            sweep_timeout_ms: DEFAULT_SWEEP_TIMEOUT_MS,
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct BotLivenessConfig {
    /// Watch for bots that stop heartbeating, and sweep their instruments when they do.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Silence, in milliseconds, after which a heartbeating bot is presumed dead.
    #[serde(default = "default_bot_timeout_ms")]
    pub timeout_ms: u64,
}

impl Default for BotLivenessConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            timeout_ms: DEFAULT_BOT_TIMEOUT_MS,
        }
    }
}

impl BotLivenessConfig {
    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms)
    }
}

impl ShutdownConfig {
    pub fn grace(&self) -> Duration {
        Duration::from_millis(self.grace_ms)
    }

    pub fn sweep_timeout(&self) -> Duration {
        Duration::from_millis(self.sweep_timeout_ms)
    }
}

/// How long after a dead bot has been swept its registry entry is kept.
///
/// It is kept at all so that a bot declared dead by a window too short for it is recognised
/// when it speaks again ("presumed dead is heartbeating again"), which is the log line that
/// tells an operator to raise `timeout_ms`. After that the entry is only ballast: a
/// restarted bot mints a fresh random id, so a supervisor restart loop would otherwise add
/// one permanent entry — and one permanent `registered_symbols()` contribution — per
/// restart, for the life of the connector.
const DEAD_BOT_RETENTION: Duration = Duration::from_secs(300);

/// What a bot registered, and when it was last heard from.
#[derive(Debug)]
struct BotState {
    /// Ordered so that a sweep and its log line are reproducible.
    symbols: BTreeSet<String>,
    /// `None` until the bot's **first** heartbeat.
    ///
    /// Liveness is armed by the bot, never assumed, and that is what makes this safe to
    /// deploy against a fleet that has not been updated together: a bot built before
    /// `LiveRequest::Heartbeat` existed sends none, is never armed, and is therefore never
    /// swept. The alternative — arming on registration — would cancel the live orders of
    /// every healthy old bot the moment the connector was upgraded.
    last_seen: Option<Instant>,
    /// Whether the death sweep has already fired for the current silence, so it fires once
    /// rather than every poll.
    swept: bool,
}

/// A bot that stopped heartbeating, and what may be done about it.
///
/// The split is the whole point. A sweep reaches the **account's** orders on a coin, not one
/// bot's — nothing on the venue records which bot asked for an order, because
/// `Connector::submit` is never told — so on a coin two bots are quoting, "cancel the dead
/// one's orders" is not an operation that exists. Only the coins this bot alone registered
/// can be cleared; the rest are named so the operator learns that the venue is holding
/// orders nobody is managing, and why they were left.
#[derive(Debug, PartialEq, Eq)]
pub struct DeadBot {
    pub id: u64,
    /// Registered by this bot and no live one. Safe to cancel on its behalf.
    pub symbols: Vec<String>,
    /// Also registered by a bot that is still heartbeating. Deliberately left alone: they
    /// are swept when the last live bot on them goes, or when the connector stops.
    pub shared: Vec<String>,
}

/// Which bots are attached, what each registered, and which have gone quiet.
///
/// Keyed by the bot id that rides in the iceoryx user header — the same `u64` the bot
/// generates for itself (`hftbacktest/src/live/bot.rs`) and stamps on every request. That
/// key is the reason this is an application-level heartbeat rather than iceoryx node
/// liveness: see the module docs of `main.rs` and design note §12.2.
#[derive(Debug, Default)]
pub struct BotRegistry {
    bots: HashMap<u64, BotState>,
}

impl BotRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records that `id` registered `symbol`. Does **not** arm liveness.
    pub fn register(&mut self, id: u64, symbol: String) {
        self.bots
            .entry(id)
            .or_insert_with(|| BotState {
                symbols: BTreeSet::new(),
                last_seen: None,
                swept: false,
            })
            .symbols
            .insert(symbol);
    }

    /// Records a heartbeat, arming liveness for `id` if this is its first.
    ///
    /// Returns `true` if this heartbeat cleared a death mark — that is, the bot is back.
    /// A restarted bot mints a fresh id, so in practice this fires when a bot was declared
    /// dead by a window that was simply too short for it.
    pub fn heartbeat(&mut self, id: u64, now: Instant) -> bool {
        let state = self.bots.entry(id).or_insert_with(|| BotState {
            symbols: BTreeSet::new(),
            last_seen: None,
            swept: false,
        });
        state.last_seen = Some(now);
        let was_swept = state.swept;
        state.swept = false;
        was_swept
    }

    /// Whether `state` has armed liveness and then gone quiet for longer than `timeout`.
    fn is_silent(state: &BotState, now: Instant, timeout: Duration) -> bool {
        state
            .last_seen
            .is_some_and(|seen| now.saturating_duration_since(seen) > timeout)
    }

    /// The bots that armed liveness, have been silent for longer than `timeout`, and have
    /// not already been swept for this silence — together with what each registered.
    ///
    /// Marks them, so a bot that stays dead is swept once and not on every poll, and forgets
    /// the ones that have been dead for [`DEAD_BOT_RETENTION`] past their window.
    pub fn take_dead(&mut self, now: Instant, timeout: Duration) -> Vec<DeadBot> {
        // Every symbol some *other* bot that is still alive registered. A venue cancel is
        // account-wide for a coin — `cancels_for` in the Hyperliquid backend filters the
        // account's open orders by coin, and it cannot do better, because no venue order
        // carries the id of the bot that asked for it. So cancelling "the dead bot's BTC"
        // cancels the live bot's BTC too, and that is a healthy market maker flattened.
        let live: BTreeSet<String> = self
            .bots
            .values()
            .filter(|state| !Self::is_silent(state, now, timeout))
            .flat_map(|state| state.symbols.iter().cloned())
            .collect();

        let mut dead: Vec<DeadBot> = self
            .bots
            .iter_mut()
            .filter(|(_, state)| !state.swept && !state.symbols.is_empty())
            .filter(|(_, state)| Self::is_silent(state, now, timeout))
            .map(|(id, state)| {
                state.swept = true;
                let (shared, symbols) = state
                    .symbols
                    .iter()
                    .cloned()
                    .partition(|symbol| live.contains(symbol));
                DeadBot {
                    id: *id,
                    symbols,
                    shared,
                }
            })
            .collect();
        // `HashMap` iteration order is not stable, and a sweep's log line should be.
        dead.sort_by_key(|bot| bot.id);

        // Nothing here is due again: a restarted bot arrives under a new id.
        self.bots.retain(|_, state| {
            !(state.swept && Self::is_silent(state, now, timeout + DEAD_BOT_RETENTION))
        });
        dead
    }

    /// Every symbol any bot registered, for the sweep an orderly stop performs.
    pub fn registered_symbols(&self) -> Vec<String> {
        let symbols: BTreeSet<&String> = self
            .bots
            .values()
            .flat_map(|state| state.symbols.iter())
            .collect();
        symbols.into_iter().cloned().collect()
    }

    /// How many bots have armed liveness. For the log line at shutdown.
    pub fn watched(&self) -> usize {
        self.bots
            .values()
            .filter(|state| state.last_seen.is_some())
            .count()
    }
}

/// How the orderly drain ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DrainOutcome {
    /// Every sender was dropped. Nothing can send again, so nothing can panic: this is the
    /// outcome that makes `exit(0)` honest.
    SendersDropped { published: usize },
    /// The grace ran out with senders still alive — the backend does not wind its tasks
    /// down. Stopping anyway is safe **only** because `main` calls `exit(0)` without ever
    /// dropping the receiver.
    GraceElapsed { published: usize },
    /// Publishing failed while draining. Not fatal: the connector is already stopping, and
    /// turning a stop into `exit(1)` is the bug this module exists to remove.
    PublishFailed { published: usize },
}

/// What ended the connector.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StopKind {
    /// A signal was received and the stop ran in order.
    Orderly,
    /// The receive loop ended on its own — the iceoryx node failed. The connector can no
    /// longer serve any bot, so it still sweeps and still stops, but it stops as a failure.
    ReceiveLoopFailed,
}

/// Everything the exit code has to answer for.
#[derive(Clone, Debug)]
pub struct StopReport {
    pub kind: StopKind,
    /// What the shutdown sweep did (SW1), or `None` if none ran — sweeping was disabled, no
    /// symbols were registered, or the backend has no order path. Only [`SweepOutcome::Failed`]
    /// is a non-zero stop; [`SweepOutcome::Cancelled`], [`SweepOutcome::NotImplemented`] and
    /// `None` are all clean.
    pub sweep: Option<SweepOutcome>,
    /// How the drain ended, or `None` if the publish task went away without reporting.
    pub drain: Option<DrainOutcome>,
}

/// The process exit code a stop deserves.
///
/// `AGENTS.md` §4.7: the exit code is the supervisor's only channel, and this decides what it
/// is told. `0` means **and only means** "asked to stop, and everything it promised to do
/// before stopping was done".
///
/// The two subtle ones:
///
/// * A publish task that vanished without reporting has failed — it cannot do that on the
///   orderly path. It will already have panicked into `exit(1)` under the process hook, and
///   the two are racing; agreeing with it is what stops the race from deciding whether the
///   supervisor sees a crash or a clean stop.
/// * A sweep that **failed** ([`SweepOutcome::Failed`]) may have left orders resting on the
///   venue with nothing managing them. That is the exact condition this whole module exists to
///   prevent, so it cannot be reported as success however orderly the rest of the stop was. A
///   sweep that confirmed ([`SweepOutcome::Cancelled`]), a documented no-op backend
///   ([`SweepOutcome::NotImplemented`], §4.7) and no sweep at all (`None`) are all clean — the
///   distinction a bare `JoinHandle<()>` could not make, which let a stop exit 0 with a grid
///   still resting (SW1).
///
/// [`DrainOutcome::GraceElapsed`] is *not* a failure: three of the four backends never drop
/// their stream senders, so it is their normal ending.
pub fn exit_code(report: &StopReport) -> i32 {
    // Classify the sweep first, **exhaustively on [`SweepOutcome`] with no wildcard** (SW1,
    // `AGENTS.md` §1.1 fail closed): a variant added later does not compile until it is ruled
    // clean or dirty here, so it cannot silently inherit a clean exit the way a `_ => 0`
    // fall-through would let it. Only a sweep that may have left orders resting is dirty.
    let sweep_left_orders_resting = match report.sweep {
        None | Some(SweepOutcome::Cancelled) | Some(SweepOutcome::NotImplemented) => false,
        Some(SweepOutcome::Failed) => true,
    };
    match report {
        StopReport {
            kind: StopKind::ReceiveLoopFailed,
            ..
        } => 1,
        _ if sweep_left_orders_resting => 1,
        StopReport { drain: None, .. } => 1,
        StopReport {
            drain: Some(DrainOutcome::PublishFailed { .. }),
            ..
        } => 1,
        _ => 0,
    }
}

/// Whether a queued [`PublishEvent`] may still be sent to the bots once the connector has
/// begun stopping.
///
/// Everything is, except a registration. A registration is not a report — it ends in
/// [`hftbacktest::types::LiveEvent::SnapshotComplete`], which tells the bot its state is
/// settled and it may submit orders (`docs/snapshot-complete-marker.md`). By the time this
/// question is asked the receive task has stopped reading requests, so an order sent in
/// answer to that promise is never seen, never rejected, and never responded to: the bot
/// waits for a reply that cannot come, on the one path where its state is supposed to be
/// certain. Cancel confirmations from the sweep, positions and fills are all facts and go
/// out as usual.
pub fn publishable_while_stopping(event: &PublishEvent) -> bool {
    !matches!(event, PublishEvent::RegisterInstrument { .. })
}

/// Drains everything still in flight, then waits for the senders to go away.
///
/// This is the whole of the shutdown fix. The publish task must be the **last** thing to
/// stop, because every other task in the connector sends to it with `.unwrap()`; while this
/// is running they can all still send, and when it returns with
/// [`DrainOutcome::SendersDropped`] there is provably nobody left who could.
///
/// `publish` is the same handler the steady-state loop uses, so a cancel confirmed by the
/// venue during the shutdown sweep still reaches whichever bots are still listening.
pub async fn drain_publish<F>(
    rx: &mut UnboundedReceiver<PublishEvent>,
    grace: Duration,
    mut publish: F,
) -> DrainOutcome
where
    F: FnMut(PublishEvent) -> Result<(), ChannelError>,
{
    let deadline = Instant::now() + grace;
    let mut published = 0usize;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return DrainOutcome::GraceElapsed { published };
        }
        match tokio::time::timeout(remaining, rx.recv()).await {
            // Every sender is gone: this is the good ending.
            Ok(None) => return DrainOutcome::SendersDropped { published },
            Ok(Some(msg)) => {
                if publish(msg).is_err() {
                    return DrainOutcome::PublishFailed { published };
                }
                published += 1;
            }
            Err(_elapsed) => return DrainOutcome::GraceElapsed { published },
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc::unbounded_channel;

    use super::*;
    use crate::connector::PublishEvent;

    fn at(base: Instant, ms: u64) -> Instant {
        base + Duration::from_millis(ms)
    }

    /// **A bot that never heartbeats is never swept.** The whole feature has to be safe to
    /// roll out against a connector serving a bot built before `LiveRequest::Heartbeat`
    /// existed; arming on registration instead would cancel every healthy old bot's live
    /// orders the moment the connector was upgraded. Liveness is armed by the bot.
    #[test]
    fn a_bot_that_never_heartbeats_is_never_declared_dead() {
        let t0 = Instant::now();
        let mut registry = BotRegistry::new();
        registry.register(1, "BTC".into());

        assert!(
            registry
                .take_dead(at(t0, 600_000), Duration::from_secs(10))
                .is_empty(),
            "a bot that never armed liveness must not be swept, however long it is quiet"
        );
        assert_eq!(registry.watched(), 0);
    }

    /// The measured incident: the bot dies, its orders stay, and a fill lands afterwards.
    /// After the grace window the connector must name the dead bot and hand back exactly
    /// what that bot registered.
    #[test]
    fn a_heartbeating_bot_that_goes_silent_is_swept_after_the_grace_window() {
        let t0 = Instant::now();
        let mut registry = BotRegistry::new();
        registry.register(7, "BTC".into());
        registry.register(7, "ETH".into());
        registry.heartbeat(7, t0);
        assert_eq!(registry.watched(), 1);

        // Inside the window: still alive.
        assert!(
            registry
                .take_dead(at(t0, 9_999), Duration::from_secs(10))
                .is_empty()
        );
        // Exactly at the window: not yet — the comparison is strictly greater.
        assert!(
            registry
                .take_dead(at(t0, 10_000), Duration::from_secs(10))
                .is_empty()
        );

        let dead = registry.take_dead(at(t0, 10_001), Duration::from_secs(10));
        assert_eq!(
            dead,
            vec![DeadBot {
                id: 7,
                symbols: vec!["BTC".to_string(), "ETH".to_string()],
                shared: vec![],
            }]
        );
    }

    /// A bot that stays dead must be swept **once**. Sweeping every poll would spend the
    /// venue's rate-limit budget for ever and fill the log with the same line.
    #[test]
    fn a_dead_bot_is_swept_once_not_on_every_poll() {
        let t0 = Instant::now();
        let mut registry = BotRegistry::new();
        registry.register(7, "BTC".into());
        registry.heartbeat(7, t0);

        assert_eq!(
            registry
                .take_dead(at(t0, 11_000), Duration::from_secs(10))
                .len(),
            1
        );
        assert!(
            registry
                .take_dead(at(t0, 12_000), Duration::from_secs(10))
                .is_empty()
        );
        assert!(
            registry
                .take_dead(at(t0, 60_000), Duration::from_secs(10))
                .is_empty()
        );
    }

    /// **A heartbeat cancels the timer.** A bot that was declared dead by a window too
    /// short for it — a long `elapse`, a stalled feed — comes back, and must be watched
    /// again rather than left unwatched for the rest of the run.
    #[test]
    fn a_heartbeat_rearms_a_bot_that_was_declared_dead() {
        let t0 = Instant::now();
        let mut registry = BotRegistry::new();
        registry.register(7, "BTC".into());
        registry.heartbeat(7, t0);
        assert_eq!(
            registry
                .take_dead(at(t0, 11_000), Duration::from_secs(10))
                .len(),
            1
        );

        // It is back.
        assert!(
            registry.heartbeat(7, at(t0, 12_000)),
            "a heartbeat after a sweep must report that it cleared the death mark"
        );
        assert!(
            registry
                .take_dead(at(t0, 20_000), Duration::from_secs(10))
                .is_empty(),
            "the timer restarts from the new heartbeat"
        );
        // ...and it can die again.
        assert_eq!(
            registry
                .take_dead(at(t0, 23_000), Duration::from_secs(10))
                .len(),
            1
        );
    }

    /// A heartbeat from a bot that has registered nothing yet arms it but sweeps nothing —
    /// there is no instrument to cancel on, and an empty sweep is a wasted REST round trip
    /// with a misleading log line.
    #[test]
    fn a_bot_with_no_registered_instrument_is_not_swept() {
        let t0 = Instant::now();
        let mut registry = BotRegistry::new();
        registry.heartbeat(9, t0);

        assert!(
            registry
                .take_dead(at(t0, 60_000), Duration::from_secs(10))
                .is_empty()
        );
        assert!(registry.registered_symbols().is_empty());
    }

    /// Several bots on one connector, on **different** instruments: only the dead one's are
    /// swept. This is the property iceoryx node liveness could not provide — a `NodeId`
    /// cannot be mapped to a bot id — and the reason the heartbeat carries the bot's own key.
    #[test]
    fn only_the_dead_bots_instruments_are_swept() {
        let t0 = Instant::now();
        let mut registry = BotRegistry::new();
        registry.register(1, "BTC".into());
        registry.register(2, "ETH".into());
        registry.heartbeat(1, t0);
        registry.heartbeat(2, t0);
        // Bot 2 keeps living.
        registry.heartbeat(2, at(t0, 10_500));

        let dead = registry.take_dead(at(t0, 11_000), Duration::from_secs(10));
        assert_eq!(
            dead,
            vec![DeadBot {
                id: 1,
                symbols: vec!["BTC".to_string()],
                shared: vec![],
            }]
        );
        assert_eq!(
            registry.registered_symbols(),
            vec!["BTC".to_string(), "ETH".to_string()],
            "an orderly stop still sweeps both"
        );
    }

    /// **A coin a live bot is quoting is never swept for a dead one.** The sweep is
    /// account-wide per coin — no venue order records which bot placed it, because
    /// `Connector::submit` is never told — so cancelling bot 1's BTC cancels bot 2's BTC
    /// with it. Two bots on one connector is the topology this whole mechanism was chosen
    /// for (design note §12.2), and flattening a healthy market maker to tidy up after a
    /// dead one is a worse outcome than the orders being left, which is at least visible.
    ///
    /// The same reasoning is already written into the Hyperliquid registration path
    /// (`private_stream.rs`, `on_registration`), which refuses to sweep every registered
    /// coin for exactly this reason.
    #[test]
    fn a_coin_a_live_bot_is_still_quoting_is_not_swept_for_a_dead_one() {
        let t0 = Instant::now();
        let mut registry = BotRegistry::new();
        registry.register(1, "BTC".into());
        registry.register(1, "SOL".into());
        registry.register(2, "BTC".into());
        registry.heartbeat(1, t0);
        registry.heartbeat(2, t0);
        // Bot 2 is alive and quoting BTC.
        registry.heartbeat(2, at(t0, 10_500));

        let dead = registry.take_dead(at(t0, 11_000), Duration::from_secs(10));
        assert_eq!(
            dead,
            vec![DeadBot {
                id: 1,
                // SOL is bot 1's alone, so it is cleared.
                symbols: vec!["SOL".to_string()],
                // BTC is not: bot 2's resting grid is on it.
                shared: vec!["BTC".to_string()],
            }],
            "a live bot's coin must never be handed to a sweep"
        );
    }

    /// ...and the contended coin is cleared as soon as the last bot on it goes, rather than
    /// being abandoned because it was shared once. Otherwise a coin two bots quoted would
    /// keep its orders resting for ever after both died.
    #[test]
    fn a_shared_coin_is_swept_once_the_last_bot_on_it_dies() {
        let t0 = Instant::now();
        let mut registry = BotRegistry::new();
        registry.register(1, "BTC".into());
        registry.register(2, "BTC".into());
        registry.heartbeat(1, t0);
        registry.heartbeat(2, t0);
        registry.heartbeat(2, at(t0, 10_500));

        // Bot 1 dies first: BTC is bot 2's too, so nothing is cancelled.
        let dead = registry.take_dead(at(t0, 11_000), Duration::from_secs(10));
        assert_eq!(dead[0].symbols, Vec::<String>::new());
        assert_eq!(dead[0].shared, vec!["BTC".to_string()]);

        // Then bot 2 dies. Nobody is left on BTC, and one account-wide cancel clears what
        // both of them left.
        let dead = registry.take_dead(at(t0, 21_000), Duration::from_secs(10));
        assert_eq!(
            dead,
            vec![DeadBot {
                id: 2,
                symbols: vec!["BTC".to_string()],
                shared: vec![],
            }]
        );
    }

    /// A bot that has registered but never heartbeated counts as **alive** for this: it is a
    /// bot too old to know the variant, quoting happily, and it is never swept itself
    /// (`a_bot_that_never_heartbeats_is_never_declared_dead`). Treating it as absent would
    /// let a newer bot's death cancel its orders — the upgrade-day failure the arming rule
    /// exists to prevent, reached by a different route.
    #[test]
    fn a_bot_that_never_heartbeats_still_protects_its_coins() {
        let t0 = Instant::now();
        let mut registry = BotRegistry::new();
        registry.register(1, "BTC".into());
        registry.register(2, "BTC".into());
        // Only bot 1 arms liveness.
        registry.heartbeat(1, t0);

        let dead = registry.take_dead(at(t0, 11_000), Duration::from_secs(10));
        assert_eq!(dead[0].id, 1);
        assert_eq!(dead[0].symbols, Vec::<String>::new());
        assert_eq!(dead[0].shared, vec!["BTC".to_string()]);
    }

    /// A crash-looping bot must not grow the registry for ever. Ids are freshly minted
    /// random `u64`s per process, so every restart is a new entry; without eviction a day of
    /// restarts leaves tens of thousands of them, each one polled every 250 ms on the
    /// receive loop's hot path and each one still contributing its symbols to the shutdown
    /// sweep's log line.
    #[test]
    fn a_bot_that_has_been_dead_for_a_long_time_is_forgotten() {
        let t0 = Instant::now();
        let mut registry = BotRegistry::new();
        registry.register(1, "BTC".into());
        registry.heartbeat(1, t0);
        let timeout = Duration::from_secs(10);

        assert_eq!(registry.take_dead(at(t0, 11_000), timeout).len(), 1);
        // Still remembered a moment later: this is the window in which a bot declared dead
        // by too tight a setting can come back and say so.
        assert_eq!(registry.watched(), 1);
        assert_eq!(registry.registered_symbols(), vec!["BTC".to_string()]);

        // Long past the retention, it is nobody's bot any more.
        assert!(registry.take_dead(at(t0, 400_000), timeout).is_empty());
        assert_eq!(registry.watched(), 0);
        assert!(registry.registered_symbols().is_empty());
    }

    /// The drain's good ending: everything queued is published, and the task only stops once
    /// the last sender is gone. That is what makes a send-after-close impossible on the
    /// orderly path — the bug that turned every SIGINT into `exit(1)`.
    #[tokio::test]
    async fn the_drain_publishes_what_is_queued_and_stops_when_the_senders_go() {
        let (tx, mut rx) = unbounded_channel();
        tx.send(PublishEvent::BatchStart(1)).unwrap();
        tx.send(PublishEvent::BatchEnd(1)).unwrap();
        let second = tx.clone();
        drop(tx);

        let mut seen = 0;
        // One sender still alive: the drain must not finish.
        let outcome = drain_publish(&mut rx, Duration::from_millis(50), |_| {
            seen += 1;
            Ok(())
        })
        .await;
        assert_eq!(outcome, DrainOutcome::GraceElapsed { published: 2 });
        assert_eq!(seen, 2);

        drop(second);
        let outcome = drain_publish(&mut rx, Duration::from_millis(50), |_| Ok(())).await;
        assert_eq!(outcome, DrainOutcome::SendersDropped { published: 0 });
    }

    /// A backend that does not wind its stream tasks down must still produce a stop. Three
    /// of the four backends are in exactly that position, so a drain that waited for ever
    /// would fix the panic on one connector and hang on the others.
    #[tokio::test]
    async fn the_drain_gives_up_on_a_backend_that_never_drops_its_sender() {
        let (_tx, mut rx) = unbounded_channel::<PublishEvent>();
        let outcome = drain_publish(&mut rx, Duration::from_millis(20), |_| Ok(())).await;
        assert_eq!(outcome, DrainOutcome::GraceElapsed { published: 0 });
    }

    /// A publish failure while stopping is reported, not fatal. Panicking here would put
    /// `exit(1)` back on the clean-stop path, which is the whole point of this module.
    #[tokio::test]
    async fn a_publish_failure_while_draining_ends_the_drain_without_killing_the_process() {
        let (tx, mut rx) = unbounded_channel();
        tx.send(PublishEvent::BatchStart(1)).unwrap();
        tx.send(PublishEvent::BatchEnd(1)).unwrap();
        drop(tx);

        let outcome = drain_publish(&mut rx, Duration::from_millis(50), |_| {
            Err(ChannelError::BuildError("nobody is listening".into()))
        })
        .await;
        assert_eq!(outcome, DrainOutcome::PublishFailed { published: 0 });
    }

    /// **The property the whole shutdown fix rests on: a send during the drain is
    /// published, never refused.** This is the shape of the real stop — the receive task has
    /// dropped its sender, a stream task or the shutdown sweep is still running and still
    /// sending, and the publish task must keep serving them until they are done rather than
    /// leaving them pointing at a closed channel.
    #[tokio::test]
    async fn a_sender_still_running_can_publish_all_the_way_through_the_drain() {
        let (receive_task_tx, mut rx) = unbounded_channel();
        let sweep_tx = receive_task_tx.clone();

        // The receive task returns first, exactly as `main` orders it.
        drop(receive_task_tx);

        // The sweep is still going, and reports as it goes.
        let sweeper = tokio::spawn(async move {
            for _ in 0..5 {
                tokio::time::sleep(Duration::from_millis(2)).await;
                // If the drain had already stopped, this `send` would fail — and in the real
                // connector it is an `.unwrap()`, which is precisely the panic that turned
                // every clean SIGINT into exit(1).
                sweep_tx
                    .send(PublishEvent::BatchEnd(1))
                    .expect("a sender that is still running must never meet a closed channel");
            }
            // ...and only now does the last sender go.
        });

        let outcome = drain_publish(&mut rx, Duration::from_secs(5), |_| Ok(())).await;
        sweeper.await.unwrap();

        assert_eq!(
            outcome,
            DrainOutcome::SendersDropped { published: 5 },
            "the drain must outlive every sender and publish everything they sent"
        );
    }

    /// **The supervisor contract, pinned — on a real send site.** `AGENTS.md` §4.7:
    /// `pub_tx.send(..).unwrap()` appears all over this crate on purpose — an *unexpected*
    /// death of the publish task must kill the process, because a connector whose bots have
    /// stopped hearing from it is worse than no connector. The shutdown fix works by
    /// ordering and deliberately did not soften a single one of those `unwrap`s.
    ///
    /// It drives production code on purpose. The version this replaces built its own channel
    /// and unwrapped its own send, which asserted a property of `tokio` and would have stayed
    /// green if every `send(..).unwrap()` in the crate had been rewritten as `let _ = send`
    /// — the one change it was documented, in three places, as preventing.
    #[cfg(feature = "hyperliquid")]
    #[test]
    #[should_panic(expected = "SendError")]
    fn an_unexpected_publish_task_death_still_panics_a_production_sender() {
        use hftbacktest::types::{OrdType, Order, Side, TimeInForce};

        use crate::hyperliquid::{HyperliquidError, private_stream::expire_and_report};

        let (tx, rx) = unbounded_channel();
        // The publish task dies without anyone asking — the receiver goes away.
        drop(rx);
        // A real order-path send site, reached the way the connector reaches it.
        expire_and_report(
            "BTC",
            Order::new(
                1,
                100,
                0.1,
                1.0,
                Side::Buy,
                OrdType::Limit,
                TimeInForce::GTC,
            ),
            &HyperliquidError::InvalidOrder("the publish task is gone".into()),
            &tx,
        );
    }

    /// **`exit(0)` means the stop did what it promised.** A supervisor has one bit of
    /// information about how a process ended, and before this every orderly ending produced
    /// `0` — including a publish task that died mid-stop and a sweep that never finished.
    #[test]
    fn only_a_stop_that_kept_its_promises_exits_zero() {
        let clean = StopReport {
            kind: StopKind::Orderly,
            sweep: Some(SweepOutcome::Cancelled),
            drain: Some(DrainOutcome::SendersDropped { published: 3 }),
        };
        assert_eq!(exit_code(&clean), 0);

        // The normal ending on the three backends that never wind their streams down.
        assert_eq!(
            exit_code(&StopReport {
                drain: Some(DrainOutcome::GraceElapsed { published: 0 }),
                ..clean.clone()
            }),
            0,
            "a backend that does not implement `shutdown` still stops cleanly"
        );

        // **A sweep that ran but could not confirm its cancels may have left orders resting.**
        // This is the exact failure a bare `JoinHandle<()>` could not report — the task
        // finished, so the old `sweep_finished` bool was `true` and the stop exited 0 — and it
        // is the whole reason the outcome is now a `SweepOutcome` (SW1).
        assert_eq!(
            exit_code(&StopReport {
                sweep: Some(SweepOutcome::Failed),
                ..clean.clone()
            }),
            1,
            "a sweep that failed to confirm its cancels must not be reported as a clean stop"
        );
        // A documented no-op backend (bybit/binance, §4.7) never claimed to sweep, so its
        // normal stop is clean; turning it into exit 1 would make every stop look broken.
        assert_eq!(
            exit_code(&StopReport {
                sweep: Some(SweepOutcome::NotImplemented),
                ..clean.clone()
            }),
            0,
            "an unimplemented sweep is a documented gap, not a failure"
        );
        // No sweep ran at all — disabled, nothing registered, or a backend with no order path.
        assert_eq!(
            exit_code(&StopReport {
                sweep: None,
                ..clean.clone()
            }),
            0,
            "no sweep to run is not a failed sweep"
        );
        // The publish task failed while stopping: the bots did not get everything.
        assert_eq!(
            exit_code(&StopReport {
                drain: Some(DrainOutcome::PublishFailed { published: 1 }),
                ..clean.clone()
            }),
            1
        );
        // It went away without reporting at all — it cannot do that on the orderly path, so
        // it failed, and it is racing its own panic hook's `exit(1)`. Agreeing with the hook
        // is what keeps the race from deciding what the supervisor is told.
        assert_eq!(
            exit_code(&StopReport {
                drain: None,
                ..clean.clone()
            }),
            1
        );
        // And a receive loop that ended on its own is never a clean stop.
        assert_eq!(
            exit_code(&StopReport {
                kind: StopKind::ReceiveLoopFailed,
                ..clean
            }),
            1
        );
    }

    /// **A stopping connector must not promise a bot that it may trade.** Everything else
    /// queued is a fact and still goes out; a registration ends in `SnapshotComplete`, which
    /// says "your state is settled, submit away" — to a connector that has already stopped
    /// reading requests and is about to exit.
    #[test]
    fn a_stopping_connector_answers_facts_but_not_registrations() {
        assert!(!publishable_while_stopping(
            &PublishEvent::RegisterInstrument {
                id: 1,
                symbol: "BTC".into(),
                tick_size: 0.1,
                lot_size: 0.001,
            }
        ));

        // The sweep's own cancel confirmations are the reason the drain publishes at all.
        assert!(publishable_while_stopping(&PublishEvent::LiveEvent(
            hftbacktest::types::LiveEvent::Position {
                symbol: "BTC".into(),
                qty: 0.0,
                exch_ts: 0,
            }
        )));
        assert!(publishable_while_stopping(&PublishEvent::BatchStart(1)));
        assert!(publishable_while_stopping(&PublishEvent::BatchEnd(1)));
    }

    /// And the orderly path is the one case where that cannot happen, because the drain only
    /// reports `SendersDropped` when there is provably nobody left who could send.
    #[tokio::test]
    async fn the_orderly_drain_only_finishes_when_no_sender_remains() {
        let (tx, mut rx) = unbounded_channel::<PublishEvent>();
        let held = tx.clone();
        drop(tx);

        assert_eq!(
            drain_publish(&mut rx, Duration::from_millis(20), |_| Ok(())).await,
            DrainOutcome::GraceElapsed { published: 0 },
            "one live sender is enough to keep the drain from claiming the process is quiet"
        );

        drop(held);
        assert_eq!(
            drain_publish(&mut rx, Duration::from_millis(20), |_| Ok(())).await,
            DrainOutcome::SendersDropped { published: 0 }
        );
    }

    /// The defaults have to be the safe ones: a stop sweeps, and liveness watches. Both are
    /// opt-out, and an operator who has never read this file gets the fail-closed side.
    #[test]
    fn the_supervision_defaults_are_the_fail_closed_ones() {
        let empty: SupervisionConfig = toml::from_str("").unwrap();
        assert!(empty.shutdown.sweep_orders);
        assert_eq!(empty.shutdown.grace(), Duration::from_millis(5_000));
        assert!(empty.bot_liveness.enabled);
        assert_eq!(empty.bot_liveness.timeout(), Duration::from_secs(10));
        // The sweep gets its own budget, and it must be more than one symbol's: while the
        // two shared `grace_ms` the default gave a 20 s-per-symbol sweep 5 s in total.
        assert_eq!(empty.shutdown.sweep_timeout(), Duration::from_secs(30));
        assert!(empty.shutdown.sweep_timeout() > empty.shutdown.grace());
    }

    /// The supervision keys ride in the backend's own config file, so the parser must
    /// ignore everything it does not own — and the backend's parser must keep ignoring
    /// these. One file, two readers.
    #[test]
    fn supervision_is_read_from_the_backends_own_config_and_ignores_its_keys() {
        let config: SupervisionConfig = toml::from_str(
            r#"
            public_url = "wss://api.hyperliquid-testnet.xyz/ws"
            rest_url = "https://api.hyperliquid-testnet.xyz"
            coins = ["BTC"]

            [shutdown]
            sweep_orders = false
            grace_ms = 7500
            sweep_timeout_ms = 45000

            [bot_liveness]
            enabled = false
            timeout_ms = 30000
            "#,
        )
        .unwrap();

        assert!(!config.shutdown.sweep_orders);
        assert_eq!(config.shutdown.grace(), Duration::from_millis(7_500));
        assert_eq!(config.shutdown.sweep_timeout(), Duration::from_secs(45));
        assert!(!config.bot_liveness.enabled);
        assert_eq!(config.bot_liveness.timeout(), Duration::from_secs(30));

        // A partial section keeps the other field's default.
        let partial: SupervisionConfig =
            toml::from_str("[bot_liveness]\ntimeout_ms = 15000\n").unwrap();
        assert!(partial.bot_liveness.enabled);
        assert_eq!(partial.bot_liveness.timeout(), Duration::from_secs(15));
        assert!(partial.shutdown.sweep_orders);
    }

    /// **Every shipped example must carry these sections, and carry them switched on.**
    /// `AGENTS.md` §6 requires a new config field to appear in `connector/examples/*.toml`,
    /// and an example that has drifted is worse than none: it is what a deployment copies.
    /// The policy is generic, so all four backends' examples answer for it — including the
    /// three whose sweep is not implemented, where the setting is honest about doing nothing
    /// rather than absent.
    #[test]
    fn every_shipped_example_documents_the_supervision_policy() {
        for (name, raw) in [
            ("hyperliquid", include_str!("../examples/hyperliquid.toml")),
            ("bybit", include_str!("../examples/bybit.toml")),
            (
                "binancefutures",
                include_str!("../examples/binancefutures.toml"),
            ),
            ("binancespot", include_str!("../examples/binancespot.toml")),
        ] {
            let config: SupervisionConfig = toml::from_str(raw)
                .unwrap_or_else(|error| panic!("{name}.toml does not parse: {error}"));

            assert!(
                config.shutdown.sweep_orders,
                "{name}.toml must ship with the fail-closed side of the stop policy"
            );
            assert!(
                config.bot_liveness.enabled,
                "{name}.toml must ship with bot liveness on"
            );
            // A window shorter than the bot's own heartbeat interval sweeps healthy bots.
            assert!(
                config.bot_liveness.timeout() >= Duration::from_secs(5),
                "{name}.toml's liveness window is too tight for a bot heartbeating once a \
                 second: {:?}",
                config.bot_liveness.timeout()
            );
            assert!(
                config.shutdown.grace() >= Duration::from_secs(1),
                "{name}.toml"
            );
            // The one that a shipped default got wrong: the sweep must be given more than
            // one symbol's cancel budget (`CANCEL_BUDGET`, 20 s in the Hyperliquid backend),
            // or a stop can be advertised as fail-closed and still leave a grid resting.
            assert!(
                config.shutdown.sweep_timeout() >= Duration::from_secs(20),
                "{name}.toml gives the shutdown sweep {:?}, which does not clear one \
                 symbol's cancel budget",
                config.shutdown.sweep_timeout()
            );
        }
    }
}
