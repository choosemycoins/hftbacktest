//! The public market-data WebSocket: one connection, every registered symbol.
//!
//! The policy here is not generic resilience boilerplate. Each rule answers something
//! measured about this venue (design note §2):
//!
//! * **Re-subscription is derived from the shared symbol set on every connect**, never from
//!   the registration broadcast alone — `AGENTS.md` §4.2, and the test in this file.
//! * **The catalog round trip runs beside this loop, not in front of it.** `select!` awaits
//!   one arm at a time, so a REST call awaited where its answer is needed stops the read half
//!   being polled at all — for up to [`CATALOG_TIMEOUT`], and again every housekeeping tick
//!   while any symbol is unresolved, which starves every symbol on the connection. It is
//!   spawned instead, and its answer is an arm ([`PublicStream::start_resolution`]).
//! * **A break in the book is detected by `begin_nonce` and by nothing else.** `offset`
//!   looks like a sequence number and *jumps backwards* on every resubscribe, so a connector
//!   that keys resync on it repairs itself into a rate limit, invisibly (§2.2).
//! * **A repair is `unsubscribe` then `subscribe`.** A bare duplicate subscribe is answered
//!   `30003 Already Subscribed` and **no snapshot follows** — measured twice on mainnet — so
//!   the obvious repair repairs nothing and says so nowhere.
//! * **Client messages are spent against a budget.** Exceeding 200 a minute is not answered
//!   with an error: the connection is throttled, which from this side is a socket that goes
//!   quiet (§2.5) — the one failure that is indistinguishable from a quiet market.
//! * **A subscription is confirmed by positive evidence.** The venue's refusals name neither
//!   channel nor market and do not close the socket, and there is no client correlation id
//!   in the protocol, so a subscription that did not take is a symbol with no data on a
//!   perfectly healthy connection. Acks are counted instead, and a symbol without one past a
//!   deadline is reported to the bots **by name** (§3.6).
//! * **Silence is reported, not assumed to be a quiet market**, for the same reason.
//!
//! Calibration worth knowing before touching the repair path: over a full recorded day —
//! 8–15 markets, ~1.9 M book frames — the chain broke **zero** times (§2.2). The risk here
//! is not a repair storm; it is an untested branch in a money path.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use hftbacktest::{
    live::ipc::TO_ALL,
    prelude::{ErrorKind, Event, LiveEvent},
};
use tokio::{
    select,
    sync::{
        broadcast::{Receiver, error::RecvError},
        mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    },
    time,
    // `tokio`'s, not `std`'s, and the difference is only visible in a test: with the
    // `test-util` feature — a dev-dependency here — a paused clock moves this one too, so
    // the idle detector and the repair cooldown can be driven in virtual time instead of by
    // sleeping through 90 seconds. In the shipped binary it is `std::time::Instant`.
    time::Instant,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Bytes, Error as WsError, Message, client::IntoClientRequest},
};
use tracing::{error, info, warn};

use crate::{
    connector::PublishEvent,
    lighter::{
        LighterError,
        depth::DepthMirror,
        msg::{Book, Frame, market_id_of, parse_frame},
        publish_error,
        rest::{MarketInfo, Resolution, resolve_symbols},
        trades::{TradeDedup, trade_event},
    },
    utils::{BackoffStrategy, ExponentialBackoff, SubscriptionTracker},
};

/// A dial that has not completed by now is not going to.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
/// A write that has not been accepted by now means the write half is wedged.
pub const WRITE_TIMEOUT: Duration = Duration::from_secs(10);
/// No frame of any kind for this long means the socket is gone, whatever it claims. Wide,
/// because a false positive costs a reconnect and a false negative costs the feed.
pub const IDLE_TIMEOUT: Duration = Duration::from_secs(90);
/// How often the housekeeping runs: the idle check, the subscription deadlines, the silence
/// watchdog, deferred repairs and the counter log.
pub const HOUSEKEEPING_INTERVAL: Duration = Duration::from_secs(15);
/// Application-level keepalive. The venue needs a client frame every two minutes and sends
/// no pings of its own; 30 s leaves three missed pings of margin.
pub const PING_INTERVAL: Duration = Duration::from_secs(30);
/// The keepalive the venue understands. It answers a protocol Ping too, but this one is
/// symmetrical with the rest of the client's traffic and is spent from the same budget.
pub const PING_FRAME: &str = r#"{"type":"ping"}"#;
/// How long a subscription may go unacknowledged before the bots are told, by name.
///
/// The venue answers a subscribe it will not serve with an error frame naming **neither**
/// the channel nor the market, and leaves the socket open (§2.4, §3.6), so this deadline is
/// the only thing that can attribute the failure. Generously above the measured ack round
/// trip of 267 ms.
pub const SUBSCRIBE_ACK_DEADLINE: Duration = Duration::from_secs(30);
/// How long a subscribed market may produce no book frame before the bots are told.
///
/// A throttled connection looks exactly like a quiet market from here (§2.5), and so does a
/// subscription the venue quietly stopped serving. Measured cadence is a 50–150 ms median
/// per market with a 1.77 s worst interval, so two minutes only ever fires on a feed that
/// has genuinely stopped.
pub const SYMBOL_SILENCE_TIMEOUT: Duration = Duration::from_secs(120);
/// The least time between two repairs of the same market.
///
/// A repair costs two client frames and brings a fresh snapshot — 105 KB and ~2 900 levels
/// on ETH. Bounded on the *attempt*, not on the outcome: the venue answers a repair it will
/// not serve with an error frame carrying no channel, so nothing here can key on the
/// outcome, and a latch cleared only by a snapshot would leave a refused market broken for
/// the life of the connection.
pub const REPAIR_COOLDOWN: Duration = Duration::from_secs(60);
/// The venue's client-message budget, per minute. Exceeding it is answered by throttling the
/// connection, not by an error (§2.5).
pub const VENUE_MSG_BUDGET_PER_MIN: usize = 200;
/// What this connector allows itself, per minute.
///
/// Half, deliberately. The remainder covers the keepalives, a reconnect inside the same
/// minute — the backoff floor is one second, so a flapping venue does that — and the fact
/// that **the 200 is not known to be per connection**: the venue documents neither, and the
/// collector that shares this venue explicitly did not measure it (§2.5). Under the per-IP
/// reading a co-located process starts from the remainder, not from 200.
pub const CLIENT_MSG_BUDGET_PER_MIN: usize = VENUE_MSG_BUDGET_PER_MIN / 2;
/// Reconnect ladder.
pub const BACKOFF_MIN: Duration = Duration::from_secs(1);
pub const BACKOFF_MAX: Duration = Duration::from_secs(30);
/// Share of an interval's book frames that may be refused before the feed is reported as
/// degraded rather than merely counted.
const REFUSED_RATE_ALARM: f64 = 0.05;
/// Below this many frames in an interval the share is noise.
const REFUSED_RATE_MIN_FRAMES: u64 = 20;

/// What has been observed since the connector started.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FeedCounts {
    pub snapshots: u64,
    pub deltas: u64,
    pub depth_events: u64,
    pub trade_events: u64,
    /// Prints dropped because the venue replayed them on a (re)subscribe.
    pub replayed_trades: u64,
    /// Book chains that broke — batches the venue lost. Zero over a full recorded day.
    pub chain_breaks: u64,
    /// Repairs actually written to the socket.
    pub repairs_sent: u64,
    /// Repairs the message budget would not pay for. Non-zero means the connector is close
    /// enough to the venue's throttle that it is rationing its own recovery.
    pub repairs_deferred: u64,
    /// Subscriptions the venue never acknowledged before the deadline.
    pub unacknowledged: u64,
    /// Markets whose book went silent on a healthy connection.
    pub silent_markets: u64,
    /// Frames for a market this connector does not track.
    pub untracked_market_frames: u64,
    /// `{"error":{…}}` frames. The connection survives these, which is the problem.
    pub venue_errors: u64,
    /// Frames whose shape this backend does not model. Skipped, never fatal.
    pub unmodelled_frames: u64,
    /// Frames that were not the venue's JSON, or contradicted their own `type`.
    pub malformed_frames: u64,
    /// Prints whose exchange time could not be expressed in nanoseconds.
    pub malformed_trades: u64,
    // --- rolled up from the mirrors ---
    pub stale_frames: u64,
    pub implausible_frames: u64,
    pub crossed_frames: u64,
    /// Venue prices that collided on the **catalog's** grid, and positive sizes below the
    /// catalog's own lot. Both are a check of the venue against its own decimals and read
    /// zero on a healthy feed; neither says anything about the tick and lot the *bot*
    /// registered, which never reach this backend — see
    /// [`crate::lighter::depth::MirrorCounts`].
    pub collapsed_levels: u64,
    pub sub_lot_levels: u64,
}

impl FeedCounts {
    /// Frames a mirror refused, of any kind.
    fn refused_frames(&self) -> u64 {
        self.stale_frames + self.implausible_frames + self.crossed_frames
    }

    fn book_frames(&self) -> u64 {
        self.snapshots + self.deltas
    }
}

/// `Some((refused, seen))` when the interval between two counter samples refused enough book
/// frames to be worth an alarm rather than a line in the periodic log.
///
/// A cumulative counter cannot answer the question that matters — *is this feed mostly
/// working?* A connector that has refused a thousand frames out of ten million is healthy;
/// one that refused a thousand out of a thousand is publishing a frozen book, and on this
/// venue that is what a misread timestamp unit looks like (§4.4).
pub fn degraded(previous: &FeedCounts, current: &FeedCounts) -> Option<(u64, u64)> {
    let seen = current.book_frames().saturating_sub(previous.book_frames());
    let refused = current
        .refused_frames()
        .saturating_sub(previous.refused_frames());
    (seen >= REFUSED_RATE_MIN_FRAMES && refused as f64 > seen as f64 * REFUSED_RATE_ALARM)
        .then_some((refused, seen))
}

/// A sliding-window allowance for client messages.
///
/// Not a nicety: going over the venue's limit is answered by **throttling the connection**,
/// which from this side is a socket that goes quiet — the failure this backend is least able
/// to tell from a quiet market (§2.5). So every client frame is asked for before it is sent,
/// and a refusal is loud rather than silently dropped.
pub struct MessageBudget {
    per_minute: usize,
    spent: VecDeque<Instant>,
}

impl MessageBudget {
    pub fn new(per_minute: usize) -> Self {
        Self {
            per_minute,
            spent: VecDeque::new(),
        }
    }

    /// Whether `count` messages may be sent now, spending them if so. All or nothing: half a
    /// subscribe batch is a market that is subscribed to its book and not its tape.
    pub fn try_spend(&mut self, count: usize, now: Instant) -> bool {
        while let Some(oldest) = self.spent.front() {
            if now.duration_since(*oldest) >= Duration::from_secs(60) {
                self.spent.pop_front();
            } else {
                break;
            }
        }
        if self.spent.len() + count > self.per_minute {
            return false;
        }
        for _ in 0..count {
            self.spent.push_back(now);
        }
        true
    }

    /// Messages spent inside the current window.
    pub fn spent(&self) -> usize {
        self.spent.len()
    }
}

/// One tracked market: the book the bot holds, and everything that says whether the venue is
/// still serving it.
struct Market {
    symbol: String,
    mirror: DepthMirror,
    /// The last `order_book.nonce` seen **on this connection**. `None` until the first book
    /// frame: a new connection restarts every chain, and comparing across a reconnect gives
    /// a phantom break and spends a repair on it.
    chain: Option<u64>,
    /// Chain breaks in this process. Survives a reconnect on purpose — it is the tally of
    /// how much was lost, and a venue that reconnects often is the one whose total matters.
    breaks: u64,
    /// When the book subscription was asked for, while no ack has come back.
    awaiting_ack: Option<Instant>,
    /// When this market last produced a book frame.
    last_frame: Option<Instant>,
    /// Whether the current silence has already been reported, so one quiet market does not
    /// report itself every housekeeping tick.
    reported: bool,
    /// When this market was last repaired — what [`REPAIR_COOLDOWN`] is measured from.
    last_repair: Option<Instant>,
}

/// Why a market is being reported to the bots.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Concern {
    /// The venue never acknowledged the subscription. Its refusals name nothing, so this is
    /// the only way the symbol can be attributed.
    Unacknowledged,
    /// Subscribed and acknowledged, and then nothing — which is what a throttled connection
    /// looks like.
    Silent,
}

/// What one frame produced.
#[derive(Debug, Default)]
pub struct Handled {
    /// Events to publish, in publication order.
    pub events: Vec<(String, Event)>,
    /// Markets whose book must be repaired: the chain broke, or a frame was refused, and
    /// either way the mirror no longer describes the venue's book.
    pub repairs: Vec<i64>,
}

/// The per-market state, owned by the stream **task** rather than by a connection.
///
/// Both halves have to survive a reconnect: the mirrors are what the bot's book was built
/// from, and the print window only ever does anything *because* of a resubscribe.
#[derive(Default)]
pub struct MarketState {
    markets: HashMap<i64, Market>,
    by_symbol: HashMap<String, i64>,
    dedup: TradeDedup,
    counts: FeedCounts,
}

impl MarketState {
    /// Starts tracking markets, keeping the mirror of any market already tracked.
    ///
    /// Keeping it is the reconnect case, and it is deliberate: the mirror describes the book
    /// the *bot* holds, which a connector reconnect does not disturb. A fresh one would
    /// leave every level the bot holds in place with nothing able to remove it, because the
    /// diff only speaks about levels it knows about.
    pub fn track(&mut self, markets: &[MarketInfo]) {
        for info in markets {
            self.by_symbol.insert(info.symbol.clone(), info.market_id);
            self.markets
                .entry(info.market_id)
                .or_insert_with(|| Market {
                    symbol: info.symbol.clone(),
                    mirror: DepthMirror::new(info.tick_size(), info.lot_size()),
                    chain: None,
                    breaks: 0,
                    awaiting_ack: None,
                    last_frame: None,
                    reported: false,
                    last_repair: None,
                });
        }
    }

    /// A new connection restarts every chain and every subscription's ack.
    ///
    /// Without this the first frame after a reconnect is compared against a nonce from
    /// before the socket dropped. It happens to work out while every subscribe is answered
    /// with a snapshot — a snapshot reseeds the chain — but a market whose subscribe is *not*
    /// served on the new connection would give a phantom break and spend a repair on it.
    pub fn connection_restarted(&mut self) {
        for market in self.markets.values_mut() {
            market.chain = None;
            market.awaiting_ack = None;
            market.last_frame = None;
            market.reported = false;
            market.last_repair = None;
        }
    }

    /// Records that `symbols` have just been asked for, so their acks can be waited on.
    pub fn awaiting_ack(&mut self, symbols: &[String], now: Instant) {
        for symbol in symbols {
            if let Some(market) = self
                .by_symbol
                .get(symbol)
                .and_then(|id| self.markets.get_mut(id))
            {
                market.awaiting_ack = Some(now);
                market.last_frame = Some(now);
                market.reported = false;
            }
        }
    }

    /// The whole current book of one symbol, as events, for a bot that has just registered
    /// it. Empty if the symbol is not tracked or nothing has arrived yet.
    pub fn restate(&self, symbol: &str, local_ts: i64) -> Vec<(String, Event)> {
        let Some(market) = self
            .by_symbol
            .get(symbol)
            .and_then(|id| self.markets.get(id))
        else {
            return Vec::new();
        };
        market
            .mirror
            .restate(local_ts)
            .into_iter()
            .map(|event| (market.symbol.clone(), event))
            .collect()
    }

    /// Restates every tracked market. Used when the registration broadcast lagged and the
    /// names were lost: the set is small and a restatement is idempotent, so restating all
    /// of them costs less than guessing which one was missed.
    pub fn restate_all(&self, local_ts: i64) -> Vec<(String, Event)> {
        self.markets
            .values()
            .flat_map(|market| self.restate(&market.symbol, local_ts))
            .collect()
    }

    pub fn counts(&self) -> FeedCounts {
        let mut counts = self.counts;
        for market in self.markets.values() {
            let mirror = market.mirror.counts();
            counts.stale_frames += mirror.stale;
            counts.implausible_frames += mirror.implausible;
            counts.crossed_frames += mirror.crossed;
            counts.collapsed_levels += mirror.collapsed;
            counts.sub_lot_levels += mirror.sub_lot;
        }
        counts.replayed_trades = self.dedup.dropped();
        counts
    }

    /// The markets that are subscribed and not delivering, and what to say about each.
    ///
    /// Reporting is once per episode, not once per tick: a market that has genuinely stopped
    /// would otherwise publish an error every fifteen seconds for as long as it stayed
    /// stopped, and a bot's `error_handler` is where policy lives (`AGENTS.md` §1.1) — it
    /// should be told once, clearly.
    pub fn concerns(&mut self, now: Instant) -> Vec<(String, Concern)> {
        let mut out = Vec::new();
        for market in self.markets.values_mut() {
            if market.reported {
                continue;
            }
            let concern = match (market.awaiting_ack, market.last_frame) {
                (Some(asked), _) if now.duration_since(asked) > SUBSCRIBE_ACK_DEADLINE => {
                    Concern::Unacknowledged
                }
                (None, Some(seen)) if now.duration_since(seen) > SYMBOL_SILENCE_TIMEOUT => {
                    Concern::Silent
                }
                _ => continue,
            };
            market.reported = true;
            out.push((market.symbol.clone(), concern));
        }
        for (_, concern) in &out {
            match concern {
                Concern::Unacknowledged => self.counts.unacknowledged += 1,
                Concern::Silent => self.counts.silent_markets += 1,
            }
        }
        out
    }

    /// Whether this market's cooldown has expired. Read-only: the cooldown is spent by
    /// [`Self::repair_sent`], when the repair is actually on the wire, because a repair the
    /// message budget refused cost the venue nothing and must not cost a cooldown either.
    pub fn may_repair(&self, market_id: i64, now: Instant) -> bool {
        let Some(market) = self.markets.get(&market_id) else {
            return false;
        };
        match market.last_repair {
            Some(at) => now.duration_since(at) >= REPAIR_COOLDOWN,
            None => true,
        }
    }

    /// Records a repair that reached the socket, starting its cooldown.
    pub fn repair_sent(&mut self, market_id: i64, now: Instant) {
        self.counts.repairs_sent += 1;
        if let Some(market) = self.markets.get_mut(&market_id) {
            market.last_repair = Some(now);
        }
    }

    pub fn repair_deferred(&mut self) {
        self.counts.repairs_deferred += 1;
    }

    pub fn malformed_frame(&mut self) {
        self.counts.malformed_frames += 1;
    }

    /// The mirrored touch of one market. Test-only: production reads the book through the
    /// events it published, never through this.
    #[cfg(test)]
    fn touch(&self, market_id: i64) -> Option<(Option<f64>, Option<f64>)> {
        let market = self.markets.get(&market_id)?;
        Some((market.mirror.best_bid(), market.mirror.best_ask()))
    }

    /// Turns one inbound frame into the events to publish and the repairs to ask for.
    pub fn on_frame(&mut self, frame: Frame, local_ts: i64, now: Instant) -> Handled {
        match frame {
            Frame::BookSnapshot(book) => {
                self.counts.snapshots += 1;
                self.on_book(book, true, local_ts, now)
            }
            Frame::BookUpdate(book) => {
                self.counts.deltas += 1;
                self.on_book(book, false, local_ts, now)
            }
            Frame::Trades(trades) => {
                let mut events = Vec::with_capacity(trades.len());
                for trade in trades {
                    let Some(market) = self.markets.get_mut(&trade.market_id) else {
                        self.counts.untracked_market_frames += 1;
                        continue;
                    };
                    // Prints are evidence the venue is serving this market, so they answer
                    // the subscription deadline — the tape's own `subscribed/trade` frame
                    // arrives as prints, not as a bare ack. They do **not** touch
                    // `last_frame`: that watches the *book*, and a market whose book
                    // subscription quietly failed while its tape did not is exactly the
                    // case the silence watchdog exists for.
                    market.awaiting_ack = None;
                    let symbol = market.symbol.clone();
                    if !self.dedup.accept(trade.market_id, trade.trade_id) {
                        continue;
                    }
                    let Some(event) = trade_event(&trade, local_ts) else {
                        self.counts.malformed_trades += 1;
                        warn!(
                            %symbol,
                            transaction_time_us = trade.transaction_time_us,
                            "Refusing a Lighter print whose exchange time cannot be expressed \
                             in nanoseconds."
                        );
                        continue;
                    };
                    events.push((symbol, event));
                }
                self.counts.trade_events += events.len() as u64;
                Handled {
                    events,
                    repairs: Vec::new(),
                }
            }
            Frame::Subscribed { channel } => {
                // The only positively attributable evidence the venue offers. The book
                // channel is the one that matters: it is what the depth deadline waits for.
                if let Some(market) =
                    market_id_of(&channel).and_then(|id| self.markets.get_mut(&id))
                {
                    market.awaiting_ack = None;
                    market.last_frame = Some(now);
                }
                info!(%channel, "Lighter acknowledged a subscription.");
                Handled::default()
            }
            Frame::Unsubscribed { channel } => {
                info!(%channel, "Lighter acknowledged an unsubscribe; a fresh snapshot follows.");
                Handled::default()
            }
            Frame::Connected => {
                info!("Lighter opened a session.");
                Handled::default()
            }
            Frame::Pong => Handled::default(),
            Frame::VenueError { code, message } => {
                self.counts.venue_errors += 1;
                // **Not attributable, by construction.** The frame names neither channel nor
                // market and the socket stays open, and there is no client correlation id in
                // this protocol — so nothing here can decide which symbol just lost its
                // feed. The deadline in `concerns` is what names it, later.
                error!(
                    code,
                    %message,
                    "Lighter reported an error on the public stream. It names no channel and \
                     no market, and the socket stays open, so a subscription that did not \
                     take shows up as a symbol with no data — see the subscription deadline."
                );
                Handled::default()
            }
            Frame::Other { label } => {
                self.counts.unmodelled_frames += 1;
                warn!(%label, "Ignoring an unmodelled Lighter frame.");
                Handled::default()
            }
        }
    }

    fn on_book(&mut self, book: Book, snapshot: bool, local_ts: i64, now: Instant) -> Handled {
        let Some(market) = self.markets.get_mut(&book.market_id) else {
            self.counts.untracked_market_frames += 1;
            warn!(
                market_id = book.market_id,
                "Received a Lighter book frame for a market this connector does not track; \
                 there is no tick size to interpret it with."
            );
            return Handled::default();
        };
        market.last_frame = Some(now);
        market.reported = false;
        if snapshot {
            market.awaiting_ack = None;
        }

        // **The chain, and only the chain** (§2.2): `begin_nonce(N+1) == nonce(N)`, per
        // market. A snapshot reseeds it rather than continuing it. `offset` is deliberately
        // never read — it is API-server-local and jumps backwards on every resubscribe.
        let previous = market.chain.replace(book.nonce);
        let broken = !snapshot && previous.is_some_and(|expected| book.begin_nonce != expected);
        if broken {
            market.breaks += 1;
            self.counts.chain_breaks += 1;
            warn!(
                symbol = %market.symbol,
                market_id = book.market_id,
                expected_begin_nonce = previous.unwrap_or_default(),
                begin_nonce = book.begin_nonce,
                breaks = market.breaks,
                "A Lighter order-book chain broke; batches were lost. Repairing with an \
                 unsubscribe and a subscribe — a bare duplicate subscribe is answered 30003 \
                 and no snapshot follows."
            );
            // The frame is not applied: it describes a book this mirror never saw the middle
            // of. **The frames after it are**, deliberately — the chain has already been
            // advanced to this frame's nonce, so the next one continues from here. The
            // mirror is then wrong only in the levels the lost batch touched and nothing has
            // touched since, and the repair's snapshot diffs that away. Freezing the market
            // until the repair lands is the alternative and it is worse in both directions:
            // the bot's whole book goes stale instead of a few levels, and a repair the
            // venue refuses — its refusals name no channel — would freeze it for ever.
            return Handled {
                events: Vec::new(),
                repairs: vec![book.market_id],
            };
        }

        let symbol = market.symbol.clone();
        let applied = if snapshot {
            market
                .mirror
                .on_snapshot(&book.bids, &book.asks, book.last_updated_at_us, local_ts)
        } else {
            market
                .mirror
                .on_delta(&book.bids, &book.asks, book.last_updated_at_us, local_ts)
        };
        match applied {
            Ok(events) => {
                self.counts.depth_events += events.len() as u64;
                Handled {
                    events: events.into_iter().map(|e| (symbol.clone(), e)).collect(),
                    repairs: Vec::new(),
                }
            }
            Err(refusal) => {
                // A refused frame is a hole in the mirror exactly as a lost batch is, and it
                // is answered the same way. Carrying on would leave the connector publishing
                // a diff against a book the venue no longer has.
                warn!(
                    %symbol,
                    market_id = book.market_id,
                    ?refusal,
                    "A Lighter order-book frame could not be applied; repairing the market \
                     rather than diffing against a mirror that has drifted."
                );
                Handled {
                    events: Vec::new(),
                    repairs: vec![book.market_id],
                }
            }
        }
    }
}

/// The subscribe frames for a set of markets: the book and the tape of each.
///
/// **The request spelling takes a slash** — `order_book/36` — while every frame comes back
/// with a colon. Sending the response spelling is answered `Invalid Channel` without closing
/// the socket, so the connector would sit on a healthy connection publishing nothing (§2.1).
pub fn subscription_frames(markets: &[MarketInfo]) -> Vec<String> {
    let mut frames = Vec::with_capacity(markets.len() * 2);
    for info in markets {
        for channel in ["order_book", "trade"] {
            frames.push(
                serde_json::json!({
                    "type": "subscribe",
                    "channel": format!("{channel}/{}", info.market_id),
                })
                .to_string(),
            );
        }
    }
    frames
}

/// The frames that repair one market's book after a chain break.
///
/// **Two frames, and the obvious one on its own does nothing.** Re-sending `subscribe` for a
/// channel this connection already holds is answered `{"error":{"code":30003,"message":
/// "Already Subscribed to : order_book:0"}}` and **no snapshot** — measured twice against
/// mainnet, snapshots after the duplicate: 0. A repair built that way leaves the book on a
/// broken chain until the socket happens to drop for some other reason, and nothing says so.
///
/// `unsubscribe` first, then `subscribe`, is honoured, and honoured back to back without
/// waiting for the ack: ack at +267 ms, a fresh snapshot at +522 ms.
///
/// Only the book: the tape carries no chain, so a repair would fix nothing about it while
/// costing the same budget — and it would replay 50 prints for the dedup to throw away.
pub fn repair_frames(market_id: i64) -> Vec<String> {
    ["unsubscribe", "subscribe"]
        .into_iter()
        .map(|kind| {
            serde_json::json!({
                "type": kind,
                "channel": format!("order_book/{market_id}"),
            })
            .to_string()
        })
        .collect()
}

pub struct PublicStream {
    public_url: String,
    rest_url: String,
    symbols: Arc<Mutex<HashSet<String>>>,
    /// Both a wake-up and the name of the symbol just registered. The shared set stays
    /// authoritative for *what to subscribe*; the payload only decides whose book to
    /// restate, and a lost payload degrades to restating all of them.
    symbol_rx: Receiver<String>,
    ev_tx: UnboundedSender<PublishEvent>,
    state: MarketState,
    /// Symbols resolved against the catalog, kept across reconnects.
    ///
    /// A `market_id` is venue configuration and does not change under a running process, so
    /// re-fetching 228 rows on every reconnect would spend a read budget for nothing. A
    /// symbol that has **not** resolved is not cached, so it is asked about again on the next
    /// impulse — connect, wake-up or housekeeping tick — which is how one listed later is
    /// picked up.
    ///
    /// **Keyed by the string the caller asked about**, which the catalog match makes identical
    /// to the venue's own spelling ([`MarketInfo::symbol`]). It is looked up by the pending
    /// symbol, so a key that was anything else would be a symbol that never subscribes, never
    /// resolves out of `pending`, and asks the catalog again for ever.
    instruments: HashMap<String, MarketInfo>,
    /// Markets whose repair is owed and not yet paid for.
    pending_repairs: HashSet<i64>,
    budget: MessageBudget,
    /// Where a spawned catalog round trip delivers its answer — see [`Self::start_resolution`]
    /// for why it is spawned rather than awaited where it is needed.
    ///
    /// Owned by the stream rather than by a connection so that an answer to a round trip
    /// started on a connection that has since dropped is still applied, instead of being
    /// thrown away and asked for again.
    catalog_tx: UnboundedSender<Resolution>,
    catalog_rx: UnboundedReceiver<Resolution>,
    /// Whether a catalog round trip is out. At most one is, ever.
    awaiting_catalog: bool,
}

impl PublicStream {
    pub fn new(
        public_url: String,
        rest_url: String,
        symbols: Arc<Mutex<HashSet<String>>>,
        symbol_rx: Receiver<String>,
        ev_tx: UnboundedSender<PublishEvent>,
    ) -> Self {
        let (catalog_tx, catalog_rx) = unbounded_channel();
        Self {
            public_url,
            rest_url,
            symbols,
            symbol_rx,
            ev_tx,
            state: MarketState::default(),
            instruments: HashMap::new(),
            pending_repairs: HashSet::new(),
            budget: MessageBudget::new(CLIENT_MSG_BUDGET_PER_MIN),
            catalog_tx,
            catalog_rx,
            awaiting_catalog: false,
        }
    }

    /// Connects, and keeps reconnecting. Never returns.
    ///
    /// The market state lives here, outside the connect loop, which is what makes the book
    /// mirrors and the print window survive a reconnect.
    pub async fn run(&mut self) {
        let mut backoff = ExponentialBackoff::with_bounds(BACKOFF_MIN, BACKOFF_MAX);
        let mut last_counts = FeedCounts::default();
        loop {
            let error = match self.connect().await {
                Ok(()) => LighterError::ConnectionInterrupted,
                Err(error) => error,
            };
            let counts = self.state.counts();
            error!(?error, ?counts, "The Lighter public stream disconnected.");
            self.report(&mut last_counts, counts);
            publish_error(&self.ev_tx, ErrorKind::ConnectionInterrupted, &error);
            let delay = backoff.backoff();
            info!(?delay, "Reconnecting to the Lighter public stream.");
            time::sleep(delay).await;
        }
    }

    /// One connection, from dial to disconnect.
    async fn connect(&mut self) -> Result<(), LighterError> {
        let request = self.public_url.as_str().into_client_request()?;
        let (ws_stream, _) = time::timeout(CONNECT_TIMEOUT, connect_async(request))
            .await
            .map_err(|_| LighterError::ConnectTimeout(CONNECT_TIMEOUT))??;
        info!(url = %self.public_url, "Connected to the Lighter public stream.");

        let (mut write, mut read) = ws_stream.split();
        self.serve(&mut write, &mut read).await
    }

    /// One connection: subscribe, then pump it until it drops.
    ///
    /// Takes the halves instead of dialling so the connect-time subscribe — the fix for
    /// `AGENTS.md` §4.2, and the one call the venue's silence depends on — is driven by the
    /// tests below rather than only modelled by them.
    pub(crate) async fn serve<S, R>(
        &mut self,
        write: &mut S,
        read: &mut R,
    ) -> Result<(), LighterError>
    where
        S: SinkExt<Message> + Unpin,
        LighterError: From<S::Error>,
        R: StreamExt<Item = Result<Message, WsError>> + Unpin,
    {
        // Per connection, deliberately: see `SubscriptionTracker`. Everything registered so
        // far goes out now, which is the whole of `AGENTS.md` §4.2.
        let mut tracker = SubscriptionTracker::default();
        self.state.connection_restarted();
        self.pending_repairs.clear();
        self.subscribe_pending(write, &mut tracker).await?;

        let mut ping = time::interval(PING_INTERVAL);
        ping.reset();
        let mut housekeeping = time::interval(HOUSEKEEPING_INTERVAL);
        housekeeping.reset();
        let mut last_frame = Instant::now();
        let mut last_counts = self.state.counts();
        // A closed broadcast answers instantly and for ever, so the arm has to stop being
        // polled: leaving it in place spins this loop at full tilt for the life of the
        // connection.
        let mut registrations_closed = false;

        loop {
            select! {
                _ = ping.tick() => {
                    if self.budget.try_spend(1, Instant::now()) {
                        send(write, Message::Text(PING_FRAME.into())).await?;
                    } else {
                        // The keepalive losing to the budget means the connector is about to
                        // be throttled anyway; saying so is the only way anyone finds out.
                        warn!(
                            spent = self.budget.spent(),
                            "Deferring the Lighter keepalive: the client message budget is \
                             exhausted. The venue drops a connection with no client frame \
                             for two minutes."
                        );
                    }
                }
                _ = housekeeping.tick() => {
                    let now = Instant::now();
                    let idle = last_frame.elapsed();
                    if idle > IDLE_TIMEOUT {
                        return Err(LighterError::IdleTimeout(idle));
                    }
                    self.report_concerns(now);
                    self.flush_repairs(write, now).await?;
                    self.subscribe_pending(write, &mut tracker).await?;
                    let counts = self.state.counts();
                    self.report(&mut last_counts, counts);
                }
                // The answer to a round trip started by one of the three impulses in
                // `start_resolution`. A pattern that does not match disables this arm rather
                // than ending the connection: the sender lives on `self`, so `None` is
                // unreachable, and if it ever happened the feed would carry on without it.
                Some((resolved, refused)) = self.catalog_rx.recv() => {
                    self.awaiting_catalog = false;
                    for info in resolved {
                        self.instruments.insert(info.symbol.clone(), info);
                    }
                    // Re-derived rather than remembered: the round trip was out for a while,
                    // and the set may have grown or the tracker moved on under it.
                    let registered = self.symbols.lock().unwrap().clone();
                    let pending = tracker.pending(&registered);
                    let markets = self.addressable(&pending);
                    self.apply_resolution(write, &mut tracker, &pending, markets, refused).await?;
                }
                registered = self.symbol_rx.recv(), if !registrations_closed => {
                    match registered {
                        Ok(symbol) => {
                            // A bot registering an instrument gets a *fresh* fused depth in
                            // `main.rs` and a `SnapshotComplete` regardless, so unless it is
                            // handed the book it has none — and on this venue the next
                            // snapshot may be hours away. See `DepthMirror::restate`.
                            let events = self.state.restate(&symbol, local_now());
                            self.publish(events);
                            self.subscribe_pending(write, &mut tracker).await?;
                        }
                        Err(RecvError::Lagged(missed)) => {
                            warn!(
                                missed,
                                "Registration wake-ups were dropped; restating every market."
                            );
                            let events = self.state.restate_all(local_now());
                            self.publish(events);
                            self.subscribe_pending(write, &mut tracker).await?;
                        }
                        Err(RecvError::Closed) => registrations_closed = true,
                    }
                }
                message = read.next() => {
                    let now = Instant::now();
                    match message {
                        Some(Ok(Message::Text(text))) => {
                            // **A data clock, not an any-frame one.** A pong proves the
                            // *socket* is alive and says nothing about whether the venue is
                            // still serving the subscriptions — and "socket up,
                            // subscriptions silently gone" is precisely the state this venue
                            // produces: it answers an unknown market with an error frame and
                            // no close, and it **throttles rather than rejects** (§2.5).
                            // Whether a throttled connection still answers keepalives is not
                            // measured, so counting a pong as life would disarm this check
                            // against the one failure it exists for. Same distinction the
                            // collector draws on this venue.
                            if self.handle(&text, now) {
                                last_frame = now;
                            }
                            self.flush_repairs(write, now).await?;
                        }
                        Some(Ok(Message::Ping(_))) => {
                            send(write, Message::Pong(Bytes::default())).await?;
                        }
                        Some(Ok(Message::Close(frame))) => {
                            return Err(LighterError::ConnectionAbort(
                                frame.map(|f| f.to_string()).unwrap_or_default(),
                            ));
                        }
                        Some(Ok(_)) => {}
                        // Named rather than `from`: with `LighterError: From<S::Error>` in
                        // scope the conversion is ambiguous.
                        Some(Err(error)) => return Err(LighterError::Tungstenite(Box::new(error))),
                        None => return Err(LighterError::ConnectionInterrupted),
                    }
                }
            }
        }
    }

    /// Subscribes every registered symbol this connection has not subscribed yet, and asks
    /// the catalog about the ones it cannot address.
    ///
    /// Reads the shared symbol set rather than the broadcast: `Connector::register` sends
    /// each symbol into the broadcast exactly once ever, while a reconnect builds a fresh
    /// `Receiver` that only sees what is sent after it — so subscribing from the broadcast
    /// alone leaves a reconnected stream connected and subscribed to nothing. `AGENTS.md`
    /// §4.2.
    ///
    /// **Whatever can be subscribed now is subscribed now.** A symbol waiting on a catalog
    /// round trip holds nothing else up, and the round trip itself does not run here — see
    /// [`Self::start_resolution`].
    async fn subscribe_pending<S>(
        &mut self,
        write: &mut S,
        tracker: &mut SubscriptionTracker,
    ) -> Result<(), LighterError>
    where
        S: SinkExt<Message> + Unpin,
        LighterError: From<S::Error>,
    {
        let registered = self.symbols.lock().unwrap().clone();
        let pending = tracker.pending(&registered);
        if pending.is_empty() {
            return Ok(());
        }

        let unresolved: Vec<String> = pending
            .iter()
            .filter(|symbol| !self.instruments.contains_key(*symbol))
            .cloned()
            .collect();
        if !unresolved.is_empty() {
            self.start_resolution(unresolved);
        }
        let markets = self.addressable(&pending);
        if markets.is_empty() {
            // Not an error yet: the answer that would name these symbols is still out. The
            // one that comes back with nothing to subscribe is reported in
            // [`Self::apply_resolution`].
            return Ok(());
        }
        self.subscribe_markets(write, tracker, markets).await
    }

    /// The pending symbols whose catalog row is in hand, in `pending` order.
    fn addressable(&self, pending: &[String]) -> Vec<MarketInfo> {
        pending
            .iter()
            .filter_map(|symbol| self.instruments.get(symbol).cloned())
            .collect()
    }

    /// Asks the catalog about `symbols`, **beside** the connection rather than in front of it.
    ///
    /// `serve` awaits its `select!` arms one at a time, so a round trip awaited where its
    /// answer is needed stops the connection polling its read half for as long as the venue's
    /// REST takes — up to [`CATALOG_TIMEOUT`], and again on every housekeeping tick for as
    /// long as the symbol stays unresolved, because a transport failure is deliberately not a
    /// verdict and is retried for ever. That starves the market data of every symbol on the
    /// connection, and the idle detector cannot rescue it: frames still arrive between the
    /// stalls. So the round trip is spawned and its answer arrives as an arm of the loop.
    ///
    /// **At most one is out, and only three impulses start one**: the connect, a registration
    /// wake-up and the housekeeping tick. In particular the arm that *applies* an answer does
    /// not start the next one — a catalog that fails fast would then be retried as fast as it
    /// fails, which is the same rate limit the whole message budget exists to stay under. The
    /// cost is that a symbol registered while a round trip is out waits for the next
    /// housekeeping tick, bounded by [`HOUSEKEEPING_INTERVAL`].
    ///
    /// A panic inside the spawned task cannot wedge this flag: the connector's panic hook is
    /// `exit(1)` for every thread (`AGENTS.md` §4.7).
    fn start_resolution(&mut self, symbols: Vec<String>) {
        if self.awaiting_catalog {
            return;
        }
        self.awaiting_catalog = true;
        let rest_url = self.rest_url.clone();
        let catalog_tx = self.catalog_tx.clone();
        tokio::spawn(async move {
            let resolution = resolve_symbols(&rest_url, &symbols).await;
            // The receiver lives on the stream, which outlives every connection, so this only
            // fails when the whole connector is going away.
            let _ = catalog_tx.send(resolution);
        });
    }

    /// Applies one catalog answer: subscribes what resolved, reports what did not, and
    /// records what may not be asked about again on this connection.
    ///
    /// Split from the round trip so the marking rule is testable without a network, because
    /// getting it wrong is invisible: a symbol marked subscribed is never revisited, and a
    /// connection whose every symbol was marked after failing to resolve stays up, answers
    /// pings, and publishes nothing until the process is restarted.
    pub(crate) async fn apply_resolution<S>(
        &mut self,
        write: &mut S,
        tracker: &mut SubscriptionTracker,
        pending: &[String],
        markets: Vec<MarketInfo>,
        refused: Vec<(String, LighterError)>,
    ) -> Result<(), LighterError>
    where
        S: SinkExt<Message> + Unpin,
        LighterError: From<S::Error>,
    {
        for (symbol, error) in &refused {
            error!(
                %symbol,
                ?error,
                "Refusing to subscribe a Lighter symbol that does not resolve to an active \
                 market. Subscribing to an id nobody meant is answered `Invalid Channel` \
                 without closing the socket, so it would read as an instrument that is \
                 simply quiet."
            );
            publish_error(&self.ev_tx, ErrorKind::CriticalConnectionError, error);
        }

        // Only a listing verdict is remembered: it will not change within a connection, so
        // re-asking on every wake-up would be a REST call — and another error to every bot —
        // for nothing. Anything else is left pending, which is the difference between a
        // symbol that recovers on the next wake-up and one silently written off for the life
        // of the process. Marked here rather than beside the subscribe below so that a
        // budget deferral does not re-report it every fifteen seconds.
        mark_verdicts(tracker, &refused);

        if markets.is_empty() {
            if !pending.is_empty() {
                error!(
                    ?pending,
                    "The Lighter catalog answered and not one symbol still waiting for a \
                     subscription could be subscribed."
                );
            }
            return Ok(());
        }
        self.subscribe_markets(write, tracker, markets).await
    }

    /// Writes the subscribe frames for markets that are ready to be asked for.
    ///
    /// `markets` is never empty here: "nothing could be subscribed" is a report, not a write,
    /// and it is made by the caller that knows whether an answer is still outstanding.
    async fn subscribe_markets<S>(
        &mut self,
        write: &mut S,
        tracker: &mut SubscriptionTracker,
        markets: Vec<MarketInfo>,
    ) -> Result<(), LighterError>
    where
        S: SinkExt<Message> + Unpin,
        LighterError: From<S::Error>,
    {
        let frames = subscription_frames(&markets);
        let deferred: Vec<&str> = markets.iter().map(|info| info.symbol.as_str()).collect();
        let now = Instant::now();
        if !self.budget.try_spend(frames.len(), now) {
            // Nothing is marked, so the next housekeeping tick asks again — the budget is a
            // sliding minute, so it will free up on its own.
            warn!(
                frames = frames.len(),
                spent = self.budget.spent(),
                budget = CLIENT_MSG_BUDGET_PER_MIN,
                symbols = ?deferred,
                "Deferring a Lighter subscribe: the client message budget is exhausted. \
                 Going over it is not answered with an error — the venue throttles the \
                 connection, which from here looks like a market that went quiet."
            );
            return Ok(());
        }

        self.state.track(&markets);
        for frame in &frames {
            send(write, Message::Text(frame.as_str().into())).await?;
        }
        let subscribed: Vec<String> = markets.iter().map(|info| info.symbol.clone()).collect();
        // Marked only once every frame is on the wire: a write that failed drops the
        // connection, and the next one has to ask about all of them again.
        tracker.mark(&subscribed);
        self.state.awaiting_ack(&subscribed, now);
        info!(
            symbols = ?subscribed,
            frames = frames.len(),
            "Subscribed to the Lighter public feeds."
        );
        Ok(())
    }

    /// Pays for and writes whatever repairs are owed.
    async fn flush_repairs<S>(&mut self, write: &mut S, now: Instant) -> Result<(), LighterError>
    where
        S: SinkExt<Message> + Unpin,
        LighterError: From<S::Error>,
    {
        if self.pending_repairs.is_empty() {
            return Ok(());
        }
        let owed: Vec<i64> = self.pending_repairs.iter().copied().collect();
        for market_id in owed {
            if !self.state.may_repair(market_id, now) {
                continue;
            }
            let frames = repair_frames(market_id);
            if !self.budget.try_spend(frames.len(), now) {
                self.state.repair_deferred();
                warn!(
                    market_id,
                    spent = self.budget.spent(),
                    budget = CLIENT_MSG_BUDGET_PER_MIN,
                    "Deferring a Lighter book repair: the client message budget is \
                     exhausted. The market keeps publishing against a mirror that has \
                     drifted until the repair goes out."
                );
                continue;
            }
            for frame in frames {
                send(write, Message::Text(frame.into())).await?;
            }
            self.pending_repairs.remove(&market_id);
            self.state.repair_sent(market_id, now);
            info!(
                market_id,
                "Repairing a Lighter order book: unsubscribe, then subscribe."
            );
        }
        Ok(())
    }

    /// Tells the bots about symbols the venue is not serving, **by name**.
    fn report_concerns(&mut self, now: Instant) {
        for (symbol, concern) in self.state.concerns(now) {
            let error = match concern {
                Concern::Unacknowledged => LighterError::SubscriptionUnacknowledged(format!(
                    "Lighter never acknowledged the subscription for {symbol}; its refusals \
                     name no channel and do not close the socket, so this symbol has no \
                     market data"
                )),
                Concern::Silent => LighterError::SymbolSilent(format!(
                    "Lighter has sent no order-book frame for {symbol} on a healthy \
                     connection; a throttled connection looks exactly like this"
                )),
            };
            error!(%symbol, ?concern, "A Lighter symbol is not being served.");
            publish_error(&self.ev_tx, ErrorKind::CriticalConnectionError, &error);
        }
    }

    /// Logs the feed counters, at `warn!` when the interval refused frames at a rate that
    /// says the feed is not delivering what it received.
    fn report(&self, last: &mut FeedCounts, counts: FeedCounts) {
        match degraded(last, &counts) {
            Some((refused, seen)) => warn!(
                refused,
                seen,
                ?counts,
                "The Lighter public feed is refusing frames; the bots' books may be frozen."
            ),
            None => info!(?counts, "Lighter public feed counters."),
        }
        *last = counts;
    }

    /// Publishes the events one frame produced, framed as a batch.
    ///
    /// The batch is what stops a bot from acting on a half-applied frame: `LiveBot`
    /// processes everything between `BatchStart` and `BatchEnd` before returning from
    /// `elapse`.
    fn publish(&self, events: Vec<(String, Event)>) {
        if events.is_empty() {
            return;
        }
        self.ev_tx.send(PublishEvent::BatchStart(TO_ALL)).unwrap();
        for (symbol, event) in events {
            self.ev_tx
                .send(PublishEvent::LiveEvent(LiveEvent::Feed { symbol, event }))
                .unwrap();
        }
        self.ev_tx.send(PublishEvent::BatchEnd(TO_ALL)).unwrap();
    }

    /// Handles one text frame; returns whether it was **market data**, which is what the
    /// idle detector counts. See the read arm of [`Self::serve`].
    fn handle(&mut self, text: &str, now: Instant) -> bool {
        let local_ts = local_now();
        let frame = match parse_frame(text) {
            Ok(frame) => frame,
            Err(error) => {
                // Counted and logged, never fatal: the connector's panic hook is `exit(1)`,
                // so one frame this backend cannot read must not take every bot's market
                // data down with it.
                self.state.malformed_frame();
                error!(?error, %text, "Couldn't read a Lighter frame.");
                return false;
            }
        };
        let data = matches!(
            frame,
            Frame::BookSnapshot(_) | Frame::BookUpdate(_) | Frame::Trades(_)
        );
        let handled = self.state.on_frame(frame, local_ts, now);
        self.publish(handled.events);
        self.pending_repairs.extend(handled.repairs);
        data
    }
}

/// Remembers only the refusals that are verdicts. Anything else is asked about again.
fn mark_verdicts(tracker: &mut SubscriptionTracker, refused: &[(String, LighterError)]) {
    let verdicts: Vec<String> = refused
        .iter()
        .filter(|(_, error)| error.is_listing_verdict())
        .map(|(symbol, _)| symbol.clone())
        .collect();
    tracker.mark(&verdicts);
}

/// The local receive time in nanoseconds. `0` means the clock could not be read, which
/// [`DepthMirror`] reads as "no reference to sanity-check the venue's time against".
pub(crate) fn local_now() -> i64 {
    Utc::now().timestamp_nanos_opt().unwrap_or(0)
}

/// Writes one message, or fails.
///
/// Unconditionally bounded: a wedged write half is silent, and an unbounded `send` on one
/// simply never returns.
pub(crate) async fn send<S>(write: &mut S, message: Message) -> Result<(), LighterError>
where
    S: SinkExt<Message> + Unpin,
    LighterError: From<S::Error>,
{
    time::timeout(WRITE_TIMEOUT, write.send(message))
        .await
        .map_err(|_| LighterError::WriteTimeout(WRITE_TIMEOUT))?
        .map_err(LighterError::from)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        sync::{
            Arc,
            Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use hftbacktest::prelude::{
        LOCAL_ASK_DEPTH_EVENT,
        LOCAL_BID_DEPTH_EVENT,
        LOCAL_BUY_TRADE_EVENT,
        LiveEvent,
    };
    use tokio::{
        sync::{broadcast, mpsc::unbounded_channel},
        time::{self, Instant},
    };
    use tokio_tungstenite::tungstenite::{Error as WsError, Message};

    use crate::{
        connector::PublishEvent,
        lighter::{
            LighterError,
            fixtures::{
                CONNECTED,
                ERROR_ALREADY_SUBSCRIBED,
                HEIGHT,
                ORDER_BOOK_SNAPSHOT_CRV,
                ORDER_BOOK_UPDATE_CRV_1,
                ORDER_BOOK_UPDATE_CRV_2,
                ORDER_BOOK_UPDATE_CRV_3,
                PONG,
                TICKER_BTC,
                TRADE_CRV,
                TRADE_REPLAY_CRV,
            },
            msg::{Frame, parse_frame},
            public_stream::{
                CLIENT_MSG_BUDGET_PER_MIN,
                Concern,
                FeedCounts,
                Handled,
                IDLE_TIMEOUT,
                MarketState,
                MessageBudget,
                PING_INTERVAL,
                PublicStream,
                REPAIR_COOLDOWN,
                SUBSCRIBE_ACK_DEADLINE,
                SYMBOL_SILENCE_TIMEOUT,
                VENUE_MSG_BUDGET_PER_MIN,
                degraded,
                repair_frames,
                subscription_frames,
            },
            rest::MarketInfo,
        },
        utils::{
            SubscriptionTracker,
            testing::{RecordingSink, closed_read, read_after_connect, read_frames},
        },
    };

    /// CRV, as the mainnet catalog describes it.
    fn crv() -> MarketInfo {
        MarketInfo {
            symbol: "CRV".to_string(),
            market_id: 36,
            price_decimals: 5,
            size_decimals: 1,
        }
    }

    fn hype() -> MarketInfo {
        MarketInfo {
            symbol: "HYPE".to_string(),
            market_id: 24,
            price_decimals: 4,
            size_decimals: 2,
        }
    }

    fn set(symbols: &[&str]) -> HashSet<String> {
        symbols.iter().map(|s| s.to_string()).collect()
    }

    fn names(symbols: &[&str]) -> Vec<String> {
        symbols.iter().map(|s| s.to_string()).collect()
    }

    /// A stream whose catalog is already in hand, so `serve` writes what it would write
    /// without a network round trip in the way.
    fn stream(
        registered: &[&str],
        known: &[MarketInfo],
    ) -> (
        PublicStream,
        tokio::sync::mpsc::UnboundedReceiver<PublishEvent>,
    ) {
        let (ev_tx, ev_rx) = unbounded_channel();
        let (symbol_tx, symbol_rx) = broadcast::channel(16);
        // Kept alive so the receiver does not see the channel close.
        std::mem::forget(symbol_tx);
        let mut stream = PublicStream::new(
            "wss://example.invalid/stream".to_string(),
            "https://example.invalid".to_string(),
            Arc::new(Mutex::new(set(registered))),
            symbol_rx,
            ev_tx,
        );
        for info in known {
            stream.instruments.insert(info.symbol.clone(), info.clone());
        }
        (stream, ev_rx)
    }

    fn tracked(markets: &[MarketInfo]) -> MarketState {
        let mut state = MarketState::default();
        state.track(markets);
        state
    }

    /// The local receive clock the mirrors sanity-check the venue's time against. Has to be
    /// a real one: the fixtures carry real exchange times, and a mirror whose clock says the
    /// venue is 56 years in the future refuses every frame (§4.4).
    const NOW_NS: i64 = 1_785_358_812_000_000_000;

    /// The snapshot the venue would send if it were asked for one *now*: the same frame,
    /// carrying the stamp of the last book change it contains — which is the diff before it.
    /// Only `last_updated_at` moves; every other byte, and the whole chain, is the venue's.
    fn later(text: &str) -> String {
        text.replace("1785358811946459", "1785358812048570")
    }

    fn feed(state: &mut MarketState, text: &str) -> Handled {
        state.on_frame(parse_frame(text).unwrap(), NOW_NS, Instant::now())
    }

    /// How many market-data events reached the bots.
    fn feed_events(rx: &mut tokio::sync::mpsc::UnboundedReceiver<PublishEvent>) -> usize {
        let mut count = 0;
        while let Ok(event) = rx.try_recv() {
            if matches!(event, PublishEvent::LiveEvent(LiveEvent::Feed { .. })) {
                count += 1;
            }
        }
        count
    }

    fn errors(rx: &mut tokio::sync::mpsc::UnboundedReceiver<PublishEvent>) -> Vec<String> {
        let mut out = Vec::new();
        while let Ok(event) = rx.try_recv() {
            if let PublishEvent::LiveEvent(LiveEvent::Error(error)) = event {
                out.push(format!("{:?}", error.value));
            }
        }
        out
    }

    /// **The venue's sharpest trap, on the writing side** (§2.1). A subscription is asked
    /// for with a **slash**; the colon form is answered `Invalid Channel` without closing the
    /// socket, so the connector would sit on a healthy connection publishing nothing.
    #[test]
    fn a_subscription_is_addressed_by_market_id_with_a_slash() {
        let frames = subscription_frames(&[crv(), hype()]);
        assert_eq!(frames.len(), 4, "the book and the tape of each market");

        let parsed: Vec<serde_json::Value> = frames
            .iter()
            .map(|f| serde_json::from_str(f).unwrap())
            .collect();
        for (market, id) in [("CRV", 36), ("HYPE", 24)] {
            for channel in ["order_book", "trade"] {
                let wanted = format!("{channel}/{id}");
                assert!(
                    parsed
                        .iter()
                        .any(|f| f["type"] == "subscribe" && f["channel"] == wanted.as_str()),
                    "{market} was never subscribed to {channel}: {frames:?}"
                );
            }
        }
        for frame in &parsed {
            let channel = frame["channel"].as_str().unwrap();
            assert!(channel.contains('/'), "{channel}");
            assert!(!channel.contains(':'), "{channel}");
        }
    }

    /// **A repair is two frames, and the obvious one is not one of them.** Re-sending
    /// `subscribe` on a channel this connection already holds is answered `30003 Already
    /// Subscribed` and **no snapshot follows** — measured twice on mainnet — so a repair
    /// built that way leaves the book on a broken chain indefinitely, and the refusal names
    /// no channel, so nothing downstream can even notice.
    #[test]
    fn a_repair_is_an_unsubscribe_then_a_subscribe() {
        let frames: Vec<serde_json::Value> = repair_frames(36)
            .iter()
            .map(|f| serde_json::from_str(f).unwrap())
            .collect();
        assert_eq!(
            frames,
            [
                serde_json::json!({"type": "unsubscribe", "channel": "order_book/36"}),
                serde_json::json!({"type": "subscribe", "channel": "order_book/36"}),
            ]
        );

        // The refusal the missing unsubscribe earns, as a fixture: it names no channel, so
        // the repair has to be right rather than merely observed to have failed.
        let refusal: serde_json::Value = serde_json::from_str(ERROR_ALREADY_SUBSCRIBED).unwrap();
        assert_eq!(refusal["error"]["code"], 30003);
        assert!(refusal.get("channel").is_none());
    }

    /// `AGENTS.md` §4.2: the shared symbol set is authoritative, and a reconnect re-derives
    /// every subscription from it. A reconnect is modelled the way `serve` does it — a fresh
    /// `SubscriptionTracker` per connection — so a change that hoisted the tracker out of
    /// `serve` fails this test instead of passing it.
    #[tokio::test]
    async fn every_symbol_is_subscribed_at_connect_and_resubscribed_after_a_reconnect() {
        let (mut stream, _ev_rx) = stream(&["CRV", "HYPE"], &[crv(), hype()]);

        let mut sink = RecordingSink::<LighterError>::default();
        stream
            .serve(&mut sink, &mut closed_read())
            .await
            .unwrap_err();
        assert_eq!(sink.sent.len(), 4, "two channels for each of two symbols");

        // The connection drops and comes back. Everything must go out again — including the
        // symbols registered long before this connection existed.
        let mut second = RecordingSink::<LighterError>::default();
        stream
            .serve(&mut second, &mut closed_read())
            .await
            .unwrap_err();
        assert_eq!(
            second.sent, sink.sent,
            "a reconnect must resubscribe exactly the same set"
        );
    }

    /// A symbol that does not resolve is refused **by name** and takes only itself down;
    /// everything beside it still subscribes. All-or-nothing is what Hyperliquid had to roll
    /// back — one bot registering a typo left nobody with market data (§3.5).
    #[tokio::test]
    async fn an_unresolvable_symbol_is_refused_by_name_and_the_rest_still_subscribe() {
        let (mut stream, mut ev_rx) = stream(&["CRV", "NOTACOIN"], &[]);
        let mut sink = RecordingSink::<LighterError>::default();
        let mut tracker = SubscriptionTracker::default();

        stream
            .apply_resolution(
                &mut sink,
                &mut tracker,
                &names(&["CRV", "NOTACOIN"]),
                vec![crv()],
                vec![(
                    "NOTACOIN".to_string(),
                    LighterError::UnknownSymbol("NOTACOIN is not listed on Lighter".into()),
                )],
            )
            .await
            .unwrap();

        assert_eq!(sink.sent.len(), 2, "CRV still got its two channels");
        let errors = errors(&mut ev_rx);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("NOTACOIN"), "{errors:?}");
        // A listing verdict will not change within a connection, so it is remembered.
        assert!(tracker.pending(&set(&["CRV", "NOTACOIN"])).is_empty());
    }

    /// **The blocker this rule prevents.** A symbol left unresolved because the venue would
    /// not answer must stay pending. Marking it writes it off for the life of the
    /// connection: no later wake-up revisits it, the socket stays up because the keepalive
    /// keeps the idle detector happy, and the connector runs for hours publishing nothing.
    #[tokio::test]
    async fn a_symbol_the_venue_would_not_answer_about_is_asked_about_again() {
        let (mut stream, mut ev_rx) = stream(&["CRV", "HYPE"], &[]);
        let mut sink = RecordingSink::<LighterError>::default();
        let mut tracker = SubscriptionTracker::default();

        stream
            .apply_resolution(
                &mut sink,
                &mut tracker,
                &names(&["CRV", "HYPE"]),
                vec![],
                names(&["CRV", "HYPE"])
                    .into_iter()
                    .map(|symbol| {
                        (
                            symbol,
                            LighterError::CatalogUnavailable("the catalog answered 503".into()),
                        )
                    })
                    .collect(),
            )
            .await
            .unwrap();

        assert!(sink.sent.is_empty(), "{:?}", sink.sent);
        assert_eq!(tracker.pending(&set(&["CRV", "HYPE"])), ["CRV", "HYPE"]);
        assert_eq!(errors(&mut ev_rx).len(), 2, "each symbol reported by name");
    }

    /// **The catalog round trip runs beside the read loop, never in front of it.**
    ///
    /// `serve` awaits its `select!` arms one at a time, so a resolve awaited *inline* stops
    /// the connection polling its read half for as long as the venue's REST takes to answer —
    /// up to [`CATALOG_TIMEOUT`], and again on every housekeeping tick for as long as any
    /// symbol stays unresolved (a transport failure is deliberately not a verdict, so it is
    /// retried for ever). That starves the market data of **every** symbol on the connection,
    /// including the ones that resolved long ago, and the idle detector cannot rescue it
    /// because frames still trickle through between the stalls.
    ///
    /// Driven against a REST endpoint that accepts and never answers, because that is the
    /// only shape that can starve anything: a refusal comes back in milliseconds. The wall
    /// clock is the assertion — `CRV` is already resolved and its snapshot is waiting on the
    /// read half from the first poll, so a connection that reads it promptly is one that did
    /// not wait for the catalog.
    #[tokio::test]
    async fn a_catalog_that_does_not_answer_never_stalls_the_read_half() {
        // `reqwest` builds its TLS backend when the client is built, and rustls needs a
        // process-level provider even for a plain-HTTP request. `main` installs one; a test
        // has no `main`. Idempotent — `AGENTS.md` §4.7.
        crate::install_crypto_provider();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            // Accepted and held open, unanswered, for as long as the test runs.
            let mut held = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                held.push(socket);
            }
        });

        let (mut stream, mut ev_rx) = stream(&["CRV", "NOTACOIN"], &[crv()]);
        stream.rest_url = format!("http://{address}");
        let mut sink = RecordingSink::<LighterError>::default();
        let mut read = read_frames(vec![ORDER_BOOK_SNAPSHOT_CRV.to_string()]);

        time::timeout(Duration::from_secs(2), stream.serve(&mut sink, &mut read))
            .await
            .expect("the read half was starved while the catalog round trip was outstanding")
            .unwrap_err();

        assert_eq!(
            sink.sent.len(),
            2,
            "CRV resolved long ago and must subscribe without waiting for anyone else"
        );
        assert!(
            feed_events(&mut ev_rx) > 0,
            "and its book must be read and published while the catalog is still outstanding"
        );
    }

    /// **A catalog that fails must not be retried as fast as it fails.**
    ///
    /// The other half of moving the round trip off the read loop. A transport failure is
    /// deliberately *not* a verdict, so the symbol stays pending — which makes "ask again as
    /// soon as an answer comes back" a hot loop against a venue that is already unwell, and
    /// spends the budget the throttle guard exists to protect. Only the connect, a
    /// registration wake-up and the housekeeping tick may start a round trip, so a REST that
    /// refuses instantly is asked exactly once per connect and then not again for
    /// [`HOUSEKEEPING_INTERVAL`].
    #[tokio::test]
    async fn a_catalog_that_fails_is_not_retried_faster_than_the_housekeeping_tick() {
        crate::install_crypto_provider();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let attempts = Arc::new(AtomicUsize::new(0));
        let counted = attempts.clone();
        tokio::spawn(async move {
            // Accepted and dropped: a round trip that fails as fast as the venue can fail it.
            while let Ok((socket, _)) = listener.accept().await {
                counted.fetch_add(1, Ordering::SeqCst);
                drop(socket);
            }
        });

        let (mut stream, _ev_rx) = stream(&["NOTACOIN"], &[]);
        stream.rest_url = format!("http://{address}");
        let mut sink = RecordingSink::<LighterError>::default();
        // A read half that never speaks and never ends, so the loop is driven by nothing but
        // its own arms. Real time, deliberately: a paused clock would fast-forward to the
        // housekeeping tick and the test would be measuring nothing.
        let mut read = read_after_connect(|| {});
        let _ = time::timeout(
            Duration::from_millis(250),
            stream.serve(&mut sink, &mut read),
        )
        .await;

        assert_eq!(
            attempts.load(Ordering::SeqCst),
            1,
            "the catalog was asked again the moment its own failure came back"
        );
    }

    /// A real consecutive run: the snapshot becomes the whole book, the diffs become the
    /// levels they touched, and every event is keyed by the **symbol** even though the wire
    /// is keyed by an integer.
    #[test]
    fn book_frames_become_kind_one_depth_events_for_their_symbol() {
        let mut state = tracked(&[crv()]);

        let handled = feed(&mut state, ORDER_BOOK_SNAPSHOT_CRV);
        assert_eq!(handled.events.len(), 79);
        assert!(handled.repairs.is_empty());
        assert!(handled.events.iter().all(|(symbol, _)| symbol == "CRV"));
        assert!(handled.events[0].1.is(LOCAL_BID_DEPTH_EVENT));
        assert_eq!(handled.events[0].1.local_ts, NOW_NS);

        let handled = feed(&mut state, ORDER_BOOK_UPDATE_CRV_1);
        assert_eq!(handled.events.len(), 5);
        assert!(
            handled
                .events
                .iter()
                .all(|(_, e)| e.is(LOCAL_BID_DEPTH_EVENT) || e.is(LOCAL_ASK_DEPTH_EVENT)),
            "LiveBot applies nothing else"
        );
        assert_eq!(state.counts().snapshots, 1);
        assert_eq!(state.counts().deltas, 1);
    }

    /// The tape becomes trade events, and the replay the venue sends on every subscribe does
    /// not — the connector resubscribes on every reconnect *and* every repair.
    #[test]
    fn a_trade_frame_becomes_trade_events_and_its_replay_does_not() {
        let mut state = tracked(&[crv()]);

        let handled = feed(&mut state, TRADE_REPLAY_CRV);
        assert_eq!(handled.events.len(), 3);
        assert!(handled.events[0].1.is(LOCAL_BUY_TRADE_EVENT));
        assert_eq!(handled.events[0].0, "CRV");

        let handled = feed(&mut state, TRADE_REPLAY_CRV);
        assert!(handled.events.is_empty());
        assert_eq!(state.counts().replayed_trades, 3);

        // A genuinely new print still gets through.
        assert_eq!(feed(&mut state, TRADE_CRV).events.len(), 1);
    }

    /// **The chain, and only the chain** (§2.2). A break asks for a repair and the broken
    /// frame is **not** applied: it describes a book whose middle this mirror never saw.
    #[test]
    fn a_broken_chain_asks_for_a_repair_and_the_frame_is_not_applied() {
        let mut state = tracked(&[crv()]);
        feed(&mut state, ORDER_BOOK_SNAPSHOT_CRV);
        feed(&mut state, ORDER_BOOK_UPDATE_CRV_1);

        // Skipping a frame is exactly what a lost batch looks like: update 3 chains from
        // update 2's nonce, which never arrived.
        let handled = feed(&mut state, ORDER_BOOK_UPDATE_CRV_3);
        assert_eq!(handled.repairs, [36]);
        assert!(handled.events.is_empty(), "{:?}", handled.events);
        assert_eq!(state.counts().chain_breaks, 1);

        // In order, nothing breaks and nothing is repaired — the guard is not a tripwire on
        // every frame.
        let mut state = tracked(&[crv()]);
        for text in [
            ORDER_BOOK_SNAPSHOT_CRV,
            ORDER_BOOK_UPDATE_CRV_1,
            ORDER_BOOK_UPDATE_CRV_2,
            ORDER_BOOK_UPDATE_CRV_3,
        ] {
            assert!(feed(&mut state, text).repairs.is_empty(), "{text}");
        }
        assert_eq!(state.counts().chain_breaks, 0);
    }

    /// The first frame of a connection has nothing to chain onto, and a snapshot **reseeds**
    /// the chain rather than continuing it. Calling either a break would repair a book that
    /// was just subscribed — and a repair costs a fresh snapshot, so it would loop.
    #[test]
    fn the_first_frame_of_a_connection_and_every_snapshot_reseed_the_chain() {
        // A diff arriving before any snapshot has nothing to chain onto.
        let mut state = tracked(&[crv()]);
        assert!(feed(&mut state, ORDER_BOOK_UPDATE_CRV_3).repairs.is_empty());
        assert_eq!(state.counts().chain_breaks, 0);

        let mut state = tracked(&[crv()]);
        feed(&mut state, ORDER_BOOK_SNAPSHOT_CRV);
        feed(&mut state, ORDER_BOOK_UPDATE_CRV_1);

        // A snapshot **reseeds**: its own `begin_nonce` is 0 and does not chain from the
        // diff before it, and the diff after it chains from the snapshot's `nonce`. Reading
        // either as a break would repair a book that was just subscribed — and a repair
        // brings a fresh snapshot, so it would loop.
        //
        // Stamped at the diff before it, because that is what a repair's snapshot is: it
        // describes the book *including* everything the break lost, so it never carries a
        // stamp behind the last frame already applied. The fixture is the real frame with
        // that one field advanced.
        assert!(
            feed(&mut state, &later(ORDER_BOOK_SNAPSHOT_CRV))
                .repairs
                .is_empty()
        );
        assert!(feed(&mut state, ORDER_BOOK_UPDATE_CRV_1).repairs.is_empty());
        assert_eq!(state.counts().chain_breaks, 0);

        // And a reconnect forgets the chain: the venue's nonces are unrelated across
        // connections, so comparing across one is a phantom break that spends a repair on a
        // book that was just subscribed.
        //
        // The frame fed here is deliberately one that does **not** chain from the last frame
        // of the previous connection — diff 3 follows diff 2, which this state never saw. A
        // frame that happened to chain would pass whether the reset happened or not.
        state.connection_restarted();
        assert!(feed(&mut state, ORDER_BOOK_UPDATE_CRV_3).repairs.is_empty());
        assert_eq!(state.counts().chain_breaks, 0);
    }

    /// A snapshot stamped **behind** the last frame applied is refused and repaired rather
    /// than published: the fused depth downstream drops any event older than the level it
    /// would update, so publishing that diff would leave this connector's mirror describing
    /// a book the bot never received. Not a chain break — the counter separates the two,
    /// because they call for the same repair for different reasons.
    #[test]
    fn a_snapshot_older_than_the_last_applied_frame_is_refused_and_repaired() {
        let mut state = tracked(&[crv()]);
        feed(&mut state, &later(ORDER_BOOK_SNAPSHOT_CRV));

        let handled = feed(&mut state, ORDER_BOOK_SNAPSHOT_CRV);
        assert_eq!(handled.repairs, [36]);
        assert!(handled.events.is_empty());
        assert_eq!(state.counts().chain_breaks, 0);
        assert_eq!(state.counts().stale_frames, 1);
    }

    /// **The repair reaches the socket**, driven through `serve` with a real broken run —
    /// snapshot, first diff, then a diff that chains from a batch that never arrived.
    #[tokio::test]
    async fn a_broken_chain_writes_an_unsubscribe_and_a_subscribe_to_the_socket() {
        let (mut stream, _ev_rx) = stream(&["CRV"], &[crv()]);
        let mut sink = RecordingSink::<LighterError>::default();

        stream
            .serve(
                &mut sink,
                &mut read_frames(vec![
                    ORDER_BOOK_SNAPSHOT_CRV.to_string(),
                    ORDER_BOOK_UPDATE_CRV_1.to_string(),
                    ORDER_BOOK_UPDATE_CRV_3.to_string(),
                ]),
            )
            .await
            .unwrap_err();

        assert_eq!(
            &sink.sent[2..],
            repair_frames(36).as_slice(),
            "the two subscribes, then the repair: {:?}",
            sink.sent
        );
    }

    /// A healthy run repairs nothing. Over a full recorded day — 8–15 markets, ~1.9 M book
    /// frames — the chain broke **zero** times (§2.2), so a connector that repairs on a
    /// healthy feed is spending a budget it has to keep.
    #[tokio::test]
    async fn a_healthy_run_never_writes_a_repair() {
        let (mut stream, _ev_rx) = stream(&["CRV"], &[crv()]);
        let mut sink = RecordingSink::<LighterError>::default();

        stream
            .serve(
                &mut sink,
                &mut read_frames(vec![
                    CONNECTED.to_string(),
                    ORDER_BOOK_SNAPSHOT_CRV.to_string(),
                    ORDER_BOOK_UPDATE_CRV_1.to_string(),
                    ORDER_BOOK_UPDATE_CRV_2.to_string(),
                    ORDER_BOOK_UPDATE_CRV_3.to_string(),
                    TRADE_CRV.to_string(),
                    PONG.to_string(),
                ]),
            )
            .await
            .unwrap_err();

        assert_eq!(sink.sent.len(), 2, "only the subscribe: {:?}", sink.sent);
    }

    /// **A socket that answers keepalives and serves nothing is dropped.** Going over the
    /// venue's message budget is answered by throttling the connection, and a subscription
    /// it quietly stopped serving looks identical — in both cases the socket stays open
    /// (§2.5, §2.4). So the idle detector is a **data** clock: an any-frame one would be
    /// reset for ever by the venue's own pongs, and this connector would sit connected,
    /// subscribed and publishing nothing until somebody noticed.
    ///
    /// Whether a throttled connection still answers keepalives is not measured, which is
    /// exactly why this fails closed. The clock is frozen, so the 90 s wait costs nothing.
    ///
    /// Verified by mutation: counting every frame instead of only data makes this test
    /// **hang** rather than fail an assertion — which is the honest reproduction, because
    /// that is precisely what the connector would do against a throttled venue.
    #[tokio::test(start_paused = true)]
    async fn a_socket_that_only_ever_pongs_is_dropped_by_the_idle_detector() {
        let (mut stream, _ev_rx) = stream(&["CRV"], &[crv()]);
        let mut sink = RecordingSink::<LighterError>::default();

        // A read half that answers every keepalive, for ever, and never delivers data — a
        // connection the venue has throttled but is still speaking on. Two things about it
        // are load-bearing, and both were learned by watching this test pass against the
        // bug:
        //
        // * the pongs must never stop. A mock that sends a few and then goes quiet trips the
        //   idle detector whatever the detector counts, so it cannot tell the two apart;
        // * it must be **cancel-safe**. `select!` drops the losing branches on every
        //   iteration, so a read half written as an `async` block restarts its sleep each
        //   time the 15 s housekeeping tick beats it and never delivers anything at all.
        //   `Interval::poll_tick` resumes instead of restarting.
        let mut keepalive = time::interval(PING_INTERVAL);
        keepalive.reset();
        let mut pongs = futures_util::stream::poll_fn(move |cx| {
            keepalive
                .poll_tick(cx)
                .map(|_| Some(Ok::<_, WsError>(Message::Text(PONG.into()))))
        });

        let error = stream.serve(&mut sink, &mut pongs).await.unwrap_err();
        assert!(
            matches!(error, LighterError::IdleTimeout(idle) if idle > IDLE_TIMEOUT),
            "{error:?}"
        );
        assert_eq!(stream.state.counts().snapshots, 0);
    }

    /// The cooldown is on the **attempt**, not the outcome: the venue answers a repair it
    /// will not serve with an error frame carrying no channel, so a latch cleared only by a
    /// snapshot would leave a refused market broken for the life of the connection.
    ///
    /// And the cooldown is spent by the repair **reaching the socket**, not by wanting one:
    /// a repair the message budget refused cost the venue nothing, so charging it a full
    /// minute would turn a budget squeeze into a market left broken for that minute.
    #[test]
    fn a_market_is_repaired_at_most_once_per_cooldown() {
        let mut state = tracked(&[crv()]);
        let now = Instant::now();

        assert!(state.may_repair(36, now), "nothing has been repaired yet");
        assert!(
            state.may_repair(36, now),
            "wanting one does not spend the cooldown"
        );
        state.repair_sent(36, now);
        assert!(!state.may_repair(36, now + REPAIR_COOLDOWN / 2));
        assert!(state.may_repair(36, now + REPAIR_COOLDOWN));
        // A market that is not tracked cannot be repaired, and asking must not panic.
        assert!(!state.may_repair(999, now));
    }

    /// **A subscription that never took is the failure this venue makes easiest to reach**
    /// (§3.6): the refusal names neither channel nor market and leaves the socket open. The
    /// only attributable evidence is the ack, so its absence past a deadline is what names
    /// the symbol — and it is reported once per episode, not once per tick.
    #[test]
    fn a_subscription_the_venue_never_acknowledged_is_reported_by_name() {
        let mut state = tracked(&[crv(), hype()]);
        let now = Instant::now();
        state.awaiting_ack(&names(&["CRV", "HYPE"]), now);

        assert!(state.concerns(now).is_empty(), "nothing is late yet");

        // CRV's book snapshot is its ack; HYPE says nothing.
        state.on_frame(
            parse_frame(ORDER_BOOK_SNAPSHOT_CRV).unwrap(),
            NOW_NS,
            now + Duration::from_secs(1),
        );

        let late = now + SUBSCRIBE_ACK_DEADLINE + Duration::from_secs(1);
        assert_eq!(
            state.concerns(late),
            [("HYPE".to_string(), Concern::Unacknowledged)]
        );
        assert_eq!(state.counts().unacknowledged, 1);
        assert!(
            state.concerns(late + SUBSCRIBE_ACK_DEADLINE).is_empty(),
            "once per episode, not once per tick"
        );
    }

    /// A subscription acknowledged on **any** channel counts, and the tape's ack is the one
    /// that arrives when a market is genuinely quiet.
    #[test]
    fn an_ack_on_the_tape_also_answers_the_deadline() {
        let mut state = tracked(&[crv()]);
        let now = Instant::now();
        state.awaiting_ack(&names(&["CRV"]), now);

        state.on_frame(parse_frame(TRADE_REPLAY_CRV).unwrap(), NOW_NS, now);
        assert!(
            state
                .concerns(now + SUBSCRIBE_ACK_DEADLINE + Duration::from_secs(1))
                .is_empty()
        );
    }

    /// **Silence is not a quiet market.** Going over the venue's message budget is answered
    /// by throttling the connection rather than with an error (§2.5), and a subscription the
    /// venue quietly stopped serving looks the same. Measured cadence is a 50–150 ms median
    /// per market, so two minutes of nothing is a fault, and the bots are told which symbol.
    #[test]
    fn a_market_that_goes_silent_is_reported_by_name() {
        let mut state = tracked(&[crv()]);
        let now = Instant::now();
        state.awaiting_ack(&names(&["CRV"]), now);
        state.on_frame(parse_frame(ORDER_BOOK_SNAPSHOT_CRV).unwrap(), NOW_NS, now);

        let quiet = now + SYMBOL_SILENCE_TIMEOUT + Duration::from_secs(1);
        assert_eq!(
            state.concerns(quiet),
            [("CRV".to_string(), Concern::Silent)]
        );
        assert_eq!(state.counts().silent_markets, 1);

        // Data resuming re-arms it, so a second episode is reported again.
        state.on_frame(parse_frame(ORDER_BOOK_UPDATE_CRV_1).unwrap(), NOW_NS, quiet);
        assert!(state.concerns(quiet).is_empty());
        assert_eq!(
            state.concerns(quiet + SYMBOL_SILENCE_TIMEOUT + Duration::from_secs(1)),
            [("CRV".to_string(), Concern::Silent)]
        );
    }

    /// **The budget exists because going over it is not answered with an error** — the venue
    /// throttles the connection, which from here is a socket that goes quiet (§2.5). It is a
    /// sliding minute, so an exhausted budget recovers on its own; and it is all-or-nothing
    /// per batch, because half a subscribe is a market subscribed to its book and not its
    /// tape.
    #[test]
    fn the_message_budget_is_a_sliding_minute_and_spends_whole_batches() {
        let mut budget = MessageBudget::new(4);
        let now = Instant::now();

        assert!(budget.try_spend(2, now));
        assert!(budget.try_spend(2, now));
        assert!(!budget.try_spend(1, now), "the minute is spent");

        // Half a batch is never sent.
        let mut budget = MessageBudget::new(4);
        assert!(budget.try_spend(3, now));
        assert!(!budget.try_spend(2, now));
        assert_eq!(budget.spent(), 3, "the refused batch spent nothing");

        // A sliding window, not a fixed one: the oldest spend ages out.
        assert!(budget.try_spend(2, now + Duration::from_secs(61)));
    }

    /// The arithmetic the budget has to close, in the worst minute a connection can have:
    /// every market breaking continuously — so each spends a full repair per cooldown — plus
    /// the keepalives, plus a reconnect inside the same minute resubscribing everything.
    ///
    /// It has to fit inside what this connector allows itself, which is **half** the venue's
    /// 200, because the venue documents neither whether that 200 is per connection or per
    /// IP, and the collector sharing this venue measured neither (§2.5).
    #[test]
    fn the_worst_repair_rate_still_fits_the_message_budget() {
        let markets = 15; // the deployed symbol set
        let repairs_per_min = 60 / REPAIR_COOLDOWN.as_secs().max(1) as usize;
        let repair_cost = markets * repair_frames(0).len() * repairs_per_min;
        let subscribe_cost = subscription_frames(&vec![crv(); markets]).len();
        let keepalives = 60 / super::PING_INTERVAL.as_secs().max(1) as usize;

        assert!(
            repair_cost + subscribe_cost + keepalives <= CLIENT_MSG_BUDGET_PER_MIN,
            "{markets} markets repairing every {REPAIR_COOLDOWN:?} costs {repair_cost} \
             messages a minute; with a reconnect's {subscribe_cost} subscribes and \
             {keepalives} keepalives that is over the {CLIENT_MSG_BUDGET_PER_MIN} this \
             connector allows itself"
        );
        const { assert!(CLIENT_MSG_BUDGET_PER_MIN * 2 <= VENUE_MSG_BUDGET_PER_MIN) };
    }

    /// A repair the budget will not pay for is **deferred, loudly, and still owed** — not
    /// dropped. A dropped repair is a market publishing diffs against a mirror that has
    /// drifted, for ever, because a chain that broke once may chain perfectly afterwards.
    #[tokio::test]
    async fn a_repair_the_budget_refuses_is_kept_and_retried() {
        let (mut stream, _ev_rx) = stream(&["CRV"], &[crv()]);
        let mut sink = RecordingSink::<LighterError>::default();
        let now = Instant::now();

        stream.state.track(&[crv()]);
        stream.pending_repairs.insert(36);
        stream.budget.try_spend(CLIENT_MSG_BUDGET_PER_MIN, now);
        stream.flush_repairs(&mut sink, now).await.unwrap();
        assert!(sink.sent.is_empty(), "{:?}", sink.sent);
        assert_eq!(stream.state.counts().repairs_deferred, 1);
        assert!(stream.pending_repairs.contains(&36), "still owed");

        // A minute later the window has slid, and the repair goes out.
        let later = now + Duration::from_secs(61) + REPAIR_COOLDOWN;
        stream.flush_repairs(&mut sink, later).await.unwrap();
        assert_eq!(sink.sent, repair_frames(36));
        assert!(stream.pending_repairs.is_empty());
        assert_eq!(stream.state.counts().repairs_sent, 1);
    }

    /// Data for a market this connector does not track cannot be turned into events — there
    /// is no tick size to interpret it with. Counted rather than dropped in silence.
    #[test]
    fn data_for_an_untracked_market_is_counted_not_guessed_at() {
        let mut state = MarketState::default();
        assert!(feed(&mut state, ORDER_BOOK_SNAPSHOT_CRV).events.is_empty());
        assert!(feed(&mut state, TRADE_CRV).events.is_empty());
        assert_eq!(state.counts().untracked_market_frames, 2);
    }

    /// **Venue-verbatim: an unmodelled frame is skipped and counted, never fatal.** `ticker`
    /// is the dangerous one — it carries a bare `nonce` and no `begin_nonce`, so a backend
    /// that read it as a book would report a break, and every break costs a repair.
    #[test]
    fn control_and_unmodelled_frames_produce_no_events_and_no_repairs() {
        let mut state = tracked(&[crv()]);
        for text in [
            CONNECTED,
            PONG,
            TICKER_BTC,
            HEIGHT,
            ERROR_ALREADY_SUBSCRIBED,
        ] {
            let handled = feed(&mut state, text);
            assert!(handled.events.is_empty(), "{text}");
            assert!(handled.repairs.is_empty(), "{text}");
        }
        assert_eq!(state.counts().venue_errors, 1);
        assert_eq!(state.counts().unmodelled_frames, 2);
    }

    /// Bytes this backend cannot read are counted and logged, never fatal: the connector's
    /// panic hook is `exit(1)`, so one bad frame must not take every bot's market data down.
    #[tokio::test]
    async fn a_frame_that_cannot_be_read_does_not_end_the_connection() {
        let (mut stream, _ev_rx) = stream(&["CRV"], &[crv()]);
        let mut sink = RecordingSink::<LighterError>::default();

        stream
            .serve(
                &mut sink,
                &mut read_frames(vec![
                    "not json".to_string(),
                    ORDER_BOOK_SNAPSHOT_CRV.to_string(),
                ]),
            )
            .await
            .unwrap_err();

        assert_eq!(stream.state.counts().malformed_frames, 1);
        assert_eq!(
            stream.state.counts().snapshots,
            1,
            "the good frame still landed"
        );
    }

    /// A bot registering against an already-running connector is handed the whole book, keyed
    /// by its symbol — the `AGENTS.md` §4.1 trap, which on this venue is not a corner case
    /// because the snapshot comes once per subscription.
    #[test]
    fn a_registration_restates_that_symbols_whole_book() {
        let mut state = tracked(&[crv(), hype()]);
        feed(&mut state, ORDER_BOOK_SNAPSHOT_CRV);

        let events = state.restate("CRV", 11);
        assert_eq!(events.len(), 79);
        assert!(events.iter().all(|(symbol, _)| symbol == "CRV"));
        assert!(
            events
                .iter()
                .all(|(_, e)| e.is(LOCAL_BID_DEPTH_EVENT) || e.is(LOCAL_ASK_DEPTH_EVENT)),
            "LiveBot applies nothing else"
        );
        assert_eq!(events[0].1.local_ts, 11);

        // A market with no data yet, and one that is not tracked at all, say nothing.
        assert!(state.restate("HYPE", 11).is_empty());
        assert!(state.restate("SUI", 11).is_empty());
        assert_eq!(state.restate_all(11).len(), 79);
    }

    /// Re-tracking a market on reconnect must not throw its mirror away: the mirror is what
    /// the bot's book was built from, and a fresh one would leave every level the bot holds
    /// with nothing able to remove it.
    #[test]
    fn tracking_a_market_again_keeps_its_mirror() {
        let mut state = tracked(&[crv()]);
        feed(&mut state, ORDER_BOOK_SNAPSHOT_CRV);

        state.track(&[crv()]);
        state.connection_restarted();
        let handled = feed(&mut state, ORDER_BOOK_SNAPSHOT_CRV);
        assert!(
            handled.events.is_empty(),
            "a kept mirror has nothing new to say about the same snapshot: {:?}",
            handled.events
        );
    }

    /// **`offset` is not a sequence number, and this replays the recording that says so.**
    ///
    /// The field looks exactly like one — it is monotone inside a connection and it sits
    /// beside the nonces — and it is API-server-local: it **jumps backwards** on every
    /// resubscribe (design note §2.2, measured on HYPE: 7 440 494 → 7 411 097 and
    /// 3 301 306 → 3 284 089, both at a `subscribed/order_book`). A connector that keyed
    /// resync on it would repair itself into the venue's rate limit every time it
    /// resubscribed, and one that keyed on nothing would never repair at all. Neither is
    /// visible in any log, which is why this is pinned rather than remembered.
    ///
    /// The same pass verifies the two things the design note asks to be verified against
    /// real bytes rather than assumed:
    ///
    /// * `begin_nonce(N+1) == nonce(N)` holds with **zero** breaks over the whole day;
    /// * the delta semantics — a level named with a size replaces, a level named with zero
    ///   is gone — reconstruct a book that is never crossed and that this mirror never has
    ///   to refuse;
    /// * **and the reconstructed touch is the venue's own.** The recording carries the
    ///   `ticker` channel beside the book, and a ticker states the touch until the next one,
    ///   so a book frame stamped inside that interval must leave this mirror holding exactly
    ///   that touch. It is an oracle this connector cannot fake: a mirror that mishandled
    ///   deltas — reading a diff as a replacement of the side, say — would still be uncrossed
    ///   and would still chain perfectly, and would disagree here immediately.
    ///
    ///   **The oracle is approximate, and its residual is the venue's, not the mirror's.**
    ///   Measured over the day below: **12 074 agreements against 2 disagreements**, and both
    ///   disagreements are the `ticker` channel lagging the book on an ask move — at
    ///   `1785369600052848` the book moved the ask 0.20901 → 0.20914 and the ticker went on
    ///   reporting 0.20901 for **1.9 s**, then reported 0.20915, which is what the mirror
    ///   already held. The chain is unbroken across the whole window, so this is two
    ///   independent feeds at two cadences, not a reconstruction error. Hence a rate rather
    ///   than an equality — a delta-semantics bug disagrees on nearly every comparison, not
    ///   on two in twelve thousand.
    ///
    /// **Measured, 2026-07-31**, against `crv_20260730.gz` — 248 696 frames, 208 182 deltas,
    /// 3 snapshots (so two resubscribes): **2 offset rollbacks, both at a resubscribe and
    /// nowhere else; 0 chain breaks; 0 refused frames; 0 collapsed levels; 0 sub-lot levels;
    /// 162 replayed prints dropped**, 558 691 depth events published. The whole of §2.2 and
    /// §3.1, on the venue's own bytes.
    ///
    /// **Ignored because the recording is not in git**: it is 1.8–184 MB a symbol a day,
    /// under `/home/storage/hft-data/lighter/`. Run it against one, uncompressed:
    ///
    /// ```text
    /// zcat /home/storage/hft-data/lighter/crv_20260729.gz > /tmp/crv.jsonl
    /// LIGHTER_RECORDING=/tmp/crv.jsonl \
    ///   cargo test -p connector --bins lighter::public_stream -- --ignored --nocapture
    /// ```
    #[test]
    #[ignore = "replays a recorded mainnet day; set LIGHTER_RECORDING to an uncompressed dump"]
    fn offset_goes_backwards_on_a_resubscribe_while_begin_nonce_does_not() {
        let Ok(path) = std::env::var("LIGHTER_RECORDING") else {
            eprintln!("SKIPPED: set LIGHTER_RECORDING to an uncompressed collector dump");
            return;
        };
        let text = std::fs::read_to_string(&path).unwrap();
        let frames: Vec<&str> = text
            .lines()
            // The collector writes `<local_ts_ns> <json>`; a bare dump of frames also works.
            .map(|line| {
                line.split_once(" {")
                    .map_or(line, |(_, rest)| &line[line.len() - rest.len() - 1..])
            })
            .filter(|line| line.starts_with('{'))
            .collect();

        // The venue quantises every price and size to its catalog decimals, so the grid can
        // be read off its own strings rather than fetched — this test must not need a
        // network to say something about bytes already on disk.
        let (mut price_decimals, mut size_decimals) = (0usize, 0usize);
        for frame in &frames {
            let Ok(Frame::BookSnapshot(book) | Frame::BookUpdate(book)) = parse_frame(frame) else {
                continue;
            };
            for raw in frame.split(r#""price":""#).skip(1) {
                price_decimals = price_decimals.max(decimals(raw));
            }
            for raw in frame.split(r#""size":""#).skip(1) {
                size_decimals = size_decimals.max(decimals(raw));
            }
            let _ = book;
        }
        let info = MarketInfo {
            symbol: "RECORDED".to_string(),
            market_id: frames
                .iter()
                .find_map(|f| match parse_frame(f) {
                    Ok(Frame::BookSnapshot(b) | Frame::BookUpdate(b)) => Some(b.market_id),
                    _ => None,
                })
                .expect("the recording has no order_book frames"),
            price_decimals: price_decimals as i64,
            size_decimals: size_decimals as i64,
        };
        eprintln!("replaying {} frames as {info:?}", frames.len());

        let mut state = tracked(std::slice::from_ref(&info));
        let mut snapshots = 0u64;
        let mut rollbacks = 0u64;
        let mut last_offset: Option<i64> = None;
        let mut events = 0u64;
        let (mut agreed, mut disagreed) = (0u64, 0u64);
        // The venue's last touch, and the check it owes: `(book time, that ticker, the touch
        // this mirror held when the book frame landed)`, settled by the next ticker.
        let mut last_ticker: Option<Ticker> = None;
        let mut pending: Option<(i64, Ticker, Option<Touch>)> = None;
        for frame in &frames {
            let parsed = match parse_frame(frame) {
                Ok(parsed) => parsed,
                Err(error) => panic!("could not read a recorded frame: {error}\n{frame}"),
            };
            let snapshot = matches!(parsed, Frame::BookSnapshot(_));
            let is_book = snapshot || matches!(parsed, Frame::BookUpdate(_));
            let time_us = frame_time_us(frame);
            snapshots += u64::from(snapshot);

            if is_book {
                let offset = serde_json::from_str::<serde_json::Value>(frame).unwrap()
                    ["order_book"]["offset"]
                    .as_i64()
                    .expect("every order_book frame carries an offset");
                if last_offset.is_some_and(|last| offset < last) {
                    rollbacks += 1;
                    assert!(
                        snapshot,
                        "offset only ever goes backwards at a resubscribe: {last_offset:?} -> \
                         {offset}"
                    );
                }
                last_offset = Some(offset);
            }

            // The local receive clock the recording was made with, so the mirror's
            // plausibility guard compares like with like.
            let handled = state.on_frame(parsed, time_us * 1_000, Instant::now());
            events += handled.events.len() as u64;
            assert!(
                handled.repairs.is_empty(),
                "the chain broke or a frame was refused: {frame}"
            );
            if is_book {
                // The venue's own touch, compared at an instant it is known to describe.
                // The two channels run at different cadences — the book batches at ~50 ms,
                // the ticker is event-driven at ~15 ms — and their engine stamps never
                // coincide (measured: 0 of 3 111 ticker frames in a recorded day). What
                // *does* bracket them: a ticker at T states the touch for every instant
                // until the next ticker at T', so a book frame stamped B with T <= B < T'
                // must leave this mirror holding exactly that ticker's touch.
                pending = last_ticker.map(|ticker| (time_us, ticker, state.touch(info.market_id)));
            }
            if let Some(t) = ticker_time(frame) {
                let (now_us, _, _) = t;
                if let Some((book_us, (ticker_us, bid, ask), mirrored)) = pending
                    && now_us > book_us
                    && ticker_us <= book_us
                {
                    if mirrored == Some((Some(bid), Some(ask))) {
                        agreed += 1;
                    } else {
                        disagreed += 1;
                        eprintln!(
                            "touch disagreed at book {book_us} (ticker {ticker_us}): mirror \
                             {mirrored:?} vs venue {bid}/{ask}"
                        );
                    }
                    pending = None;
                }
                last_ticker = Some(t);
            }
        }

        let counts = state.counts();
        eprintln!(
            "{} frames, {snapshots} snapshots, {} deltas, {events} depth events, \
             {rollbacks} offset rollbacks, touch agreed {agreed} / disagreed {disagreed}, \
             counts {counts:?}",
            frames.len(),
            counts.deltas,
        );
        assert!(
            agreed > 1_000,
            "only {agreed} touch comparisons lined up, which is too few to say anything \
             about the delta semantics; use a recording that carries the ticker channel"
        );
        assert!(
            disagreed * 200 <= agreed,
            "the reconstructed touch disagreed with the venue's {disagreed} times in {} — \
             far above the measured residual of 2 in 12 076, which is the ticker channel \
             lagging the book. Something is wrong with how deltas are applied.",
            agreed + disagreed
        );
        assert_eq!(
            counts.chain_breaks, 0,
            "begin_nonce chained across the whole day"
        );
        assert_eq!(counts.refused_frames(), 0, "no frame had to be refused");
        // A venue-integrity check, and only that: the mirror's grid is derived from the same
        // decimals the venue quantises to, so zero is what a healthy day looks like and a
        // non-zero count would mean the catalog no longer describes the feed. Neither counter
        // can say anything about the tick and lot the *bot* registered — see `MirrorCounts`.
        assert_eq!(
            counts.collapsed_levels, 0,
            "the venue quoted two prices its own price decimals cannot tell apart"
        );
        assert_eq!(
            counts.sub_lot_levels, 0,
            "the venue quoted a size below its own size decimals"
        );
        if snapshots > 1 {
            assert!(
                rollbacks > 0,
                "this recording resubscribed {} times and offset never went backwards; the \
                 field's behaviour has changed and §2.2 needs re-measuring",
                snapshots - 1
            );
        } else {
            eprintln!(
                "NOTE: this recording holds one snapshot, so it contains no resubscribe and \
                 says nothing about the offset rollback. Use a day with a reconnect."
            );
        }
    }

    /// Decimal places in a venue number that starts a `"…"` JSON string.
    fn decimals(raw: &str) -> usize {
        let value = raw.split('"').next().unwrap_or_default();
        value.split_once('.').map_or(0, |(_, frac)| frac.len())
    }

    /// `(engine time in µs, best bid, best ask)` — the venue's own touch, as `ticker` states
    /// it.
    type Ticker = (i64, f64, f64);

    /// A mirrored touch: either side may be missing on a one-sided book.
    type Touch = (Option<f64>, Option<f64>);

    /// `(engine time in µs, best bid, best ask)` of a `ticker` frame, and `None` for
    /// anything else. The venue's own touch — an oracle this connector cannot fake.
    fn ticker_time(frame: &str) -> Option<Ticker> {
        if !frame.contains(r#""type":"update/ticker""#) {
            return None;
        }
        let raw: serde_json::Value = serde_json::from_str(frame).ok()?;
        let price =
            |side: &str| -> Option<f64> { raw["ticker"][side]["price"].as_str()?.parse().ok() };
        Some((raw["last_updated_at"].as_i64()?, price("b")?, price("a")?))
    }

    /// The envelope's `last_updated_at`, in microseconds.
    fn frame_time_us(frame: &str) -> i64 {
        serde_json::from_str::<serde_json::Value>(frame).unwrap()["last_updated_at"]
            .as_i64()
            .unwrap_or_default()
    }

    /// The counters answer "is this feed mostly working?" only as a rate. Cumulative totals
    /// cannot: a connector that refused a thousand frames out of ten million is healthy, and
    /// one that refused a thousand out of a thousand is publishing a frozen book — which is
    /// what a misread timestamp unit looks like, with no other symptom (§4.4).
    #[test]
    fn a_feed_refusing_most_of_its_frames_is_reported_not_just_counted() {
        let quiet = FeedCounts {
            deltas: 100,
            stale_frames: 1,
            ..Default::default()
        };
        assert_eq!(degraded(&FeedCounts::default(), &quiet), None);

        let degrading = FeedCounts {
            deltas: 130,
            stale_frames: 21,
            ..quiet
        };
        assert_eq!(degraded(&quiet, &degrading), Some((20, 30)));

        // Below the floor a ratio is noise.
        let barely = FeedCounts {
            deltas: 105,
            stale_frames: 5,
            ..quiet
        };
        assert_eq!(degraded(&quiet, &barely), None);

        // Trade events are not book frames and must not dilute the rate.
        let busy_tape = FeedCounts {
            deltas: 120,
            crossed_frames: 20,
            trade_events: 10_000,
            ..quiet
        };
        assert_eq!(degraded(&quiet, &busy_tape), Some((20, 20)));
    }
}
