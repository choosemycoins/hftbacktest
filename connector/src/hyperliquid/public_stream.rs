//! The public market-data WebSocket: one connection, all coins.
//!
//! The connection policy is not generic resilience boilerplate — every timeout here was
//! bought with an incident on this venue:
//!
//! * **A 15 s cap on the dial.** A half-open SYN once hung a task for three hours; the
//!   dial cannot be allowed to be the thing that never returns.
//! * **A 90 s any-frame idle detector, checked every 15 s.** Hyperliquid sits behind a
//!   CDN and drops roughly nine connections a day per socket, most of them as half-open
//!   sockets that never deliver a close frame. Only silence reveals them. Pongs count as
//!   frames: they are the sole evidence of liveness in a market with no trades.
//! * **A timeout on every write.** A wedged write half once hung for fifteen hours.
//! * **Re-subscription from the shared symbol set on every connect**, never from the
//!   broadcast alone — see `AGENTS.md` §4.2 and the test in this file.
//! * **A bound on the `/info` round trip**, which runs inside the read loop and would
//!   otherwise stall the keepalive for as long as the venue cared to take.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
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
        mpsc::UnboundedSender,
    },
    time,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Bytes, Message, client::IntoClientRequest},
};
use tracing::{error, info, warn};

use crate::{
    connector::PublishEvent,
    hyperliquid::{
        HyperliquidError,
        L2BookMode,
        depth::DepthMirror,
        msg::{Frame, parse_frame},
        publish_error,
        rest::{SymbolInfo, resolve_symbols},
        trades::{TradeDedup, trade_event},
    },
    utils::{BackoffStrategy, ExponentialBackoff},
};

/// A dial that has not completed by now is not going to.
pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
/// No frame of any kind for this long means the socket is gone, whatever it claims.
pub(crate) const IDLE_TIMEOUT: Duration = Duration::from_secs(90);
/// How often the idle detector looks.
pub(crate) const IDLE_CHECK_INTERVAL: Duration = Duration::from_secs(15);
/// Application-level keepalive. The venue drops a connection after 60 s of silence, and
/// its liveness protocol is `{"method":"ping"}` → `{"channel":"pong"}`, not RFC6455 pings.
pub(crate) const PING_INTERVAL: Duration = Duration::from_secs(30);
/// A write that has not been accepted by now means the write half is wedged.
pub(crate) const WRITE_TIMEOUT: Duration = Duration::from_secs(10);
/// The whole coin-resolution round trip, however many perp dexes it has to ask about.
///
/// It is awaited from inside the read loop, so while it is pending nothing else in that
/// loop runs — including the keepalive. `rest::INFO_TIMEOUT` bounds one dex at 15 s, so an
/// unbounded resolve across two dexes could hold the loop for 30 s and, landing just before
/// a keepalive was due, push the connection to the venue's 60 s silence limit. Bounded
/// below the keepalive interval instead, and a resolve that exceeds it drops the connection
/// — which costs ~1.2 s on this venue and is retried with everything re-derived.
const RESOLVE_BUDGET: Duration = Duration::from_secs(20);
/// Reconnect ladder. Measured recovery after a CDN drop is ~1.2 s, so the floor is 1 s.
pub(crate) const BACKOFF_MIN: Duration = Duration::from_secs(1);
pub(crate) const BACKOFF_MAX: Duration = Duration::from_secs(30);
/// How often the feed counters are logged.
pub(crate) const COUNTER_LOG_INTERVAL: Duration = Duration::from_secs(60);
/// Share of a reporting interval's frames that may be refused before the feed is reported
/// as degraded rather than merely counted.
const REFUSED_RATE_ALARM: f64 = 0.05;
/// Below this many frames in an interval the share is noise.
const REFUSED_RATE_MIN_FRAMES: u64 = 20;
/// The keepalive the venue understands.
pub(crate) const PING_FRAME: &str = r#"{"method":"ping"}"#;

/// What has been observed since the connector started.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FeedCounts {
    pub snapshots: u64,
    pub bbo_frames: u64,
    pub depth_events: u64,
    pub trade_events: u64,
    /// Fills dropped because the venue replayed them on a (re)subscribe.
    pub replayed_trades: u64,
    /// Frames whose coin is not tracked — they cannot be interpreted without a tick size.
    pub untracked_coin_frames: u64,
    /// `{"channel":"error"}` frames. The connection survives these.
    pub venue_errors: u64,
    /// Frames dropped for being older than the last one applied to the same coin.
    pub stale_frames: u64,
    /// Frames dropped for carrying an exchange time the local clock says cannot be real.
    pub implausible_frames: u64,
    /// `l2Book` frames dropped for contradicting themselves.
    pub crossed_frames: u64,
    /// Venue levels summed into another because they shared a tick. Non-zero means the tick
    /// derived from `szDecimals` is coarser than the prices the venue is sending.
    pub collapsed_levels: u64,
    /// Levels whose size rounded below one lot and were mirrored as absent.
    pub sub_lot_levels: u64,
    /// Fills whose exchange time could not be expressed in nanoseconds.
    pub malformed_trades: u64,
}

impl FeedCounts {
    /// Frames the mirrors refused, of any kind.
    fn refused_frames(&self) -> u64 {
        self.stale_frames + self.implausible_frames + self.crossed_frames
    }

    /// Depth frames seen, refused or not.
    fn depth_frames(&self) -> u64 {
        self.snapshots + self.bbo_frames
    }
}

/// `Some((refused, seen))` when the interval between two counter samples refused enough
/// frames to be worth an alarm rather than a line in the periodic log.
///
/// A raw cumulative counter cannot answer the question that matters — *is this feed mostly
/// working?* — and the failure it watches for produces no other signal at all. The
/// monotonic gate is shared by `bbo` and `l2Book`, whose `time` fields come from different
/// venue subsystems; if the `bbo` clock ran systematically ahead, every snapshot would be
/// refused and the bot would run on a touch-only book with the deeper levels frozen at
/// whatever the first snapshot held, indefinitely, with nothing above `info!` to show for
/// it. The evidence that this does not happen is 644 frames over ~60 s of quiet testnet,
/// which is thin; this makes the assumption self-reporting on mainnet instead.
pub fn degraded(previous: &FeedCounts, current: &FeedCounts) -> Option<(u64, u64)> {
    let seen = current
        .depth_frames()
        .saturating_sub(previous.depth_frames());
    let refused = current
        .refused_frames()
        .saturating_sub(previous.refused_frames());
    (seen >= REFUSED_RATE_MIN_FRAMES && refused as f64 > seen as f64 * REFUSED_RATE_ALARM)
        .then_some((refused, seen))
}

/// The per-coin market state: the book mirror the published events have built, and the
/// window of fill ids already published.
///
/// Deliberately owned by the stream **task**, not by a connection: both halves have to
/// survive a reconnect. The mirror is what the bot's book was built from, and the fill
/// window only ever does anything *because* of a reconnect.
#[derive(Default)]
pub struct MarketState {
    mirrors: HashMap<String, DepthMirror>,
    dedup: TradeDedup,
    counts: FeedCounts,
}

impl MarketState {
    /// Starts tracking coins, keeping the mirror of any coin already tracked.
    ///
    /// Keeping it is the reconnect case and it is deliberate: the mirror describes the book
    /// the *bot* holds, which a connector reconnect does not disturb, so a fresh one would
    /// leave every level above the new touch in place — a permanently crossed book. A bot
    /// registering is the opposite case and is served by [`MarketState::restate`].
    pub fn track(&mut self, symbols: &[SymbolInfo]) {
        for info in symbols {
            self.mirrors
                .entry(info.wire.clone())
                .or_insert_with(|| DepthMirror::new(info.tick_size(), info.lot_size()));
        }
    }

    /// The whole current book of one coin, as events, for a bot that has just registered
    /// it. Empty if the coin is not tracked or nothing has arrived yet.
    ///
    /// See [`DepthMirror::restate`] for why a registering bot needs this and gets nothing
    /// without it.
    pub fn restate(&self, coin: &str, local_ts: i64) -> Vec<(String, Event)> {
        let Some(mirror) = self.mirrors.get(coin) else {
            return Vec::new();
        };
        mirror
            .restate(local_ts)
            .into_iter()
            .map(|event| (coin.to_string(), event))
            .collect()
    }

    /// Restates every tracked coin. Used when the registration broadcast lagged and the
    /// coin names were lost: the set of coins is small and a restatement is idempotent, so
    /// restating all of them costs less than guessing which one was missed.
    pub fn restate_all(&self, local_ts: i64) -> Vec<(String, Event)> {
        self.mirrors
            .keys()
            .flat_map(|coin| self.restate(coin, local_ts))
            .collect()
    }

    pub fn counts(&self) -> FeedCounts {
        let mut counts = self.counts;
        for mirror in self.mirrors.values() {
            let mirror = mirror.counts();
            counts.stale_frames += mirror.stale;
            counts.implausible_frames += mirror.implausible;
            counts.crossed_frames += mirror.crossed;
            counts.collapsed_levels += mirror.collapsed;
            counts.sub_lot_levels += mirror.sub_lot;
        }
        counts
    }

    /// Turns one inbound frame into the events to publish, in publication order.
    pub fn on_frame(&mut self, frame: Frame, local_ts: i64) -> Vec<(String, Event)> {
        match frame {
            Frame::L2Book(book) => {
                self.counts.snapshots += 1;
                let Some(mirror) = self.mirrors.get_mut(&book.coin) else {
                    self.untracked(&book.coin);
                    return Vec::new();
                };
                let events =
                    mirror.on_snapshot(&book.levels[0], &book.levels[1], book.time, local_ts);
                self.counts.depth_events += events.len() as u64;
                events
                    .into_iter()
                    .map(|event| (book.coin.clone(), event))
                    .collect()
            }
            Frame::Bbo(quote) => {
                self.counts.bbo_frames += 1;
                let Some(mirror) = self.mirrors.get_mut(&quote.coin) else {
                    self.untracked(&quote.coin);
                    return Vec::new();
                };
                let events = mirror.on_bbo(&quote.bbo, quote.time, local_ts);
                self.counts.depth_events += events.len() as u64;
                events
                    .into_iter()
                    .map(|event| (quote.coin.clone(), event))
                    .collect()
            }
            Frame::Trades(trades) => {
                let mut out = Vec::with_capacity(trades.len());
                for trade in trades {
                    if !self.mirrors.contains_key(&trade.coin) {
                        self.untracked(&trade.coin);
                        continue;
                    }
                    if !self.dedup.accept(&trade.coin, trade.tid) {
                        continue;
                    }
                    let Some(event) = trade_event(&trade, local_ts) else {
                        self.counts.malformed_trades += 1;
                        warn!(
                            coin = %trade.coin,
                            time = trade.time,
                            "Refusing a Hyperliquid fill whose exchange time cannot be \
                             expressed in nanoseconds."
                        );
                        continue;
                    };
                    out.push((trade.coin.clone(), event));
                }
                self.counts.replayed_trades = self.dedup.dropped();
                self.counts.trade_events += out.len() as u64;
                out
            }
            Frame::SubscriptionResponse(ack) => {
                info!(subscription = %ack, "Hyperliquid acknowledged a subscription.");
                Vec::new()
            }
            Frame::Error(message) => {
                // The venue answers an unknown subscription *type* with this and keeps the
                // connection; an unknown *coin* instead closes the socket without a word.
                // Neither is data, and neither may be swallowed.
                self.counts.venue_errors += 1;
                error!(%message, "Hyperliquid reported an error on the public stream.");
                Vec::new()
            }
            Frame::Pong => Vec::new(),
            // Account channels, on the wrong socket. The private stream owns them; seeing
            // one here would mean a subscription was written to the public connection,
            // which is worth saying rather than silently dropping.
            Frame::OrderUpdates(_) | Frame::UserFills(_) => {
                warn!("An account frame arrived on the Hyperliquid public stream.");
                Vec::new()
            }
            Frame::Other(channel) => {
                warn!(%channel, "Ignoring an unhandled Hyperliquid channel.");
                Vec::new()
            }
        }
    }

    fn untracked(&mut self, coin: &str) {
        self.counts.untracked_coin_frames += 1;
        warn!(
            %coin,
            "Received data for a coin this connector does not track; it has no tick size \
             to interpret it with."
        );
    }
}

/// Which coins have been subscribed **on the current connection**.
///
/// A connection owns one of these and never inherits another's: `connect` builds a fresh
/// tracker, so every reconnect re-derives its subscriptions from the shared symbol set.
#[derive(Default)]
pub struct SubscriptionTracker {
    subscribed: HashSet<String>,
}

impl SubscriptionTracker {
    /// Registered coins that this connection has not subscribed yet, in a stable order.
    pub fn pending(&self, registered: &HashSet<String>) -> Vec<String> {
        let mut pending: Vec<String> = registered
            .difference(&self.subscribed)
            .cloned()
            .collect::<Vec<_>>();
        pending.sort();
        pending
    }

    pub fn mark(&mut self, symbols: &[String]) {
        for symbol in symbols {
            self.subscribed.insert(symbol.clone());
        }
    }
}

/// The subscribe frames for a set of coins: `bbo`, `l2Book` and `trades` each.
///
/// Split out so the wire shape can be asserted without a socket. It is the one thing about
/// this backend that no log line would contradict — the venue serves what it was asked for
/// and says nothing about the rest, which reads downstream as a coin that was simply quiet.
pub fn subscription_frames(symbols: &[SymbolInfo], l2_book: L2BookMode) -> Vec<String> {
    let mut frames = Vec::with_capacity(symbols.len() * 3);
    for info in symbols {
        for kind in ["bbo", "l2Book", "trades"] {
            let mut subscription = serde_json::Map::new();
            subscription.insert("type".into(), kind.into());
            // The dex prefix is part of the coin name: `test:ABC` goes across verbatim.
            subscription.insert("coin".into(), info.wire.as_str().into());
            if kind == "l2Book"
                && let Some(fast) = l2_book.fast_flag()
            {
                subscription.insert("fast".into(), fast.into());
            }
            frames.push(
                serde_json::json!({
                    "method": "subscribe",
                    "subscription": serde_json::Value::Object(subscription),
                })
                .to_string(),
            );
        }
    }
    frames
}

pub struct PublicStream {
    public_url: String,
    rest_url: String,
    l2_book: L2BookMode,
    symbols: Arc<Mutex<HashSet<String>>>,
    /// Both a wake-up and the name of the coin that was just registered. The shared set
    /// stays authoritative for *what to subscribe*; the payload is used only to decide
    /// whose book to restate, and a lost payload degrades to restating all of them.
    symbol_rx: Receiver<String>,
    /// Coins resolved against the venue's universe, shared with the order path.
    ///
    /// Resolution has to happen here — subscribing to an unlisted coin closes the socket,
    /// so this stream must ask before it subscribes — and the order path needs the same two
    /// answers: `szDecimals` for the price grid, and the universe index for the `a` field.
    /// Publishing them here rather than asking twice keeps one `/info` round trip per
    /// connect instead of two, and keeps the two halves from disagreeing about a coin.
    instruments: crate::hyperliquid::private_stream::SharedInstruments,
    ev_tx: UnboundedSender<PublishEvent>,
    state: MarketState,
}

impl PublicStream {
    pub fn new(
        public_url: String,
        rest_url: String,
        l2_book: L2BookMode,
        symbols: Arc<Mutex<HashSet<String>>>,
        symbol_rx: Receiver<String>,
        instruments: crate::hyperliquid::private_stream::SharedInstruments,
        ev_tx: UnboundedSender<PublishEvent>,
    ) -> Self {
        Self {
            public_url,
            rest_url,
            l2_book,
            symbols,
            symbol_rx,
            instruments,
            ev_tx,
            state: MarketState::default(),
        }
    }

    /// Connects, and keeps reconnecting. Never returns.
    ///
    /// The market state lives here, outside the connect loop, which is what makes the fill
    /// window and the book mirror survive a reconnect.
    pub async fn run(&mut self) {
        let mut backoff = ExponentialBackoff::with_bounds(BACKOFF_MIN, BACKOFF_MAX);
        let mut last_counts = FeedCounts::default();
        loop {
            let error = match self.connect().await {
                Ok(()) => HyperliquidError::ConnectionInterrupted,
                Err(error) => error,
            };
            // Counters go out with the disconnect, not only on the periodic tick: the
            // tick lives inside a connection, so on a venue that drops a socket every few
            // seconds the periodic log would never fire and the feed would be unobservable
            // exactly when it matters most.
            let counts = self.state.counts();
            error!(
                ?error,
                ?counts,
                "The Hyperliquid public stream disconnected."
            );
            self.report(&mut last_counts, counts);
            publish_error(&self.ev_tx, ErrorKind::ConnectionInterrupted, &error);
            let delay = backoff.backoff();
            info!(?delay, "Reconnecting to the Hyperliquid public stream.");
            time::sleep(delay).await;
        }
    }

    /// One connection, from dial to disconnect.
    async fn connect(&mut self) -> Result<(), HyperliquidError> {
        let request = self.public_url.as_str().into_client_request()?;
        let (ws_stream, _) = time::timeout(CONNECT_TIMEOUT, connect_async(request))
            .await
            .map_err(|_| HyperliquidError::ConnectTimeout(CONNECT_TIMEOUT))??;
        info!(url = %self.public_url, "Connected to the Hyperliquid public stream.");

        let (mut write, mut read) = ws_stream.split();
        let mut tracker = SubscriptionTracker::default();
        self.subscribe_pending(&mut write, &mut tracker).await?;

        let mut ping = time::interval(PING_INTERVAL);
        ping.reset();
        let mut idle_check = time::interval(IDLE_CHECK_INTERVAL);
        idle_check.reset();
        let mut counter_log = time::interval(COUNTER_LOG_INTERVAL);
        counter_log.reset();
        let mut last_frame = Instant::now();
        let mut last_counts = self.state.counts();
        // A closed broadcast answers instantly and for ever, so the arm has to stop being
        // polled: leaving it in place spins this loop at full tilt for the life of the
        // connection, burning a core and delaying every other task on the runtime.
        let mut registrations_closed = false;

        loop {
            select! {
                _ = ping.tick() => {
                    send(&mut write, Message::Text(PING_FRAME.into())).await?;
                }
                _ = idle_check.tick() => {
                    // Every frame counts, pongs included: a quiet market and a half-open
                    // socket look identical in the data, and only the keepalive tells them
                    // apart.
                    let idle = last_frame.elapsed();
                    if idle > IDLE_TIMEOUT {
                        return Err(HyperliquidError::IdleTimeout(idle));
                    }
                }
                _ = counter_log.tick() => {
                    let counts = self.state.counts();
                    self.report(&mut last_counts, counts);
                }
                registered = self.symbol_rx.recv(), if !registrations_closed => {
                    match registered {
                        Ok(coin) => {
                            // A bot registering an instrument gets a *fresh* fused depth in
                            // `main.rs` and a `SnapshotComplete` regardless, so unless it is
                            // handed the book it has none. See `DepthMirror::restate`.
                            let events = self.state.restate(&coin, local_now());
                            self.publish(events);
                            self.subscribe_pending(&mut write, &mut tracker).await?;
                        }
                        Err(RecvError::Lagged(missed)) => {
                            warn!(
                                missed,
                                "Registration wake-ups were dropped; restating every coin."
                            );
                            let events = self.state.restate_all(local_now());
                            self.publish(events);
                            self.subscribe_pending(&mut write, &mut tracker).await?;
                        }
                        Err(RecvError::Closed) => {
                            // Every sender is gone, so no new instrument can be registered.
                            // The existing subscriptions stay up; this arm stops being polled.
                            registrations_closed = true;
                        }
                    }
                }
                message = read.next() => {
                    last_frame = Instant::now();
                    match message {
                        Some(Ok(Message::Text(text))) => self.handle(&text),
                        Some(Ok(Message::Ping(_))) => {
                            send(&mut write, Message::Pong(Bytes::default())).await?;
                        }
                        Some(Ok(Message::Close(frame))) => {
                            return Err(HyperliquidError::ConnectionAbort(
                                frame.map(|f| f.to_string()).unwrap_or_default(),
                            ));
                        }
                        Some(Ok(_)) => {}
                        Some(Err(error)) => return Err(HyperliquidError::from(error)),
                        None => return Err(HyperliquidError::ConnectionInterrupted),
                    }
                }
            }
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
                "The Hyperliquid public feed is refusing frames; the bot's book below the \
                 touch may be frozen."
            ),
            None => info!(?counts, "Hyperliquid public feed counters."),
        }
        *last = counts;
    }

    /// Validates and subscribes every registered coin this connection has not subscribed.
    ///
    /// Validation is not optional politeness: subscribing to a coin the venue does not
    /// list closes the whole WebSocket, with no error frame and no close reason, taking
    /// every other coin's subscriptions with it. A coin that does not resolve is therefore
    /// refused — reported to the bots and skipped — while the rest still subscribe.
    ///
    /// Called from the read loop, so the `/info` round trip pauses frame processing *and*
    /// the keepalive; it is bounded by [`RESOLVE_BUDGET`] and preceded by a keepalive of
    /// its own for that reason. The alternative — a separate task — would need the write
    /// half to be shared, which is what leaves the collector's connection unable to
    /// subscribe anything after its first batch.
    async fn subscribe_pending<S>(
        &mut self,
        write: &mut S,
        tracker: &mut SubscriptionTracker,
    ) -> Result<(), HyperliquidError>
    where
        S: SinkExt<Message> + Unpin,
        HyperliquidError: From<S::Error>,
    {
        let registered = self.symbols.lock().unwrap().clone();
        let pending = tracker.pending(&registered);
        if pending.is_empty() {
            return Ok(());
        }

        // Keeps the connection alive across the round trip below, which holds this loop.
        send(write, Message::Text(PING_FRAME.into())).await?;

        // A failure here is the venue being unreachable or unwilling to answer, which is
        // transient: the caller drops the connection and retries, and the coins stay
        // unmarked so they are asked about again. A coin the venue does not *list* comes
        // back in `rejected` instead, and only that coin is refused.
        let (resolved, rejected) =
            time::timeout(RESOLVE_BUDGET, resolve_symbols(&self.rest_url, &pending))
                .await
                .map_err(|_| HyperliquidError::ResolveTimeout(RESOLVE_BUDGET))??;

        self.apply_resolution(write, tracker, &pending, resolved, rejected)
            .await
    }

    /// Subscribes what resolved, reports what did not, and records what may not be asked
    /// about again on this connection.
    ///
    /// Split from the round trip so the marking rule is testable without a network, because
    /// getting it wrong is invisible: a coin marked subscribed is never revisited, and a
    /// connection whose every coin was marked after failing to resolve stays up, answers
    /// pings, and publishes nothing until the process is restarted.
    async fn apply_resolution<S>(
        &mut self,
        write: &mut S,
        tracker: &mut SubscriptionTracker,
        pending: &[String],
        resolved: Vec<SymbolInfo>,
        rejected: Vec<(String, HyperliquidError)>,
    ) -> Result<(), HyperliquidError>
    where
        S: SinkExt<Message> + Unpin,
        HyperliquidError: From<S::Error>,
    {
        for (coin, error) in &rejected {
            error!(
                %coin,
                ?error,
                "Refusing to subscribe to a coin Hyperliquid does not list. Subscribing to \
                 one closes the entire WebSocket without an error frame, taking every other \
                 coin's subscriptions with it, so this coin gets no market data at all."
            );
            publish_error(&self.ev_tx, ErrorKind::CriticalConnectionError, error);
        }

        self.state.track(&resolved);
        // Published for the order path before the subscribe, so a coin is never subscribable
        // but un-orderable.
        {
            let mut instruments = self.instruments.lock().unwrap();
            for info in &resolved {
                instruments.insert(info.wire.clone(), info.clone());
            }
        }
        for frame in subscription_frames(&resolved, self.l2_book) {
            send(write, Message::Text(frame.into())).await?;
        }

        // Only what the venue gave a *listing verdict* on is remembered: that answer will
        // not change within a connection, so re-asking on every registration would be a
        // REST call per wake-up for nothing. Anything else is left pending and asked again,
        // which is the difference between a coin that recovers on the next wake-up and one
        // that is silently written off for the life of the process. A reconnect resets the
        // tracker and asks about everything again, which is how a coin listed later gets
        // picked up.
        let mut marked: Vec<String> = resolved.iter().map(|info| info.wire.clone()).collect();
        marked.extend(
            rejected
                .iter()
                .filter(|(_, error)| error.is_listing_verdict())
                .map(|(coin, _)| coin.clone()),
        );
        tracker.mark(&marked);

        if resolved.is_empty() {
            error!(
                ?pending,
                "Not one Hyperliquid coin could be subscribed; this connection is \
                 publishing no market data."
            );
        } else {
            info!(
                coins = ?resolved.iter().map(|s| s.wire.as_str()).collect::<Vec<_>>(),
                "Subscribed to the Hyperliquid public feeds."
            );
        }
        Ok(())
    }

    /// Publishes the events one frame produced, framed as a batch.
    ///
    /// The batch is what stops a bot from acting on a half-applied snapshot: `LiveBot`
    /// processes everything between `BatchStart` and `BatchEnd` before returning from
    /// `elapse` (`live/bot.rs:305–320`).
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

    fn handle(&mut self, text: &str) {
        let local_ts = local_now();
        let frame = match parse_frame(text) {
            Ok(frame) => frame,
            Err(error) => {
                error!(?error, %text, "Couldn't parse a Hyperliquid frame.");
                return;
            }
        };
        let events = self.state.on_frame(frame, local_ts);
        self.publish(events);
    }
}

/// The local receive time in nanoseconds. `0` means the clock could not be read, which
/// [`DepthMirror`] reads as "no reference to sanity-check the venue's time against".
pub(crate) fn local_now() -> i64 {
    Utc::now().timestamp_nanos_opt().unwrap_or(0)
}

/// Writes one message, or fails.
///
/// Unconditionally bounded: a wedged write half is silent, and an unbounded `send` on one
/// simply never returns — fifteen hours, once, on this venue.
pub(crate) async fn send<S>(write: &mut S, message: Message) -> Result<(), HyperliquidError>
where
    S: SinkExt<Message> + Unpin,
    HyperliquidError: From<S::Error>,
{
    time::timeout(WRITE_TIMEOUT, write.send(message))
        .await
        .map_err(|_| HyperliquidError::WriteTimeout(WRITE_TIMEOUT))?
        .map_err(HyperliquidError::from)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        pin::Pin,
        sync::{Arc, Mutex},
        task::{Context, Poll},
    };

    use futures_util::Sink;
    use hftbacktest::prelude::{
        LOCAL_ASK_DEPTH_EVENT,
        LOCAL_BID_DEPTH_EVENT,
        LOCAL_BUY_TRADE_EVENT,
    };
    use tokio::sync::{broadcast, mpsc::unbounded_channel};
    use tokio_tungstenite::tungstenite::Message;

    use crate::{
        connector::PublishEvent,
        hyperliquid::{
            HyperliquidError,
            L2BookMode,
            fixtures::{
                BBO_BTC_2,
                ERROR_FRAME,
                L2BOOK_FAST_BTC_1,
                PONG,
                SUBSCRIPTION_RESPONSE,
                TRADES_BTC_REPLAY,
            },
            msg::parse_frame,
            public_stream::{
                FeedCounts,
                MarketState,
                PublicStream,
                SubscriptionTracker,
                degraded,
                subscription_frames,
            },
            rest::SymbolInfo,
        },
    };

    /// The local receive clock the mirrors sanity-check the venue's time against. Has to be
    /// a real one: the fixtures carry real exchange times.
    const NOW_NS: i64 = 1_785_251_522_000 * 1_000_000;

    fn info(wire: &str, sz_decimals: u32) -> SymbolInfo {
        SymbolInfo {
            wire: wire.to_string(),
            dex: crate::hyperliquid::rest::dex_of(wire).to_string(),
            sz_decimals,
            asset_index: Some(0),
        }
    }

    fn set(symbols: &[&str]) -> HashSet<String> {
        symbols.iter().map(|s| s.to_string()).collect()
    }

    /// A write half that records what was written, so the subscribe path can be driven
    /// without a socket.
    #[derive(Default)]
    struct RecordingSink {
        sent: Vec<String>,
    }

    impl Sink<Message> for RecordingSink {
        type Error = HyperliquidError;

        fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
            self.sent.push(item.into_text().unwrap().to_string());
            Ok(())
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    fn stream(
        registered: &[&str],
    ) -> (
        PublicStream,
        tokio::sync::mpsc::UnboundedReceiver<PublishEvent>,
    ) {
        let (ev_tx, ev_rx) = unbounded_channel();
        let (symbol_tx, symbol_rx) = broadcast::channel(16);
        // Kept alive so the receiver does not see the channel close.
        std::mem::forget(symbol_tx);
        (
            PublicStream::new(
                "wss://example.invalid/ws".to_string(),
                "https://example.invalid".to_string(),
                L2BookMode::Fast,
                Arc::new(Mutex::new(set(registered))),
                symbol_rx,
                Default::default(),
                ev_tx,
            ),
            ev_rx,
        )
    }

    /// Three subscriptions per coin, and the dex prefix goes across verbatim — `test:ABC`
    /// is the wire coin name in full, not a coin plus a dex field. Getting that wrong is
    /// not a rejected subscription but a closed WebSocket that takes every other coin with
    /// it.
    #[test]
    fn every_coin_gets_bbo_l2book_and_trades() {
        let frames = subscription_frames(&[info("BTC", 5), info("test:ABC", 0)], L2BookMode::Fast);

        assert_eq!(frames.len(), 6);
        let parsed: Vec<serde_json::Value> = frames
            .iter()
            .map(|f| serde_json::from_str(f).unwrap())
            .collect();
        for coin in ["BTC", "test:ABC"] {
            for kind in ["bbo", "l2Book", "trades"] {
                assert!(
                    parsed.iter().any(|f| {
                        f["method"] == "subscribe"
                            && f["subscription"]["type"] == kind
                            && f["subscription"]["coin"] == coin
                    }),
                    "{coin} was never subscribed to {kind}: {frames:?}"
                );
            }
        }
    }

    /// Only the book subscription carries `fast`, and `slow` omits the field rather than
    /// sending `false`: the two are equivalent to the venue, and an omitted field keeps
    /// the frame identical to what a recording made before the fast feed existed used.
    #[test]
    fn only_the_book_subscription_carries_the_fast_flag() {
        let fast: Vec<serde_json::Value> = subscription_frames(&[info("BTC", 5)], L2BookMode::Fast)
            .iter()
            .map(|f| serde_json::from_str(f).unwrap())
            .collect();
        let book = fast
            .iter()
            .find(|f| f["subscription"]["type"] == "l2Book")
            .unwrap();
        assert_eq!(book["subscription"]["fast"], true);
        for frame in &fast {
            if frame["subscription"]["type"] != "l2Book" {
                assert!(frame["subscription"].get("fast").is_none(), "{frame}");
            }
        }

        let slow: Vec<serde_json::Value> = subscription_frames(&[info("BTC", 5)], L2BookMode::Slow)
            .iter()
            .map(|f| serde_json::from_str(f).unwrap())
            .collect();
        let book = slow
            .iter()
            .find(|f| f["subscription"]["type"] == "l2Book")
            .unwrap();
        assert!(book["subscription"].get("fast").is_none(), "{book}");
    }

    /// `AGENTS.md` §4.2: the other three backends broadcast the symbol list exactly once
    /// and build their subscribe frames from a `broadcast::Receiver` created inside the
    /// retry closure — so after a reconnect they are connected and subscribed to nothing,
    /// with the original `ConnectionInterrupted` as the only trace. The fix is to treat
    /// the shared set as authoritative and re-derive the subscriptions on every connect.
    ///
    /// A reconnect is modelled the way the production path does it — `connect` constructs
    /// `SubscriptionTracker::default()` — rather than by resetting one, so a change that
    /// hoisted the tracker out of `connect` fails this test instead of passing it.
    #[tokio::test]
    async fn every_symbol_is_resubscribed_after_a_reconnect() {
        let (mut stream, _ev_rx) = stream(&["BTC", "ETH"]);
        let mut sink = RecordingSink::default();
        let mut tracker = SubscriptionTracker::default();
        let registered = set(&["BTC", "ETH"]);

        assert_eq!(tracker.pending(&registered), vec!["BTC", "ETH"]);
        stream
            .apply_resolution(
                &mut sink,
                &mut tracker,
                &["BTC".to_string(), "ETH".to_string()],
                vec![info("BTC", 5), info("ETH", 4)],
                vec![],
            )
            .await
            .unwrap();
        assert_eq!(sink.sent.len(), 6, "three subscriptions a coin");
        assert!(tracker.pending(&registered).is_empty());

        // A bot registers a third coin while the connection is up.
        let registered = set(&["BTC", "ETH", "SOL"]);
        assert_eq!(tracker.pending(&registered), vec!["SOL"]);
        tracker.mark(&["SOL".to_string()]);
        assert!(tracker.pending(&registered).is_empty());

        // The connection drops. `connect` builds a new tracker, and everything must go out
        // again — including the coins registered long before this connection existed.
        let tracker = SubscriptionTracker::default();
        assert_eq!(tracker.pending(&registered), vec!["BTC", "ETH", "SOL"]);
    }

    /// **The blocker this rule prevents.** A coin left unresolved because the venue would
    /// not answer must stay pending. Marking it subscribed writes it off for the life of
    /// the connection: no later registration wake-up revisits it (`pending` is empty), the
    /// socket stays up because the keepalive keeps the idle detector happy, and the
    /// connector runs for hours connected and publishing nothing. Only a listing verdict —
    /// an answer that will not change — may be remembered.
    #[tokio::test]
    async fn a_coin_the_venue_would_not_answer_about_is_asked_about_again() {
        let (mut stream, _ev_rx) = stream(&["BTC", "ETH", "SOL"]);
        let mut sink = RecordingSink::default();
        let mut tracker = SubscriptionTracker::default();
        let pending = vec!["BTC".to_string(), "ETH".to_string(), "SOL".to_string()];

        stream
            .apply_resolution(
                &mut sink,
                &mut tracker,
                &pending,
                vec![info("BTC", 5)],
                vec![
                    (
                        "ETH".to_string(),
                        HyperliquidError::UnknownSymbol("ETH is not listed".into()),
                    ),
                    (
                        "SOL".to_string(),
                        HyperliquidError::UniverseUnavailable("/info answered HTTP 429".into()),
                    ),
                ],
            )
            .await
            .unwrap();

        let registered = set(&["BTC", "ETH", "SOL"]);
        assert_eq!(
            tracker.pending(&registered),
            vec!["SOL"],
            "only the coin the venue refused to answer about may be re-asked"
        );
    }

    /// The same rule at the scale that matters: a rate-limit window at startup rejects
    /// *every* coin. None may be marked, or the connection never recovers without a
    /// process restart.
    #[tokio::test]
    async fn a_venue_wide_failure_leaves_every_coin_pending() {
        let (mut stream, mut ev_rx) = stream(&["BTC", "ETH"]);
        let mut sink = RecordingSink::default();
        let mut tracker = SubscriptionTracker::default();
        let pending = vec!["BTC".to_string(), "ETH".to_string()];

        stream
            .apply_resolution(
                &mut sink,
                &mut tracker,
                &pending,
                vec![],
                pending
                    .iter()
                    .map(|coin| {
                        (
                            coin.clone(),
                            HyperliquidError::UniverseUnavailable("/info answered HTTP 503".into()),
                        )
                    })
                    .collect(),
            )
            .await
            .unwrap();

        assert!(
            sink.sent.is_empty(),
            "nothing to subscribe: {:?}",
            sink.sent
        );
        assert_eq!(tracker.pending(&set(&["BTC", "ETH"])), vec!["BTC", "ETH"]);
        // And the bots were told, once per coin.
        let mut errors = 0;
        while let Ok(PublishEvent::LiveEvent(event)) = ev_rx.try_recv() {
            if matches!(event, hftbacktest::types::LiveEvent::Error(_)) {
                errors += 1;
            }
        }
        assert_eq!(errors, 2);
    }

    #[test]
    fn a_snapshot_frame_becomes_depth_events_for_its_coin() {
        let mut state = MarketState::default();
        state.track(&[info("BTC", 5)]);

        let events = state.on_frame(parse_frame(L2BOOK_FAST_BTC_1).unwrap(), NOW_NS);
        assert_eq!(events.len(), 10);
        assert!(events.iter().all(|(symbol, _)| symbol == "BTC"));
        assert!(events[0].1.is(LOCAL_BID_DEPTH_EVENT));
        assert_eq!(events[0].1.local_ts, NOW_NS);

        // The bbo is fused into the same mirror rather than published as a BBO event,
        // which `LiveBot` would drop.
        let events = state.on_frame(parse_frame(BBO_BTC_2).unwrap(), NOW_NS);
        assert_eq!(events.len(), 1);
        assert_eq!((events[0].1.px, events[0].1.qty), (63460.0, 0.0));
    }

    #[test]
    fn a_trades_frame_becomes_trade_events_and_its_replay_does_not() {
        let mut state = MarketState::default();
        state.track(&[info("BTC", 5)]);

        let events = state.on_frame(parse_frame(TRADES_BTC_REPLAY).unwrap(), NOW_NS);
        assert_eq!(events.len(), 2);
        assert!(events[0].1.is(LOCAL_BUY_TRADE_EVENT));

        // The same frame again is what a reconnect delivers.
        let events = state.on_frame(parse_frame(TRADES_BTC_REPLAY).unwrap(), NOW_NS);
        assert!(events.is_empty());
        assert_eq!(state.counts().replayed_trades, 2);
    }

    /// Acks, pongs and the venue's own error frames are not market data. None of them may
    /// produce an event, and none of them may be treated as a parse failure.
    #[test]
    fn control_frames_produce_no_events() {
        let mut state = MarketState::default();
        state.track(&[info("BTC", 5)]);

        for text in [PONG, SUBSCRIPTION_RESPONSE, ERROR_FRAME] {
            assert!(
                state
                    .on_frame(parse_frame(text).unwrap(), NOW_NS)
                    .is_empty(),
                "{text}"
            );
        }
        assert_eq!(state.counts().venue_errors, 1);
    }

    /// Data for a coin the state does not track cannot be turned into events — there is no
    /// tick size to interpret it with. It is counted rather than dropped in silence.
    #[test]
    fn data_for_an_untracked_coin_is_counted_not_guessed_at() {
        let mut state = MarketState::default();
        let events = state.on_frame(parse_frame(L2BOOK_FAST_BTC_1).unwrap(), NOW_NS);
        assert!(events.is_empty());
        assert_eq!(state.counts().untracked_coin_frames, 1);
    }

    /// Re-tracking a coin on reconnect must not throw the mirror away: the mirror is what
    /// the bot's book was built from, and a fresh one would leave every level above the
    /// new touch in place — a permanently crossed book, with nothing in the log.
    ///
    /// This is the *reconnect* case. A bot registering is the opposite one and is served by
    /// `restate`, which is why the two are not the same code path.
    #[test]
    fn tracking_a_coin_again_keeps_its_mirror() {
        let mut state = MarketState::default();
        state.track(&[info("BTC", 5)]);
        state.on_frame(parse_frame(L2BOOK_FAST_BTC_1).unwrap(), NOW_NS);

        state.track(&[info("BTC", 5)]);
        let events = state.on_frame(parse_frame(L2BOOK_FAST_BTC_1).unwrap(), NOW_NS);
        assert!(
            events.is_empty(),
            "a kept mirror has nothing new to say about the same snapshot: {events:?}"
        );
    }

    /// …and the bot that registered while that mirror was already primed gets the whole
    /// book, named with its coin, because the diff has nothing left to tell it.
    #[test]
    fn a_registration_restates_that_coins_whole_book() {
        let mut state = MarketState::default();
        state.track(&[info("BTC", 5), info("ETH", 4)]);
        state.on_frame(parse_frame(L2BOOK_FAST_BTC_1).unwrap(), NOW_NS);

        let events = state.restate("BTC", 11);
        assert_eq!(events.len(), 10, "5 levels a side");
        assert!(events.iter().all(|(coin, _)| coin == "BTC"));
        assert!(
            events
                .iter()
                .all(|(_, e)| e.is(LOCAL_BID_DEPTH_EVENT) || e.is(LOCAL_ASK_DEPTH_EVENT)),
            "LiveBot applies nothing else"
        );
        assert_eq!(events[0].1.local_ts, 11);

        // A coin with no data yet, and a coin that is not tracked at all, say nothing.
        assert!(state.restate("ETH", 11).is_empty());
        assert!(state.restate("SOL", 11).is_empty());
        assert_eq!(state.restate_all(11).len(), 10);
    }

    /// The counters answer "is this feed mostly working?" only as a rate. Cumulative totals
    /// cannot: a connector that has refused a thousand frames out of ten million is
    /// healthy, and one that refused a thousand out of a thousand is publishing a frozen
    /// book — and the second is what a systematic clock skew between `bbo` and `l2Book`
    /// would look like, with no other symptom.
    #[test]
    fn a_feed_refusing_most_of_its_frames_is_reported_not_just_counted() {
        let quiet = FeedCounts {
            snapshots: 100,
            stale_frames: 1,
            ..Default::default()
        };
        assert_eq!(degraded(&FeedCounts::default(), &quiet), None);

        // The same absolute count, in an interval that only saw a few frames.
        let degrading = FeedCounts {
            snapshots: 130,
            stale_frames: 21,
            ..quiet
        };
        assert_eq!(degraded(&quiet, &degrading), Some((20, 30)));

        // Below the floor, a ratio is noise.
        let barely = FeedCounts {
            snapshots: 105,
            stale_frames: 5,
            ..quiet
        };
        assert_eq!(degraded(&quiet, &barely), None);

        // Every kind of refusal counts; they all mean the same thing downstream.
        let implausible = FeedCounts {
            bbo_frames: 50,
            implausible_frames: 25,
            crossed_frames: 5,
            ..quiet
        };
        assert_eq!(degraded(&quiet, &implausible), Some((30, 50)));
    }

    /// Trade events are not depth frames and must not dilute the rate.
    #[test]
    fn the_refusal_rate_is_measured_against_depth_frames_only() {
        let before = FeedCounts::default();
        let after = FeedCounts {
            snapshots: 20,
            stale_frames: 20,
            trade_events: 10_000,
            ..Default::default()
        };
        assert_eq!(degraded(&before, &after), Some((20, 20)));
    }

    /// A restated book must be uncrossed and applicable in the order it is published — it
    /// goes through the same fused depth in `main.rs` as every other event.
    #[test]
    fn a_restatement_is_ordered_bids_then_asks_and_never_crosses() {
        let mut state = MarketState::default();
        state.track(&[info("BTC", 5)]);
        state.on_frame(parse_frame(L2BOOK_FAST_BTC_1).unwrap(), NOW_NS);

        let events = state.restate("BTC", 0);
        let (mut best_bid, mut best_ask) = (f64::MIN, f64::MAX);
        for (_, event) in &events {
            if event.is(LOCAL_BID_DEPTH_EVENT) {
                best_bid = best_bid.max(event.px);
            } else if event.is(LOCAL_ASK_DEPTH_EVENT) {
                best_ask = best_ask.min(event.px);
            }
            assert!(best_bid < best_ask, "crossed at {event:?}");
        }
        assert_eq!((best_bid, best_ask), (63460.0, 63488.0));
    }
}
