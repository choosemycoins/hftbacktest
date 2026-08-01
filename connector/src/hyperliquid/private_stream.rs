//! The account WebSocket, the order path, and what happens after a reconnect.
//!
//! # Both subscriptions are keyed by the *master* account
//!
//! Actions are signed by the API wallet; state is read for the account that owns the
//! positions. `orderUpdates` and `userFills` subscribed with the agent's address are
//! accepted, acknowledged, and **silently produce nothing** — an account that appears never
//! to trade. Every address that leaves this module is therefore the configured
//! `account_address`, and the venue lowercases what it echoes, so comparisons are
//! case-insensitive.
//!
//! # A reconnect is the only chance to find out the truth
//!
//! `orderUpdates` has no replay: a lifecycle transition that happened while the socket was
//! down is simply gone, and nothing re-sends it. Hyperliquid's resting orders survive
//! disconnects, restarts and API-wallet expiry — there is no cancel-on-disconnect — so a
//! connector that does not re-ask comes back believing the account is flat while it holds
//! live orders. Every connect therefore re-subscribes **and** re-queries
//! `frontendOpenOrders` and `clearinghouseState` before the connector's view is trusted
//! (design note §5.10).
//!
//! `userFills` is the opposite case and needs a guard rather than a query: it replays its
//! whole history with `isSnapshot: true` on every subscribe, so without deduplication one
//! reconnect applies each live order's fills twice.
//!
//! # What the bot does *not* get, including on a cold start
//!
//! `SnapshotComplete` is published by the shared publish task the moment a registration is
//! processed, and `snapshot_ready` is a write-once latch in `LiveBot` with no reset
//! (`AGENTS.md` §4.4). After a reconnect it therefore says nothing at all, because it has
//! already fired — failing closed on `ConnectionInterrupted` stays the consumer's job.
//!
//! **A cold start is not automatically honest either, and the earlier text here claimed it
//! was.** `run_receive_task` publishes `RegisterInstrument` *before* calling
//! `Connector::register`, and this backend's registration handling is asynchronous — a
//! broadcast wake, then a REST round trip. The marker is emitted synchronously in between.
//! What the registering bot is handed is whatever the publish task has cached, so the marker
//! is honest about **position** only when this stream had already connected and reconciled,
//! which needs `coins` to list the traded symbols in the connector's own config. With
//! `coins` unset — it is optional — the first registration is answered with no position at
//! all and the bot's `position()` stays `0.0` until the registration's own reconciliation
//! lands a moment later. The connector warns at startup when that is the configuration.
//!
//! Orders are the better case: `GetOrders::orders()` reads this module's live map, so
//! whatever has been reconciled by then is included, and a connector that has just started
//! genuinely holds none of its own.

use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use futures_util::StreamExt;
use hftbacktest::types::{ErrorKind, LiveEvent, Order, Status};
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
use tracing::{debug, error, info, warn};

use crate::{
    connector::PublishEvent,
    hyperliquid::{
        HyperliquidError,
        exchange::{
            ActionOutcome,
            CancelAction,
            CancelByCloidAction,
            CancelByCloidWire,
            CancelWire,
            ExchangeClient,
            OrderAction,
        },
        msg::{Frame, OrderUpdate, UserFills, parse_frame},
        ordermanager::{FillDisposition, OrderExt, SharedOrderManager},
        public_stream::{
            CONNECT_TIMEOUT,
            IDLE_CHECK_INTERVAL,
            IDLE_TIMEOUT,
            PING_FRAME,
            PING_INTERVAL,
            ReconnectPolicy,
            local_now,
            send,
        },
        publish_error,
        rest::{OpenOrder, SymbolInfo, open_orders, positions, resolve_symbols},
    },
};

/// The whole reconciliation round trip, bounded for the same reason the public stream's
/// resolve is: it is awaited inside the read loop, so while it is pending the keepalive
/// does not run, against a venue that closes a silent connection after 60 s.
const RECONCILE_BUDGET: Duration = Duration::from_secs(20);

/// One symbol's sweep, bounded for the same reason [`RECONCILE_BUDGET`] is.
///
/// It is awaited inside the read loop and it can make three venue calls — resolve the asset
/// index, ask what is open, cancel — each with its own timeout. Unbounded, a slow venue
/// could hold the loop past the 60 s Hyperliquid gives a silent socket, so the keepalive
/// would stop and the connection would be dropped by the far end mid-sweep.
const CANCEL_BUDGET: Duration = Duration::from_secs(20);

/// How often a healthy connection re-asks the venue what it is holding.
///
/// A reconnect is not the only way to lose a lifecycle update: an `orderUpdates` frame
/// dropped on a socket that stays up is invisible, and without this the recovery path is
/// "wait for the next disconnect" — which on this venue averages a few hours. Two `/info`
/// requests every five minutes is nothing against the weight budget, and `/info` is metered
/// per IP rather than against the address-based order budget (design note §5.9), so it does
/// not compete with quoting.
const RECONCILE_INTERVAL: Duration = Duration::from_secs(300);

/// Coins resolved against the venue's universe, shared with the public stream.
///
/// The public stream is what resolves them — it must, because subscribing to an unlisted
/// coin closes the socket — and the order path only reads. A coin not in here yet cannot be
/// ordered on: without `szDecimals` there is no legal price grid, and without the asset
/// index the order would address a **different coin**. Both are refusals with a reason
/// rather than a guess.
pub type SharedInstruments = Arc<Mutex<std::collections::HashMap<String, SymbolInfo>>>;

pub type SharedSymbolSet = Arc<Mutex<HashSet<String>>>;

/// How the connector treats orders already resting when it (re)connects.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectPolicy {
    /// Ask the venue what is resting and adopt that answer. **The default**, and the
    /// opposite of what the Bybit backend does.
    ///
    /// Cancelling on every connect turns a transient CDN blip — of which this venue serves
    /// about nine a day — into a round trip through an empty book, and each one spends
    /// address-based rate-limit budget that only trading volume replenishes (design note
    /// §5.9, §5.10).
    #[default]
    Reconcile,
    /// Cancel everything for the registered coins and start clean, the way
    /// `bybit/private_stream.rs` does on its subscribe acknowledgement.
    ///
    /// The reason it exists: it is what makes a `myhft` restart free of duplicate orders,
    /// because the connector guarantees the venue holds nothing it did not place this run.
    CancelAll,
}

pub struct PrivateStream {
    ws_url: String,
    rest_url: String,
    /// The **master** account. Every subscription and every `/info` query uses it.
    account_address: String,
    order_manager: SharedOrderManager,
    symbols: SharedSymbolSet,
    symbol_rx: Receiver<String>,
    ev_tx: UnboundedSender<PublishEvent>,
    policy: ConnectPolicy,
    /// Cancelling resting orders. Owns the `ExchangeClient` and the resolved instruments,
    /// which is why this struct no longer holds either.
    sweeper: Sweeper,
}

impl PrivateStream {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ws_url: String,
        rest_url: String,
        account_address: String,
        order_manager: SharedOrderManager,
        symbols: SharedSymbolSet,
        symbol_rx: Receiver<String>,
        ev_tx: UnboundedSender<PublishEvent>,
        exchange: Arc<ExchangeClient>,
        instruments: SharedInstruments,
        policy: ConnectPolicy,
    ) -> Self {
        Self {
            sweeper: Sweeper::new(
                rest_url.clone(),
                account_address.clone(),
                order_manager.clone(),
                exchange.clone(),
                instruments.clone(),
                ev_tx.clone(),
            ),
            ws_url,
            rest_url,
            account_address,
            order_manager,
            symbols,
            symbol_rx,
            ev_tx,
            policy,
        }
    }

    /// Connects, and keeps reconnecting. Never returns.
    pub async fn run(&mut self) {
        let mut policy = ReconnectPolicy::new();
        loop {
            let connected_at = Instant::now();
            let error = match self.connect().await {
                Ok(()) => HyperliquidError::ConnectionInterrupted,
                Err(error) => error,
            };
            // How long this connection was up — the clean-close fast path is granted only to
            // a session that actually lasted (see `ReconnectPolicy`).
            let session = connected_at.elapsed();
            // One lock, taken once and released before the macro. Two
            // `lock()` calls inside a single `error!` invocation deadlock the
            // thread against itself: both `MutexGuard` temporaries live to the
            // end of the statement, and `std::sync::Mutex` is not reentrant.
            // Found by cutting the socket live — the log line below never
            // appeared and the account stream never reconnected.
            let (tracked_orders, replayed_fills) = {
                let om = self.order_manager.lock().unwrap();
                (om.tracked(), om.replayed_fills())
            };
            error!(
                ?error,
                tracked_orders, replayed_fills, "The Hyperliquid account stream disconnected."
            );
            publish_error(&self.ev_tx, ErrorKind::ConnectionInterrupted, &error);
            let delay = policy.delay(&error, session);
            info!(
                ?delay,
                ?session,
                "Reconnecting to the Hyperliquid account stream."
            );
            time::sleep(delay).await;
        }
    }

    async fn connect(&mut self) -> Result<(), HyperliquidError> {
        let request = self.ws_url.as_str().into_client_request()?;
        let (ws_stream, _) = time::timeout(CONNECT_TIMEOUT, connect_async(request))
            .await
            .map_err(|_| HyperliquidError::ConnectTimeout(CONNECT_TIMEOUT))??;
        info!(
            url = %self.ws_url,
            account = %self.account_address,
            "Connected to the Hyperliquid account stream."
        );

        let (mut write, mut read) = ws_stream.split();
        for frame in subscription_frames(&self.account_address) {
            send(&mut write, Message::Text(frame.into())).await?;
        }

        // The policy, then the truth. Under `cancel_all` the sweep runs first so that what
        // is reconciled afterwards is what the venue holds *after* the clean-out — the same
        // order Bybit uses on its subscribe acknowledgement.
        if self.policy == ConnectPolicy::CancelAll {
            self.cancel_all_registered().await;
        }
        // Before anything is believed. `orderUpdates` cannot replay what was missed, and
        // resting orders outlive the connection, so the only source of truth is a query.
        self.reconcile().await?;

        let mut ping = time::interval(PING_INTERVAL);
        ping.reset();
        let mut idle_check = time::interval(IDLE_CHECK_INTERVAL);
        idle_check.reset();
        let mut refresh = time::interval(RECONCILE_INTERVAL);
        refresh.reset();
        let mut last_frame = Instant::now();
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
                    let idle = last_frame.elapsed();
                    if idle > IDLE_TIMEOUT {
                        return Err(HyperliquidError::IdleTimeout(idle));
                    }
                }
                _ = refresh.tick() => {
                    // A failure here is not a reason to drop a healthy socket — the venue
                    // may be briefly unreachable and the next tick asks again.
                    if let Err(error) = self.reconcile().await {
                        warn!(?error, "A periodic Hyperliquid reconciliation didn't complete.");
                    }
                }
                registered = self.symbol_rx.recv(), if !registrations_closed => {
                    match registered {
                        // A bot registering an instrument is the moment its state has to be
                        // right, and the account subscriptions are account-wide, so nothing
                        // needs subscribing — only re-asking.
                        Ok(symbol) => self.on_registration(Some(&symbol)).await?,
                        Err(RecvError::Lagged(missed)) => {
                            warn!(
                                missed,
                                "Missed Hyperliquid instrument registrations; re-asking for \
                                 every registered coin."
                            );
                            self.on_registration(None).await?;
                        }
                        Err(RecvError::Closed) => {
                            warn!(
                                "No more Hyperliquid instrument registrations will arrive; \
                                 the connector is shutting down or its registry was dropped."
                            );
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
                            // A clean code-1000 "Expired" close is a scheduled TTL retirement
                            // and reconnects fast (§B1); `from_close_frame` reads the close
                            // code before `to_string()` consumes the frame.
                            return Err(HyperliquidError::from_close_frame(frame));
                        }
                        Some(Ok(_)) => {}
                        Some(Err(error)) => return Err(HyperliquidError::from(error)),
                        None => return Err(HyperliquidError::ConnectionInterrupted),
                    }
                }
            }
        }
    }

    /// What a registration triggers, which is the [`ConnectPolicy`] applied to that symbol.
    ///
    /// `symbol` is `None` only when the wake-up was lost to a lagging broadcast, which does
    /// not say which coin arrived.
    async fn on_registration(&mut self, symbol: Option<&str>) -> Result<(), HyperliquidError> {
        if self.policy == ConnectPolicy::CancelAll {
            match symbol {
                // **Only the coin that was just registered**, the way Bybit's registration
                // branch does it. Sweeping every registered coin here would cancel the
                // resting grid of a bot that has been quoting for hours because a *second*
                // bot attached — or because one instrument was added late.
                Some(symbol) => self.sweeper.sweep(symbol).await,
                // Nothing says which coin this was, and under a policy whose whole purpose
                // is "the venue holds nothing from before", leaving one unswept is the worse
                // of the two errors.
                None => self.cancel_all_registered().await,
            }
        }
        self.reconcile().await
    }

    /// Re-asks the venue what it holds, and makes the connector's view match.
    ///
    /// Bounded as a whole: it runs inside the read loop, so an unbounded round trip would
    /// stall the keepalive on a venue that drops a silent socket after 60 s.
    async fn reconcile(&mut self) -> Result<(), HyperliquidError> {
        let rest_url = self.rest_url.clone();
        let user = self.account_address.clone();
        // Taken **before** the request goes out: an answer cannot say anything about an
        // order acknowledged after it was assembled, and this query runs concurrently with
        // `submit_order`'s spawned task.
        let queried_at = Instant::now();
        let queried = time::timeout(RECONCILE_BUDGET, async {
            let open = open_orders(&rest_url, &user).await?;
            let held = positions(&rest_url, &user).await?;
            Ok::<_, HyperliquidError>((open, held))
        })
        .await
        .map_err(|_| HyperliquidError::ResolveTimeout(RECONCILE_BUDGET))?;

        let (open, (held, exch_ts)) = queried?;

        // Orders the venue lists that **this process** is not tracking are not adopted:
        // their `order_id` belongs to a bot that no longer exists, and inventing one would
        // collide with a live bot's numbering. The test is "not tracked", not "not our
        // prefix" — the prefix is configuration, so a prefix match would hide exactly the
        // case that matters, a grid left resting by the previous run of this connector.
        let untracked = {
            let manager = self.order_manager.lock().unwrap();
            open.iter().filter(|order| !manager.tracks(order)).count()
        };
        if untracked > 0 {
            warn!(
                untracked,
                open_orders = open.len(),
                "Hyperliquid is holding open orders this process is not tracking — a \
                 previous run's, or a human's on the same account. They are left alone: \
                 adopting them would invent order ids that collide with the bot's own. Set \
                 connect_policy = \"cancel_all\" to clear them instead."
            );
        }

        let expired = self
            .order_manager
            .lock()
            .unwrap()
            .reconcile(&open, queried_at);
        for OrderExt { symbol, order, .. } in expired {
            self.publish_order(symbol, order);
        }

        // A coin with no entry is flat, not unknown: an account that has never traded a perp
        // answers with an empty `assetPositions`. Every registered coin is therefore
        // published, so a bot is never left without a position at all.
        let registered: Vec<String> = self.symbols.lock().unwrap().iter().cloned().collect();
        let exch_ts = exch_ts.checked_mul(1_000_000).unwrap_or_else(local_now);
        for symbol in registered {
            let qty = held
                .iter()
                .find(|(coin, _)| *coin == symbol)
                .map(|(_, qty)| *qty)
                .unwrap_or(0.0);
            self.publish_position(symbol, qty, exch_ts);
        }

        info!(
            open_orders = open.len(),
            untracked,
            positions = held.len(),
            "Reconciled the Hyperliquid account against the venue."
        );
        Ok(())
    }

    /// Sweeps every coin registered so far. What a `cancel_all` connect does.
    async fn cancel_all_registered(&self) {
        let symbols: Vec<String> = self.symbols.lock().unwrap().iter().cloned().collect();
        self.sweeper.sweep_symbols(&symbols).await;
    }

    fn handle(&mut self, text: &str) {
        let frame = match parse_frame(text) {
            Ok(frame) => frame,
            Err(error) => {
                error!(?error, %text, "Couldn't parse a Hyperliquid account frame.");
                return;
            }
        };
        match frame {
            Frame::OrderUpdates(updates) => self.on_order_updates(&updates),
            Frame::UserFills(fills) => self.on_user_fills(&fills),
            Frame::SubscriptionResponse(ack) => {
                info!(subscription = %ack, "Hyperliquid acknowledged an account subscription.");
            }
            Frame::Error(message) => {
                error!(%message, "Hyperliquid reported an error on the account stream.");
            }
            Frame::Pong => {}
            other => {
                warn!(
                    ?other,
                    "Ignoring an unexpected frame on the Hyperliquid account stream."
                );
            }
        }
    }

    fn on_order_updates(&self, updates: &[OrderUpdate]) {
        for update in updates {
            let applied = self
                .order_manager
                .lock()
                .unwrap()
                .apply_order_update(update);
            match applied {
                Some(OrderExt { symbol, order, .. }) => self.publish_order(symbol, order),
                // Not ours, or already terminal, or stale. All three are ordinary and none
                // of them is an error: the account is shared with a human and a UI.
                None => debug!(
                    coin = %update.order.coin,
                    status = %update.status,
                    "An orderUpdates entry this connector does not act on."
                ),
            }
        }
    }

    fn on_user_fills(&self, fills: &UserFills) {
        if fills.is_snapshot {
            // Not a warning: the venue does this on every subscribe, by design. It is worth
            // a line because the count is the only evidence the deduplication is working.
            info!(
                fills = fills.fills.len(),
                "Hyperliquid replayed the account's fill history on subscribe."
            );
        }
        for fill in &fills.fills {
            let disposition = self.order_manager.lock().unwrap().apply_fill(fill);
            match disposition {
                FillDisposition::Replay => {}
                FillDisposition::Applied {
                    symbol,
                    order,
                    position,
                    exch_ts,
                } => {
                    if let Some(order) = order {
                        self.publish_order(symbol.clone(), order);
                    }
                    // Published even for a fill on an order this connector did not place:
                    // it is still the account's position, and the bot trades against it.
                    self.publish_position(symbol, position, exch_ts);
                }
            }
        }
    }

    fn publish_order(&self, symbol: String, order: Order) {
        self.ev_tx
            .send(PublishEvent::LiveEvent(LiveEvent::Order { symbol, order }))
            .unwrap();
    }

    fn publish_position(&self, symbol: String, qty: f64, exch_ts: i64) {
        self.ev_tx
            .send(PublishEvent::LiveEvent(LiveEvent::Position {
                symbol,
                qty,
                exch_ts,
            }))
            .unwrap();
    }
}

/// Cancelling this connector's resting orders, reachable from **outside** the account
/// stream's task.
///
/// It used to live on [`PrivateStream`], which meant the only thing that could ask for a
/// sweep was the stream's own connect. That is fine for the connect policy and useless for
/// the two cases this connector actually loses money in: a bot that died with orders resting
/// (`SweepReason::BotDied`) and a connector that is stopping with the same
/// (`SweepReason::ConnectorStopping`). Both are noticed by the generic supervision layer,
/// which holds no `PrivateStream` and never will.
///
/// Everything it needs is already an `Arc` or a `String`, so `Hyperliquid` can build one on
/// demand and hand it to a task. The stream holds one too, and calls the same code.
#[derive(Clone)]
pub struct Sweeper {
    rest_url: String,
    /// The **master** account. Every `/info` query uses it.
    account_address: String,
    order_manager: SharedOrderManager,
    exchange: Arc<ExchangeClient>,
    instruments: SharedInstruments,
    ev_tx: UnboundedSender<PublishEvent>,
}

impl Sweeper {
    pub fn new(
        rest_url: String,
        account_address: String,
        order_manager: SharedOrderManager,
        exchange: Arc<ExchangeClient>,
        instruments: SharedInstruments,
        ev_tx: UnboundedSender<PublishEvent>,
    ) -> Self {
        Self {
            rest_url,
            account_address,
            order_manager,
            exchange,
            instruments,
            ev_tx,
        }
    }

    /// Sweeps each symbol in turn.
    pub async fn sweep_symbols(&self, symbols: &[String]) {
        for symbol in symbols {
            self.sweep(symbol).await;
        }
    }

    /// One symbol's sweep, bounded. See [`CANCEL_BUDGET`].
    async fn sweep(&self, symbol: &str) {
        if time::timeout(CANCEL_BUDGET, self.cancel_all(symbol))
            .await
            .is_err()
        {
            error!(
                %symbol,
                ?CANCEL_BUDGET,
                "Timed out clearing Hyperliquid's open orders. Whatever was not cancelled \
                 is still resting, and the bot has not been told otherwise."
            );
        }
    }

    /// Cancels everything **the venue** is holding for one symbol, then takes out of this
    /// connector's own map the orders it confirmed.
    ///
    /// The source is the venue's open-order list, not the local map, and that is the whole
    /// point of the policy: the orders a restart has to clear belong to a run that no longer
    /// exists, so this process has never seen them. A sweep built from `orders()` cancels
    /// nothing at all on a freshly started connector — exactly when the guarantee in
    /// §11.9 ("a `myhft` restart provably free of orders from the previous run") is being
    /// relied on. It also reaches orders placed by hand in the UI, which is what an operator
    /// asking for "start clean" means, and what Bybit's venue-side `cancel_all_orders` does.
    ///
    /// **Local state is only changed for cancels the venue confirmed.** A failed POST leaves
    /// the orders alone: they may still be resting, and telling the bot they are cancelled
    /// invites a re-quote on top of live orders. The reconciliation that follows settles
    /// whatever this left ambiguous.
    async fn cancel_all(&self, symbol: &str) {
        let Some(asset_index) = self.asset_index(symbol).await else {
            warn!(
                %symbol,
                "Not sweeping Hyperliquid orders for a coin whose asset index is unknown; a \
                 cancel with a guessed index would address a different coin."
            );
            return;
        };
        let open = match open_orders(&self.rest_url, &self.account_address).await {
            Ok(open) => open,
            Err(error) => {
                error!(
                    %symbol,
                    ?error,
                    "Couldn't ask Hyperliquid what it is holding, so nothing was cancelled."
                );
                return;
            }
        };
        let cancels = cancels_for(&open, symbol, asset_index);
        if cancels.is_empty() {
            return;
        }

        let outcomes = match self
            .exchange
            .post(&CancelAction::new(cancels.clone()))
            .await
        {
            Ok(outcomes) => outcomes,
            Err(error) => {
                error!(
                    %symbol,
                    asked = cancels.len(),
                    ?error,
                    "Couldn't cancel Hyperliquid's open orders. They are left as they are — \
                     they may still be resting — and reconciliation will settle them."
                );
                return;
            }
        };
        let confirmed = confirmed_cancels(&cancels, &outcomes);
        if confirmed.len() < cancels.len() {
            warn!(
                %symbol,
                asked = cancels.len(),
                confirmed = confirmed.len(),
                ?outcomes,
                "Hyperliquid did not confirm every cancel; the rest are left for \
                 reconciliation rather than reported as cancelled."
            );
        } else {
            info!(%symbol, cancelled = confirmed.len(), "Cleared Hyperliquid's open orders for a registered coin.");
        }
        for oid in confirmed {
            let cancelled = self.order_manager.lock().unwrap().cancel_confirmed(oid);
            if let Some(OrderExt { symbol, order, .. }) = cancelled {
                self.publish_order(symbol, order);
            }
        }
    }

    /// This coin's index in the venue's universe, asking the venue if nothing has yet.
    ///
    /// The public stream is what fills the shared map, and on a cold start the sweep can run
    /// before it has answered. Refusing then would make `cancel_all` silently do nothing on
    /// the one path it exists for, so one bounded REST question is asked instead. It is
    /// never *guessed*: an order or cancel carrying the wrong index addresses a different
    /// coin.
    async fn asset_index(&self, symbol: &str) -> Option<u32> {
        let known = self
            .instruments
            .lock()
            .unwrap()
            .get(symbol)
            .and_then(|info| info.asset_index);
        if known.is_some() {
            return known;
        }
        match resolve_symbols(&self.rest_url, &[symbol.to_string()]).await {
            Ok((resolved, _)) => resolved.first().and_then(|info| info.asset_index),
            Err(error) => {
                warn!(%symbol, ?error, "Couldn't resolve a Hyperliquid asset index.");
                None
            }
        }
    }

    fn publish_order(&self, symbol: String, order: Order) {
        self.ev_tx
            .send(PublishEvent::LiveEvent(LiveEvent::Order { symbol, order }))
            .unwrap();
    }
}

/// The cancels one symbol's sweep has to send, taken from what the **venue** lists.
///
/// Split out and pure because the property that matters cannot be observed from the local
/// map: a connector that has just started tracks nothing, and these are exactly the orders
/// it must still cancel.
fn cancels_for(open: &[OpenOrder], symbol: &str, asset_index: u32) -> Vec<CancelWire> {
    open.iter()
        .filter(|order| order.coin == symbol)
        .map(|order| CancelWire {
            a: asset_index,
            o: order.oid,
        })
        .collect()
}

/// The order ids the venue **confirmed** it cancelled, given the answer to those cancels.
///
/// A `200 ok` is not a per-item answer: the venue reports each cancel separately and one of
/// them can be `{"error": …}` while the envelope says `ok`. An answer that does not line up
/// with the request one-for-one confirms nothing — the alignment is positional, so a
/// mismatched length means the mapping is unknown, and claiming a cancel that did not happen
/// is what leaves the bot quoting on top of a live order.
fn confirmed_cancels(requested: &[CancelWire], outcomes: &[ActionOutcome]) -> Vec<u64> {
    if outcomes.len() != requested.len() {
        return Vec::new();
    }
    requested
        .iter()
        .zip(outcomes)
        .filter(|(_, outcome)| matches!(outcome, ActionOutcome::Success))
        .map(|(cancel, _)| cancel.o)
        .collect()
}

/// The two account subscriptions, both keyed by the **master** address.
///
/// Split out so the wire shape can be asserted without a socket, because getting it wrong
/// is silent: the venue accepts a subscription for the API wallet's address, acknowledges
/// it, and then sends nothing at all — which is indistinguishable from an account that is
/// not trading.
pub fn subscription_frames(account_address: &str) -> Vec<String> {
    ["orderUpdates", "userFills"]
        .into_iter()
        .map(|kind| {
            serde_json::json!({
                "method": "subscribe",
                "subscription": { "type": kind, "user": account_address },
            })
            .to_string()
        })
        .collect()
}

/// Submits one order, off the caller's thread.
///
/// `Connector::submit` must not block, so everything after the local bookkeeping happens in
/// a spawned task and comes back as a `PublishEvent`.
///
/// **The two failure kinds are deliberately not treated alike**, and this is the part worth
/// arguing with:
///
/// * A **venue rejection** is definitive — the order does not exist — so it is expired back
///   to the bot, which frees the `order_id` and clears the `Status::New` that
///   `LiveBot::submit_order` inserted before the request left.
/// * A **transport failure** (timeout, connection reset) is *not*. The order may be resting
///   on the venue right now. Expiring it would tell the bot it is flat and invite a
///   re-quote on top of a live order — real, duplicated exposure. So the order is kept, the
///   error is reported, and the next reconciliation is what resolves it: it has no venue id
///   yet, so `OrderManager::reconcile` expires it only on positive evidence that the venue
///   does not hold it.
pub fn submit_order(
    symbol: String,
    order: Order,
    order_manager: SharedOrderManager,
    exchange: Arc<ExchangeClient>,
    instruments: SharedInstruments,
    ev_tx: UnboundedSender<PublishEvent>,
) {
    let info = instruments.lock().unwrap().get(&symbol).cloned();
    let prepared = match info {
        Some(info) => match info.asset_index {
            Some(asset_index) => {
                order_manager
                    .lock()
                    .unwrap()
                    .new_order(&symbol, &info, asset_index, order.clone())
            }
            None => Err(HyperliquidError::InvalidOrder(format!(
                "{symbol} is on a HIP-3 builder dex, whose asset index this backend does \
                 not resolve; its market data works but orders would address a different \
                 coin"
            ))),
        },
        None => Err(HyperliquidError::InvalidOrder(format!(
            "{symbol} has not been resolved against Hyperliquid's universe yet, so neither \
             its price grid nor its asset index is known"
        ))),
    };

    let (cloid, wire) = match prepared {
        Ok(prepared) => prepared,
        Err(error) => {
            expire_and_report(&symbol, order, &error, &ev_tx);
            return;
        }
    };
    order_manager
        .lock()
        .unwrap()
        .mark_requested(&cloid, Status::New);

    tokio::spawn(async move {
        let action = OrderAction::new(vec![wire]);
        match exchange.post(&action).await {
            Ok(outcomes) => match outcomes.first() {
                Some(ActionOutcome::Resting { oid, .. })
                | Some(ActionOutcome::Filled { oid, .. }) => {
                    // The lifecycle itself arrives on `orderUpdates`; this records the venue
                    // id — without which the order cannot be reconciled later — and that the
                    // request has been answered.
                    order_manager.lock().unwrap().note_venue_ack(&cloid, *oid);
                }
                Some(outcome) => {
                    let reason = outcome.error().unwrap_or("an unrecognised outcome");
                    let error = HyperliquidError::ExchangeRejected(reason.to_string());
                    let expired = order_manager.lock().unwrap().submit_failed(&cloid);
                    report_rejection(&symbol, expired, &error, &ev_tx);
                }
                None => {
                    // `ok` with no per-order verdict. Nothing was refused, so the order is
                    // kept and `orderUpdates` remains the authority.
                    order_manager.lock().unwrap().request_settled(&cloid);
                    warn!(%symbol, %cloid, "Hyperliquid accepted an order action without a per-order status.");
                }
            },
            // A verdict, and only a verdict: the matching engine decided against this order,
            // so it does not exist and the bot gets its id back.
            Err(error) if error.is_venue_verdict() => {
                let expired = order_manager.lock().unwrap().submit_failed(&cloid);
                report_rejection(&symbol, expired, &error, &ev_tx);
            }
            Err(error) => {
                // Transport, not verdict: the order may be resting. Keeping it is the
                // choice that cannot double the position. A gateway `5xx` lands here too —
                // the request may have been executed and only the answer lost.
                error!(
                    %symbol,
                    %cloid,
                    ?error,
                    "Couldn't reach Hyperliquid to submit an order. The order is kept — it \
                     may be resting — and the next reconciliation will settle it."
                );
                order_manager.lock().unwrap().request_settled(&cloid);
                publish_error(&ev_tx, ErrorKind::OrderError, &error);
            }
        }
    });
}

/// The exact `cancelByCloid` action the per-order cancel path signs and sends.
///
/// Both `cancel_order` and its test build the action through this one function, and
/// `cancel_order` posts the result verbatim, so the wire shape is pinned by a test rather
/// than by review. **`f` (fast-cancel) stays off here.** The flag has no effect today
/// (`CancelByCloidAction::fast` — future mempool prioritisation only), and we have **no
/// evidence the venue accepts an `f: true` direct-signed cancel**: the official API docs and
/// python-SDK describe cancel without it, and the only testnet capture sent a plain cancel.
/// Enabling it on the money path unverified risks every cancel being rejected — the order
/// then stays live — for zero present benefit. Turning it on is gated on a testnet
/// direct-signing capture that proves the venue accepts an `f: true` cancel (or a primary
/// venue contract); until then it is omitted. See `the_per_order_cancel_omits_the_fast_flag`.
fn cancel_action_for_order(wire: CancelByCloidWire) -> CancelByCloidAction {
    CancelByCloidAction::new(vec![wire])
}

/// Cancels one order, off the caller's thread.
///
/// A refused cancel leaves the order **live**: the usual reason is that it has already gone,
/// and `orderUpdates` is what says so. Dropping it here would lose an order that may still
/// be resting.
pub fn cancel_order(
    symbol: String,
    order: Order,
    order_manager: SharedOrderManager,
    exchange: Arc<ExchangeClient>,
    instruments: SharedInstruments,
    ev_tx: UnboundedSender<PublishEvent>,
) {
    let asset_index = instruments
        .lock()
        .unwrap()
        .get(&symbol)
        .and_then(|info| info.asset_index);
    let Some(asset_index) = asset_index else {
        let error = HyperliquidError::InvalidOrder(format!(
            "{symbol} has no known Hyperliquid asset index, so its orders cannot be cancelled"
        ));
        publish_error(&ev_tx, ErrorKind::OrderError, &error);
        return;
    };

    let prepared = order_manager
        .lock()
        .unwrap()
        .cancel_order(&symbol, order.order_id, asset_index);
    let (cloid, wire) = match prepared {
        Ok(prepared) => prepared,
        Err(error) => {
            // Nothing to cancel: the order is already gone as far as this connector knows.
            // Reported, but not turned into a fake order event — the bot's own copy is
            // reconciled by whatever terminal update removed it here.
            publish_error(&ev_tx, ErrorKind::OrderError, &error);
            return;
        }
    };
    order_manager
        .lock()
        .unwrap()
        .mark_requested(&cloid, Status::Canceled);

    tokio::spawn(async move {
        // The action is built by `cancel_action_for_order` and posted verbatim; fast-cancel
        // (`f`) stays off there — the flag is a no-op today and unverified against the venue,
        // so it is not enabled on the money path (see that function).
        let action = cancel_action_for_order(wire);
        let failure = match exchange.post(&action).await {
            Ok(outcomes) => match outcomes.first() {
                Some(ActionOutcome::Success) | None => None,
                Some(outcome) => Some(HyperliquidError::ExchangeRejected(
                    outcome
                        .error()
                        .unwrap_or("an unrecognised outcome")
                        .to_string(),
                )),
            },
            Err(error) => Some(error),
        };
        if let Some(error) = failure {
            warn!(%symbol, %cloid, ?error, "Hyperliquid refused a cancel; the order is left alone.");
            if let Some(OrderExt { symbol, order, .. }) =
                order_manager.lock().unwrap().cancel_failed(&cloid)
            {
                ev_tx
                    .send(PublishEvent::LiveEvent(LiveEvent::Order { symbol, order }))
                    .unwrap();
            }
            publish_error(&ev_tx, ErrorKind::OrderError, &error);
        }
    });
}

/// Takes an order the connector never sent back out of the bot's state, then reports why.
///
/// Order matters: `LiveBot`'s error handler may abort the `elapse`, so the state has to be
/// clean before it runs.
///
/// `pub(crate)` so that `supervision`'s test of the supervisor contract can drive a real send
/// site rather than a stand-in — see `an_unexpected_publish_task_death_still_panics_a_
/// production_sender`.
pub(crate) fn expire_and_report(
    symbol: &str,
    order: Order,
    error: &HyperliquidError,
    ev_tx: &UnboundedSender<PublishEvent>,
) {
    error!(%symbol, order_id = order.order_id, ?error, "Refusing a Hyperliquid order.");
    let mut order = order;
    order.req = Status::None;
    order.status = Status::Expired;
    ev_tx
        .send(PublishEvent::LiveEvent(LiveEvent::Order {
            symbol: symbol.to_string(),
            order,
        }))
        .unwrap();
    publish_error(ev_tx, ErrorKind::OrderError, error);
}

fn report_rejection(
    symbol: &str,
    expired: Option<OrderExt>,
    error: &HyperliquidError,
    ev_tx: &UnboundedSender<PublishEvent>,
) {
    error!(%symbol, ?error, "Hyperliquid rejected an order.");
    if let Some(OrderExt { symbol, order, .. }) = expired {
        ev_tx
            .send(PublishEvent::LiveEvent(LiveEvent::Order { symbol, order }))
            .unwrap();
    }
    publish_error(ev_tx, ErrorKind::OrderError, error);
}

#[cfg(test)]
mod tests {
    use hftbacktest::types::{ErrorKind, LiveEvent, OrdType, Order, Side, Status, TimeInForce};
    use tokio::sync::mpsc::unbounded_channel;

    use crate::{
        connector::{GetOrders, PublishEvent},
        hyperliquid::{
            HyperliquidError,
            exchange::{ActionOutcome, CancelByCloidWire, CancelWire},
            ordermanager::OrderManager,
            private_stream::{
                ConnectPolicy,
                cancel_action_for_order,
                cancels_for,
                confirmed_cancels,
                expire_and_report,
                subscription_frames,
            },
            rest::OpenOrder,
        },
    };

    fn open(coin: &str, oid: u64, cloid: Option<&str>) -> OpenOrder {
        OpenOrder {
            coin: coin.into(),
            oid,
            cloid: cloid.map(str::to_string),
        }
    }

    /// **The live per-order cancel must not set fast-cancel.** `cancel_order` posts exactly
    /// what `cancel_action_for_order` returns, so this pins the wire the money path signs:
    /// the `f` flag has no effect today and is unverified against the venue, so enabling it
    /// on every user cancel risks the venue rejecting the request (leaving the order live)
    /// for zero present benefit. Re-adding `.fast()` to the production cancel — the reviewer's
    /// named mutation — puts `f: true` back on the wire and fails this test. The dormant
    /// `exchange::tests::a_fast_cancel_serialises_the_f_flag_true` still pins that the wire
    /// *would* be correct if the flag were ever activated.
    #[test]
    fn the_per_order_cancel_omits_the_fast_flag() {
        let action = cancel_action_for_order(CancelByCloidWire {
            asset: 3,
            cloid: "0xa1b2000000000000000000000000dead".into(),
        });
        let body = serde_json::to_value(&action).unwrap();
        assert_eq!(body["type"], "cancelByCloid");
        assert!(
            body.get("f").is_none(),
            "the live cancel must not set fast-cancel until the venue is shown to accept it: \
             {body}"
        );
    }

    /// **The sweep must come from the venue, not from this process's own map.** A connector
    /// that has just started tracks nothing, and the orders `cancel_all` exists to clear are
    /// precisely the ones the previous run left resting — Hyperliquid has no
    /// cancel-on-disconnect, so they outlive the process. Sourcing the cancels locally makes
    /// the policy a no-op exactly when its guarantee (design note §11.9) is being relied on:
    /// `myhft` restarts, the bot is told it holds nothing, and it quotes a fresh grid
    /// alongside the old one.
    #[test]
    fn a_sweep_cancels_what_the_venue_lists_even_with_nothing_tracked_locally() {
        let manager = OrderManager::new("a1f0").unwrap();
        assert!(
            manager.orders(None).is_empty(),
            "a freshly started connector tracks nothing"
        );

        let venue = [
            // The previous run's: same prefix, unknown to this process.
            open("BTC", 101, Some("0xa1f0000000000000000000000000dead")),
            // A human's, from the UI. "Start clean" means this one too.
            open("BTC", 102, None),
            // Another coin — not this sweep's business.
            open("ETH", 103, Some("0xa1f0000000000000000000000000beef")),
        ];

        let cancels = cancels_for(&venue, "BTC", 3);
        assert_eq!(
            cancels,
            vec![CancelWire { a: 3, o: 101 }, CancelWire { a: 3, o: 102 },],
            "the sweep is keyed on the venue's own order ids"
        );
        assert!(cancels_for(&venue, "SOL", 0).is_empty());
    }

    /// **A sweep is account-wide for the coin, and cannot be anything else.** The venue lists
    /// open orders by account; nothing in an `OpenOrder` says which bot asked for it, because
    /// `Connector::submit` is never told a bot id. So there is no such operation as
    /// "cancel bot 7's BTC orders" — asking for BTC cancels everything the account has on
    /// BTC, including a second bot's live grid.
    ///
    /// That is why `supervision::BotRegistry::take_dead` refuses to hand a dead bot's symbol
    /// to a sweep while another bot is still quoting it. This test is the other end of that
    /// contract: if a future change ever makes cancels attributable, that refusal can be
    /// revisited, and this is where it will show up.
    #[test]
    fn a_sweep_cancels_every_order_the_account_holds_on_that_coin() {
        let venue = [
            // This connector's, this run.
            open("BTC", 201, Some("0xa1f0000000000000000000000000000a")),
            // A second bot's, on the same account and the same coin.
            open("BTC", 202, Some("0xb2e1000000000000000000000000000b")),
            // A human's, from the UI.
            open("BTC", 203, None),
        ];

        assert_eq!(
            cancels_for(&venue, "BTC", 3)
                .iter()
                .map(|cancel| cancel.o)
                .collect::<Vec<_>>(),
            vec![201, 202, 203],
            "every open order on the coin is cancelled: the sweep has no bot-level filter to \
             apply, so its caller must not ask for a coin a live bot is quoting"
        );
    }

    /// **Only a confirmed cancel may be reported as cancelled.** The venue answers per item
    /// under a `200 ok`, and an answer that does not line up with the request confirms
    /// nothing at all — publishing `Status::Canceled` for an order that is still resting is
    /// what invites a re-quote on top of it.
    #[test]
    fn only_the_cancels_the_venue_confirmed_are_treated_as_done() {
        let asked = vec![
            CancelWire { a: 3, o: 101 },
            CancelWire { a: 3, o: 102 },
            CancelWire { a: 3, o: 103 },
        ];
        let answered = vec![
            ActionOutcome::Success,
            ActionOutcome::Error("Order was never placed, already canceled, or filled.".into()),
            ActionOutcome::Success,
        ];
        assert_eq!(confirmed_cancels(&asked, &answered), vec![101, 103]);

        // An `ok` with no per-item data confirms nothing; reconciliation settles it instead.
        assert!(confirmed_cancels(&asked, &[]).is_empty());
        // …and so does a mismatched count, where the positional mapping is unknown.
        assert!(confirmed_cancels(&asked, &answered[..2]).is_empty());
    }

    /// **Both subscriptions must name the master account.** Subscribing with the API
    /// wallet's address is accepted and acknowledged and then produces nothing at all — an
    /// account that appears never to trade, with no error anywhere to say why.
    #[test]
    fn both_account_subscriptions_name_the_master_account() {
        let master = "0x70997970C51812dc3A010C7d01b50e0d17dc79C8";
        let frames = subscription_frames(master);

        assert_eq!(frames.len(), 2);
        let parsed: Vec<serde_json::Value> = frames
            .iter()
            .map(|frame| serde_json::from_str(frame).unwrap())
            .collect();
        for kind in ["orderUpdates", "userFills"] {
            let frame = parsed
                .iter()
                .find(|f| f["subscription"]["type"] == kind)
                .unwrap_or_else(|| panic!("{kind} was never subscribed: {frames:?}"));
            assert_eq!(frame["method"], "subscribe");
            assert_eq!(frame["subscription"]["user"], master);
            // No coin: these are account-wide, which is why a registration re-queries
            // rather than subscribing anything.
            assert!(frame["subscription"].get("coin").is_none(), "{frame}");
        }
    }

    /// The default must be the one that does not cancel. Nine CDN drops a day against a
    /// cancel-on-connect policy is nine round trips through an empty book, each spending
    /// address rate-limit budget that only traded volume replenishes.
    #[test]
    fn the_default_connect_policy_reconciles_rather_than_cancels() {
        assert_eq!(ConnectPolicy::default(), ConnectPolicy::Reconcile);

        #[derive(serde::Deserialize)]
        struct Holder {
            #[serde(default)]
            connect_policy: ConnectPolicy,
        }
        let absent: Holder = toml::from_str("").unwrap();
        assert_eq!(absent.connect_policy, ConnectPolicy::Reconcile);
        let chosen: Holder = toml::from_str(r#"connect_policy = "cancel_all""#).unwrap();
        assert_eq!(chosen.connect_policy, ConnectPolicy::CancelAll);
    }

    /// A request the connector cannot send must take the order back out of the bot's state
    /// **before** the error is reported: `LiveBot`'s error handler may abort the `elapse`,
    /// and an order left at `Status::New` exists nowhere and burns its id for the life of
    /// the process.
    #[test]
    fn a_refused_request_expires_the_order_before_it_reports_the_error() {
        let (tx, mut rx) = unbounded_channel();
        let order = Order::new(
            7,
            100,
            0.1,
            1.0,
            Side::Buy,
            OrdType::Limit,
            TimeInForce::GTC,
        );
        expire_and_report(
            "BTC",
            order,
            &HyperliquidError::InvalidOrder("no asset index".into()),
            &tx,
        );

        let PublishEvent::LiveEvent(LiveEvent::Order { symbol, order }) = rx.try_recv().unwrap()
        else {
            panic!("the order must come back first");
        };
        assert_eq!(symbol, "BTC");
        assert_eq!(order.order_id, 7);
        assert_eq!(order.status, Status::Expired);
        assert_eq!(order.req, Status::None);

        let PublishEvent::LiveEvent(LiveEvent::Error(error)) = rx.try_recv().unwrap() else {
            panic!("the reason must still be reported");
        };
        assert_eq!(error.kind, ErrorKind::OrderError);
    }

    /// Whatever the venue says, the message that reaches the bots has to fit the 512-byte
    /// IPC payload — an encode that does not fit kills the connector under its own panic
    /// hook, on every restart. Venue text is not bounded by anything at the source.
    #[test]
    fn a_venue_rejection_of_any_length_still_fits_the_ipc_payload() {
        let (tx, mut rx) = unbounded_channel();
        let order = Order::new(
            7,
            100,
            0.1,
            1.0,
            Side::Buy,
            OrdType::Limit,
            TimeInForce::GTC,
        );
        expire_and_report(
            "BTC",
            order,
            &HyperliquidError::ExchangeRejected("x".repeat(4096)),
            &tx,
        );
        let _ = rx.try_recv();

        let PublishEvent::LiveEvent(event) = rx.try_recv().unwrap() else {
            panic!("expected the error");
        };
        let mut buffer = [0u8; 512];
        bincode::encode_into_slice(&event, &mut buffer, bincode::config::standard())
            .expect("a venue rejection must never be able to kill the connector");
    }
}
