use std::{
    collections::{HashMap, hash_map::Entry},
    time::{Duration, Instant},
};

use chrono::Utc;
use rand::Rng;
use thiserror::Error;
use tracing::{debug, error, info, warn};

use crate::{
    depth::{L2MarketDepth, MarketDepth},
    live::{Instrument, ipc::Channel},
    types::{
        Bot,
        BuildError,
        ElapseResult,
        Event,
        LOCAL_ASK_DEPTH_EVENT,
        LOCAL_BID_DEPTH_EVENT,
        LOCAL_BUY_TRADE_EVENT,
        LOCAL_SELL_TRADE_EVENT,
        LiveError,
        LiveEvent,
        LiveRequest,
        OrdType,
        Order,
        OrderId,
        OrderRequest,
        Side,
        StateValues,
        Status,
        TimeInForce,
        WaitOrderResponse,
    },
};

#[derive(Error, Debug)]
pub enum BotError {
    #[error("OrderIdExist")]
    OrderIdExist,
    #[error("AssetNotFound")]
    InstrumentNotFound,
    #[error("OrderNotFound")]
    OrderNotFound,
    #[error("InvalidOrderStatus")]
    InvalidOrderStatus,
    #[error("Timeout")]
    Timeout,
    #[error("Interrupted")]
    Interrupted,
    /// A transport failure on the channel of one named connector.
    ///
    /// The name is stamped by the receiving side, which knows which channel it just polled or
    /// failed to send on — it is not carried over the wire, and no connector has to be rebuilt
    /// for it to appear. It exists so that a consumer can tell "the connector I trade on is
    /// unreachable" from "a market-data-only connector hiccuped" without parsing prose; the
    /// library names the connector and the consumer decides what it is worth
    /// (`AGENTS.md` §1.1).
    #[error("Channel({connector}): {message}")]
    Channel { connector: String, message: String },
    #[error("Custom: {0}")]
    Custom(String),
}

pub type ErrorHandler = Box<dyn Fn(LiveError) -> Result<(), BotError>>;
pub type OrderRecvHook = Box<dyn Fn(&Order, &Order) -> Result<(), BotError>>;

/// How often a [`LiveBot`] tells its connectors that its event loop is still turning.
///
/// One second, against a connector default of ten: the connector only acts after a bot has
/// been silent for its whole window, so the ratio is the headroom a bot has to be late —
/// through a slow `elapse`, a stalled feed, a long GC pause on the other side of the
/// process — before its orders are cancelled underneath it.
///
/// The cost is one 1-byte message per instrument per second on a shared-memory queue.
pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);

/// The floor on a single receive, so that a pathologically small heartbeat interval polls
/// the channel rather than spinning on it. See [`LiveBot::recv_chunk`].
const MIN_RECV_CHUNK: Duration = Duration::from_millis(1);

/// The longest one connector's open batch may hold a single `elapse` from returning.
///
/// A batch defers the return so that the strategy never reads a half-applied frame. A
/// `BatchStart` whose `BatchEnd` never arrives would therefore defer it forever: on a busy feed
/// the receive never times out, and the bot keeps heartbeating from inside the loop, so it goes
/// on looking alive to its connector while it has stopped quoting. That failure predates this
/// ceiling — a lost marker did it with one connector too — but per-connector batch state
/// removes the accidental cure, where any other connector's `BatchEnd` cleared the single flag.
///
/// Choosing to return on a possibly torn book is a fail-closed choice, not a departure from one
/// (`AGENTS.md` §1.1): a torn read heals on the next frame and is visible to a consumer's
/// staleness gate, whereas an `elapse` that never returns is a bot that has stopped managing
/// money and does not say so.
///
/// The number is bounded from above and not yet from below. It must stay well under the
/// connector's 10 s liveness window and a consumer's staleness gate (5 s in `myhft`), or it is
/// decoration. Below, it must stay well above the drain of the largest real batch — a
/// 200-level Binance snapshot is ~400 separate messages — and *that has not been measured*
/// (design note §9.1; this repository has no benchmarks). 250 ms is two orders of magnitude
/// above any plausible drain and twenty times under the tightest gate above it; measure before
/// the second connector goes live.
const MAX_BATCH_HOLD: Duration = Duration::from_millis(250);

/// How many batches one connector currently has open, and since when.
///
/// Per connector rather than one flag for the bot, because the receiver round-robins between
/// connectors: their batches interleave 1:1 whenever both have traffic, so a shared flag lets
/// one connector's `BatchEnd` release another's open batch and hand the strategy a
/// half-applied book (design note §2.1).
#[derive(Default, Clone, Copy)]
struct BatchHold {
    /// `BatchStart`s not yet matched by a `BatchEnd`.
    ///
    /// A count, not a flag: a connector nests its own batches — a registration batch is
    /// assembled whole inside one `handle_msg` and can land between a market-data
    /// `BatchStart` and its `BatchEnd` — and with a flag the inner end would release the outer
    /// batch (design note §3.6). An unmatched `BatchEnd` saturates at zero rather than
    /// wrapping; an unmatched `BatchStart` is what the ceiling is for.
    depth: u32,
    /// When the depth last went from zero to one, measured from the start of this `elapse_`.
    since: Option<Duration>,
}

impl BatchHold {
    /// Whether this connector's batch still defers the return, i.e. it is open *and* has not
    /// been open longer than the ceiling.
    fn holds(&self, elapsed: Duration, ceiling: Duration) -> bool {
        self.depth > 0
            && self
                .since
                .is_none_or(|since| elapsed.saturating_sub(since) <= ceiling)
    }
}

fn generate_random_id() -> u64 {
    // Initialize the random number generator
    let mut rng = rand::rng();

    // Generate a random u64 value
    rng.random::<u64>()
}

/// Live [`LiveBot`] builder.
pub struct LiveBotBuilder<MD> {
    id: u64,
    instruments: Vec<Instrument<MD>>,
    error_handler: Option<ErrorHandler>,
    order_hook: Option<OrderRecvHook>,
    heartbeat_interval: Option<Duration>,
}

impl<MD> Default for LiveBotBuilder<MD> {
    fn default() -> Self {
        Self::new()
    }
}

impl<MD> LiveBotBuilder<MD> {
    /// Constructs a builder to construct [`LiveBot`] instances.
    pub fn new() -> Self {
        Self {
            id: generate_random_id(),
            instruments: Default::default(),
            error_handler: None,
            order_hook: None,
            heartbeat_interval: Some(DEFAULT_HEARTBEAT_INTERVAL),
        }
    }

    /// Registers an instrument.
    pub fn register(self, instrument: Instrument<MD>) -> Self {
        Self {
            instruments: {
                let mut instruments = self.instruments;
                instruments.push(instrument);
                instruments
            },
            ..self
        }
    }

    /// Registers the error handler to deal with an error from connectors.
    pub fn error_handler<Handler>(self, handler: Handler) -> Self
    where
        Handler: Fn(LiveError) -> Result<(), BotError> + 'static,
    {
        Self {
            error_handler: Some(Box::new(handler)),
            ..self
        }
    }

    /// Registers the order response receive hook.
    pub fn order_recv_hook<Hook>(self, hook: Hook) -> Self
    where
        Hook: Fn(&Order, &Order) -> Result<(), BotError> + 'static,
    {
        Self {
            order_hook: Some(Box::new(hook)),
            ..self
        }
    }

    /// Sets the bot ID. It must be unique among all bots connected to the same `Connector`.
    pub fn id(self, id: u64) -> Self {
        Self { id, ..self }
    }

    /// How often to tell the connectors this bot's event loop is still turning, or `None` to
    /// say nothing at all. Defaults to [`DEFAULT_HEARTBEAT_INTERVAL`].
    ///
    /// **Leaving it on is the safe choice.** A connector that has seen this bot heartbeat
    /// will cancel its resting orders when it stops hearing from it; a bot that never
    /// heartbeats is never watched, and its orders keep trading after it dies
    /// (`connector/src/supervision.rs`).
    ///
    /// Switch it off only when pointing at a connector built before
    /// [`LiveRequest::Heartbeat`] existed: that connector cannot decode the variant and will
    /// exit on the first one. The two processes are updated as a pair — `AGENTS.md` §2.
    ///
    /// **Not reachable from Python.** `py-hftbacktest`'s `build_*_livebot` construct the
    /// builder themselves and do not surface this, so a Python bot always heartbeats and its
    /// connector must be new enough to decode the variant. The pairwise upgrade is not
    /// optional there.
    pub fn heartbeat_interval(self, interval: Option<Duration>) -> Self {
        Self {
            heartbeat_interval: interval,
            ..self
        }
    }

    /// Builds a live [`LiveBot`] based on the registered connectors and assets.
    pub fn build<CH>(self) -> Result<LiveBot<CH, MD>, BuildError>
    where
        CH: Channel,
    {
        let id = self.id;
        let mut channel = CH::build(&self.instruments)?;

        // Requests to prepare a given asset for trading.
        // The Connector will send the current orders on this asset.
        for (inst_no, instrument) in self.instruments.iter().enumerate() {
            info!(
                connector_name = instrument.connector_name,
                symbol = instrument.symbol,
                "Registers the instrument."
            );
            channel
                .send(
                    id,
                    inst_no,
                    LiveRequest::RegisterInstrument {
                        symbol: instrument.symbol.clone(),
                        tick_size: instrument.tick_size,
                        lot_size: instrument.lot_size,
                    },
                )
                .map_err(|error| BuildError::Error(anyhow::Error::from(error)))?;
        }

        Ok(LiveBot::new(
            id,
            channel,
            self.instruments,
            self.error_handler,
            self.order_hook,
            self.heartbeat_interval,
        ))
    }
}

/// A live trading bot.
///
/// Provides the same interface as the backtesters in [`backtest`](`crate::backtest`).
///
/// ```
/// use hftbacktest::{live::{Instrument, LiveBot}, prelude::HashMapMarketDepth};
///
/// let tick_size = 0.1;
/// let lot_size = 1.0;
///
/// let mut hbt = LiveBot::builder()
///     .register(Instrument::new(
///         "connector_name",
///         "symbol",
///         tick_size,
///         lot_size,
///         HashMapMarketDepth::new(tick_size, lot_size),
///         0
///     ))
///     .build()
///     .unwrap();
/// ```
pub struct LiveBot<CH, MD> {
    id: u64,
    channel: CH,
    instruments: Vec<Instrument<MD>>,
    error_handler: Option<ErrorHandler>,
    order_hook: Option<OrderRecvHook>,
    /// `None` disables the heartbeat. See [`LiveBotBuilder::heartbeat_interval`].
    heartbeat_interval: Option<Duration>,
    /// When the last heartbeat went out; `None` until the first one does.
    last_heartbeat: Option<Instant>,
    /// Name of each connector, in order of first appearance in `instruments`.
    connector_names: Vec<String>,
    /// Which connector each instrument belongs to: an index into `connector_names`, one entry
    /// per instrument. Derived once in [`LiveBot::new`], never afterwards.
    conn_of_inst: Vec<usize>,
    /// Open batches per connector, for the current `elapse_` only — reset at its start, which
    /// is exactly the lifetime the single `batch_mode` flag it replaces had.
    batches: Vec<BatchHold>,
    /// See [`MAX_BATCH_HOLD`]. A field so tests can shorten it; not a builder option, because
    /// it guards against a protocol violation rather than expressing a trading policy.
    max_batch_hold: Duration,
}

impl<CH, MD> LiveBot<CH, MD> {
    /// The one place a [`LiveBot`] is assembled, so that the tables derived from `instruments`
    /// cannot drift out of step with them.
    fn new(
        id: u64,
        channel: CH,
        instruments: Vec<Instrument<MD>>,
        error_handler: Option<ErrorHandler>,
        order_hook: Option<OrderRecvHook>,
        heartbeat_interval: Option<Duration>,
    ) -> Self {
        let mut connector_names: Vec<String> = Vec::new();
        let mut conn_of_inst = Vec::with_capacity(instruments.len());
        for instrument in &instruments {
            let conn = connector_names
                .iter()
                .position(|name| *name == instrument.connector_name)
                .unwrap_or_else(|| {
                    connector_names.push(instrument.connector_name.clone());
                    connector_names.len() - 1
                });
            conn_of_inst.push(conn);
        }
        let batches = vec![BatchHold::default(); connector_names.len()];

        Self {
            id,
            channel,
            instruments,
            error_handler,
            order_hook,
            heartbeat_interval,
            last_heartbeat: None,
            connector_names,
            conn_of_inst,
            batches,
            max_batch_hold: MAX_BATCH_HOLD,
        }
    }

    /// Which connector an instrument belongs to, refusing an index the channel should never
    /// have reported. See the same check at the head of [`Self::process_event`] — this one
    /// exists because the batch arms of `elapse_` reach for the connector *before*
    /// `process_event` runs.
    fn connector_of(&self, inst_no: usize) -> Result<usize, BotError> {
        match self.conn_of_inst.get(inst_no) {
            Some(conn) => Ok(*conn),
            None => {
                error!(
                    %inst_no,
                    len = self.conn_of_inst.len(),
                    "The channel reported an out-of-range instrument."
                );
                Err(BotError::InstrumentNotFound)
            }
        }
    }

    fn open_batch(&mut self, conn: usize, elapsed: Duration) {
        let batch = &mut self.batches[conn];
        if batch.depth == 0 {
            batch.since = Some(elapsed);
        }
        batch.depth = batch.depth.saturating_add(1);
    }

    fn close_batch(&mut self, conn: usize) {
        let batch = &mut self.batches[conn];
        batch.depth = batch.depth.saturating_sub(1);
        if batch.depth == 0 {
            batch.since = None;
        }
    }

    /// Whether any connector's open batch still defers returning to the strategy.
    ///
    /// One rule for every return in `elapse_`, deliberately: the question a gate answers is not
    /// "whose frame is this" but "may the strategy run", and a strategy that runs reads *every*
    /// book, position and order set. Making the gate per-connector — releasing on a foreign
    /// connector's feed while ours is mid-batch — would leave the tear in place on the very
    /// path this exists to protect (design note §3.3).
    fn batch_holds(&self, elapsed: Duration) -> bool {
        self.batches
            .iter()
            .any(|batch| batch.holds(elapsed, self.max_batch_hold))
    }

    fn any_batch_open(&self) -> bool {
        self.batches.iter().any(|batch| batch.depth > 0)
    }

    /// The connectors whose batch is open but has been held past the ceiling — what an operator
    /// needs named, since "something somewhere is open" is not actionable.
    fn stuck_connectors(&self, elapsed: Duration) -> Vec<&str> {
        self.batches
            .iter()
            .enumerate()
            .filter(|(_, batch)| batch.depth > 0 && !batch.holds(elapsed, self.max_batch_hold))
            .map(|(conn, _)| self.connector_names[conn].as_str())
            .collect()
    }
}

impl<CH, MD> LiveBot<CH, MD>
where
    CH: Channel,
    MD: MarketDepth + L2MarketDepth,
{
    fn process_event<const WAIT_NEXT_FEED: bool>(
        &mut self,
        inst_no: usize,
        ev: LiveEvent,
        wait_order_response: WaitOrderResponse,
    ) -> Result<ElapseResult, BotError> {
        // Everything below reaches `instruments` through `get_unchecked_mut`, and the index
        // comes from the channel. One comparison here is what makes those accesses provably in
        // bounds *locally*, instead of resting on an invariant that lives in another file — and
        // it has to be a real check: `debug-assertions` is off in both the dev and the release
        // profile of this workspace, so a `debug_assert!` would exist only in `cargo test`.
        if inst_no >= self.instruments.len() {
            error!(
                %inst_no,
                len = self.instruments.len(),
                "The channel reported an out-of-range instrument."
            );
            return Err(BotError::InstrumentNotFound);
        }
        match ev {
            LiveEvent::Feed { event, .. } => {
                let instrument = unsafe { self.instruments.get_unchecked_mut(inst_no) };
                instrument.last_feed_latency = Some((event.exch_ts, event.local_ts));
                if event.is(LOCAL_BID_DEPTH_EVENT) {
                    instrument
                        .depth
                        .update_bid_depth(event.px, event.qty, event.exch_ts);
                } else if event.is(LOCAL_ASK_DEPTH_EVENT) {
                    instrument
                        .depth
                        .update_ask_depth(event.px, event.qty, event.exch_ts);
                } else if (event.is(LOCAL_BUY_TRADE_EVENT) || event.is(LOCAL_SELL_TRADE_EVENT))
                    && instrument.last_trades.capacity() > 0
                {
                    instrument.last_trades.push(event);
                }
                if WAIT_NEXT_FEED {
                    return Ok(ElapseResult::MarketFeed);
                }
            }
            LiveEvent::Order { order, .. } => {
                debug!(%inst_no, ?order, "Event::Order");
                let received_order_resp = match wait_order_response {
                    WaitOrderResponse::Any => true,
                    WaitOrderResponse::Specified {
                        asset_no: wait_order_asset_no,
                        order_id: wait_order_id,
                    } if wait_order_id == order.order_id && wait_order_asset_no == inst_no => true,
                    _ => false,
                };
                let instrument = unsafe { self.instruments.get_unchecked_mut(inst_no) };
                instrument.last_order_latency = Some((
                    order.local_timestamp,
                    order.exch_timestamp,
                    Utc::now().timestamp_nanos_opt().unwrap(),
                ));
                match instrument.orders.entry(order.order_id) {
                    Entry::Occupied(mut entry) => {
                        let ex_order = entry.get_mut();
                        if let Some(hook) = self.order_hook.as_mut() {
                            hook(ex_order, &order)?;
                        }
                        if order.exch_timestamp >= ex_order.exch_timestamp {
                            if ex_order.status == Status::Canceled
                                || ex_order.status == Status::Expired
                                || ex_order.status == Status::Filled
                            {
                                // Ignores the update since the current status is the final status.
                            } else {
                                ex_order.update(&order);
                            }
                        }
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(order);
                    }
                }
                if received_order_resp {
                    return Ok(ElapseResult::OrderResponse);
                }
            }
            LiveEvent::Position { qty, .. } => {
                let instrument = unsafe { self.instruments.get_unchecked_mut(inst_no) };
                instrument.state.position = qty;
                instrument.position_observed = true;
            }
            LiveEvent::Error(error) => {
                if let Some(handler) = self.error_handler.as_mut() {
                    handler(error)?;
                }
            }
            LiveEvent::SnapshotComplete { .. } => {
                unsafe { self.instruments.get_unchecked_mut(inst_no) }.snapshot_ready = true;
            }
            LiveEvent::BatchStart | LiveEvent::BatchEnd => {
                unreachable!();
            }
        }
        Ok(ElapseResult::Ok)
    }

    /// Tells every connector this bot trades on that its event loop is still turning, if it
    /// is time to.
    ///
    /// One send per instrument, because [`Channel::send`] routes by `inst_no` and the set of
    /// connectors this bot talks to is exactly the set its instruments live on. Two
    /// instruments on one connector make this send twice a second instead of once, which is
    /// two bytes on a shared-memory queue — cheaper than teaching [`Channel`] to enumerate
    /// its connectors, which nothing else needs.
    ///
    /// `now` is passed in rather than read here so that the call sites can reuse a clock
    /// reading `elapse_` already takes: this runs on the receive loop's hot path.
    ///
    /// A failure propagates. It is the same treatment [`Self::submit_order`] gives a failed
    /// send, and it is the honest one: a bot that cannot reach its connector is a bot whose
    /// orders that connector is about to cancel.
    fn heartbeat(&mut self, now: Instant) -> Result<(), BotError> {
        let Some(interval) = self.heartbeat_interval else {
            return Ok(());
        };
        if self
            .last_heartbeat
            .is_some_and(|last| now.saturating_duration_since(last) < interval)
        {
            return Ok(());
        }
        for inst_no in 0..self.instruments.len() {
            self.channel
                .send(self.id, inst_no, LiveRequest::Heartbeat)?;
        }
        self.last_heartbeat = Some(now);
        Ok(())
    }

    /// The longest a single [`Channel::recv_timeout`] may block, given how much of the
    /// `elapse` is left.
    ///
    /// **This is what keeps a bot waiting on a silent feed from looking dead.** A receive
    /// returns when something arrives or when its timeout expires, so an `elapse` that
    /// receives nothing spends its whole duration inside one call — and the heartbeat is
    /// sent from around that call, never from within it. Capping the wait at the heartbeat
    /// interval is what brings the loop back often enough to report. Without it a single
    /// `wait_order_response` — 60 s, hardcoded by [`Self::submit_order`] and
    /// [`Bot::cancel`] — says nothing for six times the connector's default liveness
    /// window, and a healthy bot has its resting orders cancelled underneath it.
    ///
    /// With the heartbeat switched off this is the whole remaining duration: nothing is
    /// waiting to be told anything, so there is nothing to wake up for.
    fn recv_chunk(&self, remaining: Duration) -> Duration {
        match self.heartbeat_interval {
            Some(interval) => remaining.min(interval.max(MIN_RECV_CHUNK)),
            None => remaining,
        }
    }

    fn elapse_<const WAIT_NEXT_FEED: bool>(
        &mut self,
        duration: i64,
        wait_order_response: WaitOrderResponse,
    ) -> Result<ElapseResult, BotError> {
        let instant = Instant::now();
        // On entry, so that a strategy calling `elapse` in a tight loop heartbeats at the
        // configured interval and no faster, and so that the paths below that `return` from
        // inside the loop have already reported.
        self.heartbeat(instant)?;
        let duration = Duration::from_nanos(duration as u64);
        let mut remaining_duration = duration;
        // Per call, exactly as the single `batch_mode` flag this replaces was. A batch that
        // crosses a call boundary is not protected — it never was (design note §10).
        self.batches.fill(BatchHold::default());
        let mut wait_resp_received = false;
        let mut reported_a_stuck_batch = false;

        loop {
            let chunk = self.recv_chunk(remaining_duration);
            match self.channel.recv_timeout(self.id, chunk) {
                Ok((inst_no, ev)) => {
                    // Before anything indexes with it, including the batch arms below.
                    let conn = self.connector_of(inst_no)?;
                    // One clock reading per iteration: the gates, the ceiling and the heartbeat
                    // all ask about the same instant, and this is the receive hot path.
                    let elapsed = instant.elapsed();

                    match ev {
                        LiveEvent::BatchStart => self.open_batch(conn, elapsed),
                        LiveEvent::BatchEnd => self.close_batch(conn),
                        ev => {
                            match self.process_event::<WAIT_NEXT_FEED>(
                                inst_no,
                                ev,
                                wait_order_response,
                            )? {
                                ElapseResult::Ok => {
                                    // Keeps receiving events until the elapsed time is reached.
                                }
                                ElapseResult::EndOfData => {
                                    unreachable!()
                                }
                                ElapseResult::MarketFeed => {
                                    wait_resp_received = true;
                                    if !self.batch_holds(elapsed) {
                                        return Ok(ElapseResult::MarketFeed);
                                    }
                                }
                                ElapseResult::OrderResponse => {
                                    wait_resp_received = true;
                                    if !self.batch_holds(elapsed) {
                                        return Ok(ElapseResult::OrderResponse);
                                    }
                                }
                            }
                        }
                    }

                    let holds = self.batch_holds(elapsed);
                    if !holds && self.any_batch_open() && !reported_a_stuck_batch {
                        // Once per call: the condition stays true for the rest of it.
                        reported_a_stuck_batch = true;
                        warn!(
                            stuck = ?self.stuck_connectors(elapsed),
                            ceiling = ?self.max_batch_hold,
                            "A batch has held this elapse past its ceiling; returning on a \
                             possibly torn book rather than never returning."
                        );
                    }
                    // A result deferred by a batch is released when the last batch closes — or
                    // when the ceiling stops it holding. This is also the path a `BatchEnd`
                    // takes, which is why it no longer returns from its own arm.
                    if wait_resp_received && !holds {
                        return Ok(ElapseResult::Ok);
                    }
                    // Again from inside the loop, so that a single long `elapse` does not look
                    // dead for its whole duration. `instant + elapsed` reuses the reading above
                    // rather than taking another one.
                    self.heartbeat(instant + elapsed)?;
                    // While processing events in batch mode, all events in a batch should be
                    // processed together without interruption.
                    if !holds && elapsed > duration {
                        return Ok(ElapseResult::Ok);
                    }
                    remaining_duration = duration
                        .saturating_sub(elapsed)
                        .max(Duration::from_micros(1));
                }
                Err(BotError::Timeout) => {
                    let elapsed = instant.elapsed();
                    // Reported before deciding anything: this is the only place a bot with
                    // nothing to do passes through, and it is where a quiet feed used to go
                    // silent for the whole `elapse`.
                    self.heartbeat(instant + elapsed)?;
                    // A *capped* receive expiring is not this call expiring. Without the cap
                    // the channel's timeout was the call's, and this is the end of it.
                    if chunk >= remaining_duration || elapsed >= duration {
                        return Ok(ElapseResult::Ok);
                    }
                    remaining_duration = duration
                        .saturating_sub(elapsed)
                        .max(Duration::from_micros(1));
                    continue;
                }
                Err(BotError::Interrupted) => {
                    return Ok(ElapseResult::EndOfData);
                }
                Err(error) => {
                    return Err(error);
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn submit_order(
        &mut self,
        asset_no: usize,
        order_id: u64,
        price: f64,
        qty: f64,
        time_in_force: TimeInForce,
        order_type: OrdType,
        wait: bool,
        side: Side,
    ) -> Result<ElapseResult, BotError> {
        let instrument = self
            .instruments
            .get_mut(asset_no)
            .ok_or(BotError::InstrumentNotFound)?;
        if instrument.orders.contains_key(&order_id) {
            return Err(BotError::OrderIdExist);
        }
        let symbol = instrument.symbol.clone();
        let tick_size = instrument.tick_size;
        let order = Order {
            order_id,
            price_tick: (price / tick_size).round() as i64,
            qty,
            leaves_qty: qty,
            tick_size,
            side,
            time_in_force,
            order_type,
            status: Status::New,
            local_timestamp: Utc::now().timestamp_nanos_opt().unwrap(),
            req: Status::New,
            exec_price_tick: 0,
            exch_timestamp: 0,
            exec_qty: 0.0,
            // Invalid information
            q: Box::new(()),
            maker: false,
        };
        let order_id = order.order_id;
        instrument.orders.insert(order_id, order.clone());

        self.channel
            .send(self.id, asset_no, LiveRequest::Order { symbol, order })?;

        if wait {
            // fixme: timeout should be specified by the argument.
            return self.wait_order_response(asset_no, order_id, 60_000_000_000);
        }
        Ok(ElapseResult::Ok)
    }
}

impl<CH, MD> Bot<MD> for LiveBot<CH, MD>
where
    CH: Channel,
    MD: MarketDepth + L2MarketDepth,
{
    type Error = BotError;

    #[inline]
    fn current_timestamp(&self) -> i64 {
        Utc::now().timestamp_nanos_opt().unwrap()
    }

    #[inline]
    fn num_assets(&self) -> usize {
        self.instruments.len()
    }

    #[inline]
    fn position(&self, asset_no: usize) -> f64 {
        self.state_values(asset_no).position
    }

    #[inline]
    fn snapshot_ready(&self, asset_no: usize) -> bool {
        self.instruments
            .get(asset_no)
            .map(|i| i.snapshot_ready)
            .unwrap_or(false)
    }

    #[inline]
    fn position_observed(&self, asset_no: usize) -> bool {
        self.instruments
            .get(asset_no)
            .map(|i| i.position_observed)
            .unwrap_or(false)
    }

    #[inline]
    fn state_values(&self, asset_no: usize) -> &StateValues {
        // todo: implement the missing fields. Trade values need to be changed to a rolling manner,
        //       unlike the current Python implementation, to support live trading.
        &self.instruments.get(asset_no).unwrap().state
    }

    #[inline]
    fn depth(&self, asset_no: usize) -> &MD {
        &self.instruments.get(asset_no).unwrap().depth
    }

    #[inline]
    fn last_trades(&self, asset_no: usize) -> &[Event] {
        self.instruments
            .get(asset_no)
            .unwrap()
            .last_trades
            .as_slice()
    }

    fn clear_last_trades(&mut self, asset_no: Option<usize>) {
        match asset_no {
            Some(asset_no) => {
                self.instruments
                    .get_mut(asset_no)
                    .unwrap()
                    .last_trades
                    .clear();
            }
            None => {
                for asset_no in 0..self.instruments.len() {
                    self.instruments
                        .get_mut(asset_no)
                        .unwrap()
                        .last_trades
                        .clear();
                }
            }
        }
    }

    #[inline]
    fn orders(&self, asset_no: usize) -> &HashMap<OrderId, Order> {
        &self.instruments.get(asset_no).unwrap().orders
    }

    #[inline]
    fn submit_buy_order(
        &mut self,
        asset_no: usize,
        order_id: OrderId,
        price: f64,
        qty: f64,
        time_in_force: TimeInForce,
        order_type: OrdType,
        wait: bool,
    ) -> Result<ElapseResult, Self::Error> {
        self.submit_order(
            asset_no,
            order_id,
            price,
            qty,
            time_in_force,
            order_type,
            wait,
            Side::Buy,
        )
    }

    #[inline]
    fn submit_sell_order(
        &mut self,
        asset_no: usize,
        order_id: OrderId,
        price: f64,
        qty: f64,
        time_in_force: TimeInForce,
        order_type: OrdType,
        wait: bool,
    ) -> Result<ElapseResult, Self::Error> {
        self.submit_order(
            asset_no,
            order_id,
            price,
            qty,
            time_in_force,
            order_type,
            wait,
            Side::Sell,
        )
    }

    fn submit_order(
        &mut self,
        asset_no: usize,
        order: OrderRequest,
        wait: bool,
    ) -> Result<ElapseResult, Self::Error> {
        self.submit_order(
            asset_no,
            order.order_id,
            order.price,
            order.qty,
            order.time_in_force,
            order.order_type,
            wait,
            order.side,
        )
    }

    #[inline]
    fn modify(
        &mut self,
        asset_no: usize,
        order_id: OrderId,
        price: f64,
        qty: f64,
        wait: bool,
    ) -> Result<ElapseResult, Self::Error> {
        todo!();
    }

    #[inline]
    fn cancel(
        &mut self,
        asset_no: usize,
        order_id: OrderId,
        wait: bool,
    ) -> Result<ElapseResult, Self::Error> {
        let instrument = self
            .instruments
            .get_mut(asset_no)
            .ok_or(BotError::InstrumentNotFound)?;
        let symbol = instrument.symbol.clone();
        let order = instrument
            .orders
            .get_mut(&order_id)
            .ok_or(BotError::OrderNotFound)?;
        if !order.cancellable() {
            return Err(BotError::InvalidOrderStatus);
        }
        order.req = Status::Canceled;
        order.local_timestamp = Utc::now().timestamp_nanos_opt().unwrap();

        self.channel.send(
            self.id,
            asset_no,
            LiveRequest::Order {
                symbol,
                order: order.clone(),
            },
        )?;

        if wait {
            // fixme: timeout should be specified by the argument.
            return self.wait_order_response(asset_no, order_id, 60_000_000_000);
        }
        Ok(ElapseResult::Ok)
    }

    #[inline]
    fn clear_inactive_orders(&mut self, asset_no: Option<usize>) {
        match asset_no {
            Some(inst_no) => {
                if let Some(instrument) = self.instruments.get_mut(inst_no) {
                    instrument.orders.retain(|_, order| order.active());
                }
            }
            None => {
                for instrument in self.instruments.iter_mut() {
                    instrument.orders.retain(|_, order| order.active());
                }
            }
        }
    }

    #[inline]
    fn wait_order_response(
        &mut self,
        asset_no: usize,
        order_id: OrderId,
        timeout: i64,
    ) -> Result<ElapseResult, Self::Error> {
        self.elapse_::<false>(timeout, WaitOrderResponse::Specified { asset_no, order_id })
    }

    #[inline]
    fn wait_next_feed(
        &mut self,
        include_order_resp: bool,
        timeout: i64,
    ) -> Result<ElapseResult, Self::Error> {
        if include_order_resp {
            self.elapse_::<true>(timeout, WaitOrderResponse::Any)
        } else {
            self.elapse_::<true>(timeout, WaitOrderResponse::None)
        }
    }

    #[inline]
    fn elapse(&mut self, duration: i64) -> Result<ElapseResult, Self::Error> {
        self.elapse_::<false>(duration, WaitOrderResponse::None)
    }

    #[inline]
    fn elapse_bt(&mut self, _duration: i64) -> Result<ElapseResult, Self::Error> {
        Ok(ElapseResult::Ok)
    }

    fn close(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn feed_latency(&self, asset_no: usize) -> Option<(i64, i64)> {
        self.instruments.get(asset_no).unwrap().last_feed_latency
    }

    fn order_latency(&self, asset_no: usize) -> Option<(i64, i64, i64)> {
        self.instruments.get(asset_no).unwrap().last_order_latency
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, time::Duration};

    use super::*;
    use crate::{
        depth::HashMapMarketDepth,
        live::{Instrument, ipc::Channel},
        types::{BuildError, LiveEvent, LiveRequest, OrdType, Order, Side, Status, TimeInForce},
    };

    /// In-memory `Channel` for unit testing. `recv_timeout` drains a pre-seeded queue of events;
    /// once drained it returns `Timeout`, which lets `elapse_` terminate deterministically. `send`
    /// just records requests.
    struct MockChannel {
        incoming: VecDeque<(usize, LiveEvent)>,
        sent: Vec<(u64, usize, LiveRequest)>,
    }

    impl MockChannel {
        fn with(events: Vec<(usize, LiveEvent)>) -> Self {
            Self {
                incoming: events.into(),
                sent: Vec::new(),
            }
        }
    }

    impl Channel for MockChannel {
        fn build<MD>(_instruments: &[Instrument<MD>]) -> Result<Self, BuildError>
        where
            Self: Sized,
        {
            Ok(Self {
                incoming: VecDeque::new(),
                sent: Vec::new(),
            })
        }

        fn recv_timeout(
            &mut self,
            _id: u64,
            _timeout: Duration,
        ) -> Result<(usize, LiveEvent), BotError> {
            match self.incoming.pop_front() {
                Some(ev) => Ok(ev),
                None => Err(BotError::Timeout),
            }
        }

        fn send(&mut self, id: u64, inst_no: usize, request: LiveRequest) -> Result<(), BotError> {
            self.sent.push((id, inst_no, request));
            Ok(())
        }
    }

    /// A `Channel` that goes quiet the way the real one does: `recv_timeout` reports
    /// `Timeout` only after the whole timeout it was given has passed.
    ///
    /// [`MockChannel`] returns instantly instead, which is exactly what hid the bug this
    /// exists for — `IceoryxUnifiedChannel::recv_timeout` polls until `elapsed > timeout`,
    /// so one `elapse` against a silent feed blocks for its full duration.
    struct QuietChannel {
        sent: Vec<(u64, usize, LiveRequest)>,
        /// Every timeout `elapse_` asked to block for.
        waits: Vec<Duration>,
    }

    impl Channel for QuietChannel {
        fn build<MD>(_instruments: &[Instrument<MD>]) -> Result<Self, BuildError>
        where
            Self: Sized,
        {
            Ok(Self {
                sent: Vec::new(),
                waits: Vec::new(),
            })
        }

        fn recv_timeout(
            &mut self,
            _id: u64,
            timeout: Duration,
        ) -> Result<(usize, LiveEvent), BotError> {
            self.waits.push(timeout);
            std::thread::sleep(timeout);
            Err(BotError::Timeout)
        }

        fn send(&mut self, id: u64, inst_no: usize, request: LiveRequest) -> Result<(), BotError> {
            self.sent.push((id, inst_no, request));
            Ok(())
        }
    }

    fn make_quiet_bot(symbol: &str) -> LiveBot<QuietChannel, HashMapMarketDepth> {
        LiveBot::new(
            42,
            QuietChannel {
                sent: Vec::new(),
                waits: Vec::new(),
            },
            vec![Instrument::new(
                "mock",
                symbol,
                0.01,
                1.0,
                HashMapMarketDepth::new(0.01, 1.0),
                0,
            )],
            None,
            None,
            Some(DEFAULT_HEARTBEAT_INTERVAL),
        )
    }

    fn make_bot(
        symbols: &[&str],
        events: Vec<(usize, LiveEvent)>,
    ) -> LiveBot<MockChannel, HashMapMarketDepth> {
        let instruments: Vec<_> = symbols.iter().map(|s| ("mock", *s)).collect();
        make_multi_connector_bot(&instruments, events)
    }

    /// A bot whose instruments live on more than one connector — the configuration every batch
    /// and every error in this file is about, and the one `make_bot` cannot express.
    fn make_multi_connector_bot(
        instruments: &[(&str, &str)],
        events: Vec<(usize, LiveEvent)>,
    ) -> LiveBot<MockChannel, HashMapMarketDepth> {
        let instruments = instruments
            .iter()
            .map(|(connector, symbol)| {
                Instrument::new(
                    connector,
                    symbol,
                    0.01,
                    1.0,
                    HashMapMarketDepth::new(0.01, 1.0),
                    0,
                )
            })
            .collect();
        LiveBot::new(
            42,
            MockChannel::with(events),
            instruments,
            None,
            None,
            Some(DEFAULT_HEARTBEAT_INTERVAL),
        )
    }

    /// A bid-side depth update. `qty = 0` removes the level, which is how a real feed empties a
    /// book — and why a mid-batch return is observable as a missing best bid rather than as a
    /// flag.
    fn bid(symbol: &str, px: f64, qty: f64) -> LiveEvent {
        LiveEvent::Feed {
            symbol: symbol.into(),
            event: Event {
                ev: LOCAL_BID_DEPTH_EVENT,
                exch_ts: 1_700_000_000_000_000_000,
                local_ts: 1_700_000_000_000_000_001,
                px,
                qty,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
        }
    }

    /// How many of the scripted events the bot actually consumed before returning.
    fn consumed(bot: &LiveBot<MockChannel, HashMapMarketDepth>, scripted: usize) -> usize {
        scripted - bot.channel.incoming.len()
    }

    fn snapshot_complete(symbol: &str) -> LiveEvent {
        LiveEvent::SnapshotComplete {
            symbol: symbol.into(),
            snapshot_time_ns: 1_700_000_000_000_000_000,
        }
    }

    fn order_event(symbol: &str, order_id: u64, price: f64) -> LiveEvent {
        let mut order = Order::new(
            order_id,
            (price / 0.01).round() as i64,
            0.01,
            1.0,
            Side::Buy,
            OrdType::Limit,
            TimeInForce::GTC,
        );
        order.status = Status::New;
        order.req = Status::New;
        LiveEvent::Order {
            symbol: symbol.into(),
            order,
        }
    }

    #[test]
    fn snapshot_ready_starts_false() {
        let bot = make_bot(&["BTCUSDT"], vec![]);
        assert!(!bot.snapshot_ready(0));
    }

    #[test]
    fn snapshot_complete_flips_ready() {
        let mut bot = make_bot(&["BTCUSDT"], vec![(0, snapshot_complete("BTCUSDT"))]);
        bot.elapse(1_000_000).unwrap();
        assert!(bot.snapshot_ready(0));
    }

    #[test]
    fn snapshot_complete_is_per_asset() {
        let mut bot = make_bot(
            &["BTCUSDT", "ETHUSDT"],
            vec![(0, snapshot_complete("BTCUSDT"))],
        );
        bot.elapse(1_000_000).unwrap();
        assert!(bot.snapshot_ready(0));
        assert!(!bot.snapshot_ready(1));
    }

    #[test]
    fn orders_without_complete_leave_ready_false() {
        let mut bot = make_bot(
            &["BTCUSDT"],
            vec![
                (0, order_event("BTCUSDT", 1, 100.0)),
                (0, order_event("BTCUSDT", 2, 101.0)),
            ],
        );
        bot.elapse(1_000_000).unwrap();
        assert!(!bot.snapshot_ready(0));
        assert_eq!(bot.orders(0).len(), 2);
    }

    #[test]
    fn orders_then_complete_exposes_all_orders_at_ready_time() {
        let mut bot = make_bot(
            &["BTCUSDT"],
            vec![
                (0, order_event("BTCUSDT", 1, 100.0)),
                (0, order_event("BTCUSDT", 2, 101.0)),
                (0, snapshot_complete("BTCUSDT")),
            ],
        );
        bot.elapse(1_000_000).unwrap();
        assert!(bot.snapshot_ready(0));
        assert_eq!(bot.orders(0).len(), 2);
    }

    #[test]
    fn empty_snapshot_still_signals_ready() {
        let mut bot = make_bot(&["BTCUSDT"], vec![(0, snapshot_complete("BTCUSDT"))]);
        bot.elapse(1_000_000).unwrap();
        assert!(bot.snapshot_ready(0));
        assert_eq!(bot.orders(0).len(), 0);
    }

    fn position_event(symbol: &str, qty: f64) -> LiveEvent {
        LiveEvent::Position {
            symbol: symbol.into(),
            qty,
            exch_ts: 1_700_000_000_000_000_000,
        }
    }

    #[test]
    fn position_observed_starts_false() {
        let bot = make_bot(&["BTCUSDT"], vec![]);
        assert!(!bot.position_observed(0));
    }

    /// **The two signals must not collapse into one.** The connector publishes
    /// `SnapshotComplete` synchronously with the registration round trip while
    /// `cancel_all_orders` + `get_position` are still in flight on a spawned task
    /// (`docs/snapshot-complete-marker.md`, "Known gap"; `AGENTS.md` §4.4). A consumer that
    /// wants the sweep behind it therefore has to be able to tell "the marker arrived" from
    /// "the position round trip answered". If this assertion ever fails, the second signal
    /// has been wired to the first and every gate built on it is vacuous.
    #[test]
    fn the_snapshot_marker_alone_reports_no_position() {
        let mut bot = make_bot(&["BTCUSDT"], vec![(0, snapshot_complete("BTCUSDT"))]);
        bot.elapse(1_000_000).unwrap();
        assert!(bot.snapshot_ready(0));
        assert!(!bot.position_observed(0));
    }

    #[test]
    fn a_position_event_marks_the_asset_observed() {
        let mut bot = make_bot(&["BTCUSDT"], vec![(0, position_event("BTCUSDT", -1.5))]);
        bot.elapse(1_000_000).unwrap();
        assert!(bot.position_observed(0));
        assert_eq!(bot.position(0), -1.5);
    }

    /// A flat account answers `get_position` with a zero quantity, and that answer is exactly
    /// as informative as a non-zero one: the REST round trip returned. Keying the flag off
    /// `position != 0.0` instead of "an event arrived" would leave the common case — a bot
    /// restarting flat — waiting forever.
    #[test]
    fn a_zero_position_event_marks_the_asset_observed() {
        let mut bot = make_bot(&["BTCUSDT"], vec![(0, position_event("BTCUSDT", 0.0))]);
        bot.elapse(1_000_000).unwrap();
        assert!(bot.position_observed(0));
    }

    #[test]
    fn the_position_observation_is_per_asset() {
        let mut bot = make_bot(
            &["BTCUSDT", "ETHUSDT"],
            vec![(0, position_event("BTCUSDT", 1.0))],
        );
        bot.elapse(1_000_000).unwrap();
        assert!(bot.position_observed(0));
        assert!(!bot.position_observed(1));
    }

    /// The flag latches: it answers "has this asset ever reported a position", so later
    /// updates — including one that takes the position back to flat — cannot un-observe it.
    #[test]
    fn the_position_observation_latches() {
        let mut bot = make_bot(
            &["BTCUSDT"],
            vec![
                (0, position_event("BTCUSDT", 2.0)),
                (0, position_event("BTCUSDT", 0.0)),
            ],
        );
        bot.elapse(1_000_000).unwrap();
        assert!(bot.position_observed(0));
        assert_eq!(bot.position(0), 0.0);
    }

    #[test]
    fn timeout_without_complete_keeps_ready_false() {
        let mut bot = make_bot(&["BTCUSDT"], vec![]);
        bot.elapse(1_000).unwrap();
        assert!(!bot.snapshot_ready(0));
    }

    fn heartbeats(bot: &LiveBot<MockChannel, HashMapMarketDepth>) -> Vec<(u64, usize)> {
        bot.channel
            .sent
            .iter()
            .filter(|(_, _, request)| matches!(request, LiveRequest::Heartbeat))
            .map(|(id, inst_no, _)| (*id, *inst_no))
            .collect()
    }

    /// **The bot must announce itself before anything else can go wrong.** A connector only
    /// watches a bot that has heartbeated at least once, so the first `elapse` is what arms
    /// the protection — and it has to reach every connector this bot trades on, which is
    /// one send per instrument because `Channel::send` routes by `inst_no`.
    #[test]
    fn the_first_elapse_heartbeats_every_connector_the_bot_trades_on() {
        let mut bot = make_bot(&["BTCUSDT", "ETHUSDT"], vec![]);
        assert!(heartbeats(&bot).is_empty(), "nothing is sent before elapse");

        bot.elapse(1_000).unwrap();

        // The bot's own id rides on each one: it is the key the connector sweeps by.
        assert_eq!(heartbeats(&bot), vec![(42, 0), (42, 1)]);
    }

    /// It is a heartbeat, not a flood. `elapse` is called in a tight loop by every strategy
    /// in this repository; sending on each call would put a message on the IPC queue per
    /// iteration for no added information.
    #[test]
    fn the_heartbeat_does_not_repeat_before_its_interval() {
        let mut bot = make_bot(&["BTCUSDT"], vec![]);
        bot.heartbeat_interval = Some(Duration::from_secs(30));

        for _ in 0..50 {
            bot.elapse(1_000).unwrap();
        }

        assert_eq!(heartbeats(&bot).len(), 1);
    }

    /// ...and it does repeat once the interval has passed. Driven by moving the recorded
    /// send back in time rather than by sleeping, so the test is deterministic.
    #[test]
    fn the_heartbeat_repeats_once_its_interval_has_passed() {
        let mut bot = make_bot(&["BTCUSDT"], vec![]);
        bot.heartbeat_interval = Some(Duration::from_millis(100));
        bot.elapse(1_000).unwrap();
        assert_eq!(heartbeats(&bot).len(), 1);

        bot.last_heartbeat = Some(Instant::now() - Duration::from_millis(101));
        bot.elapse(1_000).unwrap();
        assert_eq!(heartbeats(&bot).len(), 2);
    }

    /// The opt-out is real. A deployment pointed at a connector that predates
    /// `LiveRequest::Heartbeat` must be able to stay quiet — an older connector cannot
    /// decode variant 2 and dies on it.
    #[test]
    fn the_heartbeat_can_be_switched_off_entirely() {
        let mut bot = make_bot(&["BTCUSDT"], vec![]);
        bot.heartbeat_interval = None;

        for _ in 0..10 {
            bot.elapse(1_000).unwrap();
        }

        assert!(heartbeats(&bot).is_empty());
        assert!(bot.last_heartbeat.is_none());
    }

    /// The default has to be on: a bot that does not heartbeat is a bot whose orders nobody
    /// will cancel when it dies, and that is the failure this whole mechanism exists for.
    #[test]
    fn the_heartbeat_is_on_by_default() {
        let bot: LiveBotBuilder<HashMapMarketDepth> = LiveBotBuilder::new();
        assert_eq!(bot.heartbeat_interval, Some(DEFAULT_HEARTBEAT_INTERVAL));
        assert!(
            DEFAULT_HEARTBEAT_INTERVAL < Duration::from_secs(10),
            "the default interval must leave headroom under the connector's default \
             10 s liveness window"
        );

        let quiet: LiveBotBuilder<HashMapMarketDepth> =
            LiveBotBuilder::new().heartbeat_interval(None);
        assert_eq!(quiet.heartbeat_interval, None);
    }

    /// A long `elapse` must still heartbeat while it is inside the loop, not only on entry:
    /// a strategy that elapses for a minute would otherwise look dead for that minute.
    #[test]
    fn a_long_elapse_heartbeats_from_inside_its_loop() {
        // Three events, so the loop body runs three times within one `elapse`.
        let mut bot = make_bot(
            &["BTCUSDT"],
            vec![
                (0, order_event("BTCUSDT", 1, 100.0)),
                (0, order_event("BTCUSDT", 2, 101.0)),
                (0, order_event("BTCUSDT", 3, 102.0)),
            ],
        );
        // Due again immediately, so every pass through the loop qualifies.
        bot.heartbeat_interval = Some(Duration::ZERO);

        // 20 ms rather than a second: a receive is now capped at the heartbeat interval, so
        // the loop keeps turning for the whole duration instead of ending at the first
        // `Timeout`, and a second here would be a second of polling in the test suite.
        bot.elapse(20_000_000).unwrap();

        assert!(
            heartbeats(&bot).len() > 1,
            "the heartbeat must be reachable from inside the receive loop, not just at its \
             head: {:?}",
            heartbeats(&bot)
        );
    }

    fn quiet_heartbeats(bot: &LiveBot<QuietChannel, HashMapMarketDepth>) -> usize {
        bot.channel
            .sent
            .iter()
            .filter(|(_, _, request)| matches!(request, LiveRequest::Heartbeat))
            .count()
    }

    /// **A quiet feed must not look like a dead bot.** This is the case the loop-based test
    /// above cannot reach: nothing arrives at all, so the loop body never runs, and a single
    /// `elapse` blocks inside one `recv_timeout` for its whole duration.
    ///
    /// It is not a corner case. `submit_order(.., wait = true)` and `cancel(.., wait = true)`
    /// both call `wait_order_response` with a hardcoded 60 s, and a public stream that has
    /// stopped delivering (`AGENTS.md` §4.2) is exactly when a strategy sits in
    /// `wait_next_feed`. At the shipped 10 s connector window a healthy bot would have its
    /// resting grid cancelled underneath it while it waits for its own order response.
    #[test]
    fn a_long_elapse_keeps_heartbeating_even_when_nothing_arrives_at_all() {
        let mut bot = make_quiet_bot("BTCUSDT");
        bot.heartbeat_interval = Some(Duration::from_millis(20));

        let started = Instant::now();
        bot.elapse(200_000_000).unwrap();
        let blocked_for = started.elapsed();

        assert!(
            blocked_for >= Duration::from_millis(200),
            "the elapse must still last its full duration: {blocked_for:?}"
        );
        assert!(
            quiet_heartbeats(&bot) >= 5,
            "a bot waiting on a silent feed is alive and must say so; it heartbeated \
             {} time(s) in {blocked_for:?} with a 20 ms interval",
            quiet_heartbeats(&bot)
        );
        // ...and it says so by coming back often, not by blocking for the whole duration.
        assert!(
            bot.channel
                .waits
                .iter()
                .all(|wait| *wait <= Duration::from_millis(20)),
            "no single receive may outlast the heartbeat interval: {:?}",
            bot.channel.waits
        );
    }

    // ---------------------------------------------------------------------------------------
    // Batch isolation. Two connectors publish into the bot through one round-robin receiver,
    // so their batches interleave 1:1 by construction — `AGENTS.md` §2.2 / the design note §2.1.
    // Every assertion below is money-shaped on purpose: it reads the book the strategy would
    // have quoted on, not a flag.
    // ---------------------------------------------------------------------------------------

    const HL_AND_BNC: [(&str, &str); 2] = [("hl", "BTC"), ("bnc", "BTCUSDT")];

    /// **A foreign `BatchEnd` must not release a batch it never opened.** One `bool` for all
    /// connectors means Binance closing its frame hands the strategy Hyperliquid's book with
    /// the old best bid deleted and the new one not yet applied.
    #[test]
    fn a_foreign_batch_end_cannot_release_a_batch_that_is_still_open() {
        let script = vec![
            (0, LiveEvent::BatchStart),
            (0, bid("BTC", 100.0, 5.0)),
            (1, LiveEvent::BatchEnd), // Binance's, and Binance never opened one here.
            (0, bid("BTC", 100.0, 0.0)),
            (0, bid("BTC", 99.0, 5.0)),
            (0, LiveEvent::BatchEnd),
        ];
        let scripted = script.len();
        let mut bot = make_multi_connector_bot(&HL_AND_BNC, script);

        bot.wait_next_feed(false, 1_000_000_000).unwrap();

        assert_eq!(
            bot.depth(0).best_bid(),
            99.0,
            "the strategy must never see the half-applied frame"
        );
        assert_eq!(consumed(&bot, scripted), scripted);
    }

    /// **BLOCKER-1.** The gate decides whether the strategy may run at all, not which frame to
    /// hand it: once it runs it reads *every* book. So a foreign connector's feed cannot
    /// release the return while somebody else's batch is open, even though that feed is
    /// perfectly complete in itself.
    #[test]
    fn an_open_batch_anywhere_defers_a_foreign_connectors_feed() {
        let script = vec![
            (0, LiveEvent::BatchStart),
            (0, bid("BTC", 100.0, 5.0)),
            (1, bid("BTCUSDT", 50.0, 5.0)), // complete, and still not a licence to return
            (0, bid("BTC", 100.0, 0.0)),
            (0, bid("BTC", 99.0, 5.0)),
            (0, LiveEvent::BatchEnd),
        ];
        let scripted = script.len();
        let mut bot = make_multi_connector_bot(&HL_AND_BNC, script);

        bot.wait_next_feed(false, 1_000_000_000).unwrap();

        assert_eq!(
            bot.depth(0).best_bid(),
            99.0,
            "returning on the Binance feed would expose the torn Hyperliquid book"
        );
        assert_eq!(consumed(&bot, scripted), scripted);
    }

    /// The liveness half of the pin above — without it, "never return" would satisfy it.
    /// Also pins the wart the design keeps on purpose (note §10): the release on `BatchEnd`
    /// reports `Ok`, not the deferred `MarketFeed`.
    #[test]
    fn a_return_happens_once_the_last_batch_closes() {
        let script = vec![
            (0, LiveEvent::BatchStart),
            (1, LiveEvent::BatchStart),
            (0, bid("BTC", 100.0, 5.0)),
            (1, LiveEvent::BatchEnd),    // one down, one to go
            (0, LiveEvent::BatchEnd),    // now the bot may return
            (0, bid("BTC", 101.0, 5.0)), // must be left for the next elapse
        ];
        let scripted = script.len();
        let mut bot = make_multi_connector_bot(&HL_AND_BNC, script);

        let result = bot.wait_next_feed(false, 1_000_000_000).unwrap();

        assert_eq!(result, ElapseResult::Ok, "the kept wart, note §10");
        assert_eq!(
            consumed(&bot, scripted),
            5,
            "it must return at the last BatchEnd: not earlier (a foreign one), not later"
        );
        assert_eq!(bot.depth(0).best_bid(), 100.0);
    }

    /// The same rule for a deferred order response: it waits for the last batch, not the first.
    #[test]
    fn a_pending_response_is_released_only_when_the_last_batch_closes() {
        let script = vec![
            (0, LiveEvent::BatchStart),
            (1, LiveEvent::BatchStart),
            (0, order_event("BTC", 7, 100.0)),
            (1, LiveEvent::BatchEnd),
            (0, LiveEvent::BatchEnd),
            (0, order_event("BTC", 8, 101.0)),
        ];
        let scripted = script.len();
        let mut bot = make_multi_connector_bot(&HL_AND_BNC, script);

        bot.wait_order_response(0, 7, 1_000_000_000).unwrap();

        assert_eq!(consumed(&bot, scripted), 5);
    }

    /// **Nesting inside one connector is real.** A registration batch is assembled whole inside
    /// one `handle_msg` (`connector/src/main.rs`) while a market-data batch is several separate
    /// messages (`hyperliquid/public_stream.rs`), so the registration one can land between the
    /// outer `BatchStart` and its `BatchEnd`. A flag would let the inner end release the outer
    /// batch — on every registration, i.e. at startup, exactly when the bot is about to quote.
    #[test]
    fn a_nested_batch_is_not_released_by_its_inner_batch_end() {
        let script = vec![
            (0, LiveEvent::BatchStart), // market data
            (0, bid("BTC", 100.0, 5.0)),
            (0, LiveEvent::BatchStart), // registration, nested
            (0, LiveEvent::BatchEnd),   // ...and its end
            (0, bid("BTC", 100.0, 0.0)),
            (0, bid("BTC", 99.0, 5.0)),
            (0, LiveEvent::BatchEnd), // the outer one
        ];
        let scripted = script.len();
        let mut bot = make_multi_connector_bot(&[("hl", "BTC")], script);

        bot.wait_next_feed(false, 1_000_000_000).unwrap();

        assert_eq!(bot.depth(0).best_bid(), 99.0);
        assert_eq!(consumed(&bot, scripted), scripted);
    }

    /// Executable documentation for the residual the design accepts (note §3.6): isolation is
    /// per **connector**, not per instrument, because the markers carry no symbol. A batch on
    /// one Hyperliquid coin defers the return for every other coin on that connector. Change
    /// this only together with the note.
    #[test]
    fn any_open_batch_defers_an_unrelated_symbols_return() {
        let script = vec![
            (0, LiveEvent::BatchStart), // opened for BTC's frame...
            (1, bid("ETH", 50.0, 5.0)), // ...defers ETH, a different instrument
            (1, bid("ETH", 50.0, 0.0)),
            (1, bid("ETH", 49.0, 5.0)),
            (0, LiveEvent::BatchEnd),
            (1, bid("ETH", 55.0, 5.0)),
        ];
        let scripted = script.len();
        let mut bot = make_multi_connector_bot(&[("hl", "BTC"), ("hl", "ETH")], script);

        bot.wait_next_feed(false, 1_000_000_000).unwrap();

        assert_eq!(bot.depth(1).best_bid(), 49.0);
        assert_eq!(consumed(&bot, scripted), 5);
    }

    /// The `usize` from the channel indexes `instruments` through `get_unchecked_mut`. A
    /// receiver that reports one out of range is a bug in the receiver, but the bot must say so
    /// rather than write past the end of its own state — and it must say so *before* the batch
    /// arms index the connector table, which they do earlier than `process_event` runs.
    #[test]
    fn an_out_of_range_instrument_is_reported_not_indexed() {
        let mut bot = make_bot(&["BTCUSDT"], vec![(9, bid("BTCUSDT", 100.0, 5.0))]);
        assert!(matches!(
            bot.elapse(1_000_000),
            Err(BotError::InstrumentNotFound)
        ));

        let mut bot = make_bot(&["BTCUSDT"], vec![(9, LiveEvent::BatchStart)]);
        assert!(matches!(
            bot.elapse(1_000_000),
            Err(BotError::InstrumentNotFound)
        ));

        let mut bot = make_bot(&["BTCUSDT"], vec![(9, LiveEvent::BatchEnd)]);
        assert!(matches!(
            bot.elapse(1_000_000),
            Err(BotError::InstrumentNotFound)
        ));
    }

    // ---------------------------------------------------------------------------------------
    // The hold ceiling.
    // ---------------------------------------------------------------------------------------

    /// A connector that opens a batch and never closes it, on a feed that never goes quiet —
    /// the shape a lost `BatchEnd` leaves behind, and the one case where deferring the return
    /// has no natural end.
    ///
    /// It hands out a bounded number of frames and then **fails** rather than looping forever,
    /// so a broken ceiling shows up as a red test with a message instead of a hung run. This
    /// repository has no harness timeout to fall back on.
    struct StuckBatchChannel {
        opened: bool,
        /// Handed out once, right after the batch opens: whatever this `elapse` is waiting for.
        trigger: Option<LiveEvent>,
        /// Frames left before the channel declares the ceiling broken.
        budget: usize,
        sent: Vec<(u64, usize, LiveRequest)>,
    }

    /// Real time has to move for a ceiling measured in real time to expire, so each receive
    /// costs a little of it. The ceiling in these tests is 5 ms, so the good case ends after a
    /// handful of frames and the broken one burns its budget in well under a second.
    const STUCK_FRAME: Duration = Duration::from_millis(1);

    impl Channel for StuckBatchChannel {
        fn build<MD>(_instruments: &[Instrument<MD>]) -> Result<Self, BuildError>
        where
            Self: Sized,
        {
            Ok(Self {
                opened: false,
                trigger: None,
                budget: 400,
                sent: Vec::new(),
            })
        }

        fn recv_timeout(
            &mut self,
            _id: u64,
            _timeout: Duration,
        ) -> Result<(usize, LiveEvent), BotError> {
            std::thread::sleep(STUCK_FRAME);
            if !self.opened {
                self.opened = true;
                return Ok((0, LiveEvent::BatchStart));
            }
            if let Some(trigger) = self.trigger.take() {
                return Ok((0, trigger));
            }
            self.budget = self.budget.saturating_sub(1);
            if self.budget == 0 {
                return Err(BotError::Custom(
                    "the batch held this elapse past every plausible ceiling".to_string(),
                ));
            }
            Ok((0, bid("BTCUSDT", 100.0, 5.0)))
        }

        fn send(&mut self, id: u64, inst_no: usize, request: LiveRequest) -> Result<(), BotError> {
            self.sent.push((id, inst_no, request));
            Ok(())
        }
    }

    fn make_stuck_batch_bot(
        trigger: Option<LiveEvent>,
    ) -> LiveBot<StuckBatchChannel, HashMapMarketDepth> {
        LiveBot::new(
            42,
            StuckBatchChannel {
                opened: false,
                trigger,
                budget: 400,
                sent: Vec::new(),
            },
            vec![Instrument::new(
                "hl",
                "BTCUSDT",
                0.01,
                1.0,
                HashMapMarketDepth::new(0.01, 1.0),
                0,
            )],
            None,
            None,
            Some(DEFAULT_HEARTBEAT_INTERVAL),
        )
    }

    /// **An unclosed batch must not hold the bot for ever.** It keeps heartbeating from inside
    /// the loop, so its connector goes on considering it alive while it has silently stopped
    /// quoting — and per-connector batch state removed the accidental cure, where any other
    /// connector's `BatchEnd` cleared the one shared flag.
    #[test]
    fn an_orphaned_batch_start_cannot_hold_the_bot_past_its_hold_ceiling() {
        let mut bot = make_stuck_batch_bot(None);
        bot.max_batch_hold = Duration::from_millis(5);

        let started = Instant::now();
        let result = bot.elapse(1_000_000).unwrap();
        let held = started.elapsed();

        assert_eq!(result, ElapseResult::Ok);
        assert!(
            held >= Duration::from_millis(5),
            "the batch must defer the return until the ceiling, not sail through it: {held:?}"
        );
        assert!(
            held < Duration::from_millis(200),
            "and the ceiling must end the hold: {held:?}"
        );
    }

    /// **The path the ceiling has to bound is the 60-second one.** `submit_order(.., wait =
    /// true)` and `cancel(.., wait = true)` both wait a hardcoded 60 s for their own response;
    /// with the response already received and deferred behind an unclosed batch, only the
    /// ceiling can hand it over. Returning at the duration gate — which is what a ceiling
    /// written as an early return in that gate would do — is a minute too late.
    #[test]
    fn a_wait_order_response_is_released_at_the_hold_ceiling() {
        let mut bot = make_stuck_batch_bot(Some(order_event("BTCUSDT", 7, 100.0)));
        bot.max_batch_hold = Duration::from_millis(5);

        let started = Instant::now();
        let result = bot.wait_order_response(0, 7, 60_000_000_000).unwrap();
        let held = started.elapsed();

        assert_eq!(result, ElapseResult::Ok);
        assert!(
            held < Duration::from_millis(200),
            "the deferred response must be released at the ceiling, not at the 60 s timeout: \
             {held:?}"
        );
        assert_eq!(bot.orders(0).len(), 1, "and the response was applied");
    }

    /// The ceiling arithmetic and the name it reports, on a clock the test controls rather than
    /// one it waits for. "Something somewhere is open" is not something an operator can act on.
    #[test]
    fn the_ceiling_names_the_connector_whose_batch_is_stuck() {
        let mut bot = make_multi_connector_bot(&HL_AND_BNC, vec![]);
        bot.max_batch_hold = Duration::from_millis(5);

        bot.open_batch(1, Duration::from_millis(10)); // Binance opened one at t = 10 ms

        assert!(bot.batch_holds(Duration::from_millis(14)));
        assert!(
            bot.stuck_connectors(Duration::from_millis(14)).is_empty(),
            "a batch inside its ceiling is not stuck"
        );

        assert!(!bot.batch_holds(Duration::from_millis(16)));
        assert_eq!(bot.stuck_connectors(Duration::from_millis(16)), vec!["bnc"]);

        // Closing it takes the hold away entirely, ceiling or no ceiling.
        bot.close_batch(1);
        assert!(!bot.batch_holds(Duration::from_millis(14)));
        assert!(bot.stuck_connectors(Duration::from_millis(16)).is_empty());
    }

    // ---------------------------------------------------------------------------------------
    // Single-connector equivalence.
    // ---------------------------------------------------------------------------------------

    /// The gates as they were before per-connector batch state, transcribed by hand from
    /// `elapse_` and deliberately *not* sharing code with it.
    #[derive(Copy, Clone, Debug)]
    enum RefEvent {
        BatchStart,
        BatchEnd,
        /// Whatever this call is waiting for: a feed under `wait_next_feed`, the matching order
        /// response under `wait_order_response`.
        Trigger,
        /// An event that is applied and asks for nothing.
        Inert,
    }

    fn reference_gates(trace: &[RefEvent], trigger: ElapseResult) -> (ElapseResult, usize) {
        let mut batch_mode = false;
        let mut wait_resp_received = false;
        for (i, ev) in trace.iter().enumerate() {
            match ev {
                RefEvent::BatchStart => batch_mode = true,
                RefEvent::BatchEnd => {
                    batch_mode = false;
                    if wait_resp_received {
                        return (ElapseResult::Ok, i + 1);
                    }
                }
                RefEvent::Trigger => {
                    wait_resp_received = true;
                    if !batch_mode {
                        return (trigger, i + 1);
                    }
                }
                RefEvent::Inert => {}
            }
        }
        // The queue ran dry: the channel reports `Timeout` and the call ends.
        (ElapseResult::Ok, trace.len())
    }

    /// Deterministic and dependency-free, so the 10k traces below are the same on every run.
    struct XorShift(u64);

    impl XorShift {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    /// Balanced, non-nested batches only. The two known divergences from the old behaviour —
    /// the hold ceiling and nesting (note §3.5) — are deliberate, and mixing them in here would
    /// pin the old behaviour as the reference for cases where we changed it on purpose. They
    /// have their own pins.
    fn random_trace(rng: &mut XorShift) -> Vec<RefEvent> {
        let len = 1 + rng.below(12) as usize;
        let mut trace = Vec::with_capacity(len + 1);
        let mut in_batch = false;
        for _ in 0..len {
            let roll = rng.below(10);
            if in_batch {
                match roll {
                    0..=3 => {
                        trace.push(RefEvent::BatchEnd);
                        in_batch = false;
                    }
                    4..=6 => trace.push(RefEvent::Trigger),
                    _ => trace.push(RefEvent::Inert),
                }
            } else {
                match roll {
                    0..=2 => {
                        trace.push(RefEvent::BatchStart);
                        in_batch = true;
                    }
                    3..=6 => trace.push(RefEvent::Trigger),
                    _ => trace.push(RefEvent::Inert),
                }
            }
        }
        if in_batch {
            trace.push(RefEvent::BatchEnd);
        }
        trace
    }

    /// **One connector must behave exactly as it did.** `myhft` runs one today, and the whole
    /// point of this change is to not pay for the second one with a regression on the first.
    #[test]
    fn the_single_connector_gates_match_the_previous_implementation() {
        let mut rng = XorShift(0x5EED_1234_ABCD_9876);

        for case in 0..10_000 {
            let trace = random_trace(&mut rng);

            for waiting_for_a_feed in [true, false] {
                let script: Vec<(usize, LiveEvent)> = trace
                    .iter()
                    .map(|ev| {
                        let live = match ev {
                            RefEvent::BatchStart => LiveEvent::BatchStart,
                            RefEvent::BatchEnd => LiveEvent::BatchEnd,
                            RefEvent::Trigger if waiting_for_a_feed => bid("BTCUSDT", 100.0, 5.0),
                            RefEvent::Trigger => order_event("BTCUSDT", 7, 100.0),
                            RefEvent::Inert if waiting_for_a_feed => position_event("BTCUSDT", 1.0),
                            RefEvent::Inert => bid("BTCUSDT", 100.0, 5.0),
                        };
                        (0, live)
                    })
                    .collect();
                let scripted = script.len();
                let mut bot = make_bot(&["BTCUSDT"], script);

                let (result, events) = if waiting_for_a_feed {
                    (
                        bot.wait_next_feed(false, 1_000_000_000).unwrap(),
                        ElapseResult::MarketFeed,
                    )
                } else {
                    (
                        bot.wait_order_response(0, 7, 1_000_000_000).unwrap(),
                        ElapseResult::OrderResponse,
                    )
                };
                let expected = reference_gates(&trace, events);

                assert_eq!(
                    (result, consumed(&bot, scripted)),
                    expected,
                    "case {case} (waiting_for_a_feed = {waiting_for_a_feed}): {trace:?}"
                );
            }
        }
    }

    /// The opt-out keeps the old shape exactly: one receive for the whole duration, and no
    /// waking up to say anything. A bot pointed at a connector that predates the heartbeat
    /// must not pay for a mechanism it has switched off.
    #[test]
    fn a_bot_with_the_heartbeat_off_still_blocks_for_the_whole_elapse() {
        let mut bot = make_quiet_bot("BTCUSDT");
        bot.heartbeat_interval = None;

        bot.elapse(30_000_000).unwrap();

        assert_eq!(quiet_heartbeats(&bot), 0);
        assert_eq!(
            bot.channel.waits,
            vec![Duration::from_millis(30)],
            "with no heartbeat there is nothing to wake up for"
        );
    }
}
