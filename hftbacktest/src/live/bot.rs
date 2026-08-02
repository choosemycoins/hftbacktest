use std::{
    collections::{HashMap, hash_map::Entry},
    time::{Duration, Instant},
};

use chrono::Utc;
use rand::Rng;
use thiserror::Error;
use tracing::{debug, error, info};

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
    /// The requested operation is not supported by this [`Bot`] implementation.
    ///
    /// Returned by [`LiveBot::modify`]: live trading offers cancel + submit only. A strategy
    /// that is green in backtesting — where `modify` is implemented — degrades to this
    /// recoverable error instead of aborting the live process, and the caller's
    /// `error_handler` decides fatal-vs-recoverable (`AGENTS.md` §1.1, §4.3).
    #[error("Unsupported")]
    Unsupported,
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

        Ok(LiveBot {
            id,
            channel,
            instruments: self.instruments,
            error_handler: self.error_handler,
            order_hook: self.order_hook,
            heartbeat_interval: self.heartbeat_interval,
            last_heartbeat: None,
        })
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
                            if ex_order.status.is_terminal() {
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
        let mut batch_mode = false;
        let mut wait_resp_received = false;

        loop {
            let chunk = self.recv_chunk(remaining_duration);
            match self.channel.recv_timeout(self.id, chunk) {
                Ok((_, LiveEvent::BatchStart)) => {
                    batch_mode = true;
                }
                Ok((_, LiveEvent::BatchEnd)) => {
                    batch_mode = false;
                    // If batch event processing ends and the waiting response has already been
                    // received, return immediately without checking the elapsed time.
                    if wait_resp_received {
                        return Ok(ElapseResult::Ok);
                    }
                }
                Ok((inst_no, ev)) => {
                    match self.process_event::<WAIT_NEXT_FEED>(inst_no, ev, wait_order_response)? {
                        ElapseResult::Ok => {
                            // Keeps receiving events until the elapsed time is reached.
                        }
                        ElapseResult::EndOfData => {
                            unreachable!()
                        }
                        ElapseResult::MarketFeed => {
                            wait_resp_received = true;
                            if !batch_mode {
                                return Ok(ElapseResult::MarketFeed);
                            }
                        }
                        ElapseResult::OrderResponse => {
                            wait_resp_received = true;
                            if !batch_mode {
                                return Ok(ElapseResult::OrderResponse);
                            }
                        }
                    }
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

            let elapsed = instant.elapsed();
            // Again from inside the loop, so that a single long `elapse` does not look dead
            // for its whole duration. `instant + elapsed` reuses the reading above rather
            // than taking another one.
            self.heartbeat(instant + elapsed)?;
            // While processing events in batch mode, all events in a batch should be processed
            // together without interruption.
            if !batch_mode && elapsed > duration {
                return Ok(ElapseResult::Ok);
            }
            remaining_duration = duration
                .saturating_sub(elapsed)
                .max(Duration::from_micros(1));
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

    /// `modify` is not supported in live trading — only cancel + submit are — so this always
    /// returns [`BotError::Unsupported`].
    ///
    /// Backtesting *does* implement `modify`, so a strategy green in replay can call it here.
    /// This used to be a `todo!()`, which under `panic = "abort"` (release profile) would kill
    /// the live process mid-session. Returning a recoverable error instead lets the caller's
    /// `error_handler` rule on it (fail-closed by policy, `AGENTS.md` §1.1, §4.3). Parameters
    /// are unused by design.
    #[inline]
    fn modify(
        &mut self,
        _asset_no: usize,
        _order_id: OrderId,
        _price: f64,
        _qty: f64,
        _wait: bool,
    ) -> Result<ElapseResult, Self::Error> {
        Err(BotError::Unsupported)
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
        LiveBot {
            id: 42,
            channel: QuietChannel {
                sent: Vec::new(),
                waits: Vec::new(),
            },
            instruments: vec![Instrument::new(
                "mock",
                symbol,
                0.01,
                1.0,
                HashMapMarketDepth::new(0.01, 1.0),
                0,
            )],
            error_handler: None,
            order_hook: None,
            heartbeat_interval: Some(DEFAULT_HEARTBEAT_INTERVAL),
            last_heartbeat: None,
        }
    }

    fn make_bot(
        symbols: &[&str],
        events: Vec<(usize, LiveEvent)>,
    ) -> LiveBot<MockChannel, HashMapMarketDepth> {
        let instruments = symbols
            .iter()
            .map(|s| Instrument::new("mock", s, 0.01, 1.0, HashMapMarketDepth::new(0.01, 1.0), 0))
            .collect();
        LiveBot {
            id: 42,
            channel: MockChannel::with(events),
            instruments,
            error_handler: None,
            order_hook: None,
            heartbeat_interval: Some(DEFAULT_HEARTBEAT_INTERVAL),
            last_heartbeat: None,
        }
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

    /// `modify` is not supported in live trading (only cancel + submit are), but a strategy
    /// green in backtest — where `modify` *is* implemented — can call it. It must degrade to a
    /// recoverable `Err`, never panic: this was a `todo!()`, and `panic = "abort"` in the
    /// release profile turns that panic into a process kill mid-session. AGENTS.md §4.3.
    #[test]
    fn live_modify_returns_unsupported_error_instead_of_panicking() {
        let mut bot = make_bot(&["BTCUSDT"], vec![]);

        // Reaching this assertion at all proves it did not panic; the harness would report a
        // panic as a failure otherwise.
        let result = bot.modify(0, 1, 100.0, 1.0, false);

        assert!(
            matches!(result, Err(BotError::Unsupported)),
            "live modify must return Err(Unsupported), got {result:?}"
        );
    }
}
