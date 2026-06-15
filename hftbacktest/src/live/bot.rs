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
    live::{Instrument, QuotesAccum, ReconcileAccum, SpotOrderAccum, ipc::Channel},
    types::{
        AccountTotals,
        Bot,
        BuildError,
        ElapseResult,
        Event,
        FundingRow,
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
        PerpQuote,
        QuotesFrame,
        QuotesOutcome,
        ReconcileFrame,
        ReconcileOutcome,
        ReconcileScope,
        ReconcileWalletCoin,
        Side,
        SpotOrderAck,
        SpotOrderAction,
        SpotOrderFrame,
        SpotOrderOutcome,
        SpotQuote,
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
    #[error("Custom: {0}")]
    Custom(String),
}

pub type ErrorHandler = Box<dyn Fn(LiveError) -> Result<(), BotError>>;
pub type OrderRecvHook = Box<dyn Fn(&Order, &Order) -> Result<(), BotError>>;

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
                unsafe { self.instruments.get_unchecked_mut(inst_no) }
                    .state
                    .position = qty;
            }
            LiveEvent::Error(error) => {
                if let Some(handler) = self.error_handler.as_mut() {
                    handler(error)?;
                }
            }
            LiveEvent::SnapshotComplete { .. } => {
                unsafe { self.instruments.get_unchecked_mut(inst_no) }.snapshot_ready = true;
            }
            LiveEvent::Reconcile { frame, .. } => {
                // Accumulate reconcile rows between Begin and End into `reconcile_accum`, then
                // finalize into the separate `last_reconcile` on a matching `End` (§4). The fresh
                // REST truth is kept apart from `orders`/`state` (the WS-cached view) so R-M1b can
                // compare the two. Falls through to Ok(ElapseResult::Ok) (polling consumer API).
                let inst = unsafe { self.instruments.get_unchecked_mut(inst_no) };
                match frame {
                    ReconcileFrame::Begin {
                        request_id,
                        snapshot_ts_ns,
                    } => {
                        inst.reconcile_accum =
                            Some(ReconcileAccum::new(request_id, snapshot_ts_ns));
                    }
                    ReconcileFrame::Account {
                        total_equity,
                        total_wallet_balance,
                        total_margin_balance,
                        total_available_balance,
                        total_perp_upl,
                        total_initial_margin,
                        total_maintenance_margin,
                        account_im_rate,
                        account_mm_rate,
                    } => {
                        if let Some(accum) = inst.reconcile_accum.as_mut() {
                            accum.set_account(AccountTotals {
                                total_equity,
                                total_wallet_balance,
                                total_margin_balance,
                                total_available_balance,
                                total_perp_upl,
                                total_initial_margin,
                                total_maintenance_margin,
                                account_im_rate,
                                account_mm_rate,
                            });
                        }
                    }
                    ReconcileFrame::Position { qty } => {
                        if let Some(accum) = inst.reconcile_accum.as_mut() {
                            accum.push_position(qty);
                        }
                    }
                    ReconcileFrame::OpenOrder { order } => {
                        if let Some(accum) = inst.reconcile_accum.as_mut() {
                            accum.push_open_order(order);
                        }
                    }
                    ReconcileFrame::WalletCoin {
                        coin,
                        wallet_balance,
                        equity,
                        usd_value,
                        spot_hedging_qty,
                        borrow_amount,
                        collateral_switch,
                        margin_collateral,
                    } => {
                        if let Some(accum) = inst.reconcile_accum.as_mut() {
                            accum.push_wallet_coin(ReconcileWalletCoin {
                                coin,
                                wallet_balance,
                                equity,
                                usd_value,
                                spot_hedging_qty,
                                borrow_amount,
                                collateral_switch,
                                margin_collateral,
                            });
                        }
                    }
                    ReconcileFrame::End {
                        request_id,
                        ok,
                        endpoint,
                        ret_code,
                        ret_msg,
                        http_status,
                    } => {
                        // `take()` always drops the in-flight accum. Only a matching `request_id`
                        // finalizes it into `last_reconcile`; a mismatched id (or a `Begin` with
                        // no matching `End`) leaves `last_reconcile` unchanged (fail-closed).
                        if let Some(accum) = inst.reconcile_accum.take()
                            && accum.request_id() == request_id
                        {
                            inst.last_reconcile =
                                Some(accum.finish(ok, ret_code, ret_msg, endpoint, http_status));
                        }
                    }
                }
            }
            LiveEvent::SpotOrderReply { frame, .. } => {
                // Accumulate the spot-order stream (Begin → Ack? → End) into `spot_order_accum`,
                // then finalize into `last_spot_order` on a matching `End` (B-M1a). Mirrors the
                // reconcile arm: same fail-closed id-matching discipline, falls through to
                // Ok(ElapseResult::Ok) (polling consumer API).
                let inst = unsafe { self.instruments.get_unchecked_mut(inst_no) };
                match frame {
                    SpotOrderFrame::Begin {
                        request_id,
                        snapshot_ts_ns,
                    } => {
                        inst.spot_order_accum =
                            Some(SpotOrderAccum::new(request_id, snapshot_ts_ns));
                    }
                    SpotOrderFrame::Ack {
                        order_link_id,
                        cum_exec_qty,
                        avg_price,
                        status,
                    } => {
                        if let Some(accum) = inst.spot_order_accum.as_mut() {
                            accum.set_ack(SpotOrderAck {
                                order_link_id,
                                cum_exec_qty,
                                avg_price,
                                status,
                            });
                        }
                    }
                    SpotOrderFrame::End {
                        request_id,
                        ok,
                        ret_code,
                        ret_msg,
                        http_status,
                    } => {
                        // `take()` always drops the in-flight accum. Only a matching `request_id`
                        // finalizes it into `last_spot_order`; a mismatched id (or a `Begin` with no
                        // matching `End`) leaves `last_spot_order` unchanged (fail-closed).
                        if let Some(accum) = inst.spot_order_accum.take()
                            && accum.request_id() == request_id
                        {
                            inst.last_spot_order =
                                Some(accum.finish(ok, ret_code, ret_msg, http_status));
                        }
                    }
                }
            }
            LiveEvent::QuotesReply { frame, .. } => {
                // Accumulate the quotes stream (Begin → PerpQuote → SpotQuote → FundingRow* → End)
                // into `quotes_accum`, then finalize into `last_quotes` on a matching `End` (B-M1b).
                // Mirrors the spot-order arm: same fail-closed id-matching discipline, falls through
                // to Ok(ElapseResult::Ok) (polling consumer API).
                let inst = unsafe { self.instruments.get_unchecked_mut(inst_no) };
                match frame {
                    QuotesFrame::Begin {
                        request_id,
                        snapshot_ts_ns,
                    } => {
                        inst.quotes_accum = Some(QuotesAccum::new(request_id, snapshot_ts_ns));
                    }
                    QuotesFrame::PerpQuote {
                        server_ts_ms,
                        bid,
                        ask,
                        last,
                        funding_rate,
                        next_funding_time_ms,
                        funding_interval_min,
                    } => {
                        if let Some(accum) = inst.quotes_accum.as_mut() {
                            accum.set_perp(PerpQuote {
                                server_ts_ms,
                                bid,
                                ask,
                                last,
                                funding_rate,
                                next_funding_time_ms,
                                funding_interval_min,
                            });
                        }
                    }
                    QuotesFrame::SpotQuote {
                        server_ts_ms,
                        bid,
                        ask,
                        last,
                    } => {
                        if let Some(accum) = inst.quotes_accum.as_mut() {
                            accum.set_spot(SpotQuote {
                                server_ts_ms,
                                bid,
                                ask,
                                last,
                            });
                        }
                    }
                    QuotesFrame::FundingRow {
                        funding_time_ms,
                        funding_rate,
                    } => {
                        if let Some(accum) = inst.quotes_accum.as_mut() {
                            accum.push_funding_row(FundingRow {
                                funding_time_ms,
                                funding_rate,
                            });
                        }
                    }
                    QuotesFrame::End {
                        request_id,
                        ok,
                        ret_code,
                        ret_msg,
                        http_status,
                    } => {
                        // `take()` always drops the in-flight accum. Only a matching `request_id`
                        // finalizes it into `last_quotes`; a mismatched id (or a `Begin` with no
                        // matching `End`) leaves `last_quotes` unchanged (fail-closed).
                        if let Some(accum) = inst.quotes_accum.take()
                            && accum.request_id() == request_id
                        {
                            inst.last_quotes =
                                Some(accum.finish(ok, ret_code, ret_msg, http_status));
                        }
                    }
                }
            }
            LiveEvent::BatchStart | LiveEvent::BatchEnd => {
                unreachable!();
            }
        }
        Ok(ElapseResult::Ok)
    }

    fn elapse_<const WAIT_NEXT_FEED: bool>(
        &mut self,
        duration: i64,
        wait_order_response: WaitOrderResponse,
    ) -> Result<ElapseResult, BotError> {
        let instant = Instant::now();
        let duration = Duration::from_nanos(duration as u64);
        let mut remaining_duration = duration;
        let mut batch_mode = false;
        let mut wait_resp_received = false;

        loop {
            match self.channel.recv_timeout(self.id, remaining_duration) {
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
                    return Ok(ElapseResult::Ok);
                }
                Err(BotError::Interrupted) => {
                    return Ok(ElapseResult::EndOfData);
                }
                Err(error) => {
                    return Err(error);
                }
            }

            let elapsed = instant.elapsed();
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

    /// Requests an on-demand REST reconcile of account state for `asset_no` (R-M1a, §4). Mirrors
    /// `submit_order`/`cancel`: resolves the instrument's `symbol` and sends a
    /// [`LiveRequest::Reconcile`] over the channel. The connector replies asynchronously with a
    /// `BatchStart`-bracketed group of [`LiveEvent::Reconcile`] frames keyed by `request_id`; the
    /// result is polled via [`Bot::last_reconcile`]. Non-blocking — does not wait for the reply.
    pub fn request_reconcile(
        &mut self,
        asset_no: usize,
        request_id: u64,
        scope: ReconcileScope,
    ) -> Result<(), BotError> {
        let instrument = self
            .instruments
            .get(asset_no)
            .ok_or(BotError::InstrumentNotFound)?;
        let symbol = instrument.symbol.clone();
        self.channel.send(
            self.id,
            asset_no,
            LiveRequest::Reconcile {
                symbol,
                request_id,
                scope,
            },
        )
    }

    /// Requests an on-demand spot-order op (place/cancel/status) for `asset_no` (B-M1a). Mirrors
    /// `request_reconcile`: resolves the instrument's `symbol` and sends a
    /// [`LiveRequest::SpotOrder`] over the channel. The connector replies asynchronously with a
    /// `BatchStart`-bracketed group of [`LiveEvent::SpotOrderReply`] frames keyed by `request_id`;
    /// the result is polled via [`Bot::last_spot_order`]. Non-blocking — does not wait for the reply.
    pub fn request_spot_order(
        &mut self,
        asset_no: usize,
        request_id: u64,
        action: SpotOrderAction,
    ) -> Result<(), BotError> {
        let instrument = self
            .instruments
            .get(asset_no)
            .ok_or(BotError::InstrumentNotFound)?;
        let symbol = instrument.symbol.clone();
        self.channel.send(
            self.id,
            asset_no,
            LiveRequest::SpotOrder {
                symbol,
                request_id,
                action,
            },
        )
    }

    /// Requests an on-demand combined quotes pull (perp funding + perp/spot ticker + optional
    /// funding history) for `asset_no` (B-M1b). Mirrors `request_spot_order`: resolves the
    /// instrument's `symbol` and sends a [`LiveRequest::Quotes`] over the channel. The connector
    /// replies asynchronously with a `BatchStart`-bracketed group of [`LiveEvent::QuotesReply`]
    /// frames keyed by `request_id`; the result is polled via [`Bot::last_quotes`]. Non-blocking —
    /// does not wait for the reply.
    pub fn request_quotes(
        &mut self,
        asset_no: usize,
        request_id: u64,
        include_funding_history: bool,
    ) -> Result<(), BotError> {
        let instrument = self
            .instruments
            .get(asset_no)
            .ok_or(BotError::InstrumentNotFound)?;
        let symbol = instrument.symbol.clone();
        self.channel.send(
            self.id,
            asset_no,
            LiveRequest::Quotes {
                symbol,
                request_id,
                include_funding_history,
            },
        )
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
    fn last_reconcile(&self, asset_no: usize) -> Option<&ReconcileOutcome> {
        self.instruments
            .get(asset_no)
            .and_then(|i| i.last_reconcile.as_ref())
    }

    #[inline]
    fn last_spot_order(&self, asset_no: usize) -> Option<&SpotOrderOutcome> {
        self.instruments
            .get(asset_no)
            .and_then(|i| i.last_spot_order.as_ref())
    }

    #[inline]
    fn last_quotes(&self, asset_no: usize) -> Option<&QuotesOutcome> {
        self.instruments
            .get(asset_no)
            .and_then(|i| i.last_quotes.as_ref())
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
        types::{
            BuildError,
            LiveEvent,
            LiveRequest,
            OrdType,
            Order,
            QuotesFrame,
            ReconcileEndpoint,
            ReconcileFrame,
            ReconcileScope,
            Side,
            SpotMarketUnit,
            SpotOrdType,
            SpotOrderAction,
            SpotOrderFrame,
            SpotOrderStatus,
            Status,
            TimeInForce,
        },
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

        fn send(
            &mut self,
            id: u64,
            inst_no: usize,
            request: LiveRequest,
        ) -> Result<(), BotError> {
            self.sent.push((id, inst_no, request));
            Ok(())
        }
    }

    fn make_bot(
        symbols: &[&str],
        events: Vec<(usize, LiveEvent)>,
    ) -> LiveBot<MockChannel, HashMapMarketDepth> {
        let instruments = symbols
            .iter()
            .map(|s| {
                Instrument::new(
                    "mock",
                    s,
                    0.01,
                    1.0,
                    HashMapMarketDepth::new(0.01, 1.0),
                    0,
                )
            })
            .collect();
        LiveBot {
            id: 42,
            channel: MockChannel::with(events),
            instruments,
            error_handler: None,
            order_hook: None,
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

    #[test]
    fn timeout_without_complete_keeps_ready_false() {
        let mut bot = make_bot(&["BTCUSDT"], vec![]);
        bot.elapse(1_000).unwrap();
        assert!(!bot.snapshot_ready(0));
    }

    // ----- Reconcile RPC (R-M1a Worker L) frame-accumulation tests (§6 #1-9) -----

    /// Wraps a `ReconcileFrame` into the routable outer `LiveEvent::Reconcile`.
    fn reconcile(symbol: &str, frame: ReconcileFrame) -> LiveEvent {
        LiveEvent::Reconcile {
            symbol: symbol.into(),
            frame,
        }
    }

    fn reconcile_begin(request_id: u64) -> ReconcileFrame {
        ReconcileFrame::Begin {
            request_id,
            snapshot_ts_ns: 1_700_000_000_000_000_000,
        }
    }

    fn reconcile_account() -> ReconcileFrame {
        ReconcileFrame::Account {
            total_equity: 1000.0,
            total_wallet_balance: 900.0,
            total_margin_balance: 950.0,
            total_available_balance: 800.0,
            total_perp_upl: 50.0,
            total_initial_margin: 100.0,
            total_maintenance_margin: 40.0,
            account_im_rate: 0.1,
            account_mm_rate: 0.04,
        }
    }

    fn reconcile_wallet_coin(coin: &str) -> ReconcileFrame {
        ReconcileFrame::WalletCoin {
            coin: coin.into(),
            wallet_balance: 900.0,
            equity: 950.0,
            usd_value: 950.0,
            spot_hedging_qty: 0.0,
            borrow_amount: 0.0,
            collateral_switch: true,
            margin_collateral: true,
        }
    }

    fn reconcile_open_order(order_id: u64, price: f64) -> ReconcileFrame {
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
        ReconcileFrame::OpenOrder { order }
    }

    fn reconcile_end_ok(request_id: u64) -> ReconcileFrame {
        ReconcileFrame::End {
            request_id,
            ok: true,
            endpoint: ReconcileEndpoint::None,
            ret_code: None,
            ret_msg: String::new(),
            http_status: 200,
        }
    }

    /// #1 — fail-closed default (mirror of `snapshot_ready_starts_false`).
    #[test]
    fn last_reconcile_starts_none() {
        let bot = make_bot(&["BTCUSDT"], vec![]);
        assert!(bot.last_reconcile(0).is_none());
    }

    /// #2 — `request_reconcile` records a `LiveRequest::Reconcile{symbol,request_id,scope}`.
    #[test]
    fn request_reconcile_sends_liverequest() {
        let mut bot = make_bot(&["BTCUSDT"], vec![]);
        bot.request_reconcile(0, 42, ReconcileScope::All).unwrap();
        assert_eq!(bot.channel.sent.len(), 1);
        let (id, inst_no, request) = &bot.channel.sent[0];
        assert_eq!(*id, 42);
        assert_eq!(*inst_no, 0);
        match request {
            LiveRequest::Reconcile {
                symbol,
                request_id,
                scope,
            } => {
                assert_eq!(symbol, "BTCUSDT");
                assert_eq!(*request_id, 42);
                assert!(matches!(scope, ReconcileScope::All));
            }
            other => panic!("expected LiveRequest::Reconcile, got {other:?}"),
        }
    }

    /// #3 — full round-trip populates the outcome with all rows AND account totals (D-d).
    #[test]
    fn reconcile_roundtrip_populates_outcome() {
        let mut bot = make_bot(
            &["BTCUSDT"],
            vec![
                (0, LiveEvent::BatchStart),
                (0, reconcile("BTCUSDT", reconcile_begin(7))),
                (0, reconcile("BTCUSDT", reconcile_account())),
                (0, reconcile("BTCUSDT", ReconcileFrame::Position { qty: 1.5 })),
                (0, reconcile("BTCUSDT", reconcile_open_order(11, 100.0))),
                (0, reconcile("BTCUSDT", reconcile_wallet_coin("USDT"))),
                (0, reconcile("BTCUSDT", reconcile_end_ok(7))),
                (0, LiveEvent::BatchEnd),
            ],
        );
        bot.elapse(1_000_000).unwrap();

        let outcome = bot.last_reconcile(0).expect("reconcile outcome present");
        assert_eq!(outcome.request_id, 7);
        assert!(outcome.ok);
        assert_eq!(outcome.positions, vec![1.5]);
        assert_eq!(outcome.open_orders.len(), 1);
        assert_eq!(outcome.open_orders[0].order_id, 11);
        assert_eq!(outcome.wallet_coins.len(), 1);
        assert_eq!(outcome.wallet_coins[0].coin, "USDT");
        let account = outcome.account.as_ref().expect("account totals present");
        assert_eq!(account.total_equity, 1000.0);
        assert_eq!(account.total_margin_balance, 950.0);
        assert_eq!(account.account_mm_rate, 0.04);
    }

    /// #4 — reconcile must NOT overwrite the WS-cached `orders`/`position` view.
    #[test]
    fn reconcile_does_not_overwrite_cached_orders() {
        let mut bot = make_bot(
            &["BTCUSDT"],
            vec![
                // Establish a cached order + position via the normal live paths first.
                (0, order_event("BTCUSDT", 1, 100.0)),
                (
                    0,
                    LiveEvent::Position {
                        symbol: "BTCUSDT".into(),
                        qty: 3.0,
                        exch_ts: 0,
                    },
                ),
                // Then run a reconcile carrying DIFFERENT REST truth.
                (0, LiveEvent::BatchStart),
                (0, reconcile("BTCUSDT", reconcile_begin(1))),
                (
                    0,
                    reconcile("BTCUSDT", ReconcileFrame::Position { qty: 99.0 }),
                ),
                (0, reconcile("BTCUSDT", reconcile_open_order(555, 200.0))),
                (0, reconcile("BTCUSDT", reconcile_end_ok(1))),
                (0, LiveEvent::BatchEnd),
            ],
        );
        bot.elapse(1_000_000).unwrap();

        // Cached view untouched: still the single WS-cached order id 1 and position 3.0.
        assert_eq!(bot.orders(0).len(), 1);
        assert!(bot.orders(0).contains_key(&1));
        assert!(!bot.orders(0).contains_key(&555));
        assert_eq!(bot.position(0), 3.0);
        // Reconcile truth stored separately.
        let outcome = bot.last_reconcile(0).expect("reconcile outcome present");
        assert_eq!(outcome.positions, vec![99.0]);
        assert_eq!(outcome.open_orders[0].order_id, 555);
    }

    /// #5 — a `Begin` with no matching `End` (interrupted) leaves `last_reconcile` None (fail-closed).
    #[test]
    fn reconcile_begin_without_end_stays_none() {
        let mut bot = make_bot(
            &["BTCUSDT"],
            vec![
                (0, LiveEvent::BatchStart),
                (0, reconcile("BTCUSDT", reconcile_begin(5))),
                (0, reconcile("BTCUSDT", ReconcileFrame::Position { qty: 2.0 })),
                // no End, no BatchEnd
            ],
        );
        bot.elapse(1_000_000).unwrap();
        assert!(bot.last_reconcile(0).is_none());
    }

    /// #6 — `End{request_id:99}` for `Begin{request_id:42}` is dropped (fail-closed completeness).
    #[test]
    fn reconcile_mismatched_request_id_ignored() {
        let mut bot = make_bot(
            &["BTCUSDT"],
            vec![
                (0, LiveEvent::BatchStart),
                (0, reconcile("BTCUSDT", reconcile_begin(42))),
                (0, reconcile("BTCUSDT", ReconcileFrame::Position { qty: 1.0 })),
                (0, reconcile("BTCUSDT", reconcile_end_ok(99))),
                (0, LiveEvent::BatchEnd),
            ],
        );
        bot.elapse(1_000_000).unwrap();
        assert!(bot.last_reconcile(0).is_none());
    }

    /// #7 — per-inst_no isolation: reconcile asset 1 leaves asset 0 None.
    #[test]
    fn reconcile_per_asset_isolation() {
        let mut bot = make_bot(
            &["BTCUSDT", "ETHUSDT"],
            vec![
                (1, LiveEvent::BatchStart),
                (1, reconcile("ETHUSDT", reconcile_begin(3))),
                (1, reconcile("ETHUSDT", ReconcileFrame::Position { qty: 7.0 })),
                (1, reconcile("ETHUSDT", reconcile_end_ok(3))),
                (1, LiveEvent::BatchEnd),
            ],
        );
        bot.elapse(1_000_000).unwrap();
        assert!(bot.last_reconcile(0).is_none());
        let outcome = bot.last_reconcile(1).expect("asset 1 reconcile present");
        assert_eq!(outcome.request_id, 3);
        assert_eq!(outcome.positions, vec![7.0]);
    }

    /// #8 — a genuinely empty reconcile (`Begin → End{ok:true}`, 0 rows) still completes as Some.
    #[test]
    fn reconcile_empty_still_completes() {
        let mut bot = make_bot(
            &["BTCUSDT"],
            vec![
                (0, LiveEvent::BatchStart),
                (0, reconcile("BTCUSDT", reconcile_begin(8))),
                (0, reconcile("BTCUSDT", reconcile_end_ok(8))),
                (0, LiveEvent::BatchEnd),
            ],
        );
        bot.elapse(1_000_000).unwrap();
        let outcome = bot.last_reconcile(0).expect("empty reconcile still present");
        assert!(outcome.ok);
        assert!(outcome.positions.is_empty());
        assert!(outcome.open_orders.is_empty());
        assert!(outcome.wallet_coins.is_empty());
        assert!(outcome.account.is_none());
    }

    /// #9 — `End{ok:false}` preserves the raw retCode/retMsg (not collapsed to a bool).
    #[test]
    fn reconcile_end_not_ok_preserves_retcode() {
        let mut bot = make_bot(
            &["BTCUSDT"],
            vec![
                (0, LiveEvent::BatchStart),
                (0, reconcile("BTCUSDT", reconcile_begin(9))),
                (
                    0,
                    reconcile(
                        "BTCUSDT",
                        ReconcileFrame::End {
                            request_id: 9,
                            ok: false,
                            endpoint: ReconcileEndpoint::WalletBalance,
                            ret_code: Some(10006),
                            ret_msg: "Too many visits".into(),
                            http_status: 403,
                        },
                    ),
                ),
                (0, LiveEvent::BatchEnd),
            ],
        );
        bot.elapse(1_000_000).unwrap();
        let outcome = bot.last_reconcile(0).expect("failed reconcile still recorded");
        assert!(!outcome.ok);
        assert_eq!(outcome.ret_code, Some(10006));
        assert_eq!(outcome.ret_msg, "Too many visits");
        assert_eq!(outcome.http_status, 403);
        assert!(matches!(outcome.endpoint, ReconcileEndpoint::WalletBalance));
    }

    // ----- SpotOrder RPC (B-M1a) frame-accumulation tests — clone of the reconcile tests -----

    /// Wraps a `SpotOrderFrame` into the routable outer `LiveEvent::SpotOrderReply`.
    fn spot(symbol: &str, frame: SpotOrderFrame) -> LiveEvent {
        LiveEvent::SpotOrderReply {
            symbol: symbol.into(),
            frame,
        }
    }

    fn spot_begin(request_id: u64) -> SpotOrderFrame {
        SpotOrderFrame::Begin {
            request_id,
            snapshot_ts_ns: 1_700_000_000_000_000_000,
        }
    }

    fn spot_ack(order_link_id: &str, status: SpotOrderStatus) -> SpotOrderFrame {
        SpotOrderFrame::Ack {
            order_link_id: order_link_id.into(),
            cum_exec_qty: 0.0,
            avg_price: 0.0,
            status,
        }
    }

    fn spot_end_ok(request_id: u64) -> SpotOrderFrame {
        SpotOrderFrame::End {
            request_id,
            ok: true,
            ret_code: Some(0),
            ret_msg: String::new(),
            http_status: 200,
        }
    }

    /// `last_spot_order` fail-closed default (mirror of `last_reconcile_starts_none`).
    #[test]
    fn last_spot_order_starts_none() {
        let bot = make_bot(&["ETHUSDT"], vec![]);
        assert!(bot.last_spot_order(0).is_none());
    }

    /// `request_spot_order` records a `LiveRequest::SpotOrder{symbol,request_id,action}`.
    #[test]
    fn request_spot_order_sends_liverequest() {
        let mut bot = make_bot(&["ETHUSDT"], vec![]);
        bot.request_spot_order(
            0,
            42,
            SpotOrderAction::Place {
                side: Side::Buy,
                order_type: SpotOrdType::Limit,
                qty: 0.5,
                market_unit: SpotMarketUnit::BaseCoin,
                price: Some(3000.0),
                order_link_id: "basis-42".into(),
            },
        )
        .unwrap();
        assert_eq!(bot.channel.sent.len(), 1);
        let (id, inst_no, request) = &bot.channel.sent[0];
        assert_eq!(*id, 42);
        assert_eq!(*inst_no, 0);
        match request {
            LiveRequest::SpotOrder {
                symbol,
                request_id,
                action,
            } => {
                assert_eq!(symbol, "ETHUSDT");
                assert_eq!(*request_id, 42);
                match action {
                    SpotOrderAction::Place {
                        side,
                        order_type,
                        qty,
                        market_unit,
                        price,
                        order_link_id,
                    } => {
                        assert_eq!(*side, Side::Buy);
                        assert_eq!(*order_type, SpotOrdType::Limit);
                        assert_eq!(*qty, 0.5);
                        assert_eq!(*market_unit, SpotMarketUnit::BaseCoin);
                        assert_eq!(*price, Some(3000.0));
                        assert_eq!(order_link_id, "basis-42");
                    }
                    other => panic!("expected SpotOrderAction::Place, got {other:?}"),
                }
            }
            other => panic!("expected LiveRequest::SpotOrder, got {other:?}"),
        }
    }

    /// Place/Status round-trip: `Begin → Ack → End{ok:true}` populates the outcome with the Ack.
    #[test]
    fn spot_order_place_roundtrip_populates_outcome() {
        let mut bot = make_bot(
            &["ETHUSDT"],
            vec![
                (0, LiveEvent::BatchStart),
                (0, spot("ETHUSDT", spot_begin(7))),
                (
                    0,
                    spot(
                        "ETHUSDT",
                        SpotOrderFrame::Ack {
                            order_link_id: "basis-7".into(),
                            cum_exec_qty: 0.25,
                            avg_price: 3001.5,
                            status: SpotOrderStatus::PartiallyFilled,
                        },
                    ),
                ),
                (0, spot("ETHUSDT", spot_end_ok(7))),
                (0, LiveEvent::BatchEnd),
            ],
        );
        bot.elapse(1_000_000).unwrap();

        let outcome = bot.last_spot_order(0).expect("spot-order outcome present");
        assert_eq!(outcome.request_id, 7);
        assert!(outcome.ok);
        let ack = outcome.ack.as_ref().expect("ack present");
        assert_eq!(ack.order_link_id, "basis-7");
        assert_eq!(ack.cum_exec_qty, 0.25);
        assert_eq!(ack.avg_price, 3001.5);
        assert_eq!(ack.status, SpotOrderStatus::PartiallyFilled);
    }

    /// Cancel round-trip: `Begin → End` (no `Ack`) → outcome with `ack: None`, result in `ok`.
    #[test]
    fn spot_order_cancel_roundtrip_has_no_ack() {
        let mut bot = make_bot(
            &["ETHUSDT"],
            vec![
                (0, LiveEvent::BatchStart),
                (0, spot("ETHUSDT", spot_begin(8))),
                (0, spot("ETHUSDT", spot_end_ok(8))),
                (0, LiveEvent::BatchEnd),
            ],
        );
        bot.elapse(1_000_000).unwrap();
        let outcome = bot.last_spot_order(0).expect("cancel outcome present");
        assert!(outcome.ok);
        assert!(outcome.ack.is_none(), "Cancel carries no Ack frame");
    }

    /// A `Begin` with no matching `End` (interrupted) leaves `last_spot_order` None (fail-closed).
    #[test]
    fn spot_order_begin_without_end_stays_none() {
        let mut bot = make_bot(
            &["ETHUSDT"],
            vec![
                (0, LiveEvent::BatchStart),
                (0, spot("ETHUSDT", spot_begin(5))),
                (0, spot("ETHUSDT", spot_ack("basis-5", SpotOrderStatus::New))),
                // no End, no BatchEnd
            ],
        );
        bot.elapse(1_000_000).unwrap();
        assert!(bot.last_spot_order(0).is_none());
    }

    /// `End{request_id:99}` for `Begin{request_id:42}` is dropped (fail-closed completeness).
    #[test]
    fn spot_order_mismatched_request_id_ignored() {
        let mut bot = make_bot(
            &["ETHUSDT"],
            vec![
                (0, LiveEvent::BatchStart),
                (0, spot("ETHUSDT", spot_begin(42))),
                (0, spot("ETHUSDT", spot_ack("basis-42", SpotOrderStatus::New))),
                (0, spot("ETHUSDT", spot_end_ok(99))),
                (0, LiveEvent::BatchEnd),
            ],
        );
        bot.elapse(1_000_000).unwrap();
        assert!(bot.last_spot_order(0).is_none());
    }

    /// Per-inst_no isolation: a spot-order on asset 1 leaves asset 0 None.
    #[test]
    fn spot_order_per_asset_isolation() {
        let mut bot = make_bot(
            &["ETHUSDT", "BTCUSDT"],
            vec![
                (1, LiveEvent::BatchStart),
                (1, spot("BTCUSDT", spot_begin(3))),
                (1, spot("BTCUSDT", spot_ack("basis-3", SpotOrderStatus::Filled))),
                (1, spot("BTCUSDT", spot_end_ok(3))),
                (1, LiveEvent::BatchEnd),
            ],
        );
        bot.elapse(1_000_000).unwrap();
        assert!(bot.last_spot_order(0).is_none());
        let outcome = bot.last_spot_order(1).expect("asset 1 spot-order present");
        assert_eq!(outcome.request_id, 3);
        assert_eq!(
            outcome.ack.as_ref().unwrap().status,
            SpotOrderStatus::Filled
        );
    }

    /// `End{ok:false}` preserves the raw retCode/retMsg (not collapsed to a bool) — e.g. a
    /// min-notional / insufficient-balance reject (B-M1a fail-closed retCode).
    #[test]
    fn spot_order_end_not_ok_preserves_retcode() {
        let mut bot = make_bot(
            &["ETHUSDT"],
            vec![
                (0, LiveEvent::BatchStart),
                (0, spot("ETHUSDT", spot_begin(9))),
                (
                    0,
                    spot(
                        "ETHUSDT",
                        SpotOrderFrame::End {
                            request_id: 9,
                            ok: false,
                            ret_code: Some(170131),
                            ret_msg: "Insufficient balance".into(),
                            http_status: 200,
                        },
                    ),
                ),
                (0, LiveEvent::BatchEnd),
            ],
        );
        bot.elapse(1_000_000).unwrap();
        let outcome = bot.last_spot_order(0).expect("failed spot-order still recorded");
        assert!(!outcome.ok);
        assert_eq!(outcome.ret_code, Some(170131));
        assert_eq!(outcome.ret_msg, "Insufficient balance");
        assert_eq!(outcome.http_status, 200);
        assert!(outcome.ack.is_none());
    }

    // ----- Quotes RPC (B-M1b) frame-accumulation tests — clone of the spot-order tests -----

    /// Wraps a `QuotesFrame` into the routable outer `LiveEvent::QuotesReply`.
    fn quotes(symbol: &str, frame: QuotesFrame) -> LiveEvent {
        LiveEvent::QuotesReply {
            symbol: symbol.into(),
            frame,
        }
    }

    fn quotes_begin(request_id: u64) -> QuotesFrame {
        QuotesFrame::Begin {
            request_id,
            snapshot_ts_ns: 1_700_000_000_000_000_000,
        }
    }

    fn perp_quote() -> QuotesFrame {
        QuotesFrame::PerpQuote {
            server_ts_ms: 1_718_000_000_000,
            bid: 3000.1,
            ask: 3000.2,
            last: 3000.15,
            funding_rate: 0.0001,
            next_funding_time_ms: 1_718_028_800_000,
            funding_interval_min: 480,
        }
    }

    fn spot_quote() -> QuotesFrame {
        QuotesFrame::SpotQuote {
            server_ts_ms: 1_718_000_000_001,
            bid: 2999.9,
            ask: 3000.0,
            last: 2999.95,
        }
    }

    fn quotes_end_ok(request_id: u64) -> QuotesFrame {
        QuotesFrame::End {
            request_id,
            ok: true,
            ret_code: Some(0),
            ret_msg: String::new(),
            http_status: 200,
        }
    }

    /// `last_quotes` fail-closed default (mirror of `last_spot_order_starts_none`).
    #[test]
    fn last_quotes_starts_none() {
        let bot = make_bot(&["ETHUSDT"], vec![]);
        assert!(bot.last_quotes(0).is_none());
    }

    /// `request_quotes` records a `LiveRequest::Quotes{symbol,request_id,include_funding_history}`.
    #[test]
    fn request_quotes_sends_liverequest() {
        let mut bot = make_bot(&["ETHUSDT"], vec![]);
        bot.request_quotes(0, 42, true).unwrap();
        assert_eq!(bot.channel.sent.len(), 1);
        let (id, inst_no, request) = &bot.channel.sent[0];
        assert_eq!(*id, 42);
        assert_eq!(*inst_no, 0);
        match request {
            LiveRequest::Quotes {
                symbol,
                request_id,
                include_funding_history,
            } => {
                assert_eq!(symbol, "ETHUSDT");
                assert_eq!(*request_id, 42);
                assert!(*include_funding_history);
            }
            other => panic!("expected LiveRequest::Quotes, got {other:?}"),
        }
    }

    /// Full round-trip: `Begin → PerpQuote → SpotQuote → FundingRow* → End{ok:true}` populates the
    /// outcome with both quotes and the funding rows (newest-first).
    #[test]
    fn quotes_roundtrip_populates_outcome() {
        let mut bot = make_bot(
            &["ETHUSDT"],
            vec![
                (0, LiveEvent::BatchStart),
                (0, quotes("ETHUSDT", quotes_begin(7))),
                (0, quotes("ETHUSDT", perp_quote())),
                (0, quotes("ETHUSDT", spot_quote())),
                (
                    0,
                    quotes(
                        "ETHUSDT",
                        QuotesFrame::FundingRow {
                            funding_time_ms: 1_718_028_800_000,
                            funding_rate: 0.0002,
                        },
                    ),
                ),
                (
                    0,
                    quotes(
                        "ETHUSDT",
                        QuotesFrame::FundingRow {
                            funding_time_ms: 1_718_000_000_000,
                            funding_rate: 0.0001,
                        },
                    ),
                ),
                (0, quotes("ETHUSDT", quotes_end_ok(7))),
                (0, LiveEvent::BatchEnd),
            ],
        );
        bot.elapse(1_000_000).unwrap();

        let outcome = bot.last_quotes(0).expect("quotes outcome present");
        assert_eq!(outcome.request_id, 7);
        assert!(outcome.ok);
        let perp = outcome.perp.as_ref().expect("perp quote present");
        assert_eq!(perp.bid, 3000.1);
        assert_eq!(perp.ask, 3000.2);
        assert_eq!(perp.funding_rate, 0.0001);
        assert_eq!(perp.funding_interval_min, 480);
        assert_eq!(perp.next_funding_time_ms, 1_718_028_800_000);
        let spot = outcome.spot.as_ref().expect("spot quote present");
        assert_eq!(spot.bid, 2999.9);
        assert_eq!(spot.ask, 3000.0);
        assert_eq!(outcome.funding_history.len(), 2);
        // Newest-first ordering preserved (first row carries the most recent funding_time_ms).
        assert_eq!(outcome.funding_history[0].funding_time_ms, 1_718_028_800_000);
        assert_eq!(outcome.funding_history[1].funding_time_ms, 1_718_000_000_000);
    }

    /// A quotes pull WITHOUT funding history: `Begin → PerpQuote → SpotQuote → End` → empty history.
    #[test]
    fn quotes_no_funding_history_still_completes() {
        let mut bot = make_bot(
            &["ETHUSDT"],
            vec![
                (0, LiveEvent::BatchStart),
                (0, quotes("ETHUSDT", quotes_begin(8))),
                (0, quotes("ETHUSDT", perp_quote())),
                (0, quotes("ETHUSDT", spot_quote())),
                (0, quotes("ETHUSDT", quotes_end_ok(8))),
                (0, LiveEvent::BatchEnd),
            ],
        );
        bot.elapse(1_000_000).unwrap();
        let outcome = bot.last_quotes(0).expect("quotes outcome present");
        assert!(outcome.ok);
        assert!(outcome.perp.is_some());
        assert!(outcome.spot.is_some());
        assert!(outcome.funding_history.is_empty());
    }

    /// The consumer can age a quote off the carried `server_ts_ms` (B-M1b §0 D-c).
    #[test]
    fn quotes_timestamp_carried() {
        let mut bot = make_bot(
            &["ETHUSDT"],
            vec![
                (0, LiveEvent::BatchStart),
                (0, quotes("ETHUSDT", quotes_begin(3))),
                (0, quotes("ETHUSDT", perp_quote())),
                (0, quotes("ETHUSDT", spot_quote())),
                (0, quotes("ETHUSDT", quotes_end_ok(3))),
                (0, LiveEvent::BatchEnd),
            ],
        );
        bot.elapse(1_000_000).unwrap();
        let outcome = bot.last_quotes(0).expect("quotes outcome present");
        assert_eq!(outcome.perp.unwrap().server_ts_ms, 1_718_000_000_000);
        assert_eq!(outcome.spot.unwrap().server_ts_ms, 1_718_000_000_001);
    }

    /// A `Begin` with no matching `End` (interrupted) leaves `last_quotes` None (fail-closed).
    #[test]
    fn quotes_begin_without_end_stays_none() {
        let mut bot = make_bot(
            &["ETHUSDT"],
            vec![
                (0, LiveEvent::BatchStart),
                (0, quotes("ETHUSDT", quotes_begin(5))),
                (0, quotes("ETHUSDT", perp_quote())),
                // no End, no BatchEnd
            ],
        );
        bot.elapse(1_000_000).unwrap();
        assert!(bot.last_quotes(0).is_none());
    }

    /// `End{request_id:99}` for `Begin{request_id:42}` is dropped (fail-closed completeness).
    #[test]
    fn quotes_mismatched_request_id_ignored() {
        let mut bot = make_bot(
            &["ETHUSDT"],
            vec![
                (0, LiveEvent::BatchStart),
                (0, quotes("ETHUSDT", quotes_begin(42))),
                (0, quotes("ETHUSDT", perp_quote())),
                (0, quotes("ETHUSDT", quotes_end_ok(99))),
                (0, LiveEvent::BatchEnd),
            ],
        );
        bot.elapse(1_000_000).unwrap();
        assert!(bot.last_quotes(0).is_none());
    }

    /// Per-inst_no isolation: a quotes pull on asset 1 leaves asset 0 None.
    #[test]
    fn quotes_per_asset_isolation() {
        let mut bot = make_bot(
            &["ETHUSDT", "BTCUSDT"],
            vec![
                (1, LiveEvent::BatchStart),
                (1, quotes("BTCUSDT", quotes_begin(3))),
                (1, quotes("BTCUSDT", perp_quote())),
                (1, quotes("BTCUSDT", quotes_end_ok(3))),
                (1, LiveEvent::BatchEnd),
            ],
        );
        bot.elapse(1_000_000).unwrap();
        assert!(bot.last_quotes(0).is_none());
        let outcome = bot.last_quotes(1).expect("asset 1 quotes present");
        assert_eq!(outcome.request_id, 3);
    }

    /// `End{ok:false}` preserves the raw retCode/retMsg (not collapsed to a bool) — e.g. a
    /// rate-limit / bad-symbol reject (B-M1b fail-closed retCode).
    #[test]
    fn quotes_end_not_ok_preserves_retcode() {
        let mut bot = make_bot(
            &["ETHUSDT"],
            vec![
                (0, LiveEvent::BatchStart),
                (0, quotes("ETHUSDT", quotes_begin(9))),
                (
                    0,
                    quotes(
                        "ETHUSDT",
                        QuotesFrame::End {
                            request_id: 9,
                            ok: false,
                            ret_code: Some(10001),
                            ret_msg: "params error".into(),
                            http_status: 200,
                        },
                    ),
                ),
                (0, LiveEvent::BatchEnd),
            ],
        );
        bot.elapse(1_000_000).unwrap();
        let outcome = bot.last_quotes(0).expect("failed quotes still recorded");
        assert!(!outcome.ok);
        assert_eq!(outcome.ret_code, Some(10001));
        assert_eq!(outcome.ret_msg, "params error");
        assert_eq!(outcome.http_status, 200);
        assert!(outcome.perp.is_none());
        assert!(outcome.spot.is_none());
    }
}
