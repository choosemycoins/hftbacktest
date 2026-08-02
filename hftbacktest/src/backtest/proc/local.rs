use std::collections::{HashMap, hash_map::Entry};

#[cfg(debug_assertions)]
use crate::backtest::state::ExecutionLedger;
use crate::{
    backtest::{
        BacktestError,
        assettype::AssetType,
        models::{FeeModel, LatencyModel},
        order::LocalToExch,
        proc::{LocalProcessor, Processor},
        state::State,
    },
    depth::{L2MarketDepth, MarketDepth},
    types::{
        Event,
        LOCAL_ASK_DEPTH_CLEAR_EVENT,
        LOCAL_ASK_DEPTH_EVENT,
        LOCAL_ASK_DEPTH_SNAPSHOT_EVENT,
        LOCAL_BID_DEPTH_CLEAR_EVENT,
        LOCAL_BID_DEPTH_EVENT,
        LOCAL_BID_DEPTH_SNAPSHOT_EVENT,
        LOCAL_DEPTH_CLEAR_EVENT,
        LOCAL_EVENT,
        LOCAL_TRADE_EVENT,
        OrdType,
        Order,
        OrderId,
        Price,
        PriceTick,
        Qty,
        Side,
        StateValues,
        Status,
        TickSize,
        TimeInForce,
    },
};

/// The local model.
pub struct Local<AT, LM, MD, FM>
where
    AT: AssetType,
    LM: LatencyModel,
    MD: MarketDepth,
    FM: FeeModel,
{
    orders: HashMap<OrderId, Order>,
    order_l2e: LocalToExch<LM>,
    depth: MD,
    state: State<AT, FM>,
    trades: Vec<Event>,
    last_feed_latency: Option<(i64, i64)>,
    last_order_latency: Option<(i64, i64, i64)>,
    /// Debug-only, and carried by no release build: see [`ExecutionLedger`]. It lives here, not in
    /// [`State`], because checking an execution against the previous one takes an owner that also
    /// sees the order submitted and amended.
    #[cfg(debug_assertions)]
    executions: ExecutionLedger,
}

impl<AT, LM, MD, FM> Local<AT, LM, MD, FM>
where
    AT: AssetType,
    LM: LatencyModel,
    MD: MarketDepth,
    FM: FeeModel,
{
    /// Constructs an instance of `Local`.
    pub fn new(
        depth: MD,
        state: State<AT, FM>,
        last_trades_cap: usize,
        order_l2e: LocalToExch<LM>,
    ) -> Self {
        Self {
            orders: Default::default(),
            order_l2e,
            depth,
            state,
            trades: Vec::with_capacity(last_trades_cap),
            last_feed_latency: None,
            last_order_latency: None,
            #[cfg(debug_assertions)]
            executions: ExecutionLedger::default(),
        }
    }

    pub fn process_recv_order_<const USE_HANDLER: bool, Handler>(
        &mut self,
        timestamp: i64,
        wait_resp_order_id: Option<OrderId>,
        mut handler: Handler,
    ) -> Result<bool, BacktestError>
    where
        Handler: FnMut(&Order),
    {
        let mut wait_resp_order_received = false;
        while let Some(order) = self.order_l2e.receive(timestamp) {
            // Updates the order latency only if it has a valid exchange timestamp. When the
            // order is rejected before it reaches the matching engine, it has no exchange
            // timestamp. This situation occurs in crypto exchanges.
            if order.exch_timestamp > 0 {
                self.last_order_latency =
                    Some((order.local_timestamp, order.exch_timestamp, timestamp));
            }

            if let Some(wait_resp_order_id) = wait_resp_order_id
                && order.order_id == wait_resp_order_id
            {
                wait_resp_order_received = true;
            }

            // Processes receiving order response.
            //
            // A response reports an execution when its status is [`Status::Filled`] or
            // [`Status::PartiallyFilled`] and it carries an executed quantity. `exec_qty` is the
            // quantity executed by that single execution, not the cumulative one, so applying
            // every execution response accumulates the order's fills without double-counting.
            //
            // Requests carry no execution — `LocalToExch::request` clears their execution fields
            // — so a request the exchange hands back, on a rejection or on an acknowledgement,
            // cannot be mistaken for one.
            if (order.status == Status::Filled || order.status == Status::PartiallyFilled)
                && order.exec_qty.is_some()
            {
                #[cfg(debug_assertions)]
                debug_assert!(
                    self.executions.record(&order),
                    "`exec_qty` must be the quantity executed by this execution alone, not the \
                     order's cumulative executed quantity: {order:?}"
                );
                // The side was resolved when the order was submitted, so a response that
                // reports an execution carries one — but the local is fed by a channel, not by
                // an invariant it owns, so it says so with an error rather than an assumption
                // (E2). Nothing is applied to the state without a direction.
                let side = order
                    .side
                    .try_resolve()
                    .ok_or(BacktestError::InvalidOrderRequest)?;
                self.state.apply_fill(&order, side);
            }
            // The ledger tracks live orders only, so it does not grow with the backtest.
            #[cfg(debug_assertions)]
            if order.status.is_terminal() {
                self.executions.forget(order.order_id);
            }
            // Applies the received order response to the local orders.
            match self.orders.entry(order.order_id) {
                Entry::Occupied(mut entry) => {
                    let local_order = entry.get_mut();
                    if order.req == Status::Rejected {
                        if order.local_timestamp == local_order.local_timestamp {
                            if local_order.req == Status::New {
                                local_order.req = Status::None;
                                local_order.status = Status::Expired;
                            } else {
                                local_order.req = Status::None;
                            }
                        }
                    } else {
                        local_order.update(&order);
                    }
                    if USE_HANDLER {
                        handler(&order);
                    }
                }
                Entry::Vacant(entry) => {
                    if order.req != Status::Rejected {
                        let order_ = entry.insert(order);
                        if USE_HANDLER {
                            handler(order_);
                        }
                    }
                }
            }
        }
        Ok(wait_resp_order_received)
    }
}

impl<AT, LM, MD, FM> LocalProcessor<MD> for Local<AT, LM, MD, FM>
where
    AT: AssetType,
    LM: LatencyModel,
    MD: MarketDepth + L2MarketDepth,
    FM: FeeModel,
{
    fn submit_order(
        &mut self,
        order_id: OrderId,
        side: Side,
        price: f64,
        qty: f64,
        order_type: OrdType,
        time_in_force: TimeInForce,
        current_timestamp: i64,
    ) -> Result<(), BacktestError> {
        if self.orders.contains_key(&order_id) {
            return Err(BacktestError::OrderIdExist);
        }
        // The submit boundary: an order whose side is not a direction is refused here, before
        // anything rests, so it cannot reach the position math that needs one. Past this point a
        // resting order's side resolves, by construction (invariant E2).
        if side.try_resolve().is_none() {
            return Err(BacktestError::InvalidOrderRequest);
        }

        // The one bridge (C5): a price becomes ticks only by being divided by a tick size.
        let tick_size = TickSize::new(self.depth.tick_size());
        let price_tick = Price::new(price).to_ticks(tick_size);
        let mut order = Order::new(
            order_id,
            price_tick,
            tick_size,
            Qty::new(qty),
            side,
            order_type,
            time_in_force,
        );
        order.req = Status::New;
        order.local_timestamp = current_timestamp;
        self.orders.insert(order.order_id, order.clone());
        // This id now names a new order, whatever the previous one it named had left.
        #[cfg(debug_assertions)]
        self.executions.forget(order_id);

        self.order_l2e.request(order, |order| {
            order.req = Status::Rejected;
        });

        Ok(())
    }

    fn modify(
        &mut self,
        order_id: OrderId,
        price: f64,
        qty: f64,
        current_timestamp: i64,
    ) -> Result<(), BacktestError> {
        let order = self
            .orders
            .get_mut(&order_id)
            .ok_or(BacktestError::OrderNotFound)?;

        if order.req != Status::None {
            return Err(BacktestError::OrderRequestInProcess);
        }

        let orig_price_tick = order.price_tick.get();
        let orig_qty = order.qty.get();
        let orig_leaves_qty = order.leaves_qty.get();

        let price_tick = (price / self.depth.tick_size()).round() as i64;
        order.price_tick = PriceTick::new(price_tick);
        order.qty = Qty::new(qty);
        // The amended quantity is the quantity the order rests for. The exchange re-rests the
        // order when the amendment changes its price, and it rests whatever `leaves_qty` the
        // request carries.
        order.leaves_qty = Qty::new(qty);

        order.req = Status::Replaced;
        order.local_timestamp = current_timestamp;

        self.order_l2e.request(order.clone(), |order| {
            order.req = Status::Rejected;
            order.price_tick = PriceTick::new(orig_price_tick);
            order.qty = Qty::new(orig_qty);
            order.leaves_qty = Qty::new(orig_leaves_qty);
        });
        // The amendment re-rests the order, so what it had left before says nothing about what it
        // has left now.
        #[cfg(debug_assertions)]
        self.executions.forget(order_id);

        Ok(())
    }

    fn cancel(&mut self, order_id: OrderId, current_timestamp: i64) -> Result<(), BacktestError> {
        let order = self
            .orders
            .get_mut(&order_id)
            .ok_or(BacktestError::OrderNotFound)?;

        if order.req != Status::None {
            return Err(BacktestError::OrderRequestInProcess);
        }

        order.req = Status::Canceled;
        order.local_timestamp = current_timestamp;

        self.order_l2e.request(order.clone(), |order| {
            order.req = Status::Rejected;
        });

        Ok(())
    }

    fn clear_inactive_orders(&mut self) {
        // One definition of "terminal" ([`Status::is_terminal`]); this and the live guard no
        // longer hand-maintain the set independently, and it now also clears `Rejected`.
        self.orders.retain(|_, order| !order.status.is_terminal())
    }

    fn position(&self) -> f64 {
        self.state.values().position
    }

    fn state_values(&self) -> &StateValues {
        self.state.values()
    }

    fn depth(&self) -> &MD {
        &self.depth
    }

    fn orders(&self) -> &HashMap<u64, Order> {
        &self.orders
    }

    fn last_trades(&self) -> &[Event] {
        self.trades.as_slice()
    }

    fn clear_last_trades(&mut self) {
        self.trades.clear();
    }

    fn feed_latency(&self) -> Option<(i64, i64)> {
        self.last_feed_latency
    }

    fn order_latency(&self) -> Option<(i64, i64, i64)> {
        self.last_order_latency
    }
}

impl<AT, LM, MD, FM> Processor for Local<AT, LM, MD, FM>
where
    AT: AssetType,
    LM: LatencyModel,
    MD: MarketDepth + L2MarketDepth,
    FM: FeeModel,
{
    fn event_seen_timestamp(&self, event: &Event) -> Option<i64> {
        event.is(LOCAL_EVENT).then_some(event.local_ts)
    }

    fn process(&mut self, ev: &Event) -> Result<(), BacktestError> {
        // Processes a depth event
        if ev.is(LOCAL_BID_DEPTH_CLEAR_EVENT) {
            self.depth.clear_depth(Side::Buy, ev.px);
        } else if ev.is(LOCAL_ASK_DEPTH_CLEAR_EVENT) {
            self.depth.clear_depth(Side::Sell, ev.px);
        } else if ev.is(LOCAL_DEPTH_CLEAR_EVENT) {
            self.depth.clear_depth(Side::None, 0.0);
        } else if ev.is(LOCAL_BID_DEPTH_EVENT) || ev.is(LOCAL_BID_DEPTH_SNAPSHOT_EVENT) {
            self.depth.update_bid_depth(ev.px, ev.qty, ev.local_ts);
        } else if ev.is(LOCAL_ASK_DEPTH_EVENT) || ev.is(LOCAL_ASK_DEPTH_SNAPSHOT_EVENT) {
            self.depth.update_ask_depth(ev.px, ev.qty, ev.local_ts);
        }
        // Processes a trade event
        else if ev.is(LOCAL_TRADE_EVENT) && self.trades.capacity() > 0 {
            self.trades.push(ev.clone());
        }

        // Stores the current feed latency
        self.last_feed_latency = Some((ev.exch_ts, ev.local_ts));

        Ok(())
    }

    fn process_recv_order(
        &mut self,
        timestamp: i64,
        wait_resp_order_id: Option<OrderId>,
    ) -> Result<bool, BacktestError> {
        self.process_recv_order_::<false, _>(timestamp, wait_resp_order_id, |_| {})
    }

    fn earliest_recv_order_timestamp(&self) -> i64 {
        self.order_l2e
            .earliest_recv_order_timestamp()
            .unwrap_or(i64::MAX)
    }

    fn earliest_send_order_timestamp(&self) -> i64 {
        self.order_l2e
            .earliest_send_order_timestamp()
            .unwrap_or(i64::MAX)
    }
}
