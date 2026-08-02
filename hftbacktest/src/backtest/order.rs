use std::{cell::UnsafeCell, collections::VecDeque, rc::Rc};

use crate::{
    backtest::models::LatencyModel,
    types::{ExecDelta, Order, PriceTick},
};

/// An order request travelling **local → exchange**: a submit, a cancel or an amend.
///
/// Invariant E4, in its backtest-internal form. The confusion it forecloses is specific and
/// was measured (`AGENTS.md` §4.6): the local builds a request from its own copy of the
/// order, which carries the last execution it has already applied, and the exchange hands
/// that request straight back as the response whenever it rejects it or acknowledges an
/// in-place amendment. The local then read the stale execution fields as a fresh fill and
/// counted the same quantity twice.
///
/// Zeroing those fields in one place fixed it; giving requests and responses **different
/// types** is what stops a future processor bypassing that place. [`ExchRequest::new`] is the
/// only constructor and is where the execution fields are cleared, so "a request carries no
/// execution" is now true by construction rather than by a function everyone must remember to
/// route through.
///
/// **Deliberately not named `OrderRequest`:** [`crate::types::OrderRequest`] already exists
/// and means something else — the live submit DTO — and it is in the prelude.
///
/// Not a wire type. On the live path a request is a `LiveRequest::Order` and a response is a
/// `LiveEvent::Order`: two different enums, in opposite directions, over two different iceoryx
/// services, so there is no channel on which a live request could arrive as a response. The
/// full wire split was considered and declined for that reason — it would have been this
/// fork's first non-append wire change, and an old connector decoding a shortened struct does
/// not error, it reads the next field's bytes and acts on garbage.
#[derive(Clone, Debug)]
pub struct ExchRequest(Order);

impl ExchRequest {
    /// The only way to make a request, and therefore the only place the execution fields are
    /// cleared.
    pub fn new(mut order: Order) -> Self {
        order.exec_qty = ExecDelta::ZERO;
        order.exec_price_tick = PriceTick::new(0);
        Self(order)
    }

    /// The order this request is about.
    pub fn into_order(self) -> Order {
        self.0
    }

    fn order(&self) -> &Order {
        &self.0
    }

    fn order_mut(&mut self) -> &mut Order {
        &mut self.0
    }
}

/// An order response travelling **exchange → local**: an acknowledgement, a rejection, or an
/// execution.
///
/// The counterpart to [`ExchRequest`]. `Local::process_recv_order_` takes one of these, so
/// **only a response can report a fill** — structurally, rather than because one function
/// happened to zero two fields on the way past.
#[derive(Clone, Debug)]
pub struct ExchResponse(Order);

impl ExchResponse {
    /// The exchange's answer about an order it processed. This is the one that may carry an
    /// execution.
    pub fn new(order: Order) -> Self {
        Self(order)
    }

    /// A request the exchange never accepted, handed back as the response.
    ///
    /// Previously this was spelled "append the request onto the response bus", which is the
    /// same act but unnamed and therefore un-greppable. The request's execution fields were
    /// already cleared by [`ExchRequest::new`], so a rejection cannot re-report a fill.
    pub fn rejected(request: ExchRequest) -> Self {
        Self(request.0)
    }

    /// The order this response is about.
    pub fn into_order(self) -> Order {
        self.0
    }
}

/// Provides a bus for transporting backtesting orders between the exchange and the local model
/// based on the given timestamp.
///
/// Generic in what it carries, so that the request bus and the response bus cannot be crossed:
/// the two already ran in opposite directions, this gives them their types.
#[derive(Debug)]
pub struct OrderBus<T> {
    order_list: Rc<UnsafeCell<VecDeque<(T, i64)>>>,
}

impl<T> Clone for OrderBus<T> {
    fn clone(&self) -> Self {
        Self {
            order_list: self.order_list.clone(),
        }
    }
}

impl<T> Default for OrderBus<T> {
    fn default() -> Self {
        Self {
            order_list: Rc::new(UnsafeCell::new(VecDeque::new())),
        }
    }
}

impl<T> OrderBus<T> {
    /// Constructs an instance of ``OrderBus``.
    pub fn new() -> Self {
        Default::default()
    }

    /// Returns the timestamp of the earliest order in the bus.
    pub fn earliest_timestamp(&self) -> Option<i64> {
        unsafe { &*self.order_list.get() }
            .front()
            .map(|(_order, ts)| *ts)
    }

    /// Appends the order to the bus with the timestamp.
    ///
    /// To prevent the timestamp of the order from becoming disordered, it enforces that the given
    /// timestamp must be equal to or greater than the latest timestamp in the bus.
    ///
    /// In crypto exchanges that use REST APIs, it may be still possible for order requests sent
    /// later to reach the matching engine before order requests sent earlier. However, for the
    /// purpose of simplifying the backtesting process, all requests and responses are assumed to be
    /// in order.
    pub fn append(&mut self, order: T, timestamp: i64) {
        let latest_timestamp = {
            let order_list = unsafe { &*self.order_list.get() };
            let len = order_list.len();
            if len > 0 {
                let (_, timestamp) = order_list.get(len - 1).unwrap();
                *timestamp
            } else {
                0
            }
        };
        let timestamp = timestamp.max(latest_timestamp);
        unsafe { &mut *self.order_list.get() }.push_back((order, timestamp));
    }

    /// Resets this to clear it.
    pub fn reset(&mut self) {
        unsafe { &mut *self.order_list.get() }.clear();
    }

    /// Returns the number of orders in the bus.
    pub fn len(&self) -> usize {
        unsafe { &*self.order_list.get() }.len()
    }

    /// Returns ``true`` if the ``OrderBus`` is empty.
    pub fn is_empty(&self) -> bool {
        unsafe { &*self.order_list.get() }.is_empty()
    }

    /// Removes the first order and its timestamp and returns it, or ``None`` if the bus is empty.
    pub fn pop_front(&mut self) -> Option<(T, i64)> {
        unsafe { &mut *self.order_list.get() }.pop_front()
    }
}

/// Provides a bidirectional order bus connecting the exchange to the local.
pub struct ExchToLocal<LM> {
    to_exch: OrderBus<ExchRequest>,
    to_local: OrderBus<ExchResponse>,
    order_latency: LM,
}

impl<LM> ExchToLocal<LM>
where
    LM: LatencyModel,
{
    /// Returns the timestamp of the earliest order to be received by the exchange from the local.
    pub fn earliest_recv_order_timestamp(&self) -> Option<i64> {
        self.to_exch.earliest_timestamp()
    }

    /// Returns the timestamp of the earliest order sent from the exchange to the local.
    pub fn earliest_send_order_timestamp(&self) -> Option<i64> {
        self.to_local.earliest_timestamp()
    }

    /// Responds to the local with the order processed by the exchange.
    pub fn respond(&mut self, order: Order) {
        let local_recv_timestamp =
            order.exch_timestamp + self.order_latency.response(order.exch_timestamp, &order);
        self.to_local
            .append(ExchResponse::new(order), local_recv_timestamp);
    }

    /// Receives the order request from the local, which is expected to be received at
    /// `receipt_timestamp`.
    pub fn receive(&mut self, receipt_timestamp: i64) -> Option<Order> {
        if let Some(timestamp) = self.to_exch.earliest_timestamp() {
            if timestamp == receipt_timestamp {
                self.to_exch
                    .pop_front()
                    .map(|(request, _)| request.into_order())
            } else {
                assert!(timestamp > receipt_timestamp);
                None
            }
        } else {
            None
        }
    }
}

/// Provides a bidirectional order bus connecting the local to the exchange.
pub struct LocalToExch<LM> {
    to_exch: OrderBus<ExchRequest>,
    to_local: OrderBus<ExchResponse>,
    order_latency: LM,
}

impl<LM> LocalToExch<LM>
where
    LM: LatencyModel,
{
    /// Returns the timestamp of the earliest order to be received by the local from the exchange.
    pub fn earliest_recv_order_timestamp(&self) -> Option<i64> {
        self.to_local.earliest_timestamp()
    }

    /// Returns the timestamp of the earliest order sent from the local to the exchange.
    pub fn earliest_send_order_timestamp(&self) -> Option<i64> {
        self.to_exch.earliest_timestamp()
    }

    /// Sends the order request to the exchange.
    /// If it is rejected before reaching the matching engine (as reflected in the order latency
    /// information), `reject` is invoked and the rejection response is appended to the local order
    /// bus.
    ///
    /// A request reports no execution, so its execution fields are cleared here. The local builds
    /// a request from its own copy of the order, which carries the last execution the local
    /// received and has already applied, and the exchange hands a request straight back as the
    /// response whenever it rejects it or acknowledges an in-place amendment. Only a response
    /// from the exchange reports an execution.
    pub fn request<F>(&mut self, order: Order, mut reject: F)
    where
        F: FnMut(&mut Order),
    {
        // The execution fields are cleared here, by the only constructor a request has.
        let mut request = ExchRequest::new(order);

        let local_timestamp = request.order().local_timestamp;
        let order_entry_latency = self.order_latency.entry(local_timestamp, request.order());
        // Negative latency indicates that the order is rejected for technical reasons, and its
        // value represents the latency that the local experiences when receiving the rejection
        // notification.
        if order_entry_latency < 0 {
            // Rejects the order. A request that never reached the exchange is handed back as
            // the response — a named act now, rather than "append the request onto the
            // response bus".
            reject(request.order_mut());
            let rej_recv_timestamp = local_timestamp - order_entry_latency;
            self.to_local
                .append(ExchResponse::rejected(request), rej_recv_timestamp);
        } else {
            let exch_recv_timestamp = local_timestamp + order_entry_latency;
            self.to_exch.append(request, exch_recv_timestamp);
        }
    }

    /// Receives the order response from the exchange, which is expected to be received at
    /// `receipt_timestamp`.
    pub fn receive(&mut self, receipt_timestamp: i64) -> Option<ExchResponse> {
        if let Some(timestamp) = self.to_local.earliest_timestamp() {
            if timestamp == receipt_timestamp {
                self.to_local.pop_front().map(|(response, _)| response)
            } else {
                assert!(timestamp > receipt_timestamp);
                None
            }
        } else {
            None
        }
    }
}

/// Creates bidirectional order buses with the order latency model.
pub fn order_bus<LM>(order_latency: LM) -> (ExchToLocal<LM>, LocalToExch<LM>)
where
    LM: LatencyModel + Clone,
{
    let to_exch: OrderBus<ExchRequest> = OrderBus::new();
    let to_local: OrderBus<ExchResponse> = OrderBus::new();
    (
        ExchToLocal {
            to_exch: to_exch.clone(),
            to_local: to_local.clone(),
            order_latency: order_latency.clone(),
        },
        LocalToExch {
            to_exch,
            to_local,
            order_latency,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{
        ExecDelta,
        Liquidity,
        OrdType,
        Order,
        OrderId,
        Price,
        Qty,
        Side,
        Status,
        TickSize,
        TimeInForce,
    };

    fn executed_order() -> Order {
        let mut order = Order::new(
            OrderId::new(1),
            Price::new(100.0).to_ticks(TickSize::new(0.01)),
            TickSize::new(0.01),
            Qty::new(10.0),
            Side::Buy,
            OrdType::Limit,
            TimeInForce::GTC,
        );
        // The local's copy after it has received, and already applied, an execution.
        order.status = Status::PartiallyFilled;
        order.record_execution(
            ExecDelta::of_execution(3.0),
            Price::new(100.0).to_ticks(TickSize::new(0.01)),
            Some(Liquidity::Maker),
        );
        order
    }

    /// **A request handed back cannot be mistaken for a fill** (invariant E4).
    ///
    /// This is the §4.6 double-count at its source. The local builds a request from its own
    /// copy of the order, which still carries the last execution it received *and already
    /// applied*; the exchange hands that request straight back as the response whenever it
    /// rejects it or acknowledges an in-place amendment. Reading the stale execution fields as
    /// a fresh fill counted the same quantity twice — silently, and worse the more an order
    /// was amended.
    ///
    /// Zeroing the fields in `LocalToExch::request` fixed the instance. Making a request a
    /// *different type* from a response is what closes the class: [`ExchRequest::new`] is the
    /// only constructor and is where the clearing happens, and [`ExchResponse::rejected`] is
    /// the only way a request becomes a response — so a future processor cannot route around
    /// the one place, because there is no other route.
    #[test]
    fn a_request_cannot_be_received_as_a_response() {
        let order = executed_order();
        assert_eq!(
            order.exec_qty,
            ExecDelta::of_execution(3.0),
            "the local's copy carries the execution it has already applied"
        );

        // Building a request strips it — the only constructor, so this cannot be skipped.
        let request = ExchRequest::new(order);
        assert_eq!(request.order().exec_qty, ExecDelta::ZERO);
        assert_eq!(request.order().exec_price_tick, PriceTick::new(0));
        // The rest of the order is untouched: a request still describes the order it is about.
        assert_eq!(request.order().qty, Qty::new(10.0));
        assert_eq!(request.order().leaves_qty, Qty::new(7.0));

        // A rejection is the request handed back, and it reports no execution.
        let rejected = ExchResponse::rejected(request).into_order();
        assert_eq!(
            rejected.exec_qty,
            ExecDelta::ZERO,
            "a request the exchange never accepted reports no fill"
        );
    }

    /// The response side is the *only* side that may carry an execution.
    ///
    /// `Local::process_recv_order_` takes what `LocalToExch::receive` yields, which is an
    /// [`ExchResponse`]. An [`ExchRequest`] is not one and cannot be coerced into one except
    /// through [`ExchResponse::rejected`], which consumes it and whose input has already been
    /// stripped. That is the whole invariant, and it is a compile-time fact rather than
    /// something this test could observe by running.
    #[test]
    fn only_a_response_reports_an_execution() {
        let response = ExchResponse::new(executed_order()).into_order();
        assert_eq!(
            response.exec_qty,
            ExecDelta::of_execution(3.0),
            "a response from the exchange is the one thing that may report a fill"
        );
    }
}
