use std::sync::{Arc, Mutex};

use chrono::Utc;
use hashbrown::HashMap;
use hftbacktest::types::{ExecDelta, Order, OrderId, PriceTick, Qty, Status};
use tracing::error;

use crate::{
    binancespot::{
        BinanceSpotError, // msg::{rest::OrderResponse, stream::OrderTradeUpdate},
        msg::{rest::OrderResponse, stream::ExecutionReport},
    },
    connector::GetOrders,
    utils::{RefSymbolOrderId, SymbolOrderId, generate_rand_string},
};

#[derive(Debug)]
struct OrderExt {
    symbol: String,
    order: Order,
    removed_by_ws: bool,
    removed_by_rest: bool,
}

pub type SharedOrderManager = Arc<Mutex<OrderManager>>;

pub type ClientOrderId = String;

#[derive(Default, Debug)]
pub struct OrderManager {
    prefix: String,
    orders: HashMap<ClientOrderId, OrderExt>,
    order_id_map: HashMap<SymbolOrderId, ClientOrderId>,
}

impl OrderManager {
    pub fn new(prefix: &str) -> Self {
        Self {
            prefix: prefix.to_string(),
            orders: Default::default(),
            order_id_map: Default::default(),
        }
    }

    pub fn update_from_ws(
        &mut self,
        resp: &ExecutionReport,
    ) -> Result<Option<Order>, BinanceSpotError> {
        if !resp.client_order_id.starts_with(&self.prefix) {
            return Err(BinanceSpotError::PrefixUnmatched);
        }
        let order_ext = self
            .orders
            .get_mut(&resp.client_order_id)
            .ok_or(BinanceSpotError::OrderNotFound)?;

        let already_removed = order_ext.removed_by_ws || order_ext.removed_by_rest;
        if resp.event_time * 1_000_000 >= order_ext.order.exch_timestamp {
            order_ext.order.qty = Qty::new(resp.quantity);
            order_ext.order.leaves_qty =
                Qty::new(resp.quantity - resp.order_filled_accumulated_quantity);
            order_ext.order.side = resp.side;
            order_ext.order.time_in_force = resp.time_in_force;
            order_ext.order.exch_timestamp = resp.event_time * 1_000_000;
            order_ext.order.status = resp.order_status;
            order_ext.order.exec_price_tick = PriceTick::new(
                (resp.last_filled_price / order_ext.order.tick_size.get()).round() as i64,
            );
            // `order_last_filled_quantity` is Binance's per-fill quantity — already a delta.
            order_ext.order.exec_qty = ExecDelta::of_execution(resp.order_last_filled_quantity);
            order_ext.order.order_type = resp.order_type;
        }

        let result = if already_removed {
            None
        } else {
            Some(order_ext.order.clone())
        };

        if order_ext.order.status != Status::New
            && order_ext.order.status != Status::PartiallyFilled
        {
            order_ext.removed_by_ws = true;
            if !already_removed {
                self.order_id_map.remove(&RefSymbolOrderId::new(
                    &order_ext.symbol,
                    order_ext.order.order_id,
                ));
            }

            if order_ext.removed_by_ws && order_ext.removed_by_rest {
                self.orders.remove(&resp.client_order_id).unwrap();
            }
        }

        Ok(result)
    }

    pub fn update_submit_fail(
        &mut self,
        client_order_id: &ClientOrderId,
        error: &BinanceSpotError,
    ) -> Option<Order> {
        match error {
            BinanceSpotError::OrderError { code: -5022, .. } => {
                // GTX rejection.
            }
            BinanceSpotError::OrderError { code: -1008, .. } => {
                // Server is currently overloaded with other requests. Please try again in a few minutes.
                error!(
                    "Server is currently overloaded with other requests. Please try again in a few minutes."
                );
            }
            BinanceSpotError::OrderError { code: -2019, .. } => {
                // Margin is insufficient.
                error!("Margin is insufficient.");
            }
            BinanceSpotError::OrderError { code: -1015, .. } => {
                // Too many new orders; current limit is 300 orders per TEN_SECONDS.
                error!("Too many new orders; current limit is 300 orders per TEN_SECONDS.");
            }
            error => {
                error!(?error, "submit error");
            }
        }
        self.update_from_rest_fail(client_order_id, Some(Status::Expired))
    }

    pub fn update_cancel_fail(
        &mut self,
        client_order_id: &ClientOrderId,
        error: &BinanceSpotError,
    ) -> Option<Order> {
        match error {
            BinanceSpotError::OrderError { code: -2011, .. } => {
                // The given order may no longer exist; it could have already been filled or
                // canceled. But, it cannot determine the order status because it lacks the
                // necessary information.
                self.update_from_rest_fail(client_order_id, Some(Status::None))
            }
            error => {
                error!(?error, "cancel error");
                self.update_from_rest_fail(client_order_id, None)
            }
        }
    }

    pub fn update_from_rest_fail(
        &mut self,
        client_order_id: &ClientOrderId,
        status: Option<Status>,
    ) -> Option<Order> {
        let order_ext = self.orders.get_mut(client_order_id)?;
        // .ok_or(BinanceFuturesError::OrderNotFound)?;

        let already_removed = order_ext.removed_by_ws || order_ext.removed_by_rest;
        if let Some(status) = status {
            order_ext.order.status = status;
        }
        order_ext.order.req = Status::None;

        let result = if already_removed {
            None
        } else {
            Some(order_ext.order.clone())
        };

        if order_ext.order.status != Status::New
            && order_ext.order.status != Status::PartiallyFilled
        {
            order_ext.removed_by_rest = true;
            if !already_removed {
                self.order_id_map.remove(&RefSymbolOrderId::new(
                    &order_ext.symbol,
                    order_ext.order.order_id,
                ));
            }

            if order_ext.removed_by_ws && order_ext.removed_by_rest {
                self.orders.remove(client_order_id).unwrap();
            }
        }

        result
    }

    pub fn update_from_rest(
        &mut self,
        client_order_id: &ClientOrderId,
        resp: &OrderResponse,
    ) -> Option<Order> {
        let order_ext = self.orders.get_mut(client_order_id)?;
        // .ok_or(BinanceFuturesError::OrderNotFound)?;

        let already_removed = order_ext.removed_by_ws || order_ext.removed_by_rest;
        if resp.transact_time * 1_000_000 >= order_ext.order.exch_timestamp {
            order_ext.order.qty = Qty::new(resp.orig_qty);
            order_ext.order.leaves_qty = Qty::new(resp.orig_qty - resp.executed_qty.get());
            order_ext.order.side = resp.side;
            order_ext.order.time_in_force = resp.time_in_force;
            order_ext.order.exch_timestamp = resp.transact_time * 1_000_000;
            order_ext.order.status = resp.status;
            // The last filled price isn't available in the REST response, and neither is
            // any per-execution quantity: `executedQty` is the order's **running total**. It
            // used to be written into `exec_qty` regardless, so every REST reconcile
            // re-reported everything the order had filled so far (invariant E5). Execution
            // details arrive on the WebSocket stream, which is what the comment above always
            // said; `CumulativeFilled` is what now makes writing the total here impossible.
            //
            // Zeroed rather than merely left alone: the tracked order still carries the delta
            // of the last execution the WebSocket stream reported, and republishing it on an
            // update that observed no execution reports that fill a second time — the same
            // trap, one step further along. An update that saw no execution says so.
            order_ext.order.exec_qty = ExecDelta::ZERO;
            order_ext.order.order_type = resp.order_type;
            order_ext.order.req = Status::None;
        }

        let result = if already_removed {
            None
        } else {
            Some(order_ext.order.clone())
        };

        if order_ext.order.status != Status::New
            && order_ext.order.status != Status::PartiallyFilled
        {
            order_ext.removed_by_rest = true;
            if !already_removed {
                self.order_id_map.remove(&RefSymbolOrderId::new(
                    &order_ext.symbol,
                    order_ext.order.order_id,
                ));
            }

            if order_ext.removed_by_ws && order_ext.removed_by_rest {
                self.orders.remove(client_order_id).unwrap();
            }
        }

        result
    }

    pub fn prepare_client_order_id(&mut self, symbol: String, order: Order) -> Option<String> {
        let symbol_order_id = SymbolOrderId::new(symbol.clone(), order.order_id);
        if self.order_id_map.contains_key(&symbol_order_id) {
            return None;
        }

        let client_order_id = format!("{}{}", self.prefix, generate_rand_string(16));
        if self.orders.contains_key(&client_order_id) {
            return None;
        }

        self.order_id_map
            .insert(symbol_order_id, client_order_id.clone());
        self.orders.insert(
            client_order_id.clone(),
            OrderExt {
                symbol,
                order,
                removed_by_ws: false,
                removed_by_rest: false,
            },
        );
        Some(client_order_id)
    }

    pub fn get_client_order_id(&self, symbol: &str, order_id: OrderId) -> Option<String> {
        self.order_id_map
            .get(&RefSymbolOrderId::new(symbol, order_id))
            .cloned()
    }

    /// Due to API instability or network issues, discrepancies can occur where an order is deleted
    /// by one channel but remains active because its deletion wasn't confirmed by both channels.
    /// The gc method resolves this by removing orders that were deleted by one channel but not
    /// confirmed by the other, after a defined threshold period.
    pub fn gc(&mut self) {
        let now = Utc::now().timestamp_nanos_opt().unwrap();
        let stale_ts = now - 300_000_000_000;
        let stale_ids: Vec<(_, _)> = self
            .orders
            .iter()
            .filter(|&(_, wrapper)| {
                wrapper.order.status != Status::New
                    && wrapper.order.status != Status::PartiallyFilled
                    && wrapper.order.status != Status::Unsupported
                    && wrapper.order.exch_timestamp < stale_ts
            })
            .map(|(client_order_id, wrapper)| {
                (
                    client_order_id.clone(),
                    SymbolOrderId::new(wrapper.symbol.clone(), wrapper.order.order_id),
                )
            })
            .collect();
        for (client_order_id, order_id) in stale_ids.iter() {
            if self.order_id_map.contains_key(order_id) {
                // todo: something went wrong?
                self.order_id_map.remove(order_id).unwrap();
            }
            self.orders.remove(client_order_id);
        }
    }

    pub fn cancel_all_from_rest(&mut self, symbol: &str) -> Vec<Order> {
        let mut removed_orders = Vec::new();
        let mut removed_order_ids = Vec::new();
        for (client_order_id, order_ext) in &mut self.orders {
            if order_ext.symbol != symbol {
                continue;
            }
            let already_removed = order_ext.removed_by_ws || order_ext.removed_by_rest;

            order_ext.removed_by_rest = true;
            order_ext.order.status = Status::Canceled;
            // todo: check if the exchange timestamp exists in the REST response.
            order_ext.order.exch_timestamp = Utc::now().timestamp_nanos_opt().unwrap();
            if !already_removed {
                self.order_id_map
                    .remove(&RefSymbolOrderId::new(symbol, order_ext.order.order_id));
                removed_orders.push(order_ext.order.clone());
            }

            // Completely deletes the order if it is removed by both the REST response and the
            // WebSocket stream.
            if order_ext.removed_by_ws && order_ext.removed_by_rest {
                removed_order_ids.push(client_order_id.clone());
            }
        }

        for order_id in removed_order_ids {
            self.orders.remove(&order_id).unwrap();
        }
        removed_orders
    }
}

impl GetOrders for OrderManager {
    fn orders(&self, symbol: Option<String>) -> Vec<Order> {
        self.orders
            .iter()
            .filter(|(_, order)| {
                symbol.as_ref().map(|s| order.symbol == *s).unwrap_or(true) && order.order.active()
            })
            .map(|(_, order)| &order.order)
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use hftbacktest::types::{
        ExecDelta,
        OrdType,
        Order,
        OrderId,
        PriceTick,
        Qty,
        Side,
        Status,
        TickSize,
        TimeInForce,
    };

    use super::*;

    /// A REST order response reports **no execution** (invariant E5).
    ///
    /// The spot twin of the Binance-futures pin, and it is a separate test because these are
    /// two independent backends that made the same mistake independently: Binance REST's
    /// `executedQty` is the order's *running total*, and it was assigned straight into
    /// `Order::exec_qty`, a per-execution delta. A REST reconcile therefore republished every
    /// fill the bot had already accounted for.
    ///
    /// Zeroing rather than merely not-writing matters just as much: the tracked order still
    /// holds the last WebSocket execution's delta, so leaving the field alone republishes
    /// *that* — the same double-report one step further along.
    #[test]
    fn a_rest_order_response_reports_no_execution() {
        let mut manager = OrderManager::new("test");
        let mut order = Order::new(
            OrderId::new(1),
            PriceTick::new(30_000_00),
            TickSize::new(0.01),
            Qty::new(0.005),
            Side::Buy,
            OrdType::Limit,
            TimeInForce::GTC,
        );
        order.status = Status::PartiallyFilled;
        order.leaves_qty = Qty::new(0.002);
        // The last execution the WebSocket stream reported, already applied by the bot.
        order.exec_qty = ExecDelta::of_execution(0.003);
        manager.orders.insert(
            "test_abc".to_string(),
            OrderExt {
                symbol: "btcusdt".to_string(),
                order,
                removed_by_ws: false,
                removed_by_rest: false,
            },
        );

        let resp: OrderResponse = serde_json::from_str(
            r#"{
                "symbol": "BTCUSDT",
                "orderId": 1,
                "orderListId": -1,
                "clientOrderId": "test_abc",
                "transactTime": 1700000000000,
                "price": "30000.0",
                "origQty": "0.005",
                "executedQty": "0.005",
                "origQuoteOrderQty": "0.0",
                "cummulativeQuoteQty": "150.0",
                "status": "FILLED",
                "timeInForce": "GTC",
                "type": "LIMIT",
                "side": "BUY",
                "workingTime": 1700000000000,
                "selfTradePreventionMode": "NONE",
                "fills": []
            }"#,
        )
        .unwrap();

        let published = manager
            .update_from_rest(&"test_abc".to_string(), &resp)
            .unwrap();

        assert_eq!(
            published.exec_qty,
            ExecDelta::ZERO,
            "a REST response carries no per-execution quantity; publishing its cumulative \
             executedQty re-reports fills the bot has already accounted for"
        );
        assert_eq!(published.leaves_qty, Qty::new(0.0));
        assert_eq!(published.status, Status::Filled);
    }
}
