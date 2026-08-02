use std::sync::{Arc, Mutex};

use chrono::Utc;
use hashbrown::HashMap;
use hftbacktest::types::{ExecDelta, Order, OrderId, Status};
use tracing::error;

use crate::{
    binancefutures::{
        BinanceFuturesError,
        msg::{rest::OrderResponse, stream::OrderTradeUpdate},
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

/// Binance has separated channels for REST APIs and Websocket. Order responses are delivered
/// through these channels, with no guaranteed order of transmission. To prevent duplicate handling
/// of order responses, such as order deletion due to cancellation or fill, OrderManager manages the
/// order states before transmitting the responses to a live bot.
///
/// Deletions must be confirmed by both channels. If not, differences in response times could result
/// in attempts to update an order that has already been deleted, potentially creating a ghost order
/// unintentionally.
///
/// To handle this, the `client_order_id` should include a random ID to differentiate it, even when
/// the order ID is the same(bot's order id). This is necessary because the order deletion is
/// immediately notified to the bot, but the Connector must still retain the `client_order_id` in
/// case an update arrives later from the other channel, which has not yet sent the deletion
/// message.
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
        resp: &OrderTradeUpdate,
    ) -> Result<Option<Order>, BinanceFuturesError> {
        if !resp.order.client_order_id.starts_with(&self.prefix) {
            return Err(BinanceFuturesError::PrefixUnmatched);
        }
        let order_ext = self
            .orders
            .get_mut(&resp.order.client_order_id)
            .ok_or(BinanceFuturesError::OrderNotFound)?;

        let already_removed = order_ext.removed_by_ws || order_ext.removed_by_rest;
        if resp.transaction_time * 1_000_000 >= order_ext.order.exch_timestamp {
            order_ext.order.qty = resp.order.original_qty;
            order_ext.order.leaves_qty =
                resp.order.original_qty - resp.order.order_filled_accumulated_qty;
            order_ext.order.side = resp.order.side;
            order_ext.order.time_in_force = resp.order.time_in_force;
            order_ext.order.exch_timestamp = resp.transaction_time * 1_000_000;
            order_ext.order.status = resp.order.order_status;
            order_ext.order.exec_price_tick =
                (resp.order.last_filled_price / order_ext.order.tick_size).round() as i64;
            // `order_last_filled_qty` is Binance's per-fill quantity — already a delta.
            order_ext.order.exec_qty = ExecDelta::of_execution(resp.order.order_last_filled_qty);
            order_ext.order.order_type = resp.order.order_type;
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
                self.orders.remove(&resp.order.client_order_id).unwrap();
            }
        }

        Ok(result)
    }

    pub fn update_submit_fail(
        &mut self,
        client_order_id: &ClientOrderId,
        error: &BinanceFuturesError,
    ) -> Option<Order> {
        match error {
            BinanceFuturesError::OrderError { code: -5022, .. } => {
                // GTX rejection.
            }
            BinanceFuturesError::OrderError { code: -1008, .. } => {
                // Server is currently overloaded with other requests. Please try again in a few minutes.
                error!(
                    "Server is currently overloaded with other requests. Please try again in a few minutes."
                );
            }
            BinanceFuturesError::OrderError { code: -2019, .. } => {
                // Margin is insufficient.
                error!("Margin is insufficient.");
            }
            BinanceFuturesError::OrderError { code: -1015, .. } => {
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
        error: &BinanceFuturesError,
    ) -> Option<Order> {
        match error {
            BinanceFuturesError::OrderError { code: -2011, .. } => {
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
        if resp.update_time * 1_000_000 >= order_ext.order.exch_timestamp {
            order_ext.order.qty = resp.orig_qty;
            order_ext.order.leaves_qty = resp.orig_qty - resp.cum_qty.get();
            order_ext.order.side = resp.side;
            order_ext.order.time_in_force = resp.time_in_force;
            order_ext.order.exch_timestamp = resp.update_time * 1_000_000;
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
            order_ext.order.order_type = resp.ty;
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
    use hftbacktest::types::{ExecDelta, OrdType, Order, Side, Status, TimeInForce};

    use super::*;

    fn tracked(manager: &mut OrderManager, client_order_id: &str, filled_so_far: f64) {
        let mut order = Order::new(
            1,
            30_000_00,
            0.01,
            0.005,
            Side::Buy,
            OrdType::Limit,
            TimeInForce::GTC,
        );
        order.status = Status::PartiallyFilled;
        order.leaves_qty = 0.005 - filled_so_far;
        // The last execution the WebSocket stream reported, already applied by the bot.
        order.exec_qty = ExecDelta::of_execution(filled_so_far);
        manager.orders.insert(
            client_order_id.to_string(),
            OrderExt {
                symbol: "btcusdt".to_string(),
                order,
                removed_by_ws: false,
                removed_by_rest: false,
            },
        );
    }

    /// A REST order response reports **no execution** (invariant E5).
    ///
    /// Binance's REST `executedQty` is the order's *running total*, and it used to be assigned
    /// straight into `Order::exec_qty`, which is a per-execution delta. The consequences were
    /// silent and grew with the order: an order that had already filled 0.003 and was polled
    /// by a REST reconcile republished `exec_qty = 0.003` — a fill the bot had already
    /// accounted for — and a later poll after another partial republished the whole total
    /// again.
    ///
    /// The response carries no per-fill data at all (its own comment has always said so; the
    /// last filled price is not in it either), so the honest report is `ExecDelta::ZERO` and
    /// the WebSocket stream remains the sole reporter of executions.
    ///
    /// **This is a behaviour change on this backend, not a pure refactor**: information that
    /// used to arrive — wrongly typed — now does not arrive at all if the WS stream is down.
    /// That is the correct direction (a wrong fill is worse than no fill), and neither Binance
    /// backend is in live use.
    #[test]
    fn a_rest_order_response_reports_no_execution() {
        let mut manager = OrderManager::new("test");
        tracked(&mut manager, "test_abc", 0.003);

        // The venue restates the order with a cumulative 0.005 filled.
        let resp: OrderResponse = serde_json::from_str(
            r#"{
                "clientOrderId": "test_abc",
                "cumQty": "0.005",
                "cumQuote": "150.0",
                "executedQty": "0.005",
                "orderId": 1,
                "avgPrice": "30000.0",
                "origQty": "0.005",
                "price": "30000.0",
                "reduceOnly": false,
                "side": "BUY",
                "positionSide": "BOTH",
                "status": "FILLED",
                "stopPrice": "0.0",
                "closePosition": false,
                "symbol": "BTCUSDT",
                "timeInForce": "GTC",
                "type": "LIMIT",
                "origType": "LIMIT",
                "updateTime": 1700000000000,
                "workingType": "CONTRACT_PRICE",
                "priceProtect": false,
                "priceMatch": "NONE",
                "selfTradePreventionMode": "NONE",
                "goodTillDate": 0
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
        // The cumulative figure is still used where it genuinely is a cumulative question.
        assert_eq!(published.leaves_qty, 0.0);
        assert_eq!(published.status, Status::Filled);
    }
}
