use std::sync::{Arc, Mutex};

use hashbrown::HashMap;
use hftbacktest::{
    prelude::get_precision,
    types::{ExecDelta, OrdType, Order, OrderId, Price, Side, Status, TimeInForce},
};

use crate::{
    bybit::{
        BybitError,
        msg::{Execution, FastExecution, Order as BybitOrder, PrivateOrder},
    },
    connector::GetOrders,
    utils::{RefSymbolOrderId, SymbolOrderId, generate_rand_string},
};

pub type SharedOrderManager = Arc<Mutex<OrderManager>>;

pub type OrderLinkId = String;

#[derive(Clone)]
pub struct OrderExt {
    pub symbol: String,
    pub order: Order,
}

pub struct OrderManager {
    prefix: String,
    orders: HashMap<OrderLinkId, OrderExt>,
    order_id_map: HashMap<SymbolOrderId, OrderLinkId>,
}

impl OrderManager {
    pub fn new(prefix: &str) -> Self {
        Self {
            prefix: prefix.to_string(),
            orders: Default::default(),
            order_id_map: Default::default(),
        }
    }

    pub fn update_order(&mut self, data: &PrivateOrder) -> Result<OrderExt, BybitError> {
        if !data.order_link_id.starts_with(&self.prefix) {
            return Err(BybitError::PrefixUnmatched);
        }
        let order = self
            .orders
            .get_mut(&data.order_link_id)
            .ok_or(BybitError::OrderNotFound)?;
        order.order.req = Status::None;
        order.order.status = data.order_status;
        order.order.exch_timestamp = data.updated_time * 1_000_000;
        let is_active = order.order.active();
        if !is_active {
            self.order_id_map
                .remove(&RefSymbolOrderId::new(&order.symbol, order.order.order_id));
            Ok(self.orders.remove(&data.order_link_id).unwrap())
        } else {
            Ok(order.clone())
        }
    }

    pub fn update_execution(&mut self, data: &Execution) -> Result<OrderExt, BybitError> {
        if !data.order_link_id.starts_with(&self.prefix) {
            return Err(BybitError::PrefixUnmatched);
        }
        let order_info = self
            .orders
            .get_mut(&data.order_link_id)
            .ok_or(BybitError::OrderNotFound)?;
        order_info.order.exec_price_tick =
            Price::new(data.exec_price).to_ticks(order_info.order.tick_size);
        order_info.order.exec_qty = ExecDelta::of_execution(data.exec_qty);
        order_info.order.exch_timestamp = data.exec_time * 1_000_000;
        Ok(order_info.clone())
    }

    pub fn update_fast_execution(&mut self, data: &FastExecution) -> Result<OrderExt, BybitError> {
        // fixme: there is no valid order_link_id.
        if !data.order_link_id.starts_with(&self.prefix) {
            return Err(BybitError::PrefixUnmatched);
        }
        let order_info = self
            .orders
            .get_mut(&data.order_link_id)
            .ok_or(BybitError::OrderNotFound)?;
        order_info.order.exec_price_tick =
            Price::new(data.exec_price).to_ticks(order_info.order.tick_size);
        order_info.order.exec_qty = ExecDelta::of_execution(data.exec_qty);
        order_info.order.exch_timestamp = data.exec_time * 1_000_000;
        Ok(order_info.clone())
    }

    pub fn new_order(
        &mut self,
        symbol: &str,
        category: &str,
        order: Order,
    ) -> Result<BybitOrder, BybitError> {
        let price_prec = get_precision(order.tick_size.get());
        let order_link_id = format!("{}{}", self.prefix, generate_rand_string(16));
        let bybit_order = BybitOrder {
            symbol: symbol.to_string(),
            side: Some({
                match order.side {
                    Side::Buy => "Buy".to_string(),
                    Side::Sell => "Sell".to_string(),
                    Side::None | Side::Unsupported => return Err(BybitError::InvalidArg("side")),
                }
            }),
            order_type: Some({
                match order.order_type {
                    OrdType::Limit => "Limit".to_string(),
                    OrdType::Market => "Market".to_string(),
                    OrdType::Unsupported => return Err(BybitError::InvalidArg("order_type")),
                }
            }),
            qty: Some(format!("{:.5}", order.qty.get())),
            price: Some(format!("{:.prec$}", order.price().get(), prec = price_prec)),
            category: category.to_string(),
            time_in_force: Some({
                match order.time_in_force {
                    TimeInForce::GTC => "GTC".to_string(),
                    TimeInForce::GTX => "PostOnly".to_string(),
                    TimeInForce::FOK => "FOK".to_string(),
                    TimeInForce::IOC => "IOC".to_string(),
                    TimeInForce::Unsupported => {
                        return Err(BybitError::InvalidArg("time_in_force"));
                    }
                }
            }),
            order_link_id: order_link_id.clone(),
        };

        let symbol_order_id = SymbolOrderId::new(symbol.to_string(), order.order_id);
        if self.order_id_map.contains_key(&symbol_order_id) {
            return Err(BybitError::OrderAlreadyExist);
        }

        if self.orders.contains_key(&order_link_id) {
            return Err(BybitError::OrderAlreadyExist);
        }

        self.order_id_map
            .insert(symbol_order_id, order_link_id.clone());
        self.orders.insert(
            order_link_id,
            OrderExt {
                symbol: symbol.to_string(),
                order,
            },
        );
        Ok(bybit_order)
    }

    pub fn cancel_order(
        &mut self,
        symbol: &str,
        category: &str,
        order_id: OrderId,
    ) -> Result<BybitOrder, BybitError> {
        let order_link_id = self
            .order_id_map
            .get(&RefSymbolOrderId::new(symbol, order_id))
            .ok_or(BybitError::OrderNotFound)?;
        let order = BybitOrder {
            symbol: symbol.to_string(),
            side: None,
            order_type: None,
            qty: None,
            price: None,
            category: category.to_string(),
            time_in_force: None,
            order_link_id: order_link_id.clone(),
        };
        Ok(order)
    }

    pub fn update_submit_fail(&mut self, order_link_id: &str) -> Result<OrderExt, BybitError> {
        let mut order = self
            .orders
            .remove(order_link_id)
            .ok_or(BybitError::OrderNotFound)?;
        order.order.req = Status::None;
        order.order.status = Status::Expired;
        self.order_id_map
            .remove(&RefSymbolOrderId::new(&order.symbol, order.order.order_id));
        Ok(order)
    }

    pub fn update_cancel_fail(&mut self, order_link_id: &str) -> Result<OrderExt, BybitError> {
        let mut order_info = self
            .orders
            .get_mut(order_link_id)
            .cloned()
            .ok_or(BybitError::OrderNotFound)?;
        order_info.order.req = Status::None;
        Ok(order_info)
    }

    pub fn cancel_all(&mut self, symbol: &str) -> Vec<Order> {
        let mut removed_order_ids = Vec::new();
        for (order_link_id, order_ext) in &mut self.orders {
            if order_ext.symbol != symbol {
                continue;
            }

            order_ext.order.status = Status::Canceled;

            self.order_id_map
                .remove(&RefSymbolOrderId::new(symbol, order_ext.order.order_id));
            removed_order_ids.push(order_link_id.clone());
        }

        removed_order_ids
            .iter()
            .map(|id| self.orders.remove(id).unwrap().order)
            .collect()
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
    use hftbacktest::types::{ExecDelta, OrderId, Price, PriceTick, Qty, TickSize};

    use super::*;

    fn execution(order_link_id: &str, exec_price: f64, exec_qty: f64) -> Execution {
        Execution {
            category: "linear".to_string(),
            symbol: "BTCUSDT".to_string(),
            exec_fee: "0".to_string(),
            exec_id: "e1".to_string(),
            exec_price,
            exec_qty,
            exec_type: "Trade".to_string(),
            exec_value: "0".to_string(),
            is_maker: true,
            fee_rate: "0".to_string(),
            trade_iv: String::new(),
            mark_iv: String::new(),
            block_trade_id: String::new(),
            mark_price: "0".to_string(),
            index_price: "0".to_string(),
            underlying_price: "0".to_string(),
            leaves_qty: 0.0,
            order_id: "o1".to_string(),
            order_link_id: order_link_id.to_string(),
            order_price: exec_price,
            order_qty: exec_qty,
            order_type: "Limit".to_string(),
            stop_order_type: String::new(),
            side: "Buy".to_string(),
            exec_time: 1_700_000_000_000,
            is_leverage: "0".to_string(),
            closed_size: "0".to_string(),
            seq: 1,
        }
    }

    /// **An execution price is converted by the tick size, never by the price** (invariant C5).
    ///
    /// This backend divided `exec_price` by `order.price_tick.get()` — the order's price *already in
    /// ticks* — where every other backend divides by `tick_size`. Both operands are numbers
    /// about price, so the expression reads plausibly and the result is a small integer that
    /// still looks like a tick; nothing about it is absurd enough to notice.
    ///
    /// With the order resting at 1000 ticks of 0.1 and executing at 100.5, the answer is 1005.
    /// The old expression computed `round(100.5 / 1000) == 0`, i.e. it reported every
    /// execution as having happened at tick 0 — a price of zero. `Order::exec_price()`
    /// multiplies that back by the tick size, so anything reading the execution price of a
    /// bybit fill read 0.0.
    #[test]
    fn an_execution_price_is_converted_by_the_tick_size_not_by_the_price() {
        let mut manager = OrderManager::new("test");
        let mut order = Order::new(
            OrderId::new(1),
            PriceTick::new(1000),
            TickSize::new(0.1),
            Qty::new(1.0),
            Side::Buy,
            OrdType::Limit,
            TimeInForce::GTC,
        );
        order.status = Status::New;
        manager.orders.insert(
            "test_abc".to_string(),
            OrderExt {
                symbol: "BTCUSDT".to_string(),
                order,
            },
        );

        let updated = manager
            .update_execution(&execution("test_abc", 100.5, 1.0))
            .unwrap();

        assert_eq!(
            updated.order.exec_price_tick,
            PriceTick::new(1005),
            "an execution at 100.5 with a 0.1 tick is tick 1005; dividing by price_tick \
             (1000) yields 0 and reports every fill as having happened at a price of zero"
        );
        assert_eq!(updated.order.exec_price(), Price::new(100.5));
        assert_eq!(updated.order.exec_qty, ExecDelta::of_execution(1.0));
    }
}
