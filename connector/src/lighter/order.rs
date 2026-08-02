//! The order-id mapping and order-state machine for the Lighter order path (design note
//! [`docs/design-lighter-connector.md`](../../../docs/design-lighter-connector.md) §3.5,
//! §4.11–§4.13). Modelled on `hyperliquid/ordermanager.rs`; the venue-specific parts are the
//! comments.
//!
//! ## The three identities, kept apart on purpose (§3.5, §4.14)
//!
//! * `order_id` — the bot's own `u64`, chosen by the strategy. What every `LiveEvent::Order`
//!   is keyed by.
//! * `client_order_index` (COI) — **ours**, minted here into `[1, 2^48-1]` (0 is reserved,
//!   §3.5). Signed into `CreateOrder`; the venue echoes it back on `account_all_orders`. The
//!   allocator is **monotonic** — a counter, never a pool with returns — because on the
//!   window between "cancel sent" and "cancel confirmed" a reused value collides (§3.5).
//! * `order_index` — the **venue's** internal id (~2^48), learned when the order first
//!   appears on the channel. What a cancel is addressed by (the signer cancels by index), and
//!   what reconciliation matches on. The in-order `nonce` field is a *fourth* thing and is
//!   never any of these (§4.14).
//!
//! ## Acceptance is a private-channel fact, never an HTTP one (§4.11)
//!
//! An order goes live in this map only when `account_all_orders` lists it with an open
//! status — [`OrderManager::apply_order_update`]. A `sendTx` HTTP 200 does **not** put an
//! order here: the duplicate-`ClientOrderIndex` transaction from the Phase-0 artifacts got
//! 200, spent a nonce and produced nothing, and the only place its rejection was legible was
//! `event_info.ae` (`crate::lighter::rest::parse_tx_verdict`). So the slot task confirms
//! placement off this channel with a deadline, and on a lapsed deadline expires the order
//! with [`OrderManager::submit_failed`] — it never treats 200 as "resting".
//!
//! Two more traps this encodes: state transitions are ordered **only** by `transaction_time`
//! (§4.12 — `updated_at` is block-granular and reorders a cancel before its open), and the
//! position comes from `account_all_positions` (§3.4), never from replaying fills.

use std::collections::{HashMap, HashSet};

use hftbacktest::types::{OrdType, Order, Side, Status, TimeInForce};
use thiserror::Error;
use tracing::{debug, error, warn};

use crate::{
    connector::GetOrders,
    lighter::{
        private_msg::{AccountOrder, AccountPosition},
        rest::MarketInfo,
        signer::CreateOrder,
    },
    utils::{Micros, Nanos, StatusVerdict},
};

/// 0 is `NilClientOrderIndex` and reserved; the allocator never hands it out (§3.5).
const MIN_COI: i64 = 1;
/// `ClientOrderIndex` is bounded to `[1, 2^48-1]` (§3.5). Past it the allocator fails closed
/// rather than wrap into 0 or a value the signer rejects.
const MAX_COI: i64 = (1 << 48) - 1;

// Lighter TIF codes (§3.5, SDK `constants.go`).
const TIF_IMMEDIATE_OR_CANCEL: u8 = 0;
const TIF_GOOD_TILL_TIME: u8 = 1;
const TIF_POST_ONLY: u8 = 2;
// Lighter order-type codes.
const ORDER_TYPE_LIMIT: u8 = 0;
const ORDER_TYPE_MARKET: u8 = 1;
/// `-1` asks the signer for the library-default lifetime (28 days); used for resting orders.
/// IOC/Market carry an empty (`0`) expiry (§3.5 `Validate()`).
const ORDER_EXPIRY_DEFAULT: i64 = -1;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum OrderError {
    #[error("order side {0:?} is neither buy nor sell")]
    UnsupportedSide(Side),
    #[error("time-in-force {0:?} has no Lighter equivalent (FOK is not offered)")]
    UnsupportedTif(TimeInForce),
    #[error("order_id {order_id} is already live on {symbol}")]
    OrderIdLive { symbol: String, order_id: u64 },
    #[error("no live order {order_id} on {symbol}")]
    OrderNotFound { symbol: String, order_id: u64 },
    #[error("order {order_id} on {symbol} is not confirmed yet; the venue order_index is unknown")]
    NotConfirmed { symbol: String, order_id: u64 },
    #[error("the client-order-index allocator is exhausted (reached 2^48-1)")]
    CoiExhausted,
    #[error("the price, size or market does not fit the venue's integer wire form: {0}")]
    OutOfRange(String),
}

/// A cancel addressed the way the venue wants it — by `(market_index, order_index)` (§3.5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelPlan {
    pub market_index: i64,
    pub order_index: i64,
}

/// One tracked order: the bot-facing `Order`, plus the venue bookkeeping around it.
struct Tracked {
    symbol: String,
    market_index: i64,
    order: Order,
    order_index: Option<i64>,
    /// The last `transaction_time` applied — the §4.12 watermark. A frame with a smaller one is
    /// stale and must not rewind the order. Typed [`Micros`] (like the frame it is compared to)
    /// so it can never be confused with the nanosecond `exch_timestamp` (T1).
    update_ts_us: Micros,
    /// Cumulative filled base amount seen, so a fill's `exec_qty` is reported as the delta of
    /// *this* execution, not the running total (`AGENTS.md` §4.6, `Order::exec_qty`).
    filled: f64,
    /// Whether the venue has ever confirmed this order on the channel. Until it has, a
    /// reconnect reconciliation must not expire it — its fate is the slot task's deadline to
    /// decide (§4.11), not a snapshot's silence.
    confirmed: bool,
}

/// Owns this connector's order state for one account.
pub struct OrderManager {
    next_coi: i64,
    /// Tracked orders, keyed by our COI.
    orders: HashMap<i64, Tracked>,
    /// `(symbol, order_id)` → COI. Refuses a double-live `order_id` and resolves a cancel.
    by_order_id: HashMap<(String, u64), i64>,
    /// The catalog row per registered symbol — the decimals `new_order` quantises onto (§3.5).
    markets: HashMap<String, MarketInfo>,
    /// `market_index` → the symbol the bot registered, so a private frame (which carries only
    /// `market_index`) is published under the name the bot keys its own book by (§3.5).
    symbol_by_market: HashMap<i64, String>,
}

impl OrderManager {
    /// A fresh manager. `coi_seed` is where the monotonic allocator starts — production seeds
    /// it from a millisecond timestamp so a restart does not reuse a previous run's still-live
    /// indices before reconciliation has cleared them (§3.5); tests pass a small fixed value.
    /// Clamped to `[1, 2^48-1]`.
    pub fn new(coi_seed: i64) -> Self {
        Self {
            next_coi: coi_seed.clamp(MIN_COI, MAX_COI),
            orders: HashMap::new(),
            by_order_id: HashMap::new(),
            markets: HashMap::new(),
            symbol_by_market: HashMap::new(),
        }
    }

    pub fn tracked(&self) -> usize {
        self.orders.len()
    }

    /// Records a resolved catalog row, so `new_order` can quantise onto its grid and private
    /// frames (which carry only `market_index`) map back to the registered symbol (§3.5).
    pub fn track_market(&mut self, info: MarketInfo) {
        self.symbol_by_market
            .insert(info.market_id, info.symbol.clone());
        self.markets.insert(info.symbol.clone(), info);
    }

    /// The catalog row for a registered symbol, if resolved.
    pub fn market_info(&self, symbol: &str) -> Option<MarketInfo> {
        self.markets.get(symbol).cloned()
    }

    /// The COI a live `(symbol, order_id)` maps to, if any.
    pub fn coi_for(&self, symbol: &str, order_id: u64) -> Option<i64> {
        self.by_order_id
            .get(&(symbol.to_string(), order_id))
            .copied()
    }

    /// Whether the venue has confirmed an order on the channel — `Some(true)` once it has
    /// appeared, `Some(false)` while a submit is still unconfirmed, `None` if not tracked
    /// (never placed, or already terminal). The slot task's confirmation deadline reads this:
    /// a still-tracked, still-unconfirmed order past its deadline is the §4.11 case to
    /// diagnose with `GET /tx`.
    pub fn is_confirmed(&self, coi: i64) -> Option<bool> {
        self.orders.get(&coi).map(|tracked| tracked.confirmed)
    }

    /// Mints the next monotonic COI, failing closed at the `2^48-1` ceiling and never 0 (§3.5).
    fn mint_coi(&mut self) -> Result<i64, OrderError> {
        if self.next_coi < MIN_COI || self.next_coi > MAX_COI {
            return Err(OrderError::CoiExhausted);
        }
        let coi = self.next_coi;
        self.next_coi += 1;
        Ok(coi)
    }

    /// Registers a new order and returns `(coi, CreateOrder)` — the COI the slot task will
    /// track it by and the wire form the signer signs.
    ///
    /// Quantises price and size onto the venue's grid from the **catalog** decimals (§3.5), so
    /// the integers on the wire are the venue's regardless of the tick and lot the bot chose.
    /// Refuses a second live order on the same `(symbol, order_id)` (§3.5).
    pub fn new_order(
        &mut self,
        symbol: &str,
        market: &MarketInfo,
        order: &Order,
    ) -> Result<(i64, CreateOrder), OrderError> {
        let is_ask = match order.side {
            Side::Buy => 0,
            Side::Sell => 1,
            other => return Err(OrderError::UnsupportedSide(other)),
        };
        let time_in_force = tif_of(order.time_in_force)?;
        let order_type = match order.order_type {
            OrdType::Limit => ORDER_TYPE_LIMIT,
            OrdType::Market => ORDER_TYPE_MARKET,
            other => {
                return Err(OrderError::OutOfRange(format!(
                    "unsupported order type {other:?}"
                )));
            }
        };

        // Quantise onto the venue's integer grid from the CATALOG decimals (§3.5), not from
        // the tick and lot the bot happens to have registered.
        let price_scaled = (order.price() * 10f64.powi(market.price_decimals as i32)).round();
        if !(0.0..=f64::from(u32::MAX)).contains(&price_scaled) {
            return Err(OrderError::OutOfRange(format!(
                "price {} scales to {price_scaled}, outside u32",
                order.price()
            )));
        }
        let price = price_scaled as u32;

        let base_scaled = (order.qty * 10f64.powi(market.size_decimals as i32)).round();
        if !(1.0..=(MAX_COI as f64)).contains(&base_scaled) {
            return Err(OrderError::OutOfRange(format!(
                "size {} scales to {base_scaled}, not a positive base amount",
                order.qty
            )));
        }
        let base_amount = base_scaled as i64;

        let market_index_wire = i16::try_from(market.market_id).map_err(|_| {
            OrderError::OutOfRange(format!("market_id {} exceeds i16", market.market_id))
        })?;

        let key = (symbol.to_string(), order.order_id);
        if self.by_order_id.contains_key(&key) {
            return Err(OrderError::OrderIdLive {
                symbol: symbol.to_string(),
                order_id: order.order_id,
            });
        }

        let coi = self.mint_coi()?;

        // IOC/Market carry an empty expiry; a resting order takes the library default (§3.5).
        let order_expiry = if time_in_force == TIF_IMMEDIATE_OR_CANCEL {
            0
        } else {
            ORDER_EXPIRY_DEFAULT
        };

        let wire = CreateOrder {
            market_index: market_index_wire,
            client_order_index: coi,
            base_amount,
            price,
            is_ask,
            order_type,
            time_in_force,
            reduce_only: 0,
            trigger_price: 0,
            order_expiry,
        };

        // Keep the accepted (quantised) size, so the copy published back is the truth about
        // what rests on the venue (HL §5.3).
        let mut accepted = order.clone();
        accepted.qty = base_amount as f64 * market.lot_size();
        accepted.leaves_qty = accepted.qty;
        accepted.req = Status::New;
        accepted.status = Status::New;

        self.by_order_id.insert(key, coi);
        self.orders.insert(
            coi,
            Tracked {
                symbol: symbol.to_string(),
                market_index: market.market_id,
                order: accepted,
                order_index: None,
                update_ts_us: Micros::new(0),
                filled: 0.0,
                confirmed: false,
            },
        );
        Ok((coi, wire))
    }

    /// The cancel plan for a live order — addressed by the venue's `order_index`.
    ///
    /// Errors if the order is unknown, or known but not yet confirmed (no `order_index`): a
    /// cancel cannot be addressed before the order has appeared on the channel.
    pub fn cancel_order(&self, symbol: &str, order_id: u64) -> Result<CancelPlan, OrderError> {
        let not_found = || OrderError::OrderNotFound {
            symbol: symbol.to_string(),
            order_id,
        };
        let coi = self
            .by_order_id
            .get(&(symbol.to_string(), order_id))
            .ok_or_else(not_found)?;
        let tracked = self.orders.get(coi).ok_or_else(not_found)?;
        let order_index = tracked.order_index.ok_or(OrderError::NotConfirmed {
            symbol: symbol.to_string(),
            order_id,
        })?;
        Ok(CancelPlan {
            market_index: tracked.market_index,
            order_index,
        })
    }

    /// Marks the request now in flight (`Status::New` for a submit, `Status::Canceled` for a
    /// cancel), so the bot sees it as pending. Keyed by COI.
    pub fn mark_requested(&mut self, coi: i64, req: Status) {
        if let Some(tracked) = self.orders.get_mut(&coi) {
            tracked.order.req = req;
        }
    }

    /// Applies one `account_all_orders` entry — the **only** acceptance signal (§4.11).
    ///
    /// Returns the `(symbol, order)` to publish when this is one of ours and the update moved
    /// it; `None` for a foreign COI, a previous run's, or a stale frame. Ordered strictly by
    /// `transaction_time` (§4.12): a frame older than the last applied is dropped, never
    /// rewound. A terminal status (canceled/filled) takes the order out of tracking.
    pub fn apply_order_update(&mut self, update: &AccountOrder) -> Option<(String, Order)> {
        let coi = update.client_order_index;
        let (status, symbol, order, order_id) = {
            // A COI this connector never minted — a human's order, or a previous run's — is
            // ignored, not published as ours (§4.11 / HL model).
            let tracked = self.orders.get_mut(&coi)?;
            // §4.12: the only monotone key is `transaction_time`; a frame older than the last
            // applied is stale and must not rewind the order (e.g. a late "open" after a
            // "canceled").
            if update.transaction_time_us < tracked.update_ts_us {
                debug!(
                    coi,
                    stale_by_us = tracked.update_ts_us.get() - update.transaction_time_us.get(),
                    "Ignoring an out-of-order Lighter order update (§4.12)."
                );
                return None;
            }
            // Classify before mutating. An unrecognised status is kept, never dropped: folding
            // it into a terminal `Status` would free the id and let the strategy stand a
            // duplicate at the venue (S1, §1.1). The order is left exactly as last known —
            // watermark not advanced, status unchanged, not dropped — so a later recognised
            // update applies normally, and reconnect reconciliation resolves it if truly gone.
            let status = match map_status(&update.status) {
                StatusVerdict::Known(status) => status,
                StatusVerdict::Unrecognised(raw) => {
                    error!(
                        coi,
                        status = %raw,
                        "An unrecognised Lighter order status; keeping the order tracked so it \
                         is not dropped and re-submitted as a duplicate. Reconnect \
                         reconciliation expires it if it is truly gone."
                    );
                    return None;
                }
            };
            tracked.update_ts_us = update.transaction_time_us;
            tracked.order_index = Some(update.order_index);
            tracked.confirmed = true;
            tracked.order.req = Status::None;
            tracked.order.status = status;
            // §4.4/T1: the venue states `transaction_time` in microseconds; the money-path
            // `exch_timestamp` is nanoseconds. Convert through the one checked bridge. On the
            // (year-2262) overflow, fail closed by leaving the timestamp unadvanced rather than
            // landing a wrapped value — the order-state update below still applies.
            match Nanos::from_micros(update.transaction_time_us) {
                Ok(ns) => {
                    tracked.order.exch_timestamp = tracked.order.exch_timestamp.max(ns.get());
                }
                Err(overflow) => {
                    warn!(
                        coi,
                        %overflow,
                        "A Lighter transaction_time does not fit in nanoseconds; leaving \
                         exch_timestamp unadvanced (§1.1)."
                    );
                }
            }
            tracked.order.leaves_qty = update.remaining_base_amount;
            // §4.6: `exec_qty` is the amount executed by THIS update, the delta of the
            // cumulative `filled_base_amount`, not the running total.
            if update.filled_base_amount > tracked.filled {
                tracked.order.exec_qty = update.filled_base_amount - tracked.filled;
                tracked.order.exec_price_tick =
                    (update.price / tracked.order.tick_size).round() as i64;
                // C2: `account_all_orders` states how much of the order is filled and nothing
                // about which side of the book provided it — the frame carries no per-execution
                // maker field. `None` is therefore the measurement, and it claims nothing; the
                // previous unconditional `maker = true` made the flag a constant that fee and
                // fill-quality attribution read as the venue's word (`AGENTS.md` §1.1).
                //
                // The public `trade` channel does carry `is_maker_ask`, but it says which side
                // of *that* trade rested, not which account did — it cannot attribute our own
                // execution. Reading a real per-fill flag needs an account trade feed this
                // backend does not subscribe to.
                tracked.order.set_liquidity(None);
                tracked.filled = update.filled_base_amount;
            } else {
                tracked.order.exec_qty = 0.0;
            }
            (
                status,
                tracked.symbol.clone(),
                tracked.order.clone(),
                tracked.order.order_id,
            )
        };
        // Drop (free the id) iff terminal. One definition of "terminal"
        // ([`Status::is_terminal`], exhaustive with no wildcard), so a future non-terminal
        // variant cannot fall through to this removal — it must be ruled there first (S2).
        if status.is_terminal() {
            self.by_order_id.remove(&(symbol.clone(), order_id));
            self.orders.remove(&coi);
        }
        Some((symbol, order))
    }

    /// Expires an order whose `sendTx` never produced one (a lapsed confirmation deadline, or
    /// `event_info.ae` said it was rejected — §4.11). Returns the `(symbol, order)` to publish
    /// as `Status::Expired`. Frees the `order_id`.
    pub fn submit_failed(&mut self, coi: i64) -> Option<(String, Order)> {
        let mut tracked = self.orders.remove(&coi)?;
        self.by_order_id
            .remove(&(tracked.symbol.clone(), tracked.order.order_id));
        tracked.order.req = Status::None;
        tracked.order.status = Status::Expired;
        Some((tracked.symbol, tracked.order))
    }

    /// Clears the pending flag on a cancel the venue refused, leaving the order live (a
    /// refused cancel usually means the order already went; the channel says which). Returns
    /// the `(symbol, order)` to republish.
    pub fn cancel_failed(&mut self, coi: i64) -> Option<(String, Order)> {
        let tracked = self.orders.get_mut(&coi)?;
        tracked.order.req = Status::None;
        Some((tracked.symbol.clone(), tracked.order.clone()))
    }

    /// Maps a position frame onto the registered symbol and returns `(symbol, qty, exch_ts)`
    /// to publish, `exch_ts` in **nanoseconds**. The position is the venue's stated signed size
    /// (§3.4), never inferred; a market this connector never registered is ignored (it is some
    /// other bot's).
    ///
    /// `exch_ts_us` is microseconds (the caller passes `Utc::now().timestamp_micros()`) and
    /// reaches the nanosecond wire field only through the checked bridge (T1). On the
    /// (year-2262) overflow it fails closed — the position is dropped rather than published with
    /// a wrapped timestamp (§1.1).
    pub fn apply_position(
        &self,
        position: &AccountPosition,
        exch_ts_us: Micros,
    ) -> Option<(String, f64, i64)> {
        let symbol = self.symbol_by_market.get(&position.market_index)?.clone();
        let exch_ts = match Nanos::from_micros(exch_ts_us) {
            Ok(ns) => ns.get(),
            Err(overflow) => {
                warn!(
                    %overflow,
                    market_index = position.market_index,
                    "A Lighter position timestamp does not fit in nanoseconds; dropping the \
                     position publish (§1.1)."
                );
                return None;
            }
        };
        Some((symbol, position.position, exch_ts))
    }

    /// Reconciles against the venue's open-order snapshot on (re)connect.
    ///
    /// Expires every confirmed, active order this connector believes is resting that the
    /// snapshot does not list (§3.4, §5.10). An order not yet confirmed is left alone — its
    /// fate is the slot task's deadline to decide, not a snapshot's silence (§4.11). Returns
    /// the `(symbol, order)` pairs to publish as `Status::Expired`.
    pub fn reconcile(&mut self, open: &[AccountOrder]) -> Vec<(String, Order)> {
        let open_cois: HashSet<i64> = open.iter().map(|o| o.client_order_index).collect();
        let vanished: Vec<i64> = self
            .orders
            .iter()
            .filter_map(|(coi, tracked)| {
                (tracked.confirmed && tracked.order.active() && !open_cois.contains(coi))
                    .then_some(*coi)
            })
            .collect();
        vanished
            .into_iter()
            .filter_map(|coi| {
                warn!(
                    coi,
                    "Lighter does not list an order this connector believes is resting; \
                     expiring it. The lifecycle update was lost with the connection (§5.10)."
                );
                self.submit_failed(coi)
            })
            .collect()
    }
}

/// Maps a wire time-in-force onto the Lighter TIF code (§3.5). GTX (post-only) is the
/// ALO-equivalent a maker grid rests with; FOK has no Lighter equivalent.
fn tif_of(tif: TimeInForce) -> Result<u8, OrderError> {
    match tif {
        TimeInForce::GTX => Ok(TIF_POST_ONLY),
        TimeInForce::GTC => Ok(TIF_GOOD_TILL_TIME),
        TimeInForce::IOC => Ok(TIF_IMMEDIATE_OR_CANCEL),
        other => Err(OrderError::UnsupportedTif(other)),
    }
}

/// Classifies a Lighter order status string as a [`StatusVerdict`].
///
/// The `_ => Status::Unsupported` this replaces was the drop-on-unknown bug: an unrecognised
/// status became `Unsupported`, `!active()`, and [`OrderManager::apply_order_update`] freed the
/// order id — so the strategy re-submitted and stood a duplicate at the venue (`AGENTS.md`
/// §1.1, invariant S1). Returning a `StatusVerdict` forces the caller to keep an unrecognised
/// status's order tracked: there is no `Status` value it can be folded into that drops it.
fn map_status(status: &str) -> StatusVerdict {
    match status {
        "open" | "pending" | "in-progress" => StatusVerdict::Known(Status::New),
        "canceled" | "cancelled" => StatusVerdict::Known(Status::Canceled),
        "filled" => StatusVerdict::Known(Status::Filled),
        "partially-filled" | "partially_filled" | "partial" => {
            StatusVerdict::Known(Status::PartiallyFilled)
        }
        other => StatusVerdict::Unrecognised(other.to_string()),
    }
}

impl GetOrders for OrderManager {
    fn orders(&self, symbol: Option<String>) -> Vec<Order> {
        self.orders
            .values()
            .filter(|tracked| {
                symbol
                    .as_ref()
                    .map(|wanted| tracked.symbol == *wanted)
                    .unwrap_or(true)
                    && tracked.order.active()
            })
            .map(|tracked| tracked.order.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use hftbacktest::types::{OrdType, Order, Side, Status, TimeInForce};

    use super::{MAX_COI, OrderError, OrderManager, map_status};
    use crate::{
        connector::GetOrders,
        lighter::{
            private_msg::{AccountOrder, AccountPosition},
            rest::MarketInfo,
        },
        utils::Micros,
    };

    fn btc() -> MarketInfo {
        // BTC on Lighter: price_decimals 1, size_decimals 5 (58300.0 → 583000; 0.00100 → 100).
        MarketInfo {
            symbol: "BTC".to_string(),
            market_id: 1,
            price_decimals: 1,
            size_decimals: 5,
        }
    }

    fn bid(order_id: u64, price: f64, qty: f64) -> Order {
        // tick_size and lot_size are the venue's own here, as the bot is told to register.
        Order::new(
            order_id,
            (price / 0.1).round() as i64,
            0.1,
            qty,
            Side::Buy,
            OrdType::Limit,
            TimeInForce::GTX,
        )
    }

    /// A private-channel order body for one of ours, in the venue's own shape.
    fn account_order(coi: i64, order_index: i64, status: &str, txn_us: i64) -> AccountOrder {
        AccountOrder {
            market_index: 1,
            client_order_index: coi,
            order_index,
            is_ask: false,
            status: status.to_string(),
            price: 58300.0,
            initial_base_amount: 0.001,
            remaining_base_amount: if status == "open" { 0.001 } else { 0.0 },
            filled_base_amount: 0.0,
            transaction_time_us: Micros::new(txn_us),
        }
    }

    fn manager() -> OrderManager {
        let mut m = OrderManager::new(1);
        m.track_market(btc());
        m
    }

    /// **The allocator is monotonic, never 0, and stays inside `[1, 2^48-1]`** (§3.5). Two
    /// orders get two increasing indices; it fails closed at the ceiling rather than wrap.
    #[test]
    fn the_coi_allocator_is_monotonic_never_zero_and_bounded() {
        let mut m = manager();
        let (coi_a, _) = m.new_order("BTC", &btc(), &bid(1, 58300.0, 0.001)).unwrap();
        let (coi_b, _) = m.new_order("BTC", &btc(), &bid(2, 58200.0, 0.001)).unwrap();
        assert!(
            coi_a >= 1 && coi_b > coi_a,
            "monotonic and never zero: {coi_a} {coi_b}"
        );

        // At the ceiling it refuses rather than hand out 0 or an out-of-range index.
        let mut exhausted = OrderManager::new(MAX_COI);
        exhausted.track_market(btc());
        assert!(
            exhausted
                .new_order("BTC", &btc(), &bid(9, 58300.0, 0.001))
                .is_ok()
        );
        assert!(matches!(
            exhausted.new_order("BTC", &btc(), &bid(10, 58200.0, 0.001)),
            Err(OrderError::CoiExhausted)
        ));
    }

    /// **`new_order` quantises onto the venue's integer grid from the catalog decimals** (§3.5)
    /// and maps the side and TIF. 58300.0 @ 1 price-decimal → 583000; 0.00100 @ 5
    /// size-decimals → 100; a bid is `is_ask = 0`; GTX (post-only) → TIF 2.
    #[test]
    fn new_order_builds_the_wire_form_on_the_venue_grid() {
        let mut m = manager();
        let (_, wire) = m.new_order("BTC", &btc(), &bid(1, 58300.0, 0.001)).unwrap();
        assert_eq!(wire.market_index, 1);
        assert_eq!(wire.price, 583000);
        assert_eq!(wire.base_amount, 100);
        assert_eq!(wire.is_ask, 0, "a buy is is_ask 0");
        assert_eq!(wire.time_in_force, 2, "GTX maps to PostOnly (§3.5)");
        assert_eq!(wire.order_type, 0, "Limit");
    }

    /// A second live order on the same `(symbol, order_id)` is refused (§3.5) — the bot's id
    /// space must stay unambiguous — and the refusal names it.
    #[test]
    fn a_second_live_order_on_the_same_id_is_refused() {
        let mut m = manager();
        m.new_order("BTC", &btc(), &bid(7, 58300.0, 0.001)).unwrap();
        assert!(matches!(
            m.new_order("BTC", &btc(), &bid(7, 58200.0, 0.001)),
            Err(OrderError::OrderIdLive { order_id: 7, .. })
        ));
    }

    /// **Acceptance is the channel, not the HTTP 200** (§4.11): the order goes live only when
    /// `account_all_orders` lists it open, and then it carries the venue's `order_index`.
    #[test]
    fn an_order_goes_live_only_when_the_channel_lists_it() {
        let mut m = manager();
        let (coi, _) = m.new_order("BTC", &btc(), &bid(1, 58300.0, 0.001)).unwrap();
        m.mark_requested(coi, Status::New);
        // Not confirmed until the channel lists it — the slot task's deadline reads this.
        assert_eq!(m.is_confirmed(coi), Some(false));

        let (symbol, order) = m
            .apply_order_update(&account_order(
                coi,
                844424914280027,
                "open",
                1785431774184833,
            ))
            .expect("our order, opened");
        assert_eq!(symbol, "BTC");
        assert_eq!(order.status, Status::New);
        assert_eq!(
            order.req,
            Status::None,
            "the venue answered; the request is done"
        );
        assert_eq!(m.is_confirmed(coi), Some(true), "the channel confirmed it");
        // The order is now cancellable — the order_index has been learned.
        assert!(m.cancel_order("BTC", 1).is_ok());
    }

    /// **State transitions are ordered by `transaction_time` and only it** (§4.12). A frame
    /// with an older `transaction_time` than the last applied is dropped, never rewinding a
    /// canceled order back to open.
    #[test]
    fn a_stale_transaction_time_does_not_rewind_the_order() {
        let mut m = manager();
        let (coi, _) = m.new_order("BTC", &btc(), &bid(1, 58300.0, 0.001)).unwrap();
        m.apply_order_update(&account_order(coi, 5, "open", 2_000));
        // A cancel at a later transaction_time takes it out...
        let (_, canceled) = m
            .apply_order_update(&account_order(coi, 5, "canceled", 3_000))
            .expect("cancel applied");
        assert_eq!(canceled.status, Status::Canceled);
        // ...and an older "open" frame arriving afterwards must NOT bring it back.
        assert!(
            m.apply_order_update(&account_order(coi, 5, "open", 1_000))
                .is_none(),
            "a stale open frame must not resurrect a canceled order"
        );
        assert_eq!(m.tracked(), 0);
    }

    /// A terminal update takes the order out, and repeating it does not bring it back — the
    /// order_id is freed and reusable (§3.5 reuse-after-cancel).
    #[test]
    fn a_terminal_update_removes_the_order_and_does_not_resurrect() {
        let mut m = manager();
        let (coi, _) = m.new_order("BTC", &btc(), &bid(1, 58300.0, 0.001)).unwrap();
        m.apply_order_update(&account_order(coi, 5, "open", 1_000));
        m.apply_order_update(&account_order(coi, 5, "canceled", 2_000));
        assert_eq!(m.tracked(), 0);
        // A repeat of the terminal frame is a no-op, not a resurrection.
        assert!(
            m.apply_order_update(&account_order(coi, 5, "canceled", 2_000))
                .is_none()
        );
        assert_eq!(m.tracked(), 0);
        // The order_id can now be reused for a fresh order.
        assert!(m.new_order("BTC", &btc(), &bid(1, 58200.0, 0.001)).is_ok());
    }

    /// **An execution claims maker liquidity only if the venue reported it** (C2). Lighter's
    /// `account_all_orders` frame states how much of the order is filled, and nothing at all
    /// about which side of the book provided the liquidity — there is no per-execution maker
    /// field on it. An unmeasured source must therefore claim nothing: the flag the bot reads
    /// stays unset, which under-claims rather than over-claims (`AGENTS.md` §1.1).
    ///
    /// This replaces an unconditional `maker = true` on every execution, which made the flag a
    /// constant that fee and fill-quality attribution read as a measurement.
    #[test]
    fn an_execution_claims_no_liquidity_the_channel_does_not_report() {
        let mut m = manager();
        let (coi, _) = m.new_order("BTC", &btc(), &bid(1, 58300.0, 0.001)).unwrap();
        m.apply_order_update(&account_order(coi, 5, "open", 1_000));

        let mut filled = account_order(coi, 5, "filled", 2_000);
        filled.filled_base_amount = 0.001;
        filled.remaining_base_amount = 0.0;
        let (_, order) = m
            .apply_order_update(&filled)
            .expect("our order, fully executed");

        assert_eq!(order.exec_qty, 0.001, "the execution itself is reported");
        assert!(
            !order.maker,
            "Lighter reports no per-execution liquidity, so the flag must claim none"
        );
    }

    /// **An unrecognised status must not drop the order** (S1). A venue status this connector
    /// does not map is not a licence to free the order id: dropping a possibly-live order lets
    /// the strategy re-submit and stand a duplicate at the venue (`AGENTS.md` §1.1). The order
    /// stays tracked with its last known status, and reconnect reconciliation resolves it if it
    /// is truly gone. This is the pin a re-added `_ => Status::Unsupported`-then-drop fails.
    #[test]
    fn an_unrecognised_status_keeps_the_order_and_does_not_drop_it() {
        let mut m = manager();
        let (coi, _) = m.new_order("BTC", &btc(), &bid(1, 58300.0, 0.001)).unwrap();
        m.mark_requested(coi, Status::New);
        m.apply_order_update(&account_order(coi, 5, "open", 1_000));
        assert_eq!(m.tracked(), 1);

        // A status string this connector does not recognise, on a fresh (non-stale) frame.
        let applied = m.apply_order_update(&account_order(coi, 5, "some-new-venue-status", 2_000));
        assert!(
            applied.is_none(),
            "an unrecognised status publishes nothing"
        );
        assert_eq!(
            m.tracked(),
            1,
            "an unrecognised status must not free the order id"
        );
        // Still resting and still cancellable from the bot's point of view.
        assert_eq!(m.orders(None).len(), 1);
        // A later recognised terminal update still resolves it normally.
        let (_, done) = m
            .apply_order_update(&account_order(coi, 5, "canceled", 3_000))
            .expect("a recognised terminal update after the unknown one still applies");
        assert_eq!(done.status, Status::Canceled);
        assert_eq!(m.tracked(), 0);
    }

    /// A frame for a COI this connector never minted — a human's order, or a previous run's —
    /// is ignored, not an error and above all not published as one of ours (§4.11 / HL model).
    #[test]
    fn an_update_for_a_foreign_coi_is_ignored() {
        let mut m = manager();
        assert!(
            m.apply_order_update(&account_order(999_999, 5, "open", 1_000))
                .is_none()
        );
        assert_eq!(m.tracked(), 0);
    }

    /// Cancelling an unknown order errors rather than guessing, and cancelling one that is not
    /// yet confirmed errors too — a cancel needs the venue's `order_index`.
    #[test]
    fn cancelling_an_unknown_or_unconfirmed_order_errors() {
        let mut m = manager();
        assert!(matches!(
            m.cancel_order("BTC", 42),
            Err(OrderError::OrderNotFound { .. })
        ));
        let (_coi, _) = m
            .new_order("BTC", &btc(), &bid(42, 58300.0, 0.001))
            .unwrap();
        // Placed but not yet confirmed on the channel: no order_index to address a cancel.
        assert!(matches!(
            m.cancel_order("BTC", 42),
            Err(OrderError::NotConfirmed { .. })
        ));
    }

    /// **A failed submit expires the order back to the bot** (§4.11 — the deadline/verdict
    /// path). Without it the bot holds a `Status::New` order that exists nowhere and burns the
    /// id for the life of the process.
    #[test]
    fn a_failed_submit_expires_the_order_and_frees_the_id() {
        let mut m = manager();
        let (coi, _) = m.new_order("BTC", &btc(), &bid(1, 58300.0, 0.001)).unwrap();
        let (symbol, order) = m.submit_failed(coi).expect("expired back");
        assert_eq!(symbol, "BTC");
        assert_eq!(order.status, Status::Expired);
        assert_eq!(order.req, Status::None);
        assert_eq!(m.tracked(), 0);
        // The id is free again.
        assert!(m.new_order("BTC", &btc(), &bid(1, 58200.0, 0.001)).is_ok());
    }

    /// **The finding, at the manager level: an order whose `sendTx` was ambiguous must stay
    /// adoptable — the connector must NOT `submit_failed` it before it knows the fate** (§3.3,
    /// §1.1). A `new_order` + `mark_requested` order is tracked-but-unconfirmed; if the
    /// ambiguous tx actually landed, the venue lists it and [`apply_order_update`] adopts it by
    /// COI and publishes it live. Had the ambiguous path expired it (the bug), the COI would be
    /// gone from the map and this adoption would return `None` — a silent one-sided resting
    /// order. This asserts the COI survives and is adoptable.
    #[test]
    fn an_ambiguous_orders_coi_stays_tracked_and_is_adoptable_when_it_landed() {
        let mut m = manager();
        let (coi, _) = m.new_order("BTC", &btc(), &bid(1, 58300.0, 0.001)).unwrap();
        m.mark_requested(coi, Status::New);
        // Unconfirmed and in flight — this is the state after an ambiguous send that did NOT
        // expire the order. The COI must still be tracked.
        assert_eq!(
            m.is_confirmed(coi),
            Some(false),
            "still tracked, unconfirmed"
        );

        // The ambiguous tx in fact landed: the venue's active-order list (or the private
        // channel) carries our COI. Adoption keys on the retained COI and publishes it live.
        let (symbol, order) = m
            .apply_order_update(&account_order(coi, 844424914280027, "open", 1_000))
            .expect("a still-tracked COI is adoptable — never orphaned");
        assert_eq!(symbol, "BTC");
        assert_eq!(order.order_id, 1);
        assert_eq!(order.status, Status::New);
        assert_eq!(m.is_confirmed(coi), Some(true), "adopted and confirmed");
        // Contrast: submit_failed would have removed the COI, and then this adoption returns
        // None — the orphan the finding is about.
        let mut orphaned = manager();
        let (coi2, _) = orphaned
            .new_order("BTC", &btc(), &bid(1, 58300.0, 0.001))
            .unwrap();
        orphaned.submit_failed(coi2);
        assert!(
            orphaned
                .apply_order_update(&account_order(coi2, 844424914280027, "open", 1_000))
                .is_none(),
            "an expired-and-removed COI can never be re-adopted — the silent-resting-order bug"
        );
    }

    /// **The position comes from `account_all_positions`, signed** (§3.4), and is mapped onto
    /// the registered symbol. A market this connector never registered is ignored.
    #[test]
    fn the_position_is_the_venues_signed_size_on_the_registered_symbol() {
        let m = manager();
        let short = AccountPosition {
            market_index: 1,
            symbol: "BTC".to_string(),
            position: -1.5,
            open_order_count: 0,
        };
        let (symbol, qty, exch_ts) = m.apply_position(&short, Micros::new(4_242)).expect("ours");
        assert_eq!(symbol, "BTC");
        assert_eq!(qty, -1.5);
        // The µs `now` is converted to nanoseconds (T1): 4_242 µs → 4_242_000 ns. Before the
        // fix this returned the raw 4_242.
        assert_eq!(exch_ts, 4_242_000);

        // An unregistered market is some other bot's; ignored.
        let foreign = AccountPosition {
            market_index: 77,
            symbol: "DOGE".to_string(),
            position: 10.0,
            open_order_count: 0,
        };
        assert!(m.apply_position(&foreign, Micros::new(1)).is_none());
    }

    /// **Reconciliation expires an order the venue no longer lists** (§3.4, §5.10) — the
    /// reconnect case where a lifecycle update was lost — but leaves an unconfirmed order
    /// alone, because a snapshot's silence says nothing about an order still in flight (§4.11).
    #[test]
    fn reconciliation_expires_vanished_orders_but_spares_unconfirmed_ones() {
        let mut m = manager();
        // A confirmed, resting order.
        let (coi_live, _) = m.new_order("BTC", &btc(), &bid(1, 58300.0, 0.001)).unwrap();
        m.apply_order_update(&account_order(coi_live, 5, "open", 1_000));
        // A just-submitted order the venue has not confirmed yet.
        let (_coi_flight, _) = m.new_order("BTC", &btc(), &bid(2, 58200.0, 0.001)).unwrap();

        // The venue's snapshot lists neither.
        let expired = m.reconcile(&[]);
        assert_eq!(expired.len(), 1, "only the confirmed-then-vanished one");
        assert_eq!(expired[0].0, "BTC");
        assert_eq!(expired[0].1.order_id, 1);
        assert_eq!(expired[0].1.status, Status::Expired);
        // The in-flight order is untouched.
        assert_eq!(m.tracked(), 1);
    }

    /// A confirmed order that the snapshot still lists is left resting — reconciliation only
    /// expires what is gone.
    #[test]
    fn reconciliation_leaves_an_order_the_venue_still_lists() {
        let mut m = manager();
        let (coi, _) = m.new_order("BTC", &btc(), &bid(1, 58300.0, 0.001)).unwrap();
        m.apply_order_update(&account_order(coi, 5, "open", 1_000));
        let expired = m.reconcile(&[account_order(coi, 5, "open", 1_000)]);
        assert!(expired.is_empty());
        assert_eq!(m.tracked(), 1);
    }

    /// `GetOrders` reports only active orders, filtered by symbol — the registration snapshot
    /// the bot receives.
    #[test]
    fn get_orders_reports_only_active_orders() {
        let mut m = manager();
        let (coi, _) = m.new_order("BTC", &btc(), &bid(1, 58300.0, 0.001)).unwrap();
        m.apply_order_update(&account_order(coi, 5, "open", 1_000));
        assert_eq!(m.orders(Some("BTC".to_string())).len(), 1);
        assert_eq!(m.orders(Some("ETH".to_string())).len(), 0);
        assert_eq!(m.orders(None).len(), 1);
    }

    /// **An order update's `exch_timestamp` is the venue microsecond `transaction_time`
    /// converted to nanoseconds** — the money-path unit (§4.4, T1). Before the fix it stored the
    /// raw microseconds, 1000× too small; the `Micros`-typed field now makes the raw assignment
    /// a compile error and the checked [`crate::utils::Nanos::from_micros`] the only bridge.
    #[test]
    fn an_order_updates_exch_timestamp_is_the_transaction_time_in_nanoseconds() {
        let mut m = manager();
        let (coi, _) = m.new_order("BTC", &btc(), &bid(1, 58300.0, 0.001)).unwrap();
        let txn_us = 1_785_431_774_184_833;
        let (_, order) = m
            .apply_order_update(&account_order(coi, 5, "open", txn_us))
            .expect("our order, opened");
        assert_eq!(order.exch_timestamp, txn_us * 1_000);
    }

    #[test]
    fn status_mapping_covers_the_observed_vocabulary() {
        use crate::utils::StatusVerdict;

        assert_eq!(map_status("open"), StatusVerdict::Known(Status::New));
        assert_eq!(
            map_status("canceled"),
            StatusVerdict::Known(Status::Canceled)
        );
        assert_eq!(map_status("filled"), StatusVerdict::Known(Status::Filled));
        assert_eq!(
            map_status("partially-filled"),
            StatusVerdict::Known(Status::PartiallyFilled)
        );
        // A status this connector does not recognise is Unrecognised, carrying its raw text —
        // never `Status::Unsupported`, which `apply_order_update` would drop (invariant S1).
        assert_eq!(
            map_status("what"),
            StatusVerdict::Unrecognised("what".to_string())
        );
    }
}
