use std::{
    any::Any,
    collections::HashMap,
    fmt::{Debug, Formatter},
};

use anyhow::Error;
use bincode::{
    BorrowDecode,
    Decode,
    Encode,
    de::{BorrowDecoder, Decoder},
    enc::Encoder,
    error::{DecodeError, EncodeError},
};
use dyn_clone::DynClone;
use hftbacktest_derive::NpyDTyped;
use thiserror::Error;

use crate::{backtest::data::POD, depth::MarketDepth};

#[derive(Clone, Debug, Decode, Encode)]
pub enum Value {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    List(Vec<Value>),
    Map(HashMap<String, Value>),
    Empty,
}

impl Value {
    pub fn get_str(&self) -> Option<&str> {
        if let Value::String(val) = self {
            Some(val.as_str())
        } else {
            None
        }
    }

    pub fn get_int(&self) -> Option<i64> {
        if let Value::Int(val) = self {
            Some(*val)
        } else {
            None
        }
    }

    pub fn get_float(&self) -> Option<f64> {
        if let Value::Float(val) = self {
            Some(*val)
        } else {
            None
        }
    }

    pub fn get_bool(&self) -> Option<bool> {
        if let Value::Bool(val) = self {
            Some(*val)
        } else {
            None
        }
    }

    pub fn get_list(&self) -> Option<&Vec<Value>> {
        if let Value::List(val) = self {
            Some(val)
        } else {
            None
        }
    }

    pub fn get_map(&self) -> Option<&HashMap<String, Value>> {
        if let Value::Map(val) = self {
            Some(val)
        } else {
            None
        }
    }
}

impl From<anyhow::Error> for Value {
    fn from(value: Error) -> Self {
        // todo!: improve this to deliver detailed error information.
        Value::String(value.to_string())
    }
}

/// Error conveyed through [`LiveEvent`].
#[derive(Clone, Debug, Decode, Encode)]
pub struct LiveError {
    pub kind: ErrorKind,
    pub value: Value,
}

impl LiveError {
    /// Constructs an instance of `LiveError`.
    pub fn new(kind: ErrorKind) -> LiveError {
        Self {
            kind,
            value: Value::Empty,
        }
    }

    /// Constructs an instance of `LiveError` with a value that contains detailed error information.
    pub fn with(kind: ErrorKind, value: Value) -> LiveError {
        Self { kind, value }
    }

    /// Returns a reference to the value that contains detailed error information.
    pub fn value(&self) -> &Value {
        &self.value
    }
}

/// Error type assigned to [`LiveError`].
#[derive(Clone, Copy, Eq, PartialEq, Debug, Decode, Encode)]
pub enum ErrorKind {
    ConnectionInterrupted,
    CriticalConnectionError,
    OrderError,
    Custom(i64),
}

/// Events occurring in a live bot sent by a [`Connector`](`crate::connector::Connector`).
///
/// **Variants may only be appended.** Like [`LiveRequest`], this is `bincode`-encoded with no
/// version field, so the discriminant is a varint of the *positional* index, not the `#[repr]`
/// literal. Inserting a variant anywhere but the end silently renumbers every one after it, and a
/// bot would decode one connector event as another. The enums this carries inside [`Order`] and
/// [`LiveError`] (`Status`, `Side`, `OrdType`, `TimeInForce`, `ErrorKind`, `Value`) are wire-
/// reachable for the same reason and under the same rule — their `#[repr(..)]` values are decoys
/// (`Status::Unsupported` rides as `8`, not `255`). `AGENTS.md` §2 names this rule; the
/// `*_variants_are_append_only_on_the_wire` tests enforce it byte-for-byte.
#[derive(Clone, Debug, Decode, Encode)]
pub enum LiveEvent {
    BatchStart,
    BatchEnd,
    Feed {
        symbol: String,
        event: Event,
    },
    Order {
        symbol: String,
        order: Order,
    },
    Position {
        symbol: String,
        qty: f64,
        exch_ts: i64,
    },
    Error(LiveError),
    /// Marks the end of the initial state snapshot for an instrument.
    ///
    /// Emitted by the connector after `RegisterInstrument` and after all existing `Order`,
    /// `Position`, and market-depth snapshot events for the asset have been sent. Receipt of this
    /// event for `symbol` means that, at `snapshot_time_ns`, the bot's view of orders and position
    /// for that asset mirrors the connector's cached view of the exchange. Callers may start
    /// making submit/cancel decisions based on `position(asset_no)` / `orders(asset_no)` after
    /// this event.
    ///
    /// Note: the guarantee is only as strong as the connector's own state-tracking. If the
    /// connector has not pulled initial state from the exchange (e.g. via REST) since its own
    /// start, the snapshot reflects whatever the connector has observed on the private stream —
    /// not a fresh exchange pull.
    SnapshotComplete {
        symbol: String,
        snapshot_time_ns: i64,
    },
}

/// Indicates a buy, with specific meaning that can vary depending on the situation. For example,
/// when combined with a depth event, it means a bid-side event, while when combined with a trade
/// event, it means that the trade initiator is a buyer.
pub const BUY_EVENT: u64 = 1 << 29;

/// Indicates a sell, with specific meaning that can vary depending on the situation. For example,
/// when combined with a depth event, it means an ask-side event, while when combined with a trade
/// event, it means that the trade initiator is a seller.
pub const SELL_EVENT: u64 = 1 << 28;

/// Indicates that the market depth is changed.
pub const DEPTH_EVENT: u64 = 1;

/// Indicates that a trade occurs in the market.
pub const TRADE_EVENT: u64 = 2;

/// Indicates that the market depth is cleared.
pub const DEPTH_CLEAR_EVENT: u64 = 3;

/// Indicates that the market depth snapshot is received.
pub const DEPTH_SNAPSHOT_EVENT: u64 = 4;

/// Indicates that the best bid and best ask update event is received.
pub const DEPTH_BBO_EVENT: u64 = 5;

/// Indicates that an order has been added to the order book.
pub const ADD_ORDER_EVENT: u64 = 10;

/// Indicates that an order in the order book has been canceled.
pub const CANCEL_ORDER_EVENT: u64 = 11;

/// Indicates that an order in the order book has been modified.
pub const MODIFY_ORDER_EVENT: u64 = 12;

/// Indicates that an order in the order book has been filled.
pub const FILL_EVENT: u64 = 13;

/// Indicates that it is a valid event to be handled by the exchange processor at the exchange
/// timestamp.
pub const EXCH_EVENT: u64 = 1 << 31;

/// Indicates that it is a valid event to be handled by the local processor at the local timestamp.
pub const LOCAL_EVENT: u64 = 1 << 30;

/// Represents a combination of [`DEPTH_CLEAR_EVENT`], and [`LOCAL_EVENT`].
pub const LOCAL_DEPTH_CLEAR_EVENT: u64 = DEPTH_CLEAR_EVENT | LOCAL_EVENT;

/// Represents a combination of [`DEPTH_CLEAR_EVENT`], and [`EXCH_EVENT`].
pub const EXCH_DEPTH_CLEAR_EVENT: u64 = DEPTH_CLEAR_EVENT | EXCH_EVENT;

/// Represents a combination of a [`DEPTH_EVENT`], [`BUY_EVENT`], and [`LOCAL_EVENT`].
pub const LOCAL_BID_DEPTH_EVENT: u64 = DEPTH_EVENT | BUY_EVENT | LOCAL_EVENT;

/// Represents a combination of [`DEPTH_EVENT`], [`SELL_EVENT`], and [`LOCAL_EVENT`].
pub const LOCAL_ASK_DEPTH_EVENT: u64 = DEPTH_EVENT | SELL_EVENT | LOCAL_EVENT;

/// Represents a combination of [`DEPTH_CLEAR_EVENT`], [`BUY_EVENT`], and [`LOCAL_EVENT`].
pub const LOCAL_BID_DEPTH_CLEAR_EVENT: u64 = DEPTH_CLEAR_EVENT | BUY_EVENT | LOCAL_EVENT;

/// Represents a combination of [`DEPTH_CLEAR_EVENT`], [`SELL_EVENT`], and [`LOCAL_EVENT`].
pub const LOCAL_ASK_DEPTH_CLEAR_EVENT: u64 = DEPTH_CLEAR_EVENT | SELL_EVENT | LOCAL_EVENT;

/// Represents a combination of [`DEPTH_SNAPSHOT_EVENT`], [`BUY_EVENT`], and [`LOCAL_EVENT`].
pub const LOCAL_BID_DEPTH_SNAPSHOT_EVENT: u64 = DEPTH_SNAPSHOT_EVENT | BUY_EVENT | LOCAL_EVENT;

/// Represents a combination of [`DEPTH_SNAPSHOT_EVENT`], [`SELL_EVENT`], and [`LOCAL_EVENT`].
pub const LOCAL_ASK_DEPTH_SNAPSHOT_EVENT: u64 = DEPTH_SNAPSHOT_EVENT | SELL_EVENT | LOCAL_EVENT;

/// Represents a combination of [`DEPTH_BBO_EVENT`], [`BUY_EVENT`], and [`LOCAL_EVENT`].
pub const LOCAL_BID_DEPTH_BBO_EVENT: u64 = DEPTH_BBO_EVENT | BUY_EVENT | LOCAL_EVENT;

/// Represents a combination of [`DEPTH_BBO_EVENT`], [`SELL_EVENT`], and [`LOCAL_EVENT`].
pub const LOCAL_ASK_DEPTH_BBO_EVENT: u64 = DEPTH_BBO_EVENT | SELL_EVENT | LOCAL_EVENT;

/// Represents a combination of [`TRADE_EVENT`], and [`LOCAL_EVENT`].
pub const LOCAL_TRADE_EVENT: u64 = TRADE_EVENT | LOCAL_EVENT;

/// Represents a combination of [`LOCAL_TRADE_EVENT`] and [`BUY_EVENT`].
pub const LOCAL_BUY_TRADE_EVENT: u64 = LOCAL_TRADE_EVENT | BUY_EVENT;

/// Represents a combination of [`LOCAL_TRADE_EVENT`] and [`SELL_EVENT`].
pub const LOCAL_SELL_TRADE_EVENT: u64 = LOCAL_TRADE_EVENT | SELL_EVENT;

/// Represents a combination of [`DEPTH_EVENT`], [`BUY_EVENT`], and [`EXCH_EVENT`].
pub const EXCH_BID_DEPTH_EVENT: u64 = DEPTH_EVENT | BUY_EVENT | EXCH_EVENT;

/// Represents a combination of [`DEPTH_EVENT`], [`SELL_EVENT`], and [`EXCH_EVENT`].
pub const EXCH_ASK_DEPTH_EVENT: u64 = DEPTH_EVENT | SELL_EVENT | EXCH_EVENT;

/// Represents a combination of [`DEPTH_CLEAR_EVENT`], [`BUY_EVENT`], and [`EXCH_EVENT`].
pub const EXCH_BID_DEPTH_CLEAR_EVENT: u64 = DEPTH_CLEAR_EVENT | BUY_EVENT | EXCH_EVENT;

/// Represents a combination of [`DEPTH_CLEAR_EVENT`], [`SELL_EVENT`], and [`EXCH_EVENT`].
pub const EXCH_ASK_DEPTH_CLEAR_EVENT: u64 = DEPTH_CLEAR_EVENT | SELL_EVENT | EXCH_EVENT;

/// Represents a combination of [`DEPTH_SNAPSHOT_EVENT`], [`BUY_EVENT`], and [`EXCH_EVENT`].
pub const EXCH_BID_DEPTH_SNAPSHOT_EVENT: u64 = DEPTH_SNAPSHOT_EVENT | BUY_EVENT | EXCH_EVENT;

/// Represents a combination of [`DEPTH_SNAPSHOT_EVENT`], [`SELL_EVENT`], and [`EXCH_EVENT`].
pub const EXCH_ASK_DEPTH_SNAPSHOT_EVENT: u64 = DEPTH_SNAPSHOT_EVENT | SELL_EVENT | EXCH_EVENT;

/// Represents a combination of [`DEPTH_BBO_EVENT`], [`BUY_EVENT`], and [`EXCH_EVENT`].
pub const EXCH_BID_DEPTH_BBO_EVENT: u64 = DEPTH_BBO_EVENT | BUY_EVENT | EXCH_EVENT;

/// Represents a combination of [`DEPTH_BBO_EVENT`], [`SELL_EVENT`], and [`EXCH_EVENT`].
pub const EXCH_ASK_DEPTH_BBO_EVENT: u64 = DEPTH_BBO_EVENT | SELL_EVENT | EXCH_EVENT;

/// Represents a combination of [`TRADE_EVENT`], and [`EXCH_EVENT`].
pub const EXCH_TRADE_EVENT: u64 = TRADE_EVENT | EXCH_EVENT;

/// Represents a combination of [`EXCH_TRADE_EVENT`] and [`BUY_EVENT`].
pub const EXCH_BUY_TRADE_EVENT: u64 = EXCH_TRADE_EVENT | BUY_EVENT;

/// Represents a combination of [`EXCH_TRADE_EVENT`] and [`SELL_EVENT`].
pub const EXCH_SELL_TRADE_EVENT: u64 = EXCH_TRADE_EVENT | SELL_EVENT;

/// Represents a combination of [`LOCAL_EVENT`] and [`ADD_ORDER_EVENT`].
pub const LOCAL_ADD_ORDER_EVENT: u64 = LOCAL_EVENT | ADD_ORDER_EVENT;

/// Represents a combination of [`BUY_EVENT`] and [`LOCAL_ADD_ORDER_EVENT`].
pub const LOCAL_BID_ADD_ORDER_EVENT: u64 = BUY_EVENT | LOCAL_ADD_ORDER_EVENT;

/// Represents a combination of [`SELL_EVENT`] and [`LOCAL_ADD_ORDER_EVENT`].
pub const LOCAL_ASK_ADD_ORDER_EVENT: u64 = SELL_EVENT | LOCAL_ADD_ORDER_EVENT;

/// Represents a combination of [`LOCAL_EVENT`] and [`CANCEL_ORDER_EVENT`].
pub const LOCAL_CANCEL_ORDER_EVENT: u64 = LOCAL_EVENT | CANCEL_ORDER_EVENT;

/// Represents a combination of [`LOCAL_EVENT`] and [`MODIFY_ORDER_EVENT`].
pub const LOCAL_MODIFY_ORDER_EVENT: u64 = LOCAL_EVENT | MODIFY_ORDER_EVENT;

/// Represents a combination of [`LOCAL_EVENT`] and [`FILL_EVENT`].
pub const LOCAL_FILL_EVENT: u64 = LOCAL_EVENT | FILL_EVENT;

/// Represents a combination of [`EXCH_EVENT`] and [`ADD_ORDER_EVENT`].
pub const EXCH_ADD_ORDER_EVENT: u64 = EXCH_EVENT | ADD_ORDER_EVENT;

/// Represents a combination of [`BUY_EVENT`] and [`EXCH_ADD_ORDER_EVENT`].
pub const EXCH_BID_ADD_ORDER_EVENT: u64 = BUY_EVENT | EXCH_ADD_ORDER_EVENT;

/// Represents a combination of [`SELL_EVENT`] and [`EXCH_ADD_ORDER_EVENT`].
pub const EXCH_ASK_ADD_ORDER_EVENT: u64 = SELL_EVENT | EXCH_ADD_ORDER_EVENT;

/// Represents a combination of [`EXCH_EVENT`] and [`CANCEL_ORDER_EVENT`].
pub const EXCH_CANCEL_ORDER_EVENT: u64 = EXCH_EVENT | CANCEL_ORDER_EVENT;

/// Represents a combination of [`EXCH_EVENT`] and [`MODIFY_ORDER_EVENT`].
pub const EXCH_MODIFY_ORDER_EVENT: u64 = EXCH_EVENT | MODIFY_ORDER_EVENT;

/// Represents a combination of [`EXCH_EVENT`] and [`FILL_EVENT`].
pub const EXCH_FILL_EVENT: u64 = EXCH_EVENT | FILL_EVENT;

/// Indicates that one should continue until the end of the data.
pub const UNTIL_END_OF_DATA: i64 = i64::MAX;

pub type OrderId = u64;

#[derive(Copy, Clone, PartialEq, Debug)]
pub enum WaitOrderResponse {
    None,
    Any,
    Specified { asset_no: usize, order_id: OrderId },
}

/// Feed event data.
#[repr(C, align(64))]
#[derive(Clone, PartialEq, Debug, NpyDTyped, Decode, Encode)]
pub struct Event {
    /// Event flag
    pub ev: u64,
    /// Exchange timestamp, which is the time at which the event occurs on the exchange.
    pub exch_ts: i64,
    /// Local timestamp, which is the time at which the event is received by the local.
    pub local_ts: i64,
    /// Price
    pub px: f64,
    /// Quantity
    pub qty: f64,
    /// Order ID is only for the L3 Market-By-Order feed.
    pub order_id: u64,
    /// Reserved for an additional i64 value
    pub ival: i64,
    /// Reserved for an additional f64 value
    pub fval: f64,
}

unsafe impl POD for Event {}

impl Event {
    /// Checks if this `Event` corresponds to the given event.
    #[inline(always)]
    pub fn is(&self, event: u64) -> bool {
        if (self.ev & event) != event {
            false
        } else {
            let event_kind = event & 0xff;
            if event_kind == 0 {
                true
            } else {
                self.ev & 0xff == event_kind
            }
        }
    }
}

/// Represents a side, which can refer to either the side of an order or the initiator's side in a
/// trade event, with the meaning varying depending on the context.
#[derive(Clone, Copy, Eq, PartialEq, Debug, Decode, Encode)]
#[repr(i8)]
pub enum Side {
    /// In the market depth event, this indicates the bid side; in the market trade event, it
    /// indicates that the trade initiator is a buyer.
    Buy = 1,
    /// In the market depth event, this indicates the ask side; in the market trade event, it
    /// indicates that the trade initiator is a seller.
    Sell = -1,
    /// No side provided.
    None = 0,
    /// This occurs when the [`Connector`](`crate::connector::Connector`) receives a side value that
    /// does not have a corresponding enum value.
    Unsupported = 127,
}

impl AsRef<f64> for Side {
    fn as_ref(&self) -> &f64 {
        match self {
            Side::Buy => &1.0f64,
            Side::Sell => &-1.0f64,
            Side::None => panic!("Side::None"),
            Side::Unsupported => panic!("Side::Unsupported"),
        }
    }
}

impl AsRef<str> for Side {
    fn as_ref(&self) -> &'static str {
        match self {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
            Side::None => panic!("Side::None"),
            Side::Unsupported => panic!("Side::Unsupported"),
        }
    }
}

/// Order status
#[derive(Clone, Copy, Eq, PartialEq, Debug, Decode, Encode)]
#[repr(u8)]
pub enum Status {
    None = 0,
    New = 1,
    Expired = 2,
    Filled = 3,
    Canceled = 4,
    PartiallyFilled = 5,
    Rejected = 6,
    Replaced = 7,
    /// This occurs when the [`Connector`](`crate::connector::Connector`) receives an order status
    /// value that does not have a corresponding enum value.
    Unsupported = 255,
}

impl Status {
    /// Whether this is a **terminal** status: the order is final, no later update may
    /// resurrect it, and the `order_id` it held is freed.
    ///
    /// This is the single definition of "terminal". The live final-status guard
    /// ([`Bot`] impl in `live/bot.rs`), `Local::clear_inactive_orders`
    /// (`backtest/proc/local.rs` and its L3 mirror), and the connector order managers'
    /// removal paths all read it, so "terminal" cannot drift between sites the way the
    /// hand-written `status == A || status == B` chains it replaces did (`AGENTS.md` §1.5,
    /// correctness-by-construction §1.6).
    ///
    /// The match is **exhaustive with no wildcard on purpose**: a new [`Status`] variant does
    /// not compile until it is ruled terminal-or-not here, so it cannot silently fall through
    /// to "drop the order" at a removal site — the structural half of invariants S1/S2. A
    /// future non-terminal `Status::Uncertain` (deferred design item S3) would be forced to
    /// declare itself here, keeping an uncertain order tracked rather than dropped and
    /// re-submitted as a duplicate.
    ///
    /// Ruling on the two the previous hand-written set silently omitted: `Rejected` **is**
    /// terminal (a refused order never rested and takes no later update); `Replaced` is
    /// **not** (a replaced order keeps its id and continues resting — `Local::modify`).
    /// `None`/`Unsupported` are not terminal either — neither is a resolution that frees an
    /// order id.
    pub fn is_terminal(&self) -> bool {
        match self {
            Status::Filled | Status::Canceled | Status::Expired | Status::Rejected => true,
            Status::None
            | Status::New
            | Status::PartiallyFilled
            | Status::Replaced
            | Status::Unsupported => false,
        }
    }
}

/// Time In Force
#[derive(Clone, Copy, Eq, PartialEq, Debug, Decode, Encode)]
#[repr(u8)]
pub enum TimeInForce {
    /// Good 'Til Canceled
    GTC = 0,
    /// Post-only
    GTX = 1,
    /// Fill or Kill
    FOK = 2,
    /// Immediate or Cancel
    IOC = 3,
    /// This occurs when the [`Connector`](`crate::connector::Connector`) receives a time-in-force
    /// value that does not have a corresponding enum value.
    Unsupported = 255,
}

impl AsRef<str> for TimeInForce {
    fn as_ref(&self) -> &'static str {
        match self {
            TimeInForce::GTC => "GTC",
            TimeInForce::GTX => "GTX",
            TimeInForce::FOK => "FOK",
            TimeInForce::IOC => "IOC",
            TimeInForce::Unsupported => panic!("TimeInForce::Unsupported"),
        }
    }
}

/// Order type
#[derive(Clone, Copy, Eq, PartialEq, Debug, Decode, Encode)]
#[repr(u8)]
pub enum OrdType {
    Limit = 0,
    Market = 1,
    Unsupported = 255,
}

impl AsRef<str> for OrdType {
    fn as_ref(&self) -> &'static str {
        match self {
            OrdType::Limit => "LIMIT",
            OrdType::Market => "MARKET",
            OrdType::Unsupported => panic!("OrdType::Unsupported"),
        }
    }
}

/// Provides cloning of `Box<dyn Any>`, which is utilized in [Order] for the additional data used in
/// [`QueueModel`](`crate::backtest::models::QueueModel`).
///
/// **Usage:**
/// ```
/// impl AnyClone for QueuePos {
///     fn as_any(&self) -> &dyn Any {
///         self
///     }
///
///     fn as_any_mut(&mut self) -> &mut dyn Any {
///         self
///     }
/// }
/// ```
pub trait AnyClone: DynClone {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}
dyn_clone::clone_trait_object!(AnyClone);

impl AnyClone for () {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Order
#[derive(Clone)]
#[repr(C)]
pub struct Order {
    /// Order quantity
    pub qty: f64,
    /// The quantity of this order that has not yet been executed. It represents the remaining
    /// quantity that is still open or active in the market after any partial fills.
    pub leaves_qty: f64,
    /// Executed quantity, only available when this order is executed.
    ///
    /// This is the quantity executed by the single execution the order reports, not the
    /// cumulative quantity executed by the order: an order that fills in several parts reports
    /// each part separately, and the parts sum to the order's executed quantity. Backtesting
    /// relies on this to accumulate an order's fills without double-counting them.
    pub exec_qty: f64,
    /// Executed price in ticks (`executed_price / tick_size`), only available when this order is
    /// executed. It is the price of the execution [`Order::exec_qty`] reports.
    pub exec_price_tick: i64,
    /// Order price in ticks (`price / tick_size`).
    pub price_tick: i64,
    /// The tick size of the asset associated with this order.
    pub tick_size: f64,
    /// The time at which the exchange processes this order, ideally when the matching engine
    /// processes the order, will be set if the value is available.
    pub exch_timestamp: i64,
    /// The time at which the local receives this order or sent this order to the exchange.
    pub local_timestamp: i64,
    pub order_id: u64,
    /// Additional data used for [`QueueModel`](`crate::backtest::models::QueueModel`).
    /// This is only available in backtesting, and the type `Q` is set to `()` in a live bot.
    pub q: Box<dyn AnyClone + Send>,
    /// Whether the order is executed as a maker, only available when this order is executed.
    pub maker: bool,
    pub order_type: OrdType,
    /// Request status:
    ///   * [`Status::New`]: Request to open a new order.
    ///   * [`Status::Canceled`]: Request to cancel an opened order.
    pub req: Status,
    pub status: Status,
    pub side: Side,
    pub time_in_force: TimeInForce,
}

impl Order {
    /// Constructs an instance of `Order`.
    pub fn new(
        order_id: u64,
        price_tick: i64,
        tick_size: f64,
        qty: f64,
        side: Side,
        order_type: OrdType,
        time_in_force: TimeInForce,
    ) -> Self {
        Self {
            qty,
            leaves_qty: qty,
            price_tick,
            tick_size,
            side,
            time_in_force,
            exch_timestamp: 0,
            status: Status::None,
            local_timestamp: 0,
            req: Status::None,
            exec_price_tick: 0,
            exec_qty: 0.0,
            order_id,
            q: Box::new(()),
            maker: false,
            order_type,
        }
    }

    /// Returns the order price.
    pub fn price(&self) -> f64 {
        self.price_tick as f64 * self.tick_size
    }

    /// Returns the executed price, only available when this order is executed.
    pub fn exec_price(&self) -> f64 {
        self.exec_price_tick as f64 * self.tick_size
    }

    /// Returns whether this order is cancelable.
    pub fn cancellable(&self) -> bool {
        (self.status == Status::New || self.status == Status::PartiallyFilled)
            && self.req == Status::None
    }

    /// Returns whether this order is active in the market.
    pub fn active(&self) -> bool {
        self.status == Status::New || self.status == Status::PartiallyFilled
    }

    /// Returns whether this order has an ongoing request.
    pub fn pending(&self) -> bool {
        self.req != Status::None
    }

    /// Updates this order with the given order. This is used only by the processor in backtesting
    /// or by a bot in live trading.
    pub fn update(&mut self, order: &Order) {
        //assert!(order.exch_timestamp >= self.exch_timestamp);
        if order.exch_timestamp < self.exch_timestamp {
            println!(
                "Warning: Perhaps an inaccurate order response update occurs: an order previously \
                updated by a later exchange timestamp is updated by an earlier one. \
                This issue is primarily caused by incorrect or inconsistent timestamp ordering \
                across the files.\n \
                order={:?}, \
                response={:?}",
                &self, &order
            );
        }

        self.qty = order.qty;
        self.leaves_qty = order.leaves_qty;
        self.price_tick = order.price_tick;
        self.tick_size = order.tick_size;
        self.side = order.side;
        self.time_in_force = order.time_in_force;

        if order.exch_timestamp > 0 {
            self.exch_timestamp = order.exch_timestamp;
        }
        self.status = order.status;
        // if order.local_timestamp > 0 {
        //     self.local_timestamp = order.local_timestamp;
        // }
        self.req = order.req;
        self.exec_price_tick = order.exec_price_tick;
        self.exec_qty = order.exec_qty;
        self.order_id = order.order_id;
        self.q = order.q.clone();
        self.maker = order.maker;
        self.order_type = order.order_type;
    }
}

impl Debug for Order {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Order")
            .field("qty", &self.qty)
            .field("leaves_qty", &self.leaves_qty)
            .field("price_tick", &self.price_tick)
            .field("tick_size", &self.tick_size)
            .field("side", &self.side)
            .field("time_in_force", &self.time_in_force)
            .field("exch_timestamp", &self.exch_timestamp)
            .field("status", &self.status)
            .field("local_timestamp", &self.local_timestamp)
            .field("req", &self.req)
            .field("exec_price_tick", &self.exec_price_tick)
            .field("exec_qty", &self.exec_qty)
            .field("order_id", &self.order_id)
            .field("maker", &self.maker)
            .field("order_type", &self.order_type)
            .finish()
    }
}

impl<Context> Decode<Context> for Order {
    fn decode<D: Decoder>(decoder: &mut D) -> Result<Self, DecodeError> {
        Ok(Self {
            qty: Decode::decode(decoder)?,
            leaves_qty: Decode::decode(decoder)?,
            exec_qty: Decode::decode(decoder)?,
            exec_price_tick: Decode::decode(decoder)?,
            price_tick: Decode::decode(decoder)?,
            tick_size: Decode::decode(decoder)?,
            exch_timestamp: Decode::decode(decoder)?,
            local_timestamp: Decode::decode(decoder)?,
            order_id: Decode::decode(decoder)?,
            // In a live bot, q isn't used.
            q: Box::new(()),
            maker: Decode::decode(decoder)?,
            order_type: Decode::decode(decoder)?,
            req: Decode::decode(decoder)?,
            status: Decode::decode(decoder)?,
            side: Decode::decode(decoder)?,
            time_in_force: Decode::decode(decoder)?,
        })
    }
}

impl<'de, Context> BorrowDecode<'de, Context> for Order {
    fn borrow_decode<D: BorrowDecoder<'de>>(decoder: &mut D) -> Result<Self, DecodeError> {
        Ok(Self {
            qty: Decode::decode(decoder)?,
            leaves_qty: Decode::decode(decoder)?,
            exec_qty: Decode::decode(decoder)?,
            exec_price_tick: Decode::decode(decoder)?,
            price_tick: Decode::decode(decoder)?,
            tick_size: Decode::decode(decoder)?,
            exch_timestamp: Decode::decode(decoder)?,
            local_timestamp: Decode::decode(decoder)?,
            order_id: Decode::decode(decoder)?,
            // In a live bot, q isn't used.
            q: Box::new(()),
            maker: Decode::decode(decoder)?,
            order_type: Decode::decode(decoder)?,
            req: Decode::decode(decoder)?,
            status: Decode::decode(decoder)?,
            side: Decode::decode(decoder)?,
            time_in_force: Decode::decode(decoder)?,
        })
    }
}

impl Encode for Order {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        self.qty.encode(encoder)?;
        self.leaves_qty.encode(encoder)?;
        self.exec_qty.encode(encoder)?;
        self.exec_price_tick.encode(encoder)?;
        self.price_tick.encode(encoder)?;
        self.tick_size.encode(encoder)?;
        self.exch_timestamp.encode(encoder)?;
        self.local_timestamp.encode(encoder)?;
        self.order_id.encode(encoder)?;
        // In a live bot, q isn't used.
        self.maker.encode(encoder)?;
        self.order_type.encode(encoder)?;
        self.req.encode(encoder)?;
        self.status.encode(encoder)?;
        self.side.encode(encoder)?;
        self.time_in_force.encode(encoder)?;
        Ok(())
    }
}

/// An asynchronous request to [`Connector`](`crate::connector::Connector`).
///
/// **Variants may only be appended.** The encoding is `bincode` with no version field, so
/// the discriminant is positional; inserting a variant renumbers every later one and makes a
/// connector decode one request as another.
#[derive(Clone, Debug, Encode, Decode)]
pub enum LiveRequest {
    /// An order request, a tuple consisting of an asset number and an [`Order`].
    Order { symbol: String, order: Order },
    /// A request to add an instrument for trading.
    RegisterInstrument {
        symbol: String,
        tick_size: f64,
        lot_size: f64,
    },
    /// "This bot is still running its event loop."
    ///
    /// Sent by [`LiveBot`](crate::live::LiveBot) on a timer from inside `elapse`, and
    /// carries nothing: the only thing the connector needs is *which* bot sent it, and that
    /// rides in the IPC user header alongside every other request.
    ///
    /// It exists because **a bot's orders outlive the bot**. Measured on a live session
    /// (2026-07-28), a fill landed 3 s after the bot's `SIGINT` and moved the position;
    /// until the supervisor restarted it, the venue was hosting an unattended market maker.
    /// A connector that has seen a bot heartbeat and then stops hearing from it may cancel
    /// that bot's resting orders — see `connector/src/supervision.rs`.
    ///
    /// Being sent from `elapse` is the point: it reports that the strategy loop is *turning*,
    /// which a live process whose loop has wedged would not do, and which no transport-level
    /// liveness check could tell apart from health.
    Heartbeat,
}

/// Provides state values.
///
/// **Note:** In a live bot, currently only `position` value is delivered correctly, and other
/// values are invalid.
#[repr(C)]
#[derive(PartialEq, Clone, Debug, Default)]
pub struct StateValues {
    pub position: f64,
    /// Backtest only
    pub balance: f64,
    /// Backtest only
    pub fee: f64,
    // todo: currently, they are cumulative values, but they need to be values within the record
    //       interval.
    /// Backtest only
    pub num_trades: i64,
    /// Backtest only
    pub trading_volume: f64,
    /// Backtest only
    pub trading_value: f64,
}

/// Provides errors that can occur in builders.
#[derive(Error, Debug)]
pub enum BuildError {
    #[error("`{0}` is required")]
    BuilderIncomplete(&'static str),
    #[error("{0}")]
    InvalidArgument(&'static str),
    #[error("`{0}/{1}` already exists")]
    Duplicate(String, String),
    #[error("`{0}` is not found")]
    ConnectorNotFound(String),
    #[error("{0:?}")]
    Error(#[from] anyhow::Error),
}

/// Used to submit an order in a live bot.
#[derive(Decode, Encode)]
pub struct OrderRequest {
    pub order_id: u64,
    pub price: f64,
    pub qty: f64,
    pub side: Side,
    pub time_in_force: TimeInForce,
    pub order_type: OrdType,
}

/// Provides a bot interface for backtesting and live trading.
pub trait Bot<MD>
where
    MD: MarketDepth,
{
    type Error;

    /// In backtesting, this timestamp reflects the time at which the backtesting is conducted
    /// within the provided data. In a live bot, it's literally the current local timestamp.
    fn current_timestamp(&self) -> i64;

    /// Returns the number of assets.
    fn num_assets(&self) -> usize;

    /// Returns the position you currently hold.
    ///
    /// * `asset_no` - Asset number from which the position will be retrieved.
    fn position(&self, asset_no: usize) -> f64;

    /// Returns whether the initial state snapshot has been received for this asset.
    ///
    /// In live mode, this is `false` until the connector sends a
    /// [`LiveEvent::SnapshotComplete`] for the asset's symbol following registration. After that,
    /// [`position`](Self::position) and [`orders`](Self::orders) reflect the connector's cached
    /// exchange view at some timestamp `T` and may be used to make submit/cancel decisions.
    ///
    /// In backtest mode this always returns `true` (snapshot is trivial).
    ///
    /// * `asset_no` - Asset number to query.
    fn snapshot_ready(&self, asset_no: usize) -> bool;

    /// Returns whether at least one position update has been received for this asset.
    ///
    /// In live mode this latches on the first [`LiveEvent::Position`] and never clears. It is a
    /// strictly later signal than [`snapshot_ready`](Self::snapshot_ready), and that ordering is
    /// the whole point of exposing it: the Bybit connector publishes
    /// [`LiveEvent::SnapshotComplete`] synchronously with the registration round trip, while the
    /// venue-side `cancel_all_orders` + `get_position` it kicked off run on a spawned task. The
    /// position update is what closes that round trip, so a strategy that must not quote on top
    /// of a not-yet-swept book waits for this in addition to the marker
    /// (`docs/snapshot-complete-marker.md`, "Known gap").
    ///
    /// What it does **not** promise: that the sweep succeeded (a failed `cancel_all_orders` is
    /// logged and the position fetch runs anyway), that the reported position is still current,
    /// or that any particular venue publishes a position row for a flat account. Consumers must
    /// bound their wait rather than block on it forever.
    ///
    /// In backtest mode this always returns `true` — the position is authoritative from the
    /// first tick and there is no round trip to wait for.
    ///
    /// * `asset_no` - Asset number to query.
    fn position_observed(&self, asset_no: usize) -> bool;

    /// Returns the state's values such as balance, fee, and so on.
    fn state_values(&self, asset_no: usize) -> &StateValues;

    /// Returns the [`MarketDepth`].
    ///
    /// * `asset_no` - Asset number from which the market depth will be retrieved.
    fn depth(&self, asset_no: usize) -> &MD;

    /// Returns the last market trades.
    ///
    /// * `asset_no` - Asset number from which the last market trades will be retrieved.
    fn last_trades(&self, asset_no: usize) -> &[Event];

    /// Clears the last market trades from the buffer.
    ///
    /// * `asset_no` - Asset number at which this command will be executed. If `None`, all last
    ///   trades in any assets will be cleared.
    fn clear_last_trades(&mut self, asset_no: Option<usize>);

    /// Returns a hash map of order IDs and their corresponding [`Order`]s.
    ///
    /// * `asset_no` - Asset number from which orders will be retrieved.
    fn orders(&self, asset_no: usize) -> &HashMap<OrderId, Order>;

    /// Places a buy order.
    ///
    /// * `asset_no` - Asset number at which this command will be executed.
    /// * `order_id` - The unique order ID; there should not be any existing order with the same ID
    ///   on both local and exchange sides.
    /// * `price` - Order price.
    /// * `qty` - Quantity to buy.
    /// * `time_in_force` - Available [`TimeInForce`] options vary depending on the exchange model.
    ///   See to the exchange model for details.
    ///
    /// * `order_type` - Available [`OrdType`] options vary depending on the exchange model. See to
    ///   the exchange model for details.
    ///
    /// * `wait` - If true, wait until the order placement response is received.
    #[allow(clippy::too_many_arguments)]
    fn submit_buy_order(
        &mut self,
        asset_no: usize,
        order_id: OrderId,
        price: f64,
        qty: f64,
        time_in_force: TimeInForce,
        order_type: OrdType,
        wait: bool,
    ) -> Result<ElapseResult, Self::Error>;

    /// Places a sell order.
    ///
    /// * `asset_no` - Asset number at which this command will be executed.
    /// * `order_id` - The unique order ID; there should not be any existing order with the same ID
    ///   on both local and exchange sides.
    /// * `price` - Order price.
    /// * `qty` - Quantity to buy.
    /// * `time_in_force` - Available [`TimeInForce`] options vary depending on the exchange model.
    ///   See to the exchange model for details.
    ///
    /// * `order_type` - Available [`OrdType`] options vary depending on the exchange model. See to
    ///   the exchange model for details.
    ///
    /// * `wait` - If true, wait until the order placement response is received.
    #[allow(clippy::too_many_arguments)]
    fn submit_sell_order(
        &mut self,
        asset_no: usize,
        order_id: OrderId,
        price: f64,
        qty: f64,
        time_in_force: TimeInForce,
        order_type: OrdType,
        wait: bool,
    ) -> Result<ElapseResult, Self::Error>;

    /// Places an order.
    fn submit_order(
        &mut self,
        asset_no: usize,
        order: OrderRequest,
        wait: bool,
    ) -> Result<ElapseResult, Self::Error>;

    /// Modifies an open order.
    ///
    /// * `asset_no` - Asset number at which this command will be executed.
    /// * `order_id` - Order ID to modify.
    /// * `price` - Order price.
    /// * `qty` - Quantity to buy.
    /// * `wait` - If true, wait until the order modification response is received.
    fn modify(
        &mut self,
        asset_no: usize,
        order_id: OrderId,
        price: f64,
        qty: f64,
        wait: bool,
    ) -> Result<ElapseResult, Self::Error>;

    /// Cancels an open order.
    ///
    /// * `asset_no` - Asset number at which this command will be executed.
    /// * `order_id` - Order ID to cancel.
    /// * `wait` - If true, wait until the order placement response is received.
    fn cancel(
        &mut self,
        asset_no: usize,
        order_id: OrderId,
        wait: bool,
    ) -> Result<ElapseResult, Self::Error>;

    /// Clears inactive orders from the local orders whose status is neither [`Status::New`] nor
    /// [`Status::PartiallyFilled`].
    fn clear_inactive_orders(&mut self, asset_no: Option<usize>);

    /// Waits for the response of the order with the given order ID until timeout.
    fn wait_order_response(
        &mut self,
        asset_no: usize,
        order_id: OrderId,
        timeout: i64,
    ) -> Result<ElapseResult, Self::Error>;

    /// Wait until the next feed is received, or until timeout.
    fn wait_next_feed(
        &mut self,
        include_order_resp: bool,
        timeout: i64,
    ) -> Result<ElapseResult, Self::Error>;

    /// Elapses the specified duration.
    ///
    /// Args:
    /// * `duration` - Duration to elapse. Nanoseconds is the default unit. However, unit should be
    ///   the same as the data's timestamp unit.
    ///
    /// Returns:
    ///   `Ok(true)` if the method reaches the specified timestamp within the data. If the end of
    ///   the data is reached before the specified timestamp, it returns `Ok(false)`.
    fn elapse(&mut self, duration: i64) -> Result<ElapseResult, Self::Error>;

    /// Elapses time only in backtesting. In live mode, it is ignored.
    ///
    /// The [elapse()](Self::elapse()) method exclusively manages time during backtesting, meaning
    /// that factors such as computing time are not properly accounted for. So, this method can be
    /// utilized to simulate such processing times.
    ///
    /// Args:
    /// * `duration` - Duration to elapse. Nanoseconds is the default unit. However, unit should be
    ///   the same as the data's timestamp unit.
    ///
    /// Returns:
    ///   `Ok(true)` if the method reaches the specified timestamp within the data. If the end of
    ///   the data is reached before the specified timestamp, it returns `Ok(false)`.
    fn elapse_bt(&mut self, duration: i64) -> Result<ElapseResult, Self::Error>;

    /// Closes this backtester or bot.
    fn close(&mut self) -> Result<(), Self::Error>;

    /// Returns the last feed's exchange timestamp and local receipt timestamp.
    fn feed_latency(&self, asset_no: usize) -> Option<(i64, i64)>;

    /// Returns the last order's request timestamp, exchange timestamp, and response receipt
    /// timestamp.
    fn order_latency(&self, asset_no: usize) -> Option<(i64, i64, i64)>;
}

/// Provides bot statistics and [`StateValues`] recording features for backtesting result analysis
/// or live bot logging.
pub trait Recorder {
    type Error;

    /// Records the current [`StateValues`].
    fn record<MD, I>(&mut self, hbt: &I) -> Result<(), Self::Error>
    where
        I: Bot<MD>,
        MD: MarketDepth;
}

#[derive(Eq, PartialEq, Copy, Clone, Debug)]
pub enum ElapseResult {
    Ok,
    EndOfData,
    MarketFeed,
    OrderResponse,
}

#[cfg(test)]
mod tests {
    use crate::{
        prelude::LOCAL_EVENT,
        types::{
            BUY_EVENT,
            Event,
            LOCAL_BID_DEPTH_CLEAR_EVENT,
            LOCAL_BID_DEPTH_EVENT,
            LOCAL_BID_DEPTH_SNAPSHOT_EVENT,
            LOCAL_BUY_TRADE_EVENT,
        },
    };

    #[test]
    fn test_event_is() {
        let event = Event {
            ev: LOCAL_BID_DEPTH_CLEAR_EVENT | (1 << 20),
            exch_ts: 0,
            local_ts: 0,
            order_id: 0,
            px: 0.0,
            qty: 0.0,
            ival: 0,
            fval: 0.0,
        };

        assert!(!event.is(LOCAL_BID_DEPTH_EVENT));
        assert!(!event.is(LOCAL_BUY_TRADE_EVENT));
        assert!(event.is(LOCAL_BID_DEPTH_CLEAR_EVENT));

        let event = Event {
            ev: LOCAL_EVENT | BUY_EVENT | 0xff,
            exch_ts: 0,
            local_ts: 0,
            order_id: 0,
            px: 0.0,
            qty: 0.0,
            ival: 0,
            fval: 0.0,
        };

        assert!(!event.is(LOCAL_BID_DEPTH_EVENT));
        assert!(!event.is(LOCAL_BUY_TRADE_EVENT));
        assert!(!event.is(LOCAL_BID_DEPTH_CLEAR_EVENT));
        assert!(!event.is(LOCAL_BID_DEPTH_SNAPSHOT_EVENT));
        assert!(event.is(LOCAL_EVENT));
        assert!(event.is(BUY_EVENT));
    }

    /// **`LiveRequest` is bincode-encoded with no version field, so a variant may only ever
    /// be appended.** The discriminant is a varint written first, and inserting a variant
    /// anywhere but the end silently renumbers every one after it: a connector would decode
    /// a bot's `RegisterInstrument` as something else and act on it. `AGENTS.md` §2 names
    /// this rule; this test is what enforces it.
    ///
    /// The byte-level assertions are deliberate. A structural round-trip would pass just as
    /// happily with the variants reordered — only the first byte tells the truth.
    #[test]
    fn live_request_variants_are_append_only_on_the_wire() {
        use crate::types::{LiveRequest, OrdType, Order, Side, TimeInForce};

        let encode = |request: &LiveRequest| {
            bincode::encode_to_vec(request, bincode::config::standard()).unwrap()
        };

        let order = encode(&LiveRequest::Order {
            symbol: "BTC".to_string(),
            order: Order::new(
                1,
                100,
                0.01,
                1.0,
                Side::Buy,
                OrdType::Limit,
                TimeInForce::GTC,
            ),
        });
        let register = encode(&LiveRequest::RegisterInstrument {
            symbol: "BTC".to_string(),
            tick_size: 0.01,
            lot_size: 1.0,
        });
        let heartbeat = encode(&LiveRequest::Heartbeat);

        assert_eq!(order[0], 0, "Order must stay variant 0");
        assert_eq!(register[0], 1, "RegisterInstrument must stay variant 1");
        assert_eq!(
            heartbeat[0], 2,
            "Heartbeat must be appended, never inserted: an older connector decodes by \
             position and would read a renumbered variant as a different request"
        );

        // A heartbeat carries nothing but its own discriminant. It is sent on a timer by
        // every live bot, and the connector's only interest is *who* sent it — which rides
        // in the iceoryx user header, not the payload. One byte also cannot approach
        // `MAX_PAYLOAD_SIZE`.
        assert_eq!(heartbeat.len(), 1, "{heartbeat:?}");

        // And each one decodes back to itself, on this side of the wire.
        for bytes in [&order, &register, &heartbeat] {
            let (decoded, _): (LiveRequest, usize) =
                bincode::decode_from_slice(bytes, bincode::config::standard()).unwrap();
            assert_eq!(encode(&decoded), *bytes);
        }
    }

    /// The wire-ordinal is the *positional* index bincode writes as the leading varint, which
    /// for every variant here is a single byte equal to that index. It is emphatically **not**
    /// the `#[repr(..)]` discriminant: `Status::Unsupported = 255`, `Side::Sell = -1`, etc. are
    /// decoys, and a test that asserted the repr would pass while the wire silently disagreed.
    /// Encoding each enum directly is faithful to how it rides on the wire — `Order`'s manual
    /// `Encode` delegates to each field's own `Encode`, which writes exactly this leading byte.
    ///
    /// These mirror [`live_request_variants_are_append_only_on_the_wire`] for every other
    /// wire-reachable enum: [`LiveEvent`] itself, and the enums that ride inside [`Order`]
    /// (`Status`/`Side`/`OrdType`/`TimeInForce`) and [`LiveError`] (`ErrorKind`/`Value`).
    /// A silent variant reorder fails at least two assertions; a variant may only be appended.
    #[test]
    fn live_event_variants_are_append_only_on_the_wire() {
        use crate::types::{
            ErrorKind,
            Event,
            LiveError,
            LiveEvent,
            OrdType,
            Order,
            Side,
            TimeInForce,
        };

        let ord =
            |ev: &LiveEvent| bincode::encode_to_vec(ev, bincode::config::standard()).unwrap()[0];

        let zero_event = Event {
            ev: 0,
            exch_ts: 0,
            local_ts: 0,
            px: 0.0,
            qty: 0.0,
            order_id: 0,
            ival: 0,
            fval: 0.0,
        };
        let an_order = Order::new(
            1,
            100,
            0.01,
            1.0,
            Side::Buy,
            OrdType::Limit,
            TimeInForce::GTC,
        );

        assert_eq!(
            ord(&LiveEvent::BatchStart),
            0,
            "BatchStart must stay variant 0"
        );
        assert_eq!(ord(&LiveEvent::BatchEnd), 1, "BatchEnd must stay variant 1");
        assert_eq!(
            ord(&LiveEvent::Feed {
                symbol: "BTC".to_string(),
                event: zero_event,
            }),
            2,
            "Feed must stay variant 2"
        );
        assert_eq!(
            ord(&LiveEvent::Order {
                symbol: "BTC".to_string(),
                order: an_order,
            }),
            3,
            "Order must stay variant 3"
        );
        assert_eq!(
            ord(&LiveEvent::Position {
                symbol: "BTC".to_string(),
                qty: 0.0,
                exch_ts: 0,
            }),
            4,
            "Position must stay variant 4"
        );
        assert_eq!(
            ord(&LiveEvent::Error(LiveError::new(
                ErrorKind::ConnectionInterrupted
            ))),
            5,
            "Error must stay variant 5"
        );
        assert_eq!(
            ord(&LiveEvent::SnapshotComplete {
                symbol: "BTC".to_string(),
                snapshot_time_ns: 0,
            }),
            6,
            "SnapshotComplete must stay variant 6, and any new event appended after it"
        );
    }

    #[test]
    fn status_variants_are_append_only_on_the_wire() {
        use crate::types::Status;

        let ord = |s: &Status| bincode::encode_to_vec(s, bincode::config::standard()).unwrap()[0];

        assert_eq!(ord(&Status::None), 0, "None must stay variant 0");
        assert_eq!(ord(&Status::New), 1, "New must stay variant 1");
        assert_eq!(ord(&Status::Expired), 2, "Expired must stay variant 2");
        assert_eq!(ord(&Status::Filled), 3, "Filled must stay variant 3");
        assert_eq!(ord(&Status::Canceled), 4, "Canceled must stay variant 4");
        assert_eq!(
            ord(&Status::PartiallyFilled),
            5,
            "PartiallyFilled must stay variant 5"
        );
        assert_eq!(ord(&Status::Rejected), 6, "Rejected must stay variant 6");
        assert_eq!(ord(&Status::Replaced), 7, "Replaced must stay variant 7");
        // Decoy: `#[repr(u8)] Unsupported = 255` is not what rides on the wire. bincode writes
        // the positional index, so this is 8. A future `Uncertain` (design item S3) appended
        // after it would be 9 — not 8 — whatever literal is written next to it.
        assert_eq!(
            ord(&Status::Unsupported),
            8,
            "Unsupported rides as positional 8, never its repr 255"
        );
    }

    #[test]
    fn side_variants_are_append_only_on_the_wire() {
        use crate::types::Side;

        let ord = |s: &Side| bincode::encode_to_vec(s, bincode::config::standard()).unwrap()[0];

        // `#[repr(i8)] Buy = 1, Sell = -1, None = 0, Unsupported = 127` are all decoys: the wire
        // carries the positional index. Sell's negative repr is the sharpest trap — it rides as
        // 1, not 255/-1 — and None rides as 2, not 0.
        assert_eq!(
            ord(&Side::Buy),
            0,
            "Buy rides as positional 0, not its repr 1"
        );
        assert_eq!(
            ord(&Side::Sell),
            1,
            "Sell rides as positional 1, not its repr -1"
        );
        assert_eq!(
            ord(&Side::None),
            2,
            "None rides as positional 2, not its repr 0"
        );
        assert_eq!(
            ord(&Side::Unsupported),
            3,
            "Unsupported rides as positional 3, not its repr 127"
        );
    }

    #[test]
    fn ord_type_variants_are_append_only_on_the_wire() {
        use crate::types::OrdType;

        let ord = |t: &OrdType| bincode::encode_to_vec(t, bincode::config::standard()).unwrap()[0];

        assert_eq!(ord(&OrdType::Limit), 0, "Limit must stay variant 0");
        assert_eq!(ord(&OrdType::Market), 1, "Market must stay variant 1");
        assert_eq!(
            ord(&OrdType::Unsupported),
            2,
            "Unsupported rides as positional 2, not its repr 255"
        );
    }

    #[test]
    fn time_in_force_variants_are_append_only_on_the_wire() {
        use crate::types::TimeInForce;

        let ord =
            |t: &TimeInForce| bincode::encode_to_vec(t, bincode::config::standard()).unwrap()[0];

        assert_eq!(ord(&TimeInForce::GTC), 0, "GTC must stay variant 0");
        assert_eq!(ord(&TimeInForce::GTX), 1, "GTX must stay variant 1");
        assert_eq!(ord(&TimeInForce::FOK), 2, "FOK must stay variant 2");
        assert_eq!(ord(&TimeInForce::IOC), 3, "IOC must stay variant 3");
        assert_eq!(
            ord(&TimeInForce::Unsupported),
            4,
            "Unsupported rides as positional 4, not its repr 255"
        );
    }

    #[test]
    fn error_kind_variants_are_append_only_on_the_wire() {
        use crate::types::ErrorKind;

        let ord =
            |e: &ErrorKind| bincode::encode_to_vec(e, bincode::config::standard()).unwrap()[0];

        assert_eq!(
            ord(&ErrorKind::ConnectionInterrupted),
            0,
            "must stay variant 0"
        );
        assert_eq!(
            ord(&ErrorKind::CriticalConnectionError),
            1,
            "must stay variant 1"
        );
        assert_eq!(ord(&ErrorKind::OrderError), 2, "must stay variant 2");
        assert_eq!(ord(&ErrorKind::Custom(0)), 3, "Custom must stay variant 3");
    }

    #[test]
    fn value_variants_are_append_only_on_the_wire() {
        use std::collections::HashMap;

        use crate::types::Value;

        let ord = |v: &Value| bincode::encode_to_vec(v, bincode::config::standard()).unwrap()[0];

        assert_eq!(
            ord(&Value::String(String::new())),
            0,
            "String must stay variant 0"
        );
        assert_eq!(ord(&Value::Int(0)), 1, "Int must stay variant 1");
        assert_eq!(ord(&Value::Float(0.0)), 2, "Float must stay variant 2");
        assert_eq!(ord(&Value::Bool(false)), 3, "Bool must stay variant 3");
        assert_eq!(ord(&Value::List(Vec::new())), 4, "List must stay variant 4");
        assert_eq!(
            ord(&Value::Map(HashMap::new())),
            5,
            "Map must stay variant 5"
        );
        assert_eq!(ord(&Value::Empty), 6, "Empty must stay variant 6");
    }

    /// The single definition of "terminal" ([`Status::is_terminal`]), pinned. A terminal
    /// status is final: no later update may resurrect the order, and the order id it held is
    /// freed. Before this method the set `{Canceled, Expired, Filled}` was duplicated at the
    /// live final-status guard (`live/bot.rs`) and `Local::clear_inactive_orders`
    /// (`backtest/proc/local.rs` and its L3 mirror), and **both omitted `Rejected`** — a
    /// rejected order was neither frozen against resurrection nor cleared from the book. The
    /// ruling this pins, forced by the method's wildcard-free match:
    ///
    /// * `Rejected` **is** terminal — a refused order never rested and takes no later update.
    /// * `Replaced` is **not** terminal — in this codebase a replaced order keeps its
    ///   `order_id` and continues resting (`Local::modify` sets `req = Replaced`), so it must
    ///   stay mutable and tracked.
    /// * `None`/`Unsupported` are **not** terminal — neither frees an order id, and the
    ///   connector removal path must not drop on them (invariants S1/S2).
    #[test]
    fn is_terminal_is_the_single_ruling_and_rejected_is_terminal() {
        use crate::types::Status;

        for terminal in [
            Status::Filled,
            Status::Canceled,
            Status::Expired,
            Status::Rejected,
        ] {
            assert!(terminal.is_terminal(), "{terminal:?} must be terminal");
        }
        for live in [
            Status::None,
            Status::New,
            Status::PartiallyFilled,
            Status::Replaced,
            Status::Unsupported,
        ] {
            assert!(!live.is_terminal(), "{live:?} must not be terminal");
        }
    }
}
