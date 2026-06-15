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
    /// On-demand reconcile reply frame (R-M1a). Tail-appended after `SnapshotComplete`.
    ///
    /// Streamed as `Begin → Account? → Position* → OpenOrder* → WalletCoin* → End`, bracketed by
    /// `BatchStart`/`BatchEnd` on the connector. Carries `symbol` so the outer `LiveEvent` routes
    /// through `symbol_to_inst_no` exactly like `Feed`/`Order`/`Position`/`SnapshotComplete`; all
    /// sub-frames travel under one routing arm.
    Reconcile {
        symbol: String,
        frame: ReconcileFrame,
    },
    /// On-demand spot-order reply frame (B-M1a). Tail-appended after `Reconcile`.
    ///
    /// Mirrors `Reconcile`'s framing: streamed as `Begin → Ack? → End`, bracketed by
    /// `BatchStart`/`BatchEnd` on the connector and delivered targeted to the requesting bot.
    /// Carries `symbol` so the outer `LiveEvent` routes through `symbol_to_inst_no` exactly like
    /// `Feed`/`Order`/`Position`/`Reconcile`; all sub-frames travel under one routing arm. The op
    /// is a Bybit **spot** order (place/cancel/status), correlated by `order_link_id` (Bybit
    /// `orderId` is always 0 for the bot's purposes — B-M1a D-b).
    SpotOrderReply {
        symbol: String,
        frame: SpotOrderFrame,
    },
}

/// A single frame of a reconcile reply stream (R-M1a, §2). Derives mirror [`LiveEvent`] so it is
/// bincode-encodable as IPC payload. Variant order is wire-significant — append only.
#[derive(Clone, Debug, Decode, Encode)]
pub enum ReconcileFrame {
    /// Opens a reconcile stream; matched against `End` by `request_id` (fail-closed completeness).
    Begin { request_id: u64, snapshot_ts_ns: i64 },
    /// Account-level totals (D-d); one frame per reconcile, emitted right after `Begin`.
    /// `total_margin_balance` = wallet + UPL.
    Account {
        total_equity: f64,
        total_wallet_balance: f64,
        total_margin_balance: f64,
        total_available_balance: f64,
        total_perp_upl: f64,
        total_initial_margin: f64,
        total_maintenance_margin: f64,
        account_im_rate: f64,
        account_mm_rate: f64,
    },
    /// Signed position quantity; symbol lives on the outer `Reconcile`.
    Position { qty: f64 },
    /// A single open order; reuses `Order`'s hand-written `Encode`/`Decode`.
    OpenOrder { order: Order },
    /// Per-coin wallet balance. Bybit returns numerics as JSON strings (connector parses
    /// string→f64); `collateral_switch`/`margin_collateral` are Bybit booleans.
    WalletCoin {
        coin: String,
        wallet_balance: f64,
        equity: f64,
        usd_value: f64,
        spot_hedging_qty: f64,
        borrow_amount: f64,
        collateral_switch: bool,
        margin_collateral: bool,
    },
    /// Terminates a reconcile stream. `ok=false` (with raw `ret_code`/`ret_msg`) signals a
    /// fail-closed incomplete reconcile that the bot must NOT treat as authoritative.
    End {
        request_id: u64,
        ok: bool,
        endpoint: ReconcileEndpoint,
        ret_code: Option<i64>,
        ret_msg: String,
        http_status: u16,
    },
}

/// Which REST endpoint a [`ReconcileFrame::End`] pertains to (§0 D-b). IPC-encoded → append only.
#[derive(Clone, Copy, Debug, Decode, Encode)]
pub enum ReconcileEndpoint {
    Position,
    OpenOrders,
    WalletBalance,
    None,
}

/// A single frame of a spot-order reply stream (B-M1a). Derives mirror [`ReconcileFrame`] so it is
/// bincode-encodable as IPC payload. Variant order is wire-significant — append only.
///
/// Frame sequencing (mirrors reconcile's `Begin`/content/`End`):
/// * **Place** → `Begin` + `Ack`(post-create state) + `End`
/// * **Status** → `Begin` + `Ack`(current state) + `End`
/// * **Cancel** → `Begin` + `End` (the result lives in `End.ok`/`ret_code`)
#[derive(Clone, Debug, Decode, Encode)]
pub enum SpotOrderFrame {
    /// Opens a spot-order stream; matched against `End` by `request_id` (fail-closed completeness).
    Begin { request_id: u64, snapshot_ts_ns: i64 },
    /// Post-op order state, correlated by `order_link_id` (Bybit `orderId` is unusable — B-M1a D-b).
    /// `cum_exec_qty`/`avg_price` are parsed string→f64 on the connector; `status` is mapped from
    /// Bybit `orderStatus` (fail-closed [`SpotOrderStatus::Other`] on an unknown value).
    Ack {
        order_link_id: String,
        cum_exec_qty: f64,
        avg_price: f64,
        status: SpotOrderStatus,
    },
    /// Terminates a spot-order stream. Bybit always replies HTTP 200, so correctness relies on
    /// `ret_code` — a non-zero/absent `retCode` ⇒ `ok = false` (fail-closed). Raw `ret_code`/`ret_msg`
    /// are preserved (no classification — that is the consumer's job).
    End {
        request_id: u64,
        ok: bool,
        ret_code: Option<i64>,
        ret_msg: String,
        http_status: u16,
    },
}

/// Spot-order lifecycle status, mapped from Bybit `orderStatus` (B-M1a). IPC-encoded → append only.
/// An unrecognized Bybit value maps to [`SpotOrderStatus::Other`] (fail-closed).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Decode, Encode)]
pub enum SpotOrderStatus {
    New,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected,
    Other,
}

/// The completed result of a spot-order op, accumulated bot-side from frames between `Begin` and
/// `End` (consumer-side; not IPC-encoded). Mirrors [`ReconcileOutcome`]: stored separately so the
/// strategy FSM polls a freshly REST-pulled spot-order truth keyed by `request_id`. `ack` is `None`
/// for a Cancel (which carries no `Ack` frame) — the result is then read from `ok`/`ret_code`.
#[derive(Clone, Debug)]
pub struct SpotOrderOutcome {
    pub request_id: u64,
    pub snapshot_ts_ns: i64,
    pub ack: Option<SpotOrderAck>,
    pub ok: bool,
    pub ret_code: Option<i64>,
    pub ret_msg: String,
    pub http_status: u16,
}

/// The `Ack` payload of a spot-order op (consumer-side; not IPC-encoded). See [`SpotOrderFrame::Ack`].
#[derive(Clone, Debug)]
pub struct SpotOrderAck {
    pub order_link_id: String,
    pub cum_exec_qty: f64,
    pub avg_price: f64,
    pub status: SpotOrderStatus,
}

/// Account-level totals from a reconcile (consumer-side; not IPC-encoded).
#[derive(Clone, Debug, Default)]
pub struct AccountTotals {
    pub total_equity: f64,
    pub total_wallet_balance: f64,
    pub total_margin_balance: f64,
    pub total_available_balance: f64,
    pub total_perp_upl: f64,
    pub total_initial_margin: f64,
    pub total_maintenance_margin: f64,
    pub account_im_rate: f64,
    pub account_mm_rate: f64,
}

/// Per-coin wallet balance from a reconcile (consumer-side; not IPC-encoded).
#[derive(Clone, Debug)]
pub struct ReconcileWalletCoin {
    pub coin: String,
    pub wallet_balance: f64,
    pub equity: f64,
    pub usd_value: f64,
    pub spot_hedging_qty: f64,
    pub borrow_amount: f64,
    pub collateral_switch: bool,
    pub margin_collateral: bool,
}

/// The completed result of a reconcile, accumulated bot-side from frames between `Begin` and `End`
/// (consumer-side; not IPC-encoded). Stored separately from `instrument.orders`/`state.position`
/// (the WS-cached view) so R-M1b can compare fresh REST truth against the cached view.
#[derive(Clone, Debug)]
pub struct ReconcileOutcome {
    pub request_id: u64,
    pub snapshot_ts_ns: i64,
    pub account: Option<AccountTotals>,
    pub positions: Vec<f64>,
    pub open_orders: Vec<Order>,
    pub wallet_coins: Vec<ReconcileWalletCoin>,
    pub ok: bool,
    pub ret_code: Option<i64>,
    pub ret_msg: String,
    pub endpoint: ReconcileEndpoint,
    pub http_status: u16,
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
    pub exec_qty: f64,
    /// Executed price in ticks (`executed_price / tick_size`), only available when this order is
    /// executed.
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
    /// On-demand request for a fresh REST reconcile snapshot of account state (R-M1a).
    ///
    /// Tail-appended after `RegisterInstrument` — bincode encodes by discriminant index, so new
    /// variants MUST be appended, never inserted mid-enum. The connector replies by streaming a
    /// `BatchStart`-bracketed group of [`LiveEvent::Reconcile`] frames keyed by `request_id`.
    Reconcile {
        symbol: String,
        request_id: u64,
        scope: ReconcileScope,
    },
    /// On-demand spot-order op (place/cancel/status) for a Bybit **spot** instrument (B-M1a).
    ///
    /// Tail-appended after `Reconcile` — bincode encodes by discriminant index, so new variants
    /// MUST be appended, never inserted mid-enum. The connector replies by streaming a
    /// `BatchStart`-bracketed group of [`LiveEvent::SpotOrderReply`] frames keyed by `request_id`.
    /// `category="spot"` is hardcoded per-RPC on the connector (B-M1a D-feasible).
    SpotOrder {
        symbol: String,
        request_id: u64,
        action: SpotOrderAction,
    },
}

/// Scope of a [`LiveRequest::Reconcile`] — which account-state endpoints to pull (R-M1a, §0 D-b).
#[derive(Clone, Copy, Debug, Encode, Decode)]
pub enum ReconcileScope {
    All,
    Position,
    OpenOrders,
    Wallet,
}

/// The spot-order op carried by [`LiveRequest::SpotOrder`] (B-M1a). IPC-encoded → append only.
/// Correlation is by `order_link_id` (bot-chosen) — Bybit's `orderId` is unusable for the bot
/// (always 0, D-b).
#[derive(Clone, Debug, Encode, Decode)]
pub enum SpotOrderAction {
    /// Places a new spot order. For a market BUY, Bybit defaults `qty` to QUOTE coin; a base-coin
    /// order MUST set `market_unit = BaseCoin` (de-risk §34-41) so `qty` is interpreted in base.
    Place {
        side: Side,
        order_type: SpotOrdType,
        qty: f64,
        market_unit: SpotMarketUnit,
        price: Option<f64>,
        order_link_id: String,
    },
    /// Cancels a resting spot order by its `order_link_id`.
    Cancel { order_link_id: String },
    /// Queries the current state of a spot order by its `order_link_id`.
    Status { order_link_id: String },
}

/// Which coin a spot order's `qty` is denominated in (Bybit `marketUnit`, B-M1a). IPC-encoded →
/// append only.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Encode, Decode)]
pub enum SpotMarketUnit {
    BaseCoin,
    QuoteCoin,
}

/// Order type for a spot order (B-M1a). Distinct from [`OrdType`] (which is a Copy enum with a wire
/// `repr`); kept minimal for the spot RPC. IPC-encoded → append only.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Encode, Decode)]
pub enum SpotOrdType {
    Market,
    Limit,
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

    /// Returns the most recent completed reconcile outcome for this asset, if any (R-M1a).
    ///
    /// In live mode this is `None` until the connector delivers a complete reconcile stream
    /// (matching `Begin`/`End`) for the asset — a fail-closed default: a `Begin` without a matching
    /// `End` leaves this `None`, so an interrupted reconcile is never treated as authoritative.
    /// The returned outcome is freshly REST-pulled truth, stored separately from
    /// [`position`](Self::position)/[`orders`](Self::orders) (the connector's cached view).
    ///
    /// In backtest mode this always returns `None` (no live reconcile phase).
    ///
    /// * `asset_no` - Asset number to query.
    fn last_reconcile(&self, asset_no: usize) -> Option<&ReconcileOutcome>;

    /// Returns the most recent completed spot-order outcome for this asset, if any (B-M1a).
    ///
    /// In live mode this is `None` until the connector delivers a complete spot-order stream
    /// (matching `Begin`/`End`) for the asset — a fail-closed default: a `Begin` without a matching
    /// `End` leaves this `None`, so an interrupted spot-order op is never treated as authoritative.
    /// Mirrors [`last_reconcile`](Self::last_reconcile); the strategy FSM polls it after issuing a
    /// `request_spot_order`, keyed by `request_id`.
    ///
    /// In backtest mode this always returns `None` (no live spot-order phase).
    ///
    /// * `asset_no` - Asset number to query.
    fn last_spot_order(&self, asset_no: usize) -> Option<&SpotOrderOutcome>;

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
}
