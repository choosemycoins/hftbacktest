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

/// A bot's own identifier for an order.
///
/// A newtype rather than `type OrderId = u64` (invariant E3b), so that an order id cannot be
/// silently interchanged with the other `u64`s it travels beside — an `asset_no`, a venue's
/// own order id, a nonce, a timestamp. The alias made all of those the same type to the
/// compiler while being different things to the venue.
///
/// `#[repr(transparent)]` over the `u64` already on the wire, so the encoding and the
/// `#[repr(C)] Order` layout are unchanged; `py-hftbacktest`'s `order_dtype` still reads
/// `('order_id', 'u8')` at the same offset.
///
/// `trait Bot` needed no signature change: it already spelled `OrderId` throughout, which is
/// what made this cheap.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, Decode, Encode)]
#[repr(transparent)]
pub struct OrderId(pub u64);

impl OrderId {
    pub const fn new(order_id: u64) -> Self {
        Self(order_id)
    }

    pub const fn get(&self) -> u64 {
        self.0
    }
}

impl From<u64> for OrderId {
    fn from(order_id: u64) -> Self {
        Self(order_id)
    }
}

impl std::fmt::Display for OrderId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

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

impl Side {
    /// The side as a direction, or `None` when it is not one.
    ///
    /// The only way to obtain a [`ResolvedSide`], and therefore the one place where the two
    /// non-directions — [`Side::None`] and [`Side::Unsupported`] — have to be dealt with. Call
    /// it at the boundary where an order is submitted or a venue message is turned into one:
    /// refusing there costs a recoverable error, whereas the same value reaching position math
    /// used to cost the process (invariant E2).
    pub fn try_resolve(&self) -> Option<ResolvedSide> {
        match self {
            Side::Buy => Some(ResolvedSide::Buy),
            Side::Sell => Some(ResolvedSide::Sell),
            // Neither is a direction: `None` is "no side given", `Unsupported` is "the venue
            // said something this connector does not recognise". Guessing one would guess the
            // sign of a position (`AGENTS.md` §1.1).
            Side::None | Side::Unsupported => None,
        }
    }
}

/// A side that is a direction: exactly the two [`Side`] values that have a sign.
///
/// **Not a wire type on purpose.** [`Side`] stays exactly as it is on the wire, carrying the two
/// values a venue can force on us; this is what the money path takes once they have been ruled
/// out. Everything it offers is total — [`ResolvedSide::sign`] cannot fail — so position math
/// that takes one cannot be reached with a side that has no sign, and the panic that used to
/// guard it is not merely unlikely but unwritable (correctness-by-construction §1.6, invariant
/// E2).
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum ResolvedSide {
    Buy,
    Sell,
}

impl ResolvedSide {
    /// `+1` for a buy, `-1` for a sell: the factor position and balance move by.
    pub const fn sign(&self) -> f64 {
        match self {
            ResolvedSide::Buy => 1.0,
            ResolvedSide::Sell => -1.0,
        }
    }

    /// The venue-facing spelling, for exchanges whose order payload carries the side as text.
    pub const fn as_str(&self) -> &'static str {
        match self {
            ResolvedSide::Buy => "BUY",
            ResolvedSide::Sell => "SELL",
        }
    }
}

impl From<ResolvedSide> for Side {
    fn from(side: ResolvedSide) -> Self {
        match side {
            ResolvedSide::Buy => Side::Buy,
            ResolvedSide::Sell => Side::Sell,
        }
    }
}

/// Which side of an execution our order was on: it rested and was hit ([`Liquidity::Maker`]),
/// or it crossed the spread and took ([`Liquidity::Taker`]).
///
/// **Not a wire type on purpose.** It is how a fill's liquidity is *supplied* — by the backtest
/// exchange, which knows it by construction, or by a venue that reports it — and it reaches the
/// wire only as [`Order::maker`], the `bool` that was already there. Callers pass
/// `Option<Liquidity>`: `None` is the honest answer for a venue that does not report the
/// liquidity of a fill, and it claims nothing rather than claiming the cheaper side
/// (`AGENTS.md` §1.1, correctness-by-construction §1.6 invariant C2).
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum Liquidity {
    /// The order was resting and provided the liquidity.
    Maker,
    /// The order crossed the spread and took the liquidity.
    Taker,
}

/// A price, in the instrument's quote currency.
///
/// One half of invariant C5. Prices, prices-in-ticks and quantities are all `f64`/`i64` and so
/// were freely interchangeable; the measured cost was `bybit/ordermanager.rs` dividing an
/// execution price by `order.price_tick` — a price already in ticks — instead of by the tick
/// size, which reported every bybit fill as having executed at tick 0.
///
/// The **only** crossing to ticks is [`Price::to_ticks`], which takes a [`TickSize`]. A
/// [`PriceTick`] passed there does not compile.
#[derive(Clone, Copy, PartialEq, PartialOrd, Debug, Default, Decode, Encode)]
#[repr(transparent)]
pub struct Price(f64);

impl Price {
    pub const fn new(price: f64) -> Self {
        Self(price)
    }

    pub const fn get(&self) -> f64 {
        self.0
    }

    /// This price in ticks — the one bridge, and it needs a [`TickSize`] to cross.
    ///
    /// `.round()` rather than a bare `as i64` cast: a cast truncates toward zero, so a price
    /// landing on a half-tick lands on a different tick above and below zero. The scattered
    /// hand-written copies of this idiom mostly used `.round()`, which is what makes them
    /// *mostly* equivalent — centralising it removes the "mostly".
    pub fn to_ticks(self, tick_size: TickSize) -> PriceTick {
        PriceTick((self.0 / tick_size.0).round() as i64)
    }
}

/// A price expressed in ticks — `price / tick_size`, as an integer.
///
/// The counterpart to [`Price`]. Crossing back needs the same [`TickSize`]
/// ([`PriceTick::to_price`]).
///
/// **Residual, stated rather than hidden:** `Order::price_tick` and `Order::exec_price_tick`
/// are both `PriceTick`, so swapping *those two* is still writable. Distinguishing them would
/// take two more types for no measured bug, and the confusion this closes is the
/// price-versus-tick one.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, Decode, Encode)]
#[repr(transparent)]
pub struct PriceTick(i64);

impl PriceTick {
    pub const fn new(ticks: i64) -> Self {
        Self(ticks)
    }

    /// The raw tick count, for the depth and queue interfaces that still speak `i64`.
    ///
    /// [`MarketDepth`](crate::depth::MarketDepth) and the queue models are deliberately left
    /// on raw `i64` — they are strategy-facing and are read by the numba bindings, and no bug
    /// has been measured there. This is the named adapter at that boundary; a `get()` or a
    /// `PriceTick::new` far away from a depth call is worth a second look.
    pub const fn get(&self) -> i64 {
        self.0
    }

    /// This tick back as a price — the one bridge, and it needs the same [`TickSize`].
    pub fn to_price(self, tick_size: TickSize) -> Price {
        Price(self.0 as f64 * tick_size.0)
    }
}

/// The tick size of an instrument: the price increment its venue quotes on.
///
/// Exists so that [`Price::to_ticks`] and [`PriceTick::to_price`] can demand one, which is
/// what makes a price-in-ticks unusable as a divisor (invariant C5).
#[derive(Clone, Copy, PartialEq, PartialOrd, Debug, Default, Decode, Encode)]
#[repr(transparent)]
pub struct TickSize(f64);

impl TickSize {
    pub const fn new(tick_size: f64) -> Self {
        Self(tick_size)
    }

    pub const fn get(&self) -> f64 {
        self.0
    }
}

/// A quantity of the instrument — an order's size or what is left of it.
///
/// The other half of invariant C5: a quantity and a price are both `f64`, so nothing stopped
/// one being used where the other belonged. There is no arithmetic between [`Qty`] and
/// [`Price`], so `qty * price` — a notional, which is a third thing — must be spelled out
/// through `get()` rather than falling out of a typo.
#[derive(Clone, Copy, PartialEq, PartialOrd, Debug, Default, Decode, Encode)]
#[repr(transparent)]
pub struct Qty(f64);

impl Qty {
    pub const ZERO: Self = Self(0.0);

    pub const fn new(qty: f64) -> Self {
        Self(qty)
    }

    pub const fn get(&self) -> f64 {
        self.0
    }
}

/// The quantity executed by **one** execution — a delta, never a running total.
///
/// [`Order::exec_qty`] has always meant "how much this particular execution filled", so that
/// an order filling in parts reports each part and the parts sum to the filled quantity.
/// Nothing enforced it: the field was a bare `f64`, and a venue that reports a *cumulative*
/// filled quantity — most REST APIs do — could be assigned straight into it. Both Binance
/// backends did exactly that with Binance REST's `executedQty`, so a second partial fill
/// re-reported everything filled before it (invariant E5, `AGENTS.md` §4.6).
///
/// **There is deliberately no `ExecDelta::new(f64)` and no `From<f64>`.** A value gets in
/// only through:
///
/// * [`ExecDelta::ZERO`] — "this update reports no execution", the honest answer for a status
///   change or a rejected request;
/// * [`ExecDelta::of_execution`] — a quantity computed *as* one execution, which in the
///   backtest exchanges is `qty.min(order.leaves_qty)` and is a delta by construction;
/// * a venue's cumulative total advanced against a watermark (`CumulativeFilled::advance` in
///   the connector), which is the only place a running total is turned into a difference.
///
/// `#[repr(transparent)]` over the `f64` that was already on the wire, so the encoding and
/// the `#[repr(C)] Order` layout are byte-identical — pinned by
/// `an_order_encodes_to_the_bytes_it_has_always_encoded_to` and
/// `the_order_layout_the_python_dtype_mirrors_is_unchanged`.
#[derive(Clone, Copy, PartialEq, PartialOrd, Debug, Decode, Encode)]
#[repr(transparent)]
pub struct ExecDelta(f64);

impl ExecDelta {
    /// No execution was reported by this update.
    pub const ZERO: Self = Self(0.0);

    /// The quantity filled by a single execution.
    ///
    /// The name is the contract: pass what *this* execution filled. Passing a running total
    /// compiles — a delta and a total are both quantities — but [`Order::record_execution`]
    /// subtracts it from `leaves_qty`, so a total drives the remainder negative rather than
    /// quietly over-reporting a position.
    pub const fn of_execution(qty: f64) -> Self {
        Self(qty)
    }

    /// The quantity, for arithmetic that has to leave the type.
    pub const fn get(&self) -> f64 {
        self.0
    }

    /// Whether this update reported an execution at all.
    pub fn is_some(&self) -> bool {
        self.0 > 0.0
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
    /// The venue said something about this order that the connector could not read, so its
    /// state is **unknown** — and unknown is not a resolution.
    ///
    /// Minted only by a connector, at the one place a venue status string fails to classify
    /// (`StatusVerdict::Unrecognised`). Before this variant existed the connector logged the
    /// unreadable status and published nothing, so the bot's copy silently kept whatever
    /// status it last held — an order the venue had just said something unexpected about
    /// looked, to the strategy, exactly like one it had confirmed as resting.
    ///
    /// The two properties that make it safe are enforced elsewhere and are easy to break
    /// independently: it is **not terminal** ([`Status::is_terminal`]), so no removal path
    /// frees the order id; and it **may still rest at the venue**
    /// ([`Status::may_rest_at_venue`], and therefore [`Order::active`]), so no path that
    /// filters on `active()` drops it. An uncertain order that gets dropped is the exact
    /// fail-open this variant exists to prevent: the id is freed, the strategy re-submits,
    /// and a duplicate stands at a venue that may still be holding the original
    /// (`AGENTS.md` §1.1, correctness-by-construction §1.6, invariant S3).
    ///
    /// It **overwrites** the last known status, and that is the intended reading: we no
    /// longer hold the claim that the order was `New` or `PartiallyFilled`, we hold only the
    /// quantities. Reconnect reconciliation is what resolves it — which today exists on
    /// Hyperliquid and Lighter only.
    ///
    /// The `= 9` is **mandatory, not cosmetic**: `Unsupported = 255` makes an implicit
    /// discriminant here `E0370 enum discriminant overflowed`. It is also read by
    /// `py-hftbacktest`'s numpy `order_dtype`, which reads this repr byte rather than the
    /// bincode ordinal. That the two happen to both be 9 is a coincidence of appending after
    /// exactly nine variants — the wire number is positional and is pinned separately.
    Uncertain = 9,
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
    /// to "drop the order" at a removal site — the structural half of invariants S1/S2. That
    /// is how [`Status::Uncertain`] (invariant S3) was forced to declare itself when it was
    /// appended, rather than defaulting into a set it does not belong in.
    ///
    /// Ruling on the two the previous hand-written set silently omitted: `Rejected` **is**
    /// terminal (a refused order never rested and takes no later update); `Replaced` is
    /// **not** (a replaced order keeps its id and continues resting — `Local::modify`).
    /// `None`/`Unsupported`/`Uncertain` are not terminal either — none of them is a
    /// resolution that frees an order id.
    pub fn is_terminal(&self) -> bool {
        match self {
            Status::Filled | Status::Canceled | Status::Expired | Status::Rejected => true,
            Status::None
            | Status::New
            | Status::PartiallyFilled
            | Status::Replaced
            | Status::Unsupported
            | Status::Uncertain => false,
        }
    }

    /// Whether an order in this status **may still be resting at the venue**, and so must not
    /// be treated as gone.
    ///
    /// This is the predicate behind [`Order::active`], and it is a different question from
    /// [`Status::is_terminal`]. "Terminal" asks whether the order is *resolved*; this asks
    /// whether it might *still be working*. The gap between them is where orders get lost.
    ///
    /// **Where this actually bites, stated precisely** — it is easy to overstate, and the
    /// overstatement would make a reviewer trust a pin that is not there. `active()` is
    /// **strategy-facing**: it is what a bot asks about the orders in `Bot::orders(asset_no)`,
    /// and those do carry [`Status::Uncertain`]. Answering `false` there is the fail-open
    /// direction — the strategy concludes the order is gone, re-quotes, and stands a duplicate
    /// at a venue that may still be holding the original (`AGENTS.md` §1.1).
    ///
    /// It is **not** what the removal paths in this tree key on. `LiveBot::clear_inactive_orders`
    /// and its backtest twin both read `is_terminal()`; the five connector `orders()` filters
    /// do read `active()`, but never see an uncertain order, because the connectors publish
    /// the uncertainty *without* writing it into their own tracked copy. So this ruling is
    /// load-bearing for the strategy and merely defensive for those filters.
    ///
    /// **Wildcard-free on purpose.** The hand-written `status == New || status ==
    /// PartiallyFilled` this replaces would have compiled unchanged when `Uncertain` was
    /// appended and quietly answered `false` — no test failure, no log line. A future variant
    /// has to rule on itself here.
    ///
    /// `Uncertain` answers **true**: we do not know that it is gone. `None` — submitted, not
    /// yet acked — answers false, preserving the existing meaning of `active()`; an order the
    /// venue has not acknowledged is not yet resting.
    pub fn may_rest_at_venue(&self) -> bool {
        match self {
            Status::New | Status::PartiallyFilled | Status::Uncertain => true,
            Status::None
            | Status::Expired
            | Status::Filled
            | Status::Canceled
            | Status::Rejected
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
    pub qty: Qty,
    /// The quantity of this order that has not yet been executed. It represents the remaining
    /// quantity that is still open or active in the market after any partial fills.
    pub leaves_qty: Qty,
    /// Executed quantity, only available when this order is executed.
    ///
    /// This is the quantity executed by the single execution the order reports, not the
    /// cumulative quantity executed by the order: an order that fills in several parts reports
    /// each part separately, and the parts sum to the order's executed quantity. Backtesting
    /// relies on this to accumulate an order's fills without double-counting them.
    ///
    /// The [`ExecDelta`] wrapper is what makes that structural rather than a convention —
    /// a cumulative total has no constructor that reaches this field. Write it through
    /// [`Order::record_execution`], which moves `leaves_qty` by the same amount.
    pub exec_qty: ExecDelta,
    /// Executed price in ticks (`executed_price / tick_size`), only available when this order is
    /// executed. It is the price of the execution [`Order::exec_qty`] reports.
    pub exec_price_tick: PriceTick,
    /// Order price in ticks (`price / tick_size`).
    pub price_tick: PriceTick,
    /// The tick size of the asset associated with this order.
    pub tick_size: TickSize,
    /// The time at which the exchange processes this order, ideally when the matching engine
    /// processes the order, will be set if the value is available.
    pub exch_timestamp: i64,
    /// The time at which the local receives this order or sent this order to the exchange.
    pub local_timestamp: i64,
    pub order_id: OrderId,
    /// Additional data used for [`QueueModel`](`crate::backtest::models::QueueModel`).
    /// This is only available in backtesting, and the type `Q` is set to `()` in a live bot.
    pub q: Box<dyn AnyClone + Send>,
    /// Whether the execution this order reports was a maker execution.
    ///
    /// **True means a [`Liquidity::Maker`] was reported for that execution — nothing else sets
    /// it.** Write it through [`Order::set_liquidity`], which takes the liquidity as it is
    /// supplied at the moment a fill is applied. `false` therefore reads as "took liquidity, or
    /// nobody said": a venue that reports no per-fill liquidity leaves the flag unset rather
    /// than claiming the maker side it did not measure (`AGENTS.md` §1.1, invariant C2). The
    /// two are not distinguishable here, and deliberately so — the wire field is the `bool` it
    /// has always been, and under-claiming is the safe direction for fee and fill-quality
    /// attribution.
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
        order_id: OrderId,
        price_tick: PriceTick,
        tick_size: TickSize,
        qty: Qty,
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
            exec_price_tick: PriceTick::new(0),
            exec_qty: ExecDelta::ZERO,
            order_id,
            q: Box::new(()),
            maker: false,
            order_type,
        }
    }

    /// Returns the order price.
    pub fn price(&self) -> Price {
        self.price_tick.to_price(self.tick_size)
    }

    /// Returns the executed price, only available when this order is executed.
    pub fn exec_price(&self) -> Price {
        self.exec_price_tick.to_price(self.tick_size)
    }

    /// Records one execution against this order: the delta it filled, the price it filled at,
    /// and the liquidity it was on.
    ///
    /// **The single writer of [`Order::exec_qty`], and the reason the delta and the remainder
    /// cannot be written apart.** `leaves_qty` moves by exactly the amount recorded, in the
    /// same call, so "how much did this execution fill" and "how much is left" can no longer
    /// disagree — which is the temporal half of the fill-accounting identity that the
    /// property test and the debug-only `ExecutionLedger` check from the other side
    /// (`AGENTS.md` §1.6).
    ///
    /// It is also what turns a cumulative total from a silent over-report into an arithmetic
    /// contradiction: pass one and `leaves_qty` goes negative.
    pub fn record_execution(
        &mut self,
        delta: ExecDelta,
        exec_price_tick: PriceTick,
        liquidity: Option<Liquidity>,
    ) {
        self.exec_qty = delta;
        self.exec_price_tick = exec_price_tick;
        self.leaves_qty = Qty::new(self.leaves_qty.get() - delta.get());
        self.set_liquidity(liquidity);
    }

    /// Records the liquidity of the execution being applied to this order.
    ///
    /// The one writer of [`Order::maker`]. `Some(Liquidity::Maker)` is the only input that sets
    /// the flag; `Some(Liquidity::Taker)` and `None` — the venue reported no liquidity for this
    /// execution — both clear it, so a claim never outlives the execution that earned it. That
    /// is what keeps the flag a measurement instead of a constant (invariant C2): the callers
    /// that have no measurement have nothing to pass but `None`.
    pub fn set_liquidity(&mut self, liquidity: Option<Liquidity>) {
        self.maker = liquidity == Some(Liquidity::Maker);
    }

    /// Returns whether this order is cancelable.
    pub fn cancellable(&self) -> bool {
        (self.status == Status::New || self.status == Status::PartiallyFilled)
            && self.req == Status::None
    }

    /// Returns whether this order is active in the market.
    ///
    /// Delegates to [`Status::may_rest_at_venue`], the single wildcard-free ruling. It used to
    /// be the hand-written `status == New || status == PartiallyFilled` here — the same set
    /// for every status that existed then, but a hand-written set answers `false` for a *new*
    /// variant without anyone noticing, which is how appending [`Status::Uncertain`] would
    /// have quietly made every uncertain order look gone.
    ///
    /// **This predicate now also answers `true` for `Uncertain`**, which widens it: a strategy
    /// counting `orders().filter(active)` as "orders I am confident are resting" now also
    /// counts orders whose state is unknown. That is the fail-closed direction —
    /// over-counting resting orders suppresses a re-quote rather than standing a duplicate —
    /// but it is a strategy-visible change (`AGENTS.md` §4.8, invariant S3).
    pub fn active(&self) -> bool {
        self.status.may_rest_at_venue()
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
    pub order_id: OrderId,
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
            OrderId,
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

    /// **A side resolves to a direction or to nothing** (invariant E2).
    ///
    /// [`Side::try_resolve`] is total and is the only producer of a [`ResolvedSide`], so it is
    /// the single place the two sideless values are dealt with. The signs are the pin that
    /// matters downstream: they are what position and balance move by, and a swapped pair would
    /// invert every position in a backtest while every other test still passed.
    ///
    /// `Side::Sell`'s `#[repr(i8)]` is `-1` and `Side::None`'s is `0`, which makes "just cast
    /// the repr" look like a working shortcut — it is exactly the shortcut that puts a zero
    /// sign, and a position that never moves, into the money path.
    #[test]
    fn a_side_resolves_to_a_direction_or_to_nothing() {
        use crate::types::{ResolvedSide, Side};

        assert_eq!(Side::Buy.try_resolve(), Some(ResolvedSide::Buy));
        assert_eq!(Side::Sell.try_resolve(), Some(ResolvedSide::Sell));
        assert_eq!(
            Side::None.try_resolve(),
            None,
            "no side given is not a direction"
        );
        assert_eq!(
            Side::Unsupported.try_resolve(),
            None,
            "a side the venue named and we do not know is not a direction"
        );

        assert_eq!(ResolvedSide::Buy.sign(), 1.0);
        assert_eq!(ResolvedSide::Sell.sign(), -1.0);
        assert_eq!(ResolvedSide::Buy.as_str(), "BUY");
        assert_eq!(ResolvedSide::Sell.as_str(), "SELL");

        // Round trip: resolving loses nothing that was there.
        assert_eq!(Side::from(ResolvedSide::Buy), Side::Buy);
        assert_eq!(Side::from(ResolvedSide::Sell), Side::Sell);
    }

    /// **Only a reported [`Liquidity::Maker`] claims maker liquidity** (invariant C2).
    ///
    /// The flag is a measurement, so the three inputs a caller can have — "the venue said
    /// maker", "the venue said taker", "the venue said nothing" — must give true, false,
    /// false. The last is the one that matters: a source that does not report the liquidity of
    /// a fill has nothing to pass but `None`, and `None` must not read as the cheaper side.
    ///
    /// A claim also does not outlive the execution that earned it: applying an unreported
    /// execution to an order that last filled as a maker clears the flag rather than leaving
    /// the previous fill's word standing for this one.
    #[test]
    fn only_a_reported_maker_execution_claims_maker_liquidity() {
        use crate::types::{
            Liquidity,
            OrdType,
            Order,
            PriceTick,
            Qty,
            Side,
            TickSize,
            TimeInForce,
        };

        let mut order = Order::new(
            OrderId::new(1),
            PriceTick::new(100),
            TickSize::new(0.01),
            Qty::new(1.0),
            Side::Buy,
            OrdType::Limit,
            TimeInForce::GTC,
        );
        assert!(
            !order.maker,
            "an order that has not executed claims nothing"
        );

        order.set_liquidity(Some(Liquidity::Maker));
        assert!(order.maker, "the venue reported a maker execution");

        order.set_liquidity(Some(Liquidity::Taker));
        assert!(!order.maker, "the venue reported a taker execution");

        order.set_liquidity(Some(Liquidity::Maker));
        order.set_liquidity(None);
        assert!(
            !order.maker,
            "an unreported execution claims nothing, whatever the last one was"
        );
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
        use crate::types::{
            LiveRequest,
            OrdType,
            Order,
            PriceTick,
            Qty,
            Side,
            TickSize,
            TimeInForce,
        };

        let encode = |request: &LiveRequest| {
            bincode::encode_to_vec(request, bincode::config::standard()).unwrap()
        };

        let order = encode(&LiveRequest::Order {
            symbol: "BTC".to_string(),
            order: Order::new(
                OrderId::new(1),
                PriceTick::new(100),
                TickSize::new(0.01),
                Qty::new(1.0),
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
            PriceTick,
            Qty,
            Side,
            TickSize,
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
            OrderId::new(1),
            PriceTick::new(100),
            TickSize::new(0.01),
            Qty::new(1.0),
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
        // the positional index, so this is 8.
        assert_eq!(
            ord(&Status::Unsupported),
            8,
            "Unsupported rides as positional 8, never its repr 255"
        );
        // `Uncertain` (invariant S3) was **appended**, so it is 9 — the position after
        // `Unsupported`, not its `#[repr(u8)]` literal, which happens to also be 9 for an
        // unrelated reason (255 is taken, so an implicit discriminant would overflow). The
        // two nines are a coincidence; only this one is the wire.
        assert_eq!(
            ord(&Status::Uncertain),
            9,
            "Uncertain must stay appended at positional 9; inserting it anywhere earlier \
             renumbers every later variant and makes an older bot decode one status as another"
        );
    }

    /// An order id is **not** any of the other `u64`s it travels beside (invariant E3b).
    ///
    /// `pub type OrderId = u64` made the bot's own order id the same type, to the compiler, as
    /// an `asset_no`, a venue's own order id, a nonce, a timestamp and a tick count — all of
    /// which sit next to it in the same structs and the same call signatures. The alias
    /// documented an intention the compiler could not check.
    ///
    /// The newtype is byte-identical, which is what makes it affordable on a wire with no
    /// version field: `#[repr(transparent)]` over the same `u64`, so bincode writes the same
    /// varint and `order_dtype` reads `('order_id', 'u8')` at the same offset. The golden and
    /// layout pins are the proof.
    ///
    /// **`Event::order_id` deliberately stays a raw `u64`.** It is the L3 market-by-order
    /// feed's identifier — the *venue's*, arriving in market data — not the bot's, and it is
    /// part of the numpy `event_dtype` that recorded files are read back through. The first
    /// draft of this change retyped it by accident (it is declared before `Order` in this
    /// file, and a count-limited replace found it first); the derive that builds the numpy
    /// dtype rejected it outright, which is the sort of thing the second wire is good for.
    #[test]
    fn an_order_id_is_not_an_asset_number_or_a_price_tick() {
        use crate::types::{Event, OrderId};

        // It carries the number it was given, and says so the same way.
        assert_eq!(OrderId::new(7).get(), 7);
        assert_eq!(OrderId::from(7u64), OrderId::new(7));
        assert_eq!(format!("{}", OrderId::new(7)), "7");

        // Byte-identical to the `u64` it wraps: an older connector reading this field sees
        // exactly what it saw before.
        let cfg = bincode::config::standard();
        assert_eq!(
            bincode::encode_to_vec(OrderId::new(300), cfg).unwrap(),
            bincode::encode_to_vec(300u64, cfg).unwrap()
        );

        // And the L3 feed's id is a different thing, still a plain u64.
        let event = Event {
            ev: 0,
            exch_ts: 0,
            local_ts: 0,
            px: 0.0,
            qty: 0.0,
            order_id: 7,
            ival: 0,
            fval: 0.0,
        };
        assert_eq!(event.order_id, 7u64);
    }

    /// A price becomes ticks **only** by being divided by a tick size (invariant C5).
    ///
    /// The idiom `(price / tick).round() as i64` is written out at roughly ten call sites, and
    /// at two of them the divisor is not a tick size at all: `bybit/ordermanager.rs` divides an
    /// execution price by `order.price_tick` — a price already *in* ticks. Every other backend
    /// divides by `tick_size`. The result is silently wrong rather than absurd, which is why it
    /// survived: it produces a small integer that still looks like a tick.
    ///
    /// [`Price::to_ticks`] and [`PriceTick::to_price`] are the only crossings, and each takes a
    /// [`TickSize`]. Passing a `PriceTick` where a `TickSize` belongs no longer type-checks, so
    /// the bybit bug is not merely fixed — it is unwritable.
    #[test]
    fn the_only_bridge_between_a_price_and_a_tick_is_the_tick_size() {
        use crate::types::{Price, PriceTick, TickSize};

        let tick = TickSize::new(0.1);
        assert_eq!(Price::new(100.5).to_ticks(tick), PriceTick::new(1005));
        assert_eq!(PriceTick::new(1005).to_price(tick), Price::new(100.5));

        // Negatives round symmetrically: an instrument may quote below zero (a spread, a
        // basis), and `.round()` goes away from zero where a bare `as i64` would truncate
        // toward it.
        assert_eq!(Price::new(-100.5).to_ticks(tick), PriceTick::new(-1005));

        // **Not exact on a half-tick, and pinned as it is rather than as it ought to be.**
        // `0.15 / 0.1` is `1.4999999999999998` in binary floating point, not `1.5`, so it
        // rounds *down* to 1: a price sitting exactly on a half-tick boundary can land on
        // either neighbour depending on how the two decimals happen to be represented.
        //
        // This is the behaviour of the `(price / tick).round()` idiom that was already
        // written out at every call site. Centralising it neither introduced the wart nor
        // removed it, and asserting the idealised answer here would make this test a wish
        // instead of a description — the first thing a future reader would trust and the
        // first thing to mislead them. Removing it for real means decimal or scaled-integer
        // arithmetic throughout, which is a separate change with its own wire consequences.
        assert_eq!(Price::new(0.15).to_ticks(tick), PriceTick::new(1));
        assert_eq!(Price::new(-0.15).to_ticks(tick), PriceTick::new(-1));
    }

    /// An execution and the remainder it leaves **move together**, by construction.
    ///
    /// [`Order::exec_qty`] is a *per-execution delta*, never a running total — an order that
    /// fills in three parts reports three deltas that sum to the filled quantity, and
    /// backtesting adds them up. That was previously a convention held by a doc-comment and
    /// by every writer independently remembering it, and the two Binance backends did not:
    /// they wrote Binance REST's cumulative `executedQty` straight into the field.
    ///
    /// [`Order::record_execution`] is the single writer, and it lowers `leaves_qty` by exactly
    /// the delta it records. Writing a *cumulative* total through it does not merely
    /// mis-report the fill, it drives the remainder negative — the arithmetic itself rejects
    /// it, rather than a reviewer having to.
    #[test]
    fn an_execution_and_the_remainder_move_together() {
        use crate::types::{
            ExecDelta,
            OrdType,
            Order,
            PriceTick,
            Qty,
            Side,
            Status,
            TickSize,
            TimeInForce,
        };

        let mut order = Order::new(
            OrderId::new(1),
            PriceTick::new(100),
            TickSize::new(0.01),
            Qty::new(10.0),
            Side::Buy,
            OrdType::Limit,
            TimeInForce::GTC,
        );
        order.status = Status::New;
        assert_eq!(order.leaves_qty, Qty::new(10.0));
        assert_eq!(order.exec_qty, ExecDelta::ZERO);

        order.record_execution(ExecDelta::of_execution(3.0), PriceTick::new(100), None);
        assert_eq!(order.exec_qty, ExecDelta::of_execution(3.0));
        assert_eq!(
            order.leaves_qty,
            Qty::new(7.0),
            "the remainder moved by the delta"
        );

        // A second execution reports its **own** size, not the 5.0 cumulative.
        order.record_execution(ExecDelta::of_execution(2.0), PriceTick::new(100), None);
        assert_eq!(
            order.exec_qty,
            ExecDelta::of_execution(2.0),
            "each execution reports itself; the parts sum to the order's filled quantity"
        );
        assert_eq!(order.leaves_qty, Qty::new(5.0));
    }

    /// An uncertain order is **kept**: neither terminal nor droppable.
    ///
    /// [`Status::Uncertain`] means "the venue said something about this order that this
    /// connector could not read" — the order may well still be resting. Both halves matter and
    /// they are enforced at different sites:
    ///
    /// * not **terminal**, so the live final-status guard does not freeze it and
    ///   `Local::clear_inactive_orders` does not clear it;
    /// * still **resting** ([`Order::active`]), so none of the removal paths that filter on
    ///   `active()` drop it — five connector `orders()` filters and
    ///   `LiveBot::clear_inactive_orders`.
    ///
    /// The second half is the one that is easy to miss, and getting it wrong inverts the
    /// invariant silently: an uncertain order that is dropped frees its id, the strategy
    /// re-submits, and a duplicate stands at the venue — exactly the fail-open S3 exists to
    /// prevent (`AGENTS.md` §1.1).
    #[test]
    fn an_uncertain_order_is_neither_terminal_nor_droppable() {
        use crate::types::{OrdType, Order, PriceTick, Qty, Side, Status, TickSize, TimeInForce};

        assert!(
            !Status::Uncertain.is_terminal(),
            "an unreadable status is not a resolution: it frees no order id"
        );

        let mut order = Order::new(
            OrderId::new(1),
            PriceTick::new(100),
            TickSize::new(0.01),
            Qty::new(1.0),
            Side::Buy,
            OrdType::Limit,
            TimeInForce::GTC,
        );
        order.status = Status::Uncertain;
        assert!(
            order.active(),
            "an uncertain order may still be resting at the venue, so every removal path \
             that filters on active() must keep it"
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

    /// The exact bytes an [`Order`] rides on the wire as, pinned as a literal.
    ///
    /// **Why a golden literal and not a round-trip.** The next several invariants
    /// (`ExecDelta`, `Price`/`PriceTick`/`TickSize`/`Qty`, `OrderId`) retype `Order`'s fields
    /// as `#[repr(transparent)]` newtypes and claim to be byte-identical. A round-trip test
    /// cannot see a break in that claim — it would encode and decode through the *same* new
    /// code and agree with itself. Only a literal recorded before the change can fail, and a
    /// failure here means an older connector would misread every order this bot sends
    /// (`AGENTS.md` §2: the payload has no version field).
    ///
    /// **The fixture is deliberately not all-zeros.** `bincode`'s `standard()` config is
    /// varint + zigzag for integers and fixed-8 little-endian for floats, so a zeroed order
    /// encodes to a short run of `0x00`s that would survive almost any field retyping. The
    /// values below are chosen to discriminate: a **negative** `price_tick` (zigzag, so a
    /// sign flip changes the bytes), an `order_id` past the one-byte varint boundary, non-zero
    /// execution fields, and the enum variants whose positional ordinal differs from their
    /// `#[repr(..)]` literal (`Side::Sell` is repr `-1` but rides as `1`).
    #[test]
    fn an_order_encodes_to_the_bytes_it_has_always_encoded_to() {
        use crate::types::{
            ExecDelta,
            OrdType,
            Order,
            PriceTick,
            Qty,
            Side,
            Status,
            TickSize,
            TimeInForce,
        };

        let mut order = Order::new(
            OrderId::new(300),
            PriceTick::new(-5),
            TickSize::new(0.01),
            Qty::new(1.5),
            Side::Sell,
            OrdType::Limit,
            TimeInForce::IOC,
        );
        order.leaves_qty = Qty::new(0.5);
        order.exec_qty = ExecDelta::of_execution(1.0);
        order.exec_price_tick = PriceTick::new(-4);
        order.exch_timestamp = 1_700_000_000_000_000_000;
        order.local_timestamp = 1_700_000_000_000_000_001;
        order.maker = true;
        order.req = Status::New;
        order.status = Status::PartiallyFilled;

        let bytes = bincode::encode_to_vec(&order, bincode::config::standard()).unwrap();

        assert_eq!(
            bytes,
            vec![
                // qty: f64 1.5, fixed 8 bytes little-endian
                0, 0, 0, 0, 0, 0, 248, 63, //
                // leaves_qty: f64 0.5
                0, 0, 0, 0, 0, 0, 224, 63, //
                // exec_qty: f64 1.0
                0, 0, 0, 0, 0, 0, 240, 63, //
                // exec_price_tick: i64 -4, zigzag varint
                7, //
                // price_tick: i64 -5, zigzag varint
                9, //
                // tick_size: f64 0.01
                123, 20, 174, 71, 225, 122, 132, 63, //
                // exch_timestamp: i64 1_700_000_000_000_000_000, zigzag varint
                // (zigzag doubles it to 0x2F2F_39FC_6C54_0000, then 8 bytes little-endian)
                253, 0, 0, 84, 108, 252, 57, 47, 47, //
                // local_timestamp: i64 1_700_000_000_000_000_001, zigzag varint
                253, 2, 0, 84, 108, 252, 57, 47, 47, //
                // order_id: u64 300, varint (past the one-byte boundary)
                251, 44, 1, //
                // maker: bool true
                1, //
                // order_type: OrdType::Limit, positional 0
                0, //
                // req: Status::New, positional 1
                1, //
                // status: Status::PartiallyFilled, positional 5
                5, //
                // side: Side::Sell, positional 1 — NOT its repr -1
                1, //
                // time_in_force: TimeInForce::IOC, positional 3
                3,
            ],
            "the on-wire form of an Order changed; an older connector would misread every \
             order this bot sends (AGENTS.md §2 — no version field)"
        );

        // And it still decodes back to itself on this side.
        let (decoded, _): (Order, usize) =
            bincode::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
        assert_eq!(
            bincode::encode_to_vec(&decoded, bincode::config::standard()).unwrap(),
            bytes
        );
    }

    /// The **second** wire: the in-memory layout `py-hftbacktest`'s numpy `order_dtype`
    /// mirrors, pinned field by field.
    ///
    /// [`Order`] rides on two wires at once and they read different things. `bincode` reads
    /// the *positional* variant index and ignores `#[repr(..)]` entirely (that is what
    /// [`status_variants_are_append_only_on_the_wire`] and its siblings pin). numpy reads the
    /// **`#[repr(C)]` byte at a hard-coded offset**, so for `order_dtype` the repr is not a
    /// decoy at all — it is the whole contract, and `Status::Unsupported = 255` is the byte
    /// Python actually sees.
    ///
    /// Nothing connects the two definitions but this test: `order_dtype` is a Python literal
    /// that cannot know a Rust field was retyped or reordered. numpy does not validate, it
    /// just reads the offset — so a break here is silent and produces plausible-looking wrong
    /// numbers in every recorded `.npz`, not an error.
    #[test]
    fn the_order_layout_the_python_dtype_mirrors_is_unchanged() {
        use std::mem::{align_of, offset_of, size_of};

        use crate::types::Order;

        // `py-hftbacktest/hftbacktest/types.py::order_dtype`, `align=True`: itemsize 96,
        // alignment 8.
        assert_eq!(size_of::<Order>(), 96, "order_dtype itemsize is 96");
        assert_eq!(align_of::<Order>(), 8, "order_dtype alignment is 8");

        for (field, offset, dtype_name) in [
            (offset_of!(Order, qty), 0, "qty"),
            (offset_of!(Order, leaves_qty), 8, "leaves_qty"),
            (offset_of!(Order, exec_qty), 16, "exec_qty"),
            (offset_of!(Order, exec_price_tick), 24, "exec_price_tick"),
            (offset_of!(Order, price_tick), 32, "price_tick"),
            (offset_of!(Order, tick_size), 40, "tick_size"),
            (offset_of!(Order, exch_timestamp), 48, "exch_timestamp"),
            (offset_of!(Order, local_timestamp), 56, "local_timestamp"),
            (offset_of!(Order, order_id), 64, "order_id"),
            // `q` is a `Box<dyn AnyClone + Send>` — a 16-byte fat pointer, which the dtype
            // spells as the two opaque `u8` fields `_q1`/`_q2`.
            (offset_of!(Order, q), 72, "_q1/_q2"),
            (offset_of!(Order, maker), 88, "maker"),
            (offset_of!(Order, order_type), 89, "order_type"),
            (offset_of!(Order, req), 90, "req"),
            (offset_of!(Order, status), 91, "status"),
            (offset_of!(Order, side), 92, "side"),
            (offset_of!(Order, time_in_force), 93, "time_in_force"),
        ] {
            assert_eq!(
                field, offset,
                "Order::{dtype_name} moved; py-hftbacktest's order_dtype reads offset \
                 {offset} and would silently return a different field"
            );
        }
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
