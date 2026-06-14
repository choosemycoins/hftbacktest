use std::collections::HashMap;

pub use bot::{BotError, LiveBot, LiveBotBuilder};
pub use recorder::LoggingRecorder;

use crate::{
    prelude::StateValues,
    types::{
        AccountTotals,
        Event,
        Order,
        OrderId,
        ReconcileEndpoint,
        ReconcileOutcome,
        ReconcileWalletCoin,
    },
};

mod bot;
pub mod ipc;
mod recorder;

/// Provides asset information for internal use.
pub struct Instrument<MD> {
    connector_name: String,
    symbol: String,
    tick_size: f64,
    lot_size: f64,
    depth: MD,
    last_trades: Vec<Event>,
    orders: HashMap<OrderId, Order>,
    last_feed_latency: Option<(i64, i64)>,
    last_order_latency: Option<(i64, i64, i64)>,
    state: StateValues,
    /// Set to `true` after the connector delivers `LiveEvent::SnapshotComplete` for this
    /// instrument. Starts as `false` at construction, which means a freshly built bot will not
    /// report the snapshot as ready until the first marker arrives — this is the signal myhft
    /// uses to safely begin placing orders after startup/restart.
    snapshot_ready: bool,
    /// Most recent completed reconcile outcome (R-M1a). `None` until a matching `Begin`/`End`
    /// reconcile stream arrives — fail-closed default, so an interrupted reconcile is never
    /// treated as authoritative. Held separately from `orders`/`state` (the WS-cached view).
    last_reconcile: Option<ReconcileOutcome>,
    /// In-flight reconcile rows accumulated between `Begin` and `End` (R-M1a, §4). `Some` only
    /// while a reconcile stream is open; finalized into `last_reconcile` on a matching `End` and
    /// dropped otherwise (fail-closed). Never starts populated.
    reconcile_accum: Option<ReconcileAccum>,
}

/// Per-instrument accumulator for the rows of an in-flight reconcile stream (R-M1a, §4). A fresh
/// instance is created on `ReconcileFrame::Begin`; rows are pushed as `Account`/`Position`/
/// `OpenOrder`/`WalletCoin` frames arrive; `finish` consumes it on `End` into a [`ReconcileOutcome`].
/// Kept bot-side only (not IPC-encoded).
pub(crate) struct ReconcileAccum {
    request_id: u64,
    snapshot_ts_ns: i64,
    account: Option<AccountTotals>,
    positions: Vec<f64>,
    open_orders: Vec<Order>,
    wallet_coins: Vec<ReconcileWalletCoin>,
}

impl ReconcileAccum {
    /// Opens a fresh accumulator for the given reconcile request (from a `Begin` frame).
    pub(crate) fn new(request_id: u64, snapshot_ts_ns: i64) -> Self {
        Self {
            request_id,
            snapshot_ts_ns,
            account: None,
            positions: Vec::new(),
            open_orders: Vec::new(),
            wallet_coins: Vec::new(),
        }
    }

    /// `request_id` from the opening `Begin`, matched against the terminating `End`.
    pub(crate) fn request_id(&self) -> u64 {
        self.request_id
    }

    /// Records the single account-level totals frame (`Account`).
    pub(crate) fn set_account(&mut self, account: AccountTotals) {
        self.account = Some(account);
    }

    /// Pushes a signed position quantity (`Position`).
    pub(crate) fn push_position(&mut self, qty: f64) {
        self.positions.push(qty);
    }

    /// Pushes one open order (`OpenOrder`).
    pub(crate) fn push_open_order(&mut self, order: Order) {
        self.open_orders.push(order);
    }

    /// Pushes one per-coin wallet balance (`WalletCoin`).
    pub(crate) fn push_wallet_coin(&mut self, coin: ReconcileWalletCoin) {
        self.wallet_coins.push(coin);
    }

    /// Consumes the accumulated rows plus the terminating `End` fields into a [`ReconcileOutcome`].
    /// Caller has already verified `request_id` matches `Begin` (fail-closed completeness, §4).
    pub(crate) fn finish(
        self,
        ok: bool,
        ret_code: Option<i64>,
        ret_msg: String,
        endpoint: ReconcileEndpoint,
        http_status: u16,
    ) -> ReconcileOutcome {
        ReconcileOutcome {
            request_id: self.request_id,
            snapshot_ts_ns: self.snapshot_ts_ns,
            account: self.account,
            positions: self.positions,
            open_orders: self.open_orders,
            wallet_coins: self.wallet_coins,
            ok,
            ret_code,
            ret_msg,
            endpoint,
            http_status,
        }
    }
}

impl<MD> Instrument<MD> {
    /// * `connector_name` - Name of the [`Connector`], which is registered by
    ///   [`register()`](`LiveBotBuilder::register()`), through which this asset will be traded.
    /// * `symbol` - Symbol of the asset. You need to check with the [`Connector`] which symbology
    ///   is used.
    /// * `tick_size` - The minimum price fluctuation.
    /// * `lot_size` -  The minimum trade size.
    /// * `depth` -  The market depth.
    pub fn new(
        connector_name: &str,
        symbol: &str,
        tick_size: f64,
        lot_size: f64,
        depth: MD,
        last_trades_capacity: usize,
    ) -> Self {
        Self {
            connector_name: connector_name.to_string(),
            symbol: symbol.to_string(),
            tick_size,
            lot_size,
            depth,
            last_trades: Vec::with_capacity(last_trades_capacity),
            orders: Default::default(),
            last_feed_latency: None,
            last_order_latency: None,
            state: Default::default(),
            snapshot_ready: false,
            last_reconcile: None,
            reconcile_accum: None,
        }
    }
}
