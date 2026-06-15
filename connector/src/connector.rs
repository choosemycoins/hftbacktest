use std::{
    fmt::Debug,
    sync::{Arc, Mutex},
};

use hftbacktest::types::{LiveEvent, Order, ReconcileScope, SpotOrderAction};
use tokio::sync::mpsc::UnboundedSender;

/// A message will be received by the publisher thread and then published to the bots.
pub enum PublishEvent {
    BatchStart(u64),
    BatchEnd(u64),
    LiveEvent(LiveEvent),
    /// Targeted live event delivered only to the bot identified by `id` (R-M1a §0 D-a).
    ///
    /// Unlike [`PublishEvent::LiveEvent`] (which broadcasts `TO_ALL`), reconcile frames must be
    /// routed back to the single requesting bot so per-`id` `BatchStart`/`BatchEnd` brackets stay
    /// consistent. The publisher arm sends `bot_tx.send(id, &event)`.
    LiveEventTo {
        id: u64,
        event: LiveEvent,
    },
    RegisterInstrument {
        id: u64,
        symbol: String,
        tick_size: f64,
        lot_size: f64,
    },
}

/// Provides a build function for the Connector.
pub trait ConnectorBuilder {
    type Error: Debug;

    fn build_from(config: &str) -> Result<Self, Self::Error>
    where
        Self: Sized;
}

/// Provides an interface for connecting with an exchange or broker for a live bot.
pub trait Connector {
    /// Registers an instrument to be traded through this connector.
    ///
    /// `tick_size`/`lot_size` come from the bot's `LiveRequest::RegisterInstrument` and let the
    /// connector encode prices/quantities faithfully in read-only paths (e.g. the reconcile
    /// REST snapshot must produce the same `price_tick = round(price / tick_size)` the bot caches,
    /// R-M1a interop fix). Connectors that do not need them may ignore the parameters.
    fn register(&mut self, symbol: String, tick_size: f64, lot_size: f64);

    /// Returns an [`OrderManager`].
    fn order_manager(&self) -> Arc<Mutex<dyn GetOrders + Send + 'static>>;

    /// Runs the connector, establishing the connection and preparing to exchange information such
    /// as data feed and orders. This method should not block, and any response should be returned
    /// through the channel using [`PublishEvent`]. The returned error should not be related to the
    /// exchange; instead, it should indicate a connector internal error.
    fn run(&mut self, tx: UnboundedSender<PublishEvent>);

    /// Submits a new order. This method should not block, and the response should be returned
    /// through the channel using [`PublishEvent`]. The returned error should not be related to the
    /// exchange; instead, it should indicate a connector internal error.
    fn submit(&self, symbol: String, order: Order, tx: UnboundedSender<PublishEvent>);

    /// Cancels an open order. This method should not block, and the response should be returned
    /// through the channel using [`PublishEvent`]. The returned error should not be related to the
    /// exchange; instead, it should indicate a connector internal error.
    fn cancel(&self, symbol: String, order: Order, tx: UnboundedSender<PublishEvent>);

    /// Pulls a fresh REST snapshot of account state (R-M1a) and streams it back to the requesting
    /// bot as a `BatchStart`-bracketed group of [`LiveEvent::Reconcile`] frames, delivered targeted
    /// to that bot via [`PublishEvent::LiveEventTo`].
    ///
    /// * `bot_id` — the routing id of the requesting bot (from the dispatch loop), used for the
    ///   `BatchStart`/`BatchEnd` brackets and `LiveEventTo` so the reply reaches only that bot.
    /// * `request_id` — the bot-chosen reconcile id, echoed in `Begin`/`End` for completeness
    ///   matching (distinct from `bot_id`).
    ///
    /// This method must NOT block the synchronous dispatch loop; connectors should spawn the work.
    /// Results — including fail-closed `End { ok: false, .. }` terminators on any error — are
    /// returned through the channel. The default implementation is a no-op for connectors that do
    /// not yet support reconcile.
    fn reconcile(
        &self,
        _bot_id: u64,
        _symbol: String,
        _request_id: u64,
        _scope: ReconcileScope,
        _tx: UnboundedSender<PublishEvent>,
    ) {
        // Default: no-op. Non-Bybit connectors do not implement reconcile yet.
    }

    /// Performs a Bybit **spot** order op (place/cancel/status, B-M1a) via REST and streams the
    /// result back to the requesting bot as a `BatchStart`-bracketed group of
    /// [`LiveEvent::SpotOrderReply`] frames, delivered targeted via [`PublishEvent::LiveEventTo`].
    /// Clones the [`reconcile`](Self::reconcile) contract.
    ///
    /// * `bot_id` — the routing id of the requesting bot (from the dispatch loop), used for the
    ///   `BatchStart`/`BatchEnd` brackets and `LiveEventTo` so the reply reaches only that bot.
    /// * `request_id` — the bot-chosen op id, echoed in `Begin`/`End` for completeness matching
    ///   (distinct from `bot_id`).
    /// * `action` — the spot op (place/cancel/status), correlated by `order_link_id` (D-b).
    ///
    /// This method must NOT block the synchronous dispatch loop; connectors should spawn the work.
    /// Results — including fail-closed `End { ok: false, .. }` terminators on any error — are
    /// returned through the channel. The default implementation is a no-op for connectors that do
    /// not yet support spot orders.
    fn spot_order(
        &self,
        _bot_id: u64,
        _symbol: String,
        _request_id: u64,
        _action: SpotOrderAction,
        _tx: UnboundedSender<PublishEvent>,
    ) {
        // Default: no-op. Non-Bybit connectors do not implement spot orders yet.
    }
}

/// Provides `orders` method to get the current working orders.
pub trait GetOrders {
    fn orders(&self, symbol: Option<String>) -> Vec<Order>;
}
