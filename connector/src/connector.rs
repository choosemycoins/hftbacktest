use std::{
    fmt::Debug,
    sync::{Arc, Mutex},
};

use hftbacktest::types::{LiveEvent, Order};
use tokio::{sync::mpsc::UnboundedSender, task::JoinHandle};

/// A message will be received by the publisher thread and then published to the bots.
pub enum PublishEvent {
    BatchStart(u64),
    BatchEnd(u64),
    LiveEvent(LiveEvent),
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

/// Why the generic supervision layer asked a backend to cancel its resting orders.
///
/// Carried into the backend's log line, because the two cases read very differently to
/// whoever finds them: one says a bot died under a healthy connector, the other says the
/// connector itself is going away.
#[derive(Clone, Copy, Debug)]
pub enum SweepReason {
    /// A bot that had been heartbeating went silent for longer than the configured window.
    /// Its orders are still resting on the venue and nothing is managing them.
    BotDied { id: u64 },
    /// This connector is stopping. Anything it leaves resting is unattended until whatever
    /// replaces it has connected and reconciled.
    ConnectorStopping,
}

impl SweepReason {
    /// The bot this sweep is on behalf of, when it is on behalf of one.
    ///
    /// Worth a structured log field rather than only the `Debug` rendering: with several
    /// bots on one connector, "every sweep for bot 12345" is the question an operator asks
    /// after a restart, and it is not answerable from a formatted blob.
    pub fn bot_id(&self) -> Option<u64> {
        match self {
            Self::BotDied { id } => Some(*id),
            Self::ConnectorStopping => None,
        }
    }
}

/// Provides an interface for connecting with an exchange or broker for a live bot.
pub trait Connector {
    /// Registers an instrument to be traded through this connector.
    fn register(&mut self, symbol: String);

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

    /// Cancels everything this connector has resting on the venue for `symbols`, because
    /// nobody is managing it any more.
    ///
    /// Called from the generic supervision layer (`crate::supervision`) — on a bot's death,
    /// and on an orderly stop. Must not block: spawn, and report through `tx` as
    /// [`Self::cancel`] does.
    ///
    /// **Returns the spawned task**, so that an orderly stop can wait for the cancels to
    /// land rather than for a generic deadline to expire. Without it the stop had no way to
    /// tell "the sweep finished" from "a backend that never drops its senders gave up
    /// waiting", and it exited 0 either way — including when it had cut the sweep off
    /// mid-POST. `None` means nothing was started and there is nothing to wait for.
    ///
    /// **Hold `tx` until the cancels have landed**, and let the task own it: the publish
    /// task drains until every sender is gone, so this sender is also what keeps the
    /// connector alive long enough for the sweep's confirmations to reach the bots.
    ///
    /// **The unit of cancellation is a symbol, not a bot.** No venue records which bot asked
    /// for an order — [`Self::submit`] is not told — so a sweep clears every order the
    /// *account* holds on that symbol. The supervision layer knows this and only ever passes
    /// symbols no live bot is quoting (`supervision::DeadBot`); a backend must not widen
    /// that, for instance by sweeping everything it has registered.
    ///
    /// A backend with no order path, or none written yet, should say so and return `None`;
    /// the supervision layer treats an unimplemented sweep as a documented gap rather than
    /// an error, and `AGENTS.md` §4.7 records which backends are in it.
    fn sweep(
        &self,
        symbols: Vec<String>,
        reason: SweepReason,
        tx: UnboundedSender<PublishEvent>,
    ) -> Option<JoinHandle<()>>;

    /// Winds the backend's own tasks down for an orderly stop, dropping every
    /// [`PublishEvent`] sender they hold. Must not block.
    ///
    /// This is what lets the publish task observe that its channel has closed and stop
    /// **last**, which is the whole of the shutdown fix — see `crate::supervision`. A
    /// backend that does not implement it is not broken: the drain falls back to its grace
    /// deadline and the process still exits 0, just after the wait.
    ///
    /// Called after [`Self::sweep`], so a sweep in flight is not cancelled by it.
    fn shutdown(&mut self);
}

/// Provides `orders` method to get the current working orders.
pub trait GetOrders {
    fn orders(&self, symbol: Option<String>) -> Vec<Order>;
}
