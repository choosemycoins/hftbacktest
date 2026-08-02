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

/// What a sweep actually did — as opposed to whether its task merely finished.
///
/// The whole point of Finding 2: a `JoinHandle<()>` that resolves says the sweep *ran*, not
/// that anything was *cancelled*, so an orderly stop could exit 0 with a grid still resting.
/// The exit code (`crate::supervision::exit_code`) keys on this, and only [`Self::Failed`] is
/// a non-zero stop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SweepOutcome {
    /// The venue confirmed every cancel the sweep asked for — or there was nothing to cancel.
    /// Nothing this connector placed can still be resting because of it.
    Cancelled,
    /// The sweep ran but could not confirm every cancel: an unknown asset index, an
    /// open-orders query that failed, a POST that failed or a `sendTx` that was refused, or a
    /// confirmed count short of what was asked. Fail closed — orders **may** still be resting,
    /// so a stop that ends here is not clean.
    Failed,
    /// The backend has an order path but no sweep written for it (`AGENTS.md` §4.7:
    /// bybit/binance). Documented, non-fatal: orders it placed may rest, but its own
    /// connect-time cancel (bybit) or an operator restart is what clears them, and turning
    /// this into a non-zero stop would make every stop on those backends look like a failure.
    NotImplemented,
}

impl SweepOutcome {
    /// Folds one confirmation per cancel a sweep attempted into a verdict. Fail closed: the
    /// result is [`Self::Cancelled`] only when **every** confirmation is `true` (an empty
    /// iterator included — nothing asked for is nothing left resting); a single `false` is
    /// [`Self::Failed`], because that unit may still be on the venue.
    pub fn from_confirmations(confirmations: impl IntoIterator<Item = bool>) -> Self {
        if confirmations.into_iter().all(|confirmed| confirmed) {
            Self::Cancelled
        } else {
            Self::Failed
        }
    }
}

/// What applying one venue order-lifecycle update produced. Returned by the per-backend
/// `apply_order_update`, shared so HL and Lighter make the **same** fail-closed choice on an
/// unrecognised status (Finding 3).
///
/// The distinction that matters is [`Self::Unrecognised`]. A venue status this connector does
/// not recognise (`Status::Unsupported`) must **not** transition the order to a terminal state
/// that removes it from the manager: an unknown status may mean the order is still resting, and
/// dropping it lets the strategy see the price level as free and place a *second* order — a
/// double position (`AGENTS.md` §1.1, fail closed). So the manager keeps the order exactly as
/// it is and returns this; the caller escalates (a loud log plus a
/// [`hftbacktest::types::ErrorKind::CriticalConnectionError`]) so the venue's own truth —
/// reconciliation — resolves the order's real fate.
pub enum ApplyOutcome<T> {
    /// Apply this to the bot: the order is still resting, or it reached a **recognised**
    /// terminal (canceled/filled) and was removed — either way the bot must learn it.
    Publish(T),
    /// Nothing to do: not this connector's order, a stale (out-of-order) frame, or a duplicate
    /// of a terminal already applied.
    Ignored,
    /// The venue reported a status this connector does not recognise. The order was **kept**
    /// untouched; the caller escalates so reconciliation resolves it. See the type docs.
    Unrecognised,
}

impl<T> ApplyOutcome<T> {
    /// The payload to publish, if this update produced one. `None` for [`Self::Ignored`] and
    /// [`Self::Unrecognised`] alike — neither publishes an order to the bot.
    pub fn published(self) -> Option<T> {
        match self {
            ApplyOutcome::Publish(payload) => Some(payload),
            ApplyOutcome::Ignored | ApplyOutcome::Unrecognised => None,
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
    /// **Returns the spawned task, resolving to what the sweep actually did** ([`SweepOutcome`]
    /// — Finding 2). An orderly stop waits for it and turns [`SweepOutcome::Failed`] into a
    /// non-zero exit: a `JoinHandle<()>` that merely resolved said the task *ran*, not that
    /// anything was *cancelled*, so a stop could exit 0 with a grid still resting.
    ///
    /// The `Option` and the outcome answer different questions:
    ///
    /// * `None` — **no order path at all**, so nothing this connector placed can be resting
    ///   (Hyperliquid with no API wallet; Lighter market-data only). There is nothing to wait
    ///   for and nothing to fail.
    /// * `Some(_ -> `[`SweepOutcome::NotImplemented`]`)` — the backend **has** an order path but
    ///   no sweep written for it (bybit/binance, `AGENTS.md` §4.7). Orders it placed may rest;
    ///   this is a documented, non-fatal gap, not a failure.
    /// * `Some(_ -> `[`SweepOutcome::Cancelled`]` / `[`SweepOutcome::Failed`]`)` — the sweep ran;
    ///   `Cancelled` confirmed every cancel (or found nothing), `Failed` could not, so orders
    ///   may still be resting.
    ///
    /// **Hold `tx` until the cancels have landed**, and let the task own it: the publish
    /// task drains until every sender is gone, so this sender is also what keeps the
    /// connector alive long enough for the sweep's confirmations to reach the bots. A backend
    /// with no sweep to run should drop `tx` promptly (a trivial task that drops it and returns
    /// `NotImplemented`) so it does not hold the channel open through the drain.
    ///
    /// **The unit of cancellation is a symbol, not a bot.** No venue records which bot asked
    /// for an order — [`Self::submit`] is not told — so a sweep clears every order the
    /// *account* holds on that symbol. The supervision layer knows this and only ever passes
    /// symbols no live bot is quoting (`supervision::DeadBot`); a backend must not widen
    /// that, for instance by sweeping everything it has registered.
    fn sweep(
        &self,
        symbols: Vec<String>,
        reason: SweepReason,
        tx: UnboundedSender<PublishEvent>,
    ) -> Option<JoinHandle<SweepOutcome>>;

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

#[cfg(test)]
mod tests {
    use super::SweepOutcome;

    /// **Every asked cancel confirmed — or nothing to cancel — is the only success.** A sweep
    /// aggregates one boolean per unit it tried to cancel; `from_confirmations` folds them, and
    /// the fold has to be fail-closed: any unconfirmed unit means an order may still be resting.
    #[test]
    fn from_confirmations_is_cancelled_only_when_every_confirmation_is_true() {
        assert_eq!(
            SweepOutcome::from_confirmations([true, true, true]),
            SweepOutcome::Cancelled
        );
        // Nothing to cancel is success: the venue holds nothing, so nothing can rest.
        assert_eq!(
            SweepOutcome::from_confirmations(std::iter::empty::<bool>()),
            SweepOutcome::Cancelled
        );
        // One unconfirmed unit is enough to fail the whole sweep — orders may still be resting.
        assert_eq!(
            SweepOutcome::from_confirmations([true, false, true]),
            SweepOutcome::Failed
        );
        assert_eq!(
            SweepOutcome::from_confirmations([false]),
            SweepOutcome::Failed
        );
    }
}
