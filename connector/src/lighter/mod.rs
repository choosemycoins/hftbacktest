//! Lighter (zkLighter) perpetuals backend — **Phase 1: public market data only**.
//!
//! Subscribes `order_book` and `trade` for every registered symbol and publishes kind-1
//! depth events and trade events. It holds no key, signs nothing, and **refuses every
//! order**; the order path is Phase 2 (design note
//! [`docs/design-lighter-connector.md`](../../../docs/design-lighter-connector.md) §5).
//!
//! Five facts about this venue shape the module, and every one of them fails silently:
//!
//! 1. **A subscription is asked for as `order_book/36` and every frame comes back as
//!    `order_book:36`** (§2.1). Routing on the request spelling leaves the connector
//!    subscribed, served, and publishing nothing, on a socket that answers pings.
//! 2. **Markets are integers.** `market_id` is the address, and the symbol → id map is a
//!    runtime fact read from `GET /api/v1/orderBooks` (§3.5). A symbol that does not
//!    resolve is refused **per symbol**, loudly and by name — all-or-nothing is the failure
//!    Hyperliquid had to roll back (design note §3.5, HL §10.2).
//! 3. **The book is a diff feed with one snapshot per subscription** (§2.2, §3.1). Deltas
//!    map straight onto kind-1 events, which are the only depth events `LiveBot` applies
//!    (`AGENTS.md` §4.1) — but the connector must still keep a **full mirror**, because a
//!    bot registering between snapshots would otherwise be handed a near-empty book and a
//!    `SnapshotComplete` to go with it. [`depth::DepthMirror::restate`] is that moment.
//! 4. **Breaks are detected by `begin_nonce`, and only by it** (§2.2). `offset` is
//!    API-server-local and *jumps backwards* on every resubscribe, so a connector that
//!    keys resync on it repairs itself into a rate limit. Repair is
//!    `unsubscribe` → `subscribe`: a bare duplicate subscribe is answered `30003 Already
//!    Subscribed` **and no snapshot follows**.
//! 5. **Exceeding the 200-client-messages-a-minute budget is not answered with an error —
//!    the connection is throttled** (§2.5), which from this side is a socket that goes
//!    quiet. So subscribes and repairs are spent against a budget rather than sent
//!    whenever they seem due.

use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use hftbacktest::types::{ErrorKind, LiveError, LiveEvent, Order, Status, Value};
use serde::Deserialize;
use thiserror::Error;
use tokio::{
    sync::{broadcast, broadcast::Sender, mpsc::UnboundedSender},
    task::JoinHandle,
};
use tracing::{error, info, warn};

use crate::{
    connector::{Connector, ConnectorBuilder, GetOrders, PublishEvent, SweepReason},
    lighter::public_stream::PublicStream,
};

pub mod depth;
#[cfg(test)]
pub mod fixtures;
pub mod msg;
pub mod public_stream;
pub mod rest;
pub mod trades;

#[derive(Error, Debug)]
pub enum LighterError {
    #[error("ConnectionInterrupted")]
    ConnectionInterrupted,
    #[error("ConnectionAbort: {0}")]
    ConnectionAbort(String),
    #[error("ConnectTimeout: no connection within {0:?}")]
    ConnectTimeout(std::time::Duration),
    #[error("WriteTimeout: the socket did not accept a write within {0:?}")]
    WriteTimeout(std::time::Duration),
    #[error("IdleTimeout: no frame of any kind for {0:?}")]
    IdleTimeout(std::time::Duration),
    /// A frame that is the venue's JSON but not a shape this backend can read.
    #[error("Frame: {0}")]
    Frame(String),
    /// The catalog says this symbol is not something that can be subscribed.
    #[error("UnknownSymbol: {0}")]
    UnknownSymbol(String),
    /// The catalog could not be read — as opposed to answering "no such symbol".
    #[error("CatalogUnavailable: {0}")]
    CatalogUnavailable(String),
    /// The venue acknowledged nothing for this symbol before the deadline. Its subscribe
    /// refusals name neither channel nor market, so this is the only way to notice (§3.6).
    #[error("SubscriptionUnacknowledged: {0}")]
    SubscriptionUnacknowledged(String),
    /// A symbol whose book has gone quiet on a healthy connection — which is what a
    /// throttled connection looks like from here (§2.5).
    #[error("SymbolSilent: {0}")]
    SymbolSilent(String),
    #[error("OrderNotSupported: this backend is Phase 1, market data only; orders are rejected")]
    OrderNotSupported,
    #[error("Serde: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Reqwest: {0}")]
    Reqwest(#[from] reqwest::Error),
    /// Boxed: `tungstenite::Error` is 136 bytes on its own, which every
    /// `Result<_, LighterError>` in this module would otherwise pay for.
    #[error("Tungstenite: {0}")]
    Tungstenite(Box<tokio_tungstenite::tungstenite::Error>),
    #[error("Config: {0:?}")]
    Config(#[from] toml::de::Error),
}

impl From<tokio_tungstenite::tungstenite::Error> for LighterError {
    fn from(error: tokio_tungstenite::tungstenite::Error) -> Self {
        Self::Tungstenite(Box::new(error))
    }
}

/// The most bytes an error message may carry to the bots.
///
/// A `LiveEvent` is bincode-encoded into a fixed `MAX_PAYLOAD_SIZE = 512` byte slice
/// (`hftbacktest/src/live/ipc/`), and an encode that does not fit propagates into a
/// `.unwrap()` under the connector's `exit(1)` panic hook — so an over-long message does
/// not fail the message, it **kills the connector** and takes every bot's market data with
/// it. `AGENTS.md` §2 names the ceiling; the Hyperliquid backend measured it (506
/// characters encode to exactly 512 bytes).
const MAX_ERROR_BYTES: usize = 400;

const TRUNCATION_MARK: &str = "… [truncated]";

impl LighterError {
    /// The error as it goes to the bots, bounded so publishing it cannot kill the connector.
    pub fn to_value(&self) -> Value {
        Value::String(clamp_message(self.to_string()))
    }

    /// Whether the venue answered "this symbol is not subscribable", as opposed to not
    /// answering at all. A verdict will not change within a connection; anything else must
    /// be asked about again, or the connection sits there subscribed to nothing.
    pub fn is_listing_verdict(&self) -> bool {
        matches!(self, Self::UnknownSymbol(_))
    }
}

/// Truncates a message to [`MAX_ERROR_BYTES`], on a character boundary.
fn clamp_message(message: String) -> String {
    if message.len() <= MAX_ERROR_BYTES {
        return message;
    }
    let mut end = MAX_ERROR_BYTES - TRUNCATION_MARK.len();
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{TRUNCATION_MARK}", &message[..end])
}

#[derive(Deserialize)]
pub struct Config {
    /// WebSocket endpoint. Mainnet `wss://mainnet.zklighter.elliot.ai/stream`.
    pub public_url: String,
    /// REST endpoint, used for `GET /api/v1/orderBooks` — the symbol → `market_id` map.
    /// Must be the same network as `public_url`: the catalogs differ by two orders of
    /// magnitude (228 markets against 5) and the values differ even where the ids agree.
    pub rest_url: String,
    /// Symbols to resolve and subscribe at startup, before any bot registers. Optional;
    /// a bot's own `register()` is added dynamically either way.
    #[serde(default)]
    pub symbols: Vec<String>,
}

type SharedSymbolSet = Arc<Mutex<HashSet<String>>>;

/// What a market-data-only connector reports: nothing, because it holds nothing.
///
/// Deliberately not a stub that pretends. [`Lighter`] refuses every order with
/// [`ErrorKind::OrderError`], so an empty list is the truth.
#[derive(Default)]
pub struct NoOrders;

impl GetOrders for NoOrders {
    fn orders(&self, _symbol: Option<String>) -> Vec<Order> {
        Vec::new()
    }
}

pub struct Lighter {
    config: Config,
    symbols: SharedSymbolSet,
    symbol_tx: Sender<String>,
    no_orders: Arc<Mutex<NoOrders>>,
    /// The stream task, kept so an orderly stop can end it and its `PublishEvent` sender
    /// can be dropped — which is what lets the publish task stop last. `AGENTS.md` §4.7.
    tasks: Vec<JoinHandle<()>>,
}

impl ConnectorBuilder for Lighter {
    type Error = LighterError;

    fn build_from(config: &str) -> Result<Self, Self::Error> {
        let config: Config = toml::from_str(config)?;
        let (symbol_tx, _) = broadcast::channel(500);
        let symbols: SharedSymbolSet = Default::default();
        {
            let mut set = symbols.lock().unwrap();
            for symbol in &config.symbols {
                set.insert(symbol.clone());
            }
        }
        warn!("The Lighter backend is Phase 1: it serves market data and refuses every order.");
        Ok(Lighter {
            config,
            symbols,
            symbol_tx,
            no_orders: Arc::new(Mutex::new(NoOrders)),
            tasks: Vec::new(),
        })
    }
}

impl Connector for Lighter {
    fn register(&mut self, symbol: String) {
        let inserted = {
            let mut symbols = self.symbols.lock().unwrap();
            symbols.insert(symbol.clone())
        };
        if inserted {
            info!(%symbol, "Registered an instrument.");
        }
        // Sent unconditionally, and only as a wake-up: the stream re-reads the shared set on
        // every connect and on every wake, so a receiver that missed this message — or
        // lagged, or was created after it — still ends up subscribed. `AGENTS.md` §4.2. A
        // send error means the stream task is not running yet, and the set is authoritative
        // in that case too, so it is not fatal.
        let _ = self.symbol_tx.send(symbol);
    }

    fn order_manager(&self) -> Arc<Mutex<dyn GetOrders + Send + 'static>> {
        self.no_orders.clone()
    }

    fn run(&mut self, ev_tx: UnboundedSender<PublishEvent>) {
        let mut public = PublicStream::new(
            self.config.public_url.clone(),
            self.config.rest_url.clone(),
            self.symbols.clone(),
            self.symbol_tx.subscribe(),
            ev_tx,
        );
        self.tasks.push(tokio::spawn(async move {
            public.run().await;
        }));
    }

    fn submit(&self, symbol: String, order: Order, ev_tx: UnboundedSender<PublishEvent>) {
        reject_order(&symbol, &order, ev_tx);
    }

    fn cancel(&self, symbol: String, order: Order, ev_tx: UnboundedSender<PublishEvent>) {
        reject_order(&symbol, &order, ev_tx);
    }

    /// Nothing to sweep: this backend has never placed an order.
    ///
    /// A documented no-op rather than a forgotten stub — `AGENTS.md` §4.7 records which
    /// backends are in that state and why. It is **not** the same gap the other three have:
    /// theirs can be holding live orders, this one cannot, because [`Self::submit`] refuses
    /// every request. When Phase 2 arms the order path this must become a real sweep built
    /// from the venue's own open orders (`CancelAllOrders`, tx type 16 — design note §3.4),
    /// never from a map this process owns.
    fn sweep(
        &self,
        symbols: Vec<String>,
        reason: SweepReason,
        _ev_tx: UnboundedSender<PublishEvent>,
    ) -> Option<JoinHandle<()>> {
        info!(
            ?reason,
            ?symbols,
            "The Lighter backend is market-data only, so it has nothing resting to sweep."
        );
        None
    }

    /// Ends the stream task, so its sender is dropped and the publish task can see the
    /// channel close and stop **last**. Without it an orderly stop waits out
    /// `shutdown.grace_ms` in full — `AGENTS.md` §4.7.
    fn shutdown(&mut self) {
        let count = self.tasks.len();
        for task in self.tasks.drain(..) {
            task.abort();
        }
        info!(
            tasks = count,
            "Wound down the Lighter streams for an orderly stop."
        );
    }
}

/// Fails an order request loudly, and takes the order back out of the bot's state.
///
/// The error alone is not enough. `LiveBot::submit_order` inserts the order into its own map
/// as `Status::New` **before** the request leaves, and nothing in `process_event` removes it
/// on an `Error` event — the handler is called and that is all. So a bot that mistakenly
/// points at this backend would be left holding a live order that exists nowhere, and
/// `submit_order` with that id would return `OrderIdExist` for the rest of the process's
/// life. Every other backend answers an unsendable request by publishing the order back as
/// `Status::Expired`; so does this one, and it goes **first**, so the bot's state is clean
/// before the error handler — which may abort the `elapse` — ever runs.
fn reject_order(symbol: &str, order: &Order, ev_tx: UnboundedSender<PublishEvent>) {
    error!(
        %symbol,
        order_id = order.order_id,
        "The Lighter backend is Phase 1, market data only; the order was rejected."
    );
    let mut order = order.clone();
    order.req = Status::None;
    order.status = Status::Expired;
    ev_tx
        .send(PublishEvent::LiveEvent(LiveEvent::Order {
            symbol: symbol.to_string(),
            order,
        }))
        .unwrap();
    ev_tx
        .send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
            ErrorKind::OrderError,
            LighterError::OrderNotSupported.to_value(),
        ))))
        .unwrap();
}

/// Surfaces a connector-side error to every bot.
pub(crate) fn publish_error(
    ev_tx: &UnboundedSender<PublishEvent>,
    kind: ErrorKind,
    error: &LighterError,
) {
    ev_tx
        .send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
            kind,
            error.to_value(),
        ))))
        .unwrap();
}

#[cfg(test)]
mod tests {
    use hftbacktest::types::{
        ErrorKind,
        LiveError,
        LiveEvent,
        OrdType,
        Order,
        Side,
        Status,
        TimeInForce,
        Value,
    };
    use tokio::sync::mpsc::unbounded_channel;

    use crate::{
        connector::{Connector, ConnectorBuilder, PublishEvent},
        lighter::{Config, Lighter, LighterError, MAX_ERROR_BYTES, TRUNCATION_MARK, reject_order},
    };

    /// The shipped example must parse with the code that reads it. `AGENTS.md` §6 requires
    /// every config field to appear in `connector/examples/*.toml`, and an example that has
    /// drifted from the struct is worse than none: it is copied verbatim by whoever deploys
    /// this.
    #[test]
    fn the_shipped_example_config_parses() {
        let config: Config = toml::from_str(include_str!("../../examples/lighter.toml")).unwrap();

        assert!(
            config.public_url.starts_with("wss://"),
            "{}",
            config.public_url
        );
        assert!(
            config.rest_url.starts_with("https://"),
            "{}",
            config.rest_url
        );
        assert!(!config.symbols.is_empty());
        // Symbols are the venue's own bare names. A quote suffix carried over from another
        // venue is the standard mistake, and the example must not teach it.
        for symbol in &config.symbols {
            assert!(
                !symbol.contains("USDT") && !symbol.contains('/'),
                "Lighter perps are bare base assets: {symbol}"
            );
        }
    }

    /// Configured symbols are subscribed without waiting for a bot, which is what makes the
    /// feed — and the per-symbol tick and lot the catalog implies — observable on its own.
    #[test]
    fn configured_symbols_seed_the_symbol_set() {
        let mut connector = Lighter::build_from(
            r#"
            public_url = "wss://mainnet.zklighter.elliot.ai/stream"
            rest_url = "https://mainnet.zklighter.elliot.ai"
            symbols = ["CRV", "HYPE"]
            "#,
        )
        .unwrap();

        {
            let symbols = connector.symbols.lock().unwrap();
            assert!(symbols.contains("CRV"));
            assert!(symbols.contains("HYPE"));
        }

        // And a registration adds to the same authoritative set — the broadcast is only a
        // wake-up (`AGENTS.md` §4.2).
        connector.register("SUI".to_string());
        assert!(connector.symbols.lock().unwrap().contains("SUI"));
    }

    fn an_order() -> Order {
        Order::new(
            7,
            100,
            0.1,
            1.0,
            Side::Buy,
            OrdType::Limit,
            TimeInForce::GTC,
        )
    }

    /// **Phase 1 refuses orders, and refusing means taking the order back.**
    ///
    /// `LiveBot::submit_order` inserts the order as `Status::New` before the request leaves
    /// and never removes it on an `Error` event, so without the `Expired` the bot holds a
    /// live order that exists nowhere and burns that `order_id` — every later
    /// `submit_order` with it returns `OrderIdExist` — for the life of the process.
    #[test]
    fn a_rejected_order_is_expired_back_to_the_bot_before_the_error() {
        let (tx, mut rx) = unbounded_channel();
        reject_order("CRV", &an_order(), tx);

        let PublishEvent::LiveEvent(LiveEvent::Order { symbol, order }) = rx.try_recv().unwrap()
        else {
            panic!("the order must come back first, before the error handler can abort");
        };
        assert_eq!(symbol, "CRV");
        assert_eq!(order.order_id, 7);
        assert_eq!(order.status, Status::Expired);
        assert_eq!(order.req, Status::None);

        let PublishEvent::LiveEvent(LiveEvent::Error(error)) = rx.try_recv().unwrap() else {
            panic!("the rejection must still be reported");
        };
        assert_eq!(error.kind, ErrorKind::OrderError);
    }

    /// Both order verbs refuse, through the same path — a cancel that answered `Ok` and did
    /// nothing would leave the bot waiting for a response that can never arrive.
    #[test]
    fn cancel_is_refused_exactly_as_submit_is() {
        let connector = Lighter::build_from(
            r#"
            public_url = "wss://mainnet.zklighter.elliot.ai/stream"
            rest_url = "https://mainnet.zklighter.elliot.ai"
            "#,
        )
        .unwrap();

        for verb in ["submit", "cancel"] {
            let (tx, mut rx) = unbounded_channel();
            match verb {
                "submit" => connector.submit("CRV".to_string(), an_order(), tx),
                _ => connector.cancel("CRV".to_string(), an_order(), tx),
            }
            let PublishEvent::LiveEvent(LiveEvent::Order { order, .. }) = rx.try_recv().unwrap()
            else {
                panic!("{verb}: the order must come back");
            };
            assert_eq!(order.status, Status::Expired);
            let PublishEvent::LiveEvent(LiveEvent::Error(error)) = rx.try_recv().unwrap() else {
                panic!("{verb}: the rejection must be reported");
            };
            assert_eq!(error.kind, ErrorKind::OrderError);
        }
    }

    /// `hftbacktest/src/live/ipc/config.rs`. Repeated here rather than imported because the
    /// module holding it is private; the assertion below is what keeps the copy honest.
    const MAX_PAYLOAD_SIZE: usize = 512;

    fn encoded_len(message: &str) -> Result<usize, String> {
        let event = LiveEvent::Error(LiveError::with(
            ErrorKind::CriticalConnectionError,
            Value::String(message.to_string()),
        ));
        let mut buffer = [0u8; MAX_PAYLOAD_SIZE];
        bincode::encode_into_slice(&event, &mut buffer, bincode::config::standard())
            .map_err(|error| error.to_string())
    }

    /// An error message that does not fit the IPC payload does not fail the message — it
    /// **kills the connector**, because the encode error propagates into a `.unwrap()` under
    /// the process-wide `exit(1)` panic hook, and the bot re-registers the same symbol on
    /// restart. This backend composes its own error text (a symbol name, a catalog
    /// complaint), so it can reach the ceiling. `AGENTS.md` §2.
    #[test]
    fn an_error_published_to_the_bots_always_fits_the_ipc_payload() {
        // The uncapped case, reproduced.
        assert!(encoded_len(&"x".repeat(780)).is_err());

        for length in [0usize, 1, 399, 400, 401, 780, 4096] {
            let value = LighterError::UnknownSymbol("x".repeat(length)).to_value();
            let Value::String(message) = &value else {
                panic!("expected a string");
            };
            let encoded = encoded_len(message).unwrap_or_else(|error| {
                panic!("a {length}-character error did not fit the payload: {error}")
            });
            assert!(encoded <= MAX_PAYLOAD_SIZE);
        }
    }

    /// Truncation must not split a character: a message is a `String` on the wire, and half
    /// a UTF-8 sequence is not one.
    #[test]
    fn truncation_lands_on_a_character_boundary() {
        let value = LighterError::UnknownSymbol("日".repeat(400)).to_value();
        let Value::String(message) = &value else {
            panic!("expected a string");
        };
        assert!(message.len() <= MAX_ERROR_BYTES, "{}", message.len());
        assert!(message.ends_with(TRUNCATION_MARK), "{message}");
        encoded_len(message).unwrap();
    }
}
