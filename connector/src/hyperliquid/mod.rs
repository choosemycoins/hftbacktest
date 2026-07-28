//! Hyperliquid perpetuals backend — **Phase 1: public market data only**.
//!
//! What this backend does today: subscribes to `bbo`, `l2Book` and `trades` for every
//! registered coin, and turns them into `LiveEvent::Feed` events for the bot. It signs
//! nothing, holds no key, and rejects every order request (see [`Hyperliquid::submit`]).
//!
//! Design: [`docs/design-hyperliquid-connector.md`](../../../docs/design-hyperliquid-connector.md).
//!
//! The two facts that shape the whole module:
//!
//! 1. **Hyperliquid has no incremental depth channel.** Every `l2Book` message is a
//!    complete top-N snapshot with no sequence number, and `LiveBot` silently discards
//!    `DEPTH_SNAPSHOT_EVENT` / `DEPTH_BBO_EVENT` (`AGENTS.md` §4.1/§4.1a). The backend
//!    therefore keeps its own mirror of the book and synthesises kind-1 delta events —
//!    [`depth::DepthMirror`].
//! 2. **Subscribing to a coin the venue does not know closes the entire WebSocket**, with
//!    no error frame and no close reason, taking every other subscription with it.
//!    Measured against testnet on 2026-07-28. Coins are therefore validated against
//!    `/info meta` before a subscribe frame is ever written — [`rest`].

use std::{
    collections::HashSet,
    num::ParseFloatError,
    sync::{Arc, Mutex},
};

use hftbacktest::types::{ErrorKind, LiveError, LiveEvent, Order, Status, Value};
use serde::Deserialize;
use thiserror::Error;
use tokio::sync::{broadcast, broadcast::Sender, mpsc::UnboundedSender};
use tracing::{error, info};

use crate::{
    connector::{Connector, ConnectorBuilder, GetOrders, PublishEvent},
    hyperliquid::public_stream::PublicStream,
};

pub mod depth;
#[cfg(test)]
mod fixtures;
pub mod msg;
pub mod public_stream;
pub mod rest;
pub mod trades;

/// Hyperliquid's perp price rule: a price may carry at most `MAX_DECIMALS - szDecimals`
/// decimal places (and at most 5 significant figures, which only constrains order
/// submission — Phase 2).
pub const MAX_DECIMALS: u32 = 6;

#[derive(Error, Debug)]
pub enum HyperliquidError {
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
    #[error("UnknownSymbol: {0}")]
    UnknownSymbol(String),
    #[error("UniverseError: {0}")]
    UniverseError(String),
    /// The venue did not answer the universe question — as opposed to answering "no".
    #[error("UniverseUnavailable: {0}")]
    UniverseUnavailable(String),
    #[error("ResolveTimeout: the coin universe did not arrive within {0:?}")]
    ResolveTimeout(std::time::Duration),
    #[error("OrderNotSupported: this backend is market-data only (Phase 1)")]
    OrderNotSupported,
    #[error("InvalidPxQty: {0}")]
    InvalidPxQty(#[from] ParseFloatError),
    #[error("Serde: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Reqwest: {0}")]
    Reqwest(#[from] reqwest::Error),
    /// Boxed: `tungstenite::Error` is 136 bytes on its own, which would make every
    /// `Result<_, HyperliquidError>` in this module pay for it.
    #[error("Tungstenite: {0}")]
    Tungstenite(Box<tokio_tungstenite::tungstenite::Error>),
    #[error("Config: {0:?}")]
    Config(#[from] toml::de::Error),
}

impl From<tokio_tungstenite::tungstenite::Error> for HyperliquidError {
    fn from(error: tokio_tungstenite::tungstenite::Error) -> Self {
        Self::Tungstenite(Box::new(error))
    }
}

/// The most bytes an error message may carry to the bots.
///
/// A `LiveEvent` is bincode-encoded into a fixed `MAX_PAYLOAD_SIZE = 512` byte slice
/// (`hftbacktest/src/live/ipc/`), and an encode that does not fit returns an error that
/// `run_publish_task` propagates into a `.unwrap()` under the connector's `exit(1)` panic
/// hook — so an over-long message does not fail the message, it **kills the connector**,
/// taking every bot's market data with it, and does it again on every restart because the
/// bot re-registers the same symbol. `AGENTS.md` §2 names this ceiling.
///
/// Measured against this exact wire type: a 506-character message encodes to exactly 512
/// bytes, a 510-character one does not fit. This backend is the first to compose its own
/// error text rather than relay the venue's, so it is the first that can reach it. Counted
/// in bytes, not characters, because the encoding is UTF-8.
const MAX_ERROR_BYTES: usize = 400;

const TRUNCATION_MARK: &str = "… [truncated]";

impl HyperliquidError {
    /// The error as it goes to the bots, bounded so that publishing it cannot kill the
    /// connector. See [`MAX_ERROR_BYTES`].
    pub fn to_value(&self) -> Value {
        Value::String(clamp_message(self.to_string()))
    }

    /// Whether the venue answered "this coin is not tradeable", as opposed to not
    /// answering at all.
    ///
    /// A listing verdict will not change within a connection, so the coin need not be
    /// asked about again; anything else must be, or the connection sits there subscribed
    /// to nothing.
    pub fn is_listing_verdict(&self) -> bool {
        matches!(self, Self::UnknownSymbol(_) | Self::UniverseError(_))
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

/// Which `l2Book` cadence to subscribe to.
///
/// Both are full snapshots; they differ in depth and frequency, and mixing them into one
/// mirror would oscillate the book (levels 6..20 would be deleted by every fast frame and
/// restored by the next slow one), so exactly one is subscribed.
///
/// Measured on mainnet 2026-07-25 and testnet 2026-07-28: `fast` is 5 levels a side every
/// ~0.54 s, the default cadence is 20 levels every ~5.4 s.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum L2BookMode {
    /// `l2Book` with `fast: true` — 5 levels a side, ~0.54 s.
    #[default]
    Fast,
    /// `l2Book` with the flag omitted — 20 levels a side, ~5.4 s.
    Slow,
}

impl L2BookMode {
    /// The `fast` flag to put in the subscribe frame, or `None` to omit the field.
    pub fn fast_flag(&self) -> Option<bool> {
        match self {
            L2BookMode::Fast => Some(true),
            L2BookMode::Slow => None,
        }
    }
}

#[derive(Deserialize)]
pub struct Config {
    /// WebSocket endpoint. Mainnet `wss://api.hyperliquid.xyz/ws`,
    /// testnet `wss://api.hyperliquid-testnet.xyz/ws`.
    pub public_url: String,
    /// REST endpoint used for `POST /info` (the coin universe). Mainnet
    /// `https://api.hyperliquid.xyz`, testnet `https://api.hyperliquid-testnet.xyz`.
    pub rest_url: String,
    /// Which `l2Book` cadence to subscribe to. See [`L2BookMode`].
    #[serde(default)]
    pub l2_book: L2BookMode,
    /// Coins to validate and subscribe at startup, before any bot registers.
    ///
    /// Optional: bots' `register()` calls are added dynamically either way. Two reasons
    /// it exists: the connector refuses to start when a configured coin is not listed
    /// (an unknown coin would close the whole WebSocket at the first subscribe), and it
    /// makes the feed observable — and the per-coin `tick_size`/`lot_size` loggable —
    /// without a bot attached.
    #[serde(default)]
    pub coins: Vec<String>,
}

type SharedSymbolSet = Arc<Mutex<HashSet<String>>>;

/// Phase 1 has no order path, so there are never any orders to report.
///
/// Deliberately not a stub that pretends: [`Hyperliquid::submit`] and
/// [`Hyperliquid::cancel`] reject every request with [`ErrorKind::OrderError`], so an
/// empty order list is the truth rather than a placeholder.
#[derive(Default)]
pub struct NoOrders;

impl GetOrders for NoOrders {
    fn orders(&self, _symbol: Option<String>) -> Vec<Order> {
        Vec::new()
    }
}

pub struct Hyperliquid {
    config: Config,
    symbols: SharedSymbolSet,
    symbol_tx: Sender<String>,
    order_manager: Arc<Mutex<NoOrders>>,
}

impl ConnectorBuilder for Hyperliquid {
    type Error = HyperliquidError;

    fn build_from(config: &str) -> Result<Self, Self::Error> {
        let config: Config = toml::from_str(config)?;
        let (symbol_tx, _) = broadcast::channel(500);
        let symbols: SharedSymbolSet = Default::default();
        {
            let mut set = symbols.lock().unwrap();
            for coin in &config.coins {
                set.insert(coin.clone());
            }
        }
        Ok(Hyperliquid {
            config,
            symbols,
            symbol_tx,
            order_manager: Arc::new(Mutex::new(NoOrders)),
        })
    }
}

impl Connector for Hyperliquid {
    fn register(&mut self, symbol: String) {
        let inserted = {
            let mut symbols = self.symbols.lock().unwrap();
            symbols.insert(symbol.clone())
        };
        if inserted {
            info!(%symbol, "Registered an instrument.");
        }
        // Sent unconditionally, and only as a wake-up: the stream re-reads the shared set
        // on every connect and on every wake, so a receiver that missed this message — or
        // lagged, or was created after it — still ends up subscribed. That is the fix for
        // `AGENTS.md` §4.2, where the other backends build their subscriptions from the
        // broadcast alone and end up connected but subscribed to nothing after a
        // reconnect. A send error means the stream task is not running yet, and the set is
        // authoritative in that case too, so it is not fatal.
        let _ = self.symbol_tx.send(symbol);
    }

    fn order_manager(&self) -> Arc<Mutex<dyn GetOrders + Send + 'static>> {
        self.order_manager.clone()
    }

    fn run(&mut self, ev_tx: UnboundedSender<PublishEvent>) {
        let mut stream = PublicStream::new(
            self.config.public_url.clone(),
            self.config.rest_url.clone(),
            self.config.l2_book,
            self.symbols.clone(),
            self.symbol_tx.subscribe(),
            ev_tx,
        );
        tokio::spawn(async move {
            stream.run().await;
        });
    }

    fn submit(&self, symbol: String, order: Order, ev_tx: UnboundedSender<PublishEvent>) {
        reject_order(&symbol, &order, ev_tx);
    }

    fn cancel(&self, symbol: String, order: Order, ev_tx: UnboundedSender<PublishEvent>) {
        reject_order(&symbol, &order, ev_tx);
    }
}

/// Fails an order request loudly, and takes the order back out of the bot's state.
///
/// Phase 1 carries no signer and no order manager. Answering `Ok` and dropping the request
/// would leave the bot waiting for a response that can never arrive — the "hide a critical
/// error behind success" failure `AGENTS.md` §1.1 rules out.
///
/// The error alone is not enough. `LiveBot::submit_order` inserts the order into its own
/// map as `Status::New` **before** the request leaves, and nothing in `process_event`
/// removes it on an `Error` event — the handler is called and that is all. So a bot that
/// mistakenly points at this backend would be left holding a live order that exists
/// nowhere, and `submit_order` with that id would return `OrderIdExist` for the rest of
/// the process's life. Both other backends answer an unsendable request by publishing the
/// order back as `Status::Expired` (`binancefutures/mod.rs`, `bybit/ordermanager.rs`); so
/// does this one, and it goes first so the bot's state is clean before the error handler —
/// which may abort the `elapse` — ever runs.
fn reject_order(symbol: &str, order: &Order, ev_tx: UnboundedSender<PublishEvent>) {
    error!(
        %symbol,
        order_id = order.order_id,
        "The Hyperliquid backend is market-data only (Phase 1); the order was rejected."
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
            HyperliquidError::OrderNotSupported.to_value(),
        ))))
        .unwrap();
}

/// Surfaces a connector-side error to every bot.
pub(crate) fn publish_error(
    ev_tx: &UnboundedSender<PublishEvent>,
    kind: ErrorKind,
    error: &HyperliquidError,
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
        connector::{ConnectorBuilder, PublishEvent},
        hyperliquid::{
            Config,
            Hyperliquid,
            HyperliquidError,
            L2BookMode,
            MAX_ERROR_BYTES,
            TRUNCATION_MARK,
            reject_order,
        },
    };

    /// The shipped example must parse with the code that reads it. `AGENTS.md` §6 requires
    /// every config field to appear in `connector/examples/*.toml`, and an example that
    /// has drifted from the struct is worse than none: it is copied verbatim by whoever
    /// deploys this.
    #[test]
    fn the_shipped_example_config_parses() {
        let config: Config =
            toml::from_str(include_str!("../../examples/hyperliquid.toml")).unwrap();

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
        // The example points at testnet: a copied-and-forgotten example must not be the
        // thing that puts a first run on mainnet.
        assert!(
            config.public_url.contains("testnet"),
            "{}",
            config.public_url
        );
        assert!(config.rest_url.contains("testnet"), "{}", config.rest_url);
        assert!(!config.coins.is_empty());
    }

    /// The 5-level, ~0.54 s cadence is the only one fast enough to quote on; the 20-level
    /// one updates every ~5.4 s. A missing field must not silently pick the slow feed.
    #[test]
    fn the_book_cadence_defaults_to_fast() {
        let config: Config = toml::from_str(
            r#"
            public_url = "wss://api.hyperliquid-testnet.xyz/ws"
            rest_url = "https://api.hyperliquid-testnet.xyz"
            "#,
        )
        .unwrap();
        assert_eq!(config.l2_book, L2BookMode::Fast);
        assert!(config.coins.is_empty());

        let slow: Config = toml::from_str(
            r#"
            public_url = "wss://api.hyperliquid.xyz/ws"
            rest_url = "https://api.hyperliquid.xyz"
            l2_book = "slow"
            "#,
        )
        .unwrap();
        assert_eq!(slow.l2_book, L2BookMode::Slow);
    }

    /// Configured coins are subscribed without waiting for a bot, which is what makes the
    /// feed observable on its own — and what makes an unlisted coin fail at startup rather
    /// than at the first registration.
    #[test]
    fn configured_coins_seed_the_symbol_set() {
        let connector = Hyperliquid::build_from(
            r#"
            public_url = "wss://api.hyperliquid-testnet.xyz/ws"
            rest_url = "https://api.hyperliquid-testnet.xyz"
            coins = ["BTC", "test:ABC"]
            "#,
        )
        .unwrap();

        let symbols = connector.symbols.lock().unwrap();
        assert!(symbols.contains("BTC"));
        assert!(symbols.contains("test:ABC"));
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
    /// kills the connector. `IceoryxSender::send` encodes into a fixed 512-byte slice, the
    /// encode error propagates out of `run_publish_task` into a `.unwrap()`, and the
    /// connector's panic hook calls `exit(1)`; the supervisor restarts it, the bot
    /// re-registers the same symbol, and it dies again. Market data for every bot goes with
    /// it. `AGENTS.md` §2 names the ceiling.
    ///
    /// This backend is the first to compose its own error text instead of relaying the
    /// venue's short one, so it is the first that can reach it. Measured: 506 characters
    /// encode to exactly 512 bytes, 510 do not fit.
    #[test]
    fn an_error_published_to_the_bots_always_fits_the_ipc_payload() {
        // The uncapped case, reproduced: this is the shape `match_universes` used to build.
        assert!(encoded_len(&"x".repeat(780)).is_err());

        for length in [0usize, 1, 399, 400, 401, 780, 4096] {
            let value = HyperliquidError::UnknownSymbol("x".repeat(length)).to_value();
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
    /// a UTF-8 sequence is not one. Venue text and symbols both reach this path.
    #[test]
    fn truncation_lands_on_a_character_boundary() {
        let value = HyperliquidError::UnknownSymbol("日".repeat(400)).to_value();
        let Value::String(message) = &value else {
            panic!("expected a string");
        };
        assert!(message.len() <= MAX_ERROR_BYTES, "{}", message.len());
        assert!(message.ends_with(TRUNCATION_MARK), "{message}");
        encoded_len(message).unwrap();
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

    /// A rejected order must be taken back out of the bot's state, not merely complained
    /// about. `LiveBot::submit_order` inserts the order as `Status::New` before the request
    /// leaves and never removes it on an `Error` event, so without this the bot holds a
    /// live order that exists nowhere and burns that `order_id` — every later
    /// `submit_order` with it returns `OrderIdExist` — for the life of the process.
    #[test]
    fn a_rejected_order_is_expired_back_to_the_bot_not_just_complained_about() {
        let (tx, mut rx) = unbounded_channel();
        reject_order("BTC", &an_order(), tx);

        let PublishEvent::LiveEvent(LiveEvent::Order { symbol, order }) = rx.try_recv().unwrap()
        else {
            panic!("the order must come back first, before the error handler can abort");
        };
        assert_eq!(symbol, "BTC");
        assert_eq!(order.order_id, 7);
        assert_eq!(order.status, Status::Expired);
        assert_eq!(order.req, Status::None);

        let PublishEvent::LiveEvent(LiveEvent::Error(error)) = rx.try_recv().unwrap() else {
            panic!("the rejection must still be reported");
        };
        assert_eq!(error.kind, ErrorKind::OrderError);
    }
}
