//! Lighter (zkLighter) perpetuals backend.
//!
//! Subscribes `order_book` and `trade` for every registered symbol and publishes kind-1
//! depth events and trade events. The **order path is opt-in** (design note
//! [`docs/design-lighter-connector.md`](../../../docs/design-lighter-connector.md) §3.2–§3.4):
//! with a `[order_path]` config section it signs, places and cancels through the sidecar,
//! nonce slot and private stream ([`order_path`]); without one it holds no key, signs nothing
//! and **refuses every order** ([`reject_order`]) — the fail-closed default, because on this
//! venue a `sendTx` 200 is not acceptance and only a live round trip proves the path (§4.11).
//!
//! Five facts about this venue's **market data** shape the module, and every one fails
//! silently:
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
    connector::{Connector, ConnectorBuilder, GetOrders, PublishEvent, SweepOutcome, SweepReason},
    lighter::{
        order_path::{OrderPath, OrderPathConfig},
        public_stream::PublicStream,
        signer::SignerNetwork,
    },
};

pub mod depth;
#[cfg(test)]
pub mod fixtures;
pub mod msg;
/// Phase 2 order-path: the sequential tx-nonce owner for one api-key slot (§3.3).
pub mod nonce;
/// Phase 2 order-path: order-id mapping and the order-state machine (§3.5, §4.11–§4.13).
pub mod order;
/// Phase 2 order-path: the async carriage — signer sidecar, slot task, private stream (§3.2–§3.4).
pub mod order_path;
/// Phase 2 order-path: the private account channels' wire shapes (§3.4, §4.12–§4.14).
pub mod private_msg;
pub mod public_stream;
pub mod rest;
/// Phase 2 order-path signing: client to the out-of-process signer sidecar (§3.2).
pub mod signer;
/// Phase 2 order-path: the slot owner's nonce/placement decisions (§3.3, §4.11).
pub mod slot;
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
    /// The config could not be read. Carries the parser's message and line only — never the
    /// document, which may be a key file passed by mistake ([`config_error`]).
    #[error("Config: {0}")]
    Config(String),
    /// A `lighter-rh` config named an endpoint, which that instance fixes in code.
    #[error(
        "FixedEndpoint: lighter-rh fixes its endpoints in code ({RH_REST_URL}, {RH_PUBLIC_URL}); \
         remove `{0}` from the config"
    )]
    FixedEndpoint(String),
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
    /// The order path (§3.2–§3.4). **Absent ⇒ market-data only, orders refused** — the
    /// fail-closed default, because acceptance on this venue is only provable by a live round
    /// trip (§4.11) that the Fix+Testnet phase, not deployment, performs. Present ⇒ the signer
    /// sidecar, nonce slot and private stream are armed.
    #[serde(default)]
    pub order_path: Option<OrderPathConfig>,
}

pub(crate) type SharedSymbolSet = Arc<Mutex<HashSet<String>>>;

/// Lighter on Robinhood Chain (`lighter-rh`): a separate exchange instance — its own
/// markets, ids and decimals, its own points programme, maker = taker = 0 on the Standard
/// tier — speaking the same protocol as mainnet (compared frame for frame on 2026-10-08,
/// [`fixtures`]). Its endpoints are **fixed here and never read from a config**: the two
/// instances share symbol names (BTC is market 1 on both), so a misdirected URL would trade
/// the wrong venue with every symbol resolving and nothing in the log to say so. The
/// collector's `lighter-rh` backend makes the same choice for the same reason.
pub const RH_PUBLIC_URL: &str = "wss://api.rh.lighter.xyz/stream";
/// See [`RH_PUBLIC_URL`].
pub const RH_REST_URL: &str = "https://api.rh.lighter.xyz";
/// The chain id every Robinhood Chain transaction is hashed with — lighter-sdk's
/// `endpoint_profiles.ROBINHOOD`. It enters the hash the signature is over and nothing
/// else: the auth token carries no chain id, so a signer on the wrong one passes the
/// private stream's auth and has **every** transaction refused, `CancelAllOrders`
/// included. The signer sidecar used to infer 300 (testnet) for any URL that was not
/// mainnet's, which is exactly that failure on this instance.
pub const RH_CHAIN_ID: u32 = 466_324;

/// Which Lighter instance this connector process serves.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Instance {
    /// `lighter`: endpoints from the config — mainnet, or testnet for a shakeout.
    Configured,
    /// `lighter-rh`: Robinhood Chain. Endpoints and chain id are the constants above; a
    /// config that names an endpoint is refused ([`LighterError::FixedEndpoint`]).
    RobinhoodChain,
}

impl Instance {
    /// The instance a backend name on the command line selects, or `None` if the name is
    /// not a Lighter backend. The name is the **only** selector — exact, case included.
    pub fn for_backend(name: &str) -> Option<Self> {
        match name {
            "lighter" => Some(Self::Configured),
            "lighter-rh" => Some(Self::RobinhoodChain),
            _ => None,
        }
    }

    /// The backend name, as `main.rs` dispatches on it.
    pub fn name(self) -> &'static str {
        match self {
            Self::Configured => "lighter",
            Self::RobinhoodChain => "lighter-rh",
        }
    }

    /// The network the order path's signer must sign for (§3.2). `lighter-rh` is Robinhood
    /// Chain whatever it is handed; `lighter` signs for its configured REST base and names
    /// the chain when the table knows it ([`signer::known_chain_id`]).
    pub fn signer_network(self, rest_url: &str) -> SignerNetwork {
        match self {
            Self::RobinhoodChain => SignerNetwork {
                url: RH_REST_URL.to_string(),
                chain_id: Some(RH_CHAIN_ID),
            },
            Self::Configured => SignerNetwork {
                url: rest_url.to_string(),
                chain_id: signer::known_chain_id(rest_url),
            },
        }
    }
}

/// Reads a connector config for `instance`.
///
/// `lighter-rh` takes everything but the endpoints from the file, and refuses a file that
/// names one rather than obey or ignore it: obeyed, a URL left over from a copied mainnet
/// config trades mainnet; ignored, the file says one thing and the process does another.
fn parse_config(instance: Instance, text: &str) -> Result<Config, LighterError> {
    match instance {
        Instance::Configured => toml::from_str(text).map_err(|error| config_error(text, &error)),
        Instance::RobinhoodChain => {
            let mut table: toml::Table =
                toml::from_str(text).map_err(|error| config_error(text, &error))?;
            for key in ["public_url", "rest_url"] {
                if table.contains_key(key) {
                    return Err(LighterError::FixedEndpoint(key.to_string()));
                }
            }
            table.insert(
                "public_url".to_string(),
                toml::Value::String(RH_PUBLIC_URL.to_string()),
            );
            table.insert(
                "rest_url".to_string(),
                toml::Value::String(RH_REST_URL.to_string()),
            );
            toml::Value::Table(table)
                .try_into()
                .map_err(|error| config_error(text, &error))
        }
    }
}

/// A config error as it may be logged: the parser's message and the line it points at, and
/// **never** the document. `toml::de::Error`'s `Debug` carries the whole input and its
/// `Display` quotes the offending line, and build errors are logged — so a key file handed
/// over as the connector config by mistake would otherwise put its private key in the log.
fn config_error(text: &str, error: &toml::de::Error) -> LighterError {
    let line = error.span().map(|span| {
        // Bytes, not `str` slicing: a span need not land on a character boundary.
        text.as_bytes()[..span.start.min(text.len())]
            .iter()
            .filter(|byte| **byte == b'\n')
            .count()
            + 1
    });
    LighterError::Config(match line {
        Some(line) => format!("{} (line {line})", error.message()),
        None => error.message().to_string(),
    })
}

impl Lighter {
    /// Builds the connector for one instance. `ConnectorBuilder::build_from` is the
    /// `lighter` (configured-endpoint) case of this.
    pub fn build_for(instance: Instance, config: &str) -> Result<Self, LighterError> {
        let config = parse_config(instance, config)?;
        let (symbol_tx, _) = broadcast::channel(500);
        let symbols: SharedSymbolSet = Default::default();
        {
            let mut set = symbols.lock().unwrap();
            for symbol in &config.symbols {
                set.insert(symbol.clone());
            }
        }
        info!(
            backend = instance.name(),
            public_url = %config.public_url,
            rest_url = %config.rest_url,
            "The Lighter instance this connector serves."
        );
        if config.order_path.is_some() {
            info!(
                "The Lighter order path is ARMED (an [order_path] section is present): it will \
                 sign, place and cancel orders. Acceptance is confirmed off the private channel, \
                 not the sendTx 200 (§4.11)."
            );
        } else {
            warn!(
                "The Lighter backend is market-data only: no [order_path] section, so it serves \
                 the feed and refuses every order. This is the fail-closed default."
            );
        }
        Ok(Lighter {
            instance,
            config,
            symbols,
            symbol_tx,
            no_orders: Arc::new(Mutex::new(NoOrders)),
            order_path: None,
            tasks: Vec::new(),
        })
    }
}

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
    instance: Instance,
    config: Config,
    symbols: SharedSymbolSet,
    symbol_tx: Sender<String>,
    /// Reported to bots when the order path is **not** armed — an empty list, which is the
    /// truth, because [`Self::submit`] refuses every order in that mode.
    no_orders: Arc<Mutex<NoOrders>>,
    /// The armed order path, or `None` for market-data-only. When present it owns the real
    /// [`order::OrderManager`], and `order_manager()` reports from it.
    order_path: Option<OrderPath>,
    /// The stream task, kept so an orderly stop can end it and its `PublishEvent` sender
    /// can be dropped — which is what lets the publish task stop last. `AGENTS.md` §4.7.
    tasks: Vec<JoinHandle<()>>,
}

impl ConnectorBuilder for Lighter {
    type Error = LighterError;

    fn build_from(config: &str) -> Result<Self, Self::Error> {
        Self::build_for(Instance::Configured, config)
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
        // When armed, the truth is the order path's own manager; otherwise nothing rests, and
        // an empty list is the truth (this backend refuses every order in that mode).
        if let Some(order_path) = &self.order_path {
            order_path.order_manager()
        } else {
            self.no_orders.clone()
        }
    }

    fn run(&mut self, ev_tx: UnboundedSender<PublishEvent>) {
        let mut public = PublicStream::new(
            self.config.public_url.clone(),
            self.config.rest_url.clone(),
            self.symbols.clone(),
            self.symbol_tx.subscribe(),
            ev_tx.clone(),
        );
        self.tasks.push(tokio::spawn(async move {
            public.run().await;
        }));
        if let Some(config) = self.config.order_path.clone() {
            info!(
                api_key_index = config.api_key_index,
                account_index = config.account_index,
                "Arming the Lighter order path."
            );
            let network = self.instance.signer_network(&self.config.rest_url);
            info!(
                url = %network.url,
                chain_id = ?network.chain_id,
                "The Lighter signer must sign for this network; a sidecar that does not is \
                 refused before the first send."
            );
            self.order_path = Some(OrderPath::spawn(
                config,
                network,
                self.config.rest_url.clone(),
                self.config.public_url.clone(),
                self.symbols.clone(),
                // The registration wake-up: the private stream re-resolves a symbol registered
                // after it connected, so orders for it reach the venue (§4.2). Without it a
                // symbol seeded before connect works and one registered after silently never
                // places an order.
                self.symbol_tx.subscribe(),
                ev_tx,
            ));
        }
    }

    fn submit(&self, symbol: String, order: Order, ev_tx: UnboundedSender<PublishEvent>) {
        match &self.order_path {
            Some(order_path) => order_path.submit(&symbol, &order, &ev_tx),
            None => reject_order(&symbol, &order, ev_tx),
        }
    }

    fn cancel(&self, symbol: String, order: Order, ev_tx: UnboundedSender<PublishEvent>) {
        match &self.order_path {
            Some(order_path) => order_path.cancel(&symbol, &order, &ev_tx),
            None => reject_order(&symbol, &order, ev_tx),
        }
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
        ev_tx: UnboundedSender<PublishEvent>,
    ) -> Option<JoinHandle<SweepOutcome>> {
        let Some(order_path) = &self.order_path else {
            // Market-data only: nothing was ever placed, so there is nothing resting to sweep.
            info!(
                ?reason,
                ?symbols,
                "The Lighter backend is market-data only, so it has nothing resting to sweep."
            );
            // No order path at all: nothing this connector placed can be resting.
            return None;
        };
        // One `CancelAllOrders` per resolved market (§3.4) — scoped to the swept symbols rather
        // than the whole account, so a dead bot's sweep cannot flatten a live bot's markets.
        let mut unresolved: Vec<String> = Vec::new();
        let market_indices: Vec<i32> = {
            let manager = order_path.order_manager();
            let manager = manager.lock().unwrap();
            symbols
                .iter()
                .filter_map(|symbol| match manager.market_info(symbol) {
                    Some(info) => Some(info.market_id as i32),
                    None => {
                        unresolved.push(symbol.clone());
                        None
                    }
                })
                .collect()
        };
        if !unresolved.is_empty() {
            // A symbol whose market never resolved is silently skipped by the filter above, so a
            // dead-bot or shutdown sweep does NOT cancel its resting orders — and on this venue
            // orders outlive the process (§2.7), so they keep trading unattended. Name them, so
            // the operator can see what the sweep could not cover.
            //
            // **Reported rather than fixed, deliberately.** `CancelAllOrders` addresses a
            // market by index and has no by-name form; the one index that needs no catalog is
            // `255`, which means *every* market on the account and would flatten a live bot's
            // grid — the exact harm `AGENTS.md` §4.7 says a sweep must not do. What narrows the
            // residual is `OrderPath::submit`: it refuses any symbol with no catalog row, so an
            // unresolved symbol cannot be holding an order **this** process placed. What it can
            // be holding is a previous incarnation's, and the path for those is the connect-time
            // snapshot under `connect_policy = "cancel_all"` (§3.4), not this sweep.
            warn!(
                ?reason,
                ?unresolved,
                "Some Lighter symbols have no resolved market, so this sweep cannot cancel their \
                 orders; any resting orders on them keep trading until the market resolves or the \
                 connector stops."
            );
        }
        info!(
            ?reason,
            ?symbols,
            ?market_indices,
            "Sweeping Lighter orders with CancelAllOrders (§3.4)."
        );
        // The private stream holds its own publish sender and reports the cancel confirmations;
        // this sender is not needed to carry them, so it is dropped rather than parked.
        drop(ev_tx);
        Some(order_path.sweep(market_indices))
    }

    /// Ends the stream task, so its sender is dropped and the publish task can see the
    /// channel close and stop **last**. Without it an orderly stop waits out
    /// `shutdown.grace_ms` in full — `AGENTS.md` §4.7.
    fn shutdown(&mut self) {
        // Called after `sweep` (`connector/src/connector.rs`), so an in-flight sweep is not
        // cut off by aborting the slot task here.
        if let Some(order_path) = &mut self.order_path {
            order_path.shutdown();
        }
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
    use std::{
        fmt,
        sync::{Arc, Mutex},
    };

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
    use tracing::{
        Event as LogEvent,
        Level,
        Subscriber,
        field::{Field, Visit},
    };
    use tracing_subscriber::{Layer, layer::Context as LayerContext, prelude::*};

    use crate::{
        connector::{Connector, ConnectorBuilder, PublishEvent, SweepReason},
        lighter::{
            Config,
            Instance,
            Lighter,
            LighterError,
            MAX_ERROR_BYTES,
            RH_CHAIN_ID,
            RH_PUBLIC_URL,
            RH_REST_URL,
            TRUNCATION_MARK,
            fixtures::RH_ORDER_BOOKS,
            reject_order,
            rest::{MarketInfo, match_catalog},
            signer::SignerNetwork,
        },
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

    /// A market-data-only config, and the same one with the order path armed.
    ///
    /// Both point at a dead local port: `run` really does start its tasks, and no test may
    /// reach for the venue. What they do once started is irrelevant here — the wiring under
    /// test is done by the time `run` returns.
    const MARKET_DATA_ONLY: &str = r#"
        public_url = "ws://127.0.0.1:1/stream"
        rest_url = "http://127.0.0.1:1"
        symbols = ["BTC"]
        "#;

    const ARMED: &str = r#"
        public_url = "ws://127.0.0.1:1/stream"
        rest_url = "http://127.0.0.1:1"
        symbols = ["BTC"]

        [order_path]
        account_index = 516
        api_key_index = 5
        signer_python = ""
        signer_script = ""
        signer_config = ""
        "#;

    /// **The order path is subscribed to the registration wake-up.**
    ///
    /// "The wake was never wired" *is* the zero-fills bug: a symbol seeded in the config
    /// resolved and a symbol a bot registered afterwards never did, so the connector quoted
    /// nothing and said nothing. Every other test in this backend proves what the private
    /// stream does *given* a wake-up; not one of them proves anything feeds it. Measured:
    /// replacing `self.symbol_tx.subscribe()` in `run` with a detached channel left the whole
    /// lighter suite green.
    ///
    /// Counted rather than asserted through behaviour because the count is the wiring: two
    /// receivers when the order path is armed (the public stream and the private one), one
    /// when it is not.
    #[tokio::test]
    async fn the_order_path_subscribes_to_the_registration_wake_up() {
        // `reqwest` builds its rustls backend when the client is built (`AGENTS.md` §4.7).
        crate::install_crypto_provider();

        let mut armed = Lighter::build_from(ARMED).unwrap();
        let (ev_tx, _armed_events) = unbounded_channel();
        armed.run(ev_tx);
        let armed_receivers = armed.symbol_tx.receiver_count();
        armed.shutdown();

        let mut data_only = Lighter::build_from(MARKET_DATA_ONLY).unwrap();
        let (ev_tx, _data_only_events) = unbounded_channel();
        data_only.run(ev_tx);
        let data_only_receivers = data_only.symbol_tx.receiver_count();
        data_only.shutdown();

        assert_eq!(
            data_only_receivers, 1,
            "market-data only: the public stream alone listens for registrations"
        );
        assert_eq!(
            armed_receivers, 2,
            "armed: the order path must listen too, or a symbol registered after it connected \
             never resolves and every order on it is expired before signing"
        );
    }

    /// Captures the `unresolved` field of `warn!` events on **this thread only**, for as long
    /// as the guard lives (the shape `bybit/public_stream.rs` uses; `AGENTS.md` §4.2 documents
    /// why a log field sometimes has to be the pin, and that the pattern can flake when other
    /// tests race the global callsite-interest cache).
    #[derive(Clone, Default)]
    struct ReportedUnresolved(Arc<Mutex<Vec<String>>>);

    impl ReportedUnresolved {
        fn capturing(&self) -> tracing::subscriber::DefaultGuard {
            tracing::subscriber::set_default(tracing_subscriber::registry().with(self.clone()))
        }

        fn reported(&self) -> Vec<String> {
            self.0.lock().unwrap().clone()
        }
    }

    impl<S: Subscriber> Layer<S> for ReportedUnresolved {
        fn on_event(&self, event: &LogEvent<'_>, _: LayerContext<'_, S>) {
            if *event.metadata().level() != Level::WARN {
                return;
            }
            let mut field = UnresolvedField(None);
            event.record(&mut field);
            if let Some(value) = field.0 {
                self.0.lock().unwrap().push(value);
            }
        }
    }

    /// Pulls the `unresolved = ...` field out of one event and ignores every other field. It is
    /// recorded with `?`, so it arrives as a `Debug` rendering of the list.
    struct UnresolvedField(Option<String>);

    impl Visit for UnresolvedField {
        fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
            if field.name() == "unresolved" {
                self.0 = Some(format!("{value:?}"));
            }
        }
    }

    /// **A sweep that cannot cover a symbol names it.**
    ///
    /// `sweep` addresses each market by the `market_id` this process resolved, so a symbol that
    /// never resolved is dropped by the `filter_map` and its resting orders are **not**
    /// cancelled — the §4.7 failure class, and on this venue orders outlive the process (§2.7),
    /// so they keep trading unattended after a dead-bot sweep or a shutdown.
    ///
    /// This is a **report, not a fix**, and the report is all there is to pin: an unaddressable
    /// market cannot be cancelled, because `CancelAllOrders` takes a market index and there is
    /// no by-name form. The residual is narrow by construction — [`OrderPath::submit`] refuses
    /// any symbol without a catalog row, so an unresolved symbol cannot be holding an order
    /// **this** process placed; what it can be holding is a previous incarnation's, which the
    /// connect-time snapshot (under `connect_policy = "cancel_all"`) is the path for. The pin
    /// exists because a `warn!` is exactly the kind of change that reverts without a single
    /// test noticing (`AGENTS.md` §4.2), and the operator greps the symbol.
    #[tokio::test]
    async fn a_sweep_names_the_symbols_it_could_not_cover() {
        crate::install_crypto_provider();
        let mut connector = Lighter::build_from(ARMED).unwrap();
        let (ev_tx, _events) = unbounded_channel();
        connector.run(ev_tx.clone());

        // BTC resolved; ETH never did — the state a catalog refusal leaves behind.
        connector
            .order_path
            .as_ref()
            .unwrap()
            .order_manager()
            .lock()
            .unwrap()
            .track_market(MarketInfo {
                symbol: "BTC".to_string(),
                market_id: 1,
                price_decimals: 1,
                size_decimals: 5,
                ..Default::default()
            });

        let reported = ReportedUnresolved::default();
        let handle = {
            let _capturing = reported.capturing();
            connector.sweep(
                vec!["BTC".to_string(), "ETH".to_string()],
                SweepReason::ConnectorStopping,
                ev_tx,
            )
        };
        assert!(handle.is_some(), "the resolved market is still swept");
        connector.shutdown();

        let reported = reported.reported();
        assert_eq!(reported.len(), 1, "one report, naming what was missed");
        assert!(
            reported[0].contains("ETH"),
            "the symbol the sweep could not cover must be named: {}",
            reported[0]
        );
        assert!(
            !reported[0].contains("BTC"),
            "and the one it did cover must not be: {}",
            reported[0]
        );
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

    // ---- lighter-rh: Lighter on Robinhood Chain ---------------------------------------

    /// The backend is chosen by its name on the command line, and by nothing else: the two
    /// instances share symbol names (BTC is market 1 on both), so nothing downstream would
    /// show that the wrong one had been dialled.
    #[test]
    fn the_instance_is_chosen_by_the_backend_name_alone() {
        assert_eq!(Instance::for_backend("lighter"), Some(Instance::Configured));
        assert_eq!(
            Instance::for_backend("lighter-rh"),
            Some(Instance::RobinhoodChain)
        );
        for near_miss in ["lighter_rh", "lighterrh", "Lighter-RH", "rh", "lighter-rh "] {
            assert_eq!(Instance::for_backend(near_miss), None, "{near_miss:?}");
        }
    }

    /// `lighter-rh` dials Robinhood Chain's fixed endpoints, whatever else its config says.
    #[test]
    fn lighter_rh_dials_the_robinhood_chain_endpoints() {
        let connector =
            Lighter::build_for(Instance::RobinhoodChain, r#"symbols = ["SPY", "XAU"]"#).unwrap();

        assert_eq!(connector.instance, Instance::RobinhoodChain);
        assert_eq!(
            connector.config.public_url,
            "wss://api.rh.lighter.xyz/stream"
        );
        assert_eq!(connector.config.rest_url, "https://api.rh.lighter.xyz");
        assert_eq!(RH_PUBLIC_URL, "wss://api.rh.lighter.xyz/stream");
        assert_eq!(RH_REST_URL, "https://api.rh.lighter.xyz");
        assert_eq!(RH_CHAIN_ID, 466_324);
        let symbols = connector.symbols.lock().unwrap();
        assert!(symbols.contains("SPY") && symbols.contains("XAU"));
    }

    /// An endpoint in a `lighter-rh` config is refused, not ignored and not obeyed. Obeyed,
    /// a mainnet URL left over from a copied config would trade mainnet under names that all
    /// resolve; ignored, the file would say one thing and the process do another. Refused
    /// even when it names the right URL, so the rule has no exceptions to remember.
    #[test]
    fn a_lighter_rh_config_that_names_an_endpoint_is_refused() {
        for (key, value) in [
            ("rest_url", "https://mainnet.zklighter.elliot.ai"),
            ("public_url", "wss://mainnet.zklighter.elliot.ai/stream"),
            ("rest_url", "https://api.rh.lighter.xyz"),
        ] {
            let text = format!("{key} = \"{value}\"\nsymbols = [\"BTC\"]\n");
            let Err(error) = Lighter::build_for(Instance::RobinhoodChain, &text) else {
                panic!("{key} = {value} must be refused for lighter-rh");
            };
            let message = error.to_string();
            assert!(
                matches!(&error, LighterError::FixedEndpoint(named) if named == key),
                "{key}: {message}"
            );
            assert!(message.contains(key), "{message}");
        }
    }

    /// The mainnet/testnet backend is untouched: its endpoints still come from the config,
    /// and a config without them is still refused.
    #[test]
    fn the_configured_instance_still_reads_its_endpoints_from_the_config() {
        let connector = Lighter::build_for(
            Instance::Configured,
            r#"
            public_url = "wss://testnet.zklighter.elliot.ai/stream"
            rest_url = "https://testnet.zklighter.elliot.ai"
            "#,
        )
        .unwrap();
        assert_eq!(connector.instance, Instance::Configured);
        assert_eq!(
            connector.config.rest_url,
            "https://testnet.zklighter.elliot.ai"
        );
        assert!(Lighter::build_for(Instance::Configured, r#"symbols = ["BTC"]"#).is_err());
    }

    /// A config error names what is wrong and **never** echoes the document. The toml
    /// error's `Debug` carries the whole input, and the connector logs build errors; a
    /// sidecar key file passed as the connector config by mistake would put its private key
    /// in the log.
    #[test]
    fn a_config_error_does_not_echo_the_document() {
        let secret =
            "0x5ec2e7aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        // A sidecar key file: no `public_url`/`rest_url`, so `lighter` refuses it as
        // incomplete; a `[order_path]`-less RH config with only unknown keys would parse,
        // so the RH case is the half-written file below, which no instance can read.
        let sidecar_file = format!(
            "url = \"https://api.rh.lighter.xyz\"\naccount_index = 1\napi_key_index = 5\nprivate_key = \"{secret}\"\n"
        );
        let not_toml = format!("private_key = \"{secret}\nsymbols = [");
        let cases = [
            (Instance::Configured, sidecar_file.as_str()),
            (Instance::Configured, not_toml.as_str()),
            (Instance::RobinhoodChain, not_toml.as_str()),
        ];
        for (instance, text) in cases {
            let Err(error) = Lighter::build_for(instance, text) else {
                panic!("{instance:?} must refuse {text:?}");
            };
            let shown = format!("{error} {error:?}");
            assert!(!shown.contains("5ec2e7"), "{instance:?}: {shown}");
            assert!(matches!(error, LighterError::Config(_)), "{shown}");
        }
    }

    /// The RH catalog maps symbols onto **its own** ids and decimals — the mainnet ones are
    /// different for most names, so this is the table the order path quantises onto.
    #[test]
    fn the_rh_catalog_resolves_onto_robinhood_chain_markets() {
        let wanted: Vec<String> = ["BTC", "ETH", "HYPE", "SOL", "SPY", "XAU", "CASHCAT"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let (resolved, refused) = match_catalog(&wanted, RH_ORDER_BOOKS).unwrap();
        assert!(refused.is_empty(), "{refused:?}");
        let by_symbol = |symbol: &str| {
            resolved
                .iter()
                .find(|info| info.symbol == symbol)
                .cloned()
                .unwrap()
        };
        // (symbol, market_id, tick, lot) — RH, 2026-10-08.
        for (symbol, market_id, tick, lot) in [
            ("ETH", 0, 0.01, 0.0001),
            ("BTC", 1, 0.1, 0.00001),
            ("HYPE", 2, 0.001, 0.001),
            ("SOL", 3, 0.001, 0.001),
            ("SPY", 26, 0.01, 0.0001),
            ("CASHCAT", 36, 0.00001, 0.1),
            ("XAU", 40, 0.01, 0.0001),
        ] {
            let info = by_symbol(symbol);
            assert_eq!(info.market_id, market_id, "{symbol}");
            assert!(
                (info.tick_size() - tick).abs() < tick * 1e-9,
                "{symbol} tick"
            );
            assert!((info.lot_size() - lot).abs() < lot * 1e-9, "{symbol} lot");
        }
    }

    /// `lighter-rh` signs for Robinhood Chain's URL and chain id, whatever URL it is handed
    /// (none can be configured): a key provisioned for another chain is then refused at the
    /// slot's startup instead of having every transaction refused by the venue.
    #[test]
    fn lighter_rh_signs_for_chain_466324() {
        let rh = SignerNetwork {
            url: RH_REST_URL.to_string(),
            chain_id: Some(466_324),
        };
        assert_eq!(Instance::RobinhoodChain.signer_network(RH_REST_URL), rh);
        assert_eq!(
            Instance::RobinhoodChain.signer_network("https://mainnet.zklighter.elliot.ai"),
            rh
        );
        // `lighter` signs for the URL it is configured with, and names the chain when known.
        for (url, chain_id) in [
            ("https://mainnet.zklighter.elliot.ai", Some(304)),
            ("https://testnet.zklighter.elliot.ai", Some(300)),
            ("https://api.example.xyz", None),
        ] {
            assert_eq!(
                Instance::Configured.signer_network(url),
                SignerNetwork {
                    url: url.to_string(),
                    chain_id
                }
            );
        }
    }

    /// The shipped RH example builds as `lighter-rh` — no endpoint keys, so it is not
    /// refused — ships DISARMED, names only perps that resolve on the RH catalog, parses as
    /// the generic supervision config, and its commented `[order_path]` block parses with
    /// the policy and slot it documents once uncommented.
    #[test]
    fn the_shipped_rh_example_config_parses() {
        let text = include_str!("../../examples/lighter-rh.toml");
        let connector = Lighter::build_for(Instance::RobinhoodChain, text).unwrap();
        assert_eq!(connector.config.rest_url, RH_REST_URL);
        assert!(
            connector.config.order_path.is_none(),
            "the example ships disarmed"
        );

        let (resolved, refused) = match_catalog(&connector.config.symbols, RH_ORDER_BOOKS).unwrap();
        assert!(refused.is_empty(), "{refused:?}");
        assert_eq!(resolved.len(), connector.config.symbols.len());
        for symbol in &connector.config.symbols {
            assert!(!symbol.contains('/'), "spot pair in the example: {symbol}");
        }

        let supervision: crate::supervision::SupervisionConfig = toml::from_str(text).unwrap();
        assert!(supervision.shutdown.sweep_orders);
        assert!(supervision.bot_liveness.enabled);

        // Uncomment the [order_path] block exactly as an operator would.
        let mut armed = String::new();
        let mut in_block = false;
        for line in text.lines() {
            if line.trim() == "# [order_path]" {
                in_block = true;
            }
            if in_block {
                if let Some(rest) = line.strip_prefix("# ") {
                    if !rest.starts_with('#') {
                        armed.push_str(rest);
                        armed.push('\n');
                        continue;
                    }
                }
                continue;
            }
            armed.push_str(line);
            armed.push('\n');
        }
        let connector = Lighter::build_for(Instance::RobinhoodChain, &armed).unwrap();
        let order_path = connector
            .config
            .order_path
            .expect("the block arms the path");
        assert!(order_path.api_key_index >= 4, "slots 0-3 are the venue's");
        assert_eq!(
            order_path.connect_policy,
            crate::lighter::order_path::ConnectPolicy::CancelAll
        );
    }
}
