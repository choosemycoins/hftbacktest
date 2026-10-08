//! Hyperliquid perpetuals backend: market data, and the order path.
//!
//! Subscribes to `bbo`, `l2Book` and `trades` for every registered coin and turns them into
//! `LiveEvent::Feed` events; with an API wallet configured it also signs and submits orders,
//! follows them on `orderUpdates` / `userFills`, and reconciles against the venue on every
//! connect. **Without credentials it runs exactly as market data and refuses every order**,
//! which is a supported way to deploy it and the reason the key is optional.
//!
//! Design: [`docs/design-hyperliquid-connector.md`](../../../docs/design-hyperliquid-connector.md).
//!
//! Four facts shape the whole module, and each of them fails silently:
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
//! 3. **Actions are signed by the API wallet; every query and subscription uses the master
//!    account.** Asking with the agent's address is accepted and returns nothing, which is
//!    indistinguishable from an account that never trades — [`private_stream`].
//! 4. **Six different signing mistakes produce one venue message**, and it names none of
//!    them: "User or API Wallet 0x… does not exist" — [`signing`].

use std::{
    collections::HashSet,
    num::ParseFloatError,
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
    hyperliquid::{
        exchange::ExchangeClient,
        ordermanager::{OrderManager, SharedOrderManager},
        private_stream::{ConnectPolicy, PrivateStream, SharedInstruments, Sweeper},
        public_stream::PublicStream,
        signing::{NonceSource, Signer},
    },
};

pub mod depth;
pub mod exchange;
#[cfg(test)]
mod fixtures;
pub mod msg;
pub mod normalize;
pub mod ordermanager;
pub mod private_stream;
pub mod public_stream;
pub mod rest;
pub mod signing;
#[cfg(test)]
mod smoke;
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
    #[error("OrderNotSupported: no API wallet is configured, so this backend is market-data only")]
    OrderNotSupported,
    /// The key material could not be read or is not a key. Never carries the value.
    #[error("Credentials: {0}")]
    Credentials(String),
    /// The signature could not be produced, or disagreed with its own key on recovery.
    #[error("Signing: {0}")]
    Signing(String),
    /// The request cannot be turned into something the venue would accept.
    #[error("InvalidOrder: {0}")]
    InvalidOrder(String),
    /// The venue reported an order status this backend does not recognise (Finding 3). The
    /// order is **kept, not dropped** — an unknown status may mean it is still resting — and
    /// this is escalated as a [`ErrorKind::CriticalConnectionError`] so reconciliation resolves
    /// it. Carries the raw status for the operator. **Never a verdict**: it must not reach the
    /// submit path, so `is_venue_verdict` stays false for it.
    #[error("UnrecognisedOrderStatus: {0}")]
    UnrecognisedOrderStatus(String),
    #[error("OrderNotFound: {0}")]
    OrderNotFound(String),
    /// The venue refused the request, in its own words. **Includes a `200 ok` whose nested
    /// per-order status was an error** — see `exchange::parse_exchange_response`.
    ///
    /// A *verdict*: the order does not exist, and the bot is told so.
    #[error("ExchangeRejected: {0}")]
    ExchangeRejected(String),
    /// The venue answered in a way that does not say whether the action was carried out —
    /// a `5xx` from the edge, a `408`, a body that is not the venue's.
    ///
    /// **Not a verdict.** The order may be resting right now, so it is kept and
    /// reconciliation settles it (design note §11.7).
    #[error("ExchangeUnavailable: {0}")]
    ExchangeUnavailable(String),
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

    /// Whether the **matching engine** decided against this request, as opposed to the
    /// request not having reached it or its answer having been lost.
    ///
    /// The order path branches on exactly this: a verdict expires the order back to the bot
    /// and frees its `order_id`; anything else keeps the order, because it may be resting
    /// and a re-quote on top of it is real duplicated exposure (design note §11.7).
    pub fn is_venue_verdict(&self) -> bool {
        matches!(self, Self::ExchangeRejected(_))
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

    // --- Order path. Absent means market data only. ------------------------------------
    /// Path to the credentials file — **never** the key itself.
    ///
    /// Keeping key material out of this file is the point: this one is committed, copied
    /// into deployment repositories and pasted into issues. The credentials file lives
    /// outside git at mode 600 and carries two TOML keys, named exactly as the venue's own
    /// tooling names them:
    ///
    /// ```toml
    /// api_wallet_private_key = "0x…"   # the API wallet; signs actions
    /// master_account_address = "0x…"   # owns the positions; keys every query
    /// ```
    ///
    /// Overridden by the `HYPERLIQUID_CREDENTIALS` environment variable, which is how a
    /// deployment points one config at different accounts. Absent from both, the backend
    /// runs exactly as Phase 1 did: market data, and every order refused.
    #[serde(default)]
    pub credentials_path: Option<String>,

    /// Which network the phantom-agent signature claims. **Must agree with the URLs above.**
    ///
    /// It is not derived from them because a URL is a string a deployment can point
    /// anywhere — through a proxy, a relay, a local rehearsal harness — and guessing the
    /// network from it would be wrong exactly when it matters. Getting it wrong produces a
    /// valid signature for the other network, which the venue reports as an API wallet that
    /// does not exist.
    #[serde(default)]
    pub is_mainnet: bool,

    /// Four hex characters stamped into the high nibbles of every client order id.
    ///
    /// The venue's `cloid` is a fixed 128-bit value, so a readable prefix is not available
    /// (design note §5.4). This is what lets the connector tell its own orders from a
    /// human's on the same account — and therefore what stops it from reporting them as
    /// unknown orders, or cancelling them.
    #[serde(default = "default_order_prefix")]
    pub order_prefix: String,

    /// What to do about orders already resting when the account stream connects.
    /// See [`ConnectPolicy`]; the default reconciles rather than cancels.
    #[serde(default)]
    pub connect_policy: ConnectPolicy,

    /// The master account address, for cross-checking the credentials file.
    ///
    /// Optional, and worth setting. A credentials file pointing at a different account than
    /// the deployment expects fails in the most confusing way available: orders sign and
    /// rest correctly, while `orderUpdates`, `userFills` and every position query describe
    /// somebody else. Set this and a mismatch is one line at startup instead.
    #[serde(default)]
    pub account_address: Option<String>,
}

fn default_order_prefix() -> String {
    // Hexadecimal, because it becomes the high nibbles of a 128-bit client id. The obvious
    // mnemonic choices are not: `hf01` contains no valid hex digit at all, and a
    // non-hex prefix makes every `cloid` malformed, which the venue answers with an HTTP
    // 422 on every order.
    "a1f0".to_string()
}

/// The API wallet key and the account it acts for, read from outside git.
#[derive(Deserialize)]
pub struct Credentials {
    /// The API wallet ("agent wallet") private key. Signs actions, and nothing else.
    pub api_wallet_private_key: String,
    /// The account that owns the positions. Keys **every** subscription and `/info` query;
    /// asking with the API wallet's address returns an empty answer, not an error.
    pub master_account_address: String,
}

/// Environment variable that overrides `credentials_path`.
pub const CREDENTIALS_ENV: &str = "HYPERLIQUID_CREDENTIALS";

impl Config {
    /// Where the credentials should be read from, if anywhere.
    pub fn credentials_source(&self, env: Option<String>) -> Option<String> {
        env.filter(|path| !path.trim().is_empty())
            .or_else(|| self.credentials_path.clone())
            .filter(|path| !path.trim().is_empty())
    }
}

/// Reads and checks the credentials.
///
/// The consistency check is not ceremony. If `account_address` is configured and disagrees
/// with the credentials file, the connector would sign with a key belonging to one account
/// and watch another — orders would rest and their fills would never arrive. Refusing at
/// startup makes that a line in the log rather than a silent half-working day.
pub fn load_credentials(
    path: &str,
    expected_account: Option<&str>,
) -> Result<Credentials, HyperliquidError> {
    let text = std::fs::read_to_string(path).map_err(|error| {
        HyperliquidError::Credentials(format!("could not read the credentials at {path}: {error}"))
    })?;
    let credentials: Credentials = toml::from_str(&text).map_err(|_| {
        // **The parse error is deliberately dropped.** `toml::de::Error` renders the
        // offending source line, and in this file that line is a private key — so quoting
        // it writes the key into the log, and into whatever ships the log onward. Measured
        // against a file with the key present and the account missing: the error contained
        // the whole `api_wallet_private_key` assignment. The two field names below are the
        // entire diagnosis anyway.
        HyperliquidError::Credentials(format!(
            "the credentials at {path} could not be parsed; they need exactly two quoted \
             strings, `api_wallet_private_key` and `master_account_address` (the parser's \
             own message is withheld because it would quote the key)"
        ))
    })?;
    let account = credentials.master_account_address.trim();
    // Shape, before anything uses it. Hyperliquid does **not** reject a `user` it cannot
    // parse — it answers with an empty result, which is exactly what a flat account looks
    // like, so a typo here is silence rather than an error for the life of the deployment.
    if !is_evm_address(account) {
        return Err(HyperliquidError::Credentials(format!(
            "master_account_address in {path} is not an address: it must be 0x followed by \
             40 hexadecimal characters, and the venue answers a query for anything else \
             with an empty result rather than an error — got {account:?}"
        )));
    }
    if let Some(expected) = expected_account
        && !expected.trim().eq_ignore_ascii_case(account)
    {
        return Err(HyperliquidError::Credentials(format!(
            "the configured account_address {expected} is not the credentials file's \
             master_account_address; signing would act for one account while every \
             subscription watched another"
        )));
    }
    Ok(credentials)
}

/// Whether a string is an EVM address: `0x` and exactly 40 hexadecimal characters.
///
/// Compared as bytes so that a 40-*byte* string carrying multi-byte characters cannot be
/// mistaken for 40 hex digits.
fn is_evm_address(text: &str) -> bool {
    text.strip_prefix("0x")
        .is_some_and(|body| body.len() == 40 && body.as_bytes().iter().all(u8::is_ascii_hexdigit))
}

/// Refuses the one identity mistake this venue punishes in silence.
///
/// Actions are signed by the API wallet; **state is read for the master account**. Give the
/// API wallet's own address as the master and every subscription is accepted and
/// acknowledged and then produces nothing, while `clearinghouseState` answers with an empty
/// `assetPositions` — which this backend reads as *flat*, deliberately, because that is what
/// an account that has never traded a perp really does answer. The result is a bot that
/// quotes against a permanently flat position while fills accumulate on the venue, with
/// nothing in any log to say why.
///
/// Both addresses are known at build time, so the pair is checked rather than the operator
/// being asked to notice.
fn check_account_identity(
    account_address: &str,
    agent_address: &str,
) -> Result<(), HyperliquidError> {
    if account_address
        .trim()
        .eq_ignore_ascii_case(agent_address.trim())
    {
        return Err(HyperliquidError::Credentials(format!(
            "master_account_address is the API wallet's own address ({agent_address}). \
             Subscriptions and position queries keyed by the API wallet are accepted and \
             return nothing at all, so the connector would report a permanently flat \
             account while orders filled. Use the address of the account that owns the \
             positions — the one that approved this API wallet."
        )));
    }
    Ok(())
}

type SharedSymbolSet = Arc<Mutex<HashSet<String>>>;

/// What a connector without credentials reports: nothing, because it has nothing.
///
/// Deliberately not a stub that pretends. With no API wallet configured, [`Hyperliquid`]
/// refuses every order with [`ErrorKind::OrderError`], so an empty list is the truth.
#[derive(Default)]
pub struct NoOrders;

impl GetOrders for NoOrders {
    fn orders(&self, _symbol: Option<String>) -> Vec<Order> {
        Vec::new()
    }
}

/// The order path, present only when an API wallet is configured.
struct Trading {
    order_manager: SharedOrderManager,
    exchange: Arc<ExchangeClient>,
    account_address: String,
    agent_address: String,
}

pub struct Hyperliquid {
    config: Config,
    symbols: SharedSymbolSet,
    symbol_tx: Sender<String>,
    instruments: SharedInstruments,
    trading: Option<Trading>,
    no_orders: Arc<Mutex<NoOrders>>,
    /// The stream tasks, kept so an orderly stop can end them.
    ///
    /// Aborting is what drops the `PublishEvent` senders those tasks own, and dropping every
    /// sender is what lets the publish task see its channel close and stop last instead of
    /// first (`connector/src/supervision.rs`). Aborting at an arbitrary await point is
    /// acceptable *because* it only happens when the process is stopping — and the shutdown
    /// sweep is a separate task holding its own sender, so it is not cancelled with them.
    tasks: Vec<JoinHandle<()>>,
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

        let trading = match config.credentials_source(std::env::var(CREDENTIALS_ENV).ok()) {
            Some(path) => {
                let credentials = load_credentials(&path, config.account_address.as_deref())?;
                let signer = Arc::new(Signer::from_hex(
                    &credentials.api_wallet_private_key,
                    config.is_mainnet,
                )?);
                let order_manager = Arc::new(Mutex::new(OrderManager::new(&config.order_prefix)?));
                let agent_address = signer.address();
                let account_address = credentials.master_account_address.trim().to_lowercase();
                check_account_identity(&account_address, &agent_address)?;
                if config.coins.is_empty() {
                    // The registration race, made visible. `run_receive_task` publishes
                    // `RegisterInstrument` — and `run_publish_task` answers it with
                    // `SnapshotComplete` — before `Connector::register` has even woken this
                    // backend, and the position a registering bot is handed is whatever the
                    // publish task has already cached. With no coins configured there is
                    // nothing to reconcile before the first registration, so that cache is
                    // empty and the bot starts at a position of zero until the
                    // registration's own reconciliation lands (`AGENTS.md` §4.4).
                    warn!(
                        "No coins are configured while the Hyperliquid order path is armed. \
                         The first bot to register may be told the account is flat before \
                         the connector has asked the venue. List the traded coins in \
                         `coins` so the account is reconciled before any bot attaches."
                    );
                }
                info!(
                    network = if config.is_mainnet { "mainnet" } else { "testnet" },
                    %agent_address,
                    %account_address,
                    credentials = %path,
                    "Hyperliquid order path armed."
                );
                Some(Trading {
                    exchange: Arc::new(ExchangeClient::new(
                        &config.rest_url,
                        signer,
                        Arc::new(NonceSource::new()),
                    )),
                    order_manager,
                    account_address,
                    agent_address,
                })
            }
            None => {
                warn!(
                    "No Hyperliquid credentials are configured ({CREDENTIALS_ENV} or \
                     credentials_path); this connector serves market data and will refuse \
                     every order."
                );
                None
            }
        };

        Ok(Hyperliquid {
            config,
            symbols,
            symbol_tx,
            instruments: Default::default(),
            trading,
            no_orders: Arc::new(Mutex::new(NoOrders)),
            tasks: Vec::new(),
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
        match &self.trading {
            Some(trading) => trading.order_manager.clone(),
            None => self.no_orders.clone(),
        }
    }

    fn run(&mut self, ev_tx: UnboundedSender<PublishEvent>) {
        let mut public = PublicStream::new(
            self.config.public_url.clone(),
            self.config.rest_url.clone(),
            self.config.l2_book,
            self.symbols.clone(),
            self.symbol_tx.subscribe(),
            self.instruments.clone(),
            ev_tx.clone(),
        );
        self.tasks.push(tokio::spawn(async move {
            public.run().await;
        }));

        let Some(trading) = &self.trading else {
            return;
        };

        // The agent-approval probe, before anything is signed. An unapproved API wallet
        // produces a well-formed signature and the venue answers every single order with
        // "User or API Wallet 0x… does not exist" — which reads as a bad key, or a bad
        // chain id, or a bad hash, and is none of those. One question at startup replaces
        // that whole search.
        let rest_url = self.config.rest_url.clone();
        let account = trading.account_address.clone();
        let agent = trading.agent_address.clone();
        let probe_tx = ev_tx.clone();
        self.tasks.push(tokio::spawn(async move {
            match rest::agent_is_approved(&rest_url, &account, &agent).await {
                Ok(true) => {
                    info!(%agent, %account, "Hyperliquid lists this API wallet as approved.")
                }
                Ok(false) => {
                    let error = HyperliquidError::Credentials(format!(
                        "Hyperliquid does not list {agent} as an approved API wallet for \
                         {account}. Every order will be refused with \"User or API Wallet \
                         does not exist\". Approve the agent in the Hyperliquid UI (API \
                         section); it cannot be done with the agent key itself."
                    ));
                    error!(%agent, %account, "The Hyperliquid API wallet is not approved for this account.");
                    publish_error(&probe_tx, ErrorKind::CriticalConnectionError, &error);
                }
                Err(error) => {
                    // Not fatal: the venue may simply be unreachable this second, and the
                    // order path is allowed to find out for itself.
                    warn!(
                        ?error,
                        "Couldn't check whether the Hyperliquid API wallet is approved."
                    );
                }
            }
        }));

        let mut private = PrivateStream::new(
            self.config.public_url.clone(),
            self.config.rest_url.clone(),
            trading.account_address.clone(),
            trading.order_manager.clone(),
            self.symbols.clone(),
            self.symbol_tx.subscribe(),
            ev_tx,
            trading.exchange.clone(),
            self.instruments.clone(),
            self.config.connect_policy,
        );
        self.tasks.push(tokio::spawn(async move {
            private.run().await;
        }));
    }

    fn submit(&self, symbol: String, order: Order, ev_tx: UnboundedSender<PublishEvent>) {
        match &self.trading {
            Some(trading) => private_stream::submit_order(
                symbol,
                order,
                trading.order_manager.clone(),
                trading.exchange.clone(),
                self.instruments.clone(),
                ev_tx,
            ),
            None => reject_order(&symbol, &order, ev_tx),
        }
    }

    fn cancel(&self, symbol: String, order: Order, ev_tx: UnboundedSender<PublishEvent>) {
        match &self.trading {
            Some(trading) => private_stream::cancel_order(
                symbol,
                order,
                trading.order_manager.clone(),
                trading.exchange.clone(),
                self.instruments.clone(),
                ev_tx,
            ),
            None => reject_order(&symbol, &order, ev_tx),
        }
    }

    /// Cancels what the **venue** holds for `symbols`, exactly as a `cancel_all` connect
    /// does — the same [`Sweeper`], so there is one implementation of "clear this coin" and
    /// not two that drift.
    ///
    /// The venue's own open-order list is the source, not this connector's map, and that
    /// matters most here: the orders a dead bot left may have been placed by a previous run
    /// of this process, and a sweep built from `orders()` would cancel none of them.
    ///
    /// `ev_tx` is moved into the task and dropped when it finishes, and the task is returned
    /// so that an orderly stop can wait for it: the drain's deadline is not the sweep's
    /// budget, and while it was, a stop could exit 0 having cancelled part of a grid.
    fn sweep(
        &self,
        symbols: Vec<String>,
        reason: SweepReason,
        ev_tx: UnboundedSender<PublishEvent>,
    ) -> Option<JoinHandle<SweepOutcome>> {
        let Some(trading) = &self.trading else {
            info!(
                ?reason,
                "No Hyperliquid API wallet is configured, so this connector has nothing \
                 resting to sweep."
            );
            // No order path at all: nothing this connector placed can be resting.
            return None;
        };
        info!(
            ?reason,
            bot_id = ?reason.bot_id(),
            ?symbols,
            "Sweeping Hyperliquid's open orders."
        );
        let sweeper = Sweeper::new(
            self.config.rest_url.clone(),
            trading.account_address.clone(),
            trading.order_manager.clone(),
            trading.exchange.clone(),
            self.instruments.clone(),
            ev_tx,
        );
        Some(tokio::spawn(async move {
            let outcome = sweeper.sweep_symbols(&symbols).await;
            info!(
                ?reason,
                ?outcome,
                "Finished sweeping Hyperliquid's open orders."
            );
            outcome
        }))
    }

    /// Ends the stream tasks, so their senders are dropped and the publish task can see the
    /// channel close. See [`Hyperliquid::tasks`].
    fn shutdown(&mut self) {
        let count = self.tasks.len();
        for task in self.tasks.drain(..) {
            task.abort();
        }
        info!(
            tasks = count,
            "Wound down the Hyperliquid streams for an orderly stop."
        );
    }
}

/// Fails an order request loudly, and takes the order back out of the bot's state.
///
/// Reached only when no API wallet is configured — the market-data-only deployment. Answering `Ok` and dropping the request
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
        "No Hyperliquid API wallet is configured; the order was rejected."
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
            check_account_identity,
            load_credentials,
            ordermanager::OrderManager,
            private_stream::ConnectPolicy,
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

        // The order path's fields, too — and the example must not put a first run on
        // mainnet, must not carry a key, and must default to the policy that does not
        // cancel live orders on a transient reconnect.
        assert!(!config.is_mainnet);
        assert_eq!(config.connect_policy, ConnectPolicy::Reconcile);
        assert!(
            config.credentials_path.is_none(),
            "the shipped example must not point at anyone's key file"
        );
        assert!(
            OrderManager::new(&config.order_prefix).is_ok(),
            "the example's order_prefix must be a usable one: {}",
            config.order_prefix
        );
        // The example may *describe* the credentials file — it has to — but no live line of
        // it may assign key material. Commented documentation is fine; an assignment is
        // what someone copies without reading.
        let raw = include_str!("../../examples/hyperliquid.toml");
        for line in raw.lines() {
            let line = line.trim();
            assert!(
                line.starts_with('#') || !line.starts_with("api_wallet_private_key"),
                "the example must document the credentials file without being one: {line}"
            );
        }
    }

    /// The environment wins over the file, so one committed config can serve several
    /// accounts; an empty value is not a value.
    #[test]
    fn the_environment_overrides_the_configured_credentials_path() {
        let config: Config = toml::from_str(
            r#"
            public_url = "wss://api.hyperliquid-testnet.xyz/ws"
            rest_url = "https://api.hyperliquid-testnet.xyz"
            credentials_path = "/from/config.toml"
            "#,
        )
        .unwrap();

        assert_eq!(
            config.credentials_source(None).as_deref(),
            Some("/from/config.toml")
        );
        assert_eq!(
            config
                .credentials_source(Some("/from/env.toml".into()))
                .as_deref(),
            Some("/from/env.toml")
        );
        assert_eq!(
            config.credentials_source(Some("  ".into())).as_deref(),
            Some("/from/config.toml"),
            "an empty environment variable is not an answer"
        );

        // With neither, there is no order path at all — and that is a supported way to run.
        let market_data_only: Config = toml::from_str(
            r#"
            public_url = "wss://api.hyperliquid-testnet.xyz/ws"
            rest_url = "https://api.hyperliquid-testnet.xyz"
            "#,
        )
        .unwrap();
        assert!(market_data_only.credentials_source(None).is_none());
        assert!(
            Hyperliquid::build_from(
                r#"
            public_url = "wss://api.hyperliquid-testnet.xyz/ws"
            rest_url = "https://api.hyperliquid-testnet.xyz"
            "#,
            )
            .is_ok()
        );
    }

    /// **A credentials file for the wrong account is the worst available failure.** Orders
    /// sign and rest correctly while `orderUpdates`, `userFills` and every position query
    /// describe somebody else — so the connector looks healthy and the bot never learns
    /// about a single fill. Refusing at startup makes it one line instead of a day.
    #[test]
    fn credentials_for_a_different_account_are_refused_at_startup() {
        let dir = std::env::temp_dir().join("hl-credentials-test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("creds.toml");
        std::fs::write(
            &path,
            "api_wallet_private_key = \"0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae7\
             84d7bf4f2ff80\"\nmaster_account_address = \"0x70997970C51812dc3A010C7d01b50e0d17\
             dc79C8\"\n",
        )
        .unwrap();
        let path = path.to_str().unwrap();

        // The account it really is: accepted, and case-insensitively.
        load_credentials(path, Some("0x70997970c51812dc3a010c7d01b50e0d17dc79c8")).unwrap();
        load_credentials(path, None).unwrap();

        // `Credentials` deliberately has no `Debug` — it holds a private key — so the
        // error is taken out by hand rather than with `unwrap_err`.
        let Err(error) = load_credentials(path, Some("0x0000000000000000000000000000000000000001"))
        else {
            panic!("credentials for another account must be refused");
        };
        let error = error.to_string();
        assert!(error.contains("account_address"), "{error}");
        // The refusal must not quote the file — it holds a private key.
        assert!(!error.contains("ac0974"), "{error}");
    }

    /// **The one documented Hyperliquid identity mistake, refused at startup.** Subscribing
    /// `orderUpdates`/`userFills` with the API wallet's own address is accepted,
    /// acknowledged, and produces nothing at all; every position query answers empty, which
    /// this backend reads as flat *by design*. The bot then quotes for ever against a
    /// permanently flat book of its own while real fills accumulate on the venue, with no
    /// error anywhere. Both addresses are in hand at build time, so the pair is checked.
    #[test]
    fn the_api_wallets_own_address_is_not_accepted_as_the_master_account() {
        // The Hardhat key used throughout these tests, and the address it derives.
        let agent = "0xf39fd6e51aad88f6f4ce6ab8827279cffFb92266";

        let Err(error) = check_account_identity(agent, agent) else {
            panic!("the agent's own address must not be usable as the master account");
        };
        let error = error.to_string();
        assert!(error.contains("API wallet"), "{error}");
        // Case is not a defence: the venue lowercases what it echoes.
        assert!(check_account_identity(&agent.to_lowercase(), agent).is_err());
        assert!(
            check_account_identity("0x70997970c51812dc3a010c7d01b50e0d17dc79c8", agent).is_ok()
        );
    }

    /// An address the venue cannot answer for is refused where it is read. Hyperliquid does
    /// not reject a malformed `user`: it answers with an empty result, which is
    /// indistinguishable from an account that holds nothing.
    #[test]
    fn a_master_account_address_that_is_not_an_address_is_refused() {
        let dir = std::env::temp_dir().join("hl-credentials-test");
        std::fs::create_dir_all(&dir).unwrap();
        let key = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

        for bad in [
            "0x70997970c51812dc3a010c7d01b50e0d17dc79c", // 39 hex: one short
            "0x70997970c51812dc3a010c7d01b50e0d17dc79c83", // 41 hex: one long
            "70997970c51812dc3a010c7d01b50e0d17dc79c8",  // no 0x
            "0x70997970c51812dc3a010c7d01b50e0d17dc79cg", // not hexadecimal
            "",
        ] {
            let path = dir.join("malformed.toml");
            std::fs::write(
                &path,
                format!("api_wallet_private_key = \"{key}\"\nmaster_account_address = \"{bad}\"\n"),
            )
            .unwrap();
            let Err(error) = load_credentials(path.to_str().unwrap(), None) else {
                panic!("{bad:?} is not an address and must be refused");
            };
            let error = error.to_string();
            assert!(error.contains("master_account_address"), "{error}");
            assert!(
                !error.contains("ac0974"),
                "the key must never be quoted: {error}"
            );
        }
    }

    /// A credentials file that is missing or malformed is refused where it is read, and the
    /// message never echoes the contents.
    #[test]
    fn an_unreadable_credentials_file_is_refused_without_quoting_it() {
        let Err(error) = load_credentials("/nonexistent/hl.toml", None) else {
            panic!("a missing credentials file must be refused");
        };
        let error = error.to_string();
        assert!(error.contains("/nonexistent/hl.toml"), "{error}");

        let dir = std::env::temp_dir().join("hl-credentials-test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("broken.toml");
        std::fs::write(&path, "api_wallet_private_key = \"0xdeadbeef\"\n").unwrap();
        let Err(error) = load_credentials(path.to_str().unwrap(), None) else {
            panic!("a credentials file without an account must be refused");
        };
        let error = error.to_string();
        assert!(error.contains("master_account_address"), "{error}");
        assert!(!error.contains("deadbeef"), "{error}");
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
