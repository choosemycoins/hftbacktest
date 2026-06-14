use std::{
    collections::{HashMap, HashSet},
    num::{ParseFloatError, ParseIntError},
    sync::{Arc, Mutex},
};

use chrono::Utc;
use hftbacktest::types::{
    ErrorKind,
    LiveError,
    LiveEvent,
    Order,
    ReconcileEndpoint,
    ReconcileFrame,
    ReconcileScope,
    Value,
};
use serde::Deserialize;
use thiserror::Error;
use tokio::sync::{broadcast, broadcast::Sender, mpsc::UnboundedSender};
use tracing::error;

use crate::{
    bybit::{
        ordermanager::{OrderManager, SharedOrderManager},
        public_stream::PublicStream,
        reconcile::{frame_from_account, frames_from_orders, frames_from_positions, frames_from_wallet, truncate_ret_msg},
        rest::BybitClient,
        trade_stream::OrderOp,
    },
    connector::{Connector, ConnectorBuilder, GetOrders, PublishEvent},
    utils::{ExponentialBackoff, Retry},
};

#[allow(dead_code)]
mod msg;
mod ordermanager;
mod private_stream;
mod public_stream;
mod reconcile;
mod rest;
mod trade_stream;

#[derive(Error, Debug)]
pub enum BybitError {
    #[error("AssetNotFound")]
    AssetNotFound,
    #[error("AuthError: {code} - {msg}")]
    AuthError { code: i64, msg: String },
    #[error("OrderError: {code} - {msg}")]
    OrderError { code: i64, msg: String },
    #[error("InvalidPxQty: {0}")]
    InvalidPxQty(#[from] ParseFloatError),
    #[error("InvalidOrderId: {0}")]
    InvalidOrderId(ParseIntError),
    #[error("PrefixUnmatched")]
    PrefixUnmatched,
    #[error("OrderNotFound")]
    OrderNotFound,
    #[error("InvalidReqId")]
    InvalidReqId,
    #[error("InvalidArg: {0}")]
    InvalidArg(&'static str),
    #[error("OrderAlreadyExist")]
    OrderAlreadyExist,
    #[error("Serde: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Reqwest: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("Tungstenite: {0}")]
    Tungstenite(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("ConnectionAbort: {0}")]
    ConnectionAbort(String),
    #[error("ConnectionInterrupted")]
    ConnectionInterrupted,
    #[error("OpError: {0}")]
    OpError(String),
    /// A Bybit REST envelope returned a non-zero `retCode`. Carries the RAW code/msg so the
    /// reconcile path can preserve them in `ReconcileFrame::End` (classification is R-M1b's job).
    #[error("OpError: {code} - {msg}")]
    OpErrorCode { code: i64, msg: String },
    /// Open-order pagination hit the hard cap with a non-empty cursor (R-M1a §Q6). Fail-closed:
    /// the caller must NOT present a truncated order set as a complete reconcile.
    #[error("order pagination cap exceeded")]
    PaginationCapExceeded,
    #[error("Config: {0:?}")]
    Config(#[from] toml::de::Error),
}

impl BybitError {
    pub fn to_value(&self) -> Value {
        match self {
            BybitError::AssetNotFound => Value::Empty,
            BybitError::AuthError { code, msg } => Value::Map({
                let mut map = HashMap::new();
                map.insert("code".to_string(), Value::Int(*code));
                map.insert("msg".to_string(), Value::String(msg.clone()));
                map
            }),
            BybitError::OrderError { code, msg } => Value::Map({
                let mut map = HashMap::new();
                map.insert("code".to_string(), Value::Int(*code));
                map.insert("msg".to_string(), Value::String(msg.clone()));
                map
            }),
            BybitError::InvalidPxQty(_) => Value::String(self.to_string()),
            BybitError::InvalidOrderId(_) => Value::String(self.to_string()),
            BybitError::PrefixUnmatched => Value::String(self.to_string()),
            BybitError::OrderNotFound => Value::String(self.to_string()),
            BybitError::InvalidReqId => Value::String(self.to_string()),
            BybitError::InvalidArg(_) => Value::String(self.to_string()),
            BybitError::OrderAlreadyExist => Value::String(self.to_string()),
            BybitError::Serde(_) => Value::String(self.to_string()),
            BybitError::Tungstenite(_) => Value::String(self.to_string()),
            BybitError::ConnectionAbort(_) => Value::String(self.to_string()),
            BybitError::ConnectionInterrupted => Value::String(self.to_string()),
            BybitError::OpError(_) => Value::String(self.to_string()),
            BybitError::OpErrorCode { code, msg } => Value::Map({
                let mut map = HashMap::new();
                map.insert("code".to_string(), Value::Int(*code));
                map.insert("msg".to_string(), Value::String(msg.clone()));
                map
            }),
            BybitError::PaginationCapExceeded => Value::String(self.to_string()),
            BybitError::Reqwest(_) => Value::String(self.to_string()),
            BybitError::Config(_) => Value::String(self.to_string()),
        }
    }
}

#[derive(Deserialize)]
pub struct Config {
    public_url: String,
    private_url: String,
    trade_url: String,
    rest_url: String,
    api_key: String,
    secret: String,
    category: String,
    order_prefix: String,
}

type SharedSymbolSet = Arc<Mutex<HashSet<String>>>;

pub struct Bybit {
    config: Config,
    order_tx: Sender<OrderOp>,
    order_manager: SharedOrderManager,
    symbols: SharedSymbolSet,
    client: BybitClient,
    symbol_tx: Sender<String>,
}

impl Bybit {
    fn connect_public_stream(&self, ev_tx: UnboundedSender<PublishEvent>) {
        // Connects to the public stream for the market data.
        let public_url = self.config.public_url.clone();
        let symbol_tx = self.symbol_tx.clone();

        tokio::spawn(async move {
            let _ = Retry::new(ExponentialBackoff::default())
                .error_handler(|error: BybitError| {
                    error!(?error, "An error occurred in the public stream connection.");
                    ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                            ErrorKind::ConnectionInterrupted,
                            error.to_value(),
                        ))))
                        .unwrap();
                    Ok(())
                })
                .retry(|| async {
                    let mut stream = PublicStream::new(ev_tx.clone(), symbol_tx.subscribe());
                    if let Err(error) = stream.connect(&public_url).await {
                        error!(?error, "A connection error occurred.");
                        ev_tx
                            .send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                                ErrorKind::ConnectionInterrupted,
                                error.to_value(),
                            ))))
                            .unwrap();
                    } else {
                        ev_tx
                            .send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::new(
                                ErrorKind::ConnectionInterrupted,
                            ))))
                            .unwrap();
                    }
                    Err::<(), BybitError>(BybitError::ConnectionInterrupted)
                })
                .await;
        });
    }

    fn connect_private_stream(&self, ev_tx: UnboundedSender<PublishEvent>) {
        // Connects to the private stream for the position and order data.
        let private_url = self.config.private_url.clone();
        let api_key = self.config.api_key.clone();
        let secret = self.config.secret.clone();
        let category = self.config.category.clone();
        let order_manager = self.order_manager.clone();
        let instruments = self.symbols.clone();
        let client = self.client.clone();
        let symbol_tx = self.symbol_tx.clone();

        tokio::spawn(async move {
            let _ = Retry::new(ExponentialBackoff::default())
                .error_handler(|error: BybitError| {
                    error!(
                        ?error,
                        "An error occurred in the private stream connection."
                    );
                    ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                            ErrorKind::ConnectionInterrupted,
                            error.to_value(),
                        ))))
                        .unwrap();
                    Ok(())
                })
                .retry(|| async {
                    let mut stream = private_stream::PrivateStream::new(
                        api_key.clone(),
                        secret.clone(),
                        ev_tx.clone(),
                        order_manager.clone(),
                        instruments.clone(),
                        category.clone(),
                        client.clone(),
                        symbol_tx.subscribe(),
                    );

                    // // todo: fix the operation order.
                    //
                    // // Cancel all orders before connecting to the stream in order to start with the
                    // // clean state.
                    // stream.cancel_all(&category).await?;
                    //
                    // // Fetches the initial states such as positions and open orders.
                    // stream.get_all_position(&category).await?;

                    stream.connect(&private_url).await?;

                    Ok(())
                })
                .await;
        });
    }

    fn connect_trade_stream(&self, ev_tx: UnboundedSender<PublishEvent>) {
        let trade_url = self.config.trade_url.clone();
        let api_key = self.config.api_key.clone();
        let secret = self.config.secret.clone();
        let order_manager = self.order_manager.clone();
        let order_tx = self.order_tx.clone();

        tokio::spawn(async move {
            let _ = Retry::new(ExponentialBackoff::default())
                .error_handler(|error: BybitError| {
                    error!(?error, "An error occurred in the trade stream connection.");
                    ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                            ErrorKind::ConnectionInterrupted,
                            error.to_value(),
                        ))))
                        .unwrap();
                    Ok(())
                })
                .retry(|| async {
                    let mut stream = trade_stream::TradeStream::new(
                        api_key.clone(),
                        secret.clone(),
                        ev_tx.clone(),
                        order_manager.clone(),
                        order_tx.subscribe(),
                    );
                    stream.connect(&trade_url).await?;
                    Ok(())
                })
                .await;
        });
    }
}

impl ConnectorBuilder for Bybit {
    type Error = BybitError;

    fn build_from(config: &str) -> Result<Self, Self::Error> {
        let config: Config = toml::from_str(config)?;
        if config.order_prefix.contains("/") {
            panic!("order prefix cannot include '/'.");
        }
        if config.order_prefix.len() > 8 {
            panic!("order prefix length should be not greater than 8.");
        }
        let (order_tx, _) = broadcast::channel(500);
        let (symbol_tx, _) = broadcast::channel(500);
        let order_manager = Arc::new(Mutex::new(OrderManager::new(&config.order_prefix)));
        let client = BybitClient::new(&config.rest_url, &config.api_key, &config.secret);
        Ok(Bybit {
            config,
            order_tx,
            order_manager,
            client,
            symbols: Default::default(),
            symbol_tx,
        })
    }
}

impl Connector for Bybit {
    fn register(&mut self, symbol: String) {
        let mut symbols = self.symbols.lock().unwrap();
        if !symbols.contains(&symbol) {
            symbols.insert(symbol.clone());
            self.symbol_tx.send(symbol).unwrap();
        }
    }

    fn order_manager(&self) -> Arc<Mutex<dyn GetOrders + Send + 'static>> {
        self.order_manager.clone()
    }

    fn run(&mut self, ev_tx: UnboundedSender<PublishEvent>) {
        self.connect_public_stream(ev_tx.clone());
        self.connect_private_stream(ev_tx.clone());
        self.connect_trade_stream(ev_tx);
    }

    fn submit(&self, asset: String, order: Order, ev_tx: UnboundedSender<PublishEvent>) {
        match self
            .order_manager
            .lock()
            .unwrap()
            .new_order(&asset, &self.config.category, order)
        {
            Ok(bybit_order) => {
                self.order_tx
                    .send(OrderOp {
                        op: "order.create",
                        bybit_order,
                    })
                    .unwrap();
            }
            Err(error) => {
                ev_tx
                    .send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                        ErrorKind::OrderError,
                        error.to_value(),
                    ))))
                    .unwrap();
            }
        }
    }

    fn cancel(&self, asset: String, order: Order, ev_tx: UnboundedSender<PublishEvent>) {
        match self.order_manager.lock().unwrap().cancel_order(
            &asset,
            &self.config.category,
            order.order_id,
        ) {
            Ok(bybit_order) => {
                self.order_tx
                    .send(OrderOp {
                        op: "order.cancel",
                        bybit_order,
                    })
                    .unwrap();
            }
            Err(error) => {
                ev_tx
                    .send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                        ErrorKind::OrderError,
                        error.to_value(),
                    ))))
                    .unwrap();
            }
        }
    }

    fn reconcile(
        &self,
        bot_id: u64,
        symbol: String,
        request_id: u64,
        scope: ReconcileScope,
        ev_tx: UnboundedSender<PublishEvent>,
    ) {
        // Spawn so the synchronous dispatch loop is never blocked (mirrors private_stream spawn).
        let client = self.client.clone();
        let category = self.config.category.clone();

        tokio::spawn(async move {
            // 1. Fetch all requested REST endpoints into memory FIRST (every `.await` happens here,
            //    BEFORE any frame is emitted), so the frame burst below contains no yield points and
            //    cannot be interleaved with live Feed/Order events (atomicity invariant, §5e).
            let want_position =
                matches!(scope, ReconcileScope::All | ReconcileScope::Position);
            let want_orders =
                matches!(scope, ReconcileScope::All | ReconcileScope::OpenOrders);
            let want_wallet =
                matches!(scope, ReconcileScope::All | ReconcileScope::Wallet);

            // Each fetch yields either rows (collected as frames) or a fail-closed reason: the raw
            // (endpoint, BybitError). The FIRST failure aborts the reconcile (fail-closed End).
            let mut body: Vec<ReconcileFrame> = Vec::new();
            let mut account_frame: Option<ReconcileFrame> = None;
            let mut failure: Option<(ReconcileEndpoint, BybitError)> = None;

            if want_position && failure.is_none() {
                match client.get_positions_for_reconcile(&category).await {
                    Ok(positions) => match frames_from_positions(&positions) {
                        Ok(frames) => body.extend(frames),
                        Err(e) => failure = Some((ReconcileEndpoint::Position, e)),
                    },
                    Err(e) => failure = Some((ReconcileEndpoint::Position, e)),
                }
            }

            if want_orders && failure.is_none() {
                match client.get_open_orders(&category).await {
                    Ok(orders) => body.extend(frames_from_orders(&orders)),
                    Err(e) => failure = Some((ReconcileEndpoint::OpenOrders, e)),
                }
            }

            if want_wallet && failure.is_none() {
                match client.get_wallet_balance().await {
                    Ok(resp) => {
                        if resp.ret_code != 0 {
                            failure = Some((
                                ReconcileEndpoint::WalletBalance,
                                BybitError::OpErrorCode {
                                    code: resp.ret_code,
                                    msg: resp.ret_msg,
                                },
                            ));
                        } else {
                            match resp.result.list {
                                // Guard `null`/empty list — never `.unwrap()` (regression vs the
                                // panic that `exit(1)`s the connector on a transient hiccup).
                                Some(list) if !list.is_empty() => {
                                    let acct = &list[0];
                                    match frame_from_account(acct) {
                                        Ok(f) => {
                                            account_frame = Some(f);
                                            match frames_from_wallet(acct) {
                                                Ok(coins) => body.extend(coins),
                                                Err(e) => {
                                                    failure =
                                                        Some((ReconcileEndpoint::WalletBalance, e))
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            failure = Some((ReconcileEndpoint::WalletBalance, e))
                                        }
                                    }
                                }
                                _ => {
                                    // Empty/null wallet list on a zero-retCode response is unexpected
                                    // → fail-closed (do not present an empty wallet as authoritative).
                                    failure = Some((
                                        ReconcileEndpoint::WalletBalance,
                                        BybitError::OpError("empty wallet-balance list".to_string()),
                                    ));
                                }
                            }
                        }
                    }
                    Err(e) => failure = Some((ReconcileEndpoint::WalletBalance, e)),
                }
            }

            let snapshot_ts_ns = Utc::now().timestamp_nanos_opt().unwrap_or(0);

            // 2. Emit the frame burst contiguously (no `.await` between sends; the unbounded mpsc
            //    `send` never yields). Any send failure aborts — but the channel is closed only on
            //    shutdown, so we log and stop rather than spin.
            let result: Result<(), ()> = (|| {
                send_frame(&ev_tx, PublishEvent::BatchStart(bot_id))?;
                send_reconcile(
                    &ev_tx,
                    bot_id,
                    &symbol,
                    ReconcileFrame::Begin {
                        request_id,
                        snapshot_ts_ns,
                    },
                )?;

                if let Some((endpoint, err)) = failure {
                    // Fail-closed terminator with RAW retCode/retMsg preserved (R-M1b classifies).
                    let (ret_code, ret_msg) = raw_code_msg(&err);
                    send_reconcile(
                        &ev_tx,
                        bot_id,
                        &symbol,
                        ReconcileFrame::End {
                            request_id,
                            ok: false,
                            endpoint,
                            ret_code,
                            ret_msg: truncate_ret_msg(&ret_msg),
                            http_status: 200,
                        },
                    )?;
                    send_frame(&ev_tx, PublishEvent::BatchEnd(bot_id))?;
                    // Out-of-band fatal signal IN ADDITION to End{ok:false} (§4 failure-surfacing).
                    send_frame(
                        &ev_tx,
                        PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                            ErrorKind::Custom(ret_code.unwrap_or(-1)),
                            err.to_value(),
                        ))),
                    )?;
                    return Ok(());
                }

                if let Some(acct) = account_frame {
                    send_reconcile(&ev_tx, bot_id, &symbol, acct)?;
                }
                for frame in body {
                    send_reconcile(&ev_tx, bot_id, &symbol, frame)?;
                }
                send_reconcile(
                    &ev_tx,
                    bot_id,
                    &symbol,
                    ReconcileFrame::End {
                        request_id,
                        ok: true,
                        endpoint: ReconcileEndpoint::None,
                        ret_code: None,
                        ret_msg: String::new(),
                        http_status: 200,
                    },
                )?;
                send_frame(&ev_tx, PublishEvent::BatchEnd(bot_id))?;
                Ok(())
            })();

            if result.is_err() {
                error!(
                    request_id,
                    "Reconcile reply send failed (channel closed); reconcile aborted."
                );
            }
        });
    }
}

/// Extracts raw `(ret_code, ret_msg)` from a reconcile [`BybitError`], preserving Bybit's original
/// code/message verbatim (no classification — that is R-M1b's responsibility).
fn raw_code_msg(err: &BybitError) -> (Option<i64>, String) {
    match err {
        BybitError::OpErrorCode { code, msg } => (Some(*code), msg.clone()),
        BybitError::PaginationCapExceeded => (None, "order pagination cap exceeded".to_string()),
        other => (None, other.to_string()),
    }
}

#[inline]
fn send_frame(
    ev_tx: &UnboundedSender<PublishEvent>,
    ev: PublishEvent,
) -> Result<(), ()> {
    ev_tx.send(ev).map_err(|_| ())
}

#[inline]
fn send_reconcile(
    ev_tx: &UnboundedSender<PublishEvent>,
    bot_id: u64,
    symbol: &str,
    frame: ReconcileFrame,
) -> Result<(), ()> {
    ev_tx
        .send(PublishEvent::LiveEventTo {
            id: bot_id,
            event: LiveEvent::Reconcile {
                symbol: symbol.to_string(),
                frame,
            },
        })
        .map_err(|_| ())
}
