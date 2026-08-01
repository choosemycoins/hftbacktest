//! The catalog: `GET /api/v1/orderBooks`, which is what turns a symbol into an address.
//!
//! On this venue resolution is not validation that could be skipped — **`market_id` is the
//! addressing** (design note §3.5). A symbol that does not resolve has nothing to subscribe
//! to, and a subscription to an id nobody meant is answered `Invalid Channel` *without
//! closing the socket*, so the mistake looks like an instrument that is simply quiet.
//!
//! Matching is **exact, case included**, and what comes back is the string the caller asked
//! about: a resolve that answered under a different spelling would address the market under a
//! name no bot registered, and `connector/src/main.rs` looks the bot's depth up by the name
//! the bot registered — so the events would be dropped on the floor with nothing said. See
//! [`MarketInfo::symbol`].
//!
//! The same round trip carries the price and size decimals, which are the tick and lot the
//! venue quantises to. They are **logged, not enforced**: the bot chooses the tick and lot
//! it registers (`connector/src/main.rs` builds the fused depth from `RegisterInstrument`),
//! and nothing in the `Connector` trait lets this backend see or correct them —
//! `Connector::register` carries a symbol and nothing else (`connector/src/connector.rs:63`).
//! A bot whose lot is coarser than the venue's loses every level below it *before* it can be
//! published, which no amount of resending fixes — measured on Hyperliquid at 3 events in 10.
//! **The log is the whole defence**: the resolve prints both numbers per symbol and
//! `connector/examples/lighter.toml` says to copy them. Nothing downstream can check that the
//! bot did — in particular [`crate::lighter::depth::MirrorCounts`] cannot, because the mirror
//! is built from these same catalog decimals and so compares the venue only against its own
//! grid (design note §3.5 claims otherwise; it is wrong, and that module says why).

use std::time::Duration;

use serde::Deserialize;
use tracing::info;

use crate::lighter::{
    LighterError,
    private_msg::{AccountOrder, parse_active_orders},
};

/// How long the catalog round trip may take.
///
/// Bounded because `reqwest` has no default timeout and this runs inside the stream's read
/// loop: an unbounded fetch would hold the keepalive as well, and the venue drops a
/// connection that has not written for two minutes.
pub const CATALOG_TIMEOUT: Duration = Duration::from_secs(15);

/// What the catalog says about one market.
#[derive(Clone, Debug, PartialEq)]
pub struct MarketInfo {
    /// **The string the caller asked about**, which is also the catalog's, because the match
    /// is exact ([`resolve_one`]).
    ///
    /// Load-bearing, and it is the one field with no venue meaning: every frame is routed by
    /// [`Self::market_id`], but this is what the mirror, the subscription tracker and every
    /// `LiveEvent::Feed` are keyed by — and `connector/src/main.rs` keys the bot's fused
    /// depth by the string the **bot** registered. A resolve that answers under any other
    /// spelling produces a symbol whose events reach nobody, silently.
    pub symbol: String,
    /// The address. Every subscription and every frame is keyed by this.
    pub market_id: i64,
    /// `supported_price_decimals`: the venue's price grid is `10^-price_decimals`.
    pub price_decimals: i64,
    /// `supported_size_decimals`: the venue's size grid is `10^-size_decimals`.
    pub size_decimals: i64,
}

impl MarketInfo {
    /// The finest price increment the venue quotes on — what the bot should register as its
    /// `tick_size`.
    pub fn tick_size(&self) -> f64 {
        10f64.powi(-(self.price_decimals as i32))
    }

    /// The finest size increment the venue will quote — the bot's `lot_size`.
    pub fn lot_size(&self) -> f64 {
        10f64.powi(-(self.size_decimals as i32))
    }
}

#[derive(Deserialize)]
struct Catalog {
    #[serde(default)]
    order_books: Vec<Row>,
}

#[derive(Deserialize)]
struct Row {
    symbol: String,
    market_id: i64,
    #[serde(default)]
    status: String,
    #[serde(default)]
    supported_price_decimals: i64,
    #[serde(default)]
    supported_size_decimals: i64,
}

/// What a resolve produced: the markets that resolved, and per symbol the reason one did not.
pub type Resolution = (Vec<MarketInfo>, Vec<(String, LighterError)>);

/// Matches requested symbols against an already-fetched catalog.
///
/// Split from the HTTP so the interesting half is testable without a network. Returns what
/// resolved and, **per symbol**, what did not: an unknown symbol takes only itself down.
/// All-or-nothing is the shape Hyperliquid had to roll back — one bot registering a typo
/// gave an endless reconnect loop with no market data for anybody (design note §3.5).
pub fn match_catalog(symbols: &[String], catalog: &str) -> Result<Resolution, LighterError> {
    let catalog: Catalog = serde_json::from_str(catalog)?;
    if catalog.order_books.is_empty() {
        return Err(LighterError::CatalogUnavailable(
            "the catalog carries no `order_books`".to_string(),
        ));
    }

    let mut resolved = Vec::new();
    let mut refused = Vec::new();
    for symbol in symbols {
        match resolve_one(symbol, &catalog.order_books) {
            Ok(info) => resolved.push(info),
            Err(error) => refused.push((symbol.clone(), error)),
        }
    }
    Ok((resolved, refused))
}

/// A symbol with the punctuation the venue's spot names carry taken out, so `SKY/USDC` and
/// `SKYUSDC` compare equal.
fn strip_separators(symbol: &str) -> String {
    symbol
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect()
}

fn resolve_one(symbol: &str, rows: &[Row]) -> Result<MarketInfo, LighterError> {
    // **Exact, case included** — see the invariant on [`MarketInfo::symbol`]. A case-folded
    // match resolved and then addressed the market under a name no bot had registered, which
    // is a symbol with no data, no error and no end: `main.rs` looks the bot's fused depth up
    // by the string the bot registered, so every event published under the venue's spelling
    // was dropped on the floor by `handle_ev`.
    let Some(row) = rows.iter().find(|r| r.symbol == symbol) else {
        // A spelling that differs only in case is the one near miss worth its own sentence:
        // it is the mistake `examples/lighter.toml` used to invite, and the fix is one
        // keystroke rather than a different instrument.
        if let Some(row) = rows.iter().find(|r| r.symbol.eq_ignore_ascii_case(symbol)) {
            return Err(LighterError::UnknownSymbol(format!(
                "{symbol} is not how Lighter spells it: the venue's own spelling is {}, and \
                 the match is exact. Every event is keyed by the symbol the bot registered, \
                 so a spelling the venue does not use is a book no bot ever sees",
                row.symbol
            )));
        }
        // Point at the likely mistake rather than only refusing. The common one is a quote
        // suffix carried over from another venue: Lighter perps are bare base assets, and
        // its spot markets are the ones that carry a quote.
        let wanted = symbol.to_uppercase();
        let near: Vec<&str> = rows
            .iter()
            .map(|r| r.symbol.as_str())
            .filter(|listed| {
                let listed = listed.to_uppercase();
                // `SKYUSDC` for `SKY/USDC` — the separator is the whole difference — and
                // `CRVUSDT` for `CRV`, a perp name carried over from a venue that quotes it.
                strip_separators(&listed) == strip_separators(&wanted)
                    || (wanted.starts_with(&listed) && wanted != listed)
            })
            .take(3)
            .collect();
        return Err(LighterError::UnknownSymbol(if near.is_empty() {
            format!("{symbol} is not listed on Lighter")
        } else {
            format!(
                "{symbol} is not listed on Lighter; did you mean {}?",
                near.join(", ")
            )
        }));
    };
    if row.status != "active" {
        // A listed-but-dead market is subscribable and silent: 18 of mainnet's 228 were
        // `inactive` when this was written. Subscribing to one is a symbol with no data on a
        // healthy connection, which is the failure this whole module exists to prevent.
        return Err(LighterError::UnknownSymbol(format!(
            "{} is {} on Lighter, not active; it would subscribe and never produce data",
            row.symbol, row.status
        )));
    }
    Ok(MarketInfo {
        // The **caller's** string, which the exact match above makes byte-identical to
        // `row.symbol`. Written this way so the invariant holds by construction: whatever a
        // later matcher does, what comes back is what was asked about, because that is what
        // every caller keys its own maps by.
        symbol: symbol.to_string(),
        market_id: row.market_id,
        price_decimals: row.supported_price_decimals,
        size_decimals: row.supported_size_decimals,
    })
}

/// Fetches the catalog and resolves `symbols` against it.
///
/// A transport failure refuses **every** symbol with [`LighterError::CatalogUnavailable`],
/// which is deliberately not a listing verdict: the caller leaves those symbols pending and
/// asks again, rather than writing them off for the life of the connection.
pub async fn resolve_symbols(rest_url: &str, symbols: &[String]) -> Resolution {
    match fetch_catalog(rest_url).await {
        Ok(catalog) => match match_catalog(symbols, &catalog) {
            Ok((resolved, refused)) => {
                for info in &resolved {
                    // Logged per resolve, because these two numbers are what the bot has to
                    // register and nothing in the trait lets this backend check that it did.
                    info!(
                        symbol = %info.symbol,
                        market_id = info.market_id,
                        tick_size = info.tick_size(),
                        lot_size = info.lot_size(),
                        price_decimals = info.price_decimals,
                        size_decimals = info.size_decimals,
                        "Resolved a Lighter instrument. Register these tick and lot sizes; a \
                         coarser one silently drops levels the bot then never sees."
                    );
                }
                (resolved, refused)
            }
            Err(error) => refuse_all(symbols, &error.to_string()),
        },
        Err(error) => refuse_all(symbols, &error.to_string()),
    }
}

fn refuse_all(symbols: &[String], reason: &str) -> Resolution {
    (
        Vec::new(),
        symbols
            .iter()
            .map(|symbol| {
                (
                    symbol.clone(),
                    LighterError::CatalogUnavailable(format!(
                        "couldn't read the Lighter catalog for {symbol}: {reason}"
                    )),
                )
            })
            .collect(),
    )
}

async fn fetch_catalog(rest_url: &str) -> Result<String, LighterError> {
    Ok(reqwest::Client::builder()
        .timeout(CATALOG_TIMEOUT)
        .build()?
        .get(format!("{rest_url}/api/v1/orderBooks"))
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?)
}

// ========================================================================================
// Phase 2 order-path REST: nextNonce, sendTx, and the tx verdict (§3.3, §4.11).
//
// Three endpoints, none of which needs the auth token (the SDK marks all three
// `auth_settings = []`; auth is a private-WS and accountLimits concern). The interesting
// halves — how an HTTP outcome maps onto the nonce counter, and where the *real* order
// verdict lives — are pure functions of the response bytes, unit-tested below without a
// socket. The async wrappers are the thin I/O shell around them.
// ========================================================================================

/// How long an order-path REST call may take.
///
/// Tighter than [`CATALOG_TIMEOUT`] because these are on the critical order path and the
/// measured `sendTx` RTT is 285–480 ms (§2.7); an unbounded call here would stall the slot.
pub const ORDER_REST_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Deserialize)]
struct NextNonceBody {
    nonce: i64,
}

/// Parses `GET /api/v1/nextNonce` → the slot's next sequential nonce (§3.3).
///
/// The counter starts at 1 and is small (`{"code":200,"nonce":1}`), not a ten-digit global —
/// which is the shape that confirmed the per-slot sequential model in the first place (§3.3).
pub fn parse_next_nonce(body: &str) -> Result<i64, LighterError> {
    let parsed: NextNonceBody = serde_json::from_str(body)?;
    Ok(parsed.nonce)
}

/// The `sendTx` HTTP-200 acknowledgement — **which is not a verdict** (§4.11).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendTxAck {
    /// The 40-byte tx hash, hex. The handle to ask `GET /tx` for the real verdict later.
    pub tx_hash: String,
    /// The venue's own rate-limit report, a JSON string nested in the `message` field (§2.7).
    /// Parsed out so mainnet's throttle is observable from day one (§6.Б.1); testnet reports
    /// "Ratelimit is off".
    pub ratelimit: Option<String>,
    pub predicted_execution_time_ms: Option<i64>,
}

/// What a `sendTx` POST outcome means for the nonce counter and the order.
///
/// This is the join between HTTP and [`crate::lighter::nonce`]. It exists because a `sendTx`
/// response is read on **two** axes that the naive path conflates: did the sequencer *consume
/// the nonce*, and did an *order* result. Only the first is knowable here (§4.11).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendOutcome {
    /// HTTP 200 with a `tx_hash`: the sequencer took the transaction and **spent the nonce**.
    /// Whether an *order* resulted is a private-channel question (§4.11) — this says only that
    /// the nonce is consumed, so the slot advances its counter.
    Accepted(SendTxAck),
    /// `21104 invalid nonce` (HTTP 400): a repeat or a skip. The nonce was **not** consumed
    /// and the local counter disagrees with the venue — resync from `nextNonce` (§2.7, §3.3).
    InvalidNonce { message: String },
    /// The fate is unknown: a 5xx / 408 / a 200 body with no `tx_hash`, or any other 4xx. The
    /// slot must **not** guess the counter forward (§3.3); it hard-refreshes from `nextNonce`
    /// and reconciles the order via `account_all_orders`.
    Ambiguous { status: u16, detail: String },
}

#[derive(Deserialize)]
struct RespSendTx {
    #[serde(default)]
    code: i64,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    tx_hash: String,
    #[serde(default)]
    predicted_execution_time_ms: Option<i64>,
}

#[derive(Deserialize)]
struct RateLimitReport {
    #[serde(default)]
    ratelimit: Option<String>,
}

/// The body of an HTTP-200 `sendTx` into a [`SendTxAck`]. See [`classify_send_outcome`] for
/// the full status-aware mapping; this is only the success shape.
pub fn parse_send_tx_ack(body: &str) -> Result<SendTxAck, LighterError> {
    let resp: RespSendTx = serde_json::from_str(body)?;
    if resp.tx_hash.is_empty() {
        return Err(LighterError::Frame(format!(
            "a sendTx 200 carried no tx_hash (code {}): {:?}",
            resp.code, resp.message
        )));
    }
    let ratelimit = resp
        .message
        .as_deref()
        .and_then(|m| serde_json::from_str::<RateLimitReport>(m).ok())
        .and_then(|report| report.ratelimit);
    Ok(SendTxAck {
        tx_hash: resp.tx_hash,
        ratelimit,
        predicted_execution_time_ms: resp.predicted_execution_time_ms,
    })
}

/// Classifies a `sendTx` HTTP status + body into a [`SendOutcome`].
///
/// The one rule that matters: HTTP 200 is **not** a verdict about the order — it is a verdict
/// only about the nonce (§4.11). The `21104` path is singled out because it is the one 400
/// that leaves the nonce untouched (§2.7), and mapping it to `Ambiguous` would make the slot
/// re-fetch `nextNonce` needlessly (harmless but wasteful); mapping any *other* failure to
/// `Accepted` would be the dangerous mistake, so everything unrecognised is `Ambiguous`.
pub fn classify_send_outcome(status: u16, body: &str) -> SendOutcome {
    if (200..300).contains(&status) {
        // A clean 200 that reads as an ack (has a `tx_hash`): the sequencer spent the nonce.
        // A 200 whose body has no `tx_hash`, or is not JSON, is NOT a success — its fate is
        // unknown, so it is ambiguous, not accepted. Fail closed (§3.3).
        return match parse_send_tx_ack(body) {
            Ok(ack) => SendOutcome::Accepted(ack),
            Err(_) => SendOutcome::Ambiguous {
                status,
                detail: clip(body),
            },
        };
    }
    // The one rejection that leaves the nonce untouched is `21104` (§2.7); every other
    // non-2xx is ambiguous for the counter and forces a `nextNonce` resync.
    if let Ok(err) = serde_json::from_str::<ApplicationError>(body)
        && err.code == INVALID_NONCE_CODE
    {
        return SendOutcome::InvalidNonce {
            message: err.message,
        };
    }
    SendOutcome::Ambiguous {
        status,
        detail: clip(body),
    }
}

/// `21104 invalid nonce` — the one `sendTx` rejection that does not spend the nonce (§2.7).
const INVALID_NONCE_CODE: i64 = 21104;

/// Bounds a response body carried in a [`SendOutcome::Ambiguous`] so a log line cannot be
/// unbounded. Internal only — never published to the bots.
fn clip(body: &str) -> String {
    body.chars().take(200).collect()
}

/// The **only** place an order's verdict lives (§4.11): `event_info.ae`, JSON nested inside a
/// JSON string inside a JSON field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxVerdict {
    /// `event_info.ae` is the empty string — the application accepted it. (The *order* still
    /// only exists once `account_all_orders` lists it; this rules out an application error.)
    Accepted,
    /// `event_info.ae` carries an application error — the §4.11 trap made legible. The tx got
    /// HTTP 200 and spent a nonce but produced no order and never will. `code`/`message` are
    /// the venue's, e.g. `21728 "client order index already exists"`.
    Rejected { code: i64, message: String },
    /// No verdict yet: `event_info` is absent or empty. Not a failure — keep waiting within
    /// the deadline.
    Pending,
}

#[derive(Deserialize)]
struct EnrichedTx {
    #[serde(default)]
    event_info: Option<String>,
}

#[derive(Deserialize)]
struct EventInfo {
    #[serde(default)]
    ae: Option<String>,
}

#[derive(Deserialize)]
struct ApplicationError {
    #[serde(default)]
    code: i64,
    #[serde(default)]
    message: String,
}

/// Reads the real order verdict out of a `GET /api/v1/tx?by=hash` body (§4.11).
///
/// `tx.status` is deliberately never consulted: it reads `2` for an accepted **and** a
/// rejected transaction, which is exactly the trap. The verdict is `event_info.ae`, and both
/// `event_info` and `ae` are JSON *strings* that must be parsed a second and third time.
pub fn parse_tx_verdict(body: &str) -> Result<TxVerdict, LighterError> {
    let tx: EnrichedTx = serde_json::from_str(body)?;
    // No `event_info`, or an empty one: the tx has not been processed into a verdict yet.
    let Some(event_info) = tx.event_info.filter(|info| !info.is_empty()) else {
        return Ok(TxVerdict::Pending);
    };
    let info: EventInfo = serde_json::from_str(&event_info)
        .map_err(|error| LighterError::Frame(format!("tx event_info is not JSON: {error}")))?;
    // `ae` absent is treated as Pending, not Accepted: a false accept here is the §4.11
    // failure. Only an `ae` explicitly present and empty is acceptance.
    let Some(ae) = info.ae else {
        return Ok(TxVerdict::Pending);
    };
    if ae.is_empty() {
        return Ok(TxVerdict::Accepted);
    }
    let error: ApplicationError = serde_json::from_str(&ae).map_err(|error| {
        LighterError::Frame(format!("tx event_info.ae is not a JSON error: {error}"))
    })?;
    Ok(TxVerdict::Rejected {
        code: error.code,
        message: error.message,
    })
}

fn order_rest_client() -> Result<reqwest::Client, LighterError> {
    Ok(reqwest::Client::builder()
        .timeout(ORDER_REST_TIMEOUT)
        .build()?)
}

/// `GET /api/v1/nextNonce` for one slot — the authoritative counter value (§3.3).
pub async fn fetch_next_nonce(
    rest_url: &str,
    account_index: i64,
    api_key_index: u8,
) -> Result<i64, LighterError> {
    let body = order_rest_client()?
        .get(format!("{rest_url}/api/v1/nextNonce"))
        .query(&[
            ("account_index", account_index.to_string()),
            ("api_key_index", api_key_index.to_string()),
        ])
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    parse_next_nonce(&body)
}

/// POSTs a signed transaction to `POST /api/v1/sendTx` and classifies the outcome.
///
/// Does **not** `error_for_status`: a `21104` is an HTTP 400 whose body carries the verdict,
/// and discarding it would lose the one signal that tells a rejected nonce from an ambiguous
/// transport failure (§2.7). The status and body go to [`classify_send_outcome`].
pub async fn send_tx(
    rest_url: &str,
    tx_type: u8,
    tx_info: &str,
) -> Result<SendOutcome, LighterError> {
    let response = order_rest_client()?
        .post(format!("{rest_url}/api/v1/sendTx"))
        .form(&[
            ("tx_type", tx_type.to_string()),
            ("tx_info", tx_info.to_string()),
        ])
        .send()
        .await?;
    let status = response.status().as_u16();
    let body = response.text().await?;
    Ok(classify_send_outcome(status, &body))
}

/// `GET /api/v1/tx?by=hash&value=<tx_hash>` → the order verdict (§4.11). Called on a lapsed
/// confirmation deadline to learn *why* an order never appeared — a diagnosis for one REST
/// request (§4.11 note 4).
pub async fn fetch_tx_verdict(rest_url: &str, tx_hash: &str) -> Result<TxVerdict, LighterError> {
    let body = order_rest_client()?
        .get(format!("{rest_url}/api/v1/tx"))
        .query(&[("by", "hash"), ("value", tx_hash)])
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    parse_tx_verdict(&body)
}

/// `GET /api/v1/accountActiveOrders?account_index=..&market_id=..` → the account's open orders
/// on one market (§3.3, §2.7). The REST mirror of `account_all_orders` the slot reads an
/// **ambiguous** send's fate off, before building the next tx: a landed order is on this list
/// and is adopted (never re-sent → no duplicate, the §5 gate), one that never landed is absent.
///
/// Requires auth exactly like the WS subscribe — a tokenless GET is answered `20001` — so the
/// slot mints a short-lived token for it (the token carries the account and key index). A
/// non-2xx status, and a `code != 200` body ([`parse_active_orders`]), are both an `Err` the
/// caller reads as "could not check" and fails closed on; an `Err` is never "no orders".
pub async fn fetch_active_orders(
    rest_url: &str,
    account_index: i64,
    market_index: i32,
    auth_token: &str,
) -> Result<Vec<AccountOrder>, LighterError> {
    let response = order_rest_client()?
        .get(format!("{rest_url}/api/v1/accountActiveOrders"))
        .query(&[
            ("account_index", account_index.to_string()),
            ("market_id", market_index.to_string()),
        ])
        .header("Authorization", auth_token)
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;
    if !status.is_success() {
        return Err(LighterError::Frame(format!(
            "accountActiveOrders HTTP {}: {}",
            status.as_u16(),
            clip(&body)
        )));
    }
    parse_active_orders(&body)
}

// ========================================================================================
// Phase 2 auth-token verification (§3.4). The 8 h token ceiling is enforced ONLY by the
// server: a longer deadline mints without error on the `.so` and is rejected `20013` only at
// use. So a freshly minted token is verified with one cheap authorized GET
// (`GET /api/v1/accountLimits`) BEFORE the connector switches to it — turning the trap from a
// silent "subscribed to nothing" into a loud, attributable error. The verdict is a pure
// function of the HTTP status (unit-tested below); the async wrapper is the I/O shell.
// ========================================================================================

/// The verdict of the cheap authorized GET that checks a token before the connector relies on
/// it (§3.4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthCheck {
    /// HTTP 200 — the server accepts the token. Safe to switch to it.
    Valid,
    /// HTTP 401 — the server rejects it. The measured trap is `20013 invalid deadline` from a
    /// token minted over the 8 h ceiling (§3.4), but any 401 is a rejection: **never** switch
    /// to it, keep the current token and mint again.
    Invalid { code: i64, message: String },
    /// Any other status — a 5xx, a timeout body, a proxy page. **Not** a verdict about the
    /// token (it might be perfectly good): the caller treats this as "could not check", not
    /// "bad", so a transport blip on the check never discards a working token. Fail *open* on
    /// the *check* precisely so the connector fails *closed* on the *token* — it only ever
    /// abandons a token the server explicitly refused.
    Unknown { status: u16, detail: String },
}

/// Classifies a `GET /api/v1/accountLimits` response into an [`AuthCheck`] (§3.4).
///
/// 200 is the only acceptance; 401 is the only rejection (and it names the venue's code, e.g.
/// `20013` for the over-ceiling deadline); everything else is `Unknown` and must not be read
/// as a verdict about the token — treating a 503 as "bad token" would throw away a good token
/// on every upstream hiccup.
pub fn classify_auth_check(status: u16, body: &str) -> AuthCheck {
    if (200..300).contains(&status) {
        return AuthCheck::Valid;
    }
    if status == 401 {
        // Name the venue's own code/message when the body carries them; a 401 with an
        // unreadable body is still a rejection.
        let (code, message) = serde_json::from_str::<ApplicationError>(body)
            .map(|err| (err.code, err.message))
            .unwrap_or((0, clip(body)));
        return AuthCheck::Invalid { code, message };
    }
    AuthCheck::Unknown {
        status,
        detail: clip(body),
    }
}

/// `GET /api/v1/accountLimits` with `token` as the `Authorization` header — the cheap
/// authorized GET that verifies a token before the connector switches to it (§3.4).
///
/// The token itself carries the account and key index, so this needs nothing else; the
/// endpoint is account-scoped, so `account_index` is passed too. A transport error is **not**
/// an [`AuthCheck::Invalid`] — it is a `LighterError` the caller reads as "could not check",
/// so a flaky network never discards a working token.
pub async fn verify_auth_token(
    rest_url: &str,
    account_index: i64,
    token: &str,
) -> Result<AuthCheck, LighterError> {
    let response = order_rest_client()?
        .get(format!("{rest_url}/api/v1/accountLimits"))
        .query(&[("account_index", account_index.to_string())])
        .header("Authorization", token)
        .send()
        .await?;
    let status = response.status().as_u16();
    let body = response.text().await?;
    Ok(classify_auth_check(status, &body))
}

#[cfg(test)]
mod tests {
    use crate::lighter::{
        LighterError,
        fixtures::{ACCOUNT_LIMITS_INVALID_DEADLINE, ACCOUNT_LIMITS_OK, ORDER_BOOKS_CATALOG},
        rest::{
            AuthCheck,
            MarketInfo,
            SendOutcome,
            TxVerdict,
            classify_auth_check,
            classify_send_outcome,
            match_catalog,
            parse_next_nonce,
            parse_send_tx_ack,
            parse_tx_verdict,
        },
    };

    fn wanted(symbols: &[&str]) -> Vec<String> {
        symbols.iter().map(|s| s.to_string()).collect()
    }

    /// The catalog is the addressing: a symbol resolves to an integer, and to the decimals
    /// that are the venue's tick and lot. Real rows, fetched from mainnet.
    #[test]
    fn a_symbol_resolves_to_a_market_id_and_the_venues_grid() {
        let (resolved, refused) =
            match_catalog(&wanted(&["CRV", "ETH"]), ORDER_BOOKS_CATALOG).unwrap();

        assert!(refused.is_empty(), "{refused:?}");
        assert_eq!(
            resolved[0],
            MarketInfo {
                symbol: "CRV".to_string(),
                market_id: 36,
                price_decimals: 5,
                size_decimals: 1,
            }
        );
        assert_eq!(resolved[0].tick_size(), 1e-5);
        assert_eq!(resolved[0].lot_size(), 0.1);

        // ETH's grid is a different shape entirely, which is why neither number may be a
        // constant: 2 price decimals and 4 size decimals.
        assert_eq!(resolved[1].market_id, 0);
        assert_eq!(resolved[1].tick_size(), 0.01);
        assert_eq!(resolved[1].lot_size(), 1e-4);
    }

    /// **The refusal is per symbol, not all-or-nothing** (§3.5). Hyperliquid had to roll
    /// back the opposite: one bot registering a typo gave an endless reconnect loop with no
    /// market data for anyone. Here the unknown symbol is named and refused, and everything
    /// beside it still resolves.
    #[test]
    fn an_unknown_symbol_is_refused_by_name_and_the_rest_still_resolve() {
        let (resolved, refused) =
            match_catalog(&wanted(&["CRV", "NOTACOIN", "HYPE"]), ORDER_BOOKS_CATALOG).unwrap();

        assert_eq!(
            resolved
                .iter()
                .map(|i| i.symbol.as_str())
                .collect::<Vec<_>>(),
            ["CRV", "HYPE"]
        );
        assert_eq!(refused.len(), 1);
        assert_eq!(refused[0].0, "NOTACOIN");
        assert!(
            refused[0].1.to_string().contains("NOTACOIN"),
            "{:?}",
            refused[0].1
        );
        // A listing verdict: it will not change within a connection, so the caller may
        // remember it rather than asking on every wake-up.
        assert!(refused[0].1.is_listing_verdict());
    }

    /// The standard mistake is a quote suffix carried over from another venue. Lighter perps
    /// are bare base assets and its *spot* markets are the ones that carry a quote, so the
    /// refusal points at the near miss instead of only saying no.
    #[test]
    fn a_symbol_spelled_for_another_venue_is_told_what_it_probably_meant() {
        let (_, refused) = match_catalog(&wanted(&["SKYUSDC"]), ORDER_BOOKS_CATALOG).unwrap();
        assert!(
            refused[0].1.to_string().contains("SKY/USDC"),
            "{:?}",
            refused[0].1
        );
    }

    /// **A listed-but-inactive market is the worst answer to accept.** It subscribes without
    /// complaint and produces nothing for ever, which is exactly the failure this venue
    /// makes easiest to reach — 18 of mainnet's 228 markets were inactive when this was
    /// written.
    #[test]
    fn an_inactive_market_is_refused_rather_than_subscribed_to_silence() {
        let (resolved, refused) =
            match_catalog(&wanted(&["HYUNDAI"]), ORDER_BOOKS_CATALOG).unwrap();

        assert!(resolved.is_empty());
        let message = refused[0].1.to_string();
        assert!(message.contains("inactive"), "{message}");
        assert!(message.contains("never produce data"), "{message}");
    }

    /// **A symbol whose case differs from the catalog is refused, not quietly corrected.**
    ///
    /// Case-folding it and keeping the *venue's* spelling was a silent blocker. Everything
    /// downstream is keyed by the resolved string — the mirror, `SubscriptionTracker::mark`,
    /// the `instruments` cache, every `LiveEvent::Feed` — while `main.rs` keys the bot's
    /// fused depth by the string the **bot** registered (`handle_ev`'s `depth.get_mut(symbol)`
    /// returns `vec![]` on a miss). Register `crv` against a catalog that says `CRV` and the
    /// two never met: no event reached any bot, the symbol was never marked subscribed so the
    /// 228-row catalog was re-fetched every housekeeping tick for ever, and nothing was
    /// reported — while `main.rs` still published `SnapshotComplete` over the empty book.
    #[test]
    fn a_symbol_whose_case_differs_from_the_catalog_is_refused_by_name() {
        let (resolved, refused) =
            match_catalog(&wanted(&["crv", "HYPE"]), ORDER_BOOKS_CATALOG).unwrap();

        assert_eq!(
            resolved
                .iter()
                .map(|i| i.symbol.as_str())
                .collect::<Vec<_>>(),
            ["HYPE"],
            "everything beside it still resolves"
        );
        assert_eq!(refused[0].0, "crv");
        let message = refused[0].1.to_string();
        assert!(message.contains("crv"), "{message}");
        assert!(
            message.contains("CRV"),
            "the refusal must name the venue's spelling: {message}"
        );
        // A verdict: reported once and never asked about again on this connection.
        assert!(refused[0].1.is_listing_verdict());
    }

    /// The invariant the blocker above broke, stated as one: a resolved symbol is the string
    /// the **caller** asked about, byte for byte, and every symbol gets exactly one verdict.
    ///
    /// `PublicStream::subscribe_pending` caches by the requested spelling and looks the cache
    /// up by the same, so a resolve that answered under any other name is a symbol that is
    /// silently never subscribed and never reported.
    #[test]
    fn every_resolved_symbol_is_the_string_the_caller_asked_about() {
        let asked = wanted(&["HYPE", "eth", "SKYUSDC", "NOTACOIN"]);
        let (resolved, refused) = match_catalog(&asked, ORDER_BOOKS_CATALOG).unwrap();

        for info in &resolved {
            assert!(
                asked.contains(&info.symbol),
                "{info:?} was never asked about"
            );
        }
        assert_eq!(
            resolved.len() + refused.len(),
            asked.len(),
            "one verdict per symbol, and no symbol left without one"
        );
    }

    /// A catalog that is not one must not read as "no symbol exists": that is a transport
    /// failure, and treating it as a listing verdict would write every symbol off for the
    /// life of the connection.
    #[test]
    fn an_unreadable_catalog_is_not_a_verdict_about_any_symbol() {
        assert!(match_catalog(&wanted(&["CRV"]), "not json").is_err());

        let Err(error) = match_catalog(&wanted(&["CRV"]), r#"{"code":200}"#) else {
            panic!("an empty catalog must not resolve anything");
        };
        assert!(matches!(error, LighterError::CatalogUnavailable(_)));
        assert!(!error.is_listing_verdict());
    }

    // ---- Phase 2 order-path REST (§3.3, §4.11) --------------------------------------

    /// `nextNonce` starts at 1 and is a small per-slot counter, which is the shape that
    /// confirmed the sequential model (§3.3). Real body form.
    #[test]
    fn next_nonce_reads_the_counter() {
        assert_eq!(parse_next_nonce(r#"{"code":200,"nonce":1}"#).unwrap(), 1);
        assert_eq!(parse_next_nonce(r#"{"code":200,"nonce":42}"#).unwrap(), 42);
        assert!(parse_next_nonce("not json").is_err());
    }

    /// The `sendTx` 200 body, verbatim from §2.7. It carries a `tx_hash` and a rate-limit
    /// report nested as a JSON string in `message` — parsed out for observability (§6.Б.1).
    #[test]
    fn a_send_tx_ack_carries_the_hash_and_the_rate_limit_report() {
        // The 80-hex (40-byte) tx_hash form from §2.7; a real one out of step5 of the
        // Phase-0 artifacts.
        let body = r#"{"code": 200, "message": "{\"ratelimit\": \"Ratelimit is off\"}", "tx_hash": "e38dd4f2150419f1b407d1aaa4d494c71ac4cb0d1c1735fa54408b1b1275bf3357c853b0a9b826af", "predicted_execution_time_ms": 1785431671831}"#;
        let ack = parse_send_tx_ack(body).unwrap();
        assert_eq!(
            ack.tx_hash,
            "e38dd4f2150419f1b407d1aaa4d494c71ac4cb0d1c1735fa54408b1b1275bf3357c853b0a9b826af"
        );
        assert_eq!(ack.ratelimit.as_deref(), Some("Ratelimit is off"));
        assert_eq!(ack.predicted_execution_time_ms, Some(1785431671831));
    }

    /// **HTTP 200 with a `tx_hash` is `Accepted` — a nonce verdict, never an order verdict**
    /// (§4.11). The duplicate-`ClientOrderIndex` transaction from the Phase-0 artifacts
    /// (step5 B) got this exact 200 and spent its nonce; the *order* not existing is only
    /// discoverable on the private channel and via [`parse_tx_verdict`], never from here.
    #[test]
    fn a_two_hundred_with_a_hash_is_accepted_and_spends_the_nonce() {
        let body = r#"{"code":200,"message":"{\"ratelimit\": \"Ratelimit is off\"}","tx_hash":"5050882aba3f8ac068939a7b667b0e56f0624b7aa63bf0d915344b6b7b858fa1","predicted_execution_time_ms":1785431776332}"#;
        match classify_send_outcome(200, body) {
            SendOutcome::Accepted(ack) => assert!(!ack.tx_hash.is_empty()),
            other => panic!("a 200 with a hash must be Accepted, got {other:?}"),
        }
    }

    /// **A `21104` is its own outcome, not a generic failure** (§2.7): it is the one HTTP 400
    /// that leaves the nonce untouched, so the slot resyncs rather than advancing. Reproduces
    /// the gap/replay body from the Phase-0 artifacts (step7).
    #[test]
    fn an_invalid_nonce_is_classified_as_such_and_not_as_ambiguous() {
        let body = r#"{"code":21104,"message":"invalid nonce"}"#;
        match classify_send_outcome(400, body) {
            SendOutcome::InvalidNonce { message } => assert!(message.contains("nonce")),
            other => panic!("21104 must be InvalidNonce, got {other:?}"),
        }
    }

    /// **Everything unrecognised is `Ambiguous`, and fails closed** (§3.3): a 5xx, a 408, and
    /// — the subtle one — a 200 whose body has no `tx_hash` all leave the nonce's fate
    /// unknown, so the slot hard-refreshes from `nextNonce` rather than guessing the counter
    /// forward. Mapping any of these to `Accepted` would be the dangerous mistake.
    #[test]
    fn an_unknown_outcome_is_ambiguous_including_a_two_hundred_with_no_hash() {
        for (status, body) in [
            (503u16, "upstream unavailable"),
            (408, ""),
            (200, r#"{"code":200,"message":"ok"}"#),
            (429, r#"{"code":429,"message":"rate limited"}"#),
        ] {
            assert!(
                matches!(
                    classify_send_outcome(status, body),
                    SendOutcome::Ambiguous { .. }
                ),
                "status {status} body {body:?} should be Ambiguous"
            );
        }
    }

    /// The `GET /tx` body triple-nests the verdict: `event_info` is a JSON string, and inside
    /// it `ae` is another JSON string. The Phase-0 online half never captured this body
    /// (no fill/rejection was probed for `/tx`), so these reproduce the shape design note
    /// §4.11 documents byte-for-byte, built through `serde_json` so the double-encoding is
    /// exactly what the venue emits.
    fn tx_body(ae: serde_json::Value) -> String {
        let ae_field = match ae {
            serde_json::Value::String(s) => s,
            other => other.to_string(),
        };
        let event_info =
            serde_json::json!({ "i": 1, "to": { "AccountIndex": 516 }, "ae": ae_field })
                .to_string();
        // `status` is 2 for BOTH an accepted and a rejected tx — the trap — so it is present
        // here and must never be what the parser keys on.
        serde_json::json!({
            "code": 200, "hash": "ab", "type": 14, "status": 2, "event_info": event_info,
        })
        .to_string()
    }

    /// **The verdict, and the whole of §4.11: an empty `ae` accepts, a non-empty `ae` is an
    /// application error — and `tx.status` is never consulted.**
    #[test]
    fn the_order_verdict_is_event_info_ae_not_the_tx_status() {
        // Accepted: ae is the empty string.
        assert_eq!(
            parse_tx_verdict(&tx_body(serde_json::json!(""))).unwrap(),
            TxVerdict::Accepted
        );

        // Rejected: ae is a JSON string of {code, message} — the duplicate-COI case (21728).
        let ae = serde_json::json!({
            "code": 21728, "message": "client order index already exists"
        })
        .to_string();
        match parse_tx_verdict(&tx_body(serde_json::json!(ae))).unwrap() {
            TxVerdict::Rejected { code, message } => {
                assert_eq!(code, 21728);
                assert!(message.contains("client order index"), "{message}");
            }
            other => panic!("a non-empty ae is a rejection, got {other:?}"),
        }
    }

    /// No `event_info` yet — the tx has not been processed — is `Pending`, not a false accept.
    /// A deadline waiting on this keeps waiting; it must never read absence as acceptance.
    #[test]
    fn a_tx_with_no_event_info_is_pending_not_accepted() {
        assert_eq!(
            parse_tx_verdict(r#"{"code":200,"hash":"ab","status":2}"#).unwrap(),
            TxVerdict::Pending
        );
        assert_eq!(
            parse_tx_verdict(&tx_body_with_raw_event_info("")).unwrap(),
            TxVerdict::Pending
        );
    }

    fn tx_body_with_raw_event_info(event_info: &str) -> String {
        serde_json::json!({ "code": 200, "status": 2, "event_info": event_info }).to_string()
    }

    // ---- Phase 2 auth-token verification (§3.4) -------------------------------------

    /// **A 200 from the cheap GET is the only acceptance** (§3.4). Real `accountLimits` body
    /// off testnet with an 8 h token.
    #[test]
    fn a_two_hundred_from_the_auth_check_is_valid() {
        assert_eq!(
            classify_auth_check(200, ACCOUNT_LIMITS_OK),
            AuthCheck::Valid
        );
    }

    /// **The §3.4 trap made loud: a token whose deadline is over the 8 h ceiling mints without
    /// error and is rejected `20013` only at use.** The cheap GET catches it — a 401 is
    /// `Invalid` and names the venue's code, so the connector never switches to a doomed token.
    #[test]
    fn a_four_oh_one_from_the_auth_check_is_invalid_and_names_the_code() {
        match classify_auth_check(401, ACCOUNT_LIMITS_INVALID_DEADLINE) {
            AuthCheck::Invalid { code, message } => {
                assert_eq!(code, 20013);
                assert!(message.contains("deadline"), "{message}");
            }
            other => panic!("a 401 must be Invalid, got {other:?}"),
        }
    }

    /// **Any other status is `Unknown`, never a verdict about the token** (§3.4): a 503 on the
    /// check must not discard a token that is actually fine. Fail *open* on the check so the
    /// connector fails *closed* on the token — it abandons only a token the server refused with
    /// a 401. A 401 with an unreadable body is still a rejection.
    #[test]
    fn an_odd_status_from_the_auth_check_is_unknown_not_a_rejection() {
        for (status, body) in [
            (503u16, "upstream unavailable"),
            (500, ""),
            (429, "slow down"),
        ] {
            assert!(
                matches!(classify_auth_check(status, body), AuthCheck::Unknown { .. }),
                "status {status} must be Unknown, not a token verdict"
            );
        }
        // A 401 whose body is not the venue's JSON is still a rejection — fail closed.
        assert!(matches!(
            classify_auth_check(401, "<html>nope</html>"),
            AuthCheck::Invalid { .. }
        ));
    }
}
