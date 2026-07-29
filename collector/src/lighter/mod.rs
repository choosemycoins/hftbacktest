//! Lighter — a zk-rollup perp venue whose market data is addressed by integer
//! market id rather than by symbol.
//!
//! Three things about this venue shape the backend, and each of them fails
//! silently if got wrong:
//!
//! * **A subscription is asked for as `order_book/0` and every frame comes
//!   back as `order_book:0`.** Routing on the request spelling files the whole
//!   feed under `_meta` while the socket looks perfectly healthy.
//! * **The book is a diff feed with an explicit chain.** Each frame carries
//!   `begin_nonce`..`nonce` off the matching engine, and `begin_nonce(N+1)`
//!   must equal `nonce(N)`. A break means batches were lost — a hole that
//!   leaves no hole in `local_ts`, so nothing else in the recording would ever
//!   show it. `offset` is *not* usable for this: it is API-server-local and
//!   jumps on reconnect.
//! * **Markets are integers, and the mapping is a runtime fact.** `ETH` is 0
//!   and `BTC` is 1 today; nothing in the recording says so unless the catalog
//!   is written into it, and a symbol that does not resolve becomes a
//!   subscription to a market id the venue answers `Invalid Channel` to —
//!   without closing the socket. So resolution is not a check that can be
//!   skipped here, it is the addressing.

mod http;

#[cfg(test)]
mod fixtures;

use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
pub use http::keep_connection;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio_tungstenite::tungstenite::Utf8Bytes;
use tracing::{error, info, warn};

use crate::{
    backoff::retry_startup,
    error::ConnectorError,
    file::META_STREAM,
    meta,
    pump::pump,
    queue::{Record, Tx},
};

/// Mainnet endpoints. Testnet lives at `testnet.zklighter.elliot.ai` and has
/// its own market ids, so it is not a drop-in substitution.
pub const WS_URL: &str = "wss://mainnet.zklighter.elliot.ai/stream";
pub const REST_URL: &str = "https://mainnet.zklighter.elliot.ai";

/// The channels taken for every market.
///
/// Not configurable, for the same reason the other backends' stream sets are
/// not: the converters are written against exactly this set, and the four are
/// not substitutes for one another.
///
/// * `order_book` — the book itself: a full snapshot on subscribe, then diffs
///   batched at ~50 ms (measured 2026-07-28: median 0.050 s on ETH and BTC).
/// * `ticker` — the top of book, event-driven per engine nonce and an order of
///   magnitude faster than the batched book (median 0.0095 s). It is what makes
///   the touch usable between two book batches.
/// * `trade` — the tape. Note the subscribe frame replays the **last 50
///   trades**, so the first frame of a session is history, not new prints.
/// * `market_stats` — mark price, index price, funding rate, open interest.
///   The venue's own funding/oracle aggregate: the one thing about a perp its
///   book and tape cannot reconstruct afterwards, so a recording made without
///   it cannot be repaired.
pub const CHANNELS: [&str; 4] = ["order_book", "ticker", "trade", "market_stats"];

/// The venue's client-message budget, per minute.
///
/// Exceeding it is not answered with an error frame — the connection is
/// throttled, which from this side is a socket that goes quiet. That is the
/// one failure this collector is least able to tell from a quiet market, so
/// the budget is treated as a hard constraint rather than a guideline.
///
/// **Provenance, and the one thing about it that is not established.** The
/// figure is the venue's published limit, alongside 500 subscriptions per
/// connection and 255 connections per IP. What its documentation does not say
/// is whether the 200 is counted per connection or per IP, and this collector
/// has not measured it — the only way to would be to deliberately exceed a
/// limiter on a public endpoint. So the budget below is spent as if it were
/// **per connection** (which is how the other two limits are scoped) while
/// nothing is sized as if a second connection were free. Concretely: one
/// instance opens one connection, [`MAX_MARKETS`] holds its subscribe set to
/// half, and [`REPAIR_COOLDOWN`] holds the steady-state traffic well under the
/// rest. Under the per-IP reading the exposure is two connections inside one
/// minute — the backoff floor is 500 ms, so a flapping venue can do that — and
/// the mitigation there is the backoff ladder, not this constant. Anyone
/// running two instances behind one IP should read the per-IP case as the live
/// one.
pub const CLIENT_MSG_BUDGET_PER_MIN: usize = 200;

/// What the collector allows a connection's subscribe set to cost.
///
/// Half, so the remainder covers the keepalives (2/min), the per-market repairs
/// a nonce break triggers (at worst 50/min, see [`REPAIR_COOLDOWN`]), and a
/// reconnect inside the same minute — the backoff floor is 500 ms, so two
/// connections in one minute is ordinary rather than pathological.
pub const SUBSCRIBE_BUDGET_PER_MIN: usize = CLIENT_MSG_BUDGET_PER_MIN / 2;

/// The most markets one instance may record, derived from the budget above.
///
/// Enforced at resolution, because it is the only place it can be: the venue
/// does not refuse the 101st subscribe, it throttles the connection that sent
/// it, and the recording then looks like a venue that went quiet.
///
/// Raising it means either recording fewer channels or opening a second
/// connection (the venue allows 255 per IP), and neither is a decision to make
/// by accident.
pub const MAX_MARKETS: usize = SUBSCRIBE_BUDGET_PER_MIN / CHANNELS.len();

/// How long a catalog fetch may take before startup is abandoned.
///
/// Bounded because this blocks startup: `reqwest` has no default timeout, so a
/// hung endpoint would leave the collector "starting" indefinitely, recording
/// nothing and reporting nothing.
const CATALOG_TIMEOUT: Duration = Duration::from_secs(15);

/// One WebSocket subscription, before the market id is substituted in.
///
/// A newtype rather than a bare `&str` because the two spellings of a channel
/// are the venue's sharpest trap: the request takes `order_book/0` and every
/// response says `order_book:0`. Keeping the request form behind
/// [`SubscriptionSpec::topic`] and the response form behind [`route`] means
/// neither is written out by hand at a call site.
#[derive(Clone, Debug)]
pub struct SubscriptionSpec {
    pub channel: String,
}

impl SubscriptionSpec {
    pub fn plain(channel: &str) -> Self {
        Self {
            channel: channel.to_string(),
        }
    }

    /// The request spelling: `order_book/0`.
    pub fn topic(&self, market_id: i64) -> String {
        format!("{}/{market_id}", self.channel)
    }
}

/// What the venue's catalog says about one recorded market.
#[derive(Debug, Clone, PartialEq)]
pub struct MarketInfo {
    pub symbol: String,
    pub market_id: i64,
    pub market_type: String,
    pub price_decimals: i64,
    pub size_decimals: i64,
    pub min_base_amount: String,
    pub min_quote_amount: String,
}

impl MarketInfo {
    #[cfg(test)]
    fn test(symbol: &str, market_id: i64) -> Self {
        Self {
            symbol: symbol.to_string(),
            market_id,
            market_type: "perp".to_string(),
            price_decimals: 2,
            size_decimals: 4,
            min_base_amount: "0".to_string(),
            min_quote_amount: "0".to_string(),
        }
    }
}

/// Market id -> the symbol its frames are filed under.
///
/// The routing table, built once from the resolved catalog. A market id that
/// is not in here was never subscribed to, and its frames go to the sidecar
/// rather than opening a file for a market nobody asked for.
pub struct Markets {
    by_id: HashMap<i64, String>,
}

impl Markets {
    pub fn new(markets: &[MarketInfo]) -> Self {
        Self {
            by_id: markets
                .iter()
                .map(|m| (m.market_id, m.symbol.clone()))
                .collect(),
        }
    }

    fn symbol_of(&self, market_id: i64) -> Option<&str> {
        self.by_id.get(&market_id).map(String::as_str)
    }
}

/// Decides which stream a received frame belongs to.
///
/// Every frame lands somewhere. A frame whose channel names a subscribed
/// market goes to that market's file; everything else — the session
/// handshake, subscription errors, pongs, channels this collector does not
/// subscribe to, anything unrecognised — goes to [`META_STREAM`].
///
/// The channel is matched on the **response** spelling, `<channel>:<id>`. Note
/// what is deliberately not done: a channel outside [`CHANNELS`] is not filed
/// under its market even when the id resolves. The symbol files are what the
/// converter reads, and a feed nobody has written a rule for belongs beside
/// the other unexplained things rather than inside them.
fn route<'a>(j: &serde_json::Value, markets: &'a Markets) -> &'a str {
    // Lifecycle records the collector itself injects (see `meta.rs`) are
    // tagged, so they can never be confused with something the venue said.
    if meta::is_record(j) {
        return META_STREAM;
    }

    let Some(channel) = j.get("channel").and_then(|c| c.as_str()) else {
        return META_STREAM;
    };
    // `height` has no market id at all, and `market_stats:all` has one that is
    // not a number. Both are real channels; neither belongs to one market.
    let Some((kind, id)) = channel.split_once(':') else {
        return META_STREAM;
    };
    if !CHANNELS.contains(&kind) {
        return META_STREAM;
    }
    let Ok(market_id) = id.parse::<i64>() else {
        return META_STREAM;
    };
    markets.symbol_of(market_id).unwrap_or(META_STREAM)
}

/// The least time between two repairs of the same market's book.
///
/// A repair is two client frames (`unsubscribe`, then `subscribe` — see
/// [`http`]), and the thing that has to be bounded is not how many are
/// outstanding but how often they are sent. The tempting bound — one repair
/// outstanding, cleared when its snapshot lands — makes the repair rate equal
/// the repair's own round trip, measured at 522 ms on mainnet 2026-07-28. A
/// market the venue keeps losing would then cycle break → repair → snapshot →
/// break at ~2 Hz, or ~4 client messages a second; at [`MAX_MARKETS`] that is
/// ~100 a second against a budget of 200 a **minute**, and each cycle also
/// drags a fresh 105–141 KB snapshot through the writer hop.
///
/// A minute, so the arithmetic closes: 25 markets × 2 frames = 50 messages a
/// minute at the very worst, which leaves room for the keepalives and for a
/// reconnect's full subscribe set inside the same minute
/// (`the_worst_repair_rate_still_fits_the_message_budget`).
///
/// It is a **cooldown on the attempt, not on the outcome**, and that is the
/// second thing it buys. The venue answers a repair it will not serve with an
/// error frame carrying no channel, so nothing in this process can key on it; a
/// latch cleared only by the snapshot would leave a refused market recorded,
/// counted and never repaired again for the life of the connection. Time clears
/// this one whatever became of the last repair.
pub const REPAIR_COOLDOWN: Duration = Duration::from_secs(60);

/// A break in one market's book chain.
struct SequenceGap {
    market_id: i64,
    expected: u64,
    got: u64,
    /// How many breaks this market has had in this process.
    count: u64,
    /// Whether this break asks for a repair, or one was sent too recently.
    resubscribe: bool,
}

/// Per-market book-sequence state.
///
/// The venue publishes `begin_nonce`..`nonce` on every `order_book` frame,
/// snapshots included, and the chain runs through both: a snapshot's `nonce`
/// is what the first diff after it chains from (verified against a captured
/// session, `fixtures.rs`). So a snapshot restarts the chain and a diff
/// continues it.
#[derive(Default)]
pub struct Chains {
    last: HashMap<i64, u64>,
    /// When each market's book was last repaired, which is what
    /// [`REPAIR_COOLDOWN`] is measured from.
    last_repair: HashMap<i64, Instant>,
    breaks: HashMap<i64, u64>,
}

impl Chains {
    /// Forgets every market's nonce, because a new connection restarts them
    /// all.
    ///
    /// `Chains` outlives a connection — it belongs to the parser, and the
    /// socket loop reconnects underneath it — so without this the first frame
    /// after a reconnect is compared against a nonce from before the socket
    /// dropped. It works out today only because the venue answers each
    /// subscribe with a snapshot that reseeds the chain first, but a market
    /// whose subscribe is not served on the new connection (a relist, an error
    /// frame — the venue answers those without closing the socket) would give a
    /// phantom `sequence_gap` and spend a repair on it.
    ///
    /// The break **counts** deliberately survive: they are the process's tally
    /// of how much was lost, and a venue that reconnects often is exactly the
    /// one whose total matters.
    fn reset(&mut self) {
        self.last.clear();
        self.last_repair.clear();
    }

    /// Reads one frame's place in its market's chain.
    ///
    /// Returns `Some` only for a break. Frames that are not `order_book`, and
    /// markets that were never subscribed, are ignored: `ticker` and `trade`
    /// carry a bare `nonce` with no `begin_nonce`, and reading one as a chain
    /// would report a break on every frame.
    ///
    /// `now` is supplied rather than read so the cooldown can be tested without
    /// sleeping through one.
    fn observe(
        &mut self,
        j: &serde_json::Value,
        markets: &Markets,
        now: Instant,
    ) -> Option<SequenceGap> {
        let (kind, market_id) = book_frame(j)?;
        markets.symbol_of(market_id)?;

        let book = j.get("order_book")?;
        let nonce = book.get("nonce")?.as_u64()?;

        if kind == FrameKind::Snapshot {
            // The venue restarting the chain, not a loss. Note what this does
            // *not* do: it does not re-arm the repair. Doing so would make the
            // repair rate the repair's own round trip — see [`REPAIR_COOLDOWN`].
            self.last.insert(market_id, nonce);
            return None;
        }

        let begin = book.get("begin_nonce")?.as_u64()?;
        let previous = self.last.insert(market_id, nonce);
        // The first frame of a connection has nothing to chain onto. Calling
        // that a break would put a record in the sidecar for every reconnect
        // and repair a book that was just subscribed.
        let expected = previous?;
        if begin == expected {
            return None;
        }

        let count = self.breaks.entry(market_id).or_default();
        *count += 1;
        // At most one repair per market per cooldown. A venue that has
        // genuinely lost us breaks the chain on every frame that follows, and
        // at the observed 20 book frames a second one repair per frame would
        // exhaust the venue's minute budget in five seconds — the collector
        // would be throttled off the socket while trying to repair it.
        let resubscribe = match self.last_repair.get(&market_id) {
            Some(at) if now.duration_since(*at) < REPAIR_COOLDOWN => false,
            _ => {
                self.last_repair.insert(market_id, now);
                true
            }
        };
        Some(SequenceGap {
            market_id,
            expected,
            got: begin,
            count: *count,
            resubscribe,
        })
    }
}

#[derive(PartialEq, Eq, Clone, Copy)]
enum FrameKind {
    Snapshot,
    Update,
}

/// `(kind, market_id)` for an `order_book` frame, and `None` for anything else.
///
/// The frame names itself twice: `type` is `subscribed/order_book` or
/// `update/order_book`, and `channel` is `order_book:<id>`. Both are read —
/// the type is what separates a snapshot from a diff, and the channel is where
/// the market id is.
fn book_frame(j: &serde_json::Value) -> Option<(FrameKind, i64)> {
    let (prefix, kind) = j.get("type")?.as_str()?.split_once('/')?;
    if kind != "order_book" {
        return None;
    }
    let kind = match prefix {
        "subscribed" => FrameKind::Snapshot,
        "update" => FrameKind::Update,
        _ => return None,
    };
    let (_, id) = j.get("channel")?.as_str()?.split_once(':')?;
    Some((kind, id.parse().ok()?))
}

fn handle(
    chains: &mut Chains,
    markets: &Markets,
    resub_tx: &UnboundedSender<i64>,
    writer_tx: &Tx<Record>,
    recv_time: DateTime<Utc>,
    now: Instant,
    data: Utf8Bytes,
) -> Result<(), ConnectorError> {
    let j: serde_json::Value = serde_json::from_str(data.as_str())?;
    let stream = route(&j, markets).to_string();
    writer_tx.send((recv_time, stream, data.to_string()))?;

    // A new connection restarts every market's chain. The socket loop is a
    // different task and cannot reach this state; its `connected` record
    // travels this very hop, which is how the parser gets to know.
    if j[meta::TAG] == "connected" {
        chains.reset();
        return Ok(());
    }

    // After the frame, not before: the break is discovered *at* this frame, so
    // the record belongs immediately behind it in the sidecar. It travels the
    // writer hop like the frame it explains, which keeps the two in order.
    if let Some(gap) = chains.observe(&j, markets, now) {
        let symbol = markets.symbol_of(gap.market_id).unwrap_or("?");
        warn!(
            market = gap.market_id,
            %symbol,
            expected_begin_nonce = gap.expected,
            begin_nonce = gap.got,
            count = gap.count,
            // False means a repair went out for this market inside the last
            // `REPAIR_COOLDOWN`, not that the break is being ignored: it is
            // recorded and counted either way.
            repairing = gap.resubscribe,
            "the order book chain broke; batches were lost"
        );
        writer_tx.send((
            recv_time,
            META_STREAM.to_string(),
            meta::sequence_gap(
                "order_book",
                gap.market_id,
                symbol,
                gap.expected,
                gap.got,
                gap.count,
            )
            .to_string(),
        ))?;
        if gap.resubscribe {
            // Unsubscribe, then subscribe — see `http::repair_frames`. A bare
            // duplicate subscribe is refused with `30003 Already Subscribed`
            // and produces no snapshot, and a fresh snapshot is the only way
            // back to a complete book.
            //
            // A failure here means the socket loop has gone, which the pump is
            // already unwinding; there is nothing to add and nothing to repair.
            if let Err(error) = resub_tx.send(gap.market_id) {
                warn!(?error, "couldn't ask for a repair; the socket has gone");
            }
        }
    }
    Ok(())
}

/// Matches requested symbols against an already-fetched catalog.
///
/// Split out from the HTTP so the interesting half is testable without a
/// network. `catalog` is the `/api/v1/orderBooks` response.
fn match_catalog(
    symbols: &[String],
    catalog: &serde_json::Value,
) -> Result<Vec<MarketInfo>, anyhow::Error> {
    if symbols.is_empty() {
        return Err(anyhow::anyhow!("no symbols requested"));
    }

    // One market is one subscription set, however many times it was typed.
    // Asking twice would otherwise cost four subscribes the venue answers
    // `{"error":{"code":30003,"message":"Already Subscribed to : ..."}}` —
    // spent from a budget this module treats as a hard constraint — put the
    // market in the sidecar's `universe` record twice, and count twice against
    // the cap below. Case-insensitively, because the match below is: `ETH` and
    // `eth` are the same market.
    let mut seen = HashSet::new();
    let symbols: Vec<&String> = symbols
        .iter()
        .filter(|s| seen.insert(s.to_uppercase()))
        .collect();

    if symbols.len() > MAX_MARKETS {
        return Err(anyhow::anyhow!(
            "{} symbols requested; one connection may record at most MAX_MARKETS = {} \
             ({} channels each, against the venue's budget of {} client messages a minute). \
             Record the rest with a second instance and a second output directory.",
            symbols.len(),
            MAX_MARKETS,
            CHANNELS.len(),
            CLIENT_MSG_BUDGET_PER_MIN,
        ));
    }

    let rows = catalog
        .get("order_books")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("the catalog has no `order_books` array"))?;

    let mut out = Vec::new();
    let mut refused = Vec::new();
    for symbol in &symbols {
        let wanted = symbol.to_uppercase();
        let Some(row) = rows.iter().find(|r| {
            r.get("symbol")
                .and_then(|s| s.as_str())
                .is_some_and(|s| s.eq_ignore_ascii_case(&wanted))
        }) else {
            // Point at the likely mistake rather than just refusing. The common
            // one is a quote suffix carried over from another venue: Lighter
            // perps are bare base assets, and its spot markets are the ones
            // that carry a quote.
            let near: Vec<&str> = rows
                .iter()
                .filter_map(|r| r.get("symbol").and_then(|s| s.as_str()))
                .filter(|s| s.split('/').next() == Some(wanted.as_str()) || wanted.starts_with(s))
                .collect();
            refused.push(if near.is_empty() {
                format!("{symbol} (not listed on this venue)")
            } else {
                format!("{symbol} (did you mean: {}?)", near.join(", "))
            });
            continue;
        };

        let name = row
            .get("symbol")
            .and_then(|s| s.as_str())
            .unwrap_or(wanted.as_str());
        let status = row.get("status").and_then(|s| s.as_str()).unwrap_or("");
        if status != "active" {
            // A listed-but-dead market is subscribable and silent. Eighteen of
            // the venue's 227 markets were `inactive` on 2026-07-28.
            refused.push(format!("{name} (market is {status}, not active)"));
            continue;
        }
        if !is_filename_safe(name) {
            // `Writer` builds a path out of the symbol, so `ETH/USDC` becomes
            // `<dir>/eth/usdc_<date>.gz` — a directory that does not exist. The
            // first frame would end the recording with ENOENT, minutes after a
            // clean start. The venue's spot markets are all spelled this way.
            refused.push(format!(
                "{name} (cannot be a filename; the collector names files after the symbol)"
            ));
            continue;
        }

        out.push(MarketInfo {
            symbol: name.to_string(),
            market_id: row
                .get("market_id")
                .and_then(|v| v.as_i64())
                .ok_or_else(|| anyhow::anyhow!("{name}: catalog row has no market_id"))?,
            market_type: row
                .get("market_type")
                .and_then(|v| v.as_str())
                .unwrap_or("?")
                .to_string(),
            price_decimals: field_i64(row, "supported_price_decimals"),
            size_decimals: field_i64(row, "supported_size_decimals"),
            min_base_amount: field_str(row, "min_base_amount"),
            min_quote_amount: field_str(row, "min_quote_amount"),
        });
    }

    if !refused.is_empty() {
        return Err(anyhow::anyhow!(
            "Lighter cannot record: {}. Symbols are the venue's own bare names \
             (ETH, BTC — not ETHUSDT), and every one has to resolve to a market id \
             before anything can be subscribed.",
            refused.join("; ")
        ));
    }
    Ok(out)
}

fn field_i64(row: &serde_json::Value, key: &str) -> i64 {
    row.get(key).and_then(|v| v.as_i64()).unwrap_or(-1)
}

fn field_str(row: &serde_json::Value, key: &str) -> String {
    row.get(key)
        .and_then(|v| v.as_str())
        .unwrap_or("?")
        .to_string()
}

/// Whether a symbol can be part of a filename, which on this venue is not a
/// given: `Writer` writes `<dir>/<symbol lowercased>_<date>.gz`.
fn is_filename_safe(symbol: &str) -> bool {
    !symbol.is_empty()
        && symbol
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
        && !symbol.starts_with('.')
}

/// The catalog rows, as they go into the sidecar.
///
/// Recorded rather than merely logged, and under the same `universe` name
/// Hyperliquid's instrument metadata uses, so one sidecar reader covers both.
/// Without it the recording cannot say what market id 0 was on the day it was
/// made — the ids are venue configuration, not constants — and the size and
/// price decimals a converter needs for lot size would have to be re-fetched
/// from a venue that has since relisted things.
fn universe_record(markets: &[MarketInfo]) -> serde_json::Value {
    serde_json::json!({
        "_collector": "universe",
        "markets": markets.iter().map(|m| serde_json::json!({
            "symbol": m.symbol,
            "market_id": m.market_id,
            "market_type": m.market_type,
            "supported_price_decimals": m.price_decimals,
            "supported_size_decimals": m.size_decimals,
            "min_base_amount": m.min_base_amount,
            "min_quote_amount": m.min_quote_amount,
        })).collect::<Vec<_>>(),
    })
}

/// Resolves every requested symbol to a market id, or refuses to start.
///
/// Called from `main` before the recording opens, because the map it returns
/// is what `session_start` stamps into the sidecar: a recording whose payloads
/// are keyed by integer has to carry the key.
///
/// `--no-symbol-check` is refused rather than honoured. On the other venues it
/// skips a lookup that only validates; here the lookup *is* the addressing,
/// and skipping it would leave nothing to subscribe to.
pub async fn resolve_markets(
    symbols: &[String],
    rest_url: &str,
    check_symbols: bool,
) -> Result<Vec<MarketInfo>, anyhow::Error> {
    if !check_symbols {
        return Err(anyhow::anyhow!(
            "--no-symbol-check cannot be used with lighter: the venue subscribes by \
             integer market id, and the catalog at {rest_url}/api/v1/orderBooks is the \
             only thing that maps a symbol to one. There is nothing to record without it."
        ));
    }

    let client = reqwest::Client::builder()
        .timeout(CATALOG_TIMEOUT)
        .build()?;
    // Retried, because a fresh process that meets one refused connection on the
    // way up has recorded nothing and can simply ask again — see
    // `backoff::retry_startup`, and the 2026-07-29 06:09:48 incident it was
    // added for. The retry covers the fetch only: the refusal above is a
    // configuration error and `match_catalog` below is a pure function of bytes
    // already in hand, so neither has a second answer to give.
    let catalog = retry_startup("the Lighter market catalog", || {
        fetch_catalog(&client, rest_url)
    })
    .await
    .map_err(|e| anyhow::anyhow!("couldn't read the Lighter market catalog: {e}"))?;

    match_catalog(symbols, &catalog)
}

/// One catalog round trip.
///
/// Split from [`resolve_markets`] so it can be retried on its own: the helper
/// takes an `FnMut` whose future must not borrow the closure, which a free
/// function taking shared references satisfies and an inline `async` block
/// capturing them does not.
async fn fetch_catalog(
    client: &reqwest::Client,
    rest_url: &str,
) -> Result<serde_json::Value, anyhow::Error> {
    Ok(client
        .get(format!("{rest_url}/api/v1/orderBooks"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

pub async fn run_collection(
    markets: Vec<MarketInfo>,
    writer_tx: Tx<Record>,
) -> Result<(), anyhow::Error> {
    for market in &markets {
        info!(
            symbol = %market.symbol,
            market_id = market.market_id,
            market_type = %market.market_type,
            size_decimals = market.size_decimals,
            price_decimals = market.price_decimals,
            "resolved market"
        );
    }
    writer_tx.send((
        Utc::now(),
        META_STREAM.to_string(),
        universe_record(&markets).to_string(),
    ))?;

    // The reachability probe. `/stream` sits behind a jurisdiction check, and
    // from a restricted region the upgrade fails in a way that is
    // indistinguishable from a network error while REST keeps working — so the
    // catalog fetch above proves nothing about the socket. Without this the
    // collector would reconnect for ever, recording nothing, and exit 0.
    if let Err(error) = http::probe(WS_URL).await {
        // `probe_failed`, not `symbol_check_failed`: every symbol resolved a
        // moment ago (`main` does it before `session_start`), and what refused
        // is the WebSocket upgrade. Borrowing the nearer name would have the
        // offline report annotate the hole "explained by symbol_check_failed"
        // and send whoever reads it to check a symbol list that was never
        // wrong.
        if let Err(queue_error) = writer_tx.send((
            Utc::now(),
            META_STREAM.to_string(),
            meta::probe_failed(WS_URL, &error.to_string()).to_string(),
        )) {
            error!(
                ?queue_error,
                "couldn't record why the collector refused to start"
            );
        }
        error!(%error, "refusing to start");
        return Err(error);
    }

    let subscriptions: Vec<SubscriptionSpec> = CHANNELS
        .iter()
        .map(|c| SubscriptionSpec::plain(c))
        .collect();
    let index = Markets::new(&markets);
    let mut chains = Chains::default();
    // The way back from the parser to the socket: a broken book chain is
    // discovered while filing a frame, and repairing it means sending an
    // unsubscribe and a subscribe. Unbounded because it carries at most one
    // message per market per `REPAIR_COOLDOWN`, and because the alternative — a
    // bounded hop — would make a repair request able to end the recording.
    let (resub_tx, resub_rx) = unbounded_channel::<i64>();

    pump(
        writer_tx,
        |ws_tx| keep_connection(subscriptions, markets, resub_rx, ws_tx),
        move |writer_tx, recv_time, data| {
            handle(
                &mut chains,
                &index,
                &resub_tx,
                writer_tx,
                recv_time,
                Instant::now(),
                data,
            )
        },
    )
    .await
}

#[cfg(test)]
mod route_tests {
    use super::{fixtures::*, *};
    use crate::{
        file::META_STREAM,
        meta,
        queue::{self, Record, WRITER_HOP},
    };

    fn markets() -> Markets {
        Markets::new(&[MarketInfo::test("ETH", 0), MarketInfo::test("BTC", 1)])
    }

    fn r(s: &str) -> String {
        route(&serde_json::from_str(s).unwrap(), &markets()).to_string()
    }

    /// The separator flips between the request and the response: a
    /// subscription is asked for as `order_book/0` and every frame that comes
    /// back names itself `order_book:0`. Routing on the request spelling files
    /// the venue's entire output under `_meta`, which looks exactly like a
    /// venue that acked and never sent.
    #[test]
    fn the_response_separator_is_a_colon_not_a_slash() {
        assert_eq!(r(ORDER_BOOK_AFTER_SNAPSHOT_ETH), "ETH");
        assert_eq!(
            route(
                &serde_json::json!({"channel": "order_book/0", "type": "update/order_book"}),
                &markets()
            ),
            META_STREAM,
            "the request spelling is not what frames come back with"
        );
    }

    /// All four subscribed channels of one market land in that market's file.
    #[test]
    fn every_channel_of_a_market_is_filed_under_its_symbol() {
        assert_eq!(r(ORDER_BOOK_SNAPSHOT_ETH), "ETH");
        assert_eq!(r(ORDER_BOOK_AFTER_SNAPSHOT_ETH), "ETH");
        assert_eq!(r(TICKER_BTC), "BTC");
        assert_eq!(r(TRADE_ETH), "ETH");
        assert_eq!(r(MARKET_STATS_ETH), "ETH");
    }

    /// A market nobody subscribed to has no file of its own, and inventing one
    /// would open a writer per market the venue ever mentions.
    #[test]
    fn a_market_that_was_not_subscribed_goes_to_meta() {
        assert_eq!(
            r(r#"{"channel":"ticker:2","type":"update/ticker","ticker":{"s":"SOL"}}"#),
            META_STREAM
        );
    }

    /// `market_stats:all` and `height` are real channels this collector does
    /// not subscribe to; neither names a subscribed market, so neither may be
    /// filed under one — and neither may be dropped.
    #[test]
    fn channels_without_a_market_go_to_meta() {
        assert_eq!(r(HEIGHT), META_STREAM);
        assert_eq!(r(MARKET_STATS_ALL), META_STREAM);
        assert_eq!(
            r(r#"{"channel":"some_new_channel:0","type":"update/some_new_channel"}"#),
            META_STREAM,
            "an unrecognised channel must not be filed among the four the converter reads"
        );
    }

    /// The session handshake, the venue's own errors and the app-level pong
    /// carry no channel at all. Captured verbatim from mainnet 2026-07-28:
    /// the error frames are the venue's answer to `ticker/99999` and to a
    /// channel name it does not know.
    #[test]
    fn connection_level_frames_go_to_meta() {
        assert_eq!(r(CONNECTED), META_STREAM);
        assert_eq!(r(PONG), META_STREAM);
        assert_eq!(r(ERROR_UNKNOWN_MARKET), META_STREAM);
        assert_eq!(r(ERROR_UNKNOWN_CHANNEL), META_STREAM);
        assert_eq!(r(r#"{"unexpected":"shape"}"#), META_STREAM);
    }

    /// Where the venue's two answers to a book repair land, both captured
    /// verbatim.
    ///
    /// The `unsubscribed` ack names a channel this collector subscribes to, so
    /// it is filed with that market's data rather than in the sidecar — which
    /// is where it belongs: it marks the exact point the diff chain was
    /// deliberately broken, immediately before the fresh snapshot that restarts
    /// it. The `30003` refusal names no channel at all, so `_meta` is the only
    /// place for it.
    #[test]
    fn the_answers_to_a_book_repair_are_filed_where_they_can_be_read() {
        assert_eq!(r(UNSUBSCRIBED_ETH), "ETH");
        assert_eq!(r(ERROR_ALREADY_SUBSCRIBED), META_STREAM);
    }

    /// A `_collector` tag wins over anything else in the frame, so a synthetic
    /// record can never be mistaken for venue data even if it names a channel.
    #[test]
    fn collector_tag_takes_precedence() {
        assert_eq!(
            r(r#"{"_collector":"sequence_gap","channel":"order_book:0","market":0}"#),
            META_STREAM
        );
    }

    /// The Lighter end of the guarantee every backend asserts: each shared
    /// lifecycle record reaches the sidecar, and none can be filed under a
    /// market.
    #[test]
    fn every_lifecycle_record_reaches_the_sidecar() {
        for value in [
            meta::subscribe(WS_URL, 0, serde_json::json!([])),
            meta::connected(WS_URL),
            meta::disconnected("Connection reset without closing handshake", 1194),
            meta::dial_failed("connection refused", 12),
            meta::stream_ended(1194),
            meta::sequence_gap("order_book", 0, "ETH", 17926043712, 17926044752, 1),
        ] {
            assert_eq!(route(&value, &markets()), META_STREAM, "{value}");
        }
    }

    /// Every frame is written, and written byte for byte. A deletion on this
    /// venue is a level whose size is the zero of that market's size decimals
    /// — `"0.0000"` on ETH, `"0.00000"` on BTC — and the collector must not
    /// interpret it: no book is maintained here, so the zero has to survive
    /// into the file for the converter to act on.
    #[test]
    fn frames_are_recorded_verbatim_including_zero_size_deletions() {
        let (tx, mut rx, _fatal) = queue::test_bounded::<Record>(WRITER_HOP, 8);
        let mut state = Chains::default();
        let (resub_tx, _resub_rx) = tokio::sync::mpsc::unbounded_channel();
        let now = chrono::Utc::now();

        assert!(
            ORDER_BOOK_DELETE_ETH.contains(r#""size":"0.0000""#),
            "the captured frame no longer carries a deletion; the fixture is stale"
        );

        for frame in [ORDER_BOOK_DELETE_ETH, TICKER_BTC, ERROR_UNKNOWN_CHANNEL] {
            handle(
                &mut state,
                &markets(),
                &resub_tx,
                &tx,
                now,
                Instant::now(),
                frame.into(),
            )
            .unwrap();
        }
        drop(tx);

        let mut got = Vec::new();
        while let Ok((_, stream, payload)) = rx.try_recv() {
            got.push((stream, payload));
        }
        assert_eq!(got.len(), 3, "every frame must be written, none dropped");
        assert_eq!(got[0].0, "ETH");
        assert_eq!(got[1].0, "BTC");
        assert_eq!(got[2].0, META_STREAM);
        assert_eq!(
            got[0].1, ORDER_BOOK_DELETE_ETH,
            "the payload must reach the writer exactly as the venue sent it"
        );
    }

    /// The other half of "every frame lands somewhere": when the writer cannot
    /// take one, `handle` has to say so rather than report it as written.
    #[test]
    fn a_frame_the_writer_cannot_take_is_an_error_not_a_drop() {
        let (tx, _rx, _fatal) = queue::test_bounded::<Record>(WRITER_HOP, 1);
        let mut state = Chains::default();
        let (resub_tx, _resub_rx) = tokio::sync::mpsc::unbounded_channel();
        let now = chrono::Utc::now();

        handle(
            &mut state,
            &markets(),
            &resub_tx,
            &tx,
            now,
            Instant::now(),
            TICKER_BTC.into(),
        )
        .expect("the first frame fits");
        let error = handle(
            &mut state,
            &markets(),
            &resub_tx,
            &tx,
            now,
            Instant::now(),
            TICKER_BTC.into(),
        )
        .expect_err("a frame that could not be handed over must not be reported as written");
        assert!(
            matches!(error, crate::error::ConnectorError::Queue(_)),
            "{error}"
        );
    }
}

#[cfg(test)]
mod nonce_tests {
    use super::{fixtures::*, *};
    use crate::{
        file::META_STREAM,
        queue::{self, Record, WRITER_HOP},
    };

    fn markets() -> Markets {
        Markets::new(&[MarketInfo::test("ETH", 0), MarketInfo::test("BTC", 1)])
    }

    struct Harness {
        state: Chains,
        markets: Markets,
        resub_tx: tokio::sync::mpsc::UnboundedSender<i64>,
        resub_rx: tokio::sync::mpsc::UnboundedReceiver<i64>,
        tx: crate::queue::Tx<Record>,
        rx: tokio::sync::mpsc::Receiver<Record>,
        _fatal: crate::queue::FatalRx,
        /// The clock the repair cooldown is measured against, supplied rather
        /// than read, so a test can cross a 60 s boundary without sleeping
        /// through one.
        now: std::time::Instant,
    }

    impl Harness {
        fn new() -> Self {
            let (tx, rx, _fatal) = queue::test_bounded::<Record>(WRITER_HOP, 64);
            let (resub_tx, resub_rx) = tokio::sync::mpsc::unbounded_channel();
            Self {
                state: Chains::default(),
                markets: markets(),
                resub_tx,
                resub_rx,
                tx,
                rx,
                _fatal,
                now: std::time::Instant::now(),
            }
        }

        fn feed(&mut self, frame: &str) {
            let now = self.now;
            self.feed_at(frame, now);
        }

        fn feed_at(&mut self, frame: &str, now: std::time::Instant) {
            handle(
                &mut self.state,
                &self.markets,
                &self.resub_tx,
                &self.tx,
                chrono::Utc::now(),
                now,
                frame.into(),
            )
            .unwrap();
        }

        /// Feeds a record the collector wrote itself, on the hop the venue's
        /// frames travel — which is how `meta::connected` reaches the parser.
        fn feed_record(&mut self, value: serde_json::Value) {
            let now = self.now;
            self.feed_at(&value.to_string(), now);
        }

        /// Only the sequence-gap records, so a test that feeds a lifecycle
        /// record does not count it as a finding.
        fn gaps(&mut self) -> Vec<serde_json::Value> {
            self.meta()
                .into_iter()
                .filter(|r| r[meta::TAG] == "sequence_gap")
                .collect()
        }

        /// The collector's **own** records written so far, parsed.
        ///
        /// Filtered to `_collector` records rather than to everything filed
        /// under `_meta`: the sidecar also carries the venue frames that belong
        /// to no market, and counting one of those as a finding would make this
        /// harness agree with a backend that reported nothing.
        fn meta(&mut self) -> Vec<serde_json::Value> {
            let mut out = Vec::new();
            while let Ok((_, stream, payload)) = self.rx.try_recv() {
                let value: serde_json::Value = serde_json::from_str(&payload).unwrap();
                if stream == META_STREAM && meta::is_record(&value) {
                    out.push(value);
                }
            }
            out
        }

        fn resubscribes(&mut self) -> Vec<i64> {
            let mut out = Vec::new();
            while let Ok(market) = self.resub_rx.try_recv() {
                out.push(market);
            }
            out
        }
    }

    /// The chain the venue actually publishes, captured on mainnet
    /// 2026-07-28: the snapshot carries a nonce of its own and the first
    /// update continues from it, so the check spans both frame types rather
    /// than starting at the first update.
    #[test]
    fn the_snapshot_seeds_the_chain_the_updates_continue() {
        let snapshot: serde_json::Value = serde_json::from_str(ORDER_BOOK_SNAPSHOT_ETH).unwrap();
        let first: serde_json::Value = serde_json::from_str(ORDER_BOOK_AFTER_SNAPSHOT_ETH).unwrap();
        assert_eq!(
            first["order_book"]["begin_nonce"], snapshot["order_book"]["nonce"],
            "the captured frames no longer chain; the fixture is stale"
        );

        let mut h = Harness::new();
        h.feed(ORDER_BOOK_SNAPSHOT_ETH);
        h.feed(ORDER_BOOK_AFTER_SNAPSHOT_ETH);

        assert!(h.meta().is_empty(), "a contiguous chain is not a gap");
        assert!(h.resubscribes().is_empty());
    }

    /// Four consecutive frames from the capture, in the order the venue sent
    /// them. Nothing is reported: this is the ordinary case, and a check that
    /// fired here would resubscribe the book twenty times a second.
    #[test]
    fn a_contiguous_run_of_updates_reports_nothing() {
        let mut h = Harness::new();
        h.feed(ORDER_BOOK_SNAPSHOT_ETH);
        h.feed(ORDER_BOOK_AFTER_SNAPSHOT_ETH);
        h.feed(CHAIN_A);
        h.feed(CHAIN_B);

        assert!(h.meta().is_empty());
        assert!(h.resubscribes().is_empty());
    }

    /// `begin_nonce(N+1) != nonce(N)` means the engine advanced without us.
    /// The frames either side are real, and the frames between them are what
    /// a lost batch removes.
    ///
    /// Three things have to happen, and each is separately load-bearing: the
    /// sidecar has to carry the break (a hole in the nonce chain leaves no
    /// hole in `local_ts`, so nothing else in the recording would ever show
    /// it), the market's book has to be resubscribed (the venue answers with a
    /// fresh snapshot, which is the only way back to a complete book), and the
    /// frame itself has to be recorded like any other.
    #[test]
    fn a_broken_chain_is_recorded_and_resubscribed() {
        let mut h = Harness::new();
        h.feed(ORDER_BOOK_SNAPSHOT_ETH);
        h.feed(ORDER_BOOK_AFTER_SNAPSHOT_ETH);
        h.feed(ORDER_BOOK_DELETE_ETH); // from later in the session: the batches between are gone

        let records = h.meta();
        assert_eq!(records.len(), 1, "{records:?}");
        let gap = &records[0];
        assert_eq!(gap["_collector"], "sequence_gap");
        assert_eq!(gap["channel"], "order_book");
        assert_eq!(gap["market"], 0);
        assert_eq!(gap["symbol"], "ETH");
        assert_eq!(gap["expected_begin_nonce"], 17926042944u64);
        assert_eq!(gap["begin_nonce"], 17926044752u64);
        assert_eq!(gap["count"], 1, "gaps are counted in process");

        assert_eq!(
            h.resubscribes(),
            vec![0],
            "the market's order_book must be resubscribed so a fresh snapshot arrives"
        );
    }

    /// A venue that has genuinely lost us breaks the chain on every frame that
    /// follows. One repair per frame would be a subscribe storm — at the
    /// observed 20 order_book frames a second per market it blows the venue's
    /// 200 client messages a minute in five seconds, and the collector would be
    /// rate-limited off the socket while trying to repair it.
    ///
    /// So a market is repaired at most once per [`REPAIR_COOLDOWN`]. The breaks
    /// are still counted and still recorded: they are the evidence of how much
    /// was lost.
    #[test]
    fn a_second_break_inside_the_cooldown_does_not_repair_again() {
        let mut h = Harness::new();
        h.feed(ORDER_BOOK_SNAPSHOT_ETH);
        h.feed(ORDER_BOOK_AFTER_SNAPSHOT_ETH);
        h.feed(ORDER_BOOK_DELETE_ETH);
        assert_eq!(h.resubscribes(), vec![0]);
        assert_eq!(h.meta().len(), 1, "the first break was recorded");

        h.feed(CHAIN_A); // breaks again, still waiting for the snapshot
        let records = h.meta();
        assert_eq!(records.len(), 1, "the second break is still recorded");
        assert_eq!(records[0]["count"], 2, "and counted");
        assert!(
            h.resubscribes().is_empty(),
            "a second resubscribe while the first is outstanding is a subscribe storm"
        );
    }

    /// The snapshot the repair asked for restarts the chain: its own
    /// `begin_nonce` is 0, so continuing to compare against the pre-break
    /// nonce would report a second, phantom gap on every recovery.
    #[test]
    fn a_fresh_snapshot_restarts_the_chain() {
        let mut h = Harness::new();
        h.feed(ORDER_BOOK_AFTER_SNAPSHOT_ETH);
        h.feed(ORDER_BOOK_DELETE_ETH);
        assert_eq!(h.resubscribes(), vec![0]);
        let _ = h.meta();

        h.feed(ORDER_BOOK_SNAPSHOT_ETH);
        assert!(
            h.meta().is_empty(),
            "a snapshot restarts the chain; it is not itself a gap"
        );
        h.feed(ORDER_BOOK_AFTER_SNAPSHOT_ETH);
        assert!(h.meta().is_empty(), "and the chain continues from it");
    }

    /// **A snapshot must not re-arm the repair.**
    ///
    /// It is tempting — the repair is finished, so the market is ready for the
    /// next one — and it makes the rate of repairs the repair's own round trip.
    /// Measured on mainnet 2026-07-28 that round trip is 522 ms, so a market
    /// the venue keeps losing would cycle break → repair → snapshot → break at
    /// ~2 Hz, or ~4 client messages a second. At [`MAX_MARKETS`] that is ~100 a
    /// second against a budget of 200 a **minute**, and each cycle also drags a
    /// fresh 105–141 KB snapshot through the writer hop.
    ///
    /// So the cooldown is on the attempt, not on the outcome: it bounds the
    /// rate whether or not the repair worked.
    #[test]
    fn a_snapshot_does_not_re_arm_the_repair_inside_the_cooldown() {
        let mut h = Harness::new();
        let t0 = h.now;
        h.feed_at(ORDER_BOOK_AFTER_SNAPSHOT_ETH, t0);
        h.feed_at(ORDER_BOOK_DELETE_ETH, t0);
        assert_eq!(h.resubscribes(), vec![0]);
        assert_eq!(h.gaps().len(), 1, "the first break was recorded");

        // The repair worked and the market broke again straight afterwards.
        h.feed_at(ORDER_BOOK_SNAPSHOT_ETH, t0 + Duration::from_millis(522));
        h.feed_at(ORDER_BOOK_DELETE_ETH, t0 + Duration::from_millis(600));

        assert_eq!(h.gaps().len(), 1, "the second break is still recorded");
        assert!(
            h.resubscribes().is_empty(),
            "repairing at the round-trip rate is ~100 client messages a second \
             against a budget of 200 a minute"
        );
    }

    /// **A refused repair must not disarm the market for ever.**
    ///
    /// The venue answers a repair it does not like — a duplicate subscribe, a
    /// market being relisted — with an error frame that carries no channel, so
    /// nothing in this process can key on it. A latch cleared only by the
    /// snapshot that never comes would leave the market recorded, counted and
    /// never repaired again for the life of the connection. Time is therefore
    /// what clears it: the next break after the cooldown gets its own repair,
    /// whatever became of the last one.
    #[test]
    fn a_repair_the_venue_never_answered_is_retried_after_the_cooldown() {
        let mut h = Harness::new();
        let t0 = h.now;
        h.feed_at(ORDER_BOOK_AFTER_SNAPSHOT_ETH, t0);
        h.feed_at(ORDER_BOOK_DELETE_ETH, t0);
        assert_eq!(h.resubscribes(), vec![0]);

        // No snapshot arrives — the repair was refused, or lost.
        h.feed_at(CHAIN_A, t0 + Duration::from_secs(1));
        assert!(h.resubscribes().is_empty(), "still inside the cooldown");

        h.feed_at(
            ORDER_BOOK_DELETE_ETH,
            t0 + REPAIR_COOLDOWN + Duration::from_secs(1),
        );
        assert_eq!(
            h.resubscribes(),
            vec![0],
            "a repair the venue never answered has to be tried again"
        );
        assert_eq!(h.gaps().len(), 3, "and every break was recorded throughout");
    }

    /// A reconnect starts every market's chain again.
    ///
    /// `Chains` outlives a connection — it belongs to the parser, and the
    /// socket loop reconnects underneath it — so without this the first frame
    /// after a reconnect is compared against a nonce from before the socket
    /// dropped. It works out today only because the venue answers each
    /// subscribe with a snapshot that reseeds the chain first; a market whose
    /// subscribe is not served on the new connection would produce a phantom
    /// gap and spend a repair on it. The `connected` record travels the same
    /// hop the frames do, which is how the parser gets to know.
    #[test]
    fn a_reconnect_starts_the_chain_again() {
        let mut h = Harness::new();
        h.feed(ORDER_BOOK_AFTER_SNAPSHOT_ETH);
        h.feed_record(meta::connected(WS_URL));
        h.feed(ORDER_BOOK_DELETE_ETH);

        assert!(
            h.gaps().is_empty(),
            "the first frame of a connection chains onto nothing"
        );
        assert!(
            h.resubscribes().is_empty(),
            "and no repair is spent on a phantom break"
        );
    }

    /// The venue's own answers to the repair, both captured. Neither may be
    /// read as a book frame: the ack carries no `order_book` at all, and the
    /// refusal carries nothing but an error.
    #[test]
    fn the_repair_acks_are_not_chain_frames() {
        let mut h = Harness::new();
        h.feed(ORDER_BOOK_AFTER_SNAPSHOT_ETH);
        h.feed(UNSUBSCRIBED_ETH);
        h.feed(ERROR_ALREADY_SUBSCRIBED);
        assert!(h.gaps().is_empty());

        // And the chain picks up from the snapshot that follows the ack.
        h.feed(ORDER_BOOK_SNAPSHOT_ETH);
        h.feed(ORDER_BOOK_AFTER_SNAPSHOT_ETH);
        assert!(h.gaps().is_empty());
    }

    /// The very first frame of a connection has nothing to chain onto. Calling
    /// that a gap would put a `sequence_gap` in the sidecar for every
    /// reconnect, and resubscribe a book that was just subscribed.
    #[test]
    fn the_first_frame_seen_is_not_a_gap() {
        let mut h = Harness::new();
        h.feed(CHAIN_B);
        assert!(h.meta().is_empty());
        assert!(h.resubscribes().is_empty());
    }

    /// Only the book carries a chain. `ticker` and `trade` carry a bare
    /// `nonce` with no `begin_nonce`, so reading them as a chain would report
    /// a gap on every frame.
    #[test]
    fn only_the_book_is_chain_checked() {
        let mut h = Harness::new();
        h.feed(TICKER_BTC);
        h.feed(TRADE_ETH);
        h.feed(MARKET_STATS_ETH);
        assert!(h.meta().is_empty());
        assert!(h.resubscribes().is_empty());
    }
}

#[cfg(test)]
mod resolve_tests {
    use std::time::Duration;

    use super::*;

    /// The retry ladder covers a network that hiccupped, and nothing else.
    ///
    /// `--no-symbol-check` on this venue is a configuration error, not a
    /// transient one: there is no addressing without the catalog, so the answer
    /// will be the same on the third attempt as on the first. Retrying it would
    /// spend the whole ladder to arrive at the identical refusal, and print two
    /// warnings saying the venue had failed when nothing was ever asked of it.
    ///
    /// Virtual time, so a ladder that did run would be visible as an advance
    /// rather than as a slow test. The URL is unroutable for the same reason:
    /// if this ever does reach the network, it fails here instead of passing
    /// quietly on a machine that happens to be online.
    #[tokio::test(start_paused = true)]
    async fn the_refusal_to_run_without_the_catalog_is_not_retried() {
        let started = tokio::time::Instant::now();

        let error = resolve_markets(&["BTC".to_string()], "http://192.0.2.1:1", false)
            .await
            .expect_err("lighter cannot subscribe without the catalog");

        assert!(error.to_string().contains("--no-symbol-check"), "{error}");
        assert_eq!(
            started.elapsed(),
            Duration::ZERO,
            "a configuration error must be reported at once, not after the retry ladder"
        );
    }
}

#[cfg(test)]
mod catalog_tests {
    use super::*;

    /// Rows from `/api/v1/orderBooks`, captured on mainnet 2026-07-28.
    fn catalog() -> serde_json::Value {
        serde_json::json!({"code": 200, "order_books": [
            {"symbol":"ETH","market_id":0,"market_type":"perp","status":"active",
             "supported_size_decimals":4,"supported_price_decimals":2,
             "min_base_amount":"0.0005","min_quote_amount":"10.000000"},
            {"symbol":"BTC","market_id":1,"market_type":"perp","status":"active",
             "supported_size_decimals":5,"supported_price_decimals":1,
             "min_base_amount":"0.00002","min_quote_amount":"10.000000"},
            {"symbol":"MKR","market_id":28,"market_type":"perp","status":"inactive",
             "supported_size_decimals":4,"supported_price_decimals":2,
             "min_base_amount":"0.01","min_quote_amount":"10.000000"},
            {"symbol":"ETH/USDC","market_id":2048,"market_type":"spot","status":"active",
             "supported_size_decimals":4,"supported_price_decimals":2,
             "min_base_amount":"0.0005","min_quote_amount":"10.000000"}
        ]})
    }

    fn resolve(symbols: &[&str]) -> Result<Vec<MarketInfo>, anyhow::Error> {
        let v: Vec<String> = symbols.iter().map(|s| s.to_string()).collect();
        match_catalog(&v, &catalog())
    }

    /// The whole point of the catalog: the venue addresses markets by integer
    /// id, and the id is not derivable from the symbol.
    #[test]
    fn a_symbol_resolves_to_its_market_id_and_its_tick_metadata() {
        let got = resolve(&["ETH", "BTC"]).unwrap();
        assert_eq!(got[0].symbol, "ETH");
        assert_eq!(got[0].market_id, 0);
        assert_eq!(got[0].size_decimals, 4);
        assert_eq!(got[0].price_decimals, 2);
        assert_eq!(got[1].market_id, 1);
    }

    /// Case is the operator's business, not the venue's: the venue lists
    /// uppercase and the resolved name is what the file is named after, so a
    /// lowercase request must not produce a second file.
    #[test]
    fn a_requested_symbol_is_matched_case_insensitively_and_recorded_as_the_venue_spells_it() {
        let got = resolve(&["eth"]).unwrap();
        assert_eq!(got[0].symbol, "ETH");
    }

    /// Fail closed. An unknown symbol on this venue is not an error frame — it
    /// is a subscription to a market id that does not exist, which the venue
    /// answers with `Invalid Channel` and then carries on, leaving a
    /// connection that looks healthy and a file that is never written.
    #[test]
    fn an_unknown_symbol_is_refused_with_a_hint() {
        let error = resolve(&["ETHUSDT"]).unwrap_err().to_string();
        assert!(error.contains("ETHUSDT"), "{error}");
        assert!(
            error.contains("did you mean") || error.contains("not listed"),
            "{error}"
        );
    }

    #[test]
    fn every_unknown_symbol_is_reported_not_just_the_first() {
        let error = resolve(&["ETH", "NOPE", "ALSONOPE"])
            .unwrap_err()
            .to_string();
        assert!(error.contains("NOPE"), "{error}");
        assert!(error.contains("ALSONOPE"), "{error}");
    }

    /// An inactive market is listed, resolvable and dead. Subscribing to one
    /// is accepted and produces no data, which is the failure this whole
    /// resolution step exists to turn into a refusal to start.
    #[test]
    fn an_inactive_market_is_refused() {
        let error = resolve(&["MKR"]).unwrap_err().to_string();
        assert!(error.contains("MKR"), "{error}");
        assert!(error.contains("inactive"), "{error}");
    }

    /// The spot markets are named `ETH/USDC`, and `Writer` builds a path out
    /// of the symbol — `<dir>/eth/usdc_20260728.gz`, a directory that does not
    /// exist. The first frame would end the recording with ENOENT, minutes
    /// after a clean start. Refusing at resolution turns that into a message.
    #[test]
    fn a_symbol_that_cannot_be_a_filename_is_refused() {
        let error = resolve(&["ETH/USDC"]).unwrap_err().to_string();
        assert!(error.contains("ETH/USDC"), "{error}");
        assert!(
            error.contains("filename") || error.contains("file name"),
            "{error}"
        );
    }

    /// The subscribe burst for one connection has to fit inside the venue's
    /// client-message budget, and the number of markets is what decides it.
    /// Refusing at startup is the only place this can be enforced: the venue
    /// answers an over-budget instance by rate-limiting it off the socket,
    /// which reads downstream as a reconnect loop.
    #[test]
    fn more_markets_than_the_message_budget_allows_is_refused() {
        let many: Vec<String> = (0..MAX_MARKETS + 1).map(|i| format!("SYM{i}")).collect();
        let error = match_catalog(&many, &catalog()).unwrap_err().to_string();
        assert!(
            error.contains("MAX_MARKETS") || error.contains(&MAX_MARKETS.to_string()),
            "{error}"
        );
    }

    /// A symbol asked for twice is one market.
    ///
    /// Every duplicate would otherwise cost four subscribes the venue answers
    /// `{"error":{"code":30003,"message":"Already Subscribed to : ..."}}` —
    /// spent from a budget this module treats as a hard constraint — list the
    /// market twice in the sidecar's `universe` record, and count twice against
    /// [`MAX_MARKETS`], so 26 requests for 20 distinct markets would be refused.
    #[test]
    fn a_symbol_asked_for_twice_is_one_market() {
        let got = resolve(&["ETH", "eth", "BTC"]).unwrap();
        assert_eq!(
            got.len(),
            2,
            "a duplicate costs four rejected subscribes and a doubled universe row: {got:?}"
        );
        assert_eq!(got[0].market_id, 0);
        assert_eq!(got[1].market_id, 1);
    }

    /// And the cap counts markets, not keystrokes.
    #[test]
    fn duplicates_do_not_count_against_the_market_cap() {
        let mut many: Vec<String> = vec!["ETH".to_string(); MAX_MARKETS + 1];
        many.push("BTC".to_string());
        let got = match_catalog(&many, &catalog()).expect("two distinct markets are under the cap");
        assert_eq!(got.len(), 2);
    }

    /// The recording has to be able to say what `market_id` 0 was on the day
    /// it was made: ids are per venue configuration, not a constant, and the
    /// files are named after symbols.
    #[test]
    fn the_catalog_rows_are_recorded_for_the_recording_to_be_self_describing() {
        let resolved = resolve(&["ETH"]).unwrap();
        let record = universe_record(&resolved);
        assert_eq!(record["_collector"], "universe");
        assert_eq!(record["markets"][0]["symbol"], "ETH");
        assert_eq!(record["markets"][0]["market_id"], 0);
        assert_eq!(record["markets"][0]["supported_size_decimals"], 4);
    }
}
