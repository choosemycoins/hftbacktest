mod http;

/// Mainnet endpoints. The WebSocket base is a single URL whose *path* names the
/// channel — there are no subscribe frames — so every stream is a separate
/// socket opened at a different path (see [`channel_urls`]). Testnet lives under
/// `starknet.sepolia.extended.exchange` with a different instrument set, so it
/// is not a drop-in substitution.
pub const WS_BASE: &str = "wss://api.starknet.extended.exchange/stream.extended.exchange/v1";
pub const REST_URL: &str = "https://api.starknet.extended.exchange";

/// Extended requires a non-empty `User-Agent` on the public feed; the value is
/// otherwise unconstrained. Set on both the REST catalog fetch and every socket.
pub const USER_AGENT: &str = "hftbacktest-collector";

use std::{collections::HashSet, time::Duration};

use chrono::{DateTime, Utc};
pub use http::keep_connections;
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

/// What the venue's `/api/v1/info/markets` catalog says about one requested
/// market, kept to exactly what this backend needs.
///
/// `perpetual` gates the funding socket (spot markets emit no funding — the
/// venue documents the funding stream as perpetual-only). `rfq` gates the
/// executable-quote book socket, which is the whole reason this backend
/// distinguishes markets at all: on an RFQ market the plain `/orderbooks` feed
/// returns an *indicative* book, and the *executable* quotes live only on
/// `/orderbooks/rfq/{market}`. `tick`/`min_size` are captured because a
/// converter cannot reconstruct them after the fact — the same reason the
/// Hyperliquid and Paradex backends record their instrument metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct MarketInfo {
    pub name: String,
    pub perpetual: bool,
    pub rfq: bool,
    pub active: bool,
    pub status: String,
    pub tick: Option<String>,
    pub min_size: Option<String>,
}

impl MarketInfo {
    fn from_entry(name: &str, entry: &serde_json::Value) -> Self {
        let cfg = entry.get("tradingConfig");
        Self {
            name: name.to_string(),
            perpetual: entry.get("type").and_then(|t| t.as_str()) == Some("PERPETUAL"),
            rfq: entry
                .get("isRfq")
                .and_then(|r| r.as_bool())
                .unwrap_or(false),
            active: entry
                .get("active")
                .and_then(|a| a.as_bool())
                .unwrap_or(false),
            status: entry
                .get("status")
                .and_then(|s| s.as_str())
                .unwrap_or("")
                .to_string(),
            tick: cfg
                .and_then(|c| c.get("minPriceChange"))
                .and_then(|v| v.as_str())
                .map(str::to_string),
            min_size: cfg
                .and_then(|c| c.get("minOrderSize"))
                .and_then(|v| v.as_str())
                .map(str::to_string),
        }
    }

    /// The stand-in used with `--no-symbol-check`: without the catalog the type
    /// and RFQ flag are unknown, so it assumes a perpetual (opening funding and
    /// mark) and no RFQ book. The book and trades firehoses still cover it.
    fn unchecked(name: &str) -> Self {
        Self {
            name: name.to_string(),
            perpetual: true,
            rfq: false,
            active: true,
            status: "UNKNOWN".to_string(),
            tick: None,
            min_size: None,
        }
    }
}

/// One socket to open: the full stream URL, and whether it is the RFQ
/// executable book (which is routed to a separate file — see [`route`]).
#[derive(Debug, Clone, PartialEq)]
pub struct ChannelUrl {
    pub url: String,
    /// `true` only for `/orderbooks/rfq/{market}`. The RFQ executable book and
    /// the plain indicative book are byte-identical in shape, so this flag —
    /// not anything in the frame — is what keeps them in separate files.
    pub rfq: bool,
}

/// The full set of sockets to open for a collection.
///
/// # Why the book and trades sockets are opened for *all* markets
///
/// Extended's book and trades channels take an optional trailing market
/// segment; omitting it subscribes to **every** market on the venue, and the
/// per-frame `data.m` says which market each update belongs to. This backend
/// takes that all-markets form for `/orderbooks` and `/publicTrades` and
/// filters to the requested set in [`route`], exactly as the wire spec
/// prescribes ("route by data.m").
///
/// The cost, and it is real, is measured: the all-markets `/orderbooks`
/// firehose ran at **~950 frames/s across 316 markets** on mainnet 2026-08-03,
/// so a recording of a handful of symbols parses the whole venue's book traffic
/// and writes under 1% of it. That load lands on the one hop whose overflow the
/// collector treats as fatal (`queue::WS_QUEUE_CAPACITY`), and it scales with
/// the *venue*, not with the recording. It is acceptable here because the
/// recording host is co-located in AWS Tokyo (cheap ingress) and the intended
/// use records a modest subset; a recording of one or two symbols would be
/// leaner on the per-market form (`/orderbooks/{market}`), which is a one-line
/// change should the firehose ever prove too heavy.
///
/// Funding, mark price and the RFQ book stay **per-market**: funding is
/// perpetual-only and roughly hourly, mark is ~0.7 frames/s, and the RFQ book
/// has no all-markets form at all. Opening them per market costs a few quiet
/// sockets and keeps the recording free of every other market's funding/mark.
fn channel_urls(base: &str, markets: &[MarketInfo]) -> Vec<ChannelUrl> {
    let mut urls = vec![
        ChannelUrl {
            url: format!("{base}/orderbooks"),
            rfq: false,
        },
        ChannelUrl {
            url: format!("{base}/publicTrades"),
            rfq: false,
        },
    ];
    for m in markets {
        // Funding is perpetual-only; a spot funding socket would connect and
        // never send, which reads downstream as a quiet feed.
        if m.perpetual {
            urls.push(ChannelUrl {
                url: format!("{base}/funding/{}", m.name),
                rfq: false,
            });
        }
        urls.push(ChannelUrl {
            url: format!("{base}/prices/mark/{}", m.name),
            rfq: false,
        });
        // The executable-quote book. Only RFQ markets serve it, and it is the
        // reason to record this venue at all (fill-uninformed-flow thesis).
        if m.rfq {
            urls.push(ChannelUrl {
                url: format!("{base}/orderbooks/rfq/{}", m.name),
                rfq: true,
            });
        }
    }
    urls
}

/// The market a data frame belongs to: `data.m` for book/funding/mark, or the
/// first entry's `m` for a trades frame (whose `data` is an array).
///
/// Returns `None` for a frame that names no market — an ack, an error envelope,
/// an empty trades array — which [`route`] sends to the sidecar rather than a
/// symbol file.
fn market_of(j: &serde_json::Value) -> Option<&str> {
    let data = j.get("data")?;
    // Book / funding / mark: `data` is an object carrying `m`.
    if let Some(m) = data.get("m").and_then(|m| m.as_str()) {
        return Some(m);
    }
    // Trades: `data` is an array of prints, each carrying `m`.
    data.as_array()?.first()?.get("m")?.as_str()
}

/// Where a received frame is filed.
#[derive(Debug, PartialEq)]
enum Routed {
    /// Written to this stream (a symbol file, or its `-rfq` sibling).
    Stream(String),
    /// Written to the sidecar: acks, errors, no-market frames, and the
    /// collector's own lifecycle records.
    Meta,
    /// Dropped. A frame from the all-markets firehose for a market this
    /// recording did not ask for — see [`channel_urls`]. This is the one
    /// deliberate drop, and it is *not* the silent-venue-rejection drop the
    /// collector forbids: rejections carry no market and go to [`Routed::Meta`]
    /// above; this is a book update for some other listed market that the
    /// operator did not scope the recording to.
    Skip,
}

/// Decides where a received frame goes.
///
/// `rfq` is a property of the *socket* the frame arrived on, not of the frame:
/// the RFQ executable book and the plain indicative book are identical in shape
/// (`{type, data:{m, b, a}, seq}`), so a frame from the RFQ socket is filed
/// under `{market}-rfq` and a frame from the plain socket under `{market}`.
/// Mixing them in one file would give a converter two interleaved books it
/// could not tell apart; recording BOTH, separated, is the point.
///
/// Everything else within a `{market}` file — book, trades, funding, mark — is
/// distinguishable by shape (`type`, an array `data`, an `f` field, `type:MP`),
/// so those legitimately share the file.
fn route(rfq: bool, requested: &HashSet<String>, j: &serde_json::Value) -> Routed {
    // The collector's own lifecycle records (see `meta.rs`) are tagged so they
    // can never be mistaken for venue output. Matched first, before any market
    // routing, so one can never reach a symbol file.
    if meta::is_record(j) {
        return Routed::Meta;
    }
    match market_of(j) {
        // Acks, error envelopes, empty trades arrays: kept, but in the sidecar.
        None => Routed::Meta,
        Some(market) => {
            if !requested.contains(market) {
                return Routed::Skip;
            }
            Routed::Stream(if rfq {
                format!("{market}-rfq")
            } else {
                market.to_string()
            })
        }
    }
}

/// Parses one frame and hands it to the writer under the stream [`route`]
/// chose. `rfq` is fixed per socket group (the RFQ pump passes `true`), so the
/// two pumps in [`run_collection`] share this one function.
fn handle(
    rfq: bool,
    requested: &HashSet<String>,
    writer_tx: &Tx<Record>,
    recv_time: DateTime<Utc>,
    data: Utf8Bytes,
) -> Result<(), ConnectorError> {
    let j: serde_json::Value = serde_json::from_str(data.as_str())?;
    match route(rfq, requested, &j) {
        Routed::Stream(stream) => writer_tx.send((recv_time, stream, data.to_string()))?,
        Routed::Meta => writer_tx.send((recv_time, META_STREAM.to_string(), data.to_string()))?,
        // A firehose frame for an unrequested market. Nothing to record.
        Routed::Skip => {}
    }
    Ok(())
}

/// Checks every requested market exists in the venue catalog and returns what
/// the recording needs to be self-describing.
///
/// Refusing to start on an unknown symbol is deliberate (§ fail closed): the
/// clearest explanation for an empty recording is "we refused to start". The
/// catalog also carries the two facts the sockets are built from — the market
/// `type` and `isRfq` — so a typo cannot silently downgrade a perpetual into a
/// spot with no funding, or an RFQ market into one with no executable book.
async fn resolve_markets(
    requested: &[String],
    rest_url: &str,
) -> Result<Vec<MarketInfo>, anyhow::Error> {
    // Bounded because this blocks startup: reqwest has no default timeout, so a
    // hung catalog fetch would leave the collector "starting" forever, recording
    // nothing. `retry_startup` absorbs a transient blip and then fails closed.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .user_agent(USER_AGENT)
        .build()?;

    let body: serde_json::Value = retry_startup("the Extended markets catalog", || async {
        client
            .get(format!("{rest_url}/api/v1/info/markets"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
            .map_err(anyhow::Error::from)
    })
    .await?;

    let catalog = body
        .get("data")
        .and_then(|d| d.as_array())
        .ok_or_else(|| anyhow::anyhow!("Extended /api/v1/info/markets has no `data` array"))?;

    let mut resolved = Vec::new();
    let mut unknown = Vec::new();
    for name in requested {
        match catalog
            .iter()
            .find(|m| m.get("name").and_then(|n| n.as_str()) == Some(name.as_str()))
        {
            Some(entry) => {
                let info = MarketInfo::from_entry(name, entry);
                // Not fatal: an inactive market may still be worth recording,
                // and the offline gate decides. But it is the likeliest reason
                // a socket then stays quiet, so it is flagged here.
                if !info.active || info.status != "ACTIVE" {
                    warn!(
                        market = %name,
                        active = info.active,
                        status = %info.status,
                        "requested market is not active; its feeds may be quiet"
                    );
                }
                resolved.push(info);
            }
            None => unknown.push(name.clone()),
        }
    }

    if !unknown.is_empty() {
        return Err(anyhow::anyhow!(
            "unknown Extended market(s): {}. Names are case-sensitive and take the form \
             BASE-USD (e.g. BTC-USD). Pass --no-symbol-check to skip.",
            unknown.join(", ")
        ));
    }
    Ok(resolved)
}

pub async fn run_collection(
    symbols: Vec<String>,
    writer_tx: Tx<Record>,
    check_symbols: bool,
) -> Result<(), anyhow::Error> {
    let markets: Vec<MarketInfo> = if check_symbols {
        match resolve_markets(&symbols, REST_URL).await {
            Ok(resolved) => {
                for m in &resolved {
                    info!(
                        market = %m.name,
                        perpetual = m.perpetual,
                        rfq = m.rfq,
                        tick = ?m.tick,
                        "resolved market"
                    );
                }
                // Recorded, not just logged: a converter needs tick/lot, and
                // `rfq` says which markets have a second (`-rfq`) book file.
                writer_tx.send((
                    Utc::now(),
                    META_STREAM.to_string(),
                    serde_json::json!({
                        "_collector": "universe",
                        "markets": resolved.iter().map(|m| serde_json::json!({
                            "name": m.name,
                            "perpetual": m.perpetual,
                            "rfq": m.rfq,
                            "status": m.status,
                            "tick": m.tick,
                            "min_size": m.min_size,
                        })).collect::<Vec<_>>(),
                    })
                    .to_string(),
                ))?;
                resolved
            }
            Err(error) => {
                if let Err(queue_error) = writer_tx.send((
                    Utc::now(),
                    META_STREAM.to_string(),
                    serde_json::json!({
                        "_collector": "symbol_check_failed",
                        "error": error.to_string(),
                        "symbols": symbols,
                    })
                    .to_string(),
                )) {
                    error!(
                        ?queue_error,
                        "couldn't record why the collector refused to start"
                    );
                }
                error!(%error, "refusing to start");
                return Err(error);
            }
        }
    } else {
        symbols.iter().map(|s| MarketInfo::unchecked(s)).collect()
    };

    let requested: HashSet<String> = markets.iter().map(|m| m.name.clone()).collect();

    // The RFQ sockets go to their own pump so their frames are filed under
    // `{market}-rfq`; everything else to the main pump under `{market}`.
    let mut main_urls: Vec<String> = Vec::new();
    let mut rfq_urls: Vec<String> = Vec::new();
    for u in channel_urls(WS_BASE, &markets) {
        if u.rfq {
            rfq_urls.push(u.url);
        } else {
            main_urls.push(u.url);
        }
    }

    info!(
        markets = markets.len(),
        main_sockets = main_urls.len(),
        rfq_sockets = rfq_urls.len(),
        "extended sockets"
    );

    // Two pumps sharing the one writer, and the reason is the RFQ book: it is
    // indistinguishable in shape from the plain book, so which pump a frame
    // arrives on is the only thing that files it under `{market}` versus
    // `{market}-rfq`. Baking that into the handle (a closure that fixes `rfq`)
    // keeps the frame the venue's raw bytes and reuses the shared `pump`
    // wholesale, rather than threading a channel tag through every hop.
    //
    // `try_join!` means either pump failing (a fatal hand-off) brings the whole
    // collection task down, which is what closes the writer channel and makes
    // `main` exit non-zero with the files finished. When `rfq_urls` is empty the
    // RFQ pump's producer drops its sender at once and the pump returns `Ok`
    // immediately, leaving the main pump running.
    let requested_main = requested.clone();
    let requested_rfq = requested;
    let writer_tx_rfq = writer_tx.clone();

    let main = pump(
        writer_tx,
        move |ws_tx| keep_connections(main_urls, ws_tx),
        move |writer_tx, recv_time, data| {
            handle(false, &requested_main, writer_tx, recv_time, data)
        },
    );
    let rfq = pump(
        writer_tx_rfq,
        move |ws_tx| keep_connections(rfq_urls, ws_tx),
        move |writer_tx, recv_time, data| handle(true, &requested_rfq, writer_tx, recv_time, data),
    );

    tokio::try_join!(main, rfq).map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(names: &[&str]) -> HashSet<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    fn j(s: &str) -> serde_json::Value {
        serde_json::from_str(s).unwrap()
    }

    // Real frames captured from mainnet on 2026-08-03. Trimmed in the arrays,
    // untouched in shape.
    const BOOK_SNAPSHOT: &str = r#"{"type":"SNAPSHOT","data":{"t":"SNAPSHOT","m":"BTC-USD","b":[{"q":"14.79329","p":"63720"}],"a":[{"q":"5.1","p":"63721"}]},"ts":1785778403000,"seq":1}"#;
    const BOOK_DELTA: &str = r#"{"type":"DELTA","data":{"t":"DELTA","m":"BTC-USD","b":[{"q":"-0.00005","p":"63717","c":"0.85928"}],"a":[],"d":"f"},"ts":1785778403488,"seq":2}"#;
    const TRADES: &str = r#"{"data":[{"i":2084331146732113920,"m":"ETH-USD","S":"BUY","tT":"TRADE","T":1785778245038,"p":"63709","q":"0.00481"}],"ts":1785778245045,"seq":2}"#;
    const MARK: &str = r#"{"type":"MP","data":{"m":"BTC-USD","p":"63807.605956624996","ts":0},"ts":1785778593418,"seq":1}"#;
    // From the venue's own documentation for the funding stream.
    const FUNDING: &str =
        r#"{"ts":1701563440000,"data":{"m":"BTC-USD","T":1701563440000,"f":"0.001"},"seq":2}"#;
    const RFQ_SNAPSHOT: &str = r#"{"type":"SNAPSHOT","data":{"t":"SNAPSHOT","m":"ATOM-USD","b":[],"a":[],"d":"f"},"ts":1785776292909,"seq":1}"#;

    // ---- channel_urls -------------------------------------------------------

    fn market(name: &str, perpetual: bool, rfq: bool) -> MarketInfo {
        MarketInfo {
            name: name.to_string(),
            perpetual,
            rfq,
            active: true,
            status: "ACTIVE".to_string(),
            tick: None,
            min_size: None,
        }
    }

    /// The book and trades sockets are the all-markets form — the URL ends at
    /// the channel, with no market segment — because the wire spec routes them
    /// by `data.m`. A market segment accidentally appended here would silently
    /// narrow the recording to one market's book.
    #[test]
    fn book_and_trades_are_the_all_markets_form() {
        let urls = channel_urls("wss://x/v1", &[market("BTC-USD", true, false)]);
        let plain: Vec<&str> = urls
            .iter()
            .filter(|u| !u.rfq)
            .map(|u| u.url.as_str())
            .collect();
        assert!(plain.contains(&"wss://x/v1/orderbooks"));
        assert!(plain.contains(&"wss://x/v1/publicTrades"));
        // and NOT the per-market form for either.
        assert!(!plain.iter().any(|u| u.contains("/orderbooks/BTC-USD")));
        assert!(!plain.iter().any(|u| u.contains("/publicTrades/BTC-USD")));
    }

    /// Funding is per-market and perpetual-only; mark is per-market for any
    /// market. A spot market must get mark but no funding, or the funding
    /// socket connects and never serves.
    #[test]
    fn funding_is_perp_only_and_mark_is_per_market() {
        let urls = channel_urls(
            "wss://x/v1",
            &[
                market("BTC-USD", true, false),
                market("USDC-USD", false, false),
            ],
        );
        let all: Vec<&str> = urls.iter().map(|u| u.url.as_str()).collect();
        assert!(all.contains(&"wss://x/v1/funding/BTC-USD"));
        assert!(all.contains(&"wss://x/v1/prices/mark/BTC-USD"));
        // Spot: mark yes, funding no.
        assert!(all.contains(&"wss://x/v1/prices/mark/USDC-USD"));
        assert!(!all.iter().any(|u| u.contains("/funding/USDC-USD")));
    }

    /// The RFQ executable book is opened only for RFQ markets, per-market (it
    /// has no all-markets form), and is flagged so it is filed separately.
    #[test]
    fn only_rfq_markets_get_the_rfq_book_and_it_is_flagged() {
        let urls = channel_urls(
            "wss://x/v1",
            &[
                market("BTC-USD", true, false),
                market("ATOM-USD", true, true),
            ],
        );
        let rfq: Vec<&ChannelUrl> = urls.iter().filter(|u| u.rfq).collect();
        assert_eq!(rfq.len(), 1, "exactly one RFQ socket");
        assert_eq!(rfq[0].url, "wss://x/v1/orderbooks/rfq/ATOM-USD");
        // BTC-USD is not an RFQ market, so no RFQ socket for it.
        assert!(!urls.iter().any(|u| u.url.contains("/rfq/BTC-USD")));
    }

    // ---- route --------------------------------------------------------------

    /// A book/trades/funding/mark frame is filed under the market its `data.m`
    /// (or, for the array-shaped trades frame, its first entry's `m`) names.
    #[test]
    fn a_data_frame_is_routed_to_the_market_in_its_payload() {
        let requested = set(&["BTC-USD", "ETH-USD"]);
        for frame in [BOOK_SNAPSHOT, BOOK_DELTA, MARK, FUNDING] {
            assert_eq!(
                route(false, &requested, &j(frame)),
                Routed::Stream("BTC-USD".to_string()),
                "{frame}"
            );
        }
        // Trades carry the market on the array entries, not on `data` itself.
        assert_eq!(
            route(false, &requested, &j(TRADES)),
            Routed::Stream("ETH-USD".to_string())
        );
    }

    /// The RFQ socket's frames go to a separate `{market}-rfq` file. The frame
    /// is shape-identical to a plain book frame, so only the socket's `rfq`
    /// flag distinguishes them — get this wrong and two books corrupt one file.
    #[test]
    fn the_rfq_book_is_filed_under_a_separate_stream() {
        let requested = set(&["ATOM-USD"]);
        assert_eq!(
            route(true, &requested, &j(RFQ_SNAPSHOT)),
            Routed::Stream("ATOM-USD-rfq".to_string())
        );
        // The very same frame on the plain socket would be the indicative book.
        assert_eq!(
            route(false, &requested, &j(RFQ_SNAPSHOT)),
            Routed::Stream("ATOM-USD".to_string())
        );
    }

    /// The all-markets firehose delivers every listed market; one the recording
    /// did not ask for is dropped, not written and not filed to meta.
    #[test]
    fn a_firehose_frame_for_an_unrequested_market_is_skipped() {
        let requested = set(&["ETH-USD"]);
        assert_eq!(route(false, &requested, &j(BOOK_SNAPSHOT)), Routed::Skip);
    }

    /// Acks, error envelopes and no-market frames carry no market and go to the
    /// sidecar — never dropped, so a venue rejection is always recorded
    /// somewhere.
    #[test]
    fn acks_errors_and_no_market_frames_go_to_meta() {
        let requested = set(&["BTC-USD"]);
        for frame in [
            r#"{"type":"SNAPSHOT","error":"bad market","ts":1,"seq":0}"#,
            r#"{"data":{"foo":"bar"},"ts":1,"seq":0}"#,
            r#"{"data":[],"ts":1,"seq":0}"#,
            r#"{"ts":1}"#,
        ] {
            assert_eq!(route(false, &requested, &j(frame)), Routed::Meta, "{frame}");
        }
    }

    /// Every shared lifecycle record reaches the sidecar and none can be filed
    /// under a market — this is the Extended end of the guarantee every backend
    /// asserts, tying this venue to the one sidecar vocabulary.
    #[test]
    fn every_lifecycle_record_reaches_the_sidecar() {
        let requested = set(&["BTC-USD"]);
        for value in [
            meta::subscribe(WS_BASE, 0, serde_json::json!([WS_BASE])),
            meta::connected(WS_BASE),
            meta::disconnected("reset", 1194),
            meta::dial_failed("connection refused", 12),
            meta::stream_ended(1194),
        ] {
            assert_eq!(route(false, &requested, &value), Routed::Meta, "{value}");
        }
    }

    /// A `_collector` tag wins over a market, so a synthetic record can never be
    /// mistaken for venue data even if it happens to mention one.
    #[test]
    fn the_collector_tag_takes_precedence_over_a_market() {
        let requested = set(&["BTC-USD"]);
        let tagged = j(r#"{"_collector":"connected","data":{"m":"BTC-USD"}}"#);
        assert_eq!(route(false, &requested, &tagged), Routed::Meta);
    }

    // ---- handle -------------------------------------------------------------

    /// Every frame lands somewhere: a requested market to its file, a rejection
    /// to meta, an unrequested-market firehose frame dropped. The recorded
    /// payload is the venue's bytes verbatim.
    #[test]
    fn handle_files_each_frame_where_route_decided() {
        use crate::queue::{self, Record, WRITER_HOP};
        let (tx, mut rx, _fatal) = queue::test_bounded::<Record>(WRITER_HOP, 8);
        let requested = set(&["BTC-USD"]);
        let now = Utc::now();

        handle(false, &requested, &tx, now, BOOK_SNAPSHOT.into()).unwrap(); // -> BTC-USD
        handle(false, &requested, &tx, now, TRADES.into()).unwrap(); // ETH-USD: skipped
        handle(false, &requested, &tx, now, r#"{"error":"x"}"#.into()).unwrap(); // -> meta
        drop(tx);

        let mut got = Vec::new();
        while let Ok((_, stream, payload)) = rx.try_recv() {
            got.push((stream, payload));
        }
        assert_eq!(got.len(), 2, "the ETH-USD firehose frame must be dropped");
        assert_eq!(got[0].0, "BTC-USD");
        assert_eq!(got[0].1, BOOK_SNAPSHOT, "payload stored verbatim");
        assert_eq!(got[1].0, META_STREAM);
    }

    /// The RFQ pump files the same-shaped frame under `-rfq`, so the two books
    /// end up in two files.
    #[test]
    fn the_rfq_pump_and_the_plain_pump_write_different_files() {
        use crate::queue::{self, Record, WRITER_HOP};
        let (tx, mut rx, _fatal) = queue::test_bounded::<Record>(WRITER_HOP, 8);
        let requested = set(&["ATOM-USD"]);
        let now = Utc::now();

        handle(false, &requested, &tx, now, RFQ_SNAPSHOT.into()).unwrap();
        handle(true, &requested, &tx, now, RFQ_SNAPSHOT.into()).unwrap();
        drop(tx);

        let streams: Vec<String> = std::iter::from_fn(|| rx.try_recv().ok())
            .map(|(_, s, _)| s)
            .collect();
        assert_eq!(streams, vec!["ATOM-USD", "ATOM-USD-rfq"]);
    }

    /// A frame the writer cannot take is an error, not a silent drop — the one
    /// failure no log line would otherwise mention.
    #[test]
    fn a_frame_the_writer_cannot_take_is_an_error() {
        use crate::queue::{self, Record, WRITER_HOP};
        let (tx, _rx, _fatal) = queue::test_bounded::<Record>(WRITER_HOP, 1);
        let requested = set(&["BTC-USD"]);
        let now = Utc::now();

        handle(false, &requested, &tx, now, BOOK_SNAPSHOT.into()).expect("first fits");
        let error = handle(false, &requested, &tx, now, BOOK_SNAPSHOT.into())
            .expect_err("a frame that could not be handed over must not report success");
        assert!(matches!(error, ConnectorError::Queue(_)), "{error}");
    }

    /// `--no-symbol-check` builds a market with no catalog: it must assume a
    /// perpetual (so funding and mark open) and no RFQ book, and still open the
    /// two firehoses.
    #[test]
    fn unchecked_markets_still_open_the_core_sockets() {
        let m = MarketInfo::unchecked("BTC-USD");
        assert!(m.perpetual);
        assert!(!m.rfq);
        let urls = channel_urls(WS_BASE, &[m]);
        assert!(urls.iter().any(|u| u.url.ends_with("/orderbooks")));
        assert!(urls.iter().any(|u| u.url.ends_with("/publicTrades")));
        assert!(urls.iter().any(|u| u.url.contains("/funding/BTC-USD")));
        assert!(!urls.iter().any(|u| u.rfq));
    }

    /// The catalog row carries the two facts the sockets are built from — the
    /// market type and the RFQ flag — plus the tick a converter needs.
    #[test]
    fn market_info_reads_the_catalog_fields_the_sockets_need() {
        let entry = j(
            r#"{"name":"BTC-USD","type":"PERPETUAL","isRfq":false,"active":true,"status":"ACTIVE","tradingConfig":{"minPriceChange":"1","minOrderSize":"0.0001"}}"#,
        );
        let info = MarketInfo::from_entry("BTC-USD", &entry);
        assert!(info.perpetual);
        assert!(!info.rfq);
        assert!(info.active);
        assert_eq!(info.tick.as_deref(), Some("1"));
        assert_eq!(info.min_size.as_deref(), Some("0.0001"));

        let rfq_entry = j(
            r#"{"name":"ATOM-USD","type":"PERPETUAL","isRfq":true,"active":true,"status":"ACTIVE"}"#,
        );
        let rfq_info = MarketInfo::from_entry("ATOM-USD", &rfq_entry);
        assert!(rfq_info.rfq);
        assert!(rfq_info.tick.is_none());
    }
}
