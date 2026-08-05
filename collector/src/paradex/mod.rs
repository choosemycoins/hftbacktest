mod http;

/// Mainnet endpoints. `ws.api.prod` is the live feed; the REST base carries the
/// `/v1` catalog. Testnet lives under `*.testnet.paradex.trade` with a different
/// instrument set, so it is not a drop-in substitution.
pub const WS_URL: &str = "wss://ws.api.prod.paradex.trade/v1";
pub const REST_URL: &str = "https://api.prod.paradex.trade/v1";

use chrono::{DateTime, Utc};
pub use http::keep_connection;
use tokio_tungstenite::tungstenite::Utf8Bytes;
use tracing::{error, info};

use crate::{
    error::ConnectorError,
    file::META_STREAM,
    meta,
    pump::pump,
    queue::{Record, Tx},
};

/// The channel set taken for every market, `{market}` substituted per symbol.
///
/// Both order books are recorded on purpose: `snapshot` is the plain API book,
/// `interactive` is the RPI-inclusive one, and they are DIFFERENT books — the
/// gap between them is exactly the retail-price-improvement flow this backend
/// exists to capture (fill-uninformed-flow thesis). Recording only one would
/// silently discard the reason for collecting Paradex at all. `@15` is depth 15
/// (the venue max for this feed); `@100ms` is the refresh — full-depth `deltas`
/// are deliberately NOT here, they are ~65x the bytes and reserved for a small
/// core set once the compressed rate is measured. `trades` tags each print with
/// `trade_type` (RPI-labelled), which is what the fill-quality harness reads.
pub const CHANNELS: [&str; 5] = [
    "bbo.{market}",
    "trades.{market}",
    "funding_data.{market}",
    "order_book.{market}.snapshot@15@100ms",
    "order_book.{market}.interactive@15@100ms",
];

/// Decides which stream a received frame belongs to.
///
/// Every frame lands somewhere. A data update carries its channel at
/// `params.channel` (e.g. `bbo.BTC-USD-PERP` or
/// `order_book.BTC-USD-PERP.snapshot@15@100ms`); the market is the second
/// dot-segment (a Paradex market — `BTC-USD-PERP` — contains dashes, not dots).
/// Subscription acks (`result`), venue errors (`error`), pongs, and the
/// collector's own lifecycle records carry no such channel and go to
/// [`META_STREAM`] — dropping them was how the old collector recorded a venue
/// rejection nowhere at all.
fn route(j: &serde_json::Value) -> &str {
    if meta::is_record(j) {
        return META_STREAM;
    }
    j.get("params")
        .and_then(|p| p.get("channel"))
        .and_then(|c| c.as_str())
        .and_then(|channel| channel.split('.').nth(1))
        .unwrap_or(META_STREAM)
}

fn handle(
    writer_tx: &Tx<Record>,
    recv_time: DateTime<Utc>,
    data: Utf8Bytes,
) -> Result<(), ConnectorError> {
    let j: serde_json::Value = serde_json::from_str(data.as_str())?;
    let stream = route(&j).to_string();
    // The record separator belongs to the writer, which writes
    // `"{timestamp} {data}\n"` (`file.rs`). Paradex — alone among the four
    // backends — newline-terminates its WebSocket text frames, so forwarding
    // the frame verbatim the way bybit/hyperliquid/lighter do put a second
    // newline in every record and made half of every Paradex file blank. The
    // other three do not trim because their frames carry no terminator; adding
    // it there would be a change with nothing to fix.
    //
    // Only the trailing line terminator is removed, and only as text: the frame
    // is written byte for byte otherwise. Re-serialising the parsed `j` would
    // be shorter and is wrong — `serde_json::Value` sorts object keys and
    // renormalises numbers, i.e. it would rewrite the venue's bytes, and this
    // collector's contract is to record feeds as they are. Paradex sends
    // compact JSON, so there are no interior newlines to consider; one would
    // still split a record, but it cannot be removed without rewriting the
    // frame, and it has never been observed.
    let frame = data.as_str().trim_end_matches(['\r', '\n']);
    writer_tx.send((recv_time, stream, frame.to_string()))?;
    Ok(())
}

/// Checks every requested market exists in the `/v1/markets` catalog, and
/// returns the rows the recording needs to be self-describing (symbol, price
/// tick, size increment). A converter cannot reconstruct tick/lot after the
/// fact, so — like the Hyperliquid universe record — they are captured, not
/// just logged. Refusing to start on an unknown symbol is deliberate: the
/// clearest explanation for an empty recording is "we refused to start".
async fn resolve_markets(
    markets: &[String],
    rest_url: &str,
) -> Result<Vec<serde_json::Value>, anyhow::Error> {
    let body: serde_json::Value = reqwest::Client::new()
        .get(format!("{rest_url}/markets"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    let catalog = body
        .get("results")
        .and_then(|r| r.as_array())
        .ok_or_else(|| anyhow::anyhow!("Paradex /v1/markets has no `results` array"))?;

    let mut resolved = Vec::new();
    let mut unknown = Vec::new();
    for market in markets {
        match catalog
            .iter()
            .find(|m| m.get("symbol").and_then(|s| s.as_str()) == Some(market.as_str()))
        {
            Some(m) => resolved.push(serde_json::json!({
                "symbol": market,
                "price_tick_size": m.get("price_tick_size"),
                "order_size_increment": m.get("order_size_increment"),
                "asset_kind": m.get("asset_kind"),
            })),
            None => unknown.push(market.clone()),
        }
    }

    if !unknown.is_empty() {
        return Err(anyhow::anyhow!(
            "unknown Paradex market(s): {}. Symbols are case-sensitive and take the \
             form BASE-USD-PERP (e.g. BTC-USD-PERP). Pass --no-symbol-check to skip.",
            unknown.join(", ")
        ));
    }
    Ok(resolved)
}

pub async fn run_collection(
    markets: Vec<String>,
    writer_tx: Tx<Record>,
    check_symbols: bool,
) -> Result<(), anyhow::Error> {
    if check_symbols {
        match resolve_markets(&markets, REST_URL).await {
            Ok(resolved) => {
                for m in &resolved {
                    info!(market = %m["symbol"], tick = %m["price_tick_size"], "resolved market");
                }
                writer_tx.send((
                    Utc::now(),
                    META_STREAM.to_string(),
                    serde_json::json!({ "_collector": "universe", "markets": resolved })
                        .to_string(),
                ))?;
            }
            Err(error) => {
                if let Err(queue_error) = writer_tx.send((
                    Utc::now(),
                    META_STREAM.to_string(),
                    serde_json::json!({
                        "_collector": "symbol_check_failed",
                        "error": error.to_string(),
                        "markets": markets,
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
    }

    pump(
        writer_tx,
        |ws_tx| keep_connection(CHANNELS.to_vec(), markets, ws_tx),
        handle,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::{self, WRITER_HOP};

    /// Drains everything `handle` handed to the writer.
    fn written(frames: &[&str]) -> Vec<(String, String)> {
        let (tx, mut rx, _fatal) = queue::test_bounded::<Record>(WRITER_HOP, 8);
        let now = Utc::now();
        for frame in frames {
            handle(&tx, now, (*frame).into()).unwrap();
        }
        drop(tx);

        let mut got = Vec::new();
        while let Ok((_, stream, payload)) = rx.try_recv() {
            got.push((stream, payload));
        }
        got
    }

    /// A data frame, with its keys deliberately NOT in sorted order: any fix
    /// that re-serialised the parsed `Value` would reorder them (`Value` is a
    /// `BTreeMap` here — `preserve_order` is off), which is a rewrite of the
    /// venue's bytes and exactly what the collector's "record feeds as they
    /// are" contract forbids.
    const UNSORTED: &str = concat!(
        r#"{"params":{"channel":"bbo.BTC-USD-PERP","data":{"market":"BTC-USD-PERP","#,
        r#""ask":"2.0","bid":"1.0"}},"method":"subscription","jsonrpc":"2.0"}"#
    );

    /// The writer owns the record separator (`file.rs`: `"{timestamp} {data}\n"`),
    /// so a frame that arrives already newline-terminated writes a blank line
    /// after every record — half the tape. Paradex is the only venue of the four
    /// that terminates its text frames, so it is the only backend that trims.
    #[test]
    fn a_newline_terminated_frame_is_written_without_it() {
        let got = written(&[&format!("{UNSORTED}\n")]);
        assert_eq!(got.len(), 1);
        assert!(
            !got[0].1.ends_with('\n'),
            "the transport newline reached the writer: {:?}",
            got[0].1
        );
        assert_eq!(got[0].1, UNSORTED);
    }

    #[test]
    fn a_crlf_terminated_frame_is_written_without_it() {
        let got = written(&[&format!("{UNSORTED}\r\n")]);
        assert_eq!(got.len(), 1);
        assert!(
            !got[0].1.ends_with('\n') && !got[0].1.ends_with('\r'),
            "a CRLF terminator reached the writer: {:?}",
            got[0].1
        );
        assert_eq!(got[0].1, UNSORTED);
    }

    /// The trim removes the transport terminator and nothing else: an
    /// unterminated frame must reach the writer byte for byte, key order
    /// included.
    #[test]
    fn an_unterminated_frame_is_written_byte_for_byte() {
        let got = written(&[UNSORTED]);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].1, UNSORTED, "the venue's bytes were rewritten");
    }

    /// Routing reads the parsed frame, and `serde_json` tolerates trailing
    /// whitespace — so the terminator never disturbed it, and the trim must not
    /// disturb it either.
    #[test]
    fn routing_is_unchanged_by_the_trim() {
        let got = written(&[
            "{\"params\":{\"channel\":\"bbo.BTC-USD-PERP\",\"data\":{}},\"method\":\"subscription\"}\n",
            "{\"params\":{\"channel\":\"trades.ETH-USD-PERP\",\"data\":[]},\"method\":\"subscription\"}\n",
            "{\"params\":{\"channel\":\"funding_data.SOL-USD-PERP\",\"data\":{}},\"method\":\"subscription\"}\n",
            "{\"jsonrpc\":\"2.0\",\"result\":{\"channel\":\"bbo.BTC-USD-PERP\"},\"id\":1}\n",
            "{\"jsonrpc\":\"2.0\",\"error\":{\"code\":-32000,\"message\":\"bad channel\"},\"id\":2}\n",
            "{\"_collector\":\"connected\",\"url\":\"wss://x\"}\n",
        ]);
        let streams: Vec<&str> = got.iter().map(|(s, _)| s.as_str()).collect();
        assert_eq!(
            streams,
            [
                "BTC-USD-PERP",
                "ETH-USD-PERP",
                "SOL-USD-PERP",
                META_STREAM,
                META_STREAM,
                META_STREAM
            ]
        );
    }

    #[test]
    fn a_data_frame_is_routed_to_the_market_in_its_channel() {
        let bbo = serde_json::json!({
            "jsonrpc": "2.0", "method": "subscription",
            "params": { "channel": "bbo.BTC-USD-PERP", "data": {} }
        });
        assert_eq!(route(&bbo), "BTC-USD-PERP");

        // The order_book channel has a trailing feed_type@depth@refresh; the
        // market is still the second dot-segment, unaffected by the suffix.
        let book = serde_json::json!({
            "jsonrpc": "2.0", "method": "subscription",
            "params": { "channel": "order_book.ETH-USD-PERP.interactive@15@100ms", "data": {} }
        });
        assert_eq!(route(&book), "ETH-USD-PERP");
    }

    #[test]
    fn acks_errors_and_lifecycle_go_to_meta() {
        // A subscribe ack: `result`, no `params.channel`.
        let ack = serde_json::json!({
            "jsonrpc": "2.0", "result": { "channel": "bbo.BTC-USD-PERP" }, "id": 1
        });
        assert_eq!(route(&ack), META_STREAM);

        // A JSON-RPC error carries no channel.
        let err = serde_json::json!({
            "jsonrpc": "2.0", "error": { "code": -32000, "message": "bad channel" }, "id": 2
        });
        assert_eq!(route(&err), META_STREAM);

        // A lifecycle record the collector injected is tagged and never confused
        // with venue output.
        let lifecycle = serde_json::json!({ "_collector": "connected", "url": WS_URL });
        assert_eq!(route(&lifecycle), META_STREAM);
    }
}
