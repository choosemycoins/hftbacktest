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
    writer_tx.send((recv_time, stream, data.to_string()))?;
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
