mod http;

/// Mainnet endpoints. Testnet lives at `api.hyperliquid-testnet.xyz` and has
/// different asset indices, so it is not a drop-in substitution.
pub const WS_URL: &str = "wss://api.hyperliquid.xyz/ws";
pub const REST_URL: &str = "https://api.hyperliquid.xyz";

use chrono::{DateTime, Utc};
pub use http::keep_connection;
use tokio::sync::mpsc::UnboundedSender;
use tokio_tungstenite::tungstenite::Utf8Bytes;
use tracing::{error, info};

use crate::{error::ConnectorError, file::META_STREAM};

/// Decides which stream a received frame belongs to.
///
/// Every frame lands somewhere. A frame that can be attributed to a coin goes
/// to that coin's file; everything else — subscription acks, venue errors,
/// the collector's own connection events, anything unrecognised — goes to
/// [`META_STREAM`]. Dropping frames was how the collector came to record a
/// venue rejection nowhere at all, leaving a file whose gaps had no
/// explanation.
fn route(j: &serde_json::Value) -> &str {
    // Synthetic frames the collector itself injects (see http.rs) are tagged
    // so they can never be confused with something the venue said.
    if j.get("_collector").is_some() {
        return META_STREAM;
    }

    let Some(channel) = j.get("channel").and_then(|c| c.as_str()) else {
        return META_STREAM;
    };

    match channel {
        // A trades frame is an array; the coin is on its entries. An empty
        // array names no coin, so it goes to meta rather than nowhere.
        "trades" => j
            .get("data")
            .and_then(|d| d.as_array())
            .and_then(|a| a.first())
            .and_then(|t| t.get("coin"))
            .and_then(|c| c.as_str())
            .unwrap_or(META_STREAM),
        // Everything else that carries a coin is filed under it. `error`
        // frames carry a bare string as `data`, and `subscriptionResponse`
        // carries the echoed subscription — neither has a coin, so both fall
        // through to meta, which is exactly where they are wanted.
        _ => j
            .get("data")
            .and_then(|d| d.get("coin"))
            .and_then(|c| c.as_str())
            .unwrap_or(META_STREAM),
    }
}

fn handle(
    writer_tx: &UnboundedSender<(DateTime<Utc>, String, String)>,
    recv_time: DateTime<Utc>,
    data: Utf8Bytes,
) -> Result<(), ConnectorError> {
    let j: serde_json::Value = serde_json::from_str(data.as_str())?;
    let stream = route(&j).to_string();
    let _ = writer_tx.send((recv_time, stream, data.to_string()));
    Ok(())
}

/// One WebSocket subscription, before the coin is substituted in.
///
/// `l2Book` accepts a `fast` flag that trades depth for frequency: omitted (or
/// false) gives 20 levels per side roughly every 5s, `true` gives 5 levels
/// roughly every 0.5s (measured against mainnet 2026-07-25).
///
/// The collector subscribes to both and makes no attempt to reconcile them.
/// Merging depth from feeds of different rates and depths is a policy choice
/// with no single right answer, and baking one in at capture time would make
/// the recording unable to answer any other question. Recording both, plus the
/// subscription provenance in [`META_STREAM`], is what lets several merge
/// policies be run over the same bytes afterwards and compared.
#[derive(Clone, Debug)]
pub struct SubscriptionSpec {
    pub kind: String,
    /// `Some(_)` serialises a `fast` field; `None` omits it entirely.
    pub fast: Option<bool>,
}

impl SubscriptionSpec {
    pub fn plain(kind: &str) -> Self {
        Self {
            kind: kind.to_string(),
            fast: None,
        }
    }

    pub fn l2_book(fast: bool) -> Self {
        Self {
            kind: "l2Book".to_string(),
            // The plain feed omits the field rather than sending `false`. The
            // venue treats the two identically, but omitting it keeps the
            // subscription byte-identical to what the collector sent before
            // the fast feed was added, so recordings from either version are
            // directly comparable. The ack in the meta stream still reports
            // the venue's normalised `fast: false`, so no provenance is lost.
            fast: if fast { Some(true) } else { None },
        }
    }
}

/// Fails unless every requested coin exists in the venue's perp universe.
///
/// Hyperliquid closes the ENTIRE WebSocket when asked to subscribe to a coin it
/// does not know — no error frame, no close reason, and every valid
/// subscription on that connection dies with it. The retry loop then reconnects
/// forever, so the collector records a trickle of data punctuated by holes and
/// exits 0. Verified against mainnet: `BTC NOPE_XYZ` produced eight reconnects
/// in sixteen seconds and a file full of gaps.
///
/// One REST call at startup turns that into a refusal to start.
async fn verify_symbols(symbols: &[String], rest_url: &str) -> Result<(), anyhow::Error> {
    let resp: serde_json::Value = reqwest::Client::new()
        .post(format!("{rest_url}/info"))
        .json(&serde_json::json!({ "type": "meta" }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    let known: std::collections::HashSet<&str> = resp
        .get("universe")
        .and_then(|u| u.as_array())
        .ok_or_else(|| anyhow::anyhow!("unexpected /info meta response: no `universe` array"))?
        .iter()
        .filter_map(|a| a.get("name").and_then(|n| n.as_str()))
        .collect();

    if known.is_empty() {
        return Err(anyhow::anyhow!(
            "the venue returned an empty perp universe; refusing to guess"
        ));
    }

    let unknown: Vec<&str> = symbols
        .iter()
        .map(|s| s.as_str())
        .filter(|s| !known.contains(s))
        .collect();
    if !unknown.is_empty() {
        return Err(anyhow::anyhow!(
            "unknown Hyperliquid perp symbol(s): {}. \
             Names are case-sensitive and unprefixed (BTC, not BTCUSDT). \
             {} perps are listed; pass --no-symbol-check to skip this.",
            unknown.join(", "),
            known.len()
        ));
    }
    Ok(())
}

pub async fn run_collection(
    subscriptions: Vec<SubscriptionSpec>,
    symbols: Vec<String>,
    writer_tx: UnboundedSender<(DateTime<Utc>, String, String)>,
    check_symbols: bool,
) -> Result<(), anyhow::Error> {
    if check_symbols {
        if let Err(error) = verify_symbols(&symbols, REST_URL).await {
            // Recorded as well as logged: the sidecar should be able to explain
            // an empty recording, and "we refused to start" is the clearest
            // explanation there is.
            let _ = writer_tx.send((
                Utc::now(),
                META_STREAM.to_string(),
                serde_json::json!({
                    "_collector": "symbol_check_failed",
                    "error": error.to_string(),
                    "symbols": symbols,
                })
                .to_string(),
            ));
            error!(%error, "refusing to start");
            return Err(error);
        }
        info!(count = symbols.len(), "symbols verified against /info meta");
    }

    let (ws_tx, mut ws_rx) = tokio::sync::mpsc::unbounded_channel();
    let h = tokio::spawn(keep_connection(subscriptions, symbols, ws_tx.clone()));

    while let Some((recv_time, data)) = ws_rx.recv().await {
        if let Err(error) = handle(&writer_tx, recv_time, data) {
            error!(?error, "couldn't handle the received data.");
        }
    }
    let _ = h.await;
    Ok(())
}

#[cfg(test)]
mod route_tests {
    use super::*;

    fn r(s: &str) -> String {
        route(&serde_json::from_str(s).unwrap()).to_string()
    }

    #[test]
    fn market_data_is_filed_under_its_coin() {
        assert_eq!(
            r(r#"{"channel":"l2Book","data":{"coin":"BTC","time":1,"levels":[[],[]]}}"#),
            "BTC"
        );
        assert_eq!(
            r(r#"{"channel":"l2Book","data":{"coin":"BTC","time":1,"levels":[[],[]],"fast":true}}"#),
            "BTC"
        );
        assert_eq!(
            r(r#"{"channel":"bbo","data":{"coin":"ETH","time":1,"bbo":[null,null]}}"#),
            "ETH"
        );
        assert_eq!(
            r(r#"{"channel":"trades","data":[{"coin":"SOL","px":"1"}]}"#),
            "SOL"
        );
    }

    /// These three were previously discarded. A venue rejection recorded
    /// nowhere is why a broken subscription could look identical to a quiet
    /// market in the resulting file.
    #[test]
    fn connection_level_frames_go_to_meta() {
        assert_eq!(
            r(r#"{"channel":"error","data":"Error parsing JSON into valid websocket request"}"#),
            META_STREAM
        );
        assert_eq!(
            r(r#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC","fast":true}}}"#),
            META_STREAM
        );
        assert_eq!(r(r#"{"_collector":"disconnected","error":"reset"}"#), META_STREAM);
    }

    /// An empty trades array names no coin. It is still a frame the venue
    /// sent, so it is kept rather than dropped.
    #[test]
    fn unattributable_frames_are_kept_not_dropped() {
        assert_eq!(r(r#"{"channel":"trades","data":[]}"#), META_STREAM);
        assert_eq!(r(r#"{"channel":"somethingNew","data":{"x":1}}"#), META_STREAM);
        assert_eq!(r(r#"{"channel":"pong"}"#), META_STREAM);
        assert_eq!(r(r#"{"unexpected":"shape"}"#), META_STREAM);
    }

    /// A `_collector` tag wins over anything else in the frame, so a synthetic
    /// record can never be mistaken for venue data even if it mentions a coin.
    #[test]
    fn collector_tag_takes_precedence() {
        assert_eq!(
            r(r#"{"_collector":"subscribe","channel":"l2Book","data":{"coin":"BTC"}}"#),
            META_STREAM
        );
    }

    #[test]
    fn handle_writes_every_frame_somewhere() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let now = Utc::now();
        for frame in [
            r#"{"channel":"l2Book","data":{"coin":"BTC","time":1,"levels":[[],[]]}}"#,
            r#"{"channel":"error","data":"boom"}"#,
            r#"{"channel":"trades","data":[]}"#,
        ] {
            handle(&tx, now, frame.into()).unwrap();
        }
        drop(tx);

        let mut got = Vec::new();
        while let Ok((_, stream, payload)) = rx.try_recv() {
            got.push((stream, payload));
        }
        assert_eq!(got.len(), 3, "every frame must be written, none dropped");
        assert_eq!(got[0].0, "BTC");
        assert_eq!(got[1].0, META_STREAM);
        assert_eq!(got[2].0, META_STREAM);
        // The payload is stored verbatim — that is what makes the recording
        // replayable under a different merge policy later.
        assert!(got[1].1.contains("boom"));
    }
}
