mod http;

/// Mainnet endpoints. Testnet lives at `api.hyperliquid-testnet.xyz` and has
/// different asset indices, so it is not a drop-in substitution.
pub const WS_URL: &str = "wss://api.hyperliquid.xyz/ws";
pub const REST_URL: &str = "https://api.hyperliquid.xyz";

use std::{collections::HashMap, time::Duration};

use chrono::{DateTime, Utc};
pub use http::keep_connection;
use tokio_tungstenite::tungstenite::Utf8Bytes;
use tracing::{error, info, warn};

use crate::{
    error::ConnectorError,
    file::META_STREAM,
    queue::{self, Frame, QUEUE_CAPACITY, Record, Tx, WS_HOP},
};

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
    writer_tx: &Tx<Record>,
    recv_time: DateTime<Utc>,
    data: Utf8Bytes,
) -> Result<(), ConnectorError> {
    let j: serde_json::Value = serde_json::from_str(data.as_str())?;
    let stream = route(&j).to_string();
    writer_tx.send((recv_time, stream, data.to_string()))?;
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

/// The perp DEX a wire coin string belongs to. `""` is the canonical one.
///
/// HIP-3 lets third parties deploy their own perp DEXes, each with its own
/// instrument universe and its own collateral token, and a universe entry's
/// `name` already carries the prefix — `hyna:ENA` is both the entry name and
/// the wire coin string, so nothing is concatenated. Verified live: subscribing
/// to `hyna:ENA` returns data, and it is a different instrument from the
/// canonical `ENA`.
fn dex_of(symbol: &str) -> &str {
    symbol.split_once(':').map_or("", |(dex, _)| dex)
}

/// What the venue says about one requested instrument.
#[derive(Debug, Clone, PartialEq)]
pub struct SymbolInfo {
    pub wire: String,
    pub dex: String,
    pub collateral: String,
    pub sz_decimals: i64,
    pub max_leverage: i64,
}

/// Matches requested symbols against already-fetched universes.
///
/// Split out from the HTTP so the interesting half is testable without a
/// network: `universes` maps a dex name (`""` for canonical) to that dex's
/// `meta` response, and `collateral` maps a dex name to its collateral token.
fn match_universes(
    symbols: &[String],
    universes: &HashMap<String, serde_json::Value>,
    collateral: &HashMap<String, String>,
) -> Result<Vec<SymbolInfo>, anyhow::Error> {
    let mut out = Vec::new();
    let mut unknown = Vec::new();

    for symbol in symbols {
        let dex = dex_of(symbol);
        let Some(meta) = universes.get(dex) else {
            return Err(anyhow::anyhow!(
                "unknown Hyperliquid perp dex {dex:?} in symbol {symbol:?}. \
                 The canonical dex has no prefix; builder dexes are listed by \
                 the `perpDexs` info request."
            ));
        };
        let entries = meta
            .get("universe")
            .and_then(|u| u.as_array())
            .ok_or_else(|| anyhow::anyhow!("dex {dex:?}: /info meta has no `universe` array"))?;

        match entries
            .iter()
            .find(|a| a.get("name").and_then(|n| n.as_str()) == Some(symbol.as_str()))
        {
            Some(a) => out.push(SymbolInfo {
                wire: symbol.clone(),
                dex: dex.to_string(),
                collateral: collateral.get(dex).cloned().unwrap_or_else(|| "?".into()),
                sz_decimals: a.get("szDecimals").and_then(|v| v.as_i64()).unwrap_or(-1),
                max_leverage: a.get("maxLeverage").and_then(|v| v.as_i64()).unwrap_or(-1),
            }),
            None => {
                // Point at the likely mistake rather than just refusing. The
                // two common ones are a quote suffix carried over from another
                // venue, and double-prefixing a builder dex.
                let base = symbol.rsplit(':').next().unwrap_or(symbol);
                let elsewhere: Vec<&str> = universes
                    .values()
                    .flat_map(|m| m.get("universe").and_then(|u| u.as_array()).into_iter())
                    .flatten()
                    .filter_map(|a| a.get("name").and_then(|n| n.as_str()))
                    .filter(|n| n.rsplit(':').next() == Some(base))
                    .collect();
                unknown.push(if elsewhere.is_empty() {
                    format!("{symbol} (not listed on any perp dex)")
                } else {
                    format!("{symbol} (did you mean: {}?)", elsewhere.join(", "))
                });
            }
        }
    }

    if !unknown.is_empty() {
        return Err(anyhow::anyhow!(
            "unknown Hyperliquid perp symbol(s): {}. Names are case-sensitive; \
             the canonical dex takes a bare coin (BTC, not BTCUSDT) and a builder \
             dex takes its prefixed name (xyz:GOLD). Pass --no-symbol-check to skip.",
            unknown.join("; ")
        ));
    }
    Ok(out)
}

/// Fails unless every requested coin exists on the perp dex it names.
///
/// Hyperliquid closes the ENTIRE WebSocket when asked to subscribe to a coin it
/// does not know — no error frame, no close reason, and every valid
/// subscription on that connection dies with it. The retry loop then reconnects
/// forever, so the collector records a trickle of data punctuated by holes and
/// exits 0. Verified against mainnet: `BTC NOPE_XYZ` produced eight reconnects
/// in sixteen seconds and a file full of gaps.
///
/// A handful of REST calls at startup turn that into a refusal to start, and
/// yield the instrument metadata worth recording (§ the `universe` meta record).
async fn resolve_symbols(
    symbols: &[String],
    rest_url: &str,
) -> Result<Vec<SymbolInfo>, anyhow::Error> {
    // Bounded because this blocks startup: reqwest has no default timeout, so
    // a hung `/info` would leave the collector "starting" indefinitely,
    // recording nothing and reporting nothing. The responses are small, and a
    // refusal to start is a better outcome than an invisible stall.
    const INFO_TIMEOUT: Duration = Duration::from_secs(15);

    let client = reqwest::Client::builder().timeout(INFO_TIMEOUT).build()?;
    let info = async |body: serde_json::Value| -> Result<serde_json::Value, anyhow::Error> {
        Ok(client
            .post(format!("{rest_url}/info"))
            .json(&body)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    };

    // Token index -> name, so the collateral can be recorded as "USDE" rather
    // than "235". Best effort: a failure here must not block startup.
    let token_names: HashMap<i64, String> = match info(serde_json::json!({"type":"spotMeta"})).await
    {
        Ok(sm) => sm
            .get("tokens")
            .and_then(|t| t.as_array())
            .map(|ts| {
                ts.iter()
                    .filter_map(|t| {
                        Some((
                            t.get("index")?.as_i64()?,
                            t.get("name")?.as_str()?.to_string(),
                        ))
                    })
                    .collect()
            })
            .unwrap_or_default(),
        Err(error) => {
            warn!(?error, "couldn't read spotMeta; collateral will be unnamed");
            HashMap::new()
        }
    };

    // Only fetch the dexes actually referenced.
    let mut wanted: Vec<String> = symbols.iter().map(|s| dex_of(s).to_string()).collect();
    wanted.sort();
    wanted.dedup();

    let mut universes = HashMap::new();
    let mut collateral = HashMap::new();
    for dex in wanted {
        let body = if dex.is_empty() {
            serde_json::json!({"type": "meta"})
        } else {
            serde_json::json!({"type": "meta", "dex": dex})
        };
        let meta = info(body)
            .await
            .map_err(|e| anyhow::anyhow!("couldn't read the perp universe for dex {dex:?}: {e}"))?;
        let ct = meta.get("collateralToken").and_then(|v| v.as_i64());
        collateral.insert(
            dex.clone(),
            ct.and_then(|i| token_names.get(&i).cloned())
                .unwrap_or_else(|| ct.map_or("?".into(), |i| i.to_string())),
        );
        universes.insert(dex, meta);
    }

    match_universes(symbols, &universes, &collateral)
}

pub async fn run_collection(
    subscriptions: Vec<SubscriptionSpec>,
    symbols: Vec<String>,
    writer_tx: Tx<Record>,
    check_symbols: bool,
) -> Result<(), anyhow::Error> {
    if check_symbols {
        match resolve_symbols(&symbols, REST_URL).await {
            Ok(resolved) => {
                for s in &resolved {
                    info!(
                        wire = %s.wire,
                        dex = %if s.dex.is_empty() { "canonical" } else { &s.dex },
                        collateral = %s.collateral,
                        sz_decimals = s.sz_decimals,
                        "resolved symbol"
                    );
                }
                // Recorded, not just logged. Two instruments can share a base
                // asset across dexes — `ENA` on the canonical dex and
                // `hyna:ENA` on a USDE-collateralised one are different books
                // — and `szDecimals` is what a converter needs for lot size.
                // Without this the recording cannot say which was which.
                writer_tx.send((
                    Utc::now(),
                    META_STREAM.to_string(),
                    serde_json::json!({
                        "_collector": "universe",
                        "symbols": resolved.iter().map(|s| serde_json::json!({
                            "wire": s.wire,
                            "dex": s.dex,
                            "collateral": s.collateral,
                            "szDecimals": s.sz_decimals,
                            "maxLeverage": s.max_leverage,
                        })).collect::<Vec<_>>(),
                    })
                    .to_string(),
                ))?;
            }
            Err(error) => {
                // The sidecar should be able to explain an empty recording, and
                // "we refused to start" is the clearest explanation there is.
                // A failure to write it does not replace the original error:
                // the refusal to start is the more useful of the two, and
                // `send` has already raised its own signal.
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
    }

    pump(writer_tx, |ws_tx| {
        keep_connection(subscriptions, symbols, ws_tx)
    })
    .await
}

/// Drains the producer's frames into the writer channel, and returns once the
/// producer has released its sender.
///
/// Split out of [`run_collection`] so a test can supply a producer that ends.
/// The real one ends only when the venue closes the socket without an error,
/// which never happens on a live connection — so this exit path went
/// unexercised, and the leaked sender clone that used to hold it open was
/// invisible.
async fn pump<P, F>(writer_tx: Tx<Record>, producer: P) -> Result<(), anyhow::Error>
where
    P: FnOnce(Tx<Frame>) -> F,
    F: Future<Output = ()> + Send + 'static,
{
    let (ws_tx, mut ws_rx) = queue::bounded(WS_HOP, QUEUE_CAPACITY, writer_tx.fatal());
    // The producer takes the only sender: holding a clone here would keep
    // `recv()` pending for ever after the producer died, and the collector
    // would sit idle recording nothing instead of exiting non-zero.
    let h = tokio::spawn(producer(ws_tx));

    while let Some((recv_time, data)) = ws_rx.recv().await {
        match handle(&writer_tx, recv_time, data) {
            Ok(()) => {}
            // Not a bad frame but a broken recording: the writer has stopped
            // taking records, so every frame after this one would be discarded
            // in silence. Returning ends the task, which closes the writer
            // channel and brings `main` down with the files finished.
            Err(error @ ConnectorError::Queue(_)) => {
                error!(?error, "fatal stream error; stopping the collection task.");
                return Err(error.into());
            }
            Err(error) => {
                error!(?error, "couldn't handle the received data.");
            }
        }
    }
    let _ = h.await;
    Ok(())
}

#[cfg(test)]
mod pump_tests {
    use super::*;
    use crate::queue::WRITER_HOP;

    /// A producer that stops is the collector's last line of defence: the main
    /// loop treats the writer channel closing as fatal and exits non-zero
    /// (`main.rs`, the `None` arm of `writer_rx.recv()`). That only fires if
    /// the collection task actually returns, and it only returns once the last
    /// `ws_tx` is gone. Keeping a clone alongside the producer's — as this used
    /// to — turned a dead producer into an idle process that systemd reports as
    /// perfectly healthy while nothing is being recorded.
    #[tokio::test]
    async fn producer_finishing_with_a_clean_eof_ends_the_collection() {
        let (writer_tx, mut writer_rx, _fatal) = queue::test_bounded::<Record>(WRITER_HOP, 8);

        let done = tokio::time::timeout(
            Duration::from_secs(5),
            pump(writer_tx, |ws_tx| async move {
                let frame =
                    r#"{"channel":"l2Book","data":{"coin":"BTC","time":1,"levels":[[],[]]}}"#;
                ws_tx.send((Utc::now(), Utf8Bytes::from(frame))).unwrap();
                // Returning drops the sender, exactly as `keep_connection`
                // does when the socket closes without an error.
            }),
        )
        .await;

        assert!(
            done.is_ok(),
            "pump must return once the producer releases the last ws_tx"
        );
        done.unwrap().unwrap();

        // The frame sent before the producer exited is not lost on the way out.
        let (_, stream, _) = writer_rx.try_recv().expect("the frame must be written");
        assert_eq!(stream, "BTC");
    }

    /// The other end of the same guarantee. A writer that has stopped draining
    /// must end the collection task, because that is what makes `main` exit
    /// non-zero with the files closed. Logging and reading the next frame — the
    /// behaviour a bounded channel inherits from `let _ = send` — would instead
    /// discard market data silently for as long as the process stayed up.
    #[tokio::test]
    async fn a_writer_that_stops_draining_ends_the_collection() {
        // The receiver is held but never read, so the queue fills and stays
        // full; capacity 1 just gets there in one frame instead of 4096.
        let (writer_tx, _writer_rx, mut fatal) = queue::test_bounded::<Record>(WRITER_HOP, 1);

        let result = tokio::time::timeout(
            Duration::from_secs(5),
            pump(writer_tx, |ws_tx| async move {
                let frame =
                    r#"{"channel":"l2Book","data":{"coin":"BTC","time":1,"levels":[[],[]]}}"#;
                // The socket keeps delivering while the writer is stuck. This
                // hop has room, so the producer notices nothing.
                for _ in 0..4 {
                    ws_tx.send((Utc::now(), Utf8Bytes::from(frame))).unwrap();
                }
            }),
        )
        .await
        .expect("pump must not hang once the writer stops draining");

        assert!(
            result.is_err(),
            "the collection task must fail rather than carry on dropping frames"
        );
        assert!(
            tokio::time::timeout(Duration::from_secs(1), fatal.recv())
                .await
                .is_ok(),
            "and the reason must reach main, which is the half a returned error cannot cover"
        );
    }
}

#[cfg(test)]
mod universe_tests {
    use super::*;

    fn uni(entries: serde_json::Value) -> serde_json::Value {
        serde_json::json!({ "universe": entries })
    }

    fn fixture() -> (HashMap<String, serde_json::Value>, HashMap<String, String>) {
        let mut u = HashMap::new();
        u.insert(
            "".to_string(),
            uni(serde_json::json!([
                {"name":"BTC","szDecimals":5,"maxLeverage":40},
                {"name":"ENA","szDecimals":0,"maxLeverage":10},
            ])),
        );
        // A builder dex: the entry name already carries the prefix.
        u.insert(
            "hyna".to_string(),
            uni(serde_json::json!([{"name":"hyna:ENA","szDecimals":0,"maxLeverage":10}])),
        );
        u.insert(
            "xyz".to_string(),
            uni(serde_json::json!([{"name":"xyz:GOLD","szDecimals":4,"maxLeverage":25}])),
        );
        let mut c = HashMap::new();
        c.insert("".to_string(), "USDC".to_string());
        c.insert("hyna".to_string(), "USDE".to_string());
        c.insert("xyz".to_string(), "USDC".to_string());
        (u, c)
    }

    fn resolve(syms: &[&str]) -> Result<Vec<SymbolInfo>, anyhow::Error> {
        let (u, c) = fixture();
        let v: Vec<String> = syms.iter().map(|s| s.to_string()).collect();
        match_universes(&v, &u, &c)
    }

    #[test]
    fn dex_prefix_is_the_part_before_the_first_colon() {
        assert_eq!(dex_of("BTC"), "");
        assert_eq!(dex_of("hyna:ENA"), "hyna");
        assert_eq!(dex_of("xyz:GOLD"), "xyz");
    }

    /// The same base asset on two dexes is two instruments with two books and
    /// two collateral tokens. Conflating them would silently mix the data.
    #[test]
    fn same_asset_on_two_dexes_resolves_to_distinct_instruments() {
        let got = resolve(&["ENA", "hyna:ENA"]).unwrap();
        assert_eq!(got[0].dex, "");
        assert_eq!(got[0].collateral, "USDC");
        assert_eq!(got[1].dex, "hyna");
        assert_eq!(got[1].collateral, "USDE");
        assert_ne!(got[0].wire, got[1].wire);
    }

    /// `szDecimals` is what a converter needs for lot size, so it has to
    /// survive into the recording rather than being re-guessed later.
    #[test]
    fn instrument_metadata_is_captured() {
        let got = resolve(&["xyz:GOLD"]).unwrap();
        assert_eq!(got[0].sz_decimals, 4);
        assert_eq!(got[0].max_leverage, 25);
        assert_eq!(got[0].collateral, "USDC");
    }

    /// The entry name already contains the prefix, so concatenating the dex
    /// onto it is the obvious mistake. It must be caught, and named.
    #[test]
    fn double_prefixing_is_rejected_with_a_hint() {
        let err = resolve(&["hyna:hyna:ENA"]).unwrap_err().to_string();
        assert!(err.contains("hyna:hyna:ENA"), "{err}");
        assert!(err.contains("did you mean"), "{err}");
        assert!(err.contains("hyna:ENA"), "{err}");
    }

    /// A quote suffix carried over from another venue is the other common
    /// mistake — Hyperliquid perps have none.
    #[test]
    fn quote_suffixed_name_is_rejected() {
        let err = resolve(&["ENAUSDC"]).unwrap_err().to_string();
        assert!(err.contains("not listed on any perp dex"), "{err}");
    }

    /// An unknown dex is a different error from an unknown coin, and saying so
    /// saves the operator hunting for a typo in the wrong half of the string.
    #[test]
    fn unknown_dex_is_reported_as_such() {
        let err = resolve(&["nope:BTC"]).unwrap_err().to_string();
        assert!(err.contains("unknown Hyperliquid perp dex"), "{err}");
        assert!(err.contains("perpDexs"), "{err}");
    }

    #[test]
    fn every_unknown_symbol_is_reported_not_just_the_first() {
        let err = resolve(&["BTC", "NOPE", "ALSONOPE"])
            .unwrap_err()
            .to_string();
        assert!(err.contains("NOPE"), "{err}");
        assert!(err.contains("ALSONOPE"), "{err}");
    }
}

#[cfg(test)]
mod route_tests {
    use super::*;
    use crate::queue::WRITER_HOP;

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
            r(
                r#"{"channel":"l2Book","data":{"coin":"BTC","time":1,"levels":[[],[]],"fast":true}}"#
            ),
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
            r(
                r#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC","fast":true}}}"#
            ),
            META_STREAM
        );
        assert_eq!(
            r(r#"{"_collector":"disconnected","error":"reset"}"#),
            META_STREAM
        );
    }

    /// An empty trades array names no coin. It is still a frame the venue
    /// sent, so it is kept rather than dropped.
    #[test]
    fn unattributable_frames_are_kept_not_dropped() {
        assert_eq!(r(r#"{"channel":"trades","data":[]}"#), META_STREAM);
        assert_eq!(
            r(r#"{"channel":"somethingNew","data":{"x":1}}"#),
            META_STREAM
        );
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
        let (tx, mut rx, _fatal) = queue::test_bounded::<Record>(WRITER_HOP, 8);
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

    /// The other half of "every frame lands somewhere": when the writer cannot
    /// take one, `handle` has to say so. Discarding the send result — harmless
    /// while the channel was unbounded — is a silent data drop on a bounded
    /// one, and the only failure here that no log line would ever mention.
    #[test]
    fn a_frame_the_writer_cannot_take_is_an_error_not_a_drop() {
        let (tx, _rx, _fatal) = queue::test_bounded::<Record>(WRITER_HOP, 1);
        let now = Utc::now();
        let frame = r#"{"channel":"l2Book","data":{"coin":"BTC","time":1,"levels":[[],[]]}}"#;

        handle(&tx, now, frame.into()).expect("the first frame fits");

        let error = handle(&tx, now, frame.into())
            .expect_err("a frame that could not be handed over must not be reported as written");
        assert!(matches!(error, ConnectorError::Queue(_)), "{error}");
    }
}
