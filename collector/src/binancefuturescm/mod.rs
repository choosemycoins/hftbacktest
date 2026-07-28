mod http;

use std::collections::HashMap;

use chrono::{DateTime, Utc};
pub use http::{fetch_depth_snapshot, keep_connection};
use tokio_tungstenite::tungstenite::Utf8Bytes;
use tracing::{error, warn};

use crate::{
    error::ConnectorError,
    file::META_STREAM,
    meta,
    pump::pump,
    queue::{Record, Tx},
    throttler::Throttler,
};

/// The streams recorded for every symbol, before `$symbol` is substituted.
///
/// See [`crate::binancefuturesum::STREAMS`] for why this is a constant with a
/// test on it rather than a literal at the call site.
///
/// `@markPrice@1s` is carried here for the same reason as on USD-M: `i` is the
/// venue's own spot basket and `r` is the funding rate that basket determines.
/// It was worth checking rather than assuming, because COIN-M also publishes a
/// separate `<pair>@indexPrice` stream and might have kept the index only
/// there — measured on `dstream.binance.com` 2026-07-28, it does not: the
/// per-symbol `markPriceUpdate` carries `i` directly, alongside two fields
/// USD-M does not document (`ap`, `st`).
///
/// That settles the pairing too. An `indexPriceUpdate` frame is keyed by pair
/// (`"i":"BTCUSD"`) and has no `s` at all, so `handle` would file it nowhere
/// and return `Ok` — a stream that records absolutely nothing, silently. Not
/// needing it is the reason this backend gets the same one line as USD-M
/// rather than a second, differently-shaped path.
pub const STREAMS: [&str; 4] = [
    "$symbol@trade",
    "$symbol@bookTicker",
    "$symbol@depth@0ms",
    "$symbol@markPrice@1s",
];

fn handle(
    prev_u_map: &mut HashMap<String, i64>,
    writer_tx: &Tx<Record>,
    recv_time: DateTime<Utc>,
    data: Utf8Bytes,
    throttler: &Throttler,
) -> Result<(), ConnectorError> {
    let j: serde_json::Value = serde_json::from_str(data.as_str())?;
    // The collector's own lifecycle records travel this hop alongside the
    // venue's frames (see `meta.rs`). Matched before anything else, so one can
    // never reach the symbol routing below, which has no symbol to give it.
    if meta::is_record(&j) {
        writer_tx.send((recv_time, META_STREAM.to_string(), data.to_string()))?;
        return Ok(());
    }
    if let Some(j_data) = j.get("data")
        && let Some(j_symbol) = j_data
            .as_object()
            .ok_or(ConnectorError::FormatError)?
            .get("s")
    {
        let symbol = j_symbol.as_str().ok_or(ConnectorError::FormatError)?;
        let ev = j_data
            .get("e")
            .ok_or(ConnectorError::FormatError)?
            .as_str()
            .ok_or(ConnectorError::FormatError)?;
        if ev == "depthUpdate" {
            let u = j_data
                .get("u")
                .ok_or(ConnectorError::FormatError)?
                .as_i64()
                .ok_or(ConnectorError::FormatError)?;
            let pu = j_data
                .get("pu")
                .ok_or(ConnectorError::FormatError)?
                .as_i64()
                .ok_or(ConnectorError::FormatError)?;
            let prev_u = prev_u_map.get(symbol);
            if prev_u.is_none() || pu != *prev_u.unwrap() {
                warn!(%symbol, "missing depth feed has been detected.");
                let symbol_ = symbol.to_string();
                let writer_tx_ = writer_tx.clone();
                let mut throttler_ = throttler.clone();
                tokio::spawn(async move {
                    match throttler_.execute(fetch_depth_snapshot(&symbol_)).await {
                        Some(Ok(data)) => {
                            let recv_time = Utc::now();
                            // Detached: there is no caller to return an error
                            // to, so discarding this result would be the one
                            // silent drop the bound cannot catch. `send` has
                            // already raised the fatal signal by the time this
                            // logs — that signal is the whole error path a
                            // spawned task has.
                            if let Err(error) = writer_tx_.send((recv_time, symbol_, data)) {
                                error!(?error, "couldn't hand the depth snapshot to the writer");
                            }
                        }
                        Some(Err(error)) => {
                            error!(
                                symbol = symbol_,
                                ?error,
                                "couldn't fetch the depth snapshot."
                            );
                        }
                        None => {
                            warn!(
                                symbol = symbol_,
                                "Fetching the depth snapshot is rate-limited."
                            )
                        }
                    }
                });
            }
            *prev_u_map.entry(symbol.to_string()).or_insert(0) = u;
        }
        writer_tx.send((recv_time, symbol.to_string(), data.to_string()))?;
    }
    Ok(())
}

pub async fn run_collection(
    streams: Vec<String>,
    symbols: Vec<String>,
    writer_tx: Tx<Record>,
) -> Result<(), anyhow::Error> {
    let mut prev_u_map = HashMap::new();
    // https://www.binance.com/en/support/faq/rate-limits-on-binance-futures-281596e222414cdd9051664ea621cdc3
    // The default rate limit per IP is 2,400/min and the weight is 20 at a depth of 1000.
    // The maximum request rate for fetching snapshots is 120 per minute.
    // Sets the rate limit with a margin to account for connection requests.
    let throttler = Throttler::new(100);
    pump(
        writer_tx,
        |ws_tx| keep_connection(streams, symbols, ws_tx),
        move |writer_tx, recv_time, data| {
            handle(&mut prev_u_map, writer_tx, recv_time, data, &throttler)
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::{self, WRITER_HOP};

    /// Captured verbatim from `dstream.binance.com` on 2026-07-28, combined
    /// stream `btcusd_perp@markPrice@1s`.
    ///
    /// Recorded here because COIN-M's payload is not the documented USD-M one:
    /// it carries two extra fields (`ap`, `st`) and — the question that decided
    /// whether this stream was worth subscribing to on COIN-M at all — it does
    /// carry `i`, the index price, per symbol. The separate `<pair>@indexPrice`
    /// stream is therefore not needed, which matters because an
    /// `indexPriceUpdate` frame is keyed by pair (`"i":"BTCUSD"`) and has no
    /// `s` at all: `handle` would file it nowhere and return `Ok`.
    fn mark_price_update() -> String {
        concat!(
            r#"{"stream":"btcusd_perp@markPrice@1s","data":{"e":"markPriceUpdate","#,
            r#""E":1785239516000,"s":"BTCUSD_PERP","p":"63406.00000000","#,
            r#""ap":"63406.00000000","P":"63402.48303662","i":"63427.35155222","#,
            r#""r":"0.00005945","T":1785254400000,"st":2}}"#
        )
        .to_string()
    }

    /// The COIN-M half of the rule spelled out on `binancefuturesum::STREAMS`:
    /// a misspelled stream name is accepted, acked and then silently never
    /// served, so nothing but this assertion stands between a typo and a
    /// recording that is quietly missing a feed.
    #[test]
    fn every_recorded_stream_name_is_well_formed() {
        assert!(
            STREAMS.contains(&"$symbol@markPrice@1s"),
            "the mark-price feed carries the index price and funding rate"
        );
        for stream in STREAMS {
            assert!(
                stream.starts_with("$symbol@"),
                "{stream}: every stream is per-symbol and the placeholder is substituted verbatim"
            );
            assert!(
                !stream.contains("@@"),
                "{stream}: a doubled `@` is silently accepted by the venue and records nothing"
            );
        }
    }

    /// Mark-price frames have to reach the symbol's file, and they have to
    /// leave the depth-gap detector alone on the way.
    ///
    /// They carry `s` but neither `u` nor `pu`, so they route by symbol like
    /// everything else — but the `e == "depthUpdate"` guard is the only thing
    /// standing between them and the gap logic. Without it, every one of them
    /// would fail to find `u` and leave through `FormatError`, killing the
    /// collector once a second; with a laxer guard they would reset
    /// `prev_u_map` and make the next genuine depth frame look like a gap,
    /// firing a REST snapshot refetch per second against a 100/min throttle.
    #[test]
    fn mark_price_frames_are_filed_under_their_symbol_and_leave_gap_detection_alone() {
        let (tx, mut rx, _fatal) = queue::test_bounded::<Record>(WRITER_HOP, 4);
        let mut prev_u_map = HashMap::from([("BTCUSD_PERP".to_string(), 100)]);
        let throttler = Throttler::new(100);
        let now = Utc::now();

        handle(
            &mut prev_u_map,
            &tx,
            now,
            mark_price_update().as_str().into(),
            &throttler,
        )
        .expect("a mark-price frame is ordinary market data, not a parse failure");

        let (_, stream, payload) = rx.try_recv().expect("the frame must be written");
        assert_eq!(
            stream, "BTCUSD_PERP",
            "it belongs to the symbol, not the sidecar"
        );
        assert!(
            payload.contains(r#""i":"63427.35155222""#),
            "the index price is the whole reason this stream is recorded"
        );
        assert_eq!(
            prev_u_map.get("BTCUSD_PERP"),
            Some(&100),
            "a non-depth frame must not touch the depth sequence state"
        );
    }

    /// A record the writer cannot take must be an error, not a discarded frame.
    /// Bounding the channel is what makes that distinction exist at all: the
    /// `let _ = send` this used to be was harmless while the queue was
    /// unbounded and is silent data loss now.
    #[test]
    fn a_frame_the_writer_cannot_take_is_an_error_not_a_drop() {
        let (tx, _rx, _fatal) = queue::test_bounded::<Record>(WRITER_HOP, 1);
        let mut prev_u_map = HashMap::new();
        let throttler = Throttler::new(100);
        let now = Utc::now();
        let frame = r#"{"data":{"e":"trade","s":"BTCUSD_PERP"}}"#;

        handle(&mut prev_u_map, &tx, now, frame.into(), &throttler).expect("the first frame fits");

        let error = handle(&mut prev_u_map, &tx, now, frame.into(), &throttler)
            .expect_err("a frame that could not be handed over must not be reported as written");
        assert!(matches!(error, ConnectorError::Queue(_)), "{error}");
    }

    /// The collector's own lifecycle records ride the same hop as the venue's
    /// frames, so `handle` is both what files them in the sidecar and what
    /// keeps them out of the symbol files — this backend routes on a symbol
    /// parsed out of the frame, and a lifecycle record has none. Until they
    /// were wired in, a Binance recording wrote nothing to `_meta` at all and
    /// so could not explain a single gap.
    #[test]
    fn lifecycle_records_are_filed_under_meta_and_market_data_still_is_not() {
        let (tx, mut rx, _fatal) = queue::test_bounded::<Record>(WRITER_HOP, 4);
        let mut prev_u_map = HashMap::new();
        let throttler = Throttler::new(100);
        let now = Utc::now();
        let lifecycle = meta::disconnected("Connection reset without closing handshake", 1194);

        handle(
            &mut prev_u_map,
            &tx,
            now,
            lifecycle.to_string().as_str().into(),
            &throttler,
        )
        .unwrap();
        handle(
            &mut prev_u_map,
            &tx,
            now,
            r#"{"data":{"e":"trade","s":"BTCUSD_PERP"}}"#.into(),
            &throttler,
        )
        .unwrap();

        let (_, stream, payload) = rx.try_recv().expect("the lifecycle record must be written");
        assert_eq!(stream, META_STREAM);
        let j: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(j["_collector"], "disconnected");
        assert_eq!(j["connected_for_ms"], 1194);

        assert_eq!(
            rx.try_recv().expect("market data must still be written").1,
            "BTCUSD_PERP",
            "the symbol routing below the tag check is untouched"
        );
    }
}
