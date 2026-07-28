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
/// A constant rather than a literal at the call site because a wrong stream
/// name is invisible at runtime: Binance accepts any name in the
/// combined-stream URL, acks it, and sends nothing — measured 2026-07-28, a
/// deliberately bogus `btcusdt@totalnonsense` behaved exactly like a stream
/// that exists and is quiet. `every_recorded_stream_name_is_well_formed` is
/// therefore the only place a typo here can be caught at all.
///
/// `@markPrice@1s` is the odd one out: it is not order flow. Its
/// `markPriceUpdate` frames carry the **index price** — Binance's own spot
/// basket, aggregated across its constituent exchanges — alongside the mark
/// price and the funding rate. That basket is the cheap way to record what
/// perpetual funding is priced against, at a few hundred bytes a second
/// instead of the raw spot books of every constituent venue.
pub const STREAMS: [&str; 4] = [
    "$symbol@trade",
    "$symbol@bookTicker",
    "$symbol@depth@0ms",
    // Not `@@markPrice@1s`, which is how this line sat commented out for a
    // while. The venue would have accepted the doubled `@` without a word.
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

    fn depth_update(u: i64, pu: i64) -> String {
        format!(r#"{{"data":{{"e":"depthUpdate","s":"BTCUSDT","u":{u},"pu":{pu}}}}}"#)
    }

    /// A `markPriceUpdate` frame as the combined-stream endpoint delivers it.
    ///
    /// The field set is Binance's documented USD-M one; it could not be
    /// captured here, because `fstream.binance.com` serves only `@trade`,
    /// `@bookTicker` and `@depth@0ms` from this vantage point — see the note on
    /// [`STREAMS`]. The COIN-M sibling of this fixture in `binancefuturescm` was
    /// captured live and agrees on every field this test depends on.
    fn mark_price_update() -> String {
        concat!(
            r#"{"stream":"btcusdt@markPrice@1s","data":{"e":"markPriceUpdate","#,
            r#""E":1785239516000,"s":"BTCUSDT","p":"63406.00000000","#,
            r#""i":"63427.35155222","P":"63402.48303662","r":"0.00005945","#,
            r#""T":1785254400000}}"#
        )
        .to_string()
    }

    /// A misspelled stream name is not an error anywhere: Binance accepts any
    /// name in the combined-stream URL, acks it, and simply never sends for it.
    /// Measured 2026-07-28 — `btcusdt@totalnonsense` connected and delivered
    /// zero frames in eight seconds, which is indistinguishable from a stream
    /// that exists and is quiet. The mark-price line sat in this list commented
    /// out and misspelled with two `@` (`$symbol@@markPrice@1s`); had it simply
    /// been uncommented, the recording would have been missing the index price
    /// and funding rate with nothing at all to say so. Nothing but this
    /// assertion can catch that class of typo before it reaches a recording.
    #[test]
    fn every_recorded_stream_name_is_well_formed() {
        assert!(
            STREAMS.contains(&"$symbol@markPrice@1s"),
            "the mark-price feed carries the index price — Binance's own spot \
             basket — and the funding rate, which is why it is recorded at all"
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
    /// collector once a second; with a laxer guard they would reset `prev_u_map`
    /// and make the next genuine depth frame look like a gap, firing a REST
    /// snapshot refetch per second against a 100/min throttle.
    #[test]
    fn mark_price_frames_are_filed_under_their_symbol_and_leave_gap_detection_alone() {
        let (tx, mut rx, _fatal) = queue::test_bounded::<Record>(WRITER_HOP, 4);
        let mut prev_u_map = HashMap::from([("BTCUSDT".to_string(), 100)]);
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
            stream, "BTCUSDT",
            "it belongs to the symbol, not the sidecar"
        );
        assert!(
            payload.contains(r#""i":"63427.35155222""#),
            "the index price is the whole reason this stream is recorded"
        );
        assert_eq!(
            prev_u_map.get("BTCUSDT"),
            Some(&100),
            "a non-depth frame must not touch the depth sequence state"
        );
    }

    /// The same rule at the Binance call site, which is also the one holding
    /// the detached REST snapshot task. That task has no caller to return an
    /// error to at all, so its only route out is the fatal signal `Tx::send`
    /// raises — which is why the result may not be discarded here either.
    #[test]
    fn a_frame_the_writer_cannot_take_is_an_error_not_a_drop() {
        let (tx, _rx, _fatal) = queue::test_bounded::<Record>(WRITER_HOP, 1);
        // Primed so both frames continue the sequence: a gap would spawn the
        // REST snapshot fetch, which is not what this test is about.
        let mut prev_u_map = HashMap::from([("BTCUSDT".to_string(), 1)]);
        let throttler = Throttler::new(100);
        let now = Utc::now();

        handle(
            &mut prev_u_map,
            &tx,
            now,
            depth_update(2, 1).as_str().into(),
            &throttler,
        )
        .expect("the first frame fits");

        let error = handle(
            &mut prev_u_map,
            &tx,
            now,
            depth_update(3, 2).as_str().into(),
            &throttler,
        )
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
            r#"{"data":{"e":"trade","s":"BTCUSDT"}}"#.into(),
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
            "BTCUSDT",
            "the symbol routing below the tag check is untouched"
        );
    }
}
