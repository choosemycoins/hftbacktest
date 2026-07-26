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
