use chrono::{DateTime, Utc};
use tokio_tungstenite::tungstenite::Utf8Bytes;
use tracing::error;

use self::http::keep_connection;
use crate::{
    error::ConnectorError,
    file::META_STREAM,
    meta,
    pump::pump,
    queue::{Record, Tx},
};

mod http;

fn handle(
    writer_tx: &Tx<Record>,
    recv_time: DateTime<Utc>,
    data: Utf8Bytes,
) -> Result<(), ConnectorError> {
    let j: serde_json::Value = serde_json::from_str(data.as_str())?;
    // The collector's own lifecycle records travel this hop alongside the
    // venue's frames (see `meta.rs`). Matched before anything else, so one can
    // never reach the symbol routing below, whatever it happens to contain.
    if meta::is_record(&j) {
        writer_tx.send((recv_time, META_STREAM.to_string(), data.to_string()))?;
        return Ok(());
    }
    if let Some(j_topic) = j.get("topic") {
        let topic = j_topic.as_str().ok_or(ConnectorError::FormatError)?;
        let symbol = topic.split(".").last().ok_or(ConnectorError::FormatError)?;
        writer_tx.send((recv_time, symbol.to_string(), data.to_string()))?;
    } else if let Some(j_success) = j.get("success") {
        // Record the subscribe ack, successful or not. This is the frame that
        // says which topics the venue actually accepted; without it in the
        // file, a recording that captured nothing because a topic was rejected
        // looks exactly like a recording of a silent market.
        writer_tx.send((recv_time, META_STREAM.to_string(), data.to_string()))?;

        let success = j_success.as_bool().ok_or(ConnectorError::FormatError)?;
        if !success {
            error!(%data, "couldn't subscribe the topics.");
            return Err(ConnectorError::ConnectionAbort);
        }
    }
    Ok(())
}

pub async fn run_collection(
    topics: Vec<String>,
    symbols: Vec<String>,
    writer_tx: Tx<Record>,
) -> Result<(), anyhow::Error> {
    pump(
        writer_tx,
        |ws_tx| keep_connection(topics, symbols, ws_tx),
        handle,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::{self, WRITER_HOP};

    /// A record the writer cannot take must be an error, not a discarded frame.
    /// Bybit's `handle` writes both market data and the subscribe ack, and the
    /// ack is the frame that explains an empty recording — losing it in silence
    /// is how a rejected subscription came to look like a quiet market.
    #[test]
    fn a_frame_the_writer_cannot_take_is_an_error_not_a_drop() {
        let (tx, _rx, _fatal) = queue::test_bounded::<Record>(WRITER_HOP, 1);
        let now = Utc::now();
        let frame = r#"{"topic":"orderbook.50.BTCUSDT","type":"delta","data":{}}"#;

        handle(&tx, now, frame.into()).expect("the first frame fits");

        let error = handle(&tx, now, frame.into())
            .expect_err("a frame that could not be handed over must not be reported as written");
        assert!(matches!(error, ConnectorError::Queue(_)), "{error}");
    }

    /// The collector's own lifecycle records ride the same hop as the venue's
    /// frames, so `handle` is both what files them in the sidecar and what
    /// keeps them out of the symbol files. Bybit used to forward only what the
    /// venue chose to say, which left every gap the venue was silent about —
    /// every reconnect — unexplained.
    #[test]
    fn lifecycle_records_are_filed_under_meta_and_market_data_still_is_not() {
        let (tx, mut rx, _fatal) = queue::test_bounded::<Record>(WRITER_HOP, 4);
        let now = Utc::now();
        let lifecycle = meta::connected("wss://stream.bybit.com/v5/public/linear");

        handle(&tx, now, lifecycle.to_string().as_str().into()).unwrap();
        handle(
            &tx,
            now,
            r#"{"topic":"orderbook.50.BTCUSDT","type":"delta","data":{}}"#.into(),
        )
        .unwrap();

        let (_, stream, payload) = rx.try_recv().expect("the lifecycle record must be written");
        assert_eq!(stream, META_STREAM);
        let j: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(j["_collector"], "connected");
        assert_eq!(j["url"], "wss://stream.bybit.com/v5/public/linear");

        assert_eq!(
            rx.try_recv().expect("market data must still be written").1,
            "BTCUSDT",
            "the symbol routing below the tag check is untouched"
        );
    }
}
