use chrono::{DateTime, Utc};
use tokio_tungstenite::tungstenite::Utf8Bytes;
use tracing::error;

use self::http::keep_connection;
use crate::{
    error::ConnectorError,
    file::META_STREAM,
    queue::{self, QUEUE_CAPACITY, Record, Tx, WS_HOP},
};

mod http;

fn handle(
    writer_tx: &Tx<Record>,
    recv_time: DateTime<Utc>,
    data: Utf8Bytes,
) -> Result<(), ConnectorError> {
    let j: serde_json::Value = serde_json::from_str(data.as_str())?;
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
    let (ws_tx, mut ws_rx) = queue::bounded(WS_HOP, QUEUE_CAPACITY, writer_tx.fatal());
    // By value, not a clone: the producer must hold the only sender, so that
    // its death closes the channel and ends this loop. See the pump test in
    // `hyperliquid/mod.rs` for what a retained clone costs.
    let h = tokio::spawn(keep_connection(topics, symbols, ws_tx));
    while let Some((recv_time, data)) = ws_rx.recv().await {
        match handle(&writer_tx, recv_time, data) {
            Ok(()) => {}
            // A rejected subscribe is fatal, not transient. Bybit fails the
            // whole batch if any one topic is unknown (e.g. `orderbook.500`
            // for a symbol that does not offer that depth), and retrying the
            // identical batch can never succeed. Merely logging it — as this
            // loop used to — left the socket open and ping-ponging with no
            // subscription at all: zero bytes recorded, indefinitely, while
            // the process looked perfectly healthy to systemd.
            //
            // A refused hand-off is fatal for the same reason from the other
            // end: the writer has stopped taking records, so carrying on would
            // discard market data in silence.
            Err(error @ (ConnectorError::ConnectionAbort | ConnectorError::Queue(_))) => {
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
}
