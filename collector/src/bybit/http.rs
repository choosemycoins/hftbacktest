use std::{
    io,
    io::ErrorKind,
    time::{Duration, Instant},
};

use anyhow::Error;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use tokio::{select, sync::mpsc::unbounded_channel};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Bytes, Message, client::IntoClientRequest},
};
use tracing::{error, warn};

use crate::{
    backoff::reconnect_delay,
    meta::{self, StreamEnd},
    queue::{Frame, Tx},
};

/// The public linear perpetuals endpoint.
const WS_URL: &str = "wss://stream.bybit.com/v5/public/linear";

/// `connected_at` is set the moment the socket comes up, and stays `None` if
/// the dial itself fails. It is an out-parameter because the dial failure
/// leaves through `?`, and the caller has to be able to tell a connection that
/// dropped from one that never existed.
pub async fn connect(
    url: &str,
    topics: Vec<String>,
    ws_tx: Tx<Frame>,
    connected_at: &mut Option<Instant>,
) -> Result<StreamEnd, anyhow::Error> {
    let request = url.into_client_request()?;
    let (ws_stream, _) = connect_async(request).await?;
    *connected_at = Some(Instant::now());
    meta::emit(&ws_tx, meta::connected(url));
    let (mut write, mut read) = ws_stream.split();
    let (tx, mut rx) = unbounded_channel::<()>();

    write
        .send(Message::Text(
            format!(
                r#"{{"req_id": "subscribe", "op": "subscribe", "args": [{}]}}"#,
                topics
                    .iter()
                    .map(|s| format!("\"{s}\""))
                    .collect::<Vec<_>>()
                    .join(",")
            )
            .into(),
        ))
        .await?;

    tokio::spawn(async move {
        let mut ping_interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            select! {
                result = rx.recv() => {
                    match result {
                        Some(_) => {
                            if write.send(Message::Pong(Bytes::default())).await.is_err() {
                                return;
                            }
                        }
                        None => {
                            break;
                        }
                    }
                }
                _ = ping_interval.tick() => {
                    if write.send(
                        Message::Text(r#"{"req_id": "ping", "op": "ping"}"#.into())
                    ).await.is_err() {
                        return;
                    }
                }
            }
        }
    });

    loop {
        match read.next().await {
            Some(Ok(Message::Text(text))) => {
                let recv_time = Utc::now();
                // A refused hand-off is terminal, whether the parser has gone
                // or has simply stopped draining: `send` has already raised
                // the fatal signal, and reading on would drop frames in
                // silence. Returning also ends the retry loop in
                // `keep_connection`, releasing `ws_tx` and unwinding the
                // collection task behind it.
                if ws_tx.send((recv_time, text)).is_err() {
                    return Ok(StreamEnd::HandOffRefused);
                }
            }
            Some(Ok(Message::Binary(_))) => {}
            Some(Ok(Message::Ping(_))) => {
                tx.send(()).unwrap();
            }
            Some(Ok(Message::Pong(_))) => {}
            Some(Ok(Message::Close(close_frame))) => {
                warn!(?close_frame, "closed");
                return Err(Error::from(io::Error::new(
                    ErrorKind::ConnectionAborted,
                    "closed",
                )));
            }
            Some(Ok(Message::Frame(_))) => {}
            Some(Err(e)) => {
                return Err(Error::from(e));
            }
            None => {
                break;
            }
        }
    }
    Ok(StreamEnd::Eof)
}

pub async fn keep_connection(topics: Vec<String>, symbol_list: Vec<String>, ws_tx: Tx<Frame>) {
    let mut error_count: u32 = 0;
    let mut attempt: u64 = 0;
    loop {
        let topics_ = symbol_list
            .iter()
            .flat_map(|pair| {
                topics
                    .iter()
                    .cloned()
                    .map(|stream| {
                        stream
                            .replace("$symbol", pair.to_uppercase().as_str())
                            .to_string()
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        // What was asked for. The venue's own ack says which topics it
        // accepted, but only if it answers at all — and Bybit rejects the whole
        // batch over a single unknown topic, so the requested set is exactly
        // what one needs to see next to the rejection.
        meta::emit(
            &ws_tx,
            meta::subscribe(WS_URL, attempt, serde_json::json!(&topics_)),
        );
        attempt += 1;

        // Started here, not at the top of the loop: the dial must not be
        // charged for the time spent building the topic list. The
        // stale-error-count reset below reads better for the same reason.
        let dial_time = Instant::now();
        let mut connected_at = None;

        match connect(WS_URL, topics_, ws_tx.clone(), &mut connected_at).await {
            Err(error) => {
                error!(?error, "websocket error");
                // A disconnect is otherwise indistinguishable from a quiet
                // market: the file just stops for a couple of seconds. A dial
                // that never came up is a different event, because there is no
                // time-connected to report for a connection that never existed.
                meta::emit(
                    &ws_tx,
                    match connected_at {
                        Some(at) => {
                            meta::disconnected(&error.to_string(), at.elapsed().as_millis() as u64)
                        }
                        None => meta::dial_failed(
                            &error.to_string(),
                            dial_time.elapsed().as_millis() as u64,
                        ),
                    },
                );
                error_count += 1;
                if dial_time.elapsed() > Duration::from_secs(30) {
                    error_count = 0;
                }
                tokio::time::sleep(reconnect_delay(error_count)).await;
            }
            Ok(end) => {
                let connected_for = connected_at.map_or(0, |at| at.elapsed().as_millis() as u64);
                if let Some(record) = meta::end_of_stream(end, connected_for) {
                    meta::emit(&ws_tx, record);
                }
                break;
            }
        }
    }
}
