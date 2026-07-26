use std::{
    io,
    io::ErrorKind,
    time::{Duration, Instant},
};

use anyhow::Error;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use tokio::{select, sync::mpsc::unbounded_channel, time::interval};
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

/// The combined-stream endpoint; the streams themselves go in the query.
const WS_URL: &str = "wss://stream.binance.com:9443/stream";

pub async fn fetch_symbol_list() -> Result<Vec<String>, reqwest::Error> {
    // `exchangeInfo` is a multi-megabyte response fetched once, so this is
    // generous — but bounded, because reqwest applies no timeout by default
    // and a hung connect would otherwise wait for ever.
    const EXCHANGE_INFO_TIMEOUT: Duration = Duration::from_secs(30);

    Ok(reqwest::Client::builder()
        .timeout(EXCHANGE_INFO_TIMEOUT)
        .build()?
        .get("https://api.binance.com/api/v3/exchangeInfo")
        .header("Accept", "application/json")
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?
        .get("symbols")
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .map(|j_symbol| {
            j_symbol
                .get("symbol")
                .unwrap()
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect())
}

pub async fn fetch_depth_snapshot(symbol: &str) -> Result<String, reqwest::Error> {
    // A depth snapshot is fetched to repair a gap in the incremental feed and
    // is worthless once stale; failing fast also frees the throttler slot.
    const DEPTH_SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(10);

    reqwest::Client::builder()
        .timeout(DEPTH_SNAPSHOT_TIMEOUT)
        .build()?
        .get(format!(
            "https://api.binance.com/api/v3/depth?symbol={symbol}&limit=1000"
        ))
        .header("Accept", "application/json")
        .send()
        .await?
        .text()
        .await
}

/// `connected_at` is set the moment the socket comes up, and stays `None` if
/// the dial itself fails. It is an out-parameter because the dial failure
/// leaves through `?`, and the caller has to be able to tell a connection that
/// dropped from one that never existed.
pub async fn connect(
    url: &str,
    ws_tx: Tx<Frame>,
    connected_at: &mut Option<Instant>,
) -> Result<StreamEnd, anyhow::Error> {
    let request = url.into_client_request()?;
    let (ws_stream, _) = connect_async(request).await?;
    *connected_at = Some(Instant::now());
    meta::emit(&ws_tx, meta::connected(url));
    let (mut write, mut read) = ws_stream.split();
    let (tx, mut rx) = unbounded_channel::<Bytes>();

    tokio::spawn(async move {
        while let Some(data) = rx.recv().await {
            if write.send(Message::Pong(data)).await.is_err() {
                let _ = write.close().await;
                return;
            }
        }
    });

    let mut last_ping = Instant::now();
    let mut checker = interval(Duration::from_secs(5));

    loop {
        select! {
            msg = read.next() => match msg {
                Some(Ok(Message::Text(text))) => {
                    let recv_time = Utc::now();
                    // A refused hand-off is terminal, whether the parser has
                    // gone or has simply stopped draining: `send` has already
                    // raised the fatal signal, and reading on would drop
                    // frames in silence. Returning also ends the retry loop in
                    // `keep_connection`, releasing `ws_tx` and unwinding the
                    // collection task behind it.
                    if ws_tx.send((recv_time, text)).is_err() {
                        return Ok(StreamEnd::HandOffRefused);
                    }
                }
                Some(Ok(Message::Binary(_))) => {}
                Some(Ok(Message::Ping(data))) => {
                    if tx.send(data).is_err() {
                        return Err(Error::from(io::Error::new(
                            ErrorKind::ConnectionAborted,
                            "closed",
                        )));
                    }
                    last_ping = Instant::now();
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
            },
            _ = checker.tick() => {
                if last_ping.elapsed() > Duration::from_secs(30) {
                    warn!("Ping timeout.");
                    return Err(Error::from(io::Error::new(
                        ErrorKind::TimedOut,
                        "Ping",
                    )));
                }
            }
        }
    }
    Ok(StreamEnd::Eof)
}

pub async fn keep_connection(streams: Vec<String>, symbol_list: Vec<String>, ws_tx: Tx<Frame>) {
    let mut error_count: u32 = 0;
    let mut attempt: u64 = 0;
    loop {
        let streams_ = symbol_list
            .iter()
            .flat_map(|pair| {
                streams
                    .iter()
                    .cloned()
                    .map(|stream| {
                        stream
                            .replace("$symbol", pair.to_lowercase().as_str())
                            .to_string()
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let url = format!("{WS_URL}?streams={}", streams_.join("/"));

        // Binance subscribes through the URL rather than a subscribe frame, so
        // the venue never acks anything and this is the only account of what
        // was asked for. Written before the dial, because the dial that never
        // completes is the case it is most needed for.
        meta::emit(
            &ws_tx,
            meta::subscribe(&url, attempt, serde_json::json!(&streams_)),
        );
        attempt += 1;

        // Started here, not at the top of the loop: the dial must not be
        // charged for the time spent building the stream list. The
        // stale-error-count reset below reads better for the same reason.
        let dial_time = Instant::now();
        let mut connected_at = None;

        match connect(&url, ws_tx.clone(), &mut connected_at).await {
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
