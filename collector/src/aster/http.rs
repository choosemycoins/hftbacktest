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
///
/// Aster is a Binance-USD-M-compatible fork but does NOT publish the routed
/// classes on 2026-03-06 — `/public` (bookTicker, depth), `/market`
/// (aggTrade, markPrice, kline, ...), `/private` (user data) — and an
/// UNROUTED connection has been a degraded alias of `/public` since the
/// legacy decommission date, 2026-04-23: subscriptions to another class are
/// acked and then never served, with no error anywhere. That is exactly how
/// the `@markPrice@1s` entry sat silent in this backend's stream list
/// (measured 2026-07-28; see the note on [`super::STREAMS`]). The legacy
/// alias itself lives on borrowed time, so this dials the routed path our
/// stream classes actually belong to.
const WS_URL: &str = "wss://fstream.asterdex.com/stream";

/// Builds the combined-stream dial URL. Extracted so the routed path is
/// pinned by a test — an unrouted URL fails exactly like a typo'd stream
/// name: the venue acks it and serves nothing.
pub(crate) fn ws_url(streams: &[String]) -> String {
    format!("{WS_URL}?streams={}", streams.join("/"))
}

#[cfg(test)]
mod url_tests {
    use super::ws_url;

    #[test]
    fn the_dial_url_uses_the_routed_public_path() {
        let url = ws_url(&["btcusdt@trade".to_string(), "btcusdt@depth@0ms".to_string()]);
        assert!(
            url.starts_with("wss://fstream.asterdex.com/stream?streams="),
            "unrouted fstream URLs are a degraded legacy alias of /public and \
             silently drop every non-public stream class; got {url}"
        );
        assert!(url.ends_with("btcusdt@trade/btcusdt@depth@0ms"));
    }
}

pub async fn fetch_symbol_list() -> Result<Vec<String>, reqwest::Error> {
    // `exchangeInfo` is a multi-megabyte response fetched once, so this is
    // generous — but bounded, because reqwest applies no timeout by default
    // and a hung connect would otherwise wait for ever.
    const EXCHANGE_INFO_TIMEOUT: Duration = Duration::from_secs(30);

    Ok(reqwest::Client::builder()
        .timeout(EXCHANGE_INFO_TIMEOUT)
        .build()?
        .get("https://fapi.asterdex.com/fapi/v1/exchangeInfo")
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
        .filter(|j_symbol| j_symbol.get("contractType").unwrap().as_str().unwrap() == "PERPETUAL")
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
            "https://fapi.asterdex.com/fapi/v1/depth?symbol={symbol}&limit=1000"
        ))
        .header("Accept", "application/json")
        .send()
        .await?
        .text()
        .await
}

/// Fetches the venue's premium-index snapshot for **every** symbol at once.
///
/// No `symbol` parameter on purpose: the endpoint then answers one array
/// covering the whole venue (851 elements, 188 KB — measured 2026-07-28), and
/// the caller keeps the ones it records. That is one request per cycle however
/// many symbols an instance carries, against `symbol`-at-a-time's one request
/// each. Binance weighs the unfiltered call at 10 and the filtered one at 1, so
/// the crossover is ten symbols; below that this trades a little weight for a
/// constant request rate, which is the thing a rate limit actually punishes.
///
/// Returns the body as text, not as a parsed document: the caller writes the
/// venue's own bytes into the recording and must not re-encode them on the way.
pub async fn fetch_premium_index() -> Result<String, reqwest::Error> {
    // A premium-index sample is superseded by the next one, so a request still
    // outstanding when the next poll is due has nothing left to contribute —
    // hence the timeout IS the period. Failing at that point also keeps the
    // consecutive-failure counter honest: one skipped cycle, one failure.
    const PREMIUM_INDEX_TIMEOUT: Duration = super::PREMIUM_INDEX_INTERVAL;

    reqwest::Client::builder()
        .timeout(PREMIUM_INDEX_TIMEOUT)
        .build()?
        .get("https://fapi.asterdex.com/fapi/v1/premiumIndex")
        .header("Accept", "application/json")
        .send()
        .await?
        .error_for_status()?
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
    let mut checker = interval(Duration::from_secs(10));

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
                if last_ping.elapsed() > Duration::from_secs(300) {
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
        let url = ws_url(&streams_);

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
