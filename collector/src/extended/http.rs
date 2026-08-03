use std::{
    io,
    io::ErrorKind,
    time::{Duration, Instant},
};

use anyhow::Error;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use tokio::{
    select,
    sync::mpsc::unbounded_channel,
    time::{Interval, MissedTickBehavior},
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        Message,
        client::IntoClientRequest,
        http::{HeaderValue, header::USER_AGENT},
    },
};
use tracing::{error, info, warn};

use super::SocketLiveness;
use crate::{
    backoff::reconnect_delay,
    meta::{self, StreamEnd},
    queue::{Frame, Tx},
};

/// Opens one channel socket and forwards each text frame to `ws_tx` tagged with
/// its local receive time.
///
/// Extended has no subscribe frames: the URL path *is* the subscription, so
/// this only sets the required `User-Agent` and reads. `connected_at` is the
/// out-parameter that lets the caller tell a dropped connection from one that
/// never dialled — the dial failure leaves through `?`.
///
/// `liveness` and `minute` are owned by the caller so they persist across
/// reconnects (see [`keep_connection_one`]): every text frame notes itself on
/// the gauge, and on each minute tick the gauge's record is emitted and a socket
/// that has never served a frame is warned about.
pub async fn connect(
    url: &str,
    ws_tx: Tx<Frame>,
    connected_at: &mut Option<Instant>,
    liveness: &mut SocketLiveness,
    minute: &mut Interval,
) -> Result<StreamEnd, anyhow::Error> {
    let mut request = url.into_client_request()?;
    // Required by the venue; the value is otherwise unconstrained. Without it
    // the upgrade is refused.
    request
        .headers_mut()
        .insert(USER_AGENT, HeaderValue::from_static(super::USER_AGENT));

    let (ws_stream, _) = connect_async(request).await?;
    *connected_at = Some(Instant::now());
    meta::emit(&ws_tx, meta::connected(url));
    let (mut write, mut read) = ws_stream.split();
    let (_ping_tx, mut ping_rx) = unbounded_channel::<()>();

    // A client-side WebSocket ping. The venue pings us every 15s and
    // tokio-tungstenite auto-pongs those, so this is not required for the venue;
    // it is here so a half-open socket that has gone quiet is torn down by the
    // send failing, rather than sitting silent until the process-level stall
    // watchdog notices minutes later.
    tokio::spawn(async move {
        let mut ping_interval = tokio::time::interval(Duration::from_secs(15));
        loop {
            select! {
                _ = ping_interval.tick() => {
                    if write.send(Message::Ping(Vec::new().into())).await.is_err() {
                        return;
                    }
                }
                result = ping_rx.recv() => {
                    if result.is_none() {
                        break;
                    }
                }
            }
        }
    });

    loop {
        select! {
            // The per-socket liveness sample, on the same one-a-minute cadence
            // as `main`'s host gauges. `read.next()` is cancel-safe — a frame in
            // the socket buffer is not consumed until it is returned — so losing
            // this race to the tick drops no data.
            _ = minute.tick() => {
                let (record, never_served) = liveness.sample(tokio::time::Instant::now());
                meta::emit(&ws_tx, record);
                if never_served {
                    warn!(
                        %url,
                        "extended socket has served no frame yet; the venue may be \
                         silently not feeding this URL (the dial succeeded)"
                    );
                }
            }
            message = read.next() => match message {
                Some(Ok(Message::Text(text))) => {
                    liveness.saw_frame(tokio::time::Instant::now());
                    let recv_time = Utc::now();
                    // A refused hand-off is terminal (parser gone or not
                    // draining): `send` has already raised the fatal signal, and
                    // reading on would drop frames in silence. Returning ends the
                    // retry loop, releasing this socket's `ws_tx` clone.
                    if ws_tx.send((recv_time, text)).is_err() {
                        return Ok(StreamEnd::HandOffRefused);
                    }
                }
                Some(Ok(Message::Binary(_))) => {}
                // The venue's own pings; tokio-tungstenite has already ponged.
                Some(Ok(Message::Ping(_))) => {}
                Some(Ok(Message::Pong(_))) => {}
                Some(Ok(Message::Close(close_frame))) => {
                    warn!(?close_frame, %url, "connection closed");
                    return Err(Error::from(io::Error::new(
                        ErrorKind::ConnectionAborted,
                        "connection closed",
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
    }
    Ok(StreamEnd::Eof)
}

/// Keeps one channel socket alive for the life of the collection.
///
/// Reconnect is "reopen the URL" — there is nothing to resubscribe. Unlike the
/// single-socket backends, a clean close (`Eof`) is **reconnected**, not
/// treated as the end of the collection: each Extended channel is one socket of
/// several, and ending the whole recording because one channel's socket was
/// cycled would be wrong. The only terminal exit is `HandOffRefused`, which
/// means the parser is gone and the process is already coming down.
///
/// The full order book re-snapshots every ~60s and every delta carries an
/// absolute size (`c`), so a dropped frame self-heals within a minute without a
/// reconnect; this loop therefore reconnects on transport loss only, and the
/// per-frame `seq` is recorded raw for the offline layer to check. (Spec calls
/// for reconnect on a `seq` gap as well; that is deliberately deferred — on the
/// all-markets firehose one dropped frame among hundreds of markets would
/// trigger a full-venue re-snapshot, and the self-healing above makes the cost
/// not worth paying until gap frequency is measured live.)
async fn keep_connection_one(url: String, ws_tx: Tx<Frame>) {
    let mut error_count: u32 = 0;
    let mut attempt: u64 = 0;

    // One gauge and one timer for the whole life of this socket, owned here so
    // they persist across reconnects: a socket that keeps reconnecting without
    // ever serving a frame still accumulates the "never served" verdict, and a
    // reconnect does not restart the minute clock. The interval's first tick is
    // immediate, so it is consumed now — the first real sample is a minute in,
    // not at t=0 when nothing could have arrived. `Delay` keeps a long backoff
    // from firing a burst of catch-up ticks on reconnect.
    let mut liveness = SocketLiveness::new(url.clone(), tokio::time::Instant::now());
    let mut minute = tokio::time::interval(Duration::from_secs(60));
    minute.set_missed_tick_behavior(MissedTickBehavior::Delay);
    minute.tick().await;

    loop {
        // What was opened, recorded before the dial: the case it is most needed
        // for is the dial that never completes.
        meta::emit(
            &ws_tx,
            meta::subscribe(&url, attempt, serde_json::json!([&url])),
        );
        attempt += 1;

        let dial_time = Instant::now();
        let mut connected_at = None;

        match connect(
            &url,
            ws_tx.clone(),
            &mut connected_at,
            &mut liveness,
            &mut minute,
        )
        .await
        {
            Ok(StreamEnd::HandOffRefused) => return,
            Ok(StreamEnd::Eof) => {
                // A clean close. For a persistent per-channel socket this is a
                // reconnect, not the end — recorded as a disconnect so a gap in
                // the file is explained.
                let for_ms = connected_at.map_or(0, |at| at.elapsed().as_millis() as u64);
                meta::emit(
                    &ws_tx,
                    meta::disconnected("stream closed by venue (eof)", for_ms),
                );
                error_count += 1;
                if dial_time.elapsed() > Duration::from_secs(30) {
                    error_count = 0;
                }
                tokio::time::sleep(reconnect_delay(error_count)).await;
            }
            Err(error) => {
                error!(%url, ?error, "websocket error");
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
        }
    }
}

/// Runs every channel socket for a pump, each with its own reconnect loop, all
/// feeding the one parser hop.
///
/// Takes `ws_tx` by value and drops its own clone before awaiting the sockets:
/// `pump` recognises a finished producer by every sender being gone (`main`
/// then sees the writer channel close), so a clone retained here would keep the
/// collection task alive forever after the sockets had all stopped. With an
/// empty `urls` — the RFQ pump when no RFQ market is requested — nothing is
/// spawned, the sender is dropped at once, and the pump returns immediately.
pub async fn keep_connections(urls: Vec<String>, ws_tx: Tx<Frame>) {
    let mut handles = Vec::with_capacity(urls.len());
    for url in urls {
        info!(%url, "opening extended channel socket");
        handles.push(tokio::spawn(keep_connection_one(url, ws_tx.clone())));
    }
    // See the doc comment: our own clone must go before we wait on the sockets.
    drop(ws_tx);
    for handle in handles {
        let _ = handle.await;
    }
}
