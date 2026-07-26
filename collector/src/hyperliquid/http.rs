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
    tungstenite::{Message, client::IntoClientRequest},
};
use tracing::{error, info, warn};

use super::WS_URL;
use crate::{
    backoff::reconnect_delay,
    meta::{self, StreamEnd},
    queue::{Frame, Tx},
};

/// `connected_at` is set the moment the socket comes up, and stays `None` if
/// the dial itself fails. It is an out-parameter because the dial failure
/// leaves through `?`, and the caller has to be able to tell a connection that
/// dropped from one that never existed.
pub async fn connect(
    url: &str,
    subscriptions: Vec<serde_json::Value>,
    ws_tx: Tx<Frame>,
    connected_at: &mut Option<Instant>,
) -> Result<StreamEnd, anyhow::Error> {
    let request = url.into_client_request()?;
    let (ws_stream, _) = connect_async(request).await?;
    *connected_at = Some(Instant::now());
    meta::emit(&ws_tx, meta::connected(url));
    let (mut write, mut read) = ws_stream.split();
    let (_ping_tx, mut ping_rx) = unbounded_channel::<()>();

    for subscription in subscriptions {
        write
            .send(Message::Text(subscription.to_string().into()))
            .await?;
    }

    tokio::spawn(async move {
        let mut ping_interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            select! {
                _ = ping_interval.tick() => {
                    if write.send(Message::Text(r#"{"method":"ping"}"#.into())).await.is_err() {
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
        match read.next().await {
            Some(Ok(Message::Text(text))) => {
                let recv_time = Utc::now();

                // Pongs used to be filtered out here. They are kept now: a pong
                // is the only positive evidence the socket was alive during a
                // stretch with no market data, which is exactly what separates
                // a quiet market from a half-open connection when reading the
                // meta stream afterwards. `route` files them under META_STREAM.
                //
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
                // Hyperliquid uses JSON ping/pong, not WebSocket ping/pong
            }
            Some(Ok(Message::Pong(_))) => {}
            Some(Ok(Message::Close(close_frame))) => {
                warn!(?close_frame, "connection closed");
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
    Ok(StreamEnd::Eof)
}

pub async fn keep_connection(
    subscription_types: Vec<super::SubscriptionSpec>,
    symbol_list: Vec<String>,
    ws_tx: Tx<Frame>,
) {
    let mut error_count: u32 = 0;
    let mut attempt: u64 = 0;
    loop {
        let subscriptions: Vec<serde_json::Value> = symbol_list
            .iter()
            .flat_map(|symbol| {
                subscription_types.iter().map(move |spec| {
                    let mut sub = serde_json::Map::new();
                    sub.insert("type".into(), serde_json::Value::from(spec.kind.as_str()));
                    sub.insert("coin".into(), serde_json::Value::from(symbol.as_str()));
                    // Omit `fast` entirely rather than sending `false`: the two
                    // are equivalent for the venue, but an omitted field keeps
                    // the default subscription byte-identical to what earlier
                    // recordings used, so old and new files stay comparable.
                    if let Some(fast) = spec.fast {
                        sub.insert("fast".into(), serde_json::Value::from(fast));
                    }
                    serde_json::json!({
                        "method": "subscribe",
                        "subscription": serde_json::Value::Object(sub),
                    })
                })
            })
            .collect();

        info!(
            "Connecting to Hyperliquid WebSocket with {} subscriptions",
            subscriptions.len()
        );

        // Record the exact subscription set, including the `fast` flags: the
        // `fast` flag is the difference between a 5-level 0.5s feed and a
        // 20-level 5s one, and no other record says which was asked for.
        meta::emit(
            &ws_tx,
            meta::subscribe(WS_URL, attempt, serde_json::json!(&subscriptions)),
        );
        attempt += 1;

        // Started here, not at the top of the loop: the dial must not be
        // charged for the time spent building the subscription set and writing
        // the subscribe record. The stale-error-count reset below reads better
        // for the same reason.
        let dial_time = Instant::now();
        let mut connected_at = None;

        match connect(WS_URL, subscriptions, ws_tx.clone(), &mut connected_at).await {
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
