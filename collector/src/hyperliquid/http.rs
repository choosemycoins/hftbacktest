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
    tungstenite::{Message, Utf8Bytes, client::IntoClientRequest},
};
use tracing::{error, info, warn};

use super::WS_URL;
use crate::{
    backoff::reconnect_delay,
    queue::{Frame, Tx},
};

/// Injects a synthetic record into the same stream the venue's frames travel
/// on, so it is timestamped, ordered and stored exactly like real data. The
/// `_collector` key marks it as ours; `route` in mod.rs sends anything
/// carrying that key to the meta stream.
fn emit(ws_tx: &Tx<Frame>, value: serde_json::Value) {
    // The caller is the reconnect loop, which has no error path of its own.
    // `send` has already raised the fatal signal, so the process is on its way
    // down either way; logging is all that is left to add here.
    if let Err(error) = ws_tx.send((Utc::now(), Utf8Bytes::from(value.to_string()))) {
        error!(?error, "couldn't record a collector lifecycle event");
    }
}

pub async fn connect(
    url: &str,
    subscriptions: Vec<serde_json::Value>,
    ws_tx: Tx<Frame>,
) -> Result<(), anyhow::Error> {
    let request = url.into_client_request()?;
    let (ws_stream, _) = connect_async(request).await?;
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
                    break;
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
    Ok(())
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

        // Record the exact subscription set, including the `fast` flags, into
        // the meta stream. Without it a consumer has to infer what was
        // recorded from what happens to be present, which is wrong whenever a
        // subscribed feed was simply silent.
        emit(
            &ws_tx,
            serde_json::json!({
                "_collector": "subscribe",
                "url": WS_URL,
                "attempt": attempt,
                "subscriptions": &subscriptions,
            }),
        );
        attempt += 1;

        // Started here, not at the top of the loop: `connected_for_ms` must
        // measure the connection, not the connection plus the time spent
        // building the subscription set and writing the subscribe record.
        // The stale-error-count reset below reads better for the same reason.
        let connect_time = Instant::now();

        if let Err(error) = connect(WS_URL, subscriptions, ws_tx.clone()).await {
            error!(?error, "websocket error");
            // A disconnect is otherwise indistinguishable from a quiet market:
            // the file just stops for a couple of seconds.
            emit(
                &ws_tx,
                serde_json::json!({
                    "_collector": "disconnected",
                    "error": error.to_string(),
                    "connected_for_ms": connect_time.elapsed().as_millis() as u64,
                }),
            );
            error_count += 1;
            if connect_time.elapsed() > Duration::from_secs(30) {
                error_count = 0;
            }

            tokio::time::sleep(reconnect_delay(error_count)).await;
        } else {
            emit(
                &ws_tx,
                serde_json::json!({
                    "_collector": "stream_ended",
                    "connected_for_ms": connect_time.elapsed().as_millis() as u64,
                }),
            );
            break;
        }
    }
}
