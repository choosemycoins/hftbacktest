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

/// Opens the socket, sends every subscribe frame, and forwards each text frame
/// to `ws_tx` tagged with its local receive time. Mirrors the Hyperliquid
/// backend: same JSON-over-WS shape, same "a refused hand-off is terminal"
/// contract. `connected_at` is the out-parameter that lets the caller tell a
/// dropped connection from one that never dialled.
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

    // A WebSocket-level Ping, not a JSON-RPC one: it is accepted by every
    // compliant server and cannot be mistaken by the venue for a malformed
    // subscribe method, which — as on Hyperliquid — would close the whole
    // socket. The pong (if any) is a `Message::Pong`, filed under meta below.
    tokio::spawn(async move {
        let mut ping_interval = tokio::time::interval(Duration::from_secs(30));
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
        match read.next().await {
            Some(Ok(Message::Text(text))) => {
                let recv_time = Utc::now();
                // A refused hand-off is terminal (parser gone or not draining):
                // reading on would drop frames in silence. `send` already raised
                // the fatal signal; returning unwinds the collection task.
                if ws_tx.send((recv_time, text)).is_err() {
                    return Ok(StreamEnd::HandOffRefused);
                }
            }
            Some(Ok(Message::Binary(_))) => {}
            Some(Ok(Message::Ping(_))) => {}
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

/// The subscribe frames for one connection: one JSON-RPC `subscribe` per
/// (market, channel-template) pair, each with a monotone `id`.
///
/// Split out so the wire shape can be asserted without a socket — this is the
/// one thing no log line would ever contradict. Paradex answers a bad channel
/// with a JSON-RPC error echoing the request `id` (it does NOT close the socket
/// the way Hyperliquid does), so an unrecognised subscribe is recoverable and
/// attributable; but a template typo that the venue silently never serves would
/// still read downstream as a market that was simply quiet — hence the test.
pub fn subscription_frames(
    channel_templates: &[&str],
    markets: &[String],
) -> Vec<serde_json::Value> {
    let mut id: u64 = 1;
    let mut frames = Vec::with_capacity(markets.len() * channel_templates.len());
    for market in markets {
        for template in channel_templates {
            let channel = template.replace("{market}", market);
            frames.push(serde_json::json!({
                "jsonrpc": "2.0",
                "method": "subscribe",
                "params": { "channel": channel },
                "id": id,
            }));
            id += 1;
        }
    }
    frames
}

pub async fn keep_connection(
    channel_templates: Vec<&'static str>,
    markets: Vec<String>,
    ws_tx: Tx<Frame>,
) {
    let mut error_count: u32 = 0;
    let mut attempt: u64 = 0;
    loop {
        let subscriptions = subscription_frames(&channel_templates, &markets);

        info!(
            "Connecting to Paradex WebSocket with {} subscriptions",
            subscriptions.len()
        );

        // Record the exact subscription set: the order_book feed_type
        // (`snapshot` vs the RPI-inclusive `interactive`) and the @depth@refresh
        // suffix are the whole reason this backend exists, and no other record
        // says which was asked for.
        meta::emit(
            &ws_tx,
            meta::subscribe(WS_URL, attempt, serde_json::json!(&subscriptions)),
        );
        attempt += 1;

        let dial_time = Instant::now();
        let mut connected_at = None;

        match connect(WS_URL, subscriptions, ws_tx.clone(), &mut connected_at).await {
            Err(error) => {
                error!(?error, "websocket error");
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::paradex::CHANNELS;

    fn markets(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    /// One subscribe frame per market per channel template, each a JSON-RPC
    /// `subscribe` with a unique `id`, and the market substituted into the
    /// channel string. A missing pairing is invisible at runtime — the venue
    /// serves what it was asked and says nothing about the rest.
    #[test]
    fn every_market_gets_every_channel_with_a_unique_id() {
        let ms = markets(&["BTC-USD-PERP", "ETH-USD-PERP"]);
        let frames = subscription_frames(&CHANNELS, &ms);

        assert_eq!(frames.len(), CHANNELS.len() * ms.len());
        let ids: std::collections::HashSet<_> =
            frames.iter().map(|f| f["id"].as_u64().unwrap()).collect();
        assert_eq!(ids.len(), frames.len(), "ids must be unique");
        for m in &ms {
            for template in CHANNELS {
                let channel = template.replace("{market}", m);
                assert!(
                    frames.iter().any(|f| {
                        f["method"] == "subscribe" && f["params"]["channel"] == channel.as_str()
                    }),
                    "{m} was never subscribed to {channel}"
                );
            }
        }
    }

    /// Both order_book books are recorded — the plain `snapshot` API book and
    /// the RPI-inclusive `interactive` book. They are different books (the whole
    /// point of collecting Paradex for the fill-uninformed-flow thesis), so a
    /// template that dropped one would silently discard the reason we came.
    #[test]
    fn both_the_snapshot_and_the_interactive_book_are_subscribed() {
        let frames = subscription_frames(&CHANNELS, &markets(&["BTC-USD-PERP"]));
        let channels: Vec<&str> = frames
            .iter()
            .map(|f| f["params"]["channel"].as_str().unwrap())
            .collect();
        assert!(channels.contains(&"order_book.BTC-USD-PERP.snapshot@15@100ms"));
        assert!(channels.contains(&"order_book.BTC-USD-PERP.interactive@15@100ms"));
        assert!(channels.contains(&"bbo.BTC-USD-PERP"));
        assert!(channels.contains(&"trades.BTC-USD-PERP"));
        assert!(channels.contains(&"funding_data.BTC-USD-PERP"));
    }
}
