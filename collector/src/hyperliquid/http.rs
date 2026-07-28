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

/// The subscribe frames for one connection: the cross product of the requested
/// subscription kinds and the requested coins.
///
/// Split out from [`keep_connection`] so the wire shape can be asserted without
/// a socket. It is the one thing about this backend that no log line and no
/// error would ever contradict — Hyperliquid answers a malformed subscription
/// by closing the entire WebSocket, taking every valid subscription on it with
/// it, and the collector then reconnects for ever recording a trickle.
fn subscription_frames(
    subscription_types: &[super::SubscriptionSpec],
    symbol_list: &[String],
) -> Vec<serde_json::Value> {
    symbol_list
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
        .collect()
}

pub async fn keep_connection(
    subscription_types: Vec<super::SubscriptionSpec>,
    symbol_list: Vec<String>,
    ws_tx: Tx<Frame>,
) {
    let mut error_count: u32 = 0;
    let mut attempt: u64 = 0;
    loop {
        let subscriptions = subscription_frames(&subscription_types, &symbol_list);

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hyperliquid::{ALWAYS_ON, SubscriptionSpec};

    fn coins(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    /// The exact frame mainnet accepted on 2026-07-28 — the ack echoed it back
    /// unchanged and data followed, for the canonical coin and for a HIP-3
    /// builder-dex one alike.
    ///
    /// The dex prefix is the part worth pinning: `xyz:GOLD` is the wire name in
    /// full, so it goes across verbatim rather than being split into a coin and
    /// a dex field. Getting that wrong is not a rejected subscription but a
    /// closed WebSocket, which takes every other coin's subscriptions with it.
    #[test]
    fn the_funding_and_oracle_subscription_goes_out_per_coin_dex_prefix_and_all() {
        let specs = [SubscriptionSpec::plain("activeAssetCtx")];
        let frames = subscription_frames(&specs, &coins(&["BTC", "xyz:GOLD"]));

        assert_eq!(
            frames,
            vec![
                serde_json::json!({
                    "method": "subscribe",
                    "subscription": {"type": "activeAssetCtx", "coin": "BTC"},
                }),
                serde_json::json!({
                    "method": "subscribe",
                    "subscription": {"type": "activeAssetCtx", "coin": "xyz:GOLD"},
                }),
            ]
        );
    }

    /// One subscribe frame per coin per kind, and no coin left without the
    /// always-on set. A missing pairing here is invisible at runtime: the venue
    /// serves the ones it was asked for and says nothing about the rest, which
    /// reads downstream as a coin that was simply quiet.
    #[test]
    fn every_coin_gets_every_always_on_subscription() {
        let specs: Vec<SubscriptionSpec> = ALWAYS_ON
            .iter()
            .map(|k| SubscriptionSpec::plain(k))
            .collect();
        let symbols = coins(&["BTC", "ETH", "xyz:GOLD"]);
        let frames = subscription_frames(&specs, &symbols);

        assert_eq!(frames.len(), ALWAYS_ON.len() * symbols.len());
        for coin in &symbols {
            for kind in ALWAYS_ON {
                assert!(
                    frames.iter().any(|f| {
                        f["subscription"]["type"] == kind
                            && f["subscription"]["coin"] == coin.as_str()
                    }),
                    "{coin} was never subscribed to {kind}"
                );
            }
        }
    }

    /// `fast` is omitted rather than sent as `false`, which keeps a default
    /// recording's subscribe frames byte-identical to those made before the
    /// fast feed existed. Only `l2Book` ever carries the flag; the funding feed
    /// must not grow one by accident.
    #[test]
    fn only_the_book_subscription_carries_the_fast_flag() {
        let specs = [
            SubscriptionSpec::l2_book(false),
            SubscriptionSpec::l2_book(true),
            SubscriptionSpec::plain("activeAssetCtx"),
        ];
        let frames = subscription_frames(&specs, &coins(&["BTC"]));

        assert!(frames[0]["subscription"].get("fast").is_none());
        assert_eq!(frames[1]["subscription"]["fast"], true);
        assert!(frames[2]["subscription"].get("fast").is_none());
    }
}
