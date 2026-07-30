use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use hftbacktest::{live::ipc::TO_ALL, prelude::*};
use tokio::{
    select,
    sync::{
        broadcast::{Receiver, error::RecvError},
        mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    },
    time,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Error as WsError, Message, client::IntoClientRequest},
};
use tracing::{debug, error, info, warn};

use crate::{
    binancespot::{
        BinanceSpotError,
        SharedSymbolSet,
        msg::{
            rest,
            stream,
            stream::{MarketEventStream, MarketStream},
        },
        rest::BinanceSpotClient,
    },
    connector::PublishEvent,
    utils::{SubscriptionTracker, generate_rand_string, parse_depth, parse_px_qty_tup},
};

/// The subscribe frame for one symbol: its trade and diff-depth streams.
///
/// The symbol goes across exactly as given — `Connector::register` lowercases it, and the
/// venue's stream names are lowercase. Split out from the socket so the wire shape can be
/// asserted without one.
pub fn subscription_frame(symbol: &str, id: &str) -> String {
    format!(
        r#"{{
    "method": "SUBSCRIBE",
    "params": [
        "{symbol}@trade",
        "{symbol}@depth@0ms"
    ],
    "id": "{id}"
}}"#
    )
}

pub struct MarketDataStream {
    client: BinanceSpotClient,
    ev_tx: UnboundedSender<PublishEvent>,
    /// The registered symbols, authoritative for what to subscribe. The broadcast below is
    /// only a wake-up — see [`MarketDataStream::subscribe_pending`].
    symbols: SharedSymbolSet,
    symbol_rx: Receiver<String>,
    pending_depth_messages: HashMap<String, Vec<stream::Depth>>,
    prev_u: HashMap<String, i64>,
    rest_tx: UnboundedSender<(String, rest::Depth)>,
    rest_rx: UnboundedReceiver<(String, rest::Depth)>,
}

impl MarketDataStream {
    pub fn new(
        client: BinanceSpotClient,
        ev_tx: UnboundedSender<PublishEvent>,
        symbols: SharedSymbolSet,
        symbol_rx: Receiver<String>,
    ) -> Self {
        let (rest_tx, rest_rx) = unbounded_channel::<(String, rest::Depth)>();
        Self {
            client,
            ev_tx,
            symbols,
            symbol_rx,
            pending_depth_messages: Default::default(),
            prev_u: Default::default(),
            rest_tx,
            rest_rx,
        }
    }

    /// Subscribes every registered symbol this connection has not subscribed yet.
    ///
    /// Reads the shared symbol set rather than the broadcast: `Connector::register` sends each
    /// symbol into the broadcast exactly once ever, while a reconnect builds a fresh
    /// `Receiver` that only sees what is sent after it — so subscribing from the broadcast
    /// alone left a reconnected stream connected and subscribed to nothing.
    ///
    /// The tracker belongs to the connection and is what keeps a registration wake-up from
    /// resubscribing what is already live.
    async fn subscribe_pending<S>(
        &self,
        write: &mut S,
        tracker: &mut SubscriptionTracker,
    ) -> Result<(), BinanceSpotError>
    where
        S: SinkExt<Message> + Unpin,
        BinanceSpotError: From<S::Error>,
    {
        let registered = self.symbols.lock().unwrap().clone();
        let pending = tracker.pending(&registered);
        if pending.is_empty() {
            return Ok(());
        }
        for symbol in &pending {
            let id = generate_rand_string(16);
            write
                .send(Message::Text(subscription_frame(symbol, &id).into()))
                .await?;
        }
        // Marked only once every frame is on the wire: a write that failed drops the
        // connection, and the next one has to ask about all of them again.
        tracker.mark(&pending);
        info!(
            symbols = pending.len(),
            ?pending,
            "Subscribed to the Binance Spot market data streams."
        );
        Ok(())
    }

    /// A SUBSCRIBE either took effect or the symbol has no market data at all.
    ///
    /// A refusal is not retried on this connection — the tracker has already marked the symbol,
    /// and the frame is deterministic, so re-sending it would be refused again — and the socket
    /// stays open and keeps answering pings, so the failure is silent unless it is reported:
    /// the bots are told, and their `error_handler` decides what it is worth.
    fn handle_subscription_response(&self, result: stream::Result) {
        match &result.error {
            Some(error) => {
                error!(
                    ?error,
                    id = %result.id,
                    "Binance Spot refused a subscribe request. That symbol gets no market data \
                     on this connection, and it will not be retried."
                );
                // The venue's message stays in the log rather than going into the payload: a
                // `LiveEvent` is capped at 512 bytes (`live/ipc/config.rs`) and an oversize one
                // fails the publish task, which would take the connector down over a string
                // the venue chose.
                self.ev_tx
                    .send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::new(
                        ErrorKind::CriticalConnectionError,
                    ))))
                    .unwrap();
            }
            None => {
                debug!(?result, "Subscription request response is received.");
            }
        }
    }

    fn process_message(&mut self, stream: MarketEventStream) {
        match stream {
            MarketEventStream::DepthUpdate(data) => {
                let prev_u_val = self.prev_u.get_mut(&data.symbol);
                if prev_u_val.is_none()
                /* fixme: || data.prev_update_id != **prev_u_val.as_ref().unwrap()*/
                {
                    // if !pending_depth_messages.contains_key(&data.symbol) {
                    let client_ = self.client.clone();
                    let symbol = data.symbol.clone();
                    let rest_tx = self.rest_tx.clone();
                    tokio::spawn(async move {
                        let resp = client_.get_depth(&symbol).await;
                        match resp {
                            Ok(depth) => {
                                rest_tx.send((symbol, depth)).unwrap();
                            }
                            Err(error) => {
                                error!(
                                    ?error,
                                    %symbol,
                                    "Couldn't get the market depth via REST."
                                );
                            }
                        }
                    });
                    // }
                    // pending_depth_messages
                    //     .entry(data.symbol.clone())
                    //     .or_insert(Vec::new())
                    //     .push(data);
                    // continue;
                }
                // *prev_u_val.unwrap() = data.last_update_id;
                // fixme: currently supports natural refresh only.
                *self
                    .prev_u
                    .entry(data.symbol.clone())
                    .or_insert(data.last_update_id) = data.last_update_id;

                match parse_depth(data.bids, data.asks) {
                    Ok((bids, asks)) => {
                        self.ev_tx.send(PublishEvent::BatchStart(TO_ALL)).unwrap();

                        for (px, qty) in bids {
                            self.ev_tx
                                .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                                    symbol: data.symbol.clone(),
                                    event: Event {
                                        ev: LOCAL_BID_DEPTH_EVENT,
                                        exch_ts: data.event_time * 1_000_000,
                                        local_ts: Utc::now().timestamp_nanos_opt().unwrap(),
                                        order_id: 0,
                                        px,
                                        qty,
                                        ival: 0,
                                        fval: 0.0,
                                    },
                                }))
                                .unwrap();
                        }

                        for (px, qty) in asks {
                            self.ev_tx
                                .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                                    symbol: data.symbol.clone(),
                                    event: Event {
                                        ev: LOCAL_ASK_DEPTH_EVENT,
                                        exch_ts: data.event_time * 1_000_000,
                                        local_ts: Utc::now().timestamp_nanos_opt().unwrap(),
                                        order_id: 0,
                                        px,
                                        qty,
                                        ival: 0,
                                        fval: 0.0,
                                    },
                                }))
                                .unwrap();
                        }

                        self.ev_tx.send(PublishEvent::BatchEnd(TO_ALL)).unwrap();
                    }
                    Err(error) => {
                        error!(?error, "Couldn't parse DepthUpdate stream.");
                    }
                }
            }
            MarketEventStream::Trade(data) => match parse_px_qty_tup(data.price, data.quantity) {
                Ok((px, qty)) => {
                    if data.ignore {
                        return;
                    }
                    self.ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                            symbol: data.symbol,
                            event: Event {
                                ev: {
                                    if data.is_market_maker {
                                        LOCAL_SELL_TRADE_EVENT
                                    } else {
                                        LOCAL_BUY_TRADE_EVENT
                                    }
                                },
                                exch_ts: data.event_time * 1_000_000,
                                local_ts: Utc::now().timestamp_nanos_opt().unwrap(),
                                order_id: 0,
                                px,
                                qty,
                                ival: 0,
                                fval: 0.0,
                            },
                        }))
                        .unwrap();
                }
                Err(e) => {
                    error!(error = ?e, "Couldn't parse trade stream.");
                }
            },
            _ => unreachable!(),
        }
    }

    fn process_snapshot(&self, symbol: String, data: rest::Depth) {
        match parse_depth(data.bids, data.asks) {
            Ok((bids, asks)) => {
                self.ev_tx.send(PublishEvent::BatchStart(TO_ALL)).unwrap();

                for (px, qty) in bids {
                    self.ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                            symbol: symbol.clone(),
                            event: Event {
                                ev: LOCAL_BID_DEPTH_EVENT,
                                exch_ts: data.last_update_id * 1_000_000,
                                local_ts: Utc::now().timestamp_nanos_opt().unwrap(),
                                order_id: 0,
                                px,
                                qty,
                                ival: 0,
                                fval: 0.0,
                            },
                        }))
                        .unwrap();
                }

                for (px, qty) in asks {
                    self.ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                            symbol: symbol.clone(),
                            event: Event {
                                ev: LOCAL_ASK_DEPTH_EVENT,
                                exch_ts: data.last_update_id * 1_000_000,
                                local_ts: Utc::now().timestamp_nanos_opt().unwrap(),
                                order_id: 0,
                                px,
                                qty,
                                ival: 0,
                                fval: 0.0,
                            },
                        }))
                        .unwrap();
                }

                self.ev_tx.send(PublishEvent::BatchEnd(TO_ALL)).unwrap();
            }
            Err(error) => {
                error!(?error, "Couldn't parse Depth response.");
            }
        }
        // fixme: waits for pending messages without blocking.
        // prev_u.remove(&symbol);
        // let mut new_prev_u: Option<i64> = None;
        // while new_prev_u.is_none() {
        //     if let Some(msg) = pending_depth_messages.get_mut(&symbol) {
        //         for pending_depth in msg.into_iter() {
        //             // https://binance-docs.github.io/apidocs/futures/en/#how-to-manage-a-local-order-book-correctly
        //             // The first processed event should have U <= lastUpdateId AND u >= lastUpdateId
        //             if (
        //                 pending_depth.last_update_id < resp.last_update_id
        //                 || pending_depth.first_update_id > resp.last_update_id
        //             ) && new_prev_u.is_none() {
        //                 continue;
        //             }
        //             if new_prev_u.is_some() && pending_depth.prev_update_id != *new_prev_u.as_ref().unwrap() {
        //                 warn!(%symbol, ?pending_depth, "UpdateId does not match.");
        //             }
        //
        //             // Processes a pending depth message
        //             new_prev_u = Some(pending_depth.last_update_id);
        //             *prev_u.entry(symbol.clone())
        //                 .or_insert(pending_depth.last_update_id) = pending_depth.last_update_id;
        //         }
        //     }
        //     if new_prev_u.is_none() {
        //         // Waits for depth messages.
        //         todo!()
        //     }
        // }
    }

    pub async fn connect(&mut self, url: &str) -> Result<(), BinanceSpotError> {
        let request = url.into_client_request()?;
        let (ws_stream, _) = connect_async(request).await?;
        let (mut write, mut read) = ws_stream.split();
        self.serve(&mut write, &mut read).await
    }

    /// One connection: subscribe, then pump it until it drops.
    ///
    /// Takes the halves instead of dialling so the connect-time subscribe — the one call the
    /// venue's silence depends on — is driven by the tests below rather than only modelled by
    /// them.
    async fn serve<S, R>(&mut self, write: &mut S, read: &mut R) -> Result<(), BinanceSpotError>
    where
        S: SinkExt<Message> + Unpin,
        BinanceSpotError: From<S::Error>,
        R: StreamExt<Item = Result<Message, WsError>> + Unpin,
    {
        // Per connection, deliberately: see `SubscriptionTracker`. Everything registered so
        // far goes out now, which is the whole of the fix.
        let mut tracker = SubscriptionTracker::default();
        self.subscribe_pending(write, &mut tracker).await?;
        let mut ping_checker = time::interval(Duration::from_secs(10));
        let mut last_ping = Instant::now();

        loop {
            select! {
                Some((symbol, data)) = self.rest_rx.recv() => {
                    self.process_snapshot(symbol, data);
                }
                _ = ping_checker.tick() => {
                    if last_ping.elapsed() > Duration::from_secs(300) {
                        warn!("Ping timeout.");
                        return Err(BinanceSpotError::ConnectionInterrupted);
                    }
                }
                msg = self.symbol_rx.recv() => match msg {
                    // Only a wake-up: the shared symbol set says what to subscribe, and the
                    // tracker says what is already live.
                    Ok(_) => {
                        self.subscribe_pending(write, &mut tracker).await?;
                    }
                    Err(RecvError::Closed) => {
                        return Ok(());
                    }
                    Err(RecvError::Lagged(num)) => {
                        // Recoverable, now that the wake-up is not the source of truth:
                        // whatever was missed is in the shared set.
                        error!("{num} subscription requests were missed.");
                        self.subscribe_pending(write, &mut tracker).await?;
                    }
                },
                message = read.next() => match message {
                    Some(Ok(Message::Text(text))) => {
                        match serde_json::from_str::<MarketStream>(&text) {
                            Ok(MarketStream::EventStream(stream)) => {
                                self.process_message(stream);
                            }
                            Ok(MarketStream::Result(result)) => {
                                self.handle_subscription_response(result);
                            }
                            Err(error) => {
                                error!(?error, %text, "Couldn't parse Stream.");
                            }
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        write.send(Message::Pong(data)).await?;
                        last_ping = Instant::now();
                    }
                    Some(Ok(Message::Close(close_frame))) => {
                        return Err(BinanceSpotError::ConnectionAbort(
                            close_frame.map(|f| f.to_string()).unwrap_or(String::new())
                        ));
                    }
                    Some(Ok(Message::Binary(_)))
                    | Some(Ok(Message::Frame(_)))
                    | Some(Ok(Message::Pong(_))) => {}
                    Some(Err(error)) => {
                        // Named rather than `from`: with `BinanceSpotError: From<S::Error>` in
                        // scope the conversion is ambiguous.
                        return Err(BinanceSpotError::Tunstenite(error));
                    }
                    None => {
                        return Err(BinanceSpotError::ConnectionInterrupted);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        sync::{Arc, Mutex},
    };

    use hftbacktest::types::{ErrorKind, LiveEvent};
    use tokio::sync::{
        broadcast,
        mpsc::{UnboundedReceiver, unbounded_channel},
    };

    use crate::{
        binancespot::{
            BinanceSpotError,
            SharedSymbolSet,
            market_data_stream::{MarketDataStream, subscription_frame},
            rest::BinanceSpotClient,
        },
        connector::PublishEvent,
        utils::testing::{RecordingSink, closed_read, read_after_connect, read_frames},
    };

    /// The stream under test, plus the handles the venue side of it is driven through.
    type Fixture = (
        MarketDataStream,
        SharedSymbolSet,
        broadcast::Sender<String>,
        UnboundedReceiver<PublishEvent>,
    );

    /// A stream whose shared symbol set already holds `registered` and whose broadcast
    /// receiver holds nothing — exactly the state a reconnect finds.
    fn stream(registered: &[&str]) -> Fixture {
        stream_with_capacity(registered, 16)
    }

    fn stream_with_capacity(registered: &[&str], wake_ups: usize) -> Fixture {
        let (ev_tx, ev_rx) = unbounded_channel();
        let (symbol_tx, symbol_rx) = broadcast::channel(wake_ups);
        let symbols: SharedSymbolSet = Arc::new(Mutex::new(
            registered
                .iter()
                .map(|s| s.to_string())
                .collect::<HashSet<_>>(),
        ));
        (
            MarketDataStream::new(
                BinanceSpotClient::new("https://example.invalid", "", ""),
                ev_tx,
                symbols.clone(),
                symbol_rx,
            ),
            symbols,
            symbol_tx,
            ev_rx,
        )
    }

    /// The subscribe frames that went out, in order.
    fn subscribed(sink: &RecordingSink<BinanceSpotError>) -> Vec<serde_json::Value> {
        sink.sent
            .iter()
            .map(|frame| serde_json::from_str::<serde_json::Value>(frame).unwrap())
            .filter(|frame| frame["method"] == "SUBSCRIBE")
            .collect()
    }

    /// Every symbol registered before this connection existed has to be subscribed at
    /// connect. The broadcast cannot deliver them: `Connector::register` sends each symbol
    /// into it exactly once ever, and a reconnect builds a fresh `Receiver` that only sees
    /// what is sent *after* it — so a stream subscribing from the broadcast alone came back
    /// connected and subscribed to nothing, with market data silently stopped.
    ///
    /// Drives the real connection loop, so this fails if the connect-time subscribe is
    /// removed or moved: the read half is already gone, and everything asserted here went out
    /// before the loop read its first frame.
    #[tokio::test]
    async fn every_registered_symbol_is_subscribed_at_connect() {
        let (mut stream, _symbols, _symbol_tx, _ev_rx) = stream(&["btcusdt", "ethusdt"]);
        let mut sink = RecordingSink::<BinanceSpotError>::default();

        let error = stream
            .serve(&mut sink, &mut closed_read())
            .await
            .unwrap_err();

        assert!(
            matches!(error, BinanceSpotError::ConnectionInterrupted),
            "{error:?}"
        );
        let frames = subscribed(&sink);
        assert_eq!(frames.len(), 2, "one frame per symbol: {:?}", sink.sent);
        for symbol in ["btcusdt", "ethusdt"] {
            let frame = frames
                .iter()
                .find(|frame| frame["params"][0] == format!("{symbol}@trade"))
                .unwrap_or_else(|| panic!("{symbol} was never subscribed: {:?}", sink.sent));
            let params: Vec<String> = frame["params"]
                .as_array()
                .unwrap()
                .iter()
                .map(|param| param.as_str().unwrap().to_string())
                .collect();
            assert_eq!(
                params,
                vec![format!("{symbol}@trade"), format!("{symbol}@depth@0ms")]
            );
        }
    }

    /// A symbol registered *during* a connection is subscribed exactly once, and one already
    /// live is not subscribed again.
    ///
    /// Both halves matter, and they arrive together: `register` inserts into the shared set
    /// and *then* broadcasts, so a symbol can sit in the connect-time snapshot and in the
    /// broadcast both. Binance answers a resubscription of a live stream with an error result,
    /// on a connection that is otherwise healthy.
    #[tokio::test]
    async fn a_symbol_registered_after_connect_is_subscribed_exactly_once() {
        let (mut stream, symbols, symbol_tx, _ev_rx) = stream(&["btcusdt"]);
        let mut sink = RecordingSink::<BinanceSpotError>::default();
        // Runs once the connect-time subscribe has gone out: btcusdt is already live, and
        // ethusdt has just registered. Dropping the sender is what ends the loop.
        let mut read = read_after_connect(move || {
            symbols.lock().unwrap().insert("ethusdt".to_string());
            symbol_tx.send("btcusdt".to_string()).unwrap();
            symbol_tx.send("ethusdt".to_string()).unwrap();
        });

        stream.serve(&mut sink, &mut read).await.unwrap();

        let frames = subscribed(&sink);
        assert_eq!(frames.len(), 2, "{:?}", sink.sent);
        assert_eq!(frames[0]["params"][0], "btcusdt@trade");
        assert_eq!(frames[1]["params"][0], "ethusdt@trade");
    }

    /// **The bug itself.** The connection drops, the retry closure reconnects, and everything
    /// registered has to go out again — including symbols registered long before this
    /// connection existed, which the fresh broadcast `Receiver` will never carry.
    ///
    /// The second connection is served by the *same* stream object, which is stricter than
    /// production (the retry closure builds a new one): a tracker hoisted out of `serve` to
    /// struct level would survive the reconnect and suppress the resubscription, and this
    /// test is what refuses it.
    #[tokio::test]
    async fn every_symbol_is_resubscribed_after_a_reconnect() {
        let (mut stream, _symbols, _symbol_tx, _ev_rx) = stream(&["btcusdt", "ethusdt"]);
        let mut sink = RecordingSink::<BinanceSpotError>::default();

        stream
            .serve(&mut sink, &mut closed_read())
            .await
            .unwrap_err();
        assert_eq!(subscribed(&sink).len(), 2);
        sink.sent.clear();

        stream
            .serve(&mut sink, &mut closed_read())
            .await
            .unwrap_err();

        let frames = subscribed(&sink);
        assert_eq!(frames.len(), 2, "{:?}", sink.sent);
        for symbol in ["btcusdt", "ethusdt"] {
            assert!(
                frames
                    .iter()
                    .any(|frame| frame["params"][0] == format!("{symbol}@trade")),
                "{symbol} was not resubscribed: {:?}",
                sink.sent
            );
        }
    }

    /// Dropped wake-ups are recoverable now that the broadcast is not the source of truth:
    /// whatever was missed is in the shared set, so the lagged arm resubscribes from it
    /// instead of only logging. Before, symbols whose wake-up was dropped were never
    /// subscribed on that connection.
    #[tokio::test]
    async fn dropped_wake_ups_still_subscribe_every_registered_symbol() {
        let (mut stream, symbols, symbol_tx, _ev_rx) = stream_with_capacity(&["btcusdt"], 1);
        let mut sink = RecordingSink::<BinanceSpotError>::default();
        let mut read = read_after_connect(move || {
            let mut symbols = symbols.lock().unwrap();
            symbols.insert("ethusdt".to_string());
            symbols.insert("solusdt".to_string());
            // More wake-ups than the channel holds, so the receiver is told it lagged instead
            // of being handed the symbols.
            symbol_tx.send("ethusdt".to_string()).unwrap();
            symbol_tx.send("solusdt".to_string()).unwrap();
        });

        stream.serve(&mut sink, &mut read).await.unwrap();

        let frames = subscribed(&sink);
        assert_eq!(frames.len(), 3, "{:?}", sink.sent);
        for symbol in ["btcusdt", "ethusdt", "solusdt"] {
            assert_eq!(
                frames
                    .iter()
                    .filter(|frame| frame["params"][0] == format!("{symbol}@trade"))
                    .count(),
                1,
                "{symbol}: {:?}",
                sink.sent
            );
        }
    }

    /// A refused SUBSCRIBE leaves that symbol with no market data on a connection that stays
    /// up and keeps answering pings, and nothing retries it. It cannot stay at `debug!` — the
    /// bots are told, and their `error_handler` decides what that is worth.
    #[tokio::test]
    async fn a_refused_subscribe_is_reported_to_the_bots() {
        let (mut stream, _symbols, _symbol_tx, mut ev_rx) = stream(&["btcusdt"]);
        let mut sink = RecordingSink::<BinanceSpotError>::default();
        let mut read = read_frames(vec![
            r#"{"result":null,"id":"accepted"}"#.to_string(),
            r#"{"error":{"code":2,"msg":"Invalid request: invalid stream"},"id":"refused"}"#
                .to_string(),
        ]);

        stream
            .serve(&mut sink, &mut read)
            .await
            .expect_err("the read half ended");

        let mut errors = 0;
        while let Ok(PublishEvent::LiveEvent(event)) = ev_rx.try_recv() {
            if let LiveEvent::Error(error) = event {
                assert_eq!(error.kind, ErrorKind::CriticalConnectionError);
                errors += 1;
            }
        }
        assert_eq!(errors, 1, "the accepted subscribe must not report anything");
    }

    /// The symbol goes across exactly as it sits in the shared set, which `register`
    /// lowercased: Binance's stream names are lowercase and an uppercase one is simply not a
    /// stream the venue has.
    #[test]
    fn the_frame_carries_the_symbol_verbatim() {
        let frame: serde_json::Value =
            serde_json::from_str(&subscription_frame("btcusdt", "abc123")).unwrap();

        assert_eq!(frame["id"], "abc123");
        assert_eq!(frame["params"][0], "btcusdt@trade");
        assert_eq!(frame["params"][1], "btcusdt@depth@0ms");
    }
}
