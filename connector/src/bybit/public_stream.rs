use std::time::Duration;

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use hftbacktest::prelude::{
    ErrorKind,
    Event,
    LOCAL_ASK_DEPTH_BBO_EVENT,
    LOCAL_ASK_DEPTH_EVENT,
    LOCAL_BID_DEPTH_BBO_EVENT,
    LOCAL_BID_DEPTH_EVENT,
    LOCAL_BUY_TRADE_EVENT,
    LOCAL_SELL_TRADE_EVENT,
    LiveError,
    LiveEvent,
    Side,
};
use tokio::{
    select,
    sync::{
        broadcast::{Receiver, error::RecvError},
        mpsc::UnboundedSender,
    },
    time,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Bytes, Error as WsError, Message, client::IntoClientRequest},
};
use tracing::{debug, error, info};

use crate::{
    bybit::{
        BybitError,
        SharedSymbolSet,
        msg,
        msg::{Op, OrderBook, PublicStreamMsg},
    },
    connector::PublishEvent,
    utils::{SubscriptionTracker, parse_depth},
};

/// The subscribe batch for one symbol: the configured orderbook depths plus `publicTrade`.
///
/// One batch **per symbol**, deliberately: a single unknown topic fails the whole batch with
/// `error:handler not found,topic:orderbook.<N>.<SYMBOL>`, so a batch covering every symbol
/// would take every symbol's market data down with the first bad topic. See the doc comment
/// on `Config::orderbook_depths` for what mainnet actually accepts.
///
/// Split out from the socket so the wire shape can be asserted without one.
///
/// Ref: <https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook>
pub fn subscription_frames(symbols: &[String], depths: &[u32]) -> Vec<String> {
    symbols
        .iter()
        .map(|symbol| {
            let mut args: Vec<String> = depths
                .iter()
                .map(|depth| format!("orderbook.{depth}.{symbol}"))
                .collect();
            args.push(format!("publicTrade.{symbol}"));
            serde_json::to_string(&Op {
                req_id: "subscribe".to_string(),
                op: "subscribe".to_string(),
                args,
            })
            .unwrap()
        })
        .collect()
}

pub struct PublicStream {
    ev_tx: UnboundedSender<PublishEvent>,
    /// The registered symbols, authoritative for what to subscribe. The broadcast below is
    /// only a wake-up — see [`PublicStream::subscribe_pending`].
    symbols: SharedSymbolSet,
    symbol_rx: Receiver<String>,
    depths: Vec<u32>,
}

impl PublicStream {
    pub fn new(
        ev_tx: UnboundedSender<PublishEvent>,
        symbols: SharedSymbolSet,
        symbol_rx: Receiver<String>,
        depths: Vec<u32>,
    ) -> Self {
        Self {
            ev_tx,
            symbols,
            symbol_rx,
            depths,
        }
    }

    /// Subscribes every registered symbol this connection has not subscribed yet.
    ///
    /// Reads the shared symbol set rather than the broadcast: `Connector::register` sends each
    /// symbol into the broadcast exactly once ever, while a reconnect builds a fresh
    /// `Receiver` that only sees what is sent after it — so subscribing from the broadcast
    /// alone left a reconnected stream connected and subscribed to nothing. `AGENTS.md` §4.2.
    ///
    /// The tracker belongs to the connection and is what keeps a registration wake-up from
    /// resubscribing what is already live.
    async fn subscribe_pending<S>(
        &self,
        write: &mut S,
        tracker: &mut SubscriptionTracker,
    ) -> Result<(), BybitError>
    where
        S: SinkExt<Message> + Unpin,
        BybitError: From<S::Error>,
    {
        let registered = self.symbols.lock().unwrap().clone();
        let pending = tracker.pending(&registered);
        if pending.is_empty() {
            return Ok(());
        }
        for frame in subscription_frames(&pending, &self.depths) {
            write.send(Message::Text(frame.into())).await?;
        }
        // Marked only once every batch is on the wire: a write that failed drops the
        // connection, and the next one has to ask about all of them again.
        tracker.mark(&pending);
        info!(
            symbols = pending.len(),
            ?pending,
            "Subscribed to the Bybit public streams."
        );
        Ok(())
    }

    async fn handle_public_stream(&self, text: &str) -> Result<(), BybitError> {
        let stream = serde_json::from_str::<PublicStreamMsg>(text)?;
        match stream {
            PublicStreamMsg::Op(resp) => {
                // **`AGENTS.md` §4.1 in the wild.** One unknown topic fails the *whole* batch —
                // measured on mainnet for `orderbook.500` — and Bybit then keeps the socket
                // open and goes on answering pings, so the symbols in that batch simply have
                // no market data while everything looks healthy. That makes the rejection
                // something the bots have to hear about, and the operator's only lead is
                // `ret_msg`, which names the offending topic.
                //
                // Not retried on this connection: the tracker has already marked those
                // symbols, and a rejection over a topic *name* is a verdict that will not
                // change while the connection lives. A transient rejection — rate limit
                // (`10006`, "Too many visits") — is not told apart from it here, because
                // `req_id` is not per symbol, so nothing can say which symbols to un-mark.
                // Those recover on the next reconnect, which re-derives every subscription.
                // Attributing rejections would mean putting the symbol in `req_id`; until
                // then this error is the only signal, and it is not a quiet one.
                if resp.op == "subscribe" && !resp.success.unwrap_or(true) {
                    error!(
                        ?resp,
                        "Bybit rejected a subscribe batch. Every topic in it failed, so those \
                         symbols get no market data on this connection, and it will not be \
                         retried. Check `orderbook_depths` against what the venue accepts."
                    );
                    // The venue's message stays in the log rather than going into the payload:
                    // a `LiveEvent` is capped at 512 bytes (`live/ipc/config.rs`) and an
                    // oversize one fails the publish task, which would take the connector down
                    // over a string the venue chose.
                    self.ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::new(
                            ErrorKind::CriticalConnectionError,
                        ))))
                        .unwrap();
                } else {
                    debug!(?resp, "Op");
                }
            }
            PublicStreamMsg::Topic(stream) => {
                if stream.topic.starts_with("orderbook.1") {
                    let data: OrderBook = serde_json::from_value(stream.data)?;
                    let (bids, asks) = parse_depth(data.bids, data.asks)?;

                    for (px, qty) in bids {
                        self.ev_tx
                            .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                                symbol: data.symbol.clone(),
                                event: Event {
                                    ev: LOCAL_BID_DEPTH_BBO_EVENT,
                                    exch_ts: stream.cts.unwrap() * 1_000_000,
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
                                    ev: LOCAL_ASK_DEPTH_BBO_EVENT,
                                    exch_ts: stream.cts.unwrap() * 1_000_000,
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
                } else if stream.topic.starts_with("orderbook") {
                    let data: OrderBook = serde_json::from_value(stream.data)?;
                    let (bids, asks) = parse_depth(data.bids, data.asks)?;

                    for (px, qty) in bids {
                        self.ev_tx
                            .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                                symbol: data.symbol.clone(),
                                event: Event {
                                    ev: LOCAL_BID_DEPTH_EVENT,
                                    exch_ts: stream.cts.unwrap() * 1_000_000,
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
                                    exch_ts: stream.cts.unwrap() * 1_000_000,
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
                } else if stream.topic.starts_with("publicTrade") {
                    let data: Vec<msg::Trade> = serde_json::from_value(stream.data)?;
                    for item in data {
                        self.ev_tx
                            .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                                symbol: item.symbol.clone(),
                                event: Event {
                                    ev: {
                                        if item.side == Side::Sell {
                                            LOCAL_SELL_TRADE_EVENT
                                        } else {
                                            LOCAL_BUY_TRADE_EVENT
                                        }
                                    },
                                    exch_ts: item.ts * 1_000_000,
                                    local_ts: Utc::now().timestamp_nanos_opt().unwrap(),
                                    order_id: 0,
                                    px: item.trade_price,
                                    qty: item.trade_size,
                                    ival: 0,
                                    fval: 0.0,
                                },
                            }))
                            .unwrap();
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn connect(&mut self, url: &str) -> Result<(), BybitError> {
        let mut request = url.into_client_request()?;
        let _ = request.headers_mut();

        let (ws_stream, _) = connect_async(request).await?;
        let (mut write, mut read) = ws_stream.split();
        self.serve(&mut write, &mut read).await
    }

    /// One connection: subscribe, then pump it until it drops.
    ///
    /// Takes the halves instead of dialling so the connect-time subscribe — the fix for
    /// `AGENTS.md` §4.2, and the one call the venue's silence depends on — is driven by the
    /// tests below rather than only modelled by them.
    async fn serve<S, R>(&mut self, write: &mut S, read: &mut R) -> Result<(), BybitError>
    where
        S: SinkExt<Message> + Unpin,
        BybitError: From<S::Error>,
        R: StreamExt<Item = Result<Message, WsError>> + Unpin,
    {
        // Per connection, deliberately: see `SubscriptionTracker`. Everything registered so
        // far goes out now, which is the whole of `AGENTS.md` §4.2.
        let mut tracker = SubscriptionTracker::default();
        self.subscribe_pending(write, &mut tracker).await?;
        let mut interval = time::interval(Duration::from_secs(15));

        loop {
            select! {
                _ = interval.tick() => {
                    let op = Op {
                        req_id: "ping".to_string(),
                        op: "ping".to_string(),
                        args: vec![]
                    };
                    let s = serde_json::to_string(&op).unwrap();
                    write.send(Message::Text(s.into())).await?;
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
                message = read.next() => {
                    match message {
                        Some(Ok(Message::Text(text))) => {
                            if let Err(error) = self.handle_public_stream(&text).await {
                                error!(?error, %text, "Couldn't handle PublicStreamMsg.");
                            }
                        }
                        Some(Ok(Message::Ping(_))) => {
                            write.send(Message::Pong(Bytes::default())).await?;
                        }
                        Some(Ok(Message::Close(close_frame))) => {
                            return Err(BybitError::ConnectionAbort(
                                close_frame
                                    .map(|f| f.to_string())
                                    .unwrap_or(String::new())
                            ));
                        }
                        Some(Ok(Message::Binary(_)))
                        | Some(Ok(Message::Frame(_)))
                        | Some(Ok(Message::Pong(_))) => {}
                        Some(Err(error)) => {
                            // Named rather than `from`: with `BybitError: From<S::Error>` in
                            // scope the conversion is ambiguous.
                            return Err(BybitError::Tungstenite(error));
                        }
                        None => {
                            return Err(BybitError::ConnectionInterrupted);
                        }
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
        bybit::{
            BybitError,
            SharedSymbolSet,
            public_stream::{PublicStream, subscription_frames},
        },
        connector::PublishEvent,
        utils::testing::{RecordingSink, closed_read, read_after_connect, read_frames},
    };

    /// The stream under test, plus the handles the venue side of it is driven through.
    type Fixture = (
        PublicStream,
        SharedSymbolSet,
        broadcast::Sender<String>,
        UnboundedReceiver<PublishEvent>,
    );

    /// A stream whose shared symbol set already holds `registered` and whose broadcast
    /// receiver holds nothing — exactly the state a reconnect finds.
    fn stream(registered: &[&str], depths: &[u32]) -> Fixture {
        stream_with_capacity(registered, depths, 16)
    }

    fn stream_with_capacity(registered: &[&str], depths: &[u32], wake_ups: usize) -> Fixture {
        let (ev_tx, ev_rx) = unbounded_channel();
        let (symbol_tx, symbol_rx) = broadcast::channel(wake_ups);
        let symbols: SharedSymbolSet = Arc::new(Mutex::new(
            registered
                .iter()
                .map(|s| s.to_string())
                .collect::<HashSet<_>>(),
        ));
        (
            PublicStream::new(ev_tx, symbols.clone(), symbol_rx, depths.to_vec()),
            symbols,
            symbol_tx,
            ev_rx,
        )
    }

    /// The subscribe batches that went out, in order, ignoring the keepalive pings the
    /// connection loop also writes.
    fn subscribed(sink: &RecordingSink<BybitError>) -> Vec<serde_json::Value> {
        sink.sent
            .iter()
            .map(|frame| serde_json::from_str::<serde_json::Value>(frame).unwrap())
            .filter(|frame| frame["op"] == "subscribe")
            .collect()
    }

    /// **`AGENTS.md` §4.2.** Every symbol registered before this connection existed has to be
    /// subscribed at connect. The broadcast cannot deliver them: `Connector::register` sends
    /// each symbol into it exactly once ever, and a reconnect builds a fresh `Receiver` that
    /// only sees what is sent *after* it — so a stream subscribing from the broadcast alone
    /// came back connected and subscribed to nothing, with market data silently stopped.
    ///
    /// Drives the real connection loop, so this fails if the connect-time subscribe is
    /// removed or moved: the read half is already gone, and everything asserted here went out
    /// before the loop read its first frame.
    #[tokio::test]
    async fn every_registered_symbol_is_subscribed_at_connect() {
        let (mut stream, _symbols, _symbol_tx, _ev_rx) = stream(&["BTCUSDT", "ETHUSDT"], &[1, 50]);
        let mut sink = RecordingSink::<BybitError>::default();

        let error = stream
            .serve(&mut sink, &mut closed_read())
            .await
            .unwrap_err();

        assert!(
            matches!(error, BybitError::ConnectionInterrupted),
            "{error:?}"
        );
        let batches = subscribed(&sink);
        assert_eq!(batches.len(), 2, "one batch per symbol: {:?}", sink.sent);
        for symbol in ["BTCUSDT", "ETHUSDT"] {
            let batch = batches
                .iter()
                .find(|batch| batch["args"][0] == format!("orderbook.1.{symbol}"))
                .unwrap_or_else(|| panic!("{symbol} was never subscribed: {:?}", sink.sent));
            let args: Vec<String> = batch["args"]
                .as_array()
                .unwrap()
                .iter()
                .map(|arg| arg.as_str().unwrap().to_string())
                .collect();
            assert_eq!(
                args,
                vec![
                    format!("orderbook.1.{symbol}"),
                    format!("orderbook.50.{symbol}"),
                    format!("publicTrade.{symbol}"),
                ]
            );
        }
    }

    /// A symbol registered *during* a connection is subscribed exactly once, and one already
    /// live is not subscribed again.
    ///
    /// Both halves matter, and they arrive together: `register` inserts into the shared set
    /// and *then* broadcasts, so a symbol can sit in the connect-time snapshot and in the
    /// broadcast both. Subscribing it twice would spend rate limit (`10006`, "Too many
    /// visits") on a connection that is otherwise healthy, and this path now runs on every
    /// wake-up.
    #[tokio::test]
    async fn a_symbol_registered_after_connect_is_subscribed_exactly_once() {
        let (mut stream, symbols, symbol_tx, _ev_rx) = stream(&["BTCUSDT"], &[1, 50]);
        let mut sink = RecordingSink::<BybitError>::default();
        // Runs once the connect-time subscribe has gone out: BTCUSDT is already live, and
        // ETHUSDT has just registered. Dropping the sender is what ends the loop.
        let mut read = read_after_connect(move || {
            symbols.lock().unwrap().insert("ETHUSDT".to_string());
            symbol_tx.send("BTCUSDT".to_string()).unwrap();
            symbol_tx.send("ETHUSDT".to_string()).unwrap();
        });

        stream.serve(&mut sink, &mut read).await.unwrap();

        let batches = subscribed(&sink);
        assert_eq!(batches.len(), 2, "{:?}", sink.sent);
        assert_eq!(batches[0]["args"][0], "orderbook.1.BTCUSDT");
        assert_eq!(batches[1]["args"][0], "orderbook.1.ETHUSDT");
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
        let (mut stream, _symbols, _symbol_tx, _ev_rx) = stream(&["BTCUSDT", "ETHUSDT"], &[1, 50]);
        let mut sink = RecordingSink::<BybitError>::default();

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

        let batches = subscribed(&sink);
        assert_eq!(batches.len(), 2, "{:?}", sink.sent);
        for symbol in ["BTCUSDT", "ETHUSDT"] {
            assert!(
                batches
                    .iter()
                    .any(|batch| batch["args"][0] == format!("orderbook.1.{symbol}")),
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
        let (mut stream, symbols, symbol_tx, _ev_rx) =
            stream_with_capacity(&["BTCUSDT"], &[1, 50], 1);
        let mut sink = RecordingSink::<BybitError>::default();
        let mut read = read_after_connect(move || {
            let mut symbols = symbols.lock().unwrap();
            symbols.insert("ETHUSDT".to_string());
            symbols.insert("SOLUSDT".to_string());
            // More wake-ups than the channel holds, so the receiver is told it lagged instead
            // of being handed the symbols.
            symbol_tx.send("ETHUSDT".to_string()).unwrap();
            symbol_tx.send("SOLUSDT".to_string()).unwrap();
        });

        stream.serve(&mut sink, &mut read).await.unwrap();

        let batches = subscribed(&sink);
        assert_eq!(batches.len(), 3, "{:?}", sink.sent);
        for symbol in ["BTCUSDT", "ETHUSDT", "SOLUSDT"] {
            assert_eq!(
                batches
                    .iter()
                    .filter(|batch| batch["args"][0] == format!("orderbook.1.{symbol}"))
                    .count(),
                1,
                "{symbol}: {:?}",
                sink.sent
            );
        }
    }

    /// **`AGENTS.md` §4.1, the other half of the trap.** Bybit fails the *whole* batch for one
    /// unknown topic and then keeps the socket open, answering pings, so the symbols in it have
    /// no market data while the connection looks healthy. Nothing retries the batch, so the
    /// rejection cannot stay at `debug!` — the bots are told, and their `error_handler` decides
    /// what that is worth.
    #[tokio::test]
    async fn a_rejected_subscribe_batch_is_reported_to_the_bots() {
        let (mut stream, _symbols, _symbol_tx, mut ev_rx) = stream(&["BTCUSDT"], &[1, 500]);
        let mut sink = RecordingSink::<BybitError>::default();
        let mut read = read_frames(vec![
            r#"{"success":true,"ret_msg":"","conn_id":"c","req_id":"subscribe","op":"subscribe"}"#
                .to_string(),
            r#"{"success":false,"ret_msg":"error:handler not found,topic:orderbook.500.BTCUSDT","conn_id":"c","req_id":"subscribe","op":"subscribe"}"#
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
        assert_eq!(errors, 1, "the accepted batch must not report anything");
    }

    /// **`AGENTS.md` §4.1.** One rejected topic fails the whole batch, so the batches stay
    /// per-symbol: merged into one, the first bad topic would take every symbol's market data
    /// down, and the connector would sit connected and subscribed to nothing.
    #[test]
    fn each_symbol_gets_its_own_batch() {
        let frames = subscription_frames(
            &["BTCUSDT".to_string(), "ETHUSDT".to_string()],
            &[1, 50, 200],
        );

        assert_eq!(frames.len(), 2);
        for (frame, symbol) in frames.iter().zip(["BTCUSDT", "ETHUSDT"]) {
            let batch: serde_json::Value = serde_json::from_str(frame).unwrap();
            let args = batch["args"].as_array().unwrap();
            assert_eq!(args.len(), 4, "three depths plus publicTrade: {frame}");
            assert!(
                args.iter()
                    .all(|arg| arg.as_str().unwrap().ends_with(symbol)),
                "a batch must not mix symbols: {frame}"
            );
        }
    }
}
