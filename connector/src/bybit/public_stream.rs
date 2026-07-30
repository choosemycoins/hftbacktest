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

/// What a subscribe batch's `req_id` starts with; the symbol it covers follows.
///
/// Written by [`subscription_frames`], read back by [`acked_symbol`] — one constant, because a
/// rejection can only be pinned on a symbol if the two agree. Bybit echoes `req_id` on the ack
/// and it is the sole field there that leads back to a registration. `AGENTS.md` §4.2.
const SUBSCRIBE_REQ_ID_PREFIX: &str = "subscribe:";

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
                req_id: format!("{SUBSCRIBE_REQ_ID_PREFIX}{symbol}"),
                op: "subscribe".to_string(),
                args,
            })
            .unwrap()
        })
        .collect()
}

/// The symbol whose batch an ack belongs to, read out of the `req_id` it was sent with.
///
/// `None` for any other shape — the keepalive's `"ping"`, an older build's constant
/// `"subscribe"`, a venue reply that carries no `req_id` at all. Guessing there would point the
/// operator at a symbol that is fine, which is worse than naming none.
fn acked_symbol(req_id: Option<&str>) -> Option<&str> {
    req_id?
        .strip_prefix(SUBSCRIBE_REQ_ID_PREFIX)
        .filter(|symbol| !symbol.is_empty())
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
                // something the bots have to hear about.
                //
                // The batch is one symbol's, and `req_id` carries which one, so the log names
                // the symbol whose market data has just stopped instead of leaving the operator
                // to grep for it. The bots still only get `CriticalConnectionError`: the symbol
                // belongs in the log, not the payload — see below.
                //
                // Not retried on this connection: the tracker has already marked that symbol,
                // and a rejection over a topic *name* is a verdict that will not change while
                // the connection lives — re-asking would spend rate limit for the same answer.
                // A transient rejection (`10006`, "Too many visits") is still not told apart
                // from a permanent one; both recover on the next reconnect, which re-derives
                // every subscription from the shared symbol set.
                if resp.op == "subscribe" && !resp.success.unwrap_or(true) {
                    error!(
                        symbol = acked_symbol(resp.req_id.as_deref()).unwrap_or("unattributed"),
                        ?resp,
                        "Bybit rejected a subscribe batch. Every topic in it failed, so that \
                         symbol gets no market data on this connection, and it will not be \
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
        fmt,
        sync::{Arc, Mutex},
        task::Poll,
    };

    use futures_util::{Stream, stream};
    use hftbacktest::types::{ErrorKind, LiveEvent};
    use tokio::sync::{
        broadcast,
        mpsc::{UnboundedReceiver, unbounded_channel},
    };
    use tokio_tungstenite::tungstenite::{Error as WsError, Message};
    use tracing::{
        Event as LogEvent,
        Level,
        Subscriber,
        field::{Field, Visit},
    };
    use tracing_subscriber::{
        Layer,
        layer::{Context as LayerContext, SubscriberExt},
    };

    use crate::{
        bybit::{
            BybitError,
            SharedSymbolSet,
            public_stream::{PublicStream, acked_symbol, subscription_frames},
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

    /// How many `CriticalConnectionError`s reached the bots, draining what is queued.
    fn errors(ev_rx: &mut UnboundedReceiver<PublishEvent>) -> usize {
        let mut errors = 0;
        while let Ok(event) = ev_rx.try_recv() {
            if let PublishEvent::LiveEvent(LiveEvent::Error(error)) = event {
                assert_eq!(error.kind, ErrorKind::CriticalConnectionError);
                errors += 1;
            }
        }
        errors
    }

    /// The `symbol` field of every `error!` the code under test emitted, in order.
    ///
    /// The attribution **is** a log field, and only a log field: nothing about the connection
    /// changes when a batch is rejected — nothing retries, nothing un-marks, and the payload the
    /// bots get is deliberately symbol-free because a `LiveEvent` is capped at 512 bytes (§2).
    /// So the field is what has to be asserted, or the one line that is the whole feature is
    /// pinned by nothing: hard-coding `symbol = "unattributed"` at the call site kept every
    /// other test in this module green.
    ///
    /// The *field*, never the message — prose is free to change, `symbol` is what the operator
    /// greps and therefore the contract.
    #[derive(Clone, Default)]
    struct ReportedSymbols(Arc<Mutex<Vec<String>>>);

    impl ReportedSymbols {
        /// Captures on **this thread only**, for as long as the guard lives. `#[tokio::test]`
        /// polls the future on the calling thread, so the loop under test logs in here while
        /// every other test's subscriber is left alone.
        fn capturing(&self) -> tracing::subscriber::DefaultGuard {
            tracing::subscriber::set_default(tracing_subscriber::registry().with(self.clone()))
        }

        fn reported(&self) -> Vec<String> {
            self.0.lock().unwrap().clone()
        }
    }

    impl<S: Subscriber> Layer<S> for ReportedSymbols {
        fn on_event(&self, event: &LogEvent<'_>, _: LayerContext<'_, S>) {
            if *event.metadata().level() != Level::ERROR {
                return;
            }
            let mut symbol = SymbolField(None);
            event.record(&mut symbol);
            if let Some(symbol) = symbol.0 {
                self.0.lock().unwrap().push(symbol);
            }
        }
    }

    /// Pulls a `symbol = ...` field out of one event and ignores every other field.
    struct SymbolField(Option<String>);

    impl Visit for SymbolField {
        fn record_str(&mut self, field: &Field, value: &str) {
            if field.name() == "symbol" {
                self.0 = Some(value.to_string());
            }
        }

        fn record_debug(&mut self, _: &Field, _: &dyn fmt::Debug) {}
    }

    /// A rejection ack for `symbol`, echoing the `req_id` its subscribe batch really carried.
    ///
    /// Built from [`subscription_frames`] rather than hand-written, so writer and reader have to
    /// agree through the code under test. Two matching literals in two tests would let the two
    /// sides drift apart into a `req_id` nothing can attribute.
    fn rejection_for(symbol: &str, depths: &[u32]) -> String {
        let batch: serde_json::Value =
            serde_json::from_str(&subscription_frames(&[symbol.to_string()], depths)[0]).unwrap();
        let req_id = batch["req_id"].as_str().unwrap();
        format!(
            r#"{{"success":false,"ret_msg":"error:handler not found,topic:orderbook.500.{symbol}","conn_id":"c","req_id":"{req_id}","op":"subscribe"}}"#
        )
    }

    /// A read half that hands the loop `frames` as text and then runs `script`, staying quiet
    /// afterwards.
    ///
    /// `utils::testing::read_frames` ends instead, which is a dropped socket — and a connection
    /// that is gone cannot be asked what a frame did to its subscription state. The script ends
    /// the loop itself by dropping the wake-up sender it captured, as `read_after_connect` does.
    fn read_frames_then<F>(
        frames: Vec<String>,
        script: F,
    ) -> impl Stream<Item = Result<Message, WsError>> + Unpin
    where
        F: FnOnce(),
    {
        let mut frames = frames.into_iter();
        let mut script = Some(script);
        stream::poll_fn(move |_| match frames.next() {
            Some(frame) => Poll::Ready(Some(Ok(Message::Text(frame.into())))),
            None => {
                if let Some(script) = script.take() {
                    script();
                }
                Poll::Pending
            }
        })
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
            r#"{"success":true,"ret_msg":"","conn_id":"c","req_id":"subscribe:BTCUSDT","op":"subscribe"}"#
                .to_string(),
            r#"{"success":false,"ret_msg":"error:handler not found,topic:orderbook.500.BTCUSDT","conn_id":"c","req_id":"subscribe:BTCUSDT","op":"subscribe"}"#
                .to_string(),
        ]);

        stream
            .serve(&mut sink, &mut read)
            .await
            .expect_err("the read half ended");

        assert_eq!(
            errors(&mut ev_rx),
            1,
            "the accepted batch must not report anything"
        );
    }

    /// **`AGENTS.md` §4.2, the attribution.** The rejection has to name the symbol whose market
    /// data just stopped: with the old constant `req_id` the `error!` named nobody and the
    /// operator grepped blind.
    ///
    /// A round trip on purpose — the ack echoes the `req_id` [`subscription_frames`] really
    /// wrote, so writer and reader agree through the code rather than through two literals in
    /// two tests. And the assertion is on the `symbol` field the loop emitted, because that
    /// field is the entire deliverable: replacing it with a hard-coded `"unattributed"` left the
    /// other 196 tests green, so nothing else here can notice the feature being reverted.
    #[tokio::test]
    async fn a_rejection_names_the_symbol_it_answers_for() {
        let reported = ReportedSymbols::default();
        let _capturing = reported.capturing();
        let (mut stream, _symbols, _symbol_tx, mut ev_rx) = stream(&["BTCUSDT"], &[1, 500]);
        let mut sink = RecordingSink::<BybitError>::default();
        let mut read = read_frames(vec![rejection_for("BTCUSDT", &[1, 500])]);

        stream
            .serve(&mut sink, &mut read)
            .await
            .expect_err("the read half ended");

        assert_eq!(
            reported.reported(),
            vec!["BTCUSDT"],
            "the rejection must be pinned on the symbol whose batch it answers"
        );
        assert_eq!(errors(&mut ev_rx), 1);
    }

    /// A rejection must not disturb anything else: nothing un-subscribes or retries on this
    /// connection, so a rejection that quietly un-marked a symbol would leave the next wake-up
    /// resubscribing a live one and spending rate limit (`10006`, "Too many visits").
    ///
    /// Tracker state is only visible in what the connection writes next, so this drives a
    /// wake-up after the rejection and requires it to write nothing — for the rejected symbol
    /// and for the healthy one beside it alike.
    #[tokio::test]
    async fn a_rejection_un_marks_nothing() {
        let (mut stream, _symbols, symbol_tx, mut ev_rx) =
            stream(&["BTCUSDT", "ETHUSDT"], &[1, 500]);
        let mut sink = RecordingSink::<BybitError>::default();
        let mut read = read_frames_then(vec![rejection_for("BTCUSDT", &[1, 500])], move || {
            // Whatever the rejection did to this connection's subscription state is only
            // visible in what the next wake-up writes. Dropping the sender ends the loop.
            symbol_tx.send("BTCUSDT".to_string()).unwrap();
        });

        stream.serve(&mut sink, &mut read).await.unwrap();

        let batches = subscribed(&sink);
        assert_eq!(
            batches.len(),
            2,
            "the two connect-time batches and nothing else: {:?}",
            sink.sent
        );
        assert_eq!(
            errors(&mut ev_rx),
            1,
            "the rejection is still reported to the bots"
        );
    }

    /// A `req_id` this connector did not write — the keepalive's `"ping"`, an older build's
    /// constant `"subscribe"`, a venue that echoes nothing — is not a symbol, and guessing one
    /// would point the operator at a symbol that is fine. Unattributed still means reported:
    /// the batch's topics are dead either way, and the log says so in as many words rather than
    /// dropping the field and leaving a rejection that looks like every other one.
    #[tokio::test]
    async fn a_rejection_with_no_symbol_in_its_req_id_is_reported_unattributed() {
        let reported = ReportedSymbols::default();
        let _capturing = reported.capturing();
        let (mut stream, _symbols, _symbol_tx, mut ev_rx) = stream(&["BTCUSDT"], &[1, 500]);
        let mut sink = RecordingSink::<BybitError>::default();
        for req_id in [Some("ping"), Some("subscribe"), Some("subscribe:"), None] {
            assert_eq!(acked_symbol(req_id), None, "{req_id:?} is not a symbol");
        }
        let mut read = read_frames(vec![
            r#"{"success":false,"ret_msg":"error:handler not found,topic:orderbook.500.BTCUSDT","conn_id":"c","req_id":"subscribe","op":"subscribe"}"#.to_string(),
            r#"{"success":false,"ret_msg":"Too many visits","conn_id":"c","op":"subscribe"}"#
                .to_string(),
        ]);

        stream
            .serve(&mut sink, &mut read)
            .await
            .expect_err("the read half ended");

        assert_eq!(errors(&mut ev_rx), 2);
        assert_eq!(reported.reported(), vec!["unattributed", "unattributed"]);
    }

    /// **`AGENTS.md` §4.2, the attribution half.** A batch's `req_id` names the symbol it
    /// covers, because it is the only field that comes back on the ack: a rejection carries
    /// `ret_msg` at the venue's discretion and nothing else that ties it to a registration.
    /// With a constant `req_id` the `error!` and the `CriticalConnectionError` named nobody,
    /// and the operator had to guess which symbol's market data had just stopped.
    ///
    /// `op` is pinned alongside it: the ack handler keys the rejection branch off `resp.op`,
    /// so folding the symbol into `op` instead would make every rejection invisible.
    #[test]
    fn every_batch_names_its_symbol_in_the_req_id() {
        let frames = subscription_frames(&["BTCUSDT".to_string(), "ETHUSDT".to_string()], &[1, 50]);

        assert_eq!(frames.len(), 2);
        for (frame, symbol) in frames.iter().zip(["BTCUSDT", "ETHUSDT"]) {
            let batch: serde_json::Value = serde_json::from_str(frame).unwrap();
            assert_eq!(batch["req_id"], format!("subscribe:{symbol}"), "{frame}");
            assert_eq!(batch["op"], "subscribe", "{frame}");
        }
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
