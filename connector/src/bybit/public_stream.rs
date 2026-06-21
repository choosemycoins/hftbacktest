use std::time::Duration;

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use hftbacktest::prelude::{
    Event,
    LOCAL_ASK_DEPTH_BBO_EVENT,
    LOCAL_ASK_DEPTH_EVENT,
    LOCAL_BID_DEPTH_BBO_EVENT,
    LOCAL_BID_DEPTH_EVENT,
    LOCAL_BUY_TRADE_EVENT,
    LOCAL_SELL_TRADE_EVENT,
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
    tungstenite::{Bytes, Message, client::IntoClientRequest},
};
use simd_json::Buffers;
use tracing::{debug, error};

use crate::{
    bybit::{
        BybitError,
        msg::Op,
        simd_parse::{PublicFeed, decode_public_feed},
    },
    connector::PublishEvent,
};

pub struct PublicStream {
    ev_tx: UnboundedSender<PublishEvent>,
    symbol_rx: Receiver<String>,
}

impl PublicStream {
    pub fn new(ev_tx: UnboundedSender<PublishEvent>, symbol_rx: Receiver<String>) -> Self {
        Self { ev_tx, symbol_rx }
    }

    fn handle_public_stream(
        &self,
        scratch: &mut Vec<u8>,
        buffers: &mut Buffers,
        text: &str,
    ) -> Result<(), BybitError> {
        // Parse the frame once with simd-json and read the fields directly from the resulting DOM,
        // reusing `scratch` and `buffers` so steady-state decoding allocates nothing.
        match decode_public_feed(scratch, buffers, text)? {
            PublicFeed::OrderBook { bbo, update } => {
                let (bid_ev, ask_ev) = if bbo {
                    (LOCAL_BID_DEPTH_BBO_EVENT, LOCAL_ASK_DEPTH_BBO_EVENT)
                } else {
                    (LOCAL_BID_DEPTH_EVENT, LOCAL_ASK_DEPTH_EVENT)
                };
                let exch_ts = update.cts * 1_000_000;

                for (px, qty) in update.bids {
                    self.ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                            symbol: update.symbol.clone(),
                            event: Event {
                                ev: bid_ev,
                                exch_ts,
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

                for (px, qty) in update.asks {
                    self.ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                            symbol: update.symbol.clone(),
                            event: Event {
                                ev: ask_ev,
                                exch_ts,
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
            }
            PublicFeed::Trades(trades) => {
                for item in trades {
                    self.ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                            symbol: item.symbol,
                            event: Event {
                                ev: if item.side == Side::Sell {
                                    LOCAL_SELL_TRADE_EVENT
                                } else {
                                    LOCAL_BUY_TRADE_EVENT
                                },
                                exch_ts: item.ts * 1_000_000,
                                local_ts: Utc::now().timestamp_nanos_opt().unwrap(),
                                order_id: 0,
                                px: item.price,
                                qty: item.size,
                                ival: 0,
                                fval: 0.0,
                            },
                        }))
                        .unwrap();
                }
            }
            PublicFeed::Other => {
                debug!(%text, "Op");
            }
        }
        Ok(())
    }

    pub async fn connect(&mut self, url: &str) -> Result<(), BybitError> {
        let mut request = url.into_client_request()?;
        let _ = request.headers_mut();

        let (ws_stream, _) = connect_async(request).await?;
        let (mut write, mut read) = ws_stream.split();
        let mut interval = time::interval(Duration::from_secs(15));

        // Reused across frames so steady-state decoding does not allocate: `scratch` holds the
        // mutable copy simd-json parses in place, `buffers` holds simd-json's internal buffers.
        let mut scratch: Vec<u8> = Vec::with_capacity(64 * 1024);
        let mut buffers = Buffers::new(64 * 1024);

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
                    Ok(symbol) => {
                        // Subscribes to the orderbook.1, orderbook.50 and orderbook.200 topics to
                        // obtain a wider range of depth and the most frequent updates.
                        // The different updates are handled by data fusion.
                        // Please see: `<https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook>`
                        let args = vec![
                            format!("orderbook.1.{symbol}"),
                            format!("orderbook.50.{symbol}"),
                            format!("orderbook.200.{symbol}"),
                            format!("orderbook.1000.{symbol}"),
                            format!("publicTrade.{symbol}")
                        ];
                        let op = Op {
                            req_id: "subscribe".to_string(),
                            op: "subscribe".to_string(),
                            args,
                        };
                        let s = serde_json::to_string(&op).unwrap();
                        write.send(Message::Text(s.into())).await?;
                    }
                    Err(RecvError::Closed) => {
                        return Ok(());
                    }
                    Err(RecvError::Lagged(num)) => {
                        error!("{num} subscription requests were missed.");
                    }
                },
                message = read.next() => {
                    match message {
                        Some(Ok(Message::Text(text))) => {
                            if let Err(error) =
                                self.handle_public_stream(&mut scratch, &mut buffers, &text)
                            {
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
                            return Err(BybitError::from(error));
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
