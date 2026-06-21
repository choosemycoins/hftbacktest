//! simd-json based decoding of the Bybit public market-data stream.
//!
//! This is the connector's live hot path. The frame is parsed once with simd-json straight into a
//! typed struct via its serde bridge (`from_slice_with_buffers`, i.e. tape -> struct, no
//! intermediate DOM), which benchmarks ~2.4-2.6x faster than the original `serde_json` +
//! `serde_json::Value` two-stage decode and faster than DOM navigation of a `BorrowedValue`.
//!
//! Two pieces of state are reused across calls so steady-state decoding allocates nothing:
//! * `scratch: Vec<u8>` — a mutable copy of the (immutable) WebSocket text frame, which simd-json
//!   requires to parse in place;
//! * [`Buffers`] — simd-json's internal aligned input / string / structural-index buffers.
//!
//! The serde bridge needs the target type up front, but the payload is polymorphic on `topic`
//! (`OrderBook` vs trade array). Bybit always emits `topic` as the first field, so a cheap prefix
//! check on the raw text selects the concrete envelope before parsing. If the prefix doesn't match
//! (op responses such as subscribe acks / pongs) the frame is reported as [`PublicFeed::Other`].

use hftbacktest::prelude::Side;
use serde::Deserialize;
use simd_json::Buffers;

use crate::{
    bybit::{
        BybitError,
        msg::{OrderBook, Trade},
    },
    utils::parse_depth,
};

/// A parsed `orderbook.*` frame, reduced to the fields the connector publishes.
pub struct OrderBookUpdate {
    pub symbol: String,
    pub cts: i64,
    pub bids: Vec<(f64, f64)>,
    pub asks: Vec<(f64, f64)>,
}

/// A single parsed `publicTrade` item, reduced to the fields the connector publishes.
pub struct TradeUpdate {
    pub symbol: String,
    pub side: Side,
    pub price: f64,
    pub size: f64,
    pub ts: i64,
}

/// The decoded public-stream frame.
pub enum PublicFeed {
    /// `orderbook.*` frame. `bbo` is true for the `orderbook.1.*` (best bid/offer) topic.
    OrderBook { bbo: bool, update: OrderBookUpdate },
    /// `publicTrade.*` frame.
    Trades(Vec<TradeUpdate>),
    /// Anything else (op responses such as subscribe acks / pongs, or unsubscribed topics).
    Other,
}

/// `orderbook.*` envelope; only the fields needed downstream are declared (others are ignored).
#[derive(Deserialize)]
struct OrderBookEnvelope {
    data: OrderBook,
    cts: Option<i64>,
}

/// `publicTrade.*` envelope.
#[derive(Deserialize)]
struct TradeEnvelope {
    data: Vec<Trade>,
}

/// Parses a Bybit public-stream text frame with simd-json, reusing `scratch` and `buffers`.
pub fn decode_public_feed(
    scratch: &mut Vec<u8>,
    buffers: &mut Buffers,
    text: &str,
) -> Result<PublicFeed, BybitError> {
    // Bybit always sends `topic` as the first field, so dispatch on the raw prefix before parsing.
    // The trailing dot in `orderbook.1.` ensures `orderbook.1000.` is treated as depth, not BBO.
    let bbo = text.starts_with("{\"topic\":\"orderbook.1.");
    let is_orderbook = bbo || text.starts_with("{\"topic\":\"orderbook");
    let is_trade = text.starts_with("{\"topic\":\"publicTrade");

    if !is_orderbook && !is_trade {
        return Ok(PublicFeed::Other);
    }

    // simd-json parses in place, so copy the immutable frame into the reusable scratch buffer.
    scratch.clear();
    scratch.extend_from_slice(text.as_bytes());

    if is_orderbook {
        let env: OrderBookEnvelope =
            simd_json::serde::from_slice_with_buffers(scratch.as_mut_slice(), buffers)?;
        let (bids, asks) = parse_depth(env.data.bids, env.data.asks)?;
        Ok(PublicFeed::OrderBook {
            bbo,
            update: OrderBookUpdate {
                symbol: env.data.symbol,
                cts: env.cts.unwrap_or_default(),
                bids,
                asks,
            },
        })
    } else {
        let env: TradeEnvelope =
            simd_json::serde::from_slice_with_buffers(scratch.as_mut_slice(), buffers)?;
        let trades = env
            .data
            .into_iter()
            .map(|t| TradeUpdate {
                symbol: t.symbol,
                side: t.side,
                price: t.trade_price,
                size: t.trade_size,
                ts: t.ts,
            })
            .collect();
        Ok(PublicFeed::Trades(trades))
    }
}
