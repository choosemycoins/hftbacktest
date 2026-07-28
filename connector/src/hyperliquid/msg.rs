//! Wire types for the Hyperliquid public WebSocket.
//!
//! Every inbound frame is `{"channel": <name>, "data": <payload>}` — except the keepalive
//! reply, which is `{"channel":"pong"}` with no `data` at all. That single exception is
//! why this parses in two steps (channel first, payload second) instead of deriving one
//! tagged enum: a `#[serde(tag = "channel", content = "data")]` enum rejects the pong.

use serde::Deserialize;

use crate::{hyperliquid::HyperliquidError, utils::from_str_to_f64};

/// One price level, as `l2Book` and `bbo` both encode it.
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct Level {
    #[serde(deserialize_with = "from_str_to_f64")]
    pub px: f64,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub sz: f64,
    /// Number of orders resting at this level. Unused here; kept because it is the only
    /// hint of queue shape the public feed carries.
    #[serde(default)]
    pub n: u32,
}

/// A complete top-N snapshot of one side pair. There is no sequence number and no diff:
/// each message supersedes the last (`AGENTS.md` §4.1a).
#[derive(Clone, Debug, Deserialize)]
pub struct L2Book {
    pub coin: String,
    /// Exchange time, milliseconds.
    pub time: i64,
    /// `[bids, asks]`; bids descend, asks ascend.
    pub levels: [Vec<Level>; 2],
    /// Echoed back by the venue when the subscription asked for `fast: true`.
    #[serde(default)]
    pub fast: Option<bool>,
}

/// The touch feed. Either side is typed nullable — a null means "no news about that
/// side", never "that side is empty".
#[derive(Clone, Debug, Deserialize)]
pub struct Bbo {
    pub coin: String,
    /// Exchange time, milliseconds.
    pub time: i64,
    /// `[bid, ask]`.
    pub bbo: [Option<Level>; 2],
}

/// One public fill.
#[derive(Clone, Debug, Deserialize)]
pub struct Trade {
    pub coin: String,
    /// Aggressor side: `"B"` a buyer, `"A"` a seller.
    pub side: String,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub px: f64,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub sz: f64,
    /// Exchange time, milliseconds.
    pub time: i64,
    /// The venue's fill id. Optional only defensively: it is the sole means of telling a
    /// replayed fill from a new one, and a fill without it cannot be deduplicated.
    #[serde(default)]
    pub tid: Option<u64>,
}

impl Trade {
    /// Whether the aggressor was a buyer.
    pub fn is_buy(&self) -> bool {
        self.side != "A"
    }
}

/// A parsed inbound frame.
#[derive(Debug)]
pub enum Frame {
    Bbo(Bbo),
    L2Book(L2Book),
    Trades(Vec<Trade>),
    /// The echoed subscription, as text — the only confirmation the venue understood it.
    SubscriptionResponse(String),
    Pong,
    /// The venue's own error text. The connection survives these.
    Error(String),
    /// A channel this backend does not handle, by name.
    Other(String),
}

#[derive(Deserialize)]
struct Envelope {
    channel: String,
    /// Absent on `{"channel":"pong"}`.
    #[serde(default)]
    data: Option<serde_json::Value>,
}

/// Parses one text frame.
///
/// Errors are for frames that could not be read at all; an unhandled channel is data, not
/// an error, because the venue adds channels over time and a hard failure on one would
/// take the whole market-data connection down.
pub fn parse_frame(text: &str) -> Result<Frame, HyperliquidError> {
    let envelope: Envelope = serde_json::from_str(text)?;
    let data = || -> Result<serde_json::Value, HyperliquidError> {
        envelope.data.clone().ok_or_else(|| {
            HyperliquidError::UniverseError(format!(
                "the {} frame carried no `data`",
                envelope.channel
            ))
        })
    };
    Ok(match envelope.channel.as_str() {
        "bbo" => Frame::Bbo(serde_json::from_value(data()?)?),
        "l2Book" => Frame::L2Book(serde_json::from_value(data()?)?),
        "trades" => Frame::Trades(serde_json::from_value(data()?)?),
        "subscriptionResponse" => Frame::SubscriptionResponse(data()?.to_string()),
        "pong" => Frame::Pong,
        "error" => Frame::Error(match data()? {
            serde_json::Value::String(message) => message,
            other => other.to_string(),
        }),
        _ => Frame::Other(envelope.channel),
    })
}

#[cfg(test)]
mod tests {
    use crate::hyperliquid::{
        fixtures::{
            BBO_BTC_2,
            ERROR_FRAME,
            L2BOOK_FAST_BTC_1,
            PONG,
            SUBSCRIPTION_RESPONSE,
            TRADES_BTC_REPLAY,
        },
        msg::{Frame, parse_frame},
    };

    #[test]
    fn an_l2_book_frame_parses_into_a_full_snapshot() {
        let Frame::L2Book(book) = parse_frame(L2BOOK_FAST_BTC_1).unwrap() else {
            panic!("expected an l2Book frame");
        };
        assert_eq!(book.coin, "BTC");
        assert_eq!(book.time, 1785251521889);
        // The venue echoes the `fast` flag, which is how a frame says which cadence it
        // came from — the two carry different depths and must not share a mirror.
        assert_eq!(book.fast, Some(true));

        let (bids, asks) = (&book.levels[0], &book.levels[1]);
        assert_eq!(bids.len(), 5);
        assert_eq!(asks.len(), 5);
        // Bids descend, asks ascend, prices and sizes arrive as strings.
        assert_eq!(bids[0].px, 63460.0);
        assert_eq!(bids[0].sz, 0.02666);
        assert_eq!(bids[4].px, 63429.0);
        assert_eq!(asks[0].px, 63488.0);
        assert!(bids[0].px < asks[0].px);
    }

    #[test]
    fn a_bbo_frame_parses_both_sides() {
        let Frame::Bbo(bbo) = parse_frame(BBO_BTC_2).unwrap() else {
            panic!("expected a bbo frame");
        };
        assert_eq!(bbo.coin, "BTC");
        assert_eq!(bbo.time, 1785251522090);
        let (bid, ask) = (bbo.bbo[0].as_ref().unwrap(), bbo.bbo[1].as_ref().unwrap());
        assert_eq!((bid.px, bid.sz), (63457.0, 0.00028));
        assert_eq!((ask.px, ask.sz), (63488.0, 0.01246));
    }

    /// The venue types both sides nullable. None was ever observed in a day of mainnet or
    /// in the testnet capture, but a null must mean "no news about that side" rather than
    /// "that side is empty", so it has to survive parsing to be distinguishable.
    #[test]
    fn a_null_bbo_side_parses_as_absent() {
        let text = r#"{"channel":"bbo","data":{"coin":"BTC","time":1,"bbo":[null,{"px":"2.0","sz":"3.0","n":1}]}}"#;
        let Frame::Bbo(bbo) = parse_frame(text).unwrap() else {
            panic!("expected a bbo frame");
        };
        assert!(bbo.bbo[0].is_none());
        assert_eq!(bbo.bbo[1].as_ref().unwrap().px, 2.0);
    }

    #[test]
    fn a_trades_frame_parses_side_price_size_and_tid() {
        let Frame::Trades(trades) = parse_frame(TRADES_BTC_REPLAY).unwrap() else {
            panic!("expected a trades frame");
        };
        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].coin, "BTC");
        assert_eq!(trades[0].px, 63719.0);
        assert_eq!(trades[0].sz, 0.00016);
        assert_eq!(trades[0].time, 1785251456343);
        assert_eq!(trades[0].tid, Some(447757798903687));
        // `side` is the aggressor: "B" a buyer, "A" a seller.
        assert!(trades[0].is_buy());
        let sell = r#"{"channel":"trades","data":[{"coin":"BTC","side":"A","px":"1.0","sz":"2.0","time":3,"tid":4}]}"#;
        let Frame::Trades(trades) = parse_frame(sell).unwrap() else {
            panic!("expected a trades frame");
        };
        assert!(!trades[0].is_buy());
    }

    /// The keepalive reply carries **no `data` field at all**. A parser that requires one
    /// treats every keepalive as malformed — and since the pong is the only positive
    /// evidence of liveness in a quiet market, that noise is exactly where it hurts.
    #[test]
    fn a_pong_frame_has_no_data_and_still_parses() {
        assert!(matches!(parse_frame(PONG).unwrap(), Frame::Pong));
    }

    /// An unknown subscription *type* produces this and leaves the connection up; an
    /// unknown *coin* closes the socket with no frame at all. Two failure modes for one
    /// class of mistake, so the frame must be surfaced rather than counted as data.
    #[test]
    fn an_error_frame_carries_the_venues_text() {
        let Frame::Error(message) = parse_frame(ERROR_FRAME).unwrap() else {
            panic!("expected an error frame");
        };
        assert!(message.contains("noSuchType"), "{message}");
    }

    #[test]
    fn a_subscription_ack_is_recognised() {
        let Frame::SubscriptionResponse(ack) = parse_frame(SUBSCRIPTION_RESPONSE).unwrap() else {
            panic!("expected a subscriptionResponse frame");
        };
        assert!(ack.contains("bbo"), "{ack}");
    }

    /// A channel this backend does not subscribe to must not be an error: the venue adds
    /// channels over time, and a hard failure on an unexpected one would take the whole
    /// market-data connection down with it.
    #[test]
    fn an_unhandled_channel_is_reported_not_fatal() {
        let Frame::Other(channel) = parse_frame(r#"{"channel":"candle","data":{}}"#).unwrap()
        else {
            panic!("expected an unhandled channel");
        };
        assert_eq!(channel, "candle");
    }

    /// Malformed JSON is an error, not a panic — the stream logs it and keeps reading.
    #[test]
    fn malformed_json_is_an_error() {
        assert!(parse_frame("{not json").is_err());
    }
}
