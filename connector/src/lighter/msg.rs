//! The public stream's wire shapes, and the one routing rule that matters.
//!
//! **A subscription is asked for as `order_book/36` and every frame comes back as
//! `order_book:36`** (design note §2.1). Routing on the request spelling produces a
//! connector that is connected, subscribed and publishing nothing, on a socket that answers
//! pings — so the response spelling is parsed here, once, and nowhere else.
//!
//! Everything the venue can put on this socket is a variant of [`Frame`], including the
//! frames this backend does not want: an unrecognised shape is data the venue chose to send
//! and is counted and logged, never a parse error and never a panic.

use serde::Deserialize;

use crate::lighter::LighterError;

/// One price level, exactly as the venue spells it.
///
/// Both fields are strings on this venue, in every channel. They are parsed to `f64` here
/// and never re-formatted: the published event carries the venue's own value rather than
/// one reconstructed from a tick index.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Level {
    pub px: f64,
    pub qty: f64,
}

/// The `order_book` payload, one level down from the envelope.
///
/// The chain lives **here**, not on the envelope: `begin_nonce` is a key of this object and
/// of nothing else the venue sends (`ticker` and `trade` carry a bare `nonce`). Reading the
/// envelope's `nonce` as a chain reports a break on every frame — design note §2.2.
#[derive(Debug)]
pub struct Book {
    pub market_id: i64,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
    /// The chain's high water mark for this frame.
    pub nonce: u64,
    /// What this frame chains from. `0` on a snapshot, which reseeds the chain.
    pub begin_nonce: u64,
    /// The engine's own stamp, in **microseconds** — not milliseconds. §4.4.
    pub last_updated_at_us: i64,
}

/// One print off the tape.
#[derive(Clone, Debug)]
pub struct Trade {
    pub market_id: i64,
    /// The dedup key. The venue replays the last 50 prints on **every** subscribe (§4.5).
    pub trade_id: u64,
    pub px: f64,
    pub qty: f64,
    /// True when the resting side was the ask, i.e. **the aggressor bought**.
    pub is_maker_ask: bool,
    /// Microseconds, like the book's `last_updated_at` and unlike its `timestamp`. §4.12.
    pub transaction_time_us: i64,
}

/// Everything the venue puts on the public socket.
#[derive(Debug)]
pub enum Frame {
    /// `subscribed/order_book` — the full book, sent exactly once per subscription.
    BookSnapshot(Book),
    /// `update/order_book` — a diff. Levels not named are unchanged; a level whose size is
    /// zero is gone.
    BookUpdate(Book),
    /// `subscribed/trade` (a replay of the last 50) or `update/trade`.
    Trades(Vec<Trade>),
    /// A subscription acknowledged, named by the channel in its **response** spelling.
    ///
    /// The only positively attributable evidence this venue offers that a subscription
    /// took: its refusals name neither channel nor market (§3.6).
    Subscribed {
        channel: String,
    },
    /// An unsubscribe acknowledged — the first half of a repair.
    Unsubscribed {
        channel: String,
    },
    /// `{"session_id":…,"type":"connected"}`, the first frame of every session.
    Connected,
    Pong,
    /// `{"error":{"code":…,"message":…}}`. Carries no channel and does not close the socket.
    VenueError {
        code: i64,
        message: String,
    },
    /// A frame this backend does not model. Counted and logged, never fatal.
    Other {
        label: String,
    },
}

/// The envelope every data frame shares.
#[derive(Deserialize)]
struct Envelope<'a> {
    #[serde(borrow, default)]
    r#type: Option<&'a str>,
    #[serde(borrow, default)]
    channel: Option<&'a str>,
    #[serde(default)]
    error: Option<VenueError>,
}

#[derive(Deserialize)]
struct VenueError {
    code: i64,
    #[serde(default)]
    message: String,
}

#[derive(Deserialize)]
struct RawBookFrame {
    order_book: RawBook,
}

#[derive(Deserialize)]
struct RawBook {
    #[serde(default)]
    bids: Vec<RawLevel>,
    #[serde(default)]
    asks: Vec<RawLevel>,
    nonce: u64,
    #[serde(default)]
    begin_nonce: u64,
    last_updated_at: i64,
}

#[derive(Deserialize)]
struct RawLevel {
    price: String,
    size: String,
}

#[derive(Deserialize)]
struct RawTradeFrame {
    #[serde(default)]
    trades: Vec<RawTrade>,
    /// A **second, disjoint** array of genuine prints: measured over a recorded CRV day,
    /// 454 `trades` and 6 `liquidation_trades` with zero ids in common. Dropping it would
    /// lose tape silently.
    #[serde(default)]
    liquidation_trades: Vec<RawTrade>,
}

#[derive(Deserialize)]
struct RawTrade {
    market_id: i64,
    trade_id: u64,
    price: String,
    size: String,
    is_maker_ask: bool,
    transaction_time: i64,
}

/// The market id out of a channel in its **response** spelling, `<channel>:<id>`.
///
/// `None` for a channel that names no single market (`height`, `market_stats:all`) and for
/// the request spelling, which must never reach here.
pub fn market_id_of(channel: &str) -> Option<i64> {
    channel.split_once(':')?.1.parse().ok()
}

/// Parses one frame off the socket.
///
/// Every well-formed JSON object is a [`Frame`]; only bytes that are not the venue's JSON
/// at all, or a body that contradicts its own `type`, are an error.
pub fn parse_frame(text: &str) -> Result<Frame, LighterError> {
    let envelope: Envelope = serde_json::from_str(text)?;

    if let Some(error) = envelope.error {
        return Ok(Frame::VenueError {
            code: error.code,
            message: error.message,
        });
    }

    let Some(kind) = envelope.r#type else {
        return Ok(Frame::Other {
            label: envelope.channel.unwrap_or("<no type>").to_string(),
        });
    };

    match kind {
        "connected" => return Ok(Frame::Connected),
        "pong" => return Ok(Frame::Pong),
        "unsubscribed" => {
            return Ok(Frame::Unsubscribed {
                channel: envelope.channel.unwrap_or_default().to_string(),
            });
        }
        _ => {}
    }

    let Some((prefix, channel_kind)) = kind.split_once('/') else {
        return Ok(Frame::Other {
            label: kind.to_string(),
        });
    };
    let channel = envelope.channel.unwrap_or_default();

    match (prefix, channel_kind) {
        ("subscribed" | "update", "order_book") => {
            let market_id = market_id_of(channel).ok_or_else(|| {
                LighterError::Frame(format!("an order_book frame with no market id: {channel}"))
            })?;
            let raw: RawBookFrame = serde_json::from_str(text)?;
            let book = Book {
                market_id,
                bids: levels(&raw.order_book.bids)?,
                asks: levels(&raw.order_book.asks)?,
                nonce: raw.order_book.nonce,
                begin_nonce: raw.order_book.begin_nonce,
                last_updated_at_us: raw.order_book.last_updated_at,
            };
            Ok(match prefix {
                "subscribed" => Frame::BookSnapshot(book),
                _ => Frame::BookUpdate(book),
            })
        }
        ("subscribed" | "update", "trade") => {
            let raw: RawTradeFrame = serde_json::from_str(text)?;
            let mut trades = Vec::with_capacity(raw.trades.len() + raw.liquidation_trades.len());
            for trade in raw.trades.iter().chain(raw.liquidation_trades.iter()) {
                trades.push(Trade {
                    market_id: trade.market_id,
                    trade_id: trade.trade_id,
                    px: parse_number(&trade.price)?,
                    qty: parse_number(&trade.size)?,
                    is_maker_ask: trade.is_maker_ask,
                    transaction_time_us: trade.transaction_time,
                });
            }
            Ok(Frame::Trades(trades))
        }
        // Every other subscription ack, including the channels this backend does not
        // subscribe: the tracker keys on the ack it asked for and ignores the rest.
        ("subscribed", _) => Ok(Frame::Subscribed {
            channel: channel.to_string(),
        }),
        _ => Ok(Frame::Other {
            label: kind.to_string(),
        }),
    }
}

fn levels(raw: &[RawLevel]) -> Result<Vec<Level>, LighterError> {
    raw.iter()
        .map(|level| {
            Ok(Level {
                px: parse_number(&level.price)?,
                qty: parse_number(&level.size)?,
            })
        })
        .collect()
}

/// Parses one of the venue's stringly-typed numbers.
///
/// Every price and size on this venue is a JSON string, in every channel, and they are the
/// only fields whose exact value this backend depends on. A field that cannot be read is an
/// error rather than a zero: a size silently read as zero is a level *deletion*.
fn parse_number(text: &str) -> Result<f64, LighterError> {
    text.parse()
        .map_err(|_| LighterError::Frame(format!("not a number: {text:?}")))
}

#[cfg(test)]
mod tests {
    use crate::lighter::{
        fixtures::{
            CONNECTED,
            ERROR_ALREADY_SUBSCRIBED,
            ERROR_UNKNOWN_MARKET,
            HEIGHT,
            ORDER_BOOK_SNAPSHOT_CRV,
            ORDER_BOOK_UPDATE_CRV_1,
            ORDER_BOOK_UPDATE_CRV_2,
            PONG,
            TICKER_BTC,
            TRADE_CRV,
            TRADE_REPLAY_CRV,
            UNSUBSCRIBED_ETH,
        },
        msg::{Frame, market_id_of, parse_frame},
    };

    fn book(text: &str) -> super::Book {
        match parse_frame(text).unwrap() {
            Frame::BookSnapshot(book) | Frame::BookUpdate(book) => book,
            other => panic!("expected a book frame, got {other:?}"),
        }
    }

    /// **The venue's sharpest trap** (design note §2.1). A subscription is asked for with a
    /// slash and every frame comes back with a colon. Routing on the request spelling gives
    /// a connector that subscribes, is served, and publishes nothing — on a socket that
    /// answers pings, so nothing anywhere says so.
    #[test]
    fn a_frames_market_is_read_from_the_colon_spelling() {
        assert_eq!(market_id_of("order_book:36"), Some(36));
        assert_eq!(market_id_of("trade:0"), Some(0));
        // The request spelling must not resolve: it is what a subscribe is *written* with.
        assert_eq!(market_id_of("order_book/36"), None);
        // Channels that name no single market.
        assert_eq!(market_id_of("height"), None);
        assert_eq!(market_id_of("market_stats:all"), None);

        assert_eq!(book(ORDER_BOOK_SNAPSHOT_CRV).market_id, 36);
    }

    /// A snapshot is the whole book and reseeds the chain with `begin_nonce = 0`; the first
    /// diff after it chains from its `nonce`. Both halves are real consecutive frames.
    #[test]
    fn a_snapshot_carries_the_whole_book_and_reseeds_the_chain() {
        let snapshot = book(ORDER_BOOK_SNAPSHOT_CRV);
        assert!(matches!(
            parse_frame(ORDER_BOOK_SNAPSHOT_CRV).unwrap(),
            Frame::BookSnapshot(_)
        ));
        assert_eq!((snapshot.bids.len(), snapshot.asks.len()), (39, 40));
        assert_eq!(snapshot.begin_nonce, 0);
        assert_eq!(snapshot.nonce, 18032383898);
        // Microseconds, not milliseconds — the trap in §4.4 is a factor of a thousand.
        assert_eq!(snapshot.last_updated_at_us, 1785358811946459);

        // Prices and sizes are strings on the wire and are parsed exactly.
        assert_eq!(snapshot.bids[0].px, 0.20351);
        assert_eq!(snapshot.bids[0].qty, 7673.9);
        assert_eq!(snapshot.asks[0].px, 0.20382);

        let first = book(ORDER_BOOK_UPDATE_CRV_1);
        assert!(matches!(
            parse_frame(ORDER_BOOK_UPDATE_CRV_1).unwrap(),
            Frame::BookUpdate(_)
        ));
        assert_eq!(first.begin_nonce, snapshot.nonce);
        assert_eq!(book(ORDER_BOOK_UPDATE_CRV_2).begin_nonce, first.nonce);
    }

    /// A diff names only the levels that changed, and `"size":"0.0"` is a deletion. The two
    /// are the same field, so a size that failed to parse and was read as zero would be an
    /// invented cancellation — which is why [`super::parse_number`] refuses instead.
    #[test]
    fn a_diff_carries_changed_levels_and_a_zero_size_is_a_deletion() {
        let diff = book(ORDER_BOOK_UPDATE_CRV_1);
        assert_eq!(diff.bids.len(), 3);
        assert_eq!(diff.asks.len(), 2);

        assert_eq!(diff.bids[0].px, 0.20351);
        assert_eq!(diff.bids[0].qty, 0.0, "the venue's spelling is \"0.0\"");
        assert_eq!(diff.asks[0].qty, 25374.8);
    }

    /// The tape, including the second array beside it. `liquidation_trades` holds genuine
    /// prints with ids disjoint from `trades` (measured over a recorded day: 454 and 6, zero
    /// in common), so it is published rather than dropped.
    #[test]
    fn both_trade_arrays_are_published() {
        let Frame::Trades(trades) = parse_frame(TRADE_CRV).unwrap() else {
            panic!("expected a trades frame");
        };
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].trade_id, 26412422124);
        assert_eq!(trades[0].market_id, 36);
        assert_eq!(trades[0].px, 0.20382);
        assert_eq!(trades[0].qty, 0.7);
        assert!(trades[0].is_maker_ask, "the aggressor bought");
        // Microseconds, like the book's engine stamp.
        assert_eq!(trades[0].transaction_time_us, 1785358835917189);

        let Frame::Trades(replay) = parse_frame(TRADE_REPLAY_CRV).unwrap() else {
            panic!("expected a trades frame");
        };
        assert_eq!(
            replay.len(),
            3,
            "two prints plus the liquidation beside them"
        );
        assert!(replay.iter().any(|t| t.trade_id == 26218621834));
    }

    /// Control frames are not data and are not parse failures. The venue's error frame is
    /// the sharp one: it carries neither `type` nor `channel`, so it can be recognised but
    /// never attributed (§3.6).
    #[test]
    fn control_frames_are_recognised_rather_than_refused() {
        assert!(matches!(parse_frame(CONNECTED).unwrap(), Frame::Connected));
        assert!(matches!(parse_frame(PONG).unwrap(), Frame::Pong));

        let Frame::Unsubscribed { channel } = parse_frame(UNSUBSCRIBED_ETH).unwrap() else {
            panic!("expected an unsubscribe ack");
        };
        assert_eq!(channel, "order_book:0");

        for (text, expected_code) in [
            (ERROR_UNKNOWN_MARKET, 30005),
            (ERROR_ALREADY_SUBSCRIBED, 30003),
        ] {
            let Frame::VenueError { code, message } = parse_frame(text).unwrap() else {
                panic!("expected a venue error: {text}");
            };
            assert_eq!(code, expected_code);
            assert!(!message.is_empty());
        }
    }

    /// A subscription ack is the **only** attributable evidence that a subscription took,
    /// because the refusals name nothing. It is read for every channel, in the response
    /// spelling.
    #[test]
    fn a_subscription_ack_names_the_channel_it_answers_for() {
        let Frame::Subscribed { channel } = parse_frame(
            TRADE_REPLAY_CRV
                .replace(
                    r#""type":"subscribed/trade""#,
                    r#""type":"subscribed/market_stats""#,
                )
                .as_str(),
        )
        .unwrap() else {
            panic!("expected a subscription ack");
        };
        assert_eq!(channel, "trade:36");

        // A book snapshot is an ack too, and is parsed as the book it is.
        assert!(matches!(
            parse_frame(ORDER_BOOK_SNAPSHOT_CRV).unwrap(),
            Frame::BookSnapshot(_)
        ));
    }

    /// **Venue-verbatim: an unmodelled frame is data, not an error.** Channels this backend
    /// does not subscribe still arrive — on a shared connection, after a relist, or because
    /// the venue added one — and every one of them must be skippable without a panic and
    /// without being mistaken for a book. `ticker` is the dangerous one: it carries a bare
    /// `nonce` and **no** `begin_nonce`, so reading it as a chain reports a break, and a
    /// break costs a repair.
    #[test]
    fn an_unmodelled_frame_is_skipped_rather_than_refused() {
        for text in [TICKER_BTC, HEIGHT] {
            let frame = parse_frame(text).unwrap();
            assert!(
                matches!(frame, Frame::Other { .. }),
                "{text} parsed as {frame:?}"
            );
        }
    }

    /// Bytes that are not the venue's JSON are the one thing that is an error — and an
    /// error, not a panic: the connector's panic hook is `exit(1)`, which would take every
    /// bot's market data down over one malformed frame.
    #[test]
    fn malformed_bytes_are_an_error_and_never_a_panic() {
        assert!(parse_frame("").is_err());
        assert!(parse_frame("not json").is_err());
        assert!(parse_frame(r#"{"type":"update/order_book","channel":"order_book:36"}"#).is_err());
        // A book frame whose channel names no market cannot be routed.
        assert!(
            parse_frame(&ORDER_BOOK_UPDATE_CRV_1.replace("order_book:36", "order_book")).is_err()
        );
        // A size that is not a number must not be read as zero — that is a level deletion.
        assert!(
            parse_frame(&ORDER_BOOK_UPDATE_CRV_1.replace(r#""size":"0.0""#, r#""size":"""#))
                .is_err()
        );
    }
}
