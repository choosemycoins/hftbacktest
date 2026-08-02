//! Wire types for the Hyperliquid WebSockets, public and private.
//!
//! Every inbound frame is `{"channel": <name>, "data": <payload>}` — except the keepalive
//! reply, which is `{"channel":"pong"}` with no `data` at all. That single exception is
//! why this parses in two steps (channel first, payload second) instead of deriving one
//! tagged enum: a `#[serde(tag = "channel", content = "data")]` enum rejects the pong.
//!
//! The private half ([`OrderUpdate`], [`UserFills`], [`map_order_status`]) is separated
//! only by which socket carries it; the parsing rules are the same, and so is the reason
//! they are pinned by fixtures — a field the venue renames shows up downstream as an
//! account that simply never seems to trade.

use hftbacktest::types::Status;
use serde::Deserialize;
use tracing::warn;

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

/// One order as `orderUpdates` describes it.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderState {
    pub coin: String,
    /// `"B"` a bid, `"A"` an ask.
    pub side: String,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub limit_px: f64,
    /// Remaining size. Zero on a terminal update.
    #[serde(deserialize_with = "from_str_to_f64")]
    pub sz: f64,
    /// Original size, so a partial fill is computable without the fill stream.
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub orig_sz: f64,
    pub oid: u64,
    /// The client id this connector set, if this order is one of ours. `null` for an order
    /// placed by a human or another system on the same account — those are ignored rather
    /// than treated as an unknown-order error (design note §5.4).
    #[serde(default)]
    pub cloid: Option<String>,
    #[serde(default)]
    pub timestamp: i64,
}

impl OrderState {
    pub fn is_buy(&self) -> bool {
        self.side != "A"
    }
}

/// One `orderUpdates` entry: an order plus the status it just reached.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderUpdate {
    pub order: OrderState,
    pub status: String,
    /// Venue time of the transition, milliseconds. The ordering key — updates can arrive
    /// out of order and a stale one must not overwrite a newer state.
    #[serde(default)]
    pub status_timestamp: i64,
}

/// One execution from `userFills`.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserFill {
    pub coin: String,
    /// **Our** side, not the aggressor's: `"B"` bought, `"A"` sold.
    pub side: String,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub px: f64,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub sz: f64,
    pub time: i64,
    pub oid: u64,
    /// The venue's fill id — a JSON **number**, unlike the public `trades` feed's, and the
    /// only way to tell a replayed fill from a new one.
    pub tid: u64,
    #[serde(default)]
    pub cloid: Option<String>,
    /// Signed position *before* this fill. Present on every fill, and the only in-band
    /// anchor for the position the account actually holds.
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub start_position: f64,
}

impl UserFill {
    pub fn is_buy(&self) -> bool {
        self.side != "A"
    }

    /// The signed size this fill moved the position by.
    pub fn signed_size(&self) -> f64 {
        if self.is_buy() { self.sz } else { -self.sz }
    }
}

/// The `userFills` payload.
///
/// **Not a bare array**, which is the shape the public `trades` channel uses and the shape
/// a reader coming from that code expects. `isSnapshot` marks the replay the venue sends on
/// every subscribe — see [`crate::hyperliquid::private_stream`] for why it must be consumed
/// but not double-applied.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserFills {
    #[serde(default)]
    pub is_snapshot: bool,
    #[serde(default)]
    pub user: String,
    pub fills: Vec<UserFill>,
}

/// A parsed inbound frame.
#[derive(Debug)]
pub enum Frame {
    Bbo(Bbo),
    L2Book(L2Book),
    Trades(Vec<Trade>),
    /// Order lifecycle. Authoritative for status (design note §5.5).
    OrderUpdates(Vec<OrderUpdate>),
    UserFills(UserFills),
    /// The echoed subscription, as text — the only confirmation the venue understood it.
    SubscriptionResponse(String),
    Pong,
    /// The venue's own error text. The connection survives these.
    Error(String),
    /// A channel this backend does not handle, by name.
    Other(String),
}

/// The venue's status string as an order status.
///
/// **Unknown means [`Status::Unsupported`], not terminal (Finding 3).** Hyperliquid documents
/// around thirty status strings and **adds to the set over time** — Hummingbot shipped a
/// production bug precisely by hardcoding the list and then meeting `perpMarginRejected`. An
/// unrecognised status cannot be assumed to mean "still resting" (a bot then quotes against
/// orders that no longer exist), but it must **not** be assumed terminal either: mapping it to
/// `Status::Expired` drops the order from the manager, and if it was in fact still resting the
/// strategy sees its price level free and places a *second* order — a double position. Both
/// are the uncertain-state trade `AGENTS.md` §1.1 forbids.
///
/// So the catch-all is `Status::Unsupported`: the order manager treats that as "keep the order
/// exactly as it is and escalate", rather than transitioning it to a terminal state that
/// removes it. Venue truth (periodic reconciliation) then resolves the order's real fate. See
/// [`crate::hyperliquid::ordermanager::OrderManager::apply_order_update`].
///
/// The suffix rules below are what keep that path rare rather than routine: every `…Canceled`
/// and every `…Rejected` the venue invents is classified correctly without a code change, and
/// only a genuinely new *kind* of status reaches the catch-all.
pub fn map_order_status(raw: &str) -> Status {
    match raw {
        // Still live.
        "open" => Status::New,
        // A trigger order that has become a live limit order. Not submitted by this
        // backend, but documented as live, so it must not be treated as terminal.
        "triggered" => Status::New,
        "filled" => Status::Filled,
        "canceled" | "cancelled" => Status::Canceled,
        "rejected" => Status::Rejected,
        other => {
            // The venue's own naming convention, used rather than a list that will go
            // stale: `marginCanceled`, `vaultWithdrawalCanceled`, `siblingFilledCanceled`,
            // `openInterestCapCanceled`, `scheduledCancel`, …
            if other.ends_with("Canceled")
                || other.ends_with("Cancelled")
                || other == "scheduledCancel"
            {
                warn!(status = %other, "Hyperliquid cancelled an order for a reason this backend does not name individually.");
                Status::Canceled
            } else if other.ends_with("Rejected") {
                warn!(status = %other, "Hyperliquid rejected an order for a reason this backend does not name individually.");
                Status::Rejected
            } else {
                warn!(
                    status = %other,
                    "An unrecognised Hyperliquid order status; keeping the order in its \
                     current state and leaving reconciliation to resolve it. Neither \
                     'still resting' nor 'gone' can be assumed (Finding 3, §1.1)."
                );
                Status::Unsupported
            }
        }
    }
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
        // The venue sends either an array or a single update; both have been reported in
        // production, and the single form would otherwise be a parse failure — i.e. an
        // order lifecycle transition dropped on the floor.
        "orderUpdates" => Frame::OrderUpdates(match data()? {
            value @ serde_json::Value::Array(_) => serde_json::from_value(value)?,
            single => vec![serde_json::from_value(single)?],
        }),
        "userFills" => Frame::UserFills(serde_json::from_value(data()?)?),
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
        msg::{Frame, map_order_status, parse_frame},
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

    // --- Private stream ------------------------------------------------------------

    #[test]
    fn an_order_update_carries_the_order_its_status_and_when() {
        use crate::hyperliquid::fixtures::{ORDER_UPDATE_CANCELED, ORDER_UPDATE_OPEN};

        let Frame::OrderUpdates(updates) = parse_frame(ORDER_UPDATE_OPEN).unwrap() else {
            panic!("expected an orderUpdates frame");
        };
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].status, "open");
        assert_eq!(updates[0].status_timestamp, 1785261289146);
        let order = &updates[0].order;
        assert_eq!(order.coin, "BTC");
        // A real venue id is past `u32`; anything narrower silently truncates it.
        assert_eq!(order.oid, 57118842019);
        assert_eq!(order.limit_px, 31795.0);
        assert_eq!(order.sz, 0.00037);
        assert_eq!(order.orig_sz, 0.00037);
        assert!(order.is_buy());
        assert_eq!(
            order.cloid.as_deref(),
            Some("0xa1f0cddc17303d07f3fd128d5b35071d")
        );

        // **A terminal update does not zero `sz`.** Measured: a fully unfilled order that
        // was cancelled still reports its whole size, and only `statusTimestamp` moves.
        // Inferring execution from `origSz - sz` therefore works — and a reader who assumed
        // the documented-looking opposite would have called this order fully filled.
        let Frame::OrderUpdates(updates) = parse_frame(ORDER_UPDATE_CANCELED).unwrap() else {
            panic!("expected an orderUpdates frame");
        };
        assert_eq!(updates[0].order.sz, 0.00037);
        assert_eq!(updates[0].order.orig_sz, 0.00037);
        assert_eq!(updates[0].status, "canceled");
        assert_eq!(
            updates[0].order.timestamp, 1785261289146,
            "creation, unchanged"
        );
        assert_eq!(updates[0].status_timestamp, 1785261290665, "the transition");
    }

    /// The venue sends either an array or a single object. A parser that only reads the
    /// array form treats the other as malformed — and an `orderUpdates` frame dropped is
    /// an order lifecycle transition the bot never learns about.
    #[test]
    fn a_single_order_update_parses_as_readily_as_an_array() {
        let single = r#"{"channel":"orderUpdates","data":{"order":{"coin":"ETH","side":"A","limitPx":"2000.5","sz":"1.5","oid":7,"origSz":"1.5","cloid":null,"timestamp":1},"status":"open","statusTimestamp":2}}"#;
        let Frame::OrderUpdates(updates) = parse_frame(single).unwrap() else {
            panic!("expected an orderUpdates frame");
        };
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].order.oid, 7);
        assert!(!updates[0].order.is_buy());
        // An order placed by a human or another system has no client id, and must survive
        // parsing so it can be *ignored* rather than reported as an unknown order.
        assert!(updates[0].order.cloid.is_none());
    }

    /// **`userFills` is an object, not an array.** The public `trades` channel is an array,
    /// so a parser written from that one rejects every fill this account ever makes — and
    /// the symptom is an account that appears never to trade.
    #[test]
    fn user_fills_is_an_object_carrying_the_snapshot_flag() {
        use crate::hyperliquid::fixtures::{
            USER_FILLS_ACK,
            USER_FILLS_EMPTY_SNAPSHOT,
            USER_FILLS_INCREMENT,
        };

        // The ack the venue answers a `userFills` subscribe with. It echoes fields the
        // request never carried (`aggregateByTime`), so an ack matched by exact equality
        // against what was sent would never match.
        let Frame::SubscriptionResponse(ack) = parse_frame(USER_FILLS_ACK).unwrap() else {
            panic!("expected a subscriptionResponse frame");
        };
        assert!(ack.contains("aggregateByTime"), "{ack}");

        let Frame::UserFills(fills) = parse_frame(USER_FILLS_EMPTY_SNAPSHOT).unwrap() else {
            panic!("expected a userFills frame");
        };
        assert!(fills.is_snapshot, "the subscribe replay must be marked");
        assert!(fills.fills.is_empty());
        // The venue lowercases the address it echoes; comparisons must not be exact.
        assert_eq!(fills.user, fills.user.to_lowercase());

        let Frame::UserFills(fills) = parse_frame(USER_FILLS_INCREMENT).unwrap() else {
            panic!("expected a userFills frame");
        };
        assert!(!fills.is_snapshot);
        let fill = &fills.fills[0];
        assert_eq!(fill.coin, "BTC");
        assert_eq!(fill.oid, 57118842019);
        // A JSON number, not a string — unlike almost every other quantity on this venue.
        assert_eq!(fill.tid, 123456789);
        assert_eq!(fill.px, 31795.0);
        assert_eq!(fill.signed_size(), 0.00016);
        assert_eq!(fill.start_position, 0.0);

        // `side` here is *ours*, so a sell is a negative move in the position.
        let sold = r#"{"channel":"userFills","data":{"isSnapshot":false,"user":"0x1","fills":[{"coin":"BTC","px":"1.0","sz":"2.0","side":"A","time":1,"oid":2,"tid":3,"startPosition":"5.0"}]}}"#;
        let Frame::UserFills(fills) = parse_frame(sold).unwrap() else {
            panic!("expected a userFills frame");
        };
        assert_eq!(fills.fills[0].signed_size(), -2.0);
    }

    /// **The Hummingbot bug, pinned.** The venue documents around thirty status strings and
    /// keeps adding to them; hardcoding the list shipped a production failure when
    /// `perpMarginRejected` appeared. The suffix rules classify the whole family, and anything
    /// genuinely new maps to [`Status::Unsupported`] — the sentinel the order manager keeps
    /// (never drops) and escalates on, so a still-resting order is never silently forgotten
    /// (Finding 3, `AGENTS.md` §1.1).
    #[test]
    fn every_status_family_maps_and_an_invented_one_is_unsupported() {
        use hftbacktest::types::Status;

        assert_eq!(map_order_status("open"), Status::New);
        assert_eq!(map_order_status("filled"), Status::Filled);
        assert_eq!(map_order_status("canceled"), Status::Canceled);
        assert_eq!(map_order_status("cancelled"), Status::Canceled);
        assert_eq!(map_order_status("rejected"), Status::Rejected);

        // Documented in the venue's list today, and none of them are named individually
        // above — the suffix is what classifies them.
        for cancelled in [
            "marginCanceled",
            "vaultWithdrawalCanceled",
            "openInterestCapCanceled",
            "selfTradeCanceled",
            "reduceOnlyCanceled",
            "siblingFilledCanceled",
            "delistedCanceled",
            "liquidatedCanceled",
            "scheduledCancel",
        ] {
            assert_eq!(map_order_status(cancelled), Status::Canceled, "{cancelled}");
        }
        for rejected in [
            "tickRejected",
            "minTradeNtlRejected",
            "perpMarginRejected",
            "reduceOnlyRejected",
            "badAloPxRejected",
            "iocCancelRejected",
            "badTriggerPxRejected",
            "marketOrderNoLiquidityRejected",
            "oracleRejected",
            "perpMaxPositionRejected",
        ] {
            assert_eq!(map_order_status(rejected), Status::Rejected, "{rejected}");
        }

        // A trigger order becoming live is documented as *still resting*, so it must not be
        // swept up as terminal — that would drop a live order from the manager.
        assert_eq!(map_order_status("triggered"), Status::New);

        // …and a status nobody has seen before is `Unsupported` — neither "still resting"
        // (which would quote against a dead order) nor terminal (which would drop an order
        // that may still be live). The order manager keeps it and escalates; §1.1 fail closed.
        for invented in ["somethingEntirelyNew", "", "OPEN", "Filled"] {
            assert_eq!(
                map_order_status(invented),
                Status::Unsupported,
                "{invented:?}"
            );
        }
    }
}
