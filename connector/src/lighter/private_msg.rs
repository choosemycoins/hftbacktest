//! The private account channels' wire shapes (design note
//! [`docs/design-lighter-connector.md`](../../../docs/design-lighter-connector.md) §3.4), and
//! the three traps that live inside their bodies (§4.12–§4.14).
//!
//! Three channels feed the order path: `account_all_orders` (auth required — a subscribe with
//! no token is refused `20001`), `account_all_positions`, and `account_all_trades`. The first
//! two are all the order state machine needs — an order's fills show up on
//! `account_all_orders` as `filled_base_amount`/`remaining_base_amount` moving, and the
//! position is stated directly on `account_all_positions` and is **never** reconstructed by
//! replaying fills (§3.4).
//!
//! What this module refuses to invent: the **delta** shape of `account_all_trades`. Its
//! snapshot is always empty and no fill was probed in Phase 0, so the fill-field layout is
//! unmeasured (§6.Б.3). A trades frame is recognised and counted, never decoded into typed
//! fill fields — guessing money fields is exactly what fails closed forbids. The realized
//! maker fee (the §6.Б self-cross) is read in the live-testnet phase once that body is
//! captured.
//!
//! ### The traps, encoded by what this parser does and does not declare
//!
//! * **§4.13 field drift.** The same order's `integrator_fee_collector_index` /
//!   `integrator_taker_fee` / `integrator_maker_fee` are `""` in a snapshot and `"0"` in a
//!   delta. A serde struct that typed them as numbers would lose the whole snapshot on the
//!   reconnect path. They are simply **not declared** — serde ignores them — so neither
//!   spelling can break the parse. The pin feeds a real snapshot *and* a real delta through
//!   and requires both.
//! * **§4.13 side.** `side` is always the empty string; the side is [`AccountOrder::is_ask`].
//! * **§4.12 ordering.** `updated_at`/`created_at`/`timestamp` are block-granular seconds and
//!   non-monotone across a state change — the only usable key is
//!   [`AccountOrder::transaction_time_us`] (microseconds).
//! * **§4.14 nonce.** The in-order `nonce` (~2^48) is the order's internal id, not the slot's
//!   tx-nonce. It is not declared here; the order path keys on `client_order_index`.

use std::collections::HashMap;

use serde::Deserialize;

use crate::lighter::LighterError;

/// One order as `account_all_orders` states it — only the fields the order path keys on.
#[derive(Clone, Debug, PartialEq)]
pub struct AccountOrder {
    pub market_index: i64,
    /// Our client order index (§3.5), the u64 order-id mapping's venue-side handle.
    pub client_order_index: i64,
    /// The venue's own internal order id (the `order_index`/`order_id` pair, ~2^48). What a
    /// cancel is addressed by, and what reconciliation matches on.
    pub order_index: i64,
    /// The side (§4.13: `side` is always `""`, so this is authoritative).
    pub is_ask: bool,
    /// `"open" | "canceled" | "filled" | ...` — mapped to a `Status` by the order manager.
    pub status: String,
    pub price: f64,
    pub initial_base_amount: f64,
    pub remaining_base_amount: f64,
    pub filled_base_amount: f64,
    /// **The only monotone ordering key** (§4.12), microseconds. Never order by `updated_at`.
    pub transaction_time_us: i64,
}

/// One account position as `account_all_positions` states it.
#[derive(Clone, Debug, PartialEq)]
pub struct AccountPosition {
    pub market_index: i64,
    pub symbol: String,
    /// **Signed** size: `sign` (±1) × `position` magnitude (§3.4). The source of truth for
    /// the position — never inferred from fills.
    pub position: f64,
    /// How many orders the venue counts as this account's on this market — a cheap
    /// reconciliation invariant the connector can cross-check against its own map (§3.4).
    pub open_order_count: i64,
}

/// Everything the venue puts on the private socket.
#[derive(Debug)]
pub enum PrivateFrame {
    /// `{"session_id":…,"type":"connected"}`, the first frame.
    Connected,
    /// A subscription acknowledged — the positive evidence the channel is live (§3.6). The
    /// snapshot frames double as this, so it is only for channels with no data snapshot.
    Subscribed { channel: String },
    /// `subscribed/` (snapshot) or `update/` (delta) `account_all_orders`. `snapshot` is true
    /// for the full state a (re)subscribe sends, which the reconnect reconciliation keys on.
    Orders {
        snapshot: bool,
        orders: Vec<AccountOrder>,
    },
    Positions {
        snapshot: bool,
        positions: Vec<AccountPosition>,
    },
    /// `account_all_trades`. **Not decoded** beyond a count — the delta body is unmeasured
    /// (§6.Б.3). `count` is how many trade entries the frame carried (0 for the always-empty
    /// snapshot).
    Trades { snapshot: bool, count: usize },
    /// `{"error":{"code":…,"message":…}}`. `20001` = auth required, `20013` = invalid auth.
    VenueError { code: i64, message: String },
    /// A frame this backend does not model. Counted and logged, never fatal.
    Other { label: String },
}

#[derive(Deserialize)]
struct Envelope<'a> {
    #[serde(borrow, default)]
    r#type: Option<&'a str>,
    #[serde(borrow, default)]
    channel: Option<&'a str>,
    #[serde(default)]
    error: Option<RawError>,
}

#[derive(Deserialize)]
struct RawError {
    code: i64,
    #[serde(default)]
    message: String,
}

#[derive(Deserialize)]
struct RawOrdersFrame {
    #[serde(default)]
    orders: HashMap<String, Vec<RawOrder>>,
}

/// Only the fields the order path reads. Everything else on the wire — `integrator_*` (whose
/// type drifts, §4.13), `side` (always `""`), the in-order `nonce` (§4.14), `updated_at`
/// (§4.12) — is deliberately absent so serde skips it and no drift can break the parse.
#[derive(Deserialize)]
struct RawOrder {
    market_index: i64,
    client_order_index: i64,
    order_index: i64,
    is_ask: bool,
    status: String,
    price: String,
    initial_base_amount: String,
    remaining_base_amount: String,
    filled_base_amount: String,
    transaction_time: i64,
}

#[derive(Deserialize)]
struct RawPositionsFrame {
    #[serde(default)]
    positions: HashMap<String, RawPosition>,
}

#[derive(Deserialize)]
struct RawPosition {
    market_id: i64,
    #[serde(default)]
    symbol: String,
    #[serde(default)]
    sign: i64,
    position: String,
    #[serde(default)]
    open_order_count: i64,
}

#[derive(Deserialize)]
struct RawTradesFrame {
    #[serde(default)]
    trades: HashMap<String, Vec<serde_json::Value>>,
}

/// What a venue error frame on the private socket is about (§4.17). Its code is all the
/// structure it has: the frame names neither a channel nor an order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VenueErrorScope {
    /// A transaction the account sent: the `21xxx` application codes.
    Order,
    /// The account's session or subscriptions — or a code not known to be about a transaction.
    Account,
}

/// The scope of a private-socket error code.
///
/// `21xxx` are the venue's application codes for a transaction — the two measured here are
/// `21104` invalid nonce and `21728` duplicate client order index (§2.7, §4.11). `20xxx` is
/// auth and parameters (`20001` auth required, `20013` invalid auth), `30xxx` subscriptions
/// (`30003` already subscribed, `30005` invalid channel). The range is read off those codes,
/// not off a published table — the venue publishes none — so it is narrow on purpose: only
/// `21xxx` is about a transaction, and every other code, unknown ones included, is about the
/// account. Fail closed: misreading an account-level error as one order's would hide it from
/// the critical breaker it belongs to.
pub fn venue_error_scope(code: i64) -> VenueErrorScope {
    if (21_000..22_000).contains(&code) {
        VenueErrorScope::Order
    } else {
        VenueErrorScope::Account
    }
}

/// `20001` auth required: a subscription was refused for want of a token.
pub const AUTH_REQUIRED: i64 = 20001;
/// `20013` invalid auth: the token was refused — a bad deadline, a revoked key.
pub const INVALID_AUTH: i64 = 20013;
/// `30003` already subscribed: the subscription it answers is in place.
pub const ALREADY_SUBSCRIBED: i64 = 30003;

/// Whether an error frame leaves the private socket serving nothing, so that only a reconnect —
/// a fresh token, fresh subscriptions — recovers it (§4.17). `snapshotted` is whether this
/// connection's `account_all_orders` snapshot has arrived: the evidence its subscriptions took.
///
/// * [`AUTH_REQUIRED`] / [`INVALID_AUTH`] — the subscription or the token was refused, and the
///   socket answers pings while carrying no fill, cancel or position: always.
/// * [`ALREADY_SUBSCRIBED`], and every `21xxx` ([`VenueErrorScope::Order`]) — never: the
///   subscription is in place; one transaction's business.
/// * Anything else, unknown codes included — only **before** the snapshot. Until then nothing
///   shows the subscriptions took, and a socket that never snapshots refuses every order until
///   the ~7.5 h token refresh. After it the account stream is demonstrably serving, and a
///   reconnect on a code the venue repeats costs, every time, a token mint and a catalog read,
///   a `ConnectionInterrupted` to every bot and — under `connect_policy = "cancel_all"` — a
///   sweep of every market with orders: about every 30 s, for as long as the venue says it.
///   The bots are told such a code either way (it is [`VenueErrorScope::Account`]).
pub fn venue_error_ends_connection(code: i64, snapshotted: bool) -> bool {
    match code {
        AUTH_REQUIRED | INVALID_AUTH => true,
        ALREADY_SUBSCRIBED => false,
        _ if venue_error_scope(code) == VenueErrorScope::Order => false,
        _ => !snapshotted,
    }
}

/// Parses one frame off the private socket.
///
/// Like the public parser, every well-formed JSON object is a [`PrivateFrame`]; only bytes
/// that are not JSON, or a body that contradicts its own `type`, are an error — the
/// connector's panic hook is `exit(1)`, so a single odd frame must never be a panic.
pub fn parse_private_frame(text: &str) -> Result<PrivateFrame, LighterError> {
    let envelope: Envelope = serde_json::from_str(text)?;

    if let Some(error) = envelope.error {
        return Ok(PrivateFrame::VenueError {
            code: error.code,
            message: error.message,
        });
    }

    let Some(kind) = envelope.r#type else {
        return Ok(PrivateFrame::Other {
            label: envelope.channel.unwrap_or("<no type>").to_string(),
        });
    };

    if kind == "connected" {
        return Ok(PrivateFrame::Connected);
    }

    let Some((prefix, channel_kind)) = kind.split_once('/') else {
        return Ok(PrivateFrame::Other {
            label: kind.to_string(),
        });
    };
    // `subscribed/` is the full state a (re)subscribe sends; `update/` is a delta.
    let snapshot = prefix == "subscribed";

    match channel_kind {
        "account_all_orders" => {
            let raw: RawOrdersFrame = serde_json::from_str(text)?;
            Ok(PrivateFrame::Orders {
                snapshot,
                orders: account_orders(raw.orders)?,
            })
        }
        "account_all_positions" => {
            let raw: RawPositionsFrame = serde_json::from_str(text)?;
            Ok(PrivateFrame::Positions {
                snapshot,
                positions: account_positions(raw.positions)?,
            })
        }
        "account_all_trades" => {
            let raw: RawTradesFrame = serde_json::from_str(text)?;
            Ok(PrivateFrame::Trades {
                snapshot,
                count: raw.trades.values().map(Vec::len).sum(),
            })
        }
        // Any other subscription ack is positive subscribe evidence for a channel that has no
        // data snapshot of its own; anything else is a frame this backend does not model.
        _ if snapshot => Ok(PrivateFrame::Subscribed {
            channel: envelope.channel.unwrap_or_default().to_string(),
        }),
        _ => Ok(PrivateFrame::Other {
            label: kind.to_string(),
        }),
    }
}

fn account_orders(raw: HashMap<String, Vec<RawOrder>>) -> Result<Vec<AccountOrder>, LighterError> {
    account_orders_from(raw.into_values().flatten())
}

/// Maps raw order rows into [`AccountOrder`]s — shared by the WS frame (a map of arrays) and
/// the REST `accountActiveOrders` mirror (a flat array), so the two paths cannot diverge in how
/// they read the venue's stringly-typed amounts (§4.13).
fn account_orders_from(
    raw: impl IntoIterator<Item = RawOrder>,
) -> Result<Vec<AccountOrder>, LighterError> {
    let mut orders = Vec::new();
    for order in raw {
        orders.push(AccountOrder {
            market_index: order.market_index,
            client_order_index: order.client_order_index,
            order_index: order.order_index,
            is_ask: order.is_ask,
            status: order.status,
            price: parse_number(&order.price)?,
            initial_base_amount: parse_number(&order.initial_base_amount)?,
            remaining_base_amount: parse_number(&order.remaining_base_amount)?,
            filled_base_amount: parse_number(&order.filled_base_amount)?,
            transaction_time_us: order.transaction_time,
        });
    }
    Ok(orders)
}

/// The `GET /api/v1/accountActiveOrders` envelope: the venue's `code`, an optional error
/// `message`, and a **flat** `orders` array (the WS channel keys by market-index string; the
/// REST mirror does not). The per-order rows are the same [`RawOrder`], so the §4.13 field
/// drift is tolerated identically.
#[derive(Deserialize)]
struct RestActiveOrders {
    #[serde(default)]
    code: i64,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    orders: Vec<RawOrder>,
}

/// Parses a `GET /api/v1/accountActiveOrders` body into [`AccountOrder`]s — the REST mirror of
/// `account_all_orders` the slot reads an ambiguous send's fate off (§3.3). A `code 200` body
/// yields its orders (possibly empty — the venue positively holds nothing); any other code is
/// an [`LighterError::Frame`], **never** an empty list, because "could not check" and "no
/// orders" must not collapse: reading a `20001` auth-required as "no orders" would expire a
/// possibly-resting order (§1.1). Requires auth on the wire exactly like the WS subscribe.
pub fn parse_active_orders(body: &str) -> Result<Vec<AccountOrder>, LighterError> {
    let resp: RestActiveOrders = serde_json::from_str(body)?;
    if resp.code != 200 {
        return Err(LighterError::Frame(format!(
            "accountActiveOrders returned code {} ({})",
            resp.code,
            resp.message.as_deref().unwrap_or("no message")
        )));
    }
    account_orders_from(resp.orders)
}

fn account_positions(
    raw: HashMap<String, RawPosition>,
) -> Result<Vec<AccountPosition>, LighterError> {
    let mut positions = Vec::new();
    for position in raw.into_values() {
        let magnitude = parse_number(&position.position)?;
        // Signed by `sign` (±1). §3.4: the venue states the position; the connector never
        // rebuilds it from fills.
        let signed = if position.sign < 0 {
            -magnitude
        } else {
            magnitude
        };
        positions.push(AccountPosition {
            market_index: position.market_id,
            symbol: position.symbol,
            position: signed,
            open_order_count: position.open_order_count,
        });
    }
    Ok(positions)
}

/// One of the venue's stringly-typed numbers. An unparseable one is an error, never a zero: a
/// `remaining_base_amount` silently read as zero would look like a fully consumed order.
fn parse_number(text: &str) -> Result<f64, LighterError> {
    text.parse()
        .map_err(|_| LighterError::Frame(format!("not a number: {text:?}")))
}

#[cfg(test)]
mod tests {
    use super::{PrivateFrame, parse_private_frame};
    use crate::lighter::fixtures::{
        CONNECTED,
        PRIVATE_ERROR_AUTH_REQUIRED,
        PRIVATE_ORDERS_SNAPSHOT,
        PRIVATE_ORDERS_UPDATE_CANCELED,
        PRIVATE_POSITIONS_UPDATE_FLAT,
        PRIVATE_TRADES_SNAPSHOT_EMPTY,
        REST_ACTIVE_ORDERS_AUTH_REQUIRED,
        REST_ACTIVE_ORDERS_EMPTY,
        REST_ACTIVE_ORDERS_ONE,
    };

    fn orders(text: &str) -> (bool, Vec<super::AccountOrder>) {
        match parse_private_frame(text).unwrap() {
            PrivateFrame::Orders { snapshot, orders } => (snapshot, orders),
            other => panic!("expected an orders frame, got {other:?}"),
        }
    }

    /// **§4.13, the dearest trap: the same order parses from both a snapshot and a delta.**
    /// The snapshot's `integrator_*` are `""` and the delta's are `"0"`; a parser that typed
    /// them as numbers would lose the whole snapshot on reconnect. Both frames are real bytes
    /// off testnet for one order (COI 424242424242).
    #[test]
    fn the_same_order_parses_from_a_snapshot_and_a_delta_despite_field_drift() {
        let (snapshot, snap) = orders(PRIVATE_ORDERS_SNAPSHOT);
        assert!(snapshot, "subscribed/ is the snapshot");
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].client_order_index, 424242424242);
        assert_eq!(snap[0].order_index, 844424914280027);
        assert_eq!(snap[0].status, "open");
        assert!(
            !snap[0].is_ask,
            "side lives in is_ask; `side` is always empty"
        );
        assert_eq!(snap[0].price, 58300.0);
        assert_eq!(snap[0].remaining_base_amount, 0.001);

        let (snapshot, delta) = orders(PRIVATE_ORDERS_UPDATE_CANCELED);
        assert!(!snapshot, "update/ is a delta");
        assert_eq!(delta[0].client_order_index, 424242424242);
        assert_eq!(delta[0].status, "canceled");
        assert_eq!(delta[0].remaining_base_amount, 0.0);
    }

    /// **§4.12: `transaction_time` is the only monotone ordering key.** The canceled delta's
    /// `transaction_time` is strictly greater than the open snapshot's, even though the two
    /// share a block and their `updated_at` would reorder them.
    #[test]
    fn transaction_time_is_monotone_where_updated_at_is_not() {
        let (_, snap) = orders(PRIVATE_ORDERS_SNAPSHOT);
        let (_, delta) = orders(PRIVATE_ORDERS_UPDATE_CANCELED);
        assert_eq!(snap[0].transaction_time_us, 1785431774184833);
        assert_eq!(delta[0].transaction_time_us, 1785431906364320);
        assert!(
            delta[0].transaction_time_us > snap[0].transaction_time_us,
            "the cancel must sort after the open, and only transaction_time does that"
        );
    }

    /// The position comes off `account_all_positions`, signed, with the open-order count that
    /// is a cheap reconciliation cross-check (§3.4). Real flat-account bytes.
    #[test]
    fn a_position_frame_carries_a_signed_size_and_the_open_order_count() {
        match parse_private_frame(PRIVATE_POSITIONS_UPDATE_FLAT).unwrap() {
            PrivateFrame::Positions {
                snapshot,
                positions,
            } => {
                assert!(!snapshot);
                assert_eq!(positions.len(), 1);
                assert_eq!(positions[0].market_index, 1);
                assert_eq!(positions[0].symbol, "BTC");
                assert_eq!(positions[0].position, 0.0);
                assert_eq!(positions[0].open_order_count, 0);
            }
            other => panic!("expected a positions frame, got {other:?}"),
        }
    }

    /// A short position is `sign = -1` × magnitude. No non-zero position was captured in Phase
    /// 0 (no fill was taken), so this is a synthetic body exercising the sign arithmetic only;
    /// the field names and shape are the real ones.
    #[test]
    fn a_short_position_is_negative() {
        let short = PRIVATE_POSITIONS_UPDATE_FLAT
            .replace(r#""sign":1"#, r#""sign":-1"#)
            .replace(r#""position":"0.00000""#, r#""position":"1.50000""#);
        match parse_private_frame(&short).unwrap() {
            PrivateFrame::Positions { positions, .. } => {
                assert_eq!(positions[0].position, -1.5);
            }
            other => panic!("expected a positions frame, got {other:?}"),
        }
    }

    /// The trades snapshot is always empty and is recognised without decoding fill fields —
    /// the delta body is unmeasured (§6.Б.3), and inventing money fields is forbidden.
    #[test]
    fn a_trades_snapshot_is_recognised_and_empty() {
        match parse_private_frame(PRIVATE_TRADES_SNAPSHOT_EMPTY).unwrap() {
            PrivateFrame::Trades { snapshot, count } => {
                assert!(snapshot);
                assert_eq!(count, 0);
            }
            other => panic!("expected a trades frame, got {other:?}"),
        }
    }

    /// **The REST `accountActiveOrders` mirror parses into the same [`AccountOrder`] the WS
    /// channel yields** (§3.3): the slot reads an ambiguous send's fate off this list. A `code
    /// 200` body with an `orders` array yields the orders (keyed by our `client_order_index`,
    /// with the venue's `order_index`); the same §4.13 `integrator_*` drift is tolerated because
    /// the fields are not declared. A non-200 body (e.g. `20001` auth-required) is an `Err`, not
    /// an empty list — an empty list would be read as "the order never landed" and wrongly
    /// expire a possibly-resting order (§1.1).
    #[test]
    fn the_rest_active_orders_mirror_parses_into_account_orders() {
        let orders = super::parse_active_orders(REST_ACTIVE_ORDERS_ONE).expect("a code-200 body");
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].client_order_index, 178554256601);
        assert_eq!(orders[0].order_index, 844424912956320);
        assert_eq!(orders[0].status, "open");
        assert!(!orders[0].is_ask);
        assert_eq!(orders[0].price, 56608.2);
        assert_eq!(orders[0].remaining_base_amount, 0.001);
        // `transaction_time` is microseconds, like the WS channel (§4.12) — confirmed live.
        assert_eq!(orders[0].transaction_time_us, 1785542568396486);
    }

    /// A flat account's `accountActiveOrders` is `code 200` with an empty array — the venue
    /// holds nothing. That is `Ok(empty)`, distinct from a failed check: an ambiguous order
    /// against a positively-empty list never landed and is safe to expire.
    #[test]
    fn a_flat_rest_active_orders_body_is_ok_and_empty() {
        let orders = super::parse_active_orders(REST_ACTIVE_ORDERS_EMPTY).expect("code 200");
        assert!(orders.is_empty());
    }

    /// A non-200 `accountActiveOrders` body (auth required, `20001`) is an `Err` — the caller
    /// must read it as "could not check", never as "no orders". Fail closed (§3.3/§1.1).
    #[test]
    fn a_non_200_rest_active_orders_body_is_an_error_not_an_empty_list() {
        assert!(super::parse_active_orders(REST_ACTIVE_ORDERS_AUTH_REQUIRED).is_err());
    }

    /// Auth is required on `account_all_orders`, and its absence is `20001` — a code the
    /// connector has to recognise (§3.4). The error frame names neither type nor channel.
    #[test]
    fn an_auth_required_error_is_read_as_a_venue_error() {
        match parse_private_frame(PRIVATE_ERROR_AUTH_REQUIRED).unwrap() {
            PrivateFrame::VenueError { code, message } => {
                assert_eq!(code, 20001);
                assert!(message.contains("auth"), "{message}");
            }
            other => panic!("expected a venue error, got {other:?}"),
        }
    }

    /// **A private-socket error is about a transaction only when its code says so; anything
    /// else is about the account — and so is a code nobody has seen** (§4.17). `21xxx` are the
    /// application codes measured on transactions (`21104` invalid nonce, `21728` duplicate
    /// client order index); `20xxx` is auth and parameters (`20001`, `20013`), `30xxx`
    /// subscriptions (`30003`, `30005`).
    #[test]
    fn a_private_error_is_about_a_transaction_only_by_its_code() {
        use super::{VenueErrorScope, venue_error_scope};
        for code in [21104, 21728, 21000, 21999] {
            assert_eq!(venue_error_scope(code), VenueErrorScope::Order, "{code}");
        }
        for code in [20001, 20013, 30003, 30005, 0, -1, 22000, 99_999, 429] {
            assert_eq!(venue_error_scope(code), VenueErrorScope::Account, "{code}");
        }
    }

    /// **Only an error that kills the session ends the connection — and before the snapshot,
    /// anything about the account does** (§4.17). A refused subscription (`20001`) or token
    /// (`20013`) leaves a socket that answers pings and carries nothing: always a reconnect.
    /// `30003` already subscribed says the subscription is in place, and a `21xxx` is one
    /// transaction's business: never. Any other code — unknown ones included — ends the
    /// connection only while nothing has shown the subscriptions took; once this connection's
    /// `account_all_orders` snapshot has arrived the account stream is demonstrably serving, and
    /// a reconnect on a code the venue repeats would cost a token mint and catalog read, a
    /// `ConnectionInterrupted` to every bot and, under `cancel_all`, a sweep of every market
    /// with orders — about every 30 s, for as long as the venue keeps saying it.
    #[test]
    fn only_a_refused_token_or_subscription_ends_a_connection_that_has_snapshotted() {
        use super::venue_error_ends_connection;
        for snapshotted in [false, true] {
            for code in [20001, 20013] {
                assert!(
                    venue_error_ends_connection(code, snapshotted),
                    "{code}, snapshotted: {snapshotted}"
                );
            }
            for code in [30003, 21104, 21728] {
                assert!(
                    !venue_error_ends_connection(code, snapshotted),
                    "{code}, snapshotted: {snapshotted}"
                );
            }
        }
        for code in [50000, 30005, 20002, 0, -1, 99_999] {
            assert!(
                venue_error_ends_connection(code, false),
                "{code} before the snapshot: nothing says the subscriptions took"
            );
            assert!(
                !venue_error_ends_connection(code, true),
                "{code} after the snapshot: the account stream is serving"
            );
        }
    }

    /// Control frames are recognised, and malformed bytes are an error, never a panic.
    #[test]
    fn control_frames_are_recognised_and_garbage_is_an_error_not_a_panic() {
        assert!(matches!(
            parse_private_frame(CONNECTED).unwrap(),
            PrivateFrame::Connected
        ));
        assert!(parse_private_frame("").is_err());
        assert!(parse_private_frame("not json").is_err());
        // An orders frame whose amount is not a number must not be read as zero.
        assert!(
            parse_private_frame(&PRIVATE_ORDERS_SNAPSHOT.replace(
                r#""remaining_base_amount":"0.00100""#,
                r#""remaining_base_amount":"oops""#
            ))
            .is_err()
        );
    }
}
