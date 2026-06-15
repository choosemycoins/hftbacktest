//! SpotOrder (B-M1a) pure framing/parsing logic, separated from REST IO so it is unit-testable
//! against JSON fixtures without live HTTP. `Bybit::spot_order` (in `mod.rs`) is the thin IO shim
//! that performs the REST op (POST create/cancel, GET realtime) and then calls these functions to
//! build the frame burst. Clones `reconcile.rs`.
//!
//! Fail-closed: Bybit always replies HTTP 200, so correctness relies on `retCode` — a non-zero
//! (or absent) `retCode` ⇒ `End { ok: false, .. }`. Numerics (`cumExecQty`/`avgPrice`) are parsed
//! string→f64 via the reconcile module's `parse_f64` (rejects non-finite). `orderStatus` is mapped
//! to [`SpotOrderStatus`] with a fail-closed [`SpotOrderStatus::Other`] for unknown values.

use hftbacktest::types::{SpotMarketUnit, SpotOrderFrame, SpotOrderStatus, SpotOrdType};

use crate::bybit::{BybitError, reconcile::parse_f64};

/// Maps a Bybit `orderStatus` string to [`SpotOrderStatus`]. An unrecognized value maps to
/// [`SpotOrderStatus::Other`] (fail-closed — never silently treat an unknown state as terminal).
///
/// Bybit spot `orderStatus` values (V5 docs): `New`, `PartiallyFilled`, `Filled`, `Cancelled`,
/// `Rejected`, plus `PartiallyFilledCanceled`/`Untriggered`/`Triggered`/`Deactivated` etc. which
/// fall through to `Other`.
pub fn map_order_status(s: &str) -> SpotOrderStatus {
    match s {
        "New" => SpotOrderStatus::New,
        "PartiallyFilled" => SpotOrderStatus::PartiallyFilled,
        "Filled" => SpotOrderStatus::Filled,
        // Bybit spelling is "Cancelled" (two l's) for spot; accept the US spelling defensively too.
        "Cancelled" | "Canceled" => SpotOrderStatus::Cancelled,
        "Rejected" => SpotOrderStatus::Rejected,
        _ => SpotOrderStatus::Other,
    }
}

/// Bybit `side` wire string for a [`hftbacktest::types::Side`].
pub fn side_str(side: hftbacktest::types::Side) -> Result<&'static str, BybitError> {
    use hftbacktest::types::Side;
    match side {
        Side::Buy => Ok("Buy"),
        Side::Sell => Ok("Sell"),
        Side::None | Side::Unsupported => Err(BybitError::InvalidArg("spot order side")),
    }
}

/// Bybit `orderType` wire string for a [`SpotOrdType`].
pub fn order_type_str(ot: SpotOrdType) -> &'static str {
    match ot {
        SpotOrdType::Market => "Market",
        SpotOrdType::Limit => "Limit",
    }
}

/// Bybit `marketUnit` wire string for a [`SpotMarketUnit`]. Spot market-BUY defaults qty to the
/// QUOTE coin, so a base-coin order MUST send `baseCoin` (de-risk §34-41).
pub fn market_unit_str(mu: SpotMarketUnit) -> &'static str {
    match mu {
        SpotMarketUnit::BaseCoin => "baseCoin",
        SpotMarketUnit::QuoteCoin => "quoteCoin",
    }
}

/// Builds the JSON body for a spot `POST /v5/order/create`. `category="spot"` is hardcoded per-RPC
/// (B-M1a D-feasible). `price` is omitted for Market orders. `marketUnit` denominates `qty`.
pub fn build_create_body(
    symbol: &str,
    side: hftbacktest::types::Side,
    order_type: SpotOrdType,
    qty: f64,
    market_unit: SpotMarketUnit,
    price: Option<f64>,
    order_link_id: &str,
) -> Result<String, BybitError> {
    let side = side_str(side)?;
    let ot = order_type_str(order_type);
    let mu = market_unit_str(market_unit);
    // qty/price are JSON strings on Bybit. Reject non-finite at the source (fail-closed).
    if !qty.is_finite() {
        return Err(BybitError::InvalidArg("spot order qty (non-finite)"));
    }
    let mut body = format!(
        "{{\"category\":\"spot\",\"symbol\":\"{symbol}\",\"side\":\"{side}\",\
         \"orderType\":\"{ot}\",\"qty\":\"{qty}\",\"marketUnit\":\"{mu}\",\
         \"orderLinkId\":\"{order_link_id}\""
    );
    if let Some(p) = price {
        if !p.is_finite() {
            return Err(BybitError::InvalidArg("spot order price (non-finite)"));
        }
        body.push_str(&format!(",\"price\":\"{p}\""));
    }
    body.push('}');
    Ok(body)
}

/// Builds the JSON body for a spot `POST /v5/order/cancel` correlated by `orderLinkId`.
pub fn build_cancel_body(symbol: &str, order_link_id: &str) -> String {
    format!(
        "{{\"category\":\"spot\",\"symbol\":\"{symbol}\",\"orderLinkId\":\"{order_link_id}\"}}"
    )
}

/// Builds the `Ack` frame for a place/status op from a Bybit spot order item (the realtime/status
/// view, which carries `cumExecQty`/`avgPrice`/`orderStatus`). Numerics parsed string→f64
/// (fail-closed on non-finite); `orderStatus` mapped via [`map_order_status`] (fail-closed `Other`).
///
/// Uses the dedicated [`SpotOrderItem`](crate::bybit::msg::SpotOrderItem) (keeping `orderStatus` as
/// a raw string) rather than `PrivateOrder`: the latter's `from_str_to_status` deserializer is lossy
/// (maps `Rejected`→`Expired`) and HARD-FAILS on an unknown status, which would defeat the
/// fail-closed `Other` mapping the spot RPC needs (B-M1a deviation, see report).
pub fn ack_from_spot_item(
    item: &crate::bybit::msg::SpotOrderItem,
) -> Result<SpotOrderFrame, BybitError> {
    Ok(SpotOrderFrame::Ack {
        order_link_id: item.order_link_id.clone(),
        cum_exec_qty: parse_f64(&item.cum_exec_qty)?,
        avg_price: parse_f64(&item.avg_price)?,
        status: map_order_status(&item.order_status),
    })
}

/// Builds the `Ack` frame for a freshly-placed order from the create-response `orderLinkId`. A
/// Bybit `POST /v5/order/create` reply carries only `orderId`/`orderLinkId` (not executed state),
/// so the post-create Ack reports zero fill and `status = New` (the order is accepted-but-resting;
/// fills arrive later and are observable via a `Status` poll).
pub fn ack_from_create(order_link_id: &str) -> SpotOrderFrame {
    SpotOrderFrame::Ack {
        order_link_id: order_link_id.to_string(),
        cum_exec_qty: 0.0,
        avg_price: 0.0,
        status: SpotOrderStatus::New,
    }
}

#[cfg(test)]
mod tests {
    use bincode::config;
    use hftbacktest::types::{LiveEvent, Side, SpotOrderFrame, SpotOrderStatus};

    use super::*;
    use crate::bybit::{
        msg::{OrderCreateResponse, RestResponse, SpotOrderItem},
        reconcile::RET_MSG_CAP,
    };

    // --- 512B frame-size test (mirror reconcile's frame_max_size_under_512) --------------------

    /// Worst-case Place reply frames (Begin/Ack/End) must each encode < MAX_PAYLOAD_SIZE (512).
    #[test]
    fn spot_order_place_frame_roundtrip_under_512b() {
        let cfg = config::standard();

        let begin = SpotOrderFrame::Begin {
            request_id: u64::MAX,
            snapshot_ts_ns: i64::MAX,
        };
        let ack = SpotOrderFrame::Ack {
            // A long order_link_id is the only unbounded field — budget against it.
            order_link_id: "basis-1718000000000-18446744073709551615".to_string(),
            cum_exec_qty: f64::MAX,
            avg_price: f64::MAX,
            status: SpotOrderStatus::PartiallyFilled,
        };
        let end = SpotOrderFrame::End {
            request_id: u64::MAX,
            ok: false,
            ret_code: Some(i64::MIN),
            ret_msg: "x".repeat(RET_MSG_CAP),
            http_status: u16::MAX,
        };

        for frame in [begin, ack, end] {
            // Encode the OUTER LiveEvent envelope (symbol + frame) — that is what crosses IPC.
            let ev = LiveEvent::SpotOrderReply {
                symbol: "ETHUSDT".to_string(),
                frame,
            };
            let bytes = bincode::encode_to_vec(&ev, cfg).unwrap();
            assert!(bytes.len() < 512, "encoded frame {} bytes >= 512", bytes.len());
        }
    }

    // --- frame round-trips (encode→decode equal) ----------------------------------------------

    fn roundtrip(frame: SpotOrderFrame) -> SpotOrderFrame {
        let cfg = config::standard();
        let ev = LiveEvent::SpotOrderReply {
            symbol: "ETHUSDT".to_string(),
            frame,
        };
        let bytes = bincode::encode_to_vec(&ev, cfg).unwrap();
        let (decoded, _): (LiveEvent, usize) = bincode::decode_from_slice(&bytes, cfg).unwrap();
        match decoded {
            LiveEvent::SpotOrderReply { frame, .. } => frame,
            other => panic!("expected SpotOrderReply, got {other:?}"),
        }
    }

    #[test]
    fn spot_order_cancel_roundtrip() {
        // Cancel reply is just Begin + End (no Ack).
        let begin = SpotOrderFrame::Begin {
            request_id: 11,
            snapshot_ts_ns: 1_700_000_000_000_000_000,
        };
        match roundtrip(begin) {
            SpotOrderFrame::Begin { request_id, .. } => assert_eq!(request_id, 11),
            f => panic!("expected Begin, got {f:?}"),
        }
        let end = SpotOrderFrame::End {
            request_id: 11,
            ok: true,
            ret_code: Some(0),
            ret_msg: "OK".to_string(),
            http_status: 200,
        };
        match roundtrip(end) {
            SpotOrderFrame::End {
                request_id,
                ok,
                ret_code,
                ret_msg,
                http_status,
            } => {
                assert_eq!(request_id, 11);
                assert!(ok);
                assert_eq!(ret_code, Some(0));
                assert_eq!(ret_msg, "OK");
                assert_eq!(http_status, 200);
            }
            f => panic!("expected End, got {f:?}"),
        }
    }

    #[test]
    fn spot_order_status_roundtrip() {
        let ack = SpotOrderFrame::Ack {
            order_link_id: "basis-7".to_string(),
            cum_exec_qty: 0.25,
            avg_price: 3001.5,
            status: SpotOrderStatus::PartiallyFilled,
        };
        match roundtrip(ack) {
            SpotOrderFrame::Ack {
                order_link_id,
                cum_exec_qty,
                avg_price,
                status,
            } => {
                assert_eq!(order_link_id, "basis-7");
                assert_eq!(cum_exec_qty, 0.25);
                assert_eq!(avg_price, 3001.5);
                assert_eq!(status, SpotOrderStatus::PartiallyFilled);
            }
            f => panic!("expected Ack, got {f:?}"),
        }
    }

    /// A fail-closed `End{ok:false}` carrying a min-notional/insufficient-balance retCode must
    /// round-trip faithfully (raw retCode preserved, never collapsed to a bool).
    #[test]
    fn spot_order_end_preserves_retcode() {
        let end = SpotOrderFrame::End {
            request_id: 9,
            ok: false,
            ret_code: Some(170131), // Bybit: insufficient balance
            ret_msg: "Insufficient balance".to_string(),
            http_status: 200,
        };
        match roundtrip(end) {
            SpotOrderFrame::End { ok, ret_code, ret_msg, .. } => {
                assert!(!ok);
                assert_eq!(ret_code, Some(170131));
                assert_eq!(ret_msg, "Insufficient balance");
            }
            f => panic!("expected End, got {f:?}"),
        }
    }

    // --- orderStatus mapping (fail-closed Other) ----------------------------------------------

    #[test]
    fn spot_order_status_maps_known() {
        assert_eq!(map_order_status("New"), SpotOrderStatus::New);
        assert_eq!(
            map_order_status("PartiallyFilled"),
            SpotOrderStatus::PartiallyFilled
        );
        assert_eq!(map_order_status("Filled"), SpotOrderStatus::Filled);
        assert_eq!(map_order_status("Cancelled"), SpotOrderStatus::Cancelled);
        assert_eq!(map_order_status("Rejected"), SpotOrderStatus::Rejected);
    }

    #[test]
    fn spot_order_status_maps_unknown_to_other() {
        assert_eq!(map_order_status("Untriggered"), SpotOrderStatus::Other);
        assert_eq!(map_order_status("Deactivated"), SpotOrderStatus::Other);
        assert_eq!(map_order_status(""), SpotOrderStatus::Other);
        assert_eq!(map_order_status("garbage"), SpotOrderStatus::Other);
    }

    // --- create-body builder (category=spot, marketUnit=baseCoin) ------------------------------

    #[test]
    fn create_body_market_buy_base_coin_omits_price() {
        let body = build_create_body(
            "ETHUSDT",
            Side::Buy,
            SpotOrdType::Market,
            0.5,
            SpotMarketUnit::BaseCoin,
            None,
            "basis-1",
        )
        .unwrap();
        assert!(body.contains("\"category\":\"spot\""));
        assert!(body.contains("\"side\":\"Buy\""));
        assert!(body.contains("\"orderType\":\"Market\""));
        // base-qty order MUST set marketUnit=baseCoin (de-risk §34-41).
        assert!(body.contains("\"marketUnit\":\"baseCoin\""));
        assert!(body.contains("\"orderLinkId\":\"basis-1\""));
        assert!(!body.contains("\"price\""), "market order omits price");
        // Body must be valid JSON.
        let _: serde_json::Value = serde_json::from_str(&body).unwrap();
    }

    #[test]
    fn create_body_limit_sell_includes_price() {
        let body = build_create_body(
            "ETHUSDT",
            Side::Sell,
            SpotOrdType::Limit,
            1.0,
            SpotMarketUnit::BaseCoin,
            Some(3000.25),
            "basis-2",
        )
        .unwrap();
        assert!(body.contains("\"orderType\":\"Limit\""));
        assert!(body.contains("\"side\":\"Sell\""));
        assert!(body.contains("\"price\":\"3000.25\""));
        let _: serde_json::Value = serde_json::from_str(&body).unwrap();
    }

    #[test]
    fn create_body_rejects_non_finite_qty() {
        assert!(build_create_body(
            "ETHUSDT",
            Side::Buy,
            SpotOrdType::Market,
            f64::NAN,
            SpotMarketUnit::BaseCoin,
            None,
            "basis-3",
        )
        .is_err());
    }

    // --- representative Bybit JSON parse tests -------------------------------------------------

    /// A successful spot `POST /v5/order/create` reply: retCode 0, result carries orderId/orderLinkId.
    #[test]
    fn parse_create_ack_ok() {
        let json = r#"{"retCode":0,"retMsg":"OK","result":{"orderId":"1779020","orderLinkId":"basis-7"},"retExtInfo":{},"time":1718000000000}"#;
        let resp: OrderCreateResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.ret_code, 0);
        assert_eq!(resp.result.order_link_id, "basis-7");
        // The post-create Ack reports zero fill + New (state is fetched later via Status).
        let ack = ack_from_create(&resp.result.order_link_id);
        match ack {
            SpotOrderFrame::Ack { order_link_id, cum_exec_qty, status, .. } => {
                assert_eq!(order_link_id, "basis-7");
                assert_eq!(cum_exec_qty, 0.0);
                assert_eq!(status, SpotOrderStatus::New);
            }
            f => panic!("expected Ack, got {f:?}"),
        }
    }

    /// A min-notional reject: non-zero retCode → the caller emits End{ok:false} with raw code/msg.
    #[test]
    fn parse_create_reject_min_notional() {
        let json = r#"{"retCode":170131,"retMsg":"Insufficient balance.","result":{},"retExtInfo":{},"time":1718000000000}"#;
        let resp: OrderCreateResponse = serde_json::from_str(json).unwrap();
        assert_ne!(resp.ret_code, 0, "non-zero retCode ⇒ fail-closed (ok:false)");
        assert_eq!(resp.ret_code, 170131);
        assert_eq!(resp.ret_msg, "Insufficient balance.");
    }

    /// A spot `GET /v5/order/realtime` status reply: parse the order and build the Ack. Uses the
    /// dedicated `SpotOrderItem` (raw orderStatus) so an unknown status would map to `Other` rather
    /// than hard-fail the parse (unlike `PrivateOrder`'s lossy deserializer).
    #[test]
    fn parse_status_realtime_ack() {
        let json = r#"{"retCode":0,"retMsg":"OK","result":{"list":[{
            "symbol":"ETHUSDT","orderId":"1779020","side":"Buy","orderType":"Limit",
            "price":"3000.0","qty":"0.5","timeInForce":"GTC","orderStatus":"PartiallyFilled",
            "orderLinkId":"basis-7","leavesQty":"0.25","cumExecQty":"0.25","avgPrice":"3002.0",
            "category":"spot","createdTime":"1","updatedTime":"2"
        }],"nextPageCursor":"","category":"spot"},"retExtInfo":{},"time":1}"#;
        let resp: RestResponse = serde_json::from_str(json).unwrap();
        let list = resp.result.list.unwrap();
        let orders: Vec<SpotOrderItem> = serde_json::from_value(list).unwrap();
        assert_eq!(orders.len(), 1);
        let ack = ack_from_spot_item(&orders[0]).unwrap();
        match ack {
            SpotOrderFrame::Ack { order_link_id, cum_exec_qty, avg_price, status } => {
                assert_eq!(order_link_id, "basis-7");
                assert_eq!(cum_exec_qty, 0.25);
                assert_eq!(avg_price, 3002.0);
                assert_eq!(status, SpotOrderStatus::PartiallyFilled);
            }
            f => panic!("expected Ack, got {f:?}"),
        }
    }

    /// An unknown orderStatus on a status query maps to `Other` (fail-closed) WITHOUT failing the
    /// JSON parse — the raison d'être of the dedicated `SpotOrderItem`.
    #[test]
    fn parse_status_unknown_orderstatus_is_other_not_parse_fail() {
        let json = r#"{"retCode":0,"retMsg":"OK","result":{"list":[{
            "orderLinkId":"basis-9","cumExecQty":"0","avgPrice":"","orderStatus":"SomeNewBybitState"
        }],"nextPageCursor":"","category":"spot"},"retExtInfo":{},"time":1}"#;
        let resp: RestResponse = serde_json::from_str(json).unwrap();
        let list = resp.result.list.unwrap();
        let orders: Vec<SpotOrderItem> = serde_json::from_value(list).unwrap();
        let ack = ack_from_spot_item(&orders[0]).unwrap();
        match ack {
            SpotOrderFrame::Ack { status, avg_price, .. } => {
                assert_eq!(status, SpotOrderStatus::Other);
                assert_eq!(avg_price, 0.0, "empty avgPrice → 0.0");
            }
            f => panic!("expected Ack, got {f:?}"),
        }
    }
}
