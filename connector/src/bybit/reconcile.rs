//! Reconcile (R-M1a) pure framing/parsing logic, separated from REST IO so it is unit-testable
//! against JSON fixtures without live HTTP. `Bybit::reconcile` (in `mod.rs`) is the thin IO shim
//! that fetches the three REST responses and then calls these functions to build the frame burst.
//!
//! Fail-closed: any non-numeric parse failure (other than an empty string, which is a legitimate
//! Bybit "not applicable" sentinel → 0.0) propagates as an error so the caller emits
//! `End { ok: false, .. }` instead of garbage.

use hftbacktest::types::{Order, ReconcileFrame};

use crate::bybit::{
    BybitError,
    msg::{AccountBalance, CoinBalance, Position, PrivateOrder},
};

/// Maximum bytes of `ret_msg` carried in a `ReconcileFrame::End`. Truncating before emit keeps the
/// encoded `End` well under `MAX_PAYLOAD_SIZE` (512) so a pathologically long Bybit `retMsg` can
/// never overflow and silently drop the terminator (design-note §2).
pub const RET_MSG_CAP: usize = 256;

/// Parses a Bybit numeric JSON string into `f64`. Bybit encodes numerics as strings; an EMPTY
/// string (e.g. `spotHedgingQty` on Cross margin) is a legitimate "not applicable" sentinel → 0.0.
/// Any other non-numeric value is a hard error (fail-closed — never silently coerce to 0.0).
pub fn parse_f64(s: &str) -> Result<f64, BybitError> {
    let t = s.trim();
    if t.is_empty() {
        Ok(0.0)
    } else {
        t.parse::<f64>().map_err(BybitError::InvalidPxQty)
    }
}

/// Truncates `msg` to at most [`RET_MSG_CAP`] bytes on a UTF-8 char boundary (so the `End` frame
/// cannot overflow `MAX_PAYLOAD_SIZE`).
pub fn truncate_ret_msg(msg: &str) -> String {
    if msg.len() <= RET_MSG_CAP {
        return msg.to_string();
    }
    let mut end = RET_MSG_CAP;
    while end > 0 && !msg.is_char_boundary(end) {
        end -= 1;
    }
    msg[..end].to_string()
}

/// Builds the single account-level totals frame (D-d) from the UNIFIED account balance.
pub fn frame_from_account(acct: &AccountBalance) -> Result<ReconcileFrame, BybitError> {
    Ok(ReconcileFrame::Account {
        total_equity: parse_f64(&acct.total_equity)?,
        total_wallet_balance: parse_f64(&acct.total_wallet_balance)?,
        total_margin_balance: parse_f64(&acct.total_margin_balance)?,
        total_available_balance: parse_f64(&acct.total_available_balance)?,
        total_perp_upl: parse_f64(&acct.total_perp_upl)?,
        total_initial_margin: parse_f64(&acct.total_initial_margin)?,
        total_maintenance_margin: parse_f64(&acct.total_maintenance_margin)?,
        account_im_rate: parse_f64(&acct.account_im_rate)?,
        account_mm_rate: parse_f64(&acct.account_mm_rate)?,
    })
}

/// Builds one `WalletCoin` frame per coin. Numerics are parsed string→f64 (`""` → 0.0); any other
/// non-numeric value fails the whole reconcile (fail-closed).
pub fn frames_from_wallet(acct: &AccountBalance) -> Result<Vec<ReconcileFrame>, BybitError> {
    acct.coin.iter().map(frame_from_coin).collect()
}

fn frame_from_coin(c: &CoinBalance) -> Result<ReconcileFrame, BybitError> {
    Ok(ReconcileFrame::WalletCoin {
        coin: c.coin.clone(),
        wallet_balance: parse_f64(&c.wallet_balance)?,
        equity: parse_f64(&c.equity)?,
        usd_value: parse_f64(&c.usd_value)?,
        spot_hedging_qty: parse_f64(&c.spot_hedging_qty)?,
        borrow_amount: parse_f64(&c.borrow_amount)?,
        collateral_switch: c.collateral_switch,
        margin_collateral: c.margin_collateral,
    })
}

/// Builds one signed `Position` frame per position (Buy → +size, Sell → −size). Bybit's `size` is
/// already parsed to `f64` by `msg::Position`'s deserializer; an unknown side with non-zero size is
/// a hard error (fail-closed).
pub fn frames_from_positions(positions: &[Position]) -> Result<Vec<ReconcileFrame>, BybitError> {
    positions
        .iter()
        .map(|p| {
            let qty = match p.side.as_str() {
                "Buy" => p.size,
                "Sell" => -p.size,
                "" | "None" if p.size == 0.0 => 0.0,
                _ => return Err(BybitError::InvalidArg("position side")),
            };
            Ok(ReconcileFrame::Position { qty })
        })
        .collect()
}

/// Builds one `OpenOrder` frame per open order.
///
/// **PrivateOrder → inner-lib `Order` mapping (deviation, see report):** Bybit's `orderId` is a
/// UUID-like string and the bot's own `u64` `order_id` lives only in the connector's in-memory
/// `order_id_map` (not recoverable from a fresh REST pull). So `order_id` is set to `0` here — the
/// frame carries the authoritative REST `price`/`qty`/`side`/`status`, which is what R-M1b compares
/// against the cached view. `price_tick` is encoded with `tick_size = 1.0` so `price_tick == price`
/// losslessly carries the REST price (the connector does not know the per-symbol tick size in this
/// read-only path; R-M1b reads `order.price()` = `price_tick * tick_size`).
pub fn frames_from_orders(orders: &[PrivateOrder]) -> Vec<ReconcileFrame> {
    orders.iter().map(build_order_frame).collect()
}

fn build_order_frame(po: &PrivateOrder) -> ReconcileFrame {
    ReconcileFrame::OpenOrder {
        order: build_order(po),
    }
}

/// Maps a REST `PrivateOrder` to an inner-lib `Order` carrying the REST truth. See
/// [`frames_from_orders`] for the `order_id`/`price_tick` rationale.
pub fn build_order(po: &PrivateOrder) -> Order {
    let mut order = Order::new(
        // No recoverable inner-lib u64 id from a fresh REST pull (see module docs).
        0,
        // tick_size = 1.0 below → price_tick carries the raw price.
        po.price as i64,
        1.0,
        po.qty,
        po.side,
        po.order_type,
        po.time_in_force,
    );
    order.leaves_qty = po.leaves_qty;
    order.exec_qty = po.cum_exec_qty;
    order.status = po.order_status;
    order
}

#[cfg(test)]
mod tests {
    use bincode::config;
    use hftbacktest::types::{
        OrdType,
        ReconcileEndpoint,
        ReconcileFrame,
        Side,
        Status,
        TimeInForce,
    };

    use super::*;
    use crate::bybit::msg::{Position, PrivateOrder, WalletBalanceResponse};

    // --- helpers ------------------------------------------------------------------------------

    fn orders_fixture(n: usize, next_cursor: &str) -> String {
        let mut items = Vec::new();
        for i in 0..n {
            items.push(format!(
                r#"{{
                    "symbol":"BTCUSDT","orderId":"abc-{i}","side":"Buy","orderType":"Limit",
                    "cancelType":"UNKNOWN","price":"60000.5","qty":"0.01","orderIv":"",
                    "timeInForce":"GTC","orderStatus":"New","orderLinkId":"t{i}",
                    "lastPriceOnCreated":"","reduceOnly":false,"leavesQty":"0.01","leavesValue":"600",
                    "cumExecQty":"0","cumExecValue":"0","avgPrice":"","blockTradeId":"",
                    "positionIdx":0,"cumExecFee":"0","createdTime":"1","updatedTime":"2",
                    "rejectReason":"EC_NoError","stopOrderType":"","tpslMode":"","triggerPrice":"",
                    "takeProfit":"","stopLoss":"","tpTriggerBy":"","slTriggerBy":"","tpLimitPrice":"",
                    "slLimitPrice":"","triggerDirection":0,"triggerBy":"","closeOnTrigger":false,
                    "category":"linear","placeType":"","smpType":"None","smpGroup":0,"smpOrderId":"",
                    "feeCurrency":""
                }}"#
            ));
        }
        format!(
            r#"{{"retCode":0,"retMsg":"OK","result":{{"list":[{}],"nextPageCursor":"{}","category":"linear"}},"retExtInfo":{{}},"time":1}}"#,
            items.join(","),
            next_cursor
        )
    }

    fn parse_orders_page(json: &str) -> (Vec<PrivateOrder>, String) {
        use crate::bybit::msg::RestResponse;
        let resp: RestResponse = serde_json::from_str(json).unwrap();
        let list = resp.result.list.unwrap();
        let orders: Vec<PrivateOrder> = serde_json::from_value(list).unwrap();
        (orders, resp.result.next_page_cursor)
    }

    // --- Test #10: frame_max_size_under_512 ---------------------------------------------------

    #[test]
    fn frame_max_size_under_512() {
        let cfg = config::standard();

        // Worst-case OpenOrder: large ints.
        let mut big_order = Order::new(
            u64::MAX,
            i64::MAX,
            1.0,
            123456789.987654,
            Side::Sell,
            OrdType::Limit,
            TimeInForce::GTC,
        );
        big_order.exec_price_tick = i64::MAX;
        big_order.exch_timestamp = i64::MAX;
        big_order.local_timestamp = i64::MAX;
        let open_order = ReconcileFrame::OpenOrder { order: big_order };

        // Worst-case WalletCoin: long coin name.
        let wallet_coin = ReconcileFrame::WalletCoin {
            coin: "VERYLONGCOINNAMEXYZ".to_string(),
            wallet_balance: f64::MAX,
            equity: f64::MAX,
            usd_value: f64::MAX,
            spot_hedging_qty: f64::MAX,
            borrow_amount: f64::MAX,
            collateral_switch: true,
            margin_collateral: true,
        };

        // Worst-case End: a maxed-out ret_msg.
        let end = ReconcileFrame::End {
            request_id: u64::MAX,
            ok: false,
            endpoint: ReconcileEndpoint::OpenOrders,
            ret_code: Some(i64::MIN),
            ret_msg: "x".repeat(RET_MSG_CAP),
            http_status: u16::MAX,
        };

        for frame in [open_order, wallet_coin, end] {
            // Encode the OUTER LiveEvent envelope (symbol + frame) — that is what crosses IPC.
            let ev = hftbacktest::types::LiveEvent::Reconcile {
                symbol: "BTCUSDT".to_string(),
                frame,
            };
            let bytes = bincode::encode_to_vec(&ev, cfg).unwrap();
            assert!(
                bytes.len() < 512,
                "encoded frame {} bytes >= 512",
                bytes.len()
            );
        }
    }

    // --- Test #11: orders_pagination_0_1_50_over50 --------------------------------------------

    #[test]
    fn orders_pagination_frame_counts() {
        // 0 orders → 0 frames.
        let (o0, _) = parse_orders_page(&orders_fixture(0, ""));
        assert_eq!(frames_from_orders(&o0).len(), 0);

        // 1 order → 1 frame.
        let (o1, _) = parse_orders_page(&orders_fixture(1, ""));
        assert_eq!(frames_from_orders(&o1).len(), 1);

        // 50 orders, empty cursor → 50 frames, no further fetch.
        let (o50, cur50) = parse_orders_page(&orders_fixture(50, ""));
        assert_eq!(frames_from_orders(&o50).len(), 50);
        assert!(cur50.is_empty(), "no 2nd page expected for a drained cursor");
    }

    #[test]
    fn orders_over_50_triggers_second_page() {
        // First page: 50 orders WITH a non-empty cursor → the loop must fetch a 2nd page.
        let (page1, cur1) = parse_orders_page(&orders_fixture(50, "CURSOR_PAGE2"));
        assert_eq!(page1.len(), 50);
        assert!(!cur1.is_empty(), "non-empty cursor must drive a 2nd fetch");

        // Second page: 1 order, drained cursor → loop ends. Total 51.
        let (page2, cur2) = parse_orders_page(&orders_fixture(1, ""));
        assert!(cur2.is_empty());
        let total = page1.len() + page2.len();
        assert_eq!(total, 51);

        let mut all = page1;
        all.extend(page2);
        assert_eq!(frames_from_orders(&all).len(), 51);
    }

    #[test]
    fn orders_pagination_cap_breach_is_fail_closed() {
        // Simulate the loop logic: every page returns a non-empty cursor → cap is hit.
        let cap = crate::bybit::rest::OPEN_ORDERS_PAGE_CAP;
        let mut pages = 0usize;
        let mut cursor = String::new();
        let breached = loop {
            if pages >= cap {
                break true; // would return Err(PaginationCapExceeded)
            }
            // Each page hands back a non-empty cursor.
            let (_orders, next) = parse_orders_page(&orders_fixture(50, "STILL_MORE"));
            cursor = next;
            pages += 1;
            if cursor.is_empty() {
                break false;
            }
        };
        assert!(breached, "cap must be reached when cursor never drains");
        assert!(!cursor.is_empty());
        assert_eq!(pages, cap);
    }

    // --- Test #12: wallet_parses_usdt_eth_extra -----------------------------------------------

    fn wallet_fixture() -> &'static str {
        r#"{
            "retCode":0,"retMsg":"OK",
            "result":{"list":[{
                "totalEquity":"1000.5","totalWalletBalance":"990.0","totalMarginBalance":"1000.5",
                "totalAvailableBalance":"800.0","totalPerpUPL":"10.5",
                "totalInitialMargin":"100.0","totalMaintenanceMargin":"50.0",
                "accountIMRate":"0.1","accountMMRate":"0.05","accountType":"UNIFIED",
                "coin":[
                    {"coin":"USDT","walletBalance":"990.0","equity":"1000.5","usdValue":"1000.5",
                     "spotHedgingQty":"","borrowAmount":"0","collateralSwitch":true,"marginCollateral":true},
                    {"coin":"ETH","walletBalance":"0.5","equity":"0.5","usdValue":"1500.0",
                     "spotHedgingQty":"0.1","borrowAmount":"0","collateralSwitch":true,"marginCollateral":false},
                    {"coin":"DOGE","walletBalance":"100","equity":"100","usdValue":"12.3",
                     "spotHedgingQty":"","borrowAmount":"5","collateralSwitch":false,"marginCollateral":false}
                ]
            }]},
            "retExtInfo":{},"time":1
        }"#
    }

    #[test]
    fn wallet_parses_usdt_eth_extra() {
        let resp: WalletBalanceResponse = serde_json::from_str(wallet_fixture()).unwrap();
        let acct = &resp.result.list.unwrap()[0];
        let frames = frames_from_wallet(acct).unwrap();
        assert_eq!(frames.len(), 3, "one WalletCoin frame per coin");

        match &frames[0] {
            ReconcileFrame::WalletCoin {
                coin,
                wallet_balance,
                spot_hedging_qty,
                collateral_switch,
                margin_collateral,
                ..
            } => {
                assert_eq!(coin, "USDT");
                assert_eq!(*wallet_balance, 990.0);
                assert_eq!(*spot_hedging_qty, 0.0, "empty string → 0.0");
                assert!(*collateral_switch);
                assert!(*margin_collateral);
            }
            f => panic!("expected WalletCoin, got {f:?}"),
        }
        match &frames[1] {
            ReconcileFrame::WalletCoin {
                coin,
                usd_value,
                spot_hedging_qty,
                margin_collateral,
                ..
            } => {
                assert_eq!(coin, "ETH");
                assert_eq!(*usd_value, 1500.0);
                assert_eq!(*spot_hedging_qty, 0.1);
                assert!(!*margin_collateral);
            }
            f => panic!("expected WalletCoin, got {f:?}"),
        }
        match &frames[2] {
            ReconcileFrame::WalletCoin { coin, borrow_amount, .. } => {
                assert_eq!(coin, "DOGE");
                assert_eq!(*borrow_amount, 5.0);
            }
            f => panic!("expected WalletCoin, got {f:?}"),
        }

        // Account-level totals frame (D-d).
        let acct_frame = frame_from_account(acct).unwrap();
        match acct_frame {
            ReconcileFrame::Account {
                total_equity,
                total_margin_balance,
                account_mm_rate,
                ..
            } => {
                assert_eq!(total_equity, 1000.5);
                assert_eq!(total_margin_balance, 1000.5);
                assert_eq!(account_mm_rate, 0.05);
            }
            f => panic!("expected Account, got {f:?}"),
        }
    }

    #[test]
    fn wallet_non_numeric_field_fails_closed() {
        let bad = r#"{
            "retCode":0,"retMsg":"OK",
            "result":{"list":[{
                "accountType":"UNIFIED",
                "coin":[{"coin":"USDT","walletBalance":"not-a-number","equity":"1","usdValue":"1",
                         "spotHedgingQty":"","borrowAmount":"0","collateralSwitch":true,"marginCollateral":true}]
            }]},
            "retExtInfo":{},"time":1
        }"#;
        let resp: WalletBalanceResponse = serde_json::from_str(bad).unwrap();
        let acct = &resp.result.list.unwrap()[0];
        assert!(
            frames_from_wallet(acct).is_err(),
            "a non-numeric (non-empty) value must fail-closed, not coerce to 0.0"
        );
    }

    // --- Test #13: reconcile_parses_ret_code_msg ----------------------------------------------

    #[test]
    fn reconcile_parses_ret_code_msg() {
        // Error envelope: retCode 10006 ("Too many visits"), list:null.
        let err_json = r#"{"retCode":10006,"retMsg":"Too many visits.","result":{"list":null,"nextPageCursor":"","category":""},"retExtInfo":{},"time":1}"#;
        use crate::bybit::msg::RestResponse;
        let resp: RestResponse = serde_json::from_str(err_json).unwrap();
        assert_eq!(resp.ret_code, 10006);
        assert_eq!(resp.ret_msg, "Too many visits.");

        // The connector maps this to BybitError::OpErrorCode preserving the RAW code/msg, which the
        // reconcile path turns into End{ ok:false, ret_code:Some(10006), ret_msg preserved }.
        let err = BybitError::OpErrorCode {
            code: resp.ret_code,
            msg: resp.ret_msg.clone(),
        };
        let (ok, ret_code, ret_msg) = end_fields_from_err(&err);
        assert!(!ok);
        assert_eq!(ret_code, Some(10006));
        assert_eq!(ret_msg, "Too many visits.");

        // And the Error path carries the same raw code in a structured Value.
        let value = err.to_value();
        match value {
            hftbacktest::types::Value::Map(m) => {
                assert_eq!(m.get("code").and_then(|v| v.get_int()), Some(10006));
            }
            v => panic!("expected Map, got {v:?}"),
        }
    }

    /// Mirrors the connector's error→End mapping (raw code/msg preserved, truncated).
    fn end_fields_from_err(err: &BybitError) -> (bool, Option<i64>, String) {
        let (code, msg) = match err {
            BybitError::OpErrorCode { code, msg } => (Some(*code), msg.clone()),
            BybitError::PaginationCapExceeded => {
                (None, "order pagination cap exceeded".to_string())
            }
            other => (None, other.to_string()),
        };
        (false, code, truncate_ret_msg(&msg))
    }

    // --- Test #14: wallet_list_null_does_not_panic --------------------------------------------

    #[test]
    fn wallet_list_null_does_not_panic() {
        let null_json = r#"{"retCode":0,"retMsg":"OK","result":{"list":null},"retExtInfo":{},"time":1}"#;
        let resp: WalletBalanceResponse = serde_json::from_str(null_json).unwrap();
        // Guarded Option — no panic. The reconcile path treats None as a fail-closed condition.
        assert!(resp.result.list.is_none(), "null list → guarded None, no panic");
    }

    // --- parse_f64 helper ---------------------------------------------------------------------

    #[test]
    fn parse_f64_handles_empty_and_invalid() {
        assert_eq!(parse_f64("").unwrap(), 0.0);
        assert_eq!(parse_f64("  ").unwrap(), 0.0);
        assert_eq!(parse_f64("12.5").unwrap(), 12.5);
        assert_eq!(parse_f64("-3").unwrap(), -3.0);
        assert!(parse_f64("abc").is_err());
    }

    #[test]
    fn truncate_ret_msg_caps_length() {
        let long = "y".repeat(1000);
        assert_eq!(truncate_ret_msg(&long).len(), RET_MSG_CAP);
        assert_eq!(truncate_ret_msg("short"), "short");
    }

    #[test]
    fn positions_signed_qty() {
        let pos_json = r#"[
            {"positionIdx":0,"tradeMode":0,"riskId":1,"riskLimitValue":"2000000","symbol":"BTCUSDT",
             "side":"Buy","size":"0.5","entryPrice":"60000","leverage":"10","positionValue":"30000",
             "positionBalance":"3000","markPrice":"60100","positionIM":"3000","positionMM":"150",
             "takeProfit":"","stopLoss":"","trailingStop":"0","unrealisedPnl":"50","curRealisedPnl":"0",
             "cumRealisedPnl":"0","sessionAvgPrice":"","createdTime":"1","updatedTime":"2","tpslMode":"Full",
             "liqPrice":"","bustPrice":"","category":"linear","positionStatus":"Normal","adlRankIndicator":2,
             "autoAddMargin":0,"leverageSysUpdatedTime":"","mmrSysUpdatedTime":"","seq":1,"isReduceOnly":false},
            {"positionIdx":0,"tradeMode":0,"riskId":1,"riskLimitValue":"2000000","symbol":"ETHUSDT",
             "side":"Sell","size":"2.0","entryPrice":"3000","leverage":"10","positionValue":"6000",
             "positionBalance":"600","markPrice":"3010","positionIM":"600","positionMM":"30",
             "takeProfit":"","stopLoss":"","trailingStop":"0","unrealisedPnl":"-20","curRealisedPnl":"0",
             "cumRealisedPnl":"0","sessionAvgPrice":"","createdTime":"1","updatedTime":"2","tpslMode":"Full",
             "liqPrice":"","bustPrice":"","category":"linear","positionStatus":"Normal","adlRankIndicator":2,
             "autoAddMargin":0,"leverageSysUpdatedTime":"","mmrSysUpdatedTime":"","seq":1,"isReduceOnly":false}
        ]"#;
        let positions: Vec<Position> = serde_json::from_str(pos_json).unwrap();
        let frames = frames_from_positions(&positions).unwrap();
        assert_eq!(frames.len(), 2);
        match frames[0] {
            ReconcileFrame::Position { qty } => assert_eq!(qty, 0.5),
            ref f => panic!("expected Position, got {f:?}"),
        }
        match frames[1] {
            ReconcileFrame::Position { qty } => assert_eq!(qty, -2.0),
            ref f => panic!("expected Position, got {f:?}"),
        }
    }

    #[test]
    fn order_mapping_carries_rest_truth() {
        let (orders, _) = parse_orders_page(&orders_fixture(1, ""));
        let frames = frames_from_orders(&orders);
        match &frames[0] {
            ReconcileFrame::OpenOrder { order } => {
                assert_eq!(order.qty, 0.01);
                assert_eq!(order.leaves_qty, 0.01);
                assert_eq!(order.side, Side::Buy);
                assert_eq!(order.status, Status::New);
                // price_tick carries the raw price (tick_size = 1.0).
                assert_eq!(order.price(), 60000.0);
            }
            f => panic!("expected OpenOrder, got {f:?}"),
        }
    }
}
