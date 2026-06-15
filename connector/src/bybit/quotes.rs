//! Quotes (B-M1b) pure framing/parsing logic, separated from REST IO so it is unit-testable against
//! JSON fixtures without live HTTP. `Bybit::quotes` (in `mod.rs`) is the thin IO shim that performs
//! the UNSIGNED public GETs (tickers linear+spot, funding-history, instruments-info) and then calls
//! these functions to build the frame burst. Clones `spot_order.rs`/`reconcile.rs`.
//!
//! Fail-closed: Bybit always replies HTTP 200, so correctness relies on `retCode` — a non-zero (or
//! absent) `retCode` ⇒ `End { ok: false, .. }`. Numerics (`bid1Price`/`fundingRate`/...) are parsed
//! string→f64 via the reconcile module's `parse_f64` (rejects non-finite). `nextFundingTime` is a ms
//! epoch STRING → parsed string→i64 ("" → 0).

use hftbacktest::types::QuotesFrame;

use crate::bybit::{
    BybitError,
    msg::{FundingHistoryItem, InstrumentInfoItem, TickerItem},
    reconcile::parse_f64,
};

/// Maximum number of `FundingRow` frames carried in a quotes reply (SF-4). Mirrors the reconcile
/// open-orders page cap discipline: bound the burst so the encoded stream stays predictable. The
/// carry signal does not depend on rows beyond the cap, and Bybit returns funding history
/// NEWEST-FIRST, so we keep the FIRST `FUNDING_HISTORY_CAP` rows (the newest ones).
pub const FUNDING_HISTORY_CAP: usize = 50;

/// Parses a Bybit ms-epoch JSON string into `i64`. An EMPTY string (e.g. `nextFundingTime` on spot
/// or when not applicable) is a legitimate "not applicable" sentinel → 0. Any other non-numeric
/// value is a hard error (fail-closed — never silently coerce to 0).
pub fn parse_i64(s: &str) -> Result<i64, BybitError> {
    let t = s.trim();
    if t.is_empty() {
        Ok(0)
    } else {
        t.parse::<i64>()
            .map_err(|_| BybitError::InvalidArg("non-numeric timestamp"))
    }
}

/// Builds the `PerpQuote` frame from a linear `TickerItem` + the instruments-info funding interval.
/// `server_ts_ms` is the tickers envelope `time` (see msg.rs note). Numerics parsed string→f64
/// (fail-closed on non-finite); `next_funding_time` parsed string→i64.
pub fn perp_quote_frame(
    item: &TickerItem,
    server_ts_ms: i64,
    funding_interval_min: i64,
) -> Result<QuotesFrame, BybitError> {
    Ok(QuotesFrame::PerpQuote {
        server_ts_ms,
        bid: parse_f64(&item.bid1_price)?,
        ask: parse_f64(&item.ask1_price)?,
        last: parse_f64(&item.last_price)?,
        funding_rate: parse_f64(&item.funding_rate)?,
        next_funding_time_ms: parse_i64(&item.next_funding_time)?,
        funding_interval_min,
    })
}

/// Builds the `SpotQuote` frame from a spot `TickerItem`. `server_ts_ms` is the tickers envelope
/// `time`. Numerics parsed string→f64 (fail-closed on non-finite).
pub fn spot_quote_frame(item: &TickerItem, server_ts_ms: i64) -> Result<QuotesFrame, BybitError> {
    Ok(QuotesFrame::SpotQuote {
        server_ts_ms,
        bid: parse_f64(&item.bid1_price)?,
        ask: parse_f64(&item.ask1_price)?,
        last: parse_f64(&item.last_price)?,
    })
}

/// Reads the funding interval (MINUTES) from the instruments-info list. Returns 0 if the list is
/// empty (the caller keeps it; the consumer must not assume 480). Carries the REAL interval — never
/// hardcodes 8h/480.
pub fn funding_interval_min(items: &[InstrumentInfoItem]) -> i64 {
    items.first().map(|i| i.funding_interval).unwrap_or(0)
}

/// Builds the capped, newest-first `FundingRow` frames from a Bybit funding-history list. Bybit
/// returns rows NEWEST-FIRST, so taking the FIRST [`FUNDING_HISTORY_CAP`] keeps the newest (SF-4).
/// Numerics parsed string→f64 / string→i64 (fail-closed). Returns the frames plus whether the input
/// was truncated (so the caller can log it — mirrors the reconcile cap's surfacing).
pub fn funding_row_frames(
    items: &[FundingHistoryItem],
) -> Result<(Vec<QuotesFrame>, bool), BybitError> {
    let truncated = items.len() > FUNDING_HISTORY_CAP;
    let frames = items
        .iter()
        .take(FUNDING_HISTORY_CAP)
        .map(|it| {
            Ok(QuotesFrame::FundingRow {
                funding_time_ms: parse_i64(&it.funding_rate_timestamp)?,
                funding_rate: parse_f64(&it.funding_rate)?,
            })
        })
        .collect::<Result<Vec<_>, BybitError>>()?;
    Ok((frames, truncated))
}

#[cfg(test)]
mod tests {
    use bincode::config;
    use hftbacktest::types::{LiveEvent, QuotesFrame};

    use super::*;
    use crate::bybit::{
        msg::{FundingHistoryResponse, InstrumentsInfoResponse, TickersResponse},
        reconcile::RET_MSG_CAP,
    };

    // --- 512B frame-size test (mirror spot_order's frame_roundtrip_under_512b) ------------------

    /// Worst-case quotes reply frames (Begin/PerpQuote/SpotQuote/FundingRow/End with a 256B
    /// ret_msg) must each encode < MAX_PAYLOAD_SIZE (512).
    #[test]
    fn quotes_frame_roundtrip_under_512b() {
        let cfg = config::standard();

        let begin = QuotesFrame::Begin {
            request_id: u64::MAX,
            snapshot_ts_ns: i64::MAX,
        };
        let perp = QuotesFrame::PerpQuote {
            server_ts_ms: i64::MAX,
            bid: f64::MAX,
            ask: f64::MAX,
            last: f64::MAX,
            funding_rate: f64::MAX,
            next_funding_time_ms: i64::MAX,
            funding_interval_min: i64::MAX,
        };
        let spot = QuotesFrame::SpotQuote {
            server_ts_ms: i64::MAX,
            bid: f64::MAX,
            ask: f64::MAX,
            last: f64::MAX,
        };
        let row = QuotesFrame::FundingRow {
            funding_time_ms: i64::MAX,
            funding_rate: f64::MAX,
        };
        let end = QuotesFrame::End {
            request_id: u64::MAX,
            ok: false,
            ret_code: Some(i64::MIN),
            ret_msg: "x".repeat(RET_MSG_CAP),
            http_status: u16::MAX,
        };

        for frame in [begin, perp, spot, row, end] {
            // Encode the OUTER LiveEvent envelope (symbol + frame) — that is what crosses IPC.
            let ev = LiveEvent::QuotesReply {
                symbol: "ETHUSDT".to_string(),
                frame,
            };
            let bytes = bincode::encode_to_vec(&ev, cfg).unwrap();
            assert!(bytes.len() < 512, "encoded frame {} bytes >= 512", bytes.len());
        }
    }

    // --- frame round-trips (encode→decode equal) ----------------------------------------------

    fn roundtrip(frame: QuotesFrame) -> QuotesFrame {
        let cfg = config::standard();
        let ev = LiveEvent::QuotesReply {
            symbol: "ETHUSDT".to_string(),
            frame,
        };
        let bytes = bincode::encode_to_vec(&ev, cfg).unwrap();
        let (decoded, _): (LiveEvent, usize) = bincode::decode_from_slice(&bytes, cfg).unwrap();
        match decoded {
            LiveEvent::QuotesReply { frame, .. } => frame,
            other => panic!("expected QuotesReply, got {other:?}"),
        }
    }

    /// A fail-closed `End{ok:false}` carrying a Bybit reject retCode must round-trip faithfully (raw
    /// retCode preserved, never collapsed to a bool).
    #[test]
    fn quotes_end_preserves_retcode() {
        let end = QuotesFrame::End {
            request_id: 9,
            ok: false,
            ret_code: Some(10001), // Bybit: params error / bad symbol
            ret_msg: "params error".to_string(),
            http_status: 200,
        };
        match roundtrip(end) {
            QuotesFrame::End { ok, ret_code, ret_msg, .. } => {
                assert!(!ok);
                assert_eq!(ret_code, Some(10001));
                assert_eq!(ret_msg, "params error");
            }
            f => panic!("expected End, got {f:?}"),
        }
    }

    /// A parsed quote carries the server timestamp so the consumer can age it (design §0 D-c).
    #[test]
    fn quotes_timestamp_carried() {
        let perp = perp_quote_frame(&perp_item(), 1_718_000_000_000, 480).unwrap();
        match roundtrip(perp) {
            QuotesFrame::PerpQuote { server_ts_ms, .. } => {
                assert_eq!(server_ts_ms, 1_718_000_000_000);
            }
            f => panic!("expected PerpQuote, got {f:?}"),
        }
    }

    // --- representative Bybit JSON parse tests -------------------------------------------------

    fn perp_item() -> TickerItem {
        TickerItem {
            symbol: "ETHUSDT".to_string(),
            bid1_price: "3000.10".to_string(),
            ask1_price: "3000.20".to_string(),
            last_price: "3000.15".to_string(),
            funding_rate: "0.0001".to_string(),
            next_funding_time: "1718028800000".to_string(),
        }
    }

    /// Parse a representative linear `GET /v5/market/tickers` reply and build the PerpQuote.
    #[test]
    fn quotes_parse_perp_ticker() {
        let json = r#"{"retCode":0,"retMsg":"OK","result":{"category":"linear","list":[{
            "symbol":"ETHUSDT","lastPrice":"3000.15","bid1Price":"3000.10","ask1Price":"3000.20",
            "markPrice":"3000.12","indexPrice":"3000.11","fundingRate":"0.0001",
            "nextFundingTime":"1718028800000","openInterest":"123.45","volume24h":"99999"
        }]},"retExtInfo":{},"time":1718000000000}"#;
        let resp: TickersResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.ret_code, 0);
        let list = resp.result.list.unwrap();
        assert_eq!(list.len(), 1);
        let frame = perp_quote_frame(&list[0], resp.time, 480).unwrap();
        match frame {
            QuotesFrame::PerpQuote {
                server_ts_ms,
                bid,
                ask,
                last,
                funding_rate,
                next_funding_time_ms,
                funding_interval_min,
            } => {
                assert_eq!(server_ts_ms, 1718000000000);
                assert_eq!(bid, 3000.10);
                assert_eq!(ask, 3000.20);
                assert_eq!(last, 3000.15);
                assert_eq!(funding_rate, 0.0001);
                assert_eq!(next_funding_time_ms, 1718028800000);
                assert_eq!(funding_interval_min, 480);
            }
            f => panic!("expected PerpQuote, got {f:?}"),
        }
    }

    /// Parse a representative spot `GET /v5/market/tickers` reply (no funding fields) and build the
    /// SpotQuote.
    #[test]
    fn quotes_parse_spot_ticker() {
        let json = r#"{"retCode":0,"retMsg":"OK","result":{"category":"spot","list":[{
            "symbol":"ETHUSDT","lastPrice":"2999.95","bid1Price":"2999.90","ask1Price":"3000.00",
            "highPrice24h":"3100","lowPrice24h":"2900","volume24h":"12345"
        }]},"retExtInfo":{},"time":1718000000001}"#;
        let resp: TickersResponse = serde_json::from_str(json).unwrap();
        let list = resp.result.list.unwrap();
        let frame = spot_quote_frame(&list[0], resp.time).unwrap();
        match frame {
            QuotesFrame::SpotQuote {
                server_ts_ms,
                bid,
                ask,
                last,
            } => {
                assert_eq!(server_ts_ms, 1718000000001);
                assert_eq!(bid, 2999.90);
                assert_eq!(ask, 3000.00);
                assert_eq!(last, 2999.95);
            }
            f => panic!("expected SpotQuote, got {f:?}"),
        }
    }

    /// Parse a representative `GET /v5/market/funding/history` reply (NEWEST-FIRST) into rows.
    #[test]
    fn quotes_parse_funding_history() {
        let json = r#"{"retCode":0,"retMsg":"OK","result":{"category":"linear","list":[
            {"symbol":"ETHUSDT","fundingRate":"0.0002","fundingRateTimestamp":"1718028800000"},
            {"symbol":"ETHUSDT","fundingRate":"0.0001","fundingRateTimestamp":"1718000000000"}
        ]},"retExtInfo":{},"time":1718000000000}"#;
        let resp: FundingHistoryResponse = serde_json::from_str(json).unwrap();
        let list = resp.result.list.unwrap();
        let (frames, truncated) = funding_row_frames(&list).unwrap();
        assert!(!truncated);
        assert_eq!(frames.len(), 2);
        // Newest-first preserved.
        match frames[0] {
            QuotesFrame::FundingRow { funding_time_ms, funding_rate } => {
                assert_eq!(funding_time_ms, 1718028800000);
                assert_eq!(funding_rate, 0.0002);
            }
            ref f => panic!("expected FundingRow, got {f:?}"),
        }
        match frames[1] {
            QuotesFrame::FundingRow { funding_time_ms, .. } => {
                assert_eq!(funding_time_ms, 1718000000000);
            }
            ref f => panic!("expected FundingRow, got {f:?}"),
        }
    }

    /// Parse a representative `GET /v5/market/instruments-info` reply for `fundingInterval` (MINUTES,
    /// a JSON NUMBER not a string).
    #[test]
    fn quotes_parse_instruments_info() {
        let json = r#"{"retCode":0,"retMsg":"OK","result":{"category":"linear","list":[{
            "symbol":"ETHUSDT","contractType":"LinearPerpetual","status":"Trading",
            "fundingInterval":480,"baseCoin":"ETH","quoteCoin":"USDT"
        }]},"retExtInfo":{},"time":1718000000000}"#;
        let resp: InstrumentsInfoResponse = serde_json::from_str(json).unwrap();
        let list = resp.result.list.unwrap();
        assert_eq!(funding_interval_min(&list), 480);
        // Empty list → 0 (consumer must not assume 480).
        assert_eq!(funding_interval_min(&[]), 0);
    }

    /// SF-4: feeding > CAP rows (newest-first) emits exactly CAP `FundingRow`s and keeps the NEWEST.
    #[test]
    fn funding_history_cap_keeps_newest() {
        let n = FUNDING_HISTORY_CAP + 10;
        // Build newest-first: index 0 is the newest (largest timestamp).
        let items: Vec<FundingHistoryItem> = (0..n)
            .map(|i| FundingHistoryItem {
                symbol: "ETHUSDT".to_string(),
                funding_rate: "0.0001".to_string(),
                // newest-first: ts decreases as i increases.
                funding_rate_timestamp: (1_718_000_000_000_i64 - i as i64).to_string(),
            })
            .collect();
        let (frames, truncated) = funding_row_frames(&items).unwrap();
        assert!(truncated, "input over cap must be flagged truncated");
        assert_eq!(frames.len(), FUNDING_HISTORY_CAP);
        // The first kept frame is the NEWEST (largest ts); the last kept is at index CAP-1.
        match frames[0] {
            QuotesFrame::FundingRow { funding_time_ms, .. } => {
                assert_eq!(funding_time_ms, 1_718_000_000_000);
            }
            ref f => panic!("expected FundingRow, got {f:?}"),
        }
        match frames[FUNDING_HISTORY_CAP - 1] {
            QuotesFrame::FundingRow { funding_time_ms, .. } => {
                assert_eq!(
                    funding_time_ms,
                    1_718_000_000_000 - (FUNDING_HISTORY_CAP as i64 - 1),
                    "kept rows must be the newest contiguous block (the dropped rows are older)"
                );
            }
            ref f => panic!("expected FundingRow, got {f:?}"),
        }
    }

    /// A non-finite numeric anywhere in a quote fails the whole pull (fail-closed via parse_f64).
    #[test]
    fn quotes_non_finite_fails_closed() {
        let mut bad = perp_item();
        bad.bid1_price = "NaN".to_string();
        assert!(perp_quote_frame(&bad, 1, 480).is_err());
        let mut bad2 = perp_item();
        bad2.funding_rate = "inf".to_string();
        assert!(perp_quote_frame(&bad2, 1, 480).is_err());
    }

    /// `parse_i64`: empty → 0, valid → parsed, garbage → err.
    #[test]
    fn parse_i64_handles_empty_and_invalid() {
        assert_eq!(parse_i64("").unwrap(), 0);
        assert_eq!(parse_i64("  ").unwrap(), 0);
        assert_eq!(parse_i64("1718028800000").unwrap(), 1718028800000);
        assert!(parse_i64("not-a-number").is_err());
    }

    /// A null/error funding-history list is guarded (no panic).
    #[test]
    fn funding_history_null_list_guarded() {
        let json = r#"{"retCode":10001,"retMsg":"params error","result":{"list":null},"retExtInfo":{},"time":1}"#;
        let resp: FundingHistoryResponse = serde_json::from_str(json).unwrap();
        assert_ne!(resp.ret_code, 0);
        assert!(resp.result.list.is_none());
    }
}
