use std::{collections::HashMap, fmt, fmt::Debug};

use hftbacktest::types::{OrdType, Side, Status, TimeInForce};
use serde::{
    Deserialize,
    Deserializer,
    Serialize,
    de,
    de::{Error, Unexpected, Visitor},
};

use crate::utils::{from_str_to_f64, from_str_to_f64_opt, from_str_to_i64};

struct SideVisitor;

impl Visitor<'_> for SideVisitor {
    type Value = Side;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string containing \"Buy\" or \"Sell\"")
    }

    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        match s {
            "Buy" => Ok(Side::Buy),
            "Sell" => Ok(Side::Sell),
            s => Err(Error::invalid_value(Unexpected::Other(s), &"Buy or Sell")),
        }
    }
}

fn from_str_to_side<'de, D>(deserializer: D) -> Result<Side, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_str(SideVisitor)
}

struct OrdTypeVisitor;

impl Visitor<'_> for OrdTypeVisitor {
    type Value = OrdType;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string containing \"Market\" or \"Limit\"")
    }

    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        match s {
            "Market" => Ok(OrdType::Market),
            "Limit" => Ok(OrdType::Limit),
            s => Err(Error::invalid_value(
                Unexpected::Other(s),
                &"Market or Limit",
            )),
        }
    }
}

fn from_str_to_ord_type<'de, D>(deserializer: D) -> Result<OrdType, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_str(OrdTypeVisitor)
}

struct TimeInForceVisitor;

impl Visitor<'_> for TimeInForceVisitor {
    type Value = TimeInForce;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string containing \"IOC\" or \"GTC\"")
    }

    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        match s {
            "IOC" => Ok(TimeInForce::IOC),
            "GTC" => Ok(TimeInForce::GTC),
            "FOK" => Ok(TimeInForce::FOK),
            "PostOnly" => Ok(TimeInForce::GTX),
            s => Err(Error::invalid_value(Unexpected::Other(s), &"IOC or GTC")),
        }
    }
}

fn from_str_to_time_in_force<'de, D>(deserializer: D) -> Result<TimeInForce, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_str(TimeInForceVisitor)
}

struct StatusVisitor;

impl Visitor<'_> for StatusVisitor {
    type Value = Status;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string containing \"IOC\" or \"GTC\"")
    }

    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        match s {
            "New" => Ok(Status::New),
            "PartiallyFilled" => Ok(Status::PartiallyFilled),
            "Untriggered" => Ok(Status::Unsupported),
            "Rejected" => Ok(Status::Expired),
            "PartiallyFilledCanceled" => Ok(Status::Canceled),
            "Filled" => Ok(Status::Filled),
            "Cancelled" => Ok(Status::Canceled),
            "Triggered" => Ok(Status::Unsupported),
            "Deactivated" => Ok(Status::Unsupported),
            s => Err(Error::invalid_value(Unexpected::Other(s), &"IOC or GTC")),
        }
    }
}

fn from_str_to_status<'de, D>(deserializer: D) -> Result<Status, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_str(StatusVisitor)
}

#[derive(Serialize, Debug)]
pub struct Op {
    pub req_id: String,
    pub op: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
}

#[derive(Deserialize, Debug)]
pub struct OpResponse {
    pub success: Option<bool>,
    pub ret_msg: Option<String>,
    pub conn_id: Option<String>,
    pub op: String,
    #[serde(default)]
    pub args: Vec<String>,
    pub req_id: Option<String>,
    #[serde(rename = "failTopics", default)]
    pub fail_topics: Vec<String>,
    #[serde(rename = "successTopics", default)]
    pub success_topics: Vec<String>,
    #[serde(rename = "type")]
    pub ty: Option<String>,
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum PublicStreamMsg {
    Topic(PublicStream),
    Op(OpResponse),
}

#[derive(Deserialize, Debug)]
pub struct PublicStream {
    pub topic: String,
    pub ts: i64,
    pub data: serde_json::Value,
    pub cts: Option<i64>,
}

#[derive(Deserialize, Debug)]
pub struct OrderBook {
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "b")]
    pub bids: Vec<(String, String)>,
    #[serde(rename = "a")]
    pub asks: Vec<(String, String)>,
    #[serde(rename = "u")]
    pub update_id: i64,
    pub seq: i64,
}

#[derive(Deserialize, Debug)]
pub struct Trade {
    #[serde(rename = "T")]
    pub ts: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "S")]
    #[serde(deserialize_with = "from_str_to_side")]
    pub side: Side,
    #[serde(rename = "v")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub trade_size: f64,
    #[serde(rename = "p")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub trade_price: f64,
    #[serde(rename = "L")]
    pub direction: String,
    #[serde(rename = "i")]
    pub trade_id: String,
    #[serde(rename = "BT")]
    pub block_trade: bool,
    #[serde(rename = "mP")]
    #[serde(default)]
    #[serde(deserialize_with = "from_str_to_f64_opt")]
    pub mark_price: Option<f64>,
    #[serde(rename = "iP")]
    #[serde(default)]
    #[serde(deserialize_with = "from_str_to_f64_opt")]
    pub index_price: Option<f64>,
    #[serde(rename = "mIv")]
    #[serde(default)]
    #[serde(deserialize_with = "from_str_to_f64_opt")]
    pub mark_iv: Option<f64>,
    #[serde(default)]
    #[serde(deserialize_with = "from_str_to_f64_opt")]
    pub iv: Option<f64>,
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum PrivateStreamMsg {
    Topic(PrivateStreamTopicMsg),
    Op(OpResponse),
}

#[derive(Deserialize, Debug)]
#[serde(tag = "topic")]
pub enum PrivateStreamTopicMsg {
    #[serde(rename = "position")]
    Position(PrivateStream<Vec<Position>>),
    #[serde(rename = "execution")]
    Execution(PrivateStream<Vec<Execution>>),
    #[serde(rename = "execution.fast")]
    FastExecution(PrivateStream<Vec<FastExecution>>),
    #[serde(rename = "order")]
    Order(PrivateStream<Vec<PrivateOrder>>),
}

#[derive(Deserialize, Debug)]
#[serde(bound = "for <'a> T: Deserialize<'a>")]
pub struct PrivateStream<T>
where
    for<'a> T: Deserialize<'a> + Debug,
{
    #[serde(default)]
    pub id: Option<String>,
    #[serde(rename = "creationTime")]
    pub creation_time: i64,
    pub data: T,
}

#[derive(Deserialize, Debug)]
pub struct Position {
    #[serde(rename = "positionIdx")]
    pub position_idx: i64,
    #[serde(rename = "tradeMode")]
    pub trade_mode: i64,
    #[serde(rename = "riskId")]
    pub risk_id: i64,
    #[serde(rename = "riskLimitValue")]
    pub risk_limit_value: String,
    pub symbol: String,
    pub side: String,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub size: f64,
    #[serde(rename = "entryPrice", default)]
    #[serde(deserialize_with = "from_str_to_f64_opt")]
    pub entry_price: Option<f64>,
    pub leverage: String,
    #[serde(rename = "positionValue")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub position_value: f64,
    #[serde(rename = "positionBalance")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub position_balance: f64,
    #[serde(rename = "markPrice")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub mark_price: f64,
    #[serde(rename = "positionIM")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub position_im: f64,
    #[serde(rename = "positionMM")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub position_mm: f64,
    #[serde(rename = "takeProfit")]
    pub take_profit: String,
    #[serde(rename = "stopLoss")]
    pub stop_loss: String,
    #[serde(rename = "trailingStop")]
    pub trailing_stop: String,
    #[serde(rename = "unrealisedPnl")]
    pub unrealised_pnl: String,
    #[serde(rename = "curRealisedPnl")]
    pub cur_realised_pnl: String,
    #[serde(rename = "cumRealisedPnl")]
    pub cum_realised_pnl: String,
    #[serde(rename = "sessionAvgPrice")]
    pub session_avg_price: String,
    #[serde(rename = "createdTime")]
    #[serde(deserialize_with = "from_str_to_i64")]
    pub created_time: i64,
    #[serde(rename = "updatedTime")]
    #[serde(deserialize_with = "from_str_to_i64")]
    pub updated_time: i64,
    #[serde(rename = "tpslMode")]
    pub tpsl_mode: String,
    #[serde(rename = "liqPrice", default)]
    #[serde(deserialize_with = "from_str_to_f64_opt")]
    pub liq_price: Option<f64>,
    #[serde(rename = "bustPrice", default)]
    #[serde(deserialize_with = "from_str_to_f64_opt")]
    pub bust_price: Option<f64>,
    #[serde(default)]
    pub category: Option<String>,
    #[serde(rename = "positionStatus")]
    pub position_status: String,
    #[serde(rename = "adlRankIndicator")]
    pub adl_rank_indicator: i64,
    #[serde(rename = "autoAddMargin")]
    pub auto_add_margin: i64,
    #[serde(rename = "leverageSysUpdatedTime")]
    pub leverage_sys_updated_time: String,
    #[serde(rename = "mmrSysUpdatedTime")]
    pub mmr_sys_updated_time: String,
    pub seq: i64,
    #[serde(rename = "isReduceOnly")]
    pub is_reduce_only: bool,
}

#[derive(Deserialize, Debug)]
pub struct Execution {
    pub category: String,
    pub symbol: String,
    #[serde(rename = "execFee")]
    pub exec_fee: String,
    #[serde(rename = "execId")]
    pub exec_id: String,
    #[serde(rename = "execPrice")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub exec_price: f64,
    #[serde(rename = "execQty")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub exec_qty: f64,
    #[serde(rename = "execType")]
    pub exec_type: String,
    #[serde(rename = "execValue")]
    pub exec_value: String,
    #[serde(rename = "isMaker")]
    pub is_maker: bool,
    #[serde(rename = "feeRate")]
    pub fee_rate: String,
    #[serde(rename = "tradeIv")]
    pub trade_iv: String,
    #[serde(rename = "markIv")]
    pub mark_iv: String,
    #[serde(rename = "blockTradeId")]
    pub block_trade_id: String,
    #[serde(rename = "markPrice")]
    pub mark_price: String,
    #[serde(rename = "indexPrice")]
    pub index_price: String,
    #[serde(rename = "underlyingPrice")]
    pub underlying_price: String,
    #[serde(rename = "leavesQty")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub leaves_qty: f64,
    #[serde(rename = "orderId")]
    pub order_id: String,
    #[serde(rename = "orderLinkId")]
    pub order_link_id: String,
    #[serde(rename = "orderPrice")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub order_price: f64,
    #[serde(rename = "orderQty")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub order_qty: f64,
    #[serde(rename = "orderType")]
    pub order_type: String,
    #[serde(rename = "stopOrderType")]
    pub stop_order_type: String,
    pub side: String,
    #[serde(rename = "execTime")]
    #[serde(deserialize_with = "from_str_to_i64")]
    pub exec_time: i64,
    #[serde(rename = "isLeverage")]
    pub is_leverage: String,
    #[serde(rename = "closedSize")]
    pub closed_size: String,
    pub seq: i64,
}

#[derive(Deserialize, Debug)]
pub struct FastExecution {
    pub category: String,
    pub symbol: String,
    #[serde(rename = "execId")]
    pub exec_id: String,
    #[serde(rename = "execPrice")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub exec_price: f64,
    #[serde(rename = "execQty")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub exec_qty: f64,
    #[serde(rename = "orderId")]
    pub order_id: String,
    #[serde(rename = "orderLinkId")]
    pub order_link_id: String,
    #[serde(deserialize_with = "from_str_to_side")]
    pub side: Side,
    #[serde(rename = "execTime")]
    #[serde(deserialize_with = "from_str_to_i64")]
    pub exec_time: i64,
    pub seq: i64,
}

#[derive(Deserialize, Debug)]
pub struct PrivateOrder {
    pub symbol: String,
    #[serde(rename = "orderId")]
    pub order_id: String,
    #[serde(deserialize_with = "from_str_to_side")]
    pub side: Side,
    #[serde(rename = "orderType")]
    #[serde(deserialize_with = "from_str_to_ord_type")]
    pub order_type: OrdType,
    #[serde(rename = "cancelType")]
    pub cancel_type: String,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub price: f64,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub qty: f64,
    #[serde(rename = "orderIv")]
    pub order_iv: String,
    #[serde(rename = "timeInForce")]
    #[serde(deserialize_with = "from_str_to_time_in_force")]
    pub time_in_force: TimeInForce,
    #[serde(rename = "orderStatus")]
    #[serde(deserialize_with = "from_str_to_status")]
    pub order_status: Status,
    #[serde(rename = "orderLinkId")]
    pub order_link_id: String,
    #[serde(rename = "lastPriceOnCreated")]
    pub last_price_on_created: String,
    #[serde(rename = "reduceOnly")]
    pub reduce_only: bool,
    #[serde(rename = "leavesQty")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub leaves_qty: f64,
    #[serde(rename = "leavesValue")]
    pub leaves_value: String,
    #[serde(rename = "cumExecQty")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub cum_exec_qty: f64,
    #[serde(rename = "cumExecValue")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub cum_exec_value: f64,
    #[serde(rename = "avgPrice")]
    pub avg_price: String,
    #[serde(rename = "blockTradeId")]
    pub block_trade_id: String,
    #[serde(rename = "positionIdx")]
    pub position_idx: i64,
    #[serde(rename = "cumExecFee")]
    pub cum_exec_fee: String,
    #[serde(rename = "createdTime")]
    #[serde(deserialize_with = "from_str_to_i64")]
    pub created_time: i64,
    #[serde(rename = "updatedTime")]
    #[serde(deserialize_with = "from_str_to_i64")]
    pub updated_time: i64,
    #[serde(rename = "rejectReason")]
    pub reject_reason: String,
    #[serde(rename = "stopOrderType")]
    pub stop_order_type: String,
    #[serde(rename = "tpslMode")]
    pub tpsl_mode: String,
    #[serde(rename = "triggerPrice")]
    pub trigger_price: String,
    #[serde(rename = "takeProfit")]
    pub take_profit: String,
    #[serde(rename = "stopLoss")]
    pub stop_loss: String,
    #[serde(rename = "tpTriggerBy")]
    pub tp_trigger_by: String,
    #[serde(rename = "slTriggerBy")]
    pub sl_trigger_by: String,
    #[serde(rename = "tpLimitPrice")]
    pub tp_limit_price: String,
    #[serde(rename = "slLimitPrice")]
    pub sl_limit_price: String,
    #[serde(rename = "triggerDirection")]
    pub trigger_direction: i64,
    #[serde(rename = "triggerBy")]
    pub trigger_by: String,
    #[serde(rename = "closeOnTrigger")]
    pub close_on_trigger: bool,
    pub category: String,
    #[serde(rename = "placeType")]
    pub place_type: String,
    #[serde(rename = "smpType")]
    pub smp_type: String,
    #[serde(rename = "smpGroup")]
    pub smp_group: i64,
    #[serde(rename = "smpOrderId")]
    pub smp_order_id: String,
    #[serde(rename = "feeCurrency")]
    pub fee_currency: String,
}

#[derive(Deserialize, Debug)]
pub struct OrderResponseData {
    #[serde(rename = "orderId")]
    pub order_id: String,
    #[serde(rename = "orderLinkId")]
    pub order_link_id: String,
}

// --- Spot order (B-M1a) -----------------------------------------------------------------------
//
// Bybit `POST /v5/order/create` and `/v5/order/cancel` return `result: {orderId, orderLinkId}`.
// `result` is `{}` (empty object) on a non-zero-retCode reject, so all fields are
// `#[serde(default)]` (fail-soft: an absent field stays empty; correctness is keyed on `retCode`).

#[derive(Deserialize, Debug, Default)]
pub struct OrderCreateResult {
    #[serde(rename = "orderId", default)]
    pub order_id: String,
    #[serde(rename = "orderLinkId", default)]
    pub order_link_id: String,
}

#[derive(Deserialize, Debug)]
pub struct OrderCreateResponse {
    #[serde(rename = "retCode")]
    pub ret_code: i64,
    #[serde(rename = "retMsg")]
    pub ret_msg: String,
    #[serde(default)]
    pub result: OrderCreateResult,
}

/// A spot order item from `GET /v5/order/realtime?category=spot`, keeping `orderStatus` as a RAW
/// string so the spot RPC can map it fail-closed to [`SpotOrderStatus`](hftbacktest::types::SpotOrderStatus)
/// (`Other` on unknown). Distinct from [`PrivateOrder`] whose `from_str_to_status` deserializer is
/// lossy (`Rejected`→`Expired`) and HARD-FAILS on an unknown status (B-M1a deviation). Only the
/// reconcile-relevant fields are deserialized; everything else is ignored. Numerics stay strings
/// (parsed string→f64 on the connector, fail-closed on non-finite).
#[derive(Deserialize, Debug, Default)]
pub struct SpotOrderItem {
    #[serde(rename = "orderLinkId", default)]
    pub order_link_id: String,
    #[serde(rename = "orderId", default)]
    pub order_id: String,
    #[serde(rename = "orderStatus", default)]
    pub order_status: String,
    #[serde(rename = "cumExecQty", default)]
    pub cum_exec_qty: String,
    #[serde(rename = "avgPrice", default)]
    pub avg_price: String,
}

// --- Quotes (B-M1b: tickers / funding-history / instruments-info) -----------------------------
//
// All three are UNSIGNED public `/v5/market/*` GETs. Their envelope is the standard
// `{retCode, retMsg, result:{...}, time}` shape, but each `result` differs, so each gets a
// dedicated response struct (precedent: `WalletBalanceResponse`). Per-symbol numerics are JSON
// STRINGS (parsed string→f64 on the connector, fail-closed on non-finite via `parse_f64`). Every
// field is `#[serde(default)]` so a missing field is fail-soft (empty string → 0.0 later) rather
// than a hard deserialize failure; correctness is keyed on `retCode`.
//
// [verify — design §E] Bybit `/v5/market/tickers` carries an ENVELOPE-level `time` (ms) — the
// per-symbol ticker item does NOT carry its own `time`. So `server_ts_ms` is sourced from the
// envelope `time` here. Confirm on testnet whether a per-symbol `time` exists (would tighten skew).

/// Tickers envelope (`GET /v5/market/tickers?category={linear|spot}&symbol=...`). The same shape is
/// used for both categories; linear-only fields (`fundingRate`/`nextFundingTime`) are absent on spot
/// and default to empty strings.
#[derive(Deserialize, Debug)]
pub struct TickersResponse {
    #[serde(rename = "retCode")]
    pub ret_code: i64,
    #[serde(rename = "retMsg")]
    pub ret_msg: String,
    #[serde(default)]
    pub result: TickersResult,
    /// Envelope-level server timestamp (ms). Carried per quote as `server_ts_ms` (see module note).
    #[serde(default)]
    pub time: i64,
}

#[derive(Deserialize, Debug, Default)]
pub struct TickersResult {
    /// `null` on error envelopes → guarded `Option` (never `.unwrap()`).
    #[serde(default)]
    pub list: Option<Vec<TickerItem>>,
}

/// One ticker item. Numeric JSON strings (parsed string→f64; "" → 0.0). `funding_rate`/
/// `next_funding_time` are linear-only (empty on spot → 0.0 / 0). `next_funding_time` is a ms epoch
/// STRING on Bybit (parsed string→i64 on the connector).
#[derive(Deserialize, Debug, Default)]
pub struct TickerItem {
    #[serde(default)]
    pub symbol: String,
    #[serde(rename = "bid1Price", default)]
    pub bid1_price: String,
    #[serde(rename = "ask1Price", default)]
    pub ask1_price: String,
    #[serde(rename = "lastPrice", default)]
    pub last_price: String,
    #[serde(rename = "fundingRate", default)]
    pub funding_rate: String,
    #[serde(rename = "nextFundingTime", default)]
    pub next_funding_time: String,
}

/// Funding-history envelope (`GET /v5/market/funding/history?category=linear&symbol=...&limit=N`).
/// Bybit returns rows NEWEST-FIRST.
#[derive(Deserialize, Debug)]
pub struct FundingHistoryResponse {
    #[serde(rename = "retCode")]
    pub ret_code: i64,
    #[serde(rename = "retMsg")]
    pub ret_msg: String,
    #[serde(default)]
    pub result: FundingHistoryResult,
}

#[derive(Deserialize, Debug, Default)]
pub struct FundingHistoryResult {
    #[serde(default)]
    pub list: Option<Vec<FundingHistoryItem>>,
}

/// One funding-history row. `funding_rate` and `funding_rate_timestamp` are JSON STRINGS (rate→f64,
/// timestamp ms→i64).
#[derive(Deserialize, Debug, Default)]
pub struct FundingHistoryItem {
    #[serde(default)]
    pub symbol: String,
    #[serde(rename = "fundingRate", default)]
    pub funding_rate: String,
    #[serde(rename = "fundingRateTimestamp", default)]
    pub funding_rate_timestamp: String,
}

/// Instruments-info envelope (`GET /v5/market/instruments-info?category=linear&symbol=...`). Carries
/// the real `fundingInterval` (MINUTES) so the consumer does NOT assume 8h/480.
#[derive(Deserialize, Debug)]
pub struct InstrumentsInfoResponse {
    #[serde(rename = "retCode")]
    pub ret_code: i64,
    #[serde(rename = "retMsg")]
    pub ret_msg: String,
    #[serde(default)]
    pub result: InstrumentsInfoResult,
}

#[derive(Deserialize, Debug, Default)]
pub struct InstrumentsInfoResult {
    #[serde(default)]
    pub list: Option<Vec<InstrumentInfoItem>>,
}

/// One instrument-info item. `funding_interval` is the funding interval in MINUTES (a JSON NUMBER on
/// Bybit, not a string — e.g. `480` for 8h).
#[derive(Deserialize, Debug, Default)]
pub struct InstrumentInfoItem {
    #[serde(default)]
    pub symbol: String,
    #[serde(rename = "fundingInterval", default)]
    pub funding_interval: i64,
}

#[derive(Deserialize, Debug)]
pub struct TradeStreamMsg {
    #[serde(rename = "reqId")]
    pub req_id: Option<String>,
    #[serde(rename = "retCode")]
    pub ret_code: i64,
    #[serde(rename = "retMsg")]
    pub ret_msg: String,
    pub op: String,
    #[serde(default)]
    pub data: serde_json::Value,
    #[serde(default)]
    pub header: HashMap<String, String>,
    #[serde(rename = "connId")]
    pub conn_id: String,
}

#[derive(Serialize, Debug)]
pub struct TradeOp<T>
where
    T: Serialize + Debug,
{
    #[serde(rename = "reqId")]
    pub req_id: String,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub header: HashMap<String, String>,
    pub op: &'static str,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<T>,
}

#[derive(Serialize, Clone, Debug)]
pub struct Order {
    pub symbol: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub side: Option<String>,
    #[serde(rename = "orderType")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub qty: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<String>,
    pub category: String,
    #[serde(rename = "timeInForce")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_in_force: Option<String>,
    #[serde(rename = "orderLinkId")]
    pub order_link_id: String,
}

#[derive(Deserialize, Debug)]
pub struct RestResult {
    pub list: Option<serde_json::Value>,
    #[serde(default)]
    pub success: String,
    // Bybit V5 REST returns `nextPageCursor` (camelCase, verified against docs 2026-06-14). The
    // original `next_page_cursor` rename never matched the wire — `alias` is additive (both keys
    // accepted) so existing single-page callers are unaffected while reconcile pagination works.
    #[serde(rename = "next_page_cursor", alias = "nextPageCursor")]
    #[serde(default)]
    pub next_page_cursor: String,
    #[serde(default)]
    pub category: String,
}

#[derive(Deserialize, Debug)]
pub struct RestResponse {
    #[serde(rename = "retCode")]
    pub ret_code: i64,
    #[serde(rename = "retMsg")]
    pub ret_msg: String,
    pub result: RestResult,
    #[serde(rename = "retExtInfo")]
    pub ret_ext_info: serde_json::Value,
    pub time: i64,
}

// --- Wallet balance (R-M1a reconcile) ---------------------------------------------------------
//
// `/v5/account/wallet-balance?accountType=UNIFIED` returns a NESTED `result.list[0].coin[]` shape
// that the flat `RestResult` (`list: Option<Value>`) cannot deserialize. Schema verified against
// Bybit V5 docs (design-note §5d, 2026-06-14): per-coin numerics are JSON STRINGS (parsed
// string→f64 on the connector); `collateralSwitch`/`marginCollateral` are Bybit BOOLEANS.
//
// All fields are `#[serde(default)]` so a missing field is fail-soft (empty string → 0.0 later)
// rather than a hard deserialize failure. The outer envelope (`retCode`/`retMsg`) is still the
// `RestResponse` shape — only `result` differs, hence a dedicated response struct.

#[derive(Deserialize, Debug)]
pub struct WalletBalanceResponse {
    #[serde(rename = "retCode")]
    pub ret_code: i64,
    #[serde(rename = "retMsg")]
    pub ret_msg: String,
    #[serde(default)]
    pub result: WalletBalanceResult,
}

#[derive(Deserialize, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct WalletBalanceResult {
    /// `null` on error envelopes → guarded `Option` (never `.unwrap()`).
    #[serde(default)]
    pub list: Option<Vec<AccountBalance>>,
}

#[derive(Deserialize, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct AccountBalance {
    // Account-level totals (D-d) — numeric JSON strings; emitted as one `Account` frame.
    // Acronym fields keep Bybit's verbatim casing via explicit `rename` (camelCase would yield
    // `totalPerpUpl`/`accountImRate`/`accountMmRate`, which do NOT match the wire — verified
    // against Bybit V5 docs 2026-06-14).
    #[serde(default)]
    pub total_equity: String,
    #[serde(default)]
    pub total_wallet_balance: String,
    #[serde(default)]
    pub total_margin_balance: String,
    #[serde(default)]
    pub total_available_balance: String,
    #[serde(default, rename = "totalPerpUPL")]
    pub total_perp_upl: String,
    #[serde(default)]
    pub total_initial_margin: String,
    #[serde(default)]
    pub total_maintenance_margin: String,
    #[serde(default, rename = "accountIMRate")]
    pub account_im_rate: String,
    #[serde(default, rename = "accountMMRate")]
    pub account_mm_rate: String,
    #[serde(default)]
    pub account_type: String,
    #[serde(default)]
    pub coin: Vec<CoinBalance>,
}

#[derive(Deserialize, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct CoinBalance {
    #[serde(default)]
    pub coin: String,
    // Numeric JSON strings (parsed string→f64 on the connector; "" → 0.0).
    #[serde(default)]
    pub wallet_balance: String,
    #[serde(default)]
    pub equity: String,
    #[serde(default)]
    pub usd_value: String,
    /// PM-only; `""` on Cross-margin → 0.0.
    #[serde(default)]
    pub spot_hedging_qty: String,
    #[serde(default)]
    pub borrow_amount: String,
    // Bybit BOOLEANS (not amounts).
    #[serde(default)]
    pub collateral_switch: bool,
    #[serde(default)]
    pub margin_collateral: bool,
}
