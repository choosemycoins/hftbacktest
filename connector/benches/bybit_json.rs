//! JSON serialization / deserialization benchmarks for the Bybit connector.
//!
//! The Bybit connector spends a large share of its hot-path time turning exchange WebSocket /
//! REST text into typed Rust structs (and the order path turning structs back into text). This
//! benchmark isolates exactly those calls so the cost can be tracked across parsing strategies.
//!
//! Run with:
//! ```text
//! cargo bench -p connector --bench bybit_json
//! ```
//!
//! Sample payloads under `benches/data/` are real Bybit V5 frames captured by the `collector`
//! (public market data) plus representative private / trade / REST frames built from the Bybit V5
//! API schema. Throughput is reported in bytes so deserialization results read as MB/s.
//!
//! ## Before / after: the public market-data decode strategies
//!
//! The public order-book / trade path is by far the highest-volume JSON the connector handles.
//! The `deser/orderbook` and `deser/trade` groups measure four strategies side by side on the
//! same samples, so the win from each change is directly comparable:
//!
//! * `serde_value`  — the **original** two-stage path: `from_str` into an envelope whose `data`
//!   field is a `serde_json::Value` DOM, then `from_value` re-walks that DOM into the typed
//!   struct. (Reproduced here with local types matching the pre-optimization code.)
//! * `serde_raw`    — the **current** path: `data` is a borrowed `RawValue`, parsed once directly
//!   into the typed struct. No intermediate DOM. (Uses the production `msg::PublicStream`.)
//! * `serde_direct` — single struct with a typed `data` field, parsed in one pass (reference; not
//!   used in production because the real `data` type is polymorphic on `topic`).
//! * `simd_live`    — the **current production live path**: `simd_parse::decode_public_feed`, which
//!   parses the frame once with `simd-json` (reusing warm scratch + `Buffers`) and reads fields
//!   directly from the DOM. This is the exact function the connector runs per WebSocket frame.

use std::{collections::HashMap, hint::black_box};

use connector::{
    bybit::{
        msg::{
            Op,
            OpResponse,
            Order,
            OrderBook,
            Position,
            PrivateStreamMsg,
            PublicStream,
            RestResponse,
            Trade,
            TradeOp,
            TradeStreamMsg,
        },
        simd_parse::decode_public_feed,
    },
    utils::parse_depth,
};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use serde::{Deserialize, de::DeserializeOwned};
use simd_json::Buffers;

// ----- Captured / representative payloads (embedded at compile time) -------------------------

const OB1_SNAPSHOT: &str = include_str!("data/orderbook_1_snapshot.json");
const OB50_DELTA: &str = include_str!("data/orderbook_50_delta.json");
const OB200_DELTA: &str = include_str!("data/orderbook_200_delta.json");
const OB200_SNAPSHOT: &str = include_str!("data/orderbook_200_snapshot.json");
const PUBLIC_TRADE: &str = include_str!("data/public_trade.json");

const PRIVATE_ORDER: &str = include_str!("data/private_order.json");
const PRIVATE_EXECUTION: &str = include_str!("data/private_execution.json");
const PRIVATE_POSITION: &str = include_str!("data/private_position.json");
const TRADE_ACK: &str = include_str!("data/trade_ack.json");
const REST_RESPONSE: &str = include_str!("data/rest_response.json");

/// Order book frames (every depth/type variant of `orderbook.*`).
fn orderbook_samples() -> Vec<(&'static str, &'static str)> {
    vec![
        ("orderbook.1.snapshot", OB1_SNAPSHOT.trim()),
        ("orderbook.50.delta", OB50_DELTA.trim()),
        ("orderbook.200.delta", OB200_DELTA.trim()),
        ("orderbook.200.snapshot", OB200_SNAPSHOT.trim()),
    ]
}

type DecodeFn = fn(&str);

// ----- Local reproduction of the ORIGINAL two-stage envelope (pre-optimization) --------------

/// Mirrors the pre-optimization `msg::PublicStream`: `data` materialized as a `serde_json::Value`.
#[derive(Deserialize)]
struct ValueStream {
    #[allow(dead_code)]
    topic: String,
    #[allow(dead_code)]
    ts: i64,
    data: serde_json::Value,
    #[allow(dead_code)]
    cts: Option<i64>,
}

/// Mirrors the pre-optimization `msg::PublicStreamMsg` untagged enum.
#[derive(Deserialize)]
#[serde(untagged)]
enum ValueStreamMsg {
    Topic(ValueStream),
    #[allow(dead_code)]
    Op(OpResponse),
}

// ----- Single-pass typed envelopes (used by `serde_direct` / `simd_direct`) ------------------

#[derive(Deserialize)]
struct OrderBookStreamDirect {
    #[allow(dead_code)]
    topic: String,
    #[allow(dead_code)]
    ts: i64,
    data: OrderBook,
    #[allow(dead_code)]
    cts: Option<i64>,
}

#[derive(Deserialize)]
struct TradeStreamDirect {
    #[allow(dead_code)]
    topic: String,
    #[allow(dead_code)]
    ts: i64,
    data: Vec<Trade>,
}

// ----- Order-book decode strategies ----------------------------------------------------------

/// Original: envelope -> `serde_json::Value` -> `from_value::<OrderBook>` -> `parse_depth`.
fn ob_serde_value(text: &str) {
    let msg: ValueStreamMsg = serde_json::from_str(text).unwrap();
    match msg {
        ValueStreamMsg::Topic(stream) => {
            let data: OrderBook = serde_json::from_value(stream.data).unwrap();
            black_box(parse_depth(data.bids, data.asks).unwrap());
        }
        ValueStreamMsg::Op(_) => unreachable!("sample is a topic frame"),
    }
}

/// Current production path: envelope with borrowed `RawValue` -> single typed parse.
fn ob_serde_raw(text: &str) {
    let stream: PublicStream<'_> = serde_json::from_str(text).unwrap();
    let data: OrderBook = serde_json::from_str(stream.data.unwrap().get()).unwrap();
    black_box(parse_depth(data.bids, data.asks).unwrap());
}

/// Reference: single typed struct, one pass, no intermediate value.
fn ob_serde_direct(text: &str) {
    let stream: OrderBookStreamDirect = serde_json::from_str(text).unwrap();
    black_box(parse_depth(stream.data.bids, stream.data.asks).unwrap());
}

// ----- Public-trade decode strategies --------------------------------------------------------

fn tr_serde_value(text: &str) {
    let msg: ValueStreamMsg = serde_json::from_str(text).unwrap();
    match msg {
        ValueStreamMsg::Topic(stream) => {
            let data: Vec<Trade> = serde_json::from_value(stream.data).unwrap();
            black_box(data);
        }
        ValueStreamMsg::Op(_) => unreachable!("sample is a topic frame"),
    }
}

fn tr_serde_raw(text: &str) {
    let stream: PublicStream<'_> = serde_json::from_str(text).unwrap();
    let data: Vec<Trade> = serde_json::from_str(stream.data.unwrap().get()).unwrap();
    black_box(data);
}

fn tr_serde_direct(text: &str) {
    let stream: TradeStreamDirect = serde_json::from_str(text).unwrap();
    black_box(stream.data);
}

// ----- Deserialization benchmarks: public market data (before / after) -----------------------

fn bench_orderbook(c: &mut Criterion) {
    let serde_strategies: [(&str, DecodeFn); 3] = [
        ("serde_value", ob_serde_value),
        ("serde_raw", ob_serde_raw),
        ("serde_direct", ob_serde_direct),
    ];

    let mut group = c.benchmark_group("deser/orderbook");
    for (sample, text) in orderbook_samples() {
        for (strategy, decode) in serde_strategies {
            decode(text); // smoke check before timing
            group.throughput(Throughput::Bytes(text.len() as u64));
            group.bench_function(format!("{sample}/{strategy}"), |b| {
                b.iter(|| decode(black_box(text)))
            });
        }
        // The real production live path: simd-json serde-bridge decode (tape -> typed struct, no
        // DOM) with warm, reusable buffers — exactly `simd_parse::decode_public_feed`.
        group.throughput(Throughput::Bytes(text.len() as u64));
        group.bench_function(format!("{sample}/simd_live"), |b| {
            let mut scratch: Vec<u8> = Vec::with_capacity(64 * 1024);
            let mut buffers = Buffers::new(64 * 1024);
            decode_public_feed(&mut scratch, &mut buffers, text).unwrap(); // smoke
            b.iter(|| {
                black_box(decode_public_feed(&mut scratch, &mut buffers, black_box(text)).unwrap());
            })
        });
    }
    group.finish();
}

fn bench_trade(c: &mut Criterion) {
    let serde_strategies: [(&str, DecodeFn); 3] = [
        ("serde_value", tr_serde_value),
        ("serde_raw", tr_serde_raw),
        ("serde_direct", tr_serde_direct),
    ];

    let text = PUBLIC_TRADE.trim();
    let mut group = c.benchmark_group("deser/trade");
    for (strategy, decode) in serde_strategies {
        decode(text); // smoke check
        group.throughput(Throughput::Bytes(text.len() as u64));
        group.bench_function(format!("publicTrade.snapshot/{strategy}"), |b| {
            b.iter(|| decode(black_box(text)))
        });
    }
    // The real production live path: simd-json decode with warm, reusable buffers.
    group.throughput(Throughput::Bytes(text.len() as u64));
    group.bench_function("publicTrade.snapshot/simd_live", |b| {
        let mut scratch: Vec<u8> = Vec::with_capacity(64 * 1024);
        let mut buffers = Buffers::new(64 * 1024);
        decode_public_feed(&mut scratch, &mut buffers, text).unwrap(); // smoke
        b.iter(|| {
            black_box(decode_public_feed(&mut scratch, &mut buffers, black_box(text)).unwrap());
        })
    });
    group.finish();
}

// ----- Deserialization benchmarks: private / account (now also via the simd-json serde bridge) -

/// simd-json serde-bridge parse, reusing `scratch` + `buffers`; mirrors the production `_with_buffers`
/// call applied to the private / trade / REST paths.
fn simd_parse<T: DeserializeOwned>(scratch: &mut Vec<u8>, buffers: &mut Buffers, text: &str) -> T {
    scratch.clear();
    scratch.extend_from_slice(text.as_bytes());
    simd_json::serde::from_slice_with_buffers(scratch.as_mut_slice(), buffers).unwrap()
}

/// Benchmark `serde_json::from_str` vs the simd-json serde bridge for one owned message type.
fn bench_serde_vs_simd<T: DeserializeOwned>(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    name: &str,
    text: &str,
) {
    // Correctness gate: both parsers must accept the sample.
    assert!(serde_json::from_str::<T>(text).is_ok(), "serde: sample `{name}` failed");
    {
        let mut scratch = Vec::with_capacity(8 * 1024);
        let mut buffers = Buffers::new(8 * 1024);
        let _: T = simd_parse(&mut scratch, &mut buffers, text);
    }
    group.throughput(Throughput::Bytes(text.len() as u64));
    group.bench_function(format!("{name}/serde"), |b| {
        b.iter(|| {
            let msg: T = serde_json::from_str(black_box(text)).unwrap();
            black_box(msg);
        })
    });
    group.throughput(Throughput::Bytes(text.len() as u64));
    group.bench_function(format!("{name}/simd"), |b| {
        let mut scratch = Vec::with_capacity(8 * 1024);
        let mut buffers = Buffers::new(8 * 1024);
        b.iter(|| {
            let msg: T = simd_parse(&mut scratch, &mut buffers, black_box(text));
            black_box(msg);
        })
    });
}

fn bench_private(c: &mut Criterion) {
    let mut group = c.benchmark_group("deser/private");
    bench_serde_vs_simd::<PrivateStreamMsg>(&mut group, "order", PRIVATE_ORDER.trim());
    bench_serde_vs_simd::<PrivateStreamMsg>(&mut group, "execution", PRIVATE_EXECUTION.trim());
    bench_serde_vs_simd::<PrivateStreamMsg>(&mut group, "position", PRIVATE_POSITION.trim());
    group.finish();
}

// Local baselines reproducing the PRE-cleanup types (payloads materialized as `serde_json::Value`).
#[allow(dead_code)]
#[derive(Deserialize)]
struct RestResultValueBaseline {
    list: Option<serde_json::Value>,
    #[serde(default)]
    success: String,
    #[serde(rename = "next_page_cursor", default)]
    next_page_cursor: String,
    #[serde(default)]
    category: String,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct RestResponseValueBaseline {
    #[serde(rename = "retCode")]
    ret_code: i64,
    #[serde(rename = "retMsg")]
    ret_msg: String,
    result: RestResultValueBaseline,
    #[serde(rename = "retExtInfo")]
    ret_ext_info: serde_json::Value,
    time: i64,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct TradeAckValueBaseline {
    #[serde(rename = "reqId")]
    req_id: Option<String>,
    #[serde(rename = "retCode")]
    ret_code: i64,
    #[serde(rename = "retMsg")]
    ret_msg: String,
    op: String,
    #[serde(default)]
    data: serde_json::Value,
    #[serde(default)]
    header: HashMap<String, String>,
    #[serde(rename = "connId")]
    conn_id: String,
}

// The plain-serde cleanup (no simd-json here — it regresses these message shapes). Measures the
// real before/after of eliminating intermediate / unused `serde_json::Value`s.
fn bench_account(c: &mut Criterion) {
    let mut group = c.benchmark_group("deser/account");

    // REST full decode (envelope + `Vec<Position>`): before = two-stage `Value`, after = `RawValue`.
    let rest = REST_RESPONSE.trim();
    group.throughput(Throughput::Bytes(rest.len() as u64));
    group.bench_function("rest_response/value_2stage", |b| {
        b.iter(|| {
            let resp: RestResponseValueBaseline = serde_json::from_str(black_box(rest)).unwrap();
            let positions: Vec<Position> =
                serde_json::from_value(resp.result.list.unwrap()).unwrap();
            black_box(positions);
        })
    });
    group.throughput(Throughput::Bytes(rest.len() as u64));
    group.bench_function("rest_response/raw_1pass", |b| {
        b.iter(|| {
            let resp: RestResponse = serde_json::from_str(black_box(rest)).unwrap();
            let positions: Vec<Position> =
                serde_json::from_str(resp.result.list.unwrap().get()).unwrap();
            black_box(positions);
        })
    });

    // Trade ack: before parses the unused `data` into a `Value`; after omits the field entirely.
    let ack = TRADE_ACK.trim();
    group.throughput(Throughput::Bytes(ack.len() as u64));
    group.bench_function("trade_ack/with_value", |b| {
        b.iter(|| {
            let msg: TradeAckValueBaseline = serde_json::from_str(black_box(ack)).unwrap();
            black_box(msg);
        })
    });
    group.throughput(Throughput::Bytes(ack.len() as u64));
    group.bench_function("trade_ack/no_value", |b| {
        b.iter(|| {
            let msg: TradeStreamMsg = serde_json::from_str(black_box(ack)).unwrap();
            black_box(msg);
        })
    });
    group.finish();
}

// ----- Serialization benchmarks (outgoing frames) -------------------------------------------

fn subscribe_op() -> Op {
    Op {
        req_id: "subscribe".to_string(),
        op: "subscribe".to_string(),
        args: vec![
            "orderbook.1.ETHUSDT".to_string(),
            "orderbook.50.ETHUSDT".to_string(),
            "orderbook.200.ETHUSDT".to_string(),
            "orderbook.1000.ETHUSDT".to_string(),
            "publicTrade.ETHUSDT".to_string(),
        ],
    }
}

fn ping_op() -> Op {
    Op {
        req_id: "ping".to_string(),
        op: "ping".to_string(),
        args: vec![],
    }
}

fn new_order() -> Order {
    Order {
        symbol: "ETHUSDT".to_string(),
        side: Some("Buy".to_string()),
        order_type: Some("Limit".to_string()),
        qty: Some("0.10".to_string()),
        price: Some("2057.50".to_string()),
        category: "linear".to_string(),
        time_in_force: Some("PostOnly".to_string()),
        order_link_id: "t_ETHUSDT_aBcDeFgH12345678".to_string(),
    }
}

fn cancel_order() -> Order {
    Order {
        symbol: "ETHUSDT".to_string(),
        side: None,
        order_type: None,
        qty: None,
        price: None,
        category: "linear".to_string(),
        time_in_force: None,
        order_link_id: "t_ETHUSDT_aBcDeFgH12345678".to_string(),
    }
}

fn order_header() -> HashMap<String, String> {
    let mut header = HashMap::new();
    header.insert("X-BAPI-TIMESTAMP".to_string(), "1775425340000".to_string());
    header.insert("X-BAPI-RECV-WINDOW".to_string(), "5000".to_string());
    header
}

fn bench_serialize_op(c: &mut Criterion) {
    let mut group = c.benchmark_group("ser/op");
    for (name, op) in [("subscribe", subscribe_op()), ("ping", ping_op())] {
        group.bench_function(name, |b| {
            b.iter(|| black_box(serde_json::to_string(black_box(&op)).unwrap()))
        });
    }
    group.finish();
}

fn bench_serialize_trade_op(c: &mut Criterion) {
    let mut group = c.benchmark_group("ser/trade_op");

    let create = TradeOp {
        req_id: "t_ETHUSDT_aBcDeFgH12345678/Xy12Z9qp".to_string(),
        header: order_header(),
        op: "order.create",
        args: vec![new_order()],
    };
    group.bench_function("order.create", |b| {
        b.iter(|| black_box(serde_json::to_string(black_box(&create)).unwrap()))
    });

    let cancel = TradeOp {
        req_id: "t_ETHUSDT_aBcDeFgH12345678/Zy98Q1pr".to_string(),
        header: order_header(),
        op: "order.cancel",
        args: vec![cancel_order()],
    };
    group.bench_function("order.cancel", |b| {
        b.iter(|| black_box(serde_json::to_string(black_box(&cancel)).unwrap()))
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_orderbook,
    bench_trade,
    bench_private,
    bench_account,
    bench_serialize_op,
    bench_serialize_trade_op,
);
criterion_main!(benches);
