//! Real Hyperliquid frames, captured from **testnet on 2026-07-28**.
//!
//! Every constant here is a byte-for-byte capture, not a hand-written sample. Parsing is
//! the one part of an exchange backend that no log line and no error will ever contradict
//! — a field renamed by the venue shows up as a quiet absence of data — so the fixtures
//! are the venue's own bytes, trimmed only by dropping array entries (never fields).
//!
//! Provenance of each group is in its doc comment. Re-capture with a WebSocket client
//! against `wss://api.hyperliquid-testnet.xyz/ws`; no key is needed for any of these.

/// One `l2Book` frame with `fast: true` for BTC. Note the echoed `fast` field: it is how
/// a recording tells the 5-level cadence from the 20-level one.
pub const L2BOOK_FAST_BTC_1: &str = r#"{"channel":"l2Book","data":{"coin":"BTC","time":1785251521889,"levels":[[{"px":"63460.0","sz":"0.02666","n":2},{"px":"63457.0","sz":"0.00028","n":1},{"px":"63454.0","sz":"0.02659","n":2},{"px":"63441.0","sz":"0.04944","n":2},{"px":"63429.0","sz":"0.00046","n":1}],[{"px":"63488.0","sz":"0.01246","n":2},{"px":"63495.0","sz":"0.01263","n":2},{"px":"63496.0","sz":"0.00019","n":1},{"px":"63507.0","sz":"0.02357","n":2},{"px":"63516.0","sz":"0.02619","n":2}]],"fast":true}}"#;

/// The `bbo` frame that arrived 201 ms after [`L2BOOK_FAST_BTC_1`]. Its best bid is
/// 63457, i.e. the 63460 level in the snapshot above is gone — a real touch move, not a
/// constructed one.
pub const BBO_BTC_2: &str = r#"{"channel":"bbo","data":{"coin":"BTC","time":1785251522090,"bbo":[{"px":"63457.0","sz":"0.00028","n":1},{"px":"63488.0","sz":"0.01246","n":2}]}}"#;

/// The next `l2Book fast` frame, 312 ms after [`BBO_BTC_2`]. It confirms the touch move
/// (63460 is gone) and brings one new level in at the deep end (63423).
pub const L2BOOK_FAST_BTC_3: &str = r#"{"channel":"l2Book","data":{"coin":"BTC","time":1785251522402,"levels":[[{"px":"63457.0","sz":"0.00028","n":1},{"px":"63454.0","sz":"0.02659","n":2},{"px":"63441.0","sz":"0.04944","n":2},{"px":"63429.0","sz":"0.00046","n":1},{"px":"63423.0","sz":"0.00019","n":1}],[{"px":"63488.0","sz":"0.01246","n":2},{"px":"63495.0","sz":"0.01263","n":2},{"px":"63496.0","sz":"0.00019","n":1},{"px":"63507.0","sz":"0.02357","n":2},{"px":"63516.0","sz":"0.02619","n":2}]],"fast":true}}"#;

/// The first two entries of the `trades` frame the venue replayed on subscribe.
///
/// The full frame carried exactly 30 fills, as it did for every (re)subscribe measured.
pub const TRADES_BTC_REPLAY: &str = r#"{"channel":"trades","data":[{"coin":"BTC","side":"B","px":"63719.0","sz":"0.00016","time":1785251456343,"hash":"0x668506587a64f61a67fe0425a1e37c0102001e3e156814ec0a4db1ab3968d005","tid":447757798903687,"users":["0x1260b8f7e0c4caa2ef3a51b6c08507ecc104641d","0x86914e6492aab9847dfd8d088060ba2075bb315c"]},{"coin":"BTC","side":"B","px":"63530.0","sz":"0.00016","time":1785251457213,"hash":"0xc000fde9d952a262c17a0425a1e38a01030015cf7455c13463c9a93c98567c4d","tid":355416041315622,"users":["0x1260b8f7e0c4caa2ef3a51b6c08507ecc104641d","0x4547b9e33f07965711f67d0be2423939a4e33af5"]}]}"#;

/// The subscribe acknowledgement. Carries no data of its own; the echoed subscription is
/// the only confirmation that the venue understood the request.
pub const SUBSCRIPTION_RESPONSE: &str = r#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"bbo","coin":"BTC"}}}"#;

/// The reply to `{"method":"ping"}`. **It has no `data` field at all** — a parser that
/// requires one silently treats every keepalive as a malformed frame.
pub const PONG: &str = r#"{"channel":"pong"}"#;

/// What an unknown subscription *type* produces. The connection survives this one; an
/// unknown *coin* instead closes the socket with no frame at all.
pub const ERROR_FRAME: &str = r#"{"channel":"error","data":"Error parsing JSON into valid websocket request: {\"method\": \"subscribe\", \"subscription\": {\"type\": \"noSuchType\", \"coin\": \"BTC\"}}"}"#;

/// Four entries of the canonical (no-prefix) perp universe, from `POST /info {"type":"meta"}`.
///
/// `szDecimals` is the only field this backend needs: it fixes both the lot size and the
/// finest legal price increment.
pub const META_CANONICAL: &str = r#"{"universe":[{"szDecimals":2,"name":"SOL","maxLeverage":10,"marginTableId":10},{"szDecimals":5,"name":"BTC","maxLeverage":40,"marginTableId":54},{"szDecimals":4,"name":"ETH","maxLeverage":25,"marginTableId":50},{"szDecimals":1,"name":"MATIC","maxLeverage":10,"marginTableId":10}],"collateralToken":0}"#;

/// The `test` builder dex's universe, from `POST /info {"type":"meta","dex":"test"}`.
///
/// The entry's `name` already carries the prefix (`test:ABC`), and that full string is
/// also the wire coin name — nothing is concatenated at subscribe time.
pub const META_DEX_TEST: &str = r#"{"universe":[{"szDecimals":0,"name":"test:ABC","maxLeverage":3,"marginTableId":3}],"collateralToken":1}"#;

/// The `unit` builder dex's universe. `unit:NQ` carries `isDelisted: true`, which is the
/// only warning the venue gives before a subscription to it stops producing data.
pub const META_DEX_UNIT: &str = r#"{"universe":[{"szDecimals":2,"name":"unit:ES","maxLeverage":20,"marginTableId":20,"onlyIsolated":true,"marginMode":"strictIsolated"},{"szDecimals":2,"name":"unit:NQ","maxLeverage":20,"marginTableId":20,"onlyIsolated":true,"isDelisted":true,"marginMode":"strictIsolated"}],"collateralToken":1}"#;
