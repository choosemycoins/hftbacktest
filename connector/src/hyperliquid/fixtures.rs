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

// --- Private stream -------------------------------------------------------------------
//
// Captured from `wss://api.hyperliquid-testnet.xyz/ws` on 2026-07-28 against the master
// account in `~/.config/hftbacktest-connector/hl-testnet.toml`.
//
// **One field is not verbatim: the `user` address.** It has been replaced throughout with
// the well-known Hardhat account #1, `0x7099…79c8`, the same public test vector `signing.rs`
// builds on. An address is public on-chain and not a secret, so this is not a leak — but
// `connector/` is written to be upstreamable, and a fixture committed to a public repository
// should not name somebody's trading account. Nothing else is edited, and no key material
// appears anywhere here. The properties the fixtures exist to pin — the object envelope, the
// lowercased echo, `tid` as a number — are untouched by the substitution.

/// The `userFills` frame the venue sends **on every subscribe**, verbatim.
///
/// Two things it pins that a hand-written sample would not. It is an **object**, not the
/// bare array the public `trades` channel uses — a parser written from that one silently
/// rejects every fill. And the venue **lowercases the address** it echoes, so any
/// comparison against a configured address has to be case-insensitive.
pub const USER_FILLS_EMPTY_SNAPSHOT: &str = r#"{"channel":"userFills","data":{"isSnapshot":true,"user":"0x70997970c51812dc3a010c7d01b50e0d17dc79c8","fills":[]}}"#;

/// The subscription acknowledgement for `userFills`. Note `aggregateByTime`, which the
/// venue adds to the echo even though the request did not carry it.
pub const USER_FILLS_ACK: &str = r#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"userFills","user":"0x70997970c51812dc3a010c7d01b50e0d17dc79c8","aggregateByTime":false}}}"#;

/// An `orderUpdates` frame for a resting post-only bid — **captured**, from the round trip
/// in `smoke.rs` on 2026-07-28.
///
/// A real `oid` is a 64-bit number well past `u32` (57 118 842 019 here), and `limitPx`
/// carries a trailing `.0` that the price never had. The client id is this connector's own,
/// prefix `a1f0`.
pub const ORDER_UPDATE_OPEN: &str = r#"{"channel":"orderUpdates","data":[{"order":{"coin":"BTC","side":"B","limitPx":"31795.0","sz":"0.00037","oid":57118842019,"timestamp":1785261289146,"origSz":"0.00037","cloid":"0xa1f0cddc17303d07f3fd128d5b35071d"},"status":"open","statusTimestamp":1785261289146}]}"#;

/// The same order cancelled, 1.5 s later. Also a capture.
///
/// **`sz` does not drop to zero on a terminal update** — it stays at the unfilled remainder,
/// and `timestamp` stays at the order's creation while `statusTimestamp` moves. Guessing
/// the opposite (as the documentation reads) would have made a cancelled order look fully
/// filled to anything that inferred execution from `origSz - sz`.
pub const ORDER_UPDATE_CANCELED: &str = r#"{"channel":"orderUpdates","data":[{"order":{"coin":"BTC","side":"B","limitPx":"31795.0","sz":"0.00037","oid":57118842019,"timestamp":1785261289146,"origSz":"0.00037","cloid":"0xa1f0cddc17303d07f3fd128d5b35071d"},"status":"canceled","statusTimestamp":1785261290665}]}"#;

/// A `userFills` increment. `tid` is a JSON **number**, and `side` is *our* side, not the
/// aggressor's.
///
/// **Shape from the venue's documentation, not a capture.** The smoke round trip quotes far
/// from the market on purpose, so no order of this connector's has ever filled; the
/// envelope around it (`isSnapshot`, `user`, `fills`) is a capture and the entries are not.
/// Replace them the first time one fills.
pub const USER_FILLS_INCREMENT: &str = r#"{"channel":"userFills","data":{"isSnapshot":false,"user":"0x70997970c51812dc3a010c7d01b50e0d17dc79c8","fills":[{"coin":"BTC","px":"31795.0","sz":"0.00016","side":"B","time":1785261290000,"startPosition":"0.0","dir":"Open Long","closedPnl":"0.0","hash":"0x0000000000000000000000000000000000000000000000000000000000000000","oid":57118842019,"crossed":false,"fee":"0.0","tid":123456789,"cloid":"0xa1f0cddc17303d07f3fd128d5b35071d","feeToken":"USDC"}]}}"#;

/// `POST /info {"type":"clearinghouseState","user":…}` for an account with no perp margin,
/// verbatim from testnet on 2026-07-28. An account that has never traded a perp has an
/// **empty** `assetPositions`, so "no entry" must read as "flat", not as "unknown".
pub const CLEARINGHOUSE_STATE_FLAT: &str = r#"{"marginSummary":{"accountValue":"0.0","totalNtlPos":"0.0","totalRawUsd":"0.0","totalMarginUsed":"0.0"},"crossMarginSummary":{"accountValue":"0.0","totalNtlPos":"0.0","totalRawUsd":"0.0","totalMarginUsed":"0.0"},"crossMaintenanceMarginUsed":"0.0","withdrawable":"0.0","assetPositions":[],"time":1785259956738}"#;

/// The same endpoint for an account holding a short. `szi` is **signed** — negative is
/// short — which is the whole reason no side field is needed.
///
/// Shape from the venue's documentation; the testnet account has never held a position.
pub const CLEARINGHOUSE_STATE_POSITIONS: &str = r#"{"marginSummary":{"accountValue":"1000.0","totalNtlPos":"200.0","totalRawUsd":"1200.0","totalMarginUsed":"20.0"},"crossMaintenanceMarginUsed":"10.0","withdrawable":"980.0","assetPositions":[{"type":"oneWay","position":{"coin":"BTC","szi":"-0.00032","leverage":{"type":"cross","value":20},"entryPx":"63500.0","positionValue":"20.3","unrealizedPnl":"0.1","returnOnEquity":"0.01","liquidationPx":"120000.0","marginUsed":"1.0","maxLeverage":40}},{"type":"oneWay","position":{"coin":"ETH","szi":"1.5","leverage":{"type":"cross","value":10},"entryPx":"2000.0","positionValue":"3000.0","unrealizedPnl":"0.0","returnOnEquity":"0.0","liquidationPx":null,"marginUsed":"300.0","maxLeverage":25}}],"time":1785259956739}"#;

/// `POST /info {"type":"openOrders","user":…}` — the reconciliation source after a
/// reconnect, since `orderUpdates` gaps are unrecoverable from the stream itself.
///
/// Both are verbatim testnet captures from 2026-07-28. `frontendOpenOrders` **does** echo
/// the `cloid` — measured, not assumed — which is what makes it the endpoint this backend
/// asks. Reconciliation still keys on `oid`, because `openOrders` does not echo it and a
/// venue that stopped would otherwise expire every live order.
pub const OPEN_ORDERS_EMPTY: &str = "[]";
pub const OPEN_ORDERS_ONE: &str = r#"[{"coin":"BTC","limitPx":"31795.0","oid":57118842019,"side":"B","sz":"0.00037","timestamp":1785261289146,"origSz":"0.00037","cloid":"0xa1f0cddc17303d07f3fd128d5b35071d"}]"#;

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
