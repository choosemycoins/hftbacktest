//! Frames captured verbatim from Lighter mainnet on 2026-07-28.
//!
//! Every fixture in this file is a line of a real `wss://mainnet.zklighter.
//! elliot.ai/stream` session, copied byte for byte — nothing here was written
//! by hand. That matters more than usual on this venue: the request spelling
//! of a channel (`order_book/0`) is not the response spelling
//! (`order_book:0`), the nonce chain lives two levels down inside
//! `order_book`, and a deletion is a level whose size is the zero of that
//! market's size decimals. Every one of those is a detail a hand-written
//! fixture gets subtly right and the venue gets differently.
//!
//! The order-book frames are a genuinely consecutive run, so their nonces
//! chain: snapshot -> `AFTER_SNAPSHOT` -> `CHAIN_A` -> `CHAIN_B`.
//! `ORDER_BOOK_DELETE_ETH` is from a second later in the same session, so it
//! does *not* follow them — which is exactly what a lost batch looks like.
//!
//! The snapshot is the one frame that is not whole: the real one is 105 KB of
//! levels (1414 asks, 1500 bids). Its levels are truncated to two a side and
//! its envelope — which is all the routing and the chain read — is untouched.

/// `subscribed/order_book`, the full book. `begin_nonce` is 0 and `nonce`
/// is what the first update chains from. Levels truncated; envelope real.
pub const ORDER_BOOK_SNAPSHOT_ETH: &str = r#"{"channel":"order_book:0","last_updated_at":1785252530119936,"offset":2043106,"order_book":{"code":0,"asks":[{"price":"1914.28","size":"0.1021"},{"price":"1914.42","size":"4.5605"}],"bids":[{"price":"1914.22","size":"0.1900"},{"price":"1914.18","size":"8.5318"}],"offset":2043106,"nonce":17926042877,"last_updated_at":1785252530119936,"begin_nonce":0},"timestamp":1785252530166,"type":"subscribed/order_book"}"#;

/// The first diff after the snapshot: `begin_nonce` == the snapshot's `nonce`.
pub const ORDER_BOOK_AFTER_SNAPSHOT_ETH: &str = r#"{"channel":"order_book:0","last_updated_at":1785252530170379,"offset":2043113,"order_book":{"code":0,"asks":[{"price":"1915.49","size":"2.9243"},{"price":"1915.52","size":"31.3043"},{"price":"1916.44","size":"0.5223"},{"price":"1916.47","size":"0.0000"},{"price":"1918.36","size":"0.2085"},{"price":"1918.39","size":"0.0000"}],"bids":[{"price":"1914.24","size":"0.1900"},{"price":"1914.23","size":"3.7732"},{"price":"1914.22","size":"0.0000"},{"price":"1913.90","size":"13.3579"},{"price":"1913.60","size":"0.0000"},{"price":"1913.57","size":"0.0522"},{"price":"1912.64","size":"127.6970"},{"price":"1912.61","size":"0.1045"},{"price":"1910.73","size":"0.0000"},{"price":"1910.70","size":"0.2093"}],"offset":2043113,"nonce":17926042944,"last_updated_at":1785252530170379,"begin_nonce":17926042877},"timestamp":1785252530174,"type":"update/order_book"}"#;

/// The next diff. `begin_nonce` == the previous frame's `nonce`.
pub const CHAIN_A: &str = r#"{"channel":"order_book:0","last_updated_at":1785252530221157,"offset":2043118,"order_book":{"code":0,"asks":[{"price":"1914.46","size":"0.0000"}],"bids":[{"price":"1914.11","size":"10.2437"},{"price":"1914.07","size":"88.0308"}],"offset":2043118,"nonce":17926043003,"last_updated_at":1785252530221157,"begin_nonce":17926042944},"timestamp":1785252530230,"type":"update/order_book"}"#;

/// And the next. These three are consecutive on the wire.
pub const CHAIN_B: &str = r#"{"channel":"order_book:0","last_updated_at":1785252530249625,"offset":2043124,"order_book":{"code":0,"asks":[{"price":"1914.54","size":"177.7001"},{"price":"1914.68","size":"0.4000"},{"price":"1914.93","size":"19.5880"}],"bids":[{"price":"1914.25","size":"0.4622"},{"price":"1914.24","size":"0.5488"},{"price":"1914.23","size":"0.0000"},{"price":"1914.04","size":"199.7482"}],"offset":2043124,"nonce":17926043048,"last_updated_at":1785252530249625,"begin_nonce":17926043003},"timestamp":1785252530274,"type":"update/order_book"}"#;

/// A diff carrying a deletion — `"size":"0.0000"`, the zero of ETH's four
/// size decimals. From ~1.6s later in the same session, so its `begin_nonce`
/// follows none of the frames above.
pub const ORDER_BOOK_DELETE_ETH: &str = r#"{"channel":"order_book:0","last_updated_at":1785252531782526,"offset":2043361,"order_book":{"code":0,"asks":[],"bids":[{"price":"1914.10","size":"0.0000"}],"offset":2043361,"nonce":17926044824,"last_updated_at":1785252531782526,"begin_nonce":17926044752},"timestamp":1785252531830,"type":"update/order_book"}"#;

/// `ticker`, the event-driven top of book. Carries a bare `nonce` and no
/// `begin_nonce`: there is no chain to check here.
pub const TICKER_BTC: &str = r#"{"channel":"ticker:1","last_updated_at":1785252530834835,"nonce":17926043680,"ticker":{"s":"BTC","a":{"price":"63777.3","size":"0.00020"},"b":{"price":"63777.2","size":"2.49220"},"last_updated_at":1785252530834835},"timestamp":1785252530836,"type":"update/ticker"}"#;

/// `trade`, one block's prints. Also carries a bare `nonce`.
pub const TRADE_ETH: &str = r#"{"channel":"trade:0","liquidation_trades":[],"nonce":17926062253,"trades":[{"trade_id":26280867773,"trade_id_str":"26280867773","tx_hash":"00000019a5f79fdd0000019fa95808ba000000000000000000000000000000000000000000000000","type":"trade","market_id":0,"size":"0.0008","price":"1913.99","usd_amount":"1.531192","ask_id":281477536273316,"ask_id_str":"281477536273316","bid_id":562947300641615,"bid_id_str":"562947300641615","ask_client_id":2145634480,"ask_client_id_str":"2145634480","bid_client_id":0,"bid_client_id_str":"0","ask_account_id":708516,"bid_account_id":326677,"is_maker_ask":true,"block_height":301814264,"timestamp":1785252546746,"taker_position_size_before":"0.0432","taker_entry_quote_before":"81.879184","taker_initial_margin_fraction_before":200,"maker_fee":28,"maker_position_size_before":"0.8843","maker_entry_quote_before":"1696.919247","maker_initial_margin_fraction_before":500,"transaction_time":1785252546746546}],"type":"update/trade"}"#;

/// `market_stats` — mark price, index price, funding rate and open interest.
/// The venue's own funding/oracle aggregate, and the one thing about a perp
/// that its book and tape cannot reconstruct afterwards.
pub const MARKET_STATS_ETH: &str = r#"{"channel":"market_stats:0","market_stats":{"symbol":"ETH","market_id":0,"index_price":"1915.31","mark_price":"1914.32","mid_price":"1914.27","best_ask_price":"1914.28","best_bid_price":"1914.26","open_interest":"86574362.972120","open_interest_limit":"72057594037927936.000000","funding_clamp_small":"0.0500","funding_clamp_big":"4.0000","last_trade_price":"1914.28","current_funding_rate":"-0.0012","funding_rate":"-0.0011","funding_timestamp":1785250800001,"daily_base_token_volume":220273.9901,"daily_quote_token_volume":418335825.404646,"daily_price_low":1856.3,"daily_price_high":1954.58,"daily_price_change":-1.2914301622407474,"base_interest_rate":"0.0100"},"timestamp":1785252530326,"type":"update/market_stats"}"#;

/// `market_stats:all`, which this collector does not subscribe to. Its
/// channel names no single market, so it can only go to the sidecar.
pub const MARKET_STATS_ALL: &str = r#"{"channel":"market_stats:all","market_stats":{"116":{"symbol":"GOOGL","market_id":116,"index_price":"332.58","mark_price":"332.80","mid_price":"332.86","best_ask_price":"332.89","best_bid_price":"332.82","open_interest":"1580482.874880","open_interest_limit":"50000000000000.000000","funding_clamp_small":"0.0500","funding_clamp_big":"4.0000","last_trade_price":"332.89","current_funding_rate":"0.0004","funding_rate":"0.0004","funding_timestamp":1785250800001,"daily_base_token_volume":1887.7484,"daily_quote_token_volume":617554.8948070001,"daily_price_low":323.78,"daily_price_high":333.27,"daily_price_change":1.8573483063553746,"base_interest_rate":"0.0032"}},"timestamp":1785252530620,"type":"update/market_stats"}"#;

/// `height`, the block-height feed. A channel with no market id at all.
pub const HEIGHT: &str =
    r#"{"channel":"height","height":301813997,"timestamp":1785252531621,"type":"update/height"}"#;

/// The first frame of every session.
pub const CONNECTED: &str =
    r#"{"session_id":"b23c70b2-9ef8-4080-8f02-4edf34138329","type":"connected"}"#;

/// The venue's answer to an app-level `{"type":"ping"}`.
pub const PONG: &str = r#"{"type":"pong"}"#;

/// What the venue says to `ticker/99999`. Note what it does not do: it does
/// not close the socket, so an unresolvable symbol is a subscription that
/// silently never produces data.
pub const ERROR_UNKNOWN_MARKET: &str =
    r#"{"error":{"code":30005,"message":"Invalid Channel:  (marketId)"}}"#;

/// And to a channel name it does not know.
pub const ERROR_UNKNOWN_CHANNEL: &str = r#"{"error":{"code":30005,"message":"Invalid Channel"}}"#;

/// What the venue says to a **duplicate** subscribe — a `subscribe` for a
/// channel this connection already holds.
///
/// Measured on mainnet 2026-07-28 by re-sending
/// `{"type":"subscribe","channel":"order_book/0"}` on a live session: the
/// refusal came back in 267 ms and **no snapshot followed**. So a book repair
/// built out of one subscribe repairs nothing, and this is the frame that says
/// so. Note it carries neither `type` nor `channel`, which is why it can only
/// be filed in the sidecar.
pub const ERROR_ALREADY_SUBSCRIBED: &str =
    r#"{"error":{"code":30003,"message":"Already Subscribed to : order_book:0"}}"#;

/// What the venue says to a subscribe it refuses for rate: one of these per
/// refused frame, and it names no channel at all.
///
/// Byte for byte from our own recording — `_meta_lighter_20260812.jsonl` and
/// `_meta_lighter_20260818.jsonl`, 41 and 53 of them respectively, in bursts of
/// eight 246ms apart, beginning 1.01s after `connected` in both. This is the
/// frame that proves the venue is not silent about a dropped subscription, and
/// the reason a rate-limited connection must never be reconnected: the fresh
/// connection re-sends the whole set into the same limiter.
pub const ERROR_TOO_MANY_MESSAGES: &str =
    r#"{"error":{"code":30009,"message":"Too Many Websocket Messages!"}}"#;

/// The ack for `{"type":"unsubscribe","channel":"order_book/0"}` — the first
/// half of the repair the venue does honour.
///
/// Byte for byte from the wire, spaces included: this is the one frame the
/// venue spells with them. Measured 2026-07-28, the ack came 267 ms after the
/// unsubscribe and a full 105 KB `subscribed/order_book` 255 ms after that,
/// with both client frames sent back to back and no wait for this ack in
/// between.
pub const UNSUBSCRIBED_ETH: &str = r#"{"type": "unsubscribed", "channel": "order_book:0"}"#;
