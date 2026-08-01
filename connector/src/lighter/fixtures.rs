//! Frames captured verbatim from Lighter mainnet, and the catalog they are addressed by.
//!
//! Sources, all real bytes:
//!
//! * the order-book and trade frames are lines of `/home/storage/hft-data/lighter/
//!   crv_20260729.gz`, a recorded mainnet day of this repository's own collector — CRV,
//!   `market_id = 36`, from the first seconds of the session, so the snapshot and the six
//!   diffs after it are **genuinely consecutive** and their nonces chain;
//! * the catalog is five rows of `GET /api/v1/orderBooks`, fetched 2026-07-31;
//! * the control and error frames are copied from `collector/src/lighter/fixtures.rs`,
//!   which took them off a mainnet session on 2026-07-28. They name `order_book:0` (ETH)
//!   because that is what was recorded; rewriting the id would make them ours, not the
//!   venue's.
//!
//! Nothing here was written by hand, and on this venue that matters more than usual: a
//! subscription is *asked for* as `order_book/36` and every frame *comes back* as
//! `order_book:36` (design note §2.1), the chain lives two levels down inside `order_book`
//! (§2.2), and a deletion is a level whose size is the zero of that market's size decimals
//! (§3.1). A hand-written fixture gets each of those subtly right and the venue gets them
//! differently.

/// `subscribed/order_book` for CRV, complete and untouched: 39 bids, 40 asks.
///
/// `begin_nonce` is 0 — a snapshot reseeds the chain — and its `nonce` is what
/// [`ORDER_BOOK_UPDATE_CRV_1`] chains from. Small enough to keep whole because CRV is a
/// thin market; ETH is 105 KB and ~2 900 levels (design note §2.2), which is what a repair
/// really costs.
pub const ORDER_BOOK_SNAPSHOT_CRV: &str = r#"{"channel":"order_book:36","last_updated_at":1785358811946459,"offset":415803,"order_book":{"code":0,"asks":[{"price":"0.20382","size":"7816.7"},{"price":"0.20383","size":"1915.0"},{"price":"0.20391","size":"18240.6"},{"price":"0.20395","size":"47502.5"},{"price":"0.20398","size":"39308.1"},{"price":"0.20399","size":"47502.5"},{"price":"0.20410","size":"47518.6"},{"price":"0.20421","size":"25374.8"},{"price":"0.20436","size":"2495.4"},{"price":"0.20452","size":"135935.0"},{"price":"0.20462","size":"39565.1"},{"price":"0.20501","size":"6666.0"},{"price":"0.20515","size":"195642.5"},{"price":"0.20526","size":"195642.5"},{"price":"0.20537","size":"195642.5"},{"price":"0.20548","size":"195317.5"},{"price":"0.20559","size":"195317.5"},{"price":"0.20570","size":"195317.5"},{"price":"0.20581","size":"195204.4"},{"price":"0.20592","size":"194780.1"},{"price":"0.20603","size":"195572.2"},{"price":"0.20614","size":"195482.0"},{"price":"0.20660","size":"200.4"},{"price":"0.20673","size":"10587.2"},{"price":"0.20681","size":"390870.9"},{"price":"0.21012","size":"509.7"},{"price":"0.21522","size":"509.7"},{"price":"0.22133","size":"411.7"},{"price":"0.24000","size":"700.0"},{"price":"0.24444","size":"330.0"},{"price":"0.25222","size":"700.0"},{"price":"0.26444","size":"700.0"},{"price":"0.27666","size":"700.0"},{"price":"0.28888","size":"700.0"},{"price":"0.30111","size":"700.0"},{"price":"0.31333","size":"700.0"},{"price":"0.32555","size":"700.0"},{"price":"0.33777","size":"700.0"},{"price":"0.35000","size":"700.0"},{"price":"0.62997","size":"426.6"}],"bids":[{"price":"0.20351","size":"7673.9"},{"price":"0.20350","size":"2495.4"},{"price":"0.20341","size":"47502.5"},{"price":"0.20338","size":"47502.5"},{"price":"0.20335","size":"39327.4"},{"price":"0.20334","size":"47518.6"},{"price":"0.20326","size":"18000.0"},{"price":"0.20308","size":"25775.3"},{"price":"0.20300","size":"32203.4"},{"price":"0.20290","size":"40189.8"},{"price":"0.20274","size":"6668.4"},{"price":"0.20270","size":"102857.3"},{"price":"0.20240","size":"195641.1"},{"price":"0.20229","size":"195687.4"},{"price":"0.20218","size":"195797.5"},{"price":"0.20207","size":"195896.8"},{"price":"0.20196","size":"196005.3"},{"price":"0.20185","size":"196110.4"},{"price":"0.20174","size":"196217.7"},{"price":"0.20163","size":"196325.6"},{"price":"0.20152","size":"196436.4"},{"price":"0.20141","size":"196549.0"},{"price":"0.20129","size":"200.4"},{"price":"0.20121","size":"10587.2"},{"price":"0.20058","size":"385714.9"},{"price":"0.19900","size":"20000.0"},{"price":"0.19785","size":"509.7"},{"price":"0.19676","size":"508.3"},{"price":"0.19510","size":"12814.0"},{"price":"0.19502","size":"512.8"},{"price":"0.19370","size":"413.1"},{"price":"0.19275","size":"509.7"},{"price":"0.19197","size":"1302.3"},{"price":"0.19110","size":"785.0"},{"price":"0.18910","size":"793.3"},{"price":"0.18710","size":"1336.2"},{"price":"0.18663","size":"411.7"},{"price":"0.17036","size":"587.0"},{"price":"0.10500","size":"426.6"}],"offset":415803,"nonce":18032383898,"last_updated_at":1785358811946459,"begin_nonce":0},"timestamp":1785358811976,"type":"subscribed/order_book"}"#;

/// The first diff after the snapshot: `begin_nonce` == the snapshot's `nonce`.
///
/// It carries both halves of what a diff means: `"size":"0.0"` deletes a level and any
/// other size replaces it. Nothing else in the frame says which is which.
pub const ORDER_BOOK_UPDATE_CRV_1: &str = r#"{"channel":"order_book:36","last_updated_at":1785358812048570,"offset":415806,"order_book":{"code":0,"asks":[{"price":"0.20416","size":"25374.8"},{"price":"0.20421","size":"0.0"}],"bids":[{"price":"0.20351","size":"0.0"},{"price":"0.20308","size":"0.0"},{"price":"0.20302","size":"25775.2"}],"offset":415806,"nonce":18032384006,"last_updated_at":1785358812048570,"begin_nonce":18032383898},"timestamp":1785358812058,"type":"update/order_book"}"#;

/// Consecutive diff 2 of 6, in wire order. `begin_nonce` == the previous frame's `nonce`.
pub const ORDER_BOOK_UPDATE_CRV_2: &str = r#"{"channel":"order_book:36","last_updated_at":1785358812073840,"offset":415807,"order_book":{"code":0,"asks":[],"bids":[{"price":"0.20334","size":"55192.5"}],"offset":415807,"nonce":18032384057,"last_updated_at":1785358812073840,"begin_nonce":18032384006},"timestamp":1785358812115,"type":"update/order_book"}"#;

/// Consecutive diff 3 of 6, in wire order. `begin_nonce` == the previous frame's `nonce`.
pub const ORDER_BOOK_UPDATE_CRV_3: &str = r#"{"channel":"order_book:36","last_updated_at":1785358812113531,"offset":415808,"order_book":{"code":0,"asks":[{"price":"0.20416","size":"0.0"},{"price":"0.20417","size":"25374.8"}],"bids":[{"price":"0.20315","size":"25775.2"},{"price":"0.20302","size":"0.0"}],"offset":415808,"nonce":18032384126,"last_updated_at":1785358812113531,"begin_nonce":18032384057},"timestamp":1785358812162,"type":"update/order_book"}"#;

/// Consecutive diff 4 of 6, in wire order. `begin_nonce` == the previous frame's `nonce`.
pub const ORDER_BOOK_UPDATE_CRV_4: &str = r#"{"channel":"order_book:36","last_updated_at":1785358812597325,"offset":415809,"order_book":{"code":0,"asks":[],"bids":[{"price":"0.20129","size":"0.0"}],"offset":415809,"nonce":18032384612,"last_updated_at":1785358812597325,"begin_nonce":18032384126},"timestamp":1785358812609,"type":"update/order_book"}"#;

/// Consecutive diff 5 of 6, in wire order. `begin_nonce` == the previous frame's `nonce`.
pub const ORDER_BOOK_UPDATE_CRV_5: &str = r#"{"channel":"order_book:36","last_updated_at":1785358812644078,"offset":415811,"order_book":{"code":0,"asks":[{"price":"0.20660","size":"0.0"}],"bids":[{"price":"0.20316","size":"25775.2"},{"price":"0.20315","size":"0.0"}],"offset":415811,"nonce":18032384667,"last_updated_at":1785358812644078,"begin_nonce":18032384612},"timestamp":1785358812659,"type":"update/order_book"}"#;

/// Consecutive diff 6 of 6, in wire order. `begin_nonce` == the previous frame's `nonce`.
pub const ORDER_BOOK_UPDATE_CRV_6: &str = r#"{"channel":"order_book:36","last_updated_at":1785358812738144,"offset":415812,"order_book":{"code":0,"asks":[],"bids":[{"price":"0.20129","size":"200.4"}],"offset":415812,"nonce":18032384785,"last_updated_at":1785358812738144,"begin_nonce":18032384667},"timestamp":1785358812764,"type":"update/order_book"}"#;

/// `update/trade`, one block's prints, verbatim.
pub const TRADE_CRV: &str = r#"{"channel":"trade:36","liquidation_trades":[],"nonce":18032408814,"trades":[{"trade_id":26412422124,"trade_id_str":"26412422124","tx_hash":"00000019c75017e30000019fafade0cc000000000000000000000000000000000000000000000000","type":"trade","market_id":36,"size":"0.7","price":"0.20382","usd_amount":"0.142674","ask_id":10414574259102759,"ask_id_str":"10414574259102759","bid_id":10696048997502383,"bid_id_str":"10696048997502383","ask_client_id":69016212847,"ask_client_id_str":"69016212847","bid_client_id":0,"bid_client_id_str":"0","ask_account_id":314233,"bid_account_id":382258,"is_maker_ask":true,"block_height":303153774,"timestamp":1785358835916,"taker_position_size_before":"619.5","taker_entry_quote_before":"129.621128","taker_initial_margin_fraction_before":1000,"maker_fee":28,"maker_position_size_before":"47295.0","maker_entry_quote_before":"9694.464700","maker_initial_margin_fraction_before":1000,"transaction_time":1785358835917189}],"type":"update/trade"}"#;

/// `subscribed/trade` — the replay the venue sends on **every** subscribe.
///
/// The real frame carries the last 50 prints (49 611 bytes); this one keeps the envelope and
/// the first two, plus the first entry of `liquidation_trades`. Those are a second, disjoint
/// array of genuine prints: measured over a recorded CRV day, 454 `trades` and 6
/// `liquidation_trades` with **zero** ids in common, so dropping them would silently lose
/// tape.
pub const TRADE_REPLAY_CRV: &str = r#"{"channel":"trade:36","liquidation_trades":[{"trade_id":26218621834,"trade_id_str":"26218621834","tx_hash":"0000001995872dea0000019fa5ce8ff3000000000000000000000000000000000000000000000000","type":"liquidation","market_id":36,"size":"8215.3","price":"0.20039","usd_amount":"1646.263967","ask_id":10414574258696109,"ask_id_str":"10414574258696109","bid_id":10696048997897706,"bid_id_str":"10696048997897706","ask_client_id":0,"ask_client_id_str":"0","bid_client_id":55006172834,"bid_client_id_str":"55006172834","ask_account_id":722982,"bid_account_id":314233,"is_maker_ask":false,"block_height":301149026,"timestamp":1785193205747,"taker_fee":10000,"taker_position_size_before":"8432.2","taker_entry_quote_before":"1798.870797","taker_initial_margin_fraction_before":1000,"maker_fee":28,"maker_position_size_before":"-4282.3","maker_entry_quote_before":"865.558815","maker_initial_margin_fraction_before":1000,"maker_position_sign_changed":true,"transaction_time":1785193205753071}],"nonce":18032383935,"trades":[{"trade_id":26412385647,"trade_id_str":"26412385647","tx_hash":"00000019c74d740c0000019fafad6ba9000000000000000000000000000000000000000000000000","type":"trade","market_id":36,"size":"0.7","price":"0.20382","usd_amount":"0.142674","ask_id":10414574259102759,"ask_id_str":"10414574259102759","bid_id":10696048997502478,"bid_id_str":"10696048997502478","ask_client_id":69016212847,"ask_client_id_str":"69016212847","bid_client_id":0,"bid_client_id_str":"0","ask_account_id":314233,"bid_account_id":382258,"is_maker_ask":true,"block_height":303153355,"timestamp":1785358805929,"taker_position_size_before":"618.8","taker_entry_quote_before":"129.478454","taker_initial_margin_fraction_before":1000,"maker_fee":28,"maker_position_size_before":"47295.7","maker_entry_quote_before":"9694.608186","maker_initial_margin_fraction_before":1000,"transaction_time":1785358805929929},{"trade_id":26412344671,"trade_id_str":"26412344671","tx_hash":"00000019c74ac6a80000019fafacf686000000000000000000000000000000000000000000000000","type":"trade","market_id":36,"size":"0.7","price":"0.20393","usd_amount":"0.142751","ask_id":10414574259102610,"ask_id_str":"10414574259102610","bid_id":10696048997502569,"bid_id_str":"10696048997502569","ask_client_id":69016212842,"ask_client_id_str":"69016212842","bid_client_id":0,"bid_client_id_str":"0","ask_account_id":314233,"bid_account_id":382258,"is_maker_ask":true,"block_height":303152935,"timestamp":1785358775942,"taker_position_size_before":"618.1","taker_entry_quote_before":"129.335703","taker_initial_margin_fraction_before":1000,"maker_fee":28,"maker_position_size_before":"47296.4","maker_entry_quote_before":"9694.751672","maker_initial_margin_fraction_before":1000,"transaction_time":1785358775942778}],"type":"subscribed/trade"}"#;

/// `GET /api/v1/orderBooks`, five real rows out of mainnet's 228 (fetched 2026-07-31).
///
/// Enough to cover every branch of the resolve: two perps whose decimals differ (ETH 2/4,
/// CRV 5/1), an inactive market (`HYUNDAI`), and a spot market whose symbol carries a quote
/// (`SKY/USDC`) — the shape a symbol copied from another venue takes.
pub const ORDER_BOOKS_CATALOG: &str = r#"{"code":200,"order_books":[{"symbol":"ETH","market_id":0,"market_type":"perp","base_asset_id":0,"quote_asset_id":0,"status":"active","taker_fee":"0.0000","is_taker_fee_enabled":true,"maker_fee":"0.0000","is_maker_fee_enabled":true,"liquidation_fee":"1.0000","min_base_amount":"0.0050","min_quote_amount":"10.000000","order_quote_limit":"281474976.710655","supported_size_decimals":4,"supported_price_decimals":2,"supported_quote_decimals":6,"created_at":"1737098461100","multiplier":"1.000000000000000000"},{"symbol":"CRV","market_id":36,"market_type":"perp","base_asset_id":0,"quote_asset_id":0,"status":"active","taker_fee":"0.0000","is_taker_fee_enabled":true,"maker_fee":"0.0000","is_maker_fee_enabled":true,"liquidation_fee":"1.0000","min_base_amount":"35.0","min_quote_amount":"10.000000","order_quote_limit":"281474976.710655","supported_size_decimals":1,"supported_price_decimals":5,"supported_quote_decimals":6,"created_at":"1745443199889","multiplier":"1.000000000000000000"},{"symbol":"HYPE","market_id":24,"market_type":"perp","base_asset_id":0,"quote_asset_id":0,"status":"active","taker_fee":"0.0000","is_taker_fee_enabled":true,"maker_fee":"0.0000","is_maker_fee_enabled":true,"liquidation_fee":"1.0000","min_base_amount":"0.15","min_quote_amount":"10.000000","order_quote_limit":"281474976.710655","supported_size_decimals":2,"supported_price_decimals":4,"supported_quote_decimals":6,"created_at":"1740427690638","multiplier":"1.000000000000000000"},{"symbol":"HYUNDAI","market_id":141,"market_type":"perp","base_asset_id":0,"quote_asset_id":0,"status":"inactive","taker_fee":"0.0000","is_taker_fee_enabled":true,"maker_fee":"0.0000","is_maker_fee_enabled":true,"liquidation_fee":"1.0000","min_base_amount":"0.000010","min_quote_amount":"10.000000","order_quote_limit":"5000000.000000","supported_size_decimals":6,"supported_price_decimals":0,"supported_quote_decimals":6,"created_at":"1770825551906","multiplier":"1.000000000000000000"},{"symbol":"SKY/USDC","market_id":2053,"market_type":"spot","base_asset_id":8,"quote_asset_id":3,"status":"active","taker_fee":"0.0000","is_taker_fee_enabled":true,"maker_fee":"0.0000","is_maker_fee_enabled":true,"liquidation_fee":"0.0000","min_base_amount":"100","min_quote_amount":"10.000000","order_quote_limit":"281474976.710655","supported_size_decimals":0,"supported_price_decimals":6,"supported_quote_decimals":6,"created_at":"1768058911384","multiplier":"1.000000000000000000"}]}"#;

/// `ticker`, the event-driven touch. Phase 1 does not subscribe it (design note §3.1), so
/// it is here as a frame the backend must skip without mistaking it for a book: it carries
/// a bare `nonce` and **no** `begin_nonce`, and reading one as a chain reports a break on
/// every frame.
pub const TICKER_BTC: &str = r#"{"channel":"ticker:1","last_updated_at":1785252530834835,"nonce":17926043680,"ticker":{"s":"BTC","a":{"price":"63777.3","size":"0.00020"},"b":{"price":"63777.2","size":"2.49220"},"last_updated_at":1785252530834835},"timestamp":1785252530836,"type":"update/ticker"}"#;

/// `height`, the block-height feed: a channel with no market id at all.
pub const HEIGHT: &str =
    r#"{"channel":"height","height":301813997,"timestamp":1785252531621,"type":"update/height"}"#;

/// The first frame of every session.
pub const CONNECTED: &str =
    r#"{"session_id":"b23c70b2-9ef8-4080-8f02-4edf34138329","type":"connected"}"#;

/// The venue's answer to an app-level `{"type":"ping"}`.
pub const PONG: &str = r#"{"type":"pong"}"#;

/// What the venue says to `ticker/99999`. Note what it does **not** do: it does not close
/// the socket, and it names neither the channel nor the market. So a subscription that did
/// not take is a symbol with no data on a perfectly healthy connection, and this frame
/// cannot be pinned on it — which is why the subscription tracker counts acks rather than
/// reading errors (design note §3.6).
pub const ERROR_UNKNOWN_MARKET: &str =
    r#"{"error":{"code":30005,"message":"Invalid Channel:  (marketId)"}}"#;

/// What the venue says to a **duplicate** subscribe, and the reason a repair is two frames.
///
/// Measured on mainnet 2026-07-28 by re-sending `{"type":"subscribe","channel":
/// "order_book/0"}` on a live session: the refusal came back in 267 ms and **no snapshot
/// followed**. A repair built out of one subscribe repairs nothing.
pub const ERROR_ALREADY_SUBSCRIBED: &str =
    r#"{"error":{"code":30003,"message":"Already Subscribed to : order_book:0"}}"#;

/// The ack for `{"type":"unsubscribe","channel":"order_book/0"}` — the first half of the
/// repair the venue does honour. Byte for byte from the wire, spaces included: this is the
/// one frame the venue spells with them.
pub const UNSUBSCRIBED_ETH: &str = r#"{"type": "unsubscribed", "channel": "order_book:0"}"#;

// ============================================================================================
// Phase 2 PRIVATE-channel frames, captured live on testnet 2026-07-30 (account_index 516).
//
// Source: `docs/lighter-phase0-artifacts/step6b_out.json`, one real order object out of each
// captured `account_all_orders` frame (the frame carried two; one is kept for size, byte for
// byte). These carry no key and no auth token — verified by grep before the artifacts were
// committed (their README) and again here: the only account identifier is the public index
// 516. They exist to pin the three traps that live in these bodies (design note §4.12–§4.14):
//
//   * the SAME order's `integrator_*` fields are `""` in the snapshot and `"0"` in the delta
//     (§4.13) — a serde struct that types them as numbers loses the whole snapshot, on the
//     reconnect path, where it is dearest;
//   * `side` is always the empty string; the side lives in `is_ask` (§4.13);
//   * `updated_at` is block-granular seconds and non-monotone across a state change, so it is
//     useless as an ordering key; `transaction_time` (microseconds) is the only one (§4.12);
//   * the in-order `nonce` field (2^48-ish) is NOT the slot's tx-nonce (§4.14).

/// `subscribed/account_all_orders`: one resting PostOnly bid, `status:"open"`, integrator
/// fields `""`. Auth was required to receive it (§3.4).
pub const PRIVATE_ORDERS_SNAPSHOT: &str = r#"{"channel":"account_all_orders:516","orders":{"1":[{"order_index":844424914280027,"client_order_index":424242424242,"order_id":"844424914280027","client_order_id":"424242424242","market_index":1,"owner_account_index":516,"initial_base_amount":"0.00100","price":"58300.0","nonce":281474960858715,"remaining_base_amount":"0.00100","is_ask":false,"base_size":100,"base_price":583000,"filled_base_amount":"0.00000","filled_quote_amount":"0.000000","side":"","type":"limit","time_in_force":"post-only","reduce_only":false,"trigger_price":"0.0","order_expiry":1787850973695,"status":"open","trigger_status":"na","trigger_time":0,"parent_order_index":0,"parent_order_id":"0","to_trigger_order_id_0":"0","to_trigger_order_id_1":"0","to_cancel_order_id_0":"0","integrator_fee_collector_index":"","integrator_taker_fee":"","integrator_maker_fee":"","order_flags":0,"block_height":510820,"timestamp":1785431769,"created_at":1785431769,"updated_at":1785431769,"transaction_time":1785431774184833}]},"type":"subscribed/account_all_orders"}"#;

/// `update/account_all_orders`: the SAME order (COI 424242424242, order_index
/// 844424914280027) after a `CancelAllOrders`, `status:"canceled"`, `base_size:0`,
/// `remaining_base_amount:"0.00000"` — and integrator fields now `"0"` rather than `""`
/// (§4.13). Its `transaction_time` is strictly greater than the snapshot's; its `updated_at`
/// moved only because a new block sealed it (§4.12).
pub const PRIVATE_ORDERS_UPDATE_CANCELED: &str = r#"{"channel":"account_all_orders:516","orders":{"1":[{"order_index":844424914280027,"client_order_index":424242424242,"order_id":"844424914280027","client_order_id":"424242424242","market_index":1,"owner_account_index":516,"initial_base_amount":"0.00100","price":"58300.0","nonce":281474960858715,"remaining_base_amount":"0.00000","is_ask":false,"base_size":0,"base_price":583000,"filled_base_amount":"0.00000","filled_quote_amount":"0.000000","side":"","type":"limit","time_in_force":"post-only","reduce_only":false,"trigger_price":"0.0","order_expiry":1787850973695,"status":"canceled","trigger_status":"na","trigger_time":0,"parent_order_index":0,"parent_order_id":"0","to_trigger_order_id_0":"0","to_trigger_order_id_1":"0","to_cancel_order_id_0":"0","integrator_fee_collector_index":"0","integrator_taker_fee":"0","integrator_maker_fee":"0","order_flags":0,"block_height":510869,"timestamp":1785431903,"created_at":1785431769,"updated_at":1785431903,"transaction_time":1785431906364320}]},"type":"update/account_all_orders"}"#;

/// `update/account_all_positions`, flat account: `open_order_count` fell to 0 after the
/// cancel. Position comes from here, never from replaying fills (§3.4).
pub const PRIVATE_POSITIONS_UPDATE_FLAT: &str = r#"{"channel":"account_all_positions:516","positions":{"1":{"market_id":1,"symbol":"BTC","initial_margin_fraction":"5.00","open_order_count":0,"pending_order_count":0,"position_tied_order_count":0,"sign":1,"position":"0.00000","avg_entry_price":"0.0","position_value":"-0.000000","unrealized_pnl":"0.000000","realized_pnl":"0.000000","liquidation_price":"0","margin_mode":0,"margin_set_flag":1,"allocated_margin":"0.000000"}},"shares":[],"type":"update/account_all_positions"}"#;

/// `subscribed/account_all_trades` — the snapshot is **always empty** (§3.4, confirmed): the
/// fill history does not replay, so a restart cannot rebuild the past from it.
pub const PRIVATE_TRADES_SNAPSHOT_EMPTY: &str = r#"{"channel":"account_all_trades:516","daily_volume":0,"monthly_volume":0,"total_volume":0,"trades":{},"type":"subscribed/account_all_trades","weekly_volume":0}"#;

/// The refusal to `subscribe account_all_orders` with no auth token (§3.4, both networks):
/// code 20001. Carries neither `type` nor `channel`, like every venue error frame.
pub const PRIVATE_ERROR_AUTH_REQUIRED: &str = r#"{"error":{"code":20001,"message":"invalid param : auth field is required: account_all_orders:1"}}"#;

/// `subscribed/account_all_orders` with an **empty** book — the shape a (re)connect gets when
/// the venue holds nothing (`docs/lighter-phase0-artifacts/step3_out.json`). The clean-slate
/// reconciliation must read this as "nothing to sweep" and spend no nonce (§3.4).
pub const PRIVATE_ORDERS_SNAPSHOT_EMPTY: &str =
    r#"{"channel":"account_all_orders:516","orders":{},"type":"subscribed/account_all_orders"}"#;

// --------------------------------------------------------------------------------------------
// Phase 2 (Fix) `GET /api/v1/accountActiveOrders` bodies — the REST mirror of the WS
// `account_all_orders` list the slot reads an ambiguous send's fate off (§3.3). Unlike the WS
// frame (a map of arrays keyed by market-index string), the REST body is a flat `orders` array
// under the venue's `code: 200` envelope; the per-order fields are identical (same
// `client_order_index` / `order_index` / string amounts / `transaction_time` in microseconds,
// and the same §4.13 `integrator_*` drift). All three were captured **byte for byte** off the
// live testnet endpoint during the Fix+Testnet round-trip (2026-08-01): `_ONE` is a real
// resting PostOnly bid on BTC (COI 178554256601, placed far below mid and confirmed here),
// `_EMPTY` is the flat account, and `_AUTH_REQUIRED` is the exact refusal with no token.

/// `GET /api/v1/accountActiveOrders` with one resting order — a **real** testnet PostOnly BTC
/// bid (our COI 178554256601), captured live 2026-08-01. `code 200`, a flat `orders` array; the
/// fate-check reads `client_order_index` to learn a landed order and `order_index` to cancel it.
pub const REST_ACTIVE_ORDERS_ONE: &str = r#"{"code":200,"orders":[{"order_index":844424912956320,"client_order_index":178554256601,"order_id":"844424912956320","client_order_id":"178554256601","market_index":1,"owner_account_index":516,"initial_base_amount":"0.00100","price":"56608.2","nonce":281474959535008,"remaining_base_amount":"0.00100","is_ask":false,"base_size":100,"base_price":566082,"filled_base_amount":"0.00000","filled_quote_amount":"0.000000","side":"","type":"limit","time_in_force":"post-only","reduce_only":false,"trigger_price":"0.0","order_expiry":1787961767927,"status":"open","trigger_status":"na","trigger_time":0,"parent_order_index":0,"parent_order_id":"0","to_trigger_order_id_0":"0","to_trigger_order_id_1":"0","to_cancel_order_id_0":"0","integrator_fee_collector_index":"","integrator_taker_fee":"","integrator_maker_fee":"","order_flags":0,"block_height":543323,"timestamp":1785542568,"created_at":1785542568,"updated_at":1785542568,"transaction_time":1785542568396486}]}"#;

/// `GET /api/v1/accountActiveOrders` on a flat account: `code 200`, empty `orders` — captured
/// live off the flat testnet account (2026-08-01). The venue positively holds nothing, so an
/// ambiguous order absent from this never landed (§3.3).
pub const REST_ACTIVE_ORDERS_EMPTY: &str = r#"{"code":200,"orders":[]}"#;

/// `GET /api/v1/accountActiveOrders` with no token — `20001`, exactly like the WS subscribe. An
/// `Err`, never an empty list: the fate is unknown, and reading it as "no orders" would expire
/// a possibly-resting order (§3.3, §1.1).
pub const REST_ACTIVE_ORDERS_AUTH_REQUIRED: &str = r#"{"code":20001,"message":"invalid param : auth query param and Authorization header are empty"}"#;

// --------------------------------------------------------------------------------------------
// Phase 2 auth-token verification bodies, captured live on testnet 2026-07-30
// (`docs/lighter-phase0-artifacts/step3_out.json`). The 8 h token ceiling is checked ONLY by
// the server (§3.4): a 9 h token mints without error on the `.so` and is rejected `20013` only
// at use. So a freshly minted token is verified with one cheap authorized GET
// (`GET /api/v1/accountLimits`) before the connector relies on it. These are the two answers
// that GET gives, byte for byte off the wire.

/// `GET /api/v1/accountLimits` with a **valid** (8 h) token: HTTP 200. Carries the fee tier
/// too, but the verification only reads the 200 (§3.4). No key or token is in this body — the
/// only account identifier is the public tier metadata.
pub const ACCOUNT_LIMITS_OK: &str = r#"{"code":200,"max_llp_percentage":100,"max_llp_amount":"0.000000","user_tier":"standard","can_create_public_pool":false,"user_tier_name":"standard","current_maker_fee_tick":0,"current_taker_fee_tick":0,"leased_lit":"0.00000000","effective_lit_stakes":"0.00000000","user_tier_last_update":0}"#;

/// `GET /api/v1/accountLimits` with a token whose deadline is over the 8 h ceiling: HTTP 401
/// `20013 invalid deadline` — the §3.4 trap, the one the cheap GET exists to catch before the
/// connector switches to a doomed token.
pub const ACCOUNT_LIMITS_INVALID_DEADLINE: &str =
    r#"{"code":20013,"message":"invalid auth: invalid deadline"}"#;
