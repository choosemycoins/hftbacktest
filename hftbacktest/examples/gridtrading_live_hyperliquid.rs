//! A small, calm grid quoted on Hyperliquid through the `hyperliquid` connector backend.
//!
//! This is `gridtrading_live.rs` pointed at a different venue, with the parameters turned
//! down to what a testnet rehearsal wants. Four things differ from that example, and each
//! one is a venue constraint rather than taste:
//!
//! * **`tick_size` / `lot_size` are derived from `szDecimals`, not published.** Hyperliquid
//!   has no tick size field. A price is legal with at most `6 - szDecimals` decimals *and*
//!   at most 5 significant figures, so the finest legal grid is `10^-(6 - szDecimals)` and
//!   the lot is `10^-szDecimals`. BTC is `szDecimals = 5`, measured on testnet 2026-07-28,
//!   hence 0.1 and 0.00001 below. The connector logs both at subscribe time ("Resolved a
//!   Hyperliquid instrument"); these must match, and a mismatch is silent — a coarser lot
//!   makes book levels below it vanish before the bot ever sees them.
//!
//! * **The five-significant-figure rule is the binding one up here.** At a BTC price near
//!   63,700 the 5-figure cap allows *zero* decimals, so the venue's effective grid is 1.0
//!   even though `tick_size` is 0.1. `MIN_GRID_STEP` is set to 1.0 to match: quoting on a
//!   finer grid than the venue honours just means the connector rounds the price and the
//!   grid churns for no reason. The connector rounds toward passive, so nothing crosses.
//!
//! * **Order size has a floor in dollars, not in lots.** Hyperliquid refuses an order below
//!   roughly $10 notional whatever the lot grid says. See `ORDER_QTY`.
//!
//! * **Requoting costs rate-limit budget that only volume replenishes.** The address-based
//!   limit gives a fresh account a 10,000-request buffer and then refills at 1 request per
//!   1 USDC traded — so a quoting bot that does not fill only ever spends. Hence a 1-second
//!   cadence rather than the 100 ms of `gridtrading_live.rs`, and a 3-level grid.
//!
//! `LiveBot::modify` is `todo!()` and panics, so the loop below only ever cancels and
//! submits. Do not "improve" it into an amend.
//!
//! Run the connector first, then this:
//!
//! ```text
//! cargo run -p connector -- hyperliquid hyperliquid /path/to/hl-testnet-connector.toml
//! cargo run -p hftbacktest --example gridtrading_live_hyperliquid
//! ```

use std::{collections::HashMap, fmt::Debug};

use hftbacktest::{
    live::{
        Instrument,
        LiveBot,
        LiveBotBuilder,
        LoggingRecorder,
        ipc::iceoryx::IceoryxUnifiedChannel,
    },
    prelude::*,
};

/// Must equal `order_prefix` in the connector's config.
///
/// Hyperliquid's `cloid` is a fixed 128-bit value — `0x` plus exactly 32 hex characters —
/// so unlike Bybit's `orderLinkId` it cannot carry readable text. The connector stamps
/// these four characters into the high nibbles instead, and that is how it tells its own
/// orders from a human's on the same account. It must be four *hexadecimal* characters: a
/// mnemonic like "hf01" contains no valid hex digit, which makes every cloid malformed and
/// earns an HTTP 422 on every single order.
const ORDER_PREFIX: &str = "a1f0";

/// The connector name passed to `connector <name> hyperliquid <config>`.
const CONNECTOR: &str = "hyperliquid";

/// Hyperliquid names the bare coin, not a pair: `BTC`, never `BTCUSDT`.
const SYMBOL: &str = "BTC";

/// `10^-(6 - szDecimals)` for `szDecimals = 5`. Verify against the connector's own log line.
const TICK_SIZE: f64 = 0.1;

/// `10^-szDecimals` for `szDecimals = 5`. Verify against the connector's own log line.
const LOT_SIZE: f64 = 0.00001;

/// The venue's effective price grid for BTC at this price band — see the module note.
const MIN_GRID_STEP: f64 = 1.0;

/// One order's size, on the lot grid and above the venue's ~$10 minimum notional.
///
/// 0.0002 BTC is about $12.7 at the testnet price measured on 2026-07-28 (~63,700). It is a
/// constant rather than a figure derived from the live mid because a derived size drifts off
/// the lot grid and turns a rejection into a puzzle; if BTC ever trades near $50,000 this
/// needs raising, and the startup check below says so out loud rather than letting the venue
/// refuse every order.
const ORDER_QTY: f64 = 0.0002;

/// The venue's minimum order value, in USD.
const MIN_NOTIONAL_USD: f64 = 10.0;

/// How long one loop iteration waits. See the module note on rate-limit budget.
const ELAPSE_NS: i64 = 1_000_000_000;

fn prepare_live() -> LiveBot<IceoryxUnifiedChannel, HashMapMarketDepth> {
    LiveBotBuilder::new()
        .register(Instrument::new(
            CONNECTOR,
            SYMBOL,
            TICK_SIZE,
            LOT_SIZE,
            HashMapMarketDepth::new(TICK_SIZE, LOT_SIZE),
            0,
        ))
        .build()
        .unwrap()
}

/// Blocks until the connector has confirmed account state *and* the book has two sides.
///
/// The two conditions are genuinely separate, and conflating them is the trap.
/// `snapshot_ready` says the connector has finished its account round-trip — it says nothing
/// whatever about the book, because `LiveBot` applies only incremental depth events and
/// drops the snapshot ones. Quoting off a half-built book is how a grid puts a bid through
/// somebody's ask.
fn wait_until_ready<I, MD>(hbt: &mut I) -> Result<bool, I::Error>
where
    MD: MarketDepth,
    I: Bot<MD>,
{
    while hbt.elapse(ELAPSE_NS)? == ElapseResult::Ok {
        let ready = hbt.snapshot_ready(0);
        let depth = hbt.depth(0);
        let has_book = depth.best_bid_tick() != INVALID_MIN && depth.best_ask_tick() != INVALID_MAX;
        if ready && has_book {
            let mid = (depth.best_bid() + depth.best_ask()) / 2.0;
            let notional = mid * ORDER_QTY;
            tracing::info!(
                best_bid = depth.best_bid(),
                best_ask = depth.best_ask(),
                mid,
                order_qty = ORDER_QTY,
                notional_usd = notional,
                "Book is two-sided and the account snapshot is in; starting to quote."
            );
            if notional < MIN_NOTIONAL_USD {
                // Fail closed: every order would be refused, and the venue's reason for
                // refusing is easy to misread as a signing problem.
                tracing::error!(
                    notional_usd = notional,
                    minimum_usd = MIN_NOTIONAL_USD,
                    "ORDER_QTY is below the venue's minimum notional; raise it and restart."
                );
                return Ok(false);
            }
            return Ok(true);
        }
        tracing::info!(
            snapshot_ready = ready,
            has_book,
            "Waiting for the connector."
        );
    }
    Ok(false)
}

/// The grid from `examples/algo.rs`, kept deliberately close to it so the two can be
/// diffed, with the elapse interval taken as an argument instead of hardcoded at 100 ms.
///
/// It is copied rather than shared because `algo.rs` is what the *backtest* examples run,
/// and re-timing that helper to suit one venue's rate limiter would change results the
/// backtest examples are meant to produce.
#[allow(clippy::too_many_arguments)]
fn gridtrading<MD, I, R>(
    hbt: &mut I,
    recorder: &mut R,
    relative_half_spread: f64,
    relative_grid_interval: f64,
    grid_num: usize,
    min_grid_step: f64,
    skew: f64,
    order_qty: f64,
    max_position: f64,
    elapse_ns: i64,
) -> Result<(), i64>
where
    MD: MarketDepth,
    I: Bot<MD>,
    <I as Bot<MD>>::Error: Debug,
    R: Recorder,
    <R as Recorder>::Error: Debug,
{
    let tick_size = hbt.depth(0).tick_size();
    // min_grid_step should be in multiples of tick_size.
    let min_grid_step = (min_grid_step / tick_size).round() * tick_size;
    let mut int = 0;
    while ElapseResult::Ok == hbt.elapse(elapse_ns).unwrap() {
        int += 1;
        if int % 10 == 0 {
            recorder.record(hbt).unwrap();
        }

        let depth = hbt.depth(0);
        let position = hbt.position(0);

        if depth.best_bid_tick() == INVALID_MIN || depth.best_ask_tick() == INVALID_MAX {
            // Market depth is incomplete.
            continue;
        }

        let mid_price = (depth.best_bid() + depth.best_ask()) / 2.0;

        let normalized_position = position / order_qty;

        let relative_bid_depth = relative_half_spread + skew * normalized_position;
        let relative_ask_depth = relative_half_spread - skew * normalized_position;
        let alpha = 0.0;
        let forecast_mid_price = mid_price + alpha;

        let bid_price = (forecast_mid_price * (1.0 - relative_bid_depth)).min(depth.best_bid());
        let ask_price = (forecast_mid_price * (1.0 + relative_ask_depth)).max(depth.best_ask());

        // min_grid_step enforces grid interval changes to be no less than min_grid_step,
        // which stabilizes the grid_interval and keeps the orders on the grid more stable.
        let grid_interval = ((forecast_mid_price * relative_grid_interval / min_grid_step).round()
            * min_grid_step)
            .max(min_grid_step);

        let mut bid_price = (bid_price / grid_interval).floor() * grid_interval;
        let mut ask_price = (ask_price / grid_interval).ceil() * grid_interval;

        //--------------------------------------------------------
        // Updates quotes

        hbt.clear_inactive_orders(Some(0));

        {
            let orders = hbt.orders(0);
            let mut new_bid_orders = HashMap::new();
            if position < max_position && bid_price.is_finite() {
                for _ in 0..grid_num {
                    let bid_price_tick = (bid_price / tick_size).round() as u64;

                    // order price in tick is used as order id.
                    new_bid_orders.insert(bid_price_tick, bid_price);

                    bid_price -= grid_interval;
                }
            }
            // Cancels if an order is not in the new grid.
            let cancel_order_ids: Vec<u64> = orders
                .values()
                .filter(|order| {
                    order.side == Side::Buy
                        && order.cancellable()
                        && !new_bid_orders.contains_key(&order.order_id)
                })
                .map(|order| order.order_id)
                .collect();
            // Posts an order if it doesn't exist.
            let new_orders: Vec<(u64, f64)> = new_bid_orders
                .into_iter()
                .filter(|(order_id, _)| !orders.contains_key(order_id))
                .collect();
            for order_id in cancel_order_ids {
                hbt.cancel(0, order_id, false).unwrap();
            }
            for (order_id, order_price) in new_orders {
                hbt.submit_buy_order(
                    0,
                    order_id,
                    order_price,
                    order_qty,
                    TimeInForce::GTX,
                    OrdType::Limit,
                    false,
                )
                .unwrap();
            }
        }

        {
            let orders = hbt.orders(0);
            let mut new_ask_orders = HashMap::new();
            if position > -max_position && ask_price.is_finite() {
                for _ in 0..grid_num {
                    let ask_price_tick = (ask_price / tick_size).round() as u64;

                    // order price in tick is used as order id.
                    new_ask_orders.insert(ask_price_tick, ask_price);

                    ask_price += grid_interval;
                }
            }
            // Cancels if an order is not in the new grid.
            let cancel_order_ids: Vec<u64> = orders
                .values()
                .filter(|order| {
                    order.side == Side::Sell
                        && order.cancellable()
                        && !new_ask_orders.contains_key(&order.order_id)
                })
                .map(|order| order.order_id)
                .collect();
            // Posts an order if it doesn't exist.
            let new_orders: Vec<(u64, f64)> = new_ask_orders
                .into_iter()
                .filter(|(order_id, _)| !orders.contains_key(order_id))
                .collect();
            for order_id in cancel_order_ids {
                hbt.cancel(0, order_id, false).unwrap();
            }
            for (order_id, order_price) in new_orders {
                hbt.submit_sell_order(
                    0,
                    order_id,
                    order_price,
                    order_qty,
                    TimeInForce::GTX,
                    OrdType::Limit,
                    false,
                )
                .unwrap();
            }
        }
    }
    Ok(())
}

fn main() {
    tracing_subscriber::fmt::init();

    tracing::info!(
        connector = CONNECTOR,
        symbol = SYMBOL,
        tick_size = TICK_SIZE,
        lot_size = LOT_SIZE,
        order_prefix = ORDER_PREFIX,
        "Starting. tick_size/lot_size and order_prefix must match the connector's config."
    );

    let mut hbt = prepare_live();

    if !wait_until_ready(&mut hbt).unwrap() {
        tracing::error!("Never became ready to quote; exiting without placing an order.");
        hbt.close().unwrap();
        return;
    }

    // 0.008% either side — about $5 at a BTC near 63,700, which is inside the venue's own
    // half-spread, so `bid_price.min(best_bid)` / `ask_price.max(best_ask)` clamp the quotes
    // onto the touch. That is deliberate: the previous rehearsal at 0.08% ($51 off mid) sat
    // wider than the entire session's price range and never filled once, which proves the
    // order path only up to the ack. Quoting at the touch is what makes a fill — and hence
    // the position, fee and reconcile paths — observable. It costs more requoting, which is
    // the trade being made knowingly.
    let relative_half_spread = 0.00008;
    let relative_grid_interval = 0.00008;
    let grid_num = 2;
    let skew = relative_half_spread / grid_num as f64;
    let max_position = grid_num as f64 * ORDER_QTY;

    let mut recorder = LoggingRecorder::new();
    gridtrading(
        &mut hbt,
        &mut recorder,
        relative_half_spread,
        relative_grid_interval,
        grid_num,
        MIN_GRID_STEP,
        skew,
        ORDER_QTY,
        max_position,
        ELAPSE_NS,
    )
    .unwrap();
    hbt.close().unwrap();
}
