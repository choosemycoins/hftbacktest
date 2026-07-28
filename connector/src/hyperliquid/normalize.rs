//! Putting an outgoing price and size on a grid the venue will accept.
//!
//! Hyperliquid publishes **no tick size**. A perp price is legal if it has at most five
//! significant figures *and* at most `MAX_DECIMALS - szDecimals` decimal places; a size is
//! legal if it has at most `szDecimals` of them. The effective tick is therefore
//! *price-dependent* — 1.0 for BTC near 64 000, 0.1 near 9 999 — which the depth model in
//! this repository has no way to express (design note §5.3).
//!
//! The consequence lands here. The bot registers the **finest** legal grid
//! (`10^-(6-szDecimals)`), so it can ask for prices the venue will not take, and this
//! module is what decides which legal price it gets instead. Three rules:
//!
//! * **Round toward passive**: a buy goes down, a sell goes up. The design note leaves the
//!   direction open (§5.3), so it is chosen here and recorded in §11.4 — see the note on
//!   [`normalize_price`] for why the opposite choice is unsafe for this consumer.
//! * **A price that is already legal is returned unchanged.** Anything else would move
//!   every order by a tick for no reason, and would do it invisibly.
//! * **Refuse rather than approximate.** A size that floors below one lot, or a price that
//!   rounds to zero, is not an order — it is a different order, and `AGENTS.md` §1.1 says
//!   fail closed.
//!
//! **The six-digit-price trap, now measured.** The venue's documentation says integer prices
//! are always legal regardless of significant figures. It is wrong, and the strict reading
//! implemented here is right — asked directly on testnet on 2026-07-28 (`smoke.rs`,
//! `the_significant_figure_rule_is_what_the_venue_enforces`):
//!
//! ```text
//!   six figures   12345.6  ->  Error("Price must be divisible by tick size. asset=3")
//!  five figures     12345  ->  Error("Order price cannot be more than 80% away …")
//! ```
//!
//! `12345.6` carries one decimal, which BTC's `szDecimals = 5` allows, and six significant
//! figures, which the venue does not — and the venue calls the composite rule a "tick size"
//! even though it publishes none. The five-figure form got past price validation entirely
//! (it failed a different check). So: five significant figures always, `123456` becomes
//! `123460`, and the effective tick above 100 000 is 10.
//!
//! **A second rule surfaced in the same measurement and is deliberately not implemented
//! here: a price may not be more than 80 % away from the reference price.** It is a venue
//! rejection with a clear message, it depends on a reference this module cannot see, and
//! guessing at it would refuse orders the venue would have taken. It is worth knowing about
//! because it is what a "test order far from the market" runs into first.

use hftbacktest::types::Side;

use crate::hyperliquid::{HyperliquidError, MAX_DECIMALS, rest::SymbolInfo};

/// Significant figures a perp price may carry.
const MAX_SIGNIFICANT_FIGURES: i32 = 5;

/// How close a value must be to a grid point to count as already on it.
///
/// Relative, because it is applied to a scaled integer: at BTC's 63 460 the scaled value is
/// around 6.3e5, and a double carries about 1e-16 of relative error, so 1e-9 is roughly
/// seven orders of magnitude of headroom over the error it is meant to absorb while still
/// being far smaller than one grid step.
const ON_GRID_TOLERANCE: f64 = 1e-9;

/// Rounds a price to the finest grid the venue will accept for this coin, away from the
/// market.
///
/// **Direction.** A buy rounds down and a sell rounds up. The design note fixes the
/// mechanism and not the direction, so this is the connector's call: the consumer is a
/// quoting bot, its orders are `Alo`/`GTX` more often than not, and rounding a buy *up*
/// moves it toward the touch — turning an intended maker order into a taker, or into an
/// `Alo` the venue rejects outright (`badAloPxRejected`). Rounding toward passive can only
/// ever make an order less likely to fill, which is the direction `AGENTS.md` §1.1 asks a
/// venue adapter to err in. It is the opposite of what an aggressive taker wants, and a
/// taker-first strategy should say so rather than inherit this by accident.
///
/// The adjustment is at most one venue tick, and the bot is told: the `LiveEvent::Order`
/// that comes back carries the **accepted** price, not the requested one.
pub fn normalize_price(
    price: f64,
    symbol: &SymbolInfo,
    side: Side,
) -> Result<f64, HyperliquidError> {
    if !price.is_finite() || price <= 0.0 {
        return Err(HyperliquidError::InvalidOrder(format!(
            "a price of {price} is not a price"
        )));
    }

    // Two independent caps on the number of decimals, and the venue applies both.
    let by_sz_decimals = (MAX_DECIMALS as i32) - (symbol.sz_decimals as i32);
    let by_significant_figures = MAX_SIGNIFICANT_FIGURES - 1 - decimal_exponent(price);
    let decimals = by_sz_decimals.min(by_significant_figures);

    let normalized = if decimals >= 0 {
        round_to_decimals(price, decimals, side)
    } else {
        // A six-or-more-digit integer part: the grid is coarser than 1, so the *integer*
        // side is what gets rounded. Clamping to zero decimals here is the mistake that
        // rejected every order in this band.
        let step = 10f64.powi(-decimals);
        round_to_decimals(price / step, 0, side) * step
    };

    if !normalized.is_finite() || normalized <= 0.0 {
        return Err(HyperliquidError::InvalidOrder(format!(
            "{price} has no legal price on {}'s grid: the coin allows {decimals} decimal \
             places and rounding {} leaves nothing",
            symbol.wire,
            match side {
                Side::Buy => "down",
                _ => "up",
            }
        )));
    }
    Ok(normalized)
}

/// Floors a size to the coin's lot grid, `10^-szDecimals`.
///
/// Always **down**, both sides: rounding a size up would send an order larger than the bot
/// asked for, which is a position it never requested. A size that floors to nothing is
/// refused — the venue would reject it anyway, and doing it here keeps the reason legible.
pub fn normalize_size(size: f64, symbol: &SymbolInfo) -> Result<f64, HyperliquidError> {
    if !size.is_finite() || size <= 0.0 {
        return Err(HyperliquidError::InvalidOrder(format!(
            "a size of {size} is not a size"
        )));
    }
    let normalized = round_to_decimals(size, symbol.sz_decimals as i32, Side::Buy);
    if normalized <= 0.0 {
        return Err(HyperliquidError::InvalidOrder(format!(
            "a size of {size} is below one lot of {} ({})",
            symbol.wire,
            symbol.lot_size()
        )));
    }
    Ok(normalized)
}

/// `floor(log10(value))`, without trusting `log10` at the powers of ten.
///
/// `f64::log10(1000.0)` is not required to be exactly 3, and one ulp low would put a
/// four-digit price in the three-digit band — a whole decimal place of difference in what
/// the venue accepts.
fn decimal_exponent(value: f64) -> i32 {
    let mut exponent = value.abs().log10().floor() as i32;
    // Two corrections, each at most one step: the estimate is never further out than that.
    if 10f64.powi(exponent + 1) <= value {
        exponent += 1;
    } else if 10f64.powi(exponent) > value {
        exponent -= 1;
    }
    exponent
}

/// Rounds to `decimals` places, downwards for a buy and upwards for a sell — but leaves a
/// value already on the grid exactly where it is.
///
/// The tolerance is the whole point. `63460.1 * 10.0` is `634601.0000000001` in binary
/// floating point, and a bare `floor` would turn a price that is already legal into one a
/// tick away, on every single order, silently.
fn round_to_decimals(value: f64, decimals: i32, side: Side) -> f64 {
    let scale = 10f64.powi(decimals);
    let scaled = value * scale;
    let nearest = scaled.round();
    if (scaled - nearest).abs() <= ON_GRID_TOLERANCE * nearest.abs().max(1.0) {
        return nearest / scale;
    }
    match side {
        Side::Buy => scaled.floor() / scale,
        _ => scaled.ceil() / scale,
    }
}

#[cfg(test)]
mod tests {
    use hftbacktest::types::Side;

    use crate::hyperliquid::{
        normalize::{normalize_price, normalize_size},
        rest::SymbolInfo,
        signing::hashing_string,
    };

    fn coin(wire: &str, sz_decimals: u32) -> SymbolInfo {
        SymbolInfo {
            wire: wire.to_string(),
            dex: String::new(),
            sz_decimals,
            asset_index: Some(3),
        }
    }

    /// A price the venue already accepts must come back untouched. Floating-point scaling
    /// makes this the easy thing to get wrong: `63460.1 * 10.0` is `634601.0000000001`, and
    /// a bare `floor` would move every legal price by a tick, on every order, in silence.
    #[test]
    fn a_price_already_on_the_grid_is_left_exactly_where_it_is() {
        let btc = coin("BTC", 5); // 1 decimal by szDecimals, 0 by significant figures
        let eth = coin("ETH", 4); // 2 decimals by szDecimals
        let cases = [
            (&btc, 63460.0),
            (&btc, 63457.0),
            (&btc, 9999.0),
            (&eth, 2000.5),
            (&eth, 3123.4),
            (&eth, 12.34),
        ];
        for (symbol, price) in cases {
            for side in [Side::Buy, Side::Sell] {
                assert_eq!(
                    normalize_price(price, symbol, side).unwrap(),
                    price,
                    "{} {price} {side:?}",
                    symbol.wire
                );
            }
        }
    }

    /// The five-significant-figure rule bites before the decimal-place rule for anything
    /// above 10 000, and that is where a market maker's prices live.
    #[test]
    fn five_significant_figures_is_the_binding_rule_for_a_large_price() {
        let btc = coin("BTC", 5);
        // szDecimals 5 would allow one decimal, but 63460.x is already five figures.
        assert_eq!(normalize_price(63460.7, &btc, Side::Buy).unwrap(), 63460.0);
        assert_eq!(normalize_price(63460.7, &btc, Side::Sell).unwrap(), 63461.0);
        // Below 10 000 the fifth figure is a decimal, and it is allowed.
        assert_eq!(normalize_price(9999.15, &btc, Side::Buy).unwrap(), 9999.1);
        assert_eq!(normalize_price(9999.15, &btc, Side::Sell).unwrap(), 9999.2);
    }

    /// **The trap that rejected every BTC order.** Above 99 999 the five-figure rule needs
    /// the *integer* side rounded to a multiple of ten; clamping the decimal count at zero
    /// leaves a six-figure price unchanged, and `/exchange` refuses it.
    #[test]
    fn a_six_digit_price_is_rounded_on_the_integer_side() {
        let btc = coin("BTC", 5);
        assert_eq!(
            normalize_price(123456.0, &btc, Side::Buy).unwrap(),
            123450.0
        );
        assert_eq!(
            normalize_price(123456.0, &btc, Side::Sell).unwrap(),
            123460.0
        );
        // Already a multiple of ten: unchanged, not nudged.
        assert_eq!(
            normalize_price(123460.0, &btc, Side::Buy).unwrap(),
            123460.0
        );
        assert_eq!(
            normalize_price(123460.0, &btc, Side::Sell).unwrap(),
            123460.0
        );
        // And a step further up the ladder: seven digits round to a multiple of 100.
        assert_eq!(
            normalize_price(1_234_567.0, &btc, Side::Buy).unwrap(),
            1_234_500.0
        );
        assert_eq!(
            normalize_price(1_234_567.0, &btc, Side::Sell).unwrap(),
            1_234_600.0
        );
    }

    /// A buy never rounds up and a sell never rounds down. The consumer quotes, often
    /// post-only, and the other direction turns an intended maker order into a taker — or
    /// into an `Alo` the venue rejects outright.
    #[test]
    fn rounding_always_moves_an_order_away_from_the_market() {
        let sol = coin("SOL", 2); // 4 decimals by szDecimals
        for price in [123.45678, 0.0123456, 99.999999, 1.000001] {
            let bid = normalize_price(price, &sol, Side::Buy).unwrap();
            let ask = normalize_price(price, &sol, Side::Sell).unwrap();
            assert!(bid <= price, "buy {price} became {bid}");
            assert!(ask >= price, "sell {price} became {ask}");
            assert!(bid <= ask);
        }
    }

    /// Every `szDecimals` the venue currently uses, on both caps, with the result checked
    /// against the rules rather than against a table — the point is that the two caps
    /// compose, not that six numbers are memorised.
    #[test]
    fn both_caps_are_honoured_for_every_sz_decimals() {
        for sz_decimals in 0..=5u32 {
            let symbol = coin("X", sz_decimals);
            let by_sz = 6i32 - sz_decimals as i32;
            for price in [0.001234567, 1.23456789, 1234.56789, 98765.4321, 1234567.0] {
                for side in [Side::Buy, Side::Sell] {
                    // A price too fine for this coin's grid has no legal value at all, and
                    // being refused is the whole point; it is the accepted ones that must
                    // obey both caps.
                    let Ok(out) = normalize_price(price, &symbol, side) else {
                        continue;
                    };
                    let rendered = hashing_string(out).unwrap();

                    let decimals = rendered.split_once('.').map_or(0, |(_, frac)| frac.len());
                    assert!(
                        decimals as i32 <= by_sz,
                        "szDecimals={sz_decimals} {price} -> {rendered}: {decimals} decimals \
                         exceeds the {by_sz} allowed"
                    );
                    let figures = rendered
                        .replace(['.', '-'], "")
                        .trim_start_matches('0')
                        .trim_end_matches('0')
                        .len();
                    assert!(
                        figures <= 5,
                        "szDecimals={sz_decimals} {price} -> {rendered}: {figures} significant \
                         figures"
                    );
                }
            }
        }
    }

    /// A normalised price must always survive the canonical-string renderer. If it does
    /// not, the order is refused at signing time with an error about decimals — which is
    /// the correct outcome but a terrible place to discover a normalisation bug.
    #[test]
    fn a_normalised_number_is_always_renderable_for_signing() {
        let mut seed: u64 = 0x1234_5678_9abc_def0;
        for sz_decimals in 0..=6u32 {
            let symbol = coin("X", sz_decimals);
            for _ in 0..2_000 {
                seed = seed
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                let price = ((seed >> 11) % 100_000_000) as f64 * 10f64.powi((seed % 7) as i32 - 4);
                if !(1e-6..1e9).contains(&price) {
                    continue;
                }
                for side in [Side::Buy, Side::Sell] {
                    if let Ok(normalized) = normalize_price(price, &symbol, side) {
                        hashing_string(normalized).unwrap_or_else(|error| {
                            panic!(
                                "szDecimals={sz_decimals} {price} -> {normalized} is not \
                                 renderable: {error}"
                            )
                        });
                    }
                }
                let size = ((seed >> 7) % 1_000_000) as f64 * 1e-6;
                if let Ok(normalized) = normalize_size(size, &symbol) {
                    hashing_string(normalized).unwrap();
                }
            }
        }
    }

    /// A size is floored, on both sides. Rounding one up would open a position larger than
    /// the bot asked for — a quantity it never requested and cannot see it acquired.
    #[test]
    fn a_size_is_floored_to_the_lot_grid_never_raised() {
        let btc = coin("BTC", 5); // lot 1e-5
        assert_eq!(normalize_size(0.000123456, &btc).unwrap(), 0.00012);
        assert_eq!(normalize_size(0.001, &btc).unwrap(), 0.001);
        assert_eq!(normalize_size(1.999999, &btc).unwrap(), 1.99999);

        let sol = coin("SOL", 2); // lot 0.01
        assert_eq!(normalize_size(1.2345, &sol).unwrap(), 1.23);
        assert_eq!(normalize_size(1.0, &sol).unwrap(), 1.0);

        let whole = coin("test:ABC", 0); // lot 1
        assert_eq!(normalize_size(7.9, &whole).unwrap(), 7.0);
    }

    /// Below one lot there is no order to send, and the venue would say so in its own words
    /// several hundred milliseconds later. Refusing here keeps the reason attached to the
    /// request that caused it.
    #[test]
    fn a_size_below_one_lot_is_refused_rather_than_rounded_to_something() {
        let whole = coin("test:ABC", 0);
        let error = normalize_size(0.4, &whole).unwrap_err().to_string();
        assert!(error.contains("test:ABC"), "{error}");
        assert!(error.contains("lot"), "{error}");

        let btc = coin("BTC", 5);
        assert!(normalize_size(0.000001, &btc).is_err());
        // …and the smallest size that *is* a lot still goes through.
        assert_eq!(normalize_size(0.00001, &btc).unwrap(), 0.00001);
    }

    /// Nonsense in, refusal out — never a silently plausible order.
    #[test]
    fn a_non_positive_or_non_finite_price_or_size_is_refused() {
        let btc = coin("BTC", 5);
        for bad in [0.0, -1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(normalize_price(bad, &btc, Side::Buy).is_err(), "{bad}");
            assert!(normalize_size(bad, &btc).is_err(), "{bad}");
        }
    }

    /// A price too small to survive its own coin's decimal cap has no legal value, and
    /// rounding a buy down would produce zero — which is not a cheaper order, it is a
    /// rejected one. Reachable whenever a coin's `szDecimals` is coarse relative to its
    /// price.
    #[test]
    fn a_price_that_rounds_away_to_nothing_is_refused() {
        let coarse = coin("X", 5); // one decimal place allowed
        let error = normalize_price(0.04, &coarse, Side::Buy)
            .unwrap_err()
            .to_string();
        assert!(error.contains('X'), "{error}");
        // The sell side of the same price rounds up to the first legal grid point.
        assert_eq!(normalize_price(0.04, &coarse, Side::Sell).unwrap(), 0.1);
    }
}
