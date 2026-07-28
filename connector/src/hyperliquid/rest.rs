//! `POST /info`: the perp universe, and what it fixes about an instrument.
//!
//! The universe is not optional bookkeeping. Hyperliquid publishes no tick size — a price
//! is legal if it has at most `MAX_DECIMALS - szDecimals` decimals (and at most 5
//! significant figures) — so `szDecimals` is the only thing that fixes the price grid.
//! And subscribing to a coin the venue does not list closes the entire WebSocket without
//! an error frame, so nothing may be subscribed before it has been matched here.

use std::{collections::HashMap, time::Duration};

use tracing::{info, warn};

use crate::hyperliquid::{HyperliquidError, MAX_DECIMALS};

/// Bounded because it blocks the connect path: `reqwest` has no default timeout, so a hung
/// `/info` would leave the connector "connecting" indefinitely, with no data and no error.
const INFO_TIMEOUT: Duration = Duration::from_secs(15);

/// Shortest coin name a near-miss suggestion is computed from, and how many are named.
///
/// Both bound the length of a message that ends up inside a `LiveEvent::Error`; see the
/// call site.
const MIN_NEAR_MISS_PREFIX: usize = 2;
const MAX_NEAR_MISSES: usize = 5;

/// What the venue says about one perp instrument.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SymbolInfo {
    /// The coin string as it goes on the wire, prefix and all (`BTC`, `test:ABC`).
    pub wire: String,
    /// The perp dex it belongs to; `""` is the canonical one.
    pub dex: String,
    /// Decimal places allowed in a size. Fixes both the lot size and the price grid.
    pub sz_decimals: u32,
}

impl SymbolInfo {
    /// The finest legal price increment, `10^-(MAX_DECIMALS - szDecimals)`.
    ///
    /// Hyperliquid's effective tick is *price-dependent* — the 5-significant-figure rule
    /// makes it 1.0 near 64,000 and 0.1 near 9,999 — and `tick_size` is fixed at
    /// registration, so the finest grid is the only value that never rejects a price the
    /// bot could legally have wanted. Rounding to a legal price at submit time is Phase 2's
    /// job; see the design note §5.3.
    pub fn tick_size(&self) -> f64 {
        10f64.powi(-((MAX_DECIMALS as i32) - (self.sz_decimals as i32)))
    }

    /// The size increment, `10^-szDecimals`.
    pub fn lot_size(&self) -> f64 {
        10f64.powi(-(self.sz_decimals as i32))
    }
}

/// The perp dex a wire coin string belongs to. `""` is the canonical one.
///
/// HIP-3 lets third parties deploy their own perp dexes, each with its own universe. A
/// universe entry's `name` already carries the prefix — `test:ABC` is both the entry name
/// and the wire coin string — so nothing is ever concatenated.
pub fn dex_of(symbol: &str) -> &str {
    symbol.split_once(':').map_or("", |(dex, _)| dex)
}

/// The dexes the requested coins name, deduplicated and in first-seen order.
pub fn referenced_dexes(symbols: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for symbol in symbols {
        let dex = dex_of(symbol).to_string();
        if !out.contains(&dex) {
            out.push(dex);
        }
    }
    out
}

/// Matches requested coins against already-fetched universes.
///
/// Split from the HTTP so the half that can be wrong is testable without a network:
/// `universes` maps a dex name (`""` for canonical) to that dex's `meta` response.
///
/// Fails on the first problem rather than skipping it. A skipped coin would look exactly
/// like a quiet market for the rest of the process's life.
pub fn match_universes(
    symbols: &[String],
    universes: &HashMap<String, serde_json::Value>,
) -> Result<Vec<SymbolInfo>, HyperliquidError> {
    let mut out = Vec::with_capacity(symbols.len());
    for symbol in symbols {
        let dex = dex_of(symbol);
        let meta = universes.get(dex).ok_or_else(|| {
            HyperliquidError::UniverseError(format!(
                "unknown perp dex {dex:?} in coin {symbol:?}: its universe was never \
                 fetched. The canonical dex has no prefix; builder dexes are listed by \
                 the `perpDexs` info request."
            ))
        })?;
        let entries = meta
            .get("universe")
            .and_then(|u| u.as_array())
            .ok_or_else(|| {
                HyperliquidError::UniverseError(format!(
                    "dex {dex:?}: the /info meta response has no `universe` array"
                ))
            })?;

        let entry = entries
            .iter()
            .find(|e| e.get("name").and_then(|n| n.as_str()) == Some(symbol.as_str()));

        let Some(entry) = entry else {
            // Point at the likely mistake rather than just refusing. The two common ones
            // are a quote suffix carried over from another venue (BTCUSDT) and a
            // double-prefixed builder-dex coin.
            //
            // Both the prefix length and the number of suggestions are bounded, and not for
            // readability: this message travels to the bots inside a `LiveEvent::Error`,
            // which is bincode-encoded into a fixed 512-byte slice, and an encode that does
            // not fit kills the connector process (see `HyperliquidError::to_value`). A
            // one-character base matched most of a real universe — 780 characters against
            // 103 names, measured — and an empty one matched all of it.
            let base = symbol.rsplit(':').next().unwrap_or(symbol);
            let near: Vec<&str> = if base.len() < MIN_NEAR_MISS_PREFIX {
                Vec::new()
            } else {
                universes
                    .values()
                    .filter_map(|m| m.get("universe").and_then(|u| u.as_array()))
                    .flatten()
                    .filter_map(|e| e.get("name").and_then(|n| n.as_str()))
                    .filter(|name| {
                        let name_base = name.rsplit(':').next().unwrap_or(name);
                        base.starts_with(name_base) || name_base.starts_with(base)
                    })
                    .take(MAX_NEAR_MISSES)
                    .collect()
            };
            return Err(HyperliquidError::UnknownSymbol(if near.is_empty() {
                format!("{symbol} is not listed on perp dex {dex:?}")
            } else {
                format!(
                    "{symbol} is not listed on perp dex {dex:?}; did you mean: {}? \
                     Names are case-sensitive, the canonical dex takes a bare coin \
                     (BTC, not BTCUSDT), and a builder dex takes its prefixed name.",
                    near.join(", ")
                )
            }));
        };

        // `isDelisted` is the venue's only warning that a subscription will produce
        // nothing at all. Refusing to start beats running for a day and reporting silence.
        if entry
            .get("isDelisted")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            return Err(HyperliquidError::UnknownSymbol(format!(
                "{symbol} is delisted on perp dex {dex:?}; it would never produce data"
            )));
        }

        let sz_decimals = entry
            .get("szDecimals")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| {
                HyperliquidError::UniverseError(format!(
                    "{symbol}: the universe entry has no usable `szDecimals`, so neither \
                     the lot size nor the price grid can be derived"
                ))
            })? as u32;

        out.push(SymbolInfo {
            wire: symbol.clone(),
            dex: dex.to_string(),
            sz_decimals,
        });
    }
    Ok(out)
}

/// Splits requested coins into the ones that can be subscribed and the ones the venue
/// does not list, with the reason.
///
/// All-or-nothing resolution would let one typo cost every other coin its market data: the
/// subscribe step fails, the connection is torn down and retried, and the typo is still in
/// the symbol set. This keeps the failure local to the coin that caused it.
pub fn split_resolvable(
    symbols: &[String],
    universes: &HashMap<String, serde_json::Value>,
) -> (Vec<SymbolInfo>, Vec<(String, HyperliquidError)>) {
    let mut resolved = Vec::new();
    let mut rejected = Vec::new();
    for symbol in symbols {
        match match_universes(std::slice::from_ref(symbol), universes) {
            Ok(mut one) => resolved.append(&mut one),
            Err(error) => rejected.push((symbol.clone(), error)),
        }
    }
    (resolved, rejected)
}

/// Whether an unsuccessful `POST /info` answer means "there is no such perp dex".
///
/// Measured on testnet 2026-07-28: `{"type":"meta","dex":"nope_xyz"}` answers **HTTP 500
/// with a `null` body**. That exact pair is the whole signature, and the narrowness is the
/// point.
///
/// Reading *any* status error as "no such dex" is what makes a transient hiccup permanent:
/// `/info` is weight-20 against a 1200/min IP budget and the venue sits behind a CDN, so a
/// 429 or a 502 on the canonical dex would leave its universe unfetched, every bare coin
/// refused as unlistable, and — because refusals are remembered for the connection — a
/// socket that stays up, answers pings, and publishes nothing until the process is
/// restarted. Anything but the measured signature is therefore "the venue did not answer",
/// which the caller retries.
pub fn is_unknown_dex(status: u16, body: &str) -> bool {
    status == 500 && body.trim() == "null"
}

/// Fetches the `meta` universe of every dex the requested coins name.
///
/// A dex the venue says it does not know is **skipped**, not fatal — coins naming it are
/// then reported per-coin by [`split_resolvable`]. Every other failure, transport or
/// status, is propagated so the caller retries the connection; see [`is_unknown_dex`].
pub async fn fetch_universes(
    rest_url: &str,
    symbols: &[String],
) -> Result<HashMap<String, serde_json::Value>, HyperliquidError> {
    let client = reqwest::Client::builder().timeout(INFO_TIMEOUT).build()?;
    let mut universes = HashMap::new();
    for dex in referenced_dexes(symbols) {
        let body = if dex.is_empty() {
            serde_json::json!({ "type": "meta" })
        } else {
            serde_json::json!({ "type": "meta", "dex": dex })
        };
        let response = client
            .post(format!("{rest_url}/info"))
            .json(&body)
            .send()
            .await?;
        let status = response.status();
        if status.is_success() {
            universes.insert(dex, response.json().await?);
            continue;
        }
        let body = response.text().await.unwrap_or_default();
        if is_unknown_dex(status.as_u16(), &body) {
            warn!(%dex, %status, "Hyperliquid does not know this perp dex.");
        } else {
            return Err(HyperliquidError::UniverseUnavailable(format!(
                "/info answered HTTP {status} for perp dex {dex:?}, which is not the \
                 venue's \"no such dex\" answer; treating it as such would refuse every \
                 coin on a dex that exists"
            )));
        }
    }
    Ok(universes)
}

/// Resolves the requested coins against the venue, reporting per coin.
///
/// Fails only when the venue could not be reached at all — that is transient, and the
/// caller should retry. A coin the venue does not list comes back in the second list and
/// is the caller's to refuse, loudly, without taking the other coins down with it.
///
/// The `tick_size`/`lot_size` of each coin is logged because the bot — not the connector —
/// declares them when it registers the instrument (`LiveRequest::RegisterInstrument`), and
/// nothing in the current `Connector` trait lets this backend see or correct the bot's
/// values. A bot that registers a coarser tick than the venue's grid silently collapses
/// two price levels into one in the connector's own fused depth, so the operator needs
/// these numbers in front of them.
#[allow(clippy::type_complexity)]
pub async fn resolve_symbols(
    rest_url: &str,
    symbols: &[String],
) -> Result<(Vec<SymbolInfo>, Vec<(String, HyperliquidError)>), HyperliquidError> {
    let universes = fetch_universes(rest_url, symbols).await?;
    let (resolved, rejected) = split_resolvable(symbols, &universes);
    for info in &resolved {
        info!(
            coin = %info.wire,
            sz_decimals = info.sz_decimals,
            tick_size = info.tick_size(),
            lot_size = info.lot_size(),
            "Resolved a Hyperliquid instrument; register it in the bot with exactly this \
             tick_size and lot_size."
        );
    }
    Ok((resolved, rejected))
}

#[cfg(test)]
mod tests {
    use crate::hyperliquid::{
        fixtures::{META_CANONICAL, META_DEX_TEST, META_DEX_UNIT},
        rest::{SymbolInfo, dex_of, is_unknown_dex, match_universes},
    };

    fn universes(entries: &[(&str, &str)]) -> std::collections::HashMap<String, serde_json::Value> {
        entries
            .iter()
            .map(|(dex, json)| (dex.to_string(), serde_json::from_str(json).unwrap()))
            .collect()
    }

    /// Hyperliquid publishes no tick size. The finest legal increment is
    /// `10^-(6 - szDecimals)` and the lot is `10^-szDecimals`, so both fall out of the one
    /// field the universe does carry. Getting the tick wrong by a factor of ten is not an
    /// error anywhere: it silently collapses two price levels into one in the connector's
    /// own fused depth.
    #[test]
    fn tick_and_lot_are_derived_from_sz_decimals() {
        let cases = [
            // (szDecimals, tick_size, lot_size)
            (0u32, 1e-6, 1.0),
            (1, 1e-5, 0.1),
            (2, 1e-4, 0.01),
            (4, 1e-2, 1e-4),
            (5, 1e-1, 1e-5),
            (6, 1.0, 1e-6),
        ];
        for (sz_decimals, tick, lot) in cases {
            let info = SymbolInfo {
                wire: "X".into(),
                dex: String::new(),
                sz_decimals,
            };
            assert!(
                (info.tick_size() - tick).abs() < tick * 1e-9,
                "szDecimals={sz_decimals}: tick {} != {tick}",
                info.tick_size()
            );
            assert!(
                (info.lot_size() - lot).abs() < lot * 1e-9,
                "szDecimals={sz_decimals}: lot {} != {lot}",
                info.lot_size()
            );
        }
    }

    /// A bare coin belongs to the canonical dex, which is keyed by the empty string.
    #[test]
    fn a_canonical_coin_resolves_to_its_universe_entry() {
        let resolved = match_universes(
            &["BTC".to_string(), "ETH".to_string()],
            &universes(&[("", META_CANONICAL)]),
        )
        .unwrap();

        assert_eq!(resolved.len(), 2);
        assert_eq!(resolved[0].wire, "BTC");
        assert_eq!(resolved[0].sz_decimals, 5);
        assert_eq!(resolved[0].tick_size(), 0.1);
        assert_eq!(resolved[0].lot_size(), 1e-5);
        assert_eq!(resolved[1].sz_decimals, 4);
    }

    /// A HIP-3 builder dex has its own universe, fetched with `{"type":"meta","dex":...}`.
    /// The universe entry's `name` is the full prefixed string and is also the wire coin
    /// name, so nothing is concatenated — and `test:ABC` has `szDecimals: 0`, i.e. a
    /// different tick from every canonical coin.
    #[test]
    fn a_dex_prefixed_coin_resolves_from_its_own_dex_universe() {
        let resolved = match_universes(
            &["BTC".to_string(), "test:ABC".to_string()],
            &universes(&[("", META_CANONICAL), ("test", META_DEX_TEST)]),
        )
        .unwrap();

        assert_eq!(resolved[1].wire, "test:ABC");
        assert_eq!(resolved[1].dex, "test");
        assert_eq!(resolved[1].sz_decimals, 0);
        assert_eq!(resolved[1].tick_size(), 1e-6);
        assert_eq!(resolved[1].lot_size(), 1.0);
    }

    #[test]
    fn dex_of_reads_the_prefix() {
        assert_eq!(dex_of("BTC"), "");
        assert_eq!(dex_of("test:ABC"), "test");
        assert_eq!(dex_of("unit:ES"), "unit");
    }

    /// An unknown coin must never reach a subscribe frame: Hyperliquid answers one by
    /// closing the whole WebSocket, with no error frame and no close reason, taking every
    /// other coin's subscriptions with it. Verified against testnet on 2026-07-28.
    #[test]
    fn an_unknown_coin_is_refused_and_the_near_miss_is_named() {
        let error = match_universes(
            &["BTCUSDT".to_string()],
            &universes(&[("", META_CANONICAL)]),
        )
        .unwrap_err();

        let msg = error.to_string();
        assert!(msg.contains("BTCUSDT"), "{msg}");
        assert!(
            msg.contains("BTC"),
            "the near miss should be suggested: {msg}"
        );
    }

    /// A coin on a dex whose universe was never fetched is an error, not a silent skip.
    #[test]
    fn a_coin_on_an_unfetched_dex_is_refused() {
        let error = match_universes(
            &["nope:ABC".to_string()],
            &universes(&[("", META_CANONICAL)]),
        )
        .unwrap_err();
        assert!(error.to_string().contains("nope"), "{error}");
    }

    /// `isDelisted` is the venue's only warning that a subscription will produce nothing.
    /// Refusing to start beats a connector that runs for a day and reports no data.
    #[test]
    fn a_delisted_coin_is_refused() {
        let error = match_universes(
            &["unit:NQ".to_string()],
            &universes(&[("", META_CANONICAL), ("unit", META_DEX_UNIT)]),
        )
        .unwrap_err();
        assert!(error.to_string().contains("unit:NQ"), "{error}");
        assert!(error.to_string().contains("delisted"), "{error}");

        // Its live sibling on the same dex still resolves.
        let ok = match_universes(
            &["unit:ES".to_string()],
            &universes(&[("", META_CANONICAL), ("unit", META_DEX_UNIT)]),
        )
        .unwrap();
        assert_eq!(ok[0].sz_decimals, 2);
    }

    /// One coin the venue does not list must not cost every other coin its market data.
    /// A bot registering a typo would otherwise take the whole feed down: the subscribe
    /// step fails, the connection is torn down and retried, and the typo is still in the
    /// symbol set — an endless reconnect loop with no data for anyone.
    #[test]
    fn an_unlisted_coin_does_not_hide_the_ones_that_resolve() {
        use crate::hyperliquid::rest::split_resolvable;

        let (resolved, rejected) = split_resolvable(
            &[
                "BTC".to_string(),
                "BTCUSDT".to_string(),
                "nope:ABC".to_string(),
                "ETH".to_string(),
            ],
            &universes(&[("", META_CANONICAL)]),
        );

        assert_eq!(
            resolved.iter().map(|s| s.wire.as_str()).collect::<Vec<_>>(),
            ["BTC", "ETH"]
        );
        assert_eq!(rejected.len(), 2);
        assert_eq!(rejected[0].0, "BTCUSDT");
        assert!(rejected[0].1.to_string().contains("BTCUSDT"));
        assert_eq!(rejected[1].0, "nope:ABC");
        // The dex whose universe was never fetched is named, since that is the fix.
        assert!(
            rejected[1].1.to_string().contains("nope"),
            "{}",
            rejected[1].1
        );
    }

    /// The dexes to fetch are exactly those the requested coins name, deduplicated. The
    /// canonical one is the empty string, and it is only fetched if a bare coin was asked
    /// for.
    #[test]
    fn only_the_referenced_dexes_are_fetched() {
        use crate::hyperliquid::rest::referenced_dexes;

        assert_eq!(
            referenced_dexes(&[
                "BTC".to_string(),
                "ETH".to_string(),
                "test:ABC".to_string(),
                "test:DEF".to_string(),
            ]),
            vec!["".to_string(), "test".to_string()]
        );
        assert_eq!(
            referenced_dexes(&["unit:ES".to_string()]),
            vec!["unit".to_string()]
        );
    }

    /// **The blocker this discriminator prevents.** Reading every unsuccessful `/info`
    /// answer as "no such perp dex" turns a transient venue hiccup into a permanent one:
    /// the canonical dex's universe is missing, every bare coin is refused as unlistable,
    /// the refusals are remembered for the connection, and the connector then runs
    /// connected, answering pings, and publishing nothing until it is restarted. `/info` is
    /// weight-20 against a 1200/min IP budget and the venue sits behind a CDN, so 429 and
    /// 5xx are ordinary events, not exotic ones.
    ///
    /// Only the measured signature counts — HTTP 500 with a `null` body, testnet
    /// 2026-07-28. Everything else means the venue did not answer, and is retried.
    #[test]
    fn only_the_measured_answer_means_there_is_no_such_perp_dex() {
        assert!(is_unknown_dex(500, "null"));
        assert!(is_unknown_dex(500, "null\n"));

        // A rate limit, a CDN failure, and a 500 that carries something else are all "the
        // venue did not answer".
        assert!(!is_unknown_dex(429, "Too many requests"));
        assert!(!is_unknown_dex(502, "<html>...</html>"));
        assert!(!is_unknown_dex(503, ""));
        assert!(!is_unknown_dex(500, ""));
        assert!(!is_unknown_dex(500, "{\"error\":\"internal\"}"));
    }

    /// The near-miss suggestion travels to the bots inside a `LiveEvent::Error`, which is
    /// bincode-encoded into a fixed 512-byte slice; an encode that does not fit kills the
    /// connector process under its own panic hook, on every restart. Measured before the
    /// bound: a symbol whose portion after the last `:` was empty matched every name in a
    /// 103-name universe and produced a 780-character message. Any symbol ending in `:`
    /// reaches it, as does a one-character prefix against a real ~200-coin mainnet universe.
    #[test]
    fn the_near_miss_suggestion_cannot_grow_without_bound() {
        let names: Vec<String> = (0..103).map(|i| format!("COIN{i:03}")).collect();
        let big = serde_json::json!({
            "universe": names
                .iter()
                .map(|name| serde_json::json!({ "name": name, "szDecimals": 2 }))
                .collect::<Vec<_>>(),
        });
        let universes: std::collections::HashMap<String, serde_json::Value> =
            [(String::new(), big)].into_iter().collect();

        for symbol in ["", ":", "test:", "C", "COIN0"] {
            let error = match_universes(&[symbol.to_string()], &universes).unwrap_err();
            assert!(
                error.to_string().len() < 400,
                "{symbol:?} produced {} characters",
                error.to_string().len()
            );
        }

        // A prefix long enough to mean something still gets its suggestions.
        let error = match_universes(&["COIN00".to_string()], &universes).unwrap_err();
        assert!(error.to_string().contains("COIN000"), "{error}");
    }
}
