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
    /// The `a` field of an order action: this coin's index in `meta.universe`.
    ///
    /// `None` for a coin on a HIP-3 builder dex. Their asset ids are
    /// `100000 + dex_index * 10000 + universe_index`, and `dex_index` comes from a
    /// `perpDexs` request this backend does not make — so rather than guess an index that
    /// would address **a different coin entirely**, orders on those coins are refused with
    /// a legible reason and their market data keeps flowing.
    ///
    /// The index is also why it is resolved at run time and never cached across networks:
    /// testnet and mainnet number their universes differently, and a delisting reshuffles
    /// both.
    pub asset_index: Option<u32>,
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

        let found = entries
            .iter()
            .enumerate()
            .find(|(_, e)| e.get("name").and_then(|n| n.as_str()) == Some(symbol.as_str()));

        let Some((universe_index, entry)) = found else {
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
            asset_index: dex.is_empty().then_some(universe_index as u32),
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

/// One order the venue says is resting, as far as reconciliation cares.
#[derive(Clone, Debug, PartialEq)]
pub struct OpenOrder {
    pub coin: String,
    /// The venue's own id. Present in every `/info` open-order shape, which is why
    /// reconciliation keys on it rather than on the client id.
    pub oid: u64,
    /// The client id, when the endpoint returns one. `openOrders` does not, and
    /// `frontendOpenOrders` does — so this being `None` says nothing about ownership.
    pub cloid: Option<String>,
}

/// Reads `frontendOpenOrders` (or `openOrders`) into the shape reconciliation needs.
///
/// Tolerant on purpose: an entry without a usable `oid` is skipped rather than failing the
/// whole list, because losing the list entirely is what causes a connector to expire live
/// orders it should have kept.
pub fn parse_open_orders(value: &serde_json::Value) -> Vec<OpenOrder> {
    value
        .as_array()
        .map(|entries| {
            entries
                .iter()
                .filter_map(|entry| {
                    Some(OpenOrder {
                        coin: entry.get("coin")?.as_str()?.to_string(),
                        oid: entry.get("oid")?.as_u64()?,
                        cloid: entry
                            .get("cloid")
                            .and_then(|c| c.as_str())
                            .map(str::to_string),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// The account's signed position per coin, from `clearinghouseState`.
///
/// `szi` is **signed** — negative is short — so there is no side field to misread. A coin
/// with no entry is flat: an account that has never traded a perp has an empty
/// `assetPositions`, and reading that as "unknown" would leave a bot without a position at
/// all.
pub fn parse_positions(value: &serde_json::Value) -> (Vec<(String, f64)>, i64) {
    let time = value.get("time").and_then(|t| t.as_i64()).unwrap_or(0);
    let positions = value
        .get("assetPositions")
        .and_then(|p| p.as_array())
        .map(|entries| {
            entries
                .iter()
                .filter_map(|entry| {
                    let position = entry.get("position")?;
                    let coin = position.get("coin")?.as_str()?.to_string();
                    let szi = position.get("szi")?;
                    let qty = match szi {
                        serde_json::Value::String(text) => text.parse().ok()?,
                        serde_json::Value::Number(number) => number.as_f64()?,
                        _ => return None,
                    };
                    Some((coin, qty))
                })
                .collect()
        })
        .unwrap_or_default();
    (positions, time)
}

/// `POST /info` with an arbitrary body, bounded.
async fn info(
    rest_url: &str,
    body: serde_json::Value,
) -> Result<serde_json::Value, HyperliquidError> {
    let client = reqwest::Client::builder().timeout(INFO_TIMEOUT).build()?;
    let response = client
        .post(format!("{rest_url}/info"))
        .json(&body)
        .send()
        .await?;
    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(HyperliquidError::UniverseUnavailable(format!(
            "/info answered HTTP {status}: {}",
            text.trim()
        )));
    }
    Ok(serde_json::from_str(&text)?)
}

/// The orders the venue says are resting for `user`.
///
/// **`frontendOpenOrders`, not `openOrders`**: the two carry the same orders, and only this
/// one echoes the `cloid`. That matters because a client id is how this backend tells its
/// own orders from a human's on the same account — but reconciliation deliberately does not
/// *depend* on it (see `OrderManager::reconcile`), so a venue that stops returning it
/// degrades rather than expiring live orders.
///
/// `user` is the **master account address**. Asking with the API wallet's address returns an
/// empty list rather than an error, which reads exactly like a flat account.
pub async fn open_orders(rest_url: &str, user: &str) -> Result<Vec<OpenOrder>, HyperliquidError> {
    let value = info(
        rest_url,
        serde_json::json!({ "type": "frontendOpenOrders", "user": user }),
    )
    .await?;
    Ok(parse_open_orders(&value))
}

/// The account's positions, and the venue time they were read at.
pub async fn positions(
    rest_url: &str,
    user: &str,
) -> Result<(Vec<(String, f64)>, i64), HyperliquidError> {
    let value = info(
        rest_url,
        serde_json::json!({ "type": "clearinghouseState", "user": user }),
    )
    .await?;
    Ok(parse_positions(&value))
}

/// Whether the venue lists `agent` as an approved API wallet for `master`.
///
/// The startup probe. An unapproved agent signs perfectly well and every order comes back
/// as `"User or API Wallet 0x… does not exist"` — a message that reads like a bad key and
/// is not one. Asking first turns that into one legible line at startup.
pub async fn agent_is_approved(
    rest_url: &str,
    master: &str,
    agent: &str,
) -> Result<bool, HyperliquidError> {
    let value = info(
        rest_url,
        serde_json::json!({ "type": "extraAgents", "user": master }),
    )
    .await?;
    let agent = agent.to_lowercase();
    Ok(value
        .as_array()
        .map(|entries| {
            entries.iter().any(|entry| {
                entry
                    .get("address")
                    .and_then(|a| a.as_str())
                    .is_some_and(|address| address.eq_ignore_ascii_case(&agent))
            })
        })
        .unwrap_or(false))
}

#[cfg(test)]
mod tests {
    use crate::hyperliquid::{
        fixtures::{
            CLEARINGHOUSE_STATE_FLAT,
            CLEARINGHOUSE_STATE_POSITIONS,
            META_CANONICAL,
            META_DEX_TEST,
            META_DEX_UNIT,
            OPEN_ORDERS_EMPTY,
            OPEN_ORDERS_ONE,
        },
        rest::{
            OpenOrder,
            SymbolInfo,
            dex_of,
            is_unknown_dex,
            match_universes,
            parse_open_orders,
            parse_positions,
        },
    };

    fn json(text: &str) -> serde_json::Value {
        serde_json::from_str(text).unwrap()
    }

    /// The reconciliation source. `oid` is mandatory — it is what an order is matched on —
    /// and the client id is a bonus that `openOrders` does not supply.
    #[test]
    fn open_orders_are_read_by_venue_id_with_the_client_id_optional() {
        assert!(parse_open_orders(&json(OPEN_ORDERS_EMPTY)).is_empty());

        assert_eq!(
            parse_open_orders(&json(OPEN_ORDERS_ONE)),
            vec![OpenOrder {
                coin: "BTC".into(),
                oid: 57118842019,
                cloid: Some("0xa1f0cddc17303d07f3fd128d5b35071d".into()),
            }]
        );

        // `openOrders` omits the client id entirely; the entry must still be usable.
        let without = json(
            r#"[{"coin":"BTC","limitPx":"31700.0","oid":7,"side":"B","sz":"0.001","timestamp":1}]"#,
        );
        assert_eq!(parse_open_orders(&without)[0].cloid, None);
        assert_eq!(parse_open_orders(&without)[0].oid, 7);

        // An entry the venue sends in a shape this backend cannot read is skipped, not
        // fatal: losing the whole list is what makes a connector expire live orders.
        let partly_broken = json(r#"[{"coin":"BTC"},{"coin":"ETH","oid":9}]"#);
        assert_eq!(parse_open_orders(&partly_broken).len(), 1);
        assert_eq!(parse_open_orders(&json("null")).len(), 0);
    }

    /// **An empty `assetPositions` means flat, not unknown.** A testnet account that has
    /// never touched a perp answers exactly that, verbatim; reading it as "no information"
    /// would leave the bot with no position at all.
    #[test]
    fn an_account_with_no_positions_reads_as_flat_not_as_unknown() {
        let (positions, time) = parse_positions(&json(CLEARINGHOUSE_STATE_FLAT));
        assert!(positions.is_empty());
        assert_eq!(time, 1785259956738);
    }

    /// `szi` is signed, so a short is a negative quantity and there is no side field to
    /// misread — which is exactly the field Bybit's backend has to `panic!` on.
    #[test]
    fn a_position_is_signed_and_needs_no_side_field() {
        let (positions, time) = parse_positions(&json(CLEARINGHOUSE_STATE_POSITIONS));
        assert_eq!(
            positions,
            vec![("BTC".to_string(), -0.00032), ("ETH".to_string(), 1.5)]
        );
        assert_eq!(time, 1785259956739);
    }

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
                asset_index: Some(0),
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
