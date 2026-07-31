//! The catalog: `GET /api/v1/orderBooks`, which is what turns a symbol into an address.
//!
//! On this venue resolution is not validation that could be skipped — **`market_id` is the
//! addressing** (design note §3.5). A symbol that does not resolve has nothing to subscribe
//! to, and a subscription to an id nobody meant is answered `Invalid Channel` *without
//! closing the socket*, so the mistake looks like an instrument that is simply quiet.
//!
//! Matching is **exact, case included**, and what comes back is the string the caller asked
//! about: a resolve that answered under a different spelling would address the market under a
//! name no bot registered, and `connector/src/main.rs` looks the bot's depth up by the name
//! the bot registered — so the events would be dropped on the floor with nothing said. See
//! [`MarketInfo::symbol`].
//!
//! The same round trip carries the price and size decimals, which are the tick and lot the
//! venue quantises to. They are **logged, not enforced**: the bot chooses the tick and lot
//! it registers (`connector/src/main.rs` builds the fused depth from `RegisterInstrument`),
//! and nothing in the `Connector` trait lets this backend see or correct them —
//! `Connector::register` carries a symbol and nothing else (`connector/src/connector.rs:63`).
//! A bot whose lot is coarser than the venue's loses every level below it *before* it can be
//! published, which no amount of resending fixes — measured on Hyperliquid at 3 events in 10.
//! **The log is the whole defence**: the resolve prints both numbers per symbol and
//! `connector/examples/lighter.toml` says to copy them. Nothing downstream can check that the
//! bot did — in particular [`crate::lighter::depth::MirrorCounts`] cannot, because the mirror
//! is built from these same catalog decimals and so compares the venue only against its own
//! grid (design note §3.5 claims otherwise; it is wrong, and that module says why).

use std::time::Duration;

use serde::Deserialize;
use tracing::info;

use crate::lighter::LighterError;

/// How long the catalog round trip may take.
///
/// Bounded because `reqwest` has no default timeout and this runs inside the stream's read
/// loop: an unbounded fetch would hold the keepalive as well, and the venue drops a
/// connection that has not written for two minutes.
pub const CATALOG_TIMEOUT: Duration = Duration::from_secs(15);

/// What the catalog says about one market.
#[derive(Clone, Debug, PartialEq)]
pub struct MarketInfo {
    /// **The string the caller asked about**, which is also the catalog's, because the match
    /// is exact ([`resolve_one`]).
    ///
    /// Load-bearing, and it is the one field with no venue meaning: every frame is routed by
    /// [`Self::market_id`], but this is what the mirror, the subscription tracker and every
    /// `LiveEvent::Feed` are keyed by — and `connector/src/main.rs` keys the bot's fused
    /// depth by the string the **bot** registered. A resolve that answers under any other
    /// spelling produces a symbol whose events reach nobody, silently.
    pub symbol: String,
    /// The address. Every subscription and every frame is keyed by this.
    pub market_id: i64,
    /// `supported_price_decimals`: the venue's price grid is `10^-price_decimals`.
    pub price_decimals: i64,
    /// `supported_size_decimals`: the venue's size grid is `10^-size_decimals`.
    pub size_decimals: i64,
}

impl MarketInfo {
    /// The finest price increment the venue quotes on — what the bot should register as its
    /// `tick_size`.
    pub fn tick_size(&self) -> f64 {
        10f64.powi(-(self.price_decimals as i32))
    }

    /// The finest size increment the venue will quote — the bot's `lot_size`.
    pub fn lot_size(&self) -> f64 {
        10f64.powi(-(self.size_decimals as i32))
    }
}

#[derive(Deserialize)]
struct Catalog {
    #[serde(default)]
    order_books: Vec<Row>,
}

#[derive(Deserialize)]
struct Row {
    symbol: String,
    market_id: i64,
    #[serde(default)]
    status: String,
    #[serde(default)]
    supported_price_decimals: i64,
    #[serde(default)]
    supported_size_decimals: i64,
}

/// What a resolve produced: the markets that resolved, and per symbol the reason one did not.
pub type Resolution = (Vec<MarketInfo>, Vec<(String, LighterError)>);

/// Matches requested symbols against an already-fetched catalog.
///
/// Split from the HTTP so the interesting half is testable without a network. Returns what
/// resolved and, **per symbol**, what did not: an unknown symbol takes only itself down.
/// All-or-nothing is the shape Hyperliquid had to roll back — one bot registering a typo
/// gave an endless reconnect loop with no market data for anybody (design note §3.5).
pub fn match_catalog(symbols: &[String], catalog: &str) -> Result<Resolution, LighterError> {
    let catalog: Catalog = serde_json::from_str(catalog)?;
    if catalog.order_books.is_empty() {
        return Err(LighterError::CatalogUnavailable(
            "the catalog carries no `order_books`".to_string(),
        ));
    }

    let mut resolved = Vec::new();
    let mut refused = Vec::new();
    for symbol in symbols {
        match resolve_one(symbol, &catalog.order_books) {
            Ok(info) => resolved.push(info),
            Err(error) => refused.push((symbol.clone(), error)),
        }
    }
    Ok((resolved, refused))
}

/// A symbol with the punctuation the venue's spot names carry taken out, so `SKY/USDC` and
/// `SKYUSDC` compare equal.
fn strip_separators(symbol: &str) -> String {
    symbol
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect()
}

fn resolve_one(symbol: &str, rows: &[Row]) -> Result<MarketInfo, LighterError> {
    // **Exact, case included** — see the invariant on [`MarketInfo::symbol`]. A case-folded
    // match resolved and then addressed the market under a name no bot had registered, which
    // is a symbol with no data, no error and no end: `main.rs` looks the bot's fused depth up
    // by the string the bot registered, so every event published under the venue's spelling
    // was dropped on the floor by `handle_ev`.
    let Some(row) = rows.iter().find(|r| r.symbol == symbol) else {
        // A spelling that differs only in case is the one near miss worth its own sentence:
        // it is the mistake `examples/lighter.toml` used to invite, and the fix is one
        // keystroke rather than a different instrument.
        if let Some(row) = rows.iter().find(|r| r.symbol.eq_ignore_ascii_case(symbol)) {
            return Err(LighterError::UnknownSymbol(format!(
                "{symbol} is not how Lighter spells it: the venue's own spelling is {}, and \
                 the match is exact. Every event is keyed by the symbol the bot registered, \
                 so a spelling the venue does not use is a book no bot ever sees",
                row.symbol
            )));
        }
        // Point at the likely mistake rather than only refusing. The common one is a quote
        // suffix carried over from another venue: Lighter perps are bare base assets, and
        // its spot markets are the ones that carry a quote.
        let wanted = symbol.to_uppercase();
        let near: Vec<&str> = rows
            .iter()
            .map(|r| r.symbol.as_str())
            .filter(|listed| {
                let listed = listed.to_uppercase();
                // `SKYUSDC` for `SKY/USDC` — the separator is the whole difference — and
                // `CRVUSDT` for `CRV`, a perp name carried over from a venue that quotes it.
                strip_separators(&listed) == strip_separators(&wanted)
                    || (wanted.starts_with(&listed) && wanted != listed)
            })
            .take(3)
            .collect();
        return Err(LighterError::UnknownSymbol(if near.is_empty() {
            format!("{symbol} is not listed on Lighter")
        } else {
            format!(
                "{symbol} is not listed on Lighter; did you mean {}?",
                near.join(", ")
            )
        }));
    };
    if row.status != "active" {
        // A listed-but-dead market is subscribable and silent: 18 of mainnet's 228 were
        // `inactive` when this was written. Subscribing to one is a symbol with no data on a
        // healthy connection, which is the failure this whole module exists to prevent.
        return Err(LighterError::UnknownSymbol(format!(
            "{} is {} on Lighter, not active; it would subscribe and never produce data",
            row.symbol, row.status
        )));
    }
    Ok(MarketInfo {
        // The **caller's** string, which the exact match above makes byte-identical to
        // `row.symbol`. Written this way so the invariant holds by construction: whatever a
        // later matcher does, what comes back is what was asked about, because that is what
        // every caller keys its own maps by.
        symbol: symbol.to_string(),
        market_id: row.market_id,
        price_decimals: row.supported_price_decimals,
        size_decimals: row.supported_size_decimals,
    })
}

/// Fetches the catalog and resolves `symbols` against it.
///
/// A transport failure refuses **every** symbol with [`LighterError::CatalogUnavailable`],
/// which is deliberately not a listing verdict: the caller leaves those symbols pending and
/// asks again, rather than writing them off for the life of the connection.
pub async fn resolve_symbols(rest_url: &str, symbols: &[String]) -> Resolution {
    match fetch_catalog(rest_url).await {
        Ok(catalog) => match match_catalog(symbols, &catalog) {
            Ok((resolved, refused)) => {
                for info in &resolved {
                    // Logged per resolve, because these two numbers are what the bot has to
                    // register and nothing in the trait lets this backend check that it did.
                    info!(
                        symbol = %info.symbol,
                        market_id = info.market_id,
                        tick_size = info.tick_size(),
                        lot_size = info.lot_size(),
                        price_decimals = info.price_decimals,
                        size_decimals = info.size_decimals,
                        "Resolved a Lighter instrument. Register these tick and lot sizes; a \
                         coarser one silently drops levels the bot then never sees."
                    );
                }
                (resolved, refused)
            }
            Err(error) => refuse_all(symbols, &error.to_string()),
        },
        Err(error) => refuse_all(symbols, &error.to_string()),
    }
}

fn refuse_all(symbols: &[String], reason: &str) -> Resolution {
    (
        Vec::new(),
        symbols
            .iter()
            .map(|symbol| {
                (
                    symbol.clone(),
                    LighterError::CatalogUnavailable(format!(
                        "couldn't read the Lighter catalog for {symbol}: {reason}"
                    )),
                )
            })
            .collect(),
    )
}

async fn fetch_catalog(rest_url: &str) -> Result<String, LighterError> {
    Ok(reqwest::Client::builder()
        .timeout(CATALOG_TIMEOUT)
        .build()?
        .get(format!("{rest_url}/api/v1/orderBooks"))
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?)
}

#[cfg(test)]
mod tests {
    use crate::lighter::{
        LighterError,
        fixtures::ORDER_BOOKS_CATALOG,
        rest::{MarketInfo, match_catalog},
    };

    fn wanted(symbols: &[&str]) -> Vec<String> {
        symbols.iter().map(|s| s.to_string()).collect()
    }

    /// The catalog is the addressing: a symbol resolves to an integer, and to the decimals
    /// that are the venue's tick and lot. Real rows, fetched from mainnet.
    #[test]
    fn a_symbol_resolves_to_a_market_id_and_the_venues_grid() {
        let (resolved, refused) =
            match_catalog(&wanted(&["CRV", "ETH"]), ORDER_BOOKS_CATALOG).unwrap();

        assert!(refused.is_empty(), "{refused:?}");
        assert_eq!(
            resolved[0],
            MarketInfo {
                symbol: "CRV".to_string(),
                market_id: 36,
                price_decimals: 5,
                size_decimals: 1,
            }
        );
        assert_eq!(resolved[0].tick_size(), 1e-5);
        assert_eq!(resolved[0].lot_size(), 0.1);

        // ETH's grid is a different shape entirely, which is why neither number may be a
        // constant: 2 price decimals and 4 size decimals.
        assert_eq!(resolved[1].market_id, 0);
        assert_eq!(resolved[1].tick_size(), 0.01);
        assert_eq!(resolved[1].lot_size(), 1e-4);
    }

    /// **The refusal is per symbol, not all-or-nothing** (§3.5). Hyperliquid had to roll
    /// back the opposite: one bot registering a typo gave an endless reconnect loop with no
    /// market data for anyone. Here the unknown symbol is named and refused, and everything
    /// beside it still resolves.
    #[test]
    fn an_unknown_symbol_is_refused_by_name_and_the_rest_still_resolve() {
        let (resolved, refused) =
            match_catalog(&wanted(&["CRV", "NOTACOIN", "HYPE"]), ORDER_BOOKS_CATALOG).unwrap();

        assert_eq!(
            resolved
                .iter()
                .map(|i| i.symbol.as_str())
                .collect::<Vec<_>>(),
            ["CRV", "HYPE"]
        );
        assert_eq!(refused.len(), 1);
        assert_eq!(refused[0].0, "NOTACOIN");
        assert!(
            refused[0].1.to_string().contains("NOTACOIN"),
            "{:?}",
            refused[0].1
        );
        // A listing verdict: it will not change within a connection, so the caller may
        // remember it rather than asking on every wake-up.
        assert!(refused[0].1.is_listing_verdict());
    }

    /// The standard mistake is a quote suffix carried over from another venue. Lighter perps
    /// are bare base assets and its *spot* markets are the ones that carry a quote, so the
    /// refusal points at the near miss instead of only saying no.
    #[test]
    fn a_symbol_spelled_for_another_venue_is_told_what_it_probably_meant() {
        let (_, refused) = match_catalog(&wanted(&["SKYUSDC"]), ORDER_BOOKS_CATALOG).unwrap();
        assert!(
            refused[0].1.to_string().contains("SKY/USDC"),
            "{:?}",
            refused[0].1
        );
    }

    /// **A listed-but-inactive market is the worst answer to accept.** It subscribes without
    /// complaint and produces nothing for ever, which is exactly the failure this venue
    /// makes easiest to reach — 18 of mainnet's 228 markets were inactive when this was
    /// written.
    #[test]
    fn an_inactive_market_is_refused_rather_than_subscribed_to_silence() {
        let (resolved, refused) =
            match_catalog(&wanted(&["HYUNDAI"]), ORDER_BOOKS_CATALOG).unwrap();

        assert!(resolved.is_empty());
        let message = refused[0].1.to_string();
        assert!(message.contains("inactive"), "{message}");
        assert!(message.contains("never produce data"), "{message}");
    }

    /// **A symbol whose case differs from the catalog is refused, not quietly corrected.**
    ///
    /// Case-folding it and keeping the *venue's* spelling was a silent blocker. Everything
    /// downstream is keyed by the resolved string — the mirror, `SubscriptionTracker::mark`,
    /// the `instruments` cache, every `LiveEvent::Feed` — while `main.rs` keys the bot's
    /// fused depth by the string the **bot** registered (`handle_ev`'s `depth.get_mut(symbol)`
    /// returns `vec![]` on a miss). Register `crv` against a catalog that says `CRV` and the
    /// two never met: no event reached any bot, the symbol was never marked subscribed so the
    /// 228-row catalog was re-fetched every housekeeping tick for ever, and nothing was
    /// reported — while `main.rs` still published `SnapshotComplete` over the empty book.
    #[test]
    fn a_symbol_whose_case_differs_from_the_catalog_is_refused_by_name() {
        let (resolved, refused) =
            match_catalog(&wanted(&["crv", "HYPE"]), ORDER_BOOKS_CATALOG).unwrap();

        assert_eq!(
            resolved
                .iter()
                .map(|i| i.symbol.as_str())
                .collect::<Vec<_>>(),
            ["HYPE"],
            "everything beside it still resolves"
        );
        assert_eq!(refused[0].0, "crv");
        let message = refused[0].1.to_string();
        assert!(message.contains("crv"), "{message}");
        assert!(
            message.contains("CRV"),
            "the refusal must name the venue's spelling: {message}"
        );
        // A verdict: reported once and never asked about again on this connection.
        assert!(refused[0].1.is_listing_verdict());
    }

    /// The invariant the blocker above broke, stated as one: a resolved symbol is the string
    /// the **caller** asked about, byte for byte, and every symbol gets exactly one verdict.
    ///
    /// `PublicStream::subscribe_pending` caches by the requested spelling and looks the cache
    /// up by the same, so a resolve that answered under any other name is a symbol that is
    /// silently never subscribed and never reported.
    #[test]
    fn every_resolved_symbol_is_the_string_the_caller_asked_about() {
        let asked = wanted(&["HYPE", "eth", "SKYUSDC", "NOTACOIN"]);
        let (resolved, refused) = match_catalog(&asked, ORDER_BOOKS_CATALOG).unwrap();

        for info in &resolved {
            assert!(
                asked.contains(&info.symbol),
                "{info:?} was never asked about"
            );
        }
        assert_eq!(
            resolved.len() + refused.len(),
            asked.len(),
            "one verdict per symbol, and no symbol left without one"
        );
    }

    /// A catalog that is not one must not read as "no symbol exists": that is a transport
    /// failure, and treating it as a listing verdict would write every symbol off for the
    /// life of the connection.
    #[test]
    fn an_unreadable_catalog_is_not_a_verdict_about_any_symbol() {
        assert!(match_catalog(&wanted(&["CRV"]), "not json").is_err());

        let Err(error) = match_catalog(&wanted(&["CRV"]), r#"{"code":200}"#) else {
            panic!("an empty catalog must not resolve anything");
        };
        assert!(matches!(error, LighterError::CatalogUnavailable(_)));
        assert!(!error.is_listing_verdict());
    }
}
