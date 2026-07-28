//! Testnet smoke: sign, submit, follow, cancel — against the real venue.
//!
//! **Every test here is `#[ignore]` and refuses to run without
//! `HYPERLIQUID_CREDENTIALS` pointing at a credentials file, and refuses outright unless
//! the endpoint is testnet.** `cargo test` does not touch the network; this is run by hand:
//!
//! ```text
//! HYPERLIQUID_CREDENTIALS=~/.config/hftbacktest-connector/hl-testnet.toml \
//!   cargo test -p connector --bins hyperliquid::smoke -- --ignored --nocapture
//! ```
//!
//! It exists because the unit tests cannot answer the one question that matters most: the
//! golden vectors prove this code agrees with the official SDK, and only the venue can say
//! whether the *account* agrees — whether the API wallet is approved, and whether the asset
//! index, the price grid and the client-id format are what this network expects today.
//!
//! Nothing here can lift a position: the only order it places is a post-only bid at half
//! the market price, at the smallest size that clears the venue's minimum notional, and it
//! is cancelled immediately.

use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use hftbacktest::types::{OrdType, Order, Side, Status, TimeInForce};
use tokio::time::{Duration, Instant, timeout};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::hyperliquid::{
    CREDENTIALS_ENV,
    exchange::{CancelByCloidAction, ExchangeClient, OrderAction},
    load_credentials,
    ordermanager::OrderManager,
    rest::{SymbolInfo, agent_is_approved, open_orders, positions, resolve_symbols},
    signing::{NonceSource, Signer},
};

const WS_URL: &str = "wss://api.hyperliquid-testnet.xyz/ws";
const REST_URL: &str = "https://api.hyperliquid-testnet.xyz";
/// The venue refuses an order below this notional; the bid has to clear it to be a test.
const MIN_NOTIONAL_USD: f64 = 10.0;

/// The credentials, or `None` when the harness was not asked to run.
fn credentials() -> Option<(Signer, String)> {
    let path = std::env::var(CREDENTIALS_ENV).ok()?;
    // Belt and braces: this file signs real actions, so the endpoint is asserted, not
    // assumed. A credentials file for mainnet plus these URLs would sign for the wrong
    // network anyway and be refused by the venue, but the intent should be unambiguous.
    assert!(
        WS_URL.contains("testnet") && REST_URL.contains("testnet"),
        "the smoke harness is testnet-only"
    );
    let credentials = load_credentials(&path, None).expect("credentials");
    let signer = Signer::from_hex(&credentials.api_wallet_private_key, false).expect("signer");
    Some((
        signer,
        credentials.master_account_address.trim().to_lowercase(),
    ))
}

async fn mid(coin: &str) -> f64 {
    let client = reqwest::Client::new();
    let mids: serde_json::Value = client
        .post(format!("{REST_URL}/info"))
        .json(&serde_json::json!({ "type": "allMids" }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    mids[coin].as_str().unwrap().parse().unwrap()
}

async fn btc() -> SymbolInfo {
    let (resolved, rejected) = resolve_symbols(REST_URL, &["BTC".to_string()])
        .await
        .expect("resolve");
    assert!(rejected.is_empty(), "{rejected:?}");
    resolved.into_iter().next().unwrap()
}

/// The full round trip: resolve, sign, submit, watch `orderUpdates`, cancel.
///
/// Reports timings and the venue's own words for each step, so a failure says which of the
/// six signing footguns — or none of them — is responsible.
#[tokio::test]
#[ignore = "touches the Hyperliquid testnet; set HYPERLIQUID_CREDENTIALS to run"]
async fn a_post_only_bid_round_trips_on_testnet() {
    let Some((signer, account)) = credentials() else {
        eprintln!("SKIPPED: set {CREDENTIALS_ENV} to run the Hyperliquid smoke harness");
        return;
    };
    let agent = signer.address();
    eprintln!("agent   {agent}\naccount {account}");

    // 1. Agent approval, before anything is signed. This is the one check that separates
    //    "the signature is wrong" from "the wallet was never approved", which the venue
    //    reports identically.
    let approved = agent_is_approved(REST_URL, &account, &agent).await.unwrap();
    eprintln!("agent approved: {approved}");
    assert!(
        approved,
        "the API wallet is not an approved agent for {account}; approve it in the \
         Hyperliquid UI (API section) — it cannot be done with the agent key"
    );

    // 2. The universe, at run time. Testnet numbers its assets differently from mainnet.
    let info = btc().await;
    eprintln!(
        "BTC szDecimals={} asset_index={:?} tick={} lot={}",
        info.sz_decimals,
        info.asset_index,
        info.tick_size(),
        info.lot_size()
    );
    let asset_index = info.asset_index.expect("a canonical coin has an index");

    // 3. A bid at half the market, sized to clear the venue's minimum notional.
    let market = mid("BTC").await;
    let price = market * 0.5;
    let size = (MIN_NOTIONAL_USD / price * 1.2).max(info.lot_size());
    eprintln!("mid {market}, bidding {price} for {size}");

    // 4. Watch `orderUpdates` before submitting, so the ack cannot be missed.
    let (mut ws, _) = connect_async(WS_URL).await.unwrap();
    for kind in ["orderUpdates", "userFills"] {
        ws.send(Message::Text(
            serde_json::json!({
                "method": "subscribe",
                "subscription": { "type": kind, "user": account },
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
    }

    let mut manager = OrderManager::new("a1f0").unwrap();
    let mut order = Order::new(
        7,
        (price / info.tick_size()).round() as i64,
        info.tick_size(),
        size,
        Side::Buy,
        OrdType::Limit,
        TimeInForce::GTX,
    );
    order.status = Status::New;
    order.req = Status::New;
    let (cloid, wire) = manager
        .new_order("BTC", &info, asset_index, order)
        .expect("build the order");
    eprintln!(
        "cloid {cloid}\nwire  {}",
        serde_json::to_string(&wire).unwrap()
    );

    let exchange = ExchangeClient::new(REST_URL, Arc::new(signer), Arc::new(NonceSource::new()));
    let started = Instant::now();
    let submitted = exchange.post(&OrderAction::new(vec![wire])).await;
    eprintln!(
        "submit  {:>7.1} ms  {submitted:?}",
        started.elapsed().as_secs_f64() * 1000.0
    );

    let outcomes = match submitted {
        Ok(outcomes) => outcomes,
        Err(error) => {
            // The message the whole signing module exists to avoid. If it appears here the
            // fault is agent approval, not arithmetic — the check above already passed, so
            // say so rather than leaving it to be rediscovered.
            let text = error.to_string();
            assert!(
                !text.contains("does not exist"),
                "the venue does not recognise the API wallet even though extraAgents lists \
                 it: {text}"
            );
            panic!("the venue refused the order: {text}");
        }
    };
    let oid = match outcomes.first() {
        Some(crate::hyperliquid::exchange::ActionOutcome::Resting { oid, .. }) => *oid,
        other => panic!("the order did not rest: {other:?}"),
    };
    manager.note_venue_ack(&cloid, oid);
    eprintln!("resting oid {oid}");

    // 5. The ack on `orderUpdates`, which is the only authoritative confirmation.
    let ack = timeout(Duration::from_secs(15), async {
        while let Some(Ok(Message::Text(text))) = ws.next().await {
            if text.contains("orderUpdates") && text.contains(&cloid) {
                return text.to_string();
            }
        }
        String::new()
    })
    .await
    .expect("an orderUpdates ack within 15 s");
    eprintln!(
        "ack     {:>7.1} ms  {ack}",
        started.elapsed().as_secs_f64() * 1000.0
    );

    // 6. …and it is visible to the reconciliation path too.
    let open = open_orders(REST_URL, &account).await.unwrap();
    eprintln!("open orders: {open:?}");
    assert!(
        open.iter().any(|order| order.oid == oid),
        "the resting order must be visible to the reconciliation query"
    );

    // 7. Cancel, and wait for the venue to say so.
    let cancel_started = Instant::now();
    let cancelled = exchange
        .post(&CancelByCloidAction::new(vec![
            crate::hyperliquid::exchange::CancelByCloidWire {
                asset: asset_index,
                cloid: cloid.clone(),
            },
        ]))
        .await;
    eprintln!(
        "cancel  {:>7.1} ms  {cancelled:?}",
        cancel_started.elapsed().as_secs_f64() * 1000.0
    );
    assert_eq!(
        cancelled.unwrap(),
        vec![crate::hyperliquid::exchange::ActionOutcome::Success]
    );

    let done = timeout(Duration::from_secs(15), async {
        while let Some(Ok(Message::Text(text))) = ws.next().await {
            if text.contains("canceled") && text.contains(&cloid) {
                return text.to_string();
            }
        }
        String::new()
    })
    .await
    .expect("a cancellation on orderUpdates within 15 s");
    eprintln!(
        "done    {:>7.1} ms  {done}",
        cancel_started.elapsed().as_secs_f64() * 1000.0
    );

    // 8. Nothing left behind.
    let open = open_orders(REST_URL, &account).await.unwrap();
    assert!(
        !open.iter().any(|order| order.oid == oid),
        "the cancelled order is still listed: {open:?}"
    );
    eprintln!(
        "positions: {:?}",
        positions(REST_URL, &account).await.unwrap()
    );
}

/// Whether the venue really rejects a six-significant-figure price.
///
/// The documentation says integer prices are always legal; production experience on
/// `/exchange` says a six-digit price is refused, and `normalize` implements the strict
/// reading because it is the only one that cannot be silently wrong. This asks the venue
/// directly, with a price far below the market so nothing can fill either way.
#[tokio::test]
#[ignore = "touches the Hyperliquid testnet; set HYPERLIQUID_CREDENTIALS to run"]
async fn the_significant_figure_rule_is_what_the_venue_enforces() {
    let Some((signer, _account)) = credentials() else {
        eprintln!("SKIPPED: set {CREDENTIALS_ENV} to run the Hyperliquid smoke harness");
        return;
    };
    let info = btc().await;
    let asset_index = info.asset_index.unwrap();
    let exchange = ExchangeClient::new(REST_URL, Arc::new(signer), Arc::new(NonceSource::new()));

    // Six significant figures, one decimal — legal by `szDecimals`, illegal by the
    // five-figure rule. Far below any market, so a fill is impossible.
    for (label, price) in [("six figures", "12345.6"), ("five figures", "12345")] {
        let wire = crate::hyperliquid::exchange::OrderWire {
            a: asset_index,
            b: true,
            p: price.into(),
            s: "0.001".into(),
            r: false,
            t: crate::hyperliquid::exchange::OrderKind {
                limit: crate::hyperliquid::exchange::LimitKind { tif: "Alo" },
            },
            c: Some(format!("0xa1f0{:028x}", rand::random::<u128>() >> 16)),
        };
        let answer = exchange.post(&OrderAction::new(vec![wire])).await;
        eprintln!("{label:>13} {price:>9}  ->  {answer:?}");
    }
}
