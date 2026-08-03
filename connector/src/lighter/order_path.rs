//! The async order path that carries the tested cores (design note
//! [`docs/design-lighter-connector.md`](../../../docs/design-lighter-connector.md) §3.2–§3.4,
//! §4.11): the signer sidecar, one nonce-owning slot task, and the authenticated private
//! stream that confirms placements and states the position.
//!
//! **Opt-in, fail closed.** The backend arms this only when the config carries an
//! `[order_path]` section (`crate::lighter::Config::order_path`). Without it the connector is
//! Phase-1 market-data-only and refuses orders loudly — the safe default — because acceptance
//! on this venue is a private-channel fact that only a live round trip proves (§4.11), and
//! that round trip is the Fix+Testnet phase's gate, not this one's. So the module below is the
//! carriage; the correctness that matters rides in the unit-tested modules it calls
//! (`nonce`, `order`, `rest`, `slot`, `private_msg`), and its own async plumbing follows the
//! shape of the already-shipped `public_stream`.
//!
//! ## Shape
//!
//! * One **slot task** owns the [`SignerClient`] and the [`NonceOwner`] for the configured
//!   api-key slot (§3.3, one writer). It serialises submit/cancel/cancel-all, applies the
//!   nonce rule from the measured outcomes ([`crate::lighter::slot::nonce_action`]), and on a
//!   lapsed confirmation deadline asks `GET /tx` for the `event_info.ae` verdict and expires a
//!   rejected order rather than leaving the bot a phantom (§4.11). It also mints the WS auth
//!   token on request, so the private key lives in exactly one place.
//! * The **private stream** authenticates, subscribes `account_all_orders` /
//!   `account_all_positions` / `account_all_trades`, feeds each order and position frame into
//!   the shared [`OrderManager`], publishes the resulting `LiveEvent`s, and reconciles against
//!   the snapshot on every (re)connect (§3.4). The auth token is re-minted each connect and
//!   before its 8 h server ceiling ([`clamp_auth_lifetime`]).
//!
//! One api-key slot is provisioned, not two: the testnet account has a single key, so submit
//! and cancel share it for now. A dedicated cancel slot (§3.3) is a multi-key / mainnet
//! concern (§6.В.3) and is left to provisioning, not hard-coded here.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use hftbacktest::types::{ErrorKind, LiveError, LiveEvent, Order, Status};
use serde::Deserialize;
use tokio::{
    select,
    sync::{
        broadcast::{self, error::RecvError},
        mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
        oneshot,
    },
    task::JoinHandle,
    time::{self, Instant},
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Error as WsError, Message, client::IntoClientRequest},
};
use tracing::{error, info, warn};

use crate::{
    connector::PublishEvent,
    lighter::{
        LighterError,
        SharedSymbolSet,
        nonce::NonceOwner,
        order::{CancelPlan, OrderManager},
        private_msg::{AccountOrder, PrivateFrame, parse_private_frame},
        public_stream::{BACKOFF_MAX, BACKOFF_MIN, CONNECT_TIMEOUT, PING_FRAME, PING_INTERVAL},
        publish_error,
        rest::{self, MarketInfo, SendOutcome},
        signer::{CreateOrder, SignerClient, python_sidecar_command},
        slot::{
            AmbiguousFate,
            DeadlineAction,
            NonceAction,
            Placement,
            deadline_action,
            fate_of_ambiguous,
            nonce_action,
            placement_after_deadline,
        },
    },
    utils::{BackoffStrategy, ExponentialBackoff},
};

/// The 8 h server ceiling on an auth token (§3.4). Minting longer succeeds on the client and
/// is rejected `20013` by the server — a trap only a live GET catches — so the connector
/// clamps below it.
const AUTH_MAX_LIFETIME_S: i64 = 8 * 3600;
/// Re-mint this far before expiry, so a refresh never races the ceiling.
const AUTH_REFRESH_MARGIN_S: i64 = 30 * 60;
/// How far before a token's own expiry the stream proactively refreshes it (§3.4). The refresh
/// is a clean reconnect: the next connect mints a fresh token, verifies it with the cheap GET,
/// and re-subscribes — so a connection never outlives its token, which the one-shot mint of
/// the draft left it doing after ~7.5 h. A hot re-subscribe with the new token would avoid the
/// reconnect, but Phase 0 never measured whether an established subscription accepts one, so
/// the fail-closed choice is the reconnect, whose semantics are known. Consequence: under
/// [`ConnectPolicy::CancelAll`] the refresh re-applies the clean-slate sweep, like any
/// reconnect — expected under a policy whose contract is "cancel on every connect"; under the
/// default [`ConnectPolicy::Reconcile`] it merely re-adopts and is invisible to the bot.
const AUTH_REFRESH_LEAD_S: i64 = 30 * 60;

/// Default confirmation deadline: how long `account_all_orders` has to list a just-placed
/// order before the slot asks `GET /tx` why it has not (§4.11). Generous over the measured
/// ~200 ms channel appearance.
fn default_confirm_deadline_ms() -> u64 {
    5_000
}

/// How the order path treats orders already resting on the venue when it (re)connects (§3.4).
///
/// The account channels re-snapshot on every (re)connect, and on this venue orders outlive
/// both a socket drop and the death of the process (§2.7 — two PostOnly bids came back in a
/// fresh process's snapshot ~90 s after a `kill`). That snapshot is the reconciliation hook;
/// what to do with it is this policy. Modelled on `hyperliquid/private_stream.rs`'s
/// `ConnectPolicy`, and the default is the same — adopt, not cancel.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectPolicy {
    /// Adopt what the venue's snapshot lists as the truth, and only expire orders this
    /// connector believed were resting that the snapshot no longer lists
    /// ([`OrderManager::reconcile`]). **The default.**
    ///
    /// Cancelling on every connect turns a transient blip into a round trip through an empty
    /// book, and each `CancelAllOrders` spends the 60-`sendTx`/min Standard budget (§4.3) that
    /// only trading replenishes. So the safe default is to reconcile, exactly as the
    /// Hyperliquid backend chose.
    #[default]
    Reconcile,
    /// Cancel everything the connect-time snapshot lists, **then** reconcile — the clean-slate
    /// discipline of the snapshot-marker note (`AGENTS.md` §4.4) that `bybit` runs on its
    /// subscribe-ack. This is what makes a `myhft` restart free of duplicate orders: the
    /// connector guarantees the venue holds nothing it did not place this run.
    ///
    /// The sweep is built from the **snapshot** — the venue's own open-order list — not from
    /// this process's order map, which a freshly restarted connector's is empty (the §5 gate:
    /// "sweep cancels what the venue actually holds, built from the open-orders query, not from
    /// a local map"). Scope caveat (§3.4): `CancelAllOrders` on a market clears **every** order
    /// the *account* holds there, so the isolation unit is a subaccount per bot.
    CancelAll,
}

/// The `[order_path]` config. Present ⇒ the order path is armed.
#[derive(Deserialize, Clone, Debug)]
pub struct OrderPathConfig {
    /// This connector's account (a subaccount per bot is the scoping unit, §3.4).
    pub account_index: i64,
    /// The api-key slot to sign with. Must be ≥ 4: indices 0–3 are reserved for
    /// desktop/mobile (§3.3), and the connector must not reuse a default.
    pub api_key_index: u8,
    /// The signer sidecar: interpreter, script, and the sidecar's OWN key-config path. The key
    /// is read by the sidecar and never enters this process (§3.2).
    pub signer_python: String,
    pub signer_script: String,
    pub signer_config: String,
    #[serde(default = "default_confirm_deadline_ms")]
    pub confirm_deadline_ms: u64,
    /// What to do about orders already resting when the private stream (re)connects (§3.4).
    /// Defaults to [`ConnectPolicy::Reconcile`] — adopt the venue's truth. Set to `cancel_all`
    /// for the clean-slate discipline that makes a restart duplicate-free.
    #[serde(default)]
    pub connect_policy: ConnectPolicy,
}

/// Clamps a requested auth-token lifetime (seconds) below the 8 h server ceiling, minus the
/// refresh margin, and never below a minute (§3.4).
pub fn clamp_auth_lifetime(requested_s: i64) -> i64 {
    requested_s.clamp(60, AUTH_MAX_LIFETIME_S - AUTH_REFRESH_MARGIN_S)
}

/// How long after minting a token of `lifetime_s` seconds the stream should refresh it — a
/// clean reconnect, `AUTH_REFRESH_LEAD_S` before the token's own expiry (§3.4). Always
/// strictly less than the lifetime, so the refresh never races the expiry; for a short
/// lifetime that cannot afford the full lead, it refreshes at half-life instead.
pub fn auth_refresh_after(lifetime_s: i64) -> Duration {
    let after = if lifetime_s > AUTH_REFRESH_LEAD_S {
        lifetime_s - AUTH_REFRESH_LEAD_S
    } else {
        (lifetime_s / 2).max(1)
    };
    Duration::from_secs(after as u64)
}

/// The distinct market indices to cancel-all on (re)connect under `policy`, built from the
/// venue's own open-order snapshot (§3.4) — **never** from a local map, so it works on a
/// freshly restarted process whose map is empty and whose COIs it does not recognise (the §5
/// gate / §2.7 kill case).
///
/// `Reconcile` sweeps nothing (adopt, don't cancel). `CancelAll` returns each market the
/// snapshot lists an order on, deduplicated and sorted for determinism; an empty snapshot
/// sweeps nothing, so a connect against a flat account spends no nonce (§4.3).
pub fn markets_to_sweep_on_connect(policy: ConnectPolicy, snapshot: &[AccountOrder]) -> Vec<i32> {
    if policy != ConnectPolicy::CancelAll {
        return Vec::new();
    }
    let mut markets: Vec<i32> = snapshot
        .iter()
        .filter_map(|order| i32::try_from(order.market_index).ok())
        .collect();
    markets.sort_unstable();
    markets.dedup();
    markets
}

/// The three private-channel subscribe frames (§3.4): `account_all_orders` (auth **required** —
/// a subscribe with no token is refused `20001`), `account_all_positions`, `account_all_trades`.
///
/// The token is carried on all three even though only orders requires it: it is accepted on
/// the others (§3.4), and sending it uniformly is one shape rather than three. The channel is
/// spelled `name/{account}` on the way out and comes back `name:{account}` — the asymmetry the
/// whole backend is built around (§2.1), so the request spelling is pinned here.
pub fn subscription_frames(account_index: i64, token: &str) -> Vec<String> {
    [
        "account_all_orders",
        "account_all_positions",
        "account_all_trades",
    ]
    .iter()
    .map(|channel| {
        format!(r#"{{"type":"subscribe","channel":"{channel}/{account_index}","auth":"{token}"}}"#)
    })
    .collect()
}

/// A command to the slot task. Every variant that changes venue state carries the COI it acts
/// on, so the confirmation/rollback keys back onto the [`OrderManager`].
enum Command {
    Submit {
        coi: i64,
        order: Box<CreateOrder>,
    },
    Cancel {
        coi: i64,
        plan: CancelPlan,
    },
    /// Cancel-all for a market (`255` = all). `reply` fires once the sweep's `sendTx` has been
    /// answered, so an orderly stop can wait for it before the tasks are aborted.
    CancelAll {
        market_index: i32,
        reply: Option<oneshot::Sender<()>>,
    },
    /// Mint a WS auth token expiring at `deadline` (absolute unix seconds). The private stream
    /// asks the slot because the slot owns the one signer.
    MintAuth {
        deadline: i64,
        reply: oneshot::Sender<Option<String>>,
    },
}

/// The armed order path: the handle the [`crate::lighter::Lighter`] connector holds.
pub struct OrderPath {
    order_manager: Arc<Mutex<OrderManager>>,
    command_tx: UnboundedSender<Command>,
    tasks: Vec<JoinHandle<()>>,
}

impl OrderPath {
    /// The shared order manager — the connector returns this from `order_manager()`.
    pub fn order_manager(&self) -> Arc<Mutex<OrderManager>> {
        self.order_manager.clone()
    }

    /// Spawns the slot task and the private stream.
    ///
    /// `symbol_rx` is the registration wake-up (`Connector::register` broadcasts on it). The
    /// private stream re-resolves any newly registered symbol off it, so a symbol registered
    /// **after** the private stream connected is tracked for orders — without it, `market_info`
    /// stays `None` and every submit is expired before signing (the zero-fills bug). The shared
    /// symbol set stays authoritative for *what* is registered; the broadcast is only the
    /// impulse to re-read it, exactly as `public_stream` uses it (`AGENTS.md` §4.2).
    pub fn spawn(
        config: OrderPathConfig,
        rest_url: String,
        public_url: String,
        symbols: SharedSymbolSet,
        symbol_rx: broadcast::Receiver<String>,
        ev_tx: UnboundedSender<PublishEvent>,
    ) -> Self {
        // The COI allocator is seeded from a millisecond timestamp so a restart does not reuse
        // a previous run's still-live indices before reconciliation clears them (§3.5).
        let coi_seed = Utc::now().timestamp_millis().max(1);
        let order_manager = Arc::new(Mutex::new(OrderManager::new(coi_seed)));

        let (command_tx, command_rx) = unbounded_channel();
        let mut tasks = Vec::new();

        let slot = SlotTask {
            config: config.clone(),
            rest_url: rest_url.clone(),
            order_manager: order_manager.clone(),
            ev_tx: ev_tx.clone(),
        };
        tasks.push(tokio::spawn(slot.run(command_rx)));

        let stream = PrivateStreamTask {
            config: config.clone(),
            rest_url,
            public_url,
            symbols,
            symbol_rx,
            order_manager: order_manager.clone(),
            command_tx: command_tx.clone(),
            ev_tx,
        };
        tasks.push(tokio::spawn(stream.run()));

        Self {
            order_manager,
            command_tx,
            tasks,
        }
    }

    /// Registers a new order and queues it to be signed and sent. Acceptance is confirmed off
    /// the private channel, never here (§4.11).
    pub fn submit(&self, symbol: &str, order: &Order, ev_tx: &UnboundedSender<PublishEvent>) {
        let plan = {
            let mut manager = self.order_manager.lock().unwrap();
            match manager.market_info(symbol) {
                Some(market) => manager.new_order(symbol, &market, order),
                // The market has not resolved yet — the zero-fills bug's true cause. Report it
                // as its own error, NOT the reused `OrderNotFound` ("no live order …") message
                // that made this look like an order-id problem and that a downstream classifier
                // downgrades to benign (`OrderError::MarketUnresolved` doc, `order.rs`). The
                // registration wake arm resolves the symbol so this window is transient rather
                // than permanent.
                None => Err(crate::lighter::order::OrderError::MarketUnresolved {
                    symbol: symbol.to_string(),
                }),
            }
        };
        match plan {
            Ok((coi, wire)) => {
                let mut manager = self.order_manager.lock().unwrap();
                manager.mark_requested(coi, Status::New);
                drop(manager);
                if self
                    .command_tx
                    .send(Command::Submit {
                        coi,
                        order: Box::new(wire),
                    })
                    .is_err()
                {
                    self.fail_back(coi, symbol, ev_tx, "the order path is shutting down");
                }
            }
            Err(error) => expire_unsent(symbol, order, ev_tx, &error.to_string()),
        }
    }

    /// Queues a cancel for a live order. Errors (unknown/unconfirmed) are reported without
    /// touching the order — a cancel that cannot be addressed must not look like one that was.
    pub fn cancel(&self, symbol: &str, order: &Order, ev_tx: &UnboundedSender<PublishEvent>) {
        let plan = {
            let mut manager = self.order_manager.lock().unwrap();
            let plan = manager.cancel_order(symbol, order.order_id);
            if let Ok(ref plan) = plan {
                // The COI is what the slot rolls back on; look it up alongside the plan.
                if let Some(coi) = manager.coi_for(symbol, order.order_id) {
                    manager.mark_requested(coi, Status::Canceled);
                    Some((coi, plan.clone()))
                } else {
                    None
                }
            } else {
                report_error(
                    ev_tx,
                    ErrorKind::OrderError,
                    &plan.err().unwrap().to_string(),
                );
                None
            }
        };
        if let Some((coi, plan)) = plan
            && self.command_tx.send(Command::Cancel { coi, plan }).is_err()
        {
            report_error(
                ev_tx,
                ErrorKind::OrderError,
                "the order path is shutting down",
            );
            let _ = coi;
        }
    }

    /// Sweeps the given markets with one `CancelAllOrders` tx each (§3.4), and returns a task
    /// that completes once every sweep's `sendTx` has been answered — so an orderly stop can
    /// wait for the cancels to be sent before the tasks are torn down. Bounded, so a wedged
    /// slot cannot hang the stop.
    pub fn sweep(&self, market_indices: Vec<i32>) -> JoinHandle<()> {
        let command_tx = self.command_tx.clone();
        tokio::spawn(async move {
            let mut replies = Vec::new();
            for market_index in market_indices {
                let (reply_tx, reply_rx) = oneshot::channel();
                if command_tx
                    .send(Command::CancelAll {
                        market_index,
                        reply: Some(reply_tx),
                    })
                    .is_ok()
                {
                    replies.push(reply_rx);
                }
            }
            for reply in replies {
                // A dropped sender (slot gone) resolves the await immediately; the bound keeps
                // a slow one from hanging the stop.
                let _ = time::timeout(Duration::from_secs(20), reply).await;
            }
        })
    }

    /// Aborts the tasks for an orderly stop.
    pub fn shutdown(&mut self) {
        for task in self.tasks.drain(..) {
            task.abort();
        }
    }

    fn fail_back(
        &self,
        coi: i64,
        symbol: &str,
        ev_tx: &UnboundedSender<PublishEvent>,
        reason: &str,
    ) {
        let expired = self.order_manager.lock().unwrap().submit_failed(coi);
        if let Some((symbol, order)) = expired {
            publish_order(ev_tx, &symbol, order);
        }
        report_error(ev_tx, ErrorKind::OrderError, &format!("{symbol}: {reason}"));
    }
}

/// Owns the signer and the nonce for one slot, and serialises the venue traffic through them.
struct SlotTask {
    config: OrderPathConfig,
    rest_url: String,
    order_manager: Arc<Mutex<OrderManager>>,
    ev_tx: UnboundedSender<PublishEvent>,
}

/// A placement awaiting confirmation, and when its deadline is up (§4.11).
struct Awaiting {
    /// The `sendTx` tx hash for the `GET /tx` verdict (§4.11), or **empty** when the send was
    /// ambiguous / a transport error and produced no hash — then the deadline resolves the
    /// order's fate against `accountActiveOrders` on `market_index` instead (§3.3).
    tx_hash: String,
    /// The order's market, so the fate check can scope `accountActiveOrders` to it (§3.3).
    market_index: i32,
    deadline: Instant,
}

impl SlotTask {
    async fn run(self, mut command_rx: UnboundedReceiver<Command>) {
        let command = python_sidecar_command(
            &self.config.signer_python,
            &self.config.signer_script,
            &self.config.signer_config,
        );
        let mut signer = match SignerClient::spawn(command).await {
            Ok(signer) => signer,
            Err(error) => {
                // Fail closed: without the signer nothing can be sent. Say so loudly; the
                // supervisor restarts the connector.
                error!(
                    ?error,
                    "Couldn't start the Lighter signer sidecar; the order path is down."
                );
                report_error(
                    &self.ev_tx,
                    ErrorKind::CriticalConnectionError,
                    &format!("signer sidecar failed to start: {error}"),
                );
                return;
            }
        };

        let mut nonce = NonceOwner::new(self.config.api_key_index);
        self.reseed(&mut nonce).await;

        let mut awaiting: HashMap<i64, Awaiting> = HashMap::new();
        let mut deadline_tick = time::interval(Duration::from_millis(250));

        loop {
            select! {
                command = command_rx.recv() => {
                    match command {
                        Some(Command::Submit { coi, order }) => {
                            self.send_order(&mut signer, &mut nonce, coi, &order, &mut awaiting).await;
                        }
                        Some(Command::Cancel { coi, plan }) => {
                            self.send_cancel(&mut signer, &mut nonce, coi, &plan).await;
                        }
                        Some(Command::CancelAll { market_index, reply }) => {
                            self.send_cancel_all(&mut signer, &mut nonce, market_index).await;
                            if let Some(reply) = reply {
                                let _ = reply.send(());
                            }
                        }
                        Some(Command::MintAuth { deadline, reply }) => {
                            let token = signer
                                .auth_token(deadline, self.config.api_key_index)
                                .await
                                .map_err(|error| warn!(?error, "Couldn't mint a Lighter auth token."))
                                .ok();
                            let _ = reply.send(token);
                        }
                        None => break,
                    }
                }
                _ = deadline_tick.tick() => {
                    self.check_deadlines(&mut signer, &mut awaiting).await;
                }
            }
        }
        let _ = signer.shutdown().await;
    }

    /// Hard-refreshes the nonce from `GET /nextNonce` (§3.3). On failure the slot is left
    /// unseeded, so the next reserve refuses rather than guessing.
    async fn reseed(&self, nonce: &mut NonceOwner) {
        match rest::fetch_next_nonce(
            &self.rest_url,
            self.config.account_index,
            self.config.api_key_index,
        )
        .await
        {
            Ok(value) => nonce.seed(value),
            Err(error) => {
                nonce.invalidated();
                warn!(
                    ?error,
                    "Couldn't fetch the Lighter nextNonce; the slot is unseeded."
                );
            }
        }
    }

    async fn send_order(
        &self,
        signer: &mut SignerClient,
        nonce: &mut NonceOwner,
        coi: i64,
        order: &CreateOrder,
        awaiting: &mut HashMap<i64, Awaiting>,
    ) {
        let Some(reserved) = self.reserve(nonce).await else {
            self.expire(coi, "the nonce slot could not be seeded");
            return;
        };
        let signed = match signer
            .create_order(order, reserved, self.config.api_key_index)
            .await
        {
            Ok(signed) => signed,
            Err(error) => {
                nonce.released();
                self.expire(coi, &format!("the signer refused the order: {error}"));
                return;
            }
        };
        let market_index = order.market_index as i32;
        match rest::send_tx(&self.rest_url, signed.tx_type, &signed.tx_info).await {
            Ok(outcome) => {
                self.apply_nonce(nonce, &outcome).await;
                match outcome {
                    SendOutcome::Accepted(ack) => {
                        // 200 is not acceptance (§4.11): arm the confirmation deadline. The
                        // tx_hash routes the deadline to `GET /tx` for the `event_info.ae`
                        // verdict if `account_all_orders` has not listed the order by then.
                        self.arm_deadline(awaiting, coi, ack.tx_hash, market_index);
                    }
                    SendOutcome::InvalidNonce { .. } => {
                        // `21104`: the sequencer rejected THIS tx on nonce grounds, so no order
                        // was created for this COI, and `apply_nonce` has already resynced the
                        // counter. Nothing rests on the venue, so expiring cannot orphan.
                        self.expire(
                            coi,
                            "the venue rejected the nonce (21104); the order did not take",
                        );
                    }
                    SendOutcome::Ambiguous { status, detail } => {
                        // Unknown fate — the tx MAY already rest on the venue (§4.11). §3.3: the
                        // nonce is hard-refreshed (`apply_nonce` → Resync above), and the order
                        // is KEPT TRACKED with the deadline armed but NO tx_hash, so the
                        // deadline reads its fate off `accountActiveOrders` before the bot is
                        // told anything. NEVER expire here: that orphans a resting order into a
                        // silent one-sided quote (`AGENTS.md` §1.1) — the bug this path fixes.
                        warn!(
                            coi,
                            status,
                            %detail,
                            "An ambiguous Lighter sendTx; keeping the order tracked and \
                             resolving its fate against accountActiveOrders at the deadline (§3.3)."
                        );
                        self.arm_deadline(awaiting, coi, String::new(), market_index);
                    }
                }
            }
            Err(error) => {
                // A transport error (e.g. the 10 s ORDER_REST_TIMEOUT firing after the sequencer
                // has already processed the tx) is the same unknown fate as Ambiguous. Hard-
                // refresh the nonce and keep the order tracked for the fate check — do not
                // orphan a possibly-resting order (§3.3, §1.1).
                nonce.invalidated();
                self.reseed(nonce).await;
                warn!(
                    coi,
                    %error,
                    "A Lighter sendTx transport error; keeping the order tracked and resolving \
                     its fate against accountActiveOrders at the deadline (§3.3)."
                );
                self.arm_deadline(awaiting, coi, String::new(), market_index);
            }
        }
    }

    /// Arms the confirmation deadline for a placement (§4.11, §3.3). A non-empty `tx_hash` (a
    /// clean HTTP 200) routes the lapsed deadline to `GET /tx`; an empty one (an ambiguous or
    /// transport-failed send that was kept tracked rather than orphaned) routes it to the
    /// `accountActiveOrders` fate check on `market_index`.
    fn arm_deadline(
        &self,
        awaiting: &mut HashMap<i64, Awaiting>,
        coi: i64,
        tx_hash: String,
        market_index: i32,
    ) {
        awaiting.insert(
            coi,
            Awaiting {
                tx_hash,
                market_index,
                deadline: Instant::now() + Duration::from_millis(self.config.confirm_deadline_ms),
            },
        );
    }

    /// Mints a short-lived auth token and asks `accountActiveOrders` for the account's open
    /// orders on `market_index` — the authoritative list the slot reads an ambiguous send's
    /// fate off (§3.3). Any failure (the auth mint, the token, the GET) is an `Err` the caller
    /// reads as "could not check" and fails closed on; it is never an empty list.
    async fn check_order_fate(
        &self,
        signer: &mut SignerClient,
        market_index: i32,
    ) -> Result<Vec<AccountOrder>, LighterError> {
        // A short deadline: this token signs exactly one authorized GET, right now.
        let deadline = Utc::now().timestamp() + 60;
        let token = signer
            .auth_token(deadline, self.config.api_key_index)
            .await
            .map_err(|error| {
                LighterError::Frame(format!(
                    "couldn't mint an auth token for the order fate check: {error}"
                ))
            })?;
        rest::fetch_active_orders(
            &self.rest_url,
            self.config.account_index,
            market_index,
            &token,
        )
        .await
    }

    /// A confirmation deadline lapsed on an order whose send was **ambiguous** (no tx_hash):
    /// read its fate off `accountActiveOrders` (§3.3). Landed ⇒ adopt it exactly as the private
    /// channel would (the bot sees its resting order; no re-send, no duplicate — the §5 gate);
    /// absent from a list the venue positively returned ⇒ expire it back to the bot; a check
    /// that could not run ⇒ keep waiting (re-arm), because expiring on a failed check would
    /// orphan a possibly-resting order (§1.1).
    async fn resolve_ambiguous_fate(
        &self,
        signer: &mut SignerClient,
        coi: i64,
        now: Instant,
        awaiting: &mut HashMap<i64, Awaiting>,
    ) {
        let Some(market_index) = awaiting.get(&coi).map(|entry| entry.market_index) else {
            return;
        };
        let active = self.check_order_fate(signer, market_index).await;
        let fate = fate_of_ambiguous(coi, active.as_ref().map(Vec::as_slice).map_err(|_| ()));
        match fate {
            AmbiguousFate::Landed => {
                awaiting.remove(&coi);
                let order = active
                    .into_iter()
                    .flatten()
                    .find(|order| order.client_order_index == coi);
                if let Some(order) = order {
                    let applied = self
                        .order_manager
                        .lock()
                        .unwrap()
                        .apply_order_update(&order);
                    if let Some((symbol, published)) = applied {
                        warn!(
                            coi,
                            %symbol,
                            "An ambiguous Lighter send DID land; adopting the resting order from \
                             accountActiveOrders — no re-send, no duplicate (§3.3, §5 gate)."
                        );
                        publish_order(&self.ev_tx, &symbol, published);
                    }
                }
            }
            AmbiguousFate::DidNotLand => {
                awaiting.remove(&coi);
                self.expire(
                    coi,
                    "the ambiguous sendTx never produced an order (absent from \
                     accountActiveOrders, §3.3)",
                );
            }
            AmbiguousFate::Unknown => {
                if let Some(entry) = awaiting.get_mut(&coi) {
                    entry.deadline = now + Duration::from_millis(self.config.confirm_deadline_ms);
                }
                warn!(
                    coi,
                    "Couldn't check a Lighter order's fate after an ambiguous send; keeping it \
                     tracked and retrying at the next deadline (§3.3, fail closed)."
                );
            }
        }
    }

    async fn send_cancel(
        &self,
        signer: &mut SignerClient,
        nonce: &mut NonceOwner,
        coi: i64,
        plan: &CancelPlan,
    ) {
        let Some(reserved) = self.reserve(nonce).await else {
            self.clear_cancel_request(coi, "the nonce slot could not be seeded");
            return;
        };
        let market_index = plan.market_index as i16;
        let signed = match signer
            .cancel_order(
                market_index,
                plan.order_index,
                reserved,
                self.config.api_key_index,
            )
            .await
        {
            Ok(signed) => signed,
            Err(error) => {
                nonce.released();
                self.clear_cancel_request(coi, &format!("the signer refused the cancel: {error}"));
                return;
            }
        };
        match rest::send_tx(&self.rest_url, signed.tx_type, &signed.tx_info).await {
            Ok(outcome) => self.apply_nonce(nonce, &outcome).await,
            Err(error) => {
                nonce.invalidated();
                self.reseed(nonce).await;
                self.clear_cancel_request(coi, &format!("the cancel sendTx failed: {error}"));
            }
        }
        // The confirmed cancel arrives on the private channel as `status: canceled`; nothing
        // here reports it, so a refused transport does not look like a cancel that took.
    }

    async fn send_cancel_all(
        &self,
        signer: &mut SignerClient,
        nonce: &mut NonceOwner,
        market_index: i32,
    ) {
        let Some(reserved) = self.reserve(nonce).await else {
            warn!("Couldn't seed the nonce for a Lighter cancel-all sweep.");
            return;
        };
        // CANCEL_ALL_TIF_IMMEDIATE requires time_ms = 0, or the .so refuses on the client (§3.4).
        let signed = match signer
            .cancel_all(0, 0, market_index, reserved, self.config.api_key_index)
            .await
        {
            Ok(signed) => signed,
            Err(error) => {
                nonce.released();
                warn!(?error, "The signer refused the Lighter cancel-all.");
                return;
            }
        };
        match rest::send_tx(&self.rest_url, signed.tx_type, &signed.tx_info).await {
            Ok(outcome) => {
                self.apply_nonce(nonce, &outcome).await;
                info!(market_index, "Sent a Lighter CancelAllOrders sweep.");
            }
            Err(error) => {
                nonce.invalidated();
                self.reseed(nonce).await;
                warn!(?error, "The Lighter cancel-all sendTx failed.");
            }
        }
    }

    /// Reserves a nonce, seeding first if the slot is unseeded. `None` if it cannot be seeded.
    async fn reserve(&self, nonce: &mut NonceOwner) -> Option<i64> {
        if !nonce.is_seeded() {
            self.reseed(nonce).await;
        }
        nonce.reserve().ok()
    }

    async fn apply_nonce(&self, nonce: &mut NonceOwner, outcome: &SendOutcome) {
        match nonce_action(outcome) {
            NonceAction::Consumed => nonce.consumed(),
            NonceAction::Resync => {
                nonce.invalidated();
                self.reseed(nonce).await;
            }
        }
    }

    /// On a lapsed confirmation deadline, settle each waiting order (§4.11, §3.3). A confirmed
    /// order (or one no longer tracked) is dropped; an unconfirmed one is split by
    /// [`deadline_action`] on whether its send produced a `tx_hash`: with one, ask `GET /tx` for
    /// the `event_info.ae` verdict (§4.11); with none — an ambiguous / transport-failed send
    /// kept tracked — read its fate off `accountActiveOrders` (§3.3). Either way an order is
    /// only ever expired on a positive verdict, never on silence, so a resting order is never
    /// orphaned (§1.1).
    async fn check_deadlines(
        &self,
        signer: &mut SignerClient,
        awaiting: &mut HashMap<i64, Awaiting>,
    ) {
        let now = Instant::now();
        let due: Vec<i64> = awaiting
            .iter()
            .filter(|(_, a)| a.deadline <= now)
            .map(|(coi, _)| *coi)
            .collect();
        for coi in due {
            let confirmed = self.order_manager.lock().unwrap().is_confirmed(coi);
            let has_tx_hash = awaiting
                .get(&coi)
                .is_some_and(|entry| !entry.tx_hash.is_empty());
            match deadline_action(confirmed, has_tx_hash) {
                DeadlineAction::Drop => {
                    // Confirmed (published already) or no longer tracked: stop waiting.
                    awaiting.remove(&coi);
                }
                DeadlineAction::ViaTxVerdict => {
                    let Some(entry) = awaiting.get(&coi) else {
                        continue;
                    };
                    let verdict = rest::fetch_tx_verdict(&self.rest_url, &entry.tx_hash)
                        .await
                        .unwrap_or(rest::TxVerdict::Pending);
                    match placement_after_deadline(false, &verdict) {
                        Placement::Rejected { code, message } => {
                            awaiting.remove(&coi);
                            self.expire(
                                coi,
                                &format!(
                                    "the venue rejected the order (event_info.ae {code}): {message}"
                                ),
                            );
                        }
                        Placement::Unresolved => {
                            // Give it one more interval; the channel may still be catching up.
                            if let Some(entry) = awaiting.get_mut(&coi) {
                                entry.deadline =
                                    now + Duration::from_millis(self.config.confirm_deadline_ms);
                            }
                            warn!(
                                coi,
                                "A Lighter placement is unconfirmed past its deadline; still waiting (§4.11)."
                            );
                        }
                        Placement::Confirmed => {
                            awaiting.remove(&coi);
                        }
                    }
                }
                DeadlineAction::ViaFateCheck => {
                    self.resolve_ambiguous_fate(signer, coi, now, awaiting)
                        .await;
                }
            }
        }
    }

    fn expire(&self, coi: i64, reason: &str) {
        let expired = self.order_manager.lock().unwrap().submit_failed(coi);
        if let Some((symbol, order)) = expired {
            warn!(coi, reason, %symbol, "Expiring a Lighter order back to the bot (§4.11).");
            publish_order(&self.ev_tx, &symbol, order);
            report_error(
                &self.ev_tx,
                ErrorKind::OrderError,
                &format!("{symbol}: {reason}"),
            );
        }
    }

    fn clear_cancel_request(&self, coi: i64, reason: &str) {
        let republished = self.order_manager.lock().unwrap().cancel_failed(coi);
        if let Some((symbol, order)) = republished {
            warn!(coi, reason, %symbol, "A Lighter cancel did not send; leaving the order live.");
            publish_order(&self.ev_tx, &symbol, order);
            report_error(
                &self.ev_tx,
                ErrorKind::OrderError,
                &format!("{symbol}: {reason}"),
            );
        }
    }
}

/// Why [`PrivateStreamTask::connect`] returned. A token refresh is a clean, expected cycle,
/// **not** a disconnection: the run loop reconnects immediately, resets the backoff and
/// publishes no `ConnectionInterrupted`, so an 8 h-ceiling refresh (§3.4) never looks to a bot
/// like a lost connection.
enum StreamEnd {
    /// The auth token is nearing its expiry; cycle cleanly to mint a fresh one (§3.4).
    TokenRefresh,
}

/// Authenticates and pumps the private account channels into the order manager.
struct PrivateStreamTask {
    config: OrderPathConfig,
    rest_url: String,
    public_url: String,
    symbols: SharedSymbolSet,
    /// The registration wake-up. Re-read on every connect and on every wake so a symbol
    /// registered after this stream connected is resolved for orders (`AGENTS.md` §4.2). The
    /// receiver lives across reconnects: the shared set is authoritative, so a wake missed
    /// while disconnected is recovered by the connect-time re-resolve, and a `Lagged` is a
    /// re-resolve, not a lost symbol.
    symbol_rx: broadcast::Receiver<String>,
    order_manager: Arc<Mutex<OrderManager>>,
    command_tx: UnboundedSender<Command>,
    ev_tx: UnboundedSender<PublishEvent>,
}

impl PrivateStreamTask {
    async fn run(mut self) {
        let mut backoff = ExponentialBackoff::with_bounds(BACKOFF_MIN, BACKOFF_MAX);
        loop {
            match self.connect().await {
                Ok(StreamEnd::TokenRefresh) => {
                    // A proactive token refresh, not a fault: reconnect at once, no error to
                    // the bots, and the backoff starts fresh after ~7 h of a healthy socket.
                    info!(
                        "Refreshing the Lighter private-stream auth token with a clean \
                         reconnect (§3.4)."
                    );
                    backoff = ExponentialBackoff::with_bounds(BACKOFF_MIN, BACKOFF_MAX);
                }
                Err(error) => {
                    error!(?error, "The Lighter private stream disconnected.");
                    report_error(
                        &self.ev_tx,
                        ErrorKind::ConnectionInterrupted,
                        &error.to_string(),
                    );
                    let delay = backoff.backoff();
                    info!(?delay, "Reconnecting to the Lighter private stream.");
                    time::sleep(delay).await;
                }
            }
        }
    }

    /// Mints a WS auth token via the slot's signer, returning it with the lifetime it was
    /// minted for (so the caller can schedule the refresh, §3.4).
    async fn mint_auth(&self) -> Option<(String, i64)> {
        let lifetime = clamp_auth_lifetime(AUTH_MAX_LIFETIME_S);
        let deadline = Utc::now().timestamp() + lifetime;
        let (reply_tx, reply_rx) = oneshot::channel();
        self.command_tx
            .send(Command::MintAuth {
                deadline,
                reply: reply_tx,
            })
            .ok()?;
        let token = reply_rx.await.ok().flatten()?;
        Some((token, lifetime))
    }

    async fn connect(&mut self) -> Result<StreamEnd, LighterError> {
        // The market↔symbol map the position frames and every submit need, resolved per connect
        // (§3.4). Only the not-yet-tracked symbols are asked about, so a reconnect does not
        // re-fetch a `market_id` that cannot change under a running process; a refusal is
        // reported per symbol and re-tried on the next wake, rather than dropped (the fold-1
        // fix — a REST blip at connect used to reject 100 % of orders for the ~7.5 h life of
        // the connection with no error at all).
        self.resolve_registered().await;

        let (token, lifetime_s) = self
            .mint_auth()
            .await
            .ok_or_else(|| LighterError::ConnectionAbort("no auth token".to_string()))?;

        // Verify the fresh token with one cheap authorized GET before switching to it (§3.4).
        // The 8 h ceiling is server-only: a bad deadline (a clamp bug, a skewed clock) mints
        // without error and is rejected `20013` only at use, which otherwise looks like a
        // creds problem and a book subscribed to nothing. A 401 is loud and fatal here; a
        // transport hiccup on the check is not, and does not discard the token — the WS
        // subscribe is the next test either way.
        match rest::verify_auth_token(&self.rest_url, self.config.account_index, &token).await {
            Ok(rest::AuthCheck::Valid) => {}
            Ok(rest::AuthCheck::Invalid { code, message }) => {
                return Err(LighterError::ConnectionAbort(format!(
                    "the freshly minted Lighter auth token was rejected by the server \
                     ({code}: {message}); check the clock and the 8 h clamp (§3.4)"
                )));
            }
            Ok(rest::AuthCheck::Unknown { status, detail }) => {
                warn!(
                    status,
                    %detail,
                    "Couldn't verify the Lighter auth token before use; proceeding, the \
                     subscribe will test it (§3.4)."
                );
            }
            Err(error) => {
                warn!(
                    ?error,
                    "The Lighter auth-token check didn't complete; proceeding, the subscribe \
                     will test it (§3.4)."
                );
            }
        }

        let request = self.public_url.as_str().into_client_request()?;
        let (ws_stream, _) = time::timeout(CONNECT_TIMEOUT, connect_async(request))
            .await
            .map_err(|_| LighterError::ConnectTimeout(CONNECT_TIMEOUT))??;
        info!(url = %self.public_url, "Connected to the Lighter private stream.");
        let (mut write, mut read) = ws_stream.split();

        for frame in subscription_frames(self.config.account_index, &token) {
            write.send(Message::Text(frame.into())).await?;
        }

        self.serve(&mut write, &mut read, lifetime_s).await
    }

    /// One connection: pump the account channels, keep it alive, refresh the token before it
    /// expires — and re-resolve any symbol registered after connect off the wake-up.
    ///
    /// Split from [`Self::connect`] so the registration wake arm — the fix for the zero-fills
    /// bug — is driven by the tests below with the halves in hand, rather than only modelled.
    /// Modelled on `public_stream::serve`. No socket drop on a wake: the account channels are
    /// account-scoped, not per-symbol, so a newly registered symbol needs only its market
    /// resolved, not a resubscribe.
    pub(crate) async fn serve<S, R>(
        &mut self,
        write: &mut S,
        read: &mut R,
        lifetime_s: i64,
    ) -> Result<StreamEnd, LighterError>
    where
        S: SinkExt<Message> + Unpin,
        LighterError: From<S::Error>,
        R: StreamExt<Item = Result<Message, WsError>> + Unpin,
    {
        let mut ping = time::interval(PING_INTERVAL);
        // Refresh the token by cycling cleanly before it expires — a connection must never
        // outlive its token (§3.4). Fires once, ~`AUTH_REFRESH_LEAD_S` before expiry.
        let refresh_at = auth_refresh_after(lifetime_s);
        let refresh = time::sleep(refresh_at);
        tokio::pin!(refresh);
        // A closed broadcast resolves instantly and for ever; stop polling the arm once it has,
        // or the loop spins at full tilt for the life of the connection (public_stream §4.2).
        let mut registrations_closed = false;
        loop {
            select! {
                _ = ping.tick() => {
                    write.send(Message::Text(PING_FRAME.into())).await?;
                }
                _ = &mut refresh => {
                    return Ok(StreamEnd::TokenRefresh);
                }
                registered = self.symbol_rx.recv(), if !registrations_closed => {
                    match registered {
                        Ok(symbol) => {
                            // A symbol registered after this stream connected: resolve its
                            // market so submits can address it. Without this arm `market_info`
                            // stays `None` and every submit is expired before signing — the
                            // zero-fills bug (§4.2, mirrors `public_stream`'s wake).
                            info!(%symbol, "A Lighter symbol was registered; resolving its market for the order path.");
                            self.resolve_registered().await;
                        }
                        Err(RecvError::Lagged(missed)) => {
                            // A missed wake never loses a symbol: the shared set is authoritative
                            // and re-read here (§4.2).
                            warn!(missed, "Lighter order-path registration wake-ups were dropped; re-resolving the registered set.");
                            self.resolve_registered().await;
                        }
                        Err(RecvError::Closed) => registrations_closed = true,
                    }
                }
                message = read.next() => {
                    match message {
                        Some(Ok(Message::Text(text))) => self.on_frame(&text),
                        Some(Ok(Message::Ping(_))) => {
                            write.send(Message::Pong(Default::default())).await?;
                        }
                        Some(Ok(Message::Close(frame))) => {
                            return Err(LighterError::ConnectionAbort(
                                frame.map(|f| f.to_string()).unwrap_or_default(),
                            ));
                        }
                        Some(Ok(_)) => {}
                        Some(Err(error)) => return Err(LighterError::Tungstenite(Box::new(error))),
                        None => return Err(LighterError::ConnectionInterrupted),
                    }
                }
            }
        }
    }

    /// Resolves every registered symbol not yet tracked and records the catalog rows, so submits
    /// can address them and position frames map back to the registered name (§3.4). Refusals are
    /// reported per symbol, not dropped (fold-1); a transiently unavailable catalog leaves the
    /// symbol untracked, so the next wake re-resolves it rather than writing it off for the life
    /// of the connection. Called at connect and on every registration wake.
    async fn resolve_registered(&self) {
        let unresolved: Vec<String> = {
            let registered: Vec<String> = self.symbols.lock().unwrap().iter().cloned().collect();
            let manager = self.order_manager.lock().unwrap();
            registered
                .into_iter()
                .filter(|symbol| manager.market_info(symbol).is_none())
                .collect()
        };
        if unresolved.is_empty() {
            return;
        }
        let (resolved, refused) = rest::resolve_symbols(&self.rest_url, &unresolved).await;
        self.apply_resolution(resolved, refused);
    }

    /// Tracks what resolved and reports what did not, per symbol (fold-1, modelled on
    /// `public_stream::apply_resolution`). A refused symbol is surfaced as a
    /// [`ErrorKind::CriticalConnectionError`] — a symbol that resolves to no market can neither
    /// be served nor traded, the same class of failure the market-data side reports — and is
    /// left untracked so the next wake asks again. Split from the round trip so it is testable
    /// without a socket.
    fn apply_resolution(&self, resolved: Vec<MarketInfo>, refused: Vec<(String, LighterError)>) {
        {
            let mut manager = self.order_manager.lock().unwrap();
            for info in resolved {
                info!(
                    symbol = %info.symbol,
                    market_id = info.market_id,
                    "Tracked a Lighter market for the order path."
                );
                manager.track_market(info);
            }
        }
        for (symbol, error) in &refused {
            error!(
                %symbol,
                ?error,
                "Refusing to track a Lighter symbol for the order path: it does not resolve to a \
                 market. Every submit for it is expired until it resolves; the next registration \
                 wake retries a transient catalog failure."
            );
            publish_error(&self.ev_tx, ErrorKind::CriticalConnectionError, error);
        }
    }

    fn on_frame(&self, text: &str) {
        let frame = match parse_private_frame(text) {
            Ok(frame) => frame,
            Err(error) => {
                warn!(?error, "A malformed Lighter private frame was skipped.");
                return;
            }
        };
        match frame {
            PrivateFrame::Orders { snapshot, orders } => {
                if snapshot {
                    // Policy first, then truth (mirrors `hyperliquid/private_stream.rs`). Under
                    // `cancel_all` the clean-slate sweep is built from THIS snapshot — the
                    // venue's own open-order list — so it cancels a previous incarnation's
                    // surviving orders even though a freshly restarted process's map is empty
                    // and does not recognise their COIs (§3.4, §5 gate / §2.7 kill case). The
                    // confirmations arrive as `canceled` deltas on this same channel.
                    for market_index in
                        markets_to_sweep_on_connect(self.config.connect_policy, &orders)
                    {
                        info!(
                            market_index,
                            "Clean-slate sweep of a Lighter market the connect snapshot lists \
                             (connect_policy = cancel_all, §3.4)."
                        );
                        let _ = self.command_tx.send(Command::CancelAll {
                            market_index,
                            reply: None,
                        });
                    }
                    // Reconnect reconciliation: expire our orders the venue no longer lists,
                    // then adopt the ones it does (§3.4, §5.10).
                    let expired = self.order_manager.lock().unwrap().reconcile(&orders);
                    for (symbol, order) in expired {
                        publish_order(&self.ev_tx, &symbol, order);
                    }
                }
                for order in &orders {
                    let applied = self.order_manager.lock().unwrap().apply_order_update(order);
                    if let Some((symbol, published)) = applied {
                        publish_order(&self.ev_tx, &symbol, published);
                    }
                }
            }
            PrivateFrame::Positions { positions, .. } => {
                let now = Utc::now().timestamp_micros();
                for position in &positions {
                    let mapped = self
                        .order_manager
                        .lock()
                        .unwrap()
                        .apply_position(position, now);
                    if let Some((symbol, qty, exch_ts)) = mapped {
                        let _ = self
                            .ev_tx
                            .send(PublishEvent::LiveEvent(LiveEvent::Position {
                                symbol,
                                qty,
                                exch_ts,
                            }));
                    }
                }
            }
            PrivateFrame::Trades { count, .. } if count > 0 => {
                // The delta body is unmeasured (§6.Б.3); fills are read off account_all_orders'
                // filled_base_amount instead. Log that one arrived so the live phase can
                // capture the shape.
                info!(
                    count,
                    "A Lighter account_all_trades delta arrived (shape unmeasured, §6.Б.3)."
                );
            }
            PrivateFrame::VenueError { code, message } => {
                warn!(code, %message, "The Lighter private stream returned an error frame.");
            }
            _ => {}
        }
    }
}

fn publish_order(ev_tx: &UnboundedSender<PublishEvent>, symbol: &str, order: Order) {
    let _ = ev_tx.send(PublishEvent::LiveEvent(LiveEvent::Order {
        symbol: symbol.to_string(),
        order,
    }));
}

fn report_error(ev_tx: &UnboundedSender<PublishEvent>, kind: ErrorKind, message: &str) {
    let value = LighterError::Frame(message.to_string()).to_value();
    let _ = ev_tx.send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
        kind, value,
    ))));
}

/// Expires an order that never reached the venue back to the bot, then reports the error —
/// order first, so the bot's state is clean before an error handler can abort (as the Phase-1
/// `reject_order` does).
fn expire_unsent(symbol: &str, order: &Order, ev_tx: &UnboundedSender<PublishEvent>, reason: &str) {
    let mut order = order.clone();
    order.req = Status::None;
    order.status = Status::Expired;
    publish_order(ev_tx, symbol, order);
    report_error(ev_tx, ErrorKind::OrderError, &format!("{symbol}: {reason}"));
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use hftbacktest::types::{ErrorKind, LiveEvent};
    use serde::Deserialize;
    use tokio::{
        sync::{broadcast, mpsc::unbounded_channel},
        time,
    };

    use super::{
        AUTH_MAX_LIFETIME_S,
        AUTH_REFRESH_MARGIN_S,
        Command,
        ConnectPolicy,
        OrderPathConfig,
        PrivateStreamTask,
        auth_refresh_after,
        clamp_auth_lifetime,
        markets_to_sweep_on_connect,
        subscription_frames,
    };
    use crate::{
        connector::PublishEvent,
        lighter::{
            LighterError,
            fixtures::{
                PRIVATE_ORDERS_SNAPSHOT,
                PRIVATE_ORDERS_SNAPSHOT_EMPTY,
                PRIVATE_POSITIONS_UPDATE_FLAT,
            },
            order::OrderManager,
            private_msg::AccountOrder,
            rest::MarketInfo,
        },
        utils::testing::{RecordingSink, read_after_connect},
    };

    /// **The 8 h auth ceiling is server-only (§3.4): a 9 h token mints without error and is
    /// rejected `20013` at use.** The connector clamps below the ceiling minus a refresh
    /// margin so a refresh never races it — and never below a minute.
    #[test]
    fn an_auth_lifetime_is_clamped_below_the_eight_hour_ceiling() {
        assert_eq!(
            clamp_auth_lifetime(9 * 3600),
            AUTH_MAX_LIFETIME_S - AUTH_REFRESH_MARGIN_S,
            "a 9h request is clamped below the ceiling"
        );
        assert_eq!(
            clamp_auth_lifetime(3600),
            3600,
            "a 1h request is left alone"
        );
        assert_eq!(clamp_auth_lifetime(0), 60, "never below a minute");
    }

    /// **A token is refreshed strictly before it expires (§3.4).** The draft minted once and
    /// let the connection outlive the token; the refresh reconnects ahead of expiry — by the
    /// full lead for a normal (clamped ~7.5 h) lifetime, and at half-life for a lifetime too
    /// short to afford the lead. Never at or after expiry.
    #[test]
    fn a_token_is_refreshed_strictly_before_it_expires() {
        let lifetime = clamp_auth_lifetime(AUTH_MAX_LIFETIME_S);
        let after = auth_refresh_after(lifetime).as_secs() as i64;
        assert!(
            after < lifetime,
            "refresh {after}s must precede expiry {lifetime}s"
        );
        assert_eq!(
            after,
            lifetime - super::AUTH_REFRESH_LEAD_S,
            "full lead for a long token"
        );

        // A short lifetime that cannot afford the full lead refreshes at half-life, still
        // strictly before expiry and never zero.
        let short = auth_refresh_after(60).as_secs() as i64;
        assert!(
            short > 0 && short < 60,
            "half-life for a short token: {short}"
        );
    }

    fn order_on(market_index: i64, coi: i64) -> AccountOrder {
        AccountOrder {
            market_index,
            client_order_index: coi,
            order_index: 844424914280000 + coi,
            is_ask: false,
            status: "open".to_string(),
            price: 58300.0,
            initial_base_amount: 0.001,
            remaining_base_amount: 0.001,
            filled_base_amount: 0.0,
            transaction_time_us: 1785431774184833,
        }
    }

    /// **`Reconcile` (the default) sweeps nothing on connect** (§3.4): it adopts the venue's
    /// snapshot rather than cancelling it. So even a snapshot full of resting orders yields no
    /// cancels — the safe default that does not spend the Standard `sendTx` budget on a blip.
    #[test]
    fn reconcile_sweeps_nothing_on_connect() {
        let snapshot = vec![order_on(1, 111), order_on(24, 222)];
        assert!(markets_to_sweep_on_connect(ConnectPolicy::Reconcile, &snapshot).is_empty());
    }

    /// **`CancelAll` sweeps exactly the markets the connect snapshot lists, from the snapshot
    /// itself** (§3.4 clean-slate, §5 gate). This is the `kill -9` case: the orders carry COIs
    /// this process never minted (a previous incarnation's), and the sweep must still cancel
    /// them — which it does because it is built from the venue's snapshot, not from a local map
    /// that a restarted process's is empty. Distinct and sorted.
    #[test]
    fn cancel_all_sweeps_the_markets_the_snapshot_lists_even_for_foreign_orders() {
        // A restarted process's view: three surviving orders across two markets, all with
        // COIs this process did not mint.
        let snapshot = vec![
            order_on(24, 999_001),
            order_on(1, 999_002),
            order_on(1, 999_003),
        ];
        let markets = markets_to_sweep_on_connect(ConnectPolicy::CancelAll, &snapshot);
        assert_eq!(markets, vec![1, 24], "distinct market indices, sorted");
    }

    /// **`CancelAll` against a flat account sweeps nothing** (§4.3): an empty snapshot means
    /// the venue holds nothing, so no `CancelAllOrders` is sent and no nonce is spent.
    #[test]
    fn cancel_all_sweeps_nothing_when_the_snapshot_is_empty() {
        assert!(markets_to_sweep_on_connect(ConnectPolicy::CancelAll, &[]).is_empty());
    }

    /// **The subscribe frames carry the auth token and the account-scoped channels** (§3.4).
    /// `account_all_orders` is refused `20001` without a token, so the token must be present;
    /// the channel is asked for as `name/{account}` (the request spelling, §2.1).
    #[test]
    fn the_subscribe_frames_carry_auth_and_the_account_scoped_channels() {
        let frames = subscription_frames(516, "1785460345:516:5:deadbeef");
        assert_eq!(frames.len(), 3);
        for (channel, frame) in [
            "account_all_orders",
            "account_all_positions",
            "account_all_trades",
        ]
        .iter()
        .zip(&frames)
        {
            let value: serde_json::Value = serde_json::from_str(frame).unwrap();
            assert_eq!(value["type"], "subscribe");
            assert_eq!(value["channel"], format!("{channel}/516"));
            assert_eq!(
                value["auth"], "1785460345:516:5:deadbeef",
                "every private subscribe carries the token (§3.4)"
            );
        }
    }

    #[derive(Deserialize)]
    struct PolicyHolder {
        #[serde(default)]
        connect_policy: ConnectPolicy,
    }

    /// **The default connect policy reconciles rather than cancels** (§3.4), and `cancel_all`
    /// is the opt-in clean-slate — the same default and spelling as the Hyperliquid backend.
    #[test]
    fn the_default_connect_policy_reconciles_and_cancel_all_is_opt_in() {
        assert_eq!(ConnectPolicy::default(), ConnectPolicy::Reconcile);
        let absent: PolicyHolder = toml::from_str("").unwrap();
        assert_eq!(absent.connect_policy, ConnectPolicy::Reconcile);
        let chosen: PolicyHolder = toml::from_str(r#"connect_policy = "cancel_all""#).unwrap();
        assert_eq!(chosen.connect_policy, ConnectPolicy::CancelAll);
    }

    /// Builds a stream task with a fresh (empty) order manager and the given policy — the
    /// restarted-process state — returning the command and publish receivers to inspect.
    fn stream_task(
        policy: ConnectPolicy,
    ) -> (
        PrivateStreamTask,
        tokio::sync::mpsc::UnboundedReceiver<Command>,
        tokio::sync::mpsc::UnboundedReceiver<PublishEvent>,
    ) {
        let (command_tx, command_rx) = unbounded_channel();
        let (ev_tx, ev_rx) = unbounded_channel();
        let config = OrderPathConfig {
            account_index: 516,
            api_key_index: 5,
            signer_python: String::new(),
            signer_script: String::new(),
            signer_config: String::new(),
            confirm_deadline_ms: 5_000,
            connect_policy: policy,
        };
        // The wake-up receiver: the on_frame tests here never send on it, and the tx is leaked
        // so the receiver never sees the channel close (mirrors `public_stream`'s test helper).
        let (symbol_tx, symbol_rx) = broadcast::channel(16);
        std::mem::forget(symbol_tx);
        let task = PrivateStreamTask {
            config,
            rest_url: String::new(),
            public_url: String::new(),
            symbols: Arc::new(Mutex::new(Default::default())),
            symbol_rx,
            order_manager: Arc::new(Mutex::new(OrderManager::new(1))),
            command_tx,
            ev_tx,
        };
        (task, command_rx, ev_rx)
    }

    /// **The §5 gate, socketless: after a `kill -9` and restart, the clean-slate sweep cancels
    /// what the venue actually holds — built from the connect snapshot, not a local map.**
    ///
    /// The order manager is empty (a fresh process) and the snapshot is a real testnet frame
    /// for an order whose COI this process never minted (a previous incarnation's, §2.7). Under
    /// `cancel_all` the connect handler must still emit a `CancelAllOrders` for that market;
    /// the reconcile that follows cannot help, because the map is empty and the COI is foreign.
    #[test]
    fn cancel_all_sweeps_a_previous_runs_surviving_order_from_the_connect_snapshot() {
        let (task, mut command_rx, _ev_rx) = stream_task(ConnectPolicy::CancelAll);
        task.on_frame(PRIVATE_ORDERS_SNAPSHOT);
        match command_rx.try_recv() {
            Ok(Command::CancelAll {
                market_index,
                reply,
            }) => {
                assert_eq!(market_index, 1, "the market the snapshot lists (BTC)");
                assert!(reply.is_none(), "the connect sweep is fire-and-forget");
            }
            _ => panic!("cancel_all must sweep the market the connect snapshot holds an order on"),
        }
        assert!(
            command_rx.try_recv().is_err(),
            "exactly one CancelAll for the one market"
        );
    }

    /// **`reconcile` (the default) never cancels on connect** (§3.4): the same snapshot yields
    /// no `CancelAllOrders`. Cancelling on every connect would spend the Standard `sendTx`
    /// budget on a blip — the reason the safe default adopts rather than sweeps.
    #[test]
    fn reconcile_never_sweeps_on_connect() {
        let (task, mut command_rx, _ev_rx) = stream_task(ConnectPolicy::Reconcile);
        task.on_frame(PRIVATE_ORDERS_SNAPSHOT);
        assert!(
            command_rx.try_recv().is_err(),
            "reconcile adopts the venue's orders; it never cancels them"
        );
    }

    /// **A flat connect snapshot sweeps nothing even under `cancel_all`** (§4.3): there is
    /// nothing resting, so no `CancelAllOrders` is sent and no nonce is spent.
    #[test]
    fn cancel_all_against_a_flat_snapshot_sends_no_sweep() {
        let (task, mut command_rx, _ev_rx) = stream_task(ConnectPolicy::CancelAll);
        task.on_frame(PRIVATE_ORDERS_SNAPSHOT_EMPTY);
        assert!(
            command_rx.try_recv().is_err(),
            "an empty snapshot means the venue holds nothing to sweep"
        );
    }

    /// **The position is fetched from the private channel on (re)connect, and a flat one is
    /// still reported** (§3.4). The `account_all_positions` snapshot arrives on every connect;
    /// `on_frame` maps it onto the registered symbol and publishes a `LiveEvent::Position` —
    /// even at zero, because "flat" and "nobody told me yet" must be distinguishable to the
    /// bot's startup gate (snapshot-marker note). Never inferred from fills.
    #[test]
    fn a_positions_frame_is_published_as_a_live_position_even_when_flat() {
        let (task, _command_rx, mut ev_rx) = stream_task(ConnectPolicy::Reconcile);
        // The connect resolves the catalog; here, track BTC (market 1) so the position frame
        // (which carries only the market index) maps back onto the registered symbol.
        task.order_manager.lock().unwrap().track_market(MarketInfo {
            symbol: "BTC".to_string(),
            market_id: 1,
            price_decimals: 1,
            size_decimals: 5,
        });
        task.on_frame(PRIVATE_POSITIONS_UPDATE_FLAT);
        match ev_rx.try_recv() {
            Ok(PublishEvent::LiveEvent(LiveEvent::Position { symbol, qty, .. })) => {
                assert_eq!(symbol, "BTC");
                assert_eq!(qty, 0.0, "a flat position is reported, not swallowed");
            }
            _ => panic!("the positions snapshot must publish a LiveEvent::Position (§3.4)"),
        }
    }

    /// **fold-1, socketless: what resolves is tracked, and what does not is reported per
    /// symbol.** The old code did `let (resolved, _refused) = …` and dropped every refusal,
    /// so a REST blip at connect rejected 100 % of orders for the ~7.5 h life of the
    /// connection with no error at all. Modelled on `public_stream`'s
    /// `an_unresolvable_symbol_is_refused_by_name`. A refused symbol is left untracked (so the
    /// next wake asks again) and surfaced as a `CriticalConnectionError`.
    #[test]
    fn apply_resolution_tracks_resolved_markets_and_reports_refused_ones() {
        let (task, _command_rx, mut ev_rx) = stream_task(ConnectPolicy::Reconcile);

        task.apply_resolution(
            vec![MarketInfo {
                symbol: "ETH".to_string(),
                market_id: 42,
                price_decimals: 2,
                size_decimals: 4,
            }],
            vec![(
                "NOTACOIN".to_string(),
                LighterError::UnknownSymbol("NOTACOIN is not listed on Lighter".into()),
            )],
        );

        assert!(
            task.order_manager
                .lock()
                .unwrap()
                .market_info("ETH")
                .is_some(),
            "the resolved market must be tracked so submits can address it"
        );
        assert!(
            task.order_manager
                .lock()
                .unwrap()
                .market_info("NOTACOIN")
                .is_none(),
            "a refused symbol is left untracked, so the next wake re-resolves it"
        );

        let mut errors = Vec::new();
        while let Ok(event) = ev_rx.try_recv() {
            if let PublishEvent::LiveEvent(LiveEvent::Error(error)) = event {
                errors.push(error);
            }
        }
        assert_eq!(errors.len(), 1, "the one refusal is reported, not dropped");
        assert_eq!(
            errors[0].kind,
            ErrorKind::CriticalConnectionError,
            "a symbol that resolves to no market can neither be served nor traded"
        );
    }

    /// A local HTTP endpoint that answers `GET /api/v1/orderBooks` with `body` — the catalog
    /// the resolve fetches — so the wake arm can be driven end to end without the internet.
    /// Same shape the `public_stream` catalog tests use (a bound `TcpListener`), but this one
    /// returns a body rather than holding the socket open.
    async fn catalog_server(body: &'static str) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                // Read the request line and headers (a GET has no body), then answer.
                let mut buf = [0u8; 1024];
                let _ = socket.read(&mut buf).await;
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: \
                     {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            }
        });
        format!("http://{address}")
    }

    /// **The zero-fills bug, socketless: a symbol registered AFTER the private stream connected
    /// is resolved for orders by the wake arm.**
    ///
    /// The order manager starts empty (the post-connect state) and no market is tracked. The
    /// bot then registers `ETH` — the shared set gains it and a wake-up is broadcast, exactly
    /// as `Connector::register` does. Driving `serve` (with the halves in hand, the way
    /// `public_stream`'s reconnect pin drives it) must resolve `ETH` off the wake and track it,
    /// so `market_info` goes `None` → `Some` and a submit would no longer be expired before
    /// signing. Delete the wake arm and the market is never tracked — the mutation this pins.
    #[tokio::test]
    async fn an_order_path_tracks_a_symbol_registered_after_connect() {
        // `reqwest` builds its rustls backend when the client is built; a test has no `main` to
        // install the process-level provider. Idempotent (`AGENTS.md` §4.7).
        crate::install_crypto_provider();
        let rest_url = catalog_server(
            r#"{"order_books":[{"symbol":"ETH","market_id":42,"status":"active","supported_price_decimals":2,"supported_size_decimals":4}]}"#,
        )
        .await;

        let (command_tx, _command_rx) = unbounded_channel();
        let (ev_tx, _ev_rx) = unbounded_channel();
        let (symbol_tx, symbol_rx) = broadcast::channel(16);
        let symbols: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let order_manager = Arc::new(Mutex::new(OrderManager::new(1)));
        let config = OrderPathConfig {
            account_index: 516,
            api_key_index: 5,
            signer_python: String::new(),
            signer_script: String::new(),
            signer_config: String::new(),
            confirm_deadline_ms: 5_000,
            connect_policy: ConnectPolicy::Reconcile,
        };
        let mut task = PrivateStreamTask {
            config,
            rest_url,
            public_url: String::new(),
            symbols: symbols.clone(),
            symbol_rx,
            order_manager: order_manager.clone(),
            command_tx,
            ev_tx,
        };

        // Post-connect state: ETH is not registered yet, so nothing is tracked.
        assert!(
            order_manager.lock().unwrap().market_info("ETH").is_none(),
            "precondition: ETH is not tracked before it is registered"
        );

        // The bot registers ETH after connect: the shared set gains it and the wake fires.
        symbols.lock().unwrap().insert("ETH".to_string());
        symbol_tx.send("ETH".to_string()).unwrap();

        // Drive the connection loop. The read half never speaks and never ends, so the loop is
        // driven only by its own arms — the wake arm resolves ETH against the local catalog.
        // A large lifetime keeps the token refresh far out of the window. Real time, so the
        // local round trip runs; bounded so the (otherwise endless) loop is stopped.
        let mut sink = RecordingSink::<LighterError>::default();
        let mut read = read_after_connect(|| {});
        let _ = time::timeout(
            Duration::from_secs(2),
            task.serve(&mut sink, &mut read, AUTH_MAX_LIFETIME_S),
        )
        .await;

        assert!(
            order_manager.lock().unwrap().market_info("ETH").is_some(),
            "a symbol registered after connect must be resolved and tracked by the wake arm — \
             without it every submit for ETH is expired before signing (the zero-fills bug)"
        );
    }
}
