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
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
    // `Instant` unqualified in this file is `tokio::time::Instant` (the confirmation
    // deadlines); the outbound stopwatch is plain monotonic time, so it is spelled out.
    time::{Duration, Instant as StdInstant},
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
use tracing::{debug, error, info, warn};

use crate::{
    connector::{ApplyOutcome, PublishEvent, SweepOutcome},
    lighter::{
        LighterError,
        SharedSymbolSet,
        gate::{Admission, LogThrottle, Refusal, SendGate, TxBudget, TxClass, admit_by},
        nonce::NonceOwner,
        order::{CancelPlan, HeldCancel, OrderManager, status_is_known, venue_refusal},
        private_msg::{
            AccountOrder,
            PrivateFrame,
            VenueErrorScope,
            parse_private_frame,
            venue_error_scope,
        },
        public_stream::{
            BACKOFF_MAX,
            BACKOFF_MIN,
            CONNECT_TIMEOUT,
            PING_FRAME,
            PING_INTERVAL,
            local_now,
        },
        publish_error,
        rest::{self, MarketInfo, Resolution, SendOutcome},
        signer::{
            CreateOrder,
            SignerClient,
            SignerNetwork,
            python_sidecar_command,
            verify_identity,
        },
        slot::{
            AmbiguousFate,
            CancelFollowUp,
            DeadlineAction,
            NonceAction,
            Outbound,
            Placement,
            SweepSend,
            cancel_after_send,
            deadline_action,
            fate_of_ambiguous,
            nonce_action,
            placement_after_deadline,
            sweep_with_retries,
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

/// How long a `CancelAllOrders` command may wait for the send gate after it was asked for.
///
/// [`OrderPath::sweep`] waits [`SWEEP_REPLY_WAIT`] for each verdict and counts a late one as a
/// failure, so a sweep must start its attempt early enough to be answered inside that: this
/// leaves the send itself five seconds. Both sit under the stop's own `shutdown.sweep_timeout_ms`
/// (30 s in every shipped config).
const SWEEP_GATE_DEADLINE: Duration = Duration::from_secs(15);

/// How long [`OrderPath::sweep`] waits for each market's verdict before counting it unconfirmed.
const SWEEP_REPLY_WAIT: Duration = Duration::from_secs(20);

/// How often the same private-stream error code is told to the bots at most (§4.17).
const VENUE_ERROR_EVERY: Duration = Duration::from_secs(10);

/// The largest `tx_budget_per_minute` a config may name. The venue's largest `sendTx` tier is
/// 24 000 a minute (Premium; RH docs "Rate Limits", 2026-09-30), and the window is sized up front
/// to hold one stamp per send (`gate::TxBudget`), so a number past this guards nothing a real
/// account can reach while asking for gigabytes at startup.
const MAX_TX_BUDGET_PER_MINUTE: u32 = 100_000;

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
///
/// **An unknown key is refused at startup.** Every protective field here is optional, with
/// the unprotected behaviour as its default, so a misspelt one — `rate_limit_cooldown = 60000`
/// — would otherwise arm an order path without what the file says it has, silently. The rest
/// of the file is shared with the generic supervision config and stays permissive; this
/// section is the backend's alone.
#[derive(Deserialize, Clone, Debug)]
#[serde(deny_unknown_fields)]
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
    /// The connector's own ceiling on `sendTx` in any rolling minute, for the **whole L1
    /// account** — every bot this connector serves shares it, as they share the venue's limit
    /// (Standard: 60, §2.5). Every send counts: submits, cancels, `CancelAllOrders`. A send
    /// over it is refused before a nonce is spent ([`crate::lighter::gate`], §4.15). Absent ⇒
    /// no client-side budget, the behaviour before it existed.
    #[serde(default)]
    pub tx_budget_per_minute: Option<u32>,
    /// Of `tx_budget_per_minute`, how many only **risk-reducing** sends may take — cancels,
    /// `CancelAllOrders`, and IOC (or reduce-only) submits that shrink |position|; a post-only
    /// quote is a quote on either side — so a burst of quotes cannot leave the account unable to
    /// cancel them. Absent ⇒ 0. Needs `tx_budget_per_minute`, and must leave at least one token
    /// for quotes; anything else is refused at startup.
    #[serde(default)]
    pub tx_reserve: Option<u32>,
    /// How long nothing is sent after the venue answers a `sendTx` with 429/405, ms (§4.16).
    /// Its docs: a firewall rate limit holds the address a static 60 s, an API server's
    /// `weight / (total / 60)`; which one answered cannot be told from here, so the longer is
    /// the safe reading. Meanwhile new orders are refused locally, cancels are held and sent
    /// when it ends, a sweep waits for it if it ends before the sweep's deadline, and the order
    /// path reads nothing from the venue (no nonce re-read, no fate check). Absent ⇒ a 429/405
    /// is handled as the ambiguous answer it was before.
    #[serde(default)]
    pub rate_limit_cooldown_ms: Option<u64>,
    /// How soon into a 429/405 cooldown a **risk-reducing** send — a cancel, `CancelAllOrders`,
    /// a reducing IOC — may try anyway, ms; each probe the venue refuses doubles the wait, up to
    /// the end of the cooldown (§4.16). The docs' two cooldowns cannot be told apart from here
    /// — the firewall's static 60 s, an API server's `weight / (total / 60)`, seconds — and a
    /// cancel that waits a whole minute for the second kind leaves a stale quote resting all
    /// that time. A refused probe does not extend the cooldown and tells the bots nothing new;
    /// quotes always wait it out. Needs `rate_limit_cooldown_ms` and must be shorter than it.
    /// Absent ⇒ cancels wait the whole cooldown, the behaviour before it existed.
    #[serde(default)]
    pub rate_limit_cancel_probe_ms: Option<u64>,
}

impl OrderPathConfig {
    /// Refuses a combination that cannot do what it says, so it is found at startup rather
    /// than at the first quote it would refuse.
    pub fn validate(&self) -> Result<(), String> {
        match (self.tx_budget_per_minute, self.tx_reserve) {
            (None, Some(_)) => Err(
                "[order_path] tx_reserve needs tx_budget_per_minute: a reserve of no budget \
                 holds nothing back"
                    .to_string(),
            ),
            (Some(0), _) => Err(
                "[order_path] tx_budget_per_minute = 0 would refuse every send, cancels included"
                    .to_string(),
            ),
            (Some(per_minute), _) if per_minute > MAX_TX_BUDGET_PER_MINUTE => Err(format!(
                "[order_path] tx_budget_per_minute = {per_minute} is past every tier the venue \
                 sells (the largest is 24000 a minute); at most {MAX_TX_BUDGET_PER_MINUTE}"
            )),
            (Some(per_minute), Some(reserve)) if reserve >= per_minute => Err(format!(
                "[order_path] tx_reserve = {reserve} leaves no token of tx_budget_per_minute = \
                 {per_minute} for a new order"
            )),
            _ if self.rate_limit_cooldown_ms == Some(0) => Err(
                "[order_path] rate_limit_cooldown_ms = 0 is no cooldown; leave the field out for \
                 the behaviour without one"
                    .to_string(),
            ),
            _ => match (self.rate_limit_cooldown_ms, self.rate_limit_cancel_probe_ms) {
                (None, Some(_)) => Err(
                    "[order_path] rate_limit_cancel_probe_ms needs rate_limit_cooldown_ms: there is \
                     no cooldown to probe"
                        .to_string(),
                ),
                (_, Some(0)) => Err(
                    "[order_path] rate_limit_cancel_probe_ms = 0 would send into the cooldown at \
                     once; leave the field out for cancels that wait it out"
                        .to_string(),
                ),
                (Some(cooldown), Some(probe)) if probe >= cooldown => Err(format!(
                    "[order_path] rate_limit_cancel_probe_ms = {probe} never probes a cooldown of \
                     {cooldown} ms; it must be shorter"
                )),
                _ => Ok(()),
            },
        }
    }

    /// The cooldown after a 429/405, if one is configured.
    pub fn rate_limit_cooldown(&self) -> Option<Duration> {
        self.rate_limit_cooldown_ms.map(Duration::from_millis)
    }

    /// How soon into a cooldown a risk-reducing send may probe it, if probes are configured.
    pub fn rate_limit_cancel_probe(&self) -> Option<Duration> {
        self.rate_limit_cancel_probe_ms.map(Duration::from_millis)
    }

    /// The client-side `sendTx` budget, if one is configured.
    pub fn send_budget(&self) -> Option<TxBudget> {
        self.tx_budget_per_minute
            .map(|per_minute| TxBudget::new(per_minute, self.tx_reserve.unwrap_or(0)))
    }

    /// What the send gate this config arms will do, in one line — `"OFF"` when it does
    /// nothing, otherwise e.g. `"budget 48/min, reserve 12, cooldown 60000 ms, cancel probe
    /// 5000 ms"`. Logged once
    /// at arming: the file is not the evidence of what the process runs with — a build that
    /// predates a field never read it, and a field read as absent turns its protection off.
    pub fn send_gate_summary(&self) -> String {
        if self.tx_budget_per_minute.is_none() && self.rate_limit_cooldown_ms.is_none() {
            return "OFF".to_string();
        }
        let budget = match self.tx_budget_per_minute {
            Some(per_minute) => format!(
                "budget {per_minute}/min, reserve {}",
                self.tx_reserve.unwrap_or(0)
            ),
            None => "budget OFF".to_string(),
        };
        let cooldown = match (self.rate_limit_cooldown_ms, self.rate_limit_cancel_probe_ms) {
            (Some(cooldown_ms), Some(probe_ms)) => {
                format!("cooldown {cooldown_ms} ms, cancel probe {probe_ms} ms")
            }
            (Some(cooldown_ms), None) => format!("cooldown {cooldown_ms} ms, cancel probe OFF"),
            (None, _) => "cooldown OFF".to_string(),
        };
        format!("{budget}, {cooldown}")
    }
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
        /// The clock marks this placement has collected so far, opened in
        /// [`OrderPath::submit`]. Carried here because the queue wait it measures is the
        /// wait on *this* channel, and only the two ends together can see it.
        outbound: Outbound,
    },
    Cancel {
        coi: i64,
        plan: CancelPlan,
    },
    /// Cancel-all for a market (`255` = all). `reply` carries **whether the sweep's `sendTx`
    /// succeeded** (Finding 2) — not merely that it was attempted — so an orderly stop can turn
    /// a failed sweep into a non-zero exit rather than claiming success. The `bool` type makes
    /// answering success after a failed send structurally impossible.
    CancelAll {
        market_index: i32,
        reply: Option<oneshot::Sender<bool>>,
        /// The last moment an attempt may still be started: the send gate is waited for until
        /// then and no longer ([`crate::lighter::gate::admit_by`]).
        deadline: Instant,
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
    /// Whether the private account stream is live: connected AND snapshotted since it last
    /// (re)connected. Written by the [`PrivateStreamTask`], read by [`Self::submit`], which
    /// refuses to place an order whose fill nothing would report.
    account_stream_live: Arc<AtomicBool>,
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
        network: SignerNetwork,
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
            network,
            rest_url: rest_url.clone(),
            order_manager: order_manager.clone(),
            ev_tx: ev_tx.clone(),
        };
        tasks.push(tokio::spawn(slot.run(command_rx)));

        let account_stream_live = Arc::new(AtomicBool::new(false));
        let stream = PrivateStreamTask {
            config: config.clone(),
            rest_url,
            public_url,
            symbols,
            symbol_rx,
            unlisted: HashSet::new(),
            awaiting_catalog: false,
            order_manager: order_manager.clone(),
            command_tx: command_tx.clone(),
            ev_tx,
            account_stream_live: account_stream_live.clone(),
            venue_errors: HashMap::new(),
        };
        tasks.push(tokio::spawn(stream.run()));

        Self {
            order_manager,
            command_tx,
            tasks,
            account_stream_live,
        }
    }

    /// Registers a new order and queues it to be signed and sent. Acceptance is confirmed off
    /// the private channel, never here (§4.11).
    pub fn submit(&self, symbol: &str, order: &Order, ev_tx: &UnboundedSender<PublishEvent>) {
        // Open the outbound stopwatch before anything else this call does, so nothing the
        // connector spends is charged to the venue by omission. `order.local_timestamp` was
        // stamped in the bot process before iceoryx.
        let outbound = Outbound {
            requested_ns: order.local_timestamp,
            received_ns: local_now(),
            queued: StdInstant::now(),
        };
        let plan = {
            let mut manager = self.order_manager.lock().unwrap();
            match manager.market_info(symbol) {
                // Checked before `new_order`, so a refused order is never tracked (its id
                // would otherwise be refused as already live for the life of the process).
                Some(_) if !self.account_stream_live.load(Ordering::Acquire) => {
                    Err(crate::lighter::order::OrderError::AccountStreamDown {
                        symbol: symbol.to_string(),
                    })
                }
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
                        outbound,
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
    /// that resolves to the folded [`SweepOutcome`] once every sweep's `sendTx` has been
    /// answered — so an orderly stop can both wait for the cancels and learn whether they
    /// succeeded (Finding 2). Bounded, so a wedged slot cannot hang the stop.
    ///
    /// Fail closed ([`SweepOutcome::from_confirmations`]): a market whose `CancelAll` could not
    /// even be queued (the slot task is gone), whose reply was dropped, or which timed out
    /// counts as an unconfirmed cancel — [`SweepOutcome::Failed`] — because its orders may
    /// still be resting. An empty market list is [`SweepOutcome::Cancelled`]: nothing to do.
    pub fn sweep(&self, market_indices: Vec<i32>) -> JoinHandle<SweepOutcome> {
        let command_tx = self.command_tx.clone();
        let deadline = Instant::now() + SWEEP_GATE_DEADLINE;
        tokio::spawn(async move {
            let mut replies = Vec::new();
            let mut confirmations: Vec<bool> = Vec::new();
            for market_index in market_indices {
                let (reply_tx, reply_rx) = oneshot::channel();
                if command_tx
                    .send(Command::CancelAll {
                        market_index,
                        reply: Some(reply_tx),
                        deadline,
                    })
                    .is_ok()
                {
                    replies.push(reply_rx);
                } else {
                    // The slot task is gone; this market cannot be swept.
                    confirmations.push(false);
                }
            }
            for reply in replies {
                // A dropped sender (slot gone) or a timeout is an unconfirmed cancel; the bound
                // keeps a slow one from hanging the stop. Only an explicit `true` confirms.
                let confirmed =
                    matches!(time::timeout(SWEEP_REPLY_WAIT, reply).await, Ok(Ok(true)));
                confirmations.push(confirmed);
            }
            SweepOutcome::from_confirmations(confirmations)
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
    /// What the signer must sign for; checked against the sidecar's own answer before the
    /// slot sends anything ([`verify_identity`]).
    network: SignerNetwork,
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
    async fn run(self, command_rx: UnboundedReceiver<Command>) {
        let command = python_sidecar_command(
            &self.config.signer_python,
            &self.config.signer_script,
            &self.config.signer_config,
            &self.network,
        );
        let signer = match SignerClient::spawn(command).await {
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
        self.serve(signer, command_rx).await;
    }

    /// The slot's life once its signer is up: verify what it signs for, seed the nonce, then
    /// serve commands and confirmation deadlines until the command channel closes.
    ///
    /// Split from [`Self::run`] so a test can hand it an in-process signer
    /// ([`SignerClient::over`]) and drive the whole money path — admission, nonce, sign,
    /// `sendTx`, the answer — against a scripted venue, with no subprocess and no key.
    async fn serve(self, mut signer: SignerClient, mut command_rx: UnboundedReceiver<Command>) {
        // Before anything is signed: does the sidecar sign for the network, account and slot
        // this connector serves? A sidecar that resolved another chain exits on its own (its
        // stderr says why, and `identity` sees the pipe close); one that started on the wrong
        // network for any other reason is refused here. Either way the slot never sends —
        // the venue's answer to a wrong-chain signature is every transaction refused, after
        // the nonce is spent and with a sweep reporting success.
        match signer.identity().await.and_then(|identity| {
            verify_identity(
                &identity,
                &self.network,
                self.config.account_index,
                self.config.api_key_index,
            )
            .map(|()| identity)
        }) {
            Ok(identity) => info!(
                chain_id = identity.chain_id,
                url = %identity.url,
                account_index = identity.account_index,
                api_key_index = self.config.api_key_index,
                "The Lighter signer signs for the network this connector dials."
            ),
            Err(error) => {
                error!(
                    %error,
                    "The Lighter signer does not sign for this connector's network, account \
                     or slot; the order path is down."
                );
                report_error(
                    &self.ev_tx,
                    ErrorKind::CriticalConnectionError,
                    &format!("signer refused: {error}"),
                );
                let _ = signer.shutdown().await;
                return;
            }
        }

        let mut nonce = NonceOwner::new(self.config.api_key_index);
        self.reseed(&mut nonce).await;

        // Asked before every `sendTx`, and before the nonce it would spend (§4.15, §4.16).
        let mut gate = SendGate::new(
            self.config.send_budget(),
            self.config.rate_limit_cooldown(),
            self.config.rate_limit_cancel_probe(),
        );
        // Cancels a cooldown kept from going out, sent when it is over (§4.16).
        let mut held = HeldCancels::default();

        let mut awaiting: HashMap<i64, Awaiting> = HashMap::new();
        let mut deadline_tick = time::interval(Duration::from_millis(250));

        loop {
            select! {
                command = command_rx.recv() => {
                    // Held cancels go before anything newer, so a requote cannot overtake the
                    // cancel of the quote it replaces.
                    self.release_held_cancels(&mut signer, &mut nonce, &mut gate, &mut held).await;
                    match command {
                        Some(Command::Submit { coi, order, outbound }) => {
                            if let Some(admission) = self.admit_submit(&mut gate, coi) {
                                self.send_order(&mut signer, &mut nonce, &mut gate, admission, coi, &order, &outbound, &mut awaiting).await;
                            }
                        }
                        Some(Command::Cancel { coi, plan }) => {
                            if let Some(admission) = self.admit_cancel(&mut gate, &mut held, coi, &plan) {
                                self.send_cancel(&mut signer, &mut nonce, &mut gate, &mut held, admission, coi, &plan).await;
                            }
                        }
                        Some(Command::CancelAll { market_index, reply, deadline }) => {
                            let mut sender = CancelAllSender {
                                slot: &self,
                                signer: &mut signer,
                                nonce: &mut nonce,
                                gate: &mut gate,
                                market_index,
                                deadline,
                                admission: Admission::Send,
                            };
                            sweep_market(&mut sender, &self.ev_tx, market_index, reply).await;
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
                    self.release_held_cancels(&mut signer, &mut nonce, &mut gate, &mut held).await;
                    self.check_deadlines(&mut signer, &gate, &mut awaiting).await;
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

    #[allow(clippy::too_many_arguments)]
    async fn send_order(
        &self,
        signer: &mut SignerClient,
        nonce: &mut NonceOwner,
        gate: &mut SendGate,
        admission: Admission,
        coi: i64,
        order: &CreateOrder,
        outbound: &Outbound,
        awaiting: &mut HashMap<i64, Awaiting>,
    ) {
        let dequeued = StdInstant::now();
        let Some(reserved_nonce) = self.reserve(nonce).await else {
            self.probe_failed(gate, admission, "the nonce could not be read");
            self.expire(coi, "the nonce slot could not be seeded");
            return;
        };
        let reserved_at = StdInstant::now();
        let signed = match signer
            .create_order(order, reserved_nonce, self.config.api_key_index)
            .await
        {
            Ok(signed) => signed,
            Err(error) => {
                nonce.released();
                self.expire(coi, &format!("the signer refused the order: {error}"));
                return;
            }
        };
        let signed_at = StdInstant::now();
        let market_index = order.market_index as i32;
        let sent = rest::send_tx(&self.rest_url, signed.tx_type, &signed.tx_info).await;

        // One line per placement, whatever the venue answered — the outbound leg cost what it
        // cost even when the send failed. `info!`, not `debug!`: the running unit has no
        // `RUST_LOG` and emits INFO only, so a `debug!` here would be invisible in exactly the
        // deployment this measures. MEASURED: ~0.23 placements/s, so the volume is nothing.
        let spans = outbound.spans(dequeued, reserved_at, signed_at, StdInstant::now());
        info!(
            coi,
            ipc_us = spans.ipc_us,
            queue_us = spans.queue_us,
            nonce_us = spans.nonce_us,
            sign_us = spans.sign_us,
            send_us = spans.send_us,
            "Lighter placement timing."
        );

        match sent {
            Ok(outcome) => {
                let outcome = self.received(gate, admission, outcome);
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
                    SendOutcome::RateLimited { status, detail } => {
                        // The venue's docs promise a cooldown, not that a 429 went unprocessed
                        // (§4.16): the order may rest. Kept tracked with no tx_hash, exactly as
                        // an ambiguous send, so its fate is read off accountActiveOrders — once
                        // the cooldown is over (`check_deadlines`); the nonce was invalidated,
                        // not re-read. Never expired blind (§1.1).
                        warn!(
                            coi,
                            status,
                            %detail,
                            "A Lighter sendTx was rate-limited; keeping the order tracked until \
                             its fate can be read after the cooldown (§4.16)."
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
                    match applied {
                        ApplyOutcome::Publish((symbol, published)) => {
                            warn!(
                                coi,
                                %symbol,
                                "An ambiguous Lighter send DID land; adopting the resting order \
                                 from accountActiveOrders — no re-send, no duplicate (§3.3, §5 \
                                 gate)."
                            );
                            publish_order(&self.ev_tx, &symbol, published);
                        }
                        ApplyOutcome::Ignored => {}
                        // Finding 3: the landed order carries a status this connector cannot
                        // classify. Keep it (never dropped) and escalate; the next reconnect's
                        // reconciliation resolves it — Lighter has no periodic reconcile.
                        ApplyOutcome::Unrecognised => {
                            error!(
                                coi,
                                status = %order.status,
                                "An ambiguous Lighter send landed with an unrecognised status; \
                                 keeping the order and escalating (Finding 3, §1.1)."
                            );
                            report_error(
                                &self.ev_tx,
                                ErrorKind::CriticalConnectionError,
                                &format!("unrecognised Lighter order status {:?}", order.status),
                            );
                        }
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

    #[allow(clippy::too_many_arguments)]
    async fn send_cancel(
        &self,
        signer: &mut SignerClient,
        nonce: &mut NonceOwner,
        gate: &mut SendGate,
        held: &mut HeldCancels,
        admission: Admission,
        coi: i64,
        plan: &CancelPlan,
    ) {
        let Some(reserved) = self.reserve(nonce).await else {
            if admission == Admission::Probe {
                // The cooldown's 429 left the nonce to be re-read, and a venue that is still
                // refusing the account refuses that read too: a refused probe, not a cancel
                // that failed. It stays held — and pending to the bot — for the next probe.
                self.probe_failed(gate, admission, "the nonce could not be read");
                held.hold(coi, plan.clone());
                return;
            }
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
            Ok(outcome) => {
                let outcome = self.received(gate, admission, outcome);
                self.apply_nonce(nonce, &outcome).await;
                match cancel_after_send(&outcome) {
                    CancelFollowUp::AwaitChannel => {}
                    // A `21104` or an ambiguous answer must not leave the order looking
                    // cancel-pending to the bot while it stands on the venue (§3.3).
                    CancelFollowUp::HandBack(reason) => self.clear_cancel_request(coi, &reason),
                    // Rate-limited under a cooldown: held, still pending, and sent again when the
                    // cooldown is over unless the channel has closed the order by then (§4.16).
                    CancelFollowUp::AfterCooldown => {
                        info!(
                            coi,
                            "A Lighter cancel the venue answered 429/405 is held. It stays \
                             pending to the bot and goes out again when the cooldown is over \
                             (§4.16)."
                        );
                        held.hold(coi, plan.clone());
                    }
                }
            }
            Err(error) => {
                nonce.invalidated();
                self.reseed(nonce).await;
                self.clear_cancel_request(coi, &format!("the cancel sendTx failed: {error}"));
            }
        }
        // The confirmed cancel arrives on the private channel as `status: canceled`; nothing
        // here reports it, so a refused transport does not look like a cancel that took.
    }

    /// One reserve-sign-send round of a sweep. `Err` is why nothing was sent, or why no answer
    /// came back; the nonce is left exactly as the order path leaves it for any other send.
    async fn cancel_all_once(
        &self,
        signer: &mut SignerClient,
        nonce: &mut NonceOwner,
        gate: &mut SendGate,
        admission: Admission,
        market_index: i32,
    ) -> Result<SendOutcome, String> {
        let Some(reserved) = self.reserve(nonce).await else {
            self.probe_failed(gate, admission, "the nonce could not be read");
            return Err("couldn't seed the nonce for the sweep".to_string());
        };
        // CANCEL_ALL_TIF_IMMEDIATE requires time_ms = 0, or the .so refuses on the client (§3.4).
        let signed = match signer
            .cancel_all(0, 0, market_index, reserved, self.config.api_key_index)
            .await
        {
            Ok(signed) => signed,
            Err(error) => {
                nonce.released();
                return Err(format!("the signer refused the sweep: {error}"));
            }
        };
        match rest::send_tx(&self.rest_url, signed.tx_type, &signed.tx_info).await {
            Ok(outcome) => {
                let outcome = self.received(gate, admission, outcome);
                self.apply_nonce(nonce, &outcome).await;
                Ok(outcome)
            }
            Err(error) => {
                nonce.invalidated();
                self.reseed(nonce).await;
                Err(format!("the sweep's sendTx failed: {error}"))
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
            // Re-read by `reserve` before the next send, which the cooldown holds (§4.16).
            NonceAction::Invalidate => nonce.invalidated(),
        }
    }

    /// Reads the venue's answer to a send before anything acts on it (§4.16).
    ///
    /// * An ack's rate-limit report (`message.ratelimit`, §2.7 — its format on this instance is
    ///   unmeasured) is logged at `debug!` every time, and quoted in a `warn!` — at most every
    ///   10 s — while three quarters of the send budget are spent.
    /// * A 429/405 starts the cooldown and is told to the bots **once**, as an `OrderError`
    ///   (a rate limit to the myhft classifier), then handed on as
    ///   [`SendOutcome::RateLimited`].
    /// * A 429/405 answering a **probe** ([`Admission::Probe`], §4.16) backs the probes off and
    ///   leaves the cooldown as it was, and the bots are not told again: the 429 that started
    ///   the cooldown was their one error for it. One that arrives after the cooldown has ended
    ///   is a new rate limit, told as one.
    /// * Without a configured cooldown a 429/405 is handed on as `Ambiguous` — the answer it
    ///   was before `RateLimited` existed — so that path is unchanged by construction.
    fn received(
        &self,
        gate: &mut SendGate,
        admission: Admission,
        outcome: SendOutcome,
    ) -> SendOutcome {
        let now = Instant::now();
        match outcome {
            SendOutcome::RateLimited { status, detail }
                if admission == Admission::Probe && gate.cooling(now).is_some() =>
            {
                self.probe_failed(gate, admission, &format!("HTTP {status}: {detail}"));
                SendOutcome::RateLimited { status, detail }
            }
            SendOutcome::Accepted(ack) => {
                let ratelimit = ack.ratelimit.as_deref().unwrap_or("<none>");
                debug!(ratelimit, "A Lighter sendTx was accepted.");
                if gate.near_limit(now)
                    && let Some(held_back) = gate.log_near_limit(now)
                {
                    let (used, per_minute) = gate.usage(now).unwrap_or_default();
                    warn!(
                        used,
                        per_minute,
                        held_back,
                        ratelimit,
                        "The Lighter account is near its sendTx budget; the venue's own \
                         rate-limit report is quoted."
                    );
                }
                SendOutcome::Accepted(ack)
            }
            SendOutcome::RateLimited { status, detail } => match gate.rate_limited(now) {
                None => SendOutcome::Ambiguous { status, detail },
                Some(until) => {
                    let cooldown_ms = until.saturating_duration_since(now).as_millis() as u64;
                    error!(
                        status,
                        %detail,
                        cooldown_ms,
                        "The venue rate-limited this Lighter account's sendTx; nothing is sent \
                         until the cooldown is over — new orders are refused, cancels are held, \
                         a sweep waits if it can (§4.16)."
                    );
                    let cancels = match self.config.rate_limit_cancel_probe_ms {
                        None => "cancels wait for it".to_string(),
                        Some(probe_ms) => {
                            format!("cancels are held and probe it from {probe_ms} ms")
                        }
                    };
                    report_error(
                        &self.ev_tx,
                        ErrorKind::OrderError,
                        &format!(
                            "the venue rate-limited this account's sendTx (HTTP {status}); the \
                             connector sends nothing for {cooldown_ms} ms — new orders are \
                             refused, {cancels}; rate-limited"
                        ),
                    );
                    SendOutcome::RateLimited { status, detail }
                }
            },
            other => other,
        }
    }

    /// A probe could not get through — the venue answered it 429/405, or the nonce it needed
    /// could not be read: the probes back off and the cooldown stands as it was (§4.16). Says
    /// so in the log only; the bots heard the one error the cooldown's own 429 was worth. A
    /// send that was not a probe is not touched.
    fn probe_failed(&self, gate: &mut SendGate, admission: Admission, why: &str) {
        if admission != Admission::Probe {
            return;
        }
        let now = Instant::now();
        let next_probe_in_ms = gate
            .probe_refused(now)
            .map(|at| at.saturating_duration_since(now).as_millis() as u64);
        warn!(
            why,
            ?next_probe_in_ms,
            "A Lighter send probing the cooldown did not get through; the cooldown stands as it \
             was, the next probe waits longer, and the bots are not told again (§4.16)."
        );
    }

    /// Sends the cancels a cooldown held, once it is over (§4.16).
    ///
    /// What decides is whether the order still **rests** ([`OrderManager::resume_held_cancel`]):
    /// one that has gone meanwhile — filled, closed by the venue — is dropped, there is nothing
    /// left to cancel and the cancel would spend a send on a refusal; one that filled in part is
    /// as stale a quote as before, smaller, and its cancel goes — after the bot is told it is in
    /// flight again, because the fill's update cleared the request it had seen.
    async fn release_held_cancels(
        &self,
        signer: &mut SignerClient,
        nonce: &mut NonceOwner,
        gate: &mut SendGate,
        held: &mut HeldCancels,
    ) {
        // With probes configured a cancel may try before the cooldown is over (§4.16).
        if held.is_empty()
            || gate
                .cooling_for(Instant::now(), TxClass::RiskReducing)
                .is_some()
        {
            return;
        }
        for (coi, plan) in held.take() {
            let resumed = self.order_manager.lock().unwrap().resume_held_cancel(coi);
            match resumed {
                HeldCancel::Gone => {
                    info!(
                        coi,
                        "Dropping a Lighter cancel held through a cooldown: its order has gone \
                         (filled, or closed by the venue), so nothing is left to cancel."
                    );
                    continue;
                }
                HeldCancel::StillPending => {}
                HeldCancel::Rerequested(symbol, order) => {
                    info!(
                        coi,
                        %symbol,
                        leaves_qty = order.leaves_qty,
                        "A Lighter order filled in part while its cancel was held; it still \
                         rests, so the cancel goes out now (§4.16)."
                    );
                    publish_order(&self.ev_tx, &symbol, order);
                }
            }
            if let Some(admission) = self.admit_cancel(gate, held, coi, &plan) {
                self.send_cancel(signer, nonce, gate, held, admission, coi, &plan)
                    .await;
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
        gate: &SendGate,
        awaiting: &mut HashMap<i64, Awaiting>,
    ) {
        let now = Instant::now();
        if let Some(until) = gate.cooling(now) {
            // `GET /tx` and `accountActiveOrders` are requests to a venue that is refusing this
            // account; a deadline that lapses inside the cooldown is read once it is over
            // (§4.16). An order that confirms on the channel meanwhile is simply dropped then.
            for entry in awaiting.values_mut() {
                if entry.deadline <= now {
                    entry.deadline = until;
                }
            }
            return;
        }
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
        if let Some(symbol) = self.hand_back_unsent(coi, reason) {
            warn!(coi, reason, %symbol, "Expiring a Lighter order back to the bot (§4.11).");
        }
    }

    /// Takes a submit that produced no order out of tracking and hands it back to the bot:
    /// the order as `Expired` first, then the error — the order `reject_order` keeps, so the
    /// bot's state is clean before its handler runs. Returns the symbol; logs nothing.
    fn hand_back_unsent(&self, coi: i64, reason: &str) -> Option<String> {
        let (symbol, order) = self.order_manager.lock().unwrap().submit_failed(coi)?;
        publish_order(&self.ev_tx, &symbol, order);
        report_error(
            &self.ev_tx,
            ErrorKind::OrderError,
            &format!("{symbol}: {reason}"),
        );
        Some(symbol)
    }

    fn clear_cancel_request(&self, coi: i64, reason: &str) {
        if let Some(symbol) = self.hand_back_cancel(coi, reason) {
            warn!(coi, reason, %symbol, "A Lighter cancel did not send; leaving the order live.");
        }
    }

    /// Clears the pending cancel on a still-live order and tells the bot why — the order first,
    /// its request cleared, then the error — so it can cancel again instead of waiting on an
    /// answer that will never come. Returns the symbol; logs nothing.
    fn hand_back_cancel(&self, coi: i64, reason: &str) -> Option<String> {
        let (symbol, order) = self.order_manager.lock().unwrap().cancel_failed(coi)?;
        publish_order(&self.ev_tx, &symbol, order);
        report_error(
            &self.ev_tx,
            ErrorKind::OrderError,
            &format!("{symbol}: {reason}"),
        );
        Some(symbol)
    }

    /// Whether submit `coi` may go out now (§4.15). A refused one is handed back to the bot
    /// exactly as a submit refused while the account stream is down — `Expired`, then an
    /// `OrderError` — before a nonce is reserved, so nothing is spent on it. An IOC (or
    /// reduce-only) submit that shrinks |position| may take the reserve
    /// ([`OrderManager::reduces_position`]); a quote may not — a post-only one on the reducing
    /// side included.
    fn admit_submit(&self, gate: &mut SendGate, coi: i64) -> Option<Admission> {
        let class = if self.order_manager.lock().unwrap().reduces_position(coi) {
            TxClass::RiskReducing
        } else {
            TxClass::Open
        };
        match gate.admit(Instant::now(), class) {
            Ok(admission) => Some(admission),
            Err(refusal) => {
                self.log_refusal(gate, &refusal, "submit");
                self.hand_back_unsent(coi, &refusal.to_string());
                None
            }
        }
    }

    /// Whether cancel `coi` may go out now (§4.15, §4.16). A cancel may take the reserve, so
    /// the budget refuses it only when it is spent through; then it goes back to the bot as a
    /// cancel that did not go — request cleared, order still live — and the bot may cancel
    /// again. A cooldown does not hand it back: it is held, still pending to the bot, and sent
    /// the moment the cooldown is over — or, with probes, the moment the next probe is due
    /// ([`Self::release_held_cancels`]) — handed back, the bot would only cancel again into the
    /// same cooldown. Inside a cooldown it is let through as a probe ([`Admission::Probe`]).
    fn admit_cancel(
        &self,
        gate: &mut SendGate,
        held: &mut HeldCancels,
        coi: i64,
        plan: &CancelPlan,
    ) -> Option<Admission> {
        match gate.admit(Instant::now(), TxClass::RiskReducing) {
            Ok(admission) => Some(admission),
            Err(refusal @ Refusal::Cooldown { .. }) => {
                // Not `log_refusal`: that line says the send went back to the bot, and this one
                // did not — it stays pending to the bot and goes out when the gate opens.
                info!(
                    coi,
                    wait_ms = refusal
                        .free_at()
                        .saturating_duration_since(Instant::now())
                        .as_millis() as u64,
                    "A Lighter cancel is held: the venue rate-limited the account. It stays \
                     pending to the bot and goes out when the cooldown is over (§4.16)."
                );
                held.hold(coi, plan.clone());
                None
            }
            Err(refusal) => {
                self.log_refusal(gate, &refusal, "cancel");
                self.hand_back_cancel(coi, &refusal.to_string());
                None
            }
        }
    }

    /// One line for a closed gate, at most every [`crate::lighter::gate::REFUSAL_LOG_EVERY`]:
    /// every refused order already reaches the bot with its own error, so the log only has to
    /// say that the gate is shut, why, and for how many it stayed quiet.
    fn log_refusal(&self, gate: &mut SendGate, refusal: &Refusal, what: &str) {
        let now = Instant::now();
        if let Some(held_back) = gate.log_refusal(now) {
            let (used, per_minute) = gate.usage(now).unwrap_or_default();
            warn!(
                what,
                held_back,
                used,
                per_minute,
                free_in_ms = refusal.free_at().saturating_duration_since(now).as_millis() as u64,
                %refusal,
                "The Lighter send gate refused a send; it went back to the bot unsent."
            );
        }
    }
}

/// Cancels a rate-limit cooldown kept from going out, in the order they were asked for, to be
/// sent when it is over or the next probe is due (§4.16). One per order: a cancel already held
/// is not held twice.
#[derive(Default)]
struct HeldCancels(Vec<(i64, CancelPlan)>);

impl HeldCancels {
    fn hold(&mut self, coi: i64, plan: CancelPlan) {
        if !self.0.iter().any(|(held, _)| *held == coi) {
            self.0.push((coi, plan));
        }
    }

    fn take(&mut self) -> Vec<(i64, CancelPlan)> {
        std::mem::take(&mut self.0)
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// One market's `CancelAllOrders` (§3.4) driven to its verdict — the slot's whole answer to a
/// [`Command::CancelAll`]: sent until the venue takes it or
/// [`crate::lighter::slot::SWEEP_ATTEMPTS`] are spent ([`sweep_with_retries`]), a failure
/// reported loudly, and the verdict answered on `reply`. Returns whether it took.
///
/// The reply carries the **real** outcome, after every retry, never an unconditional success:
/// [`OrderPath::sweep`] folds it into the `SweepOutcome` an orderly stop exits on, so a sweep
/// that never took is a non-zero stop rather than the exit 0 a bare "done" reply gave.
///
/// A sweep that did not take is an `error!` and a `CriticalConnectionError` to every bot. It
/// used to be one send and an `info!` "Sent a Lighter CancelAllOrders sweep." whatever came back
/// — a `21104` or a 429 included — so the one line an operator reads after a bot's death or a
/// stop said the market had been swept while its orders kept resting.
async fn sweep_market(
    sender: &mut impl SweepSend,
    ev_tx: &UnboundedSender<PublishEvent>,
    market_index: i32,
    reply: Option<oneshot::Sender<bool>>,
) -> bool {
    let took = match sweep_with_retries(sender).await {
        Ok(attempts) => {
            info!(
                market_index,
                attempts, "Sent a Lighter CancelAllOrders sweep; the venue took it."
            );
            true
        }
        Err(reason) => {
            error!(
                market_index,
                %reason,
                "A Lighter CancelAllOrders sweep FAILED; orders may still be resting on this \
                 market."
            );
            report_error(
                ev_tx,
                ErrorKind::CriticalConnectionError,
                &format!("CancelAllOrders on market {market_index} failed: {reason}"),
            );
            false
        }
    };
    if let Some(reply) = reply {
        let _ = reply.send(took);
    }
    took
}

/// The slot's [`SweepSend`]: one market's `CancelAllOrders`, one reserve-sign-send round at a
/// time ([`SlotTask::cancel_all_once`]).
struct CancelAllSender<'a> {
    slot: &'a SlotTask,
    signer: &'a mut SignerClient,
    nonce: &'a mut NonceOwner,
    gate: &'a mut SendGate,
    market_index: i32,
    /// The command's [`Command::CancelAll`] deadline.
    deadline: Instant,
    /// How the gate let the attempt about to be sent through — a probe inside a cooldown or an
    /// ordinary send — set by [`SweepSend::admit`] and read with the answer.
    admission: Admission,
}

impl SweepSend for CancelAllSender<'_> {
    async fn admit(&mut self) -> Result<(), String> {
        let now = Instant::now();
        match self.gate.admit(now, TxClass::RiskReducing) {
            Ok(admission) => self.admission = admission,
            Err(refusal) => {
                info!(
                    market_index = self.market_index,
                    wait_ms = refusal.free_at().saturating_duration_since(now).as_millis() as u64,
                    %refusal,
                    "A Lighter CancelAllOrders sweep finds the send gate shut; it waits if the gate \
                     opens before the sweep's deadline."
                );
                self.admission = admit_by(self.gate, TxClass::RiskReducing, self.deadline)
                    .await
                    .map_err(|refusal| {
                        format!(
                            "{refusal}, and the send gate does not open before the sweep's deadline"
                        )
                    })?;
            }
        }
        Ok(())
    }

    fn send_once(&mut self) -> impl Future<Output = Result<SendOutcome, String>> + Send {
        self.slot.cancel_all_once(
            self.signer,
            self.nonce,
            self.gate,
            self.admission,
            self.market_index,
        )
    }
}

/// Why [`PrivateStreamTask::connect`] returned. A token refresh is a clean, expected cycle,
/// **not** a disconnection: the run loop reconnects immediately, resets the backoff and
/// publishes no `ConnectionInterrupted`, so an 8 h-ceiling refresh (§3.4) never looks to a bot
/// like a lost connection.
#[derive(Debug)]
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
    /// Symbols the venue has given a **listing verdict** about — "not listed", or listed and
    /// not `active` (18 of mainnet's 228 markets were inactive when this was written).
    ///
    /// A verdict does not change while the process runs, so re-asking about one on every wake
    /// buys nothing and spends a REST call each time; a typo in a bot's config would ask for
    /// ever. Anything else — a catalog that could not be read — is deliberately **not** a
    /// verdict and stays askable, because writing it off would cost every order on that symbol.
    /// Re-armed by a fresh `register()` of the same symbol (a corrected typo, or a market that
    /// has since listed) and by the connect, which re-asks about everything regardless.
    unlisted: HashSet<String>,
    /// Whether a catalog round trip is out. At most one is, ever — per connection: `serve`
    /// clears it, because the answer to a trip started on a connection that has since dropped
    /// is delivered to that connection's channel and goes nowhere.
    awaiting_catalog: bool,
    order_manager: Arc<Mutex<OrderManager>>,
    command_tx: UnboundedSender<Command>,
    ev_tx: UnboundedSender<PublishEvent>,
    /// Shared with [`OrderPath`]: set on this connection's `account_all_orders` snapshot,
    /// cleared the moment the connection ends ([`Self::serve`]).
    account_stream_live: Arc<AtomicBool>,
    /// When each venue error code was last told to the bots ([`Self::on_venue_error`]).
    venue_errors: HashMap<i64, LogThrottle>,
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
        // The market↔symbol map the position frames and every submit need, re-read in full on
        // every connect (§3.4) — see [`Self::resolve_all_registered`] for why "in full" and not
        // "only what is missing". Awaited rather than spawned, deliberately: there is no socket
        // yet, so nothing can be starved, and the map is then in hand before the first account
        // frame — which carries only a `market_index` — arrives.
        self.resolve_all_registered().await;

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
        let ended = self.serve_connection(write, read, lifetime_s).await;
        // Whatever ended it — a drop, a close, a token refresh — the order state is no longer
        // current until the next connection snapshots. Orders wait for that
        // ([`OrderPath::submit`]); cancels do not.
        if self.account_stream_live.swap(false, Ordering::AcqRel) {
            match &ended {
                Ok(StreamEnd::TokenRefresh) => info!(
                    "The Lighter account stream is cycling for its token refresh; new orders \
                     wait for the next snapshot."
                ),
                Err(error) => warn!(
                    ?error,
                    "The Lighter account stream is down; new orders are refused until it has \
                     reconnected and snapshotted."
                ),
            }
        }
        ended
    }

    async fn serve_connection<S, R>(
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
        // Where a spawned catalog round trip delivers its answer — see [`Self::start_resolution`]
        // for why it is spawned rather than awaited where it is needed. Per connection: an
        // answer to a trip started on a connection that has since dropped has nowhere useful to
        // go, because the next connect re-reads the whole catalog anyway.
        let (catalog_tx, mut catalog_rx) = unbounded_channel::<Resolution>();
        self.awaiting_catalog = false;

        // The first tick fires immediately (`time::interval`), which is on purpose: it sends the
        // keepalive at once and, if the connect-time resolve left anything unresolved, retries
        // it straight away rather than a full period later. Every tick after that is a period
        // apart, so the retry is bounded.
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
                    // Keepalive first: this connection's liveness does not depend on the
                    // catalog, and the venue drops a connection with no client frame.
                    write.send(Message::Text(PING_FRAME.into())).await?;
                    // The timed retry. A refused resolve used to be surfaced and then never
                    // asked about again: a catalog blip while every symbol was already
                    // registered left the whole account unable to place an order, and the only
                    // impulses that could clear it were a *new* `register()`, a socket drop, or
                    // the ~7.5 h token refresh. `public_stream` retries on its housekeeping
                    // tick and `bybit` on its ping tick (`AGENTS.md` §4.2); this is the same
                    // move. A healthy connection has nothing unresolved, so this sends nothing.
                    self.start_resolution(&catalog_tx);
                }
                _ = &mut refresh => {
                    return Ok(StreamEnd::TokenRefresh);
                }
                // The answer to a round trip started by one of the impulses in
                // `start_resolution`. A pattern that does not match disables this arm rather
                // than ending the connection: the sender lives as long as this loop does, so
                // `None` is unreachable.
                Some((resolved, refused)) = catalog_rx.recv() => {
                    self.awaiting_catalog = false;
                    self.apply_resolution(resolved, refused);
                }
                registered = self.symbol_rx.recv(), if !registrations_closed => {
                    match registered {
                        Ok(symbol) => {
                            // A symbol registered after this stream connected: resolve its
                            // market so submits can address it. Without this arm `market_info`
                            // stays `None` and every submit is expired before signing — the
                            // zero-fills bug (§4.2, mirrors `public_stream`'s wake).
                            info!(%symbol, "A Lighter symbol was registered; resolving its market for the order path.");
                            // A fresh registration re-arms a symbol the venue had refused by
                            // name: the operator may have corrected a typo, or the market may
                            // have listed since.
                            self.rearm(&symbol);
                            self.start_resolution(&catalog_tx);
                        }
                        Err(RecvError::Lagged(missed)) => {
                            // A missed wake never loses a symbol: the shared set is authoritative
                            // and re-read here (§4.2). Which symbol was registered is exactly
                            // what was lost, so every verdict is re-armed rather than guessing.
                            warn!(missed, "Lighter order-path registration wake-ups were dropped; re-resolving the registered set.");
                            self.unlisted.clear();
                            self.start_resolution(&catalog_tx);
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

    /// Every registered symbol, tracked or not.
    fn registered_symbols(&self) -> Vec<String> {
        self.symbols.lock().unwrap().iter().cloned().collect()
    }

    /// Registered symbols with no catalog row in hand, minus the ones the venue has already
    /// given a listing verdict about ([`Self::unlisted`]).
    fn unresolved_symbols(&self) -> Vec<String> {
        let registered = self.registered_symbols();
        let manager = self.order_manager.lock().unwrap();
        registered
            .into_iter()
            .filter(|symbol| {
                manager.market_info(symbol).is_none() && !self.unlisted.contains(symbol)
            })
            .collect()
    }

    /// Un-remembers a listing verdict, so the symbol is asked about again.
    fn rearm(&mut self, symbol: &str) {
        self.unlisted.remove(symbol);
    }

    /// Re-reads the catalog for **every** registered symbol and re-tracks what it says. The
    /// connect-time resolve, awaited before the socket exists.
    ///
    /// In full, not "only what is missing", and that is the money-safety call rather than a
    /// missed optimisation. `market_id` cannot change under a running process, but
    /// `price_decimals` / `size_decimals` are the exponents every wire price and size is scaled
    /// by, and [`OrderManager::track_market`] is their only writer — so resolving only the
    /// untracked symbols freezes the grid a symbol was first seen on for the life of the
    /// **process**. A venue decimals migration would then mis-scale every order by a power of
    /// ten, silently, in a direction that is harmless one way and catastrophic the other. One
    /// REST call per reconnect does not buy that (`AGENTS.md` §7).
    ///
    /// Refusals are reported per symbol, not dropped (fold-1), and a refused symbol is left
    /// untracked so a later impulse can ask again.
    async fn resolve_all_registered(&mut self) {
        let registered = self.registered_symbols();
        if registered.is_empty() {
            return;
        }
        let (resolved, refused) = rest::resolve_symbols(&self.rest_url, &registered).await;
        self.apply_resolution(resolved, refused);
    }

    /// Asks the catalog about the still-unresolved symbols, **beside** the connection rather
    /// than in front of it.
    ///
    /// [`Self::serve`] awaits its `select!` arms one at a time, so a round trip awaited inside
    /// an arm stops the connection polling its read half for as long as the venue's REST takes
    /// — up to [`rest::CATALOG_TIMEOUT`] (15 s), and again on every impulse for as long as the
    /// symbol stays unresolved, because a transport failure is deliberately not a verdict. On
    /// **this** connection the read half carries fills, cancel acknowledgements and position
    /// updates, and the connector is shared: one bot registering a new symbol would otherwise
    /// freeze another bot's money path for fifteen seconds at a time. So the round trip is
    /// spawned and its answer arrives as an arm of the loop, exactly as
    /// `public_stream::start_resolution` does it.
    ///
    /// **At most one is out**, and the arm that *applies* an answer does not start the next one
    /// — a catalog that fails fast would then be retried as fast as it fails. The impulses are
    /// the keepalive tick and a registration wake-up, so a symbol that is waiting on an
    /// outstanding trip is asked about again within one [`PING_INTERVAL`].
    ///
    /// A panic inside the spawned task cannot wedge the flag: the connector's panic hook is
    /// `exit(1)` for every thread (`AGENTS.md` §4.7).
    fn start_resolution(&mut self, catalog_tx: &UnboundedSender<Resolution>) {
        if self.awaiting_catalog {
            return;
        }
        let symbols = self.unresolved_symbols();
        if symbols.is_empty() {
            return;
        }
        self.awaiting_catalog = true;
        let rest_url = self.rest_url.clone();
        let catalog_tx = catalog_tx.clone();
        tokio::spawn(async move {
            let resolution = rest::resolve_symbols(&rest_url, &symbols).await;
            // The receiver lives as long as the connection that started this; a send that fails
            // is a connection that has already dropped, and the next connect re-reads anyway.
            let _ = catalog_tx.send(resolution);
        });
    }

    /// Tracks what resolved and reports what did not, per symbol (fold-1, modelled on
    /// `public_stream::apply_resolution`). A refused symbol is surfaced as a
    /// [`ErrorKind::CriticalConnectionError`] — a symbol that resolves to no market can neither
    /// be served nor traded, the same class of failure the market-data side reports — and is
    /// left untracked. Split from the round trip so it is testable without a socket.
    ///
    /// A refusal that is a **listing verdict** is remembered ([`Self::unlisted`]) so later
    /// impulses skip it; anything else — a catalog that could not be read — is not a verdict
    /// and stays askable, because writing it off would cost every order on that symbol.
    fn apply_resolution(
        &mut self,
        resolved: Vec<MarketInfo>,
        refused: Vec<(String, LighterError)>,
    ) {
        {
            let mut manager = self.order_manager.lock().unwrap();
            for info in resolved {
                info!(
                    symbol = %info.symbol,
                    market_id = info.market_id,
                    "Tracked a Lighter market for the order path."
                );
                // A symbol that has just resolved is listed, whatever an earlier answer said.
                self.unlisted.remove(&info.symbol);
                manager.track_market(info);
            }
        }
        for (symbol, error) in &refused {
            let verdict = error.is_listing_verdict();
            if verdict {
                self.unlisted.insert(symbol.clone());
            }
            error!(
                %symbol,
                ?error,
                listing_verdict = verdict,
                "Refusing to track a Lighter symbol for the order path: it does not resolve to a \
                 market. Every submit for it is expired until it resolves; a catalog that could \
                 not be read is retried on the next keepalive tick, while a symbol the venue \
                 says is not listed is asked about again only if it is registered afresh."
            );
            publish_error(&self.ev_tx, ErrorKind::CriticalConnectionError, error);
        }
    }

    fn on_frame(&mut self, text: &str) {
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
                            deadline: Instant::now() + SWEEP_GATE_DEADLINE,
                        });
                    }
                    // Reconnect reconciliation: expire our orders the venue no longer lists,
                    // then adopt the ones it does (§3.4, §5.10).
                    let expired = self.order_manager.lock().unwrap().reconcile(&orders);
                    for (symbol, order) in expired {
                        publish_order(&self.ev_tx, &symbol, order);
                    }
                }
                // A status nobody here can read moves nothing (`apply_order_update`) and pages,
                // once per frame: the venue has started saying something about orders that this
                // process cannot vouch for.
                let unknown: Vec<&str> = orders
                    .iter()
                    .filter(|order| !status_is_known(&order.status))
                    .map(|order| order.status.as_str())
                    .collect();
                if !unknown.is_empty() {
                    error!(
                        ?unknown,
                        "Lighter reported order statuses this connector cannot read; those orders \
                         are left exactly as they were until a reconnect's snapshot settles them."
                    );
                    report_error(
                        &self.ev_tx,
                        ErrorKind::CriticalConnectionError,
                        &format!(
                            "unrecognised Lighter order status {unknown:?}; orders left as they were"
                        ),
                    );
                }
                for order in &orders {
                    let applied = self.order_manager.lock().unwrap().apply_order_update(order);
                    match applied {
                        ApplyOutcome::Publish((symbol, published)) => {
                            publish_order(&self.ev_tx, &symbol, published);
                            // The order went terminal for a reason the bot must hear about (a
                            // margin refusal, a liquidation): after the order, so its state is
                            // clean before the handler runs — the same order `reject_order`
                            // keeps.
                            if let Some((kind, message)) = venue_refusal(&order.status) {
                                warn!(
                                    %symbol,
                                    status = %order.status,
                                    "Lighter refused an order of ours."
                                );
                                report_error(&self.ev_tx, kind, &format!("{symbol}: {message}"));
                            }
                        }
                        ApplyOutcome::Ignored => {}
                        // Kept exactly as it was (`apply_order_update`), and already paged
                        // above — once per frame, naming every unreadable status in it, whether
                        // or not the order is ours.
                        ApplyOutcome::Unrecognised => {}
                    }
                }
                if snapshot && !self.account_stream_live.swap(true, Ordering::AcqRel) {
                    // The order state is current again: from here a fill is seen, so orders
                    // may go out ([`OrderPath::submit`]).
                    info!("The Lighter account stream is live; orders may be placed.");
                }
            }
            PrivateFrame::Positions { positions, .. } => {
                // NANOseconds: the unit every other backend publishes on
                // `LiveEvent::Position.exch_ts` (bybit and both Binance scale their
                // millisecond source by 1_000_000; Hyperliquid publishes nanoseconds). The
                // frame carries no venue stamp of its own, so this is the local receive time.
                let now = local_now();
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
            PrivateFrame::VenueError { code, message } => self.on_venue_error(code, &message),
            _ => {}
        }
    }
}

impl PrivateStreamTask {
    /// An error frame on the private socket, told to the bots (§4.17): an `OrderError` when it
    /// is about a transaction (`21xxx`), a `CriticalConnectionError` when it is about the
    /// account — auth, a subscription — or is a code not known to be about a transaction (fail
    /// closed; [`venue_error_scope`]). It used to be a `warn!` and nothing more, so a refused
    /// subscription or a rejected token left the order state uncurrent with no bot told why.
    ///
    /// The same code is told at most once every [`VENUE_ERROR_EVERY`], with how many it held
    /// back: a venue repeating itself — every subscription of a reconnect refused alike — must
    /// not trip a bot's breaker on one condition many times over. A different code is new
    /// information and goes straight through.
    fn on_venue_error(&mut self, code: i64, message: &str) {
        let now = Instant::now();
        let Some(held_back) = self
            .venue_errors
            .entry(code)
            .or_insert_with(|| LogThrottle::new(VENUE_ERROR_EVERY))
            .admit(now)
        else {
            debug!(
                code,
                %message,
                "A repeated Lighter private-stream error; not told to the bots again yet."
            );
            return;
        };
        let kind = match venue_error_scope(code) {
            VenueErrorScope::Order => ErrorKind::OrderError,
            VenueErrorScope::Account => ErrorKind::CriticalConnectionError,
        };
        error!(
            code,
            %message,
            ?kind,
            held_back,
            "The Lighter private stream returned an error frame; telling the bots."
        );
        report_error(
            &self.ev_tx,
            kind,
            &format!("Lighter private stream error {code}: {message}"),
        );
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
        sync::{
            Arc,
            Mutex,
            atomic::{AtomicBool, Ordering},
        },
        task::Poll,
        time::Duration,
    };

    use chrono::Utc;
    use futures_util::{Stream, stream};
    use hftbacktest::types::{
        ErrorKind,
        LiveEvent,
        OrdType,
        Order,
        Side,
        Status,
        TimeInForce,
        Value,
    };
    use serde::Deserialize;
    use tokio::{
        sync::{broadcast, mpsc::unbounded_channel},
        time,
    };
    use tokio_tungstenite::tungstenite::{Error as WsError, Message};

    use super::{
        AUTH_MAX_LIFETIME_S,
        AUTH_REFRESH_MARGIN_S,
        Command,
        ConnectPolicy,
        OrderPath,
        OrderPathConfig,
        PrivateStreamTask,
        SWEEP_GATE_DEADLINE,
        SWEEP_REPLY_WAIT,
        SlotTask,
        auth_refresh_after,
        clamp_auth_lifetime,
        markets_to_sweep_on_connect,
        subscription_frames,
        sweep_market,
    };
    use crate::{
        connector::{PublishEvent, SweepOutcome},
        lighter::{
            LighterError,
            fixtures::{
                PRIVATE_ERROR_AUTH_REQUIRED,
                PRIVATE_ORDERS_SNAPSHOT,
                PRIVATE_ORDERS_SNAPSHOT_EMPTY,
                PRIVATE_ORDERS_UPDATE_CANCELED,
                PRIVATE_POSITIONS_UPDATE_FLAT,
            },
            order::OrderManager,
            private_msg::{AccountOrder, AccountPosition},
            rest::{MarketInfo, SendOutcome, SendTxAck},
            signer::SignerNetwork,
            slot::{
                SWEEP_ATTEMPTS,
                testing::{ScriptedVenue, throttled},
            },
            test_venue::{ScriptedRestVenue, SidecarIdentity, fake_signer},
        },
        supervision::{DrainOutcome, StopKind, StopReport, exit_code},
        utils::testing::{RecordingSink, closed_read, read_after_connect},
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
    /// restarted-process state — returning the registration sender and the command and publish
    /// receivers to inspect.
    ///
    /// The sender comes back rather than being leaked so a caller can register a symbol the way
    /// `Connector::register` does; holding it also keeps the broadcast open, so the wake arm
    /// never sees the channel close.
    fn stream_task(
        policy: ConnectPolicy,
    ) -> (
        PrivateStreamTask,
        broadcast::Sender<String>,
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
            tx_budget_per_minute: None,
            tx_reserve: None,
            rate_limit_cooldown_ms: None,
            rate_limit_cancel_probe_ms: None,
        };
        let (symbol_tx, symbol_rx) = broadcast::channel(16);
        let task = PrivateStreamTask {
            config,
            rest_url: String::new(),
            public_url: String::new(),
            symbols: Arc::new(Mutex::new(Default::default())),
            symbol_rx,
            unlisted: HashSet::new(),
            awaiting_catalog: false,
            order_manager: Arc::new(Mutex::new(OrderManager::new(1))),
            command_tx,
            ev_tx,
            account_stream_live: Arc::new(AtomicBool::new(false)),
            venue_errors: Default::default(),
        };
        (task, symbol_tx, command_rx, ev_rx)
    }

    /// A read half that is **silent on its first two polls** and hands over `frames` after
    /// that, running `script` on the second.
    ///
    /// The silence is the point. `time::interval` fires its first tick immediately, so the
    /// connection's own timer arms are ready the moment `serve` starts; a read half that had a
    /// frame waiting would race them, and `select!` picks at random. Staying quiet on the first
    /// poll lets the timers go first, and running `script` on the second — then still returning
    /// `Pending` — lets the arm the script wakes be served **before** the frame arrives. What
    /// the loop did with the frame is then a fact about the loop, not about a coin flip.
    ///
    /// It wakes itself while silent on the first poll, so a loop with nothing else ready comes
    /// back to it instead of parking until the next keepalive.
    fn read_after_the_first_poll<F>(
        script: F,
        frames: Vec<String>,
    ) -> impl Stream<Item = Result<Message, WsError>> + Unpin
    where
        F: FnOnce(),
    {
        let mut polls = 0usize;
        let mut script = Some(script);
        let mut frames = frames.into_iter();
        stream::poll_fn(move |cx| {
            polls += 1;
            match polls {
                1 => {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                2 => {
                    if let Some(script) = script.take() {
                        script();
                    }
                    // No self-wake: whatever the script did (a registration broadcast) wakes the
                    // arm that is waiting for it, and that arm must run before the frame below.
                    Poll::Pending
                }
                _ => match frames.next() {
                    Some(frame) => Poll::Ready(Some(Ok(Message::Text(frame.into())))),
                    // Never ends: these tests assert on what the loop *did*, not on how it
                    // stopped, and a read half that ended would end the loop with it.
                    None => Poll::Pending,
                },
            }
        })
    }

    /// A REST endpoint that accepts a connection and never answers it.
    ///
    /// The only shape that can starve anything: a refusal comes back in milliseconds, so a
    /// connector that awaits its catalog inline looks healthy against one.
    async fn never_answering_endpoint() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                held.push(socket);
            }
        });
        format!("http://{address}")
    }

    /// **The catalog round trip runs beside the private read loop, never in front of it.**
    ///
    /// `serve` awaits its `select!` arms one at a time, so a resolve awaited *inline* on the
    /// registration wake stops the connection polling its read half for as long as the venue's
    /// REST takes — up to [`crate::lighter::rest::CATALOG_TIMEOUT`] (15 s), and again on every
    /// impulse for as long as the symbol stays unresolved. On this connection that read half
    /// carries **fills, cancel acknowledgements and position updates**: a bot registering a new
    /// symbol would freeze another bot's money path for fifteen seconds at a time, on a shared
    /// connector, with nothing in the logs. `public_stream` already refuses to do this
    /// (`start_resolution`); so must this one.
    ///
    /// Driven against an endpoint that accepts and never answers, with the registration
    /// delivered by the read half itself so the wake arm is served **before** the frame — the
    /// frame is therefore waiting while the round trip is outstanding, which is the whole
    /// question.
    #[tokio::test]
    async fn a_resolve_in_flight_never_stalls_the_private_read_half() {
        // `reqwest` builds its rustls backend when the client is built; a test has no `main` to
        // install the process-level provider. Idempotent (`AGENTS.md` §4.7).
        crate::install_crypto_provider();
        let rest_url = never_answering_endpoint().await;
        let (mut task, symbol_tx, mut command_rx, _ev_rx) = stream_task(ConnectPolicy::CancelAll);
        task.rest_url = rest_url;

        let symbols = task.symbols.clone();
        let mut sink = RecordingSink::<LighterError>::default();
        let mut read = read_after_the_first_poll(
            move || {
                // Exactly what `Connector::register` does: the authoritative set first, then
                // the wake-up.
                symbols.lock().unwrap().insert("ETH".to_string());
                symbol_tx.send("ETH".to_string()).unwrap();
            },
            vec![PRIVATE_ORDERS_SNAPSHOT.to_string()],
        );
        let _ = time::timeout(
            Duration::from_millis(500),
            task.serve(&mut sink, &mut read, AUTH_MAX_LIFETIME_S),
        )
        .await;

        // The snapshot names one market, and under `cancel_all` applying it emits one
        // `CancelAllOrders` — an observable that only exists if the frame was read.
        match command_rx.try_recv() {
            Ok(Command::CancelAll { market_index, .. }) => assert_eq!(market_index, 1),
            _ => panic!(
                "the private frame waiting on the read half was not applied while the catalog \
                 round trip was outstanding: the resolve is being awaited inline, so every fill, \
                 cancel ack and position update on this connection waits out the venue's REST"
            ),
        }
    }

    /// **Every connect re-reads the catalog and re-tracks what it says** (§3.5, `AGENTS.md` §7).
    ///
    /// `price_decimals` / `size_decimals` are the exponents every wire price and size is scaled
    /// by (`price * 10^price_decimals`). Resolving only the *not-yet-tracked* symbols freezes
    /// them for the life of the **process**, because `track_market` is the only writer: a venue
    /// that migrates a market's decimals is then mis-scaled by a power of ten on every order —
    /// harmless in one direction, instantly catastrophic in the other. One saved REST call does
    /// not buy that (§7: money-safety above performance), so the connect asks about every
    /// registered symbol, tracked or not.
    ///
    /// Driven through the real `connect`, with the command receiver dropped so the mint that
    /// follows the resolve fails and stops it — no socket, no token, and the one thing under
    /// test already done.
    #[tokio::test]
    async fn every_connect_re_reads_the_decimals_the_venue_publishes() {
        crate::install_crypto_provider();
        let rest_url = catalog_server(
            r#"{"order_books":[{"symbol":"ETH","market_id":42,"status":"active","supported_price_decimals":3,"supported_size_decimals":4}]}"#,
        )
        .await;
        let (mut task, _symbol_tx, command_rx, _ev_rx) = stream_task(ConnectPolicy::Reconcile);
        task.rest_url = rest_url;
        task.symbols.lock().unwrap().insert("ETH".to_string());
        // Tracked on an earlier connection, with the decimals of that day.
        task.order_manager.lock().unwrap().track_market(MarketInfo {
            symbol: "ETH".to_string(),
            market_id: 42,
            price_decimals: 2,
            size_decimals: 4,
            ..Default::default()
        });

        drop(command_rx);
        task.connect()
            .await
            .expect_err("no slot task, so the auth mint fails and the connect stops there");

        assert_eq!(
            task.order_manager
                .lock()
                .unwrap()
                .market_info("ETH")
                .unwrap()
                .price_decimals,
            3,
            "a reconnect must re-read the decimals the venue now publishes; a tracked symbol \
             whose grid the process never re-reads mis-scales every order by a power of ten"
        );
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
        let (mut task, _symbol_tx, mut command_rx, _ev_rx) = stream_task(ConnectPolicy::CancelAll);
        task.on_frame(PRIVATE_ORDERS_SNAPSHOT);
        match command_rx.try_recv() {
            Ok(Command::CancelAll {
                market_index,
                reply,
                ..
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
        let (mut task, _symbol_tx, mut command_rx, _ev_rx) = stream_task(ConnectPolicy::Reconcile);
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
        let (mut task, _symbol_tx, mut command_rx, _ev_rx) = stream_task(ConnectPolicy::CancelAll);
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
        let (mut task, _symbol_tx, _command_rx, mut ev_rx) = stream_task(ConnectPolicy::Reconcile);
        // The connect resolves the catalog; here, track BTC (market 1) so the position frame
        // (which carries only the market index) maps back onto the registered symbol.
        task.order_manager.lock().unwrap().track_market(MarketInfo {
            symbol: "BTC".to_string(),
            market_id: 1,
            price_decimals: 1,
            size_decimals: 5,
            ..Default::default()
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

    /// One of our orders, resting on BTC, as the private stream knows it — and the venue's
    /// real `canceled` frame for it, rewritten to `status`.
    fn our_order_canceled_as(
        status: &str,
    ) -> (
        PrivateStreamTask,
        tokio::sync::mpsc::UnboundedReceiver<PublishEvent>,
        String,
    ) {
        let (task, _symbol_tx, _command_rx, ev_rx) = stream_task(ConnectPolicy::Reconcile);
        let market = MarketInfo {
            symbol: "BTC".to_string(),
            market_id: 1,
            price_decimals: 1,
            size_decimals: 5,
            ..Default::default()
        };
        let coi = {
            let mut manager = task.order_manager.lock().unwrap();
            manager.track_market(market.clone());
            let order = Order::new(
                7,
                583_000,
                0.1,
                0.001,
                Side::Buy,
                OrdType::Limit,
                TimeInForce::GTX,
            );
            manager.new_order("BTC", &market, &order).unwrap().0
        };
        let frame = PRIVATE_ORDERS_UPDATE_CANCELED
            .replace("424242424242", &coi.to_string())
            .replace(r#""status":"canceled""#, &format!(r#""status":"{status}""#));
        (task, ev_rx, frame)
    }

    /// **A refusal the next quote will meet again reaches the bot as an error** — after the
    /// order, so the bot's state is clean when its handler runs. Before, a
    /// `canceled-margin-not-allowed` came back as `Status::Unsupported` and nothing else: the
    /// bot freed the level and re-quoted into the same refusal every tick, and its breaker
    /// never saw one error.
    #[test]
    fn a_margin_refusal_is_published_as_a_cancel_and_then_an_order_error() {
        let (mut task, mut ev_rx, frame) = our_order_canceled_as("canceled-margin-not-allowed");
        task.on_frame(&frame);
        match ev_rx.try_recv() {
            Ok(PublishEvent::LiveEvent(LiveEvent::Order { symbol, order })) => {
                assert_eq!(symbol, "BTC");
                assert_eq!(order.order_id, 7);
                assert_eq!(order.status, Status::Canceled);
            }
            _ => panic!("the order comes back first"),
        }
        match ev_rx.try_recv() {
            Ok(PublishEvent::LiveEvent(LiveEvent::Error(error))) => {
                assert_eq!(error.kind, ErrorKind::OrderError);
                let Value::String(message) = error.value() else {
                    panic!("prose");
                };
                assert!(message.contains("canceled-margin-not-allowed"), "{message}");
            }
            _ => panic!("then the refusal is reported"),
        }
    }

    /// A post-only bounce comes back as a cancel and nothing more: it is what post-only
    /// means, and an error per bounce would trip a maker's own breaker.
    #[test]
    fn a_post_only_bounce_is_published_as_a_cancel_and_nothing_else() {
        let (mut task, mut ev_rx, frame) = our_order_canceled_as("canceled-post-only");
        task.on_frame(&frame);
        match ev_rx.try_recv() {
            Ok(PublishEvent::LiveEvent(LiveEvent::Order { order, .. })) => {
                assert_eq!(order.status, Status::Canceled);
            }
            _ => panic!("the order comes back"),
        }
        assert!(ev_rx.try_recv().is_err(), "and no error follows a bounce");
    }

    /// **`LiveEvent::Position.exch_ts` is nanoseconds**, the unit every other backend writes:
    /// bybit and both Binance multiply their millisecond source by 1_000_000, Hyperliquid
    /// publishes nanoseconds outright. Lighter stamped it `Utc::now().timestamp_micros()` — a
    /// thousandfold under.
    ///
    /// It is inert today for the same reason the order stamp was: `main.rs`'s position cache
    /// only compares the field against itself and `LiveBot` ignores it entirely. That is
    /// exactly why it must be fixed in the same commit as `Order::exch_timestamp` — leaving one
    /// microsecond field on a backend whose sibling field just became nanoseconds is the
    /// mixed-unit trap itself, waiting for the first reader that compares the two.
    ///
    /// Bracketing the real clock is the pin: microseconds of now are ~1_000x below nanoseconds
    /// of now, so the wrong unit cannot land inside the bracket by accident.
    #[test]
    fn a_position_is_stamped_in_nanoseconds() {
        let (mut task, _symbol_tx, _command_rx, mut ev_rx) = stream_task(ConnectPolicy::Reconcile);
        task.order_manager.lock().unwrap().track_market(MarketInfo {
            symbol: "BTC".to_string(),
            market_id: 1,
            price_decimals: 1,
            size_decimals: 5,
            ..Default::default()
        });

        let before = Utc::now().timestamp_nanos_opt().unwrap();
        task.on_frame(PRIVATE_POSITIONS_UPDATE_FLAT);
        let after = Utc::now().timestamp_nanos_opt().unwrap();

        match ev_rx.try_recv() {
            Ok(PublishEvent::LiveEvent(LiveEvent::Position { exch_ts, .. })) => assert!(
                (before..=after).contains(&exch_ts),
                "the position stamp must be nanoseconds of now ({before}..={after}), got \
                 {exch_ts} — microseconds are ~1000x under and every other backend publishes ns"
            ),
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
        let (mut task, _symbol_tx, _command_rx, mut ev_rx) = stream_task(ConnectPolicy::Reconcile);

        task.apply_resolution(
            vec![MarketInfo {
                symbol: "ETH".to_string(),
                market_id: 42,
                price_decimals: 2,
                size_decimals: 4,
                ..Default::default()
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
        flaky_catalog_server(0, body).await
    }

    /// The same endpoint, with the first `refusals` requests answered `503` — a catalog that is
    /// down and then comes back, which is the failure the timed retry exists for.
    async fn flaky_catalog_server(refusals: usize, body: &'static str) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let mut served = 0usize;
            while let Ok((mut socket, _)) = listener.accept().await {
                // Read the request line and headers (a GET has no body), then answer.
                let mut buf = [0u8; 1024];
                let _ = socket.read(&mut buf).await;
                let response = if served < refusals {
                    "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: \
                     close\r\n\r\n"
                        .to_string()
                } else {
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: \
                         {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    )
                };
                served += 1;
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            }
        });
        format!("http://{address}")
    }

    /// Drives one connection for `window` of (virtual) time against a catalog that cannot be
    /// read, and counts the refusals it reported — one per symbol per attempt, so with one
    /// symbol registered this is the number of round trips the connection started.
    ///
    /// `already_refused_by_name` seeds the verdicts the venue is taken to have already given.
    ///
    /// The endpoint is an unreachable **URL**, not an unreachable host: it fails inside
    /// `reqwest` with no socket and no timer of its own, so the count is a fact about the
    /// connection's impulses rather than about how fast a network failed — which is what makes
    /// it safe to measure on a paused clock.
    async fn refusals_within(window: Duration, already_refused_by_name: &[&str]) -> usize {
        crate::install_crypto_provider();
        let (mut task, _symbol_tx, _command_rx, mut ev_rx) = stream_task(ConnectPolicy::Reconcile);
        task.rest_url = "lighter-catalog-that-cannot-be-reached".to_string();
        task.symbols.lock().unwrap().insert("ETH".to_string());
        for symbol in already_refused_by_name {
            task.unlisted.insert((*symbol).to_string());
        }

        let mut sink = RecordingSink::<LighterError>::default();
        let mut read = read_after_connect(|| {});
        let _ = time::timeout(
            window,
            task.serve(&mut sink, &mut read, AUTH_MAX_LIFETIME_S),
        )
        .await;

        let mut refusals = 0;
        while let Ok(event) = ev_rx.try_recv() {
            if matches!(event, PublishEvent::LiveEvent(LiveEvent::Error(_))) {
                refusals += 1;
            }
        }
        refusals
    }

    /// **A refused resolve is asked again — on the keepalive tick, and no faster.**
    ///
    /// A refusal was surfaced (`error!` + `CriticalConnectionError`) and then never retried:
    /// the loop's only impulses were a *new* `register()`, a socket drop and the ~7.5 h token
    /// refresh. So a catalog blip at connect, with every symbol already registered — a
    /// connector restart under a bot that is already running is exactly that — left the account
    /// unable to place a single order until one of those three happened, hours away. The other
    /// half matters just as much: a transport failure is deliberately not a verdict, so "ask
    /// again as soon as the failure comes back" would be a hot loop against a venue that is
    /// already unwell. The tick is both the retry and its bound.
    ///
    /// Measured in refusals reported, over two virtual windows: one shorter than a tick period,
    /// one spanning three more.
    #[tokio::test(start_paused = true)]
    async fn a_refused_catalog_is_retried_on_the_keepalive_tick_and_no_faster() {
        assert_eq!(
            refusals_within(Duration::from_secs(25), &[]).await,
            1,
            "one ask when the connection opens, and not another before the tick is due: a \
             catalog that refuses instantly must not be retried as fast as it refuses"
        );
        assert_eq!(
            refusals_within(Duration::from_secs(100), &[]).await,
            4,
            "and one on every keepalive tick after that (t = 0, 30, 60, 90 s) — without the \
             retry a refused symbol takes no orders until a new register(), a socket drop or \
             the ~7.5 h token refresh"
        );
    }

    /// **A symbol the venue has refused *by name* is not asked about again on every tick.**
    ///
    /// The other side of the retry. `UnknownSymbol` is a verdict — a typo in a bot's config, or
    /// a market that is listed and not `active` (18 of mainnet's 228 were, when this was
    /// written) — and it does not change while the process runs. Retried on the keepalive it
    /// would spend a REST call every thirty seconds for the life of the connector and never
    /// once succeed. A catalog that could not be *read* is not a verdict and must keep being
    /// asked, which is the assertion above; the two must not be treated alike in either
    /// direction.
    ///
    /// Same connection, same window, same unreachable catalog as the pin above — only the
    /// verdict differs, so the difference in the counts is the verdict and nothing else.
    #[tokio::test(start_paused = true)]
    async fn a_symbol_refused_by_name_is_not_asked_about_on_every_tick() {
        assert_eq!(
            refusals_within(Duration::from_secs(100), &["ETH"]).await,
            0,
            "a symbol the venue says is not listed must not be re-asked on every keepalive tick \
             for the life of the process"
        );
    }

    /// `symbols` is a `HashSet`, so anything derived from it comes out in whatever order it
    /// comes out in.
    fn sorted(mut symbols: Vec<String>) -> Vec<String> {
        symbols.sort();
        symbols
    }

    fn an_order() -> Order {
        Order::new(
            7,
            100,
            0.1,
            1.0,
            Side::Buy,
            OrdType::Limit,
            TimeInForce::GTC,
        )
    }

    /// **A submit for a market that has not resolved says exactly that, and never says "no live
    /// order".**
    ///
    /// This is the sentence the zero-fills bug shipped behind. The submit path borrowed
    /// [`crate::lighter::order::OrderError::OrderNotFound`]'s message — "no live order {id} on
    /// {symbol}" — for a market that had simply never been resolved, and that string is exactly
    /// what a downstream classifier (`myhft/src/classifiers/lighter.rs`) reads as a cancel that
    /// raced a fill and downgrades to benign. So every order the connector refused for the real
    /// reason was reported as harmless on the way past, and the operator saw a quiet, healthy
    /// bot placing nothing for hours.
    ///
    /// Driven through [`OrderPath::submit`] with an empty manager, because the reason rides
    /// `expire_unsent`'s payload: comparing the two `Display`s side by side would leave the
    /// call site free to go back to the old error without a single test noticing.
    #[test]
    fn a_submit_for_an_unresolved_market_reports_the_unresolved_market() {
        let (command_tx, _command_rx) = unbounded_channel();
        let (ev_tx, mut ev_rx) = unbounded_channel();
        let order_path = OrderPath {
            order_manager: Arc::new(Mutex::new(OrderManager::new(1))),
            command_tx,
            tasks: Vec::new(),
            account_stream_live: Arc::new(AtomicBool::new(false)),
        };

        order_path.submit("ETH", &an_order(), &ev_tx);

        // The order goes back first, so the bot's state is clean before an error handler that
        // may abort the `elapse` ever runs.
        let Ok(PublishEvent::LiveEvent(LiveEvent::Order { symbol, order })) = ev_rx.try_recv()
        else {
            panic!("an unsendable order must be expired back to the bot before the error");
        };
        assert_eq!(symbol, "ETH");
        assert_eq!(order.status, Status::Expired);

        let Ok(PublishEvent::LiveEvent(LiveEvent::Error(error))) = ev_rx.try_recv() else {
            panic!("the refusal must be reported");
        };
        let Value::String(message) = error.value else {
            panic!("the error payload carries the reason as a string");
        };
        assert!(
            message.contains("no resolved Lighter market"),
            "the refusal must name its real cause: {message}"
        );
        assert!(
            !message.contains("no live order"),
            "and must never wear the message a downstream classifier downgrades to benign: \
             {message}"
        );
    }

    /// **A listing verdict is remembered; a catalog that could not be read is not — and a fresh
    /// registration re-arms the verdict.**
    ///
    /// The unit-level counterpart of the two pins above, on the exact set the round trip asks
    /// about. Three properties, because getting any of them backwards is silent: forget the
    /// verdict and a typo re-asks for ever; remember the transport failure and every order on a
    /// perfectly good symbol is expired until something reconnects; refuse to re-arm and a
    /// corrected config needs a connector restart to take effect.
    #[test]
    fn a_listing_verdict_is_remembered_an_unreadable_catalog_is_not_and_registering_re_arms() {
        let (mut task, _symbol_tx, _command_rx, _ev_rx) = stream_task(ConnectPolicy::Reconcile);
        for symbol in ["ETH", "NOTACOIN"] {
            task.symbols.lock().unwrap().insert(symbol.to_string());
        }
        assert_eq!(
            sorted(task.unresolved_symbols()),
            ["ETH", "NOTACOIN"],
            "precondition: nothing is tracked and nothing has been refused"
        );

        task.apply_resolution(
            Vec::new(),
            vec![
                (
                    "NOTACOIN".to_string(),
                    LighterError::UnknownSymbol("NOTACOIN is not listed on Lighter".into()),
                ),
                (
                    "ETH".to_string(),
                    LighterError::CatalogUnavailable(
                        "couldn't read the Lighter catalog for ETH: 503".into(),
                    ),
                ),
            ],
        );

        assert_eq!(
            task.unresolved_symbols(),
            ["ETH"],
            "the venue's verdict is remembered and the transport failure is not"
        );
        assert_eq!(
            sorted(task.registered_symbols()),
            ["ETH", "NOTACOIN"],
            "the connect is not bound by the memory — it re-reads everything, which is how a \
             market that has since listed is picked up across a reconnect"
        );

        task.rearm("NOTACOIN");
        assert_eq!(
            sorted(task.unresolved_symbols()),
            ["ETH", "NOTACOIN"],
            "registering a symbol afresh asks about it again: the operator may have corrected a \
             typo, or the market may have listed since"
        );
    }

    /// **The recovery, end to end: a symbol refused at connect is resolved and tracked with no
    /// new registration and no reconnect.**
    ///
    /// The catalog is down for the connect-time resolve and up afterwards. Nothing else
    /// happens: no bot registers anything, the socket does not drop, the token does not
    /// refresh. Only time passes — and the symbol becomes addressable, which is the difference
    /// between a bot that quotes and a bot that has every order expired before signing.
    #[tokio::test]
    async fn a_symbol_refused_at_connect_is_resolved_without_a_registration_or_a_reconnect() {
        crate::install_crypto_provider();
        let rest_url = flaky_catalog_server(
            1,
            r#"{"order_books":[{"symbol":"ETH","market_id":42,"status":"active","supported_price_decimals":2,"supported_size_decimals":4}]}"#,
        )
        .await;
        let (mut task, _symbol_tx, _command_rx, _ev_rx) = stream_task(ConnectPolicy::Reconcile);
        task.rest_url = rest_url;
        task.symbols.lock().unwrap().insert("ETH".to_string());

        // The connect-time resolve, against a catalog that is down.
        task.resolve_all_registered().await;
        let order_manager = task.order_manager.clone();
        assert!(
            order_manager.lock().unwrap().market_info("ETH").is_none(),
            "precondition: the catalog refused, so nothing is tracked"
        );

        let mut sink = RecordingSink::<LighterError>::default();
        let mut read = read_after_connect(|| {});
        let _ = time::timeout(
            Duration::from_millis(500),
            task.serve(&mut sink, &mut read, AUTH_MAX_LIFETIME_S),
        )
        .await;

        assert!(
            order_manager.lock().unwrap().market_info("ETH").is_some(),
            "a symbol the catalog refused at connect must be resolved by the connection itself \
             once the catalog answers again — otherwise every order on it is expired before \
             signing until something unrelated happens to reconnect the socket"
        );
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
    ///
    /// The registration is delivered by the read half rather than queued up front, so the
    /// connection's own immediate keepalive tick runs first, against a still-empty registered
    /// set. Without that the tick would resolve ETH by itself and the pin would survive the
    /// mutation it exists for.
    #[tokio::test]
    async fn an_order_path_tracks_a_symbol_registered_after_connect() {
        // `reqwest` builds its rustls backend when the client is built; a test has no `main` to
        // install the process-level provider. Idempotent (`AGENTS.md` §4.7).
        crate::install_crypto_provider();
        let rest_url = catalog_server(
            r#"{"order_books":[{"symbol":"ETH","market_id":42,"status":"active","supported_price_decimals":2,"supported_size_decimals":4}]}"#,
        )
        .await;
        let (mut task, symbol_tx, _command_rx, _ev_rx) = stream_task(ConnectPolicy::Reconcile);
        task.rest_url = rest_url;

        // Post-connect state: ETH is not registered yet, so nothing is tracked.
        let order_manager = task.order_manager.clone();
        assert!(
            order_manager.lock().unwrap().market_info("ETH").is_none(),
            "precondition: ETH is not tracked before it is registered"
        );

        // Drive the connection loop. A large lifetime keeps the token refresh far out of the
        // window. Real time, so the local round trip runs; bounded so the (otherwise endless)
        // loop is stopped.
        let symbols = task.symbols.clone();
        let mut sink = RecordingSink::<LighterError>::default();
        let mut read = read_after_the_first_poll(
            move || {
                // The bot registers ETH after connect: the authoritative set gains it and the
                // wake-up fires — exactly what `Connector::register` does.
                symbols.lock().unwrap().insert("ETH".to_string());
                symbol_tx.send("ETH".to_string()).unwrap();
            },
            Vec::new(),
        );
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

    // ---- an order status nobody can read ------------------------------------------------

    /// **A status the connector cannot read pages and moves nothing.** The order stays as it
    /// was (the manager's pin) and the bots hear a `CriticalConnectionError` naming the status,
    /// because a venue that has started speaking a new word about orders is a venue whose
    /// order state this process can no longer vouch for.
    #[test]
    fn an_unrecognised_order_status_pages_and_publishes_nothing() {
        let (mut task, _symbol_tx, _command_rx, mut ev_rx) = stream_task(ConnectPolicy::Reconcile);
        task.order_manager.lock().unwrap().track_market(MarketInfo {
            symbol: "BTC".to_string(),
            market_id: 1,
            price_decimals: 1,
            size_decimals: 5,
            ..Default::default()
        });
        let order = Order::new(
            7,
            583_000,
            0.1,
            0.001,
            Side::Buy,
            OrdType::Limit,
            TimeInForce::GTX,
        );
        let market = task
            .order_manager
            .lock()
            .unwrap()
            .market_info("BTC")
            .unwrap();
        let (coi, _) = task
            .order_manager
            .lock()
            .unwrap()
            .new_order("BTC", &market, &order)
            .unwrap();
        let frame = PRIVATE_ORDERS_UPDATE_CANCELED
            .replace("424242424242", &coi.to_string())
            .replace(r#""status":"canceled""#, r#""status":"frozen-by-venue""#);

        task.on_frame(&frame);

        let Ok(PublishEvent::LiveEvent(LiveEvent::Error(error))) = ev_rx.try_recv() else {
            panic!("an unreadable status must page");
        };
        assert_eq!(error.kind, ErrorKind::CriticalConnectionError);
        let Value::String(message) = error.value else {
            panic!("the reason rides as a string");
        };
        assert!(message.contains("frozen-by-venue"), "{message}");
        assert!(ev_rx.try_recv().is_err(), "and nothing else is published");
        assert_eq!(task.order_manager.lock().unwrap().tracked(), 1);
    }

    // ---- the account stream gates submits ---------------------------------------------

    /// An order path over a resolved ETH market, with the account stream `live` or not.
    fn order_path_over_eth(
        live: bool,
    ) -> (
        OrderPath,
        tokio::sync::mpsc::UnboundedReceiver<Command>,
        tokio::sync::mpsc::UnboundedSender<PublishEvent>,
        tokio::sync::mpsc::UnboundedReceiver<PublishEvent>,
    ) {
        let (command_tx, command_rx) = unbounded_channel();
        let (ev_tx, ev_rx) = unbounded_channel();
        let order_manager = Arc::new(Mutex::new(OrderManager::new(1)));
        order_manager.lock().unwrap().track_market(MarketInfo {
            symbol: "ETH".to_string(),
            market_id: 42,
            price_decimals: 2,
            size_decimals: 4,
            ..Default::default()
        });
        let order_path = OrderPath {
            order_manager,
            command_tx,
            tasks: Vec::new(),
            account_stream_live: Arc::new(AtomicBool::new(live)),
        };
        (order_path, command_rx, ev_tx, ev_rx)
    }

    /// **No order is placed while the account stream is down.** Fills, cancels and the
    /// position all arrive on that one socket: an order sent while it is down fills unseen,
    /// the bot keeps its level "resting", re-quotes over it at the next requote, and sizes a
    /// risk-off flatten off a position that is no longer true. The public book keeps the bot
    /// believing all is well — its recovery machine resumes on the first depth frame — so the
    /// refusal has to be here. Refused before a nonce is spent, expired back first, and
    /// reported under its own name (never "no live order", which the bot's classifier reads
    /// as benign).
    #[test]
    fn a_submit_while_the_account_stream_is_down_is_expired_and_never_sent() {
        let (order_path, mut command_rx, ev_tx, mut ev_rx) = order_path_over_eth(false);

        order_path.submit("ETH", &an_order(), &ev_tx);

        assert!(
            command_rx.try_recv().is_err(),
            "nothing may reach the slot while the account stream is down"
        );
        let Ok(PublishEvent::LiveEvent(LiveEvent::Order { order, .. })) = ev_rx.try_recv() else {
            panic!("the refused order is expired back first");
        };
        assert_eq!(order.status, Status::Expired);
        let Ok(PublishEvent::LiveEvent(LiveEvent::Error(error))) = ev_rx.try_recv() else {
            panic!("then reported");
        };
        assert_eq!(error.kind, ErrorKind::OrderError);
        let Value::String(message) = error.value else {
            panic!("the reason rides as a string");
        };
        assert!(message.contains("account stream"), "{message}");
        assert!(!message.contains("no live order"), "{message}");
        assert_eq!(
            order_path.order_manager.lock().unwrap().tracked(),
            0,
            "a refused order is not left tracked — its id would be refused for ever"
        );

        // The witness: the same order with the stream live goes to the slot.
        order_path.account_stream_live.store(true, Ordering::SeqCst);
        order_path.submit("ETH", &an_order(), &ev_tx);
        assert!(
            matches!(command_rx.try_recv(), Ok(Command::Submit { .. })),
            "with the stream live the order is queued for signing"
        );
    }

    /// The stream is live once THIS connection's `account_all_orders` snapshot has arrived —
    /// the moment the order state and the reconciliation are current — and not on a bare
    /// update.
    #[test]
    fn the_account_stream_is_live_only_once_this_connection_has_snapshotted() {
        let (mut task, _symbol_tx, _command_rx, _ev_rx) = stream_task(ConnectPolicy::CancelAll);
        task.on_frame(PRIVATE_ORDERS_UPDATE_CANCELED);
        assert!(
            !task.account_stream_live.load(Ordering::SeqCst),
            "an update is not a snapshot"
        );
        task.on_frame(PRIVATE_ORDERS_SNAPSHOT_EMPTY);
        assert!(task.account_stream_live.load(Ordering::SeqCst));
    }

    /// ...and it stops being live the moment the connection ends, whatever ended it.
    #[tokio::test]
    async fn a_connection_that_ended_is_not_live() {
        let (mut task, _symbol_tx, _command_rx, _ev_rx) = stream_task(ConnectPolicy::CancelAll);
        task.account_stream_live.store(true, Ordering::SeqCst);
        let mut sink = RecordingSink::<LighterError>::default();
        let mut read = closed_read();

        let ended = task.serve(&mut sink, &mut read, AUTH_MAX_LIFETIME_S).await;

        assert!(ended.is_err(), "the read half closed");
        assert!(
            !task.account_stream_live.load(Ordering::SeqCst),
            "a dropped connection must not leave submits open"
        );
    }

    // ---- a sweep's verdict reaches the exit code ---------------------------------------

    /// Drives one supervision sweep of market 42 the way the slot serves it: `OrderPath::sweep`
    /// asks for a `CancelAllOrders`, and the slot's own handler ([`sweep_market`]) answers it
    /// against `venue`. Returns the folded outcome, the exit code an orderly stop would take
    /// on it, and what the bots were told.
    async fn stop_after_sweeping_against(
        venue: &mut ScriptedVenue,
    ) -> (SweepOutcome, i32, Vec<PublishEvent>) {
        let (order_path, mut command_rx, _ev_tx, _ev_rx) = order_path_over_eth(true);
        let sweep = order_path.sweep(vec![42]);
        let Some(Command::CancelAll {
            market_index,
            reply,
            ..
        }) = command_rx.recv().await
        else {
            panic!("a sweep asks the slot for one CancelAllOrders per market");
        };
        assert_eq!(market_index, 42);

        let (ev_tx, mut ev_rx) = unbounded_channel();
        sweep_market(venue, &ev_tx, market_index, reply).await;
        let outcome = sweep.await.expect("the sweep task does not panic");
        let code = exit_code(&StopReport {
            kind: StopKind::Orderly,
            sweep: Some(outcome),
            drain: Some(DrainOutcome::SendersDropped { published: 0 }),
        });
        let mut told = Vec::new();
        while let Ok(event) = ev_rx.try_recv() {
            told.push(event);
        }
        (outcome, code, told)
    }

    /// **A Lighter sweep whose `CancelAllOrders` failed every retry stops the connector
    /// non-zero.** The whole chain, with only the signer and the REST call replaced by a venue
    /// that throttles every attempt: the slot's handler retries `SWEEP_ATTEMPTS` times, answers
    /// `false`, `OrderPath::sweep` folds that into `SweepOutcome::Failed`, and the stop's exit
    /// code is 1 — the supervisor sees a stop that left orders resting, rather than the exit 0
    /// it got while the reply carried no verdict. The bots hear a `CriticalConnectionError`.
    ///
    /// The witness below it is the same chain against a venue that takes the sweep: exit 0.
    #[tokio::test(start_paused = true)]
    async fn a_lighter_sweep_whose_cancel_all_never_took_stops_the_connector_non_zero() {
        let mut venue = ScriptedVenue::new((0..10).map(|_| Ok(throttled())).collect());

        let (outcome, code, told) = stop_after_sweeping_against(&mut venue).await;

        assert_eq!(venue.sends, SWEEP_ATTEMPTS, "every retry was spent");
        assert_eq!(outcome, SweepOutcome::Failed);
        assert_eq!(code, 1, "a stop whose sweep never took must not exit 0");
        assert!(
            told.iter().any(|event| matches!(
                event,
                PublishEvent::LiveEvent(LiveEvent::Error(error))
                    if error.kind == ErrorKind::CriticalConnectionError
            )),
            "the bots are told the market may still hold orders"
        );

        let mut venue = ScriptedVenue::new(vec![Ok(SendOutcome::Accepted(SendTxAck {
            tx_hash: "ab".to_string(),
            ratelimit: None,
            predicted_execution_time_ms: None,
        }))]);
        let (outcome, code, _) = stop_after_sweeping_against(&mut venue).await;
        assert_eq!(outcome, SweepOutcome::Cancelled);
        assert_eq!(code, 0, "witness: a sweep the venue took is a clean stop");
    }

    /// **A sweep the slot cannot even be asked for is a failed sweep** — the slot task is gone
    /// (its command channel closed), so nothing will cancel that market's orders; a stop that
    /// read it as done would exit 0 over a resting grid. Nothing to sweep is done, though.
    #[tokio::test]
    async fn a_sweep_the_slot_cannot_be_asked_for_is_failed() {
        let (order_path, command_rx, _ev_tx, _ev_rx) = order_path_over_eth(true);
        drop(command_rx);
        assert_eq!(
            order_path.sweep(vec![42]).await.unwrap(),
            SweepOutcome::Failed
        );
        assert_eq!(
            order_path.sweep(Vec::new()).await.unwrap(),
            SweepOutcome::Cancelled,
            "no market to sweep is nothing left resting"
        );
    }

    /// **A sweep the slot never answers is failed once its wait is up — and not before.** A
    /// wedged slot must not hang the stop, and a verdict that never came is not a cancel.
    #[tokio::test(start_paused = true)]
    async fn a_sweep_the_slot_never_answers_is_failed_after_its_wait() {
        let (order_path, mut command_rx, _ev_tx, _ev_rx) = order_path_over_eth(true);
        let started = time::Instant::now();
        let sweep = order_path.sweep(vec![42]);
        let Some(Command::CancelAll { reply, .. }) = command_rx.recv().await else {
            panic!("the sweep asks the slot");
        };
        // Held, never answered: the slot is alive and silent.
        let _unanswered = reply;
        assert_eq!(sweep.await.unwrap(), SweepOutcome::Failed);
        assert_eq!(
            started.elapsed(),
            SWEEP_REPLY_WAIT,
            "it waited exactly its wait"
        );
    }

    /// **The connect-time sweep is held to the sweep's own deadline.** Under `cancel_all` a
    /// (re)connect's `CancelAllOrders` waits for a shut send gate no longer than any sweep —
    /// 15 s from when it was asked for — so a cooldown it cannot outwait fails it loudly rather
    /// than parking the slot's queue behind it.
    #[tokio::test(start_paused = true)]
    async fn the_connect_sweep_waits_for_the_gate_no_longer_than_any_sweep() {
        assert_eq!(SWEEP_GATE_DEADLINE, Duration::from_secs(15));
        let (mut task, _symbol_tx, mut command_rx, _ev_rx) = stream_task(ConnectPolicy::CancelAll);
        let asked = time::Instant::now();
        task.on_frame(PRIVATE_ORDERS_SNAPSHOT);
        let Ok(Command::CancelAll { deadline, .. }) = command_rx.try_recv() else {
            panic!("cancel_all sweeps the market the snapshot lists");
        };
        assert_eq!(deadline, asked + SWEEP_GATE_DEADLINE);
    }

    // ---- the slot, end to end against a scripted venue ---------------------------------
    //
    // The slot task, with its signer replaced by an in-process stand-in that signs nothing
    // (`test_venue::fake_signer`) and the venue's REST by a local server that answers from a
    // script and records every request (`test_venue::ScriptedRestVenue`). Real time, not a
    // paused clock: the requests are real local HTTP, and a paused clock would fire the
    // client's own timeouts while a request is in flight.

    /// A slot serving ETH (market 42) on a scripted venue, and the `OrderPath` handle in front
    /// of it — the same pair `OrderPath::spawn` builds, minus the private stream (the account
    /// stream is taken as live).
    struct SlotOnScriptedVenue {
        order_path: OrderPath,
        ev_tx: tokio::sync::mpsc::UnboundedSender<PublishEvent>,
        events: tokio::sync::mpsc::UnboundedReceiver<PublishEvent>,
        venue: ScriptedRestVenue,
    }

    /// The `[order_path]` the scripted slot runs with before a test tweaks it: no budget, no
    /// cooldown, a confirmation deadline far outside any test.
    fn scripted_config() -> OrderPathConfig {
        OrderPathConfig {
            account_index: 516,
            api_key_index: 5,
            signer_python: String::new(),
            signer_script: String::new(),
            signer_config: String::new(),
            confirm_deadline_ms: 60_000,
            connect_policy: ConnectPolicy::Reconcile,
            tx_budget_per_minute: None,
            tx_reserve: None,
            rate_limit_cooldown_ms: None,
            rate_limit_cancel_probe_ms: None,
        }
    }

    async fn slot_on_scripted_venue(
        tweak: impl FnOnce(&mut OrderPathConfig),
    ) -> SlotOnScriptedVenue {
        crate::install_crypto_provider();
        let venue = ScriptedRestVenue::start(7).await;
        let mut config = scripted_config();
        tweak(&mut config);
        let order_manager = Arc::new(Mutex::new(OrderManager::new(1)));
        order_manager.lock().unwrap().track_market(MarketInfo {
            symbol: "ETH".to_string(),
            market_id: 42,
            price_decimals: 2,
            size_decimals: 4,
            ..Default::default()
        });
        let (ev_tx, events) = unbounded_channel();
        let (command_tx, command_rx) = unbounded_channel();
        let signer = fake_signer(SidecarIdentity {
            url: venue.url.clone(),
            chain_id: 466_324,
            account_index: config.account_index,
            api_key_index: config.api_key_index,
        });
        let slot = SlotTask {
            config,
            network: SignerNetwork {
                url: venue.url.clone(),
                chain_id: Some(466_324),
            },
            rest_url: venue.url.clone(),
            order_manager: order_manager.clone(),
            ev_tx: ev_tx.clone(),
        };
        tokio::spawn(slot.serve(signer, command_rx));
        let order_path = OrderPath {
            order_manager,
            command_tx,
            tasks: Vec::new(),
            account_stream_live: Arc::new(AtomicBool::new(true)),
        };
        SlotOnScriptedVenue {
            order_path,
            ev_tx,
            events,
            venue,
        }
    }

    impl SlotOnScriptedVenue {
        fn submit(&self, order: &Order) {
            self.order_path.submit("ETH", order, &self.ev_tx);
        }

        fn cancel(&self, order: &Order) {
            self.order_path.cancel("ETH", order, &self.ev_tx);
        }

        /// What the account stream would do once the venue listed `order_id` as resting: the
        /// order is confirmed and has the venue index a cancel is addressed by.
        fn confirm(&self, order_id: u64) {
            let mut manager = self.order_path.order_manager.lock().unwrap();
            let coi = manager.coi_for("ETH", order_id).expect("tracked");
            let applied = manager.apply_order_update(&AccountOrder {
                market_index: 42,
                client_order_index: coi,
                order_index: 900_000 + coi,
                is_ask: false,
                status: "open".to_string(),
                price: 1_000.0,
                initial_base_amount: 0.01,
                remaining_base_amount: 0.01,
                filled_base_amount: 0.0,
                transaction_time_us: 1_000,
            });
            assert!(applied.published().is_some(), "confirmed");
        }

        /// What the account stream would do on an `account_all_positions` frame.
        fn state_position(&self, position: f64) {
            self.order_path
                .order_manager
                .lock()
                .unwrap()
                .apply_position(
                    &AccountPosition {
                        market_index: 42,
                        symbol: "ETH".to_string(),
                        position,
                        open_order_count: 0,
                    },
                    0,
                );
        }

        /// Waits for the next event the bots would receive.
        async fn next_event(&mut self) -> PublishEvent {
            time::timeout(Duration::from_secs(5), self.events.recv())
                .await
                .expect("an event within 5 s")
                .expect("the channel is open")
        }

        /// Waits until `done` holds of what the venue has seen.
        async fn venue_until(&self, done: impl Fn(&ScriptedRestVenue) -> bool) {
            let deadline = time::Instant::now() + Duration::from_secs(5);
            while !done(&self.venue) {
                assert!(
                    time::Instant::now() < deadline,
                    "the venue never saw it: {:?}",
                    self.venue.seen()
                );
                time::sleep(Duration::from_millis(10)).await;
            }
        }
    }

    fn eth_order(order_id: u64, side: Side) -> Order {
        Order::new(
            order_id,
            100_000,
            0.01,
            0.01,
            side,
            OrdType::Limit,
            TimeInForce::GTX,
        )
    }

    /// The same order as an IOC — what a flatten sends.
    fn eth_ioc(order_id: u64, side: Side) -> Order {
        let mut order = eth_order(order_id, side);
        order.time_in_force = TimeInForce::IOC;
        order
    }

    /// The error the bots receive next, as `(kind, prose)`.
    fn as_error(event: PublishEvent) -> (ErrorKind, String) {
        let PublishEvent::LiveEvent(LiveEvent::Error(error)) = event else {
            panic!("an error was expected");
        };
        let Value::String(message) = error.value else {
            panic!("the reason rides as a string");
        };
        (error.kind, message)
    }

    /// **The harness, on the wiring it must reproduce: a submit goes out once, signed with the
    /// nonce the slot seeded from `nextNonce`; a `21104` expires it back and re-reads the
    /// counter before the next send.** Nothing new here — this is what the slot already does —
    /// and it is pinned first so that the pins built on this harness stand on a harness that is
    /// known to see the slot's real traffic.
    #[tokio::test]
    async fn the_scripted_slot_sends_with_the_seeded_nonce_and_resyncs_on_21104() {
        let mut slot = slot_on_scripted_venue(|_| {}).await;

        slot.submit(&eth_order(1, Side::Buy));
        slot.venue_until(|venue| venue.sent().len() == 1).await;
        assert_eq!(slot.venue.sent(), [("create_order".to_string(), 7)]);
        assert_eq!(slot.venue.count("/api/v1/nextNonce"), 1, "seeded once");

        slot.venue.answer_send_tx_with([(
            400,
            r#"{"code":21104,"message":"invalid nonce"}"#.to_string(),
        )]);
        slot.submit(&eth_order(2, Side::Buy));
        let PublishEvent::LiveEvent(LiveEvent::Order { order, .. }) = slot.next_event().await
        else {
            panic!("the refused order comes back first");
        };
        assert_eq!((order.order_id, order.status), (2, Status::Expired));
        let (kind, message) = as_error(slot.next_event().await);
        assert_eq!(kind, ErrorKind::OrderError);
        assert!(message.contains("21104"), "{message}");
        assert_eq!(
            slot.venue.count("/api/v1/nextNonce"),
            2,
            "a 21104 re-reads the counter"
        );
        assert_eq!(slot.venue.sent()[1], ("create_order".to_string(), 8));
    }

    // ---- the client-side sendTx budget (§4.15) -----------------------------------------

    /// **A quote over the send budget is handed back unsent — exactly as one refused while the
    /// account stream is down — and spends no nonce.** The order comes back `Expired` first, then
    /// an `OrderError` that the bot's classifier counts toward its rate-limit breaker (any
    /// `OrderError` but "no live order" is `Soft`, myhft `classifiers/lighter.rs`). Nothing
    /// reaches the venue for it: no `sendTx`, no `nextNonce`; the nonce it did not take is the
    /// one the next admitted send goes out with.
    #[tokio::test]
    async fn a_quote_over_the_send_budget_is_handed_back_unsent_and_spends_no_nonce() {
        let mut slot = slot_on_scripted_venue(|config| {
            config.tx_budget_per_minute = Some(2);
            config.tx_reserve = Some(1);
        })
        .await;
        slot.submit(&eth_order(1, Side::Buy));
        slot.venue_until(|venue| venue.sent().len() == 1).await;

        slot.submit(&eth_order(2, Side::Buy));
        let PublishEvent::LiveEvent(LiveEvent::Order { order, .. }) = slot.next_event().await
        else {
            panic!("the refused order comes back first");
        };
        assert_eq!((order.order_id, order.status), (2, Status::Expired));
        let (kind, message) = as_error(slot.next_event().await);
        assert_eq!(kind, ErrorKind::OrderError);
        assert!(message.contains("rate-limited"), "{message}");
        assert!(message.contains("budget"), "{message}");
        assert!(!message.contains("no live order"), "{message}");
        assert!(
            message.starts_with("Frame: ETH: "),
            "names its symbol: {message}"
        );

        // The witness that it reserved no nonce: the next admitted send — an IOC sell that
        // shrinks a long, so it may take the reserve — goes out with the very next nonce.
        slot.state_position(0.05);
        slot.submit(&eth_ioc(3, Side::Sell));
        slot.venue_until(|venue| venue.sent().len() == 2).await;
        assert_eq!(
            slot.venue.sent(),
            [
                ("create_order".to_string(), 7),
                ("create_order".to_string(), 8)
            ]
        );
        assert_eq!(
            slot.venue.count("/api/v1/nextNonce"),
            1,
            "and read no nonce"
        );
    }

    /// **The reserve is for getting out, and a quote is not a way out.** With 3 a minute and 1
    /// held back: two quotes spend the open part. A post-only ask against the long is a QUOTE —
    /// refused, not handed the reserve: on the 2026-10-08 probe three such asks took the whole
    /// reserve between them and the next cancel, of the bid that was opening the account up,
    /// was refused. An IOC sell no larger than the long goes, on the reserve. Then the budget is
    /// spent through, and a cancel is handed back to the bot as a cancel that did not go — its
    /// request cleared, the order still live — with the reason.
    #[tokio::test]
    async fn the_reserve_is_kept_for_what_gets_the_account_out() {
        let mut slot = slot_on_scripted_venue(|config| {
            config.tx_budget_per_minute = Some(3);
            config.tx_reserve = Some(1);
        })
        .await;
        let first = eth_order(1, Side::Buy);
        slot.submit(&first);
        slot.submit(&eth_order(2, Side::Buy));
        slot.venue_until(|venue| venue.sent().len() == 2).await;

        slot.state_position(0.05);
        for (order_id, side) in [(3, Side::Buy), (4, Side::Sell)] {
            slot.submit(&eth_order(order_id, side));
            let PublishEvent::LiveEvent(LiveEvent::Order { order, .. }) = slot.next_event().await
            else {
                panic!("a quote into the reserve is refused, whichever side it rests on");
            };
            assert_eq!((order.order_id, order.status), (order_id, Status::Expired));
            let (kind, message) = as_error(slot.next_event().await);
            assert_eq!(kind, ErrorKind::OrderError);
            assert!(message.contains("sendTx budget is spent"), "{message}");
        }

        slot.submit(&eth_ioc(5, Side::Sell));
        slot.venue_until(|venue| venue.sent().len() == 3).await;

        // Spent through, reserve included: a cancel goes back to the bot as one that did not go.
        slot.confirm(1);
        slot.cancel(&first);
        let PublishEvent::LiveEvent(LiveEvent::Order { order, .. }) = slot.next_event().await
        else {
            panic!("the cancel comes back first");
        };
        assert_eq!(order.order_id, 1);
        assert_eq!(order.status, Status::New, "the order is still live");
        assert_eq!(order.req, Status::None, "and no cancel is pending on it");
        let (kind, message) = as_error(slot.next_event().await);
        assert_eq!(kind, ErrorKind::OrderError);
        assert!(message.contains("reserve"), "{message}");
        assert_eq!(slot.venue.sent().len(), 3, "the cancel never went out");
    }

    /// **Every `sendTx` spends the budget: cancels and sweeps too.** Unmeasured whether the venue
    /// counts cancels (§6.Б.1), so the connector does — the conservative reading. With 3 a
    /// minute: a quote, its cancel and a sweep spend it, and the next quote is refused.
    #[tokio::test]
    async fn every_send_spends_the_budget_cancels_and_sweeps_included() {
        let mut slot = slot_on_scripted_venue(|config| {
            config.tx_budget_per_minute = Some(3);
        })
        .await;
        let quote = eth_order(1, Side::Buy);
        slot.submit(&quote);
        slot.venue_until(|venue| venue.sent().len() == 1).await;
        slot.confirm(1);
        slot.cancel(&quote);
        slot.venue_until(|venue| venue.sent().len() == 2).await;
        let sweep = slot.order_path.sweep(vec![42]);
        assert_eq!(sweep.await.unwrap(), SweepOutcome::Cancelled);
        assert_eq!(
            slot.venue
                .sent()
                .into_iter()
                .map(|(op, _)| op)
                .collect::<Vec<_>>(),
            ["create_order", "cancel_order", "cancel_all"]
        );

        slot.submit(&eth_order(2, Side::Buy));
        loop {
            if let PublishEvent::LiveEvent(LiveEvent::Error(error)) = slot.next_event().await {
                assert_eq!(error.kind, ErrorKind::OrderError);
                break;
            }
        }
        assert_eq!(slot.venue.sent().len(), 3, "the fourth send was refused");
    }

    /// **A sweep is risk-reducing: it takes the reserve the quotes left.** With 3 a minute and
    /// 1 held back, two quotes spend everything a quote may take; the sweep still goes, on the
    /// reserve, at once — it answers for orders nobody is managing any more.
    #[tokio::test]
    async fn a_sweep_takes_the_reserve_the_quotes_left() {
        let slot = slot_on_scripted_venue(|config| {
            config.tx_budget_per_minute = Some(3);
            config.tx_reserve = Some(1);
        })
        .await;
        slot.submit(&eth_order(1, Side::Buy));
        slot.submit(&eth_order(2, Side::Buy));
        slot.venue_until(|venue| venue.sent().len() == 2).await;
        let started = time::Instant::now();
        assert_eq!(
            slot.order_path.sweep(vec![42]).await.unwrap(),
            SweepOutcome::Cancelled
        );
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "on the reserve, not after a token came back"
        );
        assert_eq!(slot.venue.sent()[2].0, "cancel_all");
    }

    /// Without `tx_budget_per_minute` nothing is refused — the behaviour before the budget.
    #[tokio::test]
    async fn without_a_send_budget_nothing_is_refused() {
        let slot = slot_on_scripted_venue(|_| {}).await;
        for order_id in 1..=5 {
            slot.submit(&eth_order(order_id, Side::Buy));
        }
        slot.venue_until(|venue| venue.sent().len() == 5).await;
    }

    // ---- a 429/405: one cooldown for everything (§4.16) ---------------------------------

    fn too_many_requests() -> (u16, String) {
        (
            429,
            r#"{"code":429,"message":"Too Many Requests"}"#.to_string(),
        )
    }

    /// **A 429 holds every send for the cooldown and reads nothing meanwhile — then the order
    /// it left in doubt is settled off the venue.** The bots hear one rate-limit error for the
    /// one 429; the order it answered is not handed back (the docs do not promise a 429 went
    /// unprocessed, so it may rest) — its fate is read off `accountActiveOrders`, but only once
    /// the cooldown is over, and the nonce is re-read only before the next send after it. A
    /// quote asked for inside the cooldown goes back to the bot unsent, as over the budget.
    #[tokio::test]
    async fn a_429_holds_every_send_for_the_cooldown_and_reads_nothing_meanwhile() {
        let mut slot = slot_on_scripted_venue(|config| {
            config.rate_limit_cooldown_ms = Some(1_500);
            config.confirm_deadline_ms = 100;
        })
        .await;
        slot.venue.answer_send_tx_with([too_many_requests()]);
        let started = time::Instant::now();
        slot.submit(&eth_order(1, Side::Buy));
        let (kind, message) = as_error(slot.next_event().await);
        assert_eq!(
            kind,
            ErrorKind::OrderError,
            "a rate limit, to the bot's classifier"
        );
        assert!(message.contains("rate-limited"), "{message}");
        assert!(message.contains("429"), "{message}");

        slot.submit(&eth_order(2, Side::Buy));
        let PublishEvent::LiveEvent(LiveEvent::Order { order, .. }) = slot.next_event().await
        else {
            panic!("a quote inside the cooldown comes back");
        };
        assert_eq!((order.order_id, order.status), (2, Status::Expired));
        let (kind, message) = as_error(slot.next_event().await);
        assert_eq!(kind, ErrorKind::OrderError);
        assert!(message.contains("cooldown"), "{message}");
        assert!(message.contains("rate-limited"), "{message}");

        // Order 1's confirmation deadline (100 ms) lapses inside the cooldown.
        time::sleep(Duration::from_millis(300)).await;
        assert_eq!(slot.venue.sent().len(), 1, "nothing sent into the cooldown");
        assert_eq!(
            slot.venue.count("/api/v1/nextNonce"),
            1,
            "the nonce is not re-read inside the cooldown"
        );
        assert_eq!(
            slot.venue.count("/api/v1/accountActiveOrders"),
            0,
            "nor is the doubtful order's fate"
        );

        let PublishEvent::LiveEvent(LiveEvent::Order { order, .. }) = slot.next_event().await
        else {
            panic!("the doubtful order is settled once the cooldown is over");
        };
        assert_eq!((order.order_id, order.status), (1, Status::Expired));
        assert!(
            started.elapsed() >= Duration::from_millis(1_500),
            "not before the cooldown ended"
        );
        assert_eq!(slot.venue.count("/api/v1/accountActiveOrders"), 1);
        let (kind, _) = as_error(slot.next_event().await);
        assert_eq!(kind, ErrorKind::OrderError);

        slot.submit(&eth_order(3, Side::Buy));
        slot.venue_until(|venue| venue.sent().len() == 2).await;
        assert_eq!(
            slot.venue.count("/api/v1/nextNonce"),
            2,
            "re-read once, before the first send after the cooldown"
        );
        assert_eq!(slot.venue.sent()[1], ("create_order".to_string(), 7));
    }

    /// **A cancel asked for inside the cooldown is held, not handed back, and goes out once
    /// the cooldown is over.** Handed back, the bot would cancel again into the same cooldown —
    /// every refusal another error toward its storm breaker — while the order stood on the
    /// venue; held, it leaves the moment the venue will take it, even if the bot has died.
    #[tokio::test]
    async fn a_cancel_inside_the_cooldown_is_held_and_goes_out_after_it() {
        let mut slot = slot_on_scripted_venue(|config| {
            config.rate_limit_cooldown_ms = Some(1_500);
        })
        .await;
        let resting = eth_order(1, Side::Buy);
        slot.submit(&resting);
        slot.venue_until(|venue| venue.sent().len() == 1).await;
        slot.confirm(1);

        slot.venue.answer_send_tx_with([too_many_requests()]);
        slot.submit(&eth_order(2, Side::Buy));
        let (kind, _) = as_error(slot.next_event().await);
        assert_eq!(kind, ErrorKind::OrderError, "the one rate-limit error");
        let told_at = time::Instant::now();

        slot.cancel(&resting);
        time::sleep(Duration::from_millis(200)).await;
        assert_eq!(slot.venue.sent().len(), 2, "held inside the cooldown");
        assert!(
            slot.events.try_recv().is_err(),
            "and not handed back to the bot"
        );

        slot.venue_until(|venue| venue.sent().len() == 3).await;
        assert_eq!(slot.venue.sent()[2].0, "cancel_order");
        assert!(
            told_at.elapsed() >= Duration::from_millis(1_000),
            "it went out when the cooldown was over, not before"
        );
    }

    /// **A cancel the venue answered 429 is held too, and sent again once the cooldown is
    /// over** — it may or may not have taken, and either way handing it back would only send
    /// the bot's next cancel into the same cooldown. The bots hear the one rate-limit error and
    /// nothing else.
    #[tokio::test]
    async fn a_cancel_answered_429_is_held_and_sent_again_after_the_cooldown() {
        let mut slot = slot_on_scripted_venue(|config| {
            config.rate_limit_cooldown_ms = Some(400);
        })
        .await;
        let resting = eth_order(1, Side::Buy);
        slot.submit(&resting);
        slot.venue_until(|venue| venue.sent().len() == 1).await;
        slot.confirm(1);

        slot.venue.answer_send_tx_with([too_many_requests()]);
        slot.cancel(&resting);
        let (kind, message) = as_error(slot.next_event().await);
        assert_eq!(kind, ErrorKind::OrderError);
        assert!(message.contains("rate-limited"), "{message}");

        slot.venue_until(|venue| venue.sent().len() == 3).await;
        assert_eq!(
            slot.venue
                .sent()
                .into_iter()
                .map(|(op, _)| op)
                .collect::<Vec<_>>(),
            ["create_order", "cancel_order", "cancel_order"]
        );
        assert!(
            slot.events.try_recv().is_err(),
            "no hand-back: the cancel stayed pending throughout"
        );
    }

    /// **Held cancels go out before anything newer.** Once the cooldown is over, the first
    /// command the slot is woken for — here a new quote — finds the held cancel sent ahead of
    /// it, so a requote can never overtake the cancel of the quote it replaces. The quote is
    /// asked for 2 ms after the cooldown; the slot's own 250 ms tick, which would release the
    /// cancel by itself, almost never comes in between.
    #[tokio::test]
    async fn held_cancels_go_out_before_anything_newer() {
        let mut slot = slot_on_scripted_venue(|config| {
            config.rate_limit_cooldown_ms = Some(1_500);
        })
        .await;
        let resting = eth_order(1, Side::Buy);
        slot.submit(&resting);
        slot.venue_until(|venue| venue.sent().len() == 1).await;
        slot.confirm(1);
        slot.venue.answer_send_tx_with([too_many_requests()]);
        slot.submit(&eth_order(2, Side::Buy));
        let _ = as_error(slot.next_event().await);
        let told_at = time::Instant::now();
        slot.cancel(&resting);

        time::sleep_until(told_at + Duration::from_millis(1_502)).await;
        slot.submit(&eth_order(3, Side::Buy));
        slot.venue_until(|venue| venue.sent().len() == 4).await;
        let ops: Vec<String> = slot.venue.sent().into_iter().map(|(op, _)| op).collect();
        assert_eq!(ops[2..], ["cancel_order", "create_order"]);
    }

    /// **A cancel asked for twice in a cooldown goes out once.** After a partial fill the bot
    /// sees no cancel pending and may ask again; holding it twice would spend two `sendTx` on
    /// one order, the second into a refusal.
    #[tokio::test]
    async fn a_cancel_asked_for_twice_in_a_cooldown_goes_out_once() {
        let mut slot = slot_on_scripted_venue(|config| {
            config.rate_limit_cooldown_ms = Some(1_000);
        })
        .await;
        let resting = eth_order(1, Side::Buy);
        slot.submit(&resting);
        slot.venue_until(|venue| venue.sent().len() == 1).await;
        slot.confirm(1);
        slot.venue.answer_send_tx_with([too_many_requests()]);
        slot.submit(&eth_order(2, Side::Buy));
        let _ = as_error(slot.next_event().await);

        slot.cancel(&resting);
        slot.cancel(&resting);
        slot.venue_until(|venue| venue.sent().len() == 3).await;
        // A few more ticks for a second one to show up, if it were coming.
        time::sleep(Duration::from_millis(800)).await;
        let cancels = slot
            .venue
            .sent()
            .iter()
            .filter(|(op, _)| op == "cancel_order")
            .count();
        assert_eq!(cancels, 1);
    }

    /// A held cancel whose order has gone meanwhile — filled, or canceled by the venue — is
    /// dropped, not sent: the venue holds nothing for it to cancel.
    #[tokio::test]
    async fn a_held_cancel_for_an_order_that_went_meanwhile_is_dropped() {
        let mut slot = slot_on_scripted_venue(|config| {
            config.rate_limit_cooldown_ms = Some(1_500);
        })
        .await;
        let resting = eth_order(1, Side::Buy);
        slot.submit(&resting);
        slot.venue_until(|venue| venue.sent().len() == 1).await;
        slot.confirm(1);
        slot.venue.answer_send_tx_with([too_many_requests()]);
        slot.submit(&eth_order(2, Side::Buy));
        let _ = slot.next_event().await;

        slot.cancel(&resting);
        // The account stream reports the order filled before the cooldown is over.
        {
            let mut manager = slot.order_path.order_manager.lock().unwrap();
            let coi = manager.coi_for("ETH", 1).unwrap();
            let filled = manager.apply_order_update(&AccountOrder {
                market_index: 42,
                client_order_index: coi,
                order_index: 900_000 + coi,
                is_ask: false,
                status: "filled".to_string(),
                price: 1_000.0,
                initial_base_amount: 0.01,
                remaining_base_amount: 0.0,
                filled_base_amount: 0.01,
                transaction_time_us: 2_000,
            });
            assert!(filled.published().is_some());
        }
        time::sleep(Duration::from_millis(2_000)).await;
        assert_eq!(
            slot.venue.sent().len(),
            2,
            "the cancel of a filled order never went out"
        );
    }

    /// **A held cancel whose order filled in part meanwhile still goes out** — the order is
    /// resting, smaller, and its quote is exactly as stale as before. The 2026-10-08 probe saw
    /// it dropped instead ("its order has gone"): every account-stream update clears the
    /// pending request (`apply_order_update` writes `req = None`), and the release asked for a
    /// pending request rather than for a live order. The bot is told the cancel is in flight
    /// again — the order republished with `req = Canceled` and no execution of its own (the
    /// fill went out with the update that reported it).
    #[tokio::test]
    async fn a_held_cancel_whose_order_filled_in_part_meanwhile_goes_out_after_the_cooldown() {
        let mut slot = slot_on_scripted_venue(|config| {
            config.rate_limit_cooldown_ms = Some(1_500);
        })
        .await;
        let resting = eth_order(1, Side::Buy);
        slot.submit(&resting);
        slot.venue_until(|venue| venue.sent().len() == 1).await;
        slot.confirm(1);
        slot.venue.answer_send_tx_with([too_many_requests()]);
        slot.submit(&eth_order(2, Side::Buy));
        let (kind, _) = as_error(slot.next_event().await);
        assert_eq!(kind, ErrorKind::OrderError, "the one rate-limit error");

        slot.cancel(&resting);
        // The account stream reports a partial fill before the cooldown is over.
        {
            let mut manager = slot.order_path.order_manager.lock().unwrap();
            let coi = manager.coi_for("ETH", 1).unwrap();
            let partial = manager.apply_order_update(&AccountOrder {
                market_index: 42,
                client_order_index: coi,
                order_index: 900_000 + coi,
                is_ask: false,
                status: "open".to_string(),
                price: 1_000.0,
                initial_base_amount: 0.01,
                remaining_base_amount: 0.006,
                filled_base_amount: 0.004,
                transaction_time_us: 2_000,
            });
            let (_, order) = partial.published().expect("a fill of ours");
            assert_eq!(order.req, Status::None, "the update cleared the request");
        }

        slot.venue_until(|venue| venue.sent().len() == 3).await;
        assert_eq!(
            slot.venue.sent()[2].0,
            "cancel_order",
            "the cancel of a still-resting order went out once the cooldown was over"
        );
        let PublishEvent::LiveEvent(LiveEvent::Order { order, .. }) = slot.next_event().await
        else {
            panic!("the bot is told the cancel is in flight again");
        };
        assert_eq!(order.order_id, 1);
        assert_eq!(order.req, Status::Canceled);
        assert_eq!(order.leaves_qty, 0.006, "the order as it now rests");
        assert_eq!(order.exec_qty, 0.0, "a republish is not an execution");
    }

    /// **A sweep waits out a cooldown that ends before its deadline**, and goes out once it is
    /// over: a sweep answers for orders nobody manages any more, so it is held for the venue
    /// rather than given up.
    #[tokio::test]
    async fn a_sweep_waits_out_a_cooldown_that_ends_before_its_deadline() {
        let mut slot = slot_on_scripted_venue(|config| {
            config.rate_limit_cooldown_ms = Some(1_500);
        })
        .await;
        slot.venue.answer_send_tx_with([too_many_requests()]);
        slot.submit(&eth_order(1, Side::Buy));
        let _ = slot.next_event().await;
        let told_at = time::Instant::now();

        let outcome = slot.order_path.sweep(vec![42]).await.unwrap();
        assert_eq!(outcome, SweepOutcome::Cancelled);
        assert!(told_at.elapsed() >= Duration::from_millis(1_000));
        assert_eq!(
            slot.venue.sent().last().map(|(op, _)| op.as_str()),
            Some("cancel_all")
        );
    }

    /// **A sweep that cannot outwait the cooldown fails at once, unsent, and says so** — the
    /// bots hear a `CriticalConnectionError`, the stop's exit code a `Failed` sweep — rather than
    /// sending into a cooldown that will refuse it, or sleeping past the moment its waiter has
    /// given up.
    #[tokio::test]
    async fn a_sweep_that_cannot_outwait_the_cooldown_fails_unsent() {
        let mut slot = slot_on_scripted_venue(|config| {
            config.rate_limit_cooldown_ms = Some(60_000);
        })
        .await;
        slot.venue.answer_send_tx_with([too_many_requests()]);
        slot.submit(&eth_order(1, Side::Buy));
        let _ = slot.next_event().await;

        let asked = time::Instant::now();
        let outcome = slot.order_path.sweep(vec![42]).await.unwrap();
        assert_eq!(outcome, SweepOutcome::Failed);
        assert!(
            asked.elapsed() < Duration::from_secs(2),
            "it did not wait for a cooldown it could not outlast"
        );
        assert_eq!(slot.venue.sent().len(), 1, "nothing sent into the cooldown");
        let (kind, message) = as_error(slot.next_event().await);
        assert_eq!(kind, ErrorKind::CriticalConnectionError);
        assert!(message.contains("cooldown"), "{message}");
    }

    // ---- probing the cooldown with what gets the account out (§4.16) -------------------

    /// **With `rate_limit_cancel_probe_ms`, a held cancel tries one probe interval into the
    /// cooldown instead of waiting it out — and so does a reducing IOC — while a quote is still
    /// refused.** Which of the docs' two cooldowns answered cannot be told from here: the
    /// firewall's static minute, or an API server's few seconds. Waiting the minute for the
    /// second kind leaves a stale quote resting for nothing. The bots hear the one rate-limit
    /// error, which now says the cancels probe, and nothing for the probe that worked.
    #[tokio::test]
    async fn a_held_cancel_probes_the_cooldown_and_goes_out_before_it_ends() {
        let mut slot = slot_on_scripted_venue(|config| {
            config.rate_limit_cooldown_ms = Some(10_000);
            config.rate_limit_cancel_probe_ms = Some(300);
        })
        .await;
        let resting = eth_order(1, Side::Buy);
        slot.submit(&resting);
        slot.venue_until(|venue| venue.sent().len() == 1).await;
        slot.confirm(1);
        slot.venue.answer_send_tx_with([too_many_requests()]);
        slot.submit(&eth_order(2, Side::Buy));
        let (kind, message) = as_error(slot.next_event().await);
        assert_eq!(kind, ErrorKind::OrderError);
        assert!(message.contains("probe it from 300 ms"), "{message}");
        assert!(message.contains("rate-limited"), "{message}");
        let told_at = time::Instant::now();

        slot.cancel(&resting);
        slot.venue_until(|venue| venue.sent().len() == 3).await;
        assert_eq!(slot.venue.sent()[2].0, "cancel_order");

        slot.submit(&eth_order(3, Side::Buy));
        let PublishEvent::LiveEvent(LiveEvent::Order { order, .. }) = slot.next_event().await
        else {
            panic!("a quote is still refused: the cooldown stands for it");
        };
        assert_eq!((order.order_id, order.status), (3, Status::Expired));
        let (_, message) = as_error(slot.next_event().await);
        assert!(message.contains("until its cooldown is over"), "{message}");

        slot.state_position(0.05);
        slot.submit(&eth_ioc(4, Side::Sell));
        slot.venue_until(|venue| venue.sent().len() == 4).await;
        assert_eq!(
            slot.venue.sent()[3].0,
            "create_order",
            "an IOC that reduces the long probes like a cancel"
        );
        assert!(
            told_at.elapsed() < Duration::from_secs(5),
            "all of it well inside the 10 s cooldown"
        );
        assert!(
            slot.events.try_recv().is_err(),
            "the probes that worked told the bots nothing"
        );
    }

    /// **A probe the venue refuses neither lengthens the cooldown nor tells the bots again.**
    /// The firewall's minute is static (docs): a probe into it buys only another refusal, and
    /// read as a fresh rate limit it would push the quotes' cooldown out with every probe and
    /// put one more error in the bots' storm window each time. Here the probe at 700 ms is
    /// refused; a quote right after the original 1.5 s still goes out — a lengthened cooldown
    /// would have held it to ~2.2 s — and the bots heard one error in all.
    #[tokio::test]
    async fn a_refused_probe_neither_lengthens_the_cooldown_nor_tells_the_bots_again() {
        let mut slot = slot_on_scripted_venue(|config| {
            config.rate_limit_cooldown_ms = Some(1_500);
            config.rate_limit_cancel_probe_ms = Some(700);
        })
        .await;
        let resting = eth_order(1, Side::Buy);
        slot.submit(&resting);
        slot.venue_until(|venue| venue.sent().len() == 1).await;
        slot.confirm(1);
        slot.venue
            .answer_send_tx_with([too_many_requests(), too_many_requests()]);
        slot.submit(&eth_order(2, Side::Buy));
        let (kind, _) = as_error(slot.next_event().await);
        assert_eq!(kind, ErrorKind::OrderError, "the cooldown's one error");
        let told_at = time::Instant::now();

        slot.cancel(&resting);
        slot.venue_until(|venue| venue.sent().len() == 3).await;
        assert_eq!(slot.venue.sent()[2].0, "cancel_order", "the probe, refused");

        time::sleep_until(told_at + Duration::from_millis(1_650)).await;
        slot.submit(&eth_order(3, Side::Buy));
        slot.venue_until(|venue| {
            venue
                .sent()
                .iter()
                .filter(|(op, _)| op == "create_order")
                .count()
                == 3
        })
        .await;
        while let Ok(event) = slot.events.try_recv() {
            assert!(
                !matches!(event, PublishEvent::LiveEvent(LiveEvent::Error(_))),
                "a refused probe told the bots again"
            );
        }
    }

    /// **A probe that cannot read its nonce is a refused probe, not a failed cancel.** The
    /// cooldown's 429 left the nonce to be re-read, and a venue that is still refusing the
    /// account refuses that read too. Handed back, the cancel would reach the bot as an error —
    /// one more in its storm window — and its order would stand; held, it goes on the next
    /// probe, and the bots hear nothing.
    #[tokio::test]
    async fn a_probe_that_cannot_read_its_nonce_stays_held_and_tells_the_bots_nothing() {
        let mut slot = slot_on_scripted_venue(|config| {
            config.rate_limit_cooldown_ms = Some(10_000);
            config.rate_limit_cancel_probe_ms = Some(300);
        })
        .await;
        let resting = eth_order(1, Side::Buy);
        slot.submit(&resting);
        slot.venue_until(|venue| venue.sent().len() == 1).await;
        slot.confirm(1);
        slot.venue.answer_send_tx_with([too_many_requests()]);
        slot.submit(&eth_order(2, Side::Buy));
        let _ = as_error(slot.next_event().await);
        let told_at = time::Instant::now();

        slot.venue.answer_next_nonce_with([503]);
        slot.cancel(&resting);
        slot.venue_until(|venue| venue.sent().len() == 3).await;
        assert_eq!(slot.venue.sent()[2].0, "cancel_order");
        assert_eq!(
            slot.venue.count("/api/v1/nextNonce"),
            3,
            "seeded, refused once on the first probe, read on the next"
        );
        assert!(told_at.elapsed() < Duration::from_secs(5));
        assert!(
            slot.events.try_recv().is_err(),
            "no hand-back and no error for a probe that could not be signed"
        );
    }

    /// **A 429 on the sweep itself starts the cooldown, tells the bots once, and — without
    /// probes — gets no second attempt before the cooldown is over.** The retry finds the gate
    /// shut past the sweep's 15 s and the sweep fails there, unsent, loudly (a
    /// `CriticalConnectionError`; a stop exits non-zero on it). The cooldown it started holds
    /// the next quote.
    #[tokio::test]
    async fn a_429_on_the_sweep_itself_starts_the_cooldown_and_gets_no_second_attempt_inside_it() {
        let mut slot = slot_on_scripted_venue(|config| {
            config.rate_limit_cooldown_ms = Some(60_000);
        })
        .await;
        slot.venue.answer_send_tx_with([too_many_requests()]);
        let started = time::Instant::now();
        assert_eq!(
            slot.order_path.sweep(vec![42]).await.unwrap(),
            SweepOutcome::Failed
        );
        assert!(started.elapsed() < Duration::from_secs(5), "failed at once");
        assert_eq!(slot.venue.sent(), [("cancel_all".to_string(), 7)]);
        let (kind, message) = as_error(slot.next_event().await);
        assert_eq!(
            kind,
            ErrorKind::OrderError,
            "the cooldown's one rate-limit error"
        );
        assert!(message.contains("rate-limited"), "{message}");
        let (kind, message) = as_error(slot.next_event().await);
        assert_eq!(kind, ErrorKind::CriticalConnectionError, "the failed sweep");
        assert!(message.contains("cooldown"), "{message}");

        slot.submit(&eth_order(1, Side::Buy));
        let PublishEvent::LiveEvent(LiveEvent::Order { order, .. }) = slot.next_event().await
        else {
            panic!("the cooldown the sweep started holds the next quote");
        };
        assert_eq!(order.status, Status::Expired);
        let (_, message) = as_error(slot.next_event().await);
        assert!(message.contains("until its cooldown is over"), "{message}");
    }

    /// **With probes, the sweep answered 429 tries again on the first probe** — still one
    /// error to the bots, and the sweep takes well inside the cooldown.
    #[tokio::test]
    async fn with_probes_a_sweep_answered_429_tries_again_on_the_first_probe() {
        let mut slot = slot_on_scripted_venue(|config| {
            config.rate_limit_cooldown_ms = Some(60_000);
            config.rate_limit_cancel_probe_ms = Some(300);
        })
        .await;
        slot.venue.answer_send_tx_with([too_many_requests()]);
        let started = time::Instant::now();
        assert_eq!(
            slot.order_path.sweep(vec![42]).await.unwrap(),
            SweepOutcome::Cancelled
        );
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_millis(300) && elapsed < Duration::from_secs(5),
            "on the first probe: {elapsed:?}"
        );
        let ops: Vec<String> = slot.venue.sent().into_iter().map(|(op, _)| op).collect();
        assert_eq!(ops, ["cancel_all", "cancel_all"]);
        let (kind, _) = as_error(slot.next_event().await);
        assert_eq!(kind, ErrorKind::OrderError);
        assert!(slot.events.try_recv().is_err(), "one error, nothing more");
    }

    /// **Without `rate_limit_cooldown_ms` a 429 is handled exactly as before**: as an
    /// ambiguous answer — the nonce re-read at once, the order kept for its fate check, no
    /// error of its own, and the next quote not held back.
    #[tokio::test]
    async fn without_a_cooldown_a_429_is_handled_as_before() {
        let mut slot = slot_on_scripted_venue(|_| {}).await;
        slot.venue.answer_send_tx_with([too_many_requests()]);
        slot.submit(&eth_order(1, Side::Buy));
        slot.venue_until(|venue| venue.count("/api/v1/nextNonce") == 2)
            .await;
        slot.submit(&eth_order(2, Side::Buy));
        slot.venue_until(|venue| venue.sent().len() == 2).await;
        assert!(
            slot.events.try_recv().is_err(),
            "no rate-limit error, and nothing handed back"
        );
    }

    /// **The one place "as before" is not byte for byte, pinned so it stays deliberate**
    /// (§4.16): before `RateLimited` existed, a 429/405 whose body carried `21104` was read as
    /// `InvalidNonce` and its order expired at once; now the status decides first, and without
    /// a cooldown it is `Ambiguous` — the nonce re-read all the same, the order kept tracked for
    /// its fate check instead of expired. The rate limiter answered; whether the transaction
    /// was processed is not something its body can vouch for, and expiring an order that may
    /// rest is the orphan §3.3 exists to prevent.
    #[tokio::test]
    async fn without_a_cooldown_a_429_carrying_21104_is_still_read_as_ambiguous() {
        let mut slot = slot_on_scripted_venue(|_| {}).await;
        slot.venue.answer_send_tx_with([(
            429,
            r#"{"code":21104,"message":"invalid nonce"}"#.to_string(),
        )]);
        slot.submit(&eth_order(1, Side::Buy));
        slot.venue_until(|venue| venue.count("/api/v1/nextNonce") == 2)
            .await;
        time::sleep(Duration::from_millis(200)).await;
        assert!(
            slot.events.try_recv().is_err(),
            "not expired at once as an invalid nonce would be: its fate is checked first"
        );
        assert_eq!(
            slot.order_path.order_manager.lock().unwrap().tracked(),
            1,
            "still tracked"
        );
    }

    // ---- the private stream's own error frames reach the bots (§4.17) ------------------

    /// The bots' next error, if any is waiting.
    fn next_error(
        ev_rx: &mut tokio::sync::mpsc::UnboundedReceiver<PublishEvent>,
    ) -> Option<(ErrorKind, String)> {
        while let Ok(event) = ev_rx.try_recv() {
            if let PublishEvent::LiveEvent(LiveEvent::Error(error)) = event {
                let Value::String(message) = error.value else {
                    panic!("the reason rides as a string");
                };
                return Some((error.kind, message));
            }
        }
        None
    }

    /// **An error frame about the account reaches the bots as a `CriticalConnectionError`.** It
    /// used to be a `warn!` and nothing more: a refused subscription (`20001` auth required) or
    /// a rejected token (`20013`) left the order state uncurrent with no bot told why.
    #[tokio::test]
    async fn a_private_stream_error_about_the_account_is_critical_to_the_bots() {
        let (mut task, _symbol_tx, _command_rx, mut ev_rx) = stream_task(ConnectPolicy::Reconcile);
        task.on_frame(PRIVATE_ERROR_AUTH_REQUIRED);
        let (kind, message) = next_error(&mut ev_rx).expect("the bots are told");
        assert_eq!(kind, ErrorKind::CriticalConnectionError);
        assert!(message.contains("20001"), "{message}");
        assert!(message.contains("auth"), "{message}");
    }

    /// **One about a transaction is an `OrderError`** — a rate-limit-class refusal to the bot's
    /// classifier, as the same code is when it comes back on a `sendTx`.
    #[tokio::test]
    async fn a_private_stream_error_about_a_transaction_is_an_order_error() {
        let (mut task, _symbol_tx, _command_rx, mut ev_rx) = stream_task(ConnectPolicy::Reconcile);
        task.on_frame(r#"{"error":{"code":21104,"message":"invalid nonce"}}"#);
        let (kind, message) = next_error(&mut ev_rx).expect("the bots are told");
        assert_eq!(kind, ErrorKind::OrderError);
        assert!(message.contains("21104"), "{message}");
    }

    /// **A code nobody has seen is critical** — fail closed: a private socket saying something
    /// unknown is one whose account state this process cannot vouch for.
    #[tokio::test]
    async fn an_unknown_private_stream_error_is_critical() {
        let (mut task, _symbol_tx, _command_rx, mut ev_rx) = stream_task(ConnectPolicy::Reconcile);
        task.on_frame(r#"{"error":{"code":77777,"message":"something new"}}"#);
        let (kind, _) = next_error(&mut ev_rx).expect("the bots are told");
        assert_eq!(kind, ErrorKind::CriticalConnectionError);
    }

    /// **The same error is told at most once every ten seconds, a different one at once.** A
    /// venue repeating itself — every subscription of a reconnect refused alike — must not
    /// trip a bot's critical breaker (8 a minute on the RH bots) on one condition many times
    /// over; a new code is new information and goes straight through.
    #[tokio::test(start_paused = true)]
    async fn a_repeated_private_stream_error_is_told_once_per_window() {
        let (mut task, _symbol_tx, _command_rx, mut ev_rx) = stream_task(ConnectPolicy::Reconcile);
        task.on_frame(PRIVATE_ERROR_AUTH_REQUIRED);
        task.on_frame(PRIVATE_ERROR_AUTH_REQUIRED);
        task.on_frame(PRIVATE_ERROR_AUTH_REQUIRED);
        assert!(next_error(&mut ev_rx).is_some());
        assert!(
            next_error(&mut ev_rx).is_none(),
            "the repeats are held back"
        );

        task.on_frame(r#"{"error":{"code":20013,"message":"invalid auth: invalid deadline"}}"#);
        let (_, message) = next_error(&mut ev_rx).expect("a different code goes through");
        assert!(message.contains("20013"), "{message}");

        time::advance(Duration::from_secs(10)).await;
        task.on_frame(PRIVATE_ERROR_AUTH_REQUIRED);
        assert!(
            next_error(&mut ev_rx).is_some(),
            "told again once the window has passed"
        );
    }
}
