//! The `cloid` ↔ `(symbol, order_id)` map, and every order-state transition.
//!
//! Mirrors `bybit/ordermanager.rs` in shape, and differs in three ways that are all forced
//! by the venue:
//!
//! * **The client id is 128 bits of hex, not a free string.** Bybit's `orderLinkId` can
//!   carry a readable prefix, so it can ask `starts_with(prefix)` to tell its own orders
//!   from a human's on the same account. Hyperliquid's `cloid` is exactly `0x` + 32 hex
//!   characters — 30 is an HTTP 422 — so the prefix lives in the **high nibbles** instead
//!   (design note §5.4). The property that matters survives: an order whose prefix does
//!   not match is not ours and is ignored, not reported as an unknown order.
//! * **Fills arrive on their own stream and carry the account's position with them.** Each
//!   `userFill` states `startPosition`, so the position after it is
//!   `startPosition + signedSize` — an *absolute* answer rather than an increment onto a
//!   local guess. A fill this connector never saw therefore corrects itself on the next
//!   one, instead of leaving a permanent offset.
//! * **Fills must be deduplicated.** `userFills` replays its whole history with
//!   `isSnapshot: true` on **every** subscribe, so a reconnect re-delivers fills already
//!   applied. Without a guard, one reconnect doubles `exec_qty` on every live order.
//!
//! The reject path is the one worth reading twice. `LiveBot::submit_order` inserts the
//! order into its own map as `Status::New` **before** the request leaves, and nothing in
//! `process_event` removes it when an `Error` event arrives. An order that the venue or
//! this module refuses must therefore be published back as `Status::Expired`, or the bot
//! holds a live order that exists nowhere and burns that `order_id` for the life of the
//! process (`AGENTS.md` §4.4 context; the same fix Phase 1 made for `reject_order`).

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Instant,
};

use hashbrown::HashMap;
use hftbacktest::types::{OrdType, Order, OrderId, Side, Status};
use rand::Rng;
use tracing::{debug, warn};

use crate::{
    connector::{ApplyOutcome, GetOrders},
    hyperliquid::{
        HyperliquidError,
        exchange::{CancelByCloidWire, OrderWire, tif_of, wire_numbers},
        msg::{OrderUpdate, UserFill, map_order_status},
        normalize::{normalize_price, normalize_size},
        rest::{OpenOrder, SymbolInfo},
    },
    utils::{RefSymbolOrderId, SymbolOrderId},
};

pub type SharedOrderManager = Arc<Mutex<OrderManager>>;

/// `0x` + 32 hex characters, exactly. Anything else is an HTTP 422 from `/exchange`.
pub type Cloid = String;

/// Hex characters of [`Cloid`] reserved for the configured prefix.
pub const PREFIX_LEN: usize = 4;
/// Hex characters of the 128-bit client id.
const CLOID_HEX_LEN: usize = 32;

/// How many recent fill ids are remembered.
///
/// Only *this account's* fills pass through here, which is orders of magnitude fewer than
/// the public trade feed. The window has to cover everything a reconnect can replay while
/// an order is still live; a few thousand is far past that, and the memory is a rounding
/// error. Note the window can be overrun by the initial `isSnapshot` replay of a long
/// history — harmless, because the fills that fall out of it are the *oldest*, whose orders
/// are long gone, while a live order's fills are by construction the most recent.
const FILL_HISTORY: usize = 4096;

#[derive(Clone, Debug)]
pub struct OrderExt {
    pub symbol: String,
    pub order: Order,
    /// The venue's own order id, once anything has told this connector what it is.
    ///
    /// `None` while a submit is in flight. Reconciliation leans on this rather than on the
    /// client id because `oid` is the one identifier **every** `/info` open-orders shape
    /// carries; see [`OrderManager::reconcile`].
    pub oid: Option<u64>,
    /// Venue time of the last `orderUpdates` transition applied, in nanoseconds.
    ///
    /// **Deliberately not `order.exch_timestamp`.** That field also moves with `userFills`,
    /// and `time` there is a *different venue clock* from `statusTimestamp` here — the same
    /// match can be stamped a millisecond apart by the two. Gating the lifecycle guard on a
    /// watermark a fill advanced drops the order's own terminal update, leaving the bot
    /// holding a filled order it believes is resting.
    update_ts: i64,
    /// When the `/exchange` request for this order finished, either way.
    ///
    /// `None` while the POST is in flight, which is what stops [`OrderManager::reconcile`]
    /// from expiring an order the venue has not been told about yet (design note §11.6).
    /// Compared against the moment the open-orders query *started*, so an order acknowledged
    /// after that query went out is not judged by an answer that predates it.
    settled_at: Option<Instant>,
}

/// What happened to one inbound fill.
///
/// Deliberately not `PartialEq`: `Order` carries a `Box<dyn AnyClone>` and cannot be
/// compared, so callers and tests match on the shape rather than on equality.
#[derive(Debug)]
pub enum FillDisposition {
    /// Already applied — the venue replayed it on a (re)subscribe.
    Replay,
    /// New. `order` is set only when it belongs to an order this connector tracks; the
    /// position moves either way, because a fill from anywhere on the account is still the
    /// account's position.
    Applied {
        symbol: String,
        order: Option<Order>,
        /// The account's position in this coin **after** this fill, as the venue states it.
        position: f64,
        exch_ts: i64,
    },
}

impl FillDisposition {
    /// Whether the venue re-sent a fill already applied — which it does on every
    /// (re)subscribe.
    pub fn is_replay(&self) -> bool {
        matches!(self, FillDisposition::Replay)
    }
}

/// Remembers which fills have been applied.
struct FillDedup {
    seen: hashbrown::HashSet<u64>,
    order: VecDeque<u64>,
    dropped: u64,
}

impl FillDedup {
    fn new() -> Self {
        Self {
            seen: hashbrown::HashSet::with_capacity(FILL_HISTORY),
            order: VecDeque::with_capacity(FILL_HISTORY),
            dropped: 0,
        }
    }

    /// `true` when this fill has not been seen before.
    fn accept(&mut self, tid: u64) -> bool {
        if !self.seen.insert(tid) {
            self.dropped += 1;
            return false;
        }
        self.order.push_back(tid);
        if self.order.len() > FILL_HISTORY
            && let Some(evicted) = self.order.pop_front()
        {
            self.seen.remove(&evicted);
        }
        true
    }
}

pub struct OrderManager {
    prefix: String,
    orders: HashMap<Cloid, OrderExt>,
    order_id_map: HashMap<SymbolOrderId, Cloid>,
    fills: FillDedup,
}

impl OrderManager {
    /// `prefix` must be exactly [`PREFIX_LEN`] hex characters — it occupies the high
    /// nibbles of every client id this connector mints, and is what tells its own orders
    /// from anyone else's on the same account.
    pub fn new(prefix: &str) -> Result<Self, HyperliquidError> {
        let prefix = prefix.trim().to_lowercase();
        if prefix.len() != PREFIX_LEN || !prefix.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(HyperliquidError::Credentials(format!(
                "order_prefix must be exactly {PREFIX_LEN} hexadecimal characters (the \
                 Hyperliquid cloid is a 128-bit value, so a readable prefix is not \
                 available); got {prefix:?}"
            )));
        }
        Ok(Self {
            prefix,
            orders: HashMap::new(),
            order_id_map: HashMap::new(),
            fills: FillDedup::new(),
        })
    }

    /// Fills replayed by a (re)subscribe and dropped.
    pub fn replayed_fills(&self) -> u64 {
        self.fills.dropped
    }

    pub fn tracked(&self) -> usize {
        self.orders.len()
    }

    /// Whether a client id carries this connector's prefix.
    ///
    /// An id that fails this is a human's, or another bot's, on the same account. It is
    /// **ignored** — not an error, and above all not something to cancel.
    ///
    /// It answers one question only: *may this process act on a lifecycle update for that
    /// id*. It is **not** process identity — the prefix is configuration, and an order left
    /// by a previous run of this connector passes. Use [`OrderManager::tracks`] for "does
    /// this process know the order".
    ///
    /// Compared as bytes: the argument is venue text, and a body 32 *bytes* long whose fifth
    /// byte falls inside a multi-byte character would panic a `str` slice — which in this
    /// process means `exit(1)` (`AGENTS.md` §4.7).
    pub fn is_ours(&self, cloid: Option<&str>) -> bool {
        let Some(cloid) = cloid else {
            // No client id at all: placed outside this connector, by construction.
            return false;
        };
        cloid.strip_prefix("0x").is_some_and(|body| {
            let body = body.as_bytes();
            body.len() == CLOID_HEX_LEN
                && body.iter().all(u8::is_ascii_hexdigit)
                && body[..PREFIX_LEN].eq_ignore_ascii_case(self.prefix.as_bytes())
        })
    }

    /// Whether **this process** is tracking the order the venue is listing.
    ///
    /// The predicate a reconnect needs: an order carrying the configured prefix but placed
    /// by a previous run is no more actionable than a human's — its `order_id` belongs to a
    /// bot that no longer exists — so it has to be counted and reported, not quietly
    /// swallowed by a prefix match.
    pub fn tracks(&self, open: &OpenOrder) -> bool {
        if let Some(cloid) = open.cloid.as_deref()
            && self.orders.contains_key(&cloid.to_lowercase())
        {
            return true;
        }
        self.orders
            .values()
            .any(|tracked| tracked.oid == Some(open.oid))
    }

    fn mint_cloid(&self) -> Cloid {
        let mut rng = rand::rng();
        loop {
            let mut cloid = String::with_capacity(2 + CLOID_HEX_LEN);
            cloid.push_str("0x");
            cloid.push_str(&self.prefix);
            // 28 hex characters of randomness, which is not a whole number of `u32`s —
            // three of them and a `u16`. Getting the width wrong is not a subtle bug: the
            // venue answers a `cloid` of any other length with an HTTP 422, so *every*
            // order becomes unsendable.
            for _ in 0..3 {
                cloid.push_str(&format!("{:08x}", rng.random::<u32>()));
            }
            cloid.push_str(&format!("{:04x}", rng.random::<u16>()));
            debug_assert_eq!(cloid.len(), 2 + CLOID_HEX_LEN);
            // 112 bits of randomness makes a collision unreachable, but the map is right
            // here and a duplicate client id would silently retarget a cancel.
            if !self.orders.contains_key(&cloid) {
                return cloid;
            }
        }
    }

    /// Registers a new order and returns the wire form to sign.
    ///
    /// The `Order` kept here carries the **accepted** price and size — what normalisation
    /// made of the request — not what was asked for, so the copy published back to the bot
    /// is the truth about what rests on the venue (design note §5.3).
    pub fn new_order(
        &mut self,
        symbol: &str,
        info: &SymbolInfo,
        asset_index: u32,
        order: Order,
    ) -> Result<(Cloid, OrderWire), HyperliquidError> {
        let side_is_buy = match order.side {
            Side::Buy => true,
            Side::Sell => false,
            other => {
                return Err(HyperliquidError::InvalidOrder(format!(
                    "an order with side {other:?}"
                )));
            }
        };
        // Finding 4: this backend places limit orders only — the wire below is always a limit
        // (`OrderKind::limit`). A Market or Unsupported order was previously coerced to a limit
        // at the passed price, diverging live behaviour from the strategy-facing API. Refuse it
        // instead, before any state is minted, exactly as `tif_of` refuses FOK. The grid uses
        // `OrdType::Limit` only, so this is safe and correct.
        match order.order_type {
            OrdType::Limit => {}
            order_type => {
                return Err(HyperliquidError::InvalidOrder(format!(
                    "the Hyperliquid backend places limit orders only; got {order_type:?}"
                )));
            }
        }
        let tif = tif_of(order.time_in_force)?;

        let requested_price = order.price_tick as f64 * order.tick_size;
        let price = normalize_price(requested_price, info, order.side)?;
        let size = normalize_size(order.qty, info)?;
        let (p, s) = wire_numbers(price, size)?;

        if price != requested_price || size != order.qty {
            debug!(
                %symbol,
                order_id = order.order_id,
                requested_price,
                price,
                requested_size = order.qty,
                size,
                "Adjusted a Hyperliquid order onto the venue's grid; the bot is told the \
                 accepted values."
            );
        }

        let symbol_order_id = SymbolOrderId::new(symbol.to_string(), order.order_id);
        if self.order_id_map.contains_key(&symbol_order_id) {
            return Err(HyperliquidError::InvalidOrder(format!(
                "order_id {} is already live on {symbol}",
                order.order_id
            )));
        }

        let cloid = self.mint_cloid();
        let wire = OrderWire {
            a: asset_index,
            b: side_is_buy,
            p,
            s,
            r: false,
            t: crate::hyperliquid::exchange::OrderKind {
                limit: crate::hyperliquid::exchange::LimitKind { tif },
            },
            c: Some(cloid.clone()),
        };

        let mut accepted = order;
        accepted.price_tick = (price / accepted.tick_size).round() as i64;
        accepted.qty = size;
        accepted.leaves_qty = size;

        self.order_id_map.insert(symbol_order_id, cloid.clone());
        self.orders.insert(
            cloid.clone(),
            OrderExt {
                symbol: symbol.to_string(),
                order: accepted,
                oid: None,
                update_ts: 0,
                // In flight from here until `/exchange` answers: the caller posts next.
                settled_at: None,
            },
        );
        Ok((cloid, wire))
    }

    /// The cancel wire form for an order this connector placed.
    pub fn cancel_order(
        &self,
        symbol: &str,
        order_id: OrderId,
        asset_index: u32,
    ) -> Result<(Cloid, CancelByCloidWire), HyperliquidError> {
        let cloid = self
            .order_id_map
            .get(&RefSymbolOrderId::new(symbol, order_id))
            .ok_or_else(|| {
                HyperliquidError::OrderNotFound(format!("{symbol} order_id {order_id}"))
            })?
            .clone();
        Ok((
            cloid.clone(),
            CancelByCloidWire {
                asset: asset_index,
                cloid,
            },
        ))
    }

    /// Applies one `orderUpdates` entry.
    ///
    /// [`ApplyOutcome::Ignored`] for an order that is not this connector's — a human's on the
    /// same account, or one placed before this process started — or a stale/duplicate frame.
    /// That is not an error: reporting it would fill the log with every manual action on the
    /// account.
    ///
    /// [`ApplyOutcome::Unrecognised`] when the venue's status does not map to a known
    /// [`Status`] (Finding 3): the order is **kept exactly as it is** — not transitioned, not
    /// removed — because an unknown status may mean it is still resting, and dropping it would
    /// let the strategy re-quote its price level into a double position (`AGENTS.md` §1.1). The
    /// caller escalates; reconciliation resolves the true fate.
    pub fn apply_order_update(&mut self, update: &OrderUpdate) -> ApplyOutcome<OrderExt> {
        if !self.is_ours(update.order.cloid.as_deref()) {
            return ApplyOutcome::Ignored;
        }
        let Some(cloid) = update.order.cloid.as_deref() else {
            return ApplyOutcome::Ignored;
        };
        let Some(tracked) = self.orders.get_mut(cloid) else {
            // Ours by prefix but unknown: an order from a previous run of this connector.
            // The reconnect reconciliation is what adopts or cancels those; a status
            // update on its own has nothing to attach to.
            debug!(%cloid, status = %update.status, "An orderUpdates entry for an order this process does not track.");
            return ApplyOutcome::Ignored;
        };

        let Some(exch_ts) = update.status_timestamp.checked_mul(1_000_000) else {
            return ApplyOutcome::Ignored;
        };
        // Out-of-order updates are possible and a stale one must not overwrite a newer
        // state. `LiveBot` guards its own copy the same way (`live/bot.rs`), but the
        // connector's map is what answers `orders()` on registration, so it needs its own.
        //
        // Compared against the watermark of **this stream only**: `userFills` stamps a
        // different venue clock, and mixing the two drops an order's own terminal update
        // whenever the fill's clock happens to run ahead.
        if exch_ts < tracked.update_ts {
            debug!(
                %cloid,
                status = %update.status,
                stale_by_ns = tracked.update_ts - exch_ts,
                "Ignoring an out-of-order Hyperliquid order update."
            );
            return ApplyOutcome::Ignored;
        }
        // Finding 3: an unrecognised status is resolved against the venue's truth, not acted
        // on here. Map it **before any mutation**, and if it is `Status::Unsupported` return
        // with the order left exactly as it is — not transitioned, not removed. Treating it as
        // terminal would drop an order that may still be resting (a re-quote into a double
        // position); treating it as live would quote against one that is gone. §1.1 fail
        // closed: keep it, and let the caller escalate so reconciliation settles it.
        let status = map_order_status(&update.status);
        if status == Status::Unsupported {
            warn!(
                %cloid,
                status = %update.status,
                "Keeping a Hyperliquid order whose status this connector does not recognise; \
                 reconciliation will resolve it (Finding 3, §1.1)."
            );
            return ApplyOutcome::Unrecognised;
        }
        tracked.update_ts = exch_ts;
        // The venue is answering about this order, so whatever the submit did, it is done.
        tracked.settled_at.get_or_insert_with(Instant::now);

        tracked.oid = Some(update.order.oid);
        tracked.order.req = Status::None;
        tracked.order.status = status;
        // Never rewound: `LiveBot::process_event` drops an order update older than its own
        // copy, and a fill may already have carried this order past this stamp.
        tracked.order.exch_timestamp = tracked.order.exch_timestamp.max(exch_ts);
        if update.order.orig_sz > 0.0 {
            tracked.order.leaves_qty = update.order.sz;
        }

        if tracked.order.active() {
            ApplyOutcome::Publish(tracked.clone())
        } else {
            let symbol = tracked.symbol.clone();
            let order_id = tracked.order.order_id;
            self.order_id_map
                .remove(&RefSymbolOrderId::new(&symbol, order_id));
            match self.orders.remove(cloid) {
                Some(removed) => ApplyOutcome::Publish(removed),
                None => ApplyOutcome::Ignored,
            }
        }
    }

    /// Applies one fill.
    ///
    /// The position it reports is the venue's own arithmetic — `startPosition + signedSize`
    /// — not an increment onto a local counter, so a fill this connector never saw is
    /// corrected by the next one rather than leaving a permanent offset.
    pub fn apply_fill(&mut self, fill: &UserFill) -> FillDisposition {
        if !self.fills.accept(fill.tid) {
            return FillDisposition::Replay;
        }
        let position = fill.start_position + fill.signed_size();
        let exch_ts = fill.time.checked_mul(1_000_000).unwrap_or(0);

        let order = self
            .is_ours(fill.cloid.as_deref())
            .then_some(fill.cloid.as_deref())
            .flatten()
            .and_then(|cloid| self.orders.get_mut(cloid))
            .map(|tracked| {
                tracked.order.exec_qty = fill.sz;
                tracked.order.exec_price_tick = (fill.px / tracked.order.tick_size).round() as i64;
                tracked.order.exch_timestamp = tracked.order.exch_timestamp.max(exch_ts);
                tracked.order.clone()
            });

        FillDisposition::Applied {
            symbol: fill.coin.clone(),
            order,
            position,
            exch_ts,
        }
    }

    /// Takes a request that never reached the venue back out, as `Status::Expired`.
    ///
    /// Not optional bookkeeping: without it the bot keeps a `Status::New` order that exists
    /// nowhere, and `submit_order` with that id returns `OrderIdExist` for ever after.
    pub fn submit_failed(&mut self, cloid: &str) -> Option<OrderExt> {
        let mut removed = self.orders.remove(cloid)?;
        self.order_id_map.remove(&RefSymbolOrderId::new(
            &removed.symbol,
            removed.order.order_id,
        ));
        removed.order.req = Status::None;
        removed.order.status = Status::Expired;
        Some(removed)
    }

    /// Clears the pending flag on a cancel the venue refused, leaving the order live.
    ///
    /// The order is deliberately **not** removed: a refused cancel usually means the order
    /// already went away, and `orderUpdates` is what says so. Removing it here would drop
    /// an order that may still be resting.
    pub fn cancel_failed(&mut self, cloid: &str) -> Option<OrderExt> {
        let tracked = self.orders.get_mut(cloid)?;
        tracked.order.req = Status::None;
        Some(tracked.clone())
    }

    /// Marks the request that is now in flight, so the bot can see it is pending.
    pub fn mark_requested(&mut self, cloid: &str, req: Status) {
        if let Some(tracked) = self.orders.get_mut(cloid) {
            tracked.order.req = req;
        }
    }

    /// Records the venue id `/exchange` returned for an accepted order, and that the submit
    /// request has been answered.
    ///
    /// The id usually arrives on `orderUpdates` a moment later anyway, but not always first
    /// — and until something supplies it the order cannot be reconciled after a reconnect.
    /// The request flag matters as much: it is what lets reconciliation tell "the venue has
    /// never heard of this order" from "the venue has not answered yet".
    ///
    /// `req` is cleared here too. The bot inserted the order as `Status::New` on both
    /// fields before the request left; the venue has now answered, and nothing else clears
    /// it if the `orderUpdates` frame that would have is lost.
    pub fn note_venue_ack(&mut self, cloid: &str, oid: u64) {
        if let Some(tracked) = self.orders.get_mut(cloid) {
            tracked.oid = Some(oid);
            tracked.order.req = Status::None;
            tracked.settled_at = Some(Instant::now());
        }
    }

    /// Records that the `/exchange` request for an order finished without an answer about
    /// it — a transport failure, where the order may be resting all the same.
    ///
    /// The order is kept (§11.7); this clears the pending flag the bot set before the
    /// request left, and stops reconciliation from treating the order as still in flight for
    /// ever.
    pub fn request_settled(&mut self, cloid: &str) {
        if let Some(tracked) = self.orders.get_mut(cloid) {
            tracked.order.req = Status::None;
            tracked.settled_at = Some(Instant::now());
        }
    }

    /// Takes out the order the venue **confirmed** it cancelled, as `Status::Canceled`.
    ///
    /// Keyed by the venue's own id, because the cancel that clears a previous run's orders
    /// is sourced from the venue's open-order list rather than from this map. An id this
    /// process does not track is not an error: it is a human's order, or the previous run's,
    /// and there is nothing local to take out.
    pub fn cancel_confirmed(&mut self, oid: u64) -> Option<OrderExt> {
        let cloid = self
            .orders
            .iter()
            .find(|(_, tracked)| tracked.oid == Some(oid))
            .map(|(cloid, _)| cloid.clone())?;
        let mut removed = self.orders.remove(&cloid)?;
        self.order_id_map.remove(&RefSymbolOrderId::new(
            &removed.symbol,
            removed.order.order_id,
        ));
        removed.order.req = Status::None;
        removed.order.status = Status::Canceled;
        Some(removed)
    }

    /// Reconciles this connector's view against the venue's open orders.
    ///
    /// **Only a reconnect can tell the connector it is wrong.** `orderUpdates` gaps are not
    /// recoverable from the stream — nothing re-sends them — and Hyperliquid's resting
    /// orders survive disconnects, restarts and API-wallet expiry, so a connector that does
    /// not re-ask starts up believing an account is flat while it holds live orders
    /// (design note §5.10).
    ///
    /// Returns the orders that are gone as far as the venue is concerned, already marked
    /// `Status::Expired`, for publishing. Orders on the venue that this process does not
    /// know are **not** adopted: their `order_id` belongs to a bot that no longer exists,
    /// and inventing one would collide with a live bot's numbering. They are reported by
    /// the caller so an operator can see them, and cancelled under `connect_policy =
    /// "cancel_all"`.
    ///
    /// `queried_at` is the moment the open-orders query **went out**, and it is what makes
    /// this safe to run concurrently with `submit_order`: an answer assembled before an
    /// order was acknowledged says nothing about that order, and an order whose request has
    /// not answered at all is never expired (§11.6).
    pub fn reconcile(&mut self, open: &[OpenOrder], queried_at: Instant) -> Vec<OrderExt> {
        let open_oids: hashbrown::HashSet<u64> = open.iter().map(|order| order.oid).collect();
        let open_cloids: hashbrown::HashSet<String> = open
            .iter()
            .filter_map(|order| order.cloid.as_ref())
            .map(|cloid| cloid.to_lowercase())
            .collect();
        // Whether the venue's answer carried client ids at all. If it did not, a missing
        // client id proves nothing and must not expire a live order — which is the failure
        // that matters here, because a bot told its resting orders are gone will re-quote
        // on top of them.
        let cloids_are_evidence = !open_cloids.is_empty();

        let vanished: Vec<Cloid> = self
            .orders
            .iter()
            .filter(|(cloid, tracked)| {
                if !tracked.order.active() {
                    return false;
                }
                // The answer has to be about this order to say anything about it: its
                // request must have finished, and finished before the query went out.
                let settled_before_the_query =
                    tracked.settled_at.is_some_and(|at| at <= queried_at);
                if !settled_before_the_query {
                    return false;
                }
                match tracked.oid {
                    // Known to the venue by id: present or gone, no ambiguity.
                    Some(oid) => !open_oids.contains(&oid),
                    // Answered, but with no venue id — a transport failure. Expired only on
                    // positive evidence: the venue listed client ids and this one is not
                    // among them.
                    None => cloids_are_evidence && !open_cloids.contains(&cloid.to_lowercase()),
                }
            })
            .map(|(cloid, _)| cloid.clone())
            .collect();

        vanished
            .into_iter()
            .filter_map(|cloid| {
                warn!(
                    %cloid,
                    "Hyperliquid does not list an order this connector believes is resting; \
                     expiring it. The lifecycle update was lost with the connection."
                );
                self.submit_failed(&cloid)
            })
            .collect()
    }
}

impl GetOrders for OrderManager {
    fn orders(&self, symbol: Option<String>) -> Vec<Order> {
        self.orders
            .values()
            .filter(|tracked| {
                symbol
                    .as_ref()
                    .map(|wanted| tracked.symbol == *wanted)
                    .unwrap_or(true)
                    && tracked.order.active()
            })
            .map(|tracked| tracked.order.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use hftbacktest::types::{OrdType, Order, Side, Status, TimeInForce};

    use crate::{
        connector::{ApplyOutcome, GetOrders},
        hyperliquid::{
            HyperliquidError,
            fixtures::{ORDER_UPDATE_CANCELED, ORDER_UPDATE_OPEN, USER_FILLS_INCREMENT},
            msg::{Frame, parse_frame},
            ordermanager::{FillDisposition, OrderManager, PREFIX_LEN},
            rest::{OpenOrder, SymbolInfo},
        },
    };

    /// The prefix the fixtures' client ids carry.
    const PREFIX: &str = "a1f0";

    fn btc() -> SymbolInfo {
        SymbolInfo {
            wire: "BTC".into(),
            dex: String::new(),
            sz_decimals: 5,
            asset_index: Some(3),
        }
    }

    fn manager() -> OrderManager {
        OrderManager::new(PREFIX).unwrap()
    }

    /// A bot order at `price_tick * tick_size`, on the finest legal BTC grid (0.1).
    fn bot_order(order_id: u64, price: f64, qty: f64, side: Side) -> Order {
        let mut order = Order::new(
            order_id,
            (price / 0.1).round() as i64,
            0.1,
            qty,
            side,
            OrdType::Limit,
            TimeInForce::GTX,
        );
        // What `LiveBot::submit_order` actually puts on the wire (`live/bot.rs`): the order
        // is already `New` on both fields before the connector ever sees it, which is why
        // an in-flight order is reported by `orders()` and why a rejection has to expire it.
        order.status = Status::New;
        order.req = Status::New;
        order
    }

    fn updates(text: &str) -> Vec<crate::hyperliquid::msg::OrderUpdate> {
        let Frame::OrderUpdates(updates) = parse_frame(text).unwrap() else {
            panic!("expected an orderUpdates frame");
        };
        updates
    }

    fn fills(text: &str) -> Vec<crate::hyperliquid::msg::UserFill> {
        let Frame::UserFills(fills) = parse_frame(text).unwrap() else {
            panic!("expected a userFills frame");
        };
        fills.fills
    }

    /// The client id is a fixed-width 128-bit value, so the prefix has to live in the high
    /// nibbles rather than as readable text. Getting the width wrong is an HTTP 422 from
    /// `/exchange`, and getting the prefix wrong is worse: this connector then treats a
    /// human's orders on the same account as its own.
    #[test]
    fn a_minted_client_id_is_exactly_32_hex_characters_behind_the_prefix() {
        let mut manager = manager();
        let mut seen = std::collections::HashSet::new();
        for order_id in 0..500u64 {
            let (cloid, wire) = manager
                .new_order(
                    "BTC",
                    &btc(),
                    3,
                    bot_order(order_id, 31700.0, 0.001, Side::Buy),
                )
                .unwrap();

            let body = cloid.strip_prefix("0x").unwrap().to_string();
            assert_eq!(body.len(), 32, "{cloid}");
            assert!(body.chars().all(|c| c.is_ascii_hexdigit()), "{cloid}");
            assert_eq!(&body[..PREFIX_LEN], PREFIX, "{cloid}");
            assert_eq!(wire.c.as_deref(), Some(cloid.as_str()));
            assert!(manager.is_ours(wire.c.as_deref()));
            assert!(seen.insert(cloid), "a client id was minted twice");
        }
    }

    /// Somebody else's order on the same account must be **ignored**, not reported as an
    /// unknown order — the account is shared with a human and a UI.
    #[test]
    fn a_foreign_or_absent_client_id_is_not_ours() {
        let manager = manager();
        assert!(!manager.is_ours(None), "an order placed in the UI");
        assert!(!manager.is_ours(Some("0xffff000000000000000000000000dead")));
        assert!(!manager.is_ours(Some("0xa1f0")), "too short");
        assert!(
            !manager.is_ours(Some("a1f0000000000000000000000000dead")),
            "no 0x"
        );
        assert!(manager.is_ours(Some("0xa1f0000000000000000000000000dead")));
        // The venue lowercases what it echoes, but a mixed-case id must still match.
        assert!(manager.is_ours(Some("0xA1F0000000000000000000000000DEAD")));
    }

    /// **A client id is venue text, and venue text is not trusted.** A body 32 *bytes* long
    /// whose fifth byte is inside a multi-byte character panics a `str` slice — and a panic
    /// in this process is `exit(1)` under the connector's own hook (`AGENTS.md` §4.7), on
    /// every frame the venue re-sends.
    #[test]
    fn a_client_id_that_is_not_ascii_hex_is_refused_rather_than_panicking() {
        let manager = manager();
        // 3 ASCII + 'é' (2 bytes, spanning byte indices 3..5) + 27 ASCII = 32 bytes.
        let awkward = format!("0xa1f\u{e9}{}", "0".repeat(27));
        assert_eq!(awkward.strip_prefix("0x").unwrap().len(), 32);
        assert!(!manager.is_ours(Some(&awkward)));

        // The same length in ASCII that is not hexadecimal is likewise not ours.
        assert!(!manager.is_ours(Some(&format!("0xa1f0{}", "z".repeat(28)))));
    }

    /// The prefix is validated where it is configured. A malformed one is not a cosmetic
    /// problem: it makes every client id an HTTP 422, i.e. every order silently unsendable.
    #[test]
    fn a_prefix_that_is_not_four_hex_characters_is_refused_at_construction() {
        for bad in ["", "abc", "abcde", "zzzz", "a1-2", "a1 2"] {
            assert!(OrderManager::new(bad).is_err(), "{bad:?}");
        }
        assert!(OrderManager::new("A1B2").is_ok(), "case is normalised");
        assert!(OrderManager::new(" a1b2 ").is_ok(), "whitespace is trimmed");
    }

    /// The order this connector remembers carries the price the **venue** accepted, not the
    /// one the bot asked for. Without that the bot's book of its own orders disagrees with
    /// the exchange by up to a tick, permanently and invisibly (design note §5.3).
    #[test]
    fn the_recorded_order_carries_the_accepted_price_not_the_requested_one() {
        let mut manager = manager();
        // 31700.05 is on the bot's finest grid (0.1) and off the venue's — five
        // significant figures leave no decimals at this magnitude.
        let (cloid, wire) = manager
            .new_order(
                "BTC",
                &btc(),
                3,
                bot_order(7, 31700.05, 0.0012345, Side::Buy),
            )
            .unwrap();
        let cloid = cloid.clone();

        // Rounded toward passive for a buy, and floored to the lot grid.
        assert_eq!(wire.p, "31700");
        assert_eq!(wire.s, "0.00123");
        assert!(wire.b);
        assert!(!wire.r);
        assert_eq!(wire.t.limit.tif, "Alo");

        let recorded = manager.orders(Some("BTC".into()));
        assert_eq!(recorded.len(), 1);
        assert_eq!(
            recorded[0].price_tick as f64 * recorded[0].tick_size,
            31700.0
        );
        assert_eq!(recorded[0].qty, 0.00123);
        assert_eq!(recorded[0].leaves_qty, 0.00123);

        // A sell at the same price rounds the other way.
        let (_, wire) = manager
            .new_order("BTC", &btc(), 3, bot_order(8, 31700.05, 0.001, Side::Sell))
            .unwrap();
        assert_eq!(wire.p, "31701");
        assert!(!wire.b);

        // And the cancel finds the same client id from the bot's own order id.
        let (found, cancel) = manager.cancel_order("BTC", 7, 3).unwrap();
        assert_eq!(found, cloid);
        assert_eq!(cancel.cloid, cloid);
        assert_eq!(cancel.asset, 3);
    }

    /// An order id already live must not be reissued — the bot would lose track of which
    /// venue order the id refers to.
    #[test]
    fn the_same_order_id_cannot_be_live_twice_on_one_symbol() {
        let mut manager = manager();
        manager
            .new_order("BTC", &btc(), 3, bot_order(7, 31700.0, 0.001, Side::Buy))
            .unwrap();
        assert!(
            manager
                .new_order("BTC", &btc(), 3, bot_order(7, 31700.0, 0.001, Side::Buy))
                .is_err()
        );
        // The same number on a different symbol is a different order.
        let eth = SymbolInfo {
            wire: "ETH".into(),
            dex: String::new(),
            sz_decimals: 4,
            asset_index: Some(1),
        };
        assert!(
            manager
                .new_order("ETH", &eth, 1, bot_order(7, 2000.5, 1.0, Side::Buy))
                .is_ok()
        );
    }

    /// **The Hyperliquid backend places limit orders only (Finding 4).** `new_order` built a
    /// limit wire regardless of `order.order_type`, so a `Market` or `Unsupported` order was
    /// silently sent as a limit at the passed price — the "green in backtest, wrong in live"
    /// divergence. It must refuse a non-limit type, the way `tif_of` refuses FOK, and refuse
    /// **before** any state is minted so a refusal leaves no residue and frees the id.
    #[test]
    fn a_non_limit_order_type_is_refused_rather_than_silently_sent_as_a_limit() {
        let mut manager = manager();

        // A limit order is accepted, exactly as before.
        assert!(
            manager
                .new_order("BTC", &btc(), 3, bot_order(1, 31700.0, 0.001, Side::Buy))
                .is_ok()
        );

        // A market order — with a perfectly valid TIF, so it is the order type that is
        // refused, not the time-in-force — is an error, not a silent limit.
        let market = Order::new(
            2,
            (31700.0_f64 / 0.1).round() as i64,
            0.1,
            0.001,
            Side::Buy,
            OrdType::Market,
            TimeInForce::GTC,
        );
        assert!(
            matches!(
                manager.new_order("BTC", &btc(), 3, market),
                Err(HyperliquidError::InvalidOrder(_))
            ),
            "a Market order must be refused, not coerced to a limit"
        );
        // …and an unsupported order type likewise.
        let unsupported = Order::new(
            3,
            (31700.0_f64 / 0.1).round() as i64,
            0.1,
            0.001,
            Side::Buy,
            OrdType::Unsupported,
            TimeInForce::GTC,
        );
        assert!(matches!(
            manager.new_order("BTC", &btc(), 3, unsupported),
            Err(HyperliquidError::InvalidOrder(_))
        ));

        // Neither refusal left a residue: only the one accepted limit is tracked, and both
        // refused order ids are free to reuse.
        assert_eq!(manager.orders(None).len(), 1);
        assert_eq!(manager.tracked(), 1);
        assert!(
            manager
                .new_order("BTC", &btc(), 3, bot_order(2, 31650.0, 0.001, Side::Buy))
                .is_ok(),
            "a refused market order must not burn its order id"
        );
        assert!(
            manager
                .new_order("BTC", &btc(), 3, bot_order(3, 31640.0, 0.001, Side::Buy))
                .is_ok()
        );
    }

    /// A cancel for something this connector never placed cannot be sent — there is no
    /// client id to send. Reported, not guessed at.
    #[test]
    fn cancelling_an_unknown_order_is_an_error_rather_than_a_guess() {
        let manager = manager();
        assert!(manager.cancel_order("BTC", 99, 3).is_err());
    }

    /// The ordinary lifecycle: `open` leaves the order live, a terminal status removes it
    /// from the manager so `orders()` stops reporting it.
    #[test]
    fn an_order_goes_live_on_open_and_is_dropped_when_it_ends() {
        let mut manager = manager();
        let (_, _) = manager
            .new_order("BTC", &btc(), 3, bot_order(7, 31700.0, 0.00032, Side::Buy))
            .unwrap();
        // Point the fixture's client id at the order just created.
        let cloid = manager.cancel_order("BTC", 7, 3).unwrap().0;

        let mut open = updates(ORDER_UPDATE_OPEN);
        open[0].order.cloid = Some(cloid.clone());
        let live = manager.apply_order_update(&open[0]).published().unwrap();
        assert_eq!(live.symbol, "BTC");
        assert_eq!(live.order.status, Status::New);
        assert_eq!(live.order.req, Status::None);
        assert_eq!(live.order.exch_timestamp, 1785261289146 * 1_000_000);
        assert_eq!(manager.orders(None).len(), 1);

        let mut cancelled = updates(ORDER_UPDATE_CANCELED);
        cancelled[0].order.cloid = Some(cloid);
        let done = manager
            .apply_order_update(&cancelled[0])
            .published()
            .unwrap();
        assert_eq!(done.order.status, Status::Canceled);
        assert!(
            manager.orders(None).is_empty(),
            "a dead order is not reported"
        );
        assert_eq!(manager.tracked(), 0);
        // …and the order id is free again.
        assert!(
            manager
                .new_order("BTC", &btc(), 3, bot_order(7, 31700.0, 0.001, Side::Buy))
                .is_ok()
        );
    }

    /// **Finding 3: an unrecognised status keeps the order and escalates — it does not drop
    /// it.** An order the venue reports with a status this connector cannot classify may still
    /// be resting; removing it (the old behaviour, which mapped the catch-all to `Expired` and
    /// then dropped it as terminal) would let the strategy see its price level free and place a
    /// second order — a double position (§1.1). So the order stays exactly as it was,
    /// `orders()` still lists it active, its id stays taken, and the caller is told to
    /// reconcile via [`ApplyOutcome::Unrecognised`].
    #[test]
    fn an_unrecognised_status_keeps_the_order_and_signals_reconcile() {
        let mut manager = manager();
        manager
            .new_order("BTC", &btc(), 3, bot_order(7, 31700.0, 0.00032, Side::Buy))
            .unwrap();
        let cloid = manager.cancel_order("BTC", 7, 3).unwrap().0;

        // It is live and resting.
        let mut open = updates(ORDER_UPDATE_OPEN);
        open[0].order.cloid = Some(cloid.clone());
        open[0].status_timestamp = 1_000_000;
        assert!(matches!(
            manager.apply_order_update(&open[0]),
            ApplyOutcome::Publish(_)
        ));

        // The venue now reports a status nobody has seen before, at a later timestamp.
        let mut bogus = updates(ORDER_UPDATE_OPEN);
        bogus[0].order.cloid = Some(cloid);
        bogus[0].status = "somethingEntirelyNew".into();
        bogus[0].status_timestamp = 2_000_000;
        assert!(
            matches!(
                manager.apply_order_update(&bogus[0]),
                ApplyOutcome::Unrecognised
            ),
            "an unrecognised status must signal reconcile, not publish a drop"
        );

        // Untouched: still tracked, still active (New, not rewritten to Unsupported), still
        // cancellable — the strategy can neither re-use the id nor re-quote the price level.
        assert_eq!(manager.tracked(), 1);
        let live = manager.orders(None);
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].status, Status::New);
        assert!(manager.cancel_order("BTC", 7, 3).is_ok());
    }

    /// **A rejected order must not leave a phantom.** `LiveBot::submit_order` inserts the
    /// order as `Status::New` before the request leaves and never removes it on an `Error`
    /// event, so an order the venue refuses has to come back as `Status::Expired` — or the
    /// bot holds an order that exists nowhere and burns that id for the life of the process.
    #[test]
    fn a_rejected_order_is_expired_and_gives_its_order_id_back() {
        let mut manager = manager();
        let (cloid, _) = manager
            .new_order("BTC", &btc(), 3, bot_order(7, 31700.0, 0.001, Side::Buy))
            .unwrap();

        let expired = manager.submit_failed(&cloid).unwrap();
        assert_eq!(expired.order.status, Status::Expired);
        assert_eq!(expired.order.req, Status::None);
        assert!(!expired.order.active());
        assert!(manager.orders(None).is_empty());
        assert!(
            manager
                .new_order("BTC", &btc(), 3, bot_order(7, 31700.0, 0.001, Side::Buy))
                .is_ok(),
            "the order id must be reusable after a rejection"
        );
    }

    /// A cancel the venue refused leaves the order **alone**. It usually means the order is
    /// already gone and `orderUpdates` will say so; dropping it here would lose an order
    /// that may still be resting.
    #[test]
    fn a_refused_cancel_clears_the_request_but_keeps_the_order() {
        let mut manager = manager();
        let (cloid, _) = manager
            .new_order("BTC", &btc(), 3, bot_order(7, 31700.0, 0.001, Side::Buy))
            .unwrap();
        manager.mark_requested(&cloid, Status::Canceled);
        assert!(manager.orders(None)[0].pending());

        let still_there = manager.cancel_failed(&cloid).unwrap();
        assert_eq!(still_there.order.req, Status::None);
        assert_eq!(manager.orders(None).len(), 1);
    }

    /// Updates can arrive out of order, and a stale one must not overwrite a newer state.
    #[test]
    fn an_out_of_order_status_does_not_rewind_the_order() {
        let mut manager = manager();
        manager
            .new_order("BTC", &btc(), 3, bot_order(7, 31700.0, 0.00032, Side::Buy))
            .unwrap();
        let cloid = manager.cancel_order("BTC", 7, 3).unwrap().0;

        let mut newer = updates(ORDER_UPDATE_OPEN);
        newer[0].order.cloid = Some(cloid.clone());
        newer[0].status_timestamp = 1_000_000;
        manager.apply_order_update(&newer[0]).published().unwrap();

        let mut older = updates(ORDER_UPDATE_CANCELED);
        older[0].order.cloid = Some(cloid);
        older[0].status_timestamp = 999_999;
        assert!(
            manager.apply_order_update(&older[0]).published().is_none(),
            "a stale update must be ignored"
        );
        assert_eq!(manager.orders(None)[0].status, Status::New);
        assert_eq!(manager.orders(None).len(), 1);
    }

    /// A duplicate of the *same* update is idempotent rather than a resurrection: the
    /// terminal one removed the order, and the repeat finds nothing to change.
    #[test]
    fn repeating_a_terminal_update_does_not_bring_the_order_back() {
        let mut manager = manager();
        manager
            .new_order("BTC", &btc(), 3, bot_order(7, 31700.0, 0.00032, Side::Buy))
            .unwrap();
        let cloid = manager.cancel_order("BTC", 7, 3).unwrap().0;

        let mut cancelled = updates(ORDER_UPDATE_CANCELED);
        cancelled[0].order.cloid = Some(cloid);
        assert!(
            manager
                .apply_order_update(&cancelled[0])
                .published()
                .is_some()
        );
        assert!(
            manager
                .apply_order_update(&cancelled[0])
                .published()
                .is_none()
        );
        assert!(manager.orders(None).is_empty());
    }

    /// An update for an order on the same account that this connector did not place is
    /// ignored — quietly. It is neither an error nor something to act on.
    #[test]
    fn an_update_for_someone_elses_order_is_ignored() {
        let mut manager = manager();
        let mut foreign = updates(ORDER_UPDATE_OPEN);
        foreign[0].order.cloid = Some("0xffff000000000000000000000000dead".into());
        assert!(
            manager
                .apply_order_update(&foreign[0])
                .published()
                .is_none()
        );

        let mut manual = updates(ORDER_UPDATE_OPEN);
        manual[0].order.cloid = None;
        assert!(manager.apply_order_update(&manual[0]).published().is_none());
    }

    /// **The reconnect double-count.** `userFills` replays its whole history with
    /// `isSnapshot: true` on every subscribe, so without a guard one reconnect applies each
    /// live order's fills a second time and doubles `exec_qty`.
    #[test]
    fn a_replayed_fill_is_applied_exactly_once() {
        let mut manager = manager();
        manager
            .new_order("BTC", &btc(), 3, bot_order(7, 31700.0, 0.00032, Side::Buy))
            .unwrap();
        let cloid = manager.cancel_order("BTC", 7, 3).unwrap().0;

        let mut fill = fills(USER_FILLS_INCREMENT).remove(0);
        fill.cloid = Some(cloid);

        let FillDisposition::Applied {
            symbol,
            order,
            position,
            exch_ts,
        } = manager.apply_fill(&fill)
        else {
            panic!("the first delivery of a fill is not a replay");
        };
        assert_eq!(symbol, "BTC");
        assert_eq!(order.unwrap().exec_qty, 0.00016);
        assert_eq!(position, 0.00016, "startPosition 0 plus a 0.00016 buy");
        assert_eq!(exch_ts, 1785261290000 * 1_000_000);

        // The same fill again — which is exactly what a reconnect delivers.
        assert!(manager.apply_fill(&fill).is_replay());
        assert_eq!(manager.replayed_fills(), 1);
        assert_eq!(manager.orders(None)[0].exec_qty, 0.00016, "not doubled");
    }

    /// The position the venue states is **absolute**, so a fill this connector never saw
    /// corrects itself on the next one instead of leaving a permanent offset. And a fill on
    /// an order placed elsewhere on the account still moves the account's position.
    #[test]
    fn the_position_comes_from_the_venue_not_from_a_local_counter() {
        let mut manager = manager();
        let mut fill = fills(USER_FILLS_INCREMENT).remove(0);

        // Someone else's order — no tracked order, but the account still holds the coin.
        fill.cloid = None;
        fill.tid = 1;
        fill.start_position = 5.0;
        fill.sz = 2.0;
        fill.side = "A".into();
        let FillDisposition::Applied {
            order, position, ..
        } = manager.apply_fill(&fill)
        else {
            panic!("a new fill is not a replay");
        };
        assert!(order.is_none(), "not this connector's order");
        assert_eq!(position, 3.0, "5 held, 2 sold");

        // A later fill states its own starting point, so a gap heals rather than compounds.
        fill.tid = 2;
        fill.start_position = -10.0;
        fill.sz = 1.0;
        fill.side = "B".into();
        let FillDisposition::Applied { position, .. } = manager.apply_fill(&fill) else {
            panic!("a new fill is not a replay");
        };
        assert_eq!(position, -9.0);
    }

    /// **What a reconnect is for.** `orderUpdates` gaps cannot be recovered from the
    /// stream, and Hyperliquid's resting orders survive everything — so the only way to
    /// learn that an order died while the socket was down is to ask. An order the venue no
    /// longer lists is expired back to the bot rather than quietly kept.
    #[test]
    fn reconciliation_expires_orders_the_venue_no_longer_lists() {
        let mut manager = manager();
        let (alive, _) = manager
            .new_order("BTC", &btc(), 3, bot_order(7, 31700.0, 0.001, Side::Buy))
            .unwrap();
        let (gone, _) = manager
            .new_order("BTC", &btc(), 3, bot_order(8, 31600.0, 0.001, Side::Buy))
            .unwrap();

        // The venue reports both by its own id; only the first is still there.
        manager.note_venue_ack(&alive, 101);
        manager.note_venue_ack(&gone, 102);
        let queried_at = Instant::now();
        let expired = manager.reconcile(
            &[OpenOrder {
                coin: "BTC".into(),
                oid: 101,
                cloid: Some(alive.to_uppercase()),
            }],
            queried_at,
        );
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].order.order_id, 8);
        assert_eq!(expired[0].order.status, Status::Expired);
        assert_eq!(manager.orders(None).len(), 1);
        assert_eq!(manager.orders(None)[0].order_id, 7);

        // A reconciliation against an empty venue clears everything.
        assert_eq!(manager.reconcile(&[], Instant::now()).len(), 1);
        assert!(manager.orders(None).is_empty());
    }

    /// **An order whose submit is still in flight is never expired** (design note §11.6).
    /// The venue cannot list what it has not been told about yet, and expiring it hands the
    /// `order_id` back while the order goes on to rest — an orphan on the venue that the bot
    /// has been told does not exist.
    #[test]
    fn reconciliation_leaves_an_order_whose_submit_has_not_answered() {
        let mut manager = manager();
        let (resting, _) = manager
            .new_order("BTC", &btc(), 3, bot_order(7, 31700.0, 0.001, Side::Buy))
            .unwrap();
        manager.note_venue_ack(&resting, 101);
        // …and one POSTed, with `/exchange` yet to answer.
        let (in_flight, _) = manager
            .new_order("BTC", &btc(), 3, bot_order(8, 31600.0, 0.001, Side::Buy))
            .unwrap();

        // The venue's answer *does* carry client ids, which is what used to make the
        // in-flight order's absence look like evidence about it.
        let expired = manager.reconcile(
            &[OpenOrder {
                coin: "BTC".into(),
                oid: 101,
                cloid: Some(resting.clone()),
            }],
            Instant::now(),
        );
        assert!(expired.is_empty(), "{expired:?}");
        assert_eq!(manager.orders(None).len(), 2);

        // Once the request has answered — and the answer was a transport failure, so the
        // order has no venue id — the next reconciliation is free to settle it.
        manager.request_settled(&in_flight);
        let expired = manager.reconcile(
            &[OpenOrder {
                coin: "BTC".into(),
                oid: 101,
                cloid: Some(resting),
            }],
            Instant::now(),
        );
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].order.order_id, 8);
    }

    /// The same hazard on the other side of the clock: a query whose answer was assembled
    /// **before** an order was acknowledged says nothing about that order. Reconciliation
    /// runs concurrently with `submit_order`'s spawned task, so this is not hypothetical.
    #[test]
    fn reconciliation_ignores_an_order_acknowledged_after_the_query_started() {
        let mut manager = manager();
        let (cloid, _) = manager
            .new_order("BTC", &btc(), 3, bot_order(7, 31700.0, 0.001, Side::Buy))
            .unwrap();

        let queried_at = Instant::now();
        // The venue answers the submit after the open-orders query went out.
        manager.note_venue_ack(&cloid, 202);

        assert!(manager.reconcile(&[], queried_at).is_empty());
        assert_eq!(manager.orders(None).len(), 1);
        // A later query, whose answer post-dates the acknowledgement, does settle it.
        assert_eq!(manager.reconcile(&[], Instant::now()).len(), 1);
    }

    /// **An order left by a previous run of this connector is not "ours" in any useful
    /// sense.** The prefix is configuration, not process identity, so a prefix match tells
    /// the caller nothing about whether *this* process can act on the order — and counting
    /// it as ours is what makes a restart's orphaned grid invisible.
    #[test]
    fn an_order_this_process_does_not_track_is_reported_however_it_is_labelled() {
        let mut manager = manager();
        let (mine, _) = manager
            .new_order("BTC", &btc(), 3, bot_order(7, 31700.0, 0.001, Side::Buy))
            .unwrap();
        manager.note_venue_ack(&mine, 101);

        let tracked = OpenOrder {
            coin: "BTC".into(),
            oid: 101,
            cloid: Some(mine.to_uppercase()),
        };
        // Same prefix, never seen by this process: the previous run's.
        let previous_run = OpenOrder {
            coin: "BTC".into(),
            oid: 102,
            cloid: Some("0xa1f0000000000000000000000000dead".into()),
        };
        // A human's, from the UI.
        let manual = OpenOrder {
            coin: "BTC".into(),
            oid: 103,
            cloid: None,
        };

        assert!(manager.tracks(&tracked));
        assert!(!manager.tracks(&previous_run));
        assert!(!manager.tracks(&manual));
        // `is_ours` still answers the question it is for — may this connector act on a
        // lifecycle update — and that answer is deliberately different for the previous run.
        assert!(manager.is_ours(previous_run.cloid.as_deref()));
    }

    /// A cancel the venue confirmed takes the order out and hands the `order_id` back. Only
    /// the confirmed one: a cancel that was refused, or whose request never answered, leaves
    /// its order alone for reconciliation to settle.
    #[test]
    fn only_a_confirmed_cancel_takes_the_order_out() {
        let mut manager = manager();
        let (first, _) = manager
            .new_order("BTC", &btc(), 3, bot_order(1, 31700.0, 0.001, Side::Buy))
            .unwrap();
        let (second, _) = manager
            .new_order("BTC", &btc(), 3, bot_order(2, 31600.0, 0.001, Side::Buy))
            .unwrap();
        manager.note_venue_ack(&first, 101);
        manager.note_venue_ack(&second, 102);

        let cancelled = manager.cancel_confirmed(101).unwrap();
        assert_eq!(cancelled.order.order_id, 1);
        assert_eq!(cancelled.order.status, Status::Canceled);
        assert_eq!(cancelled.order.req, Status::None);
        assert_eq!(manager.orders(None).len(), 1);
        assert_eq!(manager.orders(None)[0].order_id, 2);
        // The id is free again.
        assert!(
            manager
                .new_order("BTC", &btc(), 3, bot_order(1, 31700.0, 0.001, Side::Buy))
                .is_ok()
        );

        // An id the venue confirmed that this process never placed is not an error and not
        // an event: there is nothing local to take out.
        assert!(manager.cancel_confirmed(999).is_none());
    }

    /// **Two clocks, one order.** `userFills` stamps `time` and `orderUpdates` stamps
    /// `statusTimestamp`; they are independent venue clocks for the same match. Gating the
    /// lifecycle guard on a watermark the *fill* advanced drops the terminal update whenever
    /// it lands a millisecond earlier — and the bot then holds a filled order for ever,
    /// believing it rests.
    #[test]
    fn a_fill_does_not_make_the_orders_own_terminal_update_look_stale() {
        let mut manager = manager();
        manager
            .new_order("BTC", &btc(), 3, bot_order(7, 31700.0, 0.00032, Side::Buy))
            .unwrap();
        let cloid = manager.cancel_order("BTC", 7, 3).unwrap().0;

        let mut open = updates(ORDER_UPDATE_OPEN);
        open[0].order.cloid = Some(cloid.clone());
        open[0].status_timestamp = 1_000_000;
        manager.apply_order_update(&open[0]).published().unwrap();

        // The fill's clock runs 2 ms ahead of the lifecycle clock.
        let mut fill = fills(USER_FILLS_INCREMENT).remove(0);
        fill.cloid = Some(cloid.clone());
        fill.time = 1_000_002;
        assert!(!manager.apply_fill(&fill).is_replay());

        // …and the venue's own `filled` lands with an earlier stamp.
        let mut filled = updates(ORDER_UPDATE_CANCELED);
        filled[0].order.cloid = Some(cloid);
        filled[0].status = "filled".into();
        filled[0].status_timestamp = 1_000_001;
        let done = manager
            .apply_order_update(&filled[0])
            .published()
            .expect("the terminal update must be applied");
        assert_eq!(done.order.status, Status::Filled);
        assert!(
            manager.orders(None).is_empty(),
            "a filled order must not stay live"
        );
        // The order the bot is handed never rewinds in time: `LiveBot` drops an update whose
        // `exch_timestamp` predates its own copy (`live/bot.rs`).
        assert_eq!(done.order.exch_timestamp, 1_000_002 * 1_000_000);
    }
}
