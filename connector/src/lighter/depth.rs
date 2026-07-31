//! The full book mirror, and the diff that carries it to the bot as kind-1 events.
//!
//! Lighter *is* an incremental feed — one snapshot per subscription, then deltas — so the
//! deltas map onto `LOCAL_*_DEPTH_EVENT` almost directly, and none of Hyperliquid's
//! snapshot-to-delta synthesis is needed. The mirror exists for three other reasons, each
//! sufficient on its own (design note §3.1):
//!
//! 1. **A bot that registers between snapshots would otherwise get a near-empty book.** A
//!    snapshot arrives once per subscription — measured over a day: HYPE, 4 snapshots
//!    against 1 346 477 deltas — and `main.rs` hands a newly registered instrument a
//!    *fresh* `FusedHashMapMarketDepth` while publishing `SnapshotComplete` regardless. Its
//!    book would then be filled only by the 1–4 levels a delta happens to touch, for as
//!    long as it runs, with nothing in any log. [`DepthMirror::restate`] is that moment.
//! 2. **A repair needs a before state.** Recovering from a chain break means diffing the
//!    fresh snapshot against what the bot currently holds; a mirror that is not fed every
//!    delta cannot produce that diff. And a fresh snapshot *cannot* simply be republished:
//!    `LiveBot` drops `DEPTH_SNAPSHOT_EVENT` (`AGENTS.md` §4.1), so it has to be a diff.
//! 3. **The mirror is the only record of what the bot holds**, because unchanged levels are
//!    suppressed.
//!
//! Three invariants, all load-bearing:
//!
//! * **The mirror records only what it published.** A level whose size rounds to zero lots,
//!   and two levels that share a tick, are reconciled *here*: the fused depth in `main.rs`
//!   would drop or overwrite them, and a mirror that believed otherwise would stop
//!   describing the bot's book. Both are counted — see [`MirrorCounts`] for what that count
//!   can and cannot mean, because it is **not** the mismatch the design note advertises.
//! * **Deletions precede inserts, in every frame** — not only on a resync. Every published
//!   event goes through `FusedHashMapMarketDepth`, whose own deletion events are built
//!   without the `LOCAL_EVENT` bit and are therefore dropped by `LiveBot` (`AGENTS.md`
//!   §4.7). A Lighter frame carries both sides at once with no documented ordering: in the
//!   collector's ETH fixture the asks come first, and inside the bids an insert at 1914.24
//!   precedes a deletion at 1914.22. So the ordering is imposed here (§4.6).
//! * **A frame that would leave the published book crossed is refused whole, and counted**
//!   (§4.6) — and a refusal means the mirror no longer describes the venue's book, so the
//!   caller repairs that market rather than carrying on.
//!
//! **Times are microseconds** (§4.4). `last_updated_at` is µs and `timestamp` is ms in the
//! same frame; feeding µs into a millisecond conversion is an error of 1000× that does not
//! fail loudly — it latches: either the clock-lead guard refuses every frame for ever, or,
//! without one, the stale gate in `depth/fuse.rs` does. Measured on Hyperliquid: one frame
//! stamped in the year 2100 rejected 1 000 of 1 000 good frames after it.

use std::collections::{BTreeMap, btree_map::Entry};

use hftbacktest::prelude::{Event, LOCAL_ASK_DEPTH_EVENT, LOCAL_BID_DEPTH_EVENT};
use tracing::warn;

use crate::lighter::msg::Level;

/// How far the venue's clock may lead the local one before a frame is unusable.
///
/// The accepted time is *latched* as this market's monotonic gate, so one frame from the far
/// future would reject every later frame for the life of the process. Generous enough that
/// ordinary skew never trips it — the measured age of a frame at the engine is a median of
/// 19.6 ms with a maximum of 425 ms (§2.1) — and tight enough that a millisecond field read
/// as microseconds does.
const MAX_CLOCK_LEAD_NS: i64 = 60_000_000_000;

/// Why a frame could not be applied. Every variant means the same thing to the caller: this
/// mirror no longer describes the venue's book, so the market must be repaired.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// The exchange time cannot be real — it does not fit in nanoseconds, or it leads the
    /// local clock by more than [`MAX_CLOCK_LEAD_NS`].
    Implausible,
    /// Older than the last frame applied to this market. Applying it would desynchronise the
    /// mirror from the bot, because the fused depth downstream rejects a rewind.
    Stale,
    /// Applying it would publish a book whose best ask is at or below its best bid.
    Crossed,
}

/// What a market's mirror had to refuse, and why. Every field is a silent failure otherwise.
///
/// **What [`Self::collapsed`] and [`Self::sub_lot`] do not measure, and the design note says
/// they do** (§3.5, and `connector/examples/lighter.toml` said so too until this was
/// written): the tick and lot the **bot** registered. Those never reach this backend —
/// `Connector::register` carries a symbol and nothing else (`connector/src/connector.rs:63`),
/// and the bot's own numbers arrive in `RegisterInstrument` and build the fused depth over in
/// `connector/src/main.rs`. This mirror is built from the **catalog's** decimals
/// (`PublicStream`'s `MarketState::track`), so these two counters compare the venue against
/// its own quantisation grid and nothing else. A bot that registered a coarser lot loses its
/// levels in `hftbacktest/src/depth/fuse.rs` — `qty_lot == 0` takes the entry out — where
/// nothing counts them, and these counters still read zero. The only defence against that
/// remains the tick and lot logged per symbol at resolve time (`rest.rs`) and an operator who
/// copies them.
///
/// What they *are* worth: a check of the venue against its own catalog. Both are structurally
/// zero while the feed quantises to the decimals the catalog advertises — measured zero over
/// full-day replays (CRV 2026-07-30, 208 182 deltas; PENDLE 2026-07-29, 342 890 deltas) —
/// so a non-zero count means the addressing itself is wrong: the catalog's decimals do not
/// describe the numbers arriving on the feed, and every price this connector publishes is
/// suspect.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MirrorCounts {
    /// Frames older than the last one applied to this market.
    pub stale: u64,
    /// Frames whose exchange time the local clock says cannot be real.
    pub implausible: u64,
    /// Frames refused for leaving the published book crossed.
    pub crossed: u64,
    /// Venue levels that shared a tick with another and were summed into it. Non-zero means
    /// the venue quoted two prices that its own `supported_price_decimals` cannot tell apart.
    pub collapsed: u64,
    /// Levels whose **positive** size rounded to zero lots and were mirrored as absent.
    /// Non-zero means the venue quoted a size below its own `supported_size_decimals`. An
    /// explicit `"0.0"` is a deletion and is not counted here.
    pub sub_lot: u64,
}

/// A mirrored price level. The price is kept as the venue sent it, so every published event
/// carries the venue's own value rather than one reconstructed from a tick index.
#[derive(Clone, Copy, Debug)]
struct MirrorLevel {
    px: f64,
    qty: f64,
}

/// The connector's model of the book the bot currently holds, for one market.
///
/// Deliberately **not** reset on reconnect: it is what the published events have built, so
/// the fresh snapshot a reconnect brings must be diffed against it. Clearing it would leave
/// every stale level in the bot's book with nothing to remove it.
pub struct DepthMirror {
    tick_size: f64,
    lot_size: f64,
    /// Keyed by price tick so that two spellings of one price cannot become two levels.
    bids: BTreeMap<i64, MirrorLevel>,
    asks: BTreeMap<i64, MirrorLevel>,
    last_exch_ts: i64,
    counts: MirrorCounts,
}

impl DepthMirror {
    pub fn new(tick_size: f64, lot_size: f64) -> Self {
        Self {
            tick_size,
            lot_size,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_exch_ts: i64::MIN,
            counts: MirrorCounts::default(),
        }
    }

    pub fn counts(&self) -> MirrorCounts {
        self.counts
    }

    pub fn best_bid(&self) -> Option<f64> {
        self.bids.values().next_back().map(|level| level.px)
    }

    pub fn best_ask(&self) -> Option<f64> {
        self.asks.values().next().map(|level| level.px)
    }

    /// `(bids, asks)` currently mirrored — what a restatement would cost.
    pub fn depth(&self) -> (usize, usize) {
        (self.bids.len(), self.asks.len())
    }

    fn tick(&self, px: f64) -> i64 {
        (px / self.tick_size).round() as i64
    }

    fn lots(&self, qty: f64) -> i64 {
        (qty / self.lot_size).round() as i64
    }

    /// Every mirrored level as an insert, bids then asks, in an order that is never crossed.
    ///
    /// This is what a bot registering against an already-running connector needs, and the
    /// reason is entirely in `connector/src/main.rs`: a newly registered instrument gets a
    /// **fresh** `FusedHashMapMarketDepth` (the `Entry::Vacant` arm), and the one path that
    /// replays an existing book (`Entry::Occupied` → `depth_.snapshot()`) emits
    /// `DEPTH_SNAPSHOT_EVENT`, which `LiveBot` drops without a word (`AGENTS.md` §4.1).
    /// Meanwhile `SnapshotComplete` is published either way, so the bot is told it is ready.
    ///
    /// On this venue that is not a corner case: the snapshot comes **once per
    /// subscription**, so a bot registering at any other moment is fed only the 1–4 levels
    /// each delta touches, and can sit indefinitely with one side missing and a `NaN` mid.
    /// And a fresh snapshot cannot be asked for — a duplicate subscribe is answered `30003`
    /// with no snapshot (§2.2).
    ///
    /// Stamped with the exchange time of the frame the mirror was built from, so nothing
    /// downstream sees a rewind and a later frame still supersedes it.
    pub fn restate(&self, local_ts: i64) -> Vec<Event> {
        if self.bids.is_empty() && self.asks.is_empty() {
            return Vec::new();
        }
        let mut events = Vec::with_capacity(self.bids.len() + self.asks.len());
        for level in self.bids.values() {
            events.push(depth_event(
                true,
                level.px,
                level.qty,
                self.last_exch_ts,
                local_ts,
            ));
        }
        for level in self.asks.values() {
            events.push(depth_event(
                false,
                level.px,
                level.qty,
                self.last_exch_ts,
                local_ts,
            ));
        }
        events
    }

    /// Applies a `subscribed/order_book` frame: the venue's whole book, **diffed** against
    /// what the bot holds.
    ///
    /// A diff rather than a replay, for both of the moments a snapshot arrives. On the first
    /// one the mirror is empty and the diff is the whole book anyway. On a repair — the only
    /// other way to get one — the bot already holds the pre-break book, and republishing it
    /// wholesale would restate thousands of unchanged levels through a 512-byte-per-event
    /// IPC (§4.7: an ETH snapshot is ~2 900 levels) while leaving every level the break lost
    /// *in place*, because nothing would delete it.
    pub fn on_snapshot(
        &mut self,
        bids: &[Level],
        asks: &[Level],
        exch_ts_us: i64,
        local_ts: i64,
    ) -> Result<Vec<Event>, Refusal> {
        let new_bids = self.side_of(bids);
        let new_asks = self.side_of(asks);
        self.apply(new_bids, new_asks, exch_ts_us, local_ts)
    }

    /// Applies an `update/order_book` frame: only the levels that changed.
    ///
    /// A level the frame does not name is unchanged; a level whose size is zero is gone.
    /// Verified against a real consecutive run (see the tests): the first diff after the CRV
    /// snapshot deletes the best bid with `"size":"0.0"` and moves the touch down a level.
    pub fn on_delta(
        &mut self,
        bids: &[Level],
        asks: &[Level],
        exch_ts_us: i64,
        local_ts: i64,
    ) -> Result<Vec<Event>, Refusal> {
        let mut new_bids = self.bids.clone();
        let mut new_asks = self.asks.clone();
        self.overlay(&mut new_bids, bids);
        self.overlay(&mut new_asks, asks);
        self.apply(new_bids, new_asks, exch_ts_us, local_ts)
    }

    /// Checks a candidate state, adopts it, and returns the difference.
    fn apply(
        &mut self,
        new_bids: BTreeMap<i64, MirrorLevel>,
        new_asks: BTreeMap<i64, MirrorLevel>,
        exch_ts_us: i64,
        local_ts: i64,
    ) -> Result<Vec<Event>, Refusal> {
        // Crossed first, and **before** the monotonic gate is latched: a refused frame must
        // not also poison the timestamp the next good frame is compared against.
        if let (Some(bid), Some(ask)) = (new_bids.keys().next_back(), new_asks.keys().next())
            && ask <= bid
        {
            self.counts.crossed += 1;
            warn!(
                best_bid_tick = bid,
                best_ask_tick = ask,
                exch_ts_us,
                "Refusing a Lighter order-book frame that would publish a crossed book; the \
                 fused depth would answer it with deletion events that carry no LOCAL_EVENT \
                 bit, which LiveBot drops, and the two books would then diverge in silence."
            );
            return Err(Refusal::Crossed);
        }
        let exch_ts = self.accept(exch_ts_us, local_ts)?;
        Ok(self.emit(new_bids, new_asks, exch_ts, local_ts))
    }

    /// Returns the exchange timestamp in nanoseconds, or the reason the frame is unusable.
    ///
    /// Monotonicity is not cosmetic: the fused depth in `main.rs` rejects any event whose
    /// timestamp precedes the state it would update, so an out-of-order frame applied here
    /// would be dropped there and this mirror would stop describing the bot's book.
    ///
    /// Because the accepted timestamp is *latched*, it has to be sane before it is latched.
    /// `local_ts` is the local receive time in nanoseconds; `0` means the local clock could
    /// not be read, and then there is nothing to compare against.
    fn accept(&mut self, exch_ts_us: i64, local_ts: i64) -> Result<i64, Refusal> {
        // Microseconds. §4.4 — the field beside it, `timestamp`, is milliseconds.
        let Some(exch_ts) = exch_ts_us.checked_mul(1_000) else {
            self.implausible(exch_ts_us, "the exchange time does not fit in nanoseconds");
            return Err(Refusal::Implausible);
        };
        if local_ts > 0 && exch_ts.saturating_sub(local_ts) > MAX_CLOCK_LEAD_NS {
            self.implausible(exch_ts_us, "the exchange time leads the local clock");
            return Err(Refusal::Implausible);
        }
        if exch_ts < self.last_exch_ts {
            self.counts.stale += 1;
            return Err(Refusal::Stale);
        }
        self.last_exch_ts = exch_ts;
        Ok(exch_ts)
    }

    fn implausible(&mut self, exch_ts_us: i64, reason: &str) {
        self.counts.implausible += 1;
        // Once per mirror: a systematically wrong clock would otherwise fill the log at the
        // frame rate. The rate is what `public_stream::degraded` alarms on.
        if self.counts.implausible == 1 {
            warn!(
                exch_ts_us,
                reason,
                "Refusing a Lighter frame with an unusable exchange time; applying it would \
                 latch this market's feed shut."
            );
        }
    }

    /// A whole side as the mirror will record it: one entry per tick, nothing below one lot.
    fn side_of(&mut self, levels: &[Level]) -> BTreeMap<i64, MirrorLevel> {
        let mut out = BTreeMap::new();
        self.fold(&mut out, levels);
        out.retain(|_, level| self.lots(level.qty) > 0);
        out
    }

    /// Applies a delta's levels over an existing side: a level with a usable size replaces
    /// what is there, and one without removes it.
    fn overlay(&mut self, side: &mut BTreeMap<i64, MirrorLevel>, levels: &[Level]) {
        let mut changed = BTreeMap::new();
        self.fold(&mut changed, levels);
        for (tick, level) in changed {
            if self.lots(level.qty) > 0 {
                side.insert(tick, level);
            } else {
                // Both a deletion (`"size":"0.0"`) and a size that rounds below one lot land
                // here, and they must: the fused depth downstream deletes a level whose
                // `qty_lot` is zero, so a mirror that recorded it would believe the bot holds
                // a level it does not, and would never re-emit it — the diff only speaks when
                // a size changes. Only the second says anything about the venue, so only it
                // is counted (see `MirrorCounts` for what that is and is not evidence of).
                if level.qty > 0.0 {
                    self.counts.sub_lot += 1;
                }
                side.remove(&tick);
            }
        }
    }

    /// Groups one frame's levels by tick, summing any that collide.
    ///
    /// Two venue prices become one level in the fused depth downstream whatever this does,
    /// so their sizes are summed rather than one silently replacing the other. Unreachable
    /// while the venue quotes on the grid its own catalog advertises — which is why the
    /// collision is counted rather than assumed away ([`MirrorCounts`]).
    fn fold(&mut self, out: &mut BTreeMap<i64, MirrorLevel>, levels: &[Level]) {
        for level in levels {
            match out.entry(self.tick(level.px)) {
                Entry::Occupied(mut entry) => {
                    entry.get_mut().qty += level.qty;
                    self.counts.collapsed += 1;
                }
                Entry::Vacant(entry) => {
                    entry.insert(MirrorLevel {
                        px: level.px,
                        qty: level.qty,
                    });
                }
            }
        }
    }

    /// Diffs the new state against the mirror, emits the difference, and adopts it.
    ///
    /// Deletions for both sides come first — see the module comment — then the levels the
    /// new state introduced or resized.
    fn emit(
        &mut self,
        new_bids: BTreeMap<i64, MirrorLevel>,
        new_asks: BTreeMap<i64, MirrorLevel>,
        exch_ts: i64,
        local_ts: i64,
    ) -> Vec<Event> {
        let mut events = Vec::new();
        self.push_deletions(&mut events, &self.bids, &new_bids, true, exch_ts, local_ts);
        self.push_deletions(&mut events, &self.asks, &new_asks, false, exch_ts, local_ts);
        self.push_updates(&mut events, &self.bids, &new_bids, true, exch_ts, local_ts);
        self.push_updates(&mut events, &self.asks, &new_asks, false, exch_ts, local_ts);
        self.bids = new_bids;
        self.asks = new_asks;
        events
    }

    fn push_deletions(
        &self,
        events: &mut Vec<Event>,
        old: &BTreeMap<i64, MirrorLevel>,
        new: &BTreeMap<i64, MirrorLevel>,
        bid: bool,
        exch_ts: i64,
        local_ts: i64,
    ) {
        for (tick, level) in old {
            if !new.contains_key(tick) {
                events.push(depth_event(bid, level.px, 0.0, exch_ts, local_ts));
            }
        }
    }

    fn push_updates(
        &self,
        events: &mut Vec<Event>,
        old: &BTreeMap<i64, MirrorLevel>,
        new: &BTreeMap<i64, MirrorLevel>,
        bid: bool,
        exch_ts: i64,
        local_ts: i64,
    ) {
        for (tick, level) in new {
            // Compared in lots, not floats: a size that rounds to the same lot is not news,
            // and a repair's snapshot restates thousands of levels that have not moved. What
            // makes the suppression safe is that the mirror only ever holds what was
            // published — including across a bot registering late, which `restate` covers.
            let unchanged = old
                .get(tick)
                .is_some_and(|prev| self.lots(prev.qty) == self.lots(level.qty));
            if !unchanged {
                events.push(depth_event(bid, level.px, level.qty, exch_ts, local_ts));
            }
        }
    }
}

/// Builds a kind-1 local depth event — the only depth kind `LiveBot` applies.
fn depth_event(bid: bool, px: f64, qty: f64, exch_ts: i64, local_ts: i64) -> Event {
    Event {
        ev: if bid {
            LOCAL_BID_DEPTH_EVENT
        } else {
            LOCAL_ASK_DEPTH_EVENT
        },
        exch_ts,
        local_ts,
        order_id: 0,
        px,
        qty,
        ival: 0,
        fval: 0.0,
    }
}

#[cfg(test)]
mod tests {
    use hftbacktest::{
        depth::{HashMapMarketDepth, L2MarketDepth},
        prelude::{
            Event,
            FusedHashMapMarketDepth,
            LOCAL_ASK_DEPTH_EVENT,
            LOCAL_BID_DEPTH_EVENT,
            MarketDepth,
        },
    };

    use crate::lighter::{
        depth::{DepthMirror, Refusal},
        fixtures::{
            ORDER_BOOK_SNAPSHOT_CRV,
            ORDER_BOOK_UPDATE_CRV_1,
            ORDER_BOOK_UPDATE_CRV_2,
            ORDER_BOOK_UPDATE_CRV_3,
            ORDER_BOOK_UPDATE_CRV_4,
            ORDER_BOOK_UPDATE_CRV_5,
            ORDER_BOOK_UPDATE_CRV_6,
        },
        msg::{Book, Frame, Level, parse_frame},
    };

    /// CRV on mainnet: `supported_price_decimals = 5`, `supported_size_decimals = 1`
    /// (`GET /api/v1/orderBooks`, and the fixture catalog beside these frames).
    const TICK: f64 = 1e-5;
    const LOT: f64 = 0.1;

    /// The local receive clock the mirror sanity-checks the venue's time against. A real
    /// one, because the fixtures carry real exchange times.
    const NOW_NS: i64 = 1_785_358_812_000_000_000;

    fn level(px: f64, qty: f64) -> Level {
        Level { px, qty }
    }

    fn book(text: &str) -> Book {
        match parse_frame(text).unwrap() {
            Frame::BookSnapshot(book) | Frame::BookUpdate(book) => book,
            other => panic!("expected a book frame, got {other:?}"),
        }
    }

    fn snapshot(mirror: &mut DepthMirror, text: &str) -> Vec<Event> {
        let book = book(text);
        mirror
            .on_snapshot(&book.bids, &book.asks, book.last_updated_at_us, NOW_NS)
            .unwrap()
    }

    fn delta(mirror: &mut DepthMirror, text: &str) -> Vec<Event> {
        let book = book(text);
        mirror
            .on_delta(&book.bids, &book.asks, book.last_updated_at_us, NOW_NS)
            .unwrap()
    }

    /// The bot's book stores a level by tick index and rebuilds the price from it, so its
    /// `best_bid`/`best_ask` are within a rounding error of the venue's string rather than
    /// equal to it. Compared at a hundredth of a tick, which is far finer than the grid and
    /// far coarser than the reconstruction error.
    fn assert_price(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < TICK / 100.0,
            "expected {expected}, got {actual}"
        );
    }

    /// `(side, px, qty)` of each event, in emission order.
    fn shape(events: &[Event]) -> Vec<(char, f64, f64)> {
        events
            .iter()
            .map(|e| {
                let side = if e.is(LOCAL_BID_DEPTH_EVENT) {
                    'B'
                } else if e.is(LOCAL_ASK_DEPTH_EVENT) {
                    'A'
                } else {
                    panic!("event {:#x} is neither a bid nor an ask depth event", e.ev)
                };
                (side, e.px, e.qty)
            })
            .collect()
    }

    /// The one assertion that permanently prevents the `AGENTS.md` §4.1 failure:
    /// `LiveBot::process_event` applies only kind 1 and drops `DEPTH_SNAPSHOT_EVENT` (kind
    /// 4) and `DEPTH_BBO_EVENT` (kind 5) without a word.
    fn assert_all_kind_one_local(events: &[Event]) {
        for event in events {
            assert!(
                event.is(LOCAL_BID_DEPTH_EVENT) || event.is(LOCAL_ASK_DEPTH_EVENT),
                "event flags {:#x} are not a kind-1 local depth event; LiveBot would drop it",
                event.ev
            );
        }
    }

    /// The bot's own book, driven exactly as the connector drives it: through the same
    /// `FusedHashMapMarketDepth` `connector/src/main.rs` puts in the way, applying only the
    /// kind-1 events and dropping everything else, as `LiveBot::process_event` does.
    struct Downstream {
        fused: FusedHashMapMarketDepth,
        bot: HashMapMarketDepth,
    }

    impl Downstream {
        fn new() -> Self {
            Self {
                fused: FusedHashMapMarketDepth::new(TICK, LOT),
                bot: HashMapMarketDepth::new(TICK, LOT),
            }
        }

        fn feed(&mut self, events: Vec<Event>) {
            for event in events {
                let out = if event.is(LOCAL_BID_DEPTH_EVENT) {
                    self.fused.update_bid_depth(event.clone())
                } else {
                    self.fused.update_ask_depth(event.clone())
                };
                for event in out {
                    if event.is(LOCAL_BID_DEPTH_EVENT) {
                        self.bot
                            .update_bid_depth(event.px, event.qty, event.exch_ts);
                    } else if event.is(LOCAL_ASK_DEPTH_EVENT) {
                        self.bot
                            .update_ask_depth(event.px, event.qty, event.exch_ts);
                    }
                }
            }
        }
    }

    /// The whole first snapshot becomes one insert per level, and the exchange time is
    /// carried through from **microseconds** — the field beside it in the same frame is
    /// milliseconds (§4.4).
    #[test]
    fn the_first_snapshot_becomes_one_insert_per_level() {
        let mut mirror = DepthMirror::new(TICK, LOT);
        let events = snapshot(&mut mirror, ORDER_BOOK_SNAPSHOT_CRV);

        assert_all_kind_one_local(&events);
        assert_eq!(events.len(), 79, "39 bids and 40 asks");
        assert_eq!(mirror.depth(), (39, 40));
        assert_eq!(mirror.best_bid(), Some(0.20351));
        assert_eq!(mirror.best_ask(), Some(0.20382));
        assert_eq!(events[0].exch_ts, 1_785_358_811_946_459 * 1_000);
        assert_eq!(events[0].local_ts, NOW_NS);
    }

    /// **The delta semantics, verified against real consecutive frames rather than assumed.**
    /// The design note says to verify them, and this is that: in the first diff after the
    /// snapshot the venue deletes the best bid with `"size":"0.0"` and names three other
    /// levels with sizes; the touch must move down to the next mirrored level, and every
    /// level the frame did not name must stay exactly where it was.
    #[test]
    fn a_delta_deletes_a_zero_size_level_and_replaces_the_rest() {
        let mut mirror = DepthMirror::new(TICK, LOT);
        snapshot(&mut mirror, ORDER_BOOK_SNAPSHOT_CRV);
        let before = mirror.depth();

        let events = delta(&mut mirror, ORDER_BOOK_UPDATE_CRV_1);
        assert_all_kind_one_local(&events);
        assert_eq!(
            shape(&events),
            [
                // Deletions first — both sides — then the inserts, whatever order the venue
                // used. In this frame the venue sent asks first and put the deletion of
                // 0.20421 after an insert; within a side the diff walks the book upwards.
                ('B', 0.20308, 0.0),
                ('B', 0.20351, 0.0),
                ('A', 0.20421, 0.0),
                ('B', 0.20302, 25775.2),
                ('A', 0.20416, 25374.8),
            ]
        );
        // The touch moved down one level: 0.20351 is gone, and the level below it — which
        // this frame never mentions — is untouched.
        assert_eq!(mirror.best_bid(), Some(0.20350));
        assert_eq!(mirror.best_ask(), Some(0.20382));
        assert_eq!(
            mirror.depth(),
            (before.0 - 1, before.1),
            "two bids deleted and one added; one ask deleted and one added"
        );
    }

    /// **The empty-book trap, pinned** (design note §3.1, `AGENTS.md` §4.1). A snapshot
    /// arrives once per subscription — HYPE over a recorded day: 4 snapshots against
    /// 1 346 477 deltas — so a bot that registers at any other moment is fed only the 1–4
    /// levels each delta touches, while `main.rs` publishes `SnapshotComplete` regardless.
    ///
    /// Without the restatement its book is what the deltas since happen to have named: here,
    /// four levels out of seventy-nine, with a best ask that is not the market's.
    #[test]
    fn a_bot_registering_between_snapshots_is_given_the_whole_book() {
        let mut mirror = DepthMirror::new(TICK, LOT);
        snapshot(&mut mirror, ORDER_BOOK_SNAPSHOT_CRV);

        // What the connector publishes while no bot is attached.
        let mut only_deltas = Downstream::new();
        for text in [ORDER_BOOK_UPDATE_CRV_1, ORDER_BOOK_UPDATE_CRV_2] {
            only_deltas.feed(delta(&mut mirror, text));
        }
        assert!(
            only_deltas.bot.best_ask() != 0.20382,
            "the deltas alone cannot have built the real book"
        );

        // The bot registers now, and `main.rs` gives it a fresh, empty fusion.
        let mut downstream = Downstream::new();
        let restatement = mirror.restate(7);
        assert_all_kind_one_local(&restatement);
        assert_eq!(
            restatement.len(),
            78,
            "the book as the deltas left it — 38 bids and 40 asks — not the snapshot"
        );
        assert_eq!(restatement.len(), mirror.depth().0 + mirror.depth().1);
        assert_eq!(restatement[0].local_ts, 7);
        downstream.feed(restatement);

        assert_price(downstream.bot.best_bid(), 0.20350);
        assert_price(downstream.bot.best_ask(), 0.20382);

        // And restating changed nothing, so the next real delta still diffs against what the
        // bot holds.
        downstream.feed(delta(&mut mirror, ORDER_BOOK_UPDATE_CRV_3));
        assert_price(downstream.bot.best_ask(), mirror.best_ask().unwrap());
    }

    /// A restatement of an empty mirror is nothing, not a batch of zero-quantity events: the
    /// connector may be asked for one before the first frame of a market has arrived.
    #[test]
    fn restating_an_empty_mirror_says_nothing() {
        assert!(DepthMirror::new(TICK, LOT).restate(1).is_empty());
    }

    /// The restatement carries the exchange time of the frame the mirror was built from, so
    /// a bot that already holds the book sees no rewind — the fused depth rejects an event
    /// older than the level it would update — and the next real frame still supersedes it.
    #[test]
    fn a_restatement_does_not_rewind_a_book_that_is_already_there() {
        let mut mirror = DepthMirror::new(TICK, LOT);
        let mut downstream = Downstream::new();
        downstream.feed(snapshot(&mut mirror, ORDER_BOOK_SNAPSHOT_CRV));

        let restatement = mirror.restate(0);
        assert_eq!(restatement[0].exch_ts, 1_785_358_811_946_459 * 1_000);
        downstream.feed(restatement);
        assert_price(downstream.bot.best_bid(), 0.20351);

        downstream.feed(delta(&mut mirror, ORDER_BOOK_UPDATE_CRV_1));
        assert_price(downstream.bot.best_bid(), 0.20350);
    }

    /// **The repair, and why it is a diff.** After a chain break the fresh snapshot is
    /// reconciled against what the bot holds: levels the break lost are deleted, levels that
    /// appeared are inserted, and the thousands that did not move say nothing — an ETH
    /// snapshot is ~2 900 levels and every event is a 512-byte IPC message (§4.7).
    ///
    /// Republishing the snapshot wholesale is not an option in either direction: `LiveBot`
    /// drops `DEPTH_SNAPSHOT_EVENT`, and a wholesale *restatement* would leave every level
    /// the break lost resting in the bot's book for ever, because nothing would remove it.
    #[test]
    fn a_repair_snapshot_is_diffed_against_the_mirror_deletions_first() {
        let mut mirror = DepthMirror::new(TICK, LOT);
        let mut downstream = Downstream::new();
        downstream.feed(snapshot(&mut mirror, ORDER_BOOK_SNAPSHOT_CRV));

        // The venue's book has moved on while the chain was broken: one mirrored bid is
        // gone, one is resized, and a new one appeared.
        let repaired = mirror
            .on_snapshot(
                &[level(0.20350, 2495.4), level(0.20345, 100.0)],
                &[level(0.20382, 7816.7)],
                1_785_358_812_000_000,
                NOW_NS,
            )
            .unwrap();

        let shape = shape(&repaired);
        let first_insert = shape.iter().position(|(_, _, qty)| *qty > 0.0).unwrap();
        let last_deletion = shape.iter().rposition(|(_, _, qty)| *qty == 0.0).unwrap();
        assert!(
            last_deletion < first_insert,
            "deletions must precede inserts: {shape:?}"
        );
        assert!(shape.contains(&('B', 0.20351, 0.0)), "{shape:?}");
        assert!(shape.contains(&('B', 0.20345, 100.0)), "{shape:?}");
        // 0.20350 was already mirrored at that size, so the repair says nothing about it.
        assert!(!shape.iter().any(|(_, px, _)| *px == 0.20350), "{shape:?}");

        downstream.feed(repaired);
        assert_price(downstream.bot.best_bid(), 0.20350);
        assert_price(downstream.bot.best_ask(), 0.20382);
        assert_eq!(mirror.depth(), (2, 1), "the mirror is the fresh snapshot");
    }

    /// **`AGENTS.md` §4.7 / design note §4.6.** `FusedHashMapMarketDepth` generates its own
    /// deletion events when an update crosses the book, and those carry no `LOCAL_EVENT`
    /// bit, so `LiveBot` drops them and the two books diverge in silence. Every depth event
    /// this backend publishes goes through that fusion in `connector/src/main.rs`, so the
    /// property that matters is: **fusion never has to generate one.**
    ///
    /// Replayed over a real consecutive run — the snapshot and the six diffs after it, in
    /// wire order — plus a restatement, which is published on a bot's registration and must
    /// be exactly as safe as the frames are.
    #[test]
    fn fusion_never_generates_a_bitless_event_from_a_real_run() {
        let mut mirror = DepthMirror::new(TICK, LOT);
        let mut fused = FusedHashMapMarketDepth::new(TICK, LOT);

        let feed = |events: Vec<Event>, fused: &mut FusedHashMapMarketDepth| {
            for event in events {
                let out = if event.is(LOCAL_BID_DEPTH_EVENT) {
                    fused.update_bid_depth(event.clone())
                } else {
                    fused.update_ask_depth(event.clone())
                };
                assert_eq!(
                    out,
                    vec![event.clone()],
                    "fusion changed the event stream: {out:?}"
                );
                assert_all_kind_one_local(&out);
            }
        };

        feed(snapshot(&mut mirror, ORDER_BOOK_SNAPSHOT_CRV), &mut fused);
        for text in [
            ORDER_BOOK_UPDATE_CRV_1,
            ORDER_BOOK_UPDATE_CRV_2,
            ORDER_BOOK_UPDATE_CRV_3,
            ORDER_BOOK_UPDATE_CRV_4,
            ORDER_BOOK_UPDATE_CRV_5,
            ORDER_BOOK_UPDATE_CRV_6,
        ] {
            feed(delta(&mut mirror, text), &mut fused);
        }

        // **The frame that makes fusion generate events of its own**: a taker sweeps the
        // touch, so one frame deletes the best ask and rests a bid on the same tick. The
        // resulting book is uncrossed — so the frame is legal and must be published — and
        // the *order* of the two events is the whole of §4.6. Emitted insert-first, fusion
        // answers the crossing bid with an ask deletion of its own, built without the
        // `LOCAL_EVENT` bit, which `LiveBot` drops: the bot then holds an ask the market no
        // longer has, for ever, with nothing in any log.
        //
        // Not in the six real frames above — the recorded run never crosses — which is why
        // it is constructed here rather than left to a fixture that happens not to contain
        // it. Verified by mutation: swapping the two loops in `emit` fails this assertion.
        let ask = mirror.best_ask().unwrap();
        feed(
            mirror
                .on_delta(
                    &[level(ask, 500.0)],
                    &[level(ask, 0.0)],
                    1_785_358_813_000_000,
                    NOW_NS,
                )
                .unwrap(),
            &mut fused,
        );
        assert_price(mirror.best_bid().unwrap(), ask);

        feed(mirror.restate(0), &mut fused);

        assert_price(fused.best_bid(), mirror.best_bid().unwrap());
        assert_price(fused.best_ask(), mirror.best_ask().unwrap());
    }

    /// A delta that would leave the published book crossed is refused whole and counted, and
    /// the caller is told so it can repair the market — carrying on would leave the mirror
    /// describing a book the venue does not have.
    #[test]
    fn a_frame_that_would_publish_a_crossed_book_is_refused_whole() {
        let mut mirror = DepthMirror::new(TICK, LOT);
        mirror
            .on_snapshot(&[level(100.0, 1.0)], &[level(101.0, 1.0)], 1_000, 0)
            .unwrap();

        // A bid at the ask: applying it would make fusion delete the ask on its own.
        let refused = mirror.on_delta(&[level(101.0, 5.0)], &[], 1_001, 0);
        assert_eq!(refused, Err(Refusal::Crossed));
        assert_eq!(mirror.counts().crossed, 1);
        assert_eq!(mirror.best_bid(), Some(100.0), "the mirror is untouched");

        // Refused *before* the monotonic gate latches, so the next good frame still lands.
        let events = mirror
            .on_delta(&[level(100.0, 2.0)], &[], 1_001, 0)
            .unwrap();
        assert_eq!(shape(&events), [('B', 100.0, 2.0)]);

        // A crossed snapshot is refused the same way: it cannot be resolved in favour of
        // either side without inventing a book.
        assert_eq!(
            mirror.on_snapshot(&[level(105.0, 1.0)], &[level(103.0, 1.0)], 1_002, 0),
            Err(Refusal::Crossed)
        );
    }

    /// Out-of-order frames must not rewind the book: the fused depth downstream drops any
    /// event older than the level it would update, so an applied-but-rejected event would
    /// silently desynchronise this mirror from the book the bot actually holds.
    #[test]
    fn a_frame_older_than_the_last_applied_one_is_refused() {
        let mut mirror = DepthMirror::new(TICK, LOT);
        mirror
            .on_snapshot(&[level(100.0, 1.0)], &[level(101.0, 1.0)], 2_000, 0)
            .unwrap();

        assert_eq!(
            mirror.on_delta(&[level(90.0, 9.0)], &[], 1_999, 0),
            Err(Refusal::Stale)
        );
        assert_eq!(mirror.counts().stale, 1);
        assert_eq!(mirror.best_bid(), Some(100.0));
    }

    /// **The 1000× trap** (§4.4). One frame from the far future latches the gate and rejects
    /// every later frame for the life of the process — measured on Hyperliquid: 0 events out
    /// of 1 000 well-formed frames, with a counter as the only trace. Feeding this venue's
    /// microseconds into a millisecond conversion is exactly that mistake.
    #[test]
    fn one_frame_from_the_future_cannot_latch_the_feed_shut() {
        let now_us = 1_785_358_811_946_459i64;
        let now_ns = now_us * 1_000;
        let mut mirror = DepthMirror::new(TICK, LOT);
        mirror
            .on_snapshot(&[level(100.0, 1.0)], &[level(101.0, 1.0)], now_us, now_ns)
            .unwrap();

        // The same field read as if it were milliseconds: 1000× out, and no overflow.
        assert_eq!(
            mirror.on_delta(&[level(100.0, 2.0)], &[], now_us * 1_000, now_ns),
            Err(Refusal::Implausible)
        );
        assert_eq!(mirror.counts().implausible, 1);

        // The frames that follow are unaffected.
        let events = mirror
            .on_delta(&[level(100.0, 3.0)], &[], now_us + 1, now_ns + 1_000)
            .unwrap();
        assert_eq!(shape(&events), [('B', 100.0, 3.0)]);
        assert_eq!(mirror.counts().stale, 0);
    }

    /// The same multiplication, one step further out. `overflow-checks` is on for `dev` and
    /// `test` and **off** for `release`, so an unchecked `exch_ts_us * 1_000` is a panic in a
    /// test — and a panic in the connector is `exit(1)` under its own hook, market data down
    /// for every bot — and a silently wrapped, arbitrary timestamp in production.
    #[test]
    fn an_exchange_time_that_does_not_fit_in_nanoseconds_is_refused_not_wrapped() {
        let mut mirror = DepthMirror::new(TICK, LOT);
        assert_eq!(
            mirror.on_snapshot(&[level(100.0, 1.0)], &[], i64::MAX / 999, 0),
            Err(Refusal::Implausible)
        );
        assert_eq!(mirror.counts().implausible, 1);

        // And the mirror is still usable.
        let events = mirror
            .on_snapshot(&[level(100.0, 1.0)], &[], 1_000, 0)
            .unwrap();
        assert_eq!(shape(&events), [('B', 100.0, 1.0)]);
    }

    /// A level whose size rounds to zero lots is *deleted* by the fused depth in `main.rs`
    /// (`Entry::Occupied` with `qty_lot == 0` removes it), so a mirror that recorded it would
    /// believe a level rests where the bot has none — and would never re-emit it, because
    /// the diff only speaks when a size changes.
    ///
    /// The mirror is built from the catalog's decimals, so on a healthy feed this is
    /// unreachable and the counter reads zero for ever; it is a check of the venue against
    /// its own grid, **not** of the lot the bot registered — that one never reaches this
    /// backend at all ([`MirrorCounts`]). The state is constructed here rather than fed,
    /// because a real frame cannot produce it. An explicit `"0.0"` is a deletion and must
    /// **not** be counted, or the signal is noise.
    #[test]
    fn a_level_that_rounds_below_one_lot_is_mirrored_as_absent_and_counted() {
        let mut mirror = DepthMirror::new(TICK, LOT);
        let mut downstream = Downstream::new();
        downstream.feed(
            mirror
                .on_snapshot(&[level(100.0, 1.0)], &[level(101.0, 1.0)], 1_000, 0)
                .unwrap(),
        );

        let events = mirror
            .on_delta(&[level(100.0, LOT / 100.0)], &[], 1_001, 0)
            .unwrap();
        assert_eq!(
            shape(&events),
            [('B', 100.0, 0.0)],
            "a deletion, not a dust size"
        );
        downstream.feed(events);

        assert_eq!(mirror.best_bid(), None, "the mirror agrees with the bot");
        assert!(downstream.bot.best_bid().is_nan());
        assert_eq!(mirror.counts().sub_lot, 1);

        // A genuine deletion is not a symptom and is not counted as one.
        mirror
            .on_delta(&[], &[level(101.0, 0.0)], 1_002, 0)
            .unwrap();
        assert_eq!(mirror.counts().sub_lot, 1);
    }

    /// Two venue prices that round to one tick become one level in the fused depth
    /// downstream whatever this does, so their sizes are summed rather than one silently
    /// replacing the other — and the collision is counted.
    ///
    /// **What it is evidence of.** Production builds this mirror from the catalog's
    /// `supported_price_decimals`, so a venue quoting on its own advertised grid can never
    /// reach this arm: the counter is a check that the catalog describes the feed, not a
    /// check on the tick the bot registered, which this backend never sees ([`MirrorCounts`]).
    /// The coarse tick below is therefore a state production does not construct — it is how
    /// the summing rule is exercised at all.
    #[test]
    fn two_prices_on_one_tick_are_summed_and_counted() {
        // A grid ten times coarser than the prices arriving — what a catalog that
        // under-reports `supported_price_decimals` would produce.
        let mut mirror = DepthMirror::new(1e-4, LOT);
        let events = mirror
            .on_snapshot(&[level(0.20351, 1.0), level(0.20353, 2.0)], &[], 1_000, 0)
            .unwrap();

        assert_eq!(shape(&events), [('B', 0.20351, 3.0)]);
        assert_eq!(mirror.counts().collapsed, 1);
    }
}
