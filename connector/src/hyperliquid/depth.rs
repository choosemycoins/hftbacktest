//! Snapshot → incremental-delta synthesis for the depth feed.
//!
//! Hyperliquid publishes no incremental depth channel: every `l2Book` message is a
//! complete top-N snapshot with no sequence number, and `bbo` is a one-level touch feed.
//! `LiveBot` applies only kind-1 depth events and silently drops kinds 4 and 5
//! (`AGENTS.md` §4.1/§4.1a), so this backend keeps a mirror of the book it has published
//! and emits the difference.
//!
//! Four rules, all of them load-bearing:
//!
//! * **`bbo` is authoritative about the touch.** Every mirrored bid above the new best bid
//!   and every mirrored ask below the new best ask is gone, and is deleted.
//! * **Deletions precede inserts**, so the published book is uncrossed after every
//!   individual event rather than only at the end of a frame. That is also what stops the
//!   fused depth in `main.rs` from generating deletion events of its own, which would lack
//!   the `LOCAL_EVENT` bit and be dropped by the bot (`AGENTS.md` §4.7). A frame that is
//!   itself crossed would break that guarantee, so it is refused rather than published.
//! * **A snapshot is diffed against the mirror as the interleaved `bbo` frames left it**,
//!   never against the previous snapshot. Diffing against the previous snapshot would call
//!   a level that a `bbo` moved and the snapshot moved back "unchanged", stranding the bot
//!   on the `bbo`'s size.
//! * **The mirror only ever records what it published.** A level whose size rounds to zero
//!   lots, or that shares a tick with another, is reconciled here rather than downstream:
//!   the fused depth in `main.rs` would drop or overwrite it, and a mirror that believed
//!   otherwise would stop describing the bot's book.
//!
//! Because the diff suppresses unchanged levels, **the mirror is the only record of what a
//! bot holds**, and a bot that registers against an already-running connector has none of
//! it. [`DepthMirror::restate`] exists for exactly that moment; see its doc comment.
//!
//! **What the published book is, precisely:** the last observed `l2Book` window, minus
//! anything a later `bbo` moved the touch past, plus the `bbo` touch levels seen since. The
//! last part is worth saying out loud: a touch level was accurate only at the instant it
//! *was* the touch. Once the touch moves on, its size is unverified resting size until the
//! next snapshot reconciles it, so any depth-derived quantity computed between snapshots
//! includes it. The residue is bounded by the snapshot cadence — roughly 4 levels at
//! `l2_book = "fast"` (0.54 s against a ~0.14 s `bbo`) and roughly 38 at `"slow"` (5.4 s),
//! which is one more reason `"fast"` is the default.
//!
//! The rules are the ones the offline converter applies to the same two feeds
//! (`py-hftbacktest/hftbacktest/data/utils/hyperliquid.py`, `book_mode='bbo+fast'`), which
//! was verified by replaying a converted day: 1 764 199 depth rows, zero crossings.

use std::collections::{BTreeMap, btree_map::Entry};

use hftbacktest::prelude::{Event, LOCAL_ASK_DEPTH_EVENT, LOCAL_BID_DEPTH_EVENT};
use tracing::warn;

use crate::hyperliquid::msg::Level;

/// How far the venue's clock may lead the local one before a frame is unusable.
///
/// The venue's `time` is latched as the mirror's monotonic gate, so a single frame from the
/// far future would reject every subsequent frame for the life of the process. Generous
/// enough that ordinary clock skew — the venue stamps in milliseconds and the two clocks
/// are NTP-disciplined — never trips it, and tight enough that a corrupt field does.
const MAX_CLOCK_LEAD_NS: i64 = 60_000_000_000;

/// What a coin's mirror had to refuse, and why. Every field is a silent failure otherwise.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MirrorCounts {
    /// Frames older than the last one applied to this coin.
    pub stale: u64,
    /// Frames whose exchange time the local clock says cannot be real.
    pub implausible: u64,
    /// `l2Book` frames that contradicted themselves — an ask at or below the best bid.
    pub crossed: u64,
    /// Venue levels that shared a tick with another and were summed into it. Non-zero means
    /// the tick derived from `szDecimals` is coarser than the prices the venue is sending.
    pub collapsed: u64,
    /// Levels whose size rounded to zero lots and were therefore mirrored as absent.
    pub sub_lot: u64,
}

/// A mirrored price level. The price is kept as the venue sent it so that every published
/// event carries the venue's own value rather than one reconstructed from a tick index.
#[derive(Clone, Copy, Debug)]
struct MirrorLevel {
    px: f64,
    qty: f64,
}

/// The connector's model of the book the bot currently holds, per coin.
///
/// It is deliberately **not** reset on reconnect: it is what the published events have
/// built, so a fresh snapshot after a reconnect must be diffed against it. Clearing it
/// would leave every level the bot holds above the new touch in place — a permanently
/// crossed book with nothing in the log.
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

    /// Frames dropped for being older than the last applied one.
    pub fn stale_frames(&self) -> u64 {
        self.counts.stale
    }

    pub fn best_bid(&self) -> Option<f64> {
        self.bids.values().next_back().map(|level| level.px)
    }

    pub fn best_ask(&self) -> Option<f64> {
        self.asks.values().next().map(|level| level.px)
    }

    fn tick(&self, px: f64) -> i64 {
        (px / self.tick_size).round() as i64
    }

    fn lots(&self, qty: f64) -> i64 {
        (qty / self.lot_size).round() as i64
    }

    /// Every mirrored level as an insert, in an order that is never crossed.
    ///
    /// This is what a bot registering against an already-running connector needs, and the
    /// reason is entirely in `connector/src/main.rs`: a newly registered instrument gets a
    /// **fresh** `FusedHashMapMarketDepth` (the `Entry::Vacant` arm), and the one path that
    /// replays an existing book (`Entry::Occupied` → `depth_.snapshot()`) emits
    /// `DEPTH_SNAPSHOT_EVENT`, which `LiveBot` drops without a word (`AGENTS.md` §4.1).
    /// Meanwhile `SnapshotComplete` is published either way, so the bot is told it is ready.
    ///
    /// Without this, that bot's book is filled only by levels that happen to *change* size
    /// — the diff suppresses the rest — so it can sit indefinitely with one side missing
    /// and a `NaN` mid, and nothing in any log says so. Measured: replaying the same
    /// snapshot into a primed mirror produces zero events, and the bot's book stays empty.
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

    /// Applies a complete `l2Book` snapshot; returns the events that carry it to the bot.
    pub fn on_snapshot(
        &mut self,
        bids: &[Level],
        asks: &[Level],
        exch_ts_ms: i64,
        local_ts: i64,
    ) -> Vec<Event> {
        // A snapshot that contradicts itself would put a crossed book on the wire: the
        // deletions-first ordering in `emit` keeps every intermediate state uncrossed only
        // while the *new* state is uncrossed. An ask at or below the best bid instead makes
        // the fused depth in `main.rs` generate deletion events of its own, which carry no
        // `LOCAL_EVENT` bit and are dropped by `LiveBot` — the two books then diverge with
        // nothing in any log (`AGENTS.md` §4.7). Never observed; refused, not trusted.
        let best_bid = bids.iter().map(|l| self.tick(l.px)).max();
        let best_ask = asks.iter().map(|l| self.tick(l.px)).min();
        if let (Some(bid), Some(ask)) = (best_bid, best_ask)
            && ask <= bid
        {
            self.counts.crossed += 1;
            warn!(
                best_bid_tick = bid,
                best_ask_tick = ask,
                exch_ts_ms,
                "Refusing a self-crossed Hyperliquid l2Book frame; publishing it would \
                 desynchronise the bot's book from this mirror."
            );
            return Vec::new();
        }

        let Some(exch_ts) = self.accept(exch_ts_ms, local_ts) else {
            return Vec::new();
        };
        let new_bids = self.side_of(bids);
        let new_asks = self.side_of(asks);
        self.emit(new_bids, new_asks, exch_ts, local_ts)
    }

    /// Applies a `bbo` frame. `sides` is `[bid, ask]`; a `None` side is "no news about
    /// that side" and leaves it untouched.
    pub fn on_bbo(
        &mut self,
        sides: &[Option<Level>; 2],
        exch_ts_ms: i64,
        local_ts: i64,
    ) -> Vec<Event> {
        if sides[0].is_none() && sides[1].is_none() {
            return Vec::new();
        }
        let Some(exch_ts) = self.accept(exch_ts_ms, local_ts) else {
            return Vec::new();
        };

        let mut new_bids = self.bids.clone();
        let mut new_asks = self.asks.clone();

        // The touch states the whole top of the book: nothing rests above the best bid,
        // nothing below the best ask, and nothing on the far side at or through either.
        //
        // The sides are applied in order, so a `bbo` that is itself crossed — never once
        // observed, 655 873 uncrossed frames in a day of mainnet, but the venue makes no
        // promise — resolves in favour of the ask and leaves the published book uncrossed
        // rather than propagating the contradiction. See the test.
        if let Some(bid) = &sides[0] {
            let tick = self.tick(bid.px);
            new_bids.retain(|&t, _| t < tick);
            new_asks.retain(|&t, _| t > tick);
            if self.lots(bid.sz) > 0 {
                new_bids.insert(
                    tick,
                    MirrorLevel {
                        px: bid.px,
                        qty: bid.sz,
                    },
                );
            } else {
                self.counts.sub_lot += 1;
            }
        }
        if let Some(ask) = &sides[1] {
            let tick = self.tick(ask.px);
            new_asks.retain(|&t, _| t > tick);
            new_bids.retain(|&t, _| t < tick);
            if self.lots(ask.sz) > 0 {
                new_asks.insert(
                    tick,
                    MirrorLevel {
                        px: ask.px,
                        qty: ask.sz,
                    },
                );
            } else {
                self.counts.sub_lot += 1;
            }
        }

        self.emit(new_bids, new_asks, exch_ts, local_ts)
    }

    /// Returns the exchange timestamp in nanoseconds, or `None` for a frame that cannot be
    /// applied.
    ///
    /// Monotonicity is not cosmetic. The fused depth in `main.rs` rejects any event whose
    /// timestamp precedes the state it would update, so an out-of-order frame applied here
    /// would be dropped there, and this mirror would stop describing the bot's book.
    ///
    /// The gate is deliberately shared by `bbo` and `l2Book` rather than kept per channel.
    /// Per-channel gates would let a snapshot stamped behind the last `bbo` through, and
    /// fusion would then reject that snapshot's write to the touch level — leaving this
    /// mirror believing it published something the bot never received. Dropping the frame
    /// costs at most one snapshot interval and keeps the two in step; the price is that a
    /// cross-channel reordering discards a whole snapshot, which is why the refusal rate is
    /// alarmed on rather than merely counted (see `public_stream::degraded`).
    ///
    /// Because the accepted timestamp is *latched*, it has to be sane before it is latched:
    /// one frame from the far future would otherwise reject every later frame for the life
    /// of the process, with only a counter to show for it. `local_ts` is the local receive
    /// time in nanoseconds; `0` means the local clock could not be read, and then there is
    /// nothing to compare against.
    fn accept(&mut self, exch_ts_ms: i64, local_ts: i64) -> Option<i64> {
        let Some(exch_ts) = exch_ts_ms.checked_mul(1_000_000) else {
            self.implausible(exch_ts_ms, "the exchange time does not fit in nanoseconds");
            return None;
        };
        if local_ts > 0 && exch_ts.saturating_sub(local_ts) > MAX_CLOCK_LEAD_NS {
            self.implausible(exch_ts_ms, "the exchange time leads the local clock");
            return None;
        }
        if exch_ts < self.last_exch_ts {
            self.counts.stale += 1;
            return None;
        }
        self.last_exch_ts = exch_ts;
        Some(exch_ts)
    }

    fn implausible(&mut self, exch_ts_ms: i64, reason: &str) {
        self.counts.implausible += 1;
        // Once per mirror: a systematically wrong clock would otherwise fill the log at the
        // frame rate. The rate itself is what `public_stream::degraded` alarms on.
        if self.counts.implausible == 1 {
            warn!(
                exch_ts_ms,
                reason,
                "Refusing a Hyperliquid frame with an unusable exchange time; applying it \
                 would latch this coin's feed shut."
            );
        }
    }

    /// The venue's levels as the mirror will record them: one entry per tick, nothing below
    /// one lot.
    ///
    /// Both reconciliations exist because the mirror must record exactly what the fused
    /// depth downstream will accept. Two prices that round to one tick become one level
    /// there whatever this does, so their sizes are summed rather than one silently
    /// replacing the other; a size that rounds to zero lots is *deleted* there, so it is
    /// recorded as absent here. Neither is reachable while `szDecimals` is right — the
    /// venue quantises both fields to the grid it implies — so both are counted, because a
    /// non-zero count is the only evidence that it is not.
    fn side_of(&mut self, levels: &[Level]) -> BTreeMap<i64, MirrorLevel> {
        let mut out: BTreeMap<i64, MirrorLevel> = BTreeMap::new();
        let mut collapsed = 0;
        for level in levels {
            match out.entry(self.tick(level.px)) {
                Entry::Occupied(mut entry) => {
                    entry.get_mut().qty += level.sz;
                    collapsed += 1;
                }
                Entry::Vacant(entry) => {
                    entry.insert(MirrorLevel {
                        px: level.px,
                        qty: level.sz,
                    });
                }
            }
        }
        let before = out.len();
        out.retain(|_, level| (level.qty / self.lot_size).round() as i64 > 0);
        self.counts.collapsed += collapsed;
        self.counts.sub_lot += (before - out.len()) as u64;
        out
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
            // Compared in lots, not floats: the venue restates an unchanged book
            // constantly, and a size that rounds to the same lot is not news. What makes
            // the suppression safe is that the mirror is only ever allowed to hold what was
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

/// Builds a kind-1 local depth event — the only kind `LiveBot` applies.
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

    use crate::hyperliquid::{
        depth::DepthMirror,
        fixtures::{BBO_BTC_2, L2BOOK_FAST_BTC_1, L2BOOK_FAST_BTC_3},
        msg::{Frame, Level, parse_frame},
    };

    /// BTC on Hyperliquid: `szDecimals = 5`, so the price grid is `10^-(6-5)` and the lot
    /// is `10^-5`.
    const TICK: f64 = 0.1;
    const LOT: f64 = 1e-5;

    fn level(px: f64, sz: f64) -> Level {
        Level { px, sz, n: 1 }
    }

    fn snapshot(mirror: &mut DepthMirror, text: &str) -> Vec<Event> {
        let Frame::L2Book(book) = parse_frame(text).unwrap() else {
            panic!("expected an l2Book frame");
        };
        mirror.on_snapshot(&book.levels[0], &book.levels[1], book.time, 0)
    }

    fn bbo(mirror: &mut DepthMirror, text: &str) -> Vec<Event> {
        let Frame::Bbo(quote) = parse_frame(text).unwrap() else {
            panic!("expected a bbo frame");
        };
        mirror.on_bbo(&quote.bbo, quote.time, 0)
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

    /// The one assertion that permanently prevents the failure mode in the design note §1:
    /// `LiveBot::process_event` only ever applies `LOCAL_BID_DEPTH_EVENT` /
    /// `LOCAL_ASK_DEPTH_EVENT` (kind 1), and drops `DEPTH_SNAPSHOT_EVENT` (kind 4) and
    /// `DEPTH_BBO_EVENT` (kind 5) without a word. Emitting a snapshot as what it is would
    /// give a bot whose book stays permanently empty and a log with nothing in it.
    fn assert_all_kind_one_local(events: &[Event]) {
        for event in events {
            assert!(
                event.is(LOCAL_BID_DEPTH_EVENT) || event.is(LOCAL_ASK_DEPTH_EVENT),
                "event flags {:#x} are not a kind-1 local depth event; LiveBot would drop it",
                event.ev
            );
        }
    }

    /// The bot's own book, driven exactly as `LiveBot::process_event` drives it: through
    /// the same `FusedHashMapMarketDepth` `connector/src/main.rs` puts in the way, applying
    /// only the kind-1 events and dropping everything else.
    struct Downstream {
        fused: FusedHashMapMarketDepth,
        bot: HashMapMarketDepth,
    }

    impl Downstream {
        fn new(tick_size: f64, lot_size: f64) -> Self {
            Self {
                fused: FusedHashMapMarketDepth::new(tick_size, lot_size),
                bot: HashMapMarketDepth::new(tick_size, lot_size),
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

    #[test]
    fn the_first_snapshot_becomes_one_insert_per_level() {
        let mut mirror = DepthMirror::new(TICK, LOT);
        let events = snapshot(&mut mirror, L2BOOK_FAST_BTC_1);

        assert_all_kind_one_local(&events);
        assert_eq!(events.len(), 10, "5 levels a side");
        assert_eq!(
            shape(&events)[..3],
            [
                ('B', 63429.0, 0.00046),
                ('B', 63441.0, 0.04944),
                ('B', 63454.0, 0.02659),
            ]
        );
        // Exchange time is carried through in nanoseconds.
        assert_eq!(events[0].exch_ts, 1785251521889 * 1_000_000);
    }

    #[test]
    fn an_unchanged_snapshot_emits_nothing() {
        let mut mirror = DepthMirror::new(TICK, LOT);
        snapshot(&mut mirror, L2BOOK_FAST_BTC_1);

        let Frame::L2Book(book) = parse_frame(L2BOOK_FAST_BTC_1).unwrap() else {
            unreachable!()
        };
        // Same content, later time — the venue re-sends an unchanged book constantly.
        let repeat = mirror.on_snapshot(&book.levels[0], &book.levels[1], book.time + 500, 0);
        assert!(repeat.is_empty(), "{:?}", shape(&repeat));
    }

    /// **The reason unchanged levels may be suppressed at all.**
    ///
    /// A bot registering against a connector that is already running gets a *fresh*
    /// `FusedHashMapMarketDepth` from `main.rs`, and `main.rs`'s one replay path emits
    /// `DEPTH_SNAPSHOT_EVENT`, which `LiveBot` drops (`AGENTS.md` §4.1) — while
    /// `SnapshotComplete` is published regardless, so the bot is told it is ready. Without
    /// a restatement the bot's book is filled only by levels that happen to change size,
    /// which can leave a whole side empty and the mid `NaN` for an unbounded time.
    ///
    /// Measured before the fix: replaying the same snapshot into a primed mirror produced
    /// **0 events**, and the bot's best bid and best ask were both `NaN` while the mirror
    /// held 5 levels a side.
    #[test]
    fn a_bot_registering_after_the_mirror_is_primed_is_given_the_whole_book() {
        let mut mirror = DepthMirror::new(TICK, LOT);
        // The connector has been running: `coins = ["BTC", "ETH"]` primed this at startup.
        snapshot(&mut mirror, L2BOOK_FAST_BTC_1);

        // A bot registers now. `main.rs` inserts a fresh fusion with nothing in it.
        let mut downstream = Downstream::new(TICK, LOT);
        let restatement = mirror.restate(7);
        assert_all_kind_one_local(&restatement);
        assert_eq!(restatement.len(), 10, "5 levels a side");
        assert_eq!(restatement[0].local_ts, 7);
        downstream.feed(restatement);

        assert_eq!(downstream.bot.best_bid(), 63460.0);
        assert_eq!(downstream.bot.best_ask(), 63488.0);

        // And the mirror is unchanged by having restated, so the next real frame still
        // diffs against what the bot holds.
        let events = snapshot(&mut mirror, L2BOOK_FAST_BTC_3);
        assert_eq!(shape(&events).len(), 2, "{:?}", shape(&events));
        downstream.feed(events);
        assert_eq!(downstream.bot.best_bid(), 63457.0);
    }

    /// A restatement of an empty mirror is nothing, not a batch of zero-quantity events:
    /// the connector may be asked for one before the first frame has arrived.
    #[test]
    fn restating_an_empty_mirror_says_nothing() {
        let mirror = DepthMirror::new(TICK, LOT);
        assert!(mirror.restate(1).is_empty());
    }

    /// The restatement carries the exchange time of the frame the mirror was built from, so
    /// a bot that already holds the book sees no rewind — the fused depth in `main.rs`
    /// rejects any event older than the level it would update — and the next real frame
    /// still supersedes it.
    #[test]
    fn a_restatement_does_not_rewind_a_book_that_is_already_there() {
        let mut mirror = DepthMirror::new(TICK, LOT);
        let mut downstream = Downstream::new(TICK, LOT);
        downstream.feed(snapshot(&mut mirror, L2BOOK_FAST_BTC_1));

        let restatement = mirror.restate(0);
        assert_eq!(restatement[0].exch_ts, 1785251521889 * 1_000_000);
        downstream.feed(restatement);
        assert_eq!(downstream.bot.best_bid(), 63460.0);
        assert_eq!(downstream.bot.best_ask(), 63488.0);

        downstream.feed(snapshot(&mut mirror, L2BOOK_FAST_BTC_3));
        assert_eq!(downstream.bot.best_bid(), 63457.0);
    }

    #[test]
    fn a_size_change_inside_the_window_emits_only_that_level() {
        let mut mirror = DepthMirror::new(TICK, LOT);
        let bids = [level(100.0, 1.0), level(99.0, 2.0)];
        let asks = [level(101.0, 3.0)];
        mirror.on_snapshot(&bids, &asks, 1000, 0);

        let bids = [level(100.0, 1.5), level(99.0, 2.0)];
        let events = mirror.on_snapshot(&bids, &asks, 1001, 0);
        assert_eq!(shape(&events), [('B', 100.0, 1.5)]);
    }

    /// A level that disappears from *inside* the observed window was cancelled, and the
    /// bot must be told with a zero quantity — the only way to remove a level.
    #[test]
    fn a_level_cancelled_inside_the_window_is_emitted_as_zero_quantity() {
        let mut mirror = DepthMirror::new(TICK, LOT);
        let bids = [level(100.0, 1.0), level(99.0, 2.0), level(98.0, 3.0)];
        let asks = [level(101.0, 1.0)];
        mirror.on_snapshot(&bids, &asks, 1000, 0);

        // 99 is gone; the window still reaches 98, so its absence is a cancellation.
        let bids = [level(100.0, 1.0), level(98.0, 3.0)];
        let events = mirror.on_snapshot(&bids, &asks, 1001, 0);
        assert_eq!(shape(&events), [('B', 99.0, 0.0)]);
    }

    /// **Truncation policy.** A level leaving the deep end of a top-N window is
    /// indistinguishable from a cancelled one. This backend deletes it, so the bot's book
    /// is exactly the last observed window and nothing else.
    ///
    /// The alternative — keeping it — was rejected: the residue never expires, so the book
    /// accumulates levels of unknown age forever, and any depth-derived number computed
    /// from it is silently part stale. Deleting also matches the offline converter's
    /// default (`delete_out_of_book=True`), which is what a backtest of this feed is built
    /// from, so live and replay agree. Divergence from design note §5.2 recorded there.
    #[test]
    fn a_level_that_leaves_the_deep_end_of_the_window_is_deleted() {
        let mut mirror = DepthMirror::new(TICK, LOT);
        let bids = [level(100.0, 1.0), level(99.0, 2.0)];
        let asks = [level(101.0, 1.0), level(102.0, 1.0)];
        mirror.on_snapshot(&bids, &asks, 1000, 0);

        // The window shifted up: 99 fell off the deep end, 102 fell off the ask side.
        let bids = [level(103.0, 5.0), level(100.0, 1.0)];
        let asks = [level(104.0, 1.0), level(105.0, 1.0)];
        let events = mirror.on_snapshot(&bids, &asks, 1001, 0);

        assert_all_kind_one_local(&events);
        let shape = shape(&events);
        assert!(shape.contains(&('B', 99.0, 0.0)), "{shape:?}");
        assert!(shape.contains(&('A', 101.0, 0.0)), "{shape:?}");
        assert!(shape.contains(&('A', 102.0, 0.0)), "{shape:?}");
        assert!(shape.contains(&('B', 103.0, 5.0)), "{shape:?}");
    }

    /// Real frames, 201 ms apart: the snapshot's best bid (63460) is gone by the time the
    /// `bbo` arrives. `bbo` is authoritative about the touch, so everything above the new
    /// best bid must be deleted — that is both the correct book and what keeps it
    /// uncrossed when a `bbo` arrives through a mirror up to half a second stale.
    #[test]
    fn a_bbo_deletes_the_levels_the_touch_moved_past() {
        let mut mirror = DepthMirror::new(TICK, LOT);
        snapshot(&mut mirror, L2BOOK_FAST_BTC_1);

        let events = bbo(&mut mirror, BBO_BTC_2);
        assert_all_kind_one_local(&events);
        // 63457 is restated at an unchanged size, so it is not re-emitted; only the level
        // the touch passed is.
        assert_eq!(shape(&events), [('B', 63460.0, 0.0)]);
    }

    /// The next real snapshot, 312 ms later. It is diffed against the mirror **as the
    /// `bbo` left it**, not against the previous snapshot: 63460 was already deleted, so
    /// the only news is the level that entered at the deep end.
    #[test]
    fn a_snapshot_reconciles_against_the_bbo_modified_mirror() {
        let mut mirror = DepthMirror::new(TICK, LOT);
        snapshot(&mut mirror, L2BOOK_FAST_BTC_1);
        bbo(&mut mirror, BBO_BTC_2);

        let events = snapshot(&mut mirror, L2BOOK_FAST_BTC_3);
        assert_eq!(shape(&events), [('B', 63423.0, 0.00019)]);
    }

    /// A `bbo` whose bid arrives through the mirrored asks (measured: 4.6 % of frames on
    /// mainnet, 0.4 % on the quiet testnet). The asks it passed must be deleted **before**
    /// the new bid is emitted, so the book is uncrossed after every individual event and
    /// not merely at the end of the batch.
    #[test]
    fn a_crossing_bbo_deletes_the_opposite_side_before_inserting() {
        let mut mirror = DepthMirror::new(TICK, LOT);
        let bids = [level(100.0, 1.0), level(99.0, 1.0)];
        let asks = [level(101.0, 1.0), level(102.0, 1.0), level(103.0, 1.0)];
        mirror.on_snapshot(&bids, &asks, 1000, 0);

        // The touch jumps to 102.0, through two mirrored asks.
        let events = mirror.on_bbo(&[Some(level(102.0, 4.0)), None], 1001, 0);
        assert_all_kind_one_local(&events);
        assert_eq!(
            shape(&events),
            [('A', 101.0, 0.0), ('A', 102.0, 0.0), ('B', 102.0, 4.0),],
            "deletions must precede the insert"
        );

        // Replaying the events one at a time never leaves the book crossed.
        let mut best_bid = f64::MIN;
        let mut best_ask = f64::MAX;
        for (side, px, qty) in shape(&events) {
            match (side, qty > 0.0) {
                ('B', true) => best_bid = best_bid.max(px),
                ('A', true) => best_ask = best_ask.min(px),
                ('A', false) if px == best_ask => best_ask = 103.0,
                _ => {}
            }
            assert!(best_bid < best_ask, "crossed after ({side}, {px}, {qty})");
        }
    }

    /// A `null` side is "no news", not "empty". Never observed in a day of mainnet frames
    /// or in the testnet capture, but the field is typed nullable and the two readings
    /// differ by a whole side of the book.
    #[test]
    fn a_null_bbo_side_leaves_that_side_untouched() {
        let mut mirror = DepthMirror::new(TICK, LOT);
        let bids = [level(100.0, 1.0)];
        let asks = [level(101.0, 1.0)];
        mirror.on_snapshot(&bids, &asks, 1000, 0);

        let events = mirror.on_bbo(&[None, None], 1001, 0);
        assert!(events.is_empty(), "{:?}", shape(&events));

        let events = mirror.on_bbo(&[Some(level(100.5, 2.0)), None], 1002, 0);
        assert_eq!(shape(&events), [('B', 100.5, 2.0)]);
    }

    /// A `bbo` frame that contradicts itself must not put a crossed book on the wire. The
    /// venue never sent one — 655 873 frames in a day of mainnet all had bid < ask — but
    /// nothing in the protocol forbids it, and the published book is the bot's whole view
    /// of the market.
    #[test]
    fn a_self_crossed_bbo_still_leaves_the_published_book_uncrossed() {
        let mut mirror = DepthMirror::new(TICK, LOT);
        mirror.on_snapshot(&[level(100.0, 1.0)], &[level(101.0, 1.0)], 1000, 0);

        let events = mirror.on_bbo(&[Some(level(102.0, 1.0)), Some(level(101.5, 2.0))], 1001, 0);
        assert_all_kind_one_local(&events);

        let bid = mirror.best_bid().unwrap_or(f64::MIN);
        let ask = mirror.best_ask().unwrap_or(f64::MAX);
        assert!(bid < ask, "mirror is crossed: bid={bid} ask={ask}");
    }

    /// The same guarantee for the other channel, and it is not symmetric with `bbo`'s: a
    /// crossed *snapshot* cannot be resolved in favour of either side without inventing a
    /// book, so it is refused whole. Publishing it would make the fused depth in `main.rs`
    /// generate its own deletion events, which carry no `LOCAL_EVENT` bit and are dropped
    /// by `LiveBot` — the §4.7 trap, and the books diverge in silence.
    ///
    /// Measured before the guard: a crossed snapshot produced two `ev = 0x20000001` events
    /// out of fusion.
    #[test]
    fn a_self_crossed_snapshot_is_refused_whole() {
        let mut mirror = DepthMirror::new(TICK, LOT);
        let mut downstream = Downstream::new(TICK, LOT);
        downstream.feed(mirror.on_snapshot(&[level(100.0, 1.0)], &[level(101.0, 1.0)], 1000, 0));

        // levels[1][0] is below levels[0][0]: the frame contradicts itself.
        let events = mirror.on_snapshot(
            &[level(105.0, 1.0), level(104.0, 1.0)],
            &[level(103.0, 1.0), level(106.0, 1.0)],
            1001,
            0,
        );
        assert!(events.is_empty(), "{:?}", shape(&events));
        assert_eq!(mirror.counts().crossed, 1);
        // Refused *before* the monotonic gate latches, so the next good frame still lands.
        let events = mirror.on_snapshot(&[level(100.0, 2.0)], &[level(101.0, 1.0)], 1001, 0);
        assert_eq!(shape(&events), [('B', 100.0, 2.0)]);
    }

    /// Out-of-order frames must not rewind the book. Nothing rewound in the whole testnet
    /// capture (644 frames across two channels and two coins), but the connector's own
    /// fused depth in `main.rs` drops any event older than the level it would update, so
    /// an applied-but-rejected event would silently desynchronise this mirror from the
    /// book the bot actually holds.
    #[test]
    fn a_frame_older_than_the_last_applied_one_is_ignored() {
        let mut mirror = DepthMirror::new(TICK, LOT);
        let bids = [level(100.0, 1.0)];
        let asks = [level(101.0, 1.0)];
        mirror.on_snapshot(&bids, &asks, 2000, 0);

        let stale = [level(90.0, 9.0)];
        let events = mirror.on_snapshot(&stale, &asks, 1999, 0);
        assert!(events.is_empty(), "{:?}", shape(&events));
        assert_eq!(mirror.stale_frames(), 1);

        // The mirror still holds the newer state: a fresh restatement is a no-op.
        let events = mirror.on_snapshot(&bids, &asks, 2001, 0);
        assert!(events.is_empty(), "{:?}", shape(&events));
    }

    /// The gate the test above relies on is *latched*, which makes the timestamp it latches
    /// safety-critical: one frame from the far future rejects every later frame for the
    /// life of the process.
    ///
    /// Measured before the guard: a single frame stamped `4_102_444_800_000` ms (year 2100,
    /// no overflow) was followed by **0 events out of 1000 well-formed frames**, with a
    /// counter in a 60-second `info!` line as the only trace.
    #[test]
    fn one_frame_from_the_future_cannot_latch_the_feed_shut() {
        let now_ms = 1_785_251_521_889i64;
        let now_ns = now_ms * 1_000_000;
        let mut mirror = DepthMirror::new(TICK, LOT);
        mirror.on_snapshot(&[level(100.0, 1.0)], &[level(101.0, 1.0)], now_ms, now_ns);

        // Year 2100, in milliseconds. Well within i64; nothing overflows.
        let events = mirror.on_snapshot(
            &[level(100.0, 2.0)],
            &[level(101.0, 1.0)],
            4_102_444_800_000,
            now_ns,
        );
        assert!(events.is_empty(), "{:?}", shape(&events));
        assert_eq!(mirror.counts().implausible, 1);

        // The frames that follow are unaffected.
        let events = mirror.on_snapshot(
            &[level(100.0, 3.0)],
            &[level(101.0, 1.0)],
            now_ms + 1,
            now_ns + 1_000_000,
        );
        assert_eq!(shape(&events), [('B', 100.0, 3.0)]);
        assert_eq!(mirror.stale_frames(), 0);
    }

    /// The same multiplication, one step further out. `overflow-checks` is on for `dev` and
    /// `test` and **off** for `release`, so an unchecked `exch_ts_ms * 1_000_000` is a panic
    /// in a test and a silently wrapped, arbitrary timestamp in production — which then
    /// latches the feed shut exactly as above.
    ///
    /// Measured before the guard: `cargo test` panicked with "attempt to multiply with
    /// overflow", and the release wrap of `9_999_999_999_999_999` is `1_864_712_049_422_024_128`.
    #[test]
    fn an_exchange_time_that_does_not_fit_in_nanoseconds_is_refused_not_wrapped() {
        let mut mirror = DepthMirror::new(TICK, LOT);
        let events = mirror.on_snapshot(&[level(100.0, 1.0)], &[], 9_999_999_999_999_999, 0);
        assert!(events.is_empty(), "{:?}", shape(&events));
        assert_eq!(mirror.counts().implausible, 1);

        // And the mirror is still usable.
        let events = mirror.on_snapshot(&[level(100.0, 1.0)], &[], 1000, 0);
        assert_eq!(shape(&events), [('B', 100.0, 1.0)]);
    }

    /// A level whose size rounds to zero lots is *deleted* by the fused depth in `main.rs`
    /// (`Entry::Occupied` with `qty_lot == 0` removes it), so a mirror that recorded it
    /// would believe a level rests where the bot has none — and would never re-emit it,
    /// because the diff only speaks when a size changes.
    ///
    /// Not reachable while `szDecimals` is right; the venue quantises `sz` to the lot. The
    /// guard exists because the divergence it prevents is permanent and silent, and the
    /// counter exists because a non-zero count is the only evidence `szDecimals` is wrong.
    #[test]
    fn a_level_that_rounds_below_one_lot_is_mirrored_as_absent() {
        let mut mirror = DepthMirror::new(TICK, LOT);
        let mut downstream = Downstream::new(TICK, LOT);
        downstream.feed(mirror.on_snapshot(&[level(100.0, 1.0)], &[level(101.0, 1.0)], 1000, 0));

        let events = mirror.on_snapshot(&[level(100.0, 1e-9)], &[level(101.0, 1.0)], 1001, 0);
        assert_eq!(shape(&events), [('B', 100.0, 0.0)], "a deletion, not 1e-9");
        downstream.feed(events);

        assert_eq!(mirror.best_bid(), None, "the mirror agrees with the bot");
        assert!(downstream.bot.best_bid().is_nan());
        assert_eq!(mirror.counts().sub_lot, 1);
    }

    /// Two venue prices that round to one tick become one level in the fused depth
    /// downstream whatever this does, so their sizes are summed rather than one silently
    /// replacing the other. Before the fix the later entry overwrote the earlier and its
    /// size was discarded: `[(100.00, 1.0), (100.04, 2.0)]` at tick 0.1 published a single
    /// `(100.04, 2.0)`, losing a whole level's worth of size in both directions.
    #[test]
    fn two_prices_on_one_tick_are_summed_and_counted() {
        let mut mirror = DepthMirror::new(TICK, LOT);
        let events = mirror.on_snapshot(&[level(100.00, 1.0), level(100.04, 2.0)], &[], 1000, 0);

        assert_eq!(shape(&events), [('B', 100.0, 3.0)]);
        assert_eq!(mirror.counts().collapsed, 1);
    }

    /// `AGENTS.md` §4.7: `FusedHashMapMarketDepth` generates its own deletion events when
    /// an update crosses the book, and those lack the `LOCAL_EVENT` bit, so `LiveBot`
    /// drops them and the two books diverge. Every depth event this backend publishes goes
    /// through that fusion in `connector/src/main.rs`, so the property that matters is:
    /// **fusion never has to generate one.** It cannot, because deletions are always
    /// emitted before the levels that displaced them — and because a frame that would
    /// break that ordering is refused (see `a_self_crossed_snapshot_is_refused_whole`).
    ///
    /// The test replays a real sequence — snapshot, crossing `bbo`, snapshot — through the
    /// same fusion `main.rs` uses, and asserts fusion returned exactly what it was given,
    /// bit for bit.
    #[test]
    fn fusion_never_generates_a_bitless_event_from_this_stream() {
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

        feed(snapshot(&mut mirror, L2BOOK_FAST_BTC_1), &mut fused);
        feed(bbo(&mut mirror, BBO_BTC_2), &mut fused);
        feed(snapshot(&mut mirror, L2BOOK_FAST_BTC_3), &mut fused);

        // A deliberately crossing touch, the case that makes fusion generate events.
        feed(
            mirror.on_bbo(&[Some(level(63500.0, 0.5)), None], 1785251522500, 0),
            &mut fused,
        );
        // And a restatement, which is published on a bot's registration and must be just
        // as safe as the frames are.
        feed(mirror.restate(0), &mut fused);

        assert_eq!(fused.best_bid(), 63500.0);
        assert_eq!(fused.best_ask(), 63507.0);
        assert_eq!(mirror.best_bid(), Some(63500.0));
        assert_eq!(mirror.best_ask(), Some(63507.0));
    }

    /// The `bbo` touch levels the mirror keeps between snapshots are *unverified resting
    /// size* once the touch has moved past them, and they accumulate until a snapshot
    /// reconciles. Bounded by the snapshot cadence — roughly 4 levels at `"fast"`, 38 at
    /// `"slow"` — and reconciled in one batch, which is the property this pins.
    #[test]
    fn bbo_touch_levels_accumulate_between_snapshots_and_a_snapshot_clears_them() {
        let mut mirror = DepthMirror::new(TICK, LOT);
        mirror.on_snapshot(&[level(100.0, 1.0)], &[level(200.0, 1.0)], 1000, 0);

        // Four upward touch moves, the measured residue at `l2_book = "fast"`.
        for i in 0..4 {
            mirror.on_bbo(
                &[Some(level(100.1 + i as f64 * 0.1, 1.0)), None],
                1001 + i,
                0,
            );
        }
        assert_eq!(mirror.bids.len(), 5, "one snapshot level plus four touches");

        // The next snapshot restates the window, and every phantom goes with it.
        let events = mirror.on_snapshot(&[level(100.4, 1.0)], &[level(200.0, 1.0)], 1010, 0);
        assert_eq!(mirror.bids.len(), 1);
        assert_eq!(
            shape(&events)
                .iter()
                .filter(|(_, _, qty)| *qty == 0.0)
                .count(),
            4,
            "{:?}",
            shape(&events)
        );
    }
}
