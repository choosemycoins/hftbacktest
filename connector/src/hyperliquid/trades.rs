//! The public `trades` feed, and the guard that keeps its replays out of the bot.

use std::collections::{HashMap, HashSet, VecDeque};

use hftbacktest::prelude::{Event, LOCAL_BUY_TRADE_EVENT, LOCAL_SELL_TRADE_EVENT};

use crate::hyperliquid::msg::Trade;

/// Fill ids remembered per coin.
///
/// Hyperliquid replays the last 30 fills of a coin on every (re)subscribe, so any window
/// above 30 covers a whole replay; the margin absorbs one that arrives interleaved with
/// fresh fills. Bounded because a liquid coin sees hundreds of thousands of fills a day
/// and a replay never reaches further back than those 30.
///
/// Same value, and same reasoning, as the offline converter's `TRADE_TID_HISTORY`
/// (`py-hftbacktest/hftbacktest/data/utils/hyperliquid.py`).
pub const TRADE_TID_HISTORY: usize = 64;

/// Recently published fill ids, per coin.
///
/// Owned by the stream task rather than by a connection: a guard that reset on reconnect
/// would drop exactly nothing, since a replay only ever arrives *because* of a reconnect.
#[derive(Default)]
pub struct TradeDedup {
    /// Insertion-ordered window per coin: the queue gives O(1) eviction of the oldest id,
    /// the set gives O(1) membership. Keyed by coin because one busy coin must not evict
    /// another's ids before that coin's replay arrives.
    per_coin: HashMap<String, (VecDeque<u64>, HashSet<u64>)>,
    dropped: u64,
}

impl TradeDedup {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fills dropped as replays.
    pub fn dropped(&self) -> u64 {
        self.dropped
    }

    /// Whether this fill should be published.
    ///
    /// A fill without a `tid` is passed through: two fills of the same size at the same
    /// price and time are legitimate, so there is nothing to tell them apart by.
    pub fn accept(&mut self, coin: &str, tid: Option<u64>) -> bool {
        let Some(tid) = tid else {
            return true;
        };
        let (queue, seen) = match self.per_coin.get_mut(coin) {
            Some(window) => window,
            // Only on a coin's first fill, so the key allocation is not in any hot path.
            None => self
                .per_coin
                .entry(coin.to_string())
                .or_insert_with(|| (VecDeque::with_capacity(TRADE_TID_HISTORY), HashSet::new())),
        };
        if !seen.insert(tid) {
            self.dropped += 1;
            return false;
        }
        queue.push_back(tid);
        if queue.len() > TRADE_TID_HISTORY
            && let Some(oldest) = queue.pop_front()
        {
            seen.remove(&oldest);
        }
        true
    }
}

/// Builds the local trade event for one fill, or `None` if the venue's timestamp cannot be
/// expressed in nanoseconds. `side` is the aggressor's.
///
/// The multiplication is checked because the unchecked one is two different bugs in the two
/// profiles this crate is built with: the root `Cargo.toml` sets `overflow-checks = true`
/// for `dev`/`test` and `false` for `release`, so any `time > 9_223_372_036_854` panics in a
/// test and wraps silently in production — and a panic in the connector is `exit(1)` under
/// its own hook, i.e. market data down for every bot. Refusing the fill is the fail-closed
/// answer; the count is what makes it visible.
pub fn trade_event(trade: &Trade, local_ts: i64) -> Option<Event> {
    Some(Event {
        ev: if trade.is_buy() {
            LOCAL_BUY_TRADE_EVENT
        } else {
            LOCAL_SELL_TRADE_EVENT
        },
        exch_ts: trade.time.checked_mul(1_000_000)?,
        local_ts,
        order_id: 0,
        px: trade.px,
        qty: trade.sz,
        ival: 0,
        fval: 0.0,
    })
}

#[cfg(test)]
mod tests {
    use hftbacktest::prelude::{LOCAL_BUY_TRADE_EVENT, LOCAL_SELL_TRADE_EVENT};

    use crate::hyperliquid::{
        fixtures::TRADES_BTC_REPLAY,
        msg::{Frame, Trade, parse_frame},
        trades::{TRADE_TID_HISTORY, TradeDedup, trade_event},
    };

    fn replay() -> Vec<Trade> {
        let Frame::Trades(trades) = parse_frame(TRADES_BTC_REPLAY).unwrap() else {
            panic!("expected a trades frame");
        };
        trades
    }

    fn accepted(guard: &mut TradeDedup, trades: &[Trade]) -> usize {
        trades
            .iter()
            .filter(|t| guard.accept(&t.coin, t.tid))
            .count()
    }

    /// Hyperliquid replays the last 30 fills of a coin in one frame on **every**
    /// (re)subscribe, each carrying the fill's original `tid`. Verified against testnet on
    /// 2026-07-28: the first `trades` frame of a fresh connection held 30 fills, all 30 of
    /// which had already arrived on the previous connection.
    ///
    /// Forwarded straight through, those become genuine `TRADE_EVENT`s in the bot's
    /// `last_trades` after every network blip — 223 phantom fills for BTC in a day with
    /// ten reconnects, measured offline.
    #[test]
    fn a_replayed_frame_is_dropped_in_full() {
        let mut guard = TradeDedup::new();
        let trades = replay();

        assert_eq!(
            accepted(&mut guard, &trades),
            trades.len(),
            "first delivery"
        );
        assert_eq!(accepted(&mut guard, &trades), 0, "the replay");
        assert_eq!(guard.dropped(), trades.len() as u64);
    }

    /// The whole point of the guard is that it outlives a connection. It is owned by the
    /// stream task, not by one socket, so a reconnect cannot reset it.
    #[test]
    fn only_the_fills_that_are_new_after_a_replay_get_through() {
        let mut guard = TradeDedup::new();
        let mut trades = replay();
        accepted(&mut guard, &trades);

        // What a reconnect actually delivers: the same window, plus what happened since.
        let mut fresh = trades[0].clone();
        fresh.tid = Some(999_999_999);
        trades.push(fresh);

        assert_eq!(accepted(&mut guard, &trades), 1);
    }

    /// The window is bounded on purpose: a liquid coin sees hundreds of thousands of fills
    /// a day, and remembering every id would grow the connector's memory for as long as it
    /// runs. A replay never reaches further back than 30 fills, so 64 covers one whole
    /// replay with margin for one that arrives interleaved with fresh fills.
    #[test]
    fn the_window_is_bounded_and_evicts_the_oldest_id_first() {
        let mut guard = TradeDedup::new();
        for tid in 0..TRADE_TID_HISTORY as u64 {
            assert!(guard.accept("BTC", Some(tid)));
        }
        // Still inside the window.
        assert!(!guard.accept("BTC", Some(0)));

        // The window is ordered by first publication and a repeat does not refresh it —
        // same rule as the offline converter. So the next new id evicts id 0, the oldest,
        // and only then does 0 become publishable again.
        assert!(guard.accept("BTC", Some(1000)));
        assert!(guard.accept("BTC", Some(0)), "the oldest id fell out");
        assert!(
            !guard.accept("BTC", Some(63)),
            "the newest id is still inside the window"
        );
    }

    /// One busy coin must not evict another's ids before that coin's replay arrives.
    #[test]
    fn each_coin_keeps_its_own_window() {
        let mut guard = TradeDedup::new();
        assert!(guard.accept("ETH", Some(7)));
        for tid in 0..(TRADE_TID_HISTORY as u64 * 2) {
            guard.accept("BTC", Some(tid));
        }
        assert!(
            !guard.accept("ETH", Some(7)),
            "ETH's window was evicted by BTC"
        );
    }

    /// A fill with no `tid` carries no evidence either way — two fills of the same size at
    /// the same price and time are legitimate — so it is passed through rather than
    /// guessed at.
    #[test]
    fn a_fill_without_a_tid_is_passed_through() {
        let mut guard = TradeDedup::new();
        assert!(guard.accept("BTC", None));
        assert!(guard.accept("BTC", None));
        assert_eq!(guard.dropped(), 0);
    }

    /// `side` is the aggressor's, and the event kind must be the local trade kind — the
    /// only trade events `LiveBot` pushes into `last_trades`.
    #[test]
    fn a_trade_becomes_a_local_trade_event_on_the_aggressors_side() {
        let trades = replay();
        let event = trade_event(&trades[0], 42).unwrap();

        assert!(trades[0].is_buy());
        assert!(event.is(LOCAL_BUY_TRADE_EVENT));
        assert_eq!(event.px, 63719.0);
        assert_eq!(event.qty, 0.00016);
        assert_eq!(event.exch_ts, 1785251456343 * 1_000_000);
        assert_eq!(event.local_ts, 42);

        let mut sell = trades[0].clone();
        sell.side = "A".to_string();
        assert!(trade_event(&sell, 0).unwrap().is(LOCAL_SELL_TRADE_EVENT));
    }

    /// `overflow-checks` is on for `dev`/`test` and off for `release`, so the unchecked
    /// millisecond→nanosecond multiply was a panic in a test — and in the connector a panic
    /// is `exit(1)` under its own hook, market data down for every bot — and a silent wrap
    /// in production. The fill is refused instead.
    #[test]
    fn a_fill_whose_timestamp_does_not_fit_in_nanoseconds_is_refused_not_wrapped() {
        let mut trade = replay()[0].clone();
        trade.time = 9_999_999_999_999_999;
        assert!(trade_event(&trade, 0).is_none());

        trade.time = i64::MAX / 1_000_000;
        assert!(trade_event(&trade, 0).is_some(), "the boundary still works");
    }
}
