//! The `trade` channel, and the guard that keeps its replays out of the bot.

use std::collections::{HashMap, HashSet, VecDeque};

use hftbacktest::prelude::{Event, LOCAL_BUY_TRADE_EVENT, LOCAL_SELL_TRADE_EVENT};

use crate::lighter::msg::Trade;

/// Print ids remembered per market.
///
/// The venue replays the **last 50 prints** of a market on every subscribe (design note
/// §2.4), and the connector resubscribes on every reconnect *and* on every repair — so this
/// is not a rare path. Any window above 50 covers a whole replay; the margin absorbs one
/// that arrives interleaved with fresh prints. Bounded because a market sees hundreds of
/// thousands of prints a day and a replay never reaches further back than those 50.
pub const TRADE_ID_HISTORY: usize = 96;

/// Recently published print ids, per market.
///
/// Owned by the stream **task** rather than by a connection: a guard that reset on reconnect
/// would drop exactly nothing, since a replay only ever arrives *because* of a (re)subscribe.
#[derive(Default)]
pub struct TradeDedup {
    /// Insertion-ordered window per market: the queue gives O(1) eviction of the oldest id,
    /// the set gives O(1) membership. Keyed by market because one busy market must not evict
    /// another's ids before that market's replay arrives.
    per_market: HashMap<i64, (VecDeque<u64>, HashSet<u64>)>,
    dropped: u64,
}

impl TradeDedup {
    /// Prints dropped because the venue replayed them.
    pub fn dropped(&self) -> u64 {
        self.dropped
    }

    /// Whether this print should be published.
    pub fn accept(&mut self, market_id: i64, trade_id: u64) -> bool {
        let (queue, seen) = self
            .per_market
            .entry(market_id)
            .or_insert_with(|| (VecDeque::with_capacity(TRADE_ID_HISTORY), HashSet::new()));
        if !seen.insert(trade_id) {
            self.dropped += 1;
            return false;
        }
        queue.push_back(trade_id);
        if queue.len() > TRADE_ID_HISTORY
            && let Some(oldest) = queue.pop_front()
        {
            seen.remove(&oldest);
        }
        true
    }
}

/// Builds the local trade event for one print, or `None` if the venue's timestamp cannot be
/// expressed in nanoseconds.
///
/// **The side is the aggressor's**, which on this venue is the negation of the field's name:
/// `is_maker_ask` says the *resting* side was the ask, so the taker bought.
///
/// `transaction_time` is **microseconds** — the field beside it, `timestamp`, is
/// milliseconds (§4.12). The multiplication is checked because the unchecked one is two
/// different bugs in the two profiles this crate is built with: the root `Cargo.toml` sets
/// `overflow-checks = true` for `dev`/`test` and `false` for `release`, so an oversized
/// stamp panics in a test — and a panic in the connector is `exit(1)` under its own hook,
/// i.e. market data down for every bot — and wraps silently in production.
pub fn trade_event(trade: &Trade, local_ts: i64) -> Option<Event> {
    Some(Event {
        ev: if trade.is_maker_ask {
            LOCAL_BUY_TRADE_EVENT
        } else {
            LOCAL_SELL_TRADE_EVENT
        },
        exch_ts: trade.transaction_time_us.checked_mul(1_000)?,
        local_ts,
        order_id: 0,
        px: trade.px,
        qty: trade.qty,
        ival: 0,
        fval: 0.0,
    })
}

#[cfg(test)]
mod tests {
    use hftbacktest::prelude::{LOCAL_BUY_TRADE_EVENT, LOCAL_SELL_TRADE_EVENT};

    use crate::lighter::{
        fixtures::{TRADE_CRV, TRADE_REPLAY_CRV},
        msg::{Frame, Trade, parse_frame},
        trades::{TRADE_ID_HISTORY, TradeDedup, trade_event},
    };

    fn trades(text: &str) -> Vec<Trade> {
        let Frame::Trades(trades) = parse_frame(text).unwrap() else {
            panic!("expected a trades frame");
        };
        trades
    }

    fn accepted(guard: &mut TradeDedup, trades: &[Trade]) -> usize {
        trades
            .iter()
            .filter(|t| guard.accept(t.market_id, t.trade_id))
            .count()
    }

    /// **The venue replays the last 50 prints on every subscribe** (§2.4, §4.5), and this
    /// connector subscribes on every reconnect *and* on every book repair. Forwarded
    /// straight through, those become genuine `TRADE_EVENT`s in the bot's `last_trades`
    /// after every blip — the same failure measured on Hyperliquid, where three forced
    /// reconnects produced 0 → 60 → 120 phantom prints.
    #[test]
    fn a_replayed_frame_is_dropped_in_full() {
        let mut guard = TradeDedup::default();
        let replay = trades(TRADE_REPLAY_CRV);

        assert_eq!(
            accepted(&mut guard, &replay),
            replay.len(),
            "first delivery"
        );
        assert_eq!(accepted(&mut guard, &replay), 0, "the replay");
        assert_eq!(guard.dropped(), replay.len() as u64);
    }

    /// The guard outlives a connection — it is owned by the stream task, not by one socket —
    /// and everything that is genuinely new after a replay still gets through.
    #[test]
    fn only_the_prints_that_are_new_after_a_replay_get_through() {
        let mut guard = TradeDedup::default();
        let mut replay = trades(TRADE_REPLAY_CRV);
        accepted(&mut guard, &replay);

        replay.extend(trades(TRADE_CRV));
        assert_eq!(accepted(&mut guard, &replay), 1);
    }

    /// The window is bounded on purpose: a market sees hundreds of thousands of prints a
    /// day, and remembering every id would grow the connector's memory for as long as it
    /// runs. A replay never reaches further back than 50, so 96 covers one whole replay with
    /// margin for one that arrives interleaved with fresh prints.
    #[test]
    fn the_window_is_bounded_and_evicts_the_oldest_id_first() {
        let mut guard = TradeDedup::default();
        for trade_id in 0..TRADE_ID_HISTORY as u64 {
            assert!(guard.accept(36, trade_id));
        }
        assert!(!guard.accept(36, 0), "still inside the window");

        // Ordered by first publication, and a repeat does not refresh it: the next new id
        // evicts id 0, and only then does 0 become publishable again.
        assert!(guard.accept(36, 1_000));
        assert!(guard.accept(36, 0), "the oldest id fell out");
        assert!(
            !guard.accept(36, TRADE_ID_HISTORY as u64 - 1),
            "the newest id is still inside the window"
        );
    }

    /// One busy market must not evict another's ids before that market's replay arrives.
    #[test]
    fn each_market_keeps_its_own_window() {
        let mut guard = TradeDedup::default();
        assert!(guard.accept(0, 7));
        for trade_id in 0..(TRADE_ID_HISTORY as u64 * 2) {
            guard.accept(36, trade_id);
        }
        assert!(!guard.accept(0, 7), "market 0's window was evicted by 36");
    }

    /// **The side is the aggressor's, and the field is named for the other one.**
    /// `is_maker_ask: true` means the resting side was the ask, so the taker *bought*.
    /// Getting this backwards inverts every print in the bot's tape, and nothing anywhere
    /// would say so.
    #[test]
    fn a_print_becomes_a_local_trade_event_on_the_aggressors_side() {
        let trades = trades(TRADE_CRV);
        let event = trade_event(&trades[0], 42).unwrap();

        assert!(trades[0].is_maker_ask);
        assert!(event.is(LOCAL_BUY_TRADE_EVENT), "the maker was the ask");
        assert_eq!(event.px, 0.20382);
        assert_eq!(event.qty, 0.7);
        // Microseconds, like the book's engine stamp and unlike the `timestamp` beside it.
        assert_eq!(event.exch_ts, 1_785_358_835_917_189 * 1_000);
        assert_eq!(event.local_ts, 42);

        let mut sell = trades[0].clone();
        sell.is_maker_ask = false;
        assert!(trade_event(&sell, 0).unwrap().is(LOCAL_SELL_TRADE_EVENT));
    }

    /// `overflow-checks` is on for `dev`/`test` and off for `release`, so the unchecked
    /// microsecond→nanosecond multiply is a panic in a test and a silent wrap in production.
    /// The print is refused instead.
    #[test]
    fn a_print_whose_timestamp_does_not_fit_in_nanoseconds_is_refused_not_wrapped() {
        let mut trade = trades(TRADE_CRV)[0].clone();
        trade.transaction_time_us = i64::MAX / 999;
        assert!(trade_event(&trade, 0).is_none());

        trade.transaction_time_us = i64::MAX / 1_000;
        assert!(trade_event(&trade, 0).is_some(), "the boundary still works");
    }
}
