//! Reconnect backoff, shared by every backend.
//!
//! This lived inline in all five `keep_connection` loops as five copies of the
//! same ladder, four of which tested the rungs in ascending order — so
//! `error_count > 3` matched first and the 5s and 10s rungs could never be
//! reached no matter how long the venue stayed down. One tested place with
//! tests is what stops that happening again.

use std::time::Duration;

/// How long to wait before the next reconnect attempt.
///
/// `error_count` is the number of consecutive failures; callers reset it once a
/// connection has survived long enough to count as healthy, so the ladder
/// measures how long the venue has been unreachable rather than how long the
/// process has been running.
pub fn reconnect_delay(error_count: u32) -> Duration {
    // Descending, so each rung is reachable. The floor is not zero: the four
    // Binance and Bybit loops used to retry the first three failures with no
    // delay at all, which against a venue refusing connections is a retry storm
    // rather than a reconnect.
    if error_count > 20 {
        Duration::from_secs(10)
    } else if error_count > 10 {
        Duration::from_secs(5)
    } else if error_count > 3 {
        Duration::from_secs(1)
    } else {
        Duration::from_millis(500)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The bug this module exists for: with the comparisons in ascending order
    /// the first rung swallows every count above it, and a venue that has been
    /// down for an hour is retried once a second for ever.
    #[test]
    fn every_rung_of_the_ladder_is_reachable() {
        assert_eq!(reconnect_delay(4), Duration::from_secs(1));
        assert_eq!(reconnect_delay(11), Duration::from_secs(5));
        assert_eq!(reconnect_delay(21), Duration::from_secs(10));
    }

    /// Backoff that goes down as things get worse is not backoff.
    #[test]
    fn the_delay_never_decreases_as_errors_pile_up() {
        let mut previous = Duration::ZERO;
        for n in 0..=40 {
            let delay = reconnect_delay(n);
            assert!(
                delay >= previous,
                "delay dropped from {previous:?} to {delay:?} at error_count={n}"
            );
            previous = delay;
        }
    }

    /// A transient blip must be retried promptly, but not in a tight loop: four
    /// of the five backends had no floor at all, so the first three failures
    /// reconnected with zero delay. Against a venue refusing connections that
    /// is an unthrottled retry storm from the collector's own IP, which is a
    /// good way to turn a blip into a ban.
    #[test]
    fn the_first_reconnect_is_delayed_but_not_stalled() {
        let first = reconnect_delay(0);
        assert!(first > Duration::ZERO, "no floor: {first:?}");
        assert!(
            first < Duration::from_secs(1),
            "too slow to recover: {first:?}"
        );
    }
}
