//! Retry timing, shared by every backend.
//!
//! Two ladders, and the difference between them is the point of putting them in
//! one file. [`reconnect_delay`] is unbounded: a running collector reconnects
//! for as long as the venue is away, because the alternative is to stop
//! recording over a blip. [`retry_startup`] is bounded to three attempts: a
//! *starting* collector has recorded nothing, so retrying for ever would leave
//! systemd believing the unit is coming up while nothing is being captured and
//! nothing has failed. It absorbs a hiccup and then fails closed.
//!
//! The reconnect ladder lived inline in all five `keep_connection` loops as five
//! copies of the same thing, four of which tested the rungs in ascending order —
//! so `error_count > 3` matched first and the 5s and 10s rungs could never be
//! reached no matter how long the venue stayed down. One tested place with tests
//! is what stops that happening again.

use std::{fmt::Display, time::Duration};

use tracing::warn;

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

/// How many times a startup REST call is tried before the collector gives up.
///
/// Three, not more. Each attempt already carries a timeout of its own — 15s for
/// both Hyperliquid's `/info` and Lighter's catalog — so the ladder multiplies a
/// wait that is not small to begin with, and the fault it exists for is a
/// transient one: the second attempt is where nearly all of the value is.
pub const STARTUP_ATTEMPTS: usize = 3;

/// What to wait before each retry. One rung shorter than [`STARTUP_ATTEMPTS`]:
/// the ladder waits *between* attempts, never after the last one.
///
/// Seconds rather than the reconnect ladder's milliseconds. A reconnect races to
/// get back to a live feed it is currently missing; a startup retry is answering
/// a network that has just refused, and the thing worth waiting out — a DNS
/// blip, a rate limiter, a load balancer moving — is measured in seconds. It is
/// also one process among several restarting together after a host event, which
/// is exactly when hammering a venue is least welcome.
pub const STARTUP_BACKOFF: [Duration; STARTUP_ATTEMPTS - 1] =
    [Duration::from_secs(2), Duration::from_secs(4)];

/// Runs a startup pre-flight, absorbing up to [`STARTUP_ATTEMPTS`] - 1
/// transient failures, and returns the last error if none of them worked.
///
/// For the REST calls that resolve symbols before anything is recorded, and for
/// nothing else. Mid-run REST — the depth-snapshot refetch, the `premiumIndex`
/// poller — has policies of its own that this must not be wired into: a
/// snapshot is worthless once stale and a poll is superseded by the next one, so
/// retrying either behind the caller's back would hold a throttle slot to
/// deliver something out of date.
///
/// `what` names the call in the warning, because by the time an operator reads
/// the journal the interesting question is which endpoint was flaky, not that
/// something was.
///
/// The failures are warnings and not errors: a retried failure is not what
/// stopped the collector, and logging it at the same level as the refusal to
/// start would put two indistinguishable lines in the journal, only one of
/// which ended the process.
///
/// # What this costs a supervisor
///
/// The ladder's own delay is 6s, but each attempt carries the caller's timeout,
/// so a venue that **hangs** rather than refuses now costs 3 × 15s + 6s = 51s
/// per endpoint against 15s before. Hyperliquid makes one call for `spotMeta`
/// plus one per referenced dex, so the shipped example config (canonical plus
/// `xyz`) refuses to start in ~153s where it used to take ~45s; a venue that
/// refuses outright is unchanged bar the 6s. Nothing masks a down venue — the
/// exit is the same one with the same error — but the unit takes longer to get
/// there, and `hft-collector@.service`'s `StartLimitIntervalSec=3600` /
/// `StartLimitBurst=10` is what turns a repeated failure into `failed` and an
/// alert. At 153s + `RestartSec=5s` that is ~26 minutes to the tenth start,
/// still inside the hour. It stops being inside the hour at **six** referenced
/// dexes (7 endpoints, 362s a cycle against the 360s ten starts allow), and at
/// that point the unit would restart for ever without ever reaching `failed` —
/// the exact silent-crash-loop the unit file's `StartLimitIntervalSec` comment
/// was written to prevent. Worth a `TimeoutStartSec` on the unit if a config
/// that wide ever becomes real rather than arithmetic.
pub async fn retry_startup<T, E, F, Fut>(what: &str, mut attempt: F) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: Display,
{
    for (n, delay) in STARTUP_BACKOFF.iter().enumerate() {
        match attempt().await {
            Ok(value) => return Ok(value),
            Err(error) => warn!(
                attempt = n + 1,
                of = STARTUP_ATTEMPTS,
                %error,
                retry_in_s = delay.as_secs(),
                "{what} failed at startup; retrying"
            ),
        }
        tokio::time::sleep(*delay).await;
    }
    // The last attempt's error is the one that gets returned, reported and
    // recorded in the sidecar. No wrapping: the venue's own words are what an
    // operator needs, and a "3 attempts failed" prefix would push them past the
    // end of a journal line for no information.
    attempt().await
}

#[cfg(test)]
mod startup_tests {
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use super::*;

    /// The incident, in one test. On 2026-07-29 at 06:09:48 a Hyperliquid
    /// instance came up, met one `error sending request` on the `/info` round
    /// trip that resolves its symbols, and exited a second later; the next
    /// systemd restart got through on the first try. A fresh process has to
    /// survive one network hiccup on the way up.
    #[tokio::test(start_paused = true)]
    async fn a_resolve_that_fails_twice_and_then_succeeds_starts_the_collector() {
        let calls = AtomicUsize::new(0);

        let resolved = retry_startup("the perp universe", || async {
            match calls.fetch_add(1, Ordering::Relaxed) {
                0 | 1 => Err(anyhow::anyhow!("error sending request")),
                _ => Ok("BTC"),
            }
        })
        .await
        .expect("two transient failures must not stop a startup that would have worked");

        assert_eq!(resolved, "BTC");
        assert_eq!(calls.load(Ordering::Relaxed), STARTUP_ATTEMPTS);
    }

    /// Retrying is not the same as not failing closed. A venue that is actually
    /// down still has to stop the process, with the venue's own last error and
    /// not a synthetic one — that error is what `symbol_check_failed` records in
    /// the sidecar, and it is the only explanation a missing day will have.
    #[tokio::test(start_paused = true)]
    async fn a_resolve_that_never_succeeds_still_fails_closed() {
        let calls = AtomicUsize::new(0);

        let error = retry_startup("the perp universe", || async {
            calls.fetch_add(1, Ordering::Relaxed);
            Err::<(), _>(anyhow::anyhow!("dns error: no record found"))
        })
        .await
        .expect_err("an unreachable venue must still refuse to start");

        assert!(error.to_string().contains("dns error"), "{error}");
        assert_eq!(
            calls.load(Ordering::Relaxed),
            STARTUP_ATTEMPTS,
            "the ladder is bounded: a venue that is down must not be retried for ever \
             while systemd believes the unit is starting"
        );
    }

    /// A healthy start pays nothing for the retries. The whole ladder is only
    /// reached by a startup that was going to fail anyway, so the delay budget
    /// is spent where it buys something.
    #[tokio::test(start_paused = true)]
    async fn a_resolve_that_works_first_time_is_not_delayed() {
        let start = tokio::time::Instant::now();
        retry_startup("the perp universe", || async { Ok::<_, anyhow::Error>(()) })
            .await
            .unwrap();
        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    /// Bounded in time, not only in attempts, because the thing waiting is
    /// systemd. `Restart=on-failure` with a `RestartSec` measured in seconds
    /// expects a process that either records or exits; the ladder has to stay
    /// small next to that.
    ///
    /// Virtual time: paused, the clock only advances when every task is parked,
    /// so this measures the sleeps and nothing else.
    ///
    /// **And only the sleeps** — which is why the name says "the ladder" and
    /// not "the startup". The closure here fails instantly; a real one carries
    /// its own 15s timeout, so a venue that hangs rather than refuses costs
    /// `STARTUP_ATTEMPTS × 15s + 6s` = 51s per endpoint, and Hyperliquid makes
    /// one call for `spotMeta` plus one per dex. The wall-clock worst case is
    /// bounded, but it is minutes and not seconds, and the figure this test
    /// pins is not it. See [`retry_startup`] for what that costs a supervisor.
    #[tokio::test(start_paused = true)]
    async fn the_whole_ladder_is_over_in_seconds() {
        let start = tokio::time::Instant::now();
        let _ = retry_startup("the perp universe", || async {
            Err::<(), _>(anyhow::anyhow!("connection refused"))
        })
        .await;

        assert_eq!(
            start.elapsed(),
            STARTUP_BACKOFF.iter().sum::<Duration>(),
            "the delay between attempts must be the ladder and nothing else"
        );
        assert!(
            start.elapsed() <= Duration::from_secs(10),
            "a startup that is going to fail must fail promptly: {:?}",
            start.elapsed()
        );
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
