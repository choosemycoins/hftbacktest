//! A floor under everything else: if no market data at all reaches disk for
//! long enough, stop.
//!
//! Every other guard in the collector answers a fault it can name — a full
//! queue, a closed channel, a disk floor, an unknown symbol. This one answers
//! the faults nobody enumerated. A venue that accepts a subscription and then
//! sends nothing, a reconnect loop that reconnects but never resubscribes, a
//! parser that files every frame under a branch that drops it: none of those
//! produce an error, and the process looks perfectly healthy from outside while
//! the recording is empty.
//!
//! ## What it does not catch
//!
//! Being honest about this matters, because a watchdog is exactly the sort of
//! thing that gets mistaken for coverage it does not have. It fires only on
//! **total silence across every symbol and every stream**. It will not notice:
//!
//! * a dead depth stream while trades keep arriving — or the reverse;
//! * one symbol out of ten that stopped;
//! * one Hyperliquid cadence stopping while the other two continue;
//! * a partially accepted subscription, which looks identical to a full one.
//!
//! Completeness — did we get what we asked for, on every stream, all day — is
//! Phase 2's offline report over finished files, not something this process can
//! decide about itself. See `docs/design-multi-venue-collection.md`.
//!
//! It is also driven by **writes**, not by messages arriving. A backlog
//! draining out of a bounded queue into the writer is life; a backlog sitting
//! in the queue behind a stalled consumer is not, and must not reset the clock
//! — that is the failure `queue.rs` bounds the hand-offs for. The one case
//! neither of them reaches is a writer wedged in a syscall: `main` never
//! returns to the select loop, so this arm is never polled either, and only an
//! external watchdog (systemd `WatchdogSec`) gets the process out of it.
//!
//! And it is driven by writes **the venue caused**. See [`Source`]: a record the
//! collector produced on a timer of its own says nothing about whether the feed
//! is still arriving, and counting one would disarm this guard on the backend
//! that has such a producer.

use std::time::Duration;

use tokio::time::{Instant, sleep_until};

use crate::file::META_STREAM;

/// What caused a record, and therefore whether it vouches for the feed.
///
/// The distinction exists because not every line in a symbol file comes off a
/// socket. `binancefuturesum` polls `GET /fapi/v1/premiumIndex` on a timer of
/// its own and files each element under the symbol it names, exactly as a
/// WebSocket frame is filed — the two are indistinguishable by stream name, by
/// filename and by shape of hand-off.
///
/// They must not be indistinguishable **here**. A poller keeps running when the
/// socket is dead: measured 2026-07-28 with `fstream` blackholed, a USD-M
/// instance survived past a one-minute stall timeout having recorded nothing
/// but index samples, and would have done so indefinitely with systemd
/// reporting it healthy. Counting only [`Source::Venue`] is what keeps "nothing
/// is reaching disk" meaning "the feed has stopped" rather than "even the timer
/// has stopped".
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Source {
    /// A venue feed produced it. This is what the recording is.
    Venue,
    /// The collector produced it beside the feed, on a schedule of its own.
    Collector,
}

/// Stops the collector when nothing at all is reaching disk.
pub struct StallWatchdog {
    /// `None` disables the check entirely.
    timeout: Option<Duration>,
    last_write: Instant,
}

impl StallWatchdog {
    /// `timeout_min` of `0` disables the check.
    pub fn new(timeout_min: u64) -> Self {
        Self {
            // Saturating, because this comes off the command line: an absurd
            // value must give an absurd deadline, not an arithmetic panic. The
            // release profile aborts on panic, so one here would take the day's
            // unfinished gzip members with it.
            timeout: (timeout_min > 0).then(|| Duration::from_secs(timeout_min.saturating_mul(60))),
            // The clock starts now, not at the first record: a collector that
            // never records anything is the case this is most needed for.
            last_write: Instant::now(),
        }
    }

    /// Notes a record on its way to disk, restarting the clock.
    ///
    /// Sidecar records do not count, and the case that makes the exclusion
    /// load-bearing is a reconnect storm: every retry emits `subscribe`,
    /// `connected`/`dial_failed` and `disconnected` into the same hop the
    /// venue's frames travel (`meta.rs`), all of them filed under `_meta`. With
    /// the backoff floor at 500ms a venue refusing connections produces a
    /// couple a second indefinitely, which would keep this satisfied while not
    /// one market-data frame is recorded.
    ///
    /// `main`'s own `_meta` records — the minutely disk gauge and the two
    /// terminal ones — never arrive here at all: they are written straight to
    /// the `Writer`, bypassing the queue this counts records off.
    ///
    /// [`Source::Collector`] records do not count either, and for the same
    /// reason one step further out: a record the collector produced on its own
    /// timer is evidence that the timer is running, not that the venue is still
    /// sending. See [`Source`].
    pub fn record_write(&mut self, source: Source, stream: &str) {
        if source == Source::Venue && stream != META_STREAM {
            self.last_write = Instant::now();
        }
    }

    /// Resolves when the timeout has passed with nothing recorded, reporting
    /// how long the silence has lasted.
    ///
    /// Borrows rather than consumes, so it can sit in a `select!` arm: whenever
    /// another arm wins, this future is dropped and rebuilt next time round the
    /// loop against whatever `record_write` has since done to the deadline.
    pub async fn stalled(&self) -> Duration {
        let Some(timeout) = self.timeout else {
            // Disabled. Pending for ever, so the arm simply never wins —
            // returning immediately would make `0` mean "stop at once", and
            // any finite answer would be a check the operator asked not to have.
            return std::future::pending().await;
        };
        // `Instant + Duration` panics when the sum is unrepresentable. A
        // deadline that far out is unreachable anyway, so treat it as the
        // disabled case rather than as a reason to abort the process.
        let Some(deadline) = self.last_write.checked_add(timeout) else {
            return std::future::pending().await;
        };
        sleep_until(deadline).await;
        self.last_write.elapsed()
    }
}

#[cfg(test)]
mod tests {
    use tokio::select;

    use super::*;

    const TIMEOUT_MIN: u64 = 5;
    const TIMEOUT: Duration = Duration::from_secs(TIMEOUT_MIN * 60);

    /// The failure this exists for: the process is up, the socket looks
    /// connected, and not one record has been written. Nothing else in the
    /// collector notices — a connection that never delivers raises no error,
    /// and the disk gauge keeps reporting healthily.
    ///
    /// The clock starts at construction, so a collector that never records
    /// anything at all trips it too.
    #[tokio::test(start_paused = true)]
    async fn total_silence_trips_it_after_the_timeout() {
        let start = Instant::now();
        let watchdog = StallWatchdog::new(TIMEOUT_MIN);

        let silence = watchdog.stalled().await;

        assert!(
            silence >= TIMEOUT,
            "reported only {silence:?} of silence, expected at least {TIMEOUT:?}"
        );
        assert!(
            start.elapsed() >= TIMEOUT && start.elapsed() < TIMEOUT * 2,
            "fired after {:?}, expected close to {TIMEOUT:?}",
            start.elapsed()
        );
    }

    /// A recording collector must never be stopped by its own watchdog. The
    /// margin is wide on purpose: the slowest legitimate feed is Hyperliquid's
    /// plain `l2Book` at ~5.4s, so a record a minute is already an order of
    /// magnitude slower than anything real.
    #[tokio::test(start_paused = true)]
    async fn a_feed_that_keeps_writing_never_trips_it() {
        let mut watchdog = StallWatchdog::new(TIMEOUT_MIN);

        for _ in 0..(TIMEOUT_MIN * 3) {
            select! {
                silence = watchdog.stalled() => {
                    panic!("tripped after {silence:?} while records were still arriving")
                }
                _ = tokio::time::sleep(Duration::from_secs(60)) => {
                    watchdog.record_write(Source::Venue, "BTC");
                }
            }
        }
    }

    /// The sidecar must not vouch for the market data, and the case that makes
    /// that load-bearing is a reconnect storm.
    ///
    /// Every retry writes `subscribe`, then `connected` or `dial_failed`, then
    /// `disconnected` — all filed under `_meta`, all travelling the same hop as
    /// market data and so all reaching `record_write`. With the backoff floor at
    /// 500ms (`backoff.rs`) a venue refusing connections produces a couple a
    /// second for as long as it stays down. Counting them would leave the
    /// watchdog permanently satisfied by a collector recording nothing at all,
    /// which is precisely the failure it exists for.
    ///
    /// Bounded by a timeout rather than left to `loop`: without the exclusion
    /// this never stops, and under a paused clock it does not stop slowly — it
    /// spins virtual time forwards for ever. `cargo test` is the only gate this
    /// repo has, and a wedged run reads as "still compiling", not as a failure.
    #[tokio::test(start_paused = true)]
    async fn sidecar_records_do_not_count_as_life() {
        let start = Instant::now();
        let mut watchdog = StallWatchdog::new(TIMEOUT_MIN);
        let mut retry = tokio::time::interval(Duration::from_millis(500));

        let silence = tokio::time::timeout(TIMEOUT * 2, async {
            loop {
                select! {
                    silence = watchdog.stalled() => return silence,
                    _ = retry.tick() => {
                        // One failed reconnect's worth of sidecar traffic.
                        watchdog.record_write(Source::Venue, META_STREAM);
                        watchdog.record_write(Source::Venue, META_STREAM);
                        watchdog.record_write(Source::Venue, META_STREAM);
                    }
                }
            }
        })
        .await
        .expect("a reconnect storm's sidecar records held the watchdog off indefinitely");

        assert!(
            silence >= TIMEOUT,
            "tripped after only {silence:?} of silence"
        );
        assert!(
            start.elapsed() < TIMEOUT * 2,
            "tripped after {:?}, expected close to {TIMEOUT:?}",
            start.elapsed()
        );
    }

    /// The collector's own periodic output must not vouch for the venue's feed,
    /// and the case that makes it load-bearing is `binancefuturesum`'s
    /// `premiumIndex` poller.
    ///
    /// It writes into the **symbol** files, not the sidecar — that is the whole
    /// design, the index and funding numbers belong beside the book they price
    /// — so the `_meta` exclusion above does not reach it. And it runs on a
    /// timer of its own, so it keeps writing with the socket dead. Measured
    /// 2026-07-28 with `fstream` blackholed: a USD-M instance ran 120s past a
    /// 60s stall timeout, having recorded twelve index samples and not one
    /// frame, and would have kept going for as long as `fapi` answered. The
    /// identical run with the poller filing nothing exited at exactly 60s.
    ///
    /// Bounded by a timeout for the same reason as the sidecar test below it:
    /// without the exclusion this never fires, and under a paused clock a
    /// `loop` would spin virtual time forwards for ever rather than fail.
    #[tokio::test(start_paused = true)]
    async fn the_collectors_own_records_do_not_count_as_life() {
        let start = Instant::now();
        let mut watchdog = StallWatchdog::new(TIMEOUT_MIN);
        let mut poll = tokio::time::interval(Duration::from_secs(10));

        let silence = tokio::time::timeout(TIMEOUT * 2, async {
            loop {
                select! {
                    silence = watchdog.stalled() => return silence,
                    _ = poll.tick() => {
                        // One cycle of the poller, filed under the venue's own
                        // spelling of two recorded symbols — the same stream
                        // names the WebSocket frames would arrive under.
                        watchdog.record_write(Source::Collector, "BTCUSDT");
                        watchdog.record_write(Source::Collector, "ETHUSDT");
                    }
                }
            }
        })
        .await
        .expect("a working poller held the watchdog off while nothing was being recorded");

        assert!(
            silence >= TIMEOUT,
            "tripped after only {silence:?} of silence"
        );
        assert!(
            start.elapsed() < TIMEOUT * 2,
            "tripped after {:?}, expected close to {TIMEOUT:?}",
            start.elapsed()
        );
    }

    /// The other half of it: the exclusion must be about provenance, not about
    /// the stream name. The poller files under exactly the names the socket
    /// files under, so a rule written against names would either count the
    /// poller or stop counting the feed.
    #[tokio::test(start_paused = true)]
    async fn a_venue_frame_under_the_same_stream_name_does_count() {
        let mut watchdog = StallWatchdog::new(TIMEOUT_MIN);

        for _ in 0..(TIMEOUT_MIN * 3) {
            select! {
                silence = watchdog.stalled() => {
                    panic!("tripped after {silence:?} while the venue was still sending")
                }
                _ = tokio::time::sleep(Duration::from_secs(60)) => {
                    watchdog.record_write(Source::Venue, "BTCUSDT");
                }
            }
        }
    }

    /// A nonsensical timeout must be inert, not a panic. The release profile
    /// sets `panic = "abort"`, so a panic anywhere in the select loop kills the
    /// process without finishing a single gzip member — exactly the outcome the
    /// rest of this collector is arranged to avoid.
    #[tokio::test(start_paused = true)]
    async fn a_timeout_too_large_to_represent_is_inert() {
        let watchdog = StallWatchdog::new(u64::MAX);

        let fired = tokio::time::timeout(Duration::from_secs(24 * 3600), watchdog.stalled()).await;

        assert!(fired.is_err(), "an unreachable deadline must never fire");
    }

    /// `0` is the documented escape hatch, and it has to be inert rather than
    /// instantaneous — a zero timeout that fired immediately would turn "I
    /// don't want this check" into a collector that cannot start.
    #[tokio::test(start_paused = true)]
    async fn zero_disables_it() {
        let watchdog = StallWatchdog::new(0);

        let fired = tokio::time::timeout(Duration::from_secs(24 * 3600), watchdog.stalled()).await;

        assert!(
            fired.is_err(),
            "a disabled watchdog fired after {:?}",
            fired.unwrap()
        );
    }
}
